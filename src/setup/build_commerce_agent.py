# Databricks notebook source
# src/setup/build_commerce_agent.py — Configure the AI-Gateway LLM endpoint,
# log+register the commerce agent, and (re)create the agent Model Serving endpoint.
# Mirrors train_recommender.py conventions.
import sys

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_notebook_path.replace("/Workspace", "").split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)


def _widget(name, default):
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return default


catalog_name = _widget("catalog_name", "jmrdemo")
schema_prefix = _widget("schema_prefix", "synth_")
agent_llm_model = _widget("agent_llm_model", "databricks-claude-3-7-sonnet")
query_principal = _widget("commerce_agent_query_principal", "")
print(f"[INFO] build_commerce_agent: catalog={catalog_name} prefix={schema_prefix} "
      f"llm={agent_llm_model}")

sp = f"{catalog_name}.{schema_prefix}"
fq = lambda t: f"{catalog_name}.{schema_prefix}features.{t}"  # noqa: E731

from databricks.sdk import WorkspaceClient
import time as _t
w = WorkspaceClient()

# --- 1. AI Gateway: the agent reaches its LLM through the existing Databricks
# foundation-model endpoint (agent_llm_model), with AI Gateway ENABLED IN PLACE on that
# endpoint (usage tracking, rate limits, PII guardrails). Pay-per-token FM endpoints are
# system-managed and cannot be re-served in a new endpoint, so the gateway choke-point is
# applied to the FM endpoint itself. This keeps "all model access routes through AI
# Gateway" true. (Workspace decision 2026-06-18; see contract ledger §1/§6.) ---
from src.agent.gateway import build_gateway_endpoint_body
llm_endpoint = agent_llm_model
gw_body = build_gateway_endpoint_body(llm_endpoint, agent_llm_model, rate_limit_rpm=200)
try:
    w.api_client.do("PUT", f"/api/2.0/serving-endpoints/{llm_endpoint}/ai-gateway",
                    body=gw_body["ai_gateway"])
    print(f"[INFO] enabled AI Gateway on foundation-model endpoint {llm_endpoint}")
except Exception as e:
    print(f"[WARN] AI Gateway enable on {llm_endpoint} skipped: {repr(e)}")

# --- 2. Bake menu + price + occasion artifacts (read to driver; small).
# Prices are INDICATIVE (contract §3.1 — the BFF re-prices authoritatively at
# place_order), so base_price is the honest indicative figure. item_price holds only
# per-period multipliers and unit market indices apply per store, neither known at bake
# time — base_price avoids that and keeps the served container Spark-free. ---
menu_rows = spark.read.table(f"{sp}ref.menu_item").select(
    "menu_item_id", "item_name", "category", "subcategory", "base_price").collect()
menu = {int(r["menu_item_id"]): {"item_name": r["item_name"], "category": r["category"],
                                 "subcategory": r["subcategory"]} for r in menu_rows}
price_lookup = {int(r["menu_item_id"]): float(r["base_price"] or 0.0) for r in menu_rows}
# Bake a small set of upcoming local events for the occasion tool (metro-keyed upstream;
# store->metro mapping + live recency are a v2 follow-up — see contract ledger).
try:
    occ_rows = spark.sql(f"""
        SELECT event_name, event_date, event_category, metro_area
        FROM {sp}ref.local_events
        WHERE event_date >= current_date()
        ORDER BY event_date LIMIT 200
    """).collect()
    occasions = [{"name": r["event_name"], "date": str(r["event_date"]),
                  "category": r["event_category"], "metro": r["metro_area"]} for r in occ_rows]
except Exception as e:
    occasions = []
    print(f"[WARN] occasion bake skipped: {repr(e)}")
print(f"[INFO] baked {len(menu)} menu items, {len(price_lookup)} prices, {len(occasions)} occasions")

# --- 3. Log + register the agent as a baked instance (cloudpickle), mirroring
# train_recommender's RecommenderModel pattern. The instance carries menu/price/occasion +
# endpoint config; CommerceAgent.load_context builds the live OpenAI client + toolbox at
# serving load. (Models-from-Code path logging would require set_model() in the module;
# the baked-instance path is the proven convention in this repo.) ---
import mlflow
from src.agent.commerce_agent import CommerceAgent
from src.agent.prompts import SYSTEM_PROMPT
mlflow.set_registry_uri("databricks-uc")

rec_endpoint = f"{schema_prefix}qsr-recommender"
feat_endpoint = f"{schema_prefix}qsr-customer-features"
agent = CommerceAgent(
    SYSTEM_PROMPT, menu=menu, price_lookup=price_lookup, occasions=occasions,
    config={"llm_endpoint": llm_endpoint, "recommender_endpoint": rec_endpoint,
            "feature_endpoint": feat_endpoint})

model_name = fq("qsr_commerce_agent")
import mlflow.models
resources = [
    mlflow.models.resources.DatabricksServingEndpoint(endpoint_name=llm_endpoint),
    mlflow.models.resources.DatabricksServingEndpoint(endpoint_name=rec_endpoint),
    mlflow.models.resources.DatabricksServingEndpoint(endpoint_name=feat_endpoint),
]
import mlflow.pyfunc
with mlflow.start_run(run_name="qsr_commerce_agent"):
    mlflow.pyfunc.log_model(
        artifact_path="commerce_agent",
        python_model=agent,
        registered_model_name=model_name,
        resources=resources,
        code_paths=[f"{_bundle_root}/src"],
        # The serving container reaches the AI-Gateway FM endpoint via the SDK's OpenAI
        # client (needs the `openai` package) and calls the recommender/feature endpoints
        # via api_client.do (databricks-sdk). Pin both.
        pip_requirements=["mlflow", "openai", "databricks-sdk", "pyyaml"],
    )
print(f"[INFO] registered {model_name}")

# --- 4. (Re)create the agent serving endpoint via raw REST (Fix 9 pattern) ---
latest = max(int(v.version) for v in w.model_versions.list(full_name=model_name))
endpoint = f"{schema_prefix}qsr-commerce-agent"
served_entity = {
    "entity_name": model_name,
    "entity_version": str(latest),
    "scale_to_zero_enabled": True,
    "workload_size": "Small",
    "environment_vars": {
        "LLM_ENDPOINT": llm_endpoint,
        "RECOMMENDER_ENDPOINT": f"{schema_prefix}qsr-recommender",
        "FEATURE_ENDPOINT": f"{schema_prefix}qsr-customer-features",
        "CATALOG_NAME": catalog_name,
        "SCHEMA_PREFIX": schema_prefix,
    },
}
try:
    w.api_client.do("GET", f"/api/2.0/serving-endpoints/{endpoint}")
    _exists = True
except Exception:
    _exists = False
_ok = False
for _attempt in range(3):
    try:
        if _exists:
            w.api_client.do("PUT", f"/api/2.0/serving-endpoints/{endpoint}/config",
                            body={"served_entities": [served_entity]})
        else:
            w.api_client.do("POST", "/api/2.0/serving-endpoints",
                            body={"name": endpoint, "config": {"served_entities": [served_entity]}})
        _ok = True
        break
    except Exception as e:
        print(f"[WARN] agent endpoint REST submit attempt {_attempt + 1} failed: {repr(e)}")
        if _attempt < 2:  # don't sleep after the final attempt
            _t.sleep(20)
if not _ok:
    raise RuntimeError(f"failed to submit serving endpoint {endpoint} via REST API")
print(f"[INFO] {endpoint} submitted via REST; provisions to READY asynchronously")

# --- 5. Grant CAN_QUERY to the website principal ---
if query_principal:
    from databricks.sdk.service.serving import (
        ServingEndpointAccessControlRequest, ServingEndpointPermissionLevel)
    try:
        w.serving_endpoints.set_permissions(
            serving_endpoint_id=w.serving_endpoints.get(name=endpoint).id,
            access_control_list=[ServingEndpointAccessControlRequest(
                service_principal_name=query_principal,
                permission_level=ServingEndpointPermissionLevel.CAN_QUERY)])
        print(f"[INFO] granted CAN_QUERY on {endpoint} to {query_principal}")
    except Exception as e:
        print(f"[WARN] grant CAN_QUERY skipped: {e}")
else:
    print("[INFO] no commerce_agent_query_principal set; skipping CAN_QUERY grant")
print("[DONE] build_commerce_agent complete")
