# src/setup/build_commerce_agent.py
# Databricks notebook source
# Configure the AI-Gateway LLM endpoint, log+register the commerce agent, and (re)create
# the agent Model Serving endpoint. Mirrors train_recommender.py conventions.
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

# --- 1. AI-Gateway LLM endpoint (the ONLY model path the agent uses) ---
from src.agent.gateway import build_gateway_endpoint_body
llm_endpoint = f"{schema_prefix}qsr-agent-llm"
gw_body = build_gateway_endpoint_body(llm_endpoint, agent_llm_model, rate_limit_rpm=200)
try:
    w.api_client.do("GET", f"/api/2.0/serving-endpoints/{llm_endpoint}")
    w.api_client.do("PUT", f"/api/2.0/serving-endpoints/{llm_endpoint}/ai-gateway",
                    body=gw_body["ai_gateway"])
    print(f"[INFO] updated AI Gateway config on existing {llm_endpoint}")
except Exception:
    try:
        w.api_client.do("POST", "/api/2.0/serving-endpoints", body=gw_body)
        print(f"[INFO] created AI-Gateway LLM endpoint {llm_endpoint}")
    except Exception as e:
        print(f"[WARN] AI-Gateway LLM endpoint setup: {repr(e)}")

# --- 2. Bake menu + price lookup artifacts (read to driver; small) ---
menu_rows = spark.read.table(f"{sp}ref.menu_item").select(
    "menu_item_id", "item_name", "category", "subcategory").collect()
menu = {int(r["menu_item_id"]): {"item_name": r["item_name"], "category": r["category"],
                                 "subcategory": r["subcategory"]} for r in menu_rows}
# current-period price per item (latest effective row)
price_rows = spark.sql(f"""
    SELECT menu_item_id, price FROM {sp}ref.item_price ip
    WHERE ip.effective_to IS NULL OR ip.effective_to >= current_date()
""").collect()
price_lookup = {int(r["menu_item_id"]): float(r["price"]) for r in price_rows}
print(f"[INFO] baked {len(menu)} menu items, {len(price_lookup)} prices")

# --- 3. Log + register the agent ---
import mlflow
from src.agent.commerce_agent import CommerceAgent
from src.agent.prompts import SYSTEM_PROMPT
mlflow.set_registry_uri("databricks-uc")

# The real llm_client + toolbox_factory are constructed inside the model module at load
# time (they need the serving runtime's workspace creds + endpoint names). For logging we
# pass an instance carrying the config it needs; load_context rebinds live clients.
model_name = fq("qsr_commerce_agent")
import mlflow.models
resources = [
    mlflow.models.resources.DatabricksServingEndpoint(endpoint_name=llm_endpoint),
    mlflow.models.resources.DatabricksServingEndpoint(endpoint_name=f"{schema_prefix}qsr-recommender"),
    mlflow.models.resources.DatabricksServingEndpoint(endpoint_name=f"{schema_prefix}qsr-customer-features"),
    mlflow.models.resources.DatabricksTable(table_name=f"{sp}ref.menu_item"),
    mlflow.models.resources.DatabricksTable(table_name=f"{sp}silver.guest_order"),
]
import mlflow.pyfunc
with mlflow.start_run(run_name="qsr_commerce_agent"):
    mlflow.pyfunc.log_model(
        artifact_path="commerce_agent",
        python_model=f"{_bundle_root}/src/agent/commerce_agent.py",
        registered_model_name=model_name,
        resources=resources,
        code_paths=[f"{_bundle_root}/src"],
        pip_requirements=["mlflow", "databricks-openai", "databricks-sdk", "pyyaml"],
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
