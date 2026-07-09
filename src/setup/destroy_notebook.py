# Databricks notebook source
# COMMAND ----------
import sys

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_nb_path.replace("/Workspace", "").split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

# COMMAND ----------
try:
    catalog_name = dbutils.widgets.get("catalog_name")
except Exception:
    catalog_name = "jmrdemo"

try:
    schema_prefix = dbutils.widgets.get("schema_prefix")
except Exception:
    schema_prefix = "synth_"

print(f"[INFO] Destroy: catalog={catalog_name}, schema_prefix={schema_prefix}")

# COMMAND ----------
# Step 0a: Drop column masks from staging tables BEFORE dropping ref schema/functions.
# Masks on staging.guest_events reference synth_ref.mask_email/mask_phone. If these
# functions are dropped while the masks remain, any query on guest_events fails with
# UC_DEPENDENCY_DOES_NOT_EXIST — including DLT streaming reads and backfill queries.
for col in ["email", "phone"]:
    try:
        spark.sql(
            f"ALTER TABLE {catalog_name}.{schema_prefix}staging.guest_events "
            f"ALTER COLUMN {col} DROP MASK"
        )
        print(f"[INFO] Dropped mask on staging.guest_events.{col}")
    except Exception as e:
        print(f"[WARN] Drop mask on guest_events.{col} skipped: {e}")

for col in ["email", "phone"]:
    try:
        spark.sql(
            f"ALTER TABLE {catalog_name}.{schema_prefix}silver.guest_profile "
            f"ALTER COLUMN {col} DROP MASK"
        )
        print(f"[INFO] Dropped mask on silver.guest_profile.{col}")
    except Exception as e:
        print(f"[WARN] Drop mask on guest_profile.{col} skipped: {e}")

# COMMAND ----------
# Step 0d: Delete Lakehouse Monitors — non-fatal
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.errors import NotFound
    w = WorkspaceClient()
    for table in ["order_events", "inventory_events", "loyalty_events"]:
        full_name = f"{catalog_name}.{schema_prefix}staging.{table}"
        try:
            w.quality_monitors.delete(table_name=full_name)
            print(f"[INFO] Monitor deleted: {full_name}")
        except NotFound:
            print(f"[INFO] Monitor not found (ok): {full_name}")
        except Exception as e:
            print(f"[WARN] Monitor delete skipped for {full_name}: {e}")
    guest_order_monitor = f"{catalog_name}.{schema_prefix}silver.guest_order"
    try:
        w.quality_monitors.delete(table_name=guest_order_monitor)
        print(f"[INFO] Monitor deleted: {guest_order_monitor}")
    except NotFound:
        print(f"[INFO] Monitor not found (ok): {guest_order_monitor}")
    except Exception as e:
        print(f"[WARN] Monitor delete skipped for {guest_order_monitor}: {e}")
except Exception as e:
    print(f"[WARN] Monitor cleanup step skipped entirely: {e}")

# COMMAND ----------
# Step 0e: Drop ABAC policies BEFORE dropping mask functions they reference.
# DROP POLICY has no IF EXISTS guard — use SHOW POLICIES to check existence first.
_ABAC_POLICIES = ["mask_email_policy", "mask_phone_policy"]
try:
    _existing = {
        row["Policy Name"]
        for row in spark.sql(f"SHOW POLICIES ON CATALOG {catalog_name}").collect()
    }
    for _policy_name in _ABAC_POLICIES:
        if _policy_name in _existing:
            spark.sql(f"DROP POLICY {_policy_name} ON CATALOG {catalog_name}")
            print(f"[INFO] Dropped ABAC policy: {_policy_name}")
        else:
            print(f"[INFO] ABAC policy {_policy_name} not found (ok)")
except Exception as e:
    print(f"[WARN] ABAC policy cleanup skipped: {e}")

# COMMAND ----------
# Step 0f: Delete demo workspace groups — non-fatal
DEMO_GROUPS = ["franchisee_1", "franchisee_2", "region_1"]
try:
    from databricks.sdk import WorkspaceClient
    _wc = WorkspaceClient()
    for _group_name in DEMO_GROUPS:
        _existing = list(_wc.groups.list(filter=f"displayName eq '{_group_name}'", attributes="id,displayName"))
        if _existing:
            _wc.groups.delete(id=_existing[0].id)
            print(f"[INFO] Deleted demo group: {_group_name}")
        else:
            print(f"[INFO] Demo group not found (ok): {_group_name}")
except Exception as e:
    print(f"[WARN] Demo group cleanup skipped: {e}")

# COMMAND ----------
# Step 0g: Tear down Genie layer — BU domains + governed tags, the 11 spaces, and the
# synth_genie functions/metric views. Runs BEFORE the metrics/ref schema drops (Steps 1-4)
# so spaces/domains are removed before the views/functions/tables they reference. Non-fatal.
try:
    import requests
    from databricks.sdk import WorkspaceClient
    from genie_domains import _domains
    from genie_domains._spaces import DOMAINS

    _wc = WorkspaceClient()
    _workspace_url = _wc.config.host.rstrip("/")
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    _token = _ctx.apiToken().get()
    _headers = {"Authorization": f"Bearer {_token}", "Content-Type": "application/json"}

    def _get(path):
        r = requests.get(f"{_workspace_url}{path}", headers=_headers, timeout=30)
        return r.json() if r.status_code == 200 else {}

    def _delete(path):
        r = requests.delete(f"{_workspace_url}{path}", headers=_headers, timeout=30)
        return r.status_code in (200, 204)

    def _sql(stmt, best_effort=False):
        try:
            spark.sql(stmt)
        except Exception as _e:
            if best_effort:
                print(f"[WARN] sql skipped: {stmt[:70]} -> {str(_e)[:120]}")
            else:
                raise

    # List ALL spaces (paginated — the endpoint caps per page) and map our titles -> ids.
    def _all_spaces():
        out, tok = [], None
        for _ in range(200):
            _p = "/api/2.0/genie/spaces?page_size=100" + (f"&page_token={tok}" if tok else "")
            _j = _get(_p)
            out += _j.get("spaces", [])
            tok = _j.get("next_page_token")
            if not tok:
                break
        return out

    _title_to_key = {d["title"]: k for k, d in DOMAINS.items()}
    _spaces_map = {}
    for _s in _all_spaces():
        _k = _title_to_key.get(_s.get("title"))
        if _k:
            _spaces_map[_k] = {"space_id": _s["space_id"]}

    # 1. Delete BU domain cards + governed tags + space tag-assignments (children first).
    _domains.teardown(_get, _delete, _sql, drop_tags=True, spaces=_spaces_map)
    print("[INFO] BU domains + governed tags + space tag-assignments torn down")

    # 2. Trash all our spaces by title (handles any timestamp-suffixed duplicates too).
    _titles = set(_title_to_key)
    for _s in _all_spaces():
        _t = _s.get("title", "")
        if _t in _titles or any(_t.startswith(x + " ") for x in _titles):
            if _delete(f"/api/2.0/genie/spaces/{_s['space_id']}"):
                print(f"[INFO] Trashed space: {_t}")
            else:
                print(f"[WARN] Space delete failed: {_t}")

    # 3. Drop the synth_genie schema (functions + metric views) — CASCADE.
    _sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_prefix}genie CASCADE", best_effort=True)
    print(f"[INFO] Dropped schema: {catalog_name}.{schema_prefix}genie")
except Exception as e:
    print(f"[WARN] Genie layer cleanup skipped: {e}")

# COMMAND ----------
# Step 0b: Drop UC functions (governance pack)
FUNCTIONS = ["mask_email", "mask_phone", "tier_to_multiplier", "filter_by_franchisee"]
for fn in FUNCTIONS:
    try:
        spark.sql(f"DROP FUNCTION IF EXISTS {catalog_name}.{schema_prefix}ref.{fn}")
        print(f"[INFO] Dropped function: {catalog_name}.{schema_prefix}ref.{fn}")
    except Exception as e:
        print(f"[WARN] Drop function {fn} skipped: {e}")

# COMMAND ----------
# Step 0c: Drop UC volume (governance pack)
try:
    spark.sql(f"DROP VOLUME IF EXISTS {catalog_name}.{schema_prefix}ref.assets")
    print(f"[INFO] Dropped volume: {catalog_name}.{schema_prefix}ref.assets")
except Exception as e:
    print(f"[WARN] Drop volume assets skipped: {e}")

# COMMAND ----------
# Step 0h: Tear down feature store + recommender (billable endpoints first).
features_schema = f"{schema_prefix}features"
ffq = lambda t: f"{catalog_name}.{features_schema}.{t}"  # noqa: E731
_fs_endpoint = f"{schema_prefix}qsr-customer-features"
_model_endpoint = f"{schema_prefix}qsr-recommender"
_model_name = ffq("qsr_recommender")

try:
    from databricks.sdk import WorkspaceClient as _WSC
    w = _WSC()
except Exception as _e:
    print(f"[WARN] WorkspaceClient init for 0h skipped: {_e}")
    w = None

# 0h-1: serving endpoints (billable) — delete via raw REST. The SDK serving_endpoints
# wrapper is unreliable in the serverless job env (it broke create/update); raw REST
# matches the working CLI delete and avoids leaking billable endpoints on teardown.
for _ep in [_model_endpoint, _fs_endpoint, f"{schema_prefix}qsr-commerce-agent", f"{schema_prefix}qsr-agent-llm"]:
    try:
        w.api_client.do("DELETE", f"/api/2.0/serving-endpoints/{_ep}")
        print(f"[INFO] deleted serving endpoint {_ep}")
    except Exception as e:
        print(f"[WARN] delete serving endpoint {_ep} skipped: {repr(e)}")

# 0h-1b: commerce-agent observability teardown — trace PATs, trace experiment, trace
# secret, and the inference-table payload table created by the agent endpoint's
# auto_capture_config. Best-effort; each guarded independently.
if w is not None:
    _TRACE_TOKEN_COMMENT = "synth_qsr-commerce-agent trace logging"
    try:
        for _pat in w.tokens.list():
            if (_pat.comment or "") == _TRACE_TOKEN_COMMENT:
                w.tokens.delete(token_id=_pat.token_id)
                print(f"[INFO] revoked trace PAT {_pat.token_id}")
    except Exception as e:
        print(f"[WARN] revoke trace PATs skipped: {repr(e)}")
    try:
        import mlflow as _mlflow
        _exp = _mlflow.get_experiment_by_name("/Shared/qsr-commerce-agent-traces")
        if _exp:
            _mlflow.delete_experiment(_exp.experiment_id)
            print("[INFO] deleted trace experiment /Shared/qsr-commerce-agent-traces")
    except Exception as e:
        print(f"[WARN] delete trace experiment skipped: {repr(e)}")
    try:
        w.secrets.delete_secret(scope="qsr-synth", key="commerce_agent_trace_pat")
        print("[INFO] deleted trace secret qsr-synth/commerce_agent_trace_pat")
    except Exception as e:
        print(f"[WARN] delete trace secret skipped: {repr(e)}")
try:
    spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_prefix}silver.commerce_agent_payload_payload")
    print("[INFO] dropped inference table commerce_agent_payload_payload")
except Exception as e:
    print(f"[WARN] drop inference table skipped: {repr(e)}")

# 0h-2: feature spec
try:
    from databricks.feature_engineering import FeatureEngineeringClient
    _fe = FeatureEngineeringClient()
    _fe.delete_feature_spec(name=ffq("customer_store_spec"))
    print("[INFO] deleted feature spec")
except Exception as e:
    print(f"[WARN] delete feature spec skipped: {e}")

# 0h-3: online feature store (Lakebase, billable) — delete the store, which removes the
# published online tables with it.
#
# NOTE (repeatability): published online tables (customer_features_online,
# store_features_online) CANNOT be dropped individually — fe.drop_online_table raises
# ValueError("Dropping Databricks online tables is not supported"), and they are not UC
# tables (they live inside the Lakebase store, so DROP TABLE is a no-op). Deleting the
# online store is the ONLY supported way to remove them. This teardown is what makes the
# next deploy re-runnable: if the store survives, build_feature_tables.publish_table hits
# AlreadyExists on the *_online destinations. This step REQUIRES databricks-feature-
# engineering in the destroy_job env — without it the import below raises ImportError,
# gets swallowed, and the billable store leaks (breaking the next deploy).
_online_store_name = f"{schema_prefix.replace('_', '-')}qsr-online-store"
try:
    from databricks.feature_engineering import FeatureEngineeringClient
    _fe = FeatureEngineeringClient()
    _os = _fe.get_online_store(name=_online_store_name)
    if _os is not None:
        # delete_online_store drops the store AND its published online tables in one call.
        _fe.delete_online_store(name=_online_store_name)
        print(f"[INFO] deleted online store {_online_store_name} (removes published online tables)")
    else:
        print(f"[INFO] online store {_online_store_name} not found; nothing to delete")
except Exception as e:
    print(f"[WARN] online store teardown skipped: {e}")

# 0h-4: registered model (all versions)
try:
    w.registered_models.delete(full_name=_model_name)
    print(f"[INFO] deleted registered model {_model_name}")
except Exception as e:
    print(f"[WARN] delete registered model {_model_name} skipped: {e}")

# 0h-5: feature tables + schema
for _t in ["customer_features", "store_features"]:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {ffq(_t)}")
        print(f"[INFO] dropped feature table {ffq(_t)}")
    except Exception as e:
        print(f"[WARN] drop feature table {ffq(_t)} skipped: {e}")
try:
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{features_schema} CASCADE")
    print(f"[INFO] dropped schema {catalog_name}.{features_schema}")
except Exception as e:
    print(f"[WARN] drop features schema skipped: {e}")

# COMMAND ----------
# Step 1: Drop UC Metric Views
METRIC_VIEWS = [
    "order_performance",
    "loyalty_performance",
    "inventory_waste",
    "staff_hours",
]

for view_name in METRIC_VIEWS:
    spark.sql(f"DROP VIEW IF EXISTS {catalog_name}.{schema_prefix}metrics.{view_name}")
    print(f"[INFO] Dropped view: {catalog_name}.{schema_prefix}metrics.{view_name}")

# COMMAND ----------
# Step 2: Drop metrics schema
spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_prefix}metrics CASCADE")
print(f"[INFO] Dropped schema: {catalog_name}.{schema_prefix}metrics")

# COMMAND ----------
# Step 3: Drop reference tables
REF_TABLES = [
    "unit",
    "franchisee",
    "financial_period",
    "supplier",
    "menu_item",
    "recipe_ingredient",
    "weather_conditions",
    "local_events",
]

for table in REF_TABLES:
    spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{schema_prefix}ref.{table}")
    print(f"[INFO] Dropped table: {catalog_name}.{schema_prefix}ref.{table}")

# COMMAND ----------
# Step 4: Drop ref schema
spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema_prefix}ref CASCADE")
print(f"[INFO] Dropped schema: {catalog_name}.{schema_prefix}ref")

# COMMAND ----------
# Note: staging schema is intentionally preserved so historical data survives destroy/redeploy cycles.
# Gold/Silver schemas are managed by the DLT pipeline and dropped via `databricks bundle destroy`.

# COMMAND ----------
# Step 7: Tear down ontos ontological layer (best-effort)
try:
    import sys
    from pathlib import Path
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    _bundle_root = "/Workspace" + "/".join(_nb_path.replace("/Workspace","").split("/")[:-3])
    if _bundle_root not in sys.path:
        sys.path.insert(0, _bundle_root)

    from src.setup.ontos_client import OntosClient

    try:
        ontos_app_url = dbutils.widgets.get("ontos_app_url")
    except Exception:
        ontos_app_url = "https://ontos-7405605519549535.15.azure.databricksapps.com"

    try:
        ontos_enabled = dbutils.widgets.get("ontos_enabled").lower() != "false"
    except Exception:
        ontos_enabled = True

    if not ontos_enabled:
        print("[INFO] ontos_enabled=false — skipping ontos teardown")
    else:
        _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        c = OntosClient(ontos_app_url, _ctx.apiToken().get())

        # Delete in reverse dependency order: semantic links first, then
        # products, contracts, assets, teams, domains.
        print("[INFO] Deleting QSR semantic links...")
        _links_path = Path(_bundle_root) / "conf" / "ontos" / "semantic_links.yaml"
        if _links_path.exists():
            import yaml as _yaml
            _links = _yaml.safe_load(_links_path.read_text()).get("semantic_links", [])
            deleted_links = 0
            for _link in _links:
                existing = c.get_semantic_links_for_entity("uc_column", _link["entity_id"])
                for _lnk in existing:
                    if c.delete_semantic_link(_lnk["id"]):
                        deleted_links += 1
            print(f"  [OK] deleted {deleted_links} semantic links")
        else:
            print(f"  [WARN] semantic_links.yaml not found at {_links_path} — skipping link deletion")

        print("[INFO] Deleting QSR data products...")
        for pid in [
            "becd3d6c-a31d-4ba1-b0f0-69a18eff8afd",  # Demand Risk Forecast
            "f1507790-7355-4c52-a622-35e8f970cc9c",  # Guest 360
            "530590a1-18ad-447d-80d2-53ada47adfe6",  # Loyalty Performance
            "149a91fd-c6c6-411c-a72b-350ef570b692",  # Inventory Operations
            "0e435fbf-cc99-4cf9-9ed5-8a305e696d9a",  # SOS Compliance
            "7d6cb0ac-25fe-49dc-9d18-d29a603949b0",  # Order Performance
        ]:
            ok = c._delete(f"/api/data-products/{pid}")
            print(f"  [{'OK' if ok else 'WARN'}] deleted product {pid[:8]}")

        print("[INFO] Deleting QSR contracts...")
        for cid in [
            "d6914d0b-d89f-4db1-8050-693f59b03745",
            "b44aa3a9-0f43-4eda-85ef-04d3272d38e3",
            "49af13fb-5c8c-45df-81ba-afa809003dfc",
            "8b1699c5-8f57-41e6-bee5-07507164aa39",
            "3c2ed7a1-aa99-4bcf-9959-3f4d1db787d5",
            "991cb105-c17a-47d3-a79a-03b4c9ff1e9d",
            "94c03d69-0314-4c22-8911-9b92aaf9905e",
        ]:
            ok = c._delete(f"/api/data-contracts/{cid}")
            print(f"  [{'OK' if ok else 'WARN'}] deleted contract {cid[:8]}")

        print("[INFO] Deleting QSR assets...")
        assets = c.get_assets(limit=200)
        qsr_assets = [a for a in assets if (a.get("location") or "").startswith(catalog_name)]
        for a in qsr_assets:
            c._delete(f"/api/assets/{a['id']}")
        print(f"  [OK] deleted {len(qsr_assets)} QSR assets")

        print("[INFO] Deleting QSR teams...")
        for tid in [
            "31cd71e0-7f54-4b99-9562-c27b129d08c1",  # QSR Analytics
            "07309281-1f83-4045-a749-e3cb5d87bb13",  # Restaurant Ops Data
        ]:
            c._delete(f"/api/teams/{tid}")
            print(f"  [OK] deleted team {tid[:8]}")

        print("[INFO] Deleting QSR domains (leaves first)...")
        leaf_to_root = [
            "60c9ad4d-befb-4549-a29d-74f91264dbbf",  # Order Management
            "85af43b5-1b21-4e54-a4f8-bc29a74268f7",  # Inventory
            "983a2f31-fc99-408b-9250-68e0eab8317f",  # Guest Experience
            "4223a7ed-3792-4015-b41f-884ccffa052f",  # Loyalty
            "9bc5397b-d633-475e-befd-cf0595e7b2e8",  # Workforce
            "0f5a9ce8-c0a8-4e8d-9395-54abbb0c7890",  # Restaurant Reference
            "bce049ad-f33d-4e38-ad89-de1f3a95df55",  # External Signals
            "8cd4c424-87e5-4d48-91ec-67827af3c9e9",  # QSR Operations (root — last)
        ]
        for did in leaf_to_root:
            ok = c._delete(f"/api/data-domains/{did}")
            print(f"  [{'OK' if ok else 'WARN'}] deleted domain {did[:8]}")

        print("[OK] ontos teardown complete")
except Exception as e:
    print(f"[WARN] ontos teardown failed (non-fatal): {e}")
print(f"[INFO] Destroy complete. {schema_prefix}staging schema preserved. Run `databricks bundle destroy` to remove DAB-managed resources.")
