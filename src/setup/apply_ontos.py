# Databricks notebook source
# COMMAND ----------
import sys
from pathlib import Path

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_nb_path.replace("/Workspace", "").split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

# COMMAND ----------
try:
    catalog_name  = dbutils.widgets.get("catalog_name")
except Exception:
    catalog_name  = "jmrdemo"

try:
    schema_prefix = dbutils.widgets.get("schema_prefix")
except Exception:
    schema_prefix = "synth_"

try:
    ontos_app_url = dbutils.widgets.get("ontos_app_url")
except Exception:
    ontos_app_url = "https://ontos-7405605519549535.15.azure.databricksapps.com"

try:
    ontos_enabled = dbutils.widgets.get("ontos_enabled").lower() != "false"
except Exception:
    ontos_enabled = True

print(f"[INFO] apply_ontos: catalog={catalog_name}, prefix={schema_prefix}, enabled={ontos_enabled}")

if not ontos_enabled:
    print("[INFO] ontos_enabled=false — skipping")
    dbutils.notebook.exit("skipped")

# COMMAND ----------
# Get auth token from the running cluster's service principal
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
token = w.config.token

from src.setup.ontos_client import OntosClient
c = OntosClient(base_url=ontos_app_url, token=token)

# Verify connectivity
import urllib.request
req = urllib.request.Request(
    ontos_app_url + "/api/health",
    headers={"Authorization": f"Bearer {token}"},
)
try:
    with urllib.request.urlopen(req) as resp:
        health = resp.read().decode()
        print(f"[OK] ontos health: {health}")
except Exception as e:
    print(f"[WARN] ontos unreachable: {e}. Exiting gracefully.")
    dbutils.notebook.exit("ontos_unreachable")

# COMMAND ----------
# ── Phase 2: Column-level ODCS schemas ────────────────────────────────────
# Hard-coded contract IDs from Phase 1 bootstrap (see research/ontos-qsr-ontological-layer_2026-05-26.md)

p = schema_prefix

CONTRACT_SCHEMAS = {
    "d6914d0b-d89f-4db1-8050-693f59b03745": {  # Order Management
        "schema": f"{p}silver",
        "tables": ["guest_order", "order_item", "delivery_order", "payment", "status_event"],
        "pii": set(),
    },
    "b44aa3a9-0f43-4eda-85ef-04d3272d38e3": {  # Guest Experience
        "schema": f"{p}silver",
        "tables": ["guest_profile", "digital_account"],
        "pii": {"email", "phone", "first_name", "last_name"},
    },
    "49af13fb-5c8c-45df-81ba-afa809003dfc": {  # Loyalty & Rewards
        "schema": f"{p}silver",
        "tables": ["loyalty_transaction", "loyalty_cohort_metrics", "reward_redemption"],
        "pii": set(),
    },
    "8b1699c5-8f57-41e6-bee5-07507164aa39": {  # Inventory Operations
        "schema": f"{p}silver",
        "tables": ["on_hand_balance", "receiving_order", "replenishment_order",
                   "inventory_waste_summary", "waste_log"],
        "pii": set(),
    },
    "3c2ed7a1-aa99-4bcf-9959-3f4d1db787d5": {  # Workforce Operations
        "schema": f"{p}silver",
        "tables": ["shift", "time_punch", "sos_compliance_summary", "unit_performance_daily"],
        "pii": set(),
    },
    "991cb105-c17a-47d3-a79a-03b4c9ff1e9d": {  # Restaurant Reference
        "schema": f"{p}ref",
        "tables": ["unit", "franchisee", "menu_item", "recipe_ingredient",
                   "item_price", "supplier", "financial_period"],
        "pii": set(),
    },
    "94c03d69-0314-4c22-8911-9b92aaf9905e": {  # External Signals
        "schema": f"{p}ref",
        "tables": ["weather_conditions", "local_events"],
        "pii": set(),
    },
}

print("\n── Phase 2: Seeding column schemas ──")
for contract_id, spec in CONTRACT_SCHEMAS.items():
    schema_key = spec["schema"].lstrip(schema_prefix[:-1]) if schema_prefix else spec["schema"]
    print(f"\n  Contract {contract_id[:8]}… → {spec['schema']}")
    try:
        c.seed_contract_schemas(
            contract_id=contract_id,
            catalog=catalog_name,
            schema=spec["schema"],
            tables=spec["tables"],
            pii_columns=spec["pii"],
        )
        print(f"  [OK] schemas seeded for {len(spec['tables'])} tables")
    except Exception as e:
        print(f"  [WARN] schema seeding failed: {e}")

# COMMAND ----------
# ── Phase 3: QSR Ontology Upload ──────────────────────────────────────────
import yaml

conf_root = Path(_bundle_root) / "conf" / "ontos"
ttl_path  = conf_root / "qsr-ontology.ttl"
links_path = conf_root / "semantic_links.yaml"

print("\n── Phase 3a: Uploading QSR ontology TTL ──")
if ttl_path.exists():
    ttl_bytes = ttl_path.read_bytes()
    # Check if already uploaded
    models = c._get("/api/semantic-models") or []
    existing_titles = [m.get("title", "") for m in (models if isinstance(models, list) else [])]
    if "qsr-ontology" in existing_titles:
        print("  [SKIP] qsr-ontology already uploaded")
    else:
        model_id = c.upload_ttl(ttl_bytes, "qsr-ontology")
        if model_id:
            print(f"  [OK] uploaded qsr-ontology.ttl (model_id={model_id})")
        else:
            print("  [WARN] TTL upload failed — semantic links may not resolve")
else:
    print(f"  [WARN] TTL not found at {ttl_path} — skipping")

# COMMAND ----------
# ── Phase 3b: Semantic Links ──────────────────────────────────────────────
print("\n── Phase 3b: Creating semantic links ──")
if links_path.exists():
    links_config = yaml.safe_load(links_path.read_text())
    links = links_config.get("semantic_links", [])
    ok, fail = 0, 0
    for link in links:
        entity_id = link["entity_id"]
        iri = link["iri"]
        result = c.create_semantic_link("uc_column", entity_id, iri)
        if result:
            ok += 1
        else:
            fail += 1
    print(f"  [OK] {ok} semantic links created, {fail} failed/skipped")
else:
    print(f"  [WARN] semantic_links.yaml not found at {links_path}")

print("\n[DONE] apply_ontos complete.")
