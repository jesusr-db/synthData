# Databricks notebook source
# COMMAND ----------
import sys, json, uuid, requests

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_notebook_path.replace("/Workspace", "").split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

from databricks.sdk import WorkspaceClient

try:
    catalog_name = dbutils.widgets.get("catalog_name")
except Exception:
    catalog_name = "jmrdemo"

try:
    schema_prefix = dbutils.widgets.get("schema_prefix")
except Exception:
    schema_prefix = "synth_"

w = WorkspaceClient()
workspace_url = w.config.host.rstrip("/")
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
token = ctx.apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

SPACE_TITLE = f"QSR Synthetic Data — {catalog_name}"

print(f"[INFO] create_genie_space: catalog={catalog_name}, schema_prefix={schema_prefix}, workspace={workspace_url}")

# COMMAND ----------
# Check if space already exists
resp = requests.get(f"{workspace_url}/api/2.0/genie/spaces", headers=headers, timeout=30)
if resp.status_code == 200:
    existing = [s for s in resp.json().get("spaces", []) if s.get("title") == SPACE_TITLE]
    if existing:
        space_id = existing[0]["space_id"]
        print(f"[SKIP] Genie Space '{SPACE_TITLE}' already exists (id={space_id})")
        dbutils.notebook.exit(space_id)

# COMMAND ----------
# Resolve warehouse
warehouses = list(w.warehouses.list())
warehouse = next((wh for wh in warehouses if wh.state.value in ("RUNNING", "STOPPED")), warehouses[0] if warehouses else None)
if warehouse is None:
    raise ValueError("No SQL warehouse found — create one before running setup.")
warehouse_id = warehouse.id
print(f"[INFO] Using warehouse: {warehouse.name} ({warehouse_id})")

# COMMAND ----------
def hex_id():
    return uuid.uuid4().hex  # 32-char lowercase hex

SILVER_TABLES = [
    "guest_order", "order_item", "payment", "status_event", "delivery_order",
    "on_hand_balance", "waste_log", "receiving_order", "replenishment_order",
    "guest_profile", "loyalty_transaction", "reward_redemption", "shift", "time_punch",
]
METRIC_VIEWS = [
    "order_performance", "loyalty_performance", "inventory_waste", "staff_hours",
]

SEED_QUESTIONS = [
    "Which units have the highest SOS breach rate this month?",
    "Show me loyalty tier distribution and points burn rate by tier",
    "What is the channel mix trend over the last 4 weeks by unit?",
    "Which stock SKUs have the highest waste cost per unit?",
    "Compare average order value by channel and metro area",
    "Show me the top 10 units by revenue last week",
    "What percentage of guests are loyalty members per unit?",
    "Which items are most frequently refunded?",
    "Show me staff hours worked by unit over the last 30 days",
    "How does catering AOV compare to carryout across units?",
]

INSTRUCTION_TEXT = (
    "You are analyzing synthetic QSR (Quick Service Restaurant) data modeled after a Domino's-style franchise operation. "
    "Key domain context: "
    "250 restaurant units across 20 US metro areas, 80% franchised. "
    "Channels: carryout (40%), 3pd_delivery (40%), own_delivery (16%), catering (4%). "
    "Loyalty tiers: bronze, silver, gold, platinum — higher tiers earn more points per dollar. "
    "Loyalty redemptions are burn events paired with a loyalty_transaction of type=redeem. "
    "SOS (speed of service) target: 720 sec carryout, 1800 sec delivery. "
    "Financial periods are monthly; item prices drift ±3-6% per quarter. "
    "waste_flag on order_item correlates with waste_log inventory events. "
    "account_status on guest_profile: active (normal), inactive (churned/unverified), suspended (fraud). "
    "unit_volume_bias drives order count differences; market_price_index drives AOV differences across units."
)

serialized = {
    "version": 2,
    "config": {
        "sample_questions": [
            {"id": hex_id(), "question": [q]} for q in SEED_QUESTIONS
        ]
    },
    "data_sources": {
        # API requires identifiers sorted alphabetically
        "tables": [
            {"identifier": f"{catalog_name}.{schema_prefix}silver.{t}"} for t in sorted(SILVER_TABLES)
        ],
        "metric_views": [
            {"identifier": f"{catalog_name}.{schema_prefix}metrics.{v}"} for v in sorted(METRIC_VIEWS)
        ],
    },
    "instructions": {
        "text_instructions": [
            {"id": hex_id(), "content": [INSTRUCTION_TEXT]}
        ],
        "example_question_sqls": [],
        "join_specs": [],
        "sql_snippets": {"filters": [], "expressions": [], "measures": []},
    },
    "benchmarks": {"questions": []},
}

# COMMAND ----------
payload = {
    "title": SPACE_TITLE,
    "description": "Pre-configured Genie Space for the QSR synthetic dataset (250 units, 1-month backfill + live stream).",
    "warehouse_id": warehouse_id,
    "serialized_space": json.dumps(serialized),
}

resp = requests.post(
    f"{workspace_url}/api/2.0/genie/spaces",
    headers=headers,
    json=payload,
    timeout=60,
)

if resp.status_code not in (200, 201):
    raise RuntimeError(f"Failed to create Genie Space: {resp.status_code} {resp.text}")

space = resp.json()
space_id = space.get("space_id", space.get("id", "unknown"))
print(f"[OK] Genie Space created: '{SPACE_TITLE}' (id={space_id})")
print(f"[INFO] URL: {workspace_url}/genie/spaces/{space_id}")
dbutils.notebook.exit(space_id)
