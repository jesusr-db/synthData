# Databricks notebook source
# COMMAND ----------
import sys

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_notebook_path.replace("/Workspace", "").split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

try:
    catalog_name = dbutils.widgets.get("catalog_name")
except Exception:
    catalog_name = "jmrdemo"

try:
    schema_prefix = dbutils.widgets.get("schema_prefix")
except Exception:
    schema_prefix = "synth_"

print(f"[INFO] apply_governance: catalog={catalog_name}, schema_prefix={schema_prefix}")
c = catalog_name
p = schema_prefix

# COMMAND ----------
# Step 1: Create volume + sample files
spark.sql(f"CREATE VOLUME IF NOT EXISTS {c}.{p}ref.assets")
print(f"[OK] volume {c}.{p}ref.assets")

volume_path = f"/Volumes/{c}/{p}ref/assets"

# menu_catalog.csv — exported from ref.menu_item
try:
    menu_df = spark.read.table(f"{c}.{p}ref.menu_item")
    (
        menu_df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(f"{volume_path}/menu_catalog_csv")
    )
    print(f"[OK] menu_catalog.csv written to {volume_path}/menu_catalog_csv")
except Exception as e:
    print(f"[WARN] menu_catalog export skipped: {e}")

# franchise_locations.csv — exported from ref.unit (selected columns)
try:
    unit_df = spark.read.table(f"{c}.{p}ref.unit").select(
        "unit_id", "unit_name", "city", "state", "franchisee_id", "region_id"
    )
    (
        unit_df.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(f"{volume_path}/franchise_locations_csv")
    )
    print(f"[OK] franchise_locations.csv written to {volume_path}/franchise_locations_csv")
except Exception as e:
    print(f"[WARN] franchise_locations export skipped: {e}")

# sample_receipt.json — hardcoded representative receipt
try:
    import json
    sample_receipt = {
        "receipt_id": "RCP-000001",
        "unit_id": 1001,
        "unit_name": "QSR Downtown",
        "channel": "carryout",
        "placed_at": "2026-05-20T12:34:56Z",
        "items": [
            {"menu_item_id": 101, "name": "Large Pepperoni Pizza", "quantity": 1, "unit_price": 14.99},
            {"menu_item_id": 202, "name": "Garlic Knots", "quantity": 1, "unit_price": 5.49},
            {"menu_item_id": 303, "name": "Soda 2L", "quantity": 1, "unit_price": 3.49}
        ],
        "subtotal": 23.97,
        "discount_amount": 2.00,
        "tax_amount": 1.92,
        "total_amount": 23.89,
        "tender_type": "credit_card"
    }
    # Write JSON to the volume via dbutils.fs
    dbutils.fs.put(
        f"{volume_path}/sample_receipt.json",
        json.dumps(sample_receipt, indent=2),
        overwrite=True
    )
    print(f"[OK] sample_receipt.json written to {volume_path}/sample_receipt.json")
except Exception as e:
    print(f"[WARN] sample_receipt write skipped: {e}")

# COMMAND ----------
# Step 2: Table + column descriptions
TABLE_COMMENTS = {
    f"{c}.{p}silver.guest_order": "Completed and in-flight customer orders placed at QSR units.",
    f"{c}.{p}silver.order_item": "Line items attached to guest orders (one row per menu item).",
    f"{c}.{p}silver.payment": "Payment transactions linked to guest orders.",
    f"{c}.{p}silver.status_event": "Timestamped status transitions for guest orders.",
    f"{c}.{p}silver.delivery_order": "Delivery metadata for orders fulfilled via own or 3PD delivery.",
    f"{c}.{p}silver.guest_profile": "Customer profile record at loyalty enrollment or first order.",
    f"{c}.{p}silver.digital_account": "Digital account/app credentials linked to a guest profile.",
    f"{c}.{p}silver.loyalty_transaction": "Loyalty program earn and redeem points ledger.",
    f"{c}.{p}silver.reward_redemption": "Loyalty reward redemptions applied to orders.",
    f"{c}.{p}silver.waste_log": "Recorded inventory waste events by SKU, unit, and category.",
    f"{c}.{p}silver.on_hand_balance": "Daily snapshot of on-hand inventory quantity and value.",
    f"{c}.{p}silver.receiving_order": "Inbound inventory receiving records from suppliers.",
    f"{c}.{p}silver.replenishment_order": "System-generated replenishment orders below par level.",
    f"{c}.{p}silver.shift": "Scheduled employee shifts at units, with planned start/end times.",
    f"{c}.{p}silver.time_punch": "Actual clock-in/clock-out records for employees within a shift.",
    f"{c}.{p}staging.guest_events": "Raw guest profile events streamed from generator.",
    f"{c}.{p}staging.order_events": "Raw order events (orders, items, payments, status) streamed from generator.",
    f"{c}.{p}staging.inventory_events": "Raw inventory events (waste, receiving, replenishment) streamed from generator.",
    f"{c}.{p}staging.loyalty_events": "Raw loyalty events streamed from generator.",
    f"{c}.{p}staging.workforce_events": "Raw workforce events (shifts, punches) streamed from generator.",
    f"{c}.{p}ref.unit": "Restaurant units (locations), franchisee ownership, and address.",
    f"{c}.{p}ref.franchisee": "Franchisee owners of restaurant units.",
    f"{c}.{p}ref.financial_period": "Financial periods (week/month/year) for reporting.",
    f"{c}.{p}ref.supplier": "Suppliers of inventory items.",
    f"{c}.{p}ref.menu_item": "Menu items sold at units with pricing and category.",
    f"{c}.{p}ref.recipe_ingredient": "Recipe ingredients mapping menu items to SKUs.",
    f"{c}.{p}ref.weather_conditions": "Weather conditions reference for unit-day combinations.",
    f"{c}.{p}ref.local_events": "Local events that may affect unit traffic (concerts, holidays).",
}

for table, comment in TABLE_COMMENTS.items():
    try:
        # escape single quotes in comments
        safe_comment = comment.replace("'", "''")
        spark.sql(f"COMMENT ON TABLE {table} IS '{safe_comment}'")
        print(f"[OK] comment on {table}")
    except Exception as e:
        print(f"[WARN] comment on {table} skipped: {e}")

# Column descriptions — PII, financial, supply-chain, key dimensions
COLUMN_COMMENTS = [
    # PII columns on guest_profile + guest_events
    (f"{c}.{p}silver.guest_profile", "first_name", "Customer first name (PII)."),
    (f"{c}.{p}silver.guest_profile", "last_name", "Customer last name (PII)."),
    (f"{c}.{p}silver.guest_profile", "email", "Customer email address (PII, masked for non-admin)."),
    (f"{c}.{p}silver.guest_profile", "phone", "Customer phone number (PII, masked for non-admin)."),
    (f"{c}.{p}silver.guest_profile", "zip_code", "Customer postal code (PII)."),
    (f"{c}.{p}staging.guest_events", "first_name", "Customer first name (PII)."),
    (f"{c}.{p}staging.guest_events", "last_name", "Customer last name (PII)."),
    (f"{c}.{p}staging.guest_events", "email", "Customer email address (PII, masked for non-admin)."),
    (f"{c}.{p}staging.guest_events", "phone", "Customer phone number (PII, masked for non-admin)."),
    (f"{c}.{p}staging.guest_events", "zip_code", "Customer postal code (PII)."),
    # Financial columns
    (f"{c}.{p}silver.guest_order", "subtotal", "Pre-discount, pre-tax order subtotal (USD)."),
    (f"{c}.{p}silver.guest_order", "discount_amount", "Dollar value of promotions/coupons applied (USD)."),
    (f"{c}.{p}silver.guest_order", "tax_amount", "Tax charged on the order (USD)."),
    (f"{c}.{p}silver.guest_order", "total_amount", "Total order revenue including items, taxes, fees (USD)."),
    (f"{c}.{p}silver.waste_log", "waste_cost", "Dollar cost of wasted inventory (USD)."),
    # Supply-chain columns
    (f"{c}.{p}silver.waste_log", "stock_sku", "Inventory SKU of the wasted item."),
    (f"{c}.{p}silver.on_hand_balance", "stock_sku", "Inventory SKU tracked at this unit."),
    (f"{c}.{p}silver.waste_log", "waste_quantity", "Quantity of inventory wasted (units of measure)."),
    (f"{c}.{p}ref.supplier", "supplier_id", "Surrogate key for the inventory supplier."),
    # Key dimensions
    (f"{c}.{p}silver.guest_order", "unit_id", "Restaurant unit where the order was placed (FK ref.unit)."),
    (f"{c}.{p}silver.guest_order", "channel", "Order channel: carryout, own_delivery, 3pd_delivery, catering."),
    (f"{c}.{p}silver.guest_order", "order_type", "Order type: dine_in, takeout, delivery."),
    (f"{c}.{p}silver.loyalty_transaction", "tier", "Loyalty tier at transaction time: bronze, silver, gold, elite."),
    (f"{c}.{p}silver.loyalty_transaction", "transaction_type", "earn or redeem."),
]

for table, column, comment in COLUMN_COMMENTS:
    try:
        safe_comment = comment.replace("'", "''")
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {column} COMMENT '{safe_comment}'")
        print(f"[OK] comment {table}.{column}")
    except Exception as e:
        print(f"[WARN] comment {table}.{column} skipped: {e}")

# COMMAND ----------
# Step 3: Column tags — class.* for PII (feeds ABAC policy); financial and supply_chain unchanged
COLUMN_TAGS = [
    # PII — class.* namespace (data classification standard tags)
    (f"{c}.{p}staging.guest_events", "email",      "class.email_address", ""),
    (f"{c}.{p}staging.guest_events", "phone",       "class.phone_number",  ""),
    (f"{c}.{p}staging.guest_events", "first_name",  "class.name",          ""),
    (f"{c}.{p}staging.guest_events", "last_name",   "class.name",          ""),
    (f"{c}.{p}staging.guest_events", "zip_code",    "class.zip_code",      ""),
    # email/phone intentionally omitted — per-table SET MASK used instead (ABAC not supported on DLT-owned tables)
    (f"{c}.{p}silver.guest_profile", "first_name",  "class.name",          ""),
    (f"{c}.{p}silver.guest_profile", "last_name",   "class.name",          ""),
    (f"{c}.{p}silver.guest_profile", "zip_code",    "class.zip_code",      ""),
    # Financial
    (f"{c}.{p}silver.guest_order",   "subtotal",         "financial", "true"),
    (f"{c}.{p}silver.guest_order",   "discount_amount",  "financial", "true"),
    (f"{c}.{p}silver.guest_order",   "tax_amount",       "financial", "true"),
    (f"{c}.{p}silver.guest_order",   "total_amount",     "financial", "true"),
    (f"{c}.{p}silver.waste_log",     "waste_cost",       "financial", "true"),
    # Supply chain
    (f"{c}.{p}silver.waste_log",       "stock_sku",   "supply_chain", "true"),
    (f"{c}.{p}silver.on_hand_balance", "stock_sku",   "supply_chain", "true"),
    (f"{c}.{p}ref.supplier",           "supplier_id", "supply_chain", "true"),
]

for table, column, tag, value in COLUMN_TAGS:
    try:
        spark.sql(f"ALTER TABLE {table} ALTER COLUMN {column} SET TAGS ('{tag}' = '{value}')")
        print(f"[OK] tag {table}.{column} {tag}={value}")
    except Exception as e:
        print(f"[WARN] tag {table}.{column} skipped: {e}")

# COMMAND ----------
# Step 4: UC scalar functions in ref schema
spark.sql(f"""
CREATE OR REPLACE FUNCTION {c}.{p}ref.mask_email(email STRING)
RETURNS STRING
RETURN CASE
  WHEN email IS NULL THEN NULL
  WHEN INSTR(email, '@') > 1 THEN CONCAT(LEFT(email, 1), REPEAT('*', INSTR(email,'@')-2), SUBSTR(email, INSTR(email,'@')))
  ELSE '***'
END
""")
print(f"[OK] function {c}.{p}ref.mask_email")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {c}.{p}ref.mask_phone(phone STRING)
RETURNS STRING
RETURN CASE
  WHEN phone IS NULL THEN NULL
  ELSE CONCAT(REPEAT('*', GREATEST(0, LENGTH(REGEXP_REPLACE(phone,'[^0-9]','')) - 4)), RIGHT(REGEXP_REPLACE(phone,'[^0-9]',''), 4))
END
""")
print(f"[OK] function {c}.{p}ref.mask_phone")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {c}.{p}ref.tier_to_multiplier(tier STRING)
RETURNS DOUBLE
RETURN CASE tier
  WHEN 'bronze' THEN 1.0
  WHEN 'silver' THEN 1.5
  WHEN 'gold'   THEN 2.0
  WHEN 'elite'  THEN 3.0
  ELSE 1.0
END
""")
print(f"[OK] function {c}.{p}ref.tier_to_multiplier")

# COMMAND ----------
# Step 5: ABAC column mask policies — catalog-level, tag-driven, scoped to staging only.
# silver.guest_profile uses per-table SET MASK (Step 5b) because ABAC_POLICIES_NOT_SUPPORTED
# fires on every DLT update (not just full_refresh) when a catalog ABAC policy matches a
# DLT-owned table. Removing class.email_address/phone_number from silver.guest_profile
# prevents the ABAC policy from matching it.

# Best-effort: drop any legacy per-table masks on staging tables to avoid double-masking.
for _table, _col in [
    (f"{c}.{p}staging.guest_events", "email"),
    (f"{c}.{p}staging.guest_events", "phone"),
]:
    try:
        spark.sql(f"ALTER TABLE {_table} ALTER COLUMN {_col} DROP MASK")
        print(f"[INFO] Dropped legacy per-table mask: {_table}.{_col}")
    except Exception as e:
        print(f"[WARN] Could not drop legacy mask {_table}.{_col}: {e}")

# ABAC policies — idempotent: SHOW POLICIES → drop if exists → create
ABAC_POLICIES = [
    ("mask_email_policy", f"{c}.{p}ref.mask_email", "class.email_address"),
    ("mask_phone_policy", f"{c}.{p}ref.mask_phone", "class.phone_number"),
]

try:
    _existing_policies = {
        row["Policy Name"]
        for row in spark.sql(f"SHOW POLICIES ON CATALOG {c}").collect()
    }
except Exception as e:
    print(f"[WARN] SHOW POLICIES failed, assuming empty: {e}")
    _existing_policies = set()

for policy_name, mask_fn, tag_name in ABAC_POLICIES:
    try:
        if policy_name in _existing_policies:
            spark.sql(f"DROP POLICY {policy_name} ON CATALOG {c}")
            print(f"[INFO] Dropped existing ABAC policy: {policy_name}")
        spark.sql(f"""
            CREATE POLICY {policy_name}
              ON CATALOG {c}
              COLUMN MASK {mask_fn}
              TO `account users`
              FOR TABLES
                MATCH COLUMNS (has_tag('{tag_name}')) AS m
              ON COLUMN m
        """)
        print(f"[OK] ABAC policy {policy_name}: {mask_fn} for columns with tag '{tag_name}'")
    except Exception as e:
        print(f"[WARN] ABAC policy {policy_name} skipped: {e}")

# COMMAND ----------
# Step 5b: Per-table SET MASK on silver.guest_profile — applied after full_refresh resets the table.
# DLT does not support catalog ABAC on tables it owns, so masking is applied here instead.
# Regular DLT updates preserve SET MASK; full_refresh resets it, but apply_governance always
# runs after start_pipeline in the setup job DAG so this fires after every full_refresh.
for _col, _fn in [
    ("email", f"{c}.{p}ref.mask_email"),
    ("phone", f"{c}.{p}ref.mask_phone"),
]:
    try:
        spark.sql(f"ALTER TABLE {c}.{p}silver.guest_profile ALTER COLUMN {_col} SET MASK {_fn}")
        print(f"[OK] per-table mask on silver.guest_profile.{_col}")
    except Exception as e:
        print(f"[WARN] per-table mask silver.guest_profile.{_col} skipped: {e}")

# COMMAND ----------
# Step 6: Row filter function + attach
spark.sql(f"""
CREATE OR REPLACE FUNCTION {c}.{p}ref.filter_by_franchisee(franchisee_id BIGINT, region_id BIGINT)
RETURNS BOOLEAN
RETURN IS_MEMBER(CONCAT('franchisee_', CAST(franchisee_id AS STRING)))
    OR IS_MEMBER(CONCAT('region_', CAST(region_id AS STRING)))
    OR IS_MEMBER('qsr_admin')
""")
print(f"[OK] function {c}.{p}ref.filter_by_franchisee")

ROW_FILTER_TABLES = [
    f"{c}.{p}silver.guest_order",
    f"{c}.{p}silver.waste_log",
    f"{c}.{p}silver.loyalty_transaction",
    f"{c}.{p}silver.guest_profile",
    f"{c}.{p}silver.time_punch",
    f"{c}.{p}ref.unit",
]

for table in ROW_FILTER_TABLES:
    try:
        spark.sql(f"ALTER TABLE {table} SET ROW FILTER {c}.{p}ref.filter_by_franchisee ON (franchisee_id, region_id)")
        print(f"[OK] row filter on {table}")
    except Exception as e:
        print(f"[WARN] row filter on {table} skipped: {e}")

# COMMAND ----------
# Data classification is handled by Lakehouse Monitors (configure_monitoring.py).
# Each monitor refresh runs MonitorDataClassificationConfig(enabled=True), which writes
# class.* tags automatically. The class.* tags applied in Step 3 above serve as the
# deterministic fallback so classification is populated before the first monitor refresh.
print("[INFO] Data classification driven by monitors — see configure_monitoring task")

# COMMAND ----------
print("[INFO] apply_governance complete — volume, comments, class.* tags, functions, ABAC policies, row filters applied")
