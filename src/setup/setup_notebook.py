# Databricks notebook source
# COMMAND ----------
import sys

# Add bundle root to sys.path so `src.*` imports resolve
_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_notebook_path.replace("/Workspace", "").split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

# Load params — injected as job widgets (all params live in databricks.yml variables)
try:
    catalog_name = dbutils.widgets.get("catalog_name")
    num_units = int(dbutils.widgets.get("num_units"))
except Exception:
    # Defaults for interactive execution — always use job parameters in deployment
    catalog_name = "jmrdemo"
    num_units = 250

try:
    schema_prefix = dbutils.widgets.get("schema_prefix")
except Exception:
    schema_prefix = "synth_"

print(f"[INFO] Setup: catalog={catalog_name}, schema_prefix={schema_prefix}, num_units={num_units}")

# COMMAND ----------
# Step 1: Verify catalog exists (managed externally — declared in databricks.yml)
catalogs = [r.catalog for r in spark.sql("SHOW CATALOGS").collect()]
if catalog_name not in catalogs:
    raise ValueError(f"Catalog '{catalog_name}' does not exist. Create it before running setup.")
print(f"[INFO] Catalog verified: {catalog_name}")

# COMMAND ----------
# Step 2: Create schemas
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_prefix}staging")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_prefix}ref")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_prefix}metrics")
print(f"[INFO] Schemas ready: {schema_prefix}staging, {schema_prefix}ref, {schema_prefix}metrics")

# COMMAND ----------
# Step 2.5: Recreate mask functions in synth_ref so any existing column masks on staging
# tables (applied by apply_governance in a prior run) remain valid. Without these functions
# present, any query on staging.guest_events fails with UC_DEPENDENCY_DOES_NOT_EXIST,
# breaking both backfill (MAX(event_ts)) and DLT streaming reads.
spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog_name}.{schema_prefix}ref.mask_email(email STRING)
RETURNS STRING
RETURN CASE
  WHEN email IS NULL THEN NULL
  WHEN INSTR(email, '@') > 1 THEN CONCAT(LEFT(email, 1), REPEAT('*', INSTR(email,'@')-2), SUBSTR(email, INSTR(email,'@')))
  ELSE '***'
END
""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION {catalog_name}.{schema_prefix}ref.mask_phone(phone STRING)
RETURNS STRING
RETURN CASE
  WHEN phone IS NULL THEN NULL
  ELSE CONCAT(REPEAT('*', GREATEST(0, LENGTH(REGEXP_REPLACE(phone,'[^0-9]','')) - 4)), RIGHT(REGEXP_REPLACE(phone,'[^0-9]',''), 4))
END
""")
print(f"[INFO] Mask functions ready: {catalog_name}.{schema_prefix}ref.mask_email, mask_phone")

# COMMAND ----------
# Step 3: Create staging tables with full schemas so DLT can analyze flows at startup.
# IF NOT EXISTS preserves existing data and Delta table IDs — required for DLT streaming checkpoints.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_prefix}staging.order_events (
        event_type                      STRING,
        event_id                        BIGINT,
        unit_id                         BIGINT,
        event_ts                        TIMESTAMP,
        guest_order_id                  BIGINT,
        order_item_id                   BIGINT,
        payment_id                      BIGINT,
        status_event_id                 BIGINT,
        delivery_order_id               BIGINT,
        channel                         STRING,
        order_type                      STRING,
        order_status                    STRING,
        profile_id                      BIGINT,
        member_id                       BIGINT,
        subtotal                        DOUBLE,
        discount_amount                 DOUBLE,
        tax_amount                      DOUBLE,
        total_amount                    DOUBLE,
        placed_at                       TIMESTAMP,
        ready_at                        TIMESTAMP,
        fulfilled_at                    TIMESTAMP,
        cancelled_at                    TIMESTAMP,
        financial_period_id             BIGINT,
        sos_breach                      BOOLEAN,
        menu_item_id                    BIGINT,
        quantity                        BIGINT,
        unit_price                      DOUBLE,
        line_gross_amount               DOUBLE,
        line_net_amount                 DOUBLE,
        line_discount_amount            DOUBLE,
        item_status                     STRING,
        waste_flag                      BOOLEAN,
        tender_type                     STRING,
        amount                          DOUBLE,
        settlement_date                 STRING,
        paid_at                         TIMESTAMP,
        prior_state                     STRING,
        current_state                   STRING,
        event_timestamp                 TIMESTAMP,
        elapsed_seconds_in_prior_state  BIGINT,
        sos_target_seconds              BIGINT,
        is_sos_breach                   BOOLEAN,
        platform_order_reference        STRING,
        estimated_delivery_seconds      BIGINT,
        actual_delivery_seconds         BIGINT,
        delivery_status                 STRING
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion'   = '2',
        'delta.minWriterVersion'   = '5'
    )
""")
print(f"[INFO] Staging table ready: {catalog_name}.{schema_prefix}staging.order_events")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_prefix}staging.inventory_events (
        event_type                  STRING,
        event_id                    BIGINT,
        unit_id                     BIGINT,
        event_ts                    TIMESTAMP,
        on_hand_balance_id          BIGINT,
        waste_log_id                BIGINT,
        receiving_order_id          BIGINT,
        replenishment_order_id      BIGINT,
        stock_sku                   STRING,
        quantity_on_hand            DOUBLE,
        quantity_reserved           DOUBLE,
        par_level                   DOUBLE,
        snapshot_at                 TIMESTAMP,
        waste_quantity              DOUBLE,
        waste_category              STRING,
        waste_cost                  DOUBLE,
        logged_at                   TIMESTAMP,
        received_quantity           DOUBLE,
        delivery_date               STRING,
        quality_inspection_result   STRING,
        temperature_check_pass      BOOLEAN,
        order_type                  STRING,
        order_quantity              DOUBLE,
        order_status                STRING,
        ordered_at                  TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion'   = '2',
        'delta.minWriterVersion'   = '5'
    )
""")
print(f"[INFO] Staging table ready: {catalog_name}.{schema_prefix}staging.inventory_events")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_prefix}staging.guest_events (
        event_type          STRING,
        event_id            BIGINT,
        unit_id             BIGINT,
        event_ts            TIMESTAMP,
        guest_profile_id    BIGINT,
        digital_account_id  BIGINT,
        first_name          STRING,
        last_name           STRING,
        email               STRING,
        phone               STRING,
        zip_code            STRING,
        created_date        STRING,
        account_status      STRING
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion'   = '2',
        'delta.minWriterVersion'   = '5'
    )
""")
print(f"[INFO] Staging table ready: {catalog_name}.{schema_prefix}staging.guest_events")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_prefix}staging.loyalty_events (
        event_type              STRING,
        event_id                BIGINT,
        unit_id                 BIGINT,
        event_ts                TIMESTAMP,
        loyalty_transaction_id  BIGINT,
        reward_redemption_id    BIGINT,
        member_id               BIGINT,
        guest_order_id          BIGINT,
        transaction_type        STRING,
        points_delta            BIGINT,
        transaction_at          TIMESTAMP,
        tier                    STRING,
        points_redeemed         BIGINT,
        reward_value            DOUBLE,
        redeemed_at             TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion'   = '2',
        'delta.minWriterVersion'   = '5'
    )
""")
print(f"[INFO] Staging table ready: {catalog_name}.{schema_prefix}staging.loyalty_events")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_prefix}staging.workforce_events (
        event_type      STRING,
        event_id        BIGINT,
        unit_id         BIGINT,
        event_ts        TIMESTAMP,
        shift_id        BIGINT,
        time_punch_id   BIGINT,
        employee_id     BIGINT,
        shift_label     STRING,
        shift_start     TIMESTAMP,
        shift_end       TIMESTAMP,
        status          STRING,
        date            STRING,
        punch_in        TIMESTAMP,
        punch_out       TIMESTAMP,
        hours_worked    DOUBLE
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion'   = '2',
        'delta.minWriterVersion'   = '5'
    )
""")
print(f"[INFO] Staging table ready: {catalog_name}.{schema_prefix}staging.workforce_events")

# COMMAND ----------
# Step 4: Seed reference tables
from src.generator.reference.seeder import seed_all

seed_all(spark, catalog_name, num_units=num_units, schema_prefix=schema_prefix)
print(f"[INFO] Reference tables seeded")

print("[INFO] Setup complete")
