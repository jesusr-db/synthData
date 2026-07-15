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

try:
    otel_catalog = dbutils.widgets.get("otel_catalog")
except Exception:
    otel_catalog = "jmrdemo"

try:
    otel_schema = dbutils.widgets.get("otel_schema")
except Exception:
    otel_schema = "zerobus"

print(f"[INFO] create_metric_views: catalog={catalog_name}, schema_prefix={schema_prefix}")
c = catalog_name

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {c}.{schema_prefix}metrics")

# COMMAND ----------
# 1. Order Performance — volume, revenue, SOS compliance per unit/channel
spark.sql(f"""
    CREATE OR REPLACE VIEW {c}.{schema_prefix}metrics.order_performance
    WITH METRICS LANGUAGE YAML AS$$
version: 1.1
comment: "QSR order volume, revenue, and speed-of-service compliance by unit and channel"
source: {c}.{schema_prefix}silver.guest_order
dimensions:
  - name: Unit ID
    expr: unit_id
  - name: Channel
    expr: channel
  - name: Order Type
    expr: order_type
  - name: Order Status
    expr: order_status
  - name: Order Date
    expr: CAST(placed_at AS DATE)
  - name: Order Month
    expr: DATE_TRUNC('MONTH', placed_at)
measures:
  - name: Total Orders
    expr: COUNT(1)
    comment: "Total orders placed"
  - name: Total Revenue
    expr: SUM(total_amount)
    comment: "Gross revenue across all orders"
  - name: Average Order Value
    expr: SUM(total_amount) / COUNT(1)
    comment: "Revenue per order"
  - name: Fulfilled Orders
    expr: COUNT(CASE WHEN order_status = 'fulfilled' THEN 1 END)
    comment: "Orders successfully completed"
  - name: Cancelled Orders
    expr: COUNT(CASE WHEN order_status = 'cancelled' THEN 1 END)
  - name: Total Discount
    expr: SUM(discount_amount)
    comment: "Total discount dollars applied"
  - name: SOS Breach Rate
    expr: SUM(CAST(sos_breach AS INT)) / COUNT(1)
    comment: "Fraction of orders breaching speed-of-service target"
$$
""")
print("[OK] metrics.order_performance")

# COMMAND ----------
# 2. Loyalty Performance — points activity and member engagement by tier
spark.sql(f"""
    CREATE OR REPLACE VIEW {c}.{schema_prefix}metrics.loyalty_performance
    WITH METRICS LANGUAGE YAML AS$$
version: 1.1
comment: "Loyalty program points activity and member engagement by tier and unit"
source: {c}.{schema_prefix}silver.loyalty_transaction
dimensions:
  - name: Tier
    expr: tier
  - name: Transaction Type
    expr: transaction_type
  - name: Unit ID
    expr: unit_id
  - name: Transaction Month
    expr: DATE_TRUNC('MONTH', transaction_at)
measures:
  - name: Unique Members
    expr: COUNT(DISTINCT member_id)
    comment: "Active loyalty members"
  - name: Total Transactions
    expr: COUNT(1)
  - name: Points Earned
    expr: SUM(CASE WHEN transaction_type = 'earn' THEN points_delta ELSE 0 END)
    comment: "Total loyalty points earned"
  - name: Points Redeemed
    expr: SUM(CASE WHEN transaction_type = 'redeem' THEN ABS(points_delta) ELSE 0 END)
    comment: "Total loyalty points redeemed"
$$
""")
print("[OK] metrics.loyalty_performance")

# COMMAND ----------
# 3. Inventory Waste — waste quantity and cost by unit, SKU, and category
spark.sql(f"""
    CREATE OR REPLACE VIEW {c}.{schema_prefix}metrics.inventory_waste
    WITH METRICS LANGUAGE YAML AS$$
version: 1.1
comment: "Inventory waste quantity and cost by unit, SKU, and waste category"
source: {c}.{schema_prefix}silver.waste_log
dimensions:
  - name: Unit ID
    expr: unit_id
  - name: Stock SKU
    expr: stock_sku
  - name: Waste Category
    expr: waste_category
  - name: Waste Week
    expr: DATE_TRUNC('WEEK', logged_at)
  - name: Waste Month
    expr: DATE_TRUNC('MONTH', logged_at)
measures:
  - name: Total Waste Quantity
    expr: SUM(waste_quantity)
    comment: "Total units of product wasted"
  - name: Total Waste Cost
    expr: SUM(waste_cost)
    comment: "Dollar cost of wasted inventory"
  - name: Waste Events
    expr: COUNT(1)
    comment: "Number of waste log entries"
  - name: Average Waste Cost per Event
    expr: SUM(waste_cost) / COUNT(1)
$$
""")
print("[OK] metrics.inventory_waste")

# COMMAND ----------
# 4. Staff Hours — actual hours worked and shift counts per unit
spark.sql(f"""
    CREATE OR REPLACE VIEW {c}.{schema_prefix}metrics.staff_hours
    WITH METRICS LANGUAGE YAML AS$$
version: 1.1
comment: "Actual hours worked and shift counts per unit and date"
source: {c}.{schema_prefix}silver.time_punch
dimensions:
  - name: Unit ID
    expr: unit_id
  - name: Shift Date
    expr: CAST(punch_in AS DATE)
  - name: Shift Month
    expr: DATE_TRUNC('MONTH', punch_in)
measures:
  - name: Total Hours Worked
    expr: SUM(hours_worked)
    comment: "Actual hours worked across all punches"
  - name: Total Shifts
    expr: COUNT(1)
    comment: "Number of time punches recorded"
  - name: Unique Employees
    expr: COUNT(DISTINCT employee_id)
  - name: Average Hours per Shift
    expr: SUM(hours_worked) / COUNT(1)
$$
""")
print("[OK] metrics.staff_hours")

# COMMAND ----------
# 5. Demand Risk Forecast — (unit, date) risk signal for next 14 days based on weather + events
spark.sql(f"""
    CREATE OR REPLACE VIEW {c}.{schema_prefix}metrics.demand_risk_forecast AS
    SELECT
        u.unit_id,
        u.metro_area,
        u.franchisee_id,
        u.region_id,
        w.forecast_date,
        w.observation_type,
        w.weather_condition,
        w.alert_level,
        w.high_temp_f,
        w.low_temp_f,
        w.precipitation_inches,
        w.demand_multiplier                             AS weather_demand_multiplier,
        w.channel_shift_delivery,
        e.event_name,
        e.event_category,
        e.venue,
        e.est_attendance,
        e.est_demand_multiplier                         AS event_demand_multiplier,
        e.source                                        AS event_source,
        ROUND(LEAST(2.5, GREATEST(0.3,
            COALESCE(w.demand_multiplier, 1.0) * COALESCE(e.est_demand_multiplier, 1.0)
        )), 4)                                          AS combined_demand_multiplier,
        CASE
            WHEN LEAST(2.5, GREATEST(0.3,
                COALESCE(w.demand_multiplier, 1.0) * COALESCE(e.est_demand_multiplier, 1.0)
            )) < 0.8  THEN 'demand_risk'
            WHEN LEAST(2.5, GREATEST(0.3,
                COALESCE(w.demand_multiplier, 1.0) * COALESCE(e.est_demand_multiplier, 1.0)
            )) > 1.4  THEN 'capacity_risk'
            ELSE 'normal'
        END                                             AS risk_level
    FROM {c}.{schema_prefix}ref.unit u
    JOIN {c}.{schema_prefix}ref.weather_conditions w
        ON  u.metro_area   = w.metro_area
        AND w.forecast_date BETWEEN CURRENT_DATE AND DATE_ADD(CURRENT_DATE, 14)
    LEFT JOIN {c}.{schema_prefix}ref.local_events e
        ON  u.metro_area = e.metro_area
        AND w.forecast_date = e.event_date
""")
print(f"[INFO] View ready: {c}.{schema_prefix}metrics.demand_risk_forecast")

# COMMAND ----------
# 6. Order Reconciliation — maps real PizzaTel web orders (OTel) to their synth rows.
# Best-effort: if the OTel tables are missing/ungranted, skip gracefully (bolt-on contract).
# The web app.order.id (UUID) has no native column in silver by design (seamless blend);
# this view recomputes the id-bridge make_id("otel", trace_id) so web ↔ synth orders reconcile.
try:
    spark.sql(f"SELECT 1 FROM {otel_catalog}.{otel_schema}.otel_logs LIMIT 1")
    spark.sql(f"""
        CREATE OR REPLACE VIEW {c}.{schema_prefix}metrics.order_reconciliation
        COMMENT 'Reconciles real PizzaTel web orders (OTel) to their synth-pipeline rows AND to the customer record. web_order_id is the storefront app.order.id (UUID); guest_order_id is the bridged synth key = make_id("otel", trace_id). reconciled=true means the web order flowed through to silver.guest_order. member_id is the web-injected app.order.member_id (synth customer key, 1..50000; NULL = anonymous); customer_matched=true means it joined an existing customer_features record, exposing customer_tier and customer_lifetime_spend. amount_diff should be ~0.'
        AS
        WITH web AS (
            SELECT
                attributes['app.order.id']                          AS web_order_id,
                trace_id,
                CAST(attributes['app.order.member_id'] AS BIGINT)   AS member_id,
                CAST(attributes['app.order.amount'] AS DOUBLE)      AS web_amount,
                CAST(attributes['app.order.items.count'] AS INT)    AS web_item_count,
                attributes['app.shipping.tracking.id']              AS web_tracking_id,
                CAST(to_timestamp(time_unix_nano/1e9) AS TIMESTAMP) AS web_order_ts
            FROM {otel_catalog}.{otel_schema}.otel_logs
            WHERE attributes['app.order.id'] IS NOT NULL
              AND CAST(attributes['app.order.amount'] AS DOUBLE) > 0
        ),
        web_dedup AS (
            SELECT web_order_id,
                   MAX(trace_id)       AS trace_id,
                   MAX(member_id)       AS member_id,
                   MAX(web_amount)      AS web_amount,
                   MAX(web_item_count)  AS web_item_count,
                   MAX(web_tracking_id) AS web_tracking_id,
                   MAX(web_order_ts)    AS web_order_ts
            FROM web GROUP BY web_order_id
        )
        SELECT
            w.web_order_id,
            w.trace_id,
            CAST(CONV(SUBSTR(SHA2(CONCAT_WS(':','otel', w.trace_id),256),1,14),16,10) AS BIGINT) AS guest_order_id,
            w.member_id,
            w.web_amount,
            w.web_item_count,
            w.web_tracking_id,
            w.web_order_ts,
            (g.guest_order_id IS NOT NULL)             AS reconciled,
            g.unit_id,
            g.channel,
            g.order_status,
            g.total_amount                             AS silver_total_amount,
            g.placed_at                                AS silver_placed_at,
            ROUND(w.web_amount - g.total_amount, 2)    AS amount_diff,
            (cf.profile_id IS NOT NULL)                AS customer_matched,
            cf.tier                                    AS customer_tier,
            cf.total_orders                            AS customer_total_orders,
            cf.monetary_total                          AS customer_lifetime_spend
        FROM web_dedup w
        LEFT JOIN {c}.{schema_prefix}silver.guest_order g
            ON g.guest_order_id = CAST(CONV(SUBSTR(SHA2(CONCAT_WS(':','otel', w.trace_id),256),1,14),16,10) AS BIGINT)
        LEFT JOIN {c}.{schema_prefix}features.customer_features cf
            ON cf.profile_id = w.member_id
    """)
    print(f"[INFO] View ready: {c}.{schema_prefix}metrics.order_reconciliation")
except Exception as e:
    print(f"[WARN] order_reconciliation view skipped (OTel source unavailable): {e}")

# COMMAND ----------
print("[INFO] create_metric_views complete — order/loyalty/inventory/labor/demand-risk + order_reconciliation views")
