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

print(f"[INFO] create_metric_views: catalog={catalog_name}")
c = catalog_name

# COMMAND ----------
# Ensure metrics schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {c}.metrics")

# COMMAND ----------
# 1. unit_daily_summary — orders, revenue, SOS per unit per day
spark.sql(f"""
    CREATE OR REPLACE VIEW {c}.metrics.unit_daily_summary
    COMMENT 'Daily order volume, revenue, and SOS compliance per unit.'
    AS
    SELECT
        unit_id,
        CAST(placed_at AS DATE)                                         AS order_date,
        COUNT(*)                                                        AS total_orders,
        COUNT(CASE WHEN order_status = 'fulfilled' THEN 1 END)          AS fulfilled_orders,
        COUNT(CASE WHEN order_status = 'cancelled' THEN 1 END)          AS cancelled_orders,
        ROUND(SUM(total_amount), 2)                                     AS total_revenue,
        ROUND(AVG(total_amount) FILTER (WHERE order_status='fulfilled'), 2) AS avg_order_value,
        ROUND(AVG(CAST(sos_breach AS INT)) * 100, 2)                    AS sos_breach_pct,
        ROUND(AVG(discount_amount) FILTER (WHERE discount_amount > 0), 2) AS avg_discount_when_applied
    FROM {c}.silver.guest_order
    GROUP BY unit_id, CAST(placed_at AS DATE)
""")
print("[OK] metrics.unit_daily_summary")

# COMMAND ----------
# 2. loyalty_tier_distribution — member count and avg spend by tier
spark.sql(f"""
    CREATE OR REPLACE VIEW {c}.metrics.loyalty_tier_distribution
    COMMENT 'Loyalty member transaction counts and average spend by tier and month.'
    AS
    SELECT
        tier,
        DATE_TRUNC('month', transaction_at)                AS month,
        COUNT(DISTINCT member_id)                          AS active_members,
        COUNT(CASE WHEN transaction_type = 'earn' THEN 1 END)   AS earn_transactions,
        COUNT(CASE WHEN transaction_type = 'redeem' THEN 1 END) AS redeem_transactions,
        SUM(points_delta) FILTER (WHERE transaction_type = 'earn')   AS total_points_earned,
        SUM(-points_delta) FILTER (WHERE transaction_type = 'redeem') AS total_points_redeemed
    FROM {c}.silver.loyalty_transaction
    GROUP BY tier, DATE_TRUNC('month', transaction_at)
""")
print("[OK] metrics.loyalty_tier_distribution")

# COMMAND ----------
# 3. inventory_waste_rate — waste as % of total usage by unit and week
spark.sql(f"""
    CREATE OR REPLACE VIEW {c}.metrics.inventory_waste_rate
    COMMENT 'Weekly waste quantity and cost as a percentage of total inventory usage per unit.'
    AS
    WITH usage AS (
        SELECT
            unit_id,
            DATE_TRUNC('week', snapshot_at)  AS week,
            stock_sku,
            SUM(quantity_reserved)           AS total_used
        FROM {c}.silver.on_hand_balance
        GROUP BY unit_id, DATE_TRUNC('week', snapshot_at), stock_sku
    ),
    waste AS (
        SELECT
            unit_id,
            DATE_TRUNC('week', logged_at)    AS week,
            stock_sku,
            waste_category,
            SUM(waste_quantity)              AS total_waste,
            SUM(waste_cost)                  AS total_waste_cost
        FROM {c}.silver.waste_log
        GROUP BY unit_id, DATE_TRUNC('week', logged_at), stock_sku, waste_category
    )
    SELECT
        w.unit_id,
        w.week,
        w.stock_sku,
        w.waste_category,
        ROUND(w.total_waste, 3)                                       AS waste_qty,
        ROUND(w.total_waste_cost, 2)                                  AS waste_cost,
        ROUND(w.total_waste / NULLIF(u.total_used, 0) * 100, 2)      AS waste_pct_of_usage
    FROM waste w
    LEFT JOIN usage u USING (unit_id, week, stock_sku)
""")
print("[OK] metrics.inventory_waste_rate")

# COMMAND ----------
# 4. staff_utilization — scheduled vs actual hours, no-show rate
spark.sql(f"""
    CREATE OR REPLACE VIEW {c}.metrics.staff_utilization
    COMMENT 'Scheduled vs actual hours worked and no-show rate per unit per day.'
    AS
    WITH scheduled AS (
        SELECT
            unit_id,
            CAST(shift_start AS DATE)                                     AS shift_date,
            COUNT(*)                                                       AS scheduled_shifts,
            ROUND(SUM(DATEDIFF(HOUR, shift_start, shift_end)), 2)         AS scheduled_hours
        FROM {c}.silver.shift
        GROUP BY unit_id, CAST(shift_start AS DATE)
    ),
    actual AS (
        SELECT
            unit_id,
            CAST(punch_in AS DATE)      AS shift_date,
            COUNT(*)                    AS punched_shifts,
            ROUND(SUM(hours_worked), 2) AS actual_hours
        FROM {c}.silver.time_punch
        GROUP BY unit_id, CAST(punch_in AS DATE)
    )
    SELECT
        s.unit_id,
        s.shift_date,
        s.scheduled_shifts,
        s.scheduled_hours,
        COALESCE(a.punched_shifts, 0)                                              AS punched_shifts,
        COALESCE(a.actual_hours, 0)                                                AS actual_hours,
        ROUND((s.scheduled_shifts - COALESCE(a.punched_shifts, 0))
              / NULLIF(s.scheduled_shifts, 0) * 100, 2)                            AS no_show_pct
    FROM scheduled s
    LEFT JOIN actual a USING (unit_id, shift_date)
""")
print("[OK] metrics.staff_utilization")

# COMMAND ----------
# 5. channel_mix_trend — 3PD vs own delivery vs carryout share over time
spark.sql(f"""
    CREATE OR REPLACE VIEW {c}.metrics.channel_mix_trend
    COMMENT 'Weekly channel mix (order count share and revenue share) per unit.'
    AS
    SELECT
        unit_id,
        DATE_TRUNC('week', placed_at)                                   AS week,
        channel,
        COUNT(*)                                                        AS orders,
        ROUND(SUM(total_amount), 2)                                     AS revenue,
        ROUND(COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY unit_id, DATE_TRUNC('week', placed_at)) * 100, 2) AS order_share_pct,
        ROUND(SUM(total_amount) / NULLIF(SUM(SUM(total_amount)) OVER (PARTITION BY unit_id, DATE_TRUNC('week', placed_at)), 0) * 100, 2) AS revenue_share_pct
    FROM {c}.silver.guest_order
    WHERE order_status = 'fulfilled'
    GROUP BY unit_id, DATE_TRUNC('week', placed_at), channel
""")
print("[OK] metrics.channel_mix_trend")

# COMMAND ----------
print("[INFO] create_metric_views complete — 5 views created in metrics schema")
