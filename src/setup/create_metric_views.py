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
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {c}.metrics")

# COMMAND ----------
# 1. Order Performance — volume, revenue, SOS compliance per unit/channel
spark.sql(f"""
    CREATE OR REPLACE VIEW {c}.metrics.order_performance
    WITH METRICS LANGUAGE YAML AS$$
version: 1.1
comment: "QSR order volume, revenue, and speed-of-service compliance by unit and channel"
source: {c}.silver.guest_order
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
    CREATE OR REPLACE VIEW {c}.metrics.loyalty_performance
    WITH METRICS LANGUAGE YAML AS$$
version: 1.1
comment: "Loyalty program points activity and member engagement by tier and unit"
source: {c}.silver.loyalty_transaction
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
  - name: Redemption Value
    expr: SUM(CASE WHEN transaction_type = 'redeem' THEN reward_value ELSE 0 END)
    comment: "Dollar value of redeemed rewards"
$$
""")
print("[OK] metrics.loyalty_performance")

# COMMAND ----------
# 3. Inventory Waste — waste quantity and cost by unit, SKU, and category
spark.sql(f"""
    CREATE OR REPLACE VIEW {c}.metrics.inventory_waste
    WITH METRICS LANGUAGE YAML AS$$
version: 1.1
comment: "Inventory waste quantity and cost by unit, SKU, and waste category"
source: {c}.silver.waste_log
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
    CREATE OR REPLACE VIEW {c}.metrics.staff_hours
    WITH METRICS LANGUAGE YAML AS$$
version: 1.1
comment: "Actual hours worked and shift counts per unit and date"
source: {c}.silver.time_punch
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
print("[INFO] create_metric_views complete — 4 metric views created in metrics schema")
