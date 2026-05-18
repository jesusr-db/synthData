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

print(f"[INFO] apply_catalog_metadata: catalog={catalog_name}")

# COMMAND ----------
# Table descriptions — sourced from MVM v1 data model
TABLE_COMMENTS = {
    "guest_order": "One row per customer order placed at a QSR unit. Captures channel, order status, monetary amounts, and timestamps for the full order lifecycle.",
    "order_item": "One row per line item within a guest order. Links to menu_item with quantity, unit price, discount, and fulfilment status.",
    "payment": "Payment transaction associated with a fulfilled guest order. Records tender type and settlement date.",
    "status_event": "State machine event for a guest order transition (placed → preparing → ready → fulfilled). Used for speed-of-service (SOS) analysis.",
    "delivery_order": "Delivery details for orders fulfilled via own-delivery or third-party delivery (3PD) channels.",
    "on_hand_balance": "Point-in-time inventory snapshot per SKU per unit. Records quantity on hand, reserved, and par level at the time of a tick.",
    "waste_log": "Inventory waste event — quantity of a stock SKU wasted per unit per tick, with category and cost.",
    "receiving_order": "Daily supplier delivery record restocking a SKU to par level at a unit.",
    "replenishment_order": "Automated replenishment request triggered when on-hand falls below 25% of par level.",
    "guest_profile": "Customer account record. Created on registration; updated on churn (account_status → inactive).",
    "loyalty_transaction": "Loyalty points earn or redeem event associated with an order. Points delta is positive for earn, negative for redeem.",
    "reward_redemption": "Reward voucher redemption event. Points redeemed and reward dollar value. Always paired with a loyalty_transaction of type=redeem.",
    "shift": "Scheduled work shift for an employee at a unit. Covers a single date with start/end times and role.",
    "time_punch": "Actual punch-in/punch-out record for an employee, linked to a scheduled shift.",
    "unit_performance_daily": "Gold table: daily summary of orders, revenue, and SOS compliance per unit.",
    "sos_compliance_summary": "Gold table: speed-of-service breach rate aggregated by unit, channel, and period.",
    "loyalty_cohort_metrics": "Gold table: loyalty member engagement metrics by tier and cohort month.",
    "inventory_waste_summary": "Gold table: waste rate as a percentage of inventory usage by unit and week.",
}

# Column descriptions per table — subset of highest-value columns
COLUMN_COMMENTS = {
    "guest_order": {
        "guest_order_id": "Surrogate primary key for this order.",
        "unit_id": "FK to ref.unit — the location where the order was placed.",
        "channel": "Order channel: carryout, own_delivery, 3pd_delivery, or catering.",
        "order_status": "Final order state: fulfilled or cancelled.",
        "profile_id": "FK to silver.guest_profile — null for anonymous orders.",
        "member_id": "FK to loyalty member — null for non-members.",
        "subtotal": "Sum of line_net_amount across all items (post-discount, pre-tax).",
        "discount_amount": "Total discount applied to this order.",
        "tax_amount": "Sales tax computed on the discounted subtotal.",
        "total_amount": "Amount charged to the customer: subtotal + tax. Zero for cancelled orders.",
        "financial_period_id": "FK to ref.financial_period — the accounting month this order falls in.",
        "sos_breach": "True if the order exceeded the speed-of-service target for its channel.",
    },
    "order_item": {
        "order_item_id": "Surrogate primary key for this line item.",
        "guest_order_id": "FK to silver.guest_order.",
        "menu_item_id": "FK to ref.menu_item.",
        "quantity": "Number of units ordered.",
        "unit_price": "Per-unit price after market index and channel markup, before discount.",
        "line_gross_amount": "unit_price × quantity, before discount.",
        "line_discount_amount": "Discount allocated to this line item, proportional to its share of order gross.",
        "line_net_amount": "line_gross_amount - line_discount_amount.",
        "item_status": "fulfilled, cancelled (parent order cancelled), or refunded (~1% of fulfilled items).",
        "waste_flag": "True if this item was logged as waste. Higher rate on cancelled orders and late-night ticks.",
    },
    "waste_log": {
        "waste_log_id": "Surrogate PK.",
        "stock_sku": "FK to ref.recipe_ingredient — the stock-keeping unit wasted.",
        "waste_quantity": "Units wasted (in recipe ingredient units).",
        "waste_category": "Reason for waste: overproduction (50%), spoilage (25%), theft (10%), expired (10%), damaged (5%).",
        "waste_cost": "Estimated cost of waste at $2.50 per unit.",
    },
    "loyalty_transaction": {
        "loyalty_transaction_id": "Surrogate PK.",
        "member_id": "FK to loyalty member.",
        "transaction_type": "earn (points awarded on purchase) or redeem (points deducted on reward use).",
        "points_delta": "Points change: positive for earn, negative for redeem.",
        "tier": "Member loyalty tier at transaction time: bronze, silver, gold, or platinum.",
    },
    "guest_profile": {
        "guest_profile_id": "Surrogate PK. Reused as digital_account_id.",
        "account_status": "active (normal), inactive (unverified or churned), or suspended (fraud flag).",
    },
}

# COMMAND ----------
# Apply table comments
for table, comment in TABLE_COMMENTS.items():
    full = f"{catalog_name}.silver.{table}"
    try:
        spark.sql(f"COMMENT ON TABLE {full} IS '{comment}'")
        print(f"[OK] table comment: {table}")
    except Exception as e:
        print(f"[WARN] Could not comment {table}: {e}")

# COMMAND ----------
# Apply column comments
for table, cols in COLUMN_COMMENTS.items():
    full = f"{catalog_name}.silver.{table}"
    for col, comment in cols.items():
        try:
            spark.sql(f"ALTER TABLE {full} ALTER COLUMN {col} COMMENT '{comment}'")
        except Exception as e:
            print(f"[WARN] {table}.{col}: {e}")
    print(f"[OK] column comments: {table}")

# COMMAND ----------
# Apply primary key constraints (Unity Catalog informational PKs)
PK_CONSTRAINTS = {
    "guest_order":         ("pk_guest_order",         "guest_order_id"),
    "order_item":          ("pk_order_item",           "order_item_id"),
    "payment":             ("pk_payment",              "payment_id"),
    "status_event":        ("pk_status_event",         "status_event_id"),
    "delivery_order":      ("pk_delivery_order",       "delivery_order_id"),
    "on_hand_balance":     ("pk_on_hand_balance",      "on_hand_balance_id"),
    "waste_log":           ("pk_waste_log",            "waste_log_id"),
    "receiving_order":     ("pk_receiving_order",      "receiving_order_id"),
    "replenishment_order": ("pk_replenishment_order",  "replenishment_order_id"),
    "guest_profile":       ("pk_guest_profile",        "guest_profile_id"),
    "loyalty_transaction": ("pk_loyalty_transaction",  "loyalty_transaction_id"),
    "reward_redemption":   ("pk_reward_redemption",    "reward_redemption_id"),
    "shift":               ("pk_shift",                "shift_id"),
    "time_punch":          ("pk_time_punch",           "time_punch_id"),
}

for table, (constraint_name, col) in PK_CONSTRAINTS.items():
    full = f"{catalog_name}.silver.{table}"
    try:
        spark.sql(f"ALTER TABLE {full} DROP CONSTRAINT IF EXISTS {constraint_name}")
        spark.sql(f"ALTER TABLE {full} ADD CONSTRAINT {constraint_name} PRIMARY KEY ({col}) NOT ENFORCED")
        print(f"[OK] PK: {table}.{col}")
    except Exception as e:
        print(f"[WARN] PK {table}: {e}")

# COMMAND ----------
# Apply foreign key constraints (informational, not enforced)
FK_CONSTRAINTS = [
    # (child_table, constraint_name, child_col, parent_table, parent_col)
    ("order_item",          "fk_order_item_order",     "guest_order_id", "guest_order",   "guest_order_id"),
    ("payment",             "fk_payment_order",        "guest_order_id", "guest_order",   "guest_order_id"),
    ("status_event",        "fk_status_event_order",   "guest_order_id", "guest_order",   "guest_order_id"),
    ("delivery_order",      "fk_delivery_order_order", "guest_order_id", "guest_order",   "guest_order_id"),
    ("loyalty_transaction", "fk_lt_order",             "guest_order_id", "guest_order",   "guest_order_id"),
    ("reward_redemption",   "fk_rr_order",             "guest_order_id", "guest_order",   "guest_order_id"),
    ("guest_order",         "fk_order_guest",          "profile_id",     "guest_profile", "guest_profile_id"),
]

for child, constraint, child_col, parent, parent_col in FK_CONSTRAINTS:
    child_full = f"{catalog_name}.silver.{child}"
    parent_full = f"{catalog_name}.silver.{parent}"
    try:
        spark.sql(f"ALTER TABLE {child_full} DROP CONSTRAINT IF EXISTS {constraint}")
        spark.sql(f"""
            ALTER TABLE {child_full}
            ADD CONSTRAINT {constraint}
            FOREIGN KEY ({child_col}) REFERENCES {parent_full}({parent_col})
            NOT ENFORCED
        """)
        print(f"[OK] FK: {child}.{child_col} → {parent}.{parent_col}")
    except Exception as e:
        print(f"[WARN] FK {constraint}: {e}")

print("[INFO] apply_catalog_metadata complete")
