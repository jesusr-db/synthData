# Phase 2 — Catalog Enrichment + Genie Space

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply table/column descriptions and PK/FK constraints to all silver tables, create 5 proper metric views on silver data, and set up a Genie Space scoped to the QSR dataset — all fully automated from `setup_job`.

**Architecture:** Three new notebook tasks added to `setup_job.yml`, running after `start_pipeline` (so silver tables exist). The existing metric view stub in `setup_notebook.py` Step 5 is removed to avoid confusion. Genie Space creation uses the Databricks REST API called from a notebook. All new notebooks follow the same pattern as existing setup notebooks (bundle root on sys.path, widget params).

**Tech Stack:** Python, PySpark SQL, Databricks REST API (`/api/2.0/genie/spaces`), Databricks Asset Bundles, `requests` library

---

## File Map

| File | Changes |
|---|---|
| `src/setup/apply_catalog_metadata.py` | **CREATE** — notebook: apply table/column COMMENT + PK/FK constraints on all silver tables |
| `src/setup/create_metric_views.py` | **CREATE** — notebook: create 5 aggregation views in `{catalog}.metrics` schema |
| `src/setup/create_genie_space.py` | **CREATE** — notebook: POST to `/api/2.0/genie/spaces` to create a pre-configured Genie Space |
| `src/setup/setup_notebook.py` | **MODIFY** — remove Step 5 (stub metric views) to avoid running before silver tables exist |
| `resources/setup_job.yml` | **MODIFY** — add `apply_catalog_metadata`, `create_metric_views`, `create_genie_space` tasks |

---

## Task Order in `setup_job` (after this plan)

```
setup
  ├── start_pipeline (full_refresh=true)
  │     ├── apply_catalog_metadata   ← NEW: descriptions + PK/FK after silver populated
  │     │     └── create_metric_views  ← NEW: aggregation views over silver tables
  │     │           └── create_genie_space  ← NEW: Genie Space last (needs metric views)
  │     │                 └── unpause_generator
  └── backfill (parallel to start_pipeline)
```

---

## Task 1: Remove stub metric views from `setup_notebook.py`

**Files:**
- Modify: `src/setup/setup_notebook.py`

Step 5 in `setup_notebook.py` creates stub metric views by doing `SELECT * FROM gold_table`. This runs before the DLT pipeline creates gold tables, so it either skips or makes trivial views. Proper metric views will be created by the new `create_metric_views` task that runs after `start_pipeline`.

- [ ] **Step 1: Remove Step 5 from `setup_notebook.py`**

Delete the entire Step 5 block from `src/setup/setup_notebook.py` — from the `# COMMAND ----------` before `# Step 5: Create UC Metric Views` through the final `print("[INFO] Setup complete")` line, replacing the removed section with just:

```python
# COMMAND ----------
print("[INFO] Setup complete")
```

The final file should end with Step 4 (seed reference tables) and a setup complete print.

- [ ] **Step 2: Verify setup notebook still runs**

Review the file top-to-bottom. Steps 1–4 should be intact. No metric view code remains.

- [ ] **Step 3: Commit**

```bash
git add src/setup/setup_notebook.py
git commit -m "chore: remove stub metric views from setup_notebook — moved to dedicated task"
```

---

## Task 2: Create `apply_catalog_metadata.py` Notebook

**Files:**
- Create: `src/setup/apply_catalog_metadata.py`

This notebook applies `COMMENT ON TABLE` and `ALTER TABLE ... ALTER COLUMN ... COMMENT` to all 14 silver tables and 4 gold tables, then adds PK/FK constraints where supported. It is idempotent — re-running applies the same comments again.

- [ ] **Step 1: Create the file**

Create `src/setup/apply_catalog_metadata.py` with the following content:

```python
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
    full = f"{catalog_name}.silver.{table}" if table not in (
        "unit_performance_daily", "sos_compliance_summary",
        "loyalty_cohort_metrics", "inventory_waste_summary"
    ) else f"{catalog_name}.silver.{table}"
    try:
        spark.sql(f"COMMENT ON TABLE {full} IS '{comment}'")
        print(f"[OK] table comment: {table}")
    except Exception as e:
        print(f"[WARN] Could not comment {table}: {e}")

# COMMAND ----------
# Apply column comments
for table, cols in COLUMN_COMMENTS.items():
    schema = "silver"
    full = f"{catalog_name}.{schema}.{table}"
    for col, comment in cols.items():
        try:
            spark.sql(f"ALTER TABLE {full} ALTER COLUMN {col} COMMENT '{comment}'")
        except Exception as e:
            print(f"[WARN] {table}.{col}: {e}")
    print(f"[OK] column comments: {table}")

# COMMAND ----------
# Apply primary key constraints (Unity Catalog informational PKs)
PK_CONSTRAINTS = {
    "guest_order":        ("pk_guest_order",        "guest_order_id"),
    "order_item":         ("pk_order_item",          "order_item_id"),
    "payment":            ("pk_payment",             "payment_id"),
    "status_event":       ("pk_status_event",        "status_event_id"),
    "delivery_order":     ("pk_delivery_order",      "delivery_order_id"),
    "on_hand_balance":    ("pk_on_hand_balance",     "on_hand_balance_id"),
    "waste_log":          ("pk_waste_log",           "waste_log_id"),
    "receiving_order":    ("pk_receiving_order",     "receiving_order_id"),
    "replenishment_order":("pk_replenishment_order", "replenishment_order_id"),
    "guest_profile":      ("pk_guest_profile",       "guest_profile_id"),
    "loyalty_transaction":("pk_loyalty_transaction", "loyalty_transaction_id"),
    "reward_redemption":  ("pk_reward_redemption",   "reward_redemption_id"),
    "shift":              ("pk_shift",               "shift_id"),
    "time_punch":         ("pk_time_punch",          "time_punch_id"),
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
    ("order_item",          "fk_order_item_order",       "guest_order_id",  "guest_order",    "guest_order_id"),
    ("payment",             "fk_payment_order",          "guest_order_id",  "guest_order",    "guest_order_id"),
    ("status_event",        "fk_status_event_order",     "guest_order_id",  "guest_order",    "guest_order_id"),
    ("delivery_order",      "fk_delivery_order_order",   "guest_order_id",  "guest_order",    "guest_order_id"),
    ("loyalty_transaction", "fk_lt_order",               "guest_order_id",  "guest_order",    "guest_order_id"),
    ("reward_redemption",   "fk_rr_order",               "guest_order_id",  "guest_order",    "guest_order_id"),
    ("guest_order",         "fk_order_guest",            "profile_id",      "guest_profile",  "guest_profile_id"),
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
```

- [ ] **Step 2: Commit**

```bash
git add src/setup/apply_catalog_metadata.py
git commit -m "feat: apply_catalog_metadata notebook — table/column comments + PK/FK constraints"
```

---

## Task 3: Create `create_metric_views.py` Notebook

**Files:**
- Create: `src/setup/create_metric_views.py`

The 5 views aggregate from silver tables. All use `CREATE OR REPLACE VIEW` so they are safe to re-run.

- [ ] **Step 1: Create the file**

Create `src/setup/create_metric_views.py`:

```python
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
        COUNTIF(order_status = 'fulfilled')                             AS fulfilled_orders,
        COUNTIF(order_status = 'cancelled')                             AS cancelled_orders,
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
        COUNTIF(transaction_type = 'earn')                 AS earn_transactions,
        COUNTIF(transaction_type = 'redeem')               AS redeem_transactions,
        SUM(points_delta) FILTER (WHERE transaction_type = 'earn')  AS total_points_earned,
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
        ROUND(w.total_waste, 3)                                        AS waste_qty,
        ROUND(w.total_waste_cost, 2)                                   AS waste_cost,
        ROUND(w.total_waste / NULLIF(u.total_used, 0) * 100, 2)       AS waste_pct_of_usage
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
            CAST(shift_start AS DATE)                                       AS shift_date,
            COUNT(*)                                                         AS scheduled_shifts,
            ROUND(SUM(DATEDIFF(HOUR, shift_start, shift_end)), 2)           AS scheduled_hours
        FROM {c}.silver.shift
        GROUP BY unit_id, CAST(shift_start AS DATE)
    ),
    actual AS (
        SELECT
            unit_id,
            CAST(punch_in AS DATE)   AS shift_date,
            COUNT(*)                 AS punched_shifts,
            ROUND(SUM(hours_worked), 2) AS actual_hours
        FROM {c}.silver.time_punch
        GROUP BY unit_id, CAST(punch_in AS DATE)
    )
    SELECT
        s.unit_id,
        s.shift_date,
        s.scheduled_shifts,
        s.scheduled_hours,
        COALESCE(a.punched_shifts, 0)                                               AS punched_shifts,
        COALESCE(a.actual_hours, 0)                                                 AS actual_hours,
        ROUND((s.scheduled_shifts - COALESCE(a.punched_shifts, 0))
              / NULLIF(s.scheduled_shifts, 0) * 100, 2)                             AS no_show_pct
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
```

- [ ] **Step 2: Commit**

```bash
git add src/setup/create_metric_views.py
git commit -m "feat: create_metric_views notebook — 5 aggregation views over silver tables"
```

---

## Task 4: Create `create_genie_space.py` Notebook

**Files:**
- Create: `src/setup/create_genie_space.py`

The Genie Space is created via the Databricks REST API. The notebook is idempotent: it checks if a space with the same title already exists and skips creation if found.

- [ ] **Step 1: Create the file**

Create `src/setup/create_genie_space.py`:

```python
# Databricks notebook source
# COMMAND ----------
import sys, json, requests

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_notebook_path.replace("/Workspace", "").split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

try:
    catalog_name = dbutils.widgets.get("catalog_name")
except Exception:
    catalog_name = "jmrdemo"

# Resolve workspace URL and token from notebook context
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
workspace_url = "https://" + ctx.browserHostName().get()
token = ctx.apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

print(f"[INFO] create_genie_space: catalog={catalog_name}, workspace={workspace_url}")

# COMMAND ----------
SPACE_TITLE = f"QSR Synthetic Data — {catalog_name}"

# Check if space already exists
resp = requests.get(f"{workspace_url}/api/2.0/genie/spaces", headers=headers, timeout=30)
if resp.status_code == 200:
    existing = [s for s in resp.json().get("spaces", []) if s.get("title") == SPACE_TITLE]
    if existing:
        print(f"[SKIP] Genie Space '{SPACE_TITLE}' already exists (id={existing[0]['space_id']})")
        dbutils.notebook.exit(existing[0]["space_id"])

# COMMAND ----------
# Table references: all silver + metrics views
SILVER_TABLES = [
    "guest_order", "order_item", "payment", "status_event", "delivery_order",
    "on_hand_balance", "waste_log", "receiving_order", "replenishment_order",
    "guest_profile", "loyalty_transaction", "reward_redemption", "shift", "time_punch",
]
METRICS_VIEWS = [
    "unit_daily_summary", "loyalty_tier_distribution", "inventory_waste_rate",
    "staff_utilization", "channel_mix_trend",
]

table_refs = (
    [{"table_name": f"{catalog_name}.silver.{t}"} for t in SILVER_TABLES]
    + [{"table_name": f"{catalog_name}.metrics.{v}"} for v in METRICS_VIEWS]
)

# COMMAND ----------
INSTRUCTIONS = """
You are analyzing synthetic QSR (Quick Service Restaurant) data modeled after a Domino's-style franchise operation.

Key domain context:
- 250 restaurant units across 20 US metro areas, 80% franchised
- Channels: carryout (40%), 3pd_delivery (40%), own_delivery (16%), catering (4%)
- Loyalty tiers: bronze, silver, gold, platinum — higher tiers earn more points per dollar
- Loyalty redemptions are burn events; always paired with a loyalty_transaction of type=redeem
- SOS (speed of service) target: 720 sec carryout, 1800 sec delivery
- Financial periods are monthly; item prices drift ±3-6% per quarter
- waste_flag on order_item correlates with waste_log inventory events
- account_status on guest_profile: active (normal), inactive (churned/unverified), suspended (fraud)
- unit_volume_bias drives order count differences; market_price_index drives AOV differences
"""

SEED_QUESTIONS = [
    "Which units have the highest SOS breach rate this month?",
    "Show me loyalty tier distribution and points burn rate by tier",
    "What is the channel mix trend over the last 4 weeks by unit?",
    "Which stock SKUs have the highest waste cost per unit?",
    "Compare average order value by channel and metro area",
    "Show me the top 10 units by revenue last week",
    "What percentage of guests are loyalty members per unit?",
    "Which items are most frequently refunded?",
    "Show me staff no-show rate by unit over the last 30 days",
    "How does catering AOV compare to carryout across units?",
]

# COMMAND ----------
payload = {
    "title": SPACE_TITLE,
    "description": "Pre-configured Genie Space for exploring the QSR synthetic dataset (250 units, 1-month backfill + live stream).",
    "table_identifiers": [t["table_name"] for t in table_refs],
    "instructions": INSTRUCTIONS,
    "sample_questions": SEED_QUESTIONS,
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
```

- [ ] **Step 2: Commit**

```bash
git add src/setup/create_genie_space.py
git commit -m "feat: create_genie_space notebook — Genie Space via REST API with seed questions"
```

---

## Task 5: Wire New Tasks into `setup_job.yml`

**Files:**
- Modify: `resources/setup_job.yml`

- [ ] **Step 1: Add the three new tasks**

Replace the contents of `resources/setup_job.yml` with:

```yaml
resources:
  jobs:
    setup_job:
      name: "QSR Setup [${bundle.target}]"
      tags:
        project: qsr-synth-data-generator
      environments:
        - environment_key: generator
          spec:
            client: "1"
            dependencies:
              - faker>=20.0.0
      tasks:
        - task_key: setup
          notebook_task:
            notebook_path: ../src/setup/setup_notebook.py
            base_parameters:
              catalog_name: ${var.catalog_name}
              num_units: ${var.num_units}

        - task_key: start_pipeline
          depends_on:
            - task_key: setup
          pipeline_task:
            pipeline_id: ${resources.pipelines.mvm_pipeline.id}
            full_refresh: true

        - task_key: apply_catalog_metadata
          depends_on:
            - task_key: start_pipeline
          notebook_task:
            notebook_path: ../src/setup/apply_catalog_metadata.py
            base_parameters:
              catalog_name: ${var.catalog_name}

        - task_key: create_metric_views
          depends_on:
            - task_key: apply_catalog_metadata
          notebook_task:
            notebook_path: ../src/setup/create_metric_views.py
            base_parameters:
              catalog_name: ${var.catalog_name}

        - task_key: create_genie_space
          depends_on:
            - task_key: create_metric_views
          notebook_task:
            notebook_path: ../src/setup/create_genie_space.py
            base_parameters:
              catalog_name: ${var.catalog_name}

        - task_key: backfill
          depends_on:
            - task_key: setup
          environment_key: generator
          notebook_task:
            notebook_path: ../src/generator/main.py
            base_parameters:
              catalog_name: ${var.catalog_name}
              num_units: ${var.num_units}
              backfill_months: ${var.backfill_months}
              live_tick_seconds: ${var.live_tick_seconds}
              base_orders_per_unit_per_hour: ${var.base_orders_per_unit_per_hour}
              mode: backfill

        - task_key: unpause_generator
          depends_on:
            - task_key: backfill
            - task_key: create_genie_space
          notebook_task:
            notebook_path: ../src/setup/unpause_generator_notebook.py
            base_parameters:
              generator_job_id: ${resources.jobs.generator_job.id}
```

Note: `unpause_generator` now depends on BOTH `backfill` AND `create_genie_space` so it waits for all setup tasks before starting the live feed.

- [ ] **Step 2: Verify YAML syntax**

```bash
cd /Users/jesus.rodriguez/Documents/ItsAVibe/gitrepos_FY27/synthData
python -c "import yaml; yaml.safe_load(open('resources/setup_job.yml'))"
```
Expected: no output (valid YAML).

- [ ] **Step 3: Deploy bundle to validate**

```bash
databricks bundle validate
```
Expected: `Validation OK`.

- [ ] **Step 4: Commit**

```bash
git add resources/setup_job.yml
git commit -m "feat: wire apply_catalog_metadata + create_metric_views + create_genie_space into setup_job"
```

---

## Task 6: Final Verification

- [ ] **Step 1: Confirm all 5 new files are present**

```bash
ls src/setup/
```
Expected output includes: `apply_catalog_metadata.py`, `create_metric_views.py`, `create_genie_space.py`, `setup_notebook.py`, `unpause_generator_notebook.py`, `destroy_notebook.py`.

- [ ] **Step 2: Confirm setup_notebook.py no longer has metric view code**

```bash
grep -n "metric" src/setup/setup_notebook.py
```
Expected: no matches (Step 5 removed).

- [ ] **Step 3: Deploy to workspace**

```bash
databricks bundle deploy --target dev
```
Expected: bundle deploys cleanly, all 3 new notebook tasks appear in the setup_job in the Databricks UI.

- [ ] **Step 4: Run setup_job to test end-to-end**

Trigger via Databricks UI or CLI:
```bash
databricks jobs run-now --job-id <setup_job_id>
```

Verify each task completes:
- `apply_catalog_metadata`: check table descriptions appear in Unity Catalog Explorer
- `create_metric_views`: `SHOW TABLES IN <catalog>.metrics` should return 5 views
- `create_genie_space`: Genie Space link printed in task output

- [ ] **Step 5: Commit everything on branch**

Ensure the branch is `feat/phase-2-catalog-enrichment`:
```bash
git checkout -b feat/phase-2-catalog-enrichment
git log --oneline -6
```

All 5 commits from this plan should be visible.
