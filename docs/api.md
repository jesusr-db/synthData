# API Reference

This project has no HTTP REST API. "API" here covers three surfaces: **job parameters** (how to configure the four Databricks jobs), **metric view interface** (how to query the UC metric views), and **governance functions** (UC scalar and row filter functions callable from SQL).

---

## Job Parameters

All parameters are declared as Databricks Asset Bundle variables in `databricks.yml` and passed to notebooks as job widgets.

### Common Parameters (all jobs)

| Parameter | Default | Description |
|---|---|---|
| `catalog_name` | `jmrdemo` | Unity Catalog catalog. Must exist before setup runs. |
| `schema_prefix` | `synth_` | Prefix for all UC schemas: `{prefix}staging`, `{prefix}ref`, `{prefix}silver`, `{prefix}metrics`. Use `""` for no prefix. |

### `setup_job` — task: `setup`

Source: `src/setup/setup_notebook.py`

| Parameter | Default | Description |
|---|---|---|
| `catalog_name` | `jmrdemo` | |
| `num_units` | `250` | Number of restaurant units to seed in `ref.unit`. |
| `schema_prefix` | `synth_` | |

### `setup_job` — task: `backfill` (and `generator_job` — task: `generate`)

Source: `src/generator/main.py`

| Parameter | Default | Description |
|---|---|---|
| `catalog_name` | `jmrdemo` | |
| `num_units` | `250` | Must match the value used during setup (controls EntityRegistry load). |
| `backfill_months` | `1` | Months of history to generate when no staging data exists. Ignored if `start_dt_override` is set or data already exists. |
| `live_tick_seconds` | `60` | Sub-tick granularity within each hour. `60` = one sub-tick per minute, matching per-minute historical cadence. |
| `base_orders_per_unit_per_hour` | `18` | Base order rate per unit; modified by `unit_volume_bias` and demand model. |
| `start_dt_override` | `""` | ISO datetime to force backfill start (e.g. `2026-05-19T00:00:00`). Empty = auto-detect from staging MAX(event_ts). |
| `mode` | `live` | `backfill` or `live`. Backfill generates a historical window; live generates the previous hour. |
| `schema_prefix` | `synth_` | |

### `setup_job` — task: `start_pipeline`

Source: `src/setup/start_pipeline_notebook.py`

| Parameter | Default | Description |
|---|---|---|
| `catalog_name` | `jmrdemo` | |
| `schema_prefix` | `synth_` | |

### `setup_job` — task: `create_metric_views`

Source: `src/setup/create_metric_views.py`

| Parameter | Default | Description |
|---|---|---|
| `catalog_name` | `jmrdemo` | |
| `schema_prefix` | `synth_` | |

### `setup_job` — task: `create_genie_space`

Source: `src/setup/create_genie_space.py`

| Parameter | Default | Description |
|---|---|---|
| `catalog_name` | `jmrdemo` | |
| `schema_prefix` | `synth_` | |

### `setup_job` — task: `apply_governance`

Source: `src/setup/apply_governance.py`

| Parameter | Default | Description |
|---|---|---|
| `catalog_name` | `jmrdemo` | |
| `schema_prefix` | `synth_` | |

### `setup_job` — task: `configure_monitoring`

Source: `src/setup/configure_monitoring.py`

| Parameter | Default | Description |
|---|---|---|
| `catalog_name` | `jmrdemo` | |
| `schema_prefix` | `synth_` | |

### `setup_job` — task: `unpause_generator`

Source: `src/setup/unpause_generator_notebook.py`

| Parameter | Default | Description |
|---|---|---|
| `generator_job_id` | (resolved from bundle) | Job ID of `generator_job`. Injected automatically by DAB via `${resources.jobs.generator_job.id}`. |

### `destroy_job`

Source: `src/setup/destroy_notebook.py`

| Parameter | Default | Description |
|---|---|---|
| `catalog_name` | `jmrdemo` | |
| `schema_prefix` | `synth_` | |

---

## Metric View Interface

Metric views are Unity Catalog objects created with `WITH METRICS LANGUAGE YAML`. They are queried like tables but expose named measures and dimensions that can be sliced ad-hoc.

### `{catalog}.{prefix}metrics.order_performance`

Source: `silver.guest_order`

**Dimensions:** Unit ID, Channel, Order Type, Order Status, Order Date (`CAST(placed_at AS DATE)`), Order Month (`DATE_TRUNC('MONTH', placed_at)`)

**Measures:**

| Measure | Expression | Description |
|---|---|---|
| Total Orders | `COUNT(1)` | Total orders placed |
| Total Revenue | `SUM(total_amount)` | Gross revenue |
| Average Order Value | `SUM(total_amount) / COUNT(1)` | Revenue per order |
| Fulfilled Orders | `COUNT(CASE WHEN order_status = 'fulfilled' THEN 1 END)` | |
| Cancelled Orders | `COUNT(CASE WHEN order_status = 'cancelled' THEN 1 END)` | |
| Total Discount | `SUM(discount_amount)` | Total discount dollars |
| SOS Breach Rate | `SUM(CAST(sos_breach AS INT)) / COUNT(1)` | Fraction exceeding SOS target |

### `{catalog}.{prefix}metrics.loyalty_performance`

Source: `silver.loyalty_transaction`

**Dimensions:** Tier, Transaction Type, Unit ID, Transaction Month (`DATE_TRUNC('MONTH', transaction_at)`)

**Measures:**

| Measure | Expression |
|---|---|
| Unique Members | `COUNT(DISTINCT member_id)` |
| Total Transactions | `COUNT(1)` |
| Points Earned | `SUM(CASE WHEN transaction_type = 'earn' THEN points_delta ELSE 0 END)` |
| Points Redeemed | `SUM(CASE WHEN transaction_type = 'redeem' THEN ABS(points_delta) ELSE 0 END)` |

### `{catalog}.{prefix}metrics.inventory_waste`

Source: `silver.waste_log`

**Dimensions:** Unit ID, Stock SKU, Waste Category, Waste Week (`DATE_TRUNC('WEEK', logged_at)`), Waste Month

**Measures:**

| Measure | Expression |
|---|---|
| Total Waste Quantity | `SUM(waste_quantity)` |
| Total Waste Cost | `SUM(waste_cost)` |
| Waste Events | `COUNT(1)` |
| Average Waste Cost per Event | `SUM(waste_cost) / COUNT(1)` |

### `{catalog}.{prefix}metrics.staff_hours`

Source: `silver.time_punch`

**Dimensions:** Unit ID, Shift Date (`CAST(punch_in AS DATE)`), Shift Month (`DATE_TRUNC('MONTH', punch_in)`)

**Measures:**

| Measure | Expression |
|---|---|
| Total Hours Worked | `SUM(hours_worked)` |
| Total Shifts | `COUNT(1)` |
| Unique Employees | `COUNT(DISTINCT employee_id)` |
| Average Hours per Shift | `SUM(hours_worked) / COUNT(1)` |

---

## Governance Functions (`{catalog}.{prefix}ref`)

### `mask_email(email STRING) RETURNS STRING`

Masks all characters before `@` except the first letter. `NULL` input returns `NULL`.

```sql
SELECT mask_email('john.doe@example.com')  -- j*******@example.com
SELECT mask_email(NULL)                     -- NULL
```

### `mask_phone(phone STRING) RETURNS STRING`

Strips non-numeric characters, then masks all digits except the last 4.

```sql
SELECT mask_phone('+1 (555) 123-4567')  -- *******4567
```

### `tier_to_multiplier(tier STRING) RETURNS DOUBLE`

Maps loyalty tier to points earn multiplier.

```sql
SELECT tier_to_multiplier('gold')  -- 2.0
-- bronze=1.0, silver=1.5, gold=2.0, elite=3.0, other=1.0
```

### `filter_by_franchisee(franchisee_id BIGINT) RETURNS BOOLEAN`

Row filter function. Returns `TRUE` if the calling user is a member of the `franchisee_{id}` account group or the `qsr_admin` group. Applied as a row filter on: `silver.guest_order`, `silver.waste_log`, `silver.loyalty_transaction`, `silver.guest_profile`, `silver.time_punch`, `ref.unit`.

```sql
-- Applied automatically when querying filtered tables:
SELECT * FROM jmrdemo.synth_silver.guest_order
-- Returns only rows where franchisee_id matches caller's group membership
```
