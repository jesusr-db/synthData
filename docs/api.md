# API Reference

This project has no HTTP REST API of its own (aside from the Databricks Model Serving endpoints described below). "API" here covers four surfaces: **job parameters** (how to configure the Databricks jobs), **metric view interface** (how to query the UC metric views), **governance functions** (UC scalar and row filter functions callable from SQL), and the **recommender / feature serving endpoints**.

> This project does not expose application routers — there are no FastAPI/Flask router modules in the source tree. The serving endpoints below are Databricks Model Serving / Feature Serving endpoints, not in-repo routers.

---

## Job Parameters

All parameters are declared as Databricks Asset Bundle variables in `databricks.yml` and passed to notebooks as job widgets.

### Common Parameters (most jobs)

| Parameter | Default | Description |
|---|---|---|
| `catalog_name` | `jmrdemo` | Unity Catalog catalog. Must exist before setup runs. |
| `schema_prefix` | `synth_` | Prefix for all UC schemas: `{prefix}staging`, `{prefix}ref`, `{prefix}silver`, `{prefix}metrics`, `{prefix}features`. Use `""` for no prefix. |

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
| `mode` | `live` / `backfill` | `backfill` (setup task) generates a historical window; `live` (generator task) generates the previous hour. |
| `schema_prefix` | `synth_` | |

### `setup_job` — task: `initial_weather_refresh` (and `weather_events_refresh_job` — task: `refresh_weather_events`)

Source: `src/refresh/refresh_notebook.py`

| Parameter | Default | Description |
|---|---|---|
| `catalog_name` | `jmrdemo` | |
| `schema_prefix` | `synth_` | |
| `ticketmaster_secret_scope` | `qsr-synth` | Secret scope holding `ticketmaster_consumer_key`. Skipped silently if missing. |
| `ticketmaster_secret_key` | `ticketmaster_consumer_key` | Secret key name. |
| `seatgeek_secret_scope` | `qsr-synth` | Secret scope holding `seatgeek_client_id`. Skipped silently if missing. |
| `seatgeek_secret_key` | `seatgeek_client_id` | Secret key name. |

### `setup_job` — task: `start_pipeline`

Source: `src/setup/start_pipeline_notebook.py`

| Parameter | Default | Description |
|---|---|---|
| `catalog_name` | `jmrdemo` | |
| `schema_prefix` | `synth_` | |

### `setup_job` — task: `build_feature_tables` (and `feature_refresh_job` — task: `refresh_features`)

Source: `src/setup/build_feature_tables.py` (env: `ml`)

| Parameter | Default | Description |
|---|---|---|
| `catalog_name` | `jmrdemo` | |
| `schema_prefix` | `synth_` | |

### `setup_job` — task: `train_recommender`

Source: `src/ml/train_recommender.py` (env: `ml`)

| Parameter | Default | Description |
|---|---|---|
| `catalog_name` | `jmrdemo` | |
| `schema_prefix` | `synth_` | |
| `recommender_query_principal` | `""` | SP/principal granted `CAN_QUERY` on the recommender endpoint. Empty = skip the grant. |

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

### `setup_job` — task: `apply_ontos`

Source: `src/setup/apply_ontos.py` (env: `refresh`)

| Parameter | Default | Description |
|---|---|---|
| `catalog_name` | `jmrdemo` | |
| `schema_prefix` | `synth_` | |
| `ontos_app_url` | `https://ontos-7405605519549535.15.azure.databricksapps.com` | Base URL of the deployed ontos app. |
| `ontos_enabled` | `true` | `false` → notebook exits immediately, no ontos API calls. |

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
| `ontos_app_url` | `https://ontos-...` | ontos base URL for teardown of registered schemas/links. |
| `ontos_enabled` | `true` | `false` → skip ontos teardown. Feature store/recommender teardown (Step 0h) runs unconditionally. |

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

### `{catalog}.{prefix}metrics.demand_risk_forecast` (standard view)

Source: `ref.weather_conditions` joined against unit/forecast. Standard view (`SELECT`-able as raw rows), not a metric view. Returns 0 rows until `initial_weather_refresh` populates the weather table. Key columns: `risk_level` (`normal` / `demand_risk`), `demand_multiplier`.

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

---

## Serving Endpoints

Two Databricks serving endpoints are provisioned during setup (names track `schema_prefix`; default prefix `synth_`).

### `synth_qsr-customer-features` — Feature Serving

Online lookup for `{prefix}features.customer_features` (key `profile_id`) and `{prefix}features.store_features` (key `unit_id`). Called internally by the recommender pyfunc at inference time; not intended for direct external calls.

### `synth_qsr-recommender` — Model Serving

PizzaTel-facing recommendation endpoint.

**Method / path:** `POST https://<workspace-host>/serving-endpoints/synth_qsr-recommender/invocations`
**Auth:** `Authorization: Bearer <PAT or OAuth token with CAN_QUERY>`

**Request** — `dataframe_records` envelope, one record per call:

```json
{
  "dataframe_records": [
    {
      "profile_id": 1234,
      "member_id": 5678,
      "store_id": 42,
      "cart_product_ids": [1, 14],
      "viewed_product_id": 8,
      "num_recommendations": 5
    }
  ]
}
```

All IDs are integers. `cart_product_ids` may be empty. `viewed_product_id` and `member_id` are nullable. `num_recommendations` defaults to 5 (clamped 1–10). Unknown `profile_id` → cold-start (store-popularity fallback, `personalized: false`).

**Response** — `predictions` envelope:

```json
{
  "predictions": [
    {
      "personalized": true,
      "recommendations": [
        {"menu_item_id": 53, "score": 0.94, "item_name": "20oz Coca-Cola", "category": "drinks", "subcategory": "soda", "reason": "..."}
      ]
    }
  ]
}
```

`recommendations` excludes any item in `cart_product_ids` (and `viewed_product_id`), is sorted by `score` descending, and has length ≤ `num_recommendations`. The website consumes `predictions[0].recommendations[*].menu_item_id` + `score`.

**Notes:**
- The serving signature includes FE `RequestSource` columns (scalar only); `cart_product_ids`/`member_id`/`viewed_product_id`/`num_recommendations` are threaded through the feature training set so the basket signal reaches the pyfunc.
- Endpoint create/update and delete use **raw REST via `api_client.do()`**, not the SDK `serving_endpoints` wrapper (unreliable in serverless). See [gotchas.md](gotchas.md).
- `CAN_QUERY` is granted only when `recommender_query_principal` is non-empty.

### `synth_qsr-commerce-agent` — Model Serving (ResponsesAgent)

Conversational ordering agent. PizzaTel-facing; called once per chat turn.

**Method / path:** `POST https://<workspace-host>/serving-endpoints/synth_qsr-commerce-agent/invocations`
**Auth:** `Authorization: Bearer <PAT or OAuth token with CAN_QUERY>`

**Request** — Responses input shape; stateless (web resends full history each turn):

```json
{
  "input": [{"role": "user", "content": "I want two pepperoni pizzas for the game"}],
  "custom_inputs": {
    "profile_id": 1234, "member_id": 5678, "store_id": 42,
    "app_trace_context": "00-<traceid>-<spanid>-01"
  }
}
```

`profile_id` may be the string `"guest"` (cold-start path). `app_trace_context` is a W3C `traceparent` carried in the payload (not an HTTP header — Model Serving may strip headers).

**Response** — assistant text plus structured outputs:

```json
{
  "output": [{"type": "message", "role": "assistant",
              "content": [{"type": "output_text", "text": "Here's your order — sound good?"}]}],
  "custom_outputs": {
    "propose_order": {"tool": "propose_order",
      "items": [{"menu_item_id": 1, "quantity": 2, "item_name": "Large Pepperoni", "unit_price": 14.99}],
      "order_type": "delivery", "subtotal": 29.98, "tax_estimate": 2.70, "total": 32.68,
      "currency": "USD", "pricing_note": "indicative — BFF is pricing authority at place_order"},
    "mlflow_trace_id": "tr-..."
  }
}
```

`propose_order` is present only on turns where the agent proposes an order. The agent never places the order — the web BFF executes `place_order` after the customer approves. Prices are indicative; the BFF is the pricing authority at placement.

**Model access:** the agent reaches its LLM only via the AI-Gateway endpoint `synth_qsr-agent-llm` (usage tracking, rate limits, PII guardrails). Endpoint create/update/delete use raw REST via `api_client.do()`.
