# Data Model

All schema names use the `schema_prefix` variable (default `synth_`). Examples below use the default prefix.

## Config Property → Schema/Table Mapping

| Config property | Default | Resolves to |
|---|---|---|
| `catalog_name` | `jmrdemo` | UC catalog root for every object below |
| `schema_prefix` | `synth_` | Prefix prepended to every schema name |
| → `{prefix}staging` | `synth_staging` | Raw event tables (written by generator) |
| → `{prefix}ref` | `synth_ref` | Reference/dimension tables + weather/events + UC functions + `assets` volume |
| → `{prefix}silver` | `synth_silver` | Cleaned domain tables + co-located gold aggregates (DLT-managed); also the pipeline `target` |
| → `{prefix}metrics` | `synth_metrics` | UC metric views, `demand_risk_forecast` view, Lakehouse Monitor output tables |
| → `{prefix}features` | `synth_features` | Customer + store feature tables (online-table backed) |
| `num_units` | `250` | Row count of `ref.unit` |

## Unity Catalog Layout

```
{catalog}
├── synth_staging    — raw event tables (written by generator)
├── synth_ref        — reference/dimension tables + weather/events (seeded/refreshed)
├── synth_silver     — cleaned domain tables + gold aggregates (DLT-managed)
├── synth_metrics    — UC metric views + demand_risk_forecast view + monitor output
└── synth_features   — customer + store feature tables (online-table backed)
```

> **Live PG/INFORMATION_SCHEMA note:** this project does not synchronize any objects to a Lakebase / PostgreSQL instance — all persisted state is Delta / Unity Catalog. The schemas below are derived from the SQL/source files, not from a live `information_schema.columns` dump (which was unavailable at regeneration time).
<!-- TODO: human narrative needed — confirm exact column lists/types for ref.weather_conditions, ref.local_events, synth_features.customer_features, and synth_features.store_features against the live catalog -->

---

## Staging Tables (`synth_staging`)

Wide, sparse schema. All columns not relevant to a given `event_type` are NULL by design.

### `order_events`

| Column | Type | Event types that populate it |
|---|---|---|
| `event_type` | STRING | all |
| `event_id` | BIGINT | all |
| `unit_id` | BIGINT | all |
| `event_ts` | TIMESTAMP | all |
| `guest_order_id` | BIGINT | guest_order, order_item, payment, status_event, delivery_order |
| `order_item_id` | BIGINT | order_item |
| `payment_id` | BIGINT | payment |
| `status_event_id` | BIGINT | status_event |
| `delivery_order_id` | BIGINT | delivery_order |
| `channel` | STRING | guest_order |
| `order_type` | STRING | guest_order |
| `order_status` | STRING | guest_order |
| `profile_id` | BIGINT | guest_order |
| `member_id` | BIGINT | guest_order |
| `subtotal` | DOUBLE | guest_order |
| `discount_amount` | DOUBLE | guest_order |
| `tax_amount` | DOUBLE | guest_order |
| `total_amount` | DOUBLE | guest_order |
| `placed_at` | TIMESTAMP | guest_order, order_item |
| `ready_at` | TIMESTAMP | guest_order |
| `fulfilled_at` | TIMESTAMP | guest_order |
| `cancelled_at` | TIMESTAMP | guest_order |
| `financial_period_id` | BIGINT | guest_order |
| `sos_breach` | BOOLEAN | guest_order |
| `menu_item_id` | BIGINT | order_item |
| `quantity` | BIGINT | order_item |
| `unit_price` | DOUBLE | order_item |
| `line_gross_amount` | DOUBLE | order_item |
| `line_net_amount` | DOUBLE | order_item |
| `line_discount_amount` | DOUBLE | order_item |
| `item_status` | STRING | order_item |
| `waste_flag` | BOOLEAN | order_item |
| `tender_type` | STRING | payment |
| `amount` | DOUBLE | payment |
| `settlement_date` | STRING | payment |
| `paid_at` | TIMESTAMP | payment |
| `prior_state` | STRING | status_event |
| `current_state` | STRING | status_event |
| `event_timestamp` | TIMESTAMP | status_event |
| `elapsed_seconds_in_prior_state` | BIGINT | status_event |
| `sos_target_seconds` | BIGINT | status_event |
| `is_sos_breach` | BOOLEAN | status_event |
| `platform_order_reference` | STRING | delivery_order |
| `estimated_delivery_seconds` | BIGINT | delivery_order |
| `actual_delivery_seconds` | BIGINT | delivery_order |
| `delivery_status` | STRING | delivery_order |

### `inventory_events`

| Column | Type | Event types that populate it |
|---|---|---|
| `event_type` | STRING | all |
| `event_id` | BIGINT | all |
| `unit_id` | BIGINT | all |
| `event_ts` | TIMESTAMP | all |
| `on_hand_balance_id` | BIGINT | on_hand_balance |
| `waste_log_id` | BIGINT | waste_log |
| `receiving_order_id` | BIGINT | receiving_order |
| `replenishment_order_id` | BIGINT | replenishment_order |
| `stock_sku` | STRING | on_hand_balance, waste_log, receiving_order, replenishment_order |
| `quantity_on_hand` | DOUBLE | on_hand_balance |
| `quantity_reserved` | DOUBLE | on_hand_balance |
| `par_level` | DOUBLE | on_hand_balance |
| `snapshot_at` | TIMESTAMP | on_hand_balance |
| `waste_quantity` | DOUBLE | waste_log |
| `waste_category` | STRING | waste_log |
| `waste_cost` | DOUBLE | waste_log |
| `logged_at` | TIMESTAMP | waste_log |
| `received_quantity` | DOUBLE | receiving_order |
| `delivery_date` | STRING | receiving_order |
| `quality_inspection_result` | STRING | receiving_order |
| `temperature_check_pass` | BOOLEAN | receiving_order |
| `order_type` | STRING | replenishment_order |
| `order_quantity` | DOUBLE | replenishment_order |
| `order_status` | STRING | replenishment_order |
| `ordered_at` | TIMESTAMP | replenishment_order |

### `guest_events`

| Column | Type | Notes |
|---|---|---|
| `event_type` | STRING | guest_profile |
| `event_id` | BIGINT | |
| `unit_id` | BIGINT | |
| `event_ts` | TIMESTAMP | |
| `guest_profile_id` | BIGINT | |
| `digital_account_id` | BIGINT | |
| `first_name` | STRING | PII — `class.*` tagged |
| `last_name` | STRING | PII — `class.*` tagged |
| `email` | STRING | PII — column mask applied |
| `phone` | STRING | PII — column mask applied |
| `zip_code` | STRING | PII |
| `created_date` | STRING | |
| `account_status` | STRING | active, inactive, suspended |

### `loyalty_events`

| Column | Type | Event types that populate it |
|---|---|---|
| `event_type` | STRING | all |
| `event_id` | BIGINT | all |
| `unit_id` | BIGINT | all |
| `event_ts` | TIMESTAMP | all |
| `loyalty_transaction_id` | BIGINT | loyalty_transaction |
| `reward_redemption_id` | BIGINT | reward_redemption |
| `member_id` | BIGINT | loyalty_transaction, reward_redemption |
| `guest_order_id` | BIGINT | loyalty_transaction, reward_redemption |
| `transaction_type` | STRING | loyalty_transaction — `earn` or `redeem` |
| `points_delta` | BIGINT | loyalty_transaction |
| `transaction_at` | TIMESTAMP | loyalty_transaction |
| `tier` | STRING | loyalty_transaction — bronze, silver, gold, elite |
| `points_redeemed` | BIGINT | reward_redemption |
| `reward_value` | DOUBLE | reward_redemption |
| `redeemed_at` | TIMESTAMP | reward_redemption |

### `workforce_events`

| Column | Type | Event types that populate it |
|---|---|---|
| `event_type` | STRING | all |
| `event_id` | BIGINT | all |
| `unit_id` | BIGINT | all |
| `event_ts` | TIMESTAMP | all |
| `shift_id` | BIGINT | shift |
| `time_punch_id` | BIGINT | time_punch |
| `employee_id` | BIGINT | shift, time_punch |
| `shift_label` | STRING | shift |
| `shift_start` | TIMESTAMP | shift |
| `shift_end` | TIMESTAMP | shift |
| `status` | STRING | shift |
| `date` | STRING | shift |
| `punch_in` | TIMESTAMP | time_punch |
| `punch_out` | TIMESTAMP | time_punch |
| `hours_worked` | DOUBLE | time_punch |

---

## Silver Tables (`synth_silver`) — DLT-Managed

All silver tables include a `created_at TIMESTAMP` column set to `current_timestamp()` at write time. Tables marked ⭐ include `franchisee_id` via broadcast join from `ref.unit`.

### Orders Domain

**`guest_order`** ⭐ — `@dp.expect_or_drop("valid_total", "total_amount >= 0")`, `@dp.expect_or_drop("valid_unit", "unit_id IS NOT NULL")`

| Column | Type | Description |
|---|---|---|
| `guest_order_id` | BIGINT | Surrogate PK |
| `unit_id` | BIGINT | Restaurant unit |
| `franchisee_id` | BIGINT | From ref.unit broadcast join |
| `channel` | STRING | carryout, own_delivery, 3pd_delivery, catering |
| `order_type` | STRING | dine_in, takeout, delivery |
| `order_status` | STRING | placed, in_progress, ready, fulfilled, cancelled |
| `profile_id` | BIGINT | FK guest_profile; null for anonymous |
| `member_id` | BIGINT | |
| `subtotal` | DOUBLE | Pre-discount, pre-tax (USD) |
| `discount_amount` | DOUBLE | Promotions/coupons applied (USD) |
| `tax_amount` | DOUBLE | Tax charged (USD) |
| `total_amount` | DOUBLE | Total revenue (USD) |
| `placed_at` | TIMESTAMP | |
| `ready_at` | TIMESTAMP | |
| `fulfilled_at` | TIMESTAMP | |
| `cancelled_at` | TIMESTAMP | |
| `financial_period_id` | BIGINT | |
| `sos_breach` | BOOLEAN | Exceeded speed-of-service target |
| `created_at` | TIMESTAMP | |

**`order_item`** — `@dp.expect_or_drop("positive_price", "unit_price > 0")`

| Column | Type | Description |
|---|---|---|
| `order_item_id` | BIGINT | Surrogate PK |
| `guest_order_id` | BIGINT | FK guest_order |
| `unit_id` | BIGINT | |
| `menu_item_id` | BIGINT | |
| `quantity` | INT | |
| `unit_price` | DOUBLE | |
| `line_gross_amount` | DOUBLE | |
| `line_net_amount` | DOUBLE | |
| `line_discount_amount` | DOUBLE | |
| `item_status` | STRING | fulfilled, cancelled, refunded |
| `waste_flag` | BOOLEAN | Item later flagged as waste |
| `placed_at` | TIMESTAMP | |
| `created_at` | TIMESTAMP | |

**`payment`**

| Column | Type |
|---|---|
| `payment_id` | BIGINT |
| `guest_order_id` | BIGINT |
| `unit_id` | BIGINT |
| `tender_type` | STRING |
| `amount` | DOUBLE |
| `settlement_date` | STRING |
| `paid_at` | TIMESTAMP |
| `created_at` | TIMESTAMP |

**`status_event`**

| Column | Type | Description |
|---|---|---|
| `status_event_id` | BIGINT | |
| `guest_order_id` | BIGINT | FK guest_order |
| `unit_id` | BIGINT | |
| `prior_state` | STRING | |
| `current_state` | STRING | |
| `event_timestamp` | TIMESTAMP | |
| `elapsed_seconds_in_prior_state` | INT | |
| `sos_target_seconds` | INT | |
| `is_sos_breach` | BOOLEAN | |
| `created_at` | TIMESTAMP | |

**`delivery_order`**

| Column | Type |
|---|---|
| `delivery_order_id` | BIGINT |
| `guest_order_id` | BIGINT |
| `unit_id` | BIGINT |
| `platform_order_reference` | STRING |
| `estimated_delivery_seconds` | INT |
| `actual_delivery_seconds` | INT |
| `delivery_status` | STRING |
| `created_at` | TIMESTAMP |

### Inventory Domain

**`on_hand_balance`** — `@dp.expect_or_drop("nonnegative_quantity", "quantity_on_hand >= 0")`

| Column | Type |
|---|---|
| `on_hand_balance_id` | BIGINT |
| `unit_id` | BIGINT |
| `stock_sku` | STRING |
| `quantity_on_hand` | DOUBLE |
| `quantity_reserved` | DOUBLE |
| `par_level` | DOUBLE |
| `snapshot_at` | TIMESTAMP |
| `created_at` | TIMESTAMP |

**`waste_log`** ⭐

| Column | Type | Description |
|---|---|---|
| `waste_log_id` | BIGINT | |
| `unit_id` | BIGINT | |
| `franchisee_id` | BIGINT | From ref.unit broadcast join |
| `stock_sku` | STRING | |
| `waste_quantity` | DOUBLE | |
| `waste_category` | STRING | spoilage, over_prep, damage, expiry |
| `waste_cost` | DOUBLE | (USD) |
| `logged_at` | TIMESTAMP | |
| `created_at` | TIMESTAMP | |

**`receiving_order`**

| Column | Type |
|---|---|
| `receiving_order_id` | BIGINT |
| `unit_id` | BIGINT |
| `stock_sku` | STRING |
| `received_quantity` | DOUBLE |
| `delivery_date` | STRING |
| `quality_inspection_result` | STRING |
| `temperature_check_pass` | BOOLEAN |
| `created_at` | TIMESTAMP |

**`replenishment_order`**

| Column | Type |
|---|---|
| `replenishment_order_id` | BIGINT |
| `unit_id` | BIGINT |
| `stock_sku` | STRING |
| `order_type` | STRING |
| `order_quantity` | DOUBLE |
| `order_status` | STRING |
| `ordered_at` | TIMESTAMP |
| `created_at` | TIMESTAMP |

### Guest Domain

**`guest_profile`** ⭐ — populated via `dp.create_auto_cdc_flow` (SCD Type 1, keyed on `guest_profile_id`)

| Column | Type | Description |
|---|---|---|
| `guest_profile_id` | BIGINT | Surrogate PK |
| `unit_id` | BIGINT | |
| `franchisee_id` | BIGINT | From ref.unit broadcast join (in source view) |
| `first_name` | STRING | PII |
| `last_name` | STRING | PII |
| `email` | STRING | PII — column mask applied |
| `phone` | STRING | PII — column mask applied |
| `zip_code` | STRING | PII |
| `created_date` | STRING | |
| `account_status` | STRING | active, inactive, suspended |
| `created_at` | TIMESTAMP | |

**`digital_account`**

| Column | Type |
|---|---|
| `digital_account_id` | BIGINT |
| `guest_profile_id` | BIGINT |
| `account_status` | STRING |
| `created_date` | STRING |
| `created_at` | TIMESTAMP |

### Loyalty Domain

**`loyalty_transaction`** ⭐

| Column | Type | Description |
|---|---|---|
| `loyalty_transaction_id` | BIGINT | |
| `member_id` | BIGINT | FK guest_profile |
| `guest_order_id` | BIGINT | |
| `unit_id` | BIGINT | |
| `franchisee_id` | BIGINT | From ref.unit broadcast join |
| `transaction_type` | STRING | earn or redeem |
| `points_delta` | INT | Positive = earn, negative = redeem |
| `transaction_at` | TIMESTAMP | |
| `tier` | STRING | bronze, silver, gold, elite |
| `created_at` | TIMESTAMP | |

**`reward_redemption`**

| Column | Type |
|---|---|
| `reward_redemption_id` | BIGINT |
| `member_id` | BIGINT |
| `guest_order_id` | BIGINT |
| `unit_id` | BIGINT |
| `points_redeemed` | INT |
| `reward_value` | DOUBLE |
| `redeemed_at` | TIMESTAMP |
| `created_at` | TIMESTAMP |

### Workforce Domain

**`shift`**

| Column | Type |
|---|---|
| `shift_id` | BIGINT |
| `unit_id` | BIGINT |
| `employee_id` | BIGINT |
| `shift_label` | STRING |
| `shift_start` | TIMESTAMP |
| `shift_end` | TIMESTAMP |
| `status` | STRING |
| `date` | STRING |
| `created_at` | TIMESTAMP |

**`time_punch`** ⭐

| Column | Type | Description |
|---|---|---|
| `time_punch_id` | BIGINT | |
| `employee_id` | BIGINT | |
| `unit_id` | BIGINT | |
| `franchisee_id` | BIGINT | From ref.unit broadcast join |
| `punch_in` | TIMESTAMP | |
| `punch_out` | TIMESTAMP | |
| `hours_worked` | DOUBLE | |
| `created_at` | TIMESTAMP | |

---

## Gold Tables (`synth_silver`) — DLT-Managed, co-located with Silver

| Table | Source(s) | Key Columns |
|---|---|---|
| `unit_performance_daily` | `guest_order` | `unit_id`, `date`, `order_count`, `daily_revenue`, `avg_order_value`, `cancelled_count` |
| `sos_compliance_summary` | `status_event` + `guest_order` | `unit_id`, `channel`, `date`, `total_orders`, `sos_breaches`, `avg_prep_seconds`, `sos_compliance_pct` |
| `loyalty_cohort_metrics` | `loyalty_transaction` | `unit_id`, `tier`, `date`, `active_members`, `total_points_earned`, `transaction_count` |
| `inventory_waste_summary` | `waste_log` | `unit_id`, `date`, `waste_category`, `total_waste_cost`, `total_waste_qty`, `waste_event_count` |

---

## Reference Tables (`synth_ref`)

| Table | Contents | Notes |
|---|---|---|
| `unit` | 250 restaurant units — `unit_id`, `unit_name`, `city`, `state`, `metro_area`, `lat`, `lon`, `franchisee_id`, `unit_volume_bias`, `market_price_index` | Seeded deterministically (`seed=42`). Row filter `filter_by_franchisee` applied. Weather refresh reads distinct `(metro_area, state, AVG(lat), AVG(lon))` from here. **Coordinates are `lat`/`lon`, not `latitude`/`longitude`.** |
| `franchisee` | Franchisee entities — `franchisee_id`, `franchisee_name`, `contact_email`, `status` | Derived from unit seed |
| `financial_period` | Monthly periods — `financial_period_id`, `period_name`, `start_date`, `end_date`, `fiscal_year`, `fiscal_quarter`, `status` | |
| `menu_item` | Menu catalog — `menu_item_id`, name, category, daypart, base price | Also exported to `ref.assets` volume as CSV |
| `recipe_ingredient` | Bill of materials — `menu_item_id` → `stock_sku` mapping | |
| `item_price` | Per `(menu_item_id, financial_period_id)` price multiplier, drifts ±3-6%/quarter | |
| `supplier` | 6 suppliers — `supplier_id`, `supplier_name`, `category`, `status` | |
| `weather_conditions` | Populated by `weather_events_refresh_job` (no longer a Phase-2 stub) | See schema below |
| `local_events` | Populated by `weather_events_refresh_job` (no longer a Phase-2 stub) | See schema below |

### `weather_conditions` (`synth_ref`)

Populated by `src/refresh/refresh_notebook.py` via `MERGE INTO`. Roughly 880 rows (≈20 metros × ≈44 days: ~30 back + ~14 forward).

| Column | Type | Source attribute / key |
|---|---|---|
| `metro_area` | STRING | from `ref.unit` distinct metro |
| `state` | STRING | from `ref.unit` |
| `forecast_date` | DATE | Open-Meteo daily forecast/observation date (MERGE key) |
| `temp_max` | DOUBLE | Open-Meteo daily max (°F) |
| `temp_min` | DOUBLE | Open-Meteo daily min (°F) |
| `condition` | STRING | WMO-code-derived; overridden to `extreme_heat` (temp_max > 100) / `extreme_cold` (temp_min < 15) |
| `demand_multiplier` | DOUBLE | Derived demand effect from weather (see `conf/weather_event_multipliers.yml`) |

<!-- TODO: human narrative needed — confirm full weather_conditions column list (NOAA alert overlay fields, lat/lon, wmo_code) and exact types from the live catalog -->

### `local_events` (`synth_ref`)

Populated from Nager.Date (holidays, unconditional), Ticketmaster (key-gated), and SeatGeek (key-gated). At minimum ~28 holiday rows (current + next year). Deduplicated by `event_id`.

| Column | Type | Source attribute / key |
|---|---|---|
| `event_id` | STRING | 16-char SHA-256 prefix of `(source, metro, date, name)` (MERGE key) |
| `source` | STRING | `nager`, `ticketmaster`, or `seatgeek` |
| `metro_area` | STRING | event metro |
| `event_date` | DATE | event date |
| `event_name` | STRING | event/holiday name |
| `event_category` | STRING | category (holiday, concert, sports, …) |
| `demand_multiplier` | DOUBLE | Derived demand effect (see `conf/weather_event_multipliers.yml`) |

<!-- TODO: human narrative needed — confirm full local_events column list and exact types from the live catalog -->

### UC Functions in `synth_ref`

| Function | Signature | Purpose |
|---|---|---|
| `mask_email(email STRING)` | `RETURNS STRING` | Masks all chars before `@` except first: `j***@example.com` |
| `mask_phone(phone STRING)` | `RETURNS STRING` | Masks all digits except last 4 |
| `tier_to_multiplier(tier STRING)` | `RETURNS DOUBLE` | bronze=1.0, silver=1.5, gold=2.0, elite=3.0 |
| `filter_by_franchisee(franchisee_id BIGINT)` | `RETURNS BOOLEAN` | True if caller is in `franchisee_{id}` group or `qsr_admin` |

### UC Volume in `synth_ref`

`ref.assets` — UC managed volume created by `apply_governance.py`. Holds the exported `menu_catalog_csv/`, `franchise_locations_csv/` (Spark part-file directories, not single `.csv` files), and a sample receipt asset.

---

## Feature Tables (`synth_features`)

Schema `{catalog}.{prefix}features`, built by `src/setup/build_feature_tables.py`. Each table is backed by an Online Table (billable). Map-typed columns are serialized to JSON strings because Online Tables do not support `MAP<...>` types.

### `customer_features` — PK `profile_id`

Per-guest aggregates from silver. Join key is `guest_order.profile_id` (not `guest_profile.guest_profile_id`).

| Column | Type | Source |
|---|---|---|
| `profile_id` | BIGINT | PK — `guest_order.profile_id` |
| (order frequency, avg spend, category mix, loyalty tier aggregates) | — | derived from silver `guest_order` / `loyalty_transaction` |

<!-- TODO: human narrative needed — confirm exact customer_features column names and types from build_feature_tables.py / live catalog -->

### `store_features` — PK `unit_id`

Per-store aggregates.

| Column | Type | Source |
|---|---|---|
| `unit_id` | BIGINT | PK |
| `popularity` | STRING (JSON) | per-item popularity map serialized with `json.dumps` |
| `top_item_per_category` | STRING (JSON) | per-category top item map serialized with `json.dumps` |
| (avg daily volume, category distribution) | — | derived from silver `guest_order` / `order_item` |

<!-- TODO: human narrative needed — confirm exact store_features column names and types from build_feature_tables.py / live catalog -->

---

## Metric Views & Views (`synth_metrics`)

Unity Catalog metric views defined with `WITH METRICS LANGUAGE YAML`. These expose named measures and dimensions for ad-hoc slicing without rewriting SQL.

| View | Source Table | Dimensions | Key Measures |
|---|---|---|---|
| `order_performance` | `silver.guest_order` | Unit ID, Channel, Order Type, Order Status, Order Date, Order Month | Total Orders, Total Revenue, Average Order Value, Fulfilled Orders, Cancelled Orders, Total Discount, SOS Breach Rate |
| `loyalty_performance` | `silver.loyalty_transaction` | Tier, Transaction Type, Unit ID, Transaction Month | Unique Members, Total Transactions, Points Earned, Points Redeemed |
| `inventory_waste` | `silver.waste_log` | Unit ID, Stock SKU, Waste Category, Waste Week, Waste Month | Total Waste Quantity, Total Waste Cost, Waste Events, Avg Waste Cost per Event |
| `staff_hours` | `silver.time_punch` | Unit ID, Shift Date, Shift Month | Total Hours Worked, Total Shifts, Unique Employees, Avg Hours per Shift |

### `demand_risk_forecast` (standard view)

A standard view (not a metric view) joining `silver`/`ref.unit` against `ref.weather_conditions`. Returns zero rows until `initial_weather_refresh` has populated the weather table. Approx `num_units × 13 forecast days` rows.

| Column | Notes |
|---|---|
| `risk_level` | `normal` (avg multiplier ~0.97) or `demand_risk` (avg multiplier ~0.61) |
| `demand_multiplier` | composite weather/event demand effect |

<!-- TODO: human narrative needed — confirm full demand_risk_forecast column list (unit_id, forecast_date, weather/event drivers) from create_metric_views.py -->

Lakehouse Monitor output tables (profile and drift) also land in `synth_metrics` for the three monitored staging tables: `order_events`, `inventory_events`, `loyalty_events`.
