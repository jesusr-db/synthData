# Data Model

All schema names use the `schema_prefix` variable (default `synth_`). Examples below use the default prefix.

## Unity Catalog Layout

```
{catalog}
├── synth_staging    — raw event tables (written by generator)
├── synth_ref        — reference/dimension tables (seeded at setup)
├── synth_silver     — cleaned domain tables + gold aggregates (DLT-managed)
└── synth_metrics    — UC metric views + Lakehouse Monitor output tables
```

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
| `first_name` | STRING | PII — masked for non-admin |
| `last_name` | STRING | PII — masked for non-admin |
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
| `unit` | 250 restaurant units — `unit_id`, `unit_name`, `city`, `state`, `franchisee_id`, `unit_volume_bias`, `market_price_index` | Seeded deterministically (`seed=42`). Row filter `filter_by_franchisee` applied. |
| `franchisee` | Franchisee entities — `franchisee_id`, `franchisee_name`, `contact_email`, `status` | Derived from unit seed |
| `financial_period` | Monthly periods — `financial_period_id`, `period_name`, `start_date`, `end_date`, `fiscal_year`, `fiscal_quarter`, `status` | |
| `menu_item` | Menu catalog — `menu_item_id`, name, category, daypart, base price | Also exported to `ref.assets` volume as CSV |
| `recipe_ingredient` | Bill of materials — `menu_item_id` → `stock_sku` mapping | |
| `item_price` | Per `(menu_item_id, financial_period_id)` price multiplier, drifts ±3-6%/quarter | |
| `supplier` | 6 suppliers — `supplier_id`, `supplier_name`, `category`, `status` | |
| `weather_conditions` | Stub (Phase 2) — `stub_id`, `placeholder` | Empty |
| `local_events` | Stub (Phase 2) — `stub_id`, `placeholder` | Empty |

### UC Functions in `synth_ref`

| Function | Signature | Purpose |
|---|---|---|
| `mask_email(email STRING)` | `RETURNS STRING` | Masks all chars before `@` except first: `j***@example.com` |
| `mask_phone(phone STRING)` | `RETURNS STRING` | Masks all digits except last 4 |
| `tier_to_multiplier(tier STRING)` | `RETURNS DOUBLE` | bronze=1.0, silver=1.5, gold=2.0, elite=3.0 |
| `filter_by_franchisee(franchisee_id BIGINT)` | `RETURNS BOOLEAN` | True if caller is in `franchisee_{id}` group or `qsr_admin` |

---

## Metric Views (`synth_metrics`)

Unity Catalog metric views defined with `WITH METRICS LANGUAGE YAML`. These expose named measures and dimensions for ad-hoc slicing without rewriting SQL.

| View | Source Table | Dimensions | Key Measures |
|---|---|---|---|
| `order_performance` | `silver.guest_order` | Unit ID, Channel, Order Type, Order Status, Order Date, Order Month | Total Orders, Total Revenue, Average Order Value, Fulfilled Orders, Cancelled Orders, Total Discount, SOS Breach Rate |
| `loyalty_performance` | `silver.loyalty_transaction` | Tier, Transaction Type, Unit ID, Transaction Month | Unique Members, Total Transactions, Points Earned, Points Redeemed |
| `inventory_waste` | `silver.waste_log` | Unit ID, Stock SKU, Waste Category, Waste Week, Waste Month | Total Waste Quantity, Total Waste Cost, Waste Events, Avg Waste Cost per Event |
| `staff_hours` | `silver.time_punch` | Unit ID, Shift Date, Shift Month | Total Hours Worked, Total Shifts, Unique Employees, Avg Hours per Shift |

Lakehouse Monitor output tables (profile and drift) land in `synth_metrics` for the three monitored staging tables: `order_events`, `inventory_events`, `loyalty_events`.
