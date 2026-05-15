# QSR Synthetic Data Generator — Design Spec
**Date:** 2026-05-15  
**Author:** jesus.rodriguez@databricks.com  
**Data model:** [vibe-business-data-models/restaurants/mvm_v1](https://github.com/amralieg/vibe-business-data-models/tree/main/restaurants/mvm_v1)  
**Scope:** MVM (153 tables, 13 domains)  
**Brand archetype:** Domino's Pizza (delivery + carryout QSR, franchise-heavy)

---

## 1. Purpose

A fully automated synthetic data generator deployable on Databricks that produces realistic, referentially consistent restaurant operations data mimicking a national quick-service pizza brand. Data streams continuously into Unity Catalog MVM tables via Structured Streaming, suitable for live demos, analytics/BI development, and ML model training.

---

## 2. Architecture

### 2.1 Pattern
**Domain-partitioned staging + Spark Declarative Pipeline (DLT).** A generator job writes synthetic events to domain-specific Bronze staging tables. A continuous DLT pipeline reads those tables via `readStream` and writes to all 153 MVM Silver tables with quality expectations. Gold metric tables are computed by DLT; UC Metric Views sit on top of Gold.

```
External Signal (Phase 2)
        │
        ▼
┌──────────────────┐     ┌────────────────────────┐
│  Reference       │     │  Event Generator Job   │
│  Seeder (once)   │     │  (60s live / batch)    │
└────────┬─────────┘     └──────────┬─────────────┘
         │                          │ causal_context
         ▼                          ▼
    qsr_synth.ref          qsr_synth.staging
    (static entities)      (Bronze — per domain)
                                    │
                                    ▼
                          DLT Pipeline (continuous)
                          ├── Silver: 153 MVM tables
                          └── Gold: aggregated tables
                                    │
                                    ▼
                          UC Metric Views (setup_job)
```

### 2.2 Unity Catalog Layout

**Catalog:** `qsr_synth` (parameterized as `catalog_name`)

| Schema | Purpose | Managed by |
|--------|---------|------------|
| `staging` | Bronze event tables; generator writes here | Generator job |
| `silver` | All 153 MVM tables organized as sub-schemas by domain | DLT pipeline |
| `gold` | Pre-aggregated metric tables (unit performance, SOS, loyalty, waste) | DLT pipeline |
| `ref` | Seeded reference entities (units, menu, staff, GL, franchisees) | Setup job |

`ref.weather_conditions` and `ref.local_events` are created as empty stubs in Phase 1; populated in Phase 2.

### 2.3 Staging Tables (Bronze)

One Delta table per domain group, append-only, partitioned by `unit_id`:

- `staging.order_events`
- `staging.inventory_events`
- `staging.guest_events`
- `staging.loyalty_events`
- `staging.workforce_events`

---

## 3. Domain Coverage

### 3.1 Fully Streaming (live heartbeat)

| Domain | Key Tables Generated |
|--------|---------------------|
| **Order** | `guest_order`, `order_item`, `order_modifier`, `payment`, `status_event`, `delivery_order` |
| **Inventory** | `on_hand_balance`, `waste_log`, `receiving_order`, `replenishment_order`, `stock_transfer`, `adjustment` |
| **Guest** | `guest_profile`, `digital_account`, `guest_preference` |
| **Loyalty** | `member`, `loyalty_transaction`, `point_balance`, `reward_redemption` |
| **Workforce** | `shift`, `labor_assignment`, `time_punch` |

### 3.2 Reference / Slowly Changing

Seeded once by `setup_job`; updated periodically (menu LTOs monthly, prices quarterly):

**Restaurant** — `unit`, `location_profile`, `format_config`, `operating_hours`, `area_management`, `capacity_config`, `pos_terminal`  
**Menu** — `menu_item`, `menu`, `recipe`, `recipe_ingredient`, `item_price`, `nutrition_profile`, `allergen_declaration`  
**Finance** — `cost_center`, `profit_center`, `gl_account`, `financial_period`  
**Franchise** — `franchisee`, `territory`, `unit_ownership`  
**Procurement** — `supplier`, `purchase_order` (reference stubs, seeded once); `goods_receipt` records are created by the inventory domain generator to match `replenishment_order` events — not seeded statically  
**Food Safety** — `haccp_plan`, `critical_control_point`, `temperature_log` (periodic, 4× daily per unit)

### 3.3 Computed (DLT Gold)

`unit_performance` — derived from order + inventory + workforce actuals. Not directly generated.

### 3.4 Roadmap (Phase 2)

**Marketing** — campaigns, promotions, loyalty program config  
**Real Estate** — site, lease, property records

---

## 4. Referential Consistency

All generated events draw foreign keys from the **entity registry** — a set of `ref.*` tables containing every seeded ID. Key consistency guarantees:

- `guest_order` → valid `unit_id`, `channel_id`, `financial_period_id`; 40% of orders linked to a `profile_id` + `member_id`
- `order_item` → `menu_item_id` filtered to items available at that unit's channel and daypart
- `payment` → tender type consistent with guest loyalty membership status
- `loyalty_transaction` → same `guest_order_id` and `member_id`; points = f(order total, tier multiplier)
- `on_hand_balance` decrements driven by `recipe_ingredient` BOM for each `menu_item` sold
- `shift` → `unit_id` and real `employee_id`; staffing level correlates with projected order volume

A query joining guest → order → loyalty → inventory returns coherent, believable data end-to-end.

---

## 5. Demand Model

### 5.1 Time-of-Day Multipliers (Domino's pattern)
Lunch peak 11a–1p, dinner peak 6–9p, late-night tail 10p–1a. Each hour has a baseline multiplier applied to the per-unit base transaction rate.

### 5.2 Day-of-Week Multipliers

| Day | Multiplier |
|-----|-----------|
| Monday | 1.0× (baseline) |
| Tuesday | 1.1× |
| Wednesday | 1.2× |
| Thursday | 1.25× |
| Friday | 1.45× |
| Saturday | 1.6× |
| Sunday | 1.35× |

### 5.3 Special Event Multipliers (hardcoded calendar)

| Event | Multiplier |
|-------|-----------|
| Super Bowl Sunday | 3.2× |
| New Year's Eve | 2.3× |
| NFL Sunday (game day) | 2.0× |
| Halloween | 1.8× |
| Black Friday | 1.6× |
| Summer Fridays (Jun–Aug) | 1.4× |
| Christmas Eve | 1.1× |
| Christmas Day | 0.7× |
| January (post-holiday) | 0.85× monthly avg |

### 5.4 Channel Distribution (baseline)

| Channel | Share |
|---------|-------|
| 3PD Delivery | 40% |
| Own Delivery | 16% |
| Carryout | 40% |
| Catering | 4% |

No dine-in. Late-night hours shift +15pp toward delivery. Catering spikes around holidays.

### 5.5 Payment Tender Mix

| Tender | Share |
|--------|-------|
| Credit/Debit Card | 55% |
| Digital Wallet | 22% |
| Loyalty Redemption | 12% |
| Cash | 11% |

### 5.6 Entropy Injectors

- Order volume: ±15% Gaussian noise per hour per unit
- Prep time: carryout Normal(μ=12min, σ=3min); delivery Normal(μ=31min, σ=6min)
- SOS breach rate: ~8% baseline, spikes to 18% during event multipliers
- Cancelled orders: ~2.5% rate, higher on 3PD channel
- Waste events: ~3% of prep volume, skewed toward end-of-day
- Item price drift: quarterly ±3–6% per menu item
- Unit-level volume bias: each location assigned a persistent ±20% multiplier at seed time
- Staff no-show rate: ~4%, positively correlated with SOS breaches
- Loyalty tier drift: members progress based on cumulative spend
- New guest growth: ~0.8% net new profiles per day per location

---

## 6. Causal Model

### 6.1 Architecture

The generator uses a **Structural Causal Model (SCM)** implemented in `src/generator/causal_context.py`. Each variable is a function of its causal parents plus noise:

```
output = f(causal_parents) + ε
```

A `CausalContext` object is evaluated once per location per 60-second tick. All domain generators receive the same context object to ensure cross-domain consistency.

### 6.2 Causal Graph (Phase 1 — algorithmic inputs)

```
[hour_of_day, day_of_week] ──→ base_demand_multiplier
[holiday_flag, event_type] ──→ event_multiplier
[unit_volume_bias] ──────────→ location_multiplier
                                      │
                                      ▼
                             effective_order_volume
                                      │
              ┌───────────────────────┼───────────────────────┐
              ▼                       ▼                       ▼
       channel_mix              item_mix              prep_time_distribution
              │                       │                       │
              ▼                       ▼                       ▼
    delivery_order_rate      wing_spike (events)      sos_breach_rate
              │                                               │
              ▼                                               ▼
    cancellation_rate                              cancellation_rate (additive)
                                      │
                              inventory_depletion_rate
                                      │
                              replenishment_trigger
                                      │
                              waste_event_probability
```

### 6.3 Phase 2 Extension Points

The following `CausalContext` fields are stubbed as `None` in Phase 1 and populated by `daily_refresh_job` in Phase 2:

- `weather_condition` — affects channel mix (rain/snow → delivery spike)
- `precipitation_inches` — continuous variable for delivery demand model
- `temperature_f` — hot weather → delivery preference
- `local_event_type` — concert/game/festival within 3 miles → volume spike
- `local_event_attendance` — scales the event multiplier magnitude

**No changes to domain generators are required when Phase 2 is enabled.** Causal graph nodes that receive `None` fall back to Phase 1 defaults.

---

## 7. Deployment

### 7.1 Repository Structure

```
databricks.yml                    # root bundle
resources/
  pipeline.yml                    # DLT pipeline declaration
  generator_job.yml               # 60s live micro-batch job
  setup_job.yml                   # one-time setup (seeder + metric views)
  destroy_job.yml                 # full teardown
  daily_refresh_job.yml           # Phase 2: weather + events API refresh
src/
  generator/
    causal_context.py             # SCM causal graph engine
    demand_model.py               # time/day/season multiplier curves
    entropy.py                    # noise injectors
    domains/
      orders.py
      inventory.py
      guest.py
      loyalty.py
      workforce.py
    reference/
      seeder.py                   # orchestrates full reference seed
      menu_catalog.py             # Domino's-style ~80-item catalog
      us_locations.py             # 100–500 units + US metro geo
      external_apis.py            # Phase 2: NOAA + Ticketmaster clients
  pipeline/
    mvm_pipeline.py               # DLT notebook (all 153 tables + Gold)
  setup/
    setup_notebook.py             # creates schemas, seeds ref, creates metric views
    destroy_notebook.py           # drops staging, ref; removes metric views
conf/
  params.yml                      # backfill_months, num_units, catalog_name
```

### 7.2 Configuration Parameters (`conf/params.yml`)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `catalog_name` | `qsr_synth` | UC catalog to deploy into |
| `num_units` | `250` | Number of restaurant locations to seed |
| `backfill_months` | `12` | Months of historical data to generate |
| `live_tick_seconds` | `60` | Interval for live event generation |
| `base_orders_per_unit_per_hour` | `18` | Baseline order rate at peak hour |

### 7.3 Job Topology

```
deploy time:
  setup_job Task 1 ────────────────→ (creates catalog, schemas, seeds ref data)
       │
       └──→ generator_job (backfill mode) ──→ DLT pipeline (processes backfill)
                                                     │
                                            (Gold tables exist after first DLT batch)
                                                     │
                                            setup_job Task 2 ──→ CREATE METRIC VIEWS
  Note: Task 2 is triggered manually (or via a separate job run) after confirming
  the DLT pipeline has completed at least one successful batch and Gold tables exist.

steady state:
  generator_job (live, every 60s) ──→ staging tables ──→ DLT pipeline (continuous)
                                                                │
                                                        Silver + Gold + Metric Views

teardown:
  destroy_job (manual) ──→ DROP metric views, staging, ref
  databricks bundle destroy ──→ removes jobs, pipeline, catalog
```

### 7.4 DAB-Managed vs. Setup-Job-Managed Resources

| Resource | Managed by |
|----------|-----------|
| DLT pipeline | DAB (`resources/pipeline.yml`) |
| Generator job | DAB (`resources/generator_job.yml`) |
| Setup job | DAB (`resources/setup_job.yml`) |
| Destroy job | DAB (`resources/destroy_job.yml`) |
| UC catalog + schemas | Setup notebook |
| Reference Delta tables | Setup notebook |
| UC Metric Views | Setup notebook (after DLT Gold tables exist) |
| Metric View teardown | Destroy notebook |

---

## 8. Metrics Layer

### 8.1 DLT Gold Tables

Computed by `mvm_pipeline.py` from Silver actuals:

- `gold.unit_performance_daily` — AUV, COGS%, labor%, SSS growth per unit per day
- `gold.sos_compliance_summary` — SOS breach rate, avg prep time by channel/daypart
- `gold.loyalty_cohort_metrics` — tier distribution, redemption rate, CLV by cohort
- `gold.inventory_waste_summary` — waste %, shrink by category, replenishment frequency

### 8.2 UC Metric Views

Declared in `setup_notebook.py` using SQL from the repo's `metrics/` folder. Created after DLT Gold tables exist. Exposed to Genie and AI/BI Dashboards for natural language querying.

Example metrics exposed:
- Average Unit Volume (AUV) by territory/region
- Speed of Service (SOS) compliance % by store/daypart
- Loyalty active member rate and redemption frequency
- Inventory waste cost as % of COGS

---

## 9. Roadmap

### Phase 2 — Weather & Events Integration
- `daily_refresh_job` calls NOAA weather API + Ticketmaster events API
- Results stored in `ref.weather_conditions` and `ref.local_events` (stubs exist in Phase 1)
- `CausalContext` fields `weather_condition`, `precipitation_inches`, `temperature_f`, `local_event_type`, `local_event_attendance` populated automatically
- No changes to domain generators required

### Phase 2 — Marketing Domain
- Campaign and promotion generation
- Discount / coupon events linked to orders
- Loyalty program configuration variations

### Phase 2 — Real Estate Domain
- Site and lease record seeding
- Property profile linked to `unit` records

---

## 10. Out of Scope

- Data quality failure injection (bad data) — this is a demo/analytics/ML generator, not a testing harness
- Multi-brand simulation — single Domino's-style brand archetype only
- International locations — US only in Phase 1
- PII anonymization tooling — all guest data is fully synthetic, no real PII involved
