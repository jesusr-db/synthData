# QSR Synthetic Data Generator — Roadmap

## Current State (as of 2026-05-18)

- Backfill complete: 1-month window (Apr 18 – May 18, 2026), ~12.6M rows across 5 staging tables
- DLT pipeline: 19 silver tables populated, triggered per live generator run
- Live generator: running every minute, Poisson demand model, ~2,600 rows/tick
- Setup job: fully automated (setup → pipeline → backfill → unpause_generator)
- Auto-rehydrate: backfill resumes from MAX(event_ts) on re-run

---

## Phase 2 — Catalog Enrichment + Genie Space

Source: [amralieg/vibe-business-data-models/restaurants/mvm_v1](https://github.com/amralieg/vibe-business-data-models/tree/main/restaurants/mvm_v1)

### 2.1 Table & Column Descriptions

**What:** Apply full-sentence descriptions to every silver table and column in Unity Catalog.

**How:** Parse the 14 DDL files from the data model repo. Each column has a `COMMENT` clause. Apply via:
```sql
COMMENT ON TABLE jmrdemo.silver.guest_order IS '...';
ALTER TABLE jmrdemo.silver.guest_order ALTER COLUMN guest_order_id COMMENT '...';
```

**Automate:** New `apply_catalog_metadata` notebook task in setup_job. Runs after `start_pipeline`.

---

### 2.2 PK/FK Constraints

**What:** Register primary key and foreign key relationships on all silver tables so UC lineage and Genie can understand entity relationships.

**How:**
- PKs: named `CONSTRAINT pk_<table> PRIMARY KEY(...)` inline — apply via `ALTER TABLE ... ADD CONSTRAINT`
- FKs: 827 cross-domain constraints in `restaurants_cross_domain_foreign_keys_v1_mvm.sql` — apply via `ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY (...) REFERENCES ...`

**Automate:** Same `apply_catalog_metadata` notebook. Run after descriptions.

---

### 2.3 UC Metric Views

**What:** Create `metrics` schema views on top of gold tables for direct use in Genie and BI tools.

**Planned views (from design spec):**
- `metrics.unit_daily_summary` — orders, revenue, SOS compliance per unit per day
- `metrics.loyalty_tier_distribution` — member count and avg spend by tier
- `metrics.inventory_waste_rate` — waste as % of usage by unit/week
- `metrics.staff_utilization` — scheduled vs actual hours, no-show rate
- `metrics.channel_mix_trend` — 3PD vs own delivery vs carryout share over time

**Automate:** New `create_metric_views` notebook task in setup_job. Runs after `apply_catalog_metadata`.

---

### 2.4 Genie Space

**What:** A pre-configured Genie Space scoped to the QSR dataset with curated instructions, verified questions, and suggested queries.

**How:** Genie Spaces are not DAB-manageable — create via REST API:
```
POST /api/2.0/genie/spaces
```
Include:
- Title and description
- Table references (all silver + metrics views)
- Curated instructions (domain context: Domino's-style QSR, channel mix, loyalty tiers)
- Seed questions: "Which units have the highest SOS breach rate?", "Show me loyalty tier distribution this month", etc.

**Automate:** New `create_genie_space` notebook task in setup_job. Runs last (after metric views exist).

---

## Updated Setup Job Task Order (Phase 2)

```
setup
  └── start_pipeline (full_refresh=true)
  └── backfill
        └── apply_catalog_metadata   ← NEW: descriptions + PK/FK
              └── create_metric_views  ← NEW: UC metric views on gold
                    └── create_genie_space  ← NEW: Genie Space via REST API
                          └── unpause_generator
```

---

## Phase 3 — External Signal Integration (from original design spec)

- `ref.weather_conditions` — currently empty stub; populate with weather API
- `ref.local_events` — currently empty stub; populate with events API
- Causal model upgrade: weather + local events feed into `CausalContext` as real inputs
- Marketing domain: campaigns, promotions, loyalty program config

---

## Open Issues / Known Gaps

| Issue | Status |
|---|---|
| `shift`/`time_punch` not generated in live ticks (only backfill daily @ 10:00) | Known — by design; workforce is daily |
| `digital_account` events never generated | Gap — `guest.py` generates `guest_profile` but not `digital_account` |
| `order_modifier` event type in DOMAIN_TABLE_MAP but never generated | Gap — no modifier generator implemented |
| `stock_transfer` and `adjustment` in DOMAIN_TABLE_MAP but never generated | Gap — inventory domain incomplete |
| `receiving_order` only generated at 10:00 AM daily | By design — daily receiving window |
| DLT pipeline in triggered mode (dev target forces non-continuous) | Known limitation — pipeline triggered per generator run |
