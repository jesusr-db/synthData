# QSR Synthetic Data Generator

A fully automated Domino's-style quick-service restaurant data simulator for Databricks. It generates realistic transactional data across five business domains — orders, inventory, loyalty, guests, and workforce — for 250 configurable restaurant units. A Python generator writes to five wide/sparse staging Delta tables every hour; a Lakeflow Declarative Pipeline (`mvm_pipeline`) promotes that data to 14 typed Silver tables and 4 co-located Gold aggregates; four Unity Catalog metric views (using `WITH METRICS LANGUAGE YAML`) expose named measures and dimensions for ad-hoc Genie queries. A daily refresh job overlays real weather and events context (Open-Meteo, NOAA, Nager.Date, Ticketmaster, SeatGeek) into `ref.weather_conditions` and `ref.local_events`, driving a `demand_risk_forecast` view. A customer feature store (`synth_features`) plus a basket-aware recommender model-serving endpoint power the PizzaTel recommendation surface. A governance pack layers on UC `class.*` column tags, per-table PII masking, franchisee-scoped row filters, and Lakehouse Monitors. The entire stack — schemas, staging, ref seed, weather/events refresh, backfill, pipeline start, feature tables, recommender training, metric views, Genie Space, governance, monitoring, ontos ontology layer, and generator unpausing — is orchestrated by a single twelve-task Databricks setup job and is fully rebuildable from zero with `databricks bundle deploy` followed by one job run.

| Document | Description |
|---|---|
| [architecture.md](architecture.md) | System diagram, deployed resources, design decisions |
| [data-model.md](data-model.md) | Staging, Silver, Gold, Reference, Feature, and Metric View schemas |
| [dataflow.md](dataflow.md) | End-to-end data flow, pipeline cadence, and sync status |
| [api.md](api.md) | Job parameters, metric view interface, governance functions, recommender endpoint |
| [quickstart.md](quickstart.md) | Prerequisites, environment variables, deploy steps, common commands |
| [gotchas.md](gotchas.md) | Sharp edges and workarounds by subsystem |

**Last regenerated:** 2026-06-15
