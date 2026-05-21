# QSR Synthetic Data Generator

A fully automated Domino's-style quick-service restaurant data simulator for Databricks. It generates realistic transactional data across five business domains — orders, inventory, loyalty, guests, and workforce — for 250 configurable restaurant units. A Python generator writes to five staging Delta tables every hour; a Lakeflow Declarative Pipeline promotes that data to 14 typed Silver tables and 4 Gold aggregates; four Unity Catalog metric views (using `WITH METRICS LANGUAGE YAML`) expose named measures and dimensions for ad-hoc Genie queries. A governance pack layers on UC column tags, PII masking functions, franchisee-scoped row filters, and Lakehouse Monitors. The entire stack — setup, backfill, pipeline start, Genie Space creation, governance, monitoring, and generator unpausing — is orchestrated by a single nine-task Databricks job and is fully rebuildable from zero with `databricks bundle deploy` followed by one job run.

| Document | Description |
|---|---|
| [architecture.md](architecture.md) | System diagram, deployed resources, design decisions |
| [data-model.md](data-model.md) | Staging, Silver, Gold, Reference, and Metric View schemas |
| [dataflow.md](dataflow.md) | End-to-end data flow and pipeline cadence |
| [api.md](api.md) | Job parameters, metric view interface, governance functions |
| [quickstart.md](quickstart.md) | Prerequisites, environment variables, deploy steps, common commands |
| [gotchas.md](gotchas.md) | Sharp edges and workarounds by subsystem |

**Last regenerated:** 2026-05-21
