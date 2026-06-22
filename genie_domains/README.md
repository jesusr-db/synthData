# QSR Genie Spaces + Governed Domains (Genie One + Ontology demo)

Four grounded Genie spaces over `jmrdemo.synth_*`, each wired to a **governed UC tag** so it
surfaces under **Domains** in Genie One / the Discover page. Built as a one-time live patch on
the `DEFAULT` profile (Azure workspace `adb-7405605519549535`, catalog `jmrdemo`,
warehouse `d56091a1171f30ff`). Codify into the setup job later.

## What's deployed

### Genie spaces (live, validated end-to-end)
| Space | Governed tag (= Domain) | URL |
|---|---|---|
| Orders & SOS — PizzaTel QSR | `Orders and SOS` | https://adb-7405605519549535.15.azuredatabricks.net/genie/rooms/01f16e38e68b1a81827e98c3a9162a7e |
| Loyalty & Rewards — PizzaTel QSR | `Loyalty and Rewards` | https://adb-7405605519549535.15.azuredatabricks.net/genie/rooms/01f16e38e840101092ea6ad834f0e215 |
| Inventory & Waste — PizzaTel QSR | `Inventory and Waste` | https://adb-7405605519549535.15.azuredatabricks.net/genie/rooms/01f16e38ea04146fa8f4bb493607e96b |
| Workforce & Labor — PizzaTel QSR | `Workforce and Labor` | https://adb-7405605519549535.15.azuredatabricks.net/genie/rooms/01f16e38eae110c69093d8bab73dcd99 |

Each space has: 12 sample questions, a rich text-instruction block (glossary + business rules +
metric formulas + which trusted asset to use), join specs, trusted SQL functions, and a curated
table set. Validated live — Genie answers by calling the trusted functions and metric views
(e.g. `SELECT * FROM jmrdemo.synth_genie.f_sos_compliance(p_days => 7)`, and `MEASURE()` over
`metric_waste`).

### Grounding layer — `jmrdemo.synth_genie`
- **14 trusted SQL functions** (the governed "curated SQL"), registered into the spaces as `sql_functions`:
  `f_sos_compliance`, `f_revenue_by_channel`, `f_top_menu_items`, `f_late_delivery_rate`,
  `f_loyalty_summary`, `f_member_vs_nonmember`, `f_tier_breakdown`,
  `f_waste_by_category`, `f_top_waste_stores`, `f_below_par_skus`,
  `f_labor_hours`, `f_sales_per_labor_hour`, `f_overtime_employees`.
- **4 metric views** (semantic/ontology layer): `metric_orders_sos`, `metric_loyalty`,
  `metric_waste`, `metric_labor` (query with `MEASURE()`).
- **Table comments** on every `synth_*` table the spaces use (column comments were skipped on the
  silver streaming tables — Lakeflow blocks `COMMENT ON COLUMN`; the same disambiguation lives in
  the table comments).

### Governance → Domains
- **4 governed tags** (account-level): `Orders and SOS`, `Loyalty and Rewards`,
  `Inventory and Waste`, `Workforce and Labor`. (`&` is illegal in tag keys, hence "and".)
- Each tag is applied to: its **Genie space** (via `POST /api/2.0/entity-tag-assignments`,
  `entity_type=geniespaces`), its **metric view**, its domain aggregate/silver tables, and the
  shared `synth_ref.unit` store dimension.

## ⚠️ The one manual step — create + publish the 4 Domain cards (Discover page is Beta; no API)

Tag assignment is done. To make the tagged assets appear under **Domains** in Genie One you must
create the Domain cards once in the UI:

1. **Confirm the Beta previews are on** (you're an account admin):
   Settings → Previews → enable **"Domains and Discover Page"** (account) and **"Discover Page"** (workspace).
2. Sidebar → **Discover** → **Create domain** (top-right). For each of the 4:
   - Select the **existing governed tag** with the matching name (`Orders and SOS`, etc.).
   - Fill Subtitle/Description, click **Create** (lands as a draft).
3. Open each domain → **Edit Sections** → **Publish**. The Genie space + tagged tables/metric view
   now show under that Domain, and the Domain becomes a filter in Genie One global search.

(`MANAGE DISCOVERY` is required — admins have it by default.)

## Files
- `01_grounding.sql` — schema, comments, trusted functions, metric views (`;;;`-separated; run with `runsql.py`)
- `build_spaces.py` — builds/refreshes the 4 spaces (idempotent: updates by title)
- `space_*.json` — the serialized_space payload for each space
- `spaces_created.json` — space_id / tag / title map
- `apply_tags.sql` is inlined in build notes; tags were applied via CLI (see teardown)
- `teardown.sh` — removes everything this patch created
- `verify.sh` — re-checks spaces, functions, metric views, and tag assignments

## Re-run / verify / teardown
```bash
python3 genie_domains/build_spaces.py     # idempotent rebuild of the 4 spaces
bash    genie_domains/verify.sh           # health check
bash    genie_domains/teardown.sh         # remove spaces, tags, schema (UI: delete domain cards)
```

## serialized_space v2 schema (reverse-engineered — for codifying later)
```jsonc
{ "version": 2,
  "config": { "sample_questions": [ {"id": "<32hex>", "question": ["..."]} ] },   // sorted by id
  "data_sources": { "tables": [ {"identifier": "cat.sch.tbl"} ] },                // sorted by identifier
  "instructions": {
    "text_instructions": [ {"id":"<32hex>","content":["...big markdown..."]} ],   // EXACTLY one item
    "join_specs": [ {"id":"<32hex>","left":{"identifier","alias"},"right":{...},
                    "sql":["`a`.`k` = `b`.`k`","--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--"]} ],
    "sql_functions": [ {"id":"<32hex>","identifier":"cat.sch.func"} ]             // UC functions = trusted/curated SQL
  } }
```
All id-bearing lists must be sorted by `id`; ids are lowercase 32-hex with no hyphens. Create/update
via `databricks genie create-space WAREHOUSE_ID '<json>'` / `update-space SPACE_ID '<json>'`.
