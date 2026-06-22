# PizzaTel QSR — Genie One + Ontology Demo (One-Pager)

**The story:** 4 governed **Domains** in Genie One, each a grounded Genie space over `jmrdemo.synth_*`.
Same governed UC tags organize the Domain *and* govern the data — one ontology, many questions.
Ask a question → Genie answers with **trusted SQL functions** and **metric views**, not guesses →
drill from *what happened* to *why* to *what to do*.

## Headline numbers (live in the data, 2026-06-22)
| Domain | Hook stat | Drill-down → recommendation |
|---|---|---|
| **Orders & SOS** | Worst stores breach SOS at **9.2–9.6%** vs 8.2% avg (NY/LA flagships) | They're highest-volume + leanest-staffed → **add peak labor** |
| **Orders & SOS** | **75% of deliveries "late"** — promised 46 min, actual **51 min** (+5 min bias) | **Recalibrate the ETA model** |
| **Loyalty & Rewards** | Members' AOV **$40.12 < non-members' $40.56**; ~12% penetration | No basket lift → **add member-only basket builders** |
| **Loyalty & Rewards** | **96–97% point breakage** (gold 96.5%, platinum 97.4%) | Liability + churn signal → **redemption / expiry campaigns** |
| **Inventory & Waste** | **Overproduction = 50%** of waste cost (receiving fails ~0%) | Prep-planning, not supplier → **tighten prep-par by daypart** |
| **Workforce & Labor** | Sales/labor-hour spans **$70 → $325** (4.6× gap) | Overstaffed low-end → **reallocate hours to SOS-breach stores** |
| **Workforce & Labor** | **1,854 employees** on overtime, **~18k OT hrs/week** | **Convert chronic OT to hires** at flagships |

## 5-minute demo flow
1. **Open Discover → Domains.** Four PizzaTel cards, organized by business area (governed tags), not catalog folders.
2. **Open *Orders & SOS*.** Ask *"Which stores have the highest SOS breach rate over the last 14 days?"* → flagships at ~9.5%.
   - Drill: *"What hours of the day have the highest SOS breach rate?"* (open/lunch/dinner peaks).
3. **Pivot to *Workforce & Labor*** (the ontology payoff — different Domain, same store entity).
   Ask *"Which stores have the lowest sales per labor hour?"* and *"Which stores have the most labor hours per order this week?"*
   → the SOS-breach flagships are **understaffed**, others are **overstaffed**. One fix, two domains.
4. **Show governance:** the tag that defines the Domain also governs the tables — UC row filters/column masks apply per user inside Genie.

## What's under the hood (the "grounded" proof)
- **13 trusted SQL functions** + **4 metric views** in `jmrdemo.synth_genie` — Genie calls them directly
  (e.g. `SELECT * FROM f_sos_compliance(p_days => 7)`, `MEASURE()` over `metric_waste`).
- Rich per-space instructions (glossary, metric formulas, business rules), curated joins, table comments.
- 4 governed tags applied to spaces + metric views + tables → assets self-organize into Domains.

## Caveats (so nothing surprises you on stage)
- Receiving QA / temp-check failures are **~0%** in the synthetic data — don't probe there.
- Revenue week-over-week is a **backfill ramp** (partial first/last weeks) — don't claim a revenue trend.
- "Store N" = `unit_id` N (1–250); e.g. store 85 = "Domino's #1085".
