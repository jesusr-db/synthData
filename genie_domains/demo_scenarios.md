# Genie Domains — Demo Investigation Scenarios

Grounded in real `jmrdemo.synth_*` data (queried 2026-06-22). Each scenario is a
**Level 1 (what happened) → Level 2 (why) → Level 3 (recommend/act)** drill-down you can type
straight into the relevant Genie space. Expected findings are noted so you can narrate confidently.
The headline cross-domain story ties **Orders & SOS** to **Workforce & Labor** — the Genie One + ontology payoff.

---

## 🛵 Orders & SOS

### Scenario A — "Our flagship urban stores are breaching SOS"
- **L1 — What happened?** *"Which stores have the highest SOS breach rate over the last 14 days?"*
  → #1113, #1085, #1081 (New York-Newark) and #1040, #1201 (Los Angeles) at **9.2–9.6%** vs **~8.2%** network average.
- **L2 — Why?** *"For the 8 worst SOS stores, show order volume and labor hours per order over the last 14 days."*
  → They're the **highest-volume** stores (4,200–5,600 orders/14d) running the **lowest labor-hours-per-order** (0.13–0.19 vs 0.16–0.30 network). Understaffed for their throughput.
- **L2b — When?** *"What hours of day have the highest SOS breach rate?"* → open (6am), lunch (12–1pm), dinner (9pm) peaks.
- **L3 — Recommend.** *"Rank stores into labor-hours-per-order quartiles and show average SOS breach rate per quartile."*
  → Leanest-staffed quartile breaches **8.5%** vs **8.1%** for better-staffed. **Action: add peak-daypart labor at high-volume NY/LA stores** (see Workforce scenario A for where to pull hours from).

### Scenario B — "Delivery promises are systematically wrong"
- **L1 — What happened?** *"What is the late-delivery rate by channel over the last 14 days?"* → **~75% late across every channel.**
- **L2 — Why?** *"What is the average gap between actual and estimated delivery time?"*
  → Actual **51 min** vs promised **46 min** — a **+5-minute systematic underestimate**, not random variance.
- **L3 — Recommend.** *"Which stores have the largest delivery-time gap?"* → **Action: recalibrate the ETA model (+5 min baseline) or fix routing; set realistic customer promises** to cut "late" perception.

---

## 🎁 Loyalty & Rewards

### Scenario A — "Loyalty isn't growing the basket"
- **L1 — What happened?** *"Compare average order value for members vs non-members over the last 30 days."*
  → Members **$40.12** vs non-members **$40.56** — members actually spend **slightly less**.
- **L2 — Why?** *"What share of orders come from loyalty members, by store?"* → only **~12%** penetration, flat across all stores.
- **L3 — Recommend.** *"Does higher member penetration correlate with higher average order value?"* → no relationship (AOV ~$40 across all penetration quintiles). **Action: introduce basket-building, member-only offers (bundles, threshold rewards); the program currently adds no AOV lift.**

### Scenario B — "We're sitting on a huge unredeemed-points liability"
- **L1 — What happened?** *"Points earned vs redeemed by tier over the last 30 days."*
  → Gold earns **72M**, redeems **2.5M** (**96.5% breakage**); Platinum **97.4%**; network redemption only **~3.7%**.
- **L2 — Why?** *"Show the active-members trend by week"* → grew to ~34k then **declining** (~30k) — engagement cooling while points pile up.
- **L3 — Recommend.** *"Which tiers and stores have the lowest redemption rate?"* → **Action: launch redemption nudges / expiring-points campaigns, lower redemption thresholds.** High breakage = growing balance-sheet liability **and** a churn signal.

---

## 📦 Inventory & Waste

### Scenario A — "Half of all waste is overproduction"
- **L1 — What happened?** *"Show waste cost by category over the last 30 days."*
  → **Overproduction 49.8%**, spoilage 24.9%, theft 10.1%, expired 10%, damaged 5%.
- **L2 — Why?** *"Show the overproduction waste trend by week"* and *"Which stores have the highest overproduction waste?"*
  → Overproduction holds ~50% as volume rises; concentrated in high-volume LA / NY-Newark stores.
- **L3 — Recommend.** *"Which dayparts drive the most waste?"* → **Action: tighten prep-par by daypart and tie prep to the demand forecast** (overproduction is a prep-planning problem, not a supplier one — receiving QA/temp-check failures are ~0%).

### Scenario B — "Stockout-risk pockets"
- **L1 — What happened?** *"Which stores have the most SKUs below par right now?"* → #31 (6 SKUs), then several at 5.
- **L2 — Why?** *"For store 31, which SKUs are below par and by how much?"* (uses `f_below_par_skus(31)`).
- **L3 — Recommend.** *"Are below-par stores also high-waste (ordering mismatch)?"* → **Action: rebalance par levels — raise on chronic shortfall SKUs, lower on overproduced ones.**

---

## 🧑‍🍳 Workforce & Labor

### Scenario A — "A 4.6× labor-productivity gap"
- **L1 — What happened?** *"Which stores have the lowest sales per labor hour over the last 7 days?"*
  → #216 (**$69.76**), #6, #170 vs network **avg $181**, **max $325** — a **4.6× spread**.
- **L2 — Why?** *"For the lowest-productivity stores, show labor hours vs revenue."* → ~390 labor hours for ~$28k revenue — **overstaffed for their volume**.
- **L3 — Recommend (cross-domain capstone).** *"Compare labor productivity to SOS breach rate by store."*
  → Low-productivity stores are **overstaffed**, while the worst-SOS stores are **understaffed**. **Action: reallocate hours from low-SPLH stores to high-volume high-breach NY/LA stores** — fixes SOS *and* productivity with the same headcount.

### Scenario B — "Overtime is masking a hiring gap"
- **L1 — What happened?** *"How many employees worked overtime (over 40 hours) in the last 7 days?"* → **1,854 employees / ~18,100 OT hours.**
- **L2 — Why?** *"Which stores have the most overtime hours?"*
- **L3 — Recommend.** *"Do overtime-heavy stores overlap with the understaffed high-SOS stores?"* → **Action: convert chronic OT into additional hires at the flagship stores** (cheaper than OT, fixes SOS).

---

## 🌐 Cross-domain capstone (show Genie One Domains working together)
**"The understaffed-flagship problem."** Start in **Orders & SOS** (breaches at #1113/#1085/#1081) →
pivot to **Workforce & Labor** (those stores have the lowest labor-per-order and lean staffing, while
other stores are overstaffed at $70 SPLH) → the recommendation (**reallocate labor hours / convert OT to hires
at flagship stores**) spans two domains but one governed ontology. This is the Genie One narrative:
each domain answers its own questions, and together they tell one operational story.
