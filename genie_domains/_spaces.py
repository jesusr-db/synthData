#!/usr/bin/env python3
"""Shared builder for the 11 QSR Genie spaces (best-practice serialized_space v2).

Imported by both the local CLI wrapper (build_spaces.py) and the setup-job notebook
(src/setup/build_genie_spaces.py) so the space definitions never drift between the two.

Best-practice v2 fields (empirically verified 2026-07-08 via probe round-trip — all persist):
  - instructions.example_question_sqls[]: {id, question:[..], sql:[..], usage_guidance:[..]}  (all arrays)
  - benchmarks.questions[]:               {id, question:[..], answer:[{content:["<sql>"], format:"SQL"}]}
  - data_sources.tables[].column_configs[]: {column_name, synonyms:[..],
                                             enable_format_assistance:bool, enable_entity_matching:bool}
  - instructions.sql_snippets:            {filters:[], expressions:[], measures:[]}
"""
import json, subprocess, uuid, os

# --- schema qualifiers -------------------------------------------------------
S, M, R, G = "jmrdemo.synth_silver", "jmrdemo.synth_metrics", "jmrdemo.synth_ref", "jmrdemo.synth_genie"
F = "jmrdemo.synth_features"

# --- id + builder helpers ----------------------------------------------------
def hx():              return uuid.uuid4().hex
def sq(qs):            return [{"id": hx(), "question": [q]} for q in qs]
def ti(blocks):        return [{"id": hx(), "content": ["\n\n".join(blocks)]}]
def fn(idents):        return [{"id": hx(), "identifier": i} for i in idents]
def tbl(idents):       return [{"identifier": i} for i in idents]

def join(l, la, r, ra, cond, rt="MANY_TO_ONE"):
    return {"id": hx(),
            "left": {"identifier": l, "alias": la},
            "right": {"identifier": r, "alias": ra},
            "sql": [cond, f"--rt=FROM_RELATIONSHIP_TYPE_{rt}--"]}

def exsql(question, sql, guidance):
    """Question -> verified SQL pair (the #1 grounding lever after trusted functions)."""
    return {"id": hx(), "question": [question], "sql": [sql], "usage_guidance": [guidance]}

def bench(question, answer_sql):
    """SME-verified benchmark: question + ground-truth SQL answer."""
    return {"id": hx(), "question": [question],
            "answer": [{"content": [answer_sql], "format": "SQL"}]}

def colcfg(identifier, cols, extra_cols=None):
    """Table entry with column_configs. cols: {column_name: [synonyms...]} for categoricals
    that get value-dictionary + format-assistance enabled."""
    ccs = [{"column_name": c, "synonyms": syns,
            "enable_format_assistance": True, "enable_entity_matching": True}
           for c, syns in cols.items()]
    ccs.sort(key=lambda x: x["column_name"])  # API requires column_configs sorted by column_name
    return {"identifier": identifier, "column_configs": ccs}

# --- shared glossary (prepended to every space's text instructions) ----------
GLOSSARY = ("Shared glossary: a 'store'/'location'/'restaurant'/'unit' = a row in jmrdemo.synth_ref.unit "
            "(unit_id 1..250). Always join facts to synth_ref.unit on unit_id to get unit_name, city, state, "
            "metro_area, region_id, district_id, franchisee_id. A 'franchisee'/'owner' = synth_ref.franchisee. "
            "Data covers roughly 2026-04-21 through today. 'This week'/'last 7 days' = the trailing 7 days from "
            "current_date(); 'this month' = current month to date. When a user names a store by number "
            "(e.g. 'store 36'), filter unit_id = 36.")

# Asset-selection rule injected into every space so Genie chooses consistently (finding #4).
MEASURE_HIERARCHY = (
    "ASSET SELECTION (follow in order): (1) For standard business measures, query the metric view with "
    "MEASURE(). (2) For time-windowed operational questions (last N days) prefer the trusted SQL functions "
    "(f_*). (3) For ad-hoc slicing not covered by either, query the silver tables directly using the joins "
    "and formulas above. The functions and metric view are the canonical definitions — do not re-derive a "
    "metric in raw SQL if a function or MEASURE() already provides it.")


def serialized(d):
    """Assemble a best-practice v2 serialized_space from a domain dict."""
    byid   = lambda lst: sorted(lst, key=lambda x: x["id"])
    tables = sorted(d["tables"], key=lambda t: t["identifier"])
    instr = {
        "text_instructions":     byid(d["instructions"]),
        "join_specs":            byid(d["joins"]),
        "sql_functions":         byid(d["functions"]),
        "example_question_sqls": byid(d["example_sqls"]),
        "sql_snippets":          {"filters": [], "expressions": [], "measures": []},
    }
    return {
        "version": 2,
        "config": {"sample_questions": byid(d["questions"])},
        "data_sources": {"tables": tables},
        "instructions": instr,
        "benchmarks": {"questions": byid(d["benchmarks"])},
    }


# ============================================================================
# DOMAINS — 11 best-practice spaces
# ============================================================================
DOMAINS = {

# ---- 1. Orders & SOS (reworked) --------------------------------------------
"orders_sos": {
  "title": "Orders & SOS — PizzaTel QSR",
  "tag": "Orders and SOS",
  "bu": "Store Operations",
  "description": "Orders, revenue, channels, Speed-of-Service (SOS) compliance, and delivery performance across all PizzaTel QSR stores.",
  "tables": [
     colcfg(f"{S}.guest_order", {"channel": ["order channel","fulfillment channel"],
                                 "order_type": ["fulfillment type"],
                                 "order_status": ["status","state"]}),
     ] + tbl([f"{S}.order_item", f"{S}.delivery_order", f"{S}.sos_compliance_summary",
             f"{S}.unit_performance_daily", f"{M}.order_performance", f"{G}.metric_orders_sos",
             f"{M}.order_reconciliation", f"{M}.web_order_live", f"{M}.web_order_item_live",
             f"{R}.unit", f"{R}.menu_item", f"{R}.franchisee"]),
  "questions": sq([
     "Which stores have the highest SOS breach rate over the last 14 days?",
     "For the 8 worst SOS stores, show order volume and SOS breach rate over the last 14 days",
     "What hours of the day have the highest SOS breach rate?",
     "What is the SOS breach rate by channel over the last 14 days?",
     "What is the late-delivery rate by channel over the last 14 days?",
     "What is the average gap between actual and estimated delivery time over the last 30 days?",
     "Which stores have the largest average delivery-time gap over the last 30 days?",
     "What was total revenue and average order value by channel over the last 30 days?",
     "Which stores have the highest order cancellation rate this month?",
     "What are the top menu items by revenue at store 113 over the last 30 days?",
     "Show the daily revenue trend for store 85 over the last 30 days",
     "Which franchisees have the highest revenue this month?",
     "What is the revenue by order type (delivery, carryout, catering) over the last 30 days?",
     "How many orders were cancelled vs fulfilled over the last 14 days?",
     "How many real web orders have reconciled to the synth data?",
     "Did web order b2b4819f-8080-11f1-9d1b-3641fe8bc2eb make it into the data?",
     "Give me the full details of web order b2b4819f-8080-11f1-9d1b-3641fe8bc2eb: order items, customer, and amount"]),
  "instructions": ti([
     GLOSSARY,
     "DOMAIN = Orders & Speed-of-Service. Order header = synth_silver.guest_order (one row per order, grain guest_order_id). "
     "Line items = synth_silver.order_item (grain order_item_id; join to guest_order on guest_order_id, to menu_item on menu_item_id).",
     "Revenue = SUM(guest_order.total_amount) (= subtotal - discount_amount + tax_amount). AOV = AVG(total_amount). "
     "SOS breach rate = AVG(CASE WHEN sos_breach THEN 1 ELSE 0 END). Cancellation rate = AVG(CASE WHEN order_status='cancelled' THEN 1 ELSE 0 END). "
     "A delivery is LATE when delivery_order.actual_delivery_seconds > estimated_delivery_seconds.",
     "Channels: '3pd_delivery', 'own_delivery', 'carryout', 'catering'. For fast daily SOS trends use synth_silver.sos_compliance_summary.",
     "REAL WEB ORDERS (a UUID = the storefront app.order.id). Two views, pick by intent: "
     "(1) DETAILS / ITEMS / STORE / CUSTOMER of a web order -> use the LIVE, pipeline-independent views "
     "synth_metrics.web_order_live (header, one row per UUID) + synth_metrics.web_order_item_live (line items), joined on "
     "web_order_id. These are sourced straight from OTel and are populated within SECONDS of the order — do NOT wait for, "
     "or read from, silver for this. web_order_live carries: web_store_id/web_store_city/web_store_state/web_store_zip (the "
     "REAL storefront the guest ordered from), channel, order_stage (live fulfillment status: Prep/Bake/QualityCheck/"
     "OutForDelivery/Delivered), web_amount (true order total), web_item_count, web_total_quantity, and customer 360 "
     "(member_id, customer_matched, customer_tier, customer_total_orders, customer_lifetime_spend, NULL member = anonymous). "
     "web_order_item_live carries menu_item_id, item_name, category, quantity, unit_price (catalog base_price), line_amount "
     "(= base_price*qty; line sums need NOT equal web_amount — discounts/tax/promos live only on the header web_amount). "
     "See the 'full details of web order' example SQL. "
     "(2) RECONCILIATION AUDIT only ('did web order <UUID> reach the synth pipeline / silver?', 'how many real web orders "
     "reconciled?', 'real vs synthetic counts') -> use synth_metrics.order_reconciliation, where reconciled=TRUE means the "
     "order flowed through to synth_silver.guest_order (amount_diff ~ 0). Do not use it for item/store/customer lookups — "
     "those come from the live views above. "
     "REAL vs SYNTH STORE: web_store_id/city/state/zip are the ACTUAL storefront; synth unit_id (guest_order/synth_ref.unit) "
     "is the SYNTH store the order was blended into for analytics and is NOT the real store. When a user asks 'which store "
     "did web order <UUID> come from', answer with web_store_id + web_store_city/state. Both live views only exist when the "
     "OTel source is configured.",
     MEASURE_HIERARCHY]),
  "functions": fn([f"{G}.f_sos_compliance", f"{G}.f_revenue_by_channel", f"{G}.f_top_menu_items", f"{G}.f_late_delivery_rate"]),
  "joins": [
     join(f"{S}.guest_order","guest_order",f"{R}.unit","unit","`guest_order`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.order_item","order_item",f"{S}.guest_order","guest_order","`order_item`.`guest_order_id` = `guest_order`.`guest_order_id`"),
     join(f"{S}.order_item","order_item",f"{R}.menu_item","menu_item","`order_item`.`menu_item_id` = `menu_item`.`menu_item_id`"),
     join(f"{S}.delivery_order","delivery_order",f"{S}.guest_order","guest_order","`delivery_order`.`guest_order_id` = `guest_order`.`guest_order_id`"),
     join(f"{S}.guest_order","guest_order",f"{R}.franchisee","franchisee","`guest_order`.`franchisee_id` = `franchisee`.`franchisee_id`"),
     join(f"{M}.order_reconciliation","order_reconciliation",f"{S}.guest_order","guest_order","`order_reconciliation`.`guest_order_id` = `guest_order`.`guest_order_id`"),
     join(f"{M}.web_order_item_live","web_order_item_live",f"{M}.web_order_live","web_order_live","`web_order_item_live`.`web_order_id` = `web_order_live`.`web_order_id`")],
  "example_sqls": [
     exsql("Which stores have the highest order cancellation rate this month?",
           "SELECT unit_id, AVG(CASE WHEN order_status='cancelled' THEN 1.0 ELSE 0.0 END) AS cancellation_rate, COUNT(*) AS orders "
           "FROM jmrdemo.synth_silver.guest_order WHERE placed_at >= date_trunc('month', current_date()) "
           "GROUP BY unit_id ORDER BY cancellation_rate DESC LIMIT 20",
           "Use for store-level cancellation-rate ranking within the current month."),
     exsql("What hours of the day have the highest SOS breach rate?",
           "SELECT hour(placed_at) AS hour_of_day, AVG(CASE WHEN sos_breach THEN 1.0 ELSE 0.0 END) AS sos_breach_rate, COUNT(*) AS orders "
           "FROM jmrdemo.synth_silver.guest_order WHERE placed_at >= current_timestamp() - INTERVAL 14 DAYS "
           "GROUP BY hour(placed_at) ORDER BY hour_of_day",
           "Use for hour-of-day SOS breach analysis."),
     exsql("What is the average gap between actual and estimated delivery time over the last 30 days?",
           "SELECT AVG(actual_delivery_seconds - estimated_delivery_seconds) AS avg_gap_seconds "
           "FROM jmrdemo.synth_silver.delivery_order WHERE created_at >= current_timestamp() - INTERVAL 30 DAYS",
           "Use for the overall delivery-time gap; group by unit_id for per-store."),
     exsql("Which franchisees have the highest revenue this month?",
           "SELECT f.franchisee_name, SUM(go.total_amount) AS revenue FROM jmrdemo.synth_silver.guest_order go "
           "JOIN jmrdemo.synth_ref.franchisee f ON f.franchisee_id = go.franchisee_id "
           "WHERE go.placed_at >= date_trunc('month', current_date()) GROUP BY f.franchisee_name ORDER BY revenue DESC",
           "Use for franchisee revenue ranking in the current month."),
     exsql("What was total revenue and average order value by channel over the last 30 days?",
           "SELECT channel, COUNT(*) AS orders, SUM(total_amount) AS revenue, AVG(total_amount) AS aov "
           "FROM jmrdemo.synth_silver.guest_order WHERE placed_at >= current_timestamp() - INTERVAL 30 DAYS "
           "GROUP BY channel ORDER BY revenue DESC",
           "Equivalent to f_revenue_by_channel(30); use the function when only channel revenue is needed."),
     exsql("How many real web orders have reconciled to the synth data?",
           "SELECT reconciled, COUNT(*) AS orders, ROUND(SUM(web_amount),2) AS web_revenue "
           "FROM jmrdemo.synth_metrics.order_reconciliation GROUP BY reconciled",
           "Use synth_metrics.order_reconciliation to audit real web orders vs their synth rows; reconciled=TRUE means it reached silver."),
     exsql("Did web order b2b4819f-8080-11f1-9d1b-3641fe8bc2eb make it into the data?",
           "SELECT web_order_id, guest_order_id, reconciled, web_amount, silver_total_amount, amount_diff "
           "FROM jmrdemo.synth_metrics.order_reconciliation WHERE web_order_id = 'b2b4819f-8080-11f1-9d1b-3641fe8bc2eb'",
           "Look up a single web order UUID and its bridged synth guest_order_id; reconciled tells you if it reached silver."),
     exsql("Give me the full details of web order b2b4819f-8080-11f1-9d1b-3641fe8bc2eb: order items, customer, and amount",
           "SELECT h.web_order_id, "
           "       h.web_store_id, h.web_store_city, h.web_store_state, h.web_store_zip, "
           "       h.channel, h.order_stage, h.web_amount, h.web_item_count, h.web_total_quantity, "
           "       h.web_order_ts, "
           "       li.menu_item_id, li.item_name, li.category, li.quantity, li.unit_price, li.line_amount, "
           "       h.member_id, h.customer_matched, h.customer_tier, h.customer_total_orders, "
           "       h.customer_lifetime_spend "
           "FROM jmrdemo.synth_metrics.web_order_live h "
           "LEFT JOIN jmrdemo.synth_metrics.web_order_item_live li ON li.web_order_id = h.web_order_id "
           "WHERE h.web_order_id = 'b2b4819f-8080-11f1-9d1b-3641fe8bc2eb' "
           "ORDER BY li.menu_item_id",
           "CANONICAL full web-order drill-down — LIVE, works within seconds of the order (no pipeline wait). "
           "web_order_live is the header (one row per web order UUID); web_order_item_live is the line items "
           "(parsed straight from OTel). web_store_id / web_store_city / web_store_state / web_store_zip are the "
           "REAL storefront the guest ordered from — answer 'what store' with these. order_stage is the live "
           "fulfillment status (Prep/Bake/QualityCheck/OutForDelivery/Delivered). web_amount is the true order "
           "total; unit_price/line_amount are CATALOG price x qty (line sums need NOT equal web_amount). "
           "customer_* is the injected customer 360. One row per line item; header columns repeat across rows. "
           "Do NOT use order_reconciliation/silver for this — those lag by up to an hour."),
  ],
  "benchmarks": [
     bench("Which stores have the highest SOS breach rate over the last 14 days?",
           "SELECT * FROM jmrdemo.synth_genie.f_sos_compliance(p_days => 14) ORDER BY sos_breach_rate DESC LIMIT 10"),
     bench("What was total revenue and average order value by channel over the last 30 days?",
           "SELECT * FROM jmrdemo.synth_genie.f_revenue_by_channel(p_days => 30)"),
     bench("What is the late-delivery rate by channel over the last 14 days?",
           "SELECT * FROM jmrdemo.synth_genie.f_late_delivery_rate(p_days => 14)"),
     bench("What are the top menu items by revenue at store 113 over the last 30 days?",
           "SELECT * FROM jmrdemo.synth_genie.f_top_menu_items(p_unit => 113, p_days => 30)"),
     bench("Which franchisees have the highest revenue this month?",
           "SELECT f.franchisee_name, SUM(go.total_amount) AS revenue FROM jmrdemo.synth_silver.guest_order go "
           "JOIN jmrdemo.synth_ref.franchisee f ON f.franchisee_id = go.franchisee_id "
           "WHERE go.placed_at >= date_trunc('month', current_date()) GROUP BY f.franchisee_name ORDER BY revenue DESC"),
  ],
},

# ---- 2. Loyalty & Rewards (reworked) ---------------------------------------
"loyalty": {
  "title": "Loyalty & Rewards — PizzaTel QSR",
  "tag": "Loyalty and Rewards",
  "bu": "Customer and Loyalty",
  "description": "Loyalty membership, points earned and redeemed, reward redemptions, tier performance, and member vs non-member behavior.",
  "tables": [
     colcfg(f"{S}.loyalty_transaction", {"tier": ["loyalty tier","membership tier"],
                                          "transaction_type": ["txn type","earn or redeem"]}),
     ] + tbl([f"{S}.reward_redemption", f"{S}.digital_account", f"{S}.guest_profile",
             f"{S}.loyalty_cohort_metrics", f"{S}.guest_order",
             f"{M}.loyalty_performance", f"{G}.metric_loyalty", f"{R}.unit", f"{R}.franchisee"]),
  "questions": sq([
     "Compare average order value for members vs non-members over the last 30 days",
     "What share of orders come from loyalty members, by store, over the last 30 days?",
     "Does higher member penetration correlate with higher average order value, by store over the last 30 days?",
     "Show points earned vs redeemed by tier over the last 30 days",
     "What is the points breakage rate by tier (share of earned points not redeemed)?",
     "What is the redemption rate by store over the last 30 days?",
     "Show the active members trend by week",
     "Which tiers have the lowest redemption rate?",
     "How many reward redemptions happened this week and what was their total reward value?",
     "Which stores have the most active loyalty members this month?",
     "How many active digital accounts are there?",
     "Which franchisees have the highest loyalty engagement this month?",
     "What is the net points change by tier over the last 30 days?",
     "How many loyalty transactions were earns vs redeems over the last 30 days?"]),
  "instructions": ti([
     GLOSSARY,
     "DOMAIN = Loyalty & Rewards. Point ledger = synth_silver.loyalty_transaction (grain loyalty_transaction_id). "
     "points_delta is signed: positive = earned, negative = redeemed. tier in (bronze, silver, gold, platinum). "
     "member_id = guest_profile.guest_profile_id.",
     "Points earned = SUM(CASE WHEN points_delta>0 THEN points_delta ELSE 0 END). "
     "Points redeemed = SUM(CASE WHEN points_delta<0 THEN -points_delta ELSE 0 END). "
     "Redemption rate = points_redeemed / points_earned. Active members = COUNT(DISTINCT member_id). "
     "Member vs non-member: guest_order.member_id IS NOT NULL means a member order.",
     "Reward currency value = synth_silver.reward_redemption (points_redeemed spent for reward_value). "
     "Digital adoption = synth_silver.digital_account (account_status='active'). Fast cohort trends = synth_silver.loyalty_cohort_metrics.",
     MEASURE_HIERARCHY]),
  "functions": fn([f"{G}.f_loyalty_summary", f"{G}.f_member_vs_nonmember", f"{G}.f_tier_breakdown"]),
  "joins": [
     join(f"{S}.loyalty_transaction","loyalty_transaction",f"{R}.unit","unit","`loyalty_transaction`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.loyalty_transaction","loyalty_transaction",f"{S}.guest_order","guest_order","`loyalty_transaction`.`guest_order_id` = `guest_order`.`guest_order_id`"),
     join(f"{S}.loyalty_transaction","loyalty_transaction",f"{S}.guest_profile","guest_profile","`loyalty_transaction`.`member_id` = `guest_profile`.`guest_profile_id`"),
     join(f"{S}.reward_redemption","reward_redemption",f"{S}.guest_order","guest_order","`reward_redemption`.`guest_order_id` = `guest_order`.`guest_order_id`"),
     join(f"{S}.reward_redemption","reward_redemption",f"{R}.unit","unit","`reward_redemption`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.digital_account","digital_account",f"{S}.guest_profile","guest_profile","`digital_account`.`guest_profile_id` = `guest_profile`.`guest_profile_id`"),
     join(f"{S}.guest_order","guest_order",f"{R}.unit","unit","`guest_order`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.guest_order","guest_order",f"{R}.franchisee","franchisee","`guest_order`.`franchisee_id` = `franchisee`.`franchisee_id`")],
  "example_sqls": [
     exsql("What share of orders come from loyalty members, by store, over the last 30 days?",
           "SELECT unit_id, AVG(CASE WHEN member_id IS NOT NULL THEN 1.0 ELSE 0.0 END) AS member_order_share, COUNT(*) AS orders "
           "FROM jmrdemo.synth_silver.guest_order WHERE placed_at >= current_timestamp() - INTERVAL 30 DAYS "
           "GROUP BY unit_id ORDER BY member_order_share DESC",
           "Use for member penetration by store."),
     exsql("What is the points breakage rate by tier (share of earned points not redeemed)?",
           "SELECT tier, 1 - (SUM(CASE WHEN points_delta<0 THEN -points_delta ELSE 0 END) "
           "/ NULLIF(SUM(CASE WHEN points_delta>0 THEN points_delta ELSE 0 END),0)) AS breakage_rate "
           "FROM jmrdemo.synth_silver.loyalty_transaction WHERE transaction_at >= current_timestamp() - INTERVAL 30 DAYS "
           "GROUP BY tier ORDER BY breakage_rate DESC",
           "Breakage = 1 - redemption rate; use for unredeemed-points analysis by tier."),
     exsql("Show the active members trend by week",
           "SELECT date_trunc('week', transaction_at) AS week, COUNT(DISTINCT member_id) AS active_members "
           "FROM jmrdemo.synth_silver.loyalty_transaction WHERE transaction_at >= current_timestamp() - INTERVAL 90 DAYS "
           "GROUP BY date_trunc('week', transaction_at) ORDER BY week",
           "Use for weekly active-member trend."),
     exsql("How many reward redemptions happened this week and what was their total reward value?",
           "SELECT COUNT(*) AS redemptions, SUM(reward_value) AS total_reward_value "
           "FROM jmrdemo.synth_silver.reward_redemption WHERE redeemed_at >= current_date() - 7",
           "Use for weekly reward-redemption volume and value."),
     exsql("How many active digital accounts are there?",
           "SELECT COUNT(*) AS active_digital_accounts FROM jmrdemo.synth_silver.digital_account WHERE account_status = 'active'",
           "Use for digital-adoption counts."),
  ],
  "benchmarks": [
     bench("Compare average order value for members vs non-members over the last 30 days",
           "SELECT * FROM jmrdemo.synth_genie.f_member_vs_nonmember(p_days => 30)"),
     bench("What is the redemption rate by store over the last 30 days?",
           "SELECT * FROM jmrdemo.synth_genie.f_loyalty_summary(p_days => 30)"),
     bench("Show points earned vs redeemed by tier over the last 30 days",
           "SELECT * FROM jmrdemo.synth_genie.f_tier_breakdown(p_days => 30)"),
     bench("How many active digital accounts are there?",
           "SELECT COUNT(*) AS active_digital_accounts FROM jmrdemo.synth_silver.digital_account WHERE account_status = 'active'"),
     bench("How many reward redemptions happened this week and what was their total reward value?",
           "SELECT COUNT(*) AS redemptions, SUM(reward_value) AS total_reward_value "
           "FROM jmrdemo.synth_silver.reward_redemption WHERE redeemed_at >= current_date() - 7"),
  ],
},

# ---- 3. Inventory & Waste (reworked) ---------------------------------------
"inventory": {
  "title": "Inventory & Waste — PizzaTel QSR",
  "tag": "Inventory and Waste",
  "bu": "Supply Chain and Merchandising",
  "description": "On-hand inventory, stockout risk vs par, waste cost and categories, receiving quality, and replenishment across stores.",
  "tables": [
     colcfg(f"{S}.waste_log", {"waste_category": ["waste reason","waste type"]}),
     ] + tbl([f"{S}.on_hand_balance", f"{S}.receiving_order", f"{S}.replenishment_order",
             f"{S}.inventory_waste_summary", f"{M}.inventory_waste", f"{G}.metric_waste",
             f"{R}.recipe_ingredient", f"{R}.supplier", f"{R}.menu_item", f"{R}.unit"]),
  "questions": sq([
     "Show waste cost by category over the last 30 days",
     "What share of total waste cost is overproduction over the last 30 days?",
     "Show the overproduction waste trend by week",
     "Which stores have the highest overproduction waste over the last 30 days?",
     "Which stores have the highest total waste cost this month?",
     "Which stores have the most SKUs below par right now?",
     "Which SKUs are currently below par at store 31 and by how much?",
     "What is the average waste cost per event by store?",
     "Which waste category drives the most cost across all stores?",
     "Show the waste cost trend by week for store 85",
     "What is the receiving quality failure rate over the last 30 days?",
     "What is the cold-chain compliance rate on receiving over the last 30 days?",
     "How many replenishment orders are still open?",
     "Which menu items consume the most expensive ingredients by total extended ingredient cost?"]),
  "instructions": ti([
     GLOSSARY,
     "DOMAIN = Inventory & Waste. Waste events = synth_silver.waste_log (grain waste_log_id). "
     "waste_category in (expired, overproduction, damaged, spoilage, theft). Waste cost = SUM(waste_cost); "
     "waste events = COUNT(*). Fast trends = synth_silver.inventory_waste_summary.",
     "On-hand = synth_silver.on_hand_balance (snapshots; grain unit_id+stock_sku+snapshot_at). BELOW PAR when "
     "quantity_on_hand < par_level. For CURRENT state use latest snapshot_at per unit+stock_sku "
     "(ROW_NUMBER() OVER (PARTITION BY unit_id, stock_sku ORDER BY snapshot_at DESC) = 1), or f_below_par_skus(unit).",
     "Receiving = synth_silver.receiving_order. Quality failure rate = share where quality_inspection_result != 'pass'. "
     "Cold-chain = temperature_check_pass. Replenishment/POs = synth_silver.replenishment_order (order_status; 'open'=not completed). "
     "BOM = synth_ref.recipe_ingredient (menu_item_id -> stock_sku, quantity, cost_per_unit).",
     MEASURE_HIERARCHY]),
  "functions": fn([f"{G}.f_waste_by_category", f"{G}.f_top_waste_stores", f"{G}.f_below_par_skus"]),
  "joins": [
     join(f"{S}.waste_log","waste_log",f"{R}.unit","unit","`waste_log`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.on_hand_balance","on_hand_balance",f"{R}.unit","unit","`on_hand_balance`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.receiving_order","receiving_order",f"{R}.unit","unit","`receiving_order`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.replenishment_order","replenishment_order",f"{R}.unit","unit","`replenishment_order`.`unit_id` = `unit`.`unit_id`"),
     join(f"{R}.recipe_ingredient","recipe_ingredient",f"{R}.menu_item","menu_item","`recipe_ingredient`.`menu_item_id` = `menu_item`.`menu_item_id`")],
  "example_sqls": [
     exsql("What share of total waste cost is overproduction over the last 30 days?",
           "SELECT SUM(CASE WHEN waste_category='overproduction' THEN waste_cost ELSE 0 END) / NULLIF(SUM(waste_cost),0) AS overproduction_share "
           "FROM jmrdemo.synth_silver.waste_log WHERE logged_at >= current_timestamp() - INTERVAL 30 DAYS",
           "Use for overproduction's share of total waste cost."),
     exsql("What is the receiving quality failure rate over the last 30 days?",
           "SELECT AVG(CASE WHEN quality_inspection_result != 'pass' THEN 1.0 ELSE 0.0 END) AS quality_failure_rate, COUNT(*) AS receipts "
           "FROM jmrdemo.synth_silver.receiving_order WHERE created_at >= current_timestamp() - INTERVAL 30 DAYS",
           "Use for receiving-quality failure rate."),
     exsql("What is the cold-chain compliance rate on receiving over the last 30 days?",
           "SELECT AVG(CASE WHEN temperature_check_pass THEN 1.0 ELSE 0.0 END) AS cold_chain_compliance_rate "
           "FROM jmrdemo.synth_silver.receiving_order WHERE created_at >= current_timestamp() - INTERVAL 30 DAYS",
           "Use for cold-chain (temperature check) compliance."),
     exsql("How many replenishment orders are still open?",
           "SELECT COUNT(*) AS open_pos FROM jmrdemo.synth_silver.replenishment_order WHERE order_status = 'open'",
           "Use for open purchase-order counts; order_status 'open' = not yet completed."),
     exsql("Which menu items consume the most expensive ingredients by total extended ingredient cost?",
           "SELECT mi.item_name, SUM(ri.quantity * ri.cost_per_unit) AS extended_ingredient_cost "
           "FROM jmrdemo.synth_ref.recipe_ingredient ri JOIN jmrdemo.synth_ref.menu_item mi ON mi.menu_item_id = ri.menu_item_id "
           "GROUP BY mi.item_name ORDER BY extended_ingredient_cost DESC LIMIT 20",
           "Extended ingredient cost = SUM(quantity * cost_per_unit) from the BOM."),
  ],
  "benchmarks": [
     bench("Show waste cost by category over the last 30 days",
           "SELECT * FROM jmrdemo.synth_genie.f_waste_by_category(p_days => 30)"),
     bench("Which stores have the highest total waste cost this month?",
           "SELECT * FROM jmrdemo.synth_genie.f_top_waste_stores(p_days => 30)"),
     bench("Which SKUs are currently below par at store 31 and by how much?",
           "SELECT * FROM jmrdemo.synth_genie.f_below_par_skus(p_unit => 31)"),
     bench("What is the receiving quality failure rate over the last 30 days?",
           "SELECT AVG(CASE WHEN quality_inspection_result != 'pass' THEN 1.0 ELSE 0.0 END) AS quality_failure_rate "
           "FROM jmrdemo.synth_silver.receiving_order WHERE created_at >= current_timestamp() - INTERVAL 30 DAYS"),
     bench("Which menu items consume the most expensive ingredients by total extended ingredient cost?",
           "SELECT mi.item_name, SUM(ri.quantity * ri.cost_per_unit) AS extended_ingredient_cost "
           "FROM jmrdemo.synth_ref.recipe_ingredient ri JOIN jmrdemo.synth_ref.menu_item mi ON mi.menu_item_id = ri.menu_item_id "
           "GROUP BY mi.item_name ORDER BY extended_ingredient_cost DESC LIMIT 20"),
  ],
},

# ---- 4. Workforce & Labor (reworked) ---------------------------------------
"workforce": {
  "title": "Workforce & Labor — PizzaTel QSR",
  "tag": "Workforce and Labor",
  "bu": "Store Operations",
  "description": "Shifts, time punches, labor hours, overtime, headcount, and labor productivity (sales per labor hour) across stores.",
  "tables": [
     colcfg(f"{S}.shift", {"shift_label": ["shift name","daypart shift"], "status": ["shift status"]}),
     ] + tbl([f"{S}.time_punch", f"{S}.unit_performance_daily", f"{M}.staff_hours",
             f"{G}.metric_labor", f"{R}.unit", f"{R}.franchisee"]),
  "questions": sq([
     "Which stores have the lowest sales per labor hour over the last 7 days?",
     "For the lowest-productivity stores, show labor hours versus revenue over the last 7 days",
     "Which stores have the highest sales per labor hour over the last 7 days?",
     "How many employees worked overtime (over 40 hours) in the last 7 days?",
     "Which stores have the most overtime hours in the last 7 days?",
     "Which stores have the most labor hours per order this week?",
     "What are the total labor hours by store this week?",
     "How many unique employees worked at store 113 this month?",
     "What is the average hours per shift by store?",
     "Show the labor hours trend by week for store 85",
     "How many shifts were scheduled vs completed this week?",
     "What is total headcount by franchisee this month?",
     "What is the shift completion rate this week?",
     "Which employees worked the most hours in the last 7 days?"]),
  "instructions": ti([
     GLOSSARY,
     "DOMAIN = Workforce & Labor. Actual worked time = synth_silver.time_punch (grain time_punch_id; hours_worked per punch, "
     "employee_id, unit_id, punch_in/punch_out). Labor hours = SUM(hours_worked). Headcount = COUNT(DISTINCT employee_id). "
     "Scheduled shifts = synth_silver.shift (shift_start/shift_end, shift_label open/mid/close, status scheduled vs completed).",
     "Overtime = an employee with SUM(hours_worked) > 40 over the period; use f_overtime_employees(days). "
     "Labor productivity = revenue / labor hours. Revenue by store/day = synth_silver.unit_performance_daily.daily_revenue. "
     "Sales per labor hour = f_sales_per_labor_hour(days). Fast rollups = synth_metrics.staff_hours.",
     MEASURE_HIERARCHY]),
  "functions": fn([f"{G}.f_labor_hours", f"{G}.f_sales_per_labor_hour", f"{G}.f_overtime_employees"]),
  "joins": [
     join(f"{S}.time_punch","time_punch",f"{R}.unit","unit","`time_punch`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.shift","shift",f"{R}.unit","unit","`shift`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.unit_performance_daily","unit_performance_daily",f"{R}.unit","unit","`unit_performance_daily`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.time_punch","time_punch",f"{R}.franchisee","franchisee","`time_punch`.`franchisee_id` = `franchisee`.`franchisee_id`")],
  "example_sqls": [
     exsql("What are the total labor hours by store this week?",
           "SELECT unit_id, SUM(hours_worked) AS labor_hours FROM jmrdemo.synth_silver.time_punch "
           "WHERE punch_in >= current_date() - 7 GROUP BY unit_id ORDER BY labor_hours DESC",
           "Use for weekly labor hours by store."),
     exsql("How many unique employees worked at store 113 this month?",
           "SELECT COUNT(DISTINCT employee_id) AS employees FROM jmrdemo.synth_silver.time_punch "
           "WHERE unit_id = 113 AND punch_in >= date_trunc('month', current_date())",
           "Use for headcount at a single store this month."),
     exsql("How many shifts were scheduled vs completed this week?",
           "SELECT status, COUNT(*) AS shifts FROM jmrdemo.synth_silver.shift "
           "WHERE date >= current_date() - 7 GROUP BY status",
           "Use for scheduled-vs-completed shift comparison; completion rate = completed / all."),
     exsql("What is total headcount by franchisee this month?",
           "SELECT franchisee_id, COUNT(DISTINCT employee_id) AS headcount FROM jmrdemo.synth_silver.time_punch "
           "WHERE punch_in >= date_trunc('month', current_date()) GROUP BY franchisee_id ORDER BY headcount DESC",
           "Use for franchisee-level headcount."),
     exsql("Which stores have the most labor hours per order this week?",
           "WITH lh AS (SELECT unit_id, SUM(hours_worked) hrs FROM jmrdemo.synth_silver.time_punch WHERE punch_in >= current_date() - 7 GROUP BY unit_id), "
           "ord AS (SELECT unit_id, COUNT(*) orders FROM jmrdemo.synth_silver.guest_order WHERE placed_at >= current_date() - 7 GROUP BY unit_id) "
           "SELECT lh.unit_id, lh.hrs / NULLIF(ord.orders,0) AS labor_hours_per_order FROM lh JOIN ord ON lh.unit_id = ord.unit_id "
           "ORDER BY labor_hours_per_order DESC LIMIT 20",
           "Labor hours per order = labor hours / order count over the same window."),
  ],
  "benchmarks": [
     bench("Which stores have the lowest sales per labor hour over the last 7 days?",
           "SELECT * FROM jmrdemo.synth_genie.f_sales_per_labor_hour(p_days => 7) ORDER BY sales_per_labor_hour ASC LIMIT 10"),
     bench("How many employees worked overtime (over 40 hours) in the last 7 days?",
           "SELECT COUNT(*) AS overtime_employees FROM jmrdemo.synth_genie.f_overtime_employees(p_days => 7)"),
     bench("What are the total labor hours by store this week?",
           "SELECT * FROM jmrdemo.synth_genie.f_labor_hours(p_days => 7)"),
     bench("How many unique employees worked at store 113 this month?",
           "SELECT COUNT(DISTINCT employee_id) AS employees FROM jmrdemo.synth_silver.time_punch "
           "WHERE unit_id = 113 AND punch_in >= date_trunc('month', current_date())"),
     bench("How many shifts were scheduled vs completed this week?",
           "SELECT status, COUNT(*) AS shifts FROM jmrdemo.synth_silver.shift WHERE date >= current_date() - 7 GROUP BY status"),
  ],
},

# ---- 5. Demand Risk & External Signals (new) -------------------------------
"demand_risk": {
  "title": "Demand Risk & External Signals — PizzaTel QSR",
  "tag": "Demand Risk and External Signals",
  "bu": "Store Operations",
  "description": "Forward-looking demand risk from weather and local events: which stores face a slowdown (demand risk) or a surge (capacity risk) over the next two weeks.",
  "tables": [
     colcfg(f"{M}.demand_risk_forecast",
            {"risk_level": ["risk","risk category"], "weather_condition": ["weather"],
             "event_category": ["event type"]}),
     ] + tbl([f"{G}.metric_demand_risk", f"{R}.weather_conditions", f"{R}.local_events", f"{R}.unit"]),
  "questions": sq([
     "Which units have the highest demand risk this week?",
     "Which stores face a capacity risk (surge) over the next 14 days?",
     "Which metro areas have the most demand-risk days in the next two weeks?",
     "How does the combined demand multiplier vary by store over the next 7 days?",
     "Which upcoming local events have the highest expected demand multiplier?",
     "What is the weather forecast and demand multiplier for store 42 over the next 7 days?",
     "Which stores have severe weather alerts in the next 14 days?",
     "How many demand-risk vs capacity-risk days does each metro have this week?",
     "Which events in the next two weeks have the highest estimated attendance?",
     "What is the average demand multiplier by weather condition?",
     "Which stores should prepare for extra staffing due to capacity risk this week?",
     "Show the daily combined demand multiplier for the Chicago metro over the next 14 days"]),
  "instructions": ti([
     GLOSSARY,
     "DOMAIN = Demand Risk & External Signals (forward-looking). Primary source = synth_metrics.demand_risk_forecast "
     "(grain unit_id + forecast_date, next ~14 days). combined_demand_multiplier = weather x event multiplier (clamped 0.3..2.5). "
     "risk_level: 'demand_risk' (<0.8, expect slowdown), 'capacity_risk' (>1.4, expect surge), 'normal'.",
     "Weather detail = synth_ref.weather_conditions (per metro_area + forecast_date: weather_condition, alert_level, "
     "high_temp_f, low_temp_f, precipitation_inches, demand_multiplier, channel_shift_delivery). "
     "Events = synth_ref.local_events (per metro_area + event_date: event_name, event_category, venue, est_attendance, est_demand_multiplier).",
     "Join weather/events to stores on metro_area (synth_ref.unit.metro_area). 'This week'/'next 7 days' = forecast_date "
     "BETWEEN current_date() AND date_add(current_date(),7). Use f_demand_risk(days) for the per-store risk rollup. "
     "Metric view synth_genie.metric_demand_risk exposes governed measures (Avg Demand Multiplier, Demand Risk Days, "
     "Capacity Risk Days, Store Days) by Metro/Risk Level/Weather Condition/Forecast Date — query with MEASURE().",
     MEASURE_HIERARCHY]),
  "functions": fn([f"{G}.f_demand_risk"]),
  "joins": [
     join("jmrdemo.synth_metrics.demand_risk_forecast","demand_risk_forecast",f"{R}.unit","unit","`demand_risk_forecast`.`unit_id` = `unit`.`unit_id`"),
     join(f"{R}.weather_conditions","weather_conditions",f"{R}.unit","unit","`weather_conditions`.`metro_area` = `unit`.`metro_area`","MANY_TO_MANY"),
     join(f"{R}.local_events","local_events",f"{R}.unit","unit","`local_events`.`metro_area` = `unit`.`metro_area`","MANY_TO_MANY")],
  "example_sqls": [
     exsql("Which units have the highest demand risk this week?",
           "SELECT unit_id, metro_area, avg_demand_multiplier, demand_risk_days, capacity_risk_days "
           "FROM jmrdemo.synth_genie.f_demand_risk(p_days => 7) ORDER BY demand_risk_days DESC, avg_demand_multiplier ASC LIMIT 20",
           "Use for the per-store demand-risk outlook this week (low multiplier / many demand_risk days = slowdown risk)."),
     exsql("Which stores face a capacity risk (surge) over the next 14 days?",
           "SELECT unit_id, metro_area, capacity_risk_days, avg_demand_multiplier "
           "FROM jmrdemo.synth_genie.f_demand_risk(p_days => 14) WHERE capacity_risk_days > 0 ORDER BY capacity_risk_days DESC",
           "Use for surge/capacity planning; capacity_risk = combined multiplier > 1.4."),
     exsql("Which upcoming local events have the highest expected demand multiplier?",
           "SELECT event_name, metro_area, event_date, event_category, est_attendance, est_demand_multiplier "
           "FROM jmrdemo.synth_ref.local_events WHERE event_date BETWEEN current_date() AND date_add(current_date(),14) "
           "ORDER BY est_demand_multiplier DESC LIMIT 20",
           "Use for ranking upcoming events by expected demand lift."),
     exsql("What is the average demand multiplier by weather condition?",
           "SELECT weather_condition, AVG(demand_multiplier) AS avg_multiplier, COUNT(*) AS days "
           "FROM jmrdemo.synth_ref.weather_conditions WHERE forecast_date >= current_date() "
           "GROUP BY weather_condition ORDER BY avg_multiplier DESC",
           "Use to see how each weather condition shifts demand."),
     exsql("Which metro areas have the most demand-risk days in the next two weeks?",
           "SELECT metro_area, SUM(CASE WHEN risk_level='demand_risk' THEN 1 ELSE 0 END) AS demand_risk_days "
           "FROM jmrdemo.synth_metrics.demand_risk_forecast WHERE forecast_date BETWEEN current_date() AND date_add(current_date(),14) "
           "GROUP BY metro_area ORDER BY demand_risk_days DESC",
           "Use for metro-level demand-risk concentration."),
  ],
  "benchmarks": [
     bench("Which units have the highest demand risk this week?",
           "SELECT * FROM jmrdemo.synth_genie.f_demand_risk(p_days => 7) ORDER BY demand_risk_days DESC, avg_demand_multiplier ASC LIMIT 20"),
     bench("Which stores face a capacity risk (surge) over the next 14 days?",
           "SELECT unit_id, metro_area, capacity_risk_days FROM jmrdemo.synth_genie.f_demand_risk(p_days => 14) "
           "WHERE capacity_risk_days > 0 ORDER BY capacity_risk_days DESC"),
     bench("Which upcoming local events have the highest expected demand multiplier?",
           "SELECT event_name, metro_area, event_date, est_demand_multiplier FROM jmrdemo.synth_ref.local_events "
           "WHERE event_date BETWEEN current_date() AND date_add(current_date(),14) ORDER BY est_demand_multiplier DESC LIMIT 20"),
     bench("What is the average demand multiplier by weather condition?",
           "SELECT weather_condition, AVG(demand_multiplier) AS avg_multiplier FROM jmrdemo.synth_ref.weather_conditions "
           "WHERE forecast_date >= current_date() GROUP BY weather_condition ORDER BY avg_multiplier DESC"),
     bench("Which metro areas have the most demand-risk days in the next two weeks?",
           "SELECT metro_area, SUM(CASE WHEN risk_level='demand_risk' THEN 1 ELSE 0 END) AS demand_risk_days "
           "FROM jmrdemo.synth_metrics.demand_risk_forecast WHERE forecast_date BETWEEN current_date() AND date_add(current_date(),14) "
           "GROUP BY metro_area ORDER BY demand_risk_days DESC"),
  ],
},

# ---- 6. Franchisee / Executive (new) ---------------------------------------
"franchisee_exec": {
  "title": "Franchisee & Executive — PizzaTel QSR",
  "tag": "Franchisee and Executive",
  "bu": "Finance and Franchise",
  "description": "Cross-domain executive scorecards rolled up to the franchisee: revenue, orders, AOV, SOS compliance, waste cost, and labor across all stores a franchisee owns.",
  "tables": tbl([f"{G}.metric_franchisee", f"{S}.unit_performance_daily", f"{S}.sos_compliance_summary",
                 f"{S}.inventory_waste_summary", f"{M}.staff_hours", f"{R}.franchisee", f"{R}.unit"]),
  "questions": sq([
     "Rank franchisees by revenue this month",
     "Which franchisees have the worst SOS breach rate over the last 30 days?",
     "Show revenue, orders, AOV, SOS breach rate, waste cost, and labor hours by franchisee for the last 30 days",
     "Which franchisee's stores are underperforming across revenue and SOS?",
     "How many stores does each franchisee operate and what is their total revenue this month?",
     "Which franchisees have the highest waste cost over the last 30 days?",
     "What is the average order value by franchisee this month?",
     "Compare the top 5 and bottom 5 franchisees by revenue this month",
     "Which franchisees have the most labor hours relative to revenue over the last 30 days?",
     "Show the revenue trend by month for franchisee 12",
     "What is total revenue by region this month?",
     "Which franchisee has the best sales per labor hour over the last 30 days?"]),
  "instructions": ti([
     GLOSSARY,
     "DOMAIN = Franchisee / Executive (cross-domain rollup). A franchisee owns many units; roll facts up via unit.franchisee_id. "
     "The canonical cross-domain scorecard is f_franchisee_scorecard(days): revenue, orders, AOV, SOS breach rate, waste cost, labor hours per franchisee.",
     "The metric view synth_genie.metric_franchisee exposes governed measures (Revenue, Orders, Average Order Value, SOS Breach Rate, Stores) "
     "with a Franchisee dimension — query with MEASURE() for flexible slicing. "
     "Daily rollups: synth_silver.unit_performance_daily (revenue), sos_compliance_summary (SOS), inventory_waste_summary (waste), synth_metrics.staff_hours (labor).",
     MEASURE_HIERARCHY]),
  "functions": fn([f"{G}.f_franchisee_scorecard"]),
  "joins": [
     join(f"{S}.unit_performance_daily","unit_performance_daily",f"{R}.unit","unit","`unit_performance_daily`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.sos_compliance_summary","sos_compliance_summary",f"{R}.unit","unit","`sos_compliance_summary`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.inventory_waste_summary","inventory_waste_summary",f"{R}.unit","unit","`inventory_waste_summary`.`unit_id` = `unit`.`unit_id`"),
     join(f"{R}.unit","unit",f"{R}.franchisee","franchisee","`unit`.`franchisee_id` = `franchisee`.`franchisee_id`")],
  "example_sqls": [
     exsql("Show revenue, orders, AOV, SOS breach rate, waste cost, and labor hours by franchisee for the last 30 days",
           "SELECT * FROM jmrdemo.synth_genie.f_franchisee_scorecard(p_days => 30) ORDER BY revenue DESC",
           "The one-call cross-domain franchisee scorecard."),
     exsql("How many stores does each franchisee operate and what is their total revenue this month?",
           "SELECT u.franchisee_id, COUNT(DISTINCT u.unit_id) AS stores, SUM(go.total_amount) AS revenue "
           "FROM jmrdemo.synth_silver.guest_order go JOIN jmrdemo.synth_ref.unit u ON u.unit_id = go.unit_id "
           "WHERE go.placed_at >= date_trunc('month', current_date()) GROUP BY u.franchisee_id ORDER BY revenue DESC",
           "Use for store count + revenue per franchisee this month."),
     exsql("What is total revenue by region this month?",
           "SELECT u.region_id, SUM(go.total_amount) AS revenue FROM jmrdemo.synth_silver.guest_order go "
           "JOIN jmrdemo.synth_ref.unit u ON u.unit_id = go.unit_id WHERE go.placed_at >= date_trunc('month', current_date()) "
           "GROUP BY u.region_id ORDER BY revenue DESC",
           "Use for region-level revenue rollup."),
     exsql("Which franchisees have the highest waste cost over the last 30 days?",
           "SELECT franchisee_id, waste_cost FROM jmrdemo.synth_genie.f_franchisee_scorecard(p_days => 30) ORDER BY waste_cost DESC LIMIT 10",
           "Waste cost column of the franchisee scorecard."),
     exsql("Which franchisees have the worst SOS breach rate over the last 30 days?",
           "SELECT franchisee_id, sos_breach_rate, orders FROM jmrdemo.synth_genie.f_franchisee_scorecard(p_days => 30) "
           "ORDER BY sos_breach_rate DESC LIMIT 10",
           "SOS breach rate column of the franchisee scorecard."),
  ],
  "benchmarks": [
     bench("Rank franchisees by revenue this month",
           "SELECT franchisee_id, revenue FROM jmrdemo.synth_genie.f_franchisee_scorecard(p_days => 30) ORDER BY revenue DESC"),
     bench("Which franchisees have the worst SOS breach rate over the last 30 days?",
           "SELECT franchisee_id, sos_breach_rate FROM jmrdemo.synth_genie.f_franchisee_scorecard(p_days => 30) ORDER BY sos_breach_rate DESC LIMIT 10"),
     bench("Which franchisees have the highest waste cost over the last 30 days?",
           "SELECT franchisee_id, waste_cost FROM jmrdemo.synth_genie.f_franchisee_scorecard(p_days => 30) ORDER BY waste_cost DESC LIMIT 10"),
     bench("How many stores does each franchisee operate and what is their total revenue this month?",
           "SELECT u.franchisee_id, COUNT(DISTINCT u.unit_id) AS stores, SUM(go.total_amount) AS revenue "
           "FROM jmrdemo.synth_silver.guest_order go JOIN jmrdemo.synth_ref.unit u ON u.unit_id = go.unit_id "
           "WHERE go.placed_at >= date_trunc('month', current_date()) GROUP BY u.franchisee_id ORDER BY revenue DESC"),
     bench("What is total revenue by region this month?",
           "SELECT u.region_id, SUM(go.total_amount) AS revenue FROM jmrdemo.synth_silver.guest_order go "
           "JOIN jmrdemo.synth_ref.unit u ON u.unit_id = go.unit_id WHERE go.placed_at >= date_trunc('month', current_date()) "
           "GROUP BY u.region_id ORDER BY revenue DESC"),
  ],
},

# ---- 7. Delivery & 3PD Ops (new) -------------------------------------------
"delivery_3pd": {
  "title": "Delivery & 3PD Operations — PizzaTel QSR",
  "tag": "Delivery and 3PD Operations",
  "bu": "Store Operations",
  "description": "Delivery fulfillment and third-party (3PD) performance: late-delivery rates, delivery-time gaps, and delivery mix by store and metro.",
  "tables": [
     colcfg(f"{S}.guest_order", {"channel": ["order channel","delivery channel"]}),
     ] + tbl([f"{S}.delivery_order", f"{S}.status_event", f"{G}.metric_delivery", f"{R}.unit"]),
  "questions": sq([
     "What is the late-delivery rate by channel over the last 14 days?",
     "Which stores have the worst late-delivery rate over the last 14 days?",
     "What is the average delivery-time gap (actual minus estimated) by metro over the last 30 days?",
     "How do own_delivery and 3pd_delivery compare on late-delivery rate over the last 30 days?",
     "Which stores have the largest average delivery-time gap over the last 30 days?",
     "How many deliveries were late vs on-time over the last 14 days?",
     "What is the delivery order volume by channel over the last 30 days?",
     "Show the daily late-delivery rate trend over the last 30 days",
     "Which metro areas have the highest delivery volume over the last 30 days?",
     "What is the average actual delivery time by store over the last 14 days?",
     "Which stores improved their late-delivery rate week over week?",
     "What share of orders are delivery vs carryout over the last 30 days?"]),
  "instructions": ti([
     GLOSSARY,
     "DOMAIN = Delivery & 3PD Operations. Delivery detail = synth_silver.delivery_order (grain delivery_order_id; joins guest_order on guest_order_id). "
     "A delivery is LATE when actual_delivery_seconds > estimated_delivery_seconds. Delivery-time gap = actual_delivery_seconds - estimated_delivery_seconds. "
     "platform_order_reference identifies the 3rd-party (3PD) marketplace order.",
     "Delivery channels on guest_order: '3pd_delivery' (third-party marketplaces) and 'own_delivery' (first-party). "
     "Stage timing detail = synth_silver.status_event. Use f_late_delivery_rate(days) for per-store late rates. "
     "Metric view synth_genie.metric_delivery exposes governed measures (Deliveries, Late Deliveries, Late Delivery Rate, "
     "Avg Delivery Gap Seconds) by Channel/Store/Metro/Delivery Date — query with MEASURE().",
     MEASURE_HIERARCHY]),
  "functions": fn([f"{G}.f_late_delivery_rate"]),
  "joins": [
     join(f"{S}.delivery_order","delivery_order",f"{S}.guest_order","guest_order","`delivery_order`.`guest_order_id` = `guest_order`.`guest_order_id`"),
     join(f"{S}.guest_order","guest_order",f"{R}.unit","unit","`guest_order`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.status_event","status_event",f"{S}.guest_order","guest_order","`status_event`.`guest_order_id` = `guest_order`.`guest_order_id`")],
  "example_sqls": [
     exsql("What is the average delivery-time gap (actual minus estimated) by metro over the last 30 days?",
           "SELECT u.metro_area, AVG(d.actual_delivery_seconds - d.estimated_delivery_seconds) AS avg_gap_seconds "
           "FROM jmrdemo.synth_silver.delivery_order d JOIN jmrdemo.synth_silver.guest_order go ON go.guest_order_id = d.guest_order_id "
           "JOIN jmrdemo.synth_ref.unit u ON u.unit_id = go.unit_id WHERE d.created_at >= current_timestamp() - INTERVAL 30 DAYS "
           "GROUP BY u.metro_area ORDER BY avg_gap_seconds DESC",
           "Use for metro-level delivery-time gap."),
     exsql("How do own_delivery and 3pd_delivery compare on late-delivery rate over the last 30 days?",
           "SELECT go.channel, AVG(CASE WHEN d.actual_delivery_seconds > d.estimated_delivery_seconds THEN 1.0 ELSE 0.0 END) AS late_rate, COUNT(*) AS deliveries "
           "FROM jmrdemo.synth_silver.delivery_order d JOIN jmrdemo.synth_silver.guest_order go ON go.guest_order_id = d.guest_order_id "
           "WHERE d.created_at >= current_timestamp() - INTERVAL 30 DAYS AND go.channel IN ('own_delivery','3pd_delivery') GROUP BY go.channel",
           "Use to compare first-party vs third-party delivery reliability."),
     exsql("What is the delivery order volume by channel over the last 30 days?",
           "SELECT go.channel, COUNT(*) AS deliveries FROM jmrdemo.synth_silver.delivery_order d "
           "JOIN jmrdemo.synth_silver.guest_order go ON go.guest_order_id = d.guest_order_id "
           "WHERE d.created_at >= current_timestamp() - INTERVAL 30 DAYS GROUP BY go.channel ORDER BY deliveries DESC",
           "Use for delivery volume split by channel."),
     exsql("Which metro areas have the highest delivery volume over the last 30 days?",
           "SELECT u.metro_area, COUNT(*) AS deliveries FROM jmrdemo.synth_silver.delivery_order d "
           "JOIN jmrdemo.synth_silver.guest_order go ON go.guest_order_id = d.guest_order_id "
           "JOIN jmrdemo.synth_ref.unit u ON u.unit_id = go.unit_id WHERE d.created_at >= current_timestamp() - INTERVAL 30 DAYS "
           "GROUP BY u.metro_area ORDER BY deliveries DESC",
           "Use for metro delivery-volume ranking."),
     exsql("How many deliveries were late vs on-time over the last 14 days?",
           "SELECT CASE WHEN actual_delivery_seconds > estimated_delivery_seconds THEN 'late' ELSE 'on_time' END AS status, COUNT(*) AS deliveries "
           "FROM jmrdemo.synth_silver.delivery_order WHERE created_at >= current_timestamp() - INTERVAL 14 DAYS GROUP BY 1",
           "Use for late vs on-time delivery counts."),
  ],
  "benchmarks": [
     bench("What is the late-delivery rate by channel over the last 14 days?",
           "SELECT go.channel, AVG(CASE WHEN d.actual_delivery_seconds > d.estimated_delivery_seconds THEN 1.0 ELSE 0.0 END) AS late_rate "
           "FROM jmrdemo.synth_silver.delivery_order d JOIN jmrdemo.synth_silver.guest_order go ON go.guest_order_id = d.guest_order_id "
           "WHERE d.created_at >= current_timestamp() - INTERVAL 14 DAYS GROUP BY go.channel"),
     bench("Which stores have the worst late-delivery rate over the last 14 days?",
           "SELECT * FROM jmrdemo.synth_genie.f_late_delivery_rate(p_days => 14) ORDER BY late_rate DESC LIMIT 10"),
     bench("How many deliveries were late vs on-time over the last 14 days?",
           "SELECT CASE WHEN actual_delivery_seconds > estimated_delivery_seconds THEN 'late' ELSE 'on_time' END AS status, COUNT(*) AS deliveries "
           "FROM jmrdemo.synth_silver.delivery_order WHERE created_at >= current_timestamp() - INTERVAL 14 DAYS GROUP BY 1"),
     bench("What is the delivery order volume by channel over the last 30 days?",
           "SELECT go.channel, COUNT(*) AS deliveries FROM jmrdemo.synth_silver.delivery_order d "
           "JOIN jmrdemo.synth_silver.guest_order go ON go.guest_order_id = d.guest_order_id "
           "WHERE d.created_at >= current_timestamp() - INTERVAL 30 DAYS GROUP BY go.channel ORDER BY deliveries DESC"),
     bench("What is the average delivery-time gap by metro over the last 30 days?",
           "SELECT u.metro_area, AVG(d.actual_delivery_seconds - d.estimated_delivery_seconds) AS avg_gap_seconds "
           "FROM jmrdemo.synth_silver.delivery_order d JOIN jmrdemo.synth_silver.guest_order go ON go.guest_order_id = d.guest_order_id "
           "JOIN jmrdemo.synth_ref.unit u ON u.unit_id = go.unit_id WHERE d.created_at >= current_timestamp() - INTERVAL 30 DAYS "
           "GROUP BY u.metro_area ORDER BY avg_gap_seconds DESC"),
  ],
},

# ---- 8. Menu & Product Performance (new) -----------------------------------
"menu_product": {
  "title": "Menu & Product Performance — PizzaTel QSR",
  "tag": "Menu and Product Performance",
  "bu": "Supply Chain and Merchandising",
  "description": "Menu item performance, margin (price vs ingredient cost), price drift by financial period, and product mix by daypart and category.",
  "tables": [
     colcfg(f"{R}.menu_item", {"category": ["menu category","product category"], "daypart": ["time of day"]}),
     ] + tbl([f"{S}.order_item", f"{R}.item_price", f"{R}.recipe_ingredient",
             f"{R}.financial_period", f"{S}.guest_order", f"{G}.metric_menu"]),
  "questions": sq([
     "Which menu items have the highest gross margin (base price minus cost)?",
     "What are the top 10 menu items by units sold over the last 30 days?",
     "What are the top menu items by revenue over the last 30 days?",
     "How has item pricing drifted by financial period?",
     "Which categories generate the most revenue over the last 30 days?",
     "What is the product mix by daypart (lunch vs all day)?",
     "Which menu items have the lowest margin?",
     "What is the average base price and cost by category?",
     "Which items consume the most expensive ingredients by extended BOM cost?",
     "What share of revenue comes from pizza vs wings vs sides over the last 30 days?",
     "Which menu items are available on 3pd but not carryout?",
     "Show the price multiplier trend by financial period for menu item 5"]),
  "instructions": ti([
     GLOSSARY,
     "DOMAIN = Menu & Product Performance. Menu master = synth_ref.menu_item (grain menu_item_id; item_name, category, "
     "subcategory, base_price, cost, daypart, channel flags is_3pd_available/is_olo_available/is_delivery_available/is_carryout_available). "
     "Gross margin = base_price - cost; margin % = (base_price - cost)/base_price.",
     "Sales = synth_silver.order_item (units_sold = SUM(quantity); revenue = SUM(line_net_amount); join to guest_order on guest_order_id for time filters, to menu_item on menu_item_id). "
     "Price drift = synth_ref.item_price (price_multiplier per menu_item_id + financial_period_id). Periods = synth_ref.financial_period (period_name, start_date, fiscal_quarter). "
     "BOM extended cost = SUM(recipe_ingredient.quantity * recipe_ingredient.cost_per_unit) per menu_item_id. "
     "Metric view synth_genie.metric_menu exposes governed measures (Units Sold, Net Revenue, Line Items) by "
     "Item/Category/Daypart/Store/Order Date — query with MEASURE().",
     MEASURE_HIERARCHY]),
  "functions": fn([f"{G}.f_top_menu_items"]),
  "joins": [
     join(f"{S}.order_item","order_item",f"{R}.menu_item","menu_item","`order_item`.`menu_item_id` = `menu_item`.`menu_item_id`"),
     join(f"{S}.order_item","order_item",f"{S}.guest_order","guest_order","`order_item`.`guest_order_id` = `guest_order`.`guest_order_id`"),
     join(f"{R}.item_price","item_price",f"{R}.menu_item","menu_item","`item_price`.`menu_item_id` = `menu_item`.`menu_item_id`"),
     join(f"{R}.item_price","item_price",f"{R}.financial_period","financial_period","`item_price`.`financial_period_id` = `financial_period`.`financial_period_id`"),
     join(f"{R}.recipe_ingredient","recipe_ingredient",f"{R}.menu_item","menu_item","`recipe_ingredient`.`menu_item_id` = `menu_item`.`menu_item_id`")],
  "example_sqls": [
     exsql("Which menu items have the highest gross margin (base price minus cost)?",
           "SELECT item_name, category, base_price, cost, (base_price - cost) AS gross_margin, "
           "(base_price - cost)/NULLIF(base_price,0) AS margin_pct FROM jmrdemo.synth_ref.menu_item ORDER BY gross_margin DESC LIMIT 20",
           "Use for item-level margin ranking."),
     exsql("Which categories generate the most revenue over the last 30 days?",
           "SELECT mi.category, SUM(oi.line_net_amount) AS revenue, SUM(oi.quantity) AS units_sold "
           "FROM jmrdemo.synth_silver.order_item oi JOIN jmrdemo.synth_ref.menu_item mi ON mi.menu_item_id = oi.menu_item_id "
           "WHERE oi.placed_at >= current_timestamp() - INTERVAL 30 DAYS GROUP BY mi.category ORDER BY revenue DESC",
           "Use for category revenue ranking."),
     exsql("How has item pricing drifted by financial period?",
           "SELECT fp.period_name, AVG(ip.price_multiplier) AS avg_price_multiplier FROM jmrdemo.synth_ref.item_price ip "
           "JOIN jmrdemo.synth_ref.financial_period fp ON fp.financial_period_id = ip.financial_period_id "
           "GROUP BY fp.period_name, fp.start_date ORDER BY fp.start_date",
           "Use for price-drift trend across financial periods."),
     exsql("What is the product mix by daypart (lunch vs all day)?",
           "SELECT mi.daypart, SUM(oi.quantity) AS units_sold, SUM(oi.line_net_amount) AS revenue "
           "FROM jmrdemo.synth_silver.order_item oi JOIN jmrdemo.synth_ref.menu_item mi ON mi.menu_item_id = oi.menu_item_id "
           "WHERE oi.placed_at >= current_timestamp() - INTERVAL 30 DAYS GROUP BY mi.daypart ORDER BY revenue DESC",
           "Use for daypart product mix."),
     exsql("Which items consume the most expensive ingredients by extended BOM cost?",
           "SELECT mi.item_name, SUM(ri.quantity * ri.cost_per_unit) AS extended_ingredient_cost "
           "FROM jmrdemo.synth_ref.recipe_ingredient ri JOIN jmrdemo.synth_ref.menu_item mi ON mi.menu_item_id = ri.menu_item_id "
           "GROUP BY mi.item_name ORDER BY extended_ingredient_cost DESC LIMIT 20",
           "Extended BOM cost = SUM(quantity * cost_per_unit)."),
  ],
  "benchmarks": [
     bench("Which menu items have the highest gross margin (base price minus cost)?",
           "SELECT item_name, (base_price - cost) AS gross_margin FROM jmrdemo.synth_ref.menu_item ORDER BY gross_margin DESC LIMIT 20"),
     bench("What are the top 10 menu items by units sold over the last 30 days?",
           "SELECT mi.item_name, SUM(oi.quantity) AS units_sold FROM jmrdemo.synth_silver.order_item oi "
           "JOIN jmrdemo.synth_ref.menu_item mi ON mi.menu_item_id = oi.menu_item_id "
           "WHERE oi.placed_at >= current_timestamp() - INTERVAL 30 DAYS GROUP BY mi.item_name ORDER BY units_sold DESC LIMIT 10"),
     bench("Which categories generate the most revenue over the last 30 days?",
           "SELECT mi.category, SUM(oi.line_net_amount) AS revenue FROM jmrdemo.synth_silver.order_item oi "
           "JOIN jmrdemo.synth_ref.menu_item mi ON mi.menu_item_id = oi.menu_item_id "
           "WHERE oi.placed_at >= current_timestamp() - INTERVAL 30 DAYS GROUP BY mi.category ORDER BY revenue DESC"),
     bench("How has item pricing drifted by financial period?",
           "SELECT fp.period_name, AVG(ip.price_multiplier) AS avg_price_multiplier FROM jmrdemo.synth_ref.item_price ip "
           "JOIN jmrdemo.synth_ref.financial_period fp ON fp.financial_period_id = ip.financial_period_id "
           "GROUP BY fp.period_name, fp.start_date ORDER BY fp.start_date"),
     bench("What is the product mix by daypart (lunch vs all day)?",
           "SELECT mi.daypart, SUM(oi.quantity) AS units_sold, SUM(oi.line_net_amount) AS revenue "
           "FROM jmrdemo.synth_silver.order_item oi JOIN jmrdemo.synth_ref.menu_item mi ON mi.menu_item_id = oi.menu_item_id "
           "WHERE oi.placed_at >= current_timestamp() - INTERVAL 30 DAYS GROUP BY mi.daypart ORDER BY revenue DESC"),
  ],
},

# ---- 9. Payments & Tender Mix (new) ----------------------------------------
"payments": {
  "title": "Payments & Tender Mix — PizzaTel QSR",
  "tag": "Payments and Tender Mix",
  "bu": "Customer and Loyalty",
  "description": "Payment tender mix (credit card, digital wallet, loyalty redemption, cash), digital-wallet adoption, and settlement across stores and channels.",
  "tables": [
     colcfg(f"{S}.payment", {"tender_type": ["payment method","tender","payment type"]}),
     ] + tbl([f"{S}.guest_order", f"{G}.metric_payments", f"{R}.unit"]),
  "questions": sq([
     "What is the tender mix over the last 30 days?",
     "What share of payment amount is digital wallet over the last 30 days?",
     "What is the tender mix by channel over the last 30 days?",
     "Show the digital-wallet adoption trend by week",
     "What share of payments is loyalty redemption over the last 30 days?",
     "Which metro areas use the most cash over the last 30 days?",
     "What is the average payment amount by tender type over the last 30 days?",
     "How many payments were made by each tender type this month?",
     "Which stores have the highest digital-wallet share over the last 30 days?",
     "What is the total settled amount by tender type over the last 30 days?",
     "How does tender mix differ between members and non-members?",
     "What is the cash share trend by month?"]),
  "instructions": ti([
     GLOSSARY,
     "DOMAIN = Payments & Tender Mix. Payments = synth_silver.payment (grain payment_id; joins guest_order on guest_order_id). "
     "tender_type in (credit_card ~55%, digital_wallet ~22%, loyalty_redemption ~12%, cash ~11%). amount = paid amount; "
     "settlement_date = settlement day; paid_at = payment timestamp (use for time filters).",
     "Tender mix = share of SUM(amount) or COUNT(*) by tender_type. Join guest_order for channel/member context and unit for store/metro. "
     "Use f_tender_mix(days) for the standard mix rollup. Metric view synth_genie.metric_payments exposes governed measures "
     "(Payment Amount, Payments, Average Payment) by Tender Type/Channel/Store/Metro/Payment Date — query with MEASURE().",
     MEASURE_HIERARCHY]),
  "functions": fn([f"{G}.f_tender_mix"]),
  "joins": [
     join(f"{S}.payment","payment",f"{S}.guest_order","guest_order","`payment`.`guest_order_id` = `guest_order`.`guest_order_id`"),
     join(f"{S}.payment","payment",f"{R}.unit","unit","`payment`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.guest_order","guest_order",f"{R}.unit","unit","`guest_order`.`unit_id` = `unit`.`unit_id`")],
  "example_sqls": [
     exsql("What is the tender mix by channel over the last 30 days?",
           "SELECT go.channel, p.tender_type, SUM(p.amount) AS amount, COUNT(*) AS payments "
           "FROM jmrdemo.synth_silver.payment p JOIN jmrdemo.synth_silver.guest_order go ON go.guest_order_id = p.guest_order_id "
           "WHERE p.paid_at >= current_timestamp() - INTERVAL 30 DAYS GROUP BY go.channel, p.tender_type ORDER BY go.channel, amount DESC",
           "Use for tender mix split by order channel."),
     exsql("Show the digital-wallet adoption trend by week",
           "SELECT date_trunc('week', paid_at) AS week, "
           "SUM(CASE WHEN tender_type='digital_wallet' THEN amount ELSE 0 END)/NULLIF(SUM(amount),0) AS digital_wallet_share "
           "FROM jmrdemo.synth_silver.payment WHERE paid_at >= current_timestamp() - INTERVAL 90 DAYS "
           "GROUP BY date_trunc('week', paid_at) ORDER BY week",
           "Use for weekly digital-wallet share trend."),
     exsql("Which stores have the highest digital-wallet share over the last 30 days?",
           "SELECT unit_id, SUM(CASE WHEN tender_type='digital_wallet' THEN amount ELSE 0 END)/NULLIF(SUM(amount),0) AS digital_wallet_share "
           "FROM jmrdemo.synth_silver.payment WHERE paid_at >= current_timestamp() - INTERVAL 30 DAYS GROUP BY unit_id ORDER BY digital_wallet_share DESC LIMIT 20",
           "Use for store-level digital-wallet adoption."),
     exsql("What is the average payment amount by tender type over the last 30 days?",
           "SELECT tender_type, AVG(amount) AS avg_amount, COUNT(*) AS payments FROM jmrdemo.synth_silver.payment "
           "WHERE paid_at >= current_timestamp() - INTERVAL 30 DAYS GROUP BY tender_type ORDER BY avg_amount DESC",
           "Use for average ticket by tender type."),
     exsql("How does tender mix differ between members and non-members?",
           "SELECT CASE WHEN go.member_id IS NOT NULL THEN 'member' ELSE 'non_member' END AS segment, p.tender_type, SUM(p.amount) AS amount "
           "FROM jmrdemo.synth_silver.payment p JOIN jmrdemo.synth_silver.guest_order go ON go.guest_order_id = p.guest_order_id "
           "WHERE p.paid_at >= current_timestamp() - INTERVAL 30 DAYS GROUP BY 1, p.tender_type ORDER BY segment, amount DESC",
           "Use to compare tender mix across member segments."),
  ],
  "benchmarks": [
     bench("What is the tender mix over the last 30 days?",
           "SELECT * FROM jmrdemo.synth_genie.f_tender_mix(p_days => 30)"),
     bench("What is the average payment amount by tender type over the last 30 days?",
           "SELECT tender_type, AVG(amount) AS avg_amount FROM jmrdemo.synth_silver.payment "
           "WHERE paid_at >= current_timestamp() - INTERVAL 30 DAYS GROUP BY tender_type ORDER BY avg_amount DESC"),
     bench("How many payments were made by each tender type this month?",
           "SELECT tender_type, COUNT(*) AS payments FROM jmrdemo.synth_silver.payment "
           "WHERE paid_at >= date_trunc('month', current_date()) GROUP BY tender_type ORDER BY payments DESC"),
     bench("Which stores have the highest digital-wallet share over the last 30 days?",
           "SELECT unit_id, SUM(CASE WHEN tender_type='digital_wallet' THEN amount ELSE 0 END)/NULLIF(SUM(amount),0) AS digital_wallet_share "
           "FROM jmrdemo.synth_silver.payment WHERE paid_at >= current_timestamp() - INTERVAL 30 DAYS GROUP BY unit_id ORDER BY digital_wallet_share DESC LIMIT 20"),
     bench("What is the tender mix by channel over the last 30 days?",
           "SELECT go.channel, p.tender_type, SUM(p.amount) AS amount FROM jmrdemo.synth_silver.payment p "
           "JOIN jmrdemo.synth_silver.guest_order go ON go.guest_order_id = p.guest_order_id "
           "WHERE p.paid_at >= current_timestamp() - INTERVAL 30 DAYS GROUP BY go.channel, p.tender_type ORDER BY go.channel, amount DESC"),
  ],
},

# ---- 10. Guest / Customer 360 (new) ----------------------------------------
"guest_360": {
  "title": "Guest & Customer 360 — PizzaTel QSR",
  "tag": "Guest and Customer 360",
  "bu": "Customer and Loyalty",
  "description": "Guest account lifecycle (active / inactive / suspended), churn, digital account adoption, and new-registration trends by store and metro.",
  "tables": [
     colcfg(f"{S}.guest_profile", {"account_status": ["status","lifecycle state"]}),
     colcfg(f"{S}.digital_account", {"account_status": ["status"]}),
     ] + tbl([f"{S}.guest_order", f"{S}.order_item", f"{G}.metric_guest", f"{R}.unit",
             f"{R}.menu_item", f"{M}.order_reconciliation", f"{M}.web_order_live",
             f"{M}.web_order_item_live", f"{F}.customer_features"]),
  "questions": sq([
     "What is the inactive (churn) rate by store?",
     "How many guest profiles are active vs inactive vs suspended?",
     "Which stores have the highest churn (inactive) rate?",
     "What is the digital account adoption rate by store?",
     "How many new guest profiles were created this month?",
     "Show the new-registration trend by week",
     "Which metro areas have the most suspended accounts?",
     "How many active digital accounts are there by store?",
     "What share of guests are active by franchisee?",
     "Which stores added the most new guests over the last 30 days?",
     "How many guests have never placed an order?",
     "What is the ratio of digital accounts to guest profiles by store?",
     "Which customer placed web order b2b4819f-8080-11f1-9d1b-3641fe8bc2eb?",
     "Did web order b2b4819f-8080-11f1-9d1b-3641fe8bc2eb reconcile, and what tier is the customer?",
     "How many real web orders are tied to a known customer vs anonymous?",
     "Give me the full details of web order b2b4819f-8080-11f1-9d1b-3641fe8bc2eb: order items, customer, and amount"]),
  "instructions": ti([
     GLOSSARY,
     "DOMAIN = Guest / Customer 360 (account lifecycle, NOT loyalty points — use the Loyalty space for points). "
     "Guest master = synth_silver.guest_profile (grain guest_profile_id; account_status in active/inactive/suspended, unit_id = home store, created_at). "
     "Digital accounts = synth_silver.digital_account (one per guest_profile_id; account_status).",
     "Churn/inactive rate = AVG(CASE WHEN account_status='inactive' THEN 1 ELSE 0 END). Active = account_status='active'. "
     "Digital adoption = digital_account with account_status='active'. New registrations = guest_profile by created_at. "
     "Use f_guest_churn(days) for the per-store lifecycle rollup (pass a large p_days e.g. 3650 for all-time). "
     "Metric view synth_genie.metric_guest exposes governed measures (Profiles, Active Profiles, Inactive Profiles, "
     "Churn Rate) by Account Status/Store/Metro/Created Date — query with MEASURE().",
     "WEB ORDER LOOKUP (a UUID like 'b2b4819f-8080-11f1-9d1b-3641fe8bc2eb' = the storefront app.order.id). Two views, "
     "pick by intent: "
     "(1) WHICH CUSTOMER / DETAILS / ITEMS / STORE of a web order -> use the LIVE, pipeline-independent views "
     "synth_metrics.web_order_live (header, one row per UUID) + synth_metrics.web_order_item_live (line items), joined on "
     "web_order_id. Populated within SECONDS of the order, straight from OTel — do NOT wait for or read silver. "
     "web_order_live carries the customer 360 (member_id, customer_matched, customer_tier, customer_total_orders, "
     "customer_lifetime_spend; NULL member = anonymous), the REAL storefront (web_store_id/city/state/zip), channel, "
     "order_stage (live status), and web_amount (true total). For 'which customer placed web order X' read member_id + "
     "customer_* directly from web_order_live; join member_id to synth_features.customer_features.profile_id only if you "
     "need extra customer attributes. web_order_item_live gives menu_item_id/item_name/category/quantity/unit_price "
     "(catalog base_price)/line_amount. See the 'full details of web order' example SQL. "
     "(2) RECONCILIATION AUDIT only ('did web order X reach the synth pipeline / silver?', 'how many reconciled?', "
     "'real vs synthetic counts') -> synth_metrics.order_reconciliation, reconciled=TRUE means it flowed to "
     "synth_silver.guest_order. Do not use it for item/store/customer lookups. "
     "REAL vs SYNTH STORE: web_store_id/city/state/zip are the ACTUAL storefront; synth unit_id is the blended synth store "
     "(NOT the real one). For 'which store did web order <UUID> come from', answer with web_store_id + web_store_city/state. "
     "Both live views only exist when the OTel source is configured.",
     MEASURE_HIERARCHY]),
  "functions": fn([f"{G}.f_guest_churn"]),
  "joins": [
     join(f"{S}.guest_profile","guest_profile",f"{R}.unit","unit","`guest_profile`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.digital_account","digital_account",f"{S}.guest_profile","guest_profile","`digital_account`.`guest_profile_id` = `guest_profile`.`guest_profile_id`"),
     join(f"{M}.order_reconciliation","order_reconciliation",f"{S}.guest_order","guest_order","`order_reconciliation`.`guest_order_id` = `guest_order`.`guest_order_id`"),
     join(f"{S}.order_item","order_item",f"{S}.guest_order","guest_order","`order_item`.`guest_order_id` = `guest_order`.`guest_order_id`"),
     join(f"{S}.order_item","order_item",f"{R}.menu_item","menu_item","`order_item`.`menu_item_id` = `menu_item`.`menu_item_id`"),
     join(f"{M}.web_order_item_live","web_order_item_live",f"{M}.web_order_live","web_order_live","`web_order_item_live`.`web_order_id` = `web_order_live`.`web_order_id`")],
  "example_sqls": [
     exsql("How many guest profiles are active vs inactive vs suspended?",
           "SELECT account_status, COUNT(*) AS profiles FROM jmrdemo.synth_silver.guest_profile GROUP BY account_status ORDER BY profiles DESC",
           "Use for the overall account-status breakdown."),
     exsql("What is the digital account adoption rate by store?",
           "SELECT gp.unit_id, COUNT(DISTINCT da.guest_profile_id) / NULLIF(COUNT(DISTINCT gp.guest_profile_id),0) AS digital_adoption_rate "
           "FROM jmrdemo.synth_silver.guest_profile gp LEFT JOIN jmrdemo.synth_silver.digital_account da "
           "ON da.guest_profile_id = gp.guest_profile_id AND da.account_status='active' GROUP BY gp.unit_id ORDER BY digital_adoption_rate DESC",
           "Digital adoption = active digital accounts / guest profiles, by store."),
     exsql("How many new guest profiles were created this month?",
           "SELECT COUNT(*) AS new_profiles FROM jmrdemo.synth_silver.guest_profile WHERE created_at >= date_trunc('month', current_date())",
           "Use for new-registration counts this month."),
     exsql("Show the new-registration trend by week",
           "SELECT date_trunc('week', created_at) AS week, COUNT(*) AS new_profiles FROM jmrdemo.synth_silver.guest_profile "
           "WHERE created_at >= current_timestamp() - INTERVAL 90 DAYS GROUP BY date_trunc('week', created_at) ORDER BY week",
           "Use for weekly new-registration trend."),
     exsql("Which metro areas have the most suspended accounts?",
           "SELECT u.metro_area, COUNT(*) AS suspended FROM jmrdemo.synth_silver.guest_profile gp "
           "JOIN jmrdemo.synth_ref.unit u ON u.unit_id = gp.unit_id WHERE gp.account_status='suspended' "
           "GROUP BY u.metro_area ORDER BY suspended DESC",
           "Use for suspended-account concentration by metro."),
     exsql("Which customer placed web order b2b4819f-8080-11f1-9d1b-3641fe8bc2eb?",
           "SELECT web_order_id, member_id, customer_matched, customer_tier, customer_total_orders, "
           "customer_lifetime_spend, web_store_city, web_store_state, web_amount "
           "FROM jmrdemo.synth_metrics.web_order_live "
           "WHERE web_order_id = 'b2b4819f-8080-11f1-9d1b-3641fe8bc2eb'",
           "Look up the customer for a single web order UUID from the LIVE header view: member_id + customer 360 "
           "(tier/orders/lifetime spend), plus the real store and amount. member_id NULL = anonymous. Live — no pipeline wait."),
     exsql("How many real web orders are tied to a known customer vs anonymous?",
           "SELECT customer_matched, COUNT(*) AS orders FROM jmrdemo.synth_metrics.order_reconciliation "
           "GROUP BY customer_matched",
           "customer_matched=TRUE means the web order's injected member_id matched a synth customer_features record."),
     exsql("Give me the full details of web order b2b4819f-8080-11f1-9d1b-3641fe8bc2eb: order items, customer, and amount",
           "SELECT h.web_order_id, "
           "       h.web_store_id, h.web_store_city, h.web_store_state, h.web_store_zip, "
           "       h.channel, h.order_stage, h.web_amount, h.web_item_count, h.web_total_quantity, "
           "       h.web_order_ts, "
           "       li.menu_item_id, li.item_name, li.category, li.quantity, li.unit_price, li.line_amount, "
           "       h.member_id, h.customer_matched, h.customer_tier, h.customer_total_orders, "
           "       h.customer_lifetime_spend "
           "FROM jmrdemo.synth_metrics.web_order_live h "
           "LEFT JOIN jmrdemo.synth_metrics.web_order_item_live li ON li.web_order_id = h.web_order_id "
           "WHERE h.web_order_id = 'b2b4819f-8080-11f1-9d1b-3641fe8bc2eb' "
           "ORDER BY li.menu_item_id",
           "CANONICAL full web-order drill-down for the Guest 360 space — LIVE, works within seconds of the order "
           "(no pipeline wait). web_order_live is the header (one row per web order UUID); web_order_item_live is the "
           "line items (parsed straight from OTel). web_store_id / web_store_city / web_store_state / web_store_zip are "
           "the REAL storefront the guest ordered from — answer 'what store' with these. order_stage is the live "
           "fulfillment status. web_amount is the true order total; unit_price/line_amount are CATALOG price x qty "
           "(line sums need NOT equal web_amount). customer_* is the injected customer 360. One row per line item; "
           "header columns repeat across rows. Do NOT use order_reconciliation/silver for this — those lag by up to an hour."),
  ],
  "benchmarks": [
     bench("What is the inactive (churn) rate by store?",
           "SELECT unit_id, inactive_rate FROM jmrdemo.synth_genie.f_guest_churn(p_days => 3650) ORDER BY inactive_rate DESC"),
     bench("How many guest profiles are active vs inactive vs suspended?",
           "SELECT account_status, COUNT(*) AS profiles FROM jmrdemo.synth_silver.guest_profile GROUP BY account_status ORDER BY profiles DESC"),
     bench("How many new guest profiles were created this month?",
           "SELECT COUNT(*) AS new_profiles FROM jmrdemo.synth_silver.guest_profile WHERE created_at >= date_trunc('month', current_date())"),
     bench("Which metro areas have the most suspended accounts?",
           "SELECT u.metro_area, COUNT(*) AS suspended FROM jmrdemo.synth_silver.guest_profile gp "
           "JOIN jmrdemo.synth_ref.unit u ON u.unit_id = gp.unit_id WHERE gp.account_status='suspended' GROUP BY u.metro_area ORDER BY suspended DESC"),
     bench("How many active digital accounts are there by store?",
           "SELECT gp.unit_id, COUNT(*) AS active_digital FROM jmrdemo.synth_silver.digital_account da "
           "JOIN jmrdemo.synth_silver.guest_profile gp ON gp.guest_profile_id = da.guest_profile_id "
           "WHERE da.account_status='active' GROUP BY gp.unit_id ORDER BY active_digital DESC"),
  ],
},

# ---- 11. Customer ML Features (new) ----------------------------------------
"customer_ml": {
  "title": "Customer ML Features — PizzaTel QSR",
  "tag": "Customer ML Features",
  "bu": "Customer and Loyalty",
  "description": "Per-customer ML features: RFM (recency, frequency, monetary), tier, and category affinity for segmentation and churn-risk analysis.",
  "tables": [
     colcfg(f"{F}.customer_features", {"tier": ["loyalty tier","segment tier"]}),
     ] + tbl([f"{F}.store_features", f"{G}.metric_customer"]),
  "questions": sq([
     "Which customers are at churn risk (highest recency days)?",
     "What is the distribution of customers by tier?",
     "What is the average monetary total and AOV by tier?",
     "Which customers have the highest lifetime spend?",
     "What is the average recency in days by tier?",
     "How many customers have never ordered (recency_days = -1)?",
     "What is the average pizza affinity by tier?",
     "Which customers have the highest order frequency?",
     "Show the top 20 customers by monetary total",
     "What share of customers are high-value (monetary total in the top decile)?",
     "What is the average category affinity across all customers?",
     "Which stores have the highest average store AOV?"]),
  "instructions": ti([
     GLOSSARY,
     "DOMAIN = Customer ML Features (RFM + affinity, for segmentation/churn). Source = synth_features.customer_features "
     "(grain profile_id = guest_order.profile_id, the order-history key; NOT guest_profile.guest_profile_id). "
     "total_orders (frequency), monetary_total (lifetime spend), aov, recency_days (days since last order; -1 = never ordered), "
     "tier (bronze/silver/gold/platinum/none), affinity_<category> (share of spend on pizza/wings/sides/salads/drinks/desserts, 0..1).",
     "Churn risk = high recency_days. High-value = high monetary_total. Store features = synth_features.store_features "
     "(grain unit_id; store_orders, store_aov, category popularity). These are batch ML feature tables (refreshed weekly), not live silver data. "
     "Metric view synth_genie.metric_customer exposes governed measures (Customers, Avg Lifetime Spend, Avg Order Value, "
     "Avg Recency Days, Avg Total Orders) by Tier — query with MEASURE().",
     MEASURE_HIERARCHY]),
  "functions": fn([]),
  "joins": [],
  "example_sqls": [
     exsql("Which customers are at churn risk (highest recency days)?",
           "SELECT profile_id, recency_days, total_orders, monetary_total, tier FROM jmrdemo.synth_features.customer_features "
           "WHERE recency_days >= 0 ORDER BY recency_days DESC LIMIT 50",
           "Churn risk proxied by recency_days (exclude -1 = never ordered)."),
     exsql("What is the average monetary total and AOV by tier?",
           "SELECT tier, AVG(monetary_total) AS avg_lifetime_spend, AVG(aov) AS avg_aov, COUNT(*) AS customers "
           "FROM jmrdemo.synth_features.customer_features GROUP BY tier ORDER BY avg_lifetime_spend DESC",
           "Use for tier-level value comparison."),
     exsql("How many customers have never ordered (recency_days = -1)?",
           "SELECT COUNT(*) AS never_ordered FROM jmrdemo.synth_features.customer_features WHERE recency_days = -1",
           "recency_days = -1 sentinel means no orders on record."),
     exsql("What is the average pizza affinity by tier?",
           "SELECT tier, AVG(affinity_pizza) AS avg_pizza_affinity FROM jmrdemo.synth_features.customer_features "
           "GROUP BY tier ORDER BY avg_pizza_affinity DESC",
           "affinity_pizza = share of spend on pizza (0..1); swap the column for other categories."),
     exsql("Show the top 20 customers by monetary total",
           "SELECT profile_id, monetary_total, total_orders, aov, tier FROM jmrdemo.synth_features.customer_features "
           "ORDER BY monetary_total DESC LIMIT 20",
           "Use for highest lifetime-spend customers."),
  ],
  "benchmarks": [
     bench("Which customers are at churn risk (highest recency days)?",
           "SELECT profile_id, recency_days FROM jmrdemo.synth_features.customer_features WHERE recency_days >= 0 ORDER BY recency_days DESC LIMIT 50"),
     bench("What is the distribution of customers by tier?",
           "SELECT tier, COUNT(*) AS customers FROM jmrdemo.synth_features.customer_features GROUP BY tier ORDER BY customers DESC"),
     bench("What is the average monetary total and AOV by tier?",
           "SELECT tier, AVG(monetary_total) AS avg_lifetime_spend, AVG(aov) AS avg_aov FROM jmrdemo.synth_features.customer_features GROUP BY tier ORDER BY avg_lifetime_spend DESC"),
     bench("How many customers have never ordered (recency_days = -1)?",
           "SELECT COUNT(*) AS never_ordered FROM jmrdemo.synth_features.customer_features WHERE recency_days = -1"),
     bench("Show the top 20 customers by monetary total",
           "SELECT profile_id, monetary_total FROM jmrdemo.synth_features.customer_features ORDER BY monetary_total DESC LIMIT 20"),
  ],
},
}


# ============================================================================
# BUILD / CRUD  (used by both build_spaces.py CLI wrapper and the setup notebook)
# ============================================================================
def _cli(args, profile):
    return subprocess.run(["databricks"] + args + ["--profile", profile, "-o", "json"],
                          capture_output=True, text=True)

def existing_spaces(profile):
    """Map {title: space_id} across ALL pages. list-spaces caps ~10/page, so we MUST
    paginate — otherwise re-runs don't see prior spaces and create timestamp-suffixed dups."""
    out, token = {}, None
    for _ in range(200):  # safety bound
        args = ["genie", "list-spaces", "--page-size", "100"]
        if token:
            args += ["--page-token", token]
        p = _cli(args, profile)
        try:
            d = json.loads(p.stdout)
        except Exception:
            break
        for s in d.get("spaces", []):
            out[s["title"]] = s["space_id"]
        token = d.get("next_page_token")
        if not token:
            break
    return out

def build_all(warehouse_id, parent_path, profile="DEFAULT", outdir=None):
    """Create-or-update all 11 spaces by title. Returns {key: {title, tag, bu, space_id, action, ok}}.
    Writes space_<key>.json + spaces_created.json to outdir (default: this dir) for the record."""
    outdir = outdir or os.path.dirname(os.path.abspath(__file__))
    have = existing_spaces(profile)
    results = {}
    for key, d in DOMAINS.items():
        ss = serialized(d)
        try:
            with open(os.path.join(outdir, f"space_{key}.json"), "w") as fh:
                json.dump(ss, fh, indent=1)
        except Exception:
            pass  # notebook/DBFS may be read-only; the space payload is what matters
        blob = json.dumps(ss)
        if d["title"] in have:
            sid = have[d["title"]]
            p = _cli(["genie", "update-space", sid, "--serialized-space", blob,
                      "--title", d["title"], "--description", d["description"]], profile)
            action = "UPDATED"
        else:
            p = _cli(["genie", "create-space", warehouse_id, blob, "--title", d["title"],
                      "--description", d["description"], "--parent-path", parent_path], profile)
            action = "CREATED"
        try:    sid = json.loads(p.stdout).get("space_id", "")
        except Exception:  sid = ""
        ok = bool(sid)
        results[key] = {"title": d["title"], "tag": d["tag"], "bu": d["bu"],
                        "space_id": sid, "action": action, "ok": ok}
        print(f"{action} {'OK ' if ok else 'ERR'} {d['title']} -> {sid or (p.stdout[:160] + p.stderr[:160])}")
    try:
        with open(os.path.join(outdir, "spaces_created.json"), "w") as fh:
            json.dump(results, fh, indent=2)
    except Exception:
        pass
    return results
