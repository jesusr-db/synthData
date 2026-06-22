#!/usr/bin/env python3
"""Build/refresh the 4 QSR Genie spaces (Orders&SOS, Loyalty, Inventory, Workforce).

Idempotent: matches existing spaces by title and UPDATEs them, else CREATEs.
Writes each serialized_space to genie_domains/space_<key>.json for the record.
Usage: python3 build_spaces.py
"""
import json, subprocess, uuid, os, sys

WH = "d56091a1171f30ff"
PROFILE = "DEFAULT"
PARENT = "/Users/jesus.rodriguez@databricks.com"
OUTDIR = os.path.join(os.path.dirname(os.path.abspath(__file__)))

def hx(): return uuid.uuid4().hex
def cli(args):
    return subprocess.run(["databricks"]+args+["--profile",PROFILE,"-o","json"],
                          capture_output=True, text=True)

def sq(qs):            return [{"id": hx(), "question": [q]} for q in qs]
def ti(blocks):        return [{"id": hx(), "content": ["\n\n".join(blocks)]}]
def fn(idents):        return [{"id": hx(), "identifier": i} for i in idents]
def tbl(idents):       return [{"identifier": i} for i in idents]
def join(l, la, r, ra, cond, rt="MANY_TO_ONE"):
    return {"id": hx(),
            "left": {"identifier": l, "alias": la},
            "right": {"identifier": r, "alias": ra},
            "sql": [cond, f"--rt=FROM_RELATIONSHIP_TYPE_{rt}--"]}

S, M, R, G = "jmrdemo.synth_silver", "jmrdemo.synth_metrics", "jmrdemo.synth_ref", "jmrdemo.synth_genie"

GLOSSARY = ("Shared glossary: a 'store'/'location'/'restaurant'/'unit' = a row in jmrdemo.synth_ref.unit "
            "(unit_id 1..250). Always join facts to synth_ref.unit on unit_id to get unit_name, city, state, "
            "metro_area, region_id, district_id, franchisee_id. A 'franchisee'/'owner' = synth_ref.franchisee. "
            "Data covers roughly 2026-04-21 through today. 'This week'/'last 7 days' = the trailing 7 days from "
            "current_date(); 'this month' = current month to date. When a user names a store by number "
            "(e.g. 'store 36'), filter unit_id = 36.")

DOMAINS = {
"orders_sos": {
  "title": "Orders & SOS — PizzaTel QSR",
  "tag": "Orders and SOS",
  "description": "Orders, revenue, channels, Speed-of-Service (SOS) compliance, and delivery performance across all PizzaTel QSR stores.",
  "tables": tbl([
     f"{S}.guest_order", f"{S}.order_item", f"{S}.status_event", f"{S}.delivery_order",
     f"{S}.payment", f"{S}.sos_compliance_summary", f"{S}.unit_performance_daily",
     f"{M}.order_performance", f"{G}.metric_orders_sos",
     f"{R}.unit", f"{R}.menu_item", f"{R}.franchisee", f"{R}.financial_period"]),
  "questions": sq([
     "Which stores have the highest SOS breach rate over the last 14 days?",
     "For the 8 worst SOS stores, show order volume and SOS breach rate over the last 14 days",
     "What hours of the day have the highest SOS breach rate?",
     "What is the SOS breach rate by channel over the last 14 days?",
     "What is the late-delivery rate by channel over the last 14 days?",
     "What is the average gap between actual and estimated delivery time?",
     "Which stores have the largest average delivery-time gap?",
     "What was total revenue and average order value by channel over the last 30 days?",
     "Which stores have the highest order cancellation rate this month?",
     "What are the top menu items by revenue at store 113 over the last 30 days?",
     "Show the daily revenue trend for store 85 over the last 30 days",
     "Which franchisees have the highest revenue this month?"]),
  "instructions": ti([
     GLOSSARY,
     "DOMAIN = Orders & Speed-of-Service. Order header = synth_silver.guest_order (one row per order, grain guest_order_id). "
     "Line items = synth_silver.order_item (grain order_item_id; join to guest_order on guest_order_id, to menu_item on menu_item_id).",
     "Revenue = SUM(guest_order.total_amount), where total_amount = subtotal - discount_amount + tax_amount. "
     "Average Order Value (AOV) = AVG(guest_order.total_amount). Line-level revenue = SUM(order_item.line_net_amount).",
     "Speed of Service (SOS): an order breached SOS when guest_order.sos_breach = TRUE. "
     "SOS breach rate = AVG(CASE WHEN sos_breach THEN 1 ELSE 0 END). For fast daily trends use synth_silver.sos_compliance_summary "
     "(sos_compliance_pct = 1 - sos_breaches/total_orders, avg_prep_seconds). Stage-level timing detail is in synth_silver.status_event "
     "(is_sos_breach, elapsed_seconds_in_prior_state vs sos_target_seconds).",
     "Channels: '3pd_delivery' (third-party marketplaces), 'own_delivery' (first-party), 'carryout', 'catering'. "
     "order_type: 'delivery','carryout','catering'. A delivery is LATE when delivery_order.actual_delivery_seconds > estimated_delivery_seconds.",
     "Cancellation rate = AVG(CASE WHEN order_status = 'cancelled' THEN 1 ELSE 0 END) on guest_order. "
     "synth_metrics.order_performance is a pre-aggregate (Title-Case columns incl. `SOS Breach Rate`); prefer guest_order for flexible slicing.",
     "Prefer the trusted functions when they fit: f_sos_compliance(days), f_revenue_by_channel(days), "
     "f_top_menu_items(unit, days), f_late_delivery_rate(days). The metric view synth_genie.metric_orders_sos exposes "
     "governed measures (Orders, Revenue, Average Order Value, SOS Breach Rate, Cancellation Rate) — query with MEASURE()."]),
  "functions": fn([f"{G}.f_sos_compliance", f"{G}.f_revenue_by_channel", f"{G}.f_top_menu_items", f"{G}.f_late_delivery_rate"]),
  "joins": [
     join(f"{S}.guest_order","guest_order",f"{R}.unit","unit","`guest_order`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.order_item","order_item",f"{S}.guest_order","guest_order","`order_item`.`guest_order_id` = `guest_order`.`guest_order_id`"),
     join(f"{S}.order_item","order_item",f"{R}.menu_item","menu_item","`order_item`.`menu_item_id` = `menu_item`.`menu_item_id`"),
     join(f"{S}.status_event","status_event",f"{S}.guest_order","guest_order","`status_event`.`guest_order_id` = `guest_order`.`guest_order_id`"),
     join(f"{S}.delivery_order","delivery_order",f"{S}.guest_order","guest_order","`delivery_order`.`guest_order_id` = `guest_order`.`guest_order_id`"),
     join(f"{S}.payment","payment",f"{S}.guest_order","guest_order","`payment`.`guest_order_id` = `guest_order`.`guest_order_id`"),
     join(f"{S}.guest_order","guest_order",f"{R}.franchisee","franchisee","`guest_order`.`franchisee_id` = `franchisee`.`franchisee_id`")],
},
"loyalty": {
  "title": "Loyalty & Rewards — PizzaTel QSR",
  "tag": "Loyalty and Rewards",
  "description": "Loyalty membership, points earned and redeemed, reward redemptions, tier performance, and member vs non-member behavior.",
  "tables": tbl([
     f"{S}.loyalty_transaction", f"{S}.reward_redemption", f"{S}.digital_account",
     f"{S}.guest_profile", f"{S}.loyalty_cohort_metrics", f"{S}.guest_order",
     f"{M}.loyalty_performance", f"{G}.metric_loyalty",
     f"{R}.unit", f"{R}.franchisee"]),
  "questions": sq([
     "Compare average order value for members vs non-members over the last 30 days",
     "What share of orders come from loyalty members, by store, over the last 30 days?",
     "Does higher member penetration correlate with higher average order value?",
     "Show points earned vs redeemed by tier over the last 30 days",
     "What is the points breakage rate by tier (share of earned points not redeemed)?",
     "What is the redemption rate by store over the last 30 days?",
     "Show the active members trend by week",
     "Which tiers have the lowest redemption rate?",
     "How many reward redemptions happened this week and what was their total reward value?",
     "Which stores have the most active loyalty members?",
     "How many active digital accounts are there?",
     "Which franchisees have the highest loyalty engagement this month?"]),
  "instructions": ti([
     GLOSSARY,
     "DOMAIN = Loyalty & Rewards. Point ledger = synth_silver.loyalty_transaction (grain loyalty_transaction_id). "
     "points_delta is signed: positive = earned, negative = redeemed. transaction_type in (earn, redeem, ...). "
     "tier in (bronze, silver, gold, platinum). member_id identifies the loyalty member (= guest_profile.guest_profile_id).",
     "Points earned = SUM(CASE WHEN points_delta>0 THEN points_delta ELSE 0 END). "
     "Points redeemed = SUM(CASE WHEN points_delta<0 THEN -points_delta ELSE 0 END). "
     "Redemption rate = points_redeemed / points_earned. Net points = SUM(points_delta).",
     "Reward redemptions (currency value of rewards) = synth_silver.reward_redemption (points_redeemed spent for reward_value). "
     "Active members = COUNT(DISTINCT member_id). Digital adoption = synth_silver.digital_account (account_status='active'). "
     "Member master / PII = synth_silver.guest_profile.",
     "Member vs non-member: in synth_silver.guest_order, an order is from a member when member_id IS NOT NULL. "
     "Use this to compare AOV, order share, and revenue between members and non-members.",
     "For fast cohort trends use synth_silver.loyalty_cohort_metrics (active_members, total_points_earned, transaction_count per unit+tier+date). "
     "synth_metrics.loyalty_performance is a monthly pre-aggregate (Title-Case columns).",
     "Prefer trusted functions when they fit: f_loyalty_summary(days), f_member_vs_nonmember(days), f_tier_breakdown(days). "
     "Metric view synth_genie.metric_loyalty exposes governed measures (Active Members, Points Earned, Points Redeemed, Redemption Rate) — use MEASURE()."]),
  "functions": fn([f"{G}.f_loyalty_summary", f"{G}.f_member_vs_nonmember", f"{G}.f_tier_breakdown"]),
  "joins": [
     join(f"{S}.loyalty_transaction","loyalty_transaction",f"{R}.unit","unit","`loyalty_transaction`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.loyalty_transaction","loyalty_transaction",f"{S}.guest_order","guest_order","`loyalty_transaction`.`guest_order_id` = `guest_order`.`guest_order_id`"),
     join(f"{S}.loyalty_transaction","loyalty_transaction",f"{S}.guest_profile","guest_profile","`loyalty_transaction`.`member_id` = `guest_profile`.`guest_profile_id`"),
     join(f"{S}.reward_redemption","reward_redemption",f"{S}.guest_order","guest_order","`reward_redemption`.`guest_order_id` = `guest_order`.`guest_order_id`"),
     join(f"{S}.reward_redemption","reward_redemption",f"{R}.unit","unit","`reward_redemption`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.digital_account","digital_account",f"{S}.guest_profile","guest_profile","`digital_account`.`guest_profile_id` = `guest_profile`.`guest_profile_id`")],
},
"inventory": {
  "title": "Inventory & Waste — PizzaTel QSR",
  "tag": "Inventory and Waste",
  "description": "On-hand inventory, stockout risk vs par, waste cost and categories, receiving quality, and replenishment across stores.",
  "tables": tbl([
     f"{S}.on_hand_balance", f"{S}.waste_log", f"{S}.receiving_order", f"{S}.replenishment_order",
     f"{S}.inventory_waste_summary", f"{M}.inventory_waste", f"{G}.metric_waste",
     f"{R}.recipe_ingredient", f"{R}.supplier", f"{R}.menu_item", f"{R}.unit", f"{R}.franchisee"]),
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
     "Which suppliers are associated with the most received orders?",
     "Which menu items consume the most expensive ingredients?"]),
  "instructions": ti([
     GLOSSARY,
     "DOMAIN = Inventory & Waste. Waste events = synth_silver.waste_log (grain waste_log_id). "
     "waste_category in (expired, overproduction, damaged, spoilage, theft). Waste cost = SUM(waste_cost); "
     "waste quantity = SUM(waste_quantity); waste events = COUNT(*). For fast trends use synth_silver.inventory_waste_summary "
     "(total_waste_cost, total_waste_qty, waste_event_count per unit+date+category).",
     "On-hand inventory = synth_silver.on_hand_balance (snapshots; grain unit_id+stock_sku+snapshot_at). "
     "A SKU is BELOW PAR / at stockout risk when quantity_on_hand < par_level. For CURRENT state, use the latest snapshot_at "
     "per unit+stock_sku (ROW_NUMBER() OVER (PARTITION BY unit_id, stock_sku ORDER BY snapshot_at DESC) = 1). "
     "Use trusted function f_below_par_skus(unit) for this.",
     "Receiving = synth_silver.receiving_order. Quality failure rate = share where quality_inspection_result != 'pass'. "
     "Cold-chain compliance = temperature_check_pass (boolean). Replenishment / purchase orders = synth_silver.replenishment_order "
     "(order_status tracks PO lifecycle; 'open' = not yet completed).",
     "Ingredient bill-of-materials = synth_ref.recipe_ingredient (menu_item_id -> stock_sku, quantity, cost_per_unit). "
     "Suppliers = synth_ref.supplier. synth_metrics.inventory_waste is a pre-aggregate (Title-Case columns).",
     "Prefer trusted functions when they fit: f_waste_by_category(days), f_top_waste_stores(days), f_below_par_skus(unit). "
     "Metric view synth_genie.metric_waste exposes governed measures (Waste Cost, Waste Quantity, Waste Events, Avg Cost per Event) — use MEASURE()."]),
  "functions": fn([f"{G}.f_waste_by_category", f"{G}.f_top_waste_stores", f"{G}.f_below_par_skus"]),
  "joins": [
     join(f"{S}.waste_log","waste_log",f"{R}.unit","unit","`waste_log`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.on_hand_balance","on_hand_balance",f"{R}.unit","unit","`on_hand_balance`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.receiving_order","receiving_order",f"{R}.unit","unit","`receiving_order`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.replenishment_order","replenishment_order",f"{R}.unit","unit","`replenishment_order`.`unit_id` = `unit`.`unit_id`"),
     join(f"{R}.recipe_ingredient","recipe_ingredient",f"{R}.menu_item","menu_item","`recipe_ingredient`.`menu_item_id` = `menu_item`.`menu_item_id`")],
},
"workforce": {
  "title": "Workforce & Labor — PizzaTel QSR",
  "tag": "Workforce and Labor",
  "description": "Shifts, time punches, labor hours, overtime, headcount, and labor productivity (sales per labor hour) across stores.",
  "tables": tbl([
     f"{S}.shift", f"{S}.time_punch", f"{S}.unit_performance_daily",
     f"{M}.staff_hours", f"{G}.metric_labor",
     f"{R}.unit", f"{R}.franchisee", f"{R}.financial_period"]),
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
     "What is total headcount by franchisee this month?"]),
  "instructions": ti([
     GLOSSARY,
     "DOMAIN = Workforce & Labor. Actual worked time = synth_silver.time_punch (grain time_punch_id; hours_worked per punch, "
     "employee_id, unit_id, punch_in/punch_out). Labor hours = SUM(hours_worked). Headcount / active employees = COUNT(DISTINCT employee_id). "
     "Scheduled shifts = synth_silver.shift (shift_start/shift_end, shift_label e.g. open/mid/close, status scheduled vs completed, date).",
     "Overtime = an employee with SUM(hours_worked) > 40 over the period. Use trusted function f_overtime_employees(days).",
     "Labor productivity = revenue per labor hour. Revenue by store/day = synth_silver.unit_performance_daily.daily_revenue; "
     "labor hours by store = SUM(time_punch.hours_worked). Sales per labor hour = revenue / labor_hours. Use f_sales_per_labor_hour(days).",
     "For fast daily labor rollups use synth_metrics.staff_hours (Title-Case columns: `Total Hours Worked`, `Total Shifts`, "
     "`Unique Employees`, `Average Hours per Shift` per `Unit ID`+`Shift Date`).",
     "Prefer trusted functions when they fit: f_labor_hours(days), f_sales_per_labor_hour(days), f_overtime_employees(days). "
     "Metric view synth_genie.metric_labor exposes governed measures (Labor Hours, Employees, Punches, Avg Hours per Punch) — use MEASURE()."]),
  "functions": fn([f"{G}.f_labor_hours", f"{G}.f_sales_per_labor_hour", f"{G}.f_overtime_employees"]),
  "joins": [
     join(f"{S}.time_punch","time_punch",f"{R}.unit","unit","`time_punch`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.shift","shift",f"{R}.unit","unit","`shift`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.unit_performance_daily","unit_performance_daily",f"{R}.unit","unit","`unit_performance_daily`.`unit_id` = `unit`.`unit_id`"),
     join(f"{S}.time_punch","time_punch",f"{R}.franchisee","franchisee","`time_punch`.`franchisee_id` = `franchisee`.`franchisee_id`")],
},
}

def serialized(d):
    byid = lambda lst: sorted(lst, key=lambda x: x["id"])
    tables = sorted(d["tables"], key=lambda t: t["identifier"])
    return {
      "version": 2,
      "config": {"sample_questions": byid(d["questions"])},
      "data_sources": {"tables": tables},
      "instructions": {
         "text_instructions": byid(d["instructions"]),
         "join_specs": byid(d["joins"]),
         "sql_functions": byid(d["functions"]),
      },
    }

def existing_spaces():
    p = cli(["genie","list-spaces"])
    try: return {s["title"]: s["space_id"] for s in json.loads(p.stdout).get("spaces",[])}
    except: return {}

def main():
    have = existing_spaces()
    results = {}
    for key, d in DOMAINS.items():
        ss = serialized(d)
        path = os.path.join(OUTDIR, f"space_{key}.json")
        json.dump(ss, open(path,"w"), indent=1)
        blob = json.dumps(ss)
        if d["title"] in have:
            sid = have[d["title"]]
            p = cli(["genie","update-space", sid, "--serialized-space", blob,
                     "--title", d["title"], "--description", d["description"]])
            action = "UPDATED"
        else:
            p = cli(["genie","create-space", WH, blob, "--title", d["title"],
                     "--description", d["description"], "--parent-path", PARENT])
            action = "CREATED"
        try:
            sid = json.loads(p.stdout).get("space_id","")
        except:
            sid = ""
        ok = bool(sid)
        results[key] = {"title": d["title"], "tag": d["tag"], "space_id": sid, "action": action, "ok": ok}
        print(f"{action} {'OK' if ok else 'ERR'}  {d['title']}  -> {sid or (p.stdout[:200]+p.stderr[:200])}")
    json.dump(results, open(os.path.join(OUTDIR,"spaces_created.json"),"w"), indent=2)
    print("\nWROTE", os.path.join(OUTDIR,"spaces_created.json"))

if __name__ == "__main__":
    main()
