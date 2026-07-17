-- ============================================================================
-- QSR Genie Domains — Grounding layer (one-time demo patch)
-- Schema: jmrdemo.synth_genie  (curated metric views + trusted SQL functions)
-- Statements are separated by a line containing only ;;;  (see runsql.py)
-- ============================================================================

DROP VIEW IF EXISTS jmrdemo.synth_genie.mv_orders_test
;;;
CREATE SCHEMA IF NOT EXISTS jmrdemo.synth_genie
  COMMENT 'Curated Genie grounding assets (metric views + trusted SQL functions) for the QSR Genie spaces / Domains demo.'
;;;
-- ---------------------------------------------------------------------------
-- TABLE COMMENTS  (Genie reads these as primary grounding signal)
-- ---------------------------------------------------------------------------
COMMENT ON TABLE jmrdemo.synth_silver.guest_order IS 'One row per guest order (header). Grain: guest_order_id. channel in (3pd_delivery, own_delivery, carryout, catering); order_type in (delivery, carryout, catering); order_status in (fulfilled, cancelled, ...). total_amount = subtotal - discount_amount + tax_amount. sos_breach=TRUE when the order missed its Speed-of-Service prep target. placed_at/ready_at/fulfilled_at/cancelled_at are event timestamps. member_id is non-null for loyalty members. Join to unit on unit_id.'
;;;
COMMENT ON TABLE jmrdemo.synth_silver.order_item IS 'Order line items. Grain: order_item_id. One order (guest_order_id) has many items. menu_item_id joins to synth_ref.menu_item. line_net_amount is revenue after line discounts. quantity is units sold.'
;;;
COMMENT ON TABLE jmrdemo.synth_silver.status_event IS 'Order status transition log used for Speed-of-Service (SOS) analysis. Grain: status_event_id. elapsed_seconds_in_prior_state vs sos_target_seconds; is_sos_breach=TRUE when a stage exceeded its target. current_state/prior_state are kitchen/fulfillment stages.'
;;;
COMMENT ON TABLE jmrdemo.synth_silver.delivery_order IS 'Delivery fulfillment detail. Grain: delivery_order_id, joins to guest_order on guest_order_id. actual_delivery_seconds vs estimated_delivery_seconds; a delivery is LATE when actual_delivery_seconds > estimated_delivery_seconds. platform_order_reference is the 3rd-party (3PD) reference.'
;;;
COMMENT ON TABLE jmrdemo.synth_silver.payment IS 'Order payments / tenders. Grain: payment_id, joins to guest_order. tender_type is the payment method. amount is the paid amount.'
;;;
COMMENT ON TABLE jmrdemo.synth_silver.sos_compliance_summary IS 'Pre-aggregated daily Speed-of-Service compliance per unit and channel. sos_compliance_pct = 1 - sos_breaches/total_orders. avg_prep_seconds is mean prep time. Use this for fast SOS trend questions.'
;;;
COMMENT ON TABLE jmrdemo.synth_silver.unit_performance_daily IS 'Daily store (unit) performance rollup. Grain: unit_id + date. order_count, daily_revenue, avg_order_value, cancelled_count.'
;;;
COMMENT ON TABLE jmrdemo.synth_metrics.order_performance IS 'Order performance aggregate (column names are Title Case with spaces, e.g. `Unit ID`, `Total Revenue`, `Average Order Value`, `SOS Breach Rate`). Grain: Unit ID + Channel + Order Type + Order Status + Order Date. SOS Breach Rate is a fraction 0..1.'
;;;
COMMENT ON TABLE jmrdemo.synth_silver.loyalty_transaction IS 'Loyalty point ledger. Grain: loyalty_transaction_id. transaction_type in (earn, redeem, ...); points_delta is +earned / -redeemed. tier in (bronze, silver, gold, platinum). member_id identifies the loyalty member. Join to guest_order on guest_order_id and unit on unit_id.'
;;;
COMMENT ON TABLE jmrdemo.synth_silver.reward_redemption IS 'Reward redemptions. Grain: reward_redemption_id. points_redeemed spent for reward_value (currency). Join to guest_order and to member via member_id.'
;;;
COMMENT ON TABLE jmrdemo.synth_silver.digital_account IS 'Digital/app accounts. Grain: digital_account_id, one per guest_profile_id. account_status in (active, ...). Use for digital adoption / active account counts.'
;;;
COMMENT ON TABLE jmrdemo.synth_silver.guest_profile IS 'Guest/member master. Grain: guest_profile_id. Contains PII (name, email, phone, zip). account_status indicates active membership. Join loyalty_transaction.member_id and guest_order.profile_id here.'
;;;
COMMENT ON TABLE jmrdemo.synth_silver.loyalty_cohort_metrics IS 'Daily loyalty cohort rollup per unit and tier: active_members, total_points_earned, transaction_count. Use for fast tier/cohort trend questions.'
;;;
COMMENT ON TABLE jmrdemo.synth_metrics.loyalty_performance IS 'Loyalty performance aggregate (Title Case columns: `Tier`, `Transaction Type`, `Unit ID`, `Transaction Month`, `Unique Members`, `Total Transactions`, `Points Earned`, `Points Redeemed`).'
;;;
COMMENT ON TABLE jmrdemo.synth_silver.on_hand_balance IS 'Inventory on-hand snapshots. Grain: on_hand_balance_id (unit_id + stock_sku + snapshot_at). quantity_on_hand vs par_level: a SKU is BELOW PAR / at stockout risk when quantity_on_hand < par_level. quantity_reserved is committed stock. Use the latest snapshot_at per unit+sku for current state.'
;;;
COMMENT ON TABLE jmrdemo.synth_silver.waste_log IS 'Inventory waste events. Grain: waste_log_id. waste_category in (expired, overproduction, damaged, spoilage, theft). waste_cost is currency lost; waste_quantity is units. logged_at is the event time. Join to unit on unit_id.'
;;;
COMMENT ON TABLE jmrdemo.synth_silver.receiving_order IS 'Goods received from suppliers. Grain: receiving_order_id. quality_inspection_result (pass/fail) and temperature_check_pass (boolean) measure receiving quality / cold-chain compliance.'
;;;
COMMENT ON TABLE jmrdemo.synth_silver.replenishment_order IS 'Replenishment / purchase orders for stock. Grain: replenishment_order_id. order_status tracks the PO lifecycle; order_quantity is ordered amount; ordered_at is order time.'
;;;
COMMENT ON TABLE jmrdemo.synth_silver.inventory_waste_summary IS 'Daily waste rollup per unit and waste_category: total_waste_cost, total_waste_qty, waste_event_count. Use for fast waste trend questions.'
;;;
COMMENT ON TABLE jmrdemo.synth_metrics.inventory_waste IS 'Inventory waste aggregate (Title Case columns: `Unit ID`, `Stock SKU`, `Waste Category`, `Waste Week`, `Waste Month`, `Total Waste Quantity`, `Total Waste Cost`, `Waste Events`, `Average Waste Cost per Event`).'
;;;
COMMENT ON TABLE jmrdemo.synth_ref.recipe_ingredient IS 'Bill of materials: which stock_sku and quantity each menu_item_id consumes, with cost_per_unit and unit_of_measure. Use to link menu items to ingredient SKUs / theoretical usage.'
;;;
COMMENT ON TABLE jmrdemo.synth_ref.supplier IS 'Supplier master. Grain: supplier_id. category groups supplier type; status indicates active suppliers.'
;;;
COMMENT ON TABLE jmrdemo.synth_silver.shift IS 'Scheduled shifts. Grain: shift_id. employee_id works a shift at a unit from shift_start to shift_end; shift_label (e.g. open/mid/close); status (scheduled/completed/...). date is the shift date.'
;;;
COMMENT ON TABLE jmrdemo.synth_silver.time_punch IS 'Actual clock-in/out punches. Grain: time_punch_id. hours_worked = punch_out - punch_in (hours). Sum hours_worked for labor hours. Join to unit on unit_id; employee_id identifies the worker.'
;;;
COMMENT ON TABLE jmrdemo.synth_metrics.staff_hours IS 'Daily labor rollup per unit: Total Hours Worked, Total Shifts, Unique Employees, Average Hours per Shift (Title Case columns: `Unit ID`, `Shift Date`).'
;;;
COMMENT ON TABLE jmrdemo.synth_ref.unit IS 'Store (unit) master. Grain: unit_id (1..250). unit_name, city, state, metro_area, region_id, district_id, franchisee_id, format, is_franchise. THE canonical store dimension — join everything here on unit_id. A "store"/"location"/"restaurant" = a unit.'
;;;
COMMENT ON TABLE jmrdemo.synth_ref.menu_item IS 'Menu item master. Grain: menu_item_id. item_name, category, subcategory, base_price, cost, daypart. Channel availability flags: is_3pd_available, is_olo_available, is_delivery_available, is_carryout_available.'
;;;
COMMENT ON TABLE jmrdemo.synth_ref.franchisee IS 'Franchisee/owner master. Grain: franchisee_id. A franchisee owns many units (stores).'
;;;
-- ---- External signals + demand risk (Demand Risk space) ----
COMMENT ON TABLE jmrdemo.synth_ref.weather_conditions IS 'Daily weather per metro area (30-day history + 14-day forecast). Grain: metro_area + forecast_date. weather_condition (clear, rain, snow, extreme_heat, extreme_cold, ...); alert_level from NOAA (none/advisory/watch/warning); high_temp_f/low_temp_f; precipitation_inches; demand_multiplier (>1 raises demand, <1 lowers); channel_shift_delivery (share shifting to delivery in bad weather). Join to synth_ref.unit on metro_area.'
;;;
COMMENT ON TABLE jmrdemo.synth_ref.local_events IS 'Local events (holidays, sports, concerts) affecting demand. Grain: event_id. event_category (holiday, sports, concert, ...); venue; est_attendance; est_demand_multiplier (expected demand lift); source (nager_holidays, ticketmaster, seatgeek). Join to synth_ref.unit on metro_area and to a date on event_date.'
;;;
COMMENT ON TABLE jmrdemo.synth_metrics.demand_risk_forecast IS 'Forward-looking (unit, date) demand-risk signal for the next 14 days, joining units x weather x events. Grain: unit_id + forecast_date. combined_demand_multiplier = weather x event multiplier (clamped 0.3..2.5). risk_level: demand_risk (<0.8, expect a slowdown), capacity_risk (>1.4, expect a surge), or normal. Use this view for "which units have the highest demand/capacity risk this week".'
;;;
COMMENT ON TABLE jmrdemo.synth_metrics.order_reconciliation IS 'Reconciles REAL PizzaTel web orders (from the live OpenTelemetry storefront) to their rows in the synth pipeline AND to the customer record. Grain: web_order_id. web_order_id = the storefront order UUID (app.order.id); guest_order_id = the bridged synth key that lands in synth_silver.guest_order. reconciled=TRUE means the web order flowed through to silver (amount_diff should be ~0). member_id = the web-injected app.order.member_id (synth customer key, 1..50000; NULL = anonymous order); customer_matched=TRUE means member_id joined an existing synth_features.customer_features record, exposing customer_tier, customer_total_orders, and customer_lifetime_spend. Use this view whenever a web/website/online order ID (UUID) is given: "did web order <UUID> make it into the data?", "which customer placed web order <UUID>?", "how many real orders reconciled / are tied to a known customer?", or to distinguish real (source=otel) from synthetic orders. Join member_id to synth_features.customer_features.profile_id for the full customer 360. Only present when the OTel source is configured.'
;;;
-- ---- Customer ML features (Customer ML space) ----
COMMENT ON TABLE jmrdemo.synth_features.customer_features IS 'Per-customer ML feature record (RFM + category affinity). Grain: profile_id (= guest_order.profile_id, the order-history key; NOT guest_profile.guest_profile_id). total_orders, monetary_total (lifetime spend), aov, recency_days (days since last order; -1 = never), tier (bronze/silver/gold/platinum/none), and affinity_<category> (share of spend on pizza/wings/sides/salads/drinks/desserts, 0..1). High recency_days = churn risk.'
;;;
COMMENT ON TABLE jmrdemo.synth_features.store_features IS 'Per-store ML feature record. Grain: unit_id. store_orders, store_aov, category popularity, and top item per category. Use for store-level demand/popularity analysis.'
;;;
COMMENT ON COLUMN jmrdemo.synth_features.customer_features.recency_days IS 'Days since the customer''s last order; -1 means no orders on record. Higher = more likely churned.'
;;;
COMMENT ON COLUMN jmrdemo.synth_features.customer_features.monetary_total IS 'Lifetime spend (sum of order total_amount) for the customer. The M in RFM.'
;;;
-- ---------------------------------------------------------------------------
-- KEY COLUMN COMMENTS (disambiguation)
-- Note: payment.tender_type value guidance lives in the Payments space text
-- instructions + f_tender_mix (COMMENT ON COLUMN is blocked on silver streaming tables,
-- so the COMMENT ON COLUMN statements below are best-effort and may no-op).
-- ---------------------------------------------------------------------------
COMMENT ON COLUMN jmrdemo.synth_silver.guest_order.sos_breach IS 'TRUE if the order missed its Speed-of-Service (SOS) prep-time target. SOS breach rate = AVG(sos_breach).'
;;;
COMMENT ON COLUMN jmrdemo.synth_silver.guest_order.channel IS 'Order channel: 3pd_delivery (third-party marketplaces), own_delivery (first-party delivery), carryout, catering.'
;;;
COMMENT ON COLUMN jmrdemo.synth_silver.guest_order.total_amount IS 'Order total = subtotal - discount_amount + tax_amount. Use SUM(total_amount) for revenue and AVG(total_amount) for average order value (AOV).'
;;;
COMMENT ON COLUMN jmrdemo.synth_silver.loyalty_transaction.points_delta IS 'Signed points change: positive = points earned, negative = points redeemed.'
;;;
COMMENT ON COLUMN jmrdemo.synth_silver.on_hand_balance.par_level IS 'Target stock level. quantity_on_hand < par_level means the SKU is below par (stockout risk).'
;;;
COMMENT ON COLUMN jmrdemo.synth_silver.time_punch.hours_worked IS 'Labor hours for this punch. SUM for total labor hours; >40 per employee per week indicates overtime.'
;;;
-- ============================================================================
-- TRUSTED SQL FUNCTIONS  (curated, governed "example SQL" registered into spaces)
-- ============================================================================
-- ---- Orders & SOS ----
CREATE OR REPLACE FUNCTION jmrdemo.synth_genie.f_sos_compliance(p_days INT DEFAULT 7)
  RETURNS TABLE(unit_id BIGINT, total_orders BIGINT, sos_breaches BIGINT, sos_breach_rate DOUBLE)
  COMMENT 'Speed-of-Service compliance by store over the last p_days days: order volume, breach count, and breach rate (0..1). Default 7 days.'
  RETURN SELECT unit_id, COUNT(*) AS total_orders, SUM(CASE WHEN sos_breach THEN 1 ELSE 0 END) AS sos_breaches,
                AVG(CASE WHEN sos_breach THEN 1.0 ELSE 0.0 END) AS sos_breach_rate
         FROM jmrdemo.synth_silver.guest_order
         WHERE placed_at >= current_timestamp() - make_interval(0,0,0,p_days,0,0,0)
         GROUP BY unit_id
;;;
CREATE OR REPLACE FUNCTION jmrdemo.synth_genie.f_revenue_by_channel(p_days INT DEFAULT 30)
  RETURNS TABLE(channel STRING, orders BIGINT, revenue DOUBLE, avg_order_value DOUBLE)
  COMMENT 'Revenue, order count, and average order value by channel over the last p_days days. Default 30 days.'
  RETURN SELECT channel, COUNT(*) AS orders, SUM(total_amount) AS revenue, AVG(total_amount) AS avg_order_value
         FROM jmrdemo.synth_silver.guest_order
         WHERE placed_at >= current_timestamp() - make_interval(0,0,0,p_days,0,0,0)
         GROUP BY channel
;;;
CREATE OR REPLACE FUNCTION jmrdemo.synth_genie.f_top_menu_items(p_unit INT, p_days INT DEFAULT 30)
  RETURNS TABLE(menu_item_id BIGINT, item_name STRING, units_sold BIGINT, net_revenue DOUBLE)
  COMMENT 'Top menu items by net revenue for a given store (p_unit) over the last p_days days.'
  RETURN SELECT oi.menu_item_id, mi.item_name, SUM(oi.quantity) AS units_sold, SUM(oi.line_net_amount) AS net_revenue
         FROM jmrdemo.synth_silver.order_item oi
         JOIN jmrdemo.synth_ref.menu_item mi ON mi.menu_item_id = oi.menu_item_id
         WHERE oi.unit_id = p_unit AND oi.placed_at >= current_timestamp() - make_interval(0,0,0,p_days,0,0,0)
         GROUP BY oi.menu_item_id, mi.item_name ORDER BY net_revenue DESC LIMIT 25
;;;
CREATE OR REPLACE FUNCTION jmrdemo.synth_genie.f_late_delivery_rate(p_days INT DEFAULT 7)
  RETURNS TABLE(unit_id BIGINT, deliveries BIGINT, late_deliveries BIGINT, late_rate DOUBLE)
  COMMENT 'Late-delivery rate by store over last p_days days. A delivery is late when actual_delivery_seconds > estimated_delivery_seconds.'
  RETURN SELECT unit_id, COUNT(*) AS deliveries,
                SUM(CASE WHEN actual_delivery_seconds > estimated_delivery_seconds THEN 1 ELSE 0 END) AS late_deliveries,
                AVG(CASE WHEN actual_delivery_seconds > estimated_delivery_seconds THEN 1.0 ELSE 0.0 END) AS late_rate
         FROM jmrdemo.synth_silver.delivery_order
         WHERE created_at >= current_timestamp() - make_interval(0,0,0,p_days,0,0,0)
         GROUP BY unit_id
;;;
-- ---- Loyalty & Rewards ----
CREATE OR REPLACE FUNCTION jmrdemo.synth_genie.f_loyalty_summary(p_days INT DEFAULT 30)
  RETURNS TABLE(unit_id BIGINT, active_members BIGINT, points_earned BIGINT, points_redeemed BIGINT, redemption_rate DOUBLE)
  COMMENT 'Loyalty summary by store over last p_days days: distinct members, points earned, points redeemed, and redemption rate (points_redeemed/points_earned).'
  RETURN SELECT unit_id, COUNT(DISTINCT member_id) AS active_members,
                SUM(CASE WHEN points_delta > 0 THEN points_delta ELSE 0 END) AS points_earned,
                SUM(CASE WHEN points_delta < 0 THEN -points_delta ELSE 0 END) AS points_redeemed,
                COALESCE(SUM(CASE WHEN points_delta < 0 THEN -points_delta ELSE 0 END)
                  / NULLIF(SUM(CASE WHEN points_delta > 0 THEN points_delta ELSE 0 END),0), 0) AS redemption_rate
         FROM jmrdemo.synth_silver.loyalty_transaction
         WHERE transaction_at >= current_timestamp() - make_interval(0,0,0,p_days,0,0,0)
         GROUP BY unit_id
;;;
CREATE OR REPLACE FUNCTION jmrdemo.synth_genie.f_member_vs_nonmember(p_days INT DEFAULT 30)
  RETURNS TABLE(segment STRING, orders BIGINT, revenue DOUBLE, avg_order_value DOUBLE)
  COMMENT 'Compares loyalty members vs non-members on orders, revenue, and AOV over last p_days days. member = guest_order.member_id IS NOT NULL.'
  RETURN SELECT CASE WHEN member_id IS NOT NULL THEN 'member' ELSE 'non_member' END AS segment,
                COUNT(*) AS orders, SUM(total_amount) AS revenue, AVG(total_amount) AS avg_order_value
         FROM jmrdemo.synth_silver.guest_order
         WHERE placed_at >= current_timestamp() - make_interval(0,0,0,p_days,0,0,0)
         GROUP BY CASE WHEN member_id IS NOT NULL THEN 'member' ELSE 'non_member' END
;;;
CREATE OR REPLACE FUNCTION jmrdemo.synth_genie.f_tier_breakdown(p_days INT DEFAULT 30)
  RETURNS TABLE(tier STRING, members BIGINT, transactions BIGINT, net_points BIGINT)
  COMMENT 'Loyalty activity by tier (bronze/silver/gold/platinum) over last p_days days: distinct members, transaction count, net points.'
  RETURN SELECT tier, COUNT(DISTINCT member_id) AS members, COUNT(*) AS transactions, SUM(points_delta) AS net_points
         FROM jmrdemo.synth_silver.loyalty_transaction
         WHERE transaction_at >= current_timestamp() - make_interval(0,0,0,p_days,0,0,0)
         GROUP BY tier
;;;
-- ---- Inventory & Waste ----
CREATE OR REPLACE FUNCTION jmrdemo.synth_genie.f_waste_by_category(p_days INT DEFAULT 30)
  RETURNS TABLE(waste_category STRING, events BIGINT, waste_qty DOUBLE, waste_cost DOUBLE)
  COMMENT 'Waste by category (expired, overproduction, damaged, spoilage, theft) over last p_days days: event count, quantity, and cost.'
  RETURN SELECT waste_category, COUNT(*) AS events, SUM(waste_quantity) AS waste_qty, SUM(waste_cost) AS waste_cost
         FROM jmrdemo.synth_silver.waste_log
         WHERE logged_at >= current_timestamp() - make_interval(0,0,0,p_days,0,0,0)
         GROUP BY waste_category ORDER BY waste_cost DESC
;;;
CREATE OR REPLACE FUNCTION jmrdemo.synth_genie.f_top_waste_stores(p_days INT DEFAULT 30)
  RETURNS TABLE(unit_id BIGINT, events BIGINT, waste_cost DOUBLE)
  COMMENT 'Stores with the highest waste cost over last p_days days.'
  RETURN SELECT unit_id, COUNT(*) AS events, SUM(waste_cost) AS waste_cost
         FROM jmrdemo.synth_silver.waste_log
         WHERE logged_at >= current_timestamp() - make_interval(0,0,0,p_days,0,0,0)
         GROUP BY unit_id ORDER BY waste_cost DESC LIMIT 25
;;;
CREATE OR REPLACE FUNCTION jmrdemo.synth_genie.f_below_par_skus(p_unit INT)
  RETURNS TABLE(stock_sku STRING, quantity_on_hand DOUBLE, par_level DOUBLE, shortfall DOUBLE)
  COMMENT 'SKUs currently below par (stockout risk) for a store: uses the latest on-hand snapshot per SKU where quantity_on_hand < par_level.'
  RETURN WITH latest AS (
           SELECT *, ROW_NUMBER() OVER (PARTITION BY unit_id, stock_sku ORDER BY snapshot_at DESC) rn
           FROM jmrdemo.synth_silver.on_hand_balance WHERE unit_id = p_unit)
         SELECT stock_sku, quantity_on_hand, par_level, (par_level - quantity_on_hand) AS shortfall
         FROM latest WHERE rn = 1 AND quantity_on_hand < par_level ORDER BY shortfall DESC
;;;
-- ---- Workforce & Labor ----
CREATE OR REPLACE FUNCTION jmrdemo.synth_genie.f_labor_hours(p_days INT DEFAULT 7)
  RETURNS TABLE(unit_id BIGINT, labor_hours DOUBLE, employees BIGINT, punches BIGINT)
  COMMENT 'Labor hours by store over last p_days days: total hours_worked, distinct employees, punch count.'
  RETURN SELECT unit_id, SUM(hours_worked) AS labor_hours, COUNT(DISTINCT employee_id) AS employees, COUNT(*) AS punches
         FROM jmrdemo.synth_silver.time_punch
         WHERE punch_in >= current_timestamp() - make_interval(0,0,0,p_days,0,0,0)
         GROUP BY unit_id
;;;
CREATE OR REPLACE FUNCTION jmrdemo.synth_genie.f_sales_per_labor_hour(p_days INT DEFAULT 7)
  RETURNS TABLE(unit_id BIGINT, labor_hours DOUBLE, revenue DOUBLE, sales_per_labor_hour DOUBLE)
  COMMENT 'Labor productivity by store over last p_days days: revenue divided by labor hours (sales per labor hour).'
  RETURN WITH lh AS (SELECT unit_id, SUM(hours_worked) hrs FROM jmrdemo.synth_silver.time_punch
                     WHERE punch_in >= current_timestamp() - make_interval(0,0,0,p_days,0,0,0) GROUP BY unit_id),
              rev AS (SELECT unit_id, SUM(daily_revenue) rev FROM jmrdemo.synth_silver.unit_performance_daily
                      WHERE date >= current_date() - p_days GROUP BY unit_id)
         SELECT lh.unit_id, lh.hrs AS labor_hours, rev.rev AS revenue,
                rev.rev / NULLIF(lh.hrs,0) AS sales_per_labor_hour
         FROM lh JOIN rev ON lh.unit_id = rev.unit_id
;;;
CREATE OR REPLACE FUNCTION jmrdemo.synth_genie.f_overtime_employees(p_days INT DEFAULT 7)
  RETURNS TABLE(employee_id BIGINT, unit_id BIGINT, hours DOUBLE)
  COMMENT 'Employees exceeding 40 hours over the last p_days days (overtime risk).'
  RETURN SELECT employee_id, MAX(unit_id) AS unit_id, SUM(hours_worked) AS hours
         FROM jmrdemo.synth_silver.time_punch
         WHERE punch_in >= current_timestamp() - make_interval(0,0,0,p_days,0,0,0)
         GROUP BY employee_id HAVING SUM(hours_worked) > 40 ORDER BY hours DESC
;;;
-- ---- Demand Risk & External Signals ----
CREATE OR REPLACE FUNCTION jmrdemo.synth_genie.f_demand_risk(p_days INT DEFAULT 7)
  RETURNS TABLE(unit_id BIGINT, metro_area STRING, risk_days BIGINT, avg_demand_multiplier DOUBLE, demand_risk_days BIGINT, capacity_risk_days BIGINT)
  COMMENT 'Demand-risk outlook by store over the next p_days days from metrics.demand_risk_forecast: average combined demand multiplier, count of demand_risk days (slowdown) and capacity_risk days (surge). Use for "which units have the highest demand/capacity risk this week". Default 7 days.'
  RETURN SELECT unit_id, MAX(metro_area) AS metro_area, COUNT(*) AS risk_days,
                AVG(combined_demand_multiplier) AS avg_demand_multiplier,
                SUM(CASE WHEN risk_level = 'demand_risk' THEN 1 ELSE 0 END) AS demand_risk_days,
                SUM(CASE WHEN risk_level = 'capacity_risk' THEN 1 ELSE 0 END) AS capacity_risk_days
         FROM jmrdemo.synth_metrics.demand_risk_forecast
         WHERE forecast_date BETWEEN current_date() AND date_add(current_date(), p_days)
         GROUP BY unit_id ORDER BY avg_demand_multiplier DESC
;;;
-- ---- Franchisee / Executive ----
CREATE OR REPLACE FUNCTION jmrdemo.synth_genie.f_franchisee_scorecard(p_days INT DEFAULT 30)
  RETURNS TABLE(franchisee_id BIGINT, stores BIGINT, revenue DOUBLE, orders BIGINT, avg_order_value DOUBLE, sos_breach_rate DOUBLE, waste_cost DOUBLE, labor_hours DOUBLE)
  COMMENT 'Cross-domain franchisee scorecard over last p_days days: store count, revenue, orders, AOV, SOS breach rate, waste cost, and labor hours per franchisee. Use for ranking/benchmarking franchisees. Default 30 days.'
  RETURN WITH ord AS (
           SELECT u.franchisee_id, COUNT(DISTINCT go.unit_id) AS stores, SUM(go.total_amount) AS revenue,
                  COUNT(*) AS orders, AVG(go.total_amount) AS aov,
                  AVG(CASE WHEN go.sos_breach THEN 1.0 ELSE 0.0 END) AS sos_breach_rate
           FROM jmrdemo.synth_silver.guest_order go JOIN jmrdemo.synth_ref.unit u ON u.unit_id = go.unit_id
           WHERE go.placed_at >= current_timestamp() - make_interval(0,0,0,p_days,0,0,0)
           GROUP BY u.franchisee_id),
         wst AS (
           SELECT u.franchisee_id, SUM(w.waste_cost) AS waste_cost
           FROM jmrdemo.synth_silver.waste_log w JOIN jmrdemo.synth_ref.unit u ON u.unit_id = w.unit_id
           WHERE w.logged_at >= current_timestamp() - make_interval(0,0,0,p_days,0,0,0)
           GROUP BY u.franchisee_id),
         lab AS (
           SELECT u.franchisee_id, SUM(t.hours_worked) AS labor_hours
           FROM jmrdemo.synth_silver.time_punch t JOIN jmrdemo.synth_ref.unit u ON u.unit_id = t.unit_id
           WHERE t.punch_in >= current_timestamp() - make_interval(0,0,0,p_days,0,0,0)
           GROUP BY u.franchisee_id)
         SELECT ord.franchisee_id, ord.stores, ord.revenue, ord.orders, ord.aov AS avg_order_value,
                ord.sos_breach_rate, wst.waste_cost, lab.labor_hours
         FROM ord LEFT JOIN wst ON wst.franchisee_id = ord.franchisee_id
                  LEFT JOIN lab ON lab.franchisee_id = ord.franchisee_id
         ORDER BY ord.revenue DESC
;;;
-- ---- Payments & Tender Mix ----
CREATE OR REPLACE FUNCTION jmrdemo.synth_genie.f_tender_mix(p_days INT DEFAULT 30)
  RETURNS TABLE(tender_type STRING, payments BIGINT, amount DOUBLE, pct_of_amount DOUBLE)
  COMMENT 'Payment tender mix over last p_days days: payment count, total amount, and each tender_type share of total amount (credit_card, digital_wallet, loyalty_redemption, cash). Default 30 days.'
  RETURN SELECT tender_type, COUNT(*) AS payments, SUM(amount) AS amount,
                SUM(amount) / NULLIF(SUM(SUM(amount)) OVER (),0) AS pct_of_amount
         FROM jmrdemo.synth_silver.payment
         WHERE paid_at >= current_timestamp() - make_interval(0,0,0,p_days,0,0,0)
         GROUP BY tender_type ORDER BY amount DESC
;;;
-- ---- Guest / Customer 360 ----
CREATE OR REPLACE FUNCTION jmrdemo.synth_genie.f_guest_churn(p_days INT DEFAULT 30)
  RETURNS TABLE(unit_id BIGINT, total_profiles BIGINT, active_profiles BIGINT, inactive_profiles BIGINT, suspended_profiles BIGINT, inactive_rate DOUBLE)
  COMMENT 'Guest account lifecycle by home store: total profiles and active/inactive/suspended counts plus inactive (churn) rate. p_days filters to profiles created in the last p_days days; use a large value (e.g. 3650) for all-time. Default 30 days.'
  RETURN SELECT unit_id, COUNT(*) AS total_profiles,
                SUM(CASE WHEN account_status = 'active' THEN 1 ELSE 0 END) AS active_profiles,
                SUM(CASE WHEN account_status = 'inactive' THEN 1 ELSE 0 END) AS inactive_profiles,
                SUM(CASE WHEN account_status = 'suspended' THEN 1 ELSE 0 END) AS suspended_profiles,
                AVG(CASE WHEN account_status = 'inactive' THEN 1.0 ELSE 0.0 END) AS inactive_rate
         FROM jmrdemo.synth_silver.guest_profile
         WHERE created_at >= current_timestamp() - make_interval(0,0,0,p_days,0,0,0)
         GROUP BY unit_id ORDER BY inactive_rate DESC
;;;
-- ============================================================================
-- METRIC VIEWS  (semantic / ontology layer)
-- ============================================================================
CREATE OR REPLACE VIEW jmrdemo.synth_genie.metric_orders_sos
  COMMENT 'Orders & SOS metric view: revenue, AOV, SOS breach rate, cancellation rate by store/channel/date.'
  WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: jmrdemo.synth_silver.guest_order
joins:
  - name: unit
    source: jmrdemo.synth_ref.unit
    on: source.unit_id = unit.unit_id
dimensions:
  - name: Order Date
    expr: "DATE(placed_at)"
  - name: Channel
    expr: "channel"
  - name: Order Type
    expr: "order_type"
  - name: Store
    expr: "unit.unit_name"
  - name: Metro
    expr: "unit.metro_area"
  - name: State
    expr: "unit.state"
measures:
  - name: Orders
    expr: "COUNT(1)"
  - name: Revenue
    expr: "SUM(total_amount)"
  - name: Average Order Value
    expr: "AVG(total_amount)"
  - name: SOS Breach Rate
    expr: "AVG(CASE WHEN sos_breach THEN 1.0 ELSE 0.0 END)"
  - name: Cancellation Rate
    expr: "AVG(CASE WHEN order_status = 'cancelled' THEN 1.0 ELSE 0.0 END)"
$$
;;;
CREATE OR REPLACE VIEW jmrdemo.synth_genie.metric_loyalty
  COMMENT 'Loyalty metric view: members, points earned/redeemed, redemption rate by store/tier/date.'
  WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: jmrdemo.synth_silver.loyalty_transaction
joins:
  - name: unit
    source: jmrdemo.synth_ref.unit
    on: source.unit_id = unit.unit_id
dimensions:
  - name: Transaction Date
    expr: "DATE(transaction_at)"
  - name: Tier
    expr: "tier"
  - name: Store
    expr: "unit.unit_name"
  - name: Metro
    expr: "unit.metro_area"
measures:
  - name: Active Members
    expr: "COUNT(DISTINCT member_id)"
  - name: Transactions
    expr: "COUNT(1)"
  - name: Points Earned
    expr: "SUM(CASE WHEN points_delta > 0 THEN points_delta ELSE 0 END)"
  - name: Points Redeemed
    expr: "SUM(CASE WHEN points_delta < 0 THEN -points_delta ELSE 0 END)"
  - name: Redemption Rate
    expr: "SUM(CASE WHEN points_delta < 0 THEN -points_delta ELSE 0 END) / NULLIF(SUM(CASE WHEN points_delta > 0 THEN points_delta ELSE 0 END),0)"
$$
;;;
CREATE OR REPLACE VIEW jmrdemo.synth_genie.metric_waste
  COMMENT 'Inventory waste metric view: waste cost, quantity, and events by store/category/date.'
  WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: jmrdemo.synth_silver.waste_log
joins:
  - name: unit
    source: jmrdemo.synth_ref.unit
    on: source.unit_id = unit.unit_id
dimensions:
  - name: Waste Date
    expr: "DATE(logged_at)"
  - name: Waste Category
    expr: "waste_category"
  - name: Store
    expr: "unit.unit_name"
  - name: Metro
    expr: "unit.metro_area"
measures:
  - name: Waste Cost
    expr: "SUM(waste_cost)"
  - name: Waste Quantity
    expr: "SUM(waste_quantity)"
  - name: Waste Events
    expr: "COUNT(1)"
  - name: Avg Cost per Event
    expr: "SUM(waste_cost) / NULLIF(COUNT(1),0)"
$$
;;;
CREATE OR REPLACE VIEW jmrdemo.synth_genie.metric_labor
  COMMENT 'Workforce & labor metric view: labor hours, employees, and average hours per punch by store/date.'
  WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: jmrdemo.synth_silver.time_punch
joins:
  - name: unit
    source: jmrdemo.synth_ref.unit
    on: source.unit_id = unit.unit_id
dimensions:
  - name: Work Date
    expr: "DATE(punch_in)"
  - name: Store
    expr: "unit.unit_name"
  - name: Metro
    expr: "unit.metro_area"
measures:
  - name: Labor Hours
    expr: "SUM(hours_worked)"
  - name: Employees
    expr: "COUNT(DISTINCT employee_id)"
  - name: Punches
    expr: "COUNT(1)"
  - name: Avg Hours per Punch
    expr: "AVG(hours_worked)"
$$
;;;
CREATE OR REPLACE VIEW jmrdemo.synth_genie.metric_franchisee
  COMMENT 'Franchisee / executive metric view: revenue, orders, AOV, and SOS breach rate rolled up to the franchisee (and store) grain. Use for franchisee ranking and cross-store executive summaries.'
  WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: jmrdemo.synth_silver.guest_order
joins:
  - name: unit
    source: jmrdemo.synth_ref.unit
    on: source.unit_id = unit.unit_id
  - name: franchisee
    source: jmrdemo.synth_ref.franchisee
    on: source.franchisee_id = franchisee.franchisee_id
dimensions:
  - name: Order Date
    expr: "DATE(placed_at)"
  - name: Franchisee
    expr: "franchisee.franchisee_name"
  - name: Store
    expr: "unit.unit_name"
  - name: Metro
    expr: "unit.metro_area"
  - name: Region
    expr: "unit.region_id"
measures:
  - name: Revenue
    expr: "SUM(total_amount)"
  - name: Orders
    expr: "COUNT(1)"
  - name: Average Order Value
    expr: "AVG(total_amount)"
  - name: SOS Breach Rate
    expr: "AVG(CASE WHEN sos_breach THEN 1.0 ELSE 0.0 END)"
  - name: Stores
    expr: "COUNT(DISTINCT unit.unit_id)"
$$
;;;
CREATE OR REPLACE VIEW jmrdemo.synth_genie.metric_delivery
  COMMENT 'Delivery & 3PD metric view: deliveries, late-delivery rate, and avg delivery-time gap by store/metro/channel/date.'
  WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: jmrdemo.synth_silver.delivery_order
joins:
  - name: guest_order
    source: jmrdemo.synth_silver.guest_order
    on: source.guest_order_id = guest_order.guest_order_id
  - name: unit
    source: jmrdemo.synth_ref.unit
    on: source.unit_id = unit.unit_id
dimensions:
  - name: Delivery Date
    expr: "DATE(created_at)"
  - name: Channel
    expr: "guest_order.channel"
  - name: Store
    expr: "unit.unit_name"
  - name: Metro
    expr: "unit.metro_area"
measures:
  - name: Deliveries
    expr: "COUNT(1)"
  - name: Late Deliveries
    expr: "SUM(CASE WHEN actual_delivery_seconds > estimated_delivery_seconds THEN 1 ELSE 0 END)"
  - name: Late Delivery Rate
    expr: "AVG(CASE WHEN actual_delivery_seconds > estimated_delivery_seconds THEN 1.0 ELSE 0.0 END)"
  - name: Avg Delivery Gap Seconds
    expr: "AVG(actual_delivery_seconds - estimated_delivery_seconds)"
$$
;;;
CREATE OR REPLACE VIEW jmrdemo.synth_genie.metric_menu
  COMMENT 'Menu & product metric view: units sold, net revenue, and line count by item/category/daypart/date.'
  WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: jmrdemo.synth_silver.order_item
joins:
  - name: menu_item
    source: jmrdemo.synth_ref.menu_item
    on: source.menu_item_id = menu_item.menu_item_id
  - name: unit
    source: jmrdemo.synth_ref.unit
    on: source.unit_id = unit.unit_id
dimensions:
  - name: Order Date
    expr: "DATE(placed_at)"
  - name: Item
    expr: "menu_item.item_name"
  - name: Category
    expr: "menu_item.category"
  - name: Daypart
    expr: "menu_item.daypart"
  - name: Store
    expr: "unit.unit_name"
measures:
  - name: Units Sold
    expr: "SUM(quantity)"
  - name: Net Revenue
    expr: "SUM(line_net_amount)"
  - name: Line Items
    expr: "COUNT(1)"
$$
;;;
CREATE OR REPLACE VIEW jmrdemo.synth_genie.metric_payments
  COMMENT 'Payments metric view: payment amount, count, and average by tender type/channel/store/date.'
  WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: jmrdemo.synth_silver.payment
joins:
  - name: guest_order
    source: jmrdemo.synth_silver.guest_order
    on: source.guest_order_id = guest_order.guest_order_id
  - name: unit
    source: jmrdemo.synth_ref.unit
    on: source.unit_id = unit.unit_id
dimensions:
  - name: Payment Date
    expr: "DATE(paid_at)"
  - name: Tender Type
    expr: "tender_type"
  - name: Channel
    expr: "guest_order.channel"
  - name: Store
    expr: "unit.unit_name"
  - name: Metro
    expr: "unit.metro_area"
measures:
  - name: Payment Amount
    expr: "SUM(amount)"
  - name: Payments
    expr: "COUNT(1)"
  - name: Average Payment
    expr: "AVG(amount)"
$$
;;;
CREATE OR REPLACE VIEW jmrdemo.synth_genie.metric_guest
  COMMENT 'Guest / customer 360 metric view: profiles and active/inactive/suspended counts + churn rate by store/metro.'
  WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: jmrdemo.synth_silver.guest_profile
joins:
  - name: unit
    source: jmrdemo.synth_ref.unit
    on: source.unit_id = unit.unit_id
dimensions:
  - name: Created Date
    expr: "DATE(created_at)"
  - name: Account Status
    expr: "account_status"
  - name: Store
    expr: "unit.unit_name"
  - name: Metro
    expr: "unit.metro_area"
measures:
  - name: Profiles
    expr: "COUNT(1)"
  - name: Active Profiles
    expr: "SUM(CASE WHEN account_status = 'active' THEN 1 ELSE 0 END)"
  - name: Inactive Profiles
    expr: "SUM(CASE WHEN account_status = 'inactive' THEN 1 ELSE 0 END)"
  - name: Churn Rate
    expr: "AVG(CASE WHEN account_status = 'inactive' THEN 1.0 ELSE 0.0 END)"
$$
;;;
CREATE OR REPLACE VIEW jmrdemo.synth_genie.metric_demand_risk
  COMMENT 'Demand risk metric view: avg combined demand multiplier + demand/capacity risk day counts by store/metro/risk level/date.'
  WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: jmrdemo.synth_metrics.demand_risk_forecast
dimensions:
  - name: Forecast Date
    expr: "forecast_date"
  - name: Metro
    expr: "metro_area"
  - name: Risk Level
    expr: "risk_level"
  - name: Weather Condition
    expr: "weather_condition"
measures:
  - name: Avg Demand Multiplier
    expr: "AVG(combined_demand_multiplier)"
  - name: Demand Risk Days
    expr: "SUM(CASE WHEN risk_level = 'demand_risk' THEN 1 ELSE 0 END)"
  - name: Capacity Risk Days
    expr: "SUM(CASE WHEN risk_level = 'capacity_risk' THEN 1 ELSE 0 END)"
  - name: Store Days
    expr: "COUNT(1)"
$$
;;;
CREATE OR REPLACE VIEW jmrdemo.synth_genie.metric_customer
  COMMENT 'Customer ML metric view: customer count, avg lifetime spend, avg AOV, and avg recency by tier.'
  WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: jmrdemo.synth_features.customer_features
dimensions:
  - name: Tier
    expr: "tier"
measures:
  - name: Customers
    expr: "COUNT(1)"
  - name: Avg Lifetime Spend
    expr: "AVG(monetary_total)"
  - name: Avg Order Value
    expr: "AVG(aov)"
  - name: Avg Recency Days
    expr: "AVG(CASE WHEN recency_days >= 0 THEN recency_days END)"
  - name: Avg Total Orders
    expr: "AVG(total_orders)"
$$
