# Databricks notebook source
# Spark Declarative Pipeline (DLT)
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    TimestampType,
)

catalog = spark.conf.get("pipeline.catalog", "qsr_synth")

# --------------------------------------------------------------------------
# ORDER DOMAIN
# --------------------------------------------------------------------------


@dlt.table(name="guest_order", comment="MVM Silver: guest_order")
@dlt.expect_or_drop("valid_total", "total_amount >= 0")
@dlt.expect_or_drop("valid_unit", "unit_id IS NOT NULL")
def guest_order():
    return (
        spark.readStream.table(f"{catalog}.staging.order_events")
        .filter(F.col("event_type") == "guest_order")
        .select(
            F.col("guest_order_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("channel"),
            F.col("order_type"),
            F.col("order_status"),
            F.col("profile_id").cast(LongType()),
            F.col("member_id").cast(LongType()),
            F.col("subtotal").cast(DoubleType()),
            F.col("discount_amount").cast(DoubleType()),
            F.col("tax_amount").cast(DoubleType()),
            F.col("total_amount").cast(DoubleType()),
            F.col("placed_at").cast(TimestampType()),
            F.col("ready_at").cast(TimestampType()),
            F.col("fulfilled_at").cast(TimestampType()),
            F.col("cancelled_at").cast(TimestampType()),
            F.col("financial_period_id").cast(LongType()),
            F.col("sos_breach").cast(BooleanType()),
            F.current_timestamp().alias("created_at"),
        )
    )


@dlt.table(name="order_item", comment="MVM Silver: order_item")
@dlt.expect_or_drop("positive_price", "unit_price > 0")
def order_item():
    return (
        spark.readStream.table(f"{catalog}.staging.order_events")
        .filter(F.col("event_type") == "order_item")
        .select(
            F.col("order_item_id").cast(LongType()),
            F.col("guest_order_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("menu_item_id").cast(LongType()),
            F.col("quantity").cast(IntegerType()),
            F.col("unit_price").cast(DoubleType()),
            F.col("line_gross_amount").cast(DoubleType()),
            F.col("line_net_amount").cast(DoubleType()),
            F.col("line_discount_amount").cast(DoubleType()),
            F.col("item_status"),
            F.col("waste_flag").cast(BooleanType()),
            F.col("placed_at").cast(TimestampType()),
            F.current_timestamp().alias("created_at"),
        )
    )


@dlt.table(name="payment", comment="MVM Silver: payment")
def payment():
    return (
        spark.readStream.table(f"{catalog}.staging.order_events")
        .filter(F.col("event_type") == "payment")
        .select(
            F.col("payment_id").cast(LongType()),
            F.col("guest_order_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("tender_type"),
            F.col("amount").cast(DoubleType()),
            F.col("settlement_date"),
            F.col("paid_at").cast(TimestampType()),
            F.current_timestamp().alias("created_at"),
        )
    )


@dlt.table(name="status_event", comment="MVM Silver: status_event")
def status_event():
    return (
        spark.readStream.table(f"{catalog}.staging.order_events")
        .filter(F.col("event_type") == "status_event")
        .select(
            F.col("status_event_id").cast(LongType()),
            F.col("guest_order_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("prior_state"),
            F.col("current_state"),
            F.col("event_timestamp").cast(TimestampType()),
            F.col("elapsed_seconds_in_prior_state").cast(IntegerType()),
            F.col("sos_target_seconds").cast(IntegerType()),
            F.col("is_sos_breach").cast(BooleanType()),
            F.current_timestamp().alias("created_at"),
        )
    )


@dlt.table(name="delivery_order", comment="MVM Silver: delivery_order")
def delivery_order():
    return (
        spark.readStream.table(f"{catalog}.staging.order_events")
        .filter(F.col("event_type") == "delivery_order")
        .select(
            F.col("delivery_order_id").cast(LongType()),
            F.col("guest_order_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("platform_order_reference"),
            F.col("estimated_delivery_seconds").cast(IntegerType()),
            F.col("actual_delivery_seconds").cast(IntegerType()),
            F.col("delivery_status"),
            F.current_timestamp().alias("created_at"),
        )
    )


# --------------------------------------------------------------------------
# INVENTORY DOMAIN
# --------------------------------------------------------------------------


@dlt.table(name="on_hand_balance", comment="MVM Silver: on_hand_balance")
@dlt.expect_or_drop("nonnegative_quantity", "quantity_on_hand >= 0")
def on_hand_balance():
    return (
        spark.readStream.table(f"{catalog}.staging.inventory_events")
        .filter(F.col("event_type") == "on_hand_balance")
        .select(
            F.col("on_hand_balance_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("stock_sku"),
            F.col("quantity_on_hand").cast(DoubleType()),
            F.col("quantity_reserved").cast(DoubleType()),
            F.col("par_level").cast(DoubleType()),
            F.col("snapshot_at").cast(TimestampType()),
            F.current_timestamp().alias("created_at"),
        )
    )


@dlt.table(name="waste_log", comment="MVM Silver: waste_log")
def waste_log():
    return (
        spark.readStream.table(f"{catalog}.staging.inventory_events")
        .filter(F.col("event_type") == "waste_log")
        .select(
            F.col("waste_log_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("stock_sku"),
            F.col("waste_quantity").cast(DoubleType()),
            F.col("waste_category"),
            F.col("waste_cost").cast(DoubleType()),
            F.col("logged_at").cast(TimestampType()),
            F.current_timestamp().alias("created_at"),
        )
    )


@dlt.table(name="receiving_order", comment="MVM Silver: receiving_order")
def receiving_order():
    return (
        spark.readStream.table(f"{catalog}.staging.inventory_events")
        .filter(F.col("event_type") == "receiving_order")
        .select(
            F.col("receiving_order_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("stock_sku"),
            F.col("received_quantity").cast(DoubleType()),
            F.col("delivery_date"),
            F.col("quality_inspection_result"),
            F.col("temperature_check_pass").cast(BooleanType()),
            F.current_timestamp().alias("created_at"),
        )
    )


@dlt.table(name="replenishment_order", comment="MVM Silver: replenishment_order")
def replenishment_order():
    return (
        spark.readStream.table(f"{catalog}.staging.inventory_events")
        .filter(F.col("event_type") == "replenishment_order")
        .select(
            F.col("replenishment_order_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("stock_sku"),
            F.col("order_type"),
            F.col("order_quantity").cast(DoubleType()),
            F.col("order_status"),
            F.col("ordered_at").cast(TimestampType()),
            F.current_timestamp().alias("created_at"),
        )
    )


# --------------------------------------------------------------------------
# GUEST DOMAIN
# --------------------------------------------------------------------------


@dlt.table(name="guest_profile", comment="MVM Silver: guest_profile")
def guest_profile():
    return (
        spark.readStream.table(f"{catalog}.staging.guest_events")
        .filter(F.col("event_type") == "guest_profile")
        .select(
            F.col("guest_profile_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("first_name"),
            F.col("last_name"),
            F.col("email"),
            F.col("phone"),
            F.col("zip_code"),
            F.col("created_date"),
            F.col("account_status"),
            F.current_timestamp().alias("created_at"),
        )
    )


@dlt.table(name="digital_account", comment="MVM Silver: digital_account")
def digital_account():
    return (
        spark.readStream.table(f"{catalog}.staging.guest_events")
        .filter(F.col("event_type") == "guest_profile")
        .select(
            F.col("digital_account_id").cast(LongType()),
            F.col("guest_profile_id").cast(LongType()),
            F.col("account_status"),
            F.col("created_date"),
            F.current_timestamp().alias("created_at"),
        )
    )


# --------------------------------------------------------------------------
# LOYALTY DOMAIN
# --------------------------------------------------------------------------


@dlt.table(name="loyalty_transaction", comment="MVM Silver: loyalty_transaction")
def loyalty_transaction():
    return (
        spark.readStream.table(f"{catalog}.staging.loyalty_events")
        .filter(F.col("event_type") == "loyalty_transaction")
        .select(
            F.col("loyalty_transaction_id").cast(LongType()),
            F.col("member_id").cast(LongType()),
            F.col("guest_order_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("transaction_type"),
            F.col("points_delta").cast(IntegerType()),
            F.col("transaction_at").cast(TimestampType()),
            F.col("tier"),
            F.current_timestamp().alias("created_at"),
        )
    )


@dlt.table(name="reward_redemption", comment="MVM Silver: reward_redemption")
def reward_redemption():
    return (
        spark.readStream.table(f"{catalog}.staging.loyalty_events")
        .filter(F.col("event_type") == "reward_redemption")
        .select(
            F.col("reward_redemption_id").cast(LongType()),
            F.col("member_id").cast(LongType()),
            F.col("guest_order_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("points_redeemed").cast(IntegerType()),
            F.col("reward_value").cast(DoubleType()),
            F.col("redeemed_at").cast(TimestampType()),
            F.current_timestamp().alias("created_at"),
        )
    )


# --------------------------------------------------------------------------
# WORKFORCE DOMAIN
# --------------------------------------------------------------------------


@dlt.table(name="shift", comment="MVM Silver: shift")
def shift():
    return (
        spark.readStream.table(f"{catalog}.staging.workforce_events")
        .filter(F.col("event_type") == "shift")
        .select(
            F.col("shift_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("employee_id").cast(LongType()),
            F.col("shift_label"),
            F.col("shift_start").cast(TimestampType()),
            F.col("shift_end").cast(TimestampType()),
            F.col("status"),
            F.col("date"),
            F.current_timestamp().alias("created_at"),
        )
    )


@dlt.table(name="time_punch", comment="MVM Silver: time_punch")
def time_punch():
    return (
        spark.readStream.table(f"{catalog}.staging.workforce_events")
        .filter(F.col("event_type") == "time_punch")
        .select(
            F.col("time_punch_id").cast(LongType()),
            F.col("employee_id").cast(LongType()),
            F.col("unit_id").cast(LongType()),
            F.col("punch_in").cast(TimestampType()),
            F.col("punch_out").cast(TimestampType()),
            F.col("hours_worked").cast(DoubleType()),
            F.current_timestamp().alias("created_at"),
        )
    )


# --------------------------------------------------------------------------
# GOLD LAYER
# --------------------------------------------------------------------------


@dlt.table(name="unit_performance_daily", comment="Gold: daily unit performance")
def unit_performance_daily():
    return (
        dlt.read("guest_order")
        .groupBy(
            F.col("unit_id"),
            F.to_date("placed_at").alias("date"),
        )
        .agg(
            F.count("guest_order_id").alias("order_count"),
            F.sum("total_amount").alias("daily_revenue"),
            F.avg("total_amount").alias("avg_order_value"),
            F.sum(
                F.when(F.col("order_status") == "cancelled", 1).otherwise(0)
            ).alias("cancelled_count"),
        )
    )


@dlt.table(name="sos_compliance_summary", comment="Gold: SOS compliance by unit/channel/date")
def sos_compliance_summary():
    return (
        dlt.read("status_event")
        .filter(F.col("current_state") == "ready")
        .join(
            dlt.read("guest_order").select("guest_order_id", "channel", "placed_at"),
            "guest_order_id",
        )
        .groupBy(
            F.col("unit_id"),
            F.col("channel"),
            F.to_date("placed_at").alias("date"),
        )
        .agg(
            F.count("status_event_id").alias("total_orders"),
            F.sum(F.col("is_sos_breach").cast(IntegerType())).alias("sos_breaches"),
            F.avg("elapsed_seconds_in_prior_state").alias("avg_prep_seconds"),
        )
        .withColumn(
            "sos_compliance_pct",
            F.round(1.0 - F.col("sos_breaches") / F.col("total_orders"), 4),
        )
    )


@dlt.table(name="loyalty_cohort_metrics", comment="Gold: loyalty cohort metrics by tier/date")
def loyalty_cohort_metrics():
    return (
        dlt.read("loyalty_transaction")
        .groupBy(
            F.col("unit_id"),
            F.col("tier"),
            F.to_date("transaction_at").alias("date"),
        )
        .agg(
            F.countDistinct("member_id").alias("active_members"),
            F.sum("points_delta").alias("total_points_earned"),
            F.count("loyalty_transaction_id").alias("transaction_count"),
        )
    )


@dlt.table(name="inventory_waste_summary", comment="Gold: inventory waste by unit/date")
def inventory_waste_summary():
    return (
        dlt.read("waste_log")
        .groupBy(
            F.col("unit_id"),
            F.to_date("logged_at").alias("date"),
            F.col("waste_category"),
        )
        .agg(
            F.sum("waste_cost").alias("total_waste_cost"),
            F.sum("waste_quantity").alias("total_waste_qty"),
            F.count("waste_log_id").alias("waste_event_count"),
        )
    )
