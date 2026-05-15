# Databricks notebook source — Spark Declarative Pipeline (DLT)
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
