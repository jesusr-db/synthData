# Databricks notebook source
# Spark Declarative Pipeline (Lakeflow Declarative Pipelines)
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    TimestampType,
)

catalog = spark.conf.get("pipeline.catalog", "qsr_synth")
schema_prefix = spark.conf.get("pipeline.schema_prefix", "synth_")


def _unit_franchisee():
    """Lookup helper: returns (unit_id, franchisee_id, region_id) from ref.unit, broadcast-friendly."""
    return spark.read.table(f"{catalog}.{schema_prefix}ref.unit").select(
        "unit_id", "franchisee_id", "region_id"
    )

# --------------------------------------------------------------------------
# ORDER DOMAIN
# --------------------------------------------------------------------------


@dp.table(
    name="guest_order",
    comment="Each row is a completed or in-flight customer order placed at a QSR unit.",
    schema="""
        guest_order_id      BIGINT    COMMENT 'Surrogate primary key for the order.',
        unit_id             BIGINT    COMMENT 'Restaurant unit where the order was placed.',
        franchisee_id       BIGINT    COMMENT 'Franchisee owner of the unit (from ref.unit).',
        region_id           BIGINT    COMMENT 'Geographic region of the unit (from ref.unit).',
        channel             STRING    COMMENT 'Order channel: carryout, own_delivery, 3pd_delivery, catering.',
        order_type          STRING    COMMENT 'Broad order classification: dine_in, takeout, delivery.',
        order_status        STRING    COMMENT 'Current order state: placed, in_progress, ready, fulfilled, cancelled.',
        profile_id          BIGINT    COMMENT 'FK to guest_profile; null for anonymous orders.',
        member_id           BIGINT,
        subtotal            DOUBLE,
        discount_amount     DOUBLE    COMMENT 'Dollar value of promotions or coupons applied.',
        tax_amount          DOUBLE,
        total_amount        DOUBLE    COMMENT 'Total order revenue including items, taxes, and fees.',
        placed_at           TIMESTAMP COMMENT 'Timestamp when the order was submitted by the customer.',
        ready_at            TIMESTAMP,
        fulfilled_at        TIMESTAMP,
        cancelled_at        TIMESTAMP,
        financial_period_id BIGINT,
        sos_breach          BOOLEAN   COMMENT 'True if the order exceeded the speed-of-service target for its channel.',
        created_at          TIMESTAMP,
        CONSTRAINT pk_guest_order PRIMARY KEY (guest_order_id) NOT ENFORCED,
        CONSTRAINT fk_guest_order_profile FOREIGN KEY (profile_id) REFERENCES guest_profile(guest_profile_id) NOT ENFORCED
    """,
)
@dp.expect_or_drop("valid_total", "total_amount >= 0")
@dp.expect_or_drop("valid_unit", "unit_id IS NOT NULL")
def guest_order():
    ref_unit = _unit_franchisee()
    df = (
        spark.readStream.table(f"{catalog}.{schema_prefix}staging.order_events")
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
    return df.join(broadcast(ref_unit), on="unit_id", how="left")


@dp.table(
    name="order_item",
    comment="Line items attached to a guest order, one row per menu item ordered.",
    schema="""
        order_item_id        BIGINT  COMMENT 'Surrogate primary key for the line item.',
        guest_order_id       BIGINT  COMMENT 'FK to guest_order.',
        unit_id              BIGINT,
        menu_item_id         BIGINT,
        quantity             INT     COMMENT 'Number of units of this item in the order.',
        unit_price           DOUBLE  COMMENT 'Price charged per unit of this item.',
        line_gross_amount    DOUBLE,
        line_net_amount      DOUBLE,
        line_discount_amount DOUBLE,
        item_status          STRING,
        waste_flag           BOOLEAN COMMENT 'True if this item was later flagged as waste.',
        placed_at            TIMESTAMP,
        created_at           TIMESTAMP,
        CONSTRAINT pk_order_item PRIMARY KEY (order_item_id) NOT ENFORCED,
        CONSTRAINT fk_order_item_guest_order FOREIGN KEY (guest_order_id) REFERENCES guest_order(guest_order_id) NOT ENFORCED
    """,
)
@dp.expect_or_drop("positive_price", "unit_price > 0")
def order_item():
    return (
        spark.readStream.table(f"{catalog}.{schema_prefix}staging.order_events")
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


@dp.table(
    name="payment",
    comment="Payment transactions linked to guest orders, capturing tender type and amounts.",
    schema="""
        payment_id      BIGINT,
        guest_order_id  BIGINT,
        unit_id         BIGINT,
        tender_type     STRING,
        amount          DOUBLE,
        settlement_date STRING,
        paid_at         TIMESTAMP,
        created_at      TIMESTAMP,
        CONSTRAINT pk_payment PRIMARY KEY (payment_id) NOT ENFORCED,
        CONSTRAINT fk_payment_guest_order FOREIGN KEY (guest_order_id) REFERENCES guest_order(guest_order_id) NOT ENFORCED
    """,
)
def payment():
    return (
        spark.readStream.table(f"{catalog}.{schema_prefix}staging.order_events")
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


@dp.table(
    name="status_event",
    comment="Timestamped status transitions for guest orders (e.g., placed, in_progress, ready, delivered).",
    schema="""
        status_event_id                BIGINT,
        guest_order_id                 BIGINT,
        unit_id                        BIGINT,
        prior_state                    STRING,
        current_state                  STRING,
        event_timestamp                TIMESTAMP,
        elapsed_seconds_in_prior_state INT,
        sos_target_seconds             INT,
        is_sos_breach                  BOOLEAN,
        created_at                     TIMESTAMP,
        CONSTRAINT pk_status_event PRIMARY KEY (status_event_id) NOT ENFORCED,
        CONSTRAINT fk_status_event_guest_order FOREIGN KEY (guest_order_id) REFERENCES guest_order(guest_order_id) NOT ENFORCED
    """,
)
def status_event():
    return (
        spark.readStream.table(f"{catalog}.{schema_prefix}staging.order_events")
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


@dp.table(
    name="delivery_order",
    comment="Delivery metadata for orders fulfilled via own or third-party delivery channels.",
    schema="""
        delivery_order_id          BIGINT,
        guest_order_id             BIGINT,
        unit_id                    BIGINT,
        platform_order_reference   STRING,
        estimated_delivery_seconds INT,
        actual_delivery_seconds    INT,
        delivery_status            STRING,
        created_at                 TIMESTAMP,
        CONSTRAINT pk_delivery_order PRIMARY KEY (delivery_order_id) NOT ENFORCED,
        CONSTRAINT fk_delivery_order_guest_order FOREIGN KEY (guest_order_id) REFERENCES guest_order(guest_order_id) NOT ENFORCED
    """,
)
def delivery_order():
    return (
        spark.readStream.table(f"{catalog}.{schema_prefix}staging.order_events")
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


@dp.table(
    name="on_hand_balance",
    comment="Daily snapshot of on-hand inventory quantity and dollar value by SKU and unit.",
    schema="""
        on_hand_balance_id BIGINT,
        unit_id            BIGINT,
        stock_sku          STRING,
        quantity_on_hand   DOUBLE,
        quantity_reserved  DOUBLE,
        par_level          DOUBLE,
        snapshot_at        TIMESTAMP,
        created_at         TIMESTAMP,
        CONSTRAINT pk_on_hand_balance PRIMARY KEY (on_hand_balance_id) NOT ENFORCED
    """,
)
@dp.expect_or_drop("nonnegative_quantity", "quantity_on_hand >= 0")
def on_hand_balance():
    return (
        spark.readStream.table(f"{catalog}.{schema_prefix}staging.inventory_events")
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


@dp.table(
    name="waste_log",
    comment="Recorded inventory waste events by SKU, unit, and waste category.",
    schema="""
        waste_log_id   BIGINT COMMENT 'Surrogate primary key for the waste event.',
        unit_id        BIGINT COMMENT 'Restaurant unit where waste was recorded.',
        franchisee_id  BIGINT COMMENT 'Franchisee owner of the unit (from ref.unit).',
        region_id      BIGINT COMMENT 'Geographic region of the unit (from ref.unit).',
        stock_sku      STRING COMMENT 'Inventory SKU of the wasted item.',
        waste_quantity DOUBLE,
        waste_category STRING COMMENT 'Reason for waste: spoilage, over_prep, damage, expiry.',
        waste_cost     DOUBLE,
        logged_at      TIMESTAMP,
        created_at     TIMESTAMP,
        CONSTRAINT pk_waste_log PRIMARY KEY (waste_log_id) NOT ENFORCED
    """,
)
def waste_log():
    ref_unit = _unit_franchisee()
    df = (
        spark.readStream.table(f"{catalog}.{schema_prefix}staging.inventory_events")
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
    return df.join(broadcast(ref_unit), on="unit_id", how="left")


@dp.table(
    name="receiving_order",
    comment="Inbound inventory receiving records from suppliers to restaurant units.",
    schema="""
        receiving_order_id       BIGINT,
        unit_id                  BIGINT,
        stock_sku                STRING,
        received_quantity        DOUBLE,
        delivery_date            STRING,
        quality_inspection_result STRING,
        temperature_check_pass   BOOLEAN,
        created_at               TIMESTAMP,
        CONSTRAINT pk_receiving_order PRIMARY KEY (receiving_order_id) NOT ENFORCED
    """,
)
def receiving_order():
    return (
        spark.readStream.table(f"{catalog}.{schema_prefix}staging.inventory_events")
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


@dp.table(
    name="replenishment_order",
    comment="System-generated replenishment orders triggered when stock falls below par level.",
    schema="""
        replenishment_order_id BIGINT,
        unit_id                BIGINT,
        stock_sku              STRING,
        order_type             STRING,
        order_quantity         DOUBLE,
        order_status           STRING,
        ordered_at             TIMESTAMP,
        created_at             TIMESTAMP,
        CONSTRAINT pk_replenishment_order PRIMARY KEY (replenishment_order_id) NOT ENFORCED
    """,
)
def replenishment_order():
    return (
        spark.readStream.table(f"{catalog}.{schema_prefix}staging.inventory_events")
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


@dp.view(name="guest_profile_changes")
def guest_profile_changes_view():
    ref_unit = _unit_franchisee()
    df = (
        spark.readStream.table(f"{catalog}.{schema_prefix}staging.guest_events")
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
            F.col("event_ts").alias("created_at"),
        )
    )
    return df.join(broadcast(ref_unit), on="unit_id", how="left")


dp.create_streaming_table(
    name="guest_profile",
    comment="Customer profile record created at loyalty enrollment or first online order.",
    schema="""
        guest_profile_id BIGINT  COMMENT 'Surrogate primary key for the guest profile.',
        unit_id          BIGINT,
        franchisee_id    BIGINT  COMMENT 'Franchisee owner of the unit (from ref.unit).',
        region_id        BIGINT  COMMENT 'Geographic region of the unit (from ref.unit).',
        first_name       STRING,
        last_name        STRING,
        email            STRING,
        phone            STRING,
        zip_code         STRING,
        created_date     STRING,
        account_status   STRING  COMMENT 'Profile state: active, inactive, suspended.',
        created_at       TIMESTAMP,
        CONSTRAINT pk_guest_profile PRIMARY KEY (guest_profile_id) NOT ENFORCED
    """,
)

dp.create_auto_cdc_flow(
    target="guest_profile",
    source="guest_profile_changes",
    keys=["guest_profile_id"],
    sequence_by=F.col("created_at"),
    stored_as_scd_type=1,
)


@dp.table(
    name="digital_account",
    comment="Digital account and app credentials linked to a guest profile.",
    schema="""
        digital_account_id BIGINT,
        guest_profile_id   BIGINT,
        account_status     STRING,
        created_date       STRING,
        created_at         TIMESTAMP,
        CONSTRAINT pk_digital_account PRIMARY KEY (digital_account_id) NOT ENFORCED
    """,
)
def digital_account():
    return (
        spark.readStream.table(f"{catalog}.{schema_prefix}staging.guest_events")
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


@dp.table(
    name="loyalty_transaction",
    comment="Points ledger for loyalty program earn and redeem events.",
    schema="""
        loyalty_transaction_id BIGINT    COMMENT 'Surrogate primary key for the loyalty event.',
        member_id              BIGINT    COMMENT 'FK to guest_profile (loyalty member).',
        guest_order_id         BIGINT,
        unit_id                BIGINT,
        franchisee_id          BIGINT    COMMENT 'Franchisee owner of the unit (from ref.unit).',
        region_id              BIGINT    COMMENT 'Geographic region of the unit (from ref.unit).',
        transaction_type       STRING    COMMENT 'earn or redeem.',
        points_delta           INT       COMMENT 'Points added (positive) or subtracted (negative) in this event.',
        transaction_at         TIMESTAMP,
        tier                   STRING    COMMENT 'Loyalty tier at the time of the transaction: bronze, silver, gold, platinum.',
        created_at             TIMESTAMP,
        CONSTRAINT pk_loyalty_transaction PRIMARY KEY (loyalty_transaction_id) NOT ENFORCED,
        CONSTRAINT fk_loyalty_transaction_guest_order FOREIGN KEY (guest_order_id) REFERENCES guest_order(guest_order_id) NOT ENFORCED
    """,
)
def loyalty_transaction():
    ref_unit = _unit_franchisee()
    df = (
        spark.readStream.table(f"{catalog}.{schema_prefix}staging.loyalty_events")
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
    return df.join(broadcast(ref_unit), on="unit_id", how="left")


@dp.table(
    name="reward_redemption",
    comment="Records of loyalty reward redemptions applied to specific orders.",
    schema="""
        reward_redemption_id BIGINT,
        member_id            BIGINT,
        guest_order_id       BIGINT,
        unit_id              BIGINT,
        points_redeemed      INT,
        reward_value         DOUBLE,
        redeemed_at          TIMESTAMP,
        created_at           TIMESTAMP,
        CONSTRAINT pk_reward_redemption PRIMARY KEY (reward_redemption_id) NOT ENFORCED,
        CONSTRAINT fk_reward_redemption_guest_order FOREIGN KEY (guest_order_id) REFERENCES guest_order(guest_order_id) NOT ENFORCED
    """,
)
def reward_redemption():
    return (
        spark.readStream.table(f"{catalog}.{schema_prefix}staging.loyalty_events")
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


@dp.table(
    name="shift",
    comment="Scheduled employee shifts at a unit, with planned start/end times.",
    schema="""
        shift_id    BIGINT,
        unit_id     BIGINT,
        employee_id BIGINT,
        shift_label STRING,
        shift_start TIMESTAMP,
        shift_end   TIMESTAMP,
        status      STRING,
        date        STRING,
        created_at  TIMESTAMP,
        CONSTRAINT pk_shift PRIMARY KEY (shift_id) NOT ENFORCED
    """,
)
def shift():
    return (
        spark.readStream.table(f"{catalog}.{schema_prefix}staging.workforce_events")
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


@dp.table(
    name="time_punch",
    comment="Actual clock-in/clock-out records for employees within a shift.",
    schema="""
        time_punch_id BIGINT,
        employee_id   BIGINT,
        unit_id       BIGINT,
        franchisee_id BIGINT COMMENT 'Franchisee owner of the unit (from ref.unit).',
        region_id     BIGINT COMMENT 'Geographic region of the unit (from ref.unit).',
        punch_in      TIMESTAMP,
        punch_out     TIMESTAMP,
        hours_worked  DOUBLE,
        created_at    TIMESTAMP,
        CONSTRAINT pk_time_punch PRIMARY KEY (time_punch_id) NOT ENFORCED
    """,
)
def time_punch():
    ref_unit = _unit_franchisee()
    df = (
        spark.readStream.table(f"{catalog}.{schema_prefix}staging.workforce_events")
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
    return df.join(broadcast(ref_unit), on="unit_id", how="left")


# --------------------------------------------------------------------------
# GOLD LAYER
# --------------------------------------------------------------------------


@dp.table(name="unit_performance_daily", comment="Daily rollup of order volume, revenue, and SOS metrics per restaurant unit.")
def unit_performance_daily():
    return (
        dp.read("guest_order")
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


@dp.table(name="sos_compliance_summary", comment="Speed-of-service compliance summary by unit, channel, and order type.")
def sos_compliance_summary():
    return (
        dp.read("status_event")
        .filter(F.col("current_state") == "ready")
        .join(
            dp.read("guest_order").select("guest_order_id", "channel", "placed_at"),
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


@dp.table(name="loyalty_cohort_metrics", comment="Monthly loyalty program cohort metrics: active members, points earned and redeemed.")
def loyalty_cohort_metrics():
    return (
        dp.read("loyalty_transaction")
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


@dp.table(name="inventory_waste_summary", comment="Monthly inventory waste summary by unit and stock SKU.")
def inventory_waste_summary():
    return (
        dp.read("waste_log")
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
