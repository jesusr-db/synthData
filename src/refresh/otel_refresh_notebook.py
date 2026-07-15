# Databricks notebook source
# COMMAND ----------
import sys

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_notebook_path.replace("/Workspace", "").split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

# COMMAND ----------
def _widget(name, default):
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return default

catalog_name  = _widget("catalog_name",  "jmrdemo")
schema_prefix = _widget("schema_prefix", "synth_")
otel_catalog  = _widget("otel_catalog",  "jmrdemo")
otel_schema   = _widget("otel_schema",   "zerobus")
mode          = _widget("mode",          "incremental")  # incremental | backfill

print(
    f"[INFO] otel_refresh_notebook: catalog={catalog_name}, schema_prefix={schema_prefix}, "
    f"otel_source={otel_catalog}.{otel_schema}, mode={mode}"
)

# COMMAND ----------
try:
    from datetime import datetime
    from collections import defaultdict

    from src.refresh.otel_order_adapter import reshape_otel_orders

    # -------------------------------------------------------------------
    # 1. High-water-mark  (skipped in backfill mode)
    # -------------------------------------------------------------------
    since_ts = None
    if mode == "incremental":
        try:
            hwm_row = spark.sql(
                f"SELECT MAX(event_ts) AS ts "
                f"FROM {catalog_name}.{schema_prefix}staging.order_events "
                f"WHERE source = 'otel'"
            ).collect()[0]
            if hwm_row.ts is not None:
                since_ts = hwm_row.ts.replace(tzinfo=None) if hasattr(hwm_row.ts, "tzinfo") else hwm_row.ts
            print(f"[INFO] OTel HWM: since_ts={since_ts}")
        except Exception as e:
            print(f"[WARN] HWM query failed — running without filter: {e}")

    # -------------------------------------------------------------------
    # 2. Load unit map (state-biased pool for store→unit mapping)
    # -------------------------------------------------------------------
    unit_ids_by_state: dict[str, list[int]] = {}
    all_unit_ids: list[int] = []
    try:
        unit_rows = spark.sql(
            f"SELECT unit_id, state FROM {catalog_name}.{schema_prefix}ref.unit"
        ).collect()
        all_unit_ids = [r.unit_id for r in unit_rows]
        by_state: dict[str, list[int]] = defaultdict(list)
        for r in unit_rows:
            if r.state:
                by_state[r.state].append(r.unit_id)
        unit_ids_by_state = dict(by_state)
        print(f"[INFO] Unit map loaded: {len(all_unit_ids)} units across {len(unit_ids_by_state)} states")
    except Exception as e:
        print(f"[WARN] Unit map load failed — using empty pool (orders will be skipped): {e}")

    if not all_unit_ids:
        print("[WARN] No units available — otel adapter skipped")
        raise SystemExit(0)

    # -------------------------------------------------------------------
    # 3. Read otel_logs — hoisting attributes map to top-level columns
    #    Filtered to order-bearing rows (amount > 0) in spark, then collect.
    # -------------------------------------------------------------------
    log_rows: list[dict] = []
    try:
        logs_table = f"{otel_catalog}.{otel_schema}.otel_logs"
        # Build time filter clause for incremental mode
        ts_filter = ""
        if since_ts is not None:
            ts_filter = f"AND event_ts > TIMESTAMP '{since_ts.isoformat()}'"

        log_df = spark.sql(f"""
            SELECT
                trace_id,
                CAST(time_unix_nano AS BIGINT)       AS time_unix_nano,
                CAST(to_timestamp(time_unix_nano / 1e9) AS TIMESTAMP) AS event_ts,
                attributes['app.order.id']           AS `app.order.id`,
                attributes['app.order.member_id']    AS `app.order.member_id`,
                CAST(attributes['app.order.amount'] AS DOUBLE) AS `app.order.amount`,
                CAST(attributes['app.order.items.count'] AS INT) AS `app.order.items.count`,
                CAST(attributes['app.shipping.amount'] AS DOUBLE) AS `app.shipping.amount`,
                attributes['app.shipping.tracking.id'] AS `app.shipping.tracking.id`,
                attributes['user.id']                AS `user.id`
            FROM {logs_table}
            WHERE attributes['app.order.amount'] IS NOT NULL
              AND CAST(attributes['app.order.amount'] AS DOUBLE) > 0
              {ts_filter}
        """)
        log_rows = [row.asDict() for row in log_df.collect()]
        print(f"[INFO] Loaded {len(log_rows)} otel log rows")
    except Exception as e:
        print(f"[WARN] otel_logs read failed — adapter will run with empty logs: {e}")

    # -------------------------------------------------------------------
    # 4. Read otel_spans — order-tracker + stage spans
    # -------------------------------------------------------------------
    span_rows: list[dict] = []
    try:
        spans_table = f"{otel_catalog}.{otel_schema}.otel_spans"
        # Collect trace_ids from log rows to limit span query scope
        if log_rows:
            trace_ids_csv = ",".join(f"'{r['trace_id']}'" for r in log_rows if r.get("trace_id"))
            trace_filter = f"AND trace_id IN ({trace_ids_csv})" if trace_ids_csv else ""
        else:
            trace_filter = "AND 1=0"  # no log rows → no spans needed

        span_df = spark.sql(f"""
            SELECT
                trace_id,
                name,
                attributes['order.id']              AS `order.id`,
                attributes['order.store_id']         AS `order.store_id`,
                attributes['order.channel']          AS `order.channel`,
                attributes['order.skus']             AS `order.skus`,
                CAST(attributes['order.item_count'] AS INT) AS `order.item_count`,
                CAST(attributes['order.prep_seconds'] AS INT) AS `order.prep_seconds`,
                attributes['order.location.state']   AS `order.location.state`,
                attributes['order.location.city']    AS `order.location.city`,
                attributes['order.location.zip']     AS `order.location.zip`,
                CAST(attributes['sos.target_seconds'] AS INT) AS `sos.target_seconds`
            FROM {spans_table}
            WHERE (name = 'order-tracker received order' OR name LIKE 'stage:%')
              {trace_filter}
        """)
        span_rows = [row.asDict() for row in span_df.collect()]
        print(f"[INFO] Loaded {len(span_rows)} otel span rows")
    except Exception as e:
        print(f"[WARN] otel_spans read failed — adapter will run without span enrichment: {e}")

    # -------------------------------------------------------------------
    # 5. Reshape
    # -------------------------------------------------------------------
    envelope_rows = reshape_otel_orders(
        log_rows, span_rows, unit_ids_by_state, all_unit_ids, since_ts=since_ts
    )

    if not envelope_rows:
        print("[INFO] No new otel orders to write — skipping append")
        raise SystemExit(0)

    print(f"[INFO] Reshaping complete: {len(envelope_rows)} envelope rows")

    # -------------------------------------------------------------------
    # 6. Append — reuse the write_batch cleaning idiom (append only, no MERGE)
    # -------------------------------------------------------------------
    from pyspark.sql import Row

    order_events_table = f"{catalog_name}.{schema_prefix}staging.order_events"

    # Group by event_type so createDataFrame gets a uniform schema per call
    by_event_type: dict[str, list[dict]] = defaultdict(list)
    for row in envelope_rows:
        by_event_type[row["event_type"]].append(row)

    total_written = 0
    for event_type, event_rows in by_event_type.items():
        # Drop all-None columns (PySpark cannot infer type for fully-null columns)
        all_keys = {k for r in event_rows for k in r}
        present_keys = {k for k in all_keys if any(r.get(k) is not None for r in event_rows)}
        cleaned = [{k: r.get(k) for k in present_keys} for r in event_rows]
        df = spark.createDataFrame(cleaned)
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(order_events_table)
        total_written += len(cleaned)

    print(f"[OK] Appended {total_written} rows to {order_events_table} (source=otel)")

except SystemExit:
    pass
except Exception as _otel_exc:
    print(f"[WARN] otel adapter skipped: {_otel_exc}")
