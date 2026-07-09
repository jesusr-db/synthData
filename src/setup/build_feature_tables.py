# Databricks notebook source
# Build customer & store feature tables, sync to Online Tables, expose a Feature Serving endpoint.
# Reads silver/ref tables, computes features via the pure transforms in src.features.*,
# writes UC Delta feature tables, and (re)creates online tables + a feature serving endpoint.
# COMMAND ----------
import sys

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_notebook_path.replace("/Workspace", "").split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

try:
    catalog_name = dbutils.widgets.get("catalog_name")
except Exception:
    catalog_name = "jmrdemo"

try:
    schema_prefix = dbutils.widgets.get("schema_prefix")
except Exception:
    schema_prefix = "synth_"

print(f"[INFO] build_feature_tables: catalog={catalog_name}, schema_prefix={schema_prefix}")

# COMMAND ----------
from datetime import datetime, timezone
import json
import pandas as pd
from pyspark.sql.types import (StructType, StructField, LongType, DoubleType, StringType)
from databricks.feature_engineering import FeatureEngineeringClient

from src.features.customer_features import compute_customer_features, CATEGORIES
from src.features.store_features import compute_store_features

features_schema = f"{schema_prefix}features"
fq = lambda t: f"{catalog_name}.{features_schema}.{t}"  # noqa: E731

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{features_schema}")

# --- Load source rows (collect to driver; synthetic dataset is small) ---
sp = f"{catalog_name}.{schema_prefix}"
menu_rows = spark.read.table(f"{sp}ref.menu_item").select(
    "menu_item_id", "category", "subcategory", "item_name").collect()
menu = {int(r["menu_item_id"]): (r["category"], r["subcategory"], r["item_name"]) for r in menu_rows}

unit_rows = spark.read.table(f"{sp}ref.unit").select(
    "unit_id", "metro_area", "region_id", "franchisee_id").collect()
units = {int(r["unit_id"]): {"metro_area": r["metro_area"], "region_id": r["region_id"],
                             "franchisee_id": r["franchisee_id"]} for r in unit_rows}

orders = [r.asDict() for r in spark.read.table(f"{sp}silver.guest_order").select(
    "guest_order_id", "unit_id", "profile_id", "total_amount", "placed_at").collect()]
items = [r.asDict() for r in spark.read.table(f"{sp}silver.order_item").select(
    "guest_order_id", "unit_id", "menu_item_id", "quantity", "line_net_amount").collect()]

# latest tier per profile from loyalty_transaction
tier_rows = spark.sql(f"""
    SELECT member_id, tier FROM (
      SELECT member_id, tier,
             ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY transaction_at DESC) rn
      FROM {sp}silver.loyalty_transaction WHERE member_id IS NOT NULL
    ) WHERE rn = 1
""").collect()
tiers = {int(r["member_id"]): r["tier"] for r in tier_rows}

as_of = datetime.now(timezone.utc).replace(tzinfo=None)
cust = compute_customer_features(orders, items, tiers, menu, as_of=as_of)
store = compute_store_features(orders, items, units, menu)
print(f"[INFO] computed {len(cust)} customer rows, {len(store)} store rows")

# --- Write UC feature tables via Feature Engineering ---
fe = FeatureEngineeringClient()

cust_pdf = pd.DataFrame(cust)
cust_sdf = spark.createDataFrame(cust_pdf)
store_pdf = pd.DataFrame([{
    **{k: v for k, v in s.items() if k not in ("popularity", "top_item_per_category")},
    "popularity": json.dumps({str(k): float(v) for k, v in s["popularity"].items()}),
    "top_item_per_category": json.dumps({k: int(v) for k, v in s["top_item_per_category"].items()}),
} for s in store])
store_schema = StructType([
    # unit_id is the FE join key — must be LongType (bigint) to match the LONG
    # store_id lookup key (int -> pandas int64 -> Spark LONG) at train/serve time.
    # IntegerType here triggers "primary key type INTEGER but lookup key LONG" join errors.
    StructField("unit_id", LongType()),
    StructField("metro_area", StringType()),
    StructField("region_id", LongType()),
    StructField("franchisee_id", LongType()),
    StructField("store_orders", LongType()),
    StructField("store_aov", DoubleType()),
    StructField("popularity", StringType()),
    StructField("top_item_per_category", StringType()),
])
store_sdf = spark.createDataFrame(store_pdf, schema=store_schema)

for name, sdf, pk in [("customer_features", cust_sdf, "profile_id"),
                      ("store_features", store_sdf, "unit_id")]:
    table = fq(name)
    try:
        fe.create_table(name=table, primary_keys=pk, df=sdf,
                        description=f"QSR {name} for personalization")
        print(f"[INFO] created feature table {table}")
    except Exception as e:
        print(f"[INFO] feature table {table} exists, writing merge: {e}")
        try:
            fe.write_table(name=table, df=sdf, mode="merge")
        except Exception as e2:
            print(f"[WARN] write_table merge failed for {table}: {e2}")

# --- Enable Change Data Feed (required for TRIGGERED publish to the online store) ---
for _name in ("customer_features", "store_features"):
    try:
        spark.sql(f"ALTER TABLE {fq(_name)} SET TBLPROPERTIES (delta.enableChangeDataFeed = 'true')")
        print(f"[INFO] enabled CDF on {fq(_name)}")
    except Exception as e:
        print(f"[WARN] enable CDF on {fq(_name)} skipped: {e}")

# --- Online feature store (Lakebase-backed; replaces deprecated Online Tables) ---
# Online Tables are deprecated/blocked; the GA path is a Lakebase-backed Online Feature
# Store. Create-or-get the store, wait until AVAILABLE, then publish each feature table.
# Model-serving automatic lookup + the feature-serving endpoint resolve against it.
import time
# Lakebase resource ids allow only [a-z][a-z0-9-]* (<=63 bytes) — no underscores.
online_store_name = f"{schema_prefix.replace('_', '-')}qsr-online-store"
# get_online_store returns None (does not raise) when the store does not exist.
online_store = fe.get_online_store(name=online_store_name)
if online_store is None:
    print(f"[INFO] creating online store {online_store_name} (capacity CU_1)")
    fe.create_online_store(name=online_store_name, capacity="CU_1")
    online_store = fe.get_online_store(name=online_store_name)
else:
    print(f"[INFO] online store {online_store_name} exists (state={getattr(online_store, 'state', None)})")

# Lakebase provisioning can take several minutes; wait until AVAILABLE before publishing.
for _ in range(80):
    online_store = fe.get_online_store(name=online_store_name)
    _state = str(getattr(online_store, "state", ""))
    if "AVAILABLE" in _state:
        break
    if any(b in _state for b in ("FAIL", "ERROR", "DELET")):
        raise RuntimeError(f"online store entered terminal bad state: {_state}")
    print(f"[INFO] online store state={_state}; waiting 15s...")
    time.sleep(15)
print(f"[INFO] online store ready: state={getattr(online_store, 'state', None)}")

# Publish feature tables to the online store.
#
# REPEATABILITY — read before touching this loop:
#   * publish_mode TRIGGERED creates a synced online table; the sync keeps it fresh, so
#     publishing is a ONE-TIME operation (the weekly feature_refresh_job re-runs this
#     notebook and must not fail just because the table is already published).
#   * fe.publish_table is NOT idempotent — on a re-run (setup re-run without destroy, or
#     weekly refresh) it tries to CREATE the destination again and raises
#     AlreadyExists ("Destination table <x>_online already exists").
#   * You CANNOT drop just the online table to retry: fe.drop_online_table raises
#     ValueError("Dropping Databricks online tables is not supported"), and the published
#     table is NOT a UC table (it lives inside the Lakebase store), so DROP TABLE is a
#     no-op. The only supported way to remove a published table is delete_online_store,
#     which destroy_notebook.py does on teardown.
#   * Therefore the correct idempotent behavior here is: publish once; on AlreadyExists,
#     treat it as success (the existing table is already syncing).
#
# The ORIGINAL failure was NOT AlreadyExists — it was that this exception was swallowed as
# a bare [WARN] on the *first* deploy where publish genuinely failed (store not yet
# AVAILABLE / half-provisioned), leaving nothing published, after which both serving
# endpoints failed with "No suitable online store found for feature tables". The
# wait-until-AVAILABLE loop above now guarantees the store is ready before we publish, and
# any publish error OTHER than AlreadyExists is re-raised to fail the task loudly instead
# of silently proceeding to build endpoints on an unpublished feature store.
from databricks.sdk.errors.platform import AlreadyExists as _AlreadyExists
from databricks.sdk import WorkspaceClient as _WSC
_w_sync = _WSC()

def _wait_until_online(_online_fqn, timeout_s=1800, poll_s=20):
    """Block until a synced/online table reaches ONLINE, retrying its backing pipeline on
    failure. publish_table only KICKS OFF an async Lakeflow sync pipeline and returns; that
    pipeline can transiently fail with 'External authorization failed' when the freshly
    provisioned Lakebase instance isn't ready to accept the connection yet. The synced-table
    API is idempotent and documented as retry-on-error, and a manual pipeline restart clears
    the transient failure. Returns when ONLINE; raises if it can't get there in time — we must
    NOT let train_recommender/endpoint creation proceed against a half-synced table (that is
    what produced 'Online table ... is missing required columns')."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        st = _w_sync.database.get_synced_database_table(name=_online_fqn)
        dss = getattr(st, "data_synchronization_status", None)
        state = str(getattr(dss, "detailed_state", "") or "")
        if "ONLINE" in state and "FAILED" not in state:
            print(f"[INFO] {_online_fqn} sync ONLINE (state={state})")
            return
        if "FAILED" in state:
            # Transient (e.g. External authorization failed on a cold instance) — restart the
            # backing pipeline and keep polling. The pipeline id is on the sync status.
            pid = getattr(dss, "pipeline_id", None)
            msg = (getattr(dss, "message", "") or "")[:200]
            if pid:
                try:
                    _w_sync.pipelines.start_update(pipeline_id=pid, full_refresh=True)
                    print(f"[INFO] {_online_fqn} sync FAILED ({msg}); restarted pipeline {pid}, waiting...")
                except Exception as _e:
                    print(f"[WARN] restart of pipeline {pid} for {_online_fqn} failed: {_e}")
            else:
                print(f"[WARN] {_online_fqn} sync FAILED ({msg}) but no pipeline_id to restart; waiting...")
        else:
            print(f"[INFO] {_online_fqn} sync state={state}; waiting {poll_s}s...")
        time.sleep(poll_s)
    raise RuntimeError(f"online table {_online_fqn} did not reach ONLINE within {timeout_s}s")

for _src, _online in [("customer_features", "customer_features_online"),
                      ("store_features", "store_features_online")]:
    try:
        fe.publish_table(online_store=online_store,
                         source_table_name=fq(_src),
                         online_table_name=fq(_online))
        print(f"[INFO] published {fq(_src)} -> {fq(_online)}")
    except _AlreadyExists:
        # Already published on a prior run — the TRIGGERED sync keeps it current. Idempotent.
        print(f"[INFO] {fq(_online)} already published (sync is live); skipping re-publish")
    # Publish is async — block until the sync actually lands (retrying transient pipeline
    # failures) so downstream endpoint creation sees fully-populated online tables.
    _wait_until_online(fq(_online))

# --- Feature Serving endpoint (fold #1: real-time customer look) ---
from databricks.feature_engineering import FeatureLookup
from databricks.feature_engineering.entities.feature_serving_endpoint import (
    EndpointCoreConfig, ServedEntity)

spec_name = fq("customer_store_spec")
features = [
    FeatureLookup(table_name=fq("customer_features"), lookup_key="profile_id",
                  feature_names=["tier", "aov", "recency_days", "total_orders"]
                  + [f"affinity_{c}" for c in CATEGORIES]),
    FeatureLookup(table_name=fq("store_features"), lookup_key="store_id",
                  feature_names=["metro_area", "store_aov", "store_orders"]),
]
try:
    fe.create_feature_spec(name=spec_name, features=features)
    print(f"[INFO] created feature spec {spec_name}")
except Exception as e:
    print(f"[INFO] feature spec exists: {e}")

fs_endpoint = f"{schema_prefix}qsr-customer-features"
try:
    fe.create_feature_serving_endpoint(
        name=fs_endpoint,
        config=EndpointCoreConfig(served_entities=ServedEntity(
            feature_spec_name=spec_name, scale_to_zero_enabled=True)))
    print(f"[INFO] created feature serving endpoint {fs_endpoint}")
except Exception as e:
    print(f"[INFO] feature serving endpoint exists: {e}")

print("[DONE] build_feature_tables complete")
