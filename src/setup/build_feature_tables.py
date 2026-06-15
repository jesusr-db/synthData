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

# Publish feature tables to the online store (default publish_mode TRIGGERED = incremental
# sync; re-running this notebook weekly re-triggers the sync to refresh online values).
for _src, _online in [("customer_features", "customer_features_online"),
                      ("store_features", "store_features_online")]:
    try:
        fe.publish_table(online_store=online_store,
                         source_table_name=fq(_src),
                         online_table_name=fq(_online))
        print(f"[INFO] published {fq(_src)} -> {fq(_online)}")
    except Exception as e:
        print(f"[WARN] publish {fq(_src)} failed: {e}")

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
