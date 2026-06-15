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
import pandas as pd
from pyspark.sql.types import (StructType, StructField, IntegerType, FloatType,
                               StringType, MapType, LongType)
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
    "popularity": {int(k): float(v) for k, v in s["popularity"].items()},
    "top_item_per_category": {k: int(v) for k, v in s["top_item_per_category"].items()},
} for s in store])
store_schema = StructType([
    StructField("unit_id", IntegerType()),
    StructField("metro_area", StringType()),
    StructField("region_id", IntegerType()),
    StructField("franchisee_id", IntegerType()),
    StructField("store_orders", IntegerType()),
    StructField("store_aov", FloatType()),
    StructField("popularity", MapType(IntegerType(), FloatType())),
    StructField("top_item_per_category", MapType(StringType(), IntegerType())),
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

# --- Online tables (idempotent) ---
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    OnlineTable, OnlineTableSpec, OnlineTableSpecTriggeredSchedulingPolicy)
w = WorkspaceClient()

def ensure_online_table(source_table: str, online_name: str, pk: str):
    online_fq = fq(online_name)
    spec = OnlineTableSpec(
        source_table_full_name=source_table,
        primary_key_columns=[pk],
        run_triggered=OnlineTableSpecTriggeredSchedulingPolicy.from_dict({"triggered": "true"}),
        perform_full_copy=True,
    )
    try:
        w.online_tables.create(table=OnlineTable(name=online_fq, spec=spec))
        print(f"[INFO] created online table {online_fq}")
    except Exception as e:
        # already exists -> trigger a refresh to pick up new feature values
        print(f"[INFO] online table {online_fq} exists ({e}); triggering pipeline refresh")
        try:
            ot = w.online_tables.get(name=online_fq)
            if ot.spec and ot.spec.pipeline_id:  # best-effort; verify pipeline_id path at deploy time
                w.pipelines.start_update(pipeline_id=ot.spec.pipeline_id, full_refresh=True)
        except Exception as e2:
            print(f"[WARN] online refresh skipped: {e2}")

ensure_online_table(fq("customer_features"), "customer_features_online", "profile_id")
ensure_online_table(fq("store_features"), "store_features_online", "unit_id")

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
