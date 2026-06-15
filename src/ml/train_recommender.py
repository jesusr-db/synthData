# Databricks notebook source
# Train the basket-aware recommender, log via Feature Engineering (automatic lookup),
# register in UC, and (re)create the Model Serving endpoint.
import json
import random
import tempfile

import sys

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_notebook_path.replace("/Workspace", "").split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

try:
    catalog_name = dbutils.widgets.get("catalog_name")
    schema_prefix = dbutils.widgets.get("schema_prefix")
except Exception:
    catalog_name = "jmrdemo"
    schema_prefix = "synth_"

sp = f"{catalog_name}.{schema_prefix}"
features_schema = f"{schema_prefix}features"
fq = lambda t: f"{catalog_name}.{features_schema}.{t}"  # noqa: E731
random.seed(42)

from src.features.affinity import load_affinity, cart_categories
from src.ml.features_vector import build_feature_vector, FEATURE_NAMES

# --- Menu + affinity artifacts ---
menu_rows = spark.read.table(f"{sp}ref.menu_item").select(
    "menu_item_id", "category", "subcategory", "item_name").collect()
menu = {int(r["menu_item_id"]): (r["category"], r["subcategory"], r["item_name"]) for r in menu_rows}
cfg = load_affinity()
all_item_ids = list(menu.keys())

# --- Build training examples from historical orders (positives = items in order;
#     negatives = sampled items not in order, scored against the partial basket). ---
orders = spark.read.table(f"{sp}silver.guest_order").select(
    "guest_order_id", "profile_id", "unit_id").collect()
items_by_order = {}
for r in spark.read.table(f"{sp}silver.order_item").select("guest_order_id", "menu_item_id").collect():
    items_by_order.setdefault(r["guest_order_id"], []).append(int(r["menu_item_id"]))

# customer + store feature lookups (read feature tables to driver; small dataset)
cust_feat = {int(r["profile_id"]): r.asDict()
             for r in spark.read.table(fq("customer_features")).collect()}
store_feat = {int(r["unit_id"]): r.asDict()
              for r in spark.read.table(fq("store_features")).collect()}

def store_dict(uid):
    s = store_feat.get(uid)
    if not s:
        return {"popularity": {}, "store_aov": 0.0}
    pop = s.get("popularity") or {}
    return {"popularity": {int(k): float(v) for k, v in pop.items()},
            "store_aov": float(s.get("store_aov") or 0.0)}

X, y = [], []
for o in orders:
    order_items = items_by_order.get(o["guest_order_id"], [])
    if len(order_items) < 2:
        continue
    cust = cust_feat.get(o["profile_id"]) if o["profile_id"] is not None else None
    store = store_dict(o["unit_id"])
    # simulate a partial basket: hold out one item as the "added" positive
    held = order_items[-1]
    basket = order_items[:-1]
    basket_cats = cart_categories(basket, menu)
    # positive
    cat, _, _ = menu[held]
    X.append(build_feature_vector(held, cat, basket_cats, cust, store, cfg, menu)); y.append(1)
    # 3 negatives
    negs = [i for i in all_item_ids if i not in order_items]
    for ni in random.sample(negs, min(3, len(negs))):
        ncat, _, _ = menu[ni]
        X.append(build_feature_vector(ni, ncat, basket_cats, cust, store, cfg, menu)); y.append(0)

print(f"[INFO] training rows: {len(X)} (pos={sum(y)})")

from sklearn.ensemble import GradientBoostingClassifier
clf = GradientBoostingClassifier(random_state=42, n_estimators=100, max_depth=3)
clf.fit(X, y)
print(f"[INFO] train accuracy: {clf.score(X, y):.3f}")

# --- Log via Feature Engineering with FeatureLookups (automatic serving lookup) ---
import mlflow
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from src.ml.recommender_model import RecommenderModel

mlflow.set_registry_uri("databricks-uc")
fe = FeatureEngineeringClient()
CATS = ["pizza", "wings", "sides", "salads", "drinks", "desserts"]

# Build a tiny training_set DF carrying the lookup keys so FE records the lookups.
# Lookup-key column names match the live request fields (profile_id, store_id);
# FE maps them positionally to the feature-table PKs (profile_id, unit_id).
import pandas as pd
keys_pdf = pd.DataFrame([{"profile_id": int(o["profile_id"]) if o["profile_id"] else -1,
                          "store_id": int(o["unit_id"]), "label": 1} for o in orders[:500]])
keys_sdf = spark.createDataFrame(keys_pdf)
training_set = fe.create_training_set(
    df=keys_sdf,
    feature_lookups=[
        FeatureLookup(table_name=fq("customer_features"), lookup_key="profile_id",
                      feature_names=["tier", "aov"] + [f"affinity_{c}" for c in CATS]),
        FeatureLookup(table_name=fq("store_features"), lookup_key="store_id",
                      feature_names=["store_aov", "popularity"]),
    ],
    label="label",
    exclude_columns=["profile_id", "store_id"],
)

# Persist artifacts (menu, affinity, estimator) for the pyfunc.
import joblib, yaml
tmp = tempfile.mkdtemp()
menu_path = f"{tmp}/menu.json"; aff_path = f"{tmp}/basket_affinity.yml"; est_path = f"{tmp}/estimator.joblib"
with open(menu_path, "w") as f:
    json.dump({str(k): list(v) for k, v in menu.items()}, f)
with open(aff_path, "w") as f:
    yaml.safe_dump(cfg, f)
joblib.dump(clf, est_path)

model_name = fq("qsr_recommender")
with mlflow.start_run(run_name="qsr_recommender"):
    fe.log_model(
        model=RecommenderModel(),
        artifact_path="recommender",
        flavor=mlflow.pyfunc,
        training_set=training_set,
        registered_model_name=model_name,
        artifacts={"menu": menu_path, "affinity": aff_path, "estimator": est_path},
        pip_requirements=["scikit-learn", "pyyaml", "joblib", "mlflow", "pandas"],
    )
print(f"[INFO] registered {model_name}")

# --- (Re)create Model Serving endpoint pointing at latest version ---
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput, ServedEntityInput)
w = WorkspaceClient()
latest = max(int(v.version) for v in w.model_versions.list(full_name=model_name))
endpoint = f"{schema_prefix}qsr-recommender"
served = ServedEntityInput(entity_name=model_name, entity_version=str(latest),
                           scale_to_zero_enabled=True, workload_size="Small")
try:
    w.serving_endpoints.create(name=endpoint,
                               config=EndpointCoreConfigInput(served_entities=[served]))
    print(f"[INFO] created serving endpoint {endpoint}")
except Exception as e:
    print(f"[INFO] endpoint exists, updating config: {e}")
    w.serving_endpoints.update_config(name=endpoint, served_entities=[served])

# Grant CAN_QUERY to the website principal (PAT/SP) so PizzaTel can call the endpoint.
try:
    query_principal = dbutils.widgets.get("recommender_query_principal")
except Exception:
    query_principal = ""
if query_principal:
    from databricks.sdk.service.serving import (
        ServingEndpointAccessControlRequest, ServingEndpointPermissionLevel)
    try:
        w.serving_endpoints.set_permissions(
            serving_endpoint_id=w.serving_endpoints.get(name=endpoint).id,
            access_control_list=[ServingEndpointAccessControlRequest(
                service_principal_name=query_principal,
                permission_level=ServingEndpointPermissionLevel.CAN_QUERY)])
        print(f"[INFO] granted CAN_QUERY on {endpoint} to {query_principal}")
    except Exception as e:
        print(f"[WARN] grant CAN_QUERY skipped: {e}")
else:
    print("[INFO] no recommender_query_principal set; skipping CAN_QUERY grant")
print("[DONE] train_recommender complete")
