# Databricks notebook source
# Train the basket-aware recommender, log via Feature Engineering (automatic lookup),
# register in UC, and (re)create the Model Serving endpoint.
import json
import random

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

print(f"[INFO] train_recommender: catalog={catalog_name}, schema_prefix={schema_prefix}")

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
#     negatives = sampled items not in order). The order history is large (millions of
#     rows), so cap to a sample of orders — plenty for a demo-grade ranker, and it keeps
#     the driver-side vector build + sklearn fit fast and repeatable. Training on the full
#     history would produce ~10M rows and run for hours / risk driver OOM. ---
MAX_TRAIN_ORDERS = 8000
orders = (spark.read.table(f"{sp}silver.guest_order")
          .select("guest_order_id", "profile_id", "unit_id")
          .limit(MAX_TRAIN_ORDERS).collect())
print(f"[INFO] training on {len(orders)} sampled orders (cap {MAX_TRAIN_ORDERS})")
# Collect only the order_items for the sampled orders (join keeps the collect small).
_ids_sdf = spark.createDataFrame([(o["guest_order_id"],) for o in orders], ["guest_order_id"])
items_by_order = {}
for r in (spark.read.table(f"{sp}silver.order_item")
          .select("guest_order_id", "menu_item_id")
          .join(_ids_sdf, "guest_order_id").collect()):
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
    if isinstance(pop, str):
        pop = json.loads(pop)
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

import sklearn, numpy, scipy, joblib, pandas
from sklearn.ensemble import GradientBoostingClassifier
print(f"[INFO] training stack: sklearn={sklearn.__version__} numpy={numpy.__version__} "
      f"scipy={scipy.__version__} joblib={joblib.__version__} pandas={pandas.__version__}")
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

# Build the training_set DF. It carries the lookup KEYS (profile_id, store_id) used for the
# online feature lookup, PLUS the runtime REQUEST columns (member_id, cart_product_ids,
# viewed_product_id, num_recommendations) as scalar pass-throughs. These pass-through columns
# are NOT feature lookups and NOT excluded, so FE includes them in the served model's input
# signature and passes them straight to predict() at inference. Without them, Model Serving
# strips them as "extra inputs not in the signature" and the recommender never sees the cart.
# cart_product_ids is a JSON STRING (e.g. "[1,14]") so every request field is a scalar — this
# avoids array-typed signature inference issues; the pyfunc's _parse_cart handles JSON strings.
import pandas as pd
keys_pdf = pd.DataFrame([{"profile_id": int(o["profile_id"]) if o["profile_id"] else -1,
                          "member_id": int(o["profile_id"]) if o["profile_id"] else -1,
                          "store_id": int(o["unit_id"]),
                          "cart_product_ids": "[]",
                          "viewed_product_id": -1,
                          "num_recommendations": 5,
                          "label": 1} for o in orders[:500]])
print(f"[INFO] FE training_set key sample: {len(keys_pdf)} rows (of {len(orders)} total orders)")
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
    # Exclude only the lookup keys from the feature matrix (they drive the lookup, then drop).
    # member_id/cart_product_ids/viewed_product_id/num_recommendations are kept -> they flow
    # into the signature and through to predict().
    exclude_columns=["profile_id", "store_id"],
)

# Bake menu/affinity/estimator into the model instance before logging.
# cloudpickle (used by mlflow) preserves instance attributes set before log_model,
# so the pyfunc load_context receives a fully-initialized object without needing
# artifact files. This avoids relying on fe.log_model kwargs forwarding (undocumented).
rec = RecommenderModel()
rec._load(menu=menu, affinity=cfg, estimator=clf)

model_name = fq("qsr_recommender")
with mlflow.start_run(run_name="qsr_recommender"):
    fe.log_model(
        model=rec,
        artifact_path="recommender",
        flavor=mlflow.pyfunc,
        training_set=training_set,
        registered_model_name=model_name,
        # Pin the ENTIRE scientific stack to the exact training versions. cloudpickle loads
        # the pickled GBT at serving, importing sklearn/numpy/scipy; version drift breaks the
        # load (newer sklearn -> missing sklearn.ensemble._gb_losses; old sklearn + numpy 2.x
        # -> ImportError ComplexWarning). Pinning the whole set makes the serving env
        # self-consistent with where the model was pickled.
        pip_requirements=[
            f"scikit-learn=={sklearn.__version__}",
            f"numpy=={numpy.__version__}",
            f"scipy=={scipy.__version__}",
            f"joblib=={joblib.__version__}",
            f"pandas=={pandas.__version__}",
            "pyyaml", "mlflow",
        ],
        # Package the project source with the model so the pyfunc's `from src.*` imports
        # (scoring, features_vector, affinity, recommender_model) resolve in the serving
        # container. Without this the model loads in the notebook but fails at serving with
        # "Model server failed to load the model" (ModuleNotFoundError: src).
        code_paths=[f"{_bundle_root}/src"],
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
                           scale_to_zero_enabled=True, workload_size="Small",
                           # Route automatic feature lookup to the Lakebase online feature
                           # store (Online Tables are deprecated). Required post-migration.
                           environment_vars={"FEATURE_SOURCE": "DATABRICKS_ONLINE_STORE"})
# Submit the endpoint create/update WITHOUT blocking on readiness. serving_endpoints.create
# is a non-blocking submit; the endpoint provisions asynchronously and reaches READY a few
# minutes after this task finishes (during/after unpause_generator). We do NOT poll for READY
# here — blocking on provisioning is what made earlier runs time out and churn. Idempotent:
# update_config if the endpoint already exists. Retry only transient submit errors; raise
# loudly if the submit itself never succeeds so the setup job surfaces a real problem.
import time as _t
_ok = False
for _attempt in range(3):
    try:
        try:
            _exists = w.serving_endpoints.get(name=endpoint) is not None
        except Exception:
            _exists = False
        if _exists:
            w.serving_endpoints.update_config(name=endpoint, served_entities=[served])
            print(f"[INFO] submitted config update for {endpoint} (attempt {_attempt + 1})")
        else:
            w.serving_endpoints.create(name=endpoint,
                                       config=EndpointCoreConfigInput(served_entities=[served]))
            print(f"[INFO] submitted create for {endpoint} (attempt {_attempt + 1})")
        _ok = True
        break
    except Exception as e:
        print(f"[WARN] serving endpoint submit attempt {_attempt + 1} failed: {e}")
        _t.sleep(30)
if not _ok:
    raise RuntimeError(f"failed to submit serving endpoint {endpoint}; check online store "
                       "AVAILABLE + tables published + model registered")
print(f"[INFO] {endpoint} submitted; provisions to READY asynchronously (model loads via "
      "pinned stack + code_paths; auto-lookup via FEATURE_SOURCE online store)")

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
