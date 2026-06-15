"""MLflow pyfunc recommender. Wraps src.ml.scoring with looked-up + on-demand features.

At serving time, Feature Engineering automatic lookup joins customer/store feature
columns onto each request row by customer_id/store_id. This pyfunc reads those
columns plus the raw request fields, calls the scoring core, and returns the
contract response per row.
"""
import json
import math
import mlflow.pyfunc

from src.ml.scoring import rank_recommendations, heuristic_score

_CATS = ["pizza", "wings", "sides", "salads", "drinks", "desserts"]


def _missing(v):
    """True if v is None or a float NaN (Model Serving sends absent fields as NaN)."""
    return v is None or (isinstance(v, float) and math.isnan(v))


class RecommenderModel(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        with open(context.artifacts["menu"]) as f:
            raw = json.load(f)
        menu = {int(k): tuple(v) for k, v in raw.items()}
        import yaml
        with open(context.artifacts["affinity"]) as f:
            affinity = yaml.safe_load(f)
        estimator = None
        if "estimator" in context.artifacts:
            import joblib
            estimator = joblib.load(context.artifacts["estimator"])
        self._load(menu=menu, affinity=affinity, estimator=estimator)

    def _load(self, menu, affinity, estimator):
        # test hook + shared init. menu values may be list or tuple.
        self.menu = {int(k): tuple(v) for k, v in menu.items()}
        self.affinity = affinity
        self.estimator = estimator

    def _parse_cart(self, raw):
        if _missing(raw):
            return []
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                raw = [x for x in raw.split(",") if x]
        return [int(x) for x in raw]

    def _customer(self, row):
        # cold-start if no tier/affinity present; NaN counts as missing
        tier_val = row.get("tier")
        has_cust = (not _missing(tier_val) and tier_val not in ("", "none")) or any(
            not _missing(row.get(f"affinity_{c}")) for c in _CATS)
        if not has_cust:
            return None
        tier = row.get("tier")
        raw_aov = row.get("aov")
        cust = {
            "tier": "none" if _missing(tier) else tier,
            "aov": 0.0 if _missing(raw_aov) else float(raw_aov),
        }
        for c in _CATS:
            v = row.get(f"affinity_{c}")
            cust[f"affinity_{c}"] = 0.0 if _missing(v) else float(v)
        return cust

    def _store(self, row):
        pop = row.get("popularity")
        if _missing(pop):
            pop = {}
        if isinstance(pop, str):
            try:
                pop = json.loads(pop)
            except Exception:
                pop = {}
        pop = {int(k): float(v) for k, v in pop.items()}
        s_aov = row.get("store_aov")
        store_aov = 0.0 if _missing(s_aov) else float(s_aov)
        return {"popularity": pop, "store_aov": store_aov}

    def _score_fn(self):
        if self.estimator is None:
            return None
        est = self.estimator

        def fn(cand_id, cand_cat, basket_cats, cust, store, cfg):
            from src.ml.features_vector import build_feature_vector
            x = build_feature_vector(cand_id, cand_cat, basket_cats, cust, store, cfg, self.menu)
            try:
                return float(est.predict_proba([x])[0][1])
            except Exception:
                return heuristic_score(cand_id, cand_cat, basket_cats, cust, store, cfg)
        return fn

    def predict(self, context, model_input):
        rows = model_input.to_dict(orient="records")
        score_fn = self._score_fn()
        out = []
        for row in rows:
            cart = self._parse_cart(row.get("cart_product_ids"))
            # fold the viewed item into the basket context (it shapes complementarity
            # and is excluded from results, like cart items)
            viewed = row.get("viewed_product_id")
            if not _missing(viewed) and viewed != 0:
                try:
                    cart = cart + [int(viewed)]
                except Exception:
                    pass
            cust = self._customer(row)
            store = self._store(row)
            raw_n = row.get("num_recommendations")
            n = 5 if _missing(raw_n) else int(raw_n)
            recs = rank_recommendations(
                cart=cart, cust=cust, store=store, menu=self.menu,
                cfg=self.affinity, max_results=n, score_fn=score_fn)
            out.append({
                "personalized": cust is not None,
                "recommendations": recs,  # menu_item_id is int (scoring core coerces)
            })
        return out
