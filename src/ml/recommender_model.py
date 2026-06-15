"""MLflow pyfunc recommender. Wraps src.ml.scoring with looked-up + on-demand features.

At serving time, Feature Engineering automatic lookup joins customer/store feature
columns onto each request row by customer_id/store_id. This pyfunc reads those
columns plus the raw request fields, calls the scoring core, and returns the
contract response per row.
"""
import json
import mlflow.pyfunc

from src.ml.scoring import rank_recommendations, heuristic_score

_CATS = ["pizza", "wings", "sides", "salads", "drinks", "desserts"]


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
        if raw is None:
            return []
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                raw = [x for x in raw.split(",") if x]
        return [int(x) for x in raw]

    def _customer(self, row):
        # cold-start if no tier/affinity present
        has_cust = row.get("tier") not in (None, "", "none") or any(
            row.get(f"affinity_{c}") not in (None,) for c in _CATS)
        if not has_cust:
            return None
        cust = {"tier": row.get("tier") or "none", "aov": row.get("aov") or 0.0}
        for c in _CATS:
            v = row.get(f"affinity_{c}")
            cust[f"affinity_{c}"] = float(v) if v is not None else 0.0
        return cust

    def _store(self, row):
        pop = row.get("popularity") or {}
        if isinstance(pop, str):
            try:
                pop = json.loads(pop)
            except Exception:
                pop = {}
        pop = {int(k): float(v) for k, v in pop.items()}
        return {"popularity": pop, "store_aov": row.get("store_aov") or 0.0}

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
            if viewed not in (None, "", 0):
                try:
                    cart = cart + [int(viewed)]
                except Exception:
                    pass
            cust = self._customer(row)
            store = self._store(row)
            n = row.get("num_recommendations") or 5
            recs = rank_recommendations(
                cart=cart, cust=cust, store=store, menu=self.menu,
                cfg=self.affinity, max_results=n, score_fn=score_fn)
            out.append({
                "personalized": cust is not None,
                "recommendations": recs,  # menu_item_id is int (scoring core coerces)
            })
        return out
