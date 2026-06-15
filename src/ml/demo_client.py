# Databricks notebook source
# Demo: call the live recommender endpoint with a cart and print recommendations.
# This documents the exact request/response contract PizzaTel uses.
import json

try:
    schema_prefix = dbutils.widgets.get("schema_prefix")
except Exception:
    schema_prefix = "synth_"

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
endpoint = f"{schema_prefix}qsr-recommender"

def recommend(profile_id, store_id, cart_product_ids, viewed_product_id=None, num_recommendations=4):
    resp = w.serving_endpoints.query(
        name=endpoint,
        dataframe_records=[{
            "profile_id": int(profile_id),
            "member_id": int(profile_id) if profile_id and int(profile_id) > 0 else -1,
            "store_id": int(store_id),
            # cart is a JSON string (scalar) so the model signature stays all-scalar.
            "cart_product_ids": json.dumps([int(c) for c in cart_product_ids]),
            "viewed_product_id": int(viewed_product_id) if viewed_product_id else -1,
            "num_recommendations": num_recommendations,
        }],
    )
    return resp.predictions[0]

# Example 1: known customer, pizza in cart -> expect a drink near the top
print("== pizza cart, known gold customer ==")
print(json.dumps(recommend(profile_id=10231, store_id=42, cart_product_ids=[1]), indent=2))

# Example 2: soda already in cart -> no second soda
print("== pizza + soda cart -> soda suppressed ==")
print(json.dumps(recommend(profile_id=10231, store_id=42, cart_product_ids=[1, 53]), indent=2))

# Example 3: cold start (guest profile), empty cart -> store popular items
print("== cold start, empty cart ==")
print(json.dumps(recommend(profile_id=-1, store_id=42, cart_product_ids=[]), indent=2))
