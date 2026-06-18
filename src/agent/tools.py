"""Agent tools. Every data accessor is injected at construction so the ToolBox is
hermetically testable (no Spark / no network). The serving wrapper
(commerce_agent.py) injects real Spark reads and the recommender/feature endpoints.
"""
from src.agent.pricing import price_items

TOOL_SPECS = [
    {"type": "function", "function": {
        "name": "search_menu",
        "description": "Search the PizzaTel menu by free-text query and/or category.",
        "parameters": {"type": "object", "properties": {
            "query": {"type": "string"},
            "category": {"type": "string",
                         "enum": ["pizza", "wings", "sides", "salads", "drinks", "desserts"]},
        }}}},
    {"type": "function", "function": {
        "name": "get_recommendations",
        "description": "Personalized item recommendations from the live recommender, "
                       "given the customer, store, and current cart.",
        "parameters": {"type": "object", "properties": {
            "profile_id": {"type": ["integer", "string"]},
            "store_id": {"type": "integer"},
            "cart_product_ids": {"type": "array", "items": {"type": "integer"}},
            "num_recommendations": {"type": "integer"},
        }, "required": ["profile_id", "store_id"]}}},
    {"type": "function", "function": {
        "name": "get_customer_context",
        "description": "Loyalty tier, average order value, and category affinities for the customer.",
        "parameters": {"type": "object", "properties": {
            "profile_id": {"type": ["integer", "string"]}}, "required": ["profile_id"]}}},
    {"type": "function", "function": {
        "name": "get_order_history",
        "description": "The customer's recent orders, for reorder ('my usual') requests.",
        "parameters": {"type": "object", "properties": {
            "profile_id": {"type": ["integer", "string"]},
            "limit": {"type": "integer"}}, "required": ["profile_id"]}}},
    {"type": "function", "function": {
        "name": "get_occasion_context",
        "description": "Holidays / local events near a store, for special-occasion suggestions.",
        "parameters": {"type": "object", "properties": {
            "store_id": {"type": "integer"},
            "date": {"type": "string"}}, "required": ["store_id"]}}},
    {"type": "function", "function": {
        "name": "propose_order",
        "description": "Propose a final order for the customer to approve. Emit this when the "
                       "customer is ready to order. Do NOT place the order yourself.",
        "parameters": {"type": "object", "properties": {
            "items": {"type": "array", "items": {"type": "object", "properties": {
                "menu_item_id": {"type": "integer"},
                "quantity": {"type": "integer"}}, "required": ["menu_item_id", "quantity"]}},
            "order_type": {"type": "string", "enum": ["delivery", "pickup"]},
        }, "required": ["items", "order_type"]}}},
]


class ToolBox:
    def __init__(self, menu, price_lookup, recommend_fn, customer_fn,
                 history_fn, occasion_fn, tax_rate=0.09):
        self.menu = {int(k): v for k, v in menu.items()}
        self.price_lookup = {int(k): float(v) for k, v in price_lookup.items()}
        self._recommend_fn = recommend_fn
        self._customer_fn = customer_fn
        self._history_fn = history_fn
        self._occasion_fn = occasion_fn
        self.tax_rate = tax_rate

    def specs(self):
        return TOOL_SPECS

    def dispatch(self, name, arguments):
        arguments = arguments or {}
        if name == "search_menu":
            return self._search_menu(**arguments)
        if name == "get_recommendations":
            return self._get_recommendations(**arguments)
        if name == "get_customer_context":
            return self._get_customer_context(**arguments)
        if name == "get_order_history":
            return self._get_order_history(**arguments)
        if name == "get_occasion_context":
            return self._get_occasion_context(**arguments)
        raise ValueError(f"unknown tool: {name}")

    def _search_menu(self, query=None, category=None):
        results = []
        q = (query or "").lower()
        for mid, m in self.menu.items():
            if category and m.get("category") != category:
                continue
            if q and q not in m.get("item_name", "").lower():
                continue
            results.append({"menu_item_id": mid, "item_name": m.get("item_name"),
                            "category": m.get("category"), "subcategory": m.get("subcategory"),
                            "price": round(self.price_lookup.get(mid, 0.0), 2)})
        return {"results": sorted(results, key=lambda r: r["menu_item_id"])}

    def _get_recommendations(self, profile_id, store_id, cart_product_ids=None,
                             num_recommendations=5):
        recs = self._recommend_fn(profile_id, store_id, cart_product_ids or [],
                                  num_recommendations)
        return {"recommendations": recs}

    def _get_customer_context(self, profile_id):
        ctx = self._customer_fn(profile_id)
        if not ctx:
            return {"personalized": False}
        return {"personalized": True, **ctx}

    def _get_order_history(self, profile_id, limit=5):
        return {"orders": self._history_fn(profile_id, limit)}

    def _get_occasion_context(self, store_id, date=None):
        return {"occasions": self._occasion_fn(store_id, date)}

    def build_proposal(self, arguments):
        raw_items = arguments.get("items", [])
        # enrich with item_name from the menu before pricing
        enriched = [{"menu_item_id": int(it["menu_item_id"]),
                     "quantity": int(it.get("quantity", 1)),
                     "item_name": self.menu.get(int(it["menu_item_id"]), {}).get("item_name", "")}
                    for it in raw_items]
        priced = price_items(enriched, self.price_lookup, self.tax_rate)
        return {
            "tool": "propose_order",
            "items": priced["items"],
            "order_type": arguments.get("order_type", "delivery"),
            "subtotal": priced["subtotal"],
            "tax_estimate": priced["tax_estimate"],
            "total": priced["total"],
            "currency": priced["currency"],
            "pricing_note": "indicative — BFF is pricing authority at place_order",
        }
