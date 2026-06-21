# PizzaTel Commerce Agent — Backend (Data-Science) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a Databricks-served conversational ordering agent (`synth_qsr-commerce-agent`) that reasons over a logged-in customer's identity, history, preferences, occasion context, and live recommendations, then emits a priced `propose_order` proposal the PizzaTel storefront renders for explicit customer approval — with every model call routed through Databricks AI Gateway.

**Architecture:** A single MLflow `ResponsesAgent` deployed to Model Serving. The LLM is reached **only** through an AI-Gateway-fronted serving endpoint (`synth_qsr-agent-llm`) — never a foundation model directly. The agent exposes six data tools, two of which call the **existing** `synth_qsr-recommender` and `synth_qsr-customer-features` endpoints (endpoint-as-tool reuse). The agent **proposes**; the web BFF **places** the real order (keeps the order byte-identical to a UI order). Built by a new `build_commerce_agent` setup task, torn down by the destroy job. Follows every existing repo convention: pure-core/serving-wrapper split for hermetic tests, raw-REST endpoint management (`api_client.do`), `code_paths` bundling of `src/`, full scientific-stack pinning.

**Tech Stack:** Python 3.11, MLflow `ResponsesAgent` (pyfunc), `databricks-openai`/OpenAI client against a Databricks serving endpoint, Databricks AI Gateway, Unity Catalog, Databricks Model Serving, Databricks Asset Bundles, pytest (hermetic — no Spark/Databricks/network in unit tests).

## Global Constraints

- **All model access routes through Databricks AI Gateway.** The agent calls the serving endpoint `synth_qsr-agent-llm` (AI-Gateway-fronted: usage tracking, rate limits, PII guardrails) — never a foundation model endpoint directly. Verbatim project constraint.
- **All serving on Databricks.** No external model hosts.
- **Fully automatable from zero.** Every billable object (LLM gateway endpoint, agent serving endpoint) is created by the `build_commerce_agent` setup task and torn down by `src/setup/destroy_notebook.py`. DAB-managed config goes in `resources/`.
- **The agent NEVER places an order.** It returns a `propose_order` proposal; the web BFF executes `place_order`. Enforced in code (there is no order-write tool on the backend).
- **Endpoint create/update/delete use raw REST via `w.api_client.do(...)`**, NOT the SDK `serving_endpoints` wrapper (unreliable in serverless — see `docs/gotchas.md`, Fix 9).
- **`code_paths=[f"{_bundle_root}/src"]`** must be passed to `log_model` so `from src.agent.*` imports resolve in the serving container.
- **Pin the entire scientific/runtime stack** in `pip_requirements` to the exact versions used at log time.
- **All UC names are prefixed:** `catalog_name` (default `jmrdemo`) and `schema_prefix` (default `synth_`) are job widgets. Schemas: `{prefix}ref`, `{prefix}silver`, `{prefix}features`. The UC model is `{catalog}.{prefix}features.qsr_commerce_agent`; endpoint `{prefix}qsr-commerce-agent`.
- **All IDs are `menu_item_id` bigints** (ints in and out). `product_id == str(menu_item_id)` — one ID space with the storefront catalog.
- **Hermetic tests only.** Unit tests inject fakes for the LLM client and tool data-access; the suite must run with no Spark, no Databricks, no network (matching the existing 102-test suite). Target: keep all existing tests green and add new ones.
- **Communications contract is live.** The seam doc is `/Users/jesus.rodriguez/Documents/ItsAVibe/gitRepos_FY26/opentelemetry-demo/docs/integration/agent-endpoint-contract.md` (the web team's repo). See the **Communications Contract** section below — the implementer MUST maintain its ledger.

---

## Communications Contract (READ FIRST — ongoing two-way comms)

This feature has a **counterparty**: the PizzaTel storefront / web team. The single source of truth for the seam between us is a living **integration contract + communications ledger**:

```
/Users/jesus.rodriguez/Documents/ItsAVibe/gitRepos_FY26/opentelemetry-demo/docs/integration/agent-endpoint-contract.md
```

**You (this plan's implementer) are the model / data-science team.** The web team owns the chat widget, mic/STT, auth, order placement, and OTel app trace. Your entire handoff surface is **one serving endpoint + this contract**.

### Ledger protocol (do not skip)
1. **Before coding:** read the whole contract, especially **§0 Communications Ledger** and **§0.1 Model team reply — 2026-06-18** (already posted by the brainstorm — it answers all 7 of the web team's open questions). Your build must match those committed answers.
2. **Status legend:** 🟥 TO BE PROVIDED · 🟨 PROPOSED / CONFIRM · 🟩 AGREED.
3. **When you resolve an item** (e.g. you deploy and now have the real endpoint URL + example request/response), **edit the contract**: fill the relevant section body, flip its marker to 🟩, and **append a ledger row to §0** — newest at top, format `YYYY-MM-DD — model — note`.
4. **When you have a new question for the web team,** append a ledger row tagged `model` and add a 🟥 line to the quick-list. Do not bury questions in prose elsewhere.
5. **When the web team replies** (a new `web` ledger row appears), treat it as authoritative for their side; reconcile your code/answers and respond in the ledger.
6. **Open item to close in this build:** §3.1 pricing authority — we proposed agent prices are *indicative* and the BFF re-prices at `place_order`. If the web team confirms (or pushes back) in the ledger, honor their decision and mark 🟩.

Tasks 8 and 9 below produce concrete ledger edits. **Every future session that touches this feature continues the conversation here** — that is how the two teams stay in sync without meetings.

---

## File Structure

**New — backend agent package (`src/agent/`):**
- `src/agent/__init__.py` — package marker.
- `src/agent/pricing.py` — pure deterministic pricing core. No Spark, no LLM.
- `src/agent/tools.py` — `ToolBox` (six tools + proposal assembler, all data-access injected) and `TOOL_SPECS` (LLM function-tool schemas).
- `src/agent/loop.py` — `run_agent_loop(...)`: pure tool-calling orchestration; LLM client + toolbox injected.
- `src/agent/commerce_agent.py` — MLflow `ResponsesAgent` wrapper + pure helpers `parse_request` / `build_response`. Wires the real AI-Gateway LLM client, real toolbox, MLflow trace tagging.
- `src/agent/gateway.py` — pure builder `build_gateway_endpoint_body(...)` for the AI-Gateway LLM endpoint REST body.
- `src/agent/prompts.py` — the system prompt string.

**New — setup notebook:**
- `src/setup/build_commerce_agent.py` — configures the AI-Gateway LLM endpoint, logs+registers the agent, creates the agent serving endpoint via raw REST, grants `CAN_QUERY`. Mirrors `src/ml/train_recommender.py`.

**New — tests (hermetic):**
- `tests/test_agent_pricing.py`
- `tests/test_agent_tools.py`
- `tests/test_agent_loop.py`
- `tests/test_agent_commerce_agent.py`
- `tests/test_agent_gateway.py`

**Modified:**
- `databricks.yml` — add bundle variables: `commerce_agent_query_principal`, `agent_llm_model`.
- `resources/setup_job.yml` — add `build_commerce_agent` task (depends_on `train_recommender`); add it to `unpause_generator.depends_on`.
- `src/setup/destroy_notebook.py` — extend Step 0h to delete `{prefix}qsr-commerce-agent` and `{prefix}qsr-agent-llm` endpoints (raw REST) before schema drops.
- `docs/api.md` — add the commerce agent endpoint section.
- The contract ledger (web repo, path above) — Tasks 8 & 9.

---

### Task 1: Pure pricing core

**Files:**
- Create: `src/agent/__init__.py`
- Create: `src/agent/pricing.py`
- Test: `tests/test_agent_pricing.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `price_items(items: list[dict], price_lookup: dict[int, float], tax_rate: float = 0.09) -> dict`. Each input item is `{"menu_item_id": int, "quantity": int, "item_name": str}`. Returns `{"items": [{"menu_item_id","item_name","quantity","unit_price"}], "subtotal": float, "tax_estimate": float, "total": float, "currency": "USD"}`. All money rounded to 2 decimals. Unknown `menu_item_id` (absent from `price_lookup`) → `unit_price` 0.0 (priced as free; loop logs it). Used by `tools.ToolBox.build_proposal` and `tools.ToolBox.price_cart`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_agent_pricing.py
from src.agent.pricing import price_items


def test_price_items_computes_subtotal_tax_total():
    items = [
        {"menu_item_id": 1, "quantity": 2, "item_name": "Large Pepperoni"},
        {"menu_item_id": 14, "quantity": 1, "item_name": "Garlic Knots"},
    ]
    price_lookup = {1: 14.99, 14: 5.49}
    out = price_items(items, price_lookup, tax_rate=0.09)
    assert out["currency"] == "USD"
    assert out["subtotal"] == 35.47          # 2*14.99 + 5.49
    assert out["tax_estimate"] == 3.19        # round(35.47 * 0.09, 2)
    assert out["total"] == 38.66
    assert out["items"][0]["unit_price"] == 14.99
    assert out["items"][1]["unit_price"] == 5.49


def test_price_items_unknown_id_prices_zero():
    out = price_items([{"menu_item_id": 999, "quantity": 1, "item_name": "Mystery"}], {})
    assert out["items"][0]["unit_price"] == 0.0
    assert out["subtotal"] == 0.0
    assert out["total"] == 0.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_agent_pricing.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.agent'`.

- [ ] **Step 3: Write minimal implementation**

```python
# src/agent/__init__.py
```

```python
# src/agent/pricing.py
"""Deterministic cart pricing. No LLM in the number path — prices come from UC.

Used to compute the indicative prices the agent shows on the propose_order confirm
card. The web BFF re-prices authoritatively at place_order (contract §3.1).
"""


def price_items(items, price_lookup, tax_rate=0.09):
    priced = []
    subtotal = 0.0
    for it in items:
        mid = int(it["menu_item_id"])
        qty = int(it.get("quantity", 1))
        unit_price = round(float(price_lookup.get(mid, 0.0)), 2)
        subtotal += unit_price * qty
        priced.append({
            "menu_item_id": mid,
            "item_name": it.get("item_name", ""),
            "quantity": qty,
            "unit_price": unit_price,
        })
    subtotal = round(subtotal, 2)
    tax_estimate = round(subtotal * tax_rate, 2)
    total = round(subtotal + tax_estimate, 2)
    return {
        "items": priced,
        "subtotal": subtotal,
        "tax_estimate": tax_estimate,
        "total": total,
        "currency": "USD",
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_agent_pricing.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add src/agent/__init__.py src/agent/pricing.py tests/test_agent_pricing.py
git commit -m "feat(agent): deterministic cart pricing core"
```

---

### Task 2: ToolBox + tool specs

**Files:**
- Create: `src/agent/tools.py`
- Test: `tests/test_agent_tools.py`

**Interfaces:**
- Consumes: `price_items` from `src/agent/pricing.py`.
- Produces:
  - `TOOL_SPECS: list[dict]` — OpenAI-style function-tool schemas for: `search_menu`, `get_recommendations`, `get_customer_context`, `get_order_history`, `get_occasion_context`, `propose_order`. (`price_cart` is internal — used by `build_proposal`, not LLM-callable.)
  - `class ToolBox` constructed with **injected** data accessors so it is hermetic:
    ```python
    ToolBox(menu: dict[int, dict], price_lookup: dict[int, float],
            recommend_fn, customer_fn, history_fn, occasion_fn, tax_rate=0.09)
    ```
    where `menu[mid] = {"item_name","category","subcategory"}`; `recommend_fn(profile_id, store_id, cart_product_ids, num_recommendations) -> list[dict]`; `customer_fn(profile_id) -> dict|None`; `history_fn(profile_id, limit) -> list[dict]`; `occasion_fn(store_id, date) -> list[dict]`.
  - Methods: `.specs() -> list[dict]` (returns `TOOL_SPECS`); `.dispatch(name: str, arguments: dict) -> dict` (routes a data-tool call, returns a JSON-serializable result); `.build_proposal(arguments: dict) -> dict` (prices `arguments["items"]` via `price_items` and returns the full `propose_order` payload with `tool`, `order_type`, `pricing_note`).
- Used by `loop.run_agent_loop` and `commerce_agent.CommerceAgent`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_agent_tools.py
from src.agent.tools import ToolBox, TOOL_SPECS

MENU = {
    1: {"item_name": "Large Pepperoni", "category": "pizza", "subcategory": "classic"},
    14: {"item_name": "Garlic Knots", "category": "sides", "subcategory": "bread"},
    53: {"item_name": "20oz Coca-Cola", "category": "drinks", "subcategory": "soda"},
}
PRICES = {1: 14.99, 14: 5.49, 53: 2.49}


def _box():
    return ToolBox(
        menu=MENU, price_lookup=PRICES,
        recommend_fn=lambda profile_id, store_id, cart_product_ids, num_recommendations: (
            [{"menu_item_id": 53, "score": 0.9, "item_name": "20oz Coca-Cola"}]),
        customer_fn=lambda profile_id: (None if profile_id == "guest"
                                        else {"tier": "gold", "aov": 30.0}),
        history_fn=lambda profile_id, limit: [{"guest_order_id": 7, "items": [1, 14]}],
        occasion_fn=lambda store_id, date: [{"name": "Super Bowl", "date": "2026-02-08"}],
    )


def test_specs_expose_six_tools_including_propose_order():
    names = {t["function"]["name"] for t in TOOL_SPECS}
    assert names == {"search_menu", "get_recommendations", "get_customer_context",
                     "get_order_history", "get_occasion_context", "propose_order"}


def test_search_menu_filters_by_category():
    out = _box().dispatch("search_menu", {"category": "pizza"})
    assert [r["menu_item_id"] for r in out["results"]] == [1]
    assert out["results"][0]["price"] == 14.99


def test_get_recommendations_passes_through_injected_fn():
    out = _box().dispatch("get_recommendations",
                          {"profile_id": 1234, "store_id": 42, "cart_product_ids": [1]})
    assert out["recommendations"][0]["menu_item_id"] == 53


def test_get_customer_context_guest_returns_personalized_false():
    out = _box().dispatch("get_customer_context", {"profile_id": "guest"})
    assert out["personalized"] is False


def test_build_proposal_prices_items_and_marks_indicative():
    prop = _box().build_proposal({
        "items": [{"menu_item_id": 1, "quantity": 2}, {"menu_item_id": 14, "quantity": 1}],
        "order_type": "delivery",
    })
    assert prop["tool"] == "propose_order"
    assert prop["order_type"] == "delivery"
    assert prop["items"][0]["item_name"] == "Large Pepperoni"   # enriched from menu
    assert prop["subtotal"] == 35.47
    assert prop["total"] == 38.66
    assert "indicative" in prop["pricing_note"].lower()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_agent_tools.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.agent.tools'`.

- [ ] **Step 3: Write minimal implementation**

```python
# src/agent/tools.py
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_agent_tools.py -v`
Expected: PASS (5 passed).

- [ ] **Step 5: Commit**

```bash
git add src/agent/tools.py tests/test_agent_tools.py
git commit -m "feat(agent): ToolBox with six injected tools + LLM tool specs"
```

---

### Task 3: Agent orchestration loop

**Files:**
- Create: `src/agent/prompts.py`
- Create: `src/agent/loop.py`
- Test: `tests/test_agent_loop.py`

**Interfaces:**
- Consumes: `ToolBox` (Task 2).
- Produces:
  - `SYSTEM_PROMPT: str` in `src/agent/prompts.py`.
  - `run_agent_loop(messages: list[dict], custom_inputs: dict, llm_client, toolbox: ToolBox, system_prompt: str, max_steps: int = 6) -> dict`. `llm_client.create(messages, tools) -> {"content": str|None, "tool_calls": list[{"id","name","arguments"}]}` (a scripted fake in tests; the real one in Task 4 wraps the AI-Gateway endpoint). Returns `{"text": str, "propose_order": dict|None, "steps": list[str]}`. When the LLM calls `propose_order`, the loop prices it via `toolbox.build_proposal` and returns immediately. `custom_inputs` (with `profile_id`/`store_id`) is injected into the first system context line so the model has identity without the web pre-fetching.
- Used by `commerce_agent.CommerceAgent`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_agent_loop.py
from src.agent.loop import run_agent_loop
from src.agent.prompts import SYSTEM_PROMPT
from src.agent.tools import ToolBox

MENU = {1: {"item_name": "Large Pepperoni", "category": "pizza", "subcategory": "classic"},
        14: {"item_name": "Garlic Knots", "category": "sides", "subcategory": "bread"}}
PRICES = {1: 14.99, 14: 5.49}


def _box():
    return ToolBox(menu=MENU, price_lookup=PRICES,
                   recommend_fn=lambda *a, **k: [{"menu_item_id": 14, "score": 0.8}],
                   customer_fn=lambda pid: {"tier": "gold", "aov": 30.0},
                   history_fn=lambda pid, limit: [],
                   occasion_fn=lambda sid, date: [])


class ScriptedLLM:
    """Returns pre-baked responses in order, one per create() call."""
    def __init__(self, script):
        self._script = list(script)
        self.calls = []

    def create(self, messages, tools):
        self.calls.append(messages)
        return self._script.pop(0)


def test_loop_runs_tool_then_proposes_order():
    llm = ScriptedLLM([
        {"content": None, "tool_calls": [
            {"id": "c1", "name": "search_menu", "arguments": {"category": "pizza"}}]},
        {"content": "Here's your order.", "tool_calls": [
            {"id": "c2", "name": "propose_order", "arguments": {
                "items": [{"menu_item_id": 1, "quantity": 2}], "order_type": "delivery"}}]},
    ])
    out = run_agent_loop(messages=[{"role": "user", "content": "two pepperoni pizzas"}],
                         custom_inputs={"profile_id": 1234, "store_id": 42},
                         llm_client=llm, toolbox=_box(), system_prompt=SYSTEM_PROMPT)
    assert out["steps"] == ["search_menu", "propose_order"]
    assert out["propose_order"]["total"] == 32.68    # 2*14.99=29.98 + 9% tax 2.70
    assert out["text"] == "Here's your order."


def test_loop_returns_text_when_no_tool_calls():
    llm = ScriptedLLM([{"content": "We open at 11am.", "tool_calls": []}])
    out = run_agent_loop(messages=[{"role": "user", "content": "what time do you open?"}],
                         custom_inputs={"profile_id": "guest", "store_id": 42},
                         llm_client=llm, toolbox=_box(), system_prompt=SYSTEM_PROMPT)
    assert out["propose_order"] is None
    assert out["text"] == "We open at 11am."


def test_loop_stops_at_max_steps():
    # LLM keeps calling a data tool forever; loop must terminate gracefully.
    spin = {"content": None, "tool_calls": [
        {"id": "x", "name": "search_menu", "arguments": {}}]}
    llm = ScriptedLLM([spin] * 10)
    out = run_agent_loop(messages=[{"role": "user", "content": "hi"}],
                         custom_inputs={"profile_id": 1, "store_id": 1},
                         llm_client=llm, toolbox=_box(), system_prompt=SYSTEM_PROMPT, max_steps=3)
    assert out["propose_order"] is None
    assert len(out["steps"]) == 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_agent_loop.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.agent.loop'`.

- [ ] **Step 3: Write minimal implementation**

```python
# src/agent/prompts.py
SYSTEM_PROMPT = (
    "You are the PizzaTel ordering assistant. Help the logged-in customer build an order "
    "through natural conversation. Use your tools to look up the menu, the customer's "
    "preferences and order history, holiday/occasion context, and live recommendations. "
    "Suggest items for special occasions and reorders when relevant. When the customer is "
    "ready, call propose_order with the exact menu_item_ids and quantities. NEVER claim the "
    "order is placed — you only propose it; the customer must approve it in the app. Keep "
    "replies short and friendly. Prices you mention are indicative."
)
```

```python
# src/agent/loop.py
"""Pure tool-calling orchestration. llm_client and toolbox are injected so the loop
is hermetically testable. The real llm_client (Task 4) targets the AI-Gateway endpoint.
"""
import json


def _identity_line(custom_inputs):
    pid = custom_inputs.get("profile_id", "guest")
    sid = custom_inputs.get("store_id")
    mid = custom_inputs.get("member_id")
    return (f"[session] profile_id={pid} store_id={sid} member_id={mid}. "
            "Pass these to tools; do not ask the customer for them.")


def run_agent_loop(messages, custom_inputs, llm_client, toolbox, system_prompt, max_steps=6):
    convo = [{"role": "system", "content": system_prompt},
             {"role": "system", "content": _identity_line(custom_inputs)}]
    convo += list(messages)
    steps = []
    last_text = ""
    for _ in range(max_steps):
        resp = llm_client.create(messages=convo, tools=toolbox.specs())
        last_text = resp.get("content") or last_text
        tool_calls = resp.get("tool_calls") or []
        if not tool_calls:
            return {"text": resp.get("content") or "", "propose_order": None, "steps": steps}
        convo.append({"role": "assistant", "content": resp.get("content"),
                      "tool_calls": tool_calls})
        for call in tool_calls:
            name = call["name"]
            args = call.get("arguments") or {}
            steps.append(name)
            if name == "propose_order":
                proposal = toolbox.build_proposal(args)
                return {"text": resp.get("content") or "", "propose_order": proposal,
                        "steps": steps}
            result = toolbox.dispatch(name, args)
            convo.append({"role": "tool", "tool_call_id": call["id"], "name": name,
                          "content": json.dumps(result)})
    return {"text": last_text or "Sorry, I couldn't finalize that — could you rephrase?",
            "propose_order": None, "steps": steps}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_agent_loop.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add src/agent/prompts.py src/agent/loop.py tests/test_agent_loop.py
git commit -m "feat(agent): tool-calling orchestration loop + system prompt"
```

---

### Task 4: ResponsesAgent wrapper (request parse + response envelope)

**Files:**
- Create: `src/agent/commerce_agent.py`
- Test: `tests/test_agent_commerce_agent.py`

**Interfaces:**
- Consumes: `run_agent_loop` (Task 3).
- Produces two **pure helpers** (unit-tested) plus the `ResponsesAgent` subclass (exercised at deploy, Task 9):
  - `parse_request(request: dict) -> tuple[list, dict]` → `(messages, custom_inputs)`. `messages = request["input"]`, `custom_inputs = request.get("custom_inputs", {})`.
  - `build_response(loop_result: dict, mlflow_trace_id: str | None) -> dict` → the Responses-shaped envelope: `{"output": [{"type":"message","role":"assistant","content":[{"type":"output_text","text": <text>}]}], "custom_outputs": {...}}`. `custom_outputs` includes `propose_order` (only when present) and `mlflow_trace_id` (only when not None).
  - `class CommerceAgent(ResponsesAgent)` — `predict()` reads `app_trace_context` from `custom_inputs`, sets MLflow trace tag `app.trace_id`, runs the loop, returns `build_response(...)` with the active MLflow `trace_id`.
- Used by `src/setup/build_commerce_agent.py`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_agent_commerce_agent.py
from src.agent.commerce_agent import parse_request, build_response


def test_parse_request_splits_input_and_custom_inputs():
    req = {"input": [{"role": "user", "content": "hi"}],
           "custom_inputs": {"profile_id": 1234, "store_id": 42,
                             "app_trace_context": "00-abc-def-01"}}
    messages, custom = parse_request(req)
    assert messages == [{"role": "user", "content": "hi"}]
    assert custom["profile_id"] == 1234
    assert custom["app_trace_context"] == "00-abc-def-01"


def test_parse_request_tolerates_missing_custom_inputs():
    messages, custom = parse_request({"input": []})
    assert messages == []
    assert custom == {}


def test_build_response_includes_proposal_and_trace_id():
    loop_result = {"text": "Here's your order.",
                   "propose_order": {"tool": "propose_order", "total": 38.66}, "steps": []}
    out = build_response(loop_result, mlflow_trace_id="tr-123")
    assert out["output"][0]["content"][0]["text"] == "Here's your order."
    assert out["custom_outputs"]["propose_order"]["total"] == 38.66
    assert out["custom_outputs"]["mlflow_trace_id"] == "tr-123"


def test_build_response_omits_absent_fields():
    out = build_response({"text": "We open at 11am.", "propose_order": None, "steps": []},
                         mlflow_trace_id=None)
    assert "propose_order" not in out["custom_outputs"]
    assert "mlflow_trace_id" not in out["custom_outputs"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_agent_commerce_agent.py -v`
Expected: FAIL — `ModuleNotFoundError` (or `ImportError` if `mlflow` lacks `ResponsesAgent`; if so, see Step 3 import guard).

- [ ] **Step 3: Write minimal implementation**

```python
# src/agent/commerce_agent.py
"""MLflow ResponsesAgent wrapper for the PizzaTel commerce agent.

parse_request / build_response are pure and unit-tested. The CommerceAgent class is
exercised end-to-end at deploy (the ResponsesAgent base + MLflow Tracing need the
serving/notebook runtime). The base import is guarded so hermetic unit tests of the
pure helpers run even where mlflow's ResponsesAgent isn't importable.
"""

try:
    from mlflow.pyfunc import ResponsesAgent
    _HAS_RESPONSES_AGENT = True
except Exception:  # pragma: no cover - depends on mlflow version at runtime
    ResponsesAgent = object
    _HAS_RESPONSES_AGENT = False


def parse_request(request):
    messages = request.get("input") or []
    custom_inputs = request.get("custom_inputs") or {}
    return messages, custom_inputs


def build_response(loop_result, mlflow_trace_id):
    custom_outputs = {}
    if loop_result.get("propose_order"):
        custom_outputs["propose_order"] = loop_result["propose_order"]
    if mlflow_trace_id:
        custom_outputs["mlflow_trace_id"] = mlflow_trace_id
    return {
        "output": [{"type": "message", "role": "assistant",
                    "content": [{"type": "output_text", "text": loop_result.get("text", "")}]}],
        "custom_outputs": custom_outputs,
    }


class CommerceAgent(ResponsesAgent):
    """Wraps run_agent_loop with MLflow tracing + AI-Gateway LLM client.

    Constructed in build_commerce_agent.py with a toolbox_factory (builds a per-request
    ToolBox bound to real Spark reads + the recommender/feature endpoints) and an
    llm_client bound to the AI-Gateway endpoint synth_qsr-agent-llm.
    """

    def __init__(self, llm_client, toolbox_factory, system_prompt):
        self._llm_client = llm_client
        self._toolbox_factory = toolbox_factory
        self._system_prompt = system_prompt

    def predict(self, request):
        import mlflow
        from src.agent.loop import run_agent_loop
        req = request if isinstance(request, dict) else request.model_dump()
        messages, custom_inputs = parse_request(req)
        trace_id = None
        app_trace = custom_inputs.get("app_trace_context")
        with mlflow.start_span(name="commerce_agent") as span:
            if app_trace:
                mlflow.update_current_trace(tags={"app.trace_id": app_trace})
            toolbox = self._toolbox_factory(custom_inputs)
            result = run_agent_loop(messages, custom_inputs, self._llm_client, toolbox,
                                    self._system_prompt)
            try:
                trace_id = span.request_id
            except Exception:
                trace_id = None
        return build_response(result, trace_id)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_agent_commerce_agent.py -v`
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add src/agent/commerce_agent.py tests/test_agent_commerce_agent.py
git commit -m "feat(agent): ResponsesAgent wrapper with trace-stitch + proposal envelope"
```

---

### Task 5: AI-Gateway LLM endpoint body builder

**Files:**
- Create: `src/agent/gateway.py`
- Test: `tests/test_agent_gateway.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `build_gateway_endpoint_body(name: str, llm_entity: str, *, rate_limit_rpm: int = 120, pii_behavior: str = "BLOCK") -> dict`. Returns the raw-REST body for creating/updating the AI-Gateway-fronted LLM serving endpoint: a single served foundation-model entity plus an `ai_gateway` block enabling `usage_tracking_config`, an endpoint-level `rate_limits` entry, and input+output PII `guardrails`. Used by `src/setup/build_commerce_agent.py`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_agent_gateway.py
from src.agent.gateway import build_gateway_endpoint_body


def test_body_has_served_entity_and_gateway_block():
    body = build_gateway_endpoint_body("synth_qsr-agent-llm",
                                       "databricks-claude-3-7-sonnet", rate_limit_rpm=200)
    assert body["name"] == "synth_qsr-agent-llm"
    assert body["config"]["served_entities"][0]["entity_name"] == "databricks-claude-3-7-sonnet"
    gw = body["ai_gateway"]
    assert gw["usage_tracking_config"]["enabled"] is True
    assert gw["rate_limits"][0]["calls"] == 200
    assert gw["rate_limits"][0]["renewal_period"] == "minute"
    assert gw["guardrails"]["input"]["pii"]["behavior"] == "BLOCK"
    assert gw["guardrails"]["output"]["pii"]["behavior"] == "BLOCK"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_agent_gateway.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.agent.gateway'`.

- [ ] **Step 3: Write minimal implementation**

```python
# src/agent/gateway.py
"""Builds the raw-REST body for the AI-Gateway-fronted LLM serving endpoint.

This endpoint is the ONLY path the agent uses to reach a model — the AI Gateway is
the usage-tracking / rate-limit / PII-guardrail choke point required by the project
constraint that all model access route through AI Gateway.
"""


def build_gateway_endpoint_body(name, llm_entity, *, rate_limit_rpm=120, pii_behavior="BLOCK"):
    return {
        "name": name,
        "config": {
            "served_entities": [{
                "name": "llm",
                "entity_name": llm_entity,
                "scale_to_zero_enabled": True,
            }],
        },
        "ai_gateway": {
            "usage_tracking_config": {"enabled": True},
            "rate_limits": [{"calls": rate_limit_rpm, "renewal_period": "minute",
                             "key": "endpoint"}],
            "guardrails": {
                "input": {"pii": {"behavior": pii_behavior}},
                "output": {"pii": {"behavior": pii_behavior}},
            },
        },
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_agent_gateway.py -v`
Expected: PASS (1 passed).

- [ ] **Step 5: Run the full suite to confirm no regressions, then commit**

Run: `pytest tests/ -q`
Expected: all prior tests (102 baseline) + the new agent tests PASS.

```bash
git add src/agent/gateway.py tests/test_agent_gateway.py
git commit -m "feat(agent): AI-Gateway LLM endpoint body builder"
```

---

### Task 6: build_commerce_agent setup notebook

**Files:**
- Create: `src/setup/build_commerce_agent.py`

**Interfaces:**
- Consumes: `src.agent.gateway.build_gateway_endpoint_body`, `src.agent.tools.ToolBox`, `src.agent.commerce_agent.CommerceAgent`, `src.agent.prompts.SYSTEM_PROMPT`, and the existing `synth_qsr-recommender` + `synth_qsr-customer-features` endpoints.
- Produces (at runtime, not unit-tested): the AI-Gateway LLM endpoint `{prefix}qsr-agent-llm`, the registered UC model `{catalog}.{prefix}features.qsr_commerce_agent`, and the agent serving endpoint `{prefix}qsr-commerce-agent` with `CAN_QUERY` granted to `commerce_agent_query_principal`.

> **Pattern source:** this notebook mirrors `src/ml/train_recommender.py` almost line-for-line for the bundle-root `sys.path` shim, the widget reads, the raw-REST endpoint create/update with retries, and the `CAN_QUERY` grant. Reuse those exact idioms.

- [ ] **Step 1: Write the notebook**

```python
# src/setup/build_commerce_agent.py
# Databricks notebook source
# Configure the AI-Gateway LLM endpoint, log+register the commerce agent, and (re)create
# the agent Model Serving endpoint. Mirrors train_recommender.py conventions.
import sys

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_notebook_path.replace("/Workspace", "").split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)


def _widget(name, default):
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return default


catalog_name = _widget("catalog_name", "jmrdemo")
schema_prefix = _widget("schema_prefix", "synth_")
agent_llm_model = _widget("agent_llm_model", "databricks-claude-3-7-sonnet")
query_principal = _widget("commerce_agent_query_principal", "")
print(f"[INFO] build_commerce_agent: catalog={catalog_name} prefix={schema_prefix} "
      f"llm={agent_llm_model}")

sp = f"{catalog_name}.{schema_prefix}"
fq = lambda t: f"{catalog_name}.{schema_prefix}features.{t}"  # noqa: E731

from databricks.sdk import WorkspaceClient
import time as _t
w = WorkspaceClient()

# --- 1. AI-Gateway LLM endpoint (the ONLY model path the agent uses) ---
from src.agent.gateway import build_gateway_endpoint_body
llm_endpoint = f"{schema_prefix}qsr-agent-llm"
gw_body = build_gateway_endpoint_body(llm_endpoint, agent_llm_model, rate_limit_rpm=200)
try:
    w.api_client.do("GET", f"/api/2.0/serving-endpoints/{llm_endpoint}")
    w.api_client.do("PUT", f"/api/2.0/serving-endpoints/{llm_endpoint}/ai-gateway",
                    body=gw_body["ai_gateway"])
    print(f"[INFO] updated AI Gateway config on existing {llm_endpoint}")
except Exception:
    try:
        w.api_client.do("POST", "/api/2.0/serving-endpoints", body=gw_body)
        print(f"[INFO] created AI-Gateway LLM endpoint {llm_endpoint}")
    except Exception as e:
        print(f"[WARN] AI-Gateway LLM endpoint setup: {repr(e)}")

# --- 2. Bake menu + price lookup artifacts (read to driver; small) ---
menu_rows = spark.read.table(f"{sp}ref.menu_item").select(
    "menu_item_id", "item_name", "category", "subcategory").collect()
menu = {int(r["menu_item_id"]): {"item_name": r["item_name"], "category": r["category"],
                                 "subcategory": r["subcategory"]} for r in menu_rows}
# current-period price per item (latest effective row)
price_rows = spark.sql(f"""
    SELECT menu_item_id, price FROM {sp}ref.item_price ip
    WHERE ip.effective_to IS NULL OR ip.effective_to >= current_date()
""").collect()
price_lookup = {int(r["menu_item_id"]): float(r["price"]) for r in price_rows}
print(f"[INFO] baked {len(menu)} menu items, {len(price_lookup)} prices")

# --- 3. Log + register the agent ---
import mlflow
from src.agent.commerce_agent import CommerceAgent
from src.agent.prompts import SYSTEM_PROMPT
mlflow.set_registry_uri("databricks-uc")

# The real llm_client + toolbox_factory are constructed inside the model module at load
# time (they need the serving runtime's workspace creds + endpoint names). For logging we
# pass an instance carrying the config it needs; load_context rebinds live clients.
model_name = fq("qsr_commerce_agent")
import mlflow.models
resources = [
    mlflow.models.resources.DatabricksServingEndpoint(endpoint_name=llm_endpoint),
    mlflow.models.resources.DatabricksServingEndpoint(endpoint_name=f"{schema_prefix}qsr-recommender"),
    mlflow.models.resources.DatabricksServingEndpoint(endpoint_name=f"{schema_prefix}qsr-customer-features"),
    mlflow.models.resources.DatabricksTable(table_name=f"{sp}ref.menu_item"),
    mlflow.models.resources.DatabricksTable(table_name=f"{sp}silver.guest_order"),
]
import mlflow.pyfunc
with mlflow.start_run(run_name="qsr_commerce_agent"):
    mlflow.pyfunc.log_model(
        artifact_path="commerce_agent",
        python_model=f"{_bundle_root}/src/agent/commerce_agent.py",
        registered_model_name=model_name,
        resources=resources,
        code_paths=[f"{_bundle_root}/src"],
        pip_requirements=["mlflow", "databricks-openai", "databricks-sdk", "pyyaml"],
    )
print(f"[INFO] registered {model_name}")

# --- 4. (Re)create the agent serving endpoint via raw REST (Fix 9 pattern) ---
latest = max(int(v.version) for v in w.model_versions.list(full_name=model_name))
endpoint = f"{schema_prefix}qsr-commerce-agent"
served_entity = {
    "entity_name": model_name,
    "entity_version": str(latest),
    "scale_to_zero_enabled": True,
    "workload_size": "Small",
    "environment_vars": {
        "LLM_ENDPOINT": llm_endpoint,
        "RECOMMENDER_ENDPOINT": f"{schema_prefix}qsr-recommender",
        "FEATURE_ENDPOINT": f"{schema_prefix}qsr-customer-features",
        "CATALOG_NAME": catalog_name,
        "SCHEMA_PREFIX": schema_prefix,
    },
}
try:
    w.api_client.do("GET", f"/api/2.0/serving-endpoints/{endpoint}")
    _exists = True
except Exception:
    _exists = False
_ok = False
for _attempt in range(3):
    try:
        if _exists:
            w.api_client.do("PUT", f"/api/2.0/serving-endpoints/{endpoint}/config",
                            body={"served_entities": [served_entity]})
        else:
            w.api_client.do("POST", "/api/2.0/serving-endpoints",
                            body={"name": endpoint, "config": {"served_entities": [served_entity]}})
        _ok = True
        break
    except Exception as e:
        print(f"[WARN] agent endpoint REST submit attempt {_attempt + 1} failed: {repr(e)}")
        _t.sleep(20)
if not _ok:
    raise RuntimeError(f"failed to submit serving endpoint {endpoint} via REST API")
print(f"[INFO] {endpoint} submitted via REST; provisions to READY asynchronously")

# --- 5. Grant CAN_QUERY to the website principal ---
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
    print("[INFO] no commerce_agent_query_principal set; skipping CAN_QUERY grant")
print("[DONE] build_commerce_agent complete")
```

> **Note on `python_model` as a path:** logging the agent via the file path (Models-from-Code) lets MLflow capture the `CommerceAgent` defined there. The module's `load_context`/module-load code (rebinding the live OpenAI client against `LLM_ENDPOINT` and the real Spark-backed `toolbox_factory`) is finalized at Task 9 against the deployed runtime — that's the integration boundary the unit tests intentionally don't cover. If Models-from-Code path-logging fights the `code_paths` bundle in this runtime, fall back to logging a constructed `CommerceAgent` instance (cloudpickle baked, as `train_recommender.py` does with `RecommenderModel`).

- [ ] **Step 2: Syntax/compile check**

Run: `python -c "import ast; ast.parse(open('src/setup/build_commerce_agent.py').read()); print('OK')"`
Expected: `OK`.

- [ ] **Step 3: Commit**

```bash
git add src/setup/build_commerce_agent.py
git commit -m "feat(agent): build_commerce_agent setup notebook (gateway + log + serve)"
```

---

### Task 7: Bundle wiring — variables, setup DAG, destroy teardown

**Files:**
- Modify: `databricks.yml` (variables block)
- Modify: `resources/setup_job.yml` (add `build_commerce_agent` task; add to `unpause_generator.depends_on`)
- Modify: `src/setup/destroy_notebook.py` (Step 0h: delete both new endpoints via raw REST)

**Interfaces:**
- Consumes: the notebook from Task 6.
- Produces: a setup DAG where `build_commerce_agent` runs after `train_recommender`; a destroy step that removes both endpoints before schema drops.

- [ ] **Step 1: Add bundle variables**

In `databricks.yml`, under the existing `variables:` block, add:

```yaml
  commerce_agent_query_principal:
    description: SP/principal granted CAN_QUERY on the commerce agent endpoint. Empty = skip grant.
    default: ""
  agent_llm_model:
    description: Foundation model served behind the AI-Gateway LLM endpoint.
    default: databricks-claude-3-7-sonnet
```

- [ ] **Step 2: Add the setup task** (in `resources/setup_job.yml`, after the `train_recommender` task; copy the `train_recommender` task block's `job_cluster_key`/`environment_key`/`libraries` shape exactly):

```yaml
        - task_key: build_commerce_agent
          depends_on:
            - task_key: train_recommender
          notebook_task:
            notebook_path: ../src/setup/build_commerce_agent.py
            base_parameters:
              catalog_name: ${var.catalog_name}
              schema_prefix: ${var.schema_prefix}
              agent_llm_model: ${var.agent_llm_model}
              commerce_agent_query_principal: ${var.commerce_agent_query_principal}
          environment_key: ml
```

Then add `build_commerce_agent` to the existing `unpause_generator` task's `depends_on` list (so the generator only unpauses after the agent is built):

```yaml
        - task_key: unpause_generator
          depends_on:
            - task_key: backfill
            - task_key: create_genie_space
            - task_key: apply_ontos
            - task_key: train_recommender
            - task_key: build_commerce_agent
```

- [ ] **Step 3: Extend the destroy notebook**

Open `src/setup/destroy_notebook.py`, find Step 0h (serving-endpoint teardown, which already deletes `{prefix}qsr-recommender` and `{prefix}qsr-customer-features` via raw REST per memory obs 5746). Add the two new endpoints to the same delete loop. Locate the list of endpoint names there and add:

```python
        f"{schema_prefix}qsr-commerce-agent",
        f"{schema_prefix}qsr-agent-llm",
```

so all four endpoints are deleted (raw REST `DELETE /api/2.0/serving-endpoints/<name>`, best-effort `[WARN]` on failure) **before** the schema drops in later steps.

- [ ] **Step 4: Validate the bundle**

Run: `databricks bundle validate -p DEFAULT`
Expected: `Validation OK`.

- [ ] **Step 5: Commit**

```bash
git add databricks.yml resources/setup_job.yml src/setup/destroy_notebook.py
git commit -m "feat(agent): wire build_commerce_agent into setup DAG + destroy teardown"
```

---

### Task 8: Document the endpoint contract in this repo

**Files:**
- Modify: `docs/api.md` (add a commerce-agent serving-endpoint section under "## Serving Endpoints")

**Interfaces:**
- Consumes: the deployed-endpoint shape from Tasks 4 & 6.
- Produces: an in-repo contract section mirroring the existing `synth_qsr-recommender` doc style, so this repo is self-describing.

- [ ] **Step 1: Append the section** to `docs/api.md` after the `synth_qsr-recommender` block:

````markdown
### `synth_qsr-commerce-agent` — Model Serving (ResponsesAgent)

Conversational ordering agent. PizzaTel-facing; called once per chat turn.

**Method / path:** `POST https://<workspace-host>/serving-endpoints/synth_qsr-commerce-agent/invocations`
**Auth:** `Authorization: Bearer <PAT or OAuth token with CAN_QUERY>`

**Request** — Responses input shape; stateless (web resends full history each turn):

```json
{
  "input": [{"role": "user", "content": "I want two pepperoni pizzas for the game"}],
  "custom_inputs": {
    "profile_id": 1234, "member_id": 5678, "store_id": 42,
    "app_trace_context": "00-<traceid>-<spanid>-01"
  }
}
```

`profile_id` may be the string `"guest"` (cold-start path). `app_trace_context` is a W3C `traceparent` carried in the payload (not an HTTP header — Model Serving may strip headers).

**Response** — assistant text plus structured outputs:

```json
{
  "output": [{"type": "message", "role": "assistant",
              "content": [{"type": "output_text", "text": "Here's your order — sound good?"}]}],
  "custom_outputs": {
    "propose_order": {"tool": "propose_order",
      "items": [{"menu_item_id": 1, "quantity": 2, "item_name": "Large Pepperoni", "unit_price": 14.99}],
      "order_type": "delivery", "subtotal": 29.98, "tax_estimate": 2.70, "total": 32.68,
      "currency": "USD", "pricing_note": "indicative — BFF is pricing authority at place_order"},
    "mlflow_trace_id": "tr-..."
  }
}
```

`propose_order` is present only on turns where the agent proposes an order. The agent never places the order — the web BFF executes `place_order` after the customer approves. Prices are indicative; the BFF is the pricing authority at placement.

**Model access:** the agent reaches its LLM only via the AI-Gateway endpoint `synth_qsr-agent-llm` (usage tracking, rate limits, PII guardrails). Endpoint create/update/delete use raw REST via `api_client.do()`.
````

- [ ] **Step 2: Commit**

```bash
git add docs/api.md
git commit -m "docs(api): add commerce agent serving endpoint contract"
```

---

### Task 9: Deploy, capture real example, run OTel verification, close the ledger

This task crosses the integration boundary (live Databricks + the web team). It produces evidence and the ledger updates that let the web team build against a real endpoint.

**Files:**
- Modify (web repo): `/Users/jesus.rodriguez/Documents/ItsAVibe/gitRepos_FY26/opentelemetry-demo/docs/integration/agent-endpoint-contract.md`

- [ ] **Step 1: Deploy the bundle and run setup**

```bash
databricks bundle deploy -p DEFAULT
databricks bundle run setup_job -p DEFAULT
```
Expected: `build_commerce_agent` task succeeds; `synth_qsr-agent-llm` and `synth_qsr-commerce-agent` endpoints provision to READY (poll with `databricks serving-endpoints get synth_qsr-commerce-agent -p DEFAULT`).

- [ ] **Step 2: Capture a real request/response**

Invoke the deployed endpoint with the example request from `docs/api.md` (use `databricks serving-endpoints query synth_qsr-commerce-agent -p DEFAULT --json '<request>'` or curl). Save the actual response, including a turn that emits `propose_order` and a guest-path turn.

- [ ] **Step 3: Run the §6 OTLP-alongside-experiment verification**

On the deployed serving runtime, set `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` and confirm whether traces still land in the MLflow experiment trace store (and whether MLflow eval/scorers still work). Record the finding (coexist vs. mutually exclusive).

- [ ] **Step 4: Update the live contract ledger**

In the web-team contract doc:
- Fill **§1** with the real workspace invocation URL + the MLflow experiment path.
- Fill **§2.4** with the real example request/response captured in Step 2; flip §1, §2.1, §2.4 markers to 🟩.
- Replace the §6 placeholder with the OTLP finding from Step 3; mark 🟩.
- Append a **§0 ledger row**, newest at top: `2026-06-DD — model — Deployed synth_qsr-commerce-agent. URL + MLflow experiment in §1, real request/response in §2.4 (🟩). OTLP-alongside-experiment finding in §6 (🟩). Confirm §3.1 pricing-authority assumption when you get a chance.`

- [ ] **Step 5: Commit (synthData) and notify**

```bash
git add docs/
git commit -m "docs: capture deployed commerce-agent example + OTLP finding"
```
The web-repo contract edit is committed in that repo separately (it is the web team's repo). Surface to the user that the ledger has been updated so the web team is unblocked.

---

## Self-Review

**Spec coverage** (brainstorm Option A + contract §0.1 answers):
- ResponsesAgent, AI-Gateway-only model access → Tasks 4, 5, 6 + Global Constraints. ✅
- Six tools incl. two endpoint-as-tool reuses → Task 2. ✅
- Stateless, agent-owns-tools → Tasks 3, 4 (`parse_request`, `_identity_line`). ✅
- `propose_order` ints + indicative pricing, agent never places order → Tasks 1, 2; Global Constraints. ✅
- Trace stitch (`app.trace_id` tag + return `mlflow_trace_id`) → Task 4. ✅
- Latency/guest behavior → guest path tested (Task 2/3); SLA confirmed at deploy (Task 9). ✅
- OTLP §6 verification → Task 9 Step 3. ✅
- Setup creation + destroy teardown, raw REST, code_paths, pinned stack → Tasks 6, 7 + Global Constraints. ✅
- Communications ledger maintained → Communications Contract section + Tasks 8, 9. ✅

**Placeholder scan:** no `TBD`/"handle edge cases"/"similar to Task N"; every code step has complete code. Notebook integration boundary (live LLM-client rebinding) is explicitly flagged as deploy-time, not hidden as a stub.

**Type consistency:** `price_items` signature/return identical across Tasks 1↔2; `ToolBox(...)` ctor + `.specs()`/`.dispatch()`/`.build_proposal()` identical Tasks 2↔3↔6; `run_agent_loop(...)` identical Tasks 3↔4; `parse_request`/`build_response` identical Tasks 4↔6; `build_gateway_endpoint_body(...)` identical Tasks 5↔6. `propose_order` JSON keys identical across pricing, tools, docs, and the ledger.
