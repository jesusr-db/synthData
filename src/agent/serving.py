"""Serving-time wiring for the commerce agent — the integration boundary.

This module turns the hermetic, injected-everything core (loop + ToolBox + pricing)
into a live thing inside a Databricks Model Serving container:

  * `_GatewayLLMClient` adapts the AI-Gateway LLM serving endpoint
    (`synth_qsr-agent-llm`) to the loop's `llm_client.create(messages, tools)` contract.
    It owns the translation between the loop's INTERNAL conversation shape
    ({"id","name","arguments"} tool_calls, {"role":"tool","tool_call_id","content"}
    results) and the OpenAI WIRE shape ({"id","type":"function","function":{"name",
    "arguments": <JSON string>}}) that the OpenAI-compatible endpoint expects. This is
    the adapter the loop's docstring points to (adversarial finding I-1).

  * `build_toolbox_factory(...)` returns a `toolbox_factory(custom_inputs) -> ToolBox`
    whose data accessors are bound to: baked menu/price artifacts (read once at log
    time — no Spark in the serving container), the live recommender endpoint, and the
    live customer-features endpoint. History/occasion are best-effort v1 stubs.

The pure translation helpers (`to_openai_messages`, `from_openai_response`) are unit
tested hermetically. The client/factory themselves are exercised at deploy.
"""
import json


# --- I-1: internal <-> OpenAI wire-format translation (pure, unit-tested) ---

def _content_to_text(content):
    """Normalize a message content to a plain string. ResponsesAgent input items may
    arrive with list-of-parts content (e.g. [{"type":"input_text","text":"..."}]); the
    chat-completions API wants a string for our purposes."""
    if isinstance(content, list):
        parts = []
        for p in content:
            if isinstance(p, dict):
                parts.append(p.get("text") or p.get("content") or "")
            else:
                parts.append(str(p))
        return "".join(parts)
    return content if content is not None else ""


def to_openai_messages(convo):
    """Translate the loop's internal conversation into OpenAI chat-completions wire format.

    Critically, this WHITELISTS keys: ResponsesAgent normalizes incoming request items and
    injects extra fields (notably `status`, plus `id`/`type`) that the chat-completions API
    rejects ("Extra inputs are not permitted"). We rebuild each message from only the keys
    the API accepts. Assistant tool_calls are reshaped from the loop's internal shape
    {"id","name","arguments"(dict)} to the OpenAI wire shape
    {"id","type":"function","function":{"name","arguments": <JSON string>}}.
    """
    out = []
    for m in convo:
        role = m.get("role")
        if role == "assistant" and m.get("tool_calls"):
            wire_calls = []
            for tc in m["tool_calls"]:
                args = tc.get("arguments")
                if not isinstance(args, str):
                    args = json.dumps(args or {})
                wire_calls.append({
                    "id": tc.get("id"),
                    "type": "function",
                    "function": {"name": tc.get("name"), "arguments": args},
                })
            out.append({"role": "assistant",
                        "content": _content_to_text(m.get("content")),
                        "tool_calls": wire_calls})
        elif role == "tool":
            out.append({"role": "tool",
                        "tool_call_id": m.get("tool_call_id"),
                        "content": _content_to_text(m.get("content"))})
        else:
            out.append({"role": role, "content": _content_to_text(m.get("content"))})
    return out


def from_openai_response(response):
    """Translate an OpenAI chat-completions response into the loop's internal shape:
    {"content": str|None, "tool_calls": [{"id","name","arguments"(dict)}]}.

    Accepts either an OpenAI SDK object (with .choices[0].message) or an already-dict
    response, so it is testable without the SDK.
    """
    # normalize to a message dict
    if isinstance(response, dict):
        choices = response.get("choices") or [{}]
        msg = choices[0].get("message", {}) if choices else {}
        content = msg.get("content")
        raw_calls = msg.get("tool_calls") or []
    else:
        msg = response.choices[0].message
        content = getattr(msg, "content", None)
        raw_calls = getattr(msg, "tool_calls", None) or []

    tool_calls = []
    for tc in raw_calls:
        if isinstance(tc, dict):
            fn = tc.get("function", {})
            cid, name, args = tc.get("id"), fn.get("name"), fn.get("arguments")
        else:
            fn = tc.function
            cid, name, args = tc.id, fn.name, fn.arguments
        if isinstance(args, str):
            try:
                args = json.loads(args) if args else {}
            except Exception:
                args = {}
        tool_calls.append({"id": cid, "name": name, "arguments": args or {}})
    return {"content": content, "tool_calls": tool_calls}


class _GatewayLLMClient:
    """Adapts the AI-Gateway LLM serving endpoint to the loop's llm_client contract.

    The OpenAI-compatible client is obtained from the Databricks SDK
    (`w.serving_endpoints.get_open_ai_client()`), so every model call routes through the
    AI-Gateway-fronted endpoint (usage tracking / rate limits / PII guardrails) — never a
    foundation model directly. TOOL_SPECS are already OpenAI function-tool shaped, so
    `tools` passes straight through.
    """

    def __init__(self, openai_client, endpoint_name):
        self._client = openai_client
        self._endpoint = endpoint_name

    def create(self, messages, tools):
        resp = self._client.chat.completions.create(
            model=self._endpoint,
            messages=to_openai_messages(messages),
            tools=tools or None,
        )
        return from_openai_response(resp)


# --- toolbox_factory: binds the ToolBox data accessors to live endpoints/artifacts ---

def _post_invocations(w, endpoint, body):
    return w.api_client.do("POST", f"/serving-endpoints/{endpoint}/invocations", body=body)


def build_toolbox_factory(w, *, menu, price_lookup, occasions,
                          recommender_endpoint, feature_endpoint, tax_rate=0.09):
    """Return toolbox_factory(custom_inputs) -> ToolBox bound to live data sources.

    menu/price_lookup/occasions are baked at log time (no Spark in the serving
    container). recommend_fn and customer_fn call the live recommender/customer-features
    endpoints (declared as model resources). history is a best-effort v1 stub (returns
    []); occasion filters the baked occasions list. Every accessor is defensive — a
    failure returns empty/none and the loop (with graceful tool-error handling) keeps the
    turn alive rather than 500-ing.
    """
    from src.agent.tools import ToolBox

    def _coerce_profile(profile_id):
        # The recommender's ID space is integer; the agent's cold-start sentinel is the
        # string "guest" -> route to -1 so the recommender takes its store-popularity path.
        try:
            return int(profile_id)
        except (TypeError, ValueError):
            return -1

    def recommend_fn(profile_id, store_id, cart_product_ids, num_recommendations):
        try:
            body = {"dataframe_records": [{
                "profile_id": _coerce_profile(profile_id),
                "member_id": _coerce_profile(profile_id),
                "store_id": int(store_id) if store_id is not None else -1,
                "cart_product_ids": list(cart_product_ids or []),
                "viewed_product_id": -1,
                "num_recommendations": int(num_recommendations or 5),
            }]}
            resp = _post_invocations(w, recommender_endpoint, body)
            preds = (resp or {}).get("predictions") or [{}]
            return preds[0].get("recommendations", [])  # M-2: unwrap predictions[0]
        except Exception:
            return []

    def customer_fn(profile_id):
        pid = _coerce_profile(profile_id)
        if pid < 0:  # guest -> not personalized
            return None
        try:
            resp = _post_invocations(w, feature_endpoint,
                                     {"dataframe_records": [{"profile_id": pid}]})
            preds = (resp or {}).get("predictions") or (resp or {}).get("outputs") or []
            row = preds[0] if preds else {}
            if not row:
                return None
            ctx = {}
            for k in ("tier", "aov", "total_orders", "recency_days"):
                if row.get(k) is not None:
                    ctx[k] = row[k]
            for c in ("pizza", "wings", "sides", "salads", "drinks", "desserts"):
                v = row.get(f"affinity_{c}")
                if v is not None:
                    ctx[f"affinity_{c}"] = v
            return ctx or None
        except Exception:
            return None

    def history_fn(profile_id, limit):
        # v1: order-history reorder ("my usual") is a documented follow-up — it needs a
        # warehouse/SQL path not declared in this model's resources. Degrade to empty.
        return []

    def occasion_fn(store_id, date):
        # Baked occasions are global (metro-keyed upstream); v1 returns the baked list
        # filtered by date prefix when provided. Store->metro mapping is a v2 follow-up.
        evs = occasions or []
        if date:
            evs = [e for e in evs if str(e.get("date", "")).startswith(str(date)[:7])] or evs
        return evs[:5]

    def toolbox_factory(custom_inputs):
        return ToolBox(menu=menu, price_lookup=price_lookup,
                       recommend_fn=recommend_fn, customer_fn=customer_fn,
                       history_fn=history_fn, occasion_fn=occasion_fn, tax_rate=tax_rate)

    return toolbox_factory
