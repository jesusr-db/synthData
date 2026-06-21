# tests/test_agent_loop.py
from src.agent.loop import run_agent_loop, _identity_line
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


# ---------------------------------------------------------------------------
# FIX 1 (M-1) — graceful tool-error handling
# ---------------------------------------------------------------------------

class BrokenToolBox:
    """A toolbox whose dispatch always raises TypeError (bad args simulation)."""
    def specs(self):
        return []

    def dispatch(self, name, args):
        raise TypeError(f"dispatch got unexpected kwargs for {name}")

    def build_proposal(self, args):
        raise ValueError("build_proposal failed: missing menu_item_id")


def test_tool_dispatch_error_does_not_raise_and_loop_continues():
    """When toolbox.dispatch raises, loop catches, appends error tool result, continues."""
    broken = BrokenToolBox()
    llm = ScriptedLLM([
        # First turn: bad tool call that will raise in dispatch
        {"content": None, "tool_calls": [
            {"id": "e1", "name": "search_menu", "arguments": {"bad_kwarg": True}}]},
        # Second turn: model recovers and returns a text response
        {"content": "Sorry, let me help another way.", "tool_calls": []},
    ])
    out = run_agent_loop(
        messages=[{"role": "user", "content": "find me a pizza"}],
        custom_inputs={"profile_id": "u1", "store_id": 5},
        llm_client=llm, toolbox=broken, system_prompt=SYSTEM_PROMPT,
    )
    # Must not raise; must return valid contract shape
    assert "text" in out
    assert "propose_order" in out
    assert "steps" in out
    # Step was still recorded even though it errored
    assert "search_menu" in out["steps"]
    # Loop continued and model's recovery text is returned
    assert out["text"] == "Sorry, let me help another way."
    assert out["propose_order"] is None

    # Verify the error tool result was appended to the conversation so the
    # model could see it — check the second LLM call's messages contain a
    # tool-role message with an "error" key in its content.
    second_call_messages = llm.calls[1]
    tool_results = [m for m in second_call_messages if m.get("role") == "tool"]
    assert len(tool_results) == 1
    import json
    payload = json.loads(tool_results[0]["content"])
    assert "error" in payload


def test_propose_order_error_does_not_raise_and_returns_valid_dict():
    """When build_proposal raises, loop catches, degrades gracefully, returns contract shape."""
    broken = BrokenToolBox()
    llm = ScriptedLLM([
        # Model immediately tries to propose with bad args
        {"content": "Let me place that for you.", "tool_calls": [
            {"id": "p1", "name": "propose_order",
             "arguments": {"items": [{"quantity": 1}]}}]},  # missing menu_item_id
        # Model recovers after seeing the error
        {"content": "I couldn't place the order, please try again.", "tool_calls": []},
    ])
    out = run_agent_loop(
        messages=[{"role": "user", "content": "order something"}],
        custom_inputs={"profile_id": "u2", "store_id": 7},
        llm_client=llm, toolbox=broken, system_prompt=SYSTEM_PROMPT,
    )
    # Must not raise; must return valid contract shape
    assert "text" in out
    assert "propose_order" in out
    assert "steps" in out
    # propose_order stays None because build failed
    assert out["propose_order"] is None
    # Step was recorded
    assert "propose_order" in out["steps"]


# ---------------------------------------------------------------------------
# FIX 2 (M-5) — _identity_line coerces values to str
# ---------------------------------------------------------------------------

def test_identity_line_with_int_ids_returns_single_line():
    """Integer ids must be coerced to str; result must be a single line (no embedded newlines)."""
    line = _identity_line({"profile_id": 9001, "store_id": 42, "member_id": 7})
    assert isinstance(line, str)
    assert "\n" not in line
    assert "profile_id=9001" in line
    assert "store_id=42" in line
    assert "member_id=7" in line


def test_identity_line_with_unusual_chars_does_not_raise():
    """Values with unusual characters must not cause _identity_line to raise."""
    line = _identity_line({"profile_id": "usr\t123", "store_id": 0, "member_id": None})
    assert isinstance(line, str)
    # At minimum the function must not raise and must return something


def test_identity_line_missing_profile_id_defaults_to_guest():
    """Missing profile_id falls back to 'guest'."""
    line = _identity_line({"store_id": 5})
    assert "profile_id=guest" in line


def test_loop_uses_str_coerced_identity_in_system_message():
    """The system message injected into the conversation uses str-coerced ids."""
    llm = ScriptedLLM([{"content": "Hi!", "tool_calls": []}])
    run_agent_loop(
        messages=[{"role": "user", "content": "hello"}],
        custom_inputs={"profile_id": 12345, "store_id": 99, "member_id": 7},
        llm_client=llm, toolbox=_box(), system_prompt=SYSTEM_PROMPT,
    )
    # The first call's messages should include an identity system message
    first_call_messages = llm.calls[0]
    identity_msgs = [m for m in first_call_messages
                     if m.get("role") == "system" and "profile_id" in m.get("content", "")]
    assert len(identity_msgs) == 1
    content = identity_msgs[0]["content"]
    assert "profile_id=12345" in content
    assert "store_id=99" in content
