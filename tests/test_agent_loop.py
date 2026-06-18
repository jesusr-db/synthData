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
