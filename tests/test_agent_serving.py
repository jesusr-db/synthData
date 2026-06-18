"""Hermetic tests for the serving-time wire-format translation (adversarial finding I-1).

These cover the pure internal<->OpenAI translation helpers. The _GatewayLLMClient and
build_toolbox_factory are exercised at deploy (they need the SDK / live endpoints).
"""
import json

from src.agent.serving import to_openai_messages, from_openai_response


def test_to_openai_messages_reshapes_assistant_tool_calls():
    convo = [
        {"role": "system", "content": "sys"},
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": None,
         "tool_calls": [{"id": "c1", "name": "search_menu",
                         "arguments": {"category": "pizza"}}]},
        {"role": "tool", "tool_call_id": "c1", "name": "search_menu", "content": "{}"},
    ]
    out = to_openai_messages(convo)
    # system/user/tool pass through unchanged
    assert out[0] == {"role": "system", "content": "sys"}
    assert out[1] == {"role": "user", "content": "hi"}
    assert out[3]["role"] == "tool" and out[3]["tool_call_id"] == "c1"
    # assistant tool_calls reshaped to OpenAI wire format
    tc = out[2]["tool_calls"][0]
    assert tc["type"] == "function"
    assert tc["id"] == "c1"
    assert tc["function"]["name"] == "search_menu"
    # arguments serialized to a JSON STRING
    assert tc["function"]["arguments"] == json.dumps({"category": "pizza"})
    assert out[2]["content"] == ""  # None content normalized to ""


def test_to_openai_messages_leaves_plain_assistant_untouched():
    convo = [{"role": "assistant", "content": "We open at 11am."}]
    assert to_openai_messages(convo) == convo


def test_from_openai_response_dict_with_tool_calls():
    resp = {"choices": [{"message": {
        "content": None,
        "tool_calls": [{"id": "x1", "type": "function",
                        "function": {"name": "propose_order",
                                     "arguments": '{"order_type": "delivery"}'}}]}}]}
    out = from_openai_response(resp)
    assert out["content"] is None
    assert out["tool_calls"][0]["id"] == "x1"
    assert out["tool_calls"][0]["name"] == "propose_order"
    # arguments parsed back into a dict the loop can pass to dispatch/build_proposal
    assert out["tool_calls"][0]["arguments"] == {"order_type": "delivery"}


def test_from_openai_response_text_only():
    resp = {"choices": [{"message": {"content": "Sure!", "tool_calls": None}}]}
    out = from_openai_response(resp)
    assert out["content"] == "Sure!"
    assert out["tool_calls"] == []


def test_from_openai_response_malformed_arguments_degrade_to_empty_dict():
    resp = {"choices": [{"message": {"content": None, "tool_calls": [
        {"id": "b", "function": {"name": "search_menu", "arguments": "not json"}}]}}]}
    out = from_openai_response(resp)
    assert out["tool_calls"][0]["arguments"] == {}


def test_roundtrip_object_style_response():
    """from_openai_response also accepts an SDK-like object (attribute access)."""
    class _Fn:
        name = "get_recommendations"
        arguments = '{"store_id": 42}'

    class _TC:
        id = "t1"
        function = _Fn()

    class _Msg:
        content = None
        tool_calls = [_TC()]

    class _Choice:
        message = _Msg()

    class _Resp:
        choices = [_Choice()]

    out = from_openai_response(_Resp())
    assert out["tool_calls"][0]["name"] == "get_recommendations"
    assert out["tool_calls"][0]["arguments"] == {"store_id": 42}
