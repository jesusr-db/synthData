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
