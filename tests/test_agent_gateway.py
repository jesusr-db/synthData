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
