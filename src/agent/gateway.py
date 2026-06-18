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
