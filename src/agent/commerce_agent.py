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
