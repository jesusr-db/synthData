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


def build_response(loop_result, mlflow_trace_id, message_id="msg_0"):
    """Build the ResponsesAgent envelope.

    mlflow's ResponsesAgentResponse schema requires each output item to carry a unique
    `id` (discovered at deploy — log-time validation rejects items without it), so the
    serving caller (CommerceAgent.predict) passes a uuid; tests use the default.
    """
    custom_outputs = {}
    if loop_result.get("propose_order"):
        custom_outputs["propose_order"] = loop_result["propose_order"]
    if mlflow_trace_id:
        custom_outputs["mlflow_trace_id"] = mlflow_trace_id
    return {
        "output": [{"type": "message", "id": message_id, "role": "assistant",
                    "content": [{"type": "output_text", "text": loop_result.get("text", "")}]}],
        "custom_outputs": custom_outputs,
    }


class CommerceAgent(ResponsesAgent):
    """Wraps run_agent_loop with MLflow tracing + AI-Gateway LLM client.

    Logged as a baked instance (cloudpickle, mirroring RecommenderModel): the small menu /
    price / occasion artifacts are read once at log time and carried on the instance, so
    the serving container needs no Spark. The live wiring — the OpenAI-compatible client
    against the AI-Gateway endpoint and the endpoint-backed ToolBox — is built lazily in
    load_context() from the served-entity environment variables (LLM_ENDPOINT,
    RECOMMENDER_ENDPOINT, FEATURE_ENDPOINT). llm_client / toolbox_factory may also be
    injected directly (tests / custom callers), in which case load_context leaves them be.
    """

    def __init__(self, system_prompt, *, menu=None, price_lookup=None, occasions=None,
                 config=None, llm_client=None, toolbox_factory=None):
        self._system_prompt = system_prompt
        self._menu = menu or {}
        self._price_lookup = price_lookup or {}
        self._occasions = occasions or []
        self._config = config or {}
        self._llm_client = llm_client
        self._toolbox_factory = toolbox_factory

    def __getstate__(self):
        # Only the baked, picklable config travels in the cloudpickle artifact. The live
        # LLM client (OpenAI) and toolbox_factory (closure over WorkspaceClient) are not
        # picklable and are rebuilt by load_context at serving load — so drop them here.
        # (mlflow invokes load_context during log validation, which populates them; without
        # this, serialization of the logged instance fails.)
        state = self.__dict__.copy()
        state["_llm_client"] = None
        state["_toolbox_factory"] = None
        return state

    def load_context(self, context):
        """Build the live LLM client + endpoint-backed toolbox_factory at serving load.

        Endpoint names come from the served-entity env vars (set in build_commerce_agent),
        falling back to baked config. Skips anything already injected (tests).
        """
        import os
        from databricks.sdk import WorkspaceClient
        from src.agent.serving import _GatewayLLMClient, build_toolbox_factory
        cfg = dict(self._config or {})
        llm_endpoint = os.environ.get("LLM_ENDPOINT", cfg.get("llm_endpoint"))
        rec_endpoint = os.environ.get("RECOMMENDER_ENDPOINT", cfg.get("recommender_endpoint"))
        feat_endpoint = os.environ.get("FEATURE_ENDPOINT", cfg.get("feature_endpoint"))
        w = WorkspaceClient()
        if self._llm_client is None:
            self._llm_client = _GatewayLLMClient(
                w.serving_endpoints.get_open_ai_client(), llm_endpoint)
        if self._toolbox_factory is None:
            self._toolbox_factory = build_toolbox_factory(
                w, menu=self._menu, price_lookup=self._price_lookup,
                occasions=self._occasions, recommender_endpoint=rec_endpoint,
                feature_endpoint=feat_endpoint)

    def predict(self, request):
        import mlflow
        import uuid
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
            # mlflow 3.x exposes trace_id on the span; older builds used request_id.
            for _attr in ("trace_id", "request_id"):
                try:
                    trace_id = getattr(span, _attr, None)
                    if trace_id:
                        break
                except Exception:
                    trace_id = None
        return build_response(result, trace_id, message_id=str(uuid.uuid4()))
