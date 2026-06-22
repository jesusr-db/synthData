# src/agent/loop.py
"""Pure tool-calling orchestration. llm_client and toolbox are injected so the loop
is hermetically testable. The real llm_client (Task 4) targets the AI-Gateway endpoint.

INTERNAL CONVERSATION SHAPE vs. WIRE FORMAT — ADAPTER CONTRACT
---------------------------------------------------------------
Within this loop, tool calls are stored in the conversation using this module's
INTERNAL shape:

  Assistant message (when the model calls tools):
    {"role": "assistant", "content": <str|None>, "tool_calls": [
        {"id": "<call-id>", "name": "<tool-name>", "arguments": <dict>},
        ...
    ]}

  Tool-result message (one per tool call):
    {"role": "tool", "tool_call_id": "<call-id>", "name": "<tool-name>",
     "content": "<JSON string>"}

The deploy-time llm_client ADAPTER is responsible for translating this internal
conversation to/from the OpenAI wire format BEFORE sending to the AI-Gateway
endpoint and AFTER receiving the response. The OpenAI wire format uses:

  Assistant message:
    {"role": "assistant", "content": ..., "tool_calls": [
        {"id": "<call-id>", "type": "function",
         "function": {"name": "<tool-name>", "arguments": "<JSON string>"}},
        ...
    ]}

  Tool-result message (paired by tool_call_id):
    {"role": "tool", "tool_call_id": "<call-id>", "content": "<JSON string>"}

The loop itself never performs this translation — it is purely the adapter's
responsibility. Do NOT change the internal message shapes in this module.
"""
import json


def _identity_line(custom_inputs):
    pid = custom_inputs.get("profile_id", "guest")
    sid = custom_inputs.get("store_id")
    mid = custom_inputs.get("member_id")
    # Coerce all values to str so that non-string types (int, None, etc.)
    # produce a well-formed single-line string without raising.
    return (f"[session] profile_id={str(pid)} store_id={str(sid)} member_id={str(mid)}. "
            "Pass these to tools; do not ask the customer for them.")


def run_agent_loop(messages, custom_inputs, llm_client, toolbox, system_prompt, max_steps=6):
    # ONE system message: some gateways (e.g. Bedrock-backed AI Gateway routes) reject a
    # second system message ("System message must be at the beginning"), so fold the
    # identity line into the single system message rather than appending a second one.
    convo = [{"role": "system",
              "content": system_prompt + "\n\n" + _identity_line(custom_inputs)}]
    convo += list(messages)
    steps = []
    last_text = ""
    for _ in range(max_steps):
        resp = llm_client.create(messages=convo, tools=toolbox.specs())
        last_text = resp.get("content") or last_text
        tool_calls = resp.get("tool_calls") or []
        if not tool_calls:
            return {"text": resp.get("content") or "", "propose_order": None, "steps": steps}

        # Append the assistant turn with its tool calls.
        # (Internal shape — see module docstring for adapter contract.)
        convo.append({"role": "assistant", "content": resp.get("content"),
                      "tool_calls": tool_calls})

        for call in tool_calls:
            name = call["name"]
            args = call.get("arguments") or {}
            steps.append(name)

            if name == "propose_order":
                # FIX 1 (M-1): catch errors from build_proposal so a malformed
                # propose_order call doesn't crash the entire chat turn.
                try:
                    proposal = toolbox.build_proposal(args)
                    return {"text": resp.get("content") or "", "propose_order": proposal,
                            "steps": steps}
                except Exception as exc:
                    error_content = json.dumps({"error": str(exc)})
                    # Append error tool-result so the model can recover.
                    # (Internal shape — see module docstring for adapter contract.)
                    convo.append({"role": "tool", "tool_call_id": call["id"], "name": name,
                                  "content": error_content})
                    # Continue the loop so the model can retry or gracefully degrade.
                    continue

            # FIX 1 (M-1): catch errors from dispatch (bad/missing/hallucinated args,
            # unknown tool name, etc.) so a single bad tool call doesn't abort the turn.
            try:
                result = toolbox.dispatch(name, args)
                result_content = json.dumps(result)
            except Exception as exc:
                result_content = json.dumps({"error": str(exc)})

            # Append tool-result message.
            # (Internal shape — see module docstring for adapter contract.)
            convo.append({"role": "tool", "tool_call_id": call["id"], "name": name,
                          "content": result_content})

    return {"text": last_text or "Sorry, I couldn't finalize that — could you rephrase?",
            "propose_order": None, "steps": steps}
