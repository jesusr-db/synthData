# src/agent/loop.py
"""Pure tool-calling orchestration. llm_client and toolbox are injected so the loop
is hermetically testable. The real llm_client (Task 4) targets the AI-Gateway endpoint.
"""
import json


def _identity_line(custom_inputs):
    pid = custom_inputs.get("profile_id", "guest")
    sid = custom_inputs.get("store_id")
    mid = custom_inputs.get("member_id")
    return (f"[session] profile_id={pid} store_id={sid} member_id={mid}. "
            "Pass these to tools; do not ask the customer for them.")


def run_agent_loop(messages, custom_inputs, llm_client, toolbox, system_prompt, max_steps=6):
    convo = [{"role": "system", "content": system_prompt},
             {"role": "system", "content": _identity_line(custom_inputs)}]
    convo += list(messages)
    steps = []
    last_text = ""
    for _ in range(max_steps):
        resp = llm_client.create(messages=convo, tools=toolbox.specs())
        last_text = resp.get("content") or last_text
        tool_calls = resp.get("tool_calls") or []
        if not tool_calls:
            return {"text": resp.get("content") or "", "propose_order": None, "steps": steps}
        convo.append({"role": "assistant", "content": resp.get("content"),
                      "tool_calls": tool_calls})
        for call in tool_calls:
            name = call["name"]
            args = call.get("arguments") or {}
            steps.append(name)
            if name == "propose_order":
                proposal = toolbox.build_proposal(args)
                return {"text": resp.get("content") or "", "propose_order": proposal,
                        "steps": steps}
            result = toolbox.dispatch(name, args)
            convo.append({"role": "tool", "tool_call_id": call["id"], "name": name,
                          "content": json.dumps(result)})
    return {"text": last_text or "Sorry, I couldn't finalize that — could you rephrase?",
            "propose_order": None, "steps": steps}
