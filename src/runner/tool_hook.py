# The command a gated CLI runs as its tool hook: python -I tool_hook.py <claude|grok|cursor-agent> <socket> <arm|pre>.
# It asks the run's ToolGate and answers in the CLI's own format; whatever goes wrong, the call is refused (exit 2).
from __future__ import annotations

import json
import socket
import sys
from typing import Any

# Above the gate's own deadline (60 s), below the hook timeouts given to the CLIs (120 s).
REPLY_TIMEOUT_SECONDS = 90.0


def ask(path: str, frame: dict[str, Any]) -> dict[str, Any]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(REPLY_TIMEOUT_SECONDS)
        sock.connect(path)
        sock.sendall(json.dumps(frame).encode("utf-8") + b"\n")
        with sock.makefile("rb") as reply:
            return json.loads(reply.readline())


def tool_call(payload: dict[str, Any]) -> dict[str, Any]:
    # Only the tool name and its arguments leave the node: the payloads also carry the account's email and paths.
    name = payload.get("tool_name", payload.get("toolName"))
    args = payload.get("tool_input", payload.get("toolInput"))
    if not isinstance(name, str) or not name or not isinstance(args, dict):
        raise ValueError("hook payload names no tool call")
    if payload.get("toolInputTruncated") or payload.get("tool_input_truncated"):
        raise ValueError("the CLI truncated the tool call's arguments")
    return {"type": "tool.authorize", "tool_name": name, "args": args}


def native_session_id(raw: bytes) -> str | None:
    try:
        payload = json.loads(raw)
        found = payload.get("session_id", payload.get("sessionId"))
    except (ValueError, AttributeError):
        return None
    return found if isinstance(found, str) else None


def render(style: str, action: str, reason: str) -> dict[str, Any]:
    if action == "defer":
        reason = f"waiting for human approval: {reason}"
    if style == "cursor-agent":
        if action == "allow":
            return {"permission": "allow"}
        return {"permission": "deny", "user_message": reason, "agent_message": reason}
    reply: dict[str, Any] = {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "allow" if action == "allow" else "deny",
            "permissionDecisionReason": reason,
        }
    }
    if action == "defer":
        reply.update({"continue": False, "stopReason": reason})
    return reply


def main(argv: list[str]) -> int:
    try:
        style, path, event = argv
        raw = sys.stdin.buffer.read()
        if event == "arm":
            ask(path, {"type": "hook.armed", "session_id": native_session_id(raw)})
            print("{}")
            return 0
        decision = ask(path, tool_call(json.loads(raw)))
        if decision["action"] not in ("allow", "block", "defer"):
            raise ValueError("unknown decision")
        print(json.dumps(render(style, decision["action"], str(decision.get("reason", "")))))
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"tool call refused: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
