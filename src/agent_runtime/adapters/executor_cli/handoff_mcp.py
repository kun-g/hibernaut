#!/usr/bin/env python3
# Stdio MCP server the launcher gives a visible Claude Code session: the agent fetches each turn's task and returns
# its result through tools, so it needs no access to the handoff directory (an added directory opens a trust dialog,
# and the reviewer gets no file-writing tool at all). Started as `python -I <this file> <handoff root>`; stdlib only.
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any, cast

SERVER_NAME = "hibernaut_handoff"
_TURN_RE = re.compile(r"turn-[A-Za-z0-9_]+")
_DEFAULT_PROTOCOL = "2025-06-18"


class ToolError(Exception):
    pass


def schema_errors(value: Any, schema: dict[str, Any], where: str = "result") -> list[str]:
    # The subset of JSON Schema the role schemas use: type, enum, properties, required, items.
    kind = schema.get("type")
    checks = {"object": dict, "array": list, "string": str, "boolean": bool}
    if kind in checks and not isinstance(value, checks[kind]):
        return [f"{where} must be a {kind}"]
    if "enum" in schema and value not in schema["enum"]:
        return [f"{where} must be one of {', '.join(map(str, schema['enum']))}"]
    errors: list[str] = []
    if isinstance(value, dict):
        obj = cast(dict[str, Any], value)
        errors += [f"{where}.{key} is required" for key in schema.get("required", []) if key not in obj]
        for key, sub in cast(dict[str, Any], schema.get("properties", {})).items():
            if key in obj:
                errors += schema_errors(obj[key], sub, f"{where}.{key}")
    if isinstance(value, list) and "items" in schema:
        for i, item in enumerate(cast(list[Any], value)):
            errors += schema_errors(item, schema["items"], f"{where}[{i}]")
    return errors


class HandoffServer:
    def __init__(self, root: Path) -> None:
        self.root = Path(os.path.realpath(root))
        self.schema: dict[str, Any] = json.loads((self.root / "schema.json").read_text(encoding="utf-8"))

    def tools(self) -> list[dict[str, Any]]:
        turn = {"type": "string", "description": "The turn named in your instruction, e.g. turn-abc123."}
        result = dict(self.schema)
        result["properties"] = {"turn": turn, **self.schema.get("properties", {})}
        result["required"] = ["turn", *self.schema.get("required", [])]
        return [
            {"name": "get_task", "description": "Get the task for a turn. Call it first, then carry the task out.",
             "inputSchema": {"type": "object", "properties": {"turn": turn}, "required": ["turn"]}},
            {"name": "submit_result",
             "description": "Return the turn's final result to Hibernaut, as the last action of the turn. "
                            "This is the only way the result is read.",
             "inputSchema": result},
        ]

    def _turn(self, arguments: dict[str, Any]) -> Path:
        name = arguments.get("turn")
        # Only a direct child named turn-*, so no argument can address a path outside the handoff root.
        if not isinstance(name, str) or not _TURN_RE.fullmatch(name):
            raise ToolError("turn must be the turn-… name given in your instruction")
        turn = self.root / name
        if not turn.is_dir() or turn.is_symlink():
            raise ToolError(f"{name} is not an open turn; it may have finished or been cancelled")
        return turn

    def call(self, name: str, arguments: dict[str, Any]) -> str:
        turn = self._turn(arguments)
        if name == "get_task":
            return (turn / "prompt.md").read_text(encoding="utf-8")
        if name == "submit_result":
            body = {k: v for k, v in arguments.items() if k != "turn"}
            errors = schema_errors(body, self.schema)
            if errors:
                raise ToolError("result rejected: " + "; ".join(errors) + ". Fix it and call submit_result again.")
            tmp = turn / "result.json.tmp"
            tmp.write_text(json.dumps(body, ensure_ascii=False), encoding="utf-8")
            os.replace(tmp, turn / "result.json")
            return "Result received. The turn is complete."
        raise ToolError(f"unknown tool {name}")

    def handle(self, request: dict[str, Any]) -> dict[str, Any] | None:
        method, rid = request.get("method"), request.get("id")
        if rid is None:
            return None
        params = cast(dict[str, Any], request.get("params") or {})
        if method == "initialize":
            result: dict[str, Any] = {
                "protocolVersion": params.get("protocolVersion") or _DEFAULT_PROTOCOL,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": SERVER_NAME, "version": "1"},
            }
        elif method == "ping":
            result = {}
        elif method == "tools/list":
            result = {"tools": self.tools()}
        elif method == "tools/call":
            try:
                text = self.call(str(params.get("name")), cast(dict[str, Any], params.get("arguments") or {}))
                result = {"content": [{"type": "text", "text": text}]}
            except (ToolError, OSError) as exc:
                result = {"content": [{"type": "text", "text": str(exc)}], "isError": True}
        else:
            return {"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": f"unknown method {method}"}}
        return {"jsonrpc": "2.0", "id": rid, "result": result}


def main(root: str) -> int:
    server = HandoffServer(Path(root))
    for line in sys.stdin:
        try:
            request: object = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(request, dict):
            reply = server.handle(cast(dict[str, Any], request))
            if reply is not None:
                sys.stdout.write(json.dumps(reply, ensure_ascii=False) + "\n")
                sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
