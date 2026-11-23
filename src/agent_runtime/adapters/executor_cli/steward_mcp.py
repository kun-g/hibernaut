#!/usr/bin/env python3
# Stdio MCP server the launcher gives a Steward run. It holds no state and no credentials: every tool call goes over
# the Unix socket named in argv to the runtime that started the run, which checks permissions and answers. Started as
# `python -I <this file> <socket>`; stdlib only.
from __future__ import annotations

import json
import socket
import sys
from typing import Any, cast

SERVER_NAME = "hibernaut_steward"
_DEFAULT_PROTOCOL = "2025-06-18"


class Runtime:
    def __init__(self, path: str) -> None:
        self._socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._socket.connect(path)
        self._reader = self._socket.makefile("r", encoding="utf-8")

    def ask(self, request: dict[str, Any]) -> dict[str, Any]:
        self._socket.sendall((json.dumps(request, ensure_ascii=False) + "\n").encode("utf-8"))
        line = self._reader.readline()
        if not line:
            raise OSError("the Hibernaut run that owns this session has ended")
        return cast(dict[str, Any], json.loads(line))


def handle(runtime: Runtime, request: dict[str, Any]) -> dict[str, Any] | None:
    method, rid = request.get("method"), request.get("id")
    if rid is None:
        return None
    params = cast(dict[str, Any], request.get("params") or {})
    result: dict[str, Any]
    if method == "initialize":
        result = {
            "protocolVersion": params.get("protocolVersion") or _DEFAULT_PROTOCOL,
            "capabilities": {"tools": {}},
            "serverInfo": {"name": SERVER_NAME, "version": "1"},
        }
    elif method == "ping":
        result = {}
    elif method == "tools/list":
        result = {"tools": runtime.ask({"op": "tools"})["tools"]}
    elif method == "tools/call":
        try:
            answer = runtime.ask({"op": "call", "name": params.get("name"), "arguments": params.get("arguments") or {}})
        except (OSError, ValueError) as exc:
            answer = {"text": str(exc), "error": True}
        result = {"content": [{"type": "text", "text": str(answer.get("text", ""))}]}
        if answer.get("error"):
            result["isError"] = True
    else:
        return {"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": f"unknown method {method}"}}
    return {"jsonrpc": "2.0", "id": rid, "result": result}


def main(path: str) -> int:
    runtime = Runtime(path)
    for line in sys.stdin:
        try:
            request: object = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(request, dict):
            reply = handle(runtime, cast(dict[str, Any], request))
            if reply is not None:
                sys.stdout.write(json.dumps(reply, ensure_ascii=False) + "\n")
                sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
