#!/usr/bin/env python3
# Stdio MCP server the launcher gives an SDE session: reading the project's memory. It outlives the runtime process
# that started the session, so it holds no state, lists its tools itself and opens the runtime's socket anew for every
# call; whatever goes wrong there is the call's answer. The runtime checks the token, the permissions and the scope.
# Started as `python -I <this file> <socket> <token>`; stdlib only. A newer runtime answers the copies that older ones
# started, so a request keeps its form: one JSON line each way.
from __future__ import annotations

import json
import os
import socket
import sys
from collections.abc import Iterable
from typing import Any, cast

SERVER_NAME = "hibernaut_memory"
TOOL_NAMES = ("memory_search", "memory_read")
_DEFAULT_PROTOCOL = "2025-06-18"
_CONNECT_SECONDS = 5.0
_ANSWER_SECONDS = 60.0


def tool_definitions(
    *,
    memory: str = "the project memory",
    whose: str = "the project's",
    uris: str = "proj://, shared://, global://, kb:// or role://",
) -> list[dict[str, Any]]:
    # The Steward's tools of the same name are these, in the words of its memory.
    text, flag, number = {"type": "string"}, {"type": "boolean"}, {"type": "integer"}
    return [
        {"name": "memory_search",
         "description": f"Full-text search over {whose} and the shared "
                        "memory, and with include_kb the read-only knowledge mounts.",
         "inputSchema": {"type": "object", "properties": {
             "query": text, "top_k": number, "include_shared": flag, "include_kb": flag, "include_archived": flag,
         }, "required": ["query"]}},
        {"name": "memory_read",
         "description": f"Read a memory document or one Markdown section of it. uri: a path of {memory}, "
                        f"or a {uris} URI from memory_search.",
         "inputSchema": {"type": "object", "properties": {"uri": text, "heading": text, "max_bytes": number},
                         "required": ["uri"]}},
    ]


def ask(path: str, request: dict[str, Any]) -> dict[str, Any]:
    # A directory in /tmp could be another user's, waiting for the token.
    if os.stat(os.path.dirname(path)).st_uid != os.getuid():
        raise PermissionError("its directory belongs to another user")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(_CONNECT_SECONDS)
        connection.connect(path)
        connection.settimeout(_ANSWER_SECONDS)
        connection.sendall(json.dumps(request).encode("ascii") + b"\n")
        with connection.makefile("rb") as reader:
            line = reader.readline()
    if not line.endswith(b"\n"):
        raise ConnectionError("it closed the connection")
    answer: object = json.loads(line)
    if not isinstance(answer, dict):
        raise ValueError("its answer is malformed")
    return cast(dict[str, Any], answer)


def call(path: str, token: str, params: dict[str, Any]) -> dict[str, Any]:
    try:
        answer = ask(path, {"token": token, "name": params.get("name"), "arguments": params.get("arguments") or {}})
    except (OSError, ValueError) as exc:
        answer = {
            "text": f"Project memory cannot be reached right now ({exc}): the Hibernaut runtime may be restarting. "
                    "Go on with the task without it, or try again later.",
            "error": True,
        }
    result: dict[str, Any] = {"content": [{"type": "text", "text": str(answer.get("text", ""))}]}
    if answer.get("error"):
        result["isError"] = True
    return result


def handle(path: str, token: str, request: dict[str, Any]) -> dict[str, Any] | None:
    method, rid, given = request.get("method"), request.get("id"), request.get("params")
    if rid is None:
        return None
    params = cast(dict[str, Any], given) if isinstance(given, dict) else {}
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
        result = {"tools": tool_definitions()}
    elif method == "tools/call":
        result = call(path, token, params)
    else:
        return {"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": f"unknown method {method}"}}
    return {"jsonrpc": "2.0", "id": rid, "result": result}


def serve(path: str, token: str, lines: Iterable[bytes]) -> None:
    for line in lines:
        try:
            request: object = json.loads(line)
        except (ValueError, RecursionError):
            continue
        if isinstance(request, dict):
            reply = handle(path, token, cast(dict[str, Any], request))
            if reply is not None:
                sys.stdout.write(json.dumps(reply) + "\n")
                sys.stdout.flush()


def main(path: str, token: str) -> int:
    serve(path, token, sys.stdin.buffer)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
