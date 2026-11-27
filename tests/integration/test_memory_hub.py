from __future__ import annotations

import json
import subprocess
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from memory_hub import AuthPrincipal, MemoryHubHTTPServer, StaticTokenAuth
from tests.support.memory_hub_fixtures import _make_hub
from tests.support.memory_hub_fixtures import fast_memory_http_shutdown  # noqa: F401 -- pytest fixture, requested by parameter name


def _http_json(url: str, *, method: str = "GET", token: str | None = None, body: dict[str, Any] | None = None) -> tuple[int, dict[str, Any]]:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    headers: dict[str, str] = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as err:
        return err.code, json.loads(err.read().decode("utf-8"))


def test_phase3_primary_writer_mcp_http_auth_and_git_commit(tmp_path: Path, fast_memory_http_shutdown: None) -> None:
    mem_root = tmp_path / "memory"
    mem_root.mkdir(parents=True)
    subprocess.run(["git", "init"], cwd=mem_root, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.email", "test@local"], cwd=mem_root, check=True, capture_output=True)
    subprocess.run(["git", "config", "user.name", "Tester"], cwd=mem_root, check=True, capture_output=True)

    hub = _make_hub(tmp_path, auto_git_commit=True)
    auth = StaticTokenAuth(
        {
            "tok_ro": AuthPrincipal(subject="taskboard", role="readonly", allowed_projects=("hibernaut",), allow_kb=False),
            "tok_propose": AuthPrincipal(subject="hb-vps", role="propose", allowed_projects=("hibernaut",), allow_kb=True),
            "tok_admin": AuthPrincipal(subject="human:kun", role="admin", allowed_projects=("*",), allow_kb=True),
        }
    )
    server = MemoryHubHTTPServer(hub, auth, host="127.0.0.1", port=0)
    server.start_background()
    base = f"http://{server.address[0]}:{server.address[1]}"
    try:
        code, data = _http_json(f"{base}/healthz")
        assert code == 200 and data["status"] == "ok"

        # Unauthorized
        code, _ = _http_json(f"{base}/v1/memory/baseline?project_key=hibernaut")
        assert code == 401

        # Readonly can read baseline, cannot propose, cannot search kb
        code, data = _http_json(f"{base}/v1/memory/baseline?project_key=hibernaut", token="tok_ro")
        assert code == 200 and data["project_key"] == "hibernaut"

        code, _ = _http_json(
            f"{base}/v1/memory/search?scope=hibernaut&q=test&include_kb=true",
            token="tok_ro",
        )
        assert code == 403

        code, _ = _http_json(
            f"{base}/v1/memory/propose",
            method="POST",
            token="tok_ro",
            body={
                "target_uri": "proj://hibernaut/instructions.md",
                "op": "append",
                "content": "- rule",
                "rationale": "r",
            },
        )
        assert code == 403

        # MCP tools/list + tools/call propose -> admin approve -> git commit
        code, tools_resp = _http_json(
            f"{base}/mcp",
            method="POST",
            token="tok_propose",
            body={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        )
        assert code == 200 and len(tools_resp["result"]["tools"]) >= 5

        code, prop_resp = _http_json(
            f"{base}/mcp",
            method="POST",
            token="tok_propose",
            body={
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "memory_propose_write",
                    "arguments": {
                        "target_uri": "proj://hibernaut/instructions.md",
                        "op": "append",
                        "content": "- Primary writer rule via MCP.",
                        "rationale": "remote propose",
                    },
                },
            },
        )
        assert code == 200
        prop_id = prop_resp["result"]["structuredContent"]["proposal_id"]
        assert prop_id

        # Propose role cannot approve
        code, _ = _http_json(
            f"{base}/v1/memory/approve",
            method="POST",
            token="tok_propose",
            body={"proposal_id": prop_id},
        )
        assert code == 403

        # Admin approves -> triggers auto git commit
        code, app_resp = _http_json(
            f"{base}/v1/memory/approve",
            method="POST",
            token="tok_admin",
            body={"proposal_id": prop_id},
        )
        assert code == 200 and app_resp["status"] == "applied"

        log = subprocess.run(["git", "log", "--oneline"], cwd=mem_root, check=True, capture_output=True, text=True)
        assert prop_id in log.stdout
    finally:
        server.shutdown()
        hub.close()
