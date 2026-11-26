from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from tests.support.m7_service import REPO, SCOPE


def _child_env(**extra: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith(("SWITCHBOARD_", "TASKBOARD_"))}
    env["PYTHONPATH"] = os.pathsep.join([str(REPO / "src"), str(REPO)])
    env.update(extra)
    return env


def test_mcp_stdio_cli_round_trip(tmp_path: Path) -> None:
    config = tmp_path / "taskboard.toml"
    config.write_text(f'[sqlite]\npath = "{tmp_path / "stdio.db"}"\n', encoding="utf-8")
    requests = [
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2024-11-05"}},
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
         "params": {"name": "tb_create_task", "arguments": {"scope": SCOPE, "title": "From stdio"}}},
        {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "tb_list_tasks", "arguments": {}}},
    ]
    proc = subprocess.run(
        [sys.executable, "-m", "taskboard.app", "--mcp-stdio", "--config", str(config)],
        input="".join(json.dumps(r) + "\n" for r in requests),
        capture_output=True,
        text=True,
        timeout=30,
        env=_child_env(),
        cwd=str(tmp_path),
    )
    assert proc.returncode == 0, proc.stderr
    replies = [json.loads(line) for line in proc.stdout.splitlines()]
    assert [r["id"] for r in replies] == [1, 2, 3]
    assert replies[0]["result"]["serverInfo"]["name"] == "taskboard"
    assert replies[1]["result"]["structuredContent"]["task"]["title"] == "From stdio"
    assert [t["title"] for t in replies[2]["result"]["structuredContent"]["tasks"]] == ["From stdio"]
    assert "no Switchboard uplink" in proc.stderr


def test_service_cli_refuses_to_start_without_tokens(tmp_path: Path) -> None:
    config = tmp_path / "taskboard.toml"
    config.write_text(
        '[switchboard]\nrelay_url = "ws://127.0.0.1:9/ws/relay"\nmcp_url = "http://127.0.0.1:9/mcp"\n',
        encoding="utf-8",
    )
    proc = subprocess.run(
        [sys.executable, "-m", "taskboard.app", "--config", str(config)],
        capture_output=True,
        text=True,
        timeout=30,
        env=_child_env(),
        cwd=str(tmp_path),
    )
    assert proc.returncode == 2
    assert "SWITCHBOARD_RELAY_TOKEN" in proc.stderr
