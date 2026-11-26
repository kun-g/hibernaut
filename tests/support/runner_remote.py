# The read-only remote agent node (docs/runner.md §10): the node's service, what its CLI may read, and Hibernaut's side.
from __future__ import annotations

import hashlib
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from tests.support.fake_gated_cli import install_gated_cli
from tests.support.runner_pilot_support import ROOT, SRC, commit, free_port, protocol_repo, write_token
from tests.support.runner_pilot import Deployment
from tests.support.runner_tool_gate import plan


REMOTE_ID = "remote-node"
AGENT = "remote-claude"
INBOX = "hibernaut:83"


def _fake_claude(directory: Path, log: Path) -> Path:
    fake = install_gated_cli(directory, "fake")
    wrapper = directory / "claude"
    wrapper.write_text(f'#!/bin/sh\nFAKE_LOG="{log}" exec "{fake}" "$@"\n', encoding="utf-8")
    wrapper.chmod(0o755)
    return wrapper


def agent_toml(path: Path, *, repo: Path, port: int, token_file: Path, command: Path) -> Path:
    path.write_text(
        f'[runner]\nrunner_id = "{REMOTE_ID}"\nmax_concurrent = 1\nworktree_backend = "git"\n'
        'interactive_surface = "none"\nreadonly_only = true\nstate_dir = "state"\nlogs_dir = "logs"\n'
        '[namespaces.hb]\nprefix = "hb"\nallowed_callers = ["hibernaut"]\n'
        f'[projects.hibernaut]\ncheckout_path = "{repo}"\ndefault_base_ref = "main"\n'
        f'[executors.{AGENT}]\ncli = "claude"\ncommand = "{command}"\n'
        f'[relay]\nurl = "ws://127.0.0.1:{port}/ws/runner"\ntoken_file = "{token_file}"\n',
        encoding="utf-8",
    )
    return path


def _first_order() -> dict[str, Any]:
    text = (ROOT / "deploy/runner/REMOTE.md").read_text(encoding="utf-8")
    body = text.split("/events", 1)[1].split("-d '", 1)[1].split("'", 1)[0]
    order: dict[str, Any] = json.loads(body)
    return order


def _first_order_query() -> str:
    text = (ROOT / "deploy/runner/REMOTE.md").read_text(encoding="utf-8")
    return text.split('sqlite3 ledger.db "', 1)[1].split('"', 1)[0]


class RemoteDeployment(Deployment):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.node = root / "node"
        self.node_repo = self.node / "repo"
        protocol_repo(self.node_repo)
        (self.node_repo / "README.md").write_text("only on the node", encoding="utf-8")
        commit(self.node_repo, "node only")
        self.node_token = write_token(self.node / "relay.token")
        (root / "node-relay.token").touch(mode=0o600)
        (root / "node-relay.token").write_text(self.node_token, encoding="ascii")
        self.remote_port = free_port()
        self.cli_log, self.node_log = self.node / "cli.log", self.node / "node.log"
        self.node_config = agent_toml(
            self.node / "agent-runner.toml", repo=self.node_repo, port=self.remote_port,
            token_file=self.node / "relay.token", command=_fake_claude(self.node / "bin", self.cli_log),
        )
        self.node_proc: subprocess.Popen[bytes] | None = None
        self.runner_enabled = True

    def write(self, *, enabled: bool = True, extra: str = "") -> None:
        super().write(enabled=enabled, extra=(
            f'[executors.{AGENT}]\ncommand = "/usr/bin/false"\nmax_concurrency = 1\n'
            f'[runner]\nenabled = {str(self.runner_enabled).lower()}\nmode = "relay"\nexecutors = ["{AGENT}"]\n'
            'readonly_only = true\nallow_tools = ["Read", "Grep", "Glob"]\nnode_wait_seconds = 8\n'
            f'listen_port = {self.remote_port}\ntoken_file = "node-relay.token"\n{extra}'
        ))

    def stop_hb(self) -> None:
        assert self.hb is not None
        self.hb.terminate()
        self.hb.terminate()
        self.hb.wait(timeout=20)
        self.hb = None

    def start_node(self) -> None:
        with self.node_log.open("ab") as stream:
            self.node_proc = subprocess.Popen(
                [sys.executable, "-m", "runner.agent_service", "--config", str(self.node_config)], cwd=self.node,
                env={**os.environ, "PYTHONPATH": str(SRC), "GH_TOKEN": "synthetic-service-token"},
                stdout=stream, stderr=stream,
            )
        deadline = time.monotonic() + 20
        while "relay connected" not in self.node_log.read_text():
            assert self.node_proc.poll() is None and time.monotonic() < deadline, self.node_log.read_text()
            time.sleep(0.05)

    def stop_node(self) -> int:
        assert self.node_proc is not None
        self.node_proc.send_signal(signal.SIGTERM)
        return self.node_proc.wait(timeout=20)

    def close(self) -> None:
        if self.node_proc is not None and self.node_proc.poll() is None:
            self.stop_node()
        super().close()

    def cli(self, kind: str) -> list[dict[str, Any]]:
        if not self.cli_log.exists():
            return []
        return [e for e in map(json.loads, self.cli_log.read_text().splitlines()) if e["kind"] == kind]

    def ask(self, *calls: tuple[str, dict[str, Any]], **payload: Any) -> str:
        return self.post(INBOX, agent_id=AGENT, **{"text": plan(*calls), **payload})

    def node_run_log(self, run: sqlite3.Row, *, owner: str = INBOX) -> Path:
        line = next(x for x in self.ledger_log(run).splitlines() if x.startswith("runner://"))
        location, *pairs = line.split()
        facts = dict(p.split("=", 1) for p in pairs)
        path = self.node / "logs" / location.removeprefix(f"runner://{REMOTE_ID}/")
        assert path.name == f"{facts['run_id']}.log"
        assert hashlib.sha256(path.read_bytes()).hexdigest() == facts["sha256"]
        assert f"run {facts['run_id']} accepted (executor {AGENT}, owner {owner})" in self.node_log.read_text()
        return path


@pytest.fixture
def remote(tmp_path: Path) -> Iterator[RemoteDeployment]:
    deployment = RemoteDeployment(tmp_path)
    try:
        yield deployment
    finally:
        deployment.close()
        logs = [p for p in (deployment.hb_log, deployment.runner_log, deployment.node_log, *tmp_path.rglob("run_*.log")) if p.exists()]
        secrets = (deployment.token, deployment.node_token, "synthetic-service-token")
        assert [p.name for p in logs if any(s in p.read_text() for s in secrets)] == []
