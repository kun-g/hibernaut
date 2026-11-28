from __future__ import annotations

import hashlib
import http.client
import json
import os
import shlex
import shutil
import signal
import sqlite3
import subprocess
import sys
import textwrap
import time
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path
from typing import Any
from collections.abc import Callable

import pytest

from agent_runtime.adapters.runner_pilot import RunnerPilotRelay
from tests.support.runner_pilot_support import (
    RUNNER_ID,
    SRC,
    children_of,
    free_port,
    protocol_repo,
    runner_toml,
    start_service,
    write_token,
)


PILOT = "hibernaut:81"
OTHER = "hibernaut:82"
KEY = "github:kun-g/hibernaut#81"


def _relay(tmp_path: Path) -> RunnerPilotRelay:
    token_file = tmp_path / "relay.token"
    if not token_file.exists():
        write_token(token_file)
    return RunnerPilotRelay(runner_id=RUNNER_ID, project_key="hibernaut", port=free_port(), token_file=token_file)


ORIGINAL_CLI = textwrap.dedent(
    """
    import json, sys
    events = json.loads(sys.argv[-1].split("## events\\n", 1)[1])
    print(json.dumps({"outcome": "emit", "messages": ["original path: " + events[-1]["payload"]["text"]]}))
    """
)


class Deployment:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.runner_repo = root / "runner-clone"
        self.commit = protocol_repo(self.runner_repo)
        self.checkout = root / "hb-checkout"
        protocol_repo(self.checkout)
        (root / "cli.py").write_text(ORIGINAL_CLI)
        self.port, self.relay_port = free_port(), free_port()
        self.token = write_token(root / "relay.token")
        self.config = root / "hibernaut.toml"
        self.runner_config = runner_toml(
            root / "runner.toml", repo=self.runner_repo, port=self.relay_port, token_file=root / "relay.token"
        )
        self.hb_log, self.runner_log = root / "hb.log", root / "runner.log"
        self.hb: subprocess.Popen[bytes] | None = None
        self.runner: subprocess.Popen[bytes] | None = None
        self.seq = 0

    def write(self, *, enabled: bool, extra: str = "") -> None:
        text = (
            f'[runtime]\ndb_path = "{self.root}/hb.db"\nmemory_root = "{self.root}/mem"\ndefault_executor = "cli"\n'
            f'[channels]\nport = {self.port}\n'
            f'[executors.cli]\ncommand = "{sys.executable}"\nargs = ["{self.root}/cli.py"]\ntimeout_seconds = 30\n'
            f'[projects.hibernaut]\nrepo = "fixture/test"\ncheckout = "{self.checkout}"\nworktree_backend = "git"\n'
            'inbox_id = ["proj:hibernaut", "hibernaut:.*"]\n'
            f'[runner_pilot]\nenabled = {str(enabled).lower()}\ninbox_id = "{PILOT}"\nrunner_id = "{RUNNER_ID}"\n'
            f'project_key = "hibernaut"\nport = {self.relay_port}\ntoken_file = "relay.token"\n{extra}'
        )
        draft = self.root / "draft.toml"
        draft.write_text(text)
        draft.replace(self.config)

    def reload(self, *, enabled: bool, extra: str = "", expect: str = "[config] Reloaded") -> None:
        seen = self.hb_log.read_text().count(expect)
        self.write(enabled=enabled, extra=extra)
        self.until(lambda: self.hb_log.read_text().count(expect) > seen)

    def start_hb(self, *, fast_retry: bool = False) -> None:
        entry = ("-m", "agent_runtime.app.main")
        if fast_retry:
            entry = ("-c", "from agent_runtime.adapters import runner_bridge; runner_bridge._NODE_POLL_SECONDS = 0.02; "
                           "from agent_runtime.app.main import main; main()")
        self.launch_hb(entry)

    def launch_hb(self, entry: tuple[str, ...]) -> None:
        with self.hb_log.open("ab") as stream:
            self.hb = subprocess.Popen(
                [sys.executable, *entry, "--config", str(self.config),
                 "--config-poll-interval", "0.05", "--outbox-interval", "0.05"],
                cwd=self.root, env={**os.environ, "PYTHONPATH": str(SRC)}, stdout=stream, stderr=stream,
            )
        self.until(self.http_ready)

    def http_ready(self) -> bool:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=2)
        try:
            conn.request("POST", "/readiness-probe")
            return conn.getresponse().status == 404
        except ConnectionRefusedError:
            return False
        finally:
            conn.close()

    def start_runner(self, *, fast_retry: bool = False) -> None:
        entry = ("-m", "runner.service")
        if fast_retry:
            entry = ("-c", "from functools import partial; from runner import service; "
                           "service.RunnerRelayAgent = partial(service.RunnerRelayAgent, reconnect_delays=(0.02,)); "
                           "raise SystemExit(service.main())")
        self.runner = start_service(self.runner_config, self.runner_log, entry=entry)

    def start_gated_runner(self, monkeypatch: pytest.MonkeyPatch, *, fast_retry: bool = False) -> Path:
        gate = self.root / "hold-check"
        real_git = shutil.which("git")
        assert real_git is not None
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        shim = bin_dir / "git"
        shim.write_text(
            "#!/bin/sh\nfor arg do\n"
            f'  if [ "$arg" = cat-file ] && [ -p {shlex.quote(str(gate))} ]; then\n'
            f"    {{ printf '%s\\n' \"$PPID\" > {shlex.quote(str(gate.with_suffix('.ready')))}; read -r go; }} < {shlex.quote(str(gate))}\n"
            "    break\n  fi\ndone\n"
            f'exec {shlex.quote(real_git)} "$@"\n'
        )
        shim.chmod(0o755)
        with monkeypatch.context() as env:
            env.setenv("PATH", str(bin_dir) + os.pathsep + os.environ["PATH"])
            self.start_runner(fast_retry=fast_retry)
        return gate

    def force_stop_hb(self) -> None:
        assert self.hb is not None
        self.hb.terminate()
        self.until(lambda: "Stopping: waiting for runs in progress" in self.hb_log.read_text())
        self.hb.terminate()
        assert self.hb.wait(timeout=20) == 0, self.logs()
        self.hb = None

    def stop_runner(self) -> int:
        assert self.runner is not None
        self.runner.send_signal(signal.SIGTERM)
        return self.runner.wait(timeout=20)

    def close(self) -> None:
        for proc in (self.hb, self.runner):
            if proc is not None and proc.poll() is None:
                proc.terminate()
                proc.terminate()
                try:
                    proc.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()

    def until(self, check: Callable[[], Any], timeout: float = 25.0, *, interval: float = 0.05) -> Any:
        deadline = time.monotonic() + timeout
        while not (value := check()):
            assert self.hb is None or self.hb.poll() is None, self.logs()
            assert time.monotonic() < deadline, self.logs()
            time.sleep(interval)
        return value

    def gated_worker(self, ready: Path) -> int:
        # The Git shim identifies the exact process that requested the blocked object read.
        pid = int(self.until(lambda: ready.is_file() and ready.read_text().strip(), interval=0.01))
        parent, command = subprocess.check_output(["ps", "-ww", "-p", str(pid), "-o", "ppid=", "-o", "command="],
                                                  text=True).strip().split(None, 1)
        assert self.runner and int(parent) == self.runner.pid and "protocol_check.py" in command
        return pid

    def logs(self) -> str:
        return "\n".join(p.read_text() for p in (self.hb_log, self.runner_log) if p.exists())

    def _post(self, body: dict[str, Any]) -> dict[str, Any] | None:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        try:
            conn.request("POST", "/events", json.dumps(body), {"Content-Type": "application/json"})
            response = conn.getresponse()
            data: dict[str, Any] = json.loads(response.read())
            assert response.status == 202, data
            return data
        except ConnectionRefusedError:
            return None
        finally:
            conn.close()

    def post(self, inbox: str, *, key: str | None = None, **payload: Any) -> str:
        self.seq += 1
        body: dict[str, Any] = {
            "inbox_id": inbox, "external_id": f"e{self.seq}",
            "payload": {"text": f"t{self.seq}", **payload},
            # Also the work_key of the original path's worktree.
            "correlation_key": key or f"github:fixture/test#{self.seq}",
        }
        data = self._post(body)
        assert data is not None
        return str(data["event_id"])

    def rows(self, sql: str, *params: Any, db: str = "hb.db") -> list[sqlite3.Row]:
        with closing(sqlite3.connect(self.root / db, timeout=5)) as conn:
            conn.row_factory = sqlite3.Row
            return conn.execute(sql, params).fetchall()

    def status(self, event_id: str) -> str:
        return str(self.rows("SELECT status FROM event_inbox WHERE event_id = ?", event_id)[0]["status"])

    def finished(self, *event_ids: str, timeout: float = 25.0) -> None:
        with closing(sqlite3.connect(self.root / "hb.db", timeout=5)) as db:
            def all_done() -> bool:
                return all(db.execute("SELECT status FROM event_inbox WHERE event_id = ?", (e,)).fetchone() == ("done",)
                           for e in event_ids)
            self.until(all_done, timeout=timeout, interval=0.01)

    def outbox(self, inbox: str) -> list[str]:
        return [r["content"] for r in self.rows("SELECT content FROM outbox WHERE inbox_id = ? ORDER BY seq", inbox)]

    def runs(self, inbox: str = PILOT) -> list[sqlite3.Row]:
        return self.rows("SELECT * FROM runs WHERE inbox_id = ? ORDER BY started_at", inbox, db="ledger.db")

    def ledger_log(self, run: sqlite3.Row) -> str:
        assert str(run["log_ref"]).startswith("local://"), dict(run)
        (path,) = (self.root / "logs").rglob(f"{run['run_id']}.log")
        return path.read_text()

    def worker(self) -> int:
        # The check process the Runner service started for the run in progress.
        assert self.runner is not None
        pid = self.runner.pid
        def find() -> int | None:
            return next((c for c, cmd in children_of(pid).items() if "protocol_check.py" in cmd), None)
        found = self.until(find)
        assert isinstance(found, int)
        return found

    def relay_open(self) -> bool:
        conn = http.client.HTTPConnection("127.0.0.1", self.relay_port, timeout=2)
        try:
            conn.connect()
            return True
        except OSError:
            return False
        finally:
            conn.close()

    def check(self, inbox: str = PILOT, **payload: Any) -> str:
        return self.post(inbox, candidate_commit=self.commit, **payload)

    def assert_original_path(self, inbox: str, *, key: str | None = None) -> None:
        before = len(self.outbox(inbox))
        self.finished(self.post(inbox, key=key))
        assert self.outbox(inbox)[before:] == [f"original path: t{self.seq}"]
        assert (self.runs(inbox)[-1]["executor_id"], self.runs(inbox)[-1]["runner_id"]) == ("cli", "local")


@pytest.fixture
def pilot(tmp_path: Path) -> Iterator[Deployment]:
    deployment = Deployment(tmp_path)
    try:
        yield deployment
    finally:
        deployment.close()
        leaked = [p.name for p in (deployment.hb_log, deployment.runner_log)
                  if p.exists() and (deployment.token in p.read_text() or "synthetic-service-token" in p.read_text())]
        assert leaked == []


def _runner_run(deployment: Deployment, run: sqlite3.Row) -> Path:
    # From the ledger row to the node's own log, whose digest the ledger recorded.
    line = next(x for x in deployment.ledger_log(run).splitlines() if x.startswith("runner://"))
    location, *pairs = line.split()
    facts = dict(p.split("=", 1) for p in pairs)
    path = deployment.root / "logs" / location.removeprefix(f"runner://{RUNNER_ID}/")
    assert path.name == f"{facts['run_id']}.log"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == facts["sha256"]
    assert path.stat().st_size == int(facts["bytes"])
    return path
