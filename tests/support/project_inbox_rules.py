import asyncio
import json
import sqlite3
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path

from agent_runtime.app.config import load_config
from agent_runtime.app.main import build_daemon_from_config
from tests.support.sde_executor import make_base_repo


FAKE_CLI = textwrap.dedent(
    """
    import json, os, pathlib, sys, time

    events = json.loads(sys.argv[-1].split("## events\\n", 1)[1])
    text = events[-1]["payload"]["text"]
    cwd = os.getcwd()
    pathlib.Path(cwd, f"marker-{text}.txt").write_text(text)
    with open(os.environ["FAKE_CLI_LOG"], "a") as log:
        log.write(json.dumps({"cwd": cwd, "text": text}) + "\\n")
    barrier = os.environ.get("BARRIER_DIR")
    if barrier:
        # Every run waits here until all expected runs started: they only finish if they ran concurrently.
        pathlib.Path(barrier, text).touch()
        deadline = time.time() + 20
        while len(os.listdir(barrier)) < int(os.environ["BARRIER_N"]):
            if time.time() > deadline:
                sys.exit("barrier timeout")
            time.sleep(0.02)
    print(json.dumps({"outcome": "emit", "messages": [f"{text} ran in {cwd}"]}))
    """
)


def git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout


def project_table(name, inbox_id, checkout, **extra):
    lines = [f"[projects.{name}]", f"inbox_id = {json.dumps(inbox_id)}", f'repo = "kun-g/{name}"',
             f'checkout = "{checkout}"', 'worktree_backend = "git"']
    lines += [f"{k} = {json.dumps(v)}" for k, v in extra.items()]
    return "\n".join(lines) + "\n"


class _DispatchCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.repo = make_base_repo(self.root)
        self.cli = self.root / "fake_cli.py"
        self.cli.write_text(FAKE_CLI)
        self.cli_log = self.root / "cli.jsonl"
        self.barrier = self.root / "barrier"
        self.barrier.mkdir()
        self.db_path = self.root / "hb.db"

    def write_config(self, projects: str, barrier_n: int = 0) -> Path:
        env = {"FAKE_CLI_LOG": str(self.cli_log)}
        if barrier_n:
            env.update(BARRIER_DIR=str(self.barrier), BARRIER_N=str(barrier_n))
        path = self.root / "hibernaut.toml"
        path.write_text(
            f'[runtime]\ndb_path = "{self.db_path}"\nmemory_root = "{self.root / "mem"}"\n'
            'default_executor = "cli"\nmax_concurrency = 4\n'
            f'[executors.cli]\ncommand = "{sys.executable}"\nargs = ["{self.cli}"]\ntimeout_seconds = 60\n'
            f"[executors.cli.env]\n" + "".join(f'{k} = "{v}"\n' for k, v in env.items()) + projects
        )
        return path

    async def dispatch(self, path: Path, events: list[tuple[str, dict, str | None]]):
        daemon = build_daemon_from_config(load_config(path))
        self.addCleanup(daemon.close)
        stop = asyncio.Event()
        pool = asyncio.create_task(daemon.pool.run_forever(stop))
        try:
            for n, (inbox_id, payload, key) in enumerate(events):
                daemon.handler.ingest(inbox_id=inbox_id, source="test", external_id=f"e{n}-{time.monotonic_ns()}",
                                      payload=payload,
                                      correlation_key=key)
            await daemon.pool.wait_until_idle(timeout_seconds=60)
        finally:
            stop.set()
            await pool
        return daemon

    def rows(self, sql: str):
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(sql).fetchall()

    def outbox(self):
        return dict(self.rows("SELECT inbox_id, content FROM outbox ORDER BY seq"))

    def cli_runs(self):
        return [json.loads(line) for line in self.cli_log.read_text().splitlines()] if self.cli_log.exists() else []

    def assert_primary_untouched(self):
        self.assertEqual(git(self.repo, "status", "--porcelain", "--untracked-files=all"), "")
        self.assertEqual(sorted(p.name for p in self.repo.iterdir()), [".git", "calc.py"])
