import asyncio
import contextlib
import http.client
import io
import json
import socket
import sqlite3
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path

from agent_runtime.adapters.storage_sqlite import (
    SQLiteDatabase,
    SQLiteInboxAdapter,
    SQLiteLeaseLockAdapter,
    SQLiteOutboxAdapter,
    SQLiteSessionStoreAdapter,
    SQLiteTimerAdapter,
)
from agent_runtime.app.config import load_config
from agent_runtime.app.main import build_daemon_from_config, serve_configured
from agent_runtime.core.models import Emit
from agent_runtime.engine import AgentWorker, EventHandler
from tests.fakes import InMemoryMemoryStore, InMemoryPermission

# Runs whose text starts with "block" wait for the gate file, standing in for a two-hour SDE run.
GATED_CLI = textwrap.dedent(
    """
    import json, os, pathlib, sys, time

    events = json.loads(sys.argv[-1].split("## events\\n", 1)[1])
    text = events[-1]["payload"]["text"]
    gen = os.environ["GEN"]
    with open(os.environ["RUN_LOG"], "a") as log:
        log.write(json.dumps({"text": text, "gen": gen}) + "\\n")
    if text.startswith("block"):
        gate = pathlib.Path(os.environ["GATE"])
        deadline = time.time() + 30
        while not gate.exists():
            if time.time() > deadline:
                sys.exit("gate timeout")
            time.sleep(0.02)
    print(json.dumps({"outcome": "emit", "messages": [f"{text} by gen {gen}"]}))
    """
)

BLOCKED = "proj:blocked"
OTHER = "proj:other"


class TestReloadKeepsServing(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.cli = self.root / "gated_cli.py"
        self.cli.write_text(GATED_CLI)
        self.run_log = self.root / "runs.jsonl"
        self.gate = self.root / "gate"
        self.workdir = self.root / "work"
        self.workdir.mkdir()
        self.db_path = self.root / "hb.db"
        self.path = self.root / "hibernaut.toml"
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.stderr = io.StringIO()
        self.enterContext(contextlib.redirect_stderr(self.stderr))

    def write_config(self, gen: int) -> None:
        self.path.write_text(
            f'[runtime]\ndb_path = "{self.db_path}"\nmemory_root = "{self.root / "mem"}"\n'
            'default_executor = "cli"\nmax_concurrency = 4\n'
            f'[channels]\nhost = "127.0.0.1"\nport = {self.port}\n'
            f'[executors.cli]\ncommand = "{sys.executable}"\nargs = ["{self.cli}"]\ntimeout_seconds = 60\n'
            f'[executors.cli.env]\nGEN = "{gen}"\nRUN_LOG = "{self.run_log}"\nGATE = "{self.gate}"\n'
        )

    async def start(self):
        self.write_config(1)
        config = load_config(self.path)
        daemon = build_daemon_from_config(config)
        self.addCleanup(daemon.close)
        self.stop = asyncio.Event()
        self.serving = asyncio.create_task(
            serve_configured(self.path, config, daemon, self.stop, reload_interval=0.02, outbox_interval_seconds=0.02)
        )
        self.addAsyncCleanup(self.shutdown)
        await self.wait_for(lambda: self.post(BLOCKED, "warmup")[0] == 202)

    async def shutdown(self):
        self.gate.touch()
        self.stop.set()
        await asyncio.wait_for(self.serving, timeout=30)

    async def reload(self, gen: int) -> None:
        reloads = self.stderr.getvalue().count("[config] Reloaded")
        self.write_config(gen)
        await self.wait_for(lambda: self.stderr.getvalue().count("[config] Reloaded") > reloads)

    def post(self, inbox_id: str, text: str):
        body = {"inbox_id": inbox_id, "external_id": f"{text}-{time.monotonic_ns()}",
                "payload": {"text": text, "explicit_workdir": str(self.workdir)}}
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=2)
        try:
            conn.request("POST", "/events", json.dumps(body), {"Content-Type": "application/json"})
            response = conn.getresponse()
            return response.status, json.load(response)
        except ConnectionRefusedError:
            return None, None
        finally:
            conn.close()

    async def wait_for(self, check, timeout=20.0):
        deadline = time.monotonic() + timeout
        while not await asyncio.to_thread(check):
            if self.serving.done():
                self.serving.result()
                self.fail(f"daemon stopped\n{self.stderr.getvalue()}")
            if time.monotonic() > deadline:
                self.fail(f"timed out\n{self.stderr.getvalue()}")
            await asyncio.sleep(0.02)

    def rows(self, sql: str, *params):
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute(sql, params).fetchall()

    def runs(self):
        if not self.run_log.exists():
            return []
        return [(r["text"], r["gen"]) for r in map(json.loads, self.run_log.read_text().splitlines())]

    def event_status(self, event_id: str) -> str:
        return self.rows("SELECT status FROM event_inbox WHERE event_id = ?", event_id)[0][0]

    def outbox(self, inbox_id: str):
        return self.rows("SELECT content, status FROM outbox WHERE inbox_id = ? ORDER BY seq", inbox_id)

    async def start_blocked_run(self) -> str:
        status, body = self.post(BLOCKED, "block-1")
        self.assertEqual(status, 202)
        await self.wait_for(lambda: ("block-1", "1") in self.runs())
        return body["event_id"]

    async def test_a_run_in_progress_does_not_hold_up_ingress_delivery_or_other_inboxes(self):
        await self.start()
        blocked_id = await self.start_blocked_run()
        reload_started = time.monotonic()
        await self.reload(2)
        # The switch does not wait for the blocked run.
        self.assertLess(time.monotonic() - reload_started, 10)
        self.assertFalse(self.gate.exists())

        status, other = self.post(OTHER, "other-1")
        self.assertEqual(status, 202)
        self.assertEqual(self.event_status(other["event_id"]), "pending")
        await self.wait_for(lambda: self.outbox(OTHER) == [("other-1 by gen 2", "sent")])
        self.assertIn(("other-1", "2"), self.runs())

        status, follow_up = self.post(BLOCKED, "follow-up")
        self.assertEqual(status, 202)
        await asyncio.sleep(0.3)
        self.assertEqual(self.event_status(follow_up["event_id"]), "pending")
        self.assertNotIn("follow-up", [text for text, _ in self.runs()])
        self.assertEqual(self.event_status(blocked_id), "processing")

        self.gate.touch()
        await self.wait_for(lambda: self.event_status(follow_up["event_id"]) == "done")
        self.assertEqual(self.event_status(blocked_id), "done")
        await self.wait_for(lambda: len(self.outbox(BLOCKED)) == 3 and all(s == "sent" for _, s in self.outbox(BLOCKED)))
        self.assertEqual([c for c, _ in self.outbox(BLOCKED)],
                         ["warmup by gen 1", "block-1 by gen 1", "follow-up by gen 2"])
        await asyncio.sleep(0.2)
        self.assertEqual([r for r in self.runs() if r[0] == "follow-up"], [("follow-up", "2")])
        sessions = self.rows("SELECT status FROM sessions WHERE inbox_id = ?", BLOCKED)
        self.assertEqual(len(sessions), 3)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM worker_leases"), [(0,)])

    async def test_two_reloads_while_a_run_is_blocked(self):
        await self.start()
        blocked_id = await self.start_blocked_run()
        await self.reload(2)
        await self.reload(3)

        self.assertEqual(self.post(OTHER, "other-3")[0], 202)
        await self.wait_for(lambda: self.outbox(OTHER) == [("other-3 by gen 3", "sent")])
        status, follow_up = self.post(BLOCKED, "follow-up")
        self.assertEqual(status, 202)
        await asyncio.sleep(0.3)
        self.assertEqual(self.event_status(follow_up["event_id"]), "pending")

        self.gate.touch()
        await self.wait_for(lambda: self.event_status(follow_up["event_id"]) == "done")
        self.assertEqual(self.event_status(blocked_id), "done")
        await asyncio.sleep(0.2)
        self.assertEqual([r for r in self.runs() if r[0] in ("follow-up", "block-1")],
                         [("block-1", "1"), ("follow-up", "3")])
        await self.wait_for(lambda: [c for c, _ in self.outbox(BLOCKED)][1:] == ["block-1 by gen 1", "follow-up by gen 3"])

    async def test_an_event_from_a_request_begun_before_leaving_ingest_only_still_runs(self):
        self.path.write_text(
            f'[runtime]\ndb_path = "{self.db_path}"\nmemory_root = "{self.root / "mem"}"\n'
            f'[channels]\nhost = "127.0.0.1"\nport = {self.port}\n'
        )
        config = load_config(self.path)
        ingest_only = build_daemon_from_config(config)
        self.assertTrue(ingest_only.ingest_only)
        self.addCleanup(ingest_only.close)
        self.stop = asyncio.Event()
        self.serving = asyncio.create_task(
            serve_configured(self.path, config, ingest_only, self.stop, reload_interval=0.02,
                             outbox_interval_seconds=0.02)
        )
        self.addAsyncCleanup(self.shutdown)
        await self.wait_for(lambda: self.post(OTHER, "early")[0] == 202)

        await self.reload(2)
        await self.wait_for(lambda: ("early", "2") in self.runs())
        # A request dispatched to the ingest-only handler class before the swap appends its event only now,
        # after the new pool's startup scan of pending inboxes.
        late = ingest_only.handler.ingest(inbox_id=BLOCKED, source="webhook", external_id="late",
                                          payload={"text": "late", "explicit_workdir": str(self.workdir)})
        await self.wait_for(lambda: self.event_status(late.event_id) == "done", timeout=5)
        self.assertEqual([r for r in self.runs() if r[0] == "late"], [("late", "2")])

    async def test_stop_after_a_reload_waits_for_the_blocked_run_and_keeps_its_result(self):
        await self.start()
        blocked_id = await self.start_blocked_run()
        await self.reload(2)
        self.stop.set()
        await asyncio.sleep(0.3)
        self.assertFalse(self.serving.done())
        self.gate.touch()
        await asyncio.wait_for(self.serving, timeout=20)
        self.assertEqual(self.event_status(blocked_id), "done")
        self.assertIn(("block-1 by gen 1",), [(c,) for c, _ in self.outbox(BLOCKED)])
        self.assertEqual(self.rows("SELECT COUNT(*) FROM worker_leases"), [(0,)])


class _SlowExecutor:
    executor_id = "slow"

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.started = asyncio.Event()

    async def execute(self, events, session, caps):
        self.started.set()
        await self.release.wait()
        return Emit(messages=("finished",))


class TestLeaseHeldThroughLongRun(unittest.IsolatedAsyncioTestCase):
    async def test_a_run_longer_than_the_lease_ttl_keeps_its_lease(self):
        db = SQLiteDatabase(":memory:")
        self.addCleanup(db.close)
        inbox = SQLiteInboxAdapter(db)
        lock = SQLiteLeaseLockAdapter(db)
        executor = _SlowExecutor()
        worker = AgentWorker(
            worker_id="old-pool-worker-0", inbox=inbox, timers=SQLiteTimerAdapter(db), memory=InMemoryMemoryStore(),
            permissions=InMemoryPermission(), outbox=SQLiteOutboxAdapter(db), lock=lock,
            sessions=SQLiteSessionStoreAdapter(db), executor=executor,
            # Long enough for a renewal that a busy machine runs late.
            lease_ttl_seconds=1.0,
        )
        EventHandler(inbox=inbox).ingest(inbox_id="i", source="t", external_id="e1", payload={"text": "x"})
        run = asyncio.create_task(worker.process_inbox("i"))
        await asyncio.wait_for(executor.started.wait(), timeout=5)
        await asyncio.sleep(2.5)
        self.assertFalse(lock.acquire("i", "new-pool-worker-0", 0.15))
        executor.release.set()
        reports = await run
        self.assertEqual(len(reports), 1)
        self.assertTrue(lock.acquire("i", "new-pool-worker-0", 0.15))


if __name__ == "__main__":
    unittest.main()
