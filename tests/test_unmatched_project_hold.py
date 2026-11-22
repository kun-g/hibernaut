import asyncio
import os
import socket
import sqlite3
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_runtime.app.config import load_config
from agent_runtime.app.main import build_daemon_from_config, serve_configured
from tests.test_project_inbox_rules import _DispatchCase, project_table

HOLD_LOGGER = "agent_runtime.app.projects"
ORPHAN = "proj:orphan"


class TestUnmatchedProjectHold(_DispatchCase):
    def event_rows(self, inbox_id: str):
        return self.rows(
            f"SELECT event_id, status, attempts, session_id, last_error FROM event_inbox "
            f"WHERE inbox_id = '{inbox_id}' ORDER BY seq"
        )

    def assert_held(self, inbox_id: str, count: int):
        rows = self.event_rows(inbox_id)
        self.assertEqual(len(rows), count, rows)
        for _event_id, status, attempts, session_id, last_error in rows:
            self.assertEqual((status, attempts, session_id, last_error), ("pending", 0, None, None), rows)
        self.assertEqual(self.rows(f"SELECT COUNT(*) FROM sessions WHERE inbox_id = '{inbox_id}'"), [(0,)])
        self.assertEqual(self.rows(f"SELECT COUNT(*) FROM outbox WHERE inbox_id = '{inbox_id}'"), [(0,)])
        self.assertEqual(self.rows("SELECT COUNT(*) FROM worker_leases"), [(0,)])
        return [row[0] for row in rows]

    async def test_an_event_needing_a_project_waits_while_other_inboxes_and_explicit_routes_run(self):
        path = self.write_config(project_table("p", "proj:p", self.repo))
        loose = self.root / "loose"
        loose.mkdir()
        daemon = build_daemon_from_config(load_config(path))
        self.addCleanup(daemon.close)
        stop = asyncio.Event()
        pool = asyncio.create_task(daemon.pool.run_forever(stop))
        try:
            with self.assertLogs(HOLD_LOGGER, level="WARNING") as logs:
                daemon.handler.ingest(inbox_id=ORPHAN, source="test", external_id="o1",
                                      payload={"text": "orphan"}, correlation_key="task:o1")
                daemon.handler.ingest(inbox_id="proj:p", source="test", external_id="p1",
                                      payload={"text": "matched"}, correlation_key="task:p1")
                # A route that names its own workdir runs without any project.
                daemon.handler.ingest(inbox_id="adhoc:x", source="test", external_id="x1",
                                      payload={"text": "explicit", "explicit_workdir": str(loose)},
                                      correlation_key="task:x1")
                await daemon.pool.wait_until_idle(timeout_seconds=60)
                # Idle means no requeue loop; nothing re-logs while the event waits.
                await asyncio.sleep(0.3)
                self.assertTrue(daemon.pool._idle_event.is_set())
                (first,) = self.assert_held(ORPHAN, 1)
                self.assertEqual(len(logs.records), 1, [r.getMessage() for r in logs.records])

                # A second event entering the wait is logged once; the first is not repeated.
                daemon.handler.ingest(inbox_id=ORPHAN, source="test", external_id="o2",
                                      payload={"text": "orphan-2"}, correlation_key="task:o2")
                await daemon.pool.wait_until_idle(timeout_seconds=60)
                await asyncio.sleep(0.3)
        finally:
            stop.set()
            await pool

        held = self.assert_held(ORPHAN, 2)
        self.assertEqual(held[0], first)
        messages = [r.getMessage() for r in logs.records]
        self.assertEqual(len(messages), 2, messages)
        self.assertTrue(all(r.levelname == "WARNING" for r in logs.records))
        for event_id, message in zip(held, messages):
            self.assertIn(ORPHAN, message)
            self.assertIn(event_id, message)
            self.assertIn("matches no [projects] entry", message)
        self.assertIn("external_id=o1", messages[0])
        self.assertIn("external_id=o2", messages[1])

        out = self.outbox()
        self.assertIn("matched ran in", out["proj:p"])
        self.assertIn(f"explicit ran in {loose}", out["adhoc:x"])
        self.assertEqual(sorted(r["text"] for r in self.cli_runs()), ["explicit", "matched"])
        self.assertEqual(self.rows(f"SELECT COUNT(*) FROM workspaces WHERE inbox_id = '{ORPHAN}'"), [(0,)])

    async def test_a_restart_rematches_waiting_events_and_runs_each_once(self):
        unmatched = self.write_config(project_table("p", "proj:p", self.repo))
        with self.assertLogs(HOLD_LOGGER, level="WARNING"):
            await self.dispatch(unmatched, [(ORPHAN, {"text": "orphan"}, "task:o1")])
        (event_id,) = self.assert_held(ORPHAN, 1)

        # Restarting with the same rules re-checks the event and it keeps waiting, logged again.
        with self.assertLogs(HOLD_LOGGER, level="WARNING") as logs:
            await self.dispatch(unmatched, [])
        self.assertIn(event_id, logs.records[0].getMessage())
        self.assert_held(ORPHAN, 1)

        matched = self.write_config(
            project_table("p", "proj:p", self.repo) + project_table("orphan", ORPHAN, self.repo)
        )
        await self.dispatch(matched, [])
        await self.dispatch(matched, [])
        self.assertEqual(self.event_rows(ORPHAN)[0][:3], (event_id, "done", 1))
        self.assertEqual([r["text"] for r in self.cli_runs()], ["orphan"])
        self.assertIn("orphan ran in", self.outbox()[ORPHAN])
        self.assertEqual(self.rows(f"SELECT COUNT(*) FROM sessions WHERE inbox_id = '{ORPHAN}'"), [(1,)])

    async def test_a_config_reload_adding_the_rule_runs_the_waiting_event_once(self):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        bindir = self.root / "bin"
        bindir.mkdir()
        gh = bindir / "gh"
        gh.write_text('#!/bin/sh\nprintf "[]\\n"\n')
        gh.chmod(0o700)
        channels = f'[channels]\nhost = "127.0.0.1"\nport = {port}\n'
        path = self.write_config(channels + project_table("p", "proj:p", self.repo))
        config = load_config(path)
        daemon = build_daemon_from_config(config)
        self.addCleanup(daemon.close)

        def status():
            return self.event_rows(ORPHAN)[0][1:3]

        async def wait_for(check):
            deadline = time.monotonic() + 30
            while not check():
                self.assertLess(time.monotonic(), deadline)
                await asyncio.sleep(0.05)

        stop = asyncio.Event()
        with patch.dict(os.environ, {"PATH": f"{bindir}{os.pathsep}{os.environ['PATH']}"}):
            serving = asyncio.create_task(serve_configured(path, config, daemon, stop))
            try:
                with self.assertLogs(HOLD_LOGGER, level="WARNING"):
                    result = daemon.handler.ingest(inbox_id=ORPHAN, source="test", external_id="o1",
                                                   payload={"text": "orphan"}, correlation_key="task:o1")
                    await wait_for(lambda: bool(self.event_rows(ORPHAN)))
                    await asyncio.sleep(1.5)
                self.assertEqual(status(), ("pending", 0))

                path.write_text(path.read_text() + project_table("orphan", ORPHAN, self.repo))
                await wait_for(lambda: status()[0] == "done")
                # The same delivery again is a duplicate, so the event runs only once.
                self.assertTrue(daemon.handler.ingest(inbox_id=ORPHAN, source="test", external_id="o1",
                                                      payload={"text": "orphan"}).duplicate)
                await asyncio.sleep(0.5)
            finally:
                stop.set()
                await serving

        self.assertEqual(status(), ("done", 1))
        self.assertEqual(self.event_rows(ORPHAN)[0][0], result.event_id)
        self.assertEqual([r["text"] for r in self.cli_runs()], ["orphan"])
        with sqlite3.connect(self.db_path) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM outbox WHERE inbox_id = ?", (ORPHAN,)).fetchone(), (1,))

    async def test_suspended_session_decisions_and_close_requests_are_not_held(self):
        from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteSessionStoreAdapter
        from agent_runtime.app.projects import UnmatchedProjectHold
        from agent_runtime.core.models import Event, SessionRef

        db = SQLiteDatabase(":memory:")
        self.addCleanup(db.close)
        sessions = SQLiteSessionStoreAdapter(db)
        sessions.save(SessionRef(session_id="s-susp", inbox_id=ORPHAN, correlation_key="task:s",
                                 status="suspended", pending_request_id="req-1"))
        sessions.save(SessionRef(session_id="s-act", inbox_id=ORPHAN, correlation_key="task:a", status="active"))
        hold = UnmatchedProjectHold(sessions)

        def evt(**kw):
            payload = kw.pop("payload", {"text": "t"})
            return Event(inbox_id=ORPHAN, source=kw.pop("source", "test"), external_id="x", payload=payload, **kw)

        with self.assertLogs(HOLD_LOGGER, level="WARNING"):
            self.assertIsNotNone(hold(evt()))
            self.assertIsNotNone(hold(evt(correlation_key="task:a")))
        self.assertIsNone(hold(evt(payload={"text": "t", "explicit_workdir": "/tmp"})))
        self.assertIsNone(hold(evt(payload={"text": "t", "review_of": "task:dev"})))
        self.assertIsNone(hold(evt(payload={"approval_request_id": "req-1", "approved": True})))
        self.assertIsNone(hold(evt(payload={"action": "close_session"})))
        self.assertIsNone(hold(evt(source="session:close", payload={})))
        self.assertIsNone(hold(evt(correlation_key="task:s")))
        self.assertIsNone(hold(evt(session_id="s-susp")))


class TestHeldEventsInARunningInbox(unittest.IsolatedAsyncioTestCase):
    async def test_a_held_event_is_neither_claimed_folded_nor_an_interrupt(self):
        from tests.fakes import (
            InMemoryInbox,
            InMemoryLock,
            InMemoryMemoryStore,
            InMemoryOutbox,
            InMemoryPermission,
            InMemorySessionStore,
            InMemoryTimerStore,
        )
        from tests.test_batch_grouping import _RecordingExecutor
        from agent_runtime.core.models import Event
        from agent_runtime.engine.worker import AgentWorker

        for policy in ("defer", "fold", "interrupt"):
            with self.subTest(policy=policy):
                inbox = InMemoryInbox()
                executor = _RecordingExecutor()
                worker = AgentWorker(
                    worker_id="w1", inbox=inbox, timers=InMemoryTimerStore(), memory=InMemoryMemoryStore(),
                    permissions=InMemoryPermission(), outbox=InMemoryOutbox(), executor=executor,
                    lock=InMemoryLock(), sessions=InMemorySessionStore(), new_event_policy=policy,
                    hold=lambda evt: "needs a project" if evt.payload.get("held") else None,
                )

                def add(external_id: str, **payload: object) -> Event:
                    stored, _ = inbox.append(Event(inbox_id=ORPHAN, source="test", external_id=external_id,
                                                   payload={"text": external_id, "explicit_workdir": "/tmp", **payload},
                                                   correlation_key="task:k"))
                    return stored

                held_before = add("held-0", held=True)
                add("run-1")

                async def arrive(_batch: list[str]) -> None:
                    if not executor.calls:
                        add("held-2", held=True)
                        worker.notify_new_event(ORPHAN)
                        await asyncio.sleep(0.1)

                executor.during_run = arrive
                await worker.process_inbox(ORPHAN)

                self.assertEqual(executor.calls, [["run-1"]])
                self.assertEqual(executor.started, [["run-1"]])
                pending = inbox.fetch_pending(ORPHAN)
                self.assertEqual([e.external_id for e in pending], ["held-0", "held-2"])
                attempts = inbox.diagnostics([e.event_id for e in pending])
                self.assertEqual({d.attempts for d in attempts.values()}, {0})
                self.assertIsNone(pending[0].session_id)
                self.assertEqual(pending[0].event_id, held_before.event_id)
