"""The four thin wirings between Hibernaut / Switchboard and the Observability Ledger (src/ledger)."""

from __future__ import annotations

import asyncio
import sys
import tempfile
import time
import unittest
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from agent_runtime.adapters.executor_cli import CLITemplate, CommandResult, LocalCLIExecutor, OrcaWorktreeManager
from agent_runtime.adapters.ledger_bridge import RunLedger
from agent_runtime.adapters.storage_sqlite import SQLiteWorkspaceStore
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteInboxAdapter
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon, run_ledger_path, start_switchboard
from agent_runtime.core.models import (
    Emit,
    Event,
    FailureRoute,
    RunLedgerEntry,
    RunOutcome,
    RunTelemetry,
    SessionRef,
    Silent,
)
from agent_runtime.core.ports import ExecutorPort, RunCapabilities
from agent_runtime.engine.worker import AgentWorker, ExecutorFailed
from switchboard.envelope import CanonicalEvent
from switchboard.ports import NoopAuditSink, RecordingAuditSink, safe_audit
from tests.fakes import (
    InMemoryInbox,
    InMemoryLock,
    InMemoryMemoryStore,
    InMemoryOutbox,
    InMemoryPermission,
    InMemorySessionStore,
    InMemoryTimerStore,
)

INBOX = "proj:hibernaut"
KEY = "github:kun-g/hibernaut#42"


class _ScriptedExecutor:
    def __init__(self, script: Sequence[RunOutcome | BaseException], executor_id: str = "claude") -> None:
        self.executor_id = executor_id
        self._script = list(script)
        self.calls = 0

    async def execute(self, events: Sequence[Event], session: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
        _ = (events, session, caps)
        step = self._script[min(self.calls, len(self._script) - 1)]
        self.calls += 1
        if isinstance(step, BaseException):
            raise step
        return step


def _worker(executor: ExecutorPort, ledger: Any, inbox: InMemoryInbox, outbox: InMemoryOutbox) -> AgentWorker:
    return AgentWorker(
        worker_id="w1",
        inbox=inbox,
        timers=InMemoryTimerStore(),
        memory=InMemoryMemoryStore(),
        permissions=InMemoryPermission(),
        outbox=outbox,
        lock=InMemoryLock(),
        sessions=InMemorySessionStore(),
        executor=executor,
        run_ledger=ledger,
    )


class TestSwitchboardAuditWiring(unittest.TestCase):
    def test_audit_sinks_accept_ledger_fields_and_keep_the_old_record_shape(self) -> None:
        sink = RecordingAuditSink()
        safe_audit(sink, "ingest", "accepted", event_id="ev1")
        safe_audit(
            sink, "ingest", "accepted", event_id="ev2",
            correlation_key=KEY, scope=INBOX, trace_id="trc_1", severity="info",
        )
        NoopAuditSink().emit_audit("ingest", "accepted", correlation_key=KEY, scope=INBOX, trace_id="t", severity="warn")
        self.assertEqual(
            sink.records[0],
            {"stage": "ingest", "outcome": "accepted", "source": None, "event_id": "ev1",
             "external_id": None, "consumer_id": None, "detail": {}},
        )
        self.assertEqual(
            {k: sink.records[1][k] for k in ("correlation_key", "scope", "trace_id", "severity")},
            {"correlation_key": KEY, "scope": INBOX, "trace_id": "trc_1", "severity": "info"},
        )

    def test_in_process_switchboard_writes_its_audits_to_the_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "switchboard.toml").write_text(
                f'[http]\nport = 0\n[sqlite]\npath = "{root / "sb.db"}"\n[routing]\ndefault_scope = "user:main"\n',
                encoding="utf-8",
            )
            cfg = parse_config(
                {"runtime": {"memory_root": str(root / "memory")},
                 "switchboard": {"enabled": True, "config_path": "switchboard.toml", "consumer_id": "hibernaut"}},
                base_dir=root,
            )
            ledger = RunLedger(root / "ledger.db")
            board = start_switchboard(cfg.switchboard, audit=ledger.switchboard_audit())
            assert board is not None
            try:
                board.bus.publish(CanonicalEvent(
                    source="github", event_type="conversation.message", external_id="comment:1", scope=INBOX,
                    correlation_key=KEY, actor="kun", subject="s", body="please fix",
                    metadata={"channel": "github", "reply_target": "kun-g/hibernaut#42"},
                ))
                audits = ledger.store.list_audits(service="switchboard", stage="ingest")
            finally:
                board.stop()
                ledger.close()
            self.assertEqual([(a.outcome, a.correlation_key, a.scope) for a in audits], [("accepted", KEY, INBOX)])


class TestWorkerRunLedger(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.ledger = RunLedger(self.root / "ledger.db", retry_delays=(0.0, 0.0, 0.0))
        self.inbox = InMemoryInbox()
        self.outbox = InMemoryOutbox()

    def tearDown(self) -> None:
        self.ledger.close()
        self._tmp.cleanup()

    def _ingest(self, **payload: Any) -> str:
        stored, _ = self.inbox.append(Event(inbox_id=INBOX, source="github", external_id="i42",
                                            payload={"text": "go", **payload}, correlation_key=KEY))
        return stored.event_id

    async def _run(self, executor: _ScriptedExecutor) -> None:
        worker = _worker(executor, self.ledger.recorder, self.inbox, self.outbox)
        try:
            await worker.process_inbox(INBOX)
        except ExecutorFailed:
            pass

    def _status(self, event_id: str) -> str:
        return next(e.status for e in self.inbox.all_events() if e.event_id == event_id)

    async def test_emit_run_is_recorded_with_emit_status_log_and_usage(self) -> None:
        event_id = self._ingest()
        stdout = '{"type":"result","model":"m1","usage":{"input_tokens":320,"output_tokens":80},"total_cost_usd":0.004}'
        telemetry = RunTelemetry(stdout=stdout, stderr="warn: slow", exit_code=0)
        await self._run(_ScriptedExecutor([Emit(messages=("done",), telemetry=telemetry)]))

        runs = self.ledger.store.list_runs(event_id=event_id)
        self.assertEqual(len(runs), 1)
        run = runs[0]
        self.assertEqual((run.outcome_kind, run.emit_status, run.inbox_id, run.executor_id, run.correlation_key),
                         ("emit", "done", INBOX, "claude", KEY))
        # The ledger run_id is the Run's run_id that channels see in Outbox metadata.
        self.assertEqual(self.outbox.fetch_pending()[0].metadata["run_id"], run.run_id)
        self.assertIsNotNone(run.usage)
        assert run.usage is not None
        self.assertEqual((run.usage.usage_source, run.usage.input_tokens, run.usage.output_tokens), ("cli", 320, 80))
        self.assertTrue((run.log_ref or "").startswith("local://"))
        log = self.ledger.read_tools.hibernaut_get_run_log(run_id=run.run_id)
        self.assertIn("warn: slow", log["content"])
        self.assertIn('"total_cost_usd":0.004', log["content"])

    async def test_deterministic_failure_stops_at_the_first_attempt_and_alerts(self) -> None:
        event_id = self._ingest()
        executor = _ScriptedExecutor([PermissionError("workspace isolation: denied")])
        await self._run(executor)

        self.assertEqual(executor.calls, 1)
        self.assertEqual(self._status(event_id), "failed")
        run = self.ledger.store.list_runs(event_id=event_id)[0]
        self.assertEqual((run.outcome_kind, run.error_kind), ("error", "permission_denied"))
        alerts = [m for m in self.outbox.fetch_pending() if m.metadata.get("kind") == "run_failed"]
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].metadata["run_id"], run.run_id)
        self.assertEqual(alerts[0].metadata["error_kind"], "permission_denied")
        self.assertEqual(alerts[0].metadata["outcome_kind"], "error")
        self.assertIn("denied", alerts[0].content)

    async def test_transient_failure_backs_off_and_retries(self) -> None:
        event_id = self._ingest()
        executor = _ScriptedExecutor([ConnectionError("connection reset"), ConnectionError("econnreset"), Silent()])
        await self._run(executor)

        self.assertEqual(executor.calls, 3)
        self.assertEqual(self._status(event_id), "done")
        run = self.ledger.store.list_runs(event_id=event_id)[0]
        self.assertEqual(run.outcome_kind, "silent")
        self.assertFalse([m for m in self.outbox.fetch_pending() if m.metadata.get("kind") == "run_failed"])

    async def test_transient_retries_run_out_after_three_backoffs(self) -> None:
        event_id = self._ingest()
        executor = _ScriptedExecutor([ConnectionError("connection refused")])
        await self._run(executor)
        self.assertEqual(executor.calls, 4)
        self.assertEqual(self._status(event_id), "failed")
        self.assertEqual(self.ledger.store.list_runs(event_id=event_id)[0].error_kind, "network")
        # Out of retries, a transient failure is final and alerts like the others.
        self.assertEqual(len([m for m in self.outbox.fetch_pending() if m.metadata.get("kind") == "run_failed"]), 1)

    def test_default_routes_follow_the_locked_three_lanes(self) -> None:
        ledger = RunLedger(self.root / "default.db")
        try:
            transient = ledger.recorder.route_failure("claude", error=ConnectionError("reset"))
            deterministic = ledger.recorder.route_failure("claude", error=PermissionError("denied"))
            execution = ledger.recorder.route_failure("claude", error=RuntimeError("boom"))
        finally:
            ledger.close()
        self.assertEqual(transient, FailureRoute("network", "transient", (5.0, 15.0, 45.0)))
        self.assertEqual(deterministic, FailureRoute("permission_denied", "deterministic"))
        self.assertEqual(execution, FailureRoute("unknown", "execution"))

    async def test_execution_failure_is_not_retried(self) -> None:
        event_id = self._ingest()
        executor = _ScriptedExecutor([RuntimeError("boom")])
        await self._run(executor)
        self.assertEqual(executor.calls, 1)
        self.assertEqual(self._status(event_id), "failed")
        self.assertEqual(len([m for m in self.outbox.fetch_pending() if m.metadata.get("kind") == "run_failed"]), 1)

    async def test_failed_emit_is_classified_but_keeps_its_result(self) -> None:
        event_id = self._ingest()
        telemetry = RunTelemetry(stdout="", stderr="Error: 429 Too Many Requests", exit_code=1)
        executor = _ScriptedExecutor([Emit(messages=("failed: 429",), status="failed", telemetry=telemetry)])
        await self._run(executor)
        self.assertEqual(executor.calls, 1)
        self.assertEqual(self._status(event_id), "done")
        run = self.ledger.store.list_runs(event_id=event_id)[0]
        self.assertEqual((run.outcome_kind, run.emit_status, run.error_kind, run.exit_code),
                         ("emit", "failed", "rate_limit", 1))

    async def test_replayed_event_records_the_run_it_replays(self) -> None:
        event_id = self._ingest(_replay_of_run_id="run_earlier")
        await self._run(_ScriptedExecutor([Silent()]))
        self.assertEqual(self.ledger.store.list_runs(event_id=event_id)[0].replay_of_run_id, "run_earlier")

    async def test_a_broken_ledger_never_breaks_the_run(self) -> None:
        class _Broken:
            def route_failure(self, executor_id: str, **_: Any) -> FailureRoute:
                raise RuntimeError("ledger down")

            def record_run(self, entry: RunLedgerEntry) -> None:
                raise RuntimeError("ledger down")

        event_id = self._ingest()
        executor = _ScriptedExecutor([RuntimeError("flaky"), Silent()])
        worker = _worker(executor, _Broken(), self.inbox, self.outbox)
        await worker.process_inbox(INBOX)
        # Without a usable route the worker falls back to its own retry count.
        self.assertEqual(executor.calls, 2)
        self.assertEqual(self._status(event_id), "done")


class _Caps:
    memory = InMemoryMemoryStore()

    def is_interrupted(self) -> bool:
        return False


class TestCLIExecutorTelemetry(unittest.IsolatedAsyncioTestCase):
    async def test_cli_outcomes_carry_output_and_exit_code_for_the_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = SQLiteDatabase(":memory:")
            mgr = OrcaWorktreeManager(workspace_store=SQLiteWorkspaceStore(db), backend="git",
                                      runner=lambda _c, _d: CommandResult(0, "", ""))
            ok = ("import json, sys; print('slow start', file=sys.stderr); "
                  "print(json.dumps({'result': 'hi', 'usage': {'input_tokens': 5, 'output_tokens': 2}}))")
            templates = {
                "claude": CLITemplate(name="claude", command=sys.executable, args=("-I", "-c", ok)),
                "bad": CLITemplate(name="bad", command=sys.executable,
                                   args=("-I", "-c", "import sys; print('invalid api key', file=sys.stderr); sys.exit(3)")),
            }
            executor = LocalCLIExecutor(worktree_manager=mgr, inbox_checkout_map={}, templates=templates)
            event = Event(inbox_id=INBOX, source="t", external_id="1", payload={"explicit_workdir": tmp})
            try:
                good = await executor.execute([event], None, _Caps())  # type: ignore[arg-type]
                bad = await executor.execute(
                    [Event(inbox_id=INBOX, source="t", external_id="2",
                           payload={"explicit_workdir": tmp, "executor": "bad"})], None, _Caps())  # type: ignore[arg-type]
            finally:
                db.close()
        self.assertEqual(good, Emit(messages=("hi",), session_id=good.session_id))  # type: ignore[union-attr]
        assert good.telemetry is not None and bad.telemetry is not None
        self.assertEqual((good.telemetry.exit_code, good.telemetry.stderr), (0, "slow start"))
        self.assertIn('"input_tokens": 5', good.telemetry.stdout)
        self.assertEqual((bad.telemetry.exit_code, bad.telemetry.stderr), (3, "invalid api key"))


class TestInboxReplay(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = SQLiteDatabase(Path(self._tmp.name) / "h.db")
        self.inboxes: dict[str, Any] = {"sqlite": SQLiteInboxAdapter(self.db), "memory": InMemoryInbox()}

    def tearDown(self) -> None:
        self.db.close()
        self._tmp.cleanup()

    def _event(self, inbox: Any, external_id: str, status: str) -> str:
        stored, _ = inbox.append(Event(inbox_id=INBOX, source="s", external_id=external_id,
                                       payload={"executor_id": "grok", "text": "x"}))
        if status in ("processing", "failed", "done"):
            inbox.claim_pending(INBOX, [stored.event_id])
        if status == "failed":
            inbox.mark_failed([stored.event_id], "boom")
        if status == "done":
            inbox.mark_done([stored.event_id])
        return stored.event_id

    def test_replay_resets_a_failed_event_to_pending(self) -> None:
        for name, inbox in self.inboxes.items():
            with self.subTest(inbox=name):
                event_id = self._event(inbox, "e1", "failed")
                replayed = inbox.replay(event_id, executor_id_override="claude", replay_of_run_id="run_1")
                self.assertEqual(replayed.status, "pending")
                self.assertEqual(replayed.payload["executor_id"], "claude")
                self.assertEqual(replayed.payload["_replay_of_run_id"], "run_1")
                self.assertEqual([e.event_id for e in inbox.fetch_pending(INBOX)], [event_id])
                diag = inbox.diagnostics([event_id])[event_id]
                self.assertEqual((diag.attempts, diag.last_error), (0, None))

    def test_replay_never_takes_a_run_in_progress_or_a_duplicate(self) -> None:
        for name, inbox in self.inboxes.items():
            with self.subTest(inbox=name):
                pending = self._event(inbox, "p", "pending")
                processing = self._event(inbox, "r", "processing")
                done = self._event(inbox, "d", "done")
                dropped, _ = inbox.append(Event(inbox_id=INBOX, source="s", external_id="d", payload={}))
                with self.assertRaises(ValueError):
                    inbox.replay(pending)
                with self.assertRaises(ValueError):
                    inbox.replay(done)
                with self.assertRaises(ValueError):
                    inbox.replay(processing, force=True)
                with self.assertRaises(ValueError):
                    inbox.replay(dropped.event_id, force=True)
                with self.assertRaises(ValueError):
                    inbox.replay("evt_missing")
                self.assertEqual(inbox.replay(done, force=True).status, "pending")


class TestLedgerConfigAndMCP(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_ledger_db_path_defaults_beside_the_runtime_db(self) -> None:
        cfg = parse_config({"runtime": {"db_path": str(self.root / "hb.db")}})
        self.assertEqual(cfg.runtime.ledger_db_path, "")
        self.assertEqual(run_ledger_path(cfg.runtime.db_path, cfg.runtime.ledger_db_path),
                         (self.root / "ledger.db").resolve())
        explicit = parse_config({"runtime": {"db_path": str(self.root / "hb.db"),
                                             "ledger_db_path": str(self.root / "obs" / "l.db")}})
        self.assertEqual(run_ledger_path(explicit.runtime.db_path, explicit.runtime.ledger_db_path),
                         (self.root / "obs" / "l.db").resolve())
        self.assertIsNone(run_ledger_path(":memory:", ""))

    async def test_mcp_ledger_tools_and_replay_of_a_failed_event(self) -> None:
        state = {"fail": True}

        class _Executor:
            executor_id = "claude"

            async def execute(self, events: Sequence[Event], session: SessionRef | None,
                              caps: RunCapabilities) -> RunOutcome:
                _ = (events, session, caps)
                if state["fail"]:
                    raise PermissionError("permission denied")
                return Silent()

        daemon = build_daemon(db_path=self.root / "hb.db", memory_root=self.root / "mem",
                              executor_factory=lambda _inbox: _Executor())
        assert daemon.mcp_server is not None
        mcp = daemon.mcp_server
        self.assertTrue((self.root / "ledger.db").exists())

        def call(name: str, **arguments: Any) -> dict[str, Any]:
            reply = mcp.handle_rpc({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                                    "params": {"name": name, "arguments": arguments}})
            assert reply is not None
            self.assertNotIn("error", reply, reply)
            return reply["result"]["structuredContent"]

        async def settle(event_id: str, status: str) -> None:
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                if [e for e in daemon.inbox.list_events(inbox_id=INBOX, status=status) if e.event_id == event_id]:
                    return
                await asyncio.sleep(0.02)
            self.fail(f"{event_id} never reached {status}")

        stop = asyncio.Event()
        task = asyncio.create_task(daemon.run(stop_event=stop, timer_interval_seconds=0.1, outbox_interval_seconds=0.05))
        try:
            tools = mcp.handle_rpc({"jsonrpc": "2.0", "id": 0, "method": "tools/list"})
            assert tools is not None
            names = {t["name"] for t in tools["result"]["tools"]}
            self.assertLessEqual({"hibernaut_list_runs", "hibernaut_get_run_log", "hibernaut_list_audits",
                                  "hibernaut_executor_stats", "hibernaut_replay_event"}, names)

            event_id = daemon.handler.ingest(inbox_id=INBOX, source="mcp", external_id="t1",
                                             payload={"text": "go"}, correlation_key=KEY).event_id
            await settle(event_id, "failed")
            failed_runs = call("hibernaut_list_runs", event_id=event_id)["runs"]
            self.assertEqual([r["error_kind"] for r in failed_runs], ["permission_denied"])
            stats = call("hibernaut_executor_stats")["executors"]
            self.assertEqual((stats[0]["executor_id"], stats[0]["failed_runs"]), ("claude", 1))
            self.assertTrue(call("hibernaut_get_run_log", run_id=failed_runs[0]["run_id"])["found"])

            state["fail"] = False
            replay = call("hibernaut_replay_event", event_id=event_id, executor_id="claude")
            self.assertEqual((replay["event_id"], replay["status"]), (event_id, "pending"))
            self.assertEqual(replay["replay_of_run_id"], failed_runs[0]["run_id"])
            await settle(event_id, "done")

            runs = call("hibernaut_list_runs", event_id=event_id)["runs"]
            self.assertEqual([r["outcome_kind"] for r in runs], ["silent", "error"])
            self.assertEqual(runs[0]["replay_of_run_id"], failed_runs[0]["run_id"])
            audits = call("hibernaut_list_audits", stage="inbox.replay")["audits"]
            self.assertEqual([(a["service"], a["outcome"], a["event_id"]) for a in audits],
                             [("hibernaut", "queued", event_id)])

            reply = mcp.handle_rpc({"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                                    "params": {"name": "hibernaut_replay_event", "arguments": {"event_id": event_id}}})
            assert reply is not None
            self.assertEqual(reply["error"]["code"], -32602)
        finally:
            stop.set()
            await task
            daemon.close()


if __name__ == "__main__":
    unittest.main()
