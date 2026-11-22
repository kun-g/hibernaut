from __future__ import annotations

import json
import tempfile
import time
import unittest
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from agent_runtime.adapters.channels.switchboard_bridge import (
    SwitchboardChannelAdapter,
    SwitchboardInboundBridge,
)
from agent_runtime.adapters.permissions import AutoModePermissionAdapter
from agent_runtime.adapters.storage_sqlite import (
    SQLiteDatabase,
    SQLiteInboxAdapter,
    SQLiteLeaseLockAdapter,
    SQLiteOutboxAdapter,
    SQLiteSessionStoreAdapter,
    SQLiteTimerAdapter,
)
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config, start_switchboard, take_switchboard_secrets
from agent_runtime.core.models import Emit, Event, RunOutcome, SessionRef, Silent, Suspend
from agent_runtime.core.ports import RunCapabilities
from agent_runtime.engine import AgentWorker, EventHandler, OutboxDispatcher
from switchboard.envelope import CanonicalEvent
from switchboard.ports import IngestResult
from tests.fakes import InMemoryMemoryStore, RecordingChannel
from tests.switchboard_support import Harness, make_harness, post, send_github
from tests.test_switchboard_approval_flow import card_click

KEY = "github:kun-g/hibernaut#42"
SCOPE = "proj:hibernaut"


class ScriptedExecutor:
    executor_id = "scripted"

    def __init__(self) -> None:
        self.outcomes: list[RunOutcome] = []
        self.batches: list[list[Event]] = []

    async def execute(
        self,
        events: Sequence[Event],
        session: SessionRef | None,
        caps: RunCapabilities,
    ) -> RunOutcome:
        _ = (session, caps)
        self.batches.append(list(events))
        return self.outcomes.pop(0) if self.outcomes else Silent()


def comment_payload(body: str = "please fix", comment_id: int = 7) -> dict[str, Any]:
    return {
        "action": "created",
        "issue": {"number": 42, "title": "Switchboard phase 1"},
        "comment": {
            "id": comment_id,
            "body": body,
            "created_at": "2026-11-22T05:00:00Z",
            "html_url": f"https://github.com/Kun-G/Hibernaut/issues/42#issuecomment-{comment_id}",
            "user": {"login": "kun", "type": "User"},
        },
        "repository": {"full_name": "Kun-G/Hibernaut"},
        "sender": {"login": "kun", "type": "User"},
    }


def message_event(**overrides: Any) -> CanonicalEvent:
    fields: dict[str, Any] = {
        "source": "github",
        "event_type": "conversation.message",
        "external_id": "comment:1",
        "scope": SCOPE,
        "correlation_key": KEY,
        "actor": "kun",
        "subject": "Switchboard phase 1",
        "body": "please fix",
        "metadata": {"channel": "github", "reply_target": "kun-g/hibernaut#42", "work_key": "wk-42"},
    }
    fields.update(overrides)
    return CanonicalEvent(**fields)


class _Stack(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.h: Harness = make_harness()
        self.tmp = tempfile.TemporaryDirectory()
        self.db = SQLiteDatabase(Path(self.tmp.name) / "hibernaut.db")
        self.inbox = SQLiteInboxAdapter(self.db)
        self.outbox = SQLiteOutboxAdapter(self.db)
        self.sessions = SQLiteSessionStoreAdapter(self.db)
        self.handler = EventHandler(inbox=self.inbox)
        self.executor = ScriptedExecutor()
        self.reclaimed: list[str] = []
        self.worker = AgentWorker(
            worker_id="w1",
            inbox=self.inbox,
            timers=SQLiteTimerAdapter(self.db),
            memory=InMemoryMemoryStore(),
            permissions=AutoModePermissionAdapter(),
            outbox=self.outbox,
            lock=SQLiteLeaseLockAdapter(self.db),
            sessions=self.sessions,
            executor=self.executor,
            on_worktree_reclaim=self.reclaimed.append,
        )
        self.bridge = SwitchboardInboundBridge(self.handler)
        self.h.board.bus.subscribe("hibernaut", self.bridge.handle_canonical_event)
        self.h.board.bus.subscribe("taskboard", lambda e: None)
        self.fallback = RecordingChannel()
        self.channel = SwitchboardChannelAdapter(self.h.board.bus.publish, fallback=self.fallback)
        self.dispatcher = OutboxDispatcher(outbox=self.outbox, channel=self.channel)

    def tearDown(self) -> None:
        self.db.close()
        self.h.close()
        self.tmp.cleanup()

    def pump_switchboard(self) -> None:
        self.h.board.worker.run_once()
        self.h.board.gateway.run_once()

    def http_calls(self) -> list[dict[str, Any]]:
        return self.h.http.calls

    def reports(self) -> list[CanonicalEvent]:
        store = self.h.board.store
        found: list[CanonicalEvent] = []
        for row in store.list_deliveries():
            event = store.get_inbound(row.event_id) if row.consumer_id == "switchboard.outbound" else None
            if event is not None and event.event_type == "execution.reported":
                found.append(event)
        return found


class InboundBridgeTests(_Stack):
    def test_conversation_message_and_work_ready_land_in_event_inbox(self) -> None:
        msg = message_event()
        ready = CanonicalEvent(
            source="taskboard",
            event_type="work.ready",
            external_id="task:9:ready",
            scope=SCOPE,
            correlation_key="taskboard:9",
            subject="Ready task",
            metadata={"work_kind": "task", "state": "ready", "task_id": "9", "preferred_executor": "claude"},
        )
        self.assertEqual(self.h.board.bus.publish(msg).status, "accepted")
        self.assertEqual(self.h.board.bus.publish(ready).status, "accepted")
        self.h.board.worker.run_once()

        self.assertEqual(self.h.board.store.get_delivery(msg.event_id, "hibernaut").status, "acked")  # type: ignore[union-attr]
        pending = {evt.external_id: evt for evt in self.inbox.fetch_pending(SCOPE)}
        self.assertEqual(set(pending), {msg.event_id, ready.event_id})
        stored = pending[msg.event_id]
        self.assertEqual(stored.source, "sb:github")
        self.assertEqual(stored.correlation_key, KEY)
        self.assertEqual(stored.payload["event_type"], "conversation.message")
        self.assertEqual(stored.payload["body"], "please fix")
        self.assertEqual(stored.payload["actor"], "kun")
        self.assertEqual(stored.payload["external_id"], "comment:1")
        self.assertEqual(stored.payload["reply_target"], "kun-g/hibernaut#42")
        self.assertEqual(stored.payload["work_key"], "wk-42")
        self.assertEqual(pending[ready.event_id].payload["metadata"]["preferred_executor"], "claude")

    def test_redelivery_is_deduplicated_and_still_acked(self) -> None:
        msg = message_event()
        self.assertEqual(self.bridge.handle_canonical_event(msg), "acked")
        self.assertEqual(self.bridge.handle_canonical_event(msg), "acked")
        self.assertEqual(len(self.inbox.fetch_pending(SCOPE)), 1)

    def test_storage_failure_nacks_for_switchboard_retry(self) -> None:
        class Broken:
            def ingest(self, **kwargs: Any) -> object:
                raise RuntimeError("disk full")

        bridge = SwitchboardInboundBridge(Broken())
        self.assertEqual(bridge.handle_canonical_event(message_event()), "nack")

        self.h.board.bus.subscribe("hibernaut", bridge.handle_canonical_event)
        msg = message_event(external_id="comment:2")
        self.h.board.bus.publish(msg)
        self.h.board.worker.run_once()
        row = self.h.board.store.get_delivery(msg.event_id, "hibernaut")
        assert row is not None
        self.assertEqual(row.status, "retry")


class OutboundBridgeTests(_Stack):
    async def _dispatch(self, content: str, metadata: dict[str, Any]) -> None:
        self.outbox.enqueue(SCOPE, content, metadata)
        await self.dispatcher.dispatch_once()
        self.pump_switchboard()

    async def test_emit_goes_to_github_comment(self) -> None:
        await self._dispatch(
            "Done.", {"kind": "emit", "correlation_key": KEY, "session_id": "s1", "run_id": "run_1"}
        )
        (report,) = self.reports()
        self.assertEqual(report.metadata["outcome_kind"], "emit")
        self.assertEqual(report.metadata["run_id"], "run_1")
        self.assertEqual(report.correlation_key, KEY)
        self.assertTrue(report.external_id.startswith("outbox:"))
        (call,) = self.http_calls()
        self.assertTrue(call["url"].endswith("/repos/kun-g/hibernaut/issues/42/comments"))
        self.assertIn("Done.", call["json"]["body"])

    async def test_emit_honours_reply_target_override(self) -> None:
        await self._dispatch(
            "Answer",
            {
                "kind": "emit",
                "correlation_key": KEY,
                "session_id": "s1",
                "run_id": "run_2",
                "reply_channel": "feishu",
                "reply_target": "oc_ops",
            },
        )
        (call,) = self.http_calls()
        self.assertEqual(call["json"]["receive_id"], "oc_ops")

    async def test_suspend_sends_approval_card_with_correlation_key(self) -> None:
        await self._dispatch(
            "Push the branch?",
            {
                "kind": "approval_request",
                "request_id": "apr_9",
                "session_id": "s1",
                "correlation_key": KEY,
                "run_id": "run_3",
            },
        )
        (report,) = self.reports()
        self.assertEqual(report.metadata["outcome_kind"], "suspend")
        self.assertEqual(report.metadata["approval_request_id"], "apr_9")
        (call,) = self.http_calls()
        card = json.loads(call["json"]["content"])
        button = next(a for el in card["elements"] if el["tag"] == "action" for a in el["actions"])
        self.assertEqual(button["value"]["correlation_key"], KEY)
        self.assertEqual(button["value"]["approval_request_id"], "apr_9")

    async def test_error_is_reported(self) -> None:
        await self._dispatch(
            "Executor crashed",
            {"outcome_kind": "error", "error_kind": "executor", "correlation_key": KEY, "session_id": "s1"},
        )
        (report,) = self.reports()
        self.assertEqual(report.metadata["outcome_kind"], "error")
        self.assertEqual(report.metadata["error_kind"], "executor")
        self.assertEqual(len(self.http_calls()), 1)

    async def test_silent_is_not_published(self) -> None:
        await self._dispatch("", {"kind": "silent", "correlation_key": KEY, "session_id": "s1"})
        self.assertEqual(self.reports(), [])
        self.assertEqual(self.http_calls(), [])
        self.assertEqual(self.outbox.fetch_pending(), [])

    async def test_session_less_notice_uses_fallback_channel(self) -> None:
        await self._dispatch("worktree reclaimed", {"kind": "reclaim_alert"})
        self.assertEqual(self.reports(), [])
        self.assertEqual([m[1] for m in self.fallback.sent], ["worktree reclaimed"])

    async def test_outbox_retry_publishes_once(self) -> None:
        meta = {"kind": "emit", "correlation_key": KEY, "session_id": "s1", "run_id": "run_4"}
        await self.channel.send(SCOPE, "same", dict(meta))
        await self.channel.send(SCOPE, "same", dict(meta))
        self.assertEqual(len(self.reports()), 1)

    async def test_invalid_report_raises_so_outbox_records_failure(self) -> None:
        channel = SwitchboardChannelAdapter(lambda e: IngestResult(status="invalid", problems=("bad",)))
        with self.assertRaises(RuntimeError):
            await channel.send(SCOPE, "x", {"kind": "emit", "correlation_key": KEY, "session_id": "s1"})
        dispatcher = OutboxDispatcher(outbox=self.outbox, channel=channel)
        self.outbox.enqueue(SCOPE, "x", {"kind": "emit", "correlation_key": KEY, "session_id": "s1"})
        self.assertEqual(await dispatcher.dispatch_once(), 0)
        self.assertEqual(self.outbox.fetch_pending(), [])
        (failed,) = self.outbox.fetch_pending(now_iso="2999-01-01T00:00:00.000000+00:00")
        self.assertEqual(failed.attempts, 1)
        self.assertIsNotNone(failed.next_retry_at)


class ClosedLoopTests(_Stack):
    async def test_github_comment_to_worker_emit_to_github_comment(self) -> None:
        status, _ = send_github(self.h.url, comment_payload(), event="issue_comment", delivery="d-c1")
        self.assertEqual(status, 202)
        self.h.board.worker.run_once()

        self.executor.outcomes.append(Emit(messages=("Fixed in #43",)))
        await self.worker.process_inbox(SCOPE)
        (batch,) = self.executor.batches
        self.assertEqual(batch[0].payload["body"], "please fix")

        (queued,) = self.outbox.fetch_pending()
        for key in ("correlation_key", "session_id", "run_id"):
            self.assertTrue(queued.metadata.get(key), key)
        self.assertEqual(queued.metadata["outcome_kind"], "emit")

        await self.dispatcher.dispatch_once()
        self.pump_switchboard()
        (call,) = self.http_calls()
        self.assertTrue(call["url"].endswith("/repos/kun-g/hibernaut/issues/42/comments"))
        self.assertIn("Fixed in #43", call["json"]["body"])

    async def test_github_suspend_feishu_approval_resumes_session(self) -> None:
        send_github(self.h.url, comment_payload("deploy it"), event="issue_comment", delivery="d-c2")
        self.h.board.worker.run_once()
        self.executor.outcomes.append(Suspend(request_id="apr_77", prompt="Deploy?", session_checkpoint="cp"))
        await self.worker.process_inbox(SCOPE)
        suspended = self.sessions.find_by_request_id("apr_77")
        assert suspended is not None
        self.assertEqual(suspended.status, "suspended")

        await self.dispatcher.dispatch_once()
        self.pump_switchboard()
        (card_call,) = self.http_calls()
        self.assertEqual(card_call["json"]["receive_id"], "oc_approvals")
        card = json.loads(card_call["json"]["content"])
        approve = next(a for el in card["elements"] if el["tag"] == "action" for a in el["actions"])["value"]
        self.assertEqual(approve["correlation_key"], KEY)
        self.assertEqual(approve["session_id"], suspended.session_id)

        status, _ = post(f"{self.h.url}/webhooks/feishu", card_click(approve))
        self.assertEqual(status, 200)
        self.h.board.worker.run_once()

        self.executor.outcomes.append(Emit(messages=("Deployed",)))
        await self.worker.process_inbox(SCOPE)
        resume_batch = self.executor.batches[-1]
        self.assertEqual(resume_batch[0].source, "sb:feishu")
        self.assertEqual(resume_batch[0].payload["approval_request_id"], "apr_77")
        self.assertEqual(resume_batch[0].payload["decision"], "approve")
        resumed = self.sessions.get(suspended.session_id)
        assert resumed is not None
        self.assertEqual((resumed.status, resumed.pending_request_id), ("active", None))

    async def test_work_closed_closes_session_and_reclaims_unshared_work_key(self) -> None:
        send_github(self.h.url, comment_payload(), event="issue_comment", delivery="d-c3")
        self.h.board.worker.run_once()
        self.executor.outcomes.append(Emit(messages=("on it",)))
        await self.worker.process_inbox(SCOPE)
        session = self.sessions.find_by_correlation_key(SCOPE, KEY)
        assert session is not None and session.work_key is not None

        closed = {
            "action": "closed",
            "issue": {"number": 42, "title": "Switchboard phase 1", "state": "closed",
                      "updated_at": "2026-11-22T06:00:00Z"},
            "repository": {"full_name": "Kun-G/Hibernaut"},
            "sender": {"login": "kun", "type": "User"},
        }
        send_github(self.h.url, closed, event="issues", delivery="d-close")
        self.h.board.worker.run_once()
        await self.worker.process_inbox(SCOPE)

        after = self.sessions.get(session.session_id)
        assert after is not None
        self.assertEqual(after.status, "closed")
        self.assertEqual(self.reclaimed, [session.work_key])
        self.assertEqual(len(self.executor.batches), 1)

    async def test_work_closed_keeps_work_key_still_referenced(self) -> None:
        self.sessions.save(SessionRef(session_id="s_a", inbox_id=SCOPE, correlation_key=KEY, work_key="wk"))
        self.sessions.save(SessionRef(session_id="s_b", inbox_id=SCOPE, correlation_key="github:kun-g/hibernaut#43",
                                      work_key="wk"))
        self.bridge.handle_canonical_event(
            CanonicalEvent(source="github", event_type="work.closed", external_id="issue:42:closed", scope=SCOPE,
                           correlation_key=KEY, subject="t", metadata={"work_kind": "issue", "state": "closed"})
        )
        await self.worker.process_inbox(SCOPE)
        self.assertEqual(self.sessions.get("s_a").status, "closed")  # type: ignore[union-attr]
        self.assertEqual(self.reclaimed, [])

    async def test_work_closed_without_session_is_acked(self) -> None:
        status = self.bridge.handle_canonical_event(
            CanonicalEvent(source="github", event_type="work.closed", external_id="issue:99:closed", scope=SCOPE,
                           correlation_key="github:kun-g/hibernaut#99", subject="t",
                           metadata={"work_kind": "issue", "state": "closed"})
        )
        self.assertEqual(status, "acked")
        await self.worker.process_inbox(SCOPE)
        self.assertEqual(self.executor.batches, [])
        self.assertEqual(self.inbox.fetch_pending(SCOPE), [])


class ConfigSwitchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _raw(self, **extra: Any) -> dict[str, Any]:
        return {"runtime": {"memory_root": str(self.root / "memory")}, **extra}

    def test_disabled_by_default_keeps_legacy_channel(self) -> None:
        cfg = parse_config(self._raw())
        self.assertFalse(cfg.switchboard.enabled)
        self.assertIsNone(start_switchboard(cfg.switchboard))
        db = SQLiteDatabase(self.root / "h.db")
        legacy = RecordingChannel()
        daemon = build_daemon_from_config(cfg, database=db, channel=legacy)
        try:
            self.assertIs(daemon.channel, legacy)
            self.assertIsNone(daemon.switchboard_bridge)
        finally:
            daemon.close()

    def test_switchboard_secrets_leave_the_inherited_environment(self) -> None:
        env = {"SWITCHBOARD_GITHUB_TOKEN": "placeholder", "PATH": "/bin"}
        taken = take_switchboard_secrets(env)
        self.assertEqual(taken, {"SWITCHBOARD_GITHUB_TOKEN": "placeholder"})
        self.assertEqual(env, {"PATH": "/bin"})

    def test_enabled_needs_config_path_and_known_mode(self) -> None:
        with self.assertRaises(ValueError):
            parse_config(self._raw(switchboard={"enabled": True}))
        with self.assertRaises(ValueError):
            parse_config(self._raw(switchboard={"enabled": False, "mode": "carrier-pigeon"}))

    def test_enabled_starts_in_process_switchboard_and_wires_both_directions(self) -> None:
        sb_toml = self.root / "switchboard.toml"
        sb_toml.write_text(
            f'[http]\nport = 0\n[sqlite]\npath = "{self.root / "sb.db"}"\n[routing]\ndefault_scope = "user:main"\n',
            encoding="utf-8",
        )
        cfg = parse_config(
            self._raw(switchboard={"enabled": True, "config_path": "switchboard.toml", "consumer_id": "hibernaut"}),
            base_dir=self.root,
        )
        self.assertEqual(cfg.switchboard.config_path, str(sb_toml))
        board = start_switchboard(cfg.switchboard)
        assert board is not None
        db = SQLiteDatabase(self.root / "h.db")
        daemon = build_daemon_from_config(cfg, database=db, channel=RecordingChannel())
        daemon_sb = build_daemon_from_config(cfg, database=db, channel=RecordingChannel(), switchboard=board)
        try:
            self.assertIsInstance(daemon_sb.channel, SwitchboardChannelAdapter)
            self.assertIsNotNone(daemon_sb.switchboard_bridge)
            board.bus.publish(message_event(scope="user:main"))
            deadline = time.monotonic() + 5
            while not daemon_sb.inbox.fetch_pending("user:main") and time.monotonic() < deadline:
                board.worker.run_once()
                time.sleep(0.02)
            self.assertEqual(len(daemon_sb.inbox.fetch_pending("user:main")), 1)
        finally:
            daemon.close()
            board.stop()


if __name__ == "__main__":
    unittest.main()
