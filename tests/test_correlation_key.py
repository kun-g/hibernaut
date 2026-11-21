from __future__ import annotations

import asyncio
import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path

from agent_runtime.app.main import build_daemon
from agent_runtime.core.models import Emit, Event, RunOutcome, SessionRef, new_id
from agent_runtime.core.ports import RunCapabilities
from tests.fakes import RecordingChannel


class _SessionTrackingExecutor:
    executor_id = "mock-tracking"

    def __init__(self) -> None:
        self.calls: list[tuple[str | None, list[str]]] = []

    async def execute(
        self,
        events: Sequence[Event],
        session: SessionRef | None,
        caps: RunCapabilities,
    ) -> RunOutcome:
        _ = caps
        incoming_sid = session.session_id if session is not None else None
        self.calls.append((incoming_sid, [e.external_id for e in events]))
        active_sid = incoming_sid or new_id("sess")
        return Emit(messages=(f"handled:{active_sid}:{len(events)}",), session_id=active_sid)


class TestCorrelationKeyRouting(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="corr_e2e_")
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    async def test_external_ticket_continues_same_session_and_supports_rotation(self) -> None:
        executor = _SessionTrackingExecutor()
        channel = RecordingChannel()
        daemon = build_daemon(
            db_path=self.root / "corr.db",
            memory_root=self.root / "mem_corr",
            max_concurrency=2,
            executor_factory=lambda _inbox_id: executor,
            channel=channel,
        )
        try:
            stop = asyncio.Event()
            daemon_task = asyncio.create_task(
                daemon.run(stop_event=stop, timer_interval_seconds=0.1, outbox_interval_seconds=0.05)
            )
            issue_key = "github:kun-g/hibernaut#42"

            # Webhook 1 arrives without session_id -> creates new session and binds correlation_key.
            daemon.handler.ingest(
                inbox_id="proj:hibernaut",
                source="github_webhook",
                external_id="gh-evt-1",
                correlation_key=issue_key,
                payload={"comment": "opened issue"},
            )
            await daemon.pool.wait_until_idle(timeout_seconds=3.0)

            first_sid = daemon.pool.reports[0].session_id
            self.assertIsNotNone(first_sid)
            assert first_sid is not None
            self.assertEqual(executor.calls[0], (None, ["gh-evt-1"]))

            # Webhook 2 for same issue arrives without session_id -> automatically resumes first_sid.
            daemon.handler.ingest(
                inbox_id="proj:hibernaut",
                source="github_webhook",
                external_id="gh-evt-2",
                correlation_key=issue_key,
                payload={"comment": "added repro steps"},
            )
            await daemon.pool.wait_until_idle(timeout_seconds=3.0)

            self.assertEqual(executor.calls[1], (first_sid, ["gh-evt-2"]))
            self.assertEqual(daemon.pool.reports[1].session_id, first_sid)

            # Rotate session (e.g. context reset) by binding correlation_key to a fresh session_id.
            daemon.sessions.save(
                SessionRef(
                    session_id="sess_rotated_v2",
                    inbox_id="proj:hibernaut",
                    correlation_key=issue_key,
                )
            )

            # Webhook 3 for same issue now automatically routes to sess_rotated_v2.
            daemon.handler.ingest(
                inbox_id="proj:hibernaut",
                source="github_webhook",
                external_id="gh-evt-3",
                correlation_key=issue_key,
                payload={"comment": "follow-up after rotation"},
            )
            await daemon.pool.wait_until_idle(timeout_seconds=3.0)
            stop.set()
            await daemon_task

            self.assertEqual(executor.calls[2], ("sess_rotated_v2", ["gh-evt-3"]))
            self.assertEqual(daemon.pool.reports[2].session_id, "sess_rotated_v2")
        finally:
            daemon.close()

    async def test_consecutive_unsessions_events_with_same_correlation_key_batch_together(self) -> None:
        executor = _SessionTrackingExecutor()
        channel = RecordingChannel()
        daemon = build_daemon(
            db_path=self.root / "corr_batch.db",
            memory_root=self.root / "mem_corr_batch",
            max_concurrency=1,
            executor_factory=lambda _inbox_id: executor,
            channel=channel,
        )
        try:
            # Enqueue two consecutive events with the same correlation_key before running worker.
            daemon.inbox.append(
                Event(
                    inbox_id="proj:hibernaut",
                    source="github_webhook",
                    external_id="batch-1",
                    correlation_key="github:kun-g/hibernaut#99",
                    payload={"comment": "first"},
                )
            )
            daemon.inbox.append(
                Event(
                    inbox_id="proj:hibernaut",
                    source="github_webhook",
                    external_id="batch-2",
                    correlation_key="github:kun-g/hibernaut#99",
                    payload={"comment": "second"},
                )
            )
            daemon.pool.wake("proj:hibernaut")

            stop = asyncio.Event()
            daemon_task = asyncio.create_task(
                daemon.run(stop_event=stop, timer_interval_seconds=0.1, outbox_interval_seconds=0.05)
            )
            await daemon.pool.wait_until_idle(timeout_seconds=3.0)
            stop.set()
            await daemon_task

            self.assertEqual(len(executor.calls), 1)
            self.assertEqual(executor.calls[0], (None, ["batch-1", "batch-2"]))
        finally:
            daemon.close()


if __name__ == "__main__":
    unittest.main()
