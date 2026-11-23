from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from agent_runtime.adapters.ledger_bridge import RunLedger
from agent_runtime.adapters.runner_bridge import RunnerExecutorBridge
from agent_runtime.core.models import Event
from agent_runtime.engine.worker import AgentWorker
from runner.cli_drivers import TemplateCLIDriver
from runner.daemon import RunnerDaemon
from runner.protocol import RunRequest
from runner.transport.in_process import InProcessRunnerClient
from tests.fakes import (
    InMemoryInbox,
    InMemoryLock,
    InMemoryMemoryStore,
    InMemoryOutbox,
    InMemoryPermission,
    InMemorySessionStore,
    InMemoryTimerStore,
)
from tests.test_runner_stage1 import _init_git_repo, _make_config

INBOX = "proj:hibernaut"
KEY = "github:kun-g/hibernaut#42"


def _worker(bridge: RunnerExecutorBridge, ledger: RunLedger, inbox: InMemoryInbox, outbox: InMemoryOutbox) -> AgentWorker:
    return AgentWorker(
        worker_id="w1",
        inbox=inbox,
        timers=InMemoryTimerStore(),
        memory=InMemoryMemoryStore(),
        permissions=InMemoryPermission(),
        outbox=outbox,
        lock=InMemoryLock(),
        sessions=InMemorySessionStore(),
        executor=bridge,
        run_ledger=ledger.recorder,
    )


def _drivers(flag: Path) -> dict[str, TemplateCLIDriver]:
    py = sys.executable
    hold = f"import pathlib, time\nwhile not pathlib.Path({str(flag)!r}).exists():\n    time.sleep(0.05)\n"
    return {
        "claude": TemplateCLIDriver(
            executor_id="claude",
            driver_kind="custom",
            command=py,
            args=("-c", "import json; print(json.dumps({'outcome': 'emit', 'message': 'done from runner'}))"),
        ),
        "hold": TemplateCLIDriver(executor_id="hold", driver_kind="custom", command=py, args=("-c", hold)),
    }


def _ingest(inbox: InMemoryInbox) -> str:
    stored, _ = inbox.append(
        Event(inbox_id=INBOX, source="github", external_id="i42", payload={"text": "go"}, correlation_key=KEY)
    )
    return stored.event_id


def test_slots_full_refusal_is_recorded_as_rate_limit_on_the_executing_runner(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_git_repo(repo, with_remote=True)
    flag = tmp_path / "release"
    daemon = RunnerDaemon(_make_config(tmp_path, repo, max_concurrent=1), drivers=_drivers(flag))
    client = InProcessRunnerClient(daemon, caller_id="hibernaut", bound_namespace="hb")
    bridge = RunnerExecutorBridge(runner=client, executor_id="claude", project_key="hibernaut", executor_calls_tools=False)
    ledger = RunLedger(tmp_path / "ledger.db", retry_delays=(0.0, 0.0, 0.0))
    inbox, outbox = InMemoryInbox(), InMemoryOutbox()
    event_id = _ingest(inbox)

    async def scenario() -> None:
        hold = asyncio.create_task(
            client.run(
                RunRequest(
                    run_id="run_hold",
                    namespace="hb",
                    project_key="hibernaut",
                    owner_scope=INBOX,
                    work_key="other#1",
                    executor_id="hold",
                    readonly=False,
                    prompt="hold the only slot",
                )
            )
        )
        while daemon.capabilities_sync().slots.available > 0:
            await asyncio.sleep(0.02)
        try:
            await _worker(bridge, ledger, inbox, outbox).process_inbox(INBOX)
        finally:
            flag.write_text("go", encoding="utf-8")
            await hold

    try:
        asyncio.run(scenario())
        runs = ledger.store.list_runs(event_id=event_id)
    finally:
        ledger.close()
        daemon.close()

    assert len(runs) == 1
    run = runs[0]
    # The refusal is delivered as an Emit like before: the event is done, no retry, no run_failed alert.
    assert (run.outcome_kind, run.emit_status) == ("emit", "refused")
    assert next(e.status for e in inbox.all_events() if e.event_id == event_id) == "done"
    assert not [m for m in outbox.fetch_pending() if m.metadata.get("kind") == "run_failed"]
    # The runner's structured error_kind wins over guessing from "slots_full".
    assert run.error_kind == "rate_limit"
    assert run.runner_id == "runner-test-01"


def test_successful_runner_run_is_recorded_on_the_executing_runner(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_git_repo(repo, with_remote=True)
    daemon = RunnerDaemon(_make_config(tmp_path, repo), drivers=_drivers(tmp_path / "unused"))
    client = InProcessRunnerClient(daemon, caller_id="hibernaut", bound_namespace="hb")
    bridge = RunnerExecutorBridge(runner=client, executor_id="claude", project_key="hibernaut", executor_calls_tools=False)
    ledger = RunLedger(tmp_path / "ledger.db")
    inbox, outbox = InMemoryInbox(), InMemoryOutbox()
    event_id = _ingest(inbox)

    try:
        asyncio.run(_worker(bridge, ledger, inbox, outbox).process_inbox(INBOX))
        runs = ledger.store.list_runs(event_id=event_id)
    finally:
        ledger.close()
        daemon.close()

    assert len(runs) == 1
    assert (runs[0].outcome_kind, runs[0].emit_status, runs[0].error_kind) == ("emit", "done", None)
    assert runs[0].runner_id == "runner-test-01"
    assert runs[0].exit_code == 0


def test_new_event_interrupt_of_a_runner_run_is_recorded_on_the_executing_runner(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_git_repo(repo, with_remote=True)
    started = tmp_path / "started"
    # First Run blocks until the worker interrupts it; the merged rerun finds the marker and answers at once.
    script = (
        "import json, pathlib, time\n"
        f"p = pathlib.Path({str(started)!r})\n"
        "if p.exists():\n"
        "    print(json.dumps({'outcome': 'emit', 'message': 'merged from runner'}))\n"
        "else:\n"
        "    p.write_text('1')\n"
        "    time.sleep(60)\n"
    )
    drivers = {
        "claude": TemplateCLIDriver(executor_id="claude", driver_kind="custom", command=sys.executable, args=("-c", script))
    }
    daemon = RunnerDaemon(_make_config(tmp_path, repo), drivers=drivers)
    client = InProcessRunnerClient(daemon, caller_id="hibernaut", bound_namespace="hb")
    bridge = RunnerExecutorBridge(runner=client, executor_id="claude", project_key="hibernaut", executor_calls_tools=False)
    ledger = RunLedger(tmp_path / "ledger.db")
    inbox, outbox = InMemoryInbox(), InMemoryOutbox()
    first_id = _ingest(inbox)
    worker = AgentWorker(
        worker_id="w1",
        inbox=inbox,
        timers=InMemoryTimerStore(),
        memory=InMemoryMemoryStore(),
        permissions=InMemoryPermission(),
        outbox=outbox,
        lock=InMemoryLock(),
        sessions=InMemorySessionStore(),
        executor=bridge,
        run_ledger=ledger.recorder,
        new_event_policy="interrupt",
    )

    async def scenario() -> str:
        run = asyncio.create_task(worker.process_inbox(INBOX))
        while not started.exists():
            await asyncio.sleep(0.02)
        second, _ = inbox.append(
            Event(inbox_id=INBOX, source="github", external_id="i43", payload={"text": "more"}, correlation_key=KEY)
        )
        await asyncio.wait_for(run, timeout=30)
        return second.event_id

    try:
        second_id = asyncio.run(scenario())
        runs = ledger.store.list_runs(event_id=first_id)
    finally:
        ledger.close()
        daemon.close()

    kinds = sorted(r.outcome_kind for r in runs)
    assert kinds == ["emit", "interrupted"]
    interrupted = next(r for r in runs if r.outcome_kind == "interrupted")
    merged = next(r for r in runs if r.outcome_kind == "emit")
    assert interrupted.runner_id == "runner-test-01"
    # The interrupted batch went back to pending and ran again together with the new event.
    assert set(merged.event_ids) == {first_id, second_id}
    assert {e.event_id: e.status for e in inbox.all_events()} == {first_id: "done", second_id: "done"}
    # Only the merged Run delivered anything.
    assert [m.content for m in outbox.fetch_pending()] == ["merged from runner"]
