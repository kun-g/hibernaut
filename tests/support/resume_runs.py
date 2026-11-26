import asyncio
import json
import os
import tempfile
import time
import unittest
from collections.abc import Sequence
from pathlib import Path
from unittest.mock import Mock

from tests.support.fakes import InMemoryPermission
from tests.support.coder_reviewer import git
from tests.support.peer_handoff import FakeClaude, Incoming, PeerCase, result
from tests.support.sde_executor import make_base_repo
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.reviewer import CoderReviewerExecutor
from agent_runtime.adapters.executor_cli.run_state import STATE_FILE, read_state
from agent_runtime.adapters.executor_cli.sde import STATE_FORMAT, SDEExecutor, SDEOptions
from agent_runtime.adapters.executor_cli.worktree import OrcaWorktreeManager
from agent_runtime.adapters.storage_sqlite import (
    SQLiteDatabase,
    SQLiteInboxAdapter,
    SQLiteLeaseLockAdapter,
    SQLiteOpenRunStore,
    SQLiteOutboxAdapter,
    SQLiteSessionStoreAdapter,
    SQLiteTimerAdapter,
)
from agent_runtime.adapters.storage_sqlite.tasks import SQLiteTaskMetadataStore
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from agent_runtime.core.models import Emit, Event, RunOutcome, SessionRef
from agent_runtime.core.ports import RunCapabilities, RunContext
from agent_runtime.engine import AgentWorker


INBOX = "proj:x"


async def until(check, timeout: float = 45.0, what: str = "condition"):
    deadline = time.monotonic() + timeout
    while True:
        value = await asyncio.to_thread(check)
        if value:
            return value
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        await asyncio.sleep(0.02)


class ParkingExecutor:
    # Parks every Run on an asyncio.Event inside run.detachable(), as an executor waiting on an Orca terminal does.
    executor_id = "parker"

    def __init__(self, *, park: bool = True) -> None:
        self.release = asyncio.Event()
        self.parked = asyncio.Event()
        self.park = park
        self.calls: list[str] = []

    async def execute(self, events, session, caps) -> RunOutcome:
        raise AssertionError("a resumable executor gets execute_resumable when a registry is configured")

    async def execute_resumable(self, events: Sequence[Event], session: SessionRef | None, caps: RunCapabilities,
                                run: RunContext) -> RunOutcome:
        self.calls.append("start")
        Path(run.run_dir, STATE_FILE).write_text(json.dumps({"step": "waiting"}))
        if self.park:
            with run.detachable():
                self.parked.set()
                await self.release.wait()
        else:
            self.parked.set()
            await self.release.wait()
        return Emit(messages=(f"done by start ({len(events)} event(s))",))

    async def resume(self, events: Sequence[Event], session: SessionRef | None, caps: RunCapabilities,
                     run: RunContext) -> RunOutcome:
        self.calls.append("resume")
        state = json.loads(Path(run.run_dir, STATE_FILE).read_text())
        return Emit(messages=(f"resumed from {state['step']} ({len(events)} event(s))",))


class RuntimeCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.root = Path(os.path.realpath(self.enterContext(tempfile.TemporaryDirectory())))
        self.db = SQLiteDatabase(self.root / "hb.db")
        self.addCleanup(self.db.close)
        self.inbox = SQLiteInboxAdapter(self.db)
        self.outbox = SQLiteOutboxAdapter(self.db)
        self.sessions = SQLiteSessionStoreAdapter(self.db)
        self.runs = SQLiteOpenRunStore(self.db, self.root / "runs")

    def worker(self, name: str, executors: dict) -> AgentWorker:
        memory = Mock()
        memory.load_baseline.return_value = {}
        return AgentWorker(worker_id=name, inbox=self.inbox, timers=SQLiteTimerAdapter(self.db), memory=memory,
                           permissions=InMemoryPermission(), outbox=self.outbox, lock=SQLiteLeaseLockAdapter(self.db),
                           sessions=self.sessions, executors=executors, global_default_executor=next(iter(executors)),
                           open_runs=self.runs, code_version="9.9.9")

    def append(self, text: str = "task", executor_id: str = "parker") -> Event:
        stored, _ = self.inbox.append(Event(inbox_id=INBOX, source="test", external_id=f"e-{time.monotonic_ns()}",
                                            payload={"text": text, "executor_id": executor_id},
                                            correlation_key="task:1"))
        return stored

    def status(self, event: Event) -> str:
        return self.db.fetchone("SELECT status FROM event_inbox WHERE event_id = ?", (event.event_id,))["status"]

    def run_dirs(self) -> list[Path]:
        root = self.root / "runs"
        return [p for p in root.glob("*/*")] if root.exists() else []

    def messages(self) -> list[str]:
        return [m.content for m in self.outbox.fetch_pending(limit=50)]


def committed(repo: Path, text: str):
    def step(session: FakeClaude, msg: Incoming) -> None:
        (repo / "calc.py").write_text(text)
        git(repo, "commit", "-qam", "divide rejects zero")
        result()(session, msg)
    return step


def verdict_pass(session: FakeClaude, msg: Incoming) -> None:
    session.reply(msg, json.dumps({"task_id": msg.header["task_id"], "attempt_id": msg.header["attempt_id"],
                                   "verdict": "pass", "summary": "zero is rejected", "blocking": []}))


class SDERestartCase(PeerCase):
    # What each process hands its SDE on top of the rest.
    sde_extra: dict = {}

    def setUp(self):
        super().setUp()
        self.repo = Path(os.path.realpath(make_base_repo(self.root)))
        git(self.repo, "checkout", "-qb", "hb/task-1")
        self.runs_root = self.root / "runs"

    def process(self, db: SQLiteDatabase, name: str) -> tuple[AgentWorker, SQLiteOpenRunStore]:
        runner = self.make_runner(db)
        sessions = SQLiteSessionStoreAdapter(db)
        workspaces = SQLiteWorkspaceStore(db)
        launcher = ClaudeCodeLauncher(name="fake", command="claude")
        reviewer = CoderReviewerExecutor(profile=ClaudeCodeProfile(name="code-reviewer", launcher=launcher,
                                                                   timeout_seconds=30),
                                         workspaces=workspaces, tasks=SQLiteTaskMetadataStore(db), terminal=runner)
        sde = SDEExecutor(profile=ClaudeCodeProfile(name="sde", launcher=launcher, timeout_seconds=30),
                          options=SDEOptions(), reviewer=reviewer,
                          worktree_manager=OrcaWorktreeManager(workspace_store=workspaces, session_store=sessions,
                                                               backend="git"),
                          inbox_checkout_map={}, tasks=SQLiteTaskMetadataStore(db), terminal=runner, **self.sde_extra)
        memory = Mock()
        memory.load_baseline.return_value = {}
        runs = SQLiteOpenRunStore(db, self.runs_root)
        worker = AgentWorker(worker_id=name, inbox=SQLiteInboxAdapter(db), timers=SQLiteTimerAdapter(db),
                             memory=memory, permissions=InMemoryPermission(), outbox=SQLiteOutboxAdapter(db),
                             lock=SQLiteLeaseLockAdapter(db), sessions=sessions, executors={"sde": sde},
                             global_default_executor="sde", open_runs=runs)
        return worker, runs

    def restarted(self) -> tuple[AgentWorker, SQLiteOpenRunStore, SQLiteDatabase]:
        db = SQLiteDatabase(self.db_path)
        self.addCleanup(db.close)
        worker, runs = self.process(db, "new")
        return worker, runs, db

    def submit(self) -> Event:
        stored, _ = SQLiteInboxAdapter(self.db).append(Event(
            inbox_id=INBOX, source="test", external_id="task-1", correlation_key="task:1",
            payload={"executor_id": "sde", "text": "divide must reject a zero divisor",
                     "explicit_workdir": str(self.repo), "base_ref": "main"}))
        return stored

    def saved_state(self, runs: SQLiteOpenRunStore) -> dict | None:
        open_runs = runs.list_open(INBOX)
        if not open_runs:
            return None
        data, _ = read_state(open_runs[0].run_dir, STATE_FORMAT)
        return data

    async def stop_when(self, worker: AgentWorker, runs: SQLiteOpenRunStore, phase: str) -> None:
        running = asyncio.create_task(worker.process_inbox(INBOX))
        def sent_handle():
            state = self.saved_state(runs)
            if state and state["phase"] == phase and state["turn"] and state["turn"]["sent"]:
                return state["turn"]["handle"]
            return None

        handle = await until(sent_handle, what=f"the {phase} turn")
        peer = self.orca.terminals[handle]["session"]
        await asyncio.to_thread(peer.wait_received, 1)
        worker.detach()
        self.assertEqual(await asyncio.wait_for(running, 5), [])

    def outbox(self, db: SQLiteDatabase) -> list[str]:
        return [m.content for m in SQLiteOutboxAdapter(db).fetch_pending(limit=50)]

    def event_status(self, db: SQLiteDatabase, evt: Event) -> str:
        return db.fetchone("SELECT status FROM event_inbox WHERE event_id = ?", (evt.event_id,))["status"]
