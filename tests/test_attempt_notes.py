from __future__ import annotations

import asyncio
import contextlib
import json
import sqlite3
import threading
import unittest
from collections.abc import Callable, Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest

from agent_runtime.adapters.channels.gdrive_tasks import DriveTaskPoller, parse_task_markdown
from agent_runtime.adapters.channels.switchboard_bridge import _LIFTED_METADATA_KEYS
from agent_runtime.adapters.executor_cli.attempt_notes import (
    FAILED,
    MERGED,
    NO_CHANGE,
    STOPPED,
    UNMERGED,
    AttemptNotes,
    AttemptResult,
    MergedPR,
    note_path,
)
from agent_runtime.adapters.delivery import MERGE_TOOL
from agent_runtime.adapters.executor_cli import CommandResult
from agent_runtime.adapters.executor_cli.sde import SDEExecutor
from agent_runtime.adapters.memory_fs import GovernedMemoryHub, PartitionedMemoryAdapter, WorkspacePartitionRegistry
from agent_runtime.adapters.storage_sqlite import (
    SQLiteAttemptStore,
    SQLiteDatabase,
    SQLiteInboxAdapter,
    SQLiteLeaseLockAdapter,
    SQLiteOutboxAdapter,
    SQLiteSessionStoreAdapter,
    SQLiteTimerAdapter,
)
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config
from agent_runtime.core.models import Emit, Event, PermissionDecision, RunDetached, SessionRef, Suspend
from agent_runtime.engine.attempts import ATTEMPT_NOTES_KEY, attempt_goal, check_goal
from agent_runtime.engine.handler import EventHandler
from agent_runtime.engine.worker import AgentWorker
from memory_hub import MemoryHubConfig
from tests.fakes import InMemoryPermission
from tests.test_gdrive_tasks import _FakeDriveClient
from tests.test_pr_delivery import git
from tests.test_reclaim_sweep import SweepCase
from tests.test_resume_runs import INBOX, SDERestartCase, committed, verdict_pass
from tests.test_sde_delivery import SDEDeliveryCase
from tests.test_sde_delivery import dev as delivered_dev
from tests.test_sde_executor import FIXED, PASS, dev, event
from tests.test_sde_memory import ALPHA, BETA, MemoryCase, ungranted

GOAL = "test-speed"
NOTE = f"notes/attempts/{GOAL}.md"


class Notes:
    # The records of one process over the memory of two projects.
    def __init__(self, root: Path, *, max_note_bytes: int = 16384) -> None:
        self.hub = GovernedMemoryHub(MemoryHubConfig(memory_root=str(root / "memory"), max_note_bytes=max_note_bytes))
        self.registry = WorkspacePartitionRegistry(self.hub.root, hub=self.hub)
        self.db = SQLiteDatabase(root / "notes.db")
        self.store = SQLiteAttemptStore(self.db)
        self.notes = AttemptNotes(store=self.store, memory_for=self.registry.memory_for)

    def begin(self, session: str = "s1", *, goal: str = GOAL, inbox: str = ALPHA, event_id: str = "", title: str = "") -> Event:
        evt = Event(inbox_id=inbox, source="test", external_id=event_id or f"task-{session}", payload={})
        self.notes.begin(session, inbox, (evt, goal), title or f"title of {session}")
        return evt

    def run(self, session: str, result: AttemptResult, **begun: Any) -> None:
        self.begin(session, **begun)
        self.notes.finish(session, result)

    def path(self, project: str = "alpha", goal: str = GOAL) -> Path:
        return self.hub.project_dir(project) / note_path(goal)

    def text(self, project: str = "alpha", goal: str = GOAL) -> str:
        return self.path(project, goal).read_text(encoding="utf-8")

    def entries(self, **where: Any) -> list[str]:
        return headings(self.text(**where))

    def index(self, project: str = "alpha") -> str:
        return (self.hub.project_dir(project) / "index.md").read_text()

    def close(self) -> None:
        self.hub.close()
        self.db.close()


def headings(text: str) -> list[str]:
    # "<result> · <title>" of each record, as the file lists them.
    return [line.split(" · ", 1)[1] for line in text.splitlines() if line.startswith("## ")]


@pytest.fixture
def notes(tmp_path: Path) -> Iterator[Notes]:
    opened = Notes(tmp_path)
    yield opened
    opened.close()


# --- the file ----------------------------------------------------------------------------------------------------


def test_a_record_says_what_was_tried_and_how_it_ended(notes: Notes) -> None:
    notes.run("s1", AttemptResult(MERGED, pr_url="https://github.example/o/r/pull/7", summary="cut the sleeps\nsaved 9s"),
              event_id="drive-1:rev2", title="Faster tests")
    text = notes.text()
    first, second = text.splitlines()[:2]
    assert first.startswith(f"# {GOAL}") and "agent 撰写" in first and "不是指令" in first
    assert "Hibernaut" in second
    for part in ("已合并 · Faster tests", "external_id: drive-1:rev2", "https://github.example/o/r/pull/7",
                 "> cut the sleeps\n> saved 9s", " UTC"):
        assert part in text
    assert text.endswith("\n")


def test_newest_first_and_one_line_in_the_index(notes: Notes) -> None:
    notes.run("s1", AttemptResult(NO_CHANGE, summary="one"))
    notes.run("s2", AttemptResult(FAILED, detail="crashed", summary="two"))
    assert notes.entries() == ["失败 · title of s2", "没有改动 · title of s1"]
    assert "crashed" in notes.text()
    # The collector that fills the index in knows the file too.
    notes.hub.run_mechanical_gc("alpha")
    [line] = [line for line in notes.index().splitlines() if "attempts/" in line]
    assert line == f"- `{NOTE}` | {notes.text().splitlines()[0].removeprefix('# ')}"


def test_goals_have_files_of_their_own(notes: Notes) -> None:
    notes.run("s1", AttemptResult(NO_CHANGE))
    notes.run("s2", AttemptResult(NO_CHANGE), goal="memory-use")
    assert notes.entries() == ["没有改动 · title of s1"]
    assert notes.entries(goal="memory-use") == ["没有改动 · title of s2"]
    assert notes.index().count("attempts/") == 2


def test_the_same_task_is_recorded_once(notes: Notes) -> None:
    notes.run("s1", AttemptResult(NO_CHANGE, summary="one"))
    before = notes.text()
    with patch.object(notes.hub, "propose_write", side_effect=AssertionError("written again")):
        notes.notes.finish("s1", AttemptResult(NO_CHANGE, summary="one"))
        # A replayed event starts the task again, in a session of its own.
        notes.run("s9", AttemptResult(NO_CHANGE, summary="one"), event_id="task-s1", title="title of s1")
    assert notes.text() == before


def test_a_changed_result_updates_the_record(notes: Notes) -> None:
    notes.run("s1", AttemptResult(UNMERGED, detail="PR 未合并", pr_url="https://x/pull/1", summary="one"))
    notes.run("s2", AttemptResult(NO_CHANGE, summary="two"))
    # A follow-up Run of the same task names no goal of its own.
    assert notes.notes.begin("s1", ALPHA, None, "and merge it") == GOAL
    notes.notes.finish("s1", AttemptResult(MERGED, pr_url="https://x/pull/1", summary="one and more"))
    assert notes.entries() == ["已合并 · title of s1", "没有改动 · title of s2"]
    assert "one and more" in notes.text() and "> one\n" not in notes.text()
    # Merged is final: what the same task does afterwards does not take it back.
    notes.notes.finish("s1", AttemptResult(FAILED, detail="crashed"))
    assert notes.entries()[0] == "已合并 · title of s1"


def test_a_new_task_in_the_same_session_is_another_record(notes: Notes) -> None:
    notes.run("s1", AttemptResult(NO_CHANGE), event_id="first", title="first")
    notes.run("s1", AttemptResult(NO_CHANGE), event_id="second", title="second")
    assert notes.entries() == ["没有改动 · second", "没有改动 · first"]


def test_without_a_goal_nothing_is_recorded(notes: Notes) -> None:
    assert notes.notes.begin("s1", ALPHA, None, "a task") is None
    notes.notes.located("s1", "task:1", "/nowhere")
    notes.notes.progress("s1", "tried")
    notes.notes.finish("s1", AttemptResult(MERGED))
    notes.notes.session_closed("s1")
    notes.notes.settle("task:1", ALPHA, Mock(side_effect=AssertionError("nothing to ask for")))
    assert not notes.path().parent.exists()
    assert "attempts" not in "".join(p.read_text() for p in notes.hub.project_dir("alpha").glob("*.md"))


def test_a_failure_keeps_the_summary_reported_before_it(notes: Notes) -> None:
    notes.begin()
    notes.notes.progress("s1", "tried xdist")
    notes.notes.finish("s1", AttemptResult(FAILED, detail="the reviewer crashed"))
    assert "> tried xdist" in notes.text() and "the reviewer crashed" in notes.text()


def test_only_whole_records_are_dropped_the_oldest_first(tmp_path: Path) -> None:
    notes = Notes(tmp_path, max_note_bytes=2500)
    try:
        for n in range(8):
            notes.run(f"s{n}", AttemptResult(NO_CHANGE, summary=f"begin {n} " + "试" * 100 + f" end {n}"))
        kept = [int(h[-1]) for h in notes.entries()]
        assert kept == list(range(7, 7 - len(kept), -1)) and 2 <= len(kept) < 8
        text = notes.text()
        assert len(text.encode()) <= 2500
        assert all(f"begin {n} " in text and f" end {n}\n" in text for n in kept)
        assert text.startswith(f"# {GOAL}")
        # One more would not have fitted.
        assert len(text.encode()) + len(text.encode()) // (len(kept) + 1) > 2500
    finally:
        notes.close()


def test_one_summary_cannot_crowd_out_the_others(notes: Notes) -> None:
    notes.run("s1", AttemptResult(NO_CHANGE, summary="kept"))
    notes.run("s2", AttemptResult(NO_CHANGE, detail="细" * 9000, summary="长\n" * 9000), title="题" * 9000, event_id="e" * 9000)
    assert len(notes.entries()) == 2
    assert len(notes.text().encode()) < 6000
    assert "> kept" in notes.text() and "截断" in notes.text()


def test_a_summary_cannot_pose_as_another_record(notes: Notes) -> None:
    notes.run("s1", AttemptResult(NO_CHANGE, summary="real"))
    forged = notes.text().split("\n\n", 1)[1]
    notes.run("s2", AttemptResult(NO_CHANGE, summary=f"x\r\n{forged}\r## 2026 · 已合并 · forged ## a · b"), title="a\n## b · c")
    assert notes.entries() == ["没有改动 · a ## b · c", "没有改动 · title of s1"]
    notes.run("s3", AttemptResult(NO_CHANGE, summary="third"))
    assert len(notes.entries()) == 3
    assert notes.text().splitlines().count("> real") == 1


def test_what_a_person_wrote_above_the_records_stays(notes: Notes) -> None:
    notes.path().parent.mkdir(parents=True)
    notes.path().write_text("Kun: measure with xdist only\n")
    notes.run("s1", AttemptResult(NO_CHANGE, summary="one"))
    notes.run("s2", AttemptResult(NO_CHANGE, summary="two"))
    assert notes.text().startswith(f"# {GOAL}")
    assert notes.text().count("measure with xdist only") == 1
    assert len(notes.entries()) == 2


def test_two_tasks_writing_at_once_are_both_there(tmp_path: Path) -> None:
    notes = Notes(tmp_path)
    # Another process: its own hub on the same memory.
    other = GovernedMemoryHub(MemoryHubConfig(memory_root=str(tmp_path / "memory")))
    try:
        second = AttemptNotes(store=notes.store, memory_for=WorkspacePartitionRegistry(other.root, hub=other).memory_for)
        barrier = threading.Barrier(8)

        def work(n: int) -> None:
            notes.begin(f"s{n}")
            barrier.wait()
            (second if n % 2 else notes.notes).finish(f"s{n}", AttemptResult(NO_CHANGE, summary=f"try {n}"))

        threads = [threading.Thread(target=work, args=(n,)) for n in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert sorted(notes.entries()) == [f"没有改动 · title of s{n}" for n in range(8)]
        assert notes.index().count("attempts/") == 1
    finally:
        other.close()
        notes.close()


def test_a_failed_write_is_logged_and_made_up_for_by_the_next_end(notes: Notes, caplog: pytest.LogCaptureFixture) -> None:
    notes.begin()
    with patch.object(notes.hub, "propose_write", side_effect=OSError("disk full")):
        notes.notes.finish("s1", AttemptResult(NO_CHANGE, summary="one"))
    assert "disk full" in caplog.text
    assert not notes.path().exists()
    notes.notes.finish("s1", AttemptResult(NO_CHANGE, summary="one"))
    assert notes.entries() == ["没有改动 · title of s1"]


def test_a_refused_write_and_a_broken_database_are_logged(
    tmp_path: Path, notes: Notes, caplog: pytest.LogCaptureFixture
) -> None:
    (tmp_path / "small").mkdir()
    small = Notes(tmp_path / "small", max_note_bytes=120)
    try:
        small.run("s1", AttemptResult(NO_CHANGE))
        assert not small.path().exists()
    finally:
        small.close()
    assert "max_note_bytes" in caplog.text
    caplog.clear()
    broken = Mock(spec=SQLiteAttemptStore)
    for name in ("begin", "latest", "by_work_key"):
        getattr(broken, name).side_effect = sqlite3.OperationalError("database is locked")
    attempts = AttemptNotes(store=broken, memory_for=notes.registry.memory_for)
    evt = Event(inbox_id=ALPHA, source="test", external_id="e", payload={})
    assert attempts.begin("s1", ALPHA, (evt, GOAL), "t") == GOAL
    attempts.located("s1", "task:1", "/nowhere")
    attempts.progress("s1", "x")
    attempts.delivered("s1", "https://x/pull/1", "0" * 40)
    attempts.finish("s1", AttemptResult(NO_CHANGE))
    attempts.session_closed("s1")
    attempts.settle("task:1", ALPHA, list)
    assert caplog.text.count("database is locked") >= 7


def test_a_task_writes_its_own_projects_memory_and_no_other(notes: Notes) -> None:
    notes.run("s1", AttemptResult(NO_CHANGE))
    notes.run("s2", AttemptResult(NO_CHANGE), inbox=BETA)
    assert notes.entries() == ["没有改动 · title of s1"]
    assert notes.entries(project="beta") == ["没有改动 · title of s2"]
    alpha = notes.registry.memory_for(ALPHA)
    for path in ("proj://beta/notes/attempts/x.md", "../beta/notes/attempts/x.md", "shared://notes/x.md"):
        with pytest.raises((PermissionError, ValueError)):
            alpha.rewrite_note(path, lambda old, limit: "x\n", rationale="x")
    # Only notes: the files a person approves keep their approval.
    for path in ("project_card.md", "index.md", "instructions.md"):
        with pytest.raises((PermissionError, ValueError)):
            alpha.rewrite_note(path, lambda old, limit: "x\n", rationale="x")
    assert not (notes.hub.project_dir("beta") / "notes/attempts/x.md").exists()
    # The same session id under another inbox is still the project its event came in for.
    notes.run("s1", AttemptResult(FAILED), inbox=BETA, event_id="other")
    assert notes.entries() == ["没有改动 · title of s1"]


# --- two processes at once ---------------------------------------------------------------------------------------


PR = "https://x/pull/3"


class Race:
    # A sweep of another process, with its own connection and hub, lands between two steps of this one's writes.
    def __init__(self, root: Path, notes: Notes) -> None:
        self.notes = notes
        self.elsewhere = Notes(root)

    def unmerged(self) -> None:
        # Ended with its PR open, and at work again (a follow-up).
        self.notes.begin()
        self.notes.notes.located("s1", "task:9", "/nowhere")
        self.notes.notes.progress("s1", "one")
        self.notes.notes.finish("s1", AttemptResult(UNMERGED, detail="open", pr_url=PR, summary="one"))

    def merge(self) -> None:
        self.elsewhere.notes.settle("task:9", ALPHA, lambda: [MergedPR(url=PR, head="")])

    def after_read(self, meanwhile: Callable[[], None], work: Callable[[], None]) -> None:
        read = self.notes.store.latest

        def latest(session_id: str) -> Any:
            row = read(session_id)
            meanwhile()
            return row

        with patch.object(self.notes.store, "latest", latest):
            work()

    def row(self) -> Any:
        return self.elsewhere.db.fetchone("SELECT * FROM attempt_notes")

    def assert_merged(self) -> None:
        row = self.row()
        assert (row["result"], row["pr_url"], row["written"]) == (MERGED, PR, 1)
        assert self.notes.entries() == ["已合并 · title of s1"]


@pytest.fixture
def race(tmp_path: Path, notes: Notes) -> Iterator[Race]:
    opened = Race(tmp_path, notes)
    yield opened
    opened.elsewhere.close()


WRITERS: dict[str, Callable[[AttemptNotes], None]] = {
    "located": lambda n: n.located("s1", "task:9", "/elsewhere"),
    "progress": lambda n: n.progress("s1", "two"),
    "delivered": lambda n: n.delivered("s1", "https://x/pull/4", "c" * 40),
    "unmerged": lambda n: n.finish("s1", AttemptResult(UNMERGED, detail="still open", pr_url=PR, summary="two")),
    "failed": lambda n: n.finish("s1", AttemptResult(FAILED, detail="crashed")),
    "no change": lambda n: n.finish("s1", AttemptResult(NO_CHANGE, summary="two")),
    "closed": lambda n: n.session_closed("s1"),
}


@pytest.mark.parametrize("writer", WRITERS)
def test_a_merge_recorded_after_the_row_was_read_stays(race: Race, writer: str) -> None:
    race.unmerged()
    with race.notes.db.transaction() as conn:
        # Its own commit, so that the second PR counts as its own too.
        conn.execute("UPDATE attempt_notes SET dev_head = ?", ("c" * 40,))
    race.after_read(race.merge, lambda: WRITERS[writer](race.notes.notes))
    race.assert_merged()
    # What ends later knows it too.
    race.notes.notes.finish("s1", AttemptResult(UNMERGED, summary="three"))
    race.assert_merged()


@pytest.mark.parametrize("late", [False, True])
def test_a_merge_recorded_while_an_older_result_is_on_its_way_into_the_file_stays(race: Race, late: bool) -> None:
    race.unmerged()
    rewrite, held = PartitionedMemoryAdapter.rewrite_note, [True]

    def slow(adapter: Any, *args: Any, **options: Any) -> None:
        if held.pop() if held else False:
            if late:
                rewrite(adapter, *args, **options)
            race.merge()
            if late:
                return
        rewrite(adapter, *args, **options)

    with patch.object(PartitionedMemoryAdapter, "rewrite_note", slow):
        race.notes.notes.finish("s1", AttemptResult(UNMERGED, detail="still open", pr_url=PR, summary="two"))
    assert not held
    race.assert_merged()


@pytest.mark.parametrize("merge_first", [False, True])
@pytest.mark.parametrize("writer", ["progress", "delivered", "unmerged"])
def test_two_updates_of_one_row_never_overlap(race: Race, writer: str, merge_first: bool) -> None:
    race.unmerged()
    with race.notes.db.transaction() as conn:
        conn.execute("UPDATE attempt_notes SET dev_head = ?", ("c" * 40,))

    def merge(notes: AttemptNotes) -> None:
        notes.settle("task:9", ALPHA, lambda: [MergedPR(url=PR, head="c" * 40)])

    first, second = (merge, WRITERS[writer]) if merge_first else (WRITERS[writer], merge)
    ours, theirs = race.notes.store.update, race.elsewhere.store.update
    begun, inside, overlapped = threading.Event(), threading.Event(), list[bool]()
    other = threading.Thread(target=lambda: second(race.elsewhere.notes))

    def entered(attempt: str, change: Any) -> Any:
        def noted(row: Any) -> Any:
            inside.set()
            return change(row)
        return theirs(attempt, noted)

    def held(attempt: str, change: Any) -> Any:
        def slow(row: Any) -> Any:
            # The row is read and not yet written: the other connection starts its own update now.
            other.start()
            assert begun.wait(5)
            overlapped.append(inside.wait(0.05))
            return change(row)
        with patch.object(race.notes.store, "update", ours):
            return ours(attempt, slow)

    race.elsewhere.db._conn.set_trace_callback(lambda sql: begun.set() if sql.startswith("BEGIN") else None)
    with patch.object(race.elsewhere.store, "update", entered), patch.object(race.notes.store, "update", held):
        first(race.notes.notes)
        other.join(10)
    assert not other.is_alive()
    assert overlapped == [False]
    # Progress and a PR change the row of an unmerged attempt without writing its record.
    race.notes.notes.finish("s1", AttemptResult(UNMERGED, summary="three"))
    row = race.row()
    assert (row["result"], row["written"]) == (MERGED, 1)
    assert race.notes.entries() == ["已合并 · title of s1"]


@pytest.mark.parametrize("failing", [False, True])
@pytest.mark.parametrize("marked_last", [False, True])
def test_a_merge_recorded_between_reading_the_row_and_writing_the_file_stays(
    race: Race, marked_last: bool, failing: bool
) -> None:
    race.unmerged()
    write, mark, update = race.notes.hub.propose_write, race.notes.store.mark_written, race.elsewhere.store.update
    committed = threading.Event()
    sweep = threading.Thread(target=race.merge)

    def merging(attempt: str, change: Any) -> Any:
        row = update(attempt, change)
        committed.set()
        return row

    def propose(proposal: Any) -> Any:
        # Under the lock, with the row read: the sweep commits its merge and then waits for the lock to write it.
        sweep.start()
        assert committed.wait(5)
        assert "没合并" in proposal.content
        return write(proposal)

    def marking(row: Any) -> None:
        if marked_last:
            sweep.join(10)
        mark(row)

    broken: Any = patch.object(race.elsewhere.hub, "propose_write", side_effect=OSError("disk full")) if failing \
        else contextlib.nullcontext()
    with broken, patch.object(race.elsewhere.store, "update", merging), \
            patch.object(race.notes.hub, "propose_write", propose), patch.object(race.notes.store, "mark_written", marking):
        race.notes.notes.finish("s1", AttemptResult(UNMERGED, detail="still open", pr_url=PR, summary="two"))
        sweep.join(10)
    assert not sweep.is_alive() and committed.is_set()
    if failing:
        # What the file does not say yet must not count as written, or no end would make up for it.
        assert (race.row()["result"], race.row()["written"]) == (MERGED, 0)
    else:
        race.assert_merged()
    for ended in (AttemptResult(UNMERGED, pr_url=PR, summary="three"), AttemptResult(FAILED, detail="crashed")):
        race.notes.notes.finish("s1", ended)
        race.assert_merged()


def test_a_sweep_that_asked_before_the_attempt_went_on_takes_nothing_back(race: Race) -> None:
    race.unmerged()

    def merged() -> list[MergedPR]:
        race.notes.notes.finish("s1", AttemptResult(MERGED, pr_url="https://x/pull/4", summary="two"))
        return [MergedPR(url=PR, head="")]

    race.elsewhere.notes.settle("task:9", ALPHA, merged)
    row = race.row()
    assert (row["result"], row["pr_url"], row["summary"]) == (MERGED, "https://x/pull/4", "two")
    assert "https://x/pull/4" in race.notes.text() and "> two" in race.notes.text()


@pytest.mark.parametrize("writer", ["located", "progress"])
def test_what_says_the_same_stays_marked_as_written(race: Race, writer: str) -> None:
    race.unmerged()
    with race.notes.db.transaction() as conn:
        conn.execute("UPDATE attempt_notes SET written = 0")

    def written() -> None:
        race.elsewhere.notes.finish("s1", AttemptResult(UNMERGED, detail="open", pr_url=PR, summary="one"))
        assert race.row()["written"] == 1

    work = {**WRITERS, "progress": lambda n: n.progress("s1", "one")}[writer]
    race.after_read(written, lambda: work(race.notes.notes))
    assert race.row()["written"] == 1


# --- ends that no Run sees ---------------------------------------------------------------------------------------


def test_a_session_closed_before_any_result_is_stopped(notes: Notes) -> None:
    sessions = SQLiteSessionStoreAdapter(notes.db, on_closed=notes.notes.session_closed)
    for session in ("s1", "s2", "s3", "s4"):
        notes.begin(session)
        sessions.save(SessionRef(session_id=session, inbox_id=ALPHA, executor_id="sde", correlation_key=session))
    notes.notes.progress("s1", "half done")
    notes.notes.finish("s4", AttemptResult(UNMERGED, pr_url="https://x/pull/4"))
    for _ in range(2):
        sessions.close_session("s1")
        sessions.save(SessionRef(session_id="s2", inbox_id=ALPHA, executor_id="sde", status="closed"))
        sessions.close_session("s4")
    # Another session takes over the task's key: the first can never go on.
    sessions.save(SessionRef(session_id="s5", inbox_id=ALPHA, executor_id="sde", correlation_key="s3"))
    assert notes.entries() == ["停止 · title of s3", "停止 · title of s2", "停止 · title of s1", "没合并 · title of s4"]
    assert "> half done" in notes.text()


class OwnMergeCase(SweepCase):
    # Attempts of one task (work_key task:9) in its worktree, one after the other.
    def setUp(self) -> None:
        super().setUp()
        self.notes = Notes(self.root)
        self.addCleanup(self.notes.close)
        self.workspaces.claim_owner("task:9", ALPHA)
        plain = self.manager
        self.manager = lambda usage=None: self.settling(plain(usage))
        self.asked = 0

    def settling(self, manager: Any) -> Any:
        manager._settle = self.notes.notes.settle
        return manager

    def runner(self, cmd: Any, cwd: Any) -> Any:
        self.asked += cmd[0] == "gh"
        return super().runner(cmd, cwd)

    def attempt(self, session: str, result: AttemptResult | None, work: Any = None, inbox: str = ALPHA) -> Any:
        self.notes.begin(session, inbox=inbox)
        self.notes.notes.located(session, "task:9", str(self.wt))
        done = work() if work is not None else None
        self.notes.notes.progress(session, f"summary of {session}")
        if result is not None:
            # A PR is always at a pushed head: what lies there, unless the delivery names another one.
            pushed = result.head or (git(self.wt, "rev-parse", "HEAD") if result.pr_url else "")
            self.notes.notes.finish(session, replace(result, head=pushed))
        return done

    def merged(self, head: str, merge: str = "", url: str = "https://x/pull/3") -> None:
        self.gh["merged"] = CommandResult(0, json.dumps([
            {"number": 3, "headRefOid": head, "mergeCommit": {"oid": merge}, "url": url}]), "")

    def settle(self) -> None:
        record = self.workspaces.get("task:9")
        assert record is not None
        self.manager().settle(record)

    def entries(self, project: str = "alpha") -> list[str]:
        return self.notes.entries(project=project)


class TestOnlyItsOwnMergeCounts(OwnMergeCase, unittest.TestCase):
    def test_a_pr_merged_elsewhere_is_recorded_while_the_session_still_holds_the_worktree(self) -> None:
        head, merge = self.attempt("s1", AttemptResult(UNMERGED), self.squashed_task)
        self.sessions.save(SessionRef(session_id="s1", inbox_id=ALPHA, executor_id="sde", work_key="task:9"))
        self.sweeper().sweep_once()
        self.assertEqual(self.entries(), ["没合并 · title of s1"])
        self.merged(head, merge)
        [outcome] = self.sweeper().sweep_once()
        self.assertFalse(outcome.reclaimed)
        self.assertEqual(self.entries(), ["已合并 · title of s1"])
        self.assertIn("https://x/pull/3", self.notes.text())

    def test_a_merge_from_before_is_not_that_of_a_later_attempt_without_commits(self) -> None:
        head, merge = self.attempt("s1", AttemptResult(UNMERGED, pr_url="https://x/pull/3"), self.squashed_task)
        self.merged(head, merge)
        self.settle()
        self.attempt("s2", AttemptResult(FAILED, detail="crashed"))
        self.attempt("s3", AttemptResult(STOPPED, detail="question declined"))
        self.attempt("s4", None)
        self.notes.notes.session_closed("s4")
        expected = ["停止 · title of s4", "停止 · title of s3", "失败 · title of s2", "已合并 · title of s1"]
        self.assertEqual(self.entries(), expected)
        asked = self.asked
        self.settle()
        # Attempts that committed nothing have no merge to wait for.
        self.assertEqual(self.asked, asked)
        self.sweeper().sweep_once()
        self.assertTrue(self.workspaces.get("task:9") is None)
        self.assertEqual(self.entries(), expected)
        self.assertEqual(self.notes.text().count("https://x/pull/3"), 1)

    def test_a_merge_of_earlier_work_after_the_attempt_is_not_its_own_either(self) -> None:
        head, merge = self.attempt("s1", AttemptResult(UNMERGED, pr_url="https://x/pull/3"), self.squashed_task)
        self.attempt("s2", AttemptResult(FAILED))
        self.merged(head, merge)
        self.settle()
        self.assertEqual(self.entries(), ["已合并 · title of s1", "失败 · title of s2"])

    def test_commits_made_after_the_merged_head_are_not_merged(self) -> None:
        head, merge = self.attempt("s1", AttemptResult(UNMERGED), self.squashed_task)
        later = self.attempt("s2", AttemptResult(UNMERGED), lambda: self.commit("y = 2\n"))
        self.merged(head, merge)
        self.settle()
        self.assertEqual(self.entries(), ["已合并 · title of s1", "没合并 · title of s2"])
        # A PR of more work contains it.
        self.merged(later, url="https://x/pull/4")
        self.settle()
        self.assertEqual(self.entries()[0], "已合并 · title of s2")
        self.assertIn("https://x/pull/4", self.notes.text())

    def test_a_failed_or_closed_attempt_with_commits_follows_its_merge(self) -> None:
        head = self.attempt("s1", AttemptResult(FAILED), lambda: self.commit("x = 1\n"))
        self.merged(head)
        self.settle()
        self.notes.begin("s2")
        self.notes.notes.located("s2", "task:9", str(self.wt))
        head = self.commit("x = 2\n")
        self.notes.notes.session_closed("s2")
        self.assertEqual(self.entries(), ["停止 · title of s2", "已合并 · title of s1"])
        self.merged(head)
        self.settle()
        self.assertEqual(self.entries(), ["已合并 · title of s2", "已合并 · title of s1"])

    def test_a_later_attempts_merge_on_top_of_earlier_commits_is_not_the_earlier_ones(self) -> None:
        self.attempt("s1", AttemptResult(FAILED), lambda: self.commit("x = 1\n"))
        self.attempt("s2", AttemptResult(UNMERGED), lambda: self.commit("x = 2\n"))
        self.notes.begin("s3")
        self.notes.notes.located("s3", "task:9", str(self.wt))
        self.commit("x = 3\n")
        self.notes.notes.session_closed("s3")
        head = self.attempt("s4", AttemptResult(UNMERGED), lambda: self.commit("x = 4\n"))
        self.merged(head)
        self.settle()
        self.assertEqual(self.entries(), ["已合并 · title of s4", "停止 · title of s3", "没合并 · title of s2",
                                          "失败 · title of s1"])
        self.assertEqual(self.notes.text().count("https://x/pull/3"), 1)

    def test_commits_of_a_task_without_the_field_on_top_are_not_the_attempts_merge(self) -> None:
        self.attempt("s1", AttemptResult(FAILED), lambda: self.commit("x = 1\n"))
        self.merged(self.commit("x = 2\n"))
        self.settle()
        self.assertEqual(self.entries(), ["失败 · title of s1"])

    def test_a_replay_without_commits_is_not_given_the_merge_of_what_came_between(self) -> None:
        for between in ("s2", ""):
            with self.subTest(between or "a task without the field"):
                first = f"first{between}"
                self.attempt(first, AttemptResult(FAILED), lambda: self.commit(f"x = '{first}'\n"))
                later = self.commit(f"y = '{first}'\n")
                old = AttemptResult(MERGED, pr_url="https://x/pull/7", head=later)
                if between:
                    self.notes.begin(between)
                    self.notes.notes.located(between, "task:9", str(self.wt))
                    later = self.commit(f"z = '{first}'\n")
                    old = AttemptResult(MERGED, pr_url="https://x/pull/7", head=later)
                    self.notes.notes.progress(between, "more")
                    self.notes.notes.finish(between, old)
                self.attempt(first, old)
                self.assertEqual(self.entries()[0], f"没有改动 · title of {first}")

    def test_an_attempt_resumed_after_a_later_one_merged_is_not_given_that_merge(self) -> None:
        # Suspended meanwhile, so it does not begin again.
        self.attempt("s1", None, lambda: self.commit("x = 1\n"))
        head = self.attempt("s2", None, lambda: self.commit("x = 2\n"))
        merged = AttemptResult(MERGED, pr_url="https://x/pull/7", head=head)
        self.notes.notes.finish("s2", merged)
        self.notes.notes.finish("s1", merged)
        self.assertEqual(self.entries(), ["没合并 · title of s1", "已合并 · title of s2"])

    def test_a_task_closed_while_its_delivery_waits_follows_its_own_pr(self) -> None:
        self.attempt("s1", None, lambda: self.commit("x = 1\n"))
        # The PR's head has the delivery's own commit (the version) on top.
        self.notes.notes.delivered("s1", "https://x/pull/3", self.commit("version\n"))
        # A delivery that only knows the PR of other work leaves no trace.
        head = git(self.wt, "rev-parse", "HEAD")
        self.attempt("s2", None)
        self.notes.notes.delivered("s2", "https://x/pull/3", head)
        self.notes.notes.session_closed("s1")
        self.notes.notes.session_closed("s2")
        self.merged(head)
        self.settle()
        self.assertEqual(self.entries(), ["已合并 · title of s1", "停止 · title of s2"])

    def test_a_task_of_another_project_with_the_same_key_is_left_alone(self) -> None:
        head, merge = self.attempt("s1", AttemptResult(UNMERGED, pr_url="https://x/pull/3"), self.squashed_task, inbox=BETA)
        self.attempt("s2", AttemptResult(FAILED))
        self.merged(head, merge)
        self.settle()
        self.assertTrue(self.manager().reclaim("task:9").reclaimed)
        self.assertEqual(self.entries("beta"), ["没合并 · title of s1"])
        self.assertEqual(self.entries(), ["失败 · title of s2"])

    def test_a_workspace_of_nobody_settles_nothing(self) -> None:
        head, merge = self.attempt("s1", AttemptResult(UNMERGED), self.squashed_task)
        self.merged(head, merge)
        with self.db.transaction() as conn:
            conn.execute("UPDATE workspaces SET inbox_id = NULL")
        self.settle()
        self.assertEqual(self.entries(), ["没合并 · title of s1"])

    def test_a_reclaim_records_the_merge_first(self) -> None:
        head, merge = self.attempt("s1", AttemptResult(STOPPED), self.squashed_task)
        self.merged(head, merge)
        result = self.manager().reclaim("task:9")
        self.assertTrue(result.reclaimed, result.reason)
        self.assertEqual(self.entries(), ["已合并 · title of s1"])

    def test_a_merge_is_recorded_after_the_directory_went_too(self) -> None:
        head, merge = self.attempt("s1", AttemptResult(UNMERGED), self.squashed_task)
        self.merged(head, merge)
        git(self.primary, "worktree", "remove", "--force", str(self.wt))
        self.sweeper().sweep_once()
        self.assertEqual(self.entries(), ["已合并 · title of s1"])

    def test_what_a_delivery_says_counts_only_for_the_commits_it_covers(self) -> None:
        # The delivery record belongs to the work_key: it may still name the PR of an earlier attempt.
        head, _ = self.attempt("s1", AttemptResult(MERGED, pr_url="https://x/pull/3"), self.squashed_task)
        old = {"pr_url": "https://x/pull/3", "head": head}
        self.attempt("s2", AttemptResult(MERGED, **old))
        self.attempt("s3", AttemptResult(UNMERGED, **old))
        self.attempt("s4", AttemptResult(MERGED, **old), lambda: self.commit("y = 2\n"))
        self.attempt("s5", AttemptResult(UNMERGED, **old), lambda: self.commit("y = 3\n"))
        self.assertEqual(self.entries(), ["没合并 · title of s5", "没合并 · title of s4", "没有改动 · title of s3",
                                          "没有改动 · title of s2", "已合并 · title of s1"])
        self.merged(head)
        self.settle()
        self.assertEqual(self.entries()[:2], ["没合并 · title of s5", "没合并 · title of s4"])
        # The delivery's own commits on top (the version, the base) are still this attempt's delivery.
        self.attempt("s6", None, lambda: self.commit("y = 4\n"))
        self.notes.notes.finish("s6", AttemptResult(MERGED, pr_url="https://x/pull/6", head=self.commit("version\n")))
        self.assertEqual(self.entries()[0], "已合并 · title of s6")

    def test_a_follow_up_without_commits_keeps_the_attempts_own_work(self) -> None:
        self.attempt("s1", None, lambda: self.commit("y = 2\n"))
        # The delivery's own commit lies on top when the attempt ends, so it does not count as somebody else's work.
        head = self.commit("version\n")
        self.notes.notes.finish("s1", AttemptResult(UNMERGED, pr_url="https://x/pull/3", head=head))
        self.notes.notes.begin("s1", ALPHA, None, "again")
        self.notes.notes.located("s1", "task:9", str(self.wt))
        self.notes.notes.progress("s1", "nothing to add")
        self.notes.notes.finish("s1", AttemptResult(MERGED, pr_url="https://x/pull/3", head=head))
        self.assertEqual(self.entries(), ["已合并 · title of s1"])

    def test_a_run_started_over_before_its_end_keeps_its_commits(self) -> None:
        # A restart without a usable state begins the same event again, in a worktree that has its commits by then.
        self.notes.begin("s1")
        self.notes.notes.located("s1", "task:9", str(self.wt))
        head = self.commit("y = 2\n")
        self.attempt("s1", AttemptResult(MERGED, pr_url="https://x/pull/3", head=head))
        self.assertEqual(self.entries(), ["已合并 · title of s1"])


# --- goal names ---------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["a", "test-speed", "sde_memory2", "9", "x" * 64])
def test_usable_goal_names(name: str) -> None:
    assert check_goal(name) == name


@pytest.mark.parametrize("name", [
    "", " ", "a/b", "a\\b", "..", ".", "../x", "a.md", ".hidden", "-x", "_x", "x" * 65, "A", "测试", "a b", "a\n", "a\x00",
    " a", None, 3, True, ["a"], {"a": 1}, "ａ", "a%2Fb", "~", "c:x",
])
def test_unusable_goal_names_are_explained(name: Any) -> None:
    with pytest.raises(ValueError, match="attempt_notes"):
        check_goal(name)


def test_the_last_event_that_names_a_goal_counts() -> None:
    def evt(**payload: Any) -> Event:
        return Event(inbox_id=ALPHA, source="test", external_id=str(len(payload)), payload=payload)

    assert attempt_goal([evt(), evt(text="x")]) is None
    first, second = evt(attempt_notes="one"), evt(attempt_notes="two")
    assert attempt_goal([first, second, evt(text="x")]) == (second, "two")
    with pytest.raises(ValueError):
        attempt_goal([first, evt(attempt_notes="../x")])


def test_drive_frontmatter_knows_the_field() -> None:
    assert parse_task_markdown("---\nscope: proj:x\nattempt_notes: test-speed\n---\nbody")[0]["attempt_notes"] == GOAL


def test_drive_tasks_carry_the_goal_and_an_unusable_one_does_not_stop_the_poll() -> None:
    drive = _FakeDriveClient()
    drive.add_file("f1", "one.md", "---\nscope: proj:x\nattempt_notes: test-speed\n---\nbody")
    drive.add_file("f2", "two.md", "---\nscope: proj:x\nattempt_notes: ../x\n---\nbody")
    drive.add_file("f3", "three.md", "---\nscope: proj:x\nattempt_notes:\n---\nbody")
    drive.add_file("f4", "four.md", "---\nscope: proj:x\n---\nbody")
    db = SQLiteDatabase(":memory:")
    inbox = SQLiteInboxAdapter(db)
    DriveTaskPoller(client=drive, handler=EventHandler(inbox), root_folder_id="root-1").poll_once()
    found = {e.correlation_key: e.payload.get("attempt_notes", "absent") for e in inbox.fetch_pending("proj:x")}
    db.close()
    # The Run refuses the second and the third, and the task's file says why.
    assert found == {"gdrive:f1": GOAL, "gdrive:f2": "../x", "gdrive:f3": "", "gdrive:f4": "absent"}


def test_what_switchboard_relays_names_no_goal() -> None:
    assert ATTEMPT_NOTES_KEY not in _LIFTED_METADATA_KEYS


# --- the executor -------------------------------------------------------------------------------------------------


class SDENotesCase(MemoryCase):
    def setUp(self) -> None:
        super().setUp()
        self.notes: Any = AttemptNotes(store=SQLiteAttemptStore(self.world.db), memory_for=self.world.registry.memory_for)

    def sde(self, review: bool = False, terminal: Any = None, **options: Any) -> SDEExecutor:
        return super().sde(review, terminal, attempts=self.notes, **options)

    def task(self, **extra: Any) -> Event:
        return super().task(**{ATTEMPT_NOTES_KEY: GOAL, "title": "Faster tests", **extra})

    @property
    def note(self) -> Path:
        return self.world.hub.project_dir("alpha") / NOTE

    def text(self) -> str:
        return self.note.read_text(encoding="utf-8") if self.note.exists() else ""

    def recorded(self) -> list[str]:
        return headings(self.text())

    async def execute(self, *steps: Any, task: Event | None = None, session: SessionRef | None = None, **options: Any) -> Any:
        self.set_plan(sde=steps)
        return await self.sde(**options).execute([task or self.task()], session or self.session(), self.caps)


NO_COMMIT = {"write": {}, "commit": None, "output": {
    "status": "done", "summary": "xdist gains nothing", "test_evidence": "10s before, 10s after",
    "change_kind": "test_docs", "change_rationale": "nothing changed", "code_change": False}}


class TestEnds(SDENotesCase):
    async def test_no_change(self) -> None:
        task = self.task()
        await self.execute(NO_COMMIT, task=task)
        self.assertEqual(self.recorded(), ["没有改动 · Faster tests"])
        self.assertIn("> xdist gains nothing", self.text())
        self.assertIn(task.external_id, self.text())

    async def test_local_commits_are_not_merged(self) -> None:
        outcome = await self.execute(dev(FIXED))
        assert isinstance(outcome, Emit), outcome
        self.assertEqual(outcome.status, "done")
        self.assertEqual(self.recorded(), ["没合并 · Faster tests"])

    async def test_failure_and_its_replay(self) -> None:
        task = self.task()
        outcome = await self.execute({**dev(FIXED), "output": {"status": "nonsense"}}, task=task)
        self.assertEqual(outcome.status, "failed")
        self.assertEqual(self.recorded(), ["失败 · Faster tests"])
        self.assertIn("malformed", self.text())
        await self.execute(dev(FIXED), task=task)
        self.assertEqual(self.recorded(), ["没合并 · Faster tests"])

    async def test_a_task_that_never_started_developing_failed_too(self) -> None:
        outcome = await self.execute(task=self.task(explicit_workdir=str(self.root / "none")))
        self.assertEqual(outcome.status, "failed")
        self.assertEqual(self.recorded(), ["失败 · Faster tests"])

    async def test_an_error_the_executor_does_not_handle_failed_and_still_reaches_the_worker(self) -> None:
        executor = self.sde()
        with patch.object(executor, "_develop", side_effect=KeyError("bug")), self.assertRaises(KeyError):
            await executor.execute([self.task()], self.session(), self.caps)
        self.assertEqual(self.recorded(), ["失败 · Faster tests"])

    async def test_an_interrupted_or_detached_run_has_not_ended(self) -> None:
        for error in (asyncio.CancelledError(), RunDetached("stopping")):
            executor = self.sde()
            with patch.object(executor, "_develop", side_effect=error), self.assertRaises(type(error)):
                await executor.execute([self.task()], self.session(), self.caps)
        self.assertEqual(self.recorded(), [])

    async def test_a_suspended_task_is_recorded_once_a_person_stops_it(self) -> None:
        asked = await self.execute(dev(FIXED, status="needs_input", question="which?", summary="looked at xdist"))
        assert isinstance(asked, Suspend), asked
        self.assertEqual(self.recorded(), [])
        waiting = self.session(status="suspended", pending_request_id=asked.request_id,
                               checkpoint=asked.session_checkpoint, executor_session_id=asked.executor_session_id)
        answer = event({"approval_request_id": asked.request_id, "approved": False}, inbox_id=ALPHA)
        for _ in range(2):
            await self.execute(task=answer, session=waiting)
            self.assertEqual(self.recorded(), ["停止 · Faster tests"])
        self.assertIn("question declined", self.text())
        self.assertIn("> looked at xdist", self.text())

    async def test_a_task_without_the_field_behaves_as_before(self) -> None:
        plain = self.payload()
        outcome = await self.execute(NO_COMMIT, task=event(plain, inbox_id=ALPHA))
        assert isinstance(outcome, Emit), outcome
        self.assertFalse(self.note.parent.exists())
        self.assertIsNone(self.world.db.fetchone("SELECT 1 FROM attempt_notes"))

    async def test_an_unusable_goal_is_refused_before_anything_starts(self) -> None:
        for bad in ("../x", "", "a/b", None):
            outcome = await self.execute(task=self.task(attempt_notes=bad))
            assert isinstance(outcome, Emit), outcome
            self.assertEqual(outcome.status, "refused")
            self.assertIn("attempt_notes", outcome.messages[0])
        self.assertEqual(self.calls("sde"), [])
        self.assertFalse(self.note.parent.exists())

    async def test_a_write_failure_changes_neither_the_result_nor_the_delivery(self) -> None:
        with self.assertLogs(level="ERROR") as logged, \
                patch.object(self.world.hub, "propose_write", side_effect=OSError("disk full")):
            outcome = await self.execute(dev(FIXED))
        assert isinstance(outcome, Emit), outcome
        self.assertIn("[sde] delivered", outcome.messages[0])
        self.assertEqual(outcome.status, "done")
        self.assertIn("disk full", "".join(logged.output))
        self.assertEqual(self.recorded(), [])


class TestRestart(SDERestartCase):
    async def test_a_run_picked_up_after_a_restart_is_one_record(self) -> None:
        kept = Notes(self.root)
        self.addCleanup(kept.close)
        self.sde_extra = {"attempts": kept.notes}
        self.orca.scripts += [[None], [verdict_pass]]
        payload = {"executor_id": "sde", "text": "divide must reject a zero divisor", "title": "Reject zero",
                   "explicit_workdir": str(self.repo), "base_ref": "main", ATTEMPT_NOTES_KEY: GOAL}
        evt, _ = SQLiteInboxAdapter(self.db).append(Event(
            inbox_id=INBOX, source="test", external_id="task-1", correlation_key="task:1", payload=payload))
        worker, runs = self.process(self.db, "old")
        await self.stop_when(worker, runs, "develop")
        project = kept.registry.memory_for(INBOX).project_key
        self.assertFalse(kept.path(project).exists())
        developer = self.orca.session(0)
        [task] = self.tasks(developer)
        committed(self.repo, "def divide(a, b):\n    return a / b if b else 0\n")(developer, task)
        # The next process has the database, not the objects, of the first.
        self.sde_extra = {"attempts": AttemptNotes(store=SQLiteAttemptStore(kept.db), memory_for=kept.registry.memory_for)}
        worker, _, _ = self.restarted()
        [report] = await worker.process_inbox(evt.inbox_id)
        self.assertIsInstance(report.outcome, Emit)
        self.assertEqual(kept.entries(project=project), ["没合并 · Reject zero"])
        self.assertIn("external_id: task-1", kept.text(project))


class TestFirstPrompt(SDENotesCase):
    async def prompt(self, **extra: Any) -> str:
        self.log.unlink(missing_ok=True)
        await self.execute(NO_COMMIT, task=self.task(**extra))
        return self.calls("sde")[0]["prompt"]

    async def test_it_points_to_the_record_only_once_there_is_one(self) -> None:
        first = await self.prompt(text="one")
        self.assertNotIn("attempts", first)
        second = await self.prompt(text="two")
        self.assertIn(f'memory_read (mcp__hibernaut_memory__memory_read), uri "{NOTE}"', second)
        self.assertNotIn("xdist gains nothing", second)
        self.assertEqual(second.count("## Earlier attempts"), 1)
        # Another goal has no record yet, and a task without the field is told of none.
        self.assertNotIn("## Earlier attempts", await self.prompt(text="three", attempt_notes="memory-use"))
        self.log.unlink()
        await self.execute(NO_COMMIT, task=event(self.payload(), inbox_id=ALPHA),
                           session=SessionRef(session_id="sess-other", inbox_id=ALPHA, executor_id="sde"))
        self.assertNotIn("## Earlier attempts", self.calls("sde")[0]["prompt"])

    async def test_without_the_memory_tools_there_is_nothing_to_read_it_with(self) -> None:
        await self.prompt()
        self.access = None
        self.assertNotIn("memory_read", await self.prompt())

    async def test_a_session_that_cannot_be_granted_the_tools_is_not_told_of_the_record(self) -> None:
        self.index("- [Tried](notes/tried.md)\n")
        await self.prompt(text="one")
        granted = await self.prompt(text="two")
        self.assertIn("## Earlier attempts", granted)
        with ungranted(self.world.db):
            denied = await self.prompt(text="two")
        self.assertNotIn("memory_read", denied)
        self.assertNotIn("## Earlier attempts", denied)
        self.assertEqual(self.recorded()[0], "没有改动 · Faster tests")


class TestDelivered(SDEDeliveryCase):
    def setUp(self) -> None:
        super().setUp()
        self.kept = Notes(self.root)
        self.addCleanup(self.kept.close)

    def executor(self, review: bool = False, delivery: Any = None) -> Any:
        executor = super().executor(review, delivery)
        executor._attempts = self.kept.notes
        return executor

    async def test_a_merged_pr_is_linked(self) -> None:
        self.set_plan(sde=[delivered_dev({"calc.py": "x = 1\n"})], reviewer=[PASS])
        outcome = await self.executor(True, self.delivery(auto_merge=True)).execute(
            [event(self.payload(attempt_notes=GOAL))], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertEqual(self.kept.entries(project="x"), ["已合并 · Reject zero divisors"])
        self.assertIn("https://github.example/o/r/pull/1", self.kept.text("x"))

    async def test_an_open_pr_is_not_merged_yet(self) -> None:
        self.set_plan(sde=[delivered_dev({"calc.py": "x = 1\n"})])
        outcome = await self.executor(delivery=self.delivery()).execute(
            [event(self.payload(attempt_notes=GOAL))], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertEqual(self.kept.entries(project="x"), ["没合并 · Reject zero divisors"])
        self.assertIn("https://github.example/o/r/pull/1", self.kept.text("x"))

    async def test_a_pr_whose_delivery_waits_for_a_person_is_known_to_the_record(self) -> None:
        self.set_plan(sde=[delivered_dev({"calc.py": "x = 1\n"})], reviewer=[PASS])
        self.caps.permissions = Mock()
        self.caps.permissions.check.side_effect = lambda tool, args: PermissionDecision(
            action="defer" if tool == MERGE_TOOL else "allow", reason="ask first")
        outcome = await self.executor(True, self.delivery(auto_merge=True)).execute(
            [event(self.payload(attempt_notes=GOAL))], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        self.kept.notes.session_closed(str(outcome.session_id))
        self.assertEqual(self.kept.entries(project="x"), ["停止 · Reject zero divisors"])
        self.assertIn("- PR: https://github.example/o/r/pull/1", self.kept.text("x"))

    async def test_later_attempts_are_not_given_the_merge_of_the_first(self) -> None:
        async def run(title: str, step: Any) -> None:
            self.set_plan(sde=[step], reviewer=[PASS])
            self.log.unlink(missing_ok=True)
            await self.executor(True, self.delivery(auto_merge=True)).execute(
                [event(self.payload(attempt_notes=GOAL, title=title))], None, self.caps)

        await run("first", delivered_dev({"calc.py": "x = 1\n"}))
        nothing = delivered_dev({}, kind="test_docs", commit=None, code_change=False)
        await run("second", nothing)
        await run("third", delivered_dev({"calc.py": "x = 2\n"}))
        self.assertEqual(self.kept.entries(project="x"), ["没合并 · third", "没有改动 · second", "已合并 · first"])
        self.assertEqual(self.kept.text("x").count("- PR: "), 1)


# --- end to end ---------------------------------------------------------------------------------------------------


class TestTheNextTaskReadsIt(SDENotesCase):
    async def test_what_the_first_task_tried_reaches_the_second_through_memory_read(self) -> None:
        db = self.world.db
        sessions = SQLiteSessionStoreAdapter(db, on_closed=self.notes.session_closed)
        inbox = SQLiteInboxAdapter(db)

        def submit(number: int, text: str) -> None:
            inbox.append(Event(
                inbox_id=ALPHA, source="test", external_id=f"order-{number}", correlation_key=f"task:{number}",
                payload=self.payload(text=text, title=f"Round {number}", attempt_notes=GOAL)))

        async def process(*steps: Any) -> Any:
            self.set_plan(sde=steps)
            worker = AgentWorker(
                worker_id="w", inbox=inbox, timers=SQLiteTimerAdapter(db), memory=self.world.registry.memory_for(ALPHA),
                permissions=InMemoryPermission(), outbox=SQLiteOutboxAdapter(db), lock=SQLiteLeaseLockAdapter(db),
                sessions=sessions, executors={"sde": self.sde()}, global_default_executor="sde")
            [report] = await worker.process_inbox(ALPHA)
            return report

        async with self.world.access.serving():
            submit(1, "make the tests faster")
            await process(NO_COMMIT)
            submit(1, "make the tests faster")
            submit(2, "make the tests faster, again")
            await process({**NO_COMMIT, "memory": [["memory_read", {"uri": NOTE}]]})
        second = self.calls("sde")
        [answer] = [c for c in second if "memory" in c]
        [(failed, read)] = answer["memory"]
        self.assertFalse(failed)
        content = json.loads(read)["content"]
        self.assertEqual(headings(content), ["没有改动 · Round 1"])
        self.assertIn("external_id: order-1", content)
        self.assertIn("> xdist gains nothing", content)
        self.assertIn(NOTE, [c for c in second if "prompt" in c][-1]["prompt"])
        self.assertEqual(self.recorded(), ["没有改动 · Round 2", "没有改动 · Round 1"])


def test_the_daemon_gives_its_developers_the_record_and_closes_it_with_the_session(tmp_path: Path) -> None:
    daemon = build_daemon_from_config(parse_config({
        "runtime": {"db_path": str(tmp_path / "hb.db"), "memory_root": str(tmp_path / "memory"), "default_executor": "sde"},
        "executors": {"sde": {"role": "sde", "review": False}},
        "projects": {"alpha": {"inbox_id": ALPHA, "repo": "o/alpha", "checkout": str(tmp_path)}},
    }))
    try:
        assert daemon.memory_hub is not None
        executor: Any = daemon.pool._worker_factory(ALPHA, "w")._executors["sde"]  # pyright: ignore
        attempts: AttemptNotes = executor._attempts
        assert executor._worktrees._settle == attempts.settle
        evt = Event(inbox_id=ALPHA, source="test", external_id="e1", payload={})
        daemon.sessions.save(SessionRef(session_id="s1", inbox_id=ALPHA, executor_id="sde"))
        attempts.begin("s1", ALPHA, (evt, GOAL), "Faster tests")
        daemon.sessions.close_session("s1")
        assert headings((daemon.memory_hub.project_dir("alpha") / NOTE).read_text()) == ["停止 · Faster tests"]
    finally:
        daemon.close()
