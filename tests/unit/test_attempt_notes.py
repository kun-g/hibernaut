from __future__ import annotations

import contextlib
import queue
import sqlite3
import threading
from collections.abc import Callable, Iterator
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
    UNMERGED,
    AttemptNotes,
    AttemptResult,
    MergedPR,
)
from agent_runtime.adapters.memory_fs import GovernedMemoryHub, PartitionedMemoryAdapter, WorkspacePartitionRegistry
from agent_runtime.adapters.storage_sqlite import (
    SQLiteAttemptStore,
    SQLiteDatabase,
    SQLiteInboxAdapter,
    SQLiteSessionStoreAdapter,
)
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config
from agent_runtime.core.models import Event, SessionRef
from agent_runtime.engine.attempts import ATTEMPT_NOTES_KEY, attempt_goal, check_goal
from agent_runtime.engine.handler import EventHandler
from memory_hub import MemoryHubConfig
from tests.support.gdrive_tasks import _FakeDriveClient
from tests.support.sde_memory import ALPHA, BETA
from tests.support.attempt_notes import GOAL, NOTE, Notes, headings


@pytest.fixture
def notes(tmp_path: Path) -> Iterator[Notes]:
    opened = Notes(tmp_path)
    yield opened
    opened.close()


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
    entered_change = queue.SimpleQueue[bool]()
    overlapped = list[bool]()
    other = threading.Thread(target=lambda: second(race.elsewhere.notes))
    conn = race.elsewhere.db._conn
    timeout_ms, = conn.execute("PRAGMA busy_timeout").fetchone()

    def entered(attempt: str, change: Any) -> Any:
        callback_started = False

        def noted(row: Any) -> Any:
            nonlocal callback_started
            callback_started = True
            entered_change.put(True)
            return change(row)

        # Probe the real update once without waiting. Either its change ran or the writer holds the lock.
        conn.execute("PRAGMA busy_timeout=0")
        try:
            return theirs(attempt, noted)
        except sqlite3.OperationalError as exc:
            if callback_started or exc.sqlite_errorcode != sqlite3.SQLITE_BUSY:
                raise
            entered_change.put(False)
        finally:
            conn.execute(f"PRAGMA busy_timeout={timeout_ms}")
        return theirs(attempt, change)

    def held(attempt: str, change: Any) -> Any:
        def slow(row: Any) -> Any:
            # The row is read and not yet written: the other connection starts its own update now.
            other.start()
            overlapped.append(entered_change.get(timeout=5))
            return change(row)
        with patch.object(race.notes.store, "update", ours):
            return ours(attempt, slow)

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
