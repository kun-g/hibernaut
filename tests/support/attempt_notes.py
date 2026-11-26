from __future__ import annotations

from pathlib import Path
from typing import Any

from agent_runtime.adapters.executor_cli.attempt_notes import AttemptNotes, AttemptResult, note_path
from agent_runtime.adapters.memory_fs import GovernedMemoryHub, WorkspacePartitionRegistry
from agent_runtime.adapters.storage_sqlite import SQLiteAttemptStore, SQLiteDatabase
from agent_runtime.core.models import Event
from memory_hub import MemoryHubConfig
from tests.support.sde_memory import ALPHA


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
