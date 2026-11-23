from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path

from agent_runtime.adapters.memory_fs.fs_memory import FileSystemMemoryAdapter

_SAFE_SEGMENT_RE = re.compile(r"[^A-Za-z0-9._-]+")


def check_inbox_id(inbox_id: str) -> None:
    if not inbox_id or not inbox_id.strip():
        raise ValueError("inbox_id must be a non-empty string")
    if ".." in inbox_id or "/" in inbox_id or "\\" in inbox_id:
        raise ValueError(f"Invalid inbox_id path characters: {inbox_id}")


def directory_key(path: Path) -> tuple[object, ...]:
    # The same directory under another spelling, link or letter case is the same one. A part that is not there yet is
    # compared without letter case: created, both spellings name one directory on a volume that ignores it.
    path = path.resolve()
    missing: list[str] = []
    for directory in (path, *path.parents):
        try:
            found = directory.stat()
        except OSError:
            missing.append(_folded(directory.name))
            continue
        return found.st_dev, found.st_ino, *reversed(missing)
    return (_folded(str(path)),)


def _folded(text: str) -> str:
    return unicodedata.normalize("NFD", unicodedata.normalize("NFD", text).casefold())


def _sanitize_inbox_id(inbox_id: str) -> str:
    check_inbox_id(inbox_id)
    return _SAFE_SEGMENT_RE.sub("__", inbox_id.strip())


@dataclass(frozen=True, slots=True)
class PartitionBinding:
    inbox_id: str
    memory_dir: Path
    workspace_dir: Path | None = None


class PartitionedMemoryAdapter:
    def __init__(
        self,
        *,
        inbox_id: str,
        project_memory_dir: str | Path,
        global_instructions_path: str | Path,
        workspace_dir: str | Path | None = None,
    ) -> None:
        self._inbox_id = inbox_id
        self._project_memory = FileSystemMemoryAdapter(project_memory_dir)
        self._global_instructions_path = Path(global_instructions_path).resolve()
        self._workspace_dir = Path(workspace_dir).resolve() if workspace_dir is not None else None

    @property
    def inbox_id(self) -> str:
        return self._inbox_id

    @property
    def workspace_dir(self) -> Path | None:
        return self._workspace_dir

    def load_baseline(self) -> dict[str, str]:
        baseline = self._project_memory.load_baseline()
        global_text = (
            self._global_instructions_path.read_text(encoding="utf-8").strip()
            if self._global_instructions_path.is_file()
            else ""
        )
        local_text = baseline["instructions.md"].strip()
        if global_text and local_text:
            baseline["instructions.md"] = f"{global_text}\n\n{local_text}"
        elif global_text:
            baseline["instructions.md"] = global_text
        return baseline

    def read_on_demand(self, path: str) -> str:
        return self._project_memory.read_on_demand(path)

    def write(self, path: str, content: str) -> None:
        normalized = path.lstrip("/")
        # Global instructions.md is strictly read-only from partition runtime writes.
        if normalized == "global/instructions.md":
            raise PermissionError("Global instructions.md is read-only")
        self._project_memory.write(path, content)


class WorkspacePartitionRegistry:
    def __init__(self, memory_root: str | Path) -> None:
        self._root = Path(memory_root).resolve()
        self._global_dir = self._root / "global"
        self._projects_dir = self._root / "projects"
        self._global_dir.mkdir(parents=True, exist_ok=True)
        self._projects_dir.mkdir(parents=True, exist_ok=True)
        self._global_instructions = self._global_dir / "instructions.md"
        if not self._global_instructions.exists():
            self._global_instructions.write_text("", encoding="utf-8")

        self._bindings: dict[str, PartitionBinding] = {}
        self._workspaces: dict[str, Path] = {}

    @property
    def global_instructions_path(self) -> Path:
        return self._global_instructions

    def set_global_instructions(self, content: str) -> None:
        tmp = self._global_instructions.with_suffix(".md.tmp")
        tmp.write_text(content, encoding="utf-8")
        tmp.replace(self._global_instructions)

    def register(
        self,
        inbox_id: str,
        workspace_dir: str | Path | None = None,
    ) -> PartitionBinding:
        safe_name = _sanitize_inbox_id(inbox_id)
        memory_dir = (self._projects_dir / safe_name).resolve()
        resolved_workspace = Path(workspace_dir).resolve() if workspace_dir is not None else None

        if resolved_workspace is not None:
            # Hard concurrency invariant: two inbox_ids must never share a local code directory. Keys are read anew:
            # a directory's key changes when it is created.
            key = directory_key(resolved_workspace)
            owner = next((other for other, theirs in self._workspaces.items()
                          if other != inbox_id and directory_key(theirs) == key), None)
            if owner is not None:
                raise ValueError(
                    f"Workspace directory '{resolved_workspace}' is already mounted by "
                    f"inbox_id '{owner}'; different inbox_ids must not share a local code "
                    "directory (use an isolated git worktree)."
                )
            self._workspaces[inbox_id] = resolved_workspace

        binding = PartitionBinding(
            inbox_id=inbox_id,
            memory_dir=memory_dir,
            workspace_dir=resolved_workspace,
        )
        self._bindings[inbox_id] = binding
        return binding

    def binding_for(self, inbox_id: str) -> PartitionBinding:
        if inbox_id not in self._bindings:
            return self.register(inbox_id)
        return self._bindings[inbox_id]

    def memory_for(self, inbox_id: str) -> PartitionedMemoryAdapter:
        binding = self.binding_for(inbox_id)
        return PartitionedMemoryAdapter(
            inbox_id=inbox_id,
            project_memory_dir=binding.memory_dir,
            global_instructions_path=self._global_instructions,
            workspace_dir=binding.workspace_dir,
        )

    def workspace_for(self, inbox_id: str) -> Path | None:
        return self.binding_for(inbox_id).workspace_dir
