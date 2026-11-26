from __future__ import annotations

from collections.abc import Sequence

from agent_runtime.adapters.channels import DriveFileMeta
from agent_runtime.core.models import Emit, Event, RunOutcome, SessionRef, Suspend
from agent_runtime.core.ports import RunCapabilities


class _FakeDriveClient:
    def __init__(self, root_id: str = "root-1") -> None:
        self.root_id = root_id
        self.subfolders: dict[str, dict[str, str]] = {
            root_id: {
                "待执行": "dir-pending",
                "执行中": "dir-progress",
                "已完成": "dir-done",
                "失败": "dir-failed",
            }
        }
        self.files: dict[str, DriveFileMeta] = {}
        self.contents: dict[str, str] = {}
        self.move_history: list[tuple[str, str, str]] = []

    def add_file(
        self,
        file_id: str,
        name: str,
        content: str,
        *,
        folder_id: str = "dir-pending",
        mime_type: str = "text/markdown",
        modified_time: str = "2026-11-22T03:00:00Z",
    ) -> None:
        self.files[file_id] = DriveFileMeta(
            file_id=file_id,
            name=name,
            mime_type=mime_type,
            modified_time=modified_time,
            parents=(folder_id,),
        )
        self.contents[file_id] = content

    def find_subfolders(self, parent_id: str) -> dict[str, str]:
        return dict(self.subfolders.get(parent_id, {}))

    def list_files(self, folder_id: str) -> list[DriveFileMeta]:
        return [m for m in self.files.values() if folder_id in m.parents]

    def get_file_meta(self, file_id: str) -> DriveFileMeta:
        return self.files[file_id]

    def read_text(self, file_meta: DriveFileMeta) -> str:
        return self.contents[file_meta.file_id]

    def append_text(self, file_id: str, mime_type: str, addition: str) -> None:
        _ = mime_type
        self.contents[file_id] = self.contents[file_id].rstrip() + "\n\n" + addition.strip() + "\n"

    def move_file(self, file_id: str, *, add_parent: str, remove_parent: str) -> None:
        meta = self.files[file_id]
        # Same refusals as DriveQueueGuard's 'move', so a move from the wrong folder fails here as in production.
        if add_parent == remove_parent:
            raise RuntimeError("Drive queue proxy refused 'move' (HTTP 400): 'add_parent' and 'remove_parent' must differ")
        if remove_parent not in meta.parents:
            raise RuntimeError("Drive queue proxy refused 'move' (HTTP 403): file is not in 'remove_parent'")
        new_parents = tuple(p for p in meta.parents if p != remove_parent) + (add_parent,)
        self.files[file_id] = DriveFileMeta(
            file_id=meta.file_id,
            name=meta.name,
            mime_type=meta.mime_type,
            modified_time=meta.modified_time,
            parents=new_parents,
        )
        self.move_history.append((file_id, remove_parent, add_parent))


class _DriveTestExecutor:
    executor_id = "gdrive-mock-exec"

    def __init__(self) -> None:
        self.calls: list[tuple[str | None, dict[str, object]]] = []

    async def execute(
        self,
        events: Sequence[Event],
        session: SessionRef | None,
        caps: RunCapabilities,
    ) -> RunOutcome:
        _ = caps
        payload = events[-1].payload
        self.calls.append((session.session_id if session else None, dict(payload)))
        body = str(payload.get("body", ""))
        if "NEED_APPROVAL" in body:
            return Suspend(
                request_id="req-gd-1",
                prompt="Please approve production rollout",
                session_checkpoint='{"step": "approval"}',
                session_id=session.session_id if session else None,
            )
        if "FORCE_FAIL" in body:
            raise RuntimeError("simulated build failure")
        return Emit(
            messages=(f"Completed task: {payload.get('title')}",),
            session_id=session.session_id if session else None,
        )
