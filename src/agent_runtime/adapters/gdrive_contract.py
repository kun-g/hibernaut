from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

ATTACHMENTS_FOLDER_NAME = "附件"
QUEUE_FOLDER_NAMES = ("待执行", "执行中", "已完成", "失败")


@dataclass(frozen=True, slots=True)
class DriveFileMeta:
    file_id: str
    name: str
    mime_type: str
    modified_time: str
    parents: tuple[str, ...] = ()
    # Bytes, as Drive reports them; None for Google Docs and other files Drive stores no size for.
    size: int | None = None


@dataclass(frozen=True, slots=True)
class DriveQueueFolders:
    pending_id: str
    in_progress_id: str
    done_id: str
    failed_id: str
    # "" when the queue root has no 附件/ folder: attachments are then reported as unavailable.
    attachments_id: str = ""


@dataclass(frozen=True, slots=True)
class DriveDownload:
    name: str
    # The type of `content`: the file's own, or the export format of a Google Doc.
    mime_type: str
    content: bytes
    # "" for a file downloaded as stored; text/markdown or text/plain for an exported Google Doc.
    export_mime_type: str = ""


class DriveClientProtocol(Protocol):
    def find_subfolders(self, parent_id: str) -> dict[str, str]: ...
    def list_files(self, folder_id: str) -> list[DriveFileMeta]: ...
    def get_file_meta(self, file_id: str) -> DriveFileMeta: ...
    def read_text(self, file_meta: DriveFileMeta) -> str: ...
    def append_text(self, file_id: str, mime_type: str, addition: str) -> None: ...
    def move_file(self, file_id: str, *, add_parent: str, remove_parent: str) -> None: ...
    def download(self, file_meta: DriveFileMeta) -> DriveDownload: ...


def resolve_queue_folders(
    client: DriveClientProtocol,
    *,
    root_folder_id: str | None = None,
    pending_folder_id: str | None = None,
    in_progress_folder_id: str | None = None,
    done_folder_id: str | None = None,
    failed_folder_id: str | None = None,
    attachments_folder_id: str | None = None,
    folder_names: tuple[str, str, str, str] = QUEUE_FOLDER_NAMES,
) -> DriveQueueFolders:
    discovered: dict[str, str] = {}
    if not all((pending_folder_id, in_progress_folder_id, done_folder_id, failed_folder_id)):
        if not root_folder_id:
            raise ValueError(
                "Either root_folder_id or all four queue subfolder IDs must be configured"
            )
        discovered = client.find_subfolders(root_folder_id)
    elif root_folder_id and not attachments_folder_id:
        discovered = client.find_subfolders(root_folder_id)

    pending = pending_folder_id or discovered.get(folder_names[0])
    in_progress = in_progress_folder_id or discovered.get(folder_names[1])
    done = done_folder_id or discovered.get(folder_names[2])
    failed = failed_folder_id or discovered.get(folder_names[3])

    missing = [
        name
        for name, fid in zip(folder_names, (pending, in_progress, done, failed), strict=True)
        if not fid
    ]
    if missing:
        raise RuntimeError(
            f"Missing required Google Drive task queue subfolder(s) under '{root_folder_id}': {', '.join(missing)}"
        )
    assert pending and in_progress and done and failed
    return DriveQueueFolders(
        pending_id=pending,
        in_progress_id=in_progress,
        done_id=done,
        failed_id=failed,
        # Optional: a queue without 附件/ still runs its text tasks.
        attachments_id=attachments_folder_id or discovered.get(ATTACHMENTS_FOLDER_NAME, ""),
    )
