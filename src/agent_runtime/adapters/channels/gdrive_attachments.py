from __future__ import annotations

# Attachments of Drive tasks: files in the queue's 附件/ folder that a task references, downloaded through the
# Drive queue proxy when the task's Run starts and kept outside every worktree until the task's session ends.

import hashlib
import logging
import os
import re
import shutil
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agent_runtime.adapters.channels.gdrive_tasks import DriveClientProtocol, DriveFileMeta

ATTACHMENTS_FOLDER_NAME = "附件"

_DRIVE_ID = re.compile(r"^[A-Za-z0-9_-]{1,200}$")
_ID_CHARS = "A-Za-z0-9_-"
_UNSAFE_NAME_CHARS = re.compile(r"[/\\\x00-\x1f]")
_LOG = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class FetchedAttachment:
    # The reference as the task wrote it (an ID or a name), or the ID found in its body.
    ref: str
    name: str = ""
    path: str = ""
    size: int = 0
    sha256: str = ""
    export_mime_type: str = ""
    error: str = ""


def split_attachment_refs(raw: object) -> list[str]:
    if not isinstance(raw, str):
        return []
    refs: list[str] = []
    for part in raw.split(","):
        ref = part.strip().strip("'\"").strip()
        if ref and ref not in refs:
            refs.append(ref)
    return refs


def _mentions(body: str, file_id: str) -> bool:
    # A whole token: not part of a longer ID-like run, so a URL such as /document/d/<id>/edit counts.
    return re.search(rf"(?<![{_ID_CHARS}]){re.escape(file_id)}(?![{_ID_CHARS}])", body) is not None


def select_attachments(
    refs: Sequence[str], body: str, listing: Sequence[DriveFileMeta]
) -> tuple[list[tuple[str, DriveFileMeta]], list[FetchedAttachment]]:
    # Frontmatter refs match a file in 附件/ by ID, then by name; body IDs count only when a file in 附件/ has
    # that ID. The union keeps the frontmatter order first and fetches each file once.
    by_id = {meta.file_id: meta for meta in listing}
    chosen: list[tuple[str, DriveFileMeta]] = []
    failed: list[FetchedAttachment] = []
    seen: set[str] = set()
    for ref in refs:
        meta = by_id.get(ref)
        if meta is None:
            named = [m for m in listing if m.name == ref]
            if len(named) > 1:
                failed.append(FetchedAttachment(ref=ref, error=f"{ATTACHMENTS_FOLDER_NAME}/ 里有 {len(named)} 个同名文件，请改用文件 ID"))
                continue
            meta = named[0] if named else None
        if meta is None:
            failed.append(FetchedAttachment(ref=ref, error=f"不在 {ATTACHMENTS_FOLDER_NAME}/ 目录"))
        elif meta.file_id not in seen:
            seen.add(meta.file_id)
            chosen.append((ref, meta))
    for meta in listing:
        if meta.file_id not in seen and _mentions(body, meta.file_id):
            seen.add(meta.file_id)
            chosen.append((meta.file_id, meta))
    return chosen, failed


def _safe_name(name: str, fallback: str, export_mime_type: str) -> str:
    clean = _UNSAFE_NAME_CHARS.sub("_", name).strip().lstrip(".") or fallback
    if export_mime_type == "text/markdown" and not clean.lower().endswith(".md"):
        clean += ".md"
    elif export_mime_type == "text/plain" and not clean.lower().endswith(".txt"):
        clean += ".txt"
    return clean[:200]


def _private_dir(path: Path) -> Path:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path, 0o700)
    return path


class DriveAttachmentStore:
    # Layout: <root>/<task file ID>/<attachment file ID>/<name>; directories 0700, files 0600. The root belongs
    # outside every worktree: an SDE delivery requires its worktree to hold no untracked files.
    def __init__(self, client: DriveClientProtocol, root: str | Path) -> None:
        self._client = client
        self._root = Path(root).expanduser().resolve()

    @property
    def root(self) -> Path:
        return self._root

    def _task_dir(self, task_file_id: str) -> Path:
        if not _DRIVE_ID.fullmatch(task_file_id):
            raise ValueError(f"not a Drive file ID: {task_file_id!r}")
        return self._root / task_file_id

    def fetch(self, task_file_id: str, *, folder_id: str, refs: Sequence[str], body: str) -> list[FetchedAttachment]:
        if not folder_id:
            return [FetchedAttachment(ref=r, error=f"队列根下没有 {ATTACHMENTS_FOLDER_NAME}/ 目录（或 Drive 代理尚未更新）")
                    for r in refs]
        try:
            listing = self._client.list_files(folder_id)
        except Exception as exc:  # noqa: BLE001
            if refs:
                reason = f"无法列出 {ATTACHMENTS_FOLDER_NAME}/ 目录：{_describe(exc)}"
                return [FetchedAttachment(ref=r, error=reason) for r in refs]
            _LOG.warning("Drive task %s: listing %s/ failed: %s", task_file_id, ATTACHMENTS_FOLDER_NAME, exc)
            return []
        chosen, failed = select_attachments(refs, body, listing)
        fetched = [self._fetch_one(task_file_id, ref, meta) for ref, meta in chosen]
        return fetched + failed

    def _fetch_one(self, task_file_id: str, ref: str, meta: DriveFileMeta) -> FetchedAttachment:
        try:
            if not _DRIVE_ID.fullmatch(meta.file_id):
                raise ValueError(f"not a Drive file ID: {meta.file_id!r}")
            got = self._client.download(meta)
            target_dir = _private_dir(_private_dir(_private_dir(self._root) / task_file_id) / meta.file_id)
            target = target_dir / _safe_name(got.name or meta.name, meta.file_id, got.export_mime_type)
            fd, tmp = tempfile.mkstemp(dir=target_dir, prefix=".part-")
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(got.content)
                os.chmod(tmp, 0o600)
                os.replace(tmp, target)
            except BaseException:
                Path(tmp).unlink(missing_ok=True)
                raise
        except Exception as exc:  # noqa: BLE001
            return FetchedAttachment(ref=ref, name=meta.name, error=_describe(exc))
        return FetchedAttachment(
            ref=ref,
            name=target.name,
            path=str(target),
            size=len(got.content),
            sha256=hashlib.sha256(got.content).hexdigest(),
            export_mime_type=got.export_mime_type,
        )

    def task_ids(self) -> list[str]:
        try:
            return sorted(p.name for p in self._root.iterdir() if p.is_dir() and _DRIVE_ID.fullmatch(p.name))
        except FileNotFoundError:
            return []

    def remove(self, task_file_id: str) -> bool:
        target = self._task_dir(task_file_id)
        if not target.exists():
            return False
        shutil.rmtree(target)
        return True


def _describe(exc: Exception) -> str:
    # A DriveProxyError (gdrive_tasks, which imports this module) carries the proxy's status and reason.
    status, reason = getattr(exc, "status_code", None), getattr(exc, "reason", None)
    if isinstance(status, int) and isinstance(reason, str):
        return f"Drive 代理拒绝（HTTP {status}）：{reason}"
    return f"{type(exc).__name__}: {exc}"


def render_attachment_section(results: Sequence[FetchedAttachment]) -> str:
    if not results:
        return ""
    ok = [r for r in results if not r.error]
    failed = [r for r in results if r.error]
    lines = ["## 附件", ""]
    if ok:
        lines.append("Hibernaut 在 Run 开始时把任务引用的附件下载到了本机工作区之外（只读参考，不要复制进仓库）：")
        lines.append("")
        for r in ok:
            exported = {"text/markdown": "，Google Doc 已导出为 Markdown", "text/plain": "，Google Doc 已导出为纯文本"}
            note = exported.get(r.export_mime_type, "")
            lines.append(f"- `{r.name}`：{r.path}（{r.size} 字节，sha256 {r.sha256}{note}）")
        lines.append("")
    if failed:
        lines.append("以下附件没有拿到，任务照常开始：")
        lines.append("")
        lines.extend(f"- `{r.ref}`：{r.error}" for r in failed)
        lines.append("")
    return "\n".join(lines).rstrip()
