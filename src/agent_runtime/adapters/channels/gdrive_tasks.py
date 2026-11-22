from __future__ import annotations

import asyncio
import json
import logging
import re
import urllib.parse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from agent_runtime.adapters.channels.gdrive_attachments import (
    ATTACHMENTS_FOLDER_NAME,
    DriveAttachmentStore,
    FetchedAttachment,
    render_attachment_section,
    split_attachment_refs,
)
from agent_runtime.core.models import Emit, Event, Suspend, utc_now_iso
from agent_runtime.core.ports import ChannelPort, SessionStorePort
from agent_runtime.engine.handler import EventHandler, IngestResult
from agent_runtime.engine.worker import RunReport

_GOOGLE_DOC_MIME = "application/vnd.google-apps.document"
_DEFAULT_FOLDER_NAMES = ("待执行", "执行中", "已完成", "失败")
_GDRIVE_KEY_PREFIX = "gdrive:"
_SOURCE = "gdrive:task"


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


TASK_META_KEYS = ("scope", "executor_id", "priority", "title", "activities", "tags", "base_ref", "attachments")
# A thematic break: ---, ***, ___ (spaced or longer, such as the underscore rule a Google Doc exports as text).
_RULE = re.compile(r"^\s{0,3}([-*_])(?:\s*\1){2,}\s*$")
_SETEXT_EQ = re.compile(r"^\s{0,3}=+\s*$")
_ATX_PREFIX = re.compile(r"^#{1,6}\s+")
_MD_ESCAPE = re.compile(r"\\([!-/:-@\[-`{-~])")
_ANY_KEY_LINE = re.compile(r"^([A-Za-z_][A-Za-z0-9_-]*)\s*:(.*)$")
_KNOWN_KEY_AT = re.compile(r"(?:^|(?<=\s))(" + "|".join(TASK_META_KEYS) + r")\s*:")


def _unquote(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


def _metadata_text(line: str) -> str:
    # Google's Markdown export turns metadata lines into headings and escapes punctuation such as executor\_id.
    return _MD_ESCAPE.sub(r"\1", _ATX_PREFIX.sub("", line.strip()).rstrip("#").strip())


def _split_known_keys(text: str) -> dict[str, str] | None:
    # "scope: a priority: b title: c" (lines a Google Doc joined into one) splits at each known key;
    # a quoted value is kept whole, so `title: "Fix tags: parsing"` stays one title.
    first = _KNOWN_KEY_AT.match(text)
    if first is None:
        return None
    pairs: dict[str, str] = {}
    key, pos = first.group(1), first.end()
    while True:
        rest = text[pos:].lstrip()
        start = len(text) - len(rest)
        search_from = start
        if rest[:1] in ("'", '"'):
            close = text.find(rest[0], start + 1)
            if close != -1:
                search_from = close + 1
        nxt = _KNOWN_KEY_AT.search(text, search_from)
        while nxt is not None and nxt.group(1) in pairs | {key: ""}:
            nxt = _KNOWN_KEY_AT.search(text, nxt.end())
        pairs[key] = _unquote(text[start : nxt.start() if nxt else len(text)])
        if nxt is None:
            return pairs
        key, pos = nxt.group(1), nxt.end()


def parse_task_markdown(raw_text: str) -> tuple[dict[str, str], str]:
    # Frontmatter opens with a rule. Closed by a second rule (or a setext underline) it is read as before, any
    # `key: value` included. Unclosed, as a Google Doc export leaves it, only lines starting with a known key count,
    # and the block ends at the first other line. Anything not recognised stays in the body untouched.
    text = raw_text.lstrip("\ufeff").replace("\r\n", "\n").replace("\r", "\n")
    lines = text.split("\n")
    start = next((i for i, ln in enumerate(lines) if ln.strip()), len(lines))
    if start == len(lines) or not _RULE.match(lines[start]):
        return {}, text.strip()

    close: int | None = None
    for idx in range(start + 1, len(lines)):
        line = lines[idx].strip()
        if _RULE.match(line) or _SETEXT_EQ.match(line):
            close = idx
            break
        if line and not line.startswith("#") and not _ANY_KEY_LINE.match(_metadata_text(line)):
            break

    meta: dict[str, str] = {}
    if close is not None:
        for line in lines[start + 1 : close]:
            content = _metadata_text(line)
            known = _split_known_keys(content)
            if known is not None:
                meta.update(known)
            elif not line.strip().startswith("#") and (match := _ANY_KEY_LINE.match(content)):
                meta[match.group(1)] = _unquote(match.group(2))
        return meta, "\n".join(lines[close + 1 :]).strip()

    body_from = start + 1
    for idx in range(start + 1, len(lines)):
        if not lines[idx].strip():
            continue
        known = _split_known_keys(_metadata_text(lines[idx]))
        if known is None:
            break
        meta.update(known)
        body_from = idx + 1
    if not meta:
        return {}, text.strip()
    return meta, "\n".join(lines[body_from:]).strip()


def extract_gdrive_file_id(correlation_key: str | None) -> str | None:
    if correlation_key and correlation_key.startswith(_GDRIVE_KEY_PREFIX):
        file_id = correlation_key.removeprefix(_GDRIVE_KEY_PREFIX).strip()
        return file_id or None
    return None


class ProxyResponseLike(Protocol):
    @property
    def status_code(self) -> int: ...
    @property
    def body(self) -> str: ...


class ServiceProxy(Protocol):
    def call_service(
        self,
        *,
        service_name: str,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> ProxyResponseLike: ...

    # The response body as bytes, with its headers: for the binary 'download' operation.
    def call_service_bytes(
        self,
        *,
        service_name: str,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> ProxyBytesResponseLike: ...


class ProxyBytesResponseLike(Protocol):
    @property
    def status_code(self) -> int: ...
    @property
    def body(self) -> bytes: ...
    @property
    def headers(self) -> Mapping[str, str]: ...


class DriveProxyError(RuntimeError):
    def __init__(self, op: str, status_code: int, reason: str) -> None:
        super().__init__(f"Drive queue proxy refused '{op}' (HTTP {status_code}): {reason}")
        self.op = op
        self.status_code = status_code
        self.reason = reason


def _meta_from_json(item: Any) -> DriveFileMeta:
    return DriveFileMeta(
        file_id=str(item["id"]),
        name=str(item.get("name", "")),
        mime_type=str(item.get("mimeType", "text/markdown")),
        modified_time=str(item.get("modifiedTime", "")),
        parents=tuple(str(p) for p in item.get("parents", ())),
        size=int(item["size"]) if str(item.get("size", "")).isdigit() else None,
    )


def _error_reason(body: bytes | str) -> str:
    try:
        data = json.loads(body) if body else {}
    except (json.JSONDecodeError, UnicodeDecodeError):
        data = {}
    return str(data.get("error") or "no reason given") if isinstance(data, dict) else "no reason given"


class ProxiedDriveClient:
    # Holds no Google credential: the external proxy owns the service account key, talks to Drive
    # and returns only queue business results (file metadata, text, ok).
    def __init__(self, proxy: ServiceProxy, *, service_name: str = "gdrive-queue") -> None:
        self._proxy = proxy
        self._service = service_name

    def _call(self, op: str, payload: dict[str, Any]) -> dict[str, Any]:
        resp = self._proxy.call_service(service_name=self._service, method="POST", path=op, payload=payload)
        try:
            data = json.loads(resp.body) if resp.body else {}
        except json.JSONDecodeError:
            data = {}
        if not isinstance(data, dict):
            data = {}
        if resp.status_code != 200:
            raise DriveProxyError(op, resp.status_code, str(data.get("error") or "no reason given"))
        return data

    def find_subfolders(self, parent_id: str) -> dict[str, str]:
        folders = self._call("subfolders", {"parent_id": parent_id}).get("folders", {})
        return {str(k): str(v) for k, v in folders.items()}

    def list_files(self, folder_id: str) -> list[DriveFileMeta]:
        return [_meta_from_json(item) for item in self._call("list", {"folder_id": folder_id}).get("files", [])]

    def get_file_meta(self, file_id: str) -> DriveFileMeta:
        return _meta_from_json(self._call("meta", {"file_id": file_id})["file"])

    def read_text(self, file_meta: DriveFileMeta) -> str:
        # The proxy decides export vs. download from Drive's own metadata, not from the caller.
        return str(self._call("read", {"file_id": file_meta.file_id})["text"])

    def append_text(self, file_id: str, mime_type: str, addition: str) -> None:
        _ = mime_type
        self._call("append", {"file_id": file_id, "text": addition})

    def move_file(self, file_id: str, *, add_parent: str, remove_parent: str) -> None:
        self._call("move", {"file_id": file_id, "add_parent": add_parent, "remove_parent": remove_parent})

    def download(self, file_meta: DriveFileMeta) -> DriveDownload:
        # The body is the file itself; name and types come back in headers. The proxy enforces the folder and size.
        resp = self._proxy.call_service_bytes(
            service_name=self._service, method="POST", path="download", payload={"file_id": file_meta.file_id}
        )
        if resp.status_code != 200:
            raise DriveProxyError("download", resp.status_code, _error_reason(resp.body))
        headers = {k.lower(): v for k, v in resp.headers.items()}
        return DriveDownload(
            name=urllib.parse.unquote(headers.get("x-drive-file-name", "")) or file_meta.name,
            mime_type=headers.get("x-drive-mime-type", "") or "application/octet-stream",
            content=resp.body,
            export_mime_type=headers.get("x-drive-export-mime-type", ""),
        )


def resolve_queue_folders(
    client: DriveClientProtocol,
    *,
    root_folder_id: str | None = None,
    pending_folder_id: str | None = None,
    in_progress_folder_id: str | None = None,
    done_folder_id: str | None = None,
    failed_folder_id: str | None = None,
    attachments_folder_id: str | None = None,
    folder_names: tuple[str, str, str, str] = _DEFAULT_FOLDER_NAMES,
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


class DriveTaskPoller:
    def __init__(
        self,
        handler: EventHandler,
        *,
        client: DriveClientProtocol,
        default_inbox_id: str = "default",
        root_folder_id: str | None = None,
        pending_folder_id: str | None = None,
        in_progress_folder_id: str | None = None,
        done_folder_id: str | None = None,
        failed_folder_id: str | None = None,
    ) -> None:
        self._handler = handler
        self._client = client
        self._default_inbox_id = default_inbox_id
        self._root_folder_id = root_folder_id
        self._explicit_ids = (
            pending_folder_id,
            in_progress_folder_id,
            done_folder_id,
            failed_folder_id,
        )
        self._folders: DriveQueueFolders | None = None
        # external_id -> inbox_id of revisions seen in 待执行/ by the last poll.
        self._inbox_by_revision: dict[str, str] = {}
        # Revisions whose ingest was a dropped duplicate: re-ingesting them every poll would add a row each time.
        self._dropped_revisions: set[str] = set()

    def ensure_folders(self) -> DriveQueueFolders:
        if self._folders is None:
            self._folders = resolve_queue_folders(
                self._client,
                root_folder_id=self._root_folder_id,
                pending_folder_id=self._explicit_ids[0],
                in_progress_folder_id=self._explicit_ids[1],
                done_folder_id=self._explicit_ids[2],
                failed_folder_id=self._explicit_ids[3],
            )
        return self._folders

    @staticmethod
    def _is_supported_task_file(meta: DriveFileMeta) -> bool:
        if meta.mime_type == _GOOGLE_DOC_MIME:
            return True
        if meta.name.lower().endswith(".md"):
            return True
        return meta.mime_type.startswith("text/")

    def _is_open(self, inbox_id: str, external_id: str) -> bool:
        return self._handler.has_open_event(inbox_id=inbox_id, source=_SOURCE, external_id=external_id)

    def poll_once(self) -> list[IngestResult]:
        # A file stays in 待执行/ until its Run starts (DriveResultWriter.on_run_started moves it), so every poll
        # sees queued files again: the Inbox state, not the folder, says whether one is already taken.
        folders = self.ensure_folders()
        files = self._client.list_files(folders.pending_id)
        results: list[IngestResult] = []
        listed: dict[str, str] = {}
        for meta in files:
            if not self._is_supported_task_file(meta):
                continue
            external_id = f"gdrive:{meta.file_id}:{meta.modified_time}"
            known_inbox = self._inbox_by_revision.get(external_id)
            if known_inbox is not None:
                # Same revision as an earlier poll: skip the read while it is queued or after it was dropped.
                listed[external_id] = known_inbox
                if external_id in self._dropped_revisions or self._is_open(known_inbox, external_id):
                    continue
            raw_text = self._client.read_text(meta)
            frontmatter, body = parse_task_markdown(raw_text)
            inbox_id = frontmatter.get("scope") or self._default_inbox_id
            listed[external_id] = inbox_id
            if self._is_open(inbox_id, external_id):
                continue
            correlation_key = f"gdrive:{meta.file_id}"
            payload: dict[str, Any] = {
                "title": frontmatter.get("title") or meta.name,
                "body": body,
                "priority": frontmatter.get("priority", ""),
                "drive_file_id": meta.file_id,
                "drive_file_name": meta.name,
                "drive_mime_type": meta.mime_type,
                "drive_modified_time": meta.modified_time,
            }
            if frontmatter.get("executor_id"):
                payload["executor_id"] = frontmatter["executor_id"]
            # Guidance selection for role executors, as comma-separated lists.
            # attachments: Drive file IDs or names in 附件/, fetched when the Run starts.
            for key in ("activities", "tags", "base_ref", "attachments"):
                if frontmatter.get(key):
                    payload[key] = frontmatter[key]

            ingest_res = self._handler.ingest(
                inbox_id=inbox_id,
                source=_SOURCE,
                external_id=external_id,
                correlation_key=correlation_key,
                payload=payload,
            )
            if ingest_res.duplicate:
                self._dropped_revisions.add(external_id)
                logging.getLogger(__name__).warning(
                    "Drive task %s (%s) in 待执行 was already processed at revision %s; edit it to run it again",
                    meta.file_id,
                    meta.name,
                    meta.modified_time,
                )
            results.append(ingest_res)
        # Forget revisions no longer queued, so the memory stays bounded by the 待执行/ listing.
        self._inbox_by_revision = listed
        self._dropped_revisions &= listed.keys()
        return results

    async def run_forever(
        self,
        *,
        interval_seconds: float = 30.0,
        stop_event: asyncio.Event,
    ) -> None:
        while not stop_event.is_set():
            try:
                await asyncio.to_thread(self.poll_once)
            except Exception:  # noqa: BLE001
                logging.getLogger(__name__).exception("Google Drive task poll failed")
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            except asyncio.TimeoutError:
                continue


class DriveResultWriter:
    def __init__(
        self,
        *,
        client: DriveClientProtocol,
        sessions: SessionStorePort,
        inner_channel: ChannelPort | None = None,
        root_folder_id: str | None = None,
        pending_folder_id: str | None = None,
        in_progress_folder_id: str | None = None,
        done_folder_id: str | None = None,
        failed_folder_id: str | None = None,
        attachments_folder_id: str | None = None,
        attachments: DriveAttachmentStore | None = None,
    ) -> None:
        self._client = client
        self._sessions = sessions
        self._inner = inner_channel
        self._root_folder_id = root_folder_id
        self._explicit_ids = (
            pending_folder_id,
            in_progress_folder_id,
            done_folder_id,
            failed_folder_id,
        )
        self._attachments_folder_id = attachments_folder_id
        self._attachments = attachments
        self._folders: DriveQueueFolders | None = None
        self._reported_keys: set[str] = set()

    def ensure_folders(self) -> DriveQueueFolders:
        if self._folders is None:
            self._folders = resolve_queue_folders(
                self._client,
                root_folder_id=self._root_folder_id,
                pending_folder_id=self._explicit_ids[0],
                in_progress_folder_id=self._explicit_ids[1],
                done_folder_id=self._explicit_ids[2],
                failed_folder_id=self._explicit_ids[3],
                attachments_folder_id=self._attachments_folder_id,
            )
        return self._folders

    def _move_if_in_progress(self, file_id: str, meta: DriveFileMeta, target_folder_id: str) -> None:
        folders = self.ensure_folders()
        remove_parent = (
            folders.in_progress_id
            if (not meta.parents or folders.in_progress_id in meta.parents)
            else meta.parents[0]
        )
        if remove_parent != target_folder_id:
            self._client.move_file(
                file_id,
                add_parent=target_folder_id,
                remove_parent=remove_parent,
            )

    def _move_out_of_pending(self, file_id: str, meta: DriveFileMeta, target_folder_id: str) -> None:
        folders = self.ensure_folders()
        if folders.pending_id in meta.parents:
            self._client.move_file(file_id, add_parent=target_folder_id, remove_parent=folders.pending_id)

    def on_run_started(
        self,
        inbox_id: str,
        events: Sequence[Event],
        correlation_key: str | None,
    ) -> dict[str, dict[str, Any]] | None:
        # RunStartListener: a queued file moves to 执行中/ only once a Worker starts its Run. A file someone already
        # moved elsewhere (or a resumed task already in 执行中/) is left where it is. A failed move is logged and
        # the attachments are still fetched: neither stops the Run.
        _ = inbox_id
        file_ids = {
            fid
            for key in (correlation_key, *(evt.correlation_key for evt in events))
            if (fid := extract_gdrive_file_id(key))
        }
        for file_id in sorted(file_ids):
            try:
                folders = self.ensure_folders()
                self._move_out_of_pending(file_id, self._client.get_file_meta(file_id), folders.in_progress_id)
            except Exception as exc:  # noqa: BLE001
                logging.getLogger(__name__).warning("Drive task %s did not move to 执行中/: %s", file_id, exc)
        return self._with_attachments(events)

    def _with_attachments(self, events: Sequence[Event]) -> dict[str, dict[str, Any]] | None:
        # The newest revision of each task file in the batch gets the 附件 section appended to its body. Only this
        # Run's copy of the payload changes, so a restarted Run fetches the files again.
        newest: dict[str, Event] = {}
        for evt in events:
            file_id = evt.payload.get("drive_file_id")
            if isinstance(file_id, str) and extract_gdrive_file_id(evt.correlation_key) == file_id:
                newest[file_id] = evt
        amended: dict[str, dict[str, Any]] = {}
        for file_id, evt in newest.items():
            section = self._attachment_section(file_id, evt.payload)
            if section:
                body = str(evt.payload.get("body") or "").rstrip()
                amended[evt.event_id] = {**evt.payload, "body": f"{body}\n\n{section}" if body else section}
        return amended or None

    def _attachment_section(self, file_id: str, payload: Mapping[str, Any]) -> str:
        refs = split_attachment_refs(payload.get("attachments"))
        if self._attachments is None:
            # Unreachable from configuration, which always sets an attachments directory.
            return render_attachment_section(
                [FetchedAttachment(ref=r, error="Hibernaut 没有配置附件目录（gdrive.attachments_dir）") for r in refs]
            )
        try:
            folder_id = self.ensure_folders().attachments_id
        except Exception as exc:  # noqa: BLE001
            return render_attachment_section(
                [FetchedAttachment(ref=r, error=f"无法确定 {ATTACHMENTS_FOLDER_NAME}/ 目录：{exc}") for r in refs]
            )
        body = str(payload.get("body") or "")
        return render_attachment_section(self._attachments.fetch(file_id, folder_id=folder_id, refs=refs, body=body))

    def sweep_attachments(self) -> list[str]:
        # Drops the attachment directories of tasks with no active or suspended session. Directories are listed
        # before sessions: a Run saves its active session before it downloads, so a directory seen here whose session
        # is open is never mistaken for an orphan.
        if self._attachments is None:
            return []
        try:
            held = self._attachments.task_ids()
            if not held:
                return []
            open_ids = {
                fid
                for status in ("active", "suspended")
                for session in self._sessions.list_sessions(status=status)
                if (fid := extract_gdrive_file_id(session.correlation_key))
            }
            removed = [fid for fid in held if fid not in open_ids and self._attachments.remove(fid)]
        except OSError:
            logging.getLogger(__name__).warning("Sweeping Drive task attachments failed", exc_info=True)
            return []
        if removed:
            logging.getLogger(__name__).info("Removed attachments of closed Drive tasks: %s", ", ".join(removed))
        return removed

    def _drop_attachments(self, file_id: str) -> None:
        if self._attachments is not None:
            self._attachments.remove(file_id)

    def write_emit(
        self,
        file_id: str,
        *,
        messages: Sequence[str],
        executor_id: str | None = None,
        session_id: str | None = None,
        dedup_key: str | None = None,
    ) -> bool:
        if dedup_key and dedup_key in self._reported_keys:
            return False
        folders = self.ensure_folders()
        meta = self._client.get_file_meta(file_id)
        body = "\n\n".join(msg.strip() for msg in messages if msg.strip()) or "(no output)"
        block = (
            "## 执行结果（Hibernaut）\n"
            f"- 时间：{utc_now_iso()}\n"
            "- 状态：已完成（Emit）\n"
            f"- 执行器：{executor_id or 'unknown'}\n"
            f"- 会话：{session_id or 'unknown'}\n\n"
            f"{body}"
        )
        self._client.append_text(file_id, meta.mime_type, block)
        self._move_if_in_progress(file_id, meta, folders.done_id)
        if dedup_key:
            self._reported_keys.add(dedup_key)
        return True

    def write_suspend(
        self,
        file_id: str,
        *,
        prompt: str,
        request_id: str,
        executor_id: str | None = None,
        session_id: str | None = None,
        dedup_key: str | None = None,
    ) -> bool:
        if dedup_key and dedup_key in self._reported_keys:
            return False
        meta = self._client.get_file_meta(file_id)
        # The start move may have failed. Park the file in 执行中/ before appending: the append changes its
        # modifiedTime, and a poll seeing that new revision in 待执行/ would queue the suspended task again.
        self._move_out_of_pending(file_id, meta, self.ensure_folders().in_progress_id)
        block = (
            "## 执行结果（Hibernaut）\n"
            f"- 时间：{utc_now_iso()}\n"
            "- 状态：等待审批（Suspend，文件保留在执行中）\n"
            f"- 执行器：{executor_id or 'unknown'}\n"
            f"- 会话：{session_id or 'unknown'}\n"
            f"- 审批请求 ID：{request_id}\n\n"
            f"{prompt.strip()}"
        )
        self._client.append_text(file_id, meta.mime_type, block)
        if dedup_key:
            self._reported_keys.add(dedup_key)
        return True

    def write_failure(
        self,
        file_id: str,
        *,
        error: str,
        event_id: str | None = None,
        attempts: int = 0,
        session_id: str | None = None,
        dedup_key: str | None = None,
    ) -> bool:
        if dedup_key and dedup_key in self._reported_keys:
            return False
        folders = self.ensure_folders()
        meta = self._client.get_file_meta(file_id)
        block = (
            "## 执行结果（Hibernaut）\n"
            f"- 时间：{utc_now_iso()}\n"
            "- 状态：失败（failed）\n"
            f"- 事件 ID：{event_id or 'unknown'}\n"
            f"- 会话：{session_id or 'unknown'}\n"
            f"- 尝试次数：{attempts}\n\n"
            f"错误摘要：{error.strip() or 'unknown error'}"
        )
        self._client.append_text(file_id, meta.mime_type, block)
        self._move_if_in_progress(file_id, meta, folders.failed_id)
        if dedup_key:
            self._reported_keys.add(dedup_key)
        # 失败/ ends the task in the queue; a rerun moved back to 待执行/ fetches its attachments again.
        self._drop_attachments(file_id)
        return True

    def handle_run_report(self, report: RunReport) -> bool:
        try:
            return self._write_report(report)
        finally:
            # After every Run: a session that closed (this one, or one closed through MCP) no longer needs its files.
            self.sweep_attachments()

    def _write_report(self, report: RunReport) -> bool:
        if not report.session_id:
            return False
        session = self._sessions.get(report.session_id)
        if session is None:
            return False
        # A finished task's session is closed after its Run, which clears its correlation_key.
        file_id = extract_gdrive_file_id(report.correlation_key or session.correlation_key)
        if not file_id:
            return False
        dedup = f"report:{report.session_id}:{','.join(report.event_ids)}"
        if isinstance(report.outcome, Emit):
            return self.write_emit(
                file_id,
                messages=report.outcome.messages,
                executor_id=session.executor_id,
                session_id=session.session_id,
                dedup_key=dedup,
            )
        if isinstance(report.outcome, Suspend):
            return self.write_suspend(
                file_id,
                prompt=report.outcome.prompt,
                request_id=report.outcome.request_id,
                executor_id=session.executor_id,
                session_id=session.session_id,
                dedup_key=f"suspend:{report.outcome.request_id}",
            )
        return False

    def handle_failed_event(
        self,
        event: Event,
        *,
        error: str | None = None,
        attempts: int = 0,
    ) -> bool:
        file_id = extract_gdrive_file_id(event.correlation_key)
        if not file_id:
            return False
        return self.write_failure(
            file_id,
            error=error or "executor failed",
            event_id=event.event_id,
            attempts=attempts,
            session_id=event.session_id,
            dedup_key=f"failed:{event.event_id}",
        )

    async def send(
        self,
        inbox_id: str,
        content: str,
        metadata: dict[str, Any],
    ) -> None:
        if self._inner is not None:
            await self._inner.send(inbox_id, content, metadata)

        corr_key = metadata.get("correlation_key")
        session_id = metadata.get("session_id")
        session = self._sessions.get(str(session_id)) if isinstance(session_id, str) and session_id else None
        if not isinstance(corr_key, str) and session is not None:
            corr_key = session.correlation_key
        file_id = extract_gdrive_file_id(corr_key if isinstance(corr_key, str) else None)
        if not file_id:
            return

        kind = str(metadata.get("kind", ""))
        if kind == "approval_request":
            req_id = str(metadata.get("request_id") or "")
            await asyncio.to_thread(
                self.write_suspend,
                file_id,
                prompt=content,
                request_id=req_id,
                executor_id=session.executor_id if session else None,
                session_id=session.session_id if session else None,
                dedup_key=f"suspend:{req_id}" if req_id else None,
            )
        elif kind == "emit" and "run_id" not in metadata:
            # A worker Emit carries run_id and is already written by handle_run_report.
            await asyncio.to_thread(
                self.write_emit,
                file_id,
                messages=(content,),
                executor_id=session.executor_id if session else None,
                session_id=session.session_id if session else None,
                dedup_key=f"channel-emit:{file_id}:{hash(content)}",
            )
