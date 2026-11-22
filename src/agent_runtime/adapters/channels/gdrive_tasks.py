from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from agent_runtime.core.models import Emit, Event, Suspend, utc_now_iso
from agent_runtime.core.ports import ChannelPort, SessionStorePort
from agent_runtime.engine.handler import EventHandler, IngestResult
from agent_runtime.engine.worker import RunReport

_GOOGLE_DOC_MIME = "application/vnd.google-apps.document"
_DEFAULT_FOLDER_NAMES = ("待执行", "执行中", "已完成", "失败")
_GDRIVE_KEY_PREFIX = "gdrive:"


@dataclass(frozen=True, slots=True)
class DriveFileMeta:
    file_id: str
    name: str
    mime_type: str
    modified_time: str
    parents: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class DriveQueueFolders:
    pending_id: str
    in_progress_id: str
    done_id: str
    failed_id: str


class DriveClientProtocol(Protocol):
    def find_subfolders(self, parent_id: str) -> dict[str, str]: ...
    def list_files(self, folder_id: str) -> list[DriveFileMeta]: ...
    def get_file_meta(self, file_id: str) -> DriveFileMeta: ...
    def read_text(self, file_meta: DriveFileMeta) -> str: ...
    def append_text(self, file_id: str, mime_type: str, addition: str) -> None: ...
    def move_file(self, file_id: str, *, add_parent: str, remove_parent: str) -> None: ...


def parse_task_markdown(raw_text: str) -> tuple[dict[str, str], str]:
    text = raw_text.lstrip("\ufeff")
    if not text.startswith("---"):
        return {}, text.strip()
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return {}, text.strip()
    end_idx: int | None = None
    for idx in range(1, len(lines)):
        if lines[idx].strip() == "---":
            end_idx = idx
            break
    if end_idx is None:
        return {}, text.strip()

    meta: dict[str, str] = {}
    for line in lines[1:end_idx]:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or ":" not in stripped:
            continue
        key, val = stripped.split(":", 1)
        clean_key = key.strip()
        clean_val = val.strip()
        if len(clean_val) >= 2 and clean_val[0] == clean_val[-1] and clean_val[0] in ("'", '"'):
            clean_val = clean_val[1:-1]
        if clean_key:
            meta[clean_key] = clean_val
    body = "\n".join(lines[end_idx + 1 :]).strip()
    return meta, body


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


class DriveProxyError(RuntimeError):
    def __init__(self, op: str, status_code: int, reason: str) -> None:
        super().__init__(f"Drive queue proxy refused '{op}' (HTTP {status_code}): {reason}")
        self.op = op
        self.status_code = status_code


def _meta_from_json(item: Any) -> DriveFileMeta:
    return DriveFileMeta(
        file_id=str(item["id"]),
        name=str(item.get("name", "")),
        mime_type=str(item.get("mimeType", "text/markdown")),
        modified_time=str(item.get("modifiedTime", "")),
        parents=tuple(str(p) for p in item.get("parents", ())),
    )


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


def resolve_queue_folders(
    client: DriveClientProtocol,
    *,
    root_folder_id: str | None = None,
    pending_folder_id: str | None = None,
    in_progress_folder_id: str | None = None,
    done_folder_id: str | None = None,
    failed_folder_id: str | None = None,
    folder_names: tuple[str, str, str, str] = _DEFAULT_FOLDER_NAMES,
) -> DriveQueueFolders:
    discovered: dict[str, str] = {}
    if not all((pending_folder_id, in_progress_folder_id, done_folder_id, failed_folder_id)):
        if not root_folder_id:
            raise ValueError(
                "Either root_folder_id or all four queue subfolder IDs must be configured"
            )
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

    def poll_once(self) -> list[IngestResult]:
        folders = self.ensure_folders()
        files = self._client.list_files(folders.pending_id)
        results: list[IngestResult] = []
        for meta in files:
            if not self._is_supported_task_file(meta):
                continue
            raw_text = self._client.read_text(meta)
            frontmatter, body = parse_task_markdown(raw_text)
            inbox_id = frontmatter.get("scope") or self._default_inbox_id
            external_id = f"gdrive:{meta.file_id}:{meta.modified_time}"
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
            for key in ("activities", "tags", "base_ref"):
                if frontmatter.get(key):
                    payload[key] = frontmatter[key]

            ingest_res = self._handler.ingest(
                inbox_id=inbox_id,
                source="gdrive:task",
                external_id=external_id,
                correlation_key=correlation_key,
                payload=payload,
            )
            # Move out of 待执行/ immediately after ingest to prevent duplicate pickup.
            self._client.move_file(
                meta.file_id,
                add_parent=folders.in_progress_id,
                remove_parent=folders.pending_id,
            )
            results.append(ingest_res)
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
        return True

    def handle_run_report(self, report: RunReport) -> bool:
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
