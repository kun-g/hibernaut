from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from agent_runtime.core.models import Emit, Event, Suspend, utc_now_iso
from agent_runtime.core.ports import ChannelPort, SessionStorePort
from agent_runtime.engine.handler import EventHandler, IngestResult
from agent_runtime.engine.worker import RunReport

_GDRIVE_SCOPE = "https://www.googleapis.com/auth/drive"
_GOOGLE_DOC_MIME = "application/vnd.google-apps.document"
_GOOGLE_FOLDER_MIME = "application/vnd.google-apps.folder"
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


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def sign_rs256(signing_input: bytes, private_key_pem: str) -> bytes:
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding, rsa

        key = serialization.load_pem_private_key(private_key_pem.encode("utf-8"), password=None)
        if not isinstance(key, rsa.RSAPrivateKey):
            raise TypeError("Service Account private key must be an RSA key")
        return key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    except ImportError:
        pass

    # Fallback to system openssl (present on macOS/Linux) to keep zero mandatory third-party deps.
    fd, tmp_path = tempfile.mkstemp(prefix="hb_sa_", suffix=".pem")
    try:
        os.chmod(tmp_path, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(private_key_pem)
        proc = subprocess.run(
            ["openssl", "dgst", "-sha256", "-sign", tmp_path],
            input=signing_input,
            capture_output=True,
            check=True,
        )
        return proc.stdout
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


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


HttpRequestFn = Callable[[str, str, Mapping[str, str], bytes | None], bytes]


def _default_http_request(
    method: str,
    url: str,
    headers: Mapping[str, str],
    body: bytes | None,
) -> bytes:
    req = urllib.request.Request(url, data=body, headers=dict(headers), method=method)
    with urllib.request.urlopen(req, timeout=30.0) as resp:
        return bytes(resp.read())


class ServiceAccountTokenProvider:
    def __init__(
        self,
        *,
        key_path: str | Path,
        scope: str = _GDRIVE_SCOPE,
        http_request: HttpRequestFn | None = None,
        jwt_signer: Callable[[bytes, str], bytes] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        raw = json.loads(Path(key_path).read_text(encoding="utf-8"))
        self._client_email = str(raw["client_email"])
        self._private_key = str(raw["private_key"])
        self._token_uri = str(raw.get("token_uri") or "https://oauth2.googleapis.com/token")
        self._scope = scope
        self._http = http_request or _default_http_request
        self._signer = jwt_signer or sign_rs256
        self._clock = clock
        self._cached_token: str | None = None
        self._expires_at: float = 0.0

    def get_token(self) -> str:
        now = float(self._clock())
        if self._cached_token is not None and now < self._expires_at - 60.0:
            return self._cached_token

        header = _b64url(json.dumps({"alg": "RS256", "typ": "JWT"}).encode("utf-8"))
        claims = _b64url(
            json.dumps(
                {
                    "iss": self._client_email,
                    "scope": self._scope,
                    "aud": self._token_uri,
                    "iat": int(now),
                    "exp": int(now) + 3600,
                }
            ).encode("utf-8")
        )
        signing_input = f"{header}.{claims}".encode("ascii")
        signature = _b64url(self._signer(signing_input, self._private_key))
        assertion = f"{header}.{claims}.{signature}"

        form = urllib.parse.urlencode(
            {
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": assertion,
            }
        ).encode("utf-8")
        raw_resp = self._http(
            "POST",
            self._token_uri,
            {"Content-Type": "application/x-www-form-urlencoded"},
            form,
        )
        payload = json.loads(raw_resp.decode("utf-8"))
        token = str(payload["access_token"])
        expires_in = float(payload.get("expires_in", 3600))
        self._cached_token = token
        self._expires_at = now + expires_in
        return token


class GoogleDriveHTTPClient:
    def __init__(
        self,
        *,
        token_provider: ServiceAccountTokenProvider,
        http_request: HttpRequestFn | None = None,
    ) -> None:
        self._tokens = token_provider
        self._http = http_request or _default_http_request

    def _auth_headers(self, extra: Mapping[str, str] | None = None) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {self._tokens.get_token()}"}
        if extra:
            headers.update(extra)
        return headers

    def find_subfolders(self, parent_id: str) -> dict[str, str]:
        query = (
            f"'{parent_id}' in parents and mimeType = '{_GOOGLE_FOLDER_MIME}' and trashed = false"
        )
        params = urllib.parse.urlencode(
            {"q": query, "fields": "files(id,name)", "pageSize": "100"}
        )
        url = f"https://www.googleapis.com/drive/v3/files?{params}"
        data = json.loads(self._http("GET", url, self._auth_headers(), None).decode("utf-8"))
        return {str(item["name"]): str(item["id"]) for item in data.get("files", [])}

    def list_files(self, folder_id: str) -> list[DriveFileMeta]:
        query = f"'{folder_id}' in parents and mimeType != '{_GOOGLE_FOLDER_MIME}' and trashed = false"
        params = urllib.parse.urlencode(
            {
                "q": query,
                "fields": "files(id,name,mimeType,modifiedTime,parents)",
                "orderBy": "modifiedTime asc",
                "pageSize": "100",
            }
        )
        url = f"https://www.googleapis.com/drive/v3/files?{params}"
        data = json.loads(self._http("GET", url, self._auth_headers(), None).decode("utf-8"))
        return [
            DriveFileMeta(
                file_id=str(item["id"]),
                name=str(item.get("name", "")),
                mime_type=str(item.get("mimeType", "text/markdown")),
                modified_time=str(item.get("modifiedTime", "")),
                parents=tuple(str(p) for p in item.get("parents", ())),
            )
            for item in data.get("files", [])
        ]

    def get_file_meta(self, file_id: str) -> DriveFileMeta:
        params = urllib.parse.urlencode({"fields": "id,name,mimeType,modifiedTime,parents"})
        url = f"https://www.googleapis.com/drive/v3/files/{urllib.parse.quote(file_id)}?{params}"
        item = json.loads(self._http("GET", url, self._auth_headers(), None).decode("utf-8"))
        return DriveFileMeta(
            file_id=str(item["id"]),
            name=str(item.get("name", "")),
            mime_type=str(item.get("mimeType", "text/markdown")),
            modified_time=str(item.get("modifiedTime", "")),
            parents=tuple(str(p) for p in item.get("parents", ())),
        )

    def read_text(self, file_meta: DriveFileMeta) -> str:
        quoted = urllib.parse.quote(file_meta.file_id)
        if file_meta.mime_type == _GOOGLE_DOC_MIME:
            url = f"https://www.googleapis.com/drive/v3/files/{quoted}/export?mimeType=text%2Fplain"
        else:
            url = f"https://www.googleapis.com/drive/v3/files/{quoted}?alt=media"
        return self._http("GET", url, self._auth_headers(), None).decode("utf-8")

    def append_text(self, file_id: str, mime_type: str, addition: str) -> None:
        meta = DriveFileMeta(file_id=file_id, name="", mime_type=mime_type, modified_time="")
        existing = self.read_text(meta)
        combined = existing.rstrip() + "\n\n" + addition.strip() + "\n"
        upload_mime = "text/plain; charset=utf-8" if mime_type == _GOOGLE_DOC_MIME else "text/markdown; charset=utf-8"
        quoted = urllib.parse.quote(file_id)
        url = f"https://www.googleapis.com/upload/drive/v3/files/{quoted}?uploadType=media"
        self._http(
            "PATCH",
            url,
            self._auth_headers({"Content-Type": upload_mime}),
            combined.encode("utf-8"),
        )

    def move_file(self, file_id: str, *, add_parent: str, remove_parent: str) -> None:
        params = urllib.parse.urlencode(
            {
                "addParents": add_parent,
                "removeParents": remove_parent,
                "fields": "id,parents",
            }
        )
        quoted = urllib.parse.quote(file_id)
        url = f"https://www.googleapis.com/drive/v3/files/{quoted}?{params}"
        self._http("PATCH", url, self._auth_headers({"Content-Type": "application/json"}), b"{}")


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
        file_id = extract_gdrive_file_id(session.correlation_key)
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

    def handle_failed_event(self, event: Event) -> bool:
        file_id = extract_gdrive_file_id(event.correlation_key)
        if not file_id:
            return False
        return self.write_failure(
            file_id,
            error=event.last_error or "executor failed",
            event_id=event.event_id,
            attempts=event.attempts,
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
        elif kind == "emit":
            await asyncio.to_thread(
                self.write_emit,
                file_id,
                messages=(content,),
                executor_id=session.executor_id if session else None,
                session_id=session.session_id if session else None,
                dedup_key=f"channel-emit:{file_id}:{hash(content)}",
            )
