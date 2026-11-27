from __future__ import annotations

# External credential proxy for the Google Drive task queue. It runs as its own OS user, is the only
# process that can read the service account key, and answers a fixed set of queue operations on
# loopback. Hibernaut reaches it through CredentialProxyClient and never sees a Google token.

import argparse
import base64
import hmac
import ipaddress
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from agent_runtime.adapters.gdrive_contract import (
    ATTACHMENTS_FOLDER_NAME,
    QUEUE_FOLDER_NAMES,
    DriveClientProtocol,
    DriveDownload,
    DriveFileMeta,
    DriveQueueFolders,
    resolve_queue_folders,
)

_GDRIVE_SCOPE = "https://www.googleapis.com/auth/drive"
_GOOGLE_API_BASE = "https://www.googleapis.com"
_GOOGLE_DOC_MIME = "application/vnd.google-apps.document"
_GOOGLE_FOLDER_MIME = "application/vnd.google-apps.folder"
_GOOGLE_APPS_PREFIX = "application/vnd.google-apps."
# Drive IDs are URL-safe base64-ish; anything else could break out of the quoted Drive query.
_DRIVE_ID = re.compile(r"^[A-Za-z0-9_-]{1,200}$")
_SERVICE_NAME = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
_MAX_REQUEST_BYTES = 1024 * 1024
_MAX_DRAIN_BYTES = 16 * 1024 * 1024
_MIN_TOKEN_CHARS = 16
# 100 MiB: room for a code bundle or a scanned PDF. The proxy holds a download in memory while relaying it, and
# Hibernaut holds it again while writing it out, so the bound is per-request memory as much as transfer size.
DEFAULT_MAX_DOWNLOAD_BYTES = 100 * 1024 * 1024

HttpRequestFn = Callable[[str, str, Mapping[str, str], bytes | None], bytes]


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def sign_rs256(signing_input: bytes, private_key_pem: str) -> bytes:
    try:
        from cryptography.hazmat.primitives import hashes, serialization  # pyright: ignore[reportMissingImports]
        from cryptography.hazmat.primitives.asymmetric import padding, rsa  # pyright: ignore[reportMissingImports]

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


def _default_http_request(method: str, url: str, headers: Mapping[str, str], body: bytes | None) -> bytes:
    req = urllib.request.Request(url, data=body, headers=dict(headers), method=method)
    with urllib.request.urlopen(req, timeout=30.0) as resp:  # noqa: S310
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
        # Proxy requests run on parallel threads; one of them refreshes, the others wait for it.
        self._lock = threading.Lock()

    def __repr__(self) -> str:
        return f"ServiceAccountTokenProvider(client_email={self._client_email!r})"

    @property
    def client_email(self) -> str:
        return self._client_email

    def get_token(self) -> str:
        with self._lock:
            return self._get_token_locked()

    def _get_token_locked(self) -> str:
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
            {"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": assertion}
        ).encode("utf-8")
        raw_resp = self._http("POST", self._token_uri, {"Content-Type": "application/x-www-form-urlencoded"}, form)
        payload = json.loads(raw_resp.decode("utf-8"))
        token = str(payload["access_token"])
        expires_in = float(payload.get("expires_in", 3600))
        self._cached_token = token
        self._expires_at = now + expires_in
        return token


def _meta_from_drive(item: Mapping[str, Any]) -> DriveFileMeta:
    return DriveFileMeta(
        file_id=str(item["id"]),
        name=str(item.get("name", "")),
        mime_type=str(item.get("mimeType", "text/markdown")),
        modified_time=str(item.get("modifiedTime", "")),
        parents=tuple(str(p) for p in item.get("parents", ())),
        size=int(item["size"]) if str(item.get("size", "")).isdigit() else None,
    )


class GoogleDriveHTTPClient:
    def __init__(
        self,
        *,
        token_provider: ServiceAccountTokenProvider,
        http_request: HttpRequestFn | None = None,
        api_base: str = _GOOGLE_API_BASE,
    ) -> None:
        self._tokens = token_provider
        self._http = http_request or _default_http_request
        self._files_url = f"{api_base.rstrip('/')}/drive/v3/files"
        self._upload_url = f"{api_base.rstrip('/')}/upload/drive/v3/files"

    def _auth_headers(self, extra: Mapping[str, str] | None = None) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {self._tokens.get_token()}"}
        if extra:
            headers.update(extra)
        return headers

    def _get_json(self, url: str) -> Any:
        return json.loads(self._http("GET", url, self._auth_headers(), None).decode("utf-8"))

    def _list_all(self, params: Mapping[str, str]) -> list[Any]:
        # Drive answers at most pageSize files at a time; the rest sit behind nextPageToken.
        items: list[Any] = []
        token = ""
        while True:
            query = dict(params, pageSize="100", **({"pageToken": token} if token else {}))
            data = self._get_json(f"{self._files_url}?{urllib.parse.urlencode(query)}")
            items.extend(data.get("files", []))
            token = str(data.get("nextPageToken") or "")
            if not token:
                return items

    def find_subfolders(self, parent_id: str) -> dict[str, str]:
        query = f"'{parent_id}' in parents and mimeType = '{_GOOGLE_FOLDER_MIME}' and trashed = false"
        items = self._list_all({"q": query, "fields": "nextPageToken,files(id,name)"})
        return {str(item["name"]): str(item["id"]) for item in items}

    def list_files(self, folder_id: str) -> list[DriveFileMeta]:
        query = f"'{folder_id}' in parents and mimeType != '{_GOOGLE_FOLDER_MIME}' and trashed = false"
        items = self._list_all(
            {
                "q": query,
                "fields": "nextPageToken,files(id,name,mimeType,modifiedTime,parents,size)",
                "orderBy": "modifiedTime asc",
            }
        )
        return [_meta_from_drive(item) for item in items]

    def get_file_meta(self, file_id: str) -> DriveFileMeta:
        params = urllib.parse.urlencode({"fields": "id,name,mimeType,modifiedTime,parents,size"})
        return _meta_from_drive(self._get_json(f"{self._files_url}/{urllib.parse.quote(file_id)}?{params}"))

    def _export(self, file_id: str, mime_type: str) -> bytes:
        query = urllib.parse.urlencode({"mimeType": mime_type})
        url = f"{self._files_url}/{urllib.parse.quote(file_id)}/export?{query}"
        return self._http("GET", url, self._auth_headers(), None)

    def _export_doc(self, file_id: str) -> tuple[bytes, str]:
        # Markdown keeps a task's frontmatter and headings; plain text turns `---` into a rule of underscores. Drive
        # can refuse the newer Markdown export, and then plain text is still a readable task.
        try:
            return self._export(file_id, "text/markdown"), "text/markdown"
        except urllib.error.HTTPError as exc:
            exc.close()
            return self._export(file_id, "text/plain"), "text/plain"

    def _media(self, file_id: str) -> bytes:
        return self._http("GET", f"{self._files_url}/{urllib.parse.quote(file_id)}?alt=media", self._auth_headers(), None)

    def read_text(self, file_meta: DriveFileMeta) -> str:
        if file_meta.mime_type == _GOOGLE_DOC_MIME:
            return self._export_doc(file_meta.file_id)[0].decode("utf-8")
        return self._media(file_meta.file_id).decode("utf-8")

    def download(self, file_meta: DriveFileMeta) -> DriveDownload:
        if file_meta.mime_type == _GOOGLE_DOC_MIME:
            content, export = self._export_doc(file_meta.file_id)
            return DriveDownload(name=file_meta.name, mime_type=export, content=content, export_mime_type=export)
        return DriveDownload(name=file_meta.name, mime_type=file_meta.mime_type, content=self._media(file_meta.file_id))

    def append_text(self, file_id: str, mime_type: str, addition: str) -> None:
        # Read back as plain text: the update below uploads text/plain, which Drive converts into the Google Doc.
        if mime_type == _GOOGLE_DOC_MIME:
            existing = self._export(file_id, "text/plain").decode("utf-8")
        else:
            existing = self._media(file_id).decode("utf-8")
        combined = existing.rstrip() + "\n\n" + addition.strip() + "\n"
        upload_mime = "text/plain; charset=utf-8" if mime_type == _GOOGLE_DOC_MIME else "text/markdown; charset=utf-8"
        url = f"{self._upload_url}/{urllib.parse.quote(file_id)}?uploadType=media"
        self._http("PATCH", url, self._auth_headers({"Content-Type": upload_mime}), combined.encode("utf-8"))

    def move_file(self, file_id: str, *, add_parent: str, remove_parent: str) -> None:
        params = urllib.parse.urlencode({"addParents": add_parent, "removeParents": remove_parent, "fields": "id,parents"})
        url = f"{self._files_url}/{urllib.parse.quote(file_id)}?{params}"
        self._http("PATCH", url, self._auth_headers({"Content-Type": "application/json"}), b"{}")


class ProxyRefused(Exception):
    def __init__(self, status: int, reason: str) -> None:
        super().__init__(reason)
        self.status = status
        self.reason = reason


def _meta_json(meta: DriveFileMeta) -> dict[str, Any]:
    return {
        "id": meta.file_id,
        "name": meta.name,
        "mimeType": meta.mime_type,
        "modifiedTime": meta.modified_time,
        "parents": list(meta.parents),
        "size": meta.size,
    }


def _drive_id(args: Mapping[str, Any], key: str) -> str:
    value = args.get(key)
    if not isinstance(value, str) or not _DRIVE_ID.fullmatch(value):
        raise ProxyRefused(400, f"'{key}' must be a Drive ID")
    return value


class DriveQueueGuard:
    OPS = ("subfolders", "list", "meta", "read", "append", "move", "download", "source_folder", "source_download")

    def __init__(
        self,
        drive: DriveClientProtocol,
        *,
        root_folder_id: str,
        pending_folder_id: str = "",
        in_progress_folder_id: str = "",
        done_folder_id: str = "",
        failed_folder_id: str = "",
        attachments_folder_id: str = "",
        sources_folder_id: str = "",
        allow_any_source_folder: bool = False,
        client_email: str = "",
        max_read_bytes: int = 2 * 1024 * 1024,
        max_append_bytes: int = 256 * 1024,
        max_download_bytes: int = DEFAULT_MAX_DOWNLOAD_BYTES,
    ) -> None:
        self._drive = drive
        self._root = root_folder_id
        self._explicit = (pending_folder_id, in_progress_folder_id, done_folder_id, failed_folder_id)
        self._explicit_attachments = attachments_folder_id
        self._sources = sources_folder_id
        self._allow_any_source_folder = allow_any_source_folder
        self._service_account = f"the service account ({client_email})" if client_email else "the service account"
        self._max_read = max_read_bytes
        self._max_append = max_append_bytes
        self._max_download = max_download_bytes
        self._folders: DriveQueueFolders | None = None

    def folders(self) -> DriveQueueFolders:
        if self._folders is None:
            self._folders = resolve_queue_folders(
                self._drive,
                root_folder_id=self._root,
                pending_folder_id=self._explicit[0] or None,
                in_progress_folder_id=self._explicit[1] or None,
                done_folder_id=self._explicit[2] or None,
                failed_folder_id=self._explicit[3] or None,
                attachments_folder_id=self._explicit_attachments or None,
            )
        return self._folders

    def _queue_ids(self) -> tuple[str, str, str, str]:
        f = self.folders()
        return (f.pending_id, f.in_progress_id, f.done_id, f.failed_id)

    def _queue_folder(self, args: Mapping[str, Any], key: str, *, attachments: bool = False) -> str:
        folder_id = _drive_id(args, key)
        allowed = self._queue_ids() + ((self.folders().attachments_id,) if attachments else ())
        if not folder_id or folder_id not in allowed:
            raise ProxyRefused(403, f"'{key}' is outside the task queue")
        return folder_id

    def download(self, args: Mapping[str, Any]) -> DriveDownload:
        attachments_id = self.folders().attachments_id
        if not attachments_id:
            raise ProxyRefused(403, f"the task queue has no {ATTACHMENTS_FOLDER_NAME}/ folder")
        meta = self._drive.get_file_meta(_drive_id(args, "file_id"))
        if meta.mime_type == _GOOGLE_FOLDER_MIME or attachments_id not in meta.parents:
            raise ProxyRefused(403, f"file is not in the {ATTACHMENTS_FOLDER_NAME}/ folder")
        if meta.mime_type.startswith(_GOOGLE_APPS_PREFIX) and meta.mime_type != _GOOGLE_DOC_MIME:
            raise ProxyRefused(415, "only Google Docs among Google files can be downloaded; export it to a file first")
        # Checked from metadata first, so an oversized file is never fetched; exports have no size until fetched.
        if meta.size is not None and meta.size > self._max_download:
            raise ProxyRefused(413, f"attachment is larger than {self._max_download} bytes")
        got = self._drive.download(meta)
        if len(got.content) > self._max_download:
            raise ProxyRefused(413, f"attachment is larger than {self._max_download} bytes")
        return got

    def _sources_root(self) -> str:
        if not self._sources:
            if self._allow_any_source_folder:
                raise ProxyRefused(400, f"Provide a Drive folder link and share the folder with {self._service_account}")
            raise ProxyRefused(403, "No sources folder is configured. Configure sources_folder_id for name lookup, "
                                   "or enable allow_any_source_folder, provide a Drive folder link and share the "
                                   f"folder with {self._service_account}")
        return self._sources

    def _excluded_source_ids(self) -> set[str]:
        if not self._allow_any_source_folder:
            return set()
        return {*self._queue_ids(), self.folders().attachments_id} - {""}

    def _source_folder_meta(self, args: Mapping[str, Any], excluded: set[str]) -> tuple[str, str]:
        if "folder_id" in args:
            root = None if self._allow_any_source_folder else self._sources_root()
            folder_id = _drive_id(args, "folder_id")
            try:
                meta = self._drive.get_file_meta(folder_id)
            except urllib.error.HTTPError as exc:
                if exc.code not in (403, 404):
                    raise
                exc.close()
                raise ProxyRefused(exc.code, f"Cannot access the Drive folder. Check the link and share the folder "
                                             f"with {self._service_account}") from exc
            if meta.mime_type != _GOOGLE_FOLDER_MIME:
                raise ProxyRefused(400, "folder_id must identify a Drive folder")
            if root is not None and root not in meta.parents:
                raise ProxyRefused(403, "folder is not directly inside the sources folder")
            folder_id, name = meta.file_id, meta.name
        else:
            root = self._sources_root()
            name = args.get("name")
            if not isinstance(name, str) or not name.strip():
                raise ProxyRefused(400, "'name' or 'folder_id' is required")
            name = name.strip()
            found = self._drive.find_subfolders(root).get(name)
            if found is None:
                raise ProxyRefused(404, f"the sources folder has no folder named '{name}'")
            folder_id = found
        if folder_id in excluded:
            raise ProxyRefused(403, "task queue and attachment folders cannot be used as sources")
        return folder_id, name

    def source_folder(self, args: Mapping[str, Any]) -> dict[str, Any]:
        excluded = self._excluded_source_ids()
        folder_id, name = self._source_folder_meta(args, excluded)
        return {"folder": {"id": folder_id, "name": name},
                "files": [_meta_json(m) for m in self._drive.list_files(folder_id) if excluded.isdisjoint(m.parents)]}

    def download_source(self, args: Mapping[str, Any]) -> DriveDownload:
        excluded = self._excluded_source_ids()
        if "folder_id" in args:
            folder_id, _ = self._source_folder_meta(args, excluded)
            folders = {folder_id}
        else:
            # Older clients send only file_id. Their scope stays within the configured sources root.
            folders = set(self._drive.find_subfolders(self._sources_root()).values())
        meta = self._drive.get_file_meta(_drive_id(args, "file_id"))
        if not excluded.isdisjoint(meta.parents):
            raise ProxyRefused(403, "task queue files and attachments cannot be used as sources")
        if meta.mime_type == _GOOGLE_FOLDER_MIME or folders.isdisjoint(meta.parents):
            raise ProxyRefused(403, "file is not directly inside the source folder")
        if meta.mime_type.startswith(_GOOGLE_APPS_PREFIX) and meta.mime_type != _GOOGLE_DOC_MIME:
            raise ProxyRefused(415, "only Google Docs among Google files can be downloaded; export it to a file first")
        if meta.size is not None and meta.size > self._max_download:
            raise ProxyRefused(413, f"file is larger than {self._max_download} bytes")
        got = self._drive.download(meta)
        if len(got.content) > self._max_download:
            raise ProxyRefused(413, f"file is larger than {self._max_download} bytes")
        return got

    def _queue_file(self, args: Mapping[str, Any]) -> DriveFileMeta:
        meta = self._drive.get_file_meta(_drive_id(args, "file_id"))
        if meta.mime_type == _GOOGLE_FOLDER_MIME or not set(meta.parents) & set(self._queue_ids()):
            raise ProxyRefused(403, "file is outside the task queue")
        return meta

    @staticmethod
    def _require_text_file(meta: DriveFileMeta) -> None:
        if not (
            meta.mime_type == _GOOGLE_DOC_MIME
            or meta.mime_type.startswith("text/")
            or meta.name.lower().endswith(".md")
        ):
            raise ProxyRefused(415, "only Markdown, text files and Google Docs are task files")

    def handle(self, op: str, args: Mapping[str, Any]) -> dict[str, Any]:
        if op == "subfolders":
            if _drive_id(args, "parent_id") != self._root:
                raise ProxyRefused(403, "'parent_id' is not the task queue root")
            folders = dict(zip(QUEUE_FOLDER_NAMES, self._queue_ids(), strict=True))
            if self.folders().attachments_id:
                folders[ATTACHMENTS_FOLDER_NAME] = self.folders().attachments_id
            return {"folders": folders}
        if op == "list":
            folder_id = self._queue_folder(args, "folder_id", attachments=True)
            return {"files": [_meta_json(m) for m in self._drive.list_files(folder_id)]}
        if op == "meta":
            return {"file": _meta_json(self._queue_file(args))}
        if op == "read":
            meta = self._queue_file(args)
            self._require_text_file(meta)
            text = self._drive.read_text(meta)
            if len(text.encode("utf-8")) > self._max_read:
                raise ProxyRefused(413, f"task file is larger than {self._max_read} bytes")
            return {"text": text}
        if op == "append":
            text = args.get("text")
            if not isinstance(text, str) or not text.strip():
                raise ProxyRefused(400, "'text' must be a non-empty string")
            if len(text.encode("utf-8")) > self._max_append:
                raise ProxyRefused(413, f"'text' is larger than {self._max_append} bytes")
            meta = self._queue_file(args)
            self._require_text_file(meta)
            self._drive.append_text(meta.file_id, meta.mime_type, text)
            return {"ok": True}
        if op == "move":
            add_parent = self._queue_folder(args, "add_parent")
            remove_parent = self._queue_folder(args, "remove_parent")
            if add_parent == remove_parent:
                raise ProxyRefused(400, "'add_parent' and 'remove_parent' must differ")
            meta = self._queue_file(args)
            if remove_parent not in meta.parents:
                raise ProxyRefused(403, "file is not in 'remove_parent'")
            self._drive.move_file(meta.file_id, add_parent=add_parent, remove_parent=remove_parent)
            return {"ok": True}
        if op == "source_folder":
            return self.source_folder(args)
        raise ProxyRefused(404, f"unknown operation '{op}'")

    def check(self) -> list[str]:
        f = self.folders()
        ids = (f.pending_id, f.in_progress_id, f.done_id, f.failed_id)
        lines = [
            f"{name} = {fid} ({len(self._drive.list_files(fid))} files)"
            for name, fid in zip(QUEUE_FOLDER_NAMES, ids, strict=True)
        ]
        if f.attachments_id:
            lines.append(f"{ATTACHMENTS_FOLDER_NAME} = {f.attachments_id} ({len(self._drive.list_files(f.attachments_id))} files)")
        else:
            lines.append(f"{ATTACHMENTS_FOLDER_NAME} = (none: tasks cannot reference attachments)")
        return lines


def _log(message: str) -> None:
    print(f"[drive-proxy] {message}", file=sys.stderr, flush=True)


class _ProxyServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], guard: DriveQueueGuard, service_name: str, client_token: str) -> None:
        if ":" in address[0]:
            self.address_family = socket.AF_INET6
        self.guard = guard
        self.prefix = f"/proxy/{service_name}/"
        self.client_token = client_token.encode("utf-8")
        super().__init__(address, _ProxyHandler)


class _ProxyHandler(BaseHTTPRequestHandler):
    server: _ProxyServer
    server_version = "hibernaut-drive-proxy"
    sys_version = ""

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        pass

    def _reply(self, status: int, obj: Mapping[str, Any]) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _reply_file(self, got: DriveDownload) -> None:
        # The body is the file as stored (or as exported); name and types travel in headers.
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(got.content)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Drive-File-Name", urllib.parse.quote(got.name, safe=""))
        self.send_header("X-Drive-Mime-Type", got.mime_type)
        if got.export_mime_type:
            self.send_header("X-Drive-Export-Mime-Type", got.export_mime_type)
        self.end_headers()
        self.wfile.write(got.content)

    def _refuse(self, status: int, reason: str, op: str = "-") -> None:
        _log(f"{op} -> {status}: {reason}")
        self._reply(status, {"error": reason})

    def _not_post(self) -> None:
        self.close_connection = True
        self._refuse(405, "only POST is accepted")

    do_GET = do_PUT = do_PATCH = do_DELETE = do_HEAD = _not_post  # noqa: N815

    def _read_body(self) -> bytes | None:
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = -1
        if length < 0:
            self.close_connection = True
            self._refuse(400, "invalid Content-Length")
            return None
        if length > _MAX_REQUEST_BYTES:
            # Drain a bounded body so the client reads the 413 instead of a reset connection.
            if length <= _MAX_DRAIN_BYTES:
                remaining = length
                while remaining > 0:
                    chunk = self.rfile.read(min(remaining, 65536))
                    if not chunk:
                        break
                    remaining -= len(chunk)
            self.close_connection = True
            self._refuse(413, "request body too large")
            return None
        return self.rfile.read(length)

    def do_POST(self) -> None:  # noqa: N802
        # Browsers always send Origin on cross-site POSTs; the proxy serves only local processes.
        if self.headers.get("Origin") is not None:
            self.close_connection = True
            self._refuse(403, "browser requests are not accepted")
            return
        auth = self.headers.get("Authorization", "")
        presented = auth.removeprefix("Bearer ").encode("utf-8") if auth.startswith("Bearer ") else b""
        if not presented or not hmac.compare_digest(presented, self.server.client_token):
            self.close_connection = True
            self._refuse(401, "missing or invalid client token")
            return
        path = urllib.parse.urlsplit(self.path).path
        op = path.removeprefix(self.server.prefix) if path.startswith(self.server.prefix) else ""
        if op not in DriveQueueGuard.OPS:
            self.close_connection = True
            self._refuse(404, "unknown operation")
            return
        raw = self._read_body()
        if raw is None:
            return
        try:
            args = json.loads(raw.decode("utf-8")) if raw else {}
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._refuse(400, "body must be a JSON object", op)
            return
        if not isinstance(args, dict):
            self._refuse(400, "body must be a JSON object", op)
            return
        try:
            if op in ("download", "source_download"):
                guard = self.server.guard
                got = guard.download(args) if op == "download" else guard.download_source(args)
                _log(f"{op} -> 200 ({len(got.content)} bytes)")
                self._reply_file(got)
                return
            result = self.server.guard.handle(op, args)
        except ProxyRefused as exc:
            self._refuse(exc.status, exc.reason, op)
            return
        except urllib.error.HTTPError as exc:
            # Upstream bodies are not relayed; the status is enough for the queue to decide.
            exc.close()
            self._refuse(404 if exc.code == 404 else 502, f"Drive returned HTTP {exc.code}", op)
            return
        except Exception as exc:  # noqa: BLE001
            self._refuse(502, f"Drive request failed ({type(exc).__name__})", op)
            return
        _log(f"{op} -> 200")
        self._reply(200, result)


def build_proxy_server(
    guard: DriveQueueGuard,
    *,
    host: str,
    port: int,
    service_name: str,
    client_token: str,
) -> ThreadingHTTPServer:
    _require_loopback(host)
    if len(client_token) < _MIN_TOKEN_CHARS:
        raise ValueError(f"client token must hold at least {_MIN_TOKEN_CHARS} characters")
    return _ProxyServer((host, port), guard, service_name, client_token)


def _require_loopback(host: str) -> None:
    if host == "localhost":
        return
    try:
        loopback = ipaddress.ip_address(host).is_loopback
    except ValueError:
        loopback = False
    if not loopback:
        raise ValueError(f"listen_host must be a loopback address (127.0.0.1 or ::1), got '{host}'")


def _private_file(path: Path, key: str) -> Path:
    try:
        st = path.stat()
    except OSError as exc:
        raise ValueError(f"{key} {path} cannot be read: {exc.strerror}") from None
    if st.st_mode & 0o077:
        raise ValueError(f"{key} {path} is readable by group or others; run chmod 600 on it")
    if st.st_uid != os.geteuid():
        raise ValueError(f"{key} {path} is owned by uid {st.st_uid}, not by the proxy's user (uid {os.geteuid()})")
    return path


_CONFIG_KEYS = frozenset(
    {
        "listen_host",
        "listen_port",
        "service_name",
        "service_account_key_file",
        "client_token_file",
        "root_folder_id",
        "pending_folder_id",
        "in_progress_folder_id",
        "done_folder_id",
        "failed_folder_id",
        "attachments_folder_id",
        "sources_folder_id",
        "allow_any_source_folder",
        "max_download_bytes",
    }
)


@dataclass(frozen=True, slots=True)
class DriveProxyConfig:
    service_account_key_file: Path
    client_token: str = field(repr=False)
    root_folder_id: str
    listen_host: str = "127.0.0.1"
    listen_port: int = 9100
    service_name: str = "gdrive-queue"
    pending_folder_id: str = ""
    in_progress_folder_id: str = ""
    done_folder_id: str = ""
    failed_folder_id: str = ""
    attachments_folder_id: str = ""
    sources_folder_id: str = ""
    allow_any_source_folder: bool = False
    max_download_bytes: int = DEFAULT_MAX_DOWNLOAD_BYTES


def load_proxy_config(path: str | Path) -> DriveProxyConfig:
    cfg_path = Path(path)
    raw = tomllib.loads(cfg_path.read_text(encoding="utf-8"))
    unknown = sorted(set(raw) - _CONFIG_KEYS)
    if unknown:
        raise ValueError(f"unknown key(s) in {cfg_path}: {', '.join(unknown)}")

    host = str(raw.get("listen_host", "127.0.0.1")).strip()
    _require_loopback(host)
    port = raw.get("listen_port", 9100)
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError("listen_port must be an integer between 1 and 65535")
    service = str(raw.get("service_name", "gdrive-queue")).strip()
    if not _SERVICE_NAME.fullmatch(service):
        raise ValueError("service_name must be lowercase letters, digits and '-'")

    max_download = raw.get("max_download_bytes", DEFAULT_MAX_DOWNLOAD_BYTES)
    if isinstance(max_download, bool) or not isinstance(max_download, int) or max_download <= 0:
        raise ValueError("max_download_bytes must be a positive integer")

    allow_any = raw.get("allow_any_source_folder", False)
    if not isinstance(allow_any, bool):
        raise ValueError("allow_any_source_folder must be a boolean")

    ids: dict[str, str] = {}
    for key in ("root_folder_id", "pending_folder_id", "in_progress_folder_id", "done_folder_id", "failed_folder_id",
                "attachments_folder_id", "sources_folder_id"):
        value = str(raw.get(key, "")).strip()
        if value and not _DRIVE_ID.fullmatch(value):
            raise ValueError(f"{key} must be a Drive folder ID")
        ids[key] = value
    if not ids["root_folder_id"]:
        raise ValueError("root_folder_id is required (the task queue folder shared with the service account)")

    def _file(key: str) -> Path:
        value = str(raw.get(key, "")).strip()
        if not value:
            raise ValueError(f"{key} is required")
        p = Path(value).expanduser()
        return _private_file(p if p.is_absolute() else cfg_path.resolve().parent / p, key)

    key_file = _file("service_account_key_file")
    token = _file("client_token_file").read_text(encoding="utf-8").strip()
    if not token:
        raise ValueError("client_token_file is empty")
    if len(token) < _MIN_TOKEN_CHARS:
        raise ValueError(f"client_token_file must hold at least {_MIN_TOKEN_CHARS} characters")
    return DriveProxyConfig(
        service_account_key_file=key_file,
        client_token=token,
        listen_host=host,
        listen_port=port,
        service_name=service,
        max_download_bytes=max_download,
        allow_any_source_folder=allow_any,
        **ids,
    )


def build_guard(config: DriveProxyConfig, *, api_base: str = _GOOGLE_API_BASE) -> DriveQueueGuard:
    tokens = ServiceAccountTokenProvider(key_path=config.service_account_key_file)
    drive = GoogleDriveHTTPClient(
        token_provider=tokens,
        api_base=api_base,
    )
    return DriveQueueGuard(
        drive,
        root_folder_id=config.root_folder_id,
        pending_folder_id=config.pending_folder_id,
        in_progress_folder_id=config.in_progress_folder_id,
        done_folder_id=config.done_folder_id,
        failed_folder_id=config.failed_folder_id,
        attachments_folder_id=config.attachments_folder_id,
        sources_folder_id=config.sources_folder_id,
        allow_any_source_folder=config.allow_any_source_folder,
        client_email=tokens.client_email,
        max_download_bytes=config.max_download_bytes,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m agent_runtime.adapters.credential_proxy.gdrive_queue",
        description="Credential proxy for the Hibernaut Google Drive task queue.",
    )
    parser.add_argument("--config", required=True, help="proxy TOML (see deploy/gdrive-proxy/README.md)")
    parser.add_argument("--check", action="store_true", help="resolve the queue folders, count their files and exit")
    opts = parser.parse_args(argv)
    try:
        config = load_proxy_config(opts.config)
    except (OSError, ValueError, tomllib.TOMLDecodeError) as exc:
        _log(f"invalid config: {exc}")
        return 2
    guard = build_guard(config)
    if opts.check:
        try:
            for line in guard.check():
                _log(line)
        except Exception as exc:  # noqa: BLE001
            reason = f"HTTP {exc.code}" if isinstance(exc, urllib.error.HTTPError) else str(exc)
            _log(f"check failed: {type(exc).__name__}: {reason}")
            return 1
        _log("check ok")
        return 0
    server = build_proxy_server(
        guard,
        host=config.listen_host,
        port=config.listen_port,
        service_name=config.service_name,
        client_token=config.client_token,
    )
    _log(f"listening on http://{config.listen_host}:{config.listen_port}/proxy/{config.service_name}/")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
