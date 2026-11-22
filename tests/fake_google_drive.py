from __future__ import annotations

import base64
import functools
import json
import os
import subprocess
import tempfile
import threading
import urllib.parse
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

FOLDER_MIME = "application/vnd.google-apps.folder"
DOC_MIME = "application/vnd.google-apps.document"
ACCESS_TOKEN = "ya29.fake-google-access-token"


def _b64url_decode(data: str) -> bytes:
    return base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))


@functools.lru_cache(maxsize=1)
def _cached_test_keypair() -> tuple[str, bytes]:
    with tempfile.TemporaryDirectory(prefix="gdrive_test_key_") as tmp:
        root = Path(tmp)
        private_pem = root / "private.pem"
        public_pem = root / "public.pem"
        subprocess.run(["openssl", "genrsa", "-out", str(private_pem), "2048"], check=True, capture_output=True)
        subprocess.run(
            ["openssl", "rsa", "-in", str(private_pem), "-pubout", "-out", str(public_pem)],
            check=True,
            capture_output=True,
        )
        return private_pem.read_text(encoding="utf-8"), public_pem.read_bytes()


# A throwaway RSA key made with openssl; the fake token endpoint verifies JWTs against its public half.
def make_test_service_account(directory: Path, token_uri: str) -> tuple[Path, Path]:
    private_text, public_bytes = _cached_test_keypair()
    public_pem = directory / "test-sa-public.pem"
    public_pem.write_bytes(public_bytes)
    key_json = directory / "test-sa.json"
    key_json.write_text(
        json.dumps(
            {
                "type": "service_account",
                "client_email": "queue-test@hibernaut-test.iam.gserviceaccount.com",
                "private_key": private_text,
                "token_uri": token_uri,
            }
        ),
        encoding="utf-8",
    )
    os.chmod(key_json, 0o600)
    return key_json, public_pem


@dataclass
class _Item:
    file_id: str
    name: str
    mime_type: str
    parents: list[str]
    content: str = ""
    modified_time: str = "2026-11-22T03:00:00Z"


@dataclass
class FakeGoogleDrive:
    public_key_pem: Path
    items: dict[str, _Item] = field(default_factory=dict)
    requests: list[tuple[str, str]] = field(default_factory=list)
    token_requests: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def add_folder(self, folder_id: str, name: str, parent: str) -> None:
        self.items[folder_id] = _Item(folder_id, name, FOLDER_MIME, [parent])

    def add_file(
        self,
        file_id: str,
        name: str,
        content: str,
        parent: str,
        *,
        mime_type: str = "text/markdown",
        modified_time: str = "2026-11-22T03:00:00Z",
    ) -> None:
        self.items[file_id] = _Item(file_id, name, mime_type, [parent], content, modified_time)

    def parents(self, file_id: str) -> list[str]:
        return list(self.items[file_id].parents)

    def content(self, file_id: str) -> str:
        return self.items[file_id].content

    def _verify_assertion(self, assertion: str) -> bool:
        header, claims, signature = assertion.split(".")
        with tempfile.TemporaryDirectory() as tmp:
            sig_path = Path(tmp) / "sig"
            sig_path.write_bytes(_b64url_decode(signature))
            proc = subprocess.run(
                ["openssl", "dgst", "-sha256", "-verify", str(self.public_key_pem), "-signature", str(sig_path)],
                input=f"{header}.{claims}".encode("ascii"),
                capture_output=True,
            )
        return proc.returncode == 0

    def serve(self) -> ThreadingHTTPServer:
        drive = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
                pass

            def _reply(self, status: int, body: bytes, ctype: str = "application/json") -> None:
                self.send_response(status)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _json(self, status: int, obj: Any) -> None:
                self._reply(status, json.dumps(obj).encode("utf-8"))

            def _authorized(self) -> bool:
                if self.headers.get("Authorization") != f"Bearer {ACCESS_TOKEN}":
                    self._json(401, {"error": "invalid token"})
                    return False
                return True

            def _meta(self, item: _Item) -> dict[str, Any]:
                return {
                    "id": item.file_id,
                    "name": item.name,
                    "mimeType": item.mime_type,
                    "modifiedTime": item.modified_time,
                    "parents": list(item.parents),
                    "size": str(len(item.content.encode("utf-8"))),
                }

            def _body(self) -> bytes:
                return self.rfile.read(int(self.headers.get("Content-Length") or 0))

            def do_POST(self) -> None:  # noqa: N802
                drive.requests.append(("POST", self.path))
                if self.path != "/token":
                    self._json(404, {})
                    return
                form = urllib.parse.parse_qs(self._body().decode("utf-8"))
                assertion = form.get("assertion", [""])[0]
                if not assertion or not drive._verify_assertion(assertion):
                    self._json(400, {"error": "invalid_grant"})
                    return
                drive.token_requests += 1
                self._json(200, {"access_token": ACCESS_TOKEN, "expires_in": 3600})

            def do_GET(self) -> None:  # noqa: N802
                drive.requests.append(("GET", self.path))
                if not self._authorized():
                    return
                parsed = urllib.parse.urlsplit(self.path)
                query = urllib.parse.parse_qs(parsed.query)
                with drive._lock:
                    if parsed.path == "/drive/v3/files":
                        q = query["q"][0]
                        parent = q.split("'")[1]
                        want_folders = f"mimeType = '{FOLDER_MIME}'" in q
                        files = [
                            self._meta(it)
                            for it in drive.items.values()
                            if parent in it.parents and (it.mime_type == FOLDER_MIME) == want_folders
                        ]
                        self._json(200, {"files": files})
                        return
                    parts = parsed.path.split("/")
                    if len(parts) >= 5 and parts[:4] == ["", "drive", "v3", "files"]:
                        item = drive.items.get(urllib.parse.unquote(parts[4]))
                        if item is None:
                            self._json(404, {"error": "notFound"})
                            return
                        if len(parts) == 6 and parts[5] == "export":
                            self._reply(200, item.content.encode("utf-8"), "text/plain")
                        elif query.get("alt") == ["media"]:
                            self._reply(200, item.content.encode("utf-8"), "text/markdown")
                        else:
                            self._json(200, self._meta(item))
                        return
                self._json(404, {})

            def do_PATCH(self) -> None:  # noqa: N802
                drive.requests.append(("PATCH", self.path))
                if not self._authorized():
                    return
                parsed = urllib.parse.urlsplit(self.path)
                query = urllib.parse.parse_qs(parsed.query)
                parts = parsed.path.split("/")
                body = self._body()
                with drive._lock:
                    item = drive.items.get(urllib.parse.unquote(parts[-1]))
                    if item is None:
                        self._json(404, {"error": "notFound"})
                        return
                    if parsed.path.startswith("/upload/drive/v3/files/"):
                        item.content = body.decode("utf-8")
                    else:
                        remove = query.get("removeParents", [""])[0]
                        add = query.get("addParents", [""])[0]
                        item.parents = [p for p in item.parents if p != remove] + ([add] if add else [])
                    self._json(200, {"id": item.file_id, "parents": item.parents})

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True).start()
        return server
