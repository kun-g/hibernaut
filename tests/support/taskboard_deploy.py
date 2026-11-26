from __future__ import annotations

import io
import os
import threading
import urllib.error
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from scripts.prepare_taskboard_deploy import main as prepare_main


REPO_ROOT = Path(__file__).resolve().parents[2]
DEPLOY = REPO_ROOT / "deploy" / "taskboard"
SCRIPT = REPO_ROOT / "scripts" / "prepare_taskboard_deploy.py"
TOKEN = "test-taskboard-deploy-token"


def run_prepare(*argv: str, env: dict[str, str | None] | None = None) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    saved = dict(os.environ)
    try:
        for name, value in (env or {}).items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        with redirect_stdout(out), redirect_stderr(err):
            code = prepare_main(list(argv))
    finally:
        os.environ.clear()
        os.environ.update(saved)
    return code, out.getvalue(), err.getvalue()


class _PathProxy(BaseHTTPRequestHandler):
    # A prefix-keeping reverse proxy (like Traefik PathPrefix or Caddy handle): /<name>/... goes to that backend.
    backends: dict[str, str] = {}

    def log_message(self, format: str, *args: Any) -> None:
        pass

    def _forward(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else None
        upstream = self.backends.get(self.path.split("/", 2)[1])
        if upstream is None:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        headers = {k: v for k, v in self.headers.items() if k.lower() in ("authorization", "content-type")}
        req = urllib.request.Request(upstream + self.path, data=body, method=self.command, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                status, payload = resp.status, resp.read()
        except urllib.error.HTTPError as exc:
            status, payload = exc.code, exc.read()
        self.send_response(status)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    do_GET = _forward
    do_POST = _forward


@contextmanager
def serve(handler: type[BaseHTTPRequestHandler]) -> Iterator[str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
