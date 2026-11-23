from __future__ import annotations

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast
from urllib.parse import parse_qs, unquote, urlparse

from dashboard.config import DashboardConfig, resolve_config
from dashboard.snapshot import RemoteProbeCache, build_snapshot, read_outbox_file, read_run_detail

STATIC_DIR = Path(__file__).resolve().parent / "static"

_STATIC_ROUTES: dict[str, tuple[str, str]] = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/index.html": ("index.html", "text/html; charset=utf-8"),
    "/details": ("index.html", "text/html; charset=utf-8"),
    "/details.html": ("index.html", "text/html; charset=utf-8"),
    "/assets/style.css": ("style.css", "text/css; charset=utf-8"),
    "/assets/common.js": ("common.js", "application/javascript; charset=utf-8"),
}


class _HTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 64

    def __init__(
        self,
        address: tuple[str, int],
        config: DashboardConfig,
        static_dir: Path,
    ) -> None:
        self.config = config
        self.static_dir = static_dir
        self.remote_cache = RemoteProbeCache(ttl_seconds=config.remote_cache_ttl)
        super().__init__(address, _DashboardHandler)


class _DashboardHandler(BaseHTTPRequestHandler):
    server_version = "HibernautDashboard/0.1"

    def log_message(self, format: str, *args: Any) -> None:
        return None

    def _srv(self) -> _HTTPServer:
        return cast(_HTTPServer, self.server)

    def do_GET(self) -> None:
        srv = self._srv()
        parsed = urlparse(self.path)
        path = parsed.path

        if path in _STATIC_ROUTES:
            fname, ctype = _STATIC_ROUTES[path]
            file_path = srv.static_dir / fname
            if not file_path.is_file():
                self._send_json(404, {"error": f"{fname} missing"})
                return
            raw = file_path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
            return

        if path == "/api/snapshot":
            qs = parse_qs(parsed.query)
            force = qs.get("force", ["0"])[0] in ("1", "true", "yes")
            try:
                snap = build_snapshot(srv.config, force_remote=force, remote_cache=srv.remote_cache)
                self._send_json(200, snap)
            except Exception as exc:
                self._send_json(500, {"error": str(exc)})
            return

        if path.startswith("/api/run/"):
            sid = unquote(path.removeprefix("/api/run/"))
            detail = read_run_detail(srv.config, sid)
            if detail is None:
                self._send_json(404, {"error": "run not found"})
                return
            self._send_json(200, detail)
            return

        if path.startswith("/api/outbox/"):
            fname = unquote(path.removeprefix("/api/outbox/"))
            content = read_outbox_file(srv.config, fname)
            if content is None:
                self._send_json(404, {"error": "outbox file not found"})
                return
            self._send_json(200, {"filename": fname, "content": content})
            return

        self._send_json(404, {"error": "not found"})

    def _send_json(self, status: int, body: Any) -> None:
        raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


class DashboardServer:
    def __init__(
        self,
        config: DashboardConfig,
        *,
        host: str = "127.0.0.1",
        port: int = 8877,
        static_dir: Path = STATIC_DIR,
    ) -> None:
        self._server = _HTTPServer((host, port), config, static_dir)
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        return int(self._server.server_address[1])

    @property
    def base_url(self) -> str:
        return f"http://{self._server.server_address[0]}:{self.port}"

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.05},
            name="hibernaut-dashboard",
            daemon=True,
        )
        self._thread.start()

    def serve_forever(self) -> None:
        self._server.serve_forever(poll_interval=0.05)

    def stop(self) -> None:
        if self._thread is not None:
            self._server.shutdown()
            self._thread.join(5)
            self._thread = None
        self._server.server_close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python -m dashboard",
        description="Read-only status dashboard for Hibernaut (HB), Taskboard (TB), and Switchboard (SB).",
    )
    parser.add_argument("--config", help="Path to hibernaut.toml (defaults to $HIBERNAUT_CONFIG or ./hibernaut.toml)")
    parser.add_argument("--hb-root", help="Hibernaut checkout/state root directory")
    parser.add_argument(
        "--hub-deploy-dir",
        help="Directory holding switchboard.toml, taskboard.toml, and taskboard.env",
    )
    parser.add_argument("--hub-url", help="Override hub base URL (e.g. https://hub.example.com)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8877)
    args = parser.parse_args(argv)

    cfg = resolve_config(
        config_path=args.config,
        hb_root=args.hb_root,
        hub_deploy_dir=args.hub_deploy_dir,
        hub_url=args.hub_url,
    )
    server = DashboardServer(cfg, host=args.host, port=args.port)
    print(f"Hibernaut dashboard listening on {server.base_url}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()
