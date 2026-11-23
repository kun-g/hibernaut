#!/usr/bin/env python3
"""Prepare and verify the Switchboard + Taskboard deployment under one domain (deploy/taskboard/README.md).

    python3 scripts/prepare_taskboard_deploy.py render --token-file ~/hub/taskboard.env \\
        [--switchboard-toml deploy/switchboard/switchboard.toml] [--taskboard-ref <commit>] [--switchboard-ref <commit>]
    set -a; . ~/hub/taskboard.env; set +a
    python3 scripts/prepare_taskboard_deploy.py smoke --base-url https://hub.example.com

render validates the configs, generates TASKBOARD_MCP_TOKEN, hashes each config for its config-sha label and prints
the environment to paste into Dokploy (or deploy/taskboard/.env). smoke checks the live domain: both health checks,
a fail-closed Taskboard /mcp, and every tb_* tool behind the token. Standard library only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import secrets
import subprocess
import sys
import urllib.error
import urllib.request
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from switchboard.config import SwitchboardConfig  # noqa: E402
from switchboard.config import load_config as load_switchboard_config  # noqa: E402
from switchboard.router import DEFAULT_ROUTES  # noqa: E402
from taskboard.adapters.mcp_tools import TOOL_NAMES  # noqa: E402
from taskboard.app.config import TaskboardConfig, normalize_base_path  # noqa: E402
from taskboard.app.config import load_config as load_taskboard_config  # noqa: E402

DEFAULT_TEMPLATE = ROOT / "deploy" / "taskboard" / "taskboard.docker.toml"
TOKEN_ENV = "TASKBOARD_MCP_TOKEN"
# Event types that reach Taskboard once Switchboard runs its default routes.
CUT_OVER_EVENTS = ("work.opened", "work.updated", "work.closed", "execution.reported")
_IN_NETWORK_HOSTS = frozenset({"switchboard", "localhost", "127.0.0.1", "::1"})


class UsageError(Exception):
    pass


def config_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()[:12]


def mask(token: str) -> str:
    return "*" * 8 + token[-4:]


def _git_ignored(path: Path) -> bool:
    try:
        result = subprocess.run(
            ["git", "check-ignore", "-q", "--no-index", str(path.relative_to(ROOT))],
            cwd=ROOT,
            capture_output=True,
            check=False,
        )
    except OSError:
        return False
    return result.returncode == 0


def check_token_file(raw: str) -> Path:
    path = Path(raw).expanduser().resolve()
    if path.exists():
        raise UsageError(f"--token-file {path} exists; remove it first to rotate the token deliberately")
    if path.is_relative_to(ROOT.resolve()) and not _git_ignored(path):
        raise UsageError(
            f"--token-file {path} is inside the repository and not git-ignored; "
            "keep the token outside tracked paths (e.g. ~/hub/taskboard.env)"
        )
    if not path.parent.is_dir():
        raise UsageError(f"--token-file directory {path.parent} does not exist")
    return path


def write_token_file(path: Path, token: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        os.fchmod(handle.fileno(), 0o600)
        handle.write(f"{TOKEN_ENV}={token}\n")


class Report:
    def __init__(self) -> None:
        self.lines: list[str] = []
        self.failed = 0

    def add(self, status: str, message: str) -> None:
        if status == "fail":
            self.failed += 1
        self.lines.append(f"[{status}] {message}")


def check_taskboard(config: TaskboardConfig, prefix: str, report: Report) -> None:
    base = config.http.base_path
    if base == prefix:
        report.add("ok", f'taskboard [http] base_path = "{base}"')
    else:
        report.add("fail", f'taskboard [http] base_path is "{base}" (expected "{prefix}")')
    if not config.switchboard.relay_url or not config.switchboard.mcp_url:
        report.add("fail", "taskboard [switchboard] needs relay_url and mcp_url")
        return
    for url in (config.switchboard.relay_url, config.switchboard.mcp_url):
        parts = urlsplit(url)
        if parts.scheme in ("ws", "http") and parts.hostname not in _IN_NETWORK_HOSTS:
            report.add("warn", f"taskboard uplink {url} sends tokens in plaintext over a public network")
    report.add("ok", f"taskboard uplink {config.switchboard.relay_url} as {config.switchboard.consumer_id}")


def check_switchboard(config: SwitchboardConfig, prefix: str, report: Report) -> None:
    base = config.http.base_path
    if base == prefix:
        report.add("ok", f'switchboard [http] base_path = "{base}"')
    else:
        report.add("fail", f'switchboard [http] base_path is "{base}" (expected "{prefix}")')
    if not config.relay.enabled:
        report.add("fail", "switchboard [relay] enabled = false: neither Taskboard nor Hibernaut can connect")
    elif config.relay.listen_port is not None:
        report.add("fail", f"switchboard [relay] listen_port = {config.relay.listen_port}: the proxy only forwards 8790")

    routes = {**DEFAULT_ROUTES, **config.routing.event_consumers}
    bypassed = [event for event in CUT_OVER_EVENTS if "taskboard" not in routes.get(event, ())]
    if not bypassed:
        report.add("ok", "switchboard routing: work.* and execution.reported go through Taskboard")
    else:
        report.add(
            "todo",
            f"switchboard routing: [routing.event_consumers] still sends {', '.join(bypassed)} past Taskboard; "
            "delete those entries to cut over once smoke passes",
        )
    if "hibernaut" not in routes.get("work.ready", ()):
        report.add("warn", "switchboard routing: work.ready does not reach hibernaut")
    outbound = config.outbound
    if not (outbound.notify_target or outbound.approval_target):
        report.add(
            "warn",
            "switchboard [outbound] sets neither notify_target nor approval_target: "
            "Taskboard's human_todo notices have nowhere to go",
        )


def render(args: argparse.Namespace) -> int:
    if not args.no_token and not args.token_file and not args.show_token:
        raise UsageError(
            f"a generated {TOKEN_ENV} would be lost: pass --token-file PATH (written 0600, outside tracked "
            "repository paths) or --show-token; --no-token only hashes and checks"
        )
    token_path = check_token_file(args.token_file) if args.token_file and not args.no_token else None
    tb_prefix = normalize_base_path(args.taskboard_prefix)
    sb_prefix = normalize_base_path(args.switchboard_prefix)

    template = Path(args.template)
    try:
        tb_config = load_taskboard_config(template)
    except (ValueError, OSError) as exc:
        raise UsageError(f"{template}: {exc}") from exc
    sb_path = Path(args.switchboard_toml) if args.switchboard_toml else None
    sb_config: SwitchboardConfig | None = None
    if sb_path is not None:
        try:
            sb_config = load_switchboard_config(sb_path)
        except (ValueError, OSError) as exc:
            raise UsageError(f"{sb_path}: {exc}") from exc

    report = Report()
    check_taskboard(tb_config, tb_prefix, report)
    if sb_config is not None:
        check_switchboard(sb_config, sb_prefix, report)

    # Fix the configs first: a token written now would make the rerun stop at "--token-file exists".
    token = None if args.no_token or report.failed else secrets.token_urlsafe(32)
    if token is not None and token_path is not None:
        write_token_file(token_path, token)

    tb_sha = config_sha(template)
    out = ["# Environment: Dokploy compose service > Environment, or deploy/taskboard/.env for plain compose"]
    if token is not None:
        where = f"full value in {token_path}" if token_path else "shown because of --show-token"
        out.append(f"# {TOKEN_ENV}: {where}; clients send it as Authorization: Bearer")
        out.append(f"{TOKEN_ENV}={token if args.show_token else mask(token)}")
    elif report.failed and not args.no_token:
        out.append(f"# {TOKEN_ENV}: not generated until the [fail] checks below pass")
    out += [f"TASKBOARD_REF={args.taskboard_ref or '<commit>'}", f"TASKBOARD_CONFIG_SHA={tb_sha}"]
    sb_sha = config_sha(sb_path) if sb_path is not None else None
    if sb_path is not None or args.switchboard_ref:
        out.append(f"SWITCHBOARD_REF={args.switchboard_ref or '<commit>'}")
    if sb_sha is not None:
        out.append(f"SWITCHBOARD_CONFIG_SHA={sb_sha}")

    out += ["", "# Labels (keep the list form; docker-compose.yml fills them from the variables above)"]
    out.append(f'taskboard:   labels: ["taskboard.config-sha={tb_sha}"]')
    if sb_sha is not None:
        out.append(f'switchboard: labels: ["switchboard.config-sha={sb_sha}"]')

    out += ["", "# Checks", *report.lines]
    out += [
        "",
        "# Next",
        "1. Paste the environment above (the real token from the token file) and deploy. Re-run render after any",
        "   toml edit and update only that service's *_CONFIG_SHA, so only that container is recreated.",
        "2. Smoke the domain: set -a; . <token file>; set +a; "
        "python3 scripts/prepare_taskboard_deploy.py smoke --base-url https://<domain>",
        "3. Cut over Switchboard routing (README.md) only after smoke passes; adding [routing.event_consumers]",
        "   back rolls it back.",
    ]
    print("\n".join(out))
    return 1 if report.failed else 0


def _request(
    url: str, *, payload: dict[str, Any] | None = None, token: str | None = None, timeout: float
) -> tuple[int | None, Any]:
    headers = {"Accept": "application/json", "User-Agent": "taskboard-deploy-smoke"}
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=data, method="POST" if data is not None else "GET", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status, raw = resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        status = exc.code
        try:
            raw = exc.read()
        except OSError:
            raw = b""
    except (urllib.error.URLError, OSError) as exc:
        return None, str(getattr(exc, "reason", exc))
    try:
        return status, json.loads(raw or b"null")
    except ValueError:
        return status, raw[:200].decode("utf-8", "replace")


def _rpc(method: str) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if method == "initialize":
        params = {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "taskboard-deploy-smoke", "version": "1"},
        }
    return {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}


def smoke(args: argparse.Namespace) -> int:
    parts = urlsplit(args.base_url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise UsageError(f"--base-url {args.base_url!r} must be http(s)://<domain>")
    base = args.base_url.rstrip("/")
    sb = normalize_base_path(args.switchboard_prefix)
    tb = normalize_base_path(args.taskboard_prefix)
    timeout = float(args.timeout)
    report = Report()
    print(f"smoke {base} (switchboard {sb or '/'}, taskboard {tb or '/'})")

    for path in (f"{sb}/healthz", f"{tb}/healthz"):
        status, body = _request(base + path, timeout=timeout)
        if status == 200 and body == {"status": "ok"}:
            report.add("ok", f"GET {path} -> 200")
        elif status is None:
            report.add("fail", f"GET {path}: {body}")
        else:
            report.add("fail", f"GET {path} -> {status} {json.dumps(body)[:200]} (expected 200 {{\"status\": \"ok\"}})")

    mcp = f"{base}{tb}/mcp"
    status, body = _request(mcp, payload=_rpc("initialize"), timeout=timeout)
    if status == 401:
        report.add("ok", f"POST {tb}/mcp without a token -> 401")
    elif status is None:
        report.add("fail", f"POST {tb}/mcp without a token: {body}")
    else:
        report.add("fail", f"POST {tb}/mcp without a token -> {status} (expected 401: the endpoint must fail closed)")

    token = (os.environ.get(args.token_env) or "").strip()
    if not token:
        report.add("fail", f"initialize: {args.token_env} is not set (export it; the token never goes on the command line)")
    else:
        status, body = _request(mcp, payload=_rpc("initialize"), token=token, timeout=timeout)
        result = body.get("result") if isinstance(body, dict) else None
        if status == 200 and isinstance(result, dict) and "protocolVersion" in result:
            server = result.get("serverInfo") or {}
            report.add("ok", f"initialize (server {server.get('name', '?')} {server.get('version', '?')})")
            status, body = _request(mcp, payload=_rpc("tools/list"), token=token, timeout=timeout)
            result = body.get("result") if isinstance(body, dict) else None
            tools = result.get("tools") if isinstance(result, dict) else None
            names = {str(t.get("name")) for t in tools if isinstance(t, dict)} if isinstance(tools, list) else set()
            missing = [name for name in TOOL_NAMES if name not in names]
            if status == 200 and not missing:
                report.add("ok", f"tools/list: {len(TOOL_NAMES)} tb_* tools")
            elif status != 200:
                report.add("fail", f"tools/list -> {status} {json.dumps(body)[:200]}")
            else:
                report.add("fail", f"tools/list: missing {', '.join(missing)}")
        else:
            report.add("fail", f"initialize -> {status} {json.dumps(body)[:200]}")

    print("\n".join(report.lines))
    checks = len(report.lines)
    print(f"{checks - report.failed}/{checks} checks passed" if report.failed else f"all {checks} checks passed")
    return 1 if report.failed else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="prepare_taskboard_deploy.py",
        description="Prepare (render) and verify (smoke) the Switchboard + Taskboard same-domain deployment.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    r = sub.add_parser("render", help="check configs, generate the Taskboard token, print env and config hashes")
    r.add_argument("--template", default=str(DEFAULT_TEMPLATE), help="taskboard toml to deploy (default: %(default)s)")
    r.add_argument("--switchboard-toml", help="switchboard toml to hash and check for base_path and routing cut-over")
    r.add_argument("--token-file", help="write TASKBOARD_MCP_TOKEN=<token> here (0600; never an existing file)")
    r.add_argument("--show-token", action="store_true", help="print the generated token instead of masking it")
    r.add_argument("--no-token", action="store_true", help="do not generate a token (re-hash after a config edit)")
    r.add_argument("--taskboard-ref", help="commit Taskboard builds from (TASKBOARD_REF)")
    r.add_argument("--switchboard-ref", help="commit Switchboard builds from (SWITCHBOARD_REF)")
    r.add_argument("--taskboard-prefix", default="/taskboard")
    r.add_argument("--switchboard-prefix", default="/switchboard")

    s = sub.add_parser("smoke", help="check the deployed domain end to end")
    s.add_argument("--base-url", required=True, help="https://<domain>")
    s.add_argument("--switchboard-prefix", default="/switchboard")
    s.add_argument("--taskboard-prefix", default="/taskboard")
    s.add_argument("--token-env", default=TOKEN_ENV, help="environment variable holding the token (default: %(default)s)")
    s.add_argument("--timeout", default="10", help="seconds per request (default: %(default)s)")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return render(args) if args.command == "render" else smoke(args)
    except (UsageError, ValueError) as exc:
        print(f"[{args.command}] {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
