from __future__ import annotations

import fnmatch
import hashlib
import io
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from scripts.prepare_taskboard_deploy import main as prepare_main
from switchboard.config import load_config as load_switchboard_config
from taskboard.adapters.mcp_tools import TOOL_NAMES
from taskboard.app.config import TaskboardConfig, load_config, parse_config
from taskboard.app.main import TaskboardService, build_service
from tests.switchboard_support import Harness, make_harness

REPO_ROOT = Path(__file__).resolve().parents[1]
DEPLOY = REPO_ROOT / "deploy" / "taskboard"
SWITCHBOARD_DEPLOY = REPO_ROOT / "deploy" / "switchboard"
SCRIPT = REPO_ROOT / "scripts" / "prepare_taskboard_deploy.py"
TOKEN = "test-taskboard-deploy-token"


def compose_services(compose: str) -> dict[str, str]:
    body = compose.split("\nservices:\n", 1)[1].split("\nvolumes:\n", 1)[0]
    blocks = re.split(r"(?m)^  (?=\w)", body)
    return {block.split(":", 1)[0]: block for block in blocks if block.strip()}


def dockerignore_sends(patterns: list[str], rel: str) -> bool:
    # Docker's rule: the last pattern matching the path (or one of its parent directories) decides.
    parts = rel.split("/")
    candidates = ["/".join(parts[: i + 1]) for i in range(len(parts))]
    sent = True
    for pattern in patterns:
        negate = pattern.startswith("!")
        glob = pattern[1:] if negate else pattern
        glob = glob.rstrip("/")
        if any(fnmatch.fnmatchcase(c, glob) or fnmatch.fnmatchcase(c, glob.replace("**/", "")) for c in candidates):
            sent = negate
    return sent


class DockerConfigTemplateTests(unittest.TestCase):
    def test_docker_template_loads_with_container_settings(self) -> None:
        config = load_config(DEPLOY / "taskboard.docker.toml")
        self.assertEqual(config.http.host, "0.0.0.0")
        self.assertEqual(config.http.port, 8791)
        self.assertEqual(config.http.base_path, "/taskboard")
        self.assertEqual(config.http.max_body_bytes, 1_048_576)
        self.assertEqual(config.sqlite.path, "/data/taskboard.db")
        self.assertEqual(config.sqlite.busy_timeout_ms, 5000)
        # Same compose network: Taskboard reaches Switchboard by service name, not through the public proxy.
        self.assertEqual(config.switchboard.relay_url, "ws://switchboard:8790/ws/relay")
        self.assertEqual(config.switchboard.mcp_url, "http://switchboard:8790/mcp")
        self.assertEqual(config.switchboard.consumer_id, "taskboard")
        self.assertIsNone(config.switchboard.ca_file)
        self.assertEqual(config.intake.auto_ready_sources, ("github",))

    def test_docker_template_holds_no_secret(self) -> None:
        text = (DEPLOY / "taskboard.docker.toml").read_text(encoding="utf-8")
        self.assertNotRegex(text.lower(), r"(?m)^\s*[a-z_]*(token|secret|password)\s*=")


class DockerKitTests(unittest.TestCase):
    def test_image_ships_taskboard_switchboard_and_schemas_as_non_root(self) -> None:
        dockerfile = (DEPLOY / "Dockerfile").read_text(encoding="utf-8")
        self.assertIn("FROM python:3.12-slim", dockerfile)
        self.assertIn("PYTHONPATH=/app/src", dockerfile)
        self.assertRegex(dockerfile, r"(?m)^WORKDIR /app$")
        copies = sorted(line.strip() for line in dockerfile.splitlines() if line.strip().startswith("COPY"))
        self.assertEqual(
            copies,
            [
                "COPY schemas/taskboard /app/schemas/taskboard",
                "COPY src/switchboard /app/src/switchboard",
                "COPY src/taskboard /app/src/taskboard",
            ],
        )
        self.assertNotIn("agent_runtime", dockerfile)
        self.assertIn("--uid 10001", dockerfile)
        self.assertRegex(dockerfile, r"(?m)^USER taskboard$")
        self.assertIn('VOLUME ["/data"]', dockerfile)
        self.assertIn("/etc/taskboard", dockerfile)
        self.assertRegex(dockerfile, r"(?m)^EXPOSE 8791$")
        self.assertRegex(dockerfile, r"(?m)^STOPSIGNAL SIGTERM$")
        healthcheck = dockerfile.split("HEALTHCHECK", 1)[1].split("\n\n", 1)[0]
        self.assertIn("urllib.request", healthcheck)
        self.assertIn("http://127.0.0.1:8791/healthz", healthcheck)
        self.assertIn(
            'ENTRYPOINT ["python3", "-m", "taskboard.app", "--config", "/etc/taskboard/taskboard.toml"]',
            dockerfile,
        )

    def test_build_context_sends_only_the_stdlib_runtime(self) -> None:
        patterns = [
            line.strip()
            for line in (DEPLOY / "Dockerfile.dockerignore").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
        for rel in (
            "src/taskboard/app/main.py",
            "src/switchboard/app.py",
            "schemas/taskboard/work_ready.metadata.schema.json",
        ):
            self.assertTrue(dockerignore_sends(patterns, rel), rel)
        for rel in (
            "src/agent_runtime/app/main.py",
            "tests/test_taskboard_deploy.py",
            "docs/architecture.md",
            ".env",
            "deploy/taskboard/.env",
            "taskboard.db",
            ".venv/bin/python",
            "src/taskboard/app/__pycache__/main.cpython-312.pyc",
        ):
            self.assertFalse(dockerignore_sends(patterns, rel), rel)

    def test_image_layout_runs_taskboard_without_the_rest_of_the_repo(self) -> None:
        # What the Dockerfile copies is enough: the service imports, validates schemas and answers /healthz.
        with tempfile.TemporaryDirectory(prefix="tb_image_") as tmp:
            app = Path(tmp) / "app"
            for rel in ("src/taskboard", "src/switchboard", "schemas/taskboard"):
                shutil.copytree(REPO_ROOT / rel, app / rel, ignore=shutil.ignore_patterns("__pycache__"))
            probe = (
                "import taskboard.app.main, taskboard.adapters.json_schema as s;"
                "s.load_schema('work_ready.metadata.schema.json');"
                "import sys; print(sorted(m.split('.')[0] for m in sys.modules if m.split('.')[0] in "
                "('agent_runtime', 'scripts', 'tests')))"
            )
            result = subprocess.run(
                [sys.executable, "-I", "-c", f"import sys; sys.path.insert(0, {str(app / 'src')!r}); {probe}"],
                cwd=tmp,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), "[]")

    def test_env_example_lists_names_without_values(self) -> None:
        lines = [
            line
            for line in (DEPLOY / ".env.example").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
        self.assertEqual(lines, ["SWITCHBOARD_RELAY_TOKEN=", "SWITCHBOARD_MCP_TOKEN=", "TASKBOARD_MCP_TOKEN="])

    def test_compose_updates_each_service_on_its_own_ref_and_config_hash(self) -> None:
        compose = (DEPLOY / "docker-compose.yml").read_text(encoding="utf-8")
        services = compose_services(compose)
        self.assertEqual(set(services), {"switchboard", "taskboard", "caddy"})
        sb, tb, caddy = services["switchboard"], services["taskboard"], services["caddy"]

        self.assertIn("#${SWITCHBOARD_REF:?", sb)
        self.assertIn("dockerfile: deploy/switchboard/Dockerfile", sb)
        self.assertIn("#${TASKBOARD_REF:?", tb)
        self.assertIn("dockerfile: deploy/taskboard/Dockerfile", tb)
        self.assertNotIn("TASKBOARD_REF", sb)
        self.assertNotIn("SWITCHBOARD_REF", tb)
        # Lists, not mappings: a mapping stops Dokploy from injecting its Traefik labels.
        self.assertIn('labels: ["switchboard.config-sha=${SWITCHBOARD_CONFIG_SHA', sb)
        self.assertIn('labels: ["taskboard.config-sha=${TASKBOARD_CONFIG_SHA', tb)
        self.assertNotIn("TASKBOARD_CONFIG_SHA", sb)
        self.assertNotIn("SWITCHBOARD_CONFIG_SHA", tb)

        # Longer than Switchboard's shutdown_timeout_seconds, here and in the raw compose of the README.
        self.assertIn("stop_grace_period: 20s", sb)
        self.assertIn("       stop_grace_period: 20s\n", (DEPLOY / "README.md").read_text(encoding="utf-8"))

        self.assertIn("/etc/taskboard/taskboard.toml", tb)
        self.assertIn("taskboard_data:/data", tb)
        for name in ("SWITCHBOARD_RELAY_TOKEN", "SWITCHBOARD_MCP_TOKEN", "TASKBOARD_MCP_TOKEN"):
            self.assertRegex(tb, rf"{name}: \$\{{{name}:\?")
        self.assertIn("condition: service_healthy", tb)

        # Only the optional Caddy publishes host ports.
        self.assertNotIn("ports:", sb)
        self.assertNotIn("ports:", tb)
        self.assertNotIn("8790:", compose)
        self.assertNotIn("8791:", compose)
        self.assertIn('profiles: ["caddy"]', caddy)
        self.assertIn('"443:443"', caddy)
        self.assertIn("docker compose up -d --build --no-deps taskboard", compose)

    def test_caddy_example_routes_both_sub_paths_on_one_domain(self) -> None:
        caddyfile = (DEPLOY / "Caddyfile.example").read_text(encoding="utf-8")
        self.assertIn("hub.example.com", caddyfile)
        self.assertRegex(caddyfile, r"handle /switchboard/\*\s*\{\s*reverse_proxy switchboard:8790")
        self.assertRegex(caddyfile, r"handle /taskboard/\*\s*\{\s*reverse_proxy taskboard:8791")
        self.assertNotRegex(caddyfile, r"(?m)^\s*handle_path")
        self.assertIn("deploy/taskboard", (SWITCHBOARD_DEPLOY / "Caddyfile.example").read_text(encoding="utf-8"))
        self.assertIn("deploy/taskboard/README.md", (SWITCHBOARD_DEPLOY / "README.md").read_text(encoding="utf-8"))

    @unittest.skipUnless(shutil.which("git"), "git not installed")
    def test_local_secrets_and_instances_are_ignored(self) -> None:
        def ignored(rel: str) -> bool:
            result = subprocess.run(["git", "check-ignore", "-q", "--no-index", rel], cwd=REPO_ROOT, check=False)
            return result.returncode == 0

        for rel in ("deploy/taskboard/.env", "deploy/taskboard/taskboard.toml", "deploy/taskboard/Caddyfile"):
            self.assertTrue(ignored(rel), rel)
        for rel in (
            "deploy/taskboard/.env.example",
            "deploy/taskboard/taskboard.docker.toml",
            "deploy/taskboard/Caddyfile.example",
            "deploy/taskboard/docker-compose.yml",
        ):
            self.assertFalse(ignored(rel), rel)


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


def sha12(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()[:12]


SWITCHBOARD_CUT_OVER = """
[http]
host = "0.0.0.0"
port = 8790
base_path = "/switchboard"

[outbound]
approval_channel = "feishu"
approval_target = "oc_ops"

[relay]
enabled = true
transport = "hybrid"
"""

SWITCHBOARD_DIRECT = (
    SWITCHBOARD_CUT_OVER
    + """
[routing.event_consumers]
"work.opened" = ["hibernaut"]
"work.updated" = ["hibernaut"]
"work.closed" = ["hibernaut"]
"execution.reported" = ["switchboard.outbound"]
"""
)


class RenderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="tb_render_")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def write(self, name: str, text: str) -> Path:
        path = self.root / name
        path.write_text(text, encoding="utf-8")
        return path

    def test_hashes_template_and_writes_a_private_token_file(self) -> None:
        token_file = self.root / "taskboard.token"
        code, out, err = run_prepare("render", "--token-file", str(token_file), "--taskboard-ref", "abc123")
        self.assertEqual(code, 0, err)
        content = token_file.read_text(encoding="utf-8")
        # A dotenv line: `set -a; . <file>` exports it for smoke, or paste it into .env / Dokploy.
        self.assertRegex(content, r"^TASKBOARD_MCP_TOKEN=[A-Za-z0-9_-]{43,}\n$")
        token = content.strip().split("=", 1)[1]
        self.assertEqual(stat.S_IMODE(token_file.stat().st_mode), 0o600)
        # Masked by default: the token itself is only in the file.
        self.assertNotIn(token, out)
        self.assertIn(f"TASKBOARD_MCP_TOKEN={'*' * 8}{token[-4:]}", out)
        digest = sha12(DEPLOY / "taskboard.docker.toml")
        self.assertIn(f"TASKBOARD_CONFIG_SHA={digest}", out)
        self.assertIn(f'labels: ["taskboard.config-sha={digest}"]', out)
        self.assertIn("TASKBOARD_REF=abc123", out)
        self.assertNotIn("switchboard.config-sha=", out)

    def test_show_token_prints_it_and_each_run_generates_a_new_one(self) -> None:
        tokens = []
        for _ in range(2):
            code, out, err = run_prepare("render", "--show-token")
            self.assertEqual(code, 0, err)
            [line] = [ln for ln in out.splitlines() if ln.startswith("TASKBOARD_MCP_TOKEN=")]
            tokens.append(line.split("=", 1)[1])
        self.assertNotEqual(tokens[0], tokens[1])
        self.assertTrue(all(len(t) >= 43 and "*" not in t for t in tokens))

    def test_a_generated_token_must_go_somewhere(self) -> None:
        code, out, err = run_prepare("render")
        self.assertEqual(code, 2)
        self.assertIn("--token-file", err)
        self.assertNotIn("TASKBOARD_MCP_TOKEN=", out)
        code, out, err = run_prepare("render", "--no-token")
        self.assertEqual(code, 0, err)
        self.assertNotIn("TASKBOARD_MCP_TOKEN=", out)

    def test_token_file_is_never_written_inside_tracked_repo_paths_or_over_a_file(self) -> None:
        for target in (DEPLOY / "taskboard.token", REPO_ROOT / "taskboard.token", REPO_ROOT / "docs" / "t.token"):
            with self.subTest(target=target):
                code, _, err = run_prepare("render", "--token-file", str(target))
                self.assertEqual(code, 2)
                self.assertIn("repository", err)
                self.assertFalse(target.exists())
        existing = self.write("existing.token", "keep-me\n")
        code, _, err = run_prepare("render", "--token-file", str(existing))
        self.assertEqual(code, 2)
        self.assertIn("exists", err)
        self.assertEqual(existing.read_text(encoding="utf-8"), "keep-me\n")

    def test_a_failed_check_writes_no_token_so_the_rerun_can(self) -> None:
        bad = self.write("sb-root.toml", SWITCHBOARD_CUT_OVER.replace('base_path = "/switchboard"\n', ""))
        token_file = self.root / "taskboard.token"
        code, out, _ = run_prepare("render", "--token-file", str(token_file), "--switchboard-toml", str(bad))
        self.assertEqual(code, 1)
        self.assertFalse(token_file.exists())
        self.assertNotIn("TASKBOARD_MCP_TOKEN=", out)
        good = self.write("sb-cut.toml", SWITCHBOARD_CUT_OVER)
        code, _, err = run_prepare("render", "--token-file", str(token_file), "--switchboard-toml", str(good))
        self.assertEqual(code, 0, err)
        self.assertTrue(token_file.exists())

    def test_gitignored_instance_path_in_the_repo_is_allowed(self) -> None:
        if not shutil.which("git"):
            self.skipTest("git not installed")
        target = REPO_ROOT / f".env.tb-render-test-{os.getpid()}"
        self.addCleanup(lambda: target.unlink(missing_ok=True))
        code, _, err = run_prepare("render", "--token-file", str(target))
        self.assertEqual(code, 0, err)
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)

    def test_switchboard_config_hash_and_cut_over_state(self) -> None:
        cut = self.write("sb-cut.toml", SWITCHBOARD_CUT_OVER)
        code, out, err = run_prepare("render", "--no-token", "--switchboard-toml", str(cut), "--switchboard-ref", "def456")
        self.assertEqual(code, 0, err)
        self.assertIn(f"SWITCHBOARD_CONFIG_SHA={sha12(cut)}", out)
        self.assertIn(f'labels: ["switchboard.config-sha={sha12(cut)}"]', out)
        self.assertIn("SWITCHBOARD_REF=def456", out)
        self.assertIn('[ok] switchboard [http] base_path = "/switchboard"', out)
        self.assertIn("[ok] switchboard routing: work.* and execution.reported go through Taskboard", out)

        direct = self.write("sb-direct.toml", SWITCHBOARD_DIRECT)
        code, out, err = run_prepare("render", "--no-token", "--switchboard-toml", str(direct))
        self.assertEqual(code, 0, err)
        self.assertIn("[todo] switchboard routing: [routing.event_consumers] still sends", out)
        self.assertIn("work.opened", out)

    def test_wrong_prefix_or_bad_config_fails(self) -> None:
        no_prefix = self.write("sb-root.toml", SWITCHBOARD_CUT_OVER.replace('base_path = "/switchboard"\n', ""))
        code, out, _ = run_prepare("render", "--no-token", "--switchboard-toml", str(no_prefix))
        self.assertEqual(code, 1)
        self.assertIn('[fail] switchboard [http] base_path is "" (expected "/switchboard")', out)

        tb = self.write("tb.toml", (DEPLOY / "taskboard.docker.toml").read_text(encoding="utf-8").replace('"/taskboard"', '""'))
        code, out, _ = run_prepare("render", "--no-token", "--template", str(tb))
        self.assertEqual(code, 1)
        self.assertIn('[fail] taskboard [http] base_path is "" (expected "/taskboard")', out)

        off = self.write("sb-relay-off.toml", SWITCHBOARD_CUT_OVER.replace("enabled = true", "enabled = false").replace('transport = "hybrid"', ""))
        code, out, _ = run_prepare("render", "--no-token", "--switchboard-toml", str(off))
        self.assertEqual(code, 1)
        self.assertIn("[fail] switchboard [relay] enabled = false", out)

        broken = self.write("broken.toml", "[http]\nport = 'x'\n")
        code, _, err = run_prepare("render", "--no-token", "--template", str(broken))
        self.assertEqual(code, 2)
        self.assertIn("http.port", err)


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


class _OpenMCP(BaseHTTPRequestHandler):
    # A misconfigured Taskboard: answers /mcp without a token and lists one tool too few.
    def log_message(self, format: str, *args: Any) -> None:
        pass

    def _reply(self, payload: dict[str, Any]) -> None:
        raw = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:
        self._reply({"status": "ok"})

    def do_POST(self) -> None:
        request = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)))
        tools = [{"name": name} for name in TOOL_NAMES[:-1]]
        result: dict[str, Any] = {"tools": tools} if request["method"] == "tools/list" else {"protocolVersion": "x"}
        self._reply({"jsonrpc": "2.0", "id": request.get("id"), "result": result})


@contextmanager
def serve(handler: type[BaseHTTPRequestHandler]) -> Iterator[str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


class SmokeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="tb_smoke_")
        self.addCleanup(self.tmp.cleanup)
        self.switchboard: Harness = make_harness(http={"base_path": "/switchboard"})
        self.addCleanup(self.switchboard.close)
        self.taskboard = self.start_taskboard(token=TOKEN)

    def start_taskboard(self, *, token: str | None) -> TaskboardService:
        config: TaskboardConfig = parse_config(
            {
                "http": {"host": "127.0.0.1", "port": 0, "base_path": "/taskboard"},
                "sqlite": {"path": str(Path(self.tmp.name) / f"taskboard-{id(token)}.db")},
            },
            base_dir=Path(self.tmp.name),
        )
        service = build_service(config, mcp_token=token, bus=None)
        service.start()
        self.addCleanup(service.stop)
        return service

    @contextmanager
    def proxy(self, **backends: str) -> Iterator[str]:
        handler = type("Proxy", (_PathProxy,), {"backends": backends})
        with serve(handler) as url:
            yield url

    def taskboard_url(self, service: TaskboardService) -> str:
        assert service.http is not None
        return service.http.base_url

    def smoke(self, base_url: str, *extra: str, token: str | None = TOKEN) -> tuple[int, str]:
        code, out, err = run_prepare("smoke", "--base-url", base_url, *extra, env={"TASKBOARD_MCP_TOKEN": token})
        return code, out + err

    def test_all_checks_pass_through_one_domain(self) -> None:
        with self.proxy(switchboard=self.switchboard.url, taskboard=self.taskboard_url(self.taskboard)) as url:
            code, out = self.smoke(url + "/")
        self.assertEqual(code, 0, out)
        for line in (
            "[ok] GET /switchboard/healthz -> 200",
            "[ok] GET /taskboard/healthz -> 200",
            "[ok] POST /taskboard/mcp without a token -> 401",
            "[ok] initialize",
            f"[ok] tools/list: {len(TOOL_NAMES)} tb_* tools",
        ):
            self.assertIn(line, out)
        self.assertNotIn(TOKEN, out)

    def test_cli_reads_the_token_from_the_named_environment_variable(self) -> None:
        with self.proxy(switchboard=self.switchboard.url, taskboard=self.taskboard_url(self.taskboard)) as url:
            env = {k: v for k, v in os.environ.items() if not k.startswith(("TASKBOARD_", "SWITCHBOARD_"))}
            env["TB_SMOKE_TOKEN"] = TOKEN
            result = subprocess.run(
                [sys.executable, str(SCRIPT), "smoke", "--base-url", url, "--token-env", "TB_SMOKE_TOKEN"],
                env=env,
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertNotIn(TOKEN, result.stdout + result.stderr)

    def test_wrong_token_and_missing_token_fail(self) -> None:
        with self.proxy(switchboard=self.switchboard.url, taskboard=self.taskboard_url(self.taskboard)) as url:
            code, out = self.smoke(url, token="wrong-token")
            self.assertEqual(code, 1, out)
            self.assertIn("[fail] initialize", out)
            self.assertIn("401", out)
            self.assertNotIn("wrong-token", out)
            code, out = self.smoke(url, token=None)
            self.assertEqual(code, 1, out)
            self.assertIn("TASKBOARD_MCP_TOKEN is not set", out)

    def test_missing_route_fails_its_health_check(self) -> None:
        with self.proxy(switchboard=self.switchboard.url) as url:
            code, out = self.smoke(url)
        self.assertEqual(code, 1, out)
        self.assertIn("[ok] GET /switchboard/healthz -> 200", out)
        self.assertIn("[fail] GET /taskboard/healthz -> 404", out)

    def test_wrong_prefix_fails(self) -> None:
        with self.proxy(switchboard=self.switchboard.url, taskboard=self.taskboard_url(self.taskboard)) as url:
            code, out = self.smoke(url, "--switchboard-prefix", "/sb")
        self.assertEqual(code, 1, out)
        self.assertIn("[fail] GET /sb/healthz -> 404", out)

    def test_open_mcp_and_missing_tool_fail(self) -> None:
        with serve(_OpenMCP) as open_mcp, self.proxy(switchboard=self.switchboard.url, taskboard=open_mcp) as url:
            code, out = self.smoke(url)
        self.assertEqual(code, 1, out)
        self.assertIn("[fail] POST /taskboard/mcp without a token -> 200 (expected 401", out)
        self.assertIn(f"[fail] tools/list: missing {TOOL_NAMES[-1]}", out)

    def test_unreachable_host_fails_with_a_diagnostic(self) -> None:
        code, out = self.smoke("http://127.0.0.1:9", "--timeout", "2")
        self.assertEqual(code, 1, out)
        self.assertIn("[fail] GET /switchboard/healthz", out)
        self.assertRegex(out, r"refused|unreachable|URLError")


class ScriptDependencyTests(unittest.TestCase):
    def test_script_only_imports_stdlib_and_repo_packages(self) -> None:
        source = SCRIPT.read_text(encoding="utf-8")
        imported = set(re.findall(r"(?m)^\s*(?:from|import) ([a-zA-Z_][\w]*)", source))
        allowed = set(sys.stdlib_module_names) | {"switchboard", "taskboard", "__future__"}
        self.assertLessEqual(imported, allowed)


if __name__ == "__main__":
    unittest.main()
