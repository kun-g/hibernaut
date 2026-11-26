from __future__ import annotations

import os
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from taskboard.app.config import TaskboardConfig, parse_config
from taskboard.app.main import TaskboardService, build_service
from tests.support.switchboard_support import Harness, make_harness
from tests.support.taskboard_deploy import DEPLOY, REPO_ROOT, SCRIPT, TOKEN, _PathProxy, run_prepare, serve


class DockerKitTests(unittest.TestCase):

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


class RenderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="tb_render_")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def write(self, name: str, text: str) -> Path:
        path = self.root / name
        path.write_text(text, encoding="utf-8")
        return path

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

    def test_gitignored_instance_path_in_the_repo_is_allowed(self) -> None:
        if not shutil.which("git"):
            self.skipTest("git not installed")
        target = REPO_ROOT / f".env.tb-render-test-{os.getpid()}"
        self.addCleanup(lambda: target.unlink(missing_ok=True))
        code, _, err = run_prepare("render", "--token-file", str(target))
        self.assertEqual(code, 0, err)
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)


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


if __name__ == "__main__":
    unittest.main()
