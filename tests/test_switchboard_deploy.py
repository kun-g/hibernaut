from __future__ import annotations

import re
import shutil
import subprocess
import unittest
from pathlib import Path

from switchboard.config import load_config

REPO_ROOT = Path(__file__).resolve().parents[1]
DEPLOY = REPO_ROOT / "deploy" / "switchboard"


class DockerConfigTemplateTests(unittest.TestCase):
    def test_docker_template_loads_with_container_settings(self) -> None:
        config = load_config(DEPLOY / "switchboard.docker.toml")
        self.assertEqual(config.http.host, "0.0.0.0")
        self.assertEqual(config.http.port, 8790)
        self.assertEqual(config.sqlite.path, "/data/switchboard.db")
        self.assertIs(config.relay.enabled, True)
        self.assertEqual(config.relay.transport, "hybrid")
        self.assertEqual(config.relay.path, "/ws/relay")
        # The relay must share the [http] port, the only one Caddy forwards to.
        self.assertIsNone(config.relay.listen_port)
        # A public repo must not let anyone start a Run by opening an issue.
        self.assertEqual(config.github.require_label, "hibernaut")
        # No Taskboard is deployed, so work events must reach Hibernaut directly.
        for event_type in ("work.opened", "work.updated", "work.closed"):
            self.assertEqual(config.routing.event_consumers[event_type], ("hibernaut",))
        self.assertEqual(config.routing.event_consumers["execution.reported"], ("switchboard.outbound",))


class DockerKitTests(unittest.TestCase):
    def test_image_ships_only_switchboard_as_non_root(self) -> None:
        dockerfile = (DEPLOY / "Dockerfile").read_text(encoding="utf-8")
        self.assertIn("FROM python:3.12-slim", dockerfile)
        copies = [line for line in dockerfile.splitlines() if line.strip().startswith("COPY")]
        self.assertEqual(copies, ["COPY src/switchboard /app/src/switchboard"])
        self.assertNotIn("agent_runtime", dockerfile)
        self.assertRegex(dockerfile, r"(?m)^USER switchboard$")
        self.assertIn("http://127.0.0.1:8790/healthz", dockerfile)
        self.assertIn(
            'ENTRYPOINT ["python3", "-m", "switchboard.app", "--config", '
            '"/etc/switchboard/switchboard.toml"]',
            dockerfile,
        )

    def test_switchboard_service_publishes_no_host_port(self) -> None:
        compose = (DEPLOY / "docker-compose.yml").read_text(encoding="utf-8")
        services = re.split(r"(?m)^  (?=\w)", compose.split("\nservices:\n", 1)[1].split("\nvolumes:\n", 1)[0])
        blocks = {block.split(":", 1)[0]: block for block in services if block.strip()}
        self.assertEqual(set(blocks), {"switchboard", "caddy"})
        self.assertNotIn("ports:", blocks["switchboard"])
        self.assertNotIn("8790:", compose)
        self.assertIn('"80:80"', blocks["caddy"])
        self.assertIn('"443:443"', blocks["caddy"])
        self.assertIn("condition: service_healthy", blocks["caddy"])

    def test_env_example_lists_names_without_values(self) -> None:
        lines = [
            line
            for line in (DEPLOY / ".env.example").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
        self.assertEqual(
            lines,
            [
                "SWITCHBOARD_RELAY_WEBHOOK_SECRET=",
                "SWITCHBOARD_MCP_WEBHOOK_SECRET=",
                "SWITCHBOARD_GITHUB_WEBHOOK_SECRET=",
                "SWITCHBOARD_GITHUB_TOKEN=",
                "SWITCHBOARD_FEISHU_WEBHOOK_SECRET=",
                "SWITCHBOARD_FEISHU_APP_ID=",
                "SWITCHBOARD_FEISHU_APP_SECRET=",
            ],
        )

    def test_caddyfile_example_proxies_to_switchboard_with_placeholder_domain(self) -> None:
        caddyfile = (DEPLOY / "Caddyfile.example").read_text(encoding="utf-8")
        self.assertIn("sb.example.com", caddyfile)
        self.assertIn("reverse_proxy switchboard:8790", caddyfile)

    @unittest.skipUnless(shutil.which("git"), "git not installed")
    def test_local_secrets_and_instances_are_ignored(self) -> None:
        def ignored(rel: str) -> bool:
            result = subprocess.run(
                ["git", "check-ignore", "-q", "--no-index", rel], cwd=REPO_ROOT, check=False
            )
            return result.returncode == 0

        for rel in (
            "deploy/switchboard/.env",
            "deploy/switchboard/switchboard.toml",
            "deploy/switchboard/Caddyfile",
        ):
            self.assertTrue(ignored(rel), rel)
        for rel in (
            "deploy/switchboard/.env.example",
            "deploy/switchboard/switchboard.docker.toml",
            "deploy/switchboard/Caddyfile.example",
        ):
            self.assertFalse(ignored(rel), rel)


if __name__ == "__main__":
    unittest.main()
