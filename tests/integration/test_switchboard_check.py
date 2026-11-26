from __future__ import annotations

import base64
import json
import os
import re
import socket
import subprocess
import sys
import unittest

from tests.support.switchboard_check import CheckHarness, PKCS1_PEM, REPO


# Records every side effect --check must not have, then runs the real entry point.
AUDITED_MAIN = """
import json, os, sys

import switchboard.app
hits = []
def hook(event, args):
    if event in ("socket.bind", "socket.connect", "socket.getaddrinfo", "sqlite3.connect", "os.mkdir"):
        hits.append([event, repr(args)])
    elif event == "open" and isinstance(args[2], int) and args[2] & (os.O_WRONLY | os.O_RDWR | os.O_CREAT):
        hits.append([event, repr(args)])
sys.addaudithook(hook)
code = switchboard.app.main(sys.argv[1:])
print("HITS=" + json.dumps(hits))
sys.exit(code)
"""


def clean_env(**extra: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith(("SWITCHBOARD_", "GITHUB_", "FEISHU_"))}
    env["PYTHONPATH"] = str(REPO / "src")
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env.update(extra)
    return env


class ValidConfigTests(CheckHarness):
    def test_valid_config_has_no_side_effects(self) -> None:
        missing = self.root / "data" / "nested"
        # The configured port is taken, so any attempt to bind it would fail the check.
        with socket.create_server(("127.0.0.1", 0)) as taken:
            port = taken.getsockname()[1]
            self.path.write_text(
                f"""
shutdown_timeout_seconds = 3.5

[http]
port = {port}

[sqlite]
path = "{missing / 'switchboard.db'}"

[routing.github_repos]
"kun-g/hibernaut" = "proj:hibernaut"

[routing.feishu_chats]
"oc_ops" = "proj:ops"

[outbound]
github_app_owners = ["kun-g"]

[relay]
enabled = true
transport = "hybrid"
""",
                encoding="utf-8",
            )
            before = sorted(self.root.rglob("*"))
            result = subprocess.run(
                [sys.executable, "-c", AUDITED_MAIN, "--config", str(self.path), "--check"],
                cwd=self.root,
                env=clean_env(
                    SWITCHBOARD_GITHUB_WEBHOOK_SECRET="test-webhook",
                    SWITCHBOARD_GITHUB_APP_ID="4242",
                    SWITCHBOARD_GITHUB_APP_PRIVATE_KEY=base64.b64encode(PKCS1_PEM.encode()).decode(),
                    SWITCHBOARD_FEISHU_APP_ID="cli_test",
                    SWITCHBOARD_FEISHU_APP_SECRET="test-app-secret",
                    SWITCHBOARD_MCP_WEBHOOK_SECRET="test-mcp",
                    SWITCHBOARD_RELAY_WEBHOOK_SECRET="test-relay",
                ),
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout.rpartition("HITS=")[2]), [])
        self.assertEqual(sorted(self.root.rglob("*")), before)
        self.assertFalse((self.root / "data").exists())
        self.assertIn(f"127.0.0.1:{port}", result.stdout)
        self.assertRegex(result.stdout, r"routing rules: 2\b")
        channels = re.search(r"channels: (.*)", result.stdout)
        assert channels is not None
        for channel in ("github", "feishu", "mcp", "relay"):
            self.assertIn(channel, channels.group(1))
        for secret in ("test-webhook", "test-app-secret", "test-mcp", "test-relay"):
            self.assertNotIn(secret, result.stdout + result.stderr)


class InvalidConfigTests(CheckHarness):

    def test_process_exits_non_zero_with_the_reason_on_stderr(self) -> None:
        self.path.write_text("shutdown_timeout_seconds = 0\n", encoding="utf-8")
        result = subprocess.run(
            [sys.executable, "-m", "switchboard.app", "--config", str(self.path), "--check"],
            cwd=self.root,
            env=clean_env(),
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertIn("shutdown_timeout_seconds", result.stderr)
        self.assertIn(str(self.path), result.stderr)
        self.assertNotIn("Traceback", result.stderr)


if __name__ == "__main__":
    unittest.main()
