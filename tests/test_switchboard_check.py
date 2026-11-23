from __future__ import annotations

import base64
import contextlib
import io
import json
import os
import re
import socket
import subprocess
import sys
import tempfile
import unittest
from collections.abc import Mapping
from pathlib import Path
from unittest import mock

from switchboard.app import main
from switchboard.config import ConfigError, load_config, parse_config

REPO = Path(__file__).resolve().parent.parent

# Structurally a PKCS#1 key (nine DER integers), so it parses; it signs nothing.
PKCS1_PEM = (
    "-----BEGIN RSA PRIVATE KEY-----\n"
    + base64.b64encode(b"\x30\x1b" + bytes([0x02, 0x01, 0x05]) * 9).decode()
    + "\n-----END RSA PRIVATE KEY-----\n"
)

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


class CheckHarness(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory(prefix="sb_check_")
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.path = self.root / "switchboard.toml"

    def check(self, toml: str, env: Mapping[str, str] | None = None) -> tuple[int, str, str]:
        self.path.write_text(toml, encoding="utf-8")
        out, err = io.StringIO(), io.StringIO()
        with (
            mock.patch.dict(os.environ, dict(env or {}), clear=True),
            contextlib.redirect_stdout(out),
            contextlib.redirect_stderr(err),
        ):
            code = main(["--config", str(self.path), "--check"])
        return code, out.getvalue(), err.getvalue()


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

    def test_existing_directory_gets_no_database(self) -> None:
        code, out, err = self.check(f'[sqlite]\npath = "{self.root / "sb.db"}"\n')
        self.assertEqual((code, err), (0, ""))
        self.assertEqual([p.name for p in self.root.iterdir()], ["switchboard.toml"])
        self.assertIn("routing rules: 0", out)

    def test_shipped_templates_pass_the_check(self) -> None:
        for rel in ("switchboard.example.toml", "deploy/switchboard/switchboard.docker.toml"):
            with self.subTest(rel):
                toml = (REPO / rel).read_text(encoding="utf-8")
                code, _, err = self.check(toml, {"SWITCHBOARD_RELAY_WEBHOOK_SECRET": "test-relay"})
                self.assertEqual((code, err), (0, ""))

    def test_separate_listeners_that_do_not_overlap(self) -> None:
        relay = {"SWITCHBOARD_RELAY_WEBHOOK_SECRET": "test-relay"}
        for http, hub in (("127.0.0.1", "127.0.0.2"), ("localhost", "10.0.0.5")):
            with self.subTest(http=http, relay=hub):
                code, _, err = self.check(
                    f'[http]\nhost = "{http}"\nport = 9000\n'
                    f'[relay]\nenabled = true\nlisten_host = "{hub}"\nlisten_port = 9000\n',
                    relay,
                )
                self.assertEqual((code, err), (0, ""))
        # Port 0 is picked by the system, so it never clashes.
        code, _, err = self.check("[http]\nport = 0\n[relay]\nenabled = true\nlisten_port = 0\n", relay)
        self.assertEqual((code, err), (0, ""))

    def test_usable_api_bases(self) -> None:
        for url in ("https://api.github.com", "http://127.0.0.1:9", "https://ghe.example.com:8443/api/v3/", "http://[::1]:80"):
            with self.subTest(url):
                code, _, err = self.check(f'[outbound]\ngithub_api_base = "{url}"\n[feishu]\napi_base = "{url}"\n')
                self.assertEqual((code, err), (0, ""))


class InvalidConfigTests(CheckHarness):
    def test_each_error_names_its_field_and_the_file(self) -> None:
        cases: list[tuple[str, str]] = [
            # Missing or empty required values.
            ('[sqlite]\npath = ""', "sqlite.path"),
            ('[http]\nhost = ""', "http.host"),
            ('[routing]\ndefault_scope = ""', "routing.default_scope"),
            ('[relay]\ntransport = "relay"', "relay.enabled"),
            ('[delivery]\ndead_letter_alert_channel = "feishu"', "delivery.dead_letter_alert_target"),
            ('[delivery]\ndead_letter_alert_target = "oc_x"', "delivery.dead_letter_alert_channel"),
            ('[outbound]\nnotify_channel = "feishu"', "outbound.notify_target"),
            ('[relay]\nenabled = true\nlisten_host = "0.0.0.0"', "relay.listen_port"),
            # Ports.
            ("[http]\nport = 70000", "http.port"),
            ("[http]\nport = -1", "http.port"),
            ("[http]\nport = true", "http.port"),
            ('[http]\nport = "http"', "http.port"),
            ("[http]\nport = 80.5", "http.port"),
            ("[relay]\nenabled = true\nlisten_port = 65536", "relay.listen_port"),
            ("[http]\nport = 9000\n[relay]\nenabled = true\nlisten_port = 9000", "relay.listen_port"),
            # Listen address, body limit, SQLite.
            ("[http]\nhost = 127", "http.host"),
            ("[http]\nmax_body_bytes = 0", "http.max_body_bytes"),
            ("[sqlite]\npath = 5", "sqlite.path"),
            ("[sqlite]\nbusy_timeout_ms = -1", "sqlite.busy_timeout_ms"),
            # Routing.
            ('[routing]\nunmatched_policy = "drop"', "routing.unmatched_policy"),
            ('[routing.github_repos]\n"a/b" = ""', "routing.github_repos"),
            ('[routing.feishu_chats]\n"oc" = 5', "routing.feishu_chats"),
            ('[routing.event_consumers]\n"work.started" = ["x"]', "routing.event_consumers"),
            # Backoff and rate limit.
            ("[delivery]\nretry_backoff_seconds = []", "delivery.retry_backoff_seconds"),
            ("[delivery]\nretry_backoff_seconds = [5, -1]", "delivery.retry_backoff_seconds"),
            ("[delivery]\nretry_backoff_seconds = [5, true]", "delivery.retry_backoff_seconds"),
            ("[delivery]\nretry_backoff_seconds = [inf]", "delivery.retry_backoff_seconds"),
            ("[delivery]\nretry_backoff_seconds = [1e300]", "delivery.retry_backoff_seconds"),
            ('[outbound]\nretry_backoff_seconds = ["soon"]', "outbound.retry_backoff_seconds"),
            ("[outbound]\nretry_backoff_seconds = 5", "outbound.retry_backoff_seconds"),
            ("[outbound]\nrate_limit_per_target_rpm = 0", "outbound.rate_limit_per_target_rpm"),
            ("[outbound]\nmax_retry_attempts = 0", "outbound.max_retry_attempts"),
            ("[delivery]\nmax_parallel = 0", "delivery.max_parallel"),
            ('[relay]\nenabled = "false"', "relay.enabled"),
            ("[relay]\nack_timeout_sec = true", "relay.ack_timeout_sec"),
            ("[relay]\nheartbeat_sec = nan", "relay.heartbeat_sec"),
            ('[outbound]\ngithub_api_base = "api.github.com"', "outbound.github_api_base"),
            # Shutdown timeout, including one swallowed by the table it was written under.
            ("shutdown_timeout_seconds = 0", "shutdown_timeout_seconds"),
            ("shutdown_timeout_seconds = -0.5", "shutdown_timeout_seconds"),
            ("shutdown_timeout_seconds = true", "shutdown_timeout_seconds"),
            ("shutdown_timeout_seconds = false", "shutdown_timeout_seconds"),
            ('shutdown_timeout_seconds = "10"', "shutdown_timeout_seconds"),
            ("shutdown_timeout_seconds = inf", "shutdown_timeout_seconds"),
            ("shutdown_timeout_seconds = nan", "shutdown_timeout_seconds"),
            ("[http]\nport = 8790\nshutdown_timeout_seconds = 5", "shutdown_timeout_seconds"),
        ]
        for toml, field in cases:
            with self.subTest(toml=toml):
                code, out, err = self.check(toml + "\n")
                self.assertNotEqual(code, 0)
                self.assertEqual(out, "")
                self.assertIn(field, err)
                self.assertIn(str(self.path), err)

    def test_unusable_api_bases_name_their_field(self) -> None:
        bad = (
            "https://", "https:///path", "https://:443", "https://api.github.com:http", "https://api.github.com:70000",
            "https://api.github.com:0", "https://[::1", "https://a b.example", "https://a..example",
            "https://api.github.com?x=1", "https://api.github.com#frag", "ftp://api.github.com", "api.github.com",
        )
        for table, name in (("feishu", "api_base"), ("outbound", "github_api_base")):
            for url in bad:
                with self.subTest(table=table, url=url):
                    code, _, err = self.check(f'[{table}]\n{name} = "{url}"\n')
                    self.assertEqual(code, 1)
                    self.assertIn(f"{table}.{name}", err)

    def test_sqlite_path_that_can_never_be_a_database_file(self) -> None:
        data = self.root / "data"
        data.mkdir()
        (self.root / "plain").write_text("", encoding="utf-8")
        fifo = self.root / "fifo"
        os.mkfifo(fifo)
        paths = (data, f"{data}/", f"{self.root}/new/", self.root / "plain" / "sb.db", fifo, "sb\\u0000.db")
        for path in paths:
            with self.subTest(path=str(path)):
                code, _, err = self.check(f'[sqlite]\npath = "{path}"\n')
                self.assertEqual(code, 1)
                self.assertIn("sqlite.path", err)
        self.assertEqual(list(data.iterdir()), [])
        self.assertEqual(self.check(f'[sqlite]\npath = "{data / "new" / "sb.db"}"\n')[0], 0)

    def test_overlapping_listeners_on_one_port(self) -> None:
        for http, hub in (
            ("0.0.0.0", "127.0.0.1"),
            ("127.0.0.1", "0.0.0.0"),
            ("0.0.0.0", "0.0.0.0"),
            ("0.0.0.0", "hub.internal"),
            ("hub.internal", "0.0.0.0"),
            ("0", "10.0.0.5"),
            ("localhost", "127.0.0.1"),
            ("127.0.0.1", "LOCALHOST"),
            ("127.1", "127.0.0.1"),
            ("Hub.internal", "hub.internal"),
        ):
            with self.subTest(http=http, relay=hub):
                code, out, err = self.check(
                    f'[http]\nhost = "{http}"\nport = 8790\n'
                    f'[relay]\nenabled = true\nlisten_host = "{hub}"\nlisten_port = 8790\n',
                    {"SWITCHBOARD_RELAY_WEBHOOK_SECRET": "test-relay"},
                )
                self.assertEqual((code, out), (1, ""))
                self.assertIn("relay.listen_port", err)

    def test_listen_addresses_the_ipv4_server_cannot_bind(self) -> None:
        for host in ("::", "::1", "999.0.0.1", "1.2.3.4.5"):
            with self.subTest(host):
                code, _, err = self.check(f'[http]\nhost = "{host}"\n')
                self.assertEqual(code, 1)
                self.assertIn("http.host", err)

    def test_enabled_relay_needs_its_token(self) -> None:
        for toml in ('[relay]\nenabled = true\ntransport = "relay"\n', "[relay]\nenabled = true\n"):
            for env in ({}, {"SWITCHBOARD_RELAY_WEBHOOK_SECRET": ""}, {"SWITCHBOARD_RELAY_WEBHOOK_SECRET": "  "}):
                with self.subTest(toml=toml, env=env):
                    code, out, err = self.check(toml, env)
                    self.assertEqual((code, out), (1, ""))
                    self.assertIn("SWITCHBOARD_RELAY_WEBHOOK_SECRET", err)
                    self.assertIn("relay.enabled", err)
        self.assertEqual(self.check("[relay]\nenabled = false\n")[0], 0)

    def test_every_problem_found_after_parsing_is_reported(self) -> None:
        code, _, err = self.check(
            '[delivery]\ndead_letter_alert_channel = "feishu"\n[outbound]\nnotify_channel = "feishu"\n'
        )
        self.assertNotEqual(code, 0)
        self.assertIn("delivery.dead_letter_alert_target", err)
        self.assertIn("outbound.notify_target", err)

    def test_credentials_that_startup_would_reject_or_half_use_are_reported_without_values(self) -> None:
        owners = '[outbound]\ngithub_app_owners = ["kun-g"]\n'
        cases: list[tuple[str, dict[str, str], str]] = [
            ("", {"SWITCHBOARD_GITHUB_APP_ID": "1", "SWITCHBOARD_GITHUB_APP_PRIVATE_KEY": PKCS1_PEM}, "github_app_owners"),
            (
                owners,
                {"SWITCHBOARD_GITHUB_APP_ID": "1", "SWITCHBOARD_GITHUB_APP_PRIVATE_KEY": "not-a-secret!"},
                "SWITCHBOARD_GITHUB_APP_PRIVATE_KEY",
            ),
            (
                owners,
                {"SWITCHBOARD_GITHUB_APP_ID": "1", "SWITCHBOARD_GITHUB_APP_PRIVATE_KEY": "bm90LWEtc2VjcmV0"},
                "SWITCHBOARD_GITHUB_APP_PRIVATE_KEY",
            ),
            (owners, {"SWITCHBOARD_GITHUB_APP_ID": "1"}, "SWITCHBOARD_GITHUB_APP_PRIVATE_KEY"),
            ("", {"SWITCHBOARD_FEISHU_APP_SECRET": "not-a-secret!"}, "SWITCHBOARD_FEISHU_APP_ID"),
        ]
        for toml, env, field in cases:
            with self.subTest(field=field, env=sorted(env)):
                code, _, err = self.check(toml, env)
                self.assertNotEqual(code, 0)
                self.assertIn(field, err)
                self.assertNotIn("not-a-secret!", err)

    def test_unreadable_and_malformed_files(self) -> None:
        for toml in ("[http\n", "port = \n"):
            with self.subTest(toml=toml):
                code, _, err = self.check(toml)
                self.assertNotEqual(code, 0)
                self.assertIn(str(self.path), err)
        missing = self.root / "absent.toml"
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            self.assertNotEqual(main(["--config", str(missing), "--check"]), 0)
        self.assertIn(str(missing), err.getvalue())

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


class ShutdownTimeoutConfigTests(unittest.TestCase):
    def test_default_and_override(self) -> None:
        self.assertEqual(parse_config({}).shutdown_timeout_seconds, 10.0)
        self.assertEqual(parse_config({"shutdown_timeout_seconds": 3}).shutdown_timeout_seconds, 3.0)
        self.assertEqual(load_config(REPO / "switchboard.example.toml").shutdown_timeout_seconds, 10.0)

    def test_values_startup_already_accepted_still_load(self) -> None:
        config = parse_config(
            {
                "http": {"port": "8790"},
                "delivery": {"retry_backoff_seconds": [0, "1.5"], "dead_letter_alert_channel": "feishu"},
                "relay": {"enabled": 1, "listen_port": 0, "heartbeat_sec": "20"},
            }
        )
        self.assertEqual(config.http.port, 8790)
        self.assertEqual(config.delivery.retry_backoff_seconds, (0.0, 1.5))
        self.assertIs(config.relay.enabled, True)
        self.assertEqual((config.relay.listen_port, config.relay.heartbeat_sec), (0, 20.0))

    def test_bool_is_not_a_number(self) -> None:
        with self.assertRaisesRegex(ConfigError, "shutdown_timeout_seconds"):
            parse_config({"shutdown_timeout_seconds": True})


if __name__ == "__main__":
    unittest.main()
