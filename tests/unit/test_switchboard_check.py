from __future__ import annotations

import contextlib
import io
import os
import unittest

from switchboard.app import main
from switchboard.config import ConfigError, load_config, parse_config
from tests.support.switchboard_check import CheckHarness, PKCS1_PEM, REPO


class ValidConfigTests(CheckHarness):

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

    def test_usable_listen_names(self) -> None:
        for host in ("localhost", "LOCALHOST", "localhost.", "my_service", "a-b.example", "0x7f.1", "b\u00fccher.example"):
            with self.subTest(host):
                code, _, err = self.check(f'[http]\nhost = "{host}"\n')
                self.assertEqual(code, 0, err)

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
            "https://a$b.example", "https://-a.example", "https://a%20b.example", "https://999.0.0.1",
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
        long_label, long_name = "a" * 64, ".".join(["a" * 60] * 5)
        bad = (
            "::", "::1", "999.0.0.1", "1.2.3.4.5", "127.0.0.1:8790", "localhost/", "local host", " localhost",
            "127.0.0.1 ", "127.0.0.1 x", "http://localhost", "a..b", ".a", "-a.example", "a-.example", "a$b", "*",
            "a\\u0000b", "a\\tb", long_label, long_name, "b\u00fccher\u3002/",
        )
        for host in bad:
            with self.subTest(field="http.host", host=host):
                code, out, err = self.check(f'[http]\nhost = "{host}"\n')
                self.assertEqual((code, out), (1, ""))
                self.assertIn("http.host", err)
                self.assertNotIn("relay.listen_host", err)
            with self.subTest(field="relay.listen_host", host=host):
                code, out, err = self.check(
                    f'[relay]\nenabled = true\nlisten_host = "{host}"\nlisten_port = 8791\n',
                    {"SWITCHBOARD_RELAY_WEBHOOK_SECRET": "test-relay"},
                )
                self.assertEqual((code, out), (1, ""))
                self.assertIn("relay.listen_host", err)
                self.assertNotIn("http.host", err)

    def test_consumer_listed_twice_for_one_event_type(self) -> None:
        for consumers in ('["hibernaut", "hibernaut"]', '["hibernaut", "taskboard", "hibernaut"]'):
            with self.subTest(consumers):
                toml = f'[routing.event_consumers]\n"work.ready" = {consumers}\n'
                code, out, err = self.check(toml)
                self.assertEqual((code, out), (1, ""))
                self.assertIn("routing.event_consumers.'work.ready'", err)
                self.assertIn("'hibernaut'", err)
                with self.assertRaises(ConfigError):
                    load_config(self.path)
        # The same consumer may take several event types.
        self.assertEqual(
            self.check('[routing.event_consumers]\n"work.ready" = ["a", "b"]\n"work.opened" = ["a"]\n')[0], 0
        )

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
