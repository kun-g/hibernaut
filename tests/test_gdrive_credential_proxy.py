from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from agent_runtime.adapters.channels import DriveProxyError, ProxiedDriveClient
from agent_runtime.adapters.credential_proxy.gdrive_queue import (
    DriveProxyConfig,
    DriveQueueGuard,
    GoogleDriveHTTPClient,
    ServiceAccountTokenProvider,
    build_proxy_server,
    load_proxy_config,
)
from agent_runtime.adapters.permissions import CredentialProxyClient, ProxyResponse
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon, build_daemon_from_config
from tests.fake_google_drive import ACCESS_TOKEN, DOC_MIME, FakeGoogleDrive, make_test_service_account
from tests.test_gdrive_tasks import _DriveTestExecutor
from tests.fakes import RecordingChannel

REPO_ROOT = Path(__file__).resolve().parents[1]
INSTALLER = REPO_ROOT / "deploy" / "gdrive-proxy" / "install.sh"
CLIENT_TOKEN = "local-client-token-for-tests"


class _RecordingProxyClient(CredentialProxyClient):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.bodies: list[str] = []

    def call_service(self, **kwargs: Any) -> ProxyResponse:
        resp = super().call_service(**kwargs)
        self.bodies.append(resp.body)
        return resp


@unittest.skipUnless(shutil.which("openssl"), "openssl is needed to make the test service account key")
class DriveCredentialProxyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="gdrive_proxy_")
        self.root = Path(self._tmp.name)
        # Bind the fake Google first so the service account's token_uri can point at it.
        self.google_server = None
        placeholder = FakeGoogleDrive(public_key_pem=self.root / "unused")
        self.google_server = placeholder.serve()
        google_base = f"http://127.0.0.1:{self.google_server.server_address[1]}"
        self.key_path, public_pem = make_test_service_account(self.root, f"{google_base}/token")
        placeholder.public_key_pem = public_pem
        self.google = placeholder
        self.google.add_folder("root-q", "任务队列", "my-drive")
        for fid, name in (("dir-pending", "待执行"), ("dir-progress", "执行中"), ("dir-done", "已完成"), ("dir-failed", "失败")):
            self.google.add_folder(fid, name, "root-q")
        self.google.add_folder("dir-private", "私人", "my-drive")
        self.google.add_file("private-1", "salary.md", "SECRET SALARY DATA", "dir-private")

        provider = ServiceAccountTokenProvider(key_path=self.key_path)
        drive = GoogleDriveHTTPClient(token_provider=provider, api_base=google_base)
        guard = DriveQueueGuard(drive, root_folder_id="root-q")
        self.proxy_server = build_proxy_server(
            guard, host="127.0.0.1", port=0, service_name="gdrive-queue", client_token=CLIENT_TOKEN
        )
        threading.Thread(target=self.proxy_server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True).start()
        self.proxy_url = f"http://127.0.0.1:{self.proxy_server.server_address[1]}"
        self.proxy_client = _RecordingProxyClient(self.proxy_url, token=CLIENT_TOKEN)
        self.client = ProxiedDriveClient(self.proxy_client, service_name="gdrive-queue")

    def tearDown(self) -> None:
        self.proxy_server.shutdown()
        self.proxy_server.server_close()
        if self.google_server is not None:
            self.google_server.shutdown()
            self.google_server.server_close()
        self._tmp.cleanup()

    def _raw(self, op: str, body: Any, *, token: str | None = CLIENT_TOKEN, method: str = "POST",
             headers: dict[str, str] | None = None, path: str | None = None) -> tuple[int, str]:
        data = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
        hdrs = {"Content-Type": "application/json", **(headers or {})}
        if token is not None:
            hdrs["Authorization"] = f"Bearer {token}"
        url = self.proxy_url + (path or f"/proxy/gdrive-queue/{op}")
        req = urllib.request.Request(url, data=data if method != "GET" else None, headers=hdrs, method=method)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read().decode("utf-8")

    async def test_queue_round_trip_through_the_proxy_lists_reads_moves_and_writes_back(self) -> None:
        self.google.add_file(
            "task-md", "task1.md", "---\nscope: proj:hibernaut\ntitle: Feature X\n---\nImplement X.", "dir-pending"
        )
        self.google.add_file("task-doc", "Release Plan", "NEED_APPROVAL before prod.", "dir-pending",
                             mime_type=DOC_MIME, modified_time="2026-11-22T03:05:00Z")
        executor = _DriveTestExecutor()
        daemon = build_daemon(
            db_path=self.root / "gd.db",
            memory_root=self.root / "mem",
            max_concurrency=1,
            executor_factory=lambda _iid: executor,
            channel=RecordingChannel(),
            gdrive_client=self.client,
            gdrive_root_folder_id="root-q",
            gdrive_default_inbox_id="proj:hibernaut",
        )
        assert daemon.gdrive_poller is not None
        stop = asyncio.Event()
        task = asyncio.create_task(
            daemon.run(stop_event=stop, timer_interval_seconds=0.1, outbox_interval_seconds=0.05)
        )
        try:
            # The daemon's own poll loop picks the files up on start; nothing else polls.
            for _ in range(300):
                if self.google.parents("task-md") == ["dir-done"] and "等待审批" in self.google.content("task-doc"):
                    break
                await asyncio.sleep(0.05)
            await daemon.pool.wait_until_idle(timeout_seconds=10.0)
        finally:
            stop.set()
            await task
            daemon.close()

        self.assertEqual(self.google.parents("task-md"), ["dir-done"])
        self.assertIn("Completed task: Feature X", self.google.content("task-md"))
        self.assertIn("## 执行结果（Hibernaut）", self.google.content("task-md"))
        self.assertEqual(self.google.parents("task-doc"), ["dir-progress"])
        self.assertIn("等待审批", self.google.content("task-doc"))
        self.assertEqual(executor.calls[0][1]["body"], "Implement X.")
        # The private file outside the queue was never read, and HB never saw Google's token or the key.
        self.assertFalse(any("private-1" in path for _m, path in self.google.requests))
        joined = "\n".join(self.proxy_client.bodies)
        self.assertNotIn(ACCESS_TOKEN, joined)
        self.assertNotIn("PRIVATE KEY", joined)
        self.assertEqual(self.google.token_requests, 1)

    def test_local_access_requires_the_client_token_and_rejects_browsers(self) -> None:
        body = {"folder_id": "dir-pending"}
        self.assertEqual(self._raw("list", body, token=None)[0], 401)
        self.assertEqual(self._raw("list", body, token="wrong")[0], 401)
        self.assertEqual(self._raw("list", body, headers={"Origin": "https://evil.example"})[0], 403)
        self.assertEqual(self._raw("list", body)[0], 200)

    def test_no_generic_forwarding(self) -> None:
        self.assertEqual(self._raw("files/raw", {"url": "https://www.googleapis.com/drive/v3/files"})[0], 404)
        self.assertEqual(self._raw("", {}, path="/proxy/anthropic/v1/messages")[0], 404)
        self.assertEqual(self._raw("", {}, path="/drive/v3/files?q=x")[0], 404)
        self.assertEqual(self._raw("list", {}, method="GET")[0], 405)
        self.assertEqual(self._raw("delete", {"file_id": "dir-pending"})[0], 404)
        self.assertEqual(self._raw("list", b"x" * (2 * 1024 * 1024))[0], 413)
        self.assertEqual(self._raw("list", b"not json")[0], 400)
        self.assertNotIn(("PATCH", "/drive/v3/files/dir-pending"), self.google.requests)

    def test_operations_are_limited_to_the_queue_folders_and_their_files(self) -> None:
        self.google.add_file("task-1", "t.md", "body", "dir-pending")
        before = list(self.google.requests)
        cases = {
            "subfolders of another folder": ("subfolders", {"parent_id": "my-drive"}),
            "list outside the queue": ("list", {"folder_id": "dir-private"}),
            "meta outside the queue": ("meta", {"file_id": "private-1"}),
            "read outside the queue": ("read", {"file_id": "private-1"}),
            "append outside the queue": ("append", {"file_id": "private-1", "text": "x"}),
            "move outside the queue": ("move", {"file_id": "private-1", "add_parent": "dir-done", "remove_parent": "dir-private"}),
            "move to a folder outside the queue": ("move", {"file_id": "task-1", "add_parent": "dir-private", "remove_parent": "dir-pending"}),
            "move from a folder the file is not in": ("move", {"file_id": "task-1", "add_parent": "dir-done", "remove_parent": "dir-progress"}),
            "move a queue folder": ("move", {"file_id": "dir-pending", "add_parent": "dir-done", "remove_parent": "root-q"}),
        }
        for label, (op, body) in cases.items():
            with self.subTest(label):
                status, text = self._raw(op, body)
                self.assertEqual(status, 403, text)
        self.assertNotIn("SECRET SALARY DATA", self._raw("read", {"file_id": "private-1"})[1])
        self.assertEqual(self.google.parents("private-1"), ["dir-private"])
        self.assertEqual(self.google.parents("task-1"), ["dir-pending"])
        self.assertEqual(self.google.content("private-1"), "SECRET SALARY DATA")
        self.assertFalse(any(m == "PATCH" for m, _p in self.google.requests[len(before):]))

    def test_identifiers_cannot_inject_into_drive_queries(self) -> None:
        for bad in ("dir-pending' or 'x' in parents or '", "../files", "", "a" * 300):
            with self.subTest(bad):
                self.assertEqual(self._raw("list", {"folder_id": bad})[0], 400)
        self.assertEqual(self._raw("read", {"file_id": 7})[0], 400)

    def test_client_errors_carry_status_and_reason_without_upstream_bodies(self) -> None:
        with self.assertRaisesRegex(DriveProxyError, "403.*outside the task queue"):
            self.client.list_files("dir-private")
        self.google.add_file("gone", "g.md", "x", "dir-pending")
        self.assertEqual(len(self.client.list_files("dir-pending")), 1)
        del self.google.items["gone"]
        with self.assertRaisesRegex(DriveProxyError, "404"):
            self.client.get_file_meta("gone")


class DriveProxyStartupTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="gdrive_proxy_cfg_")
        self.root = Path(self._tmp.name)
        self.key = self.root / "sa.json"
        self.key.write_text(json.dumps({"client_email": "x@y", "private_key": "TEST", "token_uri": "http://127.0.0.1:1/t"}))
        os.chmod(self.key, 0o600)
        self.token = self.root / "client.token"
        self.token.write_text(CLIENT_TOKEN + "\n")
        os.chmod(self.token, 0o600)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _write(self, **overrides: Any) -> Path:
        values: dict[str, Any] = {
            "listen_host": "127.0.0.1",
            "listen_port": 9100,
            "service_account_key_file": str(self.key),
            "client_token_file": str(self.token),
            "root_folder_id": "root-q",
        }
        values.update(overrides)
        lines = [f"{k} = {json.dumps(v)}" for k, v in values.items()]
        path = self.root / "proxy.toml"
        path.write_text("\n".join(lines) + "\n")
        return path

    def test_loads_a_valid_config(self) -> None:
        cfg = load_proxy_config(self._write())
        self.assertIsInstance(cfg, DriveProxyConfig)
        self.assertEqual(cfg.client_token, CLIENT_TOKEN)
        self.assertEqual(cfg.service_name, "gdrive-queue")
        self.assertEqual(cfg.root_folder_id, "root-q")

    def test_rejects_unsafe_settings(self) -> None:
        cases: dict[str, dict[str, Any]] = {
            "listen_host must be a loopback": {"listen_host": "0.0.0.0"},
            "root_folder_id": {"root_folder_id": ""},
            "unknown key": {"upstream_url": "https://example.com"},
        }
        for pattern, overrides in cases.items():
            with self.subTest(pattern), self.assertRaisesRegex(ValueError, pattern):
                load_proxy_config(self._write(**overrides))

    def test_rejects_key_or_token_files_readable_by_others(self) -> None:
        for path in (self.key, self.token):
            with self.subTest(path.name):
                os.chmod(path, 0o644)
                with self.assertRaisesRegex(ValueError, "readable by group or others"):
                    load_proxy_config(self._write())
                os.chmod(path, 0o600)

    def test_rejects_an_empty_client_token(self) -> None:
        self.token.write_text("\n")
        with self.assertRaisesRegex(ValueError, "client_token_file is empty"):
            load_proxy_config(self._write())


class HibernautDriveProxyConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="gdrive_hb_cfg_")
        self.root = Path(self._tmp.name)
        self.token = self.root / "gdrive-proxy.token"
        self.token.write_text(CLIENT_TOKEN)
        os.chmod(self.token, 0o600)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _cfg(self, **gdrive: Any) -> dict[str, Any]:
        section = {
            "enabled": True,
            "proxy_url": "http://127.0.0.1:9100",
            "proxy_token_file": str(self.token),
            "root_folder_id": "root-q",
        }
        section.update(gdrive)
        return {
            "runtime": {"db_path": str(self.root / "hb.db"), "memory_root": str(self.root / "mem")},
            "gdrive": section,
        }

    def test_enabled_queue_goes_through_the_credential_proxy(self) -> None:
        cfg = parse_config(self._cfg())
        self.assertEqual(cfg.gdrive.proxy_url, "http://127.0.0.1:9100")
        self.assertEqual(cfg.gdrive.proxy_service, "gdrive-queue")
        daemon = build_daemon_from_config(cfg, demo=True)
        try:
            assert daemon.gdrive_poller is not None
            self.assertIsInstance(daemon.gdrive_poller._client, ProxiedDriveClient)  # pyright: ignore[reportPrivateUsage]
        finally:
            daemon.close()

    def test_proxy_url_defaults_to_the_credential_proxy(self) -> None:
        raw = self._cfg(proxy_url="")
        raw["credential_proxy"] = {"url": "http://127.0.0.1:9000"}
        self.assertEqual(parse_config(raw).gdrive.proxy_url, "http://127.0.0.1:9000")

    def test_relative_token_file_resolves_against_the_config_directory(self) -> None:
        cfg = parse_config(self._cfg(proxy_token_file="gdrive-proxy.token"), base_dir=self.root)
        self.assertEqual(cfg.gdrive.proxy_token_file, str(self.token))

    def test_rejects_a_key_in_the_hibernaut_process_and_missing_proxy_settings(self) -> None:
        cases: dict[str, dict[str, Any]] = {
            "service_account_key_env is no longer supported": {"service_account_key_env": "HIBERNAUT_GDRIVE_SA_KEY"},
            "gdrive.proxy_url": {"proxy_url": ""},
            "gdrive.proxy_url must be an http": {"proxy_url": "file:///tmp/x"},
            "gdrive.proxy_token_file": {"proxy_token_file": ""},
        }
        for pattern, overrides in cases.items():
            with self.subTest(pattern), self.assertRaisesRegex(ValueError, pattern):
                parse_config(self._cfg(**overrides))

    def test_unreadable_or_shared_token_file_skips_the_poller(self) -> None:
        os.chmod(self.token, 0o644)
        daemon = build_daemon_from_config(parse_config(self._cfg()), demo=True)
        try:
            self.assertIsNone(daemon.gdrive_poller)
        finally:
            daemon.close()

    def test_disabled_queue_needs_no_proxy(self) -> None:
        cfg = parse_config({"gdrive": {"enabled": False}})
        self.assertFalse(cfg.gdrive.enabled)


@unittest.skipUnless(shutil.which("bash"), "bash is needed for the installer")
class DriveProxyInstallerTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="gdrive_proxy_install_")
        self.root = Path(self._tmp.name)
        self.key = self.root / "sa.json"
        self.key.write_text('{"private_key": "TEST-KEY-MATERIAL"}')

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _dry_run(self, platform: str) -> str:
        # Model a fresh installation even on hosts already running the proxy.
        lookup = self.root / "id"
        lookup.write_text("#!/bin/sh\nexit 1\n")
        lookup.chmod(0o755)
        proc = subprocess.run(
            ["bash", str(INSTALLER), "--dry-run", "--platform", platform, "--key", str(self.key),
             "--root-folder-id", "root-q", "--hb-user", "hbuser", "--python", "/usr/bin/python3"],
            env={**os.environ, "PATH": f"{self.root}{os.pathsep}{os.environ['PATH']}"},
            capture_output=True, text=True, timeout=60,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return proc.stdout

    def test_installer_syntax(self) -> None:
        subprocess.run(["bash", "-n", str(INSTALLER)], check=True)

    def test_linux_dry_run_installs_under_an_isolated_user(self) -> None:
        out = self._dry_run("linux")
        self.assertIn("useradd --system", out)
        self.assertIn("/opt/hibernaut-drive-proxy/src", out)
        self.assertRegex(out, r"install -m 0400 -o hb-drive-proxy -g hb-drive-proxy \S+ /etc/hibernaut-drive-proxy/sa\.json")
        self.assertIn("/etc/systemd/system/hibernaut-drive-proxy.service", out)
        self.assertIn("systemctl enable --now hibernaut-drive-proxy", out)
        self.assertIn("--check", out)
        self.assertNotIn("TEST-KEY-MATERIAL", out)

    def test_macos_dry_run_installs_a_launch_daemon(self) -> None:
        out = self._dry_run("macos")
        self.assertIn("dscl . -create /Users/_hbdriveproxy", out)
        self.assertRegex(out, r"install -m 0400 -o _hbdriveproxy -g _hbdriveproxy \S+ /etc/hibernaut-drive-proxy/sa\.json")
        self.assertIn("/Library/LaunchDaemons/ai.hibernaut.drive-proxy.plist", out)
        self.assertIn("launchctl bootstrap system", out)
        self.assertNotIn("TEST-KEY-MATERIAL", out)


if __name__ == "__main__":
    unittest.main()
