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
    DriveQueueGuard,
    GoogleDriveHTTPClient,
    ServiceAccountTokenProvider,
    build_proxy_server,
)
from agent_runtime.adapters.permissions import CredentialProxyClient, ProxyResponse
from agent_runtime.app.main import build_daemon
from tests.support.fake_google_drive import ACCESS_TOKEN, DOC_MIME, FakeGoogleDrive, make_test_service_account
from tests.support.gdrive_tasks import _DriveTestExecutor
from tests.support.fakes import RecordingChannel
from tests.support.gdrive_credential_proxy import CLIENT_TOKEN


REPO_ROOT = Path(__file__).resolve().parents[2]
INSTALLER = REPO_ROOT / "deploy" / "gdrive-proxy" / "install.sh"


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
        self.google.add_folder("dir-attach", "附件", "root-q")
        self.google.add_folder("dir-private", "私人", "my-drive")
        self.google.add_file("private-1", "salary.md", "SECRET SALARY DATA", "dir-private")

        provider = ServiceAccountTokenProvider(key_path=self.key_path)
        drive = GoogleDriveHTTPClient(token_provider=provider, api_base=google_base)
        self.guard = guard = DriveQueueGuard(drive, root_folder_id="root-q", max_download_bytes=4096)
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
        status, data, _ = self._raw_bytes(op, body, token=token, method=method, headers=headers, path=path)
        return status, data.decode("utf-8")

    def _raw_bytes(self, op: str, body: Any, *, token: str | None = CLIENT_TOKEN, method: str = "POST",
                   headers: dict[str, str] | None = None, path: str | None = None) -> tuple[int, bytes, dict[str, str]]:
        data = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
        hdrs = {"Content-Type": "application/json", **(headers or {})}
        if token is not None:
            hdrs["Authorization"] = f"Bearer {token}"
        url = self.proxy_url + (path or f"/proxy/gdrive-queue/{op}")
        req = urllib.request.Request(url, data=data if method != "GET" else None, headers=hdrs, method=method)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, resp.read(), dict(resp.headers.items())
        except urllib.error.HTTPError as exc:
            with exc:
                return exc.code, exc.read(), dict(exc.headers.items())

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

    def test_download_returns_attachment_bytes_unchanged(self) -> None:
        blob = b"PK\x03\x04" + bytes(range(256)) * 8
        self.google.add_blob("att-zip", "taskboard_phase1_bundle.zip", blob, "dir-attach")
        meta = next(m for m in self.client.list_files("dir-attach") if m.file_id == "att-zip")
        self.assertEqual(meta.size, len(blob))
        got = self.client.download(meta)
        self.assertEqual(got.content, blob)
        self.assertEqual(got.name, "taskboard_phase1_bundle.zip")
        self.assertEqual(got.mime_type, "application/zip")
        self.assertEqual(got.export_mime_type, "")
        status, raw, headers = self._raw_bytes("download", {"file_id": "att-zip"})
        self.assertEqual((status, raw, headers["Content-Type"]), (200, blob, "application/octet-stream"))

    def test_google_docs_export_as_markdown_and_fall_back_to_plain_text(self) -> None:
        self.google.add_doc("att-doc", "设计文档", plain="设计\n________________", markdown="# 设计\n\n---", parent="dir-attach")
        self.google.add_doc("task-doc", "任务", plain="________________\nscope: proj:x", markdown="---\n\n## scope: proj:x",
                            parent="dir-pending")
        doc = self.client.download(next(m for m in self.client.list_files("dir-attach") if m.file_id == "att-doc"))
        self.assertEqual((doc.content.decode(), doc.export_mime_type, doc.mime_type), ("# 设计\n\n---", "text/markdown", "text/markdown"))
        self.assertEqual(self.client.read_text(self.client.get_file_meta("task-doc")), "---\n\n## scope: proj:x")

        self.google.markdown_export_status = 500
        doc = self.client.download(next(m for m in self.client.list_files("dir-attach") if m.file_id == "att-doc"))
        self.assertEqual((doc.content.decode(), doc.export_mime_type), ("设计\n________________", "text/plain"))
        self.assertEqual(self.client.read_text(self.client.get_file_meta("task-doc")), "________________\nscope: proj:x")
        exports = [path for _m, path in self.google.requests if "/export?" in path]
        self.assertTrue(any("mimeType=text%2Fmarkdown" in path for path in exports))
        self.assertTrue(any("mimeType=text%2Fplain" in path for path in exports))

    def test_download_is_limited_to_files_directly_in_the_attachments_folder(self) -> None:
        self.google.add_file("task-1", "t.md", "body", "dir-pending")
        self.google.add_folder("dir-nested", "nested", "dir-attach")
        self.google.add_blob("nested-file", "n.zip", b"x", "dir-nested")
        self.google.add_blob("att-big", "big.zip", b"x" * 5000, "dir-attach")
        self.google.add_blob("att-sheet", "表格", b"", "dir-attach", mime_type="application/vnd.google-apps.spreadsheet")
        cases = {
            "a task file": ({"file_id": "task-1"}, 403),
            "a private file": ({"file_id": "private-1"}, 403),
            "a file in a subfolder of 附件": ({"file_id": "nested-file"}, 403),
            "the attachments folder itself": ({"file_id": "dir-attach"}, 403),
            "a nested folder": ({"file_id": "dir-nested"}, 403),
            "a file over the limit": ({"file_id": "att-big"}, 413),
            "a Google Sheet": ({"file_id": "att-sheet"}, 415),
            "a missing file": ({"file_id": "no-such-file"}, 404),
            "a malformed id": ({"file_id": "../x"}, 400),
        }
        for label, (body, expected) in cases.items():
            with self.subTest(label):
                status, text = self._raw("download", body)
                self.assertEqual(status, expected, text)
                self.assertIn("error", json.loads(text))
        self.assertNotIn("SECRET SALARY DATA", self._raw("download", {"file_id": "private-1"})[1])
        # Refusals are decided from metadata: no file content was fetched for any of them.
        self.assertFalse(any("alt=media" in path for _m, path in self.google.requests))
        with self.assertRaisesRegex(DriveProxyError, "413"):
            self.client.download(next(m for m in self.client.list_files("dir-attach") if m.file_id == "att-big"))
        # Attachments are not task files: no meta, read, append or move on them.
        for op, body in (("meta", {"file_id": "att-big"}), ("read", {"file_id": "att-big"}),
                         ("move", {"file_id": "att-big", "add_parent": "dir-done", "remove_parent": "dir-attach"})):
            with self.subTest(op):
                self.assertEqual(self._raw(op, body)[0], 403)

    def test_an_export_over_the_limit_is_refused_after_the_fetch(self) -> None:
        self.google.add_doc("att-doc", "长文档", plain="x" * 5000, markdown="x" * 5000, parent="dir-attach")
        self.assertEqual(self._raw("download", {"file_id": "att-doc"})[0], 413)

    def test_download_keeps_the_http_layer_rules(self) -> None:
        self.google.add_blob("att-zip", "a.zip", b"zip", "dir-attach")
        self.assertEqual(self._raw("download", {"file_id": "att-zip"}, method="GET")[0], 405)
        self.assertEqual(self._raw("download", {"file_id": "att-zip"}, token=None)[0], 401)
        self.assertEqual(self._raw("download", {"file_id": "att-zip"}, headers={"Origin": "https://evil.example"})[0], 403)
        self.google.add_doc("att-doc", "d", plain="p", markdown="m", parent="dir-attach")
        # Both exports fail upstream: the proxy answers 502 and relays none of Google's body.
        self.google.export_status = 500
        status, text = self._raw("download", {"file_id": "att-doc"})
        self.assertEqual(status, 502, text)
        self.assertNotIn("exportFailed", text)

    def test_subfolders_and_list_include_the_attachments_folder(self) -> None:
        self.google.add_blob("att-zip", "a.zip", b"zip", "dir-attach")
        self.assertEqual(self.client.find_subfolders("root-q")["附件"], "dir-attach")
        self.assertEqual([m.file_id for m in self.client.list_files("dir-attach")], ["att-zip"])
        self.assertIn("附件 = dir-attach (1 files)", self.guard.check())

    def test_listing_follows_every_page_of_a_large_folder(self) -> None:
        # Drive returns at most pageSize files per response; the newest uploads sit past the first page.
        for i in range(250):
            self.google.add_blob(f"att-{i:03d}", f"file-{i:03d}.zip", b"x", "dir-attach")
        listed = self.client.list_files("dir-attach")
        self.assertEqual(len(listed), 250)
        self.assertEqual(listed[-1].name, "file-249.zip")
        self.assertIn("附件 = dir-attach (250 files)", self.guard.check())
        pages = [p for _m, p in self.google.requests if p.startswith("/drive/v3/files?") and "dir-attach" in p]
        self.assertTrue(any("pageToken=" in p for p in pages), pages)

    def test_client_errors_carry_status_and_reason_without_upstream_bodies(self) -> None:
        with self.assertRaisesRegex(DriveProxyError, "403.*outside the task queue"):
            self.client.list_files("dir-private")
        self.google.add_file("gone", "g.md", "x", "dir-pending")
        self.assertEqual(len(self.client.list_files("dir-pending")), 1)
        del self.google.items["gone"]
        with self.assertRaisesRegex(DriveProxyError, "404"):
            self.client.get_file_meta("gone")


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


@unittest.skipUnless(shutil.which("bash"), "bash is needed for the installer")
class DriveProxyUpgradeTests(unittest.TestCase):
    PREFIX = "/opt/hibernaut-drive-proxy"
    ETC = "/etc/hibernaut-drive-proxy"

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="gdrive_proxy_upgrade_")
        self.tmp = Path(self._tmp.name)
        self.fs = self.tmp / "fs"
        self.bin = self.tmp / "bin"
        self.bin.mkdir()
        self._stub_id(exists=True)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _stub_id(self, *, exists: bool) -> None:
        stub = self.bin / "id"
        stub.write_text("#!/bin/sh\necho 400\nexit 0\n" if exists else "#!/bin/sh\nexit 1\n")
        stub.chmod(0o755)

    def _install_files(self, platform: str, *, https_proxy: str = "") -> None:
        etc = self.fs / self.ETC.lstrip("/")
        etc.mkdir(parents=True)
        for name in ("sa.json", "client.token", "config.toml"):
            (etc / name).write_text("UNREADABLE-SECRET-MATERIAL")
        launcher = self.fs / self.PREFIX.lstrip("/") / "bin" / "hibernaut-drive-proxy"
        launcher.parent.mkdir(parents=True)
        launcher.write_text("#!/usr/local/bin/python3.12 -I\nimport sys\n")
        if platform == "linux":
            unit = self.fs / "etc/systemd/system/hibernaut-drive-proxy.service"
            unit.parent.mkdir(parents=True)
            env = f"Environment=HTTPS_PROXY={https_proxy}\n" if https_proxy else ""
            unit.write_text(f"[Service]\nUser=hb-drive-proxy\n{env}Restart=on-failure\n")
        else:
            plist = self.fs / "Library/LaunchDaemons/ai.hibernaut.drive-proxy.plist"
            plist.parent.mkdir(parents=True)
            env = (f"  <key>EnvironmentVariables</key><dict><key>HTTPS_PROXY</key><string>{https_proxy}</string></dict>\n"
                   if https_proxy else "")
            plist.write_text(f"<plist version=\"1.0\"><dict>\n{env}</dict></plist>\n")

    def _run(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(INSTALLER), *args],
            env={**os.environ, "PATH": f"{self.bin}{os.pathsep}{os.environ['PATH']}",
                 "HB_DRIVE_PROXY_DRY_RUN_ROOT": str(self.fs)},
            capture_output=True, text=True, timeout=60,
        )

    def _upgrade(self, platform: str, *extra: str) -> list[str]:
        proc = self._run("--upgrade", "--dry-run", "--platform", platform, *extra)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertNotIn("UNREADABLE-SECRET-MATERIAL", proc.stdout + proc.stderr)
        self.assertNotIn("sa.json", proc.stdout)
        self.assertNotIn("client.token", proc.stdout)
        return [line for line in proc.stdout.splitlines() if line.startswith("+ ")]

    def _assert_only_code_launcher_restart_and_check(self, commands: list[str], restart: str) -> None:
        self.assertTrue(commands)
        for cmd in commands:
            if cmd.endswith("--check"):
                self.assertIn(f"--config {self.ETC}/config.toml --check", cmd)
                continue
            if cmd == restart:
                continue
            self.assertIn(self.PREFIX, cmd)
            self.assertNotIn("/etc/", cmd)
            self.assertNotIn("useradd", cmd)
            self.assertNotIn("dscl", cmd)

    def test_linux_upgrade_replaces_code_and_launcher_checks_then_restarts(self) -> None:
        self._install_files("linux", https_proxy="http://127.0.0.1:7890")
        commands = self._upgrade("linux")
        restart = "+ systemctl restart hibernaut-drive-proxy"
        self._assert_only_code_launcher_restart_and_check(commands, restart)
        joined = "\n".join(commands)
        self.assertIn(f"+ cp -R {REPO_ROOT / 'src' / 'agent_runtime'} {self.PREFIX}/src.new/agent_runtime", joined)
        self.assertIn(f"chown -R root:root {self.PREFIX}/src.new", joined)
        self.assertIn(f"/usr/local/bin/python3.12 -I -c", joined)
        self.assertIn(f"+ mv -f {self.PREFIX}/bin/hibernaut-drive-proxy.new {self.PREFIX}/bin/hibernaut-drive-proxy", joined)
        check = next(c for c in commands if c.endswith("--check"))
        self.assertIn("HTTPS_PROXY=http://127.0.0.1:7890", check)
        self.assertIn("+ sudo -u hb-drive-proxy", check)
        # The check runs on the new code before the service is restarted onto it.
        self.assertLess(commands.index(check), commands.index(restart))
        self.assertEqual(commands[-1], restart)
        self.assertNotIn("daemon-reload", joined)

    def test_macos_upgrade_kickstarts_the_launch_daemon(self) -> None:
        self._install_files("macos")
        commands = self._upgrade("macos")
        restart = "+ launchctl kickstart -k system/ai.hibernaut.drive-proxy"
        self._assert_only_code_launcher_restart_and_check(commands, restart)
        joined = "\n".join(commands)
        self.assertIn(f"chown -R root:wheel {self.PREFIX}/src.new", joined)
        self.assertNotIn("bootstrap", joined)
        self.assertNotIn("HTTPS_PROXY", joined)
        self.assertEqual(commands[-1], restart)

    def test_macos_upgrade_reuses_the_daemon_https_proxy(self) -> None:
        self._install_files("macos", https_proxy="http://127.0.0.1:7890")
        check = next(c for c in self._upgrade("macos") if c.endswith("--check"))
        self.assertIn("HTTPS_PROXY=http://127.0.0.1:7890", check)

    def test_upgrade_python_option_overrides_the_launcher(self) -> None:
        self._install_files("linux")
        joined = "\n".join(self._upgrade("linux", "--python", "/usr/bin/python3"))
        self.assertIn("/usr/bin/python3 -I -c", joined)
        self.assertNotIn("python3.12", joined)

    def test_upgrade_no_check_skips_the_queue_check(self) -> None:
        self._install_files("linux")
        commands = self._upgrade("linux", "--no-check")
        self.assertFalse([c for c in commands if "--check" in c])
        self.assertEqual(commands[-1], "+ systemctl restart hibernaut-drive-proxy")

    def test_upgrade_refuses_without_an_installation(self) -> None:
        self._install_files("linux")
        (self.fs / self.ETC.lstrip("/") / "config.toml").unlink()
        proc = self._run("--upgrade", "--dry-run", "--platform", "linux")
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn(f"{self.ETC}/config.toml", proc.stderr)
        self.assertIn("--key PATH --root-folder-id ID --hb-user USER", proc.stderr)
        self.assertNotIn("+ ", proc.stdout)

    def test_upgrade_refuses_without_the_service_user_or_unit(self) -> None:
        self._stub_id(exists=False)
        proc = self._run("--upgrade", "--dry-run", "--platform", "macos")
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("service user _hbdriveproxy", proc.stderr)
        self.assertIn("/Library/LaunchDaemons/ai.hibernaut.drive-proxy.plist", proc.stderr)
        self.assertNotIn("+ ", proc.stdout)

    def test_upgrade_refuses_install_options(self) -> None:
        self._install_files("linux")
        cases = [
            ("--key", str(self.tmp / "sa.json")),
            ("--root-folder-id", "root-q"),
            ("--hb-user", "hbuser"),
            ("--port", "9100"),
            ("--https-proxy", "http://127.0.0.1:7890"),
        ]
        for option, value in cases:
            with self.subTest(option=option):
                proc = self._run("--upgrade", "--dry-run", "--platform", "linux", option, value)
                self.assertNotEqual(proc.returncode, 0)
                self.assertIn(f"cannot take {option}", proc.stderr)
                self.assertNotIn("+ ", proc.stdout)


if __name__ == "__main__":
    unittest.main()
