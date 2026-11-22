from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path

from agent_runtime.adapters.channels import (
    DriveFileMeta,
    DriveResultWriter,
    DriveTaskPoller,
    parse_task_markdown,
)
from agent_runtime.adapters.credential_proxy.gdrive_queue import ServiceAccountTokenProvider
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon, build_daemon_from_config
from agent_runtime.core.models import Emit, Event, RunOutcome, SessionRef, Suspend
from agent_runtime.core.ports import RunCapabilities
from tests.fakes import RecordingChannel


class _FakeDriveClient:
    def __init__(self, root_id: str = "root-1") -> None:
        self.root_id = root_id
        self.subfolders: dict[str, dict[str, str]] = {
            root_id: {
                "待执行": "dir-pending",
                "执行中": "dir-progress",
                "已完成": "dir-done",
                "失败": "dir-failed",
            }
        }
        self.files: dict[str, DriveFileMeta] = {}
        self.contents: dict[str, str] = {}
        self.move_history: list[tuple[str, str, str]] = []

    def add_file(
        self,
        file_id: str,
        name: str,
        content: str,
        *,
        folder_id: str = "dir-pending",
        mime_type: str = "text/markdown",
        modified_time: str = "2026-11-22T03:00:00Z",
    ) -> None:
        self.files[file_id] = DriveFileMeta(
            file_id=file_id,
            name=name,
            mime_type=mime_type,
            modified_time=modified_time,
            parents=(folder_id,),
        )
        self.contents[file_id] = content

    def find_subfolders(self, parent_id: str) -> dict[str, str]:
        return dict(self.subfolders.get(parent_id, {}))

    def list_files(self, folder_id: str) -> list[DriveFileMeta]:
        return [m for m in self.files.values() if folder_id in m.parents]

    def get_file_meta(self, file_id: str) -> DriveFileMeta:
        return self.files[file_id]

    def read_text(self, file_meta: DriveFileMeta) -> str:
        return self.contents[file_meta.file_id]

    def append_text(self, file_id: str, mime_type: str, addition: str) -> None:
        _ = mime_type
        self.contents[file_id] = self.contents[file_id].rstrip() + "\n\n" + addition.strip() + "\n"

    def move_file(self, file_id: str, *, add_parent: str, remove_parent: str) -> None:
        meta = self.files[file_id]
        new_parents = tuple(p for p in meta.parents if p != remove_parent) + (add_parent,)
        self.files[file_id] = DriveFileMeta(
            file_id=meta.file_id,
            name=meta.name,
            mime_type=meta.mime_type,
            modified_time=meta.modified_time,
            parents=new_parents,
        )
        self.move_history.append((file_id, remove_parent, add_parent))


class _DriveTestExecutor:
    executor_id = "gdrive-mock-exec"

    def __init__(self) -> None:
        self.calls: list[tuple[str | None, dict[str, object]]] = []

    async def execute(
        self,
        events: Sequence[Event],
        session: SessionRef | None,
        caps: RunCapabilities,
    ) -> RunOutcome:
        _ = caps
        payload = events[-1].payload
        self.calls.append((session.session_id if session else None, dict(payload)))
        body = str(payload.get("body", ""))
        if "NEED_APPROVAL" in body:
            return Suspend(
                request_id="req-gd-1",
                prompt="Please approve production rollout",
                session_checkpoint='{"step": "approval"}',
                session_id=session.session_id if session else None,
            )
        if "FORCE_FAIL" in body:
            raise RuntimeError("simulated build failure")
        return Emit(
            messages=(f"Completed task: {payload.get('title')}",),
            session_id=session.session_id if session else None,
        )


class TestGoogleDriveTaskQueue(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="gdrive_tasks_")
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_parse_task_markdown_frontmatter_and_plain_text(self) -> None:
        raw = (
            "---\n"
            "scope: proj:hibernaut\n"
            "executor_id: grok\n"
            "priority: P1\n"
            'title: "Add Drive poller"\n'
            "---\n"
            "# Requirements\nBuild poller.\n"
        )
        meta, body = parse_task_markdown(raw)
        self.assertEqual(meta["scope"], "proj:hibernaut")
        self.assertEqual(meta["executor_id"], "grok")
        self.assertEqual(meta["priority"], "P1")
        self.assertEqual(meta["title"], "Add Drive poller")
        self.assertEqual(body, "# Requirements\nBuild poller.")

        plain_meta, plain_body = parse_task_markdown("Just plain body text")
        self.assertEqual(plain_meta, {})
        self.assertEqual(plain_body, "Just plain body text")

    def test_service_account_token_provider_caches_and_refreshes_jwt(self) -> None:
        key_file = self.root / "sa.json"
        key_file.write_text(
            json.dumps(
                {
                    "client_email": "hibernaut@demo.iam.gserviceaccount.com",
                    "private_key": "FAKE-PEM",
                    "token_uri": "https://oauth2.googleapis.com/token",
                }
            ),
            encoding="utf-8",
        )
        now = [1000.0]
        token_calls: list[bytes] = []

        def fake_http(method: str, url: str, headers: object, body: bytes | None) -> bytes:
            _ = (method, url, headers)
            token_calls.append(body or b"")
            idx = len(token_calls)
            return json.dumps({"access_token": f"tok-{idx}", "expires_in": 3600}).encode("utf-8")

        provider = ServiceAccountTokenProvider(
            key_path=key_file,
            http_request=fake_http,
            jwt_signer=lambda data, _pem: b"sig:" + data[:8],
            clock=lambda: now[0],
        )
        self.assertEqual(provider.get_token(), "tok-1")
        # Within expiry window -> cached.
        now[0] += 1000.0
        self.assertEqual(provider.get_token(), "tok-1")
        self.assertEqual(len(token_calls), 1)
        # Past refresh margin (exp - 60s) -> refreshes.
        now[0] += 2600.0
        self.assertEqual(provider.get_token(), "tok-2")
        self.assertEqual(len(token_calls), 2)

    async def test_e2e_poll_emit_rework_continuation_suspend_and_failed(self) -> None:
        drive = _FakeDriveClient()
        executor = _DriveTestExecutor()
        inner_channel = RecordingChannel()
        daemon = build_daemon(
            db_path=self.root / "gd.db",
            memory_root=self.root / "mem_gd",
            max_concurrency=1,
            executor_factory=lambda _iid: executor,
            channel=inner_channel,
            gdrive_client=drive,
            gdrive_root_folder_id="root-1",
            gdrive_default_inbox_id="proj:hibernaut",
        )
        assert daemon.gdrive_poller is not None
        assert daemon.gdrive_result_writer is not None

        stop = asyncio.Event()
        daemon_task = asyncio.create_task(
            daemon.run(stop_event=stop, timer_interval_seconds=0.1, outbox_interval_seconds=0.05)
        )
        try:
            # 1. Markdown task with frontmatter -> ingested (stays in 待执行 until a Run starts) -> Emit ->
            #    appended & moved to 已完成.
            drive.add_file(
                "file-md-1",
                "task1.md",
                "---\nscope: proj:hibernaut\ntitle: Feature X\n---\nImplement feature X.",
                modified_time="2026-11-22T03:00:00Z",
            )
            polled1 = daemon.gdrive_poller.poll_once()
            self.assertEqual(len(polled1), 1)
            self.assertFalse(polled1[0].duplicate)
            self.assertEqual(drive.files["file-md-1"].parents, ("dir-pending",))

            await daemon.pool.wait_until_idle(timeout_seconds=3.0)
            self.assertEqual(drive.files["file-md-1"].parents, ("dir-done",))
            self.assertIn("## 执行结果（Hibernaut）", drive.contents["file-md-1"])
            self.assertIn("Completed task: Feature X", drive.contents["file-md-1"])
            first_sid = executor.calls[0][0]
            self.assertIsNotNone(first_sid)

            # 2. User edits file-md-1 (new modifiedTime) and moves it back to 待执行 -> continues SAME Session!
            drive.contents["file-md-1"] += "\nPlease also add edge-case tests."
            drive.files["file-md-1"] = DriveFileMeta(
                file_id="file-md-1",
                name="task1.md",
                mime_type="text/markdown",
                modified_time="2026-11-22T03:10:00Z",
                parents=("dir-pending",),
            )
            polled_rework = daemon.gdrive_poller.poll_once()
            self.assertEqual(len(polled_rework), 1)
            self.assertFalse(polled_rework[0].duplicate)
            await daemon.pool.wait_until_idle(timeout_seconds=3.0)
            self.assertEqual(executor.calls[1][0], first_sid)
            self.assertEqual(drive.files["file-md-1"].parents, ("dir-done",))

            # 3. Same modifiedTime moved back -> idempotent duplicate (dropped).
            drive.files["file-md-1"] = DriveFileMeta(
                file_id="file-md-1",
                name="task1.md",
                mime_type="text/markdown",
                modified_time="2026-11-22T03:10:00Z",
                parents=("dir-pending",),
            )
            polled_dup = daemon.gdrive_poller.poll_once()
            self.assertTrue(polled_dup[0].duplicate)
            self.assertEqual(polled_dup[0].status, "dropped")

            # 4. Google Doc task triggering Suspend -> stays in 执行中, appends approval prompt, notifies inner channel.
            drive.add_file(
                "file-doc-2",
                "Release Plan",
                "NEED_APPROVAL before deploying to prod.",
                mime_type="application/vnd.google-apps.document",
                modified_time="2026-11-22T03:20:00Z",
            )
            daemon.gdrive_poller.poll_once()
            await daemon.pool.wait_until_idle(timeout_seconds=3.0)
            await daemon.dispatcher.dispatch_once()
            self.assertEqual(drive.files["file-doc-2"].parents, ("dir-progress",))
            self.assertIn("等待审批", drive.contents["file-doc-2"])
            self.assertIn("req-gd-1", drive.contents["file-doc-2"])
            self.assertTrue(
                any(m[2].get("kind") == "approval_request" for m in inner_channel.sent)
            )

            # 5. Failing task -> exhausts retries, appends error summary, moves to 失败/.
            drive.add_file(
                "file-fail-3",
                "broken.md",
                "FORCE_FAIL on this run",
                modified_time="2026-11-22T03:30:00Z",
            )
            with self.assertLogs("agent_runtime.app.main", level="ERROR"):
                daemon.gdrive_poller.poll_once()
                await daemon.pool.wait_until_idle(timeout_seconds=3.0)
            self.assertEqual(drive.files["file-fail-3"].parents, ("dir-failed",))
            self.assertIn("失败（failed）", drive.contents["file-fail-3"])
            self.assertIn("simulated build failure", drive.contents["file-fail-3"])
        finally:
            stop.set()
            await daemon_task
            daemon.close()

    def test_parse_gdrive_config_and_build_daemon_from_config(self) -> None:
        cfg = parse_config(
            {
                "runtime": {
                    "db_path": str(self.root / "cfg_gd.db"),
                    "memory_root": str(self.root / "mem_cfg_gd"),
                },
                "gdrive": {
                    "enabled": True,
                    "proxy_url": "http://127.0.0.1:9100",
                    "proxy_token_file": str(self.root / "gdrive-proxy.token"),
                    "root_folder_id": "root-1",
                    "poll_interval_seconds": 15.0,
                    "default_inbox_id": "proj:hibernaut",
                },
            }
        )
        self.assertTrue(cfg.gdrive.enabled)
        self.assertEqual(cfg.gdrive.root_folder_id, "root-1")
        self.assertEqual(cfg.gdrive.poll_interval_seconds, 15.0)
        self.assertEqual(cfg.gdrive.default_inbox_id, "proj:hibernaut")

        drive = _FakeDriveClient()
        daemon = build_daemon_from_config(cfg, demo=True, gdrive_client=drive)
        try:
            self.assertIsInstance(daemon.gdrive_poller, DriveTaskPoller)
            self.assertIsInstance(daemon.gdrive_result_writer, DriveResultWriter)
            folders = daemon.gdrive_poller.ensure_folders()
            self.assertEqual(folders.pending_id, "dir-pending")
            self.assertEqual(folders.done_id, "dir-done")
        finally:
            daemon.close()
