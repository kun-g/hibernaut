from __future__ import annotations

import asyncio
import hashlib
import os
import stat
import subprocess
import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path

from agent_runtime.adapters.channels import DriveDownload, DriveFileMeta, DriveProxyError
from agent_runtime.app.main import build_daemon
from agent_runtime.core.models import Emit, Event, RunOutcome, SessionRef
from agent_runtime.core.ports import RunCapabilities
from tests.support.fakes import RecordingChannel
from tests.support.gdrive_tasks import _FakeDriveClient

ATTACH = "dir-attach"
BUNDLE_ID = "1BundleZipAbCdEfGhIjKlMnOpQrStUvW"
DESIGN_ID = "1DesignDocAbCdEfGhIjKlMnOpQrStUvW"
OTHER_ID = "1OtherFileAbCdEfGhIjKlMnOpQrStUvW"
BIG_ID = "1BigFileXyAbCdEfGhIjKlMnOpQrStUvW"
DOC_MIME = "application/vnd.google-apps.document"
BUNDLE_BYTES = b"PK\x03\x04" + bytes(range(256)) * 4


class _AttachmentDrive(_FakeDriveClient):
    def __init__(self, *, with_attachment_folder: bool = True) -> None:
        super().__init__()
        if with_attachment_folder:
            self.subfolders[self.root_id]["附件"] = ATTACH
        self.blobs: dict[str, bytes] = {}
        self.refusals: dict[str, DriveProxyError] = {}
        self.downloads: list[str] = []
        # Set: listing 附件/ fails with it, as a proxy timeout or a 502 would.
        self.list_error: Exception | None = None

    def list_files(self, folder_id: str) -> list[DriveFileMeta]:
        if folder_id == ATTACH and self.list_error is not None:
            raise self.list_error
        return super().list_files(folder_id)

    def add_attachment(self, file_id: str, name: str, content: bytes, *, folder_id: str = ATTACH,
                       mime_type: str = "application/zip") -> None:
        self.files[file_id] = DriveFileMeta(file_id=file_id, name=name, mime_type=mime_type,
                                            modified_time="2026-11-22T03:00:00Z", parents=(folder_id,),
                                            size=len(content))
        self.blobs[file_id] = content

    def download(self, file_meta: DriveFileMeta) -> DriveDownload:
        self.downloads.append(file_meta.file_id)
        if file_meta.file_id in self.refusals:
            raise self.refusals[file_meta.file_id]
        meta = self.files[file_meta.file_id]
        # Same rule as DriveQueueGuard's 'download': only files directly inside 附件/.
        if ATTACH not in meta.parents:
            raise DriveProxyError("download", 403, "file is not in the attachments folder")
        if meta.mime_type == DOC_MIME:
            return DriveDownload(name=meta.name, mime_type="text/markdown", content=self.blobs[meta.file_id],
                                 export_mime_type="text/markdown")
        return DriveDownload(name=meta.name, mime_type=meta.mime_type, content=self.blobs[meta.file_id])


class _RecordingExecutor:
    executor_id = "attach-exec"

    def __init__(self, *, close: bool = True) -> None:
        self.bodies: list[str] = []
        self.paths_seen: list[list[Path]] = []
        self.close = close

    async def execute(self, events: Sequence[Event], session: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
        _ = caps
        body = str(events[-1].payload.get("body", ""))
        self.bodies.append(body)
        self.paths_seen.append([Path(p) for p in _local_paths(body)])
        return Emit(messages=("done",), session_id=session.session_id if session else None,
                    close_session=self.close)


def _local_paths(body: str) -> list[str]:
    return [line.split("：", 1)[1].split("（", 1)[0].strip()
            for line in body.splitlines() if line.startswith("- `") and "sha256" in line]


class DriveAttachmentTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory(prefix="gdrive_attach_"))).resolve()
        # A git checkout standing in for the SDE worktree: attachments must never land in it.
        self.worktree = self.root / "worktree"
        self.worktree.mkdir()
        subprocess.run(["git", "init", "-q", str(self.worktree)], check=True)
        self.attach_root = self.root / "data" / "attachments"
        self.drive = _AttachmentDrive()
        self.executor = _RecordingExecutor()

    def build(self, drive: _AttachmentDrive | None = None, executor: _RecordingExecutor | None = None,
              db_name: str = "gd.db") -> None:
        self.daemon = build_daemon(
            db_path=self.root / db_name,
            memory_root=self.root / "mem",
            max_concurrency=1,
            executor_factory=lambda _iid: executor or self.executor,
            channel=RecordingChannel(),
            gdrive_client=drive or self.drive,
            gdrive_root_folder_id="root-1",
            gdrive_default_inbox_id="proj:drive",
            gdrive_attachments_dir=self.attach_root,
        )
        self.addCleanup(self.daemon.close)

    async def run_once(self) -> None:
        assert self.daemon.gdrive_poller is not None
        self.daemon.gdrive_poller.poll_once()
        stop = asyncio.Event()
        pool = asyncio.create_task(self.daemon.pool.run_forever(stop))
        try:
            await asyncio.sleep(0)
            await self.daemon.pool.wait_until_idle(timeout_seconds=5.0)
        finally:
            stop.set()
            await pool

    def _git_status(self) -> str:
        return subprocess.run(["git", "status", "--porcelain", "--untracked-files=all"], cwd=self.worktree,
                              capture_output=True, text=True, check=True).stdout

    async def test_frontmatter_and_body_references_download_outside_the_worktree(self) -> None:
        self.drive.add_attachment(BUNDLE_ID, "taskboard_phase1_bundle.zip", BUNDLE_BYTES)
        self.drive.add_attachment(DESIGN_ID, "设计文档", "# 设计\n内容".encode(), mime_type=DOC_MIME)
        self.drive.add_attachment(OTHER_ID, "unreferenced.bin", b"never fetched")
        self.drive.add_file(
            "task-1", "task.md",
            "---\nscope: proj:drive\nattachments: taskboard_phase1_bundle.zip\n---\n"
            f"Apply the bundle. Design: https://docs.google.com/document/d/{DESIGN_ID}/edit",
        )
        # The executor checks the files while it runs: they exist until the session closes.
        seen_during_run: list[bytes] = []

        class _Checking(_RecordingExecutor):
            async def execute(self, events: Sequence[Event], session: SessionRef | None,
                              caps: RunCapabilities) -> RunOutcome:
                outcome = await super().execute(events, session, caps)
                seen_during_run.extend(p.read_bytes() for p in self.paths_seen[-1])
                return outcome

        executor = _Checking()
        self.build(executor=executor)
        await self.run_once()

        body = executor.bodies[0]
        self.assertTrue(body.startswith("Apply the bundle."))
        self.assertIn("## 附件", body)
        bundle_path, design_path = executor.paths_seen[0]
        self.assertEqual(bundle_path, self.attach_root / "task-1" / BUNDLE_ID / "taskboard_phase1_bundle.zip")
        self.assertEqual(design_path, self.attach_root / "task-1" / DESIGN_ID / "设计文档.md")
        self.assertIn(f"{len(BUNDLE_BYTES)} 字节", body)
        self.assertIn(f"sha256 {hashlib.sha256(BUNDLE_BYTES).hexdigest()}", body)
        self.assertIn("已导出为 Markdown", body)
        self.assertEqual(seen_during_run, [BUNDLE_BYTES, "# 设计\n内容".encode()])
        self.assertNotIn(OTHER_ID, self.drive.downloads)
        self.assertFalse(str(bundle_path).startswith(str(self.worktree)))
        self.assertEqual(self._git_status(), "")

    async def test_files_and_directories_are_private(self) -> None:
        self.drive.add_attachment(BUNDLE_ID, "b.zip", BUNDLE_BYTES)
        self.drive.add_file("task-1", "task.md", f"Use {BUNDLE_ID}.")
        self.build(executor=_RecordingExecutor(close=False))
        await self.run_once()
        target = self.attach_root / "task-1" / BUNDLE_ID / "b.zip"
        self.assertEqual(target.read_bytes(), BUNDLE_BYTES)
        for path, mode in ((self.attach_root, 0o700), (self.attach_root / "task-1", 0o700),
                           (target.parent, 0o700), (target, 0o600)):
            with self.subTest(str(path)):
                self.assertEqual(stat.S_IMODE(os.stat(path).st_mode), mode)

    async def test_failures_are_listed_and_the_task_still_starts(self) -> None:
        self.drive.add_attachment(BIG_ID, "huge.iso", b"x")
        self.drive.refusals[BIG_ID] = DriveProxyError("download", 413, "attachment is larger than 104857600 bytes")
        self.drive.add_attachment(OTHER_ID, "elsewhere.zip", b"x", folder_id="dir-pending")
        self.drive.add_file(
            "task-1", "task.md",
            f"---\nattachments: huge.iso, {OTHER_ID}, missing.zip, 1NoSuchFileAbCdEfGhIjKlMnOpQrStU\n---\nGo.",
        )
        self.build()
        await self.run_once()

        body = self.executor.bodies[0]
        self.assertIn("没有拿到", body)
        self.assertIn("`huge.iso`：Drive 代理拒绝（HTTP 413）：attachment is larger than 104857600 bytes", body)
        self.assertIn(f"`{OTHER_ID}`：不在 附件/ 目录", body)
        self.assertIn("`missing.zip`：不在 附件/ 目录", body)
        self.assertIn("`1NoSuchFileAbCdEfGhIjKlMnOpQrStU`：不在 附件/ 目录", body)
        # A file outside 附件/ is never requested, even when its ID is referenced.
        self.assertEqual(self.drive.downloads, [BIG_ID])
        self.assertEqual(self.drive.files["task-1"].parents, ("dir-done",))

    async def test_a_body_id_outside_the_attachments_folder_is_not_an_attachment(self) -> None:
        self.drive.add_attachment(OTHER_ID, "elsewhere.zip", b"x", folder_id="dir-done")
        self.drive.add_file("task-1", "task.md", f"See {OTHER_ID} and {BUNDLE_ID}x for context.")
        self.drive.add_attachment(BUNDLE_ID, "b.zip", BUNDLE_BYTES)
        self.build()
        await self.run_once()
        # Neither ID appears as a whole token of a file in 附件/, so there is no 附件 section at all.
        self.assertEqual(self.executor.bodies[0], f"See {OTHER_ID} and {BUNDLE_ID}x for context.")
        self.assertEqual(self.drive.downloads, [])

    async def test_a_listing_failure_is_reported_for_body_ids_and_each_frontmatter_ref(self) -> None:
        self.drive.add_attachment(BUNDLE_ID, "b.zip", BUNDLE_BYTES)
        self.drive.list_error = DriveProxyError("list", 502, "Drive returned HTTP 503")
        self.drive.add_file("task-1", "task.md", f"Use {BUNDLE_ID}.")
        self.drive.add_file("task-2", "task2.md", f"---\nattachments: b.zip, other.zip\n---\nUse {BUNDLE_ID}.",
                            modified_time="2026-11-22T03:01:00Z")
        executor = _RecordingExecutor(close=False)
        self.build(executor=executor)
        await self.run_once()

        only_body, with_refs = executor.bodies
        failure = "无法列出 附件/ 目录：Drive 代理拒绝（HTTP 502）：Drive returned HTTP 503"
        self.assertIn("## 附件", only_body)
        self.assertIn(f"- 正文里的 Drive 文件 ID：{failure}，未能识别正文引用的附件", only_body)
        for ref in ("b.zip", "other.zip"):
            self.assertIn(f"- `{ref}`：{failure}", with_refs)
        self.assertIn(f"- 正文里的 Drive 文件 ID：{failure}，未能识别正文引用的附件", with_refs)
        self.assertEqual(self.drive.downloads, [])

    async def test_real_yaml_list_reference_also_in_the_body_regression(self) -> None:
        # The 2026-11-23 task: frontmatter wrote the ID as a YAML list and the body repeated it. The section used to
        # list the file as downloaded and `[<ID>]` as 「不在 附件/ 目录」.
        real_id = "1QvaVZhZkRo1f_EknUHVJtBmE7UhNyxUm03fMV2fJGXw"
        self.drive.add_attachment(real_id, "taskboard.md", b"# Taskboard")
        self.drive.add_file("task-1", "task.md",
                            f"---\nscope: proj:drive\nattachments: [{real_id}]\n---\n设计方案见附件 {real_id}。")
        self.build()
        await self.run_once()
        body = self.executor.bodies[0]
        self.assertIn("`taskboard.md`：", body)
        self.assertNotIn("没有拿到", body)
        self.assertNotIn(f"[{real_id}", body)
        self.assertEqual(self.drive.downloads, [real_id])

    async def test_list_spellings_give_the_same_section(self) -> None:
        spellings = {
            "comma": "attachments: b.zip, {other}\n",
            "inline list": 'attachments: [b.zip, "{other}"]\n',
            "block list": "attachments:\n  - b.zip\n  - '{other}'\n",
        }
        sections: dict[str, str] = {}
        for label, line in spellings.items():
            with self.subTest(label):
                drive = _AttachmentDrive()
                drive.add_attachment(BUNDLE_ID, "b.zip", BUNDLE_BYTES)
                drive.add_attachment(DESIGN_ID, "d.bin", b"design")
                drive.add_file("task-1", "task.md", "---\n" + line.format(other=DESIGN_ID) + "---\nGo.")
                executor = _RecordingExecutor()
                self.attach_root = self.root / label / "attachments"
                self.build(drive=drive, executor=executor, db_name=f"{label}.db")
                await self.run_once()
                body = executor.bodies[0]
                self.assertEqual(drive.downloads, [BUNDLE_ID, DESIGN_ID])
                self.assertNotIn("没有拿到", body)
                sections[label] = body.replace(str(self.attach_root), "<root>")
        self.assertEqual(len(set(sections.values())), 1, sections)

    async def test_an_unrecognised_spelling_still_fails_with_its_raw_text(self) -> None:
        self.drive.add_attachment(BUNDLE_ID, "b.zip", BUNDLE_BYTES)
        self.drive.add_file("task-1", "task.md", "---\nattachments: [b.zip, 1NoSuchFileAbCdEfGhIjKlMnOpQrStU\n---\nGo.")
        self.build()
        await self.run_once()
        body = self.executor.bodies[0]
        # An unclosed bracket is not a list: the parts are matched as before and the bracketed one fails as written.
        self.assertIn("- `[b.zip`：不在 附件/ 目录（引用原文 `[b.zip`", body)
        self.assertIn("- `1NoSuchFileAbCdEfGhIjKlMnOpQrStU`：不在 附件/ 目录\n", body + "\n")
        self.assertEqual(self.drive.downloads, [])

    async def test_a_failed_spelling_of_a_chosen_file_is_not_a_failure(self) -> None:
        self.drive.add_attachment(BUNDLE_ID, "b.zip", BUNDLE_BYTES)
        self.drive.add_file("task-1", "task.md", f"---\nattachments: [{BUNDLE_ID}, b.zip\n---\nUse {BUNDLE_ID}.")
        self.build()
        await self.run_once()
        body = self.executor.bodies[0]
        self.assertNotIn("没有拿到", body)
        self.assertEqual(self.drive.downloads, [BUNDLE_ID])

    async def test_duplicate_names_must_be_referenced_by_id(self) -> None:
        self.drive.add_attachment(BUNDLE_ID, "same.zip", b"a")
        self.drive.add_attachment(OTHER_ID, "same.zip", b"b")
        self.drive.add_file("task-1", "task.md", "---\nattachments: same.zip\n---\nGo.")
        self.build()
        await self.run_once()
        self.assertIn("`same.zip`：附件/ 里有 2 个同名文件，请改用文件 ID", self.executor.bodies[0])

    async def test_missing_attachments_folder_is_reported(self) -> None:
        drive = _AttachmentDrive(with_attachment_folder=False)
        drive.add_file("task-1", "task.md", "---\nattachments: bundle.zip\n---\nGo.")
        self.build(drive=drive)
        await self.run_once()
        self.assertIn("`bundle.zip`：队列根下没有 附件/ 目录", self.executor.bodies[0])

    async def test_a_task_without_references_is_unchanged(self) -> None:
        self.drive.add_attachment(BUNDLE_ID, "b.zip", BUNDLE_BYTES)
        self.drive.add_file("task-1", "task.md", "Just do it.")
        self.build()
        await self.run_once()
        self.assertEqual(self.executor.bodies, ["Just do it."])
        self.assertFalse((self.attach_root / "task-1").exists())

    async def test_the_attachments_directory_is_removed_when_the_session_closes(self) -> None:
        self.drive.add_attachment(BUNDLE_ID, "b.zip", BUNDLE_BYTES)
        self.drive.add_file("task-1", "task.md", f"Use {BUNDLE_ID}.")
        self.build()
        await self.run_once()
        self.assertTrue(self.executor.paths_seen[0][0].name == "b.zip")
        self.assertFalse((self.attach_root / "task-1").exists())

    async def test_an_open_session_keeps_its_attachments_and_the_sweep_drops_orphans(self) -> None:
        self.drive.add_attachment(BUNDLE_ID, "b.zip", BUNDLE_BYTES)
        self.drive.add_file("task-1", "task.md", f"Use {BUNDLE_ID}.")
        orphan = self.attach_root / "task-gone" / BUNDLE_ID
        orphan.mkdir(parents=True)
        (orphan / "b.zip").write_bytes(b"left over by a closed session")
        self.build(executor=_RecordingExecutor(close=False))
        await self.run_once()
        # The Emit is written; the session stays open, so its files stay. The orphan has no open session.
        self.assertTrue((self.attach_root / "task-1" / BUNDLE_ID / "b.zip").exists())
        self.assertFalse((self.attach_root / "task-gone").exists())

        # Closing the session as MCP hibernaut_close_session does: the report of that close sweeps it.
        self.daemon.handler.ingest(inbox_id="proj:drive", source="mcp:close", external_id="close-1",
                                   correlation_key="gdrive:task-1", payload={"action": "close_session"})
        await self.run_once()
        self.assertEqual(self.daemon.sessions.list_sessions(status="active"), [])
        self.assertFalse((self.attach_root / "task-1").exists())

    async def test_a_failed_task_drops_its_attachments(self) -> None:
        self.drive.add_attachment(BUNDLE_ID, "b.zip", BUNDLE_BYTES)
        self.drive.add_file("task-1", "task.md", f"Use {BUNDLE_ID}.")

        class _Failing(_RecordingExecutor):
            async def execute(self, events: Sequence[Event], session: SessionRef | None,
                              caps: RunCapabilities) -> RunOutcome:
                await super().execute(events, session, caps)
                raise RuntimeError("boom")

        self.build(executor=_Failing())
        for _ in range(3):
            await self.run_once()
        self.assertEqual(self.drive.files["task-1"].parents, ("dir-failed",))
        self.assertFalse((self.attach_root / "task-1").exists())


if __name__ == "__main__":
    unittest.main()
