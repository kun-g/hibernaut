from __future__ import annotations

import asyncio
import contextlib
import io
import json
import os
import re
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import unittest
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

from agent_runtime.adapters.channels import (
    ConsoleChannelAdapter,
    FileOutboxChannel,
    WebhookHTTPServer,
    local_events_url,
    outbox_context,
)
from agent_runtime.adapters.channels.switchboard_bridge import SwitchboardChannelAdapter
from agent_runtime.adapters.executor_cli.executor import CLITemplate, LocalCLIExecutor
from agent_runtime.adapters.executor_cli.worktree import CommandResult, OrcaWorktreeManager
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteWorkspaceStore
from agent_runtime.app.config import load_config, parse_config
from agent_runtime.app.main import build_daemon, build_daemon_from_config
from agent_runtime.core.models import TASK_CONTEXT_KEY, Emit, Event, RunOutcome, SessionRef, Suspend
from agent_runtime.core.ports import RunCapabilities
from agent_runtime.engine.dispatcher import OutboxDispatcher
from tests.fakes import InMemoryOutbox, RecordingChannel
from tests.test_gdrive_tasks import _DriveTestExecutor, _FakeDriveClient  # pyright: ignore[reportPrivateUsage]

NAME = re.compile(r"^\d{8}-\d{6}\.\d{3}(-\d+)?\.md$")
INBOX = "proj:hibernaut"


class _Clock:
    def __init__(self, *stamps: datetime) -> None:
        self._stamps = list(stamps)

    def __call__(self) -> datetime:
        return self._stamps.pop(0) if len(self._stamps) > 1 else self._stamps[0]


def _channel(directory: Path, inner: Any = None, **kwargs: Any) -> tuple[FileOutboxChannel, list[str]]:
    lines: list[str] = []
    kwargs.setdefault("events_url", "http://127.0.0.1:8080/events")
    channel = FileOutboxChannel(directory, inner or RecordingChannel(), announce=lines.append, **kwargs)
    return channel, lines


def _md(directory: Path) -> list[Path]:
    return sorted(directory.glob("*.md"))


class TestFileNames(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))

    async def test_names_are_local_millisecond_stamps_that_sort_in_write_order(self) -> None:
        base = datetime(2026, 11, 22, 18, 13, 5, 123456)
        stamps = [base, base + timedelta(milliseconds=1), base + timedelta(seconds=2), base + timedelta(hours=1)]
        channel, _ = _channel(self.root / "nested" / "outbox", clock=_Clock(*stamps))
        for i in range(4):
            await channel.send(INBOX, f"message {i}", {"kind": "emit"})
        files = _md(self.root / "nested" / "outbox")
        self.assertEqual(
            [f.name for f in files],
            ["20261122-181305.123.md", "20261122-181305.124.md", "20261122-181307.123.md", "20261122-191305.123.md"],
        )
        self.assertEqual([f.read_text().rstrip().rsplit("\n", 1)[-1] for f in files], [f"message {i}" for i in range(4)])

    async def test_same_millisecond_never_overwrites(self) -> None:
        stamp = datetime(2026, 11, 22, 18, 13, 5, 123000)
        outbox = self.root / "outbox"
        outbox.mkdir()
        (outbox / "20261122-181305.123.md").write_text("written by someone else\n")
        channel, _ = _channel(outbox, clock=_Clock(stamp))
        await channel.send(INBOX, "first", {"kind": "emit"})
        await channel.send(INBOX, "second", {"kind": "emit"})
        self.assertEqual((outbox / "20261122-181305.123.md").read_text(), "written by someone else\n")
        self.assertTrue((outbox / "20261122-181305.123-2.md").read_text().endswith("first\n"))
        self.assertTrue((outbox / "20261122-181305.123-3.md").read_text().endswith("second\n"))
        self.assertTrue(all(NAME.match(f.name) for f in _md(outbox)))

    async def test_two_messages_in_one_millisecond_on_the_real_clock_keep_their_order(self) -> None:
        channel, _ = _channel(self.root)
        for i in range(5):
            await channel.send(INBOX, f"m{i}", {"kind": "emit"})
        files = _md(self.root)
        self.assertEqual(len(files), 5)
        self.assertEqual([f.read_text().rstrip().rsplit("\n", 1)[-1] for f in files], [f"m{i}" for i in range(5)])

    async def test_write_is_atomic_and_leaves_no_temporary_file(self) -> None:
        seen: list[tuple[str, list[str]]] = []
        real_link = os.link

        def spy(src: Any, dst: Any, *args: Any, **kwargs: Any) -> None:
            # At publish time the full text is already on disk under a hidden name and no .md is visible yet.
            seen.append((Path(src).read_text(), [p.name for p in _md(self.root)]))
            real_link(src, dst, *args, **kwargs)

        channel, _ = _channel(self.root)
        with patch("agent_runtime.adapters.channels.file_outbox.os.link", spy):
            await channel.send(INBOX, "body text", {"kind": "emit"})
        (final,) = _md(self.root)
        self.assertEqual(seen, [(final.read_text(), [])])
        self.assertEqual(sorted(p.name for p in self.root.iterdir()), [final.name])

    async def test_write_failure_raises_so_the_dispatcher_retries_without_losing_the_message(self) -> None:
        blocker = self.root / "outbox"
        blocker.write_text("not a directory")
        inner = RecordingChannel()
        channel, _ = _channel(blocker, inner)
        outbox = InMemoryOutbox()
        outbox.enqueue(INBOX, "keep me", {"kind": "emit"})
        dispatcher = OutboxDispatcher(outbox=outbox, channel=channel)
        with self.assertLogs("agent_runtime.adapters.channels.file_outbox", level="ERROR"):
            self.assertEqual(await dispatcher.dispatch_once(), 0)
        (pending,) = outbox.fetch_pending()
        self.assertEqual((pending.status, pending.attempts), ("pending", 1))
        self.assertEqual(inner.sent, [])

        blocker.unlink()
        self.assertEqual(await dispatcher.dispatch_once(), 1)
        self.assertEqual(inner.sent, [(INBOX, "keep me", {"kind": "emit"})])
        self.assertTrue(_md(blocker)[0].read_text().endswith("keep me\n"))

    async def test_inner_failure_retry_reuses_the_written_file(self) -> None:
        inner = RecordingChannel(fail_times=1)
        channel, lines = _channel(self.root, inner)
        outbox = InMemoryOutbox()
        outbox.enqueue(INBOX, "once", {"kind": "emit"})
        dispatcher = OutboxDispatcher(outbox=outbox, channel=channel)
        self.assertEqual(await dispatcher.dispatch_once(), 0)
        self.assertEqual(await dispatcher.dispatch_once(), 1)
        self.assertEqual(len(_md(self.root)), 1)
        self.assertEqual(len(lines), 1)

    async def test_inner_channel_gets_the_message_unchanged(self) -> None:
        inner = RecordingChannel()
        channel, _ = _channel(self.root, inner)
        meta = {"kind": "emit", "session_id": "sess_1", TASK_CONTEXT_KEY: {"title": "T", "status": "done"}}
        await channel.send(INBOX, "same text", meta)
        self.assertEqual(inner.sent, [(INBOX, "same text", meta)])


class TestConfig(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))

    def test_relative_paths_resolve_against_the_config_file_and_tilde_expands(self) -> None:
        cfg_dir = self.root / "conf"
        cfg_dir.mkdir()
        path = cfg_dir / "hibernaut.toml"
        path.write_text('[channels]\noutbox_dir = "outbox"\n')
        self.assertEqual(load_config(path).channels.outbox_dir, str(cfg_dir.resolve() / "outbox"))
        cfg = parse_config({"channels": {"outbox_dir": "~/hb-outbox"}}, base_dir=self.root)
        self.assertEqual(cfg.channels.outbox_dir, str(Path.home() / "hb-outbox"))
        self.assertEqual(parse_config({}, base_dir=self.root).channels.outbox_dir, "")
        with self.assertRaises(ValueError):
            parse_config({"channels": {"outbox_dir": 3}}, base_dir=self.root)

    def test_local_events_url_points_at_loopback(self) -> None:
        self.assertEqual(local_events_url("0.0.0.0", 9000), "http://127.0.0.1:9000/events")
        self.assertEqual(local_events_url("127.0.0.1", 8080), "http://127.0.0.1:8080/events")
        self.assertEqual(local_events_url("::", 8080), "http://[::1]:8080/events")


class TestWiring(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))

    def _from_config(self, channels: dict[str, Any]) -> Any:
        cfg = parse_config(
            {"runtime": {"db_path": str(self.root / "hb.db"), "memory_root": str(self.root / "mem")},
             "channels": channels},
            base_dir=self.root,
        )
        with patch.dict(os.environ, {"FEISHU_APP_ID": "", "FEISHU_APP_SECRET": ""}):
            daemon = build_daemon_from_config(cfg, demo=True)
        self.addCleanup(daemon.close)
        return daemon

    async def test_unconfigured_prints_full_text_and_writes_no_file(self) -> None:
        daemon = self._from_config({})
        self.assertIsInstance(daemon.channel, ConsoleChannelAdapter)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            await daemon.channel.send(INBOX, "full body", {"kind": "emit"})
        self.assertEqual(out.getvalue(), f"[OUTBOX:emit] inbox={INBOX} | full body\n")
        self.assertEqual(sorted(p.name for p in self.root.iterdir() if p.is_dir()), ["mem"])

    async def test_configured_console_prints_one_summary_line(self) -> None:
        daemon = self._from_config({"outbox_dir": "outbox", "port": 9123})
        self.assertIsInstance(daemon.channel, FileOutboxChannel)
        meta = {"kind": "emit", "session_id": "sess_9", "correlation_key": "gdrive:f9",
                TASK_CONTEXT_KEY: {"title": "Fix login", "status": "failed", "source": "gdrive:task"}}
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            await daemon.channel.send(INBOX, "line one\nline two", meta)
        (path,) = _md(self.root / "outbox")
        (line,) = out.getvalue().splitlines()
        self.assertRegex(line, rf"^\[OUTBOX\] \d{{4}}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{{3}} 失败 \| Fix login \| {re.escape(str(path))}$")
        self.assertNotIn("line one", out.getvalue())


class _ScriptedExecutor:
    executor_id = "scripted"

    def __init__(self) -> None:
        self.calls: list[tuple[list[Event], SessionRef | None]] = []

    async def execute(self, events: Sequence[Event], session: SessionRef | None, caps: RunCapabilities) -> RunOutcome:
        _ = caps
        self.calls.append((list(events), session))
        payload = events[-1].payload
        if payload.get("approval_request_id"):
            return Emit(messages=(f"resumed approved={payload.get('approved')}",))
        mode = payload.get("mode")
        if mode == "suspend":
            return Suspend(request_id="req_file_1", prompt="Deploy to prod?", session_checkpoint="{}")
        if mode == "failed":
            return Emit(messages=("[sde] development failed: payload needs the task requirements",), status="failed")
        return Emit(messages=("all good",))


def _header(path: Path) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in path.read_text().split("## ", 1)[0].splitlines():
        if line.startswith("- ") and "：" in line:
            key, value = line[2:].split("：", 1)
            fields[key] = value
    return fields


async def _until(check: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not check():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.02)


class TestHeadersEndToEnd(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.outbox = self.root / "outbox"
        self.executor = _ScriptedExecutor()
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.daemon = build_daemon(
            db_path=self.root / "hb.db",
            memory_root=self.root / "mem",
            executor_factory=lambda _iid: self.executor,
            outbox_dir=self.outbox,
            approval_events_url=local_events_url("127.0.0.1", self.port),
        )
        self.server = WebhookHTTPServer(("127.0.0.1", self.port), self.daemon.http_handler())
        self.stop = asyncio.Event()
        with contextlib.redirect_stdout(io.StringIO()):
            self.task = asyncio.create_task(
                self.daemon.run(server=self.server, stop_event=self.stop, timer_interval_seconds=0.1,
                                outbox_interval_seconds=0.02)
            )
        await asyncio.sleep(0)

    async def asyncTearDown(self) -> None:
        self.stop.set()
        await self.task
        self.server.server_close()
        self.daemon.close()

    def ingest(self, external_id: str, payload: dict[str, Any], *, source: str = "gdrive:task",
               correlation_key: str | None = None) -> None:
        self.daemon.handler.ingest(inbox_id=INBOX, source=source, external_id=external_id, payload=payload,
                                   correlation_key=correlation_key)

    async def files(self, count: int) -> list[Path]:
        await _until(lambda: len(_md(self.outbox)) >= count)
        return _md(self.outbox)

    async def test_done_and_failed_headers(self) -> None:
        self.ingest("e1", {"title": "Ship the release notes", "mode": "done"}, correlation_key="gdrive:f1")
        (done,) = await self.files(1)
        self.ingest("e2", {"issue": {"title": "Login breaks on Safari"}, "mode": "failed"},
                    source="github:issue_poll", correlation_key="github:kun-g/hibernaut#7")
        failed = (await self.files(2))[1]
        sid_done = self.executor.calls[0][1]
        assert sid_done is not None
        self.assertEqual(_header(done), {
            "任务": "Ship the release notes",
            "来源": "Drive（gdrive:task）",
            "inbox_id": INBOX,
            "correlation_key": "gdrive:f1",
            "会话 ID": sid_done.session_id,
            "执行器": "scripted",
            "状态": "完成",
            "需要人做什么": "无需操作，查看结果即可",
            "时间": _header(done)["时间"],
        })
        self.assertTrue(done.read_text().startswith("# Ship the release notes\n"))
        self.assertTrue(done.read_text().endswith("## 正文\n\nall good\n"))
        h = _header(failed)
        self.assertEqual((h["任务"], h["来源"], h["状态"], h["需要人做什么"]), (
            "Login breaks on Safari", "GitHub（github:issue_poll）", "失败", "查看失败原因，修正后重新提交任务"))
        self.assertIn("[sde] development failed: payload needs the task requirements", failed.read_text())
        self.assertNotIn("## 审批", failed.read_text())

    async def test_title_falls_back_to_drive_file_name_then_correlation_key(self) -> None:
        self.ingest("e1", {"drive_file_name": "task1.md"}, correlation_key="gdrive:a")
        self.ingest("e2", {"text": "no title here"}, source="webhook", correlation_key="corr:b")
        first, second = await self.files(2)
        self.assertEqual(_header(first)["任务"], "task1.md")
        self.assertEqual((_header(second)["任务"], _header(second)["来源"]), ("corr:b", "/events（webhook）"))

    async def test_approval_message_curl_is_accepted_by_events_and_resumes_the_session(self) -> None:
        if shutil.which("curl") is None:
            self.skipTest("curl is not installed")
        self.ingest("e1", {"title": "Deploy hibernaut", "mode": "suspend"}, correlation_key="gdrive:dep")
        (card,) = await self.files(1)
        h = _header(card)
        self.assertEqual((h["状态"], h["任务"]), ("等待审批", "Deploy hibernaut"))
        self.assertIn("req_file_1", h["需要人做什么"])
        text = card.read_text()
        self.assertIn("审批请求 ID：req_file_1", text)
        self.assertIn('{"inbox_id": "proj:hibernaut", "approval_request_id": "req_file_1", "approved": true}', text)
        self.assertIn("hibernaut_resolve_approval", text)
        approve = text.split("批准：\n\n```bash\n", 1)[1].split("\n```", 1)[0]
        reject = text.split("拒绝：\n\n```bash\n", 1)[1].split("\n```", 1)[0]
        self.assertIn('"approved": false', reject)
        self.assertIn(f"http://127.0.0.1:{self.port}/events", approve)

        proc = await asyncio.to_thread(subprocess.run, shlex.split(approve), capture_output=True, text=True, timeout=10)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertFalse(json.loads(proc.stdout)["duplicate"])

        resumed = (await self.files(2))[1]
        events, session = self.executor.calls[-1]
        assert session is not None
        self.assertEqual(session.pending_request_id, "req_file_1")
        self.assertEqual(events[-1].payload, {"approval_request_id": "req_file_1", "approved": True})
        self.assertIn("resumed approved=True", resumed.read_text())
        self.assertEqual((_header(resumed)["状态"], _header(resumed)["会话 ID"]), ("完成", session.session_id))
        self.assertEqual(_header(resumed)["correlation_key"], "gdrive:dep")


class TestNonWorkerAndOtherChannels(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))

    async def test_message_without_context_writes_none_fields(self) -> None:
        channel, lines = _channel(self.root)
        await channel.send(INBOX, "worktree reclaimed", {"kind": "reclaim_alert"})
        (path,) = _md(self.root)
        h = _header(path)
        self.assertEqual({k: v for k, v in h.items() if k not in ("inbox_id", "时间")},
                         dict.fromkeys(["任务", "来源", "correlation_key", "会话 ID", "执行器", "状态", "需要人做什么"], "无"))
        self.assertIn(" 无 | 无 | ", lines[0])

    def test_context_reads_legacy_approval_rows(self) -> None:
        ctx = outbox_context(INBOX, {"kind": "approval_request", "request_id": "req_old"})
        self.assertEqual((ctx.status, ctx.approval_request_id, ctx.status_label), ("awaiting_approval", "req_old", "等待审批"))

    async def test_switchboard_report_is_unchanged_by_the_task_context(self) -> None:
        published: list[Any] = []
        sb = SwitchboardChannelAdapter(published.append)
        channel, _ = _channel(self.root, sb)
        meta = {"run_id": "run_1", "correlation_key": "github:o/r#1", "kind": "emit", "outcome_kind": "emit",
                "session_id": "sess_1"}
        await channel.send(INBOX, "done text", {**meta, TASK_CONTEXT_KEY: {"title": "T", "executor_id": "sde",
                                                                           "status": "failed", "source": "github:x"}})
        plain = sb.build_report(INBOX, "done text", meta)
        (report,) = published
        assert plain is not None
        self.assertEqual((report.subject, report.body, dict(report.metadata)),
                         (plain.subject, plain.body, dict(plain.metadata)))
        self.assertEqual(report.subject, "Hibernaut update")


class TestDriveWriteBack(unittest.IsolatedAsyncioTestCase):
    async def test_drive_write_back_is_unchanged_with_the_file_outbox(self) -> None:
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        drive = _FakeDriveClient()
        inner = RecordingChannel()
        daemon = build_daemon(
            db_path=root / "gd.db", memory_root=root / "mem", max_concurrency=1,
            executor_factory=lambda _iid: _DriveTestExecutor(), channel=inner, gdrive_client=drive,
            gdrive_root_folder_id="root-1", gdrive_default_inbox_id=INBOX, outbox_dir=root / "outbox",
        )
        self.addCleanup(daemon.close)
        assert daemon.gdrive_poller is not None
        drive.add_file("f1", "task1.md", "---\ntitle: Feature X\n---\nImplement feature X.",
                       modified_time="2026-11-22T03:00:00Z")
        drive.add_file("f2", "Release Plan", "NEED_APPROVAL before deploying to prod.",
                       mime_type="application/vnd.google-apps.document", modified_time="2026-11-22T03:20:00Z")
        stop = asyncio.Event()
        with contextlib.redirect_stdout(io.StringIO()):
            task = asyncio.create_task(daemon.run(stop_event=stop, timer_interval_seconds=0.1,
                                                  outbox_interval_seconds=0.02))
            try:
                daemon.gdrive_poller.poll_once()
                await daemon.pool.wait_until_idle(timeout_seconds=3.0)
                await _until(lambda: len(inner.sent) >= 2)
            finally:
                stop.set()
                await task
        self.assertEqual(drive.files["f1"].parents, ("dir-done",))
        self.assertIn("## 执行结果（Hibernaut）", drive.contents["f1"])
        self.assertIn("Completed task: Feature X", drive.contents["f1"])
        self.assertEqual(drive.files["f2"].parents, ("dir-progress",))
        self.assertIn("等待审批", drive.contents["f2"])
        self.assertIn("req-gd-1", drive.contents["f2"])
        files = _md(root / "outbox")
        self.assertEqual(len(files), len(inner.sent))
        self.assertEqual(sorted(_header(f)["任务"] for f in files), ["Feature X", "Release Plan"])


class TestExecutorStatus(unittest.IsolatedAsyncioTestCase):
    async def test_cli_failure_and_refusal_are_marked(self) -> None:
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        db = SQLiteDatabase(":memory:")
        self.addCleanup(db.close)
        mgr = OrcaWorktreeManager(workspace_store=SQLiteWorkspaceStore(db), backend="git",
                                  runner=lambda _c, _d: CommandResult(0, "", ""))
        fail = CLITemplate(name="claude", command=sys.executable, args=("-I", "-c", "import sys; sys.exit(3)"))
        executor = LocalCLIExecutor(worktree_manager=mgr, inbox_checkout_map={}, templates={"claude": fail})

        class _Caps:
            def is_interrupted(self) -> bool:
                return False

            class memory:
                @staticmethod
                def load_baseline() -> dict[str, str]:
                    return {}

        evt = Event(inbox_id="proj:x", source="test", external_id="t1", payload={"explicit_workdir": str(root)})
        out = await executor.execute([evt], None, _Caps())  # type: ignore[arg-type]
        assert isinstance(out, Emit)
        self.assertEqual(out.status, "failed")
        no_checkout = await executor.execute([Event(inbox_id="proj:x", source="t", external_id="t2", payload={})],
                                             None, _Caps())  # type: ignore[arg-type]
        assert isinstance(no_checkout, Emit)
        self.assertEqual(no_checkout.status, "failed")
        self.assertEqual(Emit(messages=("x",)).status, "done")


if __name__ == "__main__":
    unittest.main()
