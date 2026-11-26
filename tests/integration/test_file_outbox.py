from __future__ import annotations

import asyncio
import contextlib
import io
import json
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any

from agent_runtime.adapters.channels import WebhookHTTPServer, local_events_url
from agent_runtime.adapters.executor_cli.executor import CLITemplate, LocalCLIExecutor
from agent_runtime.adapters.executor_cli.worktree import CommandResult, OrcaWorktreeManager
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteWorkspaceStore
from agent_runtime.app.main import build_daemon
from agent_runtime.core.models import Emit, Event
from tests.support.file_outbox import INBOX, _ScriptedExecutor, _header, _md, _until


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
