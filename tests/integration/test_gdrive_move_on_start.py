from __future__ import annotations

import asyncio
import textwrap

from agent_runtime.app.config import load_config
from agent_runtime.app.main import build_daemon_from_config
from tests.support.gdrive_tasks import _FakeDriveClient
from tests.support.project_inbox_rules import _DispatchCase, project_table
from tests.support.gdrive_move_on_start import DONE, PENDING, PROGRESS


_EMIT_CLI = textwrap.dedent(
    """
    import json
    print(json.dumps({"outcome": "emit", "messages": ["drive task ran"]}))
    """
)


class TestDriveHoldThroughConfig(_DispatchCase):
    async def test_a_held_drive_task_stays_queued_and_a_matched_one_moves_on_start(self) -> None:
        self.cli.write_text(_EMIT_CLI)
        path = self.write_config(
            project_table("p", "proj:p", self.repo)
            + '[gdrive]\nroot_folder_id = "root-1"\ndefault_inbox_id = "proj:orphan"\n'
        )
        drive = _FakeDriveClient()
        daemon = build_daemon_from_config(load_config(path), gdrive_client=drive)
        self.addCleanup(daemon.close)
        assert daemon.gdrive_poller is not None
        drive.add_file("held", "held.md", "No project for me.")
        drive.add_file("run", "run.md", "---\nscope: proj:p\n---\nRun me.", modified_time="2026-11-22T03:01:00Z")
        stop = asyncio.Event()
        pool = asyncio.create_task(daemon.pool.run_forever(stop))
        try:
            with self.assertLogs("agent_runtime.app.projects", level="WARNING"):
                daemon.gdrive_poller.poll_once()
                await asyncio.sleep(0)
                await daemon.pool.wait_until_idle(timeout_seconds=60)
            self.assertEqual(daemon.gdrive_poller.poll_once(), [])
        finally:
            stop.set()
            await pool
        self.assertEqual(drive.files["held"].parents, (PENDING,))
        self.assertEqual(drive.move_history[0], ("run", PENDING, PROGRESS))
        self.assertEqual(drive.files["run"].parents, (DONE,))
        held = daemon.db.fetchall("SELECT status FROM event_inbox WHERE inbox_id = 'proj:orphan'")
        self.assertEqual([r["status"] for r in held], ["pending"])
