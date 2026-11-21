"""Runnable end-to-end demonstration of the Sleep -> Wake -> Run -> Suspend -> Resume cycle."""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

from agent_runtime.app.bootstrap import build_runtime
from agent_runtime.core.models import Suspend


async def run_demo() -> None:
    with tempfile.TemporaryDirectory(prefix="always_on_demo_") as tmp:
        root = Path(tmp)
        app = build_runtime(
            db_path=root / "runtime.db",
            memory_dir=root / "memory",
        )
        try:
            app.memory.write("instructions.md", "默认静默运行，仅在有明确结果或需审批时发声。")

            # 1. Silent Run: background heartbeat produces no user notification
            app.handler.ingest(
                inbox_id="user-1",
                source="cron",
                external_id="hb-001",
                payload={"action": "heartbeat_check"},
            )
            r1 = await app.worker.process_inbox("user-1")
            print("Run 1 (Silent):", r1[0].outcome)

            # 2. Suspend Run: high-impact send_email triggers async approval without blocking Worker
            app.handler.ingest(
                inbox_id="user-1",
                source="im",
                external_id="msg-002",
                payload={"action": "send_email", "to": "boss@example.com", "subject": "周报"},
            )
            r2 = await app.worker.process_inbox("user-1")
            await app.dispatcher.dispatch_once()
            assert isinstance(r2[0].outcome, Suspend)
            req_id = r2[0].outcome.request_id

            # 3. Resume Run: human approval arrives as a new Event in Inbox -> resumes suspended Session
            app.handler.ingest(
                inbox_id="user-1",
                source="approval_ui",
                external_id="appr-002",
                payload={"approval_request_id": req_id, "approved": True},
            )
            r3 = await app.worker.process_inbox("user-1")
            await app.dispatcher.dispatch_once()
            print("Run 3 (Resumed Emit):", r3[0].outcome)
        finally:
            app.close()


if __name__ == "__main__":
    asyncio.run(run_demo())
