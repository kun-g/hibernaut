from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from agent_runtime.adapters.channels import ConsoleChannelAdapter
from agent_runtime.adapters.executor_whitebox import (
    LLMBackend,
    RuleBasedDemoLLM,
    WhiteboxExecutor,
)
from agent_runtime.adapters.memory_fs import FileSystemMemoryAdapter
from agent_runtime.adapters.permissions import AutoModePermissionAdapter
from agent_runtime.adapters.storage_sqlite import (
    SQLiteDatabase,
    SQLiteInboxAdapter,
    SQLiteLeaseLockAdapter,
    SQLiteOutboxAdapter,
    SQLiteSessionStoreAdapter,
    SQLiteTimerAdapter,
)
from agent_runtime.core.ports import ChannelPort, RunCapabilities
from agent_runtime.engine import (
    AgentWorker,
    EventHandler,
    OutboxDispatcher,
    TimerScheduler,
)


@dataclass
class AgentRuntimeApp:
    db: SQLiteDatabase
    handler: EventHandler
    worker: AgentWorker
    scheduler: TimerScheduler
    dispatcher: OutboxDispatcher
    memory: FileSystemMemoryAdapter
    timers: SQLiteTimerAdapter
    outbox: SQLiteOutboxAdapter
    channel: ChannelPort

    def close(self) -> None:
        self.db.close()


def build_runtime(
    *,
    db_path: str | Path = ":memory:",
    memory_dir: str | Path = "./agent_memory",
    worker_id: str = "worker-1",
    llm: LLMBackend | None = None,
    channel: ChannelPort | None = None,
    min_outbox_interval_seconds: float = 0.0,
) -> AgentRuntimeApp:
    db = SQLiteDatabase(db_path)
    inbox = SQLiteInboxAdapter(db)
    timers = SQLiteTimerAdapter(db)
    outbox = SQLiteOutboxAdapter(db)
    lock = SQLiteLeaseLockAdapter(db)
    sessions = SQLiteSessionStoreAdapter(db)
    memory = FileSystemMemoryAdapter(memory_dir)
    permissions = AutoModePermissionAdapter()
    resolved_channel: ChannelPort = channel if channel is not None else ConsoleChannelAdapter()

    def _send_email_tool(args: dict[str, Any], _caps: RunCapabilities) -> str:
        return f"Email sent to {args.get('to')} (subject={args.get('subject')})"

    def _fetch_status_tool(args: dict[str, Any], _caps: RunCapabilities) -> str:
        return f"status=healthy args={args}"

    executor = WhiteboxExecutor(
        llm=llm if llm is not None else RuleBasedDemoLLM(),
        tools={
            "send_email": _send_email_tool,
            "fetch_status": _fetch_status_tool,
        },
    )

    worker = AgentWorker(
        worker_id=worker_id,
        inbox=inbox,
        timers=timers,
        memory=memory,
        permissions=permissions,
        outbox=outbox,
        executor=executor,
        lock=lock,
        sessions=sessions,
    )
    handler = EventHandler(inbox=inbox)
    scheduler = TimerScheduler(timers=timers, inbox=inbox)
    dispatcher = OutboxDispatcher(
        outbox=outbox,
        channel=resolved_channel,
        min_interval_seconds=min_outbox_interval_seconds,
    )

    return AgentRuntimeApp(
        db=db,
        handler=handler,
        worker=worker,
        scheduler=scheduler,
        dispatcher=dispatcher,
        memory=memory,
        timers=timers,
        outbox=outbox,
        channel=resolved_channel,
    )
