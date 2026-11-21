from __future__ import annotations

import argparse
import asyncio
import signal
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from agent_runtime.adapters.channels import ConsoleChannelAdapter, create_webhook_server
from agent_runtime.adapters.executor_whitebox import (
    LLMBackend,
    RuleBasedDemoLLM,
    WhiteboxExecutor,
)
from agent_runtime.adapters.memory_fs import WorkspacePartitionRegistry
from agent_runtime.adapters.permissions import AutoModePermissionAdapter
from agent_runtime.adapters.storage_sqlite import (
    SQLiteDatabase,
    SQLiteInboxAdapter,
    SQLiteLeaseLockAdapter,
    SQLiteOutboxAdapter,
    SQLiteSessionStoreAdapter,
    SQLiteTimerAdapter,
)
from agent_runtime.core.models import NewEventPolicy
from agent_runtime.core.ports import ChannelPort, ExecutorPort, RunCapabilities
from agent_runtime.engine import (
    AgentWorker,
    EventHandler,
    OutboxDispatcher,
    RunReport,
    TimerScheduler,
)


class AsyncWorkerPool:
    def __init__(
        self,
        *,
        worker_factory: Callable[[str, str], AgentWorker],
        max_concurrency: int = 4,
    ) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        self._worker_factory = worker_factory
        self._max_concurrency = max_concurrency
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._queued: set[str] = set()
        self._active: dict[str, AgentWorker] = {}
        self._requeue: set[str] = set()
        self._idle_event = asyncio.Event()
        self._idle_event.set()
        self.reports: list[RunReport] = []

    def wake(self, inbox_id: str) -> None:
        active_worker = self._active.get(inbox_id)
        if active_worker is not None:
            # Notify active worker immediately (for interrupt policy) and ensure re-drain.
            active_worker.notify_new_event(inbox_id)
            self._requeue.add(inbox_id)
            return
        if inbox_id in self._queued:
            return
        self._queued.add(inbox_id)
        self._idle_event.clear()
        self._queue.put_nowait(inbox_id)

    async def _worker_loop(self, worker_idx: int, stop_event: asyncio.Event) -> None:
        worker_id = f"pool-worker-{worker_idx}"
        while not stop_event.is_set() or not self._queue.empty():
            try:
                inbox_id = await asyncio.wait_for(self._queue.get(), timeout=0.05)
            except asyncio.TimeoutError:
                continue

            self._queued.discard(inbox_id)
            worker = self._worker_factory(inbox_id, worker_id)
            self._active[inbox_id] = worker
            try:
                batch_reports = await worker.process_inbox(inbox_id)
                self.reports.extend(batch_reports)
            finally:
                self._active.pop(inbox_id, None)
                self._queue.task_done()
                if inbox_id in self._requeue:
                    self._requeue.discard(inbox_id)
                    self.wake(inbox_id)
                elif not self._queued and not self._active:
                    self._idle_event.set()

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        tasks = [
            asyncio.create_task(self._worker_loop(idx, stop_event))
            for idx in range(self._max_concurrency)
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()

    async def wait_until_idle(self, timeout_seconds: float = 5.0) -> None:
        await asyncio.wait_for(self._idle_event.wait(), timeout=timeout_seconds)


@dataclass
class AlwaysOnDaemon:
    db: SQLiteDatabase
    partitions: WorkspacePartitionRegistry
    handler: EventHandler
    scheduler: TimerScheduler
    dispatcher: OutboxDispatcher
    pool: AsyncWorkerPool
    inbox: SQLiteInboxAdapter
    timers: SQLiteTimerAdapter
    outbox: SQLiteOutboxAdapter
    lock: SQLiteLeaseLockAdapter
    sessions: SQLiteSessionStoreAdapter
    channel: ChannelPort
    _stop_event: asyncio.Event = field(default_factory=asyncio.Event)

    def register_partition(
        self,
        inbox_id: str,
        workspace_dir: str | Path | None = None,
    ) -> None:
        self.partitions.register(inbox_id, workspace_dir=workspace_dir)

    async def run(
        self,
        *,
        stop_event: asyncio.Event | None = None,
        timer_interval_seconds: float = 5.0,
        outbox_interval_seconds: float = 1.0,
        webhook_host: str | None = None,
        webhook_port: int | None = None,
    ) -> None:
        stopper = stop_event or self._stop_event
        server = None
        server_thread = None

        if webhook_host is not None and webhook_port is not None:
            server = create_webhook_server(webhook_host, webhook_port, self.handler)
            server_thread = threading.Thread(target=server.serve_forever, daemon=True)
            server_thread.start()

        try:
            await asyncio.gather(
                self.pool.run_forever(stopper),
                self.scheduler.run_forever(
                    interval_seconds=timer_interval_seconds,
                    stop_event=stopper,
                ),
                self.dispatcher.run_forever(
                    interval_seconds=outbox_interval_seconds,
                    stop_event=stopper,
                ),
            )
        finally:
            if server is not None:
                server.shutdown()
                server.server_close()
            if server_thread is not None:
                server_thread.join(timeout=2.0)

    def close(self) -> None:
        self.db.close()


def build_daemon(
    *,
    db_path: str | Path = "./hibernaut.db",
    memory_root: str | Path = "./agent_memory",
    max_concurrency: int = 4,
    llm: LLMBackend | None = None,
    executor_factory: Callable[[str], ExecutorPort] | None = None,
    channel: ChannelPort | None = None,
    min_outbox_interval_seconds: float = 0.0,
    lease_ttl_seconds: float = 60.0,
    new_event_policy: NewEventPolicy = "defer",
) -> AlwaysOnDaemon:
    db = SQLiteDatabase(db_path)
    inbox = SQLiteInboxAdapter(db)
    timers = SQLiteTimerAdapter(db)
    outbox = SQLiteOutboxAdapter(db)
    lock = SQLiteLeaseLockAdapter(db)
    sessions = SQLiteSessionStoreAdapter(db)
    partitions = WorkspacePartitionRegistry(memory_root)
    permissions = AutoModePermissionAdapter()
    resolved_channel: ChannelPort = channel if channel is not None else ConsoleChannelAdapter()

    def _send_email_tool(args: dict[str, Any], _caps: RunCapabilities) -> str:
        return f"Email sent to {args.get('to')} (subject={args.get('subject')})"

    def _fetch_status_tool(args: dict[str, Any], _caps: RunCapabilities) -> str:
        return f"status=healthy args={args}"

    default_executor = WhiteboxExecutor(
        llm=llm if llm is not None else RuleBasedDemoLLM(),
        tools={
            "send_email": _send_email_tool,
            "fetch_status": _fetch_status_tool,
        },
    )

    def _make_worker(inbox_id: str, worker_id: str) -> AgentWorker:
        executor = executor_factory(inbox_id) if executor_factory is not None else default_executor
        return AgentWorker(
            worker_id=worker_id,
            inbox=inbox,
            timers=timers,
            memory=partitions.memory_for(inbox_id),
            permissions=permissions,
            outbox=outbox,
            executor=executor,
            lock=lock,
            sessions=sessions,
            lease_ttl_seconds=lease_ttl_seconds,
            new_event_policy=new_event_policy,
        )

    pool = AsyncWorkerPool(
        worker_factory=_make_worker,
        max_concurrency=max_concurrency,
    )
    handler = EventHandler(inbox=inbox, on_wake=pool.wake)
    scheduler = TimerScheduler(timers=timers, inbox=inbox, on_wake=pool.wake)
    dispatcher = OutboxDispatcher(
        outbox=outbox,
        channel=resolved_channel,
        min_interval_seconds=min_outbox_interval_seconds,
    )

    return AlwaysOnDaemon(
        db=db,
        partitions=partitions,
        handler=handler,
        scheduler=scheduler,
        dispatcher=dispatcher,
        pool=pool,
        inbox=inbox,
        timers=timers,
        outbox=outbox,
        lock=lock,
        sessions=sessions,
        channel=resolved_channel,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Hibernaut multi-inbox Always-on Agent daemon")
    parser.add_argument("--db", default="./hibernaut.db")
    parser.add_argument("--memory-root", default="./agent_memory")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--policy", choices=("defer", "fold", "interrupt"), default="defer")
    args = parser.parse_args()

    daemon = build_daemon(
        db_path=args.db,
        memory_root=args.memory_root,
        max_concurrency=args.workers,
        new_event_policy=args.policy,
    )

    async def _serve() -> None:
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except NotImplementedError:
                pass
        try:
            await daemon.run(
                stop_event=stop_event,
                webhook_host=args.host,
                webhook_port=args.port,
            )
        finally:
            daemon.close()

    asyncio.run(_serve())


if __name__ == "__main__":
    main()
