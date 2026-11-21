from __future__ import annotations

import argparse
import asyncio
import os
import signal
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from agent_runtime.adapters.channels import (
    ConsoleChannelAdapter,
    FeishuWebhookAdapter,
    GitHubIssuePoller,
    GitHubWebhookAdapter,
    PollTarget,
    build_feishu_or_console_channel,
    create_webhook_server,
)
from agent_runtime.adapters.executor_cli import LocalCLIExecutor, OrcaWorktreeManager
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
    SQLiteWorkspaceStore,
)
from agent_runtime.app.config import HibernautConfig, load_config
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
        self._loop: asyncio.AbstractEventLoop | None = None
        self.reports: list[RunReport] = []

    def wake(self, inbox_id: str) -> None:
        # HTTP webhook threads call wake() off-loop; marshal safely onto the event loop.
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if self._loop is not None and running is not self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._wake_on_loop, inbox_id)
            return
        self._wake_on_loop(inbox_id)

    def _wake_on_loop(self, inbox_id: str) -> None:
        active_worker = self._active.get(inbox_id)
        if active_worker is not None:
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
                    self._wake_on_loop(inbox_id)
                elif not self._queued and not self._active:
                    self._idle_event.set()

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        self._loop = asyncio.get_running_loop()
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
    workspaces: SQLiteWorkspaceStore
    channel: ChannelPort
    github_webhook: GitHubWebhookAdapter | None = None
    feishu_webhook: FeishuWebhookAdapter | None = None
    github_poller: GitHubIssuePoller | None = None
    poll_interval_seconds: float = 60.0
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
            server = create_webhook_server(
                webhook_host,
                webhook_port,
                self.handler,
                github_adapter=self.github_webhook,
                feishu_adapter=self.feishu_webhook,
            )
            server_thread = threading.Thread(target=server.serve_forever, daemon=True)
            server_thread.start()

        loops: list[Any] = [
            self.pool.run_forever(stopper),
            self.scheduler.run_forever(
                interval_seconds=timer_interval_seconds,
                stop_event=stopper,
            ),
            self.dispatcher.run_forever(
                interval_seconds=outbox_interval_seconds,
                stop_event=stopper,
            ),
        ]
        if self.github_poller is not None:
            loops.append(
                self.github_poller.run_forever(
                    interval_seconds=self.poll_interval_seconds,
                    stop_event=stopper,
                )
            )

        try:
            await asyncio.gather(*loops)
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
    github_webhook_secret: str | None = None,
    github_repo_inbox_map: Mapping[str, str] | None = None,
    feishu_verification_token: str | None = None,
    feishu_chat_inbox_map: Mapping[str, str] | None = None,
    poll_targets: list[PollTarget] | None = None,
    poll_interval_seconds: float = 60.0,
) -> AlwaysOnDaemon:
    db = SQLiteDatabase(db_path)
    inbox = SQLiteInboxAdapter(db)
    timers = SQLiteTimerAdapter(db)
    outbox = SQLiteOutboxAdapter(db)
    lock = SQLiteLeaseLockAdapter(db)
    sessions = SQLiteSessionStoreAdapter(db)
    workspaces = SQLiteWorkspaceStore(db)
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
    github_webhook = GitHubWebhookAdapter(
        handler,
        webhook_secret=github_webhook_secret,
        repo_inbox_map=github_repo_inbox_map,
    )
    feishu_webhook = FeishuWebhookAdapter(
        handler,
        verification_token=feishu_verification_token,
        chat_inbox_map=feishu_chat_inbox_map,
    )
    github_poller = (
        GitHubIssuePoller(
            handler,
            targets=poll_targets,
            repo_inbox_map=github_repo_inbox_map,
        )
        if poll_targets
        else None
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
        workspaces=workspaces,
        channel=resolved_channel,
        github_webhook=github_webhook,
        feishu_webhook=feishu_webhook,
        github_poller=github_poller,
        poll_interval_seconds=poll_interval_seconds,
    )


def build_daemon_from_config(
    config: HibernautConfig,
    *,
    channel: ChannelPort | None = None,
) -> AlwaysOnDaemon:
    repo_inbox_map: dict[str, str] = {}
    inbox_checkout_map: dict[str, str] = {}
    inbox_chat_map: dict[str, str] = {}
    chat_inbox_map: dict[str, str] = {}
    poll_targets: list[PollTarget] = []
    project_backends: dict[str, tuple[str, bool]] = {}

    for proj in config.projects.values():
        repo_inbox_map[proj.repo] = proj.inbox_id
        inbox_checkout_map[proj.inbox_id] = proj.checkout
        project_backends[proj.inbox_id] = (proj.worktree_backend, proj.orca_create_terminal)
        if proj.feishu_chat_id:
            inbox_chat_map[proj.inbox_id] = proj.feishu_chat_id
            chat_inbox_map[proj.feishu_chat_id] = proj.inbox_id
        poll_targets.append(
            PollTarget(
                repo=proj.repo,
                inbox_id=proj.inbox_id,
                labels=proj.poll_labels,
            )
        )

    resolved_channel = channel or build_feishu_or_console_channel(
        app_id_env=config.channels.feishu_app_id_env,
        app_secret_env=config.channels.feishu_app_secret_env,
        default_chat_id=config.channels.feishu_default_chat_id or None,
        inbox_chat_map=inbox_chat_map,
    )

    gh_secret = os.environ.get(config.channels.github_webhook_secret_env) or None
    feishu_token = os.environ.get(config.channels.feishu_verification_token_env) or None

    # Create DB & workspace store up front so LocalCLIExecutor shares the same SQLite database.
    daemon = build_daemon(
        db_path=config.runtime.db_path,
        memory_root=config.runtime.memory_root,
        max_concurrency=config.runtime.max_concurrency,
        channel=resolved_channel,
        new_event_policy=config.runtime.new_event_policy,
        github_webhook_secret=gh_secret,
        github_repo_inbox_map=repo_inbox_map,
        feishu_verification_token=feishu_token,
        feishu_chat_inbox_map=chat_inbox_map,
        poll_targets=poll_targets,
        poll_interval_seconds=config.runtime.poll_interval_seconds,
    )

    if config.projects:
        executors_by_inbox: dict[str, LocalCLIExecutor] = {}
        for proj in config.projects.values():
            wt_mgr = OrcaWorktreeManager(
                workspace_store=daemon.workspaces,
                backend=proj.worktree_backend,
                create_terminal=proj.orca_create_terminal,
            )
            executors_by_inbox[proj.inbox_id] = LocalCLIExecutor(
                worktree_manager=wt_mgr,
                inbox_checkout_map=inbox_checkout_map,
                templates=config.executors,
                default_executor=config.runtime.default_executor,
            )

        default_mgr = OrcaWorktreeManager(workspace_store=daemon.workspaces)
        fallback_cli = LocalCLIExecutor(
            worktree_manager=default_mgr,
            inbox_checkout_map=inbox_checkout_map,
            templates=config.executors,
            default_executor=config.runtime.default_executor,
        )

        def _cli_worker_factory(inbox_id: str, worker_id: str) -> AgentWorker:
            return AgentWorker(
                worker_id=worker_id,
                inbox=daemon.inbox,
                timers=daemon.timers,
                memory=daemon.partitions.memory_for(inbox_id),
                permissions=AutoModePermissionAdapter(),
                outbox=daemon.outbox,
                executor=executors_by_inbox.get(inbox_id, fallback_cli),
                lock=daemon.lock,
                sessions=daemon.sessions,
                new_event_policy=config.runtime.new_event_policy,
            )

        daemon.pool = AsyncWorkerPool(
            worker_factory=_cli_worker_factory,
            max_concurrency=config.runtime.max_concurrency,
        )
        daemon.handler = EventHandler(inbox=daemon.inbox, on_wake=daemon.pool.wake)
        daemon.scheduler = TimerScheduler(
            timers=daemon.timers,
            inbox=daemon.inbox,
            on_wake=daemon.pool.wake,
        )
        daemon.github_webhook = GitHubWebhookAdapter(
            daemon.handler,
            webhook_secret=gh_secret,
            repo_inbox_map=repo_inbox_map,
        )
        daemon.feishu_webhook = FeishuWebhookAdapter(
            daemon.handler,
            verification_token=feishu_token,
            chat_inbox_map=chat_inbox_map,
        )
        daemon.github_poller = GitHubIssuePoller(
            daemon.handler,
            targets=poll_targets,
            mode=config.runtime.poll_mode,
            github_token_env=config.channels.github_token_env,
            repo_inbox_map=repo_inbox_map,
        )

    return daemon


def main() -> None:
    parser = argparse.ArgumentParser(description="Hibernaut multi-inbox Always-on Agent daemon")
    parser.add_argument("--config", default=None, help="Path to hibernaut.toml")
    parser.add_argument("--db", default="./hibernaut.db")
    parser.add_argument("--memory-root", default="./agent_memory")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--policy", choices=("defer", "fold", "interrupt"), default="defer")
    parser.add_argument("--github-secret", default=None)
    parser.add_argument("--feishu-token", default=None)
    args = parser.parse_args()

    if args.config:
        cfg = load_config(args.config)
        daemon = build_daemon_from_config(cfg)
        host = cfg.channels.host
        port = cfg.channels.port
    else:
        daemon = build_daemon(
            db_path=args.db,
            memory_root=args.memory_root,
            max_concurrency=args.workers,
            new_event_policy=args.policy,
            github_webhook_secret=args.github_secret,
            feishu_verification_token=args.feishu_token,
        )
        host = args.host
        port = args.port

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
                webhook_host=host,
                webhook_port=port,
            )
        finally:
            daemon.close()

    asyncio.run(_serve())


if __name__ == "__main__":
    main()
