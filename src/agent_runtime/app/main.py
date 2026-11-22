from __future__ import annotations

import argparse
import asyncio
import itertools
import logging
import os
import signal
import ssl
import sys
import threading
from collections.abc import Callable, Iterable, Mapping, MutableMapping
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from agent_runtime import __version__
from agent_runtime.adapters.channels import (
    ConsoleChannelAdapter,
    DriveClientProtocol,
    DriveResultWriter,
    DriveTaskPoller,
    FeishuWebhookAdapter,
    FileOutboxChannel,
    MCPServerAdapter,
    ProxiedDriveClient,
    WebhookHTTPServer,
    build_feishu_or_console_channel,
    local_events_url,
)
from agent_runtime.adapters.channels.channels import webhook_request_handler
from agent_runtime.adapters.channels.switchboard_bridge import SwitchboardChannelAdapter, SwitchboardInboundBridge
from agent_runtime.adapters.channels.switchboard_relay import SwitchboardBus, SwitchboardRelayLink
from agent_runtime.adapters.delivery import DeliveryConfig, PRDelivery, merged_head
from agent_runtime.adapters.executor_cli import (
    CODE_REVIEWER_ROLE,
    DEFAULT_CLI_TEMPLATES,
    CLIReviewer,
    CoderReviewerExecutor,
    LocalCLIExecutor,
    OrcaWorktreeManager,
    Reviewer,
    SDEExecutor,
)
from agent_runtime.adapters.executor_cli.orca import (
    OrcaCommandTerminal,
    OrcaVisibleRunner,
    TerminalUnavailable,
    default_transcript_roots,
)
from agent_runtime.adapters.executor_cli.worktree import ReclaimResult
from agent_runtime.adapters.executor_cli.peer import DeveloperPeerResolver, registry_dirs
from agent_runtime.adapters.executor_whitebox import (
    LLMBackend,
    RuleBasedDemoLLM,
    WhiteboxExecutor,
)
from agent_runtime.adapters.memory_fs import WorkspacePartitionRegistry
from agent_runtime.adapters.permissions import AutoModePermissionAdapter, CredentialProxyClient, read_private_token
from agent_runtime.adapters.storage_sqlite.terminals import SQLiteTerminalBindingStore
from agent_runtime.adapters.storage_sqlite import (
    SQLiteDatabase,
    SQLiteDeliveryStore,
    SQLiteWorkUsage,
    SQLiteInboxAdapter,
    SQLiteLeaseLockAdapter,
    SQLiteOutboxAdapter,
    SQLiteSessionStoreAdapter,
    SQLiteTimerAdapter,
    SQLiteTaskMetadataStore,
    SQLiteWorkspaceStore,
)
from agent_runtime.app.config import (
    HibernautConfig,
    RoleConfig,
    SwitchboardLinkConfig,
    config_changes,
    load_config,
)
from agent_runtime.app.projects import ProjectRouter, UnmatchedProjectHold
from agent_runtime.core.models import NewEventPolicy
from agent_runtime.core.ports import ChannelPort, ExecutorPort, OutboxPort, RunCapabilities
from agent_runtime.engine import (
    AgentWorker,
    EventHandler,
    OutboxDispatcher,
    RunReport,
    TimerScheduler,
)
from agent_runtime.engine.worker import ExecutorFailed
from switchboard.app import EnvCredentialBroker, Switchboard, build_switchboard
from switchboard.config import load_config as load_switchboard_config

SWITCHBOARD_SECRET_PREFIX = "SWITCHBOARD_"
SWITCHBOARD_RELAY_TOKEN_ENV = "SWITCHBOARD_RELAY_TOKEN"
SWITCHBOARD_MCP_TOKEN_ENV = "SWITCHBOARD_MCP_TOKEN"
_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})

# in_process: the whole Switchboard runs here. relay (Stage B): a link to a remote hub.
SwitchboardHandle = Switchboard | SwitchboardRelayLink

# Lease holders must differ across pools: a config reload runs the old pool's last Runs beside the new pool.
_POOL_IDS = itertools.count(1)


class AsyncWorkerPool:
    def __init__(
        self,
        *,
        worker_factory: Callable[[str, str], AgentWorker],
        max_concurrency: int = 4,
        on_run_reports: Callable[[list[RunReport]], None] | None = None,
        on_inbox_failed: Callable[[str], None] | None = None,
        pending_inboxes: Callable[[], Iterable[str]] | None = None,
    ) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        self._worker_factory = worker_factory
        self._max_concurrency = max_concurrency
        self._on_run_reports = on_run_reports
        self._on_inbox_failed = on_inbox_failed
        self._pending_inboxes = pending_inboxes
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._queued: set[str] = set()
        self._active: dict[str, AgentWorker] = {}
        self._requeue: set[str] = set()
        self._idle_event = asyncio.Event()
        self._idle_event.set()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pool_id = f"pool-{next(_POOL_IDS)}"
        self._handoff: Callable[[str], None] | None = None
        self.reports: list[RunReport] = []

    def bind(self, loop: asyncio.AbstractEventLoop) -> None:
        # Bind before ingress can reach the pool, so wakes from HTTP threads are marshalled from the first request.
        self._loop = loop

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
        if self._handoff is not None:
            self._handoff(inbox_id)
            return
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

    def retire(self, handoff: Callable[[str], None], *, interruptible: bool = True) -> None:
        # Called on the event loop when a config reload replaces this pool. Runs in progress finish with this
        # generation's executors; queued inboxes, later wakes and each inbox whose lease is released go to handoff,
        # which wakes the current pool. worker_leases keeps the two pools off the same inbox.
        self._handoff = handoff
        for worker in self._active.values():
            worker.retire(interruptible=interruptible)
        self._requeue.clear()
        while not self._queue.empty():
            inbox_id = self._queue.get_nowait()
            self._queue.task_done()
            self._queued.discard(inbox_id)
            handoff(inbox_id)
        if not self._active:
            self._idle_event.set()

    def drain(self) -> None:
        # Called on the event loop when the process stops: Runs in progress finish and write their results, nothing
        # new is claimed, and wakes are dropped; their events stay pending for the next process's startup scan.
        self.retire(lambda _inbox_id: None, interruptible=False)

    async def _worker_loop(self, worker_idx: int, stop_event: asyncio.Event) -> None:
        worker_id = f"{self._pool_id}-worker-{worker_idx}"
        while self._handoff is None and (not stop_event.is_set() or not self._queue.empty()):
            try:
                inbox_id = await asyncio.wait_for(self._queue.get(), timeout=0.05)
            except asyncio.TimeoutError:
                continue

            self._queued.discard(inbox_id)
            if self._handoff is not None:
                self._queue.task_done()
                self._handoff(inbox_id)
                break
            try:
                # Inside the try: a factory that refuses the inbox (AmbiguousProjectMatch) leaves its events pending.
                worker = self._worker_factory(inbox_id, worker_id)
                self._active[inbox_id] = worker
                batch_reports = await worker.process_inbox(inbox_id)
                self.reports.extend(batch_reports)
                if batch_reports and self._on_run_reports is not None:
                    try:
                        await asyncio.to_thread(self._on_run_reports, batch_reports)
                    except Exception:  # noqa: BLE001
                        logging.getLogger(__name__).exception(
                            "Post-run callback failed for inbox %s",
                            inbox_id,
                        )
            except ExecutorFailed:
                logging.getLogger(__name__).exception(
                    "Executor failed for inbox %s; batch remains pending",
                    inbox_id,
                )
                if self._on_inbox_failed is not None:
                    try:
                        await asyncio.to_thread(self._on_inbox_failed, inbox_id)
                    except Exception:  # noqa: BLE001
                        logging.getLogger(__name__).exception(
                            "Failure callback failed for inbox %s",
                            inbox_id,
                        )
            finally:
                self._active.pop(inbox_id, None)
                self._queue.task_done()
                if self._handoff is not None:
                    # The lease is released now; the current pool picks up whatever arrived meanwhile.
                    self._handoff(inbox_id)
                    if not self._active:
                        self._idle_event.set()
                elif inbox_id in self._requeue:
                    self._requeue.discard(inbox_id)
                    self._wake_on_loop(inbox_id)
                elif not self._queued and not self._active:
                    self._idle_event.set()

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        self._loop = asyncio.get_running_loop()
        if self._pending_inboxes is not None:
            # Events left pending by an earlier process or config generation (e.g. held for a missing project)
            # are re-matched once against this generation's rules; nothing re-polls them afterwards.
            for inbox_id in self._pending_inboxes():
                self._wake_on_loop(inbox_id)
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
    feishu_webhook: FeishuWebhookAdapter | None = None
    gdrive_poller: DriveTaskPoller | None = None
    gdrive_result_writer: DriveResultWriter | None = None
    mcp_server: MCPServerAdapter | None = None
    switchboard_bridge: SwitchboardInboundBridge | None = None
    gdrive_poll_interval_seconds: float = 30.0
    ingest_only: bool = False
    _stop_event: asyncio.Event = field(default_factory=asyncio.Event)

    def register_partition(
        self,
        inbox_id: str,
        workspace_dir: str | Path | None = None,
    ) -> None:
        self.partitions.register(inbox_id, workspace_dir=workspace_dir)

    def http_handler(self) -> type[BaseHTTPRequestHandler]:
        return webhook_request_handler(
            self.handler,
            feishu_adapter=self.feishu_webhook,
            mcp_adapter=self.mcp_server,
        )

    async def run(
        self,
        *,
        server: HTTPServer | None = None,
        stop_event: asyncio.Event | None = None,
        force_stop_event: asyncio.Event | None = None,
        timer_interval_seconds: float = 5.0,
        outbox_interval_seconds: float = 1.0,
        webhook_host: str | None = None,
        webhook_port: int | None = None,
    ) -> None:
        stopper = stop_event or self._stop_event
        self.pool.bind(asyncio.get_running_loop())
        owns_server = server is None
        server_thread = None
        if server is None and webhook_host is not None and webhook_port is not None:
            server = WebhookHTTPServer((webhook_host, webhook_port), self.http_handler())
        if server is not None:
            server_thread = threading.Thread(
                target=server.serve_forever,
                kwargs={"poll_interval": 0.02},
                daemon=True,
            )
            server_thread.start()

        workers_stop = asyncio.Event()
        services_stop = asyncio.Event()
        async with asyncio.TaskGroup() as tasks:
            runs = [] if self.ingest_only else [tasks.create_task(self.pool.run_forever(workers_stop))]
            services = tasks.create_task(
                self.run_services(
                    services_stop,
                    timer_interval_seconds=timer_interval_seconds,
                    outbox_interval_seconds=outbox_interval_seconds,
                )
            )
            try:
                await stopper.wait()
            finally:
                # Ingress and services keep running until the Runs in progress finish.
                await drain_pools([self.pool], runs, force_stop_event)
                workers_stop.set()
                if server is not None:
                    await asyncio.to_thread(server.shutdown)
                    if owns_server:
                        server.server_close()
                if server_thread is not None:
                    server_thread.join()
                services_stop.set()
                await services

    async def run_services(
        self,
        stop_event: asyncio.Event,
        *,
        timer_interval_seconds: float = 5.0,
        outbox_interval_seconds: float = 1.0,
    ) -> None:
        # Everything but HTTP ingress and the worker pool; each loop finishes its current tick once stop_event is set.
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(self.scheduler.run_forever(timer_interval_seconds, stop_event))
            tasks.create_task(self.dispatcher.run_forever(outbox_interval_seconds, stop_event))
            if self.gdrive_poller is not None:
                tasks.create_task(
                    self.gdrive_poller.run_forever(
                        interval_seconds=self.gdrive_poll_interval_seconds,
                        stop_event=stop_event,
                    )
                )

    def close(self) -> None:
        self.db.close()


async def drain_pools(
    pools: Iterable[AsyncWorkerPool],
    runs: Iterable[asyncio.Task[None]],
    force_stop: asyncio.Event | None,
) -> None:
    # First stage of a stop: no pool claims anything new while the Runs in progress (each pool's run_forever task)
    # finish. force_stop (the second signal) cancels them instead; a cancelled Run writes nothing to the Outbox and
    # its events return to pending, as on interrupt.
    for pool in pools:
        pool.drain()
    pending: set[asyncio.Future[Any]] = {task for task in runs if not task.done()}
    if not pending:
        return
    print(
        "[hibernaut] Stopping: waiting for runs in progress; ingress, Outbox delivery, timers and polling keep "
        "running. Send SIGINT or SIGTERM again to exit without waiting.",
        file=sys.stderr,
        flush=True,
    )
    forced = asyncio.ensure_future(force_stop.wait()) if force_stop is not None else None
    try:
        while pending:
            done, _ = await asyncio.wait(
                pending | ({forced} if forced is not None else set()),
                return_when=asyncio.FIRST_COMPLETED,
            )
            pending -= done
            if forced is not None and forced in done and pending:
                print(
                    "[hibernaut] Second stop signal: cancelling runs in progress; their events stay pending.",
                    file=sys.stderr,
                    flush=True,
                )
                for task in pending:
                    task.cancel()
                await asyncio.wait(pending)
                return
    finally:
        if forced is not None:
            forced.cancel()


def worktree_reclaimer(
    manager: OrcaWorktreeManager,
    *,
    outbox: OutboxPort,
    inbox_id: str,
    delivery: DeliveryConfig | None,
    deliveries: SQLiteDeliveryStore,
) -> Callable[[str], ReclaimResult]:
    # A task whose delivery merged its PR loses its local branch with the worktree (delivery.delete_branch).
    def reclaim(work_key: str) -> ReclaimResult:
        head = merged_head(delivery, deliveries, manager.workspace_key(inbox_id, work_key))
        return manager.reclaim(work_key, notify_outbox=outbox, inbox_id=inbox_id, merged_head=head)

    return reclaim


def _report_failed_events(inbox: SQLiteInboxAdapter, writer: DriveResultWriter, inbox_id: str) -> None:
    failed = inbox.list_events(inbox_id=inbox_id, status="failed", limit=20)
    diagnostics = inbox.diagnostics([evt.event_id for evt in failed])
    for evt in failed:
        diag = diagnostics.get(evt.event_id)
        writer.handle_failed_event(
            evt,
            error=diag.last_error if diag else None,
            attempts=diag.attempts if diag else 0,
        )


def build_daemon(
    *,
    db_path: str | Path = "./hibernaut.db",
    database: SQLiteDatabase | None = None,
    memory_root: str | Path = "./agent_memory",
    max_concurrency: int = 4,
    llm: LLMBackend | None = None,
    executor_factory: Callable[[str], ExecutorPort] | None = None,
    executors_factory: Callable[[str], Mapping[str, ExecutorPort]] | None = None,
    project_default_executor: str | None = None,
    global_default_executor: str | None = None,
    channel: ChannelPort | None = None,
    min_outbox_interval_seconds: float = 0.0,
    lease_ttl_seconds: float = 60.0,
    new_event_policy: NewEventPolicy = "defer",
    feishu_verification_token: str | None = None,
    feishu_chat_inbox_map: Mapping[str, str] | None = None,
    mcp_bearer_token: str | None = None,
    worktree_manager: OrcaWorktreeManager | None = None,
    ingest_only: bool = False,
    gdrive_client: DriveClientProtocol | None = None,
    gdrive_root_folder_id: str | None = None,
    gdrive_pending_folder_id: str | None = None,
    gdrive_in_progress_folder_id: str | None = None,
    gdrive_done_folder_id: str | None = None,
    gdrive_failed_folder_id: str | None = None,
    gdrive_poll_interval_seconds: float = 30.0,
    gdrive_default_inbox_id: str = "default",
    outbox_dir: str | Path | None = None,
    outbox_display_base: str | Path | None = None,
    approval_events_url: str = "http://127.0.0.1:8080/events",
) -> AlwaysOnDaemon:
    db = database if database is not None else SQLiteDatabase(db_path)
    inbox = SQLiteInboxAdapter(db)
    timers = SQLiteTimerAdapter(db)
    outbox = SQLiteOutboxAdapter(db)
    lock = SQLiteLeaseLockAdapter(db)
    sessions = SQLiteSessionStoreAdapter(db)
    workspaces = SQLiteWorkspaceStore(db)
    partitions = WorkspacePartitionRegistry(memory_root)
    permissions = AutoModePermissionAdapter()
    resolved_channel: ChannelPort = (
        channel if channel is not None else ConsoleChannelAdapter(echo=outbox_dir is None)
    )

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

    resolved_wt_mgr = worktree_manager or OrcaWorktreeManager(
        workspace_store=workspaces,
        session_store=sessions,
        usage=SQLiteWorkUsage(db).blockers,
    )

    def _make_worker(inbox_id: str, worker_id: str) -> AgentWorker:
        if executors_factory is not None:
            return AgentWorker(
                worker_id=worker_id,
                inbox=inbox,
                timers=timers,
                memory=partitions.memory_for(inbox_id),
                permissions=permissions,
                outbox=outbox,
                executors=executors_factory(inbox_id),
                project_default_executor=project_default_executor,
                global_default_executor=global_default_executor,
                on_worktree_reclaim=lambda wk, _iid=inbox_id: resolved_wt_mgr.reclaim(
                    wk, notify_outbox=outbox, inbox_id=_iid
                ),
                lock=lock,
                sessions=sessions,
                lease_ttl_seconds=lease_ttl_seconds,
                new_event_policy=new_event_policy,
            )
        executor = executor_factory(inbox_id) if executor_factory is not None else default_executor
        return AgentWorker(
            worker_id=worker_id,
            inbox=inbox,
            timers=timers,
            memory=partitions.memory_for(inbox_id),
            permissions=permissions,
            outbox=outbox,
            executor=executor,
            on_worktree_reclaim=lambda wk, _iid=inbox_id: resolved_wt_mgr.reclaim(
                wk, notify_outbox=outbox, inbox_id=_iid
            ),
            lock=lock,
            sessions=sessions,
            lease_ttl_seconds=lease_ttl_seconds,
            new_event_policy=new_event_policy,
        )

    gdrive_writer: DriveResultWriter | None = None
    if gdrive_client is not None:
        gdrive_writer = DriveResultWriter(
            client=gdrive_client,
            sessions=sessions,
            inner_channel=resolved_channel,
            root_folder_id=gdrive_root_folder_id or None,
            pending_folder_id=gdrive_pending_folder_id or None,
            in_progress_folder_id=gdrive_in_progress_folder_id or None,
            done_folder_id=gdrive_done_folder_id or None,
            failed_folder_id=gdrive_failed_folder_id or None,
        )
        resolved_channel = gdrive_writer
    if outbox_dir is not None:
        # Outermost: the file is written first, then every existing channel gets the message unchanged.
        resolved_channel = FileOutboxChannel(
            outbox_dir, resolved_channel, events_url=approval_events_url, display_base=outbox_display_base
        )

    def _on_reports(reports: list[RunReport]) -> None:
        if gdrive_writer is None:
            return
        for rep in reports:
            gdrive_writer.handle_run_report(rep)

    def _on_failed(inbox_id: str) -> None:
        if gdrive_writer is None:
            return
        _report_failed_events(inbox, gdrive_writer, inbox_id)

    pool = AsyncWorkerPool(
        worker_factory=_make_worker,
        max_concurrency=max_concurrency,
        on_run_reports=_on_reports if gdrive_writer is not None else None,
        on_inbox_failed=_on_failed if gdrive_writer is not None else None,
        # Events a stopped process accepted while draining are picked up here.
        pending_inboxes=inbox.pending_inbox_ids,
    )
    # An ingest-only pool never runs but still collects wakes, so a config reload can hand them to a running pool.
    handler = EventHandler(inbox=inbox, on_wake=pool.wake)
    scheduler = TimerScheduler(timers=timers, inbox=inbox, on_wake=pool.wake)
    dispatcher = OutboxDispatcher(
        outbox=outbox,
        channel=resolved_channel,
        min_interval_seconds=min_outbox_interval_seconds,
    )
    feishu_webhook = FeishuWebhookAdapter(
        handler,
        verification_token=feishu_verification_token,
        chat_inbox_map=feishu_chat_inbox_map,
    )
    mcp_server = MCPServerAdapter(
        handler=handler,
        sessions=sessions,
        workspaces=workspaces,
        inbox=inbox,
        worktree_manager=resolved_wt_mgr,
        outbox=outbox,
        bearer_token=mcp_bearer_token,
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
        feishu_webhook=feishu_webhook,
        gdrive_poller=(
            DriveTaskPoller(
                handler,
                client=gdrive_client,
                default_inbox_id=gdrive_default_inbox_id,
                root_folder_id=gdrive_root_folder_id or None,
                pending_folder_id=gdrive_pending_folder_id or None,
                in_progress_folder_id=gdrive_in_progress_folder_id or None,
                done_folder_id=gdrive_done_folder_id or None,
                failed_folder_id=gdrive_failed_folder_id or None,
            )
            if gdrive_client is not None
            else None
        ),
        gdrive_result_writer=gdrive_writer,
        mcp_server=mcp_server,
        gdrive_poll_interval_seconds=gdrive_poll_interval_seconds,
        ingest_only=ingest_only,
    )


def build_daemon_from_config(
    config: HibernautConfig,
    *,
    channel: ChannelPort | None = None,
    database: SQLiteDatabase | None = None,
    demo: bool = False,
    gdrive_client: DriveClientProtocol | None = None,
    switchboard: SwitchboardHandle | None = None,
) -> AlwaysOnDaemon:
    # Every inbox -> project lookup goes through the router, so pattern-matched inboxes get their project's settings
    # and an inbox matching two projects raises AmbiguousProjectMatch instead of picking one.
    router = ProjectRouter(config.projects.values())
    chat_inbox_map: dict[str, str] = {}
    inbox_checkout_map = router.inbox_map(lambda p: p.checkout)
    inbox_chat_map = router.inbox_map(lambda p: p.feishu_chat_id or None)

    for proj in config.projects.values():
        if proj.feishu_chat_id:
            chat_inbox_map[proj.feishu_chat_id] = proj.inbox_id

    resolved_channel = channel or build_feishu_or_console_channel(
        app_id_env=config.channels.feishu_app_id_env,
        app_secret_env=config.channels.feishu_app_secret_env,
        default_chat_id=config.channels.feishu_default_chat_id or None,
        inbox_chat_map=inbox_chat_map,
        console_echo=not config.channels.outbox_dir,
    )
    if switchboard is not None:
        # Replies with an origin go out through the Switchboard gateway; session-less notices keep the old channel.
        resolved_channel = SwitchboardChannelAdapter(switchboard_bus(switchboard).publish, fallback=resolved_channel)

    feishu_token = os.environ.get(config.channels.feishu_verification_token_env) or None
    mcp_token = (
        (os.environ.get(config.channels.mcp_token_env) or None)
        if config.channels.mcp_enabled
        else None
    )

    has_configured_executor = (
        config.runtime.default_executor in config.executors
        or config.runtime.default_executor in config.roles
    )
    ingest_only = (not demo) and (not has_configured_executor)
    if ingest_only:
        print(
            f"[hibernaut] Executor '{config.runtime.default_executor}' is not configured in "
            "[executors]; starting in ingest-only mode (events remain pending). "
            "Pass --demo to enable RuleBasedDemoLLM.",
            file=sys.stderr,
            flush=True,
        )

    resolved_gdrive_client: DriveClientProtocol | None = gdrive_client
    if resolved_gdrive_client is None and config.gdrive.enabled:
        try:
            proxy_token = read_private_token(Path(config.gdrive.proxy_token_file))
        except ValueError as exc:
            print(f"[hibernaut] [gdrive] {exc}; skipping Google Drive task poller.", file=sys.stderr, flush=True)
        else:
            resolved_gdrive_client = ProxiedDriveClient(
                CredentialProxyClient(config.gdrive.proxy_url, timeout_seconds=60.0, token=proxy_token),
                service_name=config.gdrive.proxy_service,
            )

    daemon = build_daemon(
        db_path=config.runtime.db_path,
        database=database,
        memory_root=config.runtime.memory_root,
        max_concurrency=config.runtime.max_concurrency,
        channel=resolved_channel,
        new_event_policy=config.runtime.new_event_policy,
        feishu_verification_token=feishu_token,
        feishu_chat_inbox_map=chat_inbox_map,
        mcp_bearer_token=mcp_token,
        ingest_only=ingest_only,
        gdrive_client=resolved_gdrive_client,
        gdrive_root_folder_id=config.gdrive.root_folder_id or None,
        gdrive_pending_folder_id=config.gdrive.pending_folder_id or None,
        gdrive_in_progress_folder_id=config.gdrive.in_progress_folder_id or None,
        gdrive_done_folder_id=config.gdrive.done_folder_id or None,
        gdrive_failed_folder_id=config.gdrive.failed_folder_id or None,
        gdrive_poll_interval_seconds=config.gdrive.poll_interval_seconds,
        gdrive_default_inbox_id=config.gdrive.default_inbox_id,
        outbox_dir=config.channels.outbox_dir or None,
        outbox_display_base=config.config_dir or None,
        approval_events_url=local_events_url(config.channels.host, config.channels.port),
    )

    if has_configured_executor and not demo:
        # Instantiate all configured [executors.<id>] per project so AgentWorker can sticky-route.
        # Role executors are rebuilt with each config reload, so the next run uses the new profile and launcher.
        terminal_bindings = SQLiteTerminalBindingStore(daemon.db)
        terminal_runner = OrcaVisibleRunner(bindings=terminal_bindings)
        cli_terminal = OrcaCommandTerminal()
        # An independently dispatched review finds its developer only through bindings this runtime recorded.
        developer_peers = DeveloperPeerResolver(
            sessions=daemon.sessions,
            bindings=terminal_bindings,
            developer_executors=[name for name, role in config.roles.items() if role.sde is not None],
            registry=registry_dirs(default_transcript_roots()),
        )
        tasks = SQLiteTaskMetadataStore(daemon.db)

        deliveries = SQLiteDeliveryStore(daemon.db)
        usage = SQLiteWorkUsage(daemon.db)

        def _role_executors(
            wt_mgr: OrcaWorktreeManager, no_terminal: str | None, delivery: DeliveryConfig | None = None,
        ) -> dict[str, ExecutorPort]:
            # no_terminal says why this inbox has no terminal backend; None means Orca opens the sessions.
            # Each SDE run reviews through the reviewer built for the same project and config generation.
            def terminal(role: RoleConfig) -> OrcaVisibleRunner | TerminalUnavailable | None:
                if not role.interactive:
                    return None
                return terminal_runner if no_terminal is None else TerminalUnavailable(no_terminal)

            reviewers: dict[str, CoderReviewerExecutor] = {
                name: CoderReviewerExecutor(profile=role.profile, workspaces=daemon.workspaces,
                                            terminal=terminal(role), peers=developer_peers, tasks=tasks,
                                            workspace_key=wt_mgr.workspace_key)
                for name, role in config.roles.items()
                if role.role == CODE_REVIEWER_ROLE
            }
            templates = {**DEFAULT_CLI_TEMPLATES, **config.executors}

            def reviewer_for(name: str) -> Reviewer | None:
                # Config parsing guarantees the name is a code-reviewer executor or a CLI template.
                if name in reviewers:
                    return reviewers[name]
                template = templates.get(name)
                if template is None:
                    return None
                # A CLI review that asks for a terminal runs as a subprocess where the project has none.
                return CLIReviewer(template=template,
                                   terminal=cli_terminal if no_terminal is None else TerminalUnavailable(no_terminal))

            built: dict[str, ExecutorPort] = dict(reviewers)
            for name, role in config.roles.items():
                if role.sde is None:
                    continue
                built[name] = SDEExecutor(
                    profile=role.profile,
                    options=role.sde,
                    reviewer=reviewer_for(role.sde.reviewer) if role.sde.review else None,
                    # An independently dispatched review always comes from a code-reviewer executor.
                    peer_reviewer=role.sde.reviewer if role.sde.reviewer in reviewers else next(iter(reviewers), None),
                    worktree_manager=wt_mgr,
                    inbox_checkout_map=inbox_checkout_map,
                    terminal=terminal(role),
                    tasks=tasks,
                    delivery=PRDelivery(delivery, deliveries) if delivery is not None else None,
                    live_peers=usage.peers,
                )
            return built

        executors_by_project: dict[str, dict[str, ExecutorPort]] = {}
        wt_mgr_by_project: dict[str, OrcaWorktreeManager] = {}
        for proj in config.projects.values():
            wt_mgr = OrcaWorktreeManager(
                workspace_store=daemon.workspaces,
                session_store=daemon.sessions,
                backend=proj.worktree_backend,
                create_terminal=proj.orca_create_terminal,
                usage=usage.blockers,
                primary_inbox_id=proj.inbox_id,
                isolate_inboxes=proj.serves_multiple_inboxes,
            )
            wt_mgr_by_project[proj.name] = wt_mgr
            executors_by_project[proj.name] = {
                exec_name: LocalCLIExecutor(
                    worktree_manager=wt_mgr,
                    inbox_checkout_map=inbox_checkout_map,
                    templates=config.executors,
                    default_executor=exec_name,
                    orca_workspace=proj.worktree_backend == "orca",
                    terminal_bindings=terminal_bindings,
                    visible_runner=terminal_runner,
                )
                for exec_name in config.executors
            }
            executors_by_project[proj.name].update(_role_executors(
                wt_mgr,
                None if proj.terminal_backend == "orca" else f"project '{proj.name}' has terminal_backend = \"none\"",
                proj.delivery,
            ))

        default_mgr = OrcaWorktreeManager(
            workspace_store=daemon.workspaces,
            session_store=daemon.sessions,
            usage=usage.blockers,
        )
        fallback_executors: dict[str, ExecutorPort] = {
            exec_name: LocalCLIExecutor(
                worktree_manager=default_mgr,
                inbox_checkout_map=inbox_checkout_map,
                templates=config.executors,
                default_executor=exec_name,
            )
            for exec_name in config.executors
        }
        fallback_executors.update(_role_executors(default_mgr, "the inbox matches no [projects] entry"))

        def _manager_for(inbox_id: str | None, work_key: str) -> OrcaWorktreeManager:
            if inbox_id is not None:
                project = router.resolve(inbox_id)
            else:
                record = daemon.workspaces.get(work_key)
                checkout = Path(record.repo_checkout).resolve() if record is not None else None
                project = next((p for p in config.projects.values() if Path(p.checkout).resolve() == checkout), None)
            return wt_mgr_by_project[project.name] if project is not None else default_mgr

        # One per config generation, so a held event is logged once each time it enters the wait.
        unmatched_hold = UnmatchedProjectHold(daemon.sessions)

        def _cli_worker_factory(inbox_id: str, worker_id: str) -> AgentWorker:
            # Raises AmbiguousProjectMatch before any lease, executor or workspace exists for the inbox.
            project = router.resolve(inbox_id)
            wt_mgr = wt_mgr_by_project[project.name] if project is not None else default_mgr
            reclaim = worktree_reclaimer(wt_mgr, outbox=daemon.outbox, inbox_id=inbox_id,
                                         delivery=project.delivery if project is not None else None,
                                         deliveries=deliveries)
            return AgentWorker(
                worker_id=worker_id,
                inbox=daemon.inbox,
                timers=daemon.timers,
                memory=daemon.partitions.memory_for(inbox_id),
                permissions=AutoModePermissionAdapter(),
                outbox=daemon.outbox,
                executors=executors_by_project[project.name] if project is not None else fallback_executors,
                project_default_executor=project.executor if project is not None else None,
                global_default_executor=config.runtime.default_executor,
                on_worktree_reclaim=reclaim,
                hold=unmatched_hold if project is None else None,
                lock=daemon.lock,
                sessions=daemon.sessions,
                new_event_policy=config.runtime.new_event_policy,
            )

        def _cli_on_reports(reports: list[RunReport]) -> None:
            if daemon.gdrive_result_writer is None:
                return
            for rep in reports:
                daemon.gdrive_result_writer.handle_run_report(rep)

        def _cli_on_failed(inbox_id: str) -> None:
            if daemon.gdrive_result_writer is None:
                return
            _report_failed_events(daemon.inbox, daemon.gdrive_result_writer, inbox_id)

        daemon.pool = AsyncWorkerPool(
            worker_factory=_cli_worker_factory,
            max_concurrency=config.runtime.max_concurrency,
            on_run_reports=_cli_on_reports if daemon.gdrive_result_writer is not None else None,
            on_inbox_failed=_cli_on_failed if daemon.gdrive_result_writer is not None else None,
            pending_inboxes=daemon.inbox.pending_inbox_ids,
        )
        daemon.handler = EventHandler(inbox=daemon.inbox, on_wake=daemon.pool.wake)
        daemon.scheduler = TimerScheduler(
            timers=daemon.timers,
            inbox=daemon.inbox,
            on_wake=daemon.pool.wake,
        )
        daemon.feishu_webhook = FeishuWebhookAdapter(
            daemon.handler,
            verification_token=feishu_token,
            chat_inbox_map=chat_inbox_map,
        )
        daemon.mcp_server = MCPServerAdapter(
            handler=daemon.handler,
            sessions=daemon.sessions,
            workspaces=daemon.workspaces,
            inbox=daemon.inbox,
            worktree_manager=default_mgr,
            outbox=daemon.outbox,
            bearer_token=mcp_token,
            worktree_manager_for=_manager_for,
        )

    if resolved_gdrive_client is not None:
        daemon.gdrive_poller = DriveTaskPoller(
            daemon.handler,
            client=resolved_gdrive_client,
            default_inbox_id=config.gdrive.default_inbox_id,
            root_folder_id=config.gdrive.root_folder_id or None,
            pending_folder_id=config.gdrive.pending_folder_id or None,
            in_progress_folder_id=config.gdrive.in_progress_folder_id or None,
            done_folder_id=config.gdrive.done_folder_id or None,
            failed_folder_id=config.gdrive.failed_folder_id or None,
        )

    if switchboard is not None:
        attach_switchboard(daemon, switchboard, config.switchboard.consumer_id)

    return daemon


def take_switchboard_secrets(environ: MutableMapping[str, str] | None = None) -> dict[str, str]:
    # Executor subprocesses inherit os.environ; moving the Switchboard tokens out keeps them away from agents.
    env = os.environ if environ is None else environ
    return {name: env.pop(name) for name in [n for n in env if n.startswith(SWITCHBOARD_SECRET_PREFIX)]}


def switchboard_bus(handle: SwitchboardHandle) -> SwitchboardBus:
    return handle.bus if isinstance(handle, Switchboard) else handle


def start_switchboard(
    link: SwitchboardLinkConfig,
    *,
    environ: MutableMapping[str, str] | None = None,
) -> SwitchboardHandle | None:
    if not link.enabled:
        return None
    credentials = EnvCredentialBroker(take_switchboard_secrets(environ))
    if link.mode == "relay":
        return open_switchboard_relay(link, credentials)
    assert link.config_path is not None
    board = build_switchboard(load_switchboard_config(link.config_path), credentials=credentials)
    board.start()
    print(f"[hibernaut] [switchboard] in-process switchboard listening on {board.http.base_url}", file=sys.stderr, flush=True)
    return board


def open_switchboard_relay(link: SwitchboardLinkConfig, credentials: EnvCredentialBroker) -> SwitchboardRelayLink:
    assert link.relay_url is not None and link.mcp_url is not None
    missing = [
        name
        for name, channel in ((SWITCHBOARD_RELAY_TOKEN_ENV, "relay"), (SWITCHBOARD_MCP_TOKEN_ENV, "mcp"))
        if not credentials.issue_ephemeral(channel)
    ]
    if missing:
        raise ValueError(f'switchboard.mode = "relay" needs {" and ".join(missing)} in the environment')
    for url in (link.relay_url, link.mcp_url):
        parsed = urlparse(url)
        if parsed.scheme in ("ws", "http") and parsed.hostname not in _LOOPBACK_HOSTS:
            print(
                f"[hibernaut] [switchboard] WARNING: {url} is not TLS; the relay and MCP tokens travel in clear text.",
                file=sys.stderr,
                flush=True,
            )
    context = ssl.create_default_context(cafile=link.ca_file) if link.ca_file else None
    # The agent connects when build_daemon_from_config subscribes, so no push arrives before ingest is wired.
    relay = SwitchboardRelayLink(
        relay_url=link.relay_url,
        mcp_url=link.mcp_url,
        consumer_id=link.consumer_id,
        credentials=credentials,
        ssl_context=context,
    )
    print(
        f"[hibernaut] [switchboard] relay mode: receiving from {link.relay_url}, reporting to {link.mcp_url}",
        file=sys.stderr,
        flush=True,
    )
    return relay


def attach_switchboard(daemon: AlwaysOnDaemon, board: SwitchboardHandle, consumer_id: str) -> SwitchboardInboundBridge:
    # Subscribing again replaces the previous handler, so a config reload re-points delivery at the new daemon.
    bridge = SwitchboardInboundBridge(daemon.handler)
    switchboard_bus(board).subscribe(consumer_id, bridge.handle_canonical_event)
    daemon.switchboard_bridge = bridge
    return bridge


async def serve_configured(
    path: Path,
    config: HibernautConfig,
    daemon: AlwaysOnDaemon,
    stop: asyncio.Event,
    *,
    force_stop: asyncio.Event | None = None,
    demo: bool = False,
    switchboard: SwitchboardHandle | None = None,
    reload_interval: float = 1.0,
    outbox_interval_seconds: float = 1.0,
) -> None:
    # A reload never waits for Runs in progress: the listener stays up and takes the new handler, services restart
    # within one tick, and the old worker pool is retired (no new claims) while its last Runs finish in the
    # background. worker_leases keeps the old and new pools off the same inbox. A stop follows the same principle:
    # see drain_pools().
    server = WebhookHTTPServer((config.channels.host, config.channels.port), daemon.http_handler())
    changes = config_changes(path, stop, interval=reload_interval)

    async with asyncio.TaskGroup() as tasks:
        generation = _Generation.start(daemon, tasks, outbox_interval_seconds=outbox_interval_seconds)
        # Every generation whose pool may still hold Runs: the current one and those retired by reloads.
        generations = [generation]
        server_thread = _serve_in_thread(server)

        def wake_current(inbox_id: str) -> None:
            generation.daemon.pool.wake(inbox_id)

        try:
            async for candidate in changes:
                if candidate == config:
                    continue
                if (candidate.runtime.db_path, candidate.runtime.memory_root) != (
                    config.runtime.db_path,
                    config.runtime.memory_root,
                ):
                    print(
                        "[config] Storage path changes require a restart.",
                        file=sys.stderr,
                        flush=True,
                    )
                    continue
                if candidate.switchboard != config.switchboard:
                    print(
                        "[config] [switchboard] changes require a restart.",
                        file=sys.stderr,
                        flush=True,
                    )
                    continue
                address = (candidate.channels.host, candidate.channels.port)
                next_server: WebhookHTTPServer | None = None
                try:
                    replacement = build_daemon_from_config(
                        candidate,
                        database=daemon.db,
                        demo=demo,
                        switchboard=switchboard,
                    )
                    handler = replacement.http_handler()
                    if address != (config.channels.host, config.channels.port):
                        next_server = WebhookHTTPServer(address, handler)
                except (OSError, ValueError, OverflowError) as exc:
                    print(
                        f"[config] Cannot activate settings ({type(exc).__name__}); "
                        "keeping current settings.",
                        file=sys.stderr,
                        flush=True,
                    )
                    continue

                previous = generation
                # Stop the old services first so two dispatchers never send the same Outbox row.
                await previous.stop_services()
                generation = _Generation.start(replacement, tasks, outbox_interval_seconds=outbox_interval_seconds)
                generations = [g for g in generations if g.runs is not None and not g.runs.done()] + [generation]
                # From here every wake reaching the old pool, from its in-flight requests or released leases,
                # is forwarded to the new one.
                previous.retire_pool(wake_current)
                if next_server is None:
                    server.RequestHandlerClass = handler
                else:
                    next_thread = _serve_in_thread(next_server)
                    # Accepted requests finish against the old adapters before the old listener closes.
                    await _close_server(server, server_thread)
                    server, server_thread = next_server, next_thread
                daemon, config = replacement, candidate
                print(f"[config] Reloaded {path}", file=sys.stderr, flush=True)
        finally:
            await changes.aclose()
            # A stop drains first: ingress and services keep running until every pool, retired or current, has
            # finished its Runs; only then do the listener (after its accepted requests) and the services close.
            await drain_pools(
                [g.daemon.pool for g in generations],
                [g.runs for g in generations if g.runs is not None],
                force_stop,
            )
            for g in generations:
                g.pool_stop.set()
            await _close_server(server, server_thread)
            await generation.stop_services()


@dataclass
class _Generation:
    daemon: AlwaysOnDaemon
    services: asyncio.Task[None]
    services_stop: asyncio.Event
    pool_stop: asyncio.Event
    runs: asyncio.Task[None] | None

    @classmethod
    def start(
        cls,
        daemon: AlwaysOnDaemon,
        tasks: asyncio.TaskGroup,
        *,
        outbox_interval_seconds: float,
    ) -> _Generation:
        services_stop = asyncio.Event()
        pool_stop = asyncio.Event()
        daemon.pool.bind(asyncio.get_running_loop())
        runs = None if daemon.ingest_only else tasks.create_task(daemon.pool.run_forever(pool_stop))
        services = tasks.create_task(
            daemon.run_services(services_stop, outbox_interval_seconds=outbox_interval_seconds)
        )
        return cls(daemon, services, services_stop, pool_stop, runs)

    async def stop_services(self) -> None:
        self.services_stop.set()
        await self.services

    def retire_pool(self, handoff: Callable[[str], None]) -> None:
        # Ingest-only pools are retired too: requests begun on their handler may append (and wake) after the swap.
        self.daemon.pool.retire(handoff)


def _serve_in_thread(server: WebhookHTTPServer) -> threading.Thread:
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
    thread.start()
    return thread


async def _close_server(server: WebhookHTTPServer, thread: threading.Thread) -> None:
    await asyncio.to_thread(server.shutdown)
    server.server_close()
    thread.join()


def main() -> None:
    parser = argparse.ArgumentParser(description="Hibernaut multi-inbox Always-on Agent daemon")
    parser.add_argument("--config", default=None, help="Path to hibernaut.toml")
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Explicitly allow RuleBasedDemoLLM when starting with --config",
    )
    parser.add_argument(
        "--mcp-stdio",
        action="store_true",
        help="Run Hibernaut MCP Server over stdio (JSON-RPC 2.0 lines)",
    )
    parser.add_argument("--db", default="./hibernaut.db")
    parser.add_argument("--memory-root", default="./agent_memory")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--policy", choices=("defer", "fold", "interrupt"), default="defer")
    parser.add_argument("--feishu-token", default=None)
    parser.add_argument("--mcp-token", default=None)
    parser.add_argument("--config-poll-interval", type=float, default=1.0)
    parser.add_argument("--outbox-interval", type=float, default=1.0)
    banner = f"Hibernaut {__version__}"
    parser.add_argument("--version", action="version", version=banner)
    args = parser.parse_args()

    board: SwitchboardHandle | None = None
    if args.config:
        cfg = load_config(args.config)
        # The stdio MCP companion runs beside a daemon that already owns the Switchboard port or relay
        # connection; a second relay agent with the same consumer_id would supersede the daemon's.
        board = None if args.mcp_stdio else start_switchboard(cfg.switchboard)
        daemon = build_daemon_from_config(cfg, demo=args.demo, switchboard=board)
        host = cfg.channels.host
        port = cfg.channels.port
    else:
        daemon = build_daemon(
            db_path=args.db,
            memory_root=args.memory_root,
            max_concurrency=args.workers,
            new_event_policy=args.policy,
            feishu_verification_token=args.feishu_token,
            mcp_bearer_token=args.mcp_token,
        )
        host = args.host
        port = args.port

    if args.mcp_stdio:
        # Keep stdout clean for JSON-RPC 2.0 frames in stdio mode.
        assert daemon.mcp_server is not None
        try:
            daemon.mcp_server.serve_stdio(sys.stdin, sys.stdout)
        finally:
            daemon.close()
            if board is not None:
                board.stop()
        return

    print(banner, flush=True)

    async def _serve() -> None:
        stop_event = asyncio.Event()
        force_stop = asyncio.Event()

        def _on_stop_signal() -> None:
            # The first signal drains; a second one exits without waiting for Runs in progress.
            if stop_event.is_set():
                force_stop.set()
            stop_event.set()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _on_stop_signal)
            except NotImplementedError:
                pass
        try:
            if args.config:
                await serve_configured(
                    Path(args.config),
                    cfg,
                    daemon,
                    stop_event,
                    force_stop=force_stop,
                    demo=args.demo,
                    switchboard=board,
                    reload_interval=args.config_poll_interval,
                    outbox_interval_seconds=args.outbox_interval,
                )
            else:
                await daemon.run(
                    stop_event=stop_event,
                    force_stop_event=force_stop,
                    webhook_host=host,
                    webhook_port=port,
                    outbox_interval_seconds=args.outbox_interval,
                )
        finally:
            daemon.close()
            if board is not None:
                board.stop()

    asyncio.run(_serve())


if __name__ == "__main__":
    main()
