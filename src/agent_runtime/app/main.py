from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

from agent_runtime import __version__
from agent_runtime.adapters.channels import (
    ConsoleChannelAdapter,
    DriveClientProtocol,
    DriveResultWriter,
    DriveTaskPoller,
    FeishuWebhookAdapter,
    GitHubIssuePoller,
    GitHubWebhookAdapter,
    MCPServerAdapter,
    PollTarget,
    ProxiedDriveClient,
    WebhookHTTPServer,
    build_feishu_or_console_channel,
)
from agent_runtime.adapters.channels.channels import webhook_request_handler
from agent_runtime.adapters.executor_cli import (
    CODER_REVIEWER_ROLE,
    CoderReviewerExecutor,
    LocalCLIExecutor,
    OrcaWorktreeManager,
    SDEExecutor,
)
from agent_runtime.adapters.executor_cli.orca import OrcaVisibleRunner, TerminalUnavailable, default_transcript_roots
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
    SQLiteInboxAdapter,
    SQLiteLeaseLockAdapter,
    SQLiteOutboxAdapter,
    SQLiteSessionStoreAdapter,
    SQLiteTimerAdapter,
    SQLiteTaskMetadataStore,
    SQLiteWorkspaceStore,
)
from agent_runtime.app.config import HibernautConfig, RoleConfig, config_changes, load_config
from agent_runtime.core.models import NewEventPolicy
from agent_runtime.core.ports import ChannelPort, ExecutorPort, RunCapabilities
from agent_runtime.engine import (
    AgentWorker,
    EventHandler,
    OutboxDispatcher,
    RunReport,
    TimerScheduler,
)
from agent_runtime.engine.worker import ExecutorFailed


class AsyncWorkerPool:
    def __init__(
        self,
        *,
        worker_factory: Callable[[str, str], AgentWorker],
        max_concurrency: int = 4,
        on_run_reports: Callable[[list[RunReport]], None] | None = None,
        on_inbox_failed: Callable[[str], None] | None = None,
    ) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        self._worker_factory = worker_factory
        self._max_concurrency = max_concurrency
        self._on_run_reports = on_run_reports
        self._on_inbox_failed = on_inbox_failed
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
    gdrive_poller: DriveTaskPoller | None = None
    gdrive_result_writer: DriveResultWriter | None = None
    mcp_server: MCPServerAdapter | None = None
    poll_interval_seconds: float = 60.0
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
            github_adapter=self.github_webhook,
            feishu_adapter=self.feishu_webhook,
            mcp_adapter=self.mcp_server,
        )

    async def run(
        self,
        *,
        server: HTTPServer | None = None,
        stop_event: asyncio.Event | None = None,
        timer_interval_seconds: float = 5.0,
        outbox_interval_seconds: float = 1.0,
        webhook_host: str | None = None,
        webhook_port: int | None = None,
    ) -> None:
        stopper = stop_event or self._stop_event
        owns_server = server is None
        server_thread = None
        if server is None and webhook_host is not None and webhook_port is not None:
            server = WebhookHTTPServer((webhook_host, webhook_port), self.http_handler())
        if server is not None:
            server_thread = threading.Thread(target=server.serve_forever, daemon=True)
            server_thread.start()

        workers_stop = asyncio.Event()
        async with asyncio.TaskGroup() as tasks:
            if not self.ingest_only:
                tasks.create_task(self.pool.run_forever(workers_stop))
            tasks.create_task(self.scheduler.run_forever(timer_interval_seconds, stopper))
            tasks.create_task(self.dispatcher.run_forever(outbox_interval_seconds, stopper))
            poll_task = None
            if self.github_poller is not None:
                poll_task = tasks.create_task(
                    self.github_poller.run_forever(
                        interval_seconds=self.poll_interval_seconds,
                        stop_event=stopper,
                    )
                )
            gdrive_task = None
            if self.gdrive_poller is not None:
                gdrive_task = tasks.create_task(
                    self.gdrive_poller.run_forever(
                        interval_seconds=self.gdrive_poll_interval_seconds,
                        stop_event=stopper,
                    )
                )
            try:
                await stopper.wait()
                if poll_task is not None:
                    await poll_task
                if gdrive_task is not None:
                    await gdrive_task
            finally:
                # Keep workers available until the last accepted webhook has queued its wake.
                if server is not None:
                    await asyncio.to_thread(server.shutdown)
                    if owns_server:
                        server.server_close()
                if server_thread is not None:
                    server_thread.join()
                workers_stop.set()

    def close(self) -> None:
        self.db.close()


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
    github_webhook_secret: str | None = None,
    github_repo_inbox_map: Mapping[str, str] | None = None,
    feishu_verification_token: str | None = None,
    feishu_chat_inbox_map: Mapping[str, str] | None = None,
    poll_targets: list[PollTarget] | None = None,
    poll_interval_seconds: float = 60.0,
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

    resolved_wt_mgr = worktree_manager or OrcaWorktreeManager(
        workspace_store=workspaces,
        session_store=sessions,
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
    )
    wake_cb = None if ingest_only else pool.wake
    handler = EventHandler(inbox=inbox, on_wake=wake_cb)
    scheduler = TimerScheduler(timers=timers, inbox=inbox, on_wake=wake_cb)
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
        github_webhook=github_webhook,
        feishu_webhook=feishu_webhook,
        github_poller=github_poller,
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
        poll_interval_seconds=poll_interval_seconds,
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
) -> AlwaysOnDaemon:
    repo_inbox_map: dict[str, str] = {}
    inbox_checkout_map: dict[str, str] = {}
    inbox_chat_map: dict[str, str] = {}
    chat_inbox_map: dict[str, str] = {}
    inbox_project_executor: dict[str, str | None] = {}
    poll_targets: list[PollTarget] = []

    for proj in config.projects.values():
        repo_inbox_map[proj.repo] = proj.inbox_id
        inbox_checkout_map[proj.inbox_id] = proj.checkout
        inbox_project_executor[proj.inbox_id] = proj.executor
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
        github_webhook_secret=gh_secret,
        github_repo_inbox_map=repo_inbox_map,
        feishu_verification_token=feishu_token,
        feishu_chat_inbox_map=chat_inbox_map,
        poll_targets=poll_targets,
        poll_interval_seconds=config.runtime.poll_interval_seconds,
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
    )

    if has_configured_executor and not demo:
        # Instantiate all configured [executors.<id>] per project so AgentWorker can sticky-route.
        # Role executors are rebuilt with each config reload, so the next run uses the new profile and launcher.
        terminal_bindings = SQLiteTerminalBindingStore(daemon.db)
        terminal_runner = OrcaVisibleRunner(bindings=terminal_bindings)
        # An independently dispatched review finds its developer only through bindings this runtime recorded.
        developer_peers = DeveloperPeerResolver(
            sessions=daemon.sessions,
            bindings=terminal_bindings,
            developer_executors=[name for name, role in config.roles.items() if role.sde is not None],
            registry=registry_dirs(default_transcript_roots()),
        )
        tasks = SQLiteTaskMetadataStore(daemon.db)

        def _role_executors(wt_mgr: OrcaWorktreeManager, no_terminal: str | None) -> dict[str, ExecutorPort]:
            # no_terminal says why this inbox has no terminal backend; None means Orca opens the sessions.
            # Each SDE run reviews through the reviewer built for the same project and config generation.
            def terminal(role: RoleConfig) -> OrcaVisibleRunner | TerminalUnavailable | None:
                if not role.interactive:
                    return None
                return terminal_runner if no_terminal is None else TerminalUnavailable(no_terminal)

            reviewers: dict[str, CoderReviewerExecutor] = {
                name: CoderReviewerExecutor(profile=role.profile, workspaces=daemon.workspaces,
                                            terminal=terminal(role), peers=developer_peers, tasks=tasks)
                for name, role in config.roles.items()
                if role.role == CODER_REVIEWER_ROLE
            }
            built: dict[str, ExecutorPort] = dict(reviewers)
            for name, role in config.roles.items():
                if role.sde is None:
                    continue
                built[name] = SDEExecutor(
                    profile=role.profile,
                    options=role.sde,
                    reviewer=reviewers.get(role.sde.reviewer) if role.sde.review else None,
                    worktree_manager=wt_mgr,
                    inbox_checkout_map=inbox_checkout_map,
                    terminal=terminal(role),
                    tasks=tasks,
                )
            return built

        executors_by_inbox: dict[str, dict[str, ExecutorPort]] = {}
        wt_mgr_by_inbox: dict[str, OrcaWorktreeManager] = {}
        for proj in config.projects.values():
            wt_mgr = OrcaWorktreeManager(
                workspace_store=daemon.workspaces,
                session_store=daemon.sessions,
                backend=proj.worktree_backend,
                create_terminal=proj.orca_create_terminal,
            )
            wt_mgr_by_inbox[proj.inbox_id] = wt_mgr
            executors_by_inbox[proj.inbox_id] = {
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
            executors_by_inbox[proj.inbox_id].update(_role_executors(
                wt_mgr,
                None if proj.terminal_backend == "orca" else f"project '{proj.name}' has terminal_backend = \"none\"",
            ))

        default_mgr = OrcaWorktreeManager(
            workspace_store=daemon.workspaces,
            session_store=daemon.sessions,
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

        def _cli_worker_factory(inbox_id: str, worker_id: str) -> AgentWorker:
            return AgentWorker(
                worker_id=worker_id,
                inbox=daemon.inbox,
                timers=daemon.timers,
                memory=daemon.partitions.memory_for(inbox_id),
                permissions=AutoModePermissionAdapter(),
                outbox=daemon.outbox,
                executors=executors_by_inbox.get(inbox_id, fallback_executors),
                project_default_executor=inbox_project_executor.get(inbox_id),
                global_default_executor=config.runtime.default_executor,
                on_worktree_reclaim=lambda wk, _iid=inbox_id: wt_mgr_by_inbox.get(
                    _iid, default_mgr
                ).reclaim(wk, notify_outbox=daemon.outbox, inbox_id=_iid),
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
        daemon.mcp_server = MCPServerAdapter(
            handler=daemon.handler,
            sessions=daemon.sessions,
            workspaces=daemon.workspaces,
            inbox=daemon.inbox,
            worktree_manager=default_mgr,
            outbox=daemon.outbox,
            bearer_token=mcp_token,
        )

    if poll_targets:
        daemon.github_poller = GitHubIssuePoller(
            daemon.handler,
            targets=poll_targets,
            mode=config.runtime.poll_mode,
            github_token_env=config.channels.github_token_env,
            repo_inbox_map=repo_inbox_map,
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

    return daemon


async def serve_configured(
    path: Path,
    config: HibernautConfig,
    daemon: AlwaysOnDaemon,
    stop: asyncio.Event,
    *,
    demo: bool = False,
) -> None:
    server = WebhookHTTPServer((config.channels.host, config.channels.port), daemon.http_handler())
    changes = config_changes(path, stop)
    next_server = server
    try:
        while not stop.is_set():
            drain = asyncio.Event()
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(daemon.run(server=server, stop_event=drain))
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
                        address = (candidate.channels.host, candidate.channels.port)
                        try:
                            replacement = build_daemon_from_config(
                                candidate,
                                database=daemon.db,
                                demo=demo,
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
                        break
                    else:
                        return
                finally:
                    drain.set()
            # Accepted requests finish against the old adapters before the handover.
            if next_server is not server:
                server.server_close()
                server = next_server
            server.RequestHandlerClass = handler
            daemon, config = replacement, candidate
            print(f"[config] Reloaded {path}", file=sys.stderr, flush=True)
    finally:
        await changes.aclose()
        if next_server is not server:
            next_server.server_close()
        server.server_close()


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
    parser.add_argument("--github-secret", default=None)
    parser.add_argument("--feishu-token", default=None)
    parser.add_argument("--mcp-token", default=None)
    banner = f"Hibernaut {__version__}"
    parser.add_argument("--version", action="version", version=banner)
    args = parser.parse_args()

    if args.config:
        cfg = load_config(args.config)
        daemon = build_daemon_from_config(cfg, demo=args.demo)
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
        return

    print(banner, flush=True)

    async def _serve() -> None:
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except NotImplementedError:
                pass
        try:
            if args.config:
                await serve_configured(
                    Path(args.config),
                    cfg,
                    daemon,
                    stop_event,
                    demo=args.demo,
                )
            else:
                await daemon.run(stop_event=stop_event, webhook_host=host, webhook_port=port)
        finally:
            daemon.close()

    asyncio.run(_serve())


if __name__ == "__main__":
    main()
