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
from collections import deque
from collections.abc import AsyncGenerator, Callable, Iterable, Mapping, MutableMapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager, nullcontext
from dataclasses import dataclass, field, replace
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from agent_runtime import __version__
from agent_runtime.adapters.channels import (
    ConsoleChannelAdapter,
    DriveAttachmentStore,
    DriveClientProtocol,
    DriveResultWriter,
    DriveTaskPoller,
    FileOutboxChannel,
    ListenerHandover,
    MCPServerAdapter,
    ProxiedDriveClient,
    WebhookHTTPServer,
    local_events_url,
)
from agent_runtime.adapters.channels.channels import webhook_request_handler
from agent_runtime.adapters.channels.ingress import FeishuGroup, GroupEntrances, event_origin_for
from agent_runtime.adapters.channels.switchboard_bridge import SwitchboardChannelAdapter, SwitchboardInboundBridge
from agent_runtime.adapters.channels.switchboard_relay import SwitchboardBus, SwitchboardRelayLink
from agent_runtime.adapters.collaboration_bridge import (
    ChiefOfStaffInboxSink,
    CollaborationControlPlane,
    CollaborationRuntime,
    ProjectCloseoutCoordinator,
    TaskboardFollowUpSink,
)
from agent_runtime.adapters.delivery import DeliveryConfig, PRDelivery, merged_head
from agent_runtime.adapters.executor_cli import (
    CODE_REVIEWER_ROLE,
    DEFAULT_CLI_TEMPLATES,
    STEWARD_ROLE,
    CLIReviewer,
    CoderReviewerExecutor,
    LocalCLIExecutor,
    OrcaWorktreeManager,
    Reviewer,
    SDEExecutor,
    StewardExecutor,
)
from agent_runtime.adapters.executor_cli.memory_access import MemoryAccess, channel_path
from agent_runtime.adapters.executor_cli.orca import (
    OrcaCommandTerminal,
    OrcaVisibleRunner,
    TerminalUnavailable,
    default_transcript_roots,
)
from agent_runtime.adapters.executor_cli.attempt_notes import AttemptNotes
from agent_runtime.adapters.executor_cli.steward import GlobalScope
from agent_runtime.adapters.executor_cli.reviewer_pool import ReviewerPool, SwitchNotice
from agent_runtime.adapters.executor_cli.reclaim_sweep import (
    DEFAULT_SWEEP_INTERVAL_SECONDS,
    SweepTarget,
    WorktreeReclaimSweeper,
)
from agent_runtime.adapters.executor_cli.worktree import ReclaimResult
from agent_runtime.adapters.storage_sqlite.workspaces import WorkspaceRecord
from agent_runtime.adapters.executor_cli.peer import DeveloperPeerResolver, registry_dirs
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeError
from agent_runtime.adapters.executor_cli.peer_handoff import PeerHandoff, require_receiver
from agent_runtime.adapters.storage_sqlite.peer_mailbox import SQLitePeerMailbox
from agent_runtime.adapters.executor_whitebox import (
    LLMBackend,
    RuleBasedDemoLLM,
    WhiteboxExecutor,
)
from agent_runtime.adapters.memory_bridge import (
    DEFAULT_GC_INTERVAL_SECONDS,
    MemoryControlPlane,
    MemoryGarbageCollector,
    ScopedMemoryHTTPServer,
)
from agent_runtime.adapters.memory_fs import ExpertMount, GovernedMemoryHub, WorkspacePartitionRegistry, check_inbox_id
from agent_runtime.adapters.permissions import AutoModePermissionAdapter, CredentialProxyClient, read_private_token
from agent_runtime.adapters.runner_pilot import ClosedRouteExecutor, ProtocolCheckExecutor, RunnerPilotRelay
from agent_runtime.adapters.storage_sqlite.terminals import SQLiteTerminalBindingStore
from agent_runtime.adapters.storage_sqlite.open_runs import SQLiteOpenRunStore
from agent_runtime.adapters.storage_sqlite import (
    SQLiteAttemptStore,
    SQLiteDatabase,
    SQLiteDeliveryStore,
    SQLiteWorkUsage,
    SQLiteInboxAdapter,
    SQLiteLeaseLockAdapter,
    SQLiteMemoryGrantStore,
    SQLiteOutboxAdapter,
    SQLiteReviewerStore,
    SQLiteSessionStoreAdapter,
    SQLiteTimerAdapter,
    SQLiteTaskMetadataStore,
    SQLiteWorkspaceStore,
)
from agent_runtime.app.bootstrap import (
    RunnerHandle,
    open_collaboration,
    open_collaboration_hub,
    open_memory_http,
    open_memory_hub,
    open_runner,
)
from agent_runtime.app.concurrency import (
    ConcurrencyLimiter,
    ConcurrencyLimits,
    ExecutorQuotaHold,
    LimitKeyResolver,
    LimitKeys,
    LimitRoute,
)
from agent_runtime.app.config import (
    CollaborationHubConfig,
    CollaborationTaskboardConfig,
    HibernautConfig,
    MemoryHTTPConfig,
    ProjectConfig,
    RoleConfig,
    RunnerLinkConfig,
    RunnerPilotConfig,
    SwitchboardLinkConfig,
    config_changes,
    load_config,
)
from agent_runtime.app.projects import (
    AmbiguousProjectMatch,
    ProjectRouter,
    InboxRefused,
    MissingWorkdirHold,
    UnmatchedProjectHold,
    WorkdirBinding,
    build_global_manifest,
    build_routing_manifest,
    build_system_manifest,
    project_memory_key,
)
from agent_runtime.app.reload import ReloadStatus
from agent_runtime.app.system_tools import SystemToolbox
from agent_runtime.core.models import Event, InboxBinding, InboxRoute, NewEventPolicy
from agent_runtime.core.ports import ChannelPort, ExecutorPort, OutboxPort, RunCapabilities
from agent_runtime.engine import (
    AgentWorker,
    EventHandler,
    OutboxDispatcher,
    RunReport,
    TimerScheduler,
)
from agent_runtime.engine.worker import ExecutorFailed
from agent_runtime.adapters.executor_cli.role_skills import canonical_role
from agent_runtime.adapters.executor_cli.sde import SDE_ROLE
from agent_runtime.adapters.ledger_bridge import RunLedger
from agent_runtime.adapters.storage_sqlite.ops_status import RunSummary, SQLiteOpsStatus
from collaboration import CloseoutCoordinator, NotificationPolicyEngine
from memory_hub import MemoryHubConfig
from runner.protocol_check import PROTOCOL_CHECK_EXECUTOR_ID
from switchboard.ports import AuditSinkPort
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
        limits: ConcurrencyLimits | None = None,
        limit_keys: Callable[[str], LimitKeys] | None = None,
        limiter: ConcurrencyLimiter | None = None,
    ) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        self._worker_factory = worker_factory
        self._max_concurrency = max_concurrency
        self._limiter = limiter if limiter is not None else ConcurrencyLimiter(max_concurrency, limits)
        self._limit_keys = limit_keys
        self._on_run_reports = on_run_reports
        self._on_inbox_failed = on_inbox_failed
        self._pending_inboxes = pending_inboxes
        # Inboxes waiting to run, oldest first; one whose quota is full stays in place while later ones pass it.
        self._queue: deque[str] = deque()
        self._queued: set[str] = set()
        # Limit keys a queued inbox was last refused with, dropped by its next wake, so a rescan that refuses it again
        # costs no database reads.
        self._queued_keys: dict[str, LimitKeys] = {}
        # Set on every change that can make a queued inbox runnable or end a worker loop.
        self._changed = asyncio.Event()
        self._active: dict[str, AgentWorker] = {}
        self._requeue: set[str] = set()
        self._deferred: set[str] = set()
        self._idle_event = asyncio.Event()
        self._idle_event.set()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pool_id = f"pool-{next(_POOL_IDS)}"
        self._handoff: Callable[[str], None] | None = None
        self.reports: list[RunReport] = []

    @property
    def limiter(self) -> ConcurrencyLimiter:
        return self._limiter

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
        self._queued_keys.pop(inbox_id, None)
        self._changed.set()
        if inbox_id in self._queued:
            return
        self._queued.add(inbox_id)
        self._idle_event.clear()
        self._queue.append(inbox_id)

    def defer(self, inbox_id: str) -> None:
        # The inbox's worker left events behind for want of an executor slot: queue it again once this dispatch ends.
        if inbox_id in self._active:
            self._deferred.add(inbox_id)

    def _claim_ready(self) -> str | None:
        # The first queued inbox with quota in every dimension. Never waits, so a full quota holds up only its own
        # inboxes and occupies no worker.
        for inbox_id in () if self._limiter.saturated() else tuple(self._queue):
            cached = self._queued_keys.get(inbox_id)
            if cached is not None and not self._limiter.available(
                    inbox_id, project_key=cached.project, executor_key=cached.executor, agent_key=cached.agent):
                continue
            # Cached keys only rule an inbox out; it is admitted on fresh ones. Another thread may have stored a
            # change (an event replayed for another executor) whose wake has not reached the loop yet.
            keys = self._limit_keys(inbox_id) if self._limit_keys is not None else LimitKeys()
            self._queued_keys[inbox_id] = keys
            if self._limiter.try_acquire(inbox_id, project_key=keys.project, executor_key=keys.executor,
                                         agent_key=keys.agent):
                self._queue.remove(inbox_id)
                self._queued.discard(inbox_id)
                del self._queued_keys[inbox_id]
                return inbox_id
        return None

    def retire(self, handoff: Callable[[str], None], *, interruptible: bool = True) -> None:
        # Called on the event loop when a config reload replaces this pool. Runs in progress finish with this
        # generation's executors; queued inboxes, later wakes and each inbox whose lease is released go to handoff,
        # which wakes the current pool. worker_leases keeps the two pools off the same inbox.
        self._handoff = handoff
        for worker in self._active.values():
            worker.retire(interruptible=interruptible)
        self._requeue.clear()
        self._deferred.clear()
        waiting = tuple(self._queue)
        self._queue.clear()
        self._queued.clear()
        self._queued_keys.clear()
        for inbox_id in waiting:
            handoff(inbox_id)
        self._changed.set()
        if not self._active:
            self._idle_event.set()

    def drain(self) -> None:
        # Called on the event loop when the process stops: Runs in progress finish and write their results, nothing
        # new is claimed, and wakes are dropped; their events stay pending for the next process's startup scan.
        # Resumable Runs waiting on an Orca terminal are detached instead: the next process resumes them.
        self.retire(lambda _inbox_id: None, interruptible=False)
        for worker in self._active.values():
            worker.detach()

    def _make_worker(self, inbox_id: str, worker_id: str) -> AgentWorker:
        # Anyone who can post an event chooses its inbox_id: one that has no memory directory must not end the loop.
        try:
            return self._worker_factory(inbox_id, worker_id)
        except (ValueError, OSError) as exc:
            raise InboxRefused(inbox_id, exc) from exc

    async def _worker_loop(self, worker_idx: int, stop_event: asyncio.Event) -> None:
        worker_id = f"{self._pool_id}-worker-{worker_idx}"
        while self._handoff is None and (not stop_event.is_set() or self._queue):
            inbox_id = self._claim_ready()
            if inbox_id is None:
                # No await between the scan and the wait, so no change is missed.
                self._changed.clear()
                await self._changed.wait()
                continue

            settled = False
            try:
                # Inside the try: a factory that refuses the inbox (AmbiguousProjectMatch) leaves its events pending.
                worker = self._make_worker(inbox_id, worker_id)
                self._active[inbox_id] = worker
                batch_reports = await worker.process_inbox(inbox_id)
                settled = True
                self.reports.extend(batch_reports)
                if batch_reports and self._on_run_reports is not None:
                    try:
                        await asyncio.to_thread(self._on_run_reports, batch_reports)
                    except Exception:  # noqa: BLE001
                        logging.getLogger(__name__).exception(
                            "Post-run callback failed for inbox %s",
                            inbox_id,
                        )
            except InboxRefused as exc:
                logging.getLogger(__name__).warning("%s; its events stay pending", exc)
            except ExecutorFailed as exc:
                settled = exc.batch_settled
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
                self._limiter.release(inbox_id)
                self._changed.set()
                # A batch that failed but stays pending (no such executor, ambiguous project) waits for the next wake:
                # requeueing it here would retry it in a loop.
                deferred = settled and inbox_id in self._deferred
                self._deferred.discard(inbox_id)
                if self._handoff is not None:
                    # The lease is released now; the current pool picks up whatever arrived meanwhile.
                    self._handoff(inbox_id)
                    if not self._active:
                        self._idle_event.set()
                elif deferred or inbox_id in self._requeue:
                    self._requeue.discard(inbox_id)
                    self._wake_on_loop(inbox_id)
                elif not self._queued and not self._active:
                    self._idle_event.set()
            # A dispatch may finish without ever suspending (lease held elsewhere); let wakes and other workers in.
            await asyncio.sleep(0)

    async def _notice_stop(self, stop_event: asyncio.Event) -> None:
        await stop_event.wait()
        self._changed.set()

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
        stop_watch = asyncio.create_task(self._notice_stop(stop_event))
        unsubscribe = self._limiter.subscribe(self._changed.set)
        try:
            await asyncio.gather(*tasks)
        finally:
            unsubscribe()
            for task in (*tasks, stop_watch):
                if not task.done():
                    task.cancel()

    async def wait_until_idle(self, timeout_seconds: float = 5.0) -> None:
        await asyncio.wait_for(self._idle_event.wait(), timeout=timeout_seconds)


class ForwardTargets:
    # What the configuration in force lets each inbox outside a project forward to. Shared by every generation: one that
    # a reload retired still runs what it started, but sends nothing to a project that was taken off the list since.
    def __init__(self, config: HibernautConfig) -> None:
        self._in_force = config.runtime.workdir_binding

    def activate(self, config: HibernautConfig) -> None:
        self._in_force = config.runtime.workdir_binding

    def allowed(self, inbox_id: str, own: tuple[str, ...]) -> tuple[str, ...]:
        bound = self._in_force(inbox_id)
        return tuple(target for target in own if bound is not None and target in bound.forward_to)

    def has_system_tools(self, inbox_id: str) -> bool:
        bound = self._in_force(inbox_id)
        return bound is not None and bound.system_tools


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
    gdrive_poller: DriveTaskPoller | None = None
    gdrive_result_writer: DriveResultWriter | None = None
    mcp_server: MCPServerAdapter | None = None
    switchboard_bridge: SwitchboardInboundBridge | None = None
    worktree_sweeper: WorktreeReclaimSweeper | None = None
    worktree_sweep_interval_seconds: float = DEFAULT_SWEEP_INTERVAL_SECONDS
    gdrive_poll_interval_seconds: float = 30.0
    ingest_only: bool = False
    open_runs: SQLiteOpenRunStore | None = None
    # Shared by every config generation, like db; closed with it.
    ledger: RunLedger | None = None
    # Likewise shared; run() and serve_configured() start it before the first Run and stop it after the last.
    runner: RunnerHandle | None = None
    # One per process, whatever the config generation; closed, with its HTTP service, by the generation that opened it.
    memory_hub: GovernedMemoryHub | None = None
    memory_http: ScopedMemoryHTTPServer | None = None
    # Likewise one per process, with its hub server.
    collaboration: CollaborationRuntime | None = None
    memory_gc: MemoryGarbageCollector | None = None
    memory_gc_interval_seconds: float = DEFAULT_GC_INTERVAL_SECONDS
    # One per process too; a reload points it at the new configuration once that is in force.
    forward_targets: ForwardTargets | None = None
    # One per process and re-pointed likewise; served while Runs may go on. None: SDE sessions get no memory tools.
    memory_access: MemoryAccess | None = None
    reloads: ReloadStatus = field(default_factory=ReloadStatus)
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
        async with _memory_served(self.memory_access), _running_runner(self.runner):
            await self._run(
                server=server,
                stop_event=stop_event,
                force_stop_event=force_stop_event,
                timer_interval_seconds=timer_interval_seconds,
                outbox_interval_seconds=outbox_interval_seconds,
                webhook_host=webhook_host,
                webhook_port=webhook_port,
            )

    async def _run(
        self,
        *,
        server: HTTPServer | None,
        stop_event: asyncio.Event | None,
        force_stop_event: asyncio.Event | None,
        timer_interval_seconds: float,
        outbox_interval_seconds: float,
        webhook_host: str | None,
        webhook_port: int | None,
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
                    halt=stopper,
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
        halt: asyncio.Event | None = None,
    ) -> None:
        # Everything but HTTP ingress and the worker pool; each loop finishes its current tick once stop_event is set.
        # halt is the process's first stop signal: the worktree sweep starts no new round from then on, while the
        # other services keep running through the drain.
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(self.scheduler.run_forever(timer_interval_seconds, stop_event))
            tasks.create_task(self.dispatcher.run_forever(outbox_interval_seconds, stop_event))
            if self.worktree_sweeper is not None:
                tasks.create_task(
                    self.worktree_sweeper.run_forever(
                        interval_seconds=self.worktree_sweep_interval_seconds,
                        stop_event=stop_event,
                        halt=halt,
                    )
                )
            if self.memory_gc is not None:
                tasks.create_task(
                    self.memory_gc.run_forever(
                        interval_seconds=self.memory_gc_interval_seconds, stop_event=stop_event
                    )
                )
            if self.gdrive_poller is not None:
                tasks.create_task(
                    self.gdrive_poller.run_forever(
                        interval_seconds=self.gdrive_poll_interval_seconds,
                        stop_event=stop_event,
                    )
                )

    def close(self) -> None:
        if self.collaboration is not None:
            self.collaboration.close()
        if self.memory_http is not None:
            self.memory_http.shutdown()
        if self.memory_hub is not None:
            self.memory_hub.close()
        if self.ledger is not None:
            self.ledger.close()
        self.db.close()


@asynccontextmanager
async def _running_runner(runner: RunnerHandle | None) -> AsyncGenerator[None]:
    if runner is None:
        yield
        return
    try:
        await runner.start()
        yield
    finally:
        await runner.stop()


def _memory_served(access: MemoryAccess | None) -> AbstractAsyncContextManager[None]:
    return access.serving() if access is not None else nullcontext()


def run_ledger_path(db_path: str | Path, ledger_db_path: str | Path | None) -> Path | None:
    # An in-memory runtime database has no directory to put ledger.db in, so it runs without a ledger.
    if ledger_db_path:
        return Path(ledger_db_path).expanduser().resolve()
    if str(db_path) == ":memory:":
        return None
    return Path(db_path).expanduser().resolve().parent / "ledger.db"


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


def startup_inboxes(inbox: SQLiteInboxAdapter, open_runs: SQLiteOpenRunStore | None) -> Callable[[], list[str]]:
    def scan() -> list[str]:
        ids = inbox.pending_inbox_ids()
        return [*ids, *(i for i in (open_runs.inbox_ids() if open_runs is not None else []) if i not in ids)]
    return scan


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


def concurrency_limits(config: HibernautConfig) -> ConcurrencyLimits:
    executors = {**config.executors, **config.roles}
    return ConcurrencyLimits(
        projects={n: p.max_concurrency for n, p in config.projects.items() if p.max_concurrency is not None},
        executors={
            **config.driver_limits,
            **{n: e.max_concurrency for n, e in executors.items() if e.max_concurrency is not None},
        },
        agents={n: a.max_concurrency for n, a in config.agents.items() if a.max_concurrency is not None},
    )


def executor_quota_hold(
    inbox_id: str,
    *,
    pool: AsyncWorkerPool,
    limits: ConcurrencyLimits,
    keys: LimitKeyResolver,
    route: LimitRoute,
) -> Callable[[Event], str | None] | None:
    if not limits.executors and not limits.agents:
        return route.hold
    return ExecutorQuotaHold(inbox_id, limiter=pool.limiter, on_defer=pool.defer, inner=route.hold,
                             keys_of=lambda event: keys.event_keys(event, route))


def batch_counter(pool: AsyncWorkerPool, keys: LimitKeyResolver) -> Callable[[str, str], None]:
    return lambda inbox_id, agent_id: pool.limiter.count(
        inbox_id, executor_key=keys.driver(agent_id), agent_key=agent_id)


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
    mcp_bearer_token: str | None = None,
    worktree_manager: OrcaWorktreeManager | None = None,
    ingest_only: bool = False,
    gdrive_client: DriveClientProtocol | None = None,
    gdrive_root_folder_id: str | None = None,
    gdrive_pending_folder_id: str | None = None,
    gdrive_in_progress_folder_id: str | None = None,
    gdrive_done_folder_id: str | None = None,
    gdrive_failed_folder_id: str | None = None,
    gdrive_attachments_folder_id: str | None = None,
    gdrive_attachments_dir: str | Path | None = None,
    gdrive_poll_interval_seconds: float = 30.0,
    gdrive_default_inbox_id: str = "default",
    outbox_dir: str | Path | None = None,
    outbox_display_base: str | Path | None = None,
    approval_events_url: str = "http://127.0.0.1:8080/events",
    runs_root: str | Path | None = None,
    limits: ConcurrencyLimits | None = None,
    project_for: Callable[[str], str | None] | None = None,
    ledger: RunLedger | None = None,
    ledger_db_path: str | Path | None = None,
    limiter: ConcurrencyLimiter | None = None,
    memory_hub: GovernedMemoryHub | None = None,
    memory_gc_interval_seconds: float = DEFAULT_GC_INTERVAL_SECONDS,
    expert_mounts: Callable[[str], ExpertMount | None] = lambda inbox_id: None,
) -> AlwaysOnDaemon:
    db = database if database is not None else SQLiteDatabase(db_path)
    if ledger is None and (database is None or ledger_db_path):
        ledger_path = run_ledger_path(db_path, ledger_db_path)
        ledger = RunLedger(ledger_path) if ledger_path is not None else None
    run_ledger = ledger.recorder if ledger is not None else None
    if runs_root is None and database is None and str(db_path) != ":memory:":
        runs_root = Path(db_path).expanduser().resolve().parent / "runs"
    # Directories of Runs that a stop detached; orphans past the retention period are removed at startup.
    open_runs = SQLiteOpenRunStore(db, runs_root) if runs_root is not None else None
    if open_runs is not None:
        open_runs.sweep()
    inbox = SQLiteInboxAdapter(db)
    timers = SQLiteTimerAdapter(db)
    outbox = SQLiteOutboxAdapter(db)
    lock = SQLiteLeaseLockAdapter(db)
    sessions = SQLiteSessionStoreAdapter(db)
    workspaces = SQLiteWorkspaceStore(db)
    if memory_hub is None:
        memory_hub = open_memory_hub(MemoryHubConfig(memory_root=str(memory_root)), ledger=ledger)
    partitions = WorkspacePartitionRegistry(memory_root, hub=memory_hub, mounts=expert_mounts)
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

    def _quota_hold(inbox_id: str) -> Callable[[Event], str | None] | None:
        return executor_quota_hold(inbox_id, pool=pool, limits=resolved_limits, keys=limit_keys,
                                   route=LimitRoute(default_executor=project_default_executor))

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
                on_forward=pool.wake,
                on_worktree_reclaim=lambda wk, _iid=inbox_id: resolved_wt_mgr.reclaim(
                    wk, notify_outbox=outbox, inbox_id=_iid
                ),
                hold=_quota_hold(inbox_id),
                on_batch_start=batch_counter(pool, limit_keys),
                lock=lock,
                sessions=sessions,
                run_start_listener=gdrive_writer,
                open_runs=open_runs,
                run_ledger=run_ledger,
                code_version=__version__,
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
            on_forward=pool.wake,
            on_worktree_reclaim=lambda wk, _iid=inbox_id: resolved_wt_mgr.reclaim(
                wk, notify_outbox=outbox, inbox_id=_iid
            ),
            hold=_quota_hold(inbox_id),
            on_batch_start=batch_counter(pool, limit_keys),
            lock=lock,
            sessions=sessions,
            run_start_listener=gdrive_writer,
            open_runs=open_runs,
            run_ledger=run_ledger,
            code_version=__version__,
            lease_ttl_seconds=lease_ttl_seconds,
            new_event_policy=new_event_policy,
        )

    # Read by _make_worker, like pool, when the pool builds a worker; a queued Drive file moves to 执行中/ as its Run starts.
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
            attachments_folder_id=gdrive_attachments_folder_id or None,
            attachments=(
                DriveAttachmentStore(gdrive_client, gdrive_attachments_dir)
                if gdrive_attachments_dir is not None
                else None
            ),
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

    def _route(inbox_id: str) -> LimitRoute:
        return LimitRoute(project=project_for(inbox_id) if project_for is not None else None,
                          default_executor=project_default_executor)

    resolved_limits = limits or ConcurrencyLimits()
    limit_keys = LimitKeyResolver(
        inbox=inbox, sessions=sessions, open_runs=open_runs, route=_route,
        global_default_executor=global_default_executor or (
            default_executor.executor_id if executors_factory is None and executor_factory is None else None
        ),
    )
    pool = AsyncWorkerPool(
        worker_factory=_make_worker,
        max_concurrency=max_concurrency,
        limits=resolved_limits,
        limit_keys=limit_keys,
        limiter=limiter,
        on_run_reports=_on_reports if gdrive_writer is not None else None,
        on_inbox_failed=_on_failed if gdrive_writer is not None else None,
        # Events a stopped process accepted while draining, and Runs it detached, are picked up here.
        pending_inboxes=startup_inboxes(inbox, open_runs),
    )
    # An ingest-only pool never runs but still collects wakes, so a config reload can hand them to a running pool.
    handler = EventHandler(inbox=inbox, on_wake=pool.wake)
    scheduler = TimerScheduler(timers=timers, inbox=inbox, on_wake=pool.wake)
    dispatcher = OutboxDispatcher(
        outbox=outbox,
        channel=resolved_channel,
        min_interval_seconds=min_outbox_interval_seconds,
    )
    mcp_server = MCPServerAdapter(
        handler=handler,
        sessions=sessions,
        workspaces=workspaces,
        lock=lock,
        inbox=inbox,
        worktree_manager=resolved_wt_mgr,
        outbox=outbox,
        bearer_token=mcp_bearer_token,
        ledger=ledger,
        memory=MemoryControlPlane(memory_hub, ledger),
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
        open_runs=open_runs,
        ledger=ledger,
        memory_hub=memory_hub,
        memory_gc=MemoryGarbageCollector(memory_hub, sessions) if memory_gc_interval_seconds > 0 else None,
        memory_gc_interval_seconds=memory_gc_interval_seconds,
    )


def open_runner_pilot(pilot: RunnerPilotConfig) -> RunnerPilotRelay | None:
    # Reads the Relay token (ValueError when it cannot be used) and listens on nothing yet.
    if not pilot.enabled:
        return None
    return RunnerPilotRelay(
        runner_id=pilot.runner_id,
        project_key=pilot.project_key,
        port=pilot.port,
        token_file=Path(pilot.token_file),
    )


async def start_runner_pilot(relay: RunnerPilotRelay | None) -> bool:
    if relay is None or relay.listening:
        return relay is not None
    try:
        await relay.start()
    except OSError as exc:
        print(f"[runner_pilot] Relay cannot listen: {exc.strerror}; the pilot inbox stays held.",
              file=sys.stderr, flush=True)
        return False
    return True


async def next_runner_pilot(
    current: RunnerPilotRelay | None, before: RunnerPilotConfig, after: RunnerPilotConfig
) -> RunnerPilotRelay | None:
    # The Relay after a reload. A route being turned on has to open, or the reload is refused; one that was on already
    # but could not open is tried again without holding up the other settings.
    if not after.enabled:
        return None
    try:
        relay = current if current is not None else open_runner_pilot(after)
    except ValueError as exc:
        print(f"[runner_pilot] {exc}; the pilot inbox stays held.", file=sys.stderr, flush=True)
        if not before.enabled:
            raise
        return None
    if not await start_runner_pilot(relay) and not before.enabled:
        raise OSError("the pilot relay cannot listen")
    return relay


def pilot_inbox(project: str) -> InboxRoute:
    return InboxRoute(project=project, has_bindings=True, binding=InboxBinding(
        inbox="[runner_pilot]", default_agent=PROTOCOL_CHECK_EXECUTOR_ID, agents=(PROTOCOL_CHECK_EXECUTOR_ID,),
    ))


def pilot_hold(
    relay: RunnerPilotRelay | None, executor_of: Callable[[Event], str | None]
) -> Callable[[Event], str | None]:
    # The pilot inbox never runs on another executor, so whatever cannot run on the route waits for a person.
    def hold(event: Event) -> str | None:
        executor = executor_of(event)
        if executor is None:
            return None
        if executor != PROTOCOL_CHECK_EXECUTOR_ID:
            return f"the pilot inbox only runs {PROTOCOL_CHECK_EXECUTOR_ID}"
        return None if relay is not None and relay.listening else "the pilot relay is not listening"

    return hold


def projectless_inboxes(config: HibernautConfig) -> Callable[[str], bool]:
    return lambda inbox_id: config.runtime.workdir_binding(inbox_id) is not None


def _permissions(link: RunnerLinkConfig) -> AutoModePermissionAdapter:
    if not link.enabled:
        return AutoModePermissionAdapter()
    return AutoModePermissionAdapter(
        allow_tools=AutoModePermissionAdapter.DEFAULT_ALLOW_TOOLS | link.allow_tools,
        block_tools=AutoModePermissionAdapter.DEFAULT_BLOCK_TOOLS | link.block_tools,
    )


def build_daemon_from_config(
    config: HibernautConfig,
    *,
    channel: ChannelPort | None = None,
    database: SQLiteDatabase | None = None,
    demo: bool = False,
    gdrive_client: DriveClientProtocol | None = None,
    switchboard: SwitchboardHandle | None = None,
    # Only the active daemon owns timer reconciliation and Switchboard delivery.
    attach: bool = True,
    ledger: RunLedger | None = None,
    runner_pilot: RunnerPilotRelay | None = None,
    runner: RunnerHandle | None = None,
    limiter: ConcurrencyLimiter | None = None,
    memory_hub: GovernedMemoryHub | None = None,
    collaboration: CollaborationRuntime | None = None,
    forward_targets: ForwardTargets | None = None,
    memory_access: MemoryAccess | None = None,
    reloads: ReloadStatus | None = None,
) -> AlwaysOnDaemon:
    if demo and config.runner_pilot.enabled:
        raise ValueError("runner_pilot.enabled cannot be used with --demo")
    # Every inbox -> project lookup goes through the router, so pattern-matched inboxes get their project's settings
    # and an inbox matching two projects raises AmbiguousProjectMatch instead of picking one.
    router = ProjectRouter(config.projects.values())
    inbox_checkout_map = router.inbox_map(lambda p: p.checkout)

    resolved_channel = channel if channel is not None else ConsoleChannelAdapter(echo=not config.channels.outbox_dir)
    if switchboard is not None:
        resolved_channel = SwitchboardChannelAdapter(
            switchboard_bus(switchboard).publish, fallback=resolved_channel,
            push_policy=NotificationPolicyEngine() if config.collaboration.enabled else None,
        )

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

    peer_projects = [p.name for p in config.projects.values() if p.handoff == "peer" and p.terminal_backend == "orca"]
    if has_configured_executor and not demo and peer_projects:
        # Fails before anything runs: a peer-handoff task sent with no receiver would have nowhere to answer.
        # ValueError, like any unusable setting: a cold start exits, a hot reload keeps the running generation.
        try:
            require_receiver(config.peer)
        except ClaudeCodeError as exc:
            raise ValueError(
                f"handoff = \"peer\" for {', '.join(peer_projects)}, but {exc}"
            ) from exc

    limits = concurrency_limits(config)

    def _project_name(inbox_id: str) -> str | None:
        project = router.resolve(inbox_id)
        return project.name if project is not None else None

    if ledger is None and (database is None or config.runtime.ledger_db_path):
        ledger_path = run_ledger_path(config.runtime.db_path, config.runtime.ledger_db_path)
        ledger = RunLedger(ledger_path) if ledger_path is not None else None
    opened_hub = memory_hub is None
    if memory_hub is None:
        memory_hub = open_memory_hub(config.memory_hub.hub_config(config.runtime.memory_root), ledger=ledger)
    memory_hub.is_projectless = projectless_inboxes(config)
    if collaboration is None:
        collaboration = open_collaboration(config.collaboration, memory_hub)

    def _expert_mount(inbox_id: str) -> ExpertMount | None:
        plan = config.expert_plan(inbox_id)
        return ExpertMount(plan, collaboration.role_store) if plan is not None and collaboration is not None else None

    daemon = build_daemon(
        db_path=config.runtime.db_path,
        database=database,
        memory_hub=memory_hub,
        memory_gc_interval_seconds=config.memory_hub.gc_interval_seconds,
        expert_mounts=_expert_mount,
        limits=limits,
        project_for=_project_name,
        memory_root=config.runtime.memory_root,
        max_concurrency=config.runtime.max_concurrency,
        channel=resolved_channel,
        new_event_policy=config.runtime.new_event_policy,
        mcp_bearer_token=mcp_token,
        ingest_only=ingest_only,
        gdrive_client=resolved_gdrive_client,
        gdrive_root_folder_id=config.gdrive.root_folder_id or None,
        gdrive_pending_folder_id=config.gdrive.pending_folder_id or None,
        gdrive_in_progress_folder_id=config.gdrive.in_progress_folder_id or None,
        gdrive_done_folder_id=config.gdrive.done_folder_id or None,
        gdrive_failed_folder_id=config.gdrive.failed_folder_id or None,
        gdrive_attachments_folder_id=config.gdrive.attachments_folder_id or None,
        gdrive_attachments_dir=(
            config.gdrive.attachments_dir
            or Path(config.runtime.db_path).expanduser().resolve().parent / "attachments"
        ),
        gdrive_poll_interval_seconds=config.gdrive.poll_interval_seconds,
        gdrive_default_inbox_id=config.gdrive.default_inbox_id,
        outbox_dir=config.channels.outbox_dir or None,
        outbox_display_base=config.config_dir or None,
        approval_events_url=local_events_url(config.channels.host, config.channels.port),
        runs_root=Path(config.runtime.db_path).expanduser().resolve().parent / "runs",
        ledger=ledger,
        ledger_db_path=config.runtime.ledger_db_path or None,
        limiter=limiter,
    )

    targets = forward_targets if forward_targets is not None else ForwardTargets(config)
    daemon.forward_targets = targets
    daemon.reloads = reloads if reloads is not None else ReloadStatus()

    channel_at = channel_path(config.runtime.db_path) if not demo else None
    if memory_access is None and channel_at is not None:
        memory_access = MemoryAccess(grants=SQLiteMemoryGrantStore(daemon.db), sessions=daemon.sessions, path=channel_at)
        memory_access.activate(daemon.partitions.memory_for, _permissions(config.runner))
    daemon.memory_access = memory_access

    attempts = AttemptNotes(store=SQLiteAttemptStore(daemon.db), memory_for=daemon.partitions.memory_for)
    if not demo:
        daemon.sessions.on_closed = attempts.session_closed

    if config.runner.enabled and not demo:
        # A reload passes the running one, whose settings cannot change without a restart.
        daemon.runner = runner if runner is not None else open_runner(config)

    if has_configured_executor and not demo:
        # Instantiate all configured [executors.<id>] per project so AgentWorker can sticky-route.
        # Role executors are rebuilt with each config reload, so the next run uses the new profile and launcher.
        terminal_bindings = SQLiteTerminalBindingStore(daemon.db)
        terminal_runner = OrcaVisibleRunner(bindings=terminal_bindings)

        def _notify(inbox_id: str, text: str) -> None:
            daemon.outbox.enqueue(inbox_id, text, metadata={"kind": "peer_notice"})

        def _reviewer_switched(notice: SwitchNotice, text: str) -> None:
            daemon.outbox.enqueue(notice.inbox_id, text, metadata=dict(notice.metadata))

        # Projects with handoff = "peer" share one runner; the receiver's inbox and cursor live outside any Run.
        peer_runner = OrcaVisibleRunner(
            bindings=terminal_bindings, notify=_notify,
            peer=PeerHandoff(config.peer, SQLitePeerMailbox(daemon.db, config.peer.receiver)),
        )
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
        reviewer_store = SQLiteReviewerStore(daemon.db)
        usage = SQLiteWorkUsage(daemon.db)

        def _steward(
            role: RoleConfig, project: ProjectConfig | None, no_terminal: str | None
        ) -> StewardExecutor:
            def in_project(inbox_id: str) -> bool:
                try:
                    return project is not None and router.resolve(inbox_id) is project
                except AmbiguousProjectMatch:
                    return False

            return StewardExecutor(
                profile=role.profile,
                checkouts=inbox_checkout_map,
                sessions=daemon.sessions,
                events=daemon.inbox,
                manifest=lambda: build_routing_manifest(project) if project is not None else "",
                in_project=in_project,
                terminal=cli_terminal if no_terminal is None else TerminalUnavailable(no_terminal),
                workdir=role.workdir,
                branch=role.branch,
            )

        def _delivers(inbox_id: str) -> bool:
            try:
                project = router.resolve(inbox_id)
            except AmbiguousProjectMatch:
                return False
            return project is not None and project.delivery is not None

        ops_status = SQLiteOpsStatus(
            daemon.db, timers=lambda: config.timers, delivers=_delivers, runs=ledger_runs(daemon.ledger),
            sde_agents=lambda: frozenset(
                name for name, agent in config.agents.items()
                if agent.role is not None and canonical_role(agent.role) == SDE_ROLE
            ),
        )

        def _bound_steward(role: RoleConfig, bound: WorkdirBinding) -> StewardExecutor:
            def listed(inbox_id: str) -> tuple[ProjectConfig, ...]:
                route = _route(inbox_id)
                allowed = route.forward_to if route is not None else ()
                return tuple(p for target in allowed for p in config.projects.values() if p.inbox_id == target)

            def systems(inbox_id: str) -> tuple[WorkdirBinding, ...]:
                route = _route(inbox_id)
                return tuple(b for b in config.runtime.inbox_bindings
                             if b.system_tools and route is not None and b.rule.literal in route.forward_to)

            def readable(inbox_id: str, asked: str) -> bool:
                if any(b.rule.literal == asked for b in systems(inbox_id)):
                    return True
                try:
                    project = router.resolve(asked)
                except AmbiguousProjectMatch:
                    return False
                return project is not None and project in listed(inbox_id)

            return StewardExecutor(
                profile=role.profile,
                checkouts=inbox_checkout_map,
                sessions=daemon.sessions,
                events=daemon.inbox,
                manifest=lambda: "",
                in_project=lambda _inbox_id: False,
                terminal=cli_terminal,
                workdir=bound.workdir,
                branch=role.branch,
                scope=GlobalScope(
                    manifest=lambda inbox_id: build_system_manifest(
                        inbox_id, bound.binding, tuple(config.agents.values())
                    ) if bound.system_tools else build_global_manifest(
                        inbox_id, bound.binding, tuple(config.agents.values()), listed(inbox_id),
                        config.runtime.default_agent, systems(inbox_id),
                    ),
                    readable=readable,
                    system=SystemToolbox(
                        reloads=daemon.reloads,
                        allowed=lambda: targets.has_system_tools(bound.rule.literal or ""),
                        status=ops_status,
                    ) if bound.system_tools else None,
                ),
            )

        def _role_executors(
            wt_mgr: OrcaWorktreeManager, no_terminal: str | None, project: ProjectConfig | None = None,
        ) -> dict[str, ExecutorPort]:
            # no_terminal says why this inbox has no terminal backend; None means Orca opens the sessions.
            # Each SDE run reviews through the reviewer built for the same project and config generation.
            runner = peer_runner if project is not None and project.handoff == "peer" else terminal_runner
            delivery = project.delivery if project is not None else None

            def terminal(role: RoleConfig) -> OrcaVisibleRunner | TerminalUnavailable | None:
                if not role.interactive:
                    return None
                return runner if no_terminal is None else TerminalUnavailable(no_terminal)

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
                if role.role == STEWARD_ROLE:
                    built[name] = _steward(role, project, no_terminal)
                if role.sde is None:
                    continue
                built[name] = SDEExecutor(
                    profile=role.profile,
                    options=role.sde,
                    # A list has no reviewer of its own: the pool chooses one per task.
                    reviewer=(reviewer_for(role.sde.reviewer)
                              if role.sde.review and isinstance(role.sde.reviewer, str) else None),
                    pool=ReviewerPool(
                        configured=role.sde.reviewer, lookup=reviewer_for, store=reviewer_store, owner=name,
                        table=config.availability.file,
                        cooldown_seconds=config.availability.quota_cooldown_seconds, notify=_reviewer_switched,
                    ) if role.sde.review else None,
                    # An independently dispatched review always comes from a code-reviewer executor: the first one
                    # the role names, else the first one declared.
                    peer_reviewer=next((n for n in (*role.sde.candidates, *reviewers) if n in reviewers), None),
                    worktree_manager=wt_mgr,
                    inbox_checkout_map=inbox_checkout_map,
                    terminal=terminal(role),
                    tasks=tasks,
                    delivery=PRDelivery(delivery, deliveries) if delivery is not None else None,
                    live_peers=usage.peers,
                    memory=memory_access,
                    attempts=attempts,
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
                settle=attempts.settle,
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
            if daemon.runner is not None:
                for exec_name in config.runner.executors:
                    executors_by_project[proj.name][exec_name] = daemon.runner.bridge(exec_name, proj)
            executors_by_project[proj.name].update(_role_executors(
                wt_mgr,
                None if proj.terminal_backend == "orca" else f"project '{proj.name}' has terminal_backend = \"none\"",
                proj,
            ))

        default_mgr = OrcaWorktreeManager(
            workspace_store=daemon.workspaces,
            session_store=daemon.sessions,
            usage=usage.blockers,
            settle=attempts.settle,
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
        # Only the agents that can work outside a project are registered for an inbox bound to a workdir.
        bound_executors: dict[str, dict[str, ExecutorPort]] = {
            bound.rule.pattern: {
                exec_name: LocalCLIExecutor(
                    worktree_manager=default_mgr,
                    inbox_checkout_map=inbox_checkout_map,
                    templates=config.executors,
                    default_executor=exec_name,
                    bound_workdir=bound.workdir,
                    readonly=bound.readonly,
                )
                for exec_name in config.executors
            }
            for bound in config.runtime.inbox_bindings
        }
        for bound in config.runtime.inbox_bindings:
            for name in bound.binding.agents:
                if name in config.roles:
                    bound_executors[bound.rule.pattern][name] = _bound_steward(config.roles[name], bound)
            if not bound.readonly and bound.rule.literal is not None:
                daemon.partitions.register(bound.rule.literal, bound.workdir)
        for registry in (*executors_by_project.values(), *bound_executors.values(), fallback_executors):
            registry[PROTOCOL_CHECK_EXECUTOR_ID] = ClosedRouteExecutor()

        def _manager_for(inbox_id: str | None, work_key: str) -> OrcaWorktreeManager:
            if inbox_id is not None:
                project = router.resolve(inbox_id)
            else:
                record = daemon.workspaces.get(work_key)
                checkout = Path(record.repo_checkout).resolve() if record is not None else None
                project = next((p for p in config.projects.values() if Path(p.checkout).resolve() == checkout), None)
            return wt_mgr_by_project[project.name] if project is not None else default_mgr

        def _sweep_target(record: WorkspaceRecord) -> SweepTarget | None:
            # Only worktrees under a configured project's checkout; anything else is left to a person.
            checkout = Path(record.repo_checkout).resolve()
            project = next((p for p in config.projects.values() if Path(p.checkout).resolve() == checkout), None)
            if project is None:
                return None
            return SweepTarget(manager=wt_mgr_by_project[project.name], delivery=project.delivery,
                               inbox_id=record.inbox_id or project.inbox_id)

        if config.runtime.worktree_sweep_interval_seconds > 0:
            daemon.worktree_sweeper = WorktreeReclaimSweeper(
                workspaces=daemon.workspaces, deliveries=deliveries, outbox=daemon.outbox, target_for=_sweep_target,
            )
            daemon.worktree_sweep_interval_seconds = config.runtime.worktree_sweep_interval_seconds

        pilot = config.runner_pilot

        def _on_pilot_route(inbox_id: str) -> bool:
            return pilot.enabled and inbox_id == pilot.inbox_id

        def _route(inbox_id: str) -> InboxRoute | None:
            # None means nothing may be forwarded there: no project, two projects, or no memory partition of that name.
            try:
                check_inbox_id(inbox_id)
                project = router.resolve(inbox_id)
            except (ValueError, AmbiguousProjectMatch):
                return None
            # The pilot inbox offers its own executor and no other, whatever binding covers it when the route is off.
            if project is not None:
                route = project.route(inbox_id)
            else:
                bound = config.runtime.workdir_binding(inbox_id)
                route = replace(bound.route(), forward_to=targets.allowed(inbox_id, bound.forward_to)) if (
                    bound is not None) else None
            return pilot_inbox(route.project) if route is not None and _on_pilot_route(inbox_id) else route

        # One per config generation, so a held event is logged once each time it enters the wait.
        unmatched_hold = UnmatchedProjectHold(daemon.sessions)
        workdir_holds = {b.rule.pattern: MissingWorkdirHold(daemon.sessions, b.workdir)
                         for b in config.runtime.inbox_bindings}

        def _pilot_executor(event: Event) -> str | None:
            return AgentWorker.expected_executor_id(
                event, daemon.sessions, project_default=PROTOCOL_CHECK_EXECUTOR_ID, global_default=None,
            )

        pilot_route = LimitRoute(
            default_executor=PROTOCOL_CHECK_EXECUTOR_ID, hold=pilot_hold(runner_pilot, _pilot_executor)
        )

        def _cli_route(inbox_id: str) -> LimitRoute:
            if _on_pilot_route(inbox_id):
                return pilot_route
            project = router.resolve(inbox_id)
            if project is None:
                bound = config.runtime.workdir_binding(inbox_id)
                if bound is None:
                    return LimitRoute(hold=unmatched_hold)
                return LimitRoute(binding=bound.binding, hold=workdir_holds[bound.rule.pattern])
            return LimitRoute(project=project.name, default_executor=project.default_agent,
                              binding=project.route(inbox_id).binding)

        origin = event_origin_for(GroupEntrances(config.feishu_groups))
        limit_keys = LimitKeyResolver(
            inbox=daemon.inbox, sessions=daemon.sessions, open_runs=daemon.open_runs, route=_cli_route,
            global_default_executor=config.runtime.default_agent,
            drivers={name: agent.executor for name, agent in config.agents.items()},
        )

        def _pilot_worker(inbox_id: str, worker_id: str) -> AgentWorker:
            # Detached Runs of another executor belong to the worker that can resume them, so nothing runs meanwhile.
            detached = daemon.open_runs.list_open(inbox_id) if daemon.open_runs is not None else []
            if detached:
                raise ExecutorFailed(
                    f"Inbox {inbox_id} still has detached Runs of "
                    f"{', '.join(sorted({run.executor_id for run in detached}))}; "
                    "turn runner_pilot off to let them finish first"
                )
            executor: ExecutorPort = (
                ProtocolCheckExecutor(
                    runner=runner_pilot, inbox_id=pilot.inbox_id, project_key=pilot.project_key,
                    timeout_seconds=pilot.timeout_seconds,
                )
                if runner_pilot is not None
                else ClosedRouteExecutor()
            )
            return AgentWorker(
                worker_id=worker_id,
                inbox=daemon.inbox,
                timers=daemon.timers,
                memory=daemon.partitions.memory_for(inbox_id),
                permissions=_permissions(config.runner),
                outbox=daemon.outbox,
                executors={PROTOCOL_CHECK_EXECUTOR_ID: executor},
                project_default_executor=PROTOCOL_CHECK_EXECUTOR_ID,
                routes=_route,
                origin=origin,
                on_forward=daemon.pool.wake,
                # The runner reclaims its own read-only worktrees; no worktree of this process is involved.
                on_worktree_reclaim=None,
                hold=executor_quota_hold(inbox_id, pool=daemon.pool, limits=limits, keys=limit_keys, route=pilot_route),
                on_batch_start=batch_counter(daemon.pool, limit_keys),
                run_start_listener=daemon.gdrive_result_writer,
                open_runs=daemon.open_runs,
                run_ledger=daemon.ledger.recorder if daemon.ledger is not None else None,
                code_version=__version__,
                lock=daemon.lock,
                sessions=daemon.sessions,
                # A check has no side effects to protect, and a new event is the controlled way to interrupt it.
                new_event_policy="interrupt",
            )

        def _cli_worker_factory(inbox_id: str, worker_id: str) -> AgentWorker:
            # Both raise (InboxRefused by way of the pool, AmbiguousProjectMatch) before any lease, executor or
            # workspace exists for the inbox.
            check_inbox_id(inbox_id)
            if _on_pilot_route(inbox_id):
                return _pilot_worker(inbox_id, worker_id)
            project = router.resolve(inbox_id)
            bound = config.runtime.workdir_binding(inbox_id) if project is None else None
            if project is not None:
                executors = executors_by_project[project.name]
            else:
                executors = bound_executors[bound.rule.pattern] if bound is not None else fallback_executors
            wt_mgr = wt_mgr_by_project[project.name] if project is not None else default_mgr
            reclaim = worktree_reclaimer(wt_mgr, outbox=daemon.outbox, inbox_id=inbox_id,
                                         delivery=project.delivery if project is not None else None,
                                         deliveries=deliveries)
            return AgentWorker(
                worker_id=worker_id,
                inbox=daemon.inbox,
                timers=daemon.timers,
                memory=daemon.partitions.memory_for(inbox_id),
                permissions=_permissions(config.runner),
                outbox=daemon.outbox,
                executors=executors,
                project_default_executor=project.default_agent if project is not None else None,
                global_default_executor=config.runtime.default_agent,
                routes=_route,
                origin=origin,
                on_forward=daemon.pool.wake,
                on_worktree_reclaim=reclaim,
                hold=executor_quota_hold(inbox_id, pool=daemon.pool, limits=limits, keys=limit_keys,
                                         route=_cli_route(inbox_id)),
                on_batch_start=batch_counter(daemon.pool, limit_keys),
                run_start_listener=daemon.gdrive_result_writer,
                open_runs=daemon.open_runs,
                run_ledger=daemon.ledger.recorder if daemon.ledger is not None else None,
                code_version=__version__,
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
            limits=limits,
            limit_keys=limit_keys,
            limiter=limiter,
            on_run_reports=_cli_on_reports if daemon.gdrive_result_writer is not None else None,
            on_inbox_failed=_cli_on_failed if daemon.gdrive_result_writer is not None else None,
            pending_inboxes=startup_inboxes(daemon.inbox, daemon.open_runs),
        )
        daemon.handler = EventHandler(inbox=daemon.inbox, on_wake=daemon.pool.wake)
        daemon.scheduler = TimerScheduler(
            timers=daemon.timers,
            inbox=daemon.inbox,
            on_wake=daemon.pool.wake,
        )
        daemon.mcp_server = MCPServerAdapter(
            handler=daemon.handler,
            sessions=daemon.sessions,
            workspaces=daemon.workspaces,
            lock=daemon.lock,
            inbox=daemon.inbox,
            worktree_manager=default_mgr,
            outbox=daemon.outbox,
            bearer_token=mcp_token,
            worktree_manager_for=_manager_for,
            ledger=daemon.ledger,
            memory=MemoryControlPlane(memory_hub, daemon.ledger),
        )

    daemon.handler.keep_local(
        lambda inbox_id: (bound := config.runtime.workdir_binding(inbox_id)) is not None and bound.system_tools
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

    if switchboard is not None and attach:
        attach_switchboard(daemon, switchboard, config.switchboard.consumer_id, config.feishu_groups)

    coordinator: CloseoutCoordinator | None = None
    if collaboration is not None:
        daemon.collaboration = collaboration
        coordinator = mount_collaboration(daemon, collaboration, config, router)

    if opened_hub:
        # Last, so settings refused above leave no listener behind. A reload passes the running hub, whose service goes
        # on; its settings cannot change without a restart.
        if collaboration is not None and coordinator is not None:
            # It keeps this generation's handler, whose wakes a reload forwards to the pool in force.
            collaboration.hub_server = open_collaboration_hub(
                config.collaboration, collaboration, coordinator, hub=memory_hub, ledger=daemon.ledger
            )
        try:
            daemon.memory_http = open_memory_http(config.memory_hub.http, memory_hub)
        except BaseException:
            if collaboration is not None:
                collaboration.close()
            raise
    if attach:
        daemon.timers.reconcile(config.timers)
    return daemon


def mount_collaboration(
    daemon: AlwaysOnDaemon, runtime: CollaborationRuntime, config: HibernautConfig, router: ProjectRouter[ProjectConfig]
) -> CloseoutCoordinator:
    keys = {key for p in router.projects if (key := project_memory_key(p.inbox_id)) is not None}
    coordinator = ProjectCloseoutCoordinator(
        runtime.closeouts,
        chief=ChiefOfStaffInboxSink(daemon.handler.queue, config.collaboration.chief_inbox),
        taskboard=TaskboardFollowUpSink(runtime.taskboard) if runtime.taskboard is not None else None,
        memory=runtime.memory_sink,
        knows_project=keys.__contains__,
    )
    assert daemon.mcp_server is not None
    daemon.mcp_server.mount_collaboration(CollaborationControlPlane(
        roles=runtime.role_store,
        pool=runtime.pool,
        closeout=coordinator,
        drafted={(e.plan.project_key, e.plan.role_id): e.plan for p in router.projects for e in p.expert_bindings},
        budget_chars=config.collaboration.expert_budget_chars,
    ))
    return coordinator


def companion_config(config: HibernautConfig, environ: Mapping[str, str]) -> HibernautConfig:
    # The stdio MCP companion runs no agent, so it opens no runner, and the HTTP services belong to the daemon.
    link = config.collaboration
    taskboard = link.taskboard if environ.get(link.taskboard.token_env, "").strip() else CollaborationTaskboardConfig()
    return replace(
        config, runner=RunnerLinkConfig(), memory_hub=replace(config.memory_hub, http=MemoryHTTPConfig()),
        collaboration=replace(link, hub=CollaborationHubConfig(), taskboard=taskboard),
    )


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
    audit: AuditSinkPort | None = None,
) -> SwitchboardHandle | None:
    # audit reaches only the in-process Switchboard (Stage A); a relay's audits stay with the remote hub.
    if not link.enabled:
        return None
    credentials = EnvCredentialBroker(take_switchboard_secrets(environ))
    if link.mode == "relay":
        return open_switchboard_relay(link, credentials)
    assert link.config_path is not None
    board = build_switchboard(load_switchboard_config(link.config_path), credentials=credentials, audit=audit)
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


def ledger_runs(ledger: RunLedger | None) -> Callable[[str], RunSummary | None] | None:
    if ledger is None:
        return None

    def latest(event_id: str) -> RunSummary | None:
        found = ledger.store.list_runs(event_id=event_id, limit=1)
        if not found:
            return None
        run = found[0]
        return RunSummary(result=run.outcome_kind, error=run.error_summary or "", seconds=run.duration_ms / 1000)

    return latest


def attach_switchboard(
    daemon: AlwaysOnDaemon, board: SwitchboardHandle, consumer_id: str, groups: Sequence[FeishuGroup] = (),
) -> SwitchboardInboundBridge:
    # Subscribing again replaces the previous handler, so a config reload re-points delivery at the new daemon.
    bridge = SwitchboardInboundBridge(daemon.handler, groups=GroupEntrances(groups))
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
    listeners: ListenerHandover | None = None,
    runner_pilot: RunnerPilotRelay | None = None,
) -> None:
    # A reload never waits for Runs in progress: the listener stays up and takes the new handler, services restart
    # within one tick, and the old worker pool is retired (no new claims) while its last Runs finish in the
    # background. worker_leases keeps the old and new pools off the same inbox. A stop follows the same principle:
    # see drain_pools().
    address = (config.channels.host, config.channels.port)
    server = (
        listeners.server(address, daemon.http_handler())
        if listeners is not None
        else WebhookHTTPServer(address, daemon.http_handler())
    )
    reloads = daemon.reloads
    reloads.watch(path)
    changes = config_changes(path, stop, interval=reload_interval, reloads=reloads)

    version = reloads.latest

    def refuse(reason: str) -> None:
        print(f"[config] {reason}", file=sys.stderr, flush=True)
        reloads.settle(version, reason)

    # Before the first pool scans its inbox: held events are not looked at again until the next wake.
    await start_runner_pilot(runner_pilot)

    # The runner outlives the task group, which ends with the last Run of every generation.
    async with _memory_served(daemon.memory_access), _running_runner(daemon.runner), asyncio.TaskGroup() as tasks:
        # Shared across config-reload generations so retired Runs keep occupying slots until they finish.
        shared_limiter = daemon.pool.limiter
        generation = _Generation.start(daemon, tasks, outbox_interval_seconds=outbox_interval_seconds, halt=stop)
        # Every generation whose pool may still hold Runs: the current one and those retired by reloads.
        generations = [generation]
        # Relays a reload took out of service, each stopping once the Runs that were using it have ended.
        closing: list[asyncio.Task[None]] = []
        server_thread = _serve_in_thread(server)

        def wake_current(inbox_id: str) -> None:
            generation.daemon.pool.wake(inbox_id)

        async def close_relay(relay: RunnerPilotRelay, users: list[asyncio.Task[None]]) -> None:
            if users:
                await asyncio.wait(users)
            await relay.stop()
            print("[runner_pilot] Relay closed.", file=sys.stderr, flush=True)

        try:
            async for candidate in changes:
                version = reloads.latest
                if candidate == config:
                    reloads.settle(version)
                    continue
                if (candidate.runtime.db_path, candidate.runtime.memory_root, candidate.runtime.ledger_db_path) != (
                    config.runtime.db_path,
                    config.runtime.memory_root,
                    config.runtime.ledger_db_path,
                ):
                    refuse("Storage path changes require a restart.")
                    continue
                if candidate.switchboard != config.switchboard:
                    refuse("[switchboard] changes require a restart.")
                    continue
                if config.runner_pilot.enabled and candidate.runner_pilot.enabled and (
                    candidate.runner_pilot != config.runner_pilot
                ):
                    refuse("[runner_pilot] changes need enabled = false first; keeping current settings.")
                    continue
                if replace(candidate.runner, executors=()) != replace(config.runner, executors=()):
                    refuse("[runner] changes require a restart.")
                    continue
                if candidate.memory_hub != config.memory_hub:
                    refuse("[memory_hub] changes require a restart.")
                    continue
                if candidate.collaboration != config.collaboration:
                    refuse("[collaboration] and [experts] changes require a restart.")
                    continue
                address = (candidate.channels.host, candidate.channels.port)
                next_server: WebhookHTTPServer | None = None
                next_pilot: RunnerPilotRelay | None = None
                try:
                    next_pilot = await next_runner_pilot(runner_pilot, config.runner_pilot, candidate.runner_pilot)
                    replacement = build_daemon_from_config(
                        candidate,
                        database=daemon.db,
                        demo=demo,
                        switchboard=switchboard,
                        ledger=daemon.ledger,
                        runner_pilot=next_pilot,
                        runner=daemon.runner,
                        limiter=shared_limiter,
                        memory_hub=daemon.memory_hub,
                        collaboration=daemon.collaboration,
                        forward_targets=daemon.forward_targets,
                        memory_access=daemon.memory_access,
                        reloads=reloads,
                        attach=False,
                    )
                    handler = replacement.http_handler()
                    if address != (config.channels.host, config.channels.port):
                        next_server = WebhookHTTPServer(address, handler)
                except (OSError, ValueError, OverflowError) as exc:
                    if reloads.on_disk(version):
                        refuse(f"Cannot activate settings ({type(exc).__name__}); keeping current settings.")
                    if next_pilot is not None and next_pilot is not runner_pilot:
                        await next_pilot.stop()
                    continue
                finally:
                    # Building it applies the candidate's to the shared hub, but the current settings are in force still.
                    if daemon.memory_hub is not None:
                        daemon.memory_hub.is_projectless = projectless_inboxes(config)

                # Stop the old services first so two dispatchers never send the same Outbox row.
                await generation.stop_services()
                # An editor knows no lock. Nothing is awaited from here until the version is settled.
                if not reloads.on_disk(version):
                    print(f"[config] {path} changed again before it was in force; reading it again.",
                          file=sys.stderr, flush=True)
                    generation.start_services(tasks, outbox_interval_seconds=outbox_interval_seconds, halt=stop)
                    if next_server is not None:
                        next_server.server_close()
                    if next_pilot is not None and next_pilot is not runner_pilot:
                        await next_pilot.stop()
                    continue

                if daemon.memory_hub is not None:
                    daemon.memory_hub.is_projectless = projectless_inboxes(candidate)
                if replacement.runner is not None:
                    replacement.runner.apply(candidate)
                if replacement.forward_targets is not None:
                    replacement.forward_targets.activate(candidate)
                if replacement.memory_access is not None:
                    replacement.memory_access.activate(replacement.partitions.memory_for, _permissions(candidate.runner))
                replacement.timers.reconcile(candidate.timers)
                previous = generation
                # Apply the new caps only after activation succeeds; retired Runs stay counted in shared_limiter.
                shared_limiter.reconfigure(candidate.runtime.max_concurrency, concurrency_limits(candidate))
                generation = _Generation.start(replacement, tasks, outbox_interval_seconds=outbox_interval_seconds,
                                               halt=stop)
                generations = [g for g in generations if g.runs is not None and not g.runs.done()] + [generation]
                if switchboard is not None:
                    attach_switchboard(replacement, switchboard, candidate.switchboard.consumer_id,
                                       candidate.feishu_groups)
                # From here every wake reaching the old pool, from its in-flight requests or released leases,
                # is forwarded to the new one.
                previous.retire_pool(wake_current)
                if runner_pilot is not None and next_pilot is not runner_pilot:
                    closing.append(tasks.create_task(close_relay(
                        runner_pilot,
                        [g.runs for g in generations if g is not generation and g.runs is not None],
                    )))
                runner_pilot = next_pilot
                replaced_listener: tuple[WebhookHTTPServer, threading.Thread] | None = None
                if next_server is None:
                    server.RequestHandlerClass = handler
                else:
                    replaced_listener = server, server_thread
                    server, server_thread = next_server, _serve_in_thread(next_server)
                daemon, config = replacement, candidate
                reloads.settle(version)
                if replaced_listener is not None:
                    # Accepted requests finish against the old adapters before the old listener closes.
                    await _close_server(*replaced_listener)
                print(f"[config] Reloaded {path}", file=sys.stderr, flush=True)
        finally:
            reloads.watch(None)
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
            # Every Run has ended, and an interrupted one was stopped on the node first.
            await asyncio.gather(*closing)
            if runner_pilot is not None:
                await runner_pilot.stop()
            await _close_server(server, server_thread, release=listeners.release if listeners is not None else None)
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
        halt: asyncio.Event | None = None,
    ) -> _Generation:
        services_stop = asyncio.Event()
        pool_stop = asyncio.Event()
        daemon.pool.bind(asyncio.get_running_loop())
        runs = None if daemon.ingest_only else tasks.create_task(daemon.pool.run_forever(pool_stop))
        services = tasks.create_task(
            daemon.run_services(services_stop, outbox_interval_seconds=outbox_interval_seconds, halt=halt)
        )
        return cls(daemon, services, services_stop, pool_stop, runs)

    def start_services(
        self, tasks: asyncio.TaskGroup, *, outbox_interval_seconds: float, halt: asyncio.Event | None = None
    ) -> None:
        self.services_stop = asyncio.Event()
        self.services = tasks.create_task(
            self.daemon.run_services(self.services_stop, outbox_interval_seconds=outbox_interval_seconds, halt=halt)
        )

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


async def _close_server(
    server: WebhookHTTPServer,
    thread: threading.Thread,
    *,
    release: Callable[[WebhookHTTPServer], None] | None = None,
) -> None:
    await asyncio.to_thread(server.shutdown)
    if release is not None:
        # Waits for the supervisor's ack; services still run on the loop until stop_services().
        await asyncio.to_thread(release, server)
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
    parser.add_argument("--mcp-token", default=None)
    parser.add_argument("--config-poll-interval", type=float, default=1.0)
    parser.add_argument("--outbox-interval", type=float, default=1.0)
    banner = f"Hibernaut {__version__}"
    parser.add_argument("--version", action="version", version=banner)
    args = parser.parse_args()

    # Before anything can start a subprocess: the descriptor numbers leave os.environ here.
    listeners = ListenerHandover.from_environ(os.environ)
    board: SwitchboardHandle | None = None
    if args.config:
        cfg = load_config(args.config)
        ledger_path = run_ledger_path(cfg.runtime.db_path, cfg.runtime.ledger_db_path)
        # Opened before the Switchboard, whose in-process audits it records; the daemon owns and closes it.
        ledger = RunLedger(ledger_path) if ledger_path is not None else None
        # The stdio MCP companion runs beside a daemon that already owns the Switchboard port or relay
        # connection; a second relay agent with the same consumer_id would supersede the daemon's.
        board = (
            None
            if args.mcp_stdio
            else start_switchboard(cfg.switchboard, audit=ledger.switchboard_audit() if ledger is not None else None)
        )
        runner_pilot: RunnerPilotRelay | None = None
        if not args.mcp_stdio:
            try:
                runner_pilot = open_runner_pilot(cfg.runner_pilot)
            except ValueError as exc:
                # The rest of the daemon runs; the pilot inbox is held, never run on another executor.
                print(f"[runner_pilot] {exc}; the pilot inbox stays held.", file=sys.stderr, flush=True)
        companion = companion_config(cfg, os.environ) if args.mcp_stdio else cfg
        daemon = build_daemon_from_config(
            companion, demo=args.demo, switchboard=board, ledger=ledger, runner_pilot=runner_pilot, attach=not args.mcp_stdio
        )
        host = cfg.channels.host
        port = cfg.channels.port
    else:
        daemon = build_daemon(
            db_path=args.db,
            memory_root=args.memory_root,
            max_concurrency=args.workers,
            new_event_policy=args.policy,
            mcp_bearer_token=args.mcp_token,
        )
        host = args.host
        port = args.port

    if args.mcp_stdio:
        # Keep stdout clean for JSON-RPC 2.0 frames in stdio mode.
        assert daemon.mcp_server is not None
        listeners.close()
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
                    listeners=listeners,
                    runner_pilot=runner_pilot,
                )
            else:
                server = listeners.server((host, port), daemon.http_handler())
                try:
                    await daemon.run(
                        server=server,
                        stop_event=stop_event,
                        force_stop_event=force_stop,
                        outbox_interval_seconds=args.outbox_interval,
                    )
                finally:
                    listeners.release(server)
                    server.server_close()
        finally:
            listeners.close()
            # The relay closes while the database is still open: a push arriving until then is stored and acked,
            # and from the close on the hub keeps events for this consumer until the next daemon connects.
            if board is not None:
                board.stop()
            daemon.close()

    asyncio.run(_serve())


if __name__ == "__main__":
    main()
