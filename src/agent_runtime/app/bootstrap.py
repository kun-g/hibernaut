from __future__ import annotations

import os
import sys
from collections.abc import MutableMapping
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
from agent_runtime.adapters.runner_bridge import RunnerExecutorBridge
from agent_runtime.app.config import HibernautConfig, ProjectConfig, RunnerLinkConfig, runner_driver
from agent_runtime.core.ports import ChannelPort, RunCapabilities
from agent_runtime.engine import (
    AgentWorker,
    EventHandler,
    OutboxDispatcher,
    TimerScheduler,
)
from runner.cli_drivers import CLIDriver
from runner.config import NamespacePolicy, ProjectCheckout, RunnerConfig
from runner.daemon import RunnerDaemon
from runner.protocol import RunnerPort
from runner.registry import WorkspaceAffinityStore
from runner.transport.in_process import InProcessRunnerClient
from runner.transport.relay_hub import RunnerRelayHub

RUNNER_CALLER_ID = "hibernaut"
RUNNER_NAMESPACE = "hb"
_LOOPBACK_ADDRESSES = frozenset({"127.0.0.1", "::1"})


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


class RunnerHandle:
    # One per process and shared by every config generation: a reload hands it on, so the Runs of a retired pool and of
    # the next one go through the same daemon or hub.
    def __init__(self, link: RunnerLinkConfig, port: RunnerPort) -> None:
        self._link = link
        self._port = port
        self._closed = False
        self.started = False

    @property
    def address(self) -> tuple[str, int] | None:
        return None

    def bridge(self, executor_id: str, project: ProjectConfig) -> RunnerExecutorBridge:
        return RunnerExecutorBridge(
            runner=self._port,
            executor_id=executor_id,
            project_key=project.name,
            timeout_seconds=self._link.timeout_seconds,
            # Every executor here is a coding CLI; its calls go to the PermissionPort of the Run's worker.
            executor_calls_tools=True,
            authorize_tools=True,
            node_wait_seconds=self._link.node_wait_seconds,
        )

    def apply(self, config: HibernautConfig) -> None:
        pass

    async def start(self) -> None:
        if not self.started:
            await self._open()
            self.started = True

    async def stop(self) -> None:
        # Called once the pools are done: by then every Run has ended and its process group is reaped.
        self.started = False
        if not self._closed:
            self._closed = True
            await self._close()

    async def _open(self) -> None:
        pass

    async def _close(self) -> None:
        pass


class InProcessRunnerHandle(RunnerHandle):
    def __init__(self, config: HibernautConfig) -> None:
        link = config.runner
        state_dir = _runner_dir(config, link.state_dir)
        self._daemon = RunnerDaemon(
            RunnerConfig(
                runner_id=link.runner_id,
                max_concurrent=link.max_concurrent or config.runtime.max_concurrency,
                worktree_backend=link.worktree_backend,
                interactive_surface="none",
                namespaces={
                    RUNNER_NAMESPACE: NamespacePolicy(
                        namespace=RUNNER_NAMESPACE, prefix=RUNNER_NAMESPACE, allowed_callers=(RUNNER_CALLER_ID,)
                    )
                },
                projects=_runner_projects(config),
                state_dir=state_dir,
                logs_dir=Path(link.logs_dir) if link.logs_dir else state_dir / "logs",
            ),
            drivers=_runner_drivers(config),
        )
        super().__init__(
            link,
            InProcessRunnerClient(self._daemon, caller_id=RUNNER_CALLER_ID, bound_namespace=RUNNER_NAMESPACE),
        )

    def apply(self, config: HibernautConfig) -> None:
        self._daemon.reconfigure(projects=_runner_projects(config), drivers=_runner_drivers(config))

    async def _close(self) -> None:
        self._daemon.close()


class RelayRunnerHandle(RunnerHandle):
    def __init__(self, config: HibernautConfig, environ: MutableMapping[str, str]) -> None:
        link = config.runner
        # Executor subprocesses inherit os.environ; moving the token out keeps it away from agents.
        token = environ.pop(link.token_env, "")
        if not token:
            raise ValueError(f'runner.mode = "relay" needs {link.token_env} in the environment')
        affinity_db = Path(link.affinity_db_path) if link.affinity_db_path else _runner_dir(config, "") / "affinity.db"
        self._affinity = WorkspaceAffinityStore(affinity_db)
        self._hub = RunnerRelayHub(shared_token=token, affinity=self._affinity)
        self._address: tuple[str, int] | None = None
        super().__init__(link, self._hub)

    @property
    def address(self) -> tuple[str, int] | None:
        return self._address

    async def _open(self) -> None:
        self._address = await self._hub.start(self._link.listen_host, self._link.listen_port)
        host, port = self._address
        print(f"[hibernaut] [runner] relay hub listening on ws://{host}:{port}/ws/runner", file=sys.stderr, flush=True)
        if host not in _LOOPBACK_ADDRESSES:
            print(
                "[hibernaut] [runner] WARNING: the relay hub speaks plain ws; put TLS in front of it before nodes "
                "connect over a network.",
                file=sys.stderr,
                flush=True,
            )

    async def _close(self) -> None:
        self._address = None
        await self._hub.stop()
        self._affinity.close()


def open_runner(
    config: HibernautConfig, *, environ: MutableMapping[str, str] | None = None
) -> RunnerHandle | None:
    if not config.runner.enabled:
        return None
    if config.runner.mode == "relay":
        return RelayRunnerHandle(config, os.environ if environ is None else environ)
    return InProcessRunnerHandle(config)


def _runner_dir(config: HibernautConfig, configured: str) -> Path:
    if configured:
        return Path(configured)
    return Path(config.runtime.db_path).expanduser().resolve().parent / "runner"


def _runner_projects(config: HibernautConfig) -> dict[str, ProjectCheckout]:
    return {
        project.name: ProjectCheckout(
            project_key=project.name,
            checkout_path=Path(project.checkout).expanduser().resolve(),
            default_base_ref=f"origin/{project.delivery.base_branch}" if project.delivery is not None else "origin/main",
        )
        for project in config.projects.values()
    }


def _runner_drivers(config: HibernautConfig) -> dict[str, CLIDriver]:
    return {name: runner_driver(config.agents[name], config.executors[name]) for name in config.runner.executors}
