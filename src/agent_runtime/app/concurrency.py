from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from agent_runtime.app.projects import AmbiguousProjectMatch
from agent_runtime.core.models import Event, InboxBinding
from agent_runtime.core.ports import InboxPort, OpenRunPort, SessionStorePort
from agent_runtime.engine import AgentWorker

_log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ConcurrencyLimits:
    projects: Mapping[str, int] = field(default_factory=dict[str, int])
    executors: Mapping[str, int] = field(default_factory=dict[str, int])
    agents: Mapping[str, int] = field(default_factory=dict[str, int])


@dataclass(frozen=True, slots=True)
class LimitKeys:
    project: str | None = None
    # The driver the agent runs on; a legacy executor is its own agent and driver.
    executor: str | None = None
    agent: str | None = None


class ConcurrencyLimiter:
    # Used only on the pool's event loop: check and take happen without an await in between, so they are atomic.
    # Shared across config-reload generations so retired Runs keep counting against the current limits until they finish.
    def __init__(self, max_concurrency: int, limits: ConcurrencyLimits | None = None) -> None:
        self._max_concurrency = max_concurrency
        self._limits = limits or ConcurrencyLimits()
        self._held: dict[str, LimitKeys] = {}
        self._projects: Counter[str] = Counter()
        self._executors: Counter[str] = Counter()
        self._agents: Counter[str] = Counter()
        self._listeners: list[Callable[[], None]] = []

    def subscribe(self, listener: Callable[[], None]) -> Callable[[], None]:
        self._listeners.append(listener)

        def _unsubscribe() -> None:
            try:
                self._listeners.remove(listener)
            except ValueError:
                pass

        return _unsubscribe

    def _notify(self) -> None:
        for listener in tuple(self._listeners):
            listener()

    def reconfigure(self, max_concurrency: int, limits: ConcurrencyLimits | None = None) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        self._max_concurrency = max_concurrency
        self._limits = limits or ConcurrencyLimits()
        self._notify()

    @property
    def active(self) -> int:
        return len(self._held)

    def saturated(self) -> bool:
        return len(self._held) >= self._max_concurrency

    def held(self, inbox_id: str) -> LimitKeys | None:
        return self._held.get(inbox_id)

    def covers(self, held: LimitKeys, needed: LimitKeys) -> bool:
        # Whether work of the needed agent may run in the slot held for another event of the inbox.
        return (
            (needed.executor is None or needed.executor == held.executor
             or needed.executor not in self._limits.executors)
            and (needed.agent is None or needed.agent == held.agent or needed.agent not in self._limits.agents)
        )

    def available(
        self, inbox_id: str, *, project_key: str | None, executor_key: str | None, agent_key: str | None = None,
    ) -> bool:
        if inbox_id in self._held or self.saturated():
            return False
        return all(
            key is None or used[key] < limits.get(key, self._max_concurrency)
            for key, used, limits in (
                (project_key, self._projects, self._limits.projects),
                (executor_key, self._executors, self._limits.executors),
                (agent_key, self._agents, self._limits.agents),
            )
        )

    def try_acquire(
        self, inbox_id: str, *, project_key: str | None, executor_key: str | None, agent_key: str | None = None,
    ) -> bool:
        if not self.available(inbox_id, project_key=project_key, executor_key=executor_key, agent_key=agent_key):
            return False
        self._held[inbox_id] = LimitKeys(project_key, executor_key, agent_key)
        if project_key is not None:
            self._projects[project_key] += 1
        if executor_key is not None:
            self._executors[executor_key] += 1
        if agent_key is not None:
            self._agents[agent_key] += 1
        return True

    def release(self, inbox_id: str) -> None:
        keys = self._held.pop(inbox_id, None)
        if keys is None:
            return
        if keys.project is not None:
            self._projects[keys.project] -= 1
            if self._projects[keys.project] <= 0:
                del self._projects[keys.project]
        if keys.executor is not None:
            self._executors[keys.executor] -= 1
            if self._executors[keys.executor] <= 0:
                del self._executors[keys.executor]
        if keys.agent is not None:
            self._agents[keys.agent] -= 1
            if self._agents[keys.agent] <= 0:
                del self._agents[keys.agent]
        self._notify()


@dataclass(frozen=True, slots=True)
class LimitRoute:
    project: str | None = None
    default_executor: str | None = None
    binding: InboxBinding | None = None
    # The worker's hold policy for this inbox, e.g. UnmatchedProjectHold; held events never run, so they need no quota.
    hold: Callable[[Event], str | None] | None = None


class LimitKeyResolver:
    def __init__(
        self,
        *,
        limits: ConcurrencyLimits,
        inbox: InboxPort,
        sessions: SessionStorePort,
        open_runs: OpenRunPort | None,
        route: Callable[[str], LimitRoute],
        global_default_executor: str | None,
        # Agent id -> the [executors.<id>] it runs on; an id not listed is its own executor.
        drivers: Mapping[str, str] | None = None,
    ) -> None:
        self._limits = limits
        self._inbox = inbox
        self._sessions = sessions
        self._open_runs = open_runs
        self._route = route
        self._global_default = global_default_executor
        self._drivers = drivers or {}

    def __call__(self, inbox_id: str) -> LimitKeys:
        try:
            route = self._route(inbox_id)
        except AmbiguousProjectMatch:
            # The worker factory raises it again and the pool reports it there.
            return LimitKeys()
        if not self._limits.executors and not self._limits.agents:
            return LimitKeys(route.project)
        try:
            return self._next_keys(inbox_id, route)
        except Exception:  # noqa: BLE001
            _log.warning("Next executor of inbox %s cannot be determined; its executor limit is not applied",
                         inbox_id, exc_info=True)
            return LimitKeys(route.project)

    def event_keys(self, event: Event, route: LimitRoute) -> LimitKeys:
        try:
            return self._agent_keys(route, AgentWorker.expected_executor_id(
                event, self._sessions, binding=route.binding, project_default=route.default_executor,
                global_default=self._global_default,
            ))
        except Exception:  # noqa: BLE001
            _log.warning("Executor of event %s in inbox %s cannot be determined; its executor limit is not applied",
                         event.event_id, event.inbox_id, exc_info=True)
            return LimitKeys(route.project)

    def _agent_keys(self, route: LimitRoute, agent_id: str | None) -> LimitKeys:
        if agent_id is None:
            return LimitKeys(route.project)
        return LimitKeys(route.project, self._drivers.get(agent_id, agent_id), agent_id)

    def _next_keys(self, inbox_id: str, route: LimitRoute) -> LimitKeys:
        # What the worker runs first: a Run a stopped process detached, else the first event it may claim.
        for run in self._open_runs.list_open(inbox_id) if self._open_runs is not None else ():
            return self._agent_keys(route, run.executor_id)
        for event in self._inbox.fetch_pending(inbox_id):
            if route.hold is None or route.hold(event) is None:
                return self.event_keys(event, route)
        return LimitKeys(route.project)


class ExecutorQuotaHold:
    # One per worker. An inbox holds one agent's and driver's slot per dispatch but may have batches for several, some
    # arriving mid-run. From the first event that needs another limited slot on, nothing more is claimed in this
    # dispatch, so inbox order is kept; on_defer makes the pool queue the inbox again under that event's keys.
    # Only that first event is checked again: later ones would overtake it.
    def __init__(
        self,
        inbox_id: str,
        *,
        limiter: ConcurrencyLimiter,
        keys_of: Callable[[Event], LimitKeys],
        on_defer: Callable[[str], None],
        inner: Callable[[Event], str | None] | None = None,
    ) -> None:
        self._inbox_id = inbox_id
        self._limiter = limiter
        self._keys_of = keys_of
        self._on_defer = on_defer
        self._inner = inner
        self._admitted: set[str] = set()
        self._blocking: str | None = None

    def __call__(self, event: Event) -> str | None:
        reason = self._inner(event) if self._inner is not None else None
        if reason is not None:
            return reason
        # Admitted events are checked again: a replay can put one back to pending for another executor.
        if (self._blocking in (None, event.event_id) or event.event_id in self._admitted) and self._covered(event):
            if self._blocking == event.event_id:
                self._blocking = None
            self._admitted.add(event.event_id)
            return None
        if self._blocking is None:
            self._blocking = event.event_id
            self._on_defer(self._inbox_id)
        return "waiting for an executor slot"

    def _covered(self, event: Event) -> bool:
        held = self._limiter.held(self._inbox_id)
        if held is None:
            # A worker used outside the pool has no slot to compare with.
            return True
        return self._limiter.covers(held, self._keys_of(event))
