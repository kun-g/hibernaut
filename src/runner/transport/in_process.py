from __future__ import annotations

from dataclasses import replace

from runner.daemon import RunnerDaemon
from runner.protocol import (
    InterruptRequest,
    InterruptResult,
    ReclaimRequest,
    ReclaimResult,
    RunRequest,
    RunResult,
    RunnerCapabilities,
)


class InProcessRunnerClient:
    def __init__(
        self,
        daemon: RunnerDaemon,
        *,
        caller_id: str,
        bound_namespace: str,
    ) -> None:
        self._daemon = daemon
        self._caller_id = caller_id
        self._bound_namespace = bound_namespace

    async def capabilities(self) -> RunnerCapabilities:
        return await self._daemon.capabilities()

    async def run(self, req: RunRequest) -> RunResult:
        enforced = replace(req, namespace=self._bound_namespace)
        return await self._daemon.run(enforced, caller_id=self._caller_id)

    async def interrupt(self, req: InterruptRequest) -> InterruptResult:
        return await self._daemon.interrupt(req)

    async def reclaim_workspace(self, req: ReclaimRequest) -> ReclaimResult:
        enforced = replace(req, namespace=self._bound_namespace)
        return await self._daemon.reclaim_workspace(enforced, caller_id=self._caller_id)
