from __future__ import annotations

import asyncio
import logging
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from agent_runtime.adapters.delivery import DeliveryConfig, DeliveryRecord, merged_head
from agent_runtime.adapters.executor_cli.worktree import OrcaWorktreeManager
from agent_runtime.adapters.storage_sqlite.deliveries import SQLiteDeliveryStore
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore, WorkspaceRecord
from agent_runtime.core.ports import OutboxPort

# Five minutes: a worktree left behind by a delivery-time skip (a queued event, a reviewer finishing) is gone within
# minutes of it settling, while each round's GitHub lookups (one `gh pr list` per unmerged idle worktree, two for
# a merged one) stay far below the API rate limits.
DEFAULT_SWEEP_INTERVAL_SECONDS = 300.0

_log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SweepTarget:
    # The project a recorded worktree belongs to: its manager (backend, isolation), its delivery settings (branch
    # deletion) and the inbox told about a reclaim.
    manager: OrcaWorktreeManager
    delivery: DeliveryConfig | None
    inbox_id: str


@dataclass(frozen=True, slots=True)
class SweepOutcome:
    work_key: str
    workdir: str
    reclaimed: bool
    reason: str


def _stderr(line: str) -> None:
    print(line, file=sys.stderr, flush=True)


def _never() -> bool:
    return False


class WorktreeReclaimSweeper:
    # Background fallback for the reclaim a delivery attempts once: every task worktree Hibernaut recorded whose PR
    # is merged and that nothing uses any more is reclaimed through OrcaWorktreeManager.reclaim, with all its gates.
    # Only rows of the workspaces table in 'ready' state are looked at, and only those whose checkout belongs to a
    # configured project (target_for). A skip is logged once per worktree and reason, never sent to the Outbox; the
    # Outbox hears of a worktree only when the sweep finally reclaims it.
    def __init__(
        self,
        *,
        workspaces: SQLiteWorkspaceStore,
        deliveries: SQLiteDeliveryStore,
        outbox: OutboxPort,
        target_for: Callable[[WorkspaceRecord], SweepTarget | None],
        log: Callable[[str], None] = _stderr,
    ) -> None:
        self._workspaces = workspaces
        self._deliveries = deliveries
        self._outbox = outbox
        self._target_for = target_for
        self._log = log
        self._reported: dict[str, str] = {}

    def sweep_once(self, should_stop: Callable[[], bool] = _never) -> list[SweepOutcome]:
        outcomes: list[SweepOutcome] = []
        records = [r for r in self._workspaces.list_all() if r.status == "ready"]
        for record in records:
            # A stopping process starts no further removal; the one in progress finishes.
            if should_stop():
                break
            try:
                outcome = self._sweep(record)
            except Exception as exc:  # noqa: BLE001
                _log.exception("Worktree sweep failed for %s", record.work_key)
                outcome = SweepOutcome(record.work_key, record.workdir, False, f"sweep failed: {exc}")
            outcomes.append(outcome)
            self._report(outcome)
        live = {r.work_key for r in records}
        self._reported = {k: v for k, v in self._reported.items() if k in live}
        return outcomes

    async def run_forever(
        self,
        *,
        interval_seconds: float,
        stop_event: asyncio.Event,
        halt: asyncio.Event | None = None,
    ) -> None:
        # The first round runs at once (startup), then one per interval. halt is the process's stop signal: once it
        # is set no new round starts, while stop_event ends the services as usual.
        events = [e for e in (stop_event, halt) if e is not None]

        def stopped() -> bool:
            return any(e.is_set() for e in events)

        while not stopped():
            try:
                await asyncio.to_thread(self.sweep_once, stopped)
            except Exception:  # noqa: BLE001
                # A failed round (the database busy, say) must not take the other services down; the next one retries.
                _log.exception("Worktree sweep round failed")
            waits = [asyncio.ensure_future(e.wait()) for e in events]
            try:
                await asyncio.wait(waits, timeout=interval_seconds, return_when=asyncio.FIRST_COMPLETED)
            finally:
                for w in waits:
                    w.cancel()

    def _sweep(self, record: WorkspaceRecord) -> SweepOutcome:
        def skipped(reason: str) -> SweepOutcome:
            return SweepOutcome(record.work_key, record.workdir, False, reason)

        target = self._target_for(record)
        if target is None:
            return skipped(f"checkout {record.repo_checkout} belongs to no configured project")
        if not Path(record.workdir).name.startswith("hb-"):
            return skipped(f"Refused reclaim for '{record.workdir}': directory name lacks 'hb-' prefix")
        # Database-only checks first, so a busy worktree costs no git or GitHub call.
        in_use = target.manager.in_use(record)
        if in_use is not None:
            return skipped(in_use)
        merged = DeliveryRecord.from_dict(self._deliveries.get(record.work_key)).state == "merged"
        if not merged:
            at_head = target.manager.merged_at_head(record)
            if at_head is None:
                return skipped(f"could not tell whether the PR of '{record.branch}' is merged")
            if not at_head:
                return skipped(f"PR of '{record.branch}' not merged")
        head = merged_head(target.delivery, self._deliveries, record.work_key)
        result = target.manager.reclaim(record.work_key, merged_head=head)
        if not result.reclaimed:
            return skipped(result.reason)
        detail = f"; {result.branch}" if result.branch else ""
        self._outbox.enqueue(
            target.inbox_id,
            f"Reclaimed '{record.workdir}' in the background sweep: its PR is merged and nothing uses it{detail}",
            metadata={"kind": "reclaim_alert"},
        )
        return SweepOutcome(record.work_key, record.workdir, True, "reclaimed")

    def _report(self, outcome: SweepOutcome) -> None:
        if outcome.reclaimed:
            self._reported.pop(outcome.work_key, None)
            self._log(f"[hibernaut] [worktree-sweep] reclaimed {outcome.workdir} ({outcome.work_key})")
            return
        if self._reported.get(outcome.work_key) == outcome.reason:
            return
        self._reported[outcome.work_key] = outcome.reason
        self._log(f"[hibernaut] [worktree-sweep] kept {outcome.workdir} ({outcome.work_key}): {outcome.reason}")
