from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

from runner.protocol import RunRequest, RunnerCapabilities, utc_now_iso


class NodeAffinityOfflineError(RuntimeError):
    def __init__(self, runner_id: str, work_key: str) -> None:
        super().__init__(f"affinity runner {runner_id!r} for work_key {work_key!r} is offline")
        self.runner_id = runner_id
        self.work_key = work_key


class NoEligibleRunnerError(RuntimeError):
    pass


class WorkspaceAffinityStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self._db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS workspace_affinity (
                namespace TEXT NOT NULL,
                project_key TEXT NOT NULL,
                owner_scope TEXT NOT NULL,
                work_key TEXT NOT NULL,
                runner_id TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (namespace, project_key, owner_scope, work_key)
            )
            """
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_affinity_runner ON workspace_affinity(runner_id)"
        )
        self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def bind(
        self,
        *,
        namespace: str,
        project_key: str,
        owner_scope: str,
        work_key: str,
        runner_id: str,
    ) -> None:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            self._conn.execute(
                """
                INSERT INTO workspace_affinity (namespace, project_key, owner_scope, work_key, runner_id, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(namespace, project_key, owner_scope, work_key)
                DO UPDATE SET runner_id = excluded.runner_id, updated_at = excluded.updated_at
                """,
                (namespace, project_key, owner_scope, work_key, runner_id, utc_now_iso()),
            )
            self._conn.commit()

    def lookup(
        self,
        *,
        namespace: str,
        project_key: str,
        owner_scope: str,
        work_key: str,
    ) -> str | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT runner_id FROM workspace_affinity
                WHERE namespace = ? AND project_key = ? AND owner_scope = ? AND work_key = ?
                """,
                (namespace, project_key, owner_scope, work_key),
            ).fetchone()
        return str(row[0]) if row is not None else None

    def unbind(
        self,
        *,
        namespace: str,
        project_key: str,
        owner_scope: str,
        work_key: str,
    ) -> None:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            self._conn.execute(
                """
                DELETE FROM workspace_affinity
                WHERE namespace = ? AND project_key = ? AND owner_scope = ? AND work_key = ?
                """,
                (namespace, project_key, owner_scope, work_key),
            )
            self._conn.commit()

    def reconcile_runner(
        self,
        runner_id: str,
        ready_work_keys: Sequence[Mapping[str, str]],
    ) -> None:
        reported: set[tuple[str, str, str, str]] = {
            (
                str(item["namespace"]),
                str(item["project_key"]),
                str(item["owner_scope"]),
                str(item["work_key"]),
            )
            for item in ready_work_keys
        }
        now = utc_now_iso()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            existing_rows = self._conn.execute(
                "SELECT namespace, project_key, owner_scope, work_key FROM workspace_affinity WHERE runner_id = ?",
                (runner_id,),
            ).fetchall()
            existing = {(r[0], r[1], r[2], r[3]) for r in existing_rows}
            for stale in existing - reported:
                self._conn.execute(
                    """
                    DELETE FROM workspace_affinity
                    WHERE namespace = ? AND project_key = ? AND owner_scope = ? AND work_key = ? AND runner_id = ?
                    """,
                    (*stale, runner_id),
                )
            for key in reported:
                self._conn.execute(
                    """
                    INSERT INTO workspace_affinity (namespace, project_key, owner_scope, work_key, runner_id, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(namespace, project_key, owner_scope, work_key)
                    DO UPDATE SET runner_id = excluded.runner_id, updated_at = excluded.updated_at
                    """,
                    (*key, runner_id, now),
                )
            self._conn.commit()


@dataclass(slots=True)
class NodeRecord:
    runner_id: str
    conn_epoch: int
    capabilities: RunnerCapabilities
    reserved_slots: int = 0
    last_seen_mono: float = 0.0
    online: bool = True

    @property
    def effective_available(self) -> int:
        used = max(self.capabilities.slots.active_runs, self.reserved_slots)
        return max(0, self.capabilities.slots.max_concurrent - used)


class NodeRegistry:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._nodes: dict[str, NodeRecord] = {}
        self._epochs: dict[str, int] = {}

    def next_epoch(self, runner_id: str) -> int:
        with self._lock:
            nxt = self._epochs.get(runner_id, 0) + 1
            self._epochs[runner_id] = nxt
            return nxt

    def register(self, runner_id: str, conn_epoch: int, caps: RunnerCapabilities) -> None:
        with self._lock:
            self._nodes[runner_id] = NodeRecord(
                runner_id=runner_id,
                conn_epoch=conn_epoch,
                capabilities=caps,
                reserved_slots=caps.slots.active_runs,
                last_seen_mono=time.monotonic(),
                online=True,
            )

    def calibrate_heartbeat(self, runner_id: str, conn_epoch: int, caps: RunnerCapabilities) -> bool:
        with self._lock:
            rec = self._nodes.get(runner_id)
            if rec is None or rec.conn_epoch != conn_epoch:
                return False
            rec.capabilities = caps
            rec.reserved_slots = caps.slots.active_runs
            rec.last_seen_mono = time.monotonic()
            rec.online = True
            return True

    def mark_offline(self, runner_id: str, conn_epoch: int) -> bool:
        with self._lock:
            rec = self._nodes.get(runner_id)
            if rec is None or rec.conn_epoch != conn_epoch:
                return False
            rec.online = False
            rec.reserved_slots = 0
            return True

    def sweep_stale(self, *, stale_after_seconds: float) -> list[str]:
        now = time.monotonic()
        offlined: list[str] = []
        with self._lock:
            for rec in self._nodes.values():
                if rec.online and (now - rec.last_seen_mono) >= stale_after_seconds:
                    rec.online = False
                    rec.reserved_slots = 0
                    offlined.append(rec.runner_id)
        return offlined

    def get(self, runner_id: str) -> NodeRecord | None:
        with self._lock:
            return self._nodes.get(runner_id)

    def list_online(self) -> list[NodeRecord]:
        with self._lock:
            return [r for r in self._nodes.values() if r.online]

    def reserve_slot(self, runner_id: str) -> bool:
        with self._lock:
            rec = self._nodes.get(runner_id)
            if rec is None or not rec.online:
                return False
            if rec.effective_available <= 0:
                return False
            rec.reserved_slots += 1
            return True

    def release_slot(self, runner_id: str) -> None:
        with self._lock:
            rec = self._nodes.get(runner_id)
            if rec is not None and rec.reserved_slots > 0:
                rec.reserved_slots -= 1


class RunnerRouter:
    def __init__(self, registry: NodeRegistry, affinity: WorkspaceAffinityStore) -> None:
        self._registry = registry
        self._affinity = affinity

    def select_node(self, req: RunRequest, *, exclude: set[str] | None = None) -> str:
        excluded = exclude or set()
        bound = self._affinity.lookup(
            namespace=req.namespace,
            project_key=req.project_key,
            owner_scope=req.owner_scope,
            work_key=req.work_key,
        )
        if bound is not None:
            rec = self._registry.get(bound)
            if rec is not None and rec.online and bound not in excluded and self._matches(rec, req):
                if rec.effective_available > 0:
                    return bound
                raise NoEligibleRunnerError(f"affinity runner {bound!r} has no available slots")
            # 2A: Write tasks (and local-dependent readonly tasks) strictly wait for their affinity node.
            if not (req.readonly and req.remote_readonly_ok):
                raise NodeAffinityOfflineError(bound, req.work_key)

        candidates = [
            rec
            for rec in self._registry.list_online()
            if rec.runner_id not in excluded and self._matches(rec, req) and rec.effective_available > 0
        ]
        if not candidates:
            raise NoEligibleRunnerError(
                f"no online runner matches project={req.project_key!r} executor={req.executor_id!r} interactive={req.interactive}"
            )
        candidates.sort(key=lambda r: (-r.effective_available, r.runner_id))
        return candidates[0].runner_id

    @staticmethod
    def _matches(rec: NodeRecord, req: RunRequest) -> bool:
        caps = rec.capabilities
        if req.namespace not in caps.namespaces:
            return False
        if req.project_key not in caps.projects:
            return False
        if not any(e.executor_id == req.executor_id for e in caps.executors):
            return False
        if req.interactive and caps.interactive_surface != "orca":
            return False
        return True
