from __future__ import annotations

import json
import logging
import os
import re
import shutil
import sqlite3
import time
from collections.abc import Sequence
from pathlib import Path

from agent_runtime.adapters.storage_sqlite.db import SQLiteDatabase
from agent_runtime.core.models import OpenRun, new_id, utc_now_iso

_SAFE_RE = re.compile(r"[^A-Za-z0-9_.-]")
DEFAULT_ORPHAN_RETENTION_SECONDS = 7 * 24 * 3600.0
_log = logging.getLogger(__name__)


class SQLiteOpenRunStore:
    # OpenRunPort over the shared database, with one directory per Run under `root`
    # (<root>/<session_id>/<run_id>, mode 0700). Only paths inside root are ever deleted.
    def __init__(self, db: SQLiteDatabase, root: str | Path) -> None:
        self._db = db
        self._root = Path(os.path.realpath(Path(root).expanduser()))

    @property
    def root(self) -> Path:
        return self._root

    def open(
        self, *, session_id: str, inbox_id: str, executor_id: str, event_ids: Sequence[str], code_version: str
    ) -> OpenRun:
        stale = self._get(session_id)
        if stale is not None:
            self._remove_dir(stale.run_dir)
        run_dir = self._root / _SAFE_RE.sub("_", session_id) / new_id("run")
        self._root.mkdir(mode=0o700, parents=True, exist_ok=True)
        run_dir.mkdir(mode=0o700, parents=True)
        run = OpenRun(session_id=session_id, inbox_id=inbox_id, executor_id=executor_id, run_dir=str(run_dir),
                      event_ids=tuple(event_ids), started_at=utc_now_iso(), code_version=code_version)
        with self._db.transaction() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO open_runs "
                "(session_id, inbox_id, executor_id, run_dir, event_ids_json, started_at, code_version) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (run.session_id, run.inbox_id, run.executor_id, run.run_dir, json.dumps(list(run.event_ids)),
                 run.started_at, run.code_version),
            )
        return run

    def list_open(self, inbox_id: str) -> list[OpenRun]:
        rows = self._db.fetchall(
            f"SELECT {_COLUMNS} FROM open_runs WHERE inbox_id = ? ORDER BY started_at, session_id", (inbox_id,)
        )
        return [_row(r) for r in rows]

    def inbox_ids(self) -> list[str]:
        rows = self._db.fetchall("SELECT inbox_id FROM open_runs GROUP BY inbox_id ORDER BY MIN(started_at)")
        return [str(r["inbox_id"]) for r in rows]

    def add_events(self, session_id: str, event_ids: Sequence[str]) -> None:
        if not event_ids:
            return
        with self._db.transaction() as conn:
            row = conn.execute("SELECT event_ids_json FROM open_runs WHERE session_id = ?", (session_id,)).fetchone()
            if row is None:
                return
            ids: list[str] = list(json.loads(str(row["event_ids_json"])))
            ids += [e for e in event_ids if e not in ids]
            conn.execute("UPDATE open_runs SET event_ids_json = ? WHERE session_id = ?", (json.dumps(ids), session_id))

    def finish(self, session_id: str, done_event_ids: Sequence[str]) -> None:
        ids = list(done_event_ids)
        with self._db.transaction() as conn:
            row = conn.execute("SELECT run_dir FROM open_runs WHERE session_id = ?", (session_id,)).fetchone()
            chunk = max(1, conn.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER))
            for start in range(0, len(ids), chunk):
                part = ids[start : start + chunk]
                conn.execute(
                    f"UPDATE event_inbox SET status = 'done' WHERE event_id IN ({','.join('?' for _ in part)})",
                    tuple(part),
                )
            conn.execute("DELETE FROM open_runs WHERE session_id = ?", (session_id,))
        if row is not None:
            self._remove_dir(str(row["run_dir"]))

    def discard(self, session_id: str) -> None:
        run = self._get(session_id)
        if run is None:
            return
        with self._db.transaction() as conn:
            conn.execute("DELETE FROM open_runs WHERE session_id = ?", (session_id,))
        self._remove_dir(run.run_dir)

    def sweep(self, retention_seconds: float = DEFAULT_ORPHAN_RETENTION_SECONDS) -> list[str]:
        # Run directories no registration names, left by a crash; younger ones stay for a person to inspect.
        if not self._root.is_dir():
            return []
        kept = {os.path.realpath(str(r["run_dir"])) for r in self._db.fetchall("SELECT run_dir FROM open_runs")}
        cutoff = time.time() - retention_seconds
        removed: list[str] = []
        for session_dir in list(self._root.iterdir()):
            if not session_dir.is_dir() or session_dir.is_symlink():
                continue
            for run_dir in list(session_dir.iterdir()):
                path = os.path.realpath(run_dir)
                try:
                    old = run_dir.stat().st_mtime < cutoff
                except OSError:
                    continue
                if path not in kept and old and self._remove_dir(path):
                    removed.append(path)
            try:
                session_dir.rmdir()
            except OSError:
                pass
        return removed

    def _get(self, session_id: str) -> OpenRun | None:
        row = self._db.fetchone(f"SELECT {_COLUMNS} FROM open_runs WHERE session_id = ?", (session_id,))
        return _row(row) if row is not None else None

    def _remove_dir(self, run_dir: str) -> bool:
        path = Path(os.path.realpath(run_dir))
        if self._root not in path.parents:
            _log.warning("Not deleting run directory %s outside %s", run_dir, self._root)
            return False
        shutil.rmtree(path, ignore_errors=True)
        try:
            path.parent.rmdir()
        except OSError:
            pass
        return True


_COLUMNS = "session_id, inbox_id, executor_id, run_dir, event_ids_json, started_at, code_version"


def _row(row: sqlite3.Row) -> OpenRun:
    return OpenRun(
        session_id=str(row["session_id"]), inbox_id=str(row["inbox_id"]), executor_id=str(row["executor_id"]),
        run_dir=str(row["run_dir"]), event_ids=tuple(str(e) for e in json.loads(str(row["event_ids_json"]))),
        started_at=str(row["started_at"]), code_version=str(row["code_version"] or ""),
    )
