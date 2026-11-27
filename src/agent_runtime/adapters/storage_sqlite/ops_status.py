from __future__ import annotations

import json
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, UTC
from typing import Any, cast

from agent_runtime.adapters.executor_cli.sde import WAIT_IDLE, suspended_stage
from agent_runtime.adapters.storage_sqlite.db import SQLiteDatabase
from agent_runtime.core.models import CONFIG_TIMER_PREFIX, TASK_CONTEXT_KEY, ConfiguredTimer, to_utc_iso

_MAX_ITEMS = 20
_TITLE = 80
_REASON = 200
_FAILED_ENDS = ("failed", "stopped", "refused")


@dataclass(frozen=True, slots=True)
class RunSummary:
    # How the Run that took an event ended, as the run ledger has it.
    result: str
    error: str
    seconds: float


def _clip(text: object, limit: int) -> str:
    if not isinstance(text, str):
        return ""
    line = next((part.strip() for part in text.splitlines() if part.strip()), "")
    return line if len(line) <= limit else line[: limit - 1] + "…"


def _json(raw: object) -> dict[str, Any]:
    try:
        found: object = json.loads(raw) if isinstance(raw, str) else None
    except ValueError:
        return {}
    return cast(dict[str, Any], found) if isinstance(found, dict) else {}


def _title(payload: dict[str, Any]) -> str:
    # Never the body: only what names the task.
    for key in ("title", "subject", "event_type"):
        found = _clip(payload.get(key), _TITLE)
        if found:
            return found
    return ""


def _task(metadata: dict[str, Any]) -> dict[str, Any]:
    context: object = metadata.get(TASK_CONTEXT_KEY)
    return cast(dict[str, Any], context) if isinstance(context, dict) else {}


class SQLiteOpsStatus:
    # How Hibernaut has been running, read from its own database: summaries only, never a payload or a message body.
    def __init__(
        self,
        db: SQLiteDatabase,
        *,
        timers: Callable[[], Sequence[ConfiguredTimer]],
        # The agents with the SDE role, whose results are deliveries.
        sde_agents: Callable[[], frozenset[str]],
        # Whether an inbox's project has a delivery step.
        delivers: Callable[[str], bool],
        # event_id -> the latest Run that took it; None without a run ledger.
        runs: Callable[[str], RunSummary | None] | None = None,
    ) -> None:
        self._db = db
        self._timers = timers
        self._sde_agents = sde_agents
        self._delivers = delivers
        self._runs = runs

    def report(self, *, hours: int, timer_runs: int, now: datetime | None = None) -> dict[str, Any]:
        end = now or datetime.now(UTC)
        since = to_utc_iso(end - timedelta(hours=hours))
        results = self._results(since)
        not_covered = [
            "Switchboard, Taskboard and other hosts: only what reached this Hibernaut is here",
            f"failed tasks and deliveries before {since}; suspended tasks and undelivered retries are listed "
            "whenever they began",
            "message bodies, event payloads and run logs are left out",
        ]
        if self._runs is None:
            not_covered.append("how each timer firing's Run ended: this process keeps no run ledger")
        return {
            "window": {"from": since, "to": to_utc_iso(end), "hours": hours},
            "failed": self._failed(since, results),
            "suspended": self._suspended(),
            "deliveries": self._deliveries(results),
            "undelivered_messages": self._undelivered(since),
            "timers": [self._timer(timer, timer_runs) for timer in self._timers()],
            "not_covered": not_covered,
        }

    def _results(self, since: str) -> list[dict[str, Any]]:
        # What Runs ended with, as the Outbox recorded it for the person: task_context carries the status.
        rows = self._db.fetchall(
            "SELECT inbox_id, created_at, metadata_json FROM outbox WHERE created_at >= ? ORDER BY seq", (since,)
        )
        found: list[dict[str, Any]] = []
        for row in rows:
            context = _task(_json(row["metadata_json"]))
            if context.get("status") in (None, "awaiting_approval"):
                continue
            found.append({"inbox": str(row["inbox_id"]), "at": str(row["created_at"]), **context})
        return found

    def _failed(self, since: str, results: list[dict[str, Any]]) -> dict[str, Any]:
        rows = self._db.fetchall(
            "SELECT inbox_id, source, occurred_at, payload_json, attempts, last_error FROM event_inbox "
            "WHERE status = 'failed' AND occurred_at >= ? ORDER BY seq DESC", (since,)
        )
        items: list[dict[str, Any]] = [
            {"inbox": str(row["inbox_id"]), "what": _title(_json(row["payload_json"])), "how": "run error",
             "source": str(row["source"]), "arrived": str(row["occurred_at"]), "attempts": int(row["attempts"]),
             "error": _clip(row["last_error"], _REASON)}
            for row in rows
        ]
        items += [
            {"inbox": r["inbox"], "what": _clip(r.get("title"), _TITLE), "how": f"ended {r['status']}",
             "agent": str(r.get("executor_id") or ""), "at": r["at"]}
            for r in reversed(results) if r.get("status") in _FAILED_ENDS
        ]
        return {"count": len(items), "items": items[:_MAX_ITEMS]}

    def _suspended(self) -> dict[str, Any]:
        rows = self._db.fetchall(
            "SELECT session_id, inbox_id, executor_id, checkpoint, pending_request_id, updated_at FROM sessions "
            "WHERE status = 'suspended' ORDER BY updated_at"
        )
        items = [self._waiting(row) for row in rows]
        return {"count": len(items), "items": items[:_MAX_ITEMS]}

    def _waiting(self, row: Any) -> dict[str, Any]:
        request = str(row["pending_request_id"] or "")
        item: dict[str, Any] = {"inbox": str(row["inbox_id"]), "session": str(row["session_id"]),
                                "agent": str(row["executor_id"] or ""), "since": str(row["updated_at"])}
        due = self._db.fetchone(
            "SELECT due_at FROM timers WHERE session_id = ? AND status = 'pending' "
            "AND json_extract(payload_json, '$.approval_request_id') = ?", (row["session_id"], request),
        ) if request else None
        sde = suspended_stage(row["checkpoint"])
        if sde is not None:
            stage, reason = sde
            item["stage"] = stage
            if stage == "needs_input":
                item["waits_for"] = "user_away" if reason.lstrip().startswith(WAIT_IDLE) else "answer"
            elif due is not None:
                item["waits_for"] = "retry"
            else:
                item["waits_for"] = "approval"
            item["why"] = _clip(reason, _REASON)
        else:
            item["waits_for"] = "retry" if due is not None else "approval"
            prompt = self._db.fetchone(
                "SELECT content FROM outbox WHERE inbox_id = ? AND json_extract(metadata_json, '$.request_id') = ? "
                "ORDER BY seq DESC LIMIT 1", (row["inbox_id"], request),
            ) if request else None
            item["why"] = _clip(prompt["content"], _REASON) if prompt is not None else ""
        if due is not None:
            item["retry_at"] = str(due["due_at"])
        return item

    def _deliveries(self, results: list[dict[str, Any]]) -> dict[str, Any]:
        agents = self._sde_agents()
        items: list[dict[str, Any]] = []
        for r in reversed(results):
            if r.get("executor_id") not in agents:
                continue
            delivery: object = r.get("delivery")
            item: dict[str, Any] = {"inbox": r["inbox"], "what": _clip(r.get("title"), _TITLE),
                                    "agent": r["executor_id"], "at": r["at"]}
            if r.get("status") in _FAILED_ENDS:
                item["result"] = "failed" if r["status"] == "failed" else str(r["status"])
            elif isinstance(delivery, dict):
                found = cast(dict[str, Any], delivery)
                item["result"] = str(found.get("state") or "local")
                if found.get("pr_url"):
                    item["pr"] = str(found["pr_url"])
                if found.get("hold"):
                    item["hold"] = f"{found['hold']}: {_clip(found.get('hold_detail'), _REASON)}"
            else:
                # With a delivery step, an SDE that changed something always reports where its delivery stands.
                item["result"] = "no_change" if self._delivers(r["inbox"]) else "no_delivery_step"
            items.append(item)
        counted = Counter(item["result"] for item in items)
        return {"by_result": dict(counted), "items": items[:_MAX_ITEMS]}

    def _undelivered(self, since: str) -> dict[str, Any]:
        rows = self._db.fetchall(
            "SELECT inbox_id, created_at, status, attempts, last_error, metadata_json, next_retry_at FROM outbox "
            "WHERE (status = 'failed' AND created_at >= ?) OR (status = 'pending' AND attempts > 0) ORDER BY seq DESC",
            (since,),
        )
        items: list[dict[str, Any]] = []
        for row in rows:
            metadata = _json(row["metadata_json"])
            item: dict[str, Any] = {
                "inbox": str(row["inbox_id"]), "what": _clip(_task(metadata).get("title"), _TITLE),
                "kind": str(metadata.get("kind") or ""), "created": str(row["created_at"]),
                "state": "failed" if row["status"] == "failed" else "retrying", "attempts": int(row["attempts"]),
                "error": _clip(row["last_error"], _REASON),
            }
            if row["status"] != "failed" and row["next_retry_at"]:
                item["next_retry_at"] = str(row["next_retry_at"])
            items.append(item)
        return {"count": len(items), "items": items[:_MAX_ITEMS]}

    def _timer(self, timer: ConfiguredTimer, limit: int) -> dict[str, Any]:
        due = self._db.fetchone("SELECT due_at FROM timers WHERE timer_id = ? AND status = 'pending'",
                                (CONFIG_TIMER_PREFIX + timer.name,))
        rows = self._db.fetchall(
            "SELECT event_id, occurred_at, status, last_error, payload_json FROM event_inbox "
            "WHERE inbox_id = ? AND source = ? AND status != 'dropped' ORDER BY seq DESC LIMIT ?",
            (timer.inbox_id, f"timer:{timer.name}", limit),
        )
        fired: list[dict[str, Any]] = []
        for row in rows:
            due_at: object = _json(row["payload_json"]).get("due_at")
            firing: dict[str, Any] = {"at": due_at if isinstance(due_at, str) else str(row["occurred_at"]),
                                      "status": str(row["status"])}
            run = self._runs(str(row["event_id"])) if self._runs is not None else None
            error = _clip(row["last_error"], _REASON)
            if run is not None:
                firing |= {"result": run.result, "seconds": run.seconds}
                error = error or _clip(run.error, _REASON)
            if error:
                firing["error"] = error
            fired.append(firing)
        return {"name": timer.name, "inbox": timer.inbox_id, "every_seconds": timer.interval_seconds,
                "enabled": timer.enabled, "next_due": str(due["due_at"]) if due is not None else None, "fired": fired}
