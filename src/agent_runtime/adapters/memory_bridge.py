from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Iterable, Mapping
from http import HTTPStatus
from typing import Any

from agent_runtime.adapters.ledger_bridge import RunLedger
from agent_runtime.adapters.memory_fs.hub import GovernedMemoryHub
from agent_runtime.adapters.memory_fs.tool_args import (
    flag,
    optional,
    read_document,
    required,
    search_query,
    write_proposal,
)
from agent_runtime.core.models import SessionStatus
from agent_runtime.core.ports import SessionStorePort
from memory_hub import AuthPrincipal, GCReport, MemoryHubHTTPServer, PostMortemCandidate, parse_memory_uri
from memory_hub.mcp_server import MCP_TOOLS
from memory_hub.partition import sanitize_segment

_log = logging.getLogger(__name__)

DEFAULT_GC_INTERVAL_SECONDS = 86_400.0
_MAX_RUNS = 500
_OPEN: tuple[SessionStatus, ...] = ("active", "suspended")
_FINISHED: tuple[SessionStatus, ...] = ("closed", "archived")
# What differs between two occurrences of one failure: ids, hashes, line numbers, counts.
_VARYING_RE = re.compile(r"\b(?=[0-9a-f]*\d)[0-9a-f]{6,}\b|\d+")

_OWN_TOOLS: list[dict[str, Any]] = [
    {
        "name": "memory_reject_proposal",
        "description": "Reject a pending memory proposal.",
        "inputSchema": {
            "type": "object",
            "properties": {"proposal_id": {"type": "string"}, "reason": {"type": "string"}},
            "required": ["proposal_id"],
        },
    },
    {
        "name": "memory_record_postmortem",
        "description": "Record the conclusion of reviewing a failed Run. Project, error kind and recurrence come from "
        "the run ledger; a failure seen twice (or force_propose) also opens an instructions.md proposal.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "root_cause": {"type": "string"},
                "proposed_rule": {"type": "string", "description": "At most three lines are kept."},
                "slug": {"type": "string"},
                "force_propose": {"type": "boolean"},
            },
            "required": ["run_id", "root_cause", "proposed_rule"],
        },
    },
]
_HUB_TOOLS = ("memory_search", "memory_read", "memory_propose_write", "memory_approve_proposal", "memory_list_proposals")


class MemoryControlPlane:
    # The memory_* tools of Hibernaut's MCP control plane. Its caller is an operator, so unlike a Run it may name any
    # project; invalid calls raise ValueError.
    def __init__(self, hub: GovernedMemoryHub, ledger: RunLedger | None) -> None:
        self._hub = hub
        self._ledger = ledger

    @staticmethod
    def definitions() -> list[dict[str, Any]]:
        return [t for t in MCP_TOOLS if t["name"] in _HUB_TOOLS] + _OWN_TOOLS

    @staticmethod
    def handles(name: str) -> bool:
        return name in _HUB_TOOLS or any(t["name"] == name for t in _OWN_TOOLS)

    def call(self, name: str, args: Mapping[str, Any], *, actor: str = "mcp") -> dict[str, Any]:
        try:
            return self._call(name, args, actor)
        except OSError as exc:
            raise ValueError(str(exc)) from exc

    def _call(self, name: str, args: Mapping[str, Any], actor: str) -> dict[str, Any]:
        if name == "memory_search":
            return self._hub.search(search_query(required(args, "scope"), args)).to_dict()
        if name == "memory_read":
            return read_document(self._hub, required(args, "uri"), args)
        if name == "memory_propose_write":
            return self._hub.propose_write(
                write_proposal(required(args, "target_uri"), args, author=actor)
            ).to_dict()
        if name == "memory_approve_proposal":
            return self._hub.approve_proposal(required(args, "proposal_id"), actor=actor).to_dict()
        if name == "memory_reject_proposal":
            return self._hub.reject_proposal(
                required(args, "proposal_id"), actor=actor, reason=optional(args, "reason") or ""
            ).to_dict()
        if name == "memory_list_proposals":
            return {"items": self._hub.list_proposals(status=optional(args, "status") or "pending")}
        if name == "memory_record_postmortem":
            return self._record_postmortem(args)
        raise ValueError(f"unknown memory tool {name}")

    def _record_postmortem(self, args: Mapping[str, Any]) -> dict[str, Any]:
        if self._ledger is None:
            raise ValueError("the run ledger is off, so there is no Run to review")
        run = self._ledger.store.get_run(required(args, "run_id"))
        if run is None:
            raise ValueError(f"Run not found: {args['run_id']}")
        if run.is_success:
            raise ValueError(f"Run {run.run_id} did not fail")
        project_key = self._hub.resolve_project_key(run.inbox_id)
        kind = run.error_kind or "unknown"
        recurrence = 1
        signature = _signature(run.error_summary)
        if signature and run.error_kind is not None:
            recurrence += sum(
                1
                for other in self._ledger.store.list_runs(error_kind=run.error_kind, limit=_MAX_RUNS)
                if other.run_id != run.run_id
                and not other.is_success
                and _signature(other.error_summary) == signature
                and self._hub.resolve_project_key(other.inbox_id) == project_key
            )
        result = self._hub.record_postmortem(PostMortemCandidate(
            project_key=project_key,
            run_id=run.run_id,
            error_kind=kind,
            root_cause=required(args, "root_cause"),
            proposed_rule=required(args, "proposed_rule"),
            recurrence_count=recurrence,
            correlation_key=run.correlation_key,
            slug=optional(args, "slug"),
            force_propose=flag(args, "force_propose"),
        ))
        return {**result.to_dict(), "recurrence_count": recurrence}


def _signature(summary: str | None) -> str:
    return _VARYING_RE.sub("#", " ".join((summary or "").lower().split()))


class MemoryGarbageCollector:
    def __init__(self, hub: GovernedMemoryHub, sessions: SessionStorePort | None) -> None:
        self._hub = hub
        self._sessions = sessions

    def collect_once(self) -> list[GCReport]:
        reports: list[GCReport] = []
        finished = self._finished_work()
        for key in self._hub.project_keys():
            try:
                reports.append(self._hub.run_mechanical_gc(key, closed_work_keys=finished.get(key, set())))
            except Exception:  # noqa: BLE001
                _log.exception("Memory GC failed for project %s", key)
        return reports

    async def run_forever(self, *, interval_seconds: float, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await asyncio.to_thread(self.collect_once)
            except Exception:  # noqa: BLE001
                _log.exception("Memory GC round failed")
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            except TimeoutError:
                pass

    def _finished_work(self) -> dict[str, set[str]]:
        # Per project, the names a progress file of finished work can have. Never None: MemoryHub then archives every
        # old progress file, open work included.
        if self._sessions is None:
            return {}
        finished = self._work_names(_FINISHED)
        for key, names in self._work_names(_OPEN).items():
            finished.get(key, set()).difference_update(names)
        return finished

    def _work_names(self, statuses: Iterable[SessionStatus]) -> dict[str, set[str]]:
        found: dict[str, set[str]] = {}
        assert self._sessions is not None
        for status in statuses:
            for session in self._sessions.list_sessions(status=status):
                try:
                    key = self._hub.resolve_project_key(session.inbox_id)
                except ValueError:
                    continue
                for work in (session.work_key, session.correlation_key):
                    if work:
                        found.setdefault(key, set()).update(_progress_names(work))
        return found


def _progress_names(work: str) -> set[str]:
    # A key such as kun-g/hibernaut#9 is no file name, so a progress file carries it with the separators replaced.
    names = {work, re.sub(r"[^A-Za-z0-9._#-]+", "__", work), re.sub(r"[^A-Za-z0-9._-]+", "-", work)}
    if "/" not in work and ".." not in work and "\\" not in work:
        names.add(sanitize_segment(work))
    return names


class ScopedMemoryHTTPServer(MemoryHubHTTPServer):
    # MemoryHubHTTPServer lists every project's proposals to any token and keeps an author the caller claims.
    def _dispatch_mcp_tool(
        self, principal: AuthPrincipal, name: str, args: dict[str, Any]
    ) -> tuple[int, dict[str, Any]]:
        if name == "memory_propose_write":
            args = {**args, "author": principal.subject}
        code, result = super()._dispatch_mcp_tool(principal, name, args)
        if name == "memory_list_proposals" and code == HTTPStatus.OK:
            result = {"items": [item for item in result["items"] if _visible(principal, item)]}
        return code, result


def _visible(principal: AuthPrincipal, record: Mapping[str, Any]) -> bool:
    parsed = parse_memory_uri(str(record["proposal"]["target_uri"]))
    return parsed.scheme != "proj" or principal.can_access_project(parsed.scope)
