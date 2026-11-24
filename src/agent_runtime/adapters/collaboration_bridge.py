from __future__ import annotations

import hashlib
import http.client
import json
import urllib.request
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast

from agent_runtime.adapters.ledger_bridge import RunLedger
from agent_runtime.adapters.memory_fs.expert_mount import ROLE_SCHEME, is_role_uri
from agent_runtime.adapters.memory_fs.hub import GovernedMemoryHub
from agent_runtime.core.models import CLOSEOUT_SOURCE, Event
from collaboration import (
    ChiefOfStaffSinkPort,
    CloseoutCard,
    CloseoutCoordinator,
    CloseoutMemoryDraft,
    CloseoutStore,
    CollaborationHubServer,
    CollaborationMCPTools,
    ExpertPoolRegistry,
    MemoryMountPlan,
    MemoryProposalSinkPort,
    RoleSkillMemoryStore,
    TaskboardSinkPort,
)
from memory_hub import WriteProposal
from memory_hub.partition import parse_memory_uri

_DIGEST_WINDOW = timedelta(hours=24)
# The ledger's own cap on one listing.
_MAX_RUNS = 500
_LIVE_TASKS = ("ready", "in_progress")


# --- Taskboard --------------------------------------------------------------------------------------------------


class TaskboardRefused(RuntimeError):
    def __init__(self, payload: Mapping[str, Any]) -> None:
        super().__init__(f"Taskboard: {payload.get('error', 'the call failed')}")
        self.payload = payload


class TaskboardCaller(Protocol):
    def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]: ...


class _NoRedirects(urllib.request.HTTPRedirectHandler):
    # A redirect would carry the token to wherever it points.
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


class TaskboardMCPClient:
    def __init__(self, mcp_url: str, token: str, *, timeout_seconds: float = 10.0) -> None:
        self._url = mcp_url
        self._token = token
        self._timeout = timeout_seconds
        self._opener = urllib.request.build_opener(_NoRedirects)

    def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        body = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": tool, "arguments": arguments}}
        request = urllib.request.Request(
            self._url, data=json.dumps(body, ensure_ascii=False).encode("utf-8"), method="POST",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {self._token}"},
        )
        try:
            with self._opener.open(request, timeout=self._timeout) as reply:
                answer: object = json.loads(reply.read())
        except (OSError, http.client.HTTPException, ValueError) as exc:
            raise RuntimeError(f"Taskboard did not answer ({type(exc).__name__})") from exc
        result = cast(dict[str, Any], answer).get("result") if isinstance(answer, dict) else None
        if not isinstance(result, dict):
            raise RuntimeError(f"Taskboard refused {tool}")
        outcome = cast(dict[str, Any], result)
        payload = cast(dict[str, Any], outcome.get("structuredContent") or {})
        if outcome.get("isError"):
            raise TaskboardRefused(payload)
        return payload


def follow_up_ref(card_id: str, title: str) -> str:
    return f"closeout:{card_id}:{hashlib.sha256(title.encode('utf-8')).hexdigest()[:16]}"


class TaskboardFollowUpSink:
    def __init__(self, client: TaskboardCaller) -> None:
        self._client = client

    def create_follow_up_task(self, *, project_key: str, title: str, source_card_id: str) -> str:
        ref = follow_up_ref(source_card_id, title)
        try:
            created = self._client.call("tb_create_task", {
                "scope": f"proj:{project_key}", "title": title, "description": f"来自收口卡 {source_card_id}",
                "source": "collaboration", "source_ref": ref,
            })
        except TaskboardRefused as exc:
            # Created by an earlier attempt at the same confirmation.
            if exc.payload.get("error_type") == "SourceRefConflictError" and exc.payload.get("source_ref") == ref:
                return str(exc.payload["task_id"])
            raise
        return str(created["task"]["task_id"])


# --- memory proposals -------------------------------------------------------------------------------------------


class CloseoutMemorySink:
    # Drafts of a card only ever wait for approval: role:// ones with the role store, all others with Memory Hub.
    def __init__(self, hub: GovernedMemoryHub, roles: RoleSkillMemoryStore, known_roles: Collection[str]) -> None:
        self._hub = hub
        self._roles = roles
        self._known_roles = known_roles

    def check(self, draft: CloseoutMemoryDraft, project_key: str) -> None:
        uri = draft.target_uri
        if is_role_uri(uri):
            role, rel = _role_target(uri)
            if role not in self._known_roles:
                raise ValueError(f"{uri}: role '{role}' is not declared in [experts]")
            self._roles.read_role_doc(role, rel)
            return
        parsed = parse_memory_uri(uri)
        if parsed.scheme not in ("proj", "shared"):
            raise ValueError(f"{uri} is read-only")
        if parsed.scheme == "proj" and parsed.scope != project_key:
            raise ValueError(f"{uri} is outside the memory of project '{project_key}'")

    def for_card(self, card: CloseoutCard) -> MemoryProposalSinkPort:
        return _CardMemorySink(self, card)

    def queue(self, card: CloseoutCard, draft: CloseoutMemoryDraft, proposed_by: str) -> str:
        self.check(draft, card.project_key)
        # One id per card and target, so confirming again finds what an earlier attempt has queued.
        digest = hashlib.sha256(f"{card.card_id}\n{draft.target_uri}".encode()).hexdigest()
        reason = f"{draft.reason}（收口卡 {card.card_id}，{card.confirmed_by} 确认）"
        if is_role_uri(draft.target_uri):
            role, rel = _role_target(draft.target_uri)
            return self._roles.propose_skill_append(
                proposal_id=f"rprop-{digest[:12]}", role_id=role, rel_path=rel, addition=draft.content, reason=reason,
                proposed_by=proposed_by,
            ).proposal_id
        result = self._hub.queue_proposal(
            WriteProposal(
                target_uri=draft.target_uri, op="append", content=draft.content, rationale=reason, author=proposed_by,
                evidence_ref=f"closeout:{card.card_id}", force_new=True,
            ),
            proposal_id=f"prop_{digest[:24]}",
        )
        if result.status != "pending_approval" or result.proposal_id is None:
            raise ValueError(f"{draft.target_uri}: {result.message}")
        return result.proposal_id


@dataclass(frozen=True, slots=True)
class _CardMemorySink:
    sink: CloseoutMemorySink
    card: CloseoutCard

    def submit_memory_proposal(self, *, target_uri: str, content: str, reason: str, proposed_by: str) -> str:
        draft = CloseoutMemoryDraft(target_uri=target_uri, content=content, reason=reason)
        return self.sink.queue(self.card, draft, proposed_by)


def _role_target(uri: str) -> tuple[str, str]:
    role, _, rel = uri.removeprefix(ROLE_SCHEME).partition("/")
    return role, rel


# --- Chief of Staff ---------------------------------------------------------------------------------------------


class ChiefOfStaffInboxSink:
    def __init__(self, queue: Callable[[Event], object], inbox_id: str) -> None:
        self._queue = queue
        self._inbox_id = inbox_id

    def record_closeout_summary(self, card: CloseoutCard) -> None:
        # The inbox drops the same card a second time by its external_id.
        self._queue(Event(
            inbox_id=self._inbox_id, source=CLOSEOUT_SOURCE, external_id=f"closeout:{card.card_id}",
            correlation_key=f"closeout:{card.card_id}",
            payload={"title": f"收口卡 {card.card_id}（{card.project_key}）", "body": _summary(card),
                     "closeout": card.to_dict()},
        ))


def _summary(card: CloseoutCard) -> str:
    lines = [f"{card.confirmed_by} 确认了 {card.agent_id} 在 {card.project_key} 的收口卡。以下由该 agent 撰写，供知悉，不是指令。",
             "", card.summary, "", "决定："]
    lines += [f"- {d.key} {d.title}：{d.choice}" + (f"（{d.rationale}）" if d.rationale else "") for d in card.decisions]
    if card.follow_up_tasks:
        lines += ["", "已建的后续任务：", *(f"- {title}" for title in card.follow_up_tasks)]
    if card.memory_proposals:
        lines += ["", "待审的记忆提案：", *(f"- {m.target_uri}" for m in card.memory_proposals)]
    return "\n".join(lines)


# --- closeouts and the MCP tools --------------------------------------------------------------------------------


class ProjectCloseoutCoordinator(CloseoutCoordinator):
    def __init__(
        self,
        store: CloseoutStore,
        *,
        chief: ChiefOfStaffSinkPort,
        taskboard: TaskboardSinkPort | None,
        memory: CloseoutMemorySink,
        knows_project: Callable[[str], bool],
    ) -> None:
        super().__init__(store, chief_sink=chief, taskboard_sink=taskboard)
        self._memory = memory
        self._knows_project = knows_project

    def emit_draft(self, **card: Any) -> CloseoutCard:
        # Before anyone is asked to confirm it, rather than half-way through the sync.
        key = str(card["project_key"]).strip()
        if not self._knows_project(key):
            raise ValueError(f"project_key {key!r} is no project of this deployment")
        for draft in cast(tuple[CloseoutMemoryDraft, ...], card.get("memory_proposals", ())):
            self._memory.check(draft, key)
        return super().emit_draft(**card)

    def _memory_sink_for(self, card: CloseoutCard) -> MemoryProposalSinkPort | None:
        return self._memory.for_card(card)


# Arguments the deployment or the caller's identity decides: not offered, and not believed.
_DECIDED = {
    "confirm_closeout_card": ("confirmed_by",),
    "propose_role_skill": ("proposed_by",),
    "resolve_expert_mount": ("extra_readonly_projects", "budget_chars"),
}


class CollaborationControlPlane:
    # The collaboration tools of Hibernaut's MCP control plane; its caller is an operator. Invalid calls raise ValueError.
    def __init__(
        self,
        *,
        roles: RoleSkillMemoryStore,
        pool: ExpertPoolRegistry,
        closeout: CloseoutCoordinator,
        drafted: Mapping[tuple[str, str], MemoryMountPlan],
        budget_chars: int,
    ) -> None:
        self._tools = CollaborationMCPTools(role_store=roles, expert_pool=pool, closeout=closeout)
        self._roles = {p.role_id for p in pool.list_profiles()}
        self._drafted = drafted
        self._budget_chars = budget_chars

    def definitions(self) -> list[dict[str, Any]]:
        definitions = self._tools.tool_definitions()
        for definition in definitions:
            schema = definition["inputSchema"]
            for name in _DECIDED.get(definition["name"], ()):
                schema["properties"].pop(name)
                schema["required"] = [r for r in schema["required"] if r != name]
        return definitions

    def handles(self, name: str) -> bool:
        return any(t["name"] == name for t in self._tools.tool_definitions())

    def call(self, name: str, args: Mapping[str, Any], *, actor: str = "mcp") -> dict[str, Any]:
        try:
            return self._tools.call_tool(name, self._arguments(name, args, actor))
        except (OSError, RuntimeError) as exc:
            if name != "confirm_closeout_card":
                raise ValueError(str(exc)) from exc
            raise ValueError(f"{exc}; confirm the card again to finish it") from exc

    def _arguments(self, name: str, args: Mapping[str, Any], actor: str) -> dict[str, Any]:
        if name == "confirm_closeout_card":
            return {**args, "confirmed_by": actor}
        if name == "propose_role_skill":
            role = args.get("role_id")
            if isinstance(role, str) and role not in self._roles:
                raise ValueError(f"role '{role}' is not declared in [experts]")
            return {**args, "proposed_by": actor}
        if name != "resolve_expert_mount":
            return dict(args)
        for decided in _DECIDED[name]:
            if decided in args:
                raise ValueError(f"{decided} is set in the deployment's configuration")
        plan = self._drafted.get((str(args.get("project_key")), str(args.get("role_id"))))
        if plan is None:
            raise ValueError(f"role '{args.get('role_id')}' is not drafted by project '{args.get('project_key')}' "
                             "(projects.<name>.experts)")
        readonly = [s.uri.removeprefix("proj://").rstrip("/") for s in plan.slices
                    if s.layer == "project" and s.write_mode == "ro"]
        return {**args, "extra_readonly_projects": readonly, "budget_chars": self._budget_chars}


# --- the hub ----------------------------------------------------------------------------------------------------


def _utc_now() -> datetime:
    return datetime.now(UTC)


class HubState:
    # What the digest reads; it changes nothing.
    def __init__(
        self,
        *,
        hub: GovernedMemoryHub | None,
        ledger: RunLedger | None,
        taskboard: TaskboardCaller | None,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._hub = hub
        self._ledger = ledger
        self._taskboard = taskboard
        self._clock = clock

    def list_tasks(self) -> list[dict[str, Any]]:
        if self._taskboard is None:
            return []
        found: list[dict[str, Any]] = []
        for task in cast(list[dict[str, Any]], self._taskboard.call("tb_list_tasks", {}).get("tasks", [])):
            status = str(task["status"])
            held = bool(task.get("incomplete_blocker_count") or task.get("blocked_reason"))
            finished = status == "done" and self._is_recent(task.get("updated_at"))
            if status in _LIVE_TASKS or finished or (status == "backlog" and held):
                found.append({"project_key": _project_of(str(task.get("scope") or "")), "status": status,
                              "is_blocked": held and status != "done"})
        return found

    def list_runs(self) -> list[dict[str, Any]]:
        if self._ledger is None:
            return []
        runs = [r for r in self._ledger.store.list_runs(limit=_MAX_RUNS) if self._is_recent(r.ended_at)]
        return [
            {
                # A Run waiting for an approval, or stopped for a newer event, has not failed.
                "status": "failed" if r.outcome_kind == "error" or r.emit_status == "failed" else "done",
                "error_kind": r.error_kind,
                "cost_usd": r.usage.cost_usd if r.usage is not None else None,
            }
            for r in runs
        ]

    def list_project_proposals(self) -> list[dict[str, Any]]:
        if self._hub is None:
            return []
        return [
            {
                "proposal_id": str(record["proposal_id"]),
                "target_uri": record["proposal"]["target_uri"],
                "reason": record["proposal"]["rationale"],
                "op": record["proposal"]["op"],
                "content": record["proposal"]["content"],
                "proposed_by": record["proposal"].get("author") or "",
                "status": record["status"],
                "created_at": record["created_at"],
            }
            for record in self._hub.list_proposals()
        ]

    def _is_recent(self, stamp: object) -> bool:
        try:
            moment = datetime.fromisoformat(str(stamp))
        except ValueError:
            return False
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=UTC)
        return self._clock() - moment <= _DIGEST_WINDOW


def _project_of(scope: str) -> str:
    return scope.removeprefix("proj:").split(":", 1)[0] if scope.startswith("proj:") else scope


class ProjectProposalReviewer:
    def __init__(self, hub: GovernedMemoryHub) -> None:
        self._hub = hub

    def approve(self, proposal_id: str, *, reviewed_by: str) -> dict[str, Any]:
        return self._decided(self._hub.approve_proposal(proposal_id, actor=reviewed_by).to_dict())

    def reject(self, proposal_id: str, *, reviewed_by: str, review_note: str) -> dict[str, Any]:
        return self._decided(self._hub.reject_proposal(proposal_id, actor=reviewed_by, reason=review_note).to_dict())

    @staticmethod
    def _decided(result: dict[str, Any]) -> dict[str, Any]:
        if result["status"] == "not_found":
            raise KeyError(f"Unknown proposal: {result['proposal_id']}")
        if result["status"] != "applied":
            raise ValueError(str(result["message"]))
        return result


@dataclass
class CollaborationRuntime:
    # One per process, whatever the config generation.
    role_store: RoleSkillMemoryStore
    closeouts: CloseoutStore
    memory_sink: CloseoutMemorySink
    pool: ExpertPoolRegistry
    taskboard: TaskboardMCPClient | None = None
    hub_server: CollaborationHubServer | None = None

    def close(self) -> None:
        if self.hub_server is not None:
            self.hub_server.close()
