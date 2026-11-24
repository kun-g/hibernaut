from __future__ import annotations

import copy
from typing import Any, get_args

from collaboration._schema import check
from collaboration.closeout import CloseoutCoordinator
from collaboration.digest import DigestAssembler
from collaboration.expert_pool import ExpertPoolRegistry
from collaboration.models import CloseoutDecision, CloseoutMemoryDraft, NotificationKind
from collaboration.notification_policy import NotificationPolicyEngine
from collaboration.role_memory import RoleSkillMemoryStore


_TEXT: dict[str, Any] = {"type": "string", "minLength": 1}
_FREE_TEXT: dict[str, Any] = {"type": "string"}


def _object(required: dict[str, Any], optional: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {**required, **(optional or {})},
        "required": list(required),
        "additionalProperties": False,
    }


def _list(items: dict[str, Any], **limits: int) -> dict[str, Any]:
    return {"type": "array", "items": items, **limits}


_TOOLS: list[dict[str, Any]] = [
    {
        "name": "emit_closeout_card",
        "description": "Emit a draft CloseoutCard after a direct Owner-Expert session (D6-A).",
        "inputSchema": _object(
            {
                "session_id": _TEXT,
                "agent_id": _TEXT,
                "project_key": _TEXT,
                "summary": _TEXT,
                "decisions": _list(
                    _object({"key": _TEXT, "title": _TEXT, "choice": _TEXT}, {"rationale": _FREE_TEXT}), minItems=1
                ),
            },
            {
                "task_id": _TEXT,
                "follow_up_tasks": _list(_TEXT),
                "memory_proposals": _list(_object({"target_uri": _TEXT, "content": _TEXT, "reason": _TEXT})),
            },
        ),
    },
    {
        "name": "confirm_closeout_card",
        "description": "Confirm a CloseoutCard by Owner and sync to Chief of Staff, Taskboard, and Memory proposals.",
        "inputSchema": _object({"card_id": _TEXT, "confirmed_by": _TEXT}),
    },
    {
        "name": "resolve_expert_mount",
        "description": "Resolve a shared Expert Pool role into a single-project RW + role-skill RO/propose mount plan (D3-B/D7-B).",
        "inputSchema": _object(
            {"role_id": _TEXT, "project_key": _TEXT},
            {
                "task_slug": _TEXT,
                "extra_readonly_projects": _list(_TEXT),
                "budget_chars": {"type": "integer", "minimum": 1},
            },
        ),
    },
    {
        "name": "propose_role_skill",
        "description": "Queue a CAS-protected skill update proposal under roles/<role>/ (D7-B).",
        "inputSchema": _object(
            {"role_id": _TEXT, "content": _TEXT, "reason": _TEXT},
            {"proposed_by": _TEXT, "rel_path": _TEXT, "base_sha256": _TEXT},
        ),
    },
    {
        "name": "route_notification",
        "description": "Evaluate D8-B push vs. pull routing for an outbound notification.",
        "inputSchema": _object(
            {"kind": {"type": "string", "enum": list(get_args(NotificationKind))}},
            {"correlation_key": _FREE_TEXT, "is_urgent_blocker": {"type": "boolean"}},
        ),
    },
]


class CollaborationMCPTools:
    def __init__(
        self,
        *,
        role_store: RoleSkillMemoryStore,
        expert_pool: ExpertPoolRegistry,
        closeout: CloseoutCoordinator,
        policy: NotificationPolicyEngine | None = None,
        digest: DigestAssembler | None = None,
    ) -> None:
        self._role_store = role_store
        self._expert_pool = expert_pool
        self._closeout = closeout
        self._policy = policy or NotificationPolicyEngine()
        self._digest = digest or DigestAssembler()

    def tool_definitions(self) -> list[dict[str, Any]]:
        return copy.deepcopy(_TOOLS)

    def call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        # KeyError means the tool is unknown; whatever is wrong with a call to a known one is a ValueError.
        schema = next((t["inputSchema"] for t in _TOOLS if t["name"] == name), None)
        if schema is None:
            raise KeyError(f"Unknown collaboration MCP tool: {name}")
        check(schema, arguments)
        try:
            return self._call(name, arguments)
        except KeyError as exc:
            raise ValueError(str(exc.args[0]) if exc.args else "not found") from exc

    def _call(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if name == "emit_closeout_card":
            decisions = tuple(
                CloseoutDecision(
                    key=str(d["key"]),
                    title=str(d["title"]),
                    choice=str(d["choice"]),
                    rationale=str(d.get("rationale", "")),
                )
                for d in arguments.get("decisions", [])
            )
            mem_drafts = tuple(
                CloseoutMemoryDraft(
                    target_uri=str(m["target_uri"]),
                    content=str(m["content"]),
                    reason=str(m["reason"]),
                )
                for m in arguments.get("memory_proposals", [])
            )
            card = self._closeout.emit_draft(
                session_id=str(arguments["session_id"]),
                agent_id=str(arguments["agent_id"]),
                project_key=str(arguments["project_key"]),
                summary=str(arguments["summary"]),
                decisions=decisions,
                task_id=arguments.get("task_id"),
                follow_up_tasks=tuple(str(t) for t in arguments.get("follow_up_tasks", [])),
                memory_proposals=mem_drafts,
            )
            return card.to_dict()

        if name == "confirm_closeout_card":
            res = self._closeout.confirm_and_sync(
                str(arguments["card_id"]),
                confirmed_by=str(arguments["confirmed_by"]),
            )
            return {
                "card_id": res.card_id,
                "chief_of_staff_notified": res.chief_of_staff_notified,
                "tasks_created": list(res.tasks_created),
                "proposals_queued": list(res.proposals_queued),
                "already_synced": res.already_synced,
            }

        if name == "resolve_expert_mount":
            binding = self._expert_pool.resolve_for_project(
                role_id=str(arguments["role_id"]),
                project_key=str(arguments["project_key"]),
                task_slug=arguments.get("task_slug"),
                extra_readonly_projects=tuple(arguments.get("extra_readonly_projects", [])),
                budget_chars=int(arguments.get("budget_chars", 6500)),
            )
            return {
                "agent_id": binding.profile.agent_id,
                "role_id": binding.profile.role_id,
                "executor_id": binding.profile.executor_id,
                "inbox_scope": binding.inbox_scope,
                "slices": [
                    {
                        "uri": s.uri,
                        "layer": s.layer,
                        "write_mode": s.write_mode,
                        "budget_chars": s.budget_chars,
                    }
                    for s in binding.mount_plan.slices
                ],
            }

        if name == "propose_role_skill":
            proposal = self._role_store.propose_skill_write(
                role_id=str(arguments["role_id"]),
                content=str(arguments["content"]),
                reason=str(arguments["reason"]),
                proposed_by=str(arguments.get("proposed_by", "agent")),
                rel_path=str(arguments.get("rel_path", "skills.md")),
                base_sha256=arguments.get("base_sha256"),
            )
            return proposal.to_dict()

        if name == "route_notification":
            decision = self._policy.route(
                kind=arguments["kind"],
                correlation_key=str(arguments.get("correlation_key", "")),
                is_urgent_blocker=bool(arguments.get("is_urgent_blocker", False)),
            )
            return {
                "kind": decision.kind,
                "channel": decision.channel,
                "reason": decision.reason,
                "correlation_key": decision.correlation_key,
            }

        raise KeyError(f"Unknown collaboration MCP tool: {name}")
