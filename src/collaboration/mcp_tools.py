from __future__ import annotations

from typing import Any

from collaboration.closeout import CloseoutCoordinator
from collaboration.digest import DigestAssembler
from collaboration.expert_pool import ExpertPoolRegistry
from collaboration.models import CloseoutDecision, CloseoutMemoryDraft
from collaboration.notification_policy import NotificationPolicyEngine
from collaboration.role_memory import RoleSkillMemoryStore


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
        return [
            {
                "name": "emit_closeout_card",
                "description": "Emit a draft CloseoutCard after a direct Owner-Expert session (D6-A).",
            },
            {
                "name": "confirm_closeout_card",
                "description": "Confirm a CloseoutCard by Owner and sync to Chief of Staff, Taskboard, and Memory proposals.",
            },
            {
                "name": "resolve_expert_mount",
                "description": "Resolve a shared Expert Pool role into a single-project RW + role-skill RO/propose mount plan (D3-B/D7-B).",
            },
            {
                "name": "propose_role_skill",
                "description": "Queue a CAS-protected skill update proposal under roles/<role>/ (D7-B).",
            },
            {
                "name": "route_notification",
                "description": "Evaluate D8-B push vs. pull routing for an outbound notification.",
            },
        ]

    def call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
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
