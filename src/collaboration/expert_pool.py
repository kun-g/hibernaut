from __future__ import annotations

from dataclasses import dataclass

from collaboration.models import ExpertProfile, MemoryMountPlan
from collaboration.role_memory import build_four_layer_mount_plan


@dataclass(frozen=True, slots=True)
class ExpertDispatchBinding:
    profile: ExpertProfile
    mount_plan: MemoryMountPlan
    inbox_scope: str


def default_expert_profiles() -> tuple[ExpertProfile, ...]:
    return (
        ExpertProfile(
            agent_id="pool:architect",
            role_id="architect",
            display_name="系统架构师",
            executor_id="claude-opus",
            skills=("architecture-blueprint", "adr-contract", "clean-layers"),
            quality_gates=("schema-valid", "zero-reverse-deps"),
            max_concurrency=2,
            allow_direct_channel=True,
        ),
        ExpertProfile(
            agent_id="pool:sde-backend",
            role_id="sde-backend",
            display_name="后端研发专家",
            executor_id="claude-sonnet",
            skills=("python-clean-arch", "sqlite-wal", "pytest-tdd"),
            quality_gates=("pytest", "pyright", "lint-imports"),
            max_concurrency=3,
            allow_direct_channel=True,
        ),
        ExpertProfile(
            agent_id="pool:sde-frontend",
            role_id="sde-frontend",
            display_name="前端与原型专家",
            executor_id="claude-sonnet",
            skills=("editorial-ui", "canvas-prototype", "responsive-a11y"),
            quality_gates=("check-web-app", "visual-smoke"),
            max_concurrency=2,
            allow_direct_channel=True,
        ),
        ExpertProfile(
            agent_id="pool:researcher",
            role_id="researcher",
            display_name="深度调研专家",
            executor_id="claude-sonnet",
            skills=("deep-web-research", "source-grounding", "pkb-synthesis"),
            quality_gates=("citation-complete",),
            max_concurrency=3,
            allow_direct_channel=True,
        ),
        ExpertProfile(
            agent_id="pool:qa-gatekeeper",
            role_id="qa-gatekeeper",
            display_name="质量门禁与复盘专家",
            executor_id="claude-sonnet",
            skills=("regression-audit", "postmortem-triage"),
            quality_gates=("pytest", "pyright", "lint-imports"),
            max_concurrency=2,
            allow_direct_channel=False,
        ),
    )


class ExpertPoolRegistry:
    def __init__(self, profiles: tuple[ExpertProfile, ...] | None = None) -> None:
        initial = default_expert_profiles() if profiles is None else profiles
        self._by_role: dict[str, ExpertProfile] = {}
        self._by_agent: dict[str, ExpertProfile] = {}
        for profile in initial:
            self.register(profile)

    def register(self, profile: ExpertProfile) -> None:
        if profile.max_concurrency < 1:
            raise ValueError(f"max_concurrency must be >= 1 for {profile.agent_id}")
        self._by_role[profile.role_id] = profile
        self._by_agent[profile.agent_id] = profile

    def get_by_role(self, role_id: str) -> ExpertProfile:
        if role_id not in self._by_role:
            raise KeyError(f"Unknown expert role_id: {role_id}")
        return self._by_role[role_id]

    def get_by_agent(self, agent_id: str) -> ExpertProfile:
        if agent_id not in self._by_agent:
            raise KeyError(f"Unknown expert agent_id: {agent_id}")
        return self._by_agent[agent_id]

    def list_profiles(self) -> list[ExpertProfile]:
        return [self._by_role[k] for k in sorted(self._by_role)]

    def resolve_for_project(
        self,
        *,
        role_id: str,
        project_key: str,
        task_slug: str | None = None,
        extra_readonly_projects: tuple[str, ...] = (),
        budget_chars: int = 6500,
    ) -> ExpertDispatchBinding:
        profile = self.get_by_role(role_id)
        mount_plan = build_four_layer_mount_plan(
            agent_id=profile.agent_id,
            role_id=profile.role_id,
            project_key=project_key,
            extra_readonly_projects=extra_readonly_projects,
            total_budget_chars=budget_chars,
        )
        scope = f"proj:{project_key}:{profile.role_id}"
        if task_slug:
            scope = f"{scope}:{task_slug.strip()}"
        return ExpertDispatchBinding(
            profile=profile,
            mount_plan=mount_plan,
            inbox_scope=scope,
        )
