from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

CloseoutStatus = Literal["draft", "confirmed", "synced", "discarded"]
ProposalStatus = Literal["pending", "approved", "rejected"]
NotificationChannel = Literal["feishu_push", "hub_pull", "silent"]
NotificationKind = Literal[
    "approval_card",
    "blocker_alert",
    "daily_progress",
    "digest_ready",
    "proposal_queued",
    "run_completed",
    "closeout_synced",
]
MemoryWriteMode = Literal["rw", "propose", "ro"]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class CloseoutDecision:
    key: str
    title: str
    choice: str
    rationale: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class CloseoutMemoryDraft:
    target_uri: str
    content: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class CloseoutCard:
    card_id: str
    session_id: str
    agent_id: str
    project_key: str
    summary: str
    decisions: tuple[CloseoutDecision, ...]
    created_at: str
    task_id: str | None = None
    follow_up_tasks: tuple[str, ...] = ()
    memory_proposals: tuple[CloseoutMemoryDraft, ...] = ()
    status: CloseoutStatus = "draft"
    confirmed_by: str | None = None
    confirmed_at: str | None = None
    schema_version: str = "1.0"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "card_id": self.card_id,
            "session_id": self.session_id,
            "agent_id": self.agent_id,
            "project_key": self.project_key,
            "task_id": self.task_id,
            "summary": self.summary,
            "decisions": [d.to_dict() for d in self.decisions],
            "follow_up_tasks": list(self.follow_up_tasks),
            "memory_proposals": [m.to_dict() for m in self.memory_proposals],
            "status": self.status,
            "confirmed_by": self.confirmed_by,
            "confirmed_at": self.confirmed_at,
            "created_at": self.created_at,
        }


@dataclass(frozen=True, slots=True)
class CloseoutSyncResult:
    card_id: str
    chief_of_staff_notified: bool
    tasks_created: tuple[str, ...]
    proposals_queued: tuple[str, ...]
    already_synced: bool = False


@dataclass(frozen=True, slots=True)
class ExpertProfile:
    agent_id: str
    role_id: str
    display_name: str
    executor_id: str
    skills: tuple[str, ...]
    quality_gates: tuple[str, ...]
    max_concurrency: int = 2
    allow_direct_channel: bool = True
    role_memory_uri: str = ""
    schema_version: str = "1.0"

    def __post_init__(self) -> None:
        if not self.role_memory_uri:
            object.__setattr__(self, "role_memory_uri", f"role://{self.role_id}/skills.md")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "agent_id": self.agent_id,
            "role_id": self.role_id,
            "display_name": self.display_name,
            "executor_id": self.executor_id,
            "skills": list(self.skills),
            "quality_gates": list(self.quality_gates),
            "max_concurrency": self.max_concurrency,
            "allow_direct_channel": self.allow_direct_channel,
            "role_memory_uri": self.role_memory_uri,
        }


@dataclass(frozen=True, slots=True)
class MemoryMountSlice:
    uri: str
    layer: Literal["user_prefs", "project", "role_skill", "shared", "kb"]
    write_mode: MemoryWriteMode
    budget_chars: int


@dataclass(frozen=True, slots=True)
class MemoryMountPlan:
    agent_id: str
    role_id: str
    project_key: str
    slices: tuple[MemoryMountSlice, ...]
    total_budget_chars: int


@dataclass(frozen=True, slots=True)
class RoleMemoryProposal:
    proposal_id: str
    role_id: str
    rel_path: str
    content: str
    reason: str
    proposed_by: str
    base_sha256: str
    status: ProposalStatus
    created_at: str
    reviewed_by: str | None = None
    reviewed_at: str | None = None
    review_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class NotificationDecision:
    kind: NotificationKind
    channel: NotificationChannel
    reason: str
    correlation_key: str = ""


@dataclass(frozen=True, slots=True)
class DigestProjectSummary:
    project_key: str
    in_progress: int
    ready: int
    done_recent: int
    blocked: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DigestCostSummary:
    total_runs: int
    failed_runs: int
    total_cost_usd: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DigestProposalItem:
    proposal_id: str
    scope: str
    target: str
    reason: str
    status: ProposalStatus
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DigestSnapshot:
    generated_at: str
    window_label: str
    headline: str
    projects: tuple[DigestProjectSummary, ...]
    cost_summary: DigestCostSummary
    pending_proposals: tuple[DigestProposalItem, ...]
    recent_closeouts: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    schema_version: str = "1.0"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at,
            "window_label": self.window_label,
            "headline": self.headline,
            "projects": [p.to_dict() for p in self.projects],
            "cost_summary": self.cost_summary.to_dict(),
            "pending_proposals": [p.to_dict() for p in self.pending_proposals],
            "recent_closeouts": list(self.recent_closeouts),
        }
