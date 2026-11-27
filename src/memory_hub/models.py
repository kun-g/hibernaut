from __future__ import annotations

import hashlib
import math
import secrets
from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Any, Literal

SCHEMA_VERSION = "memory.v1"

TierKind = Literal["stable", "volatile"]
LayerKind = Literal["project", "shared", "kb"]
WriteOp = Literal["append", "replace_section", "upsert_file", "archive", "append_reported"]
WriteStatus = Literal[
    "applied",
    "pending_approval",
    "duplicate_warning",
    "rejected_budget",
    "rejected_readonly",
    "rejected_transient",
    "conflict",
    "not_found",
]
SearchMode = Literal["fts", "hybrid"]
AccessRole = Literal["readonly", "propose", "admin"]


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def estimate_tokens(text: str) -> int:
    if not text:
        return 0
    return math.ceil(len(text.encode("utf-8")) / 3)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def generate_id(prefix: str) -> str:
    ts_ms = int(datetime.now(UTC).timestamp() * 1000)
    return f"{prefix}_{ts_ms:012x}{secrets.token_hex(6)}"


@dataclass(frozen=True, slots=True)
class BaselineBudgetConfig:
    global_instructions: int = 1500
    project_instructions: int = 2500
    project_index: int = 1500
    project_reported: int = 1000
    total_max_tokens: int = 6500

    def __post_init__(self) -> None:
        for name in (
            "global_instructions",
            "project_instructions",
            "project_index",
            "project_reported",
            "total_max_tokens",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"Budget '{name}' must be positive")


@dataclass(frozen=True, slots=True)
class BaselineBlock:
    uri: str
    tier: TierKind
    content: str
    sha256: str
    est_tokens: int
    truncated: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "uri": self.uri,
            "tier": self.tier,
            "content": self.content,
            "sha256": self.sha256,
            "est_tokens": self.est_tokens,
            "truncated": self.truncated,
        }


@dataclass(frozen=True, slots=True)
class BaselineBundle:
    project_key: str
    stable_prefix: tuple[BaselineBlock, ...]
    volatile_tail: tuple[BaselineBlock, ...]
    total_est_tokens: int
    truncated_blocks: tuple[str, ...] = ()
    stable_prefix_sha256: str = ""
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "project_key": self.project_key,
            "stable_prefix": [b.to_dict() for b in self.stable_prefix],
            "volatile_tail": [b.to_dict() for b in self.volatile_tail],
            "total_est_tokens": self.total_est_tokens,
            "truncated_blocks": list(self.truncated_blocks),
            "stable_prefix_sha256": self.stable_prefix_sha256,
        }

    def to_legacy_dict(self) -> dict[str, str]:
        by_uri = {b.uri: b.content for b in (*self.stable_prefix, *self.volatile_tail)}
        global_txt = by_uri.get("global://instructions.md", "").strip()
        proj_instr = by_uri.get(f"proj://{self.project_key}/instructions.md", "").strip()
        if global_txt and proj_instr:
            combined_instr = f"{global_txt}\n\n{proj_instr}"
        else:
            combined_instr = global_txt or proj_instr
        return {
            "instructions.md": combined_instr,
            "index.md": by_uri.get(f"proj://{self.project_key}/index.md", ""),
            "reported.md": by_uri.get(f"proj://{self.project_key}/reported.md", ""),
        }


@dataclass(frozen=True, slots=True)
class MemoryDoc:
    uri: str
    content: str
    sha256: str
    est_tokens: int
    updated_at: str
    readonly: bool
    heading: str | None = None
    truncated: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "uri": self.uri,
            "content": self.content,
            "sha256": self.sha256,
            "est_tokens": self.est_tokens,
            "updated_at": self.updated_at,
            "readonly": self.readonly,
            "heading": self.heading,
            "truncated": self.truncated,
        }


@dataclass(frozen=True, slots=True)
class WriteProposal:
    target_uri: str
    op: WriteOp
    content: str
    rationale: str
    section_heading: str | None = None
    base_sha256: str | None = None
    evidence_ref: str | None = None
    supersedes: tuple[str, ...] = ()
    force_new: bool = False
    author: str | None = None
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "target_uri": self.target_uri,
            "op": self.op,
            "content": self.content,
            "rationale": self.rationale,
            "section_heading": self.section_heading,
            "base_sha256": self.base_sha256,
            "evidence_ref": self.evidence_ref,
            "supersedes": list(self.supersedes),
            "force_new": self.force_new,
            "author": self.author,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WriteProposal:
        return cls(
            target_uri=str(data["target_uri"]),
            op=data["op"],
            content=str(data["content"]),
            rationale=str(data["rationale"]),
            section_heading=data.get("section_heading"),
            base_sha256=data.get("base_sha256"),
            evidence_ref=data.get("evidence_ref"),
            supersedes=tuple(data.get("supersedes") or ()),
            force_new=bool(data.get("force_new", False)),
            author=data.get("author"),
            schema_version=str(data.get("schema_version", SCHEMA_VERSION)),
        )


@dataclass(frozen=True, slots=True)
class WriteResult:
    status: WriteStatus
    target_uri: str
    proposal_id: str | None = None
    new_sha256: str | None = None
    duplicate_candidates: tuple[str, ...] = ()
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "target_uri": self.target_uri,
            "proposal_id": self.proposal_id,
            "new_sha256": self.new_sha256,
            "duplicate_candidates": list(self.duplicate_candidates),
            "message": self.message,
        }


@dataclass(frozen=True, slots=True)
class SearchQuery:
    scope: str
    query: str
    top_k: int = 8
    include_project: bool = True
    include_shared: bool = True
    include_kb: bool = False
    include_archived: bool = False
    mode: SearchMode = "fts"


@dataclass(frozen=True, slots=True)
class SearchHit:
    uri: str
    layer: LayerKind
    heading_path: str
    snippet: str
    score: float
    sha256: str
    updated_at: str
    readonly: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "uri": self.uri,
            "layer": self.layer,
            "heading_path": self.heading_path,
            "snippet": self.snippet,
            "score": round(self.score, 6),
            "sha256": self.sha256,
            "updated_at": self.updated_at,
            "readonly": self.readonly,
        }


@dataclass(frozen=True, slots=True)
class SearchResponse:
    scope: str
    query: str
    mode: SearchMode
    items: tuple[SearchHit, ...]
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "scope": self.scope,
            "query": self.query,
            "mode": self.mode,
            "items": [hit.to_dict() for hit in self.items],
        }


@dataclass(frozen=True, slots=True)
class IndexStats:
    indexed_files: int
    indexed_chunks: int
    removed_files: int
    kb_indexed_files: int = 0
    kb_indexed_chunks: int = 0


@dataclass(frozen=True, slots=True)
class PostMortemCandidate:
    project_key: str
    run_id: str
    error_kind: str
    root_cause: str
    proposed_rule: str
    recurrence_count: int = 1
    correlation_key: str | None = None
    slug: str | None = None
    force_propose: bool = False
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "project_key": self.project_key,
            "run_id": self.run_id,
            "error_kind": self.error_kind,
            "root_cause": self.root_cause,
            "proposed_rule": self.proposed_rule,
            "recurrence_count": self.recurrence_count,
            "correlation_key": self.correlation_key,
            "slug": self.slug,
            "force_propose": self.force_propose,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PostMortemCandidate:
        return cls(
            project_key=str(data["project_key"]),
            run_id=str(data["run_id"]),
            error_kind=str(data["error_kind"]),
            root_cause=str(data["root_cause"]),
            proposed_rule=str(data["proposed_rule"]),
            recurrence_count=int(data.get("recurrence_count", 1)),
            correlation_key=data.get("correlation_key"),
            slug=data.get("slug"),
            force_propose=bool(data.get("force_propose", False)),
            schema_version=str(data.get("schema_version", SCHEMA_VERSION)),
        )


@dataclass(frozen=True, slots=True)
class PostMortemResult:
    status: Literal["staged", "staged_and_proposed", "rejected_transient"]
    note_uri: str | None = None
    proposal_id: str | None = None
    message: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "note_uri": self.note_uri,
            "proposal_id": self.proposal_id,
            "message": self.message,
        }


@dataclass(frozen=True, slots=True)
class GCReport:
    project_key: str
    archived_reported_entries: int = 0
    archived_progress_files: tuple[str, ...] = ()
    pruned_index_links: int = 0
    added_index_links: int = 0
    expired_proposals: tuple[str, ...] = ()
    git_committed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "project_key": self.project_key,
            "archived_reported_entries": self.archived_reported_entries,
            "archived_progress_files": list(self.archived_progress_files),
            "pruned_index_links": self.pruned_index_links,
            "added_index_links": self.added_index_links,
            "expired_proposals": list(self.expired_proposals),
            "git_committed": self.git_committed,
        }


@dataclass(frozen=True, slots=True)
class AuthPrincipal:
    subject: str
    role: AccessRole
    allowed_projects: tuple[str, ...] = ("*",)
    allow_kb: bool = True

    def can_access_project(self, project_key: str) -> bool:
        return "*" in self.allowed_projects or project_key in self.allowed_projects

    def has_role(self, required: AccessRole) -> bool:
        order = {"readonly": 1, "propose": 2, "admin": 3}
        return order[self.role] >= order[required]


@dataclass(frozen=True, slots=True)
class KnowledgeMountConfig:
    name: str
    root: str
    include_globs: tuple[str, ...] = ("**/*.md",)
    exclude_globs: tuple[str, ...] = (".git/**", "raw/**", "assets/**", "runtime/**")
    max_file_bytes: int = 262144
    mode: Literal["readonly"] = "readonly"


@dataclass(frozen=True, slots=True)
class MemoryHubConfig:
    memory_root: str
    default_budget: BaselineBudgetConfig = field(default_factory=BaselineBudgetConfig)
    project_budgets: dict[str, BaselineBudgetConfig] = field(default_factory=dict)
    inbox_project_map: dict[str, str] = field(default_factory=dict)
    mounts: tuple[KnowledgeMountConfig, ...] = ()
    index_dir: str | None = None
    max_note_bytes: int = 16384
    proposal_ttl_days: int = 14
    progress_archive_days: int = 7
    auto_git_commit: bool = False

    def budget_for(self, project_key: str) -> BaselineBudgetConfig:
        return self.project_budgets.get(project_key, self.default_budget)
