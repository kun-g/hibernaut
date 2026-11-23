from __future__ import annotations

from typing import Any, Protocol

from memory_hub.models import (
    AuthPrincipal,
    BaselineBundle,
    GCReport,
    IndexStats,
    MemoryDoc,
    PostMortemCandidate,
    PostMortemResult,
    SearchQuery,
    SearchResponse,
    WriteProposal,
    WriteResult,
)


class AuditHookPort(Protocol):
    def emit(self, stage: str, outcome: str, detail: dict[str, Any], *, severity: str = "info") -> None: ...


class TokenAuthPort(Protocol):
    def authenticate(self, bearer_token: str) -> AuthPrincipal | None: ...


class MemoryPortV2(Protocol):
    def load_baseline(self, project_key: str) -> BaselineBundle: ...

    def read_on_demand(
        self,
        uri: str,
        *,
        heading: str | None = None,
        max_bytes: int | None = None,
    ) -> MemoryDoc: ...

    def propose_write(self, proposal: WriteProposal) -> WriteResult: ...

    def approve_proposal(self, proposal_id: str, *, actor: str) -> WriteResult: ...

    def reject_proposal(self, proposal_id: str, *, actor: str, reason: str = "") -> WriteResult: ...

    def list_proposals(self, *, status: str | None = "pending") -> list[dict[str, Any]]: ...

    def search(self, query: SearchQuery) -> SearchResponse: ...

    def rebuild_index(self, *, scopes: tuple[str, ...] | None = None, include_kb: bool = True) -> IndexStats: ...

    def record_postmortem(self, candidate: PostMortemCandidate) -> PostMortemResult: ...

    def run_mechanical_gc(
        self,
        project_key: str,
        *,
        closed_work_keys: set[str] | None = None,
    ) -> GCReport: ...
