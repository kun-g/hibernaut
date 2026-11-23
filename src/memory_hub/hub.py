from __future__ import annotations

import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

from memory_hub.dreaming import CompactionAndDreaming
from memory_hub.fs_store import MarkdownFileStore
from memory_hub.governance import WriteGovernanceGate
from memory_hub.index_fts5 import DualFTSIndex
from memory_hub.models import (
    BaselineBundle,
    GCReport,
    IndexStats,
    MemoryDoc,
    MemoryHubConfig,
    PostMortemCandidate,
    PostMortemResult,
    SearchQuery,
    SearchResponse,
    WriteProposal,
    WriteResult,
)
from memory_hub.mounts import KnowledgeMountRegistry
from memory_hub.partition import parse_memory_uri, resolve_project_key
from memory_hub.ports import AuditHookPort
from memory_hub.postmortem import PostMortemGate


class MemoryHub:
    def __init__(self, config: MemoryHubConfig, *, audit: AuditHookPort | None = None) -> None:
        self._config = config
        self._audit = audit
        self._store = MarkdownFileStore(config, audit=audit)
        self._mounts = KnowledgeMountRegistry(config.mounts)
        idx_dir = Path(config.index_dir).expanduser().resolve() if config.index_dir else None
        self._index = DualFTSIndex(self._store, self._mounts, index_dir=idx_dir)
        self._gate = WriteGovernanceGate(
            self._store,
            self._index,
            config,
            on_commit=self._git_commit_if_enabled,
        )
        self._postmortem = PostMortemGate(self._gate)
        self._dreaming = CompactionAndDreaming(
            self._store,
            self._index,
            self._gate,
            config,
            on_commit=self._git_commit_if_enabled,
        )

    @property
    def config(self) -> MemoryHubConfig:
        return self._config

    @property
    def root(self) -> Path:
        return self._store.root

    def close(self) -> None:
        self._index.close()

    def set_global_instructions(self, content: str) -> None:
        self._store.set_global_instructions(content)

    def resolve_project_key(self, inbox_id: str) -> str:
        return resolve_project_key(inbox_id, self._config.inbox_project_map)

    def load_baseline(self, project_key: str) -> BaselineBundle:
        return self._store.load_baseline(project_key)

    def read_on_demand(
        self,
        uri: str,
        *,
        heading: str | None = None,
        max_bytes: int | None = None,
    ) -> MemoryDoc:
        parsed = parse_memory_uri(uri)
        if parsed.scheme == "kb":
            return self._mounts.read(parsed.scope, parsed.rel_path, max_bytes=max_bytes)
        return self._store.read_doc(uri, heading=heading, max_bytes=max_bytes)

    def propose_write(self, proposal: WriteProposal) -> WriteResult:
        return self._gate.propose_write(proposal)

    def approve_proposal(self, proposal_id: str, *, actor: str) -> WriteResult:
        return self._gate.approve_proposal(proposal_id, actor=actor)

    def reject_proposal(self, proposal_id: str, *, actor: str, reason: str = "") -> WriteResult:
        return self._gate.reject_proposal(proposal_id, actor=actor, reason=reason)

    def list_proposals(self, *, status: str | None = "pending") -> list[dict[str, Any]]:
        return self._gate.list_proposals(status=status)

    def search(self, query: SearchQuery) -> SearchResponse:
        return self._index.search(query)

    def rebuild_index(self, *, scopes: tuple[str, ...] | None = None, include_kb: bool = True) -> IndexStats:
        return self._index.rebuild(scopes=scopes, include_kb=include_kb)

    def record_postmortem(self, candidate: PostMortemCandidate) -> PostMortemResult:
        return self._postmortem.record(candidate)

    def run_mechanical_gc(
        self,
        project_key: str,
        *,
        closed_work_keys: set[str] | None = None,
        now: datetime | None = None,
    ) -> GCReport:
        return self._dreaming.run_mechanical_gc(project_key, closed_work_keys=closed_work_keys, now=now)

    def submit_dreaming_proposal(
        self,
        project_key: str,
        *,
        distilled_rule: str,
        rationale: str,
        supersedes: tuple[str, ...] = (),
        evidence_ref: str | None = None,
    ) -> WriteResult:
        return self._dreaming.submit_dreaming_proposal(
            project_key,
            distilled_rule=distilled_rule,
            rationale=rationale,
            supersedes=supersedes,
            evidence_ref=evidence_ref,
        )

    def _git_commit_if_enabled(self, message: str) -> bool:
        if not self._config.auto_git_commit:
            return False
        if not (self._store.root / ".git").exists():
            return False
        try:
            subprocess.run(
                ["git", "add", "-A", "global", "projects", "shared", "proposals"],
                cwd=self._store.root,
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
            )
            diff_check = subprocess.run(
                ["git", "diff", "--cached", "--quiet"],
                cwd=self._store.root,
                check=False,
                capture_output=True,
                timeout=10,
            )
            if diff_check.returncode == 0:
                return False
            res = subprocess.run(
                ["git", "commit", "-m", message],
                cwd=self._store.root,
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
            )
            return res.returncode == 0
        except Exception:
            return False


class LegacyMemoryPortAdapter:
    def __init__(self, hub: MemoryHub, inbox_id: str) -> None:
        self._hub = hub
        self._inbox_id = inbox_id
        self._project_key = hub.resolve_project_key(inbox_id)

    @property
    def project_key(self) -> str:
        return self._project_key

    def load_baseline(self) -> dict[str, str]:
        return self._hub.load_baseline(self._project_key).to_legacy_dict()

    def load_baseline_bundle(self) -> BaselineBundle:
        return self._hub.load_baseline(self._project_key)

    def read_on_demand(self, path: str) -> str:
        if "://" in path:
            return self._hub.read_on_demand(path).content
        uri = f"proj://{self._project_key}/{path.lstrip('/')}"
        return self._hub.read_on_demand(uri).content

    def write(self, path: str, content: str) -> None:
        normalized = path.lstrip("/")
        if normalized == "global/instructions.md" or normalized.startswith("global/"):
            raise PermissionError("Global instructions.md is read-only")
        uri = path if "://" in path else f"proj://{self._project_key}/{normalized}"
        op = "append_reported" if normalized == "reported.md" else "upsert_file"
        res = self._hub.propose_write(
            WriteProposal(
                target_uri=uri,
                op=op,
                content=content,
                rationale="legacy memory write",
                force_new=True,
            )
        )
        if res.status == "rejected_readonly":
            raise PermissionError(res.message)
        if res.status in ("rejected_budget", "conflict"):
            raise ValueError(res.message)
