from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Literal
from collections.abc import Callable

from memory_hub.fs_store import MarkdownFileStore, atomic_write_text, replace_markdown_section
from memory_hub.index_fts5 import DualFTSIndex
from memory_hub.models import (
    MemoryHubConfig,
    SearchQuery,
    WriteProposal,
    WriteResult,
    estimate_tokens,
    generate_id,
    sha256_text,
    utc_now_iso,
)
from memory_hub.partition import ParsedUri, parse_memory_uri

PolicyAction = Literal["DENY_RUNTIME", "REQUIRE_APPROVAL", "ALLOW_WITH_DEDUP", "ALLOW_DIRECT", "ALLOW_ROLLING_APPEND"]
_NORM_FP_RE = re.compile(r"[^\w\u4e00-\u9fff]+")


def classify_write_policy(parsed: ParsedUri) -> PolicyAction:
    if parsed.scheme in ("global", "kb"):
        return "DENY_RUNTIME"
    if parsed.scheme == "shared":
        return "REQUIRE_APPROVAL"
    rel = parsed.rel_path.lstrip("/")
    if rel == "instructions.md":
        return "REQUIRE_APPROVAL"
    if rel == "reported.md":
        return "ALLOW_ROLLING_APPEND"
    if rel.startswith("progress/"):
        return "ALLOW_DIRECT"
    if rel.startswith("notes/") or rel == "index.md":
        return "ALLOW_WITH_DEDUP"
    return "REQUIRE_APPROVAL"


def content_fingerprint(text: str) -> str:
    norm = _NORM_FP_RE.sub("", text.lower())
    return sha256_text(norm)


def _apply_op(existing: str, proposal: WriteProposal) -> str:
    if proposal.op == "upsert_file":
        return proposal.content
    if proposal.op == "append":
        if not existing.strip():
            return proposal.content.rstrip() + "\n"
        return existing.rstrip() + "\n\n" + proposal.content.strip() + "\n"
    if proposal.op == "append_reported":
        line = proposal.content.strip()
        if not line.startswith("- "):
            line = f"- {utc_now_iso()[:10]} {line}"
        if not existing.strip():
            return line + "\n"
        return existing.rstrip() + "\n" + line + "\n"
    if proposal.op == "replace_section":
        heading = proposal.section_heading or "Notes"
        return replace_markdown_section(existing, heading, proposal.content)
    if proposal.op == "archive":
        return proposal.content
    raise ValueError(f"Unsupported write op: {proposal.op}")


class WriteGovernanceGate:
    def __init__(
        self,
        store: MarkdownFileStore,
        index: DualFTSIndex,
        config: MemoryHubConfig,
        *,
        on_commit: Callable[[str], None] | None = None,
    ) -> None:
        self._store = store
        self._index = index
        self._config = config
        self._on_commit = on_commit

    def _check_budget(self, parsed: ParsedUri, new_text: str) -> str | None:
        est = estimate_tokens(new_text)
        if parsed.scheme == "proj":
            b = self._config.budget_for(parsed.scope)
            if parsed.rel_path == "instructions.md" and est > b.project_instructions:
                return f"instructions.md exceeds budget ({est} > {b.project_instructions} est_tokens)"
            if parsed.rel_path == "index.md" and est > b.project_index:
                return f"index.md exceeds budget ({est} > {b.project_index} est_tokens)"
            if parsed.rel_path.startswith("notes/") and len(new_text.encode("utf-8")) > self._config.max_note_bytes:
                return f"note exceeds max_note_bytes ({self._config.max_note_bytes})"
        elif parsed.scheme == "shared":
            if len(new_text.encode("utf-8")) > self._config.max_note_bytes:
                return f"shared note exceeds max_note_bytes ({self._config.max_note_bytes})"
        return None

    def _find_duplicates(self, parsed: ParsedUri, proposal: WriteProposal, existing_text: str) -> tuple[str, ...]:
        if proposal.force_new or proposal.supersedes:
            return ()
        candidates: list[str] = []
        new_fp = content_fingerprint(proposal.content)
        if existing_text and new_fp in (content_fingerprint(existing_text),):
            candidates.append(parsed.raw_uri)
        elif existing_text:
            norm_new = _NORM_FP_RE.sub("", proposal.content.lower())
            norm_existing = _NORM_FP_RE.sub("", existing_text.lower())
            if norm_new and len(norm_new) >= 12 and norm_new in norm_existing:
                candidates.append(parsed.raw_uri)

        if parsed.scheme == "proj" and parsed.rel_path.startswith("notes/"):
            first_line = proposal.content.strip().splitlines()[0] if proposal.content.strip() else ""
            q_text = first_line[:120]
            if len(q_text) >= 6:
                res = self._index.search(
                    SearchQuery(
                        scope=parsed.scope,
                        query=q_text,
                        top_k=3,
                        include_project=True,
                        include_shared=False,
                        include_kb=False,
                    )
                )
                for hit in res.items:
                    if hit.uri != parsed.raw_uri and hit.score >= 0.65:
                        candidates.append(hit.uri)
        return tuple(dict.fromkeys(candidates))

    def _update_project_index(self, project_key: str, rel_path: str, summary: str) -> None:
        if not rel_path.startswith("notes/") or rel_path.startswith("notes/reported_archive/"):
            return
        proj_dir = self._store.ensure_project(project_key)
        index_path = proj_dir / "index.md"
        existing = index_path.read_text(encoding="utf-8") if index_path.is_file() else ""
        if rel_path in existing:
            return
        one_line = " ".join(summary.strip().splitlines()[:1])[:120]
        entry = f"- `{rel_path}` | {one_line}"
        updated = f"{existing.rstrip()}\n{entry}\n" if existing.strip() else f"{entry}\n"
        if estimate_tokens(updated) <= self._config.budget_for(project_key).project_index:
            atomic_write_text(index_path, updated)

    def _refresh_index_safe(self, parsed: ParsedUri, target_path: Path) -> None:
        try:
            layer = "project" if parsed.scheme == "proj" else "shared"
            self._index.refresh_uri(parsed.raw_uri, parsed.scope, layer, target_path)
        except Exception:
            pass

    def propose_write(self, proposal: WriteProposal, *, queue_as: str | None = None) -> WriteResult:
        # queue_as: wait for approval whatever the target's policy, under this id; queued once, however often proposed.
        parsed = parse_memory_uri(proposal.target_uri)
        policy = classify_write_policy(parsed)
        if queue_as is not None and policy != "DENY_RUNTIME":
            if (self._store.proposals_dir / f"{queue_as}.json").is_file():
                return self._queued(proposal, queue_as)
            policy = "REQUIRE_APPROVAL"
        if policy == "DENY_RUNTIME":
            return WriteResult(
                status="rejected_readonly",
                target_uri=proposal.target_uri,
                message=f"Target '{proposal.target_uri}' is strictly read-only for runtime writes",
            )

        target_path = self._store.resolve_uri_path(parsed)
        existing = target_path.read_text(encoding="utf-8") if target_path.is_file() else ""
        existing_sha = sha256_text(existing) if target_path.is_file() else None

        if target_path.is_file() and existing and proposal.base_sha256 is not None:
            if proposal.base_sha256 != existing_sha:
                return WriteResult(
                    status="conflict",
                    target_uri=proposal.target_uri,
                    message=f"CAS base_sha256 mismatch (expected {existing_sha}, got {proposal.base_sha256})",
                )

        if policy == "ALLOW_ROLLING_APPEND":
            new_text = _apply_op(existing, proposal)
            atomic_write_text(target_path, new_text)
            self._store.roll_reported_if_needed(parsed.scope)
            final_text = target_path.read_text(encoding="utf-8")
            self._refresh_index_safe(parsed, target_path)
            return WriteResult(
                status="applied",
                target_uri=proposal.target_uri,
                new_sha256=sha256_text(final_text),
                message="Appended to reported.md with rolling budget check",
            )

        new_text = _apply_op(existing, proposal)
        budget_err = self._check_budget(parsed, new_text)
        if budget_err:
            return WriteResult(
                status="rejected_budget",
                target_uri=proposal.target_uri,
                message=budget_err,
            )

        if policy in ("ALLOW_WITH_DEDUP", "REQUIRE_APPROVAL"):
            dups = self._find_duplicates(parsed, proposal, existing)
            if dups:
                return WriteResult(
                    status="duplicate_warning",
                    target_uri=proposal.target_uri,
                    duplicate_candidates=dups,
                    message="Duplicate or highly similar memory entry detected; merge or pass supersedes/force_new=True",
                )

        if policy == "REQUIRE_APPROVAL":
            prop_id = queue_as or generate_id("prop")
            record: dict[str, Any] = {
                "proposal_id": prop_id,
                "status": "pending",
                "created_at": utc_now_iso(),
                "decided_at": None,
                "decided_by": None,
                "expected_base_sha256": existing_sha,
                "proposal": proposal.to_dict(),
            }
            prop_path = self._store.proposals_dir / f"{prop_id}.json"
            atomic_write_text(prop_path, json.dumps(record, ensure_ascii=False, indent=2) + "\n")
            return self._queued(proposal, prop_id)

        atomic_write_text(target_path, new_text)
        if parsed.scheme == "proj":
            self._update_project_index(parsed.scope, parsed.rel_path, proposal.rationale or proposal.content)
        self._refresh_index_safe(parsed, target_path)
        return WriteResult(
            status="applied",
            target_uri=proposal.target_uri,
            new_sha256=sha256_text(new_text),
            message="Applied atomically",
        )

    @staticmethod
    def _queued(proposal: WriteProposal, proposal_id: str) -> WriteResult:
        return WriteResult(
            status="pending_approval",
            target_uri=proposal.target_uri,
            proposal_id=proposal_id,
            message=f"Queued for human approval as {proposal_id}",
        )

    def approve_proposal(self, proposal_id: str, *, actor: str) -> WriteResult:
        prop_path = self._store.proposals_dir / f"{proposal_id}.json"
        if not prop_path.is_file():
            return WriteResult(status="not_found", target_uri="", proposal_id=proposal_id, message="Proposal not found")

        record = json.loads(prop_path.read_text(encoding="utf-8"))
        proposal = WriteProposal.from_dict(record["proposal"])
        if record.get("status") != "pending":
            return WriteResult(
                status="conflict",
                target_uri=proposal.target_uri,
                proposal_id=proposal_id,
                message=f"Proposal already in status '{record.get('status')}'",
            )

        parsed = parse_memory_uri(proposal.target_uri)
        target_path = self._store.resolve_uri_path(parsed)
        existing = target_path.read_text(encoding="utf-8") if target_path.is_file() else ""
        current_sha = sha256_text(existing) if target_path.is_file() else None
        expected_sha = proposal.base_sha256 if proposal.base_sha256 is not None else record.get("expected_base_sha256")
        if expected_sha is not None and current_sha != expected_sha:
            return WriteResult(
                status="conflict",
                target_uri=proposal.target_uri,
                proposal_id=proposal_id,
                message=f"Target changed since proposal creation (expected {expected_sha}, got {current_sha})",
            )

        new_text = _apply_op(existing, proposal)
        budget_err = self._check_budget(parsed, new_text)
        if budget_err:
            return WriteResult(
                status="rejected_budget",
                target_uri=proposal.target_uri,
                proposal_id=proposal_id,
                message=budget_err,
            )

        atomic_write_text(target_path, new_text)
        record["status"] = "approved"
        record["decided_at"] = utc_now_iso()
        record["decided_by"] = actor
        atomic_write_text(prop_path, json.dumps(record, ensure_ascii=False, indent=2) + "\n")
        self._refresh_index_safe(parsed, target_path)
        if self._on_commit is not None:
            self._on_commit(f"memory_hub: approve {proposal_id} ({proposal.target_uri}) by {actor}")
        return WriteResult(
            status="applied",
            target_uri=proposal.target_uri,
            proposal_id=proposal_id,
            new_sha256=sha256_text(new_text),
            message=f"Approved by {actor} and applied atomically",
        )

    def reject_proposal(self, proposal_id: str, *, actor: str, reason: str = "") -> WriteResult:
        prop_path = self._store.proposals_dir / f"{proposal_id}.json"
        if not prop_path.is_file():
            return WriteResult(status="not_found", target_uri="", proposal_id=proposal_id, message="Proposal not found")
        record = json.loads(prop_path.read_text(encoding="utf-8"))
        proposal = WriteProposal.from_dict(record["proposal"])
        record["status"] = "rejected"
        record["decided_at"] = utc_now_iso()
        record["decided_by"] = actor
        record["reject_reason"] = reason
        atomic_write_text(prop_path, json.dumps(record, ensure_ascii=False, indent=2) + "\n")
        return WriteResult(
            status="applied",
            target_uri=proposal.target_uri,
            proposal_id=proposal_id,
            message=f"Rejected by {actor}",
        )

    def list_proposals(self, *, status: str | None = "pending") -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for path in sorted(self._store.proposals_dir.glob("prop_*.json")):
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if status is None or record.get("status") == status:
                results.append(record)
        return results
