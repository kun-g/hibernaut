from __future__ import annotations

import re

from memory_hub.governance import WriteGovernanceGate
from memory_hub.models import PostMortemCandidate, PostMortemResult, WriteProposal, utc_now_iso
from memory_hub.partition import sanitize_segment

TRANSIENT_ERROR_KINDS = frozenset({"rate_limit", "network", "crash"})
_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _build_slug(candidate: PostMortemCandidate) -> str:
    if candidate.slug:
        return sanitize_segment(candidate.slug, field_name="slug")
    raw = f"{candidate.error_kind}-{candidate.root_cause[:40]}".lower()
    slug = _SLUG_RE.sub("-", raw).strip("-")
    return slug or candidate.run_id


class PostMortemGate:
    def __init__(self, write_gate: WriteGovernanceGate) -> None:
        self._gate = write_gate

    def record(self, candidate: PostMortemCandidate) -> PostMortemResult:
        if candidate.error_kind.strip().lower() in TRANSIENT_ERROR_KINDS:
            return PostMortemResult(
                status="rejected_transient",
                message=f"Transient error_kind '{candidate.error_kind}' is forbidden from post-mortem promotion",
            )

        pk = sanitize_segment(candidate.project_key, field_name="project_key")
        day = utc_now_iso()[:10]
        slug = _build_slug(candidate)
        note_uri = f"proj://{pk}/notes/postmortem/{day}-{slug}.md"

        rule_lines = [ln.strip() for ln in candidate.proposed_rule.strip().splitlines() if ln.strip()][:3]
        concise_rule = "\n".join(rule_lines)
        should_propose = candidate.recurrence_count >= 2 or candidate.force_propose
        stage_status = "pending_promotion" if should_propose else "candidate"

        note_content = (
            f"---\n"
            f"status: {stage_status}\n"
            f"error_kind: {candidate.error_kind}\n"
            f"recurrence_count: {candidate.recurrence_count}\n"
            f"run_id: {candidate.run_id}\n"
            f"correlation_key: {candidate.correlation_key or ''}\n"
            f"updated_at: {utc_now_iso()}\n"
            f"---\n\n"
            f"# Post-Mortem: {candidate.error_kind}\n\n"
            f"## Root Cause\n\n{candidate.root_cause.strip()}\n\n"
            f"## Proposed Rule\n\n{concise_rule}\n"
        )

        stage_res = self._gate.propose_write(
            WriteProposal(
                target_uri=note_uri,
                op="upsert_file",
                content=note_content,
                rationale=f"Post-mortem candidate ({candidate.error_kind}, recurrence={candidate.recurrence_count})",
                evidence_ref=candidate.run_id,
                force_new=True,
            )
        )
        if stage_res.status != "applied":
            return PostMortemResult(
                status="staged",
                note_uri=note_uri,
                message=f"Failed to stage postmortem note: {stage_res.message}",
            )

        if not should_propose:
            return PostMortemResult(
                status="staged",
                note_uri=note_uri,
                message="Staged in notes/postmortem/; awaiting recurrence >= 2 or manual promotion",
            )

        promo_res = self._gate.propose_write(
            WriteProposal(
                target_uri=f"proj://{pk}/instructions.md",
                op="append",
                content=f"- [{candidate.error_kind}] {concise_rule} (ref: `{note_uri}`)",
                rationale=f"Promote recurring post-mortem ({candidate.error_kind}, recurrence={candidate.recurrence_count})",
                evidence_ref=candidate.run_id,
                force_new=True,
            )
        )
        return PostMortemResult(
            status="staged_and_proposed",
            note_uri=note_uri,
            proposal_id=promo_res.proposal_id,
            message="Staged in notes/postmortem/ and opened instructions.md approval proposal",
        )
