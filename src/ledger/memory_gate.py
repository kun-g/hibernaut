"""Post-mortem -> Memory anti-pollution gate (Locked Q4 4.2)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

from ledger.models import ErrorKind, PostMortemCandidate, new_ledger_id, utc_now_iso

_FORBIDDEN_TRANSIENT_KINDS = frozenset({"rate_limit", "network", "crash"})
_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")


class MemoryGateError(ValueError):
    """Raised when a post-mortem rule violates anti-pollution policy."""


@dataclass(frozen=True, slots=True)
class MemoryPromotionResult:
    promoted: bool
    reason: str
    candidate_path: Path
    instructions_path: Path


def create_postmortem_candidate(
    project_memory_dir: str | Path,
    *,
    inbox_id: str,
    source_run_id: str,
    error_kind: ErrorKind,
    title: str,
    rule_text: str,
    invalidation_condition: str,
    recurrence_count: int = 1,
    candidate_id: str | None = None,
) -> tuple[PostMortemCandidate, Path]:
    """Stage a post-mortem candidate under notes/postmortem/ instead of polluting instructions.md."""
    if error_kind in _FORBIDDEN_TRANSIENT_KINDS:
        raise MemoryGateError(
            f"transient error_kind '{error_kind}' is forbidden from entering project Memory"
        )
    if not rule_text.strip():
        raise MemoryGateError("rule_text must not be empty")
    if not invalidation_condition.strip():
        raise MemoryGateError("invalidation_condition is required to prevent stale rules")

    cid = candidate_id or new_ledger_id("pm")
    if not _SAFE_ID_RE.match(cid):
        raise MemoryGateError(f"unsafe candidate_id: {cid!r}")

    cand = PostMortemCandidate(
        candidate_id=cid,
        inbox_id=inbox_id,
        source_run_id=source_run_id,
        error_kind=error_kind,
        title=title.strip(),
        rule_text=rule_text.strip(),
        invalidation_condition=invalidation_condition.strip(),
        recurrence_count=max(1, recurrence_count),
        status="pending",
        created_at=utc_now_iso(),
    )

    out_dir = Path(project_memory_dir) / "notes" / "postmortem"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{cid}.md"
    out_path.write_text(_render_candidate_md(cand), encoding="utf-8")
    return cand, out_path


def promote_to_instructions(
    candidate_path: str | Path,
    instructions_path: str | Path,
    *,
    approved: bool = False,
    min_recurrence: int = 2,
) -> MemoryPromotionResult:
    """Promote a staged candidate into instructions.md only if approved OR recurrence >= min_recurrence."""
    cpath = Path(candidate_path)
    ipath = Path(instructions_path)
    raw = cpath.read_text(encoding="utf-8")
    meta, body = _parse_frontmatter(raw)

    cid = meta.get("candidate_id", cpath.stem)
    error_kind = meta.get("error_kind", "unknown")
    if error_kind in _FORBIDDEN_TRANSIENT_KINDS:
        raise MemoryGateError(f"transient error_kind '{error_kind}' cannot be promoted")

    recurrence = int(meta.get("recurrence_count", "1"))
    if not approved and recurrence < min_recurrence:
        return MemoryPromotionResult(
            promoted=False,
            reason=f"recurrence_count={recurrence} < {min_recurrence} and approved=False",
            candidate_path=cpath,
            instructions_path=ipath,
        )

    existing = ipath.read_text(encoding="utf-8") if ipath.exists() else "# Project Instructions\n"
    marker = f"<!-- ledger:{cid} -->"
    if marker in existing:
        return MemoryPromotionResult(
            promoted=False,
            reason=f"candidate {cid} already promoted into instructions.md",
            candidate_path=cpath,
            instructions_path=ipath,
        )

    title = meta.get("title", cid)
    source_run_id = meta.get("source_run_id", "unknown")
    inv = meta.get("invalidation_condition", "")
    entry = (
        f"\n{marker}\n"
        f"- **{title}** (source: `{source_run_id}`, invalidation: {inv}): {body.strip()}\n"
    )
    ipath.parent.mkdir(parents=True, exist_ok=True)
    ipath.write_text(existing.rstrip() + "\n" + entry, encoding="utf-8")

    # Mark candidate status as promoted
    updated_raw = re.sub(r"(?m)^status:\s*\S+$", "status: promoted", raw, count=1)
    cpath.write_text(updated_raw, encoding="utf-8")
    return MemoryPromotionResult(
        promoted=True,
        reason="approved by human" if approved else f"recurrence_count={recurrence} >= {min_recurrence}",
        candidate_path=cpath,
        instructions_path=ipath,
    )


def _render_candidate_md(cand: PostMortemCandidate) -> str:
    return (
        "---\n"
        f"candidate_id: {cand.candidate_id}\n"
        f"inbox_id: {cand.inbox_id}\n"
        f"source_run_id: {cand.source_run_id}\n"
        f"error_kind: {cand.error_kind}\n"
        f"title: {cand.title}\n"
        f"recurrence_count: {cand.recurrence_count}\n"
        f"invalidation_condition: {cand.invalidation_condition}\n"
        f"status: {cand.status}\n"
        f"created_at: {cand.created_at}\n"
        "---\n\n"
        f"{cand.rule_text}\n"
    )


def _parse_frontmatter(raw: str) -> tuple[dict[str, str], str]:
    if not raw.startswith("---\n"):
        return {}, raw
    end = raw.find("\n---\n", 4)
    if end == -1:
        return {}, raw
    header = raw[4:end]
    body = raw[end + 5 :]
    meta: dict[str, str] = {}
    for line in header.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            meta[k.strip()] = v.strip()
    return meta, body
