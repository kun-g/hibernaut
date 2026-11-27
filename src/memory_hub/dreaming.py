from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, UTC
from collections.abc import Callable

from memory_hub.fs_store import MarkdownFileStore, atomic_write_text
from memory_hub.governance import WriteGovernanceGate
from memory_hub.index_fts5 import DualFTSIndex
from memory_hub.models import GCReport, MemoryHubConfig, WriteProposal, WriteResult, estimate_tokens, utc_now_iso
from memory_hub.partition import sanitize_segment

_INDEX_PATH_RE = re.compile(r"`([^`]+\.md)`")


def _parse_iso(ts: str) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


class CompactionAndDreaming:
    def __init__(
        self,
        store: MarkdownFileStore,
        index: DualFTSIndex,
        gate: WriteGovernanceGate,
        config: MemoryHubConfig,
        *,
        on_commit: Callable[[str], bool] | None = None,
    ) -> None:
        self._store = store
        self._index = index
        self._gate = gate
        self._config = config
        self._on_commit = on_commit

    def run_mechanical_gc(
        self,
        project_key: str,
        *,
        closed_work_keys: set[str] | None = None,
        now: datetime | None = None,
    ) -> GCReport:
        pk = sanitize_segment(project_key, field_name="project_key")
        proj_dir = self._store.ensure_project(pk)
        cur_now = now or datetime.now(UTC)

        archived_reported = self._store.roll_reported_if_needed(pk)

        archived_progress: list[str] = []
        progress_dir = proj_dir / "progress"
        archive_dir = proj_dir / "notes" / "progress_archive"
        archive_dir.mkdir(parents=True, exist_ok=True)
        cutoff = cur_now - timedelta(days=self._config.progress_archive_days)

        for pfile in sorted(progress_dir.glob("*.md")):
            mtime = datetime.fromtimestamp(pfile.stat().st_mtime, tz=UTC)
            stem = pfile.stem
            is_closed = closed_work_keys is not None and stem in closed_work_keys
            if (is_closed and mtime <= cutoff) or (closed_work_keys is None and mtime <= cutoff):
                dest = archive_dir / pfile.name
                pfile.replace(dest)
                archived_progress.append(f"progress/{pfile.name}")

        # Calibrate index.md: prune dangling links & register missing notes
        index_path = proj_dir / "index.md"
        raw_index = index_path.read_text(encoding="utf-8") if index_path.is_file() else ""
        kept_lines: list[str] = []
        referenced: set[str] = set()
        pruned = 0
        for line in raw_index.splitlines():
            m = _INDEX_PATH_RE.search(line)
            if m:
                rel = m.group(1).lstrip("/")
                if not (proj_dir / rel).is_file():
                    pruned += 1
                    continue
                referenced.add(rel)
            kept_lines.append(line)

        added = 0
        budget_index = self._config.budget_for(pk).project_index
        for nfile in sorted((proj_dir / "notes").rglob("*.md")):
            rel = nfile.relative_to(proj_dir).as_posix()
            if "/reported_archive/" in f"/{rel}" or "/progress_archive/" in f"/{rel}":
                continue
            if rel in referenced:
                continue
            first_line = ""
            for ln in nfile.read_text(encoding="utf-8", errors="replace").splitlines():
                stripped = ln.strip().lstrip("#").strip()
                if stripped and stripped != "---":
                    first_line = stripped[:100]
                    break
            candidate_line = f"- `{rel}` | {first_line or 'note'}"
            trial = "\n".join([*kept_lines, candidate_line]).strip() + "\n"
            if estimate_tokens(trial) <= budget_index:
                kept_lines.append(candidate_line)
                referenced.add(rel)
                added += 1

        if pruned > 0 or added > 0:
            atomic_write_text(index_path, ("\n".join(kept_lines).strip() + "\n") if kept_lines else "")

        # Expire stale pending proposals > proposal_ttl_days
        expired_ids: list[str] = []
        prop_cutoff = cur_now - timedelta(days=self._config.proposal_ttl_days)
        for ppath in sorted(self._store.proposals_dir.glob("prop_*.json")):
            try:
                rec = json.loads(ppath.read_text(encoding="utf-8"))
            except Exception:
                continue
            if rec.get("status") != "pending":
                continue
            created = _parse_iso(str(rec.get("created_at") or ""))
            if created is not None and created <= prop_cutoff:
                rec["status"] = "expired"
                rec["decided_at"] = utc_now_iso()
                atomic_write_text(ppath, json.dumps(rec, ensure_ascii=False, indent=2) + "\n")
                expired_ids.append(str(rec.get("proposal_id") or ppath.stem))

        self._index.sync_project_domain(scopes=(pk,))

        committed = False
        changed = bool(archived_reported or archived_progress or pruned or added or expired_ids)
        if changed and self._on_commit is not None:
            committed = self._on_commit(f"memory_hub: mechanical gc ({pk})")

        return GCReport(
            project_key=pk,
            archived_reported_entries=archived_reported,
            archived_progress_files=tuple(archived_progress),
            pruned_index_links=pruned,
            added_index_links=added,
            expired_proposals=tuple(expired_ids),
            git_committed=committed,
        )

    def submit_dreaming_proposal(
        self,
        project_key: str,
        *,
        distilled_rule: str,
        rationale: str,
        supersedes: tuple[str, ...] = (),
        evidence_ref: str | None = None,
    ) -> WriteResult:
        pk = sanitize_segment(project_key, field_name="project_key")
        return self._gate.propose_write(
            WriteProposal(
                target_uri=f"proj://{pk}/instructions.md",
                op="append",
                content=distilled_rule.strip(),
                rationale=f"[Dreaming] {rationale}",
                supersedes=supersedes,
                evidence_ref=evidence_ref,
                author="dreaming",
                force_new=bool(supersedes),
            )
        )
