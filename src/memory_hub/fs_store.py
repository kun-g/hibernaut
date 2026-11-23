from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from memory_hub.models import (
    BaselineBlock,
    BaselineBudgetConfig,
    BaselineBundle,
    MemoryDoc,
    MemoryHubConfig,
    estimate_tokens,
    sha256_text,
    utc_now_iso,
)
from memory_hub.partition import ParsedUri, parse_memory_uri, safe_join, sanitize_segment
from memory_hub.ports import AuditHookPort

BASELINE_PROJECT_FILES = ("instructions.md", "index.md", "reported.md")
_HEADING_RE = re.compile(r"^(#{1,6})\s+(.*)$")


def atomic_write_text(target: Path, content: str) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(target)


def truncate_to_tokens(text: str, max_tokens: int, *, marker: str = "\n[TRUNCATED: budget_exceeded]") -> tuple[str, bool]:
    if estimate_tokens(text) <= max_tokens:
        return text, False
    marker_bytes = len(marker.encode("utf-8"))
    allowed_bytes = max(0, max_tokens * 3 - marker_bytes)
    clipped = text.encode("utf-8")[:allowed_bytes].decode("utf-8", errors="ignore").rstrip()
    return f"{clipped}{marker}", True


def extract_markdown_section(content: str, heading_query: str) -> str | None:
    needle = heading_query.strip().lstrip("#").strip().lower()
    if not needle:
        return None
    lines = content.splitlines()
    start_idx: int | None = None
    start_level = 0
    for idx, line in enumerate(lines):
        m = _HEADING_RE.match(line)
        if not m:
            continue
        level = len(m.group(1))
        title = m.group(2).strip().lower()
        if start_idx is None:
            if title == needle or needle in title:
                start_idx = idx
                start_level = level
        elif level <= start_level:
            return "\n".join(lines[start_idx:idx]).strip()
    if start_idx is not None:
        return "\n".join(lines[start_idx:]).strip()
    return None


def replace_markdown_section(content: str, heading_query: str, new_body: str) -> str:
    needle = heading_query.strip().lstrip("#").strip().lower()
    lines = content.splitlines()
    start_idx: int | None = None
    end_idx: int = len(lines)
    start_level = 0
    for idx, line in enumerate(lines):
        m = _HEADING_RE.match(line)
        if not m:
            continue
        level = len(m.group(1))
        title = m.group(2).strip().lower()
        if start_idx is None:
            if title == needle:
                start_idx = idx
                start_level = level
        elif level <= start_level:
            end_idx = idx
            break
    if start_idx is None:
        sep = "\n\n" if content.strip() else ""
        heading_line = heading_query if heading_query.startswith("#") else f"## {heading_query}"
        return f"{content.rstrip()}{sep}{heading_line}\n\n{new_body.strip()}\n"
    before = lines[: start_idx + 1]
    after = lines[end_idx:]
    rebuilt = "\n".join(before) + "\n\n" + new_body.strip() + "\n"
    if after:
        rebuilt += "\n" + "\n".join(after).lstrip("\n") + "\n"
    return rebuilt


class MarkdownFileStore:
    def __init__(self, config: MemoryHubConfig, *, audit: AuditHookPort | None = None) -> None:
        self._config = config
        self._audit = audit
        self._root = Path(config.memory_root).expanduser().resolve()
        self._global_dir = self._root / "global"
        self._shared_notes_dir = self._root / "shared" / "notes"
        self._projects_dir = self._root / "projects"
        self._proposals_dir = self._root / "proposals"
        for d in (self._global_dir, self._shared_notes_dir, self._projects_dir, self._proposals_dir):
            d.mkdir(parents=True, exist_ok=True)
        global_instr = self._global_dir / "instructions.md"
        if not global_instr.exists():
            atomic_write_text(global_instr, "")

    @property
    def root(self) -> Path:
        return self._root

    @property
    def proposals_dir(self) -> Path:
        return self._proposals_dir

    @property
    def projects_dir(self) -> Path:
        return self._projects_dir

    @property
    def shared_notes_dir(self) -> Path:
        return self._shared_notes_dir

    def ensure_project(self, project_key: str) -> Path:
        safe_pk = sanitize_segment(project_key, field_name="project_key")
        proj_dir = self._projects_dir / safe_pk
        (proj_dir / "notes" / "postmortem").mkdir(parents=True, exist_ok=True)
        (proj_dir / "notes" / "reported_archive").mkdir(parents=True, exist_ok=True)
        (proj_dir / "notes" / "progress_archive").mkdir(parents=True, exist_ok=True)
        (proj_dir / "progress").mkdir(parents=True, exist_ok=True)
        for fname in BASELINE_PROJECT_FILES:
            target = proj_dir / fname
            if not target.exists():
                atomic_write_text(target, "")
        return proj_dir

    def resolve_uri_path(self, parsed: ParsedUri) -> Path:
        if parsed.scheme == "global":
            return safe_join(self._global_dir, parsed.rel_path)
        if parsed.scheme == "shared":
            return safe_join(self._root / "shared", parsed.rel_path)
        if parsed.scheme == "proj":
            proj_dir = self.ensure_project(parsed.scope)
            return safe_join(proj_dir, parsed.rel_path)
        raise ValueError(f"MarkdownFileStore cannot resolve scheme: {parsed.scheme}")

    def set_global_instructions(self, content: str) -> None:
        atomic_write_text(self._global_dir / "instructions.md", content)

    def load_baseline(self, project_key: str) -> BaselineBundle:
        safe_pk = sanitize_segment(project_key, field_name="project_key")
        proj_dir = self.ensure_project(safe_pk)
        budget: BaselineBudgetConfig = self._config.budget_for(safe_pk)

        raw_items: list[tuple[str, str, int, Path]] = [
            ("global://instructions.md", "stable", budget.global_instructions, self._global_dir / "instructions.md"),
            (f"proj://{safe_pk}/instructions.md", "stable", budget.project_instructions, proj_dir / "instructions.md"),
            (f"proj://{safe_pk}/index.md", "stable", budget.project_index, proj_dir / "index.md"),
            (f"proj://{safe_pk}/reported.md", "volatile", budget.project_reported, proj_dir / "reported.md"),
        ]

        remaining_total = budget.total_max_tokens
        stable_blocks: list[BaselineBlock] = []
        volatile_blocks: list[BaselineBlock] = []
        truncated_uris: list[str] = []

        for uri, tier, per_file_cap, path in raw_items:
            raw = path.read_text(encoding="utf-8") if path.is_file() else ""
            effective_cap = min(per_file_cap, remaining_total)
            if effective_cap <= 0 and raw:
                text = "[TRUNCATED: total_budget_exceeded]"
                was_trunc = True
            else:
                text, was_trunc = truncate_to_tokens(raw, effective_cap)
            est = estimate_tokens(text)
            remaining_total = max(0, remaining_total - est)
            if was_trunc:
                truncated_uris.append(uri)
            block = BaselineBlock(
                uri=uri,
                tier="stable" if tier == "stable" else "volatile",
                content=text,
                sha256=sha256_text(text),
                est_tokens=est,
                truncated=was_trunc,
            )
            if tier == "stable":
                stable_blocks.append(block)
            else:
                volatile_blocks.append(block)

        total_est = sum(b.est_tokens for b in (*stable_blocks, *volatile_blocks))
        stable_hash = sha256_text("\n---\n".join(b.sha256 for b in stable_blocks))

        if truncated_uris and self._audit is not None:
            self._audit.emit(
                "memory.budget_exceeded",
                "truncated",
                {
                    "project_key": safe_pk,
                    "truncated_blocks": truncated_uris,
                    "total_est_tokens": total_est,
                    "total_max_tokens": budget.total_max_tokens,
                },
                severity="warn",
            )

        return BaselineBundle(
            project_key=safe_pk,
            stable_prefix=tuple(stable_blocks),
            volatile_tail=tuple(volatile_blocks),
            total_est_tokens=total_est,
            truncated_blocks=tuple(truncated_uris),
            stable_prefix_sha256=stable_hash,
        )

    def read_doc(self, uri: str, *, heading: str | None = None, max_bytes: int | None = None) -> MemoryDoc:
        parsed = parse_memory_uri(uri)
        target = self.resolve_uri_path(parsed)
        if not target.is_file():
            raise FileNotFoundError(f"Memory file not found: {uri}")
        raw = target.read_text(encoding="utf-8")
        full_sha = sha256_text(raw)
        selected = raw
        if heading:
            section = extract_markdown_section(raw, heading)
            if section is None:
                raise KeyError(f"Heading '{heading}' not found in {uri}")
            selected = section
        truncated = False
        if max_bytes is not None and len(selected.encode("utf-8")) > max_bytes:
            selected = selected.encode("utf-8")[:max_bytes].decode("utf-8", errors="ignore") + "\n[TRUNCATED]"
            truncated = True
        mtime = datetime.fromtimestamp(target.stat().st_mtime, tz=timezone.utc).replace(microsecond=0)
        return MemoryDoc(
            uri=uri,
            content=selected,
            sha256=full_sha,
            est_tokens=estimate_tokens(selected),
            updated_at=mtime.isoformat().replace("+00:00", "Z"),
            readonly=(parsed.scheme == "global"),
            heading=heading,
            truncated=truncated,
        )

    def roll_reported_if_needed(self, project_key: str) -> int:
        safe_pk = sanitize_segment(project_key, field_name="project_key")
        proj_dir = self.ensure_project(safe_pk)
        budget = self._config.budget_for(safe_pk).project_reported
        reported_path = proj_dir / "reported.md"
        raw = reported_path.read_text(encoding="utf-8") if reported_path.is_file() else ""
        if estimate_tokens(raw) <= budget:
            return 0

        entries = [e.strip() for e in re.split(r"\n(?=- \d{4}-\d{2}-\d{2}|\#\# )", raw.strip()) if e.strip()]
        if len(entries) <= 1:
            return 0

        kept: list[str] = []
        archived: list[str] = []
        # Preserve newest entries at the bottom; archive oldest from the top.
        for entry in reversed(entries):
            candidate = "\n".join(reversed([*kept, entry]))
            if estimate_tokens(candidate) <= budget or not kept:
                kept.append(entry)
            else:
                archived.append(entry)
        if not archived:
            return 0

        archived.reverse()
        kept.reverse()
        ym = utc_now_iso()[:7]
        archive_path = proj_dir / "notes" / "reported_archive" / f"{ym}.md"
        existing_archive = archive_path.read_text(encoding="utf-8") if archive_path.is_file() else ""
        sep = "\n\n" if existing_archive.strip() else ""
        atomic_write_text(archive_path, f"{existing_archive.rstrip()}{sep}" + "\n".join(archived) + "\n")
        atomic_write_text(reported_path, "\n".join(kept) + "\n")
        return len(archived)

    def iter_indexable_files(self, scopes: tuple[str, ...] | None = None) -> Iterator[tuple[str, str, str, Path]]:
        if scopes is None:
            project_dirs = sorted(p for p in self._projects_dir.iterdir() if p.is_dir())
        else:
            project_dirs = [self.ensure_project(s) for s in scopes]

        for proj_dir in project_dirs:
            pk = proj_dir.name
            for path in sorted(proj_dir.rglob("*.md")):
                if not path.is_file():
                    continue
                rel = path.relative_to(proj_dir).as_posix()
                yield f"proj://{pk}/{rel}", pk, "project", path

        for path in sorted(self._shared_notes_dir.rglob("*.md")):
            if not path.is_file():
                continue
            rel = path.relative_to(self._root / "shared").as_posix()
            yield f"shared://{rel}", "shared", "shared", path
