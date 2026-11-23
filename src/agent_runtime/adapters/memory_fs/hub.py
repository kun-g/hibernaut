from __future__ import annotations

import fcntl
import os
import re
import threading
from collections.abc import Callable, Generator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from memory_hub import (
    AuditHookPort,
    BaselineBundle,
    GCReport,
    MemoryDoc,
    MemoryHub,
    MemoryHubConfig,
    PostMortemCandidate,
    PostMortemResult,
    WriteProposal,
    WriteResult,
)
from memory_hub.partition import sanitize_segment

_PROPOSAL_ID_RE = re.compile(r"prop_[0-9a-f]+")


class GovernedMemoryHub(MemoryHub):
    # MemoryHub reads, changes and replaces files without a lock and through one fixed temp name per file, so writers
    # of one memory root must take turns: Runs in threads of this process, its HTTP service, and the stdio MCP
    # companion process. search() stays outside, since indexing a large mount must not hold up writers; each index
    # keeps its own sync and query in one SQLite transaction, which GC and writers wait for.
    def __init__(self, config: MemoryHubConfig, *, audit: AuditHookPort | None = None) -> None:
        root = Path(config.memory_root).expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        self._thread_lock = threading.RLock()
        self._held = 0
        # Whether the deployment binds this inbox to a workdir of no project; every config generation sets its own.
        self.is_projectless: Callable[[str], bool] = lambda inbox_id: False
        self._lock_file = (root / ".memory_hub.lock").open("a")
        try:
            with self.exclusive():
                super().__init__(config, audit=audit)
        except BaseException:
            self._lock_file.close()
            raise

    @contextmanager
    def exclusive(self) -> Generator[None]:
        with self._thread_lock:
            if self._held == 0:
                fcntl.flock(self._lock_file, fcntl.LOCK_EX)
            self._held += 1
            try:
                yield
            finally:
                self._held -= 1
                if self._held == 0:
                    fcntl.flock(self._lock_file, fcntl.LOCK_UN)

    def close(self) -> None:
        with self._thread_lock:
            if not self._lock_file.closed:
                super().close()
                self._lock_file.close()

    def resolve_project_key(self, inbox_id: str) -> str:
        # MemoryHub reads <name>:<rest> as a queue of project <name>, which would give all sys:* inboxes, and a project
        # that happens to be called so, one memory.
        if inbox_id not in self.config.inbox_project_map and (
            inbox_id.strip().startswith("sys:") or self.is_projectless(inbox_id)
        ):
            return sanitize_segment(inbox_id, field_name="inbox_id")
        return super().resolve_project_key(inbox_id)

    def lay_out(self, project_key: str) -> None:
        # OSError when the key is no directory name.
        with self.exclusive():
            self._store.ensure_project(project_key)

    def is_laid_out(self, folder: Path) -> bool:
        # By Memory Hub itself, unlike the directories of the per-inbox_id layout before it.
        return os.path.isdir(folder / "notes" / "reported_archive")

    def project_dir(self, project_key: str) -> Path:
        return self.root / "projects" / project_key

    def project_keys(self) -> list[str]:
        return sorted(p.name for p in (self.root / "projects").iterdir() if self.is_laid_out(p))

    def set_global_instructions(self, content: str) -> None:
        with self.exclusive():
            super().set_global_instructions(content)

    def load_baseline(self, project_key: str) -> BaselineBundle:
        with self.exclusive():
            return super().load_baseline(project_key)

    def read_on_demand(self, uri: str, *, heading: str | None = None, max_bytes: int | None = None) -> MemoryDoc:
        # Reading a proj:// path lays out that project's directory.
        with self.exclusive():
            return super().read_on_demand(uri, heading=heading, max_bytes=max_bytes)

    def propose_write(self, proposal: WriteProposal) -> WriteResult:
        with self.exclusive():
            return super().propose_write(proposal)

    def approve_proposal(self, proposal_id: str, *, actor: str) -> WriteResult:
        # The id becomes a file name in proposals/.
        if not _PROPOSAL_ID_RE.fullmatch(proposal_id):
            return _not_found(proposal_id)
        with self.exclusive():
            return super().approve_proposal(proposal_id, actor=actor)

    def reject_proposal(self, proposal_id: str, *, actor: str, reason: str = "") -> WriteResult:
        if not _PROPOSAL_ID_RE.fullmatch(proposal_id):
            return _not_found(proposal_id)
        with self.exclusive():
            decided = next((r for r in self.list_proposals(status=None)
                            if r.get("proposal_id") == proposal_id and r.get("status") != "pending"), None)
            if decided is not None:
                # MemoryHub would relabel a proposal that is already applied as rejected.
                return WriteResult(status="conflict", target_uri=str(decided["proposal"]["target_uri"]),
                                   proposal_id=proposal_id, message=f"Proposal already in status '{decided['status']}'")
            return super().reject_proposal(proposal_id, actor=actor, reason=reason)

    def record_postmortem(self, candidate: PostMortemCandidate) -> PostMortemResult:
        with self.exclusive():
            return super().record_postmortem(candidate)

    def run_mechanical_gc(
        self,
        project_key: str,
        *,
        closed_work_keys: set[str] | None = None,
        now: datetime | None = None,
    ) -> GCReport:
        with self.exclusive():
            return super().run_mechanical_gc(project_key, closed_work_keys=closed_work_keys, now=now)

    def submit_dreaming_proposal(
        self,
        project_key: str,
        *,
        distilled_rule: str,
        rationale: str,
        supersedes: tuple[str, ...] = (),
        evidence_ref: str | None = None,
    ) -> WriteResult:
        with self.exclusive():
            return super().submit_dreaming_proposal(
                project_key, distilled_rule=distilled_rule, rationale=rationale, supersedes=supersedes,
                evidence_ref=evidence_ref,
            )


def _not_found(proposal_id: str) -> WriteResult:
    return WriteResult(status="not_found", target_uri="", proposal_id=proposal_id, message="Proposal not found")
