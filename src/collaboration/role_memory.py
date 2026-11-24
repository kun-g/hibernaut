from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import replace
from pathlib import Path
from typing import Any

from collaboration._files import ExclusiveLock, atomic_write_text
from collaboration.models import (
    MemoryMountPlan,
    MemoryMountSlice,
    RoleMemoryProposal,
    utc_now_iso,
)

_ROLE_ID_RE = re.compile(r"[a-z0-9][a-z0-9_-]*")
_PROJECT_KEY_RE = re.compile(r"(?!.*\.\.)[A-Za-z0-9._-]*[A-Za-z0-9_-][A-Za-z0-9._-]*")
_PROPOSAL_ID_RE = re.compile(r"rprop-[0-9a-f]{12}")


class CASConflictError(ValueError):
    pass


def _sha256_text(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _validate_role_id(role_id: str) -> str:
    cleaned = role_id.strip()
    if not _ROLE_ID_RE.fullmatch(cleaned):
        raise ValueError(f"Invalid role_id: {role_id!r}")
    return cleaned


def _validate_rel_path(rel_path: str) -> str:
    cleaned = rel_path.strip().lstrip("/")
    if not cleaned or "\\" in cleaned or ".." in cleaned.split("/"):
        raise ValueError(f"Invalid relative path: {rel_path!r}")
    if not cleaned.endswith(".md"):
        raise ValueError(f"Role skill memory files must be .md: {rel_path!r}")
    return cleaned


class RoleSkillMemoryStore:
    def __init__(
        self,
        root_dir: Path | str,
        *,
        # What holds off every other writer of root_dir; its own lock file unless the deployment has one already.
        exclusive: Callable[[], AbstractContextManager[None]] | None = None,
        on_approved: Callable[[RoleMemoryProposal], None] | None = None,
    ) -> None:
        self._root = Path(root_dir).expanduser().resolve()
        self._roles_dir = self._root / "roles"
        self._proposals_dir = self._root / "proposals" / "roles"
        self._roles_dir.mkdir(parents=True, exist_ok=True)
        self._proposals_dir.mkdir(parents=True, exist_ok=True)
        self._exclusive = exclusive or ExclusiveLock(self._root / ".roles.lock").hold
        self._on_approved = on_approved

    @property
    def root(self) -> Path:
        return self._root

    def _target_path(self, role_id: str, rel_path: str) -> Path:
        role = _validate_role_id(role_id)
        rel = _validate_rel_path(rel_path)
        role_dir = self._roles_dir / role
        target = (role_dir / rel).resolve()
        # Its own directory, not just anywhere in roles/: a link must not lead into another role's memory.
        if target == role_dir or not target.is_relative_to(role_dir):
            raise ValueError(f"Invalid relative path: {rel_path!r}")
        return target

    def read_role_doc(self, role_id: str, rel_path: str = "skills.md") -> tuple[str, str]:
        path = self._target_path(role_id, rel_path)
        if not path.is_file():
            return "", _sha256_text("")
        text = path.read_text(encoding="utf-8")
        return text, _sha256_text(text)

    def list_role_docs(self, role_id: str) -> list[str]:
        role = _validate_role_id(role_id)
        role_dir = self._roles_dir / role
        if not role_dir.is_dir():
            return []
        return sorted(
            str(p.relative_to(role_dir)).replace("\\", "/")
            for p in role_dir.rglob("*.md")
            if p.is_file()
        )

    def propose_skill_write(
        self,
        *,
        role_id: str,
        content: str,
        reason: str,
        proposed_by: str,
        rel_path: str = "skills.md",
        base_sha256: str | None = None,
    ) -> RoleMemoryProposal:
        with self._exclusive():
            return self._queue(
                f"rprop-{uuid.uuid4().hex[:12]}", role_id, rel_path, content, reason, proposed_by, base_sha256
            )

    def propose_skill_append(
        self,
        *,
        proposal_id: str,
        role_id: str,
        addition: str,
        reason: str,
        proposed_by: str,
        rel_path: str = "skills.md",
    ) -> RoleMemoryProposal:
        # The caller derives proposal_id from what it proposes, so proposing the same thing again changes nothing.
        with self._exclusive():
            try:
                return self.get_proposal(proposal_id)
            except KeyError:
                pass
            current, sha = self.read_role_doc(role_id, rel_path)
            content = f"{current.rstrip()}\n\n{addition.strip()}\n" if current.strip() else f"{addition.strip()}\n"
            return self._queue(proposal_id, role_id, rel_path, content, reason, proposed_by, sha)

    def _queue(
        self,
        proposal_id: str,
        role_id: str,
        rel_path: str,
        content: str,
        reason: str,
        proposed_by: str,
        base_sha256: str | None,
    ) -> RoleMemoryProposal:
        role, rel = _validate_role_id(role_id), _validate_rel_path(rel_path)
        if not content.strip():
            raise ValueError("a role skill proposal needs content")
        _, current_sha = self.read_role_doc(role, rel)
        if base_sha256 is not None and base_sha256 != current_sha:
            raise CASConflictError(
                f"CAS mismatch for role://{role}/{rel}: expected {base_sha256}, actual {current_sha}"
            )
        proposal = RoleMemoryProposal(
            proposal_id=proposal_id,
            role_id=role,
            rel_path=rel,
            content=content,
            reason=reason.strip() or "role skill update",
            proposed_by=proposed_by.strip() or "unknown",
            base_sha256=current_sha,
            status="pending",
            created_at=utc_now_iso(),
        )
        self._save_proposal(proposal)
        return proposal

    def list_proposals(self, *, status: str | None = None) -> list[RoleMemoryProposal]:
        out: list[RoleMemoryProposal] = []
        for path in sorted(self._proposals_dir.glob("rprop-*.json")):
            try:
                item = self._load_proposal(path)
            except (KeyError, TypeError, ValueError):
                continue
            if status is None or item.status == status:
                out.append(item)
        return out

    def get_proposal(self, proposal_id: str) -> RoleMemoryProposal:
        path = self._proposal_path(proposal_id)
        if not path.is_file():
            raise KeyError(f"Unknown role proposal: {proposal_id}")
        return self._load_proposal(path)

    def approve_proposal(
        self,
        proposal_id: str,
        *,
        reviewed_by: str,
        review_note: str = "",
    ) -> RoleMemoryProposal:
        with self._exclusive():
            proposal = self._pending(proposal_id)
            target = self._target_path(proposal.role_id, proposal.rel_path)
            _, current_sha = self.read_role_doc(proposal.role_id, proposal.rel_path)
            if current_sha == _sha256_text(proposal.content) != proposal.base_sha256:
                # Written by an approval that crashed before it recorded itself.
                pass
            elif proposal.base_sha256 != current_sha:
                raise CASConflictError(
                    f"Cannot approve {proposal_id}: role://{proposal.role_id}/{proposal.rel_path} "
                    "changed since proposal"
                )
            else:
                atomic_write_text(target, proposal.content)
            approved = replace(
                proposal, status="approved", reviewed_by=reviewed_by, reviewed_at=utc_now_iso(),
                review_note=review_note,
            )
            self._save_proposal(approved)
            if self._on_approved is not None:
                self._on_approved(approved)
            return approved

    def reject_proposal(
        self,
        proposal_id: str,
        *,
        reviewed_by: str,
        review_note: str = "",
    ) -> RoleMemoryProposal:
        with self._exclusive():
            rejected = replace(
                self._pending(proposal_id), status="rejected", reviewed_by=reviewed_by, reviewed_at=utc_now_iso(),
                review_note=review_note,
            )
            self._save_proposal(rejected)
            return rejected

    def _pending(self, proposal_id: str) -> RoleMemoryProposal:
        proposal = self.get_proposal(proposal_id)
        if proposal.status != "pending":
            raise ValueError(f"Proposal {proposal_id} is already {proposal.status}")
        return proposal

    def _proposal_path(self, proposal_id: str) -> Path:
        if not _PROPOSAL_ID_RE.fullmatch(proposal_id):
            raise KeyError(f"Unknown role proposal: {proposal_id}")
        return self._proposals_dir / f"{proposal_id}.json"

    def _save_proposal(self, proposal: RoleMemoryProposal) -> None:
        text = json.dumps(proposal.to_dict(), ensure_ascii=False, indent=2)
        atomic_write_text(self._proposal_path(proposal.proposal_id), text)

    def _load_proposal(self, path: Path) -> RoleMemoryProposal:
        raw: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
        return RoleMemoryProposal(
            proposal_id=raw["proposal_id"],
            role_id=raw["role_id"],
            rel_path=raw["rel_path"],
            content=raw["content"],
            reason=raw["reason"],
            proposed_by=raw["proposed_by"],
            base_sha256=raw["base_sha256"],
            status=raw["status"],
            created_at=raw["created_at"],
            reviewed_by=raw.get("reviewed_by"),
            reviewed_at=raw.get("reviewed_at"),
            review_note=raw.get("review_note", ""),
        )


def build_four_layer_mount_plan(
    *,
    agent_id: str,
    role_id: str,
    project_key: str,
    extra_readonly_projects: tuple[str, ...] = (),
    kb_mount_name: str = "pkb",
    total_budget_chars: int = 6500,
) -> MemoryMountPlan:
    role = _validate_role_id(role_id)
    cleaned_project = project_key.strip()
    if not cleaned_project or not _PROJECT_KEY_RE.fullmatch(cleaned_project):
        raise ValueError(f"Invalid project_key: {project_key!r}")
    if cleaned_project in extra_readonly_projects:
        raise ValueError(f"Project {cleaned_project!r} cannot be mounted both RW and RO")

    slices: list[MemoryMountSlice] = [
        MemoryMountSlice(
            uri=f"proj://{cleaned_project}/",
            layer="project",
            write_mode="rw",
            budget_chars=int(total_budget_chars * 0.45),
        ),
        MemoryMountSlice(
            uri=f"role://{role}/",
            layer="role_skill",
            write_mode="propose",
            budget_chars=int(total_budget_chars * 0.25),
        ),
        MemoryMountSlice(
            uri="shared://notes/",
            layer="shared",
            write_mode="ro",
            budget_chars=int(total_budget_chars * 0.15),
        ),
        MemoryMountSlice(
            uri=f"kb://{kb_mount_name}/",
            layer="kb",
            write_mode="ro",
            budget_chars=int(total_budget_chars * 0.15),
        ),
    ]
    for ro_proj in extra_readonly_projects:
        ro_clean = ro_proj.strip()
        if not ro_clean or not _PROJECT_KEY_RE.fullmatch(ro_clean):
            raise ValueError(f"Invalid readonly project_key: {ro_proj!r}")
        slices.append(
            MemoryMountSlice(
                uri=f"proj://{ro_clean}/",
                layer="project",
                write_mode="ro",
                budget_chars=0,
            )
        )

    rw_projects = [s for s in slices if s.layer == "project" and s.write_mode == "rw"]
    if len(rw_projects) != 1:
        raise ValueError("Invariant violated: exactly 1 RW project mount allowed")

    return MemoryMountPlan(
        agent_id=agent_id,
        role_id=role,
        project_key=cleaned_project,
        slices=tuple(slices),
        total_budget_chars=total_budget_chars,
    )
