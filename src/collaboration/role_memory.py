from __future__ import annotations

import hashlib
import json
import re
import uuid
from pathlib import Path
from typing import Any

from collaboration.models import (
    MemoryMountPlan,
    MemoryMountSlice,
    RoleMemoryProposal,
    utc_now_iso,
)

_ROLE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_PROJECT_KEY_RE = re.compile(r"^[A-Za-z0-9._-]+$")


class CASConflictError(ValueError):
    pass


def _sha256_text(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _validate_role_id(role_id: str) -> str:
    cleaned = role_id.strip()
    if not _ROLE_ID_RE.match(cleaned):
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
    def __init__(self, root_dir: Path | str) -> None:
        self._root = Path(root_dir).expanduser().resolve()
        self._roles_dir = self._root / "roles"
        self._proposals_dir = self._root / "proposals" / "roles"
        self._roles_dir.mkdir(parents=True, exist_ok=True)
        self._proposals_dir.mkdir(parents=True, exist_ok=True)

    @property
    def root(self) -> Path:
        return self._root

    def _target_path(self, role_id: str, rel_path: str) -> Path:
        role = _validate_role_id(role_id)
        rel = _validate_rel_path(rel_path)
        target = (self._roles_dir / role / rel).resolve()
        target.relative_to(self._roles_dir.resolve())
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
        role = _validate_role_id(role_id)
        rel = _validate_rel_path(rel_path)
        _, current_sha = self.read_role_doc(role, rel)
        expected_sha = current_sha if base_sha256 is None else base_sha256
        if expected_sha != current_sha:
            raise CASConflictError(
                f"CAS mismatch for role://{role}/{rel}: expected {expected_sha}, actual {current_sha}"
            )
        proposal = RoleMemoryProposal(
            proposal_id=f"rprop-{uuid.uuid4().hex[:12]}",
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
        for path in sorted(self._proposals_dir.glob("*.json")):
            item = self._load_proposal(path)
            if status is None or item.status == status:
                out.append(item)
        return out

    def get_proposal(self, proposal_id: str) -> RoleMemoryProposal:
        path = self._proposals_dir / f"{proposal_id}.json"
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
        proposal = self.get_proposal(proposal_id)
        if proposal.status != "pending":
            raise ValueError(f"Proposal {proposal_id} is already {proposal.status}")
        target = self._target_path(proposal.role_id, proposal.rel_path)
        _, current_sha = self.read_role_doc(proposal.role_id, proposal.rel_path)
        if proposal.base_sha256 != current_sha:
            raise CASConflictError(
                f"Cannot approve {proposal_id}: role://{proposal.role_id}/{proposal.rel_path} changed since proposal"
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(proposal.content, encoding="utf-8")
        updated = RoleMemoryProposal(
            proposal_id=proposal.proposal_id,
            role_id=proposal.role_id,
            rel_path=proposal.rel_path,
            content=proposal.content,
            reason=proposal.reason,
            proposed_by=proposal.proposed_by,
            base_sha256=proposal.base_sha256,
            status="approved",
            created_at=proposal.created_at,
            reviewed_by=reviewed_by,
            reviewed_at=utc_now_iso(),
            review_note=review_note,
        )
        self._save_proposal(updated)
        return updated

    def reject_proposal(
        self,
        proposal_id: str,
        *,
        reviewed_by: str,
        review_note: str = "",
    ) -> RoleMemoryProposal:
        proposal = self.get_proposal(proposal_id)
        if proposal.status != "pending":
            raise ValueError(f"Proposal {proposal_id} is already {proposal.status}")
        updated = RoleMemoryProposal(
            proposal_id=proposal.proposal_id,
            role_id=proposal.role_id,
            rel_path=proposal.rel_path,
            content=proposal.content,
            reason=proposal.reason,
            proposed_by=proposal.proposed_by,
            base_sha256=proposal.base_sha256,
            status="rejected",
            created_at=proposal.created_at,
            reviewed_by=reviewed_by,
            reviewed_at=utc_now_iso(),
            review_note=review_note,
        )
        self._save_proposal(updated)
        return updated

    def _save_proposal(self, proposal: RoleMemoryProposal) -> None:
        path = self._proposals_dir / f"{proposal.proposal_id}.json"
        path.write_text(json.dumps(proposal.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")

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
    if not cleaned_project or not _PROJECT_KEY_RE.match(cleaned_project):
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
        if not ro_clean or not _PROJECT_KEY_RE.match(ro_clean):
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
