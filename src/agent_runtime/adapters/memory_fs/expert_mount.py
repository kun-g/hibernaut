from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any

from agent_runtime.adapters.memory_fs.hub import GovernedMemoryHub
from agent_runtime.adapters.memory_fs.tool_args import optional, required
from collaboration.models import MemoryMountPlan, MemoryMountSlice, MemoryWriteMode, RoleMemoryProposal
from collaboration.role_memory import RoleSkillMemoryStore
from memory_hub import BaselineBlock, BaselineBundle, MemoryDoc
from memory_hub.models import estimate_tokens, sha256_text
from memory_hub.partition import ParsedUri

ROLE_SCHEME = "role://"
_SKILLS_FILE = "skills.md"
_TRUNCATED = "\n[TRUNCATED: mount budget]"
_MODES: dict[MemoryWriteMode, str] = {
    "rw": "read and write", "propose": "read; changes are proposed and wait for a person", "ro": "read only",
}


def open_role_store(hub: GovernedMemoryHub) -> RoleSkillMemoryStore:
    # roles/ lies in the memory root, so its writers take turns with Memory Hub's and share its commits.
    def committed(proposal: RoleMemoryProposal) -> None:
        hub.commit(
            f"memory_hub: approve {proposal.proposal_id} (role://{proposal.role_id}/{proposal.rel_path}) "
            f"by {proposal.reviewed_by}"
        )

    return RoleSkillMemoryStore(hub.root, exclusive=hub.exclusive, on_approved=committed)


def is_role_uri(path: str) -> bool:
    return path.startswith(ROLE_SCHEME)


@dataclass(frozen=True, slots=True)
class ExpertMount:
    # What the Runs of one drafted expert's inbox see of the memory root, and how they may change it.
    plan: MemoryMountPlan
    roles: RoleSkillMemoryStore

    def mode(self, parsed: ParsedUri) -> MemoryWriteMode:
        if parsed.scheme == "global":
            return "ro"
        at = parsed.rel_path if parsed.scheme == "shared" else f"{parsed.scope}/{parsed.rel_path}"
        return self._slice(f"{parsed.scheme}://{at}").write_mode

    def shows(self, uri: str) -> bool:
        return any(uri.startswith(s.uri) for s in self.plan.slices)

    def role_path(self, uri: str) -> str:
        return uri.removeprefix(self._slice(uri, role=True).uri)

    def _slice(self, uri: str, *, role: bool = False) -> MemoryMountSlice:
        found = next(
            (s for s in self.plan.slices if (s.layer == "role_skill") is role and uri.startswith(s.uri)), None
        )
        if found is None:
            raise PermissionError(f"{uri} is outside the memory mounted for {self.plan.agent_id}")
        return found

    # --- role memory ---

    def read(self, uri: str) -> MemoryDoc:
        rel = self.role_path(uri)
        if rel not in self.roles.list_role_docs(self.plan.role_id):
            raise FileNotFoundError(f"Memory document not found: {uri}")
        text, sha = self.roles.read_role_doc(self.plan.role_id, rel)
        return MemoryDoc(
            uri=uri, content=text, sha256=sha, est_tokens=estimate_tokens(text), updated_at="", readonly=False
        )

    def read_document(self, args: Mapping[str, Any]) -> dict[str, Any]:
        if optional(args, "heading") is not None or args.get("max_bytes") is not None:
            raise ValueError("a role:// document is read whole; leave heading and max_bytes out")
        return self.read(required(args, "uri")).to_dict()

    def write(self, uri: str, content: str) -> None:
        self._queue(uri, content, reason="memory write", actor=self.plan.agent_id, base_sha256=None)

    def propose(self, uri: str, args: Mapping[str, Any], *, actor: str) -> dict[str, Any]:
        content, op = args.get("content"), required(args, "op")
        if not isinstance(content, str):
            raise ValueError("content must be a string")
        base = optional(args, "base_sha256")
        if op == "append":
            current, sha = self.roles.read_role_doc(self.plan.role_id, self.role_path(uri))
            base = sha if base is None else base
            content = f"{current.rstrip()}\n\n{content.strip()}\n" if current.strip() else f"{content.strip()}\n"
        elif op != "upsert_file":
            raise ValueError("a role:// document takes op upsert_file or append")
        queued = self._queue(
            uri, content, reason=required(args, "rationale"), actor=actor or self.plan.agent_id, base_sha256=base
        )
        return {
            "status": "pending_approval", "target_uri": uri, "proposal_id": queued.proposal_id,
            "message": f"Queued for human approval as {queued.proposal_id}",
        }

    def _queue(
        self, uri: str, content: str, *, reason: str, actor: str, base_sha256: str | None
    ) -> RoleMemoryProposal:
        return self.roles.propose_skill_write(
            role_id=self.plan.role_id, rel_path=self.role_path(uri), content=content, reason=reason,
            proposed_by=actor, base_sha256=base_sha256,
        )

    # --- startup context ---

    def extend(self, bundle: BaselineBundle) -> BaselineBundle:
        project = next(s for s in self.plan.slices if s.write_mode == "rw")
        role = next(s for s in self.plan.slices if s.layer == "role_skill")
        left = project.budget_chars

        def within_budget(block: BaselineBlock) -> BaselineBlock:
            nonlocal left
            if not block.uri.startswith(project.uri):
                return block
            text, cut = _clipped(block.content, left)
            left -= len(text)
            return _block(block, text, block.truncated or cut)

        stable = [within_budget(b) for b in bundle.stable_prefix]
        volatile = [within_budget(b) for b in bundle.volatile_tail]
        skills, cut = _clipped(self.roles.read_role_doc(self.plan.role_id, _SKILLS_FILE)[0], role.budget_chars)
        blank = BaselineBlock(uri="", tier="stable", content="", sha256="", est_tokens=0)
        stable += [
            _block(replace(blank, uri=f"{role.uri}{_SKILLS_FILE}"), skills, cut),
            _block(replace(blank, uri="mount://plan/mounts.md"), self._manifest(), False),
        ]
        return replace(
            bundle, stable_prefix=tuple(stable), volatile_tail=tuple(volatile),
            total_est_tokens=sum(b.est_tokens for b in (*stable, *volatile)),
            truncated_blocks=tuple(b.uri for b in (*stable, *volatile) if b.truncated),
            stable_prefix_sha256=sha256_text("\n---\n".join(b.sha256 for b in stable)),
        )

    def _manifest(self) -> str:
        lines = [f"You are `{self.plan.agent_id}`, drafted into project `{self.plan.project_key}`. Memory mounted here:"]
        lines += [f"- `{s.uri}`: {_MODES[s.write_mode]}" for s in self.plan.slices]
        return "\n".join(lines)


def _clipped(text: str, budget: int) -> tuple[str, bool]:
    if len(text) <= budget:
        return text, False
    return (text[: max(0, budget - len(_TRUNCATED))].rstrip() + _TRUNCATED)[: max(0, budget)], True


def _block(block: BaselineBlock, text: str, truncated: bool) -> BaselineBlock:
    return replace(
        block, content=text, sha256=sha256_text(text), est_tokens=estimate_tokens(text), truncated=truncated
    )
