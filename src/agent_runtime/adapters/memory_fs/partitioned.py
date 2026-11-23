from __future__ import annotations

import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from agent_runtime.adapters.memory_fs.hub import GovernedMemoryHub
from agent_runtime.adapters.memory_fs.legacy import adopt_legacy_memory
from agent_runtime.adapters.memory_fs.tool_args import read_document, required, search_query, write_proposal
from memory_hub import BaselineBundle, LegacyMemoryPortAdapter, MemoryHubConfig
from memory_hub.governance import classify_write_policy
from memory_hub.partition import ParsedUri, parse_memory_uri


def check_inbox_id(inbox_id: str) -> None:
    if not inbox_id or not inbox_id.strip():
        raise ValueError("inbox_id must be a non-empty string")
    if ".." in inbox_id or "/" in inbox_id or "\\" in inbox_id:
        raise ValueError(f"Invalid inbox_id path characters: {inbox_id}")


def directory_key(path: Path) -> tuple[object, ...]:
    # The same directory under another spelling, link or letter case is the same one. A part that is not there yet is
    # compared without letter case: created, both spellings name one directory on a volume that ignores it.
    path = path.resolve()
    missing: list[str] = []
    for directory in (path, *path.parents):
        try:
            found = directory.stat()
        except OSError:
            missing.append(_folded(directory.name))
            continue
        return found.st_dev, found.st_ino, *reversed(missing)
    return (_folded(str(path)),)


def _folded(text: str) -> str:
    return unicodedata.normalize("NFD", unicodedata.normalize("NFD", text).casefold())


@dataclass(frozen=True, slots=True)
class PartitionBinding:
    inbox_id: str
    memory_dir: Path
    workspace_dir: Path | None = None


class PartitionedMemoryAdapter:
    # The MemoryPort of one inbox's Runs, on the memory of its project in Memory Hub.
    def __init__(
        self,
        *,
        inbox_id: str,
        hub: GovernedMemoryHub,
        workspace_dir: str | Path | None = None,
    ) -> None:
        self._inbox_id = inbox_id
        self._hub = hub
        self._legacy = LegacyMemoryPortAdapter(hub, inbox_id)
        self._workspace_dir = Path(workspace_dir).resolve() if workspace_dir is not None else None

    @property
    def inbox_id(self) -> str:
        return self._inbox_id

    @property
    def project_key(self) -> str:
        return self._legacy.project_key

    @property
    def workspace_dir(self) -> Path | None:
        return self._workspace_dir

    def load_baseline(self) -> dict[str, str]:
        return self._legacy.load_baseline()

    def load_baseline_bundle(self) -> BaselineBundle:
        return self._legacy.load_baseline_bundle()

    def read_on_demand(self, path: str) -> str:
        self._parse(path)
        return self._legacy.read_on_demand(path)

    def write(self, path: str, content: str) -> None:
        self._parse(path)
        self._legacy.write(path, content)

    # True when write() only queues a proposal: the port returns nothing, so a tool asks this to say what happened.
    def needs_approval(self, path: str) -> bool:
        return classify_write_policy(self._parse(path)) == "REQUIRE_APPROVAL"

    # The memory_* tools: argument objects in, JSON objects out, always within this inbox's project.

    def search(self, args: Mapping[str, Any]) -> dict[str, Any]:
        return self._hub.search(search_query(self.project_key, args)).to_dict()

    def read_document(self, args: Mapping[str, Any]) -> dict[str, Any]:
        return read_document(self._hub, self._parse(required(args, "uri")).raw_uri, args)

    def propose(self, args: Mapping[str, Any], *, actor: str = "") -> dict[str, Any]:
        uri = self._parse(required(args, "target_uri")).raw_uri
        return self._hub.propose_write(write_proposal(uri, args, author=actor or None)).to_dict()

    def proposal(self, proposal_id: str) -> dict[str, Any]:
        record = next((r for r in self._hub.list_proposals(status=None) if r.get("proposal_id") == proposal_id), None)
        if record is None:
            raise FileNotFoundError(f"Proposal not found: {proposal_id}")
        self._parse(str(record["proposal"]["target_uri"]))
        return record

    def approve(self, proposal_id: str, *, actor: str) -> dict[str, Any]:
        self.proposal(proposal_id)
        return self._hub.approve_proposal(proposal_id, actor=actor).to_dict()

    def _parse(self, path: str) -> ParsedUri:
        # A URI may name any project; a Run stays in its own.
        parsed = parse_memory_uri(path if "://" in path else f"proj://{self.project_key}/{path.lstrip('/')}")
        if parsed.scheme == "proj" and parsed.scope != self.project_key:
            raise PermissionError(f"{path} is outside the memory of project '{self.project_key}'")
        return parsed


class WorkspacePartitionRegistry:
    def __init__(self, memory_root: str | Path, *, hub: GovernedMemoryHub | None = None) -> None:
        self._hub = hub if hub is not None else GovernedMemoryHub(MemoryHubConfig(memory_root=str(memory_root)))
        self._bindings: dict[str, PartitionBinding] = {}
        self._workspaces: dict[str, Path] = {}

    @property
    def hub(self) -> GovernedMemoryHub:
        return self._hub

    @property
    def global_instructions_path(self) -> Path:
        return self._hub.root / "global" / "instructions.md"

    def set_global_instructions(self, content: str) -> None:
        self._hub.set_global_instructions(content)

    def register(
        self,
        inbox_id: str,
        workspace_dir: str | Path | None = None,
    ) -> PartitionBinding:
        check_inbox_id(inbox_id)
        # Inboxes of one project share its memory; queues, sessions and workspaces stay per inbox_id.
        memory_dir = self._hub.project_dir(self._hub.resolve_project_key(inbox_id))
        resolved_workspace = Path(workspace_dir).resolve() if workspace_dir is not None else None

        if resolved_workspace is not None:
            # Hard concurrency invariant: two inbox_ids must never share a local code directory. Keys are read anew:
            # a directory's key changes when it is created.
            key = directory_key(resolved_workspace)
            owner = next((other for other, theirs in self._workspaces.items()
                          if other != inbox_id and directory_key(theirs) == key), None)
            if owner is not None:
                raise ValueError(
                    f"Workspace directory '{resolved_workspace}' is already mounted by "
                    f"inbox_id '{owner}'; different inbox_ids must not share a local code "
                    "directory (use an isolated git worktree)."
                )
            self._workspaces[inbox_id] = resolved_workspace

        binding = PartitionBinding(
            inbox_id=inbox_id,
            memory_dir=memory_dir,
            workspace_dir=resolved_workspace,
        )
        self._bindings[inbox_id] = binding
        return binding

    def binding_for(self, inbox_id: str) -> PartitionBinding:
        if inbox_id not in self._bindings:
            return self.register(inbox_id)
        return self._bindings[inbox_id]

    def memory_for(self, inbox_id: str) -> PartitionedMemoryAdapter:
        binding = self.binding_for(inbox_id)
        adopt_legacy_memory(self._hub, inbox_id)
        return PartitionedMemoryAdapter(inbox_id=inbox_id, hub=self._hub, workspace_dir=binding.workspace_dir)

    def workspace_for(self, inbox_id: str) -> Path | None:
        return self.binding_for(inbox_id).workspace_dir
