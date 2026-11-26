from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from agent_runtime.adapters.memory_fs import GovernedMemoryHub, open_role_store
from memory_hub import MemoryHubConfig


@pytest.mark.parametrize("with_roles", [False, True])
def test_approved_role_memory_is_committed_with_the_rest(tmp_path: Path, with_roles: bool) -> None:
    def git(*args: str) -> str:
        return subprocess.run(
            ["git", "-c", "maintenance.auto=false", *args], cwd=tmp_path, check=True, capture_output=True, text=True,
            env={**os.environ, "GIT_CONFIG_GLOBAL": os.devnull},
        ).stdout

    git("init", "-q")
    git("config", "user.email", "t@example.com")
    git("config", "user.name", "t")
    opened = GovernedMemoryHub(MemoryHubConfig(memory_root=str(tmp_path), auto_git_commit=True))
    try:
        if with_roles:
            store = open_role_store(opened)
            proposal = store.propose_skill_write(role_id="architect", content="x\n", reason="r", proposed_by="a")
            store.approve_proposal(proposal.proposal_id, reviewed_by="owner")
            assert "roles/architect/skills.md" in git("ls-files")
            assert proposal.proposal_id in git("log", "-1", "--format=%s")
        else:
            # The deployment never turned experts on: there is no roles/ to add.
            opened.lay_out("hibernaut")
            from memory_hub import WriteProposal
            queued = opened.propose_write(WriteProposal(
                target_uri="proj://hibernaut/instructions.md", op="append", content="rule", rationale="r"))
            opened.approve_proposal(queued.proposal_id or "", actor="owner")
            assert "projects/hibernaut/instructions.md" in git("ls-files")
    finally:
        opened.close()
