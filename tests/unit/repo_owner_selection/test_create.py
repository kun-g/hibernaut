import json

import pytest

from agent_runtime.adapters.executor_cli.steward_git import ToolError
from agent_runtime.app.config import GDriveConfig, RuntimeConfig, parse_repo_tools
from agent_runtime.app.repo_tools import RepoCreator
from tests.support.new_project import FakeGitHub, MemoryDrive


def test_only_the_configured_owners_receive_the_first_commit():
    github = FakeGitHub()
    drive = MemoryDrive()
    drive.folder("folder-aura", "auradesk")
    drive.file("latest", "app.txt", b"app", "folder-aura", "2026-11-27T00:00:00Z")
    policy = parse_repo_tools({"owner": "kun-g", "allowed_owners": ["kun-g", "LambdaTheory"]},
                              RuntimeConfig(), GDriveConfig())
    creator = RepoCreator(config=lambda: policy, drive=lambda: drive, run=github)
    with pytest.raises(ToolError, match=r"elsewhere.*kun-g.*LambdaTheory"):
        creator.create(name="auradesk", owner="elsewhere", drive_folder=None, description=None)
    assert github.calls == []
    assert drive.downloads == []

    default = creator.create(name="auradesk", drive_folder=None, description=None)
    org = creator.create(name="auradesk", owner="lambdatheory", drive_folder=None, description=None)
    assert [json.loads(result)["repo"] for result in (default, org)] == ["kun-g/auradesk", "LambdaTheory/auradesk"]
    assert github.pushed == {"kun-g/auradesk": {"app.txt": b"app"}, "LambdaTheory/auradesk": {"app.txt": b"app"}}
