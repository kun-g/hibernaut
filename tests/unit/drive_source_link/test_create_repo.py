import threading
from email.message import Message
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

import pytest

from agent_runtime.adapters.channels import ProxiedDriveClient
from agent_runtime.adapters.channels.gdrive_tasks import DriveProxyError
from agent_runtime.adapters.credential_proxy.gdrive_queue import DriveQueueGuard, build_proxy_server
from agent_runtime.adapters.executor_cli.steward_git import ToolError
from agent_runtime.adapters.permissions import CredentialProxyClient
from agent_runtime.app.config import RepoToolsConfig
from agent_runtime.app.repo_tools import RepoCreator
from tests.support.gdrive_credential_proxy import CLIENT_TOKEN
from tests.support.new_project import FakeGitHub, MemoryDrive


@pytest.fixture
def project(tmp_path: Path):
    drive = MemoryDrive()
    drive.folder("project-folder", "Project", parent="shared")
    drive.file("old", "old.txt", b"old", "project-folder", "2026-10-01")
    drive.file("new", "new.txt", b"new", "project-folder", "2026-11-01")
    guard = DriveQueueGuard(drive, root_folder_id="queue", allow_any_source_folder=True,
                            client_email="reader@example.invalid", max_download_bytes=4)
    with build_proxy_server(guard, host="127.0.0.1", port=0, service_name="drive", client_token=CLIENT_TOKEN) as server:
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        thread.start()
        try:
            proxy = CredentialProxyClient(f"http://127.0.0.1:{server.server_port}", token=CLIENT_TOKEN)
            client = ProxiedDriveClient(proxy, service_name="drive")
            github = FakeGitHub()
            config = RepoToolsConfig(owner="acme", allowed_owners=("acme",))
            creator = RepoCreator(config=lambda: config, drive=lambda: client, run=github, scratch=tmp_path)
            yield creator, drive, client, github
        finally:
            server.shutdown()
            thread.join()


def test_link_selects_latest_direct_file_without_sources_root(project):
    creator, drive, _, github = project
    drive.folder("nested", "Nested", parent="project-folder")
    drive.file("deep", "deep.txt", b"deep", "nested", "2026-12-01")
    creator.create(name="demo", drive_folder="https://drive.google.com/drive/folders/project-folder", description=None)
    assert drive.downloads == ["new"]
    assert github.pushed["acme/demo"] == {"new.txt": b"new"}


@pytest.mark.parametrize("code", [403, 404])
def test_inaccessible_link_reports_sharing_address(project, code):
    creator, drive, _, github = project
    error = HTTPError("", code, "upstream-only", Message(), None)
    with patch.object(drive, "get_file_meta", side_effect=error), pytest.raises(ToolError) as exc:
        creator.create(name="demo", drive_folder="project-folder", description=None)
    assert "share the folder" in str(exc.value) and "reader@example.invalid" in str(exc.value)
    assert "upstream-only" not in str(exc.value)
    assert github.pushed == {}


def test_missing_link_explains_what_to_supply(project):
    creator, _, _, _ = project
    with pytest.raises(ToolError, match=r"folder link.*share.*reader@example.invalid"):
        creator.create(name="demo", drive_folder=None, description=None)


@pytest.mark.parametrize("file_id,status", [("other", 403), ("nested", 403), ("large", 413)])
def test_download_checks_selected_parent_type_and_size(project, file_id, status):
    _, drive, client, _ = project
    drive.folder("nested", "Nested", parent="project-folder")
    drive.file("other", "file.txt", b"data", "nested", "2026-12-01")
    drive.file("large", "big.zip", b"12345", "project-folder", "2026-12-01")
    with pytest.raises(DriveProxyError) as exc:
        client.download_source(drive.get_file_meta(file_id), folder_id="project-folder")
    assert exc.value.status_code == status
    assert drive.downloads == []
