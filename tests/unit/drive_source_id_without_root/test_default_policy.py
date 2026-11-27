import pytest

from agent_runtime.adapters.credential_proxy.gdrive_queue import DriveQueueGuard, ProxyRefused
from tests.support.new_project import MemoryDrive


def test_default_policy_requires_sources_root_for_explicit_folder_id():
    guard = DriveQueueGuard(MemoryDrive(), root_folder_id="queue")
    with pytest.raises(ProxyRefused, match=r"Configure sources_folder_id.*enable allow_any_source_folder") as exc:
        guard.source_folder({"folder_id": "project"})
    assert exc.value.status == 403
