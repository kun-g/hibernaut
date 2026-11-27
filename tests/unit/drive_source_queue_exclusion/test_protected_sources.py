import pytest

from agent_runtime.adapters.credential_proxy.gdrive_queue import DriveQueueGuard, ProxyRefused
from agent_runtime.adapters.gdrive_contract import ATTACHMENTS_FOLDER_NAME, QUEUE_FOLDER_NAMES
from tests.support.new_project import MemoryDrive


@pytest.mark.parametrize("name", (*QUEUE_FOLDER_NAMES, ATTACHMENTS_FOLDER_NAME))
@pytest.mark.parametrize("request_form", ["list_by_id", "list_by_name", "download", "legacy_download"])
def test_source_operations_refuse_queue_folders_and_their_files(name, request_form):
    drive = MemoryDrive()
    for i, folder_name in enumerate((*QUEUE_FOLDER_NAMES, ATTACHMENTS_FOLDER_NAME)):
        drive.folder(f"folder-{i}", folder_name, parent="queue")
    folder_id = drive.find_subfolders("queue")[name]
    drive.file("item", "item.txt", b"test", folder_id, "2026-11-01")
    # Overlapping roots exercise name lookup and the older file-ID-only download.
    guard = DriveQueueGuard(drive, root_folder_id="queue", sources_folder_id="queue", allow_any_source_folder=True)

    with pytest.raises(ProxyRefused) as exc:
        if request_form == "list_by_id":
            guard.source_folder({"folder_id": folder_id})
        elif request_form == "list_by_name":
            guard.source_folder({"name": name})
        elif request_form == "download":
            guard.download_source({"folder_id": folder_id, "file_id": "item"})
        else:
            guard.download_source({"file_id": "item"})
    assert exc.value.status == 403
    assert drive.downloads == []
