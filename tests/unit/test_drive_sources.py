from __future__ import annotations

import unittest

from agent_runtime.adapters.credential_proxy.gdrive_queue import DriveQueueGuard, ProxyRefused
from tests.support.new_project import SOURCES, MemoryDrive


class SourcesGuardTests(unittest.TestCase):
    # The proxy reads versions only from folders directly inside its sources folder, and only when one is configured.
    def setUp(self) -> None:
        self.drive = MemoryDrive()
        self.drive.folder(SOURCES, "版本", parent="my-drive")
        self.drive.folder("folder-aura", "aura-desk")
        self.drive.folder("deeper", "nested", parent="folder-aura")
        self.drive.folder("private", "私人", parent="my-drive")
        self.drive.file("v1", "v1.zip", b"zip", "folder-aura", "2026-11-20T00:00:00Z")
        self.drive.file("salary", "salary.md", b"secret", "private", "2026-11-20T00:00:00Z")
        self.drive.file("deep", "deep.zip", b"zip", "deeper", "2026-11-20T00:00:00Z")
        self.guard = DriveQueueGuard(self.drive, root_folder_id="root-q", sources_folder_id=SOURCES, max_download_bytes=10)

    def refused(self, status: int, op: str, **args: str) -> None:
        with self.assertRaises(ProxyRefused) as caught:
            if op == "source_download":
                self.guard.download_source(args)
            else:
                self.guard.handle(op, args)
        self.assertEqual(caught.exception.status, status, caught.exception.reason)

    def test_a_project_folder_by_name_or_id_and_its_files(self) -> None:
        for args in ({"name": "aura-desk"}, {"folder_id": "folder-aura"}):
            found = self.guard.handle("source_folder", args)
            self.assertEqual(found["folder"], {"id": "folder-aura", "name": "aura-desk"})
            self.assertEqual([f["id"] for f in found["files"]], ["v1"])
        self.assertEqual(self.guard.download_source({"file_id": "v1"}).content, b"zip")

    def test_nothing_outside_the_folders_directly_inside_it(self) -> None:
        self.refused(404, "source_folder", name="missing")
        self.refused(403, "source_folder", folder_id="private")
        self.refused(403, "source_folder", folder_id="deeper")
        self.refused(403, "source_download", file_id="salary", folder_id="private")
        self.refused(403, "source_download", file_id="salary")
        self.refused(403, "source_download", file_id="deep")
        self.refused(400, "source_folder", folder_id="../x")

    def test_a_file_larger_than_allowed_is_not_fetched(self) -> None:
        self.drive.file("big", "big.zip", b"0" * 11, "folder-aura", "2026-11-21T00:00:00Z")
        self.refused(413, "source_download", file_id="big")
        self.assertNotIn("big", self.drive.downloads)

    def test_without_a_sources_folder_it_is_off(self) -> None:
        self.guard = DriveQueueGuard(self.drive, root_folder_id="root-q")
        self.refused(403, "source_folder", name="aura-desk")
        self.refused(403, "source_download", file_id="v1")


if __name__ == "__main__":
    unittest.main()
