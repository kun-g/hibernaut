from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from agent_runtime.adapters.executor_cli.steward_git import ToolError
from agent_runtime.app.config import RepoToolsConfig
from agent_runtime.app.repo_tools import RepoCreator, folder_ref
from tests.support.new_project import FakeGitHub, MemoryDrive, tarred, zipped

OWNER = "kun-g"


class CreateRepoTests(unittest.TestCase):
    # create_repo of the general Steward, with gh, git and Drive faked in this process.
    def setUp(self) -> None:
        self.scratch = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.drive = MemoryDrive()
        self.drive.folder("folder-aura", "aura-desk")
        self.github = FakeGitHub()

    def creator(self, *, max_bytes: int = 1 << 20, allowed: bool = True) -> RepoCreator:
        config = RepoToolsConfig(owner=OWNER, allowed_owners=(OWNER,), max_bytes=max_bytes)
        return RepoCreator(config=lambda: config if allowed else None, drive=lambda: self.drive,
                           run=self.github, scratch=self.scratch)

    def create(self, name: str = "aura-desk", folder: str | None = None) -> dict[str, object]:
        return json.loads(self.creator().create(name=name, drive_folder=folder, description="桌面助手"))

    def test_creates_a_private_repository_from_the_newest_archive_unpacked(self) -> None:
        self.drive.file("old", "aura-desk-1.0.zip", zipped({"aura-desk-1.0/README.md": b"old"}), "folder-aura",
                        "2026-11-01T00:00:00Z")
        self.drive.file("new", "aura-desk-1.1.zip", zipped({
            "aura-desk-1.1/README.md": b"new", "aura-desk-1.1/src/app.py": b"print(1)\n",
            "__MACOSX/._README.md": b"junk", "aura-desk-1.1/.git/config": b"[core]",
        }), "folder-aura", "2026-11-20T00:00:00Z")
        told = self.create()
        self.assertEqual(told["repo"], "kun-g/aura-desk")
        self.assertTrue(told["private"])
        self.assertEqual(told["commit"], "0123456789abcdef0123456789abcdef01234567")
        self.assertEqual(told["first_version"], {"folder": "aura-desk", "file": "aura-desk-1.1.zip",
                                                 "modified": "2026-11-20T00:00:00Z", "unpacked": True, "files": 2})
        # The one folder at the archive's top is the root; what packers add is left out.
        self.assertEqual(self.github.pushed["kun-g/aura-desk"], {"README.md": b"new", "src/app.py": b"print(1)\n"})
        self.assertEqual(self.drive.downloads, ["new"])
        create = next(c for c in self.github.calls if c[:3] == ["gh", "repo", "create"])
        self.assertIn("--private", create)
        self.assertIn("aura-desk-1.1.zip", self.github.commits[0])
        # Nothing is left in the scratch directory.
        self.assertEqual(list(self.scratch.iterdir()), [])

    def test_a_tarball_is_unpacked_too_and_a_plain_file_is_committed_as_it_is(self) -> None:
        self.drive.file("t", "release.tar.gz", tarred({"a.txt": b"a", "b/c.txt": b"c"}), "folder-aura",
                        "2026-11-20T00:00:00Z")
        self.create()
        self.assertEqual(self.github.pushed["kun-g/aura-desk"], {"a.txt": b"a", "b/c.txt": b"c"})
        self.drive.folder("folder-notes", "notes")
        self.drive.file("n", "spec.md", b"# spec", "folder-notes", "2026-11-21T00:00:00Z")
        told = self.create("notes")
        self.assertEqual(self.github.pushed["kun-g/notes"], {"spec.md": b"# spec"})
        self.assertFalse(told["first_version"]["unpacked"])  # type: ignore[index]

    def test_a_repository_that_exists_is_left_as_it_is(self) -> None:
        self.drive.file("new", "v.zip", zipped({"a": b"a"}), "folder-aura", "2026-11-20T00:00:00Z")
        self.github.existing.add("kun-g/aura-desk")
        with self.assertRaises(ToolError) as caught:
            self.create()
        self.assertIn("exists on GitHub already", str(caught.exception))
        self.assertEqual([c[:3] for c in self.github.calls], [["gh", "api", "repos/kun-g/aura-desk"]])
        self.assertEqual(self.drive.downloads, [])

    def test_an_empty_folder_stops_before_anything_is_created(self) -> None:
        with self.assertRaises(ToolError) as caught:
            self.create()
        self.assertIn("the folder aura-desk is empty; nothing was created", str(caught.exception))
        self.assertNotIn(["gh", "repo", "create"], [c[:3] for c in self.github.calls])

    def test_a_missing_folder_says_so(self) -> None:
        with self.assertRaises(ToolError) as caught:
            self.create("other")
        self.assertIn("stopped at Drive, before anything was created", str(caught.exception))
        self.assertEqual(self.github.pushed, {})

    def test_a_folder_given_by_link_is_used(self) -> None:
        self.drive.folder("1AbCdEfGhIjKlMnOp", "elsewhere")
        self.drive.file("x", "x.txt", b"x", "1AbCdEfGhIjKlMnOp", "2026-11-20T00:00:00Z")
        self.create(folder="https://drive.google.com/drive/folders/1AbCdEfGhIjKlMnOp?usp=sharing")
        self.assertEqual(self.github.pushed["kun-g/aura-desk"], {"x.txt": b"x"})
        self.assertEqual(folder_ref("1AbCdEfGhIjKlMnOp"), "1AbCdEfGhIjKlMnOp")
        with self.assertRaises(ToolError):
            folder_ref("../etc")

    def test_an_archive_that_reaches_outside_itself_is_refused(self) -> None:
        for blob in (zipped({"../evil.txt": b"x"}), zipped({"ok.txt": b"x"}, links=["link"])):
            self.drive.file("bad", "bad.zip", blob, "folder-aura", "2026-11-20T00:00:00Z")
            with self.assertRaises(ToolError):
                self.create()
        self.assertEqual(self.github.pushed, {})
        self.assertFalse((self.scratch.parent / "evil.txt").exists())

    def test_a_first_version_larger_than_allowed_is_refused(self) -> None:
        self.drive.file("big", "big.zip", zipped({"a.bin": b"0" * 4096}), "folder-aura", "2026-11-20T00:00:00Z")
        small = self.creator(max_bytes=1024)
        with self.assertRaises(ToolError) as caught:
            small.create(name="aura-desk", drive_folder=None, description=None)
        self.assertIn("max_bytes", str(caught.exception))

    def test_a_failed_push_says_the_repository_is_there(self) -> None:
        self.drive.file("new", "v.zip", zipped({"a": b"a"}), "folder-aura", "2026-11-20T00:00:00Z")
        self.github.push_fails = True
        with self.assertRaises(ToolError) as caught:
            self.create()
        self.assertIn("stopped at the push: kun-g/aura-desk was created (private)", str(caught.exception))

    def test_an_unclear_answer_from_github_stops_it(self) -> None:
        self.github.api_error = "gh: connect: network is unreachable"
        with self.assertRaises(ToolError) as caught:
            self.create()
        self.assertIn("could not be told", str(caught.exception))

    def test_a_name_that_is_no_repository_name_is_refused(self) -> None:
        for name in ("a/b", "..", "x.git"):
            with self.assertRaises(ToolError):
                self.create(name)
        self.assertEqual(self.github.calls, [])

    def test_a_binding_that_lost_the_tool_refuses(self) -> None:
        with self.assertRaises(ToolError):
            self.creator(allowed=False).create(name="aura-desk", drive_folder=None, description=None)
        self.assertEqual(self.github.calls, [])


if __name__ == "__main__":
    unittest.main()
