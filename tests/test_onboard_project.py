from __future__ import annotations

import subprocess
import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path

from agent_runtime.app.config import load_config
from scripts.onboard_project import onboard

BASE = """[runtime]
db_path = "./hb.db"

[projects.hibernaut]
inbox_id = ["proj:hibernaut", "proj:hibernaut:.+"]
repo = "kun-g/hibernaut"
checkout = "/tmp/hibernaut"
"""


class FakeGh:
    def __init__(self, *, labels: set[str] | None = None, origin: str = "") -> None:
        self.labels = labels or set()
        self.origin = origin
        self.calls: list[list[str]] = []

    def __call__(self, args: Sequence[str]) -> subprocess.CompletedProcess[str]:
        args = list(args)
        self.calls.append(args)
        ok, out = True, ""
        if args[:2] == ["gh", "api"] and "/labels/" in args[2]:
            ok = args[2].rsplit("/", 1)[1] in self.labels
        elif args[:3] == ["gh", "repo", "clone"]:
            Path(args[4], ".git").mkdir(parents=True)
        elif args[:3] == ["gh", "label", "create"]:
            self.labels.add(args[3])
        elif args[:2] == ["git", "-C"]:
            out = self.origin
        return subprocess.CompletedProcess(args, 0 if ok else 1, out, "")


class OnboardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.repos = Path(self.tmp.name).resolve() / "repos"
        (self.repos / "hibernaut").mkdir(parents=True)
        self.config = self.repos / "hibernaut" / "hibernaut.toml"
        self.config.write_text(BASE, encoding="utf-8")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_clones_labels_and_adds_a_webhook_project(self) -> None:
        gh = FakeGh()
        plan = onboard(["kun-g/New-Tool", "--config", str(self.config)], run=gh)
        self.assertEqual(plan.checkout, self.repos / "new-tool")
        self.assertIn(["gh", "repo", "clone", "kun-g/New-Tool", str(self.repos / "new-tool")], gh.calls)
        self.assertIn("hibernaut", gh.labels)
        project = load_config(self.config).projects["new-tool"]
        self.assertEqual(project.inbox_id, "proj:new-tool")
        self.assertEqual(project.repo, "kun-g/New-Tool")
        self.assertFalse(project.github_poll)
        assert project.delivery is not None
        self.assertFalse(project.delivery.auto_merge)

    def test_rerun_changes_nothing(self) -> None:
        onboard(["kun-g/new-tool", "--config", str(self.config)], run=FakeGh())
        before = self.config.read_text()
        gh = FakeGh(labels={"hibernaut"}, origin="https://github.com/kun-g/new-tool.git")
        plan = onboard(["kun-g/new-tool", "--config", str(self.config)], run=gh)
        self.assertEqual(self.config.read_text(), before)
        self.assertEqual(
            plan.steps,
            [
                f"checkout: {self.repos / 'new-tool'} (existing)",
                "label: hibernaut (existing)",
                "hibernaut.toml: [projects.new-tool] already serves kun-g/new-tool",
            ],
        )
        self.assertFalse(any(c[:3] in (["gh", "label", "create"], ["gh", "repo", "clone"]) for c in gh.calls))

    def test_dry_run_writes_nothing(self) -> None:
        gh = FakeGh()
        onboard(["kun-g/new-tool", "--config", str(self.config), "--dry-run"], run=gh)
        self.assertEqual(self.config.read_text(), BASE)
        self.assertFalse(any(c[:2] == ["gh", "label"] or c[:3] == ["gh", "repo", "clone"] for c in gh.calls))

    def test_refuses_a_checkout_of_another_repository(self) -> None:
        (self.repos / "new-tool" / ".git").mkdir(parents=True)
        with self.assertRaises(SystemExit):
            onboard(["kun-g/new-tool", "--config", str(self.config)], run=FakeGh(origin="git@github.com:x/y.git"))


if __name__ == "__main__":
    unittest.main()
