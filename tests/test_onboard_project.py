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


WITH_STEWARDS = """# mine
[runtime]
db_path = "./hb.db"

[[runtime.inbox_bindings]]
pattern = "user:main"
default_agent = "ash"
agents = ["ash"]
workdir = "/tmp/workspace"
checkout_access = "readonly"
forward_to = ["proj:hibernaut"]  # by hand

[executors.claude-code]

[agents.ash]
role = "steward"
executor = "claude-code"

[agents.front]
role = "steward"
executor = "claude-code"

[agents.coder]
role = "sde"
executor = "claude-code"
review = false

[projects.hibernaut]
inbox_id = ["proj:hibernaut", "proj:hibernaut:.+"]
repo = "kun-g/hibernaut"
checkout = "/tmp/hibernaut"

[[projects.hibernaut.inbox_bindings]]
pattern = "proj:hibernaut"
default_agent = "front"

[[projects.hibernaut.inbox_bindings]]
pattern = "proj:hibernaut:.+"
default_agent = "coder"
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
        self.assertNotIn("github_poll", self.config.read_text())
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
                f"orca: {self.repos / 'new-tool'} (existing)",
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
        self.assertFalse(any(c[:3] == ["orca", "repo", "add"] for c in gh.calls))
        self.assertEqual(list(self.config.parent.iterdir()), [self.config])

    def test_r5_a_new_project_gets_the_same_steward_defaults_and_a_backup(self) -> None:
        self.config.write_text(WITH_STEWARDS)
        plan = onboard(["kun-g/new-tool", "--config", str(self.config), "--description", "一个工具"], run=FakeGh())
        config = load_config(self.config)
        project = config.projects["new-tool"]
        self.assertEqual(project.description, "一个工具")
        self.assertEqual([(b.rule.text, b.binding.default_agent) for b in project.inbox_bindings],
                         [("proj:new-tool", "front"), ("proj:new-tool:.+", "coder")])
        self.assertEqual(config.runtime.workdir_binding("user:main").forward_to, ("proj:hibernaut", "proj:new-tool"))
        assert plan.backup is not None
        self.assertEqual(plan.backup.read_text(), WITH_STEWARDS)
        after = self.config.read_text().replace(', "proj:new-tool"]', "]")
        self.assertTrue(after.startswith(WITH_STEWARDS))

    def test_r5_without_a_steward_in_the_config_the_project_is_added_and_that_is_said(self) -> None:
        plan = onboard(["kun-g/new-tool", "--config", str(self.config)], run=FakeGh())
        self.assertIn("new-tool", load_config(self.config).projects)
        self.assertTrue(any("teward" in line for line in plan.by_hand), plan.by_hand)

    def test_refuses_a_checkout_of_another_repository(self) -> None:
        (self.repos / "new-tool" / ".git").mkdir(parents=True)
        with self.assertRaises(SystemExit):
            onboard(["kun-g/new-tool", "--config", str(self.config)], run=FakeGh(origin="git@github.com:x/y.git"))


if __name__ == "__main__":
    unittest.main()
