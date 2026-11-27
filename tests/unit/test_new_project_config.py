from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any

from agent_runtime.adapters.executor_cli import steward
from agent_runtime.adapters.executor_cli.steward_tools import StewardTools
from agent_runtime.app.config import HibernautConfig, parse_config
from agent_runtime.app.projects import build_global_manifest, build_system_manifest

ASH, OPS, INFRA = "user:main", "sys:ops", "sys:infra"
# What could run a command or change a file: none of it is offered to the chat's Steward.
COMMAND_LINE = ("Bash", "Edit", "Write", "MultiEdit", "NotebookEdit", "Task", "Agent", "dokploy", "run_command",
                "onboard_project", "complete_project")


class ConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        for name in ("work", "ops", "infra", "a"):
            (self.root / name).mkdir()
        self.token = self.root / "proxy-token"
        self.token.write_text("x" * 20)
        self.token.chmod(0o600)

    def raw(self, *, ash: dict[str, Any] | None = None, infra: dict[str, Any] | None = None,
            sections: dict[str, Any] | None = None) -> dict[str, Any]:
        def clean(binding: dict[str, Any]) -> dict[str, Any]:
            return {k: v for k, v in binding.items() if v is not None}

        return {
            "runtime": {"inbox_bindings": [
                clean({"pattern": ASH, "default_agent": "ash", "agents": ["ash"], "workdir": str(self.root / "work"),
                       "checkout_access": "readonly", "forward_to": ["proj:a", OPS, INFRA], "repo_tools": True,
                       **(ash or {})}),
                {"pattern": OPS, "default_agent": "hibernaut-ops", "agents": ["hibernaut-ops"],
                 "workdir": str(self.root / "ops"), "checkout_access": "readonly", "system_tools": True},
                clean({"pattern": INFRA, "default_agent": "infra", "agents": ["infra"], "workdir": str(self.root / "infra"),
                       "checkout_access": "readonly", "ops_tools": True, **(infra or {})}),
            ]},
            "executors": {"cli": {"command": "x"}, "claude-code": {"harness": "claude-code"}},
            "agents": {
                "ash": {"role": "steward", "executor": "claude-code"},
                "hibernaut-ops": {"role": "steward", "executor": "claude-code"},
                "infra": {"role": "steward", "executor": "claude-code", "description": "运维"},
                "front": {"role": "steward", "executor": "claude-code"},
                "plain": {"executor": "cli"},
            },
            "projects": {"a": {"repo": "o/a", "checkout": str(self.root / "a"), "worktree_backend": "git",
                               "inbox_id": ["proj:a", "proj:a:.+"], "default_agent": "front"}},
            "gdrive": {"proxy_url": "http://127.0.0.1:9100", "proxy_token_file": str(self.token)},
            "repo_tools": {"owner": "kun-g"},
            "ops_tools": {"dokploy_script": str(self.root / "dokploy-api.sh")},
            **(sections or {}),
        }

    def parse(self, **kwargs: Any) -> HibernautConfig:
        return parse_config(self.raw(**kwargs))

    def refused(self, message: str, **kwargs: Any) -> None:
        with self.assertRaisesRegex(ValueError, message):
            self.parse(**kwargs)

    def test_the_three_inboxes_and_their_tools(self) -> None:
        config = self.parse()
        ash, ops, infra = config.runtime.inbox_bindings
        self.assertEqual((ash.repo_tools, ash.person_only, ash.forward_to), (True, False, ("proj:a", OPS, INFRA)))
        self.assertTrue(ops.person_only and ops.system_tools and not ops.ops_tools)
        self.assertTrue(infra.person_only and infra.ops_tools and not infra.system_tools)
        # An operations inbox takes a person's work only and passes none on, like a system inbox.
        self.assertTrue(infra.route().system)
        self.assertEqual(config.repo_tools.owner, "kun-g")
        self.assertEqual(config.repo_tools.allowed_owners, ("kun-g",))
        self.assertEqual(config.ops_tools.staging_environments, ("staging",))

    def test_an_operations_inbox_is_kept_as_narrow_as_a_system_inbox(self) -> None:
        self.refused("ops_tools needs a pattern that is one exact inbox id", infra={"pattern": "sys:infra.*"})
        self.refused("ops_tools needs an inbox id that starts with sys:", infra={"pattern": "user:infra"},
                     ash={"forward_to": ["proj:a", OPS]})
        self.refused("needs checkout_access = \"readonly\"", infra={"checkout_access": "readwrite"})
        self.refused("ops_tools needs agents with role steward only", infra={"agents": ["infra", "plain"]})
        self.refused("passes no work on", infra={"forward_to": ["proj:a"]})
        self.refused("two inboxes of their own", infra={"system_tools": True})
        self.refused("repo_tools belongs to the general Steward", infra={"repo_tools": True})
        self.refused("needs \\[ops_tools\\] dokploy_script", sections={"ops_tools": {}})

    def test_create_repo_belongs_to_the_chat_s_inbox_and_needs_its_settings(self) -> None:
        self.refused("repo_tools needs one exact inbox id", ash={"pattern": "user:.*", "forward_to": None})
        self.refused("needs \\[repo_tools\\] owner", sections={"repo_tools": {}})
        self.refused("needs \\[gdrive\\] proxy_url and proxy_token_file", sections={"gdrive": {}})
        self.refused("is not a GitHub owner", sections={"repo_tools": {"owner": "Kun G"}})
        self.refused("must include", sections={"repo_tools": {"owner": "kun-g", "allowed_owners": ["other"]}})
        self.refused("is not a GitHub owner", sections={"repo_tools": {"owner": "kun-g",
                                                                      "allowed_owners": ["kun-g", "bad/org"]}})
        self.refused("repo_tools must be true or false", ash={"repo_tools": "yes"})

    def test_no_timer_and_no_group_reaches_an_operations_inbox(self) -> None:
        with self.assertRaisesRegex(ValueError, "has ops_tools"):
            parse_config({**self.raw(), "timers": [{"name": "t", "inbox_id": INFRA, "interval_seconds": 60}]})
        with self.assertRaisesRegex(ValueError, "with system_tools"):
            parse_config({**self.raw(), "feishu_groups": [{"chat_id": "oc_x", "inbox_id": INFRA, "members": ["ou_k"]}]})

    def test_the_chat_s_steward_is_told_of_both_inboxes_and_of_create_repo(self) -> None:
        config = self.parse()
        ash, ops, infra = config.runtime.inbox_bindings
        manifest = build_global_manifest(ASH, ash.binding, tuple(config.agents.values()), (), "ash", (ops, infra),
                                         repo_owner="kun-g", repo_owners=("kun-g", "LambdaTheory"))
        self.assertIn("`sys:ops`: agent `hibernaut-ops`. Onboards projects", manifest)
        self.assertIn("`sys:infra`: agent `infra`. Operations: sets up and deploys", manifest)
        self.assertIn("waits for a person's approval", manifest)
        self.assertIn("private repository under `kun-g` by default", manifest)
        self.assertIn("Allowed GitHub owners: kun-g, LambdaTheory", manifest)
        self.assertIn("an operations inbox", build_system_manifest(INFRA, infra.binding, (), ops=True))


class ToolListTests(unittest.TestCase):
    # What each Steward is offered, as the launcher's --allowedTools lists it.
    def tools(self, **kwargs: Any) -> list[str]:
        return [t["name"] for t in StewardTools.definitions(projectless=True, **kwargs)]

    def test_the_chat_s_steward_has_create_repo_and_no_command_line(self) -> None:
        offered = self.tools(status=True, repo=True)
        create = next(t for t in StewardTools.definitions(projectless=True, repo=True) if t["name"] == "create_repo")
        self.assertIn("owner", create["inputSchema"]["properties"])
        self.assertNotIn("owner", create["inputSchema"]["required"])
        self.assertIn("create_repo", offered)
        self.assertIn("forward_to_inbox", offered)
        for name in COMMAND_LINE:
            self.assertNotIn(name, offered)
        # Claude Code's own tools are switched off and refused for every Steward run.
        self.assertIn("Bash", steward._REFUSED_TOOLS)  # pyright: ignore[reportPrivateUsage]
        self.assertNotIn("create_repo", self.tools(status=True))

    def test_the_operations_steward_has_dokploy_only_to_run(self) -> None:
        offered = self.tools(ops=True)
        self.assertIn("dokploy", offered)
        for name in ("forward_to_inbox", "create_repo", "onboard_project", "runtime_status", "Bash"):
            self.assertNotIn(name, offered)


if __name__ == "__main__":
    unittest.main()
