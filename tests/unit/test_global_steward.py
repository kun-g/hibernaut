from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from agent_runtime.adapters.executor_cli.steward_tools import StewardTools
from agent_runtime.app.config import HibernautConfig, parse_config
from tests.support.global_steward import ASH


class BindingConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        for name in ("work", "other", "a", "b"):
            (self.root / name).mkdir()

    def parse(self, agent: dict[str, Any] | None = None, **binding: Any) -> HibernautConfig:
        raw_binding = {"pattern": ASH, "default_agent": "ash", "agents": ["ash"], "workdir": str(self.root / "work"),
                       "checkout_access": "readonly", **binding}
        return parse_config({
            "runtime": {"inbox_bindings": [{k: v for k, v in raw_binding.items() if v is not None}]},
            "executors": {"cli": {"command": "x"}, "claude-code": {"harness": "claude-code"}},
            "agents": {
                "ash": {"role": "steward", "executor": "claude-code", **(agent or {})},
                "front": {"role": "steward", "executor": "claude-code"},
                "sde": {"role": "sde", "executor": "claude-code", "review": False},
                "reviewer": {"role": "code-reviewer", "executor": "claude-code"},
                "plain": {"executor": "cli"},
            },
            "projects": {
                name: {"repo": f"o/{name}", "checkout": str(self.root / name), "worktree_backend": "git",
                       "inbox_id": [f"proj:{name}", f"proj:{name}:sde", f"proj:{name}:x:.+"]}
                for name in ("a", "b")
            },
        })

    def refused(self, message: str, **kwargs: Any) -> None:
        with self.assertRaisesRegex(ValueError, message):
            self.parse(**kwargs)

    def test_a_readonly_binding_takes_a_steward(self) -> None:
        [bound] = self.parse(forward_to=["proj:a", "proj:b"]).runtime.inbox_bindings
        self.assertEqual((bound.binding.default_agent, bound.binding.agents, bound.readonly, bound.forward_to),
                         ("ash", ("ash",), True, ("proj:a", "proj:b")))
        self.assertEqual(bound.route().forward_to, ("proj:a", "proj:b"))

    def test_left_out_nothing_leaves_the_binding(self) -> None:
        [bound] = self.parse().runtime.inbox_bindings
        self.assertEqual((bound.forward_to, bound.route().forward_to), ((), ()))

    def test_a_writable_binding_takes_none(self) -> None:
        for access in ("readwrite", None):
            self.refused("agent 'ash' has role 'steward'.*checkout_access = \"readonly\"", checkout_access=access)

    def test_the_other_roles_are_refused_either_way(self) -> None:
        for role in ("sde", "reviewer"):
            for access in ("readonly", "readwrite"):
                for extra in ({"default_agent": role, "agents": [role]}, {"agents": ["ash", role]},
                              {"rules": [{"tags": ["x"], "agent": role}], "agents": None}):
                    with self.subTest(role=role, access=access, extra=extra):
                        self.refused(f"agent '{role}'" if access == "readonly" else "agent '",
                                     checkout_access=access, **extra)

    def test_omitted_agents_are_the_cli_agents_and_the_stewards_it_names(self) -> None:
        [bound] = self.parse(agents=None).runtime.inbox_bindings
        self.assertEqual(set(bound.binding.agents), {"cli", "plain", "ash"})

    def test_the_agent_s_own_workdir_has_to_be_the_binding_s(self) -> None:
        self.refused("agents.ash.*workdir", agent={"workdir": str(self.root / "other")})
        link = self.root / "link"
        link.symlink_to(self.root / "work")
        for same in (self.root / "work", link):
            self.parse(agent={"workdir": str(same)})
        self.parse(agent={"branch": "main"})

    def test_its_text_outside_a_project_has_no_name_of_its_own(self) -> None:
        self.parse(agent={"skills": ["builtin:steward"]})
        self.refused("'builtin:steward-global' is not available for role 'steward'",
                     agent={"skills": ["builtin:steward-global"]})

    def test_every_entry_is_a_project_s_main_inbox(self) -> None:
        for item in ("proj:c", "proj:a:sde", "proj:a:x:1", "proj:a:x:.+", "proj:.+", "user:main", "sys:ops", "a"):
            with self.subTest(item=item):
                self.refused(rf"runtime\.inbox_bindings\[0\]: forward_to '{item.replace('+', '[+]')}' is not the main "
                             "inbox of a project", forward_to=["proj:a", item])
        for raw in ([], "proj:a", ["proj:a", 1], [""]):
            with self.subTest(raw=raw):
                self.refused("forward_to must be a non-empty array of strings", forward_to=raw)
        self.refused("forward_to lists proj:a twice", forward_to=["proj:a", "proj:a"])

    def test_a_project_binding_has_no_list(self) -> None:
        with self.assertRaisesRegex(ValueError, "forward_to not supported"):
            parse_config({
                "executors": {"cli": {"command": "x"}}, "agents": {"plain": {"executor": "cli"}},
                "projects": {"a": {"repo": "o/a", "checkout": str(self.root / "a"), "worktree_backend": "git",
                                   "inbox_bindings": [{"pattern": "proj:a", "default_agent": "plain",
                                                       "forward_to": ["proj:a"]}]}},
            })


class ToolTests(unittest.TestCase):
    @staticmethod
    def text(projectless: bool) -> str:
        return json.dumps(StewardTools.definitions(projectless=projectless))

    def test_outside_a_project_the_tools_speak_of_none(self) -> None:
        text = self.text(True)
        for phrase in ("this project", "project memory", "main checkout", "the project's and"):
            self.assertNotIn(phrase, text)
        self.assertIn("continues", text)

    def test_a_project_steward_s_tools_are_what_they_were(self) -> None:
        text = self.text(False)
        self.assertNotIn("continues", text)
        self.assertIn("An inbox of this project", text)
        self.assertEqual(StewardTools.definitions(), StewardTools.definitions(projectless=False))

    def test_the_tool_names_are_the_same(self) -> None:
        self.assertEqual(*([t["name"] for t in StewardTools.definitions(projectless=p)] for p in (True, False)))


if __name__ == "__main__":
    unittest.main()
