from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from agent_runtime.adapters.channels.ingress import event_origin
from agent_runtime.adapters.executor_cli.steward_tools import StewardTools
from agent_runtime.adapters.permissions import AutoModePermissionAdapter
from agent_runtime.app.config import HibernautConfig, parse_config
from agent_runtime.app.projects import build_global_manifest
from agent_runtime.core.models import Event, InboxRoute

ASH = "user:main"
OPS = "sys:ops"
SYSTEM_TOOLS = ("check_projects", "onboard_project", "complete_project")


class BindingConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        for name in ("work", "other", "a"):
            (self.root / name).mkdir()

    def raw(self, ash: dict[str, Any] | None = None, extra: list[dict[str, Any]] | None = None,
            **ops: Any) -> dict[str, Any]:
        def clean(binding: dict[str, Any]) -> dict[str, Any]:
            return {k: v for k, v in binding.items() if v is not None}

        return {
            "runtime": {"inbox_bindings": [
                *(extra or []),
                clean({"pattern": ASH, "default_agent": "ash", "agents": ["ash"], "workdir": str(self.root / "work"),
                       "checkout_access": "readonly", "forward_to": ["proj:a", OPS], **(ash or {})}),
                clean({"pattern": OPS, "default_agent": "ops", "agents": ["ops"], "workdir": str(self.root / "a"),
                       "checkout_access": "readonly", "system_tools": True, **ops}),
            ]},
            "executors": {"cli": {"command": "x"}, "claude-code": {"harness": "claude-code"}},
            "agents": {
                "ash": {"role": "steward", "executor": "claude-code"},
                "ops": {"role": "steward", "executor": "claude-code", "description": "onboards projects"},
                "front": {"role": "steward", "executor": "claude-code"},
                "plain": {"executor": "cli"},
            },
            "projects": {"a": {"repo": "o/a", "checkout": str(self.root / "a"), "worktree_backend": "git",
                               "inbox_id": ["proj:a", "proj:a:.+"], "default_agent": "front"}},
        }

    def parse(self, **kwargs: Any) -> HibernautConfig:
        return parse_config(self.raw(**kwargs))

    def refused(self, message: str, **kwargs: Any) -> None:
        with self.assertRaisesRegex(ValueError, message):
            self.parse(**kwargs)

    def test_c4_c6_the_chat_s_inbox_may_list_it_and_it_forwards_nowhere(self) -> None:
        ash, ops = self.parse().runtime.inbox_bindings
        self.assertEqual((ash.forward_to, ash.system_tools), (("proj:a", OPS), False))
        self.assertEqual((ops.forward_to, ops.system_tools), ((), True))
        self.assertTrue(ops.route().system)
        self.assertFalse(ash.route().system)

    def test_c1_left_out_there_are_no_system_tools(self) -> None:
        config = self.parse(ash={"forward_to": None}, system_tools=None)
        self.assertEqual([b.system_tools for b in config.runtime.inbox_bindings], [False, False])

    def test_c6_only_an_inbox_with_system_tools_can_be_listed_beside_the_projects(self) -> None:
        self.refused(r"forward_to 'sys:ops' is not the main inbox of a project", system_tools=False)
        for target in ("sys:.*", "sys:other", "sys:ops:x", "SYS:OPS"):
            with self.subTest(target=target):
                self.refused(r"is not the main inbox of a project", ash={"forward_to": [target]})

    def test_c1_the_binding_is_one_exact_sys_inbox(self) -> None:
        for pattern in ("sys:.*", "sys:(ops|x)", "re:sys:ops"):
            with self.subTest(pattern=pattern):
                self.refused("system_tools needs .*exact", ash={"forward_to": ["proj:a"]}, pattern=pattern)
        for pattern in ("user:ops", "ops", "system:ops"):
            with self.subTest(pattern=pattern):
                self.refused("system_tools needs .*sys:", ash={"forward_to": ["proj:a"]}, pattern=pattern)

    def test_c1_the_binding_is_readonly_and_runs_stewards_only(self) -> None:
        for access in ("readwrite", None):
            self.refused("readonly", checkout_access=access)
            self.refused("readonly", checkout_access=access, default_agent="plain", agents=["plain"])
        self.refused("system_tools.*'plain'", agents=["ops", "plain"])
        self.refused("system_tools.*'plain'", agents=None, rules=[{"tags": ["x"], "agent": "plain"}])

    def test_c1_left_out_agents_are_the_stewards_it_names_and_no_cli_agent(self) -> None:
        bound = self.parse(agents=None).runtime.workdir_binding(OPS)
        assert bound is not None
        self.assertEqual(bound.binding.agents, ("ops",))

    def test_c6_it_may_not_forward_on(self) -> None:
        self.refused("system_tools.*forward_to", forward_to=["proj:a"])

    def test_c1_the_switch_is_a_boolean(self) -> None:
        for value in ("true", 1, []):
            self.refused("system_tools must be true or false", system_tools=value)

    def test_c6_an_earlier_binding_must_not_take_the_inbox(self) -> None:
        self.refused("'sys:.*' .*matches 'sys:ops' first", extra=[{
            "pattern": "sys:.*", "default_agent": "front", "agents": ["front"], "workdir": str(self.root / "other"),
            "checkout_access": "readonly"}])

    def test_c6_only_stewards_may_pass_work_to_it(self) -> None:
        self.refused("forward_to 'sys:ops' needs", ash={"agents": ["ash", "plain"]})
        self.refused("forward_to 'sys:ops' needs", ash={"agents": None})
        self.refused("forward_to 'sys:ops' needs", ash={"pattern": "user:.+"})

    def test_c3_a_project_binding_cannot_have_the_tools(self) -> None:
        raw = self.raw()
        raw["projects"]["a"]["inbox_bindings"] = [{"pattern": "proj:a", "default_agent": "front", "system_tools": True}]
        with self.assertRaisesRegex(ValueError, "system_tools not supported"):
            parse_config(raw)

    def test_r7_labels_are_created_for_the_listed_owners_only(self) -> None:
        self.assertEqual(self.parse().onboarding.label_owners, ())
        raw = self.raw()
        raw["onboarding"] = {"label_owners": ["Acme"]}
        self.assertEqual(parse_config(raw).onboarding.label_owners, ("acme",))
        for wrong in ({"label_owners": "acme"}, {"label_owners": [""]}, {"label_owners": ["a/b"]}, {"owners": []}):
            raw["onboarding"] = wrong
            with self.subTest(wrong=wrong), self.assertRaises(ValueError):
                parse_config(raw)

    def test_r9_the_chat_s_manifest_names_it_and_what_it_is_for(self) -> None:
        config = self.parse()
        ash, ops = config.runtime.inbox_bindings
        text = build_global_manifest(ASH, ash.binding, tuple(config.agents.values()), tuple(config.projects.values()),
                                     "ash", systems=(ops,))
        self.assertIn("`sys:ops`", text)
        self.assertIn("onboards projects", text)


class ToolListTests(unittest.TestCase):
    def test_c1_c4_only_the_system_steward_is_offered_the_tools(self) -> None:
        def names(**kwargs: Any) -> list[str]:
            return [tool["name"] for tool in StewardTools.definitions(**kwargs)]

        self.assertEqual([n for n in names(projectless=True, system=True) if n in SYSTEM_TOOLS], list(SYSTEM_TOOLS))
        for kwargs in ({}, {"projectless": True}):
            self.assertFalse(set(SYSTEM_TOOLS) & set(names(**kwargs)))
        self.assertNotIn("forward_to_inbox", names(projectless=True, system=True))

    def test_c1_the_tools_take_named_values_and_no_free_configuration(self) -> None:
        found = {t["name"]: t["inputSchema"] for t in StewardTools.definitions(projectless=True, system=True)}
        self.assertEqual({n: set(found[n]["properties"]) for n in SYSTEM_TOOLS}, {
            "check_projects": set(), "onboard_project": {"repo", "name", "description"},
            "complete_project": {"name", "description"},
        })
        for name in SYSTEM_TOOLS:
            self.assertIs(found[name]["additionalProperties"], False)

    def test_c1_the_gate_allows_them_by_default_and_can_take_each_away(self) -> None:
        gate = AutoModePermissionAdapter()
        for name in SYSTEM_TOOLS:
            self.assertEqual(gate.check(name, {}).action, "allow")
            blocked = AutoModePermissionAdapter(block_tools=frozenset({name}))
            self.assertEqual(blocked.check(name, {}).action, "block")


class OriginTests(unittest.TestCase):
    @staticmethod
    def origin(source: str, **payload: Any) -> str | None:
        return event_origin(Event(inbox_id=ASH, source=source, external_id="1", payload=payload))

    def test_c6_a_person_s_own_chat_and_this_host_are_told_from_everything_else(self) -> None:
        self.assertEqual(self.origin("sb:feishu", metadata={"chat_type": "p2p"}), "chat")
        self.assertEqual(self.origin("cli"), "operator")
        for source, payload in (
            ("sb:feishu", {"metadata": {"chat_type": "group"}}), ("sb:feishu", {}),
            ("sb:github", {"metadata": {"chat_type": "p2p"}}), ("sb:taskboard", {}), ("gdrive:task", {}),
            ("timer:daily", {}), ("agent:return", {}), ("agent:forward", {}), ("agent:close", {}),
            (" sb:github", {}), ("SB:github", {}),
        ):
            with self.subTest(source=source, payload=payload):
                self.assertIsNone(self.origin(source, **payload))

    def test_c6_a_decision_is_no_message(self) -> None:
        self.assertIsNone(self.origin("sb:feishu", approval_request_id="req", metadata={"chat_type": "p2p"}))


class RouteTests(unittest.TestCase):
    def test_a_route_is_no_system_inbox_unless_said_so(self) -> None:
        self.assertFalse(InboxRoute(project="p", binding=None, has_bindings=False).system)


if __name__ == "__main__":
    unittest.main()
