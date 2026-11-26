from __future__ import annotations

import asyncio
import contextlib
import io
import tempfile
import tomllib
import unittest
from pathlib import Path
from typing import Any

from agent_runtime.adapters.executor_cli.claude_code import child_env
from agent_runtime.adapters.executor_cli.role_skills import builtin_skill
from agent_runtime.app.config import HibernautConfig, config_changes, load_config, parse_config
from agent_runtime.app.projects import build_routing_manifest
from agent_runtime.core.models import InboxMatchRule

CONFIG = """
[runtime]
default_agent = "steward"

[launchers.sg]
command = "claude-sg"

[executors.claude-code]
launcher = "sg"
billing = "subscription"
timeout_seconds = 900.0
env = { LANG = "C" }

[executors.grok]
command = "grok"
args = ["-p", "{prompt}"]

[agents.steward]
role = "steward"
executor = "claude-code"
model = "claude-sonnet-5-5"
effort = "medium"
checkout_access = "readonly"
skills = ["builtin:steward"]

[agents.sde]
role = "sde"
executor = "claude-code"
model = "claude-opus-5-5"
effort = "high"
review = true
reviewer = "code-reviewer"
interactive = false
description = "develops tasks"

[agents.code-reviewer]
role = "code-reviewer"
executor = "claude-code"

[agents.ops]
executor = "grok"
description = "runs ops checks"

[projects.hibernaut]
repo = "acme/hibernaut"
checkout = "/tmp/hibernaut"
inbox_id = ["proj:hibernaut", "proj:hibernaut:.+"]
default_agent = "steward"

[[projects.hibernaut.inbox_bindings]]
pattern = "proj:hibernaut"
default_agent = "steward"
agents = ["steward", "code-reviewer", "ops"]
rules = [
  { source = "timer:ops", agent = "ops" },
  { tags = ["review"], agent = "code-reviewer" },
]

[[projects.hibernaut.inbox_bindings]]
pattern = "proj:hibernaut:sde(:.*)?"
default_agent = "sde"
agents = ["sde", "code-reviewer"]
"""


def raw_config() -> dict[str, Any]:
    return tomllib.loads(CONFIG)


def parse(change: Any = None) -> HibernautConfig:
    raw = raw_config()
    if change is not None:
        change(raw)
    return parse_config(raw)


class AgentsTableTests(unittest.TestCase):
    def test_agents_bind_a_role_and_options_to_a_pure_executor(self) -> None:
        cfg = parse()
        sde = cfg.roles["sde"]
        self.assertEqual((sde.role, sde.interactive, sde.profile.name), ("sde", False, "sde"))
        self.assertEqual(
            (sde.profile.launcher.command, sde.profile.timeout_seconds, sde.profile.env, sde.profile.model),
            ("claude-sg", 900.0, {"LANG": "C"}, "claude-opus-5-5"),
        )
        assert sde.sde is not None
        self.assertEqual((sde.sde.review, sde.sde.reviewer), (True, "code-reviewer"))
        # A pure executor has no role, so it is not something an inbox can run.
        self.assertNotIn("claude-code", cfg.roles)
        self.assertNotIn("claude-code", cfg.executors)
        self.assertEqual(cfg.runtime.default_agent, "steward")

    def test_steward_defaults_to_the_readonly_checkout_and_its_built_in_skill(self) -> None:
        def change(raw: dict[str, Any]) -> None:
            raw["agents"]["steward"] = {"role": "steward", "executor": "claude-code"}

        steward = parse(change).roles["steward"]
        self.assertEqual((steward.role, steward.checkout_access, steward.interactive), ("steward", "readonly", False))
        self.assertEqual(steward.profile.skills, (builtin_skill("steward"),))

    def test_agent_on_a_cli_template(self) -> None:
        cfg = parse()
        self.assertEqual((cfg.executors["ops"].name, cfg.executors["ops"].command), ("ops", "grok"))
        self.assertIn("grok", cfg.executors)

    def test_invalid_agents_are_rejected_with_their_own_name(self) -> None:
        cases: list[tuple[dict[str, Any], str]] = [
            ({"role": "sde"}, "agents.x: executor is required"),
            ({"role": "sde", "executor": "nope"}, "agents.x: executor 'nope' is not declared"),
            ({"executor": "claude-code"}, "agents.x: executor 'claude-code' needs a role"),
            ({"role": "boss", "executor": "claude-code"}, "agents.x: unsupported role 'boss'"),
            ({"role": "sde", "executor": "grok"}, "agents.x: role 'sde' needs a Claude Code executor"),
            ({"executor": "grok", "model": "opus"}, "agents.x: model not supported"),
            ({"role": "sde", "executor": "claude-code", "model": " "}, "agents.x: model must be a non-empty string"),
            ({"role": "sde", "executor": "claude-code", "max_concurrency": 0}, "agents.x.max_concurrency must be"),
            ({"executor": "grok", "max_concurrency": "1"}, "agents.x.max_concurrency must be"),
            ({"role": "sde", "executor": "claude-code", "launcher": "sg"}, "agents.x: launcher not supported"),
            ({"role": "steward", "executor": "claude-code", "review": True}, "agents.x: review not supported"),
            ({"role": "steward", "executor": "claude-code", "checkout_access": "write"}, "agents.x: checkout_access"),
            ({"role": "sde", "executor": "claude-code", "checkout_access": "readonly"}, "agents.x: checkout_access"),
            ({"role": "steward", "executor": "claude-code", "interactive": True}, "agents.x: interactive"),
            ({"role": "steward", "executor": "claude-code", "skills": ["builtin:sde"]}, "agents.x: 'builtin:sde'"),
            ({"role": "sde", "executor": "claude-code", "reviewer": "steward"}, "agents.x: reviewer 'steward'"),
        ]
        for item, message in cases:
            with self.subTest(item=item), self.assertRaisesRegex(ValueError, message.replace("(", r"\(")):
                parse(lambda raw, item=item: raw["agents"].update(x=item))

    def test_an_agent_runs_any_model_through_its_executors_gateway(self) -> None:
        def change(raw: dict[str, Any]) -> None:
            raw["executors"]["gateway"] = {
                "billing": "subscription",
                "env": {"ANTHROPIC_BASE_URL": "http://127.0.0.1:3425", "ANTHROPIC_AUTH_TOKEN": "t"},
            }
            for role in ("sde", "code-reviewer", "steward"):
                raw["agents"][f"gw-{role}"] = {"role": role, "executor": "gateway", "model": "codex/gpt-6.2"}

        roles = parse(change).roles
        for role in ("sde", "code-reviewer", "steward"):
            with self.subTest(role=role):
                profile = roles[f"gw-{role}"].profile
                env = child_env(profile, {"ANTHROPIC_BASE_URL": "http://inherited", "ANTHROPIC_API_KEY": "inherited"})
                self.assertEqual(profile.model, "codex/gpt-6.2")
                self.assertEqual(env["ANTHROPIC_BASE_URL"], "http://127.0.0.1:3425")
                self.assertEqual(env["ANTHROPIC_AUTH_TOKEN"], "t")
                self.assertNotIn("ANTHROPIC_API_KEY", env)

    def test_an_agent_cannot_take_the_name_of_another_executor(self) -> None:
        with self.assertRaisesRegex(ValueError, "agents.grok"):
            parse(lambda raw: raw["agents"].update(grok={"role": "sde", "executor": "claude-code"}))

    def test_pure_executor_keys_are_checked(self) -> None:
        for key, value in (("skills", ["x"]), ("review", True), ("comand", "typo")):
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, f"executors.claude-code: {key}"):
                parse(lambda raw, key=key, value=value: raw["executors"]["claude-code"].update({key: value}))

    def test_default_agents_must_be_declared(self) -> None:
        with self.assertRaisesRegex(ValueError, "runtime.default_agent 'ghost'"):
            parse(lambda raw: raw["runtime"].update(default_agent="ghost"))
        with self.assertRaisesRegex(ValueError, "projects.hibernaut.default_agent 'ghost'"):
            parse(lambda raw: raw["projects"]["hibernaut"].update(default_agent="ghost"))


class LegacyConfigTests(unittest.TestCase):
    LEGACY = {
        "runtime": {"default_executor": "coder-reviewer"},
        "executors": {
            "sde": {"role": "sde", "reviewer": "coder-reviewer", "interactive": False},
            "coder-reviewer": {"role": "coder-reviewer"},
            "codex": {"command": "codex", "args": ["{prompt}"]},
        },
        "projects": {"x": {"repo": "a/x", "checkout": "/tmp/x", "executor": "sde"}},
    }

    def test_role_executors_and_defaults_normalize_to_agents(self) -> None:
        cfg = parse_config(self.LEGACY)
        self.assertEqual(sorted(cfg.agents), ["coder-reviewer", "codex", "sde"])
        self.assertEqual(cfg.agents["coder-reviewer"].role, "code-reviewer")
        self.assertEqual((cfg.agents["codex"].role, cfg.agents["codex"].executor), (None, "codex"))
        self.assertEqual(cfg.runtime.default_agent, "coder-reviewer")
        self.assertEqual(cfg.projects["x"].default_agent, "sde")
        self.assertEqual(cfg.projects["x"].inbox_bindings, ())
        route = cfg.projects["x"].route("proj:x")
        self.assertEqual((route.project, route.binding, route.has_bindings), ("x", None, False))

    def test_new_key_wins_and_conflicting_values_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "default_agent"):
            parse(lambda raw: raw["projects"]["hibernaut"].update(executor="sde"))
        with self.assertRaisesRegex(ValueError, "default_agent"):
            parse(lambda raw: raw["runtime"].update(default_executor="sde"))

    def test_legacy_agents_can_be_bound(self) -> None:
        raw = {**self.LEGACY, "projects": {"x": {
            "repo": "a/x", "checkout": "/tmp/x", "executor": "sde",
            "inbox_bindings": [{"pattern": "proj:x", "default_agent": "sde", "agents": ["sde", "coder-reviewer"]}],
        }}}
        binding = parse_config(raw).projects["x"].route("proj:x").binding
        assert binding is not None
        self.assertEqual(binding.agents, ("sde", "coder-reviewer"))


class InboxBindingTests(unittest.TestCase):
    def test_bindings_resolve_by_inbox_in_declared_order(self) -> None:
        project = parse().projects["hibernaut"]
        main = project.route("proj:hibernaut")
        assert main.binding is not None
        self.assertEqual((main.project, main.has_bindings, main.binding.default_agent), ("hibernaut", True, "steward"))
        self.assertEqual(main.binding.rules, (
            InboxMatchRule(agent="ops", source="timer:ops"),
            InboxMatchRule(agent="code-reviewer", tags=("review",)),
        ))
        for inbox in ("proj:hibernaut:sde", "proj:hibernaut:sde:42"):
            child = project.route(inbox).binding
            assert child is not None
            self.assertEqual(child.default_agent, "sde")
        self.assertIsNone(project.route("proj:hibernaut:docs").binding)

    def test_invalid_bindings_are_rejected(self) -> None:
        def bind(**change: Any) -> Any:
            def apply(raw: dict[str, Any]) -> None:
                raw["projects"]["hibernaut"]["inbox_bindings"][0].update(change)
            return apply

        cases: list[tuple[Any, str]] = [
            (bind(default_agent="ghost"), "default_agent 'ghost' is not declared"),
            (bind(default_agent="sde"), "default_agent 'sde' is not in agents"),
            (bind(default_agent=""), "default_agent"),
            (bind(agents=["steward", "ghost"]), "'ghost' is not declared"),
            (bind(agents=[]), "agents"),
            (bind(agents="steward"), "agents"),
            (bind(agents=["steward", "steward"]), "twice"),
            (bind(rules=[{"source": "x", "agent": "ghost"}]), "'ghost' is not declared"),
            (bind(rules=[{"source": "x", "agent": "sde"}]), "'sde' is not in agents"),
            (bind(rules=[{"source": "x"}]), "agent"),
            (bind(rules=[{"agent": "ops"}]), "source, event_type or tags"),
            (bind(rules=[{"sorce": "x", "agent": "ops"}]), "sorce"),
            (bind(rules=[{"tags": "review", "agent": "ops"}]), "tags"),
            (bind(rules=[{"tags": [], "source": "x", "agent": "ops"}]), "tags"),
            (bind(rules=[{"source": "", "agent": "ops"}]), "source"),
            (bind(pattern="proj:hibernaut:("), "invalid pattern"),
            (bind(pattern=""), "pattern"),
            (bind(pattern="proj:elsewhere"), "proj:elsewhere"),
            (bind(pattern="proj:hibernaut:sde(:.*)?"), "twice"),
            (bind(extra=1), "extra"),
            (lambda raw: raw["projects"]["hibernaut"].update(inbox_bindings={"pattern": "x"}), "inbox_bindings"),
        ]
        for change, message in cases:
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                parse(change)

    def test_errors_name_the_binding(self) -> None:
        def change(raw: dict[str, Any]) -> None:
            raw["projects"]["hibernaut"]["inbox_bindings"][1]["default_agent"] = "ops"

        with self.assertRaisesRegex(ValueError, r"projects\.hibernaut\.inbox_bindings\[1\]"):
            parse(change)

    def test_pattern_reaching_another_projects_inbox_is_ambiguous(self) -> None:
        def change(raw: dict[str, Any]) -> None:
            raw["projects"]["other"] = {"repo": "a/o", "checkout": "/tmp/o", "inbox_id": "proj:other"}
            raw["projects"]["hibernaut"]["inbox_bindings"][1]["pattern"] = "proj:.*"

        with self.assertRaisesRegex(ValueError, "proj:other"):
            parse(change)


class RoutingManifestTests(unittest.TestCase):
    def test_manifest_lists_bindings_agents_and_rules_from_the_config(self) -> None:
        text = build_routing_manifest(parse().projects["hibernaut"])
        for expected in (
            "proj:hibernaut", "proj:hibernaut:sde(:.*)?", "timer:ops", "review", "sde", "code-reviewer", "ops",
            "develops tasks", "runs ops checks", "claude-opus-5-5", "grok",
        ):
            self.assertIn(expected, text)
        # Rules keep their order: the first match wins.
        self.assertLess(text.index("timer:ops"), text.index("tags"))

    def test_manifest_follows_the_config(self) -> None:
        def change(raw: dict[str, Any]) -> None:
            raw["projects"]["hibernaut"]["inbox_bindings"][1].update(pattern="proj:hibernaut:docs", agents=["sde"])
            raw["agents"]["sde"]["description"] = "writes the manual"

        text = build_routing_manifest(parse(change).projects["hibernaut"])
        self.assertIn("proj:hibernaut:docs", text)
        self.assertIn("writes the manual", text)
        self.assertNotIn("proj:hibernaut:sde(:.*)?", text)

    def test_manifest_of_a_project_without_bindings(self) -> None:
        cfg = parse_config(LegacyConfigTests.LEGACY)
        text = build_routing_manifest(cfg.projects["x"])
        self.assertIn("proj:x", text)
        self.assertIn("sde", text)


class HotReloadTests(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_binding_keeps_the_current_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "hibernaut.toml"
            path.write_text(CONFIG, encoding="utf-8")
            load_config(path)
            stop = asyncio.Event()
            errors = io.StringIO()
            with contextlib.redirect_stderr(errors):
                changes = config_changes(path, stop, interval=0.01)
                first = await asyncio.wait_for(anext(changes), 5)
                self.assertEqual(first.projects["hibernaut"].default_agent, "steward")
                path.write_text(CONFIG.replace('agents = ["sde", "code-reviewer"]', 'agents = ["sde", "ghost"]'))
                pending = asyncio.ensure_future(anext(changes))
                for _ in range(500):
                    if "keeping current settings" in errors.getvalue():
                        break
                    await asyncio.sleep(0.01)
                self.assertFalse(pending.done())
                self.assertIn("'ghost' is not declared", errors.getvalue())
                path.write_text(CONFIG.replace("develops tasks", "fixed"))
                fixed = await asyncio.wait_for(pending, 5)
                self.assertEqual(fixed.agents["sde"].description, "fixed")
            stop.set()


if __name__ == "__main__":
    unittest.main()
