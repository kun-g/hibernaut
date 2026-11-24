import tomllib
import unittest
from pathlib import Path

from agent_runtime.adapters.executor_cli.role_skills import builtin_skill, parse_skills, render_skills
from agent_runtime.app.config import load_config
from agent_runtime.core.models import MAX_FORWARD_HOPS

ROOT = Path(__file__).resolve().parent.parent
DOC = "docs/inbox-conventions.md"
NAMES = ("`proj:<p>`", "`proj:<p>:<role>`", "`proj:<p>:<role>:<slug>`", "`sys:*`", "`user:*`")


class TestStewardSkill(unittest.TestCase):
    def setUp(self):
        self.text = render_skills(parse_skills("agents.steward", "steward", ["builtin:steward"], ROOT))

    def test_the_built_in_skill_loads_with_the_naming_rules(self):
        self.assertTrue(builtin_skill("steward").is_file())
        self.assertIn("# Choosing an inbox", self.text)
        for name in NAMES:
            self.assertIn(name, self.text)

    def test_it_says_when_to_share_an_inbox_and_when_to_split(self):
        section = self.text.split("# Choosing an inbox", 1)[1].split("\n# ", 1)[0]
        for rule in ("one after another", "at the same time", "same inbox", "Taskboard", "DAG"):
            self.assertIn(rule, section)

    def test_it_claims_no_way_round_the_hard_limits(self):
        section = self.text.split("# Choosing an inbox", 1)[1].split("\n# ", 1)[0]
        for limit in ("permission", "workdir", "allowed agents", "hop"):
            self.assertIn(limit, section)


class TestConventionsDocument(unittest.TestCase):
    def test_it_is_in_the_guidance_catalog(self):
        docs = tomllib.loads((ROOT / "guidance.toml").read_text(encoding="utf-8"))["docs"]
        [entry] = [doc for doc in docs if doc["path"] == DOC]
        self.assertEqual(entry["tags"], ["inbox", "routing", "steward"])

    def test_it_states_the_names_and_the_hop_limit(self):
        text = (ROOT / DOC).read_text(encoding="utf-8")
        for name in (*NAMES, "[[runtime.inbox_bindings]]", "workdir", f"MAX_FORWARD_HOPS = {MAX_FORWARD_HOPS}"):
            self.assertIn(name, text)


class TestExampleConfig(unittest.TestCase):
    def test_the_example_loads_with_a_workdir_binding_and_an_agent_limit(self):
        config = load_config(ROOT / "hibernaut.example.toml")
        bound = config.runtime.workdir_binding("sys:patrol")
        assert bound is not None
        self.assertEqual((bound.binding.default_agent, bound.readonly, bound.system_tools), ("patrol", False, False))
        self.assertIsNotNone(config.agents["sde"].max_concurrency)

    def test_the_example_binds_the_system_steward_and_sends_the_chat_s_steward_to_it(self):
        config = load_config(ROOT / "hibernaut.example.toml")
        system, chat = (config.runtime.workdir_binding(i) for i in ("sys:ops", "user:main"))
        assert system is not None and chat is not None
        self.assertEqual((system.binding.default_agent, system.readonly, system.system_tools), ("ops", True, True))
        self.assertIn("sys:ops", chat.forward_to)
        self.assertEqual(config.onboarding.label_owners, ())


if __name__ == "__main__":
    unittest.main()
