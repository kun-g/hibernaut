import json
import os
import tempfile
import tomllib
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from agent_runtime.adapters.executor_cli.executor import DEFAULT_CLI_TEMPLATES
from agent_runtime.adapters.executor_cli.run_state import STATE_FILE
from agent_runtime.adapters.executor_cli.sde import _Progress, _State
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config
from agent_runtime.app.onboarding import project_agents
from tests.support.reviewer_candidates import GROK_PATTERN


EXAMPLE = Path(__file__).resolve().parents[2] / "hibernaut.example.toml"


def configured(reviewer, **sde):
    return parse_config({"executors": {
        "grok-reviewer": {"command": "grok"}, "cursor-reviewer": {"command": "cursor-agent"},
        "r": {"role": "code-reviewer"}, "other-sde": {"role": "sde", "review": False},
        "sde": {"role": "sde", "reviewer": reviewer, **sde},
    }})


class TestCandidateConfig(unittest.TestCase):
    def test_a_reviewer_is_one_name_or_an_ordered_list(self):
        self.assertEqual(configured("grok-reviewer").roles["sde"].sde.reviewer, "grok-reviewer")
        options = configured(["grok-reviewer", "r", "claude"]).roles["sde"].sde
        self.assertEqual(options.reviewer, ("grok-reviewer", "r", "claude"))
        self.assertEqual(options.candidates, ("grok-reviewer", "r", "claude"))
        self.assertEqual(configured("r").roles["sde"].sde.candidates, ("r",))
        # A list of one is still a list: it is chosen by availability.
        self.assertEqual(configured(["r"]).roles["sde"].sde.reviewer, ("r",))

    def test_every_candidate_follows_the_rules_of_a_single_reviewer(self):
        cases = {
            r"executors\.sde: reviewer 'sde' is this executor itself": ["grok-reviewer", "sde"],
            r"executors\.sde: reviewer 'other-sde' has role 'sde', which cannot review": ["r", "other-sde"],
            r"executors\.sde: reviewer 'ghost' is not a declared executor; choose": ["ghost", "r"],
            r"executors\.sde: reviewer '' is not a declared executor": ["r", " "],
            r"executors\.sde: reviewer 'r' is listed twice": ["r", "grok-reviewer", " r"],
            r"executors\.sde: reviewer must be an executor name or a non-empty list of them": [],
        }
        for message, reviewer in cases.items():
            with self.subTest(reviewer=reviewer), self.assertRaisesRegex(ValueError, message):
                configured(reviewer)
        for reviewer in (["r", 3], {"a": 1}, 3, True):
            with self.subTest(reviewer=reviewer), self.assertRaisesRegex(ValueError, "reviewer must be an executor name"):
                configured(reviewer)
        # With review off the names are not used, so they are not checked.
        self.assertEqual(configured(["ghost"], review=False).roles["sde"].sde.candidates, ("ghost",))

    def test_an_agent_names_itself_in_the_error(self):
        with self.assertRaisesRegex(ValueError, r"^agents\.dev: reviewer 'ghost' is not a declared executor"):
            parse_config({"executors": {"cc": {}, "g": {"command": "grok"}},
                          "agents": {"dev": {"role": "sde", "executor": "cc", "reviewer": ["g", "ghost"]}}})

    def test_the_availability_table_is_optional_and_its_path_is_resolved(self):
        self.assertEqual(parse_config({}).availability.file, "")
        self.assertEqual(parse_config({}).availability.quota_cooldown_seconds, 3600.0)
        with patch.dict(os.environ, {"HOME": "/home/kun"}):
            found = parse_config({"availability": {"file": "~/state/a.json"}}).availability
        self.assertEqual(found.file, "/home/kun/state/a.json")
        relative = parse_config({"availability": {"file": "a.json", "quota_cooldown_seconds": 60}},
                                base_dir=Path("/etc/hb")).availability
        self.assertEqual((relative.file, relative.quota_cooldown_seconds), ("/etc/hb/a.json", 60.0))
        for message, section in {
            r"\[availability\] must be a table": "a.json",
            r"availability: path not supported": {"path": "a.json"},
            r"availability: file must be a non-empty string": {"file": " "},
            r"availability\.quota_cooldown_seconds must be a positive number": {"quota_cooldown_seconds": 0},
        }.items():
            with self.subTest(message), self.assertRaisesRegex(ValueError, message):
                parse_config({"availability": section})

    def test_quota_errors_are_regular_expressions_of_an_executor(self):
        cfg = parse_config({
            "executors": {"g": {"command": "grok", "quota_errors": [GROK_PATTERN]},
                          "cc": {"quota_errors": ["limit reached"]},
                          "r": {"role": "code-reviewer", "quota_errors": ["old form"]}},
            "agents": {"g2": {"executor": "g"}, "cr": {"role": "code-reviewer", "executor": "cc"}},
        })
        self.assertEqual(cfg.executors["g"].quota_errors, (GROK_PATTERN,))
        self.assertEqual(cfg.executors["g2"].quota_errors, (GROK_PATTERN,))
        self.assertEqual(cfg.roles["cr"].profile.quota_errors, ("limit reached",))
        self.assertEqual(cfg.roles["r"].profile.quota_errors, ("old form",))
        for wrong in ("limit", [""], [3], ["("]):
            for item in ({"command": "grok"}, {}, {"role": "code-reviewer"}):
                with self.subTest(wrong=wrong, item=item), self.assertRaisesRegex(ValueError, r"executors\.g: quota_errors"):
                    parse_config({"executors": {"g": {**item, "quota_errors": wrong}}})
        with self.assertRaisesRegex(ValueError, r"agents\.a: quota_errors not supported"):
            parse_config({"executors": {"g": {"command": "grok"}}, "agents": {"a": {"executor": "g", "quota_errors": ["x"]}}})

    def test_only_grok_comes_with_a_pattern(self):
        with_patterns = {name: t.quota_errors for name, t in DEFAULT_CLI_TEMPLATES.items() if t.quota_errors}
        self.assertEqual(with_patterns, {"grok": (GROK_PATTERN,)})

    def test_the_example_lists_candidates_and_groks_pattern(self):
        text = EXAMPLE.read_text(encoding="utf-8")
        cfg = parse_config(tomllib.loads(text))
        self.assertEqual(cfg.executors["grok-reviewer"].quota_errors, (GROK_PATTERN,))
        for line in ("# [availability]", '# reviewer = ["grok-reviewer", "gpt-reviewer"]'):
            self.assertIn(line, text)

    def test_who_reviews_independently_is_the_first_listed_reviewer_role(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))

        def peer_of(reviewer):
            cfg = parse_config({
                "runtime": {"db_path": str(root / "hb.db"), "memory_root": str(root / "m"), "default_executor": "sde"},
                "executors": {"cli": {"command": "x"}, "a": {"role": "code-reviewer"}, "b": {"role": "code-reviewer"},
                              "sde": {"role": "sde", "reviewer": reviewer}},
            })
            daemon = build_daemon_from_config(cfg)
            self.addCleanup(daemon.db.close)
            return daemon.pool._worker_factory("anything", "w")._executors["sde"]._peer_reviewer  # pyright: ignore

        self.assertEqual([peer_of(r) for r in ("b", "cli", ["cli", "b", "a"], ["cli"])], ["b", "a", "b", "a"])

    def test_a_new_project_gets_the_first_candidate_that_is_an_agent(self):
        cfg = parse_config({
            "executors": {"r": {"role": "code-reviewer"}, "s": {"role": "sde", "reviewer": ["claude", "r"]},
                          "front": {"role": "steward"}},
        })
        self.assertEqual(project_agents(cfg).reviewer, "r")


class TestSavedTurn(unittest.TestCase):
    # A review turn a stopped process left behind belongs to the reviewer that started it.
    def slot(self, saved, reviewer):
        run = Mock(run_dir=self.enterContext(tempfile.TemporaryDirectory()))
        progress = _Progress(run, "s", saved_turn=saved)
        progress.phase("review", _State(stage="develop", workdir="/w", base_ref="", requirements="r"), None)
        slot = progress.slot(reviewer)
        return slot, lambda: json.loads(Path(run.run_dir, STATE_FILE).read_text())["turn"]

    def test_the_next_reviewer_never_picks_up_the_turn_of_the_one_it_replaced(self):
        saved = {"label": "review", "handle": "t", "reviewer": "grok-reviewer"}
        self.assertIsNone(self.slot(saved, "cursor-reviewer")[0].peek("review"))
        self.assertIsNotNone(self.slot(saved, "grok-reviewer")[0].peek("review"))
        # A single reviewer, and a turn recorded before reviewers were named, are picked up as before.
        self.assertIsNotNone(self.slot(saved, "")[0].peek("review"))
        self.assertIsNotNone(self.slot({"label": "review", "handle": "t"}, "cursor-reviewer")[0].peek("review"))

    def test_a_turn_is_recorded_with_its_reviewer(self):
        slot, recorded = self.slot(None, "grok-reviewer")
        slot.record("review", {"handle": "t"})
        self.assertEqual(recorded(), {"handle": "t", "label": "review", "reviewer": "grok-reviewer"})
        slot, recorded = self.slot(None, "")
        slot.record("review", {"handle": "t"})
        self.assertEqual(recorded(), {"handle": "t", "label": "review"})


if __name__ == "__main__":
    unittest.main()
