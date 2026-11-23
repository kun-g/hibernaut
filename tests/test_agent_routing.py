from __future__ import annotations

import unittest
from dataclasses import FrozenInstanceError, replace

from agent_runtime.core.models import (
    MAX_FORWARD_HOPS,
    AgentNotAllowed,
    ForwardSpec,
    InboxBinding,
    InboxMatchRule,
    SessionRef,
    resolve_agent_id,
    resolve_executor_id,
)

BINDING = InboxBinding(
    inbox="proj:hibernaut",
    default_agent="steward",
    agents=("steward", "code-reviewer", "ops"),
    rules=(
        InboxMatchRule(agent="ops", source="timer:ops"),
        InboxMatchRule(agent="code-reviewer", tags=("review",)),
        InboxMatchRule(agent="steward", source="github:issues", event_type="issue.opened"),
    ),
)


def _session(status: str = "active", agent: str | None = "code-reviewer") -> SessionRef:
    return SessionRef(session_id="s1", inbox_id="proj:hibernaut", executor_id=agent, status=status)  # type: ignore[arg-type]


class ResolveAgentIdTests(unittest.TestCase):
    def resolve(self, event_agent: str | None = None, session: SessionRef | None = None, **kw: object) -> str:
        binding = kw.pop("binding", BINDING)
        return resolve_agent_id(event_agent, session, binding, "project-default", "runtime-default", **kw)  # type: ignore[arg-type]

    def test_level_1_event_names_the_agent(self) -> None:
        self.assertEqual(self.resolve("ops", _session(), source="timer:ops", tags=("review",)), "ops")

    def test_level_1_outside_the_whitelist_is_refused_not_rerouted(self) -> None:
        with self.assertRaises(AgentNotAllowed) as refused:
            self.resolve("sde", _session(), source="timer:ops")
        self.assertIn("sde", str(refused.exception))
        self.assertIn("proj:hibernaut", str(refused.exception))
        self.assertIsInstance(refused.exception, ValueError)

    def test_level_1_without_a_whitelist_accepts_any_agent(self) -> None:
        self.assertEqual(self.resolve("sde", binding=replace(BINDING, agents=())), "sde")
        self.assertEqual(self.resolve("sde", binding=None), "sde")

    def test_level_2_open_session_sticks_over_rules(self) -> None:
        for status in ("active", "suspended"):
            self.assertEqual(self.resolve(None, _session(status), source="timer:ops"), "code-reviewer")

    def test_level_2_closed_session_does_not_stick(self) -> None:
        for status in ("closed", "archived"):
            self.assertEqual(self.resolve(None, _session(status), source="timer:ops"), "ops")
        self.assertEqual(self.resolve(None, _session(agent=None)), "steward")

    def test_level_3_first_matching_rule_wins(self) -> None:
        self.assertEqual(self.resolve(source="timer:ops", tags=("review",)), "ops")
        self.assertEqual(self.resolve(source="switchboard", tags=("docs", "review")), "code-reviewer")

    def test_level_3_every_condition_of_a_rule_must_hold(self) -> None:
        rule = InboxMatchRule(agent="a", source="github", event_type="pr", tags=("x", "y"))
        self.assertTrue(rule.matches(source="github", event_type="pr", tags=("y", "x", "z")))
        self.assertFalse(rule.matches(source="github", event_type="pr", tags=("x",)))
        self.assertFalse(rule.matches(source="github", event_type=None, tags=("x", "y")))
        self.assertFalse(rule.matches(source="github:pr", event_type="pr", tags=("x", "y")))

    def test_level_3_a_rule_without_conditions_matches_nothing(self) -> None:
        self.assertFalse(InboxMatchRule(agent="a").matches(source="any", event_type=None, tags=()))

    def test_level_4_binding_default(self) -> None:
        self.assertEqual(self.resolve(source="switchboard"), "steward")

    def test_level_5_project_then_runtime_default(self) -> None:
        self.assertEqual(self.resolve(binding=None), "project-default")
        self.assertEqual(resolve_agent_id(None, None, None, None, "runtime-default"), "runtime-default")

    def test_models_are_immutable(self) -> None:
        with self.assertRaises(FrozenInstanceError):
            BINDING.rules[0].agent = "x"  # type: ignore[misc]


class LegacyCompatibilityTests(unittest.TestCase):
    def test_resolve_executor_id_keeps_its_four_levels(self) -> None:
        session = _session(agent="from-session")
        self.assertEqual(resolve_executor_id("explicit", session, "project", "global"), "explicit")
        self.assertEqual(resolve_executor_id(None, session, "project", "global"), "from-session")
        self.assertEqual(resolve_executor_id(None, None, "project", "global"), "project")
        self.assertEqual(resolve_executor_id(None, None, None, "global"), "global")

    def test_session_agent_id_is_the_stored_executor_id(self) -> None:
        self.assertEqual(_session(agent="sde").agent_id, "sde")
        self.assertIsNone(_session(agent=None).agent_id)


class ForwardSpecTests(unittest.TestCase):
    def test_defaults_and_hop_limit(self) -> None:
        spec = ForwardSpec(target_inbox="proj:x:sde")
        self.assertEqual((spec.target_agent, spec.return_to, spec.hop_count, spec.forwarded_from), (None, None, 0, None))
        self.assertEqual(MAX_FORWARD_HOPS, 3)


if __name__ == "__main__":
    unittest.main()
