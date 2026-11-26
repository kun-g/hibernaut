from __future__ import annotations

import ast
import dataclasses
import sys
import tempfile
import unittest
from pathlib import Path

from switchboard.config import ConfigError, load_config, parse_config
from switchboard.envelope import CanonicalEvent, EnvelopeError, new_event_id, validate_envelope
from switchboard.router import Router
from switchboard.store_sqlite import SwitchboardStore

REPO_ROOT = Path(__file__).resolve().parents[2]
SWITCHBOARD_ROOT = REPO_ROOT / "src" / "switchboard"
STDLIB = frozenset(sys.stdlib_module_names) | {"__future__"}


def _work(**overrides: object) -> CanonicalEvent:
    fields: dict[str, object] = {
        "source": "github",
        "event_type": "work.opened",
        "external_id": "issue:kun-g/hibernaut#1:opened:t",
        "scope": "proj:hibernaut",
        "correlation_key": "github:kun-g/hibernaut#1",
        "subject": "title",
        "metadata": {"work_kind": "issue", "state": "open"},
    }
    fields.update(overrides)
    return CanonicalEvent(**fields)  # type: ignore[arg-type]


class EnvelopeTests(unittest.TestCase):
    def test_event_is_immutable_and_metadata_is_read_only(self) -> None:
        source = {"work_kind": "issue", "state": "open"}
        event = _work(metadata=source)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            event.scope = "other"  # type: ignore[misc]
        with self.assertRaises(TypeError):
            event.metadata["state"] = "closed"  # type: ignore[index]
        source["state"] = "mutated"
        self.assertEqual(event.metadata["state"], "open")

    def test_metadata_must_be_json(self) -> None:
        with self.assertRaises(ValueError):
            _work(metadata={"work_kind": "issue", "state": "open", "bad": object()})

    def test_event_id_is_prefixed_ulid(self) -> None:
        first, second = new_event_id(), new_event_id()
        self.assertTrue(first.startswith("evt_"))
        self.assertEqual(len(first), 30)
        self.assertNotEqual(first, second)
        self.assertLessEqual(first[4:14], second[4:14])

    def test_json_round_trip(self) -> None:
        event = _work()
        self.assertEqual(CanonicalEvent.from_json(event.to_json()), event)

    def test_valid_events_per_type(self) -> None:
        validate_envelope(_work())
        validate_envelope(
            _work(event_type="work.ready", metadata={"work_kind": "task", "state": "ready", "task_id": "t1"})
        )
        validate_envelope(
            _work(event_type="conversation.message", subject="", body="hi", metadata={"channel": "feishu"})
        )
        validate_envelope(
            _work(
                event_type="approval.decided",
                actor="ou_1",
                metadata={"approval_request_id": "apr_1", "decision": "approve"},
            )
        )
        validate_envelope(
            _work(
                source="hibernaut",
                event_type="execution.reported",
                metadata={"run_id": "r", "session_id": "s", "outcome_kind": "suspend", "approval_request_id": "a"},
            )
        )

    def test_missing_fields_are_reported(self) -> None:
        cases = [
            (_work(correlation_key=None), "work.opened requires correlation_key"),
            (_work(subject=""), "work.opened requires subject"),
            (_work(metadata={"work_kind": "issue"}), "metadata.state"),
            (_work(event_type="work.ready"), "metadata.task_id"),
            (_work(event_type="conversation.message", body="", metadata={"channel": "x"}), "requires body"),
            (_work(event_type="conversation.message", body="b", metadata={}), "metadata.channel"),
            (_work(event_type="approval.decided", metadata={"approval_request_id": "a", "decision": "d"}), "requires actor"),
            (_work(event_type="approval.decided", actor="a", metadata={"decision": "d"}), "metadata.approval_request_id"),
            (
                _work(event_type="execution.reported", metadata={"run_id": "r", "session_id": "s", "outcome_kind": "silent"}),
                "outcome_kind must be one of",
            ),
            (_work(event_type="execution.reported", metadata={"run_id": "r", "outcome_kind": "emit"}), "metadata.session_id"),
            (_work(scope=""), "missing scope"),
            (_work(occurred_at="yesterday"), "occurred_at"),
        ]
        for event, expected in cases:
            with self.subTest(expected=expected):
                with self.assertRaises(EnvelopeError) as ctx:
                    validate_envelope(event)
                self.assertIn(expected, str(ctx.exception))


class StoreTests(unittest.TestCase):
    def test_pragmas_tables_and_indexes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = SwitchboardStore(str(Path(tmp) / "sb.db"))
            try:
                self.assertEqual(store.pragma("journal_mode"), "wal")
                self.assertEqual(store.pragma("busy_timeout"), 5000)
                self.assertEqual(store.pragma("foreign_keys"), 1)
                names = store.table_names()
                for name in (
                    "inbound_events",
                    "delivery_log",
                    "outbound_messages",
                    "idx_inbound_corr",
                    "idx_delivery_sched",
                    "idx_delivery_order",
                    "idx_outbound_sched",
                ):
                    self.assertIn(name, names)
            finally:
                store.close()


class ConfigTests(unittest.TestCase):
    def test_defaults(self) -> None:
        config = parse_config({})
        self.assertEqual(config.http.max_body_bytes, 1_048_576)
        self.assertEqual(config.sqlite.busy_timeout_ms, 5000)
        self.assertEqual(config.routing.default_scope, "user:main")
        self.assertEqual(config.routing.unmatched_policy, "fallback")
        self.assertTrue(config.feishu.group_require_mention)
        self.assertEqual(config.delivery.retry_backoff_seconds, (5, 15, 45, 135, 405))
        self.assertEqual(config.outbound.rate_limit_per_target_rpm, 30)
        self.assertEqual(config.outbound.max_retry_attempts, 5)
        self.assertIsNone(config.github.require_label)

    def test_github_require_label(self) -> None:
        self.assertEqual(parse_config({"github": {"require_label": " hibernaut "}}).github.require_label, "hibernaut")
        self.assertIsNone(parse_config({"github": {"require_label": ""}}).github.require_label)

    def test_example_file_loads(self) -> None:
        config = load_config(REPO_ROOT / "switchboard.example.toml")
        self.assertEqual(config.routing.github_repos["kun-g/hibernaut"], "proj:hibernaut")
        self.assertEqual(config.routing.unmatched_policy, "fallback")

    def test_event_consumers_override_only_the_listed_event_types(self) -> None:
        config = parse_config(
            {"routing": {"event_consumers": {"work.opened": ["hibernaut"], "execution.reported": ["switchboard.outbound"]}}}
        )
        routes = config.routing.event_consumers
        self.assertEqual(routes, {"work.opened": ("hibernaut",), "execution.reported": ("switchboard.outbound",)})
        router = Router.from_config(config.routing)
        opened = _work(event_type="work.opened")
        updated = _work(event_type="work.updated")
        self.assertEqual(router.resolve(opened), ("hibernaut",))
        self.assertEqual(router.resolve(updated), ("taskboard",))

    def test_event_consumers_reject_unknown_types_and_non_lists(self) -> None:
        with self.assertRaises(ConfigError):
            parse_config({"routing": {"event_consumers": {"work.started": ["hibernaut"]}}})
        with self.assertRaises(ConfigError):
            parse_config({"routing": {"event_consumers": {"work.opened": "hibernaut"}}})

    def test_invalid_policy(self) -> None:
        with self.assertRaises(ConfigError):
            parse_config({"routing": {"unmatched_policy": "drop"}})


class SwitchboardBoundaryTests(unittest.TestCase):
    def test_switchboard_imports_only_stdlib_and_itself(self) -> None:
        for py_file in SWITCHBOARD_ROOT.rglob("*.py"):
            tree = ast.parse(py_file.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                names: list[str] = []
                if isinstance(node, ast.Import):
                    names = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                    names = [node.module]
                for name in names:
                    top = name.split(".")[0]
                    with self.subTest(file=py_file.name, module=name):
                        self.assertTrue(top in STDLIB or top == "switchboard")

    def test_no_ingress_naming(self) -> None:
        for py_file in SWITCHBOARD_ROOT.rglob("*.py"):
            self.assertNotIn("ingress", py_file.read_text(encoding="utf-8").lower(), py_file.name)


if __name__ == "__main__":
    unittest.main()
