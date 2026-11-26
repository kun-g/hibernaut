import json
import tempfile
import unittest
from pathlib import Path

from agent_runtime.adapters.executor_cli.peer_handoff import PeerHandoffConfig, parse_result
from agent_runtime.adapters.executor_cli.sde import SDE_SCHEMA
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config
from tests.support.peer_handoff import DONE


class TestParseResult(unittest.TestCase):
    def test_order_of_checks_and_fenced_json(self):
        ok = {"task_id": "t", "attempt_id": "a", **DONE}
        self.assertEqual(parse_result(json.dumps(ok), SDE_SCHEMA, task_id="t", attempt_id="a"), (DONE, ""))
        fenced = "```json\n" + json.dumps(ok) + "\n```"
        self.assertEqual(parse_result(fenced, SDE_SCHEMA, task_id="t", attempt_id="a")[0], DONE)
        self.assertIn("not a JSON object", parse_result("[1]", SDE_SCHEMA, task_id="t", attempt_id="a")[1])
        no_ids = json.dumps(DONE)
        self.assertIn("result.task_id is required", parse_result(no_ids, SDE_SCHEMA, task_id="t", attempt_id="a")[1])
        wrong_task = json.dumps({**ok, "task_id": "other"})
        self.assertIn('task_id is "other"', parse_result(wrong_task, SDE_SCHEMA, task_id="t", attempt_id="a")[1])
        bad_enum = json.dumps({**ok, "status": "maybe"})
        self.assertIn("result.status must be one of", parse_result(bad_enum, SDE_SCHEMA, task_id="t", attempt_id="a")[1])


class TestPeerConfig(unittest.TestCase):
    BASE = {
        "runtime": {"default_executor": "sde"},
        "executors": {"sde": {"role": "sde", "review": False}},
        "projects": {"demo": {"inbox_id": "proj:demo", "repo": "o/demo", "checkout": "/tmp", "worktree_backend": "git",
                              "terminal_backend": "orca"}},
    }

    def config(self, project: dict | None = None, peer: dict | None = None, **runtime):
        raw = json.loads(json.dumps(self.BASE))
        raw["projects"]["demo"].update(project or {})
        raw["runtime"].update(runtime)
        if peer is not None:
            raw["peer"] = peer
        return parse_config(raw, base_dir=Path("/etc/hb"))

    def test_handoff_defaults_to_mcp_and_peer_settings_parse(self):
        self.assertEqual(self.config().projects["demo"].handoff, "mcp")
        cfg = self.config({"handoff": "peer"}, {"receiver": "hb-2", "state_dir": "state", "config_dirs": ["~/.claude"],
                                               "max_resends": 0, "register_timeout_seconds": 30})
        self.assertEqual(cfg.projects["demo"].handoff, "peer")
        self.assertEqual((cfg.peer.receiver, cfg.peer.state_dir, cfg.peer.max_resends, cfg.peer.register_timeout),
                         ("hb-2", Path("/etc/hb/state"), 0, 30.0))
        self.assertEqual(cfg.peer.config_dirs, (Path.home() / ".claude",))
        self.assertEqual(self.config().peer, PeerHandoffConfig())

    def test_invalid_values_are_rejected(self):
        for project, peer, message in (
            ({"handoff": "files"}, None, "Unsupported handoff"),
            ({}, {"receiver": "a b"}, "peer.receiver"),
            ({}, {"max_resends": -1}, "peer.max_resends"),
            ({}, {"exit_timeout_seconds": 0}, "peer.exit_timeout_seconds"),
            ({}, {"config_dirs": []}, "peer.config_dirs"),
            ({}, {"socket": "x"}, "unknown keys: socket"),
        ):
            with self.subTest(message), self.assertRaisesRegex(ValueError, message):
                self.config(project, peer)

    def test_startup_fails_clearly_when_the_receiver_is_not_running(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = self.config({"handoff": "peer"}, {"state_dir": tmp}, db_path=f"{tmp}/hb.db",
                              memory_root=f"{tmp}/mem")
            with self.assertRaisesRegex(ValueError, r'handoff = "peer" for demo, but the ccpeer receiver '
                                                      r"'hibernaut' is not running"):
                build_daemon_from_config(cfg)


if __name__ == "__main__":
    unittest.main()
