from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any

from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config


class GitHubPollSwitchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _raw(self, **project: Any) -> dict[str, Any]:
        return {
            "runtime": {"db_path": str(self.root / "hb.db"), "memory_root": str(self.root / "mem")},
            "projects": {"tool": {"inbox_id": "proj:tool", "repo": "kun-g/tool", "checkout": str(self.root), **project}},
        }

    def test_webhook_projects_are_not_polled(self) -> None:
        for raw, polled in ((self._raw(), True), (self._raw(github_poll=False), False)):
            daemon = build_daemon_from_config(parse_config(raw), demo=True)
            try:
                self.assertEqual(daemon.github_poller is not None, polled)
            finally:
                daemon.close()

    def test_github_poll_must_be_boolean(self) -> None:
        with self.assertRaisesRegex(ValueError, "github_poll must be true or false"):
            parse_config(self._raw(github_poll="no"))


if __name__ == "__main__":
    unittest.main()
