from __future__ import annotations

import shutil
import subprocess
import unittest

from tests.support.switchboard_deploy import REPO_ROOT


class DockerKitTests(unittest.TestCase):

    @unittest.skipUnless(shutil.which("git"), "git not installed")
    def test_local_secrets_and_instances_are_ignored(self) -> None:
        def ignored(rel: str) -> bool:
            result = subprocess.run(
                ["git", "check-ignore", "-q", "--no-index", rel], cwd=REPO_ROOT, check=False
            )
            return result.returncode == 0

        for rel in (
            "deploy/switchboard/.env",
            "deploy/switchboard/switchboard.toml",
            "deploy/switchboard/Caddyfile",
        ):
            self.assertTrue(ignored(rel), rel)
        for rel in (
            "deploy/switchboard/.env.example",
            "deploy/switchboard/switchboard.docker.toml",
            "deploy/switchboard/Caddyfile.example",
        ):
            self.assertFalse(ignored(rel), rel)


if __name__ == "__main__":
    unittest.main()
