from __future__ import annotations

import subprocess
import sys
import time
import unittest
from unittest.mock import patch

from agent_runtime.app.onboarding import run_command
from tests.support.onboarding import OnboardingCase


class CompleteTests(OnboardingCase):

    def test_r6_the_way_back_works(self) -> None:
        found = self.complete("bare")
        subprocess.run(found["undo"].split(" (")[0].split(), check=True)
        self.unchanged()


class HelperTests(unittest.TestCase):
    def test_c1_commands_get_a_fixed_argument_list_and_a_time_limit(self) -> None:
        done = run_command([sys.executable, "-c", "import sys; print(sys.argv[1:])", "a; echo b", "$(id)"])
        self.assertEqual(done.stdout.strip(), "['a; echo b', '$(id)']")
        self.assertEqual(run_command(["no-such-command-here"]).returncode, 127)
        started = time.monotonic()
        script = "import subprocess,sys; subprocess.run([sys.executable, '-c', 'import time; time.sleep(60)'])"
        with patch("agent_runtime.app.onboarding._COMMAND_TIMEOUT_SECONDS", 0.5):
            done = run_command([sys.executable, "-c", script])
        self.assertEqual(done.returncode, 124)
        self.assertLess(time.monotonic() - started, 20)
        self.assertIn("did not finish", done.stderr)


if __name__ == "__main__":
    unittest.main()
