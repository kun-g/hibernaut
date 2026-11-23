import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

FAKE_CLAUDE = Path(__file__).with_name("fake_claude.py")
FAKE_CLI = Path(__file__).with_name("fake_cli_reviewer.py")
CALLS = 16


class TestFixtureGitMaintenance(unittest.TestCase):
    # git starts `maintenance run --auto` detached after these commands; its lock and repack race whatever copies or
    # clones the repository next.
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.trace = self.root / "trace"
        self.trace.mkdir()
        self.env = {**os.environ, "GIT_TRACE2_EVENT": str(self.trace)}

    def git(self, cwd, *args):
        subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@example.invalid", *args], cwd=cwd, check=True,
                       capture_output=True, env=self.env)

    def test_no_fixture_command_starts_maintenance_not_even_in_the_remote_it_pushes_to(self):
        bare, seed, other = self.root / "bare.git", self.root / "seed", self.root / "other"
        self.git(self.root, "init", "-q", "--bare", "-b", "main", str(bare))
        self.git(self.root, "clone", "-q", str(bare), str(seed))
        self.git(seed, "checkout", "-qb", "main")
        (seed / "a").write_text("a\n")
        self.git(seed, "add", "-A")
        self.git(seed, "commit", "-qm", "base")
        self.git(seed, "push", "-q", "origin", "main")
        self.git(self.root, "clone", "-q", str(bare), str(other))
        self.git(seed, "commit", "-q", "--allow-empty", "-m", "next")
        self.git(seed, "push", "-q", "origin", "main")
        self.git(other, "fetch", "-q")
        self.git(other, "merge", "-q", "origin/main")
        events = [json.loads(line) for path in self.trace.iterdir() for line in path.read_text().splitlines()]
        started = [e["argv"] for e in events if e["event"] == "child_start"]
        self.assertTrue(any("receive-pack" in " ".join(argv) for argv in started), started)
        self.assertEqual([argv for argv in started if {"maintenance", "gc"} & set(argv)], [])


class TestFakeCallCounters(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.plan = self.root / "plan.json"

    def test_concurrent_fake_claude_calls_each_take_their_own_step(self):
        steps = [{"commit": None, "output": {"step": n}} for n in range(CALLS)]
        self.plan.write_text(json.dumps({"sde": steps}))
        procs = [subprocess.Popen([sys.executable, str(FAKE_CLAUDE), "-p", "--model", "m"], cwd=self.root, text=True,
                                  env={**os.environ, "FAKE_PLAN": str(self.plan)}, stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE) for _ in range(CALLS)]
        # Every call waits for the end of its prompt, so they reach the counter together.
        for proc in procs:
            assert proc.stdin is not None
            proc.stdin.close()
            proc.stdin = None
        outputs = [proc.communicate(timeout=60)[0] for proc in procs]
        self.assertEqual([proc.returncode for proc in procs], [0] * CALLS)
        taken = [json.loads(out.splitlines()[-1])["structured_output"]["step"] for out in outputs]
        self.assertEqual(sorted(taken), list(range(CALLS)))

    def test_concurrent_fake_cli_calls_each_take_their_own_step(self):
        self.plan.write_text(json.dumps([{"stdout": str(n)} for n in range(CALLS)]))
        env = {**os.environ, "FAKE_CLI_PLAN": str(self.plan), "FAKE_CLI_LOG": str(self.root / "log")}
        procs = [subprocess.Popen([sys.executable, str(FAKE_CLI)], cwd=self.root, text=True, env=env,
                                  stdin=subprocess.DEVNULL, stdout=subprocess.PIPE) for _ in range(CALLS)]
        outputs = [proc.communicate(timeout=60)[0] for proc in procs]
        self.assertEqual([proc.returncode for proc in procs], [0] * CALLS)
        self.assertEqual(sorted(int(out) for out in outputs), list(range(CALLS)))


if __name__ == "__main__":
    unittest.main()
