import asyncio
import unittest

from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config
from tests.support.reclaim_sweep import INBOX


class TestSweepConfig(unittest.TestCase):
    def project(self, **runtime):
        return parse_config({
            "runtime": {"default_executor": "cli", "db_path": ":memory:", **runtime},
            "executors": {"cli": {"command": "true"}},
            "projects": {"x": {"repo": "o/x", "checkout": "/tmp/x", "inbox_id": INBOX}},
        })

    def test_default_interval_is_five_minutes(self):
        self.assertEqual(parse_config({}).runtime.worktree_sweep_interval_seconds, 300.0)

    def test_zero_turns_the_sweep_off_and_negative_is_refused(self):
        self.assertEqual(self.project(worktree_sweep_interval_seconds=0).runtime.worktree_sweep_interval_seconds, 0.0)
        for bad in (-1, float("inf"), float("nan")):
            with self.assertRaisesRegex(ValueError, "worktree_sweep_interval_seconds"):
                self.project(worktree_sweep_interval_seconds=bad)

    def test_a_configured_daemon_runs_the_sweep_at_the_configured_interval(self):
        daemon = build_daemon_from_config(self.project(worktree_sweep_interval_seconds=120))
        self.addCleanup(daemon.close)
        self.assertIsNotNone(daemon.worktree_sweeper)
        self.assertEqual(daemon.worktree_sweep_interval_seconds, 120.0)

    def test_interval_zero_builds_no_sweeper(self):
        daemon = build_daemon_from_config(self.project(worktree_sweep_interval_seconds=0))
        self.addCleanup(daemon.close)
        self.assertIsNone(daemon.worktree_sweeper)


class TestDaemonServices(unittest.IsolatedAsyncioTestCase):
    async def test_services_run_the_sweep_and_the_stop_signal_halts_it(self):
        daemon = build_daemon_from_config(parse_config({
            "runtime": {"default_executor": "cli", "db_path": ":memory:", "worktree_sweep_interval_seconds": 3600},
            "executors": {"cli": {"command": "true"}},
            "projects": {"x": {"repo": "o/x", "checkout": "/tmp/x", "inbox_id": INBOX}},
        }))
        self.addCleanup(daemon.close)
        rounds: list[int] = []
        assert daemon.worktree_sweeper is not None
        daemon.worktree_sweeper.sweep_once = lambda should_stop=None: rounds.append(1) or []  # type: ignore[method-assign]
        halt, services_stop = asyncio.Event(), asyncio.Event()
        services = asyncio.create_task(daemon.run_services(services_stop, halt=halt))
        for _ in range(100):
            if rounds:
                break
            await asyncio.sleep(0.02)
        self.assertEqual(rounds, [1])
        halt.set()
        await asyncio.sleep(0.1)
        # Halted: the sweep loop has ended while the other services keep running until services_stop.
        self.assertFalse(services.done())
        services_stop.set()
        await asyncio.wait_for(services, timeout=5)
        self.assertEqual(rounds, [1])


if __name__ == "__main__":
    unittest.main()
