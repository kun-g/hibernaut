import asyncio
import http.client
import json
import os
import socket
import sqlite3
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_runtime.adapters.executor_cli.orca import TerminalUnavailable
from agent_runtime.app.config import load_config, parse_config
from agent_runtime.app.main import build_daemon_from_config, serve_configured
from agent_runtime.core.models import Emit, Suspend
from tests.test_coder_reviewer import _Capture, fake_command, git
from tests.test_sde_executor import FIXED, PASS, dev, event, make_base_repo
from tests.test_visible_roles import FakeOrcaTerminals, VisibleCase

ROLES = {"sde": {"role": "sde"}, "coder-reviewer": {"role": "coder-reviewer"}}


class TestInteractiveConfig(unittest.TestCase):
    def test_interactive_is_a_role_setting_defaulting_to_true(self):
        cfg = parse_config({"executors": {"sde": {"role": "sde", "interactive": False},
                                          "coder-reviewer": {"role": "coder-reviewer"}}})
        self.assertFalse(cfg.roles["sde"].interactive)
        self.assertTrue(cfg.roles["coder-reviewer"].interactive)
        with self.assertRaisesRegex(ValueError, "executors.coder-reviewer: interactive must be true or false"):
            parse_config({"executors": {"coder-reviewer": {"role": "coder-reviewer", "interactive": "yes"}}})

    def test_legacy_visible_still_sets_interactive(self):
        for value in (False, True):
            cfg = parse_config({"executors": {"sde": {"role": "sde", "visible": value},
                                              "coder-reviewer": {"role": "coder-reviewer"}}})
            self.assertIs(cfg.roles["sde"].interactive, value)
        with self.assertRaisesRegex(ValueError, "executors.sde: visible must be true or false"):
            parse_config({"executors": {"sde": {"role": "sde", "visible": "no"},
                                        "coder-reviewer": {"role": "coder-reviewer"}}})

    def test_visible_and_interactive_together_are_rejected_even_when_equal(self):
        for old, new in ((True, True), (False, True), (True, False)):
            with self.assertRaisesRegex(ValueError, "executors.sde: both interactive and visible are set"):
                parse_config({"executors": {"sde": {"role": "sde", "visible": old, "interactive": new},
                                            "coder-reviewer": {"role": "coder-reviewer"}}})

    def test_terminal_backend_defaults_to_the_worktree_backend_only_for_orca(self):
        def project(**extra):
            cfg = parse_config({"executors": ROLES, "projects": {"p": {"repo": "o/p", "checkout": "/tmp/p", **extra}}})
            return cfg.projects["p"].terminal_backend

        self.assertEqual(project(), "orca")
        self.assertEqual(project(worktree_backend="orca"), "orca")
        self.assertEqual(project(worktree_backend="git"), "none")
        self.assertEqual(project(worktree_backend="git", terminal_backend="orca"), "orca")
        self.assertEqual(project(worktree_backend="orca", terminal_backend="none"), "none")
        for bad in ("tmux", "", 1):
            with self.assertRaisesRegex(ValueError, "Unsupported terminal_backend for 'p'"):
                project(terminal_backend=bad)


class TestInteractiveWithoutTerminal(VisibleCase):
    async def test_sde_suspends_with_the_reason_and_starts_nothing(self):
        self.set_plan(sde=[dev(FIXED)], reviewer=[PASS])
        executor = self.sde(review=False, terminal=TerminalUnavailable("project 'x' has terminal_backend = \"none\""))
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        self.assertIn("[sde] interactive session unavailable: project 'x' has terminal_backend = \"none\"",
                      outcome.prompt)
        self.assertIn('Set terminal_backend = "orca" on the project or interactive = false on executors.sde',
                      outcome.prompt)
        self.assertIn("then approve to retry; reject to stop the task", outcome.prompt)
        self.assertEqual((self.calls(), self.orca.calls), ([], []))

    async def test_approval_after_the_backend_is_configured_runs_in_a_terminal(self):
        self.set_plan(sde=[dev(FIXED)])
        executor = self.sde(review=False, terminal=TerminalUnavailable("no backend"))
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        # The config reload rebuilds the executor with a terminal backend; the person approves the retry.
        session = self.session_after(outcome, status="suspended", checkpoint=outcome.session_checkpoint,
                                     pending_request_id=outcome.request_id)
        answer = event({"approval_request_id": outcome.request_id, "approved": True})
        resumed = await self.sde(review=False).execute([answer], session, self.caps)
        assert isinstance(resumed, Emit), resumed
        self.assertIn("[sde] delivered (review=off", resumed.messages[0])
        (call,) = self.calls("sde", "interactive")
        self.assertIn("divide must reject a zero divisor", call["prompt"])
        self.assertEqual(self.calls("sde", "print"), [])

    async def test_rejecting_the_retry_stops_the_task(self):
        executor = self.sde(review=False, terminal=TerminalUnavailable("no backend"))
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        session = self.session_after(outcome, status="suspended", checkpoint=outcome.session_checkpoint,
                                     pending_request_id=outcome.request_id)
        answer = event({"approval_request_id": outcome.request_id, "approved": False})
        stopped = await executor.execute([answer], session, self.caps)
        assert isinstance(stopped, Emit), stopped
        self.assertIn("[sde] stopped: interactive session declined", stopped.messages[0])
        self.assertEqual(self.calls(), [])

    async def test_builtin_review_without_a_terminal_is_pending_not_headless(self):
        self.set_plan(sde=[dev(FIXED)], reviewer=[PASS])
        executor = self.sde(terminal=None, reviewer_terminal=TerminalUnavailable("no backend for the reviewer"))
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        self.assertIn("review pending: the review could not complete: interactive review not started: "
                      "no backend for the reviewer", outcome.prompt)
        self.assertEqual(self.calls("reviewer"), [])

    async def test_independent_review_without_a_terminal_is_reported(self):
        self.set_plan(reviewer=[PASS])
        reviewer = self.reviewer(terminal=TerminalUnavailable("no backend"))
        (self.repo / "calc.py").write_text(FIXED["calc.py"])
        review = event({"executor_id": "coder-reviewer", "text": "divide must reject zero",
                        "explicit_workdir": str(self.repo), "base_ref": "main"})
        outcome = await reviewer.execute([review], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("[coder-reviewer] review failed: interactive review not started: no backend",
                      outcome.messages[0])
        self.assertEqual((self.calls(), self.orca.calls), ([], []))

    async def test_a_backend_that_cannot_attach_is_reported_without_a_headless_run(self):
        self.set_plan(sde=[dev(FIXED)])

        async def refuse(*args, cwd):
            raise RuntimeError("worktree is not registered in Orca")

        with patch("agent_runtime.adapters.executor_cli.orca._orca", new=refuse):
            outcome = await self.sde(review=False).execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("development failed: Orca terminal (not started)", outcome.messages[0])
        self.assertIn("worktree is not registered in Orca", outcome.messages[0])
        self.assertEqual(self.calls(), [])


class TestTerminalBackendThroughDaemon(unittest.TestCase):
    """Role executors built from the config for an Orca project, Git worktree projects and an unmatched inbox."""

    def setUp(self):
        self.root = root = Path(os.path.realpath(self.enterContext(tempfile.TemporaryDirectory())))
        self.repo = make_base_repo(root)
        self.orca_wt = root / "hb-orca-1"
        git(self.repo, "worktree", "add", "-q", "-b", "hb/orca-1", str(self.orca_wt))
        self.log, self.plan = root / "calls.jsonl", root / "plan.json"
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.db_path = root / "hb.db"
        self.path = root / "hibernaut.toml"
        self.path.write_text(
            f'[runtime]\ndb_path = "{self.db_path}"\nmemory_root = "{root / "mem"}"\ndefault_executor = "sde"\n'
            f"[channels]\nport = {self.port}\n"
            f'[launchers.fake]\ncommand = "{fake_command(root)}"\n'
            '[executors.coder-reviewer]\nrole = "coder-reviewer"\nlauncher = "fake"\n'
            '[executors.sde]\nrole = "sde"\nlauncher = "fake"\nreview = false\n'
            '[executors.sde-bg]\nrole = "sde"\nlauncher = "fake"\nreview = false\ninteractive = false\n'
            f'[projects.orcap]\ninbox_id = "proj:orcap"\nrepo = "o/orcap"\ncheckout = "{self.repo}"\n'
            f'[projects.gitp]\ninbox_id = "proj:gitp"\nrepo = "o/gitp"\ncheckout = "{self.repo}"\n'
            'worktree_backend = "git"\n'
            f'[projects.gito]\ninbox_id = "proj:gito"\nrepo = "o/gito"\ncheckout = "{self.repo}"\n'
            'worktree_backend = "git"\nterminal_backend = "orca"\n'
        )
        self.orca = FakeOrcaTerminals(root)
        self.enterContext(patch("agent_runtime.adapters.executor_cli.orca._orca", new=self.orca))
        self.enterContext(patch.dict(os.environ, {"FAKE_CLAUDE_LOG": str(self.log), "FAKE_PLAN": str(self.plan),
                                                  "FAKE_SESSIONS": str(root / "sessions")}))
        self.stderr = self.enterContext(patch("sys.stderr", new_callable=_Capture))
        self.enterContext(patch("agent_runtime.adapters.channels.github_poller.GitHubIssuePoller.poll_once",
                                return_value=0))

    def post(self, inbox_id, key, payload):
        body = {"inbox_id": inbox_id, "external_id": key, "correlation_key": key, "payload": payload}
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=2)
        try:
            conn.request("POST", "/events", json.dumps(body), {"Content-Type": "application/json"})
            return conn.getresponse().status
        finally:
            conn.close()

    def outbox(self):
        with sqlite3.connect(self.db_path) as conn:
            return [row[0] for row in conn.execute("SELECT content FROM outbox ORDER BY seq")]

    def calls(self, role=None):
        rows = [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []
        return [r for r in rows if role is None or r["role"] == role]

    def run_events(self, events, done):
        async def scenario():
            config = load_config(self.path)
            daemon = build_daemon_from_config(config)
            daemon.workspaces.reserve_creating(work_key="orca-1", repo_checkout=str(self.repo),
                                               workdir=str(self.orca_wt), branch="hb/orca-1")
            daemon.workspaces.mark_ready("orca-1", workdir=str(self.orca_wt), branch="hb/orca-1")
            stop = asyncio.Event()
            server = asyncio.create_task(serve_configured(self.path, config, daemon, stop))

            async def wait_for(check):
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    self.assertFalse(server.done(), self.stderr.getvalue())
                    try:
                        if check():
                            return
                    except OSError:
                        pass
                    await asyncio.sleep(0.05)
                self.fail(f"outbox={self.outbox()} orca={self.orca.verbs()} {self.stderr.getvalue()}")

            try:
                first, *rest = events
                await wait_for(lambda: self.post(*first) == 202)
                for item in rest:
                    self.assertEqual(await asyncio.to_thread(self.post, *item), 202)
                await wait_for(lambda: done(self.outbox()))
            finally:
                stop.set()
                await server
                await self.orca.close_all()

        asyncio.run(scenario())

    @staticmethod
    def task(executor="sde", **extra):
        return {"executor_id": executor, "text": "divide must reject a zero divisor", "base_ref": "main", **extra}

    def test_git_worktree_project_without_a_terminal_backend_suspends_instead_of_running_headless(self):
        self.plan.write_text(json.dumps({"sde": [dev(FIXED)]}))
        self.run_events([("proj:gitp", "gitp-1", self.task())], lambda out: len(out) == 1)
        (message,) = self.outbox()
        self.assertIn("[sde] interactive session unavailable: project 'gitp' has terminal_backend = \"none\"", message)
        self.assertIn('Set terminal_backend = "orca" on the project or interactive = false on executors.sde', message)
        self.assertEqual((self.calls(), self.orca.calls), ([], []))

    def test_unmatched_inbox_suspends_the_developer_and_reports_the_reviewer(self):
        self.plan.write_text(json.dumps({"sde": [dev(FIXED)], "reviewer": [PASS]}))
        (self.orca_wt / "calc.py").write_text(FIXED["calc.py"])
        events = [
            ("adhoc", "adhoc-1", self.task(explicit_workdir=str(self.orca_wt))),
            ("adhoc", "adhoc-r", self.task("coder-reviewer", explicit_workdir=str(self.orca_wt))),
        ]
        self.run_events(events, lambda out: len(out) == 2)
        developer = next(m for m in self.outbox() if m.startswith("[sde]"))
        reviewer = next(m for m in self.outbox() if m.startswith("[coder-reviewer]"))
        self.assertIn("[sde] interactive session unavailable: the inbox matches no [projects] entry", developer)
        self.assertIn("[coder-reviewer] review failed: interactive review not started: the inbox matches no "
                      "[projects] entry", reviewer)
        self.assertIn("interactive = false on executors.coder-reviewer", reviewer)
        self.assertEqual((self.calls(), self.orca.calls), ([], []))

    def test_git_worktree_project_can_choose_orca_terminals(self):
        self.plan.write_text(json.dumps({"sde": [dev(FIXED)]}))
        self.run_events([("proj:gito", "gito-1", self.task())],
                        lambda out: any("[sde] delivered" in m for m in out))
        (call,) = self.calls("sde")
        self.assertEqual(call["mode"], "interactive")
        # The worktree came from the git backend; the terminal from Orca.
        workdir = Path(call["cwd"]).resolve()
        self.assertNotEqual(workdir, self.repo.resolve())
        listed = subprocess.run(["git", "worktree", "list", "--porcelain"], cwd=self.repo, check=True,
                                capture_output=True, text=True).stdout
        self.assertIn(f"worktree {workdir}\n", listed)
        (term,) = self.orca.created()
        self.assertEqual((term["title"], Path(term["worktree"]).resolve()), ("sde · gito-1", workdir))

    def test_orca_project_keeps_terminals_by_default_and_interactive_false_runs_headless(self):
        self.plan.write_text(json.dumps({"sde": [dev(FIXED), dev(FIXED)]}))
        events = [("proj:orcap", "orca-1", self.task()), ("proj:orcap", "orca-bg", self.task("sde-bg",
                                                                                           explicit_workdir=str(self.orca_wt)))]
        self.run_events(events, lambda out: sum("delivered" in m for m in out) == 2)
        modes = sorted(c["mode"] for c in self.calls("sde"))
        self.assertEqual(modes, ["interactive", "print"])
        (term,) = self.orca.created()
        self.assertEqual((term["title"], term["worktree"]), ("sde · orca-1", str(self.orca_wt)))


if __name__ == "__main__":
    unittest.main()
