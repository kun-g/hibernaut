import asyncio
import contextlib
import json
import os
import signal
import tempfile
import unittest
import uuid
from functools import partial
from pathlib import Path
from unittest.mock import Mock, patch

from agent_runtime.adapters.executor_cli import OrcaWorktreeManager
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.orca import OrcaCommandTerminal, OrcaVisibleRunner
from agent_runtime.adapters.executor_cli.reviewer import CoderReviewerExecutor
from agent_runtime.adapters.executor_cli.sde import SDEExecutor, SDEOptions
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteSessionStoreAdapter, SQLiteTaskMetadataStore
from agent_runtime.adapters.storage_sqlite.terminals import SQLiteTerminalBindingStore
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from agent_runtime.app.main import build_daemon_from_config
from agent_runtime.core.models import SessionRef
from tests.support.empty_guidance import empty_project_guidance
from tests.support.fakes import InMemoryPermission
from tests.support.coder_reviewer import fake_command, git
from tests.support.sde_executor import make_base_repo


def poll_test_terminals(root):
    # Use the real CLI and handoff files, including when a config reload builds a new daemon.
    return patch.multiple(
        "agent_runtime.app.main",
        OrcaVisibleRunner=partial(OrcaVisibleRunner, poll_interval=0.01, handoff_root=root / "handoff"),
        OrcaCommandTerminal=partial(OrcaCommandTerminal, poll_interval=0.01, root=root / "cli-runs"),
    )


def build_terminal_daemon(config, root, **kwargs):
    with poll_test_terminals(root):
        return build_daemon_from_config(config, **kwargs)


class FakeOrcaTerminals:
    """Orca CLI double that runs each terminal's --command in a real shell, the way Orca types it into a login
    shell, so the gate, the billing probe and the interactive agent all really run. `terminal send` types a
    line into the running process; `terminal list` holds only terminals whose process is still running.
    `terminal close` sends SIGTERM to the terminal's own process only (real Orca signals the whole tree), so a
    test sees what the wrapper ends by itself, then SIGKILL after a grace period. With `lose_closes` set, close
    reports success and drops the terminal from the list without signalling anything, as Orca sometimes does."""

    CLOSE_GRACE_SECONDS = 5.0

    def __init__(self, root: Path) -> None:
        self.root = root
        self.terminals: dict[str, dict] = {}
        self.calls: list[tuple[str, ...]] = []
        self.lose_closes = False

    def _info(self, handle: str) -> dict:
        term = self.terminals[handle]
        return {"handle": handle, "incarnationId": term["incarnation"], "title": term["title"],
                "connected": True, "writable": True, "agentIdentity": "claude",
                "worktreePath": term["worktree"]}

    async def __call__(self, *args: str, cwd: Path) -> dict:
        self.calls.append(args)
        verb = " ".join(args[:2])
        flags = dict(zip(args[2::2], args[3::2]))
        if verb == "terminal create":
            worktree = flags["--worktree"].removeprefix("path:")
            handle = f"term_{uuid.uuid4().hex[:8]}"
            screen = (self.root / f"{handle}.screen").open("wb")
            proc = await asyncio.create_subprocess_exec(
                "/bin/sh", "-c", flags["--command"], cwd=worktree,
                stdin=asyncio.subprocess.PIPE, stdout=screen, stderr=asyncio.subprocess.STDOUT,
            )
            screen.close()
            self.terminals[handle] = {"proc": proc, "title": flags["--title"], "worktree": worktree,
                                      "incarnation": uuid.uuid4().hex, "command": flags["--command"]}
            return {"terminal": {"handle": handle, "incarnationId": self.terminals[handle]["incarnation"],
                                 "title": flags["--title"]}}
        if verb == "terminal list":
            return {"terminals": [self._info(h) for h, t in self.terminals.items()
                                  if t["proc"].returncode is None and not t.get("lost")]}
        handle = flags.get("--terminal", "")
        term = self.terminals.get(handle)
        if verb == "terminal show":
            if term is None:
                raise RuntimeError("terminal not found")
            return {"terminal": self._info(handle)}
        if verb == "terminal send":
            assert term is not None and term["proc"].returncode is None
            term["proc"].stdin.write((flags["--text"] + "\n").encode())
            await term["proc"].stdin.drain()
            return {"send": {"handle": handle, "accepted": True, "prompt": {"processIncarnation": term["incarnation"]}}}
        if verb == "terminal close":
            # Orca may drop a terminal whose command ended and then refuses its handle.
            if term is None or term["proc"].returncode is not None:
                raise RuntimeError('{"ok": false, "error": {"code": "terminal_handle_stale"}}')
            if self.lose_closes:
                term["lost"] = True
            else:
                await self.close(handle)
            return {"close": {"handle": handle, "ptyKilled": True}}
        raise AssertionError(f"unexpected orca call {args}")

    async def close(self, handle: str) -> None:
        proc = self.terminals[handle]["proc"]
        if proc.returncode is None:
            self._signal(proc, signal.SIGTERM)
            try:
                await asyncio.wait_for(proc.wait(), timeout=self.CLOSE_GRACE_SECONDS)
            except TimeoutError:
                await self.kill(handle)

    async def kill(self, handle: str) -> None:
        proc = self.terminals[handle]["proc"]
        if proc.returncode is None:
            self._signal(proc, signal.SIGKILL)
            await proc.wait()

    @staticmethod
    def _signal(proc: asyncio.subprocess.Process, signum: int) -> None:
        # Not proc.terminate() or proc.kill(): before Python 3.13 they poll the process first, which reaps one that just
        # exited, and asyncio, finding its status gone, reports 255 as the exit code.
        with contextlib.suppress(ProcessLookupError):
            os.kill(proc.pid, signum)

    async def close_all(self) -> None:
        for handle in self.terminals:
            await self.kill(handle)

    def created(self) -> list[dict]:
        return [t for t in self.terminals.values()]

    def verbs(self) -> list[str]:
        return [" ".join(c[:2]) for c in self.calls]

    def screen(self, handle: str) -> str:
        return (self.root / f"{handle}.screen").read_text(errors="replace")


# Default for the case helpers: the case's own Orca runner.
_RUNNER = object()


def arg(call: dict, flag: str) -> str:
    return call["argv"][call["argv"].index(flag) + 1]


class VisibleCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.enterContext(empty_project_guidance())
        self.root = Path(os.path.realpath(self.enterContext(tempfile.TemporaryDirectory())))
        self.repo = make_base_repo(self.root)
        git(self.repo, "checkout", "-qb", "hb/task-1")
        self.log = self.root / "calls.jsonl"
        self.plan = self.root / "plan.json"
        self.enterContext(patch.dict(os.environ, {
            "FAKE_CLAUDE_LOG": str(self.log), "FAKE_PLAN": str(self.plan), "FAKE_SESSIONS": str(self.root / "sessions"),
        }))
        self.orca = FakeOrcaTerminals(self.root)
        self.enterContext(patch("agent_runtime.adapters.executor_cli.orca._orca", new=self.orca))
        self.db = SQLiteDatabase(":memory:")
        self.addCleanup(self.db.close)
        self.workspaces = SQLiteWorkspaceStore(self.db)
        self.tasks = SQLiteTaskMetadataStore(self.db)
        self.bindings = SQLiteTerminalBindingStore(self.db)
        self.runner = OrcaVisibleRunner(bindings=self.bindings, poll_interval=0.02, liveness_interval=0.2,
                                        handoff_root=self.root / "handoff")
        self.caps = Mock(permissions=InMemoryPermission())
        self.caps.is_interrupted.return_value = False
        self.caps.memory.load_baseline.return_value = {}

    async def asyncTearDown(self):
        await self.orca.close_all()

    def set_plan(self, sde=(), reviewer=()):
        self.plan.write_text(json.dumps({"sde": list(sde), "reviewer": list(reviewer)}))

    def launcher(self):
        return ClaudeCodeLauncher(name="fake", command=fake_command(self.root))

    def reviewer(self, terminal=_RUNNER):
        return CoderReviewerExecutor(
            profile=ClaudeCodeProfile(name="coder-reviewer", launcher=self.launcher(), model="claude-sonnet-5-5",
                                      effort="medium"),
            workspaces=self.workspaces, tasks=self.tasks, terminal=self.runner if terminal is _RUNNER else terminal,
        )

    def sde(self, review=True, timeout=30.0, terminal=_RUNNER, reviewer_terminal=_RUNNER, **overrides):
        overrides.setdefault("launcher", self.launcher())
        return SDEExecutor(
            profile=ClaudeCodeProfile(name="sde", timeout_seconds=timeout, **overrides),
            options=SDEOptions(review=review, reviewer="coder-reviewer"),
            reviewer=self.reviewer(reviewer_terminal) if review else None,
            worktree_manager=OrcaWorktreeManager(workspace_store=self.workspaces,
                                                 session_store=SQLiteSessionStoreAdapter(self.db), backend="git"),
            inbox_checkout_map={}, tasks=self.tasks,
            terminal=self.runner if terminal is _RUNNER else terminal,
        )

    def calls(self, role=None, mode=None):
        rows = [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []
        return [r for r in rows if (role is None or r["role"] == role) and (mode is None or r["mode"] == mode)]

    def payload(self, **extra):
        return {"executor_id": "sde", "text": "divide must reject a zero divisor",
                "explicit_workdir": str(self.repo), "base_ref": "main", **extra}

    @staticmethod
    def session_after(outcome, **extra):
        return SessionRef(session_id=outcome.session_id, inbox_id="proj:x", correlation_key="task:1",
                          executor_id="sde", executor_session_id=outcome.executor_session_id, **extra)
