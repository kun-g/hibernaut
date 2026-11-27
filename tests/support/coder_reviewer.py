import atexit
import functools
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.reviewer import CoderReviewerExecutor
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase
from agent_runtime.adapters.storage_sqlite.tasks import SQLiteTaskMetadataStore
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from tests.support.fakes import InMemoryPermission
from tests.support.jsonl import read_jsonl


FAKE_CLAUDE = Path(__file__).with_name("fake_claude.py")
FAKE_LAUNCHER = Path(__file__).with_name("fake_launcher.sh")
GATEWAY_ENV = {"ANTHROPIC_BASE_URL": "http://127.0.0.1:3425", "ANTHROPIC_AUTH_TOKEN": "magpie"}
GATEWAY_MODEL = "codex/gpt-6.2-astra"
CHANGES_REQUESTED = {
    "verdict": "changes_requested",
    "summary": "division is unguarded",
    "blocking": [
        {
            "location": "calc.py:2",
            "trigger": "divide(1, 0)",
            "impact": "ZeroDivisionError crashes the caller",
            "fix": "reject zero divisors",
        }
    ],
    "notes": ["name the helper more clearly"],
}


def git(cwd, *args):
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True)


@functools.lru_cache(maxsize=1)
def _template_repo() -> Path:
    tmp = tempfile.TemporaryDirectory(prefix="coder_reviewer_tmpl_")
    atexit.register(tmp.cleanup)
    repo = Path(tmp.name) / "repo"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main")
    git(repo, "config", "user.email", "t@example.invalid")
    git(repo, "config", "user.name", "t")
    (repo / "calc.py").write_text("def divide(a, b):\n    return a\n")
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "base")
    git(repo, "checkout", "-qb", "feature")
    (repo / "calc.py").write_text("def divide(a, b):\n    return a / b\n")
    git(repo, "commit", "-qam", "divide")
    (repo / "notes.txt").write_text("untracked evidence\n")
    return repo


def make_repo(root: Path) -> Path:
    repo = root / "repo"
    shutil.copytree(_template_repo(), repo)
    return repo


def fake_command(root: Path) -> str:
    script = root / "claude"
    script.write_text(f"#!/bin/sh\nexec {sys.executable} {FAKE_CLAUDE} \"$@\"\n")
    script.chmod(0o700)
    return str(script)


def launcher_env(root: Path, name: str, strip: bool = True) -> dict[str, str]:
    return {
        "FAKE_LAUNCHER_NAME": name,
        "FAKE_LAUNCHER_ROOT": str(root),
        "FAKE_LAUNCHER_STRIP": "1" if strip else "0",
        "FAKE_PYTHON": sys.executable,
        "FAKE_CLAUDE": str(FAKE_CLAUDE),
    }


class ReviewerCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.log = self.root / "calls.jsonl"
        self.env = self.enterContext(patch.dict(os.environ, {"FAKE_CLAUDE_LOG": str(self.log)}))
        self.db = SQLiteDatabase(":memory:")
        self.addCleanup(self.db.close)
        self.workspaces = SQLiteWorkspaceStore(self.db)
        self.caps = Mock(permissions=InMemoryPermission())
        self.caps.is_interrupted.return_value = False

    def executor(self, **overrides):
        overrides.setdefault("launcher", ClaudeCodeLauncher(name="fake", command=fake_command(self.root)))
        profile = ClaudeCodeProfile(name="coder-reviewer", **overrides)
        return CoderReviewerExecutor(profile=profile, workspaces=self.workspaces,
                                     tasks=SQLiteTaskMetadataStore(self.db))

    def calls(self):
        return read_jsonl(self.log)

    def payload(self, **extra):
        return {
            "executor_id": "coder-reviewer",
            "text": "divide must reject zero",
            "test_evidence": "pytest: 3 passed",
            "explicit_workdir": str(self.repo),
            "base_ref": "main",
            **extra,
        }


class _Capture:
    def __init__(self):
        self._parts = []

    def write(self, text):
        self._parts.append(text)
        return len(text)

    def flush(self):
        pass

    def getvalue(self):
        return "".join(self._parts)
