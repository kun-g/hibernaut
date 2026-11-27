import json
import os
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from agent_runtime.adapters.executor_cli import OrcaWorktreeManager
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.reviewer import CoderReviewerExecutor
from agent_runtime.adapters.executor_cli.sde import SDEExecutor, SDEOptions
from agent_runtime.adapters.storage_sqlite import SQLiteSessionStoreAdapter
from agent_runtime.adapters.storage_sqlite.tasks import SQLiteTaskMetadataStore
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from agent_runtime.core.models import Event, SessionRef
from tests.support.empty_guidance import empty_project_guidance
from tests.support.fakes import InMemoryPermission
from tests.support.coder_reviewer import fake_command
from tests.support.pr_delivery import DeliveryCase
from tests.support.jsonl import read_jsonl


REPO = Path(__file__).resolve().parents[2]
EVIDENCE = "red: test_zero FAILED\ngreen: 4 passed"
PASS = {"verdict": "pass", "summary": "ok", "blocking": []}


def dev(files, kind="feature", commit="divide rejects zero", **output):
    out = {"status": "done", "summary": "divide rejects zero", "test_evidence": EVIDENCE,
           "change_kind": kind, "change_rationale": f"{kind}: divide now raises on zero"}
    out.update(output)
    return {"write": files, "commit": commit, "output": out}


def event(payload, key="task:1"):
    return Event(inbox_id="proj:x", source="test", external_id=f"e-{os.urandom(4).hex()}", payload=payload,
                 correlation_key=key)


class SDEDeliveryCase(DeliveryCase, unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        super().setUp()
        self.enterContext(empty_project_guidance())
        self.wt = self.clone("task-1", "hb/task-1")
        self.log = self.root / "calls.jsonl"
        self.plan = self.root / "plan.json"
        self.enterContext(patch.dict(os.environ, {"FAKE_CLAUDE_LOG": str(self.log), "FAKE_PLAN": str(self.plan)}))
        self.workspaces = SQLiteWorkspaceStore(self.db)
        self.caps = Mock(permissions=InMemoryPermission())
        self.caps.is_interrupted.return_value = False
        self.caps.memory.load_baseline.return_value = {}

    def set_plan(self, sde=(), reviewer=()):
        self.plan.write_text(json.dumps({"sde": list(sde), "reviewer": list(reviewer)}))

    def executor(self, review=True, delivery=None):
        launcher = ClaudeCodeLauncher(name="fake", command=fake_command(self.root))
        reviewer = CoderReviewerExecutor(
            profile=ClaudeCodeProfile(name="coder-reviewer", launcher=launcher, model="claude-sonnet-5-5"),
            workspaces=self.workspaces, tasks=SQLiteTaskMetadataStore(self.db),
        )
        return SDEExecutor(
            profile=ClaudeCodeProfile(name="sde", launcher=launcher),
            options=SDEOptions(review=review),
            reviewer=reviewer if review else None,
            worktree_manager=OrcaWorktreeManager(workspace_store=self.workspaces,
                                                 session_store=SQLiteSessionStoreAdapter(self.db), backend="git"),
            inbox_checkout_map={},
            tasks=SQLiteTaskMetadataStore(self.db),
            delivery=delivery,
        )

    def payload(self, **extra):
        return {"executor_id": "sde", "text": "divide must reject a zero divisor", "title": "Reject zero divisors",
                "explicit_workdir": str(self.wt), "base_ref": "origin/main", **extra}

    def calls(self, role=None):
        rows = read_jsonl(self.log)
        return [r for r in rows if role is None or r["role"] == role]

    @staticmethod
    def resume(outcome, key="task:1"):
        return SessionRef(session_id=outcome.session_id or "s", inbox_id="proj:x", correlation_key=key,
                          executor_id="sde", executor_session_id=outcome.executor_session_id, status="suspended",
                          checkpoint=outcome.session_checkpoint, pending_request_id=outcome.request_id)


MERGE_MAIN = [["git", "merge", "--no-edit", "origin/main"]]
TASK_CALC = "def divide(a, b):\n    return a / b\n"
MAIN_CALC = "def divide(a, b):\n    return b\n"
RESOLVED = "def divide(a, b):\n    if b == 0:\n        raise ZeroDivisionError\n    return a / b\n"
