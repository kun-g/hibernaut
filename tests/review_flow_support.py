import unittest
from unittest.mock import AsyncMock, Mock, patch

from agent_runtime.adapters.delivery import DeliveryRecord
from agent_runtime.adapters.executor_cli import sde
from agent_runtime.adapters.executor_cli.base_merge import SinceReview
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.reviewer import render_review
from agent_runtime.adapters.executor_cli.sde import _State
from tests.fakes import InMemoryPermission
from tests.test_sde_delivery import PASS, dev


CONFLICT = DeliveryRecord(problem="merge conflict", handback={"kind": "conflict", "base": "origin/main", "files": ["calc.py"]})
CHECKS = DeliveryRecord(problem="check failed", handback={"kind": "checks", "failed": "needs ok.txt"})
DONE = DeliveryRecord(state="pr_open")
REPORT = {**dev({})["output"], "code_change": True}
QUESTION = {"status": "needs_input", "question": "which side?"}
MAIN = "--- /dev/null\n+++ b/main_only.py\n@@ -0,0 +1 @@\n+return 1\n"
FIX = "--- /dev/null\n+++ b/ok.txt\n@@ -0,0 +1 @@\n+ok\n"


class ReviewFlowCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.delivery = Mock()
        self.delivery.config.max_handbacks = 3
        self.reviewer = Mock()
        self.reviewer.check_launch.return_value.action = "allow"
        self.reviewer.review = AsyncMock()
        self.reviewer.format_review.side_effect = lambda review: render_review(review, executor_id="coder-reviewer", settings="test")
        self.executor = sde.SDEExecutor(profile=ClaudeCodeProfile(name="sde"), options=sde.SDEOptions(review=True),
                                        reviewer=self.reviewer, worktree_manager=Mock(spec=sde.OrcaWorktreeManager), inbox_checkout_map={},
                                        tasks=Mock(), delivery=self.delivery)
        self.caps = Mock(permissions=InMemoryPermission())
        self.state = _State(stage="develop", workdir="/test/repo", base_ref="main", requirements="divide",
                            project={"workdir": "/test/repo", "commit": "initial"})
        self.develop = self.enterContext(patch.object(self.executor, "_develop", new_callable=AsyncMock))
        self.enterContext(patch.object(self.executor, "_unfinished", return_value=("", None)))
        self.enterContext(patch.object(self.executor, "_delivered_snapshot", return_value="delivery:versioned"))
        self.enterContext(patch.object(self.executor, "_since_review", side_effect=lambda state, *_:
                                      SinceReview(state.reviewed_snapshot.split(":")[1])))
        self.fingerprint = self.enterContext(patch.object(sde, "worktree_fingerprint"))
        self.enterContext(patch("agent_runtime.adapters.executor_cli.reviewer.repo_root", side_effect=lambda path: path))
        self.evidence = {}
        self.enterContext(patch.object(sde, "snapshot_diff", side_effect=self.diff))
        self.enterContext(patch.object(sde, "changed_paths", side_effect=self.paths))

    def paths(self, workdir, before, after):
        return self.evidence[before, after][1]

    def diff(self, workdir, before, after):
        return self.evidence[before.split(":")[1], after.split(":")[1]][0]

    async def run_flow(self, *, snapshots, diffs, deliveries=(CONFLICT, DONE), reviews=(PASS, PASS), reports=None):
        self.evidence = diffs
        self.delivery.deliver.side_effect = deliveries
        self.reviewer.review.side_effect = reviews
        self.develop.side_effect = [(r, "native") for r in (reports if reports is not None else [REPORT] * len(reviews))]
        self.fingerprint.side_effect = [f"head-{tree}:{tree}" for tree in snapshots]
        return await self.executor._run("session", None, self.state, self.caps, "divide")

    def request(self, index=-1):
        return self.reviewer.review.await_args_list[index].args[0]
