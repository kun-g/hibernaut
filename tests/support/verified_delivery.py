import atexit
import functools
import json
import shutil
import tempfile
from pathlib import Path

from agent_runtime.adapters.executor_cli import CommandResult, OrcaWorktreeManager
from agent_runtime.adapters.executor_cli.worktree import _default_runner
from agent_runtime.adapters.storage_sqlite import SQLiteSessionStoreAdapter
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from tests.support.pr_delivery import DeliveryCase, git


@functools.cache
def _squashed_history(seed):
    tmp = tempfile.TemporaryDirectory(prefix="reclaim-history-")
    atexit.register(tmp.cleanup)
    repo = shutil.copytree(seed, Path(tmp.name) / "primary")
    git(repo, "checkout", "-qb", "task")
    (repo / "calc.py").write_text("x = 1\n")
    git(repo, "commit", "-qam", "work")
    head = git(repo, "rev-parse", "HEAD")
    git(repo, "checkout", "-q", "main")
    git(repo, "merge", "--squash", "task")
    git(repo, "commit", "-qm", "work (#3)")
    merge = git(repo, "rev-parse", "HEAD")
    git(repo, "repack", "-ad")
    return repo / ".git/objects/pack", head, merge


class ReclaimCase(DeliveryCase):
    def setUp(self):
        super().setUp()
        self.primary = self.clone("primary", None)
        self.wt = self.root / "hb-task-9"
        git(self.primary, "worktree", "add", "-q", "-b", "hb/task-9", str(self.wt))
        self.workspaces = SQLiteWorkspaceStore(self.db)
        self.sessions = SQLiteSessionStoreAdapter(self.db)
        self.workspaces.reserve_creating(work_key="task:9", repo_checkout=str(self.primary), workdir=str(self.wt),
                                         branch="hb/task-9")
        self.workspaces.mark_ready("task:9", workdir=str(self.wt), branch="hb/task-9")
        self.gh = {"open": CommandResult(0, "[]", ""), "merged": CommandResult(0, "[]", "")}

    def runner(self, cmd, cwd):
        if cmd[0] == "gh":
            return self.gh[cmd[cmd.index("--state") + 1]]
        return _default_runner(cmd, cwd)

    def manager(self, usage=None):
        return OrcaWorktreeManager(workspace_store=self.workspaces, session_store=self.sessions, backend="git",
                                   runner=self.runner, usage=usage)

    def commit(self, text="x = 1\n"):
        (self.wt / "calc.py").write_text(text)
        git(self.wt, "add", "-A")
        git(self.wt, "commit", "-qm", "work")
        return git(self.wt, "rev-parse", "HEAD")

    def squashed_task(self):
        # Only immutable objects are reused. Each case owns the refs, index and linked worktree it will reclaim.
        pack, head, merge = _squashed_history(self._tmpl_seed)
        for db in (self.primary / ".git", self.bare):
            shutil.copytree(pack, db / "objects/pack", dirs_exist_ok=True)
        git(self.wt, "merge", "-q", "--ff-only", head)
        git(self.primary, "update-ref", "refs/remotes/origin/main", merge)
        git(self.bare, "update-ref", "refs/heads/main", merge)
        return head, merge

    def merged_pr(self, head, merge):
        self.gh["merged"] = CommandResult(0, json.dumps(
            [{"number": 3, "headRefOid": head, "mergeCommit": {"oid": merge}}]), "")
