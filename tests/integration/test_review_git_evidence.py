import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from agent_runtime.adapters.executor_cli import sde
from agent_runtime.adapters.executor_cli.base_merge import without_base
from agent_runtime.adapters.executor_cli.reviewer import ReviewAborted, worktree_fingerprint
from agent_runtime.adapters.executor_cli.sde import _State, _review_delta
from tests.support.coder_reviewer import git as run_git


def out(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


class TestConflictResolution(unittest.TestCase):
    BASE = "a = 1\nb = 1\nc = 1\nd = 1\ne = 1\nf = 1\ng = 1\n"
    OURS = BASE.replace("b = 1", "b = old(x)").replace("f = 1", "f = old(y)")
    THEIRS = BASE.replace("b = 1", "b = new(1)").replace("f = 1", "f = new(1)")
    MERGED = BASE.replace("b = 1", "b = new(x)").replace("f = 1", "f = new(y)")

    def setUp(self):
        self.repo = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        run_git(self.repo, "init", "-q", "-b", "main")
        run_git(self.repo, "config", "user.email", "t@example.invalid")
        run_git(self.repo, "config", "user.name", "t")
        self.commit({"f.py": self.BASE, "keep.py": "k = 1\n"}, "base")
        run_git(self.repo, "tag", "fixture-base")
        run_git(self.repo, "checkout", "-qb", "task")

    def reset_branches(self):
        # Subcases end with all files committed. Restore both tips before building the next history.
        run_git(self.repo, "reset", "--hard", "fixture-base")
        run_git(self.repo, "branch", "-f", "main", "fixture-base")

    def commit(self, files, message="work"):
        for rel, text in files.items():
            (self.repo / rel).write_text(text)
        run_git(self.repo, "add", "-A")
        run_git(self.repo, "commit", "-q", "--allow-empty", "-m", message)

    def on_main(self, files):
        run_git(self.repo, "checkout", "-q", "main")
        self.commit(files, "main moves")
        run_git(self.repo, "checkout", "-q", "task")

    def resolve(self, files, *, ours=None, theirs=None, remove=()):
        # Reviewed on the task branch, then main moves, then the developer merges it and leaves `files`.
        self.commit(ours or {"f.py": self.OURS}, "task")
        self.reviewed = worktree_fingerprint(self.repo)
        self.on_main({**(theirs or {"f.py": self.THEIRS}), "main_only.py": "m = 1\n"})
        subprocess.run(["git", "merge", "--no-edit", "main"], cwd=self.repo, capture_output=True)
        for rel in remove:
            (self.repo / rel).unlink()
        self.commit(files, "merge main")
        current = worktree_fingerprint(self.repo)
        since = without_base(self.repo, self.reviewed, current, "main")
        return since, current

    def test_changes_outside_conflict_blocks_are_shown(self):
        cases = {
            "outside the conflict blocks": {"f.py": self.MERGED.replace("d = 1", "d = 2")},
            "before the first block": {"f.py": "z = 1\n" + self.MERGED},
            "after the last block": {"f.py": self.MERGED + "z = 1\n"},
            "another file": {"f.py": self.MERGED, "keep.py": "k = 2\n"},
            "a new file": {"f.py": self.MERGED, "extra.py": "n = 1\n"},
            "a file main brought in": {"f.py": self.MERGED, "main_only.py": "m = 2\n"},
            "new names inside a block": {"f.py": self.MERGED.replace("new(x)", "new(guard(x))")},
            "a new operator inside a block": {"f.py": self.MERGED.replace("new(x)", "-new(x)")},
            "conflict markers left in": {},
        }
        # All nine resolutions start from the same merge, including the case that leaves its markers in place.
        self.resolve({})
        reviewed_tree = self.reviewed.split(":")[1]
        for name, files in cases.items():
            with self.subTest(name):
                run_git(self.repo, "reset", "--hard", "HEAD")
                for path, content in files.items():
                    (self.repo / path).write_text(content)
                run_git(self.repo, "add", "-A")
                tree = out(self.repo, "write-tree")
                delta, paths = _review_delta(self.repo, reviewed_tree, f":{tree}", ("f.py",))
                self.assertEqual(set(paths), {"main_only.py", "f.py", *files})
                self.assertIn("+++ b/f.py", delta)

    def test_conflicts_without_blocks_are_not_judged(self):
        # main deleted a file the task changed: restoring the task's copy changes nothing, removing it is a decision.
        for remove, expected in (((), True), (("keep.py",), False)):
            with self.subTest(remove=remove):
                self.reset_branches()
                self.commit({"keep.py": "k = 3\n"}, "task")
                reviewed = worktree_fingerprint(self.repo)
                run_git(self.repo, "checkout", "-q", "main")
                run_git(self.repo, "rm", "-q", "keep.py")
                run_git(self.repo, "commit", "-qm", "drop keep")
                run_git(self.repo, "checkout", "-q", "task")
                subprocess.run(["git", "merge", "--no-edit", "main"], cwd=self.repo, capture_output=True)
                for rel in remove:
                    (self.repo / rel).unlink()
                self.commit({}, "merge main")
                current = worktree_fingerprint(self.repo)
                delta, paths = _review_delta(self.repo, reviewed.split(":")[1], current, ("keep.py",))
                self.assertEqual(paths, ["keep.py"])
                self.assertEqual(delta == "", expected)

    def test_nothing_merged_is_not_a_resolution(self):
        self.commit({"f.py": self.OURS})
        reviewed = worktree_fingerprint(self.repo)
        self.commit({"f.py": self.MERGED})
        current = worktree_fingerprint(self.repo)
        since = without_base(self.repo, reviewed, current, "main")
        self.assertEqual(since.tree, reviewed.split(":")[1])

    def test_the_base_branchs_own_changes_are_left_out(self):
        since, current = self.resolve({"f.py": self.MERGED, "keep.py": "k = 2\n"})
        names = out(self.repo, "diff", "--name-only", since.tree, current.split(":")[1]).split()
        self.assertEqual(names, ["f.py", "keep.py"])

    def test_uncommitted_work_at_the_review_counts_as_reviewed(self):
        (self.repo / "keep.py").write_text("k = 9\n")
        reviewed = worktree_fingerprint(self.repo)
        self.on_main_after_commit()
        current = worktree_fingerprint(self.repo)
        since = without_base(self.repo, reviewed, current, "main")
        self.assertEqual(out(self.repo, "diff", "--name-only", since.tree, current.split(":")[1]), "")

    def test_snapshot_delta_includes_unreviewed_text_and_mode_changes(self):
        reviewed = worktree_fingerprint(self.repo)
        path = self.repo / "keep.py"
        path.chmod(0o755)
        (self.repo / "extra.py").write_text("s = 1\n")
        current = worktree_fingerprint(self.repo)
        delta, paths = _review_delta(self.repo, reviewed.split(":")[1], current, ("f.py",))
        self.assertEqual(paths, ["extra.py", "f.py", "keep.py"])
        self.assertIn("+++ b/extra.py\n", delta)
        self.assertIn("+s = 1\n", delta)
        self.assertIn("old mode 100644\nnew mode 100755", delta)

    def on_main_after_commit(self):
        self.commit({}, "commit reviewed work")
        self.on_main({"main_only.py": "m = 1\n"})
        run_git(self.repo, "merge", "-q", "--no-edit", "main")

    def test_an_unknown_base_strips_nothing_and_a_lost_commit_is_reported(self):
        reviewed = worktree_fingerprint(self.repo)
        self.commit({"f.py": self.OURS})
        current = worktree_fingerprint(self.repo)
        self.assertEqual(without_base(self.repo, reviewed, current, "no-such-ref").tree, reviewed.split(":")[1])
        with self.assertRaises(ReviewAborted):
            self.on_main({"main_only.py": "m = 1\n"})
            run_git(self.repo, "merge", "-q", "--no-edit", "main")
            without_base(self.repo, f"{'0' * 40}:{'0' * 40}", worktree_fingerprint(self.repo), "main")


class TestReviewBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repo = Path(cls.enterClassContext(tempfile.TemporaryDirectory()))
        run_git(cls.repo, "init", "-qb", "main")
        run_git(cls.repo, "config", "user.name", "test")
        run_git(cls.repo, "config", "user.email", "test@example.invalid")
        run_git(cls.repo, "commit", "--allow-empty", "-qm", "base")
        cls.base = out(cls.repo, "rev-parse", "HEAD")
        run_git(cls.repo, "remote", "add", "origin", ".")
        run_git(cls.repo, "update-ref", "refs/remotes/origin/main", cls.base)
        run_git(cls.repo, "branch", "-u", "origin/main", "main")
        run_git(cls.repo, "checkout", "-qb", "task")
        cls.commit_file("calc.py", "return a\n")
        cls.reviewed = worktree_fingerprint(cls.repo)
        run_git(cls.repo, "checkout", "-qb", "base-change", "main")
        cls.commit_file("main_only.py", "m = 1\n")
        cls.tip = out(cls.repo, "rev-parse", "HEAD")
        run_git(cls.repo, "checkout", "-q", "task")
        run_git(cls.repo, "merge", "--no-edit", "base-change")
        cls.commit_file("calc.py", "raise ValueError\n")
        cls.current = worktree_fingerprint(cls.repo)

    @classmethod
    def commit_file(cls, path, text):
        (cls.repo / path).write_text(text)
        run_git(cls.repo, "add", path)
        run_git(cls.repo, "commit", "-qm", path)

    def setUp(self):
        # Cases vary the refs pointing into one immutable commit graph.
        run_git(self.repo, "update-ref", "refs/heads/main", self.base)
        run_git(self.repo, "update-ref", "refs/remotes/origin/main", self.tip)
        run_git(self.repo, "update-ref", "--no-deref", "-d", "refs/remotes/origin/HEAD")
        run_git(self.repo, "branch", "-u", "origin/main", "main")

    def assert_base(self, base_ref, *, delivery=False, open_pr=False, include_main=False):
        controller = sde.SDEExecutor(profile=Mock(), options=sde.SDEOptions(review=False), reviewer=None,
                                     worktree_manager=Mock(), inbox_checkout_map={}, tasks=Mock(),
                                     delivery=Mock(base_ref="refs/remotes/origin/main", config=Mock(open_pr=open_pr)) if delivery else None)
        state = _State(stage="develop", workdir=str(self.repo), base_ref=base_ref, requirements="fix",
                       reviewed_snapshot=self.reviewed)
        since = controller._since_review(state, self.repo, self.current)
        delta, paths = _review_delta(self.repo, since.tree, self.current, ())
        self.assertEqual(paths, ["calc.py", "main_only.py"] if include_main else ["calc.py"])
        self.assertIn("+raise ValueError", delta)
        evidence = sde._evidence_base(self.repo, base_ref, since.base)
        full_base, _ = sde.merge_base(self.repo, evidence)
        self.assertEqual(out(self.repo, "diff", "--name-only", full_base, "HEAD").splitlines(), paths)

    def test_main_merged_during_a_fix_round_is_left_out_too(self):
        self.assert_base("origin/main", delivery=True, open_pr=True)

    def test_a_delivery_without_prs_still_knows_the_remote_base(self):
        self.assert_base("main", delivery=True)

    def test_without_delivery_a_local_base_stands_for_its_upstream(self):
        self.assert_base("main")

    def test_without_delivery_or_upstream_the_remote_branch_of_the_same_name_counts(self):
        run_git(self.repo, "branch", "--unset-upstream", "main")
        self.assert_base("main")

    def test_the_default_base_is_resolved_the_same_way(self):
        self.assert_base("")

    def trunk_default(self):
        run_git(self.repo, "update-ref", "refs/remotes/origin/trunk", self.base)
        run_git(self.repo, "symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/trunk")

    def test_a_branch_that_is_not_the_tasks_base_is_shown_when_merged(self):
        self.trunk_default()
        self.assert_base("", include_main=True)

    def test_the_delivery_base_counts_whatever_the_tasks_base_is(self):
        self.trunk_default()
        self.assert_base("", delivery=True)

    def local_ahead(self):
        run_git(self.repo, "branch", "-f", "main", self.tip)
        run_git(self.repo, "update-ref", "refs/remotes/origin/main", self.base)

    def test_a_local_base_ahead_of_its_upstream_is_left_out(self):
        self.local_ahead()
        self.assert_base("main")

    def test_a_local_base_ahead_of_the_delivery_base_is_left_out(self):
        self.local_ahead()
        self.assert_base("main", delivery=True)
