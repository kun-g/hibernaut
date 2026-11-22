import atexit
import dataclasses
import functools
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from agent_runtime.adapters.delivery import (
    MERGE_TOOL,
    OPEN_PR_TOOL,
    DeliveryConfig,
    DeliveryRequest,
    PRDelivery,
    bump_version,
    parse_delivery,
)
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteDeliveryStore
from agent_runtime.core.models import PermissionDecision
from tests.fakes import InMemoryPermission

FAKE_GH = Path(__file__).with_name("fake_gh.py")
VERSION_FILE = "pkg/__init__.py"


def git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


class TestVersionRules(unittest.TestCase):
    def test_bump_follows_semver(self):
        self.assertEqual(bump_version("0.4.0", "major"), "1.0.0")
        self.assertEqual(bump_version("0.4.2", "minor"), "0.5.0")
        self.assertEqual(bump_version("0.4.2", "patch"), "0.4.3")
        self.assertEqual(bump_version("0.4.2", "skip"), "0.4.2")
        with self.assertRaisesRegex(ValueError, "not MAJOR.MINOR.PATCH"):
            bump_version("0.4", "patch")

    def test_parse_delivery_keeps_the_two_grants_separate_and_rejects_bad_settings(self):
        cfg = parse_delivery("projects.x.delivery", {"open_pr": True, "checks": [["pytest", "-v"]]}, repo="o/r")
        self.assertEqual((cfg.repo, cfg.open_pr, cfg.auto_merge, cfg.checks), ("o/r", True, False, (("pytest", "-v"),)))
        self.assertEqual(parse_delivery("d", {}, repo="o/r").open_pr, False)
        cases = {
            "auto_merge needs open_pr = true": {"auto_merge": True},
            "unknown key": {"open_pr": True, "token": "x"},
            "version_file must be a path inside the repository": {"version_file": "../VERSION"},
            "needs a named group": {"version_pattern": "[0-9.]+"},
            "title_template may use only": {"title_template": "{title} {sha}"},
            "each a non-empty list of strings": {"checks": ["pytest -v"]},
            "merge_method must be one of": {"merge_method": "fast-forward"},
            "open_pr must be true or false": {"open_pr": "yes"},
        }
        for message, raw in cases.items():
            with self.subTest(message), self.assertRaisesRegex(ValueError, message):
                parse_delivery("d", raw, repo="o/r")


@functools.lru_cache(maxsize=1)
def _delivery_template() -> tuple[Path, Path]:
    tmp = tempfile.TemporaryDirectory(prefix="delivery_tmpl_")
    atexit.register(tmp.cleanup)
    root = Path(tmp.name)
    bare = root / "remote.git"
    seed = root / "seed"
    git(root, "init", "-q", "--bare", "-b", "main", str(bare))
    git(root, "clone", "-q", str(bare), str(seed))
    git(seed, "config", "user.email", "t@example.invalid")
    git(seed, "config", "user.name", "t")
    (seed / "pkg").mkdir()
    (seed / VERSION_FILE).write_text('"""pkg"""\n__version__ = "0.4.0"\n')
    (seed / "calc.py").write_text("def divide(a, b):\n    return a\n")
    git(seed, "add", "-A")
    git(seed, "commit", "-qm", "base")
    git(seed, "push", "-q", "origin", "main")
    return bare, seed


def _repoint_origin(repo: Path, old_bare: Path, new_bare: Path) -> None:
    cfg = repo / ".git" / "config"
    cfg.write_text(cfg.read_text(encoding="utf-8").replace(str(old_bare), str(new_bare)), encoding="utf-8")


class DeliveryCase(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        tmpl_bare, tmpl_seed = _delivery_template()
        self._tmpl_bare = tmpl_bare
        self._tmpl_seed = tmpl_seed
        self._main_moved = False
        self.bare = self.root / "remote.git"
        self.seed = self.root / "seed"
        shutil.copytree(tmpl_bare, self.bare)
        shutil.copytree(tmpl_seed, self.seed)
        _repoint_origin(self.seed, tmpl_bare, self.bare)
        self.state_file = self.root / "gh-state.json"
        self.state_file.write_text(json.dumps({"bare": str(self.bare)}))
        gh = self.root / "gh"
        gh.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{FAKE_GH}" "$@"\n')
        gh.chmod(0o700)
        self.check_log = self.root / "checks.log"
        self.config = DeliveryConfig(
            repo="o/r", open_pr=True, version_file=VERSION_FILE, title_template="Pkg {version}: {title}",
            checks=(("/bin/sh", "-c", f'printf "ran\\n" >> "{self.check_log}"'),),
            gh_command=str(gh), ci_poll_seconds=1, ci_timeout_seconds=5,
        )
        self.db = SQLiteDatabase(":memory:")
        self.addCleanup(self.db.close)
        self.store = SQLiteDeliveryStore(self.db)
        self.env = {**os.environ, "FAKE_GH_STATE": str(self.state_file), "GH_TOKEN": "leak", "GITHUB_TOKEN": "leak"}
        self.sleeps: list[float] = []

    def _bare_unchanged(self) -> bool:
        if self._main_moved:
            return False
        heads = self.bare / "refs" / "heads"
        files = [p for p in heads.rglob("*") if p.is_file()]
        if len(files) != 1 or files[0] != heads / "main":
            return False
        return files[0].read_bytes() == (self._tmpl_bare / "refs" / "heads" / "main").read_bytes()

    def clone(self, name, branch):
        path = self.root / name
        if self._bare_unchanged():
            shutil.copytree(self._tmpl_seed, path)
            _repoint_origin(path, self._tmpl_bare, self.bare)
        else:
            git(self.root, "clone", "-q", str(self.bare), str(path))
            git(path, "config", "user.email", "t@example.invalid")
            git(path, "config", "user.name", "t")
        if branch:
            git(path, "checkout", "-qb", branch)
        return path

    def task(self, name, files, message="change"):
        path = self.clone(name, f"hb/{name}")
        for rel, text in files.items():
            (path / rel).write_text(text)
        git(path, "add", "-A")
        git(path, "commit", "-qm", message)
        return path

    def push_main(self, files, message="main moves"):
        git(self.seed, "pull", "-q", "origin", "main")
        for rel, text in files.items():
            (self.seed / rel).write_text(text)
        git(self.seed, "add", "-A")
        git(self.seed, "commit", "-qm", message)
        git(self.seed, "push", "-q", "origin", "main")
        self._main_moved = True

    def delivery(self, sleep=None, **overrides):
        return PRDelivery(dataclasses.replace(self.config, **overrides), self.store, env=self.env,
                          sleep=sleep or self.sleeps.append)

    def request(self, workdir, kind="feature", review="pass", work_key=None, title="divide rejects zero"):
        return DeliveryRequest(work_key=work_key or f"task:{workdir.name}", workdir=workdir, title=title,
                               summary="divide raises on zero", test_evidence="red: 1 failed\ngreen: 3 passed",
                               change_kind=kind, rationale="adds a new rejection path", review=review)

    def gh_state(self):
        return json.loads(self.state_file.read_text())

    def gh_calls(self, sub):
        return [c for c in self.gh_state().get("calls", []) if c[1] == sub]

    def main_file(self, rel=VERSION_FILE):
        return git(self.bare, "show", f"main:{rel}")

    def remote_head(self, branch):
        out = git(self.bare, "for-each-ref", "--format=%(objectname)", f"refs/heads/{branch}")
        return out


class TestOpenPullRequest(DeliveryCase):
    def test_merges_latest_main_sets_version_runs_checks_pushes_and_creates_the_pr(self):
        wt = self.task("task-a", {"calc.py": "def divide(a, b):\n    return a / b\n"})
        self.push_main({"README.md": "docs\n"})
        rec = self.delivery().deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertEqual(rec.problem, "")
        self.assertEqual(rec.state, "pr_open")
        self.assertEqual((rec.base_version, rec.bump, rec.version), ("0.4.0", "minor", "0.5.0"))
        self.assertEqual(rec.pr_url, "https://github.example/o/r/pull/1")
        self.assertEqual(self.remote_head("hb/task-a"), rec.head_sha)
        self.assertEqual(rec.head_sha, git(wt, "rev-parse", "HEAD"))
        # The latest main is part of the branch, and the version is set on top of it.
        self.assertEqual(git(wt, "merge-base", "--is-ancestor", git(self.bare, "rev-parse", "main"), "HEAD"), "")
        self.assertIn('__version__ = "0.5.0"', git(self.bare, "show", f"hb/task-a:{VERSION_FILE}"))
        self.assertTrue((wt / "README.md").exists())
        self.assertEqual(self.check_log.read_text(), "ran\n")
        self.assertEqual([c["exit_code"] for c in rec.checks], [0])
        pr = self.gh_state()["prs"][0]
        self.assertEqual(pr["title"], "Pkg 0.5.0: divide rejects zero")
        for part in ("Kind: feature", "adds a new rejection path", "0.4.0 -> 0.5.0 (minor for feature)",
                     "passed", "review=pass", "red: 1 failed", "task:task-a"):
            self.assertIn(part, pr["body"])
        self.assertEqual(self.gh_state()["repo_flag"], "o/r")
        # Remote calls never see a token from the daemon environment.
        self.assertEqual(self.gh_state()["tokens_seen"], [])
        self.assertIn("not merged: this project does not authorize merging", "\n".join(rec.render()))
        self.assertEqual(self.store.get("task:task-a")["state"], "pr_open")

    def test_retry_reuses_the_pr_and_repeats_no_completed_step(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        first = self.delivery().deliver(self.request(wt), permissions=InMemoryPermission())
        second = self.delivery().deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertEqual((second.state, second.pr_number, second.version), ("pr_open", first.pr_number, "0.5.0"))
        self.assertEqual(second.head_sha, first.head_sha)
        self.assertEqual(len(self.gh_calls("create")), 1)
        self.assertEqual(len(self.gh_state()["prs"]), 1)
        self.assertEqual(self.gh_state()["prs"][0]["edits"], 1)
        self.assertEqual(self.check_log.read_text(), "ran\n")
        self.assertEqual(second.attempts, 2)

    def test_partial_success_push_done_pr_failed_is_resumed(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        state = self.gh_state()
        state["fail"] = {"create": 1}
        self.state_file.write_text(json.dumps(state))
        failed = self.delivery().deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertEqual(failed.state, "local")
        self.assertIn("gh pr create failed", failed.problem)
        self.assertEqual(failed.pushed_sha, self.remote_head("hb/task-a"))
        retried = self.delivery().deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertEqual((retried.state, retried.problem), ("pr_open", ""))
        self.assertEqual(retried.head_sha, failed.head_sha)
        self.assertEqual(len(self.gh_state()["prs"]), 1)

    def test_failing_checks_push_nothing_and_are_recoverable(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        rec = self.delivery(checks=((sys.executable, "-c", "import sys; print('boom'); sys.exit(3)"),)).deliver(
            self.request(wt), permissions=InMemoryPermission())
        self.assertEqual(rec.state, "local")
        self.assertIn("repository checks failed", rec.problem)
        self.assertIn("boom", rec.problem)
        self.assertEqual(self.remote_head("hb/task-a"), "")
        self.assertEqual(self.gh_calls("create"), [])

    def test_test_and_docs_changes_skip_the_version(self):
        wt = self.task("task-a", {"test_calc.py": "def test_x():\n    pass\n"})
        rec = self.delivery().deliver(self.request(wt, kind="test_docs"), permissions=InMemoryPermission())
        self.assertEqual((rec.bump, rec.base_version, rec.version), ("skip", "0.4.0", "0.4.0"))
        self.assertEqual(self.gh_state()["prs"][0]["title"], "divide rejects zero")
        self.assertEqual(git(wt, "log", "--format=%s", "-1"), "change")
        self.assertIn("unchanged 0.4.0 (skip for test_docs)", "\n".join(rec.render()))

    def test_breaking_change_bumps_major_and_a_branch_own_bump_is_replaced(self):
        wt = self.task("task-a", {VERSION_FILE: '"""pkg"""\n__version__ = "0.4.1"\n', "calc.py": "y = 2\n"})
        rec = self.delivery().deliver(self.request(wt, kind="breaking"), permissions=InMemoryPermission())
        self.assertEqual(rec.version, "1.0.0")
        self.assertIn('"1.0.0"', git(self.bare, "show", f"hb/task-a:{VERSION_FILE}"))

    def test_without_open_pr_grant_the_change_stays_local(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        rec = self.delivery(open_pr=False).deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertEqual((rec.state, rec.problem, rec.bump), ("local", "", "minor"))
        self.assertIn("delivery.open_pr = false", rec.notes[0])
        self.assertEqual(self.remote_head("hb/task-a"), "")
        self.assertEqual(self.gh_state().get("calls", []), [])

    def test_open_pr_permission_defer_waits_for_approval(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        defer = InMemoryPermission({OPEN_PR_TOOL: PermissionDecision("defer", "publishing needs a person")})
        rec = self.delivery().deliver(self.request(wt), permissions=defer)
        self.assertEqual((rec.state, rec.approval), ("local", OPEN_PR_TOOL))
        self.assertIn("publishing needs a person", rec.problem)
        self.assertEqual(self.remote_head("hb/task-a"), "")
        rec = self.delivery().deliver(self.request(wt), permissions=defer, approved=frozenset({OPEN_PR_TOOL}))
        self.assertEqual(rec.state, "pr_open")
        blocked = InMemoryPermission({OPEN_PR_TOOL: PermissionDecision("block", "no publishing")})
        other = self.task("task-b", {"calc.py": "z = 1\n"})
        rec = self.delivery().deliver(self.request(other), permissions=blocked)
        self.assertEqual((rec.state, rec.problem, rec.notes), ("local", "", ["delivery.open_pr blocked: no publishing"]))

    def test_conflict_outside_the_version_file_is_reported_and_aborted(self):
        wt = self.task("task-a", {"calc.py": "def divide(a, b):\n    return a / b\n"})
        self.push_main({"calc.py": "def divide(a, b):\n    return b\n"})
        rec = self.delivery().deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertIn("conflicts in calc.py", rec.problem)
        self.assertEqual(git(wt, "status", "--porcelain"), "")
        self.assertFalse((wt / ".git" / "MERGE_HEAD").exists())
        self.assertEqual(self.remote_head("hb/task-a"), "")


class TestMerge(DeliveryCase):
    def merging(self, **overrides):
        return self.delivery(auto_merge=True, **overrides)

    def set_gh(self, **values):
        state = self.gh_state()
        state.update(values)
        self.state_file.write_text(json.dumps(state))

    def test_merge_waits_for_ci_and_records_the_merge_sha_and_version(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        pending = [{"name": "gates", "state": "IN_PROGRESS", "bucket": "pending"}]
        self.set_gh(checks_sequence=[[], pending])
        rec = self.merging().deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertEqual((rec.state, rec.problem), ("merged", ""))
        self.assertEqual(rec.merge_sha, git(self.bare, "rev-parse", "main"))
        self.assertIn('"0.5.0"', self.main_file())
        self.assertEqual(len(self.sleeps), 2)
        merge_call = self.gh_calls("merge")[0]
        self.assertIn("--squash", merge_call)
        self.assertEqual(merge_call[merge_call.index("--match-head-commit") + 1], rec.head_sha)
        text = "\n".join(rec.render())
        for part in ("Delivery: merged", f"Merge commit: {rec.merge_sha}", "0.4.0 -> 0.5.0", "CI: passed"):
            self.assertIn(part, text)
        again = self.merging().deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertEqual((again.state, again.merge_sha), ("merged", rec.merge_sha))
        self.assertEqual(len(self.gh_calls("merge")), 1)

    def test_merge_is_a_separate_grant(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        rec = self.delivery(auto_merge=False).deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertEqual(rec.state, "pr_open")
        self.assertEqual(self.gh_calls("merge"), [])
        defer = InMemoryPermission({MERGE_TOOL: PermissionDecision("defer", "merge needs a person")})
        rec = self.merging().deliver(self.request(wt), permissions=defer)
        self.assertEqual((rec.state, rec.approval), ("pr_open", MERGE_TOOL))
        self.assertEqual(self.gh_calls("merge"), [])
        rec = self.merging().deliver(self.request(wt), permissions=defer, approved=frozenset({MERGE_TOOL}))
        self.assertEqual(rec.state, "merged")

    def test_merge_needs_a_passing_independent_review(self):
        for review in ("off", "pending"):
            wt = self.task(f"task-{review}", {"calc.py": f"x = '{review}'\n"})
            rec = self.merging().deliver(self.request(wt, review=review), permissions=InMemoryPermission())
            with self.subTest(review):
                self.assertEqual(rec.state, "pr_open")
                self.assertIn(f"needs a passing independent review (review={review})", rec.notes[0])
        self.assertEqual(self.gh_calls("merge"), [])

    def test_failed_ci_does_not_merge(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        self.set_gh(checks=[{"name": "gates", "state": "FAILURE", "bucket": "fail"}])
        rec = self.merging().deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertEqual((rec.state, rec.problem), ("pr_open", ""))
        self.assertIn("not merged: CI failed (gates)", rec.notes)
        self.assertEqual(self.gh_calls("merge"), [])

    def test_ci_timeout_is_recoverable(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        self.set_gh(checks=[{"name": "gates", "state": "IN_PROGRESS", "bucket": "pending"}])
        rec = self.merging(ci_timeout_seconds=0.01, ci_poll_seconds=0.01).deliver(
            self.request(wt), permissions=InMemoryPermission())
        self.assertEqual(rec.state, "pr_open")
        self.assertIn("CI has not finished for PR #1", rec.problem)
        self.set_gh(checks=[{"name": "gates", "state": "SUCCESS", "bucket": "pass"}])
        rec = self.merging().deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertEqual(rec.state, "merged")

    def test_concurrent_prs_from_the_same_base_end_with_distinct_versions(self):
        a = self.task("task-a", {"calc.py": "a = 1\n"})
        b = self.task("task-b", {"other.py": "b = 1\n"})
        ra = self.delivery().deliver(self.request(a, kind="feature"), permissions=InMemoryPermission())
        rb = self.delivery().deliver(self.request(b, kind="fix"), permissions=InMemoryPermission())
        self.assertEqual((ra.version, rb.version), ("0.5.0", "0.4.1"))
        merged_a = self.merging().deliver(self.request(a, kind="feature"), permissions=InMemoryPermission())
        self.assertEqual(merged_a.state, "merged")
        merged_b = self.merging().deliver(self.request(b, kind="fix"), permissions=InMemoryPermission())
        self.assertEqual((merged_b.state, merged_b.problem), ("merged", ""))
        self.assertEqual((merged_b.base_version, merged_b.version, merged_b.pr_number), ("0.5.0", "0.5.1", rb.pr_number))
        self.assertIn("version recomputed from 0.5.0", "\n".join(merged_b.history))
        self.assertIn('"0.5.1"', self.main_file())
        self.assertEqual(self.main_file("other.py"), "b = 1")
        self.assertEqual(self.gh_state()["prs"][1]["title"], "Pkg 0.5.1: divide rejects zero")

    def test_main_version_moving_between_ci_and_merge_recomputes_before_merging(self):
        a = self.task("task-a", {"calc.py": "a = 1\n"})
        b = self.task("task-b", {"other.py": "b = 1\n"})
        self.delivery().deliver(self.request(a), permissions=InMemoryPermission())
        merged_a: list[object] = []

        def merge_a_while_b_waits(_seconds):
            if not merged_a:
                merged_a.append(self.merging().deliver(self.request(a), permissions=InMemoryPermission()))

        pending = [{"name": "gates", "state": "IN_PROGRESS", "bucket": "pending"}]
        self.set_gh(checks_sequence=[pending])
        rb = self.merging(sleep=merge_a_while_b_waits).deliver(self.request(b, kind="fix"),
                                                             permissions=InMemoryPermission())
        self.assertEqual(merged_a[0].state, "merged")  # type: ignore[attr-defined]
        self.assertEqual((rb.state, rb.problem, rb.version), ("merged", "", "0.5.1"))
        self.assertIn('"0.5.1"', self.main_file())
        self.assertEqual(rb.merge_sha, git(self.bare, "rev-parse", "main"))

    def test_a_pr_merged_by_a_person_is_adopted_not_bumped_again(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        rec = self.delivery().deliver(self.request(wt), permissions=InMemoryPermission())
        subprocess.run([str(self.root / "gh"), "pr", "merge", "1", "--squash", "--match-head-commit", rec.head_sha],
                       env=self.env, check=True, capture_output=True)
        again = self.delivery().deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertEqual(again.state, "merged")
        self.assertEqual(again.merge_sha, git(self.bare, "rev-parse", "main"))
        self.assertEqual(git(wt, "rev-parse", "HEAD"), rec.head_sha)
        self.assertIn('"0.5.0"', self.main_file())
        self.assertEqual(len(self.gh_calls("create")), 1)


if __name__ == "__main__":
    unittest.main()
