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
from unittest.mock import Mock, patch

from agent_runtime.adapters.delivery import (
    MERGE_TOOL,
    OPEN_PR_TOOL,
    DeliveryConfig,
    DeliveryRecord,
    DeliveryRequest,
    PRDelivery,
    bump_version,
    parse_delivery,
    source_issue,
)
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteDeliveryStore
from agent_runtime.core.models import DeliveryOutcome, Event, PermissionDecision
from tests import fake_gh
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
            "max_handbacks must be a non-negative integer": {"max_handbacks": -1},
        }
        for message, raw in cases.items():
            with self.subTest(message), self.assertRaisesRegex(ValueError, message):
                parse_delivery("d", raw, repo="o/r")

    def test_issue_link_defaults_to_refs_and_rejects_other_values(self):
        self.assertEqual(parse_delivery("d", {}, repo="o/r").issue_link, "refs")
        for value in ("closes", "refs", "none"):
            self.assertEqual(parse_delivery("d", {"issue_link": value}, repo="o/r").issue_link, value)
        for bad in ("close", "fixes", "", True):
            with self.subTest(bad), self.assertRaisesRegex(
                ValueError, r"d: issue_link must be one of closes, refs, none"
            ):
                parse_delivery("d", {"issue_link": bad}, repo="o/r")

    def test_hand_backs_to_the_developer_default_to_two_and_can_be_turned_off(self):
        self.assertEqual(parse_delivery("d", {}, repo="o/r").max_handbacks, 2)
        self.assertEqual(parse_delivery("d", {"max_handbacks": 0}, repo="o/r").max_handbacks, 0)
        for bad in (True, 1.5, "2"):
            with self.subTest(bad), self.assertRaisesRegex(ValueError, "max_handbacks must be a non-negative integer"):
                parse_delivery("d", {"max_handbacks": bad}, repo="o/r")


def _evt(source, payload=None, key=None):
    return Event(inbox_id="proj:x", source=source, external_id=source, payload=payload or {}, correlation_key=key)


class TestSourceIssue(unittest.TestCase):
    def test_issue_events_and_issue_correlation_keys_name_the_issue(self):
        issue = {"repository": {"full_name": "o/r"}, "issue": {"number": 7, "url": "https://x/o/r/issues/7"}}
        cases = {
            "poller": ([_evt("github:issue_poll", issue, "github:o/r#7")], None),
            "webhook issues": ([_evt("github:issues", issue)], None),
            "webhook issue_comment": ([_evt("github:issue_comment", issue)], None),
            "string number": ([_evt("github:issue_poll", {**issue, "issue": {"number": "7"}})], None),
            "issue repo from the key": ([_evt("github:issue_poll", {"issue": {"number": 7}}, "github:o/r#7")], None),
            "relayed key": ([_evt("switchboard:task", {"text": "x"}, "github:o/r#7")], None),
            "session key": ([_evt("switchboard:task", {"text": "x"})], "github:o/r#7"),
        }
        for name, (events, key) in cases.items():
            with self.subTest(name):
                self.assertEqual(source_issue(events, key), "o/r#7")

    def test_other_sources_and_missing_numbers_name_no_issue(self):
        cases = {
            "/events": ([_evt("http", {"text": "x"}, "task:1")], None),
            "drive": ([_evt("gdrive:task", {"text": "x"}, "gdrive:abc")], "gdrive:abc"),
            "feishu": ([_evt("feishu:message", {"text": "x"}, "feishu:c:t")], None),
            "mcp": ([_evt("mcp", {"text": "x"})], None),
            "pull request": ([_evt("github:pull_request", {"pull_request": {"number": 5}}, "github:o/r#5")], None),
            "no number": ([_evt("github:issue_poll", {"repository": {"full_name": "o/r"}, "issue": {}})], None),
            "bad number": ([_evt("github:issues", {"repository": {"full_name": "o/r"}, "issue": {"number": "x"}})],
                           None),
            "bool number": ([_evt("github:issues", {"repository": {"full_name": "o/r"}, "issue": {"number": True}})],
                            None),
            "no repo": ([_evt("github:issues", {"issue": {"number": 7}})], None),
            "bad key": ([_evt("http", {"text": "x"}, "github:o/r#x")], None),
        }
        for name, (events, key) in cases.items():
            with self.subTest(name):
                self.assertEqual(source_issue(events, key), "")

    def test_events_about_a_pull_request_name_no_issue_even_with_the_task_key(self):
        # Called the way SDEExecutor._new_state calls it: the batch's github key as the second argument.
        key = "github:o/r#5"
        repo = {"full_name": "o/r"}
        pr_issue = {"number": 5, "pull_request": {"url": "https://api.github.example/repos/o/r/pulls/5"}}
        cases = {
            "webhook issues on a PR": _evt("github:issues", {"repository": repo, "issue": pr_issue}, key),
            "webhook issue_comment on a PR": _evt("github:issue_comment", {"repository": repo, "issue": pr_issue}, key),
            "switchboard review": _evt("sb:github", {"event_type": "conversation.message", "body": "[review approved]",
                                                     "metadata": {"github_event": "pull_request_review",
                                                                  "number": 5}}, key),
            "switchboard review comment": _evt("sb:github", {"metadata": {
                "github_event": "pull_request_review_comment", "number": 5}}, key),
            "switchboard PR closed": _evt("sb:github", {"event_type": "work.closed", "metadata": {
                "work_kind": "pull_request", "github_event": "pull_request", "number": 5}}, key),
            "switchboard comment on a PR": _evt("sb:github", {"metadata": {
                "github_event": "issue_comment", "work_kind": "pull_request", "number": 5}}, key),
        }
        for name, evt in cases.items():
            with self.subTest(name):
                self.assertEqual(source_issue([evt], key), "")

    def test_switchboard_issue_events_still_name_the_issue(self):
        key = "github:o/r#7"
        for metadata in ({"work_kind": "issue", "github_event": "issues"},
                         {"work_kind": "issue", "github_event": "issue_comment"}):
            with self.subTest(**metadata):
                self.assertEqual(source_issue([_evt("sb:github", {"metadata": metadata}, key)], key), "o/r#7")


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


class FixtureDelivery(PRDelivery):
    def _run(self, argv, cwd, *, timeout=300.0):
        if argv[0] == self.config.gh_command:
            return fake_gh.run(argv, cwd=cwd, env=self._env)
        return super()._run(argv, cwd, timeout=timeout)


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

    def delivery(self, sleep=None, *, launch_gh=False, **overrides):
        cls = PRDelivery if launch_gh else FixtureDelivery
        return cls(dataclasses.replace(self.config, **overrides), self.store, env=self.env, sleep=sleep or self.sleeps.append)

    def request(self, workdir, kind="feature", review="pass", work_key=None, title="divide rejects zero", issue=""):
        return DeliveryRequest(work_key=work_key or f"task:{workdir.name}", workdir=workdir, title=title,
                               summary="divide raises on zero", test_evidence="red: 1 failed\ngreen: 3 passed",
                               change_kind=kind, rationale="adds a new rejection path", review=review, issue=issue)

    def gh_state(self):
        return json.loads(self.state_file.read_text())

    def gh_calls(self, sub):
        return [c for c in self.gh_state().get("calls", []) if c[1] == sub]

    def main_file(self, rel=VERSION_FILE):
        return git(self.bare, "show", f"main:{rel}")

    def remote_head(self, branch):
        out = git(self.bare, "for-each-ref", "--format=%(objectname)", f"refs/heads/{branch}")
        return out


def rendered_body(*, issue="", repo="", link="refs", followups=(), summary="s", evidence="e"):
    config = DeliveryConfig(repo=repo, issue_link=link, version_file=VERSION_FILE)
    request = DeliveryRequest(work_key="task:1", workdir=Path("repo"), title="t", summary=summary,
                              test_evidence=evidence, change_kind="fix", rationale="r", review="pass",
                              followups=followups, issue=issue)
    record = DeliveryRecord(state="pr_open", head_sha="a" * 40, base_sha="b" * 40, version="0.4.1",
                            base_version="0.4.0", bump="patch", checks=[{"command": "pytest", "exit_code": 0}])
    return PRDelivery(config, Mock(spec_set=SQLiteDeliveryStore), env={})._body(request, record)


class TestPullRequestBody(unittest.TestCase):
    def test_pr_body_opens_with_the_source_issue_line_for_each_issue_link(self):
        cases = {
            ("closes", "o/r#7"): "Closes #7\n\n## Summary",
            ("refs", "o/r#7"): "Refs #7\n\n## Summary",
            ("closes", "other/lib#12"): "Closes other/lib#12\n\n## Summary",
            ("refs", "Other/Lib#12"): "Refs Other/Lib#12\n\n## Summary",
            ("closes", "O/R#3"): "Closes #3\n\n## Summary",
            ("none", "o/r#7"): "## Summary",
            ("closes", ""): "## Summary",
        }
        for (link, issue), start in cases.items():
            with self.subTest(link=link, issue=issue):
                body = rendered_body(issue=issue, repo="o/r", link=link)
                self.assertTrue(body.startswith(start), body[:80])

    def test_without_a_configured_repo_the_issue_is_written_in_full(self):
        self.assertTrue(rendered_body(issue="o/r#7", link="closes").startswith("Closes o/r#7\n\n## Summary"))


class TestOpenPullRequest(DeliveryCase):
    def test_merges_latest_main_sets_version_runs_checks_pushes_and_creates_the_pr(self):
        wt = self.task("task-a", {"calc.py": "def divide(a, b):\n    return a / b\n"})
        self.push_main({"README.md": "docs\n"})
        rec = self.delivery(launch_gh=True).deliver(self.request(wt), permissions=InMemoryPermission())
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

    def test_a_reused_pr_keeps_the_issue_line_when_its_body_is_updated(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        self.delivery().deliver(self.request(wt, issue="o/r#7"), permissions=InMemoryPermission())
        self.delivery(issue_link="closes").deliver(self.request(wt, issue="o/r#7"), permissions=InMemoryPermission())
        pr = self.gh_state()["prs"][0]
        self.assertEqual(pr["edits"], 1)
        self.assertTrue(pr["body"].startswith("Closes #7\n\n## Summary"), pr["body"][:80])

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
        # The developer can resolve it: the record says what conflicted and which base commits caused it.
        main_sha = git(self.bare, "rev-parse", "main")
        self.assertEqual(rec.handback, {
            "kind": "conflict", "base": "origin/main", "base_sha": main_sha, "branch": "hb/task-a",
            "files": ["calc.py"], "commits": f"{main_sha[:7]} main moves",
        })

    def test_a_conflict_only_in_the_version_file_is_resolved_without_a_hand_back(self):
        wt = self.task("task-a", {VERSION_FILE: '"""pkg"""\n__version__ = "0.4.1"\n', "calc.py": "y = 2\n"})
        self.push_main({VERSION_FILE: '"""pkg"""\n__version__ = "0.5.0"\n'})
        rec = self.delivery().deliver(self.request(wt, kind="fix"), permissions=InMemoryPermission())
        self.assertEqual((rec.state, rec.problem, rec.handback, rec.version), ("pr_open", "", {}, "0.5.1"))

    def test_failing_checks_after_merging_the_base_are_handed_back(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        self.push_main({"main.txt": "new on main\n"})
        check = ("/bin/sh", "-c", "test ! -f main.txt || { echo 'main.txt needs ok.txt'; exit 1; }")
        rec = self.delivery(checks=(check,)).deliver(self.request(wt), permissions=InMemoryPermission())
        self.assertIn("repository checks failed", rec.problem)
        self.assertEqual(rec.handback["kind"], "checks")
        self.assertEqual(rec.handback["base_sha"], git(self.bare, "rev-parse", "main"))
        self.assertEqual(rec.handback["head"], git(wt, "rev-parse", "HEAD"))
        self.assertIn("main.txt needs ok.txt", rec.handback["failed"])
        self.assertEqual(self.remote_head("hb/task-a"), "")

    def test_a_missing_check_command_is_not_handed_back(self):
        wt = self.task("task-a", {"calc.py": "x = 1\n"})
        rec = self.delivery(checks=(("no-such-check-command",),)).deliver(self.request(wt),
                                                                       permissions=InMemoryPermission())
        self.assertIn("command not found", rec.problem)
        self.assertEqual(rec.handback, {})


class TestMergePolicy(unittest.TestCase):
    url = "https://github.example/o/r/pull/1"

    def setUp(self):
        self.config = DeliveryConfig(repo="o/r", open_pr=True, auto_merge=True)
        self.delivery = PRDelivery(self.config, Mock(), env={})
        self.request = DeliveryRequest(work_key="task:1", workdir=Path("repo"), title="t", summary="s", test_evidence="e",
                                       change_kind="fix", rationale="r", review="pass")
        self.record = DeliveryRecord(state="pr_open", head_sha="a" * 40, pr_number=1, pr_url=self.url)
        self.gh = self.enterContext(patch.object(self.delivery, "_gh", side_effect=AssertionError("unexpected gh call")))

    def merge(self, review="pass", permissions=None):
        self.delivery._merge(dataclasses.replace(self.request, review=review), self.record,
                             permissions or InMemoryPermission(), frozenset(), lambda: False)
        return self.delivery.outcome(self.record)

    def test_merge_needs_a_passing_independent_review(self):
        for review in ("off", "pending"):
            with self.subTest(review):
                self.record.notes.clear()
                self.merge(review)
                self.assertEqual(self.record.state, "pr_open")
                self.assertIn(f"needs a passing independent review (review={review})", self.record.notes[0])
        self.gh.assert_not_called()

    def failed_ci(self):
        self.gh.side_effect = None
        self.gh.return_value = subprocess.CompletedProcess([], 1, '[{"name":"gates","state":"FAILURE","bucket":"fail"}]', "")
        outcome = self.merge()
        self.assertEqual([call.args[1:3] for call in self.gh.call_args_list], [("pr", "checks")])
        return outcome

    def test_failed_ci_does_not_merge(self):
        self.failed_ci()
        self.assertEqual((self.record.state, self.record.problem), ("pr_open", ""))
        self.assertIn("not merged: CI failed (gates)", self.record.notes)

    def test_outcome_names_why_an_authorized_merge_did_not_happen(self):
        self.assertEqual(self.merge("pending"), DeliveryOutcome(
            state="pr_open", pr_url=self.url, auto_merge=True, hold="review",
            hold_detail="merging needs a passing independent review (review=pending)"))
        outcome = self.failed_ci()
        self.assertEqual((outcome.state, outcome.auto_merge, outcome.hold, outcome.hold_detail, outcome.pr_url),
                         ("pr_open", True, "ci", "CI failed (gates)", self.url))
        self.record = dataclasses.replace(self.record, merge_hold="", merge_hold_detail="", notes=[])
        block = InMemoryPermission({MERGE_TOOL: PermissionDecision("block", "no merges today")})
        outcome = self.merge(permissions=block)
        self.assertEqual((outcome.state, outcome.hold, outcome.hold_detail),
                         ("pr_open", "other", "delivery.merge blocked: no merges today"))

    def test_outcome_has_no_hold_when_merging_is_not_authorized_or_done(self):
        manual = PRDelivery(dataclasses.replace(self.config, auto_merge=False), Mock(), env={})
        self.assertEqual(manual.outcome(self.record), DeliveryOutcome(state="pr_open", pr_url=self.url, auto_merge=False))
        merged = dataclasses.replace(self.record, state="merged")
        self.assertEqual(self.delivery.outcome(merged), DeliveryOutcome(state="merged", pr_url=self.url, auto_merge=True))


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

    def competing_release(self):
        branch = "hb/task-a"
        a = self.task("task-a", {VERSION_FILE: '"""pkg"""\n__version__ = "0.5.0"\n', "calc.py": "a = 1\n"})
        git(a, "push", "-q", "origin", branch)
        head = git(a, "rev-parse", "HEAD")
        gh = self.delivery()._gh
        gh(a, "pr", "create", "--base", "main", "--head", branch, "--title", "Release 0.5.0", "--body", "")

        def merge_on_first_poll(seconds):
            self.sleeps.append(seconds)
            if len(self.sleeps) == 1:
                gh(a, "pr", "merge", "1", "--squash", "--match-head-commit", head)

        return merge_on_first_poll

    def test_main_version_moving_between_ci_and_merge_recomputes_before_merging(self):
        b = self.task("task-b", {"other.py": "b = 1\n"})
        merge_a_while_b_waits = self.competing_release()

        pending = [{"name": "gates", "state": "IN_PROGRESS", "bucket": "pending"}]
        self.set_gh(checks_sequence=[pending])
        rb = self.merging(sleep=merge_a_while_b_waits).deliver(self.request(b, kind="fix"),
                                                             permissions=InMemoryPermission())
        self.assertEqual(self.gh_state()["prs"][0]["state"], "MERGED")
        self.assertEqual((rb.state, rb.problem, rb.version), ("merged", "", "0.5.1"))
        self.assertIn('"0.5.1"', self.main_file())
        self.assertEqual(rb.merge_sha, git(self.bare, "rev-parse", "main"))

    def test_a_pr_that_conflicts_while_waiting_for_ci_is_prepared_again_without_waiting_for_the_timeout(self):
        b = self.task("task-b", {"other.py": "b = 1\n"})
        merge_a_while_b_waits = self.competing_release()

        pending = [{"name": "gates", "state": "IN_PROGRESS", "bucket": "pending"}]
        # Both PRs set the version: once a merges, b conflicts and GitHub reports no checks for it.
        self.set_gh(checks_sequence=[pending], dirty_blocks_ci=True)
        rb = self.merging(sleep=merge_a_while_b_waits, ci_timeout_seconds=600).deliver(
            self.request(b, kind="fix"), permissions=InMemoryPermission())
        self.assertEqual(self.gh_state()["prs"][0]["state"], "MERGED")
        self.assertEqual((rb.state, rb.problem, rb.version), ("merged", "", "0.5.1"))
        self.assertEqual(len(self.sleeps), 1)
        self.assertIn("PR #2 conflicts with origin/main while waiting for CI; merging origin/main again",
                      rb.history)
        self.assertIn('"0.5.1"', self.main_file())
        self.assertEqual(self.main_file("other.py"), "b = 1")

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
