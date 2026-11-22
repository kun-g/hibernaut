import atexit
import functools
import json
import os
import shutil
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from agent_runtime.adapters.executor_cli import OrcaWorktreeManager
from agent_runtime.adapters.executor_cli.claude_code import ClaudeCodeLauncher, ClaudeCodeProfile
from agent_runtime.adapters.executor_cli.project_guidance import (
    GuidancePin,
    Selection,
    load_guidance,
    matches,
    pin_head,
)
from agent_runtime.adapters.executor_cli.reviewer import CoderReviewerExecutor
from agent_runtime.adapters.executor_cli.sde import SDEExecutor, SDEOptions
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteSessionStoreAdapter
from agent_runtime.adapters.storage_sqlite.tasks import SQLiteTaskMetadataStore
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from agent_runtime.core.models import Emit, Event, SessionRef, Suspend
from tests.fakes import InMemoryPermission
from tests.test_coder_reviewer import fake_command, git

GENERAL = "# rules\nGENERAL-RULE: run `make check` before committing.\nDetailed rules: guidance.toml.\n"
VERSIONING = "# Versioning\nVERSIONING-BODY: bump VERSION and src/pkg/__init__.py in the same commit.\n"
TESTING = "# Tests\nTESTING-BODY: name each test test_<behaviour>.\n"
ISSUES = "# Issues\nISSUES-BODY: one problem per issue.\n"
CATALOG = """
[[docs]]
path = "docs/versioning.md"
summary = "Changing the version"
activities = ["versioning", "release"]
tags = ["version"]
paths = ["VERSION", "src/*/__init__.py"]

[[docs]]
path = "docs/testing.md"
summary = "Writing tests"
activities = ["testing"]
tags = ["tests"]
paths = ["tests/**"]

[[docs]]
path = "docs/issues.md"
summary = "Writing an issue"
activities = ["issue"]
tags = ["issue"]
"""
BODIES = ("VERSIONING-BODY", "TESTING-BODY", "ISSUES-BODY")
EVIDENCE = "red: test_zero FAILED\ngreen: 1 passed"
PASS = {"verdict": "pass", "summary": "ok", "blocking": []}
VERSION_BUMP = {"VERSION": "1.1.0\n", "src/pkg/__init__.py": "__version__ = '1.1.0'\n"}
NEW_TEST = {"tests/test_calc.py": "def test_divide_rejects_zero():\n    pass\n"}
CITES_VERSIONING = {
    "verdict": "changes_requested",
    "summary": "version bumped in one place only",
    "blocking": [{
        "location": "VERSION:1",
        "trigger": "VERSION changes without src/pkg/__init__.py",
        "impact": "the package reports the old version",
        "guidance_path": "docs/versioning.md",
        "guidance_clause": "bump VERSION and src/pkg/__init__.py in the same commit",
    }],
}


@functools.lru_cache(maxsize=1)
def _template_project() -> Path:
    tmp = tempfile.TemporaryDirectory(prefix="guidance_proj_tmpl_")
    atexit.register(tmp.cleanup)
    repo = Path(tmp.name) / "proj"
    repo.mkdir(parents=True)
    git(repo, "init", "-q", "-b", "main")
    git(repo, "config", "user.email", "t@example.invalid")
    git(repo, "config", "user.name", "t")
    files = {
        "AGENTS.md": GENERAL, "guidance.toml": CATALOG, "docs/versioning.md": VERSIONING,
        "docs/testing.md": TESTING, "docs/issues.md": ISSUES, "VERSION": "1.0.0\n",
        "src/pkg/__init__.py": "__version__ = '1.0.0'\n", "calc.py": "def divide(a, b):\n    return a\n",
        "tests/__init__.py": "",
    }
    for rel, text in files.items():
        (repo / rel).parent.mkdir(parents=True, exist_ok=True)
        (repo / rel).write_text(text)
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "base")
    return repo


def make_project(root: Path, name: str = "proj") -> Path:
    repo = root / name
    shutil.copytree(_template_project(), repo)
    return repo


def head(repo: Path) -> str:
    return subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True,
                          check=True).stdout.strip()


def dev(write=None, **extra):
    step = {"write": write or {}, "output": {"status": "done", "summary": "done", "test_evidence": EVIDENCE, "change_kind": "fix", "change_rationale": "divide handled zero wrongly"}}
    step.update(extra)
    return step


def event(payload, correlation_key="task:1", inbox_id="proj:x"):
    return Event(inbox_id=inbox_id, source="test", external_id=f"e-{time.monotonic_ns()}",
                 payload=payload, correlation_key=correlation_key)


def section(prompt: str) -> str:
    # The pinned guidance only; a reviewer's diff may quote guidance files the change edits.
    start = prompt.index("## Project guidance")
    end = prompt.find("\n## ", start + 1)
    return prompt[start:] if end < 0 else prompt[start:end]


def bodies(text: str) -> set[str]:
    return {b for b in BODIES if b in text}


class TestGuidanceCatalog(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.repo = make_project(self.root)
        self.sha = head(self.repo)

    def load(self, commit=None):
        return load_guidance(GuidancePin(workdir=str(self.repo), commit=commit or self.sha, origin="test"))

    def test_activities_win_over_tags_and_review_adds_docs_by_diff_path(self):
        g = self.load()
        paths = lambda selected: [s.doc.path for s in selected]  # noqa: E731
        self.assertEqual(paths(g.select(Selection(activities=("versioning",), tags=("tests",)))),
                         ["docs/versioning.md"])
        self.assertEqual(paths(g.select(Selection(tags=("Tests",)))), ["docs/testing.md"])
        self.assertEqual(paths(g.select(Selection())), [])
        review = g.select(Selection(tags=("tests",)), changed_paths=("VERSION", "calc.py"))
        self.assertEqual(paths(review), ["docs/versioning.md", "docs/testing.md"])
        self.assertEqual([s.reason for s in review], ['diff touches VERSION', 'tag "tests"'])

    def test_glob_star_stays_in_one_directory_and_double_star_spans_them(self):
        self.assertTrue(matches("src/*/__init__.py", "src/pkg/__init__.py"))
        self.assertFalse(matches("src/*/__init__.py", "src/a/b/__init__.py"))
        self.assertTrue(matches("tests/**", "tests/unit/deep/test_x.py"))
        self.assertTrue(matches("**/*.md", "README.md"))
        self.assertFalse(matches("VERSION", "VERSION.txt"))

    def test_no_match_renders_general_rules_and_catalog_only(self):
        text = self.load().render([])
        self.assertIn("GENERAL-RULE", text)
        self.assertEqual(bodies(text), set())
        for line in ("- docs/versioning.md: Changing the version", "- docs/testing.md: Writing tests",
                     "- docs/issues.md: Writing an issue"):
            self.assertIn(line, text)
        self.assertIn(f"Source: {self.sha}:AGENTS.md", text)
        self.assertIn(f"git show {self.sha}:<path>", text)

    def test_reads_the_commit_not_the_working_tree(self):
        g_before = self.load()
        (self.repo / "AGENTS.md").write_text("UNCOMMITTED-GENERAL\n")
        (self.repo / "docs/versioning.md").write_text("UNCOMMITTED-VERSIONING\n")
        git(self.repo, "add", "AGENTS.md")
        git(self.repo, "commit", "-qm", "later commit")
        g = self.load()
        selected = g.select(Selection(activities=("release",)))
        text = g.render(selected)
        self.assertEqual(text, g_before.render(g_before.select(Selection(activities=("release",)))))
        self.assertIn("GENERAL-RULE", text)
        self.assertIn("VERSIONING-BODY", text)
        self.assertNotIn("UNCOMMITTED", text)
        self.assertIn(f"Source: {self.sha}:docs/versioning.md", text)
        self.assertIn('Selected because: activity "release"', text)

    def test_bad_catalog_or_missing_document_fails_with_the_commit(self):
        cases = {
            "unknown key 'tag'": '[[docs]]\npath = "docs/issues.md"\ntag = ["x"]\n',
            "is not listed in commit": '[[docs]]\npath = "docs/missing.md"\n',
            "must be a relative path inside the repository": '[[docs]]\npath = "../outside.md"\n',
            "invalid TOML": "[[docs]\n",
            "listed twice": '[[docs]]\npath = "docs/issues.md"\n[[docs]]\npath = "docs/issues.md"\n',
        }
        for message, catalog in cases.items():
            with self.subTest(message):
                (self.repo / "guidance.toml").write_text(catalog)
                git(self.repo, "commit", "-qam", message)
                with self.assertRaisesRegex(ValueError, rf"guidance\.toml at {head(self.repo)[:12]}.*{message}"):
                    self.load(head(self.repo))

    def test_entry_symlinked_outside_the_repository_is_refused(self):
        (self.repo / "AGENTS.md").unlink()
        (self.repo / "AGENTS.md").symlink_to("../outside.md")
        git(self.repo, "add", "-A")
        git(self.repo, "commit", "-qm", "link")
        with self.assertRaisesRegex(ValueError, "resolves outside the worktree"):
            self.load(head(self.repo))

    def test_project_without_guidance_files_renders_nothing(self):
        for rel in ("AGENTS.md", "guidance.toml"):
            (self.repo / rel).unlink()
        git(self.repo, "commit", "-qam", "drop")
        self.assertEqual(self.load(head(self.repo)).render([]), "")


class _Harness(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.log = self.root / "calls.jsonl"
        self.plan = self.root / "plan.json"
        self.enterContext(patch.dict(os.environ, {"FAKE_CLAUDE_LOG": str(self.log), "FAKE_PLAN": str(self.plan)}))
        self.db = SQLiteDatabase(":memory:")
        self.addCleanup(self.db.close)
        self.workspaces = SQLiteWorkspaceStore(self.db)
        self.tasks = SQLiteTaskMetadataStore(self.db)
        self.caps = Mock(permissions=InMemoryPermission())
        self.caps.is_interrupted.return_value = False
        self.caps.memory.load_baseline.return_value = {}
        self.launcher = ClaudeCodeLauncher(name="fake", command=fake_command(self.root))
        self.repo = make_project(self.root)
        git(self.repo, "checkout", "-qb", "hb/task-1")
        self.start = head(self.repo)

    def set_plan(self, sde=(), reviewer=()):
        for name in ("sde", "reviewer"):
            Path(f"{self.plan}.{name}.n").unlink(missing_ok=True)
        self.plan.write_text(json.dumps({"sde": list(sde), "reviewer": list(reviewer)}))

    def calls(self, role=None):
        rows = [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []
        return [r for r in rows if role is None or r["role"] == role]

    def reviewer(self):
        return CoderReviewerExecutor(profile=ClaudeCodeProfile(name="coder-reviewer", launcher=self.launcher),
                                     workspaces=self.workspaces, tasks=self.tasks)

    def sde(self, review=True):
        return SDEExecutor(
            profile=ClaudeCodeProfile(name="sde", launcher=self.launcher),
            options=SDEOptions(review=review),
            reviewer=self.reviewer() if review else None,
            worktree_manager=OrcaWorktreeManager(workspace_store=self.workspaces,
                                                 session_store=SQLiteSessionStoreAdapter(self.db), backend="git"),
            inbox_checkout_map={},
            tasks=self.tasks,
        )

    def payload(self, **extra):
        return {"executor_id": "sde", "text": "the task", "explicit_workdir": str(self.repo), "base_ref": "main",
                **extra}

    async def run_sde(self, review=True, correlation_key="task:1", **extra):
        outcome = await self.sde(review).execute([event(self.payload(**extra), correlation_key)], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        return outcome


class TestDevelopmentAndBuiltInReview(_Harness):
    async def test_version_task_selects_by_explicit_activity_over_tags(self):
        self.set_plan(sde=[dev(VERSION_BUMP)], reviewer=[PASS])
        await self.run_sde(activities=["versioning"], tags=["tests"])
        dev_call, review_call = self.calls()
        self.assertEqual(bodies(dev_call["prompt"]), {"VERSIONING-BODY"})
        self.assertIn('Selected because: activity "versioning"', dev_call["prompt"])
        self.assertEqual(bodies(section(review_call["prompt"])), {"VERSIONING-BODY"})
        for call in (dev_call, review_call):
            self.assertIn("GENERAL-RULE", call["prompt"])
            self.assertIn("- docs/issues.md: Writing an issue", call["prompt"])
            self.assertIn(f"Source: {self.start}:docs/versioning.md", call["prompt"])
        record = self.tasks.get("task:1")
        assert record is not None
        self.assertEqual((record.guidance_commit, record.workdir, record.activities, record.tags),
                         (self.start, str(self.repo), ("versioning",), ("tests",)))

    async def test_pure_test_task_selects_by_issue_label(self):
        self.set_plan(sde=[dev(NEW_TEST)], reviewer=[PASS])
        await self.run_sde(labels=[{"name": "tests"}, {"name": "good first issue"}])
        dev_call, review_call = self.calls()
        self.assertEqual(bodies(dev_call["prompt"]), {"TESTING-BODY"})
        self.assertIn('Selected because: tag "tests"', dev_call["prompt"])
        self.assertEqual(bodies(section(review_call["prompt"])), {"TESTING-BODY"})

    async def test_diff_adds_guidance_the_developer_was_not_given(self):
        self.set_plan(sde=[dev({**NEW_TEST, "VERSION": "2.0.0\n"})], reviewer=[PASS])
        await self.run_sde(tags=["tests"])
        dev_call, review_call = self.calls()
        self.assertEqual(bodies(dev_call["prompt"]), {"TESTING-BODY"})
        self.assertEqual(bodies(section(review_call["prompt"])), {"TESTING-BODY", "VERSIONING-BODY"})
        self.assertIn("Selected because: diff touches VERSION", review_call["prompt"])

    async def test_unmatched_task_gets_general_rules_and_catalog_only(self):
        self.set_plan(sde=[dev({"calc.py": "def divide(a, b):\n    return a / b\n"})], reviewer=[PASS])
        await self.run_sde()
        for call in self.calls():
            self.assertIn("GENERAL-RULE", call["prompt"])
            self.assertIn("- docs/testing.md: Writing tests", call["prompt"])
            self.assertEqual(bodies(section(call["prompt"])), set())

    async def test_committed_and_uncommitted_guidance_edits_never_change_the_pinned_version(self):
        # Uncommitted guidance in the worktree when the task starts is not part of the task's guidance.
        (self.repo / "AGENTS.md").write_text("DRAFT-AT-START\n")
        relaxed = {
            "AGENTS.md": "RELAXED-GENERAL: reviewers pass everything\n",
            "guidance.toml": '[[docs]]\npath = "docs/issues.md"\nactivities = ["versioning"]\n',
            "docs/versioning.md": "RELAXED-VERSIONING\n",
            **VERSION_BUMP,
        }
        fix = dev({"src/pkg/__init__.py": "__version__ = '1.1.0'  # fixed\n"},
                  write_uncommitted={"docs/issues.md": "RELAXED-UNCOMMITTED\n"})
        # The uncommitted edit is sent back once by the worktree check; the developer then commits it.
        tidy = dev(commit="commit the draft")
        self.set_plan(sde=[dev(relaxed, commit="relax guidance"), fix, tidy], reviewer=[CITES_VERSIONING, PASS])
        outcome = await self.run_sde(activities=["versioning"])
        self.assertIn("review=pass after 2 round(s)", outcome.messages[0])
        self.assertNotEqual(head(self.repo), self.start)
        roles = [c["role"] for c in self.calls()]
        self.assertEqual(roles, ["sde", "reviewer", "sde", "sde", "reviewer"])
        dev_call, _, fix, nudge, _ = self.calls()
        self.assertIn("uncommitted changes", nudge["prompt"])
        self.assertIn("docs/issues.md", nudge["prompt"])
        self.assertNotIn("RELAXED", fix["prompt"])
        self.assertNotIn("RELAXED", nudge["prompt"])
        self.assertNotIn("DRAFT-AT-START", dev_call["prompt"])
        for call in (dev_call, *self.calls("reviewer")):
            guidance = section(call["prompt"])
            self.assertNotIn("RELAXED", guidance)
            self.assertIn("GENERAL-RULE", guidance)
            self.assertIn("VERSIONING-BODY", guidance)
            self.assertIn(f"Source: {self.start}:AGENTS.md", guidance)
            self.assertIn(f"Pinned to commit {self.start}", guidance)

    async def test_rework_round_resends_cited_guidance_once_with_its_clause(self):
        # Pure test task; the diff touches VERSION, the reviewer cites the versioning document.
        self.set_plan(sde=[dev({**NEW_TEST, "VERSION": "2.0.0\n"}), dev(), dev(VERSION_BUMP)],
                      reviewer=[CITES_VERSIONING, CITES_VERSIONING, PASS])
        outcome = await self.run_sde(tags=["tests"])
        self.assertIn("review=pass after 3 round(s)", outcome.messages[0])
        first, fix1, fix2 = self.calls("sde")
        self.assertEqual(bodies(first["prompt"]), {"TESTING-BODY"})
        self.assertIn("Guidance: docs/versioning.md", fix1["prompt"])
        self.assertIn('Clause: "bump VERSION and src/pkg/__init__.py in the same commit"', fix1["prompt"])
        self.assertIn("VERSIONING-BODY", fix1["prompt"])
        self.assertIn(f"Source: {self.start}:docs/versioning.md", fix1["prompt"])
        self.assertIn("Selected because: cited by blocking item 1", fix1["prompt"])
        # The developer session already holds it: the next round cites it again without resending the body.
        self.assertIn("Guidance: docs/versioning.md", fix2["prompt"])
        self.assertNotIn("VERSIONING-BODY", fix2["prompt"])

    async def test_resumed_review_retry_keeps_the_pinned_commit(self):
        self.set_plan(sde=[dev(VERSION_BUMP)], reviewer=[{"error": "reviewer crashed"}, PASS])
        executor = self.sde()
        outcome = await executor.execute([event(self.payload(activities=["versioning"]))], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        (self.repo / "docs/versioning.md").write_text("CHANGED-WHILE-SUSPENDED\n")
        git(self.repo, "commit", "-qam", "edit while suspended")
        session = SessionRef(session_id="s", inbox_id="proj:x", executor_id="sde", status="suspended",
                             checkpoint=outcome.session_checkpoint, pending_request_id=outcome.request_id)
        resumed = await executor.execute(
            [event({"approval_request_id": outcome.request_id, "approved": True})], session, self.caps)
        assert isinstance(resumed, Emit), resumed
        last = section(self.calls("reviewer")[-1]["prompt"])
        self.assertIn("VERSIONING-BODY", last)
        self.assertNotIn("CHANGED-WHILE-SUSPENDED", last)
        self.assertIn(f"Source: {self.start}:docs/versioning.md", last)

    async def test_a_later_task_on_the_same_work_key_reuses_the_recorded_commit(self):
        self.set_plan(sde=[dev(VERSION_BUMP, commit="bump")], reviewer=[PASS])
        await self.run_sde(review=False, activities=["versioning"])
        (self.repo / "docs/versioning.md").write_text("LATER-VERSIONING\n")
        git(self.repo, "commit", "-qam", "later guidance")
        self.set_plan(sde=[dev()])
        await self.run_sde(review=False, activities=["release"])
        second = self.calls("sde")[-1]["prompt"]
        self.assertIn("VERSIONING-BODY", second)
        self.assertNotIn("LATER-VERSIONING", second)
        record = self.tasks.get("task:1")
        assert record is not None
        self.assertEqual((record.guidance_commit, record.activities), (self.start, ("release",)))


class TestIndependentReview(_Harness):
    def review_payload(self, **extra):
        return {"executor_id": "coder-reviewer", "text": "the task", "base_ref": "main", **extra}

    async def review(self, payload, correlation_key="review:task:1"):
        outcome = await self.reviewer().execute([event(payload, correlation_key)], None, self.caps)
        assert isinstance(outcome, Emit), outcome
        return outcome, self.calls("reviewer")[-1]

    async def test_review_of_a_developer_task_reads_the_commit_recorded_at_task_start(self):
        relaxed = {"AGENTS.md": "RELAXED-GENERAL\n", "docs/versioning.md": "RELAXED-VERSIONING\n", **VERSION_BUMP}
        self.set_plan(sde=[dev(relaxed, commit="relax", write_uncommitted={"docs/testing.md": "RELAXED-TESTING\n"})])
        await self.run_sde(review=False, activities=["versioning"])
        self.set_plan(reviewer=[CITES_VERSIONING])
        # Neither a workspace record nor explicit_workdir: the task metadata names the developer's workdir.
        outcome, call = await self.review(self.review_payload(review_of="task:1"))
        self.assertIn(f"Pinned to commit {self.start}", call["prompt"])
        self.assertIn("developer task task:1", call["prompt"])
        self.assertEqual(Path(call["cwd"]).resolve(), self.repo)
        self.assertIn("GENERAL-RULE", section(call["prompt"]))
        self.assertNotIn("RELAXED", section(call["prompt"]))
        # Developer's activity plus the diff (VERSION, src/pkg, AGENTS.md, docs/...).
        self.assertEqual(bodies(section(call["prompt"])), {"VERSIONING-BODY"})
        self.assertIn("Guidance: docs/versioning.md", outcome.messages[0])
        self.assertIn('Clause: "bump VERSION and src/pkg/__init__.py in the same commit"', outcome.messages[0])

    async def test_review_with_only_explicit_workdir_reads_the_merge_base(self):
        (self.repo / "AGENTS.md").write_text("RELAXED-GENERAL\n")
        (self.repo / "VERSION").write_text("9.9.9\n")
        git(self.repo, "commit", "-qam", "change under review rewrites the rules")
        (self.repo / "docs/versioning.md").write_text("RELAXED-UNCOMMITTED\n")
        self.set_plan(reviewer=[PASS])
        _, call = await self.review(self.review_payload(explicit_workdir=str(self.repo)))
        base = subprocess.run(["git", "merge-base", "main", "HEAD"], cwd=self.repo, capture_output=True,
                              text=True, check=True).stdout.strip()
        self.assertEqual(base, self.start)
        self.assertIn(f"Pinned to commit {base} (merge base of main and HEAD", call["prompt"])
        self.assertIn(f"Source: {base}:AGENTS.md", call["prompt"])
        # Verifiable: the quoted body is exactly what `git show <base>:AGENTS.md` prints.
        shown = subprocess.run(["git", "show", f"{base}:AGENTS.md"], cwd=self.repo, capture_output=True,
                               text=True, check=True).stdout.strip()
        self.assertIn(f"----- begin AGENTS.md -----\n{shown}\n----- end AGENTS.md -----", call["prompt"])
        self.assertIn("GENERAL-RULE", section(call["prompt"]))
        self.assertNotIn("RELAXED", section(call["prompt"]))
        self.assertEqual(bodies(section(call["prompt"])), {"VERSIONING-BODY"})
        self.assertIn("Selected because: diff touches VERSION", call["prompt"])

    async def test_independent_review_takes_its_own_activities_and_labels(self):
        (self.repo / "calc.py").write_text("def divide(a, b):\n    return a / b\n")
        git(self.repo, "commit", "-qam", "divide")
        self.set_plan(reviewer=[PASS])
        _, call = await self.review(self.review_payload(explicit_workdir=str(self.repo),
                                                        labels=["issue"]))
        self.assertEqual(bodies(section(call["prompt"])), {"ISSUES-BODY"})

    async def test_blocking_item_citing_guidance_needs_a_clause(self):
        bad = json.loads(json.dumps(CITES_VERSIONING))
        del bad["blocking"][0]["guidance_clause"]
        (self.repo / "VERSION").write_text("9.9.9\n")
        self.set_plan(reviewer=[bad])
        outcome, _ = await self.review(self.review_payload(explicit_workdir=str(self.repo)))
        self.assertIn("review failed", outcome.messages[0])
        self.assertIn("guidance_path and guidance_clause", outcome.messages[0])


class TestNonAsciiDiffPaths(_Harness):
    review_payload = TestIndependentReview.review_payload
    review = TestIndependentReview.review

    # git quotes non-ASCII paths ("\\347\\211\\210...") unless asked for raw output; matching must see real names.
    CN_DOC = "docs/规范/版本.md"
    CN_BODY = "CN-VERSION-BODY: 改版本时同步更新 版本/当前.txt。"

    def setUp(self):
        super().setUp()
        git(self.repo, "checkout", "-q", "main")
        (self.repo / "docs/规范").mkdir(parents=True)
        (self.repo / self.CN_DOC).write_text(self.CN_BODY + "\n", encoding="utf-8")
        (self.repo / "版本").mkdir()
        (self.repo / "版本/当前.txt").write_text("1.0.0\n", encoding="utf-8")
        catalog = (self.repo / "guidance.toml").read_text(encoding="utf-8") + (
            '\n[[docs]]\npath = "docs/规范/版本.md"\nsummary = "中文版本规范"\n'
            'paths = ["版本/*.txt", "docs/规范/版本.md"]\n'
        )
        (self.repo / "guidance.toml").write_text(catalog, encoding="utf-8")
        git(self.repo, "add", "-A")
        git(self.repo, "commit", "-qm", "中文规范")
        git(self.repo, "checkout", "-qb", "hb/task-cn")
        self.base = head(self.repo)
        quoted = subprocess.run(["git", "ls-files", "版本"], cwd=self.repo, capture_output=True,
                                text=True, check=True).stdout
        # Precondition: this git quotes the path, so the regression is real on this machine.
        self.assertIn('"', quoted)

    def assert_cn_selected(self, call, touched):
        guidance = section(call["prompt"])
        self.assertIn(f"Selected because: diff touches {touched}", guidance)
        self.assertIn(f"### Selected guidance: {self.CN_DOC}", guidance)
        self.assertIn(f"Source: {self.base}:{self.CN_DOC}", guidance)
        self.assertIn(self.CN_BODY, guidance)

    async def test_tracked_chinese_paths_select_their_guidance(self):
        (self.repo / "版本/当前.txt").write_text("1.1.0\n", encoding="utf-8")
        git(self.repo, "commit", "-qam", "bump")
        self.set_plan(reviewer=[PASS])
        _, call = await self.review(self.review_payload(explicit_workdir=str(self.repo)))
        self.assert_cn_selected(call, "版本/当前.txt")

    async def test_tracked_edit_of_the_chinese_guidance_document_selects_its_pinned_body(self):
        (self.repo / self.CN_DOC).write_text("RELAXED-CN\n", encoding="utf-8")
        git(self.repo, "commit", "-qam", "relax")
        self.set_plan(reviewer=[PASS])
        _, call = await self.review(self.review_payload(explicit_workdir=str(self.repo)))
        self.assert_cn_selected(call, self.CN_DOC)
        self.assertNotIn("RELAXED-CN", section(call["prompt"]))

    async def test_uncommitted_chinese_edit_selects_its_guidance(self):
        (self.repo / "版本/当前.txt").write_text("2.0.0\n", encoding="utf-8")
        self.set_plan(reviewer=[PASS])
        _, call = await self.review(self.review_payload(explicit_workdir=str(self.repo)))
        self.assert_cn_selected(call, "版本/当前.txt")

    async def test_untracked_chinese_file_selects_its_guidance(self):
        (self.repo / "版本/新增说明.txt").write_text("new\n", encoding="utf-8")
        self.set_plan(reviewer=[PASS])
        _, call = await self.review(self.review_payload(explicit_workdir=str(self.repo)))
        self.assert_cn_selected(call, "版本/新增说明.txt")

    async def test_sde_built_in_review_matches_chinese_paths(self):
        self.set_plan(sde=[dev({"版本/当前.txt": "3.0.0\n"})], reviewer=[PASS])
        await self.run_sde()
        dev_call, review_call = self.calls()
        self.assertNotIn(self.CN_BODY, dev_call["prompt"])
        self.assert_cn_selected(review_call, "版本/当前.txt")


if __name__ == "__main__":
    unittest.main()
