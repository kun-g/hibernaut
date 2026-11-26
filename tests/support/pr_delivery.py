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
from unittest.mock import Mock

from agent_runtime.adapters.delivery import DeliveryConfig, DeliveryRecord, DeliveryRequest, PRDelivery
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase, SQLiteDeliveryStore
from tests.support import fake_gh


FAKE_GH = Path(__file__).with_name("fake_gh.py")
VERSION_FILE = "pkg/__init__.py"


def git(cwd, *args):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


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
        shutil.copytree(tmpl_bare, self.bare)
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

    @functools.cached_property
    def seed(self):
        return self.clone("seed", None)

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
