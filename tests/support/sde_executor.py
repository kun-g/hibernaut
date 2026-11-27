import atexit
import functools
import shutil
import tempfile
import time
from pathlib import Path

from agent_runtime.core.models import Event
from tests.support.coder_reviewer import git


PROXY = "http://127.0.0.1:9"
PASS = {"verdict": "pass", "summary": "zero is rejected", "blocking": []}
BUGGY = {"calc.py": "def divide(a, b):\n    return a / b\n"}
FIXED = {"calc.py": "def divide(a, b):\n    if b == 0:\n        raise ValueError('b')\n    return a / b\n"}
EVIDENCE = "red: test_zero FAILED (ZeroDivisionError)\ngreen: 4 passed; lint-imports ok; pyright 0 errors"


@functools.lru_cache(maxsize=1)
def _template_base_repo() -> Path:
    tmp = tempfile.TemporaryDirectory(prefix="sde_base_tmpl_")
    atexit.register(tmp.cleanup)
    repo = Path(tmp.name) / "repo"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main")
    git(repo, "config", "user.email", "t@example.invalid")
    git(repo, "config", "user.name", "t")
    (repo / "calc.py").write_text("def divide(a, b):\n    return a\n")
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "base")
    return repo


def make_base_repo(root: Path) -> Path:
    repo = root / "repo"
    shutil.copytree(_template_base_repo(), repo)
    return repo


def dev(write=None, **output):
    out = {"status": "done", "summary": "divide implemented", "test_evidence": EVIDENCE, "change_kind": "fix", "change_rationale": "divide handled zero wrongly"}
    out.update(output)
    return {"write": write or {}, "output": out}


def event(payload, correlation_key="task:1", inbox_id="proj:x"):
    return Event(
        inbox_id=inbox_id,
        source="test",
        external_id=f"e-{time.monotonic_ns()}",
        payload=payload,
        correlation_key=correlation_key,
    )
