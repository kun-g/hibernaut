from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from agent_runtime.adapters.executor_cli.steward_git import ToolError
from agent_runtime.app.config import HibernautConfig, load_config
from agent_runtime.app.reload import ReloadFailed, ReloadStatus
from agent_runtime.app.system_tools import SystemToolbox


CONFIG = """# the deployment
[runtime]
db_path = "./hb.db"
default_agent = "ash"

[[runtime.inbox_bindings]]
pattern = "user:main"
default_agent = "ash"
agents = ["ash"]
workdir = "{root}"
checkout_access = "readonly"
forward_to = ["proj:old", "sys:ops"]   # who may get work

[[runtime.inbox_bindings]]
pattern = "sys:ops"
default_agent = "hibernaut-ops"
agents = ["hibernaut-ops"]
workdir = "{root}"
checkout_access = "readonly"
system_tools = true

[executors.claude-code]
harness = "claude-code"
[executors.claude-code.env]
ANTHROPIC_BASE_URL = "http://127.0.0.1:3425"
ANTHROPIC_AUTH_TOKEN = "placeholder-gw-tok"   # keep

[agents]
ash = {{ role = "steward", executor = "claude-code" }}
hibernaut-ops = {{ role = "steward", executor = "claude-code" }}
front = {{ role = "steward", executor = "claude-code" }}
reviewer = {{ role = "code-reviewer", executor = "claude-code" }}
sde = {{ role = "sde", executor = "claude-code", reviewer = "reviewer" }}

[onboarding]
label_owners = ["acme"]

[projects.old]   # by hand
repo = "acme/old"
checkout = "{root}/old"
inbox_id = ["proj:old", "proj:old:.+"]
default_agent = "sde"
description = "what people said"

[[projects.old.inbox_bindings]]
pattern = "proj:old"
default_agent = "front"
[[projects.old.inbox_bindings]]
pattern = "proj:old:.+"
default_agent = "sde"

[projects.bare]
repo = "acme/bare"
checkout = "{root}/bare"
inbox_id = ["proj:bare", "proj:bare:.+"]
default_agent = "sde"

[projects.bare.delivery]
open_pr = true
# the end
"""


class FakeCommands:
    # gh, git and orca as they answer; what exists is kept here.
    def __init__(self, *, labels: set[str] | None = None, origin: str = "", registered: set[str] | None = None) -> None:
        self.labels = labels if labels is not None else set()
        self.registered = registered if registered is not None else set()
        self.origin = origin
        self.origins: dict[str, str] = {}
        self.fail: dict[str, str] = {}
        self.forget = False
        self.during_clone: Callable[[], None] = lambda: None
        self.calls: list[list[str]] = []

    def __call__(self, argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
        args = list(argv)
        self.calls.append(args)
        ok, out = True, ""
        key = " ".join(args[:3])
        if key in self.fail:
            if key == "gh repo clone":
                Path(args[4], ".git").mkdir(parents=True)
            return subprocess.CompletedProcess(args, 1, "", self.fail[key])
        if args[:2] == ["gh", "api"] and "/labels/" in args[2]:
            ok = args[2].rsplit("/", 1)[1] in self.labels
        elif key == "gh repo clone":
            self.during_clone()
            Path(args[4], ".git").mkdir(parents=True)
            self.origins[args[4]] = self.origin or f"https://github.com/{args[3]}.git"
        elif key == "gh label create":
            self.labels.add(args[3])
        elif key == "orca repo show":
            ok = args[4].removeprefix("path:") in self.registered
        elif key == "orca repo add":
            self.registered |= set() if self.forget else {args[4]}
        elif args[:2] == ["git", "-C"] and args[3] == "remote":
            out = self.origins.get(args[2], self.origin)
        elif args[:2] == ["git", "-C"] and args[3] == "symbolic-ref":
            out = "main"
        return subprocess.CompletedProcess(args, 0 if ok else 1, out, "")

    def made(self, *start: str) -> list[list[str]]:
        return [c for c in self.calls if c[: len(start)] == list(start)]


class Reloads(ReloadStatus):
    # Stands for the running Hibernaut: takes every valid version up, unless it is told to refuse.
    def __init__(self, path: Path) -> None:
        super().__init__()
        self.watch(path)
        self.refuse: str | None = None
        self.waited: list[bytes] = []
        self.before_wait: Callable[[], None] = lambda: None
        self._original = path.read_bytes()

    def wait(self, contents: bytes, timeout: float = 1.0) -> None:
        self.waited.append(contents)
        self.before_wait()
        if self.refuse is not None and contents != self._original:
            raise ReloadFailed(self.refuse)


class OnboardingCase(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve() / "repos"
        self.home = self.root / "hibernaut"
        self.home.mkdir(parents=True)
        self.path = self.home / "hibernaut.toml"
        self.path.write_text(CONFIG.format(root=self.root))
        self.path.chmod(0o600)
        self.before = self.path.read_text()
        self.run_ = FakeCommands()
        self.reloads = Reloads(self.path)
        self.allowed = True
        self.box = SystemToolbox(reloads=self.reloads, allowed=lambda: self.allowed, run=self.run_, reload_timeout=1.0)

    def onboard(self, repo: str = "acme/new-tool", name: str | None = None,
                description: str | None = None) -> dict[str, Any]:
        return json.loads(self.box.onboard(repo=repo, name=name, description=description))

    def complete(self, name: str, description: str | None = None) -> dict[str, Any]:
        return json.loads(self.box.complete(name=name, description=description))

    def refused(self, message: str, call: Callable[[], Any]) -> str:
        with self.assertRaisesRegex(ToolError, message) as caught:
            call()
        return str(caught.exception)

    def unchanged(self) -> None:
        self.assertEqual(self.path.read_text(), self.before)

    def config(self) -> HibernautConfig:
        return load_config(self.path)

    def only_added(self, after: str) -> None:
        # What is new may stand between the old lines; the one list that grew in place is compared without its item.
        lines = iter(after.replace(', "proj:new-tool"]   # who', ']   # who')
                     .replace(', "proj:bare"]   # who', ']   # who').splitlines(keepends=True))
        for line in self.before.splitlines(keepends=True):
            self.assertIn(line, lines)
