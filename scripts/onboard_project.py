"""Onboard a GitHub repository as a Hibernaut project fed by the Switchboard webhook.

    python3 scripts/onboard_project.py kun-g/new-tool [--config hibernaut.toml] [--dry-run]

Clones the repository next to the Hibernaut checkout when missing, creates the trigger label, and appends a
[projects.<name>] table to hibernaut.toml (hot-reloaded by the running daemon). Re-running is safe.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tomllib
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_runtime.app.config import parse_config  # noqa: E402

_REPO = re.compile(r"^(?P<owner>[A-Za-z0-9-]+)/(?P<name>[A-Za-z0-9._-]+)$")
_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]*$")

Runner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


def _run(args: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(list(args), capture_output=True, text=True, check=False)


@dataclass
class Plan:
    repo: str
    name: str
    config: Path
    checkout: Path
    label: str
    auto_merge: bool
    steps: list[str] = field(default_factory=list)


def project_block(plan: Plan) -> str:
    return f"""
[projects.{plan.name}]
# Onboarded by scripts/onboard_project.py: issues labelled "{plan.label}" arrive through the Switchboard webhook.
inbox_id = ["proj:{plan.name}", "proj:{plan.name}:.+"]
repo = "{plan.repo}"
checkout = "{plan.checkout}"
worktree_backend = "orca"
executor = "sde"

[projects.{plan.name}.delivery]
open_pr = true
auto_merge = {"true" if plan.auto_merge else "false"}
"""


def ensure_checkout(plan: Plan, run: Runner, dry_run: bool) -> None:
    if (plan.checkout / ".git").exists():
        origin = run(["git", "-C", str(plan.checkout), "remote", "get-url", "origin"]).stdout.strip().lower()
        if plan.repo.lower() not in origin:
            raise SystemExit(f"{plan.checkout} exists but its origin is {origin or 'unset'}, not {plan.repo}")
        plan.steps.append(f"checkout: {plan.checkout} (existing)")
        return
    if plan.checkout.exists() and any(plan.checkout.iterdir()):
        raise SystemExit(f"{plan.checkout} exists and is not a git checkout")
    if not dry_run:
        done = run(["gh", "repo", "clone", plan.repo, str(plan.checkout)])
        if done.returncode != 0:
            raise SystemExit(f"gh repo clone {plan.repo} failed: {done.stderr.strip()}")
    plan.steps.append(f"checkout: {plan.checkout} (cloned)")


def ensure_label(plan: Plan, run: Runner, dry_run: bool) -> None:
    exists = run(["gh", "api", f"repos/{plan.repo}/labels/{plan.label}", "--silent"]).returncode == 0
    if exists:
        plan.steps.append(f"label: {plan.label} (existing)")
        return
    if not dry_run:
        done = run(
            ["gh", "label", "create", plan.label, "-R", plan.repo, "--color", "5319E7",
             "--description", "交给 Hibernaut 处理（经 Switchboard Webhook 入站）"]
        )
        if done.returncode != 0:
            raise SystemExit(f"creating label {plan.label} on {plan.repo} failed: {done.stderr.strip()}")
    plan.steps.append(f"label: {plan.label} (created)")


def ensure_project(plan: Plan, dry_run: bool) -> None:
    text = plan.config.read_text(encoding="utf-8")
    existing = tomllib.loads(text).get("projects", {})
    for name, item in existing.items():
        if name == plan.name or str(item.get("repo", "")).lower() == plan.repo.lower():
            plan.steps.append(f"hibernaut.toml: [projects.{name}] already serves {item.get('repo')}")
            return
    updated = text.rstrip("\n") + "\n" + project_block(plan)
    # Validate the whole file first: the running daemon would otherwise keep its old settings and only log.
    parse_config(tomllib.loads(updated), base_dir=plan.config.parent)
    if not dry_run:
        tmp = plan.config.with_name(plan.config.name + ".onboard.tmp")
        tmp.write_text(updated, encoding="utf-8")
        tmp.replace(plan.config)
    plan.steps.append(f"hibernaut.toml: [projects.{plan.name}] added")


def onboard(argv: Sequence[str] | None = None, *, run: Runner = _run) -> Plan:
    parser = argparse.ArgumentParser(prog="onboard_project", description=__doc__.splitlines()[0])
    parser.add_argument("repo", help="owner/name on GitHub")
    parser.add_argument("--config", default="hibernaut.toml")
    parser.add_argument("--name", help="project name and inbox suffix (default: lower-cased repository name)")
    parser.add_argument("--checkout", help="default: next to the Hibernaut checkout, <config dir>/../<name>")
    parser.add_argument(
        "--label", default="hibernaut", help="repository label to create (match Switchboard [github] require_label)",
    )
    parser.add_argument("--auto-merge", action="store_true", help="merge after review and CI pass")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    match = _REPO.match(args.repo)
    if match is None:
        parser.error(f"repo must be owner/name, got {args.repo!r}")
    name = (args.name or match.group("name")).lower()
    if not _NAME.match(name):
        # The Switchboard owner route maps a repo to proj:<lower-cased repo name>, so the inbox must match it.
        parser.error(f"project name {name!r} must be lower-case letters, digits, '-' or '_'; pass --name")
    config = Path(args.config).resolve()
    checkout = Path(args.checkout).expanduser().resolve() if args.checkout else config.parent.parent / name
    plan = Plan(args.repo, name, config, checkout, args.label, args.auto_merge)

    if run(["gh", "api", f"repos/{plan.repo}", "--silent"]).returncode != 0:
        raise SystemExit(f"{plan.repo} is not reachable with gh")
    ensure_checkout(plan, run, args.dry_run)
    ensure_label(plan, run, args.dry_run)
    ensure_project(plan, args.dry_run)
    return plan


def main(argv: Sequence[str] | None = None) -> int:
    plan = onboard(argv)
    for step in plan.steps:
        print(f"[onboard] {step}")
    owner = plan.repo.split("/")[0]
    if plan.name != plan.repo.split("/")[1].lower():
        print(
            f'[onboard] --name differs from the repository name: add "{plan.repo.lower()}" = "proj:{plan.name}" '
            "to Switchboard [routing.github_repos], the owner route would send it to another inbox."
        )
    print(
        f"[onboard] Switchboard routes {plan.repo} to proj:{plan.name} when [routing.github_owners] has "
        f'"{owner.lower()}" = "proj:{{repo}}" and comments when [outbound] github_app_owners lists {owner.lower()}.'
    )
    print(f'[onboard] Label an issue "{plan.label}" in {plan.repo} to hand it to Hibernaut.')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
