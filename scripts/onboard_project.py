"""Onboard a GitHub repository as a Hibernaut project fed by the Switchboard webhook.

    python3 scripts/onboard_project.py owner/new-tool [--config hibernaut.toml] [--dry-run]

Clones the repository next to the Hibernaut checkout when missing, registers it in Orca, creates the trigger label, and
adds a [projects.<name>] table with its Steward to hibernaut.toml (hot-reloaded by the running daemon), whose previous
version stays beside it as a backup. The system Steward does the same through its onboard_project tool. Re-running is
safe.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from agent_runtime.app.onboarding import (  # noqa: E402
    DEFAULT_LABEL,
    OnboardError,
    Onboarding,
    Request,
    Runner,
    description_text,
    project_name,
    run_command,
    split_repo,
)


@dataclass
class Plan:
    repo: str
    name: str
    config: Path
    checkout: Path
    label: str
    auto_merge: bool
    steps: list[str] = field(default_factory=list[str])
    by_hand: list[str] = field(default_factory=list[str])
    backup: Path | None = None


def onboard(argv: Sequence[str] | None = None, *, run: Runner = run_command) -> Plan:
    parser = argparse.ArgumentParser(prog="onboard_project", description=(__doc__ or "").splitlines()[0])
    parser.add_argument("repo", help="owner/name on GitHub")
    parser.add_argument("--config", default="hibernaut.toml")
    parser.add_argument("--name", help="project name and inbox suffix (default: lower-cased repository name)")
    parser.add_argument("--description", help="one sentence saying what the project is")
    parser.add_argument("--checkout", help="default: next to the Hibernaut checkout, <config dir>/../<name>")
    parser.add_argument(
        "--label", default=DEFAULT_LABEL,
        help="repository label to create (match Switchboard [github] require_label)",
    )
    parser.add_argument("--auto-merge", action="store_true", help="merge after review and CI pass")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    try:
        split_repo(args.repo)
        name = project_name(args.name.lower() if args.name else None, args.repo)
        description = description_text(args.description, args.repo)
    except OnboardError as exc:
        parser.error(str(exc))
    config = Path(args.config).resolve()
    # Nobody to wait for a reload here: the whole file is checked before it is written, and the daemon logs the rest.
    onboarding = Onboarding(config, run=run, write=lambda file, before, after: file.replace(before, after),
                            label_owners=lambda _config: None, steward_optional=True, dry_run=args.dry_run)
    checkout = Path(args.checkout).expanduser().resolve() if args.checkout else onboarding.default_checkout(name)
    plan = Plan(args.repo, name, config, checkout, args.label, args.auto_merge)
    try:
        report = onboarding.onboard(Request(
            repo=plan.repo, name=name, checkout=checkout, description=description, label=plan.label,
            auto_merge=plan.auto_merge,
        ))
    except OnboardError as exc:
        raise SystemExit(str(exc)) from exc
    plan.steps, plan.by_hand, plan.backup = report.done, report.by_hand, report.backup
    return plan


def main(argv: Sequence[str] | None = None) -> int:
    plan = onboard(argv)
    for step in plan.steps:
        print(f"[onboard] {step}")
    for step in plan.by_hand:
        print(f"[onboard] left to you: {step}")
    if plan.backup is not None:
        print(f"[onboard] undo: cp {plan.backup} {plan.config}")
    print(f'[onboard] Label an issue "{plan.label}" in {plan.repo} to hand it to Hibernaut.')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
