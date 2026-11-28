"""Plan GitHub Actions test jobs after applying the PR's affected-test selection."""

import argparse
import json
import sys
from pathlib import Path

from scripts.select_tests import ROOT, git_changed_files, select_tests


def build_matrix(root: Path, selected: tuple[str, ...] | None) -> dict[str, list[dict[str, object]]]:
    full = selected is None
    integration = (
        sorted(p.relative_to(root).as_posix() for p in (root / "tests/integration").rglob("test_*.py"))
        if full else sorted(path for path in selected if path.startswith("tests/integration/"))
    )
    # Small selections cost less to run together than to start a second runner.
    offload = integration[::2] if len(integration) >= 12 else []
    if selected is None:
        # Retain pytest's default discovery, including future tests outside today's file pattern.
        primary = [f"--ignore={path}" for path in offload]
    else:
        excluded = set(offload)
        primary = [path for path in selected if path not in excluded]

    mode = "parallel" if full or len(primary) >= 4 else "serial" if primary else "skip"
    lanes = [{"lane": 1, "mode": mode, "args": primary}]
    if offload:
        lanes.append({"lane": 2, "mode": "parallel", "args": offload})
    return {"include": lanes}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", help="Select tests affected since the merge base. Omit to run the full suite.")
    args = parser.parse_args()
    selected = None
    if args.base:
        result = select_tests(git_changed_files(args.base, include_worktree=False))
        print(result.reason, file=sys.stderr)
        if not result.full_suite:
            selected = result.tests
    print(json.dumps(build_matrix(ROOT, selected), separators=(",", ":")))


if __name__ == "__main__":
    main()
