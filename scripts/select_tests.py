from __future__ import annotations

import argparse
import ast
import fnmatch
import subprocess
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

FULL_SUITE_PATTERNS = (
    "pyproject.toml",
    ".importlinter",
    "pyrightconfig.json",
    ".github/workflows/*",
    "scripts/select_tests.py",
)

# Composition roots wire every optional adapter; leaf adapter edits must not fan out through them
# to unrelated tests that only use build_daemon() for other adapters.
COMPOSITION_ROOTS = frozenset({
    "src/agent_runtime/app/main.py",
    "src/agent_runtime/app/bootstrap.py",
    "src/agent_runtime/app/config.py",
})

# Feature adapters wired conditionally by composition roots (do not transitively cross COMPOSITION_ROOTS).
ISOLATED_ADAPTER_PREFIXES = (
    "src/switchboard/",
    "src/agent_runtime/adapters/delivery/",
    "src/agent_runtime/adapters/executor_cli/",
    "src/agent_runtime/adapters/credential_proxy/",
    "src/agent_runtime/adapters/channels/",
)

# Non-import edges: subprocess entrypoints and implicit runtime wiring.
STATIC_DEPS: dict[str, tuple[str, ...]] = {
    "scripts/run_updating.py": ("src/agent_runtime/app/main.py",),
    "tests/test_config_reload.py": ("scripts/run_updating.py",),
    "tests/test_auto_update.py": (
        "scripts/run_updating.py",
        "src/agent_runtime/__init__.py",
        "src/agent_runtime/adapters/executor_whitebox/executor.py",
    ),
    "tests/test_switchboard_relay.py": ("src/switchboard/relay_agent.py",),
    "tests/test_switchboard_bridge.py": ("src/switchboard/app.py",),
    "tests/test_github_poller_resilience.py": ("src/agent_runtime/adapters/channels/github_poller.py",),
    "tests/test_webhooks.py": (
        "src/agent_runtime/adapters/channels/channels.py",
        "src/agent_runtime/adapters/channels/github_webhook.py",
        "src/agent_runtime/adapters/channels/feishu_webhook.py",
        "src/agent_runtime/adapters/channels/http_types.py",
    ),
    "tests/test_mcp_server.py": ("src/agent_runtime/adapters/channels/mcp_server.py",),
    "tests/test_gdrive_tasks.py": ("src/agent_runtime/adapters/channels/gdrive_tasks.py",),
    "tests/test_gdrive_credential_proxy.py": ("src/agent_runtime/adapters/channels/gdrive_tasks.py",),
}

RESOURCE_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("src/agent_runtime/adapters/executor_cli/skills/**", ("src/agent_runtime/adapters/executor_cli/role_skills.py",)),
    ("hibernaut.example.toml", ("tests/test_bootstrap_loop.py", "tests/test_sde_delivery.py")),
    ("switchboard.example.toml", ("tests/test_switchboard_core.py",)),
    ("deploy/gdrive-proxy/**", ("tests/test_gdrive_credential_proxy.py",)),
    ("deploy/switchboard/**", ("tests/test_switchboard_deploy.py",)),
    ("docs/dispatch/**", ("tests/test_sde_delivery.py",)),
    ("docs/process/versioning.md", ("tests/test_sde_delivery.py",)),
    ("guidance.toml", ("tests/test_sde_delivery.py",)),
    ("AGENTS.md", ("tests/test_sde_delivery.py",)),
    ("README.md", ("tests/test_sde_delivery.py",)),
)

NO_TEST_PATTERNS = (
    "docs/**",
    "*.md",
    ".gitignore",
    "LICENSE",
)


@dataclass(frozen=True)
class SelectionResult:
    tests: tuple[str, ...]
    full_suite: bool
    reason: str


def _matches(pattern: str, rel: str) -> bool:
    if pattern.endswith("/**"):
        prefix = pattern[:-3]
        return rel == prefix or rel.startswith(prefix + "/")
    return fnmatch.fnmatchcase(rel, pattern)


def _is_test_file(rel: str) -> bool:
    return rel.startswith("tests/") and Path(rel).name.startswith("test_") and rel.endswith(".py")


def _discover_py_files(root: Path) -> list[str]:
    found: list[str] = []
    for base in ("src", "scripts", "tests"):
        folder = root / base
        if not folder.is_dir():
            continue
        for path in sorted(folder.rglob("*.py")):
            if "__pycache__" in path.parts or any(p.endswith(".egg-info") for p in path.parts):
                continue
            found.append(path.relative_to(root).as_posix())
    return found


def _module_name(rel: str) -> str | None:
    if not rel.endswith(".py"):
        return None
    if rel.startswith("src/"):
        trimmed = rel[4:]
    elif rel.startswith(("tests/", "scripts/")):
        trimmed = rel
    else:
        return None
    if trimmed.endswith("/__init__.py"):
        return trimmed[:-12].replace("/", ".")
    return trimmed[:-3].replace("/", ".")


def _resolve_relative(importing_rel: str, level: int, module: str | None) -> str | None:
    mod = _module_name(importing_rel)
    if mod is None:
        return None
    parts = mod.split(".")
    is_pkg = importing_rel.endswith("/__init__.py")
    pkg_parts = parts if is_pkg else parts[:-1]
    drop = level - 1
    if drop > len(pkg_parts):
        return None
    anchor = pkg_parts[: len(pkg_parts) - drop]
    if module:
        return ".".join([*anchor, *module.split(".")])
    return ".".join(anchor) if anchor else None


def _build_barrel_exports(root: Path, py_files: list[str], mod_to_file: dict[str, str]) -> dict[str, dict[str, str]]:
    # Map package module -> {exported_symbol: defining_file} from __init__.py re-exports.
    exports: dict[str, dict[str, str]] = {}
    for rel in py_files:
        if not rel.endswith("/__init__.py"):
            continue
        pkg_mod = _module_name(rel)
        if not pkg_mod:
            continue
        tree = ast.parse((root / rel).read_text(encoding="utf-8"), filename=rel)
        sym_map: dict[str, str] = {}
        for node in tree.body:
            if isinstance(node, ast.ImportFrom):
                base = _resolve_relative(rel, node.level, node.module) if node.level else node.module
                if not base:
                    continue
                target_file = mod_to_file.get(base)
                for alias in node.names:
                    if alias.name == "*":
                        continue
                    local = alias.asname or alias.name
                    sub_file = mod_to_file.get(f"{base}.{alias.name}")
                    if sub_file:
                        sym_map[local] = sub_file
                    elif target_file:
                        sym_map[local] = target_file
        exports[pkg_mod] = sym_map
    return exports


def _extract_edges(
    root: Path,
    rel: str,
    mod_to_file: dict[str, str],
    barrel_exports: dict[str, dict[str, str]],
) -> set[str]:
    deps: set[str] = set()
    source = (root / rel).read_text(encoding="utf-8")
    tree = ast.parse(source, filename=rel)
    is_init = rel.endswith("/__init__.py")

    def add_target(target: str | None) -> None:
        if target is not None and target != rel:
            deps.add(target)

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                add_target(mod_to_file.get(alias.name))
        elif isinstance(node, ast.ImportFrom):
            base = _resolve_relative(rel, node.level, node.module) if node.level else node.module
            if not base:
                continue
            base_file = mod_to_file.get(base)
            # Barrel __init__.py files don't fan back in from submodules; consumers resolve symbols directly.
            if base_file and base_file.endswith("/__init__.py") and not is_init:
                sym_map = barrel_exports.get(base, {})
                add_target(base_file)
                for alias in node.names:
                    if alias.name == "*":
                        for target in sym_map.values():
                            add_target(target)
                        continue
                    sub_file = mod_to_file.get(f"{base}.{alias.name}")
                    if sub_file:
                        add_target(sub_file)
                    elif alias.name in sym_map:
                        add_target(sym_map[alias.name])
            else:
                add_target(base_file)
                for alias in node.names:
                    if alias.name != "*":
                        add_target(mod_to_file.get(f"{base}.{alias.name}"))
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "with_name"
            and len(node.args) == 1
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            sibling = (Path(rel).parent / node.args[0].value).as_posix()
            if (root / sibling).exists() and sibling != rel:
                deps.add(sibling)

    return deps


def build_reverse_graph(root: Path = ROOT) -> tuple[dict[str, set[str]], list[str]]:
    py_files = _discover_py_files(root)
    mod_to_file: dict[str, str] = {}
    for rel in py_files:
        mod = _module_name(rel)
        if mod:
            mod_to_file[mod] = rel
    barrel_exports = _build_barrel_exports(root, py_files, mod_to_file)

    reverse: dict[str, set[str]] = defaultdict(set)
    for rel in py_files:
        # __init__.py barrels should not pull submodule edits out to unrelated consumers.
        if rel.endswith("/__init__.py"):
            continue
        for dep in _extract_edges(root, rel, mod_to_file, barrel_exports):
            reverse[dep].add(rel)

    for consumer, deps in STATIC_DEPS.items():
        if (root / consumer).exists():
            for dep in deps:
                if (root / dep).exists():
                    reverse[dep].add(consumer)

    all_tests = sorted(rel for rel in py_files if _is_test_file(rel))
    return reverse, all_tests


def _normalize_rel(path: str) -> str:
    rel = Path(path).as_posix()
    while rel.startswith("./"):
        rel = rel[2:]
    return rel.lstrip("/")


def select_tests(changed_files: list[str], *, root: Path = ROOT) -> SelectionResult:
    reverse, all_tests = build_reverse_graph(root)
    all_test_set = set(all_tests)
    normalized = sorted({_normalize_rel(f) for f in changed_files if f.strip()})
    if not normalized:
        return SelectionResult(tests=(), full_suite=False, reason="no changed files")

    seeds: list[tuple[str, bool]] = []
    selected: set[str] = set()

    for rel in normalized:
        if any(_matches(pat, rel) for pat in FULL_SUITE_PATTERNS):
            return SelectionResult(tests=tuple(all_tests), full_suite=True, reason=f"global trigger: {rel}")

        matched_resource = False
        for pattern, targets in RESOURCE_RULES:
            if _matches(pattern, rel):
                matched_resource = True
                for target in targets:
                    if target in all_test_set:
                        selected.add(target)
                    else:
                        seeds.append((target, target.startswith(ISOLATED_ADAPTER_PREFIXES)))
        if matched_resource:
            continue

        if rel.startswith("src/agent_runtime/") and rel.endswith(".py"):
            if "tests/test_architecture_boundaries.py" in all_test_set:
                selected.add("tests/test_architecture_boundaries.py")
        if rel.startswith("src/switchboard/") and rel.endswith(".py"):
            if "tests/test_switchboard_core.py" in all_test_set:
                selected.add("tests/test_switchboard_core.py")

        exists = (root / rel).exists()
        if not exists:
            if _is_test_file(rel):
                continue
            if any(_matches(pat, rel) for pat in NO_TEST_PATTERNS):
                continue
            return SelectionResult(tests=tuple(all_tests), full_suite=True, reason=f"deleted file: {rel}")

        if rel in all_test_set:
            selected.add(rel)
            seeds.append((rel, False))
            continue

        if rel.endswith(".py") or rel in reverse:
            seeds.append((rel, rel.startswith(ISOLATED_ADAPTER_PREFIXES)))
            continue

        if any(_matches(pat, rel) for pat in NO_TEST_PATTERNS):
            continue

        return SelectionResult(tests=tuple(all_tests), full_suite=True, reason=f"unmapped file: {rel}")

    for seed, isolated in seeds:
        visited = {seed}
        queue: deque[str] = deque([seed])
        while queue:
            current = queue.popleft()
            if current in all_test_set:
                selected.add(current)
                # Only propagate across test->test helper imports if the test file itself (or a non-test helper) changed.
                if current != seed and not seed.startswith("tests/"):
                    continue
            for dependent in reverse.get(current, ()):
                if isolated and dependent in COMPOSITION_ROOTS:
                    continue
                if dependent not in visited:
                    visited.add(dependent)
                    queue.append(dependent)

    ordered = tuple(sorted(selected))
    reason = f"{len(ordered)}/{len(all_tests)} tests affected by {len(normalized)} changed file(s)"
    return SelectionResult(tests=ordered, full_suite=False, reason=reason)


def git_changed_files(base: str | None, *, include_worktree: bool, root: Path = ROOT) -> list[str]:
    files: set[str] = set()
    if base:
        out = subprocess.run(
            ["git", "diff", "--name-only", f"{base}...HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        files.update(line.strip() for line in out.splitlines() if line.strip())
    if include_worktree or not base:
        for cmd in (
            ["git", "diff", "--name-only", "HEAD"],
            ["git", "ls-files", "--others", "--exclude-standard"],
        ):
            out = subprocess.run(cmd, cwd=root, check=True, capture_output=True, text=True).stdout
            files.update(line.strip() for line in out.splitlines() if line.strip())
    return sorted(files)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Select pytest files affected by changed files.")
    parser.add_argument("--base", default=None, help="Git base ref for three-dot diff against HEAD")
    parser.add_argument("--include-worktree", action="store_true", help="Also include uncommitted/untracked files")
    parser.add_argument("--files", nargs="*", default=None, help="Explicit changed repo-relative paths")
    parser.add_argument("--explain", action="store_true", help="Print selection summary to stderr")
    args = parser.parse_args(argv)

    changed = (
        list(args.files)
        if args.files is not None
        else git_changed_files(args.base, include_worktree=args.include_worktree)
    )
    result = select_tests(changed)
    if args.explain:
        mode = "FULL" if result.full_suite else "AFFECTED"
        print(f"[select_tests] {mode}: {result.reason}", file=sys.stderr)
    for test_path in result.tests:
        print(test_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
