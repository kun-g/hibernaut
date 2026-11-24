# What a read-only agent node lets its CLI do, whatever Hibernaut's policy says: read files of the run's workspace.
# The CLI runs as the node's own user, so a path outside the workspace may be the relay token or another login.
from __future__ import annotations

from pathlib import Path
from typing import Any, Final, Mapping

# Claude Code's read tools and the arguments they are known to take; another argument could name another file.
_TOOL_ARGS: Final[Mapping[str, frozenset[str]]] = {
    "Read": frozenset({"file_path", "offset", "limit", "pages", "allow_large"}),
    "Grep": frozenset(
        {"pattern", "path", "glob", "type", "output_mode", "-A", "-B", "-C", "-i", "-n", "-o", "context", "head_limit",
         "offset", "multiline"}
    ),
    "Glob": frozenset({"pattern", "path"}),
}
_PATHS: Final[Mapping[str, str]] = {"Read": "file_path", "Grep": "path", "Glob": "path"}
_GLOBS: Final[Mapping[str, str]] = {"Grep": "glob", "Glob": "pattern"}

WORKSPACE_READ_TOOLS: Final = tuple(_TOOL_ARGS)


def workspace_read_refusal(workdir: Path, tool_name: str, args: Mapping[str, Any]) -> str | None:
    known = _TOOL_ARGS.get(tool_name)
    if known is None:
        return f"this node only runs {', '.join(WORKSPACE_READ_TOOLS)}"
    if extra := sorted(set(args) - known):
        return f"this node does not know the {tool_name} arguments {', '.join(extra)}"
    path_key = _PATHS[tool_name]
    if (path_key in args or tool_name == "Read") and not _inside(workdir, args.get(path_key)):
        return f"{path_key} must be a path inside the workspace"
    glob_key = _GLOBS.get(tool_name)
    if glob_key is not None and glob_key in args and not _relative_glob(args[glob_key]):
        return f"{glob_key} must be a pattern inside the workspace"
    return None


def _plain(value: object) -> str | None:
    # The CLI trims a path before it opens it, so it is judged as written or not at all.
    if not isinstance(value, str) or not value or value != value.strip() or not value.isprintable():
        return None
    return value


def _inside(workdir: Path, value: object) -> bool:
    path = _plain(value)
    if path is None or path.startswith("~"):
        return False
    root = workdir.resolve()
    try:
        resolved = (root / path).resolve()
    except (OSError, RuntimeError, ValueError):
        return False
    # resolve() leaves a symlink loop as it is.
    return not resolved.is_symlink() and resolved.is_relative_to(root)


def _relative_glob(value: object) -> bool:
    pattern = _plain(value)
    return pattern is not None and not pattern.startswith(("/", "~")) and ".." not in pattern
