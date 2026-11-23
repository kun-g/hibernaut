from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

from agent_runtime.adapters.executor_cli.reviewer import ReviewAborted, git_output, git_paths

# Words (each CJK character on its own) and operators; brackets, quotes and separators only join what is there.
_TOKEN = re.compile(r"[^\W㐀-鿿]+|[㐀-鿿]|[-+*/%=<>!&|^~@?\\$#]")


@dataclass(frozen=True, slots=True)
class SinceReview:
    # What the reviewed work looks like with the base branch merged into it and nothing else done. The difference
    # from here to the current tree is the task's own change; a conflicting file holds git's conflict blocks.
    tree: str
    # The newest base commit HEAD contains, "" when the base is unknown.
    base: str = ""
    # Set when that commit was merged after the review: the commits the conflict blocks are labelled with.
    reviewed: str = ""
    merged: str = ""
    conflicts: frozenset[str] = frozenset()


def without_base(workdir: Path, reviewed: str, current: str, base: str) -> SinceReview:
    # Fingerprints are "<head>:<tree>" (worktree_fingerprint).
    head, tree = reviewed.split(":", 1)
    found = _run(workdir, "merge-base", current.split(":", 1)[0], base)
    if found.returncode != 0:
        return SinceReview(tree)
    tip = found.stdout.decode().strip()
    contained = _run(workdir, "merge-base", "--is-ancestor", tip, head)
    if contained.returncode == 0:
        return SinceReview(tree, base=tip)
    if contained.returncode != 1:
        raise ReviewAborted(f"cannot compare the reviewed commit {head[:12]} with {base}: {_error(contained)}")
    if git_output(workdir, "rev-parse", f"{head}^{{tree}}").strip() != tree:
        # The review saw uncommitted work; it was reviewed all the same.
        head = git_output(workdir, "-c", "user.name=hibernaut", "-c", "user.email=hibernaut@localhost", "commit-tree",
                          tree, "-p", head, "-m", "reviewed worktree").strip()
    proc = _run(workdir, "-c", "merge.conflictStyle=merge", "merge-tree", "--write-tree", "-z", "--name-only",
                "--no-messages", head, tip)
    if proc.returncode not in (0, 1):
        raise ReviewAborted(f"git merge-tree of {head[:12]} and {tip[:12]} failed in {workdir}: {_error(proc)}")
    merged, *conflicts = [p.decode("utf-8", "surrogateescape") for p in proc.stdout.split(b"\0") if p]
    return SinceReview(merged, base=tip, reviewed=head, merged=tip, conflicts=frozenset(conflicts))


def changed_paths(workdir: Path, before: str, after: str) -> list[str]:
    # Both names of a renamed file count as changed.
    return git_paths(workdir, "diff", "--name-only", "--no-relative", "--no-renames", "-z", before, after)


def only_conflicts_resolved(workdir: Path, since: SinceReview, current: str) -> bool:
    # True when all the work since the review is a merge of the base whose conflict blocks were replaced by code made
    # of the two sides' own words and operators. Anything that cannot be shown to be that is reviewed.
    if not since.merged:
        return False
    tree = current.split(":", 1)[1]
    if git_output(workdir, "rev-parse", f"{current.split(':', 1)[0]}^{{tree}}").strip() != tree:
        return False
    changed = set(changed_paths(workdir, since.tree, tree))
    if not changed <= since.conflicts:
        return False
    for path in since.conflicts:
        before, after = _file(workdir, since.tree, path), _file(workdir, tree, path)
        blocks = _blocks(before[1], since) if before is not None else None
        if path not in changed:
            if blocks is not None:
                return False  # committed with its conflict markers
            continue
        if before is None or after is None or before[0] != after[0] or blocks is None:
            return False
        fixed, sides = blocks
        resolved = _between(fixed, after[1].splitlines(keepends=True))
        if resolved is None or not _tokens(resolved) <= _tokens(sides):
            return False
    return True


def _blocks(blob: bytes, since: SinceReview) -> tuple[list[list[bytes]], list[bytes]] | None:
    # The lines around git's conflict blocks, and the lines inside them.
    start, end = since.reviewed.encode(), since.merged.encode()
    fixed: list[list[bytes]] = [[]]
    sides: list[bytes] = []
    inside = False
    for line in blob.splitlines(keepends=True):
        if not inside and line.startswith(b"<<<<<<< ") and start in line:
            inside = True
        elif inside and line.startswith(b">>>>>>> ") and end in line:
            inside = False
            fixed.append([])
        elif not inside:
            fixed[-1].append(line)
        elif line.rstrip() != b"=======":
            sides.append(line)
    return None if inside or len(fixed) == 1 else (fixed, sides)


def _between(fixed: list[list[bytes]], lines: list[bytes]) -> list[bytes] | None:
    # What replaced the blocks, or None when a line outside them changed.
    first, *middle, last = fixed
    pos, stop = len(first), len(lines) - len(last)
    if stop < pos or lines[:pos] != first or lines[stop:] != last:
        return None
    found: list[bytes] = []
    for part in middle:
        at = next((i for i in range(pos, stop - len(part) + 1) if lines[i:i + len(part)] == part), None)
        if at is None:
            return None
        found += lines[pos:at]
        pos = at + len(part)
    return found + lines[pos:stop]


def _tokens(lines: list[bytes]) -> set[str]:
    return set(_TOKEN.findall(b"".join(lines).decode("utf-8", "replace")))


def _file(workdir: Path, tree: str, path: str) -> tuple[str, bytes] | None:
    listed = _run(workdir, "ls-tree", "--full-tree", "-z", tree, "--", path)
    entry = listed.stdout.split(b"\0")[0].split(b"\t")[0].split()
    if listed.returncode != 0 or len(entry) != 3 or not entry[0].startswith(b"100"):
        return None
    blob = _run(workdir, "cat-file", "blob", entry[2].decode())
    return (entry[0].decode(), blob.stdout) if blob.returncode == 0 else None


def file_text(workdir: Path, tree: str, path: str) -> str:
    found = _file(workdir, tree, path)
    return found[1].decode("utf-8", "surrogateescape") if found is not None else ""


def _run(workdir: Path, *args: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(["git", "--literal-pathspecs", *args], cwd=str(workdir), capture_output=True, check=False)


def _error(proc: subprocess.CompletedProcess[bytes]) -> str:
    return proc.stderr.decode("utf-8", "replace").strip()[:500]
