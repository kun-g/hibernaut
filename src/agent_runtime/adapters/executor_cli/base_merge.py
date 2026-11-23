from __future__ import annotations

import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from agent_runtime.adapters.executor_cli.reviewer import ReviewAborted, git_output, git_paths

@dataclass(frozen=True, slots=True)
class SinceReview:
    # What the reviewed work looks like with the base branch merged into it and nothing else done. The difference
    # from here to the current tree is the task's own change; a conflicting file holds git's conflict blocks.
    tree: str
    # The newest base commit HEAD contains, "" when the base is unknown.
    base: str = ""


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
    return SinceReview(proc.stdout.split(b"\0", 1)[0].decode(), base=tip)


def newest_base(workdir: Path, current: str, refs: Sequence[str]) -> str:
    # The newest commit of any of the refs that the current commit contains, "" when none can be compared.
    newest = ""
    for ref in refs:
        found = _run(workdir, "merge-base", current.split(":", 1)[0], ref)
        tip = found.stdout.decode().strip()
        if found.returncode == 0 and (
            not newest or _run(workdir, "merge-base", "--is-ancestor", newest, tip).returncode == 0
        ):
            newest = tip
    return newest


def with_upstream(workdir: Path, ref: str) -> list[str]:
    # A local branch can be behind the remote one it follows: its upstream, else the branch of that name on origin.
    full = _full_name(workdir, ref)
    if not full.startswith("refs/heads/") or full == _full_name(workdir, "HEAD"):
        return [full or ref]
    branch = full.removeprefix("refs/heads/")
    return [full, _full_name(workdir, f"{branch}@{{upstream}}") or f"refs/remotes/origin/{branch}"]


def _full_name(workdir: Path, ref: str) -> str:
    return _run(workdir, "rev-parse", "--symbolic-full-name", "--verify", "--quiet", ref).stdout.decode().strip()


def changed_paths(workdir: Path, before: str, after: str) -> list[str]:
    # Both names of a renamed file count as changed.
    return git_paths(workdir, "diff", "--name-only", "--no-relative", "--no-renames", "-z", before, after)


def regular_file(workdir: Path, tree: str, path: str) -> tuple[str, bytes] | None:
    # Mode and content; None when the tree has no regular file there.
    listed = _run(workdir, "ls-tree", "--full-tree", "-z", tree, "--", path)
    entry = listed.stdout.split(b"\0")[0].split(b"\t")[0].split()
    if listed.returncode != 0 or len(entry) != 3 or not entry[0].startswith(b"100"):
        return None
    blob = _run(workdir, "cat-file", "blob", entry[2].decode())
    return (entry[0].decode(), blob.stdout) if blob.returncode == 0 else None


def _run(workdir: Path, *args: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(["git", "--literal-pathspecs", *args], cwd=str(workdir), capture_output=True, check=False)


def _error(proc: subprocess.CompletedProcess[bytes]) -> str:
    return proc.stderr.decode("utf-8", "replace").strip()[:500]
