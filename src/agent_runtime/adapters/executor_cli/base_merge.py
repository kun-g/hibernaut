from __future__ import annotations

import re
import subprocess
from bisect import bisect_left
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from agent_runtime.adapters.executor_cli.reviewer import ReviewAborted, git_output, git_paths

# Words (each CJK character on its own), runs of spaces, and every other character by itself.
_TOKEN = re.compile(r"[^\W㐀-鿿]+|\s+|.", re.DOTALL)
# A larger block is not judged (the comparison is quadratic) and goes to review.
_MAX_BLOCK_STEPS = 1_000_000


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
    merged, *conflicts = _merge_tree(workdir, head, tip, "merge")
    return SinceReview(merged, base=tip, reviewed=head, merged=tip, conflicts=frozenset(conflicts))


def _merge_tree(workdir: Path, ours: str, theirs: str, style: str) -> list[str]:
    # The merged tree, then the files that conflict.
    proc = _run(workdir, "-c", f"merge.conflictStyle={style}", "merge-tree", "--write-tree", "-z", "--name-only",
                "--no-messages", ours, theirs)
    if proc.returncode not in (0, 1):
        raise ReviewAborted(f"git merge-tree of {ours[:12]} and {theirs[:12]} failed in {workdir}: {_error(proc)}")
    return [p.decode("utf-8", "surrogateescape") for p in proc.stdout.split(b"\0") if p]


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


def with_upstreams(workdir: Path, refs: Sequence[str]) -> list[str]:
    # A local branch can be behind the remote one it follows: its upstream, else the branch of that name on origin.
    current = _run(workdir, "symbolic-ref", "--quiet", "HEAD").stdout.decode().strip()
    found: list[str] = []
    for ref in refs:
        found.append(ref)
        full = _run(workdir, "rev-parse", "--symbolic-full-name", "--verify", "--quiet", ref).stdout.decode().strip()
        if full.startswith("refs/heads/") and full != current:
            branch = full.removeprefix("refs/heads/")
            upstream = _run(workdir, "rev-parse", "--symbolic-full-name", "--verify", "--quiet", f"{branch}@{{upstream}}")
            found.append(upstream.stdout.decode().strip() or f"refs/remotes/origin/{branch}")
    return found


def changed_paths(workdir: Path, before: str, after: str) -> list[str]:
    # Both names of a renamed file count as changed.
    return git_paths(workdir, "diff", "--name-only", "--no-relative", "--no-renames", "-z", before, after)


def only_conflicts_resolved(workdir: Path, since: SinceReview, current: str) -> bool:
    # True when all the work since the review is a merge of the base in which every conflict block was replaced by
    # text its own two sides explain. Anything that cannot be shown to be that is reviewed.
    if not since.merged:
        return False
    tree = current.split(":", 1)[1]
    if git_output(workdir, "rev-parse", f"{current.split(':', 1)[0]}^{{tree}}").strip() != tree:
        return False
    changed = set(changed_paths(workdir, since.tree, tree))
    if not changed <= since.conflicts:
        return False
    # The same merge with the base's text in each block, and without trimming lines the two sides share.
    full = _merge_tree(workdir, since.reviewed, since.merged, "diff3")[0] if changed else ""
    for path in since.conflicts:
        before, after = regular_file(workdir, since.tree, path), regular_file(workdir, tree, path)
        if path not in changed:
            if before is not None and (since.reviewed.encode() in before[1] or since.merged.encode() in before[1]):
                return False  # committed with its conflict markers
            continue
        whole = regular_file(workdir, full, path)
        if before is None or whole is None or after is None or before[0] != after[0]:
            return False
        if not _explained(before[1], whole[1], after[1], since):
            return False
    return True


def _explained(merged: bytes, merged_with_base: bytes, after: bytes, since: SinceReview) -> bool:
    lines = after.splitlines(keepends=True)
    narrow, full = _blocks(merged, since), _blocks(merged_with_base, since)
    if narrow is None or full is None or _between(narrow[0], lines) is None:
        return False
    found = _between(full[0], lines)
    if found is None:
        return False
    for block, resolved in zip(full[1], found, strict=True):
        sides = _sides(block)
        if sides is None or not _combines(resolved, *sides):
            return False
    return True


def _blocks(blob: bytes, since: SinceReview) -> tuple[list[list[bytes]], list[list[bytes]]] | None:
    # The lines around git's conflict blocks, and the lines inside each block.
    start, end = since.reviewed.encode(), since.merged.encode()
    fixed: list[list[bytes]] = [[]]
    blocks: list[list[bytes]] = []
    inside = False
    for line in blob.splitlines(keepends=True):
        if not inside and line.startswith(b"<<<<<<< ") and start in line:
            inside = True
            blocks.append([])
        elif inside and line.startswith(b">>>>>>> ") and end in line:
            inside = False
            fixed.append([])
        else:
            (blocks if inside else fixed)[-1].append(line)
    return None if inside or not blocks else (fixed, blocks)


def _sides(block: list[bytes]) -> tuple[list[bytes], list[bytes], list[bytes]] | None:
    # Ours, the common base and theirs; None when the content itself has lines that look like the separators.
    bars = [i for i, line in enumerate(block) if re.match(rb"\|{7}(?!\|)", line)]
    equals = [i for i, line in enumerate(block) if line.rstrip() == b"======="]
    if len(bars) != 1 or len(equals) != 1 or bars[0] > equals[0]:
        return None
    return block[:bars[0]], block[bars[0] + 1:equals[0]], block[equals[0] + 1:]


def _between(fixed: list[list[bytes]], lines: list[bytes]) -> list[list[bytes]] | None:
    # What replaced each block, or None when a line outside them changed.
    first, *middle, last = fixed
    pos, stop = len(first), len(lines) - len(last)
    if stop < pos or lines[:pos] != first or lines[stop:] != last:
        return None
    found: list[list[bytes]] = []
    for part in middle:
        at = next((i for i in range(pos, stop - len(part) + 1) if lines[i:i + len(part)] == part), None)
        if at is None:
            return None
        found.append(lines[pos:at])
        pos = at + len(part)
    return [*found, lines[pos:stop]]


def _combines(resolved: list[bytes], ours: list[bytes], base: list[bytes], theirs: list[bytes]) -> bool:
    # One side as it is, or both sides' text in their own order: nothing more often than both together have it, all
    # that both have, and not merely a piece of one side. Blank lines and trailing spaces do not count.
    done, left, common, right = (_tokens(lines) for lines in (resolved, ours, base, theirs))
    if done in (left, right):
        return True
    if not _within(Counter(done), Counter(left), Counter(common), Counter(right), least=True):
        return False
    if not _within(_rows(resolved), _rows(ours), _rows(base), _rows(theirs), least=False):
        return False
    if len(done) * (len(left) + len(right) + 1) > _MAX_BLOCK_STEPS:
        return False
    return not _woven(done, left, []) and not _woven(done, [], right) and _woven(done, left, right)


def _within(
    done: Counter[str], left: Counter[str], common: Counter[str], right: Counter[str], *, least: bool
) -> bool:
    for item in done.keys() | left.keys() | right.keys():
        a, b = left[item], right[item]
        # A line combined from both sides is in neither of them, so it may appear once.
        most = max(a, b, a + b - common[item], 0 if least else 1)
        if done[item] > most or (least and done[item] < min(a, b)):
            return False
    return True


def _woven(done: list[str], left: list[str], right: list[str]) -> bool:
    # Whether `done` is made of some of `left` and some of `right`, each in its own order.
    places: tuple[dict[str, list[int]], dict[str, list[int]]] = ({}, {})
    for at, tokens in zip(places, (left, right), strict=True):
        for i, token in enumerate(tokens):
            at.setdefault(token, []).append(i)

    def after(side: int, token: str, pos: int) -> int | None:
        found = places[side].get(token, [])
        k = bisect_left(found, pos)
        return found[k] + 1 if k < len(found) else None

    # How far each side is used up; of two ways that use as much of one side, the one using less of the other.
    states = {0: 0}
    for token in done:
        reached: dict[int, int] = {}
        for i, j in states.items():
            for step in ((after(0, token, i), j), (i, after(1, token, j))):
                if step[0] is not None and step[1] is not None:
                    reached[step[0]] = min(reached.get(step[0], step[1]), step[1])
        states, best = {}, len(right) + 1
        for i in sorted(reached):
            if reached[i] < best:
                best = states[i] = reached[i]
        if not states:
            return False
    return True


def _rows(lines: list[bytes]) -> Counter[str]:
    return Counter(line.strip().decode("utf-8", "surrogateescape") for line in lines if line.strip())


def _tokens(lines: list[bytes]) -> list[str]:
    # A line starts with its indentation, so moving code into or out of a block shows.
    found: list[str] = []
    for line in lines:
        text = line.decode("utf-8", "surrogateescape").rstrip()
        if text:
            body = text.lstrip()
            found += ["\n" + text[:len(text) - len(body)], *_TOKEN.findall(body)]
    return found


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
