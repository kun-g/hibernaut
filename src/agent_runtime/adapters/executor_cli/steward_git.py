from __future__ import annotations

import os
import posixpath
import re
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

GIT_COMMANDS = ("status", "diff", "log", "show", "blame", "rev-parse", "merge-base")
_GIT_TIMEOUT = 30.0
_MAX_CONTENT_FILES = 500
# A signature format (%G?) would run the configured signature checker.
_GIT_CONFIG = ("core.fsmonitor=false", "log.showSignature=false", "gpg.program=false", "gpg.openpgp.program=false",
               "gpg.x509.program=false", "gpg.ssh.program=false")
_PLAIN = ("--no-ext-diff", "--no-textconv")
# Whether a submodule's files changed is asked of a git run inside it, under that repository's configuration.
_NO_SUBMODULES = "--ignore-submodules=dirty"
_READS_CHECKOUT = ("status", "diff", "blame", "grep", "ls-files")


class ToolError(Exception):
    pass


class GitRefused(ToolError):
    pass


def _options(text: str) -> frozenset[str]:
    return frozenset(text.split())


# Options are named in full: git takes any unambiguous prefix, writes files, runs configured programs, reads files
# outside the checkout, or tells by which commits it lists what a file contains (-S, -G, -L). "=" ends one with a value.
_SUMMARIES = _options("--stat --stat= --shortstat --numstat --summary --dirstat --name-only --name-status --no-patch -s")
_FORMATS = _options("--oneline --format= --pretty= --date= --abbrev= --abbrev-commit --no-abbrev-commit --decorate "
                    "--no-decorate --no-notes --no-color --diff-filter= --first-parent --no-renames")
_LOG = _SUMMARIES | _FORMATS | _options(
    "--graph --all --branches --branches= --tags --tags= --remotes --remotes= --merges --no-merges --reverse "
    "--topo-order --date-order --author-date-order --follow --left-right --cherry-pick --cherry-mark --boundary "
    "--ancestry-path --full-history -i --regexp-ignore-case -E --extended-regexp -F --fixed-strings --all-match "
    "--invert-grep --max-count= --skip= --since= --after= --until= --before= --author= --committer= --grep= "
    "--min-parents= --max-parents="
)
_NEUTRAL = _options("-w -b --ignore-all-space --ignore-space-change --ignore-blank-lines --minimal --patience --histogram")
_PATCH = _options("-p -u --patch --unified= -W --function-context --word-diff --word-diff=")
_DIFF = _SUMMARIES | _NEUTRAL | _PATCH | _options("--cached --staged --merge-base --no-color --diff-filter= --no-renames")
_ALLOWED = {
    "status": _options("-s --short -b --branch --porcelain --porcelain= --long -u -uno -unormal -uall --untracked-files "
                       "--untracked-files= --ignored --ignored= -z --no-renames --show-stash"),
    "diff": _DIFF,
    "log": _LOG,
    "show": _SUMMARIES | _FORMATS | _NEUTRAL | _PATCH,
    "blame": _options("-w -l -t -s -e -f -n -p -M -C --porcelain --line-porcelain --date= --abbrev= --root "
                      "--first-parent --since= --ignore-rev="),
    "rev-parse": _options("--verify -q --quiet --short --short= --abbrev-ref --abbrev-ref= --symbolic "
                          "--symbolic-full-name --show-toplevel --is-inside-work-tree --all --branches --tags --remotes"),
    "merge-base": _options("-a --all --is-ancestor --octopus --independent --fork-point"),
}
# Short options with the value attached, and those whose value is the next argument.
_ATTACHED = {"log": r"-n?\d+", "show": r"-U\d+", "diff": r"-U\d+", "blame": r"-L.+"}
_VALUE_NEXT = {"log": ("-n",), "blame": ("-L", "--ignore-rev")}


@dataclass(frozen=True, slots=True)
class GitCall:
    command: str
    options: tuple[str, ...]
    revisions: tuple[str, ...]
    paths: tuple[str, ...]

    @property
    def content(self) -> bool:
        # Whether the answer carries file content rather than names, counts and messages.
        if self.command not in ("diff", "show"):
            return self.command == "blame"
        names = [_name(option) for option in self.options]
        return not any(n in _SUMMARIES for n in names) or any(n in _PATCH or n.startswith("-U") for n in names)


def _name(option: str) -> str:
    return option.split("=", 1)[0] + "=" if option.startswith("--") and "=" in option else option


def outside(path: str) -> bool:
    return path.startswith(("/", "~")) or ".." in path.split("/")


def check_git_args(args: Sequence[str]) -> GitCall:
    if not args or args[0] not in GIT_COMMANDS:
        raise GitRefused(f"git: only {', '.join(GIT_COMMANDS)} are available")
    command = args[0]
    options: list[str] = []
    plain: list[str] = []
    paths: list[str] = []
    rest = iter(args[1:])
    for arg in rest:
        if arg == "--":
            paths += rest
        elif not arg.startswith("-"):
            plain.append(arg)
        elif arg in _VALUE_NEXT.get(command, ()):
            value = next(rest, "")
            if not value or value.startswith("-"):
                raise GitRefused(f"git: option {arg} needs a value")
            options += [arg, value]
        elif _name(arg) in _ALLOWED[command] or re.fullmatch(_ATTACHED.get(command, "(?!)"), arg):
            options.append(arg)
        else:
            raise GitRefused(f"git: option {arg.split('=', 1)[0]} is not available")
    for arg in (*plain, *paths):
        # Git answers <rev>:<path> with whether that path exists on disk, so the part after a colon counts as a path
        # too. A leading colon (index entry or pathspec magic) is never needed here.
        if arg.startswith(":") or any(outside(part) for part in arg.split(":")):
            raise GitRefused(f"git: {arg} is outside the checkout")
    if command == "blame" and not paths:
        paths, plain = plain[-1:], plain[:-1]
    if any(":" in arg for arg in plain) and (command != "show" or options or paths or len(plain) > 1):
        # Elsewhere it names content without a file the gate could judge.
        raise GitRefused("git: <rev>:<path> is only available as the one argument of show")
    if command == "blame" and (len(paths) != 1 or len(plain) > 1):
        raise GitRefused("git: blame takes one file, with the revision before it")
    if command == "diff" and len(plain) > 2:
        raise GitRefused("git: diff takes at most two revisions; put paths after --")
    return GitCall(command, tuple(options), tuple(plain), tuple(paths))


def _git_env() -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    # Without optional locks `git status` does not even refresh the index; a missing object is not fetched either.
    env.update(GIT_OPTIONAL_LOCKS="0", GIT_TERMINAL_PROMPT="0", GIT_NO_LAZY_FETCH="1")
    return env


def _without_filters(checkout: Path, env: dict[str, str]) -> dict[str, str]:
    # Comparing the checkout runs the configured content filters. In the environment, since a filter's name may hold "=".
    try:
        listed = subprocess.run(
            ["git", "config", "--name-only", "-z", "--get-regexp", r"^filter\."], cwd=checkout, env=env,
            stdin=subprocess.DEVNULL, capture_output=True, timeout=_GIT_TIMEOUT,
        )
    except (OSError, ValueError, subprocess.TimeoutExpired) as exc:
        raise ToolError(f"git: {exc}") from exc
    if listed.returncode not in (0, 1):
        raise ToolError(f"git config exited {listed.returncode}: {os.fsdecode(listed.stderr).strip()[-2000:]}")
    drivers = dict.fromkeys(name.rpartition(".")[0] for name in os.fsdecode(listed.stdout).split("\0") if name.count(".") > 1)
    unset = [(f"{driver}.{key}", value) for driver in drivers
             for key, value in (("clean", ""), ("smudge", ""), ("process", ""), ("required", "false"))]
    for index, (key, value) in enumerate(unset):
        env |= {f"GIT_CONFIG_KEY_{index}": key, f"GIT_CONFIG_VALUE_{index}": value}
    return {**env, "GIT_CONFIG_COUNT": str(len(unset))}


def run_git(checkout: Path, args: Sequence[str], ok: Sequence[int] = (0,)) -> bytes:
    env = _git_env()
    if args[0] in _READS_CHECKOUT:
        env = _without_filters(checkout, env)
    try:
        done = subprocess.run(
            ["git", "--no-pager", *(part for item in _GIT_CONFIG for part in ("-c", item)), *args],
            cwd=checkout, env=env, stdin=subprocess.DEVNULL, capture_output=True, timeout=_GIT_TIMEOUT,
        )
    except (OSError, ValueError, subprocess.TimeoutExpired) as exc:
        raise ToolError(f"git: {exc}") from exc
    if done.returncode not in ok:
        said = done.stderr.decode("utf-8", errors="replace").strip()[-2000:]
        raise ToolError(f"git {args[0]} exited {done.returncode}: {said}")
    return done.stdout


def exec_git(checkout: Path, args: Sequence[str], ok: Sequence[int] = (0,)) -> str:
    # For reading, not for names: bytes that are no text are replaced.
    return run_git(checkout, args, ok).decode("utf-8", errors="replace")


def file_name(raw: bytes) -> str | None:
    # None: a name that is no text can neither be judged by the gate nor named by the Steward.
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return None


def git_names(checkout: Path, args: Sequence[str], *, skip: bool = False) -> list[str]:
    # A -z listing.
    names = [file_name(raw) for raw in run_git(checkout, args).split(b"\0") if raw]
    if None in names and not skip:
        raise ToolError(f"git {args[0]}: a file name here is not valid UTF-8; narrow it with a path")
    return list(dict.fromkeys(name for name in names if name is not None))


def _commit(checkout: Path, revision: str) -> str:
    # Git is only ever given the commit's own id, so it cannot read a revision in another way than it was judged:
    # neither as a file, nor as a blob, nor as a range.
    found = exec_git(checkout, ["rev-parse", "--verify", "--quiet", f"{revision}^{{commit}}"], ok=(0, 1, 128)).strip()
    if not re.fullmatch(r"[0-9a-f]{40,64}", found):
        raise ToolError(f"git: {revision} is not a commit (paths go after --)")
    return found


def _exact(path: str) -> str:
    if path != posixpath.normpath(path) or path == ".":
        raise ToolError(f"git: name the file as it is listed, not {path}")
    return path


def _blob(checkout: Path, commit: str, path: str) -> str:
    # Only a regular file: git shows a submodule's commit or a directory under a path just as well.
    entries = exec_git(checkout, ["ls-tree", "-z", "--full-tree", commit, "--", path]).split("\0")
    for entry in entries:
        fields, _, name = entry.partition("\t")
        mode, kind, found = (*fields.split(" "), "", "", "")[:3]
        if name == path and kind == "blob" and mode.startswith("100"):
            return found
    raise ToolError(f"git: {path} is not a file in that commit")


def _diff_revision(checkout: Path, revision: str) -> str:
    left, dots, right = revision.partition("...") if "..." in revision else revision.partition("..")
    if not dots:
        return _commit(checkout, revision)
    return dots.join(_commit(checkout, side or "HEAD") for side in (left, right))


def plan_git(checkout: Path, call: GitCall) -> tuple[list[str], list[str]]:
    # (files whose content the answer shows, the git arguments that show it; none when there is nothing to show)
    command, options, revisions = call.command, call.options, call.revisions
    if command == "show" and revisions and ":" in revisions[0]:
        revision, path = revisions[0].split(":", 1)
        return [_exact(path)], ["cat-file", "blob", _blob(checkout, _commit(checkout, revision), path)]
    if command == "blame":
        [path] = call.paths
        return [_exact(path)], ["blame", "--no-textconv", *options, *(_commit(checkout, r) for r in revisions), "--", path]
    if command == "show":
        revisions = tuple(_commit(checkout, r) for r in revisions or ("HEAD",))
    elif command == "diff":
        revisions = tuple(_diff_revision(checkout, r) for r in revisions)
    if not call.content:
        plain = _PLAIN if command in ("diff", "log", "show") else ()
        if command in ("status", "diff"):
            plain = (*plain, _NO_SUBMODULES)
        tail = ("--", *call.paths) if call.paths or command in ("diff", "show") else ()
        return [], [command, *plain, *options, *revisions, *tail]
    # A patch: first which files it touches, without renames, so every line of it belongs to one of them.
    if command == "show":
        # -m: every file a merge differs in from any parent, whatever of it git would show.
        listings = [["diff-tree", "-r", "-m", "--root", "--no-commit-id", "--name-only", "-z", "--no-renames", commit]
                    for commit in revisions]
    else:
        compared = [o for o in options if o in ("--cached", "--staged", "--merge-base")]
        listings = [["diff", *_PLAIN, _NO_SUBMODULES, "--name-only", "-z", "--no-renames", "--no-relative", *compared, *revisions]]
    files = list(dict.fromkeys(
        name for listing in listings for name in git_names(checkout, [*listing, "--", *call.paths])
    ))
    if len(files) > _MAX_CONTENT_FILES:
        raise ToolError(f"git: {len(files)} files; narrow it with paths after --, or use --stat")
    if not files:
        shown = [o for o in options if _name(o) in _FORMATS]
        return [], ["show", *_PLAIN, "--no-patch", *shown, *revisions] if command == "show" else []
    # Only the files listed here, whatever changes in the checkout meanwhile.
    return files, [
        command, *_PLAIN, *options, _NO_SUBMODULES, "--no-renames", "--submodule=short", "--no-relative", *revisions, "--",
        *(f":(top,literal){name}" for name in files),
    ]
