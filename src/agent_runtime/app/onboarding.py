from __future__ import annotations

import contextlib
import os
import re
import shutil
import signal
import subprocess
import tomllib
from collections import Counter
from collections.abc import Callable, Generator, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from agent_runtime.adapters.executor_cli.steward import STEWARD_ROLE
from agent_runtime.app.config import HibernautConfig, ProjectConfig, parse_config
from agent_runtime.app.config_edit import ConfigChanged, ConfigFile, EditRefused, TomlEdit, toml_key, toml_value
from agent_runtime.app.projects import SYSTEM_INBOX_PREFIX, WorkdirBinding

DEFAULT_LABEL = "hibernaut"
_REPO = re.compile(r"(?P<owner>[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)/(?P<name>[A-Za-z0-9_][A-Za-z0-9._-]{0,99})")
# The Switchboard owner route maps a repository to proj:<lower-cased name>, so the inbox must match it.
_NAME = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")
_MAX_DESCRIPTION = 200
_COMMAND_TIMEOUT_SECONDS = 60.0
_CLONE_TIMEOUT_SECONDS = 900.0
_MAX_OUTPUT = 500
_REMOTE_LOGIN = re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://)[^\s/@]+@")
_TOKEN = re.compile(r"\b(?:gh[a-z]_[A-Za-z0-9]{8,}|github_pat_[A-Za-z0-9_]{8,}|sk-[A-Za-z0-9_-]{8,})")
_SECRET_NAME = re.compile(r"(?i)(?:token|secret|password|passwd|api_?key|credential|_key)")
_MIN_SECRET = 6

Runner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]
# (file, what it held, what it is to hold) -> the backup. Raises when the new version is not in force afterwards.
Writer = Callable[[ConfigFile, bytes, bytes], Path]


class OnboardError(Exception):
    def __init__(self, reason: str, *, config_kept: bool = True) -> None:
        super().__init__(reason)
        # Whether the configuration is what it was before the call.
        self.config_kept = config_kept


def run_command(
    argv: Sequence[str], env: Mapping[str, str] | None = None, *, timeout: float | None = None, cwd: str | None = None,
) -> subprocess.CompletedProcess[str]:
    # A fixed argument list, never a shell; what it started ends with it.
    args = list(argv)
    if timeout is None:
        timeout = _CLONE_TIMEOUT_SECONDS if "clone" in args[:3] else _COMMAND_TIMEOUT_SECONDS
    try:
        proc = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                errors="replace", env=dict(env) if env is not None else None, start_new_session=True,
                                cwd=cwd)
    except OSError as exc:
        return subprocess.CompletedProcess(args, 127, "", f"{args[0]}: {exc.strerror}")
    try:
        out, err = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.communicate()
        return subprocess.CompletedProcess(args, 124, "", f"{args[0]} did not finish within {timeout:g} seconds")
    return subprocess.CompletedProcess(args, proc.returncode, out, err)


def _secret_values(raw: Mapping[str, Any]) -> set[str]:
    found: set[str] = set()

    def walk(node: object, secret: bool) -> None:
        if isinstance(node, dict):
            for key, value in cast(dict[str, object], node).items():
                # Keys such as token_env and token_file name where a secret is, not the secret.
                named = bool(_SECRET_NAME.search(key)) and not key.endswith(("_env", "_file"))
                walk(value, secret or key == "env" or named)
        elif isinstance(node, list):
            for item in cast(list[object], node):
                walk(item, secret)
        elif secret and isinstance(node, str):
            found.add(node)

    walk(raw, False)
    return found


def scrub(text: str, raw: Mapping[str, Any] | None = None) -> str:
    # Text of commands and parsers, before it reaches an agent, an answer or a log.
    secrets = {value for name, value in os.environ.items() if _SECRET_NAME.search(name)}
    if raw is not None:
        secrets |= _secret_values(raw)
    for value in sorted((s for s in secrets if len(s) >= _MIN_SECRET), key=len, reverse=True):
        text = text.replace(value, "***")
    return _TOKEN.sub("***", _REMOTE_LOGIN.sub(r"\1***@", text))


def split_repo(repo: str) -> tuple[str, str]:
    match = _REPO.fullmatch(repo)
    if match is None or ".." in repo or repo.endswith((".git", ".lock", ".")):
        raise OnboardError(f"repo must be owner/name on GitHub, got {repo!r}")
    return match["owner"], match["name"]


def project_name(name: str | None, repo: str) -> str:
    found = name or split_repo(repo)[1].lower()
    if _NAME.fullmatch(found) is None:
        raise OnboardError(f"project name {found!r} must be lower-case letters, digits, '-' or '_'; name one")
    return found


def description_text(description: str | None, repo: str) -> str:
    text = (description or "").strip() or f"GitHub 仓库 {repo}"
    if not text.isprintable():
        raise OnboardError("description must be one line without control characters")
    if len(text) > _MAX_DESCRIPTION:
        raise OnboardError(f"description must be at most {_MAX_DESCRIPTION} characters")
    return text


@dataclass(frozen=True, slots=True)
class Request:
    repo: str
    name: str
    checkout: Path
    description: str = ""
    label: str = DEFAULT_LABEL
    auto_merge: bool = False


@dataclass(slots=True)
class Report:
    done: list[str] = field(default_factory=list[str])
    by_hand: list[str] = field(default_factory=list[str])
    # The lines the configuration gained, and the copy of what it was.
    added: str = ""
    backup: Path | None = None
    config: Path | None = None


@dataclass(frozen=True, slots=True)
class ProjectAgents:
    steward: str
    sde: str
    reviewer: str | None

    @property
    def workers(self) -> tuple[str, ...]:
        return (self.sde, *((self.reviewer,) if self.reviewer else ()))


def _main_agent(config: HibernautConfig, project: ProjectConfig) -> str:
    bound = project.route(project.inbox_id).binding
    return bound.default_agent if bound is not None else project.default_agent or config.runtime.default_agent


def _is_steward(config: HibernautConfig, agent: str) -> bool:
    return agent in config.agents and config.agents[agent].role == STEWARD_ROLE


def _most_used(candidates: Sequence[str], used: Iterable[str], usual: str) -> str | None:
    if not candidates:
        return None
    counts = Counter(agent for agent in used if agent in candidates)
    if counts:
        return max(candidates, key=lambda agent: counts[agent])
    return usual if usual in candidates else candidates[0]


def project_agents(config: HibernautConfig) -> ProjectAgents:
    # Chosen from the configuration alone: what the other projects use, else the first that fits.
    # A Steward of an inbox outside the projects is one of theirs only where a project has it already.
    projects = list(config.projects.values())
    in_front = [_main_agent(config, p) for p in projects]
    elsewhere = {agent for b in config.runtime.inbox_bindings for agent in (*b.binding.agents, b.binding.default_agent)
                 if b.person_only or agent not in in_front}
    stewards = [name for name, role in config.roles.items()
                if role.role == STEWARD_ROLE and not role.workdir and name not in elsewhere]
    steward = _most_used(stewards, in_front, STEWARD_ROLE)
    if steward is None:
        raise OnboardError("no agent with role steward is free for a project: declare one in [agents] without a workdir "
                           "that no [[runtime.inbox_bindings]] uses")
    developers = [name for name, role in config.roles.items() if role.sde is not None]
    sde = _most_used(developers, (
        agent for p in projects
        for agent in (p.default_agent, *(b.binding.default_agent for b in p.inbox_bindings)) if agent
    ), "sde")
    if sde is None:
        raise OnboardError("no agent with role sde is declared in [agents]")
    options = config.roles[sde].sde
    candidates = options.candidates if options is not None and options.review else ()
    reviewer = next((name for name in candidates if name in config.agents), None)
    return ProjectAgents(steward, sde, reviewer)


def general_steward(config: HibernautConfig) -> tuple[int, WorkdirBinding] | None:
    # The one inbox whose Steward sends work to projects: the one that may reach a system inbox, where there is one.
    bindings = list(enumerate(config.runtime.inbox_bindings))
    systems = {b.rule.literal for _, b in bindings if b.system_tools}

    def eligible(bound: WorkdirBinding) -> bool:
        if bound.system_tools or (bound.rule.literal or bound.rule.text).startswith(SYSTEM_INBOX_PREFIX):
            return False
        return bool(bound.forward_to) and _is_steward(config, bound.binding.default_agent)

    found = [(n, b) for n, b in bindings if eligible(b)]
    if systems:
        found = [(n, b) for n, b in found if systems & set(b.forward_to)]
    return found[0] if len(found) == 1 else None


NO_GENERAL_STEWARD = ("forward_to: there is not exactly one general Steward (a readonly [[runtime.inbox_bindings]] of a "
                      "steward with forward_to); add the project's main inbox where it belongs")


@dataclass(slots=True)
class Gaps:
    steward: bool = False
    description: bool = False
    forward_to: bool = False
    # Why a gap is left to a person.
    by_hand: list[str] = field(default_factory=list[str])

    @property
    def missing(self) -> list[str]:
        return [key for key in ("steward", "description", "forward_to") if getattr(self, key)]


def project_gaps(config: HibernautConfig, project: ProjectConfig) -> Gaps:
    gaps = Gaps(steward=not _is_steward(config, _main_agent(config, project)), description=not project.description)
    bound = project.route(project.inbox_id).binding
    if gaps.steward and bound is not None:
        gaps.by_hand.append(f"steward: the binding of {project.inbox_id} names {bound.default_agent} as its "
                            "default_agent, which is not changed")
    general = general_steward(config)
    if general is None:
        gaps.by_hand.append(NO_GENERAL_STEWARD)
    else:
        gaps.forward_to = project.inbox_id not in general[1].forward_to
    return gaps


def _add_forward(edit: TomlEdit, config: HibernautConfig, inbox: str, report: Report) -> None:
    general = general_steward(config)
    if general is None:
        report.by_hand.append(NO_GENERAL_STEWARD)
        return
    index, bound = general
    if inbox in bound.forward_to:
        return
    try:
        edit.append_item(("runtime", "inbox_bindings"), index, "forward_to", inbox)
    except EditRefused as exc:
        report.by_hand.append(f"forward_to: add {inbox} to the binding of {bound.rule.text} ({exc})")
        return
    report.done.append(f"forward_to: {bound.rule.text} may send work to {inbox}")


def project_block(request: Request, agents: ProjectAgents | None) -> str:
    name, key = request.name, toml_key(request.name)
    values: dict[str, object] = {
        "description": request.description, "inbox_id": [f"proj:{name}", f"proj:{name}:.+"], "repo": request.repo,
        "checkout": str(request.checkout), "worktree_backend": "orca",
    }
    values["executor" if agents is None else "default_agent"] = "sde" if agents is None else agents.sde
    lines = [f"\n[projects.{key}]",
             f'# Onboarded by Hibernaut: issues labelled "{request.label}" arrive through the Switchboard webhook.']
    lines += [f"{k} = {toml_value(v)}" for k, v in values.items() if v]
    if agents is not None:
        # The Steward looks first; whoever names an agent still gets it.
        for pattern, default, allowed in (
            (f"proj:{name}", agents.steward, (agents.steward, *agents.workers)),
            (f"proj:{name}:.+", agents.sde, agents.workers),
        ):
            lines += [f"\n[[projects.{key}.inbox_bindings]]", f"pattern = {toml_value(pattern)}",
                      f"default_agent = {toml_value(default)}", f"agents = {toml_value(allowed)}"]
    lines += [f"\n[projects.{key}.delivery]", "open_pr = true", f"auto_merge = {toml_value(request.auto_merge)}"]
    return "\n".join(lines) + "\n"


def existing_project(config: HibernautConfig, request: Request) -> ProjectConfig | None:
    for project in config.projects.values():
        if project.repo.lower() == request.repo.lower():
            return project
    taken = config.projects.get(request.name)
    if taken is not None:
        raise OnboardError(f"the name {request.name} is taken by {taken.repo}; give {request.repo} another name")
    return None


def _parsed(text: str, base_dir: Path) -> HibernautConfig:
    raw: dict[str, Any] = {}
    try:
        raw = tomllib.loads(text)
        return parse_config(raw, base_dir=base_dir)
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError) as exc:
        raise OnboardError(f"the configuration is not valid: {scrub(str(exc), raw)}") from None


class Onboarding:
    # Setting projects up, for the command line and the system tools alike. The configuration is written last, so a
    # step that fails leaves it as it was.
    def __init__(
        self,
        config: Path,
        *,
        run: Runner,
        write: Writer,
        # Owners in whose repositories the label may be created; None: in any, an operator asked for it.
        label_owners: Callable[[HibernautConfig], tuple[str, ...] | None],
        # Whether a project may be added without a Steward, where no agent fits.
        steward_optional: bool = False,
        dry_run: bool = False,
    ) -> None:
        self._file = ConfigFile(config)
        self._run = run
        self._write = write
        self._label_owners = label_owners
        self._steward_optional = steward_optional
        self._dry_run = dry_run

    @property
    def base_dir(self) -> Path:
        return self._file.path.parent

    @contextlib.contextmanager
    def _locked(self) -> Generator[None]:
        try:
            with self._file.locked():
                yield
        except (ConfigChanged, OSError) as exc:
            raise OnboardError(str(exc)) from exc

    def default_checkout(self, name: str) -> Path:
        return self.base_dir.parent / name

    def _load(self) -> tuple[bytes, str, HibernautConfig]:
        data = self._file.read()
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            raise OnboardError("the configuration is not valid: it is not UTF-8") from None
        return data, text, _parsed(text, self.base_dir)

    def check(self) -> list[tuple[ProjectConfig, Gaps]]:
        _, _, config = self._load()
        return [(project, project_gaps(config, project)) for project in config.projects.values()]

    def onboard(self, request: Request) -> Report:
        report = Report(config=self._file.path)
        split_repo(request.repo)
        project_name(request.name, request.repo)
        with self._locked():
            _, text, config = self._load()
            known = existing_project(config, request)
            if known is None:
                # Before anything is cloned or created: what would not be written later is refused now.
                self._addition(text, config, request, Report())
                checkout, label_repo = request.checkout, request.repo
            else:
                checkout, label_repo = Path(known.checkout).expanduser(), known.repo
            if self._run(["gh", "api", f"repos/{request.repo}", "--silent"]).returncode != 0:
                raise OnboardError(f"{request.repo} is not reachable with gh; nothing was changed")
            try:
                self._checkout(label_repo, checkout, report)
                if known is None or known.worktree_backend == "orca":
                    self._orca(checkout, report)
                self._label(config, label_repo, request.label, report)
                if known is not None:
                    self._report_known(config, known, report)
                else:
                    self._add(request, report)
            except OnboardError as exc:
                kept = "The configuration is as it was; l" if exc.config_kept else "L"
                raise OnboardError(f"{exc}. {kept}eft behind: {_left(report)}", config_kept=exc.config_kept) from exc
        report.by_hand.append(_switchboard_note(
            request.repo.lower(), f"proj:{request.name}" if known is None else known.inbox_id
        ))
        return report

    def complete(self, name: str, description: str | None) -> Report:
        report = Report(config=self._file.path)
        with self._locked():
            data, text, config = self._load()
            project = config.projects.get(name)
            if project is None:
                raise OnboardError(f"there is no project {name}; projects: {', '.join(sorted(config.projects)) or 'none'}")
            gaps = project_gaps(config, project)
            report.by_hand += gaps.by_hand
            edit = TomlEdit(text)
            if gaps.description:
                wanted = description_text(description, project.repo)
                self._or_by_hand(report, "description", lambda: edit.add_key(("projects", name), "description", wanted),
                                 f"description: {wanted}")
            elif description:
                report.by_hand.append("description: it is set already and was not changed")
            if gaps.steward and project.route(project.inbox_id).binding is None:
                self._add_steward(edit, config, project, report)
            if gaps.forward_to:
                _add_forward(edit, config, project.inbox_id, report)
            self._store(edit, data, report)
        return report

    @staticmethod
    def _or_by_hand(report: Report, what: str, change: Callable[[], None], done: str) -> bool:
        try:
            change()
        except EditRefused as exc:
            report.by_hand.append(f"{what}: {exc}")
            return False
        report.done.append(done)
        return True

    def _add_steward(self, edit: TomlEdit, config: HibernautConfig, project: ProjectConfig, report: Report) -> None:
        steward = project_agents(config).steward
        former = _main_agent(config, project)
        primary = next(r for r in project.rules if r.literal == project.inbox_id)
        bindings = [(primary.pattern, steward)]
        if not project.inbox_bindings:
            # With one binding written, an inbox without any takes no more forwards: the others keep their agent.
            others = [r for r in project.rules if r is not primary]
            if others and former not in config.agents:
                report.by_hand.append(f"steward: {project.name} runs {former}, which is not declared in [agents]")
                return
            bindings += [(rule.pattern, former) for rule in others]

        def add() -> None:
            for pattern, agent in bindings:
                edit.add_table(("projects", project.name), "inbox_bindings",
                               {"pattern": pattern, "default_agent": agent})

        self._or_by_hand(report, "steward", add, f"steward: {project.inbox_id} goes to {steward} first"
                         + (f", the other inboxes stay with {former}" if len(bindings) > 1 else ""))

    def _addition(self, text: str, config: HibernautConfig, request: Request, report: Report) -> TomlEdit:
        agents: ProjectAgents | None = None
        try:
            agents = project_agents(config)
        except OnboardError as exc:
            if not self._steward_optional:
                raise
            report.by_hand.append(f"steward: {exc}")
        edit = TomlEdit(text)
        edit.append_text(project_block(request, agents))
        report.done.append(f"{self._file.path.name}: [projects.{request.name}] added")
        if agents is not None:
            report.done.append(f"steward: proj:{request.name} goes to {agents.steward} first, its other inboxes to {agents.sde}")
        _add_forward(edit, config, f"proj:{request.name}", report)
        self._parsed_edit(edit)
        return edit

    def _parsed_edit(self, edit: TomlEdit) -> str:
        # The whole file, as the daemon will read it: it would otherwise keep its old settings and only log.
        try:
            updated = edit.result()
        except EditRefused as exc:
            raise OnboardError(str(exc)) from exc
        _parsed(updated, self.base_dir)
        return updated

    def _add(self, request: Request, report: Report) -> None:
        # Read again: a clone takes long enough for a person to edit the file meanwhile.
        data, text, config = self._load()
        if existing_project(config, request) is not None:
            raise OnboardError(f"{request.repo} was added by somebody else meanwhile")
        planned = Report()
        edit = self._addition(text, config, request, planned)
        self._store(edit, data, planned)
        report.done += planned.done
        report.by_hand += planned.by_hand
        report.added, report.backup = planned.added, planned.backup
        branch = self._run(["git", "-C", str(request.checkout), "symbolic-ref", "--short", "-q", "HEAD"]).stdout.strip()
        agents = None if self._dry_run else project_agents_or_none(config)
        wanted = config.roles[agents.steward].branch if agents is not None else ""
        if wanted and branch and branch != wanted:
            report.by_hand.append(f"steward: {request.checkout} is on {branch}, but agents.{agents.steward if agents else ''} "
                                  f"only runs on {wanted}")

    def _store(self, edit: TomlEdit, data: bytes, report: Report) -> None:
        if not edit.added:
            return
        updated = self._parsed_edit(edit).encode("utf-8")
        if self._dry_run:
            return
        try:
            report.backup = self._write(self._file, data, updated)
        except (ConfigChanged, OSError) as exc:
            raise OnboardError(f"{self._file.path.name} was not written: {exc}") from exc
        report.added = edit.added.strip("\n")

    def _report_known(self, config: HibernautConfig, known: ProjectConfig, report: Report) -> None:
        report.done.append(f"{self._file.path.name}: [projects.{known.name}] already serves {known.repo}")
        gaps = project_gaps(config, known)
        if gaps.missing:
            report.by_hand.append(f"{known.name} lacks {', '.join(gaps.missing)}; nothing that is there was changed, "
                                  "complete_project adds it")

    def _checkout(self, repo: str, checkout: Path, report: Report) -> None:
        if (checkout / ".git").exists():
            found = self._run(["git", "-C", str(checkout), "remote", "get-url", "origin"]).stdout.strip()
            if not _is_remote_of(found, repo):
                raise OnboardError(f"{checkout} exists but its origin is {scrub(found) or 'unset'}, not {repo}")
            if self._run(["git", "-C", str(checkout), "rev-parse", "-q", "--verify", "HEAD^{commit}"]).returncode != 0:
                raise OnboardError(f"{checkout} exists but holds no commit, as a clone that was cut off; move it away")
            report.done.append(f"checkout: {checkout} (existing)")
            return
        present = checkout.exists() or checkout.is_symlink()
        if present and (not checkout.is_dir() or any(checkout.iterdir())):
            raise OnboardError(f"{checkout} exists and is not a git checkout")
        if not self._dry_run:
            done = self._run(["gh", "repo", "clone", repo, str(checkout)])
            if done.returncode != 0:
                # This call made it, under the lock: nothing in it is anybody's work.
                shutil.rmtree(checkout, ignore_errors=True)
                left = f"; a partial clone is left in {checkout}" if checkout.exists() and any(checkout.iterdir()) else ""
                raise OnboardError(f"gh repo clone {repo} failed: {_output(done)}{left}")
        report.done.append(f"checkout: {checkout} (cloned)")

    def _orca(self, checkout: Path, report: Report) -> None:
        # `orca worktree create` knows registered repositories only.
        show = ["orca", "repo", "show", "--repo", f"path:{checkout}", "--json"]
        if self._run(show).returncode == 0:
            report.done.append(f"orca: {checkout} (existing)")
            return
        if not self._dry_run:
            done = self._run(["orca", "repo", "add", "--path", str(checkout), "--json"])
            if done.returncode != 0 or self._run(show).returncode != 0:
                raise OnboardError(f"registering {checkout} in Orca failed: {_output(done)}")
        report.done.append(f"orca: {checkout} (registered)")

    def _label(self, config: HibernautConfig, repo: str, label: str, report: Report) -> None:
        owners = self._label_owners(config)
        if owners is not None and repo.partition("/")[0].lower() not in owners:
            report.by_hand.append(f'label: create "{label}" in {repo}; its owner is not in [onboarding] label_owners')
            return
        if self._run(["gh", "api", f"repos/{repo}/labels/{label}", "--silent"]).returncode == 0:
            report.done.append(f"label: {label} (existing)")
            return
        if not self._dry_run:
            done = self._run(["gh", "label", "create", label, "-R", repo, "--color", "5319E7",
                              "--description", "交给 Hibernaut 处理（经 Switchboard Webhook 入站）"])
            if done.returncode != 0:
                raise OnboardError(f"creating label {label} on {repo} failed: {_output(done)}")
        report.done.append(f"label: {label} (created)")


def _switchboard_note(repo: str, inbox: str) -> str:
    owner, name = repo.split("/")
    routes = [f'[routing.github_repos] "{repo}" = "{inbox}"']
    if inbox == f"proj:{name}":
        routes.insert(0, f'[routing.github_owners] "{owner}" = "proj:{{repo}}"')
    return (f"switchboard: events of {repo} reach {inbox} once its configuration has {' or '.join(routes)}; comments need "
            f"{owner} in [outbound] github_app_owners")


def project_agents_or_none(config: HibernautConfig) -> ProjectAgents | None:
    try:
        return project_agents(config)
    except OnboardError:
        return None


def _left(report: Report) -> str:
    made = [step for step in report.done if step.endswith(("(cloned)", "(registered)", "(created)"))]
    return "; ".join(made) or "nothing"


def _output(done: subprocess.CompletedProcess[str]) -> str:
    return scrub((done.stderr or done.stdout).strip())[:_MAX_OUTPUT] or f"exit code {done.returncode}"


def _is_remote_of(remote: str, repo: str) -> bool:
    match = re.fullmatch(
        r"(?i)(?:https?://(?:[^/@]+@)?(?:www\.)?github\.com/|ssh://(?:[^/@]+@)?[^/:]+(?::\d+)?/|(?:[^/@:]+@)?[^/@:]+:/?)"
        r"(?P<repo>[^/]+/[^/]+?)(?:\.git)?/?", remote,
    )
    return match is not None and match["repo"].lower() == repo.lower()
