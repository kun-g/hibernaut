from __future__ import annotations

import logging
import re
from collections.abc import Callable, Collection, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Generic, Protocol, TypeVar, cast

from agent_runtime.app.inbox_patterns import UncomparablePattern, common_match
from agent_runtime.core.models import Event, InboxBinding, InboxMatchRule
from agent_runtime.core.ports import SessionStorePort
from agent_runtime.engine.worker import ExecutorFailed

_log = logging.getLogger(__name__)

# Accepted for configs written before array entries became plain regexes; such entries never act as an address.
LEGACY_PATTERN_PREFIX = "re:"
_REGEX_SYNTAX = frozenset(".^$*+?{}[]|()")


def _fixed_inbox_id(pattern: str) -> str | None:
    # The one inbox_id a pattern can fullmatch when it uses no regex syntax beyond escaped punctuation
    # ("proj:a\.b" -> "proj:a.b"); None when it can match more than one string.
    out: list[str] = []
    chars = iter(pattern)
    for c in chars:
        if c == "\\":
            nxt = next(chars, "")
            if not nxt or (nxt.isascii() and nxt.isalnum()):
                return None
            out.append(nxt)
        elif c in _REGEX_SYNTAX:
            return None
        else:
            out.append(c)
    return "".join(out)


@dataclass(frozen=True, slots=True)
class InboxRule:
    # Every rule fullmatches the whole inbox_id. `literal` is the single inbox_id the rule can match, the only
    # form that may serve as a delivery address; patterns route but never receive deliveries.
    text: str
    pattern: str
    literal: str | None
    _compiled: re.Pattern[str] = field(compare=False, repr=False)

    @staticmethod
    def exact(inbox_id: str) -> InboxRule:
        escaped = re.escape(inbox_id)
        return InboxRule(text=inbox_id, pattern=escaped, literal=inbox_id, _compiled=re.compile(escaped))

    @staticmethod
    def parse(entry: str) -> InboxRule:
        # Raises re.error for an invalid pattern.
        legacy = entry.startswith(LEGACY_PATTERN_PREFIX)
        pattern = entry[len(LEGACY_PATTERN_PREFIX):] if legacy else entry
        compiled = re.compile(pattern)
        literal = None if legacy else _fixed_inbox_id(pattern)
        if literal is not None and compiled.fullmatch(literal) is None:
            literal = None
        return InboxRule(text=entry, pattern=pattern, literal=literal, _compiled=compiled)

    def matches(self, inbox_id: str) -> bool:
        return self._compiled.fullmatch(inbox_id) is not None

    def __str__(self) -> str:
        return self.text


def parse_inbox_rules(project: str, raw: object) -> tuple[InboxRule, ...]:
    # A string is one exact inbox_id (the pre-array form, kept verbatim). In an array every entry is a regex that
    # must fullmatch the inbox_id; an entry without regex syntax names exactly one inbox.
    where = f"projects.{project}.inbox_id"
    if raw is None:
        return (InboxRule.exact(f"proj:{project}"),)
    if isinstance(raw, str):
        if not raw.strip():
            raise ValueError(f"{where} must not be empty")
        if raw.startswith(LEGACY_PATTERN_PREFIX):
            raise ValueError(
                f"{where} = {raw!r} is a pattern; list it in an array after a literal inbox_id, "
                f'e.g. inbox_id = ["proj:{project}", {raw[len(LEGACY_PATTERN_PREFIX):]!r}]'
            )
        return (InboxRule.exact(raw),)
    if not isinstance(raw, list) or not raw:
        raise ValueError(f"{where} must be a string or a non-empty array of strings")
    rules: list[InboxRule] = []
    for item in raw:  # pyright: ignore[reportUnknownVariableType]
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{where} entries must be non-empty strings, got {item!r}")
        if item == LEGACY_PATTERN_PREFIX:
            raise ValueError(f"{where}: {item!r} has an empty pattern")
        try:
            rule = InboxRule.parse(item)
        except re.error as exc:
            raise ValueError(f"{where}: invalid pattern {item!r}: {exc}") from exc
        if any(r.pattern == rule.pattern for r in rules):
            raise ValueError(f"{where} lists {item!r} twice")
        rules.append(rule)
    if not any(r.literal is not None for r in rules):
        raise ValueError(
            f"{where} needs at least one exact inbox_id (an entry without regex syntax; escape a literal '.' as "
            "'\\.'); the first one is the primary inbox for this project"
        )
    return tuple(rules)


@dataclass(frozen=True, slots=True)
class AgentConfig:
    name: str
    # The [executors.<id>] driver; a legacy role executor is its own driver.
    executor: str
    role: str | None = None
    model: str = ""
    effort: str = ""
    checkout_access: str = ""
    description: str = ""


@dataclass(frozen=True, slots=True)
class ProjectBinding:
    rule: InboxRule
    binding: InboxBinding


_BINDING_KEYS = frozenset({"pattern", "default_agent", "agents", "rules"})
_MATCH_KEYS = frozenset({"source", "event_type", "tags", "agent"})


def _text(where: str, item: Mapping[str, Any], key: str, *, required: bool = False) -> str | None:
    value = item.get(key)
    if value is None and not required:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{where}: {key} must be a non-empty string")
    return value.strip()


def _texts(where: str, item: Mapping[str, Any], key: str) -> tuple[str, ...]:
    if key not in item:
        return ()
    value = item[key]
    listed = cast(list[object], value) if isinstance(value, list) else []
    if not listed or not all(isinstance(v, str) and v.strip() for v in listed):
        raise ValueError(f"{where}: {key} must be a non-empty array of strings")
    found = tuple(str(v).strip() for v in listed)
    twice = sorted({v for v in found if found.count(v) > 1})
    if twice:
        raise ValueError(f"{where}: {key} lists {', '.join(twice)} twice")
    return found


def _tables(where: str, raw: object) -> list[Mapping[str, Any]]:
    listed = cast(list[object], raw) if isinstance(raw, list) else None
    if listed is None or not all(isinstance(item, Mapping) for item in listed):
        raise ValueError(f"{where} must be an array of tables")
    return cast(list[Mapping[str, Any]], listed)


def _only(where: str, item: Mapping[str, Any], allowed: frozenset[str]) -> None:
    unknown = sorted(set(item) - allowed)
    if unknown:
        raise ValueError(f"{where}: {', '.join(unknown)} not supported")


def parse_inbox_bindings(
    project: str, raw: object, *, inbox_rules: Sequence[InboxRule], declared: Collection[str]
) -> tuple[ProjectBinding, ...]:
    if raw is None:
        return ()
    bindings: list[ProjectBinding] = []
    for index, item in enumerate(_tables(f"projects.{project}.inbox_bindings", raw)):
        where = f"projects.{project}.inbox_bindings[{index}]"
        _only(where, item, _BINDING_KEYS)
        pattern = _text(where, item, "pattern", required=True) or ""
        try:
            rule = InboxRule.parse(pattern)
        except re.error as exc:
            raise ValueError(f"{where}: invalid pattern {pattern!r}: {exc}") from exc
        if any(b.rule.pattern == rule.pattern for b in bindings):
            raise ValueError(f"{where}: pattern {pattern!r} is listed twice")
        if rule.literal is not None and not any(r.matches(rule.literal) for r in inbox_rules):
            raise ValueError(f"{where}: {pattern!r} is not an inbox of projects.{project} (see its inbox_id)")
        agents = _texts(where, item, "agents")

        def check(at: str, key: str, agent: str, agents: tuple[str, ...] = agents) -> str:
            if agent not in declared:
                raise ValueError(f"{at}: {key} '{agent}' is not declared in [agents] (declared: "
                                 f"{', '.join(sorted(declared)) or 'none'})")
            if agents and agent not in agents:
                raise ValueError(f"{at}: {key} '{agent}' is not in agents ({', '.join(agents)})")
            return agent

        for agent in agents:
            check(where, "agents entry", agent)
        rules: list[InboxMatchRule] = []
        for position, entry in enumerate(_tables(f"{where}.rules", item.get("rules", []))):
            at = f"{where}.rules[{position}]"
            _only(at, entry, _MATCH_KEYS)
            match = InboxMatchRule(
                agent=check(at, "agent", _text(at, entry, "agent", required=True) or ""),
                source=_text(at, entry, "source"),
                event_type=_text(at, entry, "event_type"),
                tags=_texts(at, entry, "tags"),
            )
            if match.source is None and match.event_type is None and not match.tags:
                raise ValueError(f"{at}: needs source, event_type or tags (default_agent takes everything else)")
            rules.append(match)
        bindings.append(ProjectBinding(rule=rule, binding=InboxBinding(
            inbox=pattern,
            default_agent=check(where, "default_agent", _text(where, item, "default_agent", required=True) or ""),
            agents=agents,
            rules=tuple(rules),
        )))
    return tuple(bindings)


class _Bound(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def inbox_rules(self) -> tuple[InboxRule, ...]: ...

    @property
    def inbox_bindings(self) -> tuple[ProjectBinding, ...]: ...

    @property
    def default_agent(self) -> str | None: ...

    @property
    def agents(self) -> tuple[AgentConfig, ...]: ...


def validate_project_bindings(projects: Iterable[_Bound]) -> None:
    # An inbox belongs to one project, so a binding that also matches another project's inbox is a mistake: it would
    # never apply there.
    listed = list(projects)
    for proj in listed:
        for bound in proj.inbox_bindings:
            for other in listed:
                if other.name == proj.name:
                    continue
                for rule in other.inbox_rules:
                    if rule.literal is not None and bound.rule.matches(rule.literal):
                        raise ValueError(
                            f"projects.{proj.name}.inbox_bindings pattern {bound.binding.inbox!r} also matches "
                            f"inbox_id {rule.literal!r} of projects.{other.name}"
                        )


def _agent_line(agent: AgentConfig) -> str:
    facts = [f"role {agent.role}" if agent.role else "no role", f"executor {agent.executor}"]
    if agent.model:
        facts.append(f"model {agent.model}" + (f" ({agent.effort})" if agent.effort else ""))
    if agent.checkout_access:
        facts.append(f"checkout {agent.checkout_access}")
    line = f"- `{agent.name}`: {', '.join(facts)}"
    return f"{line}. {agent.description}" if agent.description else line


def _rule_line(rule: InboxMatchRule) -> str:
    conditions = [f"{key} = {value}" for key, value in (("source", rule.source), ("event_type", rule.event_type)) if value]
    if rule.tags:
        conditions.append(f"tags include {', '.join(rule.tags)}")
    return f"{' and '.join(conditions)} -> `{rule.agent}`"


def build_routing_manifest(project: _Bound) -> str:
    # What a Steward needs to route work, rendered from the loaded config on every wake.
    used = {name for b in project.inbox_bindings for name in (*b.binding.agents, b.binding.default_agent)}
    if any(not b.binding.agents for b in project.inbox_bindings) or not project.inbox_bindings:
        used = {a.name for a in project.agents}
    default = f"`{project.default_agent}`" if project.default_agent else "the runtime's default agent"
    parts = [f"Project `{project.name}`. Its default agent is {default}.", "", "Agents:"]
    parts += [_agent_line(a) for a in project.agents if a.name in used]
    parts += ["", "Inboxes (the first that matches an inbox id applies):"]
    if not project.inbox_bindings:
        parts += [f"- {_inbox_text(rule)}: default agent {default}" for rule in project.inbox_rules]
    for bound in project.inbox_bindings:
        binding = bound.binding
        allowed = ", ".join(f"`{a}`" for a in binding.agents) or "any declared agent"
        parts.append(f"- {_inbox_text(bound.rule)}: default agent `{binding.default_agent}`; agents: {allowed}")
        parts += [f"  - rule {n}: {_rule_line(rule)}" for n, rule in enumerate(binding.rules, 1)]
    return "\n".join(parts)


def _inbox_text(rule: InboxRule) -> str:
    if rule.literal is not None:
        return f"`{rule.literal}`"
    return f"`{rule.pattern}` (a pattern: any inbox id it matches in full)"


class _Routable(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def inbox_rules(self) -> tuple[InboxRule, ...]: ...


P = TypeVar("P", bound=_Routable)
V = TypeVar("V")


class AmbiguousProjectMatch(ExecutorFailed):
    def __init__(self, inbox_id: str, projects: Iterable[str]) -> None:
        self.inbox_id = inbox_id
        self.projects = tuple(projects)
        super().__init__(
            f"inbox_id {inbox_id!r} matches more than one project ({', '.join(self.projects)}); "
            "make the projects' inbox_id rules disjoint"
        )


def validate_project_rules(projects: Iterable[_Routable]) -> None:
    owners: dict[str, str] = {}
    listed = list(projects)
    for proj in listed:
        for rule in proj.inbox_rules:
            if rule.literal is None:
                continue
            other = owners.setdefault(rule.literal, proj.name)
            if other != proj.name:
                raise ValueError(
                    f"inbox_id {rule.literal!r} is assigned to both projects.{other} and projects.{proj.name}"
                )
    # A literal one project claims and another project's pattern also matches would be ambiguous at every dispatch.
    for literal, owner in owners.items():
        for proj in listed:
            if proj.name != owner and any(r.literal is None and r.matches(literal) for r in proj.inbox_rules):
                raise ValueError(
                    f"inbox_id {literal!r} of projects.{owner} also matches a pattern of projects.{proj.name}"
                )
    _validate_disjoint_patterns(listed)


def _validate_disjoint_patterns(projects: Sequence[_Routable]) -> None:
    # An inbox of both would be ambiguous at every dispatch, so what cannot be told apart here is refused as well.
    for index, proj in enumerate(projects):
        for other in projects[index + 1:]:
            for rule in proj.inbox_rules:
                for theirs in other.inbox_rules:
                    if rule.literal is None and theirs.literal is None:
                        _require_disjoint((proj.name, rule.pattern), (other.name, theirs.pattern))


def _require_disjoint(*patterns: tuple[str, str]) -> None:
    (name, pattern), (other, theirs) = patterns
    try:
        shared = common_match(pattern, theirs)
    except UncomparablePattern:
        if pattern != theirs:
            for owner, text in patterns:
                try:
                    common_match(text, text)
                except UncomparablePattern as exc:
                    raise ValueError(
                        f"projects.{owner} pattern {text!r} cannot be compared with the inbox_id patterns of another "
                        f"project: {exc}"
                    ) from exc
            raise ValueError(
                f"projects.{name} pattern {pattern!r} cannot be compared with pattern {theirs!r} of projects.{other}: "
                "together they are too large"
            ) from None
        shared = ""
    if shared is not None:
        raise ValueError(
            f"pattern {pattern!r} of projects.{name} and pattern {theirs!r} of projects.{other} both match inbox_id "
            f"{shared!r}; make them disjoint" if shared else
            f"pattern {pattern!r} of projects.{name} and pattern {theirs!r} of projects.{other} both match the same ids"
        )


class ProjectRouter(Generic[P]):
    def __init__(self, projects: Iterable[P]) -> None:
        self._projects = tuple(projects)

    @property
    def projects(self) -> tuple[P, ...]:
        return self._projects

    def matching(self, inbox_id: str) -> list[P]:
        # Several rules of one project count as one match.
        return [p for p in self._projects if any(rule.matches(inbox_id) for rule in p.inbox_rules)]

    def resolve(self, inbox_id: str) -> P | None:
        found = self.matching(inbox_id)
        if len(found) > 1:
            raise AmbiguousProjectMatch(inbox_id, (p.name for p in found))
        return found[0] if found else None

    def inbox_map(self, value: Callable[[P], V | None]) -> ProjectInboxMap[P, V]:
        return ProjectInboxMap(self, value)


class ProjectInboxMap(Mapping[str, V], Generic[P, V]):
    # Read-only inbox_id -> project setting view that also answers pattern-matched inboxes. Iteration lists only
    # literal inbox IDs; lookups of an inbox matching two projects raise AmbiguousProjectMatch.
    def __init__(self, router: ProjectRouter[P], value: Callable[[P], V | None]) -> None:
        self._router = router
        self._value = value

    def __getitem__(self, inbox_id: str) -> V:
        project = self._router.resolve(inbox_id)
        found = self._value(project) if project is not None else None
        if found is None:
            raise KeyError(inbox_id)
        return found

    def __iter__(self) -> Iterator[str]:
        for project in self._router.projects:
            if self._value(project) is None:
                continue
            for rule in project.inbox_rules:
                if rule.literal is not None:
                    yield rule.literal

    def __len__(self) -> int:
        return sum(1 for _ in self)


class UnmatchedProjectHold:
    # Hold policy for an inbox that matches no [projects] entry. An event that would need the project's checkout
    # stays pending and unclaimed until a restart or config reload brings a rule that matches; events that name
    # their own workdir, review target, approval or close request run as before. One WARNING per held event and
    # hold instance (each worker pool generation builds its own).
    def __init__(self, sessions: SessionStorePort) -> None:
        self._sessions = sessions
        self._warned: set[str] = set()

    def __call__(self, event: Event) -> str | None:
        if self._runs_without_project(event):
            return None
        reason = (
            f"inbox_id {event.inbox_id!r} matches no [projects] entry and the event names no explicit_workdir; "
            "it stays pending until a project's inbox_id rule matches (restart or config reload)"
        )
        if event.event_id not in self._warned:
            self._warned.add(event.event_id)
            _log.warning(
                "Event %s (source=%s, external_id=%s) in inbox %s is waiting for a project: %s",
                event.event_id, event.source, event.external_id, event.inbox_id, reason,
            )
        return reason

    def _runs_without_project(self, event: Event) -> bool:
        payload = event.payload
        if any(isinstance(payload.get(key), str) and payload[key].strip()
               for key in ("explicit_workdir", "review_of", "approval_request_id")):
            return True
        if (payload.get("action") == "close_session" or payload.get("close_session") is True
                or event.source == "session:close"):
            return True
        # A follow-up to a suspended session goes to the executor holding its checkpoint, which names the workdir.
        if event.session_id:
            session = self._sessions.get(event.session_id)
        elif event.correlation_key:
            session = self._sessions.find_by_correlation_key(event.inbox_id, event.correlation_key)
        else:
            session = None
        return session is not None and session.status == "suspended"
