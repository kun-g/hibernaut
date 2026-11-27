from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import math
import re
import sys
import tomllib
import warnings
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import urlparse

from agent_runtime.adapters.channels.ingress import FeishuGroup
from agent_runtime.adapters.delivery import DeliveryConfig, parse_delivery
from agent_runtime.adapters.executor_cli.claude_code import (
    AGENT_PROFILE_KEYS,
    EXECUTOR_KEYS,
    ClaudeCodeLauncher,
    ClaudeCodeProfile,
    parse_launchers,
    parse_profile,
)
from agent_runtime.adapters.executor_cli.executor import CLI_TERMINALS, DEFAULT_CLI_TEMPLATES, CLITemplate
from agent_runtime.adapters.executor_cli.orca import registered_checkout
from agent_runtime.adapters.executor_cli.peer_handoff import PeerHandoffConfig
from agent_runtime.adapters.executor_cli.quota import parse_quota_errors
from agent_runtime.adapters.executor_cli.reviewer import CODE_REVIEWER_ROLE
from agent_runtime.adapters.executor_cli.role_skills import ROLE_ALIASES, canonical_role
from agent_runtime.adapters.executor_cli.sde import SDE_OPTION_KEYS, SDE_ROLE, SDEOptions
from agent_runtime.adapters.executor_cli.steward import STEWARD_ROLE
from agent_runtime.adapters.executor_cli.reclaim_sweep import DEFAULT_SWEEP_INTERVAL_SECONDS
from agent_runtime.adapters.executor_cli.worktree import WorktreeBackend
from agent_runtime.adapters.memory_bridge import DEFAULT_GC_INTERVAL_SECONDS
from agent_runtime.adapters.permissions.security import proxy_service_url
from agent_runtime.app.projects import (
    AgentConfig,
    ExpertBinding,
    InboxRule,
    ProjectBinding,
    ProjectRouter,
    WorkdirBinding,
    draft_experts,
    parse_inbox_bindings,
    parse_inbox_rules,
    parse_workdir_bindings,
    project_memory_key,
    validate_project_bindings,
    validate_project_rules,
    validate_workdir_bindings,
)
from agent_runtime.app.reload import ReloadStatus
from agent_runtime.core.models import ConfiguredTimer, InboxRoute, NewEventPolicy
from collaboration import ExpertPoolRegistry, ExpertProfile, MemoryMountPlan, default_expert_profiles
from memory_hub import BaselineBudgetConfig, KnowledgeMountConfig, MemoryHubConfig
from memory_hub.models import AccessRole
from memory_hub.partition import sanitize_segment
from runner.cli_drivers import TemplateCLIDriver, ToolHookStyle
from runner.protocol_check import PROTOCOL_CHECK_EXECUTOR_ID

_log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    db_path: str = "./hibernaut.db"
    # The run ledger's own SQLite file (runs, audits); "" puts ledger.db beside db_path.
    ledger_db_path: str = ""
    memory_root: str = "./agent_memory"
    max_concurrency: int = 4
    new_event_policy: NewEventPolicy = "defer"
    # default_agent; default_executor is its old name and always holds the same value.
    default_agent: str = "claude"
    default_executor: str = "claude"
    # Background reclaim of merged task worktrees a delivery-time reclaim had to skip; 0 turns it off.
    worktree_sweep_interval_seconds: float = DEFAULT_SWEEP_INTERVAL_SECONDS
    # Inboxes of no project, each with the directory its agents start in; the first that matches applies.
    inbox_bindings: tuple[WorkdirBinding, ...] = ()

    def workdir_binding(self, inbox_id: str) -> WorkdirBinding | None:
        return next((b for b in self.inbox_bindings if b.rule.matches(inbox_id)), None)


@dataclass(frozen=True, slots=True)
class ChannelsConfig:
    host: str = "127.0.0.1"
    port: int = 8080
    mcp_enabled: bool = False
    mcp_token_env: str = "HIBERNAUT_MCP_TOKEN"
    # Directory for the file Outbox (one Markdown file per delivered message); "" leaves it off.
    outbox_dir: str = ""


TerminalBackend = Literal["orca", "none"]
TERMINAL_BACKENDS: tuple[TerminalBackend, ...] = ("orca", "none")
@dataclass(frozen=True, slots=True)
class OnboardingConfig:
    # Lower-cased GitHub owners in whose repositories the system tools may create the trigger label.
    label_owners: tuple[str, ...] = ()


Handoff = Literal["mcp", "peer"]
HANDOFFS: tuple[Handoff, ...] = ("mcp", "peer")


@dataclass(frozen=True, slots=True)
class ProjectConfig:
    name: str
    # The first literal of inbox_id is the primary inbox for project mappings.
    inbox_id: str
    repo: str
    checkout: str
    worktree_backend: WorktreeBackend = "orca"
    # Where interactive role sessions open; "none" makes interactive roles stop with the reason instead.
    terminal_backend: TerminalBackend = "orca"
    orca_create_terminal: bool = False
    # How interactive Claude Code role sessions get their task and return the result: "mcp" (the launcher's
    # hibernaut_handoff server) or "peer" (peer messages through the [peer] receiver).
    handoff: Handoff = "mcp"
    # Agent of inboxes no binding covers; `executor` is its old name in the file.
    default_agent: str | None = None
    # Hibernaut's PR delivery step for finished SDE tasks; None ends a task with local commits.
    delivery: DeliveryConfig | None = None
    # Every rule routing an inbox to this project; empty means just the primary inbox_id.
    rules: tuple[InboxRule, ...] = ()
    # Which agents each inbox of the project may run; the first binding matching an inbox applies.
    inbox_bindings: tuple[ProjectBinding, ...] = ()
    # The shared experts this project drafts; they come before its own bindings.
    expert_bindings: tuple[ExpertBinding, ...] = ()
    # Every declared agent, for the routing manifest.
    agents: tuple[AgentConfig, ...] = ()
    # Inboxes of this project running at once; None leaves only runtime.max_concurrency.
    max_concurrency: int | None = None
    # What the project is, for a Steward outside it that may send work here.
    description: str = ""

    def route(self, inbox_id: str) -> InboxRoute:
        bound = next((b.binding for b in (*self.expert_bindings, *self.inbox_bindings) if b.rule.matches(inbox_id)), None)
        return InboxRoute(project=self.name, binding=bound, has_bindings=bool(self.inbox_bindings))

    @property
    def executor(self) -> str | None:
        return self.default_agent

    @property
    def inbox_rules(self) -> tuple[InboxRule, ...]:
        return self.rules or (InboxRule.exact(self.inbox_id),)

    @property
    def serves_multiple_inboxes(self) -> bool:
        # Such a project shares one checkout between concurrent inboxes, so every writable task needs its own
        # managed worktree.
        rules = self.inbox_rules
        return len(rules) > 1 or any(r.literal is None for r in rules)


ROLES = (CODE_REVIEWER_ROLE, SDE_ROLE, STEWARD_ROLE)
# Before code-reviewer the review role and its usual executor were both named coder-reviewer.
_LEGACY_REVIEWER = "coder-reviewer"
# Deployment mode shared by every role, as an executor or an agent; visible is the old name of interactive.
ROLE_DEPLOYMENT_KEYS = frozenset({"interactive", "visible"})
# On [executors.<id>] it caps all agents running on that driver together, on [agents.<id>] just that agent.
EXECUTOR_LIMIT_KEYS = frozenset({"max_concurrency"})
READONLY_CHECKOUT = "readonly"


@dataclass(frozen=True, slots=True)
class RoleConfig:
    role: str
    profile: ClaudeCodeProfile
    sde: SDEOptions | None = None
    # True: every run needs a session a person can join, opened by the project's terminal_backend; without one the
    # run stops with the reason. False: runs are non-interactive.
    interactive: bool = True
    # "readonly": the role works in the main checkout, writes nothing there and never gets a worktree.
    checkout_access: str = ""
    # Steward only: where it runs instead of the project's checkout, and the branch that directory has to be on.
    workdir: str = ""
    branch: str = ""
    max_concurrency: int | None = None


def _max_concurrency(where: str, item: Mapping[str, Any]) -> int | None:
    value = item.get("max_concurrency")
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{where}.max_concurrency must be a positive integer, got {value!r}")
    return value


def _optional_text(where: str, item: Mapping[str, Any], key: str) -> str:
    value = item.get(key)
    if value is None:
        return ""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{where}: {key} must be a non-empty string")
    return value.strip()


def _role_options(role: str) -> frozenset[str]:
    if role == SDE_ROLE:
        return SDE_OPTION_KEYS | ROLE_DEPLOYMENT_KEYS
    if role == STEWARD_ROLE:
        return ROLE_DEPLOYMENT_KEYS | {"checkout_access", "workdir", "branch"}
    return ROLE_DEPLOYMENT_KEYS


def _parse_role(
    name: str, item: dict[str, Any], *, launchers: dict[str, ClaudeCodeLauncher], anthropic_url: str,
    base_dir: Path | None, extra_keys: frozenset[str] = frozenset(),
) -> RoleConfig:
    raw_role = item["role"]
    if raw_role not in ROLES and raw_role not in ROLE_ALIASES:
        raise ValueError(f"executors.{name}: unsupported role '{raw_role}' (supported: {', '.join(ROLES)})")
    role = canonical_role(str(raw_role))
    profile = parse_profile(
        name, item, role=role, launchers=launchers, credential_proxy_url=anthropic_url,
        extra_keys=_role_options(role) | extra_keys, base_dir=base_dir,
    )
    if role == STEWARD_ROLE:
        access = item.get("checkout_access", READONLY_CHECKOUT)
        if access != READONLY_CHECKOUT:
            raise ValueError(f"executors.{name}: checkout_access must be \"{READONLY_CHECKOUT}\" for role '{role}'")
        if _interactive(name, {"interactive": False, **item} if "visible" not in item else item):
            raise ValueError(f"executors.{name}: interactive is not supported for role '{role}'; leave it out")
        workdir = _optional_text(f"executors.{name}", item, "workdir")
        return RoleConfig(role=role, profile=profile, interactive=False, checkout_access=access,
                          workdir=_resolve_path(workdir, base_dir) if workdir else "",
                          branch=_optional_text(f"executors.{name}", item, "branch"),
                          max_concurrency=_max_concurrency(f"executors.{name}", item))
    return RoleConfig(role=role, profile=profile, sde=_sde_options(name, item) if role == SDE_ROLE else None,
                      interactive=_interactive(name, item),
                      max_concurrency=_max_concurrency(f"executors.{name}", item))


_AGENT_KEYS = frozenset({"role", "executor", "description"}) | EXECUTOR_LIMIT_KEYS


def _parse_agent(
    name: str, item: Mapping[str, Any], *, bases: Mapping[str, dict[str, Any]], templates: Mapping[str, CLITemplate],
    launchers: dict[str, ClaudeCodeLauncher], anthropic_url: str, base_dir: Path | None,
) -> RoleConfig | CLITemplate:
    # An agent is a role with its own profile on a pure Claude Code executor, or a CLI template under its own name.
    driver = item.get("executor")
    if not isinstance(driver, str) or not driver.strip():
        raise ValueError(f"executors.{name}: executor is required")
    description = item.get("description", "")
    if not isinstance(description, str):
        raise ValueError(f"executors.{name}: description must be a string")
    role = item.get("role")
    if driver in templates:
        if role is not None:
            raise ValueError(
                f"executors.{name}: role '{role}' needs a Claude Code executor; '{driver}' is a CLI template"
            )
        unknown = sorted(set(item) - _AGENT_KEYS)
        if unknown:
            raise ValueError(f"executors.{name}: {', '.join(unknown)} not supported on CLI executor '{driver}'")
        template = templates[driver]
        return dataclasses.replace(template, name=name,
                                   max_concurrency=template.max_concurrency if name == driver else None)
    if driver not in bases:
        raise ValueError(f"executors.{name}: executor '{driver}' is not declared as a role-less [executors.{driver}] "
                         f"(declared: {', '.join(sorted({*bases, *templates})) or 'none'})")
    if role is None:
        raise ValueError(f"executors.{name}: executor '{driver}' needs a role (supported: {', '.join(ROLES)})")
    if role in ROLES or role in ROLE_ALIASES:
        unknown = sorted(set(item) - _AGENT_KEYS - AGENT_PROFILE_KEYS - _role_options(canonical_role(str(role))))
        if unknown:
            raise ValueError(f"executors.{name}: {', '.join(unknown)} not supported for role '{role}'; "
                             "the driver's own settings belong in [executors]")
    own = {k: v for k, v in item.items() if k not in _AGENT_KEYS or k == "role"}
    merged = {**bases[driver], **own}
    return _parse_role(name, merged, launchers=launchers, anthropic_url=anthropic_url, base_dir=base_dir)


def _as_agent_error(name: str, exc: ValueError) -> ValueError:
    # The profile parsers name their [executors.<id>] table; here the mistake is in [agents.<id>].
    return ValueError(str(exc).replace(f"executors.{name}:", f"agents.{name}:", 1))


def _default_agent(where: str, item: Mapping[str, Any], legacy_key: str) -> str | None:
    found: dict[str, str] = {}
    for key in ("default_agent", legacy_key):
        raw = item.get(key)
        if raw is not None and str(raw).strip():
            found[key] = str(raw).strip()
    if len(set(found.values())) > 1:
        raise ValueError(f"{where}: {legacy_key} is the old name of default_agent and names another agent; keep only "
                         "default_agent")
    return next(iter(found.values()), None)


def _interactive(name: str, item: dict[str, Any]) -> bool:
    if "interactive" in item and "visible" in item:
        raise ValueError(f"executors.{name}: both interactive and visible are set; visible is the old name, keep only interactive")
    key = "interactive" if "interactive" in item else "visible"
    value = item.get(key, True)
    if not isinstance(value, bool):
        raise ValueError(f"executors.{name}: {key} must be true or false")
    return value


def _sde_options(name: str, item: dict[str, Any]) -> SDEOptions:
    where = f"executors.{name}"
    defaults = SDEOptions()
    review = item.get("review", defaults.review)
    if not isinstance(review, bool):
        raise ValueError(f"{where}: review must be true or false")
    rounds = item.get("max_review_rounds", defaults.max_review_rounds)
    if isinstance(rounds, bool) or not isinstance(rounds, int) or rounds < 1:
        raise ValueError(f"{where}: max_review_rounds must be a positive integer")
    reviewer = _reviewer_option(where, item.get("reviewer", defaults.reviewer))
    return SDEOptions(review=review, reviewer=reviewer, max_review_rounds=rounds)


def _reviewer_option(where: str, raw: object) -> str | tuple[str, ...]:
    if isinstance(raw, str):
        return raw.strip()
    listed = cast(list[object], raw) if isinstance(raw, list) else []
    names = tuple(n.strip() for n in listed if isinstance(n, str))
    if not names or len(names) != len(listed):
        raise ValueError(f"{where}: reviewer must be an executor name or a non-empty list of them")
    for n, name in enumerate(names):
        if name in names[:n]:
            raise ValueError(f"{where}: reviewer '{name}' is listed twice")
    return names


def _resolve_reviewer(
    name: str, opts: SDEOptions, *, explicit: bool, roles: dict[str, RoleConfig], executors: dict[str, CLITemplate],
) -> SDEOptions:
    # A reviewer is a code-reviewer role executor or any CLI template, built-in ones included.
    templates = {**DEFAULT_CLI_TEMPLATES, **executors}
    reviewer = opts.reviewer
    if not explicit and reviewer not in roles and reviewer not in templates:
        legacy = roles.get(_LEGACY_REVIEWER)
        if legacy is not None and legacy.role == CODE_REVIEWER_ROLE:
            # Deployments from before the rename keep working without naming their reviewer.
            reviewer = _LEGACY_REVIEWER
    resolved = SDEOptions(review=opts.review, reviewer=reviewer, max_review_rounds=opts.max_review_rounds)
    if not opts.review:
        return resolved
    choices = sorted({n for n, r in roles.items() if r.role == CODE_REVIEWER_ROLE} | set(templates))
    hint = f"choose a {CODE_REVIEWER_ROLE} executor or a CLI executor ({', '.join(choices)}) or set review = false"
    for target in resolved.candidates:
        where = f"executors.{name}: reviewer '{target}'"
        if target == name:
            raise ValueError(f"{where} is this executor itself; {hint}")
        role = roles.get(target)
        if role is not None and role.role != CODE_REVIEWER_ROLE:
            raise ValueError(f"{where} has role '{role.role}', which cannot review; {hint}")
        if role is None and target not in templates:
            raise ValueError(f"{where} is not a declared executor; {hint}")
    return resolved


@dataclass(frozen=True, slots=True)
class AvailabilityConfig:
    # The availability table another program writes; "" leaves every review candidate available.
    file: str = ""
    # How long a reviewer that ran out of quota is not chosen.
    quota_cooldown_seconds: float = 3600.0


def parse_availability(raw: Any, *, base_dir: Path | None = None) -> AvailabilityConfig:
    defaults = AvailabilityConfig()
    if raw is None:
        return defaults
    if not isinstance(raw, dict):
        raise ValueError("[availability] must be a table")
    section = cast(dict[str, Any], raw)
    unknown = sorted(set(section) - {"file", "quota_cooldown_seconds"})
    if unknown:
        raise ValueError(f"availability: {', '.join(unknown)} not supported")
    path = _optional_text("availability", section, "file")
    cooldown = section.get("quota_cooldown_seconds", defaults.quota_cooldown_seconds)
    if isinstance(cooldown, bool) or not isinstance(cooldown, (int, float)) or not 0 < cooldown < math.inf:
        raise ValueError("availability.quota_cooldown_seconds must be a positive number")
    return AvailabilityConfig(file=_resolve_path(path, base_dir) if path else "",
                              quota_cooldown_seconds=float(cooldown))


@dataclass(frozen=True, slots=True)
class CredentialProxyConfig:
    url: str = ""
    anthropic_service: str = "anthropic"


@dataclass(frozen=True, slots=True)
class GDriveConfig:
    enabled: bool = False
    # The service account key lives only in the external Drive queue proxy; Hibernaut holds a
    # local client token for that proxy and receives queue results, never a Google credential.
    proxy_url: str = ""
    proxy_service: str = "gdrive-queue"
    proxy_token_file: str = ""
    root_folder_id: str = ""
    pending_folder_id: str = ""
    in_progress_folder_id: str = ""
    done_folder_id: str = ""
    failed_folder_id: str = ""
    attachments_folder_id: str = ""
    # Where task attachments are downloaded, outside every worktree; "" puts them next to runtime.db_path.
    attachments_dir: str = ""
    poll_interval_seconds: float = 30.0
    default_inbox_id: str = "default"


SwitchboardMode = Literal["in_process", "relay"]
_SWITCHBOARD_MODES: tuple[SwitchboardMode, ...] = ("in_process", "relay")


@dataclass(frozen=True, slots=True)
class SwitchboardLinkConfig:
    enabled: bool = False
    mode: SwitchboardMode = "in_process"
    # in_process: the switchboard.toml to run inside this process.
    config_path: str | None = None
    consumer_id: str = "hibernaut"
    # relay: the remote hub's ws(s)://.../ws/relay and http(s)://.../mcp endpoints.
    relay_url: str | None = None
    mcp_url: str | None = None
    ca_file: str | None = None


RunnerMode = Literal["in_process", "relay"]
_RUNNER_MODES: tuple[RunnerMode, ...] = ("in_process", "relay")
_TOOL_HOOK_STYLES: tuple[ToolHookStyle, ...] = ("claude", "grok", "cursor-agent")


@dataclass(frozen=True, slots=True)
class RunnerLinkConfig:
    enabled: bool = False
    mode: RunnerMode = "in_process"
    # The CLI agents that run on the runner; the others keep running as local subprocesses.
    executors: tuple[str, ...] = ()
    timeout_seconds: float = 1800.0
    # How long a Run waits for a node with a free slot (relay: also for nodes to connect) before it fails.
    node_wait_seconds: float = 60.0
    # Added to the permission policy; a tool in neither list, nor in the built-in ones, asks a person.
    allow_tools: frozenset[str] = frozenset()
    block_tools: frozenset[str] = frozenset()
    # Every Run of these executors is read-only, whatever its events ask for.
    readonly_only: bool = False
    # in_process
    runner_id: str = "local"
    max_concurrent: int | None = None
    worktree_backend: WorktreeBackend = "git"
    state_dir: str = ""
    logs_dir: str = ""
    # relay: where this process listens for nodes, and the environment variable (or the owner-only file) holding their
    # shared token.
    listen_host: str = "127.0.0.1"
    listen_port: int = 8790
    token_env: str = "HIBERNAUT_RUNNER_TOKEN"
    token_file: str = ""
    affinity_db_path: str = ""


_RUNNER_KEYS = frozenset(f.name for f in dataclasses.fields(RunnerLinkConfig))


@dataclass(frozen=True, slots=True)
class MemoryHTTPToken:
    subject: str
    # The environment variable holding the token; the process takes it out of its environment.
    token_env: str
    role: AccessRole = "readonly"
    allowed_projects: tuple[str, ...] = ("*",)
    allow_kb: bool = False


@dataclass(frozen=True, slots=True)
class MemoryHTTPConfig:
    enabled: bool = False
    listen_host: str = "127.0.0.1"
    listen_port: int = 8795
    tokens: tuple[MemoryHTTPToken, ...] = ()


@dataclass(frozen=True, slots=True)
class MemoryHubLinkConfig:
    default_budget: BaselineBudgetConfig = field(default_factory=BaselineBudgetConfig)
    project_budgets: dict[str, BaselineBudgetConfig] = field(default_factory=dict[str, BaselineBudgetConfig])
    mounts: tuple[KnowledgeMountConfig, ...] = ()
    # inbox_id -> project_key, for inboxes whose id does not name their project.
    inbox_projects: dict[str, str] = field(default_factory=dict[str, str])
    index_dir: str = ""
    max_note_bytes: int = 16_384
    proposal_ttl_days: int = 14
    progress_archive_days: int = 7
    auto_git_commit: bool = False
    # Mechanical GC of every project's memory; 0 turns it off.
    gc_interval_seconds: float = DEFAULT_GC_INTERVAL_SECONDS
    http: MemoryHTTPConfig = field(default_factory=MemoryHTTPConfig)

    def hub_config(self, memory_root: str | Path) -> MemoryHubConfig:
        return MemoryHubConfig(
            memory_root=str(memory_root),
            default_budget=self.default_budget,
            project_budgets=self.project_budgets,
            inbox_project_map=self.inbox_projects,
            mounts=self.mounts,
            index_dir=self.index_dir or None,
            max_note_bytes=self.max_note_bytes,
            proposal_ttl_days=self.proposal_ttl_days,
            progress_archive_days=self.progress_archive_days,
            auto_git_commit=self.auto_git_commit,
        )


@dataclass(frozen=True, slots=True)
class CollaborationTaskboardConfig:
    # Taskboard's MCP endpoint, for follow-up tasks of confirmed closeouts and the digest; "" leaves both without it.
    mcp_url: str = ""
    token_env: str = "HIBERNAUT_TASKBOARD_MCP_TOKEN"


@dataclass(frozen=True, slots=True)
class CollaborationHubConfig:
    enabled: bool = False
    listen_host: str = "127.0.0.1"
    listen_port: int = 8796
    # The path prefix below which a reverse proxy serves it; "" when it has a host name of its own.
    base_path: str = ""
    # The environment variable holding the Owner's token; the process takes it out of its environment.
    token_env: str = "HIBERNAUT_COLLABORATION_TOKEN"
    # Who approvals and confirmations made with that token are recorded as.
    reviewer: str = "owner"


@dataclass(frozen=True, slots=True)
class CollaborationConfig:
    enabled: bool = False
    # Where the Chief of Staff hears of confirmed closeouts: one exact [[runtime.inbox_bindings]] inbox.
    chief_inbox: str = "user:main"
    # Startup memory of one drafted expert, shared out over its mounts.
    expert_budget_chars: int = 6500
    taskboard: CollaborationTaskboardConfig = field(default_factory=CollaborationTaskboardConfig)
    hub: CollaborationHubConfig = field(default_factory=CollaborationHubConfig)
    experts: tuple[ExpertProfile, ...] = ()


@dataclass(frozen=True, slots=True)
class RunnerPilotConfig:
    # One task inbox checked by one standalone Runner (docs/runner.md §8); off unless a deployment turns it on.
    enabled: bool = False
    inbox_id: str = ""
    runner_id: str = ""
    project_key: str = ""
    # The Relay listens on 127.0.0.1 only; the address is not configurable.
    port: int = 8791
    token_file: str = ""
    timeout_seconds: float = 120.0


@dataclass(frozen=True, slots=True)
class HibernautConfig:
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    channels: ChannelsConfig = field(default_factory=ChannelsConfig)
    executors: dict[str, CLITemplate] = field(default_factory=dict)
    launchers: dict[str, ClaudeCodeLauncher] = field(default_factory=dict)
    credential_proxy: CredentialProxyConfig = field(default_factory=CredentialProxyConfig)
    roles: dict[str, RoleConfig] = field(default_factory=dict)
    # Every agent an inbox can run: [agents], plus [executors] that carry a role or a command (the old form).
    agents: dict[str, AgentConfig] = field(default_factory=dict)
    # max_concurrency of the [executors.<id>] that are neither, and only run through agents.
    driver_limits: dict[str, int] = field(default_factory=dict)
    projects: dict[str, ProjectConfig] = field(default_factory=dict)
    gdrive: GDriveConfig = field(default_factory=GDriveConfig)
    switchboard: SwitchboardLinkConfig = field(default_factory=SwitchboardLinkConfig)
    peer: PeerHandoffConfig = field(default_factory=PeerHandoffConfig)
    runner: RunnerLinkConfig = field(default_factory=RunnerLinkConfig)
    memory_hub: MemoryHubLinkConfig = field(default_factory=MemoryHubLinkConfig)
    collaboration: CollaborationConfig = field(default_factory=CollaborationConfig)
    runner_pilot: RunnerPilotConfig = field(default_factory=RunnerPilotConfig)
    onboarding: OnboardingConfig = field(default_factory=OnboardingConfig)
    availability: AvailabilityConfig = field(default_factory=AvailabilityConfig)
    # The directory relative paths were resolved against (the config file's); "" when parsed without one.
    config_dir: str = ""
    timers: tuple[ConfiguredTimer, ...] = ()
    # Feishu groups whose listed people talk to a system inbox directly.
    feishu_groups: tuple[FeishuGroup, ...] = ()

    def expert_plan(self, inbox_id: str) -> MemoryMountPlan | None:
        return next((e.plan for p in self.projects.values() for e in p.expert_bindings if e.rule.matches(inbox_id)), None)


def _parse_timers(raw: object, runtime: RuntimeConfig, projects: Mapping[str, ProjectConfig]) -> tuple[ConfiguredTimer, ...]:
    if not isinstance(raw, list):
        raise ValueError("timers must be an array of tables")
    result: dict[str, ConfiguredTimer] = {}
    router = ProjectRouter(projects.values())
    for i, item in enumerate(raw):
        where = f"timers[{i}]"
        if not isinstance(item, dict) or item.keys() - {"name", "inbox_id", "interval_seconds", "enabled", "payload"}:
            raise ValueError(f"{where} must be a timer table with known keys")
        name = item.get("name")
        if not isinstance(name, str) or not name.strip() or name != name.strip() or name in result:
            raise ValueError(f"{where}.name must be a unique, non-blank name without surrounding whitespace")
        inbox = item.get("inbox_id")
        if not isinstance(inbox, str):
            raise ValueError(f"{where}.inbox_id must be a string")
        sanitize_segment(inbox, field_name=f"{where}.inbox_id")
        bound = runtime.workdir_binding(inbox)
        if bound and bound.system_tools:
            raise ValueError(f"{where}: {inbox} has system_tools")
        project = router.resolve(inbox)
        if not bound and (not project or (project.inbox_bindings and not project.route(inbox).binding)):
            raise ValueError(f"{where}.inbox_id {inbox!r} has no binding")
        interval = item.get("interval_seconds")
        if type(interval) is not int or not 1 <= interval <= 2**31 - 1:
            raise ValueError(f"{where}.interval_seconds must be an integer from 1 to 2147483647")
        enabled = item.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValueError(f"{where}.enabled must be true or false")
        payload = item.get("payload", {})
        if not isinstance(payload, dict):
            raise ValueError(f"{where}.payload must be a table")
        try:
            json.dumps(payload, allow_nan=False)
        except (ValueError, TypeError) as exc:
            raise ValueError(f"{where}.payload must contain JSON values") from exc
        result[name] = ConfiguredTimer(name, inbox, interval, payload, enabled)
    return tuple(result.values())


_FEISHU_GROUP_KEYS = frozenset({"chat_id", "inbox_id", "members"})


def _parse_feishu_groups(raw: object, runtime: RuntimeConfig) -> tuple[FeishuGroup, ...]:
    if not isinstance(raw, list):
        raise ValueError("feishu_groups must be an array of tables")
    found: dict[str, FeishuGroup] = {}
    for i, item in enumerate(cast(list[object], raw)):
        where = f"feishu_groups[{i}]"
        if not isinstance(item, dict) or cast(dict[str, Any], item).keys() - _FEISHU_GROUP_KEYS:
            raise ValueError(f"{where} must be a table with the known keys {', '.join(sorted(_FEISHU_GROUP_KEYS))}")
        group = cast(dict[str, Any], item)
        chat_id = group.get("chat_id")
        if not isinstance(chat_id, str) or not chat_id.strip() or chat_id != chat_id.strip() or chat_id in found:
            raise ValueError(f"{where}.chat_id must be a unique, non-blank Feishu chat id")
        inbox = group.get("inbox_id")
        bound = runtime.workdir_binding(inbox) if isinstance(inbox, str) else None
        # Only a system inbox, which admits nothing else from a chat: an ordinary one would take anybody's message.
        if bound is None or not bound.system_tools or bound.rule.literal != inbox:
            raise ValueError(f"{where}.inbox_id must be the exact pattern of a [[runtime.inbox_bindings]] entry with "
                             f"system_tools, got {inbox!r}")
        members = group.get("members")
        names = cast(list[object], members) if isinstance(members, list) else []
        if not names or not all(isinstance(m, str) and m.strip() and m == m.strip() for m in names):
            raise ValueError(f"{where}.members must list the Feishu open ids (ou_...) whose messages count")
        if any(other.inbox_id == inbox for other in found.values()):
            # Its approval cards go to that one group, and a decision counts only from there.
            raise ValueError(f"{where}.inbox_id {inbox!r} already has a group: a system inbox takes one group")
        found[chat_id] = FeishuGroup(chat_id=chat_id, inbox_id=inbox, members=frozenset(cast(list[str], names)))
    return tuple(found.values())


def _resolve_path(raw: str, base_dir: Path | None) -> str:
    resolved = Path(raw).expanduser()
    if not resolved.is_absolute() and base_dir is not None:
        resolved = base_dir / resolved
    return str(resolved)


def _project_checkout(name: str, item: Mapping[str, Any], repo: str) -> str:
    checkout = str(item.get("checkout") or "").strip()
    if checkout:
        return checkout
    try:
        return registered_checkout(repo)
    except ValueError as exc:
        raise ValueError(f"projects.{name}: checkout is not set, and Orca has none for it: {exc}") from exc


def _outbox_dir(raw: Any, base_dir: Path | None) -> str:
    if raw is None:
        return ""
    if not isinstance(raw, str):
        raise ValueError("channels.outbox_dir must be a path string")
    return _resolve_path(raw.strip(), base_dir) if raw.strip() else ""


def _switchboard_url(section: dict[str, Any], key: str, schemes: tuple[str, ...]) -> str | None:
    raw = str(section.get(key) or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    if parsed.scheme not in schemes or not parsed.hostname:
        raise ValueError(f"switchboard.{key} must be a {' or '.join(s + '://' for s in schemes)} URL")
    return raw


_PEER_KEYS = frozenset({"receiver", "state_dir", "config_dirs", "max_resends", "register_timeout_seconds",
                        "exit_timeout_seconds"})


def parse_peer(raw: Any, *, base_dir: Path | None = None) -> PeerHandoffConfig:
    if raw is None:
        return PeerHandoffConfig()
    if not isinstance(raw, dict):
        raise ValueError("[peer] must be a table")
    item = cast(dict[str, Any], raw)
    unknown = sorted(set(item) - _PEER_KEYS)
    if unknown:
        raise ValueError(f"[peer] has unknown keys: {', '.join(unknown)}")
    defaults = PeerHandoffConfig()
    receiver = item.get("receiver", defaults.receiver)
    if not isinstance(receiver, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", receiver):
        raise ValueError("peer.receiver must be a ccpeer receiver name (letters, digits, '_' and '-')")
    state_raw = item.get("state_dir")
    if state_raw is not None and (not isinstance(state_raw, str) or not state_raw.strip()):
        raise ValueError("peer.state_dir must be a path string")
    dirs_raw = item.get("config_dirs")
    if dirs_raw is not None and (not isinstance(dirs_raw, list) or not dirs_raw
                                 or not all(isinstance(d, str) and d.strip() for d in cast(list[object], dirs_raw))):
        raise ValueError("peer.config_dirs must be a non-empty list of paths")

    def number(key: str, default: float) -> float:
        value = item.get(key, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError(f"peer.{key} must be a positive number")
        return float(value)

    resends = item.get("max_resends", defaults.max_resends)
    if isinstance(resends, bool) or not isinstance(resends, int) or resends < 0:
        raise ValueError("peer.max_resends must be a non-negative integer")
    return PeerHandoffConfig(
        receiver=receiver,
        state_dir=Path(_resolve_path(state_raw.strip(), base_dir)) if isinstance(state_raw, str) else None,
        config_dirs=(tuple(Path(_resolve_path(str(d).strip(), base_dir)) for d in cast(list[object], dirs_raw))
                     if dirs_raw is not None else defaults.config_dirs),
        max_resends=resends,
        register_timeout=number("register_timeout_seconds", defaults.register_timeout),
        exit_timeout=number("exit_timeout_seconds", defaults.exit_timeout),
    )


def parse_switchboard_link(raw: Any, *, base_dir: Path | None = None) -> SwitchboardLinkConfig:
    if raw is None:
        return SwitchboardLinkConfig()
    if not isinstance(raw, dict):
        raise ValueError("[switchboard] must be a table")
    section = cast(dict[str, Any], raw)
    enabled = section.get("enabled", False)
    if not isinstance(enabled, bool):
        raise ValueError("switchboard.enabled must be true or false")
    mode_raw = str(section.get("mode", "in_process")).strip()
    mode = next((m for m in _SWITCHBOARD_MODES if m == mode_raw), None)
    if mode is None:
        raise ValueError('switchboard.mode must be "in_process" or "relay"')
    consumer_id = str(section.get("consumer_id", "hibernaut")).strip()
    if not consumer_id:
        raise ValueError("switchboard.consumer_id must not be empty")
    path_raw = str(section.get("config_path") or "").strip()
    config_path = _resolve_path(path_raw, base_dir) if path_raw else None
    relay_url = _switchboard_url(section, "relay_url", ("ws", "wss"))
    mcp_url = _switchboard_url(section, "mcp_url", ("http", "https"))
    ca_raw = str(section.get("ca_file") or "").strip()
    ca_file = _resolve_path(ca_raw, base_dir) if ca_raw else None
    if enabled and mode == "in_process" and config_path is None:
        raise ValueError("switchboard.enabled needs switchboard.config_path (the switchboard.toml to run in process)")
    if enabled and mode == "relay" and (relay_url is None or mcp_url is None):
        raise ValueError('switchboard.mode = "relay" needs switchboard.relay_url and switchboard.mcp_url')
    return SwitchboardLinkConfig(
        enabled=enabled,
        mode=mode,
        config_path=config_path,
        consumer_id=consumer_id,
        relay_url=relay_url,
        mcp_url=mcp_url,
        ca_file=ca_file,
    )


_PILOT_KEYS = frozenset({"enabled", "inbox_id", "runner_id", "project_key", "port", "token_file", "timeout_seconds"})
# <project>:<task>: a user's or a project's main inbox would put ordinary work on the pilot route.
_TASK_INBOX_RE = re.compile(r"(?!user:|proj:)[^\s:]+:[^\s:]+")


def parse_runner_pilot(raw: Any, *, base_dir: Path | None = None) -> RunnerPilotConfig:
    defaults = RunnerPilotConfig()
    if raw is None:
        return defaults
    if not isinstance(raw, dict):
        raise ValueError("[runner_pilot] must be a table")
    section = cast(dict[str, Any], raw)
    unknown = sorted(set(section) - _PILOT_KEYS)
    if unknown:
        raise ValueError(f"[runner_pilot] has unknown keys: {', '.join(unknown)}")
    enabled = section.get("enabled", False)
    if not isinstance(enabled, bool):
        raise ValueError("runner_pilot.enabled must be true or false")
    texts: dict[str, str] = {}
    for key in ("inbox_id", "runner_id", "project_key", "token_file"):
        value = section.get(key, "")
        if not isinstance(value, str) or value != value.strip() or (enabled and not value):
            raise ValueError(f"runner_pilot.{key} must be a non-empty string when the route is enabled")
        texts[key] = value
    port = section.get("port", defaults.port)
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError("runner_pilot.port must be a port number")
    timeout = section.get("timeout_seconds", defaults.timeout_seconds)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout <= 600:
        raise ValueError("runner_pilot.timeout_seconds must be a number above 0 and at most 600")
    if enabled and _TASK_INBOX_RE.fullmatch(texts["inbox_id"]) is None:
        raise ValueError('runner_pilot.inbox_id must be one task inbox ("<project>:<task>", not "proj:" or "user:")')
    return RunnerPilotConfig(
        enabled=enabled,
        inbox_id=texts["inbox_id"],
        runner_id=texts["runner_id"],
        project_key=texts["project_key"],
        port=port,
        token_file=_resolve_path(texts["token_file"], base_dir) if texts["token_file"] else "",
        timeout_seconds=float(timeout),
    )


_OWNER_RE = re.compile(r"[a-z0-9][a-z0-9-]*")


def parse_onboarding(raw: Any) -> OnboardingConfig:
    section = _Table(raw if raw is not None else {}, "onboarding", frozenset({"label_owners"}))
    owners = tuple(dict.fromkeys(owner.lower() for owner in section.names("label_owners", ())))
    for owner in owners:
        if not _OWNER_RE.fullmatch(owner):
            raise ValueError(f"onboarding.label_owners: {owner!r} is not a GitHub owner")
    return OnboardingConfig(label_owners=owners)


def _validate_runner_pilot(config: HibernautConfig) -> None:
    pilot = config.runner_pilot
    if not pilot.enabled:
        return
    if pilot.inbox_id == config.gdrive.default_inbox_id:
        raise ValueError("runner_pilot.inbox_id is gdrive.default_inbox_id; use a task inbox of its own")
    for project in config.projects.values():
        if project.inbox_id == pilot.inbox_id:
            raise ValueError(
                f"runner_pilot.inbox_id is the primary inbox of projects.{project.name}; use a task inbox of its own"
            )
    if config.runtime.default_agent not in config.agents:
        raise ValueError("runner_pilot.enabled needs runtime.default_agent to be a declared agent (no run starts without it)")
    if pilot.port == config.channels.port:
        raise ValueError("runner_pilot.port is channels.port")
    if config.runner.enabled and config.runner.mode == "relay" and pilot.port == config.runner.listen_port:
        raise ValueError("runner_pilot.port is runner.listen_port; each relay needs its own port")
    if config.runner.enabled and config.runner.mode == "relay" and pilot.token_file == config.runner.token_file:
        raise ValueError("runner_pilot.token_file is runner.token_file; each relay needs its own token")
    if config.memory_hub.http.enabled and pilot.port == config.memory_hub.http.listen_port:
        raise ValueError("runner_pilot.port is memory_hub.http.listen_port")


def tool_hook_style(agent: AgentConfig, template: CLITemplate) -> ToolHookStyle | None:
    # By the CLI it starts; an executor whose command is a wrapper is recognized by its own name.
    named = (Path(template.command).name, agent.executor)
    return next((style for style in _TOOL_HOOK_STYLES if style in named), None)


def runner_driver(agent: AgentConfig, template: CLITemplate) -> TemplateCLIDriver:
    style = tool_hook_style(agent, template)
    return TemplateCLIDriver(
        executor_id=agent.name,
        driver_kind=style or "custom",
        command=template.command,
        args=template.args,
        resume_args=tuple(a.replace("{executor_session_id}", "{native_session_id}") for a in template.resume_args),
        env=dict(template.env),
        # Claude's json and stream-json output both end in a result object; plain text has no such line.
        supports_stream_json=True,
        calls_tools=True,
        tool_hook=style,
    )


def parse_runner_link(
    raw: Any, *, agents: Mapping[str, AgentConfig], templates: Mapping[str, CLITemplate], base_dir: Path | None = None,
) -> RunnerLinkConfig:
    if raw is None:
        return RunnerLinkConfig()
    if not isinstance(raw, dict):
        raise ValueError("[runner] must be a table")
    item = cast(dict[str, Any], raw)
    unknown = sorted(set(item) - _RUNNER_KEYS)
    if unknown:
        raise ValueError(f"[runner] has unknown keys: {', '.join(unknown)}")
    defaults = RunnerLinkConfig()
    enabled = item.get("enabled", False)
    if not isinstance(enabled, bool):
        raise ValueError("runner.enabled must be true or false")
    readonly_only = item.get("readonly_only", False)
    if not isinstance(readonly_only, bool):
        raise ValueError("runner.readonly_only must be true or false")
    if "token_env" in item and "token_file" in item:
        raise ValueError("runner.token_env and runner.token_file cannot both be set")
    mode = next((m for m in _RUNNER_MODES if m == item.get("mode", defaults.mode)), None)
    if mode is None:
        raise ValueError('runner.mode must be "in_process" or "relay"')
    backend = item.get("worktree_backend", defaults.worktree_backend)
    if backend not in ("git", "orca"):
        raise ValueError('runner.worktree_backend must be "git" or "orca"')

    def seconds(key: str, default: float, *, zero: bool = False) -> float:
        value = item.get(key, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or (
            value < 0 or (value == 0 and not zero)
        ):
            raise ValueError(f"runner.{key} must be a {'non-negative' if zero else 'positive'} number")
        return float(value)

    def text(key: str, default: str) -> str:
        value = item.get(key, default)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"runner.{key} must be a non-empty string")
        return value.strip()

    def path(key: str) -> str:
        return _resolve_path(text(key, ""), base_dir) if key in item else ""

    def names(key: str) -> tuple[str, ...]:
        value = item.get(key, [])
        if not isinstance(value, list) or not all(isinstance(v, str) and v.strip() for v in cast(list[object], value)):
            raise ValueError(f"runner.{key} must be a list of names")
        return tuple(dict.fromkeys(str(v).strip() for v in cast(list[object], value)))

    slots = item.get("max_concurrent")
    if slots is not None and (isinstance(slots, bool) or not isinstance(slots, int) or slots < 1):
        raise ValueError("runner.max_concurrent must be a positive integer")
    port = item.get("listen_port", defaults.listen_port)
    if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65535:
        raise ValueError("runner.listen_port must be a port number (0 picks a free one)")

    gateable = [n for n, t in templates.items() if n in agents and tool_hook_style(agents[n], t) is not None]
    executors = names("executors") if "executors" in item else tuple(gateable)
    if enabled:
        if not executors:
            raise ValueError("runner.executors names no CLI executor to run (claude, grok and cursor-agent have a tool hook)")
        for name in executors:
            if name not in templates or name not in agents:
                reason = "has a role and launches itself" if name in agents else "is not a declared CLI executor"
                raise ValueError(f"runner.executors: '{name}' {reason}")
            if mode == "in_process":
                driver = runner_driver(agents[name], templates[name])  # raises when its arguments override the hook
                if driver.tool_hook is None:
                    raise ValueError(f"runner.executors: '{name}' has no tool hook the runner can authorize its calls with")
    return RunnerLinkConfig(
        enabled=enabled,
        mode=mode,
        executors=executors,
        timeout_seconds=seconds("timeout_seconds", defaults.timeout_seconds),
        node_wait_seconds=seconds("node_wait_seconds", defaults.node_wait_seconds, zero=True),
        allow_tools=frozenset(names("allow_tools")),
        block_tools=frozenset(names("block_tools")),
        readonly_only=readonly_only,
        runner_id=text("runner_id", defaults.runner_id),
        max_concurrent=slots,
        worktree_backend=backend,
        state_dir=path("state_dir"),
        logs_dir=path("logs_dir"),
        listen_host=text("listen_host", defaults.listen_host),
        listen_port=port,
        token_env=text("token_env", defaults.token_env),
        token_file=path("token_file"),
        affinity_db_path=path("affinity_db_path"),
    )


def _warn_obsolete(section: dict[str, Any], scope: str, service: str, *keys: str) -> None:
    for key in keys:
        if key in section:
            warnings.warn(
                f"{scope}.{key} is obsolete and ignored. Configure {service} in Switchboard.",
                UserWarning,
                stacklevel=3,
            )


_BUDGET_KEYS = frozenset(f.name for f in dataclasses.fields(BaselineBudgetConfig))
_MOUNT_KEYS = frozenset(f.name for f in dataclasses.fields(KnowledgeMountConfig)) - {"name"}
_MEMORY_HUB_KEYS = frozenset({
    "budget", "projects", "mounts", "inbox_projects", "index_dir", "max_note_bytes", "proposal_ttl_days",
    "progress_archive_days", "auto_git_commit", "gc_interval_seconds", "http",
})
_MEMORY_ROLES: tuple[AccessRole, ...] = ("readonly", "propose", "admin")


class _Table:
    # One table of a section: typed reads that name the offending key.
    def __init__(self, raw: Any, where: str, keys: frozenset[str] | None = None) -> None:
        if not isinstance(raw, dict):
            raise ValueError(f"[{where}] must be a table")
        self.raw = cast(dict[str, Any], raw)
        self.where = where
        unknown = sorted(set(self.raw) - keys) if keys is not None else []
        if unknown:
            raise ValueError(f"[{where}] has unknown keys: {', '.join(unknown)}")

    def table(self, key: str, keys: frozenset[str] | None = None) -> _Table:
        return _Table(self.raw.get(key, {}), f"{self.where}.{key}", keys)

    def tables(self, keys: frozenset[str] | None = None) -> dict[str, _Table]:
        return {str(name): self.table(name, keys) for name in self.raw}

    def text(self, key: str, default: str | None = None) -> str:
        value = self.raw.get(key, default)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{self.where}.{key} must be a non-empty string")
        return value.strip()

    def path(self, key: str, base_dir: Path | None) -> str:
        return _resolve_path(self.text(key), base_dir)

    def flag(self, key: str, default: bool) -> bool:
        value = self.raw.get(key, default)
        if not isinstance(value, bool):
            raise ValueError(f"{self.where}.{key} must be true or false")
        return value

    def count(self, key: str, default: int, *, least: int = 1) -> int:
        value = self.raw.get(key, default)
        if isinstance(value, bool) or not isinstance(value, int) or value < least:
            raise ValueError(f"{self.where}.{key} must be an integer of at least {least}")
        return value

    def names(self, key: str, default: tuple[str, ...]) -> tuple[str, ...]:
        value = self.raw.get(key, list(default))
        if not isinstance(value, list) or not all(isinstance(v, str) and v.strip() for v in cast(list[object], value)):
            raise ValueError(f"{self.where}.{key} must be a list of strings")
        return tuple(str(v).strip() for v in cast(list[object], value))


def _segment(value: str, where: str) -> str:
    # Project keys and mount names are path and URI segments.
    if sanitize_segment(value, field_name=where) != value:
        raise ValueError(f"{where}: '{value}' may only hold letters, digits, '.', '_' and '-'")
    return value


def _memory_budget(table: _Table, base: BaselineBudgetConfig) -> BaselineBudgetConfig:
    return dataclasses.replace(base, **{key: table.count(key, getattr(base, key)) for key in _BUDGET_KEYS})


def _memory_mount(name: str, table: _Table, base_dir: Path | None) -> KnowledgeMountConfig:
    defaults = KnowledgeMountConfig(name=name, root="")
    if table.text("mode", defaults.mode) != "readonly":
        raise ValueError(f'{table.where}.mode must be "readonly"')
    return KnowledgeMountConfig(
        name=_segment(name, table.where),
        root=table.path("root", base_dir),
        include_globs=table.names("include_globs", defaults.include_globs),
        exclude_globs=table.names("exclude_globs", defaults.exclude_globs),
        max_file_bytes=table.count("max_file_bytes", defaults.max_file_bytes),
    )


def _memory_http(table: _Table) -> MemoryHTTPConfig:
    defaults = MemoryHTTPConfig()
    tokens: list[MemoryHTTPToken] = []
    entries = table.table("tokens").tables(frozenset({"token_env", "role", "allowed_projects", "allow_kb"}))
    for subject, entry in entries.items():
        role = next((r for r in _MEMORY_ROLES if r == entry.raw.get("role", "readonly")), None)
        if role is None:
            raise ValueError(f"{entry.where}.role must be one of {', '.join(_MEMORY_ROLES)}")
        projects = entry.names("allowed_projects", ("*",))
        if role == "admin" and "*" not in projects:
            # The service lets an admin approve any proposal and collect any project.
            raise ValueError(f"{entry.where}: role \"admin\" reaches every project; leave allowed_projects out")
        tokens.append(MemoryHTTPToken(
            subject=subject, token_env=entry.text("token_env"), role=role, allowed_projects=projects,
            allow_kb=entry.flag("allow_kb", False),
        ))
    enabled = table.flag("enabled", False)
    if enabled and not tokens:
        raise ValueError(f"{table.where}.enabled needs at least one [{table.where}.tokens.<subject>]")
    return MemoryHTTPConfig(
        enabled=enabled, listen_host=table.text("listen_host", defaults.listen_host),
        listen_port=_port(table, defaults.listen_port),
        tokens=tuple(tokens),
    )


def parse_memory_hub(raw: Any, *, base_dir: Path | None = None) -> MemoryHubLinkConfig:
    if raw is None:
        return MemoryHubLinkConfig()
    defaults = MemoryHubLinkConfig()
    section = _Table(raw, "memory_hub", _MEMORY_HUB_KEYS)
    budget = _memory_budget(section.table("budget", _BUDGET_KEYS), defaults.default_budget)
    interval = section.raw.get("gc_interval_seconds", defaults.gc_interval_seconds)
    if isinstance(interval, bool) or not isinstance(interval, (int, float)) or not math.isfinite(interval) or interval < 0:
        raise ValueError("memory_hub.gc_interval_seconds must be finite and not negative (0 turns it off)")
    inbox_projects = section.table("inbox_projects")
    return MemoryHubLinkConfig(
        default_budget=budget,
        project_budgets={
            _segment(key, table.where): _memory_budget(table.table("budget", _BUDGET_KEYS), budget)
            for key, table in section.table("projects").tables(frozenset({"budget"})).items()
        },
        mounts=tuple(
            _memory_mount(name, table, base_dir)
            for name, table in section.table("mounts").tables(_MOUNT_KEYS).items()
        ),
        inbox_projects={
            str(inbox): _segment(inbox_projects.text(inbox), f"memory_hub.inbox_projects.{inbox}")
            for inbox in inbox_projects.raw
        },
        index_dir=section.path("index_dir", base_dir) if "index_dir" in section.raw else "",
        max_note_bytes=section.count("max_note_bytes", defaults.max_note_bytes),
        proposal_ttl_days=section.count("proposal_ttl_days", defaults.proposal_ttl_days),
        progress_archive_days=section.count("progress_archive_days", defaults.progress_archive_days),
        auto_git_commit=section.flag("auto_git_commit", defaults.auto_git_commit),
        gc_interval_seconds=float(interval),
        http=_memory_http(section.table("http", frozenset({"enabled", "listen_host", "listen_port", "tokens"}))),
    )


def _port(table: _Table, default: int) -> int:
    port = table.count("listen_port", default, least=0)
    if port > 65535:
        raise ValueError(f"{table.where}.listen_port must be a port number (0 picks a free one)")
    return port


def parse_collaboration(
    raw: Any, experts_raw: Any, *, agents: Mapping[str, AgentConfig]
) -> CollaborationConfig:
    if raw is None:
        if experts_raw is not None:
            raise ValueError("[experts] needs [collaboration] enabled = true")
        return CollaborationConfig()
    section = _Table(raw, "collaboration", frozenset({"enabled", "chief_inbox", "expert_budget_chars", "taskboard", "hub"}))
    if not section.flag("enabled", False):
        if experts_raw is not None:
            raise ValueError("[experts] needs [collaboration] enabled = true")
        return CollaborationConfig()
    defaults = CollaborationConfig()
    board = section.table("taskboard", frozenset({"mcp_url", "token_env"}))
    mcp_url = board.text("mcp_url") if "mcp_url" in board.raw else ""
    if mcp_url and not mcp_url.startswith(("http://", "https://")):
        raise ValueError(f"{board.where}.mcp_url must be an http(s) URL")
    hub = section.table("hub", frozenset({"enabled", "listen_host", "listen_port", "base_path", "token_env", "reviewer"}))
    base_path = hub.text("base_path") if hub.raw.get("base_path", "") != "" else ""
    if base_path and not re.fullmatch(r"(/[A-Za-z0-9._-]+)+", base_path):
        raise ValueError(f"{hub.where}.base_path must be a path such as \"/hub\", without a slash at the end")
    return CollaborationConfig(
        enabled=True,
        chief_inbox=section.text("chief_inbox", defaults.chief_inbox),
        expert_budget_chars=section.count("expert_budget_chars", defaults.expert_budget_chars, least=1000),
        taskboard=CollaborationTaskboardConfig(
            mcp_url=mcp_url, token_env=board.text("token_env", defaults.taskboard.token_env)
        ),
        hub=CollaborationHubConfig(
            enabled=hub.flag("enabled", False),
            listen_host=hub.text("listen_host", defaults.hub.listen_host),
            listen_port=_port(hub, defaults.hub.listen_port),
            base_path=base_path,
            token_env=hub.text("token_env", defaults.hub.token_env),
            reviewer=hub.text("reviewer", defaults.hub.reviewer),
        ),
        experts=_parse_experts({} if experts_raw is None else experts_raw, agents),
    )


def _parse_experts(raw: Any, agents: Mapping[str, AgentConfig]) -> tuple[ExpertProfile, ...]:
    known = {p.role_id: p for p in default_expert_profiles()}
    keys = frozenset({"agent", "display_name", "skills", "quality_gates", "max_concurrency", "allow_direct_channel"})
    experts: list[ExpertProfile] = []
    for role, table in _Table(raw, "experts").tables(keys).items():
        if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", role):
            raise ValueError(f"experts.{role}: a role is lower-case letters, digits, '_' and '-'")
        base = known.get(role) or ExpertProfile(
            agent_id=f"pool:{role}", role_id=role, display_name=role, executor_id="", skills=(), quality_gates=()
        )
        agent = table.text("agent", base.agent_id)
        if agent not in agents:
            raise ValueError(f"experts.{role}: agent '{agent}' is not declared in [agents]")
        taken = next((e.role_id for e in experts if e.agent_id == agent), None)
        if taken is not None:
            raise ValueError(f"experts.{role}: agent '{agent}' already is expert '{taken}'")
        experts.append(dataclasses.replace(
            base, agent_id=agent, executor_id=agents[agent].executor,
            display_name=table.text("display_name", base.display_name),
            skills=table.names("skills", base.skills),
            quality_gates=table.names("quality_gates", base.quality_gates),
            max_concurrency=table.count("max_concurrency", base.max_concurrency),
            allow_direct_channel=table.flag("allow_direct_channel", base.allow_direct_channel),
        ))
    return tuple(experts)


def _draft(
    project: ProjectConfig, item: Mapping[str, Any], collaboration: CollaborationConfig, pool: ExpertPoolRegistry,
    memory_keys: Mapping[str, str | None],
) -> ProjectConfig:
    where = f"projects.{project.name}"
    table = _Table(item, where)
    roles, readonly = (table.names(key, ()) for key in ("experts", "expert_readonly_projects"))
    if not roles and not readonly:
        return project
    if not collaboration.enabled:
        raise ValueError(f"{where}.experts needs [collaboration] enabled = true")
    for key, names in (("experts", roles), ("expert_readonly_projects", readonly)):
        twice = sorted({n for n in names if names.count(n) > 1})
        if twice:
            raise ValueError(f"{where}.{key} lists {', '.join(twice)} twice")
    for name in readonly:
        if name == project.name or memory_keys.get(name) is None:
            raise ValueError(f"{where}.expert_readonly_projects: '{name}' is not another project whose primary inbox_id "
                             "is proj:<project>")
    return dataclasses.replace(project, expert_bindings=draft_experts(
        project.name, roles, inbox_id=project.inbox_id, inbox_rules=project.inbox_rules, manual=project.inbox_bindings,
        readonly_projects=tuple(key for name in readonly if (key := memory_keys[name]) is not None),
        pool=pool, budget_chars=collaboration.expert_budget_chars,
    ))


def _validate_collaboration(config: HibernautConfig) -> None:
    link = config.collaboration
    if not link.enabled:
        return
    chief = next((b for b in config.runtime.inbox_bindings if b.rule.literal == link.chief_inbox), None)
    if chief is None or chief.system_tools:
        raise ValueError(f"collaboration.chief_inbox: {link.chief_inbox!r} is not the exact pattern of a "
                         "[[runtime.inbox_bindings]] entry without system_tools")
    if config.runtime.workdir_binding(link.chief_inbox) is not chief:
        raise ValueError(f"collaboration.chief_inbox: an earlier binding matches {link.chief_inbox!r} first")
    for inbox, key in config.memory_hub.inbox_projects.items():
        plan = config.expert_plan(inbox)
        if plan is not None and plan.project_key != key:
            raise ValueError(f"memory_hub.inbox_projects.{inbox}: the inbox belongs to an expert drafted into "
                             f"'{plan.project_key}'")
    if not link.hub.enabled or not link.hub.listen_port:
        return
    http, pilot = config.memory_hub.http, config.runner_pilot
    for used, port, where in (
        (http.enabled, http.listen_port, "memory_hub.http.listen_port"),
        (pilot.enabled, pilot.port, "runner_pilot.port"),
        (True, config.channels.port, "channels.port"),
    ):
        if used and port == link.hub.listen_port:
            raise ValueError(f"collaboration.hub.listen_port is {where}")


def load_config(path: str | Path) -> HibernautConfig:
    raw = tomllib.loads(Path(path).read_text(encoding="utf-8"))
    return parse_config(raw, base_dir=Path(path).resolve().parent)


async def config_changes(
    path: Path,
    stop: asyncio.Event,
    interval: float = 1.0,
    reloads: ReloadStatus | None = None,
) -> AsyncIterator[HibernautConfig]:
    # What is yielded is settled in `reloads` by whoever activates it.
    previous: bytes | None = None
    missing = False
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
            return
        except TimeoutError:
            pass
        try:
            contents = path.read_bytes()
        except OSError as exc:
            if not missing:
                print(
                    f"[config] Cannot read {path}: {exc.strerror}; keeping current settings.",
                    file=sys.stderr,
                    flush=True,
                )
            missing = True
            continue
        missing = False
        if contents == previous:
            continue
        previous = contents
        version = reloads.seen(contents) if reloads is not None else ""
        error = ""
        config: HibernautConfig | None = None
        try:
            raw = tomllib.loads(contents.decode("utf-8"))
            if not raw:
                raise ValueError("empty configuration")
            # Off the event loop: a project without a checkout is looked up through the Orca command.
            config = await asyncio.to_thread(parse_config, raw, base_dir=path.resolve().parent)
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError) as exc:
            detail = f": {exc}" if type(exc) is ValueError else ""
            error = f"{type(exc).__name__}{detail}"
        # Parsing takes a while, and an editor knows no lock: a version that is gone by now is not used.
        try:
            replaced = path.read_bytes() != contents
        except OSError:
            replaced = True
        if replaced:
            # It may also come back as it was.
            previous = None
            continue
        if config is None:
            print(f"[config] Invalid {path} ({error}); keeping current settings.", file=sys.stderr, flush=True)
            if reloads is not None:
                reloads.settle(version, f"invalid configuration ({error})")
            continue
        yield config
        if reloads is not None and not reloads.settled(version):
            # Dropped, as the file had changed again. It may be back as it was.
            previous = None


def parse_config(raw: dict[str, Any], *, base_dir: Path | None = None) -> HibernautConfig:
    # base_dir anchors relative paths such as role skills; load_config passes the config file's directory.
    rt_raw = raw.get("runtime", {})
    policy = str(rt_raw.get("new_event_policy", "defer"))
    if policy not in ("defer", "fold", "interrupt"):
        raise ValueError(f"Unsupported new_event_policy: {policy}")
    _warn_obsolete(rt_raw, "runtime", "GitHub ingress", "poll_mode", "poll_interval_seconds")

    runtime_default = _default_agent("runtime", rt_raw, "default_executor") or "claude"
    runtime = RuntimeConfig(
        db_path=str(rt_raw.get("db_path", "./hibernaut.db")),
        ledger_db_path=str(rt_raw.get("ledger_db_path", "") or ""),
        memory_root=str(rt_raw.get("memory_root", "./agent_memory")),
        max_concurrency=int(rt_raw.get("max_concurrency", 4)),
        new_event_policy=cast(NewEventPolicy, policy),
        default_agent=runtime_default,
        default_executor=runtime_default,
        worktree_sweep_interval_seconds=float(
            rt_raw.get("worktree_sweep_interval_seconds", DEFAULT_SWEEP_INTERVAL_SECONDS)
        ),
    )

    if runtime.max_concurrency < 1:
        raise ValueError("runtime.max_concurrency must be at least 1")
    sweep = runtime.worktree_sweep_interval_seconds
    if not math.isfinite(sweep) or sweep < 0:
        raise ValueError("runtime.worktree_sweep_interval_seconds must be finite and not negative (0 turns it off)")

    ch_raw = raw.get("channels", {})
    _warn_obsolete(ch_raw, "channels", "GitHub ingress", "github_webhook_secret_env", "github_token_env")
    _warn_obsolete(ch_raw, "channels", "Feishu", "feishu_app_id_env", "feishu_app_secret_env",
                   "feishu_verification_token_env", "feishu_default_chat_id")
    channels = ChannelsConfig(
        host=str(ch_raw.get("host", "127.0.0.1")),
        port=int(ch_raw.get("port", 8080)),
        mcp_enabled=bool(ch_raw.get("mcp_enabled", False)),
        mcp_token_env=str(ch_raw.get("mcp_token_env", "HIBERNAUT_MCP_TOKEN")),
        outbox_dir=_outbox_dir(ch_raw.get("outbox_dir"), base_dir),
    )

    proxy_raw = raw.get("credential_proxy", {})
    credential_proxy = CredentialProxyConfig(
        url=str(proxy_raw.get("url", "")).strip(),
        anthropic_service=str(proxy_raw.get("anthropic_service", "anthropic")).strip(),
    )
    if credential_proxy.url and not credential_proxy.url.startswith(("http://", "https://")):
        raise ValueError("credential_proxy.url must be an http(s) URL")
    if not credential_proxy.anthropic_service:
        raise ValueError("credential_proxy.anthropic_service must not be empty")
    anthropic_url = (
        proxy_service_url(credential_proxy.url, credential_proxy.anthropic_service) if credential_proxy.url else ""
    )
    # Launchers come only from this trusted deployment file; events cannot name a command.
    launchers = parse_launchers(raw.get("launchers", {}))

    for table in ("executors", "agents"):
        if PROTOCOL_CHECK_EXECUTOR_ID in raw.get(table, {}):
            raise ValueError(f"{table}.{PROTOCOL_CHECK_EXECUTOR_ID}: the name is reserved for [runner_pilot]")

    executors: dict[str, CLITemplate] = {}
    roles: dict[str, RoleConfig] = {}
    # Pure drivers: no role and no command, so nothing to run until an agent picks them.
    bases: dict[str, dict[str, Any]] = {}
    base_limits: dict[str, int] = {}
    for name, item in raw.get("executors", {}).items():
        if item.get("role") is not None:
            roles[str(name)] = _parse_role(str(name), item, launchers=launchers, anthropic_url=anthropic_url,
                                           base_dir=base_dir, extra_keys=EXECUTOR_LIMIT_KEYS)
            continue
        if "command" not in item:
            unknown = sorted(set(item) - EXECUTOR_KEYS - EXECUTOR_LIMIT_KEYS)
            if unknown:
                raise ValueError(f"executors.{name}: {', '.join(unknown)} not supported without a role or a command")
            profile_item = {k: v for k, v in item.items() if k not in EXECUTOR_LIMIT_KEYS}
            parse_profile(str(name), profile_item, role=CODE_REVIEWER_ROLE, launchers=launchers,
                          credential_proxy_url=anthropic_url, base_dir=base_dir)
            bases[str(name)] = profile_item
            base_limit = _max_concurrency(f"executors.{name}", item)
            if base_limit is not None:
                base_limits[str(name)] = base_limit
            continue
        env_raw = item.get("env", {})
        resume_raw = item.get("resume_args")
        orca_agent = item.get("orca_agent")
        if orca_agent is not None and (not isinstance(orca_agent, str) or not orca_agent.strip()):
            raise ValueError(f"Invalid orca_agent for '{name}'")
        timeout = float(item.get("timeout_seconds", 300.0))
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError(f"Invalid timeout_seconds for '{name}'")
        cli_terminal = item.get("terminal", "none")
        if cli_terminal not in CLI_TERMINALS:
            raise ValueError(
                f"executors.{name}: unsupported terminal {cli_terminal!r} (supported: {', '.join(CLI_TERMINALS)})"
            )
        executors[str(name)] = CLITemplate(
            name=str(name),
            command=str(item["command"]),
            args=tuple(str(a) for a in item.get("args", ())),
            resume_args=(
                tuple(str(a) for a in resume_raw) if resume_raw is not None else ()
            ),
            env={str(k): str(v) for k, v in env_raw.items()},
            timeout_seconds=timeout,
            orca_agent=orca_agent,
            terminal=cli_terminal,
            max_concurrency=_max_concurrency(f"executors.{name}", item),
            quota_errors=parse_quota_errors(f"executors.{name}", item.get("quota_errors")),
        )

    agents: dict[str, AgentConfig] = {
        name: AgentConfig(name=name, executor=name, role=role_cfg.role, model=role_cfg.profile.model,
                          effort=role_cfg.profile.effort, checkout_access=role_cfg.checkout_access)
        for name, role_cfg in roles.items()
    }
    agents.update({name: AgentConfig(name=name, executor=name) for name in executors})
    legacy = frozenset(agents)
    templates = dict(executors)
    for name, item in raw.get("agents", {}).items():
        agent_id = str(name)
        if not isinstance(item, dict):
            raise ValueError(f"agents.{agent_id} must be a table")
        entry = cast(dict[str, Any], item)
        if agent_id in legacy and entry.get("executor") != agent_id:
            raise ValueError(f"agents.{agent_id}: [executors.{agent_id}] already declares an agent of that name")
        try:
            built = _parse_agent(agent_id, entry, bases=bases, templates=templates, launchers=launchers,
                                 anthropic_url=anthropic_url, base_dir=base_dir)
        except ValueError as exc:
            raise _as_agent_error(agent_id, exc) from exc
        description = str(entry.get("description", "")).strip()
        agent_limit = _max_concurrency(f"agents.{agent_id}", entry)
        if isinstance(built, CLITemplate):
            executors[agent_id] = built
            agents[agent_id] = AgentConfig(name=agent_id, executor=str(entry["executor"]), description=description,
                                           max_concurrency=agent_limit)
        else:
            roles[agent_id] = built
            agents[agent_id] = AgentConfig(
                name=agent_id, executor=str(entry["executor"]), role=built.role, model=built.profile.model,
                effort=built.profile.effort, checkout_access=built.checkout_access, description=description,
                max_concurrency=agent_limit,
            )
    if "default_agent" in rt_raw and runtime.default_agent not in agents:
        raise ValueError(f"runtime.default_agent '{runtime.default_agent}' is not declared in [agents]")

    for name, role_cfg in list(roles.items()):
        if role_cfg.sde is None:
            continue
        source = raw["executors"][name] if name in legacy else raw["agents"][name]
        try:
            options = _resolve_reviewer(name, role_cfg.sde, explicit="reviewer" in source, roles=roles,
                                        executors=executors)
        except ValueError as exc:
            raise (exc if name in legacy else _as_agent_error(name, exc)) from exc
        roles[name] = dataclasses.replace(role_cfg, sde=options)

    collaboration = parse_collaboration(raw.get("collaboration"), raw.get("experts"), agents=agents)
    for profile in collaboration.experts:
        if agents[profile.agent_id].max_concurrency is None:
            agents[profile.agent_id] = dataclasses.replace(
                agents[profile.agent_id], max_concurrency=profile.max_concurrency
            )

    projects: dict[str, ProjectConfig] = {}
    for name, item in raw.get("projects", {}).items():
        _warn_obsolete(item, f"projects.{name}", "GitHub ingress", "github_poll", "poll_labels")
        _warn_obsolete(item, f"projects.{name}", "Feishu", "feishu_chat_id")
        backend = str(item.get("worktree_backend", "orca"))
        if backend not in ("orca", "git"):
            raise ValueError(f"Unsupported worktree_backend for '{name}': {backend}")
        terminal = item.get("terminal_backend", "orca" if backend == "orca" else "none")
        if terminal not in TERMINAL_BACKENDS:
            raise ValueError(
                f"Unsupported terminal_backend for '{name}': {terminal!r} (supported: {', '.join(TERMINAL_BACKENDS)})"
            )
        handoff = item.get("handoff", "mcp")
        if handoff not in HANDOFFS:
            raise ValueError(f"Unsupported handoff for '{name}': {handoff!r} (supported: {', '.join(HANDOFFS)})")
        default_agent = _default_agent(f"projects.{name}", item, "executor")
        if "default_agent" in item and default_agent not in agents:
            raise ValueError(f"projects.{name}.default_agent '{default_agent}' is not declared in [agents]")
        repo = str(item["repo"])
        # Authorization to publish comes only from this deployment file, never from the repository under change.
        delivery = (
            parse_delivery(f"projects.{name}.delivery", item["delivery"], repo=repo) if "delivery" in item else None
        )
        rules = parse_inbox_rules(str(name), item.get("inbox_id"))
        primary = next(r.literal for r in rules if r.literal is not None)
        projects[str(name)] = ProjectConfig(
            name=str(name),
            inbox_id=primary,
            rules=rules,
            repo=repo,
            checkout=_project_checkout(str(name), item, repo),
            worktree_backend=cast(Literal["orca", "git"], backend),
            terminal_backend=terminal,
            orca_create_terminal=bool(item.get("orca_create_terminal", False)),
            handoff=cast(Handoff, handoff),
            default_agent=default_agent,
            delivery=delivery,
            inbox_bindings=parse_inbox_bindings(str(name), item.get("inbox_bindings"), inbox_rules=rules,
                                                declared=agents.keys()),
            agents=tuple(agents.values()),
            max_concurrency=_max_concurrency(f"projects.{name}", item),
            description=_optional_text(f"projects.{name}", item, "description"),
        )
    pool = ExpertPoolRegistry(collaboration.experts)
    memory_keys = {name: project_memory_key(project.inbox_id) for name, project in projects.items()}
    projects = {
        name: _draft(project, raw["projects"][name], collaboration, pool, memory_keys)
        for name, project in projects.items()
    }
    validate_project_rules(projects.values())
    validate_project_bindings(projects.values())
    runtime = dataclasses.replace(runtime, inbox_bindings=parse_workdir_bindings(
        rt_raw.get("inbox_bindings"), declared=agents, resolve=lambda path: _resolve_path(path, base_dir),
        stewards={name: role.workdir for name, role in roles.items() if role.role == STEWARD_ROLE},
        main_inboxes={project.inbox_id: project.name for project in projects.values()},
    ))
    validate_workdir_bindings(runtime.inbox_bindings, projects.values(),
                              {name: role.workdir for name, role in roles.items() if role.workdir})

    for where, limit in (
        *((f"executors.{n}", e.max_concurrency) for n, e in {**executors, **roles}.items()),
        *((f"executors.{n}", value) for n, value in base_limits.items()),
        *((f"agents.{n}", a.max_concurrency) for n, a in agents.items()),
        *((f"projects.{n}", p.max_concurrency) for n, p in projects.items()),
    ):
        if limit is not None and limit > runtime.max_concurrency:
            _log.warning(
                "%s.max_concurrency = %d is above runtime.max_concurrency = %d, which caps it",
                where, limit, runtime.max_concurrency,
            )

    gd_raw = raw.get("gdrive", {})
    gd_interval = float(gd_raw.get("poll_interval_seconds", 30.0))
    if not math.isfinite(gd_interval) or gd_interval <= 0:
        raise ValueError("gdrive.poll_interval_seconds must be finite and positive")
    if "service_account_key_env" in gd_raw:
        raise ValueError(
            "gdrive.service_account_key_env is no longer supported: the service account key belongs to the "
            "external Drive queue proxy (deploy/gdrive-proxy); set gdrive.proxy_url and gdrive.proxy_token_file"
        )
    gd_enabled = gd_raw.get("enabled", False)
    if not isinstance(gd_enabled, bool):
        raise ValueError("gdrive.enabled must be true or false")
    gd_proxy_url = str(gd_raw.get("proxy_url", "")).strip() or credential_proxy.url
    gd_service = str(gd_raw.get("proxy_service", "gdrive-queue")).strip()
    gd_token_raw = str(gd_raw.get("proxy_token_file", "")).strip()
    gd_token_file = ""
    if gd_token_raw:
        token_path = Path(gd_token_raw).expanduser()
        if not token_path.is_absolute() and base_dir is not None:
            token_path = base_dir / token_path
        gd_token_file = str(token_path)
    if gd_proxy_url and not gd_proxy_url.startswith(("http://", "https://")):
        raise ValueError("gdrive.proxy_url must be an http(s) URL")
    if gd_enabled:
        if not gd_proxy_url:
            raise ValueError("gdrive.enabled needs gdrive.proxy_url (or [credential_proxy] url) for the Drive queue proxy")
        if not gd_token_file:
            raise ValueError("gdrive.enabled needs gdrive.proxy_token_file (the local client token for the Drive queue proxy)")
        if not gd_service:
            raise ValueError("gdrive.proxy_service must not be empty")
    gd_attachments_dir = str(gd_raw.get("attachments_dir", "")).strip()
    gdrive = GDriveConfig(
        enabled=gd_enabled,
        proxy_url=gd_proxy_url,
        proxy_service=gd_service,
        proxy_token_file=gd_token_file,
        root_folder_id=str(gd_raw.get("root_folder_id", "")).strip(),
        pending_folder_id=str(gd_raw.get("pending_folder_id", "")).strip(),
        in_progress_folder_id=str(gd_raw.get("in_progress_folder_id", "")).strip(),
        done_folder_id=str(gd_raw.get("done_folder_id", "")).strip(),
        failed_folder_id=str(gd_raw.get("failed_folder_id", "")).strip(),
        attachments_folder_id=str(gd_raw.get("attachments_folder_id", "")).strip(),
        attachments_dir=_resolve_path(gd_attachments_dir, base_dir) if gd_attachments_dir else "",
        poll_interval_seconds=gd_interval,
        default_inbox_id=str(gd_raw.get("default_inbox_id", "default")).strip() or "default",
    )

    config = HibernautConfig(
        timers=_parse_timers(raw.get("timers", []), runtime, projects),
        feishu_groups=_parse_feishu_groups(raw.get("feishu_groups", []), runtime),
        runtime=runtime,
        channels=channels,
        executors=executors,
        launchers=launchers,
        credential_proxy=credential_proxy,
        roles=roles,
        agents=agents,
        driver_limits=base_limits,
        projects=projects,
        gdrive=gdrive,
        switchboard=parse_switchboard_link(raw.get("switchboard"), base_dir=base_dir),
        peer=parse_peer(raw.get("peer"), base_dir=base_dir),
        runner=parse_runner_link(raw.get("runner"), agents=agents, templates=executors, base_dir=base_dir),
        memory_hub=parse_memory_hub(raw.get("memory_hub"), base_dir=base_dir),
        collaboration=collaboration,
        runner_pilot=parse_runner_pilot(raw.get("runner_pilot"), base_dir=base_dir),
        onboarding=parse_onboarding(raw.get("onboarding")),
        availability=parse_availability(raw.get("availability"), base_dir=base_dir),
        config_dir=str(base_dir) if base_dir is not None else "",
    )
    _validate_runner_pilot(config)
    _validate_collaboration(config)
    return config
