from __future__ import annotations

import asyncio
import dataclasses
import math
import re
import sys
import tomllib
import warnings
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import urlparse

from agent_runtime.adapters.delivery import DeliveryConfig, parse_delivery
from agent_runtime.adapters.executor_cli.claude_code import (
    ClaudeCodeLauncher,
    ClaudeCodeProfile,
    parse_launchers,
    parse_profile,
)
from agent_runtime.adapters.executor_cli.executor import CLI_TERMINALS, DEFAULT_CLI_TEMPLATES, CLITemplate
from agent_runtime.adapters.executor_cli.peer_handoff import PeerHandoffConfig
from agent_runtime.adapters.executor_cli.reviewer import CODE_REVIEWER_ROLE
from agent_runtime.adapters.executor_cli.role_skills import ROLE_ALIASES, canonical_role
from agent_runtime.adapters.executor_cli.sde import SDE_OPTION_KEYS, SDE_ROLE, SDEOptions
from agent_runtime.adapters.executor_cli.reclaim_sweep import DEFAULT_SWEEP_INTERVAL_SECONDS
from agent_runtime.adapters.executor_cli.worktree import WorktreeBackend
from agent_runtime.adapters.permissions.security import proxy_service_url
from agent_runtime.app.projects import InboxRule, parse_inbox_rules, validate_project_rules
from agent_runtime.core.models import NewEventPolicy


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    db_path: str = "./hibernaut.db"
    # The run ledger's own SQLite file (runs, audits); "" puts ledger.db beside db_path.
    ledger_db_path: str = ""
    memory_root: str = "./agent_memory"
    max_concurrency: int = 4
    new_event_policy: NewEventPolicy = "defer"
    default_executor: str = "claude"
    # Background reclaim of merged task worktrees a delivery-time reclaim had to skip; 0 turns it off.
    worktree_sweep_interval_seconds: float = DEFAULT_SWEEP_INTERVAL_SECONDS


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
    executor: str | None = None
    # Hibernaut's PR delivery step for finished SDE tasks; None ends a task with local commits.
    delivery: DeliveryConfig | None = None
    # Every rule routing an inbox to this project; empty means just the primary inbox_id.
    rules: tuple[InboxRule, ...] = ()

    @property
    def inbox_rules(self) -> tuple[InboxRule, ...]:
        return self.rules or (InboxRule.exact(self.inbox_id),)

    @property
    def serves_multiple_inboxes(self) -> bool:
        # Such a project shares one checkout between concurrent inboxes, so every writable task needs its own
        # managed worktree.
        rules = self.inbox_rules
        return len(rules) > 1 or any(r.literal is None for r in rules)


ROLES = (CODE_REVIEWER_ROLE, SDE_ROLE)
# Before code-reviewer the review role and its usual executor were both named coder-reviewer.
_LEGACY_REVIEWER = "coder-reviewer"
# Deployment mode shared by every role executor; visible is the old name of interactive.
ROLE_DEPLOYMENT_KEYS = frozenset({"interactive", "visible"})


@dataclass(frozen=True, slots=True)
class RoleConfig:
    role: str
    profile: ClaudeCodeProfile
    sde: SDEOptions | None = None
    # True: every run needs a session a person can join, opened by the project's terminal_backend; without one the
    # run stops with the reason. False: runs are non-interactive.
    interactive: bool = True


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
    reviewer = str(item.get("reviewer", defaults.reviewer)).strip()
    return SDEOptions(review=review, reviewer=reviewer, max_review_rounds=rounds)


def _resolve_reviewer(
    name: str, opts: SDEOptions, *, explicit: bool, roles: dict[str, RoleConfig], executors: dict[str, CLITemplate],
) -> SDEOptions:
    # A reviewer is a code-reviewer role executor or any CLI template, built-in ones included.
    templates = {**DEFAULT_CLI_TEMPLATES, **executors}
    target = opts.reviewer
    if not explicit and target not in roles and target not in templates:
        legacy = roles.get(_LEGACY_REVIEWER)
        if legacy is not None and legacy.role == CODE_REVIEWER_ROLE:
            # Deployments from before the rename keep working without naming their reviewer.
            target = _LEGACY_REVIEWER
    resolved = SDEOptions(review=opts.review, reviewer=target, max_review_rounds=opts.max_review_rounds)
    if not opts.review:
        return resolved
    choices = sorted({n for n, r in roles.items() if r.role == CODE_REVIEWER_ROLE} | set(templates))
    hint = f"choose a {CODE_REVIEWER_ROLE} executor or a CLI executor ({', '.join(choices)}) or set review = false"
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


@dataclass(frozen=True, slots=True)
class HibernautConfig:
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    channels: ChannelsConfig = field(default_factory=ChannelsConfig)
    executors: dict[str, CLITemplate] = field(default_factory=dict)
    launchers: dict[str, ClaudeCodeLauncher] = field(default_factory=dict)
    credential_proxy: CredentialProxyConfig = field(default_factory=CredentialProxyConfig)
    roles: dict[str, RoleConfig] = field(default_factory=dict)
    projects: dict[str, ProjectConfig] = field(default_factory=dict)
    gdrive: GDriveConfig = field(default_factory=GDriveConfig)
    switchboard: SwitchboardLinkConfig = field(default_factory=SwitchboardLinkConfig)
    peer: PeerHandoffConfig = field(default_factory=PeerHandoffConfig)
    # The directory relative paths were resolved against (the config file's); "" when parsed without one.
    config_dir: str = ""


def _resolve_path(raw: str, base_dir: Path | None) -> str:
    resolved = Path(raw).expanduser()
    if not resolved.is_absolute() and base_dir is not None:
        resolved = base_dir / resolved
    return str(resolved)


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


def _warn_obsolete(section: dict[str, Any], scope: str, service: str, *keys: str) -> None:
    for key in keys:
        if key in section:
            warnings.warn(
                f"{scope}.{key} is obsolete and ignored. Configure {service} in Switchboard.",
                UserWarning,
                stacklevel=3,
            )


def load_config(path: str | Path) -> HibernautConfig:
    raw = tomllib.loads(Path(path).read_text(encoding="utf-8"))
    return parse_config(raw, base_dir=Path(path).resolve().parent)


async def config_changes(
    path: Path,
    stop: asyncio.Event,
    interval: float = 1.0,
) -> AsyncIterator[HibernautConfig]:
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
        try:
            raw = tomllib.loads(contents.decode("utf-8"))
            if not raw:
                raise ValueError("empty configuration")
            config = parse_config(raw, base_dir=path.resolve().parent)
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError) as exc:
            detail = f": {exc}" if type(exc) is ValueError else ""
            print(
                f"[config] Invalid {path} ({type(exc).__name__}{detail}); keeping current settings.",
                file=sys.stderr,
                flush=True,
            )
            continue
        yield config


def parse_config(raw: dict[str, Any], *, base_dir: Path | None = None) -> HibernautConfig:
    # base_dir anchors relative paths such as role skills; load_config passes the config file's directory.
    rt_raw = raw.get("runtime", {})
    policy = str(rt_raw.get("new_event_policy", "defer"))
    if policy not in ("defer", "fold", "interrupt"):
        raise ValueError(f"Unsupported new_event_policy: {policy}")
    _warn_obsolete(rt_raw, "runtime", "GitHub ingress", "poll_mode", "poll_interval_seconds")

    runtime = RuntimeConfig(
        db_path=str(rt_raw.get("db_path", "./hibernaut.db")),
        ledger_db_path=str(rt_raw.get("ledger_db_path", "") or ""),
        memory_root=str(rt_raw.get("memory_root", "./agent_memory")),
        max_concurrency=int(rt_raw.get("max_concurrency", 4)),
        new_event_policy=cast(NewEventPolicy, policy),
        default_executor=str(rt_raw.get("default_executor", "claude")),
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

    executors: dict[str, CLITemplate] = {}
    roles: dict[str, RoleConfig] = {}
    for name, item in raw.get("executors", {}).items():
        raw_role = item.get("role")
        if raw_role is not None:
            if raw_role not in ROLES and raw_role not in ROLE_ALIASES:
                raise ValueError(f"executors.{name}: unsupported role '{raw_role}' (supported: {', '.join(ROLES)})")
            role = canonical_role(str(raw_role))
            extra = (SDE_OPTION_KEYS if role == SDE_ROLE else frozenset[str]()) | ROLE_DEPLOYMENT_KEYS
            profile = parse_profile(
                str(name), item, role=str(role), launchers=launchers, credential_proxy_url=anthropic_url,
                extra_keys=extra, base_dir=base_dir,
            )
            options = _sde_options(str(name), item) if role == SDE_ROLE else None
            roles[str(name)] = RoleConfig(role=str(role), profile=profile, sde=options,
                                          interactive=_interactive(str(name), item))
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
        )

    for name, role_cfg in list(roles.items()):
        if role_cfg.sde is None:
            continue
        explicit = "reviewer" in raw["executors"][name]
        options = _resolve_reviewer(name, role_cfg.sde, explicit=explicit, roles=roles, executors=executors)
        roles[name] = dataclasses.replace(role_cfg, sde=options)

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
        proj_exec_raw = item.get("executor")
        proj_exec = str(proj_exec_raw).strip() if proj_exec_raw is not None else None
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
            checkout=str(item["checkout"]),
            worktree_backend=cast(Literal["orca", "git"], backend),
            terminal_backend=terminal,
            orca_create_terminal=bool(item.get("orca_create_terminal", False)),
            handoff=cast(Handoff, handoff),
            executor=proj_exec or None,
            delivery=delivery,
        )
    validate_project_rules(projects.values())

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

    return HibernautConfig(
        runtime=runtime,
        channels=channels,
        executors=executors,
        launchers=launchers,
        credential_proxy=credential_proxy,
        roles=roles,
        projects=projects,
        gdrive=gdrive,
        switchboard=parse_switchboard_link(raw.get("switchboard"), base_dir=base_dir),
        peer=parse_peer(raw.get("peer"), base_dir=base_dir),
        config_dir=str(base_dir) if base_dir is not None else "",
    )
