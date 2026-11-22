from __future__ import annotations

import asyncio
import math
import sys
import tomllib
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

from agent_runtime.adapters.channels.github_poller import PollMode
from agent_runtime.adapters.executor_cli.claude_code import (
    ClaudeCodeLauncher,
    ClaudeCodeProfile,
    parse_launchers,
    parse_profile,
)
from agent_runtime.adapters.executor_cli.executor import CLITemplate
from agent_runtime.adapters.executor_cli.reviewer import CODER_REVIEWER_ROLE
from agent_runtime.adapters.executor_cli.sde import SDE_OPTION_KEYS, SDE_ROLE, SDEOptions
from agent_runtime.adapters.executor_cli.worktree import WorktreeBackend
from agent_runtime.adapters.permissions.security import proxy_service_url
from agent_runtime.core.models import NewEventPolicy


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    db_path: str = "./hibernaut.db"
    memory_root: str = "./agent_memory"
    max_concurrency: int = 4
    new_event_policy: NewEventPolicy = "defer"
    default_executor: str = "claude"
    poll_mode: PollMode = "gh"
    poll_interval_seconds: float = 60.0


@dataclass(frozen=True, slots=True)
class ChannelsConfig:
    host: str = "127.0.0.1"
    port: int = 8080
    github_webhook_secret_env: str = "GITHUB_WEBHOOK_SECRET"
    github_token_env: str = "GITHUB_TOKEN"
    feishu_app_id_env: str = "FEISHU_APP_ID"
    feishu_app_secret_env: str = "FEISHU_APP_SECRET"
    feishu_verification_token_env: str = "FEISHU_VERIFICATION_TOKEN"
    feishu_default_chat_id: str = ""
    mcp_enabled: bool = False
    mcp_token_env: str = "HIBERNAUT_MCP_TOKEN"


TerminalBackend = Literal["orca", "none"]
TERMINAL_BACKENDS: tuple[TerminalBackend, ...] = ("orca", "none")


@dataclass(frozen=True, slots=True)
class ProjectConfig:
    name: str
    inbox_id: str
    repo: str
    checkout: str
    poll_labels: tuple[str, ...] = ()
    worktree_backend: WorktreeBackend = "orca"
    # Where interactive role sessions open; "none" makes interactive roles stop with the reason instead.
    terminal_backend: TerminalBackend = "orca"
    orca_create_terminal: bool = False
    feishu_chat_id: str = ""
    executor: str | None = None


ROLES = (CODER_REVIEWER_ROLE, SDE_ROLE)
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
    poll_interval_seconds: float = 30.0
    default_inbox_id: str = "default"


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
    poll_mode = str(rt_raw.get("poll_mode", "gh"))
    if poll_mode not in ("gh", "api"):
        raise ValueError(f"Unsupported poll_mode: {poll_mode}")

    runtime = RuntimeConfig(
        db_path=str(rt_raw.get("db_path", "./hibernaut.db")),
        memory_root=str(rt_raw.get("memory_root", "./agent_memory")),
        max_concurrency=int(rt_raw.get("max_concurrency", 4)),
        new_event_policy=cast(NewEventPolicy, policy),
        default_executor=str(rt_raw.get("default_executor", "claude")),
        poll_mode=cast(PollMode, poll_mode),
        poll_interval_seconds=float(rt_raw.get("poll_interval_seconds", 60.0)),
    )

    if runtime.max_concurrency < 1:
        raise ValueError("runtime.max_concurrency must be at least 1")
    if not math.isfinite(runtime.poll_interval_seconds) or runtime.poll_interval_seconds <= 0:
        raise ValueError("runtime.poll_interval_seconds must be finite and positive")

    ch_raw = raw.get("channels", {})
    channels = ChannelsConfig(
        host=str(ch_raw.get("host", "127.0.0.1")),
        port=int(ch_raw.get("port", 8080)),
        github_webhook_secret_env=str(
            ch_raw.get("github_webhook_secret_env", "GITHUB_WEBHOOK_SECRET")
        ),
        github_token_env=str(ch_raw.get("github_token_env", "GITHUB_TOKEN")),
        feishu_app_id_env=str(ch_raw.get("feishu_app_id_env", "FEISHU_APP_ID")),
        feishu_app_secret_env=str(ch_raw.get("feishu_app_secret_env", "FEISHU_APP_SECRET")),
        feishu_verification_token_env=str(
            ch_raw.get("feishu_verification_token_env", "FEISHU_VERIFICATION_TOKEN")
        ),
        feishu_default_chat_id=str(ch_raw.get("feishu_default_chat_id", "")),
        mcp_enabled=bool(ch_raw.get("mcp_enabled", False)),
        mcp_token_env=str(ch_raw.get("mcp_token_env", "HIBERNAUT_MCP_TOKEN")),
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
        role = item.get("role")
        if role is not None:
            if role not in ROLES:
                raise ValueError(f"executors.{name}: unsupported role '{role}' (supported: {', '.join(ROLES)})")
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
        )

    for name, role_cfg in roles.items():
        opts = role_cfg.sde
        if opts is None or not opts.review:
            continue
        target = roles.get(opts.reviewer)
        if target is None or target.role != CODER_REVIEWER_ROLE:
            raise ValueError(
                f"executors.{name}: reviewer '{opts.reviewer}' is not a coder-reviewer executor; "
                f"declare [executors.{opts.reviewer}] with role = \"{CODER_REVIEWER_ROLE}\" or set review = false"
            )

    projects: dict[str, ProjectConfig] = {}
    for name, item in raw.get("projects", {}).items():
        backend = str(item.get("worktree_backend", "orca"))
        if backend not in ("orca", "git"):
            raise ValueError(f"Unsupported worktree_backend for '{name}': {backend}")
        terminal = item.get("terminal_backend", "orca" if backend == "orca" else "none")
        if terminal not in TERMINAL_BACKENDS:
            raise ValueError(
                f"Unsupported terminal_backend for '{name}': {terminal!r} (supported: {', '.join(TERMINAL_BACKENDS)})"
            )
        proj_exec_raw = item.get("executor")
        proj_exec = str(proj_exec_raw).strip() if proj_exec_raw is not None else None
        projects[str(name)] = ProjectConfig(
            name=str(name),
            inbox_id=str(item.get("inbox_id", f"proj:{name}")),
            repo=str(item["repo"]),
            checkout=str(item["checkout"]),
            poll_labels=tuple(str(lbl) for lbl in item.get("poll_labels", ())),
            worktree_backend=cast(Literal["orca", "git"], backend),
            terminal_backend=terminal,
            orca_create_terminal=bool(item.get("orca_create_terminal", False)),
            feishu_chat_id=str(item.get("feishu_chat_id", "")),
            executor=proj_exec or None,
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
    )
