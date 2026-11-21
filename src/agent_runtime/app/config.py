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
from agent_runtime.adapters.executor_cli.executor import CLITemplate
from agent_runtime.adapters.executor_cli.worktree import WorktreeBackend
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


@dataclass(frozen=True, slots=True)
class ProjectConfig:
    name: str
    inbox_id: str
    repo: str
    checkout: str
    poll_labels: tuple[str, ...] = ()
    worktree_backend: WorktreeBackend = "orca"
    orca_create_terminal: bool = False
    feishu_chat_id: str = ""
    executor: str | None = None


@dataclass(frozen=True, slots=True)
class HibernautConfig:
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    channels: ChannelsConfig = field(default_factory=ChannelsConfig)
    executors: dict[str, CLITemplate] = field(default_factory=dict)
    projects: dict[str, ProjectConfig] = field(default_factory=dict)


def load_config(path: str | Path) -> HibernautConfig:
    raw = tomllib.loads(Path(path).read_text(encoding="utf-8"))
    return parse_config(raw)


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
            config = parse_config(raw)
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError) as exc:
            print(
                f"[config] Invalid {path} ({type(exc).__name__}); keeping current settings.",
                file=sys.stderr,
                flush=True,
            )
            continue
        yield config


def parse_config(raw: dict[str, Any]) -> HibernautConfig:
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

    executors: dict[str, CLITemplate] = {}
    for name, item in raw.get("executors", {}).items():
        env_raw = item.get("env", {})
        resume_raw = item.get("resume_args")
        executors[str(name)] = CLITemplate(
            name=str(name),
            command=str(item["command"]),
            args=tuple(str(a) for a in item.get("args", ())),
            resume_args=(
                tuple(str(a) for a in resume_raw) if resume_raw is not None else ()
            ),
            env={str(k): str(v) for k, v in env_raw.items()},
            timeout_seconds=float(item.get("timeout_seconds", 300.0)),
        )

    projects: dict[str, ProjectConfig] = {}
    for name, item in raw.get("projects", {}).items():
        backend = str(item.get("worktree_backend", "orca"))
        if backend not in ("orca", "git"):
            raise ValueError(f"Unsupported worktree_backend for '{name}': {backend}")
        proj_exec_raw = item.get("executor")
        proj_exec = str(proj_exec_raw).strip() if proj_exec_raw is not None else None
        projects[str(name)] = ProjectConfig(
            name=str(name),
            inbox_id=str(item.get("inbox_id", f"proj:{name}")),
            repo=str(item["repo"]),
            checkout=str(item["checkout"]),
            poll_labels=tuple(str(lbl) for lbl in item.get("poll_labels", ())),
            worktree_backend=cast(Literal["orca", "git"], backend),
            orca_create_terminal=bool(item.get("orca_create_terminal", False)),
            feishu_chat_id=str(item.get("feishu_chat_id", "")),
            executor=proj_exec or None,
        )

    return HibernautConfig(
        runtime=runtime,
        channels=channels,
        executors=executors,
        projects=projects,
    )
