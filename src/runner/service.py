# Standalone Runner process of the read-only pilot: python -m runner.service --config runner.toml (docs/runner.md §9).
from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import logging
import os
import signal
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Mapping, cast
from urllib.parse import urlsplit

from runner.check_driver import ProtocolCheckDriver
from runner.cli_drivers import CLIDriver
from runner.config import RunnerConfig, parse_runner_config
from runner.daemon import RunnerDaemon
from runner.transport.auth import read_relay_token
from runner.transport.relay_client import RunnerRelayAgent

_log = logging.getLogger("runner.service")

RELAY_PATH: Final = "/ws/runner"
_LOOPBACK: Final = "127.0.0.1"
_TABLES: Final[Mapping[str, frozenset[str]]] = {
    "runner": frozenset(
        {"runner_id", "max_concurrent", "worktree_backend", "interactive_surface", "readonly_only", "state_dir",
         "logs_dir", "labels"}
    ),
    "namespaces": frozenset({"hb"}),
    "projects": frozenset(),
    "relay": frozenset({"url", "token_file"}),
}


@dataclass(frozen=True, slots=True)
class RunnerServiceConfig:
    runner: RunnerConfig
    relay_url: str
    token_file: Path


def service_drivers() -> dict[str, CLIDriver]:
    # The only program this service can start. No configuration adds another: an agent CLI here would call tools
    # without Hibernaut's per-call authorization.
    driver = ProtocolCheckDriver()
    return {driver.executor_id: driver}


def parse_service_config(raw: Mapping[str, Any], *, base_dir: Path) -> RunnerServiceConfig:
    unknown = sorted(set(raw) - set(_TABLES))
    if unknown:
        raise ValueError(f"unknown tables: {', '.join(unknown)}")
    for name, keys in _TABLES.items():
        table = raw.get(name)
        if not isinstance(table, Mapping):
            raise ValueError(f"[{name}] is required")
        extra = sorted(str(k) for k in cast(Mapping[str, Any], table) if keys and k not in keys)
        if extra:
            raise ValueError(f"[{name}] has unknown keys: {', '.join(extra)}")
    config = parse_runner_config(raw, base_dir=base_dir)
    for ok, message in (
        (config.max_concurrent == 1, "runner.max_concurrent must be 1 (one slot)"),
        (config.readonly_only, "runner.readonly_only must be true"),
        (config.worktree_backend == "git", 'runner.worktree_backend must be "git"'),
        (config.interactive_surface == "none", 'runner.interactive_surface must be "none"'),
        (
            config.namespaces["hb"].allowed_callers == ("hibernaut",),
            'namespaces.hb.allowed_callers must be ["hibernaut"]',
        ),
        (len(config.projects) == 1, "exactly one [projects.<key>] is required"),
    ):
        if not ok:
            raise ValueError(message)
    relay = cast(Mapping[str, Any], raw["relay"])
    url = str(relay.get("url", ""))
    parsed = urlsplit(url)
    if (parsed.scheme, parsed.netloc, parsed.path) != ("ws", f"{_LOOPBACK}:{parsed.port}", RELAY_PATH) or parsed.query:
        raise ValueError(f"relay.url must be ws://{_LOOPBACK}:<port>{RELAY_PATH}")
    token_raw = str(relay.get("token_file", "")).strip()
    if not token_raw:
        raise ValueError("relay.token_file is required")
    token_file = Path(token_raw).expanduser()
    return RunnerServiceConfig(
        runner=config, relay_url=url, token_file=token_file if token_file.is_absolute() else base_dir / token_file
    )


def load_service_config(path: Path) -> RunnerServiceConfig:
    resolved = path.expanduser().resolve()
    return parse_service_config(tomllib.loads(resolved.read_text(encoding="utf-8")), base_dir=resolved.parent)


def _check_checkouts(config: RunnerConfig) -> None:
    for project in config.projects.values():
        if not (project.checkout_path / ".git").exists():
            raise ValueError(f"projects.{project.project_key}.checkout_path {project.checkout_path} is not a git checkout")


def _lock_state_dir(state_dir: Path) -> int:
    # One service per state directory: a second one would take over the Relay connection and the workspaces.
    state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(state_dir / "runner.lock", os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        raise ValueError(f"another runner service holds {state_dir}/runner.lock") from None
    return fd


async def serve(config: RunnerServiceConfig, token: str, stop: asyncio.Event) -> int:
    daemon = RunnerDaemon(config.runner, drivers=service_drivers())
    agent = RunnerRelayAgent(daemon=daemon, hub_url=config.relay_url, token=token)
    stopping = asyncio.ensure_future(stop.wait())
    try:
        await agent.start()
        _log.info("runner %s serving %s", config.runner.runner_id, config.relay_url)
        finished = asyncio.ensure_future(agent.wait_finished())
        await asyncio.wait({stopping, finished}, return_when=asyncio.FIRST_COMPLETED)
        # The agent ends on its own only when another connection took over this runner_id: exit for the supervisor.
        return 0 if stop.is_set() else 1
    finally:
        stopping.cancel()
        # Cancels the run in progress and waits until its process group is gone and its worktree removed.
        await agent.stop()
        daemon.close()
        _log.info("runner %s stopped", config.runner.runner_id)


async def _main(config: RunnerServiceConfig, token: str) -> int:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    return await serve(config, token, stop)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m runner.service", description="Standalone read-only Runner")
    parser.add_argument("--config", required=True, type=Path, help="Path to runner.toml")
    parser.add_argument("--check", action="store_true", help="Validate the configuration, print the capabilities, exit")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stderr)
    try:
        config = load_service_config(args.config)
        _check_checkouts(config.runner)
        token = read_relay_token(config.token_file)
        lock = _lock_state_dir(config.runner.state_dir)
    except (OSError, ValueError, KeyError) as exc:
        print(f"[runner] {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 2
    try:
        if args.check:
            daemon = RunnerDaemon(config.runner, drivers=service_drivers())
            print(json.dumps(daemon.capabilities_sync().to_dict(), ensure_ascii=False, indent=2))
            daemon.close()
            return 0
        return asyncio.run(_main(config, token))
    finally:
        os.close(lock)


if __name__ == "__main__":
    sys.exit(main())
