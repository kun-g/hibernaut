# Standalone Runner process of a read-only remote agent node: python -m runner.agent_service --config agent-runner.toml
# (docs/runner.md §10). One agent CLI, each of its tool calls authorized by Hibernaut and confined to the workspace here.
from __future__ import annotations

import re
import sys
import tomllib
from dataclasses import replace
from pathlib import Path
from typing import Any, Final, Mapping, cast

from runner.cli_drivers import TemplateCLIDriver
from runner.protocol_check import PROTOCOL_CHECK_EXECUTOR_ID
from runner.service import RunnerServiceConfig, parse_service_config, run_service
from runner.workspace_guard import WORKSPACE_READ_TOOLS, workspace_read_refusal

AGENT_TOOLS: Final = WORKSPACE_READ_TOOLS
_EXECUTOR_KEYS: Final = frozenset({"cli", "command", "model"})
_NAME_RE: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
# --restricted leaves out the user's and the repository's settings (and with them their hooks) and confines the file
# tools to the working directory; a CLI that does not know it fails the run.
_CLAUDE_ARGS: Final = (
    "-p", "--output-format", "stream-json", "--verbose", "--restricted", "--tools", ",".join(AGENT_TOOLS),
    "--strict-mcp-config", "--disable-slash-commands",
)
_CLAUDE_ENV: Final[Mapping[str, str]] = {
    # "@<path>" in a prompt would attach that file without a tool call, and the prompt is an event's text.
    "CLAUDE_CODE_DISABLE_ATTACHMENTS": "1",
    "CLAUDE_CODE_DISABLE_AUTO_MEMORY": "1",
}


def agent_driver(executor_id: str, command: str, *, model: str | None = None) -> TemplateCLIDriver:
    args = (*_CLAUDE_ARGS, *(("--model", model) if model is not None else ()))
    return TemplateCLIDriver(
        executor_id=executor_id,
        driver_kind="claude",
        command=command,
        args=args,
        resume_args=(*args, "--resume", "{native_session_id}"),
        env=dict(_CLAUDE_ENV),
        default_idle_timeout_seconds=300.0,
        supports_stream_json=True,
        pass_prompt_via_stdin=True,
        calls_tools=True,
        tool_hook="claude",
    )


def _parse_driver(raw: object) -> TemplateCLIDriver:
    if not isinstance(raw, Mapping) or len(cast(Mapping[str, Any], raw)) != 1:
        raise ValueError("exactly one [executors.<id>] is required")
    ((name, table),) = cast(Mapping[str, Any], raw).items()
    if not _NAME_RE.fullmatch(name) or name == PROTOCOL_CHECK_EXECUTOR_ID:
        raise ValueError(f"executors.{name}: the name cannot be used")
    if not isinstance(table, Mapping):
        raise ValueError(f"[executors.{name}] must be a table")
    item = cast(Mapping[str, Any], table)
    if extra := sorted(set(item) - _EXECUTOR_KEYS):
        raise ValueError(f"[executors.{name}] has unknown keys: {', '.join(extra)}")
    if item.get("cli") != "claude":
        raise ValueError(f'executors.{name}.cli must be "claude"')
    command = item.get("command")
    if not isinstance(command, str) or not Path(command).is_absolute():
        raise ValueError(f"executors.{name}.command must be an absolute path")
    model = item.get("model")
    if model is not None and (not isinstance(model, str) or not _NAME_RE.fullmatch(model)):
        raise ValueError(f"executors.{name}.model must be a model name")
    return agent_driver(name, command, model=model)


def parse_agent_service_config(raw: Mapping[str, Any], *, base_dir: Path) -> RunnerServiceConfig:
    base = parse_service_config({k: v for k, v in raw.items() if k != "executors"}, base_dir=base_dir)
    driver = _parse_driver(raw.get("executors"))
    return replace(base, drivers={driver.executor_id: driver}, tool_guard=workspace_read_refusal)


def load_agent_service_config(path: Path) -> RunnerServiceConfig:
    resolved = path.expanduser().resolve()
    return parse_agent_service_config(tomllib.loads(resolved.read_text(encoding="utf-8")), base_dir=resolved.parent)


def main(argv: list[str] | None = None) -> int:
    return run_service(
        argv, prog="python -m runner.agent_service", description="Standalone read-only agent Runner",
        load=load_agent_service_config,
    )


if __name__ == "__main__":
    sys.exit(main())
