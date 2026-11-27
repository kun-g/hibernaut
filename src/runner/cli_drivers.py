from __future__ import annotations

import json
import re
import shlex
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Protocol, TypeAlias, cast
from collections.abc import Mapping, Sequence

from runner.protocol import (
    DriverKind,
    ExecutorCapability,
    InteractiveSurfaceKind,
    MAX_FINAL_TEXT_CHARS,
    RunRequest,
)
from runner.tool_gate import ToolGateBinding

# The CLI whose hook format a driver uses to hold each tool call; None means its calls cannot be held.
ToolHookStyle: TypeAlias = Literal["claude", "grok", "cursor-agent"]
_TOOL_HOOK_SCRIPT = Path(__file__).resolve().with_name("tool_hook.py")
# Longer than the hook's own wait for an answer: a hook the CLI gives up on may count as no objection.
_TOOL_HOOK_TIMEOUT_SECONDS = 120

# Arguments that would replace what the hook needs, or switch hooks off. A gated driver must not carry them.
_HOOK_OVERRIDES: Mapping[ToolHookStyle, frozenset[str]] = {
    "claude": frozenset(
        {
            "--settings", "--permission-mode", "--dangerously-skip-permissions", "--allow-dangerously-skip-permissions",
            "--bare", "--safe-mode",
        }
    ),
    "grok": frozenset(
        {"--agent", "--agents", "--permission-mode", "--always-approve", "--no-subagents", "--disallowed-tools"}
    ),
    "cursor-agent": frozenset({"--plugin-dir"}),
}

_FENCED_JSON_RE = re.compile(r"```json\s*(\{.*?\})\s*```", re.DOTALL)


@dataclass(frozen=True, slots=True)
class StreamEvent:
    kind: str
    payload: Mapping[str, Any] | str


class StreamPipe(Protocol):
    def on_event(self, event: StreamEvent) -> None: ...

    async def send_stdin(self, text: str) -> None: ...


class NoopStreamPipe:
    def on_event(self, event: StreamEvent) -> None:
        return None

    async def send_stdin(self, text: str) -> None:
        return None


def _empty_env() -> dict[str, str]:
    return {}


@dataclass(frozen=True, slots=True)
class CommandSpec:
    argv: tuple[str, ...]
    stdin_text: str | None = None
    env: dict[str, str] = field(default_factory=_empty_env)
    idle_timeout_seconds: float | None = None


@dataclass(frozen=True, slots=True)
class ParsedCLIOutput:
    structured_output: dict[str, Any] | None = None
    executor_session_id: str | None = None
    final_text: str | None = None


def format_prefixed_session_id(driver_id: str, native_id: str | None) -> str | None:
    if not native_id:
        return None
    if native_id.startswith(f"{driver_id}:"):
        return native_id
    return f"{driver_id}:{native_id}"


def extract_matching_native_session_id(driver_id: str, prefixed_id: str | None) -> str | None:
    if not prefixed_id:
        return None
    prefix = f"{driver_id}:"
    if not prefixed_id.startswith(prefix):
        return None
    native = prefixed_id[len(prefix) :].strip()
    return native or None


class CLIDriver(Protocol):
    @property
    def executor_id(self) -> str: ...

    @property
    def driver_kind(self) -> DriverKind: ...

    @property
    def calls_tools(self) -> bool | None: ...

    @property
    def tool_hook(self) -> ToolHookStyle | None: ...

    def capability(self, *, interactive_surface: InteractiveSurfaceKind = "none") -> ExecutorCapability: ...

    def build_command(
        self, req: RunRequest, workdir: Path, *, tool_gate: ToolGateBinding | None = None
    ) -> CommandSpec: ...

    def parse_output(self, stdout: str, stderr: str, exit_code: int | None) -> ParsedCLIOutput: ...


@dataclass(frozen=True, slots=True)
class TemplateCLIDriver:
    executor_id: str
    driver_kind: DriverKind
    command: str
    args: tuple[str, ...] = ()
    resume_args: tuple[str, ...] = ()
    env: dict[str, str] = field(default_factory=_empty_env)
    default_idle_timeout_seconds: float | None = None
    supports_stream_json: bool = False
    pass_prompt_via_stdin: bool = False
    # None means unknown. A driver that says True runs only with an authorizer behind its tool_hook.
    calls_tools: bool | None = None
    tool_hook: ToolHookStyle | None = None

    def __post_init__(self) -> None:
        if self.tool_hook is None:
            return
        for where, tokens in (("args", self.args), ("resume_args", self.resume_args)):
            for token in tokens:
                if (flag := token.split("=", 1)[0]) in _HOOK_OVERRIDES[self.tool_hook]:
                    raise ValueError(
                        f"executor {self.executor_id!r}: {where} must not set {flag}, which the tool hook depends on"
                    )

    def capability(self, *, interactive_surface: InteractiveSurfaceKind = "none") -> ExecutorCapability:
        # A template driver only starts a headless subprocess with pipes; whatever the node offers, it cannot hand
        # an operator an Orca terminal, so it never claims one.
        del interactive_surface
        return ExecutorCapability(
            executor_id=self.executor_id,
            driver=self.driver_kind,
            supports_resume=bool(self.resume_args),
            supports_stream_json=self.supports_stream_json,
            interactive_surface="none",
            tool_authorization=self.tool_hook is not None,
        )

    def build_command(
        self, req: RunRequest, workdir: Path, *, tool_gate: ToolGateBinding | None = None
    ) -> CommandSpec:
        del workdir
        native_id = (
            extract_matching_native_session_id(self.executor_id, req.executor_session_id)
            if self.resume_args
            else None
        )
        template_args: Sequence[str] = self.resume_args if native_id else self.args
        argv: list[str] = [self.command]
        if tool_gate is not None:
            if self.tool_hook is None:
                raise ValueError(f"executor {self.executor_id!r} has no tool hook")
            argv.extend(_tool_hook_args(self.tool_hook, tool_gate))
        prompt_inserted = False
        for tok in template_args:
            rendered = tok.replace("{native_session_id}", native_id or "")
            if "{prompt}" in rendered:
                rendered = rendered.replace("{prompt}", req.prompt)
                prompt_inserted = True
            argv.append(rendered)
        if req.system_prompt_append and self.driver_kind == "claude":
            argv.extend(["--append-system-prompt", req.system_prompt_append])
        if req.output_schema is not None and self.driver_kind == "claude":
            argv.extend(["--json-schema", json.dumps(req.output_schema, ensure_ascii=False)])

        stdin_text: str | None = None
        if self.pass_prompt_via_stdin:
            stdin_text = req.prompt
        elif not prompt_inserted:
            argv.append(req.prompt)

        idle = req.idle_timeout_seconds if req.idle_timeout_seconds is not None else self.default_idle_timeout_seconds
        return CommandSpec(
            argv=tuple(argv),
            stdin_text=stdin_text,
            env=dict(self.env),
            idle_timeout_seconds=idle,
        )

    def parse_output(self, stdout: str, stderr: str, exit_code: int | None) -> ParsedCLIOutput:
        del stderr, exit_code
        structured: dict[str, Any] | None = None
        native_sid: str | None = None
        final_text: str | None = None

        if self.supports_stream_json:
            for line in stdout.splitlines():
                line = line.strip()
                if not line or not line.startswith("{"):
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(obj, dict):
                    continue
                obj_map = cast(dict[str, Any], obj)
                if isinstance(obj_map.get("session_id"), str):
                    native_sid = obj_map["session_id"]
                if obj_map.get("type") == "result" and isinstance(obj_map.get("result"), str):
                    final_text = obj_map["result"]
                if isinstance(obj_map.get("structured_output"), dict):
                    structured = cast(dict[str, Any], obj_map["structured_output"])
                elif obj_map.get("type") == "result" and isinstance(obj_map.get("result"), str):
                    extracted = _extract_json_object(obj_map["result"])
                    if extracted is not None:
                        structured = extracted

        if structured is None:
            structured = _extract_json_object(stdout)
            if structured is not None and native_sid is None:
                cand = structured.get("executor_session_id") or structured.get("session_id")
                if isinstance(cand, str):
                    native_sid = cand

        return ParsedCLIOutput(
            structured_output=structured,
            executor_session_id=format_prefixed_session_id(self.executor_id, native_sid),
            final_text=_bounded(final_text),
        )


def _bounded(text: str | None) -> str | None:
    if text is None or len(text) <= MAX_FINAL_TEXT_CHARS:
        return text
    return f"{text[:MAX_FINAL_TEXT_CHARS]}…(truncated)"


def _tool_hook_args(style: ToolHookStyle, gate: ToolGateBinding) -> list[str]:
    def command(event: str) -> str:
        # Grok and Cursor let a call through when its hook exits with anything but 0 or 2, e.g. when it was killed.
        argv = [sys.executable, "-I", str(_TOOL_HOOK_SCRIPT), style, str(gate.socket_path), event]
        return f"{shlex.join(argv)} || exit 2"

    if style == "claude":
        pre: dict[str, Any] = {
            "type": "command", "command": command("pre"), "timeout": _TOOL_HOOK_TIMEOUT_SECONDS, "onFailure": "block",
        }
        settings = {
            # The command line outranks the project's own settings, which could otherwise switch hooks off.
            "disableAllHooks": False,
            "hooks": {
                "SessionStart": [{"hooks": [{"type": "command", "command": command("arm")}]}],
                "PreToolUse": [{"matcher": "*", "hooks": [pre]}],
            },
        }
        # dontAsk refuses whatever the hook did not allow, so nothing runs on the CLI's own say-so.
        return ["--settings", json.dumps(settings), "--permission-mode", "dontAsk"]
    if style == "grok":
        hooks = {
            "SessionStart": [{"hooks": [{"type": "command", "command": command("arm")}]}],
            "PreToolUse": [
                {
                    "hooks": [
                        {
                            "type": "command", "command": command("pre"), "timeout": _TOOL_HOOK_TIMEOUT_SECONDS,
                            "failClosed": True,
                        }
                    ]
                }
            ],
        }
        agent = gate.config_dir / "agent.md"
        # JSON is valid YAML, so the front matter needs no YAML writer.
        agent.write_text(
            "---\nname: runner-gated\ndescription: Tool calls are authorized by the runner\npromptMode: extend\n"
            f"hooks: {json.dumps(hooks)}\n---\n",
            encoding="utf-8",
        )
        # A subagent or workflow would run its calls outside this agent's hooks.
        return [
            "--agent", str(agent), "--permission-mode", "bypassPermissions", "--no-subagents",
            "--disallowed-tools", "workflow",
        ]
    plugin = gate.config_dir / "plugin"
    (plugin / ".cursor-plugin").mkdir(parents=True)
    (plugin / "hooks").mkdir()
    (plugin / ".cursor-plugin" / "plugin.json").write_text(
        json.dumps({"name": "runner-gate", "version": "1.0.0", "description": "Runner tool authorization"}), encoding="utf-8"
    )
    cursor_hooks: dict[str, Any] = {
        "version": 1,
        "hooks": {
            "sessionStart": [{"command": command("arm")}],
            "preToolUse": [{"command": command("pre"), "timeout": _TOOL_HOOK_TIMEOUT_SECONDS, "failClosed": True}],
        },
    }
    (plugin / "hooks" / "hooks.json").write_text(json.dumps(cursor_hooks), encoding="utf-8")
    return ["--plugin-dir", str(plugin), "--force", "--trust"]


def _extract_json_object(text: str) -> dict[str, Any] | None:
    stripped = text.strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        try:
            parsed = json.loads(stripped)
            if isinstance(parsed, dict):
                return cast(dict[str, Any], parsed)
        except json.JSONDecodeError:
            pass
    matches = _FENCED_JSON_RE.findall(text)
    for m in reversed(matches):
        try:
            parsed = json.loads(m)
            if isinstance(parsed, dict):
                return cast(dict[str, Any], parsed)
        except json.JSONDecodeError:
            continue
    return None


def default_cli_drivers() -> dict[str, CLIDriver]:
    return {
        "claude": TemplateCLIDriver(
            executor_id="claude",
            driver_kind="claude",
            command="claude",
            args=("-p", "--output-format", "stream-json", "--verbose"),
            resume_args=("-p", "--output-format", "stream-json", "--verbose", "--resume", "{native_session_id}"),
            default_idle_timeout_seconds=300.0,
            supports_stream_json=True,
            pass_prompt_via_stdin=True,
            calls_tools=True,
            tool_hook="claude",
        ),
        "grok": TemplateCLIDriver(
            executor_id="grok",
            driver_kind="grok",
            command="grok",
            args=("-p", "{prompt}"),
            default_idle_timeout_seconds=None,
            calls_tools=True,
            tool_hook="grok",
        ),
        "cursor-agent": TemplateCLIDriver(
            executor_id="cursor-agent",
            driver_kind="cursor-agent",
            command="cursor-agent",
            args=("-p", "{prompt}"),
            default_idle_timeout_seconds=None,
            calls_tools=True,
            tool_hook="cursor-agent",
        ),
        "dsh": TemplateCLIDriver(
            executor_id="dsh",
            driver_kind="dsh",
            command="dsh",
            args=("-c", "{prompt}"),
            default_idle_timeout_seconds=None,
            calls_tools=True,
        ),
    }
