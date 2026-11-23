from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence, cast

from runner.protocol import (
    DriverKind,
    ExecutorCapability,
    InteractiveSurfaceKind,
    RunRequest,
)

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

    def capability(self, *, interactive_surface: InteractiveSurfaceKind = "none") -> ExecutorCapability: ...

    def build_command(self, req: RunRequest, workdir: Path) -> CommandSpec: ...

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
        )

    def build_command(self, req: RunRequest, workdir: Path) -> CommandSpec:
        del workdir
        native_id = (
            extract_matching_native_session_id(self.executor_id, req.executor_session_id)
            if self.resume_args
            else None
        )
        template_args: Sequence[str] = self.resume_args if native_id else self.args
        argv: list[str] = [self.command]
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
        )


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
        ),
        "grok": TemplateCLIDriver(
            executor_id="grok",
            driver_kind="grok",
            command="grok",
            args=("-p", "{prompt}"),
            default_idle_timeout_seconds=None,
        ),
        "cursor-agent": TemplateCLIDriver(
            executor_id="cursor-agent",
            driver_kind="cursor-agent",
            command="cursor-agent",
            args=("-p", "{prompt}"),
            default_idle_timeout_seconds=None,
        ),
        "dsh": TemplateCLIDriver(
            executor_id="dsh",
            driver_kind="dsh",
            command="dsh",
            args=("-c", "{prompt}"),
            default_idle_timeout_seconds=None,
        ),
    }
