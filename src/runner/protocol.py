from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Any, Final, Literal, Protocol, TypeAlias, cast
from collections.abc import Awaitable, Callable, Mapping

SCHEMA_VERSION: Final = "runner.v1"
MAX_STDOUT_EXCERPT_BYTES: Final = 262_144
# Sent beside the excerpt; both have to fit the relay's 1 MiB message.
MAX_FINAL_TEXT_CHARS: Final = 65_536

RunStatus: TypeAlias = Literal["succeeded", "failed", "interrupted", "timed_out", "refused"]
ErrorKind: TypeAlias = Literal[
    "rate_limit",
    "network",
    "crash",
    "timeout",
    "config_error",
    "permission_denied",
    "tool_error",
    "bad_output",
    "unknown",
]
DriverKind: TypeAlias = Literal["claude", "grok", "cursor-agent", "dsh", "custom"]
WorktreeBackendKind: TypeAlias = Literal["orca", "git"]
InteractiveSurfaceKind: TypeAlias = Literal["orca", "none"]
InterruptReason: TypeAlias = Literal["new_event", "user_cancel", "shutdown", "timeout"]
InterruptState: TypeAlias = Literal["cancelling", "already_finished", "not_found"]
ReclaimStatus: TypeAlias = Literal["reclaimed", "blocked", "not_found", "error"]
ToolAction: TypeAlias = Literal["allow", "block", "defer"]

_RUN_ID_RE: Final = re.compile(r"^run_[A-Za-z0-9_-]+$")
_NAMESPACE_RE: Final = re.compile(r"^[a-z][a-z0-9_-]{0,15}$")

_VALID_RUN_STATUSES: Final = frozenset({"succeeded", "failed", "interrupted", "timed_out", "refused"})
_VALID_ERROR_KINDS: Final = frozenset(
    {
        "rate_limit",
        "network",
        "crash",
        "timeout",
        "config_error",
        "permission_denied",
        "tool_error",
        "bad_output",
        "unknown",
    }
)
_VALID_INTERRUPT_REASONS: Final = frozenset({"new_event", "user_cancel", "shutdown", "timeout"})
_VALID_INTERRUPT_STATES: Final = frozenset({"cancelling", "already_finished", "not_found"})
_VALID_TOOL_ACTIONS: Final = frozenset({"allow", "block", "defer"})
_VALID_RECLAIM_STATUSES: Final = frozenset({"reclaimed", "blocked", "not_found", "error"})


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds")


def _empty_str_dict() -> dict[str, str]:
    return {}


def _empty_any_dict() -> dict[str, Any]:
    return {}


class ProtocolValidationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ExecutorCapability:
    executor_id: str
    driver: DriverKind
    supports_resume: bool
    supports_stream_json: bool
    interactive_surface: InteractiveSurfaceKind = "none"
    # Every tool call of this executor can be held until the caller's ToolAuthorizer answers. A node that does not
    # say so (an older one included) is never given a run that needs it.
    tool_authorization: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "executor_id": self.executor_id,
            "driver": self.driver,
            "supports_resume": self.supports_resume,
            "supports_stream_json": self.supports_stream_json,
            "interactive_surface": self.interactive_surface,
            "tool_authorization": self.tool_authorization,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> ExecutorCapability:
        return cls(
            executor_id=_req_str(raw, "executor_id"),
            driver=cast(DriverKind, _req_str(raw, "driver")),
            supports_resume=_req_bool(raw, "supports_resume"),
            supports_stream_json=_req_bool(raw, "supports_stream_json"),
            interactive_surface=cast(
                InteractiveSurfaceKind,
                raw.get("interactive_surface", "none"),
            ),
            tool_authorization=raw.get("tool_authorization") is True,
        )


@dataclass(frozen=True, slots=True)
class SlotSnapshot:
    max_concurrent: int
    active_runs: int
    available: int

    def to_dict(self) -> dict[str, int]:
        return {
            "max_concurrent": self.max_concurrent,
            "active_runs": self.active_runs,
            "available": self.available,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> SlotSnapshot:
        return cls(
            max_concurrent=_req_int(raw, "max_concurrent", minimum=1),
            active_runs=_req_int(raw, "active_runs", minimum=0),
            available=_req_int(raw, "available", minimum=0),
        )


@dataclass(frozen=True, slots=True)
class RunnerCapabilities:
    runner_id: str
    namespaces: tuple[str, ...]
    projects: tuple[str, ...]
    worktree_backend: WorktreeBackendKind
    interactive_surface: InteractiveSurfaceKind
    executors: tuple[ExecutorCapability, ...]
    slots: SlotSnapshot
    reported_at: str = field(default_factory=utc_now_iso)
    labels: dict[str, str] = field(default_factory=_empty_str_dict)
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "runner_id": self.runner_id,
            "namespaces": list(self.namespaces),
            "projects": list(self.projects),
            "worktree_backend": self.worktree_backend,
            "interactive_surface": self.interactive_surface,
            "labels": dict(self.labels),
            "executors": [e.to_dict() for e in self.executors],
            "slots": self.slots.to_dict(),
            "reported_at": self.reported_at,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> RunnerCapabilities:
        _check_schema_version(raw)
        ns_raw = _req_list(raw, "namespaces")
        if not ns_raw:
            raise ProtocolValidationError("namespaces must be non-empty")
        namespaces = tuple(str(x) for x in ns_raw)
        projects = tuple(str(x) for x in _req_list(raw, "projects"))
        execs = tuple(ExecutorCapability.from_dict(cast(Mapping[str, Any], e)) for e in _req_list(raw, "executors"))
        slots_raw = raw.get("slots")
        if not isinstance(slots_raw, Mapping):
            raise ProtocolValidationError("slots must be an object")
        labels_raw = raw.get("labels", {})
        if not isinstance(labels_raw, Mapping):
            raise ProtocolValidationError("labels must be an object")
        return cls(
            schema_version=SCHEMA_VERSION,
            runner_id=_req_str(raw, "runner_id"),
            namespaces=namespaces,
            projects=projects,
            worktree_backend=cast(WorktreeBackendKind, _req_str(raw, "worktree_backend")),
            interactive_surface=cast(InteractiveSurfaceKind, _req_str(raw, "interactive_surface")),
            executors=execs,
            slots=SlotSnapshot.from_dict(cast(Mapping[str, Any], slots_raw)),
            reported_at=_req_str(raw, "reported_at"),
            labels={str(k): str(v) for k, v in cast(Mapping[Any, Any], labels_raw).items()},
        )


@dataclass(frozen=True, slots=True)
class CredentialLease:
    broker_url: str
    lease_token: str
    expires_at: str

    def to_dict(self) -> dict[str, str]:
        return {
            "broker_url": self.broker_url,
            "lease_token": self.lease_token,
            "expires_at": self.expires_at,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> CredentialLease:
        return cls(
            broker_url=_req_str(raw, "broker_url"),
            lease_token=_req_str(raw, "lease_token"),
            expires_at=_req_str(raw, "expires_at"),
        )


@dataclass(frozen=True, slots=True)
class RunRequest:
    run_id: str
    namespace: str
    project_key: str
    owner_scope: str
    work_key: str
    executor_id: str
    readonly: bool
    prompt: str
    timeout_seconds: float = 1800.0
    name_hint: str | None = None
    executor_session_id: str | None = None
    interactive: bool = False
    remote_readonly_ok: bool = False
    system_prompt_append: str | None = None
    output_schema: dict[str, Any] | None = None
    idle_timeout_seconds: float | None = None
    credential_lease: CredentialLease | None = None
    # The run may start only where each of its tool calls is first put to the caller's ToolAuthorizer.
    authorize_tools: bool = False
    metadata: dict[str, Any] = field(default_factory=_empty_any_dict)
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ProtocolValidationError(f"unsupported schema_version: {self.schema_version}")
        if not _RUN_ID_RE.match(self.run_id):
            raise ProtocolValidationError(f"invalid run_id: {self.run_id}")
        if not _NAMESPACE_RE.match(self.namespace):
            raise ProtocolValidationError(f"invalid namespace: {self.namespace}")
        if not self.project_key or not self.owner_scope or not self.work_key or not self.executor_id:
            raise ProtocolValidationError("project_key, owner_scope, work_key, executor_id must be non-empty")
        if self.timeout_seconds <= 0:
            raise ProtocolValidationError("timeout_seconds must be > 0")
        if self.idle_timeout_seconds is not None and self.idle_timeout_seconds <= 0:
            raise ProtocolValidationError("idle_timeout_seconds must be > 0")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "namespace": self.namespace,
            "project_key": self.project_key,
            "owner_scope": self.owner_scope,
            "work_key": self.work_key,
            "name_hint": self.name_hint,
            "executor_id": self.executor_id,
            "executor_session_id": self.executor_session_id,
            "readonly": self.readonly,
            "interactive": self.interactive,
            "remote_readonly_ok": self.remote_readonly_ok,
            "prompt": self.prompt,
            "system_prompt_append": self.system_prompt_append,
            "output_schema": self.output_schema,
            "timeout_seconds": self.timeout_seconds,
            "idle_timeout_seconds": self.idle_timeout_seconds,
            "credential_lease": self.credential_lease.to_dict() if self.credential_lease else None,
            "authorize_tools": self.authorize_tools,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> RunRequest:
        _check_schema_version(raw)
        lease_raw = raw.get("credential_lease")
        lease = CredentialLease.from_dict(cast(Mapping[str, Any], lease_raw)) if isinstance(lease_raw, Mapping) else None
        out_schema = raw.get("output_schema")
        meta = raw.get("metadata", {})
        if not isinstance(meta, Mapping):
            raise ProtocolValidationError("metadata must be an object")
        idle_raw = raw.get("idle_timeout_seconds")
        return cls(
            schema_version=SCHEMA_VERSION,
            run_id=_req_str(raw, "run_id"),
            namespace=_req_str(raw, "namespace"),
            project_key=_req_str(raw, "project_key"),
            owner_scope=_req_str(raw, "owner_scope"),
            work_key=_req_str(raw, "work_key"),
            name_hint=_opt_str(raw, "name_hint"),
            executor_id=_req_str(raw, "executor_id"),
            executor_session_id=_opt_str(raw, "executor_session_id"),
            readonly=_req_bool(raw, "readonly"),
            interactive=bool(raw.get("interactive", False)),
            remote_readonly_ok=bool(raw.get("remote_readonly_ok", False)),
            prompt=_req_str(raw, "prompt", allow_empty=True),
            system_prompt_append=_opt_str(raw, "system_prompt_append"),
            output_schema=dict(cast(Mapping[str, Any], out_schema)) if isinstance(out_schema, Mapping) else None,
            timeout_seconds=_req_float(raw, "timeout_seconds"),
            idle_timeout_seconds=float(idle_raw) if isinstance(idle_raw, (int, float)) and not isinstance(idle_raw, bool) else None,
            credential_lease=lease,
            authorize_tools=_opt_bool(raw, "authorize_tools"),
            metadata=dict(cast(Mapping[str, Any], meta)),
        )


@dataclass(frozen=True, slots=True)
class LogRef:
    runner_id: str
    path: str
    bytes: int
    sha256: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "runner_id": self.runner_id,
            "path": self.path,
            "bytes": self.bytes,
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> LogRef:
        return cls(
            runner_id=_req_str(raw, "runner_id"),
            path=_req_str(raw, "path"),
            bytes=_req_int(raw, "bytes", minimum=0),
            sha256=_opt_str(raw, "sha256"),
        )


@dataclass(frozen=True, slots=True)
class WorktreeRef:
    work_key: str
    branch: str | None = None
    head_sha: str | None = None
    dirty: bool | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "work_key": self.work_key,
            "branch": self.branch,
            "head_sha": self.head_sha,
            "dirty": self.dirty,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> WorktreeRef:
        dirty_val = raw.get("dirty")
        return cls(
            work_key=_req_str(raw, "work_key"),
            branch=_opt_str(raw, "branch"),
            head_sha=_opt_str(raw, "head_sha"),
            dirty=dirty_val if isinstance(dirty_val, bool) else None,
        )


@dataclass(frozen=True, slots=True)
class RunResult:
    run_id: str
    runner_id: str
    status: RunStatus
    executor_id: str
    started_at: str
    ended_at: str
    duration_ms: int
    executor_session_id: str | None = None
    exit_code: int | None = None
    error_kind: ErrorKind | None = None
    error_summary: str | None = None
    stdout_excerpt: str = ""
    # The executor's reply as text, when its output wraps it (stream-json); None means stdout_excerpt is the reply.
    final_text: str | None = None
    structured_output: dict[str, Any] | None = None
    log_ref: LogRef | None = None
    worktree_ref: WorktreeRef | None = None
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ProtocolValidationError(f"unsupported schema_version: {self.schema_version}")
        if self.status not in _VALID_RUN_STATUSES:
            raise ProtocolValidationError(f"invalid status: {self.status}")
        if self.error_kind is not None and self.error_kind not in _VALID_ERROR_KINDS:
            raise ProtocolValidationError(f"invalid error_kind: {self.error_kind}")
        if self.duration_ms < 0:
            raise ProtocolValidationError("duration_ms must be >= 0")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "runner_id": self.runner_id,
            "status": self.status,
            "executor_id": self.executor_id,
            "executor_session_id": self.executor_session_id,
            "exit_code": self.exit_code,
            "error_kind": self.error_kind,
            "error_summary": self.error_summary,
            "stdout_excerpt": self.stdout_excerpt[:MAX_STDOUT_EXCERPT_BYTES],
            "final_text": self.final_text,
            "structured_output": self.structured_output,
            "log_ref": self.log_ref.to_dict() if self.log_ref else None,
            "worktree_ref": self.worktree_ref.to_dict() if self.worktree_ref else None,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "duration_ms": self.duration_ms,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> RunResult:
        _check_schema_version(raw)
        log_raw = raw.get("log_ref")
        wt_raw = raw.get("worktree_ref")
        so_raw = raw.get("structured_output")
        exit_raw = raw.get("exit_code")
        return cls(
            schema_version=SCHEMA_VERSION,
            run_id=_req_str(raw, "run_id"),
            runner_id=_req_str(raw, "runner_id"),
            status=cast(RunStatus, _req_str(raw, "status")),
            executor_id=_req_str(raw, "executor_id"),
            executor_session_id=_opt_str(raw, "executor_session_id"),
            exit_code=exit_raw if isinstance(exit_raw, int) and not isinstance(exit_raw, bool) else None,
            error_kind=cast(ErrorKind | None, _opt_str(raw, "error_kind")),
            error_summary=_opt_str(raw, "error_summary"),
            stdout_excerpt=str(raw.get("stdout_excerpt", "")),
            final_text=_opt_str(raw, "final_text"),
            structured_output=dict(cast(Mapping[str, Any], so_raw)) if isinstance(so_raw, Mapping) else None,
            log_ref=LogRef.from_dict(cast(Mapping[str, Any], log_raw)) if isinstance(log_raw, Mapping) else None,
            worktree_ref=WorktreeRef.from_dict(cast(Mapping[str, Any], wt_raw)) if isinstance(wt_raw, Mapping) else None,
            started_at=_req_str(raw, "started_at"),
            ended_at=_req_str(raw, "ended_at"),
            duration_ms=_req_int(raw, "duration_ms", minimum=0),
        )


@dataclass(frozen=True, slots=True)
class InterruptRequest:
    run_id: str
    reason: InterruptReason
    grace_seconds: float = 2.0
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ProtocolValidationError(f"unsupported schema_version: {self.schema_version}")
        if not _RUN_ID_RE.match(self.run_id):
            raise ProtocolValidationError(f"invalid run_id: {self.run_id}")
        if self.reason not in _VALID_INTERRUPT_REASONS:
            raise ProtocolValidationError(f"invalid interrupt reason: {self.reason}")
        if self.grace_seconds < 0:
            raise ProtocolValidationError("grace_seconds must be >= 0")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "reason": self.reason,
            "grace_seconds": self.grace_seconds,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> InterruptRequest:
        _check_schema_version(raw)
        grace_raw = raw.get("grace_seconds", 2.0)
        return cls(
            schema_version=SCHEMA_VERSION,
            run_id=_req_str(raw, "run_id"),
            reason=cast(InterruptReason, _req_str(raw, "reason")),
            grace_seconds=float(grace_raw) if isinstance(grace_raw, (int, float)) and not isinstance(grace_raw, bool) else 2.0,
        )


@dataclass(frozen=True, slots=True)
class InterruptResult:
    run_id: str
    accepted: bool
    state: InterruptState
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.state not in _VALID_INTERRUPT_STATES:
            raise ProtocolValidationError(f"invalid interrupt state: {self.state}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "accepted": self.accepted,
            "state": self.state,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> InterruptResult:
        _check_schema_version(raw)
        return cls(
            schema_version=SCHEMA_VERSION,
            run_id=_req_str(raw, "run_id"),
            accepted=_req_bool(raw, "accepted"),
            state=cast(InterruptState, _req_str(raw, "state")),
        )


@dataclass(frozen=True, slots=True)
class ReclaimRequest:
    namespace: str
    project_key: str
    owner_scope: str
    work_key: str
    merged_head_sha: str | None = None
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise ProtocolValidationError(f"unsupported schema_version: {self.schema_version}")
        if not _NAMESPACE_RE.match(self.namespace):
            raise ProtocolValidationError(f"invalid namespace: {self.namespace}")
        if not self.project_key or not self.owner_scope or not self.work_key:
            raise ProtocolValidationError("project_key, owner_scope, work_key must be non-empty")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "namespace": self.namespace,
            "project_key": self.project_key,
            "owner_scope": self.owner_scope,
            "work_key": self.work_key,
            "merged_head_sha": self.merged_head_sha,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> ReclaimRequest:
        _check_schema_version(raw)
        return cls(
            schema_version=SCHEMA_VERSION,
            namespace=_req_str(raw, "namespace"),
            project_key=_req_str(raw, "project_key"),
            owner_scope=_req_str(raw, "owner_scope"),
            work_key=_req_str(raw, "work_key"),
            merged_head_sha=_opt_str(raw, "merged_head_sha"),
        )


@dataclass(frozen=True, slots=True)
class ReclaimResult:
    reclaimed: bool
    status: ReclaimStatus
    reason: str
    blockers: tuple[str, ...] = ()
    branch_kept: str | None = None
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.status not in _VALID_RECLAIM_STATUSES:
            raise ProtocolValidationError(f"invalid reclaim status: {self.status}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "reclaimed": self.reclaimed,
            "status": self.status,
            "reason": self.reason,
            "blockers": list(self.blockers),
            "branch_kept": self.branch_kept,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> ReclaimResult:
        _check_schema_version(raw)
        blockers_raw = _req_list(raw, "blockers")
        return cls(
            schema_version=SCHEMA_VERSION,
            reclaimed=_req_bool(raw, "reclaimed"),
            status=cast(ReclaimStatus, _req_str(raw, "status")),
            reason=_req_str(raw, "reason", allow_empty=True),
            blockers=tuple(str(b) for b in blockers_raw),
            branch_kept=_opt_str(raw, "branch_kept"),
        )


@dataclass(frozen=True, slots=True)
class ToolCall:
    run_id: str
    call_id: str
    tool_name: str
    args: dict[str, Any]
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "call_id": self.call_id,
            "tool_name": self.tool_name,
            "args": dict(self.args),
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> ToolCall:
        _check_schema_version(raw)
        args = raw.get("args")
        if not isinstance(args, Mapping):
            raise ProtocolValidationError("args must be an object")
        return cls(
            run_id=_req_str(raw, "run_id"),
            call_id=_req_str(raw, "call_id"),
            tool_name=_req_str(raw, "tool_name"),
            args=dict(cast(Mapping[str, Any], args)),
        )


@dataclass(frozen=True, slots=True)
class ToolDecision:
    action: ToolAction
    reason: str = ""
    schema_version: str = SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.action not in _VALID_TOOL_ACTIONS:
            raise ProtocolValidationError(f"invalid tool action: {self.action}")

    def to_dict(self) -> dict[str, Any]:
        return {"schema_version": self.schema_version, "action": self.action, "reason": self.reason}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> ToolDecision:
        _check_schema_version(raw)
        return cls(
            action=cast(ToolAction, _req_str(raw, "action")),
            reason=_req_str(raw, "reason", allow_empty=True),
        )


# Asked once per tool call, before it runs. "block" refuses the call and tells the executor why; "defer" stops the run
# (RunResult.status = "interrupted") so the caller can ask a person. A failure to answer counts as "block".
ToolAuthorizer: TypeAlias = Callable[[ToolCall], Awaitable[ToolDecision]]


class RunnerPort(Protocol):
    async def capabilities(self) -> RunnerCapabilities: ...

    async def run(self, req: RunRequest, *, authorizer: ToolAuthorizer | None = None) -> RunResult: ...

    async def interrupt(self, req: InterruptRequest) -> InterruptResult: ...

    async def reclaim_workspace(self, req: ReclaimRequest) -> ReclaimResult: ...


def _check_schema_version(raw: Mapping[str, Any]) -> None:
    if raw.get("schema_version") != SCHEMA_VERSION:
        raise ProtocolValidationError(f"expected schema_version={SCHEMA_VERSION!r}, got {raw.get('schema_version')!r}")


def _req_str(raw: Mapping[str, Any], key: str, *, allow_empty: bool = False) -> str:
    val = raw.get(key)
    if not isinstance(val, str) or (not allow_empty and not val):
        raise ProtocolValidationError(f"field {key!r} must be a non-empty string")
    return val


def _opt_str(raw: Mapping[str, Any], key: str) -> str | None:
    val = raw.get(key)
    if val is None:
        return None
    if not isinstance(val, str):
        raise ProtocolValidationError(f"field {key!r} must be string or null")
    return val


def _req_bool(raw: Mapping[str, Any], key: str) -> bool:
    val = raw.get(key)
    if not isinstance(val, bool):
        raise ProtocolValidationError(f"field {key!r} must be boolean")
    return val


def _opt_bool(raw: Mapping[str, Any], key: str) -> bool:
    val = raw.get(key)
    if val is None:
        return False
    if not isinstance(val, bool):
        raise ProtocolValidationError(f"field {key!r} must be boolean")
    return val


def _req_int(raw: Mapping[str, Any], key: str, *, minimum: int = 0) -> int:
    val = raw.get(key)
    if not isinstance(val, int) or isinstance(val, bool) or val < minimum:
        raise ProtocolValidationError(f"field {key!r} must be int >= {minimum}")
    return val


def _req_float(raw: Mapping[str, Any], key: str) -> float:
    val = raw.get(key)
    if not isinstance(val, (int, float)) or isinstance(val, bool):
        raise ProtocolValidationError(f"field {key!r} must be number")
    return float(val)


def _req_list(raw: Mapping[str, Any], key: str) -> list[Any]:
    val = raw.get(key)
    if not isinstance(val, list):
        raise ProtocolValidationError(f"field {key!r} must be list")
    return cast(list[Any], val)


RunIdentity: TypeAlias = tuple[str, str, str, str, str, str, bool, str]

# Fields that do not change what runs: the key itself, a lease that a retry may carry refreshed, caller bookkeeping.
_NON_EXECUTION_FIELDS: Final = frozenset({"run_id", "credential_lease", "metadata", "schema_version"})


def run_identity(req: RunRequest, caller_id: str) -> RunIdentity:
    # run_id is an idempotency key only within one request: a retry must come from the same caller for the same
    # workspace and executor, and ask for the same execution (prompt, session, interactive, schema, timeouts, ...).
    # Hub and daemon both refuse a run_id reused for anything else.
    execution = {k: v for k, v in req.to_dict().items() if k not in _NON_EXECUTION_FIELDS}
    digest = hashlib.sha256(
        json.dumps(execution, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return (
        caller_id,
        req.namespace,
        req.project_key,
        req.owner_scope,
        req.work_key,
        req.executor_id,
        req.readonly,
        digest,
    )
