from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, replace
from typing import Any, Callable, Mapping, ParamSpec, TypeVar, cast

from runner.cli_drivers import (
    CLIDriver,
    NoopStreamPipe,
    ParsedCLIOutput,
    StreamEvent,
    StreamPipe,
    default_cli_drivers,
    format_prefixed_session_id,
)
from runner.config import NamespacePolicy, ProjectCheckout, RunnerConfig
from runner.protocol import (
    ErrorKind,
    InterruptRequest,
    InterruptResult,
    ReclaimRequest,
    ReclaimResult,
    RunIdentity,
    RunRequest,
    RunResult,
    RunStatus,
    RunnerCapabilities,
    SlotSnapshot,
    ToolAuthorizer,
    run_identity,
    utc_now_iso,
)
from runner.supervisor import ChildEnvError, ProcessSupervisor, build_child_env
from runner.tool_gate import ToolGate, ToolGateBinding
from runner.worktree import (
    GitWorktreeBackend,
    OrcaWorktreeBackend,
    WorkspaceIsolationError,
    WorktreeManager,
    worktree_fingerprint,
)


_P = ParamSpec("_P")
_T = TypeVar("_T")


async def _to_thread_to_completion(fn: Callable[_P, _T], /, *args: _P.args, **kwargs: _P.kwargs) -> _T:
    # For worktree work that changes the disk: a cancelled await would otherwise return while the thread keeps
    # changing the workspace, and the caller would give up its workspace claim too early. The thread always runs to
    # its end; a cancellation is raised after that.
    task = asyncio.ensure_future(asyncio.to_thread(fn, *args, **kwargs))
    cancelled = False
    while True:
        try:
            result = await asyncio.shield(task)
            break
        except asyncio.CancelledError:
            if task.done():
                if task.cancelled():
                    raise
                cancelled = True
                result = task.result()
                break
            cancelled = True
    if cancelled:
        raise asyncio.CancelledError
    return result


@dataclass(slots=True)
class _InFlightRun:
    future: asyncio.Future[RunResult]
    cancel_event: asyncio.Event
    authorizer: ToolAuthorizer | None = None
    hook_missing: bool = False
    grace_seconds: float = 2.0


def _tool_authorization_refusal(req: RunRequest, driver: CLIDriver, authorizer: ToolAuthorizer | None) -> str | None:
    if authorizer is None:
        if req.authorize_tools:
            return "the request requires tool authorization, but no authorizer is attached"
        if driver.calls_tools is True:
            return f"executor {req.executor_id!r} calls tools, and no tool authorizer is attached"
        return None
    if driver.tool_hook is None:
        return f"tool authorization is required, but executor {req.executor_id!r} has no tool hook"
    return None


class RunnerDaemon:
    def __init__(
        self,
        config: RunnerConfig,
        *,
        drivers: Mapping[str, CLIDriver] | None = None,
        worktree_manager: WorktreeManager | None = None,
        supervisor: ProcessSupervisor | None = None,
        stream_pipe: StreamPipe | None = None,
        hook_arm_timeout_seconds: float = 60.0,
    ) -> None:
        self._config = config
        self._drivers: dict[str, CLIDriver] = dict(drivers) if drivers is not None else default_cli_drivers()
        backend = OrcaWorktreeBackend() if config.worktree_backend == "orca" else GitWorktreeBackend()
        self._worktrees = worktree_manager or WorktreeManager(
            db_path=config.state_dir / "runner_workspaces.db",
            backend=backend,
        )
        self._supervisor = supervisor or ProcessSupervisor(
            runner_id=config.runner_id,
            logs_dir=config.logs_dir,
        )
        # How long a CLI may run before its tool hook reports in. Cursor takes up to 15 s to start a session.
        self._hook_arm_timeout = hook_arm_timeout_seconds
        self._stream_pipe: StreamPipe = stream_pipe or NoopStreamPipe()
        self._active_runs: dict[str, _InFlightRun] = {}
        self._completed_runs: dict[str, RunResult] = {}
        self._run_identities: dict[str, RunIdentity] = {}
        self._active_slots = 0
        # One process tree per workspace: an entry stays until its run's process group has been reaped.
        self._busy_workspaces: dict[tuple[str, str, str, str, bool], str] = {}

    @property
    def config(self) -> RunnerConfig:
        return self._config

    @property
    def worktrees(self) -> WorktreeManager:
        return self._worktrees

    def reconfigure(self, *, projects: Mapping[str, ProjectCheckout], drivers: Mapping[str, CLIDriver]) -> None:
        # For the next runs only: a run in flight holds the checkout and driver it started with.
        self._config = replace(self._config, projects=dict(projects))
        self._drivers = dict(drivers)

    def close(self) -> None:
        self._worktrees.close()

    def _owned_by(self, run_id: str, caller_id: str) -> bool:
        known = self._run_identities.get(run_id)
        return known is not None and known[0] == caller_id

    def query_run(self, run_id: str, *, caller_id: str) -> RunResult | None:
        # Another caller learns nothing about a run, not even that it exists.
        if not self._owned_by(run_id, caller_id):
            return None
        return self._completed_runs.get(run_id)

    def knows_run(self, run_id: str) -> bool:
        return run_id in self._active_runs or run_id in self._completed_runs

    def capabilities_sync(self) -> RunnerCapabilities:
        available = max(0, self._config.max_concurrent - self._active_slots)
        return RunnerCapabilities(
            runner_id=self._config.runner_id,
            namespaces=tuple(sorted(self._config.namespaces.keys())),
            projects=tuple(sorted(self._config.projects.keys())),
            worktree_backend=self._config.worktree_backend,
            interactive_surface=self._config.interactive_surface,
            executors=tuple(
                drv.capability(interactive_surface=self._config.interactive_surface)
                for drv in self._drivers.values()
            ),
            slots=SlotSnapshot(
                max_concurrent=self._config.max_concurrent,
                active_runs=self._active_slots,
                available=available,
            ),
            labels=dict(self._config.labels),
        )

    async def capabilities(self) -> RunnerCapabilities:
        return self.capabilities_sync()

    async def run(self, req: RunRequest, *, caller_id: str, authorizer: ToolAuthorizer | None = None) -> RunResult:
        started_at = utc_now_iso()
        start_mono = time.monotonic()

        # Authorization comes before any reuse of a run_id, and a reused run_id must be the same request: otherwise a
        # caller could read, or wait on, another task's run.
        try:
            policy = self._config.authorize_namespace(req.namespace, caller_id)
        except PermissionError as exc:
            return self._refused(req, started_at, start_mono, "permission_denied", str(exc))
        identity = run_identity(req, caller_id)
        known = self._run_identities.get(req.run_id)
        if known is not None and known != identity:
            return self._refused(
                req, started_at, start_mono, "permission_denied", f"run_id {req.run_id!r} belongs to another request"
            )
        if (done := self._completed_runs.get(req.run_id)) is not None:
            return done
        if (in_flight := self._active_runs.get(req.run_id)) is not None:
            return await asyncio.shield(in_flight.future)

        if self._config.readonly_only and not req.readonly:
            return self._refused(
                req, started_at, start_mono, "permission_denied", f"runner {self._config.runner_id!r} only takes readonly runs"
            )

        project = self._config.projects.get(req.project_key)
        if project is None:
            return self._refused(req, started_at, start_mono, "config_error", f"unknown project_key: {req.project_key}")

        driver = self._drivers.get(req.executor_id)
        if driver is None:
            return self._refused(req, started_at, start_mono, "config_error", f"unknown executor_id: {req.executor_id}")

        refusal = _tool_authorization_refusal(req, driver, authorizer)
        if refusal is not None:
            return self._refused(req, started_at, start_mono, "permission_denied", refusal)

        if req.interactive and self._config.interactive_surface != "orca":
            return self._refused(
                req,
                started_at,
                start_mono,
                "config_error",
                f"interactive=true requested but runner {self._config.runner_id!r} has interactive_surface={self._config.interactive_surface!r}",
            )

        if req.interactive and driver.capability(interactive_surface=self._config.interactive_surface).interactive_surface != "orca":
            # Never a silent headless fallback (docs/runner.md §4): the driver must launch the CLI in an Orca terminal.
            return self._refused(
                req,
                started_at,
                start_mono,
                "config_error",
                f"interactive=true requested but executor {req.executor_id!r} cannot launch an Orca terminal",
            )

        if self._active_slots >= self._config.max_concurrent:
            return self._refused(req, started_at, start_mono, "rate_limit", "slots_full")

        loop = asyncio.get_running_loop()
        entry = _InFlightRun(future=loop.create_future(), cancel_event=asyncio.Event(), authorizer=authorizer)
        self._active_runs[req.run_id] = entry
        self._run_identities[req.run_id] = identity
        self._active_slots += 1

        try:
            result = await self._execute_slot(req, policy=policy, project=project, driver=driver, entry=entry, started_at=started_at, start_mono=start_mono)
            # A refusal (workspace_busy, isolation) ran nothing: the same request may run once the cause is gone. The
            # hub caches the same way.
            if result.status != "refused":
                self._completed_runs[req.run_id] = result
            if not entry.future.done():
                entry.future.set_result(result)
            return result
        except Exception as exc:
            err_result = RunResult(
                run_id=req.run_id,
                runner_id=self._config.runner_id,
                status="failed",
                executor_id=req.executor_id,
                error_kind="crash",
                error_summary=str(exc),
                started_at=started_at,
                ended_at=utc_now_iso(),
                duration_ms=max(0, int((time.monotonic() - start_mono) * 1000)),
            )
            self._completed_runs[req.run_id] = err_result
            if not entry.future.done():
                entry.future.set_result(err_result)
            return err_result
        except BaseException:
            # Cancelled (e.g. the relay connection that carried the run went away). The supervisor has already
            # reaped the process group; callers joined on this run_id still get a terminal result.
            cancelled = RunResult(
                run_id=req.run_id,
                runner_id=self._config.runner_id,
                status="interrupted",
                executor_id=req.executor_id,
                error_summary="run cancelled before completion",
                started_at=started_at,
                ended_at=utc_now_iso(),
                duration_ms=max(0, int((time.monotonic() - start_mono) * 1000)),
            )
            self._completed_runs[req.run_id] = cancelled
            if not entry.future.done():
                entry.future.set_result(cancelled)
            raise
        finally:
            self._active_slots = max(0, self._active_slots - 1)
            self._active_runs.pop(req.run_id, None)

    async def interrupt(self, req: InterruptRequest, *, caller_id: str) -> InterruptResult:
        if not self._owned_by(req.run_id, caller_id):
            return InterruptResult(run_id=req.run_id, accepted=False, state="not_found")
        if req.run_id in self._completed_runs:
            return InterruptResult(run_id=req.run_id, accepted=False, state="already_finished")
        entry = self._active_runs.get(req.run_id)
        if entry is None:
            return InterruptResult(run_id=req.run_id, accepted=False, state="not_found")
        entry.grace_seconds = req.grace_seconds
        entry.cancel_event.set()
        return InterruptResult(run_id=req.run_id, accepted=True, state="cancelling")

    async def reclaim_workspace(self, req: ReclaimRequest, *, caller_id: str) -> ReclaimResult:
        try:
            policy = self._config.authorize_namespace(req.namespace, caller_id)
        except PermissionError as exc:
            return ReclaimResult(
                reclaimed=False,
                status="blocked",
                reason=str(exc),
                blockers=("unauthorized_namespace",),
            )
        project = self._config.projects.get(req.project_key)
        if project is None:
            return ReclaimResult(
                reclaimed=False,
                status="error",
                reason=f"unknown project_key: {req.project_key}",
                blockers=("unknown_project",),
            )
        active = [
            run_id
            for (ns, pk, owner, wk, _), run_id in self._busy_workspaces.items()
            if (ns, pk, owner, wk) == (req.namespace, req.project_key, req.owner_scope, req.work_key)
        ]
        if active:
            return ReclaimResult(
                reclaimed=False,
                status="blocked",
                reason=f"runs still active on this workspace: {', '.join(sorted(active))}",
                blockers=("run_active",),
            )
        # Both directories of the workspace (task and readonly) stay claimed until the reclaim thread is done, so a run
        # that arrives after the check above cannot mount a directory that is about to be deleted. git/orca run off
        # the event loop so other runs' watchdogs and interrupts keep ticking.
        keys = [(req.namespace, req.project_key, req.owner_scope, req.work_key, ro) for ro in (False, True)]
        for key in keys:
            self._busy_workspaces[key] = "(reclaim)"
        try:
            return await _to_thread_to_completion(
                self._worktrees.reclaim,
                policy=policy,
                project=project,
                owner_scope=req.owner_scope,
                work_key=req.work_key,
                merged_head_sha=req.merged_head_sha,
            )
        finally:
            for key in keys:
                if self._busy_workspaces.get(key) == "(reclaim)":
                    del self._busy_workspaces[key]

    async def _execute_slot(
        self,
        req: RunRequest,
        *,
        policy: NamespacePolicy,
        project: ProjectCheckout,
        driver: CLIDriver,
        entry: _InFlightRun,
        started_at: str,
        start_mono: float,
    ) -> RunResult:
        # Each key maps to exactly one directory (the task's worktree, or its readonly worktree); resolve() may
        # reset that directory, so the claim comes first.
        busy_key = (req.namespace, req.project_key, req.owner_scope, req.work_key, req.readonly)
        holder = self._busy_workspaces.get(busy_key)
        if holder is not None:
            return self._refused(
                req, started_at, start_mono, "rate_limit", f"workspace_busy: work_key {req.work_key!r} is in use by run {holder}"
            )
        self._busy_workspaces[busy_key] = req.run_id
        try:
            return await self._execute_claimed(
                req, policy=policy, project=project, driver=driver, entry=entry, started_at=started_at, start_mono=start_mono
            )
        finally:
            try:
                if req.readonly:
                    # A readonly worktree lives only for its run; nothing is left for a later reclaim to find.
                    await _to_thread_to_completion(
                        self._worktrees.remove_readonly,
                        policy=policy,
                        project=project,
                        owner_scope=req.owner_scope,
                        work_key=req.work_key,
                    )
            except Exception:  # noqa: BLE001
                logging.getLogger(__name__).warning("readonly worktree of %s was not removed", req.run_id, exc_info=True)
            finally:
                self._busy_workspaces.pop(busy_key, None)

    async def _execute_claimed(
        self,
        req: RunRequest,
        *,
        policy: NamespacePolicy,
        project: ProjectCheckout,
        driver: CLIDriver,
        entry: _InFlightRun,
        started_at: str,
        start_mono: float,
    ) -> RunResult:
        try:
            resolved = await _to_thread_to_completion(
                self._worktrees.resolve,
                policy=policy,
                project=project,
                owner_scope=req.owner_scope,
                work_key=req.work_key,
                readonly=req.readonly,
                name_hint=req.name_hint,
            )
            if not req.readonly:
                await _to_thread_to_completion(
                    self._worktrees.assert_writable,
                    policy=policy,
                    project=project,
                    owner_scope=req.owner_scope,
                    work_key=req.work_key,
                    workdir=resolved.workdir,
                )
        except (WorkspaceIsolationError, ChildEnvError) as exc:
            return self._refused(req, started_at, start_mono, "permission_denied", str(exc))

        pre_fp = await asyncio.to_thread(worktree_fingerprint, resolved.workdir) if req.readonly else None
        gate: ToolGate | None = None
        arm_watch: asyncio.Task[None] | None = None
        result_line: str | None = None

        def _on_line(line: str) -> None:
            nonlocal result_line
            stripped = line.strip()
            if stripped.startswith("{"):
                try:
                    parsed = json.loads(stripped)
                    if isinstance(parsed, dict):
                        payload = cast(dict[str, Any], parsed)
                        if payload.get("type") == "result":
                            result_line = stripped
                        self._stream_pipe.on_event(StreamEvent(kind="json", payload=payload))
                        return
                except json.JSONDecodeError:
                    pass
            self._stream_pipe.on_event(StreamEvent(kind="text", payload=line))

        try:
            binding: ToolGateBinding | None = None
            if entry.authorizer is not None:
                gate = ToolGate(
                    run_id=req.run_id,
                    authorizer=entry.authorizer,
                    stopped=entry.cancel_event.is_set,
                    on_defer=entry.cancel_event.set,
                )
                binding = await gate.open()
            spec = driver.build_command(req, resolved.workdir, tool_gate=binding)
            try:
                child_env = build_child_env(
                    os.environ,
                    extra_env=spec.env,
                    credential_lease=req.credential_lease,
                )
            except ChildEnvError as exc:
                return self._refused(req, started_at, start_mono, "permission_denied", str(exc))
            if gate is not None:
                arm_watch = asyncio.create_task(self._stop_unless_armed(gate, entry))
            sup_out = await self._supervisor.execute(
                run_id=req.run_id,
                argv=spec.argv,
                cwd=resolved.workdir,
                env=child_env,
                stdin_text=spec.stdin_text,
                timeout_seconds=req.timeout_seconds,
                idle_timeout_seconds=spec.idle_timeout_seconds,
                cancel_event=entry.cancel_event,
                grace_seconds=entry.grace_seconds,
                on_stdout_line=_on_line,
            )
        finally:
            if arm_watch is not None:
                arm_watch.cancel()
                await asyncio.gather(arm_watch, return_exceptions=True)
            if gate is not None:
                await gate.close()

        if pre_fp is not None:
            post_fp = await asyncio.to_thread(worktree_fingerprint, resolved.workdir)
            if post_fp != pre_fp:
                await _to_thread_to_completion(
                    self._worktrees.restore_readonly,
                    policy=policy,
                    project=project,
                    owner_scope=req.owner_scope,
                    work_key=req.work_key,
                    head_sha=resolved.ref.head_sha,
                )
                return RunResult(
                    run_id=req.run_id,
                    runner_id=self._config.runner_id,
                    status="failed",
                    executor_id=req.executor_id,
                    exit_code=sup_out.exit_code,
                    error_kind="permission_denied",
                    error_summary="readonly run mutated worktree; result discarded",
                    stdout_excerpt=sup_out.stdout_excerpt,
                    log_ref=sup_out.log_ref,
                    worktree_ref=resolved.ref,
                    started_at=started_at,
                    ended_at=utc_now_iso(),
                    duration_ms=max(0, int((time.monotonic() - start_mono) * 1000)),
                )

        stdout = sup_out.stdout_excerpt
        if result_line is not None and result_line not in stdout:
            # The excerpt keeps the start of a long stream; the line that ends it carries the reply.
            stdout = f"{stdout}\n{result_line}"
        parsed = driver.parse_output(stdout, sup_out.stderr_excerpt, sup_out.exit_code)
        if parsed.executor_session_id is None and gate is not None:
            # A run stopped by a deferred call may not have printed its session id yet.
            parsed = replace(
                parsed, executor_session_id=format_prefixed_session_id(req.executor_id, gate.native_session_id)
            )
        status: RunStatus
        error_kind: ErrorKind | None = None
        error_summary: str | None = None

        if sup_out.status == "succeeded":
            status = "succeeded"
        elif sup_out.status == "interrupted":
            status = "interrupted"
            error_summary = "run interrupted by caller"
        elif sup_out.status == "timed_out":
            status = "timed_out"
            error_kind = "timeout"
            error_summary = f"run timed out ({sup_out.timeout_kind or 'hard'})"
        else:
            status = "failed"
            error_kind = "tool_error"
            error_summary = (sup_out.stderr_excerpt.strip() or f"exit_code={sup_out.exit_code}")[:500]

        if gate is not None and gate.deferred is not None:
            call, decision = gate.deferred
            status, error_kind = "interrupted", None
            error_summary = f"tool call {call.tool_name!r} deferred for approval: {decision.reason}"[:500]
        elif entry.hook_missing or (gate is not None and not gate.armed.is_set() and status == "succeeded"):
            # Without its hook the CLI acts unchecked, so nothing it says is delivered.
            status, error_kind = "failed", "permission_denied"
            error_summary = f"executor {req.executor_id!r} did not load its tool authorization hook; result discarded"
            parsed = ParsedCLIOutput()

        return RunResult(
            run_id=req.run_id,
            runner_id=self._config.runner_id,
            status=status,
            executor_id=req.executor_id,
            executor_session_id=parsed.executor_session_id,
            exit_code=sup_out.exit_code,
            error_kind=error_kind,
            error_summary=error_summary,
            stdout_excerpt=sup_out.stdout_excerpt,
            final_text=parsed.final_text,
            structured_output=parsed.structured_output,
            log_ref=sup_out.log_ref,
            worktree_ref=resolved.ref,
            started_at=started_at,
            ended_at=utc_now_iso(),
            duration_ms=max(0, int((time.monotonic() - start_mono) * 1000)),
        )

    async def _stop_unless_armed(self, gate: ToolGate, entry: _InFlightRun) -> None:
        try:
            await asyncio.wait_for(gate.armed.wait(), timeout=self._hook_arm_timeout)
        except TimeoutError:
            if not entry.cancel_event.is_set():
                entry.hook_missing = True
                entry.cancel_event.set()

    def _refused(
        self,
        req: RunRequest,
        started_at: str,
        start_mono: float,
        error_kind: ErrorKind,
        summary: str,
    ) -> RunResult:
        return RunResult(
            run_id=req.run_id,
            runner_id=self._config.runner_id,
            status="refused",
            executor_id=req.executor_id,
            error_kind=error_kind,
            error_summary=summary,
            started_at=started_at,
            ended_at=utc_now_iso(),
            duration_ms=max(0, int((time.monotonic() - start_mono) * 1000)),
        )
