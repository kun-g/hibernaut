from __future__ import annotations

import asyncio
import json
from collections.abc import Coroutine, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any, cast

from agent_runtime.core.models import (
    Emit,
    EmitStatus,
    Event,
    RunCancelled,
    RunOutcome,
    RunTelemetry,
    SessionRef,
    Silent,
    Suspend,
    new_id,
)
from agent_runtime.core.ports import PermissionPort, RunCapabilities
from runner.protocol import (
    InterruptReason,
    InterruptRequest,
    RunRequest,
    RunResult,
    RunnerPort,
    ToolAuthorizer,
    ToolCall,
    ToolDecision,
)
from runner.registry import NoEligibleRunnerError, NodeAffinityOfflineError

_CHECKPOINT_KIND = "runner_tool_defer"
_PROMPT_ARGS_CHARS = 4000
_NODE_POLL_SECONDS = 0.5
# Free text the model words anew each time; it does not change what the call does.
_INERT_ARGS: Mapping[str, frozenset[str]] = {
    "Bash": frozenset({"description"}),
    "Shell": frozenset({"description"}),
    "run_terminal_command": frozenset({"description"}),
}


def _call_key(tool_name: str, args: Mapping[str, Any]) -> str:
    inert = _INERT_ARGS.get(tool_name, frozenset())
    return json.dumps([tool_name, {k: v for k, v in args.items() if k not in inert}], sort_keys=True)


@dataclass(frozen=True, slots=True)
class _Grant:
    # One approved call, good for one use. The request id keeps a retried Run from using it again.
    request_id: str
    key: str


@dataclass(frozen=True, slots=True)
class _DeferState:
    tool_name: str
    args: dict[str, Any]
    reason: str
    prompt: str
    readonly: bool
    interactive: bool
    grants: tuple[_Grant, ...] = ()

    def dump(self) -> str:
        return json.dumps(
            {
                "kind": _CHECKPOINT_KIND,
                "deferred_tool": {"tool_name": self.tool_name, "args": self.args},
                "reason": self.reason,
                "prompt": self.prompt,
                "readonly": self.readonly,
                "interactive": self.interactive,
                "grants": [{"request_id": g.request_id, "key": g.key} for g in self.grants],
            }
        )

    @classmethod
    def load(cls, checkpoint: str | None) -> _DeferState | None:
        try:
            parsed: object = json.loads(checkpoint or "")
            if not isinstance(parsed, dict):
                return None
            raw = cast(dict[str, Any], parsed)
            if raw.get("kind") != _CHECKPOINT_KIND:
                return None
            return cls(
                tool_name=str(raw["deferred_tool"]["tool_name"]),
                args=dict(raw["deferred_tool"]["args"]),
                reason=str(raw["reason"]),
                prompt=str(raw["prompt"]),
                readonly=bool(raw["readonly"]),
                interactive=bool(raw["interactive"]),
                grants=tuple(_Grant(request_id=str(g["request_id"]), key=str(g["key"])) for g in raw["grants"]),
            )
        except (KeyError, TypeError, ValueError):
            return None


@dataclass(slots=True)
class _ToolPolicy:
    permissions: PermissionPort
    spent: set[str]
    grants: list[_Grant] = field(default_factory=lambda: list[_Grant]())
    rejected: dict[str, str] = field(default_factory=lambda: dict[str, str]())
    deferred: tuple[ToolCall, str] | None = None

    async def authorize(self, call: ToolCall) -> ToolDecision:
        decision = self.permissions.check(call.tool_name, call.args)
        key = _call_key(call.tool_name, call.args)
        if decision.action != "block" and key in self.rejected:
            return ToolDecision(action="block", reason=self.rejected[key])
        if decision.action == "defer":
            grant = next((g for g in self.grants if g.key == key and g.request_id not in self.spent), None)
            if grant is not None:
                self.grants.remove(grant)
                self.spent.add(grant.request_id)
                return ToolDecision(action="allow", reason=f"approved in {grant.request_id}")
            self.deferred = (call, decision.reason)
        return ToolDecision(action=decision.action, reason=decision.reason)


class RunnerExecutorBridge:
    def __init__(
        self,
        *,
        runner: RunnerPort,
        executor_id: str,
        project_key: str,
        timeout_seconds: float = 1800.0,
        interrupt_poll_interval: float = 0.05,
        executor_calls_tools: bool | None = None,
        authorize_tools: bool = False,
        node_wait_seconds: float = 0.0,
    ) -> None:
        self._runner = runner
        # None means unknown. An executor that may call tools runs only with authorize_tools, which puts each call to
        # PermissionPort.check() before it is made: anything else would let the CLI act before a block or defer applies.
        self._executor_calls_tools = executor_calls_tools
        self._authorize_tools = authorize_tools
        # How long a Run waits for a node that can take it, e.g. for relay nodes to reconnect after a restart.
        self._node_wait_seconds = node_wait_seconds
        # Approvals already used. The worker may execute a Run again after an error, with the same approval event.
        self._spent_approvals: set[str] = set()
        self._executor_id = executor_id
        self._project_key = project_key
        self._timeout_seconds = timeout_seconds
        self._interrupt_poll_interval = interrupt_poll_interval

    @property
    def executor_id(self) -> str:
        return self._executor_id

    async def execute(
        self,
        events: Sequence[Event],
        session: SessionRef,
        caps: RunCapabilities,
    ) -> RunOutcome:
        gated = self._executor_calls_tools is not False
        if gated and not self._authorize_tools:
            capability = "can call tools" if self._executor_calls_tools else "has unknown tool capability"
            return Emit(
                messages=(
                    f"[{self._executor_id}] refused: executor {capability}, and per-call tool authorization "
                    "(PermissionPort.check) is not attached to it",
                ),
                session_id=session.session_id,
                executor_session_id=session.executor_session_id,
                status="refused",
            )
        run_id = new_id("run")
        work_key = session.work_key or session.correlation_key or session.session_id
        pending_id = session.pending_request_id
        state = _DeferState.load(session.checkpoint) if pending_id else None
        answers = [ev for ev in events if pending_id and ev.payload.get("approval_request_id") == pending_id]
        fresh = [ev for ev in events if ev not in answers] if state is not None else list(events)
        readonly = any(bool(ev.payload.get("readonly")) for ev in events) or (state is not None and state.readonly)
        interactive = any(bool(ev.payload.get("interactive")) for ev in events) or (state is not None and state.interactive)
        policy = _ToolPolicy(permissions=caps.permissions, spent=self._spent_approvals)
        # What the Run was asked to do, without the notes about approvals: a later approval of it starts from here.
        task_prompt = _render_events_prompt(fresh) if fresh else ""
        prompt = task_prompt
        if state is not None and pending_id and answers:
            task_prompt = "\n\n---\n\n".join(p for p in (state.prompt, task_prompt) if p)
            prompt = f"{task_prompt}\n\n---\n\n{_apply_answer(policy, state, pending_id, answers)}"
        name_hint = _derive_hb_name_hint(work_key)

        req = RunRequest(
            run_id=run_id,
            namespace="hb",
            project_key=self._project_key,
            owner_scope=session.inbox_id,
            work_key=work_key,
            name_hint=name_hint,
            executor_id=self._executor_id,
            executor_session_id=session.executor_session_id,
            readonly=readonly,
            interactive=interactive,
            prompt=prompt,
            authorize_tools=gated,
            timeout_seconds=self._timeout_seconds,
            metadata={
                "session_id": session.session_id,
                "correlation_key": session.correlation_key,
                "event_ids": [ev.event_id for ev in events],
            },
        )

        stop = asyncio.Event()
        run_task = asyncio.create_task(self._submit(req, policy.authorize if gated else None, stop))
        interrupted = False
        interrupt_accepted = False
        try:
            while not run_task.done():
                if not interrupt_accepted and caps.is_interrupted():
                    interrupted = True
                    stop.set()
                    interrupt_accepted = await self._try_interrupt(run_id, "new_event")
                await asyncio.wait({run_task}, timeout=self._interrupt_poll_interval)
        except asyncio.CancelledError:
            # The worker cancels execute() on interrupt or shutdown; the run must not outlive this Run, or the next
            # Run on the same work_key would start a second CLI in the same worktree.
            stop.set()
            await _uncancellable(self._stop_run(run_task, run_id))
            if not run_task.cancelled() and run_task.exception() is None:
                # The runner's own result says which node ran (and stopped) the Run; the worker records it.
                result = run_task.result()
                raise RunCancelled(result.error_summary or "interrupted", telemetry=_run_telemetry(result)) from None
            raise
        result = run_task.result()
        # A Run stopped for new events goes back to its inbox and asks again; only one stopped by the defer suspends.
        if policy.deferred is not None and result.status == "interrupted" and not interrupted:
            call, reason = policy.deferred
            deferred = _DeferState(
                tool_name=call.tool_name,
                args=call.args,
                reason=reason,
                prompt=task_prompt,
                readonly=readonly,
                interactive=interactive,
                grants=tuple(policy.grants),
            )
            request_id = new_id("apr")
            if (
                state is not None and pending_id and not answers
                and _call_key(state.tool_name, state.args) == _call_key(call.tool_name, call.args)
            ):
                request_id = pending_id  # the person has already been asked about this very call
            return Suspend(
                request_id=request_id,
                prompt=_approval_prompt(request_id, deferred),
                session_checkpoint=deferred.dump(),
                session_id=session.session_id,
                executor_session_id=result.executor_session_id or session.executor_session_id,
                telemetry=_run_telemetry(result),
            )
        return translate_run_result(result, session_id=session.session_id)

    async def _submit(self, req: RunRequest, authorizer: ToolAuthorizer | None, stop: asyncio.Event) -> RunResult:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._node_wait_seconds
        while True:
            try:
                if authorizer is None:
                    return await self._runner.run(req)
                return await self._runner.run(req, authorizer=authorizer)
            except (NoEligibleRunnerError, NodeAffinityOfflineError):
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise
            if await _set_within(stop, min(_NODE_POLL_SECONDS, remaining)):
                raise RunCancelled("interrupted while waiting for a runner node")

    async def _try_interrupt(self, run_id: str, reason: InterruptReason) -> bool:
        try:
            res = await self._runner.interrupt(InterruptRequest(run_id=run_id, reason=reason))
        except Exception:
            return False
        # not_found: the runner has not registered the run yet, so ask again on the next poll.
        return res.accepted or res.state == "already_finished"

    async def _stop_run(self, run_task: asyncio.Task[RunResult], run_id: str) -> None:
        accepted = False
        while not run_task.done():
            if not accepted:
                accepted = await self._try_interrupt(run_id, "shutdown")
            await asyncio.wait({run_task}, timeout=self._interrupt_poll_interval)
        if not run_task.cancelled():
            run_task.exception()


async def _uncancellable(coro: Coroutine[Any, Any, None]) -> None:
    task = asyncio.ensure_future(coro)
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            pass
    task.result()


async def _set_within(event: asyncio.Event, seconds: float) -> bool:
    try:
        await asyncio.wait_for(event.wait(), timeout=seconds)
    except TimeoutError:
        return False
    return True


def _apply_answer(policy: _ToolPolicy, state: _DeferState, request_id: str, answers: Sequence[Event]) -> str:
    key = _call_key(state.tool_name, state.args)
    approved = all(ev.payload.get("approved") is True for ev in answers)
    comments = [c.strip() for ev in answers if isinstance(c := ev.payload.get("comment"), str) and c.strip()]
    policy.grants.extend(state.grants)
    described = f"{state.tool_name} {json.dumps(state.args, ensure_ascii=False)}"
    if approved:
        policy.grants.append(_Grant(request_id=request_id, key=key))
        note = (
            f"你上一次停在一个需要人工确认的工具调用：{described}\n"
            "审批结果：已批准。可以用完全相同的参数再调用一次；参数有任何变化都需要重新确认。请继续完成任务。"
        )
    else:
        policy.rejected[key] = "rejected by the approver" + (f": {'; '.join(comments)}" if comments else "")
        note = (
            f"你上一次停在一个需要人工确认的工具调用：{described}\n"
            "审批结果：已拒绝。不要再尝试这个调用，换别的办法完成任务，做不到就说明原因。"
        )
    return note + "".join(f"\n审批人备注：{c}" for c in comments)


def _approval_prompt(request_id: str, state: _DeferState) -> str:
    shown = json.dumps(state.args, ensure_ascii=False)
    if len(shown) > _PROMPT_ARGS_CHARS:
        shown = f"{shown[:_PROMPT_ARGS_CHARS]}…（参数共 {len(shown)} 字符，已截断）"
    reason = f"\n原因：{state.reason}" if state.reason else ""
    return f"需要人工确认（{request_id}）：准备执行 {state.tool_name} {shown}{reason}"


def _run_telemetry(result: RunResult) -> RunTelemetry:
    # The worker's run ledger logs, prices and classifies a Run from its telemetry, as it does for LocalCLIExecutor.
    return RunTelemetry(
        stdout=result.stdout_excerpt,
        stderr=result.error_summary or "",
        exit_code=result.exit_code,
        error_kind=result.error_kind,
        runner_id=result.runner_id,
    )


def translate_run_result(result: RunResult, *, session_id: str) -> RunOutcome:
    return replace(_translate_status(result, session_id=session_id), telemetry=_run_telemetry(result))


def _translate_status(result: RunResult, *, session_id: str) -> RunOutcome:
    if result.status == "interrupted":
        raise RunCancelled(result.error_summary or "interrupted", telemetry=_run_telemetry(result))

    if result.status == "refused":
        return Emit(
            messages=(f"[{result.executor_id}] refused: {result.error_summary or 'request refused'}",),
            session_id=session_id,
            executor_session_id=result.executor_session_id,
            status="refused",
        )

    if result.status in ("failed", "timed_out"):
        return Emit(
            messages=(f"[{result.executor_id}] failed ({result.error_kind or result.status}): {result.error_summary or ''}".strip(),),
            session_id=session_id,
            executor_session_id=result.executor_session_id,
            status="failed",
        )

    so = result.structured_output
    if so is not None:
        outcome = so.get("outcome")
        if outcome == "silent":
            return Silent(executor_session_id=result.executor_session_id)
        if outcome == "suspend" and isinstance(so.get("prompt"), str):
            req_id = str(so.get("request_id") or new_id("apr"))
            checkpoint = json.dumps(so.get("checkpoint", {}), ensure_ascii=False)
            return Suspend(
                request_id=req_id,
                prompt=so["prompt"],
                session_checkpoint=checkpoint,
                session_id=session_id,
                executor_session_id=result.executor_session_id,
            )
        if isinstance(so.get("message"), str):
            status_val: EmitStatus = "done"
            return Emit(
                messages=(so["message"],),
                session_id=session_id,
                executor_session_id=result.executor_session_id,
                status=status_val,
            )

    text = (result.final_text if result.final_text is not None else result.stdout_excerpt).strip()
    if not text:
        return Silent(executor_session_id=result.executor_session_id)
    return Emit(
        messages=(text,),
        session_id=session_id,
        executor_session_id=result.executor_session_id,
        status="done",
    )


def _render_events_prompt(events: Sequence[Event]) -> str:
    parts: list[str] = []
    for ev in events:
        body = ev.payload.get("body") or ev.payload.get("text") or ev.payload.get("requirements")
        title = ev.payload.get("title") or ev.payload.get("subject")
        if title and body:
            parts.append(f"# {title}\n\n{body}")
        elif body:
            parts.append(str(body))
        elif title:
            parts.append(str(title))
        else:
            parts.append(json.dumps(ev.payload, ensure_ascii=False))
    return "\n\n---\n\n".join(parts)


def _derive_hb_name_hint(work_key: str) -> str | None:
    if "#" in work_key:
        num = work_key.rsplit("#", 1)[1].strip()
        if num.isdigit():
            return f"issue-{num}"
    return None
