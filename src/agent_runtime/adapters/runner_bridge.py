from __future__ import annotations

import asyncio
import json
from collections.abc import Coroutine, Sequence
from dataclasses import replace
from typing import Any

from agent_runtime.core.models import (
    Emit,
    EmitStatus,
    Event,
    RunOutcome,
    RunTelemetry,
    SessionRef,
    Silent,
    Suspend,
    new_id,
)
from agent_runtime.core.ports import RunCapabilities
from runner.protocol import InterruptReason, InterruptRequest, RunRequest, RunResult, RunnerPort


class RunnerExecutorBridge:
    def __init__(
        self,
        *,
        runner: RunnerPort,
        executor_id: str,
        project_key: str,
        timeout_seconds: float = 1800.0,
        interrupt_poll_interval: float = 0.05,
    ) -> None:
        self._runner = runner
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
        run_id = new_id("run")
        work_key = session.work_key or session.correlation_key or session.session_id
        readonly = any(bool(ev.payload.get("readonly")) for ev in events)
        interactive = any(bool(ev.payload.get("interactive")) for ev in events)
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
            prompt=_render_events_prompt(events),
            timeout_seconds=self._timeout_seconds,
            metadata={
                "session_id": session.session_id,
                "correlation_key": session.correlation_key,
                "event_ids": [ev.event_id for ev in events],
            },
        )

        run_task = asyncio.create_task(self._runner.run(req))
        interrupt_accepted = False
        try:
            while not run_task.done():
                if not interrupt_accepted and caps.is_interrupted():
                    interrupt_accepted = await self._try_interrupt(run_id, "new_event")
                await asyncio.wait({run_task}, timeout=self._interrupt_poll_interval)
        except asyncio.CancelledError:
            # The worker cancels execute() on interrupt or shutdown; the run must not outlive this Run, or the next
            # Run on the same work_key would start a second CLI in the same worktree.
            await _uncancellable(self._stop_run(run_task, run_id))
            raise
        result = run_task.result()
        return translate_run_result(result, session_id=session.session_id)

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


def translate_run_result(result: RunResult, *, session_id: str) -> RunOutcome:
    # The worker's run ledger logs, prices and classifies a Run from its telemetry, as it does for LocalCLIExecutor.
    telemetry = RunTelemetry(
        stdout=result.stdout_excerpt,
        stderr=result.error_summary or "",
        exit_code=result.exit_code,
    )
    return replace(_translate_status(result, session_id=session_id), telemetry=telemetry)


def _translate_status(result: RunResult, *, session_id: str) -> RunOutcome:
    if result.status == "interrupted":
        raise asyncio.CancelledError(result.error_summary or "interrupted")

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

    text = result.stdout_excerpt.strip()
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
