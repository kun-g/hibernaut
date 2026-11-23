from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from agent_runtime.adapters.memory_fs import (
    APPROVE_TOOL,
    MEMORY_TOOLS,
    BaselineSegments,
    baseline_segments,
    call_memory_tool,
    describe_proposal,
    write_memory,
)
from agent_runtime.core.models import (
    Emit,
    Event,
    RunOutcome,
    SessionRef,
    Silent,
    Suspend,
    new_id,
)
from agent_runtime.core.ports import RunCapabilities

ToolHandler = Callable[[dict[str, Any], RunCapabilities], str]


@dataclass(frozen=True, slots=True)
class ToolCallRequest:
    tool_name: str
    args: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class LLMMessage:
    role: str
    content: str
    cache_control: str | None = None


@dataclass(frozen=True, slots=True)
class LLMReply:
    tool_calls: tuple[ToolCallRequest, ...] = ()
    emit_messages: tuple[str, ...] = ()
    silent: bool = False


class LLMBackend(Protocol):
    async def complete(
        self,
        *,
        system_prefix: str,
        messages: Sequence[LLMMessage],
        available_tools: Sequence[str],
    ) -> LLMReply: ...


class WhiteboxExecutor:
    def __init__(
        self,
        *,
        llm: LLMBackend,
        tools: dict[str, ToolHandler] | None = None,
        system_prompt: str = "You are a quiet, reliable always-on background agent.",
        max_steps: int = 8,
        max_messages_before_compact: int = 24,
        executor_id: str = "whitebox",
    ) -> None:
        self._llm = llm
        self._custom_tools: dict[str, ToolHandler] = dict(tools or {})
        self._system_prompt = system_prompt
        self._max_steps = max_steps
        self._max_messages_before_compact = max_messages_before_compact
        self._executor_id = executor_id

    @property
    def executor_id(self) -> str:
        return self._executor_id

    def _format_event_message(self, evt: Event) -> LLMMessage:
        return LLMMessage(
            role="user",
            content=json.dumps(
                {
                    "source": evt.source,
                    "occurred_at": evt.occurred_at,
                    "payload": evt.payload,
                },
                ensure_ascii=False,
            ),
        )

    async def execute(
        self,
        events: Sequence[Event],
        session: SessionRef | None,
        caps: RunCapabilities,
    ) -> RunOutcome:
        session_id = session.session_id if session is not None else new_id("sess")
        baseline = baseline_segments(caps.memory)
        system_prefix = self._build_cacheable_prefix(baseline)
        # Goes with every request but never into messages, so neither compaction nor a checkpoint holds a copy.
        volatile_tail = self._build_volatile_tail(baseline)

        messages: list[LLMMessage] = []
        pending_deferred: dict[str, Any] | None = None

        if session is not None and session.checkpoint:
            state = json.loads(session.checkpoint)
            messages = [
                LLMMessage(role=m["role"], content=m["content"])
                for m in state.get("messages", [])
            ]
            pending_deferred = state.get("deferred_tool")

        # Append incoming events first to preserve causal order:
        # User/Approval Event -> Deferred Tool Execution -> LLM Observation.
        for evt in events:
            messages.append(self._format_event_message(evt))

        if pending_deferred is not None:
            approved = any(bool(e.payload.get("approved")) for e in events)
            tool_name = str(pending_deferred["tool_name"])
            tool_args = dict(pending_deferred["args"])
            if approved:
                result_text = await self._run_tool(tool_name, tool_args, caps, actor="human approval")
                messages.append(
                    LLMMessage(
                        role="tool",
                        content=f"[APPROVED] {tool_name} -> {result_text}",
                    )
                )
            else:
                messages.append(
                    LLMMessage(
                        role="tool",
                        content=f"[REJECTED BY USER] {tool_name} was not executed.",
                    )
                )

        available_tools = sorted(
            {"read_memory", "write_memory", "schedule_timer", *MEMORY_TOOLS, *self._custom_tools.keys()}
        )
        emitted: list[str] = []

        for step in range(self._max_steps):
            if caps.is_interrupted():
                raise asyncio.CancelledError("Run interrupted by new event")

            if step > 0:
                folded = caps.poll_folded_events()
                for evt in folded:
                    messages.append(self._format_event_message(evt))

            messages = self._compact_if_needed(messages)
            reply = await self._llm.complete(
                system_prefix=system_prefix,
                messages=[*volatile_tail, *messages],
                available_tools=available_tools,
            )

            if caps.is_interrupted():
                raise asyncio.CancelledError("Run interrupted by new event")

            if reply.emit_messages:
                emitted.extend(reply.emit_messages)

            if not reply.tool_calls:
                # Re-check folded events before exiting so mid-turn arrivals are absorbed.
                trailing_folded = caps.poll_folded_events()
                if trailing_folded:
                    for evt in trailing_folded:
                        messages.append(self._format_event_message(evt))
                    continue
                if emitted:
                    return Emit(messages=tuple(emitted), session_id=session_id)
                return Silent()

            for call in reply.tool_calls:
                if caps.is_interrupted():
                    raise asyncio.CancelledError("Run interrupted by new event")

                decision = caps.permissions.check(call.tool_name, call.args)
                if decision.action == "block":
                    messages.append(
                        LLMMessage(
                            role="tool",
                            content=f"[BLOCKED] {call.tool_name}: {decision.reason}",
                        )
                    )
                    continue

                if decision.action == "defer":
                    request_id = new_id("req")
                    checkpoint = json.dumps(
                        {
                            "messages": [{"role": m.role, "content": m.content} for m in messages],
                            "deferred_tool": {
                                "tool_name": call.tool_name,
                                "args": call.args,
                            },
                        },
                        ensure_ascii=False,
                    )
                    prompt = (
                        f"需要人工确认（{request_id}）：准备执行 `{call.tool_name}`，"
                        f"参数={json.dumps(call.args, ensure_ascii=False)}。"
                        f"原因：{decision.reason}"
                    )
                    if call.tool_name == APPROVE_TOOL:
                        prompt = f"{prompt}\n\n{describe_proposal(caps.memory, call.args)}"
                    return Suspend(
                        request_id=request_id,
                        prompt=prompt,
                        session_checkpoint=checkpoint,
                        session_id=session_id,
                    )

                result_text = await self._run_tool(call.tool_name, call.args, caps, actor=self._executor_id)
                messages.append(
                    LLMMessage(
                        role="tool",
                        content=f"[OK] {call.tool_name} -> {result_text}",
                    )
                )

        if emitted:
            return Emit(messages=tuple(emitted), session_id=session_id)
        return Silent()

    def _build_cacheable_prefix(self, baseline: BaselineSegments) -> str:
        sections = [self._system_prompt]
        sections.extend(f"## {name}\n{text or '(empty)'}" for name, text in baseline.stable)
        return "\n\n".join(sections)

    @staticmethod
    def _build_volatile_tail(baseline: BaselineSegments) -> list[LLMMessage]:
        return [
            LLMMessage(role="system", content=f"## {name}\n{text}") for name, text in baseline.volatile if text
        ]

    def _compact_if_needed(self, messages: list[LLMMessage]) -> list[LLMMessage]:
        if len(messages) <= self._max_messages_before_compact:
            return messages
        head_summary = LLMMessage(
            role="system",
            content=f"[Compacted {len(messages) - 8} earlier messages; key state persisted in Memory]",
        )
        return [head_summary, *messages[-8:]]

    async def _run_tool(self, tool_name: str, args: dict[str, Any], caps: RunCapabilities, *, actor: str) -> str:
        try:
            if tool_name in MEMORY_TOOLS and tool_name not in self._custom_tools:
                # A search may have to index a whole knowledge mount first.
                return await asyncio.to_thread(call_memory_tool, caps.memory, tool_name, args, actor=actor)
            if tool_name == "write_memory":
                return write_memory(caps.memory, str(args["path"]), str(args["content"]))
        except (OSError, ValueError) as exc:
            # Memory Hub refused (budget, scope, no such document): for the agent to handle, not a failed Run.
            return f"[ERROR] {exc}"
        return self._invoke_tool(tool_name, args, caps)

    def _invoke_tool(
        self,
        tool_name: str,
        args: dict[str, Any],
        caps: RunCapabilities,
    ) -> str:
        if tool_name == "read_memory":
            return caps.memory.read_on_demand(str(args["path"]))
        if tool_name == "schedule_timer":
            timer_id = caps.timers.schedule(
                kind=args.get("kind", "follow_up"),
                due_at=str(args["due_at"]),
                payload=dict(args.get("payload", {})),
                recurrence_seconds=args.get("recurrence_seconds"),
                inbox_id=str(args.get("inbox_id", "default")),
            )
            return f"scheduled {timer_id}"
        if tool_name in self._custom_tools:
            return self._custom_tools[tool_name](args, caps)
        raise KeyError(f"Unknown tool: {tool_name}")


class RuleBasedDemoLLM:
    async def complete(
        self,
        *,
        system_prefix: str,
        messages: Sequence[LLMMessage],
        available_tools: Sequence[str],
    ) -> LLMReply:
        _ = (system_prefix, available_tools)
        last = messages[-1]

        if last.role == "tool" and last.content.startswith("[APPROVED]"):
            return LLMReply(emit_messages=(f"已完成已批准操作：{last.content}",))

        if last.role == "tool" and last.content.startswith("[REJECTED BY USER]"):
            return LLMReply(emit_messages=(f"已取消操作：{last.content}",))

        if last.role == "tool" and last.content.startswith("[BLOCKED]"):
            return LLMReply(emit_messages=(f"高危操作已被安全策略拦截，已中止执行：{last.content}",))

        if last.role == "tool" and last.content.startswith("[OK]"):
            return LLMReply(emit_messages=(f"后台任务执行完成：{last.content}",))

        try:
            event_obj = json.loads(last.content)
        except json.JSONDecodeError:
            return LLMReply(silent=True)

        payload = event_obj.get("payload", {})
        action = payload.get("action")

        if action == "heartbeat_check":
            return LLMReply(silent=True)

        if action == "send_email":
            return LLMReply(
                tool_calls=(
                    ToolCallRequest(
                        tool_name="send_email",
                        args={
                            "to": payload.get("to", "unknown@example.com"),
                            "subject": payload.get("subject", "Status Update"),
                        },
                    ),
                )
            )

        if action == "dangerous_cmd":
            return LLMReply(
                tool_calls=(
                    ToolCallRequest(
                        tool_name="fetch_status",
                        args={"cmd": "rm -rf /tmp/data"},
                    ),
                )
            )

        if action == "remember":
            return LLMReply(
                tool_calls=(
                    ToolCallRequest(
                        tool_name="write_memory",
                        args={
                            "path": str(payload.get("path", "notes/fact.md")),
                            "content": str(payload.get("content", "")),
                        },
                    ),
                )
            )

        if "text" in payload:
            return LLMReply(emit_messages=(f"收到事件：{payload['text']}",))

        return LLMReply(silent=True)
