from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import shlex
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from agent_runtime.core.models import TASK_CONTEXT_KEY
from agent_runtime.core.ports import ChannelPort

_log = logging.getLogger(__name__)

NONE = "无"
APPROVAL_EVENT_SOURCE = "events:approval"
_MAX_REMEMBERED = 1024

_STATUS_LABELS = {
    "done": "完成",
    "failed": "失败",
    "stopped": "已停止",
    "refused": "已拒绝",
    "awaiting_approval": "等待审批",
    "awaiting_merge": "待合并",
}
_ACTIONS = {
    "done": "无需操作，查看结果即可",
    "failed": "查看失败原因，修正后重新提交任务",
    "stopped": "查看停止原因，需要时重新提交任务",
    "refused": "查看拒绝原因（权限或工作区规则），调整后重新提交任务",
    "awaiting_approval": "批准或拒绝审批请求 {request_id}，命令见下方「审批」",
    "awaiting_merge": "查看 PR {pr_url}，未合并原因：{merge_reason}；处理后手动合并或重新提交任务",
}
_REVIEW_PR_ACTION = "审阅并合并 PR {pr_url}"
_HOLD_LABELS = {"review": "评审不通过", "ci": "CI 失败", "other": "其他"}
_SOURCE_LABELS = (
    ("github:", "GitHub"),
    ("gdrive:", "Drive"),
    ("mcp", "MCP"),
    ("feishu:", "飞书"),
    ("switchboard:", "Switchboard"),
    ("timer:", "定时器"),
    ("events:", "/events"),
    ("webhook", "/events"),
)


@dataclass(frozen=True, slots=True)
class OutboxContext:
    inbox_id: str
    title: str
    source: str
    correlation_key: str
    session_id: str
    executor_id: str
    # A run status key from _STATUS_LABELS, or "" for a message no Worker run produced.
    status: str
    approval_request_id: str
    # Set only while the delivered PR is open; merged and local deliveries show as before.
    open_pr_url: str = ""
    merge_reason: str = ""

    @property
    def status_label(self) -> str:
        return _STATUS_LABELS.get(self.status, NONE)

    @property
    def action(self) -> str:
        if self.status == "done" and self.open_pr_url:
            return _REVIEW_PR_ACTION.format(pr_url=self.open_pr_url)
        template = _ACTIONS.get(self.status)
        if not template:
            return NONE
        return template.format(request_id=self.approval_request_id or NONE, pr_url=self.open_pr_url or NONE,
                               merge_reason=self.merge_reason or "未记录")


def outbox_context(inbox_id: str, metadata: Mapping[str, Any]) -> OutboxContext:
    # The one place that reads a message's context; the file header and the console summary both render from it.
    raw_context = metadata.get(TASK_CONTEXT_KEY)
    context: Mapping[str, Any] = cast(Mapping[str, Any], raw_context) if isinstance(raw_context, Mapping) else {}
    approval = metadata.get("kind") == "approval_request"
    request_id = _text(metadata.get("approval_request_id")) or (_text(metadata.get("request_id")) if approval else "")
    status = _text(context.get("status")) or ("awaiting_approval" if approval else "")
    correlation_key = _text(metadata.get("correlation_key"))
    open_pr_url, merge_reason = "", ""
    raw_delivery = context.get("delivery")
    if status == "done" and isinstance(raw_delivery, Mapping):
        delivery = cast(Mapping[str, Any], raw_delivery)
        if delivery.get("state") == "pr_open":
            open_pr_url = _text(delivery.get("pr_url"))
            if delivery.get("auto_merge") is True:
                # Merging was authorized and did not happen: the PR waits for a person.
                status, merge_reason = "awaiting_merge", _merge_reason(delivery)
    return OutboxContext(
        inbox_id=_text(inbox_id),
        title=_text(context.get("title")) or correlation_key,
        source=_text(context.get("source")),
        correlation_key=correlation_key,
        session_id=_text(metadata.get("session_id")),
        executor_id=_text(context.get("executor_id")),
        status=status if status in _STATUS_LABELS else "",
        approval_request_id=request_id,
        open_pr_url=open_pr_url,
        merge_reason=merge_reason,
    )


def _merge_reason(delivery: Mapping[str, Any]) -> str:
    label = _HOLD_LABELS.get(_text(delivery.get("hold")), "")
    detail = _text(delivery.get("hold_detail"))
    if label and detail:
        return f"{label}（{detail}）"
    return label or detail


def source_label(source: str) -> str:
    if not source:
        return NONE
    for prefix, label in _SOURCE_LABELS:
        if source.startswith(prefix):
            return f"{label}（{source}）"
    return source


def local_events_url(host: str, port: int) -> str:
    # A wildcard bind is reachable on loopback; the command is meant to run on the daemon's machine.
    if host in ("", "0.0.0.0"):
        host = "127.0.0.1"
    elif host == "::":
        host = "::1"
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"http://{host}:{port}/events"


def approval_event(ctx: OutboxContext, *, approved: bool) -> dict[str, Any]:
    decision = "approve" if approved else "reject"
    return {
        "inbox_id": ctx.inbox_id,
        "source": APPROVAL_EVENT_SOURCE,
        "external_id": f"approval:{ctx.approval_request_id}:{decision}",
        "payload": {"approval_request_id": ctx.approval_request_id, "approved": approved},
    }


def approval_curl(ctx: OutboxContext, events_url: str, *, approved: bool) -> str:
    body = json.dumps(approval_event(ctx, approved=approved), ensure_ascii=False)
    return (
        f"curl -sS -X POST {shlex.quote(events_url)} -H 'Content-Type: application/json' "
        f"-d {shlex.quote(body)}"
    )


def format_stamp(stamp: datetime) -> str:
    return stamp.strftime("%Y-%m-%d %H:%M:%S.") + f"{stamp.microsecond // 1000:03d}"


def render_header(ctx: OutboxContext, *, stamp: datetime, events_url: str) -> str:
    lines = [
        f"# {_value(ctx.title)}",
        "",
        f"- 任务：{_value(ctx.title)}",
        f"- 来源：{source_label(ctx.source)}",
        f"- inbox_id：{_value(ctx.inbox_id)}",
        f"- correlation_key：{_value(ctx.correlation_key)}",
        f"- 会话 ID：{_value(ctx.session_id)}",
        f"- 执行器：{_value(ctx.executor_id)}",
        f"- 状态：{ctx.status_label}",
        f"- 需要人做什么：{ctx.action}",
        f"- 时间：{format_stamp(stamp)}",
        "",
    ]
    if ctx.status == "awaiting_approval" and ctx.approval_request_id:
        mcp_args = {"inbox_id": ctx.inbox_id, "approval_request_id": ctx.approval_request_id, "approved": True}
        lines += [
            "## 审批",
            "",
            f"审批请求 ID：{ctx.approval_request_id}",
            "",
            "批准：",
            "",
            "```bash",
            approval_curl(ctx, events_url, approved=True),
            "```",
            "",
            "拒绝：",
            "",
            "```bash",
            approval_curl(ctx, events_url, approved=False),
            "```",
            "",
            '要附说明，在 payload 里加 "comment": "<说明>"。',
            "",
            "或用 MCP 工具 hibernaut_resolve_approval（需 [channels] mcp_enabled = true），参数：",
            "",
            "```json",
            json.dumps(mcp_args, ensure_ascii=False),
            "```",
            "",
            '拒绝时 "approved" 为 false；可选 "comment"。',
            "",
        ]
    lines += ["## 正文", "", ""]
    return "\n".join(lines)


def render_summary(ctx: OutboxContext, *, stamp: datetime, path: Path, base: Path | None = None) -> str:
    return f"[OUTBOX] {format_stamp(stamp)} {ctx.status_label} | {_value(ctx.title)} | {display_path(path, base)}"


def display_path(path: Path, base: Path | None) -> str:
    # Console only: relative to base (the config file's directory), else ~/... under home, else absolute.
    # Without a base the path stays absolute, so callers that never had a config file see no change.
    if base is None:
        return str(path)
    try:
        candidates = [(Path(os.path.normpath(path.absolute())), Path(os.path.normpath(base.absolute())))]
        candidates.append((path.resolve(), base.resolve()))
        for target, root in candidates:
            if target.is_relative_to(root):
                return str(target.relative_to(root))
        home = Path.home()
        for target, root in ((candidates[0][0], home), (candidates[1][0], home.resolve())):
            if target.is_relative_to(root):
                return str(Path("~") / target.relative_to(root))
    except (OSError, RuntimeError):
        pass
    return str(path)


def file_stem(stamp: datetime) -> str:
    # Local time to the millisecond, so sorting names sorts messages by time.
    return stamp.strftime("%Y%m%d-%H%M%S.") + f"{stamp.microsecond // 1000:03d}"


class FileOutboxChannel:
    # Writes each delivered message to outbox_dir as a Markdown file, then hands it to the wrapped channel unchanged.
    def __init__(
        self,
        directory: str | Path,
        inner: ChannelPort,
        *,
        events_url: str,
        clock: Callable[[], datetime] = datetime.now,
        announce: Callable[[str], None] | None = None,
        display_base: str | Path | None = None,
    ) -> None:
        self._dir = Path(directory)
        self._display_base = Path(display_base) if display_base is not None else None
        self._inner = inner
        self._events_url = events_url
        self._clock = clock
        self._announce = announce if announce is not None else _print_line
        self._lock = threading.Lock()
        self._last_ms: datetime | None = None
        # A retry after the inner channel failed reuses the file already written for that message.
        self._written: dict[str, Path] = {}

    @property
    def directory(self) -> Path:
        return self._dir

    async def send(self, inbox_id: str, content: str, metadata: dict[str, Any]) -> None:
        key = _message_key(inbox_id, content, metadata)
        if key not in self._written:
            ctx = outbox_context(inbox_id, metadata)
            try:
                stamp, path = await asyncio.to_thread(self._write, ctx, content)
            except OSError:
                # Raising leaves the Outbox row pending, so the dispatcher retries it like any channel failure.
                _log.exception("file outbox: cannot write a message for inbox %s to %s", inbox_id, self._dir)
                raise
            if len(self._written) >= _MAX_REMEMBERED:
                # Rows that exhausted their attempts never come back; forget the oldest.
                self._written.pop(next(iter(self._written)))
            self._written[key] = path
            self._announce(render_summary(ctx, stamp=stamp, path=path, base=self._display_base))
        await self._inner.send(inbox_id, content, metadata)
        self._written.pop(key, None)

    def _write(self, ctx: OutboxContext, content: str) -> tuple[datetime, Path]:
        with self._lock:
            self._dir.mkdir(parents=True, exist_ok=True)
            stamp = self._next_stamp()
            text = render_header(ctx, stamp=stamp, events_url=self._events_url) + content
            if not text.endswith("\n"):
                text += "\n"
            tmp = self._dir / f".{uuid4().hex}.tmp"
            try:
                with open(tmp, "x", encoding="utf-8") as stream:
                    stream.write(text)
                    stream.flush()
                    os.fsync(stream.fileno())
                return stamp, self._publish(tmp, file_stem(stamp))
            finally:
                tmp.unlink(missing_ok=True)

    def _publish(self, tmp: Path, stem: str) -> Path:
        # link() is an atomic rename that refuses an existing name, so a reader never sees a partial file and
        # nothing already in the directory is overwritten.
        n = 1
        while True:
            target = self._dir / (f"{stem}.md" if n == 1 else f"{stem}-{n}.md")
            try:
                os.link(tmp, target)
            except FileExistsError:
                n += 1
                continue
            return target

    def _next_stamp(self) -> datetime:
        # Waits up to a few ms for the clock to pass the last name this writer used, so two messages in one
        # millisecond still sort in order; a clash with files from elsewhere gets a -2, -3 suffix instead.
        stamp = self._clock()
        deadline = time.monotonic() + 0.005
        while self._last_ms is not None and _ms(stamp) <= self._last_ms and time.monotonic() < deadline:
            time.sleep(0.0005)
            stamp = self._clock()
        self._last_ms = _ms(stamp)
        return stamp


def _ms(stamp: datetime) -> datetime:
    return stamp.replace(microsecond=stamp.microsecond // 1000 * 1000)


def _message_key(inbox_id: str, content: str, metadata: Mapping[str, Any]) -> str:
    raw = json.dumps([inbox_id, content, dict(metadata)], sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _text(raw: object) -> str:
    return " ".join(raw.split()) if isinstance(raw, str) else ""


def _value(text: str) -> str:
    return text or NONE


def _print_line(line: str) -> None:
    print(line, flush=True)
