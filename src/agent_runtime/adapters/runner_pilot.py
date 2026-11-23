# Hibernaut side of the read-only Runner pilot (docs/runner.md §9): the loopback Relay one standalone Runner connects
# to, and the executor that sends it nothing but a protocol-file check of one commit.
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

from agent_runtime.adapters.runner_bridge import RunnerExecutorBridge, lost_run_telemetry, run_telemetry
from agent_runtime.core.models import Emit, Event, RunOutcome, RunTelemetry, SessionRef
from agent_runtime.core.ports import RunCapabilities
from runner.protocol import (
    InterruptRequest,
    InterruptResult,
    ReclaimRequest,
    ReclaimResult,
    RunnerCapabilities,
    RunnerPort,
    RunRequest,
    RunResult,
    ToolAuthorizer,
)
from runner.protocol_check import (
    CHECK_SCHEMA,
    MAX_HOLD_SECONDS,
    PROTOCOL_CHECK_EXECUTOR_ID,
    PROTOCOL_FILES,
    CheckRequest,
    is_full_commit,
    manifest_sha256,
    manifest_text,
)
from runner.registry import NoEligibleRunnerError, WorkspaceAffinityStore
from runner.transport.auth import read_relay_token
from runner.transport.relay_hub import RunnerConnectionLostError, RunnerRelayHub

RELAY_HOST = "127.0.0.1"
LAUNCH_PERMISSION = "runner.protocol_check.launch"
_HEARTBEAT_SECONDS = 5.0
# How long a Run waits for the node to be connected and free. Longer than a heartbeat: the node counts a run that just
# ended as active until its next heartbeat.
_NODE_WAIT_SECONDS = 30.0


class RunnerPilotRelay:
    # RunnerPort on the pilot's own Relay. It admits exactly one node: the configured id, one slot, the fixed check
    # program as its only executor, the hb namespace and the one project.
    def __init__(self, *, runner_id: str, project_key: str, port: int, token_file: Path) -> None:
        self.runner_id = runner_id
        self.project_key = project_key
        self._port = port
        self._token = read_relay_token(token_file)
        self._hub: RunnerRelayHub | None = None
        self._affinity: WorkspaceAffinityStore | None = None

    @property
    def listening(self) -> bool:
        return self._hub is not None

    async def start(self) -> None:
        # Read-only runs bind no workspace to a node, so there is nothing to keep across restarts.
        affinity = WorkspaceAffinityStore(Path(":memory:"))
        hub = RunnerRelayHub(
            shared_token=self._token, affinity=affinity, heartbeat_seconds=_HEARTBEAT_SECONDS, admit=self._refusal
        )
        try:
            await hub.start(RELAY_HOST, self._port)
        except OSError:
            affinity.close()
            raise
        self._hub, self._affinity = hub, affinity

    async def stop(self) -> None:
        hub, affinity, self._hub, self._affinity = self._hub, self._affinity, None, None
        if hub is not None:
            await hub.stop()
        if affinity is not None:
            affinity.close()

    def _refusal(self, caps: RunnerCapabilities) -> str | None:
        for ok, reason in (
            (caps.runner_id == self.runner_id, "unknown runner_id"),
            (caps.slots.max_concurrent == 1, "the pilot node must have exactly one slot"),
            (tuple(caps.namespaces) == ("hb",), "the pilot node must serve the hb namespace only"),
            (tuple(caps.projects) == (self.project_key,), "the pilot node must serve the pilot project only"),
            (
                tuple(e.executor_id for e in caps.executors) == (PROTOCOL_CHECK_EXECUTOR_ID,),
                "the pilot node must offer the protocol check only",
            ),
        ):
            if not ok:
                return reason
        return None

    def _active_hub(self) -> RunnerRelayHub:
        if self._hub is None:
            raise NoEligibleRunnerError("the pilot relay is not listening")
        return self._hub

    async def capabilities(self) -> RunnerCapabilities:
        return await self._active_hub().capabilities()

    async def run(self, req: RunRequest, *, authorizer: ToolAuthorizer | None = None) -> RunResult:
        return await self._active_hub().run(req, authorizer=authorizer)

    async def interrupt(self, req: InterruptRequest) -> InterruptResult:
        return await self._active_hub().interrupt(req)

    async def reclaim_workspace(self, req: ReclaimRequest) -> ReclaimResult:
        return await self._active_hub().reclaim_workspace(req)


class _BadCheckTask(ValueError):
    pass


def _check_request(events: Sequence[Event]) -> CheckRequest:
    commits = {ev.payload["candidate_commit"] for ev in events if isinstance(ev.payload.get("candidate_commit"), str)}
    if len(commits) != 1:
        raise _BadCheckTask("任务须在 payload.candidate_commit 给出一个候选提交，同批事件不能指向不同提交")
    commit = commits.pop()
    if not is_full_commit(commit):
        raise _BadCheckTask("payload.candidate_commit 须是 40 位小写十六进制的完整提交号")
    # The latest event decides, so a drill's follow-up event can end the hold.
    hold = events[-1].payload.get("hold_seconds", 0)
    if isinstance(hold, bool) or not isinstance(hold, int) or not 0 <= hold <= MAX_HOLD_SECONDS:
        raise _BadCheckTask(f"payload.hold_seconds 须是 0 到 {MAX_HOLD_SECONDS} 的整数")
    return CheckRequest(commit=commit, hold_seconds=hold)


def _verified_digests(report: dict[str, Any] | None, commit: str) -> dict[str, str] | None:
    # The report is data from another process: it must be about the requested commit and exactly the five files, and
    # its total must be the one its file digests give.
    if report is None or report.get("schema") != CHECK_SCHEMA or report.get("commit") != commit:
        return None
    files: object = report.get("files")
    if not isinstance(files, list):
        return None
    digests: dict[str, str] = {}
    for item in cast(list[object], files):
        if not isinstance(item, dict):
            return None
        entry = cast(dict[str, object], item)
        path, digest = entry.get("path"), entry.get("sha256")
        if not isinstance(path, str) or not isinstance(digest, str) or path in digests:
            return None
        if len(digest) != 64 or set(digest) - set("0123456789abcdef"):
            return None
        digests[path] = digest
    if set(digests) != set(PROTOCOL_FILES) or report.get("manifest_sha256") != manifest_sha256(digests):
        return None
    return digests


class ProtocolCheckExecutor(RunnerExecutorBridge):
    # Tool-free by construction: the node starts one fixed program, given a commit id and a hold time. Every result
    # closes its session, so after the route is closed nothing keeps pointing at this executor.
    def __init__(
        self,
        *,
        runner: RunnerPort,
        inbox_id: str,
        project_key: str,
        timeout_seconds: float,
        node_wait_seconds: float = _NODE_WAIT_SECONDS,
    ) -> None:
        super().__init__(
            runner=runner,
            executor_id=PROTOCOL_CHECK_EXECUTOR_ID,
            project_key=project_key,
            timeout_seconds=timeout_seconds,
            executor_calls_tools=False,
            node_wait_seconds=node_wait_seconds,
        )
        self._inbox_id = inbox_id

    async def execute(self, events: Sequence[Event], session: SessionRef, caps: RunCapabilities) -> RunOutcome:
        if session.inbox_id != self._inbox_id:
            return _failed(session, f"Inbox {session.inbox_id} 不在试点路由内", "permission_denied")
        try:
            check = _check_request(events)
        except _BadCheckTask as exc:
            return _failed(session, str(exc), "config_error")
        decision = caps.permissions.check(
            LAUNCH_PERMISSION, {"inbox_id": session.inbox_id, "commit": check.commit, "hold_seconds": check.hold_seconds}
        )
        if decision.action != "allow":
            return _failed(session, f"权限检查未放行（{decision.action}）：{decision.reason}", "permission_denied")
        try:
            return await super().execute(events, session, caps)
        except RunnerConnectionLostError as exc:
            return _failed(session, f"Runner {exc.runner_id} 的连接中断，Run {exc.run_id} 未完成",
                           telemetry=lost_run_telemetry(exc))
        except RuntimeError as exc:  # no eligible node, or the node rejected the submission: nothing ran
            return _failed(session, f"未派发到 Runner：{exc}", "network")

    def _prepare_request(self, req: RunRequest, events: Sequence[Event], session: SessionRef) -> RunRequest:
        return replace(
            req,
            work_key=session.session_id,
            name_hint=None,
            executor_session_id=None,
            readonly=True,
            interactive=False,
            prompt=_check_request(events).to_json(),
        )

    def _translate(self, result: RunResult, session: SessionRef, events: Sequence[Event]) -> RunOutcome:
        telemetry = run_telemetry(result)
        if result.status == "interrupted":
            # Reached only when this Run did not ask for it (then the cancellation is raised instead).
            return _failed(session, "Runner 中断了这次校验", telemetry=telemetry)
        if result.status != "succeeded":
            return _failed(session, f"校验未通过（{result.status}）：{result.error_summary or '无说明'}", telemetry=telemetry)
        commit = _check_request(events).commit
        digests = _verified_digests(result.structured_output, commit)
        if digests is None:
            return _failed(session, "Runner 返回的结果与候选提交或五份协议文件不符",
                           telemetry=replace(telemetry, error_kind="bad_output"))
        files = " ".join(PROTOCOL_FILES)
        text = (
            f"Runner 协议文件校验通过\n\n"
            f"- 候选提交：{commit}\n"
            f"- 汇总摘要（SHA-256）：{manifest_sha256(digests)}\n"
            f"- 运行节点：{result.runner_id}，Runner run_id：{result.run_id}\n"
            f"- Runner 日志：{telemetry.runner_log or '无'}\n\n"
            f"```\n{manifest_text(digests)}```\n\n"
            "复算（在任一含该提交的克隆里运行，应得到上面的汇总摘要）：\n\n"
            f"```sh\nfor f in {files}; do printf '%s  %s\\n' \"$(git cat-file blob {commit}:$f | shasum -a 256 | "
            "cut -d' ' -f1)\" \"$f\"; done | shasum -a 256\n```"
        )
        return Emit(messages=(text,), session_id=session.session_id, close_session=True, telemetry=telemetry)


class ClosedRouteExecutor:
    # Takes the check executor's place wherever the pilot route is not open: events and sessions left over from the
    # pilot end with a notice, and the inbox is free for its original executor.
    executor_id = PROTOCOL_CHECK_EXECUTOR_ID

    async def execute(self, events: Sequence[Event], session: SessionRef, caps: RunCapabilities) -> RunOutcome:
        return _failed(
            session, "试点路由未对本 Inbox 开放，未执行。重新启用后可用 hibernaut_replay_event(force=true) 重放", "config_error"
        )


def _failed(
    session: SessionRef, reason: str, error_kind: str | None = None, *, telemetry: RunTelemetry | None = None
) -> Emit:
    return Emit(
        messages=(f"[{PROTOCOL_CHECK_EXECUTOR_ID}] {reason}",),
        session_id=session.session_id,
        status="failed",
        close_session=True,
        telemetry=telemetry or RunTelemetry(stderr=reason, error_kind=error_kind),
    )
