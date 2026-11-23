from __future__ import annotations

import asyncio
import hmac
import logging
from collections import OrderedDict
from dataclasses import dataclass, replace
from typing import Any, Callable, Mapping, cast

from runner.protocol import (
    InterruptRequest,
    InterruptResult,
    ReclaimRequest,
    ReclaimResult,
    RunIdentity,
    RunRequest,
    RunResult,
    RunnerCapabilities,
    ToolAuthorizer,
    ToolCall,
    ToolDecision,
    run_identity,
    utc_now_iso,
)
from runner.registry import (
    NoEligibleRunnerError,
    NodeRegistry,
    RunnerRouter,
    WorkspaceAffinityStore,
)
from runner.transport.ws_protocol import (
    CLOSE_AUTH_FAILED,
    CLOSE_NORMAL,
    CLOSE_SUPERSEDED,
    WSConnectionClosed,
    WSProtocolError,
    recv_json,
    send_close,
    send_json,
    server_handshake,
)


_log = logging.getLogger(__name__)


class RunnerConnectionLostError(ConnectionError):
    def __init__(self, runner_id: str, run_id: str, reason: str) -> None:
        super().__init__(f"runner {runner_id!r} {reason}; run {run_id} was not completed")
        self.runner_id = runner_id
        self.run_id = run_id


class RunIdConflictError(ValueError):
    pass


# How many finished runs the hub remembers for run_id retries (the idempotency window). A retry inside it gets the
# recorded result instead of a second execution on whichever node has a free slot now.
FINISHED_RUN_CACHE_SIZE = 4096


@dataclass(slots=True)
class _RunnerConn:
    runner_id: str
    conn_epoch: int
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    send_lock: asyncio.Lock


@dataclass(slots=True)
class _DispatchGate:
    lock: asyncio.Lock
    users: int = 0


class RunnerRelayHub:
    def __init__(
        self,
        *,
        shared_token: str,
        affinity: WorkspaceAffinityStore,
        registry: NodeRegistry | None = None,
        path: str = "/ws/runner",
        auth_timeout_seconds: float = 5.0,
        heartbeat_seconds: float = 20.0,
        ack_timeout_seconds: float = 10.0,
        admit: Callable[[RunnerCapabilities], str | None] | None = None,
    ) -> None:
        if not shared_token:
            raise ValueError("shared_token must be non-empty")
        # Says why an authenticated node may not serve here, or None; asked for the hello and every heartbeat report.
        self._admit = admit
        self._token = shared_token
        self._affinity = affinity
        self._registry = registry or NodeRegistry()
        self._router = RunnerRouter(self._registry, self._affinity)
        self._path = path
        self._auth_timeout = auth_timeout_seconds
        self._heartbeat_seconds = heartbeat_seconds
        self._ack_timeout = ack_timeout_seconds

        self._server: asyncio.AbstractServer | None = None
        self._conns: dict[str, _RunnerConn] = {}
        self._run_to_runner: dict[str, str] = {}
        # Connection epoch each in-flight run was submitted on; a run belongs to that connection only.
        self._run_epoch: dict[str, int] = {}
        self._pending_ack: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._pending_result: dict[str, asyncio.Future[RunResult]] = {}
        self._pending_rpc: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._inflight: dict[str, asyncio.Future[RunResult]] = {}
        self._inflight_identity: dict[str, RunIdentity] = {}
        self._authorizers: dict[str, ToolAuthorizer] = {}
        self._authorize_tasks: set[asyncio.Task[None]] = set()
        self._finished: OrderedDict[str, tuple[RunIdentity, RunResult]] = OrderedDict()
        # Each pending rpc belongs to the connection it was sent on (runner_id, conn_epoch).
        self._rpc_target: dict[str, tuple[str, int]] = {}
        self._dispatch_gates: dict[tuple[str, str, str, str], _DispatchGate] = {}
        self._hb_task: asyncio.Task[None] | None = None
        self._rpc_seq = 0
        self._stopping = False

    @property
    def registry(self) -> NodeRegistry:
        return self._registry

    @property
    def affinity(self) -> WorkspaceAffinityStore:
        return self._affinity

    async def start(self, host: str = "127.0.0.1", port: int = 0) -> tuple[str, int]:
        self._server = await asyncio.start_server(self._handle_client, host=host, port=port)
        self._hb_task = asyncio.create_task(self._heartbeat_loop())
        sock = self._server.sockets[0]
        addr = sock.getsockname()
        return str(addr[0]), int(addr[1])

    async def stop(self) -> None:
        if self._hb_task is not None:
            self._hb_task.cancel()
            try:
                await self._hb_task
            except asyncio.CancelledError:
                pass
        # No new connection from here on, one still in its handshake included: wait_closed() waits for every handler.
        self._stopping = True
        if self._server is not None:
            self._server.close()
        for task in list(self._authorize_tasks):
            task.cancel()
        for conn in list(self._conns.values()):
            await send_close(conn.writer, CLOSE_NORMAL, "hub shutdown", mask=False)
        self._conns.clear()
        if self._server is not None:
            await self._server.wait_closed()

    async def capabilities(self) -> RunnerCapabilities:
        online = self._registry.list_online()
        if not online:
            raise NoEligibleRunnerError("no online runners connected")
        return online[0].capabilities

    async def run(
        self, req: RunRequest, *, caller_id: str = "hibernaut", authorizer: ToolAuthorizer | None = None
    ) -> RunResult:
        # run_id is the idempotency key across waiters and retries of one request: every caller of the same run_id
        # shares one submission and one reserved slot, a caller that times out or is cancelled leaves the submission
        # running for the rest, and a finished run is answered from its recorded result. A run_id reused for another
        # request (caller, workspace or executor) is refused, the same rule the daemon applies.
        if authorizer is not None:
            req = replace(req, authorize_tools=True)
        elif req.authorize_tools:
            raise PermissionError(f"run {req.run_id} requires tool authorization, but no authorizer is attached")
        identity = run_identity(req, caller_id)
        finished = self._finished.get(req.run_id)
        if finished is not None:
            if finished[0] != identity:
                raise RunIdConflictError(f"run_id {req.run_id!r} belongs to another request")
            return finished[1]
        task = self._inflight.get(req.run_id)
        if task is not None:
            if self._inflight_identity.get(req.run_id) != identity:
                raise RunIdConflictError(f"run_id {req.run_id!r} belongs to another request")
        else:
            task = asyncio.ensure_future(self._submit_and_wait(req, caller_id))
            self._inflight[req.run_id] = task
            self._inflight_identity[req.run_id] = identity
            if authorizer is not None:
                self._authorizers[req.run_id] = authorizer
            task.add_done_callback(lambda t, rid=req.run_id, ident=identity: self._on_submission_done(rid, ident, t))
        return await asyncio.shield(task)

    def _on_submission_done(self, run_id: str, identity: RunIdentity, task: asyncio.Future[RunResult]) -> None:
        if self._inflight.get(run_id) is task:
            del self._inflight[run_id]
            self._inflight_identity.pop(run_id, None)
            self._authorizers.pop(run_id, None)
        if task.cancelled():
            return
        if task.exception() is not None:  # retrieved here so a submission nobody waits for any more does not warn
            return
        result = task.result()
        # A refusal ran nothing, so a retry may still run it; anything else ran (or was stopped) on its node.
        if result.status != "refused":
            self._finished[run_id] = (identity, result)
            self._finished.move_to_end(run_id)
            while len(self._finished) > FINISHED_RUN_CACHE_SIZE:
                self._finished.popitem(last=False)

    async def _submit_and_wait(self, req: RunRequest, caller_id: str) -> RunResult:
        gate = await self._enter_dispatch_gate(req)
        try:
            runner_id, res_fut = await self._dispatch(req, caller_id)
        finally:
            self._leave_dispatch_gate(req, gate)
        try:
            return await res_fut
        finally:
            self._forget_run(req.run_id)
            self._registry.release_slot(runner_id)

    async def _enter_dispatch_gate(self, req: RunRequest) -> _DispatchGate | None:
        # Writes to one workspace are dispatched one at a time, up to the node's acknowledgement: while a claim is
        # unconfirmed nobody else may lean on it, so a rejection proves that no request on that node uses the
        # workspace and the claim can be released.
        if req.readonly:
            return None
        key = (req.namespace, req.project_key, req.owner_scope, req.work_key)
        gate = self._dispatch_gates.get(key)
        if gate is None:
            gate = self._dispatch_gates[key] = _DispatchGate(lock=asyncio.Lock())
        gate.users += 1
        try:
            await gate.lock.acquire()
        except BaseException:
            self._drop_gate_user(key, gate)
            raise
        return gate

    def _leave_dispatch_gate(self, req: RunRequest, gate: _DispatchGate | None) -> None:
        if gate is None:
            return
        gate.lock.release()
        self._drop_gate_user((req.namespace, req.project_key, req.owner_scope, req.work_key), gate)

    def _drop_gate_user(self, key: tuple[str, str, str, str], gate: _DispatchGate) -> None:
        gate.users -= 1
        if gate.users == 0 and self._dispatch_gates.get(key) is gate:
            del self._dispatch_gates[key]

    async def _dispatch(self, req: RunRequest, caller_id: str) -> tuple[str, asyncio.Future[RunResult]]:
        excluded: set[str] = set()
        loop = asyncio.get_running_loop()
        while True:
            runner_id = self._router.select_node(req, exclude=excluded)
            if not self._registry.reserve_slot(runner_id):
                excluded.add(runner_id)
                continue
            conn = self._conns.get(runner_id)
            if conn is None:
                self._registry.release_slot(runner_id)
                excluded.add(runner_id)
                continue

            # Claimed in the same loop step that selected the node, before the first await: a concurrent submit for
            # this workspace sees the binding and goes to the same node instead of building a second copy.
            claimed = not req.readonly and self._claim_workspace(req, runner_id)

            ack_fut: asyncio.Future[dict[str, Any]] = loop.create_future()
            res_fut: asyncio.Future[RunResult] = loop.create_future()
            self._pending_ack[req.run_id] = ack_fut
            self._pending_result[req.run_id] = res_fut
            self._run_to_runner[req.run_id] = runner_id
            self._run_epoch[req.run_id] = conn.conn_epoch

            try:
                await self._send(
                    conn,
                    {
                        "type": "run.submit",
                        "conn_epoch": conn.conn_epoch,
                        "caller_id": caller_id,
                        "request": req.to_dict(),
                        "ts": utc_now_iso(),
                    },
                )
                # No ack deadline: once sent, the runner may already be executing, so giving up here would leave a
                # run nobody waits for. A lost connection fails this future (_fail_runs_of).
                ack = await ack_fut
            except BaseException:
                self._pending_ack.pop(req.run_id, None)
                self._forget_run(req.run_id)
                self._registry.release_slot(runner_id)
                if res_fut.done():
                    res_fut.exception()  # a lost connection failed it together with ack_fut; nobody else awaits it
                raise

            if not ack.get("accepted"):
                self._forget_run(req.run_id)
                self._registry.release_slot(runner_id)
                if claimed:
                    # The node said no, so it never ran this: the workspace is free for any node again. Without such
                    # an answer (a lost connection) the claim stays until the node's hello reconciles it.
                    self._affinity.unbind(
                        namespace=req.namespace,
                        project_key=req.project_key,
                        owner_scope=req.owner_scope,
                        work_key=req.work_key,
                    )
                if ack.get("reason") == "slots_full":
                    excluded.add(runner_id)
                    continue
                raise RuntimeError(f"runner {runner_id!r} rejected run: {ack.get('reason')}")

            if not req.readonly:
                self._affinity.bind(
                    namespace=req.namespace,
                    project_key=req.project_key,
                    owner_scope=req.owner_scope,
                    work_key=req.work_key,
                    runner_id=runner_id,
                )

            return runner_id, res_fut

    def _claim_workspace(self, req: RunRequest, runner_id: str) -> bool:
        if self._affinity.lookup(
            namespace=req.namespace,
            project_key=req.project_key,
            owner_scope=req.owner_scope,
            work_key=req.work_key,
        ) is not None:
            return False
        self._affinity.bind(
            namespace=req.namespace,
            project_key=req.project_key,
            owner_scope=req.owner_scope,
            work_key=req.work_key,
            runner_id=runner_id,
        )
        return True

    def _drop_connection(self, conn: _RunnerConn, reason: str) -> None:
        self._fail_runs_of(conn, reason)
        if self._conns.get(conn.runner_id) is conn:
            self._conns.pop(conn.runner_id, None)
            self._registry.mark_offline(conn.runner_id, conn.conn_epoch)
        try:
            conn.writer.close()
        except OSError:
            pass

    def _forget_run(self, run_id: str) -> None:
        self._pending_result.pop(run_id, None)
        self._run_to_runner.pop(run_id, None)
        self._run_epoch.pop(run_id, None)

    def _fail_runs_of(self, conn: _RunnerConn, reason: str) -> None:
        # The runner side cancels (and reaps) runs whose connection is gone, and a new connection does not adopt
        # them, so their callers get a terminal error and may retry under a new run_id.
        for rid, rnode in list(self._run_to_runner.items()):
            if rnode != conn.runner_id or self._run_epoch.get(rid) != conn.conn_epoch:
                continue
            err = RunnerConnectionLostError(conn.runner_id, rid, reason)
            for pending in (self._pending_ack.pop(rid, None), self._pending_result.get(rid)):
                if pending is not None and not pending.done():
                    pending.set_exception(err)

    async def query_run(self, runner_id: str, run_id: str, *, caller_id: str = "hibernaut") -> RunResult | None:
        conn = self._conns.get(runner_id)
        if conn is None:
            return None
        resp = await self._rpc(conn, "run.query", {"caller_id": caller_id, "run_id": run_id})
        res_raw = resp.get("result")
        return RunResult.from_dict(cast(Mapping[str, Any], res_raw)) if isinstance(res_raw, Mapping) else None

    async def interrupt(self, req: InterruptRequest, *, caller_id: str = "hibernaut") -> InterruptResult:
        # Only the caller that submitted a run may stop it; the node checks the same against its own record.
        identity = self._inflight_identity.get(req.run_id)
        runner_id = self._run_to_runner.get(req.run_id)
        if runner_id is None or identity is None or identity[0] != caller_id:
            return InterruptResult(run_id=req.run_id, accepted=False, state="not_found")
        conn = self._conns.get(runner_id)
        if conn is None:
            return InterruptResult(run_id=req.run_id, accepted=False, state="not_found")
        resp = await self._rpc(conn, "run.interrupt", {"caller_id": caller_id, "request": req.to_dict()})
        return InterruptResult.from_dict(cast(Mapping[str, Any], resp["result"]))

    async def reclaim_workspace(self, req: ReclaimRequest, *, caller_id: str = "hibernaut") -> ReclaimResult:
        runner_id = self._affinity.lookup(
            namespace=req.namespace,
            project_key=req.project_key,
            owner_scope=req.owner_scope,
            work_key=req.work_key,
        )
        if runner_id is None:
            return ReclaimResult(
                reclaimed=False,
                status="not_found",
                reason="no affinity binding found for work_key",
                blockers=(),
            )
        conn = self._conns.get(runner_id)
        if conn is None:
            return ReclaimResult(
                reclaimed=False,
                status="blocked",
                reason=f"affinity runner {runner_id!r} is offline",
                blockers=("runner_offline",),
            )
        resp = await self._rpc(
            conn,
            "reclaim.request",
            {"caller_id": caller_id, "request": req.to_dict()},
        )
        result = ReclaimResult.from_dict(cast(Mapping[str, Any], resp["result"]))
        if result.reclaimed:
            self._affinity.unbind(
                namespace=req.namespace,
                project_key=req.project_key,
                owner_scope=req.owner_scope,
                work_key=req.work_key,
            )
        return result

    async def _rpc(self, conn: _RunnerConn, frame_type: str, body: dict[str, Any]) -> dict[str, Any]:
        self._rpc_seq += 1
        rpc_id = f"rpc_{self._rpc_seq}"
        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending_rpc[rpc_id] = fut
        self._rpc_target[rpc_id] = (conn.runner_id, conn.conn_epoch)
        try:
            payload = dict(body)
            payload["type"] = frame_type
            payload["rpc_id"] = rpc_id
            payload["conn_epoch"] = conn.conn_epoch
            payload["ts"] = utc_now_iso()
            await self._send(conn, payload)
            return await asyncio.wait_for(fut, timeout=self._ack_timeout)
        finally:
            self._pending_rpc.pop(rpc_id, None)
            self._rpc_target.pop(rpc_id, None)

    async def _send(self, conn: _RunnerConn, payload: Mapping[str, Any]) -> None:
        async with conn.send_lock:
            await send_json(conn.writer, payload, mask=False)

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        conn: _RunnerConn | None = None
        try:
            await asyncio.wait_for(server_handshake(reader, writer, expected_path=self._path), timeout=self._auth_timeout)
            hello = await asyncio.wait_for(
                recv_json(reader, writer, expect_masked=True, send_masked=False),
                timeout=self._auth_timeout,
            )
            if hello.get("type") != "hello":
                await send_close(writer, CLOSE_AUTH_FAILED, "expected hello", mask=False)
                return
            token = str(hello.get("token", ""))
            # As bytes: compare_digest raises on a str that is not ASCII.
            if not hmac.compare_digest(token.encode("utf-8", "surrogatepass"), self._token.encode("utf-8")):
                _log.warning("relay refused a connection: invalid token")
                await send_close(writer, CLOSE_AUTH_FAILED, "invalid token", mask=False)
                return
            caps = RunnerCapabilities.from_dict(cast(Mapping[str, Any], hello["capabilities"]))
            runner_id = caps.runner_id
            if self._stopping:
                await send_close(writer, CLOSE_NORMAL, "hub shutdown", mask=False)
                return
            refusal = self._admit(caps) if self._admit is not None else None
            if refusal is not None:
                _log.warning("relay refused runner %r: %s", runner_id, refusal)
                await send_close(writer, CLOSE_AUTH_FAILED, refusal, mask=False)
                return
            epoch = self._registry.next_epoch(runner_id)

            old = self._conns.get(runner_id)
            if old is not None:
                self._fail_runs_of(old, "connection superseded")
                await send_close(old.writer, CLOSE_SUPERSEDED, "superseded by new connection", mask=False)

            conn = _RunnerConn(
                runner_id=runner_id,
                conn_epoch=epoch,
                reader=reader,
                writer=writer,
                send_lock=asyncio.Lock(),
            )
            self._conns[runner_id] = conn
            self._registry.register(runner_id, epoch, caps)
            ready_keys = hello.get("ready_work_keys")
            if isinstance(ready_keys, list):
                self._affinity.reconcile_runner(runner_id, cast(list[Mapping[str, str]], ready_keys))

            await self._send(
                conn,
                {
                    "type": "welcome",
                    "runner_id": runner_id,
                    "conn_epoch": epoch,
                    "heartbeat_sec": self._heartbeat_seconds,
                    "ts": utc_now_iso(),
                },
            )

            while True:
                frame = await recv_json(reader, writer, expect_masked=True, send_masked=False)
                if frame.get("conn_epoch") != epoch:
                    continue
                self._dispatch_inbound(runner_id, epoch, frame)
        except (
            WSConnectionClosed, WSProtocolError, asyncio.TimeoutError, KeyError, ValueError, OSError,
            # A peer that hangs up mid-frame or sends an endless header.
            EOFError, asyncio.LimitOverrunError,
        ):
            pass
        finally:
            if conn is not None:
                self._drop_connection(conn, "disconnected")
            else:
                try:
                    writer.close()
                except OSError:
                    pass

    def _dispatch_inbound(self, runner_id: str, epoch: int, frame: dict[str, Any]) -> None:
        ftype = frame.get("type")
        if ftype == "pong":
            caps_raw = frame.get("capabilities")
            if isinstance(caps_raw, Mapping):
                caps = RunnerCapabilities.from_dict(cast(Mapping[str, Any], caps_raw))
                refusal = self._admit(caps) if self._admit is not None else None
                if caps.runner_id != runner_id or refusal is not None:
                    # Ends the connection, like any other frame that breaks the protocol.
                    raise ValueError(refusal or "heartbeat reports another runner_id")
                self._registry.calibrate_heartbeat(runner_id, epoch, caps)
        # Replies only count on the connection the request went out on: another node, or an older connection of the
        # same node, may answer late with a matching id, and must neither acknowledge, finish nor answer it.
        elif ftype == "run.accepted":
            rid = str(frame.get("run_id", ""))
            if not self._sent_on(rid, runner_id, epoch):
                return
            fut = self._pending_ack.pop(rid, None)
            if fut is not None and not fut.done():
                fut.set_result(frame)
        elif ftype == "run.completed":
            res = RunResult.from_dict(cast(Mapping[str, Any], frame["result"]))
            if not self._sent_on(res.run_id, runner_id, epoch):
                return
            fut = self._pending_result.get(res.run_id)
            if fut is not None and not fut.done():
                fut.set_result(res)
        elif ftype == "tool.authorize":
            call = ToolCall.from_dict(cast(Mapping[str, Any], frame["call"]))
            conn = self._conns.get(runner_id)
            if conn is None or conn.conn_epoch != epoch or not self._sent_on(call.run_id, runner_id, epoch):
                return
            task = asyncio.create_task(self._answer_tool_call(conn, call))
            self._authorize_tasks.add(task)
            task.add_done_callback(self._authorize_tasks.discard)
        elif ftype == "rpc.reply":
            rpc_id = str(frame.get("rpc_id", ""))
            if self._rpc_target.get(rpc_id) != (runner_id, epoch):
                return
            fut = self._pending_rpc.get(rpc_id)
            if fut is not None and not fut.done():
                fut.set_result(frame)

    async def _answer_tool_call(self, conn: _RunnerConn, call: ToolCall) -> None:
        authorizer = self._authorizers.get(call.run_id)
        if authorizer is None:
            decision = ToolDecision(action="block", reason="no tool authorizer is attached to this run")
        else:
            try:
                decision = await authorizer(call)
            except Exception as exc:  # noqa: BLE001
                decision = ToolDecision(action="block", reason=f"tool authorization failed: {exc}")
        try:
            await self._send(
                conn,
                {
                    "type": "tool.decision",
                    "conn_epoch": conn.conn_epoch,
                    "run_id": call.run_id,
                    "call_id": call.call_id,
                    "decision": decision.to_dict(),
                    "ts": utc_now_iso(),
                },
            )
        except OSError:
            pass

    def _sent_on(self, run_id: str, runner_id: str, epoch: int) -> bool:
        return self._run_to_runner.get(run_id) == runner_id and self._run_epoch.get(run_id) == epoch

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_seconds)
            for runner_id in self._registry.sweep_stale(stale_after_seconds=self._heartbeat_seconds * 2.0):
                # A node that stopped answering but keeps its socket open would otherwise hold its runs' waiters
                # forever; closing it ends them the same way a disconnect does.
                stale = self._conns.get(runner_id)
                if stale is not None:
                    self._drop_connection(stale, "stopped answering heartbeats")
            for conn in list(self._conns.values()):
                try:
                    await self._send(
                        conn,
                        {"type": "ping", "conn_epoch": conn.conn_epoch, "ts": utc_now_iso()},
                    )
                except OSError:
                    pass
