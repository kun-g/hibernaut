from __future__ import annotations

import asyncio
import hmac
from dataclasses import dataclass
from typing import Any, Mapping, cast

from runner.protocol import (
    InterruptRequest,
    InterruptResult,
    ReclaimRequest,
    ReclaimResult,
    RunRequest,
    RunResult,
    RunnerCapabilities,
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


class RunnerConnectionLostError(ConnectionError):
    pass


@dataclass(slots=True)
class _RunnerConn:
    runner_id: str
    conn_epoch: int
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    send_lock: asyncio.Lock


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
    ) -> None:
        if not shared_token:
            raise ValueError("shared_token must be non-empty")
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
        self._hb_task: asyncio.Task[None] | None = None
        self._rpc_seq = 0

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
        for conn in list(self._conns.values()):
            await send_close(conn.writer, CLOSE_NORMAL, "hub shutdown", mask=False)
        self._conns.clear()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def capabilities(self) -> RunnerCapabilities:
        online = self._registry.list_online()
        if not online:
            raise NoEligibleRunnerError("no online runners connected")
        return online[0].capabilities

    async def run(self, req: RunRequest, *, caller_id: str = "hibernaut") -> RunResult:
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
                ack = await asyncio.wait_for(ack_fut, timeout=self._ack_timeout)
            except BaseException:
                self._pending_ack.pop(req.run_id, None)
                self._forget_run(req.run_id)
                self._registry.release_slot(runner_id)
                raise

            if not ack.get("accepted"):
                self._forget_run(req.run_id)
                self._registry.release_slot(runner_id)
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

            try:
                return await res_fut
            finally:
                self._forget_run(req.run_id)
                self._registry.release_slot(runner_id)

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
            err = RunnerConnectionLostError(f"runner {conn.runner_id!r} {reason}; run {rid} was not completed")
            for pending in (self._pending_ack.pop(rid, None), self._pending_result.get(rid)):
                if pending is not None and not pending.done():
                    pending.set_exception(err)

    async def query_run(self, runner_id: str, run_id: str) -> RunResult | None:
        conn = self._conns.get(runner_id)
        if conn is None:
            return None
        resp = await self._rpc(conn, "run.query", {"run_id": run_id})
        res_raw = resp.get("result")
        return RunResult.from_dict(cast(Mapping[str, Any], res_raw)) if isinstance(res_raw, Mapping) else None

    async def interrupt(self, req: InterruptRequest) -> InterruptResult:
        runner_id = self._run_to_runner.get(req.run_id)
        if runner_id is None:
            return InterruptResult(run_id=req.run_id, accepted=False, state="not_found")
        conn = self._conns.get(runner_id)
        if conn is None:
            return InterruptResult(run_id=req.run_id, accepted=False, state="not_found")
        resp = await self._rpc(conn, "run.interrupt", {"request": req.to_dict()})
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
            if not hmac.compare_digest(token, self._token):
                await send_close(writer, CLOSE_AUTH_FAILED, "invalid token", mask=False)
                return
            caps = RunnerCapabilities.from_dict(cast(Mapping[str, Any], hello["capabilities"]))
            runner_id = caps.runner_id
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
        except (WSConnectionClosed, WSProtocolError, asyncio.TimeoutError, KeyError, ValueError, OSError):
            pass
        finally:
            if conn is not None:
                self._fail_runs_of(conn, "disconnected")
                if self._conns.get(conn.runner_id) is conn:
                    self._conns.pop(conn.runner_id, None)
                    self._registry.mark_offline(conn.runner_id, conn.conn_epoch)
            try:
                writer.close()
            except OSError:
                pass

    def _dispatch_inbound(self, runner_id: str, epoch: int, frame: dict[str, Any]) -> None:
        ftype = frame.get("type")
        if ftype == "pong":
            caps_raw = frame.get("capabilities")
            if isinstance(caps_raw, Mapping):
                self._registry.calibrate_heartbeat(
                    runner_id, epoch, RunnerCapabilities.from_dict(cast(Mapping[str, Any], caps_raw))
                )
        elif ftype == "run.accepted":
            rid = str(frame.get("run_id", ""))
            fut = self._pending_ack.pop(rid, None)
            if fut is not None and not fut.done():
                fut.set_result(frame)
        elif ftype == "run.completed":
            res = RunResult.from_dict(cast(Mapping[str, Any], frame["result"]))
            fut = self._pending_result.get(res.run_id)
            if fut is not None and not fut.done():
                fut.set_result(res)
        elif ftype == "rpc.reply":
            rpc_id = str(frame.get("rpc_id", ""))
            fut = self._pending_rpc.get(rpc_id)
            if fut is not None and not fut.done():
                fut.set_result(frame)

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_seconds)
            self._registry.sweep_stale(stale_after_seconds=self._heartbeat_seconds * 2.0)
            for conn in list(self._conns.values()):
                try:
                    await self._send(
                        conn,
                        {"type": "ping", "conn_epoch": conn.conn_epoch, "ts": utc_now_iso()},
                    )
                except OSError:
                    pass
