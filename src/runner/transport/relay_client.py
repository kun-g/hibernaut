from __future__ import annotations

import asyncio
import logging
import urllib.parse
from typing import Any, Mapping, cast

from runner.daemon import RunnerDaemon
from runner.protocol import (
    InterruptRequest,
    ReclaimRequest,
    RunRequest,
    ToolCall,
    ToolDecision,
    utc_now_iso,
)
from runner.transport.ws_protocol import (
    CLOSE_NORMAL,
    CLOSE_SUPERSEDED,
    WSConnectionClosed,
    WSProtocolError,
    client_handshake,
    recv_json,
    send_close,
    send_json,
)

_log = logging.getLogger(__name__)


class RunnerRelayAgent:
    def __init__(
        self,
        *,
        daemon: RunnerDaemon,
        hub_url: str,
        token: str,
        reconnect_delays: tuple[float, ...] = (0.2, 0.5, 1.0, 2.0, 5.0),
    ) -> None:
        self._daemon = daemon
        self._hub_url = hub_url
        self._token = token
        self._reconnect_delays = reconnect_delays
        self._stop_event = asyncio.Event()
        self._connected_event = asyncio.Event()
        self._superseded_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._conn_epoch = 0

    @property
    def conn_epoch(self) -> int:
        return self._conn_epoch

    async def start(self) -> None:
        self._stop_event.clear()
        self._connected_event.clear()
        self._superseded_event.clear()
        self._task = asyncio.create_task(self._run_loop())

    async def wait_connected(self, timeout: float = 5.0) -> None:
        await asyncio.wait_for(self._connected_event.wait(), timeout=timeout)

    async def wait_superseded(self, timeout: float = 5.0) -> None:
        await asyncio.wait_for(self._superseded_event.wait(), timeout=timeout)

    async def wait_finished(self) -> None:
        # Returns when the agent gave up by itself (superseded); stop() is the normal end.
        if self._task is not None:
            await asyncio.wait({self._task})

    async def stop(self) -> None:
        self._stop_event.set()
        if self._writer is not None:
            await send_close(self._writer, CLOSE_NORMAL, "agent stop", mask=True)
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run_loop(self) -> None:
        attempt = 0
        while not self._stop_event.is_set():
            epoch = self._conn_epoch
            reason = "connection ended"
            try:
                await self._session_once()
            except WSConnectionClosed as exc:
                if exc.code == CLOSE_SUPERSEDED:
                    self._superseded_event.set()
                    _log.error("relay connection superseded by another runner with this runner_id")
                    return
                reason = f"{type(exc).__name__}: {exc}"
            except Exception as exc:  # noqa: BLE001
                # Anything else ends this connection only (a hub that goes away mid-handshake raises EOFError, a bad
                # frame ValueError or KeyError): a loop that died here would leave the node offline for good.
                reason = f"{type(exc).__name__}: {exc}"
            finally:
                self._connected_event.clear()
            if self._stop_event.is_set():
                return
            if self._conn_epoch != epoch:
                attempt = 0
            delay = self._reconnect_delays[min(attempt, len(self._reconnect_delays) - 1)]
            # Once per outage, not once per retry.
            _log.log(logging.WARNING if attempt == 0 else logging.DEBUG, "relay connection lost (%s); retrying", reason)
            attempt += 1
            await asyncio.sleep(delay)

    async def _session_once(self) -> None:
        parsed = urllib.parse.urlparse(self._hub_url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or (443 if parsed.scheme == "wss" else 80)
        path = parsed.path or "/ws/runner"
        use_ssl = parsed.scheme == "wss"
        reader, writer = await asyncio.open_connection(host=host, port=port, ssl=use_ssl)
        self._writer = writer
        send_lock = asyncio.Lock()

        async def _send(obj: Mapping[str, Any]) -> None:
            async with send_lock:
                await send_json(writer, obj, mask=True)

        pending_decisions: dict[str, asyncio.Future[ToolDecision]] = {}

        async def _authorize(call: ToolCall) -> ToolDecision:
            fut: asyncio.Future[ToolDecision] = asyncio.get_running_loop().create_future()
            pending_decisions[call.call_id] = fut
            try:
                await _send(
                    {
                        "type": "tool.authorize",
                        "conn_epoch": self._conn_epoch,
                        "call": call.to_dict(),
                        "ts": utc_now_iso(),
                    }
                )
                return await fut
            finally:
                pending_decisions.pop(call.call_id, None)

        bg_tasks: set[asyncio.Task[None]] = set()
        try:
            await client_handshake(reader, writer, host=f"{host}:{port}", path=path)
            await _send(
                {
                    "type": "hello",
                    "token": self._token,
                    "capabilities": self._daemon.capabilities_sync().to_dict(),
                    "ready_work_keys": await asyncio.to_thread(self._daemon.worktrees.list_ready_work_keys),
                    "ts": utc_now_iso(),
                }
            )
            welcome = await asyncio.wait_for(
                recv_json(reader, writer, expect_masked=False, send_masked=True),
                timeout=5.0,
            )
            if welcome.get("type") != "welcome":
                raise WSProtocolError("expected welcome frame")
            self._conn_epoch = int(welcome["conn_epoch"])
            self._connected_event.set()
            _log.info("relay connected (conn_epoch %s)", self._conn_epoch)

            while not self._stop_event.is_set():
                frame = await recv_json(reader, writer, expect_masked=False, send_masked=True)
                if frame.get("conn_epoch") != self._conn_epoch:
                    continue
                ftype = frame.get("type")
                if ftype == "ping":
                    await _send(
                        {
                            "type": "pong",
                            "conn_epoch": self._conn_epoch,
                            "capabilities": self._daemon.capabilities_sync().to_dict(),
                            "ts": utc_now_iso(),
                        }
                    )
                elif ftype == "run.submit":
                    req = RunRequest.from_dict(cast(Mapping[str, Any], frame["request"]))
                    caller_id = str(frame.get("caller_id", "hibernaut"))
                    caps = self._daemon.capabilities_sync()
                    # A run_id this node already runs (or ran) needs no new slot: daemon.run joins that execution.
                    if caps.slots.available <= 0 and not self._daemon.knows_run(req.run_id):
                        await _send(
                            {
                                "type": "run.accepted",
                                "conn_epoch": self._conn_epoch,
                                "run_id": req.run_id,
                                "accepted": False,
                                "reason": "slots_full",
                                "ts": utc_now_iso(),
                            }
                        )
                        continue
                    await _send(
                        {
                            "type": "run.accepted",
                            "conn_epoch": self._conn_epoch,
                            "run_id": req.run_id,
                            "accepted": True,
                            "ts": utc_now_iso(),
                        }
                    )

                    async def _exec_and_reply(r: RunRequest = req, cid: str = caller_id) -> None:
                        _log.info("run %s accepted (executor %s, owner %s)", r.run_id, r.executor_id, r.owner_scope)
                        res = await self._daemon.run(
                            r, caller_id=cid, authorizer=_authorize if r.authorize_tools else None
                        )
                        _log.info("run %s ended: %s in %d ms", r.run_id, res.status, res.duration_ms)
                        try:
                            await _send(
                                {
                                    "type": "run.completed",
                                    "conn_epoch": self._conn_epoch,
                                    "result": res.to_dict(),
                                    "ts": utc_now_iso(),
                                }
                            )
                        except OSError:
                            pass

                    t = asyncio.create_task(_exec_and_reply())
                    bg_tasks.add(t)
                    t.add_done_callback(bg_tasks.discard)

                elif ftype == "tool.decision":
                    waiting = pending_decisions.get(str(frame.get("call_id", "")))
                    if waiting is not None and not waiting.done():
                        waiting.set_result(ToolDecision.from_dict(cast(Mapping[str, Any], frame["decision"])))

                elif ftype == "run.interrupt":
                    ireq = InterruptRequest.from_dict(cast(Mapping[str, Any], frame["request"]))
                    ires = await self._daemon.interrupt(ireq, caller_id=str(frame.get("caller_id", "hibernaut")))
                    await _send(
                        {
                            "type": "rpc.reply",
                            "conn_epoch": self._conn_epoch,
                            "rpc_id": frame["rpc_id"],
                            "result": ires.to_dict(),
                            "ts": utc_now_iso(),
                        }
                    )
                elif ftype == "run.query":
                    qres = self._daemon.query_run(str(frame["run_id"]), caller_id=str(frame.get("caller_id", "hibernaut")))
                    await _send(
                        {
                            "type": "rpc.reply",
                            "conn_epoch": self._conn_epoch,
                            "rpc_id": frame["rpc_id"],
                            "result": qres.to_dict() if qres is not None else None,
                            "ts": utc_now_iso(),
                        }
                    )
                elif ftype == "reclaim.request":
                    rreq = ReclaimRequest.from_dict(cast(Mapping[str, Any], frame["request"]))
                    caller_id = str(frame.get("caller_id", "hibernaut"))
                    rres = await self._daemon.reclaim_workspace(rreq, caller_id=caller_id)
                    await _send(
                        {
                            "type": "rpc.reply",
                            "conn_epoch": self._conn_epoch,
                            "rpc_id": frame["rpc_id"],
                            "result": rres.to_dict(),
                            "ts": utc_now_iso(),
                        }
                    )
        finally:
            self._connected_event.clear()
            try:
                writer.close()
            except OSError:
                pass
            # The runs of this connection end with it, and the next hello must not be sent until they have: a run
            # still creating its worktree would be missing from ready_work_keys, and the hub would drop its claim.
            await _cancel_and_wait(bg_tasks)


async def _cancel_and_wait(tasks: set[asyncio.Task[None]]) -> None:
    pending: list[asyncio.Task[None]] = [t for t in tasks if not t.done()]
    for t in pending:
        t.cancel()
    cancelled = False
    while pending:
        try:
            await asyncio.shield(asyncio.gather(*pending, return_exceptions=True))
        except asyncio.CancelledError:
            cancelled = True
        pending = [t for t in pending if not t.done()]
    if cancelled:
        raise asyncio.CancelledError
