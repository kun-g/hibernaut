from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any, cast

# Longest Unix socket path every supported system accepts (macOS allows 104 bytes).
MAX_SOCKET_PATH = 100
_STREAM_LIMIT = 4 * 1024 * 1024
_MAX_TEXT = 100_000


def clip(text: str) -> str:
    return text if len(text) <= _MAX_TEXT else f"{text[:_MAX_TEXT]}\n[{len(text) - _MAX_TEXT} more characters cut]"


def request_of(line: bytes) -> dict[str, Any] | None:
    try:
        request: object = json.loads(line)
    except (ValueError, RecursionError):
        return None
    return cast(dict[str, Any], request) if isinstance(request, dict) else None


@contextlib.asynccontextmanager
async def line_server(path: str, answer: Callable[[bytes], Awaitable[dict[str, Any]]]) -> AsyncGenerator[None]:
    # One JSON line each way, over a Unix socket. A socket left at the path is replaced.
    connections: set[asyncio.Task[None]] = set()

    async def connected(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = cast(asyncio.Task[None], asyncio.current_task())
        connections.add(task)
        try:
            while line := await reader.readline():
                writer.write((json.dumps(await answer(line), ensure_ascii=False) + "\n").encode("utf-8"))
                await writer.drain()
        except (ConnectionError, ValueError):
            pass
        finally:
            connections.discard(task)
            writer.close()

    server = await asyncio.start_unix_server(connected, path, limit=_STREAM_LIMIT)
    try:
        yield
    finally:
        server.close()
        for task in list(connections):
            task.cancel()
        await asyncio.gather(*connections, return_exceptions=True)
