import asyncio
from collections.abc import Callable, Coroutine
from typing import Any, TypeVar
from unittest.mock import patch

T = TypeVar("T")


async def expire_when_ready(run: Coroutine[Any, Any, T], seconds: float, ready: Callable[[], bool]) -> T:
    # Expire the run's real asyncio deadline only after the child to be cleaned up exists.
    timeout = asyncio.timeout
    deadline: asyncio.Timeout | None = None

    def capture(delay: float | None) -> asyncio.Timeout:
        nonlocal deadline
        if delay == seconds:
            deadline = timeout(None)
            return deadline
        return timeout(delay)

    with patch.object(asyncio, "timeout", capture):
        task = asyncio.create_task(run)
        try:
            async with timeout(30):
                while not task.done():
                    if deadline is not None and ready():
                        deadline.reschedule(asyncio.get_running_loop().time())
                        break
                    await asyncio.sleep(0.01)
                return await task
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
