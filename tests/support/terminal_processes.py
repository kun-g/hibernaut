import asyncio
import os
import time


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


async def until(condition, seconds: float = 30.0) -> bool:
    # Less than the 60s the processes under test sleep, so one left behind cannot end by itself in time.
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if condition():
            return True
        await asyncio.sleep(0.05)
    return condition()
