import fcntl
import os
from pathlib import Path


def next_count(path: Path) -> int:
    # 0 for the first caller, then 1, ... across concurrent processes. Written in one call over the old number, which
    # is never longer, so a caller killed here cannot leave an empty or partial count behind.
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        count = int(os.pread(fd, 32, 0) or b"0")
        os.pwrite(fd, str(count + 1).encode(), 0)
        return count
    finally:
        os.close(fd)
