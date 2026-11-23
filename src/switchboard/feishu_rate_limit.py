from __future__ import annotations

import math

from switchboard.http_client import HttpResponse

# 230020: the send-message limit, HTTP 400. 99991400: the platform-wide limit, HTTP 429 or 400 on older endpoints.
_RATE_LIMIT_CODES = frozenset({230020, 99991400})
_RESET_HEADER = "x-ogw-ratelimit-reset"


def is_rate_limited(status: int, code: object) -> bool:
    return status == 429 or (status == 400 and isinstance(code, int) and code in _RATE_LIMIT_CODES)


def reset_seconds(response: HttpResponse) -> float | None:
    try:
        seconds = float(response.header(_RESET_HEADER) or "")
    except ValueError:
        return None
    return seconds if math.isfinite(seconds) and seconds > 0 else None
