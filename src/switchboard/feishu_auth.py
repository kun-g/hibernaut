from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from typing import Any, Protocol, cast

from switchboard.feishu_rate_limit import is_rate_limited, reset_seconds
from switchboard.http_client import HttpClient

_REFRESH_MARGIN_SECONDS = 600
# Codes Feishu returns when the bearer token itself is stale or unknown; the next send must mint a new one.
TOKEN_REJECTED_CODES = frozenset({99991661, 99991663, 99991668})


class FeishuTokenSource(Protocol):
    def token(self) -> str: ...

    def invalidate(self) -> None: ...


class FeishuAuthError(RuntimeError):
    def __init__(self, message: str, *, retryable: bool = True, retry_after_seconds: float | None = None) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.retry_after_seconds = retry_after_seconds


class FeishuTenantTokenSource:
    def __init__(
        self,
        http: HttpClient,
        *,
        app_id: str,
        app_secret: str,
        api_base: str = "https://open.feishu.cn",
        now: Callable[[], float] = time.time,
    ) -> None:
        self._http = http
        self._app_id = app_id
        self._app_secret = app_secret
        self._url = f"{api_base.rstrip('/')}/open-apis/auth/v3/tenant_access_token/internal"
        self._now = now
        self._lock = threading.Lock()
        self._cached: tuple[str, float] | None = None

    def token(self) -> str:
        with self._lock:
            if self._cached is not None and self._cached[1] - self._now() > _REFRESH_MARGIN_SECONDS:
                return self._cached[0]
            self._cached = self._mint()
            return self._cached[0]

    def invalidate(self) -> None:
        with self._lock:
            self._cached = None

    def _mint(self) -> tuple[str, float]:
        issued = self._now()
        body = json.dumps({"app_id": self._app_id, "app_secret": self._app_secret}).encode("utf-8")
        response = self._http.request(
            "POST", self._url, headers={"Content-Type": "application/json; charset=utf-8"}, body=body
        )
        try:
            data = cast(dict[str, Any], json.loads(response.body.decode("utf-8") or "{}"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            data = {}
        token = data.get("tenant_access_token")
        if response.status == 200 and data.get("code") == 0 and isinstance(token, str) and token:
            expire = data.get("expire")
            return token, issued + (float(expire) if isinstance(expire, int | float) else 7200.0)
        limited = is_rate_limited(response.status, data.get("code"))
        # Any other 4xx or non-zero code is a wrong app id/secret, which retrying cannot fix.
        raise FeishuAuthError(
            f"feishu tenant_access_token HTTP {response.status} code={data.get('code')} msg={data.get('msg')}",
            retryable=limited or response.status >= 500,
            retry_after_seconds=reset_seconds(response) if limited else None,
        )
