from __future__ import annotations

import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Protocol


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status: int
    body: bytes
    headers: Mapping[str, str] = field(default_factory=lambda: {})

    def header(self, name: str) -> str | None:
        wanted = name.lower()
        return next((value for key, value in self.headers.items() if key.lower() == wanted), None)


class HttpClient(Protocol):
    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes | None = None,
    ) -> HttpResponse: ...


class UrllibHttpClient:
    def __init__(self, timeout_seconds: float = 10.0) -> None:
        self._timeout = timeout_seconds

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes | None = None,
    ) -> HttpResponse:
        req = urllib.request.Request(url, data=body, method=method, headers=dict(headers))
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return HttpResponse(status=int(resp.status), body=resp.read(), headers=dict(resp.headers.items()))
        except urllib.error.HTTPError as exc:
            return HttpResponse(status=int(exc.code), body=exc.read(), headers=dict(exc.headers.items()))
