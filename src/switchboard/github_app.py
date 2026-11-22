from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable, Collection, Mapping
from datetime import datetime
from typing import Any, Protocol, cast

from switchboard.http_client import HttpClient
from switchboard.rs256 import RsaPrivateKey, jwt_rs256

# Installation tokens are narrowed to one repository and these permissions, whatever the App itself holds.
COMMENT_PERMISSIONS: Mapping[str, str] = {"issues": "write", "pull_requests": "write"}
_REFRESH_MARGIN_SECONDS = 600


class GitHubTokenSource(Protocol):
    def token_for(self, repo: str) -> str | None: ...


class GitHubAppError(RuntimeError):
    def __init__(self, message: str, *, retryable: bool = True) -> None:
        super().__init__(message)
        self.retryable = retryable


class GitHubAppTokenSource:
    def __init__(
        self,
        http: HttpClient,
        *,
        app_id: str,
        key: RsaPrivateKey,
        allowed_owners: Collection[str],
        api_base: str = "https://api.github.com",
        now: Callable[[], float] = time.time,
    ) -> None:
        self._http = http
        self._app_id = app_id
        self._key = key
        # The App may be installed on accounts Switchboard has no business writing to; minting stops at this list.
        self._owners = frozenset(owner.lower() for owner in allowed_owners)
        self._api_base = api_base.rstrip("/")
        self._now = now
        self._lock = threading.Lock()
        self._tokens: dict[str, tuple[str, float]] = {}

    def token_for(self, repo: str) -> str | None:
        owner, _, name = repo.lower().partition("/")
        if not name or owner not in self._owners:
            raise GitHubAppError(f"repository {repo} is outside github_app_owners", retryable=False)
        with self._lock:
            cached = self._tokens.get(repo.lower())
            if cached is not None and cached[1] - self._now() > _REFRESH_MARGIN_SECONDS:
                return cached[0]
            token, expires_at = self._mint(owner, name)
            self._tokens[repo.lower()] = (token, expires_at)
            return token

    def _mint(self, owner: str, name: str) -> tuple[str, float]:
        issued = int(self._now())
        # iat is backdated for clock skew; GitHub rejects exp more than ten minutes ahead.
        app_jwt = jwt_rs256({"iat": issued - 60, "exp": issued + 540, "iss": self._app_id}, self._key)
        headers = {
            "Authorization": f"Bearer {app_jwt}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "switchboard",
        }
        installation = self._json("GET", f"/repos/{owner}/{name}/installation", headers)
        installation_id = installation.get("id")
        if installation_id is None:
            raise GitHubAppError(f"App is not installed on {owner}/{name}", retryable=False)
        body = json.dumps({"repositories": [name], "permissions": dict(COMMENT_PERMISSIONS)}).encode("utf-8")
        minted = self._json(
            "POST",
            f"/app/installations/{int(installation_id)}/access_tokens",
            {**headers, "Content-Type": "application/json"},
            body,
        )
        token = minted.get("token")
        if not isinstance(token, str) or not token:
            raise GitHubAppError("installation token response carried no token")
        return token, _expiry(minted.get("expires_at"), issued)

    def _json(self, method: str, path: str, headers: Mapping[str, str], body: bytes | None = None) -> dict[str, Any]:
        response = self._http.request(method, f"{self._api_base}{path}", headers=headers, body=body)
        if response.status >= 300:
            excerpt = response.body[:200].decode("utf-8", errors="replace")
            raise GitHubAppError(
                f"GitHub App {method} {path} HTTP {response.status}: {excerpt}",
                retryable=response.status == 429 or response.status >= 500,
            )
        try:
            data: object = json.loads(response.body.decode("utf-8") or "{}")
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise GitHubAppError(f"GitHub App {method} {path} returned invalid JSON") from exc
        if not isinstance(data, dict):
            raise GitHubAppError(f"GitHub App {method} {path} returned non-object JSON")
        return cast(dict[str, Any], data)


def _expiry(raw: object, issued: int) -> float:
    if isinstance(raw, str):
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
        except ValueError:
            pass
    return float(issued + 3600)
