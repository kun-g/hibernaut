from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any, cast
from urllib.parse import quote

from switchboard.addressing import GITHUB_OUTBOUND_MARKER
from switchboard.http_client import HttpClient, HttpResponse
from switchboard.ports import CredentialBrokerPort, OutboundMessage, SendResult


class GitHubOutboundAdapter:
    def __init__(
        self,
        http: HttpClient,
        credentials: CredentialBrokerPort,
        *,
        api_base: str = "https://api.github.com",
    ) -> None:
        self._http = http
        self._credentials = credentials
        self._api_base = api_base.rstrip("/")

    def send(self, msg: OutboundMessage) -> SendResult:
        repo = msg.target.get("repo")
        number = msg.target.get("number")
        if not repo or number is None:
            return SendResult(ok=False, error="github target needs repo and number", retryable=False)
        token = self._credentials.issue_ephemeral("github")
        if not token:
            return SendResult(ok=False, error="github credential unavailable", retryable=True)
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
            "User-Agent": "switchboard",
        }
        issue_url = f"{self._api_base}/repos/{repo}/issues/{int(number)}"

        # Label changes are idempotent, so they go first: a retry after a failed label
        # call must not post the comment twice.
        add_labels = _labels(msg.target.get("add_labels"))
        if add_labels:
            response = self._http.request(
                "POST",
                f"{issue_url}/labels",
                headers=headers,
                body=json.dumps({"labels": add_labels}).encode("utf-8"),
            )
            if response.status >= 300:
                return _failure("add_labels", response)
        for label in _labels(msg.target.get("remove_labels")):
            response = self._http.request(
                "DELETE", f"{issue_url}/labels/{quote(label, safe='')}", headers=headers
            )
            if response.status >= 300 and response.status != 404:
                return _failure("remove_labels", response)

        text = f"**{msg.title}**\n\n{msg.body_md}" if msg.title else msg.body_md
        body = f"{text}\n\n{GITHUB_OUTBOUND_MARKER}"
        response = self._http.request(
            "POST",
            f"{issue_url}/comments",
            headers=headers,
            body=json.dumps({"body": body}, ensure_ascii=False).encode("utf-8"),
        )
        if response.status >= 300:
            return _failure("comment", response)
        data: Mapping[str, Any]
        try:
            data = cast(Mapping[str, Any], json.loads(response.body.decode("utf-8") or "{}"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            data = {}
        comment_id: Any = data.get("id")
        return SendResult(ok=True, platform_msg_id=None if comment_id is None else str(comment_id))


def _labels(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, Sequence):
        return [str(item) for item in cast(Sequence[Any], value)]
    return []


def _failure(step: str, response: HttpResponse) -> SendResult:
    excerpt = response.body[:200].decode("utf-8", errors="replace")
    return SendResult(
        ok=False,
        error=f"github {step} HTTP {response.status}: {excerpt}",
        retryable=response.status == 429 or response.status >= 500,
    )
