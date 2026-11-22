from __future__ import annotations

import hashlib
import hmac
import json
import tempfile
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from switchboard.app import Switchboard, build_switchboard
from switchboard.config import SwitchboardConfig, parse_config
from switchboard.http_client import HttpResponse
from switchboard.ports import RecordingAuditSink

# Test-only placeholders; not real credentials.
GITHUB_SECRET = "test-github-secret"
FEISHU_TOKEN = "test-feishu-verification-token"
MCP_TOKEN = "test-mcp-bearer"
RELAY_TOKEN = "test-relay-token"


class FakeClock:
    def __init__(self, start: datetime | None = None) -> None:
        self.now = start or datetime(2026, 11, 22, 8, 0, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now = self.now + timedelta(seconds=seconds)


class StaticCredentials:
    def __init__(self) -> None:
        self.secrets = {
            "github": GITHUB_SECRET,
            "feishu": FEISHU_TOKEN,
            "mcp": MCP_TOKEN,
            "relay": RELAY_TOKEN,
        }
        self.tokens = {
            "github": "ephemeral-github",
            "feishu": "ephemeral-feishu",
            "relay": RELAY_TOKEN,
        }

    def get_webhook_secret(self, channel: str) -> str | None:
        return self.secrets.get(channel)

    def issue_ephemeral(self, channel: str) -> str | None:
        return self.tokens.get(channel)


@dataclass
class FakeHttpClient:
    calls: list[dict[str, Any]] = field(default_factory=list)
    responses: list[HttpResponse] = field(default_factory=list)

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes | None = None,
    ) -> HttpResponse:
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": dict(headers),
                "json": None if body is None else json.loads(body.decode("utf-8")),
            }
        )
        if self.responses:
            return self.responses.pop(0)
        if "feishu" in url:
            return HttpResponse(200, json.dumps({"code": 0, "data": {"message_id": "om_out"}}).encode())
        return HttpResponse(201, json.dumps({"id": 9001}).encode())


def make_config(tmp: str, **sections: Mapping[str, Any]) -> SwitchboardConfig:
    raw: dict[str, Any] = {
        "sqlite": {"path": str(Path(tmp) / "switchboard.db")},
        "routing": {
            "default_scope": "user:main",
            "github_repos": {"Kun-G/Hibernaut": "proj:hibernaut", "kun-g/other": "proj:other"},
            "feishu_chats": {"oc_ops": "proj:hibernaut"},
        },
        "delivery": {
            "dead_letter_alert_channel": "feishu",
            "dead_letter_alert_target": "oc_alerts",
        },
        # These harnesses exercise the reply loop; the default origin policy has its own tests.
        "outbound": {"approval_channel": "feishu", "approval_target": "oc_approvals", "reply_to_origin": ["github", "feishu"]},
    }
    for name, values in sections.items():
        raw[name] = {**raw.get(name, {}), **values}
    return parse_config(raw)


@dataclass
class Harness:
    board: Switchboard
    clock: FakeClock
    audit: RecordingAuditSink
    http: FakeHttpClient
    tmp: tempfile.TemporaryDirectory[str]

    @property
    def url(self) -> str:
        return self.board.http.base_url

    def close(self) -> None:
        self.board.http.stop()
        self.board.store.close()
        self.tmp.cleanup()


def make_harness(*, start_http: bool = True, **sections: Mapping[str, Any]) -> Harness:
    tmp = tempfile.TemporaryDirectory()
    clock = FakeClock()
    audit = RecordingAuditSink()
    http = FakeHttpClient()
    config = make_config(tmp.name, **sections)
    config = replace(config, http=replace(config.http, port=0))
    board = build_switchboard(
        config, credentials=StaticCredentials(), audit=audit, http_client=http, clock=clock
    )
    if start_http:
        board.http.start()
    return Harness(board=board, clock=clock, audit=audit, http=http, tmp=tmp)


def post(
    url: str,
    body: bytes | Mapping[str, Any],
    headers: Mapping[str, str] | None = None,
) -> tuple[int, dict[str, Any]]:
    raw = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=raw, method="POST", headers=dict(headers or {}))
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"{}")


def github_headers(raw: bytes, event: str, delivery: str, secret: str = GITHUB_SECRET) -> dict[str, str]:
    digest = hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()
    return {
        "X-GitHub-Event": event,
        "X-GitHub-Delivery": delivery,
        "X-Hub-Signature-256": f"sha256={digest}",
        "Content-Type": "application/json",
    }


def issue_payload(
    action: str = "opened",
    *,
    number: int = 42,
    repo: str = "Kun-G/Hibernaut",
    state: str = "open",
    updated_at: str = "2026-11-22T04:00:00Z",
) -> dict[str, Any]:
    return {
        "action": action,
        "issue": {
            "number": number,
            "title": "Switchboard phase 1",
            "body": "Implement it",
            "state": state,
            "updated_at": updated_at,
            "html_url": f"https://github.com/{repo}/issues/{number}",
            "labels": [{"name": "P1"}],
        },
        "repository": {"full_name": repo},
        "sender": {"login": "kun", "type": "User"},
    }


def send_github(
    url: str, payload: Mapping[str, Any], event: str = "issues", delivery: str = "d-1"
) -> tuple[int, dict[str, Any]]:
    raw = json.dumps(payload).encode("utf-8")
    return post(f"{url}/webhooks/github", raw, github_headers(raw, event, delivery))


def mcp_call(url: str, tool: str, arguments: Mapping[str, Any], token: str = MCP_TOKEN) -> tuple[int, dict[str, Any]]:
    return post(
        f"{url}/mcp",
        {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": tool, "arguments": dict(arguments)}},
        {"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )


def tool_result(response: dict[str, Any]) -> dict[str, Any]:
    return json.loads(response["result"]["content"][0]["text"])
