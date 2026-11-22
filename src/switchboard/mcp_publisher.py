from __future__ import annotations

import itertools
import json
from typing import Any, Final, cast

from switchboard.envelope import CanonicalEvent
from switchboard.http_client import HttpClient, UrllibHttpClient
from switchboard.ports import CredentialBrokerPort, IngestResult, IngestStatus

MCP_CREDENTIAL_CHANNEL: Final = "mcp"
REPORT_TOOL: Final = "report_execution"
_INGEST_STATUSES: Final[tuple[IngestStatus, ...]] = ("accepted", "duplicate", "invalid")


class PublishError(RuntimeError):
    pass


# Sends execution.reported to a remote Switchboard's /mcp. Transport, auth and JSON-RPC failures raise,
# so the caller's outbox retries; an envelope the hub rejects comes back as an "invalid" IngestResult.
class SwitchboardMCPPublisher:
    def __init__(
        self,
        mcp_url: str,
        credentials: CredentialBrokerPort,
        *,
        http_client: HttpClient | None = None,
        credential_channel: str = MCP_CREDENTIAL_CHANNEL,
    ) -> None:
        self.mcp_url = mcp_url
        self._credentials = credentials
        self._http: HttpClient = http_client or UrllibHttpClient()
        self._channel = credential_channel
        self._ids = itertools.count(1)

    def publish(self, event: CanonicalEvent) -> IngestResult:
        if event.event_type != "execution.reported":
            raise PublishError(f"only execution.reported can be published over MCP, not {event.event_type}")
        token = self._credentials.issue_ephemeral(self._channel)
        if not token:
            raise PublishError(f"credential broker has no {self._channel!r} token")
        request = {
            "jsonrpc": "2.0",
            "id": next(self._ids),
            "method": "tools/call",
            "params": {"name": REPORT_TOOL, "arguments": report_arguments(event)},
        }
        try:
            response = self._http.request(
                "POST",
                self.mcp_url,
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                body=json.dumps(request, ensure_ascii=False).encode("utf-8"),
            )
        except OSError as exc:
            raise PublishError(f"POST {self.mcp_url} failed: {exc!r}") from exc
        if response.status != 200:
            # The body is not echoed: it is the hub's, and only the status matters for a retry.
            raise PublishError(f"POST {self.mcp_url} returned HTTP {response.status}")
        return parse_tool_result(response.body)


def report_arguments(event: CanonicalEvent) -> dict[str, Any]:
    # event_id stays local: the hub assigns its own and deduplicates on (source, external_id).
    return {
        "external_id": event.external_id,
        "scope": event.scope,
        **({"correlation_key": event.correlation_key} if event.correlation_key else {}),
        "subject": event.subject,
        "body": event.body,
        "actor": event.actor or "",
        "occurred_at": event.occurred_at,
        "metadata": dict(event.metadata),
    }


def parse_tool_result(raw: bytes) -> IngestResult:
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PublishError("switchboard /mcp returned a non-JSON body") from exc
    if not isinstance(payload, dict):
        raise PublishError("switchboard /mcp returned a non-object body")
    body = cast(dict[str, Any], payload)
    error = body.get("error")
    if isinstance(error, dict):
        raise PublishError(f"switchboard /mcp error: {cast(dict[str, Any], error).get('message')}")
    result = body.get("result")
    structured = cast(dict[str, Any], result).get("structuredContent") if isinstance(result, dict) else None
    if not isinstance(structured, dict):
        raise PublishError("switchboard /mcp result has no structuredContent")
    content = cast(dict[str, Any], structured)
    status = content.get("status")
    if status not in _INGEST_STATUSES:
        raise PublishError(f"switchboard {REPORT_TOOL} failed: {content.get('error') or status!r}")
    event_id = content.get("event_id")
    return IngestResult(
        status=status,
        event_id=event_id if isinstance(event_id, str) else None,
        consumers=tuple(str(c) for c in cast(list[Any], content.get("consumers") or [])),
        problems=tuple(str(p) for p in cast(list[Any], content.get("problems") or [])),
    )
