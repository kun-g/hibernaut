from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Mapping
from typing import Any

from agent_runtime.adapters.channels.http_types import WebhookResponse
from agent_runtime.engine.handler import EventHandler

_ISSUE_EVENTS = frozenset({"issues", "issue_comment"})
_PR_EVENTS = frozenset({"pull_request", "pull_request_review", "pull_request_review_comment"})


class GitHubWebhookAdapter:
    def __init__(
        self,
        handler: EventHandler,
        *,
        webhook_secret: str | None = None,
        repo_inbox_map: Mapping[str, str] | None = None,
    ) -> None:
        self._handler = handler
        self._webhook_secret = webhook_secret
        self._repo_inbox_map: dict[str, str] = dict(repo_inbox_map or {})

    def verify_signature(self, raw_body: bytes, signature_header: str | None) -> bool:
        if self._webhook_secret is None:
            return True
        if not signature_header:
            return False
        digest = hmac.new(
            self._webhook_secret.encode("utf-8"),
            raw_body,
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(f"sha256={digest}", signature_header)

    def handle(self, raw_body: bytes, headers: Mapping[str, str]) -> WebhookResponse:
        lower_headers = {k.lower(): v for k, v in headers.items()}
        if not self.verify_signature(raw_body, lower_headers.get("x-hub-signature-256")):
            return WebhookResponse(status_code=401, body={"error": "invalid signature"})

        delivery_id = lower_headers.get("x-github-delivery", "").strip()
        if not delivery_id:
            return WebhookResponse(status_code=400, body={"error": "missing X-GitHub-Delivery"})

        try:
            body: dict[str, Any] = json.loads(raw_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return WebhookResponse(status_code=400, body={"error": "invalid json payload"})

        event_type = lower_headers.get("x-github-event", "unknown").strip() or "unknown"
        repo_obj = body.get("repository")
        repo_full_name = (
            str(repo_obj.get("full_name"))
            if isinstance(repo_obj, dict) and repo_obj.get("full_name")
            else "unknown"
        )
        inbox_id = self._repo_inbox_map.get(repo_full_name, f"proj:{repo_full_name}")
        correlation_key = self._extract_correlation_key(event_type, repo_full_name, body)

        result = self._handler.ingest(
            inbox_id=inbox_id,
            source=f"github:{event_type}",
            external_id=delivery_id,
            correlation_key=correlation_key,
            payload=body,
        )
        return WebhookResponse(
            status_code=202,
            body={
                "event_id": result.event_id,
                "status": result.status,
                "duplicate": result.duplicate,
                "inbox_id": inbox_id,
                "correlation_key": correlation_key,
            },
        )

    def _extract_correlation_key(
        self,
        event_type: str,
        repo_full_name: str,
        body: dict[str, Any],
    ) -> str | None:
        if event_type in _ISSUE_EVENTS:
            issue = body.get("issue")
            if isinstance(issue, dict) and issue.get("number") is not None:
                return f"github:{repo_full_name}#{issue['number']}"
        if event_type in _PR_EVENTS:
            pr = body.get("pull_request")
            if isinstance(pr, dict) and pr.get("number") is not None:
                return f"github:{repo_full_name}#{pr['number']}"
        return None
