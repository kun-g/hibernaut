from __future__ import annotations

import hashlib
import hmac
from collections.abc import Mapping
from typing import Any

from switchboard.addressing import GITHUB_OUTBOUND_MARKER, github_correlation_key
from switchboard.envelope import CanonicalEvent, EventType, normalize_utc, utc_now
from switchboard.inbound._util import ignored, items, lower_headers, obj, parse_json_object, reject, text
from switchboard.pipeline import Clock
from switchboard.ports import AdapterParseResult, CredentialBrokerPort
from switchboard.router import ScopeResolver

_OPENED_ACTIONS = frozenset({"opened", "reopened", "ready_for_review"})
_WORK_EVENTS = frozenset({"issues", "pull_request"})
_COMMENT_EVENTS = frozenset({"issue_comment", "pull_request_review", "pull_request_review_comment"})


class GitHubInboundAdapter:
    def __init__(
        self,
        credentials: CredentialBrokerPort,
        scopes: ScopeResolver,
        *,
        require_label: str | None = None,
        clock: Clock = utc_now,
    ) -> None:
        self._credentials = credentials
        self._scopes = scopes
        self._require_label = require_label.lower() if require_label else None
        self._clock = clock

    def _missing_label(self, item: Mapping[str, Any]) -> bool:
        if self._require_label is None:
            return False
        names = {text(obj(label).get("name")).lower() for label in items(item.get("labels"))}
        return self._require_label not in names

    def verify_signature(self, raw_body: bytes, signature: str | None) -> bool:
        secret = self._credentials.get_webhook_secret("github")
        # Fail closed: without a configured secret no payload can be trusted.
        if not secret or not signature:
            return False
        digest = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
        return hmac.compare_digest(f"sha256={digest}", signature.strip())

    def handle(self, headers: Mapping[str, str], raw_body: bytes) -> AdapterParseResult:
        lowered = lower_headers(headers)
        if not self.verify_signature(raw_body, lowered.get("x-hub-signature-256")):
            return reject(401, "invalid signature")
        body = parse_json_object(raw_body)
        if body is None:
            return reject(400, "invalid json payload")
        github_event = lowered.get("x-github-event", "").strip()
        if github_event not in _WORK_EVENTS and github_event not in _COMMENT_EVENTS:
            return ignored(f"event {github_event or 'unknown'} not mapped")

        repo = text(obj(body.get("repository")).get("full_name")).lower()
        if not repo:
            return reject(400, "missing repository.full_name")
        scope = self._scopes.github(repo)
        if scope is None:
            return ignored(f"repository {repo} has no scope route")
        action = text(body.get("action"))
        if github_event in _WORK_EVENTS:
            return self._work_event(github_event, action, repo, scope, body)
        return self._comment_event(github_event, action, repo, scope, body)

    def _work_event(
        self,
        github_event: str,
        action: str,
        repo: str,
        scope: str,
        body: dict[str, Any],
    ) -> AdapterParseResult:
        is_pr = github_event == "pull_request"
        item = obj(body.get("pull_request" if is_pr else "issue"))
        number = item.get("number")
        if number is None or not action:
            return reject(400, "missing number or action")
        # Issues events also fire for PRs; such deliveries are reported once via pull_request.
        if not is_pr and "pull_request" in item:
            return ignored("issue event for a pull request")
        if self._missing_label(item):
            return ignored(f"{github_event} #{number} lacks label {self._require_label}")
        event_type: EventType
        if action in _OPENED_ACTIONS:
            event_type = "work.opened"
        elif action == "closed":
            event_type = "work.closed"
        else:
            event_type = "work.updated"
        updated_at = text(item.get("updated_at"))
        prefix = "pr" if is_pr else "issue"
        labels = [text(obj(label).get("name")) for label in items(item.get("labels"))]
        return AdapterParseResult(
            event=CanonicalEvent(
                source="github",
                event_type=event_type,
                external_id=f"{prefix}:{repo}#{number}:{action}:{updated_at}",
                occurred_at=normalize_utc(updated_at, self._clock()),
                scope=scope,
                correlation_key=github_correlation_key(repo, number),
                actor=text(obj(body.get("sender")).get("login")) or None,
                subject=text(item.get("title")),
                body=text(item.get("body")),
                metadata={
                    "work_kind": "pull_request" if is_pr else "issue",
                    "state": text(item.get("state")) or "open",
                    "action": action,
                    "repo": repo,
                    "number": number,
                    "url": text(item.get("html_url")),
                    "labels": labels,
                    "merged": bool(item.get("merged")) if is_pr else False,
                    "github_event": github_event,
                },
            ),
            http_status=202,
        )

    def _comment_event(
        self,
        github_event: str,
        action: str,
        repo: str,
        scope: str,
        body: dict[str, Any],
    ) -> AdapterParseResult:
        if github_event == "pull_request_review":
            if action != "submitted":
                return ignored(f"review action {action} not mapped")
            comment = obj(body.get("review"))
            item = obj(body.get("pull_request"))
            prefix = "review"
            occurred = text(comment.get("submitted_at"))
        else:
            if action != "created":
                return ignored(f"comment action {action} not mapped")
            comment = obj(body.get("comment"))
            item = obj(body.get("issue" if github_event == "issue_comment" else "pull_request"))
            prefix = "comment" if github_event == "issue_comment" else "review_comment"
            occurred = text(comment.get("created_at"))
        sender = obj(body.get("sender"))
        # Our own outbound comments come back as webhooks; dropping bot authors breaks the loop.
        if text(sender.get("type")) == "Bot" or text(obj(comment.get("user")).get("type")) == "Bot":
            return ignored("bot comment")
        if GITHUB_OUTBOUND_MARKER in text(comment.get("body")):
            return ignored("switchboard outbound comment")
        if self._missing_label(item):
            return ignored(f"{github_event} on #{item.get('number')} lacks label {self._require_label}")
        comment_id = comment.get("id")
        number = item.get("number")
        if comment_id is None or number is None:
            return reject(400, "missing comment id or number")
        comment_body = text(comment.get("body"))
        if not comment_body and prefix == "review":
            comment_body = f"[review {text(comment.get('state')).lower()}]"
        return AdapterParseResult(
            event=CanonicalEvent(
                source="github",
                event_type="conversation.message",
                external_id=f"{prefix}:{comment_id}",
                occurred_at=normalize_utc(occurred, self._clock()),
                scope=scope,
                correlation_key=github_correlation_key(repo, number),
                actor=text(obj(comment.get("user")).get("login")) or text(sender.get("login")) or None,
                subject=text(item.get("title")),
                body=comment_body,
                metadata={
                    "channel": "github",
                    "repo": repo,
                    "number": number,
                    "comment_id": comment_id,
                    "url": text(comment.get("html_url")),
                    "github_event": github_event,
                },
            ),
            http_status=202,
        )
