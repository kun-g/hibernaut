from __future__ import annotations

import hmac
import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from switchboard.envelope import CanonicalEvent, format_utc, utc_now
from switchboard.feishu_cards import render_resolved_card
from switchboard.inbound._util import ignored, items, obj, parse_json_object, reject, text
from switchboard.pipeline import Clock, describe_ingest
from switchboard.ports import AdapterParseResult, CredentialBrokerPort, IngestResult
from switchboard.router import ScopeResolver

_MENTION_PLACEHOLDER = re.compile(r"@_user_\d+\s*")


class FeishuInboundAdapter:
    def __init__(
        self,
        credentials: CredentialBrokerPort,
        scopes: ScopeResolver,
        *,
        group_require_mention: bool = True,
        bot_open_id: str | None = None,
        clock: Clock = utc_now,
    ) -> None:
        self._credentials = credentials
        self._scopes = scopes
        self._group_require_mention = group_require_mention
        self._bot_open_id = bot_open_id
        self._clock = clock

    def _verify_token(self, body: Mapping[str, Any]) -> bool:
        expected = self._credentials.get_webhook_secret("feishu")
        supplied = text(body.get("token") or obj(body.get("header")).get("token"))
        if not expected or not supplied:
            return False
        return hmac.compare_digest(expected, supplied)

    def handle(self, headers: Mapping[str, str], raw_body: bytes) -> AdapterParseResult:
        body = parse_json_object(raw_body)
        if body is None:
            return reject(400, "invalid json payload")
        if "encrypt" in body:
            return reject(400, "encrypted events are not supported; disable the Encrypt Key")
        if not self._verify_token(body):
            return reject(401, "invalid verification token")
        if body.get("type") == "url_verification":
            return AdapterParseResult(event=None, response_body={"challenge": body.get("challenge")})

        event_type = text(obj(body.get("header")).get("event_type"))
        event = obj(body.get("event"))
        if event_type == "im.message.receive_v1":
            return self._message(event)
        if event_type == "card.action.trigger":
            return self._card_action(event)
        return ignored(f"event {event_type or 'unknown'} not mapped")

    def finalize(self, parsed: AdapterParseResult, ingest: IngestResult) -> AdapterParseResult:
        if parsed.event is None:
            return parsed
        if parsed.event.event_type != "approval.decided":
            return AdapterParseResult(
                event=parsed.event,
                http_status=400 if ingest.status == "invalid" else parsed.http_status,
                response_body=dict(describe_ingest(ingest)),
            )
        # Card callbacks must answer with card JSON only, so ingest details are not merged in.
        if ingest.status == "duplicate" and ingest.existing is not None:
            first = ingest.existing
            card = render_resolved_card(
                approval_request_id=text(first.metadata.get("approval_request_id")),
                decision=text(first.metadata.get("decision")),
                actor=first.actor,
                correlation_key=first.correlation_key,
            )
            return AdapterParseResult(
                event=parsed.event,
                http_status=200,
                response_body={
                    "toast": {"type": "info", "content": "该审批已处理，本次点击已忽略"},
                    "card": {"type": "raw", "data": card},
                },
            )
        if ingest.status == "invalid":
            return AdapterParseResult(
                event=parsed.event,
                http_status=200,
                response_body={"toast": {"type": "error", "content": "审批回调缺少必要字段"}},
            )
        return parsed

    def _message(self, event: dict[str, Any]) -> AdapterParseResult:
        sender = obj(event.get("sender"))
        sender_open_id = text(obj(sender.get("sender_id")).get("open_id"))
        if text(sender.get("sender_type")) == "app" or (
            self._bot_open_id is not None and sender_open_id == self._bot_open_id
        ):
            return ignored("bot message")
        message = obj(event.get("message"))
        message_id = text(message.get("message_id"))
        chat_id = text(message.get("chat_id"))
        chat_type = text(message.get("chat_type"))
        if not message_id or not chat_id:
            return reject(400, "missing message_id or chat_id")

        mentions = [obj(item) for item in items(message.get("mentions"))]
        if chat_type == "p2p":
            correlation_key = f"feishu:p2p:{chat_id}"
        else:
            if self._group_require_mention and not self._mentions_bot(mentions):
                return ignored("group message without bot mention")
            thread = (
                text(message.get("thread_id"))
                or text(message.get("root_id"))
                or message_id
            )
            correlation_key = f"feishu:thread:{thread}"

        scope = self._scopes.feishu(chat_id)
        if scope is None:
            return ignored(f"chat {chat_id} has no scope route")
        return AdapterParseResult(
            event=CanonicalEvent(
                source="feishu",
                event_type="conversation.message",
                external_id=f"msg:{message_id}",
                occurred_at=_from_millis(message.get("create_time"), self._clock()),
                scope=scope,
                correlation_key=correlation_key,
                actor=sender_open_id or None,
                body=_message_text(message),
                metadata={
                    "channel": "feishu",
                    "chat_id": chat_id,
                    "chat_type": chat_type,
                    "message_id": message_id,
                    "message_type": text(message.get("message_type")),
                },
            ),
            http_status=200,
        )

    def _mentions_bot(self, mentions: list[dict[str, Any]]) -> bool:
        if not mentions:
            return False
        if self._bot_open_id is None:
            return True
        return any(text(obj(m.get("id")).get("open_id")) == self._bot_open_id for m in mentions)

    def _card_action(self, event: dict[str, Any]) -> AdapterParseResult:
        value = obj(obj(event.get("action")).get("value"))
        approval_request_id = text(value.get("approval_request_id"))
        decision = text(value.get("decision"))
        if not approval_request_id or not decision:
            return reject(400, "card value missing approval_request_id or decision")
        context = obj(event.get("context"))
        actor = text(obj(event.get("operator")).get("open_id")) or None
        scope = text(value.get("scope")) or self._scopes.fallback(
            self._scopes.feishu(text(context.get("open_chat_id")))
        )
        correlation_key = text(value.get("correlation_key")) or None
        card = render_resolved_card(
            approval_request_id=approval_request_id,
            decision=decision,
            actor=actor,
            correlation_key=correlation_key,
        )
        return AdapterParseResult(
            event=CanonicalEvent(
                source="feishu",
                event_type="approval.decided",
                # Excludes the decision so a second click (even a different button) is a duplicate.
                external_id=f"approval:{approval_request_id}",
                occurred_at=format_utc(self._clock()),
                scope=scope,
                correlation_key=correlation_key,
                actor=actor,
                body=decision,
                metadata={
                    "approval_request_id": approval_request_id,
                    "decision": decision,
                    "session_id": text(value.get("session_id")),
                    "channel": "feishu",
                    "message_id": text(context.get("open_message_id")),
                },
            ),
            http_status=200,
            response_body={
                "toast": {"type": "success", "content": "审批已提交"},
                "card": {"type": "raw", "data": card},
            },
        )


def _message_text(message: dict[str, Any]) -> str:
    raw = text(message.get("content"))
    try:
        content = obj(json.loads(raw))
    except json.JSONDecodeError:
        return raw
    body = text(content.get("text")) or raw
    return _MENTION_PLACEHOLDER.sub("", body).strip()


def _from_millis(value: Any, fallback: datetime) -> str:
    try:
        return format_utc(datetime.fromtimestamp(int(text(value)) / 1000, tz=UTC))
    except (TypeError, ValueError, OverflowError):
        return format_utc(fallback)
