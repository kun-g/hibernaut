from __future__ import annotations

import hmac
import json
from collections.abc import Mapping
from typing import Any

from agent_runtime.adapters.channels.http_types import WebhookResponse
from agent_runtime.engine.handler import EventHandler


class FeishuWebhookAdapter:
    def __init__(
        self,
        handler: EventHandler,
        *,
        verification_token: str | None = None,
        chat_inbox_map: Mapping[str, str] | None = None,
    ) -> None:
        self._handler = handler
        self._verification_token = verification_token
        self._chat_inbox_map: dict[str, str] = dict(chat_inbox_map or {})

    def _verify_token(self, body: dict[str, Any]) -> bool:
        if self._verification_token is None:
            return True
        header = body.get("header")
        token = (
            header.get("token")
            if isinstance(header, dict) and header.get("token") is not None
            else body.get("token")
        )
        if not isinstance(token, str):
            return False
        return hmac.compare_digest(self._verification_token, token)

    def handle(
        self,
        raw_body: bytes,
        headers: Mapping[str, str] | None = None,
    ) -> WebhookResponse:
        _ = headers
        try:
            body: dict[str, Any] = json.loads(raw_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return WebhookResponse(status_code=400, body={"error": "invalid json payload"})

        if not self._verify_token(body):
            return WebhookResponse(status_code=401, body={"error": "invalid verification token"})

        if body.get("type") == "url_verification":
            return WebhookResponse(
                status_code=200,
                body={"challenge": str(body.get("challenge", ""))},
            )

        card_response = self._try_handle_card_callback(body)
        if card_response is not None:
            return card_response

        header = body.get("header")
        if not isinstance(header, dict) or not header.get("event_id"):
            return WebhookResponse(status_code=400, body={"error": "missing header.event_id"})

        event_id = str(header["event_id"])
        event_type = str(header.get("event_type") or "event")
        event_obj = body.get("event") if isinstance(body.get("event"), dict) else {}
        message_obj = event_obj.get("message") if isinstance(event_obj.get("message"), dict) else {}

        chat_id = str(message_obj.get("chat_id") or event_obj.get("chat_id") or "default")
        inbox_id = self._chat_inbox_map.get(chat_id, f"feishu:{chat_id}")
        thread_anchor = (
            message_obj.get("thread_id")
            or message_obj.get("root_id")
            or message_obj.get("message_id")
        )
        correlation_key = f"feishu:{chat_id}:{thread_anchor}" if thread_anchor else None

        result = self._handler.ingest(
            inbox_id=inbox_id,
            source=f"feishu:{event_type}",
            external_id=event_id,
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

    def _try_handle_card_callback(self, body: dict[str, Any]) -> WebhookResponse | None:
        event_obj = body.get("event") if isinstance(body.get("event"), dict) else {}
        action_obj = (
            event_obj.get("action")
            if isinstance(event_obj.get("action"), dict)
            else body.get("action")
        )
        if not isinstance(action_obj, dict):
            return None

        action_value = action_obj.get("value")
        if not isinstance(action_value, dict) or "approval_request_id" not in action_value:
            return None

        context_obj = event_obj.get("context") if isinstance(event_obj.get("context"), dict) else {}
        chat_id = str(
            context_obj.get("open_chat_id")
            or body.get("open_chat_id")
            or action_value.get("chat_id")
            or "default"
        )
        inbox_id = str(
            action_value.get("inbox_id")
            or self._chat_inbox_map.get(chat_id, f"feishu:{chat_id}")
        )

        header = body.get("header") if isinstance(body.get("header"), dict) else {}
        open_message_id = context_obj.get("open_message_id") or body.get("open_message_id") or "card"
        action_time = body.get("action_time") or header.get("create_time") or "0"
        external_id = str(
            header.get("event_id")
            or (f"{open_message_id}:{action_time}" if open_message_id != "card" else body.get("token") or f"card:{action_time}")
        )

        approved_raw = action_value.get("approved")
        approved = (
            approved_raw
            if isinstance(approved_raw, bool)
            else str(approved_raw).strip().lower() in {"true", "1", "yes", "approve", "approved"}
        )

        result = self._handler.ingest(
            inbox_id=inbox_id,
            source="feishu:card_action",
            external_id=external_id,
            payload={
                "approval_request_id": str(action_value["approval_request_id"]),
                "approved": approved,
                "raw": body,
            },
        )
        return WebhookResponse(
            status_code=202,
            body={
                "event_id": result.event_id,
                "status": result.status,
                "duplicate": result.duplicate,
                "inbox_id": inbox_id,
            },
        )
