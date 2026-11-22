from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, cast
from urllib.parse import quote

from switchboard.feishu_auth import TOKEN_REJECTED_CODES, FeishuAuthError, FeishuTokenSource
from switchboard.http_client import HttpClient, HttpResponse
from switchboard.ports import CredentialBrokerPort, OutboundMessage, SendResult


class FeishuOutboundAdapter:
    def __init__(
        self,
        http: HttpClient,
        credentials: CredentialBrokerPort,
        *,
        api_base: str = "https://open.feishu.cn",
        tokens: FeishuTokenSource | None = None,
    ) -> None:
        self._http = http
        self._credentials = credentials
        self._tokens = tokens
        self._api_base = api_base.rstrip("/")

    def render(self, msg: OutboundMessage) -> tuple[str, str]:
        if msg.kind == "approval_card":
            if msg.card_payload is None:
                raise ValueError("approval_card requires card_payload")
            return "interactive", json.dumps(dict(msg.card_payload), ensure_ascii=False)
        text = f"{msg.title}\n{msg.body_md}" if msg.title else msg.body_md
        return "text", json.dumps({"text": text}, ensure_ascii=False)

    def send(self, msg: OutboundMessage) -> SendResult:
        try:
            token = self._tokens.token() if self._tokens is not None else self._credentials.issue_ephemeral("feishu")
        except FeishuAuthError as exc:
            return SendResult(ok=False, error=str(exc), retryable=exc.retryable)
        if not token:
            return SendResult(ok=False, error="feishu credential unavailable", retryable=True)
        msg_type, content = self.render(msg)
        reply_to = msg.target.get("reply_to_message_id")
        chat_id = msg.target.get("chat_id")
        if reply_to:
            url = f"{self._api_base}/open-apis/im/v1/messages/{quote(str(reply_to), safe='')}/reply"
            payload: dict[str, Any] = {"msg_type": msg_type, "content": content, "uuid": msg.msg_id}
        elif chat_id:
            url = f"{self._api_base}/open-apis/im/v1/messages?receive_id_type=chat_id"
            payload = {
                "receive_id": str(chat_id),
                "msg_type": msg_type,
                "content": content,
                "uuid": msg.msg_id,
            }
        else:
            return SendResult(ok=False, error="feishu target needs chat_id or reply_to_message_id", retryable=False)
        response = self._http.request(
            "POST",
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json; charset=utf-8",
            },
            body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        )
        result, code = _feishu_result(response)
        if result.ok and msg.kind == "approval_card" and not result.platform_msg_id:
            # Retry with the same UUID: the receipt is needed to bind the callback to this report.
            return SendResult(ok=False, error="feishu approval card response missing message_id", retryable=True)
        if code in TOKEN_REJECTED_CODES and self._tokens is not None:
            self._tokens.invalidate()
            return SendResult(ok=False, error=result.error, retryable=True)
        return result


def _feishu_result(response: HttpResponse) -> tuple[SendResult, object]:
    data: Mapping[str, Any]
    try:
        data = cast(Mapping[str, Any], json.loads(response.body.decode("utf-8") or "{}"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        data = {}
    if response.status == 200 and data.get("code") == 0:
        inner: Any = data.get("data")
        message_id: Any = cast(Mapping[str, Any], inner).get("message_id") if isinstance(inner, Mapping) else None
        return SendResult(ok=True, platform_msg_id=None if message_id is None else str(message_id)), 0
    retryable = response.status == 429 or response.status >= 500
    code = data.get("code")
    return SendResult(
        ok=False, error=f"feishu HTTP {response.status} code={code} msg={data.get('msg')}", retryable=retryable
    ), code
