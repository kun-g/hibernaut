from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, cast
from urllib.parse import quote

from switchboard.http_client import HttpClient, HttpResponse
from switchboard.ports import CredentialBrokerPort, OutboundMessage, SendResult


class FeishuOutboundAdapter:
    def __init__(
        self,
        http: HttpClient,
        credentials: CredentialBrokerPort,
        *,
        api_base: str = "https://open.feishu.cn",
    ) -> None:
        self._http = http
        self._credentials = credentials
        self._api_base = api_base.rstrip("/")

    def render(self, msg: OutboundMessage) -> tuple[str, str]:
        if msg.kind == "approval_card":
            if msg.card_payload is None:
                raise ValueError("approval_card requires card_payload")
            return "interactive", json.dumps(dict(msg.card_payload), ensure_ascii=False)
        text = f"{msg.title}\n{msg.body_md}" if msg.title else msg.body_md
        return "text", json.dumps({"text": text}, ensure_ascii=False)

    def send(self, msg: OutboundMessage) -> SendResult:
        token = self._credentials.issue_ephemeral("feishu")
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
        return _feishu_result(response)


def _feishu_result(response: HttpResponse) -> SendResult:
    data: Mapping[str, Any]
    try:
        data = cast(Mapping[str, Any], json.loads(response.body.decode("utf-8") or "{}"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        data = {}
    if response.status == 200 and data.get("code") == 0:
        inner: Any = data.get("data")
        message_id: Any = cast(Mapping[str, Any], inner).get("message_id") if isinstance(inner, Mapping) else None
        return SendResult(ok=True, platform_msg_id=None if message_id is None else str(message_id))
    retryable = response.status == 429 or response.status >= 500
    return SendResult(
        ok=False,
        error=f"feishu HTTP {response.status} code={data.get('code')} msg={data.get('msg')}",
        retryable=retryable,
    )
