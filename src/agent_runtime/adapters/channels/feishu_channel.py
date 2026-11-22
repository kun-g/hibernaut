from __future__ import annotations

import asyncio
import json
import os
import time
import urllib.request
from collections.abc import Callable, Mapping
from typing import Any

from agent_runtime.adapters.channels.channels import ConsoleChannelAdapter
from agent_runtime.core.ports import ChannelPort

HttpRequester = Callable[[str, dict[str, str], dict[str, Any]], dict[str, Any]]

_TOKEN_URL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
_SEND_MSG_URL = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"


def _default_http_post(
    url: str,
    headers: dict[str, str],
    payload: dict[str, Any],
) -> dict[str, Any]:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url=url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json; charset=utf-8", **headers},
    )
    with urllib.request.urlopen(req, timeout=15.0) as resp:  # noqa: S310
        return json.loads(resp.read().decode("utf-8"))


class FeishuChannelAdapter:
    def __init__(
        self,
        *,
        app_id: str,
        app_secret: str,
        default_chat_id: str | None = None,
        inbox_chat_map: Mapping[str, str] | None = None,
        http_post: HttpRequester | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._app_id = app_id
        self._app_secret = app_secret
        self._default_chat_id = default_chat_id
        # Kept as given: a configured map also answers inboxes matched by a project's pattern.
        self._inbox_chat_map: Mapping[str, str] = inbox_chat_map if inbox_chat_map is not None else {}
        self._http_post: HttpRequester = http_post or _default_http_post
        self._clock = clock
        self._cached_token: str | None = None
        self._token_expires_at: float = 0.0

    def _get_tenant_access_token(self) -> str:
        now = self._clock()
        if self._cached_token is not None and now < self._token_expires_at:
            return self._cached_token
        resp = self._http_post(
            _TOKEN_URL,
            {},
            {"app_id": self._app_id, "app_secret": self._app_secret},
        )
        token = str(resp.get("tenant_access_token") or "")
        if not token:
            raise RuntimeError(f"Failed to fetch Feishu tenant_access_token: {resp}")
        expire = float(resp.get("expire", 7200))
        self._cached_token = token
        self._token_expires_at = now + max(expire - 60.0, 30.0)
        return token

    def _resolve_chat_id(self, inbox_id: str) -> str:
        chat_id = self._inbox_chat_map.get(inbox_id)
        if chat_id is not None:
            return chat_id
        if inbox_id.startswith("feishu:"):
            return inbox_id.removeprefix("feishu:")
        if self._default_chat_id:
            return self._default_chat_id
        raise ValueError(f"No Feishu chat_id mapped for inbox_id '{inbox_id}'")

    def _build_approval_card(
        self,
        *,
        inbox_id: str,
        content: str,
        request_id: str,
    ) -> dict[str, Any]:
        return {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": "Hibernaut 审批确认"},
                "template": "orange",
            },
            "elements": [
                {"tag": "div", "text": {"tag": "lark_md", "content": content}},
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "批准执行"},
                            "type": "primary",
                            "value": {
                                "approval_request_id": request_id,
                                "approved": True,
                                "inbox_id": inbox_id,
                            },
                        },
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "拒绝"},
                            "type": "danger",
                            "value": {
                                "approval_request_id": request_id,
                                "approved": False,
                                "inbox_id": inbox_id,
                            },
                        },
                    ],
                },
            ],
        }

    def _send_sync(
        self,
        inbox_id: str,
        content: str,
        metadata: dict[str, Any],
    ) -> None:
        token = self._get_tenant_access_token()
        chat_id = self._resolve_chat_id(inbox_id)

        if metadata.get("kind") == "approval_request" and metadata.get("request_id"):
            card = self._build_approval_card(
                inbox_id=inbox_id,
                content=content,
                request_id=str(metadata["request_id"]),
            )
            body: dict[str, Any] = {
                "receive_id": chat_id,
                "msg_type": "interactive",
                "content": json.dumps(card, ensure_ascii=False),
            }
        else:
            body = {
                "receive_id": chat_id,
                "msg_type": "text",
                "content": json.dumps({"text": content}, ensure_ascii=False),
            }

        resp = self._http_post(
            _SEND_MSG_URL,
            {"Authorization": f"Bearer {token}"},
            body,
        )
        code = resp.get("code", 0)
        if code not in (0, None):
            raise RuntimeError(f"Feishu send message failed (code={code}): {resp}")

    async def send(
        self,
        inbox_id: str,
        content: str,
        metadata: dict[str, Any],
    ) -> None:
        await asyncio.to_thread(self._send_sync, inbox_id, content, metadata)


def build_feishu_or_console_channel(
    *,
    app_id_env: str = "FEISHU_APP_ID",
    app_secret_env: str = "FEISHU_APP_SECRET",
    default_chat_id: str | None = None,
    inbox_chat_map: Mapping[str, str] | None = None,
) -> ChannelPort:
    app_id = os.environ.get(app_id_env, "").strip()
    app_secret = os.environ.get(app_secret_env, "").strip()
    if not app_id or not app_secret:
        return ConsoleChannelAdapter()
    return FeishuChannelAdapter(
        app_id=app_id,
        app_secret=app_secret,
        default_chat_id=default_chat_id,
        inbox_chat_map=inbox_chat_map,
    )
