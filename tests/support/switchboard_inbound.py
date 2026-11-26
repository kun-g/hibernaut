from __future__ import annotations

import json
from typing import Any

from tests.support.switchboard_support import FEISHU_TOKEN


def feishu_message(
    *,
    chat_type: str = "p2p",
    chat_id: str = "oc_ops",
    message_id: str = "om_1",
    root_id: str = "",
    mentions: list[dict[str, Any]] | None = None,
    sender_type: str = "user",
    token: str = FEISHU_TOKEN,
) -> dict[str, Any]:
    return {
        "schema": "2.0",
        "header": {"event_type": "im.message.receive_v1", "token": token, "event_id": "fe_1"},
        "event": {
            "sender": {"sender_type": sender_type, "sender_id": {"open_id": "ou_kun"}},
            "message": {
                "message_id": message_id,
                "root_id": root_id,
                "chat_id": chat_id,
                "chat_type": chat_type,
                "message_type": "text",
                "create_time": "1795320000000",
                "content": json.dumps({"text": "@_user_1 deploy please"}),
                "mentions": mentions or [],
            },
        },
    }
