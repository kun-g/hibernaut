from __future__ import annotations

from typing import Any

from tests.support.switchboard_support import FEISHU_TOKEN


def card_click(value: dict[str, Any], operator: str = "ou_kun") -> dict[str, Any]:
    return {
        "schema": "2.0",
        "header": {"event_type": "card.action.trigger", "token": FEISHU_TOKEN},
        "event": {
            "operator": {"open_id": operator},
            "action": {"tag": "button", "value": value},
            "context": {"open_message_id": "om_out", "open_chat_id": "oc_approvals"},
        },
    }
