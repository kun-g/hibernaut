from __future__ import annotations

from typing import Any

_DECISION_LABELS = {"approve": "已批准", "approved": "已批准", "reject": "已拒绝", "rejected": "已拒绝"}


def approval_button_value(
    *,
    approval_request_id: str,
    decision: str,
    correlation_key: str,
    scope: str,
    session_id: str,
) -> dict[str, str]:
    return {
        "approval_request_id": approval_request_id,
        "decision": decision,
        "correlation_key": correlation_key,
        "scope": scope,
        "session_id": session_id,
    }


def render_approval_card(
    *,
    title: str,
    body_md: str,
    approval_request_id: str,
    correlation_key: str,
    scope: str,
    session_id: str,
) -> dict[str, Any]:
    def button(label: str, decision: str, style: str) -> dict[str, Any]:
        return {
            "tag": "button",
            "text": {"tag": "plain_text", "content": label},
            "type": style,
            "value": approval_button_value(
                approval_request_id=approval_request_id,
                decision=decision,
                correlation_key=correlation_key,
                scope=scope,
                session_id=session_id,
            ),
        }

    return {
        "config": {"wide_screen_mode": True},
        "header": {"template": "orange", "title": {"tag": "plain_text", "content": title}},
        "elements": [
            {"tag": "markdown", "content": body_md},
            {
                "tag": "action",
                "actions": [
                    button("批准", "approve", "primary"),
                    button("拒绝", "reject", "danger"),
                ],
            },
        ],
    }


def render_resolved_card(
    *,
    approval_request_id: str,
    decision: str,
    actor: str | None,
    correlation_key: str | None,
) -> dict[str, Any]:
    label = _DECISION_LABELS.get(decision, decision)
    approved = decision in ("approve", "approved")
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "green" if approved else "red",
            "title": {"tag": "plain_text", "content": f"审批{label}"},
        },
        "elements": [
            {
                "tag": "markdown",
                "content": f"**{label}**（{actor or 'unknown'}）\n审批单 `{approval_request_id}`",
            },
            {"tag": "note", "elements": [{"tag": "plain_text", "content": correlation_key or "-"}]},
        ],
    }
