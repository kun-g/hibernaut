from __future__ import annotations

from collaboration.models import NotificationDecision, NotificationKind

_FEISHU_URGENT_KINDS: frozenset[NotificationKind] = frozenset(
    {"approval_card", "blocker_alert"}
)

_HUB_PULL_KINDS: frozenset[NotificationKind] = frozenset(
    {"daily_progress", "digest_ready", "proposal_queued", "closeout_synced"}
)


class NotificationPolicyEngine:
    def route(
        self,
        *,
        kind: NotificationKind,
        correlation_key: str = "",
        is_urgent_blocker: bool = False,
    ) -> NotificationDecision:
        if kind in _FEISHU_URGENT_KINDS:
            return NotificationDecision(
                kind=kind,
                channel="feishu_push",
                reason="D8-B: Feishu push strictly reserved for urgent approval cards and real blockers",
                correlation_key=correlation_key,
            )
        if is_urgent_blocker:
            return NotificationDecision(
                kind="blocker_alert",
                channel="feishu_push",
                reason="D8-B: escalated to Feishu push because is_urgent_blocker=True",
                correlation_key=correlation_key,
            )
        if kind in _HUB_PULL_KINDS:
            return NotificationDecision(
                kind=kind,
                channel="hub_pull",
                reason="D8-B: non-urgent updates live on hub.guokun.ai for on-demand pull",
                correlation_key=correlation_key,
            )
        return NotificationDecision(
            kind=kind,
            channel="silent",
            reason="D4-B/D8-B: routine completions stay silent by default",
            correlation_key=correlation_key,
        )
