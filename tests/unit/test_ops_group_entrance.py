from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any

from agent_runtime.adapters.channels.ingress import FeishuGroup, GroupEntrances, event_origin, event_origin_for
from agent_runtime.adapters.channels.switchboard_bridge import SwitchboardInboundBridge, canonical_to_payload
from agent_runtime.app.config import parse_config
from agent_runtime.core.models import Event
from agent_runtime.engine.handler import REMOTE_KEY
from switchboard.envelope import CanonicalEvent

ASH, OPS = "user:main", "sys:ops"
GROUP, OTHER_GROUP = "oc_hb", "oc_other"
KUN, STRANGER = "ou_kun", "ou_stranger"
ENTRANCES = GroupEntrances([FeishuGroup(chat_id=GROUP, inbox_id=OPS, members=frozenset({KUN}))])


def message(*, chat_id: str = GROUP, chat_type: str = "group", actor: str = KUN, scope: str = ASH) -> CanonicalEvent:
    return CanonicalEvent(
        source="feishu", event_type="conversation.message", external_id="msg:om_1", scope=scope,
        correlation_key="feishu:thread:om_1", actor=actor, body="hb 最近运转得怎样",
        metadata={"channel": "feishu", "chat_id": chat_id, "chat_type": chat_type, "message_id": "om_1"},
    )


def decision(*, actor: str, scope: str = OPS) -> CanonicalEvent:
    return CanonicalEvent(
        source="feishu", event_type="approval.decided", external_id="approval:req_1", scope=scope,
        correlation_key="feishu:thread:om_1", actor=actor, body="approve",
        metadata={"approval_request_id": "req_1", "decision": "approve", "session_id": "s1", "channel": "feishu"},
    )


class RecordingHandler:
    def __init__(self) -> None:
        self.ingested: list[dict[str, Any]] = []

    def ingest(self, **kwargs: Any) -> object:
        self.ingested.append(kwargs)
        return None


class ConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()

    def raw(self, *groups: dict[str, Any]) -> dict[str, Any]:
        return {
            "runtime": {"inbox_bindings": [
                {"pattern": ASH, "default_agent": "ash", "agents": ["ash"], "workdir": str(self.root),
                 "checkout_access": "readonly", "forward_to": [OPS]},
                {"pattern": OPS, "default_agent": "ops", "agents": ["ops"], "workdir": str(self.root),
                 "checkout_access": "readonly", "system_tools": True},
            ]},
            "executors": {"claude-code": {"harness": "claude-code"}},
            "agents": {"ash": {"role": "steward", "executor": "claude-code"},
                       "ops": {"role": "steward", "executor": "claude-code"}},
            "feishu_groups": list(groups),
        }

    def test_r9_a_group_is_bound_to_a_system_inbox_with_the_people_who_count(self) -> None:
        config = parse_config(self.raw({"chat_id": GROUP, "inbox_id": OPS, "members": [KUN]}))
        self.assertEqual(config.feishu_groups, (FeishuGroup(chat_id=GROUP, inbox_id=OPS, members=frozenset({KUN})),))
        self.assertEqual(parse_config(self.raw()).feishu_groups, ())

    def test_c6_a_group_reaches_nothing_but_a_system_inbox_and_names_its_people(self) -> None:
        for group, why in (
            ({"chat_id": GROUP, "inbox_id": ASH, "members": [KUN]}, "system_tools"),
            ({"chat_id": GROUP, "inbox_id": "sys:other", "members": [KUN]}, "system_tools"),
            ({"chat_id": GROUP, "inbox_id": OPS, "members": []}, "members"),
            ({"chat_id": GROUP, "inbox_id": OPS, "members": [""]}, "members"),
            ({"chat_id": GROUP, "inbox_id": OPS, "members": "ou_kun"}, "members"),
            ({"chat_id": GROUP, "inbox_id": OPS}, "members"),
            ({"chat_id": "", "inbox_id": OPS, "members": [KUN]}, "chat_id"),
            ({"chat_id": GROUP, "inbox_id": OPS, "members": [KUN], "everyone": True}, "known keys"),
        ):
            with self.subTest(group=group), self.assertRaisesRegex(ValueError, why):
                parse_config(self.raw(group))
        with self.assertRaisesRegex(ValueError, "chat_id"):
            parse_config(self.raw({"chat_id": GROUP, "inbox_id": OPS, "members": [KUN]},
                                  {"chat_id": GROUP, "inbox_id": OPS, "members": [STRANGER]}))


class BridgeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.handler = RecordingHandler()
        self.bridge = SwitchboardInboundBridge(self.handler, groups=ENTRANCES)

    def inboxes(self) -> list[str]:
        return [call["inbox_id"] for call in self.handler.ingested]

    def test_r9_a_listed_person_in_the_bound_group_reaches_its_inbox_wherever_switchboard_put_it(self) -> None:
        for scope in (ASH, OPS, "proj:x"):
            self.assertEqual(self.bridge.handle_canonical_event(message(scope=scope)), "acked")
        self.assertEqual(self.inboxes(), [OPS, OPS, OPS])
        # Answers go back to the thread the message started.
        self.assertEqual(self.handler.ingested[0]["correlation_key"], "feishu:thread:om_1")
        self.assertEqual(self.handler.ingested[0]["payload"]["actor"], KUN)

    def test_new_r_somebody_else_in_the_bound_group_reaches_nothing(self) -> None:
        for scope in (ASH, OPS):
            self.assertEqual(self.bridge.handle_canonical_event(message(actor=STRANGER, scope=scope)), "acked")
            self.assertEqual(self.bridge.handle_canonical_event(message(actor="", scope=scope)), "acked")
        self.assertEqual(self.handler.ingested, [])

    def test_c6_other_chats_go_where_they_went_before(self) -> None:
        self.bridge.handle_canonical_event(message(chat_id=OTHER_GROUP))
        self.bridge.handle_canonical_event(message(chat_id="oc_person", chat_type="p2p", actor=STRANGER))
        self.bridge.handle_canonical_event(message(chat_id=OTHER_GROUP, scope=OPS))
        self.assertEqual(self.inboxes(), [ASH, ASH, OPS])
        # A one-to-one chat whose id is listed as the group is still no group.
        self.bridge.handle_canonical_event(message(chat_type="p2p", actor=STRANGER))
        self.assertEqual(self.inboxes()[-1], ASH)

    def test_new_r_only_a_listed_person_decides_on_a_card_of_the_group_s_inbox(self) -> None:
        self.bridge.handle_canonical_event(decision(actor=STRANGER))
        self.assertEqual(self.handler.ingested, [])
        self.bridge.handle_canonical_event(decision(actor=KUN))
        self.bridge.handle_canonical_event(decision(actor=STRANGER, scope=ASH))
        self.assertEqual(self.inboxes(), [OPS, ASH])

    def test_without_groups_nothing_changes(self) -> None:
        bridge = SwitchboardInboundBridge(self.handler)
        bridge.handle_canonical_event(message(actor=STRANGER))
        self.assertEqual(self.inboxes(), [ASH])


class OriginTests(unittest.TestCase):
    origin = staticmethod(event_origin_for(ENTRANCES))

    @staticmethod
    def event(inbox: str = OPS, source: str = "sb:feishu", **changes: Any) -> Event:
        payload = canonical_to_payload(message(scope=inbox))
        for key, value in changes.items():
            if key in ("chat_id", "chat_type"):
                payload["metadata"][key] = value
            else:
                payload[key] = value
        return Event(inbox_id=inbox, source=source, external_id="1", payload=payload)

    def test_c6_a_listed_person_in_the_group_bound_to_this_inbox_is_a_person(self) -> None:
        self.assertEqual(self.origin(self.event()), "group")
        self.assertEqual(self.origin(self.event(chat_type="p2p")), "chat")
        self.assertEqual(self.origin(self.event(source="cli")), "operator")

    def test_c6_nothing_else_from_a_group_is(self) -> None:
        for event in (
            self.event(actor=STRANGER), self.event(actor=None), self.event(chat_id=OTHER_GROUP),
            self.event(inbox=ASH), self.event(inbox="sys:other"), self.event(source="sb:github"),
            self.event(**{REMOTE_KEY: True}), self.event(approval_request_id="req_1"),
        ):
            with self.subTest(event=event):
                self.assertIsNone(self.origin(event))
        # The deployment without groups tells no group message from anybody else's.
        self.assertIsNone(event_origin(self.event()))


if __name__ == "__main__":
    unittest.main()
