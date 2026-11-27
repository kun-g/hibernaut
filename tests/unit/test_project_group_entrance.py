from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any

from agent_runtime.adapters.channels.ingress import CardRoute, FeishuGroup, GroupEntrances, event_origin_for
from agent_runtime.adapters.channels.switchboard_bridge import (
    SwitchboardChannelAdapter,
    SwitchboardInboundBridge,
    canonical_to_payload,
)
from agent_runtime.adapters.executor_cli.role_skills import builtin_skill
from agent_runtime.adapters.executor_cli.steward import GroupRoster
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import group_entrances
from agent_runtime.core.models import Event
from agent_runtime.engine.handler import REMOTE_KEY
from switchboard.envelope import CanonicalEvent

ASH, OPS = "user:main", "sys:ops"
CHAT, MAIN, SDE, OTHER = "proj:fruit:chat", "proj:fruit", "proj:fruit:sde", "proj:other"
GROUP, HB_GROUP, STRANGE_GROUP = "oc_fruit", "oc_hb", "oc_strange"
KUN, PARTNER, STRANGER = "ou_kun", "ou_partner", "ou_stranger"
FRUIT = FeishuGroup(chat_id=GROUP, inbox_id=CHAT, members=frozenset({KUN, PARTNER}), project="fruit",
                    approvers=frozenset({KUN}))
HB = FeishuGroup(chat_id=HB_GROUP, inbox_id=OPS, members=frozenset({KUN}))
PROJECTS = {CHAT: "fruit", MAIN: "fruit", SDE: "fruit", OTHER: "other"}
ENTRANCES = GroupEntrances([FRUIT, HB], projects_of=lambda inbox: [PROJECTS[inbox]] if inbox in PROJECTS else [])


def message(*, chat_id: str = GROUP, chat_type: str = "group", actor: str = PARTNER, scope: str = ASH) -> CanonicalEvent:
    return CanonicalEvent(
        source="feishu", event_type="conversation.message", external_id="msg:om_1", scope=scope,
        correlation_key="feishu:thread:om_1", actor=actor, body="订单页加个导出",
        metadata={"channel": "feishu", "chat_id": chat_id, "chat_type": chat_type, "message_id": "om_1"},
    )


def decision(*, actor: str, scope: str, chat_id: str = "oc_kun_p2p") -> CanonicalEvent:
    return CanonicalEvent(
        source="feishu", event_type="approval.decided", external_id="approval:req_1", scope=scope,
        correlation_key="feishu:thread:om_1", actor=actor, body="approve",
        metadata={"approval_request_id": "req_1", "decision": "approve", "session_id": "s1", "channel": "feishu",
                  "chat_id": chat_id},
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
        for name in ("work", "ops", "fruit", "other"):
            (self.root / name).mkdir()

    def raw(self, *groups: dict[str, Any], chat_agents: list[str] | None = None) -> dict[str, Any]:
        def project(name: str, bindings: list[dict[str, Any]]) -> dict[str, Any]:
            return {"repo": f"o/{name}", "checkout": str(self.root / name), "worktree_backend": "git",
                    "inbox_id": [f"proj:{name}", f"proj:{name}:.+"], "default_agent": "front",
                    "inbox_bindings": bindings}

        return {
            "runtime": {"inbox_bindings": [
                {"pattern": ASH, "default_agent": "ash", "agents": ["ash"], "workdir": str(self.root / "work"),
                 "checkout_access": "readonly", "forward_to": [MAIN, OPS]},
                {"pattern": OPS, "default_agent": "ops", "agents": ["ops"], "workdir": str(self.root / "ops"),
                 "checkout_access": "readonly", "system_tools": True},
            ]},
            "executors": {"claude-code": {"harness": "claude-code"}},
            "agents": {**{name: {"role": "steward", "executor": "claude-code"} for name in ("ash", "ops", "front")},
                       "sde": {"role": "sde", "executor": "claude-code", "review": False}},
            "projects": {
                "fruit": project("fruit", [
                    {"pattern": MAIN, "default_agent": "front", "agents": ["front"]},
                    # Before the role queues, which would take it otherwise.
                    {"pattern": CHAT, "default_agent": (chat_agents or ["front"])[0],
                     "agents": chat_agents or ["front"]},
                    {"pattern": "proj:fruit:.+", "default_agent": "sde", "agents": ["sde"]},
                ]),
                "other": project("other", [{"pattern": OTHER, "default_agent": "front", "agents": ["front"]}]),
            },
            "feishu_groups": list(groups),
        }

    def fruit(self, **changes: Any) -> dict[str, Any]:
        return {"chat_id": GROUP, "inbox_id": CHAT, "members": [KUN, PARTNER], "approvers": [KUN], **changes}

    def test_new_r_a_group_is_bound_to_an_inbox_of_one_project_with_its_partners_and_who_approves(self) -> None:
        config = parse_config(self.raw(self.fruit(), {"chat_id": HB_GROUP, "inbox_id": OPS, "members": [KUN]}))
        self.assertEqual(config.feishu_groups, (FRUIT, HB))
        # The project's main inbox may take the group too.
        [main] = parse_config(self.raw(self.fruit(inbox_id=MAIN))).feishu_groups
        self.assertEqual(main.project, "fruit")

    def test_new_c_a_project_group_reaches_a_steward_of_that_project_and_names_who_approves(self) -> None:
        for group, why in (
            # No project, and not a system inbox: its forward_to reaches other projects and the system inbox.
            (self.fruit(inbox_id=ASH), "system_tools, or an inbox of a project"),
            (self.fruit(inbox_id="proj:nobody"), "an inbox of a project"),
            # Covered by the role queues: an SDE, with a shell, would take a partner's words directly.
            (self.fruit(inbox_id=SDE), "all have role = \"steward\""),
            (self.fruit(inbox_id="proj:fruit:../x"), "inbox_id"),
            (self.fruit(approvers=[]), "approvers"),
            (self.fruit(approvers=None), "approvers"),
            (self.fruit(approvers=[""]), "approvers"),
            (self.fruit(members=[]), "members"),
            ({"chat_id": HB_GROUP, "inbox_id": OPS, "members": [KUN], "approvers": [KUN]}, "only for a project group"),
        ):
            group = {k: v for k, v in group.items() if v is not None}
            with self.subTest(group=group), self.assertRaisesRegex(ValueError, why):
                parse_config(self.raw(group))
        with self.assertRaisesRegex(ValueError, "all have role"):
            parse_config(self.raw(self.fruit(), chat_agents=["front", "sde"]))
        with self.assertRaisesRegex(ValueError, "one group"):
            parse_config(self.raw(self.fruit(), self.fruit(chat_id=STRANGE_GROUP)))

    def test_new_c_the_entrances_know_which_inboxes_belong_to_the_group_s_project(self) -> None:
        entrances = group_entrances(parse_config(self.raw(self.fruit())))
        for inbox in (CHAT, MAIN, SDE, "proj:fruit:sde:issue-1"):
            self.assertEqual(entrances.card_group(inbox), CardRoute(None, frozenset({KUN})), inbox)
        for inbox in (OTHER, ASH, OPS):
            self.assertIsNone(entrances.card_group(inbox), inbox)


class BridgeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.handler = RecordingHandler()
        self.bridge = SwitchboardInboundBridge(self.handler, groups=ENTRANCES)

    def inboxes(self) -> list[str]:
        return [call["inbox_id"] for call in self.handler.ingested]

    def test_new_r_a_listed_partner_in_the_bound_group_reaches_the_project_group_inbox(self) -> None:
        for actor in (PARTNER, KUN):
            for scope in (ASH, CHAT, OPS, OTHER):
                self.assertEqual(self.bridge.handle_canonical_event(message(actor=actor, scope=scope)), "acked")
        self.assertEqual(self.inboxes(), [CHAT] * 8)
        # The answer goes back to the thread the message started.
        self.assertEqual(self.handler.ingested[0]["correlation_key"], "feishu:thread:om_1")
        self.assertEqual(self.handler.ingested[0]["payload"]["actor"], PARTNER)

    def test_new_r_somebody_else_another_chat_or_a_one_to_one_chat_reaches_nothing_there(self) -> None:
        for event in (
            message(actor=STRANGER), message(actor=""),
            # Switchboard routing another chat to the group's inbox does not make it the group.
            message(chat_id=STRANGE_GROUP, scope=CHAT), message(chat_id=GROUP, chat_type="p2p", scope=CHAT),
            message(chat_id="oc_person", chat_type="p2p", actor=KUN, scope=CHAT),
        ):
            with self.subTest(event=event):
                self.assertEqual(self.bridge.handle_canonical_event(event), "acked")
        self.assertEqual(self.handler.ingested, [])

    def test_new_c_other_chats_and_the_system_group_go_where_they_went_before(self) -> None:
        self.bridge.handle_canonical_event(message(chat_id=STRANGE_GROUP))
        self.bridge.handle_canonical_event(message(chat_id=STRANGE_GROUP, scope=MAIN))
        self.bridge.handle_canonical_event(message(chat_id=HB_GROUP, actor=KUN))
        self.bridge.handle_canonical_event(message(chat_id=HB_GROUP, actor=PARTNER))
        self.assertEqual(self.inboxes(), [ASH, MAIN, OPS])

    def test_new_r_approvals_of_the_project_count_from_its_approvers_only(self) -> None:
        for scope in (CHAT, MAIN, SDE):
            for chat_id in ("oc_kun_p2p", GROUP):
                self.bridge.handle_canonical_event(decision(actor=PARTNER, scope=scope, chat_id=chat_id))
        self.assertEqual(self.handler.ingested, [])
        for scope in (CHAT, MAIN, SDE):
            self.bridge.handle_canonical_event(decision(actor=KUN, scope=scope))
        # Another project's approvals and the system group's are decided as before.
        self.bridge.handle_canonical_event(decision(actor=STRANGER, scope=OTHER))
        self.bridge.handle_canonical_event(decision(actor=KUN, scope=OPS, chat_id=HB_GROUP))
        self.bridge.handle_canonical_event(decision(actor=KUN, scope=OPS))
        self.assertEqual(self.inboxes(), [CHAT, MAIN, SDE, OTHER, OPS])


class CardTests(unittest.TestCase):
    def report(self, inbox_id: str, outcome: str = "suspend") -> dict[str, Any]:
        adapter = SwitchboardChannelAdapter(lambda _e: None, card_group=ENTRANCES.card_group)
        metadata = {"outcome_kind": outcome, "session_id": "s1", "approval_request_id": "req_1",
                    "correlation_key": "feishu:thread:om_1"}
        report = adapter.build_report(inbox_id, "Spend?", metadata)
        assert report is not None
        return dict(report.metadata)

    def test_new_r_a_card_of_the_project_goes_to_switchboard_s_approval_chat_for_its_approvers(self) -> None:
        for inbox in (CHAT, MAIN, SDE):
            card = self.report(inbox)
            # No target: never the group, where a partner would see it.
            self.assertEqual(card["deciders"], [KUN])
            self.assertFalse({"approval_channel", "approval_target"} & set(card), card)
        self.assertEqual(self.report(OPS)["approval_target"], HB_GROUP)
        self.assertNotIn("deciders", self.report(OTHER))
        self.assertNotIn("deciders", self.report(CHAT, outcome="emit"))


class OriginTests(unittest.TestCase):
    origin = staticmethod(event_origin_for(ENTRANCES))

    @staticmethod
    def event(inbox: str = CHAT, actor: str = PARTNER, chat_id: str = GROUP, **extra: Any) -> Event:
        payload = {**canonical_to_payload(message(scope=inbox, actor=actor, chat_id=chat_id)), **extra}
        return Event(inbox_id=inbox, source="sb:feishu", external_id="1", payload=payload)

    def test_new_c_a_project_group_s_words_are_no_person_s_own_request_to_a_system_inbox(self) -> None:
        for event in (self.event(), self.event(actor=KUN), self.event(**{REMOTE_KEY: True})):
            self.assertIsNone(self.origin(event))
        self.assertEqual(self.origin(self.event(inbox=OPS, actor=KUN, chat_id=HB_GROUP)), "group")


class RosterTests(unittest.TestCase):
    def test_new_r_the_project_steward_is_told_who_is_who_and_what_stays_in_the_project(self) -> None:
        said = GroupRoster(chat_id=GROUP, owners=frozenset({KUN}), partners=frozenset({PARTNER})).prompt()
        for part in (GROUP, f"approvals: {KUN}", f"Partners: {PARTNER}", "`actor`", "about this project only",
                     "another project, a system inbox, the general Steward", "wait for them to confirm"):
            self.assertIn(part, said)

    def test_new_r_its_skill_says_whom_to_answer_and_to_confirm_a_partner_s_work_first(self) -> None:
        skill = builtin_skill("steward").read_text()
        section = skill[skill.index("# A Feishu group"):skill.index("# Memory")]
        for part in ("`actor`", "quote the line you answer", "confirm in the same thread",
                     "Forward it only after they confirm", "refused whoever asks", "not decided in the group"):
            self.assertIn(part, section)


if __name__ == "__main__":
    unittest.main()
