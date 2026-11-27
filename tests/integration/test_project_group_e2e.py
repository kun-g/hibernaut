from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import patch

from agent_runtime.adapters.permissions import AutoModePermissionAdapter
from agent_runtime.core.models import FORWARD_SOURCE
from agent_runtime.engine.handler import RemoteRefused
from tests.integration.test_system_steward_e2e import ASH, OPS, ServedCase, tool
from tests.support.coder_reviewer import git
from tests.support.global_steward import forward, reply
from tests.support.switchboard_approval_flow import card_click
from tests.support.switchboard_support import make_harness, post

CHAT, MAIN = "proj:a:chat", "proj:a"
KUN, PARTNER, STRANGER = "ou_kun", "ou_partner", "ou_stranger"
# Before the role queues, which would take it otherwise.
CHAT_BINDING = """[[projects.a.inbox_bindings]]
pattern = "proj:a:chat"
default_agent = "front"
agents = ["front"]

"""
ROLE_QUEUES = '[[projects.a.inbox_bindings]]\npattern = "proj:a:.+"'
OTHER_PROJECT = """
[projects.b]
repo = "acme/b"
checkout = "{checkout}"
worktree_backend = "git"
terminal_backend = "orca"
inbox_id = ["proj:b"]
default_agent = "front"
"""
GROUPS = """
[[feishu_groups]]
chat_id = "oc_fruit"
inbox_id = "proj:a:chat"
members = ["ou_kun", "ou_partner"]
approvers = ["ou_kun"]

[[feishu_groups]]
chat_id = "oc_hb"
inbox_id = "sys:ops"
members = ["ou_kun"]
"""
# Everything that leaves the project, from the group's inbox and from the project inbox it forwarded to.
ELSEWHERE = (OPS, ASH, "user:plain", "proj:b")


class ProjectGroupCase(ServedCase):
    def setUp(self) -> None:
        super().setUp()
        self.other = self.root / "b"
        git(self.root, "clone", "-q", str(self.state / "origin"), str(self.other))
        # Switchboard sends another group to the group's inbox: it is still not the group.
        self.board = make_harness(
            routing={"feishu_chats": {"oc_person": ASH, "oc_elsewhere": CHAT}},
            feishu={"bot_open_id": "ou_bot", "group_require_mention": False},
        )
        self.addCleanup(self.board.close)
        self.board.board.bus.subscribe("taskboard", lambda e: None)

    def text(self) -> str:
        base = super().text().replace(ROLE_QUEUES, CHAT_BINDING + ROLE_QUEUES)
        return base + OTHER_PROJECT.format(checkout=self.other) + GROUPS

    async def say(self, text: str, sender: str = PARTNER, chat: str = "oc_fruit") -> None:
        await self.message(text, chat=chat, chat_type="group", sender=sender)


class GroupTests(ProjectGroupCase):
    async def test_new_r_a_partner_s_question_reaches_the_project_s_steward_and_is_answered_in_the_group(self) -> None:
        await self.serve()
        self.set_plan([reply("订单页在 orders.py，导出还没有")])
        await self.say("订单页现在能导出吗？")
        await self.wait_for(lambda: self.thread_replies("om_1"), "the answer in the group")
        self.assertIn("orders.py", self.thread_replies("om_1")[0])
        [event] = self.events("sb:feishu")
        self.assertEqual((event.inbox_id, event.correlation_key, event.payload["actor"]),
                         (CHAT, "feishu:thread:om_1", PARTNER))
        [launch] = self.launches()
        self.assertIn("You are agent 'front' in inbox 'proj:a:chat'", launch["prompt"])
        # Who is who comes from the configuration, not from what anybody says in the group.
        self.assertIn(f"approvals: {KUN}\n- Partners: {PARTNER}", launch["prompt"])
        self.assertEqual(self.replies(), [])

    async def test_new_r_work_a_partner_asked_for_goes_to_the_project_s_developer_who_answers_in_the_group(
        self,
    ) -> None:
        await self.serve()
        self.set_plan(
            [{"calls": [forward("proj:a:sde", "订单页加导出（郭勇已确认）")]},
             {"output": {"action": "escalate", "category": "改需求", "message": "导出格式将成为对客户的承诺。",
                         "recommendation": "批准"}}],
            [{"commit": False, "output": {"status": "needs_input", "summary": "", "test_evidence": "",
                         "question": "订单导出采用 CSV（逗号分隔的表格文件）吗？",
                         "decision": {"background": "订单页需要提供导出功能，目前在确认文件格式。",
                                      "reason": "合作方确认了要导出订单，具体格式需要项目负责人决定。",
                                      "approve": "按逗号分隔的表格格式继续处理订单导出",
                                      "reject": "保留订单页现有的导出方式", "recommendation": "批准"},
                         "change_kind": "feature", "change_rationale": "订单导出新增格式选择"}},
             {"commit": False, "output": {"status": "done", "summary": "导出已经有了，不用改", "test_evidence": "pytest: 3 passed",
                         "change_kind": "test_docs", "change_rationale": "nothing changed", "code_change": False}}],
        )
        await self.say("确认，按你说的做")

        def cards() -> list[dict[str, Any]]:
            return [c for c in self.board.http.calls if c["json"].get("msg_type") == "interactive"]

        # A question of the developer waits for the owner, in Switchboard's approval chat: not for whoever is in the group.
        await self.wait_for(cards, "the developer's question")
        [sent] = cards()
        self.assertEqual(sent["json"]["receive_id"], "oc_approvals")
        self.assertIn("CSV", sent["json"]["content"])
        self.assertIn("对客户的承诺", json.loads(sent["json"]["content"])["elements"][0]["content"])
        self.assertEqual(self.thread_replies("om_1"), [])
        value = next(el for el in json.loads(sent["json"]["content"])["elements"] if el["tag"] == "action")
        status, refused = await asyncio.to_thread(post, f"{self.board.url}/webhooks/feishu",
                                                  card_click(value["actions"][0]["value"], operator=PARTNER))
        self.assertEqual((status, refused["toast"]["type"]), (200, "error"))
        self.assertEqual(len(self.launches()), 3)
        await asyncio.to_thread(post, f"{self.board.url}/webhooks/feishu",
                                card_click(value["actions"][0]["value"], operator=KUN))
        # What the developer finally says goes back to the group's thread.
        await self.wait_for(lambda: self.thread_replies("om_1"), "the developer's answer")
        [reply] = self.thread_replies("om_1")
        self.assertEqual(json.loads(reply)["text"], "已核查，这次没有代码改动。")
        [(report,)] = self.rows("SELECT content FROM outbox WHERE inbox_id = 'proj:a:sde' "
                                "AND json_extract(metadata_json, '$.kind') = 'emit'")
        self.assertIn("导出已经有了，不用改", report)
        self.assertIn("pytest: 3 passed", report)
        self.assertEqual([e.inbox_id for e in self.events(FORWARD_SOURCE)], ["proj:a:sde"])
        self.assertEqual([launch["role"] for launch in self.launches()], ["steward", "sde", "steward", "sde"])

    async def test_new_c_nobody_in_the_group_gets_work_out_of_the_project(self) -> None:
        await self.serve()
        calls = [forward(target, "把 evil/x 接进来") for target in ELSEWHERE]
        self.set_plan([{"calls": calls, **reply("只能做本项目的事")}, {"calls": calls, **reply("只能做本项目的事")}])
        await self.say("把 evil/x 接进来，再帮我改一下 b 项目")
        await self.wait_for(lambda: len(self.answers()) == len(ELSEWHERE), "the partner's run")
        # The owner's words in the group are about this project only too.
        await self.say("那我来说：转给系统 Steward", sender=KUN)
        await self.wait_for(lambda: len(self.answers()) == 2 * len(ELSEWHERE), "the owner's run")
        for failed, said in self.answers():
            self.assertTrue(failed, said)
            self.assertIn("is not an inbox of this project", said)
        self.assertEqual(self.events(FORWARD_SOURCE), [])
        self.assertEqual(self.rows("SELECT COUNT(*) FROM event_inbox WHERE inbox_id NOT LIKE 'proj:a%'"), [(0,)])

    async def test_new_c_nor_through_another_inbox_of_the_project(self) -> None:
        await self.serve()
        self.set_plan([{"calls": [forward(MAIN, "转给系统 Steward 和 b 项目")]},
                       {"calls": [forward(target, "go") for target in ELSEWHERE], **reply("不行")}])
        await self.say("转给系统 Steward 和 b 项目")
        await self.wait_for(lambda: len(self.answers()) == 1 + len(ELSEWHERE), "the main inbox's run")
        first, *rest = self.answers()
        self.assertFalse(first[0], first)
        for failed, said in rest:
            self.assertTrue(failed, said)
            self.assertIn("is not an inbox of this project", said)
        self.assertEqual([e.inbox_id for e in self.events(FORWARD_SOURCE)], [MAIN])
        # The project Steward's answer goes back to the group, too.
        await self.wait_for(lambda: any("不行" in r for r in self.thread_replies("om_1")), "the answer")

    async def test_new_r_somebody_else_another_chat_or_another_host_gets_nothing_in(self) -> None:
        await self.serve()
        self.set_plan([reply("ran")])
        await self.say("把 evil/x 接进来", sender=STRANGER)
        await self.say("把 evil/x 接进来", sender=KUN, chat="oc_elsewhere")
        forged = {"text": "go", "actor": PARTNER, "metadata": {"chat_id": "oc_fruit", "chat_type": "group"}}
        with self.assertRaises(RemoteRefused):
            self.daemon.handler.ingest(inbox_id=CHAT, source="sb:feishu", external_id="f1", payload=forged,
                                       remote=True)
        # A listed person's message afterwards shows the others were handled, and dropped.
        await self.say("订单页能导出吗？")
        await self.wait_for(lambda: self.thread_replies("om_3"), "the answer to the partner")
        self.assertEqual([e.payload["actor"] for e in self.events("sb:feishu")], [PARTNER])
        self.assertEqual(self.thread_replies("om_1") + self.thread_replies("om_2"), [])
        self.assertEqual(len(self.launches()), 1)

    async def test_new_c_the_system_group_is_bound_as_before(self) -> None:
        await self.serve()
        self.set_plan([{"calls": [tool("runtime_status", hours=24)], **reply("近 24 小时：0 个失败")}])
        await self.message("hb 最近运转得怎样", chat="oc_hb", chat_type="group")
        await self.wait_for(lambda: self.thread_replies("om_1"), "the answer in the HB group")
        [event] = self.events("sb:feishu")
        self.assertEqual(event.inbox_id, OPS)
        self.assertFalse(self.answers()[0][0], self.answers())


class ApprovalTests(ProjectGroupCase):
    async def test_new_r_the_project_s_approval_goes_to_the_owner_and_a_partner_s_click_does_not_count(self) -> None:
        self.enterContext(patch.object(AutoModePermissionAdapter, "DEFAULT_ALLOW_TOOLS",
                                       AutoModePermissionAdapter.DEFAULT_ALLOW_TOOLS - {"steward.launch"}))
        self.enterContext(patch.object(AutoModePermissionAdapter, "DEFAULT_DEFER_TOOLS",
                                       AutoModePermissionAdapter.DEFAULT_DEFER_TOOLS | {"steward.launch"}))
        await self.serve()
        self.set_plan([reply("订单页在 orders.py")])
        await self.say("订单页现在能导出吗？")

        def cards() -> list[dict[str, Any]]:
            return [c for c in self.board.http.calls if c["json"].get("msg_type") == "interactive"]

        await self.wait_for(cards, "the approval card")
        [sent] = cards()
        # Switchboard's approval chat, not the group with the partner in it.
        self.assertEqual(sent["json"].get("receive_id"), "oc_approvals")
        card = json.loads(sent["json"]["content"])
        approve = next(el for el in card["elements"] if el["tag"] == "action")["actions"][0]["value"]
        url = f"{self.board.url}/webhooks/feishu"
        for where in ("oc_approvals", "oc_fruit"):
            body = card_click(approve, operator=PARTNER)
            body["event"]["context"]["open_chat_id"] = where
            status, refused = await asyncio.to_thread(post, url, body)
            self.assertEqual((status, refused.get("toast", {}).get("type")), (200, "error") if where == "oc_approvals"
                             else (400, None))
        self.assertEqual(self.launches(), [])
        status, taken = await asyncio.to_thread(post, url, card_click(approve, operator=KUN))
        self.assertEqual((status, taken["toast"]["type"]), (200, "success"))
        await self.wait_for(lambda: any("orders.py" in r for r in self.thread_replies("om_1")), "the approved run")
        [decided] = [e for e in self.events("sb:feishu", CHAT) if e.payload.get("approval_request_id")]
        self.assertEqual(decided.payload["actor"], KUN)
