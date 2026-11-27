from agent_runtime.adapters.channels.switchboard_bridge import SwitchboardChannelAdapter
from agent_runtime.core.models import TASK_CONTEXT_KEY, DeliveryOutcome
from switchboard.outbound.gateway import OutboundGateway
from switchboard.store_sqlite import SwitchboardStore
from tests.support.inbox_forwarding import MAIN, SDE
from tests.unit.test_cross_project_forwarding import ASH, CrossCase


PR = "https://github.com/example/repo/pull/42"
REPORT = f"Workdir: /tmp/task\nOrca terminal: term-1\nPR: {PR}\nTest evidence:\n" + "test passed\n" * 350


class ChatDeliveryTests(CrossCase):
    def setUp(self):
        super().setUp()
        store = SwitchboardStore(":memory:")
        self.addCleanup(store.close)
        self.gateway = OutboundGateway(store, {})
        self.bridge = SwitchboardChannelAdapter(lambda event: None)

    def outgoing(self, content, metadata):
        report = self.bridge.build_report(SDE, content, metadata)
        self.assertEqual(report.body, content)  # Taskboard consumes this same event.
        return self.gateway.message_for_report(report)

    async def test_two_forwards_one_short_reply_full_report_in_outbox(self):
        self.forward("steward", SDE, "实现优化")
        self.answer("sde", REPORT, delivery=DeliveryOutcome("merged", pr_url=PR))
        await self.dispatch({"text": "安排优化"})
        await self.run_inbox(MAIN)
        await self.run_inbox(SDE)
        [row] = self.outbox.fetch_pending(limit=100)
        self.assertEqual(row.content, REPORT)
        message = self.outgoing(row.content, row.metadata)
        self.assertEqual(message.target, {"chat_id": "oc_kun"})
        self.assertEqual(message.title, "")
        self.assertIn("已合并", message.body_md)
        self.assertIn(PR, message.body_md)
        self.assertLess(len(message.body_md), 100)
        for field in ("Workdir", "Orca terminal", "Test evidence"):
            self.assertNotIn(field, message.body_md)

    async def test_delivery_report_relayed_by_both_stewards_has_one_short_reply(self):
        await self.dispatch({"text": "安排优化", "return_to": ASH})
        self.forward("steward", SDE, "实现优化", return_to=MAIN)
        await self.run_inbox(MAIN)
        self.answer("sde", REPORT, delivery=DeliveryOutcome("merged", pr_url=PR))
        await self.run_inbox(SDE)
        [result] = self.queued(MAIN)
        self.assertEqual(result.payload["text"], REPORT)
        self.answer("steward", "开发结果如下：\n" + result.payload["text"])
        await self.run_inbox(MAIN)
        [result] = self.queued(ASH)
        self.answer("ash", result.payload["text"])
        await self.run_inbox(ASH)
        [row] = self.sent()
        message = self.outgoing(row.content, row.metadata)
        self.assertEqual(message.target, {"chat_id": "oc_kun"})
        self.assertLess(len(message.body_md), 100)
        self.assertIn("已合并", message.body_md)
        self.assertIn(PR, message.body_md)

    async def test_returned_stopped_report_uses_child_reply(self):
        await self.dispatch({"text": "安排优化", "return_to": ASH})
        self.forward("steward", SDE, "实现优化")
        await self.run_inbox(MAIN)
        self.answer("sde", REPORT, status="stopped", chat_reply="开发已停止。")
        await self.run_inbox(SDE)
        [back] = self.queued(ASH)
        self.answer("ash", back.payload["text"])
        await self.run_inbox(ASH)
        [row] = self.sent()
        self.assertEqual(self.outgoing(row.content, row.metadata).body_md, "开发已停止。")

    async def test_steward_can_write_the_reply_to_a_returned_delivery(self):
        self.forward("steward", SDE, "实现优化", return_to=MAIN)
        await self.dispatch({"text": "安排优化"})
        await self.run_inbox(MAIN)
        self.answer("sde", REPORT, delivery=DeliveryOutcome("merged", pr_url=PR))
        await self.run_inbox(SDE)
        self.answer("steward", "导出速度已优化。")
        await self.run_inbox(MAIN)
        [row] = self.sent()
        self.assertEqual(self.outgoing(row.content, row.metadata).body_md, f"导出速度已优化。\n{PR}")

    def test_group_thread_gets_ci_hold_github_still_gets_full_report(self):
        meta = {"kind": "emit", "session_id": "s", "correlation_key": "feishu:thread:om_group",
                TASK_CONTEXT_KEY: {"delivery": {"state": "pr_open", "pr_url": PR, "hold": "ci"}}}
        message = self.outgoing(REPORT, meta)
        self.assertEqual(message.target, {"reply_to_message_id": "om_group"})
        self.assertIn("检查", message.body_md)
        self.assertIn(PR, message.body_md)
        self.assertLess(len(message.body_md), 100)
        meta.update(reply_channel="github", reply_target="example/repo#42")
        self.assertEqual(self.outgoing(REPORT, meta).body_md, REPORT)

    async def test_stopped_report_preserves_the_human_reply_across_the_worker(self):
        self.answer("sde", REPORT, status="stopped", chat_reply="开发已停止，工作进度已保留。")
        self.ingest(SDE, key="feishu:thread:om_group")
        await self.run_inbox(SDE)
        [row] = self.sent()
        message = self.outgoing(row.content, row.metadata)
        self.assertEqual(message.body_md, "开发已停止，工作进度已保留。")
        self.assertEqual(row.content, REPORT)

    def test_approval_still_carries_the_question_and_buttons(self):
        question = "自动合并遇到网络问题。是否重试？"
        message = self.outgoing(question, {"kind": "approval_request", "session_id": "s",
                                          "approval_request_id": "req1", "correlation_key": "feishu:p2p:oc_1"})
        self.assertEqual((message.kind, message.body_md), ("approval_card", question))
        self.assertIsNotNone(message.card_payload)

    def test_forward_failure_explains_step_in_chinese(self):
        message = self.outgoing("forward refused by policy", {"kind": "forward_refused", "session_id": "s",
                                                             "correlation_key": "feishu:thread:om_group"})
        self.assertIn("转交", message.body_md)
        self.assertNotIn("refused", message.body_md)
