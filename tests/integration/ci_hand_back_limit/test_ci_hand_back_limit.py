from agent_runtime.core.models import Suspend
from tests.support.ci_failure import AGAIN, TEST, CIStates, check
from tests.support.sde_delivery import SDEDeliveryCase, dev, event


class TestCIHandBackLimit(CIStates, SDEDeliveryCase):
    async def test_ci_hand_backs_stop_at_the_limit_with_an_approval_card(self):
        self.set_plan(sde=[dev({"calc.py": "x = 1\n"})])
        self.ci_fails(after_rerun=[[check("fail", AGAIN)]])
        executor = self.executor(review=False, delivery=self.delivery(auto_merge=True, require_review=False, max_handbacks=0))
        outcome = await executor.execute([event(self.payload())], None, self.caps)
        assert isinstance(outcome, Suspend), outcome
        card = outcome.prompt
        for part in ("（PR #1）：合并前的 CI 检查未通过。", "批准：", "拒绝：", "失败的检查：gates", f"日志：{AGAIN}",
                     f"失败的用例：{TEST}", "重跑：已在同一提交上重跑失败的检查一次，仍失败",
                     "已交回开发 0 次（上限 0）"):
            self.assertIn(part, card)
        self.assertEqual(len(self.calls("sde")), 1)
