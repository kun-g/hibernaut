import dataclasses
import json

from agent_runtime.core.models import Emit
from tests.support.fakes import InMemoryPermission
from tests.support.pr_delivery import DeliveryCase, git
from tests.support.sde_delivery import PASS, SDEDeliveryCase, dev, event

TEMPLATE = "已修好，新 head {head_sha}，PR {pr_url}（#{pr_number}，版本 {version}）。{unknown} 保持原样。"


class TestPRComment(DeliveryCase):
    def deliver(self, wt, comment=TEMPLATE, **overrides):
        req = dataclasses.replace(self.request(wt, kind="fix"), pr_comment=comment)
        return self.delivery(**overrides).deliver(req, permissions=InMemoryPermission())

    def comments(self):
        return [c for pr in self.gh_state()["prs"] for c in pr.get("comments", [])]

    def inject(self, **state):
        self.state_file.write_text(json.dumps({**self.gh_state(), **state}))

    def test_posts_once_after_push_with_placeholders_replaced(self):
        wt = self.task("t1", {"calc.py": "x = 1\n"})
        rec = self.deliver(wt)
        assert rec.state == "pr_open" and not rec.problem
        [comment] = self.comments()
        head = self.remote_head("hb/t1")
        assert rec.pushed_sha == head
        assert comment["body"].startswith(
            f"已修好，新 head {head}，PR {rec.pr_url}（#{rec.pr_number}，版本 0.4.1）。{{unknown}} 保持原样。")
        assert rec.comment_head == head and rec.comment_url == comment["url"]
        assert f"PR comment: posted for {head[:12]}: {comment['url']}" in rec.render()
        [call] = self.gh_calls("comment")
        assert call[2] == str(rec.pr_number) and call[-2:] == ["-R", "o/r"]

    def test_posts_after_merge_when_auto_merge(self):
        wt = self.task("t2", {"calc.py": "x = 2\n"})
        rec = self.deliver(wt, auto_merge=True, require_ci=False)
        assert rec.state == "merged"
        [comment] = self.comments()
        assert rec.pushed_sha in comment["body"]

    def test_no_field_posts_nothing(self):
        wt = self.task("t3", {"calc.py": "x = 3\n"})
        rec = self.deliver(wt, comment="  ")
        assert rec.state == "pr_open"
        assert self.gh_calls("comment") == [] and self.comments() == []
        assert not any(line.startswith("PR comment") for line in rec.render())

    def test_transient_failure_is_retried(self):
        wt = self.task("t4", {"calc.py": "x = 4\n"})
        self.inject(fail={"comment": 2}, fail_error="Could not resolve host: api.github.com")
        rec = self.deliver(wt)
        assert rec.state == "pr_open" and not rec.problem and not rec.transient
        assert len(self.gh_calls("comment")) == 3
        assert len(self.comments()) == 1 and rec.comment_head == rec.pushed_sha
        assert sum(self.sleeps) == 10 + 30

    def test_reply_lost_after_posting_is_not_posted_again(self):
        wt = self.task("t5", {"calc.py": "x = 5\n"})
        self.inject(lose_comment_reply=1)
        rec = self.deliver(wt)
        assert len(self.gh_calls("comment")) == 1 and len(self.comments()) == 1
        assert rec.comment_head == rec.pushed_sha and rec.comment_url == self.comments()[0]["url"]

    def test_same_head_redelivered_is_not_posted_again(self):
        wt = self.task("t6", {"calc.py": "x = 6\n"})
        first = self.deliver(wt)
        again = self.deliver(wt)
        assert again.pushed_sha == first.pushed_sha
        assert len(self.comments()) == 1 and len(self.gh_calls("comment")) == 1
        assert f"PR comment: already posted for {first.pushed_sha[:12]}: {first.comment_url}" in again.render()
        # A lost record still finds the comment on the PR by its marker.
        self.store.save(f"task:{wt.name}", {**again.to_dict(), "comment_head": "", "comment_url": ""})
        self.deliver(wt)
        assert len(self.comments()) == 1 and len(self.gh_calls("comment")) == 1

    def test_new_head_gets_its_own_comment(self):
        wt = self.task("t7", {"calc.py": "x = 7\n"})
        first = self.deliver(wt)
        (wt / "calc.py").write_text("x = 8\n")
        git(wt, "commit", "-qam", "rework")
        second = self.deliver(wt)
        assert second.pushed_sha != first.pushed_sha
        bodies = [c["body"] for c in self.comments()]
        assert len(bodies) == 2 and first.pushed_sha in bodies[0] and second.pushed_sha in bodies[1]

    def test_failure_is_recorded_without_changing_the_delivery(self):
        wt = self.task("t8", {"calc.py": "x = 9\n"})
        self.inject(fail={"comment": 9}, fail_error="HTTP 403: Resource not accessible by integration")
        rec = self.deliver(wt)
        assert rec.state == "pr_open" and not rec.problem and not rec.transient and rec.notes == [
            "not merged: this project does not authorize merging (delivery.auto_merge = false)"]
        assert len(self.gh_calls("comment")) == 1 and self.sleeps == []
        assert rec.comment_head == ""
        [line] = [line for line in rec.render() if line.startswith("PR comment")]
        assert line.startswith(f"PR comment: not posted for {rec.pushed_sha[:12]}") and "HTTP 403" in line

    def test_transient_failure_that_persists_is_recorded_after_retries(self):
        wt = self.task("t9", {"calc.py": "x = 10\n"})
        self.inject(fail={"comment": 9}, fail_error="HTTP 502 Bad Gateway")
        rec = self.deliver(wt, auto_merge=True, require_ci=False)
        assert rec.state == "merged" and not rec.problem and not rec.transient
        assert len(self.gh_calls("comment")) == 4 and sum(self.sleeps) == 10 + 30 + 90
        [line] = [line for line in rec.render() if line.startswith("PR comment")]
        assert "not posted" in line and "4 attempt(s)" in line


class TestSDEReportsPRComment(SDEDeliveryCase):
    async def test_report_field_reaches_the_pr(self):
        self.set_plan(sde=[dev({"calc.py": "x = 1\n"}, pr_comment="head {head_sha} at {pr_url}")], reviewer=[PASS])
        outcome = await self.executor(delivery=self.delivery()).execute(
            [event(self.payload())], None, self.caps)
        assert isinstance(outcome, Emit) and outcome.delivery is not None
        [pr] = self.gh_state()["prs"]
        head = self.remote_head("hb/task-1")
        [comment] = pr["comments"]
        assert comment["body"].startswith(f"head {head} at {pr['url']}")
        assert f"PR comment: posted for {head[:12]}" in outcome.messages[0]
        assert "put that comment in pr_comment" in self.calls("sde")[0]["prompt"]
