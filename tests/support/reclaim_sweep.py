from pathlib import Path

from agent_runtime.adapters.delivery import DeliveryConfig
from agent_runtime.adapters.executor_cli.reclaim_sweep import SweepTarget, WorktreeReclaimSweeper
from agent_runtime.adapters.storage_sqlite import (
    SQLiteDeliveryStore,
    SQLiteInboxAdapter,
    SQLiteOutboxAdapter,
    SQLiteWorkUsage,
)
from agent_runtime.core.models import Event
from tests.support.pr_delivery import git
from tests.support.verified_delivery import ReclaimCase


INBOX = "proj:x"


class SweepCase(ReclaimCase):
    def setUp(self):
        super().setUp()
        self.usage = SQLiteWorkUsage(self.db)
        self.deliveries = SQLiteDeliveryStore(self.db)
        self.outbox = SQLiteOutboxAdapter(self.db)
        self.inbox = SQLiteInboxAdapter(self.db)
        self.logged: list[str] = []
        self.delivery_config = DeliveryConfig(repo="o/r", open_pr=True, auto_merge=True)

    def target_for(self, record):
        if Path(record.repo_checkout).resolve() != self.primary.resolve():
            return None
        return SweepTarget(manager=self.manager(self.usage.blockers), delivery=self.delivery_config,
                           inbox_id=record.inbox_id or INBOX)

    def sweeper(self):
        return WorktreeReclaimSweeper(workspaces=self.workspaces, deliveries=self.deliveries, outbox=self.outbox,
                                      target_for=self.target_for, log=self.logged.append)

    def delivered(self):
        # What a merged PR delivery leaves behind: the branch pushed, squash-merged and pruned, the record merged.
        head, merge = self.squashed_task()
        self.merged_pr(head, merge)
        self.deliveries.save("task:9", {"state": "merged", "branch": "hb/task-9", "pushed_sha": head,
                                        "merge_sha": merge})
        return head

    def queued_event(self):
        evt, _ = self.inbox.append(Event(inbox_id=INBOX, source="test", external_id="late", correlation_key="task:9",
                                         payload={"text": "one more thing"}))
        return evt.event_id

    def drop(self, event_id):
        with self.db.transaction() as conn:
            conn.execute("UPDATE event_inbox SET status = 'dropped' WHERE event_id = ?", (event_id,))

    def messages(self):
        return [m.content for m in self.outbox.fetch_pending(limit=100)]

    def local_branch(self):
        return git(self.primary, "branch", "--list", "hb/task-9")
