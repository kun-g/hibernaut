from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from dataclasses import replace
from pathlib import Path
from typing import Any

from agent_runtime.adapters.channels import create_webhook_server
from agent_runtime.adapters.channels.ingress import CardRoute
from agent_runtime.adapters.channels.switchboard_bridge import SwitchboardInboundBridge
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase
from agent_runtime.app.config import HibernautConfig, parse_config
from agent_runtime.app.main import AlwaysOnDaemon, build_daemon_from_config, group_entrances
from agent_runtime.core.models import MCP_APPROVAL_SOURCE, SessionRef
from switchboard.envelope import CanonicalEvent
from tests.support.fakes import RecordingChannel

CHAT, PARTNERS, MAIN, SDE, OTHER = "proj:fruit:chat", "proj:fruit:partners", "proj:fruit", "proj:fruit:sde", "proj:other"
GROUP, PARTNER_GROUP = "oc_fruit", "oc_fruit_partners"
KUN, PARTNER = "ou_kun", "ou_partner"
REQUEST = "req_1"


def decision(*, actor: str, scope: str, source: str = "feishu") -> CanonicalEvent:
    return CanonicalEvent(
        source=source, event_type="approval.decided", external_id=f"approval:{REQUEST}:{scope}:{actor}", scope=scope,
        correlation_key="feishu:thread:om_1", actor=actor, body="approve",
        metadata={"approval_request_id": REQUEST, "decision": "approve", "session_id": "s1", "channel": "feishu",
                  "chat_id": "oc_approvals"},
    )


class RecordingHandler:
    def __init__(self) -> None:
        self.ingested: list[dict[str, Any]] = []

    def ingest(self, **kwargs: Any) -> object:
        self.ingested.append(kwargs)
        return None


class _Config(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        for name in ("fruit", "other"):
            (self.root / name).mkdir()

    def config(self, *groups: dict[str, Any]) -> HibernautConfig:
        def project(name: str, bindings: list[dict[str, Any]]) -> dict[str, Any]:
            return {"repo": f"o/{name}", "checkout": str(self.root / name), "worktree_backend": "git",
                    "inbox_id": [f"proj:{name}", f"proj:{name}:.+"], "default_agent": "front",
                    "inbox_bindings": bindings}

        return parse_config({
            "runtime": {"db_path": str(self.root / "hibernaut.db")},
            "executors": {"claude-code": {"harness": "claude-code"}},
            "agents": {"front": {"role": "steward", "executor": "claude-code"},
                       "sde": {"role": "sde", "executor": "claude-code", "review": False}},
            "projects": {
                "fruit": project("fruit", [
                    {"pattern": MAIN, "default_agent": "front", "agents": ["front"]},
                    {"pattern": CHAT, "default_agent": "front", "agents": ["front"]},
                    {"pattern": PARTNERS, "default_agent": "front", "agents": ["front"]},
                    {"pattern": "proj:fruit:.+", "default_agent": "sde", "agents": ["sde"]},
                ]),
                "other": project("other", [{"pattern": OTHER, "default_agent": "front", "agents": ["front"]}]),
            },
            "feishu_groups": list(groups),
        })

    @staticmethod
    def kun_group() -> dict[str, Any]:
        return {"chat_id": GROUP, "inbox_id": CHAT, "members": [KUN, PARTNER], "approvers": [KUN]}

    @staticmethod
    def partner_group() -> dict[str, Any]:
        # A second group of the same project, where Kun also lets the partner decide.
        return {"chat_id": PARTNER_GROUP, "inbox_id": PARTNERS, "members": [KUN, PARTNER],
                "approvers": [KUN, PARTNER]}


class MultiGroupTests(_Config):
    def test_new_c3_an_approver_of_one_group_decides_nothing_in_another_group_s_inbox(self) -> None:
        entrances = group_entrances(self.config(self.kun_group(), self.partner_group()))
        handler = RecordingHandler()
        bridge = SwitchboardInboundBridge(handler, groups=entrances)
        for scope in (CHAT, MAIN, SDE, "proj:fruit:sde:issue-1"):
            self.assertEqual(bridge.handle_canonical_event(decision(actor=PARTNER, scope=scope)), "acked")
        self.assertEqual(handler.ingested, [], "the partner decided an approval reserved for Kun")
        bridge.handle_canonical_event(decision(actor=PARTNER, scope=PARTNERS))
        bridge.handle_canonical_event(decision(actor=KUN, scope=CHAT))
        self.assertEqual([call["inbox_id"] for call in handler.ingested], [PARTNERS, CHAT])

    def test_new_c3_each_card_names_only_the_people_who_decide_that_inbox(self) -> None:
        entrances = group_entrances(self.config(self.kun_group(), self.partner_group()))
        self.assertEqual(entrances.card_group(CHAT), CardRoute(None, frozenset({KUN})))
        self.assertEqual(entrances.card_group(PARTNERS), CardRoute(None, frozenset({KUN, PARTNER})))
        # An inbox no group is bound to: only who every group of the project trusts.
        for inbox in (MAIN, SDE):
            self.assertEqual(entrances.card_group(inbox), CardRoute(None, frozenset({KUN})), inbox)

    def test_new_c3_groups_of_one_project_must_share_an_approver(self) -> None:
        other_only = {**self.partner_group(), "approvers": [PARTNER]}
        with self.assertRaisesRegex(ValueError, "share no approver"):
            self.config(self.kun_group(), other_only)


class AmbiguityTests(_Config):
    def test_new_c3_an_inbox_of_two_projects_is_decided_by_nobody(self) -> None:
        config = self.config(self.kun_group())
        # parse_config refuses overlapping rules; this is the case it would let through if it ever did not.
        twin = replace(config.projects["other"], name="twin", rules=config.projects["fruit"].inbox_rules)
        entrances = group_entrances(replace(config, projects={**config.projects, "twin": twin}))
        handler = RecordingHandler()
        bridge = SwitchboardInboundBridge(handler, groups=entrances)
        for actor in (KUN, PARTNER):
            bridge.handle_canonical_event(decision(actor=actor, scope=SDE))
        self.assertEqual(handler.ingested, [])
        self.assertEqual(entrances.card_group(SDE), CardRoute(None, frozenset()))


class ForgedDecisionTests(_Config):
    def setUp(self) -> None:
        super().setUp()
        self.db = SQLiteDatabase(self.root / "hibernaut.db")
        self.addCleanup(self.db.close)
        self.daemon: AlwaysOnDaemon = build_daemon_from_config(
            self.config(self.kun_group()), database=self.db, channel=RecordingChannel())

    def suspend(self, inbox: str) -> None:
        self.daemon.sessions.save(SessionRef(session_id=f"s-{inbox}", inbox_id=inbox, status="suspended",
                                             pending_request_id=REQUEST, executor_id="front"))

    def forge(self, inbox: str, source: str, *, remote: bool = False, **payload: Any) -> None:
        self.daemon.handler.ingest(
            inbox_id=inbox, source=source, external_id=f"forged:{source}:{remote}:{len(payload)}",
            payload={"approval_request_id": REQUEST, "approved": True, **payload}, remote=remote,
        )

    def test_new_c3_no_entrance_but_an_approver_s_click_resumes_a_project_inbox(self) -> None:
        for inbox in (MAIN, SDE):
            self.suspend(inbox)
            for source, remote, extra in (
                ("events:approval", False, {}),
                ("events:approval", True, {}),
                (MCP_APPROVAL_SOURCE, False, {}),
                # Naming the Feishu source and Kun does not make it Kun's click.
                ("sb:feishu", False, {"event_type": "approval.decided", "actor": KUN}),
                ("timer:follow_up", False, {"event_type": "suspend.elapsed"}),
            ):
                with self.subTest(inbox=inbox, source=source, remote=remote), self.assertRaises(ValueError):
                    self.forge(inbox, source, remote=remote, **extra)
            self.assertEqual(self.daemon.inbox.fetch_pending(inbox), [], inbox)

    def test_new_c3_a_forged_decision_posted_to_events_is_forbidden(self) -> None:
        self.suspend(MAIN)
        server = create_webhook_server("127.0.0.1", 0, self.daemon.handler)
        threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        body = {"inbox_id": MAIN, "source": "events:approval", "external_id": f"approval:{REQUEST}:approve",
                "payload": {"approval_request_id": REQUEST, "approved": True}}
        request = urllib.request.Request(f"http://127.0.0.1:{server.server_address[1]}/events",
                                         data=json.dumps(body).encode(), method="POST")
        with self.assertRaises(urllib.error.HTTPError) as refused:
            urllib.request.urlopen(request, timeout=5)
        self.assertEqual(refused.exception.code, 403)
        refused.exception.close()
        self.assertEqual(self.daemon.inbox.fetch_pending(MAIN), [])

    def test_new_c3_the_mcp_approval_tool_resumes_no_project_inbox(self) -> None:
        mcp = self.daemon.mcp_server
        assert mcp is not None
        for inbox, remote in ((MAIN, False), (CHAT, True), (CHAT, False)):
            self.suspend(inbox)
            reply = mcp.handle_rpc({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {
                "name": "hibernaut_resolve_approval",
                "arguments": {"inbox_id": inbox, "approval_request_id": REQUEST, "approved": True}}}, remote=remote)
            assert reply is not None
            self.assertIn("error", reply, (inbox, remote))
            self.assertEqual(self.daemon.inbox.fetch_pending(inbox), [], inbox)

    def test_new_c3_kun_s_click_still_resumes_and_other_projects_decide_as_before(self) -> None:
        self.suspend(MAIN)
        bridge = SwitchboardInboundBridge(self.daemon.handler, groups=group_entrances(self.config(self.kun_group())))
        self.assertEqual(bridge.handle_canonical_event(decision(actor=PARTNER, scope=MAIN)), "acked")
        self.assertEqual(bridge.handle_canonical_event(decision(actor=KUN, scope=MAIN)), "acked")
        [kept] = self.daemon.inbox.fetch_pending(MAIN)
        self.assertEqual(kept.payload["actor"], KUN)
        self.suspend(OTHER)
        self.forge(OTHER, "events:approval")
        self.assertEqual(len(self.daemon.inbox.fetch_pending(OTHER)), 1)


if __name__ == "__main__":
    unittest.main()
