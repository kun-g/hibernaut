from __future__ import annotations

import json
import unittest
from typing import Any

from tests.switchboard_support import (
    FEISHU_TOKEN,
    Harness,
    github_headers,
    issue_payload,
    make_harness,
    post,
    send_github,
)


class GitHubInboundTests(unittest.TestCase):
    def setUp(self) -> None:
        self.h: Harness = make_harness()

    def tearDown(self) -> None:
        self.h.close()

    def test_issue_opened_is_normalized(self) -> None:
        status, body = send_github(self.h.url, issue_payload())
        self.assertEqual(status, 202)
        self.assertEqual(body["status"], "accepted")
        self.assertEqual(body["consumers"], ["taskboard"])
        event = self.h.board.store.get_inbound(body["event_id"])
        assert event is not None
        self.assertEqual(event.event_type, "work.opened")
        self.assertEqual(event.correlation_key, "github:kun-g/hibernaut#42")
        self.assertEqual(event.external_id, "issue:kun-g/hibernaut#42:opened:2026-11-22T04:00:00Z")
        self.assertEqual(event.scope, "proj:hibernaut")
        self.assertEqual(event.occurred_at, "2026-11-22T04:00:00.000000Z")
        self.assertEqual(event.metadata["work_kind"], "issue")
        self.assertEqual(event.metadata["labels"], ["P1"])

    def test_redelivery_with_new_delivery_uuid_is_duplicate(self) -> None:
        payload = issue_payload()
        _, first = send_github(self.h.url, payload, delivery="11111111-aaaa")
        status, second = send_github(self.h.url, payload, delivery="22222222-bbbb")
        self.assertEqual(status, 202)
        self.assertEqual(second["status"], "duplicate")
        self.assertTrue(second["duplicate"])
        self.assertEqual(second["event_id"], first["event_id"])
        self.assertEqual(self.h.board.store.count_inbound(), 1)
        self.assertEqual(len(self.h.board.store.list_deliveries()), 1)
        self.assertTrue(any(r["outcome"] == "duplicate" for r in self.h.audit.records))

    def test_update_with_new_updated_at_is_a_new_event(self) -> None:
        send_github(self.h.url, issue_payload())
        _, body = send_github(
            self.h.url, issue_payload("labeled", updated_at="2026-11-22T05:00:00Z"), delivery="d-2"
        )
        self.assertEqual(body["status"], "accepted")
        event = self.h.board.store.get_inbound(body["event_id"])
        assert event is not None
        self.assertEqual(event.event_type, "work.updated")

    def test_closed_fans_out_and_pull_request_kind(self) -> None:
        _, closed = send_github(self.h.url, issue_payload("closed", state="closed"))
        self.assertEqual(closed["consumers"], ["taskboard", "hibernaut"])
        pr = {
            "action": "opened",
            "pull_request": {"number": 7, "title": "PR", "state": "open", "updated_at": "2026-11-22T04:00:00Z"},
            "repository": {"full_name": "Kun-G/Hibernaut"},
            "sender": {"login": "kun"},
        }
        _, opened = send_github(self.h.url, pr, event="pull_request", delivery="d-pr")
        event = self.h.board.store.get_inbound(opened["event_id"])
        assert event is not None
        self.assertEqual(event.metadata["work_kind"], "pull_request")
        self.assertEqual(event.external_id, "pr:kun-g/hibernaut#7:opened:2026-11-22T04:00:00Z")

    def test_comment_maps_to_conversation_and_bot_comments_are_dropped(self) -> None:
        comment = {
            "action": "created",
            "issue": {"number": 42, "title": "t"},
            "comment": {"id": 555, "body": "please retry", "user": {"login": "kun", "type": "User"}},
            "repository": {"full_name": "kun-g/hibernaut"},
            "sender": {"login": "kun", "type": "User"},
        }
        status, body = send_github(self.h.url, comment, event="issue_comment", delivery="c1")
        self.assertEqual((status, body["consumers"]), (202, ["hibernaut"]))
        event = self.h.board.store.get_inbound(body["event_id"])
        assert event is not None
        self.assertEqual(event.external_id, "comment:555")
        self.assertEqual(event.metadata["channel"], "github")

        bot = json.loads(json.dumps(comment))
        bot["comment"]["id"] = 556
        bot["sender"] = {"login": "switchboard[bot]", "type": "Bot"}
        status, body = send_github(self.h.url, bot, event="issue_comment", delivery="c2")
        self.assertEqual((status, body["status"]), (200, "ignored"))

    def test_comments_record_whether_they_are_on_an_issue_or_a_pull_request(self) -> None:
        base = {
            "action": "created",
            "comment": {"id": 600, "body": "x", "user": {"login": "kun", "type": "User"}},
            "repository": {"full_name": "kun-g/hibernaut"},
            "sender": {"login": "kun", "type": "User"},
        }
        cases = [
            ("issue_comment", {"issue": {"number": 42, "title": "t"}}, "issue"),
            ("issue_comment", {"issue": {"number": 43, "title": "t", "pull_request": {"url": "u"}}}, "pull_request"),
            ("pull_request_review_comment", {"pull_request": {"number": 44, "title": "t"}}, "pull_request"),
        ]
        for n, (github_event, item, kind) in enumerate(cases):
            with self.subTest(github_event=github_event, kind=kind):
                payload = {**base, **item, "comment": {**base["comment"], "id": 600 + n}}
                status, body = send_github(self.h.url, payload, event=github_event, delivery=f"wk{n}")
                self.assertEqual(status, 202, body)
                event = self.h.board.store.get_inbound(body["event_id"])
                assert event is not None
                self.assertEqual(event.metadata["work_kind"], kind)
        review = {"action": "submitted", "review": {"id": 700, "body": "", "state": "APPROVED",
                                                    "user": {"login": "kun", "type": "User"}},
                  "pull_request": {"number": 45, "title": "t"}, "repository": base["repository"],
                  "sender": base["sender"]}
        status, body = send_github(self.h.url, review, event="pull_request_review", delivery="wk-review")
        self.assertEqual(status, 202, body)
        event = self.h.board.store.get_inbound(body["event_id"])
        assert event is not None
        self.assertEqual(event.metadata["work_kind"], "pull_request")

    def test_unlisted_events_are_ignored(self) -> None:
        status, body = send_github(self.h.url, {"ref": "main"}, event="push")
        self.assertEqual((status, body["status"]), (200, "ignored"))
        self.assertEqual(self.h.board.store.count_inbound(), 0)

    def test_bad_signature_is_401_and_only_audited(self) -> None:
        raw = json.dumps(issue_payload()).encode()
        status, _ = post(
            f"{self.h.url}/webhooks/github", raw, github_headers(raw, "issues", "d", secret="wrong")
        )
        self.assertEqual(status, 401)
        self.assertEqual(self.h.board.store.count_inbound(), 0)
        self.assertTrue(any(r["outcome"] == "rejected_401" for r in self.h.audit.records))

    def test_missing_fields_are_400_and_only_audited(self) -> None:
        no_number = issue_payload()
        del no_number["issue"]["number"]
        status, _ = send_github(self.h.url, no_number)
        self.assertEqual(status, 400)
        no_title = issue_payload()
        no_title["issue"]["title"] = ""
        status, body = send_github(self.h.url, no_title, delivery="d-2")
        self.assertEqual(status, 400)
        self.assertIn("work.opened requires subject", body["problems"])
        self.assertEqual(self.h.board.store.count_inbound(), 0)
        outcomes = [(r["stage"], r["outcome"]) for r in self.h.audit.records]
        self.assertIn(("inbound", "rejected_400"), outcomes)
        self.assertIn(("validate", "rejected_400"), outcomes)

    def test_body_limit(self) -> None:
        import socket

        # Only the headers are sent: the server must refuse by Content-Length before reading.
        with socket.create_connection(("127.0.0.1", self.h.board.http.port), timeout=5) as sock:
            sock.sendall(
                b"POST /webhooks/github HTTP/1.1\r\nHost: x\r\n"
                b"Content-Length: 1048577\r\nX-GitHub-Event: issues\r\n\r\n"
            )
            status_line = sock.recv(64).split(b"\r\n", 1)[0]
        self.assertIn(b" 413 ", status_line)
        self.assertEqual(self.h.board.store.count_inbound(), 0)

    def test_healthz(self) -> None:
        import urllib.request

        with urllib.request.urlopen(f"{self.h.url}/healthz", timeout=5) as resp:
            self.assertEqual(json.loads(resp.read()), {"status": "ok"})


class UnmatchedPolicyTests(unittest.TestCase):
    def test_fallback_uses_default_scope(self) -> None:
        h = make_harness()
        try:
            _, body = send_github(h.url, issue_payload(repo="someone/else"))
            event = h.board.store.get_inbound(body["event_id"])
            assert event is not None
            self.assertEqual(event.scope, "user:main")
        finally:
            h.close()

    def test_reject_drops_unrouted_repo(self) -> None:
        h = make_harness(routing={"unmatched_policy": "reject"})
        try:
            status, body = send_github(h.url, issue_payload(repo="someone/else"))
            self.assertEqual((status, body["status"]), (200, "ignored"))
            self.assertEqual(h.board.store.count_inbound(), 0)
        finally:
            h.close()


def feishu_message(
    *,
    chat_type: str = "p2p",
    chat_id: str = "oc_ops",
    message_id: str = "om_1",
    root_id: str = "",
    mentions: list[dict[str, Any]] | None = None,
    sender_type: str = "user",
    token: str = FEISHU_TOKEN,
) -> dict[str, Any]:
    return {
        "schema": "2.0",
        "header": {"event_type": "im.message.receive_v1", "token": token, "event_id": "fe_1"},
        "event": {
            "sender": {"sender_type": sender_type, "sender_id": {"open_id": "ou_kun"}},
            "message": {
                "message_id": message_id,
                "root_id": root_id,
                "chat_id": chat_id,
                "chat_type": chat_type,
                "message_type": "text",
                "create_time": "1795320000000",
                "content": json.dumps({"text": "@_user_1 deploy please"}),
                "mentions": mentions or [],
            },
        },
    }


class FeishuInboundTests(unittest.TestCase):
    def setUp(self) -> None:
        self.h: Harness = make_harness(feishu={"bot_open_id": "ou_bot"})
        self.url = f"{self.h.url}/webhooks/feishu"

    def tearDown(self) -> None:
        self.h.close()

    def test_url_verification(self) -> None:
        status, body = post(self.url, {"type": "url_verification", "challenge": "abc", "token": FEISHU_TOKEN})
        self.assertEqual((status, body), (200, {"challenge": "abc"}))

    def test_bad_token_is_401(self) -> None:
        status, _ = post(self.url, feishu_message(token="wrong"))
        self.assertEqual(status, 401)
        self.assertEqual(self.h.board.store.count_inbound(), 0)
        self.assertTrue(any(r["outcome"] == "rejected_401" for r in self.h.audit.records))

    def test_p2p_message(self) -> None:
        status, body = post(self.url, feishu_message())
        self.assertEqual((status, body["status"]), (200, "accepted"))
        event = self.h.board.store.get_inbound(body["event_id"])
        assert event is not None
        self.assertEqual(event.correlation_key, "feishu:p2p:oc_ops")
        self.assertEqual(event.body, "deploy please")
        self.assertEqual(event.scope, "proj:hibernaut")
        self.assertEqual(event.external_id, "msg:om_1")

    def test_group_requires_mention_and_uses_thread_key(self) -> None:
        status, body = post(self.url, feishu_message(chat_type="group"))
        self.assertEqual(body["status"], "ignored")
        mention = [{"key": "@_user_1", "id": {"open_id": "ou_bot"}}]
        _, body = post(self.url, feishu_message(chat_type="group", message_id="om_2", root_id="om_root", mentions=mention))
        event = self.h.board.store.get_inbound(body["event_id"])
        assert event is not None
        self.assertEqual(event.correlation_key, "feishu:thread:om_root")
        other = [{"key": "@_user_1", "id": {"open_id": "ou_someone"}}]
        _, body = post(self.url, feishu_message(chat_type="group", message_id="om_3", mentions=other))
        self.assertEqual(body["status"], "ignored")

    def test_bot_messages_are_ignored(self) -> None:
        _, body = post(self.url, feishu_message(sender_type="app"))
        self.assertEqual(body["status"], "ignored")
        self.assertEqual(self.h.board.store.count_inbound(), 0)


if __name__ == "__main__":
    unittest.main()
