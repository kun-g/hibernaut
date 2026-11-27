from __future__ import annotations

import asyncio
import os
from dataclasses import replace
from datetime import datetime, timedelta, UTC
from pathlib import Path
from typing import Any

import pytest

from agent_runtime.adapters.channels.ingress import event_origin
from agent_runtime.adapters.channels.switchboard_bridge import SwitchboardChannelAdapter
from agent_runtime.adapters.collaboration_bridge import HubState, TaskboardMCPClient
from agent_runtime.app.config import (
    CollaborationHubConfig,
    CollaborationTaskboardConfig,
    HibernautConfig,
    parse_config,
)
from agent_runtime.app.main import AlwaysOnDaemon, build_daemon_from_config
from agent_runtime.core.models import CLOSEOUT_SOURCE
from agent_runtime.engine.handler import ReservedSource
from collaboration import NotificationPolicyEngine
from ledger import RunRecord, RunUsage
from switchboard.config import OutboundConfig
from switchboard.envelope import CanonicalEvent
from switchboard.outbound.gateway import OutboundGateway
from switchboard.store_sqlite import SwitchboardStore
from tests.support.collaboration_wiring import config_of
from tests.support.collaboration_flow import (
    BOARD_TOKEN,
    Board,
    CARD,
    MCP_TOKEN,
    call,
    chief_events,
    hub_call,
    refused,
    rpc,
    settings,
)
from tests.support.collaboration_flow import board, daemon, tokens  # noqa: F401 -- pytest fixtures, requested by parameter name


SKILLS = "role://architect/skills.md"


def test_the_five_tools_are_on_the_mcp_server(daemon: AlwaysOnDaemon) -> None:
    listed = {t["name"]: t for t in rpc(daemon, "tools/list")["result"]["tools"]}
    names = {"emit_closeout_card", "confirm_closeout_card", "resolve_expert_mount", "propose_role_skill",
             "route_notification"}
    assert names <= set(listed) and "hibernaut_list_sessions" in listed and "memory_search" in listed
    # Who confirms or proposes is the caller, not an argument.
    assert "confirmed_by" not in listed["confirm_closeout_card"]["inputSchema"]["properties"]
    assert "proposed_by" not in listed["propose_role_skill"]["inputSchema"]["properties"]


def test_without_collaboration_the_tools_are_not_there(tmp_path: Path, tokens: None) -> None:
    raw = config_of(tmp_path, collaboration=None, experts=None, channels={"mcp_enabled": True})
    for project in raw["projects"].values():
        project.pop("experts", None)
        project.pop("expert_readonly_projects", None)
    plain = build_daemon_from_config(parse_config(raw))
    try:
        assert "emit_closeout_card" not in {t["name"] for t in rpc(plain, "tools/list")["result"]["tools"]}
        assert "Unknown tool" in refused(plain, "emit_closeout_card", **CARD)
    finally:
        plain.close()


def test_a_mount_is_resolved_for_drafted_experts_only(daemon: AlwaysOnDaemon) -> None:
    mount = call(daemon, "resolve_expert_mount", role_id="architect", project_key="hibernaut", task_slug="d7")
    assert (mount["agent_id"], mount["executor_id"], mount["inbox_scope"]) == (
        "pool:architect", "cli", "proj:hibernaut:architect:d7")
    assert [s["uri"] for s in mount["slices"] if s["write_mode"] == "rw"] == ["proj://hibernaut/"]
    assert "proj://workshop/" in [s["uri"] for s in mount["slices"]]
    assert "not drafted" in refused(daemon, "resolve_expert_mount", role_id="sde-backend", project_key="workshop")
    assert "extra_readonly_projects" in refused(
        daemon, "resolve_expert_mount", role_id="architect", project_key="hibernaut", extra_readonly_projects=["x"])


def test_a_role_proposal_names_its_caller(daemon: AlwaysOnDaemon) -> None:
    queued = call(daemon, "propose_role_skill", role_id="architect", content="x\n", reason="r", proposed_by="owner")
    assert (queued["status"], queued["proposed_by"]) == ("pending", "mcp")
    assert "not declared" in refused(daemon, "propose_role_skill", role_id="nobody", content="x", reason="r")
    assert "CAS" in refused(
        daemon, "propose_role_skill", role_id="architect", content="x", reason="r", base_sha256="0" * 64)


def test_notifications_are_routed_by_the_locked_policy(daemon: AlwaysOnDaemon) -> None:
    for kind, channel in {"approval_card": "feishu_push", "digest_ready": "hub_pull", "run_completed": "silent"}.items():
        assert call(daemon, "route_notification", kind=kind)["channel"] == channel


def test_a_confirmed_closeout_reaches_chief_taskboard_and_proposals_once(
    daemon: AlwaysOnDaemon, board: Board
) -> None:
    assert daemon.memory_hub is not None and daemon.collaboration is not None
    card = call(daemon, "emit_closeout_card", **CARD, follow_up_tasks=["Wire mounts", "Write docs"], memory_proposals=[
        {"target_uri": SKILLS, "content": "Draw seams first.", "reason": "review"},
        {"target_uri": "proj://hibernaut/notes/closeout.md", "content": "Locked D6-A.", "reason": "record"},
    ])
    assert chief_events(daemon) == [] and board.ingestion.tb_list_tasks() == []
    assert daemon.memory_hub.list_proposals() == [] and daemon.collaboration.role_store.list_proposals() == []

    result = call(daemon, "confirm_closeout_card", card_id=card["card_id"], confirmed_by="somebody")
    assert result["chief_of_staff_notified"] and not result["already_synced"]
    assert call(daemon, "confirm_closeout_card", card_id=card["card_id"])["already_synced"]
    assert daemon.collaboration.closeouts.get(card["card_id"]).confirmed_by == "mcp"

    tasks = [v.task for v in board.ingestion.tb_list_tasks()]
    assert sorted((t.scope, t.title, t.status) for t in tasks) == [
        ("proj:hibernaut", "Wire mounts", "backlog"), ("proj:hibernaut", "Write docs", "backlog")]
    (event,) = chief_events(daemon)
    assert event.payload["closeout"]["card_id"] == card["card_id"] and "Boundaries locked" in event.payload["body"]
    # An agent wrote the card, so it lends no operator authority to whoever reads it.
    assert event_origin(event) is None
    (note,) = daemon.memory_hub.list_proposals()
    assert note["proposal"]["target_uri"] == "proj://hibernaut/notes/closeout.md"
    (skill,) = daemon.collaboration.role_store.list_proposals()
    assert skill.content == "Draw seams first.\n"
    assert not (daemon.memory_hub.project_dir("hibernaut") / "notes" / "closeout.md").exists()


def test_a_sync_interrupted_by_taskboard_is_finished_without_repeating(
    daemon: AlwaysOnDaemon, board: Board
) -> None:
    assert daemon.memory_hub is not None and daemon.collaboration is not None
    card = call(daemon, "emit_closeout_card", **CARD, follow_up_tasks=["Wire mounts"], memory_proposals=[
        {"target_uri": SKILLS, "content": "a", "reason": "r"},
        {"target_uri": "proj://hibernaut/notes/n.md", "content": "b", "reason": "r"},
    ])
    board.stop()
    assert "confirm" in refused(daemon, "confirm_closeout_card", card_id=card["card_id"]).lower()
    assert chief_events(daemon) == []
    board.start()
    # As if the task had been created right before a crash.
    board.ingestion.tb_create_task(
        "proj:hibernaut", "Wire mounts", source_ref=_task_ref(card["card_id"], "Wire mounts"))
    call(daemon, "confirm_closeout_card", card_id=card["card_id"])
    assert len(board.ingestion.tb_list_tasks()) == 1
    assert len(daemon.memory_hub.list_proposals()) == 1 and len(daemon.collaboration.role_store.list_proposals()) == 1
    assert len(chief_events(daemon)) == 1


def _task_ref(card_id: str, title: str) -> str:
    from agent_runtime.adapters.collaboration_bridge import follow_up_ref
    return follow_up_ref(card_id, title)


def test_the_same_draft_on_another_card_is_another_proposal(daemon: AlwaysOnDaemon) -> None:
    assert daemon.collaboration is not None
    store = daemon.collaboration.role_store
    for _ in range(2):
        card = call(daemon, "emit_closeout_card", **CARD, memory_proposals=[
            {"target_uri": SKILLS, "content": "again", "reason": "r"}])
        call(daemon, "confirm_closeout_card", card_id=card["card_id"])
        pending = store.list_proposals(status="pending")
        store.reject_proposal(pending[-1].proposal_id, reviewed_by="owner")
    assert len(store.list_proposals()) == 2


@pytest.mark.parametrize("uri", [
    "proj://workshop/notes/x.md", "global://instructions.md", "kb://pkb/x.md", "role://nobody/skills.md",
    "role://architect/../x.md", "role://architect/skills.txt", "notes/x.md", "proj://hibernaut/../workshop/x.md",
])
def test_a_card_cannot_propose_memory_outside_its_project_and_roles(daemon: AlwaysOnDaemon, uri: str) -> None:
    assert refused(daemon, "emit_closeout_card", **CARD, memory_proposals=[
        {"target_uri": uri, "content": "x", "reason": "r"}])


def test_a_card_names_a_project_of_this_deployment(daemon: AlwaysOnDaemon) -> None:
    assert "nowhere" in refused(daemon, "emit_closeout_card", **{**CARD, "project_key": "nowhere"})


def test_nobody_can_post_a_closeout_into_the_chiefs_inbox(daemon: AlwaysOnDaemon) -> None:
    with pytest.raises(ReservedSource):
        daemon.handler.ingest(inbox_id="user:main", source=CLOSEOUT_SOURCE, external_id="x", payload={})


def ledger_run(number: int, *, ended: datetime, **over: Any) -> RunRecord:
    fields: dict[str, Any] = {
        "run_id": f"run_{number}", "inbox_id": "proj:hibernaut", "session_id": "s", "event_ids": ("e",),
        "executor_id": "cli", "started_at": ended.isoformat(), "ended_at": ended.isoformat(), "duration_ms": 1,
        "outcome_kind": "emit", "emit_status": "done", "usage": RunUsage(cost_usd=0.5), **over,
    }
    return RunRecord(**fields)


def test_the_digest_reads_taskboard_ledger_and_memory(daemon: AlwaysOnDaemon, board: Board) -> None:
    assert daemon.ledger is not None
    ready = board.ingestion.tb_create_task("proj:hibernaut", "ready one", initial_status="ready")
    blocked = board.ingestion.tb_create_task("proj:hibernaut", "blocked one")
    board.ingestion.tb_manage_dependency("add", ready.task_id, blocked.task_id)
    now = datetime.now(UTC)
    daemon.ledger.store.append_run(ledger_run(1, ended=now))
    daemon.ledger.store.append_run(ledger_run(
        2, ended=now, outcome_kind="error", emit_status=None, error_kind="timeout"))
    # Waiting for an approval is no failure, and yesterday's Runs are not of the last 24 hours.
    daemon.ledger.store.append_run(ledger_run(3, ended=now, outcome_kind="suspend", emit_status=None))
    daemon.ledger.store.append_run(ledger_run(4, ended=now - timedelta(hours=30)))
    call(daemon, "propose_role_skill", role_id="architect", content="x\n", reason="role change")
    call(daemon, "memory_propose_write", target_uri="proj://hibernaut/instructions.md", op="append", content="rule",
         rationale="project change")
    status, digest = hub_call(daemon, "digest")
    assert status == 200
    assert digest["cost_summary"] == {"total_runs": 3, "failed_runs": 1, "total_cost_usd": 1.5}
    (project,) = digest["projects"]
    assert (project["project_key"], project["ready"], project["blocked"]) == ("hibernaut", 1, 1)
    assert sorted((p["scope"], p["reason"]) for p in digest["pending_proposals"]) == [
        ("project_memory", "project change"), ("role_skill", "role change")]


def test_the_owner_reviews_both_kinds_of_proposals_on_the_hub(daemon: AlwaysOnDaemon) -> None:
    assert daemon.memory_hub is not None and daemon.collaboration is not None
    role = call(daemon, "propose_role_skill", role_id="architect", content="x\n", reason="r")["proposal_id"]
    project = call(daemon, "memory_propose_write", target_uri="proj://hibernaut/instructions.md", op="append",
                   content="rule", rationale="r")["proposal_id"]
    shown = {p["proposal_id"]: p for p in hub_call(daemon, "proposals")[1]["proposals"]}
    assert shown[project]["content"] == "rule" and shown[project]["reviewable"]
    assert hub_call(daemon, f"proposals/{role}/approve", post=True)[1]["reviewed_by"] == "owner"
    assert hub_call(daemon, f"proposals/{project}/approve", post=True)[0] == 200
    assert "rule" in (daemon.memory_hub.project_dir("hibernaut") / "instructions.md").read_text()
    decided = daemon.memory_hub.list_proposals(status=None)[0]
    assert (decided["status"], decided["decided_by"]) == ("approved", "owner")
    assert hub_call(daemon, f"proposals/{project}/reject", post=True)[0] == 409
    assert hub_call(daemon, "proposals/prop_00/approve", post=True)[0] == 404


def test_a_card_is_confirmed_on_the_hub(daemon: AlwaysOnDaemon, board: Board) -> None:
    card = call(daemon, "emit_closeout_card", **CARD, follow_up_tasks=["One"])
    board.stop()
    path = f"closeouts/{card['card_id']}/confirm"
    assert hub_call(daemon, path, post=True)[0] == 502
    status, digest = hub_call(daemon, "digest")
    assert status == 200 and "tasks" in digest["window_label"]
    board.start()
    assert hub_call(daemon, path, post=True)[1]["tasks_created"]
    assert len(chief_events(daemon)) == 1


def test_the_tokens_leave_the_environment_and_the_hub_needs_its_own(daemon: AlwaysOnDaemon) -> None:
    assert "HIBERNAUT_COLLABORATION_TOKEN" not in os.environ and "HIBERNAUT_TASKBOARD_MCP_TOKEN" not in os.environ
    for token in ("", MCP_TOKEN, BOARD_TOKEN):
        assert hub_call(daemon, "digest", token=token)[0] == 401


@pytest.mark.parametrize("missing", ["HIBERNAUT_COLLABORATION_TOKEN", "HIBERNAUT_TASKBOARD_MCP_TOKEN"])
def test_a_missing_token_is_refused_at_startup(
    tmp_path: Path, board: Board, tokens: None, monkeypatch: pytest.MonkeyPatch, missing: str
) -> None:
    monkeypatch.delenv(missing)
    with pytest.raises(ValueError, match=missing):
        build_daemon_from_config(settings(tmp_path, board, hub=True))


def test_without_taskboard_cards_with_follow_ups_are_refused(tmp_path: Path, tokens: None) -> None:
    opened = build_daemon_from_config(settings(tmp_path))
    try:
        assert opened.collaboration is not None and opened.collaboration.hub_server is None
        card = call(opened, "emit_closeout_card", **CARD, follow_up_tasks=["One"])
        assert "Taskboard" in refused(opened, "confirm_closeout_card", card_id=card["card_id"])
        plain = call(opened, "emit_closeout_card", **CARD)
        assert call(opened, "confirm_closeout_card", card_id=plain["card_id"])["chief_of_staff_notified"]
    finally:
        opened.close()


def test_taskboard_redirects_do_not_take_the_token_along(tmp_path: Path) -> None:
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    seen: list[str | None] = []

    class Redirecting(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            pass

        def do_POST(self) -> None:
            self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(302)
            self.send_header("Location", "/elsewhere")
            self.end_headers()

        def do_GET(self) -> None:
            seen.append(self.headers.get("Authorization"))
            self.send_response(200)
            self.end_headers()

    server = HTTPServer(("127.0.0.1", 0), Redirecting)
    # shutdown() waits for the next poll before the redirect server's thread can exit.
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        client = TaskboardMCPClient(f"http://127.0.0.1:{server.server_port}/mcp", "secret")
        with pytest.raises(RuntimeError):
            client.call("tb_list_tasks", {})
        assert seen == []
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def test_finished_tasks_are_counted_for_a_day() -> None:
    now = datetime(2026, 11, 24, 12, tzinfo=UTC)

    class Client:
        def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
            def task(status: str, hours: int, **over: Any) -> dict[str, Any]:
                return {"scope": "proj:hibernaut:issue-1", "status": status, "blocked_reason": "",
                        "updated_at": (now - timedelta(hours=hours)).isoformat(), "incomplete_blocker_count": 0, **over}
            return {"tasks": [
                task("done", 1), task("done", 30), task("cancelled", 1), task("backlog", 1),
                task("in_progress", 900), task("in_progress", 1, blocked_reason="waiting"),
                task("done", 1, incomplete_blocker_count=1),
            ]}

    found = HubState(hub=None, ledger=None, taskboard=Client(), clock=lambda: now).list_tasks()
    assert [(t["project_key"], t["status"], t["is_blocked"]) for t in found] == [
        ("hibernaut", "done", False), ("hibernaut", "in_progress", False), ("hibernaut", "in_progress", True),
        ("hibernaut", "done", False),
    ]


def test_the_stdio_companion_opens_no_hub_and_needs_no_taskboard_token(tmp_path: Path, board: Board) -> None:
    from agent_runtime.app.main import companion_config

    cfg = settings(tmp_path, board, hub=True)
    without = companion_config(cfg, {})
    assert without.collaboration.hub == CollaborationHubConfig()
    assert without.collaboration.taskboard == CollaborationTaskboardConfig()
    with_token = companion_config(cfg, {"HIBERNAUT_TASKBOARD_MCP_TOKEN": "x"})
    assert with_token.collaboration.taskboard == cfg.collaboration.taskboard
    # What the projects draft is unchanged.
    assert replace(without.collaboration, hub=cfg.collaboration.hub, taskboard=cfg.collaboration.taskboard) == (
        cfg.collaboration)


class Push:
    # What the Switchboard gateway would post for one Outbox message.
    def __init__(self, tmp_path: Path, *, policy: NotificationPolicyEngine | None) -> None:
        self.events: list[CanonicalEvent] = []
        self.channel = SwitchboardChannelAdapter(self._publish, push_policy=policy)
        self.gateway = OutboundGateway(
            SwitchboardStore(str(tmp_path / "sb.db")), {}, config=OutboundConfig(
                approval_target="oc_owner", reply_to_origin=("feishu", "github")))

    def _publish(self, event: CanonicalEvent) -> Any:
        self.events.append(event)
        return None

    def target(self, **metadata: Any) -> tuple[str, Any] | None:
        base: dict[str, Any] = {
            "kind": "emit", "outcome_kind": "emit", "session_id": "s", "run_id": "r", "correlation_key": "gdrive:1",
            "task_context": {"source": "gdrive:task", "status": "done"},
        }
        asyncio.run(self.channel.send("proj:hibernaut", "text", {**base, **metadata}))
        message = self.gateway.message_for_report(self.events[-1])
        return None if message is None else (message.channel, dict(message.target))


FEISHU: dict[str, Any] = {"reply_channel": "feishu", "reply_target": "oc_team"}


def context(source: str, status: str = "done") -> dict[str, Any]:
    return {"task_context": {"source": source, "status": status}}


@pytest.fixture
def push(tmp_path: Path) -> Push:
    return Push(tmp_path, policy=NotificationPolicyEngine())


def test_without_the_policy_nothing_changes(tmp_path: Path) -> None:
    plain = Push(tmp_path, policy=None)
    assert plain.target(**FEISHU) == ("feishu", {"chat_id": "oc_team"})
    assert plain.events[-1].metadata["outcome_kind"] == "emit"


@pytest.mark.parametrize("metadata", [
    FEISHU,
    {**FEISHU, **context("timer:digest")},
    {"correlation_key": "feishu:p2p:oc_owner", **context("timer:digest")},
    {"correlation_key": "feishu:p2p:oc_owner", **context("sb:taskboard")},
    {"correlation_key": "feishu:thread:om_1", **context("gdrive:task")},
    {"correlation_key": "feishu:p2p:oc_owner", **context(CLOSEOUT_SOURCE)},
    {"correlation_key": "feishu:p2p:oc_owner", "task_context": {}},
    {"correlation_key": "feishu:p2p:oc_owner", **context("sb:feishu"), **FEISHU},
])
def test_a_routine_result_is_not_pushed_to_feishu(push: Push, metadata: dict[str, Any]) -> None:
    assert push.target(**metadata) is None


def test_a_withheld_result_is_still_reported(push: Push) -> None:
    assert push.target(**FEISHU, task_id="t1", task_action="complete", trace_id="tr", hop_count=1) is None
    (event,) = push.events
    assert event.metadata["task_action"] == "complete" and event.body == "text"
    assert event.metadata["notification_channel"] == "silent"


@pytest.mark.parametrize("metadata", [
    {"kind": "approval_request", "outcome_kind": "suspend", "approval_request_id": "a1", "title": "t"},
    {"kind": "run_failed", "outcome_kind": "error", **FEISHU},
    {"kind": "resume_failed", "outcome_kind": "error", **FEISHU},
    {"kind": "return_withheld", "outcome_kind": "error", **FEISHU},
    {**FEISHU, **context("timer:digest", "failed")},
    {**FEISHU, **context("timer:digest", "stopped")},
    {**FEISHU, **context("timer:digest", "refused")},
])
def test_approvals_and_blockers_are_pushed(push: Push, metadata: dict[str, Any]) -> None:
    found = push.target(**metadata)
    assert found is not None and found[0] == "feishu"


@pytest.mark.parametrize("source", ["sb:feishu", "agent:return", "agent:forward", "mcp:approval", "webhook"])
@pytest.mark.parametrize("key", ["feishu:p2p:oc_owner", "feishu:thread:om_1"])
def test_an_answer_to_the_persons_own_message_is_no_push(push: Push, source: str, key: str) -> None:
    found = push.target(correlation_key=key, **context(source))
    assert found is not None and found[0] == "feishu"


def test_an_answer_addressed_back_to_the_same_chat_is_no_push(push: Push) -> None:
    assert push.target(
        correlation_key="feishu:p2p:oc_owner", reply_channel="feishu", reply_target="oc_owner", **context("sb:feishu"))


def test_other_channels_are_left_alone(push: Push) -> None:
    assert push.target(correlation_key="github:o/r#1") == ("github", {"repo": "o/r", "number": 1})
    assert push.events[-1].metadata["outcome_kind"] == "emit"
    assert push.target(correlation_key="feishu:p2p:oc_owner", reply_channel="github", reply_target="o/r#2") == (
        "github", {"repo": "o/r", "number": 2})


def test_a_daemon_with_collaboration_holds_to_the_policy(tmp_path: Path, tokens: None) -> None:
    class Handle:
        def publish(self, event: CanonicalEvent) -> Any:
            return None

    def policy_of(cfg: HibernautConfig) -> object:
        opened = build_daemon_from_config(cfg, switchboard=Handle(), attach=False)  # type: ignore[arg-type]
        try:
            assert isinstance(opened.channel, SwitchboardChannelAdapter)
            return opened.channel.push_policy
        finally:
            opened.close()

    assert isinstance(policy_of(settings(tmp_path)), NotificationPolicyEngine)
