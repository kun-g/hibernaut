from __future__ import annotations

import json
import urllib.request
from pathlib import Path
from typing import Any

import pytest

from collaboration import (
    CASConflictError,
    CloseoutCard,
    CloseoutCoordinator,
    CloseoutDecision,
    CloseoutMemoryDraft,
    CloseoutStore,
    CollaborationHubServer,
    CollaborationMCPTools,
    ExpertPoolRegistry,
    NotificationPolicyEngine,
    RoleSkillMemoryStore,
    build_four_layer_mount_plan,
)
from tests.support.collaboration_hub import fast_hub_shutdown


class _RecordingChiefSink:
    def __init__(self) -> None:
        self.cards: list[CloseoutCard] = []

    def record_closeout_summary(self, card: CloseoutCard) -> None:
        self.cards.append(card)


class _RecordingTaskboardSink:
    def __init__(self) -> None:
        self.created: list[tuple[str, str, str]] = []

    def create_follow_up_task(self, *, project_key: str, title: str, source_card_id: str) -> str:
        tid = f"task-{len(self.created) + 1}"
        self.created.append((project_key, title, source_card_id))
        return tid


class _RecordingMemorySink:
    def __init__(self) -> None:
        self.submitted: list[tuple[str, str, str, str]] = []

    def submit_memory_proposal(
        self,
        *,
        target_uri: str,
        content: str,
        reason: str,
        proposed_by: str,
    ) -> str:
        pid = f"mprop-{len(self.submitted) + 1}"
        self.submitted.append((target_uri, content, reason, proposed_by))
        return pid


class _StaticStateProvider:
    def list_tasks(self) -> list[dict[str, Any]]:
        return [
            {"project_key": "hibernaut", "status": "in_progress"},
            {"project_key": "hibernaut", "status": "done"},
            {"project_key": "lufit", "status": "ready"},
            {"project_key": "workshop", "status": "in_progress", "is_blocked": True},
        ]

    def list_runs(self) -> list[dict[str, Any]]:
        return [
            {"status": "done", "cost_usd": 0.12},
            {"status": "failed", "error_kind": "timeout", "cost_usd": 0.08},
        ]

    def list_project_proposals(self) -> list[dict[str, Any]]:
        return [
            {
                "proposal_id": "pprop-1",
                "target_uri": "proj://hibernaut/instructions.md",
                "reason": "Update CI timeout rule",
                "status": "pending",
                "created_at": "2026-11-24T08:00:00Z",
            }
        ]


def test_role_memory_cas_and_cross_project_reuse(tmp_path: Path) -> None:
    store = RoleSkillMemoryStore(tmp_path)
    p1 = store.propose_skill_write(
        role_id="sde-backend",
        content="# Python SQLite\nAlways enable WAL + busy_timeout.\n",
        reason="Learned during Hibernaut ledger work",
        proposed_by="pool:sde-backend",
    )
    assert p1.status == "pending"
    assert store.read_role_doc("sde-backend")[0] == ""

    approved = store.approve_proposal(p1.proposal_id, reviewed_by="owner")
    assert approved.status == "approved"
    text, sha = store.read_role_doc("sde-backend")
    assert "busy_timeout" in text

    with pytest.raises(CASConflictError):
        store.propose_skill_write(
            role_id="sde-backend",
            content="stale overwrite",
            reason="bad cas",
            proposed_by="pool:sde-backend",
            base_sha256="deadbeef",
        )

    p2 = store.propose_skill_write(
        role_id="sde-backend",
        content=text + "Use explicit workdir for non-project inboxes.\n",
        reason="v2 update",
        proposed_by="pool:sde-backend",
        base_sha256=sha,
    )
    rejected = store.reject_proposal(p2.proposal_id, reviewed_by="owner", review_note="too specific")
    assert rejected.status == "rejected"
    assert store.read_role_doc("sde-backend")[0] == text


def test_four_layer_mount_plan_enforces_single_project_rw() -> None:
    plan = build_four_layer_mount_plan(
        agent_id="pool:sde-backend",
        role_id="sde-backend",
        project_key="hibernaut",
        extra_readonly_projects=("workshop",),
    )
    rw_slices = [s for s in plan.slices if s.write_mode == "rw"]
    assert len(rw_slices) == 1
    assert rw_slices[0].uri == "proj://hibernaut/"
    assert any(s.uri == "role://sde-backend/" and s.write_mode == "propose" for s in plan.slices)
    assert any(s.uri == "proj://workshop/" and s.write_mode == "ro" for s in plan.slices)

    with pytest.raises(ValueError):
        build_four_layer_mount_plan(
            agent_id="pool:sde-backend",
            role_id="sde-backend",
            project_key="hibernaut",
            extra_readonly_projects=("hibernaut",),
        )


def test_expert_pool_resolves_shared_role_for_multiple_projects() -> None:
    pool = ExpertPoolRegistry()
    b1 = pool.resolve_for_project(role_id="architect", project_key="hibernaut", task_slug="d7-memory")
    b2 = pool.resolve_for_project(role_id="architect", project_key="lufit")
    assert b1.profile.agent_id == b2.profile.agent_id == "pool:architect"
    assert b1.inbox_scope == "proj:hibernaut:architect:d7-memory"
    assert b2.inbox_scope == "proj:lufit:architect"
    assert b1.mount_plan.project_key == "hibernaut"
    assert b2.mount_plan.project_key == "lufit"


def test_closeout_card_requires_explicit_confirm_and_is_idempotent(tmp_path: Path) -> None:
    store = CloseoutStore(tmp_path / "closeouts")
    chief = _RecordingChiefSink()
    tb = _RecordingTaskboardSink()
    mem = _RecordingMemorySink()
    coord = CloseoutCoordinator(store, chief_sink=chief, taskboard_sink=tb, memory_sink=mem)

    draft = coord.emit_draft(
        session_id="sess-1",
        agent_id="pool:architect",
        project_key="hibernaut",
        summary="Locked D5-A to D8-B collaboration boundaries",
        decisions=(CloseoutDecision(key="D6-A", title="Direct sync", choice="Explicit CloseoutCard"),),
        follow_up_tasks=("Wire role:// memory mount",),
        memory_proposals=(
            CloseoutMemoryDraft(
                target_uri="role://architect/skills.md",
                content="Always separate ER from architecture diagrams.",
                reason="Direct review feedback",
            ),
        ),
    )
    assert draft.status == "draft"
    assert len(chief.cards) == 0

    res = coord.confirm_and_sync(draft.card_id, confirmed_by="owner")
    assert res.chief_of_staff_notified is True
    assert res.tasks_created == ("task-1",)
    assert res.proposals_queued == ("mprop-1",)
    assert res.already_synced is False
    assert store.get(draft.card_id).status == "synced"

    res2 = coord.confirm_and_sync(draft.card_id, confirmed_by="owner")
    assert res2.already_synced is True
    assert len(chief.cards) == 1
    assert len(tb.created) == 1


def test_notification_policy_enforces_d8b_feishu_restraint() -> None:
    policy = NotificationPolicyEngine()
    assert policy.route(kind="approval_card").channel == "feishu_push"
    assert policy.route(kind="blocker_alert").channel == "feishu_push"
    assert policy.route(kind="daily_progress").channel == "hub_pull"
    assert policy.route(kind="digest_ready").channel == "hub_pull"
    assert policy.route(kind="proposal_queued").channel == "hub_pull"
    assert policy.route(kind="run_completed").channel == "silent"
    assert policy.route(kind="run_completed", is_urgent_blocker=True).channel == "feishu_push"


def test_mcp_tools_dispatch(tmp_path: Path) -> None:
    role_store = RoleSkillMemoryStore(tmp_path / "mem")
    pool = ExpertPoolRegistry()
    coord = CloseoutCoordinator(CloseoutStore(tmp_path / "closeouts"))
    mcp = CollaborationMCPTools(role_store=role_store, expert_pool=pool, closeout=coord)

    mount = mcp.call_tool("resolve_expert_mount", {"role_id": "sde-backend", "project_key": "hibernaut"})
    assert mount["inbox_scope"] == "proj:hibernaut:sde-backend"

    prop = mcp.call_tool(
        "propose_role_skill",
        {"role_id": "sde-backend", "content": "# Tip\nKeep layers clean.\n", "reason": "test"},
    )
    assert prop["status"] == "pending"

    card = mcp.call_tool(
        "emit_closeout_card",
        {
            "session_id": "s-9",
            "agent_id": "pool:architect",
            "project_key": "hibernaut",
            "summary": "Aligned contract",
            "decisions": [{"key": "D8-B", "title": "Push/Pull", "choice": "Hub pull"}],
        },
    )
    synced = mcp.call_tool("confirm_closeout_card", {"card_id": card["card_id"], "confirmed_by": "owner"})
    assert synced["already_synced"] is False


def test_hub_server_pull_digest_and_one_click_approvals(tmp_path: Path) -> None:
    role_store = RoleSkillMemoryStore(tmp_path / "mem")
    closeout_store = CloseoutStore(tmp_path / "closeouts")
    coord = CloseoutCoordinator(closeout_store)

    prop = role_store.propose_skill_write(
        role_id="architect",
        content="# Diagrams\nKeep swimlanes orthogonal.\n",
        reason="Clarity rule",
        proposed_by="pool:architect",
    )
    card = coord.emit_draft(
        session_id="s-10",
        agent_id="pool:architect",
        project_key="hibernaut",
        summary="Architecture finalized",
        decisions=(CloseoutDecision(key="D5-A", title="Split", choice="Thin CoS + Thick Steward"),),
    )

    server = CollaborationHubServer(
        host="127.0.0.1",
        port=0,
        role_store=role_store,
        closeout_store=closeout_store,
        closeout_coordinator=coord,
        state_provider=_StaticStateProvider(),
    )
    with fast_hub_shutdown():
        server.start_background()
    try:
        host, port = server.address
        base = f"http://{host}:{port}"

        with urllib.request.urlopen(f"{base}/") as resp:
            assert resp.status == 200
            assert "hub.guokun.ai" in resp.read().decode("utf-8")

        with urllib.request.urlopen(f"{base}/api/digest") as resp:
            digest = json.loads(resp.read().decode("utf-8"))
            assert digest["cost_summary"]["total_runs"] == 2
            assert digest["cost_summary"]["failed_runs"] == 1
            assert len(digest["pending_proposals"]) == 2
            assert len(digest["recent_closeouts"]) == 1

        req = urllib.request.Request(
            f"{base}/api/proposals/{prop.proposal_id}/approve",
            data=json.dumps({"reviewer": "owner"}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req) as resp:
            approved = json.loads(resp.read().decode("utf-8"))
            assert approved["status"] == "approved"

        req2 = urllib.request.Request(
            f"{base}/api/closeouts/{card.card_id}/confirm",
            data=json.dumps({"reviewer": "owner"}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req2) as resp:
            confirmed = json.loads(resp.read().decode("utf-8"))
            assert confirmed["already_synced"] is False
    finally:
        server.close()
