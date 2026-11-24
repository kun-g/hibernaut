from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
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
    RoleSkillMemoryStore,
)

DECISIONS = (CloseoutDecision(key="D6-A", title="Sync", choice="Explicit card"),)
TOKEN = "hub-token-for-tests"


def draft(coord: CloseoutCoordinator, **over: Any) -> CloseoutCard:
    fields: dict[str, Any] = {
        "session_id": "s-1", "agent_id": "pool:architect", "project_key": "hibernaut", "summary": "done",
        "decisions": DECISIONS, **over,
    }
    return coord.emit_draft(**fields)


class Recorder:
    def __init__(self, log: list[str], *, fail_once: str = "") -> None:
        self.log = log
        self._fail_once = fail_once

    def _note(self, entry: str) -> None:
        if entry == self._fail_once:
            self._fail_once = ""
            raise RuntimeError("sink down")
        self.log.append(entry)

    def record_closeout_summary(self, card: CloseoutCard) -> None:
        self._note("chief")

    def create_follow_up_task(self, *, project_key: str, title: str, source_card_id: str) -> str:
        self._note(f"task:{title}")
        return f"task-{title}"

    def submit_memory_proposal(self, *, target_uri: str, content: str, reason: str, proposed_by: str) -> str:
        self._note(f"memory:{target_uri}:{content}")
        return f"prop-{target_uri}"


# --- stores: ids, atomic writes, concurrency --------------------------------------------------------------------


@pytest.mark.parametrize("bad", ["../outside", "a/b", "..", "", "closeout-x", "/etc/passwd"])
def test_ids_cannot_leave_their_directory(tmp_path: Path, bad: str) -> None:
    closeouts, roles = CloseoutStore(tmp_path / "closeouts"), RoleSkillMemoryStore(tmp_path / "mem")
    # Complete records just outside, so only the id check can refuse them.
    card = draft(CloseoutCoordinator(closeouts))
    proposal = roles.propose_skill_write(role_id="sde", content="x", reason="r", proposed_by="x")
    (tmp_path / "outside.json").write_text(json.dumps(card.to_dict()))
    (tmp_path / "mem" / "proposals" / "outside.json").write_text(json.dumps(proposal.to_dict()))
    with pytest.raises(KeyError):
        closeouts.get(bad)
    with pytest.raises(KeyError):
        roles.get_proposal(bad)
    with pytest.raises(KeyError):
        roles.approve_proposal(bad, reviewed_by="owner")


def test_a_torn_or_foreign_file_does_not_hide_the_other_records(tmp_path: Path) -> None:
    store = CloseoutStore(tmp_path / "closeouts")
    card = draft(CloseoutCoordinator(store))
    (tmp_path / "closeouts" / "closeout-aaaaaaaaaaaa.json").write_text("{")
    (tmp_path / "closeouts" / "notes.json").write_text("[]")
    assert [c.card_id for c in store.list_cards()] == [card.card_id]
    roles = RoleSkillMemoryStore(tmp_path / "mem")
    proposal = roles.propose_skill_write(role_id="sde", content="a", reason="r", proposed_by="x")
    (roles.root / "proposals" / "roles" / "rprop-aaaaaaaaaaaa.json").write_text("{")
    assert [p.proposal_id for p in roles.list_proposals()] == [proposal.proposal_id]


def test_only_one_of_two_rival_approvals_applies(tmp_path: Path) -> None:
    # Each round: two proposals on the same base, approved at the same time through two stores on one root.
    for number in range(10):
        root = tmp_path / str(number)
        stores = [RoleSkillMemoryStore(root), RoleSkillMemoryStore(root)]
        ids = [
            stores[0].propose_skill_write(role_id="sde", content=text, reason="r", proposed_by="x").proposal_id
            for text in ("one\n", "two\n")
        ]
        outcomes: list[str] = []
        ready = threading.Barrier(2)

        def approve(store: RoleSkillMemoryStore, proposal_id: str) -> None:
            ready.wait()
            try:
                outcomes.append(store.approve_proposal(proposal_id, reviewed_by="owner").status)
            except CASConflictError:
                outcomes.append("conflict")

        threads = [threading.Thread(target=approve, args=pair) for pair in zip(stores, ids, strict=True)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert sorted(outcomes) == ["approved", "conflict"]


def test_a_role_write_must_stay_inside_its_own_role(tmp_path: Path) -> None:
    roles = RoleSkillMemoryStore(tmp_path)
    (tmp_path / "roles" / "architect").mkdir(parents=True)
    (tmp_path / "roles" / "sde").symlink_to(tmp_path / "roles" / "architect")
    with pytest.raises(ValueError):
        roles.propose_skill_write(role_id="sde", content="x", reason="r", proposed_by="x")


# --- closeout: idempotent, nothing dropped ----------------------------------------------------------------------


def test_a_failed_sync_is_finished_by_confirming_again(tmp_path: Path) -> None:
    log: list[str] = []
    sink = Recorder(log, fail_once="task:two")
    store = CloseoutStore(tmp_path)
    coord = CloseoutCoordinator(store, chief_sink=sink, taskboard_sink=sink, memory_sink=sink)
    card = draft(
        coord, follow_up_tasks=("one", "two"),
        memory_proposals=(CloseoutMemoryDraft("role://sde/skills.md", "a", "r"),),
    )
    with pytest.raises(RuntimeError):
        coord.confirm_and_sync(card.card_id, confirmed_by="owner")
    assert store.get(card.card_id).status == "confirmed"
    result = coord.confirm_and_sync(card.card_id, confirmed_by="owner")
    assert (result.tasks_created, result.already_synced) == (("task-one", "task-two"), False)
    # The chief hears of it once, after everything else has landed.
    assert log.count("chief") == 1 and log[-1] == "chief"
    assert coord.confirm_and_sync(card.card_id, confirmed_by="owner").already_synced


def test_rival_confirmations_sync_once(tmp_path: Path) -> None:
    log: list[str] = []
    sink = Recorder(log)
    first = CloseoutCoordinator(CloseoutStore(tmp_path), chief_sink=sink)
    second = CloseoutCoordinator(CloseoutStore(tmp_path), chief_sink=sink)
    card = draft(first)
    results: list[bool] = []
    ready = threading.Barrier(2)

    def confirm(coord: CloseoutCoordinator) -> None:
        ready.wait()
        results.append(coord.confirm_and_sync(card.card_id, confirmed_by="owner").already_synced)

    threads = [threading.Thread(target=confirm, args=(c,)) for c in (first, second)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(results) == [False, True]
    assert log == ["chief"]


def test_confirming_refuses_to_drop_what_no_sink_takes(tmp_path: Path) -> None:
    coord = CloseoutCoordinator(CloseoutStore(tmp_path))
    tasks = draft(coord, follow_up_tasks=("one",))
    memory = draft(coord, memory_proposals=(CloseoutMemoryDraft("role://sde/skills.md", "a", "r"),))
    for card in (tasks, memory):
        with pytest.raises(ValueError, match="sink"):
            coord.confirm_and_sync(card.card_id, confirmed_by="owner")


def test_drafts_for_one_file_become_one_proposal(tmp_path: Path) -> None:
    # Both would be queued on the same base, so approving one would make the other a conflict.
    log: list[str] = []
    sink = Recorder(log)
    coord = CloseoutCoordinator(CloseoutStore(tmp_path), memory_sink=sink)
    card = draft(coord, memory_proposals=(
        CloseoutMemoryDraft("role://sde/skills.md", "a", "first"),
        CloseoutMemoryDraft("role://sde/skills.md", "b", "second"),
    ))
    assert card.memory_proposals == (CloseoutMemoryDraft("role://sde/skills.md", "a\nb", "first; second"),)
    coord.confirm_and_sync(card.card_id, confirmed_by="owner")
    assert log == ["memory:role://sde/skills.md:a\nb"]


# --- hub server: authentication, injection, robustness ----------------------------------------------------------


class Hub:
    def __init__(self, server: CollaborationHubServer, roles: RoleSkillMemoryStore, closeouts: CloseoutStore) -> None:
        self.server, self.roles, self.closeouts = server, roles, closeouts

    def call(
        self, path: str, *, method: str = "GET", body: bytes | None = None, token: str | None = TOKEN,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, Any]:
        host, port = self.server.address
        sent = {**({"Authorization": f"Bearer {token}"} if token else {}), **(headers or {})}
        request = urllib.request.Request(f"http://{host}:{port}{path}", data=body, headers=sent, method=method)
        try:
            with urllib.request.urlopen(request, timeout=10) as reply:
                raw = reply.read().decode()
                return reply.status, json.loads(raw) if path.rstrip("/").startswith("/hub/api") or "/api/" in path else raw
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode() or "{}")


@contextmanager
def serving(tmp_path: Path, **over: Any) -> Iterator[Hub]:
    roles, closeouts = RoleSkillMemoryStore(tmp_path / "mem"), CloseoutStore(tmp_path / "closeouts")
    options: dict[str, Any] = {"bearer_token": TOKEN, "reviewer": "owner", **over}
    server = CollaborationHubServer(
        host="127.0.0.1", port=0, role_store=roles, closeout_store=closeouts,
        closeout_coordinator=options.pop("coordinator", None) or CloseoutCoordinator(closeouts), **options,
    )
    server.start_background()
    try:
        yield Hub(server, roles, closeouts)
    finally:
        server.close()


def test_the_api_needs_the_token_and_the_page_carries_no_data(tmp_path: Path) -> None:
    with serving(tmp_path) as hub:
        proposal = hub.roles.propose_skill_write(role_id="sde", content="x", reason="secret-reason", proposed_by="x")
        approve = f"/api/proposals/{proposal.proposal_id}/approve"
        for token in (None, "wrong"):
            for path in ("/api/digest", "/api/proposals", "/api/closeouts", "/api/experts"):
                assert hub.call(path, token=token)[0] == 401
            assert hub.call(approve, method="POST", body=b"{}", token=token)[0] == 401
        assert hub.roles.get_proposal(proposal.proposal_id).status == "pending"
        status, page = hub.call("/", token=None)
        assert status == 200 and "secret-reason" not in page
        assert hub.call("/api/digest")[0] == 200


def test_the_reviewer_is_whoever_the_server_says(tmp_path: Path) -> None:
    with serving(tmp_path) as hub:
        proposal = hub.roles.propose_skill_write(role_id="sde", content="x", reason="r", proposed_by="x")
        body = json.dumps({"reviewer": "somebody-else", "note": "fine"}).encode()
        status, reply = hub.call(f"/api/proposals/{proposal.proposal_id}/approve", method="POST", body=body)
        assert (status, reply["reviewed_by"], reply["review_note"]) == (200, "owner", "fine")


@pytest.mark.parametrize(("body", "expected"), [(b"{", 400), (b"[]", 400), (b"\xff", 400), (b"x" * 70_000, 413)])
def test_bad_bodies_are_answered_and_change_nothing(tmp_path: Path, body: bytes, expected: int) -> None:
    with serving(tmp_path) as hub:
        proposal = hub.roles.propose_skill_write(role_id="sde", content="x", reason="r", proposed_by="x")
        assert hub.call(f"/api/proposals/{proposal.proposal_id}/approve", method="POST", body=body)[0] == expected
        assert hub.roles.get_proposal(proposal.proposal_id).status == "pending"


def test_a_failing_sink_or_source_is_an_answer_not_a_dropped_connection(tmp_path: Path) -> None:
    class Down:
        def list_tasks(self) -> list[dict[str, Any]]:
            raise RuntimeError("secret detail")

        def list_runs(self) -> list[dict[str, Any]]:
            return [{"status": "done", "cost_usd": 1.0}]

        def list_project_proposals(self) -> list[dict[str, Any]]:
            return []

    closeouts = CloseoutStore(tmp_path / "closeouts")
    coord = CloseoutCoordinator(closeouts, chief_sink=Recorder([], fail_once="chief"))
    with serving(tmp_path, coordinator=coord, state_provider=Down()) as hub:
        status, digest = hub.call("/api/digest")
        assert status == 200 and digest["cost_summary"]["total_runs"] == 1
        assert "tasks" in digest["window_label"] and "secret detail" not in json.dumps(digest)
        card = draft(coord)
        path = f"/api/closeouts/{card.card_id}/confirm"
        assert hub.call(path, method="POST", body=b"{}")[0] == 502
        assert hub.call(path, method="POST", body=b"{}")[0] == 200


def test_the_page_builds_no_markup_from_data(tmp_path: Path) -> None:
    with serving(tmp_path) as hub:
        page = hub.call("/", token=None)[1]
        assert "innerHTML" not in page and "onclick" not in page


def test_the_page_forbids_framing_and_outside_sources(tmp_path: Path) -> None:
    with serving(tmp_path) as hub:
        host, port = hub.server.address
        with urllib.request.urlopen(f"http://{host}:{port}/", timeout=10) as reply:
            policy = reply.headers["Content-Security-Policy"]
            assert "default-src 'none'" in policy and "frame-ancestors 'none'" in policy
            assert reply.headers["X-Content-Type-Options"] == "nosniff"


def test_the_hub_works_below_a_path_prefix(tmp_path: Path) -> None:
    with serving(tmp_path, base_path="/hub") as hub:
        assert hub.call("/hub/api/digest")[0] == 200
        # A proxy that strips the prefix.
        assert hub.call("/api/digest")[0] == 200
        page = hub.call("/hub/", token=None)[1]
        assert "'/api/" not in page
        assert hub.call("/hubris/api/digest")[0] == 404


def test_close_without_start_does_not_hang(tmp_path: Path) -> None:
    roles, closeouts = RoleSkillMemoryStore(tmp_path / "mem"), CloseoutStore(tmp_path / "closeouts")
    server = CollaborationHubServer(
        host="127.0.0.1", port=0, role_store=roles, closeout_store=closeouts,
        closeout_coordinator=CloseoutCoordinator(closeouts),
    )
    closer = threading.Thread(target=server.close, daemon=True)
    closer.start()
    closer.join(timeout=5)
    assert not closer.is_alive()
