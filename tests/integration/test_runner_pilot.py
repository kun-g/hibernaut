from __future__ import annotations

import asyncio
import os
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from agent_runtime.adapters.runner_pilot import RunnerPilotRelay
from runner.protocol_check import PROTOCOL_CHECK_EXECUTOR_ID, PROTOCOL_FILES
from tests.support.runner_pilot_support import RUNNER_ID, alive, commit, git, recomputed_manifest
from tests.support.runner_pilot import Deployment, KEY, OTHER, PILOT, _relay, _runner_run, pilot


def test_relay_listens_on_loopback_only_and_reports_a_taken_port(tmp_path: Path) -> None:
    async def scenario() -> None:
        relay = _relay(tmp_path)
        assert not relay.listening
        await relay.start()
        listing = subprocess.run(
            ["lsof", "-nP", "-a", "-p", str(os.getpid()), "-iTCP", "-sTCP:LISTEN", "-Fn"], stdout=subprocess.PIPE, text=True
        ).stdout
        port = relay._port  # pyright: ignore[reportPrivateUsage]
        assert [line for line in listing.splitlines() if line.endswith(f":{port}")] == [f"n127.0.0.1:{port}"]
        second = RunnerPilotRelay(
            runner_id=RUNNER_ID, project_key="hibernaut", port=port, token_file=tmp_path / "relay.token"
        )
        with pytest.raises(OSError):
            await second.start()
        assert not second.listening
        await relay.stop()
        assert not relay.listening
        await second.start()
        await second.stop()

    asyncio.run(asyncio.wait_for(scenario(), 20))


@contextmanager
def hold_protocol_check(gate: Path) -> Iterator[Path]:
    ready = gate.with_suffix(".ready")
    ready.unlink(missing_ok=True)
    os.mkfifo(gate)
    # The runner publishes ready after opening its read end. RDWR also lets cleanup release a killed reader.
    with os.fdopen(os.open(gate, os.O_RDWR), "wb", buffering=0) as pipe:
        try:
            yield ready
        finally:
            gate.unlink()
            pipe.write(b"resume\n")


def test_a_task_is_checked_by_the_standalone_runner_and_traceable_from_the_ledger(pilot: Deployment) -> None:
    pilot.write(enabled=True)
    pilot.start_hb()
    pilot.start_runner()
    # Queued while the node is still connecting; waits for it instead of failing.
    event = pilot.check(key=KEY)
    pilot.finished(event)
    (text,) = pilot.outbox(PILOT)
    assert pilot.commit in text and recomputed_manifest(pilot.runner_repo, pilot.commit) in text
    recipe = text.split("```sh\n", 1)[1].split("\n```", 1)[0]
    digest = subprocess.check_output(["sh", "-c", recipe], cwd=pilot.runner_repo, text=True).split()[0]
    assert digest == recomputed_manifest(pilot.runner_repo, pilot.commit)
    (run,) = pilot.runs()
    assert (run["runner_id"], run["outcome_kind"], run["emit_status"], run["executor_id"]) == (
        RUNNER_ID, "emit", "done", PROTOCOL_CHECK_EXECUTOR_ID)
    assert run["exit_code"] == 0 and _runner_run(pilot, run).is_file()
    assert len(git("worktree", "list", cwd=pilot.runner_repo).splitlines()) == 1

    # A commit without the five files: a failed result, no partial digest.
    (pilot.runner_repo / PROTOCOL_FILES[0]).unlink()
    bad = commit(pilot.runner_repo, "missing file")
    pilot.finished(pilot.post(PILOT, candidate_commit=bad))
    assert "runner.capabilities.schema.json is missing" in pilot.outbox(PILOT)[-1]
    assert (pilot.runs()[-1]["emit_status"], pilot.runs()[-1]["runner_id"]) == ("failed", RUNNER_ID)

    # Only this inbox and only this executor are on the route; everything else keeps its path.
    pilot.assert_original_path(OTHER)
    pilot.assert_original_path("proj:hibernaut")
    elsewhere = pilot.check(OTHER, executor_id=PROTOCOL_CHECK_EXECUTOR_ID)
    pilot.finished(elsewhere)
    assert "试点路由未对本 Inbox 开放" in pilot.outbox(OTHER)[-1]
    foreign = pilot.check(executor_id="cli")
    pilot.finished(pilot.check())
    assert pilot.status(foreign) == "pending"
    assert all(r["executor_id"] == PROTOCOL_CHECK_EXECUTOR_ID for r in pilot.runs())
    pilot.assert_original_path(OTHER)


def test_a_new_event_interrupts_the_check_and_leaves_no_partial_result(pilot: Deployment) -> None:
    pilot.write(enabled=True)
    pilot.start_hb()
    pilot.start_runner()
    held = pilot.check(key=KEY, hold_seconds=60)
    worker = pilot.worker()
    assert pilot.outbox(PILOT) == []
    follow_up = pilot.check(key=KEY)
    pilot.finished(held, follow_up)
    assert not alive(worker)
    interrupted, final = pilot.runs()
    assert (interrupted["outcome_kind"], interrupted["runner_id"]) == ("interrupted", RUNNER_ID)
    assert _runner_run(pilot, interrupted).is_file()
    assert (final["outcome_kind"], final["emit_status"], final["runner_id"]) == ("emit", "done", RUNNER_ID)
    (text,) = pilot.outbox(PILOT)
    assert recomputed_manifest(pilot.runner_repo, pilot.commit) in text and interrupted["run_id"] not in text
    assert len(git("worktree", "list", cwd=pilot.runner_repo).splitlines()) == 1
    pilot.assert_original_path(OTHER)


def test_a_forced_stop_ends_the_worker_process_and_the_next_start_finishes_the_task(
    pilot: Deployment, monkeypatch: pytest.MonkeyPatch
) -> None:
    pilot.write(enabled=True)
    pilot.start_hb(fast_retry=True)
    gate = pilot.start_gated_runner(monkeypatch, fast_retry=True)
    with hold_protocol_check(gate) as entered:
        held = pilot.check(key=KEY)
        worker = pilot.gated_worker(entered)
        pilot.force_stop_hb()
        assert not alive(worker)
        assert (pilot.status(held), pilot.outbox(PILOT)) == ("pending", [])
        (run,) = pilot.runs()
        assert (run["outcome_kind"], run["runner_id"]) == ("interrupted", RUNNER_ID)
        assert _runner_run(pilot, run).is_file()
    # Same port, same token; the Runner reconnects on its own.
    pilot.start_hb(fast_retry=True)
    pilot.finished(held, timeout=60)
    assert len(pilot.outbox(PILOT)) == 1 and pilot.runs()[-1]["emit_status"] == "done"


def test_stopping_the_runner_service_fails_the_task_and_frees_everything(pilot: Deployment) -> None:
    pilot.write(enabled=True)
    pilot.start_hb()
    pilot.start_runner()
    held = pilot.check(hold_seconds=60)
    worker = pilot.worker()
    assert pilot.stop_runner() == 0
    assert not alive(worker)
    pilot.finished(held)
    (run,) = pilot.runs()
    assert (run["outcome_kind"], run["emit_status"], run["error_kind"], run["runner_id"]) == (
        "emit", "failed", "network", RUNNER_ID)
    assert "校验通过" not in "".join(pilot.outbox(PILOT))
    pilot.assert_original_path(OTHER)
    # The supervisor (launchd) starts it again; the next task runs.
    pilot.start_runner()
    pilot.finished(pilot.check())
    assert pilot.runs()[-1]["emit_status"] == "done"


def test_turning_the_route_off_drains_it_and_restores_the_original_path(
    pilot: Deployment, monkeypatch: pytest.MonkeyPatch
) -> None:
    pilot.write(enabled=False)
    pilot.start_hb(fast_retry=True)
    gate = pilot.start_gated_runner(monkeypatch, fast_retry=True)
    assert not pilot.relay_open()
    ordinary_key = "github:fixture/test#9000"
    pilot.assert_original_path(PILOT, key=ordinary_key)  # off: the pilot inbox is an ordinary inbox

    pilot.reload(enabled=True)
    assert pilot.relay_open()
    pilot.finished(pilot.check())
    with hold_protocol_check(gate) as entered:
        in_flight = pilot.check(key=KEY)
        worker = pilot.gated_worker(entered)
        pilot.reload(enabled=False)
        # The Run in flight keeps its Relay until it has ended, on its own terms.
        assert alive(worker) and pilot.relay_open()
    pilot.finished(in_flight)
    assert recomputed_manifest(pilot.runner_repo, pilot.commit) in pilot.outbox(PILOT)[-1]
    pilot.until(lambda: not pilot.relay_open())
    assert pilot.runs()[-1]["runner_id"] == RUNNER_ID
    pilot.assert_original_path(PILOT)
    # A session of the pilot must not keep pointing its correlation key at the closed route.
    pilot.assert_original_path(PILOT, key=KEY)
    # Leftover work addressed to the check ends with a notice. An existing ordinary task can then continue.
    pilot.finished(pilot.check(executor_id=PROTOCOL_CHECK_EXECUTOR_ID))
    assert "试点路由未对本 Inbox 开放" in pilot.outbox(PILOT)[-1]
    pilot.assert_original_path(PILOT, key=ordinary_key)

    pilot.reload(enabled=True)
    assert pilot.runner is not None and pilot.runner.poll() is None
    pilot.finished(pilot.check())
    assert pilot.runs()[-1]["emit_status"] == "done"
    pilot.reload(enabled=False)
    pilot.until(lambda: not pilot.relay_open())
    pilot.assert_original_path(PILOT)


def test_interrupted_work_left_by_a_rollback_is_not_run_elsewhere(pilot: Deployment) -> None:
    # Stopped by force with the route on, started with it off.
    pilot.write(enabled=True)
    pilot.start_hb()
    pilot.start_runner()
    held = pilot.check(key=KEY, hold_seconds=60)
    worker = pilot.worker()
    pilot.force_stop_hb()
    assert not alive(worker)
    assert pilot.stop_runner() == 0
    pilot.write(enabled=False)
    pilot.start_hb()
    pilot.finished(held)
    (text,) = pilot.outbox(PILOT)
    assert "试点路由未对本 Inbox 开放" in text
    pilot.finished(pilot.post(PILOT, key=KEY))
    assert pilot.outbox(PILOT)[-1] == f"original path: t{pilot.seq}"


def test_an_unusable_relay_holds_the_pilot_inbox_and_nothing_else(pilot: Deployment) -> None:
    (pilot.root / "relay.token").chmod(0o644)
    pilot.write(enabled=True)
    pilot.start_hb()
    waiting = pilot.check()
    pilot.assert_original_path(OTHER)
    assert pilot.status(waiting) == "pending" and not pilot.relay_open()
    assert "chmod 600" in pilot.hb_log.read_text()
    # Fixed: any reload opens it, and the held task runs.
    (pilot.root / "relay.token").chmod(0o600)
    pilot.start_runner()
    pilot.write(enabled=True, extra='[executors.extra]\ncommand = "true"\n')
    pilot.finished(waiting)
    assert pilot.runs()[-1]["emit_status"] == "done"


def test_the_route_cannot_be_changed_while_open_or_opened_on_a_taken_port(pilot: Deployment) -> None:
    import socket

    with socket.socket() as taken:
        taken.bind(("127.0.0.1", pilot.relay_port))
        taken.listen()
        pilot.write(enabled=False)
        pilot.start_hb()
        pilot.reload(enabled=True, expect="Cannot activate settings")
        pilot.assert_original_path(PILOT)
    # The relay can already be listening while its generation is still being activated.
    pilot.reload(enabled=True, extra="timeout_seconds = 60\n")
    assert pilot.relay_open()
    pilot.reload(enabled=True, extra="timeout_seconds = 61\n", expect="changes need enabled = false first")
    assert pilot.relay_open()
    pilot.assert_original_path(OTHER)
