from __future__ import annotations

import pytest

from taskboard.core.dag import assert_no_cycle, compile_flow_plan, detect_cycle
from taskboard.core.errors import (
    BlockedByDependencyError,
    CycleDetectedError,
    IllegalTransitionError,
    UpstreamTaskNotFoundError,
)
from taskboard.core.merge import apply_manual_patch, merge_external_update
from taskboard.core.models import FlowPlan, FlowStepSpec, TaskItem, TaskStatus, new_task_id
from taskboard.core.state_machine import validate_transition


@pytest.mark.parametrize(
    ("current", "target"),
    [
        ("backlog", "ready"),
        ("backlog", "cancelled"),
        ("ready", "in_progress"),
        ("ready", "backlog"),
        ("ready", "cancelled"),
        ("in_progress", "done"),
        ("in_progress", "ready"),
        ("in_progress", "cancelled"),
        ("in_progress", "in_progress"),
    ],
)
def test_legal_transitions_without_blockers(current: TaskStatus, target: TaskStatus) -> None:
    validate_transition(current, target, task_id="tb_tsk_1", incomplete_blockers=0)


@pytest.mark.parametrize(
    ("current", "target"),
    [
        ("backlog", "in_progress"),
        ("backlog", "done"),
        ("ready", "done"),
        ("in_progress", "backlog"),
        ("done", "backlog"),
        ("done", "cancelled"),
        ("cancelled", "ready"),
        ("cancelled", "in_progress"),
        ("cancelled", "done"),
    ],
)
def test_illegal_transitions_rejected(current: TaskStatus, target: TaskStatus) -> None:
    with pytest.raises(IllegalTransitionError):
        validate_transition(current, target, task_id="tb_tsk_1")


def test_reopen_transitions_require_explicit_flag() -> None:
    with pytest.raises(IllegalTransitionError):
        validate_transition("done", "in_progress", reopen=False)
    with pytest.raises(IllegalTransitionError):
        validate_transition("done", "ready", reopen=False)
    with pytest.raises(IllegalTransitionError):
        validate_transition("cancelled", "backlog", reopen=False)

    validate_transition("done", "in_progress", reopen=True)
    validate_transition("done", "ready", reopen=True)
    validate_transition("cancelled", "backlog", reopen=True)


def test_terminal_status_rejects_late_running_update() -> None:
    for terminal in ("done", "cancelled"):
        with pytest.raises(IllegalTransitionError):
            validate_transition(
                terminal,  # type: ignore[arg-type]
                "in_progress",
                reopen=True,
                is_late_running=True,
            )


def test_dependency_guards_on_ready_and_in_progress() -> None:
    with pytest.raises(BlockedByDependencyError) as exc_ready:
        validate_transition(
            "backlog",
            "ready",
            task_id="tb_tsk_down",
            incomplete_blockers=1,
            blocking_task_ids=("tb_tsk_up1",),
        )
    assert exc_ready.value.blocking_task_ids == ("tb_tsk_up1",)

    with pytest.raises(BlockedByDependencyError) as exc_start:
        validate_transition(
            "ready",
            "in_progress",
            task_id="tb_tsk_down",
            incomplete_blockers=2,
            blocking_task_ids=("tb_tsk_up1", "tb_tsk_up2"),
        )
    assert exc_start.value.blocking_task_ids == ("tb_tsk_up1", "tb_tsk_up2")


def test_three_layer_merge_manual_override_and_unlock() -> None:
    task = TaskItem(
        task_id=new_task_id(),
        title="Original",
        description="Desc 1",
        scope="proj:hibernaut",
        priority=2,
        assignee="agent:default",
        last_source_event_at=1000,
    )

    manual = apply_manual_patch(task, {"priority": 0, "assignee": "human:kun"})
    assert manual.priority == 0
    assert manual.assignee == "human:kun"
    assert {"priority", "assignee"} <= manual.manual_override_fields

    # External update skips locked Layer 2 fields, still updates Layer 1 and ignores Layer 3.
    merged = merge_external_update(
        manual,
        {
            "title": "Updated Title",
            "priority": 3,
            "assignee": "agent:other",
            "task_type": "bugfix",
            "status": "done",
        },
        occurred_at_ms=2000,
    )
    assert merged.applied is True
    assert merged.stale_ignored is False
    assert merged.skipped_override_fields == ("assignee", "priority")
    assert merged.task.title == "Updated Title"
    assert merged.task.priority == 0
    assert merged.task.assignee == "human:kun"
    assert merged.task.task_type == "bugfix"
    assert merged.task.status == "backlog"
    assert merged.task.last_source_event_at == 2000

    # Unlocking priority restores external sync for priority only.
    unlocked = apply_manual_patch(merged.task, {}, unlock_fields=("priority",))
    assert "priority" not in unlocked.manual_override_fields
    assert "assignee" in unlocked.manual_override_fields

    merged2 = merge_external_update(
        unlocked,
        {"priority": 1, "assignee": "agent:bot"},
        occurred_at_ms=3000,
    )
    assert merged2.task.priority == 1
    assert merged2.task.assignee == "human:kun"
    assert merged2.skipped_override_fields == ("assignee",)


def test_out_of_order_watermark_gate_is_noop() -> None:
    task = TaskItem(
        task_id=new_task_id(),
        title="Newer",
        last_source_event_at=5000,
    )
    res = merge_external_update(task, {"title": "Stale"}, occurred_at_ms=4999)
    assert res.applied is False
    assert res.stale_ignored is True
    assert res.task.title == "Newer"
    assert res.task.last_source_event_at == 5000


def test_dfs_cycle_detection_self_loop_and_multi_hop() -> None:
    # Self-loop
    assert detect_cycle("A", "A", lambda _: ()) == ("A", "A")
    with pytest.raises(CycleDetectedError) as self_exc:
        assert_no_cycle("A", "A", lambda _: ())
    assert self_exc.value.cycle_path == ("A", "A")

    # Multi-hop cycle: A -> B -> C, adding C -> A closes cycle (C -> A -> B -> C)
    graph = {"A": ["B"], "B": ["C"], "C": []}
    with pytest.raises(CycleDetectedError) as hop_exc:
        assert_no_cycle("C", "A", lambda n: graph.get(n, ()))
    assert hop_exc.value.cycle_path == ("C", "A", "B", "C")

    # Acyclic diamond: A -> B, A -> C, B -> D, adding C -> D is valid
    diamond = {"A": ["B", "C"], "B": ["D"], "C": [], "D": []}
    assert detect_cycle("C", "D", lambda n: diamond.get(n, ())) is None


def test_flow_plan_default_serial_and_explicit_parallel_join() -> None:
    serial_plan = FlowPlan(
        title="Serial Flow",
        scope="proj:hibernaut",
        initial_status="ready",
        steps=(
            FlowStepSpec(id="s1", title="Step 1"),
            FlowStepSpec(id="s2", title="Step 2"),
            FlowStepSpec(id="s3", title="Step 3"),
        ),
    )
    parent, tasks, edges = compile_flow_plan(serial_plan)
    assert len(tasks) == 3
    assert all(t.parent_task_id == parent.task_id for t in tasks)
    assert [t.status for t in tasks] == ["ready", "backlog", "backlog"]
    assert [(e.upstream_task_id, e.downstream_task_id) for e in edges] == [
        (tasks[0].task_id, tasks[1].task_id),
        (tasks[1].task_id, tasks[2].task_id),
    ]

    parallel_plan = FlowPlan(
        title="Fork-Join Flow",
        scope="proj:hibernaut",
        initial_status="ready",
        steps=(
            FlowStepSpec(id="checkout", title="Checkout", after=()),
            FlowStepSpec(id="codegen", title="Codegen", after=("checkout",)),
            FlowStepSpec(id="unittest", title="Unit Test", after=("codegen",)),
            FlowStepSpec(id="docgen", title="Docs", after=("codegen",)),
            FlowStepSpec(
                id="review",
                title="Human Review",
                assignee="human:group:tech_leads",
                after=("unittest", "docgen"),
            ),
        ),
    )
    _, p_tasks, p_edges = compile_flow_plan(parallel_plan)
    by_title = {t.title: t for t in p_tasks}
    assert by_title["Checkout"].status == "ready"
    assert by_title["Human Review"].is_human_assignee()
    assert len(p_edges) == 5
    edge_pairs = {(e.upstream_task_id, e.downstream_task_id) for e in p_edges}
    assert (by_title["Unit Test"].task_id, by_title["Human Review"].task_id) in edge_pairs
    assert (by_title["Docs"].task_id, by_title["Human Review"].task_id) in edge_pairs

    bad_plan = FlowPlan(
        title="Missing Ref",
        scope="proj:hibernaut",
        steps=(FlowStepSpec(id="s1", title="S1", after=("ghost",)),),
    )
    with pytest.raises(UpstreamTaskNotFoundError):
        compile_flow_plan(bad_plan)
