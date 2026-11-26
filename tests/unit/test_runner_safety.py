from __future__ import annotations

from pathlib import Path

import pytest

from runner import supervisor as supervisor_module


def _proc_stat(root: Path, pid: int, state: str, pgrp: int, threads: int = 1, comm: str = "sh") -> None:
    (root / str(pid)).mkdir(parents=True)
    rest = [state, "1", str(pgrp)] + ["0"] * 14 + [str(threads), "0", "0"]
    (root / str(pid) / "stat").write_text(f"{pid} ({comm}) {' '.join(rest)}\n", encoding="utf-8")


@pytest.mark.parametrize(
    ("members", "gone"),
    [
        ([(11, "Z", 11, 1), (12, "Z", 11, 1)], True),
        ([(11, "Z", 11, 1), (12, "S", 11, 1)], False),
        ([(11, "Z", 11, 3)], False),  # a leader thread exited while others still run
        ([(21, "Z", 21, 1)], False),  # nothing of the group is visible: the signal decides, not this scan
        ([(11, "X", 11, 1), (21, "R", 21, 1)], True),
    ],
)
def test_group_counts_as_gone_only_when_every_visible_member_is_a_zombie(
    tmp_path: Path, members: list[tuple[int, str, int, int]], gone: bool
) -> None:
    (tmp_path / "self").mkdir()
    for pid, state, pgrp, threads in members:
        _proc_stat(tmp_path, pid, state, pgrp, threads, comm="a) b (c")
    assert supervisor_module.group_is_only_zombies(11, proc_root=tmp_path) is gone


def test_group_is_not_called_gone_where_there_is_no_proc(tmp_path: Path) -> None:
    assert supervisor_module.group_is_only_zombies(11, proc_root=tmp_path / "missing") is False
