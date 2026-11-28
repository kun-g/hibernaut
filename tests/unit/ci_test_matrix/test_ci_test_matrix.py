from pathlib import Path

from scripts.ci_test_matrix import build_matrix


def test_full_discovery_excludes_exactly_the_files_assigned_to_the_second_runner(tmp_path: Path) -> None:
    directory = tmp_path / "tests/integration"
    directory.mkdir(parents=True)
    for index in range(13):
        (directory / f"test_{index}.py").touch()
    primary, second = build_matrix(tmp_path, None)["include"]
    assert primary["args"] == [f"--ignore={path}" for path in second["args"]]
    assert len(second["args"]) == 7
    assert primary["mode"] == second["mode"] == "parallel"


def test_affected_files_run_exactly_once_and_small_changes_use_one_runner(tmp_path: Path) -> None:
    files = tuple(f"tests/integration/test_{i}.py" for i in range(16)) + ("tests/unit/test_only.py",)
    for count in range(len(files) + 1):
        selection = files[:count]
        lanes = build_matrix(tmp_path, selection)["include"]
        assert sorted(path for lane in lanes for path in lane["args"]) == sorted(selection)
        assert len(lanes) == (2 if count >= 12 else 1)
        assert lanes[0]["mode"] == ("skip" if count == 0 else "serial" if count < 4 else "parallel")
