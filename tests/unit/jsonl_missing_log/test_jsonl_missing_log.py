from pathlib import Path

from tests.support.jsonl import read_jsonl


def test_a_missing_log_has_no_records(tmp_path: Path) -> None:
    assert read_jsonl(tmp_path / "absent.jsonl") == []
