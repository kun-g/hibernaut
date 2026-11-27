from pathlib import Path

from tests.support.jsonl import read_jsonl
from tests.support.jsonl_cut import DONE, WRITING, cut


def test_a_half_written_last_line_is_not_read_yet(tmp_path: Path) -> None:
    log = cut(tmp_path / "calls.jsonl", DONE, part=WRITING[:1402])
    assert read_jsonl(log) == [DONE]
    with log.open("a") as stream:
        stream.write(WRITING[1402:] + "\n")
    assert [r["role"] for r in read_jsonl(log)] == ["steward", "steward"]
