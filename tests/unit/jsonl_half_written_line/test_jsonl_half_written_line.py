import json
from pathlib import Path

from tests.integration import test_system_steward_e2e as e2e
from tests.support.jsonl import read_jsonl

DONE = {"role": "steward", "answers": [[False, "{}"]]}
WRITING = json.dumps({"role": "steward", "mode": "print", "prompt": "x" * 2000})


def _cut(path: Path, *whole: object, part: str) -> Path:
    # What a poll sees while a fake CLI is appending its record: the earlier lines whole, the last one cut short.
    path.write_text("".join(json.dumps(w) + "\n" for w in whole) + part)
    return path


def test_a_half_written_last_line_is_not_read_yet(tmp_path: Path) -> None:
    log = _cut(tmp_path / "calls.jsonl", DONE, part=WRITING[:1402])
    assert read_jsonl(log) == [DONE]
    with log.open("a") as stream:
        stream.write(WRITING[1402:] + "\n")
    assert [r["role"] for r in read_jsonl(log)] == ["steward", "steward"]


def test_a_missing_log_has_no_records(tmp_path: Path) -> None:
    assert read_jsonl(tmp_path / "absent.jsonl") == []


def test_the_steward_end_to_end_polls_survive_a_half_written_line(tmp_path: Path) -> None:
    # CI runs 46007850893 and 46019939615 failed here: "Unterminated string starting at: line 1 column 1403".
    case = e2e.EndToEndTests("test_r9_a_result_the_chat_s_steward_asked_for_comes_back_to_it")
    case.log = _cut(tmp_path / "claude.jsonl", DONE, part=WRITING[:1402])
    case.state = tmp_path / "state"
    case.state.mkdir()
    _cut(case.state / "calls.jsonl", ["gh", "api", "repos/acme/tool"], part='["orca", "wor')
    assert case.answers() == [[False, "{}"]]
    assert case.commands("gh") == [["gh", "api", "repos/acme/tool"]]
