from pathlib import Path

from tests.integration import test_system_steward_e2e as e2e
from tests.support.jsonl_cut import DONE, WRITING, cut


def test_the_steward_end_to_end_polls_survive_a_half_written_line(tmp_path: Path) -> None:
    # CI runs 46007850893 and 46019939615 failed here: "Unterminated string starting at: line 1 column 1403".
    case = e2e.EndToEndTests("test_r9_a_result_the_chat_s_steward_asked_for_comes_back_to_it")
    case.log = cut(tmp_path / "claude.jsonl", DONE, part=WRITING[:1402])
    case.state = tmp_path / "state"
    case.state.mkdir()
    cut(case.state / "calls.jsonl", ["gh", "api", "repos/acme/tool"], part='["orca", "wor')
    assert case.answers() == [[False, "{}"]]
    assert case.commands("gh") == [["gh", "api", "repos/acme/tool"]]
