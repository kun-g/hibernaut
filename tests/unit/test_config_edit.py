from __future__ import annotations

import os
import stat
import tempfile
import threading
import tomllib
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_runtime.app.config_edit import ConfigChanged, ConfigFile, EditRefused, TomlEdit, quoted

TEXT = '''# top comment
[runtime]
db_path = "./x.db"   # keep me

[[runtime.inbox_bindings]]
pattern = "user:main"
note = """
[projects.fake]
forward_to = ["not this one"]
"""
forward_to = ["proj:a"]   # why
[[runtime.inbox_bindings]]
pattern = "user:other"
forward_to = [
  "proj:a", # first
  "proj:b",
  # last words
]

[projects.a]
repo = "o/a"
args = [["x", "]"], ['[projects.b]']]
[projects.a.delivery]
open_pr = true

# about b
[projects."b"]   # quoted
repo = "o/b"
'''


def kept(before: str, after: str) -> bool:
    lines = iter(after.splitlines(keepends=True))
    return all(any(line == other for other in lines) for line in before.splitlines(keepends=True))


class TomlEditTests(unittest.TestCase):
    def test_c5_a_description_goes_under_the_header_and_every_line_stays(self) -> None:
        edit = TomlEdit(TEXT)
        edit.add_key(("projects", "b"), "description", "what b is")
        after = edit.result()
        self.assertTrue(kept(TEXT, after))
        self.assertIn('[projects."b"]   # quoted\ndescription = "what b is"\nrepo = "o/b"\n', after)
        self.assertEqual(tomllib.loads(after)["projects"]["b"]["description"], "what b is")

    def test_c5_a_binding_goes_behind_the_project_s_own_tables(self) -> None:
        edit = TomlEdit(TEXT)
        edit.add_table(("projects", "a"), "inbox_bindings", {"pattern": "proj:a", "agents": ["x"]})
        after = edit.result()
        self.assertTrue(kept(TEXT, after))
        self.assertLess(after.index("open_pr"), after.index("[[projects.a.inbox_bindings]]"))
        self.assertLess(after.index("[[projects.a.inbox_bindings]]"), after.index("# about b"))
        self.assertEqual(tomllib.loads(after)["projects"]["a"]["inbox_bindings"], [{"pattern": "proj:a", "agents": ["x"]}])

    def test_c5_two_bindings_keep_their_order(self) -> None:
        edit = TomlEdit(TEXT)
        for pattern in ("first", "second"):
            edit.add_table(("projects", "a"), "inbox_bindings", {"pattern": pattern})
        edit.add_key(("projects", "a"), "description", "what a is")
        found = tomllib.loads(edit.result())["projects"]["a"]["inbox_bindings"]
        self.assertEqual([b["pattern"] for b in found], ["first", "second"])

    def test_c5_a_list_on_one_line_grows_in_place_and_keeps_its_comment(self) -> None:
        edit = TomlEdit(TEXT)
        edit.append_item(("runtime", "inbox_bindings"), 0, "forward_to", "proj:b")
        after = edit.result()
        self.assertIn('forward_to = ["proj:a", "proj:b"]   # why\n', after)
        self.assertEqual(after.replace(', "proj:b"]   # why', ']   # why'), TEXT)

    def test_c5_a_list_over_lines_gets_a_line_of_its_own(self) -> None:
        edit = TomlEdit(TEXT)
        edit.append_item(("runtime", "inbox_bindings"), 1, "forward_to", "proj:c")
        after = edit.result()
        self.assertTrue(kept(TEXT, after))
        self.assertIn('  "proj:b",\n  "proj:c",\n  # last words\n]', after)

    def test_c5_a_list_without_a_last_comma(self) -> None:
        for before, after in (
            ('[[x]]\nf = ["a"]', '[[x]]\nf = ["a", "n"]'),
            ('[[x]]\nf = ["a", ]\n', '[[x]]\nf = ["a", "n", ]\n'),
            ("[[x]]\nf = [\n  'a',\n  'b' # last\n]\n", "[[x]]\nf = [\n  'a',\n  'b', \"n\" # last\n]\n"),
            ('[[x]]\nf = ["a", "b#]"]\n', '[[x]]\nf = ["a", "b#]", "n"]\n'),
            ('[[x]]\nf = ["""a""", \'\'\'b\'\'\']\n', '[[x]]\nf = ["""a""", \'\'\'b\'\'\', "n"]\n'),
        ):
            with self.subTest(before=before):
                edit = TomlEdit(before)
                edit.append_item(("x",), 0, "f", "n")
                self.assertEqual(edit.result(), after)

    def test_c5_line_endings_and_a_missing_last_newline_stay(self) -> None:
        for text in ("[a]\r\nx = 1\r\n", "[a]\r\nx = 1"):
            edit = TomlEdit(text)
            edit.add_key(("a",), "y", "z")
            edit.append_text("[b]\nc = 1\n")
            after = edit.result()
            self.assertEqual(after.count("\n"), after.count("\r\n"))
            self.assertEqual(tomllib.loads(after), {"a": {"x": 1, "y": "z"}, "b": {"c": 1}})
        edit = TomlEdit("[a]")
        edit.add_key(("a",), "y", "z")
        self.assertEqual(tomllib.loads(edit.result()), {"a": {"y": "z"}})

    def test_r8_what_is_written_another_way_is_left_to_a_person(self) -> None:
        for text in ('projects = {a = {repo = "o/a"}}\n', '[projects]\na.repo = "o/a"\n', 'projects.a.repo = "o/a"\n'):
            with self.subTest(text=text), self.assertRaises(EditRefused):
                TomlEdit(text).add_key(("projects", "a"), "description", "x")
        with self.assertRaises(EditRefused):
            TomlEdit('[runtime]\ninbox_bindings = [{forward_to = ["a"]}]\n').append_item(
                ("runtime", "inbox_bindings"), 0, "forward_to", "b")
        with self.assertRaises(EditRefused):
            TomlEdit(TEXT).append_item(("runtime", "inbox_bindings"), 2, "forward_to", "b")
        with self.assertRaises(EditRefused):
            TomlEdit('[[runtime.inbox_bindings]]\nforward_to = []\n').append_item(
                ("runtime", "inbox_bindings"), 0, "forward_to", "b")

    def test_c1_the_result_differs_by_the_named_values_only(self) -> None:
        # A value that would end its string writes nothing, whatever wrote it.
        edit = TomlEdit(TEXT)
        with patch("agent_runtime.app.config_edit.quoted", lambda v: f'"{v}"'):
            edit.add_key(("projects", "a"), "description", 'x"\ncheckout = "/elsewhere')
        with self.assertRaises(EditRefused):
            edit.result()


class QuotedTests(unittest.TestCase):
    def test_c1_every_printable_value_reads_back_as_itself(self) -> None:
        for value in ('"', "\\", "]", '"""', "'", "#", "a = 1", '\\"]\\', "说明：甲", "{x}", "\\u0041", "[projects.x]"):
            with self.subTest(value=value):
                self.assertEqual(tomllib.loads(f"x = {quoted(value)}")["x"], value)

    def test_c1_line_breaks_and_control_characters_are_refused(self) -> None:
        for value in ("a\nb", "a\rb", "a\tb", "\0", "\x7f", "a b", "\x1b[0m", "\ud800", "a‮b"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                quoted(value)


class ConfigFileTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.path = self.root / "hibernaut.toml"
        self.path.write_text("a = 1\n")
        self.path.chmod(0o600)

    def names(self) -> list[str]:
        return sorted(p.name for p in self.root.iterdir())

    def test_c5_the_old_file_is_backed_up_and_the_new_one_replaces_it_at_once(self) -> None:
        file = ConfigFile(self.path)
        with file.locked():
            backup = file.replace(b"a = 1\n", b"a = 2\n")
        self.assertEqual((self.path.read_bytes(), backup.read_bytes()), (b"a = 2\n", b"a = 1\n"))
        self.assertEqual(stat.S_IMODE(backup.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)
        self.assertEqual(len(self.names()), 2)
        self.assertTrue(file.owns(backup))
        self.assertTrue(file.owns(self.path))
        self.assertFalse(file.owns(self.root / "README.md"))

    def test_c5_a_wider_mode_of_the_file_does_not_reach_the_backup(self) -> None:
        self.path.chmod(0o644)
        file = ConfigFile(self.path)
        with file.locked():
            backup = file.replace(b"a = 1\n", b"a = 2\n")
        self.assertEqual(stat.S_IMODE(backup.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o644)

    def test_c5_a_file_a_person_changed_meanwhile_is_left_alone(self) -> None:
        file = ConfigFile(self.path)
        with file.locked():
            self.path.write_text("a = 9\n")
            with self.assertRaises(ConfigChanged):
                file.replace(b"a = 1\n", b"a = 2\n")
        self.assertEqual(self.path.read_text(), "a = 9\n")
        self.assertEqual(self.names(), ["hibernaut.toml"])

    def test_c5_a_write_that_fails_half_way_leaves_the_old_file_whole(self) -> None:
        file = ConfigFile(self.path)
        for broken in ("os.replace", "os.fsync"):
            with self.subTest(broken=broken), file.locked(), patch(broken, side_effect=OSError("disk full")):
                with self.assertRaises(OSError):
                    file.replace(b"a = 1\n", b"a = 2\n")
            self.assertEqual(self.path.read_text(), "a = 1\n")
            self.assertEqual(self.names(), ["hibernaut.toml"])

    def test_c5_a_link_keeps_pointing_at_the_file(self) -> None:
        link = self.root / "link.toml"
        link.symlink_to(self.path)
        file = ConfigFile(link)
        with file.locked():
            file.replace(b"a = 1\n", b"a = 2\n")
        self.assertTrue(link.is_symlink())
        self.assertEqual(self.path.read_text(), "a = 2\n")

    def test_c5_two_writers_take_turns(self) -> None:
        self.path.write_text("0")

        def add() -> None:
            for _ in range(20):
                file = ConfigFile(self.path)
                with file.locked():
                    seen = file.read()
                    file.replace(seen, str(int(seen) + 1).encode(), backup=False)

        threads = [threading.Thread(target=add) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(self.path.read_text(), "80")

    def test_c5_another_process_holding_it_is_waited_for_and_then_given_up(self) -> None:
        import fcntl

        fd = os.open(self.root, os.O_RDONLY)
        self.addCleanup(os.close, fd)
        fcntl.flock(fd, fcntl.LOCK_EX)
        with self.assertRaises(ConfigChanged), ConfigFile(self.path).locked(timeout=0.1):
            self.fail("not locked")
        fcntl.flock(fd, fcntl.LOCK_UN)
        with ConfigFile(self.path).locked(timeout=0.1):
            pass

    def test_c5_a_restore_gives_up_when_somebody_wrote_after_it(self) -> None:
        file = ConfigFile(self.path)
        with file.locked():
            file.replace(b"a = 1\n", b"a = 2\n")
            self.path.write_text("a = 3\n")
            with self.assertRaises(ConfigChanged):
                file.replace(b"a = 2\n", b"a = 1\n", backup=False)
        self.assertEqual(self.path.read_text(), "a = 3\n")



if __name__ == "__main__":
    unittest.main()
