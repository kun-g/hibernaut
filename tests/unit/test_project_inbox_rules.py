import sqlite3
import tempfile
import unittest
from pathlib import Path

from agent_runtime.adapters.storage_sqlite import SQLiteDatabase
from agent_runtime.adapters.storage_sqlite.workspaces import SQLiteWorkspaceStore
from agent_runtime.app.config import load_config, parse_config
from agent_runtime.app.projects import AmbiguousProjectMatch, InboxRule, ProjectRouter
from tests.support.project_inbox_rules import project_table


class TestInboxIdConfig(unittest.TestCase):
    def parse(self, **projects):
        return parse_config({"projects": {name: {"repo": f"o/{name}", "checkout": "/tmp/x", **item}
                                          for name, item in projects.items()}})

    def test_string_inbox_id_stays_one_exact_inbox(self):
        cfg = self.parse(hibernaut={"inbox_id": "proj:hibernaut"}, other={})
        proj = cfg.projects["hibernaut"]
        self.assertEqual(proj.inbox_id, "proj:hibernaut")
        self.assertEqual(proj.inbox_rules, (InboxRule.exact("proj:hibernaut"),))
        self.assertFalse(proj.serves_multiple_inboxes)
        self.assertEqual(cfg.projects["other"].inbox_id, "proj:other")
        self.assertFalse(cfg.projects["other"].serves_multiple_inboxes)

    def test_array_entries_are_plain_regexes_and_the_first_exact_entry_is_primary(self):
        cfg = self.parse(hibernaut={"inbox_id": ["proj:hibernaut:task-\\d+", "proj:hibernaut",
                                                 "proj:hibernaut:drive"]})
        proj = cfg.projects["hibernaut"]
        # The expression comes first but is never the delivery address.
        self.assertEqual(proj.inbox_id, "proj:hibernaut")
        self.assertTrue(proj.serves_multiple_inboxes)
        self.assertEqual([str(r) for r in proj.inbox_rules],
                         ["proj:hibernaut:task-\\d+", "proj:hibernaut", "proj:hibernaut:drive"])
        self.assertEqual([r.literal for r in proj.inbox_rules], [None, "proj:hibernaut", "proj:hibernaut:drive"])
        # Reload compares configs; the same file must compare equal.
        self.assertEqual(cfg, self.parse(hibernaut={"inbox_id": ["proj:hibernaut:task-\\d+", "proj:hibernaut",
                                                                 "proj:hibernaut:drive"]}))

    def test_a_direct_regex_without_re_prefix_matches_the_whole_inbox_id(self):
        cfg = self.parse(hibernaut={"inbox_id": ["proj:hibernaut", "proj:hibernaut:.+"]},
                         other={"inbox_id": "proj:other"})
        router = ProjectRouter(cfg.projects.values())
        for hit in ("proj:hibernaut:drive", "proj:hibernaut:task-12", "proj:hibernaut"):
            with self.subTest(hit=hit):
                self.assertEqual(router.resolve(hit).name, "hibernaut")  # type: ignore[union-attr]
        for miss in ("proj:hibernaut:", "xproj:hibernaut:drive", "proj:hibernautx", "proj:other:drive"):
            with self.subTest(miss=miss):
                self.assertIsNone(router.resolve(miss))
        self.assertEqual(router.resolve("proj:other").name, "other")  # type: ignore[union-attr]
        self.assertTrue(cfg.projects["hibernaut"].serves_multiple_inboxes)

    def test_patterns_match_the_whole_inbox_id(self):
        router = ProjectRouter(self.parse(hibernaut={"inbox_id": ["proj:hibernaut", "proj:hibernaut:task-\\d+"]})
                               .projects.values())
        self.assertIsNotNone(router.resolve("proj:hibernaut:task-12"))
        for miss in ("proj:hibernaut:task-12x", "xproj:hibernaut:task-1", "proj:hibernaut:task-", "proj:hibernaut:"):
            with self.subTest(miss):
                self.assertIsNone(router.resolve(miss))

    def test_legacy_re_prefixed_entries_keep_routing_like_the_direct_form(self):
        legacy = self.parse(hibernaut={"inbox_id": ["proj:hibernaut", "re:proj:hibernaut:.+"]}).projects["hibernaut"]
        direct = self.parse(hibernaut={"inbox_id": ["proj:hibernaut", "proj:hibernaut:.+"]}).projects["hibernaut"]
        self.assertEqual(legacy.inbox_id, "proj:hibernaut")
        self.assertTrue(legacy.serves_multiple_inboxes)
        self.assertEqual(str(legacy.inbox_rules[1]), "re:proj:hibernaut:.+")
        for inbox in ("proj:hibernaut", "proj:hibernaut:drive", "proj:hibernaut:", "proj:other"):
            with self.subTest(inbox=inbox):
                self.assertEqual(ProjectRouter([legacy]).resolve(inbox) is None,
                                 ProjectRouter([direct]).resolve(inbox) is None)
        # A legacy entry was a pattern before, so it never becomes the primary inbox, even without regex syntax.
        cfg = self.parse(a={"inbox_id": ["re:proj:a:x", "proj:a"]})
        self.assertEqual(cfg.projects["a"].inbox_id, "proj:a")
        self.assertEqual([r.literal for r in cfg.projects["a"].inbox_rules], [None, "proj:a"])

    def test_a_string_inbox_id_stays_exact_even_with_regex_characters(self):
        router = ProjectRouter(self.parse(a={"inbox_id": "proj:a.b"}).projects.values())
        self.assertIsNotNone(router.resolve("proj:a.b"))
        self.assertIsNone(router.resolve("proj:aXb"))
        self.assertFalse(self.parse(a={"inbox_id": "proj:a.b"}).projects["a"].serves_multiple_inboxes)

    def test_an_escaped_entry_is_an_exact_id_and_its_unescaped_form_is_the_address(self):
        cfg = self.parse(a={"inbox_id": ["proj:a\\.b:.+", "proj:a\\.b"]})
        proj = cfg.projects["a"]
        self.assertEqual(proj.inbox_id, "proj:a.b")
        router = ProjectRouter([proj])
        self.assertIsNotNone(router.resolve("proj:a.b:drive"))
        self.assertIsNone(router.resolve("proj:aXb"))
        self.assertEqual(list(router.inbox_map(lambda p: p.name)), ["proj:a.b"])

    def test_malformed_inbox_ids_are_rejected_with_the_reason(self):
        cases = {
            "must be a string or a non-empty array": [],
            "entries must be non-empty strings": ["proj:a", 3],
            "entries must be non-empty strings, got ' '": ["proj:a", " "],
            "has an empty pattern": ["proj:a", "re:"],
            "invalid pattern 'proj:\\('": ["proj:a", "proj:("],
            "invalid pattern 're:proj:\\('": ["proj:a", "re:proj:("],
            "invalid pattern 'proj:a:\\[x'": ["proj:a", "proj:a:[x"],
            "needs at least one exact inbox_id": ["proj:a:.*"],
            "needs at least one exact inbox_id .*escape a literal": ["proj:a.b"],
            "needs at least one exact inbox_id \\(an entry without regex syntax": ["re:proj:a:.*"],
            "lists 'proj:a' twice": ["proj:a", "proj:a"],
            "lists 're:proj:a' twice": ["proj:a", "re:proj:a"],
            "is a pattern; list it in an array.*'proj:a:\\.\\*'\\]": "re:proj:a:.*",
            "must not be empty": "  ",
        }
        for message, raw in cases.items():
            with self.subTest(message), self.assertRaisesRegex(ValueError, message):
                self.parse(a={"inbox_id": raw})

    def test_direct_patterns_leave_configured_delivery_addresses_alone(self):
        cfg = parse_config({
            "gdrive": {"default_inbox_id": "proj:hibernaut:drive"},
            "projects": {"hibernaut": {"repo": "kun-g/hibernaut", "checkout": "/tmp/h",
                                       "inbox_id": ["proj:hibernaut:.+", "proj:hibernaut"]}},
        })
        self.assertEqual(cfg.gdrive.default_inbox_id, "proj:hibernaut:drive")
        self.assertEqual(cfg.projects["hibernaut"].inbox_id, "proj:hibernaut")
        checkouts = ProjectRouter(cfg.projects.values()).inbox_map(lambda p: p.checkout)
        self.assertEqual(list(checkouts), ["proj:hibernaut"])
        self.assertEqual(checkouts.get(cfg.gdrive.default_inbox_id), "/tmp/h")

    def test_loading_a_file_that_assigns_one_literal_to_two_projects_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "hibernaut.toml"
            path.write_text(project_table("a", ["proj:a", "proj:shared"], "/tmp/a")
                            + project_table("b", "proj:shared", "/tmp/b"))
            with self.assertRaisesRegex(ValueError, "inbox_id 'proj:shared' is assigned to both projects.a and "
                                                    "projects.b"):
                load_config(path)

    def test_a_literal_of_one_project_matched_by_another_projects_pattern_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "'proj:a:drive' of projects.a also matches a pattern of projects.b"):
            self.parse(a={"inbox_id": ["proj:a", "proj:a:drive"]}, b={"inbox_id": ["proj:b", "proj:a:.*"]})
        with self.assertRaisesRegex(ValueError, "'proj:a' of projects.a also matches a pattern of projects.b"):
            self.parse(a={"inbox_id": "proj:a"}, b={"inbox_id": ["proj:b", "re:proj:.+"]})
        with self.assertRaisesRegex(ValueError, "'proj:a:x' is assigned to both projects.a and projects.b"):
            self.parse(a={"inbox_id": ["proj:a", "proj:a\\:x"]}, b={"inbox_id": ["proj:b", "proj:a:x"]})

    def test_patterns_of_two_projects_that_overlap_are_rejected(self):
        for a, b in (
            ("shared:.+", "shared:.+"), ("shared:.+", "re:shared:.+"), ("shared:.*", ".*:x"), ("shared:.+", "shared:[a-z]+"),
            ("proj:(a|b):.+", "proj:(b|c):.+"), ("t:\\d+", "t:[0-9]{3}"), ("t:\\w+", "t:é.*"), ("t:\\S{2,}", "t:(?:ab)+"),
            ("^t:.+$", "t:x.*"), ("t:[^a]", "t:[^b]"), ("t:a{2,4}", "t:a{4,}"), ("t:(x:)?y", "t:x:y+"), ("t:a*?b", "t:ab?"),
        ):
            with self.subTest(a=a, b=b):
                message = r"pattern .* of projects\.(a|b) and pattern .* of projects\.(a|b) both match"
                with self.assertRaisesRegex(ValueError, message):
                    self.parse(a={"inbox_id": ["proj:a", a]}, b={"inbox_id": ["proj:b", b]})

    def test_patterns_of_two_projects_that_never_meet_load(self):
        for a, b in (
            ("proj:a:.+", "proj:b:.+"), ("t:\\d+", "t:[a-z]+"), ("t:a{2,3}", "t:a{4,}"), ("t:[^a]", "t:a"), ("t:a+", "t:b*a?b"),
            ("t:(x|y):.*", "t:(z|w):.*"), ("^t:a.*$", "^t:b.*$"), ("t:\\w", "t:\\W"), ("t:.", "t:\\n"), ("t:é", "t:[a-z]"),
        ):
            with self.subTest(a=a, b=b):
                self.parse(a={"inbox_id": ["proj:a", a]}, b={"inbox_id": ["proj:b", b]})
        # Two patterns of one project may overlap.
        self.parse(a={"inbox_id": ["proj:a", "t:.+", "t:x.*"]})

    def test_a_pattern_whose_overlap_cannot_be_told_is_rejected_next_to_another_projects(self):
        for pattern in ("t:(?!x).+", "t:(?<=:)x", "t:(a)\\1", "(?i)t:x", "t:\\bx", "t:[à-ÿ]", "t:(?>a+)", "t:a{3000}", "t:x$y"):
            with self.subTest(pattern), self.assertRaisesRegex(ValueError, r"projects\.a pattern .* cannot be compared"):
                self.parse(a={"inbox_id": ["proj:a", pattern]}, b={"inbox_id": ["proj:b", "proj:b:.+"]})
            # Alone, or beside exact ids, it needs no comparing.
            self.parse(a={"inbox_id": ["proj:a", pattern]}, b={"inbox_id": ["proj:b", "proj:b:x"]})

    def test_a_file_with_an_overlap_does_not_load(self):
        # A reload that fails to load keeps the config in force (test_config_reload).
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "c.toml"
            path.write_text(project_table("a", ["proj:a", "shared:.+"], tmp)
                            + project_table("b", ["proj:b", "shared:.+"], tmp))
            with self.assertRaisesRegex(ValueError, "both match"):
                load_config(path)

    def test_disjoint_direct_patterns_of_two_projects_load(self):
        cfg = self.parse(a={"inbox_id": ["proj:a", "proj:a:.+"]}, b={"inbox_id": ["proj:b", "proj:b:.+"]})
        router = ProjectRouter(cfg.projects.values())
        self.assertEqual(router.resolve("proj:a:drive").name, "a")  # type: ignore[union-attr]
        self.assertEqual(router.resolve("proj:b:drive").name, "b")  # type: ignore[union-attr]

    def test_overlap_between_a_literal_and_a_pattern_is_ambiguous_at_routing(self):
        # Load rejects this; the router still refuses it for configs built another way.
        a = parse_config({"projects": {"a": {"repo": "o/a", "checkout": "/tmp/a", "inbox_id": "proj:a:x"}}})
        b = parse_config({"projects": {"b": {"repo": "o/b", "checkout": "/tmp/b", "inbox_id": ["proj:b", ".*:x"]}}})
        router = ProjectRouter([a.projects["a"], b.projects["b"]])
        with self.assertRaises(AmbiguousProjectMatch) as caught:
            router.resolve("proj:a:x")
        self.assertEqual(caught.exception.projects, ("a", "b"))
        self.assertEqual([p.name for p in router.matching("proj:b")], ["b"])

    def test_pattern_inboxes_get_their_projects_settings_through_inbox_maps(self):
        cfg = self.parse(hibernaut={"inbox_id": ["proj:hibernaut", "proj:hibernaut:.+"], "executor": "sde"},
                         other={"inbox_id": "proj:other"})
        executors = ProjectRouter(cfg.projects.values()).inbox_map(lambda p: p.executor)
        self.assertEqual(executors.get("proj:hibernaut:drive"), "sde")
        self.assertIsNone(executors.get("proj:other"))
        self.assertEqual(list(executors), ["proj:hibernaut"])


class TestOwnerMigration(unittest.TestCase):
    def test_existing_workspaces_rows_survive_and_gain_an_empty_owner(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "old.db"
            with sqlite3.connect(path) as conn:
                conn.execute(
                    "CREATE TABLE workspaces (seq INTEGER PRIMARY KEY AUTOINCREMENT, work_key TEXT NOT NULL UNIQUE, "
                    "repo_checkout TEXT NOT NULL, workdir TEXT NOT NULL, branch TEXT NOT NULL, status TEXT NOT NULL "
                    "CHECK (status IN ('creating', 'ready', 'reclaiming')), created_at TEXT NOT NULL, "
                    "updated_at TEXT NOT NULL)"
                )
                conn.execute("INSERT INTO workspaces (work_key, repo_checkout, workdir, branch, status, created_at, "
                             "updated_at) VALUES ('k', '/r', '/r/../hb-task-1', 'hb/task-1', 'ready', 't', 't')")
            db = SQLiteDatabase(path)
            try:
                record = SQLiteWorkspaceStore(db).get("k")
            finally:
                db.close()
            assert record is not None
            self.assertEqual((record.workdir, record.status, record.inbox_id), ("/r/../hb-task-1", "ready", None))


if __name__ == "__main__":
    unittest.main()
