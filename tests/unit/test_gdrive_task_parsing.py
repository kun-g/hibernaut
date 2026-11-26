from __future__ import annotations

import unittest

from agent_runtime.adapters.channels import parse_task_markdown
from agent_runtime.adapters.channels.gdrive_attachments import split_attachment_refs

# The 2026-11-22 task 「整合 Taskboard Phase 1 代码包」 as the proxy exported it: the .md upload had been converted to a
# Google Doc, so the opening `---` came back as a rule of underscores and the three metadata lines as one line.
REAL_GOOGLE_DOC_EXPORT = (
    "________________\r\n\r\n\r\n"
    "scope: proj:hibernaut priority: P1 title: 整合 Taskboard Phase 1 代码包并跑通 CI 与依赖门禁\r\n"
    "任务目标\r\n"
    "下载 taskboard_phase1_bundle.zip 并应用补丁。\r\n"
)


class ParseTaskMarkdownTests(unittest.TestCase):
    def test_real_google_doc_export_regression(self) -> None:
        meta, body = parse_task_markdown(REAL_GOOGLE_DOC_EXPORT)
        self.assertEqual(meta["scope"], "proj:hibernaut")
        self.assertEqual(meta["priority"], "P1")
        self.assertEqual(meta["title"], "整合 Taskboard Phase 1 代码包并跑通 CI 与依赖门禁")
        self.assertEqual(body, "任务目标\n下载 taskboard_phase1_bundle.zip 并应用补丁。")

    def test_plain_frontmatter_still_parses_every_key(self) -> None:
        meta, body = parse_task_markdown("---\nscope: proj:x\ntitle: 'Quoted'\ncustom: kept\n---\n# Goal\nDo it.")
        self.assertEqual(meta, {"scope": "proj:x", "title": "Quoted", "custom": "kept"})
        self.assertEqual(body, "# Goal\nDo it.")

    def test_opening_rule_variants(self) -> None:
        for rule in ("---", "***", "___", "________________", "- - -", "* * *"):
            with self.subTest(rule):
                meta, body = parse_task_markdown(f"\n{rule}\nscope: proj:x\npriority: P2\n{rule}\nBody")
                self.assertEqual(meta, {"scope": "proj:x", "priority": "P2"})
                self.assertEqual(body, "Body")

    def test_squashed_line_splits_on_known_keys_only(self) -> None:
        meta, body = parse_task_markdown(
            "___\nscope: proj:x executor_id: sde activities: versioning, release base_ref: main "
            "tags: a,b attachments: f1, bundle.zip title: Fix it\n___\nBody"
        )
        self.assertEqual(
            meta,
            {
                "scope": "proj:x",
                "executor_id": "sde",
                "activities": "versioning, release",
                "base_ref": "main",
                "tags": "a,b",
                "attachments": "f1, bundle.zip",
                "title": "Fix it",
            },
        )
        self.assertEqual(body, "Body")

    def test_a_quoted_value_is_not_split(self) -> None:
        meta, _ = parse_task_markdown('---\nscope: proj:x title: "Fix tags: parsing" priority: P3\n---\nBody')
        self.assertEqual(meta, {"scope": "proj:x", "title": "Fix tags: parsing", "priority": "P3"})

    def test_known_keys_as_markdown_headings(self) -> None:
        # What a Markdown export of the same Google Doc looks like: an ATX heading, escaped underscores.
        meta, body = parse_task_markdown(
            "---\n\n## scope: proj:hibernaut priority: P1 executor\\_id: sde title: 整合 Phase 1\n\n# 任务目标\n\n正文"
        )
        self.assertEqual(meta, {"scope": "proj:hibernaut", "priority": "P1", "executor_id": "sde", "title": "整合 Phase 1"})
        self.assertEqual(body, "# 任务目标\n\n正文")

    def test_known_keys_as_a_setext_heading(self) -> None:
        meta, body = parse_task_markdown("***\nscope: proj:x priority: P1 title: T\n===\n任务目标")
        self.assertEqual(meta, {"scope": "proj:x", "priority": "P1", "title": "T"})
        self.assertEqual(body, "任务目标")
        meta, body = parse_task_markdown("***\n\nscope: proj:x title: T\n---\n任务目标")
        self.assertEqual(meta, {"scope": "proj:x", "title": "T"})
        self.assertEqual(body, "任务目标")

    def test_unknown_content_is_left_in_the_body(self) -> None:
        cases = {
            "a rule then prose": "---\nIntro: some prose here\n\nMore.",
            "a rule then an unknown key": "___\nnote: hi\nBody",
            "no rule": "scope: proj:x\nBody",
            "no frontmatter": "Just a task.",
        }
        for label, text in cases.items():
            with self.subTest(label):
                self.assertEqual(parse_task_markdown(text), ({}, text.strip()))

    def test_attachments_block_list(self) -> None:
        closed = "---\nscope: proj:x\nattachments:\n  - a1\n\n  - 'b, c.pdf'\ntitle: T\n---\nBody"
        self.assertEqual(parse_task_markdown(closed),
                         ({"scope": "proj:x", "attachments": "[a1, 'b, c.pdf']", "title": "T"}, "Body"))
        # A Google Doc export: no closing rule, list items as `*` bullets with escaped underscores.
        unclosed = "___\nscope: proj:x attachments:\n* 1Qva\\_Id\n* d.zip, e.zip\n任务目标"
        self.assertEqual(parse_task_markdown(unclosed),
                         ({"scope": "proj:x", "attachments": '[1Qva_Id, "d.zip, e.zip"]'}, "任务目标"))
        for text in (closed, unclosed):
            with self.subTest(text):
                self.assertEqual(
                    split_attachment_refs(parse_task_markdown(text)[0]["attachments"]),
                    ["a1", "b, c.pdf"] if text is closed else ["1Qva_Id", "d.zip, e.zip"],
                )

    def test_a_list_item_after_another_key_is_not_metadata(self) -> None:
        self.assertEqual(parse_task_markdown("---\ntags:\n- a\nBody"), ({"tags": ""}, "- a\nBody"))

    def test_split_attachment_refs_spellings(self) -> None:
        cases = {
            "a, b": ["a", "b"],
            "[a, b]": ["a", "b"],
            '[a, "b"]': ["a", "b"],
            "['a', \"b, c\"]": ["a", "b, c"],
            "[a]": ["a"],
            "[]": [],
            " a , a , ": ["a"],
            "[a, b": ["[a", "b"],
            "a]": ["a]"],
        }
        for raw, refs in cases.items():
            with self.subTest(raw):
                self.assertEqual(split_attachment_refs(raw), refs)

    def test_unclosed_block_stops_at_the_first_line_that_is_not_metadata(self) -> None:
        meta, body = parse_task_markdown("---\nscope: proj:x\ntitle: T\nnote: stays\nBody")
        self.assertEqual(meta, {"scope": "proj:x", "title": "T"})
        self.assertEqual(body, "note: stays\nBody")


if __name__ == "__main__":
    unittest.main()
