from __future__ import annotations

import asyncio
import json
import os
import tempfile
import threading
import time
import tomllib
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock
from unittest.mock import patch

from agent_runtime.app import config as config_module
from agent_runtime.app.config import HibernautConfig, load_config
from agent_runtime.app.config_edit import ConfigFile
from agent_runtime.app.onboarding import (
    OnboardError,
    Onboarding,
    Request,
    _is_remote_of,
    description_text,
    project_agents,
    project_name,
    scrub,
    split_repo,
)
from agent_runtime.app.reload import ReloadFailed, ReloadStatus
from agent_runtime.app.system_tools import SystemToolbox
from tests.support.onboarding import CONFIG, FakeCommands, OnboardingCase, Reloads


# Placeholders, not real credentials.
ENV_TOKEN = "placeholder-gw-tok"


class OnboardTests(OnboardingCase):
    def test_r1_the_project_is_set_up_with_its_steward_in_front(self) -> None:
        found = self.onboard(description="a tool")
        checkout = self.root / "new-tool"
        self.assertEqual(self.run_.made("gh", "repo", "clone"), [["gh", "repo", "clone", "acme/new-tool", str(checkout)]])
        project = self.config().projects["new-tool"]
        main, sub = project.route("proj:new-tool").binding, project.route("proj:new-tool:sde:x").binding
        assert main is not None and sub is not None
        self.assertEqual((main.default_agent, main.agents), ("front", ("front", "sde", "reviewer")))
        self.assertEqual((sub.default_agent, sub.agents), ("sde", ("sde", "reviewer")))
        self.assertEqual((project.repo, project.checkout, project.inbox_id),
                         ("acme/new-tool", str(checkout), "proj:new-tool"))
        for step in ("checkout", "orca", "label", "hibernaut.toml", "steward", "forward_to"):
            self.assertTrue(any(line.startswith(step) for line in found["done"]), step)

    def test_r1_the_repository_is_registered_in_orca_so_worktrees_open(self) -> None:
        self.onboard()
        self.assertIn(str(self.root / "new-tool"), self.run_.registered)
        self.assertEqual(self.config().projects["new-tool"].worktree_backend, "orca")

    def test_r2_it_opens_pull_requests_does_not_merge_says_what_it_is_and_can_be_sent_work(self) -> None:
        self.onboard(description="它做什么")
        config = self.config()
        project = config.projects["new-tool"]
        assert project.delivery is not None
        self.assertEqual((project.delivery.open_pr, project.delivery.auto_merge), (True, False))
        self.assertEqual(project.description, "它做什么")
        self.assertEqual(config.runtime.inbox_bindings[0].forward_to, ("proj:old", "sys:ops", "proj:new-tool"))

    def test_r2_without_words_of_the_person_the_description_is_neutral(self) -> None:
        self.onboard()
        self.assertEqual(self.config().projects["new-tool"].description, "GitHub 仓库 acme/new-tool")

    def test_r3_c5_everything_else_stays_as_it_was(self) -> None:
        found = self.onboard()
        after = self.path.read_text()
        self.only_added(after)
        self.assertEqual(Path(found["backup"]).read_text(), self.before)
        self.assertIn(found["backup"], found["undo"])
        before, now = tomllib.loads(self.before), tomllib.loads(after)
        del now["projects"]["new-tool"]
        now["runtime"]["inbox_bindings"][0]["forward_to"].remove("proj:new-tool")
        self.assertEqual(now, before)

    def test_r3_a_second_time_nothing_is_made_again_or_overwritten(self) -> None:
        self.onboard()
        by_hand = self.path.read_text().replace('"GitHub 仓库 acme/new-tool"', '"my own words"').replace(
            "auto_merge = false", "auto_merge = true")
        self.path.write_text(by_hand)
        self.run_.calls.clear()
        found = self.onboard("ACME/New-Tool", description="other words")
        self.assertEqual(self.path.read_text(), by_hand)
        self.assertEqual(found["configuration"], "not changed")
        for made in (("gh", "repo", "clone"), ("gh", "label", "create"), ("orca", "repo", "add")):
            self.assertEqual(self.run_.made(*made), [])
        self.assertTrue(any("already serves acme/new-tool" in step for step in found["done"]))

    def test_r3_a_known_repository_that_lacks_something_is_only_told_so(self) -> None:
        (self.root / "bare" / ".git").mkdir(parents=True)
        self.run_.origin = "git@github.com:acme/bare.git"
        found = self.onboard("acme/bare")
        self.unchanged()
        self.assertTrue(any("bare lacks steward, description, forward_to" in note for note in found["left_to_a_person"]))

    def test_r4_a_repository_that_is_not_named_in_full_is_refused_before_anything_happens(self) -> None:
        for repo in ("new-tool", "", "acme/", "/x", "https://github.com/acme/x", "acme/x/y", "-acme/x", "acme/-x", "acme/..",
                     "acme/x.git", "acme/.x", "ac me/x", "acme/x;ls", "acme/x\n", "acme/a..b", "--upload-pack=x/y",
                     "acme/$(id)", "acme-/x", "acme/x y"):
            with self.subTest(repo=repo):
                self.refused("repo must be owner/name", lambda: self.onboard(repo))
        for name in ("New", "-x", "a b", "a:b", "a.b", "x\ny", 'x"', "x]", "", "é", "a/b"):
            if not name:
                continue
            with self.subTest(name=name):
                self.refused("project name", lambda: self.onboard(name=name))
        self.assertEqual(self.run_.calls, [])
        self.unchanged()

    def test_r8_a_taken_name_is_told_before_anything_happens(self) -> None:
        self.refused("the name old is taken by acme/old", lambda: self.onboard("other/old"))
        self.assertEqual(self.run_.calls, [])
        self.unchanged()

    def test_r8_an_unreachable_repository_changes_nothing(self) -> None:
        self.run_.fail["gh api repos/acme/new-tool"] = "HTTP 404"
        self.refused("acme/new-tool is not reachable", self.onboard)
        self.assertFalse((self.root / "new-tool").exists())
        self.unchanged()

    def test_r8_a_clone_that_fails_says_where_it_stopped_and_leaves_no_half_of_it(self) -> None:
        self.run_.fail["gh repo clone"] = "fatal: https://x:placeholder-secret@github.com/acme/new-tool broke"
        text = self.refused("gh repo clone acme/new-tool failed.*left behind: nothing", self.onboard)
        self.assertNotIn("placeholder-secret", text)
        self.assertFalse((self.root / "new-tool").exists())
        self.unchanged()

    def test_r8_a_later_step_that_fails_says_what_is_left_behind(self) -> None:
        self.run_.fail["orca repo add"] = "Orca is not running"
        self.refused(r"in Orca failed: Orca is not running.*configuration is as it was; left behind: checkout.*\(cloned\)$",
                     self.onboard)
        self.unchanged()
        # The same call goes on from there.
        self.run_.fail.clear()
        self.onboard()
        self.assertIn("new-tool", self.config().projects)

    def test_r8_orca_has_to_know_the_repository_afterwards(self) -> None:
        self.run_.forget = True
        self.refused("registering .* in Orca failed", self.onboard)
        self.unchanged()

    def test_r8_another_checkout_in_the_way_is_left_alone(self) -> None:
        there = self.root / "new-tool"
        (there / ".git").mkdir(parents=True)
        for origin in ("git@github.com:other/new-tool.git", "https://github.com/acme/new-tool-2", "/x/acme/new-tool",
                       "https://example.com/acme/new-tool", ""):
            self.run_.origin = origin
            with self.subTest(origin=origin):
                self.refused("its origin is", self.onboard)
        self.unchanged()
        (there / ".git").rmdir()
        (there / "notes.txt").write_text("mine")
        self.refused("is not a git checkout", self.onboard)
        self.assertEqual((there / "notes.txt").read_text(), "mine")

    def test_r8_no_agent_to_look_after_it_is_told_before_anything_happens(self) -> None:
        self.path.write_text(self.before.replace("front = {", "front = { workdir = \"/x\", branch = \"main\","))
        self.refused("no agent with role steward is free", self.onboard)
        self.assertEqual(self.run_.calls, [])

    def test_r8_a_configuration_that_is_broken_already_is_told(self) -> None:
        for broken in ("[runtime", "\n[projects.x]\n"):
            self.path.write_text(self.before + broken)
            self.refused("the configuration is not valid", self.onboard)
            self.assertEqual(self.path.read_text(), self.before + broken)
        self.assertEqual(self.run_.calls, [])

    def test_r7_labels_are_made_for_listed_owners_only(self) -> None:
        self.onboard("acme/one")
        self.assertEqual(len(self.run_.made("gh", "label", "create")), 1)
        found = self.onboard("stranger/two")
        self.assertEqual(len(self.run_.made("gh", "label", "create")), 1)
        self.assertTrue(any(note.startswith("label: create") and "stranger/two" in note
                            for note in found["left_to_a_person"]))
        self.path.write_text(self.path.read_text().replace('label_owners = ["acme"]', ""))
        found = self.onboard("acme/three")
        self.assertEqual(len(self.run_.made("gh", "label", "create")), 1)

    def test_r7_the_server_s_routing_is_listed_and_never_touched(self) -> None:
        found = self.onboard()
        [note] = [n for n in found["left_to_a_person"] if n.startswith("switchboard")]
        self.assertIn('"acme" = "proj:{repo}"', note)
        found = self.onboard("acme/other", name="mine")
        [note] = [n for n in found["left_to_a_person"] if n.startswith("switchboard")]
        self.assertIn('"acme/other" = "proj:mine"', note)
        self.assertNotIn("{repo}", note)

    def test_r8_without_one_general_steward_its_list_is_left_to_a_person(self) -> None:
        self.path.write_text(self.before.replace('"proj:old", "sys:ops"]', '"proj:old"]'))
        found = self.onboard()
        self.assertTrue(any(n.startswith("forward_to: there is not exactly one") for n in found["left_to_a_person"]))
        self.assertIn("new-tool", self.config().projects)

    def test_a_steward_that_wants_another_branch_is_told(self) -> None:
        self.path.write_text(self.before.replace("front = {", "front = { branch = \"release\","))
        found = self.onboard()
        self.assertTrue(any("is on main" in n and "release" in n for n in found["left_to_a_person"]))


class CompleteTests(OnboardingCase):
    def test_r5_what_each_project_lacks(self) -> None:
        found = {p["name"]: p for p in json.loads(self.box.check())["projects"]}
        self.assertEqual({n: p["missing"] for n, p in found.items()},
                         {"old": [], "bare": ["steward", "description", "forward_to"]})
        self.unchanged()

    def test_r5_a_default_agent_without_a_binding_counts_as_it_works(self) -> None:
        self.path.write_text(self.before.replace(
            'inbox_id = ["proj:bare", "proj:bare:.+"]\ndefault_agent = "sde"',
            'inbox_id = ["proj:bare", "proj:bare:.+"]\ndefault_agent = "front"'))
        found = {p["name"]: p["missing"] for p in json.loads(self.box.check())["projects"]}
        self.assertEqual(found["bare"], ["description", "forward_to"])

    def test_r6_what_is_missing_is_added_and_takes_effect(self) -> None:
        found = self.complete("bare", "what bare is")
        after = self.path.read_text()
        self.only_added(after)
        config = self.config()
        bare = config.projects["bare"]
        main, sub = bare.route("proj:bare").binding, bare.route("proj:bare:x").binding
        assert main is not None and sub is not None
        self.assertEqual((main.default_agent, sub.default_agent, bare.description), ("front", "sde", "what bare is"))
        self.assertIn("proj:bare", config.runtime.inbox_bindings[0].forward_to)
        self.assertEqual(self.reloads.waited, [after.encode()])
        self.assertEqual(Path(found["backup"]).read_text(), self.before)
        self.assertEqual(len(found["done"]), 3)
        self.assertEqual(json.loads(self.box.check())["projects"][1]["missing"], [])

    def test_r3_c3_values_set_by_hand_stay(self) -> None:
        found = self.complete("old", "new words")
        self.unchanged()
        self.assertEqual(found["configuration"], "not changed")
        self.assertTrue(any(n.startswith("description: it is set already") for n in found["left_to_a_person"]))

    def test_r3_a_binding_written_by_hand_is_not_changed(self) -> None:
        text = self.before.replace('pattern = "proj:old"\ndefault_agent = "front"',
                                   'pattern = "proj:old"\ndefault_agent = "sde"')
        self.path.write_text(text)
        found = {p["name"]: p for p in json.loads(self.box.check())["projects"]}["old"]
        self.assertEqual(found["missing"], ["steward"])
        self.assertIn("names sde", found["by_hand"][0])
        self.complete("old")
        self.assertEqual(self.path.read_text(), text)

    def test_r6_one_binding_for_the_main_inbox_is_enough_for_a_project_of_one_inbox(self) -> None:
        self.path.write_text(self.before.replace('inbox_id = ["proj:bare", "proj:bare:.+"]', 'inbox_id = "proj:bare"'))
        self.complete("bare")
        bare = self.config().projects["bare"]
        self.assertEqual([b.binding.default_agent for b in bare.inbox_bindings], ["front"])
        self.assertTrue(bare.inbox_bindings[0].binding.allows("sde"))

    def test_r8_an_unknown_project_is_told(self) -> None:
        for name in ("nope", "runtime", "old.delivery", "projects.old", 'x"]', "sys:ops", "hibernaut-ops"):
            with self.subTest(name=name):
                self.refused("there is no project", lambda: self.complete(name))
        self.unchanged()

    def test_r8_a_project_written_another_way_is_left_to_a_person(self) -> None:
        self.path.write_text(self.before.split("[projects.bare]")[0] + (
            '[projects]\nbare = { repo = "acme/bare", checkout = "/x", default_agent = "front", inbox_id = "proj:bare" }\n'
        ))
        found = self.complete("bare", "words")
        self.assertTrue(any(n.startswith("description: [projects.bare] is not written") for n in found["left_to_a_person"]))
        self.assertIn("proj:bare", self.config().runtime.inbox_bindings[0].forward_to)


class ExampleTests(OnboardingCase):
    # The real example file, comments and all.
    EXAMPLE = Path(__file__).resolve().parents[2] / "hibernaut.example.toml"

    def use(self, text: str) -> None:
        self.before = text
        self.path.write_text(text)
        self.reloads = Reloads(self.path)
        self.box = SystemToolbox(reloads=self.reloads, allowed=lambda: True, run=self.run_, reload_timeout=1.0)

    def added_lines(self) -> list[str]:
        # Fails unless the old lines are all there, verbatim and in order.
        old, added = self.before.splitlines(keepends=True), list[str]()
        for line in self.path.read_text().splitlines(keepends=True):
            if old and line == old[0]:
                old.pop(0)
            else:
                added.append(line)
        self.assertEqual(old, [])
        return added

    def test_r6_filling_in_the_example_adds_lines_and_changes_none(self) -> None:
        self.use(self.EXAMPLE.read_text(encoding="utf-8"))
        told = self.complete("hibernaut", "Always-on Agent")
        self.assertEqual(self.added_lines(), ['description = "Always-on Agent"\n'])
        self.assertEqual(Path(told["backup"]).read_bytes(), self.EXAMPLE.read_bytes())
        self.assertEqual(self.config().projects["hibernaut"].description, "Always-on Agent")
        self.assertEqual(self.complete("hibernaut")["configuration"], "not changed")

    def test_r6_a_project_of_the_example_without_any_of_it_gets_all_of_it(self) -> None:
        text = self.EXAMPLE.read_text(encoding="utf-8")
        start, end = text.index("[[projects.hibernaut.inbox_bindings]]"), text.index("[projects.hibernaut.delivery]")
        text = text[:start] + text[end:]
        other = '[projects.other]\nrepo = "o/r"\ncheckout = "/x"\ninbox_id = "proj:other"\ndefault_agent = "steward"\n'
        self.use(text.replace('forward_to = ["proj:hibernaut", "sys:ops", "sys:infra"]', 'forward_to = ["sys:ops", "sys:infra"]')
                 .replace('\ndefault_agent = "steward"\n# 可选：本项目', '\ndefault_agent = "sde"\n# 可选：本项目') + other)
        self.complete("hibernaut")
        after = self.path.read_text()
        self.assertIn('forward_to = ["sys:ops", "sys:infra", "proj:hibernaut"]   # 可选', after)
        self.path.write_text(after.replace(', "proj:hibernaut"]   #', ']   #'))
        self.assertEqual([line.split(" = ")[0] for line in self.added_lines() if line.strip()], [
            "description",
            "[[projects.hibernaut.inbox_bindings]]\n", "pattern", "default_agent",
            "[[projects.hibernaut.inbox_bindings]]\n", "pattern", "default_agent",
        ])

    def test_r1_onboarding_next_to_it_keeps_every_line(self) -> None:
        self.use(self.EXAMPLE.read_text(encoding="utf-8"))
        self.onboard()
        self.path.write_text(self.path.read_text().replace(', "proj:new-tool"]   #', ']   #'))
        self.assertEqual(self.added_lines()[1], "[projects.new-tool]\n")


class SafetyTests(OnboardingCase):
    def test_c1_values_cannot_write_anything_but_themselves(self) -> None:
        for value in ('"', "]", '"]\'', "a\\", "# x", '"""', "'''", '" , x = "', "[projects.evil]", "{}", "\\u0022",
                      'x" ]] agents = ["plain'):
            with self.subTest(value=value):
                self.path.write_text(self.before)
                self.complete("bare", f"words {value}")
                now = tomllib.loads(self.path.read_text())
                self.assertEqual(now["projects"]["bare"]["description"], f"words {value}")
                self.assertEqual(set(now), set(tomllib.loads(self.before)))
                self.assertEqual(now["executors"], tomllib.loads(self.before)["executors"])
        self.path.write_text(self.before)
        for value in ("a\nb", 'a"\n[projects.evil]\nrepo = "x/y', "a\r\nb", "a\0b", "a\x7fb", "a\u2028b", "a\u202eb", "x" * 201):
            with self.subTest(value=value):
                self.refused("description must be", lambda: self.complete("bare", value))
                self.refused("description must be", lambda: self.onboard(description=value))
        self.unchanged()

    def test_c3_the_keys_it_must_not_change_are_the_same_afterwards(self) -> None:
        self.onboard()
        self.complete("bare")
        before, now = tomllib.loads(self.before), tomllib.loads(self.path.read_text())
        for section in ("executors", "agents", "onboarding"):
            self.assertEqual(now[section], before[section])
        self.assertEqual(now["runtime"]["inbox_bindings"][1], before["runtime"]["inbox_bindings"][1])
        self.assertEqual(now["projects"]["bare"]["delivery"], before["projects"]["bare"]["delivery"])
        self.assertEqual(now["projects"]["old"], before["projects"]["old"])
        self.assertNotIn("checks", now["projects"]["new-tool"]["delivery"])

    def test_c2_environment_values_and_tokens_stay_out_of_what_it_says(self) -> None:
        said = [self.box.check(), self.box.complete(name="bare", description=None),
                self.box.onboard(repo="acme/new-tool", name=None, description=None)]
        self.run_.fail["gh repo clone"] = f"could not use {ENV_TOKEN} nor ghp_0123456789abcdef"
        said.append(self.refused("failed", lambda: self.onboard("acme/third")))
        for text in said:
            self.assertNotIn(ENV_TOKEN, text)
            self.assertNotIn("ghp_0123456789abcdef", text)
        self.assertNotIn(ENV_TOKEN, scrub(f"x {ENV_TOKEN}", tomllib.loads(self.before)))
        with patch.dict(os.environ, {"SOME_API_TOKEN": "placeholder-from-env"}):
            self.assertEqual(scrub("say placeholder-from-env"), "say ***")

    def test_c2_the_file_and_its_backups_are_hidden(self) -> None:
        found = self.complete("bare")
        for path in (self.path, Path(found["backup"]), self.home / ".hibernaut.toml.x.tmp", self.home / "HIBERNAUT.toml"):
            self.assertTrue(self.box.hides(path), path)
        self.assertFalse(self.box.hides(self.home / "hibernaut.example.toml"))

    def test_c2_no_command_gets_the_tokens_of_the_process(self) -> None:
        box = SystemToolbox(reloads=self.reloads, allowed=lambda: True)
        with patch.dict(os.environ, {"GH_TOKEN": "x", "GITHUB_TOKEN": "y"}), \
                patch("agent_runtime.app.system_tools.run_command", side_effect=RuntimeError("stop")) as run:
            with self.assertRaises(RuntimeError):
                box.onboard(repo="acme/new-tool", name=None, description=None)
        env = run.call_args.args[1]
        self.assertFalse({"GH_TOKEN", "GITHUB_TOKEN"} & set(env))

    def test_c5_a_person_who_edits_meanwhile_keeps_their_edit(self) -> None:
        edited = self.before.replace("# the end", "# a person's edit")
        replace = ConfigFile.replace

        def raced(file: ConfigFile, original: bytes, updated: bytes, *, backup: bool = True) -> Path:
            self.path.write_text(edited)
            return replace(file, original, updated, backup=backup)

        with patch.object(ConfigFile, "replace", raced):
            self.refused("changed by somebody else meanwhile", lambda: self.complete("bare"))
        self.assertEqual(self.path.read_text(), edited)
        self.assertEqual([p.name for p in self.home.iterdir()], ["hibernaut.toml"])

    def test_c5_an_edit_made_during_the_clone_is_kept_and_built_on(self) -> None:
        def edit() -> None:
            self.path.write_text(self.before.replace("# the end", "# a person's edit"))

        self.run_.during_clone = edit
        self.onboard()
        self.assertIn("# a person's edit", self.path.read_text())
        self.assertIn("new-tool", self.config().projects)

    def test_c5_r8_a_version_the_daemon_refuses_is_taken_back(self) -> None:
        self.reloads.refuse = "[runner] changes require a restart."
        text = self.refused(r"was not taken up \(\[runner\] changes require a restart.\); it was put back",
                            lambda: self.complete("bare"))
        self.assertNotIn("not confirmed", text)
        self.unchanged()
        self.assertEqual(self.reloads.waited[-1], self.before.encode())
        self.refused("it was put back.*configuration is as it was; left behind: checkout", self.onboard)
        self.unchanged()

    def test_c5_a_refused_version_somebody_changed_again_is_not_overwritten(self) -> None:
        self.reloads.refuse = "no"
        self.reloads.before_wait = lambda: self.path.write_text("# theirs\n") and None
        text = self.refused("changed by somebody else.*nothing was put back", self.onboard)
        self.assertNotIn("configuration is as it was", text)
        self.assertEqual(self.path.read_text(), "# theirs\n")

    def test_c5_a_version_somebody_changed_before_it_was_confirmed_is_not_called_in_force(self) -> None:
        for theirs in (self.before.replace("# the end", "# theirs"), "not = [valid\n"):
            for call in (self.onboard, lambda: self.complete("bare")):
                with self.subTest(theirs=theirs[-20:], call=call):
                    self.path.write_text(self.before)
                    self.reloads.before_wait = lambda theirs=theirs: self.path.write_text(theirs) and None
                    text = self.refused("changed by somebody else.*their version stays", call)
                    self.assertNotIn("configuration is as it was", text)
                    self.assertNotIn("in force", text)
                    self.assertEqual(self.path.read_text(), theirs)
                    self.assertIn("the copy from before is", text)

    def test_c5_without_hot_reload_nothing_is_done(self) -> None:
        self.reloads.watch(None)
        for call in (self.box.check, lambda: self.complete("bare"), self.onboard):
            self.refused("does not reload its configuration", call)
        self.unchanged()
        self.assertEqual(self.run_.calls, [])

    def test_c1_taken_away_by_a_reload_the_tools_stop_at_once(self) -> None:
        self.allowed = False
        for call in (self.box.check, lambda: self.complete("bare"), self.onboard):
            self.refused("no system tools any more", call)
        self.allowed = True
        self.run_.during_clone = lambda: setattr(self, "allowed", False)
        self.refused("no system tools any more", self.onboard)
        self.unchanged()

    def test_c5_two_calls_at_once_both_count(self) -> None:
        errors: list[BaseException] = []

        def call(work: Callable[[], Any]) -> None:
            try:
                work()
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=call, args=(work,)) for work in (
            lambda: self.onboard("acme/one"), lambda: self.onboard("acme/two"), lambda: self.complete("bare", "b"),
            lambda: self.onboard("acme/one"),
        )]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        config = self.config()
        self.assertEqual(set(config.projects), {"old", "bare", "one", "two"})
        self.assertEqual(set(config.runtime.inbox_bindings[0].forward_to),
                         {"proj:old", "sys:ops", "proj:one", "proj:two", "proj:bare"})
        self.assertEqual(len(self.run_.made("gh", "repo", "clone")), 2)


class HelperTests(unittest.TestCase):

    def test_the_agents_are_those_the_other_projects_use(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "h.toml")
            path.write_text(CONFIG.format(root=tmp).replace(
                "front = {", 'steward = { role = "steward", executor = "claude-code" }\nfront = {'))
            found = project_agents(load_config(path))
        self.assertEqual((found.steward, found.sde, found.reviewer), ("front", "sde", "reviewer"))

    def test_remotes(self) -> None:
        for remote in ("https://github.com/acme/x", "https://github.com/Acme/X.git", "git@github.com:acme/x.git",
                       "ssh://git@github.com/acme/x", "ssh://git@ssh.github.com:443/acme/x.git", "git@work-alias:acme/x"):
            self.assertTrue(_is_remote_of(remote, "acme/x"), remote)
        for remote in ("https://github.com/acme/x2", "https://github.com/evil/acme/x", "https://evil.example/acme/x",
                       "/home/me/acme/x", "", "https://github.com/acme/x/y"):
            self.assertFalse(_is_remote_of(remote, "acme/x"), remote)

    def test_names(self) -> None:
        self.assertEqual(split_repo("Acme/X.y_z-1"), ("Acme", "X.y_z-1"))
        self.assertEqual(project_name(None, "Acme/New-Tool"), "new-tool")
        self.assertEqual(description_text("  words ", "a/b"), "words")
        with self.assertRaises(OnboardError):
            project_name(None, "acme/x.y")

    def test_r8_one_caller_cannot_pass_a_request_that_was_not_checked(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "h.toml")
            path.write_text(CONFIG.format(root=tmp))
            run = FakeCommands()
            onboarding = Onboarding(path, run=run, write=lambda f, a, b: f.replace(a, b), label_owners=lambda _c: ())
            for request in (Request("-x/y", "y", Path(tmp, "y")), Request("x/y", 'y"]', Path(tmp, "y")),
                            Request("x/y", "y", Path(tmp, "y"), description="a\nb")):
                with self.assertRaises((OnboardError, ValueError)):
                    onboarding.onboard(request)
            self.assertEqual(run.calls, [])


class ReloadStatusTests(unittest.TestCase):
    def change_when_waiting(self, status: ReloadStatus, change: Callable[[], None]) -> None:
        entered = threading.Event()
        wait = status._changed.wait

        def observed_wait(timeout: float) -> bool:
            entered.set()
            return wait(timeout)

        def notify() -> None:
            if entered.wait(5):
                change()

        self.enterContext(patch.object(status._changed, "wait", observed_wait))
        thread = threading.Thread(target=notify)
        thread.start()
        self.addCleanup(thread.join, 5)

    def test_c5_a_version_is_in_force_once_it_is_settled(self) -> None:
        status = ReloadStatus()
        status.watch(Path("/x/h.toml"))
        # A spurious wake before the deadline must wait again, using only the remaining budget.
        with (patch("agent_runtime.app.reload.time") as clock, patch.object(status._changed, "wait") as wait):
            clock.monotonic.side_effect = [0.0, 0.0, 0.75, 1.0]
            with self.assertRaisesRegex(ReloadFailed, "within 1 seconds"):
                status.wait(b"a", 1)
            self.assertEqual(wait.call_args_list, [mock.call(1), mock.call(0.25)])
        version = status.seen(b"a")
        with self.assertRaises(ReloadFailed):
            status.wait(b"a", 0)
        self.change_when_waiting(status, lambda: status.settle(version))
        status.wait(b"a", 5)
        status.settle(status.seen(b"b"), "not valid")
        with self.assertRaisesRegex(ReloadFailed, "not valid"):
            status.wait(b"b", 5)

    def test_c5_settling_one_version_says_nothing_of_another(self) -> None:
        status = ReloadStatus()
        status.watch(Path("/x/h.toml"))
        first = status.seen(b"a")
        status.seen(b"b")
        status.settle(first)
        for contents in (b"a", b"b"):
            with self.assertRaises(ReloadFailed):
                status.wait(contents, 0)

    def test_c5_a_version_another_one_replaced_is_not_waited_for(self) -> None:
        status = ReloadStatus()
        status.watch(Path("/x/h.toml"))
        status.seen(b"a")
        self.change_when_waiting(status, lambda: status.seen(b"b"))
        started = time.monotonic()
        with self.assertRaisesRegex(ReloadFailed, "another version"):
            status.wait(b"a", 30)
        self.assertLess(time.monotonic() - started, 10)

    def test_c5_a_daemon_that_stops_ends_the_wait(self) -> None:
        status = ReloadStatus()
        status.watch(Path("/x/h.toml"))
        self.change_when_waiting(status, lambda: status.watch(None))
        started = time.monotonic()
        with self.assertRaises(ReloadFailed):
            status.wait(b"a", 30)
        self.assertLess(time.monotonic() - started, 10)


class ConfigChangesTests(unittest.IsolatedAsyncioTestCase):
    # A person's editor takes no part in the locks: it may write while a version is being parsed.
    dropped = 0

    async def race(self, *writes: str) -> tuple[list[int], list[str | None]]:
        # What is used, and the errors that were settled.
        path = Path(self.enterContext(tempfile.TemporaryDirectory()), "h.toml")
        path.write_text("[runtime]\nmax_concurrency = 1\n")
        pending = list(writes)
        real = config_module.parse_config

        def parse(raw: dict[str, Any], *, base_dir: Path | None = None) -> HibernautConfig:
            if pending:
                path.write_text(pending.pop(0))
            return real(raw, base_dir=base_dir)

        reloads = ReloadStatus()
        errors: list[str | None] = []
        stop, found = asyncio.Event(), list[int]()

        async def collect() -> None:
            async for config in config_module.config_changes(path, stop, interval=0.01, reloads=reloads):
                found.append(config.runtime.max_concurrency)
                if found.count(found[-1]) > self.dropped:
                    reloads.settle(reloads.latest)

        settle = reloads.settle

        def settling(version: str, error: str | None = None) -> None:
            if error is not None:
                errors.append(error)
            settle(version, error)

        # One tick per replaced version, then the final parse, any retry, and one unchanged poll.
        ticks = [TimeoutError] * (len(writes) + self.dropped + 2)
        with (patch.object(stop, "wait", side_effect=[*ticks, True]),
              patch.object(config_module, "parse_config", parse), patch.object(reloads, "settle", settling)):
            await collect()
        return found, errors

    async def test_c5_a_version_replaced_while_it_was_parsed_is_not_used(self) -> None:
        self.assertEqual(await self.race("[runtime]\nmax_concurrency = 2\n"), ([2], []))

    async def test_c5_replaced_by_one_that_is_not_valid_neither_is_used(self) -> None:
        found, errors = await self.race("[broken")
        self.assertEqual(found, [])
        [error] = errors
        self.assertIn("invalid configuration", error or "")

    async def test_c5_an_invalid_one_replaced_while_it_was_parsed_is_not_settled(self) -> None:
        # "nonsense" parses as TOML and is refused by parse_config, after the file was put right.
        found, errors = await self.race('[runtime]\nnew_event_policy = "nonsense"\n', "[runtime]\nmax_concurrency = 3\n")
        self.assertEqual((found, errors), ([3], []))

    async def test_c5_one_that_was_dropped_is_taken_up_again_if_it_is_still_there(self) -> None:
        # Dropped because the file had changed, which is back as it was by the next look.
        self.dropped = 1
        self.assertEqual(await self.race(), ([1, 1], []))

    async def test_c5_changed_and_changed_back_it_is_used(self) -> None:
        found, errors = await self.race("[runtime]\nmax_concurrency = 2\n", "[runtime]\nmax_concurrency = 1\n")
        self.assertEqual((found, errors), ([1], []))


if __name__ == "__main__":
    unittest.main()
