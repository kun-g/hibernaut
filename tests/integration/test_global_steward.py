from __future__ import annotations

import asyncio
import contextlib
import json
import os
import socket
import sqlite3
import tempfile
import time
import tomllib
import unittest
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any
from unittest.mock import patch

from agent_runtime.adapters.channels.switchboard_bridge import SwitchboardChannelAdapter, SwitchboardInboundBridge
from agent_runtime.adapters.permissions import AutoModePermissionAdapter
from agent_runtime.app.config import load_config, parse_config
from agent_runtime.app.main import build_daemon_from_config, serve_configured
from agent_runtime.core.models import FORWARD_SOURCE, RETURN_SOURCE, Event, PermissionDecision, SessionRef
from agent_runtime.engine import OutboxDispatcher
from tests.support.fakes import RecordingChannel
from tests.support.steward_terminal import steward_cli_terminal
from tests.support.switchboard_support import Harness, make_harness, post
from tests.support.coder_reviewer import fake_command, git
from tests.support.sde_executor import make_base_repo
from tests.support.steward import arg, git_state
from tests.support.switchboard_inbound import feishu_message
from tests.support.visible_roles import FakeOrcaTerminals, poll_test_terminals
from tests.support.global_steward import ASH, forward, reply


CHAT = "feishu:p2p:oc_kun"
CONFIG = """
[channels]
host = "127.0.0.1"
port = {port}

[runtime]
db_path = "{root}/x.db"
memory_root = "{root}/memory"
default_agent = "ash"
worktree_sweep_interval_seconds = 0

[[runtime.inbox_bindings]]
pattern = "user:main"
default_agent = "ash"
agents = ["ash"]
workdir = "{root}/workspace"
checkout_access = "readonly"
forward_to = ["proj:a", "proj:b"]

[launchers.fake]
command = "{command}"

[executors.claude-code]
launcher = "fake"

[agents.ash]
role = "steward"
executor = "claude-code"
model = "claude-sonnet-5-5"
description = "the general assistant"

[agents.front]
role = "steward"
executor = "claude-code"
model = "claude-sonnet-5-5"

[agents.sde]
role = "sde"
executor = "claude-code"
review = false
interactive = false
description = "develops tasks"

[projects.a]
repo = "acme/a"
checkout = "{repo}"
worktree_backend = "git"
terminal_backend = "orca"
inbox_id = ["proj:a", "proj:a:.+"]
default_agent = "sde"
description = "the calculator"

[projects.b]
repo = "acme/b"
checkout = "{repo}"
worktree_backend = "git"
terminal_backend = "orca"
inbox_id = ["proj:b", "proj:b:.+"]
default_agent = "sde"

[[projects.b.inbox_bindings]]
pattern = "proj:b"
default_agent = "front"
agents = ["front"]

[[projects.b.inbox_bindings]]
pattern = "proj:b:sde(:.*)?"
default_agent = "sde"
agents = ["sde"]

[projects.c]
repo = "acme/c"
checkout = "{repo}"
worktree_backend = "git"
terminal_backend = "orca"
inbox_id = "proj:c"
default_agent = "sde"
"""


class AshCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        origin = make_base_repo(self.root)
        self.repo = self.root / "checkout"
        self.workspace = self.root / "workspace"
        for path in (self.repo, self.workspace):
            git(self.root, "clone", "-q", str(origin), str(path))
            git(path, "config", "user.email", "t@example.invalid")
            git(path, "config", "user.name", "t")
        (self.workspace / "about-kun.md").write_text("the workspace\n")
        self.log = self.root / "calls.jsonl"
        self.plan = self.root / "plan.json"
        self.enterContext(patch.dict(os.environ, {"FAKE_CLAUDE_LOG": str(self.log), "FAKE_PLAN": str(self.plan)}))
        self.orca = FakeOrcaTerminals(self.root)
        self.enterContext(poll_test_terminals(self.root))
        self.enterContext(patch("agent_runtime.adapters.executor_cli.orca._orca", new=self.orca))
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.channel: Any = RecordingChannel()
        self.daemon: Any = None
        self.sent_events = 0

    async def asyncTearDown(self) -> None:
        await self.orca.close_all()

    def text(self) -> str:
        return CONFIG.format(root=self.root, repo=self.repo, command=fake_command(self.root), port=self.port)

    def set_plan(self, steward: Sequence[dict[str, Any]], sde: Sequence[dict[str, Any]] = ()) -> None:
        self.plan.write_text(json.dumps({"steward": list(steward), "sde": list(sde)}))
        for role in ("steward", "sde"):
            Path(f"{self.plan}.{role}.n").unlink(missing_ok=True)

    def open(self) -> Any:
        if self.daemon is None:
            self.daemon = build_daemon_from_config(parse_config(tomllib.loads(self.text())), channel=self.channel)
            self.addCleanup(self.daemon.close)
            self.daemon.pool.bind(asyncio.get_running_loop())
        return self.daemon

    async def drain(self) -> None:
        stop = asyncio.Event()
        pool = asyncio.create_task(self.daemon.pool.run_forever(stop))
        await self.daemon.pool.wait_until_idle(60)
        stop.set()
        await pool

    async def say(self, text: str, **payload: Any) -> None:
        daemon = self.open()
        self.sent_events += 1
        daemon.handler.ingest(inbox_id=ASH, source="switchboard:feishu", external_id=f"m{self.sent_events}",
                              payload={"text": text, "reply_channel": "feishu", "reply_target": "oc_kun", **payload},
                              correlation_key=CHAT)
        await self.drain()

    def rows(self, sql: str) -> list[Any]:
        with contextlib.closing(sqlite3.connect(self.root / "x.db")) as conn, conn:
            return conn.execute(sql).fetchall()

    def calls(self) -> list[dict[str, Any]]:
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def launches(self) -> list[dict[str, Any]]:
        return [c for c in self.calls() if "answers" not in c and c["role"] != "probe"]

    def answers(self) -> list[list[Any]]:
        return [a for c in self.calls() if "answers" in c for a in c["answers"]]

    def messages(self, kind: str = "emit") -> list[Any]:
        return [m for m in self.daemon.outbox.fetch_pending(limit=100) if m.metadata["kind"] == kind]

    def events(self, source: str) -> list[Event]:
        return sorted((e for e in self.daemon.inbox.list_events(limit=100) if e.source == source), key=lambda e: e.seq)


WORK = {"write": {"calc.py": "def divide(a, b):\n    return a / b\n"}}


class RunTests(AshCase):
    async def test_it_runs_in_the_binding_s_directory_and_reads_there(self) -> None:
        self.set_plan([{"calls": [{"tool": "read_file", "arguments": {"path": "about-kun.md"}}], **reply("hello")}])
        before = git_state(self.workspace)
        await self.say("测试")
        [call] = self.launches()
        self.assertEqual(os.path.realpath(call["cwd"]), str(self.workspace))
        self.assertIn("the workspace", self.answers()[0][1])
        [message] = self.messages()
        self.assertEqual((message.inbox_id, message.content, message.metadata["correlation_key"],
                          message.metadata["reply_target"]), (ASH, "hello", CHAT, "oc_kun"))
        self.assertEqual(git_state(self.workspace), before)
        self.assertEqual(self.daemon.db.fetchall("SELECT * FROM workspaces"), [])
        [term] = self.orca.created()
        self.assertEqual(Path(term["worktree"]), self.workspace)

    async def test_files_and_git_stay_in_the_directory(self) -> None:
        self.set_plan([{"calls": [
            {"tool": "read_file", "arguments": {"path": "../checkout/calc.py"}},
            {"tool": "read_file", "arguments": {"path": str(self.repo / "calc.py")}},
            {"tool": "git", "arguments": {"args": ["log", "--", "../checkout"]}},
        ]}])
        await self.say("look")
        self.assertEqual([failed for failed, _ in self.answers()], [True] * 3)

    async def test_the_prompt_speaks_of_no_single_project(self) -> None:
        self.set_plan([])
        await self.say("hi")
        system = arg(self.launches()[0], "--append-system-prompt")
        for phrase in ("one project", "main checkout", "Project memory", "project memory"):
            self.assertNotIn(phrase, system)
        for phrase in ("belongs to no project", "person's own words", "recommend", "continues"):
            self.assertIn(phrase, system)
        self.assertIn("测试", system)

    async def test_skills_of_the_deployment_stay_in_their_order(self) -> None:
        skill = self.root / "SKILL.md"
        skill.write_text("Speak like Ash.\n")
        text = self.text().replace('description = "the general assistant"',
                                   f'skills = ["builtin:steward", "{skill}"]')
        with patch.object(self, "text", lambda: text):
            await self.say("hi")
        system = arg(self.launches()[0], "--append-system-prompt")
        self.assertLess(system.index("# Two things stay two"), system.index("Speak like Ash."))
        self.assertNotIn("# Choosing an inbox", system)

    async def test_the_manifest_names_the_listed_projects_and_nothing_inside_them(self) -> None:
        self.set_plan([])
        await self.say("hi")
        manifest = arg(self.launches()[0], "--append-system-prompt").split("# Routing manifest")[1]
        for phrase in ("`a`", "`proj:a`", "the calculator", "`sde`", "`b`", "`proj:b`", "`front`", "`user:main`"):
            self.assertIn(phrase, manifest)
        for phrase in ("proj:c", "proj:b:sde", "develops tasks", "acme/"):
            self.assertNotIn(phrase, manifest)

    async def test_a_project_steward_is_told_what_it_was_told(self) -> None:
        self.set_plan([{"calls": [forward("proj:b", "go")]}, {}])
        await self.say("go")
        system = arg(self.launches()[1], "--append-system-prompt")
        self.assertIn("looks after one project", system)
        self.assertIn("main checkout", system)
        self.assertIn("Project `b`.", system)
        self.assertNotIn("belongs to no project", system)

    async def test_another_branch_waits_for_a_person(self) -> None:
        git(self.workspace, "checkout", "-qb", "elsewhere")
        text = self.text().replace('description = "the general assistant"', 'branch = "release"')
        self.set_plan([reply("ok")])
        with patch.object(self, "text", lambda: text):
            await self.say("hi")
        self.assertEqual(self.calls(), [])
        [card] = self.messages("approval_request")
        self.assertIn("'elsewhere'", card.content)

    async def test_without_an_orca_terminal_it_waits_like_a_project_steward(self) -> None:
        async def broken(*args: str, cwd: Path) -> dict[str, Any]:
            raise RuntimeError("Orca is not running")

        self.set_plan([reply("ok"), reply("ok")])
        with patch("agent_runtime.adapters.executor_cli.orca._orca", new=broken):
            await self.say("hi")
            self.daemon.handler.ingest(inbox_id="proj:b", source="switchboard:feishu", external_id="b",
                                       payload={"text": "hi"}, correlation_key="feishu:p2p:oc_b")
            await self.drain()
        cards = {m.inbox_id: m for m in self.messages("approval_request")}
        self.assertEqual(set(cards), {ASH, "proj:b"})
        for card in cards.values():
            self.assertIn("Orca is not running", card.content)
            self.assertIn("Approve to try again", card.content)
        self.assertFalse(self.log.exists())
        self.daemon.handler.ingest(inbox_id=ASH, source="switchboard:feishu", external_id="d", payload={
            "event_type": "approval.decided", "approved": True,
            "approval_request_id": cards[ASH].metadata["approval_request_id"]})
        await self.drain()
        self.assertEqual([m.content for m in self.messages()], ["ok"])

    async def test_a_missing_directory_holds_the_event(self) -> None:
        self.workspace.rename(self.root / "gone")
        self.set_plan([reply("ok")])
        with self.assertLogs("agent_runtime.app.projects", "WARNING"):
            await self.say("hi")
        self.assertEqual(self.calls(), [])
        self.assertEqual([e.status for e in self.events("switchboard:feishu")], ["pending"])


class RoutingCase(AshCase):
    def setUp(self) -> None:
        super().setUp()
        self.enterContext(steward_cli_terminal(self.root))


class ForwardRuleTests(RoutingCase):
    async def test_only_listed_main_inboxes_take_work(self) -> None:
        targets = ["proj:c", "proj:a:sde", "proj:b:sde", "user:two", "sys:ops", "proj:none"]
        self.set_plan([{"calls": [*(forward(t, "x") for t in targets),
                                  forward("proj:a", "x", target_agent="sde"),
                                  forward("proj:a", "x", return_to="proj:a"),
                                  forward("proj:a", "x", return_to="proj:b"),
                                  forward("proj:a", "x", continues="made up")]}])
        await self.say("go")
        self.assertEqual([failed for failed, _ in self.answers()], [True] * 10)
        self.assertEqual(self.events(FORWARD_SOURCE), [])

    async def test_a_project_cannot_start_work_in_the_chat_s_inbox(self) -> None:
        self.set_plan([{"calls": [forward("proj:b", "go", return_to=ASH)]},
                       {"calls": [forward(ASH, "x"), forward("proj:a", "x"), forward("proj:b:sde", "x", return_to="proj:a")],
                        **reply("done")},
                       reply("all done")])
        await self.say("go")
        self.assertEqual([failed for failed, _ in self.answers()], [False, True, True, True])
        self.assertEqual([m.content for m in self.messages()], ["all done"])

    async def approve_under(self, forward_to: str) -> list[str]:
        check = AutoModePermissionAdapter.check

        def gate(adapter: AutoModePermissionAdapter, tool: str, args: dict[str, Any]) -> PermissionDecision:
            if tool == "inbox.forward":
                return PermissionDecision("defer", "ask first")
            return check(adapter, tool, args)

        with patch.object(AutoModePermissionAdapter, "check", gate):
            self.set_plan([{"calls": [forward("proj:a", "go")], **reply("sent")}, reply("could not send")], [WORK])
            await self.say("go")
            [card] = self.messages("approval_request")
            self.assertIn("proj:a", card.content)
            # What a hot reload leaves behind: a new generation over the same store.
            old, text = self.daemon, self.text().replace('["proj:a", "proj:b"]', forward_to)
            self.daemon = build_daemon_from_config(parse_config(tomllib.loads(text)), channel=self.channel,
                                                   database=old.db, memory_hub=old.memory_hub)
            self.daemon.pool.bind(asyncio.get_running_loop())
            self.daemon.handler.ingest(inbox_id=ASH, source="switchboard:feishu", external_id="d", payload={
                "event_type": "approval.decided", "approved": True,
                "approval_request_id": card.metadata["approval_request_id"]})
            await self.drain()
        return [c["prompt"] for c in self.launches() if c["role"] == "steward"]

    async def test_an_approved_forward_goes_out_while_it_is_listed(self) -> None:
        asked = await self.approve_under('["proj:a"]')
        self.assertEqual((len(asked), len(self.events(FORWARD_SOURCE))), (1, 1))

    async def test_an_approved_forward_is_refused_once_it_is_not(self) -> None:
        asked = await self.approve_under('["proj:b"]')
        self.assertEqual((len(asked), self.events(FORWARD_SOURCE)), (2, []))
        self.assertIn("not an inbox of this project", asked[-1])

    async def test_sessions_and_events_of_the_listed_projects_only(self) -> None:
        daemon = self.open()
        for inbox_id in ("proj:a", "proj:a:sde", "proj:b", "proj:c", "user:two", ASH):
            daemon.sessions.save(SessionRef(session_id=f"s-{inbox_id}", inbox_id=inbox_id))
            daemon.inbox.append(Event(inbox_id=inbox_id, source="switchboard:x", external_id="old", status="done",
                                      payload={"text": f"text of {inbox_id}"}))
        self.set_plan([{"calls": [
            {"tool": "list_sessions", "arguments": {}}, {"tool": "list_events", "arguments": {"limit": 100}},
            *({"tool": tool, "arguments": {"inbox_id": i}} for tool in ("list_sessions", "list_events")
              for i in (ASH, "proj:a", "proj:a:sde", "proj:b", "proj:c", "user:two", "proj:none")),
        ]}])
        self.rows("UPDATE event_inbox SET status = 'done'")
        await self.say("what is going on")
        rows = self.answers()
        for text in (rows[0][1], rows[1][1]):
            found = {row["inbox_id"] for row in json.loads(text)}
            self.assertEqual(found, {ASH, "proj:a", "proj:a:sde", "proj:b"})
        for at in (2, 9):
            self.assertEqual([failed for failed, _ in rows[at:at + 7]],
                             [False, False, False, False, True, True, True])


class ChatCase(RoutingCase):
    # Messages come in and answers go out through a real Switchboard, with Feishu's API faked.
    def setUp(self) -> None:
        super().setUp()
        self.board: Harness = make_harness(routing={"feishu_chats": {"oc_kun": ASH}}, feishu={"bot_open_id": "ou_bot"})
        self.addCleanup(self.board.close)
        self.channel = SwitchboardChannelAdapter(self.board.board.bus.publish, fallback=RecordingChannel())
        self.board.board.bus.subscribe("taskboard", lambda e: None)

    def open(self) -> Any:
        if self.daemon is None:
            daemon = super().open()
            self.board.board.bus.subscribe("hibernaut", SwitchboardInboundBridge(daemon.handler).handle_canonical_event)
        return self.daemon

    async def chat(self, text: str) -> None:
        self.open()
        self.sent_events += 1
        message = feishu_message(chat_id="oc_kun", message_id=f"om_{self.sent_events}")
        message["event"]["message"]["content"] = json.dumps({"text": text})
        message["header"]["event_id"] = f"fe_{self.sent_events}"
        status, _ = await asyncio.to_thread(post, f"{self.board.url}/webhooks/feishu", message)
        self.assertEqual(status, 200)
        self.board.board.worker.run_once()
        await self.drain()
        await OutboxDispatcher(outbox=self.daemon.outbox, channel=self.channel).dispatch_once()
        self.board.board.worker.run_once()
        self.board.board.gateway.run_once()

    def replies(self) -> list[tuple[str, str]]:
        return [(c["json"]["receive_id"], c["json"]["content"]) for c in self.board.http.calls
                if c["json"]["receive_id"] == "oc_kun"]


class EndToEndTests(ChatCase):
    async def test_a_question_is_answered_in_the_chat(self) -> None:
        self.set_plan([reply("收到")])
        await self.chat("测试")
        [(_target, content)] = self.replies()
        self.assertIn("收到", content)
        self.assertEqual(self.events(FORWARD_SOURCE), [])

    async def test_the_agent_that_took_the_work_answers_the_chat(self) -> None:
        self.set_plan([{"calls": [forward("proj:a", "divide must reject zero", title="Zero")]}], [WORK])
        await self.chat("divide is broken")
        self.assertEqual([c["role"] for c in self.launches()], ["steward", "sde"])
        [(_target, content)] = self.replies()
        self.assertIn("implemented", content)
        [session] = self.daemon.sessions.list_sessions(inbox_id="proj:a")
        self.assertIn("divide must reject zero", self.launches()[1]["prompt"])
        self.assertEqual([e.inbox_id for e in self.events(FORWARD_SOURCE)], ["proj:a"])
        self.assertTrue(session.correlation_key.startswith("forward:user:main:"))

    async def test_a_result_ash_asked_for_is_summed_up_in_the_chat(self) -> None:
        self.set_plan([{"calls": [forward("proj:a", "part one", return_to=ASH)]}, reply("checked: all good")], [WORK])
        await self.chat("do it and tell me")
        self.assertEqual([c["role"] for c in self.launches()], ["steward", "sde", "steward"])
        self.assertIn("implemented", self.launches()[2]["prompt"])
        self.assertIn("--resume", self.launches()[2]["argv"])
        [(_target, content)] = self.replies()
        self.assertIn("checked: all good", content)
        [back] = self.events(RETURN_SOURCE)
        self.assertEqual((back.inbox_id, back.payload["forwarded_from"]), (ASH, "proj:a"))

    async def test_two_things_sent_to_one_project_stay_two(self) -> None:
        self.set_plan([{"calls": [forward("proj:a", "first thing")]}, {"calls": [forward("proj:a", "second thing")]}],
                      [{"write": {"one.py": "1\n"}}, {"write": {"two.py": "2\n"}}])
        await self.chat("first")
        await self.chat("second")
        # One conversation of ash, two sessions and two worktrees of the project.
        self.assertEqual(len(self.daemon.sessions.list_sessions(inbox_id=ASH)), 1)
        sessions = self.daemon.sessions.list_sessions(inbox_id="proj:a")
        self.assertEqual(len(sessions), 2)
        self.assertEqual(len({s.work_key for s in sessions}), 2)
        self.assertEqual(len(set(self.rows("SELECT workdir FROM workspaces WHERE inbox_id = 'proj:a'"))), 2)
        dirs = [Path(c["cwd"]) for c in self.launches() if c["role"] == "sde"]
        self.assertEqual([sorted(p.name for p in d.glob("*.py")) for d in dirs],
                         [["calc.py", "one.py"], ["calc.py", "two.py"]])
        self.assertEqual(len(self.replies()), 2)

    async def test_the_two_stay_two_behind_a_project_steward(self) -> None:
        self.set_plan([{"calls": [forward("proj:b", "first thing")]}, {"calls": [forward("proj:b:sde", "first")]},
                       {"calls": [forward("proj:b", "second thing")]}, {"calls": [forward("proj:b:sde", "second")]}],
                      [WORK, WORK])
        await self.chat("first")
        await self.chat("second")
        self.assertEqual([c["role"] for c in self.launches()], ["steward", "steward", "sde"] * 2)
        dirs = {c["cwd"] for c in self.launches() if c["role"] == "sde"}
        self.assertEqual(len(dirs), 2)
        self.assertEqual(len(self.daemon.sessions.list_sessions(inbox_id="proj:b:sde")), 2)
        self.assertEqual(len(self.replies()), 2)

    async def test_an_addition_goes_to_the_thing_it_belongs_to(self) -> None:
        self.set_plan([{"calls": [forward("proj:a", "first thing")]}, {"calls": [forward("proj:a", "second thing")]}],
                      [WORK, WORK])
        await self.chat("first")
        await self.chat("second")
        key = self.events(FORWARD_SOURCE)[0].correlation_key
        # The receipt names it, and so do the lists.
        self.assertIn(key or "", self.answers()[0][1])
        self.set_plan([{"calls": [{"tool": "list_sessions", "arguments": {"inbox_id": "proj:a"}},
                                  forward("proj:a", "and reject text", continues=key)]}],
                      [{"write": {"calc.py": "x = 3\n"}}])
        await self.chat("for the first one: reject text too")
        self.assertIn(key or "", self.answers()[-2][1])
        self.assertIn(key or "", self.answers()[-1][1])
        sde = [c for c in self.launches() if c["role"] == "sde"]
        self.assertEqual(len(sde), 3, self.calls()[-3:])
        self.assertEqual((sde[2]["cwd"], sde[2]["cwd"] == sde[1]["cwd"]), (sde[0]["cwd"], False))
        self.assertEqual(len(self.daemon.sessions.list_sessions(inbox_id="proj:a")), 2)
        self.assertEqual(len(self.replies()), 3)

    async def test_an_approval_of_the_project_goes_where_approvals_go(self) -> None:
        self.set_plan([{"calls": [forward("proj:a", "go")]}], [{"output": {
            "status": "needs_input", "summary": "", "test_evidence": "", "question": "Which one?", "change_kind": "fix",
            "change_rationale": "x"}}])
        await self.chat("go")
        [card] = [c for c in self.board.http.calls if c["json"]["msg_type"] == "interactive"]
        self.assertEqual(card["json"]["receive_id"], "oc_approvals")


class ReloadTests(RoutingCase):
    async def wait_for(self, check: Callable[[], bool], what: str) -> None:
        deadline = time.monotonic() + 30
        while not check():
            self.assertLess(time.monotonic(), deadline, what)
            await asyncio.sleep(0.02)

    async def serve(self) -> tuple[Path, Any]:
        path = self.root / "hibernaut.toml"
        path.write_text(self.text())
        config = load_config(path)
        self.daemon = build_daemon_from_config(config, channel=self.channel)
        self.addCleanup(self.daemon.close)
        stop = asyncio.Event()
        serving = asyncio.create_task(serve_configured(path, config, self.daemon, stop, reload_interval=0.02))

        async def ended() -> None:
            stop.set()
            await serving

        self.addAsyncCleanup(ended)
        return path, serving

    async def remove_a(self, path: Path) -> None:
        path.write_text(self.text().replace('["proj:a", "proj:b"]', '["proj:b"]'))
        targets = self.daemon.forward_targets
        await self.wait_for(lambda: targets.allowed(ASH, ("proj:a", "proj:b")) == ("proj:b",), "the reload")

    def post(self, text: str) -> None:
        self.sent_events += 1
        self.daemon.handler.ingest(inbox_id=ASH, source="switchboard:feishu", external_id=f"m{self.sent_events}",
                                   payload={"text": text}, correlation_key=CHAT)

    async def test_a_wrong_entry_is_refused_and_the_list_in_force_stays(self) -> None:
        path, _ = await self.serve()
        for wrong in ('["proj:a", "proj:zzz"]', '["proj:a", "proj:b:sde"]', "[]"):
            path.write_text(self.text().replace('["proj:a", "proj:b"]', wrong))
            await asyncio.sleep(0.2)
        self.set_plan([{"calls": [forward("proj:a", "go")]}], [WORK])
        self.post("go")
        await self.wait_for(lambda: len(self.events(FORWARD_SOURCE)) == 1, "the list in force")

    async def test_a_removed_project_takes_nothing_from_the_next_run(self) -> None:
        path, _ = await self.serve()
        await self.remove_a(path)
        self.set_plan([{"calls": [forward("proj:a", "go"), {"tool": "list_events", "arguments": {"inbox_id": "proj:a"}}],
                        **reply("no")}])
        self.post("go")
        await self.wait_for(lambda: bool(self.answers()), "the run")
        self.assertEqual([failed for failed, _ in self.answers()], [True, True])

    async def test_a_run_that_started_before_the_removal_sends_nothing_there(self) -> None:
        path, _ = await self.serve()
        go_on = self.root / "go-on"
        self.set_plan([{"calls": [forward("proj:a", "go", title="Plan")], "wait_for": str(go_on), **reply("sent")}])
        self.post("go")
        await self.wait_for(lambda: bool(self.answers()), "the forward")
        self.assertEqual([failed for failed, _ in self.answers()], [False])
        await self.remove_a(path)
        go_on.touch()
        await self.wait_for(lambda: self.events("switchboard:feishu")[0].status == "done", "its end")
        self.assertEqual(self.events(FORWARD_SOURCE), [])
        [(notice,)] = self.rows("SELECT content FROM outbox WHERE content LIKE '%Did not forward%'")
        self.assertIn("proj:a (Plan)", notice)


if __name__ == "__main__":
    unittest.main()
