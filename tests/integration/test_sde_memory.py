from __future__ import annotations

import asyncio
import contextlib
import json
import os
import sqlite3
import stat
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from agent_runtime.adapters.executor_cli.memory_access import MemoryAccess, channel_path
from agent_runtime.adapters.executor_cli.orca import HANDOFF_SERVER
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase
from agent_runtime.app.config import load_config, parse_config
from agent_runtime.app.main import build_daemon_from_config, serve_configured
from agent_runtime.core.models import Emit, PermissionDecision, SessionRef, Suspend
from tests.support.fakes import InMemoryPermission
from tests.support.collaboration_wiring import config_of
from tests.support.resume_runs import SDERestartCase, committed, until, verdict_pass
from tests.support.coder_reviewer import CHANGES_REQUESTED
from tests.support.sde_executor import BUGGY, FIXED, PASS, dev, event
from tests.support.visible_roles import arg
from tests.support.sde_memory import (
    ALPHA,
    BETA,
    MemoryCase,
    PERMITTED,
    SERVER,
    Session,
    TOOLS,
    World,
    _toml,
    servers,
    ungranted,
)
from tests.support.sde_memory import world  # noqa: F401 -- pytest fixture, requested by parameter name


UNAVAILABLE = "cannot be reached"


def test_a_session_reads_and_searches_its_project_the_shared_memory_and_the_knowledge_base(world: World) -> None:
    async def scenario() -> None:
        async with world.access.serving(), world.session("sess-alpha") as session:
            listed = (await session.rpc("tools/list"))["result"]["tools"]
            assert sorted(tool["name"] for tool in listed) == list(TOOLS)
            assert "tried in alpha" in await session.read("notes/tried.md")
            assert await session.read("proj://alpha/notes/tried.md", heading="Outcome") == "## Outcome\nno gain in alpha"
            assert "everybody" in await session.read("shared://notes/common.md")
            assert "knowledge base" in await session.read("kb://pkb/card.md")
            assert await session.search("layering") == {"proj://alpha/notes/tried.md", "shared://notes/common.md"}
            assert "kb://pkb/card.md" in await session.search("layering", include_kb=True)
            assert await session.search("layering", include_shared=False) == {"proj://alpha/notes/tried.md"}
            assert "query must be a non-empty string" in await session.refused("memory_search")
            assert "not found" in (await session.refused("memory_read", uri="notes/none.md")).lower()

    asyncio.run(scenario())


@pytest.mark.parametrize("uri", [
    "proj://beta/notes/tried.md", "proj://beta/index.md", "notes/../../beta/notes/tried.md", "../beta/notes/tried.md",
    "proj://alpha/../beta/notes/tried.md", "shared://../projects/beta/notes/tried.md", "/../../../hb.db",
    "kb://pkb/../hb.db", "role://architect/skills.md",
])
def test_another_project_and_paths_out_of_the_memory_are_refused(world: World, uri: str) -> None:
    async def scenario() -> None:
        async with world.access.serving(), world.session("sess-alpha") as session:
            assert "tried in beta" not in await session.refused("memory_read", uri=uri)
            # The scope is the session's; no argument changes it.
            assert not any("beta" in hit for hit in await session.search("layering", scope="beta", inbox_id=BETA))

    asyncio.run(scenario())


def test_the_channel_writes_nothing(world: World) -> None:
    asked = {"uri": "notes/new.md", "target_uri": "notes/new.md", "path": "notes/new.md", "op": "upsert_file",
             "content": "x", "rationale": "x", "proposal_id": "prop_1"}

    async def direct(name: str) -> dict[str, Any]:
        # Past the MCP process, as a session could with its own shell.
        *_, path, token = world.access.mcp_server("sess-alpha")["args"]
        reader, writer = await asyncio.open_unix_connection(path)
        writer.write(json.dumps({"token": token, "name": name, "arguments": asked}).encode() + b"\n")
        answer = json.loads(await reader.readline())
        writer.close()
        return answer

    async def scenario() -> None:
        async with world.access.serving(), world.session("sess-alpha") as session:
            await session.search("layering")
            before = world.files()
            for name in ("memory_propose_write", "write_memory", "memory_approve_proposal", "memory_reject_proposal",
                         "read_memory", ""):
                assert "unknown tool" in await session.refused(name, **asked)
                answer = await direct(name)
                assert answer["error"] and "unknown tool" in answer["text"]
            await session.read("notes/tried.md")
            assert world.files() == before

    asyncio.run(scenario())


@pytest.mark.parametrize("action", ["block", "defer"])
def test_every_call_passes_the_permission_gate_first(world: World, action: Any) -> None:
    class Gate(InMemoryPermission):
        asked: list[tuple[str, dict[str, Any]]] = []
        decision = PermissionDecision(action="allow", reason="")

        def check(self, tool_name: str, args: dict[str, Any]) -> PermissionDecision:
            self.asked.append((tool_name, args))
            return self.decision

    gate, opened = Gate(), Mock(side_effect=world.registry.memory_for)
    gate.asked = []
    world.access.activate(opened, gate)

    async def scenario() -> None:
        async with world.access.serving(), world.session("sess-alpha") as session:
            assert "alpha" in await session.read("notes/tried.md")
            await session.search("layering")
            assert gate.asked == [("memory_read", {"uri": "notes/tried.md"}), ("memory_search", {"query": "layering"})]
            opened.reset_mock()
            gate.decision = PermissionDecision(action=action, reason="closed today")
            for name, args in (("memory_read", {"uri": "notes/tried.md"}), ("memory_search", {"query": "layering"})):
                assert await session.refused(name, **args) == f"{name} is not permitted ({action}): closed today"
            opened.assert_not_called()

    asyncio.run(scenario())


def test_an_experts_session_sees_what_is_mounted_for_it(world: World) -> None:
    for role in ("architect", "critic"):
        folder = world.hub.root / "roles" / role
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "skills.md").write_text(f"layering rules of the {role}\n")

    async def scenario() -> None:
        async with world.access.serving(), world.session("sess-expert") as session:
            assert "rules of the architect" in await session.read("role://architect/skills.md")
            assert "tried in alpha" in await session.read("notes/tried.md")
            assert "everybody" in await session.read("shared://notes/common.md")
            for uri in ("role://critic/skills.md", "proj://beta/notes/tried.md", "shared://elsewhere.md"):
                assert "outside the memory mounted" in await session.refused("memory_read", uri=uri)
            assert await session.search("layering", include_kb=True) == {
                "proj://alpha/notes/tried.md", "shared://notes/common.md", "kb://pkb/card.md",
            }

    asyncio.run(scenario())


def test_sessions_of_two_projects_at_once_see_only_their_own(world: World) -> None:
    async def look(session: Session, own: str, other: str) -> None:
        for _ in range(5):
            assert f"tried in {own}" in await session.read("notes/tried.md")
            assert await session.search("layering", include_shared=False) == {f"proj://{own}/notes/tried.md"}
            assert "outside the memory" in await session.refused("memory_read", uri=f"proj://{other}/notes/tried.md")

    async def scenario() -> None:
        async with world.access.serving(), world.session("sess-alpha") as a, world.session("sess-beta") as b:
            await asyncio.gather(look(a, "alpha", "beta"), look(b, "beta", "alpha"))

    asyncio.run(scenario())


def test_only_an_open_session_that_was_granted_the_tools_gets_answers(world: World) -> None:
    server = world.access.mcp_server("sess-alpha")
    # A session keeps its grant, whichever process asks.
    assert world.process(world.db).mcp_server("sess-alpha") == server
    *_, path, token = server["args"]

    async def scenario() -> None:
        async with world.access.serving():
            for forged in ("", "sess-beta", token[:-1], "x" * len(token)):
                session = await Session.start({**server, "args": [*server["args"][:-1], forged]})
                assert "no session" in await session.refused("memory_read", uri="notes/tried.md")
                await session.end()
            async with world.session("sess-alpha") as session:
                current = world.sessions.get("sess-alpha")
                assert current is not None
                world.sessions.save(replace(current, status="suspended", pending_request_id="req"))
                assert "alpha" in await session.read("notes/tried.md")
                world.sessions.close_session("sess-alpha")
                assert "task is closed" in await session.refused("memory_read", uri="notes/tried.md")

    asyncio.run(scenario())
    assert stat.S_IMODE(os.stat(Path(path).parent).st_mode) == 0o700


def test_an_unreachable_runtime_is_explained_and_the_session_goes_on_after_a_restart(world: World) -> None:
    async def scenario() -> None:
        async with world.session("sess-alpha") as session:
            # Not started yet, stopped, and started again by another process: the same MCP process all along.
            assert sorted(t["name"] for t in (await session.rpc("tools/list"))["result"]["tools"]) == list(TOOLS)
            assert UNAVAILABLE in await session.refused("memory_read", uri="notes/tried.md")
            async with world.access.serving():
                assert "alpha" in await session.read("notes/tried.md")
            assert UNAVAILABLE in await session.refused("memory_search", query="layering")
            db = SQLiteDatabase(world.root / "hb.db")
            try:
                async with world.process(db).serving():
                    assert "alpha" in await session.read("notes/tried.md")
            finally:
                db.close()

    asyncio.run(scenario())


def test_the_socket_left_by_a_killed_process_is_replaced_and_a_successors_is_not_removed(world: World) -> None:
    path = channel_path(world.root / "hb.db")
    assert path is not None

    async def scenario() -> None:
        async with world.session("sess-alpha") as session:
            async with world.access.serving():
                async with world.process(world.db).serving():
                    assert "alpha" in await session.read("notes/tried.md")
                    first = os.stat(path).st_ino
                # The successor removed its own.
                assert not path.exists()
            outgoing = world.access.serving()
            await outgoing.__aenter__()
            async with world.process(world.db).serving():
                assert os.stat(path).st_ino
                await outgoing.__aexit__(None, None, None)
                assert "alpha" in await session.read("notes/tried.md")
            assert first

    asyncio.run(scenario())


def test_failures_inside_the_runtime_are_answers(world: World) -> None:
    def broken(inbox_id: str) -> Any:
        raise sqlite3.OperationalError("disk I/O error")

    async def scenario() -> None:
        async with world.access.serving(), world.session("sess-alpha") as session:
            world.access.activate(broken, world.permissions)
            assert "disk I/O error" in await session.refused("memory_read", uri="notes/tried.md")
            world.access.activate(world.registry.memory_for, world.permissions)
            assert "alpha" in await session.read("notes/tried.md")
            # Whatever the client sends, the process answers or ignores it and goes on.
            assert session.proc.stdin is not None
            session.proc.stdin.write(b"not json\n\xff\xfe\n[]\n" + json.dumps({"method": "notifications/x"}).encode() + b"\n")
            assert (await session.rpc("no/such"))["error"]["code"] == -32601
            for params in ([], {"name": "memory_read", "arguments": []}, {"name": ["x"], "arguments": {}}):
                reply = await session.rpc("tools/call", params)
                assert "error" in reply or reply["result"]["isError"]
            assert "项目" in await session.refused("memory_read", uri="notes/项目.md")
            assert "alpha" in await session.read("notes/tried.md")

    asyncio.run(scenario())


def test_long_answers_are_cut(world: World) -> None:
    (world.hub.project_dir("alpha") / "notes" / "long.md").write_text("layering " * 40_000)

    async def scenario() -> None:
        async with world.access.serving(), world.session("sess-alpha") as session:
            failed, text = await session.call("memory_read", uri="notes/long.md")
            assert not failed and len(text) < 101_000 and "more characters cut" in text

    asyncio.run(scenario())


READ = {"memory": [["memory_read", {"uri": "notes/tried.md"}], ["memory_search", {"query": "layering"}],
                   ["memory_read", {"uri": "proj://beta/notes/tried.md"}]]}


class TestSessionsHaveTheTools(MemoryCase):
    def assert_read(self, answer: dict[str, Any]) -> None:
        (read_failed, read), (search_failed, search), (other_failed, _) = answer["memory"]
        self.assertEqual((read_failed, search_failed, other_failed), (False, False, True))
        self.assertIn("tried in alpha", read)
        self.assertIn("proj://alpha/notes/tried.md", search)
        self.assertEqual(answer["memory_tools"], list(TOOLS))
        self.assertEqual(sorted(answer["memory_permitted"]), PERMITTED)

    async def test_a_one_off_developer_has_them_in_every_round_and_the_reviewer_does_not(self) -> None:
        self.set_plan(sde=[{**dev(BUGGY), **READ}, {**dev(FIXED), **READ}],
                      reviewer=[CHANGES_REQUESTED, PASS])
        async with self.world.access.serving():
            outcome = await self.sde().execute([self.task()], self.session(), self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("review=pass after 2 round", outcome.messages[0])
        first, second = self.answers()
        self.assert_read(first)
        self.assert_read(second)
        for call in [c for c in self.calls("reviewer") if c["mode"] == "print"]:
            self.assertNotIn(SERVER, json.dumps(call["argv"]))

    async def test_a_visible_developer_keeps_them_through_a_restart_and_the_reviewer_does_not_get_them(self) -> None:
        self.set_plan(sde=[{**dev(FIXED, status="needs_input", question="采用哪种方式？"), **READ}, {**dev(FIXED), **READ}],
                      reviewer=[PASS])
        executor = self.sde(terminal=self.runner)
        async with self.world.access.serving():
            asked = await executor.execute([self.task()], self.session(), self.caps)
        assert isinstance(asked, Suspend), asked
        # Suspended: the conversation and its MCP process go on while the next process takes over the channel.
        self.access = self.world.process(self.world.db)
        waiting = self.session(status="suspended", pending_request_id=asked.request_id,
                               checkpoint=asked.session_checkpoint, executor_session_id=asked.executor_session_id)
        approval = event({"approval_request_id": asked.request_id, "approved": True, "comment": "that one"}, inbox_id=ALPHA)
        async with self.access.serving():
            outcome = await self.sde(terminal=self.runner).execute([approval], waiting, self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertEqual([c["turn"] for c in self.calls("sde", "interactive") if "turn" in c], [1, 2])
        first, second = self.answers()
        self.assert_read(first)
        self.assert_read(second)
        launched = self.calls("sde", "interactive")[0]
        self.assertEqual(set(servers(launched["argv"])), {SERVER, HANDOFF_SERVER})
        [reviewer] = [c for c in self.calls("reviewer", "interactive") if "turn" in c]
        self.assertEqual(set(servers(reviewer["argv"])), {HANDOFF_SERVER})
        self.assertNotIn(SERVER, arg(reviewer, "--allowedTools"))

    async def test_a_resumed_conversation_gets_them_again(self) -> None:
        self.set_plan(sde=[{**dev(FIXED), **READ}])
        async with self.world.access.serving():
            first = await self.sde(review=False, terminal=self.runner).execute([self.task()], self.session(), self.caps)
            await self.orca.close_all()
            session = self.session(executor_session_id=first.executor_session_id)
            await self.sde(review=False, terminal=self.runner).execute([self.task(text="and one")], session, self.caps)
        self.assertEqual(["--resume" in c["argv"] for c in self.answers()], [False, True])
        for answer in self.answers():
            self.assert_read(answer)

    async def test_an_outage_does_not_touch_the_delivery(self) -> None:
        self.set_plan(sde=[{**dev(FIXED), **READ}], reviewer=[PASS])
        outcome = await self.sde().execute([self.task()], self.session(), self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("[sde] delivered (review=pass", outcome.messages[0])
        [answer] = self.answers()
        for failed, text in answer["memory"]:
            self.assertTrue(failed)
            self.assertIn(UNAVAILABLE, text)

    async def test_a_session_that_cannot_be_granted_them_starts_without(self) -> None:
        self.access = Mock(spec=MemoryAccess)
        self.access.mcp_server.side_effect = sqlite3.OperationalError("database is locked")
        self.set_plan(sde=[{**dev(FIXED), **READ}])
        outcome = await self.sde(review=False).execute([self.task()], self.session(), self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("[sde] delivered", outcome.messages[0])
        [answer] = self.answers()
        self.assertIsNone(answer["memory"])
        self.assertEqual(answer["memory_permitted"], [])

    async def test_without_the_channel_nothing_is_added(self) -> None:
        self.access = None
        self.index("- [Tried](notes/tried.md)\n")
        self.set_plan(sde=[dev(FIXED)])
        await self.sde(review=False).execute([self.task()], self.session(), self.caps)
        [call] = self.calls("sde")
        self.assertNotIn("--mcp-config", call["argv"])
        self.assertNotIn("memory_read", call["prompt"])


class TestFirstPrompt(MemoryCase):
    async def prompt(self, access: Any) -> str:
        self.access = access
        self.log.unlink(missing_ok=True)
        self.set_plan(sde=[dev(FIXED)])
        await self.sde(review=False).execute([self.task()], self.session(), self.caps)
        return self.calls("sde")[0]["prompt"]

    async def test_it_says_to_open_the_listed_notes_before_starting(self) -> None:
        self.index("- [Tried](notes/tried.md): what was tried\n")
        plain = await self.prompt(None)
        prompt = await self.prompt(self.world.access)
        added = prompt.replace(plain[:plain.index("## Task requirements")], "").removesuffix(
            plain[plain.index("## Task requirements"):]).strip()
        self.assertTrue(added.startswith("## Project memory tools\n"), added)
        for part in ("index.md", "memory_read", "memory_search", "Before you start", "read-only"):
            self.assertIn(part, added)
        self.assertLess(prompt.index("notes/tried.md"), prompt.index("## Project memory tools"))

    async def test_without_an_index_it_is_unchanged(self) -> None:
        for text in ("", " \n"):
            self.index(text)
            self.assertEqual(await self.prompt(self.world.access), await self.prompt(None))

    async def test_a_session_that_cannot_be_granted_the_tools_is_not_told_to_call_them(self) -> None:
        self.index("- [Tried](notes/tried.md): what was tried\n")
        plain, granted = await self.prompt(None), await self.prompt(self.world.access)
        self.assertIn("memory_read", granted)
        with ungranted(self.world.db), self.assertLogs(level="ERROR") as logged:
            denied = await self.prompt(self.world.access)
        self.assertEqual(denied, plain)
        self.assertNotIn("memory_read", denied)
        self.assertIn("starts without the memory tools", "".join(logged.output))
        self.assertEqual(await self.prompt(self.world.access), granted)

    async def test_in_a_terminal_too_and_the_task_is_delivered(self) -> None:
        self.index("- [Tried](notes/tried.md): what was tried\n")
        self.set_plan(sde=[{**dev(FIXED), **READ}], reviewer=[PASS])
        with ungranted(self.world.db):
            outcome = await self.sde(terminal=self.runner).execute([self.task()], self.session(), self.caps)
        assert isinstance(outcome, Emit), outcome
        self.assertIn("[sde] delivered (review=pass", outcome.messages[0])
        self.assertEqual(outcome.status, "done")
        [call] = [c for c in self.calls("sde", "interactive") if "turn" in c]
        self.assertNotIn("memory_read", call["prompt"])
        self.assertIn("notes/tried.md", call["prompt"])
        [answer] = self.answers()
        self.assertIsNone(answer["memory"])

    async def test_it_depends_on_the_launch_not_on_when_the_prompt_was_written(self) -> None:
        self.index("- [Tried](notes/tried.md): what was tried\n")
        for denied_at_first in (False, True):
            with self.subTest(denied_at_first=denied_at_first):
                self.log.unlink(missing_ok=True)
                self.set_plan(sde=[dev(FIXED)])
                permissions, self.caps.permissions = self.caps.permissions, Mock()
                self.caps.permissions.check.return_value = PermissionDecision(action="defer", reason="ask first")
                with ungranted(self.world.db) if denied_at_first else contextlib.nullcontext():
                    asked = await self.sde(review=False).execute([self.task()], self.session(), self.caps)
                self.caps.permissions = permissions
                assert isinstance(asked, Suspend), asked
                waiting = self.session(status="suspended", pending_request_id=asked.request_id,
                                       checkpoint=asked.session_checkpoint)
                approval = event({"approval_request_id": asked.request_id, "approved": True}, inbox_id=ALPHA)
                with contextlib.nullcontext() if denied_at_first else ungranted(self.world.db):
                    await self.sde(review=False).execute([approval], waiting, self.caps)
                [call] = self.calls("sde")
                self.assertEqual("memory_read" in call["prompt"], denied_at_first)
                self.assertEqual("--mcp-config" in call["argv"], denied_at_first)


class TestPeerHandoff(SDERestartCase):
    async def test_the_tools_outlive_the_process_that_started_the_session(self) -> None:
        (self.root / "world").mkdir()
        world = World(self.root / "world")
        self.addCleanup(world.close)
        self.sde_extra = {"memory": world.access}
        self.orca.scripts += [[None], [verdict_pass]]
        evt = self.submit()
        worker, runs = self.process(self.db, "old")
        async with world.access.serving():
            await self.stop_when(worker, runs, "develop")
            session_id = runs.list_open(evt.inbox_id)[0].session_id
            world.sessions.save(SessionRef(session_id=session_id, inbox_id=ALPHA, executor_id="sde"))
            argv = self.orca.specs[0]["exec"]
            self.assertEqual(set(servers(argv)), {SERVER})
            self.assertLessEqual({*PERMITTED, "SendMessage"}, set(argv[argv.index("--allowedTools") + 1].split(",")))
            session = await Session.start(servers(argv)[SERVER])
            self.assertIn("alpha", await session.read("notes/tried.md"))
        self.assertIn(UNAVAILABLE, await session.refused("memory_read", uri="notes/tried.md"))
        developer = self.orca.session(0)
        [task] = self.tasks(developer)
        committed(self.repo, "def divide(a, b):\n    if b == 0:\n        raise ValueError\n    return a / b\n")(developer, task)

        self.sde_extra = {"memory": world.process(world.db)}
        worker, _, db = self.restarted()
        async with self.sde_extra["memory"].serving():
            [report] = await asyncio.wait_for(worker.process_inbox(evt.inbox_id), 20)
            self.assertIsInstance(report.outcome, Emit)
            self.assertIn("alpha", await session.read("notes/tried.md"))
        self.assertEqual(await session.end(), 0)
        self.assertIn("review=pass", "".join(self.outbox(db)))
        reviewer = self.orca.specs[1]["exec"]
        self.assertNotIn(SERVER, json.dumps(reviewer))


def _ask(daemon: Any, session_id: str, inbox_id: str) -> dict[str, Any]:
    daemon.sessions.save(SessionRef(session_id=session_id, inbox_id=inbox_id, executor_id="sde"))
    return daemon.memory_access.mcp_server(session_id)


def test_the_daemon_serves_its_sessions_until_it_ends_and_the_next_one_goes_on(tmp_path: Path) -> None:
    config = parse_config({
        "runtime": {"db_path": str(tmp_path / "hb.db"), "memory_root": str(tmp_path / "memory"), "default_executor": "sde"},
        "executors": {"sde": {"role": "sde", "review": False}},
        "projects": {"alpha": {"inbox_id": ALPHA, "repo": "o/alpha", "checkout": str(tmp_path)}},
    })

    async def scenario() -> None:
        session: Session | None = None
        for _ in range(2):
            daemon = build_daemon_from_config(config)
            server = _ask(daemon, "sess-1", ALPHA)
            assert daemon.memory_hub is not None and daemon.memory_access is not None
            daemon.memory_hub.lay_out("alpha")
            (daemon.memory_hub.project_dir("alpha") / "notes" / "tried.md").write_text("layering in alpha\n")
            session = session or await Session.start(server)
            stop = asyncio.Event()
            running = asyncio.create_task(daemon.run(stop_event=stop))
            try:
                await until(lambda: running.done() or Path(server["args"][-2]).exists(), what="the channel")
                assert "alpha" in await session.read("notes/tried.md")
            finally:
                stop.set()
                await asyncio.wait_for(running, 20)
                daemon.close()
            assert UNAVAILABLE in await session.refused("memory_read", uri="notes/tried.md")
        assert session is not None and await session.end() == 0

    asyncio.run(scenario())


def test_a_reload_puts_the_new_configurations_scopes_in_force(tmp_path: Path) -> None:
    path = tmp_path / "hibernaut.toml"
    raw = config_of(tmp_path, channels={"port": 0})
    path.write_text(_toml(raw))

    async def scenario() -> None:
        config = load_config(path)
        daemon = build_daemon_from_config(config)
        assert daemon.memory_hub is not None
        server = _ask(daemon, "sess-1", "proj:hibernaut:architect")
        folder = daemon.memory_hub.root / "roles" / "architect"
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "skills.md").write_text("rules of the architect\n")
        stop = asyncio.Event()
        session = await Session.start(server)
        running = asyncio.create_task(serve_configured(path, config, daemon, stop, reload_interval=0.02))
        try:
            await until(lambda: running.done() or Path(server["args"][-2]).exists(), what="the channel")
            assert "architect" in await session.read("role://architect/skills.md")
            for project in raw["projects"].values():
                project["experts"] = []
            path.write_text(_toml(raw))

            async def dismissed() -> bool:
                return (await session.call("memory_read", uri="role://architect/skills.md"))[0]

            async with asyncio.timeout(20):
                while not await dismissed():
                    await asyncio.sleep(0.02)
            assert UNAVAILABLE not in await session.refused("memory_read", uri="role://architect/skills.md")
        finally:
            stop.set()
            await asyncio.wait_for(running, 20)
            daemon.close()
            await session.end()

    asyncio.run(scenario())
