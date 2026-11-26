from __future__ import annotations
import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path
from agent_runtime.adapters.executor_cli import (
    CLITemplate,
    CommandResult,
    LocalCLIExecutor,
    OrcaWorktreeManager,
)
from agent_runtime.adapters.storage_sqlite import SQLiteDatabase
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon, build_daemon_from_config
from agent_runtime.core.models import Emit
from tests.support.fakes import RecordingChannel


class TestExecutorSessionBindingAndIngestOnly(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="exec_bind_")
        self.root = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    async def test_multi_executor_sticky_routing_grok_resume_and_explicit_switch_to_claude(
        self,
    ) -> None:
        repo_checkout = self.root / "repo"
        repo_checkout.mkdir()
        wt_dir = self.root / "hb-issue-108"
        wt_dir.mkdir()
        invocations_log = self.root / "sticky_invocations.jsonl"

        fake_cli = self.root / "fake_sticky_cli.py"
        fake_cli.write_text(
            "\n".join(
                [
                    "import json, sys",
                    "log_path, kind, *rest = sys.argv[1:]",
                    "with open(log_path, 'a', encoding='utf-8') as f:",
                    "    f.write(json.dumps({'kind': kind, 'args': rest}) + '\\n')",
                    "resumed = '--resume' in rest",
                    "if kind == 'grok':",
                    "    print(json.dumps({",
                    "        'result': 'grok-resumed' if resumed else 'grok-fresh',",
                    "        'executor_session_id': 'grok-native-108',",
                    "    }))",
                    "else:",
                    "    print(json.dumps({",
                    "        'result': 'claude-resumed' if resumed else 'claude-fresh',",
                    "        'session_id': 'claude-native-208',",
                    "    }))",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        cfg = parse_config(
            {
                "runtime": {
                    "db_path": str(self.root / "sticky.db"),
                    "memory_root": str(self.root / "mem_sticky"),
                    "default_executor": "claude",
                    "max_concurrency": 1,
                },
                "executors": {
                    "claude": {
                        "command": sys.executable,
                        "args": [str(fake_cli), str(invocations_log), "claude", "-p", "{prompt}"],
                        "resume_args": [
                            str(fake_cli),
                            str(invocations_log),
                            "claude",
                            "-p",
                            "{prompt}",
                            "--resume",
                            "{executor_session_id}",
                        ],
                    },
                    "grok": {
                        "command": sys.executable,
                        "args": [str(fake_cli), str(invocations_log), "grok", "-p", "{prompt}"],
                        "resume_args": [
                            str(fake_cli),
                            str(invocations_log),
                            "grok",
                            "-p",
                            "{prompt}",
                            "--resume",
                            "{executor_session_id}",
                        ],
                    },
                },
                "projects": {
                    "hibernaut": {
                        "inbox_id": "proj:hibernaut",
                        "repo": "kun-g/hibernaut",
                        "checkout": str(repo_checkout),
                        "worktree_backend": "git",
                        "executor": "claude",
                    }
                },
            }
        )

        channel = RecordingChannel()
        daemon = build_daemon_from_config(cfg, channel=channel)
        self.assertFalse(daemon.ingest_only)
        # Pre-register ready worktree so git worktree creation is skipped.
        daemon.workspaces.reserve_creating(
            work_key="github:kun-g/hibernaut#108",
            repo_checkout=str(repo_checkout),
            workdir=str(wt_dir),
            branch="hb/issue-108",
        )
        daemon.workspaces.mark_ready(
            "github:kun-g/hibernaut#108",
            workdir=str(wt_dir),
            branch="hb/issue-108",
        )

        stop = asyncio.Event()
        daemon_task = asyncio.create_task(
            daemon.run(stop_event=stop, timer_interval_seconds=0.1, outbox_interval_seconds=0.05)
        )
        try:
            # 1. Event 1 explicitly selects "grok" (overriding project/global default "claude").
            daemon.handler.ingest(
                inbox_id="proj:hibernaut",
                source="github_webhook",
                external_id="sticky-evt-1",
                correlation_key="github:kun-g/hibernaut#108",
                payload={"text": "start with grok", "executor_id": "grok", "explicit_workdir": str(wt_dir)},
            )
            await daemon.pool.wait_until_idle(timeout_seconds=3.0)

            calls = [json.loads(line) for line in invocations_log.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]["kind"], "grok")
            self.assertNotIn("--resume", calls[0]["args"])

            sess = daemon.sessions.find_by_correlation_key(
                "proj:hibernaut",
                "github:kun-g/hibernaut#108",
            )
            self.assertIsNotNone(sess)
            assert sess is not None
            self.assertEqual(sess.executor_id, "grok")
            self.assertEqual(sess.executor_session_id, "grok-native-108")

            # 2. Event 2 omits executor_id -> MUST stay sticky on "grok" and use resume_args!
            daemon.handler.ingest(
                inbox_id="proj:hibernaut",
                source="github_webhook",
                external_id="sticky-evt-2",
                correlation_key="github:kun-g/hibernaut#108",
                payload={"text": "follow-up without executor_id", "explicit_workdir": str(wt_dir)},
            )
            await daemon.pool.wait_until_idle(timeout_seconds=3.0)

            calls = [json.loads(line) for line in invocations_log.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(calls), 2)
            self.assertEqual(calls[1]["kind"], "grok")
            self.assertIn("--resume", calls[1]["args"])
            self.assertIn("grok-native-108", calls[1]["args"])
            self.assertIsInstance(daemon.pool.reports[-1].outcome, Emit)
            assert isinstance(daemon.pool.reports[-1].outcome, Emit)
            self.assertEqual(daemon.pool.reports[-1].outcome.messages[0], "grok-resumed")

            # 3. Event 3 explicitly switches to "claude" -> fresh args, never leaks grok-native-108!
            daemon.handler.ingest(
                inbox_id="proj:hibernaut",
                source="github_webhook",
                external_id="sticky-evt-3",
                correlation_key="github:kun-g/hibernaut#108",
                payload={"text": "switch to claude", "executor_id": "claude", "explicit_workdir": str(wt_dir)},
            )
            await daemon.pool.wait_until_idle(timeout_seconds=3.0)

            calls = [json.loads(line) for line in invocations_log.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(calls), 3)
            self.assertEqual(calls[2]["kind"], "claude")
            self.assertNotIn("--resume", calls[2]["args"])
            self.assertNotIn("grok-native-108", calls[2]["args"])

            switched = daemon.sessions.find_by_correlation_key(
                "proj:hibernaut",
                "github:kun-g/hibernaut#108",
            )
            self.assertIsNotNone(switched)
            assert switched is not None
            self.assertEqual(switched.executor_id, "claude")
            self.assertEqual(switched.executor_session_id, "claude-native-208")
        finally:
            stop.set()
            await daemon_task
            daemon.close()

    async def test_cli_executor_persists_native_session_and_resumes_only_when_executor_matches(
        self,
    ) -> None:
        repo_checkout = self.root / "repo"
        repo_checkout.mkdir()
        wt_dir = self.root / "hb-issue-88"
        wt_dir.mkdir()
        invocations_log = self.root / "invocations.jsonl"

        fake_cli = self.root / "fake_cli.py"
        fake_cli.write_text(
            "\n".join(
                [
                    "import json, sys",
                    "log_path, kind, *rest = sys.argv[1:]",
                    "with open(log_path, 'a', encoding='utf-8') as f:",
                    "    f.write(json.dumps({'kind': kind, 'args': rest}) + '\\n')",
                    "if kind == 'claude':",
                    "    resumed = '--resume' in rest",
                    "    print(json.dumps({",
                    "        'type': 'result',",
                    "        'result': 'resumed-claude-ok' if resumed else 'fresh-claude-ok',",
                    "        'session_id': 'claude-native-88',",
                    "    }))",
                    "else:",
                    "    print(json.dumps({",
                    "        'result': 'fresh-cursor-ok',",
                    "        'executor_session_id': 'cursor-native-99',",
                    "    }))",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        templates = {
            "claude": CLITemplate(
                name="claude",
                command=sys.executable,
                args=(str(fake_cli), str(invocations_log), "claude", "-p", "{prompt}", "--output-format", "json"),
                resume_args=(
                    str(fake_cli),
                    str(invocations_log),
                    "claude",
                    "-p",
                    "{prompt}",
                    "--resume",
                    "{executor_session_id}",
                    "--output-format",
                    "json",
                ),
            ),
            "cursor-agent": CLITemplate(
                name="cursor-agent",
                command=sys.executable,
                args=(str(fake_cli), str(invocations_log), "cursor-agent", "-p", "{prompt}"),
                resume_args=(
                    str(fake_cli),
                    str(invocations_log),
                    "cursor-agent",
                    "-p",
                    "{prompt}",
                    "--resume",
                    "{executor_session_id}",
                ),
            ),
        }

        db = SQLiteDatabase(self.root / "bind.db")
        channel = RecordingChannel()

        def _make_execs(_inbox_id: str) -> dict[str, LocalCLIExecutor]:
            mgr = OrcaWorktreeManager(
                workspace_store=daemon.workspaces,
                backend="git",
                runner=lambda cmd, _cwd: CommandResult(0, "", ""),
            )
            return {
                name: LocalCLIExecutor(
                    worktree_manager=mgr,
                    inbox_checkout_map={"proj:hibernaut": str(repo_checkout)},
                    templates=templates,
                    default_executor=name,
                )
                for name in templates
            }

        daemon = build_daemon(
            database=db,
            memory_root=self.root / "mem",
            max_concurrency=1,
            executors_factory=_make_execs,
            global_default_executor="claude",
            channel=channel,
            mcp_bearer_token="tok-obs",
        )
        daemon.workspaces.reserve_creating(
            work_key="github:kun-g/hibernaut#88",
            repo_checkout=str(repo_checkout),
            workdir=str(wt_dir),
            branch="hb/issue-88",
        )
        daemon.workspaces.mark_ready(
            "github:kun-g/hibernaut#88",
            workdir=str(wt_dir),
            branch="hb/issue-88",
        )

        stop = asyncio.Event()
        daemon_task = asyncio.create_task(
            daemon.run(stop_event=stop, timer_interval_seconds=0.1, outbox_interval_seconds=0.05)
        )
        try:
            daemon.handler.ingest(
                inbox_id="proj:hibernaut",
                source="github_poll",
                external_id="evt-1",
                correlation_key="github:kun-g/hibernaut#88",
                payload={"title": "first run", "explicit_workdir": str(wt_dir)},
            )
            await daemon.pool.wait_until_idle(timeout_seconds=3.0)

            calls = [json.loads(line) for line in invocations_log.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]["kind"], "claude")
            self.assertNotIn("--resume", calls[0]["args"])

            sess = daemon.sessions.find_by_correlation_key(
                "proj:hibernaut",
                "github:kun-g/hibernaut#88",
            )
            self.assertIsNotNone(sess)
            assert sess is not None
            self.assertEqual(sess.executor_id, "claude")
            self.assertEqual(sess.executor_session_id, "claude-native-88")

            daemon.handler.ingest(
                inbox_id="proj:hibernaut",
                source="github_webhook",
                external_id="evt-2",
                correlation_key="github:kun-g/hibernaut#88",
                payload={"comment": "follow up", "explicit_workdir": str(wt_dir)},
            )
            await daemon.pool.wait_until_idle(timeout_seconds=3.0)

            calls = [json.loads(line) for line in invocations_log.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(calls), 2)
            self.assertIn("--resume", calls[1]["args"])
            self.assertIn("claude-native-88", calls[1]["args"])

            daemon.handler.ingest(
                inbox_id="proj:hibernaut",
                source="github_webhook",
                external_id="evt-3",
                correlation_key="github:kun-g/hibernaut#88",
                payload={"comment": "switch executor", "executor_id": "cursor-agent", "explicit_workdir": str(wt_dir)},
            )
            await daemon.pool.wait_until_idle(timeout_seconds=3.0)

            calls = [json.loads(line) for line in invocations_log.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(calls), 3)
            self.assertEqual(calls[2]["kind"], "cursor-agent")
            self.assertNotIn("--resume", calls[2]["args"])
            self.assertNotIn("claude-native-88", calls[2]["args"])

            switched = daemon.sessions.find_by_correlation_key(
                "proj:hibernaut",
                "github:kun-g/hibernaut#88",
            )
            self.assertIsNotNone(switched)
            assert switched is not None
            self.assertEqual(switched.executor_id, "cursor-agent")
            self.assertEqual(switched.executor_session_id, "cursor-native-99")

            # Verify MCP hibernaut_list_sessions & hibernaut_list_events expose the new fields.
            assert daemon.mcp_server is not None
            sess_reply = daemon.mcp_server.handle_rpc(
                {
                    "jsonrpc": "2.0",
                    "id": 10,
                    "method": "tools/call",
                    "params": {
                        "name": "hibernaut_list_sessions",
                        "arguments": {"inbox_id": "proj:hibernaut"},
                    },
                }
            )
            assert sess_reply is not None
            mcp_sessions = sess_reply["result"]["structuredContent"]["sessions"]
            self.assertEqual(len(mcp_sessions), 1)
            self.assertEqual(mcp_sessions[0]["executor_id"], "cursor-agent")
            self.assertEqual(mcp_sessions[0]["executor_session_id"], "cursor-native-99")

            evt_reply = daemon.mcp_server.handle_rpc(
                {
                    "jsonrpc": "2.0",
                    "id": 11,
                    "method": "tools/call",
                    "params": {
                        "name": "hibernaut_list_events",
                        "arguments": {"inbox_id": "proj:hibernaut", "status": "done", "limit": 10},
                    },
                }
            )
            assert evt_reply is not None
            mcp_events = evt_reply["result"]["structuredContent"]["events"]
            self.assertEqual(len(mcp_events), 3)
            self.assertEqual(mcp_events[0]["external_id"], "evt-3")
            self.assertEqual(mcp_events[0]["status"], "done")
            self.assertEqual(mcp_events[0]["correlation_key"], "github:kun-g/hibernaut#88")
        finally:
            stop.set()
            await daemon_task
            daemon.close()
