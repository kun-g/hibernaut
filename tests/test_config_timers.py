import asyncio
import socket
import sys

import pytest

from agent_runtime.app.config import load_config, parse_config
from agent_runtime.app.main import build_daemon_from_config, serve_configured
from agent_runtime.app.reload import ReloadFailed


PROJECT = {"repo": "fixture/p", "checkout": "/tmp/p", "inbox_id": "proj:p"}
TIMER = {"name": "health", "inbox_id": "proj:p", "interval_seconds": 60}


@pytest.mark.parametrize("declarations", [
    {}, [{}], [TIMER, TIMER], [dict(TIMER, name=" ")], [dict(TIMER, inbox_id="unknown")],
    [dict(TIMER, interval_seconds=True)], [dict(TIMER, interval_seconds=0)],
    [dict(TIMER, interval_seconds=0.5)], [dict(TIMER, payload=[])], [dict(TIMER, enabled="false")],
    [dict(TIMER, payload={"date": object()})], [dict(TIMER, cron="*")],
])
def test_invalid_declarations_fail_at_load(declarations):
    with pytest.raises(ValueError, match="timers"):
        parse_config({"projects": {"p": PROJECT}, "timers": declarations})


def test_system_tools_target_is_refused_even_when_disabled(tmp_path):
    with pytest.raises(ValueError, match="timers.*system_tools"):
        parse_config({"executors": {"ops": {"role": "steward"}},
                      "runtime": {"inbox_bindings": [{"pattern": "sys:ops", "workdir": str(tmp_path),
                          "checkout_access": "readonly", "system_tools": True, "default_agent": "ops"}]},
                      "timers": [dict(TIMER, inbox_id="sys:ops", enabled=False)]})


FUTURE = "2099-01-01T00:00:00+00:00"


def base_config(root):
    return f'''[runtime]
db_path = "{root}/runtime.db"
memory_root = "{root}/memory"
default_agent = "other"
[channels]
port = 0
[executors.other]
command = "false"
[executors.script]
command = "{sys.executable}"
args = ["{root}/health.py"]
[[runtime.inbox_bindings]]
pattern = "sys:health"
workdir = "{root}"
checkout_access = "readonly"
default_agent = "other"
rules = [{{ source = "timer:health", agent = "script" }}]
'''


DECL = '''[[timers]]
name = "health"
inbox_id = "sys:health"
interval_seconds = 600
payload = {executor = "other", executor_id = "other", agent_id = "other", command = "false"}
'''


def test_restart_keeps_due_date_and_reload_reconciles_only_owned_timers(tmp_path):
    async def exercise():
        path = tmp_path / "hibernaut.toml"
        base = base_config(tmp_path)
        path.write_text(base + DECL)
        config = load_config(path)
        daemon = build_daemon_from_config(config)
        try:
            manual = daemon.timers.schedule("follow_up", FUTURE, {"timer_name": "health"})
            first, = [t for t in daemon.timers.fetch_due(FUTURE) if t.timer_id != manual]
            assert daemon.scheduler.tick(first.due_at) == 1
            original, = [t for t in daemon.timers.fetch_due(FUTURE) if t.timer_id != manual]
        finally:
            daemon.close()
        daemon = build_daemon_from_config(config)
        stop = asyncio.Event()
        server = asyncio.create_task(serve_configured(path, config, daemon, stop, reload_interval=.01))
        try:
            await asyncio.sleep(0)
            await asyncio.to_thread(daemon.reloads.wait, path.read_bytes(), 10)

            def owned():
                rows = daemon.timers.fetch_due(FUTURE)
                assert any(t.timer_id == manual for t in rows)
                return [t for t in rows if t.timer_id != manual]

            async def apply(decl):
                contents = (base + decl).encode()
                path.write_bytes(contents)
                await asyncio.to_thread(daemon.reloads.wait, contents, 10)

            assert owned() == [original] and original.due_at > first.due_at
            with socket.socket() as occupied:
                occupied.bind(("127.0.0.1", 0))
                occupied.listen()
                rejected = base.replace("port = 0", f"port = {occupied.getsockname()[1]}").encode()
                path.write_bytes(rejected)
                with pytest.raises(ReloadFailed, match="Cannot activate"):
                    await asyncio.to_thread(daemon.reloads.wait, rejected, 10)
                assert owned() == [original]
            await apply(DECL.replace("600", "1200"))
            changed, = owned()
            assert changed.timer_id == original.timer_id
            assert changed.recurrence_seconds == 1200 and changed.due_at != original.due_at
            await apply(DECL + "enabled = false\n")
            assert owned() == []
            await apply(DECL)
            assert len(owned()) == 1
            await apply("")
            assert owned() == []
        finally:
            stop.set()
            await server
            daemon.close()

    asyncio.run(exercise())


def test_ticks_run_bound_script_once_per_idle_interval_in_one_session(tmp_path):
    async def exercise():
        path = tmp_path / "hibernaut.toml"
        path.write_text(base_config(tmp_path) + DECL)
        (tmp_path / "health.py").write_text('''from pathlib import Path
import sys, time
mode = Path("mode").read_text()
if mode == "silent":
    Path("started").touch()
    while not Path("release").exists():
        time.sleep(.01)
elif mode == "emit":
    print("healthy")
else:
    sys.exit(7)
''')
        daemon = build_daemon_from_config(load_config(path))
        stop = asyncio.Event()
        pool = asyncio.create_task(daemon.pool.run_forever(stop))
        try:
            def tick():
                timer, = daemon.timers.fetch_due(FUTURE)
                return daemon.scheduler.tick(timer.due_at)

            (tmp_path / "mode").write_text("silent")
            assert tick() == 1
            event, = daemon.inbox.fetch_pending("sys:health")
            assert event.source == "timer:health"
            assert event.payload["data"]["executor"] == "other"
            assert tick() == 0  # still pending
            async with asyncio.timeout(10):
                while not (tmp_path / "started").exists():
                    await asyncio.sleep(.01)
            assert tick() == 0  # CLI is running
            assert len(daemon.inbox.list_events()) == 1
            (tmp_path / "release").touch()
            await daemon.pool.wait_until_idle()
            assert daemon.outbox.fetch_pending() == []
            for mode in ("emit", "fail"):
                (tmp_path / "mode").write_text(mode)
                assert tick() == 1
                await daemon.pool.wait_until_idle()
            assert [m.content for m in daemon.outbox.fetch_pending()][0] == "healthy"
            runs = daemon.ledger.store.list_runs(inbox_id="sys:health")
            assert [(r.outcome_kind, r.exit_code) for r in runs] == [("emit", 7), ("emit", 0), ("silent", 0)]
            assert runs[0].emit_status == "failed"
            assert {r.executor_id for r in runs} == {"script"}
            assert len(daemon.sessions.list_sessions(inbox_id="sys:health")) == 1
        finally:
            (tmp_path / "release").touch()
            stop.set()
            await pool
            daemon.close()

    asyncio.run(exercise())
