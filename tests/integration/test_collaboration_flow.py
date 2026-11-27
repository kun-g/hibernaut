from __future__ import annotations

import asyncio
import tomllib
from pathlib import Path
from typing import Any

import pytest

from agent_runtime.app.config import parse_config
from agent_runtime.app.main import AlwaysOnDaemon, build_daemon_from_config, serve_configured
from tests.support.collaboration_flow import CARD, call, chief_events, hub_call, refused
from tests.support.collaboration_flow import tokens  # noqa: F401 -- pytest fixture, requested by parameter name


def write_config(path: Path, tmp_path: Path, *, experts: str, budget: int = 6500) -> None:
    for name in ("hibernaut", "home"):
        (tmp_path / name).mkdir(exist_ok=True)
    path.write_text(f"""
[runtime]
db_path = "{tmp_path}/hb.db"
memory_root = "{tmp_path}/memory"
default_agent = "cli"

[[runtime.inbox_bindings]]
pattern = "user:main"
default_agent = "cli"
agents = ["cli"]
workdir = "{tmp_path}/home"

[channels]
port = 0
mcp_enabled = true

[executors.cli]
command = "true"

[agents."pool:architect"]
executor = "cli"

[projects.hibernaut]
repo = "o/hibernaut"
checkout = "{tmp_path}/hibernaut"
worktree_backend = "git"
inbox_id = ["proj:hibernaut", "proj:hibernaut:.+"]
default_agent = "cli"
experts = {experts}

[collaboration]
enabled = true
expert_budget_chars = {budget}
hub = {{ enabled = true, listen_port = 0, base_path = "/hub" }}

[experts.architect]
""")


def test_a_reload_keeps_the_running_hub_and_drafts_anew(
    tmp_path: Path, tokens: None, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    from agent_runtime.app import main as app_main

    path = tmp_path / "hibernaut.toml"
    write_config(path, tmp_path, experts="[]")
    config = parse_config(tomllib.loads(path.read_text()), base_dir=tmp_path)
    first = build_daemon_from_config(config)
    built: list[AlwaysOnDaemon] = []
    original = app_main.build_daemon_from_config

    def recording(*args: Any, **kwargs: Any) -> AlwaysOnDaemon:
        built.append(original(*args, **kwargs))
        return built[-1]

    monkeypatch.setattr(app_main, "build_daemon_from_config", recording)

    async def scenario() -> None:
        stop = asyncio.Event()
        serving = asyncio.create_task(serve_configured(path, config, first, stop, reload_interval=0.02))
        try:
            assert "not drafted" in refused(first, "resolve_expert_mount", role_id="architect", project_key="hibernaut")
            write_config(path, tmp_path, experts='["architect"]')
            for _ in range(500):
                if built:
                    break
                await asyncio.sleep(0.02)
            assert built[0].collaboration is first.collaboration
            assert call(built[0], "resolve_expert_mount", role_id="architect", project_key="hibernaut")["agent_id"]
            # The hub opened by the first generation goes on.
            assert (await asyncio.to_thread(hub_call, first, "digest"))[0] == 200
            card = call(built[0], "emit_closeout_card", **CARD)
            confirmed = await asyncio.to_thread(hub_call, first, f"closeouts/{card['card_id']}/confirm", post=True)
            assert confirmed[0] == 200 and len(chief_events(built[0])) == 1
            capsys.readouterr()
            write_config(path, tmp_path, experts='["architect"]', budget=2000)
            for _ in range(500):
                if "require a restart" in capsys.readouterr().err:
                    break
                await asyncio.sleep(0.02)
            else:
                raise AssertionError("the changed [collaboration] was not refused")
            assert len(built) == 1
        finally:
            stop.set()
            await serving

    try:
        asyncio.run(scenario())
    finally:
        first.close()
    with pytest.raises(OSError):
        hub_call(first, "digest")
