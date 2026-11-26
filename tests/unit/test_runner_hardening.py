from __future__ import annotations
import asyncio
from pathlib import Path
import pytest
from runner.supervisor import ProcessSupervisor
from tests.support.runner_safety import PY
from datetime import datetime, timedelta, timezone  # noqa: E402


def test_no_process_is_left_when_the_run_log_cannot_be_opened(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    logs = tmp_path / "logs"
    now = datetime.now(timezone.utc)
    for day in (now, now + timedelta(days=1)):
        (logs / day.strftime("%Y-%m-%d") / "run_nolog.log").mkdir(parents=True)  # opening it for writing fails
    supervisor = ProcessSupervisor(runner_id="r", logs_dir=logs)

    async def unexpected_spawn(*args, **kwargs):
        pytest.fail("the supervisor started a child before it could open the run log")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", unexpected_spawn)

    async def scenario() -> None:
        with pytest.raises(OSError):
            await supervisor.execute(
                run_id="run_nolog",
                argv=[PY, "-c", "pass"],
                cwd=tmp_path,
                env={"PATH": "/usr/bin:/bin"},
                stdin_text=None,
                timeout_seconds=60.0,
                idle_timeout_seconds=None,
                cancel_event=asyncio.Event(),
                grace_seconds=0.5,
            )

    asyncio.run(scenario())
