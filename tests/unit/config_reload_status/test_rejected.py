import asyncio
import hashlib
import json

import pytest

from agent_runtime.app.config import config_changes
from agent_runtime.app.reload import ReloadFailed, ReloadStatus
from agent_runtime.app.system_tools import RuntimeStatus


class EmptyReader:
    def report(self, *, hours, timer_runs):
        return {}


def test_status_identifies_rejected_and_active_versions_until_recovery(tmp_path):
    path = tmp_path / 'hibernaut.toml'
    active = b'[runtime]\nmax_concurrency = 3'
    bad = b'[channels]\nport = "wrong"'
    path.write_bytes(active)
    reloads = ReloadStatus()
    reloads.watch(path)
    status = RuntimeStatus(EmptyReader(), reloads)

    async def exercise():
        stop = asyncio.Event()
        changes = config_changes(path, stop, interval=0.001, reloads=reloads)
        await anext(changes)
        reloads.settle(reloads.latest)
        path.write_bytes(bad)
        next_valid = asyncio.create_task(anext(changes))
        try:
            with pytest.raises(ReloadFailed):
                await asyncio.to_thread(reloads.wait, bad, 2)
            record = json.loads(status.status(hours=1, timer_runs=1))['config_reload']
            assert record['path'] == str(path.resolve())
            assert record['active_sha256'] == hashlib.sha256(active).hexdigest()
            assert record['rejected_sha256'] == hashlib.sha256(bad).hexdigest()
            assert 'channels.port' in record['error']
            path.write_bytes(active)
            await asyncio.wait_for(next_valid, timeout=2)
            reloads.settle(reloads.latest)
            assert 'config_reload' not in json.loads(status.status(hours=1, timer_runs=1))
        finally:
            stop.set()
            await asyncio.gather(next_valid, return_exceptions=True)
            await changes.aclose()

    asyncio.run(exercise())
