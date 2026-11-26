import sys
from multiprocessing.process import BaseProcess
from pathlib import Path

import pytest


_PROCESS_EVENTS = {
    "subprocess.Popen", "os.exec", "os.fork", "os.forkpty", "os.posix_spawn",
    "os.spawn", "os.system", "os.startfile", "_posixsubprocess.fork_exec",
    "_winapi.CreateProcess", "multiprocessing.Process.start",
}
_violations: list[str] | None = None


def _block_process(event, args):
    if _violations is not None and event in _PROCESS_EVENTS:
        message = f"Unit test attempted {event}; move this test to tests/integration/"
        _violations.append(message)
        pytest.fail(message, pytrace=False)


sys.addaudithook(_block_process)


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_protocol(item, nextitem):
    # One worker runs one item at a time. Include fixture lifetimes and calls from worker threads.
    global _violations
    if not item.path.is_relative_to(Path(__file__).parent):
        return (yield)
    _violations = []
    try:
        with pytest.MonkeyPatch.context() as patch:
            # A running multiprocessing forkserver can create children on request without a local fork.
            patch.setattr(BaseProcess, "start", lambda self: _block_process("multiprocessing.Process.start", ()))
            if sys.platform != "win32" and sys.version_info < (3, 14):
                # Older CPython does not audit this primitive (also used to start the resource tracker).
                import _posixsubprocess
                import subprocess

                def forbid_fork_exec(*args, **kwargs):
                    _block_process("_posixsubprocess.fork_exec", args)

                patch.setattr(_posixsubprocess, "fork_exec", forbid_fork_exec)
                # subprocess binds its own reference when it imports the native function.
                patch.setattr(subprocess, "_fork_exec", forbid_fork_exec)
            return (yield)
    finally:
        _violations = None


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item, call):
    report = yield
    if _violations:
        if not report.failed:
            # The caller may have caught the exception, or launched from a background thread.
            report.outcome = "failed"
            report.longrepr = _violations[0]
        _violations.clear()
    return report
