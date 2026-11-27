import socket
import subprocess
import sys

import pytest


def check(path):
    return subprocess.run([sys.executable, '-m', 'agent_runtime.app.main', '--config', str(path), '--check'],
                          cwd=path.parent, capture_output=True, text=True, timeout=10)


def test_checks_without_starting_services(tmp_path):
    path = tmp_path / 'candidate.toml'
    with socket.socket() as busy:
        busy.bind(('127.0.0.1', 0))
        busy.listen()
        path.write_text(f'''[runtime]
 db_path = "state.sqlite"
 memory_root = "memory"
[channels]
 port = {busy.getsockname()[1]}
[executors.script]
 command = "no-such-executable"
[[runtime.inbox_bindings]]
 pattern = "sys:timer"
 default_agent = "script"
 workdir = "/test/script"
[projects.sample]
 repo = "org/sample"
 checkout = "/test/sample"
 inbox_id = "proj:sample"
[[projects.sample.inbox_bindings]]
 pattern = "proj:sample"
 default_agent = "script"
[[timers]]
 name = "tick"
 inbox_id = "sys:timer"
 interval_seconds = 60
''')
        result = check(path)
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == ['OK projects=1 bindings=2 timers=1']
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize(('text', 'field'), [
    ('[runtime]\nmax_concurency = 3', 'max_concurency'),
    ('[channels]\nport = "wrong"', 'channels.port'),
    ('[runtime]\ndefault_agent = "missing"', 'runtime.default_agent'),
    ('[[timers]]\nname = "tick"\ninbox_id = "missing"\ninterval_seconds = 60', 'timers[0].inbox_id'),
    ('[projects.demo]', 'projects.demo.repo'),
    ('[projects.demo]\nrepo = "org/demo"\ncheckout = "/demo"\nexector = "no"', 'exector'),
    ('[executors.worker]\ncommand = "worker"\nenv = []', 'executors.worker.env'),
    ('[[timers]]\ninterval_sec = 3', 'interval_sec'),
    ('[runtime]\ndb_path = []', 'runtime.db_path'),
    ('', 'empty configuration'),
])
def test_invalid_configuration(tmp_path, text, field):
    path = tmp_path / 'bad.toml'
    path.write_text(text)
    result = check(path)
    assert result.returncode == 1, result.stderr
    assert str(path) in result.stderr and field in result.stderr
    assert 'Traceback' not in result.stderr
    assert not result.stdout
