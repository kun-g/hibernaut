import http.client
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


class TestConfigReload(unittest.TestCase):
    def test_daemon_keeps_serving_and_applies_corrected_file(self):
        repo = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / 'hibernaut.toml'
            with socket.socket() as sock:
                sock.bind(('127.0.0.1', 0))
                port = sock.getsockname()[1]
            base = f'[runtime]\ndb_path = ":memory:"\n[channels]\nport = {port}\n'
            enabled = base + 'mcp_enabled = true\n'
            path.write_text(enabled)
            log = root / 'daemon.log'
            with log.open('w') as stream:
                proc = subprocess.Popen(
                    [
                        sys.executable,
                        str(repo / 'scripts/run_updating.py'),
                        '--config', str(path),
                        '--interval', '3600',
                        '--config-poll-interval', '0.02',
                    ],
                    cwd=root, stdout=stream, stderr=stream,
                    env={**os.environ, 'HIBERNAUT_MCP_TOKEN': 'reload-test'},
                )
            headers = {'Authorization': 'Bearer reload-test', 'Content-Type': 'application/json'}

            def post(route, body):
                conn = http.client.HTTPConnection('127.0.0.1', port, timeout=2)
                try:
                    conn.request('POST', route, json.dumps(body), headers)
                    response = conn.getresponse()
                    return response.status, json.load(response)
                finally:
                    conn.close()

            def request():
                return post('/mcp', {'jsonrpc':'2.0','id':1,'method':'ping'})[0]

            event = {'inbox_id':'reload','external_id':'stable','payload':{'text':'hello'}}

            def wait_for(check):
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    self.assertIsNone(proc.poll(), log.read_text())
                    try:
                        if check():
                            return
                    except ConnectionRefusedError:
                        pass
                    time.sleep(.01)
                self.fail(log.read_text())

            try:
                wait_for(lambda: request() == 200)
                self.assertFalse(post('/events', event)[1]['duplicate'])
                path.write_text('[broken')
                wait_for(lambda: '[config] Invalid' in log.read_text())
                self.assertEqual(request(), 200)
                # Editors often save by replacing the file rather than modifying its inode.
                draft = root / 'draft.toml'
                draft.write_text(base + 'mcp_enabled = false\n')
                draft.replace(path)
                wait_for(lambda: request() == 404)
                path.write_text(enabled)
                wait_for(lambda: request() == 200)
                self.assertTrue(post('/events', event)[1]['duplicate'])
                path.write_text(enabled + f'\n[projects.local]\nrepo="fixture/test"\ncheckout="{root}"\n'
                                'inbox_id="proj:local"\nfeishu_chat_id="oc_local"\n')
                wait_for(lambda: 'projects.local.feishu_chat_id is obsolete and ignored' in log.read_text())
                self.assertEqual(post('/webhooks/feishu', {})[0], 404)
                self.assertEqual(request(), 200)
                with socket.socket() as occupied:
                    occupied.bind(('127.0.0.1', 0))
                    occupied.listen()
                    path.write_text(enabled.replace(f'port = {port}', f'port = {occupied.getsockname()[1]}'))
                    wait_for(lambda: 'Cannot activate settings' in log.read_text())
                    self.assertEqual(request(), 200)
            finally:
                proc.terminate()
                proc.wait(timeout=10)
