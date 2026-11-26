import http.client
import json
import socket
import time
from pathlib import Path


BLOCKED = "proj:blocked"
OTHER = "proj:other"


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def post(port: int, inbox_id: str, text: str, workdir: Path, external_id: str | None = None):
    body = {"inbox_id": inbox_id, "external_id": external_id or f"{text}-{time.monotonic_ns()}",
            "payload": {"text": text, "explicit_workdir": str(workdir)}}
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
    try:
        conn.request("POST", "/events", json.dumps(body), {"Content-Type": "application/json"})
        response = conn.getresponse()
        return response.status, json.load(response)
    except OSError:
        return None, None
    finally:
        conn.close()
