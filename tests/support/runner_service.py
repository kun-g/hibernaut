from __future__ import annotations

import dataclasses
from typing import Any

from runner.protocol import RunRequest
from runner.protocol_check import PROTOCOL_CHECK_EXECUTOR_ID, CheckRequest


def _run_request(head: str, **changes: Any) -> RunRequest:
    req = RunRequest(
        run_id="run_check", namespace="hb", project_key="hibernaut", owner_scope="hibernaut:pilot", work_key="w",
        executor_id=PROTOCOL_CHECK_EXECUTOR_ID, readonly=True, prompt=CheckRequest(head).to_json(), timeout_seconds=30.0,
    )
    return dataclasses.replace(req, **changes)
