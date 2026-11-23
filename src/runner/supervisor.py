from __future__ import annotations

import asyncio
import hashlib
import os
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Final, Mapping, Sequence

from runner.protocol import CredentialLease, LogRef, MAX_STDOUT_EXCERPT_BYTES

STRIPPED_SECRET_ENV_KEYS: Final = frozenset(
    {
        "GH_TOKEN",
        "GITHUB_TOKEN",
        "GH_ENTERPRISE_TOKEN",
        "GITHUB_ENTERPRISE_TOKEN",
        "ANTHROPIC_API_KEY",
        "CLAUDE_CODE_OAUTH_TOKEN",
        "OPENAI_API_KEY",
        "XAI_API_KEY",
        "FEISHU_APP_SECRET",
        "SWITCHBOARD_RELAY_TOKEN",
        "SWITCHBOARD_MCP_TOKEN",
    }
)


def strip_long_lived_secrets(
    base_env: Mapping[str, str],
    *,
    extra_env: Mapping[str, str] | None = None,
    credential_lease: CredentialLease | None = None,
) -> dict[str, str]:
    env = {k: v for k, v in base_env.items() if k not in STRIPPED_SECRET_ENV_KEYS}
    env["GIT_TERMINAL_PROMPT"] = "0"
    if extra_env:
        for k, v in extra_env.items():
            if k in STRIPPED_SECRET_ENV_KEYS:
                raise PermissionError(f"refusing to inject long-lived secret key {k!r} into child env")
            env[k] = v
    if credential_lease is not None:
        env["RUNNER_BROKER_URL"] = credential_lease.broker_url
        env["RUNNER_LEASE_TOKEN"] = credential_lease.lease_token
        env["RUNNER_LEASE_EXPIRES_AT"] = credential_lease.expires_at
    return env


@dataclass(frozen=True, slots=True)
class SupervisedRunOutput:
    status: str  # "succeeded" | "failed" | "interrupted" | "timed_out"
    exit_code: int | None
    stdout_excerpt: str
    stderr_excerpt: str
    log_ref: LogRef
    timeout_kind: str | None = None  # "hard" | "idle" | None


async def terminate_process_group(proc: asyncio.subprocess.Process, *, grace_seconds: float = 2.0) -> None:
    if proc.returncode is not None:
        return
    try:
        pgid = os.getpgid(proc.pid)
        os.killpg(pgid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError, OSError):
        try:
            proc.terminate()
        except ProcessLookupError:
            return
    try:
        await asyncio.wait_for(proc.wait(), timeout=grace_seconds)
    except asyncio.TimeoutError:
        try:
            pgid = os.getpgid(proc.pid)
            os.killpg(pgid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError, OSError):
            try:
                proc.kill()
            except ProcessLookupError:
                return
        await proc.wait()


class ProcessSupervisor:
    def __init__(self, *, runner_id: str, logs_dir: Path) -> None:
        self._runner_id = runner_id
        self._logs_dir = logs_dir

    async def execute(
        self,
        *,
        run_id: str,
        argv: Sequence[str],
        cwd: Path,
        env: Mapping[str, str],
        stdin_text: str | None,
        timeout_seconds: float,
        idle_timeout_seconds: float | None,
        cancel_event: asyncio.Event,
        grace_seconds: float = 2.0,
        on_stdout_line: Callable[[str], None] | None = None,
    ) -> SupervisedRunOutput:
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        log_dir = self._logs_dir / day
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"{run_id}.log"

        proc = await asyncio.create_subprocess_exec(
            *argv,
            cwd=str(cwd),
            env=dict(env),
            stdin=asyncio.subprocess.PIPE if stdin_text is not None else asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )

        if stdin_text is not None and proc.stdin is not None:
            proc.stdin.write(stdin_text.encode("utf-8"))
            await proc.stdin.drain()
            proc.stdin.close()

        hasher = hashlib.sha256()
        total_bytes = 0
        stdout_chunks: list[str] = []
        stderr_chunks: list[str] = []
        stdout_len = 0
        stderr_len = 0
        last_activity = time.monotonic()

        log_fp = log_path.open("wb")

        async def _pump(stream: asyncio.StreamReader | None, *, is_stdout: bool) -> None:
            nonlocal total_bytes, stdout_len, stderr_len, last_activity
            if stream is None:
                return
            while True:
                line = await stream.readline()
                if not line:
                    break
                last_activity = time.monotonic()
                log_fp.write(line)
                log_fp.flush()
                hasher.update(line)
                total_bytes += len(line)
                decoded = line.decode("utf-8", errors="replace")
                if is_stdout:
                    if on_stdout_line is not None:
                        on_stdout_line(decoded)
                    if stdout_len < MAX_STDOUT_EXCERPT_BYTES:
                        stdout_chunks.append(decoded)
                        stdout_len += len(decoded)
                else:
                    if stderr_len < 32_768:
                        stderr_chunks.append(decoded)
                        stderr_len += len(decoded)

        pump_out = asyncio.create_task(_pump(proc.stdout, is_stdout=True))
        pump_err = asyncio.create_task(_pump(proc.stderr, is_stdout=False))
        wait_task = asyncio.create_task(proc.wait())

        start_mono = time.monotonic()
        status = "succeeded"
        timeout_kind: str | None = None

        try:
            while not wait_task.done():
                if cancel_event.is_set():
                    status = "interrupted"
                    await terminate_process_group(proc, grace_seconds=grace_seconds)
                    break
                now = time.monotonic()
                if now - start_mono >= timeout_seconds:
                    status = "timed_out"
                    timeout_kind = "hard"
                    await terminate_process_group(proc, grace_seconds=grace_seconds)
                    break
                if idle_timeout_seconds is not None and (now - last_activity) >= idle_timeout_seconds:
                    status = "timed_out"
                    timeout_kind = "idle"
                    await terminate_process_group(proc, grace_seconds=grace_seconds)
                    break
                await asyncio.sleep(0.05)
            await asyncio.gather(pump_out, pump_err, return_exceptions=True)
            if not wait_task.done():
                await wait_task
        finally:
            log_fp.close()

        exit_code = proc.returncode
        if status == "succeeded" and exit_code != 0:
            status = "failed"

        return SupervisedRunOutput(
            status=status,
            exit_code=exit_code,
            stdout_excerpt="".join(stdout_chunks)[:MAX_STDOUT_EXCERPT_BYTES],
            stderr_excerpt="".join(stderr_chunks)[:32_768],
            log_ref=LogRef(
                runner_id=self._runner_id,
                path=str(log_path),
                bytes=total_bytes,
                sha256=hasher.hexdigest(),
            ),
            timeout_kind=timeout_kind,
        )
