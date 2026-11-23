from __future__ import annotations

import asyncio
import codecs
import hashlib
import os
import re
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Final, Mapping, Sequence

from runner.protocol import CredentialLease, LogRef, MAX_STDOUT_EXCERPT_BYTES

_READ_CHUNK_BYTES: Final = 65_536
_STDERR_EXCERPT_CHARS: Final = 32_768
# Bounds the buffer behind on_stdout_line only; the log and the excerpt are not affected by it.
MAX_STREAM_LINE_BYTES: Final = 4 * 1024 * 1024

# The child gets only what a CLI needs to run (paths, locale, terminal, proxy), never the runner's own
# credentials: external auth goes through the credential lease (RUNNER_BROKER_URL / RUNNER_LEASE_TOKEN).
CHILD_ENV_ALLOWLIST: Final = frozenset(
    {
        "PATH",
        "HOME",
        "USER",
        "LOGNAME",
        "SHELL",
        "TMPDIR",
        "TMP",
        "TEMP",
        "TZ",
        "LANG",
        "LANGUAGE",
        "TERM",
        "COLORTERM",
        "NO_COLOR",
        "XDG_CONFIG_HOME",
        "XDG_CACHE_HOME",
        "XDG_DATA_HOME",
        "XDG_STATE_HOME",
        "XDG_RUNTIME_DIR",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "no_proxy",
    }
)
CHILD_ENV_ALLOWED_PREFIXES: Final = ("LC_",)
_LEASE_ENV_KEYS: Final = frozenset({"RUNNER_BROKER_URL", "RUNNER_LEASE_TOKEN", "RUNNER_LEASE_EXPIRES_AT"})
# Driver env is configured, not inherited, so it may set anything except a credential.
_CREDENTIAL_NAME_RE: Final = re.compile(
    r"(TOKEN|SECRET|PASSWORD|PASSWD|CREDENTIAL|API_?KEY|ACCESS_?KEY|PRIVATE_?KEY|AUTH|COOKIE|SESSION_?KEY)",
    re.IGNORECASE,
)


# A URL with user info (scheme://user[:password]@host), e.g. an authenticated proxy: allowed names can carry secrets too.
_URL_USERINFO_RE: Final = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^/?#\s]*@")


def is_credential_env_name(name: str) -> bool:
    return bool(_CREDENTIAL_NAME_RE.search(name))


def _is_proxy_env_name(name: str) -> bool:
    lowered = name.lower()
    return lowered.endswith("_proxy") and lowered != "no_proxy"


class ChildEnvError(PermissionError):
    # The message names the variable only, never its value.
    pass


def _carries_url_credentials(name: str, value: str) -> bool:
    # A proxy value needs no URL parsing: clients take a bare "user:pass@host:port", and a password with an unescaped
    # "#", "/" or "?" ends the authority for a URL parser but not for every client. A plain proxy address has no "@".
    if _is_proxy_env_name(name) and "@" in value:
        return True
    return bool(_URL_USERINFO_RE.search(value))


def build_child_env(
    base_env: Mapping[str, str],
    *,
    extra_env: Mapping[str, str] | None = None,
    credential_lease: CredentialLease | None = None,
) -> dict[str, str]:
    env = {
        k: v
        for k, v in base_env.items()
        if (k in CHILD_ENV_ALLOWLIST or k.startswith(CHILD_ENV_ALLOWED_PREFIXES)) and not is_credential_env_name(k)
    }
    # Refused rather than dropped, so a node configured with an authenticated proxy fails loudly instead of running
    # offline; messages name the variable only, never its value.
    for k, v in env.items():
        if _carries_url_credentials(k, v):
            raise ChildEnvError(f"runner environment variable {k!r} carries URL credentials")
    env["GIT_TERMINAL_PROMPT"] = "0"
    if extra_env:
        for k, v in extra_env.items():
            if is_credential_env_name(k) or k in _LEASE_ENV_KEYS:
                raise ChildEnvError(f"refusing to inject credential-like key {k!r} into child env")
            if _carries_url_credentials(k, v):
                raise ChildEnvError(f"driver environment variable {k!r} carries URL credentials")
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


def _signal_group(pgid: int, sig: signal.Signals) -> bool:
    try:
        os.killpg(pgid, sig)
    except (ProcessLookupError, PermissionError, OSError):
        return False
    return True


def group_is_only_zombies(pgid: int, *, proc_root: Path = Path("/proc")) -> bool:
    # In a container whose PID 1 does not reap orphans, killed descendants stay as zombies and killpg() keeps
    # succeeding on them. False wherever /proc cannot say (macOS) or shows no member: the signal decides then.
    try:
        pids = [entry.name for entry in proc_root.iterdir() if entry.name.isdigit()]
    except OSError:
        return False
    seen = False
    for pid in pids:
        try:
            stat = (proc_root / pid / "stat").read_text(encoding="utf-8", errors="replace")
        except (FileNotFoundError, ProcessLookupError):
            continue
        except OSError:
            return False
        # The command name may itself hold spaces and parentheses; the fields after the last one are fixed:
        # state, ppid, pgrp, ... with num_threads 15 further on.
        fields = stat.rpartition(")")[2].split()
        if len(fields) < 18 or int(fields[2]) != pgid:
            continue
        seen = True
        # A leader thread that exited shows Z while its other threads still run.
        if fields[0] not in ("Z", "X") or int(fields[17]) > 1:
            return False
    return seen


async def terminate_process_group(proc: asyncio.subprocess.Process, *, grace_seconds: float = 2.0) -> None:
    # The child runs with start_new_session=True, so its pid is the process group id of everything it started.
    pgid = proc.pid
    if proc.returncode is None:
        if not _signal_group(pgid, signal.SIGTERM):
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
        try:
            await asyncio.wait_for(proc.wait(), timeout=grace_seconds)
        except asyncio.TimeoutError:
            if not _signal_group(pgid, signal.SIGKILL):
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
            await proc.wait()
    # Descendants that outlived (or ignored SIGTERM from) the leader would keep writing the worktree.
    deadline = time.monotonic() + max(grace_seconds, 1.0)
    while _signal_group(pgid, signal.SIGKILL) and time.monotonic() < deadline:
        if group_is_only_zombies(pgid):
            break
        await asyncio.sleep(0.02)


async def reap_uncancellable(proc: asyncio.subprocess.Process, *, grace_seconds: float) -> bool:
    # Finishes the reap even if the caller is cancelled again meanwhile; returns True when that happened so the
    # caller can re-raise the cancellation afterwards.
    reap = asyncio.ensure_future(terminate_process_group(proc, grace_seconds=grace_seconds))
    cancelled = False
    while not reap.done():
        try:
            await asyncio.shield(reap)
        except asyncio.CancelledError:
            cancelled = True
    reap.result()
    return cancelled


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

        # Opened before the child exists: once it does, every exit has to go through the reap below.
        log_fp = log_path.open("wb")
        try:
            proc = await asyncio.create_subprocess_exec(
                *argv,
                cwd=str(cwd),
                env=dict(env),
                stdin=asyncio.subprocess.PIPE if stdin_text is not None else asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            )
        except BaseException:
            log_fp.close()
            raise

        hasher = hashlib.sha256()
        total_bytes = 0
        stdout_chunks: list[str] = []
        stderr_chunks: list[str] = []
        stdout_len = 0
        stderr_len = 0
        last_activity = time.monotonic()
        status = "succeeded"
        timeout_kind: str | None = None
        read_error: BaseException | None = None

        async def _pump(stream: asyncio.StreamReader | None, *, is_stdout: bool) -> None:
            # Bounded chunks, never readline(): a line longer than the stream limit must not stop the draining, or
            # the child blocks on a full pipe and its output is lost.
            nonlocal total_bytes, stdout_len, stderr_len, last_activity
            if stream is None:
                return
            decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
            line = bytearray()

            def _emit_line() -> None:
                if on_stdout_line is not None:
                    on_stdout_line(line.decode("utf-8", errors="replace"))
                line.clear()

            def _keep_line_part(part: bytes) -> None:
                room = MAX_STREAM_LINE_BYTES - len(line)
                if room > 0:
                    line.extend(part[:room])

            while True:
                chunk = await stream.read(_READ_CHUNK_BYTES)
                if not chunk:
                    break
                last_activity = time.monotonic()
                log_fp.write(chunk)
                log_fp.flush()
                hasher.update(chunk)
                total_bytes += len(chunk)
                text = decoder.decode(chunk)
                if is_stdout:
                    if stdout_len < MAX_STDOUT_EXCERPT_BYTES:
                        stdout_chunks.append(text)
                        stdout_len += len(text)
                    if on_stdout_line is not None:
                        pos = 0
                        while (nl := chunk.find(b"\n", pos)) != -1:
                            _keep_line_part(chunk[pos : nl + 1])
                            _emit_line()
                            pos = nl + 1
                        _keep_line_part(chunk[pos:])
                elif stderr_len < _STDERR_EXCERPT_CHARS:
                    stderr_chunks.append(text)
                    stderr_len += len(text)
            tail = decoder.decode(b"", final=True)
            if is_stdout:
                if tail and stdout_len < MAX_STDOUT_EXCERPT_BYTES:
                    stdout_chunks.append(tail)
                if line:
                    _emit_line()
            elif tail and stderr_len < _STDERR_EXCERPT_CHARS:
                stderr_chunks.append(tail)

        async def _feed_stdin(stream: asyncio.StreamWriter, data: bytes) -> None:
            # Its own task: a child that never reads its input must not hold up the watchdog or an interrupt.
            try:
                stream.write(data)
                await stream.drain()
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                stream.close()

        pumps: list[asyncio.Task[None]] = []
        feed: asyncio.Task[None] | None = None
        # Every exit from here on, cancellation included, goes through the finally: the run is not over while any
        # process of its group can still touch the worktree.
        try:
            if stdin_text is not None and proc.stdin is not None:
                feed = asyncio.create_task(_feed_stdin(proc.stdin, stdin_text.encode("utf-8")))

            pumps = [
                asyncio.create_task(_pump(proc.stdout, is_stdout=True)),
                asyncio.create_task(_pump(proc.stderr, is_stdout=False)),
            ]
            start_mono = time.monotonic()
            while proc.returncode is None:
                failed_pump = next((t for t in pumps if t.done() and not t.cancelled() and t.exception() is not None), None)
                if failed_pump is not None:
                    read_error = failed_pump.exception()
                    break
                if cancel_event.is_set():
                    status = "interrupted"
                    break
                now = time.monotonic()
                if now - start_mono >= timeout_seconds:
                    status = "timed_out"
                    timeout_kind = "hard"
                    break
                if idle_timeout_seconds is not None and (now - last_activity) >= idle_timeout_seconds:
                    status = "timed_out"
                    timeout_kind = "idle"
                    break
                await asyncio.sleep(0.05)
        finally:
            if feed is not None and not feed.done():
                feed.cancel()
            recancelled = await reap_uncancellable(proc, grace_seconds=grace_seconds)
            if feed is not None:
                await asyncio.gather(feed, return_exceptions=True)
            if pumps:
                # Pipes close once the whole group is gone; a stray inheritor cannot hold the pumps open for long.
                _, pending = await asyncio.wait(pumps, timeout=grace_seconds)
                for t in pending:
                    t.cancel()
                for outcome in await asyncio.gather(*pumps, return_exceptions=True):
                    if read_error is None and isinstance(outcome, Exception):
                        read_error = outcome
            log_fp.close()
            if recancelled:
                raise asyncio.CancelledError

        exit_code = proc.returncode
        stderr_excerpt = "".join(stderr_chunks)[:_STDERR_EXCERPT_CHARS]
        if read_error is not None:
            # The output is incomplete, so whatever the exit code says, this run did not produce a usable result.
            status = "failed"
            stderr_excerpt = f"runner output read failed: {type(read_error).__name__}: {read_error}\n{stderr_excerpt}"
        elif status == "succeeded" and exit_code != 0:
            status = "failed"

        return SupervisedRunOutput(
            status=status,
            exit_code=exit_code,
            stdout_excerpt="".join(stdout_chunks)[:MAX_STDOUT_EXCERPT_BYTES],
            stderr_excerpt=stderr_excerpt,
            log_ref=LogRef(
                runner_id=self._runner_id,
                path=str(log_path),
                bytes=total_bytes,
                sha256=hasher.hexdigest(),
            ),
            timeout_kind=timeout_kind,
        )
