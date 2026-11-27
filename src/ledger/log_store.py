"""External .log store (local://) and distributed runner:// tail reader (Locked Q3 3.1)."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, UTC
from pathlib import Path
import re
from typing import Literal
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ledger.ports import RemoteLogFetcherPort

_SAFE_SEGMENT_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

DEFAULT_RETENTION_DAYS = 14
DEFAULT_MAX_TOTAL_BYTES = 500 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class LogTailResult:
    log_ref: str
    source: Literal["local", "remote", "fallback_summary", "missing"]
    content: str
    truncated: bool = False
    warning: str | None = None


def parse_log_ref(log_ref: str) -> tuple[str, str | None, str]:
    """Parse log_ref URI into (scheme, runner_id_or_none, relative_path).

    Supported formats:
      - local://YYYY-MM-DD/run_xxx.log
      - runner://<runner_id>/YYYY-MM-DD/run_xxx.log
      - bare relative path YYYY-MM-DD/run_xxx.log (treated as local)
    """
    if log_ref.startswith("local://"):
        rel = log_ref[len("local://") :]
        return ("local", None, rel)
    if log_ref.startswith("runner://"):
        rest = log_ref[len("runner://") :]
        parts = rest.split("/", 1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError(f"invalid runner log_ref: {log_ref!r}")
        return ("runner", parts[0], parts[1])
    return ("local", None, log_ref)


def _validate_rel_path(rel_path: str) -> Path:
    parts = Path(rel_path).parts
    if not parts or any(p in ("..", ".", "") or not _SAFE_SEGMENT_RE.match(p) for p in parts):
        raise ValueError(f"unsafe relative log path: {rel_path!r}")
    return Path(*parts)


class LocalLogStore:
    """Manages <base_dir>/logs/runs/YYYY-MM-DD/<run_id>.log with TTL & size retention."""

    def __init__(self, base_dir: str | Path) -> None:
        self._root = Path(base_dir) / "logs" / "runs"
        self._root.mkdir(parents=True, exist_ok=True)

    @property
    def root_dir(self) -> Path:
        return self._root

    def allocate_path(self, run_id: str, *, date_str: str | None = None) -> tuple[Path, str]:
        if not _SAFE_SEGMENT_RE.match(run_id):
            raise ValueError(f"unsafe run_id for log path: {run_id!r}")
        day = date_str or datetime.now(UTC).strftime("%Y-%m-%d")
        if not _DATE_RE.match(day):
            raise ValueError(f"date_str must be YYYY-MM-DD, got {day!r}")
        day_dir = self._root / day
        day_dir.mkdir(parents=True, exist_ok=True)
        file_path = day_dir / f"{run_id}.log"
        return file_path, f"local://{day}/{run_id}.log"

    def write_log(self, run_id: str, content: str, *, date_str: str | None = None) -> str:
        file_path, log_ref = self.allocate_path(run_id, date_str=date_str)
        file_path.write_text(content, encoding="utf-8")
        return log_ref

    def append_chunk(self, run_id: str, chunk: str, *, date_str: str | None = None) -> str:
        file_path, log_ref = self.allocate_path(run_id, date_str=date_str)
        with file_path.open("a", encoding="utf-8") as fh:
            fh.write(chunk)
        return log_ref

    def resolve_local_path(self, rel_path: str) -> Path:
        safe_rel = _validate_rel_path(rel_path)
        resolved = (self._root / safe_rel).resolve()
        root_resolved = self._root.resolve()
        if root_resolved not in resolved.parents:
            raise ValueError(f"log path escapes root directory: {rel_path!r}")
        return resolved

    def read_tail(
        self,
        log_ref: str,
        *,
        tail_lines: int = 120,
        max_bytes: int = 65536,
    ) -> str:
        scheme, _, rel_path = parse_log_ref(log_ref)
        if scheme != "local":
            raise ValueError(f"LocalLogStore cannot read non-local ref: {log_ref!r}")
        file_path = self.resolve_local_path(rel_path)
        if not file_path.is_file():
            raise FileNotFoundError(f"log file not found: {log_ref}")
        lines: deque[str] = deque(maxlen=max(1, tail_lines))
        with file_path.open("r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                lines.append(line)
        joined = "".join(lines)
        encoded = joined.encode("utf-8")
        if len(encoded) > max_bytes:
            return encoded[-max_bytes:].decode("utf-8", errors="ignore")
        return joined

    def cleanup(
        self,
        *,
        max_age_days: int = DEFAULT_RETENTION_DAYS,
        max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES,
        now_utc: datetime | None = None,
    ) -> list[Path]:
        """Remove logs older than max_age_days, then oldest logs exceeding max_total_bytes."""
        now = now_utc or datetime.now(UTC)
        cutoff_day = (now - timedelta(days=max_age_days)).strftime("%Y-%m-%d")
        removed: list[Path] = []
        remaining: list[tuple[float, int, Path]] = []

        for file_path in sorted(self._root.glob("*/*.log")):
            day_name = file_path.parent.name
            if _DATE_RE.match(day_name) and day_name < cutoff_day:
                file_path.unlink(missing_ok=True)
                removed.append(file_path)
                continue
            try:
                st = file_path.stat()
            except OSError:
                continue
            remaining.append((st.st_mtime, st.st_size, file_path))

        total_bytes = sum(sz for _, sz, _ in remaining)
        if total_bytes > max_total_bytes:
            remaining.sort(key=lambda item: item[0])
            for _, sz, fp in remaining:
                if total_bytes <= max_total_bytes:
                    break
                fp.unlink(missing_ok=True)
                removed.append(fp)
                total_bytes -= sz

        # Prune empty date directories
        for day_dir in self._root.iterdir():
            if day_dir.is_dir() and not any(day_dir.iterdir()):
                day_dir.rmdir()

        return removed


class HttpRemoteLogFetcher:
    """Pulls log tails from remote VPS Runners over HTTPS/HTTP with Bearer auth."""

    def __init__(
        self,
        runner_endpoints: dict[str, str],
        *,
        bearer_token: str | None = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        self._endpoints = {k: v.rstrip("/") for k, v in runner_endpoints.items()}
        self._token = bearer_token
        self._timeout = timeout_seconds

    def fetch_tail(
        self,
        runner_id: str,
        rel_path: str,
        *,
        tail_lines: int = 120,
        max_bytes: int = 65536,
    ) -> str:
        base = self._endpoints.get(runner_id)
        if not base:
            raise LookupError(f"unknown remote runner_id: {runner_id!r}")
        _validate_rel_path(rel_path)
        query = urlencode({"path": rel_path, "tail_lines": tail_lines, "max_bytes": max_bytes})
        url = f"{base}/logs/tail?{query}"
        headers = {"Accept": "text/plain"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        req = Request(url, headers=headers, method="GET")
        with urlopen(req, timeout=self._timeout) as resp:  # noqa: S310
            raw: bytes = resp.read(max_bytes)
        return raw.decode("utf-8", errors="replace")


class CompositeLogReader:
    """Unified log reader routing local:// to LocalLogStore and runner:// to RemoteLogFetcherPort."""

    def __init__(
        self,
        local_store: LocalLogStore,
        remote_fetcher: RemoteLogFetcherPort | None = None,
    ) -> None:
        self._local = local_store
        self._remote = remote_fetcher

    def read_tail(
        self,
        log_ref: str | None,
        *,
        tail_lines: int = 120,
        max_bytes: int = 65536,
        fallback_summary: str | None = None,
    ) -> LogTailResult:
        if not log_ref:
            if fallback_summary:
                return LogTailResult(
                    log_ref="",
                    source="fallback_summary",
                    content=fallback_summary,
                    warning="log_ref is absent; returning stored error_summary",
                )
            return LogTailResult(
                log_ref="",
                source="missing",
                content="",
                warning="no log_ref recorded for this run",
            )

        scheme, runner_id, rel_path = parse_log_ref(log_ref)
        if scheme == "local":
            try:
                text = self._local.read_tail(log_ref, tail_lines=tail_lines, max_bytes=max_bytes)
                return LogTailResult(
                    log_ref=log_ref,
                    source="local",
                    content=text,
                    truncated=len(text.encode("utf-8")) >= max_bytes,
                )
            except FileNotFoundError:
                return LogTailResult(
                    log_ref=log_ref,
                    source="fallback_summary" if fallback_summary else "missing",
                    content=fallback_summary or "",
                    warning="local log expired or missing; falling back to stored summary",
                )

        assert runner_id is not None
        if self._remote is None:
            return LogTailResult(
                log_ref=log_ref,
                source="fallback_summary" if fallback_summary else "missing",
                content=fallback_summary or "",
                warning=f"no remote fetcher configured for runner '{runner_id}'",
            )
        try:
            text = self._remote.fetch_tail(
                runner_id,
                rel_path,
                tail_lines=tail_lines,
                max_bytes=max_bytes,
            )
            return LogTailResult(
                log_ref=log_ref,
                source="remote",
                content=text,
                truncated=len(text.encode("utf-8")) >= max_bytes,
            )
        except Exception as exc:  # noqa: BLE001
            return LogTailResult(
                log_ref=log_ref,
                source="fallback_summary" if fallback_summary else "missing",
                content=fallback_summary or "",
                warning=f"remote runner '{runner_id}' unreachable ({exc!r}); returning fallback summary",
            )
