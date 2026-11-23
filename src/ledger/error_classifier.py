"""3-tier deterministic ErrorKind classifier and 3-lane retry/alert policy (Locked Q2)."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Literal

from ledger.models import ErrorKind

STDERR_TAIL_BYTES = 4096

ErrorCategory = Literal["transient", "deterministic", "execution"]


@dataclass(frozen=True, slots=True)
class ErrorPolicy:
    category: ErrorCategory
    should_retry: bool
    fast_fail_alert: bool
    backoff_seconds: tuple[int, ...] = ()
    preserve_worktree: bool = True


_TRANSIENT_KINDS = frozenset({"rate_limit", "network", "crash"})
_DETERMINISTIC_KINDS = frozenset({"config_error", "permission_denied"})

# Tier 1: Hibernaut internal exception class names (avoid importing agent_runtime!)
_INTERNAL_EXC_MAP: dict[str, ErrorKind] = {
    "TimeoutError": "timeout",
    "AsyncioTimeoutError": "timeout",
    "WorkspaceIsolationError": "permission_denied",
    "PermissionError": "permission_denied",
    "DriveProxyError": "permission_denied",
    "AmbiguousProjectMatch": "config_error",
    "FileNotFoundError": "config_error",
    "KeyError": "config_error",
    "JSONDecodeError": "bad_output",
}

# Tier 2: Structured HTTP status codes from yetone/magpie or upstream APIs
_HTTP_STATUS_MAP: dict[int, ErrorKind] = {
    401: "config_error",
    403: "permission_denied",
    408: "timeout",
    429: "rate_limit",
    502: "network",
    503: "network",
    504: "timeout",
}

# Tier 3: Executor-scoped stderr tail regex rules (deterministic checked before transient)
_COMMON_RULES: tuple[tuple[ErrorKind, re.Pattern[str]], ...] = (
    ("config_error", re.compile(r"(invalid api key|missing api key|unauthorized|authentication_error|no such file or directory.*(claude|grok|dsh))", re.I)),
    ("permission_denied", re.compile(r"(permission denied|access denied|workspace isolation|operation not permitted|forbidden)", re.I)),
    ("rate_limit", re.compile(r"(rate[ _-]?limit|too many requests|quota exceeded|\b429\b|overloaded_error)", re.I)),
    ("network", re.compile(r"(connection reset|connection refused|temporary failure in name resolution|econnreset|etimedout|ssl:.*eof|bad gateway|502|503)", re.I)),
    ("timeout", re.compile(r"(timed out|deadline exceeded|watchdog timeout)", re.I)),
    ("bad_output", re.compile(r"(invalid json|jsondecodeerror|schema validation failed|failed to parse structured output)", re.I)),
)

_EXECUTOR_SPECIFIC_RULES: dict[str, tuple[tuple[ErrorKind, re.Pattern[str]], ...]] = {
    "claude": (
        ("config_error", re.compile(r"(apikeysource.*mismatch|anthropic_api_key.*missing|not logged in)", re.I)),
        ("rate_limit", re.compile(r"(anthropic.*429|rate_limit_error|overloaded)", re.I)),
    ),
    "grok": (
        ("config_error", re.compile(r"(xai_api_key|invalid x\.ai credentials)", re.I)),
        ("rate_limit", re.compile(r"(xai.*429|resource_exhausted)", re.I)),
    ),
    "dsh": (
        ("config_error", re.compile(r"(dsh:.*command not found|missing profile)", re.I)),
    ),
}


def classify_error(
    *,
    exc: BaseException | None = None,
    http_status: int | None = None,
    structured_code: str | None = None,
    executor_id: str | None = None,
    exit_code: int | None = None,
    stderr_text: str | None = None,
) -> ErrorKind:
    """Classify failure using Locked Q2 2.1 3-tier priority (zero external deps).

    Priority:
      Tier 1: Hibernaut / Python internal exception type
      Tier 2: Structured HTTP status or gateway/CLI error code (Magpie / JSON)
      Tier 3: Executor-specific + common regex on last 4KB of stderr
      Fallback: signal exit_code (<0 or >=128) -> crash; non-zero exit -> tool_error; else unknown
    """
    # Tier 1: Internal exception
    if exc is not None:
        exc_name = type(exc).__name__
        if exc_name in _INTERNAL_EXC_MAP:
            return _INTERNAL_EXC_MAP[exc_name]
        if isinstance(exc, TimeoutError):
            return "timeout"
        if isinstance(exc, PermissionError):
            return "permission_denied"
        if isinstance(exc, (ConnectionError, OSError)) and exc_name != "FileNotFoundError":
            return "network"

    # Tier 2: Structured HTTP status or structured code from Magpie / CLI JSON
    if http_status is not None and http_status in _HTTP_STATUS_MAP:
        return _HTTP_STATUS_MAP[http_status]
    if structured_code:
        code_l = structured_code.lower()
        if any(k in code_l for k in ("rate_limit", "429", "overloaded", "quota")):
            return "rate_limit"
        if any(k in code_l for k in ("auth", "invalid_api_key", "config", "401")):
            return "config_error"
        if any(k in code_l for k in ("permission", "forbidden", "blocked", "403")):
            return "permission_denied"
        if any(k in code_l for k in ("timeout", "deadline", "504")):
            return "timeout"
        if any(k in code_l for k in ("network", "connection", "502", "503")):
            return "network"
        if any(k in code_l for k in ("bad_output", "schema", "parse")):
            return "bad_output"

    # Tier 3: Executor-specific & common regex on last 4KB of stderr (plus exc message)
    tail_source = ""
    if stderr_text:
        encoded = stderr_text.encode("utf-8", errors="ignore")
        tail_source = encoded[-STDERR_TAIL_BYTES:].decode("utf-8", errors="ignore")
    if exc is not None:
        tail_source = f"{tail_source}\n{exc}"

    if tail_source.strip():
        exec_key = (executor_id or "").lower().split("-")[0]
        for kind, pattern in _EXECUTOR_SPECIFIC_RULES.get(exec_key, ()):
            if pattern.search(tail_source):
                return kind
        for kind, pattern in _COMMON_RULES:
            if pattern.search(tail_source):
                return kind

    # Fallback on POSIX signal / non-zero exit code
    if exit_code is not None and exit_code != 0:
        if exit_code < 0 or exit_code >= 128:
            return "crash"
        return "tool_error"

    return "unknown"


def route_error(error_kind: ErrorKind) -> ErrorPolicy:
    """Return the Locked Q2 2.2 action policy for a classified ErrorKind."""
    if error_kind in _TRANSIENT_KINDS:
        return ErrorPolicy(
            category="transient",
            should_retry=True,
            fast_fail_alert=False,
            backoff_seconds=(5, 15, 45),
            preserve_worktree=True,
        )
    if error_kind in _DETERMINISTIC_KINDS:
        return ErrorPolicy(
            category="deterministic",
            should_retry=False,
            fast_fail_alert=True,
            backoff_seconds=(),
            preserve_worktree=True,
        )
    # Execution / semantic / unknown: Locked Q2 2.2 -> default 0 retries, preserve现场 & alert
    return ErrorPolicy(
        category="execution",
        should_retry=False,
        fast_fail_alert=True,
        backoff_seconds=(),
        preserve_worktree=True,
    )
