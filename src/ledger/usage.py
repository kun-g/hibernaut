"""Dual-source Token & USD cost extraction (Magpie authoritative, CLI fallback; Locked Q3 3.2)."""

from __future__ import annotations

from collections.abc import Mapping
import json
from typing import Any, cast

from ledger.models import RunUsage


def magpie_trace_headers(
    *,
    run_id: str,
    correlation_key: str | None = None,
    trace_id: str | None = None,
    executor_id: str | None = None,
) -> dict[str, str]:
    """Build tracing headers passed through to yetone/magpie for authoritative cost reconciliation."""
    headers = {"X-Run-Id": run_id}
    if correlation_key:
        headers["X-Correlation-Key"] = correlation_key
    if trace_id:
        headers["X-Trace-Id"] = trace_id
    if executor_id:
        headers["X-Executor-Id"] = executor_id
    return headers


def _opt_int(val: Any) -> int | None:
    if isinstance(val, int) and not isinstance(val, bool) and val >= 0:
        return val
    return None


def _opt_float(val: Any) -> float | None:
    if isinstance(val, (int, float)) and not isinstance(val, bool) and val >= 0:
        return float(val)
    return None


def extract_usage(
    *,
    magpie_usage: Mapping[str, Any] | None = None,
    cli_json: Mapping[str, Any] | None = None,
    cli_stdout: str | None = None,
) -> RunUsage | None:
    """Extract RunUsage with Magpie taking precedence over CLI JSON output."""
    if magpie_usage:
        return RunUsage(
            usage_source="magpie",
            model=magpie_usage.get("model") if isinstance(magpie_usage.get("model"), str) else None,
            input_tokens=_opt_int(magpie_usage.get("input_tokens") or magpie_usage.get("prompt_tokens")),
            output_tokens=_opt_int(magpie_usage.get("output_tokens") or magpie_usage.get("completion_tokens")),
            cache_read_tokens=_opt_int(magpie_usage.get("cache_read_tokens") or magpie_usage.get("cache_read_input_tokens")),
            cost_usd=_opt_float(magpie_usage.get("cost_usd") or magpie_usage.get("total_cost_usd")),
        )

    parsed_cli: Mapping[str, Any] | None = cli_json
    if parsed_cli is None and cli_stdout:
        parsed_cli = _parse_last_json_usage(cli_stdout)

    if parsed_cli:
        usage_block = parsed_cli.get("usage")
        u_map: Mapping[str, Any] = cast(Mapping[str, Any], usage_block) if isinstance(usage_block, Mapping) else parsed_cli
        in_tok = _opt_int(u_map.get("input_tokens") or u_map.get("prompt_tokens"))
        out_tok = _opt_int(u_map.get("output_tokens") or u_map.get("completion_tokens"))
        cache_tok = _opt_int(u_map.get("cache_read_input_tokens") or u_map.get("cache_read_tokens"))
        cost = _opt_float(parsed_cli.get("total_cost_usd") or parsed_cli.get("cost_usd") or u_map.get("cost_usd"))
        model = parsed_cli.get("model") if isinstance(parsed_cli.get("model"), str) else None
        if any(v is not None for v in (in_tok, out_tok, cache_tok, cost, model)):
            return RunUsage(
                usage_source="cli",
                model=model,
                input_tokens=in_tok,
                output_tokens=out_tok,
                cache_read_tokens=cache_tok,
                cost_usd=cost,
            )

    return None


def _parse_last_json_usage(stdout_text: str) -> Mapping[str, Any] | None:
    """Scan stream-json / NDJSON lines from the end to find the final result/usage object."""
    for raw_line in reversed(stdout_text.splitlines()):
        line = raw_line.strip()
        if not line.startswith("{") or not line.endswith("}"):
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict) and (
            "usage" in obj or "total_cost_usd" in obj or "input_tokens" in obj
        ):
            return cast(Mapping[str, Any], obj)
    return None
