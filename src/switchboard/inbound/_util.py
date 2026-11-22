from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, cast

from switchboard.ports import AdapterParseResult


def lower_headers(headers: Mapping[str, str]) -> dict[str, str]:
    return {key.lower(): value for key, value in headers.items()}


def parse_json_object(raw_body: bytes) -> dict[str, Any] | None:
    try:
        data = json.loads(raw_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return cast(dict[str, Any], data) if isinstance(data, dict) else None


def obj(value: Any) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def items(value: Any) -> list[Any]:
    return cast(list[Any], value) if isinstance(value, list) else []


def text(value: Any) -> str:
    return "" if value is None else str(value)


def reject(status: int, error: str) -> AdapterParseResult:
    return AdapterParseResult(event=None, http_status=status, response_body={"error": error})


def ignored(reason: str) -> AdapterParseResult:
    return AdapterParseResult(event=None, http_status=200, response_body={"status": "ignored", "reason": reason})
