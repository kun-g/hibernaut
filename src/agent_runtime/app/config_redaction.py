from __future__ import annotations

import os
import re
from collections.abc import Mapping
from typing import Any, cast

_REMOTE_LOGIN = re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://)[^\s/@]+@")
_TOKEN = re.compile(r"\b(?:gh[a-z]_[A-Za-z0-9]{8,}|github_pat_[A-Za-z0-9_]{8,}|sk-[A-Za-z0-9_-]{8,})")
_SECRET_NAME = re.compile(r"(?i)(?:token|secret|password|passwd|api_?key|credential|_key)")
_MIN_SECRET = 6


def _secret_values(raw: Mapping[str, Any]) -> set[str]:
    found: set[str] = set()

    def walk(node: object, secret: bool) -> None:
        if isinstance(node, dict):
            for key, value in cast(dict[str, object], node).items():
                # Keys such as token_env and token_file name where a secret is, not the secret.
                named = bool(_SECRET_NAME.search(key)) and not key.endswith(("_env", "_file"))
                walk(value, secret or key == "env" or named)
        elif isinstance(node, list):
            for item in cast(list[object], node):
                walk(item, secret)
        elif secret and isinstance(node, str):
            found.add(node)

    walk(raw, False)
    return found


def scrub(text: str, raw: Mapping[str, Any] | None = None) -> str:
    # Text of commands and parsers, before it reaches an agent, an answer or a log.
    secrets = {value for name, value in os.environ.items() if _SECRET_NAME.search(name)}
    if raw is not None:
        secrets |= _secret_values(raw)
    for value in sorted((s for s in secrets if len(s) >= _MIN_SECRET), key=len, reverse=True):
        text = text.replace(value, "***")
    return _TOKEN.sub("***", _REMOTE_LOGIN.sub(r"\1***@", text))
