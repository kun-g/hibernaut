from __future__ import annotations

import re
from collections.abc import Sequence
from typing import cast

_MAX_REASON_CHARS = 200


def parse_quota_errors(where: str, raw: object) -> tuple[str, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise ValueError(f"{where}: quota_errors must be a list of regular expressions")
    patterns: list[str] = []
    for item in cast(list[object], raw):
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{where}: quota_errors must be a list of regular expressions, got {item!r}")
        try:
            matches_nothing = re.compile(item).search("") is not None
        except (re.error, OverflowError, RecursionError) as exc:
            raise ValueError(f"{where}: quota_errors has an invalid regular expression {item!r}: {exc}") from exc
        if matches_nothing:
            # It would turn every failure into an exhausted quota.
            raise ValueError(f"{where}: quota_errors: {item!r} also matches empty output")
        patterns.append(item)
    return tuple(patterns)


def one_line(text: str, limit: int = _MAX_REASON_CHARS) -> str:
    # Written by other programs and kept in the database and messages: no lone surrogates or control characters.
    joined = " ".join("".join(c if c.isprintable() else " " for c in text).split())
    return joined if len(joined) <= limit else joined[: limit - 1] + "…"


def quota_hit(patterns: Sequence[str], *outputs: str) -> str | None:
    # The line that says the quota is used up, or just the matched text where that line is long; None without a match.
    for pattern in patterns:
        for text in outputs:
            found = re.search(pattern, text)
            if found is None:
                continue
            end = text.find("\n", found.end())
            line = text[text.rfind("\n", 0, found.start()) + 1: end if end >= 0 else len(text)]
            whole = one_line(line, limit=_MAX_REASON_CHARS + 1)
            return whole if 0 < len(whole) <= _MAX_REASON_CHARS else one_line(found.group()) or one_line(pattern)
    return None
