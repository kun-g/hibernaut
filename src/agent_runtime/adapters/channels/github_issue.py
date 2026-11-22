from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast


def task_fields(issue: Mapping[str, Any]) -> dict[str, Any]:
    # The task shape every executor reads at the top level (as a Drive task carries it): the issue's title and
    # body, and its label names. gh, REST and webhooks all send labels as {"name": ...} objects and an empty
    # body as "" or null.
    labels: list[str] = []
    raw = issue.get("labels")
    for item in cast(list[object], raw) if isinstance(raw, list) else []:
        name = cast(dict[str, object], item).get("name") if isinstance(item, dict) else item
        if isinstance(name, str) and name.strip():
            labels.append(name.strip())
    title, body = issue.get("title"), issue.get("body")
    return {"title": title if isinstance(title, str) else "", "body": body if isinstance(body, str) else "",
            "labels": labels}
