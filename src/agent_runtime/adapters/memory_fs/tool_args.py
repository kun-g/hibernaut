from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from memory_hub import MemoryHub, SearchQuery, WriteProposal

_MAX_TOP_K = 50


def required(args: Mapping[str, Any], key: str) -> str:
    value = args.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be a non-empty string")
    return value


def optional(args: Mapping[str, Any], key: str) -> str | None:
    value = args.get(key)
    if value is not None and not isinstance(value, str):
        raise ValueError(f"{key} must be a string")
    return value


def flag(args: Mapping[str, Any], key: str, default: bool = False) -> bool:
    value = args.get(key, default)
    if not isinstance(value, bool):
        raise ValueError(f"{key} must be true or false")
    return value


def count(args: Mapping[str, Any], key: str) -> int | None:
    value = args.get(key)
    if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 1):
        raise ValueError(f"{key} must be a positive integer")
    return value


def search_query(scope: str, args: Mapping[str, Any]) -> SearchQuery:
    return SearchQuery(
        scope=scope,
        query=required(args, "query"),
        top_k=min(count(args, "top_k") or 8, _MAX_TOP_K),
        include_shared=flag(args, "include_shared", True),
        include_kb=flag(args, "include_kb"),
        include_archived=flag(args, "include_archived"),
    )


def read_document(hub: MemoryHub, uri: str, args: Mapping[str, Any]) -> dict[str, Any]:
    try:
        doc = hub.read_on_demand(uri, heading=optional(args, "heading"), max_bytes=count(args, "max_bytes"))
    except KeyError as exc:
        raise ValueError(str(exc.args[0])) from None
    return doc.to_dict()


def write_proposal(uri: str, args: Mapping[str, Any], *, author: str | None) -> WriteProposal:
    content = args.get("content")
    if not isinstance(content, str):
        raise ValueError("content must be a string")
    supersedes: object = args.get("supersedes") or []
    if not isinstance(supersedes, list) or not all(isinstance(s, str) for s in cast(list[object], supersedes)):
        raise ValueError("supersedes must be an array of strings")
    # The author is whoever calls, never what the arguments claim.
    return WriteProposal.from_dict({
        "target_uri": uri,
        "op": required(args, "op"),
        "content": content,
        "rationale": required(args, "rationale"),
        "section_heading": optional(args, "section_heading"),
        "base_sha256": optional(args, "base_sha256"),
        "evidence_ref": optional(args, "evidence_ref"),
        "supersedes": supersedes,
        "force_new": flag(args, "force_new"),
        "author": author,
    })
