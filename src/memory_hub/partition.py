from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

_SAFE_SEGMENT_RE = re.compile(r"[^A-Za-z0-9._-]+")

UriScheme = Literal["global", "proj", "shared", "kb"]


def sanitize_segment(value: str, *, field_name: str = "segment") -> str:
    if not value or not value.strip():
        raise ValueError(f"{field_name} must be non-empty")
    if ".." in value or "/" in value or "\\" in value:
        raise ValueError(f"Invalid path characters in {field_name}: {value}")
    return _SAFE_SEGMENT_RE.sub("__", value.strip())


def resolve_project_key(inbox_id: str, inbox_project_map: dict[str, str] | None = None) -> str:
    if inbox_project_map and inbox_id in inbox_project_map:
        return sanitize_segment(inbox_project_map[inbox_id], field_name="project_key")
    cleaned = inbox_id.strip()
    if cleaned.startswith("proj:"):
        rest = cleaned[len("proj:") :].split(":", 1)[0]
        if rest:
            return sanitize_segment(rest, field_name="project_key")
    if ":" in cleaned:
        first, _ = cleaned.split(":", 1)
        if first and first != "user":
            return sanitize_segment(first, field_name="project_key")
    return sanitize_segment(cleaned, field_name="project_key")


@dataclass(frozen=True, slots=True)
class ParsedUri:
    scheme: UriScheme
    scope: str
    rel_path: str
    raw_uri: str


def parse_memory_uri(uri: str) -> ParsedUri:
    if "://" not in uri:
        raise ValueError(f"Memory URI must include scheme (global://, proj://, shared://, kb://): {uri}")
    scheme_raw, remainder = uri.split("://", 1)
    if scheme_raw not in ("global", "proj", "shared", "kb"):
        raise ValueError(f"Unsupported memory URI scheme: {scheme_raw}")
    scheme: UriScheme = scheme_raw
    cleaned = remainder.lstrip("/")
    if not cleaned:
        raise ValueError(f"Memory URI path cannot be empty: {uri}")
    if "\\" in cleaned or ".." in cleaned.split("/"):
        raise ValueError(f"Path traversal forbidden in memory URI: {uri}")

    if scheme in ("global", "shared"):
        return ParsedUri(scheme=scheme, scope=scheme, rel_path=cleaned, raw_uri=uri)

    if "/" not in cleaned:
        raise ValueError(f"URI with scheme '{scheme}://' must include '<scope>/<path>': {uri}")
    scope_raw, rel_path = cleaned.split("/", 1)
    if not rel_path:
        raise ValueError(f"Empty relative path in memory URI: {uri}")
    scope = sanitize_segment(scope_raw, field_name="scope")
    return ParsedUri(scheme=scheme, scope=scope, rel_path=rel_path, raw_uri=uri)


def safe_join(root: Path, rel_path: str) -> Path:
    base = root.resolve()
    target = (base / rel_path.lstrip("/")).resolve()
    try:
        target.relative_to(base)
    except ValueError as exc:
        raise ValueError(f"Path traversal outside root '{base}': {rel_path}") from exc
    return target
