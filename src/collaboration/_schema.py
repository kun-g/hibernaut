from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

_TYPES: dict[str, type] = {"string": str, "boolean": bool, "integer": int, "array": list, "object": dict}


def check(schema: Mapping[str, Any], value: Any, where: str = "arguments") -> None:
    # The part of JSON Schema the tool definitions use; raises ValueError naming the offending argument.
    kind = str(schema["type"])
    if not isinstance(value, _TYPES[kind]) or (kind == "integer" and isinstance(value, bool)):
        raise ValueError(f"{where} must be a JSON {kind}")
    if "enum" in schema and value not in schema["enum"]:
        raise ValueError(f"{where} must be one of {', '.join(schema['enum'])}")
    if isinstance(value, int) and value < schema.get("minimum", value):
        raise ValueError(f"{where} must be at least {schema['minimum']}")
    if isinstance(value, str) and len(value.strip()) < schema.get("minLength", 0):
        raise ValueError(f"{where} must not be blank")
    if isinstance(value, list):
        items = cast(list[Any], value)
        if len(items) < schema.get("minItems", 0):
            raise ValueError(f"{where} must not be empty")
        for index, item in enumerate(items):
            check(schema["items"], item, f"{where}[{index}]")
    if isinstance(value, dict):
        entries = cast(dict[str, Any], value)
        properties: dict[str, Any] = schema["properties"]
        missing = [name for name in schema["required"] if name not in entries]
        unknown = sorted(set(entries) - set(properties))
        problems = [f"{word} {', '.join(names)}" for word, names in (("missing", missing), ("unknown", unknown)) if names]
        if problems:
            raise ValueError(f"{where}: {'; '.join(problems)}")
        for name, item in entries.items():
            check(properties[name], item, name if where == "arguments" else f"{where}.{name}")
