from __future__ import annotations

import json
import re
from collections.abc import Mapping
from functools import lru_cache
from pathlib import Path
from typing import Any, cast

from taskboard.core.errors import SchemaValidationError

_DEFAULT_SCHEMAS_DIR = Path(__file__).resolve().parents[3] / "schemas" / "taskboard"


@lru_cache(maxsize=16)
def load_schema(schema_name: str, schemas_dir: Path | None = None) -> dict[str, Any]:
    root = schemas_dir or _DEFAULT_SCHEMAS_DIR
    path = root / schema_name
    return cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))


def _validate_node(
    value: Any,
    schema: Mapping[str, Any],
    path: str,
    problems: list[str],
) -> None:
    expected_type = schema.get("type")
    if expected_type == "object":
        if not isinstance(value, Mapping):
            problems.append(f"{path}: expected object, got {type(value).__name__}")
            return
        mapping_value = cast(Mapping[str, Any], value)
        required = schema.get("required") or ()
        for req_key in required:
            if req_key not in mapping_value or mapping_value[req_key] is None:
                problems.append(f"{path}.{req_key}: required property missing")
        props = cast(Mapping[str, Mapping[str, Any]], schema.get("properties") or {})
        if schema.get("additionalProperties") is False:
            extra = sorted(set(mapping_value.keys()) - set(props.keys()))
            for key in extra:
                problems.append(f"{path}.{key}: additional property not allowed")
        for key, item_val in mapping_value.items():
            if key in props:
                _validate_node(item_val, props[key], f"{path}.{key}", problems)
        return

    if expected_type == "string":
        if not isinstance(value, str):
            problems.append(f"{path}: expected string, got {type(value).__name__}")
            return
        if "minLength" in schema and len(value) < int(schema["minLength"]):
            problems.append(f"{path}: length {len(value)} < minLength {schema['minLength']}")
        if "enum" in schema and value not in schema["enum"]:
            problems.append(f"{path}: {value!r} not in enum {schema['enum']}")
        if "pattern" in schema and not re.match(str(schema["pattern"]), value):
            problems.append(f"{path}: {value!r} does not match pattern {schema['pattern']!r}")
        return

    if expected_type == "integer":
        # bool is a subclass of int in Python; JSON Schema integer excludes booleans.
        if isinstance(value, bool) or not isinstance(value, int):
            problems.append(f"{path}: expected integer, got {type(value).__name__}")
            return
        if "minimum" in schema and value < int(schema["minimum"]):
            problems.append(f"{path}: {value} < minimum {schema['minimum']}")
        if "maximum" in schema and value > int(schema["maximum"]):
            problems.append(f"{path}: {value} > maximum {schema['maximum']}")
        return

    if expected_type == "boolean":
        if not isinstance(value, bool):
            problems.append(f"{path}: expected boolean, got {type(value).__name__}")
        return

    if expected_type == "array":
        if not isinstance(value, (list, tuple)):
            problems.append(f"{path}: expected array, got {type(value).__name__}")
            return
        seq_val = cast(list[Any] | tuple[Any, ...], value)
        if "minItems" in schema and len(seq_val) < int(schema["minItems"]):
            problems.append(f"{path}: length {len(seq_val)} < minItems {schema['minItems']}")
        item_schema = schema.get("items")
        if isinstance(item_schema, Mapping):
            for idx, elem in enumerate(seq_val):
                _validate_node(elem, cast(Mapping[str, Any], item_schema), f"{path}[{idx}]", problems)


def validate_against_schema(
    schema_name: str,
    payload: Mapping[str, Any],
    *,
    schemas_dir: Path | None = None,
) -> None:
    validate_value(load_schema(schema_name, schemas_dir), payload, schema_name)


def validate_value(schema: Mapping[str, Any], value: Any, name: str) -> None:
    problems: list[str] = []
    _validate_node(value, schema, "$", problems)
    if problems:
        raise SchemaValidationError(name, problems)
