from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast


class SchemaValidationError(ValueError):
    pass


def _check_type(value: Any, expected: str) -> bool:
    if expected == "object":
        return isinstance(value, dict)
    if expected == "array":
        return isinstance(value, list)
    if expected == "string":
        return isinstance(value, str)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return (isinstance(value, (int, float))) and not isinstance(value, bool)
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "null":
        return value is None
    return True


def _resolve_ref(ref: str, root_schema: dict[str, Any]) -> dict[str, Any]:
    if not ref.startswith("#/"):
        raise SchemaValidationError(f"Unsupported $ref: {ref}")
    node: Any = root_schema
    for part in ref[2:].split("/"):
        node = node[part]
    return node


def _validate(instance: Any, schema: dict[str, Any], root: dict[str, Any], path: str) -> None:
    if "$ref" in schema:
        schema = _resolve_ref(schema["$ref"], root)

    if "const" in schema and instance != schema["const"]:
        raise SchemaValidationError(f"{path}: expected const {schema['const']!r}, got {instance!r}")

    if "enum" in schema and instance not in schema["enum"]:
        raise SchemaValidationError(f"{path}: {instance!r} not in enum {schema['enum']!r}")

    expected_type = schema.get("type")
    if isinstance(expected_type, str):
        if not _check_type(instance, expected_type):
            raise SchemaValidationError(f"{path}: expected {expected_type}, got {type(instance).__name__}")
    elif isinstance(expected_type, list):
        if not any(_check_type(instance, t) for t in cast(list[str], expected_type)):
            raise SchemaValidationError(f"{path}: expected one of {expected_type}, got {type(instance).__name__}")

    if isinstance(instance, str):
        if "minLength" in schema and len(instance) < schema["minLength"]:
            raise SchemaValidationError(f"{path}: string shorter than minLength {schema['minLength']}")
        if "maxLength" in schema and len(instance) > schema["maxLength"]:
            raise SchemaValidationError(f"{path}: string longer than maxLength {schema['maxLength']}")

    if isinstance(instance, (int, float)) and not isinstance(instance, bool):
        if "minimum" in schema and instance < schema["minimum"]:
            raise SchemaValidationError(f"{path}: {instance} < minimum {schema['minimum']}")

    if isinstance(instance, dict):
        fields = cast(dict[str, Any], instance)
        for req in schema.get("required", ()):
            if req not in fields:
                raise SchemaValidationError(f"{path}: missing required field '{req}'")
        props = schema.get("properties", {})
        if schema.get("additionalProperties") is False:
            extra = set(fields.keys()) - set(props.keys())
            if extra:
                raise SchemaValidationError(f"{path}: additionalProperties not allowed: {sorted(extra)}")
        for k, v in fields.items():
            if k in props:
                _validate(v, props[k], root, f"{path}.{k}")

    if isinstance(instance, list) and "items" in schema:
        item_schema = schema["items"]
        for idx, elem in enumerate(cast(list[Any], instance)):
            _validate(elem, item_schema, root, f"{path}[{idx}]")


def validate_against_schema(instance: dict[str, Any], schema_path: str | Path) -> None:
    schema = json.loads(Path(schema_path).read_text(encoding="utf-8"))
    _validate(instance, schema, schema, "$")
