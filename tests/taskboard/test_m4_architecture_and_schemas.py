from __future__ import annotations

import ast
import configparser
import json
from pathlib import Path
from typing import Any

import pytest

from taskboard.adapters.switchboard_adapter import load_schema, validate_against_schema
from taskboard.core.errors import SchemaValidationError

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMAS_DIR = REPO_ROOT / "schemas" / "taskboard"
TASKBOARD_SRC = REPO_ROOT / "src" / "taskboard"

SCHEMA_FILES = (
    "work_opened.metadata.schema.json",
    "work_ready.metadata.schema.json",
    "execution_reported.metadata.schema.json",
    "flow_plan.schema.json",
)


def _extract_imported_modules(py_file: Path) -> list[str]:
    tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
    modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.append(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.append(node.module)
    return modules


def _validate_both(schema_name: str, payload: dict[str, Any]) -> None:
    validate_against_schema(schema_name, payload, schemas_dir=SCHEMAS_DIR)
    try:
        from jsonschema import Draft202012Validator  # type: ignore[import-untyped]
    except ImportError:
        return
    schema = load_schema(schema_name, schemas_dir=SCHEMAS_DIR)
    Draft202012Validator(schema).validate(payload)


def test_all_four_schemas_exist_and_are_valid_draft_2020_12() -> None:
    for name in SCHEMA_FILES:
        path = SCHEMAS_DIR / name
        assert path.is_file(), f"missing schema file: {path}"
        schema = json.loads(path.read_text(encoding="utf-8"))
        assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
        try:
            from jsonschema import Draft202012Validator  # type: ignore[import-untyped]
        except ImportError:
            continue
        Draft202012Validator.check_schema(schema)


def test_bidirectional_schema_contracts() -> None:
    # 1. work.opened (consumer)
    valid_opened = {
        "work_kind": "issue",
        "trace_id": "trc_01",
        "hop_count": 0,
        "initial_status": "ready",
        "assignee": "agent:coder",
        "priority": 1,
        "depends_on": ["github://org/repo/issues/1"],
    }
    _validate_both("work_opened.metadata.schema.json", valid_opened)

    with pytest.raises(SchemaValidationError):
        validate_against_schema(
            "work_opened.metadata.schema.json",
            {"work_kind": "issue", "trace_id": "trc_01", "hop_count": 9},
            schemas_dir=SCHEMAS_DIR,
        )

    # 2. work.ready (producer) — additionalProperties: false and agent:* pattern
    valid_ready = {
        "work_kind": "task",
        "state": "ready",
        "task_id": "tb_tsk_01",
        "dispatch_seq": 1,
        "assignee": "agent:role:coder",
        "task_type": "coding",
        "priority": 2,
        "trace_id": "trc_01",
        "hop_count": 1,
    }
    _validate_both("work_ready.metadata.schema.json", valid_ready)

    with pytest.raises(SchemaValidationError):
        validate_against_schema(
            "work_ready.metadata.schema.json",
            {**valid_ready, "assignee": "human:kun"},
            schemas_dir=SCHEMAS_DIR,
        )
    with pytest.raises(SchemaValidationError):
        validate_against_schema(
            "work_ready.metadata.schema.json",
            {**valid_ready, "unexpected_field": "nope"},
            schemas_dir=SCHEMAS_DIR,
        )

    # 3. execution.reported (consumer & producer)
    valid_exec_in = {
        "outcome_kind": "emit",
        "trace_id": "trc_01",
        "hop_count": 1,
        "run_id": "run_1",
        "session_id": "sess_1",
        "task_id": "tb_tsk_01",
        "task_action": "claim",
        "stage": "coding",
    }
    valid_exec_out = {
        "outcome_kind": "emit",
        "trace_id": "trc_01",
        "hop_count": 2,
        "task_id": "tb_tsk_01",
        "notify_kind": "human_todo",
        "target_assignee": "human:kun",
        "reason": "task_ready_for_human",
    }
    _validate_both("execution_reported.metadata.schema.json", valid_exec_in)
    _validate_both("execution_reported.metadata.schema.json", valid_exec_out)

    # 4. flow_plan
    valid_flow = {
        "title": "Feature Flow",
        "scope": "proj:hibernaut",
        "initial_status": "ready",
        "steps": [
            {"id": "s1", "title": "Code", "assignee": "agent:coder", "priority": 1},
            {
                "id": "s2",
                "title": "Review",
                "assignee": "human:group:tech_leads",
                "after": ["s1"],
            },
        ],
    }
    _validate_both("flow_plan.schema.json", valid_flow)

    with pytest.raises(SchemaValidationError):
        validate_against_schema(
            "flow_plan.schema.json",
            {"title": "Empty", "scope": "proj:hibernaut", "steps": []},
            schemas_dir=SCHEMAS_DIR,
        )


def test_taskboard_ast_architecture_boundaries() -> None:
    forbidden_runtime = {"agent_runtime", "runner", "hibernaut", "broker", "memory", "memory_hub"}
    for py_file in TASKBOARD_SRC.rglob("*.py"):
        for mod in _extract_imported_modules(py_file):
            top = mod.split(".")[0]
            assert top not in forbidden_runtime, f"{py_file} imported forbidden module {mod}"

    for py_file in (TASKBOARD_SRC / "core").rglob("*.py"):
        for mod in _extract_imported_modules(py_file):
            top = mod.split(".")[0]
            assert top not in {"sqlite3", "switchboard"}, f"L0 core imported {mod}"
            assert not mod.startswith(("taskboard.adapters", "taskboard.engine", "taskboard.ports"))

    # Also an import-linter layer, but lint-imports may resolve `taskboard` from another checkout
    # (shared virtualenv); this AST check always reads the files of this checkout.
    for py_file in TASKBOARD_SRC.rglob("*.py"):
        if (TASKBOARD_SRC / "app") in py_file.parents:
            continue
        for mod in _extract_imported_modules(py_file):
            assert not mod.startswith("taskboard.app"), f"{py_file} imported the composition root {mod}"


def test_import_linter_layers_include_app_on_top() -> None:
    config = configparser.ConfigParser()
    config.read(REPO_ROOT / ".importlinter", encoding="utf-8")
    contract = config["importlinter:contract:taskboard-clean-layers"]
    assert contract["type"] == "layers"
    assert contract["layers"].split() == [
        "taskboard.app",
        "taskboard.adapters",
        "taskboard.engine",
        "taskboard.ports",
        "taskboard.core",
    ]
    assert "ignore_imports" not in contract
