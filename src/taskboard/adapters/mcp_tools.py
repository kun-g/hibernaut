from __future__ import annotations

import json
import sys
import traceback
from collections.abc import Callable, Mapping
from dataclasses import asdict
from typing import IO, Any, Final, Literal, cast

from taskboard import __version__
from taskboard.adapters.json_schema import load_schema, validate_value
from taskboard.core.errors import (
    BlockedByDependencyError,
    CycleDetectedError,
    OptimisticLockError,
    SchemaValidationError,
    SourceRefConflictError,
    TaskboardError,
    TaskNotFoundError,
    UpstreamTaskNotFoundError,
)
from taskboard.core.models import (
    DependencyKind,
    ExpandedFlow,
    FlowPlan,
    FlowStepSpec,
    TaskDependency,
    TaskItem,
    TaskSourceLink,
    TaskStatus,
    TaskView,
)
from taskboard.engine.ingestion import IngestionService

PROTOCOL_VERSION: Final = "2024-11-05"

_ID = {"type": "string", "minLength": 1}
_TEXT = {"type": "string"}
_ASSIGNEE = {"type": "string", "pattern": "^(agent|human):[a-z0-9.:_-]+$"}
_PRIORITY = {"type": "integer", "minimum": 0, "maximum": 3}
_STATUS = {"type": "string", "enum": ["backlog", "ready", "in_progress", "done", "cancelled"]}
_VERSION = {"type": "integer", "minimum": 1}


def _object(properties: Mapping[str, Any], required: tuple[str, ...] = ()) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": dict(properties),
        "required": list(required),
        "additionalProperties": False,
    }


def _flow_plan_schema() -> dict[str, Any]:
    schema = {k: v for k, v in load_schema("flow_plan.schema.json").items() if k not in ("$schema", "title")}
    return {**schema, "additionalProperties": False}


def _tools() -> list[dict[str, Any]]:
    return [
        {
            "name": "tb_create_task",
            "description": "Create a task. initial_status='ready' dispatches it at once (work.ready for agent:, "
            "a human_todo notification for human:).",
            "inputSchema": _object(
                {
                    "scope": _ID,
                    "title": _ID,
                    "description": _TEXT,
                    "initial_status": {"type": "string", "enum": ["backlog", "ready"]},
                    "assignee": _ASSIGNEE,
                    "task_type": _TEXT,
                    "priority": _PRIORITY,
                    "source_ref": _ID,
                    "source": _ID,
                    "parent_task_id": _ID,
                    "preferred_executor": _TEXT,
                    "artifact_ref": _TEXT,
                    "reply_target": _TEXT,
                },
                ("scope", "title"),
            ),
        },
        {
            "name": "tb_update_task",
            "description": "Patch editable fields (title, description, priority, assignee, ...). Patched priority, "
            "assignee, preferred_executor, scope and task_type stop following source updates until listed "
            "in unlock_fields. expected_version makes the write fail on a newer version.",
            "inputSchema": _object(
                {
                    "task_id": _ID,
                    "patch": {"type": "object"},
                    "unlock_fields": {"type": "array", "items": _TEXT},
                    "expected_version": _VERSION,
                },
                ("task_id", "patch"),
            ),
        },
        {
            "name": "tb_transition_task",
            "description": "Move a task through backlog -> ready -> in_progress -> done (or cancelled). Terminal "
            "tasks need reopen=true. Completing a task unlocks and dispatches its downstream tasks.",
            "inputSchema": _object(
                {
                    "task_id": _ID,
                    "target_status": _STATUS,
                    "reason": _TEXT,
                    "reopen": {"type": "boolean"},
                    "expected_version": _VERSION,
                },
                ("task_id", "target_status"),
            ),
        },
        {
            "name": "tb_link_source",
            "description": "Attach another source reference (issue, chat thread, ...) to an existing task.",
            "inputSchema": _object(
                {
                    "task_id": _ID,
                    "source_ref": _ID,
                    "source": _ID,
                    "set_primary": {"type": "boolean"},
                    "last_synced_at": {"type": "integer", "minimum": 0},
                },
                ("task_id", "source_ref", "source"),
            ),
        },
        {
            "name": "tb_manage_dependency",
            "description": "Add or remove a dependency edge. 'blocks' edges hold the downstream task until the "
            "upstream is done; a cycle is rejected.",
            "inputSchema": _object(
                {
                    "op": {"type": "string", "enum": ["add", "remove"]},
                    "upstream_task_id": _ID,
                    "downstream_task_id": _ID,
                    "kind": {"type": "string", "enum": ["blocks", "related"]},
                },
                ("op", "upstream_task_id", "downstream_task_id"),
            ),
        },
        {
            "name": "tb_instantiate_flow",
            "description": "Expand a flow plan into a parent task with one child per step. A step without 'after' "
            "follows the previous step; root steps start at initial_status (default ready).",
            "inputSchema": _flow_plan_schema(),
        },
        {
            "name": "tb_list_tasks",
            "description": "List tasks with their count of incomplete blocking upstream tasks.",
            "inputSchema": _object(
                {
                    "scope": _ID,
                    "status": _STATUS,
                    "assignee_prefix": _TEXT,
                    "dispatchable_only": {"type": "boolean"},
                    "parent_task_id": _ID,
                }
            ),
        },
    ]


TOOLS: Final[list[dict[str, Any]]] = _tools()
TOOL_NAMES: Final[tuple[str, ...]] = tuple(str(tool["name"]) for tool in TOOLS)
_SCHEMAS: Final[dict[str, dict[str, Any]]] = {str(t["name"]): cast(dict[str, Any], t["inputSchema"]) for t in TOOLS}


def task_to_dict(task: TaskItem) -> dict[str, Any]:
    data = asdict(task)
    data["manual_override_fields"] = sorted(task.manual_override_fields)
    return data


def _view_to_dict(view: TaskView) -> dict[str, Any]:
    return {**task_to_dict(view.task), "incomplete_blocker_count": view.incomplete_blocker_count}


def _link_to_dict(link: TaskSourceLink) -> dict[str, Any]:
    return asdict(link)


def _edge_to_dict(edge: TaskDependency) -> dict[str, Any]:
    return asdict(edge)


def _flow_to_dict(flow: ExpandedFlow) -> dict[str, Any]:
    return {
        "parent_task_id": flow.parent_task_id,
        "tasks": [task_to_dict(t) for t in flow.tasks],
        "edges": [_edge_to_dict(e) for e in flow.edges],
    }


def _error_payload(exc: Exception) -> dict[str, Any]:
    payload: dict[str, Any] = {"error": str(exc), "error_type": type(exc).__name__}
    if isinstance(exc, CycleDetectedError):
        payload["cycle_path"] = list(exc.cycle_path)
    elif isinstance(exc, BlockedByDependencyError):
        payload["task_id"] = exc.task_id
        payload["blocking_task_ids"] = list(exc.blocking_task_ids)
    elif isinstance(exc, OptimisticLockError):
        payload["task_id"] = exc.task_id
        payload["expected_version"] = exc.expected_version
    elif isinstance(exc, SchemaValidationError):
        payload["problems"] = list(exc.problems)
    elif isinstance(exc, SourceRefConflictError):
        payload["task_id"] = exc.task_id
        payload["source_ref"] = exc.source_ref
    elif isinstance(exc, TaskNotFoundError):
        payload["task_id"] = exc.task_id
    elif isinstance(exc, UpstreamTaskNotFoundError):
        payload["upstream_id"] = exc.upstream_id
    return payload


class UnknownToolError(TaskboardError):
    pass


# JSON-RPC 2.0 MCP dispatch shared by HTTP /mcp and --mcp-stdio. Business failures become tool results with
# isError=true; anything else is a JSON-RPC internal error that does not echo the exception text.
class TaskboardMCPServer:
    def __init__(self, ingestion: IngestionService) -> None:
        self._ingestion = ingestion
        self._handlers: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
            "tb_create_task": self._create_task,
            "tb_update_task": self._update_task,
            "tb_transition_task": self._transition_task,
            "tb_link_source": self._link_source,
            "tb_manage_dependency": self._manage_dependency,
            "tb_instantiate_flow": self._instantiate_flow,
            "tb_list_tasks": self._list_tasks,
        }

    def handle_rpc(self, request: Mapping[str, Any]) -> dict[str, Any] | None:
        method = request.get("method")
        req_id = request.get("id")
        if not isinstance(method, str):
            return _error(req_id, -32600, "invalid request")
        if method.startswith("notifications/"):
            return None
        if method == "initialize":
            return _ok(
                req_id,
                {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "taskboard", "version": __version__},
                },
            )
        if method == "ping":
            return _ok(req_id, {})
        if method == "tools/list":
            return _ok(req_id, {"tools": TOOLS})
        if method == "tools/call":
            params = _mapping(request.get("params"))
            args = dict(_mapping(params.get("arguments")))
            try:
                result = self.call_tool(str(params.get("name") or ""), args)
            except (TaskboardError, ValueError, TypeError, KeyError) as exc:
                return _ok(req_id, _tool_content(_error_payload(exc), is_error=True))
            except Exception:  # noqa: BLE001
                traceback.print_exc(file=sys.stderr)
                return _error(req_id, -32603, "internal error")
            return _ok(req_id, _tool_content(result, is_error=False))
        return _error(req_id, -32601, f"unknown method {method}")

    def call_tool(self, name: str, args: dict[str, Any]) -> dict[str, Any]:
        handler = self._handlers.get(name)
        if handler is None:
            raise UnknownToolError(f"unknown tool {name!r}")
        validate_value(_SCHEMAS[name], args, f"{name} arguments")
        return handler(args)

    def _create_task(self, args: dict[str, Any]) -> dict[str, Any]:
        task = self._ingestion.tb_create_task(
            str(args["scope"]),
            str(args["title"]),
            description=str(args.get("description", "")),
            initial_status=cast(Literal["backlog", "ready"], args.get("initial_status", "backlog")),
            assignee=str(args.get("assignee", "agent:default")),
            task_type=str(args.get("task_type", "general")),
            priority=int(args.get("priority", 2)),
            source_ref=_opt(args, "source_ref"),
            parent_task_id=_opt(args, "parent_task_id"),
            preferred_executor=str(args.get("preferred_executor", "")),
            artifact_ref=_opt(args, "artifact_ref"),
            reply_target=_opt(args, "reply_target"),
            source=str(args.get("source", "mcp")),
        )
        return {"task": task_to_dict(task)}

    def _update_task(self, args: dict[str, Any]) -> dict[str, Any]:
        task = self._ingestion.tb_update_task(
            str(args["task_id"]),
            cast(Mapping[str, Any], args["patch"]),
            unlock_fields=tuple(str(f) for f in cast(list[Any], args.get("unlock_fields", []))),
            expected_version=cast(int | None, args.get("expected_version")),
        )
        return {"task": task_to_dict(task)}

    def _transition_task(self, args: dict[str, Any]) -> dict[str, Any]:
        task = self._ingestion.tb_transition_task(
            str(args["task_id"]),
            cast(TaskStatus, args["target_status"]),
            reason=str(args.get("reason", "")),
            reopen=bool(args.get("reopen", False)),
            expected_version=cast(int | None, args.get("expected_version")),
        )
        return {"task": task_to_dict(task)}

    def _link_source(self, args: dict[str, Any]) -> dict[str, Any]:
        link = self._ingestion.tb_link_source(
            str(args["task_id"]),
            str(args["source_ref"]),
            str(args["source"]),
            set_primary=bool(args.get("set_primary", False)),
            last_synced_at=int(args.get("last_synced_at", 0)),
        )
        return {"link": _link_to_dict(link)}

    def _manage_dependency(self, args: dict[str, Any]) -> dict[str, Any]:
        op = cast(Literal["add", "remove"], args["op"])
        kind = cast(DependencyKind, args.get("kind", "blocks"))
        upstream, downstream = str(args["upstream_task_id"]), str(args["downstream_task_id"])
        changed = self._ingestion.tb_manage_dependency(op, upstream, downstream, kind=kind)
        return {
            "op": op,
            "upstream_task_id": upstream,
            "downstream_task_id": downstream,
            "kind": kind,
            "changed": changed,
        }

    def _instantiate_flow(self, args: dict[str, Any]) -> dict[str, Any]:
        steps = tuple(_flow_step(cast(Mapping[str, Any], step)) for step in cast(list[Any], args["steps"]))
        plan = FlowPlan(
            title=str(args["title"]),
            scope=str(args["scope"]),
            steps=steps,
            initial_status=cast(Literal["backlog", "ready"], args.get("initial_status", "ready")),
            template_name=_opt(args, "template_name"),
        )
        return _flow_to_dict(self._ingestion.tb_instantiate_flow(plan))

    def _list_tasks(self, args: dict[str, Any]) -> dict[str, Any]:
        views = self._ingestion.tb_list_tasks(
            scope=_opt(args, "scope"),
            status=cast(TaskStatus | None, args.get("status")),
            assignee_prefix=_opt(args, "assignee_prefix"),
            dispatchable_only=bool(args.get("dispatchable_only", False)),
            parent_task_id=_opt(args, "parent_task_id"),
        )
        return {"tasks": [_view_to_dict(v) for v in views]}


def _flow_step(step: Mapping[str, Any]) -> FlowStepSpec:
    after = step.get("after")
    return FlowStepSpec(
        id=str(step["id"]),
        title=str(step["title"]),
        assignee=str(step.get("assignee", "agent:default")),
        task_type=str(step.get("task_type", "general")),
        priority=int(step.get("priority", 2)),
        description=str(step.get("description", "")),
        after=None if after is None else tuple(str(a) for a in cast(list[Any], after)),
    )


def _mapping(value: Any) -> Mapping[str, Any]:
    return cast(Mapping[str, Any], value) if isinstance(value, Mapping) else {}


def _opt(args: Mapping[str, Any], key: str) -> str | None:
    value = args.get(key)
    return None if value is None else str(value)


def _ok(req_id: Any, result: Mapping[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "result": dict(result)}


def _error(req_id: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}


def _tool_content(payload: Mapping[str, Any], *, is_error: bool) -> dict[str, Any]:
    return {
        "content": [{"type": "text", "text": json.dumps(payload, ensure_ascii=False)}],
        "structuredContent": dict(payload),
        "isError": is_error,
    }


# One JSON-RPC message per line in each direction; logs go to stderr so stdout stays protocol-only.
def serve_stdio(server: TaskboardMCPServer, stdin: IO[str], stdout: IO[str]) -> None:
    for line in stdin:
        if not line.strip():
            continue
        try:
            request = json.loads(line)
        except json.JSONDecodeError:
            reply: dict[str, Any] | None = _error(None, -32700, "parse error")
        else:
            if isinstance(request, dict):
                reply = server.handle_rpc(cast(dict[str, Any], request))
            else:
                reply = _error(None, -32600, "invalid request")
        if reply is not None:
            stdout.write(json.dumps(reply, ensure_ascii=False) + "\n")
            stdout.flush()
