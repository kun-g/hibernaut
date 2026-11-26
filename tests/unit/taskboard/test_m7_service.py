from __future__ import annotations

import io
import json
import socket
import urllib.error
import urllib.request
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import pytest

from switchboard.envelope import CanonicalEvent, validate_envelope
from switchboard.http_client import HttpResponse
from switchboard.ports import ConsumerHandler, IngestResult
from taskboard.adapters.mcp_tools import TOOL_NAMES, TaskboardMCPServer, serve_stdio
from taskboard.adapters.remote_bus import RemotePublishRejectedError, SwitchboardRemoteBus
from taskboard.app.config import ConfigError, TaskboardConfig, load_config, parse_config, service_secrets
from taskboard.app.main import TaskboardService, build_service
from tests.support.switchboard_support import FakeHttpClient, StaticCredentials
from tests.support.m7_service import REPO, SCOPE


TOKEN = "test-taskboard-mcp-token"


def test_config_defaults_and_base_path_normalization(tmp_path: Path) -> None:
    config = parse_config({}, base_dir=tmp_path)
    assert (config.http.host, config.http.port, config.http.base_path, config.http.max_body_bytes) == (
        "127.0.0.1",
        8791,
        "",
        1_048_576,
    )
    assert (config.sqlite.path, config.sqlite.busy_timeout_ms) == ("taskboard.db", 5000)
    assert (config.switchboard.relay_url, config.switchboard.mcp_url) == (None, None)
    assert (config.switchboard.consumer_id, config.switchboard.ca_file) == ("taskboard", None)
    assert config.intake.auto_ready_sources == ("github",)

    for raw, expected in (
        ("/taskboard", "/taskboard"),
        ("/taskboard/", "/taskboard"),
        ("taskboard", "/taskboard"),
        (" /tb/v1// ", "/tb/v1"),
        ("/", ""),
        ("", ""),
    ):
        assert parse_config({"http": {"base_path": raw}}, base_dir=tmp_path).http.base_path == expected, raw
    for bad in ("/task board", "/taskboard?x=1", "/a#b", "/a//b"):
        with pytest.raises(ConfigError, match="base_path"):
            parse_config({"http": {"base_path": bad}}, base_dir=tmp_path)


def test_config_validates_urls_numbers_and_sources(tmp_path: Path) -> None:
    config = parse_config(
        {
            "switchboard": {
                "relay_url": "wss://sb.example.test/ws/relay",
                "mcp_url": "https://sb.example.test/mcp",
                "consumer_id": "taskboard-b",
                "ca_file": "certs/ca.pem",
            },
            "intake": {"auto_ready_sources": ["github", "feishu"]},
        },
        base_dir=tmp_path,
    )
    assert config.switchboard.ca_file == str(tmp_path / "certs" / "ca.pem")
    assert config.switchboard.consumer_id == "taskboard-b"
    assert config.intake.auto_ready_sources == ("github", "feishu")
    assert parse_config({"intake": {"auto_ready_sources": []}}, base_dir=tmp_path).intake.auto_ready_sources == ()

    bad_sections: list[tuple[dict[str, Any], str]] = [
        ({"switchboard": {"relay_url": "https://sb/ws/relay"}}, "relay_url"),
        ({"switchboard": {"relay_url": "ws://"}}, "relay_url"),
        ({"switchboard": {"mcp_url": "ws://sb/mcp"}}, "mcp_url"),
        ({"switchboard": {"consumer_id": ""}}, "consumer_id"),
        ({"http": {"port": 70000}}, "port"),
        ({"http": {"port": "8791"}}, "port"),
        ({"http": {"max_body_bytes": 0}}, "max_body_bytes"),
        ({"sqlite": {"busy_timeout_ms": -1}}, "busy_timeout_ms"),
        ({"intake": {"auto_ready_sources": "github"}}, "auto_ready_sources"),
        ({"intake": {"auto_ready_sources": [""]}}, "auto_ready_sources"),
        ({"http": "nope"}, "[http]"),
    ]
    for raw, needle in bad_sections:
        with pytest.raises(ConfigError, match=needle.replace("[", r"\[").replace("]", r"\]")):
            parse_config(raw, base_dir=tmp_path)


def test_service_mode_needs_switchboard_urls_and_tokens(tmp_path: Path) -> None:
    remote = parse_config(
        {"switchboard": {"relay_url": "ws://127.0.0.1:1/ws/relay", "mcp_url": "http://127.0.0.1:1/mcp"}},
        base_dir=tmp_path,
    )
    full_env = {
        "SWITCHBOARD_RELAY_TOKEN": "r",
        "SWITCHBOARD_MCP_TOKEN": "m",
        "TASKBOARD_MCP_TOKEN": "t",
    }
    secrets = service_secrets(remote, full_env, stdio=False)
    assert secrets.mcp_token == "t"
    assert (secrets.credentials.issue_ephemeral("relay"), secrets.credentials.issue_ephemeral("mcp")) == ("r", "m")

    for missing in ("SWITCHBOARD_RELAY_TOKEN", "SWITCHBOARD_MCP_TOKEN"):
        env = {k: v for k, v in full_env.items() if k != missing}
        with pytest.raises(ConfigError, match=missing):
            service_secrets(remote, env, stdio=False)
    # Without TASKBOARD_MCP_TOKEN the service starts, but HTTP /mcp rejects every request.
    assert service_secrets(remote, {k: v for k, v in full_env.items() if k != "TASKBOARD_MCP_TOKEN"}, stdio=False).mcp_token is None

    local = parse_config({}, base_dir=tmp_path)
    with pytest.raises(ConfigError, match="relay_url"):
        service_secrets(local, full_env, stdio=False)
    with pytest.raises(ConfigError, match="mcp_url"):
        service_secrets(replace(local, switchboard=replace(remote.switchboard, mcp_url=None)), full_env, stdio=False)

    # stdio mode never connects the relay; the MCP uplink is optional.
    assert service_secrets(local, {}, stdio=True).credentials.issue_ephemeral("relay") is None
    assert service_secrets(remote, {"SWITCHBOARD_MCP_TOKEN": "m"}, stdio=True).credentials.issue_ephemeral("mcp") == "m"


def test_example_config_parses_and_holds_no_secret() -> None:
    path = REPO / "taskboard.example.toml"
    config = load_config(path)
    assert config.http.port == 8791
    assert config.switchboard.relay_url and config.switchboard.mcp_url
    assert config.intake.auto_ready_sources == ("github",)
    text = path.read_text(encoding="utf-8")
    for name in ("SWITCHBOARD_RELAY_TOKEN", "SWITCHBOARD_MCP_TOKEN", "TASKBOARD_MCP_TOKEN"):
        assert name in text
        assert f"{name} =" not in text and f"{name}=" not in text


def _rpc(structured: Mapping[str, Any], *, is_error: bool = False) -> HttpResponse:
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "result": {"content": [], "structuredContent": dict(structured), "isError": is_error},
    }
    return HttpResponse(200, json.dumps(body).encode("utf-8"))


def _ready_event() -> CanonicalEvent:
    return CanonicalEvent(
        source="taskboard",
        event_type="work.ready",
        external_id="tb:t:d1",
        scope=SCOPE,
        correlation_key="t",
        subject="s",
        metadata={"work_kind": "task", "state": "ready", "task_id": "t"},
    )


def test_remote_bus_publishes_over_mcp_and_raises_on_invalid() -> None:
    http = FakeHttpClient()
    credentials = StaticCredentials()
    credentials.tokens["mcp"] = "m"
    bus = SwitchboardRemoteBus(mcp_url="http://sb.test/mcp", credentials=credentials, http_client=http)
    http.responses.append(_rpc({"status": "accepted", "event_id": "evt_1", "consumers": ["hibernaut"]}))
    assert bus.publish(_ready_event()).status == "accepted"
    assert http.calls[0]["json"]["params"]["name"] == "submit_work"

    http.responses.append(_rpc({"status": "duplicate", "event_id": "evt_1"}))
    assert bus.publish(_ready_event()).status == "duplicate"

    http.responses.append(_rpc({"status": "invalid", "problems": ["work.ready requires subject"]}, is_error=True))
    with pytest.raises(RemotePublishRejectedError, match="requires subject"):
        bus.publish(_ready_event())

    with pytest.raises(RuntimeError, match="no relay_url"):
        bus.subscribe("taskboard", lambda event: "acked")
    relay_bus = SwitchboardRemoteBus(
        mcp_url="http://sb.test/mcp", relay_url="ws://127.0.0.1:9/ws/relay", consumer_id="taskboard",
        credentials=credentials, http_client=http,
    )
    with pytest.raises(ValueError, match="'taskboard'"):
        relay_bus.subscribe("other", lambda event: "acked")
    relay_bus.stop()


@dataclass
class _Bus:
    published: list[CanonicalEvent] = field(default_factory=lambda: [])
    subscribed: list[str] = field(default_factory=lambda: [])

    def publish(self, event: CanonicalEvent) -> IngestResult:
        validate_envelope(event)
        self.published.append(event)
        return IngestResult(status="accepted", event_id=event.event_id)

    def subscribe(self, consumer_id: str, handler: ConsumerHandler) -> None:
        del handler
        self.subscribed.append(consumer_id)


@dataclass
class _Running:
    service: TaskboardService
    bus: _Bus

    @property
    def url(self) -> str:
        return self.service.http.base_url


def _config(tmp_path: Path, **http: Any) -> TaskboardConfig:
    tmp_path.mkdir(parents=True, exist_ok=True)
    return parse_config(
        {"http": {"port": 0, **http}, "sqlite": {"path": str(tmp_path / "taskboard.db")}}, base_dir=tmp_path
    )


@pytest.fixture
def running(tmp_path: Path) -> Iterator[_Running]:
    bus = _Bus()
    service = build_service(_config(tmp_path, base_path="/taskboard/"), mcp_token=TOKEN, bus=bus)
    service.start()
    try:
        yield _Running(service, bus)
    finally:
        service.stop()


def _request(
    url: str, *, body: bytes | None = None, headers: Mapping[str, str] | None = None, method: str | None = None
) -> tuple[int, Any]:
    req = urllib.request.Request(url, data=body, method=method, headers=dict(headers or {}))
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        return exc.code, json.loads(raw) if raw else None


def _rpc_post(url: str, payload: Mapping[str, Any], token: str | None = TOKEN) -> tuple[int, Any]:
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    return _request(url, body=json.dumps(payload).encode("utf-8"), headers=headers, method="POST")


def _call(url: str, tool: str, arguments: Mapping[str, Any]) -> tuple[bool, dict[str, Any]]:
    status, response = _rpc_post(
        url, {"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": {"name": tool, "arguments": dict(arguments)}}
    )
    assert status == 200, response
    result = response["result"]
    assert json.loads(result["content"][0]["text"]) == result["structuredContent"]
    return bool(result["isError"]), result["structuredContent"]


def test_healthz_answers_with_and_without_the_base_path(running: _Running, tmp_path: Path) -> None:
    assert running.service.config.http.base_path == "/taskboard"
    for path in ("/healthz", "/taskboard/healthz", "/taskboard/healthz?probe=1"):
        assert _request(running.url + path) == (200, {"status": "ok"}), path
    assert _request(running.url + "/other/healthz")[0] == 404
    assert running.bus.subscribed == ["taskboard"]

    bare = build_service(_config(tmp_path / "bare"), mcp_token=TOKEN, bus=_Bus())
    bare.start()
    try:
        assert _request(bare.http.base_url + "/healthz") == (200, {"status": "ok"})
        assert _request(bare.http.base_url + "/taskboard/healthz")[0] == 404
    finally:
        bare.stop()


def test_mcp_bearer_auth_on_both_paths(running: _Running, tmp_path: Path) -> None:
    ping = {"jsonrpc": "2.0", "id": 1, "method": "ping"}
    for path in ("/mcp", "/taskboard/mcp"):
        assert _rpc_post(running.url + path, ping) == (200, {"jsonrpc": "2.0", "id": 1, "result": {}}), path
        assert _rpc_post(running.url + path, ping, token=None)[0] == 401
        assert _rpc_post(running.url + path, ping, token="wrong")[0] == 401
        assert _request(running.url + path, body=b"x", headers={"Authorization": "Basic abc"}, method="POST")[0] == 401
    assert _rpc_post(running.url + "/elsewhere/mcp", ping)[0] == 404

    # No TASKBOARD_MCP_TOKEN: fail closed on every request.
    closed = build_service(_config(tmp_path / "closed"), mcp_token=None, bus=_Bus())
    closed.start()
    try:
        assert _rpc_post(closed.http.base_url + "/mcp", ping)[0] == 401
        assert _rpc_post(closed.http.base_url + "/mcp", ping, token="")[0] == 401
    finally:
        closed.stop()


def test_mcp_transport_errors(running: _Running) -> None:
    url = running.url + "/taskboard/mcp"
    auth = {"Authorization": f"Bearer {TOKEN}"}
    assert _request(url, body=b"{not json", headers=auth, method="POST")[0] == 400
    assert _request(url, body=b"[1, 2]", headers=auth, method="POST")[0] == 400
    # Only the headers are sent: the server must refuse by Content-Length before reading the body.
    assert running.service.http is not None
    with socket.create_connection(("127.0.0.1", running.service.http.port), timeout=5) as sock:
        sock.sendall(
            f"POST /taskboard/mcp HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {TOKEN}\r\n"
            f"Content-Length: {running.service.config.http.max_body_bytes + 1}\r\n\r\n".encode()
        )
        status_line = sock.recv(64).split(b"\r\n", 1)[0]
    assert b" 413 " in status_line
    assert _rpc_post(url, {"jsonrpc": "2.0", "method": "notifications/initialized"}) == (202, None)
    status, unknown = _rpc_post(url, {"jsonrpc": "2.0", "id": 3, "method": "resources/list"})
    assert status == 200 and unknown["error"]["code"] == -32601


def test_initialize_and_list_all_seven_tools(running: _Running) -> None:
    url = running.url + "/taskboard/mcp"
    _, init = _rpc_post(url, {"jsonrpc": "2.0", "id": 1, "method": "initialize"})
    assert init["result"]["serverInfo"]["name"] == "taskboard"
    assert init["result"]["capabilities"] == {"tools": {}}
    _, listed = _rpc_post(url, {"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    tools = listed["result"]["tools"]
    assert [t["name"] for t in tools] == list(TOOL_NAMES) == [
        "tb_create_task",
        "tb_update_task",
        "tb_transition_task",
        "tb_link_source",
        "tb_manage_dependency",
        "tb_instantiate_flow",
        "tb_list_tasks",
    ]
    for tool in tools:
        assert tool["description"] and tool["inputSchema"]["type"] == "object", tool["name"]


def test_tools_run_the_task_lifecycle(running: _Running) -> None:
    url = running.url + "/taskboard/mcp"
    err, created = _call(url, "tb_create_task", {"scope": SCOPE, "title": "A", "initial_status": "ready", "priority": 1})
    assert not err
    a = created["task"]
    assert (a["status"], a["dispatch_seq"], a["priority"], a["manual_override_fields"]) == ("ready", 1, 1, [])
    assert [e.event_type for e in running.bus.published] == ["work.ready"]

    _, b_created = _call(url, "tb_create_task", {"scope": SCOPE, "title": "B", "assignee": "agent:sde"})
    b = b_created["task"]
    assert b["status"] == "backlog"

    err, dep = _call(url, "tb_manage_dependency", {"op": "add", "upstream_task_id": a["task_id"], "downstream_task_id": b["task_id"]})
    assert not err and dep == {
        "op": "add",
        "upstream_task_id": a["task_id"],
        "downstream_task_id": b["task_id"],
        "kind": "blocks",
        "changed": True,
    }

    _, listed = _call(url, "tb_list_tasks", {"scope": SCOPE})
    by_title = {t["title"]: t for t in listed["tasks"]}
    assert (by_title["A"]["incomplete_blocker_count"], by_title["B"]["incomplete_blocker_count"]) == (0, 1)
    _, dispatchable = _call(url, "tb_list_tasks", {"dispatchable_only": True})
    assert [t["title"] for t in dispatchable["tasks"]] == ["A"]

    err, updated = _call(url, "tb_update_task", {"task_id": b["task_id"], "patch": {"priority": 0}, "expected_version": b["version"]})
    assert not err and updated["task"]["priority"] == 0 and updated["task"]["manual_override_fields"] == ["priority"]

    err, linked = _call(url, "tb_link_source", {"task_id": b["task_id"], "source_ref": "github:kun-g/hibernaut#5", "source": "github"})
    assert not err and linked["link"]["source_ref"] == "github:kun-g/hibernaut#5" and linked["link"]["is_primary"] is False

    _call(url, "tb_transition_task", {"task_id": a["task_id"], "target_status": "in_progress"})
    err, done = _call(url, "tb_transition_task", {"task_id": a["task_id"], "target_status": "done", "reason": "shipped"})
    assert not err and done["task"]["status"] == "done"
    [_, ready_b] = [e for e in running.bus.published if e.event_type == "work.ready"]
    assert ready_b.metadata["task_id"] == b["task_id"]
    _, after = _call(url, "tb_list_tasks", {"status": "ready"})
    assert [t["title"] for t in after["tasks"]] == ["B"]

    err, flow = _call(
        url,
        "tb_instantiate_flow",
        {
            "title": "Release",
            "scope": SCOPE,
            "steps": [{"id": "build", "title": "Build"}, {"id": "verify", "title": "Verify", "assignee": "human:kun"}],
        },
    )
    assert not err and len(flow["tasks"]) == 2 and len(flow["edges"]) == 1
    assert flow["edges"][0]["kind"] == "blocks"
    _, children = _call(url, "tb_list_tasks", {"parent_task_id": flow["parent_task_id"]})
    assert sorted(t["title"] for t in children["tasks"]) == ["Build", "Verify"]

    err, removed = _call(url, "tb_manage_dependency", {"op": "remove", "upstream_task_id": a["task_id"], "downstream_task_id": b["task_id"]})
    assert not err and removed["changed"] is True


def test_tool_errors_are_structured(running: _Running) -> None:
    url = running.url + "/taskboard/mcp"
    _, a = _call(url, "tb_create_task", {"scope": SCOPE, "title": "A", "source_ref": "github:kun-g/hibernaut#1"})
    _, b = _call(url, "tb_create_task", {"scope": SCOPE, "title": "B"})
    a_id, b_id = a["task"]["task_id"], b["task"]["task_id"]
    _call(url, "tb_manage_dependency", {"op": "add", "upstream_task_id": a_id, "downstream_task_id": b_id})

    err, cycle = _call(url, "tb_manage_dependency", {"op": "add", "upstream_task_id": b_id, "downstream_task_id": a_id})
    assert err and cycle["error_type"] == "CycleDetectedError"
    assert cycle["cycle_path"][0] == cycle["cycle_path"][-1]

    err, blocked = _call(url, "tb_transition_task", {"task_id": b_id, "target_status": "ready"})
    assert err and blocked["error_type"] == "BlockedByDependencyError"
    assert blocked["blocking_task_ids"] == [a_id]

    err, stale = _call(url, "tb_update_task", {"task_id": b_id, "patch": {"title": "B2"}, "expected_version": 99})
    assert err and stale["error_type"] == "OptimisticLockError"
    assert (stale["task_id"], stale["expected_version"]) == (b_id, 99)
    err, stale_move = _call(url, "tb_transition_task", {"task_id": a_id, "target_status": "ready", "expected_version": 99})
    assert err and stale_move["error_type"] == "OptimisticLockError"

    err, missing = _call(url, "tb_transition_task", {"task_id": "tb_tsk_nope", "target_status": "ready"})
    assert err and missing["error_type"] == "TaskNotFoundError"

    err, illegal = _call(url, "tb_transition_task", {"task_id": a_id, "target_status": "done"})
    assert err and illegal["error_type"] == "IllegalTransitionError"

    err, dup = _call(url, "tb_create_task", {"scope": SCOPE, "title": "again", "source_ref": "github:kun-g/hibernaut#1"})
    assert err and dup["error_type"] == "SourceRefConflictError" and dup["task_id"] == a_id

    err, layer3 = _call(url, "tb_update_task", {"task_id": a_id, "patch": {"status": "done"}})
    assert err and layer3["error_type"] == "ValueError"

    err, bad_args = _call(url, "tb_create_task", {"scope": SCOPE, "title": "x", "priority": 9, "colour": "red"})
    assert err and bad_args["error_type"] == "SchemaValidationError"
    assert any("priority" in p for p in bad_args["problems"]) and any("colour" in p for p in bad_args["problems"])
    err, missing_args = _call(url, "tb_link_source", {"task_id": a_id})
    assert err and any("source_ref" in p for p in missing_args["problems"])
    err, bad_flow = _call(url, "tb_instantiate_flow", {"title": "F", "scope": SCOPE, "steps": []})
    assert err and bad_flow["error_type"] == "SchemaValidationError"
    # A misspelt step field must not fall back to a default (agent:default would dispatch agent work).
    err, typo = _call(
        url,
        "tb_instantiate_flow",
        {"title": "F", "scope": SCOPE, "steps": [{"id": "s1", "title": "Review", "asignnee": "human:kun"}]},
    )
    assert err and typo["error_type"] == "SchemaValidationError"
    assert any("steps[0].asignnee" in p for p in typo["problems"]), typo["problems"]
    _, everything = _call(url, "tb_list_tasks", {})
    assert all(t["title"] != "Review" for t in everything["tasks"])
    err, unknown = _call(url, "tb_delete_everything", {})
    assert err and "unknown tool" in unknown["error"]


def test_stdio_dispatch_in_process(tmp_path: Path) -> None:
    store_service = build_service(_config(tmp_path), mcp_token=None, bus=_Bus())
    try:
        lines = [
            json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize"}),
            "",
            json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}),
            "{broken",
            json.dumps({"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "tb_list_tasks", "arguments": {}}}),
        ]
        out = io.StringIO()
        serve_stdio(store_service.mcp, io.StringIO("\n".join(lines) + "\n"), out)
        replies = [json.loads(line) for line in out.getvalue().splitlines()]
        assert [r.get("id") for r in replies] == [1, None, 2]
        assert replies[1]["error"]["code"] == -32700
        assert replies[2]["result"]["structuredContent"] == {"tasks": []}
    finally:
        store_service.stop()


def test_mcp_server_survives_unexpected_errors(tmp_path: Path) -> None:
    class Exploding:
        def __getattr__(self, name: str) -> Any:
            def boom(*args: Any, **kwargs: Any) -> Any:
                raise RuntimeError("disk on fire")

            return boom

    server = TaskboardMCPServer(Exploding())  # type: ignore[arg-type]
    reply = server.handle_rpc({"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": {"name": "tb_list_tasks"}})
    assert reply is not None and reply["error"]["code"] == -32603
    assert "disk on fire" not in json.dumps(reply)
