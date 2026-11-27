from __future__ import annotations

import asyncio
import configparser
import contextlib
import io
import json
import os
import socket
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from datetime import datetime, timedelta, UTC
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest

from agent_runtime.adapters.executor_cli.executor import LocalCLIExecutor
from agent_runtime.adapters.executor_whitebox import WhiteboxExecutor
from agent_runtime.adapters.executor_whitebox.executor import LLMReply, ToolCallRequest
from agent_runtime.adapters.ledger_bridge import RunLedger
from agent_runtime.adapters.memory_bridge import MemoryControlPlane, MemoryGarbageCollector
from agent_runtime.adapters.memory_fs import (
    GovernedMemoryHub,
    PartitionedMemoryAdapter,
    WorkspacePartitionRegistry,
    baseline_segments,
)
from agent_runtime.adapters.permissions import AutoModePermissionAdapter
from agent_runtime.app.bootstrap import open_memory_http
from agent_runtime.app.config import load_config, parse_config
from agent_runtime.app.main import build_daemon, build_daemon_from_config, serve_configured
from agent_runtime.core.models import Emit, Event, SessionRef, Suspend
from ledger import RunRecord
from memory_hub import BaselineBudgetConfig, LegacyMemoryPortAdapter, MemoryHubConfig, SearchQuery, WriteProposal
from tests.support.fakes import InMemoryMemoryStore
from tests.support.memory_hub_fixtures import fast_memory_http_shutdown  # noqa: F401 -- pytest fixture, requested by parameter name

ROOT = Path(__file__).resolve().parents[2]
PROJ = "proj:hibernaut"
ISSUE = "hibernaut:issue-42"


@pytest.fixture
def hub(tmp_path: Path) -> Iterator[GovernedMemoryHub]:
    opened = GovernedMemoryHub(MemoryHubConfig(memory_root=str(tmp_path / "memory")))
    yield opened
    opened.close()


@pytest.fixture
def registry(hub: GovernedMemoryHub) -> WorkspacePartitionRegistry:
    return WorkspacePartitionRegistry(hub.root, hub=hub)


class Caps:
    def __init__(self, memory: Any, permissions: Any | None = None) -> None:
        self.memory = memory
        self.timers = None
        self.permissions = permissions or AutoModePermissionAdapter()

    def poll_folded_events(self) -> list[Event]:
        return []

    def is_interrupted(self) -> bool:
        return False


def event(payload: dict[str, Any], source: str = "test") -> Event:
    return Event(inbox_id=PROJ, source=source, external_id=f"e{time.monotonic_ns()}", payload=payload)


# --- 1. package registration and dependency contracts -----------------------------------------------------------


def test_import_contracts_isolate_memory_hub_both_ways() -> None:
    parser = configparser.ConfigParser()
    parser.read(ROOT / ".importlinter")

    def names(section: str, key: str) -> set[str]:
        return set(parser[section][key].split())

    assert "memory_hub" in names("importlinter", "root_packages")
    own = "importlinter:contract:memory-hub-zero-business-deps"
    assert names(own, "source_modules") == {"memory_hub"}
    assert {"agent_runtime", "switchboard", "taskboard", "runner", "ledger"} <= names(own, "forbidden_modules")
    reverse = "importlinter:contract:business-packages-no-memory-hub"
    assert {"switchboard", "runner", "taskboard"} <= names(reverse, "source_modules")
    assert names(reverse, "forbidden_modules") == {"memory_hub"}


# --- 2. memory_fs delegates to Memory Hub -----------------------------------------------------------------------


def test_inboxes_of_one_project_share_its_memory_and_projects_stay_apart(registry: WorkspacePartitionRegistry) -> None:
    proj, issue, other = (registry.memory_for(i) for i in (PROJ, ISSUE, "proj:web-app"))
    proj.write("progress/task.md", "shared progress")
    assert issue.read_on_demand("progress/task.md") == "shared progress"
    with pytest.raises(FileNotFoundError):
        other.read_on_demand("progress/task.md")
    assert registry.binding_for(PROJ).memory_dir == registry.binding_for(ISSUE).memory_dir
    assert registry.binding_for("proj:web-app").memory_dir != registry.binding_for(PROJ).memory_dir


def test_inboxes_of_no_project_have_memory_of_their_own(registry: WorkspacePartitionRegistry) -> None:
    ids = ("sys:hb-ops", "sys:backup", "user:main", "proj:sys", "sys")
    for inbox_id in ids:
        registry.memory_for(inbox_id).write("notes/mine.md", inbox_id)
    assert [registry.memory_for(i).read_on_demand("notes/mine.md") for i in ids[:4]] == [
        "sys:hb-ops", "sys:backup", "user:main", "sys"]
    assert registry.binding_for("sys:hb-ops").memory_dir.name == "sys__hb-ops"


def _bound_config(tmp_path: Path, *patterns: str, **sections: Any) -> dict[str, Any]:
    return {
        "runtime": {
            "db_path": str(tmp_path / "hb.db"), "memory_root": str(tmp_path / "memory"), "default_agent": "cli",
            "inbox_bindings": [
                {"pattern": p, "default_agent": "cli", "agents": ["cli"], "workdir": str(tmp_path / f"work{n}")}
                for n, p in enumerate(patterns)
            ],
        },
        "executors": {"cli": {"command": "true"}},
        **sections,
    }


def test_a_workdir_binding_is_a_memory_scope_of_its_own_whatever_its_name(tmp_path: Path) -> None:
    cfg = parse_config(_bound_config(tmp_path, "ops:a", "ops:b", "sys:mapped", memory_hub={
        "inbox_projects": {"sys:mapped": "ops"}}))
    daemon = build_daemon_from_config(cfg)
    try:
        keys = {i: daemon.partitions.memory_for(i).project_key for i in ("ops:a", "ops:b", "ops:c", "sys:mapped")}
        assert keys == {"ops:a": "ops__a", "ops:b": "ops__b", "ops:c": "ops", "sys:mapped": "ops"}
        assert daemon.partitions.binding_for("ops:a").memory_dir.name == "ops__a"
        # A post-mortem and GC go by the same key.
        assert daemon.memory_hub is not None and daemon.memory_hub.resolve_project_key("ops:a") == "ops__a"
    finally:
        daemon.close()


def test_settings_that_cannot_be_activated_leave_the_memory_scopes_as_they_were(tmp_path: Path) -> None:
    path = tmp_path / "hibernaut.toml"
    taken = socket.socket()
    taken.bind(("127.0.0.1", 0))
    taken.listen()

    def write(port: int, *patterns: str) -> None:
        raw = _bound_config(tmp_path, *patterns)
        runtime = {k: v for k, v in raw["runtime"].items() if k != "inbox_bindings"}
        lines = ["[channels]", 'host = "127.0.0.1"', f"port = {port}", "[runtime]"]
        lines += [f"{k} = {json.dumps(v)}" for k, v in runtime.items()]
        for bound in raw["runtime"]["inbox_bindings"]:
            lines += ["[[runtime.inbox_bindings]]", *(f"{k} = {json.dumps(v)}" for k, v in bound.items())]
        path.write_text("\n".join([*lines, "[executors.cli]", 'command = "true"', ""]), encoding="utf-8")

    async def scenario() -> None:
        write(0, "ops:a")
        config = load_config(path)
        daemon = build_daemon_from_config(config)
        hub = daemon.memory_hub
        assert hub is not None
        stop, stderr = asyncio.Event(), io.StringIO()
        with contextlib.redirect_stderr(stderr):
            task = asyncio.create_task(serve_configured(path, config, daemon, stop, reload_interval=0.02))
            try:
                for wanted, port, patterns in (("Cannot activate", taken.getsockname()[1], ("ops:b",)),
                                               ("Reloaded", 0, ("ops:b",))):
                    write(port, *patterns)
                    async with asyncio.timeout(20):
                        while wanted not in stderr.getvalue():
                            await asyncio.sleep(0.02)
                    keys = [hub.resolve_project_key(i) for i in ("ops:a", "ops:b")]
                    assert keys == (["ops__a", "ops"] if wanted == "Cannot activate" else ["ops", "ops__b"])
            finally:
                stop.set()
                await asyncio.wait_for(task, 20)
                daemon.close()

    with taken:
        asyncio.run(scenario())


def test_an_inbox_without_a_usable_memory_directory_is_refused_before_any_run(
    registry: WorkspacePartitionRegistry,
) -> None:
    for inbox_id in ("sys:" + "x" * 300, "x" * 300, "user:" + "x" * 300, "x" * 300 + ":task"):
        with pytest.raises(OSError):
            registry.memory_for(inbox_id)
    # The project's directory can be made, however long the name of one of its queues is.
    assert registry.memory_for("proj:p:" + "x" * 300).project_key == "p"


def test_adapter_exposes_the_bundle_and_keeps_the_legacy_dict(
    registry: WorkspacePartitionRegistry, hub: GovernedMemoryHub
) -> None:
    registry.set_global_instructions("GLOBAL")
    memory = registry.memory_for(PROJ)
    assert memory.load_baseline() == LegacyMemoryPortAdapter(hub, PROJ).load_baseline()
    bundle = memory.load_baseline_bundle()
    assert [b.uri for b in bundle.volatile_tail] == ["proj://hibernaut/reported.md"]
    assert bundle.stable_prefix[0].content == "GLOBAL"


def test_instructions_need_approval_and_the_caller_is_told(
    registry: WorkspacePartitionRegistry, hub: GovernedMemoryHub
) -> None:
    memory = registry.memory_for(PROJ)
    memory.write("instructions.md", "LOCAL RULE")
    assert memory.needs_approval("instructions.md")
    assert not memory.needs_approval("notes/a.md")
    assert memory.load_baseline()["instructions.md"] == ""
    (record,) = hub.list_proposals()
    assert hub.approve_proposal(record["proposal_id"], actor="human:kun").status == "applied"
    assert memory.load_baseline()["instructions.md"].strip() == "LOCAL RULE"


@pytest.mark.parametrize("path", ["proj://web-app/notes/secret.md", "proj://web-app/reported.md"])
def test_a_run_cannot_reach_another_projects_memory_by_uri(
    registry: WorkspacePartitionRegistry, hub: GovernedMemoryHub, path: str
) -> None:
    registry.memory_for("proj:web-app").write("notes/secret.md", "SECRET")
    memory = registry.memory_for(PROJ)
    with pytest.raises(PermissionError):
        memory.read_on_demand(path)
    with pytest.raises(PermissionError):
        memory.write(path, "tampered")
    with pytest.raises(PermissionError):
        memory.read_document({"uri": path})
    with pytest.raises(PermissionError):
        memory.propose({"target_uri": path, "op": "append", "content": "x", "rationale": "r"})
    assert registry.memory_for("proj:web-app").read_on_demand("notes/secret.md") == "SECRET"
    assert hub.list_proposals(status=None) == []


def test_global_and_traversal_writes_are_refused(registry: WorkspacePartitionRegistry) -> None:
    memory = registry.memory_for(PROJ)
    for path in ("global/instructions.md", "global://instructions.md"):
        with pytest.raises(PermissionError):
            memory.write(path, "tampered")
    for path in ("../escape.md", "proj://hibernaut/../web-app/notes/a.md"):
        with pytest.raises(ValueError):
            memory.write(path, "x")
        with pytest.raises(ValueError):
            memory.read_on_demand(path)


def test_concurrent_writers_lose_nothing(tmp_path: Path) -> None:
    # Two hubs on one root stand for the daemon and its stdio MCP companion.
    config = MemoryHubConfig(memory_root=str(tmp_path / "memory"),
                             default_budget=BaselineBudgetConfig(project_reported=100_000, total_max_tokens=200_000))
    hubs = [GovernedMemoryHub(config), GovernedMemoryHub(config)]
    errors: list[BaseException] = []

    def writer(worker: int) -> None:
        try:
            for n in range(15):
                hubs[worker % 2].propose_write(WriteProposal(
                    target_uri="proj://p/reported.md", op="append_reported", content=f"- w{worker}-{n}", rationale="r"))
                hubs[worker % 2].load_baseline(f"fresh{n}")
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    try:
        assert errors == []
        lines = hubs[0].read_on_demand("proj://p/reported.md").content.split("\n- ")
        assert len(lines) == 90
    finally:
        for opened in hubs:
            opened.close()


def _note(hub: GovernedMemoryHub, uri: str, text: str) -> None:
    hub.propose_write(WriteProposal(target_uri=uri, op="upsert_file", content=text, rationale="r", force_new=True))


def _indexed(hub: GovernedMemoryHub) -> set[str]:
    # What a query can see right now: no search(), which would sync first.
    with sqlite3.connect(hub.root / ".index" / "memory_index.db") as conn:
        files = {str(r[0]) for r in conn.execute("SELECT uri FROM indexed_files")}
        assert files >= {str(r[0]) for r in conn.execute("SELECT uri FROM memory_fts")}
    return {uri for uri in files if "/notes/" in uri}


def _index_three_notes(hub: GovernedMemoryHub) -> None:
    shared = hub.root / "shared" / "notes" / "s.md"
    shared.parent.mkdir(parents=True, exist_ok=True)
    shared.write_text("kestrel shared\n")
    _note(hub, "proj://a/notes/a.md", "kestrel in a")
    _note(hub, "proj://b/notes/b.md", "kestrel in b")
    assert _search(hub, "b") >= {"proj://b/notes/b.md", "shared://notes/s.md"}


def _search(hub: GovernedMemoryHub, scope: str) -> set[str]:
    return {hit.uri for hit in hub.search(SearchQuery(scope=scope, query="kestrel")).items}


def test_collecting_one_project_keeps_the_other_projects_index(hub: GovernedMemoryHub) -> None:
    _index_three_notes(hub)
    hub.run_mechanical_gc("a", closed_work_keys=set())
    assert _indexed(hub) == {"proj://a/notes/a.md", "proj://b/notes/b.md", "shared://notes/s.md"}


def test_a_partial_sync_removes_what_is_gone_from_where_it_looked(hub: GovernedMemoryHub) -> None:
    _index_three_notes(hub)
    for rel in ("projects/a/notes/a.md", "projects/b/notes/b.md", "shared/notes/s.md"):
        (hub.root / rel).unlink()
    hub.run_mechanical_gc("a", closed_work_keys=set())
    assert _indexed(hub) == {"proj://b/notes/b.md"}
    assert _search(hub, "b") == set()


def test_rebuilding_one_project_keeps_the_other_projects_index(hub: GovernedMemoryHub) -> None:
    _index_three_notes(hub)
    hub.rebuild_index(scopes=("a",))
    assert _indexed(hub) == {"proj://a/notes/a.md", "proj://b/notes/b.md", "shared://notes/s.md"}


# The daemon's threads share one hub; its stdio MCP companion process opens another on the same root.
@pytest.fixture(params=["same hub", "another hub"])
def neighbour(request: pytest.FixtureRequest, hub: GovernedMemoryHub) -> Iterator[GovernedMemoryHub]:
    if request.param == "same hub":
        yield hub
        return
    opened = GovernedMemoryHub(hub.config)
    yield opened
    opened.close()


def test_nothing_changes_the_index_between_a_searchs_sync_and_its_query(
    hub: GovernedMemoryHub, neighbour: GovernedMemoryHub, monkeypatch: pytest.MonkeyPatch
) -> None:
    _index_three_notes(hub)
    database = hub._index._project_db  # pyright: ignore[reportPrivateUsage]
    query = database.query
    gc = threading.Thread(target=lambda: neighbour.run_mechanical_gc("a", closed_work_keys=set()))
    waited: list[bool] = []

    def query_after_gc_had_its_chance(*args: Any, **kwargs: Any) -> Any:
        gc.start()
        gc.join(0.5)
        waited.append(gc.is_alive())
        return query(*args, **kwargs)

    monkeypatch.setattr(database, "query", query_after_gc_had_its_chance)
    assert "proj://b/notes/b.md" in _search(hub, "b")
    gc.join(10)
    assert waited == [True] and not gc.is_alive()


def test_a_note_written_while_a_search_syncs_stays_indexed(
    hub: GovernedMemoryHub, neighbour: GovernedMemoryHub, monkeypatch: pytest.MonkeyPatch
) -> None:
    _index_three_notes(hub)
    store = hub._store  # pyright: ignore[reportPrivateUsage]
    listing = store.iter_indexable_files
    writer = threading.Thread(target=lambda: _note(neighbour, "proj://b/notes/new.md", "kestrel late"))

    def list_then_let_the_writer_in(**kwargs: Any) -> Any:
        files = list(listing(**kwargs))
        if writer.ident is None:
            writer.start()
            writer.join(0.5)
        return iter(files)

    monkeypatch.setattr(store, "iter_indexable_files", list_then_let_the_writer_in)
    _search(hub, "b")
    writer.join(10)
    assert not writer.is_alive()
    assert "proj://b/notes/new.md" in _indexed(hub)


def test_a_failed_sync_leaves_the_index_as_it_was_and_unlocked(
    hub: GovernedMemoryHub, monkeypatch: pytest.MonkeyPatch
) -> None:
    _index_three_notes(hub)
    before = _indexed(hub)
    (hub.root / "projects" / "b" / "notes" / "b.md").write_text("kestrel edited and longer")
    unreadable = hub.root / "projects" / "b" / "notes" / "z.md"
    unreadable.write_text("kestrel z")
    read_text = Path.read_text

    def fail_on_z(path: Path, *args: Any, **kwargs: Any) -> str:
        if path == unreadable:
            raise FileNotFoundError(path)
        return read_text(path, *args, **kwargs)

    with monkeypatch.context() as patched:
        patched.setattr(Path, "read_text", fail_on_z)
        with pytest.raises(FileNotFoundError):
            _search(hub, "b")
    assert _indexed(hub) == before
    other = GovernedMemoryHub(hub.config)
    try:
        assert "proj://b/notes/z.md" in _search(other, "b")
    finally:
        other.close()


def test_proposal_ids_cannot_leave_the_proposals_directory(hub: GovernedMemoryHub, tmp_path: Path) -> None:
    target = hub.root / "projects" / "planted.json"
    target.write_text(json.dumps({"status": "pending", "proposal": {
        "target_uri": "proj://hibernaut/instructions.md", "op": "append", "content": "PLANTED", "rationale": "r"}}))
    for proposal_id in ("../projects/planted", str(target.with_suffix("")), ""):
        assert hub.approve_proposal(proposal_id, actor="x").status == "not_found"
        assert hub.reject_proposal(proposal_id, actor="x").status == "not_found"
    assert "PLANTED" not in hub.load_baseline("hibernaut").to_legacy_dict()["instructions.md"]


def test_a_run_only_approves_proposals_of_its_own_project(
    registry: WorkspacePartitionRegistry, hub: GovernedMemoryHub
) -> None:
    registry.memory_for("proj:web-app").write("instructions.md", "theirs")
    (record,) = hub.list_proposals()
    with pytest.raises(PermissionError):
        registry.memory_for(PROJ).approve(record["proposal_id"], actor="x")
    assert hub.list_proposals()[0]["status"] == "pending"


# --- 2b. memory written before Memory Hub, one directory per inbox_id ---------------------------------------------


def _legacy(root: Path, name: str, **files: str) -> Path:
    folder = root / "projects" / name
    for rel, text in files.items():
        path = folder / rel.replace("__", "/")
        path = path.with_suffix(".md")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return folder


def test_legacy_partition_memory_is_carried_over_and_kept(tmp_path: Path) -> None:
    root = tmp_path / "memory"
    old = _legacy(root, "proj__hibernaut", instructions="OLD RULE", index="OLD INDEX", reported="- old", notes__a="NOTE")
    (old / "progress").mkdir()
    registry = WorkspacePartitionRegistry(root)
    memory = registry.memory_for(PROJ)
    baseline = memory.load_baseline()
    assert (baseline["instructions.md"], baseline["index.md"], baseline["reported.md"]) == (
        "OLD RULE", "OLD INDEX", "- old")
    assert memory.read_on_demand("notes/a.md") == "NOTE"
    assert (old / "instructions.md").read_text() == "OLD RULE"

    # Once only: what changed since is not overwritten by the old copy.
    memory.write("notes/a.md", "NEWER")
    again = WorkspacePartitionRegistry(root).memory_for(PROJ)
    assert again.read_on_demand("notes/a.md") == "NEWER"


def test_legacy_partitions_of_one_project_merge_without_losing_either(tmp_path: Path) -> None:
    root = tmp_path / "memory"
    _legacy(root, "proj__hibernaut", instructions="PROJECT RULE")
    _legacy(root, "hibernaut__issue-42", instructions="TASK RULE", progress__t="TASK PROGRESS")
    # The task inbox wakes first; the project's own directory still wins the shared file.
    memory = WorkspacePartitionRegistry(root).memory_for(ISSUE)
    assert memory.load_baseline()["instructions.md"] == "PROJECT RULE"
    assert memory.read_on_demand("progress/t.md") == "TASK PROGRESS"
    assert memory.read_on_demand("notes/legacy/hibernaut__issue-42/instructions.md") == "TASK RULE"


def test_memory_in_use_by_another_scope_is_not_taken_for_old_memory(registry: WorkspacePartitionRegistry) -> None:
    # proj__ops is where an inbox named so has its memory today, not where project ops had it before.
    registry.memory_for("proj__ops").write("notes/private.md", "not for the project")
    with pytest.raises(FileNotFoundError):
        registry.memory_for("ops:task").read_on_demand("notes/private.md")


def test_an_inbox_whose_directory_name_is_unchanged_is_left_alone(tmp_path: Path) -> None:
    root = tmp_path / "memory"
    _legacy(root, "user__main", instructions="MINE")
    memory = WorkspacePartitionRegistry(root).memory_for("user:main")
    assert memory.load_baseline()["instructions.md"] == "MINE"
    assert not (root / "projects" / "user__main" / "notes" / "legacy").exists()


# --- 3. configuration and prompt cache segments -------------------------------------------------------------------


def test_budget_defaults() -> None:
    cfg = parse_config({})
    assert cfg.memory_hub.default_budget == BaselineBudgetConfig(1500, 2500, 1500, 1000, 6500)
    assert cfg.memory_hub.mounts == ()
    assert not cfg.memory_hub.http.enabled


def test_budgets_mounts_and_inbox_projects_are_parsed(tmp_path: Path) -> None:
    cfg = parse_config({"memory_hub": {
        "budget": {"project_instructions": 3000},
        "projects": {"hibernaut": {"budget": {"project_reported": 400}}},
        "mounts": {"personal": {"root": "kb", "include_globs": ["notes/**/*.md"], "max_file_bytes": 1000}},
        "inbox_projects": {"custom:steward": "hibernaut"},
        "auto_git_commit": True,
        "gc_interval_seconds": 60,
    }}, base_dir=tmp_path)
    hub_cfg = cfg.memory_hub.hub_config(tmp_path / "memory")
    assert hub_cfg.default_budget == BaselineBudgetConfig(1500, 3000, 1500, 1000, 6500)
    # A project's budget starts from [memory_hub.budget], not from the built-in one.
    assert hub_cfg.budget_for("hibernaut") == BaselineBudgetConfig(1500, 3000, 1500, 400, 6500)
    (mount,) = hub_cfg.mounts
    assert (mount.name, mount.root, mount.include_globs, mount.max_file_bytes) == (
        "personal", str(tmp_path / "kb"), ("notes/**/*.md",), 1000)
    assert hub_cfg.inbox_project_map == {"custom:steward": "hibernaut"}
    assert hub_cfg.auto_git_commit
    assert cfg.memory_hub.gc_interval_seconds == 60


@pytest.mark.parametrize("section", [
    {"budget": {"project_instructions": 0}},
    {"budget": {"project_instructions": True}},
    {"budget": {"instructions": 10}},
    {"projects": {"../x": {"budget": {}}}},
    {"projects": {"x": {"budgets": {}}}},
    {"mounts": {"a/b": {"root": "x"}}},
    {"mounts": {"a": {}}},
    {"mounts": {"a": {"root": "x", "mode": "readwrite"}}},
    {"mounts": {"a": {"root": "x", "include_globs": "**"}}},
    {"inbox_projects": {"a": ""}},
    {"gc_interval_seconds": -1},
    {"http": {"enabled": True}},
    {"http": {"enabled": True, "tokens": {"ops": {"role": "admin"}}}},
    {"http": {"enabled": True, "tokens": {"ops": {"token_env": "T", "role": "root"}}}},
    # The package's admin role approves and collects any project, so it cannot be limited to some.
    {"http": {"enabled": True, "tokens": {"ops": {"token_env": "T", "role": "admin", "allowed_projects": ["a"]}}}},
    {"unknown": 1},
])
def test_unusable_settings_are_refused(section: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        parse_config({"memory_hub": section})


def test_configured_budget_reaches_the_daemon(tmp_path: Path) -> None:
    cfg = parse_config({
        "runtime": {"db_path": str(tmp_path / "hb.db"), "memory_root": str(tmp_path / "memory")},
        "memory_hub": {"projects": {"hibernaut": {"budget": {"global_instructions": 10}}}},
    })
    daemon = build_daemon_from_config(cfg)
    try:
        daemon.partitions.set_global_instructions("G" * 300)
        bundle = daemon.partitions.memory_for(PROJ).load_baseline_bundle()
        assert bundle.truncated_blocks == ("global://instructions.md",)
        assert daemon.ledger is not None
        (audit,) = daemon.ledger.store.list_audits(stage="memory.budget_exceeded")
        assert (audit.service, audit.scope, audit.severity) == ("hibernaut", "hibernaut", "warn")
        # A reload keeps the process's one hub.
        replacement = build_daemon_from_config(cfg, database=daemon.db, ledger=daemon.ledger,
                                               memory_hub=daemon.memory_hub)
        assert replacement.memory_hub is daemon.memory_hub
    finally:
        daemon.close()


def test_a_reload_keeps_the_hub_and_refuses_new_memory_settings(tmp_path: Path) -> None:
    path = tmp_path / "hibernaut.toml"

    def write(setting: str, ttl: int) -> None:
        path.write_text(
            f'[channels]\nport = 0\n[runtime]\ndb_path = "{tmp_path}/hb.db"\nmemory_root = "{tmp_path}/memory"\n{setting}\n'
            f"[memory_hub]\nproposal_ttl_days = {ttl}\n", encoding="utf-8")

    async def scenario() -> None:
        write("max_concurrency = 2", 14)
        config = load_config(path)
        daemon = build_daemon_from_config(config)
        stop, stderr = asyncio.Event(), io.StringIO()

        async def until(text: str) -> None:
            async with asyncio.timeout(20):
                while text not in stderr.getvalue():
                    await asyncio.sleep(0.02)

        with contextlib.redirect_stderr(stderr):
            task = asyncio.create_task(serve_configured(path, config, daemon, stop, reload_interval=0.02))
            try:
                write("max_concurrency = 3", 14)
                await until("Reloaded")
                assert daemon.memory_hub is not None and daemon.memory_hub.list_proposals() == []
                write("max_concurrency = 3", 2)
                await until("[memory_hub] changes require a restart")
                assert stderr.getvalue().count("Reloaded") == 1
            finally:
                stop.set()
                await asyncio.wait_for(task, 20)
                daemon.close()

    asyncio.run(scenario())


def _fill(registry: WorkspacePartitionRegistry, hub: GovernedMemoryHub) -> PartitionedMemoryAdapter:
    registry.set_global_instructions("GLOBAL RULE")
    memory = registry.memory_for(PROJ)
    memory.write("instructions.md", "PROJECT RULE")
    hub.approve_proposal(hub.list_proposals()[0]["proposal_id"], actor="t")
    memory.write("index.md", "THE INDEX")
    memory.write("reported.md", "REPORTED ONE")
    return memory


def test_segments_follow_the_bundle(registry: WorkspacePartitionRegistry, hub: GovernedMemoryHub) -> None:
    segments = baseline_segments(_fill(registry, hub))
    assert [name for name, _ in segments.stable] == ["instructions.md", "index.md"]
    assert dict(segments.stable)["instructions.md"] == "GLOBAL RULE\n\nPROJECT RULE"
    assert [name for name, _ in segments.volatile] == ["reported.md"]
    assert "REPORTED ONE" in dict(segments.volatile)["reported.md"]


def test_segments_of_another_memory_port() -> None:
    segments = baseline_segments(InMemoryMemoryStore({"instructions.md": "I", "index.md": "X", "reported.md": "R"}))
    assert segments.stable == (("instructions.md", "I"), ("index.md", "X"))
    assert segments.volatile == (("reported.md", "R"),)


class ScriptedLLM:
    def __init__(self, *replies: LLMReply) -> None:
        self.replies = list(replies)
        self.seen: list[tuple[str, list[Any], list[str]]] = []

    async def complete(self, *, system_prefix: str, messages: Any, available_tools: Any) -> LLMReply:
        self.seen.append((system_prefix, list(messages), list(available_tools)))
        return self.replies.pop(0) if self.replies else LLMReply(silent=True)


def test_whitebox_keeps_reported_out_of_the_cached_prefix(
    registry: WorkspacePartitionRegistry, hub: GovernedMemoryHub
) -> None:
    memory = _fill(registry, hub)
    first, second = ScriptedLLM(), ScriptedLLM()
    asyncio.run(WhiteboxExecutor(llm=first).execute([event({"n": 1})], None, Caps(memory)))  # type: ignore[arg-type]
    memory.write("reported.md", "REPORTED TWO")
    asyncio.run(WhiteboxExecutor(llm=second).execute([event({"n": 1})], None, Caps(memory)))  # type: ignore[arg-type]
    prefix, messages, _ = first.seen[0]
    assert "PROJECT RULE" in prefix and "THE INDEX" in prefix and "REPORTED" not in prefix
    assert second.seen[0][0] == prefix
    assert "REPORTED ONE" in messages[0].content
    assert "REPORTED TWO" in second.seen[0][1][0].content


def test_whitebox_keeps_reported_out_of_checkpoints_and_through_compaction(
    registry: WorkspacePartitionRegistry, hub: GovernedMemoryHub
) -> None:
    memory = _fill(registry, hub)
    call = ToolCallRequest("send_email", {"to": "a"})
    llm = ScriptedLLM(*(LLMReply(tool_calls=(ToolCallRequest("read_memory", {"path": "index.md"}),)) for _ in range(6)),
                      LLMReply(tool_calls=(call,)))
    executor = WhiteboxExecutor(llm=llm, max_messages_before_compact=4)
    outcome = asyncio.run(executor.execute([event({"n": i}) for i in range(9)], None, Caps(memory)))  # type: ignore[arg-type]
    assert isinstance(outcome, Suspend)
    assert "REPORTED ONE" not in outcome.session_checkpoint
    assert all(sum("REPORTED ONE" in m.content for m in messages) == 1 for _, messages, _ in llm.seen)


def test_cli_prompt_puts_reported_after_the_stable_part(
    registry: WorkspacePartitionRegistry, hub: GovernedMemoryHub
) -> None:
    memory = _fill(registry, hub)
    executor = LocalCLIExecutor(worktree_manager=Mock(), inbox_checkout_map={}, visible_runner=Mock())
    session = SessionRef(session_id="s", inbox_id=PROJ, checkpoint="CHECKPOINT")
    build = executor._build_prompt  # pyright: ignore[reportPrivateUsage]
    one = build([event({"n": 1})], session, Caps(memory))  # type: ignore[arg-type]
    order = [one.index(t) for t in ("GLOBAL RULE", "PROJECT RULE", "THE INDEX", "REPORTED ONE", "CHECKPOINT", "## events")]
    assert order == sorted(order)
    memory.write("reported.md", "REPORTED TWO")
    two = build([event({"n": 2})], session, Caps(memory))  # type: ignore[arg-type]
    cut = one.index("## reported.md")
    assert one[:cut] == two[:cut]


# --- 4. tools and approval ----------------------------------------------------------------------------------------


def test_memory_tools_and_the_default_policy() -> None:
    policy = AutoModePermissionAdapter()
    for name in ("memory_search", "memory_read", "memory_propose_write"):
        assert policy.check(name, {}).action == "allow", name
    assert policy.check("memory_approve_proposal", {"proposal_id": "prop_1"}).action == "defer"


def test_allowing_every_listed_tool_does_not_allow_approval() -> None:
    # What [runner] allow_tools does: an allow list cannot take approval away from a person.
    policy = AutoModePermissionAdapter(
        allow_tools=AutoModePermissionAdapter.DEFAULT_ALLOW_TOOLS | {"memory_approve_proposal"})
    assert policy.check("memory_approve_proposal", {}).action == "defer"


def test_whitebox_searches_reads_and_proposes(registry: WorkspacePartitionRegistry, hub: GovernedMemoryHub) -> None:
    memory = registry.memory_for(PROJ)
    memory.write("notes/lease.md", "# 租约\n\n单活租约\n")
    llm = ScriptedLLM(
        LLMReply(tool_calls=(ToolCallRequest("memory_search", {"query": "租约"}),)),
        LLMReply(tool_calls=(ToolCallRequest("memory_read", {"uri": "notes/lease.md", "heading": "租约"}),)),
        LLMReply(tool_calls=(ToolCallRequest("memory_propose_write", {
            "target_uri": "instructions.md", "op": "append", "content": "- rule", "rationale": "why",
            "author": "human:kun"}),)),
        LLMReply(tool_calls=(ToolCallRequest("memory_read", {"uri": "notes/none.md"}),)),
        LLMReply(tool_calls=(ToolCallRequest("write_memory", {"path": "instructions.md", "content": "all mine"}),)),
        LLMReply(tool_calls=(ToolCallRequest("write_memory", {"path": "index.md", "content": "x" * 20_000}),)),
    )
    asyncio.run(WhiteboxExecutor(llm=llm).execute([event({})], None, Caps(memory)))  # type: ignore[arg-type]
    results = [m.content for m in llm.seen[-1][1] if m.role == "tool"]
    assert {"memory_search", "memory_read", "memory_propose_write", "memory_approve_proposal"} <= set(llm.seen[0][2])
    assert "proj://hibernaut/notes/lease.md" in results[0]
    assert "单活租约" in results[1]
    assert "pending_approval" in results[2]
    assert "not found" in results[3]
    assert "approval" in results[4] and "wrote" not in results[4]
    assert "budget" in results[5]
    assert {p["proposal"]["author"] for p in hub.list_proposals()} == {"whitebox", None}
    assert memory.load_baseline()["instructions.md"] == ""


def test_an_agents_approval_waits_for_a_person_who_sees_the_content(
    registry: WorkspacePartitionRegistry, hub: GovernedMemoryHub
) -> None:
    memory = registry.memory_for(PROJ)
    memory.write("instructions.md", "PROPOSED RULE")
    (record,) = hub.list_proposals()
    call = ToolCallRequest("memory_approve_proposal", {"proposal_id": record["proposal_id"]})
    executor = WhiteboxExecutor(llm=ScriptedLLM(LLMReply(tool_calls=(call,))))
    outcome = asyncio.run(executor.execute([event({})], None, Caps(memory)))  # type: ignore[arg-type]
    assert isinstance(outcome, Suspend)
    assert "PROPOSED RULE" in outcome.prompt and "proj://hibernaut/instructions.md" in outcome.prompt
    assert hub.list_proposals()[0]["status"] == "pending"

    def resume(approved: bool, checkpoint: str) -> None:
        session = SessionRef(session_id="s", inbox_id=PROJ, checkpoint=checkpoint)
        asyncio.run(WhiteboxExecutor(llm=ScriptedLLM(LLMReply(emit_messages=("ok",)))).execute(
            [event({"approved": approved})], session, Caps(memory)))  # type: ignore[arg-type]

    resume(False, outcome.session_checkpoint)
    assert memory.load_baseline()["instructions.md"] == ""
    resume(True, outcome.session_checkpoint)
    assert memory.load_baseline()["instructions.md"].strip() == "PROPOSED RULE"


def test_memory_tools_without_memory_hub_say_so() -> None:
    llm = ScriptedLLM(LLMReply(tool_calls=(ToolCallRequest("memory_search", {"query": "x"}),)))
    outcome = asyncio.run(WhiteboxExecutor(llm=llm).execute(
        [event({})], None, Caps(InMemoryMemoryStore({}))))  # type: ignore[arg-type]
    assert not isinstance(outcome, Emit)
    assert "Memory Hub" in llm.seen[-1][1][-1].content


def _call(daemon: Any, name: str, **arguments: Any) -> dict[str, Any]:
    reply = daemon.mcp_server.handle_rpc({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                                          "params": {"name": name, "arguments": arguments}})
    return reply["error"] if "error" in reply else reply["result"]["structuredContent"]


@pytest.fixture
def daemon(tmp_path: Path) -> Iterator[Any]:
    built = build_daemon(db_path=tmp_path / "hb.db", memory_root=tmp_path / "memory")
    yield built
    built.close()


def test_mcp_control_plane_tools(daemon: Any) -> None:
    tools = daemon.mcp_server.handle_rpc({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})["result"]["tools"]
    assert {"memory_search", "memory_read", "memory_propose_write", "memory_approve_proposal",
            "memory_reject_proposal", "memory_list_proposals", "memory_record_postmortem"} <= {t["name"] for t in tools}
    proposed = _call(daemon, "memory_propose_write", target_uri="proj://hibernaut/instructions.md", op="append",
                     content="- pinned rule", rationale="why", author="human:kun")
    assert proposed["status"] == "pending_approval"
    (item,) = _call(daemon, "memory_list_proposals")["items"]
    assert (item["proposal_id"], item["proposal"]["author"]) == (proposed["proposal_id"], "mcp")
    assert _call(daemon, "memory_approve_proposal", proposal_id=proposed["proposal_id"])["status"] == "applied"
    read = _call(daemon, "memory_read", uri="proj://hibernaut/instructions.md")
    assert "pinned rule" in read["content"]
    hits = _call(daemon, "memory_search", scope="hibernaut", query="pinned rule")["items"]
    assert [h["uri"] for h in hits] == ["proj://hibernaut/instructions.md"]
    rejected = _call(daemon, "memory_propose_write", target_uri="proj://hibernaut/instructions.md", op="append",
                     content="- unwanted rule", rationale="why")
    _call(daemon, "memory_reject_proposal", proposal_id=rejected["proposal_id"], reason="no")
    assert _call(daemon, "memory_approve_proposal", proposal_id=rejected["proposal_id"])["status"] == "conflict"
    for name, arguments in (("memory_read", {"uri": "proj://hibernaut/none.md"}), ("memory_read", {}),
                            ("memory_search", {"scope": "hibernaut", "query": "x", "top_k": "many"}),
                            ("memory_propose_write", {"target_uri": "nope", "op": "append", "content": "", "rationale": "r"})):
        assert _call(daemon, name, **arguments)["code"] == -32602, name


# --- 4b. post-mortems ---------------------------------------------------------------------------------------------


def _run(run_id: str, *, inbox: str = PROJ, kind: str | None = "tool_error", summary: str | None = "exit 2 at line 10",
         status: str = "failed") -> RunRecord:
    return RunRecord(
        run_id=run_id, inbox_id=inbox, session_id="s", event_ids=(run_id,), executor_id="x",
        started_at="2026-11-23T00:00:00Z", ended_at="2026-11-23T00:00:01Z", duration_ms=1, outcome_kind="emit",
        emit_status=status, error_kind=kind, error_summary=summary, correlation_key="k",  # type: ignore[arg-type]
    )


@pytest.fixture
def ledger(tmp_path: Path) -> Iterator[RunLedger]:
    opened = RunLedger(tmp_path / "ledger.db")
    yield opened
    opened.close()


def _postmortem(hub: GovernedMemoryHub, ledger: RunLedger, run_id: str, **more: Any) -> dict[str, Any]:
    return MemoryControlPlane(hub, ledger).call("memory_record_postmortem", {
        "run_id": run_id, "root_cause": "Missing PYTHONPATH", "proposed_rule": "Run with PYTHONPATH=src.", **more})


def test_postmortem_takes_kind_project_and_recurrence_from_the_ledger(
    hub: GovernedMemoryHub, ledger: RunLedger
) -> None:
    ledger.store.append_run(_run("run_r1", summary="exit 2 at line 10"))
    first = _postmortem(hub, ledger, "run_r1")
    assert (first["status"], first["recurrence_count"], first["proposal_id"]) == ("staged", 1, None)
    note = hub.read_on_demand(first["note_uri"]).content
    assert "error_kind: tool_error" in note and "run_id: run_r1" in note

    # Another failure, another project, a success and a success-shaped retry do not count.
    ledger.store.append_run(_run("run_other", summary="connection table missing"))
    ledger.store.append_run(_run("run_elsewhere", inbox="proj:web-app"))
    ledger.store.append_run(_run("run_fine", status="done", kind=None, summary=None))
    assert _postmortem(hub, ledger, "run_r1")["recurrence_count"] == 1

    # Same project through another inbox, same failure with other numbers in it.
    ledger.store.append_run(_run("run_r2", inbox=ISSUE, summary="exit 2 at line 99"))
    second = _postmortem(hub, ledger, "run_r2")
    assert (second["status"], second["recurrence_count"]) == ("staged_and_proposed", 2)
    assert "PYTHONPATH" not in hub.load_baseline("hibernaut").to_legacy_dict()["instructions.md"]
    hub.approve_proposal(second["proposal_id"], actor="human:kun")
    assert "PYTHONPATH" in hub.load_baseline("hibernaut").to_legacy_dict()["instructions.md"]


def test_postmortem_refuses_transient_successful_and_unknown_runs(hub: GovernedMemoryHub, ledger: RunLedger) -> None:
    ledger.store.append_run(_run("run_rate", kind="rate_limit"))
    ledger.store.append_run(_run("run_fine", status="done", kind=None, summary=None))
    assert _postmortem(hub, ledger, "run_rate")["status"] == "rejected_transient"
    for run_id in ("run_fine", "run_nothing"):
        with pytest.raises(ValueError):
            _postmortem(hub, ledger, run_id)
    with pytest.raises(ValueError):
        MemoryControlPlane(hub, None).call("memory_record_postmortem", {
            "run_id": "run_r1", "root_cause": "c", "proposed_rule": "r"})
    with pytest.raises(ValueError):
        _postmortem(hub, ledger, "run_rate", root_cause="")
    assert not (hub.root / "projects" / "hibernaut" / "notes" / "postmortem").exists()


def test_failures_without_a_summary_never_count_as_each_other(hub: GovernedMemoryHub, ledger: RunLedger) -> None:
    ledger.store.append_run(_run("run_a", summary=None))
    ledger.store.append_run(_run("run_b", summary=None))
    assert _postmortem(hub, ledger, "run_a")["recurrence_count"] == 1


# --- 4c. mechanical GC --------------------------------------------------------------------------------------------


def _old(path: Path, days: int = 10) -> None:
    stamp = (datetime.now(UTC) - timedelta(days=days)).timestamp()
    os.utime(path, (stamp, stamp))


def test_gc_archives_progress_of_closed_work_only(daemon: Any) -> None:
    memory = daemon.partitions.memory_for(PROJ)
    other = daemon.partitions.memory_for("proj:web-app")
    for adapter, name in ((memory, "done-1"), (memory, "open-2"), (memory, "unknown-3"), (memory, "both-4"),
                          (other, "done-1"), (memory, "kun-g__hibernaut#9")):
        adapter.write(f"progress/{name}.md", "x")
        _old(daemon.memory_hub.root / "projects" / adapter.project_key / "progress" / f"{name}.md")
    for session_id, inbox, key, status in (
        ("s1", PROJ, "done-1", "closed"), ("s2", PROJ, "open-2", "active"), ("s3", PROJ, "both-4", "closed"),
        ("s4", ISSUE, "both-4", "suspended"), ("s5", PROJ, "kun-g/hibernaut#9", "closed"),
    ):
        daemon.sessions.save(SessionRef(session_id=session_id, inbox_id=inbox, work_key=key, correlation_key=key,
                                        status=status))
    reports = {r.project_key: r for r in MemoryGarbageCollector(daemon.memory_hub, daemon.sessions).collect_once()}
    assert set(reports["hibernaut"].archived_progress_files) == {"progress/done-1.md", "progress/kun-g__hibernaut#9.md"}
    # The session that closed done-1 belongs to another project.
    assert reports["web-app"].archived_progress_files == ()


def test_gc_leaves_directories_memory_hub_has_not_taken_over(tmp_path: Path) -> None:
    root = tmp_path / "memory"
    old = _legacy(root, "proj__sleeping", index="- `notes/gone.md` | x")
    opened = GovernedMemoryHub(MemoryHubConfig(memory_root=str(root)))
    try:
        assert MemoryGarbageCollector(opened, None).collect_once() == []
        assert (old / "index.md").read_text() == "- `notes/gone.md` | x"
    finally:
        opened.close()


def test_one_failing_project_does_not_stop_the_round(hub: GovernedMemoryHub, monkeypatch: pytest.MonkeyPatch) -> None:
    hub.load_baseline("aaa")
    hub.load_baseline("bbb")
    real = hub.run_mechanical_gc

    def collect(project_key: str, **kwargs: Any) -> Any:
        if project_key == "aaa":
            raise OSError("disk")
        return real(project_key, **kwargs)

    monkeypatch.setattr(hub, "run_mechanical_gc", collect)
    assert [r.project_key for r in MemoryGarbageCollector(hub, None).collect_once()] == ["bbb"]


def test_the_daemon_collects_periodically(tmp_path: Path) -> None:
    daemon = build_daemon(db_path=tmp_path / "hb.db", memory_root=tmp_path / "memory",
                          memory_gc_interval_seconds=0.05)
    index = daemon.memory_hub.root / "projects" / "hibernaut" / "index.md"

    async def scenario() -> None:
        stop = asyncio.Event()
        services = asyncio.create_task(daemon.run_services(stop))
        for _ in range(2):  # more than the round at startup
            daemon.partitions.memory_for(PROJ).write("index.md", "- `notes/ghost.md` | ghost")
            deadline = time.monotonic() + 20
            while "ghost" in index.read_text() and time.monotonic() < deadline:
                await asyncio.sleep(0.02)
            assert "ghost" not in index.read_text()
        stop.set()
        await services

    try:
        daemon.partitions.memory_for(PROJ).load_baseline()
        asyncio.run(scenario())
    finally:
        daemon.close()


def test_gc_interval_zero_turns_it_off(tmp_path: Path) -> None:
    daemon = build_daemon(db_path=tmp_path / "hb.db", memory_root=tmp_path / "memory", memory_gc_interval_seconds=0)
    try:
        assert daemon.memory_gc is None
    finally:
        daemon.close()


# --- 4d. Phase 3 HTTP service -------------------------------------------------------------------------------------


def _http(base: str, path: str, token: str, body: dict[str, Any] | None = None) -> tuple[int, dict[str, Any]]:
    request = urllib.request.Request(
        base + path, data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="GET" if body is None else "POST")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=5) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as err:
        with err:
            return err.code, json.loads(err.read())


HTTP = {"enabled": True, "listen_port": 0, "tokens": {
    "agent": {"token_env": "MH_AGENT", "role": "propose", "allowed_projects": ["hibernaut"]},
    "human:kun": {"token_env": "MH_ADMIN", "role": "admin"},
}}


def test_http_service_is_off_unless_enabled(hub: GovernedMemoryHub) -> None:
    assert open_memory_http(parse_config({}).memory_hub.http, hub, environ={}) is None


def test_http_service_takes_its_tokens_out_of_the_environment(hub: GovernedMemoryHub, fast_memory_http_shutdown: None) -> None:
    http = parse_config({"memory_hub": {"http": HTTP}}).memory_hub.http
    with pytest.raises(ValueError, match="MH_ADMIN"):
        open_memory_http(http, hub, environ={"MH_AGENT": "a" * 8})
    with pytest.raises(ValueError, match="same token"):
        open_memory_http(http, hub, environ={"MH_AGENT": "same", "MH_ADMIN": "same"})
    environ = {"MH_AGENT": "tok-agent", "MH_ADMIN": "tok-admin", "PATH": "/bin"}
    server = open_memory_http(http, hub, environ=environ)
    assert server is not None
    base = f"http://{server.address[0]}:{server.address[1]}"
    try:
        assert environ == {"PATH": "/bin"}
        assert server.address[0] == "127.0.0.1"
        status, proposed = _http(base, "/v1/memory/propose", "tok-agent", {
            "target_uri": "proj://hibernaut/instructions.md", "op": "append", "content": "- r", "rationale": "x",
            "author": "human:kun"})
        assert (status, proposed["status"]) == (200, "pending_approval")
        hub.propose_write(WriteProposal(target_uri="proj://web-app/instructions.md", op="append", content="THEIRS",
                                        rationale="x"))
        status, listed = _http(base, "/v1/memory/proposals", "tok-agent")
        # Only its own projects' proposals, under the name of the token that made them.
        assert [(i["proposal"]["target_uri"], i["proposal"]["author"]) for i in listed["items"]] == [
            ("proj://hibernaut/instructions.md", "agent")]
        assert len(_http(base, "/v1/memory/proposals", "tok-admin")[1]["items"]) == 2
        body = {"proposal_id": proposed["proposal_id"]}
        assert _http(base, "/v1/memory/approve", "tok-agent", body)[0] == 403
        assert _http(base, "/v1/memory/approve", "tok-admin", body)[1]["status"] == "applied"
        assert _http(base, "/v1/memory/proposals", "nobody")[0] == 401
    finally:
        server.shutdown()


def test_the_daemon_owns_the_http_service(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fast_memory_http_shutdown: None
) -> None:
    monkeypatch.setenv("MH_AGENT", "tok-agent")
    monkeypatch.setenv("MH_ADMIN", "tok-admin")
    cfg = parse_config({
        "runtime": {"db_path": str(tmp_path / "hb.db"), "memory_root": str(tmp_path / "memory")},
        "memory_hub": {"http": HTTP},
    })
    daemon = build_daemon_from_config(cfg)
    try:
        assert "MH_ADMIN" not in os.environ
        assert daemon.memory_http is not None
        base = f"http://{daemon.memory_http.address[0]}:{daemon.memory_http.address[1]}"
        assert _http(base, "/v1/memory/baseline?project_key=hibernaut", "tok-agent")[0] == 200
        replacement = build_daemon_from_config(cfg, database=daemon.db, ledger=daemon.ledger,
                                               memory_hub=daemon.memory_hub)
        assert replacement.memory_http is None
    finally:
        daemon.close()
    with pytest.raises(urllib.error.URLError):
        _http(base, "/healthz", "x")
