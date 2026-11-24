from __future__ import annotations

import configparser
import json
import os
import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config
from agent_runtime.app.projects import build_routing_manifest
from agent_runtime.adapters.executor_cli.role_skills import parse_skills, render_skills
from agent_runtime.adapters.memory_fs import (
    ExpertMount,
    GovernedMemoryHub,
    PartitionedMemoryAdapter,
    WorkspacePartitionRegistry,
    baseline_segments,
    call_memory_tool,
    open_role_store,
)
from collaboration import (
    CloseoutCoordinator,
    CloseoutDecision,
    CloseoutStore,
    DigestAssembler,
    ExpertPoolRegistry,
    ExpertProfile,
    RoleSkillMemoryStore,
)
from memory_hub import KnowledgeMountConfig, MemoryHubConfig

ROOT = Path(__file__).resolve().parents[1]


# --- 1. package registration and dependency contracts -----------------------------------------------------------


def test_import_contracts_isolate_collaboration_both_ways() -> None:
    parser = configparser.ConfigParser()
    parser.read(ROOT / ".importlinter")

    def names(section: str, key: str) -> set[str]:
        return set(parser[section][key].split())

    assert "collaboration" in names("importlinter", "root_packages")
    own = "importlinter:contract:collaboration-zero-business-deps"
    assert parser[own]["name"] == "Collaboration package has zero business dependencies"
    assert parser[own]["type"] == "forbidden"
    assert names(own, "source_modules") == {"collaboration"}
    business = {"switchboard", "taskboard", "runner", "ledger", "ccpeer", "dashboard", "memory_hub"}
    assert names(own, "forbidden_modules") == {"agent_runtime"} | business
    reverse = "importlinter:contract:business-packages-no-collaboration"
    assert names(reverse, "source_modules") == business
    assert names(reverse, "forbidden_modules") == {"collaboration"}


def test_collaboration_is_type_checked_strictly() -> None:
    assert "src/collaboration" in json.loads((ROOT / "pyrightconfig.json").read_text())["include"]


def test_records_carry_exactly_what_their_schemas_declare(tmp_path: Path) -> None:
    card = CloseoutCoordinator(CloseoutStore(tmp_path / "closeouts")).emit_draft(
        session_id="s", agent_id="architect", project_key="hibernaut", summary="done",
        decisions=(CloseoutDecision("D1", "t", "A"),),
    )
    profile = ExpertProfile(agent_id="a", role_id="architect", display_name="A", executor_id="a", skills=(),
                            quality_gates=())
    digest = DigestAssembler().assemble(window_label="24h", tasks=[], runs=[], role_proposals=[], closeouts=[card])
    for name, record in (("closeout_card", card), ("expert_profile", profile), ("digest_snapshot", digest)):
        schema = json.loads((ROOT / "schemas/v1" / f"{name}.schema.json").read_text())
        assert schema["additionalProperties"] is False
        assert set(record.to_dict()) == set(schema["properties"]) >= set(schema["required"]), name


# --- 2. role:// and the four-layer mount of a drafted expert ----------------------------------------------------

EXPERT = "proj:hibernaut:architect"
SKILLS = "role://architect/skills.md"


@pytest.fixture
def hub(tmp_path: Path) -> Iterator[GovernedMemoryHub]:
    pkb, other = tmp_path / "pkb", tmp_path / "other"
    for folder in (pkb, other):
        folder.mkdir()
        (folder / "card.md").write_text(f"# Layering\nlayering card in {folder.name}\n")
    opened = GovernedMemoryHub(MemoryHubConfig(
        memory_root=str(tmp_path / "memory"),
        mounts=(KnowledgeMountConfig(name="pkb", root=str(pkb)), KnowledgeMountConfig(name="other", root=str(other))),
    ))
    for key in ("hibernaut", "workshop", "lufit"):
        opened.lay_out(key)
        (opened.project_dir(key) / "notes" / "layering.md").write_text(f"# Layering\nlayering note of {key}\n")
    (opened.root / "shared" / "notes").mkdir(parents=True, exist_ok=True)
    (opened.root / "shared" / "notes" / "layering.md").write_text("# Layering\nlayering shared note\n")
    (opened.root / "shared" / "elsewhere.md").write_text("# Layering\nlayering elsewhere\n")
    yield opened
    opened.close()


@pytest.fixture
def roles(hub: GovernedMemoryHub) -> RoleSkillMemoryStore:
    return open_role_store(hub)


def expert(
    hub: GovernedMemoryHub, roles: RoleSkillMemoryStore, *, budget: int = 6500
) -> PartitionedMemoryAdapter:
    plan = ExpertPoolRegistry().resolve_for_project(
        role_id="architect", project_key="hibernaut", extra_readonly_projects=("workshop",), budget_chars=budget
    ).mount_plan
    registry = WorkspacePartitionRegistry(
        hub.root, hub=hub, mounts=lambda inbox_id: ExpertMount(plan, roles) if inbox_id == EXPERT else None
    )
    return registry.memory_for(EXPERT)


def test_role_memory_is_proposed_by_the_run_and_approved_by_a_person(
    hub: GovernedMemoryHub, roles: RoleSkillMemoryStore
) -> None:
    memory = expert(hub, roles)
    reply = memory.propose(
        {"target_uri": SKILLS, "op": "upsert_file", "content": "Name every seam.\n", "rationale": "review"},
        actor="pool:architect",
    )
    assert reply["status"] == "pending_approval" and reply["proposal_id"].startswith("rprop-")
    with pytest.raises(FileNotFoundError):
        memory.read_on_demand(SKILLS)
    assert roles.get_proposal(reply["proposal_id"]).proposed_by == "pool:architect"
    # A Run cannot approve it, its own least of all.
    with pytest.raises(FileNotFoundError):
        memory.approve(reply["proposal_id"], actor="pool:architect")
    roles.approve_proposal(reply["proposal_id"], reviewed_by="owner")
    assert memory.read_on_demand(SKILLS) == "Name every seam.\n"
    read = json.loads(call_memory_tool(memory, "memory_read", {"uri": SKILLS}, actor="x"))
    assert read["content"] == "Name every seam.\n"
    # Compare-and-set on the version that was read.
    with pytest.raises(ValueError, match="CAS"):
        memory.propose(
            {"target_uri": SKILLS, "op": "upsert_file", "content": "x", "rationale": "r", "base_sha256": "0" * 64},
        )
    added = memory.propose({
        "target_uri": SKILLS, "op": "append", "content": "Draw it.", "rationale": "r", "base_sha256": read["sha256"],
    })
    roles.approve_proposal(added["proposal_id"], reviewed_by="owner")
    assert memory.read_on_demand(SKILLS) == "Name every seam.\n\nDraw it.\n"


def test_writing_role_memory_only_queues_it(hub: GovernedMemoryHub, roles: RoleSkillMemoryStore) -> None:
    memory = expert(hub, roles)
    memory.write(SKILLS, "overwrite")
    assert memory.needs_approval(SKILLS)
    assert [p.content for p in roles.list_proposals(status="pending")] == ["overwrite"]
    assert roles.read_role_doc("architect")[0] == ""


def test_one_role_memory_serves_every_project(hub: GovernedMemoryHub, roles: RoleSkillMemoryStore) -> None:
    roles.approve_proposal(
        roles.propose_skill_write(role_id="architect", content="shared skill", reason="r", proposed_by="x").proposal_id,
        reviewed_by="owner",
    )
    pool = ExpertPoolRegistry()
    for key in ("hibernaut", "lufit"):
        plan = pool.resolve_for_project(role_id="architect", project_key=key).mount_plan
        registry = WorkspacePartitionRegistry(hub.root, hub=hub, mounts=lambda _, plan=plan: ExpertMount(plan, roles))
        assert registry.memory_for(f"proj:{key}:architect").read_on_demand(SKILLS) == "shared skill"


@pytest.mark.parametrize("uri", [
    "proj://lufit/notes/layering.md", "role://sde-backend/skills.md", "shared://elsewhere.md", "kb://other/card.md",
    "shared://notesbook.md",
])
def test_an_expert_reads_nothing_outside_its_mounts(
    hub: GovernedMemoryHub, roles: RoleSkillMemoryStore, uri: str
) -> None:
    memory = expert(hub, roles)
    with pytest.raises(PermissionError):
        memory.read_on_demand(uri)
    with pytest.raises(PermissionError):
        memory.read_document({"uri": uri})


@pytest.mark.parametrize("uri", [
    "proj://workshop/notes/layering.md", "proj://lufit/notes/layering.md", "shared://notes/layering.md",
    "kb://pkb/card.md", "global://instructions.md", "role://sde-backend/skills.md", "role://architect/../x.md",
])
def test_an_expert_writes_only_its_own_project(
    hub: GovernedMemoryHub, roles: RoleSkillMemoryStore, uri: str
) -> None:
    memory = expert(hub, roles)
    with pytest.raises((PermissionError, ValueError)):
        memory.write(uri, "x")
    with pytest.raises((PermissionError, ValueError)):
        memory.propose({"target_uri": uri, "op": "append", "content": "x", "rationale": "r"})
    assert hub.list_proposals(status=None) == [] and roles.list_proposals() == []
    assert "layering note of workshop" in (hub.project_dir("workshop") / "notes" / "layering.md").read_text()


def test_an_expert_reads_its_mounts_and_writes_its_project(
    hub: GovernedMemoryHub, roles: RoleSkillMemoryStore
) -> None:
    memory = expert(hub, roles)
    for uri, text in {
        "proj://workshop/notes/layering.md": "note of workshop", "shared://notes/layering.md": "shared note",
        "kb://pkb/card.md": "card in pkb", "notes/layering.md": "note of hibernaut",
    }.items():
        assert text in memory.read_on_demand(uri)
    memory.write("progress/task.md", "step 1")
    assert memory.read_on_demand("progress/task.md") == "step 1"


def test_a_search_finds_nothing_outside_the_mounts(hub: GovernedMemoryHub, roles: RoleSkillMemoryStore) -> None:
    found = expert(hub, roles).search({"query": "layering", "include_kb": True, "top_k": 50})
    assert {hit["uri"] for hit in found["items"]} == {
        "proj://hibernaut/notes/layering.md", "shared://notes/layering.md", "kb://pkb/card.md",
    }


def test_the_startup_context_carries_the_four_layers_within_budget(
    hub: GovernedMemoryHub, roles: RoleSkillMemoryStore
) -> None:
    (hub.project_dir("hibernaut") / "instructions.md").write_text("project rule\n" + "p" * 4000)
    roles.approve_proposal(
        roles.propose_skill_write(
            role_id="architect", content="skill rule\n" + "s" * 4000, reason="r", proposed_by="x"
        ).proposal_id, reviewed_by="owner",
    )
    memory = expert(hub, roles, budget=2000)
    stable = dict(baseline_segments(memory).stable)
    assert stable["instructions.md"].startswith("project rule") and len(stable["instructions.md"]) <= 900
    assert stable["skills.md"].startswith("skill rule") and len(stable["skills.md"]) <= 500
    assert "TRUNCATED" in stable["skills.md"] and "TRUNCATED" in stable["instructions.md"]
    for line in (
        "`proj://hibernaut/`: read and write", "`role://architect/`: read; changes are proposed",
        "`shared://notes/`: read only", "`kb://pkb/`: read only", "`proj://workshop/`: read only",
    ):
        assert line in stable["mounts.md"]


def test_other_runs_do_not_see_role_memory(hub: GovernedMemoryHub) -> None:
    memory = WorkspacePartitionRegistry(hub.root, hub=hub).memory_for("proj:hibernaut")
    with pytest.raises(ValueError):
        memory.read_on_demand(SKILLS)
    assert [name for name, _ in baseline_segments(memory).stable] == ["instructions.md", "index.md"]


def test_an_expert_inbox_mapped_to_another_project_is_refused(tmp_path: Path) -> None:
    opened = GovernedMemoryHub(MemoryHubConfig(memory_root=str(tmp_path), inbox_project_map={EXPERT: "lufit"}))
    try:
        with pytest.raises(ValueError, match="lufit"):
            expert(opened, open_role_store(opened))
    finally:
        opened.close()


@pytest.mark.parametrize("with_roles", [False, True])
def test_approved_role_memory_is_committed_with_the_rest(tmp_path: Path, with_roles: bool) -> None:
    def git(*args: str) -> str:
        return subprocess.run(
            ["git", "-c", "maintenance.auto=false", *args], cwd=tmp_path, check=True, capture_output=True, text=True,
            env={**os.environ, "GIT_CONFIG_GLOBAL": os.devnull},
        ).stdout

    git("init", "-q")
    git("config", "user.email", "t@example.com")
    git("config", "user.name", "t")
    opened = GovernedMemoryHub(MemoryHubConfig(memory_root=str(tmp_path), auto_git_commit=True))
    try:
        if with_roles:
            store = open_role_store(opened)
            proposal = store.propose_skill_write(role_id="architect", content="x\n", reason="r", proposed_by="a")
            store.approve_proposal(proposal.proposal_id, reviewed_by="owner")
            assert "roles/architect/skills.md" in git("ls-files")
            assert proposal.proposal_id in git("log", "-1", "--format=%s")
        else:
            # The deployment never turned experts on: there is no roles/ to add.
            opened.lay_out("hibernaut")
            from memory_hub import WriteProposal
            queued = opened.propose_write(WriteProposal(
                target_uri="proj://hibernaut/instructions.md", op="append", content="rule", rationale="r"))
            opened.approve_proposal(queued.proposal_id or "", actor="owner")
            assert "projects/hibernaut/instructions.md" in git("ls-files")
    finally:
        opened.close()


def test_a_proposal_under_an_id_of_its_own_is_queued_once_whatever_the_target(hub: GovernedMemoryHub) -> None:
    from memory_hub import WriteProposal
    proposal = WriteProposal(
        target_uri="proj://hibernaut/notes/new.md", op="append", content="a note", rationale="r", force_new=True
    )
    first, again = (hub.queue_proposal(proposal, proposal_id="prop_0123456789ab") for _ in range(2))
    assert (first.status, again.status) == ("pending_approval", "pending_approval")
    # A note would otherwise be written at once.
    assert not (hub.project_dir("hibernaut") / "notes" / "new.md").exists()
    assert [r["proposal_id"] for r in hub.list_proposals()] == ["prop_0123456789ab"]
    assert hub.approve_proposal("prop_0123456789ab", actor="owner").status == "applied"
    with pytest.raises(ValueError):
        hub.queue_proposal(proposal, proposal_id="../x")
    kb = WriteProposal(target_uri="kb://pkb/card.md", op="append", content="x", rationale="r")
    assert hub.queue_proposal(kb, proposal_id="prop_0123456789ac").status == "rejected_readonly"


# --- 3. configuration: the shared pool and what each project drafts from it -------------------------------------


def config_of(tmp_path: Path, **over: Any) -> dict[str, Any]:
    for name in ("hibernaut", "workshop", "home"):
        (tmp_path / name).mkdir(exist_ok=True)
    raw: dict[str, Any] = {
        "runtime": {
            "db_path": str(tmp_path / "hb.db"), "memory_root": str(tmp_path / "memory"), "default_agent": "cli",
            "inbox_bindings": [
                {"pattern": "user:main", "default_agent": "cli", "agents": ["cli"], "workdir": str(tmp_path / "home")},
            ],
        },
        "executors": {"cli": {"command": "true"}},
        "agents": {
            "pool:architect": {"executor": "cli"}, "pool:sde-backend": {"executor": "cli"},
            "steward": {"executor": "cli"},
        },
        "projects": {
            "hibernaut": {
                "repo": "o/hibernaut", "checkout": str(tmp_path / "hibernaut"), "worktree_backend": "git",
                "inbox_id": ["proj:hibernaut", "proj:hibernaut:.+"], "default_agent": "steward",
                "experts": ["architect", "sde-backend"], "expert_readonly_projects": ["workshop"],
                "inbox_bindings": [
                    {"pattern": "proj:hibernaut:.+", "default_agent": "cli", "agents": ["cli"]},
                ],
            },
            "workshop": {
                "repo": "o/workshop", "checkout": str(tmp_path / "workshop"), "worktree_backend": "git",
                "inbox_id": ["proj:workshop", "proj:workshop:.+"], "default_agent": "steward", "experts": ["architect"],
            },
        },
        "collaboration": {"enabled": True},
        "experts": {"architect": {}, "sde-backend": {"max_concurrency": 1, "skills": ["sqlite-wal"]}},
    }
    for key, value in over.items():
        if value is None:
            raw.pop(key)
        else:
            raw[key] = value
    return raw


def with_project(raw: dict[str, Any], **over: Any) -> dict[str, Any]:
    raw["projects"]["hibernaut"] |= over
    return raw


def test_projects_draft_shared_experts_from_one_pool(tmp_path: Path) -> None:
    cfg = parse_config(config_of(tmp_path))
    hibernaut, workshop = cfg.projects["hibernaut"], cfg.projects["workshop"]
    for inbox in ("proj:hibernaut:architect", "proj:hibernaut:architect:d7-memory"):
        binding = hibernaut.route(inbox).binding
        assert binding is not None and (binding.default_agent, binding.agents) == ("pool:architect", ("pool:architect",))
    other = workshop.route("proj:workshop:architect").binding
    assert other is not None and other.default_agent == "pool:architect"
    # The bindings written by hand keep the rest, and a project that has none is as open as before.
    manual = hibernaut.route("proj:hibernaut:anything").binding
    assert manual is not None and manual.default_agent == "cli"
    assert hibernaut.route("proj:hibernaut:architecture").binding is manual
    assert workshop.route("proj:workshop:anything").binding is None
    assert not workshop.route("proj:workshop:architect").has_bindings
    assert workshop.route("proj:workshop:sde-backend").binding is None

    mount = cfg.expert_plan("proj:hibernaut:architect:d7-memory")
    assert mount is not None and (mount.project_key, mount.role_id) == ("hibernaut", "architect")
    assert [(s.uri, s.write_mode) for s in mount.slices] == [
        ("proj://hibernaut/", "rw"), ("role://architect/", "propose"), ("shared://notes/", "ro"), ("kb://pkb/", "ro"),
        ("proj://workshop/", "ro"),
    ]
    assert cfg.expert_plan("proj:hibernaut") is None and cfg.expert_plan("proj:workshop:sde-backend") is None
    profiles = {p.role_id: p for p in cfg.collaboration.experts}
    assert profiles["architect"].display_name == "系统架构师" and profiles["architect"].executor_id == "cli"
    assert profiles["sde-backend"].skills == ("sqlite-wal",)
    # The pool's cap is the agent's, across all projects that draft it.
    assert cfg.agents["pool:sde-backend"].max_concurrency == 1
    assert cfg.agents["pool:architect"].max_concurrency == profiles["architect"].max_concurrency


def test_an_agents_own_cap_wins_over_the_pools(tmp_path: Path) -> None:
    raw = config_of(tmp_path)
    raw["agents"]["pool:sde-backend"]["max_concurrency"] = 4
    assert parse_config(raw).agents["pool:sde-backend"].max_concurrency == 4


def test_the_steward_is_told_which_experts_its_project_drafts(tmp_path: Path) -> None:
    cfg = parse_config(config_of(tmp_path))
    manifest = build_routing_manifest(cfg.projects["hibernaut"])
    assert "`proj:hibernaut:architect`" in manifest and "`pool:architect`" in manifest
    assert manifest.index("proj:hibernaut:architect") < manifest.index("`proj:hibernaut:.+`")
    assert "pool:sde-backend" not in build_routing_manifest(cfg.projects["workshop"]).split("Inboxes")[1]


def test_collaboration_is_off_unless_turned_on(tmp_path: Path) -> None:
    raw = with_project(config_of(tmp_path, collaboration=None, experts=None))
    for project in raw["projects"].values():
        project.pop("experts", None)
        project.pop("expert_readonly_projects", None)
    cfg = parse_config(raw)
    assert not cfg.collaboration.enabled and cfg.expert_plan("proj:hibernaut:architect") is None
    daemon = build_daemon_from_config(cfg)
    try:
        assert daemon.collaboration is None
        assert not (tmp_path / "memory" / "roles").exists()
    finally:
        daemon.close()


def _drop(raw: dict[str, Any], *path: str) -> dict[str, Any]:
    target = raw
    for key in path[:-1]:
        target = target[key]
    target.pop(path[-1])
    return raw


@pytest.mark.parametrize(("change", "message"), [
    (lambda raw: _drop(raw, "collaboration"), "collaboration"),
    (lambda raw: raw | {"collaboration": {"enabled": True, "push_everything": True}}, "push_everything"),
    (lambda raw: raw | {"collaboration": {"enabled": True, "chief_inbox": "proj:hibernaut"}}, "chief_inbox"),
    (lambda raw: raw | {"collaboration": {"enabled": True, "chief_inbox": "user:nobody"}}, "chief_inbox"),
    (lambda raw: raw | {"collaboration": {"enabled": True, "hub": {"listen_port": 70000}}}, "listen_port"),
    (lambda raw: raw | {"collaboration": {"enabled": True, "hub": {"base_path": "hub"}}}, "base_path"),
    (lambda raw: raw | {"collaboration": {"enabled": True, "taskboard": {"mcp_url": "file:///x"}}}, "mcp_url"),
    (lambda raw: raw | {"experts": {"Bad Role": {}}}, "Bad Role"),
    (lambda raw: raw | {"experts": {"architect": {"agent": "nobody"}}}, "nobody"),
    (lambda raw: raw | {"experts": {"architect": {"temperature": 1}}}, "temperature"),
    (lambda raw: raw | {"experts": {"architect": {}, "sde-backend": {"agent": "pool:architect"}}}, "pool:architect"),
    (lambda raw: with_project(raw, experts=["nobody"]), "nobody"),
    (lambda raw: with_project(raw, experts=["architect", "architect"]), "twice"),
    (lambda raw: with_project(raw, expert_readonly_projects=["hibernaut"]), "hibernaut"),
    (lambda raw: with_project(raw, expert_readonly_projects=["nowhere"]), "nowhere"),
    (lambda raw: with_project(raw, inbox_id=["proj:hibernaut"], inbox_bindings=[]), "proj:hibernaut:architect"),
    (lambda raw: with_project(raw, inbox_id=["hb:main", "proj:hibernaut:.+"]), "proj:<"),
    (lambda raw: with_project(raw, inbox_bindings=[{"pattern": "proj:hibernaut:architect", "default_agent": "cli"}]),
     "proj:hibernaut:architect"),
    (lambda raw: raw | {"memory_hub": {"inbox_projects": {"proj:hibernaut:architect": "workshop"}}}, "inbox_projects"),
])
def test_unusable_collaboration_settings_are_refused(tmp_path: Path, change: Any, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        parse_config(change(config_of(tmp_path)))


def test_a_drafted_experts_runs_get_the_mounts(tmp_path: Path) -> None:
    daemon = build_daemon_from_config(parse_config(config_of(tmp_path)))
    try:
        memory = daemon.partitions.memory_for("proj:hibernaut:architect:d7")
        assert "mounts.md" in dict(baseline_segments(memory).stable)
        assert memory.propose({
            "target_uri": SKILLS, "op": "append", "content": "x", "rationale": "r"})["status"] == "pending_approval"
        plain = daemon.partitions.memory_for("proj:hibernaut:other")
        assert "mounts.md" not in dict(baseline_segments(plain).stable)
    finally:
        daemon.close()


# --- 4. the Chief of Staff --------------------------------------------------------------------------------------


def test_a_steward_may_add_the_chief_of_staff_skill() -> None:
    listed = parse_skills("agents.chief", "steward", ["builtin:steward", "builtin:chief-of-staff"], ROOT)
    assert [path.parent.name for path in listed] == ["steward", "chief-of-staff"]
    text = render_skills(listed)
    assert "Thin Global Personal Chief of Staff" in text
    # Left out, a steward is the steward it was.
    assert [path.parent.name for path in parse_skills("agents.s", "steward", None, ROOT)] == ["steward"]


@pytest.mark.parametrize("role", ["sde", "code-reviewer"])
def test_no_other_role_takes_the_chief_of_staff_skill(role: str) -> None:
    with pytest.raises(ValueError, match="'builtin:chief-of-staff' is not available for role"):
        parse_skills("agents.x", role, ["builtin:chief-of-staff"], ROOT)


def test_the_chief_of_staff_serves_user_main_beside_thick_project_stewards(tmp_path: Path) -> None:
    raw = config_of(tmp_path)
    work = tmp_path / "chief"
    work.mkdir()
    raw["agents"]["chief"] = {"role": "steward", "executor": "claude-code",
                              "skills": ["builtin:steward", "builtin:chief-of-staff"]}
    raw["executors"]["claude-code"] = {"harness": "claude-code"}
    raw["runtime"]["inbox_bindings"] = [{
        "pattern": "user:main", "default_agent": "chief", "agents": ["chief"], "workdir": str(work),
        "checkout_access": "readonly", "forward_to": ["proj:hibernaut"],
    }]
    (bound,) = parse_config(raw, base_dir=tmp_path).runtime.inbox_bindings
    assert bound.binding.default_agent == "chief" and bound.forward_to == ("proj:hibernaut",)
