from __future__ import annotations

import ast
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from memory_hub import (
    BaselineBudgetConfig,
    KnowledgeMountConfig,
    LegacyMemoryPortAdapter,
    PostMortemCandidate,
    SearchQuery,
    WriteProposal,
    parse_memory_uri,
    resolve_project_key,
    validate_against_schema,
)
from tests.support.memory_hub_fixtures import _RecordingAudit, _make_hub


REPO_PKG_ROOT = Path(__file__).resolve().parents[2]
SCHEMAS_DIR = REPO_PKG_ROOT / "schemas" / "v1"


def test_q1_project_key_sharing_and_prompt_cache_segmentation(tmp_path: Path) -> None:
    hub = _make_hub(tmp_path)
    try:
        assert hub.resolve_project_key("proj:hibernaut") == "hibernaut"
        assert hub.resolve_project_key("hibernaut:issue-42") == "hibernaut"
        assert hub.resolve_project_key("custom:steward") == "hibernaut"

        hub.set_global_instructions("# Global Rule\nNever force push.")
        prop = hub.propose_write(
            WriteProposal(
                target_uri="proj://hibernaut/instructions.md",
                op="append",
                content="- Run pytest before PR.",
                rationale="project rule",
            )
        )
        assert prop.status == "pending_approval" and prop.proposal_id
        approved = hub.approve_proposal(prop.proposal_id, actor="human:kun")
        assert approved.status == "applied"

        b1 = hub.load_baseline("hibernaut")
        validate_against_schema(b1.to_dict(), SCHEMAS_DIR / "memory_baseline.schema.json")
        assert len(b1.stable_prefix) == 3
        assert len(b1.volatile_tail) == 1
        assert b1.volatile_tail[0].uri == "proj://hibernaut/reported.md"

        # Updating reported.md must NOT change stable_prefix_sha256
        rep = hub.propose_write(
            WriteProposal(
                target_uri="proj://hibernaut/reported.md",
                op="append_reported",
                content="Delivered PR #58 summary.",
                rationale="emit",
            )
        )
        assert rep.status == "applied"
        b2 = hub.load_baseline("hibernaut")
        assert b2.stable_prefix_sha256 == b1.stable_prefix_sha256
        assert b2.volatile_tail[0].sha256 != b1.volatile_tail[0].sha256

        # Legacy adapter shares project instructions across multiple inbox_ids
        a1 = LegacyMemoryPortAdapter(hub, "hibernaut:issue-42")
        a2 = LegacyMemoryPortAdapter(hub, "proj:hibernaut")
        assert "Run pytest before PR." in a1.load_baseline()["instructions.md"]
        assert a1.load_baseline()["instructions.md"] == a2.load_baseline()["instructions.md"]
    finally:
        hub.close()


def test_q1_configurable_budget_truncation_and_audit(tmp_path: Path) -> None:
    audit = _RecordingAudit()
    hub = _make_hub(
        tmp_path,
        project_budgets={
            "tight": BaselineBudgetConfig(
                global_instructions=20,
                project_instructions=30,
                project_index=20,
                project_reported=20,
                total_max_tokens=80,
            )
        },
        audit=audit,
    )
    try:
        hub.set_global_instructions("A" * 300)
        bundle = hub.load_baseline("tight")
        assert "global://instructions.md" in bundle.truncated_blocks
        assert bundle.stable_prefix[0].truncated is True
        assert bundle.total_est_tokens <= 80
        assert any(stage == "memory.budget_exceeded" and sev == "warn" for stage, _, _, sev in audit.events)
    finally:
        hub.close()


def test_q2_four_tier_write_policy_cas_dedup_and_approval(tmp_path: Path) -> None:
    hub = _make_hub(tmp_path)
    try:
        # Level 0: global & kb strictly readonly
        r_global = hub.propose_write(
            WriteProposal(target_uri="global://instructions.md", op="append", content="hack", rationale="test")
        )
        assert r_global.status == "rejected_readonly"

        r_kb = hub.propose_write(
            WriteProposal(target_uri="kb://personal/notes/a.md", op="append", content="hack", rationale="test")
        )
        assert r_kb.status == "rejected_readonly"

        # Level 2: notes/* auto-write + index.md registration + dedup detection
        r_note = hub.propose_write(
            WriteProposal(
                target_uri="proj://hibernaut/notes/sqlite_wal.md",
                op="upsert_file",
                content="# SQLite WAL\n\n## Lock Handling\nUse busy_timeout=5000 and single writer lock.\n",
                rationale="Document SQLite WAL concurrency rule",
            )
        )
        assert r_note.status == "applied" and r_note.new_sha256
        idx_doc = hub.read_on_demand("proj://hibernaut/index.md")
        assert "notes/sqlite_wal.md" in idx_doc.content

        # Duplicate content triggers duplicate_warning
        r_dup = hub.propose_write(
            WriteProposal(
                target_uri="proj://hibernaut/notes/sqlite_wal.md",
                op="append",
                content="Use busy_timeout=5000 and single writer lock.",
                rationale="repeat",
            )
        )
        assert r_dup.status == "duplicate_warning"
        assert "proj://hibernaut/notes/sqlite_wal.md" in r_dup.duplicate_candidates

        # CAS mismatch returns conflict
        r_cas = hub.propose_write(
            WriteProposal(
                target_uri="proj://hibernaut/notes/sqlite_wal.md",
                op="append",
                content="New detail.",
                rationale="update",
                base_sha256="deadbeef",
                force_new=True,
            )
        )
        assert r_cas.status == "conflict"

        # Level 1: instructions.md requires approval; mid-flight change caught at approve time
        p1 = hub.propose_write(
            WriteProposal(
                target_uri="proj://hibernaut/instructions.md",
                op="append",
                content="- Always pin schema_version.",
                rationale="ADR-0001 compliance",
            )
        )
        validate_against_schema(
            WriteProposal(
                target_uri="proj://hibernaut/instructions.md",
                op="append",
                content="- Always pin schema_version.",
                rationale="ADR-0001 compliance",
            ).to_dict(),
            SCHEMAS_DIR / "memory_proposal.schema.json",
        )
        assert p1.status == "pending_approval" and p1.proposal_id
        assert len(hub.list_proposals(status="pending")) == 1
        ok = hub.approve_proposal(p1.proposal_id, actor="human:kun")
        assert ok.status == "applied"
        assert "Always pin schema_version." in hub.read_on_demand("proj://hibernaut/instructions.md").content
    finally:
        hub.close()


def test_q2_mechanical_gc_and_dreaming_proposal(tmp_path: Path) -> None:
    hub = _make_hub(
        tmp_path,
        project_budgets={
            "hibernaut": BaselineBudgetConfig(
                global_instructions=1500,
                project_instructions=2500,
                project_index=1500,
                project_reported=40,
                total_max_tokens=6500,
            )
        },
    )
    try:
        # Overflow reported.md -> rolls into notes/reported_archive/
        for i in range(6):
            hub.propose_write(
                WriteProposal(
                    target_uri="proj://hibernaut/reported.md",
                    op="append_reported",
                    content=f"Completed task item #{i} with verification details and PR link.",
                    rationale="report",
                )
            )
        archives = list((hub.root / "projects" / "hibernaut" / "notes" / "reported_archive").glob("*.md"))
        assert len(archives) == 1

        # Create an old closed progress file and a dangling index link
        prog = hub.root / "projects" / "hibernaut" / "progress" / "issue-99.md"
        prog.write_text("# Progress 99\nDone.\n", encoding="utf-8")
        old_ts = (datetime.now(timezone.utc) - timedelta(days=10)).timestamp()
        os.utime(prog, (old_ts, old_ts))

        idx = hub.root / "projects" / "hibernaut" / "index.md"
        idx.write_text("- `notes/deleted_ghost.md` | ghost\n", encoding="utf-8")

        # Create an expired proposal (> 14 days old)
        p_res = hub.propose_write(
            WriteProposal(
                target_uri="proj://hibernaut/instructions.md",
                op="append",
                content="- Temporary rule.",
                rationale="test expiry",
            )
        )
        assert p_res.proposal_id
        p_file = hub.root / "proposals" / f"{p_res.proposal_id}.json"
        rec = json.loads(p_file.read_text(encoding="utf-8"))
        rec["created_at"] = (datetime.now(timezone.utc) - timedelta(days=20)).isoformat().replace("+00:00", "Z")
        p_file.write_text(json.dumps(rec), encoding="utf-8")

        report = hub.run_mechanical_gc("hibernaut", closed_work_keys={"issue-99"})
        assert "progress/issue-99.md" in report.archived_progress_files
        assert report.pruned_index_links == 1
        assert p_res.proposal_id in report.expired_proposals

        # Dreaming only emits pending approval proposal
        dream = hub.submit_dreaming_proposal(
            "hibernaut",
            distilled_rule="- Prefer SQLite WAL with busy_timeout=5000.",
            rationale="Distilled from 3 notes",
            supersedes=("proj://hibernaut/notes/sqlite_wal.md",),
        )
        assert dream.status == "pending_approval"
    finally:
        hub.close()


def test_q3_dual_fts5_cjk_english_lazy_sync_and_rebuild(tmp_path: Path) -> None:
    kb_root = tmp_path / "personal_kb"
    (kb_root / "notes").mkdir(parents=True)
    (kb_root / "raw").mkdir(parents=True)
    (kb_root / "notes" / "sqlite_tips.md").write_text(
        "# SQLite 并发经验\n\n## 租约与锁\n开启 WAL 模式并设置单写锁避免数据库忙。\n",
        encoding="utf-8",
    )
    (kb_root / "raw" / "ignored.md").write_text("# Raw\n租约与锁 应该被忽略。\n", encoding="utf-8")
    (kb_root / "notes" / "huge.md").write_text("x" * 400_000, encoding="utf-8")

    mount = KnowledgeMountConfig(
        name="personal",
        root=str(kb_root),
        include_globs=("notes/**/*.md",),
        exclude_globs=(".git/**", "raw/**"),
        max_file_bytes=262144,
    )
    hub = _make_hub(tmp_path, mounts=(mount,))
    try:
        hub.propose_write(
            WriteProposal(
                target_uri="proj://hibernaut/notes/lock_design.md",
                op="upsert_file",
                content="# 租约锁设计\n\n## 单活租约\n同 inbox_id 串行消费，防止去重竞态。\n",
                rationale="lock design",
            )
        )

        # 2-char Chinese word search ("去重" / "租约") must match
        res_proj = hub.search(SearchQuery(scope="hibernaut", query="去重 租约", include_kb=False))
        validate_against_schema(res_proj.to_dict(), SCHEMAS_DIR / "memory_search.schema.json")
        assert len(res_proj.items) == 1
        assert res_proj.items[0].uri == "proj://hibernaut/notes/lock_design.md"
        assert res_proj.items[0].readonly is False

        # External manual edit picked up via lazy stat sync
        manual = hub.root / "projects" / "hibernaut" / "notes" / "manual_edit.md"
        manual.write_text("# 手工笔记\n\n## 特殊标识\nconn_epoch 防脑裂机制。\n", encoding="utf-8")
        res_manual = hub.search(SearchQuery(scope="hibernaut", query="conn_epoch 防脑裂"))
        assert any(h.uri == "proj://hibernaut/notes/manual_edit.md" for h in res_manual.items)

        # Heading-scoped read_on_demand
        sec_doc = hub.read_on_demand("proj://hibernaut/notes/manual_edit.md", heading="## 特殊标识")
        assert "conn_epoch" in sec_doc.content

        # KB search explicitly enabled: hits notes/sqlite_tips.md, skips raw/ & huge.md
        res_kb = hub.search(SearchQuery(scope="hibernaut", query="租约 WAL", include_kb=True))
        uris = [h.uri for h in res_kb.items]
        assert "kb://personal/notes/sqlite_tips.md" in uris
        assert "kb://personal/raw/ignored.md" not in uris
        assert "kb://personal/notes/huge.md" not in uris

        # Delete derived DBs and rebuild 100% from Markdown
        hub.close()
        idx_dir = hub.root / ".index"
        for db in idx_dir.glob("*.db*"):
            db.unlink()
        hub = _make_hub(tmp_path, mounts=(mount,))
        stats = hub.rebuild_index(include_kb=True)
        assert stats.indexed_files >= 2
        assert stats.kb_indexed_files == 1
        res_after = hub.search(SearchQuery(scope="hibernaut", query="conn_epoch"))
        assert len(res_after.items) == 1
    finally:
        hub.close()


def test_q4_postmortem_two_stage_gate(tmp_path: Path) -> None:
    hub = _make_hub(tmp_path)
    try:
        # Transient error strictly rejected
        c_trans = PostMortemCandidate(
            project_key="hibernaut",
            run_id="run_01",
            error_kind="rate_limit",
            root_cause="429 too many requests",
            proposed_rule="Retry later",
            recurrence_count=5,
        )
        validate_against_schema(c_trans.to_dict(), SCHEMAS_DIR / "memory_postmortem.schema.json")
        r_trans = hub.record_postmortem(c_trans)
        assert r_trans.status == "rejected_transient"

        # Deterministic error recurrence=1 -> staged only
        r1 = hub.record_postmortem(
            PostMortemCandidate(
                project_key="hibernaut",
                run_id="run_02",
                error_kind="tool_error",
                root_cause="Missing PYTHONPATH when running pytest",
                proposed_rule="Always invoke pytest with PYTHONPATH=src.",
                recurrence_count=1,
                slug="pytest-pythonpath",
            )
        )
        assert r1.status == "staged"
        assert r1.proposal_id is None
        assert r1.note_uri and "notes/postmortem/" in r1.note_uri

        # Deterministic error recurrence=2 -> staged AND opens approval proposal (not auto-written)
        r2 = hub.record_postmortem(
            PostMortemCandidate(
                project_key="hibernaut",
                run_id="run_03",
                error_kind="tool_error",
                root_cause="Missing PYTHONPATH when running pytest",
                proposed_rule="Always invoke pytest with PYTHONPATH=src.",
                recurrence_count=2,
                slug="pytest-pythonpath",
            )
        )
        assert r2.status == "staged_and_proposed"
        assert r2.proposal_id is not None
        assert "PYTHONPATH=src" not in hub.read_on_demand("proj://hibernaut/instructions.md").content
        hub.approve_proposal(r2.proposal_id, actor="human:kun")
        assert "PYTHONPATH=src" in hub.read_on_demand("proj://hibernaut/instructions.md").content
    finally:
        hub.close()


def test_architecture_zero_third_party_and_zero_business_deps() -> None:
    forbidden = {"agent_runtime", "switchboard", "taskboard", "runner", "ledger", "workshop"}
    stdlib = set(sys.stdlib_module_names) | {"memory_hub"}
    src_dir = REPO_PKG_ROOT / "src" / "memory_hub"
    for py_file in sorted(src_dir.glob("*.py")):
        tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    top = alias.name.split(".")[0]
                    assert top not in forbidden, f"{py_file.name} imports forbidden module {top}"
                    assert top in stdlib, f"{py_file.name} imports non-stdlib module {top}"
            elif isinstance(node, ast.ImportFrom) and node.module:
                top = node.module.split(".")[0]
                assert top not in forbidden, f"{py_file.name} imports forbidden module {top}"
                assert top in stdlib, f"{py_file.name} imports non-stdlib module {top}"


def test_uri_path_traversal_rejected() -> None:
    with pytest.raises(ValueError):
        parse_memory_uri("proj://hibernaut/../../etc/passwd")
    with pytest.raises(ValueError):
        resolve_project_key("../escape")
