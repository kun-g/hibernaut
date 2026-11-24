from __future__ import annotations

from typing import Any

from collaboration.models import (
    CloseoutCard,
    DigestCostSummary,
    DigestProjectSummary,
    DigestProposalItem,
    DigestSnapshot,
    RoleMemoryProposal,
    utc_now_iso,
)


class DigestAssembler:
    def assemble(
        self,
        *,
        window_label: str,
        tasks: list[dict[str, Any]],
        runs: list[dict[str, Any]],
        role_proposals: list[RoleMemoryProposal],
        project_proposals: list[dict[str, Any]] | None = None,
        closeouts: list[CloseoutCard] | None = None,
    ) -> DigestSnapshot:
        by_project: dict[str, dict[str, int]] = {}
        for task in tasks:
            pkey = str(task.get("project_key") or "unassigned")
            bucket = by_project.setdefault(
                pkey,
                {"in_progress": 0, "ready": 0, "done_recent": 0, "blocked": 0},
            )
            status = str(task.get("status") or "")
            if task.get("is_blocked"):
                bucket["blocked"] += 1
            elif status == "in_progress":
                bucket["in_progress"] += 1
            elif status == "ready":
                bucket["ready"] += 1
            elif status == "done":
                bucket["done_recent"] += 1

        project_rows = tuple(
            DigestProjectSummary(
                project_key=pkey,
                in_progress=vals["in_progress"],
                ready=vals["ready"],
                done_recent=vals["done_recent"],
                blocked=vals["blocked"],
            )
            for pkey, vals in sorted(by_project.items())
        )

        total_runs = len(runs)
        failed_runs = sum(1 for r in runs if str(r.get("status") or "") == "failed" or r.get("error_kind"))
        total_cost = round(sum(float(r.get("cost_usd") or 0.0) for r in runs), 6)
        cost_summary = DigestCostSummary(
            total_runs=total_runs,
            failed_runs=failed_runs,
            total_cost_usd=total_cost,
        )

        pending_items: list[DigestProposalItem] = []
        for rp in role_proposals:
            if rp.status == "pending":
                pending_items.append(
                    DigestProposalItem(
                        proposal_id=rp.proposal_id,
                        scope="role_skill",
                        target=f"role://{rp.role_id}/{rp.rel_path}",
                        reason=rp.reason,
                        status=rp.status,
                        created_at=rp.created_at,
                    )
                )
        for pp in project_proposals or []:
            if str(pp.get("status") or "pending") == "pending":
                pending_items.append(
                    DigestProposalItem(
                        proposal_id=str(pp["proposal_id"]),
                        scope="project_memory",
                        target=str(pp.get("target_uri") or pp.get("target") or ""),
                        reason=str(pp.get("reason") or ""),
                        status="pending",
                        created_at=str(pp.get("created_at") or ""),
                    )
                )

        recent_closeouts = tuple(
            c.to_dict() for c in (closeouts or []) if c.status in ("draft", "confirmed", "synced")
        )

        headline = (
            f"{len(project_rows)} 个项目活跃 · {total_runs} 次运行（失败 {failed_runs} 次，"
            f"成本 ${total_cost:.4f}）· {len(pending_items)} 条待审提案"
        )
        return DigestSnapshot(
            generated_at=utc_now_iso(),
            window_label=window_label,
            headline=headline,
            projects=project_rows,
            cost_summary=cost_summary,
            pending_proposals=tuple(pending_items),
            recent_closeouts=recent_closeouts,
        )
