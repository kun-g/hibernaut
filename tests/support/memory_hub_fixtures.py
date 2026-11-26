from __future__ import annotations

from pathlib import Path
from typing import Any

from memory_hub import BaselineBudgetConfig, KnowledgeMountConfig, MemoryHub, MemoryHubConfig


class _RecordingAudit:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, dict[str, Any], str]] = []

    def emit(self, stage: str, outcome: str, detail: dict[str, Any], *, severity: str = "info") -> None:
        self.events.append((stage, outcome, detail, severity))


def _make_hub(
    tmp_path: Path,
    *,
    budget: BaselineBudgetConfig | None = None,
    project_budgets: dict[str, BaselineBudgetConfig] | None = None,
    mounts: tuple[KnowledgeMountConfig, ...] = (),
    audit: _RecordingAudit | None = None,
    auto_git_commit: bool = False,
) -> MemoryHub:
    cfg = MemoryHubConfig(
        memory_root=str(tmp_path / "memory"),
        default_budget=budget or BaselineBudgetConfig(),
        project_budgets=project_budgets or {},
        inbox_project_map={"custom:steward": "hibernaut"},
        mounts=mounts,
        auto_git_commit=auto_git_commit,
    )
    return MemoryHub(cfg, audit=audit)
