from __future__ import annotations

from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from memory_hub import BaselineBudgetConfig, KnowledgeMountConfig, MemoryHub, MemoryHubConfig, mcp_server


class _FastShutdownServer(ThreadingHTTPServer):
    # A short poll lets both standalone and daemon tests shut down the real server promptly.
    def serve_forever(self, poll_interval: float = 0.01) -> None:
        super().serve_forever(poll_interval)


@pytest.fixture
def fast_memory_http_shutdown(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mcp_server, "ThreadingHTTPServer", _FastShutdownServer)


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
