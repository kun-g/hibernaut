from __future__ import annotations

from pathlib import Path
from typing import Any


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
