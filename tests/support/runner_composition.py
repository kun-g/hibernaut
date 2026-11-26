from __future__ import annotations

from pathlib import Path
from typing import Any


INBOX = "proj:hibernaut"
TOKEN_ENV = "HIBERNAUT_RUNNER_TOKEN"


def raw_config(tmp_path: Path, runner: dict[str, Any] | None, **sections: Any) -> dict[str, Any]:
    repo = tmp_path / "repo"
    command = tmp_path / "bin" / "claude"
    raw: dict[str, Any] = {
        "runtime": {
            "db_path": str(tmp_path / "hb.db"), "memory_root": str(tmp_path / "memory"), "max_concurrency": 2,
            "default_agent": "claude", "worktree_sweep_interval_seconds": 0,
        },
        "executors": {
            "claude": {
                "command": str(command), "args": ["-p", "{prompt}", "--output-format", "json"],
                "resume_args": ["-p", "{prompt}", "--resume", "{executor_session_id}", "--output-format", "json"],
                "env": {"FAKE_LOG": str(tmp_path / "cli.log")},
            },
        },
        "projects": {
            "hibernaut": {
                "inbox_id": INBOX, "repo": "kun-g/hibernaut", "checkout": str(repo), "worktree_backend": "git",
            },
        },
    }
    raw.update(sections)
    if runner is not None:
        raw["runner"] = runner
    return raw


def in_process(**kw: Any) -> dict[str, Any]:
    return {"enabled": True, "allow_tools": ["Bash"], **kw}


def relay(**kw: Any) -> dict[str, Any]:
    return {"enabled": True, "mode": "relay", "listen_port": 0, "allow_tools": ["Bash"], **kw}
