from __future__ import annotations

import contextlib
import json
import os
import tomllib
from collections.abc import Callable
from pathlib import Path
from typing import Any

from agent_runtime.adapters.delivery.pr_delivery import remote_env
from agent_runtime.adapters.executor_cli.steward_git import ToolError
from agent_runtime.app.config_edit import ConfigChanged, ConfigFile
from agent_runtime.app.onboarding import (
    OnboardError,
    Onboarding,
    Report,
    Request,
    Runner,
    description_text,
    project_name,
    run_command,
    scrub,
    split_repo,
)
from agent_runtime.app.reload import RELOAD_TIMEOUT_SECONDS, ReloadFailed, ReloadStatus


class SystemToolbox:
    # The system tools of one system inbox. Only named values come in, and only what came of them goes out.
    def __init__(
        self,
        *,
        # It also says which file that is; without hot reload there is none, and no tool works.
        reloads: ReloadStatus,
        # Whether the configuration in force still gives this inbox the tools.
        allowed: Callable[[], bool],
        run: Runner | None = None,
        reload_timeout: float = RELOAD_TIMEOUT_SECONDS,
    ) -> None:
        self._reloads = reloads
        self._allowed = allowed
        self._reload_timeout = reload_timeout
        # Tokens in the daemon's environment reach no command: gh and git use the host's own credentials.
        self._run: Runner = run or (lambda argv: run_command(argv, remote_env(os.environ)))

    def _onboarding(self) -> Onboarding:
        path = self._reloads.path
        if path is None:
            raise ToolError("this process does not reload its configuration, so nothing is done through it")
        return Onboarding(path, run=self._run, write=self._write,
                          label_owners=lambda cfg: cfg.onboarding.label_owners)

    def check(self) -> str:
        def found() -> dict[str, Any]:
            return {"projects": [
                {"name": project.name, "repo": project.repo, "main_inbox": project.inbox_id, "missing": gaps.missing,
                 "by_hand": gaps.by_hand}
                for project, gaps in self._onboarding().check()
            ]}

        return self._answer(found)

    def onboard(self, *, repo: str, name: str | None, description: str | None) -> str:
        def done() -> dict[str, Any]:
            split_repo(repo)
            project, onboarding = project_name(name, repo), self._onboarding()
            return self._shown(onboarding.onboard(Request(
                repo=repo, name=project, checkout=onboarding.default_checkout(project),
                description=description_text(description, repo),
            )))

        return self._answer(done)

    def complete(self, *, name: str, description: str | None) -> str:
        return self._answer(lambda: self._shown(self._onboarding().complete(name, description)))

    def hides(self, path: Path) -> bool:
        config = self._reloads.path
        return config is not None and ConfigFile(config).owns(path)

    def _answer(self, work: Callable[[], dict[str, Any]]) -> str:
        if not self._allowed():
            raise ToolError("this inbox has no system tools any more")
        try:
            return json.dumps(work(), ensure_ascii=False, indent=1)
        except (OnboardError, OSError) as exc:
            raise ToolError(self._scrubbed(str(exc))) from exc

    def _scrubbed(self, text: str) -> str:
        raw: dict[str, Any] = {}
        path = self._reloads.path
        if path is not None:
            with contextlib.suppress(OSError, ValueError):
                raw = tomllib.loads(path.read_text(encoding="utf-8"))
        return scrub(text, raw)

    def _shown(self, report: Report) -> dict[str, Any]:
        shown: dict[str, Any] = {"done": report.done, "left_to_a_person": report.by_hand}
        if report.backup is None:
            return {**shown, "configuration": "not changed"}
        return {**shown, "configuration": "changed and in force", "added": report.added, "backup": str(report.backup),
                "undo": f"cp {report.backup} {report.config} (as long as nobody changed it since)"}

    def _write(self, file: ConfigFile, original: bytes, updated: bytes) -> Path:
        # Only a version the running Hibernaut took up stays.
        if not self._allowed():
            raise OnboardError("this inbox has no system tools any more")
        backup = file.replace(original, updated)
        reason = ""
        try:
            self._reloads.wait(updated, self._reload_timeout)
        except ReloadFailed as exc:
            reason = str(exc)
        # An editor knows no lock: what is in force counts only while it is what the file says.
        try:
            current: bytes | None = file.read()
        except OSError:
            current = None
        if current != updated:
            raise OnboardError(
                f"{file.path.name} was changed by somebody else before the new configuration was confirmed; their version "
                "stays and nothing was put back. Whether the running Hibernaut took the change up for a while is not "
                f"known; the copy from before is {backup}", config_kept=False,
            )
        if not reason:
            return backup
        try:
            file.replace(updated, original, backup=False)
        except (ConfigChanged, OSError) as exc:
            raise OnboardError(
                f"the new configuration was not taken up ({reason}), and was not put back ({exc}); the copy from before "
                f"is {backup}", config_kept=False,
            ) from exc
        try:
            self._reloads.wait(original, self._reload_timeout)
        except ReloadFailed as exc:
            # The same bytes as before are in place; only their reload is unconfirmed.
            raise OnboardError(f"the new configuration was not taken up ({reason}); it was put back, which is not "
                               f"confirmed yet ({exc})") from exc
        raise OnboardError(f"the new configuration was not taken up ({reason}); it was put back")
