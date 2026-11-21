from __future__ import annotations

import asyncio
import json
import os
import subprocess
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from agent_runtime.engine.handler import EventHandler

PollMode = Literal["gh", "api"]
IssueFetcher = Callable[["PollTarget"], list[dict[str, Any]]]


@dataclass(frozen=True, slots=True)
class PollTarget:
    repo: str
    inbox_id: str | None = None
    labels: tuple[str, ...] = ()


class GitHubIssuePoller:
    def __init__(
        self,
        handler: EventHandler,
        *,
        targets: Sequence[PollTarget],
        mode: PollMode = "gh",
        github_token_env: str = "GITHUB_TOKEN",
        repo_inbox_map: Mapping[str, str] | None = None,
        fetcher: IssueFetcher | None = None,
    ) -> None:
        self._handler = handler
        self._targets = tuple(targets)
        self._mode: PollMode = mode
        self._github_token_env = github_token_env
        self._repo_inbox_map: dict[str, str] = dict(repo_inbox_map or {})
        self._fetcher = fetcher

    def poll_once(self) -> int:
        ingested = 0
        for target in self._targets:
            issues = self._fetch_issues(target)
            inbox_id = (
                target.inbox_id
                or self._repo_inbox_map.get(target.repo)
                or f"proj:{target.repo}"
            )
            for issue in issues:
                # GitHub REST /issues endpoint includes PRs with a 'pull_request' key; skip them.
                if "pull_request" in issue and self._mode == "api":
                    continue
                number = issue.get("number")
                if number is None:
                    continue
                updated_at = str(issue.get("updatedAt") or issue.get("updated_at") or "")
                external_id = f"github-poll:{target.repo}#{number}:{updated_at}"
                correlation_key = f"github:{target.repo}#{number}"

                result = self._handler.ingest(
                    inbox_id=inbox_id,
                    source="github:issue_poll",
                    external_id=external_id,
                    correlation_key=correlation_key,
                    payload={
                        "repository": {"full_name": target.repo},
                        "issue": {
                            "number": number,
                            "title": issue.get("title", ""),
                            "body": issue.get("body", ""),
                            "url": issue.get("url") or issue.get("html_url", ""),
                            "labels": issue.get("labels", []),
                            "updated_at": updated_at,
                        },
                    },
                )
                if not result.duplicate:
                    ingested += 1
        return ingested

    async def run_forever(
        self,
        interval_seconds: float = 60.0,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        stopper = stop_event or asyncio.Event()
        while not stopper.is_set():
            await asyncio.to_thread(self.poll_once)
            try:
                await asyncio.wait_for(stopper.wait(), timeout=interval_seconds)
            except asyncio.TimeoutError:
                continue

    def _fetch_issues(self, target: PollTarget) -> list[dict[str, Any]]:
        if self._fetcher is not None:
            return self._fetcher(target)
        if self._mode == "api":
            return self._fetch_via_rest_api(target)
        return self._fetch_via_gh_cli(target)

    def _fetch_via_gh_cli(self, target: PollTarget) -> list[dict[str, Any]]:
        cmd = [
            "gh",
            "issue",
            "list",
            "--repo",
            target.repo,
            "--state",
            "open",
            "--json",
            "number,title,body,labels,updatedAt,url",
        ]
        for label in target.labels:
            cmd.extend(["--label", label])
        proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
        raw = proc.stdout.strip()
        if not raw:
            return []
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, list) else []

    def _fetch_via_rest_api(self, target: PollTarget) -> list[dict[str, Any]]:
        params: dict[str, str] = {"state": "open", "per_page": "50"}
        if target.labels:
            params["labels"] = ",".join(target.labels)
        query = urllib.parse.urlencode(params)
        url = f"https://api.github.com/repos/{target.repo}/issues?{query}"
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "Hibernaut-Poller",
        }
        token = os.environ.get(self._github_token_env)
        if token:
            headers["Authorization"] = f"Bearer {token}"
        req = urllib.request.Request(url=url, headers=headers, method="GET")
        with urllib.request.urlopen(req, timeout=20.0) as resp:  # noqa: S310
            parsed = json.loads(resp.read().decode("utf-8"))
        return parsed if isinstance(parsed, list) else []
