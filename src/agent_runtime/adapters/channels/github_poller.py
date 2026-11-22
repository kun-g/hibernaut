from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from agent_runtime.adapters.channels.github_issue import task_fields
from agent_runtime.engine.handler import EventHandler

PollMode = Literal["gh", "api"]
IssueFetcher = Callable[["PollTarget"], list[dict[str, Any]]]

_log = logging.getLogger(__name__)
_GH_TIMEOUT_SECONDS = 60.0
_DETAIL_LIMIT = 500


@dataclass(frozen=True, slots=True)
class PollTarget:
    repo: str
    inbox_id: str | None = None
    labels: tuple[str, ...] = ()


class GitHubPollError(Exception):
    def __init__(self, failed_repos: Sequence[str], ingested: int) -> None:
        super().__init__(f"poll failed for {', '.join(failed_repos)}")
        self.failed_repos = tuple(failed_repos)
        self.ingested = ingested


def describe_poll_error(exc: BaseException) -> str:
    if isinstance(exc, subprocess.CalledProcessError):
        stderr = exc.stderr if isinstance(exc.stderr, str) else ""
        detail = " ".join(stderr.split())[:_DETAIL_LIMIT] or "no stderr"
        return f"gh exit {exc.returncode}: {detail}"
    if isinstance(exc, subprocess.TimeoutExpired):
        return f"gh timed out after {exc.timeout:g}s"
    if isinstance(exc, urllib.error.HTTPError):
        try:
            body = exc.read().decode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            body = ""
        detail = " ".join(body.split())[:_DETAIL_LIMIT]
        return f"HTTP {exc.code} {exc.reason}: {detail}".rstrip(": ")
    if isinstance(exc, urllib.error.URLError):
        return f"network error: {exc.reason}"
    return f"{type(exc).__name__}: {exc}"


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
        retry_initial_seconds: float = 5.0,
        retry_max_seconds: float = 300.0,
    ) -> None:
        self._handler = handler
        self._targets = tuple(targets)
        self._mode: PollMode = mode
        self._github_token_env = github_token_env
        self._repo_inbox_map: dict[str, str] = dict(repo_inbox_map or {})
        self._fetcher = fetcher
        self._retry_initial_seconds = retry_initial_seconds
        self._retry_max_seconds = retry_max_seconds

    def retry_delay(self, consecutive_failures: int) -> float:
        exponent = min(max(consecutive_failures - 1, 0), 32)
        return min(self._retry_initial_seconds * (2**exponent), self._retry_max_seconds)

    def poll_once(self) -> int:
        ingested = 0
        failed: list[str] = []
        for target in self._targets:
            # One unreachable repo must not starve the other targets of polling.
            try:
                issues = self._fetch_issues(target)
            except Exception as exc:  # noqa: BLE001
                _log.warning(
                    "GitHub poll fetch failed for %s (mode=%s): %s",
                    target.repo,
                    self._mode,
                    describe_poll_error(exc),
                )
                failed.append(target.repo)
                continue
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
                        **task_fields(issue),
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
        if failed:
            raise GitHubPollError(failed, ingested)
        return ingested

    async def run_forever(
        self,
        interval_seconds: float = 60.0,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        stopper = stop_event or asyncio.Event()
        failures = 0
        while not stopper.is_set():
            delay = interval_seconds
            try:
                await asyncio.to_thread(self.poll_once)
            except Exception as exc:  # noqa: BLE001
                # Transient gh/network/API failures must never escape: the daemon's TaskGroup would cancel /events.
                failures += 1
                delay = self.retry_delay(failures)
                if isinstance(exc, GitHubPollError):
                    _log.warning(
                        "GitHub poll failed for %s (failure #%d); retry in %.1fs",
                        ", ".join(exc.failed_repos),
                        failures,
                        delay,
                    )
                else:
                    _log.exception("GitHub poll failed (failure #%d); retry in %.1fs", failures, delay)
            else:
                if failures:
                    _log.warning("GitHub poll recovered after %d consecutive failure(s)", failures)
                failures = 0
            try:
                await asyncio.wait_for(stopper.wait(), timeout=delay)
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
        proc = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=_GH_TIMEOUT_SECONDS)
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
