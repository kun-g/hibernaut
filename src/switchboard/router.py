from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from switchboard.config import RoutingConfig
from switchboard.envelope import CanonicalEvent

OUTBOUND_CONSUMER: Final = "switchboard.outbound"

DEFAULT_ROUTES: Final[Mapping[str, tuple[str, ...]]] = {
    "work.opened": ("taskboard",),
    "work.updated": ("taskboard",),
    "work.closed": ("taskboard", "hibernaut"),
    "work.ready": ("hibernaut",),
    "conversation.message": ("hibernaut",),
    "approval.decided": ("hibernaut",),
    "execution.reported": ("taskboard", OUTBOUND_CONSUMER),
}


class Router:
    def __init__(self, routes: Mapping[str, tuple[str, ...]] = DEFAULT_ROUTES) -> None:
        self._routes = dict(routes)

    @classmethod
    def from_config(cls, config: RoutingConfig) -> Router:
        return cls({**DEFAULT_ROUTES, **config.event_consumers})

    def resolve(self, event: CanonicalEvent) -> tuple[str, ...]:
        return self._routes.get(event.event_type, ())


class ScopeResolver:
    def __init__(self, config: RoutingConfig) -> None:
        self._config = config
        self._github = {repo.lower(): scope for repo, scope in config.github_repos.items()}

    def github(self, repo: str) -> str | None:
        return self._resolve(self._github.get(repo.lower()))

    def feishu(self, chat_id: str) -> str | None:
        return self._resolve(self._config.feishu_chats.get(chat_id))

    def fallback(self, scope: str | None) -> str:
        return scope or self._config.default_scope

    def _resolve(self, matched: str | None) -> str | None:
        if matched is not None:
            return matched
        if self._config.unmatched_policy == "reject":
            return None
        return self._config.default_scope
