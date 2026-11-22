from __future__ import annotations

import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

UnmatchedPolicy = Literal["fallback", "reject"]


@dataclass(frozen=True, slots=True)
class HttpConfig:
    host: str = "127.0.0.1"
    port: int = 8790
    max_body_bytes: int = 1_048_576


@dataclass(frozen=True, slots=True)
class SqliteConfig:
    path: str = "switchboard.db"
    busy_timeout_ms: int = 5000


@dataclass(frozen=True, slots=True)
class RoutingConfig:
    default_scope: str = "user:main"
    unmatched_policy: UnmatchedPolicy = "fallback"
    github_repos: Mapping[str, str] = field(default_factory=lambda: {})
    feishu_chats: Mapping[str, str] = field(default_factory=lambda: {})


@dataclass(frozen=True, slots=True)
class FeishuConfig:
    group_require_mention: bool = True
    bot_open_id: str | None = None
    api_base: str = "https://open.feishu.cn"


@dataclass(frozen=True, slots=True)
class DeliveryConfig:
    retry_backoff_seconds: tuple[float, ...] = (5, 15, 45, 135, 405)
    dead_letter_alert_channel: str | None = None
    dead_letter_alert_target: str | None = None


@dataclass(frozen=True, slots=True)
class OutboundConfig:
    rate_limit_per_target_rpm: int = 30
    max_retry_attempts: int = 5
    retry_backoff_seconds: tuple[float, ...] = (5, 15, 45, 135, 405)
    approval_channel: str = "feishu"
    approval_target: str | None = None
    github_api_base: str = "https://api.github.com"


@dataclass(frozen=True, slots=True)
class SwitchboardConfig:
    http: HttpConfig = field(default_factory=HttpConfig)
    sqlite: SqliteConfig = field(default_factory=SqliteConfig)
    routing: RoutingConfig = field(default_factory=RoutingConfig)
    feishu: FeishuConfig = field(default_factory=FeishuConfig)
    delivery: DeliveryConfig = field(default_factory=DeliveryConfig)
    outbound: OutboundConfig = field(default_factory=OutboundConfig)


_HTTP = HttpConfig()
_SQLITE = SqliteConfig()
_ROUTING = RoutingConfig()
_FEISHU = FeishuConfig()
_DELIVERY = DeliveryConfig()
_OUTBOUND = OutboundConfig()


class ConfigError(ValueError):
    pass


def load_config(path: str | Path) -> SwitchboardConfig:
    with Path(path).open("rb") as handle:
        return parse_config(tomllib.load(handle))


def parse_config(raw: Mapping[str, Any]) -> SwitchboardConfig:
    http = _section(raw, "http")
    sqlite = _section(raw, "sqlite")
    routing = _section(raw, "routing")
    feishu = _section(raw, "feishu")
    delivery = _section(raw, "delivery")
    outbound = _section(raw, "outbound")

    policy = str(routing.get("unmatched_policy", "fallback"))
    if policy not in ("fallback", "reject"):
        raise ConfigError("routing.unmatched_policy must be 'fallback' or 'reject'")

    delivery_backoff = _backoff(delivery, "delivery")
    outbound_backoff = _backoff(outbound, "outbound")
    return SwitchboardConfig(
        http=HttpConfig(
            host=str(http.get("host", _HTTP.host)),
            port=int(http.get("port", _HTTP.port)),
            max_body_bytes=int(http.get("max_body_bytes", _HTTP.max_body_bytes)),
        ),
        sqlite=SqliteConfig(
            path=str(sqlite.get("path", _SQLITE.path)),
            busy_timeout_ms=int(sqlite.get("busy_timeout_ms", _SQLITE.busy_timeout_ms)),
        ),
        routing=RoutingConfig(
            default_scope=str(routing.get("default_scope", _ROUTING.default_scope)),
            unmatched_policy=policy,
            github_repos={k.lower(): str(v) for k, v in _table(routing, "github_repos").items()},
            feishu_chats={k: str(v) for k, v in _table(routing, "feishu_chats").items()},
        ),
        feishu=FeishuConfig(
            group_require_mention=bool(feishu.get("group_require_mention", True)),
            bot_open_id=_optional(feishu.get("bot_open_id")),
            api_base=str(feishu.get("api_base", _FEISHU.api_base)),
        ),
        delivery=DeliveryConfig(
            retry_backoff_seconds=delivery_backoff or _DELIVERY.retry_backoff_seconds,
            dead_letter_alert_channel=_optional(delivery.get("dead_letter_alert_channel")),
            dead_letter_alert_target=_optional(delivery.get("dead_letter_alert_target")),
        ),
        outbound=OutboundConfig(
            rate_limit_per_target_rpm=int(
                outbound.get("rate_limit_per_target_rpm", _OUTBOUND.rate_limit_per_target_rpm)
            ),
            max_retry_attempts=int(
                outbound.get("max_retry_attempts", _OUTBOUND.max_retry_attempts)
            ),
            retry_backoff_seconds=outbound_backoff or _OUTBOUND.retry_backoff_seconds,
            approval_channel=str(outbound.get("approval_channel", _OUTBOUND.approval_channel)),
            approval_target=_optional(outbound.get("approval_target")),
            github_api_base=str(outbound.get("github_api_base", _OUTBOUND.github_api_base)),
        ),
    )


def _section(raw: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    value = raw.get(name, {})
    if not isinstance(value, Mapping):
        raise ConfigError(f"[{name}] must be a table")
    return cast(Mapping[str, Any], value)


def _table(section: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    value = section.get(name, {})
    if not isinstance(value, Mapping):
        raise ConfigError(f"routing.{name} must be a table")
    return cast(Mapping[str, Any], value)


def _backoff(section: Mapping[str, Any], name: str) -> tuple[float, ...]:
    value = section.get("retry_backoff_seconds")
    if value is None:
        return ()
    if not isinstance(value, list) or not value:
        raise ConfigError(f"{name}.retry_backoff_seconds must be a non-empty list")
    return tuple(float(item) for item in cast(list[Any], value))


def _optional(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)
