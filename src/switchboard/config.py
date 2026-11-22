from __future__ import annotations

import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

from switchboard.envelope import EVENT_TYPES

UnmatchedPolicy = Literal["fallback", "reject"]
TransportMode = Literal["in_process", "relay", "hybrid"]
TRANSPORT_MODES: tuple[TransportMode, ...] = ("in_process", "relay", "hybrid")


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
    # Replaces the default consumers of the listed event types only.
    event_consumers: Mapping[str, tuple[str, ...]] = field(default_factory=lambda: {})


@dataclass(frozen=True, slots=True)
class FeishuConfig:
    group_require_mention: bool = True
    bot_open_id: str | None = None
    api_base: str = "https://open.feishu.cn"


@dataclass(frozen=True, slots=True)
class GitHubConfig:
    # None accepts every issue, PR and comment of a routed repo.
    require_label: str | None = None


@dataclass(frozen=True, slots=True)
class DeliveryConfig:
    retry_backoff_seconds: tuple[float, ...] = (5, 15, 45, 135, 405)
    dead_letter_alert_channel: str | None = None
    dead_letter_alert_target: str | None = None
    # None: 1 for in_process (Phase 1 behaviour), 2 * relay.max_in_flight otherwise.
    max_parallel: int | None = None


@dataclass(frozen=True, slots=True)
class RelayConfig:
    enabled: bool = False
    path: str = "/ws/relay"
    auth_timeout_sec: float = 5
    heartbeat_sec: float = 20
    ack_timeout_sec: float = 15
    max_in_flight: int = 8
    transport: TransportMode = "in_process"
    # None: the relay endpoint shares the [http] port.
    listen_host: str | None = None
    listen_port: int | None = None


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
    github: GitHubConfig = field(default_factory=GitHubConfig)
    delivery: DeliveryConfig = field(default_factory=DeliveryConfig)
    outbound: OutboundConfig = field(default_factory=OutboundConfig)
    relay: RelayConfig = field(default_factory=RelayConfig)


_HTTP = HttpConfig()
_SQLITE = SqliteConfig()
_ROUTING = RoutingConfig()
_FEISHU = FeishuConfig()
_DELIVERY = DeliveryConfig()
_OUTBOUND = OutboundConfig()
_RELAY = RelayConfig()


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
    max_parallel = delivery.get("max_parallel")
    if max_parallel is not None and int(max_parallel) < 1:
        raise ConfigError("delivery.max_parallel must be at least 1")
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
            event_consumers=_event_consumers(_table(routing, "event_consumers")),
        ),
        feishu=FeishuConfig(
            group_require_mention=bool(feishu.get("group_require_mention", True)),
            bot_open_id=_optional(feishu.get("bot_open_id")),
            api_base=str(feishu.get("api_base", _FEISHU.api_base)),
        ),
        github=GitHubConfig(require_label=_optional(str(_section(raw, "github").get("require_label") or "").strip())),
        delivery=DeliveryConfig(
            retry_backoff_seconds=delivery_backoff or _DELIVERY.retry_backoff_seconds,
            dead_letter_alert_channel=_optional(delivery.get("dead_letter_alert_channel")),
            dead_letter_alert_target=_optional(delivery.get("dead_letter_alert_target")),
            max_parallel=None if max_parallel is None else int(max_parallel),
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
        relay=_relay(_section(raw, "relay")),
    )


def _event_consumers(table: Mapping[str, Any]) -> dict[str, tuple[str, ...]]:
    routes: dict[str, tuple[str, ...]] = {}
    for event_type, consumers in table.items():
        if event_type not in EVENT_TYPES:
            raise ConfigError(f"routing.event_consumers: unknown event type {event_type!r}")
        if not isinstance(consumers, list) or not all(isinstance(c, str) and c for c in cast(list[Any], consumers)):
            raise ConfigError(f"routing.event_consumers.{event_type!r} must be a list of consumer ids")
        routes[event_type] = tuple(cast(list[str], consumers))
    return routes


def _relay(relay: Mapping[str, Any]) -> RelayConfig:
    transport = str(relay.get("transport", _RELAY.transport))
    if transport not in TRANSPORT_MODES:
        raise ConfigError(f"relay.transport must be one of {list(TRANSPORT_MODES)}")
    enabled = bool(relay.get("enabled", _RELAY.enabled))
    if transport != "in_process" and not enabled:
        raise ConfigError(f"relay.transport = {transport!r} needs relay.enabled = true")
    path = str(relay.get("path", _RELAY.path))
    if not path.startswith("/"):
        raise ConfigError("relay.path must start with '/'")
    seconds = {
        name: float(relay.get(name, getattr(_RELAY, name)))
        for name in ("auth_timeout_sec", "heartbeat_sec", "ack_timeout_sec")
    }
    for name, value in seconds.items():
        if value <= 0:
            raise ConfigError(f"relay.{name} must be positive")
    max_in_flight = int(relay.get("max_in_flight", _RELAY.max_in_flight))
    if max_in_flight < 1:
        raise ConfigError("relay.max_in_flight must be at least 1")
    listen_port = relay.get("listen_port")
    return RelayConfig(
        enabled=enabled,
        path=path,
        auth_timeout_sec=seconds["auth_timeout_sec"],
        heartbeat_sec=seconds["heartbeat_sec"],
        ack_timeout_sec=seconds["ack_timeout_sec"],
        max_in_flight=max_in_flight,
        transport=transport,
        listen_host=_optional(relay.get("listen_host")),
        listen_port=None if listen_port is None else int(listen_port),
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
