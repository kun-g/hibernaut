from __future__ import annotations

import math
import os
import re
import socket
import threading
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from ipaddress import IPv6Address
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import urlsplit

from switchboard.envelope import EVENT_TYPES

UnmatchedPolicy = Literal["fallback", "reject"]
TransportMode = Literal["in_process", "relay", "hybrid"]
TRANSPORT_MODES: tuple[TransportMode, ...] = ("in_process", "relay", "hybrid")
_BAD_PATH_CHARS = re.compile(r"[\s?#]")
# Underscores are no DNS syntax, but names from /etc/hosts and container networks use them.
_HOST_LABEL = re.compile(r"[A-Za-z0-9_]([A-Za-z0-9_-]{0,61}[A-Za-z0-9_])?")


@dataclass(frozen=True, slots=True)
class HttpConfig:
    host: str = "127.0.0.1"
    port: int = 8790
    max_body_bytes: int = 1_048_576
    # Every route also answers under this prefix (e.g. "/switchboard"), so a proxy that keeps the path prefix
    # needs no StripPrefix; "" means no prefix.
    base_path: str = ""


@dataclass(frozen=True, slots=True)
class SqliteConfig:
    path: str = "switchboard.db"
    busy_timeout_ms: int = 5000


@dataclass(frozen=True, slots=True)
class RoutingConfig:
    default_scope: str = "user:main"
    unmatched_policy: UnmatchedPolicy = "fallback"
    github_repos: Mapping[str, str] = field(default_factory=lambda: {})
    # owner -> scope template; "{repo}" becomes the lower-cased repository name. github_repos wins.
    github_owners: Mapping[str, str] = field(default_factory=lambda: {})
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
    # Channels whose origin gets an execution report without an explicit reply_channel/reply_target. GitHub is
    # left out: a run's result goes to an issue only when the task named it, not because the issue started it.
    reply_to_origin: tuple[str, ...] = ("feishu",)
    # Accounts the GitHub App may mint comment tokens for; only used when the App credentials are set.
    github_app_owners: tuple[str, ...] = ()
    # Where Taskboard notices (human_todo, upstream_cancelled, dependency_invalidated) go when they name no
    # reply target and their origin takes no replies; unset falls back to approval_channel/approval_target.
    notify_channel: str | None = None
    notify_target: str | None = None


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
    # Drain limit of a shutdown; what is still running then is cut off.
    shutdown_timeout_seconds: float = 10.0


_HTTP = HttpConfig()
_SQLITE = SqliteConfig()
_ROUTING = RoutingConfig()
_FEISHU = FeishuConfig()
_DELIVERY = DeliveryConfig()
_OUTBOUND = OutboundConfig()
_RELAY = RelayConfig()
_SECTIONS = ("http", "sqlite", "routing", "feishu", "github", "delivery", "outbound", "relay")
_MAX_PORT = 65535


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

    for name in _SECTIONS:
        # TOML puts a top-level key written below a table header into that table, where nothing would read it.
        if "shutdown_timeout_seconds" in _section(raw, name):
            raise ConfigError(f"shutdown_timeout_seconds must come before the first table, not inside [{name}]")

    policy = str(routing.get("unmatched_policy", "fallback"))
    if policy not in ("fallback", "reject"):
        raise ConfigError("routing.unmatched_policy must be 'fallback' or 'reject'")

    delivery_backoff = _backoff(delivery, "delivery")
    outbound_backoff = _backoff(outbound, "outbound")
    max_parallel = delivery.get("max_parallel")
    return SwitchboardConfig(
        http=HttpConfig(
            host=_text(http, "http", "host", _HTTP.host),
            port=_integer(http, "http", "port", _HTTP.port, 0, _MAX_PORT),
            max_body_bytes=_integer(http, "http", "max_body_bytes", _HTTP.max_body_bytes, 1),
            base_path=normalize_base_path(http.get("base_path", _HTTP.base_path)),
        ),
        sqlite=SqliteConfig(
            path=_database_path(sqlite),
            busy_timeout_ms=_integer(sqlite, "sqlite", "busy_timeout_ms", _SQLITE.busy_timeout_ms, 0),
        ),
        routing=RoutingConfig(
            default_scope=_text(routing, "routing", "default_scope", _ROUTING.default_scope),
            unmatched_policy=policy,
            github_repos={k.lower(): v for k, v in _scopes(routing, "github_repos").items()},
            github_owners={k.lower(): v for k, v in _scopes(routing, "github_owners").items()},
            feishu_chats=_scopes(routing, "feishu_chats"),
            event_consumers=_event_consumers(_table(routing, "event_consumers")),
        ),
        feishu=FeishuConfig(
            group_require_mention=_boolean(feishu, "feishu", "group_require_mention", _FEISHU.group_require_mention),
            bot_open_id=_optional(feishu.get("bot_open_id")),
            api_base=_url(feishu, "feishu", "api_base", _FEISHU.api_base),
        ),
        github=GitHubConfig(require_label=_optional(str(_section(raw, "github").get("require_label") or "").strip())),
        delivery=DeliveryConfig(
            retry_backoff_seconds=delivery_backoff or _DELIVERY.retry_backoff_seconds,
            dead_letter_alert_channel=_optional(delivery.get("dead_letter_alert_channel")),
            dead_letter_alert_target=_optional(delivery.get("dead_letter_alert_target")),
            max_parallel=None if max_parallel is None else _integer(delivery, "delivery", "max_parallel", 1, 1),
        ),
        outbound=OutboundConfig(
            rate_limit_per_target_rpm=_integer(
                outbound, "outbound", "rate_limit_per_target_rpm", _OUTBOUND.rate_limit_per_target_rpm, 1
            ),
            max_retry_attempts=_integer(outbound, "outbound", "max_retry_attempts", _OUTBOUND.max_retry_attempts, 1),
            retry_backoff_seconds=outbound_backoff or _OUTBOUND.retry_backoff_seconds,
            approval_channel=str(outbound.get("approval_channel", _OUTBOUND.approval_channel)),
            approval_target=_optional(outbound.get("approval_target")),
            github_api_base=_url(outbound, "outbound", "github_api_base", _OUTBOUND.github_api_base),
            github_app_owners=_strings(outbound, "github_app_owners"),
            reply_to_origin=_strings(outbound, "reply_to_origin") if "reply_to_origin" in outbound else _OUTBOUND.reply_to_origin,
            notify_channel=_optional(outbound.get("notify_channel")),
            notify_target=_optional(outbound.get("notify_target")),
        ),
        relay=_relay(_section(raw, "relay")),
        shutdown_timeout_seconds=_shutdown_timeout(raw.get("shutdown_timeout_seconds", 10.0)),
    )


def normalize_base_path(value: object) -> str:
    # Same rules as Taskboard's [http] base_path, so both services can share one host under sub-paths.
    if not isinstance(value, str):
        raise ConfigError(f"http.base_path {value!r} must be a string such as '/switchboard'")
    path = value.strip().rstrip("/")
    if not path:
        return ""
    if not path.startswith("/"):
        path = "/" + path
    if _BAD_PATH_CHARS.search(path) or "//" in path:
        raise ConfigError(f"http.base_path {value!r} must be a plain path such as '/switchboard'")
    return path


# Syntax only, nothing is looked up. Digits and dots that are no IPv4 address are no host name either.
def is_ipv4_or_host_name(host: str) -> bool:
    if re.fullmatch(r"[0-9.]+", host):
        try:
            socket.inet_aton(host)
        except OSError:
            return False
        return True
    try:
        name = host.encode("idna").decode("ascii").removesuffix(".")
    except UnicodeError:
        return False
    return len(name) <= 253 and all(_HOST_LABEL.fullmatch(label) for label in name.split("."))


def _strings(section: Mapping[str, Any], name: str) -> tuple[str, ...]:
    value = section.get(name, [])
    if not isinstance(value, list) or not all(isinstance(v, str) and v.strip() for v in cast(list[Any], value)):
        raise ConfigError(f"{name} must be a list of non-empty strings")
    return tuple(v.strip() for v in cast(list[str], value))


def _event_consumers(table: Mapping[str, Any]) -> dict[str, tuple[str, ...]]:
    routes: dict[str, tuple[str, ...]] = {}
    for event_type, consumers in table.items():
        if event_type not in EVENT_TYPES:
            raise ConfigError(f"routing.event_consumers: unknown event type {event_type!r}")
        if not isinstance(consumers, list) or not all(isinstance(c, str) and c for c in cast(list[Any], consumers)):
            raise ConfigError(f"routing.event_consumers.{event_type!r} must be a list of consumer ids")
        names = cast(list[str], consumers)
        # One delivery row per event and consumer: a repeated one would fail the insert of every such event.
        repeated = sorted({name for name in names if names.count(name) > 1})
        if repeated:
            raise ConfigError(
                f"routing.event_consumers.{event_type!r} lists {', '.join(map(repr, repeated))} more than once"
            )
        routes[event_type] = tuple(names)
    return routes


def _relay(relay: Mapping[str, Any]) -> RelayConfig:
    transport = str(relay.get("transport", _RELAY.transport))
    if transport not in TRANSPORT_MODES:
        raise ConfigError(f"relay.transport must be one of {list(TRANSPORT_MODES)}")
    enabled = _boolean(relay, "relay", "enabled", _RELAY.enabled)
    if transport != "in_process" and not enabled:
        raise ConfigError(f"relay.transport = {transport!r} needs relay.enabled = true")
    path = str(relay.get("path", _RELAY.path))
    if not path.startswith("/"):
        raise ConfigError("relay.path must start with '/'")
    seconds: dict[str, float] = {}
    for name in ("auth_timeout_sec", "heartbeat_sec", "ack_timeout_sec"):
        value = _number(relay.get(name, getattr(_RELAY, name)), f"relay.{name}")
        if not 0 < value <= threading.TIMEOUT_MAX:
            raise ConfigError(f"relay.{name} must be positive")
        seconds[name] = value
    max_in_flight = _integer(relay, "relay", "max_in_flight", _RELAY.max_in_flight, 1)
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
        listen_port=None if listen_port is None else _integer(relay, "relay", "listen_port", 0, 0, _MAX_PORT),
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


def _scopes(routing: Mapping[str, Any], name: str) -> dict[str, str]:
    table = _table(routing, name)
    for key, scope in table.items():
        if not isinstance(scope, str) or not scope.strip():
            raise ConfigError(f"routing.{name}.{key!r} must be a non-empty scope")
    return dict(table)


def _text(section: Mapping[str, Any], table: str, name: str, default: str) -> str:
    value = section.get(name, default)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{table}.{name} must be a non-empty string")
    return value


def _url(section: Mapping[str, Any], table: str, name: str, default: str) -> str:
    value = _text(section, table, name, default)
    try:
        parts = urlsplit(value)
        port = parts.port
        host = parts.hostname or ""
        usable = bool(IPv6Address(host)) if ":" in host else is_ipv4_or_host_name(host)
    except ValueError as exc:
        raise ConfigError(f"{table}.{name} is not a usable URL: {exc}") from exc
    if parts.scheme not in ("http", "https") or not usable or port == 0 or _BAD_PATH_CHARS.search(value):
        raise ConfigError(
            f"{table}.{name} must be an http:// or https:// URL with a valid host and port, and no spaces, query or fragment"
        )
    return value


# Read-only: the file and its directories may not exist yet, but what exists must not rule the path out.
def _database_path(sqlite: Mapping[str, Any]) -> str:
    # "" would open a private temporary database that is gone after a restart.
    path = _text(sqlite, "sqlite", "path", _SQLITE.path)
    if "\0" in path:
        raise ConfigError("sqlite.path must not contain a NUL character")
    if path.endswith(os.sep) or os.path.isdir(path):
        raise ConfigError(f"sqlite.path {path!r} is a directory, not a database file")
    if os.path.exists(path) and not os.path.isfile(path):
        raise ConfigError(f"sqlite.path {path!r} is not a regular file")
    for parent in Path(path).parents:
        if os.path.exists(parent):
            if not os.path.isdir(parent):
                raise ConfigError(f"sqlite.path {path!r} lies below {str(parent)!r}, which is not a directory")
            break
    return path


# 0/1 and numeric strings were read as meant before validation existed and still load; what was misread
# ("false" as true, true as 1, 80.5 as 80) is rejected.
def _boolean(section: Mapping[str, Any], table: str, name: str, default: bool) -> bool:
    value = section.get(name, default)
    if value not in (0, 1):
        raise ConfigError(f"{table}.{name} must be true or false")
    return bool(value)


def _number(value: object, name: str) -> float:
    if isinstance(value, int | float | str) and not isinstance(value, bool):
        try:
            number = float(value)
        except (ValueError, OverflowError):
            pass
        else:
            if math.isfinite(number):
                return number
    raise ConfigError(f"{name} must be a finite number, got {value!r}")


def _integer(
    section: Mapping[str, Any], table: str, name: str, default: int, minimum: int, maximum: int | None = None
) -> int:
    value = _number(section.get(name, default), f"{table}.{name}")
    bounds = f"at least {minimum}" if maximum is None else f"between {minimum} and {maximum}"
    if not value.is_integer() or value < minimum or (maximum is not None and value > maximum):
        raise ConfigError(f"{table}.{name} must be an integer, {bounds}")
    return int(value)


def _shutdown_timeout(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float) or not 0 < value <= threading.TIMEOUT_MAX:
        raise ConfigError(f"shutdown_timeout_seconds must be a positive number of seconds, got {value!r}")
    return float(value)


def _backoff(section: Mapping[str, Any], name: str) -> tuple[float, ...]:
    value = section.get("retry_backoff_seconds")
    if value is None:
        return ()
    if not isinstance(value, list) or not value:
        raise ConfigError(f"{name}.retry_backoff_seconds must be a non-empty list")
    delays = tuple(_number(item, f"{name}.retry_backoff_seconds") for item in cast(list[Any], value))
    if any(not 0 <= delay <= threading.TIMEOUT_MAX for delay in delays):
        raise ConfigError(f"{name}.retry_backoff_seconds must not contain negative or unusably large delays")
    return delays


def _optional(value: Any) -> str | None:
    if value is None or value == "":
        return None
    return str(value)
