from __future__ import annotations

import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final, cast
from urllib.parse import urlsplit

from switchboard.credentials import EnvCredentialBroker

RELAY_TOKEN_ENV: Final = "SWITCHBOARD_RELAY_TOKEN"
SWITCHBOARD_MCP_TOKEN_ENV: Final = "SWITCHBOARD_MCP_TOKEN"
TASKBOARD_MCP_TOKEN_ENV: Final = "TASKBOARD_MCP_TOKEN"

_BAD_PATH_CHARS: Final = re.compile(r"[\s?#]")


class ConfigError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class HttpConfig:
    host: str = "127.0.0.1"
    port: int = 8791
    # "" or "/prefix" without a trailing slash; see adapters/http_server.py for how both are served.
    base_path: str = ""
    max_body_bytes: int = 1_048_576


@dataclass(frozen=True, slots=True)
class SqliteConfig:
    path: str = "taskboard.db"
    busy_timeout_ms: int = 5000


@dataclass(frozen=True, slots=True)
class SwitchboardLinkConfig:
    relay_url: str | None = None
    mcp_url: str | None = None
    consumer_id: str = "taskboard"
    ca_file: str | None = None


@dataclass(frozen=True, slots=True)
class IntakeConfig:
    auto_ready_sources: tuple[str, ...] = ("github",)


@dataclass(frozen=True, slots=True)
class TaskboardConfig:
    http: HttpConfig = field(default_factory=HttpConfig)
    sqlite: SqliteConfig = field(default_factory=SqliteConfig)
    switchboard: SwitchboardLinkConfig = field(default_factory=SwitchboardLinkConfig)
    intake: IntakeConfig = field(default_factory=IntakeConfig)


@dataclass(frozen=True, slots=True)
class ServiceSecrets:
    credentials: EnvCredentialBroker
    mcp_token: str | None


def load_config(path: str | Path) -> TaskboardConfig:
    config_path = Path(path)
    with config_path.open("rb") as handle:
        raw = tomllib.load(handle)
    return parse_config(raw, base_dir=config_path.resolve().parent)


def parse_config(raw: Mapping[str, Any], *, base_dir: Path) -> TaskboardConfig:
    http = _section(raw, "http")
    sqlite = _section(raw, "sqlite")
    link = _section(raw, "switchboard")
    intake = _section(raw, "intake")

    consumer_id = _string(link, "consumer_id", "switchboard", "taskboard")
    if not consumer_id:
        raise ConfigError("switchboard.consumer_id must not be empty")
    ca_file = _string(link, "ca_file", "switchboard", "")
    return TaskboardConfig(
        http=HttpConfig(
            host=_string(http, "host", "http", "127.0.0.1"),
            port=_integer(http, "port", "http", 8791, minimum=0, maximum=65535),
            base_path=normalize_base_path(_string(http, "base_path", "http", "")),
            max_body_bytes=_integer(http, "max_body_bytes", "http", 1_048_576, minimum=1),
        ),
        sqlite=SqliteConfig(
            path=_string(sqlite, "path", "sqlite", "taskboard.db"),
            busy_timeout_ms=_integer(sqlite, "busy_timeout_ms", "sqlite", 5000, minimum=0),
        ),
        switchboard=SwitchboardLinkConfig(
            relay_url=_url(link, "relay_url", ("ws", "wss")),
            mcp_url=_url(link, "mcp_url", ("http", "https")),
            consumer_id=consumer_id,
            ca_file=str(base_dir / ca_file) if ca_file else None,
        ),
        intake=IntakeConfig(auto_ready_sources=_sources(intake)),
    )


def normalize_base_path(value: str) -> str:
    path = value.strip().rstrip("/")
    if not path:
        return ""
    if not path.startswith("/"):
        path = "/" + path
    if _BAD_PATH_CHARS.search(path) or "//" in path:
        raise ConfigError(f"http.base_path {value!r} must be a plain path such as '/taskboard'")
    return path


# Secrets come only from the environment. The service needs both Switchboard client tokens; stdio mode
# never opens the relay and publishes only when SWITCHBOARD_MCP_TOKEN and mcp_url are both set.
def service_secrets(config: TaskboardConfig, environ: Mapping[str, str], *, stdio: bool) -> ServiceSecrets:
    names = (SWITCHBOARD_MCP_TOKEN_ENV,) if stdio else (RELAY_TOKEN_ENV, SWITCHBOARD_MCP_TOKEN_ENV)
    if not stdio:
        if not config.switchboard.relay_url:
            raise ConfigError("[switchboard] relay_url is required to run the service")
        if not config.switchboard.mcp_url:
            raise ConfigError("[switchboard] mcp_url is required to run the service")
        for name in names:
            if not (environ.get(name) or "").strip():
                raise ConfigError(f"environment variable {name} is required to run the service")
    tokens = {name: environ[name].strip() for name in names if (environ.get(name) or "").strip()}
    mcp_token = (environ.get(TASKBOARD_MCP_TOKEN_ENV) or "").strip() or None
    return ServiceSecrets(credentials=EnvCredentialBroker(tokens), mcp_token=mcp_token)


def _section(raw: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    value = raw.get(name, {})
    if not isinstance(value, Mapping):
        raise ConfigError(f"[{name}] must be a table")
    return cast(Mapping[str, Any], value)


def _string(section: Mapping[str, Any], key: str, name: str, default: str) -> str:
    value = section.get(key, default)
    if not isinstance(value, str):
        raise ConfigError(f"{name}.{key} must be a string")
    return value.strip()


def _integer(
    section: Mapping[str, Any], key: str, name: str, default: int, *, minimum: int, maximum: int | None = None
) -> int:
    value = section.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{name}.{key} must be an integer")
    if value < minimum or (maximum is not None and value > maximum):
        upper = f" and at most {maximum}" if maximum is not None else ""
        raise ConfigError(f"{name}.{key} must be at least {minimum}{upper}")
    return value


def _url(section: Mapping[str, Any], key: str, schemes: tuple[str, ...]) -> str | None:
    value = _string(section, key, "switchboard", "")
    if not value:
        return None
    parts = urlsplit(value)
    if parts.scheme not in schemes or not parts.hostname:
        raise ConfigError(f"switchboard.{key} must be a {' or '.join(s + '://' for s in schemes)} URL with a host")
    return value


def _sources(section: Mapping[str, Any]) -> tuple[str, ...]:
    value = section.get("auto_ready_sources", ["github"])
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item.strip() for item in cast(list[Any], value)
    ):
        raise ConfigError("intake.auto_ready_sources must be a list of non-empty strings")
    return tuple(item.strip() for item in cast(list[str], value))
