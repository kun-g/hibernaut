from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlparse


@dataclass(frozen=True, slots=True)
class DashboardConfig:
    hb_root: Path
    hb_config_path: Path
    hb_db_path: Path
    ledger_db_path: Path
    runs_dir: Path
    outbox_dir: Path
    hub_deploy_dir: Path
    sb_config_path: Path
    tb_config_path: Path
    tb_env_path: Path
    hub_base_url: str
    remote_cache_ttl: float = 4.0


def _as_dict(value: Any) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def safe_read_toml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _resolve_path(raw: str | None, base_dir: Path, default: Path) -> Path:
    if not raw:
        return default
    expanded = Path(raw).expanduser()
    return expanded if expanded.is_absolute() else (base_dir / expanded).resolve()


def _derive_hub_base(hb_cfg: dict[str, Any]) -> str:
    sb_cfg = _as_dict(hb_cfg.get("switchboard"))
    if not sb_cfg:
        return ""
    for key in ("mcp_url", "relay_url"):
        raw = sb_cfg.get(key)
        if not isinstance(raw, str) or not raw.strip():
            continue
        parsed = urlparse(raw.strip())
        if not parsed.netloc:
            continue
        scheme = {"wss": "https", "ws": "http"}.get(parsed.scheme, parsed.scheme)
        if scheme in ("http", "https"):
            return f"{scheme}://{parsed.netloc}"
    return ""


def resolve_config(
    *,
    config_path: Path | str | None = None,
    hb_root: Path | str | None = None,
    hub_deploy_dir: Path | str | None = None,
    hub_url: str | None = None,
    remote_cache_ttl: float = 4.0,
) -> DashboardConfig:
    if config_path is not None:
        cfg_file = Path(config_path).expanduser().resolve()
        root_dir = Path(hb_root).expanduser().resolve() if hb_root is not None else cfg_file.parent
    else:
        env_cfg = os.environ.get("HIBERNAUT_CONFIG", "").strip()
        if hb_root is not None:
            root_dir = Path(hb_root).expanduser().resolve()
            cfg_file = root_dir / "hibernaut.toml"
        elif env_cfg:
            cfg_file = Path(env_cfg).expanduser().resolve()
            root_dir = cfg_file.parent
        else:
            root_dir = Path.cwd().resolve()
            cfg_file = root_dir / "hibernaut.toml"

    hb_cfg = safe_read_toml(cfg_file)
    base_dir = cfg_file.parent
    rt = _as_dict(hb_cfg.get("runtime"))
    ch = _as_dict(hb_cfg.get("channels"))

    db_val = rt.get("db_path")
    db_raw = db_val if isinstance(db_val, str) else None
    hb_db = _resolve_path(db_raw, base_dir, base_dir / "hibernaut.db")

    ledger_val = rt.get("ledger_db_path")
    ledger_raw = ledger_val if isinstance(ledger_val, str) else None
    ledger_db = _resolve_path(ledger_raw, base_dir, hb_db.parent / "ledger.db")

    outbox_val = ch.get("outbox_dir")
    outbox_raw = outbox_val if isinstance(outbox_val, str) else None
    outbox_dir = _resolve_path(outbox_raw, base_dir, base_dir / "outbox")

    runs_dir = hb_db.parent / "runs"

    if hub_deploy_dir is not None:
        deploy_dir = Path(hub_deploy_dir).expanduser().resolve()
    else:
        env_deploy = os.environ.get("HIBERNAUT_HUB_DEPLOY_DIR", "").strip()
        deploy_dir = (
            Path(env_deploy).expanduser().resolve()
            if env_deploy
            else (Path.home() / ".config/hibernaut/hub-deploy")
        )

    resolved_hub = hub_url.strip().rstrip("/") if hub_url else _derive_hub_base(hb_cfg)

    return DashboardConfig(
        hb_root=root_dir,
        hb_config_path=cfg_file,
        hb_db_path=hb_db,
        ledger_db_path=ledger_db,
        runs_dir=runs_dir,
        outbox_dir=outbox_dir,
        hub_deploy_dir=deploy_dir,
        sb_config_path=deploy_dir / "switchboard.toml",
        tb_config_path=deploy_dir / "taskboard.toml",
        tb_env_path=deploy_dir / "taskboard.env",
        hub_base_url=resolved_hub,
        remote_cache_ttl=remote_cache_ttl,
    )
