from __future__ import annotations
import ast
import sys
from pathlib import Path
from dashboard import resolve_config


DASHBOARD_SRC = Path(__file__).resolve().parents[2] / "src" / "dashboard"
STDLIB_NAMES: frozenset[str] = frozenset(sys.stdlib_module_names) | {"__future__"}


def test_dashboard_zero_business_dependencies() -> None:
    for py_file in DASHBOARD_SRC.rglob("*.py"):
        tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                mods = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                mods = [node.module]
            else:
                mods = []
            for mod in mods:
                top = mod.split(".")[0]
                assert top in STDLIB_NAMES or top == "dashboard", (
                    f"dashboard/{py_file.name} imported forbidden module '{mod}'"
                )


def test_resolve_config_reads_hibernaut_toml(tmp_path: Path) -> None:
    cfg_file = tmp_path / "hibernaut.toml"
    cfg_file.write_text(
        """
[runtime]
db_path = "./state/hb.db"

[channels]
outbox_dir = "./reports"

[switchboard]
enabled = true
relay_url = "wss://hub.example.test/switchboard/ws/relay"
""",
        encoding="utf-8",
    )
    deploy_dir = tmp_path / "deploy"
    cfg = resolve_config(config_path=cfg_file, hub_deploy_dir=deploy_dir)
    assert cfg.hb_db_path == (tmp_path / "state/hb.db").resolve()
    assert cfg.ledger_db_path == (tmp_path / "state/ledger.db").resolve()
    assert cfg.outbox_dir == (tmp_path / "reports").resolve()
    assert cfg.runs_dir == (tmp_path / "state/runs").resolve()
    assert cfg.hub_base_url == "https://hub.example.test"
    assert cfg.sb_config_path == deploy_dir.resolve() / "switchboard.toml"
