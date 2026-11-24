from __future__ import annotations

import configparser
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


# --- 1. package registration and dependency contracts -----------------------------------------------------------


def test_import_contracts_isolate_collaboration_both_ways() -> None:
    parser = configparser.ConfigParser()
    parser.read(ROOT / ".importlinter")

    def names(section: str, key: str) -> set[str]:
        return set(parser[section][key].split())

    assert "collaboration" in names("importlinter", "root_packages")
    own = "importlinter:contract:collaboration-zero-business-deps"
    assert parser[own]["name"] == "Collaboration package has zero business dependencies"
    assert parser[own]["type"] == "forbidden"
    assert names(own, "source_modules") == {"collaboration"}
    business = {"switchboard", "taskboard", "runner", "ledger", "ccpeer", "dashboard", "memory_hub"}
    assert names(own, "forbidden_modules") == {"agent_runtime"} | business
    reverse = "importlinter:contract:business-packages-no-collaboration"
    assert names(reverse, "source_modules") == business
    assert names(reverse, "forbidden_modules") == {"collaboration"}


def test_collaboration_is_type_checked_strictly() -> None:
    assert "src/collaboration" in json.loads((ROOT / "pyrightconfig.json").read_text())["include"]
