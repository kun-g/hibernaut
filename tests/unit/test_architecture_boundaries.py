"""Gate 1 (Executable AST Anti-Corruption Gate):

Enforces Clean Architecture dependency rules without requiring external packages:
1. L0 core/models.py: Python stdlib ONLY (zero third-party, zero internal cross-imports).
2. L1 core/ports.py: Python stdlib + agent_runtime.core.models ONLY.
3. L2 engine/: Python stdlib + agent_runtime.core + agent_runtime.engine ONLY
   (strictly forbidden to import adapters/ or app/).
"""

from __future__ import annotations

import ast
import sys
import unittest
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[2] / "src" / "agent_runtime"
STDLIB_NAMES: frozenset[str] = frozenset(sys.stdlib_module_names) | {"__future__"}


def _extract_imported_modules(py_file: Path) -> list[str]:
    tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
    modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                modules.append(node.module)
    return modules


class TestArchitectureBoundaries(unittest.TestCase):
    def test_core_models_has_zero_external_dependencies(self) -> None:
        models_file = SRC_ROOT / "core" / "models.py"
        for mod in _extract_imported_modules(models_file):
            top = mod.split(".")[0]
            self.assertIn(
                top,
                STDLIB_NAMES,
                f"L0 core/models.py must ONLY import Python stdlib; found '{mod}'",
            )

    def test_core_ports_depends_only_on_stdlib_and_l0_models(self) -> None:
        ports_file = SRC_ROOT / "core" / "ports.py"
        for mod in _extract_imported_modules(ports_file):
            top = mod.split(".")[0]
            allowed = top in STDLIB_NAMES or mod == "agent_runtime.core.models"
            self.assertTrue(
                allowed,
                f"L1 core/ports.py may ONLY import stdlib or core.models; found '{mod}'",
            )

    def test_core_package_never_imports_outer_layers_or_third_party(self) -> None:
        for py_file in (SRC_ROOT / "core").rglob("*.py"):
            for mod in _extract_imported_modules(py_file):
                top = mod.split(".")[0]
                allowed = top in STDLIB_NAMES or mod.startswith("agent_runtime.core")
                self.assertTrue(
                    allowed,
                    f"core/ file {py_file.name} imported forbidden module '{mod}'",
                )

    def test_engine_never_imports_adapters_app_or_third_party(self) -> None:
        for py_file in (SRC_ROOT / "engine").rglob("*.py"):
            for mod in _extract_imported_modules(py_file):
                top = mod.split(".")[0]
                self.assertFalse(
                    mod.startswith("agent_runtime.adapters")
                    or mod.startswith("agent_runtime.app"),
                    f"L2 engine/{py_file.name} violated dependency rule by importing '{mod}'",
                )
                allowed = (
                    top in STDLIB_NAMES
                    or mod.startswith("agent_runtime.core")
                    or mod.startswith("agent_runtime.engine")
                )
                self.assertTrue(
                    allowed,
                    f"L2 engine/{py_file.name} imported non-stdlib/non-core module '{mod}'",
                )


if __name__ == "__main__":
    unittest.main()
