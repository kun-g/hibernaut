# The static gates cover every package under src/ and run the same everywhere: CI, AGENTS.md, README, delivery.
import json
import re
import tomllib
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
GATES = ["lint-imports", "ruff check", "pytest -n auto --dist worksteal -v", "pyright"]


def _packages() -> list[str]:
    return sorted(p.parent.relative_to(REPO).as_posix() for p in (REPO / "src").glob("*/__init__.py"))


def test_pyright_checks_every_package_under_src_strictly() -> None:
    config = json.loads((REPO / "pyrightconfig.json").read_text(encoding="utf-8"))
    covered = config["include"]
    missing = [pkg for pkg in _packages() if not any(pkg == inc or pkg.startswith(inc + "/") for inc in covered)]
    assert missing == []
    assert config["typeCheckingMode"] == "strict"


def test_ruff_checks_every_package_under_src() -> None:
    ruff = tomllib.loads((REPO / "pyproject.toml").read_text(encoding="utf-8"))["tool"]["ruff"]
    assert not ruff.get("exclude") and not ruff.get("extend-exclude")
    assert {"F", "B", "PLE"} <= set(ruff["lint"]["select"])


def test_one_type_checker_in_the_dev_dependencies() -> None:
    project = tomllib.loads((REPO / "pyproject.toml").read_text(encoding="utf-8"))
    dev = " ".join(project["project"]["optional-dependencies"]["dev"])
    assert "ruff" in dev and "pyright" in dev and "mypy" not in dev
    assert "mypy" not in project["tool"]


def test_ci_runs_every_gate() -> None:
    ci = (REPO / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert [gate for gate in GATES if gate not in ci] == []


def test_agents_md_and_readme_list_every_gate() -> None:
    agents = (REPO / "AGENTS.md").read_text(encoding="utf-8")
    section = agents.split("## 5.", 1)[1].split("\n## ", 1)[0]
    assert section.startswith(" 提交前必跑的四道门禁命令")
    block = re.search(r"```bash\n(.*?)```", section, re.S)
    assert block is not None and block.group(1).split("\n")[:-1] == GATES
    readme = (REPO / "README.md").read_text(encoding="utf-8")
    assert [gate for gate in GATES if gate.split(" -")[0] not in readme] == []


def test_delivery_runs_the_gates_in_the_same_order() -> None:
    example = tomllib.loads((REPO / "hibernaut.example.toml").read_text(encoding="utf-8"))
    checks = example["projects"]["hibernaut"]["delivery"]["checks"]
    assert [" ".join(argv) for argv in checks] == GATES
