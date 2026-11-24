from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import cast

SKILL_FILE = "SKILL.md"
BUILTIN_PREFIX = "builtin:"
# Shipped inside the package (see [tool.setuptools.package-data]); one directory per role.
BUILTIN_SKILLS_DIR = Path(__file__).resolve().with_name("skills")
# Old role names still accepted in deployment files and builtin: entries.
ROLE_ALIASES = {"coder-reviewer": "code-reviewer"}
# Built-in skills a role may list besides its own; none of them replaces it.
ROLE_EXTRA_SKILLS: dict[str, tuple[str, ...]] = {"steward": ("chief-of-staff",)}


def canonical_role(role: str) -> str:
    return ROLE_ALIASES.get(role, role)


def builtin_skill(role: str) -> Path:
    return BUILTIN_SKILLS_DIR / canonical_role(role) / SKILL_FILE


def resolve_skill(where: str, role: str, entry: str, base_dir: Path) -> Path:
    raw = entry.strip()
    if raw.startswith(BUILTIN_PREFIX):
        # A role may only name its own built-in skills, so one role's rules never leak into another.
        own, name = canonical_role(role), canonical_role(raw.removeprefix(BUILTIN_PREFIX))
        if name != own and name not in ROLE_EXTRA_SKILLS.get(own, ()):
            raise ValueError(f"{where}: '{raw}' is not available for role '{role}' (use '{BUILTIN_PREFIX}{role}')")
        path = builtin_skill(name)
    else:
        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = base_dir / path
        path = path.resolve()
        if path.is_dir():
            path = path / SKILL_FILE
    if not path.is_file():
        raise ValueError(f"{where}: skill '{raw}' not found (no file at {path})")
    return path


def parse_skills(where: str, role: str, raw: object, base_dir: Path) -> tuple[Path, ...]:
    # Relative entries resolve against the directory of the deployment file that declares them.
    if raw is None:
        return (builtin_skill(role),)
    items = cast(list[object], raw) if isinstance(raw, list) else None
    if items is None or not all(isinstance(s, str) and s.strip() for s in items):
        raise ValueError(f"{where}: skills must be a list of SKILL.md files or directories that contain one")
    entries = [str(s) for s in items]
    if not entries:
        raise ValueError(f"{where}: skills must name at least one skill; omit it to use '{BUILTIN_PREFIX}{role}'")
    return tuple(resolve_skill(where, role, entry, base_dir) for entry in entries)


def render_skills(paths: Sequence[Path]) -> str:
    # Read at every launch, so an edited SKILL.md applies to the next run; raises ValueError when one is gone.
    parts: list[str] = []
    for path in paths:
        try:
            body = path.read_text(encoding="utf-8").strip()
        except (OSError, UnicodeDecodeError) as exc:
            raise ValueError(f"cannot read role skill {path}: {exc}") from exc
        parts.append(
            f"# Role skill: {path}\n"
            f"Trusted role instructions from the Hibernaut deployment. "
            f"Relative resources are relative to {path.parent}.\n\n{body}"
        )
    return "\n\n".join(parts)
