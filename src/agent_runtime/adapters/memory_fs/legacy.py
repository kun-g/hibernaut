from __future__ import annotations

from pathlib import Path

from agent_runtime.adapters.memory_fs.hub import GovernedMemoryHub
from memory_hub.partition import sanitize_segment

_ADOPTED_MARKER = ".memory_hub_adopted"


def adopt_legacy_memory(hub: GovernedMemoryHub, inbox_id: str) -> None:
    # Before Memory Hub every inbox_id had its own projects/<sanitized inbox_id>. Its files are copied into the
    # project's directory once; the old directory stays as it was. The project's main inbox goes first, so where two
    # partitions hold the same file its version is the one in effect and the other is kept under notes/legacy/.
    key = hub.resolve_project_key(inbox_id)
    names = dict.fromkeys((f"proj__{key}", sanitize_segment(inbox_id, field_name="inbox_id")))
    pending = [d for d in (hub.project_dir(n) for n in names if n != key) if _is_unadopted(d)]
    if not pending:
        return
    with hub.exclusive():
        hub.load_baseline(key)
        for source in pending:
            if _is_unadopted(source):
                _copy(source, hub.project_dir(key))
                (source / _ADOPTED_MARKER).write_text(f"{key}\n", encoding="utf-8")


def _is_unadopted(folder: Path) -> bool:
    return folder.is_dir() and not (folder / _ADOPTED_MARKER).exists()


def _copy(source: Path, target: Path) -> None:
    for path in sorted(source.rglob("*")):
        if path.is_symlink() or not path.is_file():
            continue
        data = path.read_bytes()
        if not data:
            continue
        rel = path.relative_to(source)
        dest = target / rel
        if dest.is_file() and dest.stat().st_size > 0:
            if dest.read_bytes() == data:
                continue
            dest = target / "notes" / "legacy" / source.name / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
