from __future__ import annotations

from pathlib import Path

BASELINE_FILES = ("instructions.md", "index.md", "reported.md")


class FileSystemMemoryAdapter:
    def __init__(self, root_dir: str | Path) -> None:
        self._root = Path(root_dir).resolve()
        self._root.mkdir(parents=True, exist_ok=True)
        (self._root / "notes").mkdir(exist_ok=True)
        (self._root / "progress").mkdir(exist_ok=True)
        for filename in BASELINE_FILES:
            target = self._root / filename
            if not target.exists():
                target.write_text("", encoding="utf-8")

    def _safe_resolve(self, relative_path: str) -> Path:
        cleaned = relative_path.lstrip("/")
        resolved = (self._root / cleaned).resolve()
        try:
            resolved.relative_to(self._root)
        except ValueError as exc:
            raise ValueError(f"Invalid memory path traversal: {relative_path}") from exc
        return resolved

    def load_baseline(self) -> dict[str, str]:
        return {
            filename: (self._root / filename).read_text(encoding="utf-8")
            for filename in BASELINE_FILES
        }

    def read_on_demand(self, path: str) -> str:
        target = self._safe_resolve(path)
        if not target.is_file():
            raise FileNotFoundError(f"Memory file not found: {path}")
        return target.read_text(encoding="utf-8")

    def write(self, path: str, content: str) -> None:
        target = self._safe_resolve(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        # Atomic temp-file swap prevents partial reads across concurrent runs.
        tmp_path = target.with_suffix(target.suffix + ".tmp")
        tmp_path.write_text(content, encoding="utf-8")
        tmp_path.replace(target)
