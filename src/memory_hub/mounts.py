from __future__ import annotations

from datetime import datetime, UTC
from pathlib import Path, PurePosixPath
from collections.abc import Iterator

from memory_hub.models import KnowledgeMountConfig, MemoryDoc, estimate_tokens, sha256_text
from memory_hub.partition import safe_join


def _matches_pattern(rel_posix: str, pattern: str) -> bool:
    p = PurePosixPath(rel_posix)
    if p.match(pattern):
        return True
    if pattern.startswith("**/") and p.match(pattern[3:]):
        return True
    if "/**/" in pattern and p.match(pattern.replace("/**/", "/")):
        return True
    if pattern.endswith("/**"):
        prefix = pattern[:-3].rstrip("/")
        if rel_posix == prefix or rel_posix.startswith(prefix + "/"):
            return True
    return False


def _matches_any(rel_posix: str, patterns: tuple[str, ...]) -> bool:
    return any(_matches_pattern(rel_posix, pat) for pat in patterns)


class KnowledgeMountRegistry:
    def __init__(self, configs: tuple[KnowledgeMountConfig, ...] = ()) -> None:
        self._mounts: dict[str, tuple[KnowledgeMountConfig, Path]] = {}
        for cfg in configs:
            if cfg.mode != "readonly":
                raise ValueError(f"External mount '{cfg.name}' must be readonly")
            self._mounts[cfg.name] = (cfg, Path(cfg.root).expanduser().resolve())

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(self._mounts.keys())

    def has_mount(self, name: str) -> bool:
        return name in self._mounts

    def iter_files(self, mount_name: str | None = None) -> Iterator[tuple[str, str, Path]]:
        targets = [mount_name] if mount_name else list(self._mounts.keys())
        for name in targets:
            entry = self._mounts.get(name)
            if entry is None:
                continue
            cfg, root = entry
            if not root.is_dir():
                continue
            for path in sorted(root.rglob("*")):
                if not path.is_file():
                    continue
                rel_posix = path.relative_to(root).as_posix()
                if _matches_any(rel_posix, cfg.exclude_globs):
                    continue
                if not _matches_any(rel_posix, cfg.include_globs):
                    continue
                try:
                    if path.stat().st_size > cfg.max_file_bytes:
                        continue
                except OSError:
                    continue
                uri = f"kb://{name}/{rel_posix}"
                yield uri, name, path

    def resolve_file(self, mount_name: str, rel_path: str) -> tuple[KnowledgeMountConfig, Path]:
        entry = self._mounts.get(mount_name)
        if entry is None:
            raise FileNotFoundError(f"Unknown knowledge mount: {mount_name}")
        cfg, root = entry
        target = safe_join(root, rel_path)
        rel_posix = target.relative_to(root).as_posix()
        if _matches_any(rel_posix, cfg.exclude_globs) or not _matches_any(rel_posix, cfg.include_globs):
            raise PermissionError(f"Path not permitted by mount '{mount_name}' glob policy: {rel_path}")
        if not target.is_file():
            raise FileNotFoundError(f"Mounted knowledge file not found: kb://{mount_name}/{rel_path}")
        if target.stat().st_size > cfg.max_file_bytes:
            raise ValueError(f"Mounted knowledge file exceeds max_file_bytes ({cfg.max_file_bytes}): {rel_path}")
        return cfg, target

    def read(self, mount_name: str, rel_path: str, *, max_bytes: int | None = None) -> MemoryDoc:
        _, target = self.resolve_file(mount_name, rel_path)
        raw = target.read_text(encoding="utf-8", errors="replace")
        truncated = False
        if max_bytes is not None and len(raw.encode("utf-8")) > max_bytes:
            raw = raw.encode("utf-8")[:max_bytes].decode("utf-8", errors="ignore") + "\n[TRUNCATED]"
            truncated = True
        mtime = datetime.fromtimestamp(target.stat().st_mtime, tz=UTC).replace(microsecond=0)
        return MemoryDoc(
            uri=f"kb://{mount_name}/{rel_path.lstrip('/')}",
            content=raw,
            sha256=sha256_text(raw),
            est_tokens=estimate_tokens(raw),
            updated_at=mtime.isoformat().replace("+00:00", "Z"),
            readonly=True,
            truncated=truncated,
        )
