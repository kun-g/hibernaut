from __future__ import annotations

import re
import sqlite3
import threading
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime, UTC
from pathlib import Path

from memory_hub.fs_store import MarkdownFileStore
from memory_hub.models import IndexStats, LayerKind, SearchHit, SearchQuery, SearchResponse, sha256_text
from memory_hub.mounts import KnowledgeMountRegistry

_HEADING_RE = re.compile(r"^(#{1,3})\s+(.*)$")
_CJK_RUN_RE = re.compile(r"[\u4e00-\u9fff\u3400-\u4dbf]+")
_NON_ALNUM_RE = re.compile(r"[^\w\u4e00-\u9fff]+")

LAYER_WEIGHTS: dict[LayerKind, float] = {
    "project": 1.20,
    "shared": 1.00,
    "kb": 0.85,
}


def normalize_search_tokens(text: str) -> str:
    if not text:
        return ""
    tokens: list[str] = []
    for part in _NON_ALNUM_RE.split(text.lower()):
        if not part:
            continue
        if _CJK_RUN_RE.search(part):
            pos = 0
            for m in _CJK_RUN_RE.finditer(part):
                if m.start() > pos:
                    tokens.append(part[pos : m.start()])
                run = m.group(0)
                tokens.append(run)
                if len(run) == 1:
                    tokens.append(run)
                else:
                    for i in range(len(run) - 1):
                        tokens.append(run[i : i + 2])
                    if len(run) <= 4:
                        for ch in run:
                            tokens.append(ch)
                pos = m.end()
            if pos < len(part):
                tokens.append(part[pos:])
        else:
            tokens.append(part)
    return " ".join(tokens)


def build_fts_query(raw_query: str) -> str:
    terms = [t for t in normalize_search_tokens(raw_query).split() if t]
    if not terms:
        return ""
    deduped = list(dict.fromkeys(terms))[:24]
    escaped = ['"' + t.replace('"', "") + '"' for t in deduped]
    return " OR ".join(escaped)


def chunk_markdown(content: str, *, max_chunk_chars: int = 1200) -> list[tuple[str, str]]:
    if not content.strip():
        return []
    sections: list[tuple[str, list[str]]] = []
    h_stack: list[tuple[int, str]] = []
    cur_heading = "(root)"
    cur_lines: list[str] = []

    for line in content.splitlines():
        m = _HEADING_RE.match(line)
        if m:
            if cur_lines:
                sections.append((cur_heading, cur_lines))
                cur_lines = []
            level = len(m.group(1))
            title = f"{m.group(1)} {m.group(2).strip()}"
            while h_stack and h_stack[-1][0] >= level:
                h_stack.pop()
            h_stack.append((level, title))
            cur_heading = " > ".join(t for _, t in h_stack)
            cur_lines.append(line)
        else:
            cur_lines.append(line)

    if cur_lines:
        sections.append((cur_heading, cur_lines))

    chunks: list[tuple[str, str]] = []
    for heading_path, lines in sections:
        body = "\n".join(lines).strip()
        if not body:
            continue
        if len(body) <= max_chunk_chars:
            chunks.append((heading_path, body))
            continue
        buf: list[str] = []
        buf_len = 0
        for para in body.split("\n\n"):
            if buf and buf_len + len(para) > max_chunk_chars:
                chunks.append((heading_path, "\n\n".join(buf)))
                buf = [para]
                buf_len = len(para)
            else:
                buf.append(para)
                buf_len += len(para) + 2
        if buf:
            chunks.append((heading_path, "\n\n".join(buf)))
    return chunks


def make_snippet(body: str, query: str, *, max_len: int = 320) -> str:
    clean = " ".join(body.split())
    if len(clean) <= max_len:
        return clean
    low = clean.lower()
    pos = -1
    for term in _NON_ALNUM_RE.split(query.lower()):
        if term:
            idx = low.find(term)
            if idx != -1:
                pos = idx
                break
    if pos == -1 or pos < 60:
        return clean[: max_len - 3].rstrip() + "..."
    start = max(0, pos - 60)
    end = min(len(clean), start + max_len - 6)
    prefix = "..." if start > 0 else ""
    suffix = "..." if end < len(clean) else ""
    return f"{prefix}{clean[start:end].strip()}{suffix}"


class _SingleFTSDatabase:
    def __init__(self, db_path: Path) -> None:
        self._path = db_path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(self._path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS indexed_files (
                    uri TEXT PRIMARY KEY,
                    scope TEXT NOT NULL,
                    layer TEXT NOT NULL,
                    mtime_ns INTEGER NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    sha256 TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    readonly INTEGER NOT NULL,
                    chunk_count INTEGER NOT NULL
                );
                CREATE VIRTUAL TABLE IF NOT EXISTS memory_fts USING fts5(
                    uri UNINDEXED,
                    scope UNINDEXED,
                    layer UNINDEXED,
                    heading_path UNINDEXED,
                    body_raw UNINDEXED,
                    search_tokens,
                    tokenize = 'unicode61'
                );
                """
            )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def transaction(self) -> Generator[None]:
        # One at a time, across threads (the lock) and processes (SQLite's write lock). What lists files, syncs and
        # queries must do so in one transaction: a file another writer indexes after the listing looks stale to the sync.
        with self._lock:
            if self._conn.in_transaction:
                yield
                return
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise

    def reset(self, scopes: tuple[str, ...] | None = None) -> None:
        with self.transaction():
            if scopes is None:
                self._conn.execute("DELETE FROM indexed_files")
                self._conn.execute("DELETE FROM memory_fts")
                return
            for uri in self._rows(scopes):
                self._remove(uri)

    def _rows(self, scopes: tuple[str, ...] | None) -> dict[str, tuple[int, int]]:
        # scopes names projects; like the listing of their files, it takes in the shared layer.
        return {
            uri: (mtime_ns, size_bytes)
            for uri, scope, layer, mtime_ns, size_bytes in self._conn.execute(
                "SELECT uri, scope, layer, mtime_ns, size_bytes FROM indexed_files"
            )
            if scopes is None or layer == "shared" or scope in scopes
        }

    def _remove(self, uri: str) -> None:
        self._conn.execute("DELETE FROM memory_fts WHERE uri = ?", (uri,))
        self._conn.execute("DELETE FROM indexed_files WHERE uri = ?", (uri,))

    def sync_files(
        self,
        files: list[tuple[str, str, LayerKind, Path, bool]],
        *,
        scopes: tuple[str, ...] | None = None,
    ) -> tuple[int, int, int]:
        # files is all there is in scopes (None: in the whole index); rows outside them stay.
        indexed_files = 0
        indexed_chunks = 0
        removed_files = 0
        seen_uris: set[str] = set()

        with self.transaction():
            existing = self._rows(scopes)
            for uri, scope, layer, path, readonly in files:
                seen_uris.add(uri)
                try:
                    st = path.stat()
                except OSError:
                    continue
                prev = existing.get(uri)
                if prev is not None and prev == (st.st_mtime_ns, st.st_size):
                    continue
                content = path.read_text(encoding="utf-8", errors="replace")
                digest = sha256_text(content)
                updated_at = (
                    datetime.fromtimestamp(st.st_mtime, tz=UTC)
                    .replace(microsecond=0)
                    .isoformat()
                    .replace("+00:00", "Z")
                )
                chunks = chunk_markdown(content)
                self._conn.execute("DELETE FROM memory_fts WHERE uri = ?", (uri,))
                for heading_path, body in chunks:
                    tokens = normalize_search_tokens(f"{heading_path}\n{body}")
                    self._conn.execute(
                        "INSERT INTO memory_fts(uri, scope, layer, heading_path, body_raw, search_tokens) VALUES (?, ?, ?, ?, ?, ?)",
                        (uri, scope, layer, heading_path, body, tokens),
                    )
                self._conn.execute(
                    """
                    INSERT INTO indexed_files(uri, scope, layer, mtime_ns, size_bytes, sha256, updated_at, readonly, chunk_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(uri) DO UPDATE SET
                        scope=excluded.scope, layer=excluded.layer, mtime_ns=excluded.mtime_ns,
                        size_bytes=excluded.size_bytes, sha256=excluded.sha256,
                        updated_at=excluded.updated_at, readonly=excluded.readonly, chunk_count=excluded.chunk_count
                    """,
                    (uri, scope, layer, st.st_mtime_ns, st.st_size, digest, updated_at, int(readonly), len(chunks)),
                )
                indexed_files += 1
                indexed_chunks += len(chunks)

            stale = set(existing.keys()) - seen_uris
            for uri in stale:
                self._remove(uri)
                removed_files += 1
        return indexed_files, indexed_chunks, removed_files

    def refresh_single(self, uri: str, scope: str, layer: LayerKind, path: Path, readonly: bool) -> None:
        with self.transaction():
            if not path.is_file():
                self._remove(uri)
                return
            st = path.stat()
            content = path.read_text(encoding="utf-8", errors="replace")
            digest = sha256_text(content)
            updated_at = (
                datetime.fromtimestamp(st.st_mtime, tz=UTC)
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z")
            )
            chunks = chunk_markdown(content)
            self._conn.execute("DELETE FROM memory_fts WHERE uri = ?", (uri,))
            for heading_path, body in chunks:
                tokens = normalize_search_tokens(f"{heading_path}\n{body}")
                self._conn.execute(
                    "INSERT INTO memory_fts(uri, scope, layer, heading_path, body_raw, search_tokens) VALUES (?, ?, ?, ?, ?, ?)",
                    (uri, scope, layer, heading_path, body, tokens),
                )
            self._conn.execute(
                """
                INSERT INTO indexed_files(uri, scope, layer, mtime_ns, size_bytes, sha256, updated_at, readonly, chunk_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(uri) DO UPDATE SET
                    scope=excluded.scope, layer=excluded.layer, mtime_ns=excluded.mtime_ns,
                    size_bytes=excluded.size_bytes, sha256=excluded.sha256,
                    updated_at=excluded.updated_at, readonly=excluded.readonly, chunk_count=excluded.chunk_count
                """,
                (uri, scope, layer, st.st_mtime_ns, st.st_size, digest, updated_at, int(readonly), len(chunks)),
            )

    def query(
        self,
        raw_query: str,
        *,
        allowed_scopes: set[str] | None,
        allowed_layers: set[LayerKind],
        include_archived: bool,
        limit: int,
    ) -> list[SearchHit]:
        fts_expr = build_fts_query(raw_query)
        if not fts_expr:
            return []
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT m.uri, m.scope, m.layer, m.heading_path, m.body_raw,
                       bm25(memory_fts) AS rank_val, f.sha256, f.updated_at, f.readonly
                FROM memory_fts m
                JOIN indexed_files f ON f.uri = m.uri
                WHERE memory_fts MATCH ?
                ORDER BY rank_val ASC
                LIMIT ?
                """,
                (fts_expr, max(limit * 4, 24)),
            ).fetchall()

        hits: list[SearchHit] = []
        best_by_uri: dict[str, SearchHit] = {}
        for uri, scope, layer_raw, heading_path, body_raw, rank_val, digest, updated_at, readonly_int in rows:
            layer: LayerKind = layer_raw
            if layer not in allowed_layers:
                continue
            if allowed_scopes is not None and layer == "project" and scope not in allowed_scopes:
                continue
            if not include_archived and ("/reported_archive/" in uri or "/progress_archive/" in uri):
                continue
            bm25_norm = 1.0 / (1.0 + abs(float(rank_val)))
            weight = LAYER_WEIGHTS.get(layer, 1.0)
            if "/instructions.md" in uri or "/notes/postmortem/" in uri:
                weight *= 1.08
            score = bm25_norm * weight
            hit = SearchHit(
                uri=uri,
                layer=layer,
                heading_path=heading_path,
                snippet=make_snippet(body_raw, raw_query, max_len=320),
                score=score,
                sha256=digest,
                updated_at=updated_at,
                readonly=bool(readonly_int),
            )
            prev = best_by_uri.get(uri)
            if prev is None or hit.score > prev.score:
                best_by_uri[uri] = hit

        hits = sorted(best_by_uri.values(), key=lambda h: h.score, reverse=True)
        return hits[:limit]


class DualFTSIndex:
    def __init__(
        self,
        store: MarkdownFileStore,
        mounts: KnowledgeMountRegistry,
        *,
        index_dir: Path | None = None,
    ) -> None:
        self._store = store
        self._mounts = mounts
        base = (index_dir or (store.root / ".index")).resolve()
        self._project_db = _SingleFTSDatabase(base / "memory_index.db")
        self._kb_db = _SingleFTSDatabase(base / "kb_index.db")

    def close(self) -> None:
        self._project_db.close()
        self._kb_db.close()

    def sync_project_domain(self, scopes: tuple[str, ...] | None = None) -> tuple[int, int, int]:
        files: list[tuple[str, str, LayerKind, Path, bool]] = []
        with self._project_db.transaction():
            for uri, scope, layer_str, path in self._store.iter_indexable_files(scopes=scopes):
                layer: LayerKind = "project" if layer_str == "project" else "shared"
                files.append((uri, scope, layer, path, False))
            return self._project_db.sync_files(files, scopes=scopes)

    def sync_kb_domain(self) -> tuple[int, int, int]:
        files: list[tuple[str, str, LayerKind, Path, bool]] = []
        with self._kb_db.transaction():
            for uri, mount_name, path in self._mounts.iter_files():
                files.append((uri, mount_name, "kb", path, True))
            return self._kb_db.sync_files(files)

    def refresh_uri(self, uri: str, scope: str, layer: LayerKind, path: Path) -> None:
        if layer == "kb":
            self._kb_db.refresh_single(uri, scope, "kb", path, True)
        else:
            self._project_db.refresh_single(uri, scope, layer, path, False)

    def rebuild(self, *, scopes: tuple[str, ...] | None = None, include_kb: bool = True) -> IndexStats:
        with self._project_db.transaction():
            self._project_db.reset(scopes)
            pf, pc, pr = self.sync_project_domain(scopes=scopes)
        kf = kc = 0
        if include_kb:
            with self._kb_db.transaction():
                self._kb_db.reset()
                kf, kc, _ = self.sync_kb_domain()
        return IndexStats(
            indexed_files=pf,
            indexed_chunks=pc,
            removed_files=pr,
            kb_indexed_files=kf,
            kb_indexed_chunks=kc,
        )

    def search(self, query: SearchQuery) -> SearchResponse:
        allowed_layers: set[LayerKind] = set()
        if query.include_project:
            allowed_layers.add("project")
        if query.include_shared:
            allowed_layers.add("shared")

        combined: list[SearchHit] = []
        with self._project_db.transaction():
            self.sync_project_domain()
            if allowed_layers:
                combined.extend(
                    self._project_db.query(
                        query.query,
                        allowed_scopes={query.scope},
                        allowed_layers=allowed_layers,
                        include_archived=query.include_archived,
                        limit=query.top_k,
                    )
                )
        if query.include_kb and self._mounts.names:
            with self._kb_db.transaction():
                self.sync_kb_domain()
                combined.extend(
                    self._kb_db.query(
                        query.query,
                        allowed_scopes=None,
                        allowed_layers={"kb"},
                        include_archived=query.include_archived,
                        limit=query.top_k,
                    )
                )
        combined.sort(key=lambda h: h.score, reverse=True)
        return SearchResponse(
            scope=query.scope,
            query=query.query,
            mode=query.mode,
            items=tuple(combined[: query.top_k]),
        )
