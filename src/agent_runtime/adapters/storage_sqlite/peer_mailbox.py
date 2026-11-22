from __future__ import annotations

import json
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from agent_runtime.adapters.storage_sqlite.db import SQLiteDatabase
from agent_runtime.core.models import utc_now_iso


@dataclass(frozen=True, slots=True)
class PeerRecord:
    msg_id: str
    kind: str
    from_addr: str | None
    body: str
    ts: float
    record: dict[str, Any]


class SQLitePeerMailbox:
    # What Hibernaut has read from a ccpeer receiver's logs. The cursor and the records it covers commit in one
    # transaction, so a record is either read again after a restart or already stored here, never lost.
    def __init__(self, db: SQLiteDatabase, receiver: str) -> None:
        self._db = db
        self.receiver = receiver

    def cursor(self, log: str) -> int:
        row = self._db.fetchone("SELECT offset FROM peer_cursors WHERE receiver = ? AND log = ?", (self.receiver, log))
        return int(row["offset"]) if row is not None else 0

    def store(self, log: str, records: Sequence[Mapping[str, Any]], offset: int) -> int:
        # Returns how many records were new; a msg_id seen before is skipped.
        added = 0
        with self._db.transaction() as conn:
            for rec in records:
                msg_id = rec.get("msg_id")
                if not isinstance(msg_id, str) or not msg_id:
                    continue
                ts = rec.get("ts")
                body = rec.get("body")
                from_addr = rec.get("from_addr")
                cur = conn.execute(
                    "INSERT OR IGNORE INTO peer_messages "
                    "(msg_id, receiver, kind, from_addr, body, record_json, ts, consumed_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, NULL)",
                    (msg_id, self.receiver, str(rec.get("type") or log), from_addr if isinstance(from_addr, str) else None,
                     body if isinstance(body, str) else "", json.dumps(dict(rec), ensure_ascii=False),
                     float(ts) if isinstance(ts, (int, float)) else 0.0),
                )
                added += cur.rowcount
            conn.execute(
                "INSERT INTO peer_cursors (receiver, log, offset, updated_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(receiver, log) DO UPDATE SET offset = excluded.offset, updated_at = excluded.updated_at",
                (self.receiver, log, offset, utc_now_iso()),
            )
        return added

    def take(self, kind: str, from_addrs: Collection[str], since: float) -> list[PeerRecord]:
        # Open records of a kind from these senders received at or after `since`, oldest first; each is returned once.
        if not from_addrs:
            return []
        marks = ", ".join("?" for _ in from_addrs)
        with self._db.transaction() as conn:
            rows = conn.execute(
                f"SELECT msg_id, kind, from_addr, body, record_json, ts FROM peer_messages "
                f"WHERE receiver = ? AND kind = ? AND consumed_at IS NULL AND ts >= ? AND from_addr IN ({marks}) "
                "ORDER BY ts, rowid",
                (self.receiver, kind, since, *from_addrs),
            ).fetchall()
            now = utc_now_iso()
            for row in rows:
                conn.execute("UPDATE peer_messages SET consumed_at = ? WHERE msg_id = ?", (now, row["msg_id"]))
        return [
            PeerRecord(msg_id=str(r["msg_id"]), kind=str(r["kind"]), from_addr=r["from_addr"], body=str(r["body"]),
                       ts=float(r["ts"]), record=cast(dict[str, Any], json.loads(r["record_json"])))
            for r in rows
        ]

    def take_controls(self, orig_msg_ids: Collection[str], from_addrs: Collection[str], since: float) -> list[PeerRecord]:
        # Control receipts (`held` and the like) about messages Hibernaut sent, or from the conversation's session.
        rows = self._db.fetchall(
            "SELECT msg_id, kind, from_addr, body, record_json, ts FROM peer_messages "
            "WHERE receiver = ? AND kind = 'control' AND consumed_at IS NULL AND ts >= ? ORDER BY ts, rowid",
            (self.receiver, since),
        )
        picked: list[PeerRecord] = []
        for r in rows:
            record = cast(dict[str, Any], json.loads(r["record_json"]))
            if record.get("orig_msg_id") in orig_msg_ids or r["from_addr"] in from_addrs:
                picked.append(PeerRecord(msg_id=str(r["msg_id"]), kind="control", from_addr=r["from_addr"],
                                         body=str(r["body"]), ts=float(r["ts"]), record=record))
        if picked:
            with self._db.transaction() as conn:
                now = utc_now_iso()
                for rec in picked:
                    conn.execute("UPDATE peer_messages SET consumed_at = ? WHERE msg_id = ?", (now, rec.msg_id))
        return picked
