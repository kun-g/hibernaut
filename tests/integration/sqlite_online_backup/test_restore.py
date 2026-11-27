import sqlite3
import subprocess
import sys
import tarfile
from contextlib import closing
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[3] / "scripts/backup_sqlite.py"


def test_restore_includes_committed_wal_during_write(tmp_path):
    db = tmp_path / "board.db"
    archive = tmp_path / "backup.tgz"
    with closing(sqlite3.connect(db)) as writer:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("CREATE TABLE tasks(id INTEGER PRIMARY KEY)")
        writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        writer.execute("INSERT INTO tasks VALUES(1)")
        writer.commit()
        writer.execute("INSERT INTO tasks VALUES(2)")
        with archive.open("wb") as output:
            subprocess.run([sys.executable, SCRIPT, db], stdout=output, check=True, timeout=10)

    restored = tmp_path / "restore" / db.name
    restored.parent.mkdir()
    with tarfile.open(archive) as tar:
        assert tar.getnames() == [db.name]
        restored.write_bytes(tar.extractfile(db.name).read())
    with closing(sqlite3.connect(restored)) as snapshot:
        assert snapshot.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert snapshot.execute("SELECT id FROM tasks").fetchall() == [(1,)]
