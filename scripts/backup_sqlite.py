"""Stream a .tgz containing a standalone snapshot of the requested SQLite database."""

import argparse
import sqlite3
import sys
import tarfile
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    database = parser.parse_args().database.resolve(strict=True)
    with TemporaryDirectory() as staging:
        snapshot = Path(staging) / database.name
        with closing(sqlite3.connect(database.as_uri() + "?mode=rw", uri=True)) as source:
            with closing(sqlite3.connect(snapshot)) as dest:
                source.backup(dest, pages=256)
        with tarfile.open(fileobj=sys.stdout.buffer, mode="w|gz") as archive:
            archive.add(snapshot, arcname=database.name)


if __name__ == "__main__":
    main()
