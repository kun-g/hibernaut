import json
from pathlib import Path

DONE = {"role": "steward", "answers": [[False, "{}"]]}
WRITING = json.dumps({"role": "steward", "mode": "print", "prompt": "x" * 2000})


def cut(path: Path, *whole: object, part: str) -> Path:
    # What a poll sees while a fake CLI is appending its record: the earlier lines whole, the last one cut short.
    path.write_text("".join(json.dumps(w) + "\n" for w in whole) + part)
    return path
