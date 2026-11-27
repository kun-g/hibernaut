import json
from pathlib import Path
from typing import Any


def read_jsonl(path: Path) -> list[Any]:
    # Fake CLIs append one record per line while tests poll the file; a last line without its newline is still being
    # written, so it is left for the next poll instead of failing to parse.
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    return [json.loads(line) for line in text.split("\n")[:-1] if line.strip()]
