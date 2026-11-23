# The fixed check program of the read-only pilot (docs/runner.md §8). It runs as a script (python -I -S -B <this file>)
# inside the runner's read-only worktree, reads one JSON request from stdin and writes one JSON report to stdout. It
# imports only the standard library, takes no path from its caller and reads file contents from the named commit's
# objects, so neither the worktree's state nor the requester can change what is hashed.
from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any, Final, Mapping, cast

PROTOCOL_CHECK_EXECUTOR_ID: Final = "runner-protocol-check"
CHECK_SCHEMA: Final = "runner.protocol_check.v1"
PROTOCOL_DIR: Final = "schemas/runner/v1"
PROTOCOL_FILES: Final = (
    f"{PROTOCOL_DIR}/runner.capabilities.schema.json",
    f"{PROTOCOL_DIR}/runner.interrupt.schema.json",
    f"{PROTOCOL_DIR}/runner.reclaim.schema.json",
    f"{PROTOCOL_DIR}/runner.run_request.schema.json",
    f"{PROTOCOL_DIR}/runner.run_result.schema.json",
)
JSON_SCHEMA_DRAFT: Final = "https://json-schema.org/draft/2020-12/schema"
MAX_HOLD_SECONDS: Final = 60
_MAX_REQUEST_BYTES: Final = 1024
_MAX_FILE_BYTES: Final = 1_048_576
_GIT_TIMEOUT_SECONDS: Final = 30.0
_COMMIT_RE: Final = re.compile(r"[0-9a-f]{40}")


class CheckError(Exception):
    pass


@dataclass(frozen=True)
class CheckRequest:
    commit: str
    # Pause before the check, for interrupt drills only; it does not change the report.
    hold_seconds: int = 0

    def to_json(self) -> str:
        return json.dumps(
            {"schema": CHECK_SCHEMA, "commit": self.commit, "hold_seconds": self.hold_seconds}, separators=(",", ":")
        )


def is_full_commit(value: object) -> bool:
    return isinstance(value, str) and _COMMIT_RE.fullmatch(value) is not None


def parse_request(text: str) -> CheckRequest:
    try:
        raw: object = json.loads(text)
    except ValueError:
        raise CheckError("request is not JSON") from None
    if not isinstance(raw, dict):
        raise CheckError("request is not an object")
    req = cast(dict[str, Any], raw)
    if set(req) != {"schema", "commit", "hold_seconds"} or req["schema"] != CHECK_SCHEMA:
        raise CheckError(f"request must have exactly schema={CHECK_SCHEMA!r}, commit and hold_seconds")
    if not is_full_commit(req["commit"]):
        raise CheckError("commit must be a full 40-digit lowercase commit id")
    hold = req["hold_seconds"]
    if isinstance(hold, bool) or not isinstance(hold, int) or not 0 <= hold <= MAX_HOLD_SECONDS:
        raise CheckError(f"hold_seconds must be an integer from 0 to {MAX_HOLD_SECONDS}")
    return CheckRequest(commit=req["commit"], hold_seconds=hold)


def manifest_text(digests: Mapping[str, str]) -> str:
    # The format of `shasum -a 256`, so its SHA-256 can be recomputed with git and shasum alone.
    return "".join(f"{digests[path]}  {path}\n" for path in sorted(digests))


def manifest_sha256(digests: Mapping[str, str]) -> str:
    return hashlib.sha256(manifest_text(digests).encode("utf-8")).hexdigest()


def _git(*args: str) -> bytes:
    # Replace refs would substitute other objects; a lazy fetch would reach the network.
    proc = subprocess.run(
        ["git", "--no-replace-objects", "--no-optional-locks", *args],
        env={**os.environ, "GIT_NO_LAZY_FETCH": "1"},
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=False,
        timeout=_GIT_TIMEOUT_SECONDS,
    )
    if proc.returncode != 0:
        raise CheckError(f"git {args[0]} failed (exit {proc.returncode})")
    return proc.stdout


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in pairs:
        if key in out:
            raise ValueError(f"duplicate key {key!r}")
        out[key] = value
    return out


def _schema_problem(path: str, data: bytes) -> str | None:
    try:
        raw: object = json.loads(data.decode("utf-8"), object_pairs_hook=_no_duplicate_keys)
    except ValueError as exc:
        return f"{path} is not valid JSON ({exc})"
    if not isinstance(raw, dict):
        return f"{path} is not a JSON object"
    doc = cast(dict[str, Any], raw)
    if doc.get("$schema") != JSON_SCHEMA_DRAFT:
        return f"{path} does not declare {JSON_SCHEMA_DRAFT}"
    if not str(doc.get("$id", "")).endswith(f"/{path}"):
        return f"{path} has an $id of another file"
    return None


def _blobs(commit: str) -> dict[str, tuple[str, str]]:
    found: dict[str, tuple[str, str]] = {}
    for entry in _git("ls-tree", "-rz", "--full-tree", commit, "--", f"{PROTOCOL_DIR}/").split(b"\0"):
        if entry:
            meta, _, path = entry.partition(b"\t")
            mode, _, oid = meta.decode("ascii").split(" ")
            found[path.decode("utf-8", "backslashreplace")] = (mode, oid)
    return found


def check_commit(commit: str) -> dict[str, Any]:
    try:
        kind = _git("cat-file", "-t", commit).decode("ascii").strip()
    except CheckError:
        raise CheckError(f"commit {commit} is not in this repository") from None
    if kind != "commit":
        raise CheckError(f"{commit} is a {kind}, not a commit")

    blobs = _blobs(commit)
    problems = [f"{path} is missing" for path in PROTOCOL_FILES if path not in blobs]
    problems += [f"{path} is not one of the {len(PROTOCOL_FILES)} protocol files" for path in sorted(blobs) if path not in PROTOCOL_FILES]
    files: list[dict[str, Any]] = []
    for path in PROTOCOL_FILES:
        if path not in blobs:
            continue
        mode, oid = blobs[path]
        if mode != "100644":
            problems.append(f"{path} has mode {mode}, expected 100644")
            continue
        if int(_git("cat-file", "-s", oid)) > _MAX_FILE_BYTES:
            problems.append(f"{path} is larger than {_MAX_FILE_BYTES} bytes")
            continue
        data = _git("cat-file", "blob", oid)
        problem = _schema_problem(path, data)
        if problem is not None:
            problems.append(problem)
        files.append({"path": path, "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)})
    if problems:
        raise CheckError("; ".join(problems))
    return {
        "schema": CHECK_SCHEMA,
        "commit": commit,
        "files": files,
        "manifest_sha256": manifest_sha256({f["path"]: f["sha256"] for f in files}),
    }


def main() -> int:
    try:
        raw = sys.stdin.buffer.read(_MAX_REQUEST_BYTES + 1)
        if len(raw) > _MAX_REQUEST_BYTES:
            raise CheckError("request is too long")
        req = parse_request(raw.decode("utf-8", "replace"))
        if req.hold_seconds:
            time.sleep(req.hold_seconds)
        report = check_commit(req.commit)
    except (CheckError, OSError, subprocess.TimeoutExpired) as exc:
        print(f"protocol check failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
