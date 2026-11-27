from __future__ import annotations

import io
import json
import subprocess
import tarfile
import zipfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from agent_runtime.adapters.channels.gdrive_tasks import DriveDownload, DriveFileMeta

FOLDER = "application/vnd.google-apps.folder"
SOURCES = "sources-root"


@dataclass
class _Entry:
    meta: DriveFileMeta
    blob: bytes = b""


@dataclass
class MemoryDrive:
    # An in-memory Drive: folders and files, enough for the proxy's guard and for create_repo.
    entries: dict[str, _Entry] = field(default_factory=dict[str, _Entry])
    downloads: list[str] = field(default_factory=list[str])

    def folder(self, folder_id: str, name: str, parent: str = SOURCES) -> None:
        self.entries[folder_id] = _Entry(DriveFileMeta(folder_id, name, FOLDER, "", (parent,)))

    def file(self, file_id: str, name: str, blob: bytes, parent: str, modified: str,
             mime_type: str = "application/octet-stream") -> None:
        self.entries[file_id] = _Entry(DriveFileMeta(file_id, name, mime_type, modified, (parent,), len(blob)), blob)

    def find_subfolders(self, parent_id: str) -> dict[str, str]:
        return {e.meta.name: i for i, e in self.entries.items() if e.meta.mime_type == FOLDER and parent_id in e.meta.parents}

    def list_files(self, folder_id: str) -> list[DriveFileMeta]:
        return [e.meta for e in self.entries.values() if e.meta.mime_type != FOLDER and folder_id in e.meta.parents]

    def get_file_meta(self, file_id: str) -> DriveFileMeta:
        return self.entries[file_id].meta

    def read_text(self, file_meta: DriveFileMeta) -> str:
        return self.entries[file_meta.file_id].blob.decode()

    def append_text(self, file_id: str, mime_type: str, addition: str) -> None:
        raise AssertionError("not used")

    def move_file(self, file_id: str, *, add_parent: str, remove_parent: str) -> None:
        raise AssertionError("not used")

    def download(self, file_meta: DriveFileMeta) -> DriveDownload:
        self.downloads.append(file_meta.file_id)
        return DriveDownload(file_meta.name, file_meta.mime_type, self.entries[file_meta.file_id].blob)

    # The proxy's sources operations, as ProxiedDriveClient offers them, for tests that leave the proxy out.
    def source_folder(self, *, name: str | None = None, folder_id: str | None = None) -> tuple[str, list[DriveFileMeta]]:
        from agent_runtime.adapters.channels.gdrive_tasks import DriveProxyError

        found = folder_id or self.find_subfolders(SOURCES).get(name or "")
        if found is None or found not in self.entries:
            raise DriveProxyError("source_folder", 404, f"the sources folder has no folder named '{name}'")
        return self.entries[found].meta.name, self.list_files(found)

    def download_source(self, file_meta: DriveFileMeta) -> DriveDownload:
        return self.download(file_meta)


def zipped(files: dict[str, bytes], *, links: Sequence[str] = ()) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as archive:
        for name, data in files.items():
            archive.writestr(name, data)
        for name in links:
            info = zipfile.ZipInfo(name)
            info.external_attr = (0o120777 << 16)
            archive.writestr(info, "/etc/passwd")
    return out.getvalue()


def tarred(files: dict[str, bytes]) -> bytes:
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as archive:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return out.getvalue()


def done(argv: Sequence[str], code: int = 0, out: str = "", err: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(list(argv), code, out, err)


@dataclass
class FakeGitHub:
    # gh and git as RepoCreator runs them, all in this process: repositories by name, what was pushed to each.
    existing: set[str] = field(default_factory=set[str])
    pushed: dict[str, dict[str, bytes]] = field(default_factory=dict[str, dict[str, bytes]])
    calls: list[list[str]] = field(default_factory=list[list[str]])
    # gh api answers this instead of 404 for a repository that is not there.
    api_error: str = ""
    # The push fails after the repository was created.
    push_fails: bool = False
    commits: list[str] = field(default_factory=list[str])
    on: Callable[[list[str]], None] | None = None

    def __call__(self, argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
        args = list(argv)
        self.calls.append(args)
        if self.on is not None:
            self.on(args)
        if args[:2] == ["gh", "api"]:
            repo = args[2].removeprefix("repos/")
            if repo in self.existing:
                return done(args)
            return done(args, 1, err=self.api_error or "gh: Not Found (HTTP 404)")
        if args[:3] == ["gh", "repo", "create"]:
            repo, source = args[3], Path(args[args.index("--source") + 1])
            assert "--private" in args
            self.existing.add(repo)
            if self.push_fails:
                return done(args, 1, err="remote: Permission denied")
            self.pushed[repo] = {p.relative_to(source).as_posix(): p.read_bytes()
                                 for p in sorted(source.rglob("*")) if p.is_file()}
            return done(args)
        if args[0] == "git":
            command = args[3] if args[3] != "-c" else args[5]
            if command == "commit":
                self.commits.append(args[-1])
            return done(args, out="0123456789abcdef0123456789abcdef01234567\n" if command == "rev-parse" else "")
        raise AssertionError(f"unexpected command {args}")


def dokploy_script(path: Path, state: Path) -> Path:
    # The workspace's Dokploy script as the operations tools run it: every call is logged, application.one and
    # environment.one answer from state/dokploy.json, everything else succeeds.
    path.write_text(f'''#!/bin/sh
exec {_python()} -I - "$@" <<'PY'
import json, sys
from pathlib import Path
state = Path({str(state)!r})
args = sys.argv[1:]
with open(state / "dokploy-calls.jsonl", "a") as log:
    log.write(json.dumps(args) + "\\n")
known = json.loads((state / "dokploy.json").read_text()) if (state / "dokploy.json").exists() else {{}}
if args[:1] == ["get"]:
    found = known.get(args[1] + args[2] if len(args) > 2 else args[1])
    if found is None:
        sys.exit(22)
    print(json.dumps(found))
    sys.exit(0)
if args[:1] == ["fail"] or args[:2] in (["app-deploy", "app-broken"],):
    print("deploy failed: build error", file=sys.stderr)
    sys.exit(1)
print(json.dumps({{"ok": True, "args": args, "token": "<redacted>"}}))
PY
''')
    path.chmod(0o700)
    return path


def _python() -> str:
    import sys

    return sys.executable


def dokploy_calls(state: Path) -> list[list[str]]:
    log = state / "dokploy-calls.jsonl"
    return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []


def environments(state: Path, apps: dict[str, str]) -> None:
    # application id -> environment name, as Dokploy's application.one and environment.one tell them.
    known: dict[str, Any] = {}
    for app, environment in apps.items():
        known[f"application.one?applicationId={app}"] = {"applicationId": app, "environmentId": f"env-{environment}"}
        known[f"environment.one?environmentId=env-{environment}"] = {"name": environment}
    (state / "dokploy.json").write_text(json.dumps(known))
