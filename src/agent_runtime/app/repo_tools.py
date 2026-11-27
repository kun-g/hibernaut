from __future__ import annotations

import json
import os
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
import zipfile
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import IO, Protocol

from agent_runtime.adapters.channels.gdrive_tasks import DriveDownload, DriveFileMeta, DriveProxyError
from agent_runtime.adapters.delivery.pr_delivery import remote_env
from agent_runtime.adapters.executor_cli.steward_git import ToolError
from agent_runtime.app.onboarding import OnboardError, Runner, description_text, run_command, scrub, split_repo

_PUSH_TIMEOUT_SECONDS = 900.0
_FOLDER_URL = re.compile(r"https://drive\.google\.com/(?:drive/(?:u/\d+/)?folders|open\?id=)/?([A-Za-z0-9_-]{10,200})")
_DRIVE_ID = re.compile(r"[A-Za-z0-9_-]{10,200}")
_SAFE_NAME = re.compile(r"[/\\\x00-\x1f]")
_ARCHIVES = (".zip", ".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tbz2", ".tar.xz", ".txz")
# What a packer adds that is no part of the project.
_JUNK = frozenset({"__MACOSX", ".DS_Store", ".git"})


class DriveSources(Protocol):
    # The proxy's sources folder: one folder per project, each holding its versions.
    def source_folder(self, *, name: str | None = None, folder_id: str | None = None) -> tuple[str, list[DriveFileMeta]]: ...

    def download_source(self, file_meta: DriveFileMeta) -> DriveDownload: ...


@dataclass(frozen=True, slots=True)
class _Unpacked:
    root: Path
    files: int
    archive: bool


def folder_ref(drive_folder: str | None) -> str | None:
    # A folder link or ID as a person pastes it; None looks the folder up by the repository's name.
    if drive_folder is None or not drive_folder.strip():
        return None
    text = drive_folder.strip()
    found = _FOLDER_URL.match(text)
    if found is not None:
        return found[1]
    if _DRIVE_ID.fullmatch(text):
        return text
    raise ToolError(f"drive_folder must be a Google Drive folder link or ID, got {text!r}")


def _parts(name: str) -> tuple[str, ...] | None:
    # The path inside the archive, or None for one that would land outside the folder it is unpacked into.
    path = PurePosixPath(name.replace("\\", "/"))
    if path.is_absolute() or any(p == ".." for p in path.parts):
        return None
    return tuple(p for p in path.parts if p not in ("", "."))


def _kept(parts: tuple[str, ...]) -> bool:
    return bool(parts) and not any(p in _JUNK for p in parts)


def _copy_limited(source: IO[bytes], target: Path, budget: list[int]) -> None:
    with target.open("xb") as out:
        while chunk := source.read(1 << 20):
            budget[0] -= len(chunk)
            if budget[0] < 0:
                raise ToolError("the first version is larger unpacked than [repo_tools] max_bytes allows")
            out.write(chunk)


def _unzip(path: Path, into: Path, budget: list[int]) -> None:
    with zipfile.ZipFile(path) as archive:
        for info in archive.infolist():
            parts = _parts(info.filename)
            if parts is None:
                raise ToolError(f"the archive holds a path outside itself: {info.filename!r}")
            mode = info.external_attr >> 16
            if not _kept(parts) or info.is_dir():
                continue
            if stat.S_ISLNK(mode):
                raise ToolError(f"the archive holds a symbolic link: {info.filename!r}")
            target = into.joinpath(*parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(info) as source:
                _copy_limited(source, target, budget)
            if mode & 0o111:
                target.chmod(0o755)


def _untar(path: Path, into: Path, budget: list[int]) -> None:
    with tarfile.open(path) as archive:
        for member in archive:
            parts = _parts(member.name)
            if parts is None:
                raise ToolError(f"the archive holds a path outside itself: {member.name!r}")
            if not _kept(parts) or member.isdir():
                continue
            if not member.isfile():
                raise ToolError(f"the archive holds something other than files and folders: {member.name!r}")
            target = into.joinpath(*parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            source = archive.extractfile(member)
            if source is None:
                continue
            with source:
                _copy_limited(source, target, budget)
            if member.mode & 0o111:
                target.chmod(0o755)


def _is_archive(name: str, path: Path) -> bool:
    lowered = name.lower()
    if lowered.endswith(_ARCHIVES):
        return True
    return zipfile.is_zipfile(path)


def _files(root: Path) -> Iterator[Path]:
    return (p for p in root.rglob("*") if p.is_file())


def unpack(download: DriveDownload, into: Path, max_bytes: int) -> _Unpacked:
    # The first version as the repository's files: an archive unpacked (one folder at its top taken as the root), any
    # other file as it is.
    name = _SAFE_NAME.sub("_", download.name).strip().lstrip(".") or "first-version"
    if len(download.content) > max_bytes:
        raise ToolError("the first version is larger than [repo_tools] max_bytes allows")
    packed = into / "download"
    packed.mkdir(mode=0o700)
    original = packed / name
    original.write_bytes(download.content)
    tree = into / "tree"
    tree.mkdir(mode=0o700)
    archive = _is_archive(name, original)
    if archive:
        budget = [max_bytes]
        try:
            if zipfile.is_zipfile(original):
                _unzip(original, tree, budget)
            else:
                _untar(original, tree, budget)
        except (zipfile.BadZipFile, tarfile.TarError, EOFError, OSError) as exc:
            raise ToolError(f"{name} looks like an archive but cannot be unpacked ({exc})") from exc
        top = [p for p in tree.iterdir()]
        root = top[0] if len(top) == 1 and top[0].is_dir() else tree
    else:
        shutil.copyfile(original, tree / name)
        root = tree
    count = sum(1 for _ in _files(root))
    if count == 0:
        raise ToolError(f"{name} holds no files")
    return _Unpacked(root=root, files=count, archive=archive)


def _output(done: subprocess.CompletedProcess[str]) -> str:
    return " ".join((done.stderr or done.stdout or "").split())[:500]


class RepoCreator:
    # create_repo of the general Steward. It holds no credential: gh and git use the host's own, Drive is reached through
    # the credential proxy. What it says goes through scrub().
    def __init__(
        self,
        *,
        owner: str,
        drive: Callable[[], DriveSources],
        max_bytes: int,
        # Whether the configuration in force still gives this inbox the tool.
        allowed: Callable[[], bool] = lambda: True,
        run: Runner | None = None,
        scratch: Path | None = None,
        scrubbed: Callable[[str], str] = scrub,
    ) -> None:
        self._owner = owner
        self._drive = drive
        self._max_bytes = max_bytes
        self._allowed = allowed
        # Tokens in the daemon's environment reach no command: gh and git use the host's own credentials.
        self._run: Runner = run or (lambda argv: run_command(argv, remote_env(os.environ), timeout=_PUSH_TIMEOUT_SECONDS))
        self._scratch = scratch
        self._scrubbed = scrubbed

    def create(self, *, name: str, drive_folder: str | None, description: str | None) -> str:
        if not self._allowed():
            raise ToolError("this inbox has no repo_tools any more")
        try:
            return json.dumps(self._create(name, folder_ref(drive_folder), description), ensure_ascii=False, indent=1)
        except (ToolError, OnboardError, DriveProxyError, OSError, ValueError) as exc:
            raise ToolError(self._scrubbed(str(exc))) from exc

    def _create(self, name: str, folder_id: str | None, description: str | None) -> dict[str, object]:
        repo = f"{self._owner}/{name}"
        split_repo(repo)
        about = description_text(description, repo)
        self._check_absent(repo)
        try:
            folder, files = self._drive().source_folder(name=None if folder_id else name, folder_id=folder_id)
        except DriveProxyError as exc:
            raise ToolError(f"stopped at Drive, before anything was created: {exc.reason}") from exc
        if not files:
            raise ToolError(f"stopped at Drive: the folder {folder or name} is empty; nothing was created")
        latest = max(files, key=lambda meta: (meta.modified_time, meta.name))
        with tempfile.TemporaryDirectory(prefix="hb-repo-", dir=self._scratch) as tmp:
            work = Path(tmp)
            try:
                got = self._drive().download_source(latest)
            except DriveProxyError as exc:
                raise ToolError(f"stopped at Drive, before anything was created: {latest.name}: {exc.reason}") from exc
            unpacked = unpack(got, work, self._max_bytes)
            commit = self._commit(unpacked.root, latest)
            self._publish(repo, about, unpacked.root)
        return {
            "repo": repo, "url": f"https://github.com/{repo}", "private": True, "commit": commit,
            "first_version": {"folder": folder, "file": latest.name, "modified": latest.modified_time,
                              "unpacked": unpacked.archive, "files": unpacked.files},
        }

    def _check_absent(self, repo: str) -> None:
        done = self._run(["gh", "api", f"repos/{repo}", "--silent"])
        if done.returncode == 0:
            raise ToolError(f"stopped before anything was created: {repo} exists on GitHub already, and it is left as it is")
        if "404" not in (done.stderr or "") + (done.stdout or ""):
            raise ToolError(f"stopped before anything was created: whether {repo} exists could not be told ({_output(done)})")

    def _git(self, root: Path, *args: str) -> str:
        done = self._run(["git", "-C", str(root), *args])
        if done.returncode != 0:
            raise ToolError(f"stopped at the first commit, before anything was created: git {args[0]}: {_output(done)}")
        return done.stdout.strip()

    def _commit(self, root: Path, latest: DriveFileMeta) -> str:
        self._git(root, "init", "-q", "-b", "main")
        self._git(root, "add", "-A")
        message = f"Initial commit: {latest.name} from Google Drive ({latest.modified_time})"
        self._git(root, "-c", "commit.gpgsign=false", "commit", "-q", "-m", message)
        return self._git(root, "rev-parse", "HEAD")

    def _publish(self, repo: str, about: str, root: Path) -> None:
        argv: Sequence[str] = ["gh", "repo", "create", repo, "--private", "--description", about, "--source", str(root),
                               "--remote", "origin", "--push"]
        done = self._run(argv)
        if done.returncode == 0:
            return
        if self._run(["gh", "api", f"repos/{repo}", "--silent"]).returncode == 0:
            raise ToolError(f"stopped at the push: {repo} was created (private) but the first version did not reach it "
                            f"({_output(done)}); push it by hand or delete the repository")
        raise ToolError(f"stopped at creating {repo}: {_output(done)}; nothing was created")
