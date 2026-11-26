import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.fixture(scope="session", autouse=True)
def apple_git(tmp_path_factory):
    # /usr/bin/git dispatches through xcrun on every invocation. Resolve the same git once.
    if sys.platform != "darwin" or shutil.which("git") != "/usr/bin/git":
        yield
        return
    binary = subprocess.check_output(["/usr/bin/xcrun", "--find", "git"], text=True).strip()
    bin_dir = tmp_path_factory.mktemp("git-bin")
    (bin_dir / "git").symlink_to(Path(binary))
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("PATH", str(bin_dir) + os.pathsep + os.environ["PATH"])
        yield
