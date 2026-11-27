import shutil
import subprocess
import sys
from pathlib import Path


def test_proxy_loads_with_only_staged_agent_runtime(tmp_path: Path) -> None:
    src = Path(__file__).resolve().parents[3] / "src"
    shutil.copytree(src / "agent_runtime", tmp_path / "agent_runtime",
                    ignore=shutil.ignore_patterns("__pycache__"))
    # -I mirrors the installer's check. -S also excludes editable installs in site-packages.
    result = subprocess.run(
        [sys.executable, "-I", "-S", "-c",
         "import sys; sys.path.insert(0, sys.argv[1]); "
         "import agent_runtime.adapters.credential_proxy.gdrive_queue", str(tmp_path)],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
