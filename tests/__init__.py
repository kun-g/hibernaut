"""Test suite for Always-on Agent Runtime."""
import os
from pathlib import Path

# Detached maintenance after a commit, merge, fetch or push would race the copies and clones of fixture repositories.
os.environ["GIT_CONFIG_GLOBAL"] = str(Path(__file__).parent / "support" / "gitconfig")
