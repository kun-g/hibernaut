from contextlib import contextmanager
from unittest.mock import patch

from agent_runtime.adapters.executor_cli import reviewer, sde
from agent_runtime.adapters.executor_cli.project_guidance import GuidancePin, ProjectGuidance


@contextmanager
def empty_project_guidance():
    def load(pin: GuidancePin) -> ProjectGuidance:
        return ProjectGuidance(pin, "", (), False)

    with patch.object(sde, "load_guidance", load), patch.object(reviewer, "load_guidance", load):
        yield
