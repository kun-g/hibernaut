from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, cast

from runner import protocol_check
from runner.cli_drivers import CommandSpec, ParsedCLIOutput
from runner.protocol import DriverKind, ExecutorCapability, InteractiveSurfaceKind, RunRequest

# The runner's own copy of the program, never one from the worktree under check.
CHECK_PROGRAM: Final = Path(protocol_check.__file__).resolve()


@dataclass(frozen=True, slots=True)
class ProtocolCheckDriver:
    executor_id: str = protocol_check.PROTOCOL_CHECK_EXECUTOR_ID
    driver_kind: DriverKind = "custom"

    def capability(self, *, interactive_surface: InteractiveSurfaceKind = "none") -> ExecutorCapability:
        del interactive_surface
        return ExecutorCapability(
            executor_id=self.executor_id, driver=self.driver_kind, supports_resume=False, supports_stream_json=False
        )

    def build_command(self, req: RunRequest, workdir: Path) -> CommandSpec:
        # A fixed command line: the request only reaches the program's stdin, which accepts a commit id and nothing
        # that names a file, a command or an option. -I/-S keep the worktree and site packages off its import path.
        del workdir
        return CommandSpec(argv=(sys.executable, "-I", "-S", "-B", str(CHECK_PROGRAM)), stdin_text=req.prompt)

    def parse_output(self, stdout: str, stderr: str, exit_code: int | None) -> ParsedCLIOutput:
        del stderr
        if exit_code != 0:
            return ParsedCLIOutput()
        try:
            raw: object = json.loads(stdout)
        except ValueError:
            return ParsedCLIOutput()
        return ParsedCLIOutput(structured_output=cast(dict[str, Any], raw) if isinstance(raw, dict) else None)
