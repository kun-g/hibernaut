from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from agent_runtime.core.models import Event

# The payload field of a task whose end is recorded in its project's memory, under the goal it names.
ATTEMPT_NOTES_KEY = "attempt_notes"
MAX_GOAL_CHARS = 64
# It becomes a file name. Lower case only: on a volume that folds case two spellings would be one file listed twice.
_GOAL = re.compile(r"[a-z0-9][a-z0-9_-]*")


def check_goal(value: object) -> str:
    if not isinstance(value, str) or len(value) > MAX_GOAL_CHARS or not _GOAL.fullmatch(value):
        raise ValueError(
            f"{ATTEMPT_NOTES_KEY} must name a goal such as \"test-speed\" (lower-case letters, digits, - and _, starting "
            f"with a letter or digit, at most {MAX_GOAL_CHARS} characters); leave it out to record nothing"
        )
    return value


def check_payload(payload: Mapping[str, Any]) -> None:
    if ATTEMPT_NOTES_KEY in payload:
        check_goal(payload[ATTEMPT_NOTES_KEY])


def attempt_goal(events: Sequence[Event]) -> tuple[Event, str] | None:
    # The last event that names one, as for the other task fields.
    for event in reversed(events):
        if ATTEMPT_NOTES_KEY in event.payload:
            return event, check_goal(event.payload[ATTEMPT_NOTES_KEY])
    return None
