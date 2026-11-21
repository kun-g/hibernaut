from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class WebhookResponse:
    status_code: int
    body: dict[str, Any]
