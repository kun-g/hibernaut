from __future__ import annotations

import os
from collections.abc import Mapping


# Development broker: secrets come from the environment, never from config or code.
class EnvCredentialBroker:
    def __init__(self, environ: Mapping[str, str] | None = None) -> None:
        self._environ = os.environ if environ is None else environ

    def get_webhook_secret(self, channel: str) -> str | None:
        return self._environ.get(f"SWITCHBOARD_{channel.upper()}_WEBHOOK_SECRET") or None

    def issue_ephemeral(self, channel: str) -> str | None:
        return self._environ.get(f"SWITCHBOARD_{channel.upper()}_TOKEN") or None
