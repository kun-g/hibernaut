from __future__ import annotations

import base64
import binascii
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


def github_app_credentials(environ: Mapping[str, str] | None = None) -> tuple[str, str] | None:
    env = os.environ if environ is None else environ
    app_id = (env.get("SWITCHBOARD_GITHUB_APP_ID") or "").strip()
    raw = (env.get("SWITCHBOARD_GITHUB_APP_PRIVATE_KEY") or "").strip()
    if not app_id or not raw:
        return None
    if "-----BEGIN" in raw:
        return app_id, raw
    # A one-line env value carries the PEM base64-encoded.
    try:
        return app_id, base64.b64decode(raw, validate=True).decode("ascii").strip()
    except (binascii.Error, UnicodeDecodeError) as exc:
        raise ValueError("SWITCHBOARD_GITHUB_APP_PRIVATE_KEY is neither PEM nor base64 PEM") from exc
