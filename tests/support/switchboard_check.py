from __future__ import annotations

import base64
import contextlib
import io
import os
import tempfile
import unittest
from collections.abc import Mapping
from pathlib import Path
from unittest import mock

from switchboard.app import main


REPO = Path(__file__).resolve().parents[2]
# Structurally a PKCS#1 key (nine DER integers), so it parses; it signs nothing.
PKCS1_PEM = (
    "-----BEGIN RSA PRIVATE KEY-----\n"
    + base64.b64encode(b"\x30\x1b" + bytes([0x02, 0x01, 0x05]) * 9).decode()
    + "\n-----END RSA PRIVATE KEY-----\n"
)


class CheckHarness(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory(prefix="sb_check_")
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.path = self.root / "switchboard.toml"

    def check(self, toml: str, env: Mapping[str, str] | None = None) -> tuple[int, str, str]:
        self.path.write_text(toml, encoding="utf-8")
        out, err = io.StringIO(), io.StringIO()
        with (
            mock.patch.dict(os.environ, dict(env or {}), clear=True),
            contextlib.redirect_stdout(out),
            contextlib.redirect_stderr(err),
        ):
            code = main(["--config", str(self.path), "--check"])
        return code, out.getvalue(), err.getvalue()
