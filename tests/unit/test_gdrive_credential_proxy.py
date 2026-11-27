from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from typing import Any

from agent_runtime.adapters.channels import ProxiedDriveClient
from agent_runtime.adapters.credential_proxy.gdrive_queue import DriveProxyConfig, load_proxy_config
from agent_runtime.app.config import parse_config
from agent_runtime.app.main import build_daemon_from_config
from tests.support.gdrive_credential_proxy import CLIENT_TOKEN


class DriveProxyStartupTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="gdrive_proxy_cfg_")
        self.root = Path(self._tmp.name)
        self.key = self.root / "sa.json"
        self.key.write_text(json.dumps({"client_email": "x@y", "private_key": "TEST", "token_uri": "http://127.0.0.1:1/t"}))
        os.chmod(self.key, 0o600)
        self.token = self.root / "client.token"
        self.token.write_text(CLIENT_TOKEN + "\n")
        os.chmod(self.token, 0o600)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _write(self, **overrides: Any) -> Path:
        values: dict[str, Any] = {
            "listen_host": "127.0.0.1",
            "listen_port": 9100,
            "service_account_key_file": str(self.key),
            "client_token_file": str(self.token),
            "root_folder_id": "root-q",
        }
        values.update(overrides)
        lines = [f"{k} = {json.dumps(v)}" for k, v in values.items()]
        path = self.root / "proxy.toml"
        path.write_text("\n".join(lines) + "\n")
        return path

    def test_loads_a_valid_config(self) -> None:
        cfg = load_proxy_config(self._write())
        self.assertIsInstance(cfg, DriveProxyConfig)
        self.assertEqual(cfg.client_token, CLIENT_TOKEN)
        self.assertEqual(cfg.service_name, "gdrive-queue")
        self.assertEqual(cfg.root_folder_id, "root-q")

    def test_source_links_require_explicit_opt_in(self) -> None:
        self.assertFalse(load_proxy_config(self._write()).allow_any_source_folder)
        self.assertTrue(load_proxy_config(self._write(allow_any_source_folder=True)).allow_any_source_folder)
        with self.assertRaisesRegex(ValueError, "must be a boolean"):
            load_proxy_config(self._write(allow_any_source_folder="false"))

    def test_attachment_settings(self) -> None:
        cfg = load_proxy_config(self._write())
        self.assertEqual((cfg.attachments_folder_id, cfg.max_download_bytes), ("", 100 * 1024 * 1024))
        cfg = load_proxy_config(self._write(attachments_folder_id="dir-attach", max_download_bytes=1024))
        self.assertEqual((cfg.attachments_folder_id, cfg.max_download_bytes), ("dir-attach", 1024))

    def test_rejects_unsafe_settings(self) -> None:
        cases: dict[str, dict[str, Any]] = {
            "max_download_bytes must be a positive integer": {"max_download_bytes": 0},
            "attachments_folder_id must be a Drive folder ID": {"attachments_folder_id": "a/b"},
            "listen_host must be a loopback": {"listen_host": "0.0.0.0"},
            "root_folder_id": {"root_folder_id": ""},
            "unknown key": {"upstream_url": "https://example.com"},
        }
        for pattern, overrides in cases.items():
            with self.subTest(pattern), self.assertRaisesRegex(ValueError, pattern):
                load_proxy_config(self._write(**overrides))

    def test_rejects_key_or_token_files_readable_by_others(self) -> None:
        for path in (self.key, self.token):
            with self.subTest(path.name):
                os.chmod(path, 0o644)
                with self.assertRaisesRegex(ValueError, "readable by group or others"):
                    load_proxy_config(self._write())
                os.chmod(path, 0o600)

    def test_rejects_an_empty_client_token(self) -> None:
        self.token.write_text("\n")
        with self.assertRaisesRegex(ValueError, "client_token_file is empty"):
            load_proxy_config(self._write())


class HibernautDriveProxyConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="gdrive_hb_cfg_")
        self.root = Path(self._tmp.name)
        self.token = self.root / "gdrive-proxy.token"
        self.token.write_text(CLIENT_TOKEN)
        os.chmod(self.token, 0o600)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _cfg(self, **gdrive: Any) -> dict[str, Any]:
        section = {
            "enabled": True,
            "proxy_url": "http://127.0.0.1:9100",
            "proxy_token_file": str(self.token),
            "root_folder_id": "root-q",
        }
        section.update(gdrive)
        return {
            "runtime": {"db_path": str(self.root / "hb.db"), "memory_root": str(self.root / "mem")},
            "gdrive": section,
        }

    def test_enabled_queue_goes_through_the_credential_proxy(self) -> None:
        cfg = parse_config(self._cfg())
        self.assertEqual(cfg.gdrive.proxy_url, "http://127.0.0.1:9100")
        self.assertEqual(cfg.gdrive.proxy_service, "gdrive-queue")
        daemon = build_daemon_from_config(cfg, demo=True)
        try:
            assert daemon.gdrive_poller is not None
            self.assertIsInstance(daemon.gdrive_poller._client, ProxiedDriveClient)  # pyright: ignore[reportPrivateUsage]
        finally:
            daemon.close()

    def test_attachments_land_next_to_the_database_unless_configured(self) -> None:
        cfg = parse_config(self._cfg())
        self.assertEqual((cfg.gdrive.attachments_dir, cfg.gdrive.attachments_folder_id), ("", ""))
        daemon = build_daemon_from_config(cfg, demo=True)
        try:
            writer = daemon.gdrive_result_writer
            assert writer is not None
            store = writer._attachments  # pyright: ignore[reportPrivateUsage]
            assert store is not None
            self.assertEqual(store.root, (self.root / "attachments").resolve())
        finally:
            daemon.close()
        cfg = parse_config(self._cfg(attachments_dir="att", attachments_folder_id="dir-attach"), base_dir=self.root)
        self.assertEqual((cfg.gdrive.attachments_dir, cfg.gdrive.attachments_folder_id),
                         (str(self.root / "att"), "dir-attach"))

    def test_proxy_url_defaults_to_the_credential_proxy(self) -> None:
        raw = self._cfg(proxy_url="")
        raw["credential_proxy"] = {"url": "http://127.0.0.1:9000"}
        self.assertEqual(parse_config(raw).gdrive.proxy_url, "http://127.0.0.1:9000")

    def test_relative_token_file_resolves_against_the_config_directory(self) -> None:
        cfg = parse_config(self._cfg(proxy_token_file="gdrive-proxy.token"), base_dir=self.root)
        self.assertEqual(cfg.gdrive.proxy_token_file, str(self.token))

    def test_rejects_a_key_in_the_hibernaut_process_and_missing_proxy_settings(self) -> None:
        cases: dict[str, dict[str, Any]] = {
            "service_account_key_env is no longer supported": {"service_account_key_env": "HIBERNAUT_GDRIVE_SA_KEY"},
            "gdrive.proxy_url": {"proxy_url": ""},
            "gdrive.proxy_url must be an http": {"proxy_url": "file:///tmp/x"},
            "gdrive.proxy_token_file": {"proxy_token_file": ""},
        }
        for pattern, overrides in cases.items():
            with self.subTest(pattern), self.assertRaisesRegex(ValueError, pattern):
                parse_config(self._cfg(**overrides))

    def test_unreadable_or_shared_token_file_skips_the_poller(self) -> None:
        os.chmod(self.token, 0o644)
        daemon = build_daemon_from_config(parse_config(self._cfg()), demo=True)
        try:
            self.assertIsNone(daemon.gdrive_poller)
        finally:
            daemon.close()

    def test_disabled_queue_needs_no_proxy(self) -> None:
        cfg = parse_config({"gdrive": {"enabled": False}})
        self.assertFalse(cfg.gdrive.enabled)


if __name__ == "__main__":
    unittest.main()
