# Google Drive queue credential proxy

The Drive task queue runs through an external credential proxy. The proxy runs as its own system user, is the only process that can read the service account key, and calls Google Drive itself. Hibernaut sends queue operations to it on `127.0.0.1` with a local client token and receives only results: file metadata, task text, `ok`.

## Install

Run from a Hibernaut checkout on the machine that runs Hibernaut. It needs `sudo`, the service account JSON key, and the Drive ID of the queue folder (the folder holding `待执行`, `执行中`, `已完成` and `失败`, shared with the service account as Editor):

```bash
sudo deploy/gdrive-proxy/install.sh \
  --key ~/.config/hibernaut/gdrive-sa.json \
  --root-folder-id <queue folder ID from its Drive URL> \
  --hb-user "$USER"
```

Add `--https-proxy http://127.0.0.1:7890` when this machine reaches Google through a local proxy. `--dry-run` prints every command without running it. To update only the code later, see [Upgrade](#upgrade); it needs no key.

The installer:

1. Creates the service user: `_hbdriveproxy` (macOS, hidden, no login shell) or `hb-drive-proxy` (Linux, system account).
2. Copies `src/agent_runtime` to `/opt/hibernaut-drive-proxy/src`, owned by root, and installs the launcher `/opt/hibernaut-drive-proxy/bin/hibernaut-drive-proxy`. The proxy never runs code from the checkout Hibernaut can edit.
3. Installs into `/etc/hibernaut-drive-proxy/`: `sa.json` (the key, mode 0400, owned by the service user), `client.token` (random, 0400, owned by the service user) and `config.toml` (root, readable by the service user).
4. Copies the client token to `~<hb-user>/.config/hibernaut/gdrive-proxy.token` (mode 0600).
5. Checks as the service user: the Python and code load; the Hibernaut user cannot read `sa.json`; `--check` finds the four queue folders and counts their files (read-only; skip with `--no-check`).
6. Starts the service: LaunchDaemon `/Library/LaunchDaemons/ai.hibernaut.drive-proxy.plist` (log `/var/log/hibernaut-drive-proxy.log`) or systemd unit `hibernaut-drive-proxy.service` (`journalctl -u hibernaut-drive-proxy`).

It prints the `[gdrive]` section for `hibernaut.toml`. Then delete the original key file and unset `HIBERNAUT_GDRIVE_SA_KEY`; Hibernaut no longer reads either.

```toml
[gdrive]
enabled = true
proxy_url = "http://127.0.0.1:9100"
proxy_token_file = "~/.config/hibernaut/gdrive-proxy.token"
root_folder_id = "<queue folder ID>"
default_inbox_id = "proj:hibernaut"
```

Restart Hibernaut to start polling.

## Upgrade

The proxy runs its own copy of the code in `/opt/hibernaut-drive-proxy`, so a change to the proxy code (`src/agent_runtime/adapters/credential_proxy/` or anything it imports, such as a new operation) reaches it only after an upgrade. After pulling such a change, run from the checkout:

```bash
sudo deploy/gdrive-proxy/install.sh --upgrade
```

The upgrade:

1. Checks that the proxy is installed: the service user, `sa.json`, `client.token` and `config.toml` in `/etc/hibernaut-drive-proxy/`, and the LaunchDaemon or systemd unit. Otherwise it stops and points to the full install command.
2. Stages the checkout's `src/agent_runtime` in `/opt/hibernaut-drive-proxy/src.new` (owned by root, `go=rX`), checks that it loads as the service user, and writes the new launcher next to the old one. It reuses the launcher's Python unless `--python` is given.
3. Swaps by renaming: the running code moves to `src.old`, the new code becomes `src`, the old launcher is kept as `hibernaut-drive-proxy.old`.
4. Runs the read-only `--check` with the new code, as the service user and with the service's `HTTPS_PROXY`. The service still runs the previous code at this point. When the check fails, the upgrade moves `src.old` and the old launcher back, leaves the service alone and exits 1 with hints. When any earlier step fails, the installed code is not touched. `--no-check` skips the check.
5. Deletes `src.old` and the old launcher, then restarts the service (`launchctl kickstart -k system/ai.hibernaut.drive-proxy` or `systemctl restart hibernaut-drive-proxy`).

It never opens, copies or prints the key or the client token, and does not rewrite `config.toml`, the service unit, the service user or the token in the Hibernaut user's home, so the port, the queue folder ID and `HTTPS_PROXY` stay as installed. Configuration keys that new code adds have defaults (see [Configuration file](#configuration-file)); set them in `config.toml` by hand when the defaults do not fit. `--dry-run` prints the commands. Hibernaut needs no restart.

`--upgrade` refuses `--key`, `--root-folder-id`, `--hb-user`, `--port` and `--https-proxy`. To change any of those, or to rotate the key, run the full install command from [Install](#install), which needs the key file again and rewrites the key, the configuration and the service unit (keeping the client token).

## What the proxy allows

Every request is `POST /proxy/gdrive-queue/<operation>` with a JSON body and `Authorization: Bearer <client token>`. There is no pass-through URL, method or header.

| Operation | Body | Allowed when |
| :--- | :--- | :--- |
| `subfolders` | `parent_id` | `parent_id` is the configured queue root; returns only the four queue folders |
| `list` | `folder_id` | `folder_id` is one of the four queue folders |
| `meta` | `file_id` | the file sits directly in a queue folder and is not a folder |
| `read` | `file_id` | as `meta`, and it is Markdown, text or a Google Doc (exported as text); at most 2 MiB |
| `append` | `file_id`, `text` | as `read`; `text` at most 256 KiB |
| `move` | `file_id`, `add_parent`, `remove_parent` | both folders are queue folders, they differ, and the file is in `remove_parent` |

Refusals: 401 without the right token, 403 for a request carrying `Origin` (browsers) or a target outside the queue, 404 for any other path or operation, 405 for methods other than POST, 413 above 1 MiB of request body. IDs must be Drive IDs (`[A-Za-z0-9_-]`), so they cannot change the Drive query. Google error bodies are not passed on, only `Drive returned HTTP <status>`.

The proxy listens only on a loopback address and refuses to start when the key or token file is readable by group or others or owned by another user.

## Configuration file

`/etc/hibernaut-drive-proxy/config.toml`; unknown keys are rejected.

| Key | Default | Meaning |
| :--- | :--- | :--- |
| `listen_host` | `127.0.0.1` | must be a loopback address |
| `listen_port` | `9100` | |
| `service_name` | `gdrive-queue` | path segment after `/proxy/`; Hibernaut's `gdrive.proxy_service` |
| `service_account_key_file` | required | |
| `client_token_file` | required | at least 16 characters |
| `root_folder_id` | required | queue folder; subfolders found by name |
| `pending_folder_id`, `in_progress_folder_id`, `done_folder_id`, `failed_folder_id` | found by name | explicit queue folder IDs |

## Operating

- Check the queue: `sudo -u _hbdriveproxy /opt/hibernaut-drive-proxy/bin/hibernaut-drive-proxy --config /etc/hibernaut-drive-proxy/config.toml --check` (Linux: `-u hb-drive-proxy`).
- Rotate the key: rerun the installer with the new `--key`, then revoke the old key in Google Cloud.
- Rotate the client token: delete `/etc/hibernaut-drive-proxy/client.token`, rerun the installer, restart Hibernaut.
- Remove: macOS `sudo launchctl bootout system/ai.hibernaut.drive-proxy && sudo rm /Library/LaunchDaemons/ai.hibernaut.drive-proxy.plist`; Linux `sudo systemctl disable --now hibernaut-drive-proxy`. Then delete `/opt/hibernaut-drive-proxy` and `/etc/hibernaut-drive-proxy`.
