# Google Drive queue credential proxy

The Drive task queue runs through an external credential proxy. The proxy runs as its own system user, is the only process that can read the service account key, and calls Google Drive itself. Hibernaut sends queue operations to it on `127.0.0.1` with a local client token and receives only results: file metadata, task text, attachment content, `ok`.

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
2. Copies `src/agent_runtime` to `/opt/hibernaut-drive-proxy/src`, owned by root, and installs the launcher `/opt/hibernaut-drive-proxy/bin/hibernaut-drive-proxy`. The staged `agent_runtime` package and the Python standard library suffice for the launcher's import check. The proxy never runs code from the checkout Hibernaut can edit.
3. Installs into `/etc/hibernaut-drive-proxy/`: `sa.json` (the key, mode 0400, owned by the service user), `client.token` (random, 0400, owned by the service user) and `config.toml` (root, readable by the service user).
4. Copies the client token to `~<hb-user>/.config/hibernaut/gdrive-proxy.token` (mode 0600).
5. Checks as the service user: the Python and code load; the Hibernaut user cannot read `sa.json`; `--check` finds the four queue folders and the optional `附件` folder and counts their files (read-only; skip with `--no-check`).
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

## Queue folder layout

```
<queue folder>/
  待执行/   task files waiting to run
  执行中/   tasks whose Run has started (and suspended ones)
  已完成/   finished tasks, result appended
  失败/     failed tasks, error appended
  附件/     optional: files tasks reference (zip, PDF, Google Docs, ...)
```

A task references attachments in two ways; Hibernaut takes the union, each file once:

- frontmatter `attachments`: Drive file IDs, or exact names of files in `附件/` (a name two files share must be given by ID). Three spellings give the same result: `attachments: a, b`, a YAML inline list `attachments: [a, "b"]` (items may be quoted), or a block list with one `- a` per line under `attachments:`. Anything else, such as an unclosed `[a, b`, is split on commas as written, and a part that is not found is reported with its raw text;
- the file's Drive ID anywhere in the task text, such as a link `https://drive.google.com/file/d/<ID>/view`, as long as a file directly in `附件/` has that ID. An ID of a file anywhere else is not an attachment.

When the task's Run starts, Hibernaut downloads them to `gdrive.attachments_dir/<task file ID>/<attachment file ID>/<name>` (outside every worktree, directories 0700, files 0600) and appends a `## 附件` section to the task text the executor gets: name, absolute local path, size and sha256 of each file. A reference that is not in `附件/`, too large or refused is listed there with its reason, and the task starts anyway; a reference that only differs by brackets or quotes from a file fetched anyway (the same ID also in the task text) is not listed. When `附件/` cannot be listed at all, each frontmatter reference gets that reason, and one more line says the IDs in the task text could not be resolved. A Google Doc is saved as its Markdown export (`.md`). The directory is removed when the task's session closes (checked after every Run) or the task fails.

### Upload `.md` task files without converting them

A task file converted to a Google Doc loses its layout: the opening `---` becomes a horizontal rule and the metadata lines become one paragraph or heading. Hibernaut now exports Google Docs as Markdown and reads such metadata back (`scope: … priority: … title: …` on one line, or as a heading), but the plain `.md` file is the reliable form. In Drive on the web, open Settings → General → Uploads and turn off "Convert uploaded files to Google Docs editor format"; Google Drive for desktop uploads `.md` files unconverted.

## What the proxy allows

Every request is `POST /proxy/gdrive-queue/<operation>` with a JSON body and `Authorization: Bearer <client token>`. There is no pass-through URL, method or header.

| Operation | Body | Allowed when |
| :--- | :--- | :--- |
| `subfolders` | `parent_id` | `parent_id` is the configured queue root; returns only the four queue folders and `附件` when there is one |
| `list` | `folder_id` | `folder_id` is one of the four queue folders or `附件`; returns every file, following Drive's pages |
| `meta` | `file_id` | the file sits directly in a queue folder (not `附件`) and is not a folder |
| `read` | `file_id` | as `meta`, and it is Markdown, text or a Google Doc; at most 2 MiB |
| `append` | `file_id`, `text` | as `read`; `text` at most 256 KiB |
| `move` | `file_id`, `add_parent`, `remove_parent` | both folders are queue folders (not `附件`), they differ, and the file is in `remove_parent` |
| `download` | `file_id` | the file sits directly in `附件` (not in a subfolder) and is not a folder; at most `max_download_bytes` |
| `source_folder` | `name` or `folder_id` | by name, finds a direct child of `sources_folder_id`; by ID, checks the requested folder is a direct child unless `allow_any_source_folder` is enabled. Lists direct files across all pages |
| `source_download` | `file_id`, optional `folder_id` | checks the same folder policy and verifies direct parentage. Without `folder_id`, requires a direct child of `sources_folder_id` as parent. Files only, up to `max_download_bytes` |

`source_folder` and `source_download` serve `create_repo` of the general Steward (a new repository's first commit is the newest file of a project's folder). By default the configured sources root limits both operations. Setting `allow_any_source_folder = true` lets every proxy token holder use an explicit folder ID anywhere the service account can read, including queue folders. Share the project folder with the service account as a Viewer.

`read` and `download` export a Google Doc as `text/markdown`, falling back to `text/plain` when Drive refuses the Markdown export. `append` reads the Doc as plain text and writes plain text back, as before. `download` answers 200 with the file's bytes unchanged (`Content-Type: application/octet-stream`) and the name (percent-encoded), type and export type in the `X-Drive-File-Name`, `X-Drive-Mime-Type` and `X-Drive-Export-Mime-Type` headers; its refusals are JSON like every other operation. It refuses other Google files (Sheets, Slides: 415; export them to a file first), and a file over the limit with 413, from Drive's size before fetching anything (a Doc export, which has no size, after the export).

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
| `attachments_folder_id` | found by name (`附件`) | explicit attachments folder ID; without one and without a `附件` folder, `download` is refused |
| `sources_folder_id` | none | folder whose subfolders hold each project's versions (shared with the service account, read-only is enough); required for name lookup and for clients that download by file ID alone |
| `allow_any_source_folder` | `false` | allow an explicit project folder ID shared with the service account, independently of `sources_folder_id` |
| `max_download_bytes` | `104857600` (100 MiB) | largest attachment `download` returns. Big enough for a code bundle or a scanned PDF; the proxy and Hibernaut each hold one download in memory while passing it on, so this also bounds their memory per request |

## Operating

- Check the queue: `sudo -u _hbdriveproxy /opt/hibernaut-drive-proxy/bin/hibernaut-drive-proxy --config /etc/hibernaut-drive-proxy/config.toml --check` (Linux: `-u hb-drive-proxy`).
- Rotate the key: rerun the installer with the new `--key`, then revoke the old key in Google Cloud.
- Rotate the client token: delete `/etc/hibernaut-drive-proxy/client.token`, rerun the installer, restart Hibernaut.
- Remove: macOS `sudo launchctl bootout system/ai.hibernaut.drive-proxy && sudo rm /Library/LaunchDaemons/ai.hibernaut.drive-proxy.plist`; Linux `sudo systemctl disable --now hibernaut-drive-proxy`. Then delete `/opt/hibernaut-drive-proxy` and `/etc/hibernaut-drive-proxy`.
