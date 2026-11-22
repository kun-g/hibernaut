#!/usr/bin/env bash
# Installs the Hibernaut Google Drive queue credential proxy as a system service that runs under its
# own user. Only that user can read the service account key; Hibernaut gets a local client token.
# Usage and file layout: deploy/gdrive-proxy/README.md
set -euo pipefail

PREFIX=/opt/hibernaut-drive-proxy
ETC=/etc/hibernaut-drive-proxy
LAUNCHER="$PREFIX/bin/hibernaut-drive-proxy"
CONFIG="$ETC/config.toml"
MODULE=agent_runtime.adapters.credential_proxy.gdrive_queue
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/src/agent_runtime"

KEY="" ROOT_FOLDER_ID="" HB_USER="" PYTHON_BIN="" PORT=9100 HTTPS_PROXY_URL="" PLATFORM="" DRY_RUN=0 RUN_CHECK=1

usage() {
  cat <<'EOF'
sudo deploy/gdrive-proxy/install.sh --key PATH --root-folder-id ID --hb-user USER [options]

  --key PATH            service account JSON key (copied; the original is left for you to delete)
  --root-folder-id ID   Drive ID of the task queue folder (待执行/执行中/已完成/失败 live under it)
  --hb-user USER        account that runs Hibernaut; receives the client token in ~/.config/hibernaut
  --python PATH         Python 3.11+ outside home directories (default: first one found)
  --port N              loopback port (default 9100)
  --https-proxy URL     HTTPS_PROXY for the proxy's own calls to Google (e.g. http://127.0.0.1:7890)
  --no-check            skip the read-only queue check against Google Drive
  --dry-run             print the commands instead of running them
  --platform P          macos | linux (default: this machine)
EOF
}

die() { echo "install.sh: $*" >&2; exit 1; }

while [ $# -gt 0 ]; do
  case "$1" in
    --key) KEY="${2:-}"; shift 2 ;;
    --root-folder-id) ROOT_FOLDER_ID="${2:-}"; shift 2 ;;
    --hb-user) HB_USER="${2:-}"; shift 2 ;;
    --python) PYTHON_BIN="${2:-}"; shift 2 ;;
    --port) PORT="${2:-}"; shift 2 ;;
    --https-proxy) HTTPS_PROXY_URL="${2:-}"; shift 2 ;;
    --platform) PLATFORM="${2:-}"; shift 2 ;;
    --no-check) RUN_CHECK=0; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "unknown option: $1" ;;
  esac
done

[ -n "$KEY" ] && [ -n "$ROOT_FOLDER_ID" ] && [ -n "$HB_USER" ] || { usage >&2; die "--key, --root-folder-id and --hb-user are required"; }
[[ "$ROOT_FOLDER_ID" =~ ^[A-Za-z0-9_-]{1,200}$ ]] || die "--root-folder-id is not a Drive ID"
[[ "$HB_USER" =~ ^[A-Za-z_][A-Za-z0-9_.-]*$ ]] || die "--hb-user is not a user name"
[[ "$PORT" =~ ^[0-9]+$ ]] && [ "$PORT" -ge 1 ] && [ "$PORT" -le 65535 ] || die "--port must be 1-65535"
[ -z "$HTTPS_PROXY_URL" ] || [[ "$HTTPS_PROXY_URL" =~ ^https?://[A-Za-z0-9.:@_-]+/?$ ]] || die "--https-proxy must be http(s)://host:port"
[ -r "$KEY" ] || die "cannot read key file $KEY"
grep -q '"private_key"' "$KEY" || die "$KEY is not a service account JSON key (no private_key)"
[ -d "$SRC_DIR" ] || die "run this script from a Hibernaut checkout ($SRC_DIR is missing)"

if [ -z "$PLATFORM" ]; then
  case "$(uname -s)" in Darwin) PLATFORM=macos ;; Linux) PLATFORM=linux ;; *) die "unsupported OS $(uname -s)" ;; esac
fi
case "$PLATFORM" in
  macos) SVC_USER=_hbdriveproxy; ROOT_GROUP=wheel; LABEL=ai.hibernaut.drive-proxy
         PLIST="/Library/LaunchDaemons/$LABEL.plist"; LOG=/var/log/hibernaut-drive-proxy.log ;;
  linux) SVC_USER=hb-drive-proxy; ROOT_GROUP=root; UNIT=/etc/systemd/system/hibernaut-drive-proxy.service ;;
  *) die "--platform must be macos or linux" ;;
esac

run() {
  if [ "$DRY_RUN" = 1 ]; then
    printf '+'; printf ' %q' "$@"; printf '\n'
  else
    "$@"
  fi
}

[ "$DRY_RUN" = 1 ] || [ "$(id -u)" -eq 0 ] || die "run with sudo (creates a system user and service)"

python_ok() {
  case "$1" in /Users/*|/home/*|/root/*) return 1 ;; esac
  [ -x "$1" ] && "$1" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)' 2>/dev/null
}
if [ -z "$PYTHON_BIN" ]; then
  for cand in /opt/homebrew/bin/python3 /usr/local/bin/python3 /usr/bin/python3 \
              /opt/homebrew/bin/python3.1{4,3,2,1} /usr/local/bin/python3.1{4,3,2,1} /usr/bin/python3.1{4,3,2,1}; do
    if python_ok "$cand"; then PYTHON_BIN="$cand"; break; fi
  done
  [ -n "$PYTHON_BIN" ] || die "no Python 3.11+ outside home directories found; pass --python"
fi
case "$PYTHON_BIN" in
  /Users/*|/home/*|/root/*) die "--python must live outside home directories (the proxy user cannot read them)" ;;
  /*) ;;
  *) die "--python must be an absolute path" ;;
esac
[ "$DRY_RUN" = 1 ] || python_ok "$PYTHON_BIN" || die "$PYTHON_BIN is not Python 3.11+"

if id -u "$HB_USER" >/dev/null 2>&1; then
  HB_GROUP="$(id -gn "$HB_USER")"
  if [ "$PLATFORM" = macos ]; then
    HB_HOME="$(dscl . -read "/Users/$HB_USER" NFSHomeDirectory | awk '{print $2}')"
  else
    HB_HOME="$(getent passwd "$HB_USER" | cut -d: -f6)"
  fi
else
  [ "$DRY_RUN" = 1 ] || die "user $HB_USER does not exist"
  HB_GROUP="$HB_USER"
  if [ "$PLATFORM" = macos ]; then HB_HOME="/Users/$HB_USER"; else HB_HOME="/home/$HB_USER"; fi
fi
HB_TOKEN_DIR="$HB_HOME/.config/hibernaut"
HB_TOKEN="$HB_TOKEN_DIR/gdrive-proxy.token"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
chmod 700 "$TMP"

echo "== 1/6 service user $SVC_USER"
if id -u "$SVC_USER" >/dev/null 2>&1; then
  echo "exists"
elif [ "$PLATFORM" = linux ]; then
  run useradd --system --user-group --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin "$SVC_USER"
else
  if [ "$DRY_RUN" = 1 ]; then
    SVC_ID=FREE_ID
  else
    used="$( (dscl . -list /Users UniqueID; dscl . -list /Groups PrimaryGroupID) | awk '{print $2}')"
    SVC_ID=""
    for i in $(seq 400 499); do
      if ! printf '%s\n' "$used" | grep -qx "$i"; then SVC_ID=$i; break; fi
    done
    [ -n "$SVC_ID" ] || die "no free UID/GID between 400 and 499"
  fi
  run dscl . -create "/Groups/$SVC_USER"
  run dscl . -create "/Groups/$SVC_USER" PrimaryGroupID "$SVC_ID"
  run dscl . -create "/Groups/$SVC_USER" RealName "Hibernaut Drive proxy"
  run dscl . -create "/Users/$SVC_USER"
  run dscl . -create "/Users/$SVC_USER" UniqueID "$SVC_ID"
  run dscl . -create "/Users/$SVC_USER" PrimaryGroupID "$SVC_ID"
  run dscl . -create "/Users/$SVC_USER" UserShell /usr/bin/false
  run dscl . -create "/Users/$SVC_USER" NFSHomeDirectory /var/empty
  run dscl . -create "/Users/$SVC_USER" RealName "Hibernaut Drive proxy"
  run dscl . -create "/Users/$SVC_USER" Password '*'
  run dscl . -create "/Users/$SVC_USER" IsHidden 1
fi

echo "== 2/6 code in $PREFIX (root-owned, so neither Hibernaut nor the proxy can change it)"
run install -d -m 0755 -o root -g "$ROOT_GROUP" "$PREFIX" "$PREFIX/bin"
run rm -rf "$PREFIX/src.new"
run install -d -m 0755 -o root -g "$ROOT_GROUP" "$PREFIX/src.new"
run cp -R "$SRC_DIR" "$PREFIX/src.new/agent_runtime"
run find "$PREFIX/src.new" -name __pycache__ -prune -exec rm -rf {} +
run chown -R "root:$ROOT_GROUP" "$PREFIX/src.new"
run chmod -R u=rwX,go=rX "$PREFIX/src.new"
run rm -rf "$PREFIX/src"
run mv "$PREFIX/src.new" "$PREFIX/src"
cat >"$TMP/launcher" <<EOF
#!$PYTHON_BIN -I
import sys
sys.path.insert(0, "$PREFIX/src")
from $MODULE import main
raise SystemExit(main())
EOF
run install -m 0755 -o root -g "$ROOT_GROUP" "$TMP/launcher" "$LAUNCHER"

echo "== 3/6 key, client token and config in $ETC"
run install -d -m 0750 -o root -g "$SVC_USER" "$ETC"
run install -m 0400 -o "$SVC_USER" -g "$SVC_USER" "$KEY" "$ETC/sa.json"
if [ "$DRY_RUN" = 0 ] && [ -s "$ETC/client.token" ]; then
  cp "$ETC/client.token" "$TMP/client.token"
  echo "keeping the existing client token"
else
  (umask 077; od -An -tx1 -N32 /dev/urandom | tr -d ' \n' >"$TMP/client.token")
  run install -m 0400 -o "$SVC_USER" -g "$SVC_USER" "$TMP/client.token" "$ETC/client.token"
fi
cat >"$TMP/config.toml" <<EOF
# Hibernaut Drive queue credential proxy; written by deploy/gdrive-proxy/install.sh
listen_host = "127.0.0.1"
listen_port = $PORT
service_name = "gdrive-queue"
service_account_key_file = "$ETC/sa.json"
client_token_file = "$ETC/client.token"
root_folder_id = "$ROOT_FOLDER_ID"
EOF
run install -m 0440 -o root -g "$SVC_USER" "$TMP/config.toml" "$CONFIG"

echo "== 4/6 client token for $HB_USER at $HB_TOKEN"
if [ "$DRY_RUN" = 1 ] || [ ! -d "$HB_TOKEN_DIR" ]; then
  run install -d -m 0700 -o "$HB_USER" -g "$HB_GROUP" "$HB_TOKEN_DIR"
fi
run install -m 0600 -o "$HB_USER" -g "$HB_GROUP" "$TMP/client.token" "$HB_TOKEN"

echo "== 5/6 checks as $SVC_USER"
PROXY_ENV=()
[ -z "$HTTPS_PROXY_URL" ] || PROXY_ENV=("HTTPS_PROXY=$HTTPS_PROXY_URL")
run sudo -u "$SVC_USER" "$PYTHON_BIN" -I -c "import sys; sys.path.insert(0, '$PREFIX/src'); import $MODULE"
if [ "$DRY_RUN" = 1 ]; then
  run sudo -u "$HB_USER" test -r "$ETC/sa.json"
elif sudo -u "$HB_USER" test -r "$ETC/sa.json" 2>/dev/null; then
  die "$HB_USER can read $ETC/sa.json; the key is not isolated"
else
  echo "ok: $HB_USER cannot read $ETC/sa.json"
fi
if [ "$RUN_CHECK" = 1 ]; then
  run sudo -u "$SVC_USER" env ${PROXY_ENV[@]+"${PROXY_ENV[@]}"} "$LAUNCHER" --config "$CONFIG" --check
fi

echo "== 6/6 service"
if [ "$PLATFORM" = linux ]; then
  {
    echo "[Unit]"
    echo "Description=Hibernaut Google Drive queue credential proxy"
    echo "After=network-online.target"
    echo "Wants=network-online.target"
    echo
    echo "[Service]"
    echo "User=$SVC_USER"
    echo "Group=$SVC_USER"
    echo "ExecStart=$LAUNCHER --config $CONFIG"
    [ -z "$HTTPS_PROXY_URL" ] || echo "Environment=HTTPS_PROXY=$HTTPS_PROXY_URL"
    echo "Restart=on-failure"
    echo "RestartSec=5"
    echo "NoNewPrivileges=yes"
    echo "ProtectSystem=strict"
    echo "ProtectHome=yes"
    echo "PrivateTmp=yes"
    echo "PrivateDevices=yes"
    echo "ProtectKernelTunables=yes"
    echo "ProtectControlGroups=yes"
    echo "RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX"
    echo
    echo "[Install]"
    echo "WantedBy=multi-user.target"
  } >"$TMP/unit"
  run install -m 0644 -o root -g root "$TMP/unit" "$UNIT"
  run systemctl daemon-reload
  run systemctl enable --now hibernaut-drive-proxy
  run systemctl restart hibernaut-drive-proxy
  LOGS="journalctl -u hibernaut-drive-proxy"
else
  {
    echo '<?xml version="1.0" encoding="UTF-8"?>'
    echo '<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">'
    echo '<plist version="1.0"><dict>'
    echo "  <key>Label</key><string>$LABEL</string>"
    echo "  <key>UserName</key><string>$SVC_USER</string>"
    echo "  <key>GroupName</key><string>$SVC_USER</string>"
    echo "  <key>ProgramArguments</key><array><string>$LAUNCHER</string><string>--config</string><string>$CONFIG</string></array>"
    if [ -n "$HTTPS_PROXY_URL" ]; then
      echo "  <key>EnvironmentVariables</key><dict><key>HTTPS_PROXY</key><string>$HTTPS_PROXY_URL</string></dict>"
    fi
    echo "  <key>RunAtLoad</key><true/>"
    echo "  <key>KeepAlive</key><true/>"
    echo "  <key>StandardOutPath</key><string>$LOG</string>"
    echo "  <key>StandardErrorPath</key><string>$LOG</string>"
    echo '</dict></plist>'
  } >"$TMP/plist"
  if [ "$DRY_RUN" = 1 ] || [ ! -e "$LOG" ]; then
    run touch "$LOG"
    run chown "$SVC_USER:$SVC_USER" "$LOG"
    run chmod 0640 "$LOG"
  fi
  run install -m 0644 -o root -g wheel "$TMP/plist" "$PLIST"
  if [ "$DRY_RUN" = 1 ]; then
    run launchctl bootout "system/$LABEL"
  else
    launchctl bootout "system/$LABEL" 2>/dev/null || true
  fi
  run launchctl bootstrap system "$PLIST"
  LOGS="tail -f $LOG"
fi

cat <<EOF

Installed. The proxy listens on http://127.0.0.1:$PORT/proxy/gdrive-queue/ (logs: $LOGS).

Add to hibernaut.toml (the [gdrive] section; restart Hibernaut to start polling):

[gdrive]
enabled = true
proxy_url = "http://127.0.0.1:$PORT"
proxy_token_file = "$HB_TOKEN"
root_folder_id = "$ROOT_FOLDER_ID"
default_inbox_id = "proj:hibernaut"

Then remove the copy Hibernaut's user can still read: delete $KEY once the check above passed,
and unset HIBERNAUT_GDRIVE_SA_KEY wherever it was exported.
EOF
