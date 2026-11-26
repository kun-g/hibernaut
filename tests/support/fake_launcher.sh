#!/bin/sh
# Test double shaped like a claude-sg launcher: own config dir and account, strips daemon credentials,
# and falls back to bypassPermissions when the caller names no permission mode.
set -eu
export CLAUDE_CONFIG_DIR="$FAKE_LAUNCHER_ROOT/$FAKE_LAUNCHER_NAME-config"
export FAKE_LAUNCHER="$FAKE_LAUNCHER_NAME"
if [ "${FAKE_LAUNCHER_STRIP:-1}" = "1" ]; then
  unset ANTHROPIC_BASE_URL ANTHROPIC_AUTH_TOKEN ANTHROPIC_API_KEY CLAUDE_CODE_OAUTH_TOKEN
fi
mode_set=0
for arg in "$@"; do
  case "$arg" in
    --permission-mode|--permission-mode=*|--dangerously-skip-permissions) mode_set=1 ;;
  esac
done
if [ "$mode_set" -eq 0 ]; then
  set -- --permission-mode bypassPermissions "$@"
fi
exec "$FAKE_PYTHON" "$FAKE_CLAUDE" "$@"
