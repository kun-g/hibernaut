#!/usr/bin/env python3
# Test double for the Claude Code CLI in stream-json mode; never contacts a model.
import json
import os
import sys
from pathlib import Path

argv = sys.argv[1:]
prompt = sys.stdin.read()
log = os.environ.get("FAKE_CLAUDE_LOG")
if log:
    with open(log, "a", encoding="utf-8") as stream:
        record = {
            "argv": argv,
            "prompt": prompt,
            "cwd": os.getcwd(),
            "paid_env": sorted(k for k in ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "CLAUDE_CODE_USE_BEDROCK", "CLAUDE_CODE_OAUTH_TOKEN") if k in os.environ),
            "api_key": os.environ.get("ANTHROPIC_API_KEY"),
            "base_url": os.environ.get("ANTHROPIC_BASE_URL"),
            "launcher": os.environ.get("FAKE_LAUNCHER"),
            "config_dir": os.environ.get("CLAUDE_CONFIG_DIR"),
        }
        stream.write(json.dumps(record) + "\n")


def emit(message):
    print(json.dumps(message), flush=True)


model = argv[argv.index("--model") + 1]
source = os.environ.get("FAKE_API_KEY_SOURCE") or ("ANTHROPIC_API_KEY" if "ANTHROPIC_API_KEY" in os.environ else "none")
emit({"type": "system", "subtype": "hook_started"})
emit({"type": "system", "subtype": "init", "apiKeySource": source, "model": os.environ.get("FAKE_MODEL", model)})
touch = os.environ.get("FAKE_TOUCH")
if touch:
    Path(touch).write_text("written by the reviewer\n", encoding="utf-8")
review = json.loads(os.environ.get("FAKE_REVIEW", '{"verdict": "pass", "summary": "ok", "blocking": []}'))
emit({"type": "result", "subtype": "success", "is_error": False, "result": json.dumps(review), "structured_output": review})
