#!/usr/bin/env python3
# Test double for the Claude Code CLI in stream-json mode; never contacts a model.
import json
import os
import sys
from pathlib import Path

argv = sys.argv[1:]
prompt = sys.stdin.read()
schema = argv[argv.index("--json-schema") + 1] if "--json-schema" in argv else ""
role = "reviewer" if '"verdict"' in schema else "sde"
log = os.environ.get("FAKE_CLAUDE_LOG")
if log:
    with open(log, "a", encoding="utf-8") as stream:
        record = {
            "role": role,
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


def next_step(name):
    # FAKE_PLAN holds {"sde": [...], "reviewer": [...]}; each call of a role takes its next step, the last repeats.
    plan_path = os.environ.get("FAKE_PLAN")
    if not plan_path:
        return None
    steps = json.loads(Path(plan_path).read_text(encoding="utf-8")).get(name) or []
    if not steps:
        return None
    counter = Path(f"{plan_path}.{name}.n")
    n = int(counter.read_text()) if counter.exists() else 0
    counter.write_text(str(n + 1))
    return steps[min(n, len(steps) - 1)]


model = argv[argv.index("--model") + 1]
source = os.environ.get("FAKE_API_KEY_SOURCE") or ("ANTHROPIC_API_KEY" if "ANTHROPIC_API_KEY" in os.environ else "none")
session_id = argv[argv.index("--resume") + 1] if "--resume" in argv else f"fake-{role}-{os.getpid()}"
emit({"type": "system", "subtype": "hook_started"})
emit({"type": "system", "subtype": "init", "apiKeySource": source, "model": os.environ.get("FAKE_MODEL", model), "session_id": session_id})
touch = os.environ.get("FAKE_TOUCH")
if touch and role == "reviewer":
    Path(touch).write_text("written by the reviewer\n", encoding="utf-8")
step = next_step(role)
if role == "reviewer":
    output = step if step is not None else json.loads(
        os.environ.get("FAKE_REVIEW", '{"verdict": "pass", "summary": "ok", "blocking": []}')
    )
else:
    step = step or {}
    for rel, content in step.get("write", {}).items():
        target = Path.cwd() / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    output = step.get("output", {"status": "done", "summary": "implemented", "test_evidence": "red: 1 failed; green: 1 passed"})
if isinstance(step, dict) and step.get("error"):
    emit({"type": "result", "subtype": "error_during_execution", "is_error": True, "result": step["error"], "session_id": session_id})
    sys.exit(1)
emit({"type": "result", "subtype": "success", "is_error": False, "result": json.dumps(output), "structured_output": output, "session_id": session_id})
