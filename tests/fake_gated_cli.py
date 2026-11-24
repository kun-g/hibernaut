from __future__ import annotations

import sys
from pathlib import Path

# Stands in for Claude Code: loads the hooks of --settings, runs SessionStart, then tries each tool call of the prompt's
# "PLAN: [...]" line through PreToolUse, running it only on an allow. It behaves like the real harness in what matters
# here: a hook that exits 2, or denies, stops the call; any other failure lets the call run.
_SOURCE = r'''
import json, os, subprocess, sys, time
argv = sys.argv[1:]
settings = json.loads(argv[argv.index("--settings") + 1]) if "--settings" in argv else {}
hooks = {} if os.environ.get("FAKE_IGNORES_HOOKS") else settings.get("hooks", {})
after_p = argv[argv.index("-p") + 1:][:1] if "-p" in argv else []
prompt = after_p[0] if after_p and not after_p[0].startswith("--") else sys.stdin.read()
log = open(os.environ["FAKE_LOG"], "a")

def note(kind, **kw):
    log.write(json.dumps({"kind": kind, **kw}) + "\n")
    log.flush()

def out(obj):
    print(json.dumps(obj), flush=True)

def run_hooks(event, payload):
    replies = []
    for group in hooks.get(event, []):
        for hook in group["hooks"]:
            replies.append(subprocess.run(hook["command"], shell=True, input=json.dumps(payload), capture_output=True, text=True))
    return replies

session = argv[argv.index("--resume") + 1] if "--resume" in argv else "native-1"
note("start", cwd=os.getcwd(), prompt=prompt, argv=argv, session=session)
run_hooks("SessionStart", {"hook_event_name": "SessionStart", "session_id": session})
if not os.environ.get("FAKE_PRINTS_ONLY_A_RESULT"):
    out({"type": "system", "subtype": "init", "session_id": session})
plan = []
for line in prompt.splitlines():
    if line.startswith("PLAN: "):
        plan = json.loads(line[len("PLAN: "):])
for call in plan:
    if call["tool"] == "sleep":
        time.sleep(call["args"]["seconds"])
        continue
    if call["tool"] == "wait_for_file":
        note("waiting")
        while not os.path.exists(call["args"]["path"]):
            time.sleep(0.05)
        continue
    denied, stop = None, False
    for reply in run_hooks("PreToolUse", {"hook_event_name": "PreToolUse", "tool_name": call["tool"], "tool_input": call["args"]}):
        if reply.returncode == 2:
            denied = reply.stderr.strip() or "exit 2"
            continue
        try:
            body = json.loads(reply.stdout)
        except ValueError:
            continue
        if body.get("hookSpecificOutput", {}).get("permissionDecision") == "deny":
            denied = body["hookSpecificOutput"].get("permissionDecisionReason", "")
        stop = stop or body.get("continue") is False
    if denied is None:
        note("ran", tool=call["tool"], args=call["args"])
        if call["tool"] == "Write":
            open(call["args"]["path"], "w").write("written by a tool")
    else:
        note("denied", tool=call["tool"], reason=denied)
    if stop:
        time.sleep(30)
out({"type": "result", "subtype": "success", "session_id": session, "result": "all done"})
'''


def install_gated_cli(directory: Path, name: str = "claude") -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_text(f"#!{sys.executable}\n{_SOURCE}", encoding="utf-8")
    path.chmod(0o755)
    return path
