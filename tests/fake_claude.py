#!/usr/bin/env python3
# Test double for the Claude Code CLI in stream-json mode; never contacts a model.
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.fake_counter import next_count  # noqa: E402

argv = sys.argv[1:]
interactive = "-p" not in argv
probe = not interactive and "--tools" in argv and argv[argv.index("--tools") + 1] == "" and "--mcp-config" not in argv
prompt = "" if interactive else sys.stdin.read()
schema = argv[argv.index("--json-schema") + 1] if "--json-schema" in argv else ""
allowed = argv[argv.index("--allowedTools") + 1] if "--allowedTools" in argv else ""
role = "reviewer" if '"verdict"' in schema or "Bash(git diff:*)" in allowed else "sde"
if "mcp__hibernaut_steward__forward_to_inbox" in allowed:
    role = "steward"
if probe:
    role = "probe"
log = os.environ.get("FAKE_CLAUDE_LOG")


def write_log(**extra):
    if not log:
        return
    with open(log, "a", encoding="utf-8") as stream:
        record = {
            "role": role,
            "mode": "interactive" if interactive else "print",
            "argv": argv,
            "prompt": prompt,
            "cwd": os.getcwd(),
            "paid_env": sorted(k for k in ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "CLAUDE_CODE_USE_BEDROCK", "CLAUDE_CODE_OAUTH_TOKEN") if k in os.environ),
            "orca_env": sorted(k for k in os.environ if k.startswith("ORCA_")),
            "api_key": os.environ.get("ANTHROPIC_API_KEY"),
            "auth_token": os.environ.get("ANTHROPIC_AUTH_TOKEN"),
            "base_url": os.environ.get("ANTHROPIC_BASE_URL"),
            "launcher": os.environ.get("FAKE_LAUNCHER"),
            "config_dir": os.environ.get("CLAUDE_CONFIG_DIR"),
            "mark": os.environ.get("FAKE_MARK"),
            **extra,
        }
        stream.write(json.dumps(record) + "\n")


if not interactive:
    write_log()


# --output-format json prints the result message alone; with --verbose it is an array of every message.
single = "--output-format" in argv and argv[argv.index("--output-format") + 1] == "json"
collected = []


def emit(message):
    if not single:
        print(json.dumps(message), flush=True)
        return
    collected.append(message)
    if message["type"] == "result":
        print(json.dumps(collected if os.environ.get("FAKE_VERBOSE") else message), flush=True)


def next_step(name):
    # FAKE_PLAN holds {"sde": [...], "reviewer": [...]}; each call of a role takes its next step, the last repeats.
    plan_path = os.environ.get("FAKE_PLAN")
    if not plan_path:
        return None
    steps = json.loads(Path(plan_path).read_text(encoding="utf-8")).get(name) or []
    if not steps:
        return None
    n = next_count(Path(f"{plan_path}.{name}.n"))
    return steps[min(n, len(steps) - 1)]


def apply_work(step):
    # A developer step runs its commands (e.g. a git merge that may conflict), writes its files and commits them
    # like a developer finishing a task; "commit": null leaves the work uncommitted and "write_uncommitted" adds
    # files after the commit.
    for cmd in step.get("run", []):
        subprocess.run(cmd, capture_output=True)
    for rel, content in step.get("write", {}).items():
        target = Path.cwd() / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    message = step.get("commit", "fake sde work")
    if message and step.get("output", {}).get("status", "done") == "done":
        subprocess.run(["git", "add", "-A"], check=True, capture_output=True)
        subprocess.run(["git", "commit", "-q", "--allow-empty", "-m", message], check=True, capture_output=True)
    for rel, content in step.get("write_uncommitted", {}).items():
        (Path.cwd() / rel).write_text(content, encoding="utf-8")


HANDOFF_TOOLS = ("mcp__hibernaut_handoff__get_task", "mcp__hibernaut_handoff__submit_result")


class Handoff:
    # Talks to the stdio MCP server named in --mcp-config the way Claude Code does: one JSON-RPC message per line.
    def __init__(self, name="hibernaut_handoff"):
        config = json.loads(argv[argv.index("--mcp-config") + 1])
        server = config["mcpServers"][name]
        self.proc = subprocess.Popen([server["command"], *server["args"]], stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE, text=True)
        self.next_id = 0
        self.rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                "clientInfo": {"name": "fake-claude", "version": "0"}})
        self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n")
        self.tools = {t["name"]: t for t in self.rpc("tools/list", {})["tools"]}

    def rpc(self, method, params):
        self.next_id += 1
        self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "id": self.next_id, "method": method, "params": params}) + "\n")
        self.proc.stdin.flush()
        reply = json.loads(self.proc.stdout.readline())
        assert reply.get("id") == self.next_id, reply
        if "error" in reply:
            raise RuntimeError(reply["error"])
        return reply["result"]

    def call(self, tool, arguments):
        result = self.rpc("tools/call", {"name": tool, "arguments": arguments})
        return result.get("isError", False), "".join(c.get("text", "") for c in result["content"])


def run_interactive():
    # Interactive conversation: the first instruction is the positional argument after `--`, later ones arrive on
    # stdin the way `orca terminal send` types them. Each conversation keeps its turn count, like a resumable
    # transcript. Work is fetched and the result returned through the launcher's handoff MCP tools.
    if os.environ.get("FAKE_INTERACTIVE_PIDFILE"):
        park(os.environ["FAKE_INTERACTIVE_PIDFILE"])
    if "--add-dir" in argv:
        # A directory this session has not worked in opens a blocking trust dialog in a real terminal.
        write_log(blocked="add-dir trust dialog")
        print("Adding a new directory: This session hasn't worked here before. Is this a directory you trust?",
              flush=True)
        sys.exit(4)
    if "--session-id" in argv:
        sid = argv[argv.index("--session-id") + 1]
    else:
        sid = argv[argv.index("--resume") + 1]
    store = Path(os.environ["FAKE_SESSIONS"])
    store.mkdir(parents=True, exist_ok=True)
    transcript = store / f"{sid}.turns"
    if "--resume" in argv and not transcript.exists():
        print(f"No conversation found with session ID: {sid}", file=sys.stderr, flush=True)
        sys.exit(1)
    assert argv[-2] == "--", "the instruction must follow `--` so no variadic option swallows it"
    registry = os.environ.get("FAKE_PEER_REGISTRY")
    if registry:
        # Like Claude Code: a live interactive session registers its name, native ID and cwd for SendMessage.
        Path(registry).mkdir(parents=True, exist_ok=True)
        entry = Path(registry) / f"{os.getpid()}.json"
        entry.write_text(json.dumps({
            "pid": os.getpid(), "sessionId": sid, "cwd": os.getcwd(), "kind": "interactive",
            "name": argv[argv.index("--name") + 1] if "--name" in argv else "",
            "messagingSocketPath": f"/tmp/fake-socks/{os.getpid()}.sock",
        }))
    handoff = Handoff()
    permitted = [t for t in HANDOFF_TOOLS if t in allowed.split(",")]
    instruction = argv[-1]
    while instruction:
        turns = next_count(transcript) + 1
        turn = re.search(r'turn "([^"]+)"', instruction).group(1)
        global prompt
        if HANDOFF_TOOLS[0] not in permitted:
            write_log(session_id=sid, denied=HANDOFF_TOOLS[0])
            sys.exit(5)
        failed, prompt = handoff.call("get_task", {"turn": turn})
        assert not failed, prompt
        step = next_step(role)
        write_log(session_id=sid, turn=turns, instruction=instruction, handoff_tools=sorted(handoff.tools))
        if isinstance(step, dict) and (step.get("exit") or step.get("error")):
            sys.exit(3)
        if isinstance(step, dict) and step.get("hang"):
            time.sleep(60)
            sys.exit(0)
        touch = os.environ.get("FAKE_TOUCH")
        if touch and role == "reviewer":
            Path(touch).write_text("written by the reviewer\n", encoding="utf-8")
        if role == "reviewer":
            output = step if step is not None else {"verdict": "pass", "summary": "ok", "blocking": []}
        else:
            step = step or {}
            apply_work(step)
            output = step.get("output", {"status": "done", "summary": "implemented", "test_evidence": "red: 1 failed; green: 1 passed", "change_kind": "fix", "change_rationale": "corrects the behaviour under test"})
        if HANDOFF_TOOLS[1] in permitted:
            failed, text = handoff.call("submit_result", {"turn": turn, **output})
            if failed:
                write_log(session_id=sid, rejected=text)
        else:
            write_log(session_id=sid, denied=HANDOFF_TOOLS[1])
        instruction = sys.stdin.readline().strip()


model = argv[argv.index("--model") + 1]
source = os.environ.get("FAKE_API_KEY_SOURCE") or ("ANTHROPIC_API_KEY" if "ANTHROPIC_API_KEY" in os.environ else "none")
session_id = argv[argv.index("--resume") + 1] if "--resume" in argv else f"fake-{role}-{os.getpid()}"


def park(pidfile):
    # Records this process and a child of its own, then waits: for closing a terminal while they run.
    child = subprocess.Popen(["sleep", "60"])
    Path(pidfile).write_text(f"{os.getpid()} {child.pid}")
    time.sleep(60)
    sys.exit(0)


if probe and os.environ.get("FAKE_PROBE_PIDFILE"):
    park(os.environ["FAKE_PROBE_PIDFILE"])  # a probe that never reports
if probe:
    emit({"type": "system", "subtype": "init", "apiKeySource": source, "model": os.environ.get("FAKE_MODEL", model)})
    time.sleep(60)  # a probe is stopped at init; outliving the check would stall the test
    sys.exit(0)
if interactive:
    run_interactive()
    sys.exit(0)
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
elif role == "steward":
    # A steward step calls its MCP tools; the answers go to the log.
    step = step or {}
    server = Handoff("hibernaut_steward")
    if step.get("pidfile"):
        park(step["pidfile"])
    answers = [server.call(call["tool"], call.get("arguments", {})) for call in step.get("calls", [])]
    write_log(answers=answers, tools=sorted(server.tools))
    server.proc.stdin.close()
    if step.get("exit"):
        print("crashed", file=sys.stderr, flush=True)
        sys.exit(step["exit"])
    if step.get("calls") and step.get("pidfile_after"):
        park(step["pidfile_after"])
    while step.get("wait_for") and not Path(step["wait_for"]).exists():
        time.sleep(0.02)
    output = step.get("output", {"action": "silent", "message": ""})
else:
    step = step or {}
    apply_work(step)
    output = step.get("output", {"status": "done", "summary": "implemented", "test_evidence": "red: 1 failed; green: 1 passed", "change_kind": "fix", "change_rationale": "corrects the behaviour under test"})
if isinstance(step, dict) and step.get("error"):
    emit({"type": "result", "subtype": "error_during_execution", "is_error": True, "result": step["error"], "session_id": session_id})
    sys.exit(1)
emit({"type": "result", "subtype": "success", "is_error": False, "result": json.dumps(output), "structured_output": output, "session_id": session_id})
