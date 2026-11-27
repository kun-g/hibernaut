#!/usr/bin/env python3
# Test double for a plain reviewer CLI (grok, cursor-agent, ...): never contacts a model.
# FAKE_CLI_PLAN is a JSON list of steps; each call takes the next one and the last repeats. A step may print
# "stdout"/"stderr", "write" files into the working directory, "sleep" seconds and "exit" with a code.
import json
import os
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tests.support.fake_counter import next_count

plan_path = Path(os.environ["FAKE_CLI_PLAN"])
n = next_count(Path(f"{plan_path}.n"))
steps = json.loads(plan_path.read_text(encoding="utf-8"))
step = steps[min(n, len(steps) - 1)]

with open(os.environ["FAKE_CLI_LOG"], "a", encoding="utf-8") as log:
    log.write(json.dumps({"argv": sys.argv[1:], "cwd": os.getcwd(), "stdin_tty": sys.stdin.isatty(),
                          "stdin": sys.stdin.read(), "env": os.environ.get("FAKE_CLI_MARK")}) + "\n")

for rel, content in step.get("write", {}).items():
    (Path.cwd() / rel).write_text(content, encoding="utf-8")
if step.get("wait_for_stop"):
    signal.pause()
if "sleep" in step:
    time.sleep(float(step["sleep"]))
sys.stdout.write(step.get("stdout", ""))
sys.stderr.write(step.get("stderr", ""))
sys.exit(int(step.get("exit", 0)))
