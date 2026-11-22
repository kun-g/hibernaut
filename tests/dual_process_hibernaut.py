#!/usr/bin/env python3
# Child-process entry for tests/test_switchboard_dual_process_e2e.py: the real Hibernaut main() with --demo,
# whose rule-based LLM is swapped for one that echoes the Switchboard message body. Never contacts a model.
from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Sequence
from pathlib import Path

import agent_runtime.app.main as hibernaut_main
from agent_runtime.adapters.executor_whitebox.executor import LLMMessage, LLMReply

GATE_PREFIX = "gate:"


class EchoBodyLLM:
    async def complete(
        self,
        *,
        system_prefix: str,
        messages: Sequence[LLMMessage],
        available_tools: Sequence[str],
    ) -> LLMReply:
        try:
            event = json.loads(messages[-1].content)
        except json.JSONDecodeError:
            return LLMReply(silent=True)
        body = str(event.get("payload", {}).get("body", ""))
        if not body:
            return LLMReply(silent=True)
        if body.startswith(GATE_PREFIX):
            # Holds the run until the test creates the gate file, so it can stop Switchboard mid-run.
            gate = Path(os.environ["HB_E2E_GATE"])
            while not gate.exists():
                await asyncio.sleep(0.05)
        return LLMReply(emit_messages=(f"echo: {body}",))


if __name__ == "__main__":
    hibernaut_main.RuleBasedDemoLLM = EchoBodyLLM  # type: ignore[misc, assignment]
    hibernaut_main.main()
