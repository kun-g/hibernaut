from __future__ import annotations

import json
from typing import Any

from agent_runtime.adapters.memory_fs.partitioned import PartitionedMemoryAdapter
from agent_runtime.adapters.memory_fs.tool_args import required
from agent_runtime.core.ports import MemoryPort

APPROVE_TOOL = "memory_approve_proposal"
MEMORY_TOOLS = ("memory_search", "memory_read", "memory_propose_write", APPROVE_TOOL)


# The caller has put the call through PermissionPort.check. Refusals raise ValueError or OSError.
def call_memory_tool(memory: MemoryPort, name: str, args: dict[str, Any], *, actor: str) -> str:
    if not isinstance(memory, PartitionedMemoryAdapter):
        raise ValueError(f"{name} needs project memory in Memory Hub, and this Run's is not")
    if name == "memory_search":
        result = memory.search(args)
    elif name == "memory_read":
        result = memory.read_document(args)
    elif name == "memory_propose_write":
        result = memory.propose(args, actor=actor)
    elif name == APPROVE_TOOL:
        result = memory.approve(required(args, "proposal_id"), actor=actor)
    else:
        raise ValueError(f"unknown memory tool {name}")
    return json.dumps(result, ensure_ascii=False)


# What a person approves, since the call itself only carries an id.
def describe_proposal(memory: MemoryPort, args: dict[str, Any]) -> str:
    if not isinstance(memory, PartitionedMemoryAdapter):
        return ""
    try:
        record = memory.proposal(required(args, "proposal_id"))
    except (OSError, ValueError) as exc:
        return str(exc)
    proposal = record["proposal"]
    section = f", section {proposal['section_heading']}" if proposal.get("section_heading") else ""
    return (
        f"{proposal['op']} {proposal['target_uri']}{section} ({record['status']})\n"
        f"rationale: {proposal['rationale']}\n{proposal['content']}"
    )


def write_memory(memory: MemoryPort, path: str, content: str) -> str:
    memory.write(path, content)
    if isinstance(memory, PartitionedMemoryAdapter) and memory.needs_approval(path):
        return f"{path} is queued for a person's approval and not in effect yet"
    return f"wrote {path}"
