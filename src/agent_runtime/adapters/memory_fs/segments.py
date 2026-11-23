from __future__ import annotations

from dataclasses import dataclass

from agent_runtime.adapters.memory_fs.partitioned import PartitionedMemoryAdapter
from agent_runtime.core.ports import MemoryPort
from memory_hub import BaselineBlock

_STABLE_FILES = ("instructions.md", "index.md")
_VOLATILE_FILES = ("reported.md",)


@dataclass(frozen=True, slots=True)
class BaselineSegments:
    # (file name, text) in prompt order. stable goes before the prompt cache breakpoint; volatile changes from Run to
    # Run and goes after it.
    stable: tuple[tuple[str, str], ...]
    volatile: tuple[tuple[str, str], ...]


def baseline_segments(memory: MemoryPort) -> BaselineSegments:
    if isinstance(memory, PartitionedMemoryAdapter):
        bundle = memory.load_baseline_bundle()
        return BaselineSegments(_by_file(bundle.stable_prefix), _by_file(bundle.volatile_tail))
    baseline = memory.load_baseline()
    return BaselineSegments(
        tuple((name, baseline.get(name, "").strip()) for name in _STABLE_FILES),
        tuple((name, baseline.get(name, "").strip()) for name in _VOLATILE_FILES),
    )


def _by_file(blocks: tuple[BaselineBlock, ...]) -> tuple[tuple[str, str], ...]:
    # global://instructions.md and proj://<key>/instructions.md read as one file, the global part first.
    files: dict[str, list[str]] = {}
    for block in blocks:
        files.setdefault(block.uri.rsplit("/", 1)[-1], []).append(block.content.strip())
    return tuple((name, "\n\n".join(text for text in texts if text)) for name, texts in files.items())
