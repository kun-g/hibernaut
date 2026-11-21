# Hibernaut

Clean Architecture (L0–L3) Python runtime for **Always-on Agents** built around the **Sleep → Wake → Run → Write-back** lifecycle.

> Asleep by default (no tokens spent), wakes only when events or timers arrive, writes state back to durable storage, and exits cleanly with explicit `Silent | Emit | Suspend` outcomes.

## Architecture Layers (L0–L3)

```text
src/agent_runtime/
├── core/                  # L0 + L1: Zero external dependencies (Python stdlib only)
│   ├── models.py          # Event, SessionRef, TimerSpec, OutboxMessage, PermissionDecision, RunOutcome (Silent | Emit | Suspend)
│   └── ports.py           # InboxPort, TimerPort, MemoryPort, PermissionPort, OutboxPort, LockPort, SessionStorePort, ChannelPort, ExecutorPort
├── engine/                # L2: Depends ONLY on core/ (never imports adapters/ or app/)
│   ├── handler.py         # EventHandler: lightweight ingest, idempotent dedup (dropped), fast ACK, async Worker wake
│   ├── worker.py          # AgentWorker: Single-Active-Worker lease lock, session batching, retries, exhaustive RunOutcome handling
│   ├── scheduler.py       # TimerScheduler: scans due Follow-up & Schedule timers -> appends Inbox events
│   └── dispatcher.py      # OutboxDispatcher: paced Outbox delivery with anti-spam throttling and retries
├── adapters/              # L3: Volatile infrastructure implementations
│   ├── storage_sqlite/    # SQLite WAL persistence (Inbox, Timers, Outbox, Lease Locks, Sessions)
│   ├── memory_fs/         # Directory-backed Memory (instructions.md, index.md, reported.md, notes/, progress/)
│   ├── permissions/       # Auto Mode classifier (allow / block / defer) + external CredentialProxyClient
│   ├── executor_whitebox/ # Whitebox Executor (prompt-cacheable baseline prefix, tool interception, Suspend/Resume, compaction)
│   └── channels/          # Console, outbound Webhook, inbound HTTP Webhook server (fast 202 ACK)
└── app/
    ├── bootstrap.py       # Composition Root: single dependency-injection entry point
    └── demo.py            # Zero-API-key end-to-end demo (Silent -> Suspend -> Human Approval -> Resume -> Emit)
```

## Quick Start

Zero third-party dependencies required to run the full test suite and end-to-end demo (Python 3.11+ / 3.12+):

```bash
git clone https://github.com/kun-g/hibernaut.git
cd hibernaut

# Run end-to-end demo (Sleep -> Wake -> Suspend -> Human Approval -> Resume -> Emit)
PYTHONPATH=src python3 -m agent_runtime.app.demo

# Run all tests (AST architecture boundary gate + In-Memory/SQLite Contract Tests + E2E)
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## Anti-Corruption Gates (CI)

```bash
pip install -e ".[dev]"

# Gate 1: Static import boundary checks
lint-imports

# Gate 2: Implementation-agnostic Port Contract Tests & E2E
pytest -v

# Gate 3: Strict static type checking on core/ and engine/
pyright
```

## Plugging In Real Models & Channels

- **Whitebox LLM**: Implement `agent_runtime.adapters.executor_whitebox.LLMBackend` (Anthropic, Gemini, or OpenAI SDK) and pass it to `build_runtime(llm=...)` in `app/bootstrap.py`.
- **Blackbox Agent Runtime**: Add `adapters/executor_blackbox/` implementing `ExecutorPort` and bridge runtime hooks/MCP tools to `RunCapabilities`—zero changes required in `core/` or `engine/`.
