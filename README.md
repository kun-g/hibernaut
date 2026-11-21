# Hibernaut

Clean Architecture (L0–L3) Python runtime for **Always-on Agents** built around the **Sleep → Wake → Run → Write-back** lifecycle and **multi-`inbox_id` Actor partition concurrency**.

> Asleep by default (no tokens spent), wakes only when events or timers arrive, writes state back to durable storage, and exits cleanly with explicit `Silent | Emit | Suspend` outcomes.

- Full up-to-date architecture & glossary: [`docs/architecture.md`](docs/architecture.md)
- Contributor & coding-agent rules: [`AGENTS.md`](AGENTS.md)

## Architecture Layers (L0–L3)

```text
src/agent_runtime/
├── core/                  # L0 + L1: Zero external dependencies (Python stdlib only)
│   ├── models.py          # Event, SessionRef, TimerSpec, OutboxMessage, PermissionDecision, RunOutcome, NewEventPolicy
│   └── ports.py           # 9 L1 Ports + RunCapabilities (with fold / interrupt control hooks)
├── engine/                # L2: Depends ONLY on core/ (never imports adapters/ or app/)
│   ├── handler.py         # EventHandler: lightweight ingest, idempotent dedup (dropped), fast ACK, async Worker wake
│   ├── worker.py          # AgentWorker: per-inbox_id lease lock, session batching, defer/fold/interrupt, exhaustive RunOutcome
│   ├── scheduler.py       # TimerScheduler: scans due Follow-up & Schedule timers -> appends Inbox events
│   └── dispatcher.py      # OutboxDispatcher: paced Outbox delivery with anti-spam throttling and retries
├── adapters/              # L3: Volatile infrastructure implementations
│   ├── storage_sqlite/    # SQLite WAL persistence (Inbox, Timers, Outbox, Lease Locks, Sessions)
│   ├── memory_fs/         # FileSystemMemoryAdapter + PartitionedMemoryAdapter & WorkspacePartitionRegistry
│   ├── permissions/       # Auto Mode classifier (allow / block / defer) + external CredentialProxyClient
│   ├── executor_whitebox/ # Whitebox Executor (prompt-cacheable baseline, permission gate, fold/interrupt, Suspend/Resume)
│   └── channels/          # Console, outbound Webhook, inbound HTTP Webhook server (fast 202 ACK)
└── app/
    ├── bootstrap.py       # Single-inbox Composition Root
    ├── main.py            # Multi-inbox_id AlwaysOnDaemon & AsyncWorkerPool (--policy defer|fold|interrupt)
    └── demo.py            # Zero-API-key end-to-end demo (Silent -> Suspend -> Human Approval -> Resume -> Emit)
```

## Quick Start

Zero third-party dependencies required to run the demo, multi-inbox daemon, and full test suite (Python 3.11+ / 3.12+):

```bash
git clone https://github.com/kun-g/hibernaut.git
cd hibernaut

# Run single-inbox end-to-end demo
PYTHONPATH=src python3 -m agent_runtime.app.demo

# Run multi-inbox concurrent daemon (supports --policy defer|fold|interrupt)
PYTHONPATH=src python3 -m agent_runtime.app.main --workers 4 --policy defer --port 8080

# Run all tests (AST boundary gate + Port Contract Tests + Multi-inbox + NewEventPolicy E2E)
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## Anti-Corruption Gates (CI)

```bash
pip install -e ".[dev]"

lint-imports   # Gate 1: Static import boundary checks
pytest -v      # Gate 2: Contract, concurrency, NewEventPolicy & E2E tests
pyright        # Gate 3: Strict static type checking on core/ and engine/
```

## Plugging In Real Models & Channels

- **Whitebox LLM**: Implement `agent_runtime.adapters.executor_whitebox.LLMBackend` (Anthropic, Gemini, or OpenAI SDK) and pass it to `build_daemon(llm=...)` or `build_runtime(llm=...)`.
- **Blackbox Agent Runtime**: Add `adapters/executor_blackbox/` implementing `ExecutorPort` and bridge runtime hooks/MCP tools to `RunCapabilities`—zero changes required in `core/` or `engine/`.
