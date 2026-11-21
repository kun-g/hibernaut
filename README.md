# Hibernaut

Clean Architecture (L0–L3) Python runtime for **Always-on Agents** built around the **Sleep → Wake → Run → Write-back** lifecycle, **multi-`inbox_id` Actor partition concurrency**, and a **zero-dependency local self-bootstrap loop**.

> Asleep by default (no tokens spent), wakes only when events, polled GitHub Issues, or timers arrive, runs in isolated dynamic worktrees, and exits cleanly with explicit `Silent | Emit | Suspend` outcomes.

- Full up-to-date architecture & glossary: [`docs/architecture.md`](docs/architecture.md)
- Contributor & coding-agent rules: [`AGENTS.md`](AGENTS.md)
- Declarative config template: [`hibernaut.example.toml`](hibernaut.example.toml)

## Architecture Layers (L0–L3)

```text
src/agent_runtime/
├── core/                  # L0 + L1: Zero external dependencies (Python stdlib only)
│   ├── models.py          # Event, SessionRef (with correlation_key), TimerSpec, OutboxMessage, PermissionDecision, RunOutcome, NewEventPolicy
│   └── ports.py           # 9 L1 Ports + RunCapabilities (with fold / interrupt control hooks)
├── engine/                # L2: Depends ONLY on core/ (never imports adapters/ or app/)
│   ├── handler.py         # EventHandler: lightweight ingest, idempotent dedup (dropped), fast ACK, async Worker wake
│   ├── worker.py          # AgentWorker: per-inbox_id lease lock, 4-level session resolution, defer/fold/interrupt, exhaustive RunOutcome
│   ├── scheduler.py       # TimerScheduler: scans due Follow-up & Schedule timers -> appends Inbox events
│   └── dispatcher.py      # OutboxDispatcher: paced Outbox delivery with anti-spam throttling and retries
├── adapters/              # L3: Volatile infrastructure implementations
│   ├── storage_sqlite/    # SQLite WAL (Inbox, Timers, Outbox, Leases, Sessions + correlation_key rotation, Workspaces)
│   ├── memory_fs/         # FileSystemMemoryAdapter + PartitionedMemoryAdapter & WorkspacePartitionRegistry
│   ├── permissions/       # Auto Mode classifier (allow / block / defer) + external CredentialProxyClient
│   ├── executor_whitebox/ # Whitebox Executor (prompt-cacheable baseline, permission gate, fold/interrupt, Suspend/Resume)
│   ├── executor_cli/      # LocalCLIExecutor (claude/cursor-agent/grok/dsh + magpie env) & OrcaWorktreeManager (3 safety gates)
│   └── channels/          # Multi-route HTTP server (/events, /webhooks/github, /webhooks/feishu), GitHubIssuePoller, FeishuChannelAdapter, Console
└── app/
    ├── config.py          # Stdlib tomllib loader for hibernaut.toml
    ├── bootstrap.py       # Single-inbox Composition Root
    ├── main.py            # Multi-inbox_id AlwaysOnDaemon & AsyncWorkerPool (--config hibernaut.toml)
    └── demo.py            # Zero-API-key end-to-end demo (Silent -> Suspend -> Human Approval -> Resume -> Emit)
```

## Local Self-Bootstrap Quick Start

Zero third-party dependencies required to run the local self-bootstrap daemon and full test suite (Python 3.11+ / 3.12+):

```bash
git clone https://github.com/kun-g/hibernaut.git
cd hibernaut

# 1. Copy and edit declarative config (primary checkout, CLI template, env var names)
cp hibernaut.example.toml hibernaut.toml

# 2. Launch full local Always-on daemon (GitHub Issue Poller + LocalCLIExecutor + Dynamic Worktree + Feishu/Console)
PYTHONPATH=src python3 -m agent_runtime.app.main --config hibernaut.toml

# Or run the zero-config single-inbox demo
PYTHONPATH=src python3 -m agent_runtime.app.demo
```

### Run with automatic updates (macOS / Linux)

From a dedicated Git checkout on `main`, launch the supervisor with Python 3.11+ and Git installed:

```bash
python3 scripts/run_updating.py --port 8080
```

The supervisor checks the configured `origin/main` every 300 seconds. Use `--interval 60` for one-minute checks. Daemon options such as `--db`, `--memory-root`, `--workers`, and `--policy` are forwarded across restarts. Stop with `Ctrl+C`.

Use an origin you trust to supply executable code. A dirty checkout (including untracked files), another branch, a divergent history, or a failed fetch leaves the running daemon in place. On a fast-forward, the old daemon closes ingress and drains accepted work before the supervisor applies the commit and relaunches. Clients should retry during this short interruption using the same external event ID. Keep runtime state in ignored paths or outside the checkout. The supervisor reuses those paths. If a new daemon fails to start, the supervisor exits with its status for operator recovery.

## Anti-Corruption Gates (CI)

```bash
pip install -e ".[dev]"

lint-imports   # Gate 1: Static import boundary checks
pytest -v      # Gate 2: Contract, concurrency, NewEventPolicy, correlation_key, Webhook & Bootstrap tests
pyright        # Gate 3: Strict static type checking on core/ and engine/
```
