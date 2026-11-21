# Hibernaut

Clean Architecture (L0–L3) Python runtime for **Always-on Agents** built around the **Sleep → Wake → Run → Write-back** lifecycle, **multi-`inbox_id` Actor partition concurrency**, and a **zero-dependency local self-bootstrap loop**.

> Asleep by default (no tokens spent), wakes only when events, polled GitHub Issues, MCP tool calls, or timers arrive, runs in isolated dynamic worktrees, and exits cleanly with explicit `Silent | Emit | Suspend` outcomes.

- Full up-to-date architecture & glossary: [`docs/architecture.md`](docs/architecture.md)
- Contributor & coding-agent rules: [`AGENTS.md`](AGENTS.md)
- Declarative config template: [`hibernaut.example.toml`](hibernaut.example.toml)

## Architecture Layers (L0–L3)

```text
src/agent_runtime/
├── core/                  # L0 + L1: Zero external dependencies (Python stdlib only)
│   ├── models.py          # Event, SessionRef (correlation_key, executor_id, executor_session_id), resolve_executor_id, TimerSpec, OutboxMessage, PermissionDecision, RunOutcome, NewEventPolicy
│   └── ports.py           # 9 L1 Ports (with ExecutorPort.executor_id) + RunCapabilities (fold / interrupt control hooks)
├── engine/                # L2: Depends ONLY on core/ (never imports adapters/ or app/)
│   ├── handler.py         # EventHandler: lightweight ingest, idempotent dedup (dropped), fast ACK, async Worker wake
│   ├── worker.py          # AgentWorker: per-inbox_id lease lock, 4-level session resolution, multi-executor sticky routing, defer/fold/interrupt, exhaustive RunOutcome
│   ├── scheduler.py       # TimerScheduler: scans due Follow-up & Schedule timers -> appends Inbox events
│   └── dispatcher.py      # OutboxDispatcher: paced Outbox delivery with anti-spam throttling and retries
├── adapters/              # L3: Volatile infrastructure implementations
│   ├── storage_sqlite/    # SQLite WAL, one lock-guarded connection + BEGIN IMMEDIATE writes (Inbox, Timers, Outbox, Leases, Sessions + correlation_key/executor binding, Workspaces)
│   ├── memory_fs/         # FileSystemMemoryAdapter + PartitionedMemoryAdapter & WorkspacePartitionRegistry
│   ├── permissions/       # Auto Mode classifier (allow / block / defer) + external CredentialProxyClient
│   ├── executor_whitebox/ # Whitebox Executor (prompt-cacheable baseline, permission gate, fold/interrupt, Suspend/Resume)
│   ├── executor_cli/      # LocalCLIExecutor (claude/cursor-agent/grok/dsh + resume_args + magpie env) & OrcaWorktreeManager (3 safety gates)
│   └── channels/          # Multi-route HTTP (/events, /webhooks/github, /webhooks/feishu, /mcp), MCPServerAdapter, GitHubIssuePoller, FeishuChannelAdapter, Console
└── app/
    ├── config.py          # Stdlib tomllib loader for hibernaut.toml
    ├── bootstrap.py       # Single-inbox Composition Root
    ├── main.py            # Multi-inbox_id AlwaysOnDaemon (--config hibernaut.toml | --demo | --mcp-stdio)
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
```

When started with `--config`, Hibernaut registers all `[executors.<id>]` templates into `AgentWorker` for 4-level sticky executor routing (`event.payload["executor_id"]` → `session.executor_id` → `projects.<name>.executor` → `runtime.default_executor`) and never silently falls back to `RuleBasedDemoLLM`. If `[executors]` or `runtime.default_executor` is not configured, it prints a warning to `stderr` and starts in **`ingest-only`** mode (keeping inbound webhooks, polling, and MCP active while events remain safely `pending` in SQLite). Pass `--demo` explicitly if you want `RuleBasedDemoLLM` with `--config`.

The HTTP daemon watches the file passed to `--config` once per second, including under `scripts/run_updating.py`. Save project mappings, CLI templates, worker limits or channel settings and look for `[config] Reloaded` in the console. The daemon finishes accepted work before activating the new settings, so incoming requests may briefly wait. Invalid TOML or a busy replacement port leaves the current configuration active. Changing `db_path` or `memory_root`, updating the process environment, or reconfiguring a stdio MCP session requires a restart.

The HTTP endpoints handle requests on parallel threads (backlog 128); stopping or reloading the daemon waits for requests already accepted. SQLite access from those threads and the worker loop goes through one locked connection, and writes use `BEGIN IMMEDIATE`, so deduplication and leases stay atomic. A worker runs all pending events that share a session (by `session_id`, then `correlation_key`) as one batch, in order of each group's first event. Events with neither key are unrelated and run one at a time under `defer` and `fold`. A session's batch closes at an approval reply (earlier events for that session run with it) or at an event whose `explicit_workdir` or `executor` differs; later events for that session wait for the next pass. When an approval for a session is already queued, it always joins that session's first batch and runs on the executor that suspended the session; incompatible events ahead of it, including ones routed to another executor, run afterwards. `fold` never pulls events across that boundary, and `interrupt` never restarts a Run that carries an approval.

Hibernaut checks local branch names when allocating a worktree, including branches without a checkout. If Orca adds a numeric suffix during creation, Hibernaut records the branch and path from the creation receipt after verifying both against the primary repository's Git worktree list. On the next wake, it repairs stale SQLite paths for registered worktrees. If an executor exhausts its retries, the daemon logs the affected inbox and keeps the batch pending. A new event in that inbox wakes it for another attempt.

For a visible agent session, set `orca_agent = "claude"` under `[executors.claude]` and use `worktree_backend = "orca"` for the project. Restart the daemon after installing this version. Each managed run opens an Orca terminal in the background where you can watch progress and respond to permission prompts. The agent returns its `Silent`, `Emit`, or `Suspend` outcome in a per-run temporary file. Successful terminal tabs stay open. `timeout_seconds` includes time spent waiting for input. Configure agent accounts and environment in Orca. The template's `command`, `args`, and `env` apply to direct CLI runs, including Git worktrees and explicit directory overrides.

### Versioning

The daemon and demo print `Hibernaut <version>` at startup. To check the version directly, run `PYTHONPATH=src python3 -m agent_runtime.app.main --version`.

Stable releases use SemVer `MAJOR.MINOR.PATCH`. Bump the major number for incompatible API changes, minor for compatible features, and patch for compatible fixes. Set `__version__` in `src/agent_runtime/__init__.py` when preparing a release. The package metadata and startup banner both read that value.

### Run with automatic updates (macOS / Linux)

From a dedicated Git checkout on `main`, launch the supervisor with Python 3.11+ and Git installed:

```bash
python3 scripts/run_updating.py --port 8080
```

The supervisor checks the configured `origin/main` every 300 seconds. Use `--interval 60` for one-minute checks. Daemon options such as `--db`, `--memory-root`, `--workers`, and `--policy` are forwarded across restarts. Stop with `Ctrl+C`.

Use an origin you trust to supply executable code. A dirty checkout (including untracked files), another branch, a divergent history, or a failed fetch leaves the running daemon in place. On a fast-forward, the old daemon closes ingress and drains accepted work before the supervisor applies the commit and relaunches. Clients should retry during this short interruption using the same external event ID. Keep runtime state in ignored paths or outside the checkout. The supervisor reuses those paths. If a new daemon fails to start, the supervisor exits with its status for operator recovery.

## MCP Server Integration (`stdio` & Remote `/mcp`)

Hibernaut exposes 6 MCP tools (`hibernaut_ingest_event`, `hibernaut_list_sessions`, `hibernaut_list_events`, `hibernaut_list_workspaces`, `hibernaut_resolve_approval`, `hibernaut_reclaim_workspace`).

### 1. Local IDE / CLI (`stdio`)

Add to your Claude Code / Cursor / Claude Desktop MCP configuration:

```json
{
  "mcpServers": {
    "hibernaut": {
      "command": "python3",
      "args": ["-m", "agent_runtime.app.main", "--mcp-stdio", "--config", "/abs/path/to/hibernaut.toml"],
      "env": {
        "PYTHONPATH": "/abs/path/to/hibernaut/src"
      }
    }
  }
}
```

### 2. Remote Access via Cloudflare Tunnel (`POST /mcp`, Streamable HTTP)

Set `mcp_enabled = true` in `hibernaut.toml`, export a strong token, and expose the local port through Cloudflare Tunnel:

```bash
export HIBERNAUT_MCP_TOKEN="replace-with-strong-random-token"
PYTHONPATH=src python3 -m agent_runtime.app.main --config hibernaut.toml

# In another terminal, publish https://mcp.yourdomain.com -> http://127.0.0.1:8080
cloudflared tunnel --url http://127.0.0.1:8080
```

Remote clients call `POST https://<your-tunnel-host>/mcp` with header `Authorization: Bearer <HIBERNAUT_MCP_TOKEN>`. If `HIBERNAUT_MCP_TOKEN` is unset or `mcp_enabled = false`, `/mcp` stays disabled (404).

## Anti-Corruption Gates (CI)

```bash
pip install -e ".[dev]"

lint-imports   # Gate 1: Static import boundary checks
pytest -v      # Gate 2: Contract, concurrency, NewEventPolicy, correlation_key, Webhook, Bootstrap, MCP & Executor binding tests
pyright        # Gate 3: Strict static type checking on core/ and engine/
```
