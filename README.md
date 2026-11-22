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
│   ├── models.py          # 4-state Event (pure event facts + status) / SessionRef (status, work_key, correlation_key, executor binding, parent_session_id), resolve_executor_id, TimerSpec, OutboxMessage, RunOutcome
│   └── ports.py           # 9 L1 Ports (with ExecutorPort.executor_id) + RunCapabilities (fold / interrupt control hooks)
├── engine/                # L2: Depends ONLY on core/ (never imports adapters/ or app/)
│   ├── handler.py         # EventHandler: lightweight ingest, idempotent dedup (dropped), fast ACK, async Worker wake
│   ├── worker.py          # AgentWorker: per-inbox_id lease lock, 4-level session resolution, multi-executor sticky routing, defer/fold/interrupt, exhaustive RunOutcome
│   ├── scheduler.py       # TimerScheduler: scans due Follow-up & Schedule timers -> appends Inbox events
│   └── dispatcher.py      # OutboxDispatcher: paced Outbox delivery with anti-spam throttling and retries
├── adapters/              # L3: Volatile infrastructure implementations
│   ├── storage_sqlite/    # SQLite WAL, one lock-guarded connection + BEGIN IMMEDIATE writes (Inbox, Timers, Outbox, Leases, Sessions + correlation_key/executor binding, Workspaces, task metadata)
│   ├── memory_fs/         # FileSystemMemoryAdapter + PartitionedMemoryAdapter & WorkspacePartitionRegistry
│   ├── permissions/       # Auto Mode classifier (allow / block / defer) + external CredentialProxyClient
│   ├── credential_proxy/  # External Drive queue proxy (own OS user holds the service account key; fixed queue operations on loopback)
│   ├── executor_whitebox/ # Whitebox Executor (prompt-cacheable baseline, permission gate, fold/interrupt, Suspend/Resume)
│   ├── executor_cli/      # LocalCLIExecutor (claude/cursor-agent/grok/dsh + resume_args + magpie env), shared Claude Code launch adapter (claude_code.py), CoderReviewerExecutor (read-only review) & OrcaWorktreeManager (3 safety gates)
│   └── channels/          # Multi-route HTTP (/events, /webhooks/github, /webhooks/feishu, /mcp), MCPServerAdapter, GitHubIssuePoller, FeishuChannelAdapter, Console, Drive queue poller/writer (via ProxiedDriveClient)
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

The GitHub Issue poller tolerates transient failures (`gh` errors or timeouts, network errors, GitHub API 4xx/5xx): each failing repo logs one WARNING with the repo, poll mode and the `gh` stderr or HTTP status and body excerpt, other repos keep polling, and the poller retries with exponential backoff (5s doubling, capped at 300s) before returning to `runtime.poll_interval_seconds`. `/events`, webhooks and MCP keep accepting events during an outage, and shutdown is not delayed by the backoff.

The HTTP daemon watches the file passed to `--config` once per second, including under `scripts/run_updating.py`. Save project mappings, CLI templates, worker limits or channel settings and look for `[config] Reloaded` in the console. The daemon finishes accepted work before activating the new settings, so incoming requests may briefly wait. Invalid TOML or a busy replacement port leaves the current configuration active. Changing `db_path` or `memory_root`, updating the process environment, or reconfiguring a stdio MCP session requires a restart.

The HTTP endpoints handle requests on parallel threads (backlog 128); stopping or reloading the daemon waits for requests already accepted. SQLite access from those threads and the worker loop goes through one locked connection, and writes use `BEGIN IMMEDIATE`, so deduplication and leases stay atomic. A worker runs all pending events that share a session (by `session_id`, then `correlation_key`) as one batch, in order of each group's first event. Events with neither key are unrelated and run one at a time under `defer` and `fold`. A session's batch closes at an approval reply (earlier events for that session run with it) or at an event whose `explicit_workdir` or `executor` differs; later events for that session wait for the next pass. When an approval for a session is already queued, it always joins that session's first batch and runs on the executor that suspended the session; incompatible events ahead of it, including ones routed to another executor, run afterwards. `fold` never pulls events across that boundary, and `interrupt` never restarts a Run that carries an approval.

Hibernaut checks local branch names when allocating a worktree, including branches without a checkout. If Orca adds a numeric suffix during creation, Hibernaut records the branch and path from the creation receipt after verifying both against the primary repository's Git worktree list. On the next wake, it repairs stale SQLite paths for registered worktrees. On lease acquire, a worker recovers any orphaned `processing` events back to `pending`, claims pending events (the storage-internal `event_inbox.attempts` counter grows by one per claim and survives restarts), ensures an `active` `SessionRef` and binds `event_inbox.session_id` before execution, commits `done` immediately after `Silent`/`Emit`/`Suspend` side effects, requeues on `interrupt`, and marks `failed` with `event_inbox.last_error` once retries are exhausted. `attempts` and `last_error` are not part of the L0 `Event`; read them with `SQLiteInboxAdapter.diagnostics(event_ids)` (also returned by `hibernaut_list_events`). Session rotation archives the previous session (`archived`) while keeping the worktree reference count at 1; closing a session (`closed` via event or `hibernaut_close_session`) triggers worktree reclaim only once no `active` or `suspended` session of the same inbox references its `work_key` (`count_active_by_work_key(work_key, inbox_id) == 0`). The reclaim itself checks again: a worktree owned by one inbox of a shared project (below) counts only that inbox's sessions, any other managed worktree counts sessions of every inbox.

To hand part of a task to another agent (for example a read-only Grok review of the worktree a Claude session is editing), ingest an event into the same `inbox_id` whose payload carries `parent_session_id` and optionally `work_key`, `executor_id` and `readonly`. Give it its own `correlation_key` or none; reusing the parent's key would route it back into the parent session. The parent's Run does not wait: the worker later starts a child session that records `parent_session_id` and, unless the payload names another `work_key`, inherits the parent's `work_key`, so it reuses the same worktree and counts toward its reference count while `active` or `suspended`. Closing the parent first leaves the worktree in place; it is reclaimed once the child is closed too. `readonly` is passed through to the executor unchanged. `hibernaut_list_sessions` returns `parent_session_id` and accepts it as a filter to list a session's children.

For a visible agent session, set `orca_agent = "claude"` under `[executors.claude]` and use `worktree_backend = "orca"` for the project. Restart the daemon after installing this version. Each managed run opens an Orca terminal in the background where you can watch progress and respond to permission prompts. The agent returns its `Silent`, `Emit`, or `Suspend` outcome in a per-run temporary file. Successful terminal tabs stay open, and later events for the same session (same `session_id` or `correlation_key`) go back to that conversation: the live terminal when its process is unchanged, otherwise `<command> --resume <native session id>` in a new terminal. The native session ID is confirmed from the Claude Code transcript that holds the run's unique result path and is stored in `sessions.executor_session_id`. When neither is available the run suspends with the reason and sends nothing; approve to start a new conversation. `timeout_seconds` includes time spent waiting for input. Configure agent accounts and environment in Orca. The template's `command`, `args`, and `env` apply to direct CLI runs, including Git worktrees and explicit directory overrides.

### One project, several inboxes

`projects.<name>.inbox_id` takes a string, as before, or an array that mixes exact inbox IDs with `re:` patterns:

```toml
[projects.hibernaut]
inbox_id = ["proj:hibernaut", "proj:hibernaut:drive", "re:proj:hibernaut:task-\\d+"]
repo = "kun-g/hibernaut"
checkout = "/absolute/path/to/your/hibernaut"
```

- Matching: an exact ID matches only itself; a pattern must match the whole inbox ID (Python `re.fullmatch`), so `re:proj:hibernaut:task-\d+` matches `proj:hibernaut:task-12` but not `proj:hibernaut:task-12x`. Every inbox an event names is routed through these rules, so a matched inbox gets the project's checkout, worktree and terminal backends, default `executor`, Feishu chat and reclaim settings.
- Primary inbox: the first exact ID. GitHub webhooks and polling for `repo`, and Feishu messages from `feishu_chat_id`, deliver there. Each project polls its repo once, however many inboxes it serves.
- Still separate per inbox: queue, lease, sessions, memory partition and worktrees. Two inboxes of one project with the same `correlation_key` run concurrently in different worktrees.
- Writable tasks: when a project serves more than one inbox (several entries, or any pattern), every task runs in a managed `hb-*` worktree that the workspaces table records for that inbox and `work_key`. The primary inbox keeps the bare `work_key` as its key, the others use `<inbox_id>/<work_key>`. Before launch Hibernaut refuses the primary checkout, an `explicit_workdir` other than the task's own worktree, another inbox's worktree, and a resumed checkpoint whose workdir is not that worktree: `LocalCLIExecutor` replies `refused: ...`, the SDE replies `development failed: ...`, and nothing is started. A worktree recorded before ownership existed is adopted only by the inbox whose sessions alone used it. Projects with a string `inbox_id` behave as before, including `explicit_workdir`.
- Errors at load: an empty array, non-string or empty entries, an empty or invalid pattern, an entry listed twice, an array without an exact ID, a string starting with `re:`, an exact ID assigned to two projects, and an exact ID of one project that another project's pattern matches. An invalid file on reload leaves the current settings active.
- Errors at dispatch: an inbox that matches two projects (for example two overlapping patterns) raises `AmbiguousProjectMatch` before a lease, executor or workspace is created. The worker pool logs it and the events stay `pending` until the configuration is fixed. Several rules of one project matching the same inbox count as one match.

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

## Independent Code Review (`coder-reviewer`)

Register a reviewer in `hibernaut.toml` (defaults shown; only `role` is required). Role, harness, launcher, billing and model are separate settings:

```toml
[launchers.claude-sg]       # optional; built-in "claude" runs `claude`
command = "claude-sg"       # trusted launcher on the deployment host

[credential_proxy]          # required only for billing = "api"
url = "http://127.0.0.1:9000"

[executors.coder-reviewer]
role = "coder-reviewer"
harness = "claude-code"    # only claude-code
launcher = "claude"        # a [launchers.<name>] entry
billing = "subscription"   # subscription | api
model = "claude-opus-5-5"  # Claude full name or alias (opus, sonnet, haiku, fable)
effort = "high"            # low | medium | high | xhigh | max
```

Dispatch a review with `executor_id = "coder-reviewer"`:

```bash
curl -s localhost:8080/events -H 'Content-Type: application/json' -d '{
  "inbox_id": "proj:hibernaut", "external_id": "review-42-r1", "correlation_key": "review:task-42",
  "payload": {
    "executor_id": "coder-reviewer",
    "text": "Task requirements the change must meet",
    "test_evidence": "pytest -v: 119 passed; pyright: 0 errors",
    "review_of": "task-42",
    "base_ref": "origin/main"
  }}'
```

- Workspace: `explicit_workdir`, or the ready workspace of `review_of` (the developer task's `correlation_key`), or the workdir recorded for that task. The reviewer never creates a worktree.
- Project guidance: read from the commit recorded for the `review_of` task, otherwise from the merge base of `base_ref` and `HEAD`; selected by the task's `activities`, `tags`/`labels` and the diff's paths (see Project Guidance). Use its own `correlation_key` so the developer session stays bound to its executor.
- Diff: computed by the executor from the merge base with `base_ref` (default `origin/HEAD`, then `main`), including uncommitted and untracked files.
- Result: one Outbox message with `verdict=pass` or `verdict=changes_requested`. Each blocking item lists location, trigger, impact and an optional fix. After a fix, send a new event (new `external_id`) for a fresh, independent review.
- Read-only: edit tools are denied and the working tree is fingerprinted before and after; any change discards the review.
- Launcher: chosen only in `hibernaut.toml`; event payloads cannot name a launcher or command. The launcher may set its own config dir, account, gateway or proxy (like `claude-sg`) and receives `-p --output-format stream-json --verbose --model --effort --permission-mode dontAsk` plus the reviewer's tool and schema arguments unchanged. The permission mode is always explicit, so a launcher's own default (such as `bypassPermissions`) never applies.
- Billing: both modes remove `ANTHROPIC_API_KEY`, `ANTHROPIC_AUTH_TOKEN`, `CLAUDE_CODE_OAUTH_TOKEN`, `ANTHROPIC_BASE_URL` and the Bedrock/Vertex/Foundry switches from the daemon environment before starting the launcher. `subscription` uses the launcher's own login and stops at session start if Claude Code reports any paid credential source. `api` points Claude Code at `<credential_proxy.url>/proxy/anthropic` with a placeholder key; the external proxy injects the real key, and the run stops if Claude Code authenticates any other way. Credentials never go in `hibernaut.toml` (the parser rejects them).
- Hot reload: save the settings (including launcher) and the next review uses them. Invalid values or combinations print `[config] Invalid ... (ValueError: <reason>)` and keep the running settings.

## Role Skills

How each role works comes from a skill; what it may do and what it must return stays in code. Each role-based executor takes an ordered `skills` list in its own `[executors.<id>]` table:

```toml
[executors.sde]
role = "sde"
skills = ["builtin:sde", "./skills/team-tdd", "~/skills/security/SKILL.md"]

[executors.coder-reviewer]
role = "coder-reviewer"
skills = ["./skills/team-review"]   # replaces the built-in review method
```

- Entries: a local `SKILL.md`, a directory containing one, or `builtin:<role>` (the skill shipped in the package under `agent_runtime/adapters/executor_cli/skills/<role>/`). Relative paths resolve against the directory of `hibernaut.toml`; `~` expands.
- Default: without `skills`, the role uses its built-in skill (`sde`: test-first development; `coder-reviewer`: the review method). A listed `skills` replaces the default; list `builtin:<role>` to keep it and add your own. A role can name only its own built-in skill.
- Delivery: every launch reads the files again and passes them, in order, with `--append-system-prompt`, each headed by its absolute path and resource directory (`Relative resources are relative to <dir>`). Editing a `SKILL.md` affects the next run without a reload.
- Fixed by code: the system prompt starts with the role's contract (what to return, no force-push for `sde`, read-only for the reviewer). Tools, permission mode, JSON schema and launcher come from the executor and `hibernaut.toml`. A skill, an event payload or a project document cannot change them or choose a skill.
- Errors: a missing file, an empty list or another role's `builtin:` entry fails config parsing with the executor name and path (`executors.sde: skill './skills/team-tdd' not found (no file at ...)`); hot reload keeps the running settings. A skill deleted after loading fails that run before launch.
- Separate roles: the SDE's built-in review uses the reviewer executor's own `skills`, never the SDE's.

## Project Guidance (`AGENTS.md` + `guidance.toml`)

A project keeps its own process in its repository, versioned with its code. Two files at the repository root:

- `AGENTS.md`: short general rules for every task and a pointer to the catalog. Always included.
- `guidance.toml`: the catalog of detailed documents. Each entry names the document, a one-line summary, the activities and tags it applies to, and the file paths it governs.

```markdown
# AGENTS.md
- Run `make check` before every commit; all of it must pass.
- Detailed rules for specific work are listed in guidance.toml.
```

```toml
# guidance.toml
[[docs]]
path = "docs/process/versioning.md"   # repository-relative, must exist in the same commit
summary = "Changing the version"
activities = ["versioning", "release"]  # matched against the task's explicit activities
tags = ["version"]                      # matched against the task's tags or issue labels
paths = ["VERSION", "src/*/__init__.py"] # a review also selects it when the diff touches these

[[docs]]
path = "docs/process/testing.md"
summary = "Writing tests"
activities = ["testing"]
tags = ["tests"]
paths = ["tests/**"]                    # "*" stays in one directory, "**" spans directories
```

- Selection for development: explicit `activities` in the task payload (list or comma-separated string) win; without them the task's `tags` and `labels` (GitHub label objects included) pick documents by `tags`. Matching ignores case.
- Selection for review: the same, plus every document whose `paths` match a file the actual diff changes (committed, uncommitted or untracked). Paths are read unquoted (`git -z`), so non-ASCII names such as `docs/规范/版本.md` match as written. A version bump inside a test-only task therefore brings the versioning document into the review.
- Nothing selected: the task message carries `AGENTS.md` and the catalog (path, summary, activities, tags, paths) and says that no document matched. Bodies of unselected documents are never included; the agent can read one with `git show <commit>:<path>` if it turns out to need it.
- Pinned commit: every guidance file is read from a commit with `git show <commit>:<path>`, never from the working tree. Each quoted body is headed `Source: <commit>:<path>`, so `git show` reproduces exactly what the agent received. Uncommitted edits to guidance files are never used.
  - SDE task: the worktree's `HEAD` when the task starts. It is stored with the task's activities and tags in the `task_metadata` table under the task's `correlation_key`, and in the session checkpoint. Development, review rounds, fix rounds, resumed runs and later tasks on the same `correlation_key` and worktree all read that commit, even after the change commits edits to the guidance.
  - Independent review with `review_of`: the commit recorded for that developer task, plus its activities and tags merged with the review event's own. The workdir falls back to the task's recorded workdir when there is no ready workspace.
  - Independent review without a recorded task (only `explicit_workdir`): the merge base of `base_ref` and `HEAD`, the same base the diff is computed from. The change under review starts after it and cannot rewrite its own review rules. The message says `Pinned to commit <sha> (merge base of <ref> and HEAD; ...)`; `git merge-base <ref> HEAD` verifies it.
- Review citations: a blocking item that rests on project guidance carries `guidance_path` and `guidance_clause` (both or neither). The review shows them as `Guidance:` and `Clause:` lines. In the SDE's fix round, cited documents the developer session has not received are sent with the fix request, read from the same pinned commit; each is sent once.
- Errors: an invalid `guidance.toml` (bad TOML, unknown key, duplicate or missing document, a path outside the repository) or a guidance file that is a symlink resolving outside the repository fails the run before launch with the file and commit. A failing built-in review suspends as `review pending`.
- Limits: the text is project requirements. It cannot grant tools, change the permission mode, pick a launcher or a role skill, or change the report format.
- Neither file: the task message is the same as before, with no project section. `AGENTS.md` alone: it is included without a catalog.
- Keep project-specific rules such as versioning and release steps in the project's documents, not in role skills.

## SDE Executor (`sde`)

An SDE develops a task test-first in its own worktree and delivers the change with red/green test evidence. Register it next to a reviewer (defaults shown; only `role` is required):

```toml
[executors.sde]
role = "sde"
harness = "claude-code"
launcher = "claude"
billing = "subscription"
model = "claude-opus-5-5"
effort = "high"
review = true                # runtime switch: false delivers after tests and repo checks
reviewer = "coder-reviewer"  # a role = "coder-reviewer" executor; required while review = true
max_review_rounds = 3
```

Dispatch with `executor_id = "sde"`:

```bash
curl -s localhost:8080/events -H 'Content-Type: application/json' -d '{
  "inbox_id": "proj:hibernaut", "external_id": "task-42", "correlation_key": "task-42",
  "payload": {"executor_id": "sde", "text": "Task requirements", "base_ref": "main"}}'
```

- Workspace: `explicit_workdir`, else the task worktree for `correlation_key` (created on first use, reused afterwards), like other executors.
- Guidance selection: optional `activities` (for example `["versioning"]`) and `tags` or `labels` in the payload pick the project documents the developer receives; the commit and selection are recorded for the task when it starts.
- Development: the launcher starts Claude Code with `--permission-mode dontAsk`, read/edit/write tools and Bash allowed, normal `git push` and `gh pr` commands allowed, and force-push (`--force`, `--force-with-lease`, `-f`, `+refspec`) and subagents denied. The system prompt carries the role contract and the role skills (by default: a failing test first, the smallest fix, the repository's full checks, version bump when tracked, commit, push, and open or update a pull request); the task message carries the requirements and the project guidance selected for the task from its pinned commit (see Project Guidance). The run returns a structured report (`summary`, `test_evidence`); a question for a person becomes a `Suspend`: answer by replying on the task (same `correlation_key`) or with an approval `comment`, then approve. Rejecting stops the task with `[sde] stopped: question declined`.
- Review on: the SDE sends the requirements, the diff it computes from the worktree (not the developer's own description) and the test evidence to the configured `coder-reviewer`, which runs as its own read-only session with its own launcher, billing and model. `changes_requested` sends every blocking item back to the same developer session for a fix; the re-review also gets the previous findings and the diff since that review. `pass` delivers. If the review cannot finish (reviewer error, malformed verdict, model or billing mismatch) or rounds run out, the run suspends as `review pending`: approve to retry (another fix round when changes were requested), reject to deliver marked `review=pending`.
- Review off: the SDE delivers after its own tests and checks (`review=off`).
- While suspended: messages on the same task are kept with the suspended state (the open approval card is not posted again). Messages that arrive in the same batch as the decision count too. On approval they are added to the requirements, developed first, and shown to the reviewer; a rejected review retry lists them as unprocessed in the delivery.
- Permission gate `sde.launch` runs before every developer launch, fix rounds included. `defer` suspends with the full state; approval runs that launch, rejection stops with `[sde] stopped: launch declined`; `block` stops with `[sde] stopped: launch blocked`.
- Delivery: one Outbox message `[sde] delivered (review=pass after N round(s) | review=off | review=pending; harness=... launcher=... billing=... model=... effort=...)` with workdir, summary, test evidence, review history and the last review.
- Hot reload: all four launch settings and `review` take effect on the next run; invalid values keep the running settings. Billing and launcher rules are the same as for `coder-reviewer`. `sde.launch` is allowed by default in the auto-mode permission adapter.

## Interactive Role Sessions

`interactive` on a role executor (default `true`) means each `sde` or `coder-reviewer` run needs a session a person can join. The project's `terminal_backend` opens it; `interactive = false` runs the role non-interactively (headless), whatever the project.

- Terminal backend: `terminal_backend = "orca"` opens an interactive Claude Code terminal in the target worktree through Orca. It is set per project, independently of `worktree_backend`: it defaults to `"orca"` for `worktree_backend = "orca"` projects (their behaviour is unchanged) and to `"none"` for `worktree_backend = "git"` projects. A Git worktree project can set `terminal_backend = "orca"` to get Orca terminals in its Git worktrees. Orca is the only terminal backend.
- No backend: an interactive role never falls back to a background run. With `terminal_backend = "none"`, or for an inbox that matches no `[projects]` entry, the SDE suspends with `[sde] interactive session unavailable: <reason>` and starts nothing. Set `terminal_backend = "orca"` on the project or `interactive = false` on the role (the config reloads), then approve to retry; rejecting stops the task with `[sde] stopped: interactive session declined`. An independent review reports `[coder-reviewer] review failed: interactive review not started: <reason>`. A built-in review leaves the SDE task `review pending` with that reason.
- Attach failure: when Orca cannot open or reach the terminal (no `orca` command, worktree unknown to Orca, a failed session check), the run reports the Orca error (`[sde] development failed: Orca terminal ...`, `[coder-reviewer] review failed: ...`) instead of running headless.
- Migration: `visible` is the old name of `interactive` and is still read. Setting both on one executor is a configuration error. On an existing Git worktree project or unmatched inbox that ran roles headless, add `interactive = false` to keep doing so.

- Terminals: titled `sde · <task>`, `coder-reviewer · review of <task> round <n>` (built-in review) and `coder-reviewer · review of <task>` (independent `review_of` event). Watch progress and answer the agent there. The delivery and review messages list the Orca terminal handle and the Claude Code session ID.
- Settings: the same role prompt, tools, launcher, billing, model and effort as a headless run, with `--permission-mode dontAsk`. A launcher in the terminal first runs the same session check as a headless run (subscription vs. proxy billing, model), then starts the conversation; a failed check closes the terminal and reports the reason.
- Result: the launcher gives Claude Code a `hibernaut_handoff` MCP server. The agent calls `get_task` to read its task and `submit_result` to return the structured result. The server checks the result against the role schema and rejects a malformed one so the agent can fix it. The agent never reads or writes the handoff directory, so no `--add-dir` trust dialog appears. Claude Code still asks once if the worktree itself is not yet trusted.
- Continuation: SDE follow-up events, answers after `needs_input` and review fix rounds go back to the task's live terminal, or `--resume` its conversation in a new terminal once the tab is closed. Every review is a new conversation; a re-review receives the previous findings in its prompt.
- Read-only review: the interactive reviewer has no file-writing tool; Edit, Write, MultiEdit and NotebookEdit are denied. The worktree fingerprint check still discards a review if anything changed.
- Developer channel: an independent `review_of` review and its developer talk through Claude Code's native `SendMessage`; each keeps its own `/events` task, native session and result. Hibernaut names the developer only after verifying the binding: an open session for the `review_of` key in the same inbox, run by an `sde` executor, with a terminal binding in the reviewed worktree, whose native session is live under exactly one name in the Claude Code session registry (`<config dir>/sessions`) with that worktree as its cwd. Only then does the reviewer get the developer's exact session name and messaging socket (Claude Code sends it as the `from` of every reply, with the name in `from-name`) and `SendMessage`/`ListAgents` (still no file-writing tool); otherwise its prompt says why there is no channel and it messages no one. The reviewer tags messages `[review of <task>]`, sends questions and its conclusion, and records the exchange in `developer_messages`; the review message shows `Developer channel:` and the exchange. The SDE is told its reviewer's address (`<reviewer> · review of <task>`, or `<name>-<words>` when Claude Code de-duplicates the name for a second review), answers only such a sender whose message starts with the exact `[review of <task>]` tag, with evidence, and does not change the worktree while a review is open. The SDE cannot start a reviewer as its own subagent: `Agent` and `Task` are denied.
- Timeout and cancel: `timeout_seconds` includes time spent waiting for a person. A timeout or interrupt closes the terminal and writes nothing to the Outbox. The SDE conversation stays bound and resumes on the next event.

## MCP Server Integration (`stdio` & Remote `/mcp`)

Hibernaut exposes 7 MCP tools (`hibernaut_ingest_event`, `hibernaut_list_sessions`, `hibernaut_list_events`, `hibernaut_list_workspaces`, `hibernaut_resolve_approval`, `hibernaut_reclaim_workspace`, `hibernaut_close_session`).

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

## Google Drive Task Queue (`DriveTaskPoller` + `DriveResultWriter`)

Run a tunnel-free cloud-to-local task loop via Google Drive (`待执行/` → `执行中/` → `已完成/` / `失败/`), supporting `.md` files with optional YAML frontmatter (`scope`, `executor_id`, `priority`, `title`) and Google Docs (auto-exported as `text/plain`). Re-editing a file and moving it back to `待执行/` automatically continues the same `Session` and `hb-*` worktree via `correlation_key = "gdrive:<file_id>"`.

### Setup: service account behind the external credential proxy

Hibernaut never holds the service account key. An external credential proxy, running as its own system user, keeps the key, calls Google Drive, and answers only the queue's operations (list a queue folder, read a task file, move it between the four queue folders, append the result) on `127.0.0.1`. Hibernaut reaches it through `CredentialProxyClient` with a local client token and receives file metadata, task text and `ok`, never a Google token.

1. **Service account**: In Google Cloud Console, enable the **Google Drive API**, create a Service Account (no IAM roles needed) and a JSON key.
2. **Share only the queue folder**: Create `任务队列/{待执行, 执行中, 已完成, 失败}` in Drive and share the `任务队列` folder as **Editor** with the Service Account email only.
3. **Install the proxy** (macOS LaunchDaemon or Linux systemd unit; details, allowed operations and maintenance in [`deploy/gdrive-proxy/README.md`](deploy/gdrive-proxy/README.md)):
   ```bash
   sudo deploy/gdrive-proxy/install.sh --key ~/.config/hibernaut/gdrive-sa.json \
     --root-folder-id <queue folder ID from its Drive URL> --hb-user "$USER"
   ```
   It copies the key to `/etc/hibernaut-drive-proxy/sa.json` (readable only by the proxy user), writes the client token to `~/.config/hibernaut/gdrive-proxy.token`, checks the queue read-only and starts the service. Delete the original key file afterwards.
4. **Enable `[gdrive]` in `hibernaut.toml`** and restart Hibernaut:
   ```toml
   [gdrive]
   enabled = true
   proxy_url = "http://127.0.0.1:9100"      # default: [credential_proxy] url
   proxy_token_file = "~/.config/hibernaut/gdrive-proxy.token"
   root_folder_id = "<queue folder ID>"
   default_inbox_id = "proj:hibernaut"
   ```

- `proxy_token_file` (relative paths resolve against the directory of `hibernaut.toml`) must be mode 0600; if it is missing, empty or readable by others, Hibernaut prints the reason and starts without the Drive poller.
- `service_account_key_env` is no longer accepted: the config fails with a message pointing to the proxy. `HIBERNAUT_GDRIVE_SA_KEY` is ignored.
- A proxy refusal or Drive error reaches the poller as `DriveProxyError` with the HTTP status and reason; the poll logs it and retries on the next interval.

## Switchboard (Phase 1)

`src/switchboard/` is a separate package in this repository: a passive exchange that normalizes inbound GitHub webhooks, Feishu events and card callbacks, and MCP tool calls into one `CanonicalEvent` envelope, stores them in its own SQLite database, delivers them to consumers (`taskboard`, `hibernaut`, `switchboard.outbound`) and sends replies through one outbound gateway. It imports nothing from `agent_runtime`, `taskboard`, `runner` or `memory_hub` (enforced by `.importlinter`), and it has no pollers or timers: external cron jobs call the MCP tool `submit_work` with `source="timer"`.

```bash
cp switchboard.example.toml switchboard.toml
export SWITCHBOARD_GITHUB_WEBHOOK_SECRET=...   # GitHub webhook HMAC secret
export SWITCHBOARD_FEISHU_WEBHOOK_SECRET=...   # Feishu verification token
export SWITCHBOARD_MCP_WEBHOOK_SECRET=...      # Bearer token for /mcp
export SWITCHBOARD_GITHUB_TOKEN=... SWITCHBOARD_FEISHU_TOKEN=...   # outbound API tokens
PYTHONPATH=src python3 -m switchboard.app --config switchboard.toml
```

Secrets are read through `CredentialBrokerPort`; the default `EnvCredentialBroker` reads the variables above and nothing goes in the TOML file. Endpoints on one port: `POST /webhooks/github`, `POST /webhooks/feishu`, `POST /mcp` (tools `submit_work`, `post_message`, `decide_approval`, `replay_delivery`) and `GET /healthz`.

- A GitHub redelivery with a new `X-GitHub-Delivery` is still a duplicate: events are deduplicated by business fingerprint (`issue:{repo}#{num}:{action}:{updated_at}`, `pr:...`, `comment:{id}`).
- A consumer that still fails after the last retry (one retry per `retry_backoff_seconds` entry, default 5/15/45/135/405 s) goes to `dead_letter` and an alert is sent to `[delivery] dead_letter_alert_channel/target`; later events for the same consumer and scope wait, other scopes and consumers continue. Replay with the MCP tool `replay_delivery`.
- An `execution.reported` event with outcome `suspend` becomes a Feishu approval card at `[outbound] approval_target`. The button click returns a read-only card at once and is recorded as `approval.decided` with the original `correlation_key` (for example `github:kun-g/hibernaut#42`); a second click on the same request is dropped.
- Phase 1 delivers in-process only. Consumers register with `board.bus.subscribe(consumer_id, handler)`; an unregistered consumer counts as offline and its deliveries stay pending.

See `docs/architecture.md` section 7 for the full design.

## Anti-Corruption Gates (CI)

```bash
pip install -e ".[dev]"

lint-imports   # Gate 1: Static import boundary checks
pytest -v      # Gate 2: Contract, concurrency, NewEventPolicy, correlation_key, Webhook, Bootstrap, MCP & Executor binding tests
pyright        # Gate 3: Strict static type checking on core/, engine/ and switchboard/
```
