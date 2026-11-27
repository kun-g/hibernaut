from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import math
import re
import secrets
import shlex
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit

from agent_runtime.adapters.executor_cli.process import ProcessTimeout, supervise
from agent_runtime.adapters.executor_cli.quota import parse_quota_errors
from agent_runtime.adapters.executor_cli.role_skills import builtin_skill, parse_skills, render_skills

CLAUDE_CODE = "claude-code"
HARNESSES = (CLAUDE_CODE,)
BILLINGS = ("subscription", "api")
EFFORTS = ("low", "medium", "high", "xhigh", "max")
DEFAULT_LAUNCHER = "claude"
# Stands in for the key the external credential proxy injects; the real key never reaches the harness.
PROXY_PLACEHOLDER_KEY = "hibernaut-credential-proxy"
# Any of these makes Claude Code bill an API key or a cloud provider instead of the subscription.
_PAID_ENV_KEYS = (
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "CLAUDE_CODE_USE_BEDROCK",
    "CLAUDE_CODE_USE_VERTEX",
    "CLAUDE_CODE_USE_FOUNDRY",
)
# Never inherited: a daemon-wide token or base URL would override the launcher's own login, or send the
# subscription token to a gateway the operator did not pick for it. The deployment file may set them explicitly.
_ROUTING_ENV_KEYS = (*_PAID_ENV_KEYS, "ANTHROPIC_BASE_URL", "CLAUDE_CODE_OAUTH_TOKEN")
# Matched against each "_"-separated part of a key, so MAX_THINKING_TOKENS stays a setting while GH_TOKEN is a credential.
_CREDENTIAL_PART_RE = re.compile(r".*(KEY|TOKEN|SECRET|PASSWORD|PASSWD|AUTH|CREDENTIALS?|COOKIE)|PASS|PAT")
# A configured credential may only be a placeholder for a gateway on this machine, which holds the real one.
_MAX_PLACEHOLDER_CHARS = 24
_REAL_SECRET_PREFIXES = ("sk-", "ghp_", "gho_", "ghs_", "ghu_", "ghr_", "github_pat_", "glpat-", "xox", "AKIA", "AIza",
                         "ya29.", "eyJ", "-----BEGIN")
_ENV_CHECK_MARK = "hibernaut-env-check "
# Runs as a SessionStart hook inside Claude Code: salted hashes of the values it really started with, never the values.
_ENV_CHECK_HOOK = (
    "import hashlib,json,os,sys\n"
    "s=sys.argv[1]\n"
    "h=lambda v:None if v is None else hashlib.sha256((s+chr(0)+v).encode(errors='surrogateescape')).hexdigest()\n"
    f"sys.stderr.write({_ENV_CHECK_MARK!r}+json.dumps({{k:h(os.environ.get(k)) for k in sys.argv[2:]}}))\n"
)
_STREAM_LIMIT = 16 * 1024 * 1024
# The end of what a launcher printed outside its JSON messages, kept for telling why it failed.
_MAX_PLAIN_CHARS = 16_000
PROFILE_KEYS = frozenset({"harness", "launcher", "billing", "model", "effort", "timeout_seconds", "env", "skills",
                          "quota_errors"})


def _empty_env() -> dict[str, str]:
    return {}


def _no_skills() -> tuple[Path, ...]:
    return ()


def _str_env(where: str, raw: object) -> dict[str, str]:
    if not isinstance(raw, Mapping):
        raise ValueError(f"{where}: env must be a table")
    return {str(k): str(v) for k, v in cast(Mapping[object, object], raw).items()}


def is_credential_key(key: str) -> bool:
    return any(_CREDENTIAL_PART_RE.fullmatch(part) for part in key.upper().split("_"))


def is_loopback_url(url: str) -> bool:
    try:
        parts = urlsplit(url.strip())
        host = parts.hostname
    except ValueError:
        return False
    if parts.scheme not in ("http", "https") or not host:
        return False
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _url_password(value: str) -> bool:
    try:
        return bool(urlsplit(value.strip()).password)
    except ValueError:
        return False


def _check_placeholders(section: str, env: Mapping[str, str]) -> None:
    # Errors name the key and its section only: the value may be the very secret being refused.
    for key, value in env.items():
        if _url_password(value):
            raise ValueError(f"{section}.{key} carries a password in its URL; the agent can read its env, so the "
                             "credential stays in the gateway or the credential proxy")
        if is_credential_key(key) and (len(value) > _MAX_PLACEHOLDER_CHARS or value.startswith(_REAL_SECRET_PREFIXES)):
            raise ValueError(
                f"{section}.{key} looks like a real credential (a known key prefix or longer than "
                f"{_MAX_PLACEHOLDER_CHARS} characters); the agent can read its env, so real keys stay in the gateway "
                "or the credential proxy and the env holds only a short placeholder"
            )


def _check_routes(where: str, sections: Sequence[tuple[str, Mapping[str, str]]], billing: str) -> None:
    merged = {k: v for _, env in sections for k, v in env.items()}
    base_url = merged.get("ANTHROPIC_BASE_URL", "")
    local = is_loopback_url(base_url)
    if not local:
        for section, env in sections:
            for key in env:
                if is_credential_key(key):
                    raise ValueError(
                        f"{section}.{key} is a credential, allowed only beside an ANTHROPIC_BASE_URL on a loopback "
                        "address (a gateway on this machine that holds the real key); remove it or route through such "
                        "a gateway"
                    )
    if billing == "subscription" and base_url and not local:
        raise ValueError(
            f"{where}: ANTHROPIC_BASE_URL is not a loopback address and no gateway token is set, so Claude Code "
            "would send the subscription's OAuth token to it; point it at a gateway on this machine"
        )


@dataclass(frozen=True, slots=True)
class ClaudeCodeLauncher:
    # A trusted command that starts the harness; it may pick its own config dir, account, gateway or proxy,
    # and must pass model, effort and role arguments through unchanged.
    name: str
    command: str
    harness: str = CLAUDE_CODE
    env: dict[str, str] = field(default_factory=_empty_env)

    def __post_init__(self) -> None:
        where = f"launchers.{self.name}"
        if self.harness not in HARNESSES:
            raise ValueError(f"{where}: unsupported harness '{self.harness}' (supported: {', '.join(HARNESSES)})")
        if not self.command.strip():
            raise ValueError(f"{where}: command must not be empty")
        _check_placeholders(f"{where}.env", self.env)


BUILTIN_LAUNCHERS: dict[str, ClaudeCodeLauncher] = {
    DEFAULT_LAUNCHER: ClaudeCodeLauncher(name=DEFAULT_LAUNCHER, command="claude"),
}


def _default_launcher() -> ClaudeCodeLauncher:
    return BUILTIN_LAUNCHERS[DEFAULT_LAUNCHER]


@dataclass(frozen=True, slots=True)
class ClaudeCodeProfile:
    name: str
    harness: str = CLAUDE_CODE
    launcher: ClaudeCodeLauncher = field(default_factory=_default_launcher)
    billing: str = "subscription"
    model: str = "claude-opus-5-5"
    effort: str = "high"
    timeout_seconds: float = 1800.0
    env: dict[str, str] = field(default_factory=_empty_env)
    # Anthropic base URL on the external credential proxy; billing "api" sends every request there.
    credential_proxy_url: str = ""
    # Ordered SKILL.md files from the deployment file; empty means the role's built-in skill.
    skills: tuple[Path, ...] = field(default_factory=_no_skills)
    # Regular expressions; a non-interactive review that exits non-zero and reports a match has no quota left.
    quota_errors: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        where = f"executors.{self.name}"
        if self.harness not in HARNESSES:
            raise ValueError(f"{where}: unsupported harness '{self.harness}' (supported: {', '.join(HARNESSES)})")
        if self.launcher.harness != self.harness:
            raise ValueError(
                f"{where}: launcher '{self.launcher.name}' starts harness '{self.launcher.harness}', not '{self.harness}'"
            )
        if self.billing not in BILLINGS:
            raise ValueError(f"{where}: unsupported billing '{self.billing}' (supported: {', '.join(BILLINGS)})")
        if self.billing == "api" and not self.credential_proxy_url:
            raise ValueError(f"{where}: billing 'api' needs [credential_proxy] url; real keys never reach the harness")
        if self.effort not in EFFORTS:
            raise ValueError(f"{where}: unsupported effort '{self.effort}' (supported: {', '.join(EFFORTS)})")
        # Fields come straight from the deployment file, so the annotation is not yet a guarantee here.
        if not isinstance(self.model, str) or not self.model.strip():  # pyright: ignore[reportUnnecessaryIsInstance]
            raise ValueError(f"{where}: model must be a non-empty string")
        if not math.isfinite(self.timeout_seconds) or self.timeout_seconds <= 0:
            raise ValueError(f"{where}: timeout_seconds must be finite and positive")
        if self.billing == "api":
            # The init check cannot see a second token or a cloud switch beside the proxy's placeholder key.
            for source, env in ((f"launchers.{self.launcher.name}", self.launcher.env), (where, self.env)):
                for key in _ROUTING_ENV_KEYS:
                    if key in env:
                        raise ValueError(
                            f"{where}: billing 'api' sends every request through [credential_proxy] and conflicts "
                            f"with {source}.env.{key}; remove it or set billing = \"subscription\""
                        )
        _check_placeholders(f"{where}.env", self.env)
        _check_routes(where, ((f"launchers.{self.launcher.name}.env", self.launcher.env), (f"{where}.env", self.env)),
                      self.billing)


# Keys a pure [executors.<id>] (no role, no command) may set; an agent picks its own model, effort and skills.
EXECUTOR_KEYS = PROFILE_KEYS - {"skills"}
# What an [agents.<id>] may set on top of its executor's profile.
AGENT_PROFILE_KEYS = frozenset({"model", "effort", "skills"})


def parse_launchers(raw: Mapping[str, Any]) -> dict[str, ClaudeCodeLauncher]:
    launchers = dict(BUILTIN_LAUNCHERS)
    for name, item in raw.items():
        where = f"launchers.{name}"
        if not isinstance(item, Mapping):
            raise ValueError(f"{where} must be a table")
        entry = cast(Mapping[str, Any], item)
        unknown = sorted(set(entry) - {"harness", "command", "env"})
        if unknown:
            raise ValueError(f"{where}: {', '.join(unknown)} not supported")
        if "command" not in entry:
            raise ValueError(f"{where}: command is required")
        launchers[str(name)] = ClaudeCodeLauncher(
            name=str(name),
            command=str(entry["command"]),
            harness=str(entry.get("harness", CLAUDE_CODE)),
            env=_str_env(where, entry.get("env", {})),
        )
    return launchers


def parse_profile(
    name: str,
    item: Mapping[str, Any],
    *,
    role: str,
    launchers: Mapping[str, ClaudeCodeLauncher],
    credential_proxy_url: str,
    extra_keys: frozenset[str] = frozenset(),
    base_dir: Path | None = None,
) -> ClaudeCodeProfile:
    where = f"executors.{name}"
    unknown = sorted(set(item) - PROFILE_KEYS - extra_keys - {"role"})
    if unknown:
        raise ValueError(f"{where}: {', '.join(unknown)} not supported for role '{role}'")
    launcher_name = str(item.get("launcher", DEFAULT_LAUNCHER))
    launcher = launchers.get(launcher_name)
    if launcher is None:
        raise ValueError(f"{where}: unknown launcher '{launcher_name}' (configured: {', '.join(sorted(launchers))})")
    defaults = ClaudeCodeProfile(name=name)
    return ClaudeCodeProfile(
        name=name,
        harness=str(item.get("harness", defaults.harness)),
        launcher=launcher,
        billing=str(item.get("billing", defaults.billing)),
        model=item.get("model", defaults.model),
        effort=str(item.get("effort", defaults.effort)),
        timeout_seconds=float(item.get("timeout_seconds", defaults.timeout_seconds)),
        env=_str_env(where, item.get("env", {})),
        credential_proxy_url=credential_proxy_url,
        skills=parse_skills(where, role, item.get("skills"), base_dir or Path.cwd()),
        quota_errors=parse_quota_errors(where, item.get("quota_errors")),
    )


def role_system_prompt(profile: ClaudeCodeProfile, *, role: str, contract: str) -> str:
    # The contract is program-owned and comes first; skills only shape how the role works inside it.
    skills = profile.skills or (builtin_skill(role),)
    try:
        body = render_skills(skills)
    except ValueError as exc:
        raise ClaudeCodeError(f"executors.{profile.name}: {exc}") from exc
    return f"{contract}\n\n{body}"


def configured_env(profile: ClaudeCodeProfile) -> dict[str, str]:
    return {**profile.launcher.env, **profile.env}


def env_overrides(profile: ClaudeCodeProfile) -> tuple[tuple[str, ...], dict[str, str]]:
    # (keys to remove, then values to set) on top of whatever environment starts the launcher.
    values = configured_env(profile)
    if profile.billing == "api":
        values["ANTHROPIC_BASE_URL"] = profile.credential_proxy_url
        values["ANTHROPIC_API_KEY"] = PROXY_PLACEHOLDER_KEY
    return _ROUTING_ENV_KEYS, values


def child_env(profile: ClaudeCodeProfile, base: Mapping[str, str]) -> dict[str, str]:
    unset, values = env_overrides(profile)
    env = {k: v for k, v in base.items() if k not in unset}
    env.update(values)
    return env


@dataclass(frozen=True, slots=True)
class EnvCheck:
    # The configured routing and credential variables, which a subscription session must start with exactly as
    # configured: a launcher must not export, say, a key from the keychain over them. The salt is new for every launch.
    keys: tuple[str, ...]
    salt: str


def env_check(profile: ClaudeCodeProfile) -> EnvCheck | None:
    if profile.billing != "subscription":
        return None
    keys = tuple(sorted(k for k in configured_env(profile) if k in _ROUTING_ENV_KEYS or is_credential_key(k)))
    return EnvCheck(keys=keys, salt=secrets.token_hex(16)) if keys else None


def env_fingerprint(salt: str, value: str) -> str:
    return hashlib.sha256(f"{salt}\0{value}".encode(errors="surrogateescape")).hexdigest()


def _env_check_args(check: EnvCheck) -> list[str]:
    # The hook's report reaches the stream before init; it goes to stderr so it never enters the model's context.
    command = shlex.join([sys.executable, "-I", "-c", _ENV_CHECK_HOOK, check.salt, *check.keys])
    settings = {"hooks": {"SessionStart": [{"hooks": [{"type": "command", "command": command}]}]}}
    return ["--include-hook-events", "--settings", json.dumps(settings)]


def harness_args(
    profile: ClaudeCodeProfile, *, permission_mode: str, role_args: Sequence[str], stream: bool = True,
    check: EnvCheck | None = None,
) -> list[str]:
    # An explicit mode is mandatory: launchers such as claude-sg fall back to bypassPermissions without one.
    # Without the stream only the result is printed, so the session has to be checked before (claude_gate.py).
    return [
        "-p",
        *(("--output-format", "stream-json", "--verbose") if stream else ("--output-format", "json")),
        "--model", profile.model,
        "--effort", profile.effort,
        "--permission-mode", permission_mode,
        *(_env_check_args(check) if check is not None else ()),
        *role_args,
    ]


def with_mcp_server(
    role_args: Sequence[str], name: str, server: Mapping[str, Any], tools: Sequence[str]
) -> list[str]:
    # Adds the server to the one --mcp-config value and its tools to the allowed ones.
    args = list(role_args)
    allowed = [f"mcp__{name}__{tool}" for tool in tools]
    if "--allowedTools" in args:
        i = args.index("--allowedTools") + 1
        args[i] = ",".join((args[i], *allowed))
    else:
        args += ["--allowedTools", ",".join(allowed)]
    if "--mcp-config" not in args:
        args += ["--mcp-config", json.dumps({"mcpServers": {}})]
    i = args.index("--mcp-config") + 1
    args[i] = json.dumps({"mcpServers": {**json.loads(args[i])["mcpServers"], name: dict(server)}})
    return args


def interactive_args(profile: ClaudeCodeProfile, *, permission_mode: str, role_args: Sequence[str]) -> list[str]:
    # The same settings as harness_args, for a conversation a person can watch and answer.
    return [
        "--model", profile.model,
        "--effort", profile.effort,
        "--permission-mode", permission_mode,
        *role_args,
    ]


# A probe only needs the init message; with no tools it can do nothing even if it outlives the check.
PROBE_ROLE_ARGS = ("--no-session-persistence", "--tools", "")
PROBE_PROMPT = "Reply with OK."


class ClaudeCodeError(Exception):
    # Of a process that ended: its exit code and what it reported itself, without the conversation.
    def __init__(self, text: str, *, returncode: int | None = None, output: str = "") -> None:
        super().__init__(text)
        self.returncode = returncode
        self.output = output


async def run_claude_code(
    profile: ClaudeCodeProfile,
    *,
    cwd: Path,
    prompt: str,
    permission_mode: str,
    role_args: Sequence[str],
    env: Mapping[str, str],
    is_interrupted: Callable[[], bool],
    poll_interval: float,
) -> dict[str, Any]:
    launcher = profile.launcher
    check = env_check(profile)
    try:
        proc = await asyncio.create_subprocess_exec(
            launcher.command,
            *harness_args(profile, permission_mode=permission_mode, role_args=role_args, check=check),
            cwd=str(cwd),
            env=dict(env),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=_STREAM_LIMIT,
        )
    except OSError as exc:
        raise ClaudeCodeError(
            f"cannot start launcher '{launcher.name}' ({launcher.command}): {exc.strerror or exc}"
        ) from exc
    try:
        return await supervise(
            proc,
            _consume(profile, proc, prompt, check),
            is_interrupted=is_interrupted,
            timeout_seconds=profile.timeout_seconds,
            poll_interval=poll_interval,
        )
    except ProcessTimeout as exc:
        raise ClaudeCodeError(str(exc)) from exc


async def _consume(
    profile: ClaudeCodeProfile, proc: asyncio.subprocess.Process, prompt: str, check: EnvCheck | None
) -> dict[str, Any]:
    assert proc.stdin is not None and proc.stdout is not None and proc.stderr is not None
    stdin = proc.stdin

    async def feed() -> None:
        try:
            stdin.write(prompt.encode("utf-8"))
            await stdin.drain()
            stdin.close()
        except (BrokenPipeError, ConnectionResetError):
            pass

    stderr_task = asyncio.create_task(proc.stderr.read())
    feed_task = asyncio.create_task(feed())
    try:
        return await _read_stream(profile, proc, stderr_task, check)
    finally:
        for task in (feed_task, stderr_task):
            if not task.done():
                task.cancel()


async def _read_stream(
    profile: ClaudeCodeProfile, proc: asyncio.subprocess.Process, stderr_task: asyncio.Task[bytes],
    check: EnvCheck | None,
) -> dict[str, Any]:
    assert proc.stdout is not None
    initialized = False
    result: dict[str, Any] | None = None
    plain = ""
    hooks: list[Mapping[str, Any]] = []
    while line := await proc.stdout.readline():
        try:
            parsed: object = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            plain = (plain + line.decode("utf-8", errors="replace"))[-_MAX_PLAIN_CHARS:]
            continue
        if not isinstance(parsed, dict):
            continue
        message = cast(dict[str, Any], parsed)
        if not initialized and is_hook_response(message):
            hooks.append(message)
        elif message.get("type") == "system" and message.get("subtype") == "init":
            # init precedes the first model request, so a wrong account or model is stopped at session start.
            check_init(profile, message, check=check, hooks=hooks)
            initialized = True
        elif message.get("type") == "result":
            result = message
    returncode = await proc.wait()
    stderr = (await stderr_task).decode("utf-8", errors="replace").strip()
    who = f"{profile.harness} (launcher '{profile.launcher.name}')"
    if not initialized:
        raise ClaudeCodeError(f"{who} exited ({returncode}) before reporting its session: {stderr[-2000:]}",
                              returncode=returncode, output=f"{stderr}\n{plain}")
    return _checked_result(who, result, returncode, stderr, plain)


def _checked_result(
    who: str, result: dict[str, Any] | None, returncode: int | None, stderr: str, plain: str = ""
) -> dict[str, Any]:
    own = f"{stderr}\n{plain}"
    if result is None:
        raise ClaudeCodeError(f"{who} exited ({returncode}) without a result: {stderr[-2000:]}",
                              returncode=returncode, output=own)
    if result.get("is_error") or result.get("subtype") != "success":
        said = str(result.get("result", ""))
        raise ClaudeCodeError(f"{who} reported an error: {said[:2000]}", returncode=returncode, output=f"{said}\n{own}")
    return result


def print_result(profile: ClaudeCodeProfile, *, returncode: int | None, stdout: bytes, stderr: bytes) -> dict[str, Any]:
    # What `--output-format json` left: the result message, or with a user's verbose setting an array of every message.
    # A launcher may print lines of its own around it.
    text = stdout.decode("utf-8", errors="replace").strip()
    result: dict[str, Any] | None = None
    for candidate in (text, *reversed(text.splitlines())):
        try:
            parsed: object = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        messages = cast(list[object], parsed) if isinstance(parsed, list) else [parsed]
        found = [cast(dict[str, Any], m) for m in messages if isinstance(m, dict)]
        result = next((m for m in reversed(found) if m.get("type") == "result"), None)
        if result is not None:
            break
    return _checked_result(f"{profile.harness} (launcher '{profile.launcher.name}')", result, returncode,
                           stderr.decode("utf-8", errors="replace").strip())


def is_hook_response(message: Mapping[str, Any]) -> bool:
    return message.get("type") == "system" and message.get("subtype") == "hook_response"


def _reported_env(hooks: Iterable[Mapping[str, Any]]) -> dict[str, object] | None:
    for message in hooks:
        for line in str(message.get("stderr", "")).splitlines():
            if line.startswith(_ENV_CHECK_MARK):
                try:
                    parsed: object = json.loads(line.removeprefix(_ENV_CHECK_MARK))
                except json.JSONDecodeError:
                    continue
                if isinstance(parsed, dict):
                    return cast(dict[str, object], parsed)
    return None


def _check_effective_env(profile: ClaudeCodeProfile, check: EnvCheck, hooks: Iterable[Mapping[str, Any]]) -> None:
    launcher = profile.launcher.name
    reported = _reported_env(hooks)
    if reported is None:
        raise ClaudeCodeError(
            f"billing is 'subscription' but Claude Code (launcher '{launcher}') did not report the values it started "
            f"with, so Hibernaut could not confirm {', '.join(check.keys)}; stopped at session start. Its SessionStart "
            "hooks may be disabled"
        )
    configured = configured_env(profile)
    changed = [k for k in check.keys if reported.get(k) != env_fingerprint(check.salt, configured[k])]
    if changed:
        raise ClaudeCodeError(
            f"billing is 'subscription' but Claude Code (launcher '{launcher}') started with {', '.join(changed)} "
            "not set to the configured value (the launcher or a settings file replaced or removed it); stopped at session "
            "start. Remove that override or set billing = \"api\""
        )


def check_init(
    profile: ClaudeCodeProfile,
    init: Mapping[str, Any],
    *,
    check: EnvCheck | None = None,
    hooks: Iterable[Mapping[str, Any]] = (),
) -> None:
    source = str(init.get("apiKeySource", ""))
    launcher = profile.launcher.name
    if profile.billing == "subscription" and check is not None:
        _check_effective_env(profile, check, hooks)
    # Claude Code names the variable it authenticates with; one the deployment file sets is the operator's choice,
    # and only once the check above confirmed the session holds the configured value.
    if profile.billing == "subscription" and source != "none" and (check is None or source not in check.keys):
        raise ClaudeCodeError(
            f"billing is 'subscription' but Claude Code (launcher '{launcher}') would authenticate with "
            f"'{source}', which the launcher and executor env do not set or Hibernaut could not confirm; stopped at "
            "session start. Remove that credential source, set it in the executor's env or set billing = \"api\""
        )
    if profile.billing == "api" and source != "ANTHROPIC_API_KEY":
        raise ClaudeCodeError(
            f"billing is 'api' but Claude Code (launcher '{launcher}') authenticated with '{source or 'none'}' "
            "instead of the credential proxy route; stopped at session start"
        )
    actual = str(init.get("model", ""))
    if profile.model.startswith("claude-") and actual.removesuffix("[1m]") != profile.model.removesuffix("[1m]"):
        raise ClaudeCodeError(f"configured model '{profile.model}' but Claude Code selected '{actual}'")
