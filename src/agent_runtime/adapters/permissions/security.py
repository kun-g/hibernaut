from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from agent_runtime.core.models import PermissionDecision


class AutoModePermissionAdapter:
    DEFAULT_ALLOW_TOOLS: frozenset[str] = frozenset(
        {
            "read_memory",
            "write_memory",
            "schedule_timer",
            "send_message",
            "fetch_status",
            "orca.agent.launch",
            "coder_reviewer.launch",
            "sde.launch",
        }
    )
    DEFAULT_DEFER_TOOLS: frozenset[str] = frozenset(
        {
            "send_email",
            "deploy_production",
            "transfer_funds",
            "delete_resource",
        }
    )
    DEFAULT_BLOCK_TOOLS: frozenset[str] = frozenset(
        {
            "exec_shell_raw",
            "dump_env_secrets",
        }
    )
    DANGEROUS_SUBSTRINGS: tuple[str, ...] = (
        "rm -rf",
        "DROP TABLE",
        "curl | sh",
    )

    def __init__(
        self,
        *,
        allow_tools: frozenset[str] | None = None,
        defer_tools: frozenset[str] | None = None,
        block_tools: frozenset[str] | None = None,
    ) -> None:
        self._allow_tools = allow_tools if allow_tools is not None else self.DEFAULT_ALLOW_TOOLS
        self._defer_tools = defer_tools if defer_tools is not None else self.DEFAULT_DEFER_TOOLS
        self._block_tools = block_tools if block_tools is not None else self.DEFAULT_BLOCK_TOOLS

    def check(self, tool_name: str, args: dict[str, Any]) -> PermissionDecision:
        if tool_name in self._block_tools:
            return PermissionDecision(
                action="block",
                reason=f"Tool '{tool_name}' is blocked by security policy.",
            )

        serialized_args = json.dumps(args, ensure_ascii=False)
        for pattern in self.DANGEROUS_SUBSTRINGS:
            if pattern.lower() in serialized_args.lower():
                return PermissionDecision(
                    action="block",
                    reason=f"Dangerous argument pattern detected: '{pattern}'.",
                )

        if tool_name in self._defer_tools:
            return PermissionDecision(
                action="defer",
                reason=f"High-impact tool '{tool_name}' requires human approval.",
            )

        if tool_name in self._allow_tools:
            return PermissionDecision(action="allow", reason="Explicitly allowed tool.")

        return PermissionDecision(
            action="defer",
            reason=f"Unknown tool '{tool_name}' defaults to human approval.",
        )


def read_private_token(path: Path) -> str:
    try:
        mode = path.stat().st_mode
        token = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ValueError(f"proxy token file {path} cannot be read ({exc.strerror})") from None
    if mode & 0o077:
        raise ValueError(f"proxy token file {path} is readable by group or others; run chmod 600 on it")
    if not token:
        raise ValueError(f"proxy token file {path} is empty")
    return token


def proxy_service_url(proxy_base_url: str, service_name: str) -> str:
    return f"{proxy_base_url.rstrip('/')}/proxy/{service_name}"


@dataclass(frozen=True, slots=True)
class ProxyResponse:
    status_code: int
    body: str


class CredentialProxyClient:
    # Agent runtime holds zero real secrets; external proxy injects credentials per service_name.
    # `token` only authenticates this process to the local proxy; it grants nothing upstream by itself.
    def __init__(self, proxy_base_url: str, timeout_seconds: float = 15.0, *, token: str | None = None) -> None:
        self._proxy_base_url = proxy_base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._token = token

    def call_service(
        self,
        *,
        service_name: str,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> ProxyResponse:
        url = f"{proxy_service_url(self._proxy_base_url, service_name)}/{path.lstrip('/')}"
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        headers = {"Content-Type": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        req = urllib.request.Request(url=url, data=data, method=method.upper(), headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self._timeout_seconds) as resp:  # noqa: S310
                return ProxyResponse(status_code=int(resp.status), body=resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            # A refusal from the proxy is a business result for the caller, not a transport failure.
            with exc:
                return ProxyResponse(status_code=int(exc.code), body=exc.read().decode("utf-8", "replace"))
