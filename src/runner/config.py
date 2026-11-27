from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final, cast
from collections.abc import Mapping

from runner.protocol import InteractiveSurfaceKind, WorktreeBackendKind

_PREFIX_RE: Final = re.compile(r"^[a-z][a-z0-9_-]{0,15}$")


def _empty_labels() -> dict[str, str]:
    return {}


@dataclass(frozen=True, slots=True)
class NamespacePolicy:
    namespace: str
    prefix: str
    allowed_callers: tuple[str, ...]

    def __post_init__(self) -> None:
        if not _PREFIX_RE.match(self.namespace) or not _PREFIX_RE.match(self.prefix):
            raise ValueError(f"invalid namespace/prefix: {self.namespace!r}/{self.prefix!r}")
        if not self.allowed_callers:
            raise ValueError(f"namespace {self.namespace!r} must declare at least one allowed_caller")

    def allows(self, caller_id: str) -> bool:
        return caller_id in self.allowed_callers


@dataclass(frozen=True, slots=True)
class ProjectCheckout:
    project_key: str
    checkout_path: Path
    default_base_ref: str = "origin/main"


@dataclass(frozen=True, slots=True)
class RunnerConfig:
    runner_id: str
    max_concurrent: int
    worktree_backend: WorktreeBackendKind
    interactive_surface: InteractiveSurfaceKind
    namespaces: Mapping[str, NamespacePolicy]
    projects: Mapping[str, ProjectCheckout]
    state_dir: Path
    logs_dir: Path
    labels: dict[str, str] = field(default_factory=_empty_labels)
    # Refuse every run that asks for a writable workspace.
    readonly_only: bool = False

    def __post_init__(self) -> None:
        if not self.runner_id:
            raise ValueError("runner_id must be non-empty")
        if self.max_concurrent < 1:
            raise ValueError("max_concurrent must be >= 1")
        if self.worktree_backend not in ("orca", "git"):
            raise ValueError(f"unsupported worktree_backend: {self.worktree_backend}")
        if self.interactive_surface not in ("orca", "none"):
            raise ValueError(f"unsupported interactive_surface: {self.interactive_surface}")
        if not self.namespaces:
            raise ValueError("at least one namespace policy must be configured")

    def authorize_namespace(self, namespace: str, caller_id: str) -> NamespacePolicy:
        policy = self.namespaces.get(namespace)
        if policy is None:
            raise PermissionError(f"namespace {namespace!r} is not whitelisted on runner {self.runner_id!r}")
        if not policy.allows(caller_id):
            raise PermissionError(
                f"caller {caller_id!r} is not authorized for namespace {namespace!r} on runner {self.runner_id!r}"
            )
        return policy


def parse_runner_config(raw: Mapping[str, Any], *, base_dir: Path) -> RunnerConfig:
    runner_sec = cast(Mapping[str, Any], raw.get("runner", {}))
    runner_id = str(runner_sec.get("runner_id", "")).strip()
    max_concurrent = int(runner_sec.get("max_concurrent", 2))
    worktree_backend = cast(WorktreeBackendKind, str(runner_sec.get("worktree_backend", "git")))
    interactive_surface = cast(
        InteractiveSurfaceKind,
        str(runner_sec.get("interactive_surface", "orca" if worktree_backend == "orca" else "none")),
    )
    state_dir = _resolve_path(str(runner_sec.get("state_dir", ".runner_state")), base_dir)
    logs_dir = _resolve_path(str(runner_sec.get("logs_dir", "logs/runs")), base_dir)
    labels_raw = cast(Mapping[str, Any], runner_sec.get("labels", {}))
    labels = {str(k): str(v) for k, v in labels_raw.items()}
    readonly_only: Any = runner_sec.get("readonly_only", False)
    if not isinstance(readonly_only, bool):
        raise ValueError("runner.readonly_only must be true or false")

    ns_section = cast(Mapping[str, Any], raw.get("namespaces", {}))
    namespaces: dict[str, NamespacePolicy] = {}
    for ns_name, ns_cfg in ns_section.items():
        if not isinstance(ns_cfg, Mapping):
            raise ValueError(f"namespaces.{ns_name} must be a table")
        cfg_map = cast(Mapping[str, Any], ns_cfg)
        prefix = str(cfg_map.get("prefix", ns_name))
        callers_raw = cfg_map.get("allowed_callers")
        if callers_raw is None and "allowed_caller" in cfg_map:
            callers_raw = [cfg_map["allowed_caller"]]
        if not isinstance(callers_raw, list) or not callers_raw:
            raise ValueError(f"namespaces.{ns_name} requires allowed_caller or allowed_callers")
        callers = tuple(str(c) for c in cast(list[Any], callers_raw))
        namespaces[ns_name] = NamespacePolicy(namespace=ns_name, prefix=prefix, allowed_callers=callers)

    proj_section = cast(Mapping[str, Any], raw.get("projects", {}))
    projects: dict[str, ProjectCheckout] = {}
    for proj_key, proj_cfg in proj_section.items():
        if not isinstance(proj_cfg, Mapping):
            raise ValueError(f"projects.{proj_key} must be a table")
        pmap = cast(Mapping[str, Any], proj_cfg)
        checkout_raw = str(pmap.get("checkout_path", "")).strip()
        if not checkout_raw:
            raise ValueError(f"projects.{proj_key}.checkout_path is required")
        projects[proj_key] = ProjectCheckout(
            project_key=proj_key,
            checkout_path=_resolve_path(checkout_raw, base_dir),
            default_base_ref=str(pmap.get("default_base_ref", "origin/main")),
        )

    return RunnerConfig(
        runner_id=runner_id,
        max_concurrent=max_concurrent,
        worktree_backend=worktree_backend,
        interactive_surface=interactive_surface,
        namespaces=namespaces,
        projects=projects,
        state_dir=state_dir,
        logs_dir=logs_dir,
        labels=labels,
        readonly_only=readonly_only,
    )


def load_runner_config(path: Path) -> RunnerConfig:
    resolved = path.expanduser().resolve()
    with resolved.open("rb") as f:
        raw = tomllib.load(f)
    return parse_runner_config(raw, base_dir=resolved.parent)


def _resolve_path(raw: str, base_dir: Path) -> Path:
    p = Path(raw).expanduser()
    return p if p.is_absolute() else (base_dir / p).resolve()
