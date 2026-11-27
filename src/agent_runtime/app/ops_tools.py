from __future__ import annotations

import json
import os
import re
import subprocess
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, cast

from agent_runtime.adapters.delivery.pr_delivery import remote_env
from agent_runtime.adapters.executor_cli.steward_git import ToolError
from agent_runtime.adapters.executor_cli.steward_tools import PlannedCall
from agent_runtime.app.onboarding import scrub, run_command

# What the gate is asked for each kind of call. Staging work goes ahead; a deployment of anything that is not staging
# (production, or an environment that cannot be told) waits for a person, so does deleting, and housekeeping of the
# whole server is not on the default allow-list either.
READ, CHANGE, DEPLOY = "dokploy.read", "dokploy.change", "dokploy.deploy"
DEPLOY_PRODUCTION, DELETE, ADMIN = "deploy_production", "delete_resource", "dokploy.admin"

_ID = re.compile(r"[A-Za-z0-9_-]{1,100}")
_ENDPOINT = re.compile(r"[A-Za-z]+\.[A-Za-z]+")
_QUERY = re.compile(r"\?[A-Za-z0-9_=&.-]{0,500}")
_HOST = re.compile(r"[A-Za-z0-9.-]{1,253}")
_ENV_LINE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*=[^\n\"\\]*")
_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9 ._-]{0,99}")
_PORT = re.compile(r"\d{1,5}")

# action -> (kind, what each argument is). "id" is a Dokploy ID; a deployment's target is its first argument.
_ACTIONS: dict[str, tuple[str, tuple[str, ...]]] = {
    "projects": (READ, ()), "project": (READ, ("id",)), "app": (READ, ("id",)), "compose": (READ, ("id",)),
    "app-env": (READ, ("id",)), "domains": (READ, ("id",)), "deployments": (READ, ("id",)),
    "deployments-compose": (READ, ("id",)), "containers": (READ, ("id?",)), "health": (READ, ()),
    "version": (READ, ()),
    "project-create": (CHANGE, ("name", "name?")), "app-set-env": (CHANGE, ("id", "env")),
    "domain-create": (CHANGE, ("id", "host", "port?", "bool?")), "app-stop": (CHANGE, ("id",)),
    "compose-stop": (CHANGE, ("id",)),
    "app-deploy": (DEPLOY, ("id",)), "app-redeploy": (DEPLOY, ("id",)), "app-start": (DEPLOY, ("id",)),
    "compose-deploy": (DEPLOY, ("id",)), "compose-redeploy": (DEPLOY, ("id",)), "compose-start": (DEPLOY, ("id",)),
    "restart-container": (ADMIN, ("id", "id?")), "clean-images": (ADMIN, ()), "clean-volumes": (ADMIN, ()),
    "clean-prune": (ADMIN, ()), "reload-traefik": (ADMIN, ()),
    "get": (READ, ("endpoint", "query?")), "post": (CHANGE, ("endpoint", "json")),
}
# Services a raw post may reach; anything else is housekeeping of the server and waits for a person by default.
_POST_SERVICES = frozenset({"project", "environment", "application", "compose", "domain", "port", "mounts", "redirects",
                            "security", "postgres", "mysql", "mariadb", "mongo", "redis"})
# Services that run in one environment, with the key that names one.
_SERVICES = {"application": "applicationId", "compose": "composeId", "postgres": "postgresId", "mysql": "mysqlId",
             "mariadb": "mariadbId", "mongo": "mongoId", "redis": "redisId"}
# Words of an endpoint that puts a version live, or another one in its place.
_GOES_LIVE = ("deploy", "start", "reload", "rollback", "duplicate", "import", "restore")


def _check(kind: str, value: str, action: str) -> None:
    pattern = {"id": _ID, "name": _NAME, "env": _ENV_LINE, "host": _HOST, "port": _PORT, "endpoint": _ENDPOINT,
               "query": _QUERY}.get(kind)
    if kind == "env":
        # One KEY=value per line; quotes and backslashes would break the JSON the script builds around it.
        if not all(_ENV_LINE.fullmatch(line) for line in value.split("\n") if line):
            raise ToolError(f"{action}: env must be KEY=value lines without quotes or backslashes")
        return
    if kind == "bool":
        if value not in ("true", "false"):
            raise ToolError(f"{action}: expected true or false, got {value!r}")
        return
    if kind == "json":
        return
    if pattern is None or not pattern.fullmatch(value):
        raise ToolError(f"{action}: {value!r} is not a valid {kind}")


def _target(action: str, args: Sequence[str]) -> tuple[str, str] | None:
    # (service, id) a call deploys or changes, for the environment it belongs to.
    if action.startswith("app-"):
        return "application", args[0]
    if action.startswith("compose-"):
        return "compose", args[0]
    return None


class DokployTools:
    # The operations agent's commands: the workspace's Dokploy script, run in the agent's directory. The script reads its
    # token from the keychain and redacts what it prints; --reveal is never passed. Each call is planned first, so the
    # gate judges what it really does.
    def __init__(
        self,
        *,
        script: str,
        workdir: str,
        staging: Sequence[str],
        timeout: float,
        # Whether the configuration in force still gives this inbox the tools.
        allowed: Callable[[], bool],
        run: Callable[[Sequence[str], str], subprocess.CompletedProcess[str]] | None = None,
        scrubbed: Callable[[str], str] = scrub,
    ) -> None:
        self._script = script
        self._workdir = workdir
        self._staging = frozenset(staging)
        self._allowed = allowed
        self._scrubbed = scrubbed
        self._run: Callable[[Sequence[str], str], subprocess.CompletedProcess[str]] = run or (
            lambda argv, cwd: run_command(argv, _environment(), timeout=timeout, cwd=cwd))

    def plan(self, action: str, args: Sequence[str]) -> PlannedCall:
        if not self._allowed():
            raise ToolError("this inbox has no operations tools any more")
        if "--reveal" in (action, *args):
            raise ToolError("--reveal is not available: values stay redacted")
        known = _ACTIONS.get(action)
        if known is None:
            raise ToolError(f"dokploy: unknown action {action!r}; actions: {', '.join(sorted(_ACTIONS))}")
        kind, shape = known
        required = [s for s in shape if not s.endswith("?")]
        if not len(required) <= len(args) <= len(shape):
            raise ToolError(f"dokploy {action} takes {', '.join(shape) or 'no arguments'}")
        for value, expected in zip(args, shape, strict=False):
            _check(expected.rstrip("?"), value, action)
        argv = list(args)
        environment: str | None = None
        if action == "post":
            kind, argv, environment = self._plan_post(args[0], args[1])
        elif kind == DEPLOY and (target := _target(action, args)) is not None:
            environment = self._environment(*target)
            if environment not in self._staging:
                kind = DEPLOY_PRODUCTION
        shown = {"action": action, "args": argv, "environment": environment}
        where = f" (environment {environment or 'unknown, so production'})" if kind in (DEPLOY, DEPLOY_PRODUCTION) else ""
        return PlannedCall(tool=kind, args=shown, summary=f"dokploy {' '.join([action, *argv])}{where}")

    def _plan_post(self, endpoint: str, raw: str) -> tuple[str, list[str], str | None]:
        try:
            body: object = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ToolError(f"post {endpoint}: the body must be a JSON object ({exc})") from exc
        if not isinstance(body, dict):
            raise ToolError(f"post {endpoint}: the body must be a JSON object")
        data = cast(dict[str, Any], body)
        service, verb = endpoint.split(".", 1)
        argv = [endpoint, json.dumps(data, ensure_ascii=False, separators=(",", ":"))]
        lowered = verb.lower()
        if "remove" in lowered or "delete" in lowered:
            return DELETE, argv, None
        if service not in _POST_SERVICES:
            return ADMIN, argv, None
        # A source saved without autoDeploy = false deploys on the next push, which is going live as much as a deploy.
        source = lowered.startswith("save") and lowered.endswith("provider") and data.get("autoDeploy") is not False
        if not (any(word in lowered for word in _GOES_LIVE) or data.get("autoDeploy") is True or source):
            return CHANGE, argv, None
        key = _SERVICES.get(service)
        target = data.get(key) if key is not None else None
        if key is None or not isinstance(target, str) or not _ID.fullmatch(target):
            # What cannot be placed in one environment counts as production: a person decides.
            return DEPLOY_PRODUCTION, argv, None
        environment = self._environment(service, target)
        return (DEPLOY if environment in self._staging else DEPLOY_PRODUCTION), argv, environment

    def _environment(self, service: str, target: str) -> str | None:
        # The name of the environment a service is in, read from Dokploy; None when it cannot be told.
        found = self._get(f"{service}.one", f"?{_SERVICES[service]}={target}")
        if found is None:
            return None
        nested = found.get("environment")
        if isinstance(nested, dict) and isinstance(cast(dict[str, Any], nested).get("name"), str):
            return cast(str, cast(dict[str, Any], nested)["name"])
        environment_id = found.get("environmentId")
        if not isinstance(environment_id, str) or not _ID.fullmatch(environment_id):
            return None
        environment = self._get("environment.one", f"?environmentId={environment_id}")
        name = environment.get("name") if environment is not None else None
        return name if isinstance(name, str) else None

    def _get(self, endpoint: str, query: str) -> dict[str, Any] | None:
        done = self._run(["bash", self._script, "get", endpoint, query], self._workdir)
        if done.returncode != 0:
            return None
        try:
            found: object = json.loads(done.stdout)
        except json.JSONDecodeError:
            return None
        return cast(dict[str, Any], found) if isinstance(found, dict) else None

    def run(self, call: PlannedCall) -> str:
        if not self._allowed():
            raise ToolError("this inbox has no operations tools any more")
        action, args = str(call.args["action"]), [str(a) for a in call.args["args"]]
        if not Path(self._script).is_file():
            raise ToolError(f"the Dokploy script {self._script} is not there")
        done = self._run(["bash", self._script, action, *args], self._workdir)
        out = self._scrubbed((done.stdout or "").strip())
        if done.returncode != 0:
            err = self._scrubbed(" ".join((done.stderr or "").split())[:500])
            raise ToolError(f"dokploy {action} failed (exit {done.returncode}): {err or out[:500]}")
        return out or f"dokploy {action}: done"


def _environment() -> dict[str, str]:
    # The script falls back to the keychain; a file of settings would be read in place of it.
    env = remote_env(os.environ)
    env.pop("DOKPLOY_ENV_FILE", None)
    return env
