from __future__ import annotations

import asyncio
import json
import subprocess
import tempfile
import unittest
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from agent_runtime.adapters.executor_cli.steward_git import ToolError
from agent_runtime.adapters.executor_cli.steward_tools import HELD_DOKPLOY, StewardTools
from agent_runtime.adapters.permissions import AutoModePermissionAdapter
from agent_runtime.app.ops_tools import DEPLOY, DEPLOY_PRODUCTION, DokployTools
from tests.support.new_project import done


class FakeDokploy:
    # The Dokploy script, in this process: application.one and environment.one answer from `environments`.
    def __init__(self, environments: dict[str, str | None]) -> None:
        self.environments = environments
        self.calls: list[list[str]] = []

    def __call__(self, argv: Sequence[str], cwd: str) -> subprocess.CompletedProcess[str]:
        args = list(argv[2:])
        self.calls.append(args)
        if args[0] == "get":
            service, query = args[1], args[2]
            target = query.split("=", 1)[1]
            if service == "environment.one":
                return done(argv, out=json.dumps({"name": target.removeprefix("env-")}))
            environment = self.environments.get(target)
            if environment is None:
                return done(argv, 22, err="curl: (22) The requested URL returned error: 404")
            return done(argv, out=json.dumps({"environmentId": f"env-{environment}"}))
        if args[:2] == ["app-deploy", "broken"]:
            return done(argv, 1, err="deploy failed: build error")
        return done(argv, out=f"ran {' '.join(args)}")


def tools(fake: FakeDokploy, script: Path, allowed: bool = True) -> DokployTools:
    return DokployTools(script=str(script), workdir=str(script.parent), staging=("staging",), timeout=5,
                        allowed=lambda: allowed, run=fake)


class PlanTests(unittest.TestCase):
    # Which gate each Dokploy call passes: production waits for a person, whatever way it is asked for.
    def setUp(self) -> None:
        self.dir = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.script = self.dir / "dokploy-api.sh"
        self.script.write_text("#!/bin/sh\n")
        self.fake = FakeDokploy({"app-stg": "staging", "app-prod": "production", "cmp-stg": "staging"})
        self.ops = tools(self.fake, self.script)
        self.gate = AutoModePermissionAdapter()

    def kind(self, action: str, *args: str) -> str:
        return self.ops.plan(action, list(args)).tool

    def test_staging_deploys_and_production_waits(self) -> None:
        self.assertEqual(self.kind("app-deploy", "app-stg"), DEPLOY)
        self.assertEqual(self.gate.check(DEPLOY, {}).action, "allow")
        for action in ("app-deploy", "app-redeploy", "app-start"):
            self.assertEqual(self.kind(action, "app-prod"), DEPLOY_PRODUCTION)
        self.assertEqual(self.gate.check(DEPLOY_PRODUCTION, {}).action, "defer")
        self.assertEqual(self.kind("compose-deploy", "cmp-stg"), DEPLOY)

    def test_an_environment_that_cannot_be_told_counts_as_production(self) -> None:
        self.assertEqual(self.kind("app-deploy", "app-unknown"), DEPLOY_PRODUCTION)
        planned = self.ops.plan("app-deploy", ["app-unknown"])
        self.assertIn("unknown, so production", planned.summary)

    def test_a_raw_post_that_goes_live_is_judged_the_same(self) -> None:
        def post(endpoint: str, body: dict[str, Any]) -> str:
            return self.kind("post", endpoint, json.dumps(body))

        self.assertEqual(post("application.deploy", {"applicationId": "app-prod"}), DEPLOY_PRODUCTION)
        self.assertEqual(post("application.deploy", {"applicationId": "app-stg"}), DEPLOY)
        # A source that deploys on every push puts production live as much as a deploy does.
        self.assertEqual(post("application.saveGithubProvider", {"applicationId": "app-prod"}), DEPLOY_PRODUCTION)
        self.assertEqual(post("application.saveGithubProvider", {"applicationId": "app-prod", "autoDeploy": False}),
                         "dokploy.change")
        self.assertEqual(post("application.update", {"applicationId": "app-prod", "autoDeploy": True}),
                         DEPLOY_PRODUCTION)
        self.assertEqual(post("application.rollback", {"applicationId": "app-prod"}), DEPLOY_PRODUCTION)
        self.assertEqual(post("postgres.deploy", {"postgresId": "db-1"}), DEPLOY_PRODUCTION)
        self.assertEqual(post("compose.deploy", {}), DEPLOY_PRODUCTION)
        self.assertEqual(post("application.create", {"name": "web", "environmentId": "env-1"}), "dokploy.change")
        self.assertEqual(post("environment.create", {"projectId": "p", "name": "staging"}), "dokploy.change")
        self.assertEqual(post("project.remove", {"projectId": "p"}), "delete_resource")
        self.assertEqual(self.gate.check("delete_resource", {}).action, "defer")
        self.assertEqual(post("settings.cleanAll", {}), "dokploy.admin")
        self.assertEqual(self.gate.check("dokploy.admin", {}).action, "defer")

    def test_reads_and_staging_changes_go_ahead(self) -> None:
        for action, args in (("projects", ()), ("project", ("p1",)), ("deployments", ("app-stg",)),
                             ("project-create", ("aura-desk",)), ("domain-create", ("app-stg", "stg.example.com"))):
            kind = self.kind(action, *args)
            self.assertEqual(self.gate.check(kind, {}).action, "allow", (action, kind))

    def test_what_the_script_cannot_take_safely_is_refused(self) -> None:
        for action, args in (("app-deploy", ["--reveal"]), ("projects", ["--reveal"]), ("app-deploy", ['x"}']),
                             ("app-set-env", ["app-stg", 'A="b"']), ("exec", []), ("app-deploy", []),
                             ("post", ["application.deploy", "[1]"]), ("get", ["../x"])):
            with self.assertRaises(ToolError, msg=(action, args)):
                self.ops.plan(action, args)

    def test_run_passes_the_arguments_and_reports_a_failure(self) -> None:
        self.assertIn("ran projects", self.ops.run(self.ops.plan("projects", [])))
        self.fake.environments["broken"] = "staging"
        with self.assertRaises(ToolError) as caught:
            self.ops.run(self.ops.plan("app-deploy", ["broken"]))
        self.assertIn("build error", str(caught.exception))
        with self.assertRaises(ToolError):
            tools(self.fake, self.script, allowed=False).plan("projects", [])


class _Caps:
    def __init__(self) -> None:
        self.permissions = AutoModePermissionAdapter()


class HeldDeployTests(unittest.TestCase):
    # In a Steward run: a production deployment is held, never made, until a person approves it.
    def setUp(self) -> None:
        self.dir = Path(self.enterContext(tempfile.TemporaryDirectory()))
        script = self.dir / "dokploy-api.sh"
        script.write_text("#!/bin/sh\n")
        self.fake = FakeDokploy({"app-stg": "staging", "app-prod": "production"})
        self.tools = StewardTools(caps=_Caps(), checkout=self.dir, inbox_id="sys:infra", sessions=None,  # type: ignore[arg-type]
                                  events=None, in_project=lambda _i: False, projectless=True,  # type: ignore[arg-type]
                                  ops=tools(self.fake, script))

    def call(self, *args: str) -> tuple[str, bool]:
        return asyncio.run(self.tools.call("dokploy", {"action": args[0], "args": list(args[1:])}))

    def test_production_is_held_and_made_only_once_approved(self) -> None:
        said, failed = self.call("app-deploy", "app-stg")
        self.assertFalse(failed, said)
        said, failed = self.call("app-deploy", "app-prod")
        self.assertFalse(failed)
        self.assertIn("Held for a person's approval", said)
        deploys = [c for c in self.fake.calls if c[0] == "app-deploy"]
        self.assertEqual(deploys, [["app-deploy", "app-stg"]])
        [held] = self.tools.deferred
        self.assertEqual(held["kind"], HELD_DOKPLOY)
        self.assertIn("environment production", held["summary"])
        self.assertTrue(self.tools.run_held(held, approved=False).startswith("declined"))
        self.assertEqual([c for c in self.fake.calls if c[0] == "app-deploy"], deploys)
        made = self.tools.run_held(held, approved=True)
        self.assertTrue(made.startswith("approved and made"), made)
        self.assertEqual([c for c in self.fake.calls if c[0] == "app-deploy"][-1], ["app-deploy", "app-prod"])

    def test_the_tool_list_has_dokploy_and_no_forward_nor_shell(self) -> None:
        names = [t["name"] for t in self.tools.offered()]
        self.assertIn("dokploy", names)
        self.assertNotIn("forward_to_inbox", names)
        self.assertNotIn("create_repo", names)
        said, failed = asyncio.run(self.tools.call("forward_to_inbox", {"target_inbox": "user:main", "text": "x"}))
        self.assertTrue(failed)
        self.assertEqual(said, "unknown tool forward_to_inbox")


if __name__ == "__main__":
    unittest.main()
