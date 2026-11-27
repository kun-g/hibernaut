from __future__ import annotations

import json
import os
import subprocess
import threading
from pathlib import Path
from typing import Any
from unittest.mock import patch

from agent_runtime.adapters.credential_proxy.gdrive_queue import DriveQueueGuard, build_proxy_server
from agent_runtime.app.config import load_config
from agent_runtime.core.models import FORWARD_SOURCE, RETURN_SOURCE
from tests.integration.test_system_steward_e2e import ServedCase, tool
from tests.support.coder_reviewer import fake_command
from tests.support.global_steward import forward, reply
from tests.support.new_project import SOURCES, MemoryDrive, dokploy_calls, dokploy_script, environments, zipped
from tests.support.steward import arg

ASH, OPS, INFRA = "user:main", "sys:ops", "sys:infra"
CLIENT_TOKEN = "local-client-token-for-new-projects"
CONFIG = """# the deployment
[channels]
host = "127.0.0.1"
port = {port}

[runtime]
db_path = "{root}/x.db"
memory_root = "{root}/memory"
default_agent = "ash"
worktree_sweep_interval_seconds = 0

[[runtime.inbox_bindings]]
pattern = "user:main"
default_agent = "ash"
agents = ["ash"]
workdir = "{root}/workspace"
checkout_access = "readonly"
forward_to = ["sys:ops", "sys:infra"]
repo_tools = true

[[runtime.inbox_bindings]]
pattern = "sys:ops"
default_agent = "hibernaut-ops"
agents = ["hibernaut-ops"]
workdir = "{home}"
checkout_access = "readonly"
system_tools = true

[[runtime.inbox_bindings]]
pattern = "sys:infra"
default_agent = "infra"
agents = ["infra"]
workdir = "{root}/workspace"
checkout_access = "readonly"
ops_tools = true

[repo_tools]
owner = "acme"
allowed_owners = ["acme", "LambdaTheory"]

[ops_tools]
dokploy_script = "{root}/bin/dokploy-api.sh"

[gdrive]
proxy_url = "{proxy}"
proxy_token_file = "{root}/proxy-token"

[onboarding]
label_owners = ["acme"]

[launchers.fake]
command = "{command}"

[executors.claude-code]
launcher = "fake"

[agents.ash]
role = "steward"
executor = "claude-code"

[agents.hibernaut-ops]
role = "steward"
executor = "claude-code"

[agents.infra]
role = "steward"
executor = "claude-code"
description = "运维：Dokploy 上的预发布和生产"

[agents.front]
role = "steward"
executor = "claude-code"

[agents.sde]
role = "sde"
executor = "claude-code"
review = false
interactive = false
"""

# gh and orca as the daemon finds them on PATH: GitHub holds bare repositories under state/github, created by gh repo
# create from the directory it is given and cloned from there.
COMMANDS = r'''
import json, os, subprocess, sys
from pathlib import Path
state = Path(os.environ["FAKE_COMMANDS"])
name, args = sys.argv[1], sys.argv[2:]
with open(state / "calls.jsonl", "a") as log:
    log.write(json.dumps([name, *args]) + "\n")
def flag(key):
    return args[args.index(key) + 1]
def run(*argv, **kw):
    return subprocess.run(argv, check=True, capture_output=True, text=True, **kw)
def bare(repo):
    return state / "github" / (repo + ".git")
registered = state / "registered"
known = registered.read_text().splitlines() if registered.exists() else []
if name == "gh":
    if args[0] == "api":
        if bare(args[1].removeprefix("repos/")).exists():
            sys.exit(0)
        print("gh: Not Found (HTTP 404)", file=sys.stderr)
        sys.exit(1)
    if args[:2] == ["repo", "create"]:
        assert "--private" in args and "--push" in args
        target = bare(args[2])
        target.parent.mkdir(parents=True, exist_ok=True)
        run("git", "clone", "-q", "--bare", flag("--source"), str(target))
        sys.exit(0)
    if args[:2] == ["repo", "clone"]:
        url = "https://github.com/" + args[2] + ".git"
        run("git", "clone", "-q", str(bare(args[2])), args[3])
        run("git", "-C", args[3], "config", "remote.origin.url", url)
        for key, value in (("user.email", "t@example.invalid"), ("user.name", "t")):
            run("git", "-C", args[3], "config", key, value)
    sys.exit(0)
if args[:2] == ["repo", "show"]:
    sys.exit(0 if flag("--repo")[5:] in known else 1)
if args[:2] == ["repo", "add"]:
    registered.write_text("\n".join([*known, flag("--path")]))
    sys.exit(0)
sys.exit(1)
'''
REQUEST = ("新建一个项目 aura-desk，从 gdrive 下载最新版本作为初始化提交，另外再把预发布环境和生产环境也配置好，"
           "这两个目前都先放在 Dokploy 上")
CREATE = tool("create_repo", name="aura-desk", description="桌面助手")
ONBOARD = tool("onboard_project", repo="acme/aura-desk", description="桌面助手")


def dokploy(action: str, *args: str) -> dict[str, Any]:
    return tool("dokploy", action=action, args=list(args))


def failed(message: str) -> dict[str, Any]:
    return {"output": {"action": "reply", "message": message, "status": "failed"}}


class NewProjectTests(ServedCase):
    # One sentence in the person's chat: the repository, onboarding and both environments, with GitHub, Drive and
    # Dokploy faked and everything of Hibernaut real.
    def setUp(self) -> None:
        super().setUp()
        (self.root / "bin" / "fake.py").write_text(COMMANDS)
        dokploy_script(self.root / "bin" / "dokploy-api.sh", self.state)
        environments(self.state, {"app-stg": "staging", "app-prod": "production"})
        self.drive = MemoryDrive()
        self.drive.folder(SOURCES, "版本", parent="my-drive")
        self.drive.folder("folder-aura", "aura-desk")
        self.drive.file("v1", "aura-desk-0.9.zip", zipped({"aura-desk/README.md": b"old"}), "folder-aura",
                        "2026-11-01T00:00:00Z")
        self.drive.file("v2", "aura-desk-1.0.zip", zipped({"aura-desk/README.md": b"# Aura Desk\n",
                                                           "aura-desk/app.py": b"print('hi')\n"}),
                        "folder-aura", "2026-11-26T00:00:00Z")
        proxy = build_proxy_server(DriveQueueGuard(self.drive, root_folder_id="root-q", sources_folder_id=SOURCES),
                                   host="127.0.0.1", port=0, service_name="gdrive-queue", client_token=CLIENT_TOKEN)
        threading.Thread(target=proxy.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True).start()
        self.addCleanup(proxy.server_close)
        self.addCleanup(proxy.shutdown)
        self.proxy_url = f"http://127.0.0.1:{proxy.server_address[1]}"
        token = self.root / "proxy-token"
        token.write_text(CLIENT_TOKEN)
        token.chmod(0o600)
        self.enterContext(patch.dict(os.environ, {
            "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.invalid",
            "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.invalid",
        }))

    def text(self) -> str:
        return CONFIG.format(root=self.root, home=self.home, command=fake_command(self.root), port=self.port,
                             proxy=self.proxy_url)

    def request_id(self) -> str:
        found = self.rows(f"SELECT pending_request_id FROM sessions WHERE inbox_id = '{INFRA}' "
                          "AND pending_request_id IS NOT NULL")
        return found[0][0] if found else ""

    def decide(self, approved: bool) -> None:
        self.daemon.handler.ingest(inbox_id=INFRA, source="sb:feishu", external_id="decided", payload={
            "event_type": "approval.decided", "approved": approved, "approval_request_id": self.request_id()})

    def pushed(self) -> dict[str, str]:
        repo = self.state / "github" / "acme" / "aura-desk.git"
        listed = subprocess.run(["git", "-C", str(repo), "ls-tree", "-r", "--name-only", "main"], check=True,
                                capture_output=True, text=True).stdout.split()
        return {path: subprocess.run(["git", "-C", str(repo), "show", f"main:{path}"], check=True, capture_output=True,
                                     text=True).stdout for path in listed}

    def deploys(self) -> list[list[str]]:
        return [c for c in dokploy_calls(self.state) if c[0] == "app-deploy"]

    async def test_one_sentence_gives_the_repository_the_onboarding_and_both_environments(self) -> None:
        await self.serve()
        self.set_plan([
            {"calls": [CREATE, forward(OPS, "「把 acme/aura-desk 接进来」项目是桌面助手", return_to=ASH)],
             **reply("① 仓库 acme/aura-desk 已建（私有）；② 已交给系统 Steward 接入")},
            {"calls": [ONBOARD], **reply("接好了：aura-desk，主入口 proj:aura-desk")},
            {"calls": [forward(INFRA, "为 acme/aura-desk 在 Dokploy 上建预发布和生产；生产首次上线前等 Kun 批准",
                               return_to=ASH)], **reply("② 已接入 hb；③ 已交给运维配环境")},
            {"calls": [dokploy("projects"), dokploy("app-deploy", "app-stg"), dokploy("app-deploy", "app-prod")],
             **reply("预发布已部署 https://stg.aura.example；生产已配置，等批准后上线 https://aura.example")},
            {"calls": [dokploy("deployments", "app-prod")], **reply("预发布 https://stg.aura.example 运行中；"
                                                                   "生产 https://aura.example 已上线")},
            reply("汇总：仓库 acme/aura-desk；hb 已接入 proj:aura-desk；预发布 https://stg.aura.example 运行中；"
                  "生产 https://aura.example 已上线"),
        ])
        await self.message(REQUEST)
        await self.wait_for(self.request_id, "the approval of production")
        # Staging went live on its own; production waits, and the person was told where things stand.
        self.assertEqual(self.deploys(), [["app-deploy", "app-stg"]])
        replies = self.replies()
        self.assertTrue(any("① 仓库" in r for r in replies), replies)
        self.assertTrue(any("② 已接入 hb" in r for r in replies), replies)
        [card] = self.rows(f"SELECT content FROM outbox WHERE inbox_id = '{INFRA}' AND content LIKE '%asks to make%'")
        self.assertIn("app-deploy app-prod (environment production)", card[0])
        self.assertIn("等批准后上线", card[0])

        self.decide(approved=True)
        await self.wait_for(lambda: any("汇总" in r for r in self.replies()), "the summary in the chat")
        self.assertEqual(self.deploys(), [["app-deploy", "app-stg"], ["app-deploy", "app-prod"]])
        # The order the person heard it in.
        said = self.replies()
        steps = [next(i for i, r in enumerate(said) if mark in r) for mark in ("① 仓库", "② 已接入 hb", "汇总")]
        self.assertEqual(steps, sorted(steps))

        # The repository holds the newest version, unpacked, and Hibernaut serves it as a project.
        self.assertEqual(self.pushed(), {"README.md": "# Aura Desk\n", "app.py": "print('hi')\n"})
        self.assertEqual(self.drive.downloads, ["v2"])
        project = load_config(self.path).projects["aura-desk"]
        self.assertEqual((project.repo, project.inbox_id), ("acme/aura-desk", "proj:aura-desk"))
        created = json.loads(self.answers()[0][1])
        self.assertEqual((created["repo"], created["first_version"]["file"]), ("acme/aura-desk", "aura-desk-1.0.zip"))
        self.assertEqual([e.inbox_id for e in self.events(FORWARD_SOURCE)], [OPS, INFRA])
        self.assertEqual([e.payload["forwarded_from"] for e in self.events(RETURN_SOURCE)], [OPS, INFRA])

        def launch(inbox: str) -> dict[str, Any]:
            return next(c for c in self.launches() if f"You work in `{inbox}`" in arg(c, "--append-system-prompt"))

        ash, ops, infra = launch(ASH), launch(OPS), launch(INFRA)
        self.assertIn("create_repo", arg(ash, "--allowedTools"))
        self.assertIn("`sys:infra`: agent `infra`. Operations", arg(ash, "--append-system-prompt"))
        for name in ("Bash", "dokploy", "onboard_project"):
            self.assertNotIn(name, arg(ash, "--allowedTools"))
        self.assertIn("Bash", arg(ash, "--disallowedTools"))
        self.assertIn("onboard_project", arg(ops, "--allowedTools"))
        self.assertIn("dokploy", arg(infra, "--allowedTools"))
        self.assertNotIn("forward_to_inbox", arg(infra, "--allowedTools"))
        self.assertIn("deployment in an environment that is not staging", arg(infra, "--append-system-prompt"))

    async def test_declined_production_stays_configured_and_not_deployed(self) -> None:
        await self.serve()
        self.set_plan([
            {"calls": [CREATE, forward(OPS, "把 acme/aura-desk 接进来", return_to=ASH)], **reply("① 仓库已建")},
            {"calls": [ONBOARD], **reply("接好了")},
            {"calls": [forward(INFRA, "建预发布和生产", return_to=ASH)], **reply("② 已接入")},
            {"calls": [dokploy("app-deploy", "app-stg"), dokploy("app-deploy", "app-prod")], **reply("等批准")},
            reply("生产未上线：被拒；预发布运行中"),
            reply("汇总：生产已配置未上线"),
        ])
        await self.message(REQUEST)
        await self.wait_for(self.request_id, "the approval of production")
        self.decide(approved=False)
        await self.wait_for(lambda: any("汇总" in r for r in self.replies()), "the summary")
        self.assertEqual(self.deploys(), [["app-deploy", "app-stg"]])

    async def test_an_existing_repository_stops_it_at_the_first_step(self) -> None:
        await self.serve()
        (self.state / "github" / "LambdaTheory" / "aura-desk.git").mkdir(parents=True)
        self.set_plan([{"calls": [tool("create_repo", name="aura-desk", owner="lambdatheory")],
                        **reply("停在第 1 步：LambdaTheory/aura-desk 已存在，没有覆盖")}])
        await self.message(REQUEST)
        await self.wait_for(lambda: any("停在第 1 步" in r for r in self.replies()), "the answer")
        refused, said = self.answers()[0]
        self.assertTrue(refused)
        self.assertIn("LambdaTheory/aura-desk exists on GitHub already", said)
        self.assertEqual(self.drive.downloads, [])
        self.assertEqual(self.events(FORWARD_SOURCE), [])

    async def test_a_failed_onboarding_stops_it_there_and_nothing_goes_on_to_the_environments(self) -> None:
        await self.serve()
        self.set_plan([
            {"calls": [CREATE, forward(OPS, "把 acme/aura-desk 接进来", return_to=ASH)], **reply("① 仓库已建")},
            {"calls": [tool("onboard_project", repo="acme/elsewhere")], **failed("停在接入：acme/elsewhere 连不上")},
            # Whatever the chat's Steward then tries, the failed result carries no request on.
            {"calls": [forward(INFRA, "建预发布和生产", return_to=ASH)], **reply("停在第 2 步：接入 hb 失败")},
        ])
        await self.message(REQUEST)
        await self.wait_for(lambda: any("停在第 2 步" in r for r in self.replies()), "the answer")
        [back] = self.events(RETURN_SOURCE)
        self.assertEqual((back.payload["forwarded_from"], back.payload["status"]), (OPS, "failed"))
        refused, said = self.answers()[-1]
        self.assertTrue(refused)
        self.assertIn("takes only what a person asks for", said)
        self.assertEqual([e.inbox_id for e in self.events(FORWARD_SOURCE)], [OPS])
        self.assertEqual(dokploy_calls(self.state), [])

    async def test_a_failed_staging_deployment_is_reported_as_the_step_it_stopped_at(self) -> None:
        await self.serve()
        environments(self.state, {"app-broken": "staging", "app-prod": "production"})
        self.set_plan([
            {"calls": [CREATE, forward(OPS, "把 acme/aura-desk 接进来", return_to=ASH)], **reply("① 仓库已建")},
            {"calls": [ONBOARD], **reply("接好了")},
            {"calls": [forward(INFRA, "建预发布和生产", return_to=ASH)], **reply("② 已接入")},
            {"calls": [dokploy("app-deploy", "app-broken")], **failed("停在第 6 步：预发布部署失败 build error")},
            reply("停在第 3 步（配环境）：预发布部署失败"),
        ])
        await self.message(REQUEST)
        await self.wait_for(lambda: any("停在第 3 步" in r for r in self.replies()), "the answer")
        refused, said = self.answers()[-1]
        self.assertTrue(refused)
        self.assertIn("build error", said)
        self.assertEqual(self.request_id(), "")
        self.assertEqual(self.events(RETURN_SOURCE)[-1].payload["status"], "failed")

    async def test_the_operations_inbox_takes_nothing_but_a_person_s_work(self) -> None:
        await self.serve()
        self.set_plan([{"calls": [forward(INFRA, "deploy production")]}])
        for number, source in enumerate(("sb:github", "timer:daily")):
            self.daemon.handler.ingest(inbox_id=ASH, source=source, external_id=source, payload={"text": "go"},
                                       correlation_key=f"other:{number}")
            self.daemon.handler.ingest(inbox_id=INFRA, source=source, external_id=source, payload={"text": "go"})
        await self.wait_for(lambda: len(self.answers()) == 2, "the runs")
        for refused, said in self.answers():
            self.assertTrue(refused)
            self.assertIn("takes only what a person asks for", said)

        def turned_away() -> bool:
            found = self.rows(f"SELECT status FROM event_inbox WHERE inbox_id = '{INFRA}'")
            return len(found) == 2 and all(status == "failed" for (status,) in found)

        await self.wait_for(turned_away, "the refusals")
        self.assertEqual(dokploy_calls(self.state), [])
