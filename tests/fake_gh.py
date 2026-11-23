"""Test double for the gh CLI: pull requests live in a JSON file, merges land in a local bare repository."""
import json
import os
import subprocess
import sys
from pathlib import Path


def opt(args, name, default=None):
    return args[args.index(name) + 1] if name in args else default


def run(command, *, env, cwd):
    argv = command[1:]
    state_file = Path(env["FAKE_GH_STATE"])
    state = json.loads(state_file.read_text())

    def save():
        state_file.write_text(json.dumps(state, indent=1))

    def result(stdout="", *, code=0, stderr=""):
        return subprocess.CompletedProcess(command, code, stdout, stderr)

    def git(*args, directory=cwd, check=True):
        return subprocess.run(["git", *args], cwd=directory, env=env, check=check, capture_output=True, text=True)

    def branch_head(branch):
        proc = git("rev-parse", "--verify", f"refs/heads/{branch}", directory=state["bare"], check=False)
        return proc.stdout.strip() if proc.returncode == 0 else ""

    def conflicting(pr):
        if pr["state"] != "OPEN" or not branch_head(pr["head"]):
            return False
        return git("merge-tree", "--write-tree", f"refs/heads/{pr['base']}", f"refs/heads/{pr['head']}",
                   directory=state["bare"], check=False).returncode == 1

    state.setdefault("calls", []).append(argv)
    state.setdefault("tokens_seen", []).extend(k for k in ("GH_TOKEN", "GITHUB_TOKEN") if k in env)
    prs = state.setdefault("prs", [])
    if "-R" in argv:
        state["repo_flag"] = opt(argv, "-R")
    save()
    if len(argv) < 2 or argv[0] != "pr":
        return result(code=2, stderr="fake gh: unsupported\n")
    cmd = argv[1]
    if cmd in ("create", "edit", "merge") and state.get("fail", {}).get(cmd, 0):
        state["fail"][cmd] -= 1
        save()
        return result(code=1, stderr=f"fake gh: {cmd} failed (injected)\n")
    if cmd == "list":
        head = opt(argv, "--head")
        rows = []
        for pr in reversed(prs):
            if pr["head"] == head:
                rows.append({"number": pr["number"], "url": pr["url"], "state": pr["state"],
                             "headRefOid": pr.get("merged_head") or branch_head(head),
                             "mergeCommit": {"oid": pr["merge_sha"]} if pr.get("merge_sha") else None})
        return result(json.dumps(rows) + "\n")
    if cmd == "create":
        number = state.get("next_number", 1)
        state["next_number"] = number + 1
        url = f"https://github.example/o/r/pull/{number}"
        prs.append({"number": number, "url": url, "state": "OPEN", "head": opt(argv, "--head"),
                    "base": opt(argv, "--base"), "title": opt(argv, "--title"), "body": opt(argv, "--body")})
        save()
        return result(url + "\n")
    pr = next(p for p in prs if p["number"] == int(argv[2]))
    if cmd == "edit":
        pr["title"], pr["body"] = opt(argv, "--title", pr["title"]), opt(argv, "--body", pr["body"])
        pr["edits"] = pr.get("edits", 0) + 1
        save()
        return result()
    if cmd == "checks":
        checks = state.get("checks", [{"name": "gates", "state": "SUCCESS", "bucket": "pass"}])
        if state.get("dirty_blocks_ci") and conflicting(pr):
            # GitHub runs no pull_request workflow for a PR that conflicts with its base.
            checks = []
        elif state.get("checks_sequence"):
            checks = state["checks_sequence"].pop(0)
            save()
        if not checks:
            return result(code=1, stderr="no checks reported on the 'x' branch\n")
        return result(json.dumps(checks) + "\n", code=8 if any(c["bucket"] == "pending" for c in checks) else 0)
    if cmd == "view":
        head = state.get("view_head_override") or pr.get("merged_head") or branch_head(pr["head"])
        view = {"state": pr["state"], "url": pr["url"], "headRefOid": head,
                "mergeCommit": {"oid": pr["merge_sha"]} if pr.get("merge_sha") else None}
        if "mergeStateStatus" in opt(argv, "--json", ""):
            dirty = conflicting(pr)
            view.update(mergeable="CONFLICTING" if dirty else "MERGEABLE", mergeStateStatus="DIRTY" if dirty else "CLEAN")
        return result(json.dumps(view) + "\n")
    if cmd == "merge":
        head = branch_head(pr["head"])
        if opt(argv, "--match-head-commit") != head:
            return result(code=1, stderr="fake gh: head commit does not match\n")
        bare = state["bare"]
        base = branch_head(pr["base"])
        tree = git("merge-tree", "--write-tree", base, head, directory=bare, check=False)
        if tree.returncode:
            return result(code=1, stderr=f"fake gh: Pull request is not mergeable: {tree.stdout}{tree.stderr}\n")
        # A squash on the remote advances the base to the merged tree with one parent.
        sha = git("-c", "user.email=gh@example.invalid", "-c", "user.name=gh", "commit-tree", tree.stdout.strip(),
                  "-p", base, "-m", f"{pr['title']} (#{pr['number']})", directory=bare).stdout.strip()
        git("update-ref", f"refs/heads/{pr['base']}", sha, base, directory=bare)
        pr.update(state="MERGED", merge_sha=sha, merged_head=head, method=[a for a in argv if a in ("--squash", "--merge", "--rebase")])
        save()
        return result()
    return result(code=2, stderr=f"fake gh: unsupported pr {cmd}\n")


if __name__ == "__main__":
    reply = run(sys.argv, env=os.environ, cwd=Path.cwd())
    sys.stdout.write(reply.stdout)
    sys.stderr.write(reply.stderr)
    sys.exit(reply.returncode)
