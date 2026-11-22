"""Test double for the gh CLI: pull requests live in a JSON file, merges land in a local bare repository."""
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

STATE = Path(os.environ["FAKE_GH_STATE"])


def load():
    return json.loads(STATE.read_text()) if STATE.exists() else {}


def save(state):
    STATE.write_text(json.dumps(state, indent=1))


def opt(args, name, default=None):
    return args[args.index(name) + 1] if name in args else default


def git(*args, cwd=None):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def fail(state, op):
    remaining = state.setdefault("fail", {}).get(op, 0)
    if remaining:
        state["fail"][op] = remaining - 1
        save(state)
        print(f"fake gh: {op} failed (injected)", file=sys.stderr)
        sys.exit(1)


def branch_head(bare, branch):
    out = subprocess.run(["git", "ls-remote", bare, f"refs/heads/{branch}"], capture_output=True, text=True).stdout
    return out.split()[0] if out.split() else ""


def main(argv):
    state = load()
    state.setdefault("calls", []).append(argv)
    state.setdefault("tokens_seen", []).extend(k for k in ("GH_TOKEN", "GITHUB_TOKEN") if k in os.environ)
    prs = state.setdefault("prs", [])
    save(state)
    if len(argv) < 2 or argv[0] != "pr":
        print("fake gh: unsupported", file=sys.stderr)
        sys.exit(2)
    if "-R" in argv:
        state["repo_flag"] = opt(argv, "-R")
    bare = state["bare"]
    cmd = argv[1]
    if cmd == "list":
        head = opt(argv, "--head")
        rows = []
        for pr in reversed(prs):
            if pr["head"] != head:
                continue
            oid = pr.get("merged_head") or branch_head(bare, head)
            rows.append({"number": pr["number"], "url": pr["url"], "state": pr["state"], "headRefOid": oid,
                         "mergeCommit": {"oid": pr["merge_sha"]} if pr.get("merge_sha") else None})
        print(json.dumps(rows))
        return
    if cmd == "create":
        fail(state, "create")
        number = state.get("next_number", 1)
        state["next_number"] = number + 1
        url = f"https://github.example/o/r/pull/{number}"
        prs.append({"number": number, "url": url, "state": "OPEN", "head": opt(argv, "--head"),
                    "base": opt(argv, "--base"), "title": opt(argv, "--title"), "body": opt(argv, "--body")})
        save(state)
        print(url)
        return
    pr = next(p for p in prs if p["number"] == int(argv[2]))
    if cmd == "edit":
        fail(state, "edit")
        pr["title"], pr["body"] = opt(argv, "--title", pr["title"]), opt(argv, "--body", pr["body"])
        pr["edits"] = pr.get("edits", 0) + 1
        save(state)
        return
    if cmd == "checks":
        checks = state.get("checks", [{"name": "gates", "state": "SUCCESS", "bucket": "pass"}])
        if state.get("checks_sequence"):
            checks = state["checks_sequence"].pop(0)
            save(state)
        if not checks:
            print("no checks reported on the 'x' branch", file=sys.stderr)
            sys.exit(1)
        print(json.dumps(checks))
        sys.exit(8 if any(c["bucket"] == "pending" for c in checks) else 0)
    if cmd == "view":
        head = state.get("view_head_override") or pr.get("merged_head") or branch_head(bare, pr["head"])
        print(json.dumps({"state": pr["state"], "url": pr["url"], "headRefOid": head,
                          "mergeCommit": {"oid": pr["merge_sha"]} if pr.get("merge_sha") else None}))
        return
    if cmd == "merge":
        fail(state, "merge")
        head = branch_head(bare, pr["head"])
        if opt(argv, "--match-head-commit") != head:
            print("fake gh: head commit does not match", file=sys.stderr)
            sys.exit(1)
        with tempfile.TemporaryDirectory() as tmp:
            git("clone", "-q", bare, tmp)
            git("config", "user.email", "gh@example.invalid", cwd=tmp)
            git("config", "user.name", "gh", cwd=tmp)
            git("checkout", "-q", pr["base"], cwd=tmp)
            squash = subprocess.run(["git", "merge", "--squash", f"origin/{pr['head']}"], cwd=tmp,
                                    capture_output=True, text=True)
            if squash.returncode != 0:
                print(f"fake gh: Pull request is not mergeable: {squash.stdout}{squash.stderr}", file=sys.stderr)
                sys.exit(1)
            git("commit", "-qm", f"{pr['title']} (#{pr['number']})", cwd=tmp)
            git("push", "-q", "origin", pr["base"], cwd=tmp)
            sha = git("rev-parse", "HEAD", cwd=tmp)
        pr.update(state="MERGED", merge_sha=sha, merged_head=head, method=[a for a in argv if a in ("--squash", "--merge", "--rebase")])
        save(state)
        return
    print(f"fake gh: unsupported pr {cmd}", file=sys.stderr)
    sys.exit(2)


main(sys.argv[1:])
