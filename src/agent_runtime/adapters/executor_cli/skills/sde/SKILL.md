---
name: sde
description: Hibernaut's default way of working for the sde role — develop a task test-first in its worktree, bump the version, open or update a pull request, and report raw red/green evidence.
---

# Test-first development

Work test-first:

- For a bug or new behaviour, first write a failing test that fails for the right reason, run it and keep its raw output.
- Make the smallest change that makes it pass, run it again and keep the raw output.
- Cover every acceptance criterion that asks for a regression test.
- Then run the repository's full checks and fix whatever fails. The project guidance names them when the project has any; otherwise look in CLAUDE.md or the README.
- Documentation or configuration with no applicable test: say why no test applies.

The project guidance quotes the documents selected for this task and lists the rest in a catalog. When the catalog lists a document for the kind of work you turn out to be doing (for example how to write an issue, change a version or cut a release), read it at the pinned commit (`git show <commit>:<path>`) before you start that part and follow it. Editing the project's guidance files does not change the guidance this task is reviewed against.

# Asking for a decision

When the brief delegates implementation choices (“拿不准的自己定”), make the choice and record it in the summary. Use `needs_input` for a decision beyond that scope. The dispatcher gets the question first when available.

Write the question as a proposal the person can approve with one click. For example: “是否允许已有的读取凭证访问新文件夹？” In `decision`, explain what is blocked, why the person owns the choice, and what happens to those credentials under each button. Explain an unfamiliar name where you first use it. Recommend the button you would choose and give the trade-off in the reason. Reject ends the task, so describe what stays in place when work stops.

# Versioning and pull request

If the role contract assigns publication to Hibernaut, commit the reviewed work locally, classify the change, and report the evidence. Hibernaut handles the version and PR. When the task asks for a report on the pull request, such as the new head and the result when you rework someone else's PR, write that comment in the report's `pr_comment` field. Use `{head_sha}` and `{pr_url}` for values you cannot know before delivery. Hibernaut posts the comment once per pushed head. Anyone who can see the PR can read it, so leave out private material such as local paths, session IDs, internal notes, and credentials. Omit the field when the task does not ask for a PR comment.

For standalone publication, follow these steps:

- Locate the repository's version definition (follow the project guidance or README when specified; otherwise inspect standard project files such as `src/*/__init__.py`, `pyproject.toml`, `package.json`, `Cargo.toml`, or `VERSION`). If the repository tracks a version, bump it according to the project's versioning rules (default SemVer: patch for fixes, minor for compatible features, major for breaking changes). On a rework round where this branch already bumped the version, keep that bump unless the change scope grew.
- Re-run the repository's full checks after bumping the version so the final commit is verified.
- Commit on the current branch, push with a normal push (never force-push), and open a pull request with `gh pr create` (if an open pull request already exists for this branch, pushing updates it).
- When merging a pull request, pass `--delete-branch` to `gh pr merge` so the source branch is deleted. In a git worktree, if `gh` cannot switch branches or delete the local branch, verify the pull request is merged and delete the remote branch (`git push origin --delete <branch>`) if it still exists, without touching other worktrees.

Report test_evidence with each test's command and raw output, failing first and then passing, followed by the
full-check results. The summary says what changed and where, the version bump, and the pull request URL.
