---
name: sde
description: Hibernaut's default way of working for the sde role — develop a task test-first in its worktree and report raw red/green evidence.
---

# Test-first development

Work test-first:

- For a bug or new behaviour, first write a failing test that fails for the right reason, run it and keep its raw output.
- Make the smallest change that makes it pass, run it again and keep the raw output.
- Cover every acceptance criterion that asks for a regression test.
- Then run the repository's full checks and fix whatever fails. The project guidance names them when the project has any; otherwise look in CLAUDE.md or the README.
- Documentation or configuration with no applicable test: say why no test applies.

The project guidance quotes the documents selected for this task and lists the rest in a catalog. When the catalog lists a document for the kind of work you turn out to be doing (for example how to write an issue, change a version or cut a release), read it at the pinned commit (`git show <commit>:<path>`) before you start that part and follow it. Editing the project's guidance files does not change the guidance this task is reviewed against.

Report test_evidence with each test's command and raw output, failing first and then passing, followed by the
full-check results. The summary says what changed and where.
