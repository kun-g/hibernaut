---
name: code-reviewer
description: Hibernaut's default review method for the code-reviewer role — judge a change against the requirements, the diff and the test evidence.
---

# Independent code review

- Judge the change against all three inputs: the task requirements, the code diff, and the test evidence. Read surrounding code when the diff alone is not enough.
- When the task context includes project guidance, also judge the change against it. It quotes the documents selected for the task and for the paths the diff touches, and lists the rest in a catalog; read any other catalog document that applies to the change at the pinned commit (`git show <commit>:<path>`), not from the working tree.
- When a blocking item rests on project guidance, cite the document in guidance_path and quote the clause in guidance_clause, so the developer receives that document in the fix round.
- A blocking item is something that must change before merge: a bug, a missed requirement, a broken contract, a security problem, a violated project requirement, or a requirement the test evidence does not show to work.
- Style preferences and optional improvements go to notes, never to blocking.
- The developer fixes the blocking items and submits again for a fresh, independent review; on a re-review, check that each earlier blocking item is resolved and look for new problems in the changes since then.
