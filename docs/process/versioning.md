# Versioning

Hibernaut releases use SemVer `MAJOR.MINOR.PATCH`. The single source is `__version__` in
`src/agent_runtime/__init__.py`; the package metadata and the startup banner read it.

## Change kinds

Every SDE delivery reports one `change_kind` for the whole branch, with a `change_rationale` that names what changed:

| change_kind | Meaning | Version bump |
| :--- | :--- | :--- |
| `breaking` | Incompatible change to a documented interface, configuration key, event payload, stored data or behaviour that existing deployments rely on | major |
| `feature` | Compatible new capability (a new option, executor, endpoint, payload field) | minor |
| `fix` | Compatible correction of behaviour that was wrong | patch |
| `test_docs` | Only tests, documentation or comments; nothing shipped behaves differently | none |

When a change mixes kinds, report the highest one (`breaking` > `feature` > `fix` > `test_docs`).

## Who sets the version

- The SDE does not edit `__version__`. It develops, tests and commits on the task branch and reports `change_kind`.
- Hibernaut's delivery step reads `__version__` from the latest `origin/main`, applies the bump above and commits it
  on the task branch (`Set version X.Y.Z (...)`) before it runs the gates and opens the pull request.
- The version in an open pull request is provisional. If another pull request changes the version on `main` first,
  the delivery step merges `main` into the branch again, recomputes the bump from the new base and pushes before it
  merges. Two pull requests opened from the same base therefore end with consecutive versions, never the same one.
- A person preparing a release by hand follows the same table and edits only `__version__`.

## Gates

Before the pull request is pushed, the delivery step runs the repository gates from `AGENTS.md` in order:
`lint-imports`, `pytest -n auto --dist worksteal -v` (the full suite, in parallel), `pyright`. A failing gate pushes
nothing; the task waits with the output. Serial `pytest -v` is only for investigating a failure that may come from
running in parallel.

## Merging

This project's delivery ends at **PR created**. Merging is a separate grant (`auto_merge` in the deployment's
`[projects.hibernaut.delivery]`); without it a person merges. With it, Hibernaut merges only after the independent
review passed and CI succeeded, and the task's receipt records the version and the actual merge commit.
