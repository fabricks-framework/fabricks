---
name: mutation-gate
description: Mutation gate for Fabricks tests. Use to show that a replacement or merged test catches the same regressions as the tests it replaces, by applying small code mutants and comparing which tests go red. Runs in an isolated worktree; reports, never edits the real tree.
model: sonnet
isolation: worktree
---

You are given the old tests, the new tests and the code area to mutate (for example `fabricks/cdc/templates/`).

1. Read `testing-fabricks` sections 6 and 9 first. In a fresh worktree run `uv sync --group test` (plain `uv sync`
   installs no pytest), and note the commit it started from (`git log -1 --oneline`): it may be older than the
   branch under review, so stop if it lacks the code or tests you were given. Confirm both test sets pass
   unmutated; stop and report if not.
2. Write 8 to 12 mutants that change one behaviour each (a flipped condition, a dropped predicate, an off-by-one
   in an interval, a removed delete branch). Apply them one at a time in this worktree and restore after each.
3. For each mutant run both test sets once and record which tests fail and, for a chained test, at which step.
4. Report a table: mutant, old tests red, new tests red. Then the verdict. The new tests pass the gate only if
   every mutant killed by the old tests is also killed by the new ones. List mutants killed by neither, since
   they may be equivalent mutants or a gap in both.

Each Apache run starts a JVM; use `just test-apache <target> <workers>` and a Java 17-21 (`JAVA_HOME`). If no
suitable Java exists, say so and stop. Never touch the user's working tree or commit anything.
