# Decisions

Short notes on why something is the way it is, so code comments can stay
limited to the non-obvious why (see [CONSTITUTION.md](../CONSTITUTION.md) § 2).

- One file per decision, named `NNNN-short-title.md` (next free number).
- Keep it to: **Context** (the problem or constraint), **Decision** (what we
  chose and what we rejected), **Consequences** (what it costs or forbids).
- Write one when a future reader would otherwise have to ask "why not the
  obvious way?". Do not restate what the code does.
- Never edit an accepted decision to change history; add a new note that
  supersedes it and link both ways.
