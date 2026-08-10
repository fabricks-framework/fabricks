# `legacy.streaming`

Legacy streaming ingestion. The default Bronze load is a registered external table
(`mode: register`, batch); streaming is opt-in and isolated here so core never imports
it at load time.

- `read.py` — `read()` facade, batch + stream file readers (`cloudFiles`, `readStream`).
- `write.py` — `write_stream()` (trigger-once `foreachBatch`).
- `table.py` — `run_once_via_stream()` for table create / schema update.
- `parsers/` — `BaseParser`, `@parser`, `get_parser` for raw-file parsing.

Used only when a job opts in: Bronze `mode: append`/`memory`, Silver `stream: true`,
or a custom parser. Core lazy-imports these at the point of use.
