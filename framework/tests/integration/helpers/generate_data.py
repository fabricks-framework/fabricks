"""Generate the git-versioned `raw/` fixtures from `seed/` (pure stdlib, no Spark).

`seed/` holds the original JSON (with BEL_* columns); `raw/` is what the tests load,
transformed once here so seed.py needs none: __operation + __timestamp added, BEL_*
dropped, decimalField added, monarch/regent aliased from king/queen, royal built as a
cumulative current-snapshot (reload mode). Run: python generate_data.py
"""

import json
from pathlib import Path

_ALIAS_TARGETS = ["monarch", "regent"]  # king/queen are aliased into these
_ROYAL_SOURCES = ["king", "queen", "king__deletelog", "queen__deletelog"]
_NO_ALIAS_MARKER = "2022/04/01/0001"  # job4's single file: not aliased/merged
_DECIMAL_TOPICS = {"monarch", "regent", "royal", "prince"}
SEED = Path(__file__).resolve().parent.parent / "seed"
OUT = Path(__file__).resolve().parent.parent / "raw"


def _timestamp(rel: str) -> str:
    y, mo, d, hm = Path(rel).parent.parts[-4:]  # yyyy/MM/dd/HHmm, seconds are 00
    return f"{y}-{mo}-{d} {hm[:2]}:{hm[2:4]}:00"


def _operation(record: dict, rel: str) -> str:
    if "BEL_IsFullLoad" in record:
        if record.get("BEL_DeleteDateUtc") is not None:
            return "delete"

        return "reload" if record.get("BEL_IsFullLoad") else "upsert"

    return "delete" if "deletelog" in rel else "upsert"


def _no_bel(record: dict) -> dict:
    return {k: v for k, v in record.items() if not k.startswith("BEL_")}


def _transform(record: dict, topic: str, rel: str) -> dict:
    out = _no_bel(record)

    if topic.split("__")[0] in _DECIMAL_TOPICS:
        out["decimalField"] = 10.5

    return {**out, "__operation": _operation(record, rel), "__timestamp": _timestamp(rel)}


def _royal_snapshot(job_dir: Path, state: dict) -> str | None:
    """Replay the job's king/queen (up)serts & deletes into `state`; return its latest ts."""
    ops = []

    for src in _ROYAL_SOURCES:
        for jf in sorted((job_dir / src).rglob("*.json")):
            rel = jf.relative_to(job_dir).as_posix()

            if _NO_ALIAS_MARKER not in rel:
                ts = _timestamp(rel)
                ops += [(ts, "deletelog" in rel, r["id"], r) for r in json.loads(jf.read_text())]

    for _ts, delete, id_, r in sorted(ops, key=lambda o: (o[0], o[1])):  # delete wins on ties
        if delete:
            state.pop(id_, None)
        else:
            state[id_] = r

    return max((o[0] for o in ops), default=None)


def _royal_path(ts: str) -> str:  # inverse of _timestamp
    d, t = ts.split(" ")
    return f"{d.replace('-', '/')}/{t[:2]}{t[3:5]}/royal_{d.replace('-', '')}{t[:2]}{t[3:5]}.json"


def generate():
    royal: dict = {}
    royal_ts: str | None = None

    for job_dir in sorted(SEED.glob("job*"), key=lambda p: int(p.name[3:])):
        files: dict[str, list] = {}

        def add(records: list, src_rel: str, rel: str):
            files.setdefault(rel, []).extend(_transform(r, rel.split("/")[0], src_rel) for r in records)

        for src_dir in sorted(p for p in job_dir.iterdir() if p.is_dir()):
            for jf in sorted(src_dir.rglob("*.json")):
                rel = jf.relative_to(job_dir).as_posix()
                records = json.loads(jf.read_text())
                add(records, rel, rel)

                if _NO_ALIAS_MARKER in rel:
                    continue

                source = next((s for s in ("king", "queen") if rel.startswith(s)), None)

                if source:
                    for t in _ALIAS_TARGETS:
                        target = rel.replace(source, t)

                        if t == "regent":  # regent folds its deletelog into one folder
                            target = target.replace("__deletelog", "")

                        add(records, rel, target)

        royal_ts = _royal_snapshot(job_dir, royal) or royal_ts

        if royal and royal_ts:  # one reload load => all rows share the batch folder timestamp
            files[f"royal/{_royal_path(royal_ts)}"] = [
                {**_no_bel(royal[i]), "decimalField": 10.5, "__operation": "reload", "__timestamp": royal_ts}
                for i in sorted(royal)
            ]

        for rel, records in files.items():
            dest = OUT / job_dir.name / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(json.dumps(records, indent=2))

        print(f"{job_dir.name}: {len(files)} files")


if __name__ == "__main__":
    generate()
