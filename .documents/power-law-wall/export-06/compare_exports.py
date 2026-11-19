"""
The gate Export's first item (power-law-wall prompt 06, §2.1; README §6.10 P54; orchestrator note 06, ruling 2): the
exponential's exports of the tip against the base's, on the store of build_store.py, as export_tree.py wrote them.

    PYTHONPATH=. ./venv/bin/python .documents/power-law-wall/export-06/compare_exports.py \\
        var/power-law-wall/06/export-base var/power-law-wall/06/export-tip

- every file the base wrote at `<label>/<rest>` equals the tip's `<label>/exponential/<rest>` byte for byte, and the tip
  wrote no other file under `<label>/exponential/`;
- `histories.csv` is the one exception: the tip's file with its `model` column removed (by the csv module, with the
  writer's settings, as extract_common.write_histories_csv writes) equals the base's byte for byte, and `model` sits
  directly after `n` and holds `exponential` in every row;
- the tip wrote nothing directly under `<label>/` but the potentials' directories, `exponential/` and `inverse-power/`;
- both exports left the store unchanged (each export's sha256.json).

Any other difference is a stop (exit 1). Writes out/compare.txt beside this script.
"""

import csv
import hashlib
import io
import json
import sys
from pathlib import Path

LABEL = "QCD_Cosmology"


def files(root: Path):
    return sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file())


def without_column(data: bytes, column: str):
    rows = list(csv.reader(io.StringIO(data.decode(), newline="")))
    header = rows[0]
    k = header.index(column)
    out = io.StringIO(newline="")
    writer = csv.writer(out)
    for row in rows:
        writer.writerow(row[:k] + row[k + 1 :])
    return header, rows, k, out.getvalue().encode()


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def main(base_out: Path, tip_out: Path) -> int:
    lines = []

    def say(text=""):
        lines.append(text)
        print(text)

    stop = False
    base = base_out / LABEL
    tip_label = tip_out / LABEL
    tip = tip_label / "exponential"

    # what the tip wrote directly under <label>/
    top = sorted(p.name for p in tip_label.iterdir())
    say(f"the tip's {LABEL}/ holds: {top}")
    if top != ["exponential", "inverse-power"] or any(
        (tip_label / name).is_file() for name in top
    ):
        say(
            "STOP: the tip wrote something under <label>/ other than the potentials' directories"
        )
        stop = True

    base_files = files(base)
    tip_files = files(tip)
    say(
        f"base {LABEL}/: {len(base_files)} files; tip {LABEL}/exponential/: {len(tip_files)} files"
    )
    only_base = sorted(set(base_files) - set(tip_files))
    only_tip = sorted(set(tip_files) - set(base_files))
    if only_base or only_tip:
        say(
            f"STOP: paths differ; only in the base: {only_base}; only in the tip: {only_tip}"
        )
        stop = True
    else:
        say("the paths are the same, file for file")

    say()
    for rel in base_files:
        if rel not in tip_files:
            continue
        b = (base / rel).read_bytes()
        t = (tip / rel).read_bytes()
        if rel == "histories.csv":
            header, rows, k, stripped = without_column(t, "model")
            after_n = k == header.index("n") + 1
            cells = sorted({row[k] for row in rows[1:]})
            same = stripped == b
            say(
                f"{rel}: model at column {k} (after n: {after_n}), cells {cells}, rows {len(rows) - 1}; "
                f"with model removed: {'IDENTICAL' if same else 'DIFFERS'} ({len(stripped)} and {len(b)} bytes; "
                f"sha256 {sha(stripped)[:16]} and {sha(b)[:16]})"
            )
            if not (same and after_n and cells == ["exponential"]):
                say("STOP: histories.csv differs beyond the model column")
                stop = True
            continue
        same = b == t
        say(
            f"{rel}: {'IDENTICAL' if same else 'DIFFERS'} ({len(b)} bytes; sha256 {sha(b)[:16]}"
            + ("" if same else f" and {sha(t)[:16]}")
            + ")"
        )
        if not same:
            stop = True

    say()
    for name, out in (("base", base_out), ("tip", tip_out)):
        record = json.loads((out / "sha256.json").read_text())
        say(f"{name} export: store files unchanged: {record['equal']}")
        for f, digest in record["before"].items():
            say(f"   {f}: {digest} before, {record['after'][f]} after")
        stop |= not record["equal"]

    say()
    say(f"STOP CONDITION HIT: {stop}")
    out = Path(__file__).parent / "out"
    out.mkdir(exist_ok=True)
    (out / "compare.txt").write_text("\n".join(lines) + "\n")
    return 1 if stop else 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1]), Path(sys.argv[2])))
