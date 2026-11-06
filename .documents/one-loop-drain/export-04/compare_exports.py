"""
Compares the two trees' exports of the store of build_store.py (one-loop-drain prompt 04, §2.2).

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/export-04/compare_exports.py \\
        var/one-loop-drain/04/export-base var/one-loop-drain/04/export-tip

For histories.csv (the fixed-law records alone, then the one-loop records alone) and for each history's fields.csv:
the tip's file projected onto the base's columns, in the base's order, written with the base's writer settings (the
csv module's defaults, no quoting beyond the minimum; for fields.csv pandas' line terminator is "\\n" and so is
the csv module's here), compared byte for byte with the base's file. A fixed-law difference is a stop; a one-loop
difference is reported. Also prints the SHA-256 results of both exports and each product's path and title under both
trees.
"""

import csv
import io
import json
import sys
from pathlib import Path


def read_table(path: Path):
    with open(path, newline="") as f:
        rows = list(csv.reader(f))
    return rows[0], rows[1:]


def project(path: Path, columns, newline):
    """The file at `path` projected onto `columns`, written as csv.writer writes it (the writers' settings)."""
    header, rows = read_table(path)
    index = [header.index(c) for c in columns]
    out = io.StringIO(newline="")
    writer = csv.writer(out, lineterminator=newline)
    writer.writerow(columns)
    for row in rows:
        writer.writerow([row[i] for i in index])
    return out.getvalue().encode()


def compare(base_file: Path, tip_file: Path, newline: str):
    base_bytes = base_file.read_bytes()
    base_header, _ = read_table(base_file)
    tip_header, _ = read_table(tip_file)
    missing = [c for c in base_header if c not in tip_header]
    added = [c for c in tip_header if c not in base_header]
    order_kept = [c for c in tip_header if c in base_header] == base_header
    projected = project(tip_file, base_header, newline)
    return dict(
        identical=projected == base_bytes,
        missing_in_tip=missing,
        added_in_tip=added,
        base_order_kept=order_kept,
        base_bytes=len(base_bytes),
        projected_bytes=len(projected),
    )


def main(base: Path, tip: Path):
    manifest = json.loads((base.parent / "manifest.json").read_text())
    lines = []
    stop = False

    def say(text=""):
        lines.append(text)
        print(text)

    say("== the store's files, SHA-256 before and after each export ==")
    for name, root in (("base", base), ("tip", tip)):
        sha = json.loads((root / "sha256.json").read_text())
        say(f"{name}: unchanged={sha['equal']}")
        for file, digest in sha["before"].items():
            say(f"   {file}  {digest}  after: {sha['after'][file]}")
        stop |= not sha["equal"]
    base_sha, tip_sha = (
        json.loads((r / "sha256.json").read_text())["before"] for r in (base, tip)
    )
    say(f"both exports read the same store files: {base_sha == tip_sha}")
    stop |= base_sha != tip_sha

    say()
    say("== histories.csv, the projection of the tip's file onto the base's columns ==")
    for kind, fixed in (
        ("histories_fixed.csv", True),
        ("histories_one_loop.csv", False),
    ):
        result = compare(base / kind, tip / kind, "\r\n")
        say(f"{kind}: byte-identical={result['identical']} " + json.dumps(result))
        if fixed and not result["identical"]:
            stop = True

    say()
    say("== fields.csv per history, the projection onto the base's columns ==")
    for spec in manifest:
        if spec["failure"]:
            continue
        name = spec["name"]
        result = compare(
            base / "fields" / name / "fields.csv",
            tip / "fields" / name / "fields.csv",
            "\n",
        )
        rows = len(read_table(base / "fields" / name / "fields.csv")[1])
        say(
            f"{name} ({spec['drain_model']}, {rows} rows): byte-identical={result['identical']} "
            + json.dumps(result)
        )
        if spec["drain_model"] == "fixed" and not result["identical"]:
            stop = True

    say()
    say("== paths and titles under both trees ==")
    spec_by_name = {s["name"]: s for s in manifest}
    base_labels = json.loads((base / "labels.json").read_text())
    tip_labels = json.loads((tip / "labels.json").read_text())
    for name in [s["name"] for s in manifest]:
        b, t = base_labels[name], tip_labels[name]
        say(f"-- {name}")
        say(f"   path: base {b['path']}  | tip {t['path']}")
        if spec_by_name[name]["drain_model"] == "fixed" and b["path"] != t["path"]:
            stop = True
        for key in ("add_beta_summary_labels", "add_ScalarHistory_labels"):
            if key in b:
                say(f"   {key}: base {b[key]}  | tip {t[key]}")
                if spec_by_name[name]["drain_model"] == "fixed" and b[key] != t[key]:
                    stop = True
        for key in sorted(set(b["driver_strings"]) | set(t["driver_strings"])):
            bs, ts = b["driver_strings"].get(key), t["driver_strings"].get(key)
            marker = "same" if bs == ts else "DIFFERENT"
            say(f"   {key}: {marker}")
            if marker == "DIFFERENT" or spec_by_name[name]["drain_model"] != "fixed":
                say(f"      base: {bs!r}")
                say(f"      tip:  {ts!r}")
            if marker == "DIFFERENT" and spec_by_name[name]["drain_model"] == "fixed":
                stop = True
    say(f"plot_Delta_N_eff legend: base {base_labels['plot_Delta_N_eff_legend']}")
    say(f"                         tip  {tip_labels['plot_Delta_N_eff_legend']}")

    say()
    say(f"STOP CONDITION HIT: {stop}")
    (Path(__file__).parent / "out" / "compare.txt").write_text("\n".join(lines) + "\n")
    return 1 if stop else 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1]), Path(sys.argv[2])))
