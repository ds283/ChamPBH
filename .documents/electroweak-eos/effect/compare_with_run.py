"""
electroweak-eos prompt 02 §1.4, "The old side against the run" (README §9.2 P17): every history of the old side's export
equals its twin in the 2026.10.0 run's export, column for column as written.

    ./venv/bin/python .documents/electroweak-eos/effect/compare_with_run.py \\
        [--old var/electroweak-eos/02/export/old/QCD_Cosmology] \\
        [--run-main science-runs/2026-11-08-science-2026.10.0/fig6/inputs/histories.csv] \\
        [--run-reflections var/science-2026.10.0/out/reflections/QCD_Cosmology/histories.csv] \\
        [--run-main-root var/science-2026.10.0/out/main/QCD_Cosmology] \\
        [--run-reflections-root var/science-2026.10.0/out/reflections/QCD_Cosmology] [--out DIR]

The twin of a row is in the main store's table (the fig6 input, an unchanged copy of the run's export of the main
store), except at M = 1e-16 M_P (run r, R-ladder), whose twin is in the reflection store's. Rows are matched on beta,
n, phi_init_Mp and alpha as floats, drain_model as written, and M_Mp to a relative 1e-12 (a stored M is converted, so
1e-42 can read back as 9.999999999999999e-43). Every other column must be equal as written. The export's marker rows
("no ScalarHistory row in the store", one for each (row, M, beta) of the stored grid that was not built) are not
histories: they are counted and set aside.

Beside that, and as a supplement the prompt does not ask for: each history's fields.csv and bounces.csv in the old
side's export is compared byte for byte with the run's export of the same history, wherever the run exported one (the
main store's per-history export holds one-loop rows only; the reflection store's holds every row).

Exit 0 if every history is matched and equal, 1 otherwise (any difference is a stop: prompt §1.4). Writes
compare_with_run.txt into --out (default out/ beside this script). Reads only.
"""

import argparse
import csv
import hashlib
import math
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
MARKER = "no ScalarHistory row in the store"
KEYS_FLOAT = ("beta", "n", "phi_init_Mp", "alpha")
REFLECTIONS_M = 1e-16


def read(path: Path):
    with open(path, newline="") as fh:
        r = csv.DictReader(fh)
        return r.fieldnames, list(r)


def is_marker(row) -> bool:
    return MARKER in (row.get("failure_reasons") or "")


def same_key(a, b) -> bool:
    for k in KEYS_FLOAT:
        if float(a[k]) != float(b[k]):
            return False
    if a["drain_model"] != b["drain_model"]:
        return False
    Ma, Mb = float(a["M_Mp"]), float(b["M_Mp"])
    return math.isclose(Ma, Mb, rel_tol=1e-12, abs_tol=0.0)


def label(row) -> str:
    return (
        f"beta={row['beta']} M={row['M_Mp']} n={row['n']} phi*={row['phi_init_Mp']} "
        f"alpha={row['alpha']} model={row['drain_model']}"
    )


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument(
        "--old",
        type=Path,
        default=Path("var/electroweak-eos/02/export/old/QCD_Cosmology"),
    )
    p.add_argument(
        "--run-main",
        type=Path,
        default=Path(
            "science-runs/2026-11-08-science-2026.10.0/fig6/inputs/histories.csv"
        ),
    )
    p.add_argument(
        "--run-reflections",
        type=Path,
        default=Path(
            "var/science-2026.10.0/out/reflections/QCD_Cosmology/histories.csv"
        ),
    )
    p.add_argument(
        "--run-main-root",
        type=Path,
        default=Path("var/science-2026.10.0/out/main/QCD_Cosmology"),
    )
    p.add_argument(
        "--run-reflections-root",
        type=Path,
        default=Path("var/science-2026.10.0/out/reflections/QCD_Cosmology"),
    )
    p.add_argument("--out", type=Path, default=HERE / "out")
    p.add_argument(
        "--no-files",
        action="store_true",
        help="skip the supplementary per-history file comparison",
    )
    args = p.parse_args(argv)

    lines = []

    def say(s=""):
        print(s)
        lines.append(s)

    old_cols, old_rows = read(args.old / "histories.csv")
    main_cols, main_rows = read(args.run_main)
    refl_cols, refl_rows = read(args.run_reflections)
    say("compare_with_run.py (electroweak-eos prompt 02 §1.4, P17)")
    say(f"old side: {args.old / 'histories.csv'} ({len(old_rows)} rows)")
    say(f"run, main store: {args.run_main} ({len(main_rows)} rows)")
    say(f"run, reflection store: {args.run_reflections} ({len(refl_rows)} rows)")
    for name, cols in (("main", main_cols), ("reflections", refl_cols)):
        if cols != old_cols:
            say(f"!! the columns of the run's {name} table differ from the old side's")
            say(f"   old: {old_cols}")
            say(f"   run: {cols}")

    histories = [r for r in old_rows if not is_marker(r)]
    markers = len(old_rows) - len(histories)
    say(f"old side: {len(histories)} histories, {markers} marker rows set aside")

    ok = True
    matched = 0
    differing = 0
    failed_both = 0
    for row in histories:
        in_refl = math.isclose(float(row["M_Mp"]), REFLECTIONS_M, rel_tol=1e-12)
        table, tname = (refl_rows, "reflections") if in_refl else (main_rows, "main")
        twins = [t for t in table if not is_marker(t) and same_key(row, t)]
        if len(twins) != 1:
            say(f"!! {label(row)}: {len(twins)} twins in the run's {tname} table")
            ok = False
            continue
        twin = twins[0]
        matched += 1
        if row["failure_reasons"] and twin["failure_reasons"]:
            failed_both += 1
        diffs = [
            c
            for c in old_cols
            if c not in KEYS_FLOAT + ("drain_model", "M_Mp") and row[c] != twin[c]
        ]
        if diffs:
            differing += 1
            ok = False
            say(f"!! {label(row)} ({tname}): {len(diffs)} columns differ")
            for c in diffs:
                say(f"     {c}: old {row[c]!r} run {twin[c]!r}")
        if row["M_Mp"] != twin["M_Mp"]:
            say(
                f"   {label(row)}: M_Mp written {row['M_Mp']!r} here, {twin['M_Mp']!r} in the run"
            )
    say(
        f"matched {matched} of {len(histories)} histories; {differing} with a difference; "
        f"{failed_both} failed on both (matched on their failure reasons and blank columns)"
    )

    if not args.no_files:
        say()
        say(
            "supplementary: per-history files, byte for byte, where the run exported the same history"
        )
        n_same = n_diff = n_absent = 0
        for f in sorted(args.old.rglob("*.csv")):
            if f.name not in ("fields.csv", "bounces.csv"):
                continue
            rel = f.relative_to(args.old)
            refl_first = "M=2.4353e+11eV_n=1" in rel.parts
            roots = (
                [args.run_reflections_root]
                if refl_first
                else [args.run_main_root, args.run_reflections_root]
            )
            twin = next((r / rel for r in roots if (r / rel).is_file()), None)
            if twin is None:
                n_absent += 1
                continue
            if sha(f) == sha(twin):
                n_same += 1
            else:
                n_diff += 1
                ok = False
                say(f"!! {rel}: differs from {twin}")
        say(
            f"files equal: {n_same}; differing: {n_diff}; with no twin in the run's exports: {n_absent}"
        )

    say()
    say(
        f"RESULT: {'the old side equals the run' if ok else 'DIFFERENCES (a stop, prompt §1.4)'}"
    )
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "compare_with_run.txt").write_text("\n".join(lines) + "\n")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
