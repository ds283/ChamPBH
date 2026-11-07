"""
The gate R (iii) of one-loop-drain prompt 06 §2.2 (P54 (iii)): two export roots compared, file by file, byte for byte.

    ./venv/bin/python .documents/one-loop-drain/rehearse-06/compare_exports.py ROOT_A ROOT_B [--subset] [--names NAME ...]
    ./venv/bin/python .documents/one-loop-drain/rehearse-06/compare_exports.py ROOT_A ROOT_B --key-rows FILE [--key N]

ROOT_A and ROOT_B are export roots (the plot scripts' `<output>/QCD_Cosmology`). Stdlib only; reads the two trees and
writes nothing but stdout (prompt 06 sends it to a file under var/one-loop-drain/06/). Opens no store.

- **By default** every `*.csv` file under each root is listed by its path relative to the root; the two lists must be
  equal, and each pair is compared byte for byte. With `--subset`, only A's files are taken, each must exist in B, and
  B's other files are ignored (05's export of 230 histories against a rehearsal's nine). `--names` keeps only files of
  the given base names (e.g. bounces.csv).
- **Where two files differ** they are read as CSV: the headers are compared, the row counts, and every column with a
  differing cell is named, with its count and its first difference. A column whose name says it holds a timing (TIMING,
  or a name ending in `time` or `_wall`) is named as a timing; any other difference is reported as one.
- **`--key-rows FILE`**: A's FILE (relative to the root) against B's, row by row on the first N columns (default 7: the
  quanta_histories.csv key, spectrum_model, assoc_scale, drain_model, alpha, M_Mp, n, beta). For every row of A, the row of
  B with the same key: equal, or the columns that differ, each with both values. A measurement: it never fails.

Exit 0 when every compared pair is byte-identical or differs only in timing columns, and no file is missing; 1 otherwise.
With --key-rows, exit 0 unless a key of A is missing from B.
"""

import argparse
import csv
import io
import re
import sys
from pathlib import Path

TIMING = {
    "compute_time",
    "mean_RHS_time",
    "max_RHS_time",
    "min_RHS_time",
    "NP_compute_time",
    "BBN_compute_time",
    "wall",
}


def is_timing(col):
    return (
        col in TIMING
        or bool(re.search(r"(time|_wall)$", col))
        and not col.startswith("log_T")
    )


def files(root, names):
    out = sorted(str(p.relative_to(root)) for p in root.rglob("*.csv"))
    if names:
        out = [f for f in out if Path(f).name in names]
    return out


def rows(path):
    return list(csv.reader(io.StringIO(path.read_text(newline=""), newline="")))


def explain(a, b):
    ra, rb = rows(a), rows(b)
    out, timing_only = [], True
    if not ra or not rb or ra[0] != rb[0]:
        return ["headers differ"], False
    head = ra[0]
    if len(ra) != len(rb):
        out.append(f"row counts differ: {len(ra) - 1} vs {len(rb) - 1}")
        timing_only = False
    cols = {}
    for x, y in zip(ra[1:], rb[1:]):
        for i, (u, v) in enumerate(zip(x, y)):
            if u != v:
                n, first = cols.get(i, (0, (u, v)))
                cols[i] = (n + 1, first)
    for i, (n, (u, v)) in sorted(cols.items()):
        tag = "timing" if is_timing(head[i]) else "DIFFERENCE"
        if tag != "timing":
            timing_only = False
        out.append(
            f"{tag}: column {head[i]!r} differs in {n} row(s); first {u!r} vs {v!r}"
        )
    if not cols and len(ra) == len(rb):
        out.append("bytes differ but every cell is equal (line endings or quoting)")
        timing_only = False
    return out, timing_only


def key_rows(ra_path, rb_path, nkey):
    ra, rb = rows(ra_path), rows(rb_path)
    if ra[0] != rb[0]:
        print("headers differ")
        return 1
    head = ra[0]
    index = {tuple(r[:nkey]): r for r in rb[1:]}
    missing = 0
    equal = 0
    print(f"key columns: {head[:nkey]}")
    for r in ra[1:]:
        k = tuple(r[:nkey])
        if k not in index:
            print(f"  {k}: MISSING in B")
            missing += 1
            continue
        d = [(head[i], u, v) for i, (u, v) in enumerate(zip(r, index[k])) if u != v]
        if not d:
            equal += 1
            print(f"  {k}: equal")
        else:
            print(
                f"  {k}: {len(d)} column(s) differ: "
                + "; ".join(f"{c}: {u!r} vs {v!r}" for c, u, v in d)
            )
    print(
        f"rows of A: {len(ra) - 1}; equal {equal}; differing {len(ra) - 1 - equal - missing}; missing in B {missing}"
    )
    return 1 if missing else 0


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("root_a", type=Path)
    p.add_argument("root_b", type=Path)
    p.add_argument("--subset", action="store_true")
    p.add_argument("--names", nargs="+")
    p.add_argument("--key-rows")
    p.add_argument("--key", type=int, default=7)
    args = p.parse_args(argv)
    print(f"A: {args.root_a}")
    print(f"B: {args.root_b}")
    if args.key_rows:
        print(f"rows of {args.key_rows}, by key")
        return key_rows(
            args.root_a / args.key_rows, args.root_b / args.key_rows, args.key
        )

    fa, fb = files(args.root_a, args.names), files(args.root_b, args.names)
    ok = True
    if args.subset:
        missing = [f for f in fa if not (args.root_b / f).is_file()]
        pairs = [f for f in fa if f not in missing]
        print(f"files of A: {len(fa)}; missing in B: {len(missing)}")
        for f in missing:
            print(f"  MISSING in B: {f}")
        ok &= not missing
    else:
        only_a, only_b = sorted(set(fa) - set(fb)), sorted(set(fb) - set(fa))
        pairs = sorted(set(fa) & set(fb))
        print(
            f"files: A {len(fa)}, B {len(fb)}, in both {len(pairs)}, only A {len(only_a)}, only B {len(only_b)}"
        )
        for f in only_a:
            print(f"  only A: {f}")
        for f in only_b:
            print(f"  only B: {f}")
        ok &= not only_a and not only_b
    identical, timing, differ = 0, 0, 0
    by_name = {}
    for f in pairs:
        a, b = args.root_a / f, args.root_b / f
        name = Path(f).name
        t = by_name.setdefault(name, [0, 0, 0])
        if a.read_bytes() == b.read_bytes():
            identical += 1
            t[0] += 1
            continue
        lines, timing_only = explain(a, b)
        if timing_only:
            timing += 1
            t[1] += 1
        else:
            differ += 1
            t[2] += 1
            ok = False
        print(f"  {f}:")
        for line in lines:
            print(f"     {line}")
    print("per file name (identical / timing only / different):")
    for name, (i, tm, d) in sorted(by_name.items()):
        print(f"  {name}: {i} / {tm} / {d}")
    print(
        f"pairs compared: {len(pairs)}; byte-identical {identical}; differ only in timing columns {timing}; differ {differ}"
    )
    print(
        f"RESULT: {'every pair byte-identical or differing only in timing columns' if ok else 'NOT EQUAL (see above)'}"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
