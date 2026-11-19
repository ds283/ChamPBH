"""
power-law-wall prompt 07d §2.3 and §2.4 (P116, P118): export roots compared. Copied from prompt 07's
(.documents/power-law-wall/rehearse-07/compare_exports.py), which was adapted from
.documents/power-law-wall/export-06/compare_exports.py and one-loop-drain 06's; 07d adds the mode --across-k-ex (P118),
described at the end, and changes nothing else. Stdlib only; it reads the two trees and writes nothing but stdout
(prompt 07d sends it to a file under var/power-law-wall/07d-r2/). It opens no store.

    ./venv/bin/python .documents/power-law-wall/rehearse-07d/compare_exports.py ROOT_A ROOT_B
    ./venv/bin/python .documents/power-law-wall/rehearse-07d/compare_exports.py --base BASE_LABEL_ROOT TIP_MODEL_ROOT
    ./venv/bin/python .documents/power-law-wall/rehearse-07d/compare_exports.py --n1-subset ROOT_A ROOT_B
    ./venv/bin/python .documents/power-law-wall/rehearse-07d/compare_exports.py --across-k-ex ROOT_A ROOT_B

- **File by file** (default; §2.2, §2.4's first item): every regular file under each root, by its path relative to the
  root; the two lists must be equal, and each pair is compared byte for byte. Where two files differ they are read as
  CSV: the headers and the row counts are compared, and every column with a differing cell is named, with its count and
  its first difference, and said to hold a timing (TIMING, or a name ending in `time` or `_wall`) or not. Equal when
  every pair is byte-identical or differs only in timing columns.
- **--base** (§2.5, 06's rule): BASE_LABEL_ROOT is the base's `<output>/<label>`, TIP_MODEL_ROOT the tip's
  `<output>/<label>/exponential`. Every file of the one is at the same path in the other and no file is in one only;
  every pair is byte-identical, or differs only in timing columns (each named: two builds of one grid, unlike 06's one
  store exported by two trees, carry their own compute times in data.csv), except `histories.csv`, whose tip copy with its `model` column removed (the csv module,
  with the writer's default settings, as extract_common.write_histories_csv writes) equals the base's byte for byte,
  with `model` directly after `n` and `exponential` in every row.
- **failure reasons** (every mode), under the user's ruling (a) on prompt 07's §2.2 stop (2026-11-16): a cell of a
  `failure_reason` or `failure_reasons` column that differs is equal when, with main.py's history label (the
  parenthesised name after the reason's first word, up to the first "): ") masked in each, the shorter is a prefix of
  the longer (production cuts a reason to 256 characters, label included). Such a column is named as a masked label,
  never hidden, and accepted as a timing column is.
- **--n1-subset** (§2.4's second item): ROOT_A is `mixed`'s inverse-power root (n = 1 only), ROOT_B `pl`'s (n = 1 and 2).
  Every file of A is at the same path in B. A row file (one with an `n` column in its header: `histories.csv` and
  `quanta_histories*.csv` at the root) equals, cell for cell, B's header and B's rows with n = 1, in their order; every
  other file of A equals B's byte for byte, or differs only in timing or masked-label columns, each named. B's files not in A are counted, and each must lie under a path of n = 2
  (a path component ending `_n=2`); a file of B under an n = 1 path missing from A is a difference.

`--exclude GLOB …` (any mode) leaves out of both trees every file whose base name matches a glob, and prints how many
were left out under each root: prompt 07 runs the quanta tools (R5) on the tip's roots only, so the base rule is taken
over the plot scripts' files with `--exclude 'quanta_*'`.

**--across-k-ex** (07d, P118): ROOT_A is a root of 07's tree (var/power-law-wall/07/export-<store>/QCD_Cosmology/<model>),
ROOT_B the same root of 07d's. A path component of A named by 07's drain model is read as 07d's (DIR_MAP:
`one-loop-measured-pl-v1` -> `one-loop-inverse-power-v1`, U11; `one-loop-measured-v1` -> `one-loop-exponential-v2`,
R14), and the two path lists must then be equal. Each pair that is not byte-identical is read as CSV, and every column
with a differing cell is named with its count, its largest change (as numbers, where both cells parse) and its class:
- **timing** (as above), and **masked label** (a failure reason equal once main.py's label is masked; 07's ruling (a));
- **mapped** (`drain_model`, equal once A's cell is read through DIR_MAP);
- **P114** (allowed to move where the law reads no l): in `bounces.csv` the bounce columns of P114's list
  (KEX_BOUNCE); in `histories.csv` the history columns of P114's list it holds (KEX_HISTORY: the failing shares,
  unassessed_share and the drain record's candidates, shares, ell_min, unresolvable_assessments and all_* margins);
- **measured**: any column of a file under a `one-loop-exponential-v2` directory, of a `quanta_*` file, and of a
  `histories.csv` row whose drain model is `one-loop-exponential-v2` (rows matched by their key: beta, M_Mp, n, model,
  phi_init_Mp, alpha and the mapped drain_model, each key held once);
- **DIFFERENCE** otherwise, a stop (prompt 07d §2.4).
A file whose rows differ in number is measured where the class above says measured, and a DIFFERENCE elsewhere. Exit 0
when no DIFFERENCE is found and the path lists are equal; 1 otherwise.

Exit 0 when the mode's rule holds; 1 otherwise.
"""

import argparse
import csv
import fnmatch
import hashlib
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
    return col in TIMING or (
        bool(re.search(r"(time|_wall)$", col)) and not col.startswith("log_T")
    )


EXCLUDE = []


def files(root: Path):
    out = sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file())
    if EXCLUDE:
        gone = [
            f for f in out if any(fnmatch.fnmatch(Path(f).name, g) for g in EXCLUDE)
        ]
        if gone:
            print(
                f"excluded under {root} by --exclude {' '.join(EXCLUDE)}: {len(gone)} files"
            )
        out = [f for f in out if f not in gone]
    return out


def rows(data: bytes):
    return list(csv.reader(io.StringIO(data.decode(), newline="")))


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


REASON_COLUMNS = ("failure_reason", "failure_reasons")
LABEL = re.compile(r"^(\S+ \()(.*?)(\): )")


def mask_label(reason):
    """main.py's history label in a failure reason masked (the user's ruling (a) on prompt 07's §2.2 stop)."""
    return LABEL.sub(r"\1<label>\3", reason, count=1)


def label_equal(u, v):
    """Two reason cells equal once the label is masked, to the shorter of the two (256-character) cuts."""
    mu, mv = mask_label(u), mask_label(v)
    k = min(len(mu), len(mv))
    return mu[:k] == mv[:k]


def cell_equal(col, u, v):
    return u == v or (
        col in REASON_COLUMNS and bool(u) and bool(v) and label_equal(u, v)
    )


def explain(a: bytes, b: bytes):
    try:
        ra, rb = rows(a), rows(b)
    except Exception as e:  # noqa: BLE001 - a file that is not CSV text differs plainly
        return [f"not comparable as CSV ({type(e).__name__})"], False
    out, timing_only = [], True
    if not ra or not rb or ra[0] != rb[0]:
        return ["headers differ"], False
    head = ra[0]
    if len(ra) != len(rb):
        out.append(f"row counts differ: {len(ra) - 1} vs {len(rb) - 1}")
        timing_only = False
    cols = {}
    masked = {}
    for x, y in zip(ra[1:], rb[1:]):
        for i, (u, v) in enumerate(zip(x, y)):
            if u != v:
                n, first = cols.get(i, (0, (u, v)))
                cols[i] = (n + 1, first)
                if not cell_equal(head[i], u, v):
                    masked[i] = False
                else:
                    masked.setdefault(i, True)
    for i, (n, (u, v)) in sorted(cols.items()):
        if is_timing(head[i]):
            tag = "timing"
        elif head[i] in REASON_COLUMNS and masked.get(i):
            tag = "label (equal once main.py's label is masked, to the shorter cut; ruling (a))"
        else:
            tag = "DIFFERENCE"
        if tag == "DIFFERENCE":
            timing_only = False
        out.append(
            f"{tag}: column {head[i]!r} differs in {n} row(s); first {u!r} vs {v!r}"
        )
    if not cols and len(ra) == len(rb):
        out.append("bytes differ but every cell is equal (line endings or quoting)")
        timing_only = False
    return out, timing_only


def by_file(ra: Path, rb: Path) -> int:
    fa, fb = files(ra), files(rb)
    only_a, only_b = sorted(set(fa) - set(fb)), sorted(set(fb) - set(fa))
    pairs = sorted(set(fa) & set(fb))
    print(
        f"files: A {len(fa)}, B {len(fb)}, in both {len(pairs)}, only A {len(only_a)}, only B {len(only_b)}"
    )
    for f in only_a:
        print(f"  only A: {f}")
    for f in only_b:
        print(f"  only B: {f}")
    ok = not only_a and not only_b
    identical = timing = differ = 0
    by_name = {}
    for f in pairs:
        a, b = (ra / f).read_bytes(), (rb / f).read_bytes()
        t = by_name.setdefault(Path(f).name, [0, 0, 0])
        if a == b:
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
        f"RESULT: {'every pair byte-identical or differing only in timing columns, no file in one only' if ok else 'NOT EQUAL (see above)'}"
    )
    return 0 if ok else 1


def without_column(data: bytes, column: str):
    rs = rows(data)
    header = rs[0]
    k = header.index(column)
    out = io.StringIO(newline="")
    writer = csv.writer(out)
    for row in rs:
        writer.writerow(row[:k] + row[k + 1 :])
    return header, rs, k, out.getvalue().encode()


def base_rule(base: Path, tip: Path) -> int:
    ok = True
    fb, ft = files(base), files(tip)
    print(f"base: {len(fb)} files; tip: {len(ft)} files")
    only_b, only_t = sorted(set(fb) - set(ft)), sorted(set(ft) - set(fb))
    if only_b or only_t:
        print(f"paths differ; only in the base: {only_b}; only in the tip: {only_t}")
        ok = False
    else:
        print("the paths are the same, file for file")
    identical = timing = 0
    for rel in sorted(set(fb) & set(ft)):
        b, t = (base / rel).read_bytes(), (tip / rel).read_bytes()
        if rel == "histories.csv":
            header, rs, k, stripped = without_column(t, "model")
            after_n = k == header.index("n") + 1
            cells = sorted({row[k] for row in rs[1:]})
            same = stripped == b
            print(
                f"{rel}: model at column {k} (after n: {after_n}), cells {cells}, rows {len(rs) - 1}; "
                f"with model removed: {'IDENTICAL' if same else 'DIFFERS'} ({len(stripped)} and {len(b)} bytes; "
                f"sha256 {sha(stripped)[:16]} and {sha(b)[:16]})"
            )
            if not (same and after_n and cells == ["exponential"]):
                print("  histories.csv differs beyond the model column")
                ok = False
            continue
        if b == t:
            identical += 1
        else:
            lines, accepted = explain(b, t)
            if accepted:
                timing += 1
            else:
                ok = False
            print(
                f"{rel}: {'differs only in timing columns' if accepted else 'DIFFERS'}"
            )
            for line in lines:
                print(f"     {line}")
    print(
        f"other files byte-identical: {identical}, differing only in timing columns: {timing}, of {len(set(fb) & set(ft)) - ('histories.csv' in fb)}"
    )
    print(
        f"RESULT: {'every file equal byte for byte or in all but named timing columns, histories.csv once its model column is removed' if ok else 'NOT EQUAL (see above)'}"
    )
    return 0 if ok else 1


def n1_subset(ra: Path, rb: Path) -> int:
    ok = True
    fa, fb = files(ra), files(rb)
    missing = [f for f in fa if f not in fb]
    extra = [f for f in fb if f not in fa]
    print(
        f"files: A {len(fa)}, B {len(fb)}; A's missing in B {len(missing)}; B's not in A {len(extra)}"
    )
    for f in missing:
        print(f"  MISSING in B: {f}")
        ok = False
    not_n2 = [
        f for f in extra if not any(part.endswith("_n=2") for part in Path(f).parts)
    ]
    print(
        f"B's files not in A under an n = 2 path: {len(extra) - len(not_n2)}; elsewhere: {len(not_n2)}"
    )
    for f in not_n2:
        print(f"  B only, not under an n = 2 path: {f}")
        ok = False
    identical = row_files = timing = 0
    for rel in fa:
        if rel in missing:
            continue
        a, b = (ra / rel).read_bytes(), (rb / rel).read_bytes()
        ha = rows(a)[0] if rel.endswith(".csv") and a else []
        if "n" in ha:
            ra_rows, rb_rows = rows(a), rows(b)
            k = rb_rows[0].index("n")
            want = [rb_rows[0]] + [r for r in rb_rows[1:] if r[k] == "1"]
            head = rb_rows[0]
            exact = ra_rows == want
            same = len(ra_rows) == len(want) and ra_rows[:1] == want[:1]
            nmask = 0
            for x, y in zip(ra_rows[1:], want[1:]):
                for c, u, v in zip(head, x, y):
                    if u != v:
                        if cell_equal(c, u, v):
                            nmask += 1
                        else:
                            same = False
            if not exact and same:
                print(
                    f"{rel}: {nmask} failure-reason cell(s) equal only once main.py's label is masked, to the shorter cut "
                    "(ruling (a))"
                )
            row_files += 1
            print(
                f"{rel}: a row file; A {len(ra_rows) - 1} rows, B {len(rb_rows) - 1} rows of which n = 1: "
                f"{len(want) - 1}, n = 2: {sum(1 for r in rb_rows[1:] if r[k] == '2')}; A equal to B's n = 1 rows "
                f"in order: {same}"
            )
            ok &= same
            continue
        if a == b:
            identical += 1
        else:
            lines, accepted = explain(a, b)
            if accepted:
                timing += 1
            else:
                ok = False
            print(
                f"{rel}: {'differs only in timing or masked-label columns' if accepted else 'DIFFERS'}"
            )
            for line in lines:
                print(f"     {line}")
    print(
        f"row files: {row_files}; other files of A byte-identical to B's: {identical}, differing only in timing or "
        f"masked-label columns: {timing}, of {len(fa) - len(missing) - row_files}"
    )
    print(
        f"RESULT: {'A is B at n = 1: every file equal (timing and masked-label columns named), the row files B rows at n = 1; B alone holds n = 2' if ok else 'NOT EQUAL (see above)'}"
    )
    return 0 if ok else 1


# --across-k-ex (P118)
DIR_MAP = {
    "one-loop-measured-pl-v1": "one-loop-inverse-power-v1",
    "one-loop-measured-v1": "one-loop-exponential-v2",
}
MEASURED_DIR = "one-loop-exponential-v2"
REGIMES = ("one_loop", "gap", "classical", "unassessed")
KINDS = ("birth", "reflection", "tracker")
KEX_BOUNCE = {
    "regime",
    "ell",
    "log_k_ex_Mp",
    "log_m_Mp",
    "log_q_Mp",
    "birth_margin",
    "reflection_margin",
    "reflection_margin_N",
    "tracker_margin",
    "tracker_margin_N",
}
KEX_HISTORY = (
    {f"worst_{k}_margin" for k in KINDS}
    | {f"worst_{k}_margin_N" for k in KINDS}
    | {f"{k}_failing_share" for k in KINDS}
    | {"unassessed_share", "ell_min", "unresolvable_assessments"}
    | {f"candidates_{r}" for r in REGIMES}
    | {f"drained_share_{r}" for r in REGIMES}
    | {f"all_worst_{k}_margin" for k in KINDS}
    | {f"all_worst_{k}_margin_N" for k in KINDS}
    | {f"all_{k}_failing_share" for k in KINDS}
)
HISTORY_KEY = ("beta", "M_Mp", "n", "model", "phi_init_Mp", "alpha", "drain_model")


def map_path(rel):
    return str(Path(*[DIR_MAP.get(part, part) for part in Path(rel).parts]))


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


class ColTally:
    def __init__(self):
        self.cols = {}

    def add(self, col, cls, u, v):
        s = self.cols.setdefault(
            col, {"n": 0, "cls": set(), "big": None, "first": (u, v)}
        )
        s["n"] += 1
        s["cls"].add(cls)
        x, y = num(u), num(v)
        if x is not None and y is not None:
            d = y - x
            if s["big"] is None or abs(d) > abs(s["big"][0]):
                s["big"] = (d, u, v)

    def lines(self):
        out = []
        for col, s in self.cols.items():
            big = (
                f"; largest change {s['big'][0]:+.6g} ({s['big'][1]} -> {s['big'][2]})"
                if s["big"] is not None
                else ""
            )
            out.append(
                f"{'/'.join(sorted(s['cls']))}: column {col!r} differs in {s['n']} cell(s){big}; first {s['first'][0]!r} vs {s['first'][1]!r}"
            )
        return out

    def classes(self):
        return (
            set().union(*(s["cls"] for s in self.cols.values())) if self.cols else set()
        )


def cell_class(col, u, v, allowed, measured):
    if measured:
        return "measured"
    if is_timing(col):
        return "timing"
    if col in REASON_COLUMNS and u and v and label_equal(u, v):
        return "masked label"
    if col == "drain_model" and DIR_MAP.get(u, u) == v:
        return "mapped"
    if col in allowed:
        return "P114"
    return "DIFFERENCE"


def across_file(rel, a: bytes, b: bytes, summary):
    name = Path(rel).name
    under_measured = MEASURED_DIR in Path(rel).parts or name.startswith("quanta_")
    allowed = (
        KEX_BOUNCE
        if name == "bounces.csv"
        else KEX_HISTORY if name == "histories.csv" else set()
    )
    try:
        ra, rb = rows(a), rows(b)
    except Exception as e:  # noqa: BLE001
        return [f"DIFFERENCE: not comparable as CSV ({type(e).__name__})"], {
            "DIFFERENCE"
        }
    if not ra or not rb or ra[0] != rb[0]:
        return ["DIFFERENCE: headers differ"], {"DIFFERENCE"}
    head = ra[0]
    tally = ColTally()
    out = []
    if name == "histories.csv":
        ki = [head.index(k) for k in HISTORY_KEY]
        dmi = head.index("drain_model")

        def keyed(rs, mapped):
            d = {}
            for r in rs[1:]:
                k = tuple(
                    DIR_MAP.get(r[i], r[i]) if (mapped and i == dmi) else r[i]
                    for i in ki
                )
                d.setdefault(k, []).append(r)
            return d

        da, db = keyed(ra, True), keyed(rb, False)
        dup = [k for k, v in list(da.items()) + list(db.items()) if len(v) != 1]
        only_a, only_b = [k for k in da if k not in db], [k for k in db if k not in da]
        order_a = [
            tuple(DIR_MAP.get(r[i], r[i]) if i == dmi else r[i] for i in ki)
            for r in ra[1:]
        ]
        order_b = [tuple(r[i] for i in ki) for r in rb[1:]]
        out.append(
            f"rows A {len(ra) - 1}, B {len(rb) - 1}; keys held twice {len(dup)}; only A {len(only_a)}; only B {len(only_b)}; "
            f"the same order under the map: {order_a == order_b}"
        )
        if dup or only_a or only_b:
            out.append(
                f"DIFFERENCE: keys do not match one to one: {dup[:3]} {only_a[:3]} {only_b[:3]}"
            )
            return out, {"DIFFERENCE"}
        nrows_measured = 0
        for k in da:
            x, y = da[k][0], db[k][0]
            meas = y[dmi] == MEASURED_DIR
            nrows_measured += meas
            for i, (u, v) in enumerate(zip(x, y)):
                if u != v:
                    tally.add(head[i], cell_class(head[i], u, v, allowed, meas), u, v)
        out.append(
            f"rows of the drain model {MEASURED_DIR} (measured): {nrows_measured}"
        )
    else:
        if len(ra) != len(rb):
            cls = "measured" if under_measured else "DIFFERENCE"
            out.append(f"{cls}: row counts differ: {len(ra) - 1} vs {len(rb) - 1}")
            summary[cls] += 1
            return out, {cls}
        for x, y in zip(ra[1:], rb[1:]):
            for i, (u, v) in enumerate(zip(x, y)):
                if u != v:
                    tally.add(
                        head[i],
                        cell_class(head[i], u, v, allowed, under_measured),
                        u,
                        v,
                    )
    out.extend(tally.lines())
    cls = tally.classes()
    if not tally.cols and not out:
        out.append("DIFFERENCE: bytes differ but every cell is equal")
        cls = {"DIFFERENCE"}
    return out, cls


def across(ra: Path, rb: Path) -> int:
    from collections import Counter

    fa, fb = files(ra), files(rb)
    ma = {map_path(f): f for f in fa}
    only_a = sorted(set(ma) - set(fb))
    only_b = sorted(set(fb) - set(ma))
    pairs = sorted(set(ma) & set(fb))
    print(
        f"files: A {len(fa)}, B {len(fb)}, in both under the map {len(pairs)}, only A {len(only_a)}, only B {len(only_b)}"
    )
    print(
        "A's paths read through " + ", ".join(f"{k} -> {v}" for k, v in DIR_MAP.items())
    )
    for f in only_a:
        print(f"  only A (mapped): {f}")
    for f in only_b:
        print(f"  only B: {f}")
    ok = not only_a and not only_b
    summary = Counter()
    by_name = {}
    per_class_cols = {}
    for f in pairs:
        a, b = (ra / ma[f]).read_bytes(), (rb / f).read_bytes()
        t = by_name.setdefault(Path(f).name, Counter())
        if a == b:
            t["identical"] += 1
            summary["identical"] += 1
            continue
        lines, cls = across_file(f, a, b, Counter())
        verdict = (
            "DIFFERENCE"
            if "DIFFERENCE" in cls
            else "measured" if "measured" in cls else "allowed"
        )
        t[verdict] += 1
        summary[verdict] += 1
        if verdict == "DIFFERENCE":
            ok = False
        print(f"  {f}: {verdict}")
        for line in lines:
            print(f"     {line}")
            if ": column " in line:
                c = line.split(": column ", 1)[1].split(" differs", 1)[0]
                per_class_cols.setdefault(line.split(":", 1)[0], set()).add(
                    (Path(f).name, c)
                )
    print(
        "per file name (identical / allowed: timing, masked label, mapped or P114 only / measured / DIFFERENCE):"
    )
    for name, t in sorted(by_name.items()):
        print(
            f"  {name}: {t['identical']} / {t['allowed']} / {t['measured']} / {t['DIFFERENCE']}"
        )
    print("columns that differ, by class (file name, column):")
    for cls, cols in sorted(per_class_cols.items()):
        print(f"  {cls}: " + ", ".join(f"{n}:{c}" for n, c in sorted(cols)))
    print(
        f"pairs compared: {len(pairs)}; byte-identical {summary['identical']}; allowed only {summary['allowed']}; "
        f"measured {summary['measured']}; DIFFERENCE {summary['DIFFERENCE']}"
    )
    print(
        f"RESULT: {'no difference outside the timing, masked-label, mapped and P114 columns where the law reads no l; the rest measured' if ok else 'NOT EQUAL (see above)'}"
    )
    return 0 if ok else 1


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("root_a", type=Path)
    p.add_argument("root_b", type=Path)
    g = p.add_mutually_exclusive_group()
    g.add_argument("--base", action="store_true")
    g.add_argument("--n1-subset", action="store_true")
    g.add_argument("--across-k-ex", action="store_true")
    p.add_argument("--exclude", nargs="+", default=[], metavar="GLOB")
    args = p.parse_args(argv)
    EXCLUDE.extend(args.exclude)
    print(f"A: {args.root_a}")
    print(f"B: {args.root_b}")
    if args.base:
        print("mode: --base (06's rule)")
        return base_rule(args.root_a, args.root_b)
    if args.across_k_ex:
        print("mode: --across-k-ex (P118)")
        return across(args.root_a, args.root_b)
    if args.n1_subset:
        print("mode: --n1-subset")
        return n1_subset(args.root_a, args.root_b)
    print("mode: file by file")
    return by_file(args.root_a, args.root_b)


if __name__ == "__main__":
    sys.exit(main())
