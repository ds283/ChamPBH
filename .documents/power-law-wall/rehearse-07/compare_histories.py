"""
power-law-wall prompt 07 §2.1 and §2.4 (P64, P67): the histories of one store against the same histories of another,
bitwise. Adapted from one-loop-drain prompt 06's compare_base.py (.documents/one-loop-drain/rehearse-06/compare_base.py):
the match is by the potential and n as well, through each history's model_type, and the bounce rows are compared;
06's checks of its tip's new columns and its sample masses, which were 06's own, are not carried over.

    ./venv/bin/python .documents/power-law-wall/rehearse-07/compare_histories.py A_STEM B_STEM [--model M] [--n N]

A_STEM and B_STEM are the stores' paths without `.db`. Stdlib only. Every shard (`<stem>-shard*.db`) is read through
sqlite3 with `file:...?mode=ro` URIs only; no primary is opened, neither datastorekit nor Ray is used, and nothing is
written but stdout (prompt 07 sends it to a file under var/power-law-wall/07/).

- **The selection:** the histories of each store whose potential is --model (`exponential` or `inverse-power`, from
  ScalarHistory.model_type: 1 ExponentialModel, 2 InversePowerModel; CosmologyConcepts/Models/model_ids.py) and whose
  n is --n; every history where an option is not given.
- **Matched:** by (potential, M, n, beta, phi*, alpha, drain model): the model's row in the table model_type selects
  (never model_serial alone, README §6.9 P37), its M_value.value_eV, n_value and beta_value.value, phi_value.value and
  alpha_value's value and drain_model; the floats by float.hex. Every selected history of A must be matched exactly once
  in B's selection, and every one of B's exactly once by one of A's. A sample of ScalarHistoryValue by its ln(1 + z),
  through z_serial to its own shard's redshift table; AdiabaticHistory and BBNData by their history, and their samples by
  ln(1 + z) the same way; ScalarHistoryBounce rows by their history, row by row in `ordinal` order (the counts equal).
- **Compared bitwise:** every column both schemas hold in ScalarHistory, ScalarHistoryValue, ScalarHistoryBounce,
  AdiabaticHistory, AdiabaticHistoryValue, BBNData and BBNDataValue; floats by float.hex, the rest exactly, NULL equal
  only to NULL. extra_data is JSON, compared key by key over the keys both hold (a key one side only holds is listed and
  is a difference). **The key tables:** the rows each matched history references (the cosmology, the model's table and
  its M_value and beta_value, the three tolerance rows, alpha_value, phi_value, pi_value, temperature, IntegrationSolver),
  every column both hold but `serial`, `timestamp` and `version` (IntegrationSolver's `label` is its key, and is compared).
- **Left out, by name** (printed): `serial` and every `*_serial` column, `version`, `label` (of the history tables),
  `timestamp`, and the timing columns compute_time, mean_RHS_time, max_RHS_time, min_RHS_time, NP_compute_time,
  BBN_compute_time.
- A history that fails in both with equal reasons is a match, and its columns are compared as any other's; one that
  fails in one store only is a difference.
- **failure_reason**, under the user's ruling (a) on prompt 07's §2.2 stop (2026-11-16): main.py's history label, the
  job name and the submission time, which a failure reason carries as the parenthesised name after its first word
  (up to the first "): "), is a run-dependent fragment. Two reasons that differ are equal when, with the label masked in
  each, the shorter is a prefix of the longer: production cuts a reason to DEFAULT_STRING_LENGTH (256) characters, label
  included, so a longer job name keeps less of the reason. Each such match is printed with its lengths, and counted.
- **The count:** the number of values compared, per table.

Exit 0 if every compared value is equal and every history is matched exactly once each way; 1 otherwise.
"""

import argparse
import glob
import json
import re
import sqlite3
import sys
from collections import defaultdict
from types import SimpleNamespace
from urllib.parse import quote

TIMING = (
    "compute_time",
    "mean_RHS_time",
    "max_RHS_time",
    "min_RHS_time",
    "NP_compute_time",
    "BBN_compute_time",
)
LEFT_OUT_NAMED = ("serial", "version", "label", "timestamp") + TIMING
KEY_LEFT_OUT = ("serial", "version", "timestamp")

# model_type -> (the model's table, the potential's option value)
MODEL_TABLES = {
    1: ("ExponentialModel", "exponential"),
    2: ("InversePowerModel", "inverse-power"),
}
POTENTIALS = {v[1]: k for k, v in MODEL_TABLES.items()}

# the ScalarHistory columns each key table is reached by
KEY_REFS = {
    "atol_serial": "tolerance",
    "rtol_serial": "tolerance",
    "tuning_tol_serial": "tolerance",
    "alpha_serial": "alpha_value",
    "phi_Einstein_init_serial": "phi_value",
    "pi_Einstein_init_serial": "pi_value",
    "T_Jordan_init_serial": "temperature",
    "solver_serial": "IntegrationSolver",
}


LABEL = re.compile(r"^(\S+ \()(.*?)(\): )")


def mask_label(reason):
    """main.py's history label (job name and submission time) in a failure reason, masked: the
    parenthesised name after the reason's first word, up to the first "): "."""
    return LABEL.sub(r"\1<label>\3", reason, count=1)


def left_out(col):
    return col in LEFT_OUT_NAMED or col.endswith("_serial")


def key_left_out(col):
    return col in KEY_LEFT_OUT or col.endswith("_serial")


def ro(path):
    c = sqlite3.connect("file:" + quote(path, safe="/") + "?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    return c


def tables_of(conn):
    return {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


def cols_of(conn, table):
    return [r[1] for r in conn.execute(f'PRAGMA table_info("{table}")')]


def row(conn, table, serial):
    r = conn.execute(f'SELECT * FROM "{table}" WHERE serial = ?', (serial,)).fetchone()
    return None if r is None else dict(r)


def value(conn, table, serial):
    col = next(c for c in cols_of(conn, table) if c.startswith("value"))
    r = conn.execute(
        f'SELECT "{col}" FROM "{table}" WHERE serial = ?', (serial,)
    ).fetchone()
    return None if r is None else r[0]


def same(a, b):
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, float) or isinstance(b, float):
        try:
            return float(a).hex() == float(b).hex()
        except (TypeError, ValueError):
            return False
    return a == b


def histories(stem, model, n):
    out = []
    shards = sorted(glob.glob(glob.escape(stem) + "-shard*.db"))
    for path in shards:
        conn = ro(path)
        tabs = tables_of(conn)
        if "ScalarHistory" not in tabs:
            conn.close()
            continue
        has_model = "drain_model" in cols_of(conn, "alpha_value")
        for r in conn.execute('SELECT * FROM "ScalarHistory"'):
            r = dict(r)
            mtable, potential = MODEL_TABLES[r["model_type"]]
            em = row(conn, mtable, r["model_serial"])
            if model is not None and potential != model:
                continue
            if n is not None and em["n_value"] != n:
                continue
            a = row(conn, "alpha_value", r["alpha_serial"])
            out.append(
                SimpleNamespace(
                    path=path,
                    row=r,
                    mtable=mtable,
                    potential=potential,
                    em=em,
                    M=value(conn, "M_value", em["M_serial"]),
                    n=em["n_value"],
                    beta=value(conn, "beta_value", em["beta_serial"]),
                    phi=value(conn, "phi_value", r["phi_Einstein_init_serial"]),
                    alpha=a["value"],
                    model=a["drain_model"] if has_model else "fixed",
                )
            )
        conn.close()
    return shards, out


def samples(path, table, key, serial):
    conn = ro(path)
    if table not in tables_of(conn):
        conn.close()
        return {}, []
    q = (
        f'SELECT v.*, z.log_one_plus_z AS "__x" FROM "{table}" v '
        f'JOIN "redshift" z ON z.serial = v.z_serial WHERE v."{key}" = ?'
    )
    out, dup = {}, []
    for r in conn.execute(q, (serial,)):
        r = dict(r)
        x = r.pop("__x")
        if x in out:
            dup.append(x)
        out[x] = r
    conn.close()
    return out, dup


def children(path, table, column, serial, order=None):
    conn = ro(path)
    if table not in tables_of(conn):
        conn.close()
        return None
    q = f'SELECT * FROM "{table}" WHERE "{column}" = ?'
    if order:
        q += f' ORDER BY "{order}"'
    rows = [dict(r) for r in conn.execute(q, (serial,))]
    conn.close()
    return rows


class Tally:
    def __init__(self):
        self.compared = defaultdict(int)
        self.diffs = defaultdict(dict)
        self.masked = 0

    def cmp_row(self, table, tag, a_row, b_row, cols):
        for c in cols:
            a, b = a_row.get(c), b_row.get(c)
            self.compared[table] += 1
            if not same(a, b):
                self.diff(table, c, f"{tag}: A {a!r} B {b!r}")

    def diff(self, table, col, text):
        n, first = self.diffs[table].get(col, (0, None))
        self.diffs[table][col] = (n + 1, first or text)


def common(a_cols, b_cols, lo=left_out):
    both = [c for c in a_cols if c in b_cols]
    return [c for c in both if not lo(c)], [c for c in both if lo(c)]


def hkey(h):
    return (
        h.potential,
        float(h.M).hex(),
        h.n,
        float(h.beta).hex(),
        float(h.phi).hex(),
        float(h.alpha).hex(),
        h.model,
    )


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("a")
    p.add_argument("b")
    p.add_argument("--model", choices=tuple(POTENTIALS))
    p.add_argument("--n", type=int)
    args = p.parse_args(argv)

    print(f"A store: {args.a}")
    print(f"B store: {args.b}")
    print(
        f"selection: potential {args.model or 'any'}, n {args.n if args.n is not None else 'any'}"
    )
    print(
        "matched by (potential, M, n, beta, phi*, alpha, drain model); floats by float.hex"
    )
    print(
        "left out, by name: serial and every *_serial column; "
        + "; ".join(LEFT_OUT_NAMED[1:])
    )
    print("key tables: every column both hold but serial, timestamp, version")

    a_shards, a_h = histories(args.a, args.model, args.n)
    b_shards, b_h = histories(args.b, args.model, args.n)
    print(
        f"A: {len(a_shards)} shard files, {len(a_h)} selected histories; "
        f"B: {len(b_shards)} shard files, {len(b_h)} selected histories"
    )

    ok = True
    tally = Tally()
    reported = set()
    matched_b = defaultdict(int)
    failures_both = 0

    for a in sorted(
        a_h, key=lambda h: (h.potential, h.n, h.model, h.alpha, h.M, h.beta)
    ):
        tag = (
            f"{a.potential} n={a.n} {a.model} alpha={a.alpha:g} M={a.M:.6g}eV "
            f"beta={a.beta:g} phi*={a.phi:g}"
        )
        cands = [b for b in b_h if hkey(b) == hkey(a)]
        print()
        print(f"=== {tag}: {len(cands)} row(s) in B")
        if len(cands) != 1:
            print("    MATCH FAILED")
            ok = False
            continue
        b = cands[0]
        matched_b[id(b)] += 1
        print(
            f"    A {a.path.rsplit('-', 1)[-1]} #{a.row['serial']}; B {b.path.rsplit('-', 1)[-1]} #{b.row['serial']}"
        )
        af, bf = a.row["failure"], b.row["failure"]
        if af or bf:
            if af and bf:
                failures_both += 1
            print(
                f"    failure A {bool(af)} B {bool(bf)}; reasons "
                f"{'equal' if a.row['failure_reason'] == b.row['failure_reason'] else 'DIFFER'}"
            )

        # the key tables
        ac, bc = ro(a.path), ro(b.path)
        refs = [
            ("QCD_Cosmology", "cosmology_serial", a.row, b.row),
            (a.mtable, "model_serial", a.row, b.row),
        ] + [(tab, col, a.row, b.row) for col, tab in KEY_REFS.items()]
        refs += [
            ("M_value", "M_serial", a.em, b.em),
            ("beta_value", "beta_serial", a.em, b.em),
        ]
        for tab, col, ar, br in refs:
            tab_b = b.mtable if col == "model_serial" else tab
            if ar[col] is None or br[col] is None:
                # a reference a failure row leaves NULL (solver_serial): equal only to NULL
                tally.compared[f"key:{tab}"] += 1
                if not (ar[col] is None and br[col] is None):
                    tally.diff(f"key:{tab}", col, f"{tag}: A {ar[col]!r} B {br[col]!r}")
                continue
            x, y = row(ac, tab, ar[col]), row(bc, tab_b, br[col])
            cc, _lo = common(list(x), list(y), key_left_out)
            tally.cmp_row(f"key:{tab}", tag, x, y, cc)
            if f"key:{tab}" not in reported:
                reported.add(f"key:{tab}")
                print(
                    f"    key {tab} (via {col}): compared {', '.join(cc)}; "
                    f"A only {[c for c in x if c not in y]}; B only {[c for c in y if c not in x]}"
                )

        # ScalarHistory
        cmp, lo = common(list(a.row), list(b.row))
        cmp_nox = [c for c in cmp if c not in ("extra_data", "failure_reason")]
        if "ScalarHistory" not in reported:
            reported.add("ScalarHistory")
            print(
                f"    ScalarHistory: compared ({len(cmp_nox)} + extra_data): {', '.join(cmp_nox)}"
            )
            print(f"    ScalarHistory: left out, held by both: {', '.join(lo)}")
            print(
                f"    ScalarHistory: A-only columns: {', '.join(c for c in a.row if c not in b.row) or 'none'}"
            )
            print(
                f"    ScalarHistory: B-only columns: {', '.join(c for c in b.row if c not in a.row) or 'none'}"
            )
        tally.cmp_row("ScalarHistory", tag, a.row, b.row, cmp_nox)
        # failure_reason under the user's ruling (a) on 07's §2.2 stop: the label masked, compared to
        # the shorter of the two cuts (see the module's docstring)
        fa, fb = a.row.get("failure_reason"), b.row.get("failure_reason")
        tally.compared["ScalarHistory"] += 1
        if fa is None or fb is None:
            if not (fa is None and fb is None):
                tally.diff(
                    "ScalarHistory", "failure_reason", f"{tag}: A {fa!r} B {fb!r}"
                )
        elif fa == fb:
            pass
        else:
            ma, mb = mask_label(fa), mask_label(fb)
            k = min(len(ma), len(mb))
            if ma[:k] == mb[:k]:
                tally.masked += 1
                print(
                    f"    failure_reason: equal once the label is masked, to the shorter cut ({k} of "
                    f"{len(ma)} and {len(mb)} masked characters)"
                )
            else:
                tally.diff(
                    "ScalarHistory",
                    "failure_reason",
                    f"{tag}: masked A {ma!r} B {mb!r}",
                )
        ax = json.loads(a.row["extra_data"]) if a.row.get("extra_data") else {}
        bx = json.loads(b.row["extra_data"]) if b.row.get("extra_data") else {}
        both = [k for k in ax if k in bx]
        tally.cmp_row("ScalarHistory.extra_data", tag, ax, bx, both)
        only_a, only_b = [k for k in ax if k not in bx], [k for k in bx if k not in ax]
        print(
            f"    extra_data: {len(both)} keys compared; only A {only_a}; only B {only_b}"
        )
        for k in only_a + only_b:
            tally.diff("ScalarHistory.extra_data", k, f"{tag}: key on one side only")
        ac.close()
        bc.close()

        # ScalarHistoryBounce, row by row in ordinal order
        ab = children(
            a.path, "ScalarHistoryBounce", "history_serial", a.row["serial"], "ordinal"
        )
        bb = children(
            b.path, "ScalarHistoryBounce", "history_serial", b.row["serial"], "ordinal"
        )
        if ab is None or bb is None:
            print(
                f"    ScalarHistoryBounce: table {'absent in A' if ab is None else ''}{' absent in B' if bb is None else ''}"
            )
            if (ab or []) or (bb or []):
                ok = False
        else:
            print(f"    ScalarHistoryBounce: A {len(ab)} rows, B {len(bb)} rows")
            if len(ab) != len(bb):
                tally.diff(
                    "ScalarHistoryBounce",
                    "(row count)",
                    f"{tag}: A {len(ab)} B {len(bb)}",
                )
            if ab and bb:
                cb, lob = common(list(ab[0]), list(bb[0]))
                if "ScalarHistoryBounce" not in reported:
                    reported.add("ScalarHistoryBounce")
                    print(
                        f"    ScalarHistoryBounce: compared: {', '.join(cb)}; left out: {', '.join(lob)}"
                    )
                for x, y in zip(ab, bb):
                    tally.cmp_row("ScalarHistoryBounce", tag, x, y, cb)

        # ScalarHistoryValue
        as_, ad = samples(
            a.path, "ScalarHistoryValue", "history_serial", a.row["serial"]
        )
        bs, bd = samples(
            b.path, "ScalarHistoryValue", "history_serial", b.row["serial"]
        )
        xs = [x for x in as_ if x in bs]
        print(
            f"    ScalarHistoryValue: A {len(as_)}, B {len(bs)}, matched {len(xs)}; duplicates A {len(ad)} B {len(bd)}"
        )
        if len(xs) != len(as_) or len(xs) != len(bs) or ad or bd:
            tally.diff(
                "ScalarHistoryValue",
                "(samples)",
                f"{tag}: A {len(as_)} B {len(bs)} matched {len(xs)}",
            )
        if xs:
            c2, lo2 = common(list(as_[xs[0]]), list(bs[xs[0]]))
            if "ScalarHistoryValue" not in reported:
                reported.add("ScalarHistoryValue")
                print(f"    ScalarHistoryValue: compared: {', '.join(c2)}")
                print(f"    ScalarHistoryValue: left out: {', '.join(lo2)}")
            for x in xs:
                tally.cmp_row("ScalarHistoryValue", tag, as_[x], bs[x], c2)

        # AdiabaticHistory, BBNData and their samples
        for table, vtable, vkey in (
            ("AdiabaticHistory", "AdiabaticHistoryValue", "history_serial"),
            ("BBNData", "BBNDataValue", "bbn_serial"),
        ):
            acs = (
                children(a.path, table, "scalar_history_serial", a.row["serial"]) or []
            )
            bcs = (
                children(b.path, table, "scalar_history_serial", b.row["serial"]) or []
            )
            print(f"    {table}: A {len(acs)} row(s), B {len(bcs)}")
            if len(acs) != len(bcs) or len(acs) > 1:
                tally.diff(table, "(row count)", f"{tag}: A {len(acs)} B {len(bcs)}")
                continue
            if not acs:
                continue
            c3, lo3 = common(list(acs[0]), list(bcs[0]))
            if table not in reported:
                reported.add(table)
                print(
                    f"    {table}: compared: {', '.join(c3)}; left out: {', '.join(lo3)}"
                )
            tally.cmp_row(table, tag, acs[0], bcs[0], c3)
            av, avd = samples(a.path, vtable, vkey, acs[0]["serial"])
            bv, bvd = samples(b.path, vtable, vkey, bcs[0]["serial"])
            xs = [x for x in av if x in bv]
            print(
                f"    {vtable}: A {len(av)}, B {len(bv)}, matched {len(xs)}; duplicates A {len(avd)} B {len(bvd)}"
            )
            if len(xs) != len(av) or len(xs) != len(bv) or avd or bvd:
                tally.diff(
                    vtable,
                    "(samples)",
                    f"{tag}: A {len(av)} B {len(bv)} matched {len(xs)}",
                )
            if xs:
                c4, lo4 = common(list(av[xs[0]]), list(bv[xs[0]]))
                if vtable not in reported:
                    reported.add(vtable)
                    print(
                        f"    {vtable}: compared: {', '.join(c4)}; left out: {', '.join(lo4)}"
                    )
                for x in xs:
                    tally.cmp_row(vtable, tag, av[x], bv[x], c4)

    unmatched_b = [b for b in b_h if matched_b[id(b)] != 1]
    print()
    print(
        f"A histories matched exactly once in B: {sum(1 for a in a_h if [b for b in b_h if hkey(b) == hkey(a)].__len__() == 1)} of {len(a_h)}"
    )
    print(
        f"B histories matched exactly once by A: {len(b_h) - len(unmatched_b)} of {len(b_h)}"
    )
    print(f"histories failing in both: {failures_both}")
    if unmatched_b or len(a_h) != len(b_h):
        ok = False
        for b in unmatched_b:
            print(
                f"   !! unmatched B history {b.potential} n={b.n} {b.model} alpha={b.alpha:g} M={b.M:.6g} beta={b.beta:g}"
            )
    print()
    print(
        f"failure_reason values equal only once the label is masked and cut to the shorter length: {tally.masked}"
    )
    print()
    print("Values compared, per table:")
    total = 0
    for tname, cnt in sorted(tally.compared.items()):
        print(f"  {tname}: {cnt}")
        total += cnt
    print(f"  total: {total}")
    print()
    if tally.diffs:
        print("DIFFERENCES:")
        for tname, cols in tally.diffs.items():
            for c, (cnt, first) in cols.items():
                print(f"  {tname}.{c}: {cnt} value(s) differ; first at {first}")
        ok = False
    else:
        print("DIFFERENCES: none")
    print(
        f"RESULT: {'every history matched once each way, every compared value bitwise equal' if ok else 'NOT EQUAL (see above)'}"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
