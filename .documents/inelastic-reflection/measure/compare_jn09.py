"""
The comparison of inelastic-reflection prompt 11 §2.7 (orchestrator note 11, ruling 2): the alpha = 0
histories of a new store against the histories of the same (M, beta, phi*) in
`jordan-normalization` 09's 2026.7.0 reference store. Stdlib only.

    ./venv/bin/python .documents/inelastic-reflection/measure/compare_jn09.py NEW_STEM OLD_STEM [--M-Mp 1e-3] [--phi-Mp 5] [--betas 0.5 1.5 3 5.5]

NEW_STEM and OLD_STEM are the stores' paths without `.db`. Every shard (`<stem>-shard*.db`) is read
through sqlite3 with `file:...?mode=ro` URIs only; no primary is opened, and nothing is written but
stdout (prompt 11 sends it to a file under var/inelastic-reflection-11/).

- **Matched:** a history by (M, beta, phi*), through its model's row (M_value.value in eV, as each
  store holds it; beta_value.value; phi_value.value), and in the new store at alpha = 0 (a shard with
  no alpha_value table holds alpha = 0 only). A sample of `ScalarHistoryValue` by its ln(1 + z),
  joined through `z_serial` to the redshift table of its own shard; `AdiabaticHistory` and `BBNData`
  by their history (`scalar_history_serial`), and their samples (`AdiabaticHistoryValue`,
  `BBNDataValue`) by ln(1 + z) in the same way.
- **Left out, by name:** `serial` and every `*_serial` column; `version`; `label`; `timestamp`;
  the six timing columns `compute_time`, `mean_RHS_time`, `max_RHS_time`, `min_RHS_time`,
  `NP_compute_time`, `BBN_compute_time`. The script prints this list.
- **Compared bitwise:** every other column both schemas hold, floats by `float.hex`, the rest
  exactly, NULL equal only to NULL. `extra_data` is JSON, compared key by key over the keys both
  hold; a key only one side holds is listed, and is not a difference.
- **New columns** (those only the new schema holds) are listed, not compared, with their values at
  the matched histories.
- **The count:** the number of values compared, per table.

Exit 0 if every compared value is equal and every history matched once in each store, 1 otherwise.
"""

import argparse
import glob
import json
import sqlite3
import sys
from collections import defaultdict
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

# REDUCED_PLANCK_MASS_EV of Units/base.py, as printed; used only to select the stored M to 1e-9
M_P_EV = 2.4353235955263046e27


def left_out(col: str) -> bool:
    return col in LEFT_OUT_NAMED or col.endswith("_serial")


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


def value(conn, table, serial):
    col = next(c for c in cols_of(conn, table) if c.startswith("value"))
    r = conn.execute(
        f'SELECT "{col}" FROM "{table}" WHERE serial = ?', (serial,)
    ).fetchone()
    return None if r is None else r[0]


def same(a, b) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, float) or isinstance(b, float):
        try:
            return float(a).hex() == float(b).hex()
        except (TypeError, ValueError):
            return False
    return a == b


def diff_size(a, b):
    try:
        return abs(float(a) - float(b))
    except (TypeError, ValueError):
        return "n/a"


def histories(stem):
    """[(shard path, ScalarHistory row dict, M_eV, beta, phi, alpha)] over every shard."""
    out = []
    for path in sorted(glob.glob(glob.escape(stem) + "-shard*.db")):
        conn = ro(path)
        tabs = tables_of(conn)
        if "ScalarHistory" not in tabs:
            conn.close()
            continue
        has_alpha = "alpha_value" in tabs and "alpha_serial" in cols_of(
            conn, "ScalarHistory"
        )
        for r in conn.execute('SELECT * FROM "ScalarHistory"'):
            r = dict(r)
            em = dict(
                conn.execute(
                    'SELECT * FROM "ExponentialModel" WHERE serial = ?',
                    (r["model_serial"],),
                ).fetchone()
            )
            M = value(conn, "M_value", em["M_serial"])
            beta = value(conn, "beta_value", em["beta_serial"])
            phi = value(conn, "phi_value", r["phi_Einstein_init_serial"])
            alpha = value(conn, "alpha_value", r["alpha_serial"]) if has_alpha else 0.0
            out.append((path, r, M, beta, phi, alpha))
        conn.close()
    return out


def children(path, table, history_serial):
    conn = ro(path)
    if table not in tables_of(conn):
        conn.close()
        return []
    rows = [
        dict(r)
        for r in conn.execute(
            f'SELECT * FROM "{table}" WHERE scalar_history_serial = ?',
            (history_serial,),
        )
    ]
    conn.close()
    return rows


def samples(path, table, key, serial):
    """{ln(1 + z): row} of a value table, through z_serial to this shard's redshift table."""
    conn = ro(path)
    if table not in tables_of(conn):
        conn.close()
        return {}, []
    q = (
        f'SELECT v.*, z.log_one_plus_z AS "__x" FROM "{table}" v '
        f'JOIN "redshift" z ON z.serial = v.z_serial WHERE v."{key}" = ?'
    )
    out = {}
    dup = []
    for r in conn.execute(q, (serial,)):
        r = dict(r)
        x = r.pop("__x")
        if x in out:
            dup.append(x)
        out[x] = r
    conn.close()
    return out, dup


class Tally:
    def __init__(self):
        self.compared = defaultdict(int)
        self.diffs = defaultdict(dict)  # table -> col -> (count, worst, history)
        self.notes = []

    def cmp_row(self, table, hist, new, old, cols):
        for c in cols:
            a, b = new.get(c), old.get(c)
            self.compared[table] += 1
            if not same(a, b):
                d = diff_size(a, b)
                n, worst, h = self.diffs[table].get(c, (0, None, None))
                if worst is None or (
                    isinstance(d, float) and (not isinstance(worst, float) or d > worst)
                ):
                    worst, h = d, hist
                self.diffs[table][c] = (n + 1, worst, h)


def common_cols(new_cols, old_cols):
    both = [c for c in new_cols if c in old_cols]
    return [c for c in both if not left_out(c)], [c for c in both if left_out(c)]


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("new")
    p.add_argument("old")
    p.add_argument("--M-Mp", type=float, default=1e-3)
    p.add_argument("--phi-Mp", type=float, default=5.0)
    p.add_argument("--betas", type=float, nargs="+", default=[0.5, 1.5, 3.0, 5.5])
    args = p.parse_args(argv)

    print(f"new store: {args.new}")
    print(f"old store: {args.old}")
    print(
        "left out, by name: serial and every *_serial column; "
        + "; ".join(LEFT_OUT_NAMED[1:])
    )

    new_h, old_h = histories(args.new), histories(args.old)
    # M as each store holds it: match on the old store's M of the given M_Mp, by the ratio of the
    # two stores' M values for the same model (both are M_Mp * PlanckMass / eV, computed the same way)
    ok = True
    tally = Tally()
    schema_reported = set()
    new_only_values = []

    for beta in args.betas:
        olds = [h for h in old_h if h[3] == beta and h[4] == args.phi_Mp]
        news = [
            h for h in new_h if h[3] == beta and h[4] == args.phi_Mp and h[5] == 0.0
        ]

        # restrict to the requested M: a stored M (in eV) within 1e-9 of M_Mp times the reduced
        # Planck mass in eV (REDUCED_PLANCK_MASS_EV of Units/base.py, written out, since this script
        # imports nothing of the repository); the two stores' M are then required equal bitwise
        def pick(hs):
            return [h for h in hs if abs(h[2] / (args.M_Mp * M_P_EV) - 1.0) < 1.0e-9]

        olds, news = pick(olds), pick(news)
        tag = f"beta={beta:g} M={args.M_Mp:g} phi*={args.phi_Mp:g}"
        print()
        print(f"=== {tag}: {len(news)} new row(s) at alpha = 0, {len(olds)} old row(s)")
        if len(news) != 1 or len(olds) != 1:
            print("    MATCH FAILED")
            ok = False
            continue
        npath, nrow, nM, *_ = news[0]
        opath, orow, oM, *_ = olds[0]
        print(
            f"    new {npath.rsplit('-', 1)[-1]} serial {nrow['serial']} ({nrow['label']})\n"
            f"    old {opath.rsplit('-', 1)[-1]} serial {orow['serial']} ({orow['label']})"
        )
        print(
            f"    M_value as stored: new {nM!r} ({float(nM).hex()}), old {oM!r} ({float(oM).hex()}): "
            f"{'equal' if same(nM, oM) else 'DIFFERENT'}"
        )
        if not same(nM, oM):
            ok = False

        # ScalarHistory
        cmp, lo = common_cols(list(nrow), list(orow))
        cmp_nox = [c for c in cmp if c != "extra_data"]
        new_only = [c for c in nrow if c not in orow]
        old_only = [c for c in orow if c not in nrow]
        if "ScalarHistory" not in schema_reported:
            schema_reported.add("ScalarHistory")
            print(
                f"    ScalarHistory: compared columns ({len(cmp_nox)} + extra_data): {', '.join(cmp_nox)}"
            )
            print(f"    ScalarHistory: left out, held by both: {', '.join(lo)}")
            print(
                f"    ScalarHistory: new columns, not compared: {', '.join(new_only)}"
            )
            print(
                f"    ScalarHistory: old-only columns: {', '.join(old_only) or 'none'}"
            )
        tally.cmp_row("ScalarHistory", tag, nrow, orow, cmp_nox)
        nx = json.loads(nrow["extra_data"]) if nrow.get("extra_data") else {}
        ox = json.loads(orow["extra_data"]) if orow.get("extra_data") else {}
        both = [k for k in nx if k in ox]
        tally.cmp_row("ScalarHistory.extra_data", tag, nx, ox, both)
        print(
            f"    extra_data: keys compared {both}; only new {[k for k in nx if k not in ox]}; "
            f"only old {[k for k in ox if k not in nx]}"
        )
        new_only_values.append((tag, {c: nrow[c] for c in new_only}))

        # ScalarHistoryValue
        ns, nd = samples(npath, "ScalarHistoryValue", "history_serial", nrow["serial"])
        os_, od = samples(opath, "ScalarHistoryValue", "history_serial", orow["serial"])
        xs = [x for x in ns if x in os_]
        print(
            f"    ScalarHistoryValue: new {len(ns)} samples, old {len(os_)}, matched {len(xs)} by ln(1+z); "
            f"only new {len(ns) - len(xs)}, only old {len(os_) - len(xs)}; duplicate z new {len(nd)} old {len(od)}"
        )
        if len(xs) != len(ns) or len(xs) != len(os_) or nd or od:
            ok = False
        if xs:
            c2, lo2 = common_cols(list(ns[xs[0]]), list(os_[xs[0]]))
            if "ScalarHistoryValue" not in schema_reported:
                schema_reported.add("ScalarHistoryValue")
                print(f"    ScalarHistoryValue: compared columns: {', '.join(c2)}")
                print(f"    ScalarHistoryValue: left out: {', '.join(lo2)}")
                print(
                    f"    ScalarHistoryValue: new columns, not compared: "
                    f"{', '.join(c for c in ns[xs[0]] if c not in os_[xs[0]]) or 'none'}"
                )
            for x in xs:
                tally.cmp_row("ScalarHistoryValue", tag, ns[x], os_[x], c2)
            nz = [ns[x].get("rho_chi_over_rho_rad") for x in ns]
            if "rho_chi_over_rho_rad" in ns[xs[0]]:
                print(
                    f"    ScalarHistoryValue.rho_chi_over_rho_rad (new only): "
                    f"{sum(1 for v in nz if v == 0.0)} of {len(nz)} samples are 0.0"
                )

        # AdiabaticHistory, BBNData and their samples
        for table, vtable, vkey in (
            ("AdiabaticHistory", "AdiabaticHistoryValue", "history_serial"),
            ("BBNData", "BBNDataValue", "bbn_serial"),
        ):
            nc = children(npath, table, nrow["serial"])
            oc = children(opath, table, orow["serial"])
            print(f"    {table}: new {len(nc)} row(s), old {len(oc)}")
            if len(nc) != 1 or len(oc) != 1:
                ok = False
                continue
            c3, lo3 = common_cols(list(nc[0]), list(oc[0]))
            if table not in schema_reported:
                schema_reported.add(table)
                print(f"    {table}: compared columns: {', '.join(c3)}")
                print(f"    {table}: left out: {', '.join(lo3)}")
                print(
                    f"    {table}: new columns, not compared: "
                    f"{', '.join(c for c in nc[0] if c not in oc[0]) or 'none'}"
                )
            tally.cmp_row(table, tag, nc[0], oc[0], c3)
            nv, nvd = samples(npath, vtable, vkey, nc[0]["serial"])
            ov, ovd = samples(opath, vtable, vkey, oc[0]["serial"])
            xs = [x for x in nv if x in ov]
            print(
                f"    {vtable}: new {len(nv)}, old {len(ov)}, matched {len(xs)} by ln(1+z); "
                f"duplicate z new {len(nvd)} old {len(ovd)}"
            )
            if len(xs) != len(nv) or len(xs) != len(ov) or nvd or ovd:
                ok = False
            if xs:
                c4, lo4 = common_cols(list(nv[xs[0]]), list(ov[xs[0]]))
                if vtable not in schema_reported:
                    schema_reported.add(vtable)
                    print(f"    {vtable}: compared columns: {', '.join(c4)}")
                    print(f"    {vtable}: left out: {', '.join(lo4)}")
                for x in xs:
                    tally.cmp_row(vtable, tag, nv[x], ov[x], c4)

    print()
    print("New ScalarHistory columns at alpha = 0 (listed, not compared):")
    for tag, vals in new_only_values:
        print(f"  {tag}: " + " ".join(f"{k}={v!r}" for k, v in vals.items()))
    print()
    print("Values compared, per table:")
    total = 0
    for t, n in tally.compared.items():
        print(f"  {t}: {n}")
        total += n
    print(f"  total: {total}")
    print()
    if tally.diffs:
        print("DIFFERENCES:")
        for t, cols in tally.diffs.items():
            for c, (n, worst, h) in cols.items():
                print(
                    f"  {t}.{c}: {n} value(s) differ; largest |difference| {worst} at {h}"
                )
        ok = False
    else:
        print("DIFFERENCES: none")
    print(
        f"RESULT: {'every compared value bitwise equal' if ok else 'NOT EQUAL (see above)'}"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
