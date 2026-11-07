"""
The gate Bitwise of one-loop-drain prompt 06 §2.1 (P53): the fixed-law histories of a build at the campaign's base
(`1561b60`, format 6) against the same histories of a build at the tip (format 7). Adapted from inelastic-reflection prompt
11's `compare_jn09.py` (.documents/inelastic-reflection/measure/compare_jn09.py).

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/rehearse-06/compare_base.py BASE_STEM TIP_STEM

BASE_STEM and TIP_STEM are the stores' paths without `.db`. Run from the main checkout's root. Every shard
(`<stem>-shard*.db`) is read through sqlite3 with `file:...?mode=ro` URIs only; no primary is opened, nothing is written
but stdout (prompt 06 sends it to a file under var/one-loop-drain/06/), and nothing starts Ray. It imports the tip's
ComputeTargets, CosmologyModels and Units only for the masses' check below.

- **Matched:** each history of the base store by (M, beta, phi*, alpha) through its model's row (M_value.value_eV,
  beta_value.value), phi_value and alpha_value, and in the tip store under alpha_value.drain_model = `fixed`; M, beta, phi*
  and alpha are matched by float.hex. Every base history must be matched exactly once in the tip store, and every fixed
  history of the tip store exactly once by a base history. A sample of `ScalarHistoryValue` by its ln(1 + z), through
  `z_serial` to its own shard's redshift table; `AdiabaticHistory` and `BBNData` by their history, and their samples by
  ln(1 + z) the same way.
- **Compared bitwise:** every column both schemas hold in `ScalarHistory`, `ScalarHistoryValue`, `AdiabaticHistory`,
  `AdiabaticHistoryValue`, `BBNData` and `BBNDataValue`; floats by `float.hex`, the rest exactly, NULL equal only to NULL.
  `extra_data` is JSON, compared key by key over the keys both hold (a key one side only holds is listed). **The key
  tables:** the rows each matched history references (the cosmology, ExponentialModel and its M_value and beta_value, the
  three tolerance rows, alpha_value, phi_value, pi_value, temperature, IntegrationSolver), every column both hold but
  `serial`, `timestamp` and `version` (IntegrationSolver's `label` is its key, and is compared).
- **Left out, by name** (printed): `serial` and every `*_serial` column, `version`, `label` (of the history tables),
  `timestamp`, and the timing columns compute_time, mean_RHS_time, max_RHS_time, min_RHS_time, NP_compute_time,
  BBN_compute_time.
- **The tip's new columns** at each matched history, against prompt 03's rules: alpha_value.drain_model is `fixed`;
  every drain-record column of ScalarHistory is NULL, `bounce_rows` included (03: NULL under the fixed law); the history
  has no row in ScalarHistoryBounce; log_m_eff_Mp and log_m_tracker_Mp at every sample are counted filled and NULL, and
  each sample's pair is recomputed by ComputeTargets.ScalarHistory.sample_masses on the model realized at the stored
  normalization and phi_ref, in Planck_units (main.py's), from the sample's columns read back as ObjectFactories reads them:
  a NULL must be a None of sample_masses and a filled value a float (the count of filled values equal to the
  recomputation bit for bit, and the largest relative difference, are reported as measurements).
- **The exceptions** (README §8.14): a history the base stores as a failure and the tip as a success is listed with both
  reasons, and not compared; a history that fails at both with equal reasons is a match.
- **The count:** the number of values compared, per table.

Exit 0 if every compared value is equal, every history matched once, and every new column obeys 03's rules; 1 otherwise.
"""

import argparse
import contextlib
import glob
import io
import json
import math
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

REGIMES = ("one_loop", "gap", "classical", "unassessed")
DRAIN_COLUMNS = (
    tuple(f"drained_share_{r}" for r in REGIMES)
    + ("parking_share",)
    + tuple(f"candidates_{r}" for r in REGIMES)
    + ("ell_min", "log_R_min_drain", "unresolvable_assessments", "unresolved_declined")
    + tuple(
        f"{p}{k}{s}"
        for k in ("birth", "reflection", "tracker")
        for p, s in (
            ("all_worst_", "_margin"),
            ("all_worst_", "_margin_N"),
            ("all_", "_failing_share"),
        )
    )
    + ("bounce_rows",)
)
MASS_COLUMNS = ("log_m_eff_Mp", "log_m_tracker_Mp")


def left_out(col):
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


def hx(x):
    return float(x).hex() if isinstance(x, (int, float)) and x is not None else repr(x)


def histories(stem):
    out = []
    for path in sorted(glob.glob(glob.escape(stem) + "-shard*.db")):
        conn = ro(path)
        tabs = tables_of(conn)
        if "ScalarHistory" not in tabs:
            conn.close()
            continue
        has_model = "drain_model" in cols_of(conn, "alpha_value")
        for r in conn.execute('SELECT * FROM "ScalarHistory"'):
            r = dict(r)
            em = row(conn, "ExponentialModel", r["model_serial"])
            a = row(conn, "alpha_value", r["alpha_serial"])
            out.append(
                SimpleNamespace(
                    path=path,
                    row=r,
                    em=em,
                    M=value(conn, "M_value", em["M_serial"]),
                    beta=value(conn, "beta_value", em["beta_serial"]),
                    phi=value(conn, "phi_value", r["phi_Einstein_init_serial"]),
                    alpha=a["value"],
                    model=a["drain_model"] if has_model else "fixed",
                )
            )
        conn.close()
    return out


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


def children(path, table, serial):
    conn = ro(path)
    if table not in tables_of(conn):
        conn.close()
        return []
    rows = [
        dict(r)
        for r in conn.execute(
            f'SELECT * FROM "{table}" WHERE scalar_history_serial = ?', (serial,)
        )
    ]
    conn.close()
    return rows


class Tally:
    def __init__(self):
        self.compared = defaultdict(int)
        self.diffs = defaultdict(dict)

    def cmp_row(self, table, tag, a_row, b_row, cols):
        for c in cols:
            a, b = a_row.get(c), b_row.get(c)
            self.compared[table] += 1
            if not same(a, b):
                n, first = self.diffs[table].get(c, (0, None))
                self.diffs[table][c] = (n + 1, first or f"{tag}: tip {a!r} base {b!r}")


def common(a_cols, b_cols, lo=left_out):
    both = [c for c in a_cols if c in b_cols]
    return [c for c in both if not lo(c)], [c for c in both if lo(c)]


class Masses:
    """sample_masses on read-back samples, the model realized at the stored tuning, in Planck_units (main.py's)."""

    def __init__(self):
        from math import log

        from CosmologyConcepts import M_value, beta_value
        from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
        from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
        from CosmologyModels.LambdaCDM import Planck2018
        from Units import Planck_units
        import importlib

        # by the module's name: ComputeTargets exports the class ScalarHistory under the same attribute
        SM = importlib.import_module("ComputeTargets.ScalarHistory")

        self.SM, self.ExponentialModel, self.M_value, self.beta_value = (
            SM,
            ExponentialModel,
            M_value,
            beta_value,
        )
        self.units = Planck_units()
        with contextlib.redirect_stdout(io.StringIO()):
            self.cosmology = QCD_Cosmology(0, self.units, Planck2018())
        self.log_Mp = log(self.units.PlanckMass)
        self.log_GeV = log(self.units.GeV)

    def __call__(self, h, svals):
        u = self.units
        # M as stored (eV) back to the model's units, as ObjectFactories' M_value reads it
        model = self.ExponentialModel(
            0,
            self.M_value(0, h.M * u.eV),
            h.em["n_value"],
            self.beta_value(0, h.beta),
            u,
        )
        rm = model.realize(h.row["normalization"], h.row["phi_ref"] * u.PlanckMass)
        ss = [
            SimpleNamespace(
                phi_Einstein=r["phi_Einstein_Mp"] * u.PlanckMass,
                pi_Einstein=r["pi_Einstein_Mp"] * u.PlanckMass,
                log_rhorad_Einstein=r["log_rhorad_Einstein_Mp4"] + 4.0 * self.log_Mp,
                log_fm=r["log_fm"],
                log_T_Jordan=r["log_T_Jordan_GeV"] + self.log_GeV,
                Sigma=r["Sigma"],
            )
            for r in svals
        ]
        f, t = self.SM.sample_masses(rm, self.cosmology, ss)
        # to the columns' unit, ln(m/M_P)
        return [None if v is None else v - self.log_Mp for v in f], [
            None if v is None else v - self.log_Mp for v in t
        ]


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("base")
    p.add_argument("tip")
    args = p.parse_args(argv)

    print(f"base store: {args.base}")
    print(f"tip store:  {args.tip}")
    print(
        "left out, by name: serial and every *_serial column; "
        + "; ".join(LEFT_OUT_NAMED[1:])
    )
    print("key tables: every column both hold but serial, timestamp, version")

    base_h = histories(args.base)
    tip_h = histories(args.tip)
    tip_fixed = [h for h in tip_h if h.model == "fixed"]
    print(
        f"base: {len(base_h)} histories (models {sorted({h.model for h in base_h})}); "
        f"tip: {len(tip_h)} histories, {len(tip_fixed)} under fixed"
    )

    ok = True
    tally = Tally()
    reported = set()
    exceptions = []
    matched_tip = defaultdict(int)
    newcol = {"drain_model_fixed": 0, "drain_record_null": 0, "no_bounce_rows": 0}
    mass = {
        "samples": 0,
        "filled": {c: 0 for c in MASS_COLUMNS},
        "null": {c: 0 for c in MASS_COLUMNS},
        "null_is_none": 0,
        "null_not_none": 0,
        "filled_is_none": 0,
        "filled_bitwise": 0,
        "filled_maxrel": 0.0,
    }
    masses = Masses()

    def key(h):
        return (
            float(h.M).hex(),
            float(h.beta).hex(),
            float(h.phi).hex(),
            float(h.alpha).hex(),
        )

    for b in sorted(base_h, key=lambda h: (h.alpha, h.M, h.beta)):
        tag = f"alpha={b.alpha:g} M={b.M:.6g}eV beta={b.beta:g} phi*={b.phi:g}"
        cands = [t for t in tip_fixed if key(t) == key(b)]
        print()
        print(f"=== {tag}: {len(cands)} tip row(s) under fixed")
        if len(cands) != 1:
            print("    MATCH FAILED")
            ok = False
            continue
        t = cands[0]
        matched_tip[id(t)] += 1
        print(
            f"    base {b.path.rsplit('-', 1)[-1]} #{b.row['serial']}; tip {t.path.rsplit('-', 1)[-1]} #{t.row['serial']}"
        )
        bf, tf = b.row["failure"], t.row["failure"]
        if bf and not tf:
            exceptions.append((tag, b.row["failure_reason"], t.row["failure_reason"]))
            print(
                f"    EXCEPTION: base failure ({b.row['failure_reason']!r}), tip success; not compared"
            )
            continue
        if bf and tf:
            print(
                f"    failure at both; reasons {'equal' if b.row['failure_reason'] == t.row['failure_reason'] else 'DIFFER'}"
            )

        # the key tables
        bc, tc = ro(b.path), ro(t.path)
        # cosmology_type and model_type are integer type ids (compared as ScalarHistory columns); the store's only
        # cosmology and model tables are QCD_Cosmology and ExponentialModel
        refs = [
            ("cosmology", "QCD_Cosmology", "cosmology_serial", None),
            ("model", "ExponentialModel", "model_serial", None),
        ]
        refs += [(c, tab, c, None) for c, tab in KEY_REFS.items()]
        for name, tab, col, _ in refs:
            tr, br_ = row(tc, tab, t.row[col]), row(bc, tab, b.row[col])
            cc, _lo = common(
                list(tr),
                list(br_),
                lambda c: c in KEY_LEFT_OUT or c.endswith("_serial"),
            )
            tally.cmp_row(f"key:{tab}", tag, tr, br_, cc)
            if f"key:{tab}" not in reported:
                reported.add(f"key:{tab}")
                print(
                    f"    key {tab} (via {col}): compared {', '.join(cc)}; tip only {[c for c in tr if c not in br_]}"
                )
        for col, tab in (("M_serial", "M_value"), ("beta_serial", "beta_value")):
            tr, br_ = row(tc, tab, t.em[col]), row(bc, tab, b.em[col])
            cc, _lo = common(
                list(tr),
                list(br_),
                lambda c: c in KEY_LEFT_OUT or c.endswith("_serial"),
            )
            tally.cmp_row(f"key:{tab}", tag, tr, br_, cc)
            if f"key:{tab}" not in reported:
                reported.add(f"key:{tab}")
                print(
                    f"    key {tab} (via ExponentialModel.{col}): compared {', '.join(cc)}"
                )
        # the drain model
        talpha = row(tc, "alpha_value", t.row["alpha_serial"])
        if talpha.get("drain_model") == "fixed":
            newcol["drain_model_fixed"] += 1
        else:
            print(f"    !! alpha_value.drain_model {talpha.get('drain_model')!r}")
            ok = False

        # ScalarHistory
        cmp, lo = common(list(t.row), list(b.row))
        cmp_nox = [c for c in cmp if c != "extra_data"]
        tip_only = [c for c in t.row if c not in b.row]
        if "ScalarHistory" not in reported:
            reported.add("ScalarHistory")
            print(
                f"    ScalarHistory: compared ({len(cmp_nox)} + extra_data): {', '.join(cmp_nox)}"
            )
            print(f"    ScalarHistory: left out, held by both: {', '.join(lo)}")
            print(f"    ScalarHistory: tip-only columns: {', '.join(tip_only)}")
            print(
                f"    ScalarHistory: base-only columns: {', '.join(c for c in b.row if c not in t.row) or 'none'}"
            )
        tally.cmp_row("ScalarHistory", tag, t.row, b.row, cmp_nox)
        tx = json.loads(t.row["extra_data"]) if t.row.get("extra_data") else {}
        bx = json.loads(b.row["extra_data"]) if b.row.get("extra_data") else {}
        both = [k for k in tx if k in bx]
        tally.cmp_row("ScalarHistory.extra_data", tag, tx, bx, both)
        print(
            f"    extra_data: {len(both)} keys compared; only tip {[k for k in tx if k not in bx]}; "
            f"only base {[k for k in bx if k not in tx]}"
        )
        nn = [c for c in DRAIN_COLUMNS if t.row.get(c) is not None]
        missing = [c for c in DRAIN_COLUMNS if c not in t.row]
        if nn or missing:
            print(f"    !! drain record: not NULL {nn}; missing {missing}")
            ok = False
        else:
            newcol["drain_record_null"] += 1
        nb = tc.execute(
            'SELECT COUNT(*) FROM "ScalarHistoryBounce" WHERE history_serial = ?',
            (t.row["serial"],),
        ).fetchone()[0]
        if nb:
            print(f"    !! {nb} bounce rows")
            ok = False
        else:
            newcol["no_bounce_rows"] += 1
        bc.close()
        tc.close()

        # ScalarHistoryValue
        ts, td = samples(
            t.path, "ScalarHistoryValue", "history_serial", t.row["serial"]
        )
        bs, bd = samples(
            b.path, "ScalarHistoryValue", "history_serial", b.row["serial"]
        )
        xs = [x for x in ts if x in bs]
        print(
            f"    ScalarHistoryValue: tip {len(ts)}, base {len(bs)}, matched {len(xs)}; duplicates tip {len(td)} base {len(bd)}"
        )
        if len(xs) != len(ts) or len(xs) != len(bs) or td or bd:
            ok = False
        if xs:
            c2, lo2 = common(list(ts[xs[0]]), list(bs[xs[0]]))
            if "ScalarHistoryValue" not in reported:
                reported.add("ScalarHistoryValue")
                print(f"    ScalarHistoryValue: compared: {', '.join(c2)}")
                print(f"    ScalarHistoryValue: left out: {', '.join(lo2)}")
                print(
                    f"    ScalarHistoryValue: tip-only: {', '.join(c for c in ts[xs[0]] if c not in bs[xs[0]]) or 'none'}"
                )
            for x in xs:
                tally.cmp_row("ScalarHistoryValue", tag, ts[x], bs[x], c2)
        if ts and not t.row["failure"]:
            order = sorted(ts)
            svals = [ts[x] for x in order]
            f, tr_ = masses(t, svals)
            counts = {c: [0, 0] for c in MASS_COLUMNS}
            for r, fv, tv in zip(svals, f, tr_):
                mass["samples"] += 1
                for c, v in (("log_m_eff_Mp", fv), ("log_m_tracker_Mp", tv)):
                    stored = r[c]
                    if stored is None:
                        counts[c][1] += 1
                        mass["null"][c] += 1
                        if v is None:
                            mass["null_is_none"] += 1
                        else:
                            mass["null_not_none"] += 1
                    else:
                        counts[c][0] += 1
                        mass["filled"][c] += 1
                        if v is None:
                            mass["filled_is_none"] += 1
                        elif float(v).hex() == float(stored).hex():
                            mass["filled_bitwise"] += 1
                        else:
                            rel = abs(v - stored) / max(abs(stored), 1e-300)
                            mass["filled_maxrel"] = max(mass["filled_maxrel"], rel)
            print(
                "    masses: "
                + "; ".join(f"{c} filled {n[0]} NULL {n[1]}" for c, n in counts.items())
            )

        # AdiabaticHistory, BBNData and their samples
        for table, vtable, vkey in (
            ("AdiabaticHistory", "AdiabaticHistoryValue", "history_serial"),
            ("BBNData", "BBNDataValue", "bbn_serial"),
        ):
            tcs, bcs = children(t.path, table, t.row["serial"]), children(
                b.path, table, b.row["serial"]
            )
            print(f"    {table}: tip {len(tcs)} row(s), base {len(bcs)}")
            if len(tcs) != len(bcs) or len(tcs) > 1:
                ok = False
                continue
            if not tcs:
                continue
            c3, lo3 = common(list(tcs[0]), list(bcs[0]))
            if table not in reported:
                reported.add(table)
                print(
                    f"    {table}: compared: {', '.join(c3)}; left out: {', '.join(lo3)}; "
                    f"tip-only: {', '.join(c for c in tcs[0] if c not in bcs[0]) or 'none'}"
                )
            tally.cmp_row(table, tag, tcs[0], bcs[0], c3)
            tv, tvd = samples(t.path, vtable, vkey, tcs[0]["serial"])
            bv, bvd = samples(b.path, vtable, vkey, bcs[0]["serial"])
            xs = [x for x in tv if x in bv]
            print(
                f"    {vtable}: tip {len(tv)}, base {len(bv)}, matched {len(xs)}; duplicates tip {len(tvd)} base {len(bvd)}"
            )
            if len(xs) != len(tv) or len(xs) != len(bv) or tvd or bvd:
                ok = False
            if xs:
                c4, lo4 = common(list(tv[xs[0]]), list(bv[xs[0]]))
                if vtable not in reported:
                    reported.add(vtable)
                    print(
                        f"    {vtable}: compared: {', '.join(c4)}; left out: {', '.join(lo4)}"
                    )
                for x in xs:
                    tally.cmp_row(vtable, tag, tv[x], bv[x], c4)

    unmatched_tip = [t for t in tip_fixed if matched_tip[id(t)] != 1]
    print()
    print(
        f"tip fixed histories matched exactly once: {len(tip_fixed) - len(unmatched_tip)} of {len(tip_fixed)}"
    )
    if unmatched_tip:
        ok = False
        for t in unmatched_tip:
            print(
                f"   !! unmatched tip history alpha={t.alpha:g} M={t.M:.6g} beta={t.beta:g}"
            )
    print()
    print("The tip's new columns at the matched histories:")
    print(f"  alpha_value.drain_model = fixed: {newcol['drain_model_fixed']}")
    print(
        f"  drain record ({len(DRAIN_COLUMNS)} columns, bounce_rows included) all NULL: {newcol['drain_record_null']}"
    )
    print(f"  no ScalarHistoryBounce row: {newcol['no_bounce_rows']}")
    print(
        f"  sample masses over {mass['samples']} samples: "
        + "; ".join(
            f"{c} filled {mass['filled'][c]} NULL {mass['null'][c]}"
            for c in MASS_COLUMNS
        )
    )
    print(
        f"  recomputed by sample_masses: NULL where None {mass['null_is_none']}, NULL where a value {mass['null_not_none']}, "
        f"filled where None {mass['filled_is_none']}; filled equal bit for bit {mass['filled_bitwise']}, "
        f"largest relative difference of the others {mass['filled_maxrel']:.3e} (measured)"
    )
    if mass["null_not_none"] or mass["filled_is_none"]:
        ok = False
    print()
    print(
        "Exceptions (base failure, tip success):" + (" none" if not exceptions else "")
    )
    for tag, br, tr in exceptions:
        print(f"  {tag}: base {br!r}; tip {tr!r}")
    print()
    print("Values compared, per table:")
    total = 0
    for tname, n in sorted(tally.compared.items()):
        print(f"  {tname}: {n}")
        total += n
    print(f"  total: {total}")
    print()
    if tally.diffs:
        print("DIFFERENCES:")
        for tname, cols in tally.diffs.items():
            for c, (n, first) in cols.items():
                print(f"  {tname}.{c}: {n} value(s) differ; first at {first}")
        ok = False
    else:
        print("DIFFERENCES: none")
    print(
        f"RESULT: {'every compared value bitwise equal, new columns as 03 sets them' if ok else 'NOT EQUAL (see above)'}"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
