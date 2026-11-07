"""
The stored-values reader of one-loop-drain prompt 06 (§1.6), adapted from inelastic-reflection prompt 11's
(.documents/inelastic-reflection/measure/histories.py) to read the drain-model key, the history's drain record and the
bounce rows of one-loop-drain prompt 03. Stdlib only.

    ./venv/bin/python .documents/one-loop-drain/rehearse-06/histories.py STORE_STEM

STORE_STEM is the store's path without `.db` (e.g. var/datastores/one-loop-drain-06-reference). Every shard
(`<stem>-shard*.db`) is read through sqlite3 with `file:...?mode=ro` URIs only; the primary is not opened, and no file is
written. Its output goes to stdout; prompt 06 sends it to a file under var/one-loop-drain/06/.

One line per ScalarHistory row, sorted by (drain_model, alpha, M, beta), with:
  - its key: drain_model (`fixed` where the store's alpha_value has no drain_model column, a format-6 store), alpha (A under
    the one-loop model), M/M_P (M_value.value_eV over the reduced Planck mass in eV), n, beta, phi*;
  - failure, validated, failure_reason;
  - the tuning's outputs (TUNING_OUTPUT_KEYS) by repr, the integrations (tuning_iterations) and the steps by kind;
  - compute_time (the final integration's) and the tuning wall: the store's `timestamp` less the time in the history's
    label (main.py's datetime.now() at the compute's submission, whole seconds, local time = UTC on 2026-11-07), so that
    it includes the queueing of the compute task;
  - Delta_N_eff today and at 1 MeV, and the BBNData row's Yp and D/H (or its failure);
  - for a one-loop history: the drain record's counts and shares, ell_min, log_R_min_drain, bounce_rows, and the bounce
    rows of ScalarHistoryBounce counted by source and, for the candidate rows, by regime.
Then the counts per table, and one `JSON {...}` line per history with every value above (floats as repr and float.hex).
"""

import glob
import json
import sqlite3
import sys
from collections import Counter
from datetime import datetime
from urllib.parse import quote

# REDUCED_PLANCK_MASS_EV of Units/base.py, as printed; used only to print M in units of M_P
M_P_EV = 2.4353235955263046e27

TUNING_OUTPUT_KEYS = (
    "normalization",
    "V_ref",
    "phi_ref",
    "phi_0",
    "pi_0",
    "tracking",
    "residual_H",
    "residual_phi",
    "tuning_iterations",
    "plain_steps",
    "secant_steps",
    "guard_steps",
    "seed_kind",
    "seed_normalization",
    "seed_phi_ref",
)

REGIMES = ("one_loop", "gap", "classical", "unassessed")
DRAIN_KEYS = (
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


def one(conn, table, serial, col=None):
    if col is None:
        col = next(c for c in cols_of(conn, table) if c.startswith("value"))
    r = conn.execute(
        f'SELECT "{col}" FROM "{table}" WHERE serial = ?', (serial,)
    ).fetchone()
    return None if r is None else r[0]


def label_time(label):
    """The datetime at the end of a history's label (main.py's isoformat, whole seconds), or None."""
    try:
        return datetime.fromisoformat(
            label.rsplit("-", 3)[-3] + "-" + "-".join(label.rsplit("-", 3)[-2:])
        )
    except Exception:  # noqa: BLE001
        return None


def stored_time(ts):
    try:
        t = datetime.fromisoformat(ts)
        return t.replace(tzinfo=None)
    except Exception:  # noqa: BLE001
        return None


def fx(x):
    if x is None:
        return "NULL"
    if isinstance(x, float):
        return f"{x!r}"
    return str(x)


def read(stem):
    shards = sorted(glob.glob(glob.escape(stem) + "-shard*.db"))
    rows = []
    counts = Counter()
    for path in shards:
        conn = ro(path)
        tabs = tables_of(conn)
        for t in (
            "ExponentialModel",
            "ScalarHistory",
            "ScalarHistoryValue",
            "ScalarHistoryBounce",
            "AdiabaticHistory",
            "BBNData",
        ):
            if t in tabs:
                counts[t] += conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        if "ScalarHistory" not in tabs:
            conn.close()
            continue
        sh_cols = set(cols_of(conn, "ScalarHistory"))
        has_model = "alpha_value" in tabs and "drain_model" in cols_of(
            conn, "alpha_value"
        )
        for r in conn.execute('SELECT * FROM "ScalarHistory" ORDER BY serial'):
            r = dict(r)
            em = dict(
                conn.execute(
                    'SELECT * FROM "ExponentialModel" WHERE serial = ?',
                    (r["model_serial"],),
                ).fetchone()
            )
            M = one(conn, "M_value", em["M_serial"])
            rec = {
                "shard": path.rsplit("-", 1)[-1],
                "serial": r["serial"],
                "label": r["label"],
                "drain_model": (
                    one(conn, "alpha_value", r["alpha_serial"], "drain_model")
                    if has_model
                    else "fixed"
                ),
                "alpha": one(conn, "alpha_value", r["alpha_serial"]),
                "M_eV": M,
                "M_Mp": M / M_P_EV,
                "n": em["n_value"],
                "beta": one(conn, "beta_value", em["beta_serial"]),
                "phi_init_Mp": one(conn, "phi_value", r["phi_Einstein_init_serial"]),
                "failure": r["failure"],
                "validated": r["validated"],
                "failure_reason": r["failure_reason"],
                "compute_time": r["compute_time"],
                "timestamp": r["timestamp"],
                "Delta_N_eff_today": r.get("Delta_N_eff_today"),
                "Delta_N_eff_1MeV": r.get("Delta_N_eff_1MeV"),
                "z_samples": r["z_samples"],
            }
            lt, st = label_time(r["label"]), stored_time(r["timestamp"])
            rec["tuning_wall_s"] = (
                (st - lt).total_seconds() if lt is not None and st is not None else None
            )
            for k in TUNING_OUTPUT_KEYS:
                rec[k] = r.get(k)
            for k in DRAIN_KEYS:
                rec[k] = r.get(k) if k in sh_cols else None
            b = (
                conn.execute(
                    'SELECT failure, failure_reason, Yp_BBN, DOverH, small_network FROM "BBNData" WHERE scalar_history_serial = ?',
                    (r["serial"],),
                ).fetchall()
                if "BBNData" in tabs
                else []
            )
            rec["bbn_rows"] = len(b)
            if b:
                (
                    rec["bbn_failure"],
                    rec["bbn_reason"],
                    rec["Yp"],
                    rec["DoH"],
                    rec["small_network"],
                ) = tuple(b[0])
            if "ScalarHistoryBounce" in tabs:
                br = conn.execute(
                    'SELECT source, regime FROM "ScalarHistoryBounce" WHERE history_serial = ? ORDER BY ordinal',
                    (r["serial"],),
                ).fetchall()
                rec["bounce_by_source"] = dict(Counter(x[0] for x in br))
                rec["bounce_by_regime"] = dict(
                    Counter(x[1] for x in br if x[0] == "candidate")
                )
                rec["bounce_rows_found"] = len(br)
            rows.append(rec)
        conn.close()
    rows.sort(key=lambda x: (x["drain_model"], x["alpha"], x["M_Mp"], x["beta"]))
    return shards, rows, counts


def main(argv):
    stem = argv[0]
    shards, rows, counts = read(stem)
    print(f"store {stem}: {len(shards)} shard files; {len(rows)} ScalarHistory rows")
    for rec in rows:
        head = (
            f"{rec['drain_model']} alpha={rec['alpha']:g} M={rec['M_Mp']:.6g}Mp n={rec['n']} beta={rec['beta']:g} "
            f"phi*={rec['phi_init_Mp']:g} [{rec['shard']} #{rec['serial']}]"
        )
        if rec["failure"]:
            print(
                f"{head}: FAILURE validated={rec['validated']} reason={rec['failure_reason']!r} tuning_wall={fx(rec['tuning_wall_s'])} s"
            )
            continue
        tun = " ".join(f"{k}={fx(rec[k])}" for k in TUNING_OUTPUT_KEYS)
        line = (
            f"{head}: validated={rec['validated']} integrations={rec['tuning_iterations']} "
            f"steps p/s/g={rec['plain_steps']}/{rec['secant_steps']}/{rec['guard_steps']} compute_time={fx(rec['compute_time'])} "
            f"tuning_wall={fx(rec['tuning_wall_s'])} s z_samples={rec['z_samples']} "
            f"dNeff_today={fx(rec['Delta_N_eff_today'])} dNeff_1MeV={fx(rec['Delta_N_eff_1MeV'])} "
        )
        if rec.get("bbn_rows"):
            line += (
                f"BBN failure={rec['bbn_failure']} Yp={fx(rec['Yp'])} D/H={fx(rec['DoH'])} small={rec['small_network']}"
                + (f" reason={rec['bbn_reason']!r}" if rec["bbn_failure"] else "")
            )
        else:
            line += "BBN none"
        print(line)
        print(f"    tuning: {tun}")
        if rec["drain_model"] != "fixed":
            print("    drain: " + " ".join(f"{k}={fx(rec[k])}" for k in DRAIN_KEYS))
            print(
                f"    bounce rows: {rec.get('bounce_rows_found')} found; by source {rec.get('bounce_by_source')}; "
                f"candidates by regime {rec.get('bounce_by_regime')}"
            )
        else:
            nn = [k for k in DRAIN_KEYS if rec[k] is not None]
            print(
                f"    drain record: {'all NULL' if not nn else 'NOT NULL: ' + ', '.join(nn)}; bounce rows {rec.get('bounce_rows_found', 'no table')}"
            )
    print()
    print("counts: " + ", ".join(f"{k} {v}" for k, v in sorted(counts.items())))
    print()
    for rec in rows:
        out = {
            k: ({"hex": v.hex(), "repr": repr(v)} if isinstance(v, float) else v)
            for k, v in rec.items()
        }
        print("JSON " + json.dumps(out, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
