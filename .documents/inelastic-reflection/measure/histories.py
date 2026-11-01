"""
The stored-values reader of inelastic-reflection prompt 11 (§2.6), adapted from
`jordan-normalization` prompt 09's `histories.py` (committed there as
`.documents/jordan-normalization/out/09-histories.py.txt`) to read alpha and the dissipation
columns. Stdlib only.

    ./venv/bin/python .documents/inelastic-reflection/measure/histories.py STORE_STEM

STORE_STEM is the store's path without `.db` (e.g. var/datastores/inelastic-reflection-11-reference).
Every shard (`<stem>-shard*.db`) is read through sqlite3 with `file:...?mode=ro` URIs only; the
primary is not opened, and no file is written. Its output goes to stdout; prompt 11 sends it to a
file under var/inelastic-reflection-11/.

Per ScalarHistory row it prints what 09's reader printed (M as stored in eV, n, beta, phi*, pi*,
T*, the three tolerances, validated, failure, failure_reason, z_samples, compute_time,
compute_steps, timestamp, and the fifteen TUNING_OUTPUT_KEYS columns, floats with float.hex), and
also: alpha (alpha_value.value; a shard with no alpha_value table holds alpha = 0 only, log 10
D1), RHS_evaluations, the first bounce, the fixed-temperature values, the 24 dissipation columns
(the ln T_J ones as stored, in GeV) and extra_data. It counts the rows of ExponentialModel,
ScalarHistory, AdiabaticHistory and BBNData and gives each BBNData row's failure, Yp, D/H and
reason. Last, one `JSON {...}` line per ScalarHistory row, for the comparisons.
"""

import glob
import json
import sqlite3
import sys
from urllib.parse import quote

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

OTHER_KEYS = (
    "RHS_evaluations",
    "first_bounce_N",
    "first_bounce_log_T_Jordan",
    "first_bounce_phi_Einstein",
    "first_bounce_reflected",
    "phi_Einstein_1MeV",
    "density_NP_ratio_1MeV",
    "phi_Einstein_70keV",
    "density_NP_ratio_70keV",
)

DISSIPATION_KEYS = (
    "injections",
    "resolved_candidates",
    "unresolved_candidates",
    "injected_fraction",
    "rho_chi_ratio_1MeV",
    "rho_chi_ratio_70keV",
    "rho_chi_ratio_today",
    "Delta_N_eff_1MeV",
    "Delta_N_eff_today",
    "log_R_first_impact",
    "parking_N",
    "parking_log_T_Jordan",
    "release_N",
    "release_log_T_Jordan",
    "worst_birth_margin",
    "worst_birth_margin_N",
    "birth_failing_share",
    "worst_reflection_margin",
    "worst_reflection_margin_N",
    "reflection_failing_share",
    "worst_tracker_margin",
    "worst_tracker_margin_N",
    "tracker_failing_share",
    "unassessed_share",
)


def ro(path):
    return sqlite3.connect("file:" + quote(path, safe="/") + "?mode=ro", uri=True)


def h(x):
    if x is None:
        return "NULL"
    if isinstance(x, float):
        return f"{x!r} ({x.hex()})"
    return repr(x)


def value_col(conn, table):
    cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{table}")')]
    vs = [c for c in cols if c.startswith("value")]
    return vs[0]


def one(conn, table, serial, col=None):
    col = col or value_col(conn, table)
    r = conn.execute(
        f'SELECT "{col}" FROM "{table}" WHERE serial = ?', (serial,)
    ).fetchone()
    return None if r is None else r[0]


def main(argv):
    stem = argv[0]
    shards = sorted(glob.glob(glob.escape(stem) + "-shard*.db"))
    print(f"store {stem}: {len(shards)} shard files")
    rows = []
    counts = {
        "ExponentialModel": 0,
        "ScalarHistory": 0,
        "AdiabaticHistory": 0,
        "BBNData": 0,
    }
    bbn = []
    for path in shards:
        conn = ro(path)
        conn.row_factory = sqlite3.Row
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        for t in counts:
            if t in tables:
                counts[t] += conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        if "BBNData" in tables:
            for r in conn.execute(
                "SELECT b.serial, b.scalar_history_serial, b.failure, b.failure_reason, b.validated, "
                "b.Yp_BBN, b.DOverH, b.small_network, b.z_samples, b.timestamp, s.label "
                'FROM "BBNData" b LEFT JOIN "ScalarHistory" s ON s.serial = b.scalar_history_serial'
            ):
                bbn.append((path.rsplit("-", 1)[-1], dict(r)))
        if "ScalarHistory" not in tables:
            conn.close()
            continue
        sh_cols = {r[1] for r in conn.execute('PRAGMA table_info("ScalarHistory")')}
        for r in conn.execute('SELECT * FROM "ScalarHistory" ORDER BY serial'):
            r = dict(r)
            em = conn.execute(
                'SELECT * FROM "ExponentialModel" WHERE serial = ?',
                (r["model_serial"],),
            ).fetchone()
            em = dict(em) if em is not None else {}
            M = one(conn, "M_value", em.get("M_serial")) if em else None
            beta = one(conn, "beta_value", em.get("beta_serial")) if em else None
            phi = one(conn, "phi_value", r["phi_Einstein_init_serial"])
            pi = one(conn, "pi_value", r["pi_Einstein_init_serial"])
            T = one(conn, "temperature", r["T_Jordan_init_serial"])
            atol = one(conn, "tolerance", r["atol_serial"], "log10_tol")
            rtol = one(conn, "tolerance", r["rtol_serial"], "log10_tol")
            ttol = one(conn, "tolerance", r["tuning_tol_serial"], "log10_tol")
            if "alpha_serial" in sh_cols and "alpha_value" in tables:
                alpha = one(conn, "alpha_value", r["alpha_serial"])
            else:
                alpha = 0.0
            rec = {
                "shard": path.rsplit("-", 1)[-1],
                "serial": r["serial"],
                "label": r["label"],
                "model_type": r["model_type"],
                "model_serial": r["model_serial"],
                "M_value_eV": M,
                "n_value": em.get("n_value"),
                "beta": beta,
                "alpha": alpha,
                "phi_init_Mp": phi,
                "pi_init_Mp": pi,
                "T_init_GeV": T,
                "log10_atol": atol,
                "log10_rtol": rtol,
                "log10_tuning_tol": ttol,
                "validated": r["validated"],
                "failure": r["failure"],
                "failure_reason": r["failure_reason"],
                "z_samples": r["z_samples"],
                "compute_time": r["compute_time"],
                "compute_steps": r["compute_steps"],
                "timestamp": r["timestamp"],
            }
            for k in TUNING_OUTPUT_KEYS + OTHER_KEYS + DISSIPATION_KEYS:
                rec[k] = r.get(k)
            rec["extra_data"] = r.get("extra_data")
            rows.append(rec)
        conn.close()

    rows.sort(
        key=lambda x: (
            x["alpha"] or 0,
            x["M_value_eV"] or 0,
            x["beta"] or 0,
            x["phi_init_Mp"] or 0,
        )
    )
    for rec in rows:
        print()
        print(f"ScalarHistory {rec['shard']} serial {rec['serial']}: {rec['label']}")
        print(f"  model_type {rec['model_type']} model_serial {rec['model_serial']}")
        for k in (
            "M_value_eV",
            "n_value",
            "beta",
            "alpha",
            "phi_init_Mp",
            "pi_init_Mp",
            "T_init_GeV",
        ):
            print(f"  {k} = {h(rec[k])}")
        for k in ("log10_atol", "log10_rtol", "log10_tuning_tol"):
            v = rec[k]
            print(
                f"  {k} = {h(v)}  -> {10.0 ** v!r}"
                if v is not None
                else f"  {k} = NULL"
            )
        for k in (
            "validated",
            "failure",
            "failure_reason",
            "z_samples",
            "compute_time",
            "compute_steps",
            "timestamp",
        ):
            print(f"  {k} = {h(rec[k])}")
        for k in TUNING_OUTPUT_KEYS + OTHER_KEYS + DISSIPATION_KEYS:
            print(f"  {k} = {h(rec[k])}")
        print(f"  extra_data = {rec['extra_data']}")

    print()
    print("counts: " + ", ".join(f"{k} {v}" for k, v in counts.items()))
    print(f"BBNData rows: {len(bbn)}")
    for shard, b in sorted(bbn, key=lambda x: (x[1]["label"] or "")):
        print(
            f"  {shard} serial {b['serial']} history {b['scalar_history_serial']} ({b['label']}): "
            f"failure {b['failure']} validated {b['validated']} small_network {b['small_network']} "
            f"z_samples {b['z_samples']} Yp {b['Yp_BBN']!r} D/H {b['DOverH']!r} "
            f"timestamp {b['timestamp']} reason {b['failure_reason']!r}"
        )
    print()
    for rec in rows:
        out = {}
        for k, v in rec.items():
            out[k] = {"hex": v.hex(), "repr": repr(v)} if isinstance(v, float) else v
        print("JSON " + json.dumps(out, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
