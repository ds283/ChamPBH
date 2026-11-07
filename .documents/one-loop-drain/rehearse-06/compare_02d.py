"""
The gate R (ii) of one-loop-drain prompt 06 §2.2 (P54 (ii); orchestrator note 06, rulings 2 and 3): the nine one-loop
histories of a rehearsal store against 02d's tool route. Adapted from inelastic-reflection prompt 11's `compare_tool.py`
(.documents/inelastic-reflection/out/11-compare_tool.py.txt) and from the comparison of candidates in
.documents/one-loop-drain/derive-05/run_05.py.

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/rehearse-06/compare_02d.py STORE_STEM

STORE_STEM is the store's path without `.db`. Run from the main checkout's root. Every shard is read through sqlite3 with
`file:...?mode=ro` URIs only; var/one-loop-drain/02d/tasks.json, sample.jsonl and tasks/<id>.partial.jsonl are read; nothing
is written but stdout (prompt 06 sends it to a file under var/one-loop-drain/06/), and nothing starts Ray. It imports
ObjectFactories.ScalarHistory (to_column, BOUNCE_COLUMNS, the tuning's units) and Units (Planck_units, main.py's units).

0. **The tasks** (prompt §1.3): for each of the nine, tasks.json's (beta, M, A) and its argv, which must be
   `<beta> <M> --drain-law one_loop --alpha <A>` and nothing else (so the tool ran at main.py's defaults).
1. **Matched:** each task's history in the store, by alpha_value (A, `one-loop-measured-v1`), beta and M (M_value.value_eV
   against the task's M times the reduced Planck mass in eV, to 1e-12 relative); exactly one each.
2. **Bitwise, a difference a stop:**
   - the tuning's fifteen keys against the line's `tuning`, through the columns' own conversion (ObjectFactories'
     _tuning_unit: phi_ref, phi_0, pi_0, seed_phi_ref in M_P, V_ref in M_P^4, the others as they are);
   - every field of the line's `accepted.drain_values` against the stored drain record (candidates as the sum of the
     four candidates_<regime>, <regime> as candidates_<regime>, K_share_<regime> as drained_share_<regime>, K_share_parking
     as parking_share, ell_min, unresolvable_assessments, unresolved_declined as named);
   - the candidate rows (source `candidate`) by candidate_index against the last partial line's `candidates` (each a
     DrainCandidate: regime, ell, alpha_eff, w_gate, log_E, log_H, L, log_R) and `injections` (N, alpha_eff, delta_K):
     their number equal to both lists' lengths, their indices 0, 1, ...; each value against
     to_column(value, kind, Planck_units()) with kind from BOUNCE_COLUMNS; the count that also equals the raw value is
     reported, not asserted. The event rows (plateau, wall) are counted, not compared;
   - every field of the line's `accepted.dissipation` against the stored column (parking_log_T_Jordan_GeV as
     parking_log_T_Jordan, release likewise: both already in GeV), Delta_N_eff today and at 1 MeV among them.
3. **Yp and D/H** (ruling 3): the line's `bbn` Yp and DoH against BBNData's Yp_BBN and DOverH at full precision: equal, or
   the relative difference reported; above 5e-10 relative a stop.
Floats by float.hex, None only with NULL, strings and integers exactly. Exit 0 if every bitwise item is equal, every
history matched once and Yp and D/H are within 5e-10 relative; 1 otherwise.
"""

import glob
import json
import sqlite3
import sys
from pathlib import Path
from urllib.parse import quote

from ObjectFactories.ScalarHistory import BOUNCE_COLUMNS, _tuning_unit, to_column
from Units import Planck_units

ROOT = Path(__file__).resolve().parents[3]
D02 = ROOT / "var" / "one-loop-drain" / "02d"
M_P_EV = 2.4353235955263046e27  # REDUCED_PLANCK_MASS_EV of Units/base.py, used only to select M
MODEL = "one-loop-measured-v1"
TASKS = (
    "P-2188-A0.1",
    "P-2188-A1",
    "P-2197-A0.1",
    "P-2197-A1",
    "R-865-A0.1",
    "R-865-A1",
    "R-611-A0.1",
    "R-611-A1",
    "C-3233-A1",
)
TUNING_KEYS = (
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
CAND_FIELDS = (
    "regime",
    "ell",
    "alpha_eff",
    "w_gate",
    "log_E",
    "log_H",
    "L",
    "log_R",
)  # DrainCandidate's order
INJ_FIELDS = ("N", "alpha_eff", "delta_K")
KIND = {f: (col, kind) for f, col, kind in BOUNCE_COLUMNS}
YP_DH_LIMIT = 5.0e-10


def ro(path):
    c = sqlite3.connect("file:" + quote(path, safe="/") + "?mode=ro", uri=True)
    c.row_factory = sqlite3.Row
    return c


def same(a, b):
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, float) or isinstance(b, float):
        try:
            return float(a).hex() == float(b).hex()
        except (TypeError, ValueError):
            return False
    return a == b


def find(stem, task):
    out = []
    for path in sorted(glob.glob(glob.escape(stem) + "-shard*.db")):
        c = ro(path)
        tabs = {
            r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if "ScalarHistory" not in tabs:
            c.close()
            continue
        q = (
            'SELECT h.* , m.value_eV AS __M, b.value AS __beta FROM "ScalarHistory" h '
            'JOIN "alpha_value" a ON a.serial = h.alpha_serial '
            'JOIN "ExponentialModel" e ON e.serial = h.model_serial '
            'JOIN "M_value" m ON m.serial = e.M_serial JOIN "beta_value" b ON b.serial = e.beta_serial '
            "WHERE a.drain_model = ? AND a.value = ?"
        )
        for r in c.execute(q, (MODEL, task["A"])):
            r = dict(r)
            if (
                r["__beta"] == task["beta"]
                and abs(r["__M"] / (task["M"] * M_P_EV) - 1.0) < 1e-12
            ):
                bounces = [
                    dict(x)
                    for x in c.execute(
                        'SELECT * FROM "ScalarHistoryBounce" WHERE history_serial = ? ORDER BY ordinal',
                        (r["serial"],),
                    )
                ]
                bbn = [
                    dict(x)
                    for x in c.execute(
                        'SELECT * FROM "BBNData" WHERE scalar_history_serial = ?',
                        (r["serial"],),
                    )
                ]
                out.append((path, r, bounces, bbn))
        c.close()
    return out


def main(argv):
    stem = argv[0]
    units = Planck_units()
    tasks = {t["id"]: t for t in json.loads((D02 / "tasks.json").read_text())}
    lines = {}
    for raw in (D02 / "sample.jsonl").read_text().splitlines():
        line = json.loads(raw)
        lines[line["id"]] = line
    ok = True
    print(f"store {stem}; 02d at {D02}")
    print()
    print("0. the tasks (tasks.json):")
    for tid in TASKS:
        t = tasks[tid]
        argv_ok = (
            len(t["argv"]) == 6
            and float(t["argv"][0]) == t["beta"]
            and float(t["argv"][1]) == t["M"]
            and t["argv"][2:5] == ["--drain-law", "one_loop", "--alpha"]
            and float(t["argv"][5]) == t["A"]
        )
        ok &= argv_ok
        print(
            f"   {tid}: beta={t['beta']!r} M={t['M']!r} A={t['A']!r} argv={t['argv']} "
            f"{'only --drain-law one_loop --alpha A' if argv_ok else 'OTHER OPTIONS'}; 02d outcome {lines[tid]['outcome']}"
        )

    totals = {
        "tuning": 0,
        "drain_values": 0,
        "candidate_values": 0,
        "candidate_raw_equal": 0,
        "dissipation": 0,
    }
    worst = {"Yp": (0.0, None), "DoH": (0.0, None)}
    diffs = []
    for tid in TASKS:
        t, line = tasks[tid], lines[tid]
        partial = json.loads(
            (D02 / "tasks" / f"{tid}.partial.jsonl").read_text().splitlines()[-1]
        )
        print()
        print(f"=== {tid} (beta={t['beta']:g} M={t['M']:g} A={t['A']:g})")
        found = find(stem, t)
        if len(found) != 1:
            print(f"    MATCH FAILED: {len(found)} rows")
            ok = False
            continue
        path, r, bounces, bbn = found[0]
        print(
            f"    {path.rsplit('-', 1)[-1]} #{r['serial']} failure={r['failure']} validated={r['validated']}"
        )
        n = 0
        # tuning
        for k in TUNING_KEYS:
            v = line["tuning"][k]
            unit = _tuning_unit(k, units)
            exp = v / unit if (v is not None and unit is not None) else v
            n += 1
            if not same(r[k], exp):
                diffs.append(f"{tid} tuning {k}: stored {r[k]!r} line {exp!r}")
        totals["tuning"] += n
        # drain values
        dv = line["accepted"]["drain_values"]
        mapping = {"candidates": sum(r[f"candidates_{g}"] for g in REGIMES)}
        for g in REGIMES:
            mapping[g] = r[f"candidates_{g}"]
            mapping[f"K_share_{g}"] = r[f"drained_share_{g}"]
        mapping["K_share_parking"] = r["parking_share"]
        for k in ("ell_min", "unresolvable_assessments", "unresolved_declined"):
            mapping[k] = r[k]
        nd = 0
        for k, v in dv.items():
            nd += 1
            if k not in mapping:
                diffs.append(f"{tid} drain_values {k}: no stored field")
            elif not same(mapping[k], v):
                diffs.append(
                    f"{tid} drain_values {k}: stored {mapping[k]!r} line {v!r}"
                )
        totals["drain_values"] += nd
        # candidates
        cand_rows = [b for b in bounces if b["source"] == "candidate"]
        events = {}
        for b in bounces:
            if b["source"] != "candidate":
                events[b["source"]] = events.get(b["source"], 0) + 1
        idx = [b["candidate_index"] for b in cand_rows]
        nc_ok = (
            len(cand_rows) == len(partial["candidates"]) == len(partial["injections"])
        )
        if not nc_ok or idx != list(range(len(cand_rows))):
            diffs.append(
                f"{tid} candidate rows {len(cand_rows)} (indices {idx}) vs candidates {len(partial['candidates'])} "
                f"injections {len(partial['injections'])}"
            )
        nv, nraw = 0, 0
        for b in cand_rows:
            i = b["candidate_index"]
            if i is None or i >= len(partial["candidates"]):
                continue
            for f, val in list(zip(CAND_FIELDS, partial["candidates"][i])) + [
                (("inj_" + f), val)
                for f, val in zip(INJ_FIELDS, partial["injections"][i])
            ]:
                field = f[4:] if f.startswith("inj_") else f
                col, kind = KIND[field]
                exp = to_column(val, kind, units)
                nv += 1
                if not same(b[col], exp):
                    diffs.append(
                        f"{tid} candidate {i} {f} ({col}): stored {b[col]!r} expected {exp!r}"
                    )
                if same(b[col], val):
                    nraw += 1
        totals["candidate_values"] += nv
        totals["candidate_raw_equal"] += nraw
        # dissipation
        ndis = 0
        for k, v in line["accepted"]["dissipation"].items():
            col = k[:-4] if k.endswith("_log_T_Jordan_GeV") else k
            ndis += 1
            if not same(r[col], v):
                diffs.append(f"{tid} dissipation {k}: stored {r[col]!r} line {v!r}")
        totals["dissipation"] += ndis
        # BBN
        bb = line["bbn"]
        yp_line = "no BBNData row"
        if len(bbn) == 1 and not bbn[0]["failure"]:
            rels = {}
            for name, col in (("Yp", "Yp_BBN"), ("DoH", "DOverH")):
                s, l_ = bbn[0][col], bb[name]
                rel = abs(s - l_) / abs(l_)
                rels[name] = rel
                if worst[name][1] is None or rel > worst[name][0]:
                    worst[name] = (rel, tid)
                if rel > YP_DH_LIMIT:
                    ok = False
                    diffs.append(
                        f"{tid} {name}: stored {s!r} line {l_!r}, relative {rel:.3e} > {YP_DH_LIMIT:g}"
                    )
            yp_line = (
                f"Yp stored {bbn[0]['Yp_BBN']!r} line {bb['Yp']!r} rel {rels['Yp']:.3e}; "
                f"D/H stored {bbn[0]['DOverH']!r} line {bb['DoH']!r} rel {rels['DoH']:.3e}"
            )
        else:
            ok = False
            diffs.append(
                f"{tid} BBN: {len(bbn)} rows, failure {[x['failure'] for x in bbn]}"
            )
        print(
            f"    compared: tuning {n}, drain_values {nd}, candidates {len(cand_rows)} rows x 11 = {nv} values "
            f"({nraw} also equal to the raw value), dissipation {ndis}; event rows {events or 'none'}"
        )
        print(f"    {yp_line}")

    print()
    print("Totals: " + ", ".join(f"{k} {v}" for k, v in totals.items()))
    print(
        f"Largest relative differences: Yp {worst['Yp'][0]:.3e} ({worst['Yp'][1]}), D/H {worst['DoH'][0]:.3e} ({worst['DoH'][1]})"
    )
    if diffs:
        print("DIFFERENCES:")
        for d in diffs:
            print("  " + d)
        ok = False
    else:
        print("DIFFERENCES: none")
    print(
        f"RESULT: {'every bitwise item equal; Yp and D/H within 5e-10' if ok else 'NOT EQUAL (see above)'}"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
