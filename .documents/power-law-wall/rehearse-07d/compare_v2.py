"""
power-law-wall prompt 07d §2.2 (P115): the exponential's one-loop histories of a store of 07's tree (A, under
`one-loop-measured-v1`, the old k_ex) against the same histories of a store of 07d's (B, under `one-loop-exponential-v2`,
one k_ex), measured, not judged (P87). New in 07d.

    ./venv/bin/python .documents/power-law-wall/rehearse-07d/compare_v2.py A_STEM B_STEM

A_STEM and B_STEM are the stores' paths without `.db`. Stdlib only, and compare_histories.py of this directory for its
readers. Every shard (`<stem>-shard*.db`) is read through sqlite3 with `file:...?mode=ro` URIs only; no primary is
opened, neither datastorekit nor Ray is used, and nothing is written but stdout (prompt 07d sends it to a file under
var/power-law-wall/07d-r2/).

- **The selection:** in each store, the exponential's histories (model_type 1) whose `alpha_value.drain_model` is
  `one-loop-measured-v1` (A) or `one-loop-exponential-v2` (B).
- **Matched** by (M, n, beta, phi*, A), floats by float.hex, the drain model through the map v1 -> v2 (R14); every
  history of A matched exactly once in B and every one of B once by A, else exit 1.
- **Per history**, A beside B and the difference B - A (a relative difference beside it for a nonzero A): the outcome
  (failure, and the reason when it fails, main.py's label masked as 07's ruling (a) masks it); Delta_N_eff today and at
  1 MeV; the BBNData row's Yp and D/H; drained_share_<regime> and parking_share; candidates_<regime>; ell_min; parking_N
  and release_N; the reflections (extra_data's number_reflections); injections; the integrations (tuning_iterations) and the tuning's
  steps (plain, secant, guard).
- **Then** a table by M, beta and A of the drained share (one-loop + gap + classical + unassessed) and Delta_N_eff today,
  A beside B, and the counts of histories in which each quantity rose, fell or held.

A history that fails in one store only is reported with its reason, and is not a difference of the script: P115 measures.
Exit 0 when every history is matched exactly once each way; 1 otherwise.
"""

import argparse
import json
import sys
from collections import Counter

from compare_histories import histories, mask_label, ro

REGIMES = ("one_loop", "gap", "classical", "unassessed")
A_MODEL, B_MODEL = "one-loop-measured-v1", "one-loop-exponential-v2"
M_P_EV = 2.4353235955263046e27

QUANTITIES = (
    ("Delta_N_eff_today", "row"),
    ("Delta_N_eff_1MeV", "row"),
    ("Yp", "bbn"),
    ("D/H", "bbn"),
    *((f"drained_share_{r}", "row") for r in REGIMES),
    ("parking_share", "row"),
    *((f"candidates_{r}", "row") for r in REGIMES),
    ("ell_min", "row"),
    ("parking_N", "row"),
    ("release_N", "row"),
    ("number_reflections", "extra"),
    ("injections", "row"),
    ("tuning_iterations", "row"),
    ("plain_steps", "row"),
    ("secant_steps", "row"),
    ("guard_steps", "row"),
)


def values(h):
    r = h.row
    out = {}
    conn = ro(h.path)
    b = conn.execute(
        'SELECT failure, Yp_BBN, DOverH FROM "BBNData" WHERE scalar_history_serial = ?',
        (r["serial"],),
    ).fetchall()
    conn.close()
    x = json.loads(r["extra_data"]) if r.get("extra_data") else {}
    for q, src in QUANTITIES:
        if src == "row":
            out[q] = r.get(q)
        elif src == "extra":
            out[q] = x.get(q)
        elif q == "Yp":
            out[q] = b[0][1] if b else None
        else:
            out[q] = b[0][2] if b else None
    out["bbn_rows"] = len(b)
    out["bbn_failure"] = b[0][0] if b else None
    shares = [r.get(f"drained_share_{reg}") for reg in REGIMES]
    out["drained_total"] = (
        None if all(s is None for s in shares) else sum(s or 0.0 for s in shares)
    )
    return out


def key(h):
    return (
        float(h.M).hex(),
        h.n,
        float(h.beta).hex(),
        float(h.phi).hex(),
        float(h.alpha).hex(),
    )


def fmt(v):
    if v is None:
        return "NULL"
    if isinstance(v, float):
        return f"{v!r}"
    return str(v)


def delta(a, b):
    if a is None or b is None or isinstance(a, str) or isinstance(b, str):
        return "" if a == b else "(changed)"
    d = b - a
    rel = f" ({d / a:+.4g} rel)" if a != 0 else ""
    return f"{d:+.6g}{rel}"


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("a")
    p.add_argument("b")
    args = p.parse_args(argv)

    print(f"A store (07, {A_MODEL}): {args.a}")
    print(f"B store (07d, {B_MODEL}): {args.b}")
    _, a_all = histories(args.a, "exponential", None)
    _, b_all = histories(args.b, "exponential", None)
    a_h = [h for h in a_all if h.model == A_MODEL]
    b_h = [h for h in b_all if h.model == B_MODEL]
    print(
        f"selected: A {len(a_h)} of {len(a_all)} exponential histories; B {len(b_h)} of {len(b_all)}"
    )
    ok = True
    rows = []
    b_seen = Counter()
    for a in sorted(a_h, key=lambda h: (h.M, h.beta, h.alpha, h.n)):
        cands = [b for b in b_h if key(b) == key(a)]
        tag = f"M={a.M / M_P_EV:.6g}Mp n={a.n} beta={a.beta:g} A={a.alpha:g}"
        print()
        print(f"=== {tag}: {len(cands)} row(s) in B")
        if len(cands) != 1:
            print("    MATCH FAILED")
            ok = False
            continue
        b = cands[0]
        b_seen[id(b)] += 1
        af, bf = bool(a.row["failure"]), bool(b.row["failure"])
        print(
            f"    outcome: A {'FAILURE' if af else 'success'}, B {'FAILURE' if bf else 'success'}"
        )
        for side, h, f in (("A", a, af), ("B", b, bf)):
            if f:
                print(
                    f"    {side} reason (label masked): {mask_label(h.row['failure_reason'])!r}"
                )
        va, vb = values(a), values(b)
        for q, _src in QUANTITIES:
            print(
                f"    {q:24s} A {fmt(va[q]):>26s}  B {fmt(vb[q]):>26s}  {delta(va[q], vb[q])}"
            )
        print(
            f"    {'drained (sum)':24s} A {fmt(va['drained_total']):>26s}  B {fmt(vb['drained_total']):>26s}  "
            f"{delta(va['drained_total'], vb['drained_total'])}"
        )
        rows.append((a, b, va, vb, af, bf))

    unmatched = [b for b in b_h if b_seen[id(b)] != 1]
    for b in unmatched:
        print(
            f"   !! unmatched B history M={b.M / M_P_EV:.6g}Mp n={b.n} beta={b.beta:g} A={b.alpha:g}"
        )
        ok = False

    print()
    print(
        "By M, beta and A: the drained share (sum over regimes) and Delta_N_eff today, A -> B"
    )
    print(
        f"  {'M/Mp':>10s} {'beta':>5s} {'A':>5s}  {'drained A':>12s} {'drained B':>12s}  {'dNeff A':>12s} {'dNeff B':>12s}  {'rel dNeff':>10s}  cand A -> B"
    )
    for a, b, va, vb, af, bf in rows:
        da, db = va["Delta_N_eff_today"], vb["Delta_N_eff_today"]
        rel = f"{(db - da) / da:+.4f}" if da and db is not None else ""
        ca = sum(va[f"candidates_{r}"] or 0 for r in REGIMES)
        cb = sum(vb[f"candidates_{r}"] or 0 for r in REGIMES)
        dta = va["drained_total"]
        dtb = vb["drained_total"]
        print(
            f"  {a.M / M_P_EV:10.3g} {a.beta:5g} {a.alpha:5g}  "
            f"{(f'{dta:.6g}' if dta is not None else 'NULL'):>12s} {(f'{dtb:.6g}' if dtb is not None else 'NULL'):>12s}  "
            f"{(f'{da:.6g}' if da is not None else 'NULL'):>12s} {(f'{db:.6g}' if db is not None else 'NULL'):>12s}  "
            f"{rel:>10s}  {ca} -> {cb}"
        )
    print()
    print("Counts over the matched histories (rose / fell / held / NULL on a side):")
    for q in [q for q, _ in QUANTITIES] + ["drained_total"]:
        c = Counter()
        for _a, _b, va, vb, _af, _bf in rows:
            x, y = va[q], vb[q]
            if x is None or y is None:
                c["null"] += 1
            elif y > x:
                c["rose"] += 1
            elif y < x:
                c["fell"] += 1
            else:
                c["held"] += 1
        print(
            f"  {q:24s} rose {c['rose']}, fell {c['fell']}, held {c['held']}, NULL {c['null']}"
        )
    fails = [(a, b, af, bf) for a, b, _va, _vb, af, bf in rows if af or bf]
    print(
        f"failures: in both {sum(1 for x in fails if x[2] and x[3])}, in A only {sum(1 for x in fails if x[2] and not x[3])}, "
        f"in B only {sum(1 for x in fails if x[3] and not x[2])}"
    )
    print(
        f"RESULT: {'every history matched once each way; measured, not judged' if ok else 'MATCH FAILED (see above)'}"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
