"""
Probe (electroweak-eos prompt 01a): compare two runs of scan_all.sh (before and after 01a's change, on
the same table), history by history, to the last bit. Prints a text report.

Everything scan_turning_points.py records is compared as its hex text or exact value: the turning
points (for a history that fails, the step budget at small M, every TurningPoint constructed before its
failure, and the failure's message), first_bounce, the solution's nodes and interpolants (SHA-256), N_final and the final state,
nfev, accepted and rejected steps, the reflections, dropped_drains, the warnings, restart_retakes,
parking/release, and surfing_turning_points. unresolved_turning_point_pairs is reported (it does not
exist before the change).

  --mode old   two verdicts. As the prompt wrote the gate: every field equal, and the count 0 after.
               As the user ruled it on 2026-11-12 (ruling (a), after 01a's stop): every field but the
               turning points equal, dropped_drains included; the turning points after those before
               less the pairs dropped; and no pair dropped at or above 100 GeV, 01's scan range. The
               late pairs are listed. The exit status follows the ruled gate.
  --mode new   the fields other than turning_points and dropped_drains must be equal (dropped_drains
               is an allowed difference by the user's ruling of 2026-11-12: U9 has a dropped pair neither
               arm nor drop a drain). The turning
               points after must be those before less the pairs dropped, in order and bitwise; the
               report names every removed TurningPoint and the dropped pairs' steps, excursions and
               tolerances, and dropped_drains before and after.

Run from the repository root:
  PYTHONPATH=. ./venv/bin/python .documents/electroweak-eos/turning-points/compare_scans.py \
      BEFORE_DIR AFTER_DIR --mode old|new
Exit status 0 if the mode's criterion holds for every history, 1 otherwise.
"""

import argparse
import json
import sys
from pathlib import Path

COMMON = [
    "failure",
    "first_bounce",
    "first_bounce_from_result_equal",
    "n_nodes",
    "ts_sha256",
    "interpolants_sha256",
    "N_final",
    "final_state",
    "nfev",
    "accepted_steps",
    "steps_rejected_by_exception",
    "n_reflections",
    "reflections",
    "n_injections",
    "warnings",
    "restart_retakes",
    "parking",
    "release",
    "surfing_turning_points",
]
# 01's scans ran down to T_J = 100 GeV
LATE_GEV = 100.0

TP_FIELDS = [
    "N",
    "kind",
    "phi_Einstein",
    "log_T_Jordan",
    "nonadiabaticity",
    "m_eff_over_H",
]


def load(d):
    out = {}
    for f in sorted(Path(d).glob("*.json")):
        rec = json.load(open(f))["histories"][0]
        out[(rec["beta"], rec["M"])] = rec
    return out


def tps_of(rec):
    """
    The history's turning points: IntegrationResult.turning_points for a history that succeeded, and
    every TurningPoint constructed before the failure for one that failed (the step budget at small M).
    """
    if "turning_points" in rec:
        return rec["turning_points"]
    return rec.get("turning_points_constructed", [])


def tp_key(t):
    return tuple(t[k] for k in TP_FIELDS)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("before")
    ap.add_argument("after")
    ap.add_argument("--mode", choices=("old", "new"), required=True)
    a = ap.parse_args()

    B, A = load(a.before), load(a.after)
    ok = True
    # the old table's gate as the user ruled it on 2026-11-12, ruling (a): a pair dropped below
    # LATE_GEV, below 01's scan range, is recorded, not a failure
    ok_amended = True
    late_pairs = 0
    print(f"before: {a.before}\nafter:  {a.after}\nmode:   {a.mode}")
    print(f"histories: before {len(B)}, after {len(A)}")
    if set(B) != set(A):
        print("!! the two runs hold different histories")
        ok = ok_amended = False

    n_tp_before = n_tp_after = n_fb = 0
    total_unresolved = 0
    for key in sorted(set(B) & set(A)):
        b, x = B[key], A[key]
        label = f"beta={key[0]:<5} M={key[1]:<6g}" + (
            " FAILED" if b.get("failure") else ""
        )
        diffs = [f for f in COMMON if b.get(f) != x.get(f)]
        tb = tps_of(b)
        ta = tps_of(x)
        n_tp_before += len(tb)
        n_tp_after += len(ta)
        n_fb += b.get("first_bounce") is not None
        unres = x.get("unresolved_turning_point_pairs")
        if unres is None and x.get("failure") and "dropped_pairs_log" in x:
            # a failed history returns no IntegrationResult: count the pairs its log of
            # resolved_sign_changes recorded
            unres = sum(d["dropped"] for d in x["dropped_pairs_log"])
        total_unresolved += unres or 0
        line = (
            f"{label} tps {len(tb):>6} -> {len(ta):>6}  unresolved {unres}  "
            f"dropped_drains {b.get('dropped_drains')} -> {x.get('dropped_drains')}  "
            f"first_bounce {'same' if b.get('first_bounce') == x.get('first_bounce') else 'DIFFERS'}"
        )
        cp = x.get("closest_pair") or {}
        if cp.get("ratio") is not None:
            line += (
                f"  closest pair excursion/tau {cp['ratio']:.4g} at N={cp['N_low']:.6f} "
                f"({cp['steps_with_two_or_more_changes']} steps with >= 2 changes)"
            )
        print(line)

        # common to both modes: the turning points after must be those before less the pairs dropped
        kb = [tp_key(t) for t in tb]
        ka = set(tp_key(t) for t in ta)
        removed = [t for t in tb if tp_key(t) not in ka]
        subset_ok = [k for k in kb if k in ka] == [tp_key(t) for t in ta] and len(
            removed
        ) == 2 * (unres or 0)
        if diffs:
            print(f"   !! differs in {diffs}")
            ok = ok_amended = False
        if not subset_ok:
            print(
                f"   !! the turning points after are not those before less the "
                f"{unres} pairs dropped ({len(removed)} removed)"
            )
            ok = ok_amended = False
        if a.mode == "old":
            if removed:
                print("   !! turning points differ")
                ok = False
            if b.get("dropped_drains") != x.get("dropped_drains"):
                print("   !! dropped_drains differs")
                ok = ok_amended = False
            if unres != 0:
                print("   !! a pair was dropped on the old table")
                ok = False
                if any(t["T_J_GeV_dec"] >= LATE_GEV for t in removed):
                    print(
                        f"   !! ... at or above {LATE_GEV:g} GeV, inside 01's scan range"
                    )
                    ok_amended = False
                else:
                    late_pairs += unres
        for t in removed:
            print(
                f"   removed {t['kind']:<8} N={t['N_dec']:.9f} T_J={t['T_J_GeV_dec']:.6g} GeV "
                f"phi={t['dphi_MP_dec'] + 5.0:.6g} M_P"
            )
        for d in x.get("dropped_pairs_log", []):
            print(
                f"   dropped pair on [{d['N_low_dec']:.9f}, {d['N_high_dec']:.9f}]: "
                f"pi nodes {d['pi_low_dec']:.4g}, {d['pi_high_dec']:.4g}; excursions "
                f"{', '.join(f'{e:.4g}' for e in d['excursions_dec'])}; tau {d['tolerance_dec']:.8g}"
            )
    print(
        f"turning points compared: before {n_tp_before}, after {n_tp_after}; "
        f"histories with a first_bounce: {n_fb}; pairs dropped in all: {total_unresolved}"
    )
    if a.mode == "old":
        print(
            "RESULT (the prompt's gate as written, no pair dropped):",
            "HOLDS" if ok else "FAILS",
        )
        print(
            f"RESULT (the gate as ruled 2026-11-12, (a): no pair dropped at or above {LATE_GEV:g} GeV, "
            f"late pairs recorded, everything else bitwise): "
            + ("HOLDS" if ok_amended else "FAILS")
            + f"; late pairs dropped: {late_pairs}"
        )
        sys.exit(0 if ok_amended else 1)
    print("RESULT:", "HOLDS" if ok else "FAILS")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
