"""
Probe (electroweak-eos prompt 01a): the turning points of prompt 01's two scans, on a chosen w table,
written to JSON with every float in hex, so that two runs (before and after 01a's change, or on two
tables) can be compared to the last bit by compare_scans.py.

The histories are 01's (its handoff §3): ExponentialModel(M, n = 1, beta) at the golden histories'
normalization and phi_ref = 0.0, phi* = 5 M_P, pi* = 0, T* = 2e4 GeV, atol = rtol = 1e-8, alpha = 0,
main.py's sampling axis, compute_sample_masses=False. 01's 32-history scan is beta in
{0.5, 1.2, 1.5, 2, 3, 5.5, 10, 20} x M in {4e-31, 1e-12, 1e-3, 0.5} M_P; its 20-history scan is
beta in {0.5, 1.2, 1.5, 2, 2.5, 3, 4, 5.5, 10, 20} x M in {4e-31, 1e-3} M_P. Their union is 36
histories. 01 ran both to T_J = 100 GeV; this probe runs them to --T-stop (GeV), by default T_CMB, so
that first_bounce and every later turning point are covered.

The table is read through a redirect of the class's CSV read (pandas.read_csv inside Xav_EOS_spline),
so nothing in the tree is swapped. Without --table the class reads its own CSV.

For each history the IntegrationResult is captured by wrapping integrate_scalar_history. Recorded:
every TurningPoint (N, kind, phi_E, ln T_J, nonadiabaticity, m_eff/H), first_bounce, the solution's
nodes (ts, as hex) and a SHA-256 of every interpolant's arrays, N_final and the final state, the
counts (accepted steps, nfev, rejected, reflections, dropped_drains, and
unresolved_turning_point_pairs where the field exists), the warnings, and surfing_turning_points.
Every TurningPoint is also recorded as it is constructed (turning_points_constructed), so that a history
that fails (the step budget, at small M) is compared up to its failure. If
ScalarHistory.resolved_sign_changes exists (01a's change), each call that drops a pair is recorded (the
step, its nodes' pi, the tolerance, and the excursion of every consecutive pair), and the closest pair:
the smallest excursion/tolerance over every step showing two or more changes, and the smallest above 1
(kept).

The record was extended twice while 01a ran (log 01a §2): the constructed TurningPoints after the
first baseline run, and the closest kept pair after the scans with the change. compare_scans.py reads
the fields both versions write.

Run from the repository root:
  PYTHONPATH=. ./venv/bin/python .documents/electroweak-eos/turning-points/scan_turning_points.py \
      --out OUT.json [--table CSV] [--T-stop GeV] [--only BETA,M] | --list
No store, no Ray. scan_all.sh runs the 36 histories; the small-M ones take minutes each.
"""

import argparse
import contextlib
import hashlib
import importlib
import io
import json
import time
from math import exp
from unittest import mock

import numpy as np
import pandas as pd

import CosmologyModels.GenericEOS.Xav_EOS_spline as xav_module
from ComputeTargets.tests import golden_history as gh
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
from CosmologyModels.LambdaCDM import Planck2018
from Units import Planck_units

SM = importlib.import_module("ComputeTargets.ScalarHistory")

SCAN32 = [
    (b, M)
    for b in (0.5, 1.2, 1.5, 2.0, 3.0, 5.5, 10.0, 20.0)
    for M in (4e-31, 1e-12, 1e-3, 0.5)
]
SCAN20 = [
    (b, M)
    for b in (0.5, 1.2, 1.5, 2.0, 2.5, 3.0, 4.0, 5.5, 10.0, 20.0)
    for M in (4e-31, 1e-3)
]


def union():
    out = list(SCAN32)
    for x in SCAN20:
        if x not in out:
            out.append(x)
    return out


def hx(x):
    return None if x is None else float(x).hex()


def sha_arrays(*arrays) -> str:
    h = hashlib.sha256()
    for a in arrays:
        h.update(np.ascontiguousarray(np.asarray(a, dtype=float)).tobytes())
    return h.hexdigest()


def interpolant_hash(interpolants) -> str:
    h = hashlib.sha256()
    for it in interpolants:
        for name in ("t_old", "t", "h", "Q", "y_old"):
            v = getattr(it, name, None)
            if v is not None:
                h.update(np.ascontiguousarray(np.asarray(v, dtype=float)).tobytes())
    return h.hexdigest()


def excursions(pis):
    """The excursion of every consecutive pair of sign changes, by 01a's definition, recomputed here."""
    changes = []
    last_sign, last_j = 0, None
    for j, p in enumerate(pis):
        if p == 0.0:
            continue
        s = 1 if p > 0.0 else -1
        if last_sign != 0 and s != last_sign:
            changes.append((last_j, j))
        last_sign, last_j = s, j
    return [
        max(abs(pis[j]) for j in range(changes[i][1], changes[i + 1][0] + 1))
        for i in range(len(changes) - 1)
    ]


def run(table, beta, M, T_stop_GeV, drop_log, margin, constructed):
    units = Planck_units()
    real_read = pd.read_csv
    patches = []
    if table is not None:
        patches.append(
            mock.patch.object(
                xav_module.pd,
                "read_csv",
                lambda a, *k, **kw: real_read(table, *k, **kw),
            )
        )
    with contextlib.ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        cosmology = QCD_Cosmology(0, units, Planck2018())
    model = ExponentialModel(
        0, M_value(0, M * units.PlanckMass), 1, beta_value(0, beta), units
    )
    realized = model.realize(gh.normalization(units), 0.0)

    captured = []
    real_isc = SM.integrate_scalar_history

    def capturing(*a, **k):
        r = real_isc(*a, **k)
        captured.append(r)
        return r

    # every TurningPoint as observe_turning_points constructs it, so that a history that fails (the
    # step budget, at small M) is still compared up to its failure
    real_tp = SM.TurningPoint

    def recording_tp(*a, **k):
        t = real_tp(*a, **k)
        constructed.append(t)
        return t

    stack_patches = [
        mock.patch.object(SM, "integrate_scalar_history", capturing),
        mock.patch.object(SM, "TurningPoint", recording_tp),
    ]
    if hasattr(SM, "resolved_sign_changes"):
        real_rsc = SM.resolved_sign_changes

        def logging_rsc(N_samples, pi_samples, tolerance):
            kept, dropped = real_rsc(N_samples, pi_samples, tolerance)
            # the closest call: the smallest excursion/tolerance over every pair of consecutive
            # changes the samples show, kept or dropped
            ex = excursions([float(p) for p in pi_samples])
            if ex:
                ratio = min(ex) / tolerance
                if margin["ratio"] is None or ratio < margin["ratio"]:
                    margin.update(
                        ratio=ratio,
                        N_low=float(N_samples[0]),
                        excursion=min(ex),
                        tolerance=float(tolerance),
                    )
                margin["steps_with_two_or_more_changes"] += 1
                # the closest pair the rule keeps: the smallest excursion/tolerance above 1
                above = [e / tolerance for e in ex if e > tolerance]
                if above and (
                    margin.get("kept_ratio") is None
                    or min(above) < margin["kept_ratio"]
                ):
                    margin.update(
                        kept_ratio=min(above),
                        kept_N_low=float(N_samples[0]),
                        kept_N_high=float(N_samples[-1]),
                    )
            if dropped > 0:
                pis = [float(p) for p in pi_samples]
                drop_log.append(
                    {
                        "N_low": hx(N_samples[0]),
                        "N_high": hx(N_samples[-1]),
                        "N_low_dec": float(N_samples[0]),
                        "N_high_dec": float(N_samples[-1]),
                        "pi_low": hx(pis[0]),
                        "pi_high": hx(pis[-1]),
                        "pi_low_dec": pis[0],
                        "pi_high_dec": pis[-1],
                        "tolerance": hx(tolerance),
                        "tolerance_dec": float(tolerance),
                        "excursions_dec": excursions(pis),
                        "pi_samples_dec": pis,
                        "dropped": dropped,
                        "kept": [c.kind for c in kept],
                    }
                )
            return kept, dropped

        stack_patches.append(
            mock.patch.object(SM, "resolved_sign_changes", logging_rsc)
        )

    with contextlib.ExitStack() as stack:
        for p in stack_patches:
            stack.enter_context(p)
        with contextlib.redirect_stdout(io.StringIO()):
            data = SM.integrate_history(
                cosmology,
                temperature(0, 2.0e4 * units.GeV),
                temperature(1, T_stop_GeV * units.GeV),
                phi_value(0, 5.0 * units.PlanckMass),
                pi_value(0, 0.0),
                gh.make_z_grid(),
                realized,
                task_label="ee01a-scan",
                atol=1e-8,
                rtol=1e-8,
                alpha=0.0,
                compute_sample_masses=False,
            )
    return units, realized, data, (captured[-1] if captured else None)


def tp_record(units, t):
    return {
        "N": hx(t.N),
        "kind": t.kind,
        "phi_Einstein": hx(t.phi_Einstein),
        "log_T_Jordan": hx(t.log_T_Jordan),
        "nonadiabaticity": hx(t.nonadiabaticity),
        "m_eff_over_H": hx(t.m_eff_over_H),
        "N_dec": t.N,
        "T_J_GeV_dec": exp(t.log_T_Jordan) / units.GeV,
        "dphi_MP_dec": t.phi_Einstein / units.PlanckMass - 5.0,
    }


def record(units, realized, data, res):
    GeV, Mp = units.GeV, units.PlanckMass
    out = {}
    if data.get("failure", False):
        out["failure"] = data.get("failure_reason")
        return out
    fb = data["first_bounce"]
    out["first_bounce"] = (
        None
        if fb is None
        else {
            "N": hx(fb.N),
            "phi_Einstein": hx(fb.phi_Einstein),
            "log_T_Jordan": hx(fb.log_T_Jordan),
            "reflected": fb.reflected,
        }
    )
    fb2 = SM.first_bounce(res)
    out["first_bounce_from_result_equal"] = (fb2 == fb) or (
        fb2 is not None
        and fb is not None
        and all(
            (float(a) == float(b)) if not isinstance(a, bool) else a == b
            for a, b in zip(fb2, fb)
        )
    )
    out["turning_points"] = [tp_record(units, t) for t in res.turning_points]
    ts = np.asarray(res.solution.ts, dtype=float)
    out["n_nodes"] = int(ts.size)
    out["ts_sha256"] = sha_arrays(ts)
    out["interpolants_sha256"] = interpolant_hash(res.solution.interpolants)
    out["N_final"] = hx(res.N_final)
    out["final_state"] = [hx(x) for x in res.final_state]
    out["nfev"] = res.nfev
    out["accepted_steps"] = res.accepted_steps
    out["steps_rejected_by_exception"] = res.steps_rejected_by_exception
    out["n_reflections"] = len(res.reflections)
    out["reflections"] = [[hx(x) for x in r[:2]] for r in res.reflections]
    out["n_injections"] = len(res.injections)
    out["dropped_drains"] = res.dropped_drains
    out["warnings"] = list(res.warnings)
    out["restart_retakes"] = res.restart_retakes
    out["parking"] = res.parking is not None
    out["release"] = res.release is not None
    out["unresolved_turning_point_pairs"] = getattr(
        res, "unresolved_turning_point_pairs", None
    )
    out["surfing_turning_points"] = [
        {
            "N": hx(c.N),
            "phi_Einstein": hx(c.phi_Einstein),
            "pi_Einstein": hx(c.pi_Einstein),
            "log_T_Jordan": hx(c.log_T_Jordan),
            "direction": c.direction,
            "T_J_GeV_dec": exp(c.log_T_Jordan) / GeV,
        }
        for c in SM.surfing_turning_points(res, realized)
    ]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=None)
    ap.add_argument("--table", default=None)
    ap.add_argument("--T-stop", type=float, default=None, help="GeV; default T_CMB")
    ap.add_argument("--only", default=None, help="BETA,M: one history")
    ap.add_argument(
        "--list", action="store_true", help="print the grid, one BETA,M a line"
    )
    a = ap.parse_args()
    if a.list:
        for b, M in union():
            print(f"{b!r},{M!r}")
        return

    units = Planck_units()
    if a.T_stop is None:
        cosmo = QCD_Cosmology(0, units, Planck2018())
        T_stop_GeV = cosmo.T_CMB_Kelvin * units.Kelvin / units.GeV
    else:
        T_stop_GeV = a.T_stop

    grid = union()
    if a.only is not None:
        b, M = (float(x) for x in a.only.split(","))
        grid = [(b, M)]

    results = []
    t_all = time.time()
    for beta, M in grid:
        t0 = time.time()
        drop_log = []
        margin = {"ratio": None, "steps_with_two_or_more_changes": 0}
        constructed = []
        units, realized, data, res = run(
            a.table, beta, M, T_stop_GeV, drop_log, margin, constructed
        )
        rec = {
            "beta": beta,
            "M": M,
            "in_scan32": (beta, M) in SCAN32,
            "in_scan20": (beta, M) in SCAN20,
        }
        rec.update(record(units, realized, data, res))
        rec["turning_points_constructed"] = [tp_record(units, t) for t in constructed]
        rec["dropped_pairs_log"] = drop_log
        rec["closest_pair"] = margin
        rec["seconds"] = round(time.time() - t0, 2)
        results.append(rec)
        print(
            f"beta={beta} M={M}: {rec.get('failure') or (str(len(rec['turning_points'])) + ' tps')}, "
            f"unresolved={rec.get('unresolved_turning_point_pairs')}, "
            f"dropped_logged={len(drop_log)}, {rec['seconds']} s",
            flush=True,
        )
    meta = {
        "table": a.table or "the class's own CSV",
        "T_stop_GeV": T_stop_GeV,
        "seconds_total": round(time.time() - t_all, 1),
        "has_resolved_sign_changes": hasattr(SM, "resolved_sign_changes"),
    }
    with open(a.out, "w") as f:
        json.dump({"meta": meta, "histories": results}, f, indent=1)
    print(json.dumps(meta))


if __name__ == "__main__":
    main()
