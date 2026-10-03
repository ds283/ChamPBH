"""
bbn-tolerance prompt 01, README section 0.2 P7: re-derive the pinned
"honly" references from their stated provenance, the tree at 7b518c9 (before
science-readiness prompt 01), with the low-T solve_ivp call's tolerance
changed by interception.

Run from the root of a worktree checked out at 7b518c9, with this repository's
venv:

    git worktree add --detach WT 7b518c9
    cd WT && PYTHONPATH=. /path/to/ChamPBH/venv/bin/python \\
        /path/to/this/file CASE [--rtol X] [--atol X|species]

CASE is
  const-honly-small  CONST_HONLY_SMALL_* (test_prym_passenger): the planner's
                     honly_constant_reference.py const-honly, small network
  const-honly-full   CONST_HONLY_FULL_*: the same, full network (science-
                     readiness log 01, honly_full_reference.py full)
  builder-honly-full BUILDER_CONST_HONLY_FULL_* (test_bbn_callbacks): that
                     tree's build_NP_callbacks on test_bbn_callbacks' knots
                     (250 per decade over [1e-7, 1e2] MeV, Saikawa-Shirai in
                     GeV units, constant ratio 0.08), with the pressure
                     callback -rho_NP and the density derivative 0, full
                     network (science-readiness log 01, honly_builder_reference.py
                     full, reconstructed here from its description)

The tree at 7b518c9 has no `_limited`, so the tool's stage recognition cannot
be used there. The low-T call is recognised instead as the second
`method="BDF"` call of the solve (mid-T is the first), and checked by its atol
(1e-11 small, 1e-15 full) and its y0 length (8 small, 12 full); any mismatch
raises.
"""

import argparse
import sys
import time
from math import log, log10

import numpy as np


def main():
    p = argparse.ArgumentParser()
    p.add_argument("case")
    p.add_argument("--rtol", type=float, default=None)
    p.add_argument("--atol", default=None, help="a number, or comma-separated vector")
    args = p.parse_args()

    import PRyM.PRyM_main as PRyMmain
    from ComputeTargets.tests.prym_fixtures import (
        CONSTANT,
        RES_D_OVER_H_E5,
        RES_HE3_OVER_H_E5,
        RES_LI7_OVER_H_E10,
        RES_YP_BBN,
        run_prym,
    )

    small = args.case.endswith("small")
    atol = None
    if args.atol is not None:
        atol = (
            np.array([float(x) for x in args.atol.split(",")])
            if "," in args.atol
            else float(args.atol)
        )

    real = PRyMmain.solve_ivp
    seen = {"bdf": 0, "lowT": 0}

    def intercept(fun, t_span, y0, **kw):
        if kw.get("method") == "BDF":
            seen["bdf"] += 1
            if seen["bdf"] == 2:
                want_atol, want_n = (1e-11, 8) if small else (1e-15, 12)
                if kw.get("atol") != want_atol or len(y0) != want_n or "rtol" in kw:
                    raise RuntimeError(
                        f"unexpected low-T call: {len(y0)} {kw.get('atol')} {kw.get('rtol')}"
                    )
                seen["lowT"] += 1
                if args.rtol is not None:
                    kw["rtol"] = args.rtol
                if atol is not None:
                    kw["atol"] = atol
        return real(fun, t_span, y0, **kw)

    def minus_rho(T):
        return -CONSTANT.rho(T)

    def zero(T):
        return 0.0

    PRyMmain.solve_ivp = intercept
    t0 = time.time()
    try:
        if args.case.startswith("const-honly"):
            res = run_prym(CONSTANT.rho, minus_rho, zero, small_network=small)
        elif args.case == "builder-honly-full":
            from ComputeTargets.BBNData import build_NP_callbacks, thermodynamic_rho_SM
            from CosmologyModels.GenericEOS.SaikawaShirai_EOS_spline import (
                SaikawaShirai_EOS_spline,
            )
            from Units import GeV_units

            T_MIN_MEV, T_MAX_MEV, KNOTS_PER_DECADE = 1e-7, 100.0, 250
            units = GeV_units()
            eos = SaikawaShirai_EOS_spline(units)
            rho_SM, drho_SM_dT = thermodynamic_rho_SM(eos, units)
            n_knots = int(round(KNOTS_PER_DECADE * log10(T_MAX_MEV / T_MIN_MEV))) + 1
            x_inc = np.linspace(log(T_MIN_MEV), log(T_MAX_MEV), n_knots)
            x_knots = x_inc[::-1].copy()
            T_knots = np.exp(x_knots)
            r = 0.08 + 0.0 * np.log(T_knots)
            cb = build_NP_callbacks(
                x_knots,
                r,
                r / 3.0,
                rho_SM,
                drho_SM_dT,
                T_min_MeV=T_MIN_MEV,
                T_max_MeV=T_MAX_MEV,
                task_label="builder-honly",
            )

            def minus_cb(T):
                return -cb.rho_NP(T)

            res = run_prym(cb.rho_NP, minus_cb, zero, small_network=False)
        else:
            raise SystemExit(f"unknown case {args.case}")
    finally:
        PRyMmain.solve_ivp = real

    if seen["lowT"] != 1:
        raise RuntimeError(f"low-T call recognised {seen['lowT']} times")
    print(
        f"{args.case} rtol={args.rtol} atol={args.atol}: Yp={res[RES_YP_BBN]:.10g} "
        f"DoH={res[RES_D_OVER_H_E5]:.10g} He3oH={res[RES_HE3_OVER_H_E5]:.10g} "
        f"Li7oH={res[RES_LI7_OVER_H_E10]:.10g} wall={time.time() - t0:.1f} s"
    )


if __name__ == "__main__":
    main()
