"""
bbn-tolerance prompt 01, README section 0.2 P7: on the current tree, under the
tool's lowT_tolerance_override, the quantity each pinned PRyMordial test
computes, against its pin and its bound.

    ./venv/bin/python prompts/bbn-tolerance/logs/01-probes/pinned_values_now.py CASE [--rtol X] [--atol X|NAME]

CASE is
  passenger-c   test_prym_passenger (c): CONSTANT, small network, no wall-clock
                limit, against CONST_HONLY_SMALL_* (bound CONST_HONLY_RTOL)
  network-b     test_network_flag (b): CONSTANT small and full; 7Li/H shift
                small vs full (>= LI7_MIN_RELATIVE_SHIFT), full against
                CONST_HONLY_FULL_* (bound REFERENCE_RTOL)
  callbacks-h   test_bbn_callbacks (h): the builder's constant-ratio callback,
                full network, against BUILDER_CONST_HONLY_FULL_* (bound
                END_TO_END_RTOL)
  callbacks-i   test_bbn_callbacks (i): compute_SM_baseline(False) against
                README_BASELINE (bound BASELINE_RTOL)
Both low-T calls (small and full network) get the override.
"""

import argparse
import sys
import time
from math import log, log10
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import tools.bbn_from_store as T  # noqa: E402
from ComputeTargets.tests import prym_fixtures as F  # noqa: E402


def rel(a, b):
    return abs(a - b) / abs(b)


def line(name, value, pin, bound):
    d = rel(value, pin)
    print(
        f"  {name}: {value:.10g} against pin {pin:.10g}: {d:.3e} "
        f"(bound {bound:g}: {'PASS' if d <= bound else 'FAIL'})"
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("case")
    p.add_argument("--rtol", type=float, default=None)
    p.add_argument("--atol", default=None)
    args = p.parse_args()
    atol = T.resolve_atol(args.atol)

    t0 = time.time()
    print(f"{args.case}: low-T rtol={args.rtol} atol={args.atol}")
    with T.lowT_tolerance_override(rtol=args.rtol, atol=atol):
        if args.case == "passenger-c":
            from ComputeTargets.tests import test_prym_passenger as P

            res = F.run_prym(F.CONSTANT.rho, small_network=True, wall_clock_limit=None)
            line("Yp", res[F.RES_YP_BBN], P.CONST_HONLY_SMALL_YP, P.CONST_HONLY_RTOL)
            line(
                "D/H",
                res[F.RES_D_OVER_H_E5],
                P.CONST_HONLY_SMALL_D_OVER_H_E5,
                P.CONST_HONLY_RTOL,
            )
        elif args.case == "network-b":
            from ComputeTargets.tests import test_prym_passenger as P
            from ComputeTargets.tests import test_network_flag as N

            with F.SavedPRyMGlobals():
                small = F.run_prym(F.CONSTANT.rho, small_network=True)
                full = F.run_prym(F.CONSTANT.rho, small_network=False)
            dLi7 = rel(small[F.RES_LI7_OVER_H_E10], full[F.RES_LI7_OVER_H_E10])
            print(
                f"  small: Yp {small[F.RES_YP_BBN]:.10g} D/H {small[F.RES_D_OVER_H_E5]:.10g} "
                f"7Li/H {small[F.RES_LI7_OVER_H_E10]:.10g}; full 7Li/H {full[F.RES_LI7_OVER_H_E10]:.10g}"
            )
            print(
                f"  7Li/H small vs full {dLi7:.3e} (>= {N.LI7_MIN_RELATIVE_SHIFT:g}: "
                f"{'PASS' if dLi7 >= N.LI7_MIN_RELATIVE_SHIFT else 'FAIL'})"
            )
            line("full Yp", full[F.RES_YP_BBN], P.CONST_HONLY_FULL_YP, P.REFERENCE_RTOL)
            line(
                "full D/H",
                full[F.RES_D_OVER_H_E5],
                P.CONST_HONLY_FULL_D_OVER_H_E5,
                P.REFERENCE_RTOL,
            )
        elif args.case == "callbacks-h":
            from ComputeTargets.tests import test_bbn_callbacks as C
            from ComputeTargets.BBNData import (
                build_rho_NP_callback,
                thermodynamic_rho_SM,
            )
            from CosmologyModels.GenericEOS.SaikawaShirai_EOS_spline import (
                SaikawaShirai_EOS_spline,
            )
            from Units import GeV_units

            units = GeV_units()
            rho_SM = thermodynamic_rho_SM(SaikawaShirai_EOS_spline(units), units)
            n_knots = (
                int(round(C.KNOTS_PER_DECADE * log10(C.T_MAX_MEV / C.T_MIN_MEV))) + 1
            )
            x_knots = np.linspace(log(C.T_MIN_MEV), log(C.T_MAX_MEV), n_knots)[
                ::-1
            ].copy()
            T_knots = np.exp(x_knots)
            cb = build_rho_NP_callback(
                x_knots,
                C.ratio_constant(T_knots),
                rho_SM,
                T_min_MeV=C.T_MIN_MEV,
                T_max_MeV=C.T_MAX_MEV,
                task_label="test-constant",
            )
            res = F.run_prym(cb)
            line(
                "Yp",
                res[F.RES_YP_BBN],
                C.BUILDER_CONST_HONLY_FULL_YP,
                C.END_TO_END_RTOL,
            )
            line(
                "D/H",
                res[F.RES_D_OVER_H_E5],
                C.BUILDER_CONST_HONLY_FULL_D_OVER_H_E5,
                C.END_TO_END_RTOL,
            )
        elif args.case == "callbacks-i":
            from ComputeTargets.tests import test_bbn_callbacks as C

            with F.SavedPRyMGlobals():
                b = T.B.compute_SM_baseline(False)
            for k, ref in C.README_BASELINE.items():
                line(k, b[k], ref, C.BASELINE_RTOL)
        else:
            raise SystemExit(f"unknown case {args.case}")
    print(f"  wall {time.time() - t0:.1f} s")


if __name__ == "__main__":
    main()
