"""
bbn-tolerance prompt 01, scan block S2: the abundances Y_i at the end of the
full low-T stage, on the SM baseline and on the control beta = 1.6,
M = 1e-3 (prod), at a given low-T rtol, as the input to the per-species atol
vector. Also the minimum and maximum of each Y_i over the stage's accepted
steps.

    ./venv/bin/python prompts/bbn-tolerance/logs/01-probes/final_abundances.py [--rtol X]
"""

import argparse
import os
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import tools.bbn_from_store as T  # noqa: E402
from Units import Planck_units  # noqa: E402
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology  # noqa: E402
from CosmologyModels.LambdaCDM import Planck2018  # noqa: E402

STORE = os.path.expanduser("~/ChamPBH-stores/science-2026.6.0")


def show(label, calls):
    lt = [c for c in calls if c.stage == T.STAGE_LOW_T_FULL][0]
    y = lt.sol.y
    print(
        f"{label}: low-T t_span {lt.t_span}, {y.shape[1]} accepted points, status {lt.status}"
    )
    for j, name in enumerate(T.SPECIES):
        print(
            f"  {name:4s} initial {y[j, 0]: .4e} final {y[j, -1]: .4e} "
            f"min {y[j].min(): .4e} max {y[j].max(): .4e}"
        )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rtol", type=float, default=None)
    args = p.parse_args()

    with T.lowT_tolerance_override(
        rtol=args.rtol, keep_sol=(T.STAGE_LOW_T_FULL,)
    ) as calls:
        T.B.compute_SM_baseline(False)
    show(f"SM baseline, low-T rtol {args.rtol}", calls)

    units = Planck_units()
    cosmo = QCD_Cosmology(0, units, Planck2018())
    h = T.find_model(STORE, 1.6, 1e-3)
    logT, r = T.ratio_grid(h.rows, units)
    Tmin, Tmax = T.callback_domain_MeV(units)
    cb = T.make_callback(
        "prod", logT, r, T.B.thermodynamic_rho_SM(cosmo, units), Tmin, Tmax, "fa"
    )
    with T.lowT_tolerance_override(
        rtol=args.rtol, keep_sol=(T.STAGE_LOW_T_FULL,)
    ) as calls:
        T.B._run_PRyMordial(cb, False, 600.0)
    show(f"control beta=1.6 M=1e-3 prod, low-T rtol {args.rtol}", calls)


if __name__ == "__main__":
    main()
