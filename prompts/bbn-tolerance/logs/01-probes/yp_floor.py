"""
bbn-tolerance prompt 01, README section 2 (e): which PRyMordial stage, if any,
sets Yp's residual spread (~2e-5) once the low-T stage is at the recommended
setting. Each other stage is tightened in turn by the tool's
stage_tolerance_override; nothing in PRyM/ changes.

    ./venv/bin/python prompts/bbn-tolerance/logs/01-probes/yp_floor.py BETA M CONFIG \\
        --lowT-rtol X [--lowT-atol X|NAME] [--csv PATH]

CONFIG is `base` (the low-T setting only), `thermo`, `aT`, `highT`, `midT`
(that stage tightened as well) or `all` (all four). Tightened means
rtol 1e-9 and atol 1e-12 for thermodynamics, a(T) and high-T n <-> p
(PRyMordial passes 1e-6 and 1e-9), and rtol 1e-9, atol 1e-15 for mid-T
(PRyMordial passes 1e-6 and 1e-9; 1e-15 is the low-T stage's own atol, since
the mid-T abundances include species far below 1e-9).
Runs prod, pert12 and pert9 and prints Yp and D/H for each, and their spreads.
"""

import argparse
import csv
import fcntl
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import tools.bbn_from_store as T  # noqa: E402
from Units import Planck_units  # noqa: E402
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology  # noqa: E402
from CosmologyModels.LambdaCDM import Planck2018  # noqa: E402

STORE = os.path.expanduser("~/ChamPBH-stores/science-2026.6.0")
TIGHT = (1e-9, 1e-12)
TIGHT_MID = (1e-9, 1e-15)
CONFIGS = {
    "base": {},
    "thermo": {T.STAGE_THERMO_SM: TIGHT, T.STAGE_THERMO_NP: TIGHT},
    "aT": {T.STAGE_A_OF_T: TIGHT},
    "highT": {T.STAGE_HIGH_T: TIGHT},
    "midT": {T.STAGE_MID_T_FULL: TIGHT_MID},
}
CONFIGS["all"] = {
    k: v for c in ("thermo", "aT", "highT", "midT") for k, v in CONFIGS[c].items()
}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("beta", type=float)
    p.add_argument("M", type=float)
    p.add_argument("config")
    p.add_argument("--lowT-rtol", type=float, required=True)
    p.add_argument("--lowT-atol", default=None)
    p.add_argument(
        "--csv", default=str(Path(__file__).resolve().parent / "yp_floor.csv")
    )
    args = p.parse_args()

    atol = T.resolve_atol(args.lowT_atol)
    settings = dict(CONFIGS[args.config])
    for s in T.LOW_T_STAGES:
        settings[s] = (args.lowT_rtol, atol)

    units = Planck_units()
    cosmo = QCD_Cosmology(0, units, Planck2018())
    h = T.find_model(STORE, args.beta, args.M)
    logT, r = T.ratio_grid(h.rows, units)
    Tmin, Tmax = T.callback_domain_MeV(units)
    rho_SM = T.B.thermodynamic_rho_SM(cosmo, units)

    out = {}
    for kind in ("prod", "pert12", "pert9"):
        cb = T.make_callback(kind, logT, r, rho_SM, Tmin, Tmax, f"ypfloor-{kind}")
        t0 = time.perf_counter()
        with T.stage_tolerance_override(settings) as calls:
            res = T.B._run_PRyMordial(cb, False, 600.0)
        wall = time.perf_counter() - t0
        stages = sorted(
            {
                c.stage
                for c in calls
                if c.stage in settings and c.stage not in T.LOW_T_STAGES
            }
        )
        out[kind] = res
        row = {
            "beta": args.beta,
            "M_Mp": args.M,
            "config": args.config,
            "lowT_rtol": args.lowT_rtol,
            "lowT_atol": args.lowT_atol or "",
            "variant": kind,
            "status": "FAILURE" if res.get("failure") else "ok",
            "Yp": res.get("Yp_BBN", ""),
            "DOverH": res.get("DOverH", ""),
            "wall_s": wall,
            "tightened": ";".join(stages),
            "failure_reason": res.get("failure_reason", ""),
        }
        with open(args.csv, "a", newline="") as fh:
            fcntl.flock(fh, fcntl.LOCK_EX)
            fh.seek(0, os.SEEK_END)
            w = csv.DictWriter(fh, fieldnames=list(row))
            if fh.tell() == 0:
                w.writeheader()
            w.writerow(row)
            fh.flush()
            fcntl.flock(fh, fcntl.LOCK_UN)
        print(
            f"  {args.config} {kind}: {row['status']} Yp={row['Yp']} D/H={row['DOverH']} "
            f"tightened={stages} {wall:.1f} s",
            flush=True,
        )

    ok = [v for v in out.values() if not v.get("failure")]
    if len(ok) == 3:
        for key in ("Yp_BBN", "DOverH"):
            vals = sorted(v[key] for v in ok)
            print(f"  spread {key}: {(vals[-1] - vals[0]) / vals[1]:.3e}")


if __name__ == "__main__":
    main()
