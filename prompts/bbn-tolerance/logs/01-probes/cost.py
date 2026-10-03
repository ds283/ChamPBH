"""
bbn-tolerance prompt 01, README section 2 (d): the serial cost of each
candidate low-T setting. The SM baseline and the control beta = 1.6,
M = 1e-3 (prod) are solved three times each per setting, one solve at a
time, in one process (after one discarded warm-up solve), round-robin over
the settings so that any drift in the machine's speed is shared. The cost
is the median wall time; the ratio of medians to the default is quoted.

Run alone, on an otherwise idle machine, after the parallel scan:

    ./venv/bin/python prompts/bbn-tolerance/logs/01-probes/cost.py SETTING [SETTING ...] [--repeats 3]

A SETTING is `default` or RTOL or RTOL:ATOL (ATOL a number or a NAMED_ATOL
key), for example `1e-6` or `1e-6:species`. Prints each solve, the load
average before and after, and a table of medians and ratios; appends each
solve to cost.csv.
"""

import argparse
import csv
import os
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import tools.bbn_from_store as T  # noqa: E402
from Units import Planck_units  # noqa: E402
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology  # noqa: E402
from CosmologyModels.LambdaCDM import Planck2018  # noqa: E402

STORE = os.path.expanduser("~/ChamPBH-stores/science-2026.6.0")
HERE = Path(__file__).resolve().parent


def parse(s):
    if s == "default":
        return None, None
    if ":" in s:
        r, a = s.split(":")
        return float(r), a
    return float(s), None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("settings", nargs="+")
    p.add_argument("--repeats", type=int, default=3)
    args = p.parse_args()

    print(f"load average before: {os.getloadavg()}", flush=True)
    units = Planck_units()
    cosmo = QCD_Cosmology(0, units, Planck2018())
    h = T.find_model(STORE, 1.6, 1e-3)
    logT, r = T.ratio_grid(h.rows, units)
    Tmin, Tmax = T.callback_domain_MeV(units)
    rho_SM = T.B.thermodynamic_rho_SM(cosmo, units)

    # warm-up, discarded
    T.solve_SM_baseline(False)

    walls = {(s, inp): [] for s in args.settings for inp in ("SM", "control")}
    commit = T._commit()
    for rep in range(args.repeats):
        for s in args.settings:
            rtol, atol_name = parse(s)
            atol = T.resolve_atol(atol_name)
            out_sm, _ = T.solve_SM_baseline(False, rtol, atol)
            out_c, _ = T.solve_history_variant(
                "prod", logT, r, rho_SM, Tmin, Tmax, "cost", rtol=rtol, atol=atol
            )
            for inp, out in (("SM", out_sm), ("control", out_c)):
                walls[(s, inp)].append(out["wall_s"])
                print(
                    f"  rep {rep} {s:>14s} {inp:7s} {out['status']} Yp={out['Yp']} "
                    f"wall={out['wall_s']:.2f} s load={os.getloadavg()[0]:.2f}",
                    flush=True,
                )
                row = {
                    "commit": commit,
                    "setting": s,
                    "input": inp,
                    "repeat": rep,
                    "status": out["status"],
                    "Yp": out["Yp"],
                    "DOverH": out["DOverH"],
                    "wall_s": out["wall_s"],
                    "load1": os.getloadavg()[0],
                }
                path = HERE / "cost.csv"
                new = not path.exists()
                with open(path, "a", newline="") as fh:
                    w = csv.DictWriter(fh, fieldnames=list(row))
                    if new:
                        w.writeheader()
                    w.writerow(row)

    print(f"load average after: {os.getloadavg()}")
    print(
        f"\n{'setting':>14s} {'SM median':>10s} {'ratio':>6s} {'control median':>15s} {'ratio':>6s}"
    )
    base = {
        inp: statistics.median(walls[(args.settings[0], inp)])
        for inp in ("SM", "control")
    }
    for s in args.settings:
        mS = statistics.median(walls[(s, "SM")])
        mC = statistics.median(walls[(s, "control")])
        print(
            f"{s:>14s} {mS:10.2f} {mS / base['SM']:6.2f} {mC:15.2f} {mC / base['control']:6.2f}"
        )
    print(f"(ratios against {args.settings[0]})")


if __name__ == "__main__":
    main()
