"""
bbn-tolerance prompt 01c, README section 2 (c') and (d): the serial cost of
the small network at each T1 setting, against the full network at the default
low-T tolerance (today's production cost). Adapted from logs/01-probes/cost.py
(copied, not edited in place) to take the network in the setting.

The SM baseline and the control beta = 1.6, M = 1e-3 (prod) are solved three
times each per setting, one solve at a time, in one process, after one
discarded warm-up solve per network, round-robin over the settings so that any
drift in the machine's speed is shared. The cost is the median wall time; the
ratio of medians to the first setting is quoted.

Run alone, after the parallel scan:

    ./venv/bin/python prompts/bbn-tolerance/logs/01c-probes/cost.py SETTING [SETTING ...] [--repeats 3]

A SETTING is NETWORK:RTOL, NETWORK `full` or `small` and RTOL `default` or a
number, for example `full:default` or `small:1e-5`; the low-T atol is
PRyMordial's own (1e-15 full, 1e-11 small). Prints each solve with the
1-minute load average, the load average before and after, and a table of
medians and ratios; appends each solve to cost.csv.
"""

import argparse
import csv
import os
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import tools.bbn_from_store as T  # noqa: E402
from Units import Planck_units  # noqa: E402
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology  # noqa: E402
from CosmologyModels.LambdaCDM import Planck2018  # noqa: E402

STORE = os.path.expanduser("~/ChamPBH-stores/science-2026.6.0")
HERE = Path(__file__).resolve().parent


def parse(s):
    net, rtol = s.split(":")
    if net not in ("full", "small"):
        raise SystemExit(f"unknown network in {s}")
    return net == "small", None if rtol == "default" else float(rtol)


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

    # warm-up, discarded, one per network
    T.solve_SM_baseline(False)
    T.solve_SM_baseline(True)

    walls = {(s, inp): [] for s in args.settings for inp in ("SM", "control")}
    commit = T._commit()
    for rep in range(args.repeats):
        for s in args.settings:
            small, rtol = parse(s)
            out_sm, _ = T.solve_SM_baseline(small, rtol)
            out_c, _ = T.solve_history_variant(
                "prod",
                logT,
                r,
                rho_SM,
                Tmin,
                Tmax,
                "cost",
                small_network=small,
                rtol=rtol,
            )
            for inp, out in (("SM", out_sm), ("control", out_c)):
                walls[(s, inp)].append(out["wall_s"])
                load1 = os.getloadavg()[0]
                print(
                    f"  rep {rep} {s:>14s} {inp:7s} {out['status']} Yp={out['Yp']} "
                    f"DoH={out['DOverH']} wall={out['wall_s']:.2f} s load={load1:.2f}",
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
                    "load1": load1,
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
