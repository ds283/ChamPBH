"""
power-law-wall prompt 07b §1.6 (P80): the power wall's law, shown unchanged by the new k_ex.

At every row of 02's stage2_scan.txt (rechain.power_rows), the regime and ell in each form (kexchain), then the
fraction three ways, each compared with ==:
  - fit_fn.fraction_power(n, ln R) (the copy in wall/), which reads n and ln R only;
  - production's InversePowerModel._drain_fraction(normalization, regime, ell, ln E, ln R) at the row's ln R with the
    regime and ell of the old form, and again with those of the new form (the model at laboratory M, beta 2, the maps'
    tuned ln V0; ln E = ln V0 + ln x).
The rows whose regime moves are listed with the three values.

The gate Law's reference (03's fit_fn.py gate) is re-run separately, unchanged: 02's own source/wall/fit_fn.py with
python -B (ruling 2), its output diffed against .documents/power-law-wall/law-03/gate_law.txt (README of source/kex/).

    PYTHONPATH=. ./venv/bin/python -B prompts/power-law-wall/source/kex/powerlaw.py \\
        > .documents/power-law-wall/kex-07b/powerlaw.txt
"""

import os
import sys
import time
from math import exp

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import kexchain as kc  # noqa: E402
import rechain  # noqa: E402

FF = kc.load_aside("wall", ("fit_fn",), "w02")["fit_fn"]

from CosmologyConcepts import M_value, beta_value  # noqa: E402
from CosmologyConcepts.Models.InversePowerModel import InversePowerModel  # noqa: E402
from Units import Planck_units  # noqa: E402

LNV0 = (
    -276.63072859401797
)  # the maps' tuned ln V0 (wall-02/map, every history at laboratory M)


def main():
    t0 = time.time()
    print(f"# powerlaw.py; start: {os.popen('uptime').read().strip()}")
    units = Planck_units()
    models = {
        n: InversePowerModel(
            0, M_value(0, 4e-31 * units.PlanckMass), n, beta_value(0, 2.0), units
        )
        for n in (1, 2)
    }
    rows = rechain.power_rows()
    same_all, moved, count = 0, 0, 0
    print(
        "# set              n      ln R      ln x | regime (lambda, v3)  ell (lambda, v3) | fraction_power | production, "
        "old labels | production, new labels | all equal"
    )
    for r in rows:
        w = rechain.wall_of(r)
        R = exp(r["lnR"])
        regs, ells = [], []
        for form in kc.FORMS:
            ch = kc.chain(w, R, form)
            regs.append(kc.regime(r["lnR"], ch))
            ells.append(None if ch is None else ch[3])
        f_ref = FF.fraction_power(r["n"], r["lnR"])
        log_E = LNV0 + r["lnx"]
        f_old = models[r["n"]]._drain_fraction(LNV0, regs[0], ells[0], log_E, r["lnR"])
        f_new = models[r["n"]]._drain_fraction(LNV0, regs[1], ells[1], log_E, r["lnR"])
        same = f_ref == f_old == f_new
        same_all += same
        count += 1
        if regs[0] != regs[1]:
            moved += 1
            print(
                f"  {r['set']:15s} {r['n']} {r['lnR']:9.4f} {r['lnx']:9.4f} | {regs[0]:>9s} -> {regs[1]:<9s} "
                f"{'none' if ells[0] is None else format(ells[0], '.4f'):>7s} {ells[1]:7.4f} | {f_ref:.15f} | "
                f"{f_old:.15f} | {f_new:.15f} | {same}"
            )
    print(
        f"\n# {count} rows; {moved} change regime (listed above); fraction_power == production (old labels) == "
        f"production (new labels) at {same_all} of {count}"
    )
    print(
        "# S_n reads no k_ex: wall/stage1a_power.py's table integrates over k/omega_t on a fixed grid, "
        "`ks = np.geomspace(1e-3, 10.0, 241)` (its line 56 in source/wall/, line 59 in the copy), with "
        "`S_lnk(ks, b2) = 4 trapezoid(ks**4 * b2, ln ks)`; k_ex appears in that file only as the loop-off stage-2 "
        "check's grid end, `kref = n * sqrt(w.E) / 2.0` (line 89; 92 in the copy), not in S_n"
    )
    print(
        f"# powerlaw.py end ({time.time() - t0:.0f} s): {os.popen('uptime').read().strip()}"
    )


if __name__ == "__main__":
    main()
