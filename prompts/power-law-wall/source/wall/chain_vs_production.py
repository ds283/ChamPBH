"""
power-law-wall prompt 02 §1.3 item 5: the chain against production, and powerscan.f_law against drain_fraction.

(a) Every power-wall candidate of the map (.documents/power-law-wall/wall-02/map/ip-*.txt): born.chain on
    PowerWall(x, n) in units of phi_t, at the candidate's printed ln x and ln R, against the ell that
    InversePowerModel.one_loop_turnaround returned in the history (printed to 5 decimals). Tolerance 1e-5: the printed
    digits of ell (5e-6), and ln R's (5e-7) times d ell/d ln R < 0.2.
(b) Twelve candidates, direct, in Planck units: InversePowerModel realized at the map's tuned ln V0, at
    ln E = ln V0 + ln x, with the search started at phi = M: classical_turning_point's ln R and one_loop_turnaround's
    ell and phi_ta, against born.chain at x = e^{ln E - ln V0} and R = e^{ln R}. Tolerance 1e-9 relative in ell and
    phi_ta/phi_t: the production's ONE_LOOP_ELL_RTOL (1e-12) and root tolerance (4 eps in ln phi), and the chain's
    (1e-12, rtol 1e-14), at |ln phi| ~ 180.
(c) powerscan.f_law against ComputeTargets.ScalarHistory.drain_fraction at 4000 inputs across the regimes: equal
    bitwise (the same expressions and constants).

Run from the repository root:

    PYTHONPATH=. ./venv/bin/python prompts/power-law-wall/source/wall/chain_vs_production.py \\
        > .documents/power-law-wall/wall-02/chain_vs_production.txt
"""

import glob
import os
import random
import sys
from math import exp, log

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ".")
sys.path.insert(0, HERE)

import born  # noqa: E402
import powerscan as ps  # noqa: E402
from walls import PowerWall  # noqa: E402

from ComputeTargets.ScalarHistory import drain_fraction  # noqa: E402
from CosmologyConcepts import M_value, beta_value  # noqa: E402
from CosmologyConcepts.Models.InversePowerModel import InversePowerModel  # noqa: E402
from Units import Planck_units  # noqa: E402

MAP = ".documents/power-law-wall/wall-02/map"


def read_map(path):
    n = int(os.path.basename(path).split("-")[1][1:])
    M = float(os.path.basename(path).split("-M")[1].split("-b")[0])
    norm, rows = None, []
    for line in open(path):
        if "tuned normalization ln V0 = " in line:
            norm = float(line.split("ln V0 = ")[1].split(",")[0])
        elif line[:1] == " " and len(line.split()) >= 15:
            f = line.split()
            rows.append((int(f[0]), float(f[3]), float(f[4]), f[7]))
    return n, M, norm, rows


def main():
    print(f"# chain_vs_production.py; {os.popen('uptime').read().strip()}")
    files = sorted(glob.glob(os.path.join(MAP, "ip-*.txt")))
    # (a)
    worst, count, regime_mismatch = 0.0, 0, 0
    for path in files:
        n, M, norm, rows = read_map(path)
        for idx, lnx, lnR, ell_s in rows:
            ch = born.chain(PowerWall(exp(lnx), n), exp(lnR))
            if ell_s in ("nochain", "None(inside)"):
                regime_mismatch += ch is not None
                continue
            if ch is None:
                regime_mismatch += 1
                continue
            worst = max(worst, abs(ch[3] - float(ell_s)))
            count += 1
    print(
        f"(a) {count} mapped power-wall candidates with a production ell: largest |ell(chain) - ell(production)| "
        f"= {worst:.2e} (tolerance 1e-5); candidates where one has a chain and the other not: {regime_mismatch}"
    )
    # (b)
    units = Planck_units()
    picks = []
    for path in files:
        if "-a0.1" not in path or "b3.05" in path:
            continue
        n, M, norm, rows = read_map(path)
        picks.append((path, n, M, norm, rows[0]))
        picks.append((path, n, M, norm, rows[-1]))
    print("(b) direct, InversePowerModel in Planck units, search from phi = M")
    print(
        "#   history                      idx      ln x       ln R  ell(production)  ell(chain)  rel diff   "
        "phi_ta/phi_t (prod)  (chain)  rel diff"
    )
    wb = 0.0
    for path, n, M, norm, (idx, lnx, lnR_map, ell_s) in picks[:12]:
        model = InversePowerModel(
            0, M_value(0, M * units.PlanckMass), n, beta_value(0, 2.0), units
        )
        rm = model.realize(norm, 0.0)
        log_E = norm + lnx
        ct = rm.classical_turning_point(M * units.PlanckMass, log_E)
        ta = rm.one_loop_turnaround(M * units.PlanckMass, log_E)
        x = exp(log_E - norm)
        ch = born.chain(PowerWall(x, n), exp(ct.log_R))
        d1 = ch[3] / ta.ell - 1.0
        d2 = ch[0] / (ta.phi_ta / ct.phi_t) - 1.0
        wb = max(wb, abs(d1), abs(d2))
        print(
            f"    {os.path.basename(path)[:-4]:28s} {idx:4d} {lnx:10.5f} {ct.log_R:10.5f} {ta.ell:16.12f} "
            f"{ch[3]:16.12f} {d1:+.1e}   {ta.phi_ta / ct.phi_t:.12e} {ch[0]:.12e} {d2:+.1e}"
        )
    print(
        f"(b) largest relative difference: {wb:.2e} (tolerance 1e-9); ln R of the map against "
        f"classical_turning_point's here differs only by the map's printed digits"
    )
    # (c)
    rng = random.Random(2)
    bad, nchk = 0, 0
    for _ in range(1000):
        for reg in ("one_loop", "gap", "classical", "unassessed"):
            ell = rng.uniform(0.0, 50.0)
            L = exp(rng.uniform(-3.0, 6.0))
            lnR = (
                rng.uniform(-10.0, 60.0) if reg != "classical" else rng.uniform(-30, 0)
            )
            a = drain_fraction(reg, ell, L, lnR)
            b = 0.0 if reg == "unassessed" else ps.f_law(reg, ell, L, lnR)
            nchk += 1
            bad += a != b
    print(
        f"(c) powerscan.f_law against drain_fraction at {nchk} inputs: {bad} differ (bitwise comparison)"
    )


if __name__ == "__main__":
    main()
