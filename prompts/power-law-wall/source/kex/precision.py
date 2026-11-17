"""
power-law-wall prompt 07b §1.2: the new form at production precision. At mapped candidates on each wall, the "v3" chain's
ell and phi_ta/phi_t (walls.py's units) against kexchain.production_v3_turnaround, a scratch evaluation of P75 through
the production hooks (_log_V, _d_logV_dphi, _d2_logV_dphi2; V''' from the log-space derivatives, the third by a
centred difference of _d2_logV_dphi2) inside AbstractModel.one_loop_turnaround's own loop and search, in Planck units.
Beside it, as the harness's own check, the "lambda" chain against production's one_loop_turnaround (02's
chain_vs_production.py (b), there 1.7e-14).

The candidates (each realized as 02's chain_vs_production.py realized them: the map's tuned ln V0, ln E = ln V0 +
ln(E/V0), the search from phi = 100 M, START):
  - the exponential, n = 1: 02's three exponential maps (exp-n1-*, laboratory M, beta 2, 2.9, 3.05; four candidates
    each), and four of crossover's cascade-P-2197-A0.01 (beta 1.185) near the floor, at L ~ 10, 6, 4.3 and 3.6;
  - the power wall, n = 1 and n = 2: 02's six fixed-law maps per n at alpha 0.1, the first and last candidate of each.
The chain's inputs are x = e^{ln E - ln V0} (L = ln(1 + x) on the exponential) and R = e^{ln R} with production's ln R.

Tolerance: 1e-9 relative in ell and in phi_ta/phi_t, as 02's (b): the fixed points' 1e-12, the root tolerances (4 eps
in ln phi at |ln phi| ~ 180, the chain's rtol 1e-14), and here the centred difference's ~1e-11 relative in the rate
(kexchain's docstring), which enters ell as 1e-11 absolute.

    PYTHONPATH=. ./venv/bin/python -B prompts/power-law-wall/source/kex/precision.py \\
        > .documents/power-law-wall/kex-07b/precision.txt
"""

import glob
import os
import sys
import time
from math import exp, log1p

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import kexchain as kc  # noqa: E402
import rechain  # noqa: E402

from CosmologyConcepts import M_value, beta_value  # noqa: E402
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel  # noqa: E402
from CosmologyConcepts.Models.InversePowerModel import InversePowerModel  # noqa: E402
from Units import Planck_units  # noqa: E402

MAPS = ".documents/power-law-wall/wall-02/map"
XO_OUT = "prompts/one-loop-drain/source/crossover/out"
TOL = 1e-9
# the search starts at phi = START M: at phi = M the late candidates of 2197 are already inside their turnaround
# (V''(M) >= m_ta^2 at the first iterate), so production returns None there; the root does not depend on the start
START = 100.0


def read_map(path):
    base = os.path.basename(path)[:-4]
    n = int(base.split("-")[1][1:])
    M = float(base.split("-M")[1].split("-b")[0])
    beta = float(base.split("-b")[1].split("-a")[0])
    norm, rows = None, []
    for line in open(path):
        if "tuned normalization ln V0 = " in line:
            norm = float(line.split("ln V0 = ")[1].split(",")[0])
        elif line[:1] == " " and len(line.split()) >= 15:
            f = line.split()
            rows.append((int(f[0]), float(f[3])))
    return base, n, M, beta, norm, rows


def candidates():
    units = Planck_units()
    out = {"exponential": [], "power n=1": [], "power n=2": []}
    for path in sorted(glob.glob(os.path.join(MAPS, "exp-n1-*.txt"))):
        base, n, M, beta, norm, rows = read_map(path)
        model = ExponentialModel(
            0, M_value(0, M * units.PlanckMass), n, beta_value(0, beta), units
        )
        for i in sorted({0, len(rows) // 3, 2 * len(rows) // 3, len(rows) - 1}):
            idx, lnEV0 = rows[i]
            out["exponential"].append(
                (f"{base} #{idx}", model, norm, START * M * units.PlanckMass, lnEV0, n)
            )
    # crossover's 2197 at A = 0.01 near the floor (its header's normalization; beta 1.185, M 4e-31 M_P)
    head = next(
        line
        for line in open(os.path.join(XO_OUT, "cascade-P-2197-A0.01.txt"))
        if line.startswith("# P-2197")
    )
    norm = float(head.split("normalization(ln V0)=")[1].split()[0])
    model = ExponentialModel(
        0, M_value(0, 4e-31 * units.PlanckMass), 1, beta_value(0, 1.185), units
    )
    for idx in (78, 128 + 28, 180, 182):
        for line in open(os.path.join(XO_OUT, "cascade-P-2197-A0.01.txt")):
            f = line.split()
            if f and f[0].isdigit() and int(f[0]) == idx:
                out["exponential"].append(
                    (
                        f"cascade-P-2197-A0.01 #{idx}",
                        model,
                        norm,
                        START * 4e-31 * units.PlanckMass,
                        float(f[5]),
                        1,
                    )
                )
                break
    for n in (1, 2):
        for path in sorted(glob.glob(os.path.join(MAPS, f"ip-n{n}-*-a0.1.txt"))):
            base, n_, M, beta, norm, rows = read_map(path)
            model = InversePowerModel(
                0, M_value(0, M * units.PlanckMass), n, beta_value(0, beta), units
            )
            for idx, lnx in (rows[0], rows[-1]):
                out[f"power n={n}"].append(
                    (
                        f"{base} #{idx}",
                        model,
                        norm,
                        START * M * units.PlanckMass,
                        lnx,
                        n,
                    )
                )
    return out


def main():
    t0 = time.time()
    print(f"# precision.py; start: {os.popen('uptime').read().strip()}")
    print(
        "# per candidate: ln(E/V0), ln R (production); ell and phi_ta/phi_t from production's one_loop_turnaround "
        "(lambda) and from the scratch v3 evaluation through the hooks, each against the chain in the same form; "
        f"tolerance {TOL:g} relative"
    )
    worst = {"lambda": 0.0, "v3": 0.0}
    count = {}
    for wallname, cands in candidates().items():
        print(f"\n## {wallname}: {len(cands)} candidates")
        count[wallname] = len(cands)
        for label, model, norm, phi, lnEV0, n in cands:
            log_E = norm + lnEV0
            ct = model.classical_turning_point(norm, phi, log_E)
            x = exp(log_E - norm)
            if wallname == "exponential":
                w = rechain.XO["walls"].Wall(log1p(x))
            else:
                w = rechain.WW["walls"].PowerWall(x, n)
            R = exp(ct.log_R)
            parts = []
            for form in ("lambda", "v3"):
                ch = kc.chain(w, R, form)
                if form == "lambda":
                    try:
                        ta = model.one_loop_turnaround(norm, phi, log_E)
                        prod = None if ta is None else (ta.phi_ta, ta.ell)
                    except Exception as e:  # noqa: BLE001
                        prod = f"raised ({type(e).__name__})"
                else:
                    try:
                        r = kc.production_v3_turnaround(model, norm, phi, log_E)
                        prod = None if r is None else (r[0], r[3])
                    except Exception as e:  # noqa: BLE001
                        prod = f"raised ({type(e).__name__})"
                if ch is None or not isinstance(prod, tuple):
                    parts.append(
                        f"{form}: chain {'none' if ch is None else format(ch[3], '.6f')} production {prod}"
                    )
                    if (ch is None) != (not isinstance(prod, tuple)):
                        worst[form] = float("inf")
                    continue
                d_ell = ch[3] / prod[1] - 1.0
                d_phi = (ch[0] / w.phi_t) / (prod[0] / ct.phi_t) - 1.0
                worst[form] = max(worst[form], abs(d_ell), abs(d_phi))
                parts.append(
                    f"{form}: ell {prod[1]:.12f} (chain {ch[3]:.12f}, rel {d_ell:+.1e}), phi_ta/phi_t rel {d_phi:+.1e}"
                )
            print(
                f"  {label:32s} ln(E/V0) {lnEV0:10.5f} ln R {ct.log_R:10.5f} | "
                + " | ".join(parts)
            )
    print(
        f"\n# counts {count}; largest relative difference: lambda (production's own) {worst['lambda']:.1e}, v3 (the "
        f"scratch hooks) {worst['v3']:.1e}; tolerance {TOL:g}: "
        f"{'holds' if max(worst.values()) <= TOL else 'FAILS'}"
    )
    print(
        f"# precision.py end ({time.time() - t0:.0f} s): {os.popen('uptime').read().strip()}"
    )


if __name__ == "__main__":
    main()
