"""
power-law-wall prompt 07b §1.1 (P75, P76) and the breakage record (§3): the definition of k_ex against Erickcek et al.
(arXiv:1310.5149) §V B, on the chain (walls.py's units, omega_t = 1) and through the production hooks.

  E64  Eq. (64), k_ex = (1/2) [V_D' V''' / (2 V'')]^{1/2} at phi_ta with V_D(phi_ta) = E, evaluated numerically at the
       "v3" chain's phi_ta with the dressing V_D = E (V''(phi)/V''(phi_ta))^2 (V_D' by a centred difference, step
       1e-6 phi_ta), against the chain's k_ex = (1/2) E^{1/2} |V'''/V''|; then with the code's own dressing
       m^4 ell/32 pi^2, ell = ln(k_ex/m) varying along the approach at fixed k_ex, whose ratio to the chain's k_ex is
       the factor the reduction leaves out, against its closed form (1 - 1/(4 ell))^{1/2}. Three points per wall.
       Tolerance 1e-7 relative (the centred difference's error, (h/phi)^2 times the derivatives' ratio, ~1e-10, and its
       rounding, eps/1e-6 ~ 2e-10).
  E71  the power wall: the chain's k_ex against Eq. (71), (n + 2)|phi_dot_M|/(2 sqrt 2 phi_ta) with |phi_dot_M| =
       (2E)^{1/2}, at three points per n; tolerance 1e-13 relative (two orders of the same rounding).
  E66  the exponential: k_ex(v3)/k_ex(lambda) at the v3 chain's phi_ta against the closed form of kexchain
       (ratio_exponential(u, n), u = phi_ta^{-n}), at three points per n; tolerance 1e-13 relative. Eq. (66) is the
       leading term: k_ex(lambda) = (n/2) (M/phi)^{n+1} E^{1/2}/M is -d ln V/dphi E^{1/2}/2 exactly, and u (ratio - 1)
       tends to 2(n + 1)/n.
  R16  phi_t, ln omega_t^2 and ln R unchanged: at three mapped candidates per wall (02's maps), production's
       classical_turning_point against kexchain.production_v3_classical, which differs from it only in the rate,
       compared with ==; the chain takes phi_t and omega_t from the wall, which no form reads.
  B    the breakage record: each of "v3_over_V1" (|V'''/V'|), "v3_double" (the 1/2 dropped) and "v3_at_phi_t" (the
       rate at phi_t inside the chain) put through E64, E71 and E66 at the same points, and through the §1.3 count of
       regime moves on crossover's 107 n = 1 rows.

Run from the repository root:

    PYTHONPATH=. ./venv/bin/python -B prompts/power-law-wall/source/kex/definition.py \\
        > .documents/power-law-wall/kex-07b/definition.txt
"""

import os
import sys
import time
from math import exp, log, sqrt

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import kexchain as kc  # noqa: E402
import rechain  # noqa: E402  (its rows, and the two directories' walls loaded aside)

from CosmologyConcepts import M_value, beta_value  # noqa: E402
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel  # noqa: E402
from CosmologyConcepts.Models.InversePowerModel import InversePowerModel  # noqa: E402
from Units import Planck_units  # noqa: E402

XO, WW = rechain.XO, rechain.WW
EXP_POINTS = {
    1: ((26.74, 52.151), (7.885, 23.833), (3.619, 13.791)),
    2: ((60.0, 20.0), (16.0, 20.0), (4.0, 12.0)),
}
POW_POINTS = {
    1: ((1e40, 40.0), (1e3, 40.0), (1e2, 29.5)),
    2: ((1e40, 30.0), (1e3, 28.5), (1e2, 20.0)),
}
MAPS = ".documents/power-law-wall/wall-02/map"


def exp_wall(L, n):
    return XO["walls"].Wall(L) if n == 1 else XO["walls"].WallN(L, n)


def pow_wall(x, n):
    return WW["walls"].PowerWall(x, n)


def e64(w, ch):
    """(k from Eq. (64) with V_D ~ V''^2, k with the code's dressing m^4 ell(phi)) at the chain's phi_ta."""
    p, m, kex, ell = ch
    h = 1e-6 * p
    v2p = w.V2(p)

    def vd(q):
        return w.E * (w.V2(q) / v2p) ** 2

    def vd_ell(q):
        # m^4 ell(q) with m^2 = V''(q) and ell(q) = ln(k_ex/m(q)) at fixed k_ex, normalized to E at phi_ta
        return w.E * (w.V2(q) / v2p) ** 2 * log(kex / sqrt(w.V2(q))) / ell

    out = []
    for f in (vd, vd_ell):
        d = (f(p + h) - f(p - h)) / (2.0 * h)
        out.append(0.5 * sqrt(abs(d * w.V3(p)) / (2.0 * v2p)))
    return out


def run_e64(form="v3"):
    worst = 0.0
    lines = []
    for wallname, pts, mk in (
        ("exponential", EXP_POINTS, exp_wall),
        ("power", POW_POINTS, pow_wall),
    ):
        for n, plist in pts.items():
            for a, lnR in plist:
                w = mk(a, n)
                ch = kc.chain(w, exp(lnR), form)
                if ch is None:
                    lines.append(f"  {wallname} n={n} ({a:g}, {lnR:g}): no chain")
                    continue
                k64, k64l = e64(w, ch)
                d = k64 / ch[2] - 1.0
                worst = max(worst, abs(d))
                fac = sqrt(1.0 - 1.0 / (4.0 * ch[3]))
                lines.append(
                    f"  {wallname:11s} n={n} {'L' if wallname == 'exponential' else 'x'}={a:<8g} lnR={lnR:<7g} "
                    f"ell={ch[3]:9.5f}  k_ex(chain) {ch[2]:.10e}  Eq.(64), V_D ~ V''^2: {k64:.10e} (rel {d:+.1e})  "
                    f"with ell(phi): ratio {k64l / ch[2]:.8f}, (1 - 1/4 ell)^1/2 = {fac:.8f} "
                    f"(diff {k64l / ch[2] - fac:+.1e})"
                )
    return worst, lines


def run_e71(form="v3"):
    worst = 0.0
    lines = []
    for n, plist in POW_POINTS.items():
        for x, lnR in plist:
            w = pow_wall(x, n)
            ch = kc.chain(w, exp(lnR), form)
            if ch is None:
                lines.append(f"  n={n} x={x:g} lnR={lnR:g}: no chain")
                worst = float("inf")
                continue
            phidot = sqrt(2.0 * w.E)
            k71 = (n + 2) * phidot / (2.0 * sqrt(2.0) * ch[0])
            d = ch[2] / k71 - 1.0
            worst = max(worst, abs(d))
            lines.append(
                f"  n={n} x={x:<6g} lnR={lnR:<5g} phi_ta/phi_t={ch[0]:.8e} ell={ch[3]:9.5f}  k_ex(chain) "
                f"{ch[2]:.15e}  Eq.(71) {k71:.15e}  rel {d:+.1e}"
            )
    return worst, lines


def run_e66(form="v3"):
    worst = 0.0
    lines = []
    for n, plist in EXP_POINTS.items():
        for L, lnR in plist:
            w = exp_wall(L, n)
            ch = kc.chain(w, exp(lnR), form)
            if ch is None:
                lines.append(f"  n={n} L={L:g} lnR={lnR:g}: no chain")
                worst = float("inf")
                continue
            p = ch[0]
            u = p ** (-n)
            k_old = kc.kex_of(w, p, "lambda")
            k66 = (n / 2.0) * p ** (-(n + 1)) * sqrt(w.E)
            closed = kc.ratio_exponential(u, n)
            got = ch[2] / k_old
            d = got / closed - 1.0
            worst = max(worst, abs(d))
            lines.append(
                f"  n={n} L={L:<6g} lnR={lnR:<7g} u=(M/phi_ta)^n={u:10.5f}  k_ex(v3)/k_ex(lambda) {got:.15f}  "
                f"closed form {closed:.15f} (rel {d:+.1e});  Eq.(66) leading term / k_ex(lambda) - 1 = "
                f"{k66 / k_old - 1.0:+.1e};  u (ratio - 1) = {u * (closed - 1.0):.5f} -> 2(n+1)/n = "
                f"{2.0 * (n + 1) / n:g}"
            )
    return worst, lines


def map_candidates(prefix, k=3):
    """(model, normalization, M_Mp, ln E, label) for k candidates (first, middle, last) of the first map file."""
    import glob

    path = sorted(glob.glob(os.path.join(MAPS, prefix + "*-a0.1.txt")))[0]
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
    units = Planck_units()
    cls = ExponentialModel if prefix.startswith("exp") else InversePowerModel
    model = cls(0, M_value(0, M * units.PlanckMass), n, beta_value(0, beta), units)
    picks = [rows[0], rows[len(rows) // 2], rows[-1]][:k]
    return [
        (model, norm, M * units.PlanckMass, norm + lnEV0, f"{base} #{i}")
        for i, lnEV0 in picks
    ]


def run_r16():
    lines = []
    bad = 0
    for prefix in ("exp-n1", "ip-n1", "ip-n2"):
        for model, norm, phi, log_E, label in map_candidates(prefix):
            ct = model.classical_turning_point(norm, phi, log_E)
            nv = kc.production_v3_classical(model, norm, phi, log_E)
            same = (
                ct.phi_t == nv[0]
                and ct.log_omega_t_squared == nv[1]
                and ct.log_R == nv[2]
            )
            bad += not same
            lines.append(
                f"  {label:30s} phi_t {ct.phi_t!r} | {nv[0]!r}; ln omega_t^2 {ct.log_omega_t_squared!r} | {nv[1]!r}; "
                f"ln R {ct.log_R!r} | {nv[2]!r}; bitwise {same}; ln k_ex old {ct.log_k_ex:.6f} new {nv[4]:.6f}, "
                f"ell_t old {ct.ell_t:.6f} new {nv[5]:.6f}"
            )
    return bad, lines


def moves_on_crossover(form):
    rows = [r for r in rechain.exponential_rows() if r["n"] == 1]
    n_moves = 0
    for r in rows:
        w = rechain.wall_of(r)
        a = kc.regime(r["lnR"], kc.chain(w, exp(r["lnR"]), "lambda"))
        b = kc.regime(r["lnR"], kc.chain(w, exp(r["lnR"]), form))
        n_moves += a != b
    return n_moves, len(rows)


def main():
    t0 = time.time()
    print(f"# definition.py; start: {os.popen('uptime').read().strip()}")
    print(
        "\n## E64: Eq. (64) at the v3 chain's phi_ta (tolerance 1e-7 relative); and the factor left out by V_D ~ V''^2"
    )
    worst, lines = run_e64()
    print("\n".join(lines))
    print(f"# E64 largest |rel| {worst:.1e}: {'holds' if worst <= 1e-7 else 'FAILS'}")
    print("\n## E71: the power wall's k_ex against Eq. (71) (tolerance 1e-13 relative)")
    worst, lines = run_e71()
    print("\n".join(lines))
    print(f"# E71 largest |rel| {worst:.1e}: {'holds' if worst <= 1e-13 else 'FAILS'}")
    print(
        "\n## E66: the exponential's new/old ratio against its closed form, and Eq. (66) as its leading term "
        "(tolerance 1e-13 relative)"
    )
    worst, lines = run_e66()
    print("\n".join(lines))
    print(f"# E66 largest |rel| {worst:.1e}: {'holds' if worst <= 1e-13 else 'FAILS'}")
    print(
        "\n## R16: phi_t, ln omega_t^2, ln R from production's classical_turning_point and from the v3 version (==)"
    )
    bad, lines = run_r16()
    print("\n".join(lines))
    print(f"# R16: {9 - bad} of 9 candidates bitwise in all three")
    print(
        "\n## B: the breakage record. Each broken form through E64, E71 and E66 at the same points (largest |rel|, "
        "against the tolerances above) and the count of regime moves on crossover's n = 1 rows (v3: below)"
    )
    m, tot = moves_on_crossover("v3")
    print(f"  v3 (the ruled form): E64/E71/E66 as above; regime moves {m} of {tot}")
    for form in kc.BREAKAGES:
        w64, _ = run_e64(form)
        w71, _ = run_e71(form)
        w66, _ = run_e66(form)
        m, tot = moves_on_crossover(form)
        caught = [
            name
            for name, v, tol in (
                ("E64", w64, 1e-7),
                ("E71", w71, 1e-13),
                ("E66", w66, 1e-13),
            )
            if not v <= tol
        ]
        print(
            f"  {form:12s}: E64 {w64:.2e}, E71 {w71:.2e}, E66 {w66:.2e}; regime moves {m} of {tot}; caught by "
            f"{', '.join(caught) if caught else 'NONE'}"
        )
    print(
        f"\n# definition.py end ({time.time() - t0:.0f} s): {os.popen('uptime').read().strip()}"
    )


if __name__ == "__main__":
    main()
