"""
one-loop-drain prompt 05, Derive gate §2.1 (orchestrator note 05, rulings 2 and 3): every kernel and moment of
ComputeTargets/quanta_spectrum.py against direct quadrature, on a dense set of points spanning each table and beyond it,
at c_a = 1, 0 and 10.

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/derive-05/kernels_05.py > .documents/one-loop-drain/derive-05/out/kernels.txt

Prints:
  - the constants, kappa's five values (the fit, ruling 2) and the one taken;
  - I, J and M_k (closed form) against quad, with quad's stated error;
  - G at every node and every midpoint of its table, and at 200 points beyond each end (to four decades beyond); Omega at
    every node and midpoint of its table on [0, U], at every fourth of them mirrored to u < 0, and at 100 points beyond
    each end (to |u| = U + 10): the largest relative difference of each against quad, inside the table and beyond it,
    and quad's largest stated error;
  - the asymptotes at the tables' ends, term by term: each kept term's size relative to the leading one, and the
    residual of the series against quad;
  - the table build time.
The stop (prompt §2.1): a difference of 1e-6 or more, or an asymptote that misses 1e-7 at its end.
"""

import time
from math import exp, log

import numpy as np

from ComputeTargets import quanta_spectrum as qs

SCALES = (1.0, 0.0, 10.0)


def main():
    print(f"SPECTRUM_MODEL {qs.SPECTRUM_MODEL!r}; associated end: {qs.ASSOC_END}")
    print(
        f"x_p = {qs.X_PEAK!r}, x_* = {qs.X_STAR!r}, coefficient {qs.ASSOC_COEFFICIENT!r} (50 - 1)"
    )
    print("kappa_L = ln(|beta|^2_max,L / 1e-20) / (10 - x_p):")
    for L, k in qs.KAPPA_BY_L.items():
        print(f"  L = {L:3d}: |beta|^2_max = {qs.BETA2_MAX_BY_L[L]:.3g}, kappa = {k!r}")
    vals = list(qs.KAPPA_BY_L.values())
    print(
        f"  taken: L = {qs.KAPPA_FIT_L} (the median row), kappa = {qs.KAPPA!r}; spread {min(vals):.4f}-{max(vals):.4f}"
    )
    print(
        f"grids: ln y in [ln {qs.G_Y_MIN:g}, ln {qs.G_Y_MAX:g}] step {qs.G_LOG_Y_STEP}; u in [0, {qs.OMEGA_U_MAX}] step "
        f"{qs.OMEGA_U_STEP} (u < 0 by Omega(u) = e^(4u) Omega(-u))"
    )
    print()
    verdict = True
    for c in SCALES:
        s = qs.QuantaSpectrum(c)
        print(f"==== c_a = {c:g} (C = {s.C!r}) ====")
        t0 = time.perf_counter()
        s._G_table()
        t1 = time.perf_counter()
        s._Omega_table()
        t2 = time.perf_counter()
        print(f"tables built: G {t1 - t0:.3f} s, Omega {t2 - t1:.3f} s")
        I, eI = s.I_quad()
        J, eJ = s.J_quad()
        print(
            f"I = {s.I!r} (quad {I!r}, rel diff {s.I / I - 1:.2e}, stated {eI:.1e}); "
            f"J = {s.J!r} (quad {J!r}, rel diff {s.J / J - 1:.2e}, stated {eJ:.1e})"
        )
        worst_m = 0.0
        for k in range(1, 11):
            m, e = s.moment_quad(k)
            worst_m = max(worst_m, abs(s.moment(k) / m - 1.0))
        print(f"M_1..M_10: largest rel diff against quad {worst_m:.2e}")
        verdict &= (
            worst_m < 1e-6 and abs(s.I / I - 1) < 1e-6 and abs(s.J / J - 1) < 1e-6
        )

        lo, hi = log(qs.G_Y_MIN), log(qs.G_Y_MAX)
        n = int(round((hi - lo) / qs.G_LOG_Y_STEP))
        nodes = np.linspace(lo, hi, n + 1)
        inside = np.concatenate([nodes, 0.5 * (nodes[:-1] + nodes[1:])])
        beyond = np.concatenate(
            [
                np.linspace(lo - log(1e4), lo, 200, endpoint=False),
                np.linspace(hi, hi + log(1e4), 201)[1:],
            ]
        )
        for name, pts in (("inside", inside), ("beyond", beyond)):
            worst, where, qerr = 0.0, None, 0.0
            for t in pts:
                ref, e = s.G_quad(exp(t))
                d = abs(float(s.G_of_log_y(t)) / ref - 1.0)
                qerr = max(qerr, e)
                if d > worst:
                    worst, where = d, t
            print(
                f"G {name:6s}: {len(pts):5d} points, largest rel diff {worst:.2e} at ln y = {where:.4f}; quad's stated "
                f"error <= {qerr:.1e}"
            )
            verdict &= worst < 1e-6 and qerr < 1e-9

        U = qs.OMEGA_U_MAX
        m = int(round(U / qs.OMEGA_U_STEP))
        un = np.linspace(0.0, U, m + 1)
        inside = np.concatenate([un, 0.5 * (un[:-1] + un[1:])])
        inside = np.concatenate([inside, -inside[1::4]])
        beyond = np.concatenate(
            [np.linspace(U, U + 10.0, 101)[1:], -np.linspace(U, U + 10.0, 101)[1:]]
        )
        for name, pts in (("inside", inside), ("beyond", beyond)):
            worst, where, qerr = 0.0, None, 0.0
            for u in pts:
                ref, e = s.Omega_quad(u)
                d = abs(float(s.Omega(u)) / ref - 1.0)
                qerr = max(qerr, e)
                if d > worst:
                    worst, where = d, u
            print(
                f"Omega {name:6s}: {len(pts):5d} points, largest rel diff {worst:.2e} at u = {where:.4f}; quad's "
                f"stated error <= {qerr:.1e}"
            )
            verdict &= worst < 1e-6 and qerr < 1e-9

        # the asymptotes, term by term at the grids' ends
        y = qs.G_Y_MIN
        ref, _ = s.G_quad(y)
        lead = s.moment(1) * y * y / 2.0 / s.I
        cubic = -s.C * y**3 / 3.0 / s.I
        quart = s._B4 * y**4 / s.I
        res = float(s.G_small(y)) / ref - 1.0
        print(
            f"G small-y at y = {y:g}: leading M_1 y^2/2I = {lead:.6e}; y^3 term {cubic / lead:+.2e}, y^4 term "
            f"{quart / lead:+.2e} relative; series against quad {res:+.2e} (left out: O(y^4 ln y) relative, "
            f"{y**4 * log(qs.X_PEAK / y) / 8.0 / s.moment(1):.1e})"
        )
        verdict &= abs(res) < 1e-7
        y = qs.G_Y_MAX
        ref, _ = s.G_quad(y)
        lead = s.moment(2) * y / s.I
        terms = [
            -1.0,
            s.moment(4) / (2 * y) / s.I,
            -s.moment(6) / (8 * y**3) / s.I,
            s.moment(8) / (16 * y**5) / s.I,
        ]
        res = float(s.G_large(y)) / ref - 1.0
        left = 5.0 * s.moment(10) / (128.0 * y**7) / s.I
        print(
            f"G large-y at y = {y:g}: leading M_2 y/I = {lead:.6e}; -1, M_4/2y, -M_6/8y^3, M_8/16y^5 relative: "
            + ", ".join(f"{t / lead:+.2e}" for t in terms)
            + f"; series against quad {res:+.2e} (left out <= 5 M_10/128 y^7 I = {left / lead:.1e} relative)"
        )
        verdict &= abs(res) < 1e-7
        u = U
        ref, _ = s.Omega_quad(u)
        res = exp(float(s.log_Omega_large(u))) / ref - 1.0
        a = s.C * exp(u) * s.moment(2) / s.I
        b = exp(-2 * u) * s.moment(5) / s.I
        print(
            f"Omega large-u at u = {u:g}: C e^u M_2/I = {a:.4e}, e^(-2u) M_5/I = {b:.4e}; series against quad "
            f"{res:+.2e} (left out of order e^(-kappa x_p e^u) = e^(-{qs.KAPPA * qs.X_PEAK * exp(u):.0f}))"
        )
        verdict &= abs(res) < 1e-7
        print()
    print(
        "RESULT:",
        (
            "holds (every difference below 1e-6, every asymptote within 1e-7 at its end)"
            if verdict
            else "FAILS"
        ),
    )


if __name__ == "__main__":
    main()
