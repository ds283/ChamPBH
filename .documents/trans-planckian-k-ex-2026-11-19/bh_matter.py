"""
The matter component at BBN if the kinetic energy at the first impact became matter redshifting as a_E^{-3}
(trans-planckian-k-ex note, 2026-11-19, §5).

All in the Einstein frame, where the code keeps rho_kin,E and the ledger: rho_m,E(N) = rho_kin,E(N_1) e^{-3(N - N_1)},
N the forward e-folds of a_E (raw_N); rho_R,E from each history's fields.csv. rho_m/rho_R is the same in both frames.
rho_m/s is formed in the Jordan frame at the last sample (today) and compared with Omega_c h^2 = 0.120. The first
impact is the first candidate in bounces.csv; it comes before any drain, so it is the same for every A and drain law.
Reads the A = 1 exports of var/power-law-wall/07d-r2 (git-ignored; on the power-law-wall worktree). Run from the
repository root: ./venv/bin/python <this file>.
"""

import csv, glob, re, sys
from math import log, exp
import numpy as np

LN_MP_GEV = log(2.4353235955263043e18)
RHO_DM_OVER_S_GEV = (
    0.120 * 1.0537e-5 / 2891.2
)  # Omega_c h^2 = 0.120; rho_crit/h^2 [GeV cm^-3]; s_0 [cm^-3]
roots = {
    "ipl": "var/power-law-wall/07d-r2/export-pl/QCD_Cosmology/inverse-power/one-loop-inverse-power-v1/A=1/csv",
    "exp": "var/power-law-wall/07d-r2/export-exp/QCD_Cosmology/exponential/one-loop-exponential-v2/A=1/csv",
}


def at_T(F, lnT_target):
    lnT = np.array([float(r["log_T_Jordan_GeV"]) for r in F])
    i = np.nonzero(lnT >= lnT_target)[0][-1]  # last sample still at or above the target
    w = (lnT[i] - lnT_target) / (lnT[i] - lnT[i + 1])
    g = lambda k: (1 - w) * float(F[i][k]) + w * float(F[i + 1][k])
    return g


print(
    f"{'wall':4s} {'n':>1s} {'beta':>4s} {'M/eV':>10s} | {'T1/GeV':>8s} {'regime':>9s} {'lnk/Mp':>6s} {'rk/rR':>8s} {'a_eff':>6s} | "
    f"{'m/R@1MeV':>9s} {'dNeff@1MeV':>10s} {'m/R@0.1MeV':>10s} | {'chi/R@1MeV':>10s} | {'(m/s)/(DM/s)':>12s}"
)
for wall, root in roots.items():
    for d in sorted(glob.glob(root + "/beta=*/M=*")):
        beta, M, n = re.search(r"beta=([^/]+)/M=([^/]+)eV_n=(\d)", d).groups()
        B = [
            r
            for r in csv.DictReader(open(d + "/bounces.csv"))
            if r["source"] == "candidate"
        ]
        F = list(csv.DictReader(open(d + "/fields.csv")))
        b = B[0]
        N1 = float(b["N"])
        lnrk = float(b["log_rho_kin_Mp4"])
        Ns = np.array([float(r["raw_N"]) for r in F])
        lnR = (
            np.array([float(r["log_rhorad_Einstein_GeV4"]) for r in F]) - 4 * LN_MP_GEV
        )
        lnR1 = np.interp(N1, Ns, lnR)
        r1 = exp(lnrk - lnR1)

        def ratio(g):
            N = g("raw_N")
            return r1 * exp(
                -3 * (N - N1) + lnR1 - (g("log_rhorad_Einstein_GeV4") - 4 * LN_MP_GEV)
            )

        g1 = at_T(F, log(1e-3))
        g01 = at_T(F, log(1e-4))
        q1 = ratio(g1)
        q01 = ratio(g01)
        dneff = q1 * g1("gstar_rho") / 1.75
        last = F[-1]
        Nl = float(last["raw_N"])
        ql = r1 * exp(
            -3 * (Nl - N1)
            + lnR1
            - (float(last["log_rhorad_Einstein_GeV4"]) - 4 * LN_MP_GEV)
        )
        Y = (
            ql
            * 0.75
            * float(last["T_Jordan_GeV"])
            * float(last["gstar_rho"])
            / float(last["gstar_s"])
        )
        print(
            f"{wall:4s} {n:>1s} {beta:>4s} {float(M):10.3g} | {exp(float(b['log_T_Jordan_GeV'])):8.3g} {b['regime']:>9s} {float(b['log_k_ex_Mp'] or 'nan'):6.1f} {r1:8.2e} {float(b['alpha_eff']):6.3f} | "
            f"{q1:9.2e} {dneff:10.2e} {q01:10.2e} | {g1('rho_chi_over_rho_rad'):10.2e} | {Y/RHO_DM_OVER_S_GEV:12.2e}"
        )
