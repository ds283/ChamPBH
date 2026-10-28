# Planning probe for inelastic-reflection 04: the relativistic margin, post hoc, on prompt 03's
# histories (code at d1d5ccf), M = 0.5 M_P, n = 1, untuned at the model's seed, to T_CMB. Read-only.
#   per generation j (candidates with alpha_eff > 0), README §2.2's charter for 04:
#     lambda_j  = -d ln V''/dphi at the turning point (the third derivative of ln V analytic, n general)
#     ln k_ex,j = ln lambda_j + ln|v_j| - ln(2 sqrt 2), |v_j| = H_E |pi| at the drain point, before it
#               = ln lambda_j + (log_rho_kin + ln 2)/2 - ln(2 sqrt 2)
#     ln m_ta,j = ln m_eff at the turning point; ell_j = ln(k_ex,j/m_ta,j)
#     ln q_j    = ln k_ex,j + N_j (comoving, a = e^N, Einstein frame)
#   margin_j(set) = min over the points of `set` with N >= N_j of ln q_j - ln m_eff(N) - N, for:
#     wall:  the wall-side turning points (the transient mass at each later wall impact)
#     drain: the drain points, pre-drain states (|pi| maximum of each drained half-oscillation: the
#            field off the wall), with today's state
#     dense: S points per accepted step (the whole trajectory)
#     today: the history's last state alone
#   and the history's margin = min over j. Energy weights: Delta K_j / K(today).
# Usage: margin.py ALPHA S BETA [BETA ...]
import sys
import numpy as np
from math import exp, log, sqrt
from ComputeTargets.tests import test_injection as T
from Quadrature.supervisors.ScalarField import StateVector

SM = T.SM
LN10 = log(10.0)
alpha = float(sys.argv[1]); S = int(sys.argv[2])
for beta in [float(b) for b in sys.argv[3:]]:
    data, r, rec = T.integrate_captured(beta, 0.5, alpha, record_rhs=True)
    policy = rec.policy; rm = policy.model; u = policy.units
    hubble = SM.HubblePolicy(rm, u)
    M = rm._model._M_float; n = rm._model._n

    def lam(phi):  # -d ln V''/dphi, V'' = V q, q = (ln V)'' + (ln V)'^2
        uu = (M / phi) ** n
        d1 = -n * uu / phi; d2 = n * (n + 1) * uu / phi**2; d3 = -n * (n + 1) * (n + 2) * uu / phi**3
        q = d2 + d1 * d1; dq = d3 + 2.0 * d1 * d2
        return -(d1 + dq / q)

    def lm_H(N, y):  # (ln m_eff, ln H_E) at (N, y), the ledger as it stands at the end
        st = StateVector._make(y); d = policy(N, st)
        lm2 = SM.log_m_eff_squared(rm, st.phi_Einstein, st.log_rhorad_Einstein, st.log_fm, d.Sigma)
        return 0.5 * lm2, log(hubble(d, st).H_Einstein)

    gens = []
    for i in r.injections:
        if i.alpha_eff <= 0.0:
            continue
        lkex = log(lam(i.phi_turn)) + 0.5 * (i.log_rho_kin + log(2.0)) - log(2.0 * sqrt(2.0))
        lmta, _ = lm_H(i.N_turn, r.solution(i.N_turn))
        y = np.array(r.solution(i.N)); y[1] = i.pi_before
        lmb, lHb = lm_H(i.N, y)
        gens.append(dict(N=i.N, lq=lkex + i.N, lkex=lkex, lmta=lmta, lmb=lmb, lHb=lHb, dK=i.delta_K,
                         TJ=exp(y[4]) / u.MeV))
    K = sum(g["dK"] for g in gens)

    def points(kind):
        if kind == "wall":
            return [(t.N, lm_H(t.N, r.solution(t.N))[0]) for t in r.turning_points if t.kind == "wall"]
        if kind == "drain":
            out = []
            for g in gens:
                out.append((g["N"], g["lmb"]))
            out.append((r.N_final, lm_H(r.N_final, r.solution(r.N_final))[0]))
            return out
        if kind == "today":
            return [(r.N_final, lm_H(r.N_final, r.solution(r.N_final))[0])]
        out = []
        ts = r.solution.ts
        for k, it in enumerate(r.solution.interpolants):
            Ns = np.linspace(ts[k], ts[k + 1], S); Y = it(Ns)
            out += [(Ns[j], lm_H(Ns[j], Y[:, j])[0]) for j in range(S)]
        return out

    print(f"# alpha={alpha} beta={beta} generations={len(gens)} steps={r.accepted_steps} N_final={r.N_final:.4f}")
    ell = np.array([g["lkex"] - g["lmta"] for g in gens])
    birth = np.array([g["lkex"] - g["lmb"] for g in gens])
    kH = np.array([g["lkex"] - g["lHb"] for g in gens]); mH = np.array([g["lmb"] - g["lHb"] for g in gens])
    print(f"# ell = ln(k_ex/m_ta)          min/median/max {ell.min():.3f}/{np.median(ell):.3f}/{ell.max():.3f}")
    print(f"# ln(k_ex/m) at the drain point min/median/max {birth.min():.3f}/{np.median(birth):.3f}/{birth.max():.3f}")
    print(f"# ln(k_ex/H_E), ln(m/H_E) at the drain point: median {np.median(kH):.3f}, {np.median(mH):.3f}")
    for kind in ("wall", "drain", "dense", "today"):
        P = sorted(points(kind)); PN = np.array([p[0] for p in P]); f = np.array([p[1] + p[0] for p in P])
        suf = np.maximum.accumulate(f[::-1])[::-1]
        marg = []
        for g in gens:
            k = int(np.searchsorted(PN, g["N"], side="left"))
            marg.append(g["lq"] - suf[k] if k < len(P) else np.inf)
        marg = np.array(marg); w = np.array([g["dK"] for g in gens]) / K
        j = int(np.argmin(marg))
        print(f"{kind:6s} margin={marg[j]:8.3f} (p/m={exp(marg[j]):.3e}) set by generation {j} at T_J={gens[j]['TJ']:.4e} MeV;"
              f" generations below ln 10: {(marg < LN10).sum()}/{len(gens)}, carrying {w[marg < LN10].sum():.3e} of rho_chi;"
              f" below 0: {(marg < 0).sum()}, carrying {w[marg < 0].sum():.3e}")
    sys.stdout.flush()
