# Planning probe for inelastic-reflection 03: the non-adiabaticity A_k = max |dm/dt|/m^2 over the
# half-oscillation (N_{k-1}, N_k] ending at each turning point k, post hoc on the dense output, at
# M = 0.5 M_P, untuned at the model's seed, to T_CMB (code at bc5314e). Read-only.
#   m^2 = V''(phi) + (d ln A/dphi)^2 rho_R,E (Sigma + f_m)   (exponential coupling: (ln A)'' = 0)
#   A   = H_E |d ln m^2/dN| / (2 m), d ln m^2/dN by central differences on S samples per step
# Usage: nonadiabaticity.py ALPHA BETA [BETA ...]
import sys
import numpy as np
from math import exp, log, sqrt
from ComputeTargets.tests import test_injection as T
from ComputeTargets.tests import golden_tuned_history as gth
import importlib
SM = importlib.import_module("ComputeTargets.ScalarHistory")
from CosmologyConcepts import M_value, beta_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from Quadrature.supervisors.ScalarField import StateVector

S = 33  # samples per step, ends included
cos = gth.make_cosmology(); u = cos.units; Mp = u.PlanckMass
alpha = float(sys.argv[1])
for beta in [float(b) for b in sys.argv[2:]]:
    data, r, _ = T.integrate_captured(beta, 0.5, alpha)
    model = ExponentialModel(0, M_value(0, 0.5 * Mp), 1, beta_value(0, beta), u)
    sd = model.seed(cos); rm = model.realize(sd.normalization, sd.phi_ref)
    policy = SM.ODEPolicy("probe", cos, rm, dark_radiation=data["dark_radiation"])
    hubble = SM.HubblePolicy(rm, u)
    b = rm.d_logOmega_dphi(1.0)

    def m2_H(N, y):
        st = StateVector._make(y)
        lv, d1, d2 = rm.log_V(st.phi_Einstein), rm.d_logV_dphi(st.phi_Einstein), rm.d2_logV_dphi2(st.phi_Einstein)
        Vpp = exp(lv) * (d2 + d1 * d1)
        Sigma = 1.0 - 3.0 * float(cos.w(exp(st.log_T_Jordan)))
        trace = exp(st.log_rhorad_Einstein) * (Sigma + exp(st.log_fm))
        H = hubble(policy(N, st), st).H_Einstein
        return Vpp + b * b * trace, Vpp, b * b * trace, H

    ts = r.solution.ts; interps = r.solution.interpolants
    # per step: max A on the step, and its N
    stepA = []
    for k, it in enumerate(interps):
        Ns = np.linspace(ts[k], ts[k + 1], S); ys = it(Ns)
        lm2 = []; Hs = []
        for j in range(S):
            m2, _, _, H = m2_H(Ns[j], ys[:, j]); lm2.append(log(m2)); Hs.append((H, m2))
        dl = np.gradient(np.array(lm2), Ns)
        A = [Hs[j][0] * abs(dl[j]) / (2.0 * sqrt(Hs[j][1])) for j in range(S)]
        mH = [sqrt(Hs[j][1]) / Hs[j][0] for j in range(S)]
        # max A over the step's samples: all; where m >= H; where m >= 10 H
        stepA.append(tuple(max([A[j] for j in range(S) if mH[j] >= c] or [0.0]) for c in (0.0, 1.0, 10.0)))
    tps = r.turning_points
    starts = [float(ts[0])] + [t.N for t in tps[:-1]]
    rows = []
    for t, N0 in zip(tps, starts):
        lo = int(np.searchsorted(ts, N0, side="right")) - 1
        hi = int(np.searchsorted(ts, t.N, side="left"))
        rng = range(max(lo, 0), max(hi, lo + 1))
        Ak = tuple(max(stepA[k][c] for k in rng) for c in range(3))
        k_t = int(np.searchsorted(ts, t.N, side="right")) - 1
        y = interps[min(k_t, len(interps) - 1)](t.N)
        m2, Vpp, tr, H = m2_H(t.N, y)
        rows.append((t.kind, t.N, exp(t.log_T_Jordan) / u.MeV, t.phi_Einstein / (0.5 * Mp), Ak, sqrt(m2) / H, Vpp / tr))
    print(f"# alpha={alpha} beta={beta} turning_points={len(tps)} steps={r.accepted_steps}")
    print("# kind N T_J/MeV phi/M A_k(all) A_k(m>=H) A_k(m>=10H) m/H@turn Vpp/trace@turn")
    for row in rows:
        print(f"{row[0]:7s} {row[1]:.6f} {row[2]:.4e} {row[3]:.4e} {row[4][0]:.3e} {row[4][1]:.3e} {row[4][2]:.3e} {row[5]:.3e} {row[6]:.3e}")
    for kind in ("wall", "plateau"):
        for c, lab in enumerate(("all", "m>=H", "m>=10H")):
            A = np.array([x[4][c] for x in rows if x[0] == kind])
            if len(A):
                print(f"# {kind} {lab}: n={len(A)} A_k min={A.min():.3e} median={np.median(A):.3e} max={A.max():.3e}")
    sys.stdout.flush()
