"""Integrate the notebook's histories with the pipeline's own integrator (ODEPolicy, ODERHS,
integrate_scalar_history), with no Ray and no datastore. Run from the repository root.

Args: out_dir, betas (comma list), variant, phi_i [Mp], N_stop, [rtol]
  variant  pipe    : the pipeline's production cosmology (QCD_Cosmology, Xav table)
           nbsig   : as pipe, but w(T) replaced by the notebook's Sigma (sampled 2000/decade)
           nbthermo: as nbsig, and the T_J law's gamma = dln g_s/3 dln T replaced by the notebook's
                     (|T g_s'/g_s|/3 of its linear interpolant)
The notebook's thermodynamic grid is read from $NB_THERMO_GRID (nb_thermo.wls).
Parameters are the notebook's: M = 1e-2 Mp, v0 = 2.16 meV, T_i = 2e4 GeV, pi_i = 0.
The history is integrated to N_stop and cut where phi = 0.1 Mp, the notebook's WhenEvent.
"""

import json
import os
import sys
import time
from math import log, exp

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.getcwd())
import dkstub  # noqa: F401,E402

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from scipy.optimize import brentq  # noqa: E402

from CosmologyConcepts import (
    temperature,
    M_value,
    Lambda_value,
    beta_value,
)  # noqa: E402
from CosmologyConcepts.Potentials.ExponentialPotential import (
    ExponentialPotential,
)  # noqa: E402
from CosmologyConcepts.ConformalCouplings.ExponentialCoupling import (
    ExponentialCoupling,
)  # noqa: E402
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology  # noqa: E402
from CosmologyModels.LambdaCDM import Planck2018  # noqa: E402
from ComputeTargets.ScalarModel import (  # noqa: E402
    ODEPolicy,
    ODERHS,
    integrate_scalar_history,
    StepControl,
)
from Quadrature.supervisors.ScalarField import (  # noqa: E402
    ScalarFieldIntegrationSupervisor,
    StateVector,
)
from Units import Planck_units  # noqa: E402

out = sys.argv[1]
betas = [float(b) for b in sys.argv[2].split(",")]
variant = sys.argv[3]
phi_i = float(sys.argv[4])
N_stop = float(sys.argv[5])
rtol = float(sys.argv[6]) if len(sys.argv) > 6 else 1e-8
os.makedirs(out, exist_ok=True)

units = Planck_units()
Mp, GeV = units.PlanckMass, units.GeV
base = QCD_Cosmology(0, units, Planck2018())


class Swapped:
    """Delegate to the production cosmology, replacing w and optionally the T_J law's g_s terms."""

    def __init__(self, base, nbgrid, swap_gamma):
        self._b = base
        g = pd.read_csv(nbgrid)
        self._lnT = np.log(g.T_GeV.values)
        self._Sig = g.Sigma.values
        self._gam = np.abs(g.dgs_dlnT.values / g.gs_interp.values) / 3.0
        self._swap_gamma = swap_gamma

    def __getattr__(self, k):
        return getattr(self._b, k)

    def w(self, T):
        x = log(T / GeV)
        if x <= self._lnT[0] or x >= self._lnT[-1]:
            return 1.0 / 3.0
        return (1.0 - float(np.interp(x, self._lnT, self._Sig))) / 3.0

    def G_s(self, T):
        return 1.0 if self._swap_gamma else self._b.G_s(T)

    def dG_s_dlogT(self, T):
        if not self._swap_gamma:
            return self._b.dG_s_dlogT(T)
        # with G_s = 1, dG_s_dlogT / G_s / 3 is gamma itself
        return 3.0 * float(np.interp(log(T / GeV), self._lnT, self._gam))


class Exact(Swapped):
    """The notebook's thermodynamics reproduced exactly: Sigma from its 20000/decade samples, and gamma
    from its own g_s interpolant (linear in T between the nodes of Gstarsdata, so g_s' is constant on
    each cell), with the notebook's |.|. Files from nb_export_exact.wls, in $NB_EXACT_DIR.
    """

    def __init__(self, base, d):
        self._b = base
        s = pd.read_csv(f"{d}/nb_sigma_fine.csv")
        self._lnT, self._Sig = np.log(s.T_GeV.values), s.Sigma.values
        n = pd.read_csv(f"{d}/nb_gs_nodes.csv")
        self._Tn, self._gn = n.T_GeV.values, n.gs.values
        self._slope = np.diff(self._gn) / np.diff(self._Tn)
        self._swap_gamma = True

    def dG_s_dlogT(self, T):
        t = T / GeV
        k = int(np.clip(np.searchsorted(self._Tn, t) - 1, 0, len(self._slope) - 1))
        g = self._gn[k] + self._slope[k] * (t - self._Tn[k])
        return t * abs(self._slope[k] / g)  # with G_s = 1 this is 3 gamma


if variant == "pipe":
    cosmo = base
elif variant == "nbexact":
    cosmo = Exact(base, os.environ["NB_EXACT_DIR"])
else:
    cosmo = Swapped(base, os.environ["NB_THERMO_GRID"], variant == "nbthermo")

tag = sys.argv[2].replace(",", "_")[:40]
summary = []
for beta in betas:
    pot = ExponentialPotential(
        0, M_value(0, 1e-2 * Mp), Lambda_value(0, 2.16e-3 * units.eV), 1, units
    )
    cpl = ExponentialCoupling(0, beta_value(0, beta), units)
    T_i = 2e4 * GeV

    # compute_scalar_model's initial state
    lnrhoJ = log(np.pi**2 / 30) + 4 * log(T_i) + log(base.G_rho(T_i))
    lnrhoE = lnrhoJ + 4 * cpl.log_Omega(phi_i * Mp)
    lnfm = log(base.rho_m0) + 3 * log(1 + base.z(T_i)) - lnrhoJ
    s0 = StateVector(
        phi_Einstein=phi_i * Mp,
        pi_Einstein=0.0,
        log_rhorad_Einstein=lnrhoE,
        log_fm=lnfm,
        log_T_Jordan=log(T_i),
    )
    pol = ODEPolicy("cmp", cosmo, pot, cpl)
    rhs = ODERHS("cmp", pol)

    t0 = time.perf_counter()
    with ScalarFieldIntegrationSupervisor(
        units, temperature(0, T_i), temperature(1, 1e-13 * GeV), label="cmp"
    ) as sup:
        res = integrate_scalar_history(
            rhs,
            sup,
            s0,
            0.0,
            log(1e-13 * GeV),
            StepControl(atol=rtol, rtol=rtol),
            N_failsafe=1000.0,
            policy=pol,
            N_stop=N_stop,
        )
    wall = time.perf_counter() - t0
    sol = res.solution

    Ng = np.arange(0.0, res.N_final, 0.001)
    Y = sol(Ng)
    phi = Y[0] / Mp
    k = np.flatnonzero(phi <= 0.1)
    N_end = (
        brentq(lambda n: sol(n)[0] / Mp - 0.1, Ng[k[0] - 1], Ng[k[0]], xtol=1e-12)
        if len(k)
        else None
    )

    # T_J turning points: zeros of 1 + beta y
    g = 1 + beta * Y[1] / Mp
    tps = []
    for i in np.flatnonzero(g[1:] * g[:-1] < 0):
        n0 = brentq(lambda n: 1 + beta * sol(n)[1] / Mp, Ng[i], Ng[i + 1], xtol=1e-13)
        if N_end is not None and n0 > N_end:
            break
        tps.append(
            dict(
                N=n0,
                T_J_GeV=exp(sol(n0)[4]) / GeV,
                phi_Mp=sol(n0)[0] / Mp,
                type="min" if g[i] > 0 else "max",
            )
        )

    N_last = N_end if N_end is not None else res.N_final
    Ns = np.round(np.arange(0.0, N_last, 0.01), 10)
    Ys = sol(Ns)
    pd.DataFrame(
        dict(
            N=Ns,
            phi=Ys[0] / Mp,
            phiprime=Ys[1] / Mp,
            lnTJ=Ys[4] - log(GeV),
            lnrhoE=Ys[2],
            lnfm=Ys[3],
        )
    ).to_csv(f"{out}/b{beta:.5f}_{variant}.csv", index=False, float_format="%.12g")

    summary.append(
        dict(
            beta=beta,
            variant=variant,
            phi_i=phi_i,
            rtol=rtol,
            wall_s=round(wall, 2),
            N_end=N_end,
            T_J_end_GeV=exp(sol(N_last)[4]) / GeV,
            accepted_steps=res.accepted_steps,
            reflections=len(res.reflections),
            turning_points=tps,
        )
    )
    first = tps[0] if tps else None
    print(
        f"beta={beta:.5f} {variant} rtol={rtol:g} wall={wall:.1f}s steps={res.accepted_steps} "
        f"N_end={N_end} first="
        + (
            f"{first['type']} N={first['N']:.5f} T={first['T_J_GeV']:.6g}"
            if first
            else "none"
        ),
        flush=True,
    )

with open(f"{out}/summary_{variant}_{tag}_rtol{rtol:g}.json", "w") as f:
    json.dump(summary, f, indent=1)
