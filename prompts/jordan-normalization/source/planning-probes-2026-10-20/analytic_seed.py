"""
Planning probe 2: the analytic late-time minimum against the integrated fixed point.
Run from the ChamPBH root:  ./venv/bin/python <this> BETA M_MP PHI_INIT_MP [bbn]

Seeds (A(phi_ref) = 1 throughout, Planck2018 densities today):
  static   - field at rest at the V_eff minimum:  V(phi) = 3 Mp^2 H0^2 - rho_m0 - rho_r0
  tracking - field moving at the matter-era tracking speed phi' = 1.5 phi: Eq. 7 at A = 1
"""

import os
import sys
import time
from math import exp, log, sqrt

sys.path.insert(0, os.getcwd())
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import phimin_iter_v2 as pm  # noqa: E402
from ComputeTargets.BBNData import compute_BBN_data  # noqa: E402
from ComputeTargets.ScalarModel import (
    SampleValues,
    ScalarModelValue,
    compute_scalar_model,
)  # noqa: E402
from CosmologyConcepts import (
    Lambda_value,
    M_value,
    beta_value,
    redshift,
    temperature,
)  # noqa: E402
from CosmologyConcepts.FieldValues import phi_value, pi_value  # noqa: E402
from CosmologyConcepts.Potentials.ExponentialPotential import (
    ExponentialPotential,
)  # noqa: E402

units, cosmology, params, Mp, H0 = pm.units, pm.cosmology, pm.params, pm.Mp, pm.H0
rho_m0, rho_r0 = cosmology.rho_m0, cosmology.rho_r0
TOL = 1e-5


def analytic(beta, M, mode):
    """(Lambda, phi_ref) from the minimum of V_eff = V + rho_m0 A, A(phi_ref) = 1, n = 1."""
    Mf = M * Mp
    track = 0.0 if mode == "static" else 1.0
    phi = sqrt(Mf * Mp * (3 * Mp**2 * H0**2 - rho_m0) / (beta * rho_m0))
    for _ in range(200):
        pi = 1.5 * track * phi
        bp = 1.0 + beta * pi / Mp
        G = 1.0 - pi * pi / (6.0 * Mp * Mp)
        V = 3.0 * Mp**2 * H0**2 * G / bp**2 - rho_m0 - rho_r0
        new = sqrt(Mf * Mp * V / (beta * rho_m0))  # (M/phi^2) V = (beta/Mp) rho_m0
        if abs(new / phi - 1.0) < 1e-14:
            break
        phi = new
    Lambda4 = V * exp(-Mf / phi)
    return Lambda4**0.25, phi


def run(beta, M, phi_init, Lambda, phi_ref):
    pot = ExponentialPotential(0, M_value(0, M * Mp), Lambda_value(0, Lambda), 1, units)
    cpl = pm.Shifted(0, beta_value(0, beta), units, phi_ref)
    data = compute_scalar_model._function(
        cosmology,
        temperature(0, pm.T_INIT_GEV * units.GeV),
        temperature(1, params.T_CMB_Kelvin * units.Kelvin),
        phi_value(0, phi_init * Mp),
        pi_value(0, 0.0),
        pm.ZG,
        pot,
        cpl,
        task_label="probe2",
    )
    return pot, cpl, data


class _StandIn:
    def __init__(self, potential, coupling, values):
        self._cosmology = cosmology
        self.potential, self.coupling = potential, coupling
        self.T_Jordan_stop = temperature(1, params.T_CMB_Kelvin * units.Kelvin)
        self.values = values


class _Proxy:
    def __init__(self, m):
        self._m = m

    def get(self):
        return self._m


def bbn(pot, cpl, data):
    values = [
        ScalarModelValue(
            None, redshift(0, float(z.z)), **SampleValues._make(tuple(s))._asdict()
        )
        for z, s in zip(data["z_grid"], data["sample"])
    ]
    res = compute_BBN_data._function(
        _Proxy(_StandIn(pot, cpl, values)),
        task_label="probe2",
        small_network=True,
        wall_clock_limit=None,
    )
    return res.get("Yp_BBN"), res.get("DOverH"), res.get("failure_reason")


def iterate(beta, M, phi_init, Lambda, phi_ref, maxit=25):
    for it in range(maxit):
        r, wall, why = pm.one(beta, M, phi_init, Lambda, phi_ref)
        if r is None:
            return None, it, why
        if abs(phi_ref / r["phi_min"] - 1.0) < TOL and abs(r["h_ratio"] - 1.0) < TOL:
            return (Lambda, phi_ref), it + 1, None
        Lambda = (
            Lambda * (r["V_target"] / r["V"]) ** 0.25
            if r["V_target"] > 0
            else 0.5 * Lambda
        )
        phi_ref = r["phi_min"]
    return (Lambda, phi_ref), maxit, "not converged"


def main():
    beta, M, phi_init = map(float, sys.argv[1:4])
    do_bbn = len(sys.argv) > 4 and sys.argv[4] == "bbn"
    tag = f"beta={beta:g} M={M:g} phi*={phi_init:g}"
    meV = units.eV * 1e-3
    out = {}
    for mode in ("static", "tracking"):
        L, p = analytic(beta, M, mode)
        r, wall, why = pm.one(beta, M, phi_init, L, p)
        conv, nit, why2 = iterate(beta, M, phi_init, L, p)
        out[mode] = (L, p, r, conv, nit)
        print(
            f"{tag} seed={mode}: Lambda={L/meV:.5f} meV phi_ref={p/Mp:.6e} | one run: H/H0-1={r['h_ratio']-1:+.3e} "
            f"phi_ref/phi_min-1={p/r['phi_min']-1:+.3e} phi_ref/phi0-1={p/r['phi0']-1:+.3e} track={r['track']:.3f} "
            f"| from this seed: {nit} integrations ({why2 or 'converged'})",
            flush=True,
        )
    L0, p0 = out["static"][3]
    for mode in ("static", "tracking"):
        L, p = out[mode][0], out[mode][1]
        print(
            f"{tag} seed={mode} vs converged (Lambda={L0/meV:.5f} meV, phi_ref={p0/Mp:.6e}): "
            f"Lambda/Lambda*-1={L/L0-1:+.3e} Lambda^4 ratio-1={(L/L0)**4-1:+.3e} phi_ref/phi_ref*-1={p/p0-1:+.3e} "
            f"2 beta dphi/Mp={2*beta*(p-p0)/Mp:+.3e}",
            flush=True,
        )
    if do_bbn:
        for label, (L, p) in (
            ("converged", (L0, p0)),
            ("static", out["static"][:2]),
            ("tracking", out["tracking"][:2]),
        ):
            pot, cpl, data = run(beta, M, phi_init, L, p)
            Yp, DH, why = bbn(pot, cpl, data)
            print(f"{tag} BBN {label}: Yp={Yp} D/H={DH} failure={why}", flush=True)


if __name__ == "__main__":
    main()
