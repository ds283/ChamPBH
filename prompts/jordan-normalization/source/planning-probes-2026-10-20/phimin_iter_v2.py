"""
Planning probe for jordan-normalization: the brief's fixed point with the ruled
reference phi_ref = phi_min(T_CMB) (U2) instead of phi_0. Read-only; run from the
ChamPBH root:  ./venv/bin/python <this> BETA M_MP PHI_INIT_MP [MAXIT]
"""

import os
import sys
import time
from math import exp, log

sys.path.insert(0, os.getcwd())

import numpy as np
from scipy.optimize import brentq

from ComputeTargets.ScalarModel import SampleValues, compute_scalar_model
from CosmologyConcepts import (
    Lambda_value,
    M_value,
    beta_value,
    redshift,
    redshift_array,
    temperature,
)
from CosmologyConcepts.ConformalCouplings.ExponentialCoupling import ExponentialCoupling
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Potentials.ExponentialPotential import ExponentialPotential
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
from CosmologyModels.LambdaCDM import Planck2018
from Units import Planck_units

T_INIT_GEV = 2.0e4
TOL = 1e-5


class Shifted(ExponentialCoupling):
    def __init__(self, store_id, beta, units, phi_ref):
        super().__init__(store_id, beta, units)
        self._phi_ref = phi_ref

    def log_Omega(self, phi):
        return super().log_Omega(phi) - self._beta_float * self._phi_ref / self._Mp


units = Planck_units()
params = Planck2018()
cosmology = QCD_Cosmology(0, units, params)
Mp = units.PlanckMass
H0 = cosmology.H0


def z_grid():
    num = int(round(250 * 85.0 + 0.5, 0))
    zs = np.logspace(0.0, 85.0, num) - 1.0
    return redshift_array(z_array=[redshift(i, float(z)) for i, z in enumerate(zs)])


ZG = z_grid()


def one(beta, M, phi_init, Lambda, phi_ref):
    pot = ExponentialPotential(0, M_value(0, M * Mp), Lambda_value(0, Lambda), 1, units)
    cpl = Shifted(0, beta_value(0, beta), units, phi_ref)
    t0 = time.perf_counter()
    data = compute_scalar_model._function(
        cosmology,
        temperature(0, T_INIT_GEV * units.GeV),
        temperature(1, params.T_CMB_Kelvin * units.Kelvin),
        phi_value(0, phi_init * Mp),
        pi_value(0, 0.0),
        ZG,
        pot,
        cpl,
        task_label="probe",
    )
    wall = time.perf_counter() - t0
    if data.get("failure", False):
        return None, wall, data.get("failure_reason")
    s = SampleValues._make(tuple(data["sample"][-1]))._asdict()
    phi, pi = s["phi_Einstein"], s["pi_Einstein"]
    A = cpl.Omega(phi)
    rho_RJ = exp(s["log_rhorad_Jordan"])
    fm = exp(s["log_fm"])
    V = exp(pot.log_V(phi))
    bp = 1.0 + beta * pi / Mp
    G = 1.0 - pi * pi / (6.0 * Mp * Mp)
    V_target = A**4 * (3.0 * Mp**2 * H0**2 * G / (A**2 * bp**2) - rho_RJ * (1.0 + fm))
    rho_mE0 = A**4 * fm * rho_RJ

    def dVeff(x):
        return exp(pot.log_V(x)) * pot.d_logV_dphi(x) + (beta / Mp) * rho_mE0 * exp(
            beta * (x - phi) / Mp
        )

    try:
        phi_min = brentq(dVeff, 0.05 * phi, 20.0 * phi, xtol=1e-14 * phi, rtol=1e-13)
    except ValueError:
        phi_min = float("nan")
    return (
        dict(
            phi0=phi,
            pi0=pi,
            A0=A,
            h_ratio=s["H_Jordan"] / H0,
            V_target=V_target,
            V=V,
            phi_min=phi_min,
            track=pi / (1.5 * phi),
            bp=bp,
            refl=data["reflections"],
        ),
        wall,
        None,
    )


def main():
    beta, M, phi_init = map(float, sys.argv[1:4])
    maxit = int(sys.argv[4]) if len(sys.argv) > 4 else 25
    rho_L = 3.0 * Mp**2 * H0**2 * (1.0 - params.omega_m)
    Lambda, phi_ref = rho_L**0.25, 0.0
    tag = f"beta={beta:g} M={M:g} phi*={phi_init:g}"
    total = 0.0
    for it in range(maxit):
        r, wall, why = one(beta, M, phi_init, Lambda, phi_ref)
        total += wall
        if r is None:
            print(f"{tag} it={it} FAILURE {why} wall={wall:.1f}", flush=True)
            return
        dphi = abs(phi_ref / r["phi_min"] - 1.0) if phi_ref else float("inf")
        dh = abs(r["h_ratio"] - 1.0)
        print(
            f"{tag} it={it} Lambda={Lambda/units.eV*1e3:.6f} meV phi_ref={phi_ref/Mp:.6e} | "
            f"phi0={r['phi0']/Mp:.6e} phi_min={r['phi_min']/Mp:.6e} phi_min/phi0={r['phi_min']/r['phi0']:.4f} "
            f"track={r['track']:.4f} 1+bpi={r['bp']:.4f} A0={r['A0']:.6f} | H/H0-1={r['h_ratio']-1:.3e} "
            f"|phi_ref/phi_min-1|={dphi:.2e} Vt/V={r['V_target']/r['V']:.6f} refl={r['refl']} wall={wall:.1f}",
            flush=True,
        )
        if dphi < TOL and dh < TOL:
            print(f"{tag} CONVERGED it={it} total={total:.1f} s", flush=True)
            return
        if r["V_target"] <= 0.0:
            bad = locals().get("bad", 0) + 1
            print(
                f"{tag} V_target<=0 at it={it} (consecutive {bad}); phi_ref updated, Lambda halved",
                flush=True,
            )
            if bad >= 4:
                print(f"{tag} V_target<=0 PERSISTS", flush=True)
                return
            Lambda = 0.5 * Lambda
        else:
            bad = 0
            Lambda = Lambda * (r["V_target"] / r["V"]) ** 0.25
        phi_ref = r["phi_min"]
    print(f"{tag} NOT CONVERGED after {maxit} total={total:.1f} s", flush=True)


if __name__ == "__main__":
    main()
