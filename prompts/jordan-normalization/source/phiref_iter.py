"""
Fixed-point test of normalizing A at phi_ref = phi_0 and tuning V0 so that
H_J(T_CMB) = H0. Run from the ChamPBH root with its venv:

    ./venv/bin/python <scratch>/phiref_iter.py run  BETA M LAMBDA_EV PHI_REF_MP TAG
    ./venv/bin/python <scratch>/phiref_iter.py bbn  TAG [PHI_REF_MP_FOR_POSTHOC]

Each run is pickled to <scratch>/runs/TAG.pkl. Nothing in ChamPBH is modified.
Convention: lambda = A_code(phi_ref) = exp(+beta phi_ref / Mp).
"""

import os
import pickle
import sys
import time
from math import exp, log, sqrt
from pathlib import Path

sys.path.insert(0, os.getcwd())

import numpy as np

from ComputeTargets.BBNData import compute_BBN_data
from ComputeTargets.ScalarModel import SampleValues, ScalarModelValue, compute_scalar_model
from CosmologyConcepts import Lambda_value, M_value, beta_value, redshift, redshift_array, temperature
from CosmologyConcepts.ConformalCouplings.ExponentialCoupling import ExponentialCoupling
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Potentials.ExponentialPotential import ExponentialPotential
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
from CosmologyModels.LambdaCDM import Planck2018
from Units import Planck_units

SCRATCH = Path(__file__).resolve().parent
RUNS = SCRATCH / "runs"
RUNS.mkdir(exist_ok=True)

PHI_INIT_MP, T_INIT_GEV = 5.0, 2.0e4


class ShiftedExponentialCoupling(ExponentialCoupling):
    """A(phi) = exp(beta (phi - phi_ref)/Mp); d ln A/d phi unchanged."""

    def __init__(self, store_id, beta, units, phi_ref):
        super().__init__(store_id, beta, units)
        self._phi_ref = phi_ref

    def log_Omega(self, phi):
        return super().log_Omega(phi) - self._beta_float * self._phi_ref / self._Mp


units = Planck_units()
params = Planck2018()
cosmology = QCD_Cosmology(0, units, params)
Mp = units.PlanckMass


def z_grid():
    num = int(round(250 * 35.0 + 0.5, 0))
    zs = np.logspace(0.0, 35.0, num) - 1.0
    return redshift_array(z_array=[redshift(i, float(z)) for i, z in enumerate(zs)])


def build(beta, M, Lambda_eV, phi_ref_Mp):
    potential = ExponentialPotential(
        0, M_value(0, M * Mp), Lambda_value(0, Lambda_eV * units.eV), 1, units
    )
    coupling = ShiftedExponentialCoupling(0, beta_value(0, beta), units, phi_ref_Mp * Mp)
    return potential, coupling


def run(beta, M, Lambda_eV, phi_ref_Mp, tag):
    potential, coupling = build(beta, M, Lambda_eV, phi_ref_Mp)
    T_init = temperature(0, T_INIT_GEV * units.GeV)
    T_stop = temperature(1, params.T_CMB_Kelvin * units.Kelvin)
    t0 = time.perf_counter()
    data = compute_scalar_model._function(
        cosmology, T_init, T_stop,
        phi_value(0, PHI_INIT_MP * Mp), pi_value(0, 0.0),
        z_grid(), potential, coupling, task_label=tag,
    )
    wall = time.perf_counter() - t0
    if data.get("failure", False):
        print(f"{tag}: FAILURE {data.get('failure_reason')}")
        return 1
    samples = [SampleValues._make(tuple(s))._asdict() for s in data["sample"]]
    z = [float(zz.z) for zz in data["z_grid"]]
    rec = dict(beta=beta, M=M, Lambda_eV=Lambda_eV, phi_ref_Mp=phi_ref_Mp,
               samples=samples, z=z, wall=wall, reflections=data["reflections"])
    with open(RUNS / f"{tag}.pkl", "wb") as f:
        pickle.dump(rec, f)
    report(tag, rec)
    return 0


def today(rec):
    """State at T_CMB (the last sample, z = 0) and the V0 update."""
    beta, M = rec["beta"], rec["M"]
    potential, coupling = build(beta, M, rec["Lambda_eV"], rec["phi_ref_Mp"])
    s = rec["samples"][-1]
    phi, pi = s["phi_Einstein"], s["pi_Einstein"]
    A = coupling.Omega(phi)
    H_J = s["H_Jordan"]
    rho_RJ = exp(s["log_rhorad_Jordan"])
    fm = exp(s["log_fm"])
    V = exp(potential.log_V(phi))
    H0 = cosmology.H0
    h = 100.0 * H_J / (100.0 * units.Kilometre / (units.Second * units.Mpc))
    bp = 1.0 + beta * pi / Mp
    G = 1.0 - pi * pi / (6.0 * Mp * Mp)
    # Eq. (7) solved for the V that would give H_J = H0 at this state
    V_target = A**4 * (3.0 * Mp**2 * H0**2 * G / (A**2 * bp**2) - rho_RJ * (1.0 + fm))
    Lambda_new = rec["Lambda_eV"] * (V_target / V) ** 0.25
    rho_mJ = fm * rho_RJ
    Omega_m = rho_mJ * A**2 / (3.0 * Mp**2 * H_J**2)
    Omega_V = (V / A**4) * A**2 / (3.0 * Mp**2 * H_J**2)
    return dict(phi0=phi / Mp, pi0=pi / Mp, A0=A, h=h / 100.0, fm=fm,
                Omega_m=Omega_m, Omega_V=Omega_V, Lambda_new=Lambda_new,
                track=pi / (1.5 * phi), log_M_over_phi=M * Mp / phi)


def report(tag, rec):
    t = today(rec)
    print(
        f"{tag}: Lambda={rec['Lambda_eV']*1e3:.6f} meV phi_ref={rec['phi_ref_Mp']:.6e} Mp | "
        f"phi0={t['phi0']:.6e} Mp phi0'={t['pi0']:.4e} Mp phi0'/(1.5 phi0)={t['track']:.4f} "
        f"M/phi0={t['log_M_over_phi']:.4f} A0={t['A0']:.6f} | h={t['h']:.5f} "
        f"Omega_m={t['Omega_m']:.4f} Omega_V={t['Omega_V']:.4f} f_m={t['fm']:.1f} | "
        f"next Lambda={t['Lambda_new']*1e3:.6f} meV | reflections={rec['reflections']} wall={rec['wall']:.1f} s",
        flush=True,
    )


def ratio(s):
    rho = exp(s["log_rhorad_Jordan"])
    return 3.0 * Mp**2 * s["H_Jordan"] ** 2 / rho - 1.0 - exp(s["log_fm"])


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


def bbn(tag, H_scale=1.0):
    with open(RUNS / f"{tag}.pkl", "rb") as f:
        rec = pickle.load(f)
    potential, coupling = build(rec["beta"], rec["M"], rec["Lambda_eV"], rec["phi_ref_Mp"])
    values = []
    for zz, s in zip(rec["z"], rec["samples"]):
        s2 = dict(s)
        s2["H_Jordan"] = s["H_Jordan"] * H_scale
        values.append(ScalarModelValue(None, redshift(0, zz), **s2))
    res = compute_BBN_data._function(_Proxy(_StandIn(potential, coupling, values)),
                                     task_label=tag, small_network=True, wall_clock_limit=None)
    return res


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "run":
        beta, M, Lam, phiref, tag = sys.argv[2:7]
        sys.exit(run(float(beta), float(M), float(Lam), float(phiref), tag))
    elif cmd == "bbn":
        tag = sys.argv[2]
        phi_ref_posthoc = float(sys.argv[3]) if len(sys.argv) > 3 else 0.0
        with open(RUNS / f"{tag}.pkl", "rb") as f:
            beta = pickle.load(f)["beta"]
        res = bbn(tag, H_scale=exp(-beta * phi_ref_posthoc))
        print(f"bbn {tag} posthoc_phi_ref={phi_ref_posthoc:.6e}: Yp={res.get('Yp_BBN')} "
              f"DoH={res.get('DOverH')} failure={res.get('failure_reason')}", flush=True)
