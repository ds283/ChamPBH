# (c) University of Sussex 2026
# Created by David Seery
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Planning probe for the science-readiness campaign (README §6, the "now" columns).

Two stages, so that a BBN solve can sit under an external `timeout` without repeating the
integration. Run from the repository root:

    PYTHONPATH=. ./venv/bin/python prompts/science-readiness/planning-probes/bbn_route_probe.py \
        history BETA M OUT.pkl
    timeout 600 env PYTHONPATH=. ./venv/bin/python \
        prompts/science-readiness/planning-probes/bbn_route_probe.py bbn OUT.pkl ROUTE

`history` runs the production `compute_scalar_model._function` with main.py's initial data
(phi* = 5, pi* = 0, T* = 2e4 GeV, T_stop = T_CMB, atol = rtol = 1e-8, 250 samples per decade of
1 + z from log10(1 + z) = 0 to 35, QCD_Cosmology on Planck2018, exponential potential n = 1,
Lambda = 1e-3 eV) and pickles the stored samples. No Ray, no datastore.

`bbn` drives the production `compute_BBN_data._function` on those samples (full network):
  ROUTE = shipped  the callbacks as shipped (rho_NP, p_NP, drho_NP/dT; NP_thermo_flag = True);
  ROUTE = honly    the same rho_NP, with p_NP = -rho_NP and drho_NP/dT = 0, so that only H sees
                   the new physics. This is the Hubble-only route expressed through the existing
                   callbacks, with no PRyMordial patch.
It prints the abundances, the wall time of the PRyMordial call, and the sample-to-sample
behaviour of the stored ratio rho_NP / rho_R,J in four Jordan-temperature windows.
"""

import pickle
import sys
import time
from math import exp, log

import numpy as np

import importlib

SM = importlib.import_module("ComputeTargets.ScalarModel")
BBN = importlib.import_module("ComputeTargets.BBNData")

from CosmologyConcepts import (
    beta_value,
    M_value,
    Lambda_value,
    temperature,
    redshift,
    redshift_array,
)
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.ConformalCouplings.ExponentialCoupling import ExponentialCoupling
from CosmologyConcepts.Potentials.ExponentialPotential import ExponentialPotential
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
from CosmologyModels.LambdaCDM import Planck2018
from Units import Planck_units

units = Planck_units()
params = Planck2018()
cosmology = QCD_Cosmology(0, units, params)


def build(beta: float, M: float):
    potential = ExponentialPotential(
        0, M_value(0, M * units.PlanckMass), Lambda_value(0, 1e-3 * units.eV), 1, units
    )
    coupling = ExponentialCoupling(0, beta_value(0, beta), units)
    return potential, coupling


def z_grid():
    num = int(round(250 * (35 - 0) + 0.5, 0))
    zs = np.logspace(0, 35, num) - 1.0
    return redshift_array(z_array=[redshift(i, float(z)) for i, z in enumerate(zs)])


def run_history(beta: float, M: float, out: str):
    potential, coupling = build(beta, M)
    T_init = temperature(0, 2.0e4 * units.GeV)
    T_stop = temperature(1, params.T_CMB_Kelvin * units.Kelvin)
    t0 = time.time()
    data = SM.compute_scalar_model._function(
        cosmology,
        T_init,
        T_stop,
        phi_value(0, 5.0 * units.PlanckMass),
        pi_value(0, 0.0),
        z_grid(),
        potential,
        coupling,
        task_label=f"probe-beta{beta}-M{M}",
    )
    wall = time.time() - t0
    if data.get("failure", False):
        print(f"history beta={beta} M={M}: FAILED after {wall:.1f} s")
        sys.exit(1)
    zs = [z.z for z in data["z_grid"]]
    samples = [tuple(s) for s in data["sample"]]
    with open(out, "wb") as f:
        pickle.dump({"beta": beta, "M": M, "z": zs, "samples": samples}, f)
    md = data["metadata"]
    print(
        f"history beta={beta} M={M}: RHS={md.RHS_evaluations} accepted_steps={data['accepted_steps']} "
        f"reflections={data['reflections']} samples={len(samples)} wall={wall:.1f} s"
    )


class _StandIn:
    def __init__(self, beta, M, values):
        self.potential, self.coupling = build(beta, M)
        self._cosmology = cosmology
        self.T_Jordan_stop = temperature(1, params.T_CMB_Kelvin * units.Kelvin)
        self.values = values


class _Proxy:
    def __init__(self, model):
        self._model = model

    def get(self):
        return self._model


def _honly(original):
    def build(*args, **kwargs):
        cb = original(*args, **kwargs)

        def P_NP(T):
            return -cb.rho_NP(T)

        def drho_NP_dT(T):
            # keep the finiteness and domain guards of the shipped callback
            cb.rho_NP(T)
            return 0.0

        return BBN.NPCallbacks(rho_NP=cb.rho_NP, P_NP=P_NP, drho_NP_dT=drho_NP_dT)

    return build


def ratio_windows(values):
    keV = units.keV
    out = []
    for lo, hi in ((0.3, 1.0), (1.0, 3.0), (3.0, 10.0), (10.0, 100.0)):
        r = []
        for v in values:
            T = exp(v.log_T_Jordan) / keV
            if lo <= T < hi:
                H2 = v.H_Jordan * v.H_Jordan
                rho_R = exp(v.log_rhorad_Jordan)
                rho_NP = 3.0 * units.PlanckMass**2 * H2 - rho_R * (1.0 + exp(v.log_fm))
                r.append(rho_NP / rho_R)
        r = np.array(r)
        if len(r) < 2:
            out.append(f"[{lo:g},{hi:g}) keV: {len(r)} samples")
            continue
        d = np.diff(r)
        out.append(
            f"[{lo:g},{hi:g}) keV: n={len(r)} min={r.min():.4g} median={np.median(r):.4g} "
            f"max={r.max():.4g} rms_step={np.sqrt(np.mean(d * d)):.4g}"
        )
    return out


def run_bbn(path: str, route: str):
    with open(path, "rb") as f:
        payload = pickle.load(f)
    beta, M = payload["beta"], payload["M"]
    values = [
        SM.ScalarModelValue(None, redshift(i, z), **SM.SampleValues._make(s)._asdict())
        for i, (z, s) in enumerate(zip(payload["z"], payload["samples"]))
    ]
    model = _StandIn(beta, M, values)
    for line in ratio_windows(values):
        print(f"ratio beta={beta} M={M} {line}")

    if route == "honly":
        BBN.build_NP_callbacks = _honly(BBN.build_NP_callbacks)
    elif route != "shipped":
        raise ValueError(route)

    t0 = time.time()
    res = BBN.compute_BBN_data._function(
        _Proxy(model), task_label=f"probe-{route}", small_network=False
    )
    wall = time.time() - t0
    if res.get("failure", False):
        print(
            f"bbn beta={beta} M={M} route={route}: FAILURE wall={wall:.1f} s reason={res['failure_reason']}"
        )
        return
    print(
        f"bbn beta={beta} M={M} route={route}: Yp={res['Yp_BBN']:.10g} DoH={res['DOverH']:.10g} "
        f"He3oH={res['He3OverH']:.10g} Li7oH={res['Li7OverH']:.10g} "
        f"PRyM_time={res['BBN_compute_time']:.1f} s wall={wall:.1f} s"
    )


if __name__ == "__main__":
    if sys.argv[1] == "history":
        run_history(float(sys.argv[2]), float(sys.argv[3]), sys.argv[4])
    elif sys.argv[1] == "bbn":
        run_bbn(sys.argv[2], sys.argv[3])
    else:
        raise ValueError(sys.argv[1])
