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
Run one scalar history and its BBN through the production code, with no Ray
cluster and no datastore, and print one line per stage.

The history is `compute_scalar_model._function` with main.py's initial data:
phi* = 5 M_P, pi* = 0, T* = 2e4 GeV, T_stop = T_CMB unless --T-stop-GeV is
given, the default tolerances, and 250 samples per decade of 1 + z from
log10(1 + z) = 0 to 35. The cosmology is QCD_Cosmology on Planck2018, the
potential exponential with n = 1 and Lambda = 1e-3 eV, the coupling
exponential with the given beta. The BBN stage is `compute_BBN_data._function`
on a stand-in model holding the history's samples, through the production
route (the Hubble-only PRyMordial route since science-readiness prompt 01).
Nothing is stored.

Run from the repository root, since PRyMordial reads PRyMrates/ and the EOS
reads its tables from the working directory, one invocation at a time:

    ./venv/bin/python tools/history_and_bbn.py BETA M [--T-stop-GeV T] \\
        [--small-network] [--wall-clock-limit SECS]

M is in units of the (reduced) Planck mass. Output lines:

    history beta=... M=...: RHS=... accepted_steps=... reflections=... samples=... wall=... s
    bounce beta=... M=...: N=... T_J=... MeV phi=... reflected=...
    ratio beta=... M=... [lo,hi) keV: n=... min=... median=... max=... rms_step=...
    bbn beta=... M=...: Yp=... DoH=... He3oH=... Li7oH=... PRyM_time=... s wall=... s PRyM_version=...

or `... FAILURE ...` with the reason. The bounce line is the history's first
bounce, `first_bounce` on the dense output (science-readiness prompt 03), with
phi in units of M_P; it reads `bounce ...: none` if there was none. The ratio
lines are rho_NP / rho_R,J,
computed from the stored samples as compute_BBN_data computes it, in four
Jordan-temperature windows; rms_step is the root mean square of the
sample-to-sample difference.

Written for science-readiness prompt 01 (README section 2 (m)), from the
planner's `prompts/science-readiness/planning-probes/bbn_route_probe.py`,
running the production route only. Later prompts add their outputs here.
"""

import argparse
import os
import sys
import time
from math import exp
from pathlib import Path

import numpy as np

# make the repository root importable when run as `python tools/history_and_bbn.py`
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ComputeTargets.BBNData import (  # noqa: E402
    DEFAULT_BBN_WALL_CLOCK_LIMIT,
    compute_BBN_data,
)
from ComputeTargets.ScalarModel import (  # noqa: E402
    SampleValues,
    ScalarModelValue,
    compute_scalar_model,
)
from CosmologyConcepts import (  # noqa: E402
    Lambda_value,
    M_value,
    beta_value,
    redshift,
    redshift_array,
    temperature,
)
from CosmologyConcepts.ConformalCouplings.ExponentialCoupling import (  # noqa: E402
    ExponentialCoupling,
)
from CosmologyConcepts.FieldValues import phi_value, pi_value  # noqa: E402
from CosmologyConcepts.Potentials.ExponentialPotential import (  # noqa: E402
    ExponentialPotential,
)
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology  # noqa: E402
from CosmologyModels.LambdaCDM import Planck2018  # noqa: E402
from Units import Planck_units  # noqa: E402

# main.py's initial data (main.py, execute()) and z grid (config/argument_parser.py defaults)
PHI_INIT_MP = 5.0
PI_INIT = 0.0
T_INIT_GEV = 2.0e4
LAMBDA_EV = 1.0e-3
SAMPLES_PER_LOG10_Z = 250
LOG10_ONE_PLUS_Z_LOW = 0.0
LOG10_ONE_PLUS_Z_HIGH = 35.0

# the windows of the ratio lines, in keV
RATIO_WINDOWS_KEV = ((0.3, 1.0), (1.0, 3.0), (3.0, 10.0), (10.0, 100.0))


class _StandInModel:
    """The attributes of a ScalarModel that compute_BBN_data reads."""

    def __init__(self, cosmology, potential, coupling, T_stop, values):
        self._cosmology = cosmology
        self.potential = potential
        self.coupling = coupling
        self.T_Jordan_stop = T_stop
        self.values = values


class _Proxy:
    def __init__(self, model):
        self._model = model

    def get(self):
        return self._model


def _z_grid() -> redshift_array:
    num = int(
        round(
            SAMPLES_PER_LOG10_Z * (LOG10_ONE_PLUS_Z_HIGH - LOG10_ONE_PLUS_Z_LOW) + 0.5,
            0,
        )
    )
    zs = np.logspace(LOG10_ONE_PLUS_Z_LOW, LOG10_ONE_PLUS_Z_HIGH, num) - 1.0
    return redshift_array(z_array=[redshift(i, float(z)) for i, z in enumerate(zs)])


def _ratio_lines(values, units, label: str):
    keV = units.keV
    M_P2 = units.PlanckMass * units.PlanckMass
    lines = []
    for lo, hi in RATIO_WINDOWS_KEV:
        r = []
        for v in values:
            T = exp(v.log_T_Jordan) / keV
            if lo <= T < hi:
                rho_R = exp(v.log_rhorad_Jordan)
                rho_NP = 3.0 * M_P2 * v.H_Jordan * v.H_Jordan - rho_R * (
                    1.0 + exp(v.log_fm)
                )
                r.append(rho_NP / rho_R)
        r = np.array(r)
        if len(r) < 2:
            lines.append(f"ratio {label} [{lo:g},{hi:g}) keV: {len(r)} samples")
            continue
        d = np.diff(r)
        lines.append(
            f"ratio {label} [{lo:g},{hi:g}) keV: n={len(r)} min={r.min():.4g} "
            f"median={np.median(r):.4g} max={r.max():.4g} "
            f"rms_step={np.sqrt(np.mean(d * d)):.4g}"
        )
    return lines


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("beta", type=float, help="the coupling's beta")
    parser.add_argument("M", type=float, help="the potential's M, in units of M_P")
    parser.add_argument(
        "--T-stop-GeV",
        type=float,
        default=None,
        help="stopping temperature in the Jordan frame, in GeV (default: T_CMB)",
    )
    parser.add_argument(
        "--small-network",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="run PRyMordial's 12-reaction network (default: the full network, as main.py)",
    )
    parser.add_argument(
        "--wall-clock-limit",
        type=float,
        default=DEFAULT_BBN_WALL_CLOCK_LIMIT,
        metavar="SECS",
        help=f"wall-clock limit on the PRyMordial solve, in seconds (default "
        f"{DEFAULT_BBN_WALL_CLOCK_LIMIT:g}, as main.py; 0 disables it)",
    )
    args = parser.parse_args(argv)

    if not Path(os.getcwd(), "PRyMrates").is_dir():
        print(
            f"!! history_and_bbn: run from the repository root; PRyMordial reads PRyMrates/ "
            f"from the working directory ({os.getcwd()})"
        )
        return 2

    beta, M = args.beta, args.M
    label = f"beta={beta:g} M={M:g}"
    wall_clock_limit = None if args.wall_clock_limit == 0 else args.wall_clock_limit

    units = Planck_units()
    params = Planck2018()
    cosmology = QCD_Cosmology(0, units, params)

    potential = ExponentialPotential(
        0,
        M_value(0, M * units.PlanckMass),
        Lambda_value(0, LAMBDA_EV * units.eV),
        1,
        units,
    )
    coupling = ExponentialCoupling(0, beta_value(0, beta), units)

    T_init = temperature(0, T_INIT_GEV * units.GeV)
    if args.T_stop_GeV is None:
        T_stop = temperature(1, params.T_CMB_Kelvin * units.Kelvin)
    else:
        T_stop = temperature(1, args.T_stop_GeV * units.GeV)

    # stage 1: the history
    start = time.perf_counter()
    data = compute_scalar_model._function(
        cosmology,
        T_init,
        T_stop,
        phi_value(0, PHI_INIT_MP * units.PlanckMass),
        pi_value(0, PI_INIT),
        _z_grid(),
        potential,
        coupling,
        task_label=f"history_and_bbn-beta{beta:g}-M{M:g}",
    )
    wall = time.perf_counter() - start

    if data.get("failure", False):
        reason = data.get("failure_reason", "(no reason stored)")
        print(f"history {label}: FAILURE wall={wall:.1f} s reason={reason}")
        return 1

    metadata = data["metadata"]
    print(
        f"history {label}: RHS={metadata.RHS_evaluations} "
        f"accepted_steps={data['accepted_steps']} reflections={data['reflections']} "
        f"samples={len(data['sample'])} wall={wall:.1f} s"
    )

    bounce = data["first_bounce"]
    if bounce is None:
        print(f"bounce {label}: none")
    else:
        print(
            f"bounce {label}: N={bounce.N:.9f} "
            f"T_J={exp(bounce.log_T_Jordan) / units.MeV:.6f} MeV "
            f"phi={bounce.phi_Einstein / units.PlanckMass:.6e} "
            f"reflected={bounce.reflected}"
        )

    values = [
        ScalarModelValue(None, z, **SampleValues._make(tuple(s))._asdict())
        for z, s in zip(data["z_grid"], data["sample"])
    ]
    for line in _ratio_lines(values, units, label):
        print(line)

    # stage 2: BBN, through the production route
    model = _StandInModel(cosmology, potential, coupling, T_stop, values)
    start = time.perf_counter()
    res = compute_BBN_data._function(
        _Proxy(model),
        task_label=f"history_and_bbn-beta{beta:g}-M{M:g}",
        small_network=args.small_network,
        wall_clock_limit=wall_clock_limit,
    )
    wall = time.perf_counter() - start

    network = "small" if args.small_network else "full"
    if res.get("failure", False):
        print(
            f"bbn {label}: FAILURE network={network} wall={wall:.1f} s "
            f"reason={res['failure_reason']}"
        )
        return 1

    print(
        f"bbn {label}: Yp={res['Yp_BBN']:.10g} DoH={res['DOverH']:.10g} "
        f"He3oH={res['He3OverH']:.10g} Li7oH={res['Li7OverH']:.10g} "
        f"network={network} PRyM_time={res['BBN_compute_time']:.1f} s wall={wall:.1f} s "
        f"PRyM_version={res['PRyM_version']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
