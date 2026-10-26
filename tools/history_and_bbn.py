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

By default the history is tuned, as main.py's are: `tune_history` (jordan-normalization
prompt 06) integrates it to T_J = T_CMB with `integrate_history` and adjusts the model's
normalization and phi_ref until H_J(T_CMB) = H0 and phi_ref = phi_min(T_CMB), within
--tuning-tolerance on both residuals and --tuning-max-integrations integrations (main.py's
options and defaults: 1e-5 and 30). Its `-- tune ...` lines, one per integration, are
tune_history's own. With `--fixed-normalization NORM PHI_REF_MP` the history is one
`integrate_history` of the model realized at that normalization and phi_ref, untuned, to T_CMB
or to --T-stop-GeV (README §6.2 P18): a diagnostic. NORM is the model's own normalization, ln V0
with V0 in units of M_P^4, as a stored history holds it; PHI_REF_MP is phi_ref in units of M_P.
--T-stop-GeV is accepted only with --fixed-normalization, since a tuned history ends at T_CMB;
--tuning-tolerance and --tuning-max-integrations have no effect with it.

The setup is main.py's, and the tuned golden history's (ComputeTargets/tests/
golden_tuned_history.py): QCD_Cosmology on Planck2018 in Planck_units; ExponentialModel with
n = 1 and the given M and beta; phi* = 5 M_P (--phi-init-Mp), pi* = 0, T* = 2e4 GeV; atol =
rtol = 1e-8 (integrate_history's defaults, main.py's); and main.py's sampling axis, 250 samples
per decade of 1 + z from log10(1 + z) = 0 to DEFAULT_LOG10_ONE_PLUS_Z_HIGH (300) of
config/argument_parser.py. Tuned at beta = 3, M = 0.01 it prints the tuning record of
golden_history_06_tuned.json, bitwise. The BBN stage is `compute_BBN_data._function` on a
stand-in model holding the history's samples, ending where the history ends (T_CMB when tuned),
through the production route (the Hubble-only PRyMordial route since science-readiness
prompt 01). Nothing is stored.

Run from the repository root, since PRyMordial reads PRyMrates/ and the EOS
reads its tables from the working directory, one invocation at a time:

    ./venv/bin/python tools/history_and_bbn.py BETA M [--phi-init-Mp X] \\
        [--tuning-tolerance TOL] [--tuning-max-integrations K] \\
        [--fixed-normalization NORM PHI_REF_MP [--T-stop-GeV T]] \\
        [--no-small-network] [--wall-clock-limit SECS]

M is in units of the (reduced) Planck mass. Output lines:

    -- tune ... it=k kind=... H/H0-1=... phi_ref/phi_min-1=... norm=... phi_ref=... wall=...   (tuned only)
    history beta=... M=...: RHS=... accepted_steps=... reflections=... samples=... integrations=... wall=... s
    tuning beta=... M=...: normalization=... V_ref=... phi_ref=... ... seed_phi_ref=...       (tuned only)
    bounce beta=... M=...: N=... T_J=... MeV phi=... reflected=...
    fixed_T beta=... M=...: 1MeV crossings=... phi=... ratio=... stand_in_phi=... stand_in_ratio=... | 70keV ...
    ratio beta=... M=... [lo,hi) keV: n=... min=... median=... max=... rms_step=...
    bbn beta=... M=...: Yp=... DoH=... He3oH=... Li7oH=... PRyM_time=... s wall=... s PRyM_version=...

or `... FAILURE ...` with the reason (a tuning that fails prints `history ...: FAILURE` and
exits 1). The history line's RHS evaluations, steps, reflections and samples are the final
integration's; `integrations` is the tuning's count, the seed's included (1 with
--fixed-normalization), and `wall` the whole tuning's. The tuning line holds every key of
ComputeTargets.ScalarHistory.TUNING_OUTPUT_KEYS, in order, as key=value: a float as its repr,
which reads back exactly, an int or a string as it is. Nothing is converted: the run is in
Planck_units, so each value is the float a store would hold (V_ref in M_P^4, phi_ref, phi_0
and seed_phi_ref in M_P, pi_0 in M_P per e-fold). The bounce line is the history's first
bounce, `first_bounce` on the dense output (science-readiness prompt 03), with phi in units of
M_P; it reads `bounce ...: none` if there was none. The fixed_T line (science-readiness prompt
06b) gives, at T_J = 1 MeV and 70 keV, the number of sign changes of ln T_J - ln T* across the
accepted steps' end points, the stored `fixed_T_values` (phi in M_P, rho_NP/rho_R,J, on the
dense output), and the stand-in: the same two values interpolated linearly in ln T_J between
the two stored samples either side of the first bracketing pair. The ratio lines are
rho_NP / rho_R,J, computed from the stored samples as compute_BBN_data computes it, in four
Jordan-temperature windows; rms_step is the root mean square of the sample-to-sample
difference.

Costs, measured by jordan-normalization prompt 08 at a load of about 10: tuned at beta = 3,
M = 0.01, five integrations, 22 s (up to about 28 s under more load); one integration
(--fixed-normalization) to T_CMB about 4.5 s; a PRyMordial solve about 10 s.

Written for science-readiness prompt 01 (README section 2 (m)), from the
planner's `prompts/science-readiness/planning-probes/bbn_route_probe.py`,
running the production route only. Later prompts add their outputs here; jordan-normalization
prompt 08 made the history tuned by default (P18, D4).
"""

import argparse
import importlib
import os
import sys
import time
from math import exp, log
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
from ComputeTargets.ScalarHistory import (  # noqa: E402
    DEFAULT_TUNING_MAX_INTEGRATIONS,
    DEFAULT_TUNING_TOLERANCE,
    FIXED_T_JORDAN_HIGH_MEV,
    FIXED_T_JORDAN_LOW_MEV,
    TUNING_OUTPUT_KEYS,
    SampleValues,
    ScalarHistoryValue,
    integrate_history,
    tune_history,
)
from CosmologyConcepts import (  # noqa: E402
    M_value,
    beta_value,
    redshift,
    redshift_array,
    temperature,
)
from CosmologyConcepts.redshift import log_one_plus_z_grid  # noqa: E402
from CosmologyConcepts.FieldValues import phi_value, pi_value  # noqa: E402
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel  # noqa: E402
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology  # noqa: E402
from CosmologyModels.LambdaCDM import Planck2018  # noqa: E402
from Units import Planck_units  # noqa: E402
from config.argument_parser import (  # noqa: E402
    DEFAULT_LOG10_ONE_PLUS_Z_HIGH,
    positive_float,
    positive_int,
)

# main.py's initial data (main.py, execute()) and z grid: the sampling axis of
# CosmologyConcepts.redshift.log_one_plus_z_grid, at main.py's density, to main.py's default
# DEFAULT_LOG10_ONE_PLUS_Z_HIGH (300) decades of 1 + z. Until jordan-normalization prompt 08 it
# was 35 decades, too short for a history tuned to T_CMB (README §0.3)
PHI_INIT_MP = 5.0  # the default of --phi-init-Mp
PI_INIT = 0.0
T_INIT_GEV = 2.0e4
SAMPLES_PER_LOG10_Z = 250

# the windows of the ratio lines, in keV
RATIO_WINDOWS_KEV = ((0.3, 1.0), (1.0, 3.0), (3.0, 10.0), (10.0, 100.0))


class _StandInModel:
    """The attributes of a ScalarHistory that compute_BBN_data reads."""

    def __init__(self, cosmology, model, T_stop, values):
        self._cosmology = cosmology
        self.model = model
        self.T_Jordan_stop = T_stop
        self.values = values


class _Proxy:
    def __init__(self, model):
        self._model = model

    def get(self):
        return self._model


def _z_grid() -> redshift_array:
    xs = log_one_plus_z_grid(SAMPLES_PER_LOG10_Z, DEFAULT_LOG10_ONE_PLUS_Z_HIGH)
    return redshift_array(z_array=[redshift(i, x) for i, x in enumerate(xs)])


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


def _sample_ratio(v, units) -> float:
    """rho_NP/rho_R,J of one sample, as compute_BBN_data computes it."""
    M_P2 = units.PlanckMass * units.PlanckMass
    rho_R = exp(v.log_rhorad_Jordan)
    rho_NP = 3.0 * M_P2 * v.H_Jordan * v.H_Jordan - rho_R * (1.0 + exp(v.log_fm))
    return rho_NP / rho_R


def _sign_changes(result, log_T: float) -> int:
    """Sign changes of ln T_J - log_T across the accepted steps' end points."""
    sol = result.solution
    g = [sol.interpolants[k](t)[4] - log_T for k, t in enumerate(sol.ts[:-1])]
    g.append(sol.interpolants[-1](sol.ts[-1])[4] - log_T)
    signs = [x < 0.0 for x in g if x != 0.0]
    return sum(1 for a, b in zip(signs, signs[1:]) if a != b)


def _stand_in(values, log_T: float, units):
    """phi (M_P) and the ratio, linear in ln T_J between the first pair of samples bracketing log_T."""
    for a, b in zip(values, values[1:]):
        if (a.log_T_Jordan - log_T) * (b.log_T_Jordan - log_T) <= 0.0:
            if a.log_T_Jordan == b.log_T_Jordan:
                w = 0.0
            else:
                w = (log_T - a.log_T_Jordan) / (b.log_T_Jordan - a.log_T_Jordan)
            phi = (1.0 - w) * a.phi_Einstein + w * b.phi_Einstein
            ratio = (1.0 - w) * _sample_ratio(a, units) + w * _sample_ratio(b, units)
            return phi / units.PlanckMass, ratio
    return None, None


def _fmt(x) -> str:
    return "None" if x is None else f"{x:.9e}"


def _fixed_T_line(fixed_T, result, values, units, label: str) -> str:
    parts = []
    for name, T_MeV, phi, ratio in (
        (
            "1MeV",
            FIXED_T_JORDAN_HIGH_MEV,
            fixed_T.phi_Einstein_1MeV,
            fixed_T.density_NP_ratio_1MeV,
        ),
        (
            "70keV",
            FIXED_T_JORDAN_LOW_MEV,
            fixed_T.phi_Einstein_70keV,
            fixed_T.density_NP_ratio_70keV,
        ),
    ):
        log_T = log(T_MeV * units.MeV)
        s_phi, s_ratio = _stand_in(values, log_T, units)
        parts.append(
            f"{name} crossings={_sign_changes(result, log_T)} "
            f"phi={_fmt(phi / units.PlanckMass if phi is not None else None)} "
            f"ratio={_fmt(ratio)} stand_in_phi={_fmt(s_phi)} stand_in_ratio={_fmt(s_ratio)}"
        )
    return f"fixed_T {label}: " + " | ".join(parts)


def tuning_line(data: dict, label: str) -> str:
    """
    The tuning line: every key of TUNING_OUTPUT_KEYS, in order, as key=value, a float as its repr
    (which reads back exactly) and an int or a string as it is, unconverted.
    """
    parts = []
    for key in TUNING_OUTPUT_KEYS:
        value = data[key]
        parts.append(
            f"{key}={value!r}" if isinstance(value, float) else f"{key}={value}"
        )
    return f"tuning {label}: " + " ".join(parts)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("beta", type=float, help="the coupling's beta")
    parser.add_argument("M", type=float, help="the potential's M, in units of M_P")
    parser.add_argument(
        "--phi-init-Mp",
        type=float,
        default=PHI_INIT_MP,
        help="initial value of the scalar field in units of M_P (default 5, as main.py)",
    )
    parser.add_argument(
        "--tuning-tolerance",
        type=positive_float,
        default=DEFAULT_TUNING_TOLERANCE,
        help="the tuning's tolerance on both residuals, |H_J(T_CMB)/H0 - 1| and "
        "|phi_ref/phi_min - 1| (default %(default)s, as main.py); no effect with "
        "--fixed-normalization",
    )
    parser.add_argument(
        "--tuning-max-integrations",
        type=positive_int,
        default=DEFAULT_TUNING_MAX_INTEGRATIONS,
        help="the cap on the tuning's integrations, the seed's included (default "
        "%(default)s, as main.py); no effect with --fixed-normalization",
    )
    parser.add_argument(
        "--fixed-normalization",
        type=float,
        nargs=2,
        default=None,
        metavar=("NORM", "PHI_REF_MP"),
        help="run one untuned history of the model realized at the normalization NORM (the "
        "model's own: ln V0, V0 in units of M_P^4, as a stored history holds it) and "
        "phi_ref = PHI_REF_MP M_P, by integrate_history, for diagnostics (default: tune the "
        "history, as main.py does)",
    )
    parser.add_argument(
        "--T-stop-GeV",
        type=float,
        default=None,
        help="with --fixed-normalization only: the stopping temperature in the Jordan frame, "
        "in GeV (default: T_CMB). A tuned history ends at T_CMB",
    )
    parser.add_argument(
        "--small-network",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="run PRyMordial's 12-reaction network (default: True, the small network, as main.py runs; --no-small-network for the full network)",
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

    if args.T_stop_GeV is not None and args.fixed_normalization is None:
        print(
            "!! history_and_bbn: --T-stop-GeV is accepted only with --fixed-normalization; a "
            "tuned history ends at T_CMB",
            file=sys.stderr,
        )
        return 2

    if not Path(os.getcwd(), "PRyMrates").is_dir():
        print(
            f"!! history_and_bbn: run from the repository root; PRyMordial reads PRyMrates/ "
            f"from the working directory ({os.getcwd()})"
        )
        return 2

    beta, M = args.beta, args.M
    label = f"beta={beta:g} M={M:g}"
    wall_clock_limit = None if args.wall_clock_limit == 0 else args.wall_clock_limit
    tuned = args.fixed_normalization is None

    units = Planck_units()
    params = Planck2018()
    cosmology = QCD_Cosmology(0, units, params)

    model = ExponentialModel(
        0, M_value(0, M * units.PlanckMass), 1, beta_value(0, beta), units
    )

    T_init = temperature(0, T_INIT_GEV * units.GeV)
    # T_CMB as tune_history takes it (cosmology.T_CMB_Kelvin * units.Kelvin)
    if args.T_stop_GeV is None:
        T_stop = temperature(1, cosmology.T_CMB_Kelvin * units.Kelvin)
    else:
        T_stop = temperature(1, args.T_stop_GeV * units.GeV)
    phi_init = phi_value(0, args.phi_init_Mp * units.PlanckMass)
    pi_init = pi_value(0, PI_INIT)
    task_label = f"history_and_bbn-beta{beta:g}-M{M:g}"

    # stage 1: the history. The final IntegrationResult is kept, by wrapping
    # integrate_scalar_history for the duration of the call, so that the fixed_T line can count
    # sign changes on it; a tuning integrates several times, and only the last is kept
    SM = importlib.import_module("ComputeTargets.ScalarHistory")
    captured = []
    integrate = SM.integrate_scalar_history

    def _capturing_integrate(*a, **kw):
        result = integrate(*a, **kw)
        captured[:] = [result]
        return result

    SM.integrate_scalar_history = _capturing_integrate
    start = time.perf_counter()
    try:
        if tuned:
            data = tune_history(
                cosmology,
                model,
                T_init,
                phi_init,
                pi_init,
                _z_grid(),
                task_label=task_label,
                tolerance=args.tuning_tolerance,
                max_integrations=args.tuning_max_integrations,
            )
        else:
            normalization, phi_ref_Mp = args.fixed_normalization
            data = integrate_history(
                cosmology,
                T_init,
                T_stop,
                phi_init,
                pi_init,
                _z_grid(),
                model.realize(normalization, phi_ref_Mp * units.PlanckMass),
                task_label=task_label,
            )
    finally:
        SM.integrate_scalar_history = integrate
    wall = time.perf_counter() - start

    if data.get("failure", False):
        reason = data.get("failure_reason", "(no reason stored)")
        print(f"history {label}: FAILURE wall={wall:.1f} s reason={reason}")
        return 1

    metadata = data["metadata"]
    integrations = data["tuning_iterations"] if tuned else 1
    print(
        f"history {label}: RHS={metadata.RHS_evaluations} "
        f"accepted_steps={data['accepted_steps']} reflections={data['reflections']} "
        f"samples={len(data['sample'])} integrations={integrations} wall={wall:.1f} s"
    )
    if tuned:
        print(tuning_line(data, label))

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
        ScalarHistoryValue(None, z, **SampleValues._make(tuple(s))._asdict())
        for z, s in zip(data["z_grid"], data["sample"])
    ]
    print(_fixed_T_line(data["fixed_T_values"], captured[-1], values, units, label))

    for line in _ratio_lines(values, units, label):
        print(line)

    # stage 2: BBN, through the production route
    stand_in = _StandInModel(cosmology, model, T_stop, values)
    start = time.perf_counter()
    res = compute_BBN_data._function(
        _Proxy(stand_in),
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
