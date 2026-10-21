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

import csv
from copy import deepcopy
from datetime import datetime
from math import fabs, exp, log, sqrt, isfinite
from pathlib import Path
from typing import Optional, Any, Sequence

import numpy as np
from matplotlib.figure import Figure
from matplotlib.patches import Patch
from numpy import nan
from scipy.optimize import brentq, minimize_scalar

from ComputeTargets import ScalarHistory, ScalarHistoryValue, BBNData
from ComputeTargets.ScalarHistory import REFLECTIONS_KEY
from CosmologyConcepts.Potentials import AbstractPotential
from CosmologyModels import BaseCosmology
from Quadrature.integration_metadata import IntegrationSolver
from Units.base import UnitsLike

REDSHIFT_TEXT_DISPLACEMENT_MULTIPLIER = 0.8
EFOLDS_TEXT_DISPLACEMENT_SHIFT = 0.5

ABOVE_PLOTS_TOP_ROW = 0.96
ABOVE_PLOTS_MIDDLE_ROW = 0.94
ABOVE_PLOTS_BOTTOM_ROW = 0.92

BELOW_PLOTS_TOP_ROW = 0.04
BELOW_PLOTS_MIDDLE_ROW = 0.02
BELOW_PLOTS_BOTTOM_ROW = 0.0

LEFT_COLUMN = 0.1
MIDDLE_COLUMN = 0.5
RIGHT_COLUMN = 0.9


nice_Q_labels = {
    "kp_over_H_1E1": r"$k_p/H = 10^1$",
    "kp_over_H_1E2": r"$k_p/H = 10^2$",
    "kp_over_H_1E3": r"$k_p/H = 10^3$",
    "kp_over_H_1E4": r"$k_p/H = 10^4$",
}


def safe_fabs(x: Optional[float]) -> Optional[float]:
    if x is None:
        return nan

    return fabs(x)


def safe_fabs_positive(x: Optional[float]) -> Optional[float]:
    if x is None:
        return nan

    if x < 0.0:
        return nan

    return fabs(x)


def safe_fabs_negative(x: Optional[float]) -> Optional[float]:
    if x is None:
        return nan

    if x > 0.0:
        return nan

    return fabs(x)


def safe_div(x: Optional[float], y: float) -> Optional[float]:
    if x is None or y is None:
        return nan

    try:
        return x / y
    except ZeroDivisionError:
        pass

    return nan


def add_beta_summary_labels(fig, model_label, potential: AbstractPotential):
    now = datetime.now()

    fig.text(
        LEFT_COLUMN,
        ABOVE_PLOTS_MIDDLE_ROW,
        f"Potential: {potential.name}",
        horizontalalignment="left",
        fontsize="x-small",
        fontweight="semibold",
    )

    fig.text(
        RIGHT_COLUMN,
        ABOVE_PLOTS_MIDDLE_ROW,
        f"Created at: {now.strftime("%a %d %b %Y %H:%M:%S")}",
        horizontalalignment="right",
        fontsize="x-small",
    )
    fig.text(
        RIGHT_COLUMN,
        ABOVE_PLOTS_BOTTOM_ROW,
        f"Cosmology: {model_label}",
        horizontalalignment="right",
        fontsize="x-small",
    )


def add_BBN_info_labels(
    fig, bbn: Optional[BBNData] = None, labels: Optional[dict[str, Any]] = None
):
    if bbn is None and labels is None:
        print("!! add_BBN_info_labels: one of bbn or labels must be provided")
        return

    if bbn is not None and labels is not None:
        print("!! add_BBN_info_labels: only one of bbn or labels must be provided")

    if bbn is not None:
        if bbn.failure or not bbn.available:
            return

        small_network = "True" if bbn.small_network else "False"
        PRyM_version = bbn.PRyM_version
    else:
        small_network = labels["small_network"]
        PRyM_version = labels["PRyM_version"]

    fig.text(
        LEFT_COLUMN,
        BELOW_PLOTS_MIDDLE_ROW,
        f"Small reaction network: {small_network}",
        horizontalalignment="left",
        fontsize="xx-small",
    )
    if small_network == "True" or small_network == "Multiple":
        fig.text(
            LEFT_COLUMN,
            BELOW_PLOTS_BOTTOM_ROW,
            f"Warning: Li$^7$ results may not be reliable",
            horizontalalignment="left",
            fontsize="xx-small",
            fontweight="semibold",
            color="red",
        )

    fig.text(
        RIGHT_COLUMN,
        BELOW_PLOTS_MIDDLE_ROW,
        f"PRyMordial version: {PRyM_version}",
        horizontalalignment="right",
        fontsize="xx-small",
    )


def reflection_count(extra_data: Optional[dict]) -> int:
    """
    Return the number of elastic reflections (the floor-triggered reflection model of the
    scalar-field step loop) recorded in a ScalarHistory's extra_metadata.

    ScalarHistory stores the count under REFLECTIONS_KEY only when it is positive,
    so an absent key (or no extra_data at all) means there were none, and we return 0.
    """
    if extra_data is None:
        return 0
    return extra_data.get(REFLECTIONS_KEY, 0)


def add_ScalarHistory_labels(fig, model: ScalarHistory, model_label):
    solver: IntegrationSolver = model.solver
    now = datetime.now()

    fig.text(
        LEFT_COLUMN,
        ABOVE_PLOTS_TOP_ROW,
        f"Coupling: {model._coupling.name}",
        horizontalalignment="left",
        fontsize="x-small",
        fontweight="semibold",
    )
    fig.text(
        LEFT_COLUMN,
        ABOVE_PLOTS_MIDDLE_ROW,
        f"Potential: {model._potential.name}",
        horizontalalignment="left",
        fontsize="x-small",
        fontweight="semibold",
    )
    fig.text(
        LEFT_COLUMN,
        ABOVE_PLOTS_BOTTOM_ROW,
        f"Solver: {solver.label}",
        horizontalalignment="left",
        fontsize="x-small",
    )

    fig.text(
        RIGHT_COLUMN,
        ABOVE_PLOTS_MIDDLE_ROW,
        f"Created at: {now.strftime("%a %d %b %Y %H:%M:%S")}",
        horizontalalignment="right",
        fontsize="x-small",
    )

    fig.text(
        RIGHT_COLUMN,
        ABOVE_PLOTS_BOTTOM_ROW,
        f"Cosmology: {model_label}",
        horizontalalignment="right",
        fontsize="x-small",
    )

    extra_data = model.extra_metadata
    if extra_data is None:
        return

    fig.text(
        LEFT_COLUMN,
        BELOW_PLOTS_TOP_ROW,
        f"Reflections (elastic model): {reflection_count(extra_data)}",
        horizontalalignment="left",
        fontsize="xx-small",
    )


_T_events = {
    "Electroweak": {
        "T_Jordan": 160,
        "unit": "GeV",
        "direction": -1,
        "label": "electroweak",
        "ypos": 0.08,
        "xpos": 0.15,
        "color": "m",
        "linestyle": (0, (1, 1)),
    },
    "Lambda_QCD": {
        "T_Jordan": 200,
        "unit": "MeV",
        "direction": -1,
        "label": r"$\Lambda_{\text{QCD}}$",
        "ypos": 0.3,
        "xpos": 0.15,
        "color": "m",
        "linestyle": (0, (1, 1)),
    },
    "e+e-": {
        "T_Jordan": 511,
        "unit": "keV",
        "direction": -1,
        "label": r"$e^+e^-$ annihilation",
        "ypos": 0.5,
        "xpos": 0.15,
        "color": "m",
        "linestyle": (0, (1, 1)),
    },
    "BBN start": {
        "T_Jordan": 1,
        "unit": "MeV",
        "label": r"BBN start",
        "ypos": 0.7,
        "xpos": 0.65,
        "color": "tab:orange",
        "linestyle": (0, (3, 1, 1, 1)),
    },
    "BBN end": {
        "T_Jordan": 10,
        "unit": "keV",
        "label": r"BBN end",
        "ypos": 0.9,
        "xpos": 0.65,
        "color": "tab:orange",
        "linestyle": (0, (3, 1, 1, 1)),
    },
}


def _find_T_event_times(model: ScalarHistory):
    units: UnitsLike = model._units

    events = deepcopy(_T_events)
    for event, config in events.items():
        temperature_value = config["T_Jordan"]
        unit_label = config["unit"]
        unit = getattr(units, unit_label)
        temperature = temperature_value * unit
        config["_T_event"] = temperature

    event_list = {}
    last_T_Jordan = None
    for value in model.values:
        T_Jordan = exp(value.log_T_Jordan)

        for event, config in events.items():
            T_event = config["_T_event"]
            direction = config.get("direction", None)

            if last_T_Jordan is not None:
                prev_delta = last_T_Jordan - T_event
                this_delta = T_Jordan - T_event
                if prev_delta * this_delta < 0.0:
                    if (
                        direction is None
                        or (direction > 0 and prev_delta < 0)
                        or (direction < 0 and prev_delta > 0)
                    ):
                        if event not in event_list:
                            event_list[event] = []
                        event_list[event].append({"z": value.z.z, "raw_N": value.raw_N})

        last_T_Jordan = T_Jordan

    for event, config in events.items():
        if event in event_list:
            config["times"] = event_list[event]

    return events


def get_x_coord(value: ScalarHistoryValue, x_coord: str = "redshift") -> float:
    if x_coord == "efolds":
        return value.raw_N

    return 1.0 + value.z.z


def get_xpos_attr(obj, x_coord: str = "redshift") -> float:
    if x_coord == "efolds":
        return obj["raw_N"]

    return obj["z"]


def add_temperature_yaxis_labels(ax, model: ScalarHistory, temp_unit: str = "GeV"):
    units: UnitsLike = model._units
    cosmology: BaseCosmology = model._cosmology

    _temp_unit = getattr(units, temp_unit)
    T_CMB_in_units = cosmology._params.T_CMB_Kelvin * units.Kelvin / _temp_unit

    ytrans = ax.get_yaxis_transform()

    ax.axhline(T_CMB_in_units, color="r", linestyle=(0, (1, 1)))
    ax.text(
        0.15,
        5 * T_CMB_in_units,
        rf"$T_{{\text{{CMB}}}}$ = {T_CMB_in_units:.3g} {temp_unit}",
        color="r",
        transform=ytrans,
        fontsize="x-small",
    )

    single_lines = ["Electroweak", "Lambda_QCD", "e+e-", "BBN start", "BBN end"]
    for event in single_lines:
        if event in _T_events:
            config = _T_events[event]
            T_Jordan = config["T_Jordan"]
            unit_label = config["unit"]
            unit = getattr(units, unit_label)
            T_Jordan_in_units = T_Jordan * unit / _temp_unit

            ax.axhline(
                T_Jordan_in_units, color=config["color"], linestyle=config["linestyle"]
            )
            ax.text(
                config["xpos"],
                5 * T_Jordan_in_units,
                f"{config['label']}@{config['T_Jordan']:.3g}{config['unit']}",
                color=config["color"],
                transform=ytrans,
                fontsize="x-small",
            )


def add_redshift_xaxis_labels(
    ax,
    model: ScalarHistory,
    temp_unit: str = "GeV",
    text_labels: bool = True,
    x_coord: str = "redshift",
):
    events = _find_T_event_times(model)

    xtrans = ax.get_xaxis_transform()

    if x_coord == "efolds":
        xlabel = "N"
    else:
        xlabel = "z"

    single_lines = ["Electroweak", "Lambda_QCD", "e+e-", "BBN start", "BBN end"]
    for event in single_lines:
        if event in events:
            config = events[event]
            if "times" in config:
                event_times = config["times"]
                for time in event_times:
                    xpos = get_xpos_attr(time, x_coord)
                    ax.axvline(
                        xpos, color=config["color"], linestyle=config["linestyle"]
                    )
                    if text_labels:
                        if "x_coord" == "efolds":
                            ax.text(
                                EFOLDS_TEXT_DISPLACEMENT_SHIFT + xpos,
                                config["ypos"],
                                f"{config['label']}@{config["T_Jordan"]:.3g}{config["unit"]} ${xlabel}$={xpos:.3g}",
                                color=config["color"],
                                transform=xtrans,
                                fontsize="x-small",
                            )

                        else:
                            ax.text(
                                REDSHIFT_TEXT_DISPLACEMENT_MULTIPLIER * xpos,
                                config["ypos"],
                                f"{config['label']}@{config["T_Jordan"]:.3g}{config["unit"]} ${xlabel}$={xpos:.3g}",
                                color=config["color"],
                                transform=xtrans,
                                fontsize="x-small",
                            )

    legend_entries = set()

    band_pairs = [("BBN start", "BBN end")]
    for pair in band_pairs:
        if pair[0] in events and pair[1] in events:
            config0 = events[pair[0]]
            config1 = events[pair[1]]

            if "times" in config0 and "times" in config1:
                event_times0 = config0["times"]
                event_times1 = config1["times"]

                if len(event_times0) == len(event_times1):
                    times = zip(event_times0, event_times1)

                    for time0, time1 in times:
                        xpos0 = get_xpos_attr(time0, x_coord)
                        xpos1 = get_xpos_attr(time1, x_coord)
                        ax.axvspan(xpos0, xpos1, color="g", alpha=0.15)

                        legend_entries.add(pair)

                else:
                    print(f"-- could not match events for band {pair[0]} and {pair[1]}")

    h = []
    l = []

    for pair in legend_entries:
        if pair == ("BBN start", "BBN end"):
            h.append(Patch(facecolor=("g", 0.15), edgecolor="g"))
            l.append("BBN region")

    return h, l


# ---------------------------------------------------------------------------------------
# Extraction and the science figures (science-readiness prompt 07, README section 2 (k))
#
# Everything below takes plain floats, sequences, or stored value objects read through
# attribute names. Nothing here holds a datastore handle, so all of it is testable on
# synthetic input (ComputeTargets/tests/test_extraction.py).
# ---------------------------------------------------------------------------------------

DEFAULT_BAND_HALF_WIDTH = 0.025

# the two temperatures of figure 4 and the CSV, in MeV, by the tag in the record keys. The
# values themselves are stored on the ScalarHistory row (ScalarHistory.fixed_T_values; prompt 06b,
# whose FIXED_T_JORDAN_HIGH_MEV and FIXED_T_JORDAN_LOW_MEV these tags name); nothing here
# interpolates samples.
FIXED_T_MEV = {"1MeV": 1.0, "70keV": 0.07}

# One row of histories.csv per history. Units: M in M_P, Lambda in eV, phi in M_P, T in GeV;
# the shifts are fractions ((value - SM baseline)/SM baseline), D/H is the stored 10^5 D/H.
CSV_COLUMNS = [
    "beta",
    "M_Mp",
    "Lambda_eV",
    "phi_init_Mp",
    "Yp_BBN",
    "D_over_H",
    "He3_over_H",
    "Li7_over_H",
    "delta_Yp",
    "delta_D_over_H",
    "T_deliver_GeV",
    "phi_1MeV_Mp",
    "phi_70keV_Mp",
    "rho_ratio_1MeV",
    "rho_ratio_70keV",
    "reflections",
    "failure_reasons",
]

ADIABATIC_Q_ALIASING_ISSUE = "post-adiabatic-Q-reads-aliased-late-samples"
ADIABATIC_Q_ALIASING_M_MAX_MP = 1.0e-3


def relative_shift(value: Optional[float], baseline: Optional[float]) -> float:
    """
    Fractional shift (value - baseline)/baseline. NaN if either is missing or non-finite,
    or if the baseline is zero.
    """
    if value is None or baseline is None:
        return nan
    if not (isfinite(value) and isfinite(baseline)) or baseline == 0.0:
        return nan
    return (value - baseline) / baseline


def running_band(x: Sequence[float], y: Sequence[float], half_width: float):
    """
    The median and the 16th and 84th percentiles of y over the window [x_i - h, x_i + h],
    at every x_i. The window edges are inclusive. Non-finite y are ignored; a window with no
    finite y gives NaN for all three. Returns three arrays, aligned with x.
    """
    xa = np.asarray(x, dtype=float)
    ya = np.asarray(y, dtype=float)
    median = np.full(xa.shape, np.nan)
    lower = np.full(xa.shape, np.nan)
    upper = np.full(xa.shape, np.nan)

    good = np.isfinite(ya) & np.isfinite(xa)
    for i, xi in enumerate(xa):
        if not np.isfinite(xi):
            continue
        window = good & (xa >= xi - half_width) & (xa <= xi + half_width)
        if not window.any():
            continue
        lower[i], median[i], upper[i] = np.percentile(ya[window], [16.0, 50.0, 84.0])

    return median, lower, upper


def kick_threshold_curve(cosmology, T_grid: Sequence[float]):
    """
    beta_th(T) = 1/sqrt(3 Sigma_eff(T)) = sqrt((2 + Sigma)/(6 Sigma)), with
    Sigma_eff = Sigma/(1 + Sigma/2) and Sigma = 1 - 3 w(T), at each T of T_grid (dimensionful,
    in the cosmology's units). This is the paper's reachability condition,
    Sigma/(1 + Sigma/2) = 1/(3 beta^2) (Paper1.tex, eq:surfing-equation), solved for beta.
    A T where Sigma <= 0 is omitted. Returns (T, beta_th) as two lists.

    w is the model's own: for QCD_Cosmology it is the Xav_EOS_data.csv spline, the same w the
    integration reads in ScalarHistory.py's RHS, so Sigma here is the integration's Sigma.
    """
    T_out = []
    beta_out = []
    for T in T_grid:
        Sigma = 1.0 - 3.0 * cosmology.w(T)
        if Sigma > 0.0:
            T_out.append(T)
            beta_out.append(sqrt((2.0 + Sigma) / (6.0 * Sigma)))
    return T_out, beta_out


# Surfing-temperature prompt 01: the kicks of Sigma and the roots of the reachability condition.
KICK_SIGMA_FLOOR = (
    1e-4  # a local maximum of Sigma below this is a spline ripple, not a kick
)
KICK_LABELS_PRODUCTION = (
    "EW",
    "QCD",
    "mu-pi",
    "ee",
)  # hot to cold, on the production EOS
REACHABILITY_N_GRID = 20000  # log-spaced points, uniform in ln T
_EXTREMUM_XATOL = 1e-12  # in ln T
_ROOT_XTOL = 1e-13  # in ln T, so 1e-13 relative in T
_SLOPE_STEP = 1e-4  # in ln T, for the central-difference slope


def _Sigma(cosmology, T: float) -> float:
    return 1.0 - 3.0 * float(cosmology.w(T))


def _Sigma_eff(Sigma: float) -> float:
    return Sigma / (1.0 + 0.5 * Sigma)


def _refine_extremum(cosmology, x_lo: float, x_hi: float, maximum: bool) -> float:
    """
    The ln T in [x_lo, x_hi] at which Sigma is extremal: a bounded scalar minimisation of
    -Sigma (of Sigma, for a minimum) in ln T, then, where the cosmology has dw_dlogT, a polish
    by brentq on d Sigma/d ln T = -3 dw/d ln T = 0 within 1e-4 of the minimiser. (The
    minimiser alone locates a flat extremum only to about 1e-8 in ln T.)
    """
    sign = -1.0 if maximum else 1.0
    result = minimize_scalar(
        lambda x: sign * _Sigma(cosmology, exp(x)),
        bounds=(x_lo, x_hi),
        method="bounded",
        options={"xatol": _EXTREMUM_XATOL},
    )
    x = float(result.x)
    if hasattr(cosmology, "dw_dlogT"):
        a = max(x_lo, x - 1e-4)
        b = min(x_hi, x + 1e-4)
        ga = float(cosmology.dw_dlogT(exp(a)))
        gb = float(cosmology.dw_dlogT(exp(b)))
        if ga * gb < 0.0:
            x = float(
                brentq(
                    lambda y: float(cosmology.dw_dlogT(exp(y))),
                    a,
                    b,
                    xtol=_ROOT_XTOL,
                )
            )
    return x


def _local_maxima(cosmology, T_lo: float, T_hi: float, n_grid: int):
    """
    Every local maximum of Sigma on a log-spaced grid over [T_lo, T_hi], refined, with no floor
    applied. Returns (found, x, S): found is a list of (T, Sigma, grid index), hot to cold, and
    (x, S) is the grid in ln T and Sigma on it.
    """
    x = np.linspace(log(T_lo), log(T_hi), n_grid)
    S = np.array([_Sigma(cosmology, exp(xi)) for xi in x])
    found = []
    for i in range(1, n_grid - 1):
        if S[i] > S[i - 1] and S[i] >= S[i + 1]:
            xr = _refine_extremum(cosmology, x[i - 1], x[i + 1], True)
            found.append((exp(xr), _Sigma(cosmology, exp(xr)), i))
    found.reverse()
    return found, x, S


def find_kicks(cosmology, T_lo: float, T_hi: float, n_grid: int = REACHABILITY_N_GRID):
    """
    The kicks of Sigma(T) = 1 - 3 w(T) over [T_lo, T_hi] (dimensionful, in the cosmology's
    units; the caller passes the table's ends, which for the production EOS are 1e-5 GeV and
    25118.86 GeV), ordered hot to cold.

    Sigma is evaluated on n_grid points uniform in ln T. Each local maximum above
    KICK_SIGMA_FLOOR is refined (see _refine_extremum) and is a kick. A kick's window is bounded
    by the refined minimum of Sigma between it and the next kick; the outermost edges are T_hi
    and T_lo.

    The labels are "EW", "QCD", "mu-pi", "ee" (hot to cold) if there are exactly four kicks, and
    "kick0", "kick1", ... otherwise.

    Each entry is a dict with the keys label, T_peak, Sigma_peak, T_window_hot, T_window_cold,
    Sigma_at_hot_edge, Sigma_at_cold_edge.
    """
    maxima, x, S = _local_maxima(cosmology, T_lo, T_hi, n_grid)
    peaks = [m for m in maxima if m[1] > KICK_SIGMA_FLOOR]
    n = len(peaks)
    if n == len(KICK_LABELS_PRODUCTION):
        labels = list(KICK_LABELS_PRODUCTION)
    else:
        labels = [f"kick{k}" for k in range(n)]

    # window edges, hot to cold: T_hi, the n - 1 troughs, T_lo. The peaks are ordered hot to
    # cold, so their grid indices decrease.
    edges = [(T_hi, _Sigma(cosmology, T_hi))]
    for k in range(n - 1):
        i_hot, i_cold = peaks[k][2], peaks[k + 1][2]
        m = i_cold + int(np.argmin(S[i_cold : i_hot + 1]))
        m = min(max(m, i_cold + 1), i_hot - 1)
        xr = _refine_extremum(cosmology, x[m - 1], x[m + 1], False)
        edges.append((exp(xr), _Sigma(cosmology, exp(xr))))
    edges.append((T_lo, _Sigma(cosmology, T_lo)))

    return [
        {
            "label": labels[k],
            "T_peak": peaks[k][0],
            "Sigma_peak": peaks[k][1],
            "T_window_hot": edges[k][0],
            "T_window_cold": edges[k + 1][0],
            "Sigma_at_hot_edge": edges[k][1],
            "Sigma_at_cold_edge": edges[k + 1][1],
        }
        for k in range(n)
    ]


def _dlnSigma_dlnT(cosmology, T: float) -> float:
    """
    d ln Sigma/d ln T at T: -3 (dw/d ln T)/Sigma from cosmology.dw_dlogT where the cosmology has
    it, otherwise a central difference of ln Sigma in ln T (step 1e-4).
    """
    if hasattr(cosmology, "dw_dlogT"):
        return -3.0 * float(cosmology.dw_dlogT(T)) / _Sigma(cosmology, T)
    h = _SLOPE_STEP
    up = _Sigma(cosmology, T * exp(h))
    down = _Sigma(cosmology, T * exp(-h))
    return (log(up) - log(down)) / (2.0 * h)


def reachability_roots(
    cosmology, beta: float, kicks, n_grid: int = REACHABILITY_N_GRID
):
    """
    The hot and cold roots of the reachability condition Sigma_eff(T) = c, c = 1/(3 beta^2),
    Sigma_eff = Sigma/(1 + Sigma/2), for each kick of `kicks` (find_kicks's list, computed once
    per cosmology). Returns one dict per kick, in the order of `kicks`, with the keys

        kick, beta, T_hot, T_cold, slope_hot, slope_cold, merged, multiple_crossings.

    The sign changes of Sigma_eff - c are found on n_grid points uniform in ln T over
    [T_window_cold of the coldest kick, T_window_hot of the hottest], with each kick's T_peak
    added to the grid, and refined by brentq in ln T (xtol 1e-13). As T decreases, a crossing
    where Sigma_eff rises through c is a hot root (an attractor: d ln Sigma/d ln T < 0) and one
    where it falls through c is a cold root. Consecutive hot and cold roots form a reachable
    interval [T_cold, T_hot]; it belongs to the kicks whose T_peak it contains. Sigma_eff must
    be below c at both ends of the range (ValueError otherwise).

    A kick whose interval does not exist has T_hot = T_cold = None and both slopes None. If
    an interval holds more than one T_peak (it spans a trough), every kick in it carries the
    interval's own T_hot and T_cold and slopes, and `merged` is the label of the other kick (a
    tuple of labels if there are several). No root is placed at a window edge. Otherwise
    `merged` is None. If roots of other intervals lie in a kick's window, `multiple_crossings`
    is True and the kick's T_hot and T_cold are the outermost of its own interval's and those.

    The slopes are d ln Sigma/d ln T at the roots, from cosmology.dw_dlogT where it exists
    (-3 (dw/d ln T)/Sigma), otherwise by central difference in ln T.
    """
    c = 1.0 / (3.0 * beta * beta)
    if len(kicks) == 0:
        return []
    T_hi = kicks[0]["T_window_hot"]
    T_lo = kicks[-1]["T_window_cold"]

    x = np.unique(
        np.concatenate(
            [
                np.linspace(log(T_lo), log(T_hi), n_grid),
                [log(k["T_peak"]) for k in kicks],
            ]
        )
    )

    def f(xi: float) -> float:
        return _Sigma_eff(_Sigma(cosmology, exp(xi))) - c

    F = np.array([f(xi) for xi in x])
    if F[0] > 0.0 or F[-1] > 0.0:
        raise ValueError(
            "reachability_roots: Sigma_eff exceeds 1/(3 beta^2) at an end of the range"
        )
    positive = F > 0.0

    # the sign changes, hot to cold. Sigma_eff - c is negative at both ends, so they alternate
    # hot (Sigma_eff rises through c as T falls), cold, hot, cold, ... and consecutive pairs
    # (hot, cold) are the reachable intervals.
    roots = []
    for i in range(len(x) - 1):
        if positive[i] != positive[i + 1]:
            roots.append(exp(float(brentq(f, x[i], x[i + 1], xtol=_ROOT_XTOL))))
    roots.sort(reverse=True)
    intervals = [(roots[j], roots[j + 1]) for j in range(0, len(roots) - 1, 2)]

    records = []
    for kick in kicks:
        own = [iv for iv in intervals if iv[1] <= kick["T_peak"] <= iv[0]]
        record = {
            "kick": kick["label"],
            "beta": beta,
            "T_hot": None,
            "T_cold": None,
            "slope_hot": None,
            "slope_cold": None,
            "merged": None,
            "multiple_crossings": False,
        }
        if own:
            T_hot, T_cold = own[0]
            others = [
                k["label"]
                for k in kicks
                if k is not kick and T_cold <= k["T_peak"] <= T_hot
            ]
            if others:
                record["merged"] = others[0] if len(others) == 1 else tuple(others)
            in_window = [
                iv
                for iv in intervals
                if iv is not own[0]
                and (
                    kick["T_window_cold"] <= iv[0] <= kick["T_window_hot"]
                    or kick["T_window_cold"] <= iv[1] <= kick["T_window_hot"]
                )
            ]
            if in_window:
                record["multiple_crossings"] = True
                T_hot = max([T_hot] + [iv[0] for iv in in_window])
                T_cold = min([T_cold] + [iv[1] for iv in in_window])
            record["T_hot"] = T_hot
            record["T_cold"] = T_cold
            record["slope_hot"] = _dlnSigma_dlnT(cosmology, T_hot)
            record["slope_cold"] = _dlnSigma_dlnT(cosmology, T_cold)
        records.append(record)
    return records


def adiabatic_Q_caption(M_over_Mp: float) -> Optional[str]:
    """
    The caveat appended to the max |Q| caption for M <~ 1e-3 M_P, else None.
    """
    if M_over_Mp <= ADIABATIC_Q_ALIASING_M_MAX_MP * (1.0 + 1e-9):
        return f"may be set by aliased late samples; see [{ADIABATIC_Q_ALIASING_ISSUE}]"
    return None


def _positive_or_nan(x: Optional[float]) -> float:
    # PRyMordial output that is not positive is a failure that plot_by_beta already drops
    if x is None or not isfinite(x) or not x > 0.0:
        return nan
    return float(x)


def build_history_record(
    *,
    beta: float,
    M_Mp: float,
    Lambda_eV: float,
    phi_init_Mp: float,
    scalar,
    bbn,
    baseline: Optional[dict],
    units: UnitsLike,
    failure_reasons: Sequence[str] = (),
) -> dict:
    """
    One plain record for one history. `scalar` is a successful ScalarHistory and `bbn` a
    successful BBNData (read through attribute names), or None where that stage has no
    successful row; `baseline` is the Standard-Model dictionary of compute_SM_baseline, or
    None (then the shifts are NaN). Everything is read from the parent rows' stored columns
    (abundances, first_bounce, extra_metadata, fixed_T_values), so both objects may have been
    built with _do_not_populate and no sample is loaded. The record carries one extra key,
    first_bounce_reflected, which figure 3 reads and the CSV does not write.
    """
    record = {name: nan for name in CSV_COLUMNS}
    record.update(
        beta=beta,
        M_Mp=M_Mp,
        Lambda_eV=Lambda_eV,
        phi_init_Mp=phi_init_Mp,
        reflections=nan,
        failure_reasons="; ".join(failure_reasons),
        first_bounce_reflected=None,
    )

    if bbn is not None:
        record["Yp_BBN"] = _positive_or_nan(bbn.Yp_BBN)
        record["D_over_H"] = _positive_or_nan(bbn.DOverH)
        record["He3_over_H"] = _positive_or_nan(bbn.He3OverH)
        record["Li7_over_H"] = _positive_or_nan(bbn.Li7OverH)
        if baseline is not None:
            record["delta_Yp"] = relative_shift(record["Yp_BBN"], baseline["Yp_BBN"])
            record["delta_D_over_H"] = relative_shift(
                record["D_over_H"], baseline["DOverH"]
            )

    if scalar is not None:
        record["reflections"] = reflection_count(scalar.extra_metadata)
        bounce = scalar.first_bounce
        if bounce is not None:
            record["T_deliver_GeV"] = exp(bounce.log_T_Jordan) / units.GeV
            record["first_bounce_reflected"] = bool(bounce.reflected)
        fixed = scalar.fixed_T_values
        for tag in FIXED_T_MEV:
            phi = getattr(fixed, f"phi_Einstein_{tag}")
            ratio = getattr(fixed, f"density_NP_ratio_{tag}")
            record[f"phi_{tag}_Mp"] = nan if phi is None else phi / units.PlanckMass
            record[f"rho_ratio_{tag}"] = nan if ratio is None else ratio

    return record


def _finite(records: Sequence[dict], key: str):
    xs = np.array([r["beta"] for r in records], dtype=float)
    ys = np.array([r[key] for r in records], dtype=float)
    good = np.isfinite(xs) & np.isfinite(ys)
    order = np.argsort(xs[good])
    return xs[good][order], ys[good][order]


def _save(fig: Figure, path: Path) -> bool:
    path = Path(path)
    path.parent.mkdir(exist_ok=True, parents=True)
    try:
        fig.savefig(path)
        fig.savefig(path.with_suffix(".png"))
    except OverflowError:
        print(f"!! extract_common: could not write {path}")
        return False
    return True


SHIFT_PANELS = [
    ("delta_D_over_H", r"$\Delta(\mathrm{D}/\mathrm{H})$ [%]"),
    ("delta_Yp", r"$\Delta Y_p$ [%]"),
]


def plot_abundance_shifts(
    records: Sequence[dict],
    path: Path,
    half_width: float = DEFAULT_BAND_HALF_WIDTH,
    title: str = "",
) -> bool:
    """
    Figure 1. Shifts of D/H and Yp, in per cent, against beta relative to the same-path SM
    baseline: points, the running median and its 16-84 per cent band. Returns False, writing
    nothing, if there is no finite shift.
    """
    if not any(np.isfinite(r[key]) for r in records for key, _ in SHIFT_PANELS):
        print("!! plot_abundance_shifts: no finite shifts to plot; figure 1 skipped")
        return False

    fig = Figure(figsize=(8.0, 8.0))
    axs = fig.subplots(nrows=2, ncols=1, sharex=True)
    for ax, (key, label) in zip(axs, SHIFT_PANELS):
        x, y = _finite(records, key)
        y = 100.0 * y
        ax.plot(x, y, linestyle="none", marker=".", color="tab:blue", alpha=0.5)
        median, lower, upper = running_band(x, y, half_width)
        ax.plot(x, median, color="k", label="running median")
        ax.fill_between(
            x, lower, upper, color="tab:blue", alpha=0.25, label="16-84% band"
        )
        ax.axhline(0.0, color="gray", linestyle="dotted")
        ax.set_ylabel(label)
        ax.grid(True)
    axs[1].set_xlabel(r"coupling $\beta$")
    axs[0].legend(loc="best")
    fig.suptitle(
        f"{title}\nband half-width {half_width:g} in $\\beta$", fontsize="small"
    )
    return _save(fig, path)


def plot_convergence_in_M(
    records: Sequence[dict],
    path: Path,
    half_width: float = DEFAULT_BAND_HALF_WIDTH,
    title: str = "",
) -> bool:
    """
    Figure 2. The running medians of figure 1, one line per (M, Lambda) in the records,
    D/H above and Yp below: the approach to the M-independent limit.
    """
    groups: dict = {}
    for r in records:
        groups.setdefault((r["M_Mp"], r["Lambda_eV"]), []).append(r)

    if not any(np.isfinite(r[key]) for r in records for key, _ in SHIFT_PANELS):
        print("!! plot_convergence_in_M: no finite shifts to plot; figure 2 skipped")
        return False

    fig = Figure(figsize=(8.0, 8.0))
    axs = fig.subplots(nrows=2, ncols=1, sharex=True)
    for (M, Lam), group in sorted(groups.items()):
        for ax, (key, _) in zip(axs, SHIFT_PANELS):
            x, y = _finite(group, key)
            if len(x) == 0:
                continue
            median, _, _ = running_band(x, 100.0 * y, half_width)
            ax.plot(x, median, label=rf"$M = {M:.3g}\,M_P$, $\Lambda = {Lam:.3g}$ eV")
    for ax, (_, label) in zip(axs, SHIFT_PANELS):
        ax.axhline(0.0, color="gray", linestyle="dotted")
        ax.set_ylabel(f"median {label}")
        ax.grid(True)
    axs[1].set_xlabel(r"coupling $\beta$")
    axs[0].legend(loc="best", fontsize="x-small")
    fig.suptitle(
        f"{title}\nband half-width {half_width:g} in $\\beta$", fontsize="small"
    )
    return _save(fig, path)


def plot_T_deliver(
    records: Sequence[dict],
    path: Path,
    kick_curve: Optional[tuple] = None,
    title: str = "",
) -> bool:
    """
    Figure 3. T_deliver, the temperature of the first bounce, in GeV against beta, with
    reflected bounces marked, and the kick threshold beta_th(T) overlaid. `kick_curve` is
    (T in GeV, beta_th).
    """
    pts = [
        r for r in records if np.isfinite(r["beta"]) and np.isfinite(r["T_deliver_GeV"])
    ]
    if len(pts) == 0:
        print("!! plot_T_deliver: no first bounce to plot; figure 3 skipped")
        return False

    fig = Figure(figsize=(8.0, 6.0))
    ax = fig.subplots()
    for reflected, marker, colour, label in (
        (False, "o", "tab:blue", "first bounce (root of $\\pi$)"),
        (True, "x", "tab:red", "first bounce, elastic reflection"),
    ):
        sel = [r for r in pts if bool(r["first_bounce_reflected"]) == reflected]
        if sel:
            ax.plot(
                [r["beta"] for r in sel],
                [r["T_deliver_GeV"] for r in sel],
                linestyle="none",
                marker=marker,
                color=colour,
                label=label,
            )
    if kick_curve is not None and len(kick_curve[0]) > 0:
        ax.plot(
            kick_curve[1],
            kick_curve[0],
            color="k",
            linestyle="dashed",
            label=r"$\beta_{\mathrm{th}}(T) = 1/\sqrt{3\Sigma_{\mathrm{eff}}(T)}$",
        )
        betas = [r["beta"] for r in pts]
        pad = 0.1 * (max(betas) - min(betas) + 1e-3)
        ax.set_xlim(min(betas) - pad, max(betas) + pad)
    ax.set_yscale("log")
    ax.set_xlabel(r"coupling $\beta$")
    ax.set_ylabel(r"$T_{\mathrm{deliver}}$ [GeV]")
    ax.grid(True)
    ax.legend(loc="best", fontsize="small")
    fig.suptitle(title, fontsize="small")
    return _save(fig, path)


def _fixed_T_figure(records: Sequence[dict], title: str = "") -> Optional[Figure]:
    """
    Figure 4, built and not saved: None if no record has a fixed-T value. Both panels are
    signed. rho_NP/rho_R,J is negative at 1 MeV on the roster histories (log 06b's driver
    output), so an axis with a non-positive value is symmetric-log, with a linear region
    below 1 per cent of the largest value, and otherwise logarithmic. No point is dropped.
    """
    panels = [
        ("phi", r"$\phi$ [$M_P$]", "phi_{}_Mp"),
        ("ratio", r"$\rho_{\mathrm{NP}}/\rho_{R,J}$", "rho_ratio_{}"),
    ]
    if not any(
        np.isfinite(r[template.format(tag)])
        for r in records
        for _, _, template in panels
        for tag in FIXED_T_MEV
    ):
        return None

    fig = Figure(figsize=(8.0, 8.0))
    axs = fig.subplots(nrows=2, ncols=1, sharex=True)
    for ax, (_, ylabel, template) in zip(axs, panels):
        largest = 0.0
        smallest = np.inf
        for tag, colour in zip(FIXED_T_MEV, ("tab:blue", "tab:orange")):
            x, y = _finite(records, template.format(tag))
            ax.plot(x, y, marker=".", color=colour, label=f"$T_J$ = {tag}")
            if len(y) > 0:
                largest = max(largest, float(np.max(np.abs(y))))
                smallest = min(smallest, float(np.min(y)))
        if smallest > 0.0:
            ax.set_yscale("log")
        else:
            ax.axhline(0.0, color="gray", linestyle="dotted")
            ax.set_yscale("symlog", linthresh=max(1.0e-2 * largest, 1.0e-300))
        ax.set_ylabel(ylabel)
        ax.grid(True)
    axs[1].set_xlabel(r"coupling $\beta$")
    axs[0].legend(loc="best", fontsize="small")
    fig.suptitle(title, fontsize="small")
    return fig


def plot_fixed_T(records: Sequence[dict], path: Path, title: str = "") -> bool:
    """
    Figure 4. The values of the field phi (in M_P) and of rho_NP/rho_R,J at T_J = 1 MeV and
    at 70 keV, against beta; the values stored on the ScalarHistory rows. Returns False,
    writing nothing, if there is no value.
    """
    fig = _fixed_T_figure(records, title)
    if fig is None:
        print("!! plot_fixed_T: no fixed-T values to plot; figure 4 skipped")
        return False
    return _save(fig, path)


def write_histories_csv(records: Sequence[dict], path: Path) -> None:
    """
    histories.csv: one row per record, columns CSV_COLUMNS, sorted by (M, Lambda, beta).
    Missing values are written empty.
    """
    path = Path(path)
    path.parent.mkdir(exist_ok=True, parents=True)

    def cell(x):
        if x is None or (isinstance(x, float) and not isfinite(x)):
            return ""
        return x

    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_COLUMNS)
        for r in sorted(records, key=lambda r: (r["M_Mp"], r["Lambda_eV"], r["beta"])):
            writer.writerow([cell(r[c]) for c in CSV_COLUMNS])
