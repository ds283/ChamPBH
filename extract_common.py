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

from copy import deepcopy
from datetime import datetime
from math import fabs, exp
from typing import Optional

from matplotlib.patches import Patch
from numpy import nan

from ComputeTargets import ScalarModel, ScalarModelValue
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


def add_ScalarModel_labels(fig, model: ScalarModel, model_label):
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

    if "hard_reflections" in extra_data:
        fig.text(
            LEFT_COLUMN,
            BELOW_PLOTS_TOP_ROW,
            f"Hard reflections: {extra_data['hard_reflections']}",
            horizontalalignment="left",
            fontsize="xx-small",
        )
    else:
        # is 'hard_reflections' key is missing, it means there were none
        fig.text(
            LEFT_COLUMN,
            BELOW_PLOTS_TOP_ROW,
            f"Hard reflections: 0",
            horizontalalignment="left",
            fontsize="xx-small",
        )

    if "number_fragments" in extra_data:
        fig.text(
            RIGHT_COLUMN,
            BELOW_PLOTS_TOP_ROW,
            f"Solution fragments: {extra_data['number_fragments']}",
            horizontalalignment="right",
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


def _find_T_event_times(model: ScalarModel):
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


def get_x_coord(value: ScalarModelValue, x_coord: str = "redshift") -> float:
    if x_coord == "efolds":
        return value.raw_N

    return 1.0 + value.z.z


def get_xpos_attr(obj, x_coord: str = "redshift") -> float:
    if x_coord == "efolds":
        return obj["raw_N"]

    return obj["z"]


def add_temperature_yaxis_labels(ax, model: ScalarModel, temp_unit: str = "GeV"):
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
    model: ScalarModel,
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
