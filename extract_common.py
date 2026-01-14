from datetime import datetime
from math import fabs
from typing import Optional

from ComputeTargets import ScalarModel
from CosmologyConcepts import beta_value, M_value, Lambda_value
from Quadrature.integration_metadata import IntegrationSolver
from Units.base import UnitsLike

TEXT_DISPLACEMENT_MULTIPLIER = 0.85

TOP_ROW = 1.12
MIDDLE_ROW = 1.07
BOTTOM_ROW = 1.02

LEFT_COLUMN = 0.0
MIDDLE_COLUMN = 0.4
RIGHT_COLUMN = 0.85


def safe_fabs(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None

    return fabs(x)


def safe_div(x: Optional[float], y: float) -> Optional[float]:
    if x is None or y is None:
        return None

    try:
        return x / y
    except ZeroDivisionError:
        pass

    return None


def set_loglinear_axes(ax):
    ax.set_xscale("log")
    ax.set_yscale("linear")
    ax.legend(loc="best")
    ax.grid(True)
    ax.xaxis.set_inverted(True)


def set_loglog_axes(ax):
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.legend(loc="best")
    ax.grid(True)
    ax.xaxis.set_inverted(True)


def set_linear_axes(ax):
    ax.set_xscale("linear")
    ax.set_yscale("linear")
    ax.legend(loc="best")
    ax.grid(True)
    ax.xaxis.set_inverted(True)


def add_plot_labels(ax, model: ScalarModel, model_label):
    units: UnitsLike = model._units

    solver: IntegrationSolver = model.solver
    beta: beta_value = model.coupling._beta
    M: M_value = model.potential._M
    Lambda: Lambda_value = model.potential._Lambda

    now = datetime.now()

    ax.text(
        LEFT_COLUMN,
        TOP_ROW,
        f"$\\beta={beta.as_float:.5g}$",
        transform=ax.transAxes,
        fontsize="x-small",
    )
    ax.text(
        MIDDLE_COLUMN,
        TOP_ROW,
        f"$M={M.as_float / units.eV:.5g}$ eV",
        transform=ax.transAxes,
        fontsize="x-small",
    )
    ax.text(
        RIGHT_COLUMN,
        TOP_ROW,
        f"$\\Lambda={Lambda.as_float / units.eV:.5g}$ eV",
        transform=ax.transAxes,
        fontsize="x-small",
    )

    ax.text(
        LEFT_COLUMN,
        BOTTOM_ROW,
        f"Solver: {solver.label}",
        transform=ax.transAxes,
        fontsize="x-small",
    )
    ax.text(
        MIDDLE_COLUMN,
        BOTTOM_ROW,
        f"Created at: {now.strftime("%a %d %b %Y %H:%M:%S")}",
        transform=ax.transAxes,
        fontsize="x-small",
    )
