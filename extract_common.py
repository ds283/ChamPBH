from math import fabs
from typing import Optional

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


def add_plot_labels(ax, units, beta, M, Lambda, model_label):
    ax.text(
        LEFT_COLUMN,
        TOP_ROW,
        f"$\\beta={beta:.5g}$",
        transform=ax.transAxes,
        fontsize="x-small",
    )
    ax.text(
        MIDDLE_COLUMN,
        TOP_ROW,
        f"$M={M / units.eV:.5g}$ eV",
        transform=ax.transAxes,
        fontsize="x-small",
    )
    ax.text(
        RIGHT_COLUMN,
        TOP_ROW,
        f"$\\Lambda={Lambda / units.eV:.5g}$ eV",
        transform=ax.transAxes,
        fontsize="x-small",
    )
