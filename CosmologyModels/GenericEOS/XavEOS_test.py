from functools import partial
from math import fabs
from typing import Tuple

import pandas as pd

from CosmologyModels.GenericEOS.QCD_EOS_jax_autodiff import QCD_EOS_jax
from CosmologyModels.GenericEOS.QCD_EOS_spline import QCD_EOS_spline
from Units import GeV_units


def get_w(units, jax_eos, spline_eos, row) -> Tuple[float, float]:
    T_in_GeV: float = float(row["T"])
    G: float = float(row["G"])
    Gs: float = float(row["Gs"])
    w: float = float(row["w"])
    Xav_w: float = float(row["Xav_w"])

    T: float = T_in_GeV * units.GeV

    jax_G: float = jax_eos.G_rho(T)
    jax_Gs: float = jax_eos.G_s(T)
    jax_w: float = jax_eos.w(T)

    spline_G: float = spline_eos.G_rho(T)
    spline_Gs: float = spline_eos.G_s(T)
    spline_w: float = spline_eos.w(T)

    return (
        jax_w,
        fabs((jax_w - w) / jax_w),
        fabs((jax_w - Xav_w) / jax_w),
        spline_w,
        fabs((spline_w - w) / spline_w),
        fabs((spline_w - Xav_w) / spline_w),
        jax_G,
        fabs((jax_G - G) / jax_G),
        spline_G,
        fabs((spline_G - G) / spline_G),
        jax_Gs,
        fabs((jax_Gs - Gs) / jax_Gs),
        spline_Gs,
        fabs((spline_Gs - Gs) / spline_Gs),
    )


units = GeV_units()
jax_eos = QCD_EOS_jax(units)
spline_eos = QCD_EOS_spline(units)

data = pd.read_csv("CosmologyModels/GenericEOS/XavEOS_data.csv")

(
    data["jax_w"],
    data["jax_w_err"],
    data["jax_Xav_w_err"],
    data["spline_w"],
    data["spline_w_err"],
    data["spline_Xav_w_err"],
    data["jax_G"],
    data["jax_G_err"],
    data["spline_G"],
    data["spline_G_err"],
    data["jax_Gs"],
    data["jax_Gs_err"],
    data["spline_Gs"],
    data["spline_Gs_err"],
) = zip(*data.apply(partial(get_w, units, jax_eos, spline_eos), axis=1))

data.to_csv("CosmologyModels/GenericEOS/XavEOS_data_out.csv")
