from functools import partial
from math import fabs
from typing import Tuple

import pandas as pd

from CosmologyModels.GenericEOS.QCD_EOS import QCD_EOS
from Units import GeV_units


def get_w(units, model, row) -> Tuple[float, float]:
    T_in_GeV: float = float(row["T"])
    G: float = float(row["G"])
    Gs: float = float(row["Gs"])
    w: float = float(row["w"])
    Xav_w: float = float(row["Xav_w"])

    T: float = T_in_GeV * units.GeV
    my_G: float = model.G(T)
    my_Gs: float = model.Gs(T)
    my_w: float = model.w(T)

    return (
        my_w,
        fabs((my_w - w) / my_w),
        fabs((my_w - Xav_w) / my_w),
        my_G,
        fabs((my_G - G) / my_G),
        my_Gs,
        fabs((my_Gs - Gs) / my_Gs),
    )


units = GeV_units()
eos = QCD_EOS(units)
data = pd.read_csv("CosmologyModels/GenericEOS/XavEOS_data.csv")

(
    data["my_w"],
    data["my_w_err"],
    data["Xav_w_err"],
    data["my_G"],
    data["my_G_err"],
    data["my_Gs"],
    data["my_Gs_err"],
) = zip(*data.apply(partial(get_w, units, eos), axis=1))

data.to_csv("CosmologyModels/GenericEOS/XavEOS_data_out.csv")
