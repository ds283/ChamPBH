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

import argparse
from math import fabs
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt

DEFAULT_CHAMPBH_OUTPUT_FILE = "/Users/ds283/Documents/Code/ChamPBH/XavComparison-Radau-small-out/QCD_Cosmology/csv/beta=1.5/M=1.218e+27eV_Lambda=0.001eV/fields.csv"
DEFAULT_MATHEMATICA_OUTPUT_FILE = "/Users/ds283/Library/CloudStorage/Box-Box/Research projects/Chameleon PBHs/Code Comparison/MathematicaOut_beta=1.5.csv"
DEFAULT_OUPTUT_DIRECTORY = (
    "/Users/ds283/Documents/Code/ChamPBH/CodeComparison-out/ComparisonProducts-out"
)

parser = argparse.ArgumentParser()
parser.add_argument("--mma-output", type=str, default=DEFAULT_MATHEMATICA_OUTPUT_FILE)
parser.add_argument("--champbh-output", type=str, default=DEFAULT_CHAMPBH_OUTPUT_FILE)
parser.add_argument("--output", type=str, default=DEFAULT_OUPTUT_DIRECTORY)
args = parser.parse_args()

champbh = pd.read_csv(args.champbh_output)
mma = pd.read_csv(args.mma_output)

champbh_rows = champbh.shape[0]
mma_rows = mma.shape[0]

if champbh_rows != mma_rows:
    raise ValueError(
        f"Number of rows in ChampBH output (={champbh_rows}) does not match number of rows in Mathematica output (={mma_rows})"
    )

comparison_columns = {
    r"$\phi_{\mathrm{E}} / M_{\mathrm{P}}$": {
        "ChamPBH": "phi_Einstein_Mp",
        "Xav": "phi_Einstein_Mp",
    },
    r"$\pi_{\mathrm{E}} / M_{\mathrm{P}}$": {
        "ChamPBH": "pi_Einstein_Mp",
        "Xav": "pi_Einstein_Mp",
    },
    r"$\ln(\rho_r/\mathrm{GeV}^4)$": {
        "ChamPBH": "log_rhorad_Einstein_GeV4",
        "Xav": "log_rho_rad_Einstein_GeV4",
    },
    r"$\ln(T_{\text{Jordan}} / \mathrm{GeV})$": {
        "ChamPBH": "log_T_Jordan_GeV",
        "Xav": "log_T_Jordan_GeV",
    },
    r"$g^\ast_\rho$": {"ChamPBH": "gstar_rho", "Xav": "g*_rho"},
    r"$g^\ast_s$": {"ChamPBH": "gstar_s", "Xav": "g*s(interpolated)"},
    r"$\mathrm{d}g^\ast_s/\mathrm{d}\ln T$": {
        "ChamPBH": "dgstar_s_dlogT",
        "Xav": "dg*s/dlogT(interpolated)",
    },
}


def extract_data(
    champbh_df: pd.DataFrame, mma_df: pd.DataFrame, config
) -> List[Dict[str, Any]]:
    data = {}

    for i in range(1, champbh_df.shape[0]):
        champbh_row = champbh_df.iloc[i]
        mma_row = mma_df.iloc[i]

        for label, cfg in config.items():
            if label not in data:
                data[label] = []

            z = champbh_row["z"]
            N = champbh_row["raw_N"]

            champbh_value = champbh_row[cfg["ChamPBH"]]
            mma_value = mma_row[cfg["Xav"]]

            largest = max(fabs(champbh_value), fabs(mma_value))

            data[label].append(
                {
                    "z": z,
                    "N": N,
                    "ChampPBH": champbh_value,
                    "Xav": mma_value,
                    "diff": champbh_value - mma_value,
                    "absdiff": fabs(champbh_value - mma_value),
                    "relerr": fabs((champbh_value - mma_value) / largest),
                    "%err": 100.0 * fabs((champbh_value - mma_value) / largest),
                }
            )

    return data


paired_data = extract_data(champbh, mma, comparison_columns)
sns.set_theme()

fig = plt.figure()
ax = fig.gca()

for label in comparison_columns.keys():
    d = paired_data[label]

    x_series = [row["N"] for row in d]
    y_series = [row["%err"] for row in d]
    ax.plot(x_series, y_series, label=label)

ax.legend(loc="best")
ax.set_xlabel(r"e-folds $N$")
ax.set_yscale("log")
ax.grid(True)
ax.set_ylabel(r"Relative error [%]")

base_path = Path(args.output).resolve()
fig_path = base_path / f"plots.pdf"
fig_path.parents[0].mkdir(exist_ok=True, parents=True)
fig.savefig(fig_path)
fig.savefig(fig_path.with_suffix(".png"))
