# Read-only comparison of alpha = 0.08 dissipation histories across M, written 2026-11-02 after the
# diagnostic: the run's main store (M <= 1e-12) beside the diagnostic store (1e-9 to 0.5), at
# beta = 1.2, 2.0, 2.9. Opens every shard with sqlite's mode=ro; no Ray, no datastorekit.
# Run from the repository root:  ./venv/bin/python science-runs/2026-11-01-science-2026.8.0/diagnostic/compare_proxy.py
import glob
import math
import sqlite3

import pandas as pd

MP_EV = 2.4353e27
STORES = [
    "var/datastores/science-2026.8.0",
    "var/datastores/science-2026.8.0-large-M-dissipation",
]


def load(prefix):
    # ExponentialModel, M_value, beta_value and alpha_value are read from the shard that holds the history
    frames = []
    for f in sorted(glob.glob(prefix + "-shard*.db")):
        c = sqlite3.connect(f"file:{f}?mode=ro", uri=True)
        h = pd.read_sql("select * from ScalarHistory where failure=0", c)
        m = pd.read_sql(
            "select e.serial as model_serial, m.value_eV as M_eV, b.value as beta from ExponentialModel e"
            " join M_value m on e.M_serial=m.serial join beta_value b on e.beta_serial=b.serial",
            c,
        )
        a = pd.read_sql(
            "select serial as alpha_serial, value as alpha from alpha_value", c
        )
        frames.append(h.merge(m, on="model_serial").merge(a, on="alpha_serial"))
    h = pd.concat(frames)
    h["M_Mp"] = h.M_eV / MP_EV
    return h


h = pd.concat([load(p) for p in STORES])
h = h[(h.alpha == 0.08) & h.beta.round(6).isin([1.2, 2.0, 2.9])]
# the stores' temperatures are in GeV (QCD_Cosmology's units); checked against histories.csv's parking_T_GeV
for k in ["parking", "release"]:
    h[k + "_T_keV"] = h[k + "_log_T_Jordan"].apply(
        lambda x: math.exp(x) * 1e6 if pd.notna(x) else float("nan")
    )
h["log10_R_first_impact"] = h.log_R_first_impact / math.log(10)
cols = [
    "beta",
    "M_Mp",
    "injections",
    "resolved_candidates",
    "unresolved_candidates",
    "injected_fraction",
    "rho_chi_ratio_1MeV",
    "rho_chi_ratio_today",
    "log10_R_first_impact",
    "parking_T_keV",
    "release_T_keV",
    "tuning_iterations",
]
pd.set_option("display.width", 250)
print(
    h.sort_values(["beta", "M_Mp"])[cols].to_string(
        index=False, float_format=lambda v: f"{v:.6g}"
    )
)
