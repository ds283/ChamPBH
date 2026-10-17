"""
Collect the run outputs in WORK into this report's data/ directory. Run from the repository root:
    ./venv/bin/python .documents/peakewsurf-field-equations/scripts/build_data.py WORK

WORK holds nbrun/ (nb_common.wls outputs and logs), pipe/, pipe_r10/, pipe100/, pipephi*/
(pipe_runs.py outputs) and compare.json (compare_histories.py).
"""

import glob
import json
import os
import shutil
import sys

import numpy as np
import pandas as pd

W = sys.argv[1]
HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "..", "data")
HIST = os.path.join(DATA, "histories")
LOGS = os.path.join(DATA, "logs")
for d in (HIST, LOGS):
    os.makedirs(d, exist_ok=True)


def h(path):
    x = pd.read_csv(path)
    x["N"] = x["N"].round(6)
    return x[["N", "phi", "phiprime", "lnTJ"]]


def resid(a, b):
    m = a.merge(b, on="N", suffixes=("_a", "_b"))
    return m.N.values, (m.lnTJ_a - m.lnTJ_b).values


# 1. residuals |d ln T_J|(N) for each pairing, at the four couplings drawn in the report
pairs = {
    "common_vs_pipe": ("nbrun/scan_b{b}_common.csv", "pipe/b{b}_pipe.csv"),
    "common_vs_pipe_rtol1e-10": (
        "nbrun/scan_b{b}_common.csv",
        "pipe_r10/b{b}_pipe.csv",
    ),
    "pipe_rtol1e-8_vs_1e-10": ("pipe/b{b}_pipe.csv", "pipe_r10/b{b}_pipe.csv"),
    "nb_vs_nbexact": ("nbrun/scan_b{b}_nb.csv", "pipe/b{b}_nbexact.csv"),
    "pipeSig_vs_pipe": ("nbrun/scan_b{b}_pipeSig.csv", "pipe/b{b}_pipe.csv"),
}
for b in ("2.99500", "3.01500", "3.02500"):
    cols = {}
    for k, (fa, fb) in pairs.items():
        try:
            N, d = resid(h(f"{W}/{fa.format(b=b)}"), h(f"{W}/{fb.format(b=b)}"))
        except FileNotFoundError:
            continue
        cols[k] = pd.Series(d, index=N)
    pd.DataFrame(cols).rename_axis("N").to_csv(
        f"{HIST}/residuals_b{b}.csv", float_format="%.4e"
    )

# 1b. max |d ln T_J| over the whole history, per coupling and pairing
SCAN = [
    "2.99000",
    "2.99500",
    "3.00000",
    "3.00280",
    "3.00290",
    "3.00500",
    "3.01000",
    "3.01500",
    "3.01940",
    "3.01950",
    "3.02000",
    "3.02500",
    "3.03000",
]
rows = []
for b in SCAN:
    row = {"beta": float(b)}
    for k, (fa, fb) in pairs.items():
        try:
            row[k] = float(
                np.abs(
                    resid(h(f"{W}/{fa.format(b=b)}"), h(f"{W}/{fb.format(b=b)}"))[1]
                ).max()
            )
        except FileNotFoundError:
            row[k] = np.nan
    rows.append(row)
pd.DataFrame(rows).to_csv(f"{DATA}/max_residuals.csv", index=False, float_format="%.4e")

# 2. histories for the overlay figure (every 0.05 e-folds)
for b in ("2.99500", "3.01500", "3.01940", "3.01950", "3.02500"):
    for src, tag in (
        (f"nbrun/scan_b{b}_common.csv", "nb_common"),
        (f"pipe/b{b}_pipe.csv", "pipe_pipe"),
        (f"nbrun/scan_b{b}_nb.csv", "nb_nb"),
        (f"pipe/b{b}_nbexact.csv", "pipe_nbexact"),
    ):
        x = h(f"{W}/{src}")
        x[(np.round(x.N * 100) % 5) == 0].to_csv(
            f"{HIST}/scan_b{b}_{tag}.csv", index=False, float_format="%.9g"
        )

# 3. the phi_i sweep and the long phi_i = 100 histories (every 0.05 e-folds)
sweep = {
    "phi10_nb": "nbrun/scan_b3.00500_nb.csv",
    "phi72_nb": "nbrun/onset72_b3.00500_nb.csv",
    "phi75_nb": "nbrun/onset75_b3.00500_nb.csv",
    "phi100_nb": "nbrun/p100long_b3.00500_nb.csv",
    "phi100_nbLog": "nbrun/p100long_b3.00500_nbLog.csv",
    "phi100_common": "nbrun/p100long_b3.00500_common.csv",
    "phi100_commonLog": "nbrun/p100long_b3.00500_commonLog.csv",
    "phi100_pipe_nbexact": "pipe100/b3.00500_nbexact.csv",
    "phi100_pipe_pipe": "pipe100/b3.00500_pipe.csv",
}
for tag, src in sweep.items():
    p = f"{W}/{src}"
    if not os.path.exists(p):
        print("missing", p)
        continue
    x = h(p)
    x[(np.round(x.N * 100) % 5) == 0].to_csv(
        f"{HIST}/phisweep_b3.00500_{tag}.csv", index=False, float_format="%.9g"
    )

# 4. summaries and logs
shutil.copy(f"{W}/compare.json", f"{DATA}/compare.json")
runs = []
for f in sorted(glob.glob(f"{W}/pipe*/summary_*.json")):
    for r in json.load(open(f)):
        r["set"] = os.path.basename(os.path.dirname(f))
        runs.append(r)
json.dump(runs, open(f"{DATA}/pipeline_runs.json", "w"), indent=1)
for f in glob.glob(f"{W}/nbrun/*_log.txt"):
    shutil.copy(f, f"{LOGS}/notebook_{os.path.basename(f)}")
print("pipeline runs:", len(runs))
