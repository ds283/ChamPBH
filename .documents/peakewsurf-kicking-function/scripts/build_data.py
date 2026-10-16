"""
Collect the compact data products of the PeakEWsurf.nb kicking-function comparison into data/.

Inputs (all written by the other scripts in this directory into one work directory WORK):
    WORK/nb_w_table.csv            the notebook's w(T) table          (nb_kick.wls)
    WORK/compare.csv               Sigma and f_m on the common grid    (compare_kick.py)
    WORK/sigma_csv_nbscheme.csv    CSV rows, notebook interpolation    (sigma_cross.wls)
    WORK/runs/*.csv, log.txt       the NDSolve histories               (swap_runs.wls)
    WORK/bis/log.txt, bis2/log.txt the threshold bisections            (bisect.wls)

Run from the repository root:
    ./venv/bin/python .documents/peakewsurf-kicking-function/scripts/build_data.py WORK
"""

import json
import os
import re
import shutil
import sys

import numpy as np
import pandas as pd
from scipy.interpolate import make_interp_spline

WORK = sys.argv[1]
HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "..", "data")
os.makedirs(os.path.join(DATA, "histories"), exist_ok=True)

# ---------------------------------------------------------------- tables
shutil.copy(
    os.path.join(WORK, "nb_w_table.csv"), os.path.join(DATA, "notebook_w_table.csv")
)
nbrows = pd.read_csv(os.path.join(WORK, "nb_w_table.csv"))
csvrows = pd.read_csv("CosmologyModels/GenericEOS/Xav_EOS_data.csv")

# ---------------------------------------------------------------- Sigma grid
c = pd.read_csv(os.path.join(WORK, "compare.csv"))
x = pd.read_csv(os.path.join(WORK, "sigma_csv_nbscheme.csv"))
c = c[c.T_GeV <= 2.0e4].reset_index(drop=True)
T = c.T_GeV.values
assert np.allclose(T, x.T_GeV.values[: len(T)])

# the notebook's rows interpolated the pipeline's way (cubic spline in ln T, 1/3 outside the table)
sp = make_interp_spline(np.log(nbrows.T_GeV.values), nbrows.w.values)
lo, hi = nbrows.T_GeV.min(), nbrows.T_GeV.max()
sig_nb_pipescheme = np.where((T > lo) & (T < hi), 1.0 - 3.0 * sp(np.log(T)), 0.0)

grid = pd.DataFrame(
    {
        "T_GeV": T,
        "Sigma_nb": c.Sigma_nb.values,  # notebook rows, notebook interpolation
        "Sigma_nb_pipescheme": sig_nb_pipescheme,  # notebook rows, pipeline interpolation
        "Sigma_csv_nbscheme": x.Sigma_csvnb.values[
            : len(T)
        ],  # pipeline rows, notebook interpolation
        "Sigma_pipe": c.Sigma_pipe.values,  # pipeline rows, pipeline interpolation
        "fm_pipe": c.fm_pipe.values,
        "fmT_nb": c.fmT_nb.values,
        "fmTinterp_nb": c.fmTinterp_nb.values,
    }
)


# statistics at the full resolution (1000 points per decade)
def window(lo, hi):
    return (T >= lo) & (T < hi)


def peak(col, lo, hi):
    s = window(lo, hi)
    i = np.argmax(grid[col].values[s])
    return float(grid[col].values[s][i]), float(T[s][i])


def integral(col, lo, hi):
    s = window(lo, hi)
    return float(np.trapezoid(grid[col].values[s], np.log(T[s])))


def maxdiff(a, b, lo, hi):
    s = window(lo, hi)
    d = grid[a].values[s] - grid[b].values[s]
    k = np.argmax(abs(d))
    return float(abs(d).max()), float(T[s][k])


stats = {"peaks": {}, "integrals": {}, "maxdiff": {}, "rows": {}}
for name, (lo_, hi_) in {
    "ee": (1e-5, 2e-3),
    "QCD": (2e-3, 1.0),
    "EW": (10.0, 386.7),
}.items():
    for col in ["Sigma_nb", "Sigma_pipe"]:
        stats["peaks"][f"{name}:{col}"] = peak(col, lo_, hi_)
        stats["integrals"][f"{name}:{col}"] = integral(col, lo_, hi_)
for lo_, hi_ in [(20.0, 386.7), (386.7, 2e4), (20.0, 2e4)]:
    for col in ["Sigma_nb", "Sigma_nb_pipescheme", "Sigma_csv_nbscheme", "Sigma_pipe"]:
        stats["integrals"][f"{lo_}-{hi_}:{col}"] = integral(col, lo_, hi_)
for lo_, hi_ in [(1e-5, 2e-3), (2e-3, 1.0), (1.0, 20.0), (20.0, 386.7), (386.7, 2e4)]:
    for a, b in [
        ("Sigma_nb", "Sigma_pipe"),
        ("Sigma_csv_nbscheme", "Sigma_nb"),  # data, notebook scheme
        ("Sigma_pipe", "Sigma_nb_pipescheme"),  # data, pipeline scheme
        ("Sigma_pipe", "Sigma_csv_nbscheme"),  # scheme, pipeline rows
        ("Sigma_nb_pipescheme", "Sigma_nb"),  # scheme, notebook rows
    ]:
        stats["maxdiff"][f"{lo_}-{hi_}:{a}-{b}"] = maxdiff(a, b, lo_, hi_)
r = grid.fmT_nb / grid.fm_pipe
stats["fm"] = {
    "ratio_min": [float(r.min()), float(T[np.argmin(r)])],
    "ratio_max": [float(r.max()), float(T[np.argmax(r)])],
    "fm_over_Sigma_max_below_2e4": float(
        (grid.fm_pipe / grid.Sigma_pipe.clip(lower=1e-300))[window(1e-4, 2e4)].max()
    ),
}
for t in [1e-4, 0.18, 53.0]:
    i = int(np.argmin(abs(T - t)))
    stats["fm"][f"at_{t}"] = {
        "T": float(T[i]),
        "fm_pipe": float(grid.fm_pipe[i]),
        "fmT_nb": float(grid.fmT_nb[i]),
        "Sigma_pipe": float(grid.Sigma_pipe[i]),
    }

# row-by-row comparison of the two tables
lc = np.log10(csvrows.T_GeV.values)
rows = []
for Tn, wn in zip(nbrows.T_GeV, nbrows.w):
    j = int(np.argmin(abs(lc - np.log10(Tn))))
    on = abs(lc[j] - np.log10(Tn)) < 1e-9
    rows.append(
        {
            "T_GeV": Tn,
            "Sigma_nb": 1 - 3 * wn,
            "Sigma_csv": (1 - 3 * csvrows.w.values[j]) if on else np.nan,
        }
    )
rows = pd.DataFrame(rows)
rows["dSigma"] = rows.Sigma_nb - rows.Sigma_csv
rows.to_csv(
    os.path.join(DATA, "table_rows_compared.csv"), index=False, float_format="%.10g"
)
on = rows.Sigma_csv.notna()
stats["rows"] = {
    "notebook_rows": int(len(nbrows)),
    "csv_rows": int(len(csvrows)),
    "on_csv_grid": int(on.sum()),
    "identical_1e-12": int((abs(rows.dSigma[on]) < 1e-12).sum()),
    "differing": rows[on & (abs(rows.dSigma) >= 1e-12)][
        ["T_GeV", "Sigma_nb", "Sigma_csv", "dSigma"]
    ].values.tolist(),
}

# the downsampled grid for the figures (every 5th point: 200 per decade)
grid.iloc[::5].to_csv(
    os.path.join(DATA, "sigma_fm_grid.csv"), index=False, float_format="%.10g"
)

# ---------------------------------------------------------------- runs
NUM = r"[-+]?\d*\.?\d+(?:\*\^[-+]?\d+)?"


def parse_tps(s):
    out = []
    for m in re.finditer(
        r"\{(" + NUM + r"), (" + NUM + r"), (" + NUM + r"), (min|max)\}", s
    ):
        f = [float(v.replace("*^", "e")) for v in m.groups()[:3]]
        out.append({"N": f[0], "T_J_GeV": f[1], "phi_Mp": f[2], "type": m.group(4)})
    return out


def parse_runlog(path, tagmap):
    """swap_runs.wls log: 'beta | var | time | N_end | T_end' then 'turning points ...'."""
    recs, tag = [], None
    lines = open(path).read().splitlines()
    for k, ln in enumerate(lines):
        if ln.startswith("###"):
            tag = tagmap.get(ln.strip("# ").strip(), ln)
            continue
        m = re.match(
            r"^("
            + NUM
            + r") \| (\w+) \| ("
            + NUM
            + r") \| ("
            + NUM
            + r") \| ("
            + NUM
            + r")$",
            ln,
        )
        if m:
            recs.append(
                {
                    "set": tag,
                    "beta": float(m.group(1)),
                    "variant": m.group(2),
                    "wall_s": float(m.group(3)),
                    "N_end": float(m.group(4)),
                    "T_J_end_GeV": float(m.group(5)),
                    "turning_points": parse_tps(lines[k + 1]),
                }
            )
    return recs


runs = parse_runlog(
    os.path.join(WORK, "runs", "log.txt"),
    {
        "scan phi_i=10": "scan phi_i=10, step 1e-3",
        "control raw": "control: unmodified notebook Sigma",
        "phi_i=100": "phi_i=100, step 1e-3",
        "convergence 1e-4": "convergence: step 1e-4",
    },
)


def bisect_log(path):
    recs, thr = [], {}
    for ln in open(path).read().splitlines():
        m = re.match(r"^\s+beta=NumberForm\[(" + NUM + r"), 8\] (\w+) TPs=(.*)$", ln)
        if m:
            tps = parse_tps(m.group(3))
            recs.append(
                {
                    "beta": float(m.group(1)),
                    "variant": m.group(2),
                    "turning_points": tps,
                    "EW_captured": any(
                        t["type"] == "min" and t["T_J_GeV"] > 20 for t in tps
                    ),
                }
            )
        m = re.match(
            r"^### (\w+): EW-capture threshold in \(NumberForm\[("
            + NUM
            + r"), 8\], NumberForm\[("
            + NUM
            + r"), 8\]\)",
            ln,
        )
        if m:
            thr[m.group(1)] = [float(m.group(2)), float(m.group(3))]
    return recs, thr


b1, t1 = bisect_log(os.path.join(WORK, "bis", "log.txt"))
b2, t2 = bisect_log(os.path.join(WORK, "bis2", "log.txt"))
stats["thresholds"] = {**t1, **t2}
json.dump(
    {"runs": runs, "bisection": b1 + b2},
    open(os.path.join(DATA, "runs_summary.json"), "w"),
    indent=1,
)

# downsampled histories (every 0.05 e-folds) for the figures
for name in [
    f"scan_b{b:.3f}_{v}"
    for b in (2.995, 3.000, 3.005, 3.015, 3.020, 3.030)
    for v in ("nb", "pipe")
] + [
    "phi100_b3.005_nb",
    "phi100_b3.005_pipe",
]:
    d = pd.read_csv(os.path.join(WORK, "runs", name + ".csv"))
    d.iloc[::5].to_csv(
        os.path.join(DATA, "histories", name + ".csv"), index=False, float_format="%.9g"
    )


# controls: the substituted notebook Sigma vs the unmodified one, and the step-size check
def maxdev(a, b):
    A = pd.read_csv(os.path.join(WORK, "runs", a + ".csv"))
    B = pd.read_csv(os.path.join(WORK, "runs", b + ".csv"))
    n = min(len(A), len(B))
    return float(abs(A.lnTJ.values[:n] - B.lnTJ.values[:n]).max()), float(
        abs(A.phi.values[:n] - B.phi.values[:n]).max()
    )


stats["controls"] = {
    "raw_vs_nb_beta3.000": maxdev("ctrl_b3.000_raw", "scan_b3.000_nb"),
    "raw_vs_nb_beta3.015": maxdev("ctrl_b3.015_raw", "scan_b3.015_nb"),
    "step1e-4_vs_1e-3_nb_beta3.015": maxdev("conv_b3.015_nb", "scan_b3.015_nb"),
    "step1e-4_vs_1e-3_ew_beta3.015": maxdev("conv_b3.015_ew", "scan_b3.015_ew"),
}
ew_vs_pipe = []
for b in (2.99, 2.995, 3.0, 3.005, 3.01, 3.015, 3.02, 3.025, 3.03):
    ew_vs_pipe.append(maxdev(f"scan_b{b:.3f}_ew", f"scan_b{b:.3f}_pipe"))
stats["controls"]["ew_vs_pipe_max_over_scan"] = [
    max(v[0] for v in ew_vs_pipe),
    max(v[1] for v in ew_vs_pipe),
]

json.dump(stats, open(os.path.join(DATA, "stats.json"), "w"), indent=1)
print(
    json.dumps(
        {k: stats[k] for k in ("rows", "thresholds", "controls", "fm")}, indent=1
    )
)
