"""Compare the swapped-Sigma notebook runs. Arg: runs dir."""

import glob
import os
import re
import sys

import numpy as np
import pandas as pd

R = sys.argv[1]


def load(tag, b, var):
    f = os.path.join(R, f"{tag}_b{b:.3f}_{var}.csv")
    return pd.read_csv(f) if os.path.exists(f) else None


def turning(d, b):
    g = 1.0 + b * d.phiprime.values
    i = np.where(g[:-1] * g[1:] < 0)[0]
    out = []
    for k in i:
        # linear refine
        t = g[k] / (g[k] - g[k + 1])
        N = d.N.values[k] + t * (d.N.values[k + 1] - d.N.values[k])
        lnT = d.lnTJ.values[k] + t * (d.lnTJ.values[k + 1] - d.lnTJ.values[k])
        out.append((N, np.exp(lnT), "min" if g[k] > 0 else "max"))
    return out


def diff(a, c):
    N = np.intersect1d(np.round(a.N.values, 6), np.round(c.N.values, 6))
    A = a.set_index(np.round(a.N, 6)).loc[N]
    C = c.set_index(np.round(c.N, 6)).loc[N]
    dlnT = C.lnTJ.values - A.lnTJ.values
    dphi = C.phi.values - A.phi.values
    k = np.argmax(abs(dlnT))
    return abs(dlnT).max(), N[k], abs(dphi).max(), abs(dphi / A.phi.values).max()


def fmt_tp(tps):
    return "; ".join(f"{t}@N={N:.3f},T={T:.4g}" for N, T, t in tps) or "none"


for tag in ["scan", "phi100", "conv", "ctrl"]:
    files = glob.glob(os.path.join(R, f"{tag}_b*_*.csv"))
    if not files:
        continue
    betas = sorted({float(re.search(r"_b([0-9.]+)_", f).group(1)) for f in files})
    print(f"\n######## {tag}")
    for b in betas:
        runs = {v: load(tag, b, v) for v in ["nb", "pipe", "ew", "raw"]}
        runs = {k: v for k, v in runs.items() if v is not None}
        print(f"beta = {b:.3f}")
        for v, d in runs.items():
            print(
                f"   {v:4s} N_end {d.N.values[-1]:8.3f}  T_J(end) {np.exp(d.lnTJ.values[-1]):9.4g} GeV"
                f"  min T_J {np.exp(d.lnTJ.min()):8.4g} GeV  | TPs: {fmt_tp(turning(d, b))}"
            )
        ref = runs.get("nb")
        if ref is not None:
            for v in ["ew", "pipe"]:
                if v in runs:
                    m = diff(ref, runs[v])
                    print(
                        f"   {v} vs nb: max|dlnT_J| {m[0]:.3g} (at N={m[1]:.2f}), max|dphi| {m[2]:.3g} Mp, max rel dphi {m[3]:.3g}"
                    )

# controls against the scan / step-size variants
for b in [3.0, 3.015]:
    a, c = load("scan", b, "nb"), load("ctrl", b, "raw")
    if a is not None and c is not None:
        m = diff(a, c)
        print(
            f"\ncontrol beta={b}: raw (unguarded notebook Sigma) vs nb: max|dlnT_J| {m[0]:.3g}, max|dphi| {m[2]:.3g}"
        )
for v in ["nb", "ew"]:
    a, c = load("scan", 3.015, v), load("conv", 3.015, v)
    if a is not None and c is not None:
        m = diff(a, c)
        print(
            f"convergence beta=3.015 {v}: step 1e-3 vs 1e-4: max|dlnT_J| {m[0]:.3g}, max|dphi| {m[2]:.3g}; N_end {a.N.values[-1]:.4f} vs {c.N.values[-1]:.4f}"
        )
