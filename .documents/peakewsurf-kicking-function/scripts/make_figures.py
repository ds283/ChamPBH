"""
Draw the report's figures from data/ into figures/. Run from anywhere:
    ./venv/bin/python .documents/peakewsurf-kicking-function/scripts/make_figures.py
"""

import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "..", "data")
FIG = os.path.join(HERE, "..", "figures")
os.makedirs(FIG, exist_ok=True)

# categorical slots 1-3 of the reference palette; line style is the secondary encoding
NB, PIPE, THIRD = "#2a78d6", "#eb6834", "#1baf7a"
INK, MUTED, GRID = "#1f1f1e", "#6b6a63", "#e4e3dc"
W = 6.3  # text width in inches

plt.rcParams.update(
    {
        "font.size": 8.5,
        "axes.titlesize": 9,
        "axes.edgecolor": MUTED,
        "axes.labelcolor": INK,
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "xtick.labelcolor": INK,
        "ytick.labelcolor": INK,
        "axes.grid": True,
        "grid.color": GRID,
        "grid.linewidth": 0.5,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "legend.frameon": False,
        "lines.linewidth": 1.6,
        "savefig.bbox": "tight",
    }
)
LAB_NB = "notebook table, notebook interpolation"
LAB_PIPE = "pipeline table, pipeline spline"

g = pd.read_csv(os.path.join(DATA, "sigma_fm_grid.csv"))
nbrows = pd.read_csv(os.path.join(DATA, "notebook_w_table.csv"))
csvrows = pd.read_csv(
    os.path.join(
        os.path.dirname(HERE),
        "..",
        "..",
        "CosmologyModels",
        "GenericEOS",
        "Xav_EOS_data.csv",
    )
)
stats = json.load(open(os.path.join(DATA, "stats.json")))
runs = json.load(open(os.path.join(DATA, "runs_summary.json")))


def title(ax, s):
    ax.set_title(s, loc="left", color=INK)


# ------------------------------------------------------------------ Figure 1: Sigma(T)
fig, (a, b) = plt.subplots(2, 1, figsize=(W, 5.0), constrained_layout=True)
a.plot(g.T_GeV, g.Sigma_nb, color=NB, label=LAB_NB)
a.plot(g.T_GeV, g.Sigma_pipe, color=PIPE, ls="--", label=LAB_PIPE)
a.set_xscale("log")
a.set_xlim(1e-5, 2e4)
a.set_ylabel(r"$\Sigma = 1-3w$")
a.set_xlabel(r"$T_J$ [GeV]")
for T, S, lab in [
    (1.6e-4, 0.1007, r"$e^\pm$"),
    (0.182, 0.3145, "QCD"),
    (53.2, 0.0374, "EW"),
]:
    a.annotate(
        lab,
        (T, S),
        xytext=(0, 4),
        textcoords="offset points",
        ha="center",
        va="bottom",
        color=INK,
    )
a.set_ylim(-0.01, 0.36)
a.legend(loc="upper right")
title(a, r"(a) The kicking function over the whole range ($T_J \leq 2\times10^4$ GeV)")

s = g.T_GeV >= 10
b.plot(g.T_GeV[s], g.Sigma_nb[s], color=NB, label=LAB_NB)
b.plot(g.T_GeV[s], g.Sigma_pipe[s], color=PIPE, ls="--", label=LAB_PIPE)
r = nbrows[nbrows.T_GeV >= 10]
b.plot(
    r.T_GeV,
    1 - 3 * r.w,
    "o",
    ms=4,
    color=NB,
    mec="white",
    mew=0.6,
    label="notebook table rows",
    zorder=5,
)
r = csvrows[(csvrows.T_GeV >= 10) & (csvrows.T_GeV <= 2e4)]
b.plot(
    r.T_GeV,
    1 - 3 * r.w,
    "s",
    ms=3.6,
    mfc="white",
    mec=PIPE,
    mew=1.0,
    label="pipeline table rows",
    zorder=4,
)
b.axvline(386.7, color=MUTED, lw=0.8, ls=":")
b.annotate(
    "notebook table ends: $w=1/3$ imposed\nat 386.7 GeV, $\\Sigma=0$ above",
    (386.7, 0.012),
    xytext=(8, 0),
    textcoords="offset points",
    color=MUTED,
    fontsize=7.5,
    va="center",
)
b.axvspan(22.4, 386.7, color=GRID, alpha=0.45, lw=0)
b.text(26, 0.0405, "rows differ", color=MUTED, fontsize=7.5)
b.set_xscale("log")
b.set_xlim(10, 2e4)
b.set_ylim(-0.002, 0.043)
b.set_xlabel(r"$T_J$ [GeV]")
b.set_ylabel(r"$\Sigma = 1-3w$")
b.legend(loc="upper right", fontsize=7.5)
title(b, "(b) The electroweak scale, with the tabulated rows")
fig.savefig(os.path.join(FIG, "sigma_overview.pdf"))
plt.close(fig)

# ------------------------------------------------------------------ Figure 2: data vs scheme
fig, ax = plt.subplots(figsize=(W, 2.9), constrained_layout=True)
FLOOR = 1e-9


# the interpolants agree at the table nodes and differ between them, so plot the largest
# |Delta Sigma| in each 0.05-dex table interval rather than the raw oscillation
EDGES = 10.0 ** np.arange(-5.0, np.log10(2e4) + 0.05, 0.05)
CENTRES = np.sqrt(EDGES[:-1] * EDGES[1:])
BIN = np.digitize(g.T_GeV.values, EDGES) - 1


def envelope(y):
    y = np.abs(np.asarray(y))
    out = np.array(
        [y[BIN == k].max() if np.any(BIN == k) else np.nan for k in range(len(CENTRES))]
    )
    return np.where(out < FLOOR, np.nan, out)


ax.step(
    CENTRES,
    envelope(g.Sigma_pipe - g.Sigma_nb_pipescheme),
    where="mid",
    color=INK,
    lw=1.8,
    label="table rows changed, interpolation fixed (pipeline spline)",
)
ax.step(
    CENTRES,
    envelope(g.Sigma_pipe - g.Sigma_csv_nbscheme),
    where="mid",
    color=PIPE,
    ls="--",
    label="interpolation changed, pipeline rows fixed",
)
ax.step(
    CENTRES,
    envelope(g.Sigma_nb_pipescheme - g.Sigma_nb),
    where="mid",
    color=NB,
    ls=":",
    lw=1.8,
    label="interpolation changed, notebook rows fixed",
)
ax.set_xscale("log")
ax.set_yscale("log")
ax.set_xlim(1e-5, 2e4)
ax.set_ylim(FLOOR, 2e-2)
ax.set_xlabel(r"$T_J$ [GeV]")
ax.set_ylabel(r"$|\Delta\Sigma|$")
ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.2), ncol=2, fontsize=7.5)
title(ax, r"Largest $|\Delta\Sigma|$ in each 0.05-dex interval (gaps: below $10^{-9}$)")
fig.savefig(os.path.join(FIG, "sigma_data_vs_scheme.pdf"))
plt.close(fig)

# ------------------------------------------------------------------ Figure 3: f_m
fig, (a, b) = plt.subplots(1, 2, figsize=(W, 2.5), constrained_layout=True)
a.plot(g.T_GeV, g.fmT_nb / g.fm_pipe, color=INK)
a.axhline(1, color=MUTED, lw=0.8, ls=":")
a.set_xscale("log")
a.set_xlim(1e-5, 2e4)
a.set_xlabel(r"$T_J$ [GeV]")
a.set_ylabel(r"$f_m$ ratio, notebook / pipeline")
title(a, "(a) Ratio of the matter fractions")
b.plot(
    g.T_GeV,
    g.fm_pipe / g.Sigma_pipe.clip(lower=1e-300),
    color=PIPE,
    ls="--",
    label=r"pipeline $f_m/\Sigma$",
)
b.plot(
    g.T_GeV,
    g.fmT_nb / g.Sigma_nb.clip(lower=1e-300),
    color=NB,
    label=r"notebook $f_m/\Sigma$",
)
b.set_xscale("log")
b.set_yscale("log")
b.set_xlim(1e-4, 386)
b.set_ylim(1e-12, 1e-2)
b.set_xlabel(r"$T_J$ [GeV]")
b.legend(loc="upper right", fontsize=7.5)
title(b, r"(b) Size of $f_m$ relative to $\Sigma$")
fig.savefig(os.path.join(FIG, "fm.pdf"))
plt.close(fig)


# ------------------------------------------------------------------ Figure 4: histories
def hist(name):
    return pd.read_csv(os.path.join(DATA, "histories", name + ".csv"))


fig, axs = plt.subplots(2, 3, figsize=(W, 4.3), sharex="col", constrained_layout=True)
for j, beta in enumerate([3.005, 3.015, 3.020]):
    for var, col, ls, lab in [
        ("nb", NB, "-", "notebook $\\Sigma$"),
        ("pipe", PIPE, "--", "pipeline $\\Sigma$"),
    ]:
        d = hist(f"scan_b{beta:.3f}_{var}")
        axs[0, j].plot(d.N, np.exp(d.lnTJ), color=col, ls=ls, label=lab)
        axs[1, j].plot(d.N, d.phi, color=col, ls=ls, label=lab)
    for T in (53.2, 0.182):
        axs[0, j].axhline(T, color=MUTED, lw=0.7, ls=":")
    axs[0, j].set_yscale("log")
    axs[0, j].set_ylim(0.1, 4e4)
    axs[1, j].set_yscale("log")
    axs[1, j].set_ylim(0.08, 14)
    axs[1, j].set_xlabel("$N$")
    title(axs[0, j], f"$\\beta = {beta:.3f}$")
axs[0, 0].set_ylabel(r"$T_J$ [GeV]")
axs[1, 0].set_ylabel(r"$\phi / M_P$")
axs[0, 0].legend(loc="lower left", fontsize=7.5)
axs[0, 2].text(40, 53.2, "EW peak", color=MUTED, fontsize=7, ha="right", va="bottom")
axs[0, 2].text(40, 0.182, "QCD peak", color=MUTED, fontsize=7, ha="right", va="bottom")
fig.savefig(os.path.join(FIG, "histories.pdf"))
plt.close(fig)


# ------------------------------------------------------------------ Figure 5: first T_J minimum vs beta
def first_min(tps):
    m = [t for t in tps if t["type"] == "min"]
    return m[0]["T_J_GeV"] if m else None


pts = {"nb": {}, "pipe": {}, "csvnb": {}}
for rr in runs["runs"]:
    if rr["set"].startswith("scan") and rr["variant"] in ("nb", "pipe"):
        pts[rr["variant"]][rr["beta"]] = first_min(rr["turning_points"])
for rr in runs["bisection"]:
    pts[rr["variant"]][rr["beta"]] = first_min(rr["turning_points"])

NONE_Y = 0.5
fig, ax = plt.subplots(figsize=(W, 3.0), constrained_layout=True)
spec = {
    "nb": (NB, "o", "notebook table, notebook interpolation", 4.5),
    "pipe": (PIPE, "s", "pipeline table, pipeline spline", 4.0),
    "csvnb": (THIRD, "^", "pipeline table, notebook interpolation", 4.0),
}
for var, (col, mk, lab, ms) in spec.items():
    bs = sorted(pts[var])
    yes = [(b_, pts[var][b_]) for b_ in bs if pts[var][b_] is not None]
    no = [b_ for b_ in bs if pts[var][b_] is None]
    if yes:
        ax.plot(
            *zip(*yes), mk, color=col, ms=ms, mec="white", mew=0.5, label=lab, ls="none"
        )
    if no:
        ax.plot(
            no, [NONE_Y] * len(no), mk, mfc="white", mec=col, mew=1.0, ms=ms, ls="none"
        )
th = stats["thresholds"]
for var, col in [("nb", NB), ("pipe", PIPE)]:
    mid = 0.5 * sum(th[var])
    ax.axvline(mid, color=col, lw=0.9, ls="-." if var == "nb" else "--")
    ax.annotate(
        f"threshold {mid:.5f}",
        (mid, 6),
        xytext=(-4, 0),
        textcoords="offset points",
        rotation=90,
        ha="right",
        va="center",
        color=INK,
        fontsize=7.5,
    )
ax.set_yscale("log")
ax.set_ylim(0.3, 200)
ax.set_xlim(2.987, 3.033)
ax.axhline(20, color=MUTED, lw=0.7, ls=":")
ax.text(3.0325, 23, "EW capture", ha="right", color=MUTED, fontsize=7.5)
ax.text(3.0325, 1.25, "QCD capture", ha="right", color=MUTED, fontsize=7.5)
ax.text(
    3.0325,
    NONE_Y * 1.12,
    "open: no $T_J$ minimum before $\\phi = 0.1\\,M_P$",
    ha="right",
    va="bottom",
    color=MUTED,
    fontsize=7.5,
)
ax.set_xlabel(r"$\beta$")
ax.set_ylabel(r"$T_J$ at first minimum [GeV]")
ax.legend(loc="upper left", fontsize=7.5)
title(ax, r"Where the first $T_J$ minimum falls, $\phi_i = 10\,M_P$")
fig.savefig(os.path.join(FIG, "threshold.pdf"))
plt.close(fig)

# ------------------------------------------------------------------ Figure 6: phi_i = 100
fig, (a, b) = plt.subplots(1, 2, figsize=(W, 2.5), constrained_layout=True)
for var, col, ls, lab in [
    ("nb", NB, "-", "notebook $\\Sigma$"),
    ("pipe", PIPE, "--", "pipeline $\\Sigma$"),
]:
    d = hist(f"phi100_b3.005_{var}")
    a.plot(d.N, np.exp(d.lnTJ), color=col, ls=ls, label=lab)
    b.plot(d.N, d.phi, color=col, ls=ls, label=lab)
a.set_yscale("log")
a.set_xlabel("$N$")
a.set_ylabel(r"$T_J$ [GeV]")
a.axhline(53.2, color=MUTED, lw=0.7, ls=":")
a.legend(loc="lower left", fontsize=7.5)
title(a, r"(a) $T_J$, $\beta=3.005$, $\phi_i=100\,M_P$")
b.set_xlabel("$N$")
b.set_ylabel(r"$\phi / M_P$")
title(b, r"(b) $\phi$")
fig.savefig(os.path.join(FIG, "phi100.pdf"))
plt.close(fig)

print("figures written to", os.path.abspath(FIG))
