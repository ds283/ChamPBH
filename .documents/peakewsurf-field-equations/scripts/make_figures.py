"""
Draw the report's figures from data/ into figures/. Run from anywhere:
    ./venv/bin/python .documents/peakewsurf-field-equations/scripts/make_figures.py
The style is that of the companion report .documents/peakewsurf-kicking-function.
"""

import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "..", "data")
HIST = os.path.join(DATA, "histories")
FIG = os.path.join(HERE, "..", "figures")
os.makedirs(FIG, exist_ok=True)

# categorical slots 1-4 of the reference palette, in fixed order; line style is the secondary encoding
C1, C2, C3, C4 = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"
INK, MUTED, GRID = "#1f1f1e", "#6b6a63", "#e4e3dc"
W = 6.3

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


def title(ax, s):
    ax.set_title(s, loc="left", color=INK)


def hist(name):
    return pd.read_csv(os.path.join(HIST, name))


# ---------------------------------------------------------------- Fig. 1: agreement
fig = plt.figure(figsize=(W, 8.0), layout="constrained")
gs = fig.add_gridspec(3, 1, height_ratios=[1.1, 1, 1])
a, b, c = (fig.add_subplot(gs[i]) for i in range(3))

for beta, col in (("2.99500", C1), ("3.01940", C2), ("3.01950", C3), ("3.02500", C4)):
    n = hist(f"scan_b{beta}_nb_common.csv")
    p = hist(f"scan_b{beta}_pipe_pipe.csv")
    a.plot(n.N, np.exp(n.lnTJ), color=col, label=f"β = {float(beta):.4f}")
    a.plot(p.N, np.exp(p.lnTJ), color=INK, ls=(0, (2, 3)), lw=0.9)
a.plot([], [], color=INK, ls=(0, (2, 3)), lw=0.9, label="pipeline (all four)")
a.set_yscale("log")
a.set_xlim(0, 40)
a.set_ylim(1, 3e4)
a.set_xlabel("N")
a.set_ylabel("$T_J$ [GeV]")
a.legend(loc="upper right", ncol=1, fontsize=7.5)
title(
    a,
    "(a) $T_J(N)$ with the pipeline's thermodynamics: notebook (colour) and pipeline (dashed)",
)

r = pd.read_csv(os.path.join(HIST, "residuals_b3.01500.csv"))
series = [
    ("pipeSig_vs_pipe", C1, "-", "common Σ only (first report's runs)"),
    ("nb_vs_nbexact", C2, "-", "notebook's thermodynamics in both"),
    ("common_vs_pipe", C3, "-", "pipeline's thermodynamics in both"),
    ("common_vs_pipe_rtol1e-10", C4, "-", "  … with pipeline rtol $10^{-10}$"),
    ("pipe_rtol1e-8_vs_1e-10", MUTED, ":", "pipeline rtol $10^{-8}$ vs $10^{-10}$"),
]
for k, col, ls, lab in series:
    y = np.abs(r[k].values)
    b.plot(
        r.N,
        np.where(y > 0, y, np.nan),
        color=col,
        ls=ls,
        lw=1.2 if ls == "-" else 1.4,
        label=lab,
    )
b.set_yscale("log")
b.set_ylim(1e-11, 1e-2)
b.set_xlim(0, 40)
b.set_xlabel("N")
b.set_ylabel(r"$|\Delta \ln T_J|$")
b.legend(loc="upper center", bbox_to_anchor=(0.5, -0.2), fontsize=7, ncol=3)
title(b, "(b) Notebook minus pipeline along the β = 3.015 history")

m = pd.read_csv(os.path.join(DATA, "max_residuals.csv"))
x = np.arange(len(m))
for k, col, ls, lab in series:
    if ls != "-":
        c.plot(x, m[k], "o", mfc="none", mec=col, ms=9, mew=1.2, label=lab)
    else:
        c.plot(x, m[k], "o", color=col, ms=5, label=lab)
c.set_yscale("log")
c.set_ylim(1e-10, 1e-2)
c.set_xticks(x, [f"{v:.4f}" for v in m.beta], rotation=45, fontsize=7)
c.set_xlabel("β")
c.set_ylabel(r"max $|\Delta \ln T_J|$")
title(
    c, "(c) Largest difference over the whole history, every coupling (colours as in b)"
)
fig.savefig(os.path.join(FIG, "agreement.pdf"))
plt.close(fig)

# ---------------------------------------------------------------- Fig. 2: phi_i and machine overflow
fig, (a, b) = plt.subplots(2, 1, figsize=(W, 5.6), layout="constrained", sharex=True)
for tag, col, ls, lab in (
    ("phi10_nb", C1, "-", r"$\phi_i = 10$"),
    ("phi72_nb", C2, (0, (5, 2)), r"$\phi_i = 72$"),
    ("phi75_nb", C3, "-", r"$\phi_i = 75$"),
    ("phi100_nb", C4, "-", r"$\phi_i = 100$, notebook as written"),
    ("phi100_nbLog", INK, (0, (5, 2)), r"$\phi_i = 100$, log-space $\rho$"),
    ("phi100_pipe_nbexact", MUTED, ":", r"$\phi_i = 100$, pipeline"),
):
    p = os.path.join(HIST, f"phisweep_b3.00500_{tag}.csv")
    if not os.path.exists(p):
        continue
    h = pd.read_csv(p)
    a.plot(
        h.N, np.exp(h.lnTJ), color=col, ls=ls, lw=1.2 if ls == "-" else 1.5, label=lab
    )
a.set_yscale("log")
a.set_ylim(40, 150)
a.set_yticks([40, 50, 60, 80, 100, 150], ["40", "50", "60", "80", "100", "150"])
a.minorticks_off()
a.set_ylabel("$T_J$ [GeV]")
a.legend(loc="upper right", fontsize=7, ncol=2)
title(a, "(a) Notebook's thermodynamics, β = 3.005")

for tag, col, ls, lab in (
    ("phi100_common", C4, "-", r"$\phi_i = 100$, notebook as written"),
    ("phi100_commonLog", INK, (0, (5, 2)), r"$\phi_i = 100$, log-space $\rho$"),
    ("phi100_pipe_pipe", MUTED, ":", r"$\phi_i = 100$, pipeline"),
):
    p = os.path.join(HIST, f"phisweep_b3.00500_{tag}.csv")
    if not os.path.exists(p):
        continue
    h = pd.read_csv(p)
    b.plot(
        h.N, np.exp(h.lnTJ), color=col, ls=ls, lw=1.2 if ls == "-" else 1.5, label=lab
    )
b.set_yscale("log")
b.set_ylim(1, 300)
b.set_yticks(
    [1, 2, 5, 10, 20, 50, 100, 200], ["1", "2", "5", "10", "20", "50", "100", "200"]
)
b.minorticks_off()
b.set_xlim(0, 150)
b.set_xlabel("N")
b.set_ylabel("$T_J$ [GeV]")
b.legend(loc="upper right", fontsize=7)
title(b, "(b) Pipeline's thermodynamics, β = 3.005")
# where the overflowed phi_i = 100 history falls back below beta phi = 219.8, i.e. phi = 73.1 Mp
h = pd.read_csv(os.path.join(HIST, "phisweep_b3.00500_phi100_nbLog.csv"))
N73 = float(np.interp(-219.8 / 3.005, -h.phi.values, h.N.values))
for ax in (a, b):
    ax.axvline(N73, color=MUTED, lw=0.8, ls=":")
a.text(N73 + 1, 42, r"$\beta\phi = 220$", color=MUTED, fontsize=7.5)
fig.savefig(os.path.join(FIG, "overflow.pdf"))
plt.close(fig)

# ---------------------------------------------------------------- Fig. 3: the matter fraction to today
fm = pd.read_csv(os.path.join(DATA, "fm_late.csv"))
fig, ax = plt.subplots(figsize=(W, 2.9), layout="constrained")
ax.plot(fm.T_GeV, fm.ratio, color=C1, label=r"$f_m^{\rm nb}/f_m^{\rm pipe}$")
ax.plot(
    fm.T_GeV,
    fm.w_factor,
    color=C2,
    ls=(0, (5, 2)),
    label=r"$(1+w)$ factor, $(16/9)/(1+w)^2$",
)
ax.plot(
    fm.T_GeV,
    fm.rest,
    color=C3,
    ls=(0, (1.5, 1.5)),
    lw=1.8,
    label=r"the rest ($g_s \neq g_\rho$, $\rho_m/s$)",
)
ax.axhline(1.0, color=MUTED, lw=0.8)
# where the runs of this report end, and matter-radiation equality
ax.axvspan(1.6, 2e4, color=GRID, alpha=0.6, lw=0)
ax.text(2.2, 1.19, "runs here", color=MUTED, fontsize=7.5)
f = fm.fm_pipe.values
T_eq = np.exp(np.interp(0.0, np.log(f[::-1]), np.log(fm.T_GeV.values[::-1])))
ax.axvline(T_eq, color=MUTED, lw=0.8, ls=":")
ax.text(T_eq * 1.6, 1.19, "equality", color=MUTED, fontsize=7.5)
ax.set_xscale("log")
ax.set_xlim(2.3e-13, 2e4)
ax.set_ylim(0.82, 1.22)
ax.set_xlabel(r"$T_J$ [GeV]")
ax.set_ylabel("ratio")
ax.legend(loc="lower right", fontsize=7.5)
fig.savefig(os.path.join(FIG, "fm_late.pdf"))
plt.close(fig)
print("figures written to", os.path.abspath(FIG))
