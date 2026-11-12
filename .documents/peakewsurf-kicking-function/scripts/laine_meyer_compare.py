"""
Addendum of 2026-11-12: compare the Laine & Meyer equation of state (arXiv:1503.04935, the eos15
data file) with the notebook's w table and the pipeline's, build the merged table (the notebook's
rows with the 10 GeV points supplied on 2026-11-12), and draw the addendum's two figures.

Run from the repository root:
    ./venv/bin/python .documents/peakewsurf-kicking-function/scripts/laine_meyer_compare.py

Reads data/laine_meyer_eos15_standardmodel.dat, data/notebook_w_table.csv,
data/notebook_w_points_2026-11-12.csv and CosmologyModels/GenericEOS/Xav_EOS_data.csv.
Writes data/merged_w_table.csv, data/laine_meyer_stats.json, figures/lm_overview.pdf and
figures/lm_merged.pdf. Sigma = 1 - 3w throughout. The merged table is splined exactly as
Xav_EOS_spline splines its table: make_interp_spline (cubic, not-a-knot) in ln T, with w = 1/3
at and beyond the table's ends.
"""

import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from scipy.interpolate import make_interp_spline

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "..", "data")
FIG = os.path.join(HERE, "..", "figures")
PIPE_CSV = os.path.join(
    HERE, "..", "..", "..", "CosmologyModels", "GenericEOS", "Xav_EOS_data.csv"
)

# categorical slots 1-3 of the reference palette, as make_figures.py: notebook blue, pipeline
# orange; Laine & Meyer takes the third slot. Line style is the secondary encoding.
NB, PIPE, LM = "#2a78d6", "#eb6834", "#1baf7a"
INK, MUTED, GRID, GAP = "#1f1f1e", "#6b6a63", "#e4e3dc", "#ecebe6"
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

# Laine & Meyer's three regimes above 50 GeV (their eos15 page): T < 110 GeV from
# hep-ph/0603048, 125-270 GeV from 1503.04935, T > 290 GeV from hep-ph/0510375 as modified
REGIMES = [(0.0, 110.5), (125.0, 270.0), (291.0, np.inf)]
GAPS = [(109.95, 125.33), (269.67, 292.0)]


# (degrees of freedom, vacuum mass in GeV, +1 fermion / -1 boson): photon, gluons, W, Z, H, t, b, c,
# s, d, u, tau, mu, e, neutrinos. No interactions, no thermal masses.
SPECIES = [
    (2, 0.0, -1),
    (16, 0.0, -1),
    (6, 80.37, -1),
    (3, 91.19, -1),
    (1, 125.1, -1),
    (12, 172.7, 1),
    (12, 4.18, 1),
    (12, 1.27, 1),
    (12, 0.093, 1),
    (12, 0.0047, 1),
    (12, 0.0022, 1),
    (4, 1.777, 1),
    (4, 0.1057, 1),
    (4, 0.000511, 1),
    (6, 0.0, 1),
]


def ideal_gas_sigma(T):
    from scipy.integrate import quad

    rho = p = 0.0
    for g, m, s in SPECIES:
        x = m / T
        e = lambda k: np.sqrt(k * k + x * x)
        rho += g * quad(lambda k: k * k * e(k) / (np.exp(e(k)) + s), 0, 60)[0]
        p += g * quad(lambda k: k**4 / e(k) / 3 / (np.exp(e(k)) + s), 0, 60)[0]
    return float(1.0 - 3.0 * p / rho)


def load():
    lm = np.loadtxt(
        os.path.join(DATA, "laine_meyer_eos15_standardmodel.dat"), comments="#"
    )
    nb = np.loadtxt(
        os.path.join(DATA, "notebook_w_table.csv"), delimiter=",", skiprows=1
    )
    new = np.loadtxt(
        os.path.join(DATA, "notebook_w_points_2026-11-12.csv"),
        delimiter=",",
        skiprows=1,
    )
    pipe = np.loadtxt(PIPE_CSV, delimiter=",", skiprows=1)
    return lm, nb, new, pipe


def main():
    lm, nb, new, pipe = load()
    T_lm, S_lm = lm[:, 0] / 1e3, 1.0 - 3.0 * lm[:, 5]
    stats = {}

    # --- the eos15 grid
    stats["lm_rows"] = int(len(T_lm))
    stats["lm_T_range_GeV"] = [float(T_lm[0]), float(T_lm[-1])]
    grid = {}
    for name, lo, hi in [
        ("50-110", 50.0, 110.5),
        ("125-270", 125.0, 270.0),
        ("292-1000", 291.0, 1000.0),
    ]:
        m = (T_lm >= lo) & (T_lm <= hi)
        dT = np.diff(T_lm[m])
        dl = np.diff(np.log10(T_lm[m]))
        grid[name] = {
            "rows": int(m.sum()),
            "dT_GeV": [float(dT.min()), float(dT.max())],
            "dlog10T": [float(dl.min()), float(dl.max())],
        }
    grid["rows_50_1000"] = int(((T_lm >= 50) & (T_lm <= 1000)).sum())
    stats["lm_grid"] = grid
    stats["lm_join_steps"] = {}
    for a, b in GAPS:
        i = int(np.searchsorted(T_lm, b))
        stats["lm_join_steps"][f"{a:g}-{b:g}"] = {
            "T": [float(T_lm[i - 1]), float(T_lm[i])],
            "Sigma": [float(S_lm[i - 1]), float(S_lm[i])],
        }
    hi_m = T_lm > 300
    sign = np.where(np.diff(np.sign(S_lm[hi_m])) != 0)[0]
    stats["lm_sign_changes_above_300_GeV"] = [
        [float(T_lm[hi_m][k]), float(T_lm[hi_m][k + 1])] for k in sign
    ]
    k = int(np.argmin(S_lm[hi_m]))
    stats["lm_min_Sigma_above_300_GeV"] = [float(T_lm[hi_m][k]), float(S_lm[hi_m][k])]

    # --- the new points against the notebook's rows (they replace 100-316 GeV)
    sp_new = make_interp_spline(np.log(new[:, 0]), new[:, 1])
    stats["new_points_vs_notebook_rows"] = [
        {
            "T": float(T),
            "dSigma_new_minus_nb": float(3.0 * (w - float(sp_new(np.log(T))))),
        }
        for T, w in nb[(nb[:, 0] >= 100) & (nb[:, 0] <= 380)]
    ]

    # --- the merged table: notebook rows below 100 GeV, the new points, the notebook's 386.7 GeV row
    merged = np.vstack([nb[nb[:, 0] < 100], new, nb[nb[:, 0] > 380]])
    with open(os.path.join(DATA, "merged_w_table.csv"), "w") as f:
        f.write("T_GeV,w\n")
        for T, w in merged:
            f.write(f"{T:.17g},{w:.17g}\n")
    stats["merged_rows"] = int(len(merged))
    stats["merged_T_range_GeV"] = [float(merged[0, 0]), float(merged[-1, 0])]

    sp = make_interp_spline(np.log(merged[:, 0]), merged[:, 1])
    dsp, d2sp = sp.derivative(), sp.derivative(2)
    Tmax = merged[-1, 0]

    def S_merged(T):
        T = np.asarray(T, dtype=float)
        x = np.log(np.minimum(T, Tmax))
        return np.where(T >= Tmax, 0.0, 1.0 - 3.0 * sp(x))

    def S_nb(T):  # the notebook's rows, linear in ln T, zero from its last row
        T = np.asarray(T, dtype=float)
        v = 1.0 - 3.0 * np.interp(np.log(T), np.log(nb[:, 0]), nb[:, 1])
        return np.where(T >= nb[-1, 0], 0.0, v)

    def S_pipe(T):
        return 1.0 - 3.0 * np.interp(np.log(T), np.log(pipe[:, 0]), pipe[:, 1])

    def S_LM(T):
        return np.interp(np.log(T), np.log(T_lm), S_lm)

    x = np.linspace(np.log(40.0), np.log(Tmax), 4001)
    dS = -3.0 * dsp(x)
    stats["merged_spline"] = {
        "dSigma_dlnT_sign_changes_40_to_end": [
            float(np.exp(x[i])) for i in np.where(np.diff(np.sign(dS)) != 0)[0]
        ],
        "min_Sigma_40_to_end": float((1.0 - 3.0 * sp(x)).min()),
        "dSigma_dlnT_at_end": float(-3.0 * dsp(np.log(Tmax))),
        "at_100_GeV": {
            "dSigma_dlnT": float(-3.0 * dsp(np.log(100.0))),
            "d2Sigma_dlnT2": float(-3.0 * d2sp(np.log(100.0))),
        },
    }

    # --- the merged table against eos15 at the new points
    rows = []
    for T, w in new:
        gap = any(a < T < b for a, b in GAPS)
        rows.append(
            {
                "T": float(T),
                "Sigma_new": float(1 - 3 * w),
                "Sigma_LM": float(S_LM(T)),
                "Sigma_pipe": float(S_pipe(T)),
                "LM_in_gap": bool(gap),
            }
        )
    stats["at_new_points"] = rows
    # extremes of the difference on eos15's own rows, regime by regime
    diff = {}
    for name, (a, b) in zip(
        ["50-110", "125-270", "292-1000"], [(50, 110.5), (125, 270), (291, 1000)]
    ):
        m = (T_lm >= a) & (T_lm <= b)
        d = S_merged(T_lm[m]) - S_lm[m]
        diff[name] = {
            "min": [float(T_lm[m][d.argmin()]), float(d.min())],
            "max": [float(T_lm[m][d.argmax()]), float(d.max())],
        }
    stats["merged_minus_LM_on_LM_rows"] = diff

    # --- the notebook's own rows against eos15 and the pipeline
    stats["at_notebook_rows"] = [
        {
            "T": float(T),
            "Sigma_nb": float(1 - 3 * w),
            "Sigma_LM": float(S_LM(T)),
            "Sigma_pipe": float(S_pipe(T)),
        }
        for T, w in nb
        if T >= 1e-3
    ]

    # --- integrated kick
    integ = {}
    for a, b in [
        (30, 100),
        (100, 160),
        (160, 300),
        (100, 300),
        (300, 1000),
        (1000, 25000),
        (30, 25000),
    ]:
        xx = np.linspace(np.log(a), np.log(b), 20001)
        TT = np.exp(xx)
        integ[f"{a}-{b}"] = {
            n: float(np.trapezoid(f(TT), xx))
            for n, f in [
                ("LM", S_LM),
                ("merged", S_merged),
                ("notebook", S_nb),
                ("pipeline", S_pipe),
            ]
        }
    stats["int_Sigma_dlnT"] = integ

    # --- a free gas of Standard Model particles at vacuum masses, on the pipeline's rows above 10 GeV
    ideal = []
    for T, w in pipe[pipe[:, 0] >= 10.0]:
        s_id = ideal_gas_sigma(T)
        ideal.append(
            {"T": float(T), "Sigma_ideal": s_id, "Sigma_pipe": float(1 - 3 * w)}
        )
    stats["ideal_gas_on_pipeline_rows"] = ideal
    rel = [abs(r["Sigma_pipe"] / r["Sigma_ideal"] - 1) for r in ideal if r["T"] >= 100]
    stats["pipeline_vs_ideal_max_rel_above_100_GeV"] = float(max(rel))
    stats["pipeline_minus_ideal_max_10_to_60_GeV"] = float(
        max(r["Sigma_pipe"] - r["Sigma_ideal"] for r in ideal if r["T"] <= 60)
    )

    with open(os.path.join(DATA, "laine_meyer_stats.json"), "w") as f:
        json.dump(stats, f, indent=1)

    # --- figure 1: the three sources, 20 GeV - 25 TeV
    fig, axes = plt.subplots(2, 1, figsize=(W, 6.4))
    for ax, (lo, hi), yl in zip(
        axes, [(20.0, 600.0), (200.0, 25000.0)], [(-0.002, 0.042), (-0.0006, 0.006)]
    ):
        for g in GAPS:
            ax.axvspan(*g, color=GAP, lw=0, zorder=0)
        for i, (a, b) in enumerate(REGIMES):
            m = (T_lm >= max(a, lo / 1.2)) & (T_lm <= min(b, hi * 1.2))
            ax.plot(
                T_lm[m],
                S_lm[m],
                color=LM,
                label="Laine & Meyer (eos15)" if i == 0 else None,
            )
        m = (nb[:, 0] >= lo / 1.6) & (nb[:, 0] <= hi)
        tn = np.append(nb[m, 0], hi * 1.2)
        ax.plot(
            tn,
            np.append(1 - 3 * nb[m, 1], 0.0),
            color=NB,
            ls=(0, (5, 2)),
            marker="o",
            ms=4,
            label="notebook rows (linear in ln T)",
        )
        m = (pipe[:, 0] >= lo / 1.2) & (pipe[:, 0] <= hi * 1.2)
        ax.plot(
            pipe[m, 0],
            1 - 3 * pipe[m, 1],
            color=PIPE,
            ls=(0, (1.5, 1.5)),
            label="pipeline table",
        )
        ax.axhline(0.0, color=MUTED, lw=0.6)
        ax.set_xscale("log")
        ax.set_xlim(lo, hi)
        ax.set_ylim(*yl)
        ax.set_xlabel(r"$T$ [GeV]")
        ax.set_ylabel(r"$\Sigma = 1 - 3w$")
    axes[0].set_title(
        "Through the crossover (shaded: gaps between Laine & Meyer's regimes)",
        loc="left",
    )
    axes[1].set_title("Above the crossover", loc="left")
    axes[0].legend(fontsize=7.5)
    fig.savefig(os.path.join(FIG, "lm_overview.pdf"))
    plt.close(fig)

    # --- figure 2: the merged table against eos15, 60-600 GeV, and the difference
    lo, hi = 60.0, 600.0
    fig, (a1, a2) = plt.subplots(
        2, 1, figsize=(W, 6.0), sharex=True, gridspec_kw={"height_ratios": [3, 2]}
    )
    for ax in (a1, a2):
        for g in GAPS:
            ax.axvspan(*g, color=GAP, lw=0, zorder=0)
        ax.axhline(0.0, color=MUTED, lw=0.6)
    for i, (a, b) in enumerate(REGIMES):
        m = (T_lm >= max(a, lo)) & (T_lm <= min(b, hi))
        a1.plot(
            T_lm[m],
            S_lm[m],
            color=LM,
            label="Laine & Meyer (eos15)" if i == 0 else None,
        )
        a2.plot(T_lm[m], S_merged(T_lm[m]) - S_lm[m], color=NB)
    TT = np.exp(np.linspace(np.log(lo), np.log(hi), 2000))
    a1.plot(
        TT,
        S_merged(TT),
        color=NB,
        ls=(0, (5, 2)),
        label="merged table, pipeline spline",
    )
    a1.plot(
        new[:, 0],
        1 - 3 * new[:, 1],
        ls="none",
        marker="o",
        ms=4.5,
        color=NB,
        mec="white",
        mew=0.8,
        label="points of 2026-11-12",
    )
    m = (nb[:, 0] >= 150) & (nb[:, 0] <= 390)
    a1.plot(
        nb[m, 0],
        1 - 3 * nb[m, 1],
        ls="none",
        marker="s",
        ms=6,
        mfc="none",
        mec=INK,
        mew=0.9,
        label="notebook rows replaced, and 386.7 GeV",
    )
    m = (pipe[:, 0] >= lo) & (pipe[:, 0] <= hi)
    a1.plot(
        pipe[m, 0],
        1 - 3 * pipe[m, 1],
        color=PIPE,
        ls=(0, (1.5, 1.5)),
        label="pipeline table",
    )
    a1.set_ylim(-0.001, 0.036)
    a1.set_ylabel(r"$\Sigma = 1 - 3w$")
    a1.legend(fontsize=7.5)
    a1.set_title("The merged table through the crossover", loc="left")
    a2.set_xscale("log")
    a2.set_xlim(lo, hi)
    a2.set_xlabel(r"$T$ [GeV]")
    a2.set_ylabel(r"$\Sigma_{\mathrm{merged}} - \Sigma_{\mathrm{LM}}$")
    a2.set_title(
        "Merged table minus Laine & Meyer, on Laine & Meyer's rows", loc="left"
    )
    fig.savefig(os.path.join(FIG, "lm_merged.pdf"))
    plt.close(fig)


if __name__ == "__main__":
    main()
