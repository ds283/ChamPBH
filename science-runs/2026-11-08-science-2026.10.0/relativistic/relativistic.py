"""
The relativistic-quanta analysis of the 2026.10.0 science run: do the quanta produced at the field's
reflections stay relativistic, and how much of the dark radiation is not covered by the test? Three
figures (PLAN.md beside this file, ruled by the user 2026-11-09):

- rel-lab: at the laboratory mass M = 4e-31 M_P, against beta, under the one-loop drain law with a
  band over A = 0.05-3, A = 1 black and A = 3, 5, 10 green, in Figure 6's columns and colours: the
  largest matter-like share of rho_R (r_chi E_field, the test), the matter-like excess today, the
  worst birth and reflection margins ln(p/m) over all generations, the share of K that fails at a
  wall, and the parking share, which has no spectrum. The fixed law is drawn only where the store
  has something for it: its weighted failing share (the user's ruling of 2026-11-09).
- rel-M: A = 1 at every M of the store, one line per M, M_crit's two in a contrasting pair.
- rel-history: three histories at laboratory M (A = 0.05 at the beta with the largest failing share,
  A = 1 and 10 at beta = 2), E(N) and the margins of each generation.

    ./venv/bin/python science-runs/2026-11-08-science-2026.10.0/relativistic/relativistic.py

Reads ../fig6/inputs/histories.csv (the run's plot_by_beta.py export of the main store), the copies
of tools/quanta_measures.py's quanta_histories*.csv in inputs/, the three histories' bounces.csv,
quanta_samples.csv and quanta_bounces.csv in inputs/samples/, and the store's sidecar in
../registry/stores/. Writes rel-lab, rel-M and rel-history (.pdf, .png) and
relativistic.provenance.json beside this file. Needs no Ray and no store.

Margins are ln(p/m) (the integrator warns below ln 10). E is the matter-like excess of rho_chi over
its radiation value, sum_g Delta K_g G(m a / Q_g) / K (ComputeTargets/quanta_spectrum.py), at the
field's mass or the tracker's; r_chi E is that excess as a share of rho_R.
"""

import hashlib
import json
import subprocess
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

HERE = Path(__file__).resolve().parent
HISTORIES = HERE.parent / "fig6" / "inputs" / "histories.csv"
QUANTA = {
    1: HERE / "inputs" / "quanta_histories.csv",
    0: HERE / "inputs" / "quanta_histories-assoc0.csv",
    10: HERE / "inputs" / "quanta_histories-assoc10.csv",
}
SAMPLES = HERE / "inputs" / "samples"
SIDECAR = HERE.parent / "registry" / "stores" / "science-2026.10.0.manifest.json"

M_LAB = 4e-31
ONE_LOOP = "one-loop-measured-v1"
A_LINE = 1.0
A_BAND_MAX = 3.0
LONG_DASH = (0, (8, 3))
A_LARGE = {  # as Figure 6 of this run
    3.0: ("#71d926", LONG_DASH),
    5.0: ("#30c26d", LONG_DASH),
    10.0: ("#2d6619", LONG_DASH),
}
FIXED = {0.1: ("#2662d9", "-."), 0.01: ("#539fc6", "-.")}
BAND = "#d1495b"
BETA_QCD = 1.1074
EW_BRACKET = (3.000, 3.005)
T_WALL_GEV = 1e-2
LN10 = np.log(10.0)
SHARE_FLOOR = 1e-6  # a zero share is drawn on the bottom axis

COLUMNS = [  # Figure 6's: (x range, scale, width ratio)
    ((0.45, 1.05), "linear", 0.55),
    ((1.05, 1.52), "linear", 2.2),
    ((1.52, 3.07), "linear", 1.25),
    ((3.07, 22.0), "log", 0.8),
]

# rel-lab's rows: (column, axis label, scale, the fixed law's column or None)
LAB_ROWS = [
    (
        "r_chi_E_field_max",
        r"max $r_\chi E_{\rm field}$" "\n" r"(share of $\rho_R$)",
        "log",
        None,
    ),
    (
        "E_field_today",
        r"$E_{\rm field}$ today" "\n" r"(share of $\rho_\chi$)",
        "log",
        None,
    ),
    ("all_worst_birth_margin", "worst birth margin\n" r"$\ln(p/m)$", "linear", None),
    (
        "all_worst_reflection_margin",
        "worst reflection\n" r"margin $\ln(p/m)$",
        "linear",
        None,
    ),
    (
        "all_reflection_failing_share",
        "share of $K$ failing\nat a wall",
        "share",
        "reflection_failing_share",
    ),
    ("parking_share", "parking share of $K$\n(no spectrum)", "share", None),
]
# rel-M's rows
M_ROWS = [LAB_ROWS[0], LAB_ROWS[3], LAB_ROWS[4], LAB_ROWS[5]]
M_CRIT = {1e-15: "#d1495b", 1e-12: "#7a1f2b"}

HISTORY_PICKS = [  # (A, beta, why), at laboratory M
    (0.05, 1.0775, "the largest share of K failing at a wall"),
    (1.0, 2.0, "a typical surfing history"),
    (10.0, 2.0, "the largest A"),
]
M_LAB_DIR = "M=0.00097413eV_n=1"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(*args) -> str:
    return subprocess.run(
        ["git", *args], cwd=HERE, capture_output=True, text=True, check=True
    ).stdout.strip()


def key(df: pd.DataFrame) -> pd.Series:
    """A join key on (drain model, A or alpha, M, beta), robust to how each file prints its floats."""
    return (
        df.drain_model.astype(str)
        + "|"
        + df.alpha.astype(float).map(lambda x: f"{x:.9g}")
        + "|"
        + df.M_Mp.astype(float).map(lambda x: f"{x:.9g}")
        + "|"
        + df.beta.astype(float).map(lambda x: f"{x:.9g}")
    )


def load():
    H = pd.read_csv(HISTORIES, low_memory=False)
    H = H[~H.failure_reasons.fillna("").str.startswith("no ScalarHistory")].copy()
    H["stored"] = (
        H.normalization.notna()
    )  # a history was built (BBN may still have failed)
    H["key"] = key(H)
    Q = {}
    for c, path in QUANTA.items():
        q = pd.read_csv(path)
        q["key"] = key(q)
        Q[c] = q.set_index("key")
    q1 = Q[1].drop(
        columns=[
            "spectrum_model",
            "assoc_scale",
            "drain_model",
            "alpha",
            "M_Mp",
            "n",
            "beta",
        ]
        + [c for c in ("parking_share",) if c in Q[1].columns]
    )
    H = H.join(q1, on="key")
    H["measured"] = H.generations.notna()
    return H, Q


def wall_edge(H: pd.DataFrame) -> float:
    td = H[H.T_deliver_GeV.notna() & np.isclose(H.M_Mp, M_LAB, rtol=1e-9, atol=0.0)]
    td = td.groupby("beta").T_deliver_GeV.first()
    return 0.5 * (td[td < T_WALL_GEV].index.max() + td[td >= T_WALL_GEV].index.min())


def _runs(betas, split=None):
    """As fig6.py: runs of consecutive grid points, split at gaps and at the wall's edge."""
    b = np.sort(betas)
    if len(b) < 2:
        return [(b[0], b[0])] if len(b) else []
    steps = np.diff(b)
    out, start = [], b[0]
    for k, d in enumerate(steps):
        local = np.median(steps[max(0, k - 3) : k + 4])
        crosses = split is not None and b[k] < split <= b[k + 1]
        if crosses or (d > 3 * local and d > 0.05):
            out.append((start, b[k]))
            start = b[k + 1]
    out.append((start, b[-1]))
    return out


def values(s: pd.Series, scale: str) -> pd.Series:
    """What a row draws: a share's zero on the floor; on a log row, non-positive values dropped."""
    if scale == "share":
        return s.clip(lower=SHARE_FLOOR)
    if scale == "log":
        return s.where(s > 0)
    return s


def limits(series, scale):
    v = pd.concat(series).astype(float)
    v = v[np.isfinite(v)]
    if scale == "share":
        return (0.5 * SHARE_FLOOR, 2.0)
    if scale == "log":
        v = v[v > 0]
        return (
            10 ** np.floor(np.log10(v.min())),
            10 ** np.ceil(np.log10(v.max()) + 0.3),
        )
    lo, hi = min(v.min(), -1.0), max(v.max(), LN10 + 1)
    pad = 0.06 * (hi - lo)
    return (lo - pad, hi + pad)


def decorate(A, j, xlim, xscale, b_wall, scale, nrows, i, ylabel, xlabel=r"$\beta$"):
    A.set_xscale(xscale)
    A.set_xlim(*xlim)
    A.axvspan(0.0, b_wall, color="#f3e6d0", zorder=0, lw=0)
    A.axvspan(sum(EW_BRACKET) / 2, 100, color="0.93", zorder=0, lw=0)
    A.axvline(BETA_QCD, color="0.35", ls="--", lw=0.8, zorder=1)
    A.axvline(sum(EW_BRACKET) / 2, color="0.35", ls="-.", lw=0.8, zorder=1)
    if scale == "linear":
        A.axhline(0.0, color="0.3", lw=0.7, zorder=1)
        A.axhline(LN10, color="0.3", lw=0.7, ls=":", zorder=1)
    if j == 0:
        A.set_ylabel(ylabel, fontsize=8)
    else:
        A.tick_params(labelleft=False)
        A.spines["left"].set_linestyle((0, (2, 2)))
    if j < len(COLUMNS) - 1:
        A.spines["right"].set_visible(False)
    if i < nrows - 1:
        A.tick_params(labelbottom=False)
    else:
        A.set_xlabel(xlabel if j == 1 else "")
    if xscale == "log":
        A.set_xticks([5, 10, 20])
        A.set_xticklabels(["5", "10", "20"])
        A.xaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
    if j == 0:
        A.set_xticks([0.5, 0.75, 1.0])
    if j == 1:
        A.set_xticks([1.1, 1.2, 1.3, 1.4, 1.5])
    if j == 2:
        A.set_xticks([2.0, 2.5, 3.0])
    A.tick_params(labelsize=7)


def line(A, s: pd.DataFrame, col, scale, ylim, b_wall, **kw):
    s = s.set_index("beta").sort_index()
    y = values(s[col], scale)
    for start, stop in _runs(s.index.values, b_wall):
        seg = y.loc[start:stop]
        if scale == "linear":
            seg = seg.clip(*ylim)
        A.plot(seg.index, seg.values, **kw)


def regions_legend(b_wall):
    return [
        Patch(
            color="#f3e6d0",
            label=rf"not at the wall before BBN ($\beta < {b_wall:.4g}$)",
        ),
        Patch(
            color="0.93",
            label=rf"surfs the electroweak kick ($\beta > {EW_BRACKET[0]:.3f}$)",
        ),
        Line2D(
            [],
            [],
            color="0.35",
            ls="--",
            lw=0.8,
            label=rf"analytic QCD threshold $\beta = {BETA_QCD}$",
        ),
        Line2D(
            [],
            [],
            color="0.3",
            lw=0.7,
            ls=":",
            label=r"margin $= \ln 10$ (the integrator's warning)",
        ),
    ]


def rel_lab(H, b_wall):
    S = H[np.isclose(H.M_Mp, M_LAB, rtol=1e-9, atol=0.0)]
    L = S[(S.drain_model == ONE_LOOP) & S.stored]
    F = S[(S.drain_model == "fixed") & S.stored]
    As = sorted(L.alpha.unique())
    A_band = [a for a in As if a <= A_BAND_MAX]

    fig = plt.figure(figsize=(9.0, 13.0))
    gs = fig.add_gridspec(
        len(LAB_ROWS),
        len(COLUMNS),
        width_ratios=[c[2] for c in COLUMNS],
        wspace=0.06,
        hspace=0.1,
    )
    for i, (col, ylabel, scale, fixed_col) in enumerate(LAB_ROWS):
        drawn = L[L[col].notna()]
        series = [values(drawn[col], scale)]
        if fixed_col:
            series.append(values(F[fixed_col].dropna(), scale))
        ylim = limits(series, scale)
        # the band: at each beta where every A of the band has a value
        Bd = drawn[drawn.alpha <= A_BAND_MAX]
        n = Bd.groupby("beta").alpha.nunique()
        Bd = Bd[Bd.beta.isin(n[n == len(A_band)].index)]
        g = Bd.assign(v=values(Bd[col], scale)).groupby("beta").v
        B = pd.DataFrame({"lo": g.min(), "hi": g.max()})
        for j, (xlim, xscale, _) in enumerate(COLUMNS):
            A = fig.add_subplot(gs[i, j])
            A.set_yscale("linear" if scale == "linear" else "log")
            A.set_ylim(*ylim)
            decorate(A, j, xlim, xscale, b_wall, scale, len(LAB_ROWS), i, ylabel)
            for start, stop in _runs(B.index.values, b_wall):
                seg = B.loc[start:stop]
                A.fill_between(
                    seg.index, seg.lo, seg.hi, color=BAND, alpha=0.18, lw=0, zorder=2
                )
                A.plot(seg.index, seg.lo, color=BAND, lw=0.6, zorder=2)
                A.plot(seg.index, seg.hi, color=BAND, lw=0.6, zorder=2)
            line(
                A,
                drawn[np.isclose(drawn.alpha, A_LINE)],
                col,
                scale,
                ylim,
                b_wall,
                color="black",
                lw=0.9,
                zorder=4,
            )
            for a, (colour, ls) in A_LARGE.items():
                line(
                    A,
                    drawn[np.isclose(drawn.alpha, a)],
                    col,
                    scale,
                    ylim,
                    b_wall,
                    color=colour,
                    ls=ls,
                    lw=1.0,
                    zorder=4,
                )
            if fixed_col:
                for alpha, (colour, ls) in FIXED.items():
                    s = F[np.isclose(F.alpha, alpha) & F[fixed_col].notna()]
                    line(
                        A,
                        s,
                        fixed_col,
                        scale,
                        ylim,
                        b_wall,
                        color=colour,
                        ls=ls,
                        lw=1.0,
                        zorder=4,
                    )

    handles = [
        Patch(
            color=BAND,
            alpha=0.35,
            label=r"one-loop law: range over $A$ = "
            + ", ".join(f"{a:g}" for a in A_band),
        ),
        Line2D([], [], color="black", lw=0.9, label=rf"one-loop law, $A = {A_LINE:g}$"),
    ]
    handles += [
        Line2D([], [], color=c, ls=ls, lw=1.0, label=rf"one-loop law, $A = {a:g}$")
        for a, (c, ls) in A_LARGE.items()
    ]
    handles += [
        Line2D(
            [],
            [],
            color=c,
            ls=ls,
            lw=1.0,
            label=rf"fixed law, $\alpha = {a:g}$ (weighted generations only; no other measure)",
        )
        for a, (c, ls) in FIXED.items()
    ]
    handles += regions_legend(b_wall)
    fig.legend(
        handles=handles,
        loc="lower center",
        ncol=2,
        fontsize=7,
        frameon=False,
        bbox_to_anchor=(0.5, 0.0),
    )
    fig.suptitle(
        rf"$M = 4\times10^{{-31}}\,M_P$, $\phi_* = 5\,M_P$: do the quanta stay relativistic? "
        rf"(spectrum U4-U17-v1, $c_a = 1$; a zero share on the bottom axis)",
        fontsize=9.5,
        y=0.995,
    )
    fig.subplots_adjust(bottom=0.12, top=0.965, left=0.11, right=0.99)
    return fig


def rel_M(H, b_wall):
    L = H[(H.drain_model == ONE_LOOP) & H.stored & np.isclose(H.alpha, A_LINE)]
    Ms = sorted(L.M_Mp.unique())
    low = [m for m in Ms if m < 1e-20]
    cmap = plt.get_cmap("viridis")
    lo_norm = matplotlib.colors.LogNorm(vmin=min(low), vmax=max(low))
    colour = {
        m: cmap(0.92 * (1 - lo_norm(m)))
        for m in low
        if not np.isclose(m, M_LAB, rtol=1e-9, atol=0.0)
    }
    colour.update(
        {m: "black" for m in low if np.isclose(m, M_LAB, rtol=1e-9, atol=0.0)}
    )  # as A = 1 in rel-lab
    colour.update({m: c for m, c in M_CRIT.items() if m in Ms})
    kcheck = L[~L.measured]  # stored, but the K check failed: the tool wrote nothing

    fig = plt.figure(figsize=(9.0, 9.4))
    gs = fig.add_gridspec(
        len(M_ROWS),
        len(COLUMNS),
        width_ratios=[c[2] for c in COLUMNS],
        wspace=0.06,
        hspace=0.1,
    )
    for i, (col, ylabel, scale, _) in enumerate(M_ROWS):
        drawn = L[L[col].notna()]
        ylim = limits([values(drawn[col], scale)], scale)
        for j, (xlim, xscale, _) in enumerate(COLUMNS):
            A = fig.add_subplot(gs[i, j])
            A.set_yscale("linear" if scale == "linear" else "log")
            A.set_ylim(*ylim)
            decorate(A, j, xlim, xscale, b_wall, scale, len(M_ROWS), i, ylabel)
            for m in Ms:
                s = drawn[np.isclose(drawn.M_Mp, m, rtol=1e-9, atol=0.0)]
                dense = np.isclose(m, M_LAB, rtol=1e-9, atol=0.0)
                line(
                    A,
                    s,
                    col,
                    scale,
                    ylim,
                    None if m in M_CRIT else b_wall,
                    color=colour[m],
                    lw=0.8 if dense else 1.0,
                    marker=None if dense else "o",
                    ms=3.2 if m in M_CRIT else 2.2,
                    zorder=4 if m in M_CRIT else 3,
                )
            if i == 0 and len(kcheck):
                inside = (kcheck.beta >= xlim[0]) & (kcheck.beta <= xlim[1])
                A.scatter(
                    kcheck.beta[inside],
                    np.full(inside.sum(), 1.6 * ylim[0]),
                    marker="x",
                    s=22,
                    color=M_CRIT[1e-12],
                    clip_on=False,
                    zorder=5,
                )
    handles = [
        Line2D(
            [],
            [],
            color=colour[m],
            lw=1.0,
            marker=None if np.isclose(m, M_LAB, rtol=1e-9, atol=0.0) else "o",
            ms=2.2,
            label=rf"$M = {m:.0e}\,M_P$".replace("e-", r"\times10^{-").replace(
                r"\,M_P", r"}\,M_P"
            )
            + (" ($M_{\\rm crit}$ block)" if m in M_CRIT else ""),
        )
        for m in Ms
    ]
    handles += [
        Line2D(
            [],
            [],
            marker="x",
            color=M_CRIT[1e-12],
            ls="",
            label=r"K check failed ($10^{-12}$; nothing measured)",
        ),
    ]
    handles += regions_legend(b_wall)
    fig.legend(
        handles=handles,
        loc="lower center",
        ncol=3,
        fontsize=7,
        frameon=False,
        bbox_to_anchor=(0.5, 0.0),
    )
    fig.suptitle(
        rf"One-loop law at $A = 1$, every $M$ of the store (spectrum U4-U17-v1, $c_a = 1$; a zero share on the bottom axis)",
        fontsize=9.5,
        y=0.995,
    )
    fig.subplots_adjust(bottom=0.16, top=0.955, left=0.11, right=0.99)
    return fig


def rel_history(H):
    fig, axes = plt.subplots(2, len(HISTORY_PICKS), figsize=(11.0, 6.6), sharex="col")
    out = {}
    for j, (a, beta, why) in enumerate(HISTORY_PICKS):
        d = SAMPLES / f"A={a:g}" / f"beta={beta:g}" / M_LAB_DIR
        q = pd.read_csv(d / "quanta_samples.csv")
        b = pd.read_csv(d / "bounces.csv")
        b = b[b.source == "candidate"]
        N_end = float((q.raw_N + q.log_one_plus_z).median())
        x = np.exp(q.log_one_plus_z)  # 1 + z_E
        row = H[
            (H.drain_model == ONE_LOOP)
            & np.isclose(H.alpha, a)
            & np.isclose(H.M_Mp, M_LAB, rtol=1e-9, atol=0.0)
            & np.isclose(H.beta, beta)
        ].iloc[0]

        A = axes[0, j]
        A.set_title(
            rf"$A = {a:g}$, $\beta = {beta:g}$: {why}"
            "\n"
            rf"{int(row.generations)} generations, parking share {row.parking_share:.2g}",
            fontsize=8,
        )
        for c, colr, lab in (
            ("r_chi_E_field", "black", r"$r_\chi E_{\rm field}$"),
            ("r_chi_E_tracker", "#2d6619", r"$r_\chi E_{\rm tracker}$"),
        ):
            m = q[c] > 0
            A.plot(x[m], q[c][m], color=colr, lw=0.8, label=lab)
        A.set_yscale("log")
        A = axes[1, j]
        xb = np.exp(N_end - b.N_turn)
        size = 4 + 60 * np.sqrt(b.share / b.share.max())
        A.scatter(
            xb,
            b.birth_margin,
            s=size,
            facecolor="none",
            edgecolor="black",
            lw=0.7,
            label="birth",
            zorder=3,
        )
        A.scatter(
            xb,
            b.reflection_margin,
            s=size,
            marker="v",
            color=BAND,
            alpha=0.6,
            lw=0,
            label="worst later wall",
            zorder=3,
        )
        A.scatter(
            xb,
            b.tracker_margin,
            s=6,
            marker="s",
            color="#2d6619",
            lw=0,
            label="tracker",
            zorder=2,
        )
        A.axhline(0.0, color="0.3", lw=0.7)
        A.axhline(LN10, color="0.3", lw=0.7, ls=":")
        A.set_xscale("log")
        A.set_xlim(xb.max() * 5, 0.8)  # from the first generation to today
        A.set_xlabel(r"$1 + z_E$ (time runs to the right)", fontsize=8)
        for r in range(2):
            axes[r, j].tick_params(labelsize=7)
        out[f"A={a:g} beta={beta:g}"] = {
            "generations": int(row.generations),
            "E_field_max": float(q.E_field.max()),
            "r_chi_E_field_max": float(q.r_chi_E_field.max()),
            "birth_margin_min": float(b.birth_margin.min()),
            "reflection_margin_min": float(b.reflection_margin.min()),
            "tracker_margin_min": float(b.tracker_margin.min()),
            "parking_share": float(row.parking_share),
        }
    axes[0, 0].set_ylabel("matter-like excess\n" r"(share of $\rho_R$)", fontsize=8)
    axes[1, 0].set_ylabel(
        r"margin $\ln(p/m)$ of each generation" "\n" "(size: its share of $K$)",
        fontsize=8,
    )
    axes[0, 0].legend(fontsize=7, frameon=False, loc="upper left")
    h, l = axes[1, 0].get_legend_handles_labels()
    h.append(Line2D([], [], color="0.3", lw=0.7, ls=":"))
    l.append(r"$\ln 10$ (the integrator's warning)")
    fig.legend(h, l, loc="lower center", ncol=4, fontsize=7, frameon=False)
    fig.suptitle(
        r"Three histories at $M = 4\times10^{-31}\,M_P$, one-loop law (spectrum U4-U17-v1, $c_a = 1$)",
        fontsize=9.5,
    )
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    return fig, out


def summary(H, Q):
    """The counts of PLAN.md §2.4, by (M, A) for the one-loop law and (M, alpha) for the fixed law."""
    res = {"one_loop": {}, "fixed": {}}
    L = H[H.drain_model == ONE_LOOP]
    for (m, a), s in L.groupby(["M_Mp", "alpha"]):
        st = s[s.stored]
        ms = st[st.measured]
        res["one_loop"][f"M={m:.3g} A={a:g}"] = {
            "rows": int(len(s)),
            "stored": int(len(st)),
            "measured": int(len(ms)),
            "K_check_failed": int(len(st) - len(ms)),
            "max_r_chi_E_field": _f(ms.r_chi_E_field_max.max()),
            "max_E_field": _f(ms.E_field_max.max()),
            "max_E_tracker": _f(ms.E_tracker_max.max()),
            "max_E_field_today": _f(ms.E_field_today.max()),
            "min_birth_margin": _f(st.all_worst_birth_margin.min()),
            "min_reflection_margin": _f(st.all_worst_reflection_margin.min()),
            "min_tracker_margin": _f(st.all_worst_tracker_margin.min()),
            "max_birth_failing_share": _f(st.all_birth_failing_share.max()),
            "max_reflection_failing_share": _f(st.all_reflection_failing_share.max()),
            "histories_reflection_failing_over_1pc": int(
                (st.all_reflection_failing_share > 0.01).sum()
            ),
            "max_parking_share": _f(st.parking_share.max()),
        }
    F = H[(H.drain_model == "fixed") & H.stored & (H.alpha > 0)]
    for (m, a), s in F.groupby(["M_Mp", "alpha"]):
        res["fixed"][f"M={m:.3g} alpha={a:g}"] = {
            "stored": int(len(s)),
            "max_weighted_birth_failing_share": _f(s.birth_failing_share.max()),
            "max_weighted_reflection_failing_share": _f(
                s.reflection_failing_share.max()
            ),
            "max_weighted_tracker_failing_share": _f(s.tracker_failing_share.max()),
        }
    # the spectrum's sensitivity: the largest relative change of each measure from c_a = 1 to 0 and 10
    sens = {}
    for c in (0, 10):
        j = Q[1][["E_field_max", "r_chi_E_field_max", "E_field_today"]].join(
            Q[c][["E_field_max", "r_chi_E_field_max", "E_field_today"]],
            rsuffix="_c",
            how="inner",
        )
        sens[f"c_a={c}"] = {
            k: _f(((j[f"{k}_c"] - j[k]).abs() / j[k].where(j[k] > 0)).max())
            for k in ("E_field_max", "r_chi_E_field_max", "E_field_today")
        }
    res["assoc_scale_sensitivity_max_relative_change"] = sens
    return res


def _f(x):
    return None if x is None or not np.isfinite(x) else float(x)


def main():
    H, Q = load()
    b_wall = wall_edge(H)
    head = git("rev-parse", "HEAD")
    dirty = bool(git("status", "--porcelain", "--untracked-files=no"))
    sidecar = json.loads(SIDECAR.read_text())
    meta = {
        "Subject": f"histories.csv sha256 {sha256(HISTORIES)}; store digest {sidecar['fingerprint']['digest']}; "
        f"git {head}{' (dirty)' if dirty else ''}"
    }
    for stem, fig in (("rel-lab", rel_lab(H, b_wall)), ("rel-M", rel_M(H, b_wall))):
        fig.savefig(
            HERE / f"{stem}.pdf",
            metadata={"Title": f"{stem}, 2026.10.0 science run", **meta},
        )
        fig.savefig(HERE / f"{stem}.png", dpi=200)
    fig, picks = rel_history(H)
    fig.savefig(
        HERE / "rel-history.pdf",
        metadata={"Title": "rel-history, 2026.10.0 science run", **meta},
    )
    fig.savefig(HERE / "rel-history.png", dpi=200)

    inputs = [HISTORIES, *QUANTA.values(), *sorted(SAMPLES.rglob("*.csv"))]
    provenance = {
        "script": "relativistic.py",
        "script_sha256": sha256(Path(__file__)),
        "git_head": head,
        "git_dirty": dirty,
        "inputs": {str(p.relative_to(HERE.parent)): sha256(p) for p in inputs},
        "store": {
            "name": sidecar["name"],
            "store_id": sidecar.get("store_id"),
            "digest": sidecar["fingerprint"]["digest"],
        },
        "beta_wall_edge": b_wall,
        "histories": picks,
        **summary(H, Q),
    }
    (HERE / "relativistic.provenance.json").write_text(
        json.dumps(provenance, indent=2) + "\n"
    )
    print(
        json.dumps(
            {
                k: provenance[k]
                for k in (
                    "git_head",
                    "git_dirty",
                    "histories",
                    "assoc_scale_sensitivity_max_relative_change",
                )
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
