"""
crossover side-campaign: the qualitative drain law (fit_law.py) drawn against stage 2's points, each point beside the
law at its own (L, R).

  (a) the gap along the histories: f = 1 - exp(-y), y = 0.020 (ln R - 1.56)^1.55 (1 + 0.84 ln L), evaluated along
      each history's own curve in (L, R), which ln(V0/M^4) fixes, from R = 1 to where the one-loop chain begins on
      it (born.chain); L changes along each line. Stage 2's gap points of that history.
  (b) the gap at fixed L: the law at the grid's L = 1.3, 2, 3, 4, 6, 8, to ln R = 15 (the grid's calibrated range)
      or to where the chain begins; the grid's points, each on its own L.
  (c) the one-loop side: f against ell, stage 2's points (field back out), the fit at L = 6 and 100 (capped at
      0.95), min(0.99, 2.7/ell) and 1 - exp(-2.7/ell), and the plateau 0.9 below ell = 1.

    ../../../../venv/bin/python plot_law.py   # writes fig/drain_law.png
"""

import os
import sys
from math import exp, log, pi

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

sys.path.insert(0, ".")
import born  # noqa: E402
from fit_law import scan_rows  # noqa: E402
from walls import Wall  # noqa: E402

RAMP = ("#86b6ef", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b")
SERIES = ("#2a78d6", "#eb6834", "#1baf7a", "#eda100")
INK, MUTED, GRID = "#1f1f1e", "#6b6a63", "#e4e3dd"

# the histories: label, ln(V0/M^4), the prefix of their stage-2 rows
HISTORIES = (
    ("laboratory M (2197)", 3.345, "2197"),
    ("8e-18 M_P", -119.16, "L-M8e-18"),
    ("1e-16 M_P", -129.27, "L-M1e-16"),
    ("5e-16 M_P", -135.70, "L-M5e-16"),
)
GRID_LS = (1.3, 2.0, 3.0, 4.0, 6.0, 8.0)


def f_gap(lnR, L):
    y = 0.0198 * np.clip(lnR - 1.5601, 0.0, None) ** 1.5472 * (1 + 0.841 * np.log(L))
    return 1 - np.exp(-y)


def f_loop(ell, L):
    d = ell + 0.9691 - 0.41 * np.log(L)
    return np.where(
        d > 0, np.minimum(0.95, 1 - np.exp(-2.69 / np.maximum(d, 1e-9))), 0.95
    )


def lnR_on(L, lv):
    return (
        lv + 2 * L + 2 * np.log(L**4 + 2 * L**3) - np.log(np.expm1(L)) - log(8 * pi**2)
    )


def has_chain(L, lnR):
    return born.chain(Wall(float(L)), exp(float(lnR))) is not None


def history_gap(lv):
    """(L, ln R) along the curve from ln R = 0 to the chain's edge, and whether the edge was found."""
    Ls = np.linspace(0.05, 140.0, 28000)
    lr = lnR_on(Ls, lv)
    i0 = int(np.argmax(lr >= 0.0))
    out_L, out_R = [], []
    for L, r in zip(Ls[i0:], lr[i0:]):
        if has_chain(L, r):
            return np.array(out_L), np.array(out_R), True
        out_L.append(L)
        out_R.append(r)
        if r > 20:
            break
    return np.array(out_L), np.array(out_R), False


def style(ax):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(MUTED)
    ax.tick_params(colors=MUTED, labelcolor=INK, labelsize=9)
    ax.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


rows = scan_rows()
gap = [
    r
    for r in rows
    if r[3] is None and r[2] > 0.0 and r[5] and not r[0].startswith("curve")
]
fig, (axa, axb, axc) = plt.subplots(1, 3, figsize=(18.0, 5.4))
fig.patch.set_facecolor("white")

# (a) along the histories
for (name, lv, prefix), c in zip(HISTORIES, SERIES):
    Lp, Rp, edge = history_gap(lv)
    axa.plot(
        np.exp(Rp),
        f_gap(Rp, Lp),
        color=c,
        lw=2,
        label=f"{name}: L = {Lp[0]:.3g} to {Lp[-1]:.3g}".replace("e+02", "00"),
    )
    if edge:
        axa.plot([exp(Rp[-1])] * 2, [f_gap(Rp[-1], Lp[-1]), 0.9], color=c, lw=1, ls=":")
        axa.plot(exp(Rp[-1]), 0.9, marker="o", ms=8, color=c, mec="white", mew=2)
    pts = [r for r in gap if r[0].startswith(prefix)]
    axa.plot(
        [exp(r[2]) for r in pts],
        [r[4] for r in pts],
        marker="D",
        ms=7,
        ls="none",
        color=c,
        mec="white",
        mew=1.5,
    )
    if prefix == "2197":
        for Lmark in (1.0, 2.0, 3.0):
            r = float(lnR_on(Lmark, lv))
            axa.annotate(
                f"L = {Lmark:g}",
                (exp(r), float(f_gap(r, Lmark))),
                xytext=(4, 6),
                textcoords="offset points",
                fontsize=8,
                color=INK,
            )
axa.plot([], [], marker="D", ms=7, ls="none", color=MUTED, mec="white", label="stage 2")
axa.plot(
    [],
    [],
    marker="o",
    ms=8,
    ls="none",
    color=MUTED,
    mec="white",
    label="chain begins (plateau 0.9)",
)
axa.set_xscale("log")
axa.set_xlim(1, 2e7)
axa.set_ylim(0, 1)
axa.set_xlabel("R  (loop strength at the classical wall)", color=INK)
axa.set_ylabel("f, fraction of E absorbed in one bounce", color=INK)
axa.set_title(
    "(a) The gap along each history's own (L, R) path",
    color=INK,
    loc="left",
    fontsize=11,
)
style(axa)
axa.legend(frameon=False, fontsize=8.5, loc="upper left", labelcolor=INK)

# (b) at the grid's fixed L
for L, c in zip(GRID_LS, RAMP):
    x = np.linspace(0.0, 15.0, 400)
    top = next((r for r in x if has_chain(L, r)), None) if L >= 3.5 else None
    hi = top if top is not None else 15.0
    x = x[x <= hi]
    axb.plot(np.exp(x), f_gap(x, L), color=c, lw=2)
    axb.annotate(
        f"L = {L:g}",
        (exp(x[-1]), float(f_gap(x[-1], L))),
        xytext=(4, 0),
        textcoords="offset points",
        fontsize=8,
        color=INK,
        va="center",
    )
    pts = [r for r in gap if r[0] == "grid" and abs(r[1] - L) < 1e-6 and r[2] <= 15.0]
    axb.plot(
        [exp(r[2]) for r in pts],
        [r[4] for r in pts],
        marker="^",
        ms=8,
        ls="none",
        color=c,
        mec="white",
        mew=1.5,
    )
axb.set_xscale("log")
axb.set_xlim(1, 3e7)
axb.set_ylim(0, 1)
axb.set_xlabel("R", color=INK)
axb.set_title(
    "(b) The gap at fixed L: the off-curve grid",
    color=INK,
    loc="left",
    fontsize=11,
)
style(axb)

# (c) the one-loop side
e = np.geomspace(1.0, 40.0, 400)
axc.plot(
    e,
    np.minimum(0.99, 2.7 / e),
    color=SERIES[1],
    lw=2,
    label="min(0.99, 2.7/ℓ): the law today",
)
axc.plot(e, 1 - np.exp(-2.7 / e), color=SERIES[2], lw=2, label="1 − exp(−2.7/ℓ)")
axc.plot(e, f_loop(e, 6.0), color=RAMP[1], lw=2, label="fit, L = 6")
axc.plot(e, f_loop(e, 100.0), color=RAMP[5], lw=2, label="fit, L = 100")
axc.plot(
    [0.4, 1.0], [0.9, 0.9], color=MUTED, lw=2, ls="--", label="plateau 0.9 (ℓ < 1)"
)
one = [r for r in rows if r[3] is not None and r[5]]
for small, mk, name in (
    (True, "s", "stage 2, L ≤ 30"),
    (False, "D", "stage 2, L > 30"),
):
    pts = [r for r in one if (r[1] <= 30) == small]
    axc.plot(
        [r[3] for r in pts],
        [r[4] for r in pts],
        marker=mk,
        ms=7,
        ls="none",
        color=INK if small else RAMP[5],
        mec="white",
        mew=1.5,
        label=name,
    )
axc.set_xscale("log")
axc.set_xlim(0.4, 40)
axc.set_ylim(0, 1)
axc.set_xticks([0.5, 1, 2, 5, 10, 20])
axc.set_xticklabels(["0.5", "1", "2", "5", "10", "20"])
axc.set_xlabel("ℓ = ln(k_ex / m_ta)", color=INK)
axc.set_title("(c) The one-loop side: f against ℓ", color=INK, loc="left", fontsize=11)
style(axc)
axc.legend(frameon=False, fontsize=8.5, loc="upper right", labelcolor=INK)

fig.tight_layout()
os.makedirs("fig", exist_ok=True)
fig.savefig("fig/drain_law.png", dpi=150, facecolor="white")
print("wrote fig/drain_law.png")
