"""
crossover side-campaign: the qualitative drain law (fit_law.py) drawn against stage 2's points.

  (a) the gap: f = 1 - exp(-y), y = 0.020 (ln R - 1.56)^1.55 (1 + 0.84 ln L), against R from R = 1 (the classical
      boundary) to where the one-loop chain takes over at that L (born.chain), or to ln R = 15 where it never does
      (L <= 3; the fit is calibrated there only to ln R = 15). Stage 2's gap points, coloured by the nearest L.
  (b) the one-loop side: f against ell, stage 2's points (field back out), the fit at L = 6 and 100 (capped at 0.95),
      min(0.99, 2.7/ell) and 1 - exp(-2.7/ell), and the plateau 0.9 below ell = 1.

    ../../../../venv/bin/python plot_law.py   # writes fig/drain_law.png
"""

import os
import sys
from math import exp, log

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

sys.path.insert(0, ".")
import born  # noqa: E402
from fit_law import scan_rows  # noqa: E402
from walls import Wall  # noqa: E402

LS = (1.0, 3.0, 5.0, 10.0, 30.0, 100.0)
RAMP = ("#86b6ef", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b")
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
INK, MUTED, GRID = "#1f1f1e", "#6b6a63", "#e4e3dd"


def f_gap(lnR, L):
    y = 0.0198 * np.clip(lnR - 1.5601, 0.0, None) ** 1.5472 * (1 + 0.841 * np.log(L))
    return 1 - np.exp(-y)


def f_loop(ell, L):
    d = ell + 0.9691 - 0.41 * np.log(L)
    return np.where(
        d > 0, np.minimum(0.95, 1 - np.exp(-2.69 / np.maximum(d, 1e-9))), 0.95
    )


def gap_top(L):
    w = Wall(L)
    for lnR in np.arange(0.0, 15.001, 0.05):
        if born.chain(w, exp(lnR)) is not None:
            return lnR
    return None


def nearest(L):
    return int(np.argmin([abs(log(L / c)) for c in LS]))


def style(ax):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(MUTED)
    ax.tick_params(colors=MUTED, labelcolor=INK, labelsize=9)
    ax.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


rows = scan_rows()
fig, (axa, axb) = plt.subplots(1, 2, figsize=(12.5, 5.0))
fig.patch.set_facecolor("white")

# (a) the gap
for L, c in zip(LS, RAMP):
    top = gap_top(L)
    hi = top if top is not None else 15.0
    x = np.linspace(0.0, hi, 300)
    axa.plot(np.exp(x), f_gap(x, L), color=c, lw=2, label=f"L = {L:g}")
    if top is None:
        axa.annotate(
            f"L = {L:g}",
            (exp(hi), float(f_gap(hi, L))),
            xytext=(4, 0),
            textcoords="offset points",
            fontsize=8.5,
            color=INK,
            va="center",
        )
    if top is not None:
        axa.plot([exp(top)] * 2, [f_gap(top, L), 0.9], color=c, lw=1, ls=":")
        axa.plot(exp(top), 0.9, marker="o", ms=8, color=c, mec="white", mew=2)
        axa.annotate(
            f"{L:g}",
            (exp(top), 0.9),
            xytext={100.0: (-3, 7), 30.0: (3, 7)}.get(L, (0, 7)),
            textcoords="offset points",
            fontsize=8,
            color=INK,
            ha={100.0: "right", 30.0: "left"}.get(L, "center"),
        )
gap = [
    r
    for r in rows
    if r[3] is None
    and r[2] > 0.0
    and r[5]
    and not r[0].startswith("curve")
    and not (r[0] == "grid" and r[2] > 15)
]
for src, mk, name in (
    ("2197", "s", "laboratory curve"),
    ("grid", "^", "grid"),
    ("L-M", "D", "near M_crit"),
):
    pts = [r for r in gap if r[0].startswith(src)]
    for r in pts:
        axa.plot(
            exp(r[2]),
            r[4],
            marker=mk,
            ms=8,
            ls="none",
            color=RAMP[nearest(r[1])],
            mec="white",
            mew=1.5,
        )
    axa.plot(
        [],
        [],
        marker=mk,
        ms=8,
        ls="none",
        color=MUTED,
        mec="white",
        label=f"stage 2, {name}",
    )
axa.plot(
    [],
    [],
    marker="o",
    ms=8,
    ls="none",
    color=MUTED,
    mec="white",
    label="chain begins: plateau 0.9",
)
axa.set_xscale("log")
axa.set_xlim(1, 3e7)
axa.set_ylim(0, 1)
axa.set_xlabel("R  (loop strength at the classical wall)", color=INK)
axa.set_ylabel("f, fraction of E absorbed in one bounce", color=INK)
axa.set_title("(a) The gap: f against R at fixed L", color=INK, loc="left", fontsize=11)
style(axa)
axa.legend(frameon=False, fontsize=8.5, loc="lower right", labelcolor=INK, ncol=2)

# (b) the one-loop side
e = np.geomspace(1.0, 40.0, 400)
axb.plot(
    e,
    np.minimum(0.99, 2.7 / e),
    color=ORANGE,
    lw=2,
    label="min(0.99, 2.7/ℓ): the law today",
)
axb.plot(e, 1 - np.exp(-2.7 / e), color=AQUA, lw=2, label="1 − exp(−2.7/ℓ)")
axb.plot(e, f_loop(e, 6.0), color=RAMP[1], lw=2, label="fit, L = 6")
axb.plot(e, f_loop(e, 100.0), color=RAMP[5], lw=2, label="fit, L = 100")
axb.plot(
    [0.4, 1.0], [0.9, 0.9], color=MUTED, lw=2, ls="--", label="plateau 0.9 (ℓ < 1)"
)
one = [r for r in rows if r[3] is not None and r[5]]
for small, mk, name in (
    (True, "s", "stage 2, L ≤ 30"),
    (False, "D", "stage 2, L > 30"),
):
    pts = [r for r in one if (r[1] <= 30) == small]
    axb.plot(
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
axb.set_xscale("log")
axb.set_xlim(0.4, 40)
axb.set_ylim(0, 1)
axb.set_xticks([0.5, 1, 2, 5, 10, 20])
axb.set_xticklabels(["0.5", "1", "2", "5", "10", "20"])
axb.set_xlabel("ℓ = ln(k_ex / m_ta)", color=INK)
axb.set_title("(b) The one-loop side: f against ℓ", color=INK, loc="left", fontsize=11)
style(axb)
axb.legend(frameon=False, fontsize=8.5, loc="upper right", labelcolor=INK)

fig.tight_layout()
os.makedirs("fig", exist_ok=True)
fig.savefig("fig/drain_law.png", dpi=160, facecolor="white")
print("wrote fig/drain_law.png")
