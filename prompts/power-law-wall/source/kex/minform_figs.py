"""
power-law-wall prompt 07b2 §1.9: the figures (three, PNG, in fig/). Each plots only what a text output holds: the
measured points are those of fits.txt / refit.txt (07b's and crossover's) and stage2_points.txt (this prompt's), and the
laws' curves are minform.fraction (the variants) and fits.v1_fraction (v1, in the old chain) evaluated at the constants
of minform.json ("with_new", S3) and of production (v1).

  fig/f_vs_lnR.png        f against ln R at each grid L (n = 1 `grid`, n = 2 `n2-grid`) and on each history curve
                          (n = 1 laboratory, L-M1e-16, L-M8e-18; n = 2 laboratory), with the data, v1 and (v);
                          the grey band is outside S3 (ln(V0/M^4) > 10);
  fig/f_vs_ell.png        f against ell_new at every chain point (ln R > 0.3, S3's domain), coloured by ln L, with
                          (v)'s fall branch 1 - exp(-A/(ell - ell0)) at each n;
  fig/prediction.png      this prompt's new points: measured f against the f prediction.json predicted, by group.

    cd prompts/power-law-wall/source/kex && ../../../../venv/bin/python -B minform_figs.py
"""

import json
import os
import sys
import warnings
from math import exp, log

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import fits  # noqa: E402
import minform as mf  # noqa: E402
import minform_predict as mp  # noqa: E402

import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

warnings.simplefilter("ignore")
FIG = os.path.join(HERE, "fig")
INK, INK2, GRID = "#0b0b0b", "#52514e", "#d9d8d4"
C_V, C_V1, C_NEW = "#2a78d6", "#eb6834", "#1baf7a"


def style(ax):
    ax.grid(True, color=GRID, lw=0.6)
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(INK2)
    ax.tick_params(colors=INK2, labelsize=7)


def chains(n, L, lnR):
    import kexchain as kc
    import rechain

    w = rechain.wall_of(dict(n=n, L=L))
    out = {}
    for form, key in (("lambda", "old"), ("v3", "new")):
        ch = kc.chain(w, exp(lnR), form)
        out[key] = (None if ch is None else ch[3], kc.regime(lnR, ch))
    return out


def points_marks(ax, d, x):
    for p in d:
        new = p["file"] == "new (07b2)"
        ax.plot(
            x(p),
            p["frac"],
            "D" if new else "o",
            ms=5.0 if new else 5.5,
            mfc=(C_NEW if new else INK2) if p["back"] else "white",
            mec=C_NEW if new else INK2,
            mew=1.0,
            zorder=5,
        )


def main():
    os.makedirs(FIG, exist_ok=True)
    J = json.load(open(os.path.join(mf.OUT, "minform.json")))["with_new"]
    P = {
        "variant": "v",
        "n1": J["n1/S3/v"]["params"]["n1"],
        "n2": J["n2/S3/v"]["params"]["n2"],
    }
    ps = mf.all_points(with_new=True)

    # ---- figure 1 ----
    panels = [(1, "grid", L) for L in (1.3, 2.0, 3.0, 4.0, 6.0, 8.0)]
    panels += [(1, "curve", c) for c in ("lab", "L-M1e-16", "L-M8e-18")]
    panels += [(2, "n2-grid", L) for L in (2.0, 4.0, 8.0, 16.0, 30.0, 60.0, 100.0)]
    panels += [(2, "curve", "lab")]
    LVc = {"lab": 3.345, "L-M1e-16": -129.27, "L-M8e-18": -119.16}
    fig, axs = plt.subplots(4, 5, figsize=(17, 12.5), sharey=True)
    axs = axs.ravel()
    for ax, (n, kind, what) in zip(axs, panels):
        style(ax)
        if kind == "curve":
            lv = LVc[what]
            mp.LV["_"] = lv
            Ls = (
                np.geomspace(0.8, (110.0 if n == 1 else 12.0), 260)
                if what == "lab"
                else np.linspace(86.0, 134.0, 260)
            )
            xs = [mp.lnR_on(n, "_", L) for L in Ls]
            keep = [(L, r) for L, r in zip(Ls, xs) if r > 0.31]
            d = [
                p
                for p in ps
                if p["n"] == n and abs(p["lv"] - lv) < 0.2 and p["lnR"] > 0.3
            ]
            title = f"n = {n}, curve {what} (ln V0/M^4 = {lv})"
        else:
            L = what
            rmax = 26.0 if n == 1 else 41.0
            keep = [(L, r) for r in np.linspace(0.35, rmax, 220)]
            d = [p for p in ps if p["n"] == n and p["set"] == kind and p["L"] == L]
            title = f"n = {n}, {kind} L = {L:g}"
            hi = mp.lnR_on(n, "lab", L) - 3.345 + mf.LV_MAX
            if hi < rmax:
                ax.axvspan(hi, rmax, color=GRID, alpha=0.5, lw=0)
        cs = [chains(n, L, r) for L, r in keep]
        ax.plot(
            [r for _, r in keep],
            [
                fits.v1_fraction(c["old"][1], c["old"][0], L, r)
                for (L, r), c in zip(keep, cs)
            ],
            color=C_V1,
            ls="--",
            lw=1.5,
            label="v1 (old chain)",
        )
        ax.plot(
            [r for _, r in keep],
            [mf.fraction(c["new"][0], L, r, n, P) for (L, r), c in zip(keep, cs)],
            color=C_V,
            lw=1.8,
            label="(v), S3 with the new points",
        )
        points_marks(ax, d, lambda p: p["lnR"])
        ax.set_title(title, fontsize=9, color=INK)
        ax.set_xlabel("ln R", fontsize=8, color=INK2)
        ax.set_ylim(-0.02, 1.02)
    for ax in axs[len(panels) :]:
        ax.axis("off")
    for ax in axs[::5]:
        ax.set_ylabel("f", fontsize=9, color=INK2)
    h, lab = axs[0].get_legend_handles_labels()
    h += [
        plt.Line2D([], [], marker="o", ls="", mfc=INK2, mec=INK2),
        plt.Line2D([], [], marker="o", ls="", mfc="white", mec=INK2),
        plt.Line2D([], [], marker="D", ls="", mfc=C_NEW, mec=C_NEW),
        plt.Line2D([], [], marker="D", ls="", mfc="white", mec=C_NEW),
        plt.Rectangle((0, 0), 1, 1, color=GRID, alpha=0.5),
    ]
    lab += [
        "measured (07b, crossover), back out",
        "measured, not back out",
        "new (07b2), back out",
        "new (07b2), not back out",
        "outside S3",
    ]
    axs[-1].legend(h, lab, loc="center", fontsize=9, frameon=False)
    fig.suptitle(
        "Exponential wall: stage-2 f against ln R, with v1 and (v)",
        fontsize=12,
        color=INK,
    )
    fig.tight_layout()
    fig.savefig(os.path.join(FIG, "f_vs_lnR.png"), dpi=110)
    plt.close(fig)

    # ---- figure 2 ----
    fig, axs = plt.subplots(1, 2, figsize=(13, 5.2))
    cmap = plt.get_cmap("Blues")
    for ax, n in zip(axs, (1, 2)):
        style(ax)
        d = [
            p
            for p in ps
            if p["n"] == n
            and p["lnR"] > 0.3
            and p["ell_new"] is not None
            and mf.in_S3(p)
        ]
        lo, hi = log(min(p["L"] for p in d)), log(max(p["L"] for p in d))
        col = [cmap(0.3 + 0.7 * (log(p["L"]) - lo) / (hi - lo)) for p in d]
        ax.scatter(
            [p["ell_new"] for p in d if p["back"]],
            [p["frac"] for p in d if p["back"]],
            c=[c for c, p in zip(col, d) if p["back"]],
            s=30,
            edgecolors=INK2,
            linewidths=0.5,
            zorder=4,
            label="back out",
        )
        ax.scatter(
            [p["ell_new"] for p in d if not p["back"]],
            [p["frac"] for p in d if not p["back"]],
            facecolors="white",
            edgecolors=[c for c, p in zip(col, d) if not p["back"]],
            s=30,
            linewidths=1.2,
            zorder=4,
            label="not back out",
        )
        q = P[f"n{n}"]
        ee = np.linspace(q["ell0"] + 0.05, 34.0, 600)
        ax.plot(
            ee,
            1 - np.exp(-q["A"] / (ee - q["ell0"])),
            color=C_V,
            lw=1.6,
            label=f"(v) fall: A {q['A']:.3f}, ell0 {q['ell0']:.3f}",
        )
        ax.set_xscale("log")
        ax.set_xlabel("ell_new (colour: ln L, light = small L)", fontsize=9, color=INK2)
        ax.set_ylabel("f", fontsize=9, color=INK2)
        ax.set_title(
            f"n = {n}: chain points in S3's domain ({len(d)})", fontsize=10, color=INK
        )
        ax.set_ylim(-0.02, 1.02)
        ax.legend(fontsize=8, frameon=False)
    fig.tight_layout()
    fig.savefig(os.path.join(FIG, "f_vs_ell.png"), dpi=120)
    plt.close(fig)

    # ---- figure 3 ----
    pred = {
        p["id"]: p
        for p in json.load(open(os.path.join(mf.OUT, "prediction.json")))["points"]
    }
    rows = mf.new_points_07b2()
    fig, ax = plt.subplots(figsize=(6.4, 6.0))
    style(ax)
    groups = (
        ("07b2_lab_n=1", "laboratory curve, n = 1", "#2a78d6", "o"),
        ("07b2_L-M1e-16", "L-M1e-16, n = 1", "#eb6834", "s"),
        ("07b2_lab_n=2", "laboratory curve, n = 2", "#1baf7a", "^"),
        ("07b2_held-out", "held-out set", "#6250d6", "D"),
    )
    for key, lab, col, m in groups:
        d = [r for r in rows if r["set"] == key]
        for back in (True, False):
            dd = [r for r in d if r["back"] == back]
            ax.plot(
                [pred[r["idx"]]["f_predicted"] for r in dd],
                [r["frac"] for r in dd],
                m,
                ms=6,
                ls="",
                mfc=col if back else "white",
                mec=col,
                mew=1.2,
                label=f"{lab}{'' if back else ', not back out'}" if dd else None,
            )
    ax.plot([0, 1], [0, 1], color=INK2, lw=0.8)
    ax.set_xlabel("f predicted (prediction.json)", fontsize=9, color=INK2)
    ax.set_ylabel("f measured (stage2_points.txt)", fontsize=9, color=INK2)
    ax.set_title(
        "The new points against the prediction fixed before they ran",
        fontsize=10,
        color=INK,
    )
    ax.legend(fontsize=7.5, frameon=False, loc="upper left")
    ax.set_xlim(0, 1.0)
    ax.set_ylim(0, 1.0)
    fig.tight_layout()
    fig.savefig(os.path.join(FIG, "prediction.png"), dpi=120)
    plt.close(fig)
    print("figures written: fig/f_vs_lnR.png, fig/f_vs_ell.png, fig/prediction.png")


if __name__ == "__main__":
    main()
