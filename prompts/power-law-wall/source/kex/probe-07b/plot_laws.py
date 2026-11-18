"""Diagnostic plots: exponential stage-2 fractions against v1 (old k_ex) and 07b's candidates (new k_ex)."""

import json, os, sys
from math import exp, log
import numpy as np

SRC, OUTDIR = sys.argv[1], sys.argv[2]
ROOT = os.path.abspath(os.path.join(SRC, "../../../.."))
os.chdir(SRC)
sys.path.insert(0, SRC)
import fits, rechain
import kexchain as kc
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

PAR = json.load(open(os.path.join(ROOT, ".documents/power-law-wall/kex-07b/fits.json")))
PTS = json.load(open(os.path.join(OUTDIR, "points.json")))
INK, INK2, GRID = "#0b0b0b", "#52514e", "#d9d8d4"
C_I, C_III, C_IVF = "#2a78d6", "#eb6834", "#1baf7a"


def chains(n, L, lnR):
    w = rechain.wall_of(dict(n=n, L=L))
    out = {}
    for form, key in (("lambda", "old"), ("v3", "new")):
        ch = kc.chain(w, exp(lnR), form)
        out[key] = (None if ch is None else ch[3], kc.regime(lnR, ch))
    return out


def law_v1(c, L, lnR):
    ell, reg = c["old"]
    return fits.v1_fraction(reg, ell, L, lnR)


def law_i(p, c, L, lnR):
    ell, reg = c["new"]
    if reg == "one_loop":
        if ell < 1.0:
            return p["plateau"]
        return float(fits.f_one((ell, L), p["A"], p["B0"], p["C"]))
    return float(fits.f_gap((lnR, L), p["G"], p["R0"], p["P"], p["Q"]))


def law_iii(p, c, L, lnR):
    ell, reg = c["new"]
    if reg == "one_loop" and ell >= p["ell_g"]:
        return float(fits.f_one((ell, L), p["A"], p["B0"], p["C"]))
    return float(fits.f_gap((lnR, L), p["G"], p["R0"], p["P"], p["Q"]))


def law_iv(p, c, L, lnR):
    ell, reg = c["new"]
    e = ell if reg == "one_loop" else -1.0
    return float(
        fits.f_iv(
            (np.array([e]), np.array([L]), np.array([lnR])),
            p["G"],
            p["R0"],
            p["P"],
            p["Q"],
            p["B0"],
            p["C"],
        )[0]
    )


def laws_for(n):
    if n == 1:
        pp, pf = PAR["n1_planner_selection"], PAR["n1"]
        return [
            ("v1 (old $k_{ex}$)", INK, "-", lambda c, L, r: law_v1(c, L, r)),
            (
                "(i) new $\\ell$, plateau below 1",
                C_I,
                "-",
                lambda c, L, r: law_i(pp["i"], c, L, r),
            ),
            (
                "(iii) = (iv), split at $\\ell_g$=1.55",
                C_III,
                "-",
                lambda c, L, r: law_iii(pp["iii"], c, L, r),
            ),
            (
                "(iv) on fit_law's selection",
                C_IVF,
                "--",
                lambda c, L, r: law_iv(pf["iv"], c, L, r),
            ),
        ]
    p = PAR["n2"]
    return [
        ("v1 (old $k_{ex}$)", INK, "-", lambda c, L, r: law_v1(c, L, r)),
        (
            "(i) new $\\ell$, plateau below 1",
            C_I,
            "-",
            lambda c, L, r: law_i(p["i"], c, L, r),
        ),
        (
            "(iii) = (iv), split at $\\ell_g$=1.55",
            C_III,
            "-",
            lambda c, L, r: law_iii(p["iii"], c, L, r),
        ),
    ]


def style(ax):
    ax.grid(True, color=GRID, lw=0.6)
    ax.set_axisbelow(True)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(INK2)
    ax.tick_params(colors=INK2, labelsize=8)


def first_ell1(n, L, grid, key):
    for r in grid:
        e, reg = chains(n, L, r)[key]
        if reg == "one_loop" and e >= 1.0:
            return r
    return None


# ---- figures 1, 2: f against ln R at each grid L ---------------------------------------------------------------
for n, setname, Ls, shape in (
    (1, "grid", [1.3, 2.0, 3.0, 4.0, 6.0, 8.0], (2, 3)),
    (2, "n2-grid", [2.0, 4.0, 8.0, 16.0, 30.0, 60.0, 100.0], (2, 4)),
):
    laws = laws_for(n)
    lnRs = np.linspace(0.35, 41.0 if n == 2 else 26.0, 260)
    fig, axs = plt.subplots(
        *shape, figsize=(4.0 * shape[1], 3.3 * shape[0]), sharey=True
    )
    axs = axs.ravel()
    for ax, L in zip(axs, Ls):
        style(ax)
        cs = [chains(n, L, r) for r in lnRs]
        for name, col, ls, fn in laws:
            ax.plot(
                lnRs,
                [fn(c, L, r) for c, r in zip(cs, lnRs)],
                color=col,
                ls=ls,
                lw=1.6,
                label=name,
            )
        for key, lab in (("old", "old"), ("new", "new")):
            r1 = next(
                (
                    r
                    for c, r in zip(cs, lnRs)
                    if c[key][1] == "one_loop" and c[key][0] >= 1.0
                ),
                None,
            )
            if r1 is not None:
                ax.axvline(r1, color=INK if key == "old" else C_III, lw=0.8, ls=":")
        d = [
            p
            for p in PTS
            if p["n"] == n and p["set"] == setname and abs(p["L"] - L) < 1e-9
        ]
        b = [p for p in d if p["back"]]
        nb = [p for p in d if not p["back"]]
        ax.plot(
            [p["lnR"] for p in b],
            [p["frac"] for p in b],
            "o",
            ms=6.5,
            color=INK2,
            mec="white",
            mew=1.0,
            label="measured, field back out",
            zorder=5,
        )
        ax.plot(
            [p["lnR"] for p in nb],
            [p["frac"] for p in nb],
            "o",
            ms=6.5,
            mfc="white",
            mec=INK2,
            mew=1.2,
            label="measured, not back out",
            zorder=5,
        )
        ax.set_title(f"n = {n}, L = {L:g}", fontsize=10, color=INK)
        ax.set_xlabel("ln R", fontsize=9, color=INK2)
        ax.set_ylim(-0.02, 1.02)
    for ax in axs[:: shape[1]]:
        ax.set_ylabel("drained fraction f", fontsize=9, color=INK2)
    for ax in axs[len(Ls) :]:
        ax.axis("off")
    h, l = axs[0].get_legend_handles_labels()
    h += [
        plt.Line2D([], [], color=INK, ls=":", lw=0.8),
        plt.Line2D([], [], color=C_III, ls=":", lw=0.8),
    ]
    l += ["$\\ell = 1$, old $k_{ex}$", "$\\ell = 1$, new $k_{ex}$"]
    if len(Ls) < len(axs):
        axs[-1].legend(h, l, loc="center", fontsize=9, frameon=False)
    else:
        fig.legend(
            h,
            l,
            loc="lower center",
            ncol=4,
            fontsize=9,
            frameon=False,
            bbox_to_anchor=(0.5, -0.06),
        )
    fig.suptitle(
        f"Exponential wall, n = {n}: stage-2 grid against v1 and 07b's candidates"
        + (" (planner's selection, except the dashed curve)" if n == 1 else ""),
        fontsize=11,
        color=INK,
    )
    fig.tight_layout()
    fig.savefig(
        os.path.join(OUTDIR, f"laws_vs_lnR_n{n}.png"), dpi=130, bbox_inches="tight"
    )
    plt.close(fig)

# ---- figure 3: f against ell in each form, every chain point ---------------------------------------------------
fig, axs = plt.subplots(2, 3, figsize=(13.5, 7.4))
for row, n in enumerate((1, 2)):
    d = [
        p
        for p in PTS
        if p["n"] == n and p["back"] and p["lnR"] > 0.3 and (n == 1 or p["n2_ok"])
    ]
    lo, hi = np.log(min(p["L"] for p in d)), np.log(max(p["L"] for p in d))
    cmap = plt.get_cmap("Blues")
    col = lambda L: cmap(0.35 + 0.65 * (np.log(L) - lo) / (hi - lo))
    for k, key in enumerate(("old", "new")):
        ax = axs[row, k]
        style(ax)
        q = [p for p in d if p[f"ell_{key}"] is not None]
        ax.scatter(
            [p[f"ell_{key}"] for p in q],
            [p["frac"] for p in q],
            c=[col(p["L"]) for p in q],
            s=34,
            edgecolors=INK2,
            linewidths=0.4,
            zorder=4,
        )
        ax.axvline(1.0, color=INK2, lw=0.8, ls=":")
        if key == "old":
            pl = [p for p in q if p["ell_old"] >= 1.0]
            ee = np.linspace(1.0, 9.5, 200)
            for Lr in (2.0, 8.0, 30.0, 100.0):
                ax.plot(
                    ee,
                    [fits.v1_fraction("one_loop", e, Lr, 0.0) for e in ee],
                    color=INK,
                    lw=0.9,
                    alpha=0.6,
                )
            ax.plot([0, 1], [0.9, 0.9], color=INK, lw=0.9, alpha=0.6)
        nchain = len(q)
        ngap = len(d) - nchain
        ax.set_title(
            f"n = {n}: f against $\\ell$, {'old' if key == 'old' else 'new'} $k_{{ex}}$ "
            f"({nchain} chain points; {ngap} gap points not shown)",
            fontsize=9.5,
            color=INK,
        )
        ax.set_xlabel(
            "$\\ell$"
            + (" (v1 curves at L = 2, 8, 30, 100 in grey)" if key == "old" else ""),
            fontsize=9,
            color=INK2,
        )
        ax.set_ylabel("f", fontsize=9, color=INK2)
        ax.set_xlim(-0.1, 9.5)
        ax.set_ylim(-0.02, 1.02)
    ax = axs[row, 2]
    style(ax)
    q = [p for p in d if p["ell_new"] is not None]
    both = [p for p in q if p["ell_old"] is not None]
    only = [p for p in q if p["ell_old"] is None]
    ax.scatter(
        [p["L"] for p in both],
        [p["ell_new"] - p["ell_old"] for p in both],
        c=[col(p["L"]) for p in both],
        s=34,
        edgecolors=INK2,
        linewidths=0.4,
        label="chain in both forms",
        zorder=4,
    )
    ax.scatter(
        [p["L"] for p in only],
        [p["ell_new"] for p in only],
        marker="^",
        s=40,
        facecolors="white",
        edgecolors=C_III,
        linewidths=1.1,
        label="chain only in new form: $\\ell_{new}$ shown",
        zorder=4,
    )
    ax.set_xscale("log")
    ax.legend(fontsize=8, frameon=False)
    ax.set_title(
        f"n = {n}: $\\ell_{{new}} - \\ell_{{old}}$ against L", fontsize=9.5, color=INK
    )
    ax.set_xlabel("L (colour scale of the left panels)", fontsize=9, color=INK2)
    ax.set_ylabel("$\\Delta\\ell$", fontsize=9, color=INK2)
fig.tight_layout()
fig.savefig(os.path.join(OUTDIR, "f_vs_ell_old_new.png"), dpi=130, bbox_inches="tight")
plt.close(fig)

# ---- like-for-like residuals ----------------------------------------------------------------------------------
print(
    "max |fit - f| (rms) on one identical point set per row: back out, ln R > 0.3, n = 2 with ln(V0/M^4) <= 10"
)
for n in (1, 2):
    laws = laws_for(n)
    base = [
        p
        for p in PTS
        if p["n"] == n and p["back"] and p["lnR"] > 0.3 and (n == 1 or p["n2_ok"])
    ]
    sets = [
        ("every point", base),
        ("planner's selection", [p for p in base if p["planner"]]),
        (
            "the grid rows above ln R = 15 alone",
            [p for p in base if p["set"].endswith("grid") and p["lnR"] > 15],
        ),
    ]
    if n == 2:
        sets = sets[:1] + sets[2:]
    for sname, s in sets:
        cs = [chains(n, p["L"], p["lnR"]) for p in s]
        cells = []
        for name, _, _, fn in laws:
            r = np.array([fn(c, p["L"], p["lnR"]) - p["frac"] for c, p in zip(cs, s)])
            cells.append(
                f"{name.split(' ')[0]} {np.abs(r).max():.3f} ({np.sqrt(np.mean(r**2)):.3f})"
            )
        print(f"  n={n} {sname:38s} {len(s):3d} pts: " + "   ".join(cells))
