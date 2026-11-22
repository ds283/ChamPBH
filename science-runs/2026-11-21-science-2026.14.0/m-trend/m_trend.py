"""
The trend in M across the laboratory region, 2026.14.0 science run, for one wall: how the one-loop
drain law's Delta N_eff, the quantity the law reads at the first impact and the BBN exclusions move
with the potential's M from 1e-30 to 1e-42 M_P, on the sparse beta grid of blocks A (4e-31, whose
dense grid holds the sparse one) and B.

    ./venv/bin/python science-runs/<folder>/m-trend/m_trend.py --wall {exp,pl1,pl2}

A wall is one potential at one n (PLAN.md P10): exp, the exponential at n = 1 under
one-loop-exponential-v2; pl1 and pl2, the inverse power law at n = 1 and 2 under
one-loop-inverse-power-v1. Each wall has its own figure, with the same layout and rulings (PLAN.md
P9, treat the potentials symmetrically).

Reads ../fig6/inputs/<wall>/histories.csv (the wall's plot_by_beta.py export, committed with Figure 6),
inputs/<wall>/bounces/beta=<beta>/M=<M>eV_n=<n>/bounces.csv beside this file (unchanged copies of the
wall's plot_ScalarHistory.py --no-plots export, A = 1, beta = 1.2, 2 and 10, at every M) and the wall's
store's sidecar in ../registry/stores/. Writes m_trend-<wall>.pdf, .png and .provenance.json beside
this file. --inputs (the histories.csv), --bounces (the bounces directory), --sidecar and --out point
it elsewhere, to test it on another export. Needs no Ray and no store.

Three panels against M (drafted at the user's request of 2026-11-09):
(a) Delta N_eff today relative to its value at 4e-31 M_P, the median over the surfing beta (>= 1.06) for
    each A, with the range over beta shaded for A = 1; the fixed law is at 1 to 2e-8 at every M.
(b) ell = ln(k_ex/m_ta) at the first impact, A = 1, beta = 1.2, 2 and 10 (points), the law's ell(M) at
    that impact's E solved on V = V0 exp(M/phi) (thin line), which has no maximum (ell_law); above
    ell = 45 the law is used beyond the range one-loop-drain 02d measured it on.
(c) The largest beta excluded at each (A, M), by the tests of Figure 6: |dY_p| and |d(D/H)| within twice
    the measurement's relative 1-sigma about PRyMordial + LambdaCDM, and Delta N_eff today < 0.30.
Colours follow Figure 6: A = 1 black, A = 3, 5 and 10 green, the band's other A in shades of its red.
On the power wall, panel (b) draws ln R at the first impact (the quantity its law reads), as points
only, for the same beta (the user's ruling of 2026-11-19): it has no ell(M) curve and no maximum.

Copied on 2026-11-14 from the 2026.10.0 figure (../../2026-11-08-science-2026.10.0/m-trend/m_trend.py)
for the 2026.11.0 run, with its rulings and no new choice. Changed: the run's name and the sidecar;
inputs/bounces/ holds the same 21 histories' files from this run's export.

Ported on 2026-11-20 from the 2026.11.0 figure (../../2026-11-13-science-2026.11.0/m-trend/m_trend.py)
for the 2026.14.0 run (PLAN.md section 5), with its rulings and one new choice, ruled by the user on
2026-11-19 (panel (b) on the power wall shows ln R). Changed: one figure per wall (--wall, WALLS); the
one-loop rows are the wall's own drain model; rows are filtered by the export's model and n columns;
the n in the bounce path is the wall's; on the exponential wall the region below 4e-31 M_P is marked
in panels (a) and (b), and noted in (c), as law v2 used beyond its fitted domain ln(V0/M^4) <= 10
(PLAN.md P4, the user's ruling to extrapolate it); panel (b) on the power wall as above; panel (a)'s
y range widens only if a wall's ratios leave 2026.11.0's; the title names the potential and n.

Changed on 2026-11-22, after the build: a (beta, M) whose bounces file the export does not hold (its
history failed, so the store has no samples) is skipped in panel (b) and listed in the provenance's
first_impact_missing; a beta with none at any M keeps its legend entry, saying so. On pl1 every
history at beta = 10 failed at every M (an overflow in (ln V)'' at phi ~ 1e-156 M_P to 1e-166 M_P).

Changed again on 2026-11-22, at the user's request ("fix the ell_law curve for the new k_ex"): ell_law used
the k_ex from before power-law-wall 07b (lambda = x^2/M) and was 0.1 low at 4e-31 and 2.6 low at 1e-42. It now
uses k_ex = E^{1/2}|V'''/V''|/2, as the code does, and reproduces every stored ell to 1.4e-14. Under that k_ex ell
has no maximum over M, so the dashed maximum line, its constant and the provenance's ell_max are gone.
"""

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.optimize import brentq

HERE = Path(__file__).resolve().parent
RUN = "science-2026.14.0"

# PLAN.md P10: the walls. law: what panel (b) draws, "ell" (the exponential's law reads ell, drawn
# with its ell(M) solved on V0 exp(M/phi)) or "log_R" (the power law's reads ln R; points only)
WALLS = {
    "exp": {
        "model": "exponential",
        "n": 1,
        "drain_model": "one-loop-exponential-v2",
        "store": f"{RUN}-exponential",
        "title": r"exponential, $n = 1$",
        "law": "ell",
        "extrapolated_below_lab": True,  # PLAN.md P4: law v2 beyond ln(V0/M^4) <= 10
    },
    "pl1": {
        "model": "inverse-power",
        "n": 1,
        "drain_model": "one-loop-inverse-power-v1",
        "store": f"{RUN}-inverse-power",
        "title": r"inverse power law, $n = 1$",
        "law": "log_R",
        "extrapolated_below_lab": False,
    },
    "pl2": {
        "model": "inverse-power",
        "n": 2,
        "drain_model": "one-loop-inverse-power-v1",
        "store": f"{RUN}-inverse-power",
        "title": r"inverse power law, $n = 2$",
        "law": "log_R",
        "extrapolated_below_lab": False,
    },
}
EXTRAPOLATED_COLOUR = "#fbe9c6"  # P4's region: law v2 beyond its fitted domain

M_REF = 4e-31
M_EV = {  # M / M_P -> the directory label, M in eV, of the exports
    1e-30: "0.0024353",
    4e-31: "0.00097413",
    1e-32: "2.4353e-05",
    1e-33: "2.4353e-06",
    1e-36: "2.4353e-09",
    1e-40: "2.4353e-13",
    1e-42: "2.4353e-15",
}
BETA_SURF = 1.06  # the wall is reached before BBN from here (Figure 6)
BETA_ELL = {
    1.2: "#984ea3",
    2.0: "#ff7f00",
    10.0: "#a65628",
}  # panel (b): not the A greens or the blues
ELL_MEASURED = 45.0  # 02d's largest one-loop ell (verification section 2d.5): 44.98

YP_OBS, YP_SIG = 0.245, 0.003  # PDG 2024
DH_OBS, DH_SIG = 2.527, 0.030  # Cooke et al. 2018, in 1e-5
DNEFF_MAX = 0.30  # Planck 2018 VI, section 7.5.2, one-tailed, TT,TE,EE+lowE+lensing+BAO
TY, TD = 100 * 2 * YP_SIG / YP_OBS, 100 * 2 * DH_SIG / DH_OBS

GREENS = {3.0: "#71d926", 5.0: "#30c26d", 10.0: "#2d6619"}  # as Figure 6
BAND_REDS = {  # the band's other A, light to dark
    0.05: "#f4b6be",
    0.1: "#ec96a2",
    0.2: "#e27585",
    0.5: "#d1495b",
    1.5: "#a52a3d",
    2.0: "#7a1e2c",
}


def colour(A: float) -> str:
    if np.isclose(A, 1.0):
        return "black"
    return GREENS.get(A) or BAND_REDS[A]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(*args) -> str:
    return subprocess.run(
        ["git", *args], cwd=HERE, capture_output=True, text=True, check=True
    ).stdout.strip()


def is_M(x, M):
    return np.isclose(x, M, rtol=1e-9, atol=0.0)


def load(path: Path, wall: dict) -> pd.DataFrame:
    H = pd.read_csv(path)
    H = H[(H.model == wall["model"]) & (H.n == wall["n"])]
    H = H[~H.failure_reasons.fillna("").str.startswith("no ScalarHistory")]
    H = H[H.M_Mp <= 1.1e-30].copy()
    sparse = set(H[is_M(H.M_Mp, 1e-33)].beta)
    H = H[H.beta.isin(sparse)]
    H["Mk"] = [min(M_EV, key=lambda m: abs(np.log(m / x))) for x in H.M_Mp]
    H = H[H.failure_reasons.isna() & H.Yp_BBN.notna() & (H.beta >= BETA_SURF)].copy()
    H["excluded"] = (
        ((100 * H.delta_Yp).abs() > TY)
        | ((100 * H.delta_D_over_H).abs() > TD)
        | (H.Delta_N_eff_today > DNEFF_MAX)
    )
    return H


def ell_law(log_E: float, ln_V0: float, M: float) -> float:
    """
    ell of AbstractModel.one_loop_turnaround on V = V0 exp(M/phi), solved here in closed form for the
    turnaround: with x = M/phi, V'' = V0 e^x x^3 (x + 2)/M^2 and the pulse rate |V'''/V''| =
    x (x^2 + 6x + 6)/((x + 2) M) (ExponentialModel._pulse_rate_hook at n = 1); m_ta^4 = 32 pi^2 E/ell,
    V''(phi_ta) = m_ta^2 and ell = ln(k_ex/m_ta), k_ex = E^{1/2} |V'''/V''|/2 (the k_ex ruling of 2026-11-16,
    power-law-wall 07b-07d), iterated to a fixed point. It reproduces the stored ell of every first impact this
    figure draws, to 1.4e-14 (checked 2026-11-22). Substituting the turnaround's ln M,
    ell = (1/2) ln(E/V0) - ln 2 + h(x), h = -ln(x)/2 + ln(x^2 + 6x + 6) - (3/2) ln(x + 2) - x/2, with
    dh/dx < -1/2 for every x > 0: as M falls x falls and ell rises without bound, so it has no maximum (the
    k_ex before 07b, lambda = x^2/M, gave one at x = sqrt(3) - 1).
    """
    ell = 32.0
    for _ in range(400):
        log_m = 0.25 * (np.log(32.0 * np.pi**2) + log_E - np.log(ell))
        x = brentq(
            lambda x: ln_V0
            + x
            + 3 * np.log(x)
            + np.log(x + 2)
            - 2 * np.log(M)
            - 2 * log_m,
            1e-300,
            1e4,
        )
        new = (
            np.log(x)
            + np.log(x * x + 6.0 * x + 6.0)
            - np.log(x + 2.0)
            - np.log(M)
            + 0.5 * log_E
            - np.log(2.0)
            - log_m
        )
        if abs(new - ell) <= 1e-12 * new:
            return new
        ell = new
    raise RuntimeError("ell_law did not converge")


def first_impact(bounces: Path, beta: float, M: float, n: int) -> pd.Series | None:
    """The first impact of the A = 1 history at (beta, M), or None where the export holds no bounces
    file for it (the history failed, so the store has no samples: pl1 at beta = 10, every M).
    """
    path = bounces / f"beta={beta:g}" / f"M={M_EV[M]}eV_n={n}" / "bounces.csv"
    if not path.exists():
        return None
    b = pd.read_csv(path)
    return b[b.delta_K_Mp4 > 0].iloc[0]


def first_impacts(bounces: Path, beta: float, Ms: list, n: int, missing: list):
    """The (M, first impact) pairs at beta that have a bounces file; each M without one is added to
    missing, and drawn as nothing."""
    pairs = []
    for M in Ms:
        r = first_impact(bounces, beta, M, n)
        if r is None:
            missing.append({"beta": beta, "M_Mp": M})
        else:
            pairs.append((M, r))
    return pairs


def main():
    parser = argparse.ArgumentParser(
        description="The trend in M of the 2026.14.0 science run, for one wall"
    )
    parser.add_argument("--wall", required=True, choices=sorted(WALLS))
    parser.add_argument(
        "--inputs",
        type=Path,
        help="the histories.csv to read (default ../fig6/inputs/<wall>/histories.csv)",
    )
    parser.add_argument(
        "--bounces",
        type=Path,
        help="the bounces directory (default inputs/<wall>/bounces)",
    )
    parser.add_argument(
        "--sidecar",
        type=Path,
        help="the store's sidecar (default ../registry/stores/<store>.manifest.json)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=HERE,
        help="where to write (default beside this file)",
    )
    args = parser.parse_args()
    wall = WALLS[args.wall]
    INPUT = args.inputs or HERE.parent / "fig6" / "inputs" / args.wall / "histories.csv"
    BOUNCES = args.bounces or HERE / "inputs" / args.wall / "bounces"
    SIDECAR = args.sidecar or (
        HERE.parent / "registry" / "stores" / f"{wall['store']}.manifest.json"
    )
    args.out.mkdir(parents=True, exist_ok=True)
    stem = f"m_trend-{args.wall}"
    n = wall["n"]
    extrapolated = wall["extrapolated_below_lab"]

    H = load(INPUT, wall)
    Ms = sorted(M_EV)
    L = H[H.drain_model == wall["drain_model"]]
    F = H[H.drain_model == "fixed"]
    As = sorted(L.alpha.unique())

    fig, (ax_a, ax_b, ax_c) = plt.subplots(
        3, 1, figsize=(7.2, 11.0), gridspec_kw={"height_ratios": [1.15, 1.0, 1.05]}
    )

    # (a) Delta N_eff today relative to 4e-31
    ref = L[is_M(L.Mk, M_REF)].set_index(["alpha", "beta"]).Delta_N_eff_today
    ratios = {}
    for A in As:
        med, lo, hi = [], [], []
        for M in Ms:
            s = L[is_M(L.Mk, M) & (L.alpha == A)].set_index(["alpha", "beta"])
            r = (s.Delta_N_eff_today / ref).dropna()
            med.append(r.median())
            lo.append(r.min())
            hi.append(r.max())
        ratios[A] = (med, lo, hi)
        lw = 1.6 if np.isclose(A, 1.0) else 1.1
        ls = (0, (8, 3)) if A in GREENS else "-"
        ax_a.plot(
            Ms, med, color=colour(A), lw=lw, ls=ls, marker="o", ms=3, label=f"A = {A:g}"
        )
        if np.isclose(A, 1.0):
            ax_a.fill_between(Ms, lo, hi, color="0.6", alpha=0.25, lw=0)
    fref = F[is_M(F.Mk, M_REF)].set_index(["alpha", "beta"]).Delta_N_eff_today
    fixed_dev = max(
        float(
            np.nanmax(
                np.abs(
                    F[is_M(F.Mk, M)].set_index(["alpha", "beta"]).Delta_N_eff_today
                    / fref
                    - 1
                )
            )
        )
        for M in Ms
    )
    ax_a.axhline(
        1.0,
        color="#2662d9",
        ls="-.",
        lw=1.0,
        label=rf"fixed law, $\alpha$ = 0.01 and 0.1 (within {fixed_dev:.0e})",
    )
    ax_a.set_ylabel(
        r"$\Delta N_{\rm eff}$ today, relative to $M = 4\times10^{-31}\,M_P$"
    )
    # what is drawn: every A's median, and the range over beta at A = 1
    drawn = [v for A in As for v in ratios[A][0]] + [
        v for A in As if np.isclose(A, 1.0) for v in ratios[A][1] + ratios[A][2]
    ]
    finite = [v for v in drawn if np.isfinite(v)]
    ax_a.set_ylim(
        min([0.38] + [0.95 * v for v in finite]),
        max([1.08] + [1.05 * v for v in finite]),
    )
    handles, labels = ax_a.get_legend_handles_labels()
    if extrapolated:  # PLAN.md P4
        from matplotlib.patches import Patch

        handles.append(Patch(facecolor=EXTRAPOLATED_COLOUR, edgecolor="#c9a24a"))
        labels.append(
            "$M < 4\\times10^{-31}\\,M_P$: law v2 beyond its fitted\n"
            "domain $\\ln(V_0/M^4) \\leq 10$ (extrapolated, user's ruling)"
        )
    ax_a.legend(handles, labels, fontsize=7, ncol=3, loc="lower left", frameon=False)
    ax_a.text(
        0.01,
        0.97,
        r"(a) median over the surfing $\beta$ ($\geq 1.06$); grey: range over $\beta$ at $A = 1$",
        transform=ax_a.transAxes,
        va="top",
        fontsize=8,
    )

    # (b) ell at the first impact and the law's ell(M) at that E; on the power wall,
    # ln R at the first impact, points only
    ell_table = {}
    log_R_table = {}
    missing = []  # (beta, M) with no bounces file: the history failed
    if wall["law"] == "log_R":
        for beta, c in BETA_ELL.items():
            pairs = first_impacts(BOUNCES, beta, Ms, n, missing)
            if not pairs:
                ax_b.plot(
                    [],
                    [],
                    color=c,
                    marker="o",
                    ms=4,
                    lw=0,
                    label=rf"$\beta = {beta:g}$: failed at every $M$, none drawn",
                )
                log_R_table[f"{beta:g}"] = {"log_R": {}, "no_history_at_any_M": True}
                continue
            Mb = [M for M, _ in pairs]
            logR = [float(r.log_R) for _, r in pairs]
            stored = []
            for M in Mb:  # histories.csv's log_R_first_impact, the same impact
                s = L[
                    np.isclose(L.beta, beta) & is_M(L.Mk, M) & np.isclose(L.alpha, 1.0)
                ]
                stored.append(
                    float(s.log_R_first_impact.iloc[0]) if len(s) else float("nan")
                )
            log_R_table[f"{beta:g}"] = {
                "log_R": dict(zip([f"{M:.0e}" for M in Mb], logR)),
                "histories_log_R_first_impact_max_abs_difference": float(
                    np.nanmax(np.abs(np.array(stored) - np.array(logR)))
                ),
            }
            ax_b.plot(
                Mb, logR, color=c, marker="o", ms=4, lw=0, label=rf"$\beta = {beta:g}$"
            )
        lo, hi = ax_b.get_ylim()
        ax_b.set_ylim(lo, hi + 0.2 * (hi - lo))  # headroom for the caption
        ax_b.set_ylabel(r"$\ln R$ at the first impact, $A = 1$")
        ax_b.legend(fontsize=8, loc="lower left", frameon=False)
        ax_b.text(
            0.01,
            0.97,
            r"(b) points: stored $\ln R$ at the first impact, the quantity the power wall's law reads",
            transform=ax_b.transAxes,
            va="top",
            fontsize=8,
        )
    for beta, c in BETA_ELL.items() if wall["law"] == "ell" else ():
        pairs = first_impacts(BOUNCES, beta, Ms, n, missing)
        if not pairs:
            ax_b.plot(
                [],
                [],
                color=c,
                marker="o",
                ms=4,
                lw=0,
                label=rf"$\beta = {beta:g}$: failed at every $M$, none drawn",
            )
            ell_table[f"{beta:g}"] = {"ell": {}, "no_history_at_any_M": True}
            continue
        Mb = [M for M, _ in pairs]
        rows = [r for _, r in pairs]
        ells = [r.ell for r in rows]
        lnV0 = float(
            H[
                np.isclose(H.beta, beta)
                & is_M(H.Mk, M_REF)
                & (H.drain_model != "fixed")
                & np.isclose(H.alpha, 1.0)
            ].normalization.iloc[0]
        )
        log_E = float(np.median([r.log_E_Mp4 for r in rows]))
        M_curve = np.logspace(-42.5, -29.5, 140)
        ell_curve = [ell_law(log_E, lnV0, M) for M in M_curve]
        ell_check = [ell_law(r.log_E_Mp4, lnV0, M) for r, M in zip(rows, Mb)]
        ell_table[f"{beta:g}"] = {
            "ell": dict(zip([f"{M:.0e}" for M in Mb], ells)),
            "ell_law_max_abs_difference": float(
                np.max(np.abs(np.array(ell_check) - np.array(ells)))
            ),
            "ln_E_over_V0": log_E - lnV0,
            "ell_law_below_sample": {
                f"{M:.0e}": ell_law(log_E, lnV0, M)
                for M in (1e-44, 1e-46, 1e-50, 1e-60)
            },
        }
        ax_b.plot(M_curve, ell_curve, color=c, lw=0.8, alpha=0.8)
        ax_b.plot(
            Mb, ells, color=c, marker="o", ms=4, lw=0, label=rf"$\beta = {beta:g}$"
        )
    if wall["law"] == "ell":
        ax_b.axhspan(ELL_MEASURED, 80, color="0.93", lw=0, zorder=0)
        ax_b.set_ylabel(
            r"$\ell = \ln(k_{\rm ex}/m_{\rm ta})$ at the first impact, $A = 1$"
        )
        ax_b.set_ylim(28, 74)
        ax_b.legend(fontsize=8, loc="lower left", frameon=False)
        ax_b.text(
            0.01,
            0.97,
            r"(b) points: stored; line: the law's $\ell(M)$ at that impact's $E$, with"
            + "\n"
            + r"    $k_{\rm ex} = \frac{1}{2}E^{1/2}|V'''/V''|$; it rises without bound as $M$ falls (no maximum)"
            + "\n"
            + rf"    grey: $\ell > {ELL_MEASURED:g}$, beyond the range the law was measured on",
            transform=ax_b.transAxes,
            va="top",
            fontsize=8,
        )

    # (c) the largest excluded beta at each (A, M)
    grid = np.full((len(As), len(Ms)), np.nan)
    counts = np.zeros((len(As), len(Ms)), dtype=int)
    n_beta = L.beta.nunique()
    for i, A in enumerate(As):
        for j, M in enumerate(Ms):
            s = L[is_M(L.Mk, M) & (L.alpha == A)]
            e = s[s.excluded]
            counts[i, j] = len(e)
            if len(e):
                grid[i, j] = e.beta.max()
    im = ax_c.imshow(
        grid, aspect="auto", cmap="YlOrRd", vmin=1.1, vmax=1.9, origin="lower"
    )
    for i in range(len(As)):
        for j in range(len(Ms)):
            txt = "—" if np.isnan(grid[i, j]) else f"{grid[i, j]:.2f}\n({counts[i, j]})"
            ax_c.text(
                j,
                i,
                txt,
                ha="center",
                va="center",
                fontsize=6.5,
                color="black" if np.isnan(grid[i, j]) or grid[i, j] < 1.6 else "white",
            )
    ax_c.set_xticks(range(len(Ms)))
    ax_c.set_xticklabels([f"{M:.0e}".replace("e-", "e−") for M in Ms], fontsize=8)
    ax_c.set_yticks(range(len(As)))
    ax_c.set_yticklabels([f"A = {A:g}" for A in As], fontsize=8)
    for lab, A in zip(ax_c.get_yticklabels(), As):
        lab.set_color(colour(A) if A in GREENS or np.isclose(A, 1.0) else "#a52a3d")
    ax_c.set_xlabel(r"$M / M_P$")
    cb = fig.colorbar(im, ax=ax_c, pad=0.02, fraction=0.04)
    cb.set_label(r"largest excluded $\beta$", fontsize=8)
    ax_c.set_title(
        rf"(c) largest excluded $\beta$ (count of the {n_beta} surfing $\beta$ excluded); "
        + "tests as Figure 6"
        + (
            "\n    columns left of the laboratory mass: law v2 beyond its fitted domain"
            if extrapolated
            else ""
        ),
        fontsize=8,
        loc="left",
    )
    ax_c.axvline(Ms.index(M_REF) - 0.5, color="0.3", lw=0.8)
    ax_c.axvline(Ms.index(M_REF) + 0.5, color="0.3", lw=0.8)

    for ax in (ax_a, ax_b):
        ax.set_xscale("log")
        ax.set_xlim(3e-43, 3e-30)
        ax.axvline(M_REF, color="0.35", ls=":", lw=0.9)
        ax.grid(True, which="major", alpha=0.3)
        if (
            extrapolated
        ):  # PLAN.md P4, marked as panel (b) marks ell beyond its measured range
            ax.axvspan(1e-50, M_REF, color=EXTRAPOLATED_COLOUR, lw=0, zorder=0)
    ax_a.tick_params(labelbottom=False)
    ax_b.set_xlabel(r"$M / M_P$")
    ax_a.text(
        M_REF / 1.3,
        0.40,
        "laboratory mass",
        fontsize=7,
        color="0.35",
        va="bottom",
        ha="right",
    )
    fig.suptitle(
        rf"The trend in $M$, {wall['title']}, one-loop law, $\phi_* = 5\,M_P$, sparse $\beta$ grid",
        fontsize=10,
        y=0.995,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.985), h_pad=0.6)

    head = git("rev-parse", "HEAD")
    dirty = bool(git("status", "--porcelain", "--untracked-files=no"))
    if SIDECAR.exists():
        sidecar = json.loads(SIDECAR.read_text())
    else:  # testing on an export whose store has no sidecar here
        print(f"!! no sidecar at {SIDECAR}; the provenance records none")
        sidecar = {"name": None, "fingerprint": {"digest": None}}
    meta = {
        "Title": f"The trend in M, 2026.14.0 science run, wall {args.wall}",
        "Subject": f"histories.csv sha256 {sha256(INPUT)}; store digest {sidecar['fingerprint']['digest']}; "
        f"git {head}{' (dirty)' if dirty else ''}",
    }
    fig.savefig(args.out / f"{stem}.pdf", metadata=meta)
    fig.savefig(args.out / f"{stem}.png", dpi=200)

    bounce_files = sorted(BOUNCES.rglob("bounces.csv"))
    provenance = {
        "script": "m_trend.py",
        "script_sha256": sha256(Path(__file__)),
        "git_head": head,
        "git_dirty": dirty,
        "wall": args.wall,
        "model": wall["model"],
        "n": n,
        "drain_model": wall["drain_model"],
        "panel_b": wall["law"],
        "extrapolated_below_lab": extrapolated,
        "inputs": {
            "histories": {
                "path": os.path.relpath(INPUT, HERE),
                "sha256": sha256(INPUT),
            },
            "bounces": {str(p.relative_to(BOUNCES)): sha256(p) for p in bounce_files},
        },
        "store": {
            "name": sidecar["name"],
            "store_id": sidecar.get("store_id"),
            "digest": sidecar["fingerprint"]["digest"],
        },
        "M_Mp": Ms,
        "betas_surfing": sorted(L.beta.unique()),
        "A_values": As,
        "dNeff_today_ratio_median_by_A": {
            f"{A:g}": dict(zip([f"{M:.0e}" for M in Ms], ratios[A][0])) for A in As
        },
        "fixed_law_max_relative_deviation": fixed_dev,
        "ell_first_impact_A1": ell_table,
        "log_R_first_impact_A1": log_R_table,
        "first_impact_missing": [
            {"beta": m["beta"], "M_Mp": f"{m['M_Mp']:.0e}"} for m in missing
        ],
        "largest_excluded_beta": {
            f"{A:g}": {
                f"{M:.0e}": (None if np.isnan(grid[i, j]) else float(grid[i, j]))
                for j, M in enumerate(Ms)
            }
            for i, A in enumerate(As)
        },
        "excluded_count": {
            f"{A:g}": {f"{M:.0e}": int(counts[i, j]) for j, M in enumerate(Ms)}
            for i, A in enumerate(As)
        },
    }
    (args.out / f"{stem}.provenance.json").write_text(
        json.dumps(provenance, indent=2) + "\n"
    )
    print(
        json.dumps(
            {
                k: provenance[k]
                for k in (
                    "git_head",
                    "git_dirty",
                    "ell_first_impact_A1",
                    "log_R_first_impact_A1",
                    "first_impact_missing",
                )
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
