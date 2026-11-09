"""
Figure 6 of the 2026.10.0 science run: the shifts in Y_p and D/H, Delta N_eff today and the
temperature at which the field parks, against beta at the laboratory mass M = 4e-31 M_P, under the
one-loop drain law (one-loop-measured-v1) with a band over its strength A = 0.05-3, the black line
A = 1, green long-dashed lines at A = 3, 5 and 10 (the exclusions at 5 and 10 come from the dark
radiation present during BBN), and the fixed law at alpha = 0.01 and 0.1 as blue dash-dot lines
(the user's rulings of 2026-11-09).

    ./venv/bin/python science-runs/2026-11-08-science-2026.10.0/fig6/fig6.py [--points]

Reads only inputs/histories.csv beside this file (an unchanged copy of the run's plot_by_beta.py
export of the main store) and ../registry/stores/science-2026.10.0.manifest.json (the store's
fingerprint). Writes fig6.pdf, fig6.png and fig6.provenance.json beside this file, without a point
for each one-loop history (the user's choice of 2026-11-09); --points adds them and writes
fig6-points.pdf, .png and .provenance.json instead. Needs no Ray and no store.

Adapted from the 2026.8.0 Figure 6 (../../2026-11-01-science-2026.8.0/fig6/fig6.py), whose design
the user ruled on 2026-11-01 and 2026-11-02: shifts relative to PRyMordial + LambdaCDM (PRyMordial
with rho_NP = 0, at the same version, flags and small network as each history's BBN), differential
and two-sided tests at twice the measurement's relative 1-sigma, |dY_p| <= 2 x 0.003/0.245
(PDG 2024) and |d(D/H)| <= 2 x 0.030/2.527 (Cooke et al. 2018), and the parking row against an
indicative BBN window. Delta N_eff today is drawn against Planck 2018's one-tailed bound for
Delta N_eff > 0, Delta N_eff < 0.30 at 95% (TT,TE,EE+lowE+lensing+BAO; Planck 2018 VI, A&A 641, A6,
section 7.5.2), in place of the 2026.8.0 figure's 2 x 0.17 (the user's ruling of 2026-11-09).
"""

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import LogNorm

HERE = Path(__file__).resolve().parent
INPUT = HERE / "inputs" / "histories.csv"
SIDECAR = HERE.parent / "registry" / "stores" / "science-2026.10.0.manifest.json"

M_LAB = 4e-31
ONE_LOOP = "one-loop-measured-v1"
A_LINE = 1.0  # the one-loop law at its computed strength
A_BAND_MAX = 3.0  # the band spans A <= 3; the larger A are drawn as lines
LONG_DASH = (0, (8, 3))
# The greens and blues were chosen on 2026-11-09 so that the darkest of each (A = 10, alpha = 0.1) is
# well away from the black A = 1 line (CIELAB Delta E 62 and 84, against 43 and 45 before) while each
# group stays as spread as before (closest pair Delta E 39 in the greens, 57 in the blues)
A_LARGE = {  # A -> (colour, line style): green, long dashes (the user's request of 2026-11-09)
    3.0: ("#71d926", LONG_DASH),  # the largest A in the band
    5.0: ("#30c26d", LONG_DASH),  # 5 and 10: exclusions by dark radiation during BBN
    10.0: ("#2d6619", LONG_DASH),
}
FIXED = {  # the fixed law beside it: alpha -> (colour, line style); blue, dash-dot (2026-11-09)
    0.1: ("#2662d9", "-."),
    0.01: ("#539fc6", "-."),
}
PLANCK_COLOUR = (
    "0.25"  # grey, so that it is not read as one of the fixed law's blue lines
)
BETA_QCD = 1.1074  # the analytic QCD surfing threshold
EW_BRACKET = (
    3.000,
    3.005,
)  # where the delivery temperature jumps at laboratory M (this run)
T_WALL_GEV = (
    1e-2  # a history delivered below 10 MeV has not reached the wall before BBN
)
T_BBN_GEV = (2e-5, 2e-3)  # an indicative BBN window, 20 keV to 2 MeV

YP_OBS, YP_SIG = 0.245, 0.003  # PDG 2024
DH_OBS, DH_SIG = 2.527, 0.030  # Cooke et al. 2018, in 1e-5
# Planck 2018 VI, section 7.5.2: for Delta N_eff > 0, the one-tailed TT,TE,EE+lowE+lensing+BAO bound
DNEFF_MAX = 0.30  # Delta N_eff < 0.30 at 95%
TOL = {
    "dYp": 100 * 2 * YP_SIG / YP_OBS,
    "dDH": 100 * 2 * DH_SIG / DH_OBS,
    "dNeff": DNEFF_MAX,
}

COLUMNS = [  # (x range, scale, width ratio)
    ((0.45, 1.05), "linear", 0.55),
    ((1.05, 1.52), "linear", 2.2),
    ((1.52, 3.07), "linear", 1.25),
    ((3.07, 22.0), "log", 0.8),
]
ROWS = [  # (column, axis label, y range, scale)
    ("dYp", r"$\Delta Y_p$ [%]", (-1.5, 12.5), "linear"),
    ("dDH", r"$\Delta(\mathrm{D/H})$ [%]", (-3.0, 23.0), "linear"),
    ("dNeff", r"$\Delta N_{\rm eff}$ today", (1e-4, 1.0), "log"),
    ("parkT", r"$T_J$ at parking [GeV]", (1e-13, 1e3), "log"),
]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(*args) -> str:
    return subprocess.run(
        ["git", *args], cwd=HERE, capture_output=True, text=True, check=True
    ).stdout.strip()


def load() -> pd.DataFrame:
    H = pd.read_csv(INPUT)
    H = H[np.isclose(H.M_Mp, M_LAB, rtol=1e-9, atol=0.0)].copy()
    H = H[~H.failure_reasons.fillna("").str.startswith("no ScalarHistory")]
    H["ok"] = H.failure_reasons.isna() & H.Yp_BBN.notna()
    H["dYp"] = 100 * H.delta_Yp
    H["dDH"] = 100 * H.delta_D_over_H
    H["dNeff"] = H.Delta_N_eff_today
    H["parkT"] = H.parking_T_GeV
    return H


def band(L: pd.DataFrame, col: str) -> pd.DataFrame:
    """The range over A <= A_BAND_MAX at each beta where every such A of the run was stored (182 of the 186)."""
    L = L[L.alpha <= A_BAND_MAX]
    ok = L[L.ok]
    n_run = L.groupby("beta").alpha.nunique()
    full = n_run[n_run == n_run.max()].index
    g = ok[ok.beta.isin(full)].groupby("beta")[col]
    return pd.DataFrame({"lo": g.min(), "hi": g.max(), "n": g.size()})


def exclusions(ok: pd.DataFrame) -> dict:
    y = ok.dYp.abs() > TOL["dYp"]
    d = ok.dDH.abs() > TOL["dDH"]
    n = ok.dNeff > TOL["dNeff"]
    return {
        "histories": int(len(ok)),
        "dYp": int(y.sum()),
        "dDH": int(d.sum()),
        "dNeff": int(n.sum()),
        "any": int((y | d | n).sum()),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Figure 6 of the 2026.10.0 science run"
    )
    parser.add_argument(
        "--points",
        action="store_true",
        help="add a point for each one-loop history; write fig6-points.*",
    )
    args = parser.parse_args()
    stem = "fig6-points" if args.points else "fig6"

    H = load()
    L = H[H.drain_model == ONE_LOOP]
    F = H[H.drain_model == "fixed"]
    okL, okF = L[L.ok], F[F.ok]
    nobbn = sorted(set(L.beta) - set(okL.beta))  # stored, but no A has a BBN result
    td = H[H.T_deliver_GeV.notna()].groupby("beta").T_deliver_GeV.first()
    b_wall = 0.5 * (td[td < T_WALL_GEV].index.max() + td[td >= T_WALL_GEV].index.min())
    As = sorted(okL.alpha.unique())
    norm = LogNorm(vmin=min(As), vmax=max(As))
    cmap = plt.get_cmap("viridis")

    fig = plt.figure(figsize=(9.0, 10.9))
    gs = fig.add_gridspec(
        len(ROWS),
        len(COLUMNS),
        width_ratios=[c[2] for c in COLUMNS],
        wspace=0.06,
        hspace=0.1,
    )

    for i, (col, ylabel, ylim, yscale) in enumerate(ROWS):
        B = band(L, col)
        for j, (xlim, xscale, _) in enumerate(COLUMNS):
            A = fig.add_subplot(gs[i, j])
            A.set_xscale(xscale)
            A.set_yscale(yscale)
            A.set_xlim(*xlim)
            A.set_ylim(*ylim)

            # the regions
            A.axvspan(0.0, b_wall, color="#f3e6d0", zorder=0, lw=0)
            A.axvspan(
                EW_BRACKET[0] + 0.5 * (EW_BRACKET[1] - EW_BRACKET[0]),
                100,
                color="0.93",
                zorder=0,
                lw=0,
            )
            A.axvline(BETA_QCD, color="0.35", ls="--", lw=0.8, zorder=1)
            A.axvline(0.5 * sum(EW_BRACKET), color="0.35", ls="-.", lw=0.8, zorder=1)

            # the observational tolerance; for the parking temperature, the BBN window
            if col not in TOL:
                A.axhspan(*T_BBN_GEV, color="#d9ead3", zorder=0.5, lw=0)
            elif yscale == "linear":
                A.axhspan(-TOL[col], TOL[col], color="#c9d8e8", zorder=0.5, lw=0)
                A.axhline(0, color="0.45", lw=0.6, ls=":")
            else:
                A.axhline(TOL[col], color=PLANCK_COLOUR, lw=1.0, ls="--", zorder=1)

            # every one-loop history, coloured by A; the envelope over A; the line at A = 1
            for a in As if args.points else []:
                s = okL[okL.alpha == a]
                y = s[col].clip(*ylim) if yscale == "linear" else s[col]
                A.scatter(
                    s.beta, y, s=3, color=cmap(norm(a)), alpha=0.55, lw=0, zorder=3
                )
            # the band and the lines are split at the wall's edge, so that the non-surfing
            # samples are not joined to the surfing ones
            for start, stop in _runs(B.index.values, b_wall):
                seg = B.loc[start:stop]
                A.fill_between(
                    seg.index,
                    seg.lo.clip(*ylim),
                    seg.hi.clip(*ylim),
                    color="#d1495b",
                    alpha=0.18,
                    lw=0,
                    zorder=2,
                )
                A.plot(seg.index, seg.lo.clip(*ylim), color="#d1495b", lw=0.6, zorder=2)
                A.plot(seg.index, seg.hi.clip(*ylim), color="#d1495b", lw=0.6, zorder=2)
            s = okL[np.isclose(okL.alpha, A_LINE)].set_index("beta").sort_index()
            for start, stop in _runs(s.index.values, b_wall):
                seg = s.loc[start:stop]
                A.plot(seg.index, seg[col].clip(*ylim), color="black", lw=0.9, zorder=4)

            # the large A, outside the band: excluded by the dark radiation present during BBN
            for a_large, (colour, ls) in A_LARGE.items():
                s = okL[np.isclose(okL.alpha, a_large)].set_index("beta").sort_index()
                for start, stop in _runs(s.index.values, b_wall):
                    seg = s.loc[start:stop]
                    A.plot(
                        seg.index,
                        seg[col].clip(*ylim),
                        color=colour,
                        ls=ls,
                        lw=1.0,
                        zorder=4,
                    )

            # the fixed law beside it
            for alpha, (colour, ls) in FIXED.items():
                s = okF[np.isclose(okF.alpha, alpha)].set_index("beta").sort_index()
                for start, stop in _runs(s.index.values, b_wall):
                    seg = s.loc[start:stop]
                    A.plot(
                        seg.index,
                        seg[col].clip(*ylim),
                        color=colour,
                        ls=ls,
                        lw=1.0,
                        zorder=4,
                    )

            inside = lambda x: (np.asarray(x) >= xlim[0]) & (np.asarray(x) <= xlim[1])
            # values above the axis, as triangles at its top
            top = okL.groupby("beta")[col].max()
            top = top[(top > ylim[1]) & inside(top.index)]
            A.scatter(
                top.index,
                np.full(len(top), ylim[1]),
                marker="^",
                s=22,
                color="#d1495b",
                clip_on=False,
                zorder=5,
            )
            # beta with no BBN result at any A
            ymark = ylim[0] if yscale == "linear" else 1.6 * ylim[0]
            xs = np.asarray(nobbn)[inside(nobbn)]
            A.scatter(
                xs,
                np.full(len(xs), ymark),
                marker="x",
                s=22,
                color="0.2",
                clip_on=False,
                zorder=5,
            )

            if j == 0:
                A.set_ylabel(ylabel)
            else:
                A.tick_params(labelleft=False)
                A.spines["left"].set_linestyle((0, (2, 2)))
            if j < len(COLUMNS) - 1:
                A.spines["right"].set_visible(False)
            if i < len(ROWS) - 1:
                A.tick_params(labelbottom=False)
            else:
                A.set_xlabel(r"$\beta$" if j == 1 else "")
            if xscale == "log":
                A.set_xticks([5, 10, 20])
                A.set_xticklabels(["5", "10", "20"])
                A.xaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
            if j == 1:
                A.set_xticks([1.1, 1.2, 1.3, 1.4, 1.5])
            if j == 2:
                A.set_xticks([2.0, 2.5, 3.0])
            if j == 0:
                A.set_xticks([0.5, 0.75, 1.0])

    # the legend, under the figure
    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch

    A_band = [a for a in As if a <= A_BAND_MAX]
    A_text = ", ".join(f"{a:g}" for a in A_band)
    handles = [
        Patch(
            color="#d1495b",
            alpha=0.35,
            label=rf"one-loop law: range over $A$ = {A_text}",
        ),
    ]
    if args.points:
        handles += [
            Line2D(
                [],
                [],
                marker="o",
                ms=3,
                color="#3b8f88",
                ls="",
                label=rf"each one-loop history, coloured by $\log A$ (dark: {min(As):g}, yellow: {max(As):g})",
            )
        ]
    handles += [
        Line2D([], [], color="black", lw=0.9, label=rf"one-loop law, $A = {A_LINE:g}$"),
    ]
    handles += [
        Line2D(
            [],
            [],
            color=colour,
            ls=ls,
            lw=1.0,
            label=rf"one-loop law, $A = {a_large:g}$"
            + (
                " (largest A in the band)"
                if a_large <= A_BAND_MAX
                else " (dark radiation during BBN)"
            ),
        )
        for a_large, (colour, ls) in A_LARGE.items()
    ]
    handles += [
        Line2D(
            [],
            [],
            color=colour,
            ls=ls,
            lw=1.0,
            label=rf"fixed law, $\alpha = {alpha:g}$",
        )
        for alpha, (colour, ls) in FIXED.items()
    ]
    handles += [
        Patch(
            color="#c9d8e8",
            label=r"$\pm 2\sigma$ of the measurement, about PRyMordial + $\Lambda$CDM",
        ),
        Line2D(
            [],
            [],
            color=PLANCK_COLOUR,
            ls="--",
            lw=1.0,
            label=rf"Planck 2018: $\Delta N_{{\rm eff}} < {TOL['dNeff']:.2f}$ (95%, one-tailed, +lensing+BAO)",
        ),
        Patch(color="#d9ead3", label="BBN, 2 MeV – 20 keV (indicative; parking row)"),
        Patch(
            color="#f3e6d0",
            label=rf"not at the wall before BBN ($T_{{\rm deliver}} < 10$ MeV; $\beta < {b_wall:.4g}$)",
        ),
        Patch(
            color="0.93",
            label=rf"surfs the electroweak kick ($\beta > {EW_BRACKET[0]:.3f}$–${EW_BRACKET[1]:.3f}$)",
        ),
        Line2D(
            [],
            [],
            color="0.35",
            ls="--",
            lw=0.8,
            label=rf"analytic QCD surfing threshold $\beta = {BETA_QCD}$",
        ),
        Line2D(
            [],
            [],
            marker="x",
            color="0.2",
            ls="",
            label="no BBN result at any A (BBN temperature below PRyMordial's floor, or no history)",
        ),
        Line2D([], [], marker="^", color="#d1495b", ls="", label="band above the axis"),
    ]
    fig.legend(
        handles=handles,
        loc="lower center",
        ncol=2,
        fontsize=7,
        frameon=False,
        bbox_to_anchor=(0.5, 0.0),
    )
    fig.suptitle(
        rf"$M = 4\times10^{{-31}}\,M_P$, $\phi_* = 5\,M_P$: shifts relative to PRyMordial + $\Lambda$CDM",
        fontsize=10,
        y=0.995,
    )
    fig.subplots_adjust(bottom=0.185, top=0.955, left=0.08, right=0.99)

    head = git("rev-parse", "HEAD")
    # tracked files only: this script's own outputs are new files until they are committed
    dirty = bool(git("status", "--porcelain", "--untracked-files=no"))
    sidecar = json.loads(SIDECAR.read_text())
    meta = {
        "Title": "Figure 6, 2026.10.0 science run",
        "Subject": f"histories.csv sha256 {sha256(INPUT)}; store digest {sidecar['fingerprint']['digest']}; "
        f"git {head}{' (dirty)' if dirty else ''}",
    }
    fig.savefig(HERE / f"{stem}.pdf", metadata=meta)
    fig.savefig(HERE / f"{stem}.png", dpi=200)

    by_A = {f"{a:g}": exclusions(okL[okL.alpha == a]) for a in As}
    provenance = {
        "script": "fig6.py",
        "points": args.points,
        "script_sha256": sha256(Path(__file__)),
        "git_head": head,
        "git_dirty": dirty,
        "input": {"path": "inputs/histories.csv", "sha256": sha256(INPUT)},
        "store": {
            "name": sidecar["name"],
            "store_id": sidecar.get("store_id"),
            "digest": sidecar["fingerprint"]["digest"],
        },
        "M_Mp": M_LAB,
        "drain_model": ONE_LOOP,
        "A_values": As,
        "A_band": [a for a in As if a <= A_BAND_MAX],
        "A_lines": [A_LINE, *A_LARGE],
        "fixed_alpha": sorted(FIXED),
        "tolerances_percent_or_absolute": TOL,
        "beta_wall_edge": b_wall,
        "betas_without_BBN": nobbn,
        "histories_without_parking": int(
            okL.parkT.isna().sum() + okF.parkT.isna().sum()
        ),
        "A_count_by_beta": {
            f"{b:g}": int(n) for b, n in okL.groupby("beta").alpha.nunique().items()
        },
        "excluded_one_loop": exclusions(okL),
        "excluded_one_loop_by_A": by_A,
        "excluded_fixed_by_alpha": {
            f"{a:g}": exclusions(okF[np.isclose(okF.alpha, a)]) for a in sorted(FIXED)
        },
    }
    (HERE / f"{stem}.provenance.json").write_text(
        json.dumps(provenance, indent=2) + "\n"
    )
    print(
        json.dumps(
            {
                k: provenance[k]
                for k in (
                    "git_head",
                    "git_dirty",
                    "beta_wall_edge",
                    "excluded_one_loop",
                    "excluded_one_loop_by_A",
                    "excluded_fixed_by_alpha",
                )
            },
            indent=2,
        )
    )


def _runs(betas, split=None):
    """The runs of consecutive grid points, so that a line is not drawn across a beta with no
    result: a gap is a step more than three times the median local spacing. A run also ends at
    `split` (the wall's edge), if one is given."""
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


if __name__ == "__main__":
    main()
