"""
Figure 6b of the 2026.10.0 science run: Figure 6 around M_crit. The shifts in Y_p and D/H, Delta N_eff
today and ln R at the first impact, against beta, for the one-loop drain law at A = 1 at M = 1e-15 and
1e-12 M_P (block C, the sparse beta grid), beside the laboratory mass's A = 1 (Figure 6's black line) and
the fixed law at alpha = 0.1 and 0.01, which is the same at all three M and is drawn once.

    ./venv/bin/python science-runs/2026-11-08-science-2026.10.0/fig6b/fig6b.py

Reads ../fig6/inputs/histories.csv (the run's plot_by_beta.py export of the main store, committed with
Figure 6) and ../registry/stores/science-2026.10.0.manifest.json. Writes fig6b.pdf, .png and
.provenance.json beside this file. Needs no Ray and no store.

The design is the user's of 2026-11-09: Figure 6's columns and tests, its first three rows, ln R at the
first impact in place of the parking row, the fixed law drawn once. The tests are Figure 6's: |dY_p| and
|d(D/H)| within twice the measurement's relative 1-sigma about PRyMordial + LambdaCDM, and Planck 2018's
one-tailed Delta N_eff < 0.30. In the ln R row each point is marked by the regime that drained most of
the history's dark radiation (one-loop, gap or classical; one-loop-drain README section 8.10).
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

HERE = Path(__file__).resolve().parent
INPUT = HERE.parent / "fig6" / "inputs" / "histories.csv"
SIDECAR = HERE.parent / "registry" / "stores" / "science-2026.10.0.manifest.json"

M_LAB = 4e-31
ONE_LOOP = "one-loop-measured-v1"
A_LINE = 1.0
M_CRIT = {  # M -> colour
    1e-15: "#e7298a",
    1e-12: "#d95f02",
}
FIXED = {  # as Figure 6
    0.1: ("#2662d9", "-."),
    0.01: ("#539fc6", "-."),
}
REGIME_MARKER = {"one_loop": "o", "gap": "s", "classical": "^"}
BETA_QCD = 1.1074
EW_BRACKET = (
    3.000,
    3.005,
)  # the delivery temperatures at M_crit equal the laboratory mass's to 4e-7
T_WALL_GEV = 1e-2
PLANCK_COLOUR = "0.25"

YP_OBS, YP_SIG = 0.245, 0.003  # PDG 2024
DH_OBS, DH_SIG = 2.527, 0.030  # Cooke et al. 2018, in 1e-5
DNEFF_MAX = 0.30  # Planck 2018 VI, section 7.5.2, one-tailed, TT,TE,EE+lowE+lensing+BAO
TOL = {
    "dYp": 100 * 2 * YP_SIG / YP_OBS,
    "dDH": 100 * 2 * DH_SIG / DH_OBS,
    "dNeff": DNEFF_MAX,
}

COLUMNS = [  # as Figure 6
    ((0.45, 1.05), "linear", 0.55),
    ((1.05, 1.52), "linear", 2.2),
    ((1.52, 3.07), "linear", 1.25),
    ((3.07, 22.0), "log", 0.8),
]
ROWS = [
    ("dYp", r"$\Delta Y_p$ [%]", (-1.5, 12.5), "linear"),
    ("dDH", r"$\Delta(\mathrm{D/H})$ [%]", (-3.0, 23.0), "linear"),
    ("dNeff", r"$\Delta N_{\rm eff}$ today", (1e-9, 1.0), "log"),
    ("lnR", r"$\ln\mathcal{R}$ at the first impact", (-40.0, 32.0), "linear"),
]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(*args) -> str:
    return subprocess.run(
        ["git", *args], cwd=HERE, capture_output=True, text=True, check=True
    ).stdout.strip()


def is_M(x, M):
    return np.isclose(x, M, rtol=1e-9, atol=0.0)


def load() -> pd.DataFrame:
    H = pd.read_csv(INPUT)
    H = H[~H.failure_reasons.fillna("").str.startswith("no ScalarHistory")]
    H = H[is_M(H.M_Mp, M_LAB) | is_M(H.M_Mp, 1e-15) | is_M(H.M_Mp, 1e-12)].copy()
    H["ok"] = H.failure_reasons.isna() & H.Yp_BBN.notna()
    H["dYp"] = 100 * H.delta_Yp
    H["dDH"] = 100 * H.delta_D_over_H
    H["dNeff"] = H.Delta_N_eff_today
    H["lnR"] = H.log_R_first_impact
    shares = H[
        ["drained_share_one_loop", "drained_share_gap", "drained_share_classical"]
    ].fillna(0.0)
    H["regime"] = np.where(
        shares.sum(axis=1) > 0,
        shares.idxmax(axis=1).str.replace("drained_share_", ""),
        "none",
    )
    H["excluded"] = (
        (H.dYp.abs() > TOL["dYp"])
        | (H.dDH.abs() > TOL["dDH"])
        | (H.dNeff > TOL["dNeff"])
    )
    return H


def runs(betas, split):
    """Consecutive grid points, split at the wall's edge and across gaps below beta = 3.07 (as Figure 6)."""
    b = np.sort(betas)
    if len(b) < 2:
        return [(b[0], b[0])] if len(b) else []
    steps = np.diff(b)
    out, start = [], b[0]
    for k, d in enumerate(steps):
        local = np.median(steps[max(0, k - 3) : k + 4])
        # a gap is looked for only on the linear columns; the log column's tail grid is sparse by design
        if (b[k] < split <= b[k + 1]) or (
            d > 3 * local and d > 0.05 and b[k + 1] < 3.07
        ):
            out.append((start, b[k]))
            start = b[k + 1]
    out.append((start, b[-1]))
    return out


def draw_line(A, s, col, ylim, b_wall, **kw):
    s = s.set_index("beta").sort_index()
    for start, stop in runs(s.index.values, b_wall):
        seg = s.loc[start:stop]
        A.plot(seg.index, seg[col].clip(*ylim), **kw)


def main():
    H = load()
    ok = H[H.ok]
    lab = ok[
        is_M(ok.M_Mp, M_LAB)
        & (ok.drain_model == ONE_LOOP)
        & np.isclose(ok.alpha, A_LINE)
    ]
    fixed = ok[is_M(ok.M_Mp, M_LAB) & (ok.drain_model == "fixed")]
    crit = {
        M: H[
            is_M(H.M_Mp, M) & (H.drain_model == ONE_LOOP) & np.isclose(H.alpha, A_LINE)
        ]
        for M in M_CRIT
    }
    td = H[H.T_deliver_GeV.notna()].groupby("beta").T_deliver_GeV.first()
    b_wall = 0.5 * (td[td < T_WALL_GEV].index.max() + td[td >= T_WALL_GEV].index.min())

    # the fixed law's dependence on M, for the notes: every stored (alpha, beta) at the three M
    F = ok[ok.drain_model == "fixed"]
    Fl = F[is_M(F.M_Mp, M_LAB)].set_index(["alpha", "beta"])
    fixed_dev = {}
    for M in M_CRIT:
        j = (
            F[is_M(F.M_Mp, M)]
            .set_index(["alpha", "beta"])
            .join(Fl, rsuffix="_lab", how="inner")
        )
        fixed_dev[f"{M:.0e}"] = {
            "histories": int(len(j)),
            "max_abs_dYp_percent": float(np.max(np.abs(j.dYp - j.dYp_lab))),
            "max_abs_dDH_percent": float(np.max(np.abs(j.dDH - j.dDH_lab))),
            "max_rel_dNeff": float(np.max(np.abs(j.dNeff / j.dNeff_lab - 1))),
            "max_rel_dNeff_surfing": float(
                np.max(
                    np.abs(
                        (j.dNeff / j.dNeff_lab - 1)[
                            j.index.get_level_values("beta") >= b_wall
                        ]
                    )
                )
            ),
        }

    fig = plt.figure(figsize=(9.0, 10.9))
    gs = fig.add_gridspec(
        len(ROWS),
        len(COLUMNS),
        width_ratios=[c[2] for c in COLUMNS],
        wspace=0.06,
        hspace=0.1,
    )
    for i, (col, ylabel, ylim, yscale) in enumerate(ROWS):
        for j, (xlim, xscale, _) in enumerate(COLUMNS):
            A = fig.add_subplot(gs[i, j])
            A.set_xscale(xscale)
            A.set_yscale(yscale)
            A.set_xlim(*xlim)
            A.set_ylim(*ylim)
            A.axvspan(0.0, b_wall, color="#f3e6d0", zorder=0, lw=0)
            A.axvspan(0.5 * sum(EW_BRACKET), 100, color="0.93", zorder=0, lw=0)
            A.axvline(BETA_QCD, color="0.35", ls="--", lw=0.8, zorder=1)
            A.axvline(0.5 * sum(EW_BRACKET), color="0.35", ls="-.", lw=0.8, zorder=1)
            if col in ("dYp", "dDH"):
                A.axhspan(-TOL[col], TOL[col], color="#c9d8e8", zorder=0.5, lw=0)
                A.axhline(0, color="0.45", lw=0.6, ls=":")
            elif col == "dNeff":
                A.axhline(TOL[col], color=PLANCK_COLOUR, lw=1.0, ls="--", zorder=1)
            else:
                A.axhline(0.0, color="0.2", lw=0.9, ls="-", zorder=1)  # R = 1

            inside = lambda x: (np.asarray(x) >= xlim[0]) & (np.asarray(x) <= xlim[1])
            if col != "lnR":
                draw_line(A, lab, col, ylim, b_wall, color="black", lw=0.9, zorder=4)
                for alpha, (c, ls) in FIXED.items():
                    draw_line(
                        A,
                        fixed[np.isclose(fixed.alpha, alpha)],
                        col,
                        ylim,
                        b_wall,
                        color=c,
                        ls=ls,
                        lw=1.0,
                        zorder=4,
                    )
            for M, c in M_CRIT.items():
                s = crit[M][crit[M].ok]
                if col == "lnR":
                    s = crit[M][crit[M].lnR.notna()]
                draw_line(A, s, col, ylim, b_wall, color=c, lw=1.2, zorder=5)
                # the points are drawn unclipped, so only this column's beta
                s = s[inside(s.beta)]
                if col == "lnR":
                    for reg, mk in REGIME_MARKER.items():
                        r = s[s.regime == reg]
                        A.scatter(
                            r.beta,
                            r.lnR.clip(*ylim),
                            marker=mk,
                            s=16,
                            color=c,
                            edgecolor="white",
                            lw=0.4,
                            zorder=6,
                            clip_on=False,
                        )
                    r = s[s.regime == "none"]
                    A.scatter(
                        r.beta,
                        r.lnR.clip(*ylim),
                        marker="x",
                        s=14,
                        color=c,
                        zorder=6,
                        clip_on=False,
                    )
                else:
                    y = s[col]
                    A.scatter(
                        s.beta,
                        y.clip(*ylim),
                        s=9,
                        color=c,
                        zorder=6,
                        lw=0,
                        clip_on=False,
                    )
                    # values off the axis, as triangles at its edge
                    lo = s[(y < ylim[0]) & inside(s.beta)]
                    hi = s[(y > ylim[1]) & inside(s.beta)]
                    A.scatter(
                        lo.beta,
                        np.full(len(lo), ylim[0]),
                        marker="v",
                        s=20,
                        color=c,
                        clip_on=False,
                        zorder=6,
                    )
                    A.scatter(
                        hi.beta,
                        np.full(len(hi), ylim[1]),
                        marker="^",
                        s=20,
                        color=c,
                        clip_on=False,
                        zorder=6,
                    )
                # the histories that failed at a surfing beta, at the axis's foot
                fl = crit[M][
                    ~crit[M].ok & (crit[M].beta >= b_wall) & inside(crit[M].beta)
                ]
                A.scatter(
                    fl.beta,
                    np.full(len(fl), ylim[0] if yscale == "linear" else 1.6 * ylim[0]),
                    marker="X",
                    s=26,
                    color=c,
                    edgecolor="black",
                    lw=0.4,
                    clip_on=False,
                    zorder=7,
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

    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch

    handles = [
        Line2D(
            [],
            [],
            color=c,
            marker="o",
            ms=3,
            lw=1.2,
            label=rf"one-loop law, $A = 1$, $M = 10^{{{int(round(np.log10(M)))}}}\,M_P$",
        )
        for M, c in M_CRIT.items()
    ]
    handles += [
        Line2D(
            [],
            [],
            color="black",
            lw=0.9,
            label=r"one-loop law, $A = 1$, $M = 4\times10^{-31}\,M_P$ (Figure 6)",
        ),
    ]
    handles += [
        Line2D(
            [],
            [],
            color=c,
            ls=ls,
            lw=1.0,
            label=rf"fixed law, $\alpha = {a:g}$ (the same at all three $M$)",
        )
        for a, (c, ls) in FIXED.items()
    ]
    handles += [
        Line2D(
            [],
            [],
            color="0.4",
            marker="o",
            ls="",
            label=r"$\ln\mathcal{R}$ row: drained mostly one-loop",
        ),
        Line2D(
            [],
            [],
            color="0.4",
            marker="s",
            ls="",
            label=r"$\ln\mathcal{R}$ row: drained mostly in the gap",
        ),
        Line2D(
            [],
            [],
            color="0.4",
            marker="^",
            ls="",
            label=r"$\ln\mathcal{R}$ row: drained mostly classically",
        ),
        Line2D([], [], color="0.2", lw=0.9, label=r"$\mathcal{R} = 1$"),
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
            label=r"Planck 2018: $\Delta N_{\rm eff} < 0.30$ (95%, one-tailed, +lensing+BAO)",
        ),
        Patch(
            color="#f3e6d0",
            label=rf"not at the wall before BBN ($\beta < {b_wall:.4g}$)",
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
            marker="X",
            color="0.5",
            markeredgecolor="black",
            ls="",
            label="tuning failed (the 30-integration cap)",
        ),
        Line2D([], [], marker="v", color="0.5", ls="", label="below the axis"),
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
        r"Around $M_{\rm crit}$: $M = 10^{-15}$ and $10^{-12}\,M_P$, $\phi_* = 5\,M_P$, shifts relative to PRyMordial + $\Lambda$CDM",
        fontsize=10,
        y=0.995,
    )
    fig.subplots_adjust(bottom=0.2, top=0.955, left=0.08, right=0.99)

    head = git("rev-parse", "HEAD")
    dirty = bool(git("status", "--porcelain", "--untracked-files=no"))
    sidecar = json.loads(SIDECAR.read_text())
    meta = {
        "Title": "Figure 6b, 2026.10.0 science run",
        "Subject": f"histories.csv sha256 {sha256(INPUT)}; store digest {sidecar['fingerprint']['digest']}; "
        f"git {head}{' (dirty)' if dirty else ''}",
    }
    fig.savefig(HERE / "fig6b.pdf", metadata=meta)
    fig.savefig(HERE / "fig6b.png", dpi=200)

    def summary(s):
        s = s[s.ok & (s.beta >= b_wall)]
        e = s[s.excluded]
        return {
            "histories": int(len(s)),
            "excluded": int(len(e)),
            "largest_excluded_beta": None if e.empty else float(e.beta.max()),
            "dNeff_today_range": [float(s.dNeff.min()), float(s.dNeff.max())],
            "regimes": {k: int(v) for k, v in s.regime.value_counts().items()},
        }

    provenance = {
        "script": "fig6b.py",
        "script_sha256": sha256(Path(__file__)),
        "git_head": head,
        "git_dirty": dirty,
        "input": {"path": "../fig6/inputs/histories.csv", "sha256": sha256(INPUT)},
        "store": {
            "name": sidecar["name"],
            "store_id": sidecar.get("store_id"),
            "digest": sidecar["fingerprint"]["digest"],
        },
        "beta_wall_edge": b_wall,
        "one_loop_A1": {f"{M:.0e}": summary(crit[M]) for M in M_CRIT}
        | {
            "4e-31": summary(
                H[
                    is_M(H.M_Mp, M_LAB)
                    & (H.drain_model == ONE_LOOP)
                    & np.isclose(H.alpha, A_LINE)
                ]
            )
        },
        "failed_surfing": {
            f"{M:.0e}": sorted(
                float(b) for b in crit[M][~crit[M].ok & (crit[M].beta >= b_wall)].beta
            )
            for M in M_CRIT
        },
        "fixed_law_against_laboratory_mass": fixed_dev,
    }
    (HERE / "fig6b.provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    print(
        json.dumps(
            {
                k: provenance[k]
                for k in (
                    "git_head",
                    "git_dirty",
                    "one_loop_A1",
                    "failed_surfing",
                    "fixed_law_against_laboratory_mass",
                )
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
