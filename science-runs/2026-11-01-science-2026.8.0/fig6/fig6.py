"""
Figure 6 of the 2026.8.0 science run: the shifts in Y_p and D/H, Delta N_eff and (since 2026-11-02)
the temperature at which the field parks, against beta at the laboratory mass M = 4e-31 M_P, with a
band spanning every sampled alpha.

    ./venv/bin/python science-runs/2026-11-01-science-2026.8.0/fig6/fig6.py

Reads only inputs/histories.csv beside this file (a copy of plot_by_beta.py's output on the main
store; since 2026-11-02 the export of ../run_figure_exports.sh, which adds the parking temperature) and ../registry/stores/science-2026.8.0.manifest.json (the store's fingerprint). Writes
fig6.pdf, fig6.png and fig6.provenance.json beside this file. Needs no Ray and no store.

The shifts are relative to PRyMordial + LambdaCDM: PRyMordial with rho_NP = 0, at the same version,
flags and small network as each history's BBN (ComputeTargets.BBNData.compute_SM_baseline). The
tests are differential and two-sided (the user's ruling of 2026-11-01): a history's shift
relative to that baseline must lie within twice the
measurement's relative 1-sigma precision, |dY_p| <= 2 x 0.003/0.245 (PDG 2024) and
|d(D/H)| <= 2 x 0.030/2.527 (Cooke et al. 2018), observational errors only. Delta N_eff today is
drawn against the same 2-sigma width of Planck 2018's N_eff, 2 x 0.17. The parking temperature
is T_J where the parking cut stops the reflections, drawn against an indicative BBN window.
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
INPUT = HERE / "inputs" / "histories.csv"
SIDECAR = HERE.parent / "registry" / "stores" / "science-2026.8.0.manifest.json"

M_LAB = 4e-31
ALPHA_LINE = 0.08  # the one-loop estimate at laboratory M (inelastic-reflection README section 8.0)
BETA_QCD = 1.1074  # the analytic QCD surfing threshold
EW_BRACKET = (3.000, 3.005)  # where the delivery temperature jumps at laboratory M (this run)
T_WALL_GEV = 1e-2  # a history delivered below 10 MeV has not reached the wall before BBN
T_BBN_GEV = (2e-5, 2e-3)  # an indicative BBN window, 20 keV to 2 MeV, as in the reflection figure

YP_OBS, YP_SIG = 0.245, 0.003  # PDG 2024
DH_OBS, DH_SIG = 2.527, 0.030  # Cooke et al. 2018, in 1e-5
NEFF_SIG = 0.17  # Planck 2018, TT,TE,EE+lowE+lensing+BAO
TOL = {
    "dYp": 100 * 2 * YP_SIG / YP_OBS,
    "dDH": 100 * 2 * DH_SIG / DH_OBS,
    "dNeff": 2 * NEFF_SIG,
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
    H = H[np.isclose(H.M_Mp, M_LAB, rtol=1e-9, atol=0.0) & (H.alpha > 0)].copy()
    H["stored"] = ~H.failure_reasons.fillna("").str.startswith("no ScalarHistory")
    H = H[H.stored]
    H["ok"] = H.failure_reasons.isna() & H.Yp_BBN.notna()
    H["dYp"] = 100 * H.delta_Yp
    H["dDH"] = 100 * H.delta_D_over_H
    H["dNeff"] = H.Delta_N_eff_today
    H["parkT"] = H.parking_T_GeV
    return H


def band(H: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    The range over alpha at each beta where every alpha of the run was sampled (13: the main grid
    and the low grid). Blocks A and A2 sampled the window 1.06-1.50 and a sparse grid above it at
    all 13; the rest of A's beta at the main 7 only. A band through both would saw between them,
    so those beta are drawn as points only.
    """
    ok = H[H.ok]
    n_run = H.groupby("beta").alpha.nunique()
    full = n_run[n_run == n_run.max()].index
    g = ok[ok.beta.isin(full)].groupby("beta")[col]
    return pd.DataFrame({"lo": g.min(), "hi": g.max(), "n": g.size()})


def main():
    H = load()
    ok = H[H.ok]
    nobbn = sorted(set(H.beta) - set(ok.beta))  # stored, but no alpha has a BBN result
    td = ok.groupby("beta").T_deliver_GeV.first()
    b_wall = 0.5 * (td[td < T_WALL_GEV].index.max() + td[td >= T_WALL_GEV].index.min())
    n_alpha = ok.groupby("beta").alpha.nunique()

    fig = plt.figure(figsize=(9.0, 10.6))
    gs = fig.add_gridspec(
        len(ROWS), len(COLUMNS), width_ratios=[c[2] for c in COLUMNS], wspace=0.06, hspace=0.1
    )
    cmap = plt.get_cmap("viridis")
    alphas = sorted(ok.alpha.unique())
    colour = {a: cmap(i / (len(alphas) - 1)) for i, a in enumerate(alphas)}

    for i, (col, ylabel, ylim, yscale) in enumerate(ROWS):
        B = band(H, col)
        for j, (xlim, xscale, _) in enumerate(COLUMNS):
            A = fig.add_subplot(gs[i, j])
            A.set_xscale(xscale)
            A.set_yscale(yscale)
            A.set_xlim(*xlim)
            A.set_ylim(*ylim)

            # the regions
            A.axvspan(0.0, b_wall, color="#f3e6d0", zorder=0, lw=0)
            A.axvspan(EW_BRACKET[0] + 0.5 * (EW_BRACKET[1] - EW_BRACKET[0]), 100, color="0.93", zorder=0, lw=0)
            A.axvline(BETA_QCD, color="0.35", ls="--", lw=0.8, zorder=1)
            A.axvline(0.5 * sum(EW_BRACKET), color="0.35", ls="-.", lw=0.8, zorder=1)

            # the observational tolerance; for the parking temperature, the BBN window
            if col not in TOL:
                A.axhspan(*T_BBN_GEV, color="#d9ead3", zorder=0.5, lw=0)
            elif yscale == "linear":
                A.axhspan(-TOL[col], TOL[col], color="#c9d8e8", zorder=0.5, lw=0)
                A.axhline(0, color="0.45", lw=0.6, ls=":")
            else:
                A.axhline(TOL[col], color="#4a6f96", lw=1.0, ls="--", zorder=1)

            # every history, coloured by alpha; the envelope over alpha; the line at alpha = 0.08
            for a in alphas:
                s = ok[ok.alpha == a]
                y = s[col].clip(*ylim) if yscale == "linear" else s[col]
                A.scatter(s.beta, y, s=3, color=colour[a], alpha=0.55, lw=0, zorder=3)
            # one piece: every beta between the band's first and last has a result at some alpha
            A.fill_between(
                B.index, B.lo.clip(*ylim), B.hi.clip(*ylim),
                color="#d1495b", alpha=0.18, lw=0, zorder=2,
            )
            A.plot(B.index, B.lo.clip(*ylim), color="#d1495b", lw=0.6, zorder=2)
            A.plot(B.index, B.hi.clip(*ylim), color="#d1495b", lw=0.6, zorder=2)
            s = ok[np.isclose(ok.alpha, ALPHA_LINE)].set_index("beta").sort_index()
            for start, stop in _runs(s.index.values):
                seg = s.loc[start:stop]
                A.plot(seg.index, seg[col].clip(*ylim), color="black", lw=0.9, zorder=4)

            inside = lambda x: (np.asarray(x) >= xlim[0]) & (np.asarray(x) <= xlim[1])
            # values above the axis, as triangles at its top
            top = ok.groupby("beta")[col].max()
            top = top[(top > ylim[1]) & inside(top.index)]
            A.scatter(top.index, np.full(len(top), ylim[1]), marker="^", s=22, color="#d1495b",
                      clip_on=False, zorder=5)
            # beta with no BBN result at any alpha
            ymark = ylim[0] if yscale == "linear" else 1.6 * ylim[0]
            xs = np.asarray(nobbn)[inside(nobbn)]
            A.scatter(xs, np.full(len(xs), ymark), marker="x", s=22, color="0.2",
                      clip_on=False, zorder=5)

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

    handles = [
        Patch(color="#d1495b", alpha=0.35, label=r"range over all 13 $\alpha$, 0.001–0.15, where all were sampled"),
        Line2D([], [], marker="o", ms=3, color="#3b8f88", ls="", label=r"each history, coloured by $\alpha$ (dark: 0.001, yellow: 0.15)"),
        Line2D([], [], color="black", lw=0.9, label=rf"$\alpha = {ALPHA_LINE}$"),
        Patch(color="#c9d8e8", label=r"$\pm 2\sigma$ of the measurement, about PRyMordial + $\Lambda$CDM"),
        Line2D([], [], color="#4a6f96", ls="--", lw=1.0, label=rf"$2\sigma$ of Planck 2018's $N_{{\rm eff}}$ ({TOL['dNeff']:.2f})"),
        Patch(color="#d9ead3", label="BBN, 2 MeV – 20 keV (indicative; parking row)"),
        Patch(color="#f3e6d0", label=rf"not at the wall before BBN ($T_{{\rm deliver}} < 10$ MeV; $\beta < {b_wall:.4g}$)"),
        Patch(color="0.93", label=rf"surfs the electroweak kick ($\beta > {EW_BRACKET[0]:.3f}$–${EW_BRACKET[1]:.3f}$)"),
        Line2D([], [], color="0.35", ls="--", lw=0.8, label=rf"analytic QCD surfing threshold $\beta = {BETA_QCD}$"),
        Line2D([], [], marker="x", color="0.2", ls="", label="no BBN result at any α (BBN temperature below PRyMordial's floor, or no history)"),
        Line2D([], [], marker="^", color="#d1495b", ls="", label="band above the axis"),
    ]
    fig.legend(handles=handles, loc="lower center", ncol=2, fontsize=7, frameon=False,
               bbox_to_anchor=(0.5, 0.0))
    fig.suptitle(
        rf"$M = 4\times10^{{-31}}\,M_P$, $\phi_* = 5\,M_P$: shifts relative to PRyMordial + $\Lambda$CDM",
        fontsize=10, y=0.995,
    )
    fig.subplots_adjust(bottom=0.165, top=0.955, left=0.08, right=0.99)

    head = git("rev-parse", "HEAD")
    # tracked files only: this script's own outputs are new files until they are committed
    dirty = bool(git("status", "--porcelain", "--untracked-files=no"))
    sidecar = json.loads(SIDECAR.read_text())
    meta = {
        "Title": "Figure 6, 2026.8.0 science run",
        "Subject": f"histories.csv sha256 {sha256(INPUT)}; store digest {sidecar['fingerprint']['digest']}; "
        f"git {head}{' (dirty)' if dirty else ''}",
    }
    fig.savefig(HERE / "fig6.pdf", metadata=meta)
    fig.savefig(HERE / "fig6.png", dpi=200)

    provenance = {
        "script": "fig6.py",
        "script_sha256": sha256(Path(__file__)),
        "git_head": head,
        "git_dirty": dirty,
        "input": {"path": "inputs/histories.csv", "sha256": sha256(INPUT)},
        "store": {"name": sidecar["name"], "store_id": sidecar.get("store_id"),
                  "digest": sidecar["fingerprint"]["digest"]},
        "M_Mp": M_LAB,
        "alphas": alphas,
        "tolerances_percent_or_absolute": TOL,
        "beta_wall_edge": b_wall,
        "betas_without_BBN": nobbn,
        "histories_with_yields": int(len(ok)),
        "histories_without_parking": int(ok.parkT.isna().sum()),
        "alpha_count_by_beta": {f"{b:g}": int(n) for b, n in n_alpha.items()},
        "excluded_histories": {
            "dYp": int((ok.dYp.abs() > TOL["dYp"]).sum()),
            "dDH": int((ok.dDH.abs() > TOL["dDH"]).sum()),
            "either": int(((ok.dYp.abs() > TOL["dYp"]) | (ok.dDH.abs() > TOL["dDH"])).sum()),
            "dNeff": int((ok.dNeff.abs() > TOL["dNeff"]).sum()),
        },
    }
    (HERE / "fig6.provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    print(json.dumps({k: provenance[k] for k in ("git_head", "git_dirty", "histories_with_yields",
                                                 "beta_wall_edge", "excluded_histories")}, indent=2))


def _runs(betas):
    """The runs of consecutive grid points, so that the band is not drawn across a beta with no
    result: a gap is a step more than three times the median local spacing."""
    b = np.sort(betas)
    if len(b) < 2:
        return [(b[0], b[0])] if len(b) else []
    steps = np.diff(b)
    out, start = [], b[0]
    for k, d in enumerate(steps):
        local = np.median(steps[max(0, k - 3): k + 4])
        if d > 3 * local and d > 0.05:
            out.append((start, b[k]))
            start = b[k + 1]
    out.append((start, b[-1]))
    return out


if __name__ == "__main__":
    main()
