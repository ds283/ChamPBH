"""
Figure 6a of the 2026.8.0 science run: the shifts in Y_p and D/H against beta at large M (1e-3,
0.1 and 0.5 M_P) in natural mode (alpha = 0), the successor to the 2026.6.0 draft Figure 6, which
showed M = 1e-3 and 1e-5 M_P before jordan-normalization.

    ./venv/bin/python science-runs/2026-11-01-science-2026.8.0/fig6a/fig6a.py

Reads only ../fig6/inputs/histories.csv (the export of ../run_figure_exports.sh, plot_by_beta.py on
the main store) and ../registry/stores/science-2026.8.0.manifest.json (the store's fingerprint).
Writes fig6a.pdf, fig6a.png and fig6a.provenance.json beside this file. Needs no Ray and no store.

The shifts and tests are Figure 6's: relative to PRyMordial + LambdaCDM, differential and two-sided,
|dY_p| <= 2 x 0.003/0.245 and |d(D/H)| <= 2 x 0.030/2.527. The y axes are symmetric-log, linear
inside +-3%, since the shifts run from -90% to +580%. For reference, the laboratory-mass history at
alpha = 0.08 (Figure 6's line) is drawn in grey.
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
INPUT = HERE.parent / "fig6" / "inputs" / "histories.csv"
SIDECAR = HERE.parent / "registry" / "stores" / "science-2026.8.0.manifest.json"

# (M in M_P, label, colour, marker, filled); the colours of the 2026.6.0 draft
LARGE_M = [
    (1e-3, r"$M = 10^{-3}\,M_P$", "#1f4e9c", "o", True),
    (0.1, r"$M = 0.1\,M_P$", "#d07a12", "s", False),
    (0.5, r"$M = 0.5\,M_P$", "#b2182b", "D", False),
]
M_LAB, ALPHA_LAB = 4e-31, 0.08  # Figure 6's line, for reference
BETA_QCD = 1.1074  # the analytic QCD surfing threshold
BETA_EW = 3.020  # the electroweak threshold at M = 1e-3 (surfing-temperature campaign, prompt 04)
T_WALL_GEV = 1e-2  # a history delivered below 10 MeV has not reached the wall before BBN

YP_OBS, YP_SIG = 0.245, 0.003  # PDG 2024
DH_OBS, DH_SIG = 2.527, 0.030  # Cooke et al. 2018, in 1e-5
TOL = {"dYp": 100 * 2 * YP_SIG / YP_OBS, "dDH": 100 * 2 * DH_SIG / DH_OBS}
LINTHRESH = 3.0  # per cent: the symmetric-log axes are linear inside +-3%

COLUMNS = [((0.45, 3.07), "linear", 2.3), ((3.07, 22.0), "log", 1.0)]  # (x range, scale, width)
ROWS = [  # (column, axis label, y range, ticks)
    ("dYp", r"$\Delta Y_p$ [%]", (-100, 150), [-100, -30, -10, -3, 0, 3, 10, 30, 100]),
    ("dDH", r"$\Delta(\mathrm{D/H})$ [%]", (-100, 800), [-100, -30, -10, -3, 0, 3, 10, 30, 100, 300]),
]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(*args) -> str:
    return subprocess.run(
        ["git", *args], cwd=HERE, capture_output=True, text=True, check=True
    ).stdout.strip()


def load() -> pd.DataFrame:
    H = pd.read_csv(INPUT)
    H["failure_reasons"] = H.failure_reasons.fillna("")
    H = H[~H.failure_reasons.str.startswith("no ScalarHistory")].copy()  # never computed
    H["ok"] = (H.failure_reasons == "") & H.Yp_BBN.notna()
    H["dYp"] = 100 * H.delta_Yp
    H["dDH"] = 100 * H.delta_D_over_H
    H["passes"] = (H.dYp.abs() <= TOL["dYp"]) & (H.dDH.abs() <= TOL["dDH"])
    return H


def at(H: pd.DataFrame, M: float, alpha: float) -> pd.DataFrame:
    s = H[np.isclose(H.M_Mp, M, rtol=1e-9, atol=0.0) & (H.alpha == alpha)]
    return s.sort_values("beta")


def failure_kind(reason: str) -> str:
    """'nobbn' when the history exists but BBN gave no usable result; 'nohistory' otherwise."""
    return "nobbn" if reason.startswith("PRyMordial") else "nohistory"


def main():
    H = load()
    large = {M: at(H, M, 0.0) for M, *_ in LARGE_M}
    lab = at(H, M_LAB, ALPHA_LAB)
    lab = lab[lab.ok]

    # where the field first reaches the wall before BBN, per M (beta between two grid points)
    b_wall = {}
    for M, s in large.items():
        td = s.set_index("beta").T_deliver_GeV.dropna()
        b_wall[M] = (td[td < T_WALL_GEV].index.max(), td[td >= T_WALL_GEV].index.min())
    assert len(set(b_wall.values())) == 1, b_wall  # the same bracket at every large M
    lo, hi = next(iter(b_wall.values()))
    b_edge = 0.5 * (lo + hi)

    fig = plt.figure(figsize=(8.0, 7.6))
    gs = fig.add_gridspec(
        3, 2, width_ratios=[c[2] for c in COLUMNS], height_ratios=[1, 1, 0.42],
        wspace=0.05, hspace=0.1,
    )

    def regions(A):
        A.axvspan(0.0, b_edge, color="#f3e6d0", zorder=0, lw=0)
        A.axvspan(BETA_EW, 100, color="0.93", zorder=0, lw=0)
        A.axvline(BETA_QCD, color="0.35", ls="--", lw=0.8, zorder=1)

    def xaxis(A, j, labels):
        xlim, xscale, _ = COLUMNS[j]
        A.set_xscale(xscale)
        A.set_xlim(*xlim)
        if xscale == "log":
            A.set_xticks([5, 10, 20])
            A.set_xticklabels(["5", "10", "20"] if labels else [])
            A.xaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
        else:
            A.set_xticks([0.5, 1.0, 1.5, 2.0, 2.5, 3.0])
            if not labels:
                A.set_xticklabels([])
        if j == 1:
            A.tick_params(labelleft=False)
            A.spines["left"].set_linestyle((0, (2, 2)))
        else:
            A.spines["right"].set_visible(False)

    for i, (col, ylabel, ylim, yticks) in enumerate(ROWS):
        for j in range(len(COLUMNS)):
            A = fig.add_subplot(gs[i, j])
            A.set_yscale("symlog", linthresh=LINTHRESH, linscale=1.0)
            A.set_ylim(*ylim)
            A.set_yticks(yticks)
            A.set_yticklabels([f"{t:+g}" if t else "0" for t in yticks])
            A.yaxis.set_minor_locator(matplotlib.ticker.NullLocator())
            xaxis(A, j, labels=False)
            regions(A)
            A.axhspan(-TOL[col], TOL[col], color="#c9d8e8", zorder=0.5, lw=0)
            A.axhline(0, color="0.45", lw=0.6, ls=":")

            A.plot(lab.beta, lab[col], color="0.55", lw=0.8, zorder=2)
            for M, _, c, m, filled in LARGE_M:
                s = large[M][large[M].ok]
                A.plot(s.beta, s[col], color=c, lw=0.7, zorder=3)
                A.plot(s.beta, s[col], ls="", marker=m, ms=3.2, color=c, mew=0.7,
                       mfc=c if filled else "white", zorder=4)
            if j == 0:
                A.set_ylabel(ylabel)

    # the verdict strip: one track per M
    for j in range(len(COLUMNS)):
        A = fig.add_subplot(gs[2, j])
        xaxis(A, j, labels=True)
        regions(A)
        A.set_ylim(-0.6, len(LARGE_M) - 0.4)
        A.set_yticks(range(len(LARGE_M)))
        A.set_yticklabels([lbl.replace(r"\,M_P", "") for _, lbl, *_ in LARGE_M] if j == 0 else [])
        A.invert_yaxis()
        for k, (M, _, c, m, filled) in enumerate(LARGE_M):
            s = large[M]
            ok = s[s.ok]
            y = np.full(len(ok), k)
            A.scatter(ok.beta[ok.passes], y[ok.passes.values], marker=m, s=14, color=c,
                      edgecolors=c, zorder=4)
            A.scatter(ok.beta[~ok.passes], y[~ok.passes.values], marker="x", s=10, color="0.45",
                      lw=0.7, zorder=3)
            bad = s[~s.ok]
            kind = bad.failure_reasons.map(failure_kind)
            A.scatter(bad.beta[kind == "nobbn"], np.full((kind == "nobbn").sum(), k), marker="v",
                      s=14, color="k", zorder=4)
            A.scatter(bad.beta[kind == "nohistory"], np.full((kind == "nohistory").sum(), k),
                      marker="|", s=40, color="k", lw=1.0, zorder=4)
        if j == 0:
            A.set_xlabel(r"coupling $\beta$")
        else:
            A.set_xlabel(r"$\beta$ (log scale)")

    handles = [
        *[Line2D([], [], color=c, lw=0.7, marker=m, ms=3.2, mew=0.7, mfc=c if f else "white",
                 label=lbl + r", $\alpha = 0$") for _, lbl, c, m, f in LARGE_M],
        Line2D([], [], color="0.55", lw=0.8,
               label=r"$M = 4\times10^{-31}\,M_P$, $\alpha = 0.08$ (Figure 6's line)"),
        Patch(color="#c9d8e8", label=r"$\pm 2\sigma$ of the measurement, about PRyMordial + $\Lambda$CDM"),
        Patch(color="#f3e6d0", label=rf"not at the wall before BBN ($T_{{\rm deliver}} < 10$ MeV; $\beta \leq {lo:g}$)"),
        Patch(color="0.93", label=rf"surfs the electroweak kick ($\beta > {BETA_EW}$)"),
        Line2D([], [], color="0.35", ls="--", lw=0.8, label=rf"analytic QCD surfing threshold $\beta = {BETA_QCD}$"),
        Line2D([], [], color="0.45", marker="x", ls="", label="bottom row: excluded by $Y_p$ or D/H (marker: passes both)"),
        Line2D([], [], color="k", marker="v", ls="", label="no BBN result (below PRyMordial's floor, or $Y_p > 0.5$)"),
        Line2D([], [], color="k", marker="|", ls="", ms=7, label="no history (normalization not converged; no minimum)"),
    ]
    fig.legend(handles=handles, loc="lower center", ncol=2, fontsize=6.5, frameon=False,
               bbox_to_anchor=(0.5, 0.0))
    fig.suptitle(
        r"Large $M$, natural mode ($\alpha = 0$), $\phi_* = 5\,M_P$: shifts relative to PRyMordial + $\Lambda$CDM",
        fontsize=9.5, y=0.995,
    )
    fig.subplots_adjust(bottom=0.2, top=0.955, left=0.1, right=0.99)

    head = git("rev-parse", "HEAD")
    dirty = bool(git("status", "--porcelain", "--untracked-files=no"))
    sidecar = json.loads(SIDECAR.read_text())
    meta = {
        "Title": "Figure 6a, 2026.8.0 science run",
        "Subject": f"histories.csv sha256 {sha256(INPUT)}; store digest {sidecar['fingerprint']['digest']}; "
        f"git {head}{' (dirty)' if dirty else ''}",
    }
    fig.savefig(HERE / "fig6a.pdf", metadata=meta)
    fig.savefig(HERE / "fig6a.png", dpi=200)

    per_M = {}
    for M, *_ in LARGE_M:
        s = large[M]
        ok = s[s.ok]
        per_M[f"{M:g}"] = {
            "histories": int(len(s)),
            "with_yields": int(len(ok)),
            "passing_both": [float(b) for b in ok.beta[ok.passes]],
            "excluded_by_Yp": int((ok.dYp.abs() > TOL["dYp"]).sum()),
            "excluded_by_DH": int((ok.dDH.abs() > TOL["dDH"]).sum()),
            "without_result": {f"{b:g}": r for b, r in zip(s.beta[~s.ok], s.failure_reasons[~s.ok])},
        }
    provenance = {
        "script": "fig6a.py",
        "script_sha256": sha256(Path(__file__)),
        "git_head": head,
        "git_dirty": dirty,
        "input": {"path": "../fig6/inputs/histories.csv", "sha256": sha256(INPUT)},
        "store": {"name": sidecar["name"], "store_id": sidecar.get("store_id"),
                  "digest": sidecar["fingerprint"]["digest"]},
        "alpha": 0.0,
        "tolerances_percent": TOL,
        "beta_wall_bracket": [lo, hi],
        "reference_line": {"M_Mp": M_LAB, "alpha": ALPHA_LAB},
        "by_M": per_M,
    }
    (HERE / "fig6a.provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    print(json.dumps({k: provenance[k] for k in ("git_head", "git_dirty", "beta_wall_bracket")}, indent=2))
    for M, d in per_M.items():
        print(M, d["with_yields"], "with yields; passing both at", d["passing_both"],
              "; excluded by Yp", d["excluded_by_Yp"], "by D/H", d["excluded_by_DH"])


if __name__ == "__main__":
    main()
