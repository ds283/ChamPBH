"""
The reflection-structure figure of the 2026.8.0 science run: how the field's history through the
reflections differs between the laboratory mass and larger M, at beta = 1.2, 2.0 and 2.9.

    ./venv/bin/python science-runs/2026-11-01-science-2026.8.0/reflections/reflections.py

Reads inputs/fields.csv.gz beside this file (extract_inputs.py's columns of the reflection store's
42 histories and the diagnostic store's 12), ../fig6/inputs/histories.csv (the main store's
histories) and inputs/histories-large-M-dissipation.csv (the diagnostic store's), both
plot_by_beta.py's exports by ../run_figure_exports.sh, and the three stores' sidecars in
../registry/stores/. Writes reflections.pdf, reflections.png and reflections.provenance.json beside
this file. Needs no Ray and no store.

Rows: the field's kinetic fraction of the Einstein-frame energy density, pi^2/6 (from
3 H^2 M_P^2 (1 - pi^2/6) = rho_R (1 + f_m) + rho_chi, pi in M_P), against T_J; the dark radiation
rho_chi/rho_R against T_J (dissipation mode only), with the diagnostic store's alpha = 0.08 at
M = 1e-9 to 0.5 over it as markers, and under it a strip of their rho_chi/rho_R relative to the
laboratory history's, sample by sample (the redshift grid is the same at every M); and (since 2026-11-02, in place of the reflection count) the test-field
ratio R at the first impact against M, which decides whether an impact produces particles.
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
FIELDS = HERE / "inputs" / "fields.csv.gz"
HISTORIES = HERE.parent / "fig6" / "inputs" / "histories.csv"
HISTORIES_X = HERE / "inputs" / "histories-large-M-dissipation.csv"
SIDECARS = [
    HERE.parent / "registry" / "stores" / "science-2026.8.0-reflections.manifest.json",
    HERE.parent / "registry" / "stores" / "science-2026.8.0.manifest.json",
    HERE.parent / "registry" / "stores" / "science-2026.8.0-large-M-dissipation.manifest.json",
]

BETAS = [1.2, 2.0, 2.9]
ALPHA_KIN = 0.08  # the alpha whose dissipation-mode histories are drawn in the kinetic-fraction row
T_BBN = (2e-3, 2e-5)  # GeV: an indicative BBN window, 2 MeV to 20 keV
T_PARKED = 1e-20  # a dissipation-mode history is parked once its kinetic fraction falls below this
XLIM = (3e2, 1e-11)  # GeV, decreasing to the right

DISSIPATION = {  # M / M_P: (label, line style, line width)
    4e-31: (r"$4\times10^{-31}$ (laboratory)", "-", 2.6),
    1e-15: (r"$10^{-15}$", "--", 1.4),
    1e-12: (r"$10^{-12}$", ":", 1.4),
}
NATURAL = {1e-3: (r"$10^{-3}$", "#d1495b"), 0.5: (r"$0.5$", "#7b3294")}
DISSIPATION_COLOUR = "#2c7bb6"
LARGE_M = {  # the diagnostic store, alpha = 0.08: M / M_P: (label, line style in the strip, marker in row 2)
    1e-9: (r"$10^{-9}$", (0, (1, 1.2)), "v"),
    1e-6: (r"$10^{-6}$", (0, (4, 1.5, 1, 1.5)), "^"),
    1e-3: (r"$10^{-3}$", (0, (6, 2)), "D"),
    0.5: (r"$0.5$", "-", "o"),
}
MARKERS_PER_CURVE = 7  # row 2's markers on each large-M curve, log-spaced in T_J and staggered by M
LARGE_M_COLOUR = "#e66101"
# the diagnostic store was registered after its run, so its sidecar holds no fingerprint; the run's
# status record holds the digest taken at its end
FINGERPRINT_FALLBACK = {
    "science-2026.8.0-large-M-dissipation": HERE.parent / "registry" / "runs"
    / "science-2026.8.0-X-X-large-M-dissipation-20261101T223930" / "status.json",
}
M_CRIT_NOTES = (1.2e-15, 8.2e-13)  # the briefing notes' M_crit, median and maximum (PLAN.md, P3)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(*args) -> str:
    return subprocess.run(
        ["git", *args], cwd=HERE, capture_output=True, text=True, check=True
    ).stdout.strip()


def digest(sidecar: dict) -> str:
    if "fingerprint" in sidecar:
        return sidecar["fingerprint"]["digest"]
    return json.loads(FINGERPRINT_FALLBACK[sidecar["name"]].read_text())["fingerprint"]["digest"]


def one(F, alpha, beta, M):
    d = F[np.isclose(F.alpha, alpha, rtol=1e-9, atol=1e-15) & np.isclose(F.beta, beta)
          & np.isclose(F.M_Mp, M, rtol=1e-9, atol=0.0)].sort_values("sample")
    if d.empty:
        raise LookupError(f"no history at alpha={alpha}, beta={beta}, M={M}")
    return d


def main():
    F = pd.read_csv(FIELDS)
    F["kin"] = F.pi_Einstein_Mp ** 2 / 6.0
    H = pd.read_csv(HISTORIES)
    H = H[H.failure_reasons.isna() & (H.alpha > 0)]
    HX = pd.read_csv(HISTORIES_X)
    HX = HX[HX.failure_reasons.isna()]
    ratio = {}
    final = {}

    alphas = sorted(a for a in F.alpha.unique() if a > 0)
    cmap = plt.get_cmap("viridis")
    acol = {a: cmap(0.1 + 0.8 * i / (len(alphas) - 1)) for i, a in enumerate(alphas)}

    fig = plt.figure(figsize=(11.0, 11.6))
    gs = fig.add_gridspec(3, 3, height_ratios=[1.15, 1.45, 0.8], hspace=0.22, wspace=0.08)
    parked = {}

    for j, beta in enumerate(BETAS):
        # row 1: the kinetic fraction
        A = fig.add_subplot(gs[0, j])
        for M, (label, colour) in NATURAL.items():
            d = one(F, 0.0, beta, M)
            A.loglog(d.T_Jordan_GeV, d.kin, color=colour, lw=0.7, zorder=2)
        for M, (label, ls, lw) in DISSIPATION.items():
            d = one(F, ALPHA_KIN, beta, M)
            A.loglog(d.T_Jordan_GeV, d.kin, color=DISSIPATION_COLOUR, ls=ls, lw=lw,
                     alpha=0.45 if lw > 2 else 1.0, zorder=3)
            below = d[d.kin < T_PARKED]
            if not below.empty:
                parked[(beta, M)] = float(below.T_Jordan_GeV.iloc[0])
        Tp = parked.get((beta, 4e-31))
        if Tp is not None:
            A.annotate("parks", xy=(Tp, 1e-13), xytext=(Tp * 30, 1e-11), ha="right", fontsize=7,
                       arrowprops=dict(arrowstyle="->", lw=0.7))
        A.set_ylim(1e-14, 1.0)
        A.set_title(rf"$\beta = {beta:g}$", fontsize=10)
        if j == 0:
            A.set_ylabel(r"kinetic fraction $\pi^2/6$")

        # row 2: the dark radiation, with the strip of the large-M ratios under it
        inner = gs[1, j].subgridspec(2, 1, height_ratios=[1.0, 0.45], hspace=0.06)
        B = fig.add_subplot(inner[0], sharex=A)
        S = fig.add_subplot(inner[1], sharex=A)
        for a in alphas:
            for M, (label, ls, lw) in DISSIPATION.items():
                d = one(F, a, beta, M)
                r = d.rho_chi_over_rho_rad.where(d.rho_chi_over_rho_rad > 0)
                B.loglog(d.T_Jordan_GeV, r, color=acol[a], ls=ls, lw=lw,
                         alpha=0.45 if lw > 2 else 1.0)
        lab = one(F, ALPHA_KIN, beta, 4e-31)
        lab_r = dict(zip(lab.log_one_plus_z, lab.rho_chi_over_rho_rad))
        S.axhline(0, color="0.3", lw=0.6, zorder=1)
        for k, (M, (label, ls, marker)) in enumerate(LARGE_M.items()):
            d = one(F, ALPHA_KIN, beta, M)
            d = d[d.rho_chi_over_rho_rad > 0]
            # markers at sample points nearest a log-spaced grid in T_J, staggered by M so that the
            # four coincident curves stay distinguishable
            lo, hi = np.log10(max(d.T_Jordan_GeV.min(), XLIM[1])), np.log10(d.T_Jordan_GeV.max())
            step = (hi - lo) / MARKERS_PER_CURVE
            targets = lo + step * (np.arange(MARKERS_PER_CURVE) + (k + 0.5) / len(LARGE_M))
            logT = np.log10(d.T_Jordan_GeV.values)
            pick = [int(np.argmin(np.abs(logT - t))) for t in targets]
            B.loglog(d.T_Jordan_GeV.values[pick], d.rho_chi_over_rho_rad.values[pick], ls="",
                     marker=marker, ms=5, mfc="none", mec=LARGE_M_COLOUR, mew=1.1, zorder=5)
            # the strip: rho_chi/rho_R relative to the laboratory history at the same redshift
            ref = d.log_one_plus_z.map(lab_r)
            S.plot(d.T_Jordan_GeV, d.rho_chi_over_rho_rad / ref - 1.0, color=LARGE_M_COLOUR,
                   ls=ls, lw=0.9, zorder=3)
            final[f"beta={beta:g}"] = final.get(f"beta={beta:g}", {}) | {
                f"{M:g}": float(d.rho_chi_over_rho_rad.iloc[-1] / ref.iloc[-1] - 1.0)
            }
        S.set_xscale("log")
        S.set_yscale("symlog", linthresh=1e-4, linscale=0.5)
        S.set_ylim(-1.0, 1.0)
        S.set_yticks([-1.0, -1e-2, 0, 1e-2, 1.0])
        S.set_yticklabels(["$-100\\%$", "$-1\\%$", "0", "$1\\%$", "$100\\%$"], fontsize=8)
        S.yaxis.set_minor_locator(matplotlib.ticker.NullLocator())
        B.set_ylim(3e-5, 4e-2)
        if j == 0:
            B.set_ylabel(r"$\rho_\chi/\rho_R$ (dissipation)")
            S.set_ylabel("vs lab.", fontsize=9)
        S.set_xlabel(r"$T_J$ [GeV]")
        B.tick_params(labelbottom=False)

        for X in (A, B, S):
            X.axvspan(*T_BBN, color="#f3e6d0", zorder=0, lw=0)
            X.set_xlim(*XLIM)
            if j > 0:
                X.tick_params(labelleft=False)
        A.tick_params(labelbottom=False)

        # row 3: the test-field ratio at the first impact against M, at alpha = 0.08. The history is
        # the same at every alpha up to the first impact, so R is too; it is stored at alpha > 0 only
        C = fig.add_subplot(gs[2, j])
        C.axhspan(-80, 0, color="0.93", lw=0, zorder=0)
        C.axhline(0, color="0.3", lw=0.8, zorder=1)
        for x, ls in zip(M_CRIT_NOTES, ("--", ":")):
            C.axvline(x, color="0.45", ls=ls, lw=0.8, zorder=1)
        for src, marker, face, colour in ((H, "o", DISSIPATION_COLOUR, DISSIPATION_COLOUR),
                                          (HX, "s", "none", LARGE_M_COLOUR)):
            s = src[np.isclose(src.alpha, ALPHA_KIN) & np.isclose(src.beta, beta)
                    & src.log_R_first_impact.notna()].sort_values("M_Mp")
            C.semilogx(s.M_Mp, s.log_R_first_impact / np.log(10), ls="", marker=marker, ms=4.5,
                       mfc=face, mec=colour, mew=1.0, zorder=3)
            ratio[f"beta={beta:g}"] = ratio.get(f"beta={beta:g}", {}) | {
                f"{m:g}": l / np.log(10) for m, l in zip(s.M_Mp, s.log_R_first_impact)
            }
        C.set_xlim(3e-34, 3.0)
        C.set_ylim(-70, 80)
        C.set_xticks([1e-30, 1e-24, 1e-18, 1e-12, 1e-6, 1])
        C.set_xlabel(r"$M / M_P$")
        C.text(1e-32, -8, "elastic: no particle production", fontsize=7, va="top", color="0.3")
        if j == 0:
            C.set_ylabel(r"$\log_{10} \mathcal{R}$, first impact")
        else:
            C.tick_params(labelleft=False)

    handles = [
        Line2D([], [], color="0.35", ls=ls, lw=lw, alpha=0.45 if lw > 2 else 1.0,
               label=rf"$M = ${label} $M_P$, dissipation (rows 1, 2)")
        for M, (label, ls, lw) in DISSIPATION.items()
    ] + [
        Line2D([], [], color=colour, lw=0.9, label=rf"$M = ${label} $M_P$, natural mode ($\alpha = 0$)")
        for M, (label, colour) in NATURAL.items()
    ] + [
        Line2D([], [], color=acol[a], lw=1.6,
               label=rf"$\alpha = {a:g}$ (row 2{'; row 1 in blue' if a == ALPHA_KIN else ''})")
        for a in alphas
    ] + [
        Line2D([], [], color=LARGE_M_COLOUR, ls=ls, lw=0.9, marker=marker, ms=5, mfc="none",
               label=rf"$M = ${label} $M_P$, $\alpha = {ALPHA_KIN}$ (row 2, strip)")
        for M, (label, ls, marker) in LARGE_M.items()
    ] + [
        Patch(color="#f3e6d0", label="BBN, 2 MeV – 20 keV (indicative)"),
        Line2D([], [], ls="", marker="o", ms=4.5, color=DISSIPATION_COLOUR,
               label=r"$\mathcal{R}$ at the first impact, main store (row 3)"),
        Line2D([], [], ls="", marker="s", ms=4.5, mfc="none", mec=LARGE_M_COLOUR,
               label=r"$\mathcal{R}$ at the first impact, large-$M$ diagnostic (row 3)"),
        Line2D([], [], color="0.45", ls="--", lw=0.8,
               label=r"briefing notes' $M_{\rm crit}$, median ($1.2\times10^{-15}\,M_P$)"),
        Line2D([], [], color="0.45", ls=":", lw=0.8,
               label=r"briefing notes' $M_{\rm crit}$, maximum ($8.2\times10^{-13}\,M_P$)"),
    ]
    fig.legend(handles=handles, loc="lower center", ncol=3, fontsize=7.5, frameon=False,
               bbox_to_anchor=(0.5, 0.0))
    fig.suptitle(r"Reflection structure against $M$, $\phi_* = 5\,M_P$", fontsize=11, y=0.995)
    fig.subplots_adjust(bottom=0.175, top=0.95, left=0.07, right=0.975)

    head = git("rev-parse", "HEAD")
    dirty = bool(git("status", "--porcelain", "--untracked-files=no"))
    stores = [json.loads(p.read_text()) for p in SIDECARS]
    fig.savefig(HERE / "reflections.pdf", metadata={
        "Title": "Reflection structure, 2026.8.0 science run",
        "Subject": f"fields.csv.gz sha256 {sha256(FIELDS)}; git {head}{' (dirty)' if dirty else ''}",
    })
    fig.savefig(HERE / "reflections.png", dpi=200)

    provenance = {
        "script": "reflections.py",
        "script_sha256": sha256(Path(__file__)),
        "git_head": head,
        "git_dirty": dirty,
        "inputs": {
            "inputs/fields.csv.gz": sha256(FIELDS),
            "../fig6/inputs/histories.csv": sha256(HISTORIES),
            "inputs/histories-large-M-dissipation.csv": sha256(HISTORIES_X),
        },
        "stores": [{"name": s["name"], "digest": digest(s)} for s in stores],
        "parking_T_Jordan_GeV": {f"beta={b:g}, M={M:g}": T for (b, M), T in sorted(parked.items())},
        "log10_R_first_impact_alpha_0.08": ratio,
        "rho_chi_ratio_vs_laboratory_minus_1_at_end_alpha_0.08": final,
    }
    (HERE / "reflections.provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    print(json.dumps({k: provenance[k] for k in ("git_head", "git_dirty", "parking_T_Jordan_GeV",
                                                 "rho_chi_ratio_vs_laboratory_minus_1_at_end_alpha_0.08")}, indent=2))


if __name__ == "__main__":
    main()
