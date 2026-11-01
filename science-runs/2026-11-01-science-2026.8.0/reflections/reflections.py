"""
The reflection-structure figure of the 2026.8.0 science run: how the field's history through the
reflections differs between the laboratory mass and larger M, at beta = 1.2, 2.0 and 2.9.

    ./venv/bin/python science-runs/2026-11-01-science-2026.8.0/reflections/reflections.py

Reads inputs/fields.csv.gz beside this file (extract_inputs.py's columns of the reflection store's
42 histories), ../fig6/inputs/histories.csv (the main store's histories, for the reflection
counts) and the two stores' sidecars in ../registry/stores/. Writes reflections.pdf, reflections.png
and reflections.provenance.json beside this file. Needs no Ray and no store.

Rows: the field's kinetic fraction of the Einstein-frame energy density, pi^2/6 (from
3 H^2 M_P^2 (1 - pi^2/6) = rho_R (1 + f_m) + rho_chi, pi in M_P), against T_J; the dark radiation
rho_chi/rho_R against T_J (dissipation mode only); and the number of reflections against M, from
the main store.
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
SIDECARS = [
    HERE.parent / "registry" / "stores" / "science-2026.8.0-reflections.manifest.json",
    HERE.parent / "registry" / "stores" / "science-2026.8.0.manifest.json",
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


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(*args) -> str:
    return subprocess.run(
        ["git", *args], cwd=HERE, capture_output=True, text=True, check=True
    ).stdout.strip()


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

    alphas = sorted(a for a in F.alpha.unique() if a > 0)
    cmap = plt.get_cmap("viridis")
    acol = {a: cmap(0.1 + 0.8 * i / (len(alphas) - 1)) for i, a in enumerate(alphas)}

    fig = plt.figure(figsize=(11.0, 9.6))
    gs = fig.add_gridspec(3, 3, height_ratios=[1.15, 1.0, 0.8], hspace=0.32, wspace=0.08)
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

        # row 2: the dark radiation
        B = fig.add_subplot(gs[1, j], sharex=A)
        for a in alphas:
            for M, (label, ls, lw) in DISSIPATION.items():
                d = one(F, a, beta, M)
                r = d.rho_chi_over_rho_rad.where(d.rho_chi_over_rho_rad > 0)
                B.loglog(d.T_Jordan_GeV, r, color=acol[a], ls=ls, lw=lw,
                         alpha=0.45 if lw > 2 else 1.0)
        B.set_ylim(3e-5, 4e-2)
        if j == 0:
            B.set_ylabel(r"$\rho_\chi/\rho_R$ (dissipation)")
        B.set_xlabel(r"$T_J$ [GeV]")

        for X in (A, B):
            X.axvspan(*T_BBN, color="#f3e6d0", zorder=0, lw=0)
            X.set_xlim(*XLIM)
            if j > 0:
                X.tick_params(labelleft=False)
        A.tick_params(labelbottom=False)

        # row 3: reflections against M, from the main store, at the alpha sampled at every M there
        # (alpha = 0.001 was run at the laboratory M only, in block A2)
        C = fig.add_subplot(gs[2, j])
        for a in [a for a in alphas if a >= 0.01]:
            s = H[np.isclose(H.alpha, a) & np.isclose(H.beta, beta)].sort_values("M_Mp")
            C.semilogx(s.M_Mp, s.reflections, marker="o", ms=3.5, lw=0.8, color=acol[a])
        C.set_xlim(3e-34, 3e-12)
        C.set_xlabel(r"$M / M_P$")
        C.set_ylim(0, 1.1 * C.get_ylim()[1])
        if j == 0:
            C.set_ylabel("reflections")

    handles = [
        Line2D([], [], color=DISSIPATION_COLOUR, ls=ls, lw=lw, alpha=0.45 if lw > 2 else 1.0,
               label=rf"$M = ${label} $M_P$, dissipation ($\alpha = {ALPHA_KIN}$ in row 1)")
        for M, (label, ls, lw) in DISSIPATION.items()
    ] + [
        Line2D([], [], color=colour, lw=0.9, label=rf"$M = ${label} $M_P$, natural mode ($\alpha = 0$)")
        for M, (label, colour) in NATURAL.items()
    ] + [
        Line2D([], [], color=acol[a], lw=1.6,
               label=rf"$\alpha = {a:g}$ (row 2{' and 3' if a >= 0.01 else ''})") for a in alphas
    ] + [Patch(color="#f3e6d0", label="BBN, 2 MeV – 20 keV (indicative)")]
    fig.legend(handles=handles, loc="lower center", ncol=3, fontsize=7.5, frameon=False,
               bbox_to_anchor=(0.5, 0.0))
    fig.suptitle(r"Reflection structure against $M$, $\phi_* = 5\,M_P$", fontsize=11, y=0.995)
    fig.subplots_adjust(bottom=0.16, top=0.95, left=0.07, right=0.99)

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
        },
        "stores": [{"name": s["name"], "digest": s["fingerprint"]["digest"]} for s in stores],
        "parking_T_Jordan_GeV": {f"beta={b:g}, M={M:g}": T for (b, M), T in sorted(parked.items())},
    }
    (HERE / "reflections.provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    print(json.dumps({k: provenance[k] for k in ("git_head", "git_dirty", "parking_T_Jordan_GeV")}, indent=2))


if __name__ == "__main__":
    main()
