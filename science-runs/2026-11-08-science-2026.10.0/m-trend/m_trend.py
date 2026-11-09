"""
The trend in M across the laboratory region, 2026.10.0 science run: how the one-loop drain law's
Delta N_eff, its turnaround logarithm ell and the BBN exclusions move with the potential's M from
1e-30 to 1e-42 M_P, on the sparse beta grid of blocks A (4e-31, whose dense grid holds the sparse one)
and B.

    ./venv/bin/python science-runs/2026-11-08-science-2026.10.0/m-trend/m_trend.py

Reads ../fig6/inputs/histories.csv (the run's plot_by_beta.py export of the main store, committed with
Figure 6), inputs/bounces/beta=<beta>/M=<M>eV_n=1/bounces.csv beside this file (unchanged copies of the
run's plot_ScalarHistory.py --no-plots export, A = 1, beta = 1.2, 2 and 10, at every M) and
../registry/stores/science-2026.10.0.manifest.json. Writes m_trend.pdf, .png and .provenance.json beside
this file. Needs no Ray and no store.

Three panels against M (drafted at the user's request of 2026-11-09):
(a) Delta N_eff today relative to its value at 4e-31 M_P, the median over the surfing beta (>= 1.06) for
    each A, with the range over beta shaded for A = 1; the fixed law is at 1 to 2e-8 at every M.
(b) ell = ln(k_ex/m_ta) at the first impact, A = 1, beta = 1.2, 2 and 10 (points), the law's ell(M) at
    that impact's E solved on V = V0 exp(M/phi) (thin line), and its maximum (dashed),
    (1/2) ln(E/V0) - ln 2 - 1.024, where the turnaround sits at M/phi = sqrt(3) - 1; above
    ell = 45 the law is used beyond the range one-loop-drain 02d measured it on.
(c) The largest beta excluded at each (A, M), by the tests of Figure 6: |dY_p| and |d(D/H)| within twice
    the measurement's relative 1-sigma about PRyMordial + LambdaCDM, and Delta N_eff today < 0.30.
Colours follow Figure 6: A = 1 black, A = 3, 5 and 10 green, the band's other A in shades of its red.
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
from scipy.optimize import brentq

HERE = Path(__file__).resolve().parent
INPUT = HERE.parent / "fig6" / "inputs" / "histories.csv"
BOUNCES = HERE / "inputs" / "bounces"
SIDECAR = HERE.parent / "registry" / "stores" / "science-2026.10.0.manifest.json"

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


def load() -> pd.DataFrame:
    H = pd.read_csv(INPUT)
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


# the maximum of ell over M: -x/2 + ln(x/(x+2))/2 at x = sqrt(3) - 1
ELL_MAX_OFFSET = (
    -(np.sqrt(3.0) - 1.0) / 2.0 + 0.5 * np.log(2.0 - np.sqrt(3.0)) - np.log(2.0)
)


def ell_law(log_E: float, ln_V0: float, M: float) -> float:
    """
    ell of AbstractModel.one_loop_turnaround on V = V0 exp(M/phi), solved here in closed form for the
    turnaround: with x = M/phi, V'' = V0 e^x x^3 (x + 2)/M^2 and lambda = x^2/M; m_ta^4 = 32 pi^2 E/ell,
    V''(phi_ta) = m_ta^2 and ell = ln(lambda E^{1/2}/2 m_ta), iterated to a fixed point. It reproduces the
    stored ell of every first impact this figure draws (checked 2026-11-09).
    """
    ell = 32.0
    for _ in range(200):
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
        new = 2 * np.log(x) - np.log(M) + 0.5 * log_E - np.log(2.0) - log_m
        if abs(new - ell) <= 1e-12 * new:
            return new
        ell = new
    raise RuntimeError("ell_law did not converge")


def first_impact(beta: float, M: float) -> pd.Series:
    b = pd.read_csv(BOUNCES / f"beta={beta:g}" / f"M={M_EV[M]}eV_n=1" / "bounces.csv")
    return b[b.delta_K_Mp4 > 0].iloc[0]


def main():
    H = load()
    Ms = sorted(M_EV)
    L = H[H.drain_model != "fixed"]
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
    ax_a.set_ylim(0.38, 1.08)
    ax_a.legend(fontsize=7, ncol=3, loc="lower left", frameon=False)
    ax_a.text(
        0.01,
        0.97,
        r"(a) median over the surfing $\beta$ ($\geq 1.06$); grey: range over $\beta$ at $A = 1$",
        transform=ax_a.transAxes,
        va="top",
        fontsize=8,
    )

    # (b) ell at the first impact, the law's ell(M) at that E, and its maximum
    ax_b.axhspan(ELL_MEASURED, 80, color="0.93", lw=0, zorder=0)
    ell_table = {}
    for beta, c in BETA_ELL.items():
        rows = [first_impact(beta, M) for M in Ms]
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
        ell_max = 0.5 * (log_E - lnV0) + ELL_MAX_OFFSET
        M_curve = np.logspace(-42.5, -29.5, 140)
        ell_curve = [ell_law(log_E, lnV0, M) for M in M_curve]
        ell_check = [ell_law(r.log_E_Mp4, lnV0, M) for r, M in zip(rows, Ms)]
        ell_table[f"{beta:g}"] = {
            "ell": dict(zip([f"{M:.0e}" for M in Ms], ells)),
            "ell_law_max_abs_difference": float(
                np.max(np.abs(np.array(ell_check) - np.array(ells)))
            ),
            "ell_max": ell_max,
            "ln_E_over_V0": log_E - lnV0,
            "ell_law_below_sample": {
                f"{M:.0e}": ell_law(log_E, lnV0, M)
                for M in (1e-44, 1e-46, 1e-50, 1e-60)
            },
        }
        ax_b.plot(M_curve, ell_curve, color=c, lw=0.8, alpha=0.8)
        ax_b.plot(
            Ms, ells, color=c, marker="o", ms=4, lw=0, label=rf"$\beta = {beta:g}$"
        )
        ax_b.axhline(ell_max, color=c, ls="--", lw=0.9)
    ax_b.set_ylabel(r"$\ell = \ln(k_{\rm ex}/m_{\rm ta})$ at the first impact, $A = 1$")
    ax_b.set_ylim(28, 74)
    ax_b.legend(fontsize=8, loc="lower left", frameon=False)
    ax_b.text(
        0.01,
        0.97,
        r"(b) points: stored; line: the law's $\ell(M)$ at that impact's $E$; dashed: its maximum"
        + "\n"
        + r"    $\frac{1}{2}\ln(E/V_0) - \ln 2 - 1.024$, at $M/\phi_{\rm ta} = \sqrt{3} - 1$; below it $\ell$ falls again"
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
        + "tests as Figure 6",
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
        r"The trend in $M$, one-loop law, $\phi_* = 5\,M_P$, sparse $\beta$ grid",
        fontsize=10,
        y=0.995,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.985), h_pad=0.6)

    head = git("rev-parse", "HEAD")
    dirty = bool(git("status", "--porcelain", "--untracked-files=no"))
    sidecar = json.loads(SIDECAR.read_text())
    meta = {
        "Title": "The trend in M, 2026.10.0 science run",
        "Subject": f"histories.csv sha256 {sha256(INPUT)}; store digest {sidecar['fingerprint']['digest']}; "
        f"git {head}{' (dirty)' if dirty else ''}",
    }
    fig.savefig(HERE / "m_trend.pdf", metadata=meta)
    fig.savefig(HERE / "m_trend.png", dpi=200)

    bounce_files = sorted(BOUNCES.rglob("bounces.csv"))
    provenance = {
        "script": "m_trend.py",
        "script_sha256": sha256(Path(__file__)),
        "git_head": head,
        "git_dirty": dirty,
        "inputs": {
            "histories": {
                "path": "../fig6/inputs/histories.csv",
                "sha256": sha256(INPUT),
            },
            "bounces": {str(p.relative_to(HERE)): sha256(p) for p in bounce_files},
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
    (HERE / "m_trend.provenance.json").write_text(
        json.dumps(provenance, indent=2) + "\n"
    )
    print(
        json.dumps(
            {
                k: provenance[k]
                for k in ("git_head", "git_dirty", "ell_first_impact_A1")
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
