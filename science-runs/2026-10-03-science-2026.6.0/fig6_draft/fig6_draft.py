# skill:figure-style kernel.py (auto-injected on skill load)
exec(open('inputs/6c1a4d00-a0a-figstyle_kernel.py').read())

import glob, sqlite3, sys
import numpy as np, pandas as pd, matplotlib as mpl
from matplotlib.figure import Figure

apply_figure_style(sizes=(8, 7, 6))

HIST_ROOT = '/Users/ds283/ChamPBH-stores-bt02'
DB_STEM = '/Users/ds283/ChamPBH-stores/science-2026.6.0'
MP_EV = 2.436e27
C3, C5c, CRED = "#1f4e9c", "#d07a12", "#b2182b"


def load(p):
    h = pd.read_csv(f"{HIST_ROOT}/science-out-phi{p}/QCD_Cosmology/histories.csv")
    h["failure_reasons"] = h.failure_reasons.fillna("")
    h = h[~h.failure_reasons.str.contains("no ScalarModel row")].copy()
    h["phi"] = p
    return h


H = pd.concat([load(p) for p in (5, 2, 1)], ignore_index=True)
H["dYp"], H["dDH"], H["Td"] = 100 * H.delta_Yp, 100 * H.delta_D_over_H, H.T_deliver_GeV
ok = H.Yp_BBN.notna()
Yp0 = float(np.median(H.Yp_BBN[ok] / (1 + H.delta_Yp[ok])))
DH0 = float(np.median(H.D_over_H[ok] / (1 + H.delta_D_over_H[ok])))
assert np.allclose(H.Yp_BBN[ok] / (1 + H.delta_Yp[ok]), Yp0, rtol=1e-9)
h5 = H[H.phi == 5]
d3 = h5[np.isclose(h5.M_Mp, 1e-3) & h5.dYp.notna()]
d5 = h5[np.isclose(h5.M_Mp, 1e-5) & h5.dYp.notna()]
nf = h5[h5.Yp_BBN.isna() & ~h5.failure_reasons.str.contains("spline_min|outside")]
excl = h5[h5.Yp_BBN.isna() & h5.failure_reasons.str.contains("spline_min|outside")]

sg = pd.read_csv('inputs/9924a7a7-5a5-sigma_of_T.csv')
Tg, Sg = sg.T_GeV.values, sg.Sigma.values
lT = np.log(Tg)
dS = np.gradient(Sg, lT)
with np.errstate(divide="ignore", invalid="ignore"):
    bth = np.sqrt((1 + Sg / 2) / (3 * Sg))
ext = [i for i in range(1, len(Sg) - 1) if (Sg[i] - Sg[i - 1]) * (Sg[i + 1] - Sg[i]) < 0 and Tg[i] > 5e-5]
iq = max((i for i in ext if 0.05 < Tg[i] < 1), key=lambda i: Sg[i])
BTH = float(bth[iq])
BDEL = 0.5 * (d3[d3.Td < 1e-2].beta.max() + d3[d3.Td >= 1e-2].beta.min())
YOBS, YSIG, DOBS, DSIG = 0.245, 0.003, 2.527, 0.030
Yband = ((YOBS - 2 * YSIG) / Yp0 * 100 - 100, (YOBS + 2 * YSIG) / Yp0 * 100 - 100)
Dband = ((DOBS - 2 * DSIG) / DH0 * 100 - 100, (DOBS + 2 * DSIG) / DH0 * 100 - 100)


def allowed(d):
    return abs(Yp0 * (1 + d.delta_Yp) - YOBS) < 2 * YSIG


def logx(A, ticks, labels=True):
    A.set_xscale("log"); A.set_xticks(ticks)
    A.set_xticklabels([f"{t:g}" for t in ticks] if labels else [])
    A.xaxis.set_minor_formatter(mpl.ticker.NullFormatter())


def save(fig, stem):
    fig.savefig(f"{stem}.png", dpi=300, bbox_inches="tight")
    fig.savefig(f"{stem}.pdf", bbox_inches="tight")


win = d3[(d3.beta >= 1.07) & (d3.beta < 1.5)]
frac_ex = 1 - allowed(win).mean()
n_above = int((~allowed(d3[d3.beta >= 1.5])).sum())
fig = Figure(figsize=(7.2, 6.4))
gs = fig.add_gridspec(3, 2, width_ratios=[2.3, 1], height_ratios=[1, 1, 0.55], wspace=0.05, hspace=0.12)
ax = [[fig.add_subplot(gs[i, j]) for j in range(2)] for i in range(3)]
xl = [(0.48, 3.0), (3.0, 26)]
CL = {"dYp": 12, "dDH": 25}


def shade(A, lab):
    A.axvspan(0.48, BDEL, color="#f3e6d0", zorder=0, label="not at the wall before BBN (φ*=5)" if lab else None)
    A.axvspan(BDEL, BTH, color="#dce6f2", zorder=0, label="reaches the wall without surfing" if lab else None)
    A.axvline(BTH, color="0.4", lw=0.7, ls="--", zorder=1,
              label=rf"surfing threshold $\beta={BTH:.3f}$ ($\Sigma_{{\rm eff}}^{{\max}}=1/3\beta^2$)" if lab else None)


for i, (key, band, lab, bl) in enumerate([
        ("dYp", Yband, r"$\Delta Y_p$ vs same-path SM [%]", "PDG 2024 $Y_p$, ±2σ obs."),
        ("dDH", Dband, r"$\Delta(\mathrm{D/H})$ vs same-path SM [%]", "Cooke+2017 D/H, ±2σ obs. only")]):
    for j in range(2):
        A = ax[i][j]; lo, hi = xl[j]
        A.axhspan(*band, color="0.87", zorder=0, label=bl); A.axhline(0, color="0.45", lw=0.6, ls=":")
        if j == 0:
            shade(A, i == 0)
        for d, st in [(d3, dict(marker="o", ms=2.6, color=C3, ls="none", label=r"$M=10^{-3}M_P$")),
                      (d5, dict(marker="o", ms=3.4, mfc="none", mew=0.6, color=C5c, ls="none", label=r"$M=10^{-5}M_P$"))]:
            s = d[(d.beta >= lo) & (d.beta <= hi)]; inr = s[key] <= CL[key]
            A.plot(s.beta[inr], s[key][inr], zorder=3 if d is d3 else 2, **st)
            A.plot(s.beta[~inr], np.full((~inr).sum(), CL[key] * 0.96), marker="^", ms=3.5, color=st["color"], ls="none",
                   mfc=st.get("mfc", st["color"]), mew=0.6, zorder=3,
                   label=("off scale (value clipped)" if (d is d3 and j == 0 and i == 0) else None))
        e = excl[(excl.beta >= lo) & (excl.beta <= hi)]
        A.plot(e.beta, np.full(len(e), CL[key] * 0.96), marker="v", ms=3.5, color="k", ls="none",
               label=r"no BBN result ($\rho_{NP}/\rho_R\gg1$)" if (j == 0 and i == 0) else None)
        A.set_xlim(lo, hi); A.set_ylim(min(-3.6, band[0] - 0.6), CL[key])
        if j == 1:
            logx(A, [3, 5, 10, 20], labels=False); A.tick_params(labelleft=False)
        else:
            A.set_ylabel(lab); A.tick_params(labelbottom=False)
for j in range(2):
    A = ax[2][j]; lo, hi = xl[j]
    if j == 0:
        shade(A, False)
    for d, c, ls, lab in [(d3, C3, "-", r"$M=10^{-3}M_P$"), (d5, C5c, "--", r"$M=10^{-5}M_P$")]:
        s = d[(d.beta >= lo) & (d.beta <= hi)].sort_values("beta"); okk = allowed(s).astype(float).values; x = s.beta.values
        if j == 0:
            A.plot(x, [okk[(x >= xi - 0.025) & (x <= xi + 0.025)].mean() for xi in x], color=c, ls=ls, lw=1.1, label=lab)
        else:
            A.plot(x, okk, color=c, ls=ls, lw=1.1, marker="o", ms=2)
    f = nf[(nf.beta >= lo) & (nf.beta <= hi) & (np.isclose(nf.M_Mp, 1e-3) | np.isclose(nf.M_Mp, 1e-5))]
    if len(f):
        A.plot(f.beta, np.full(len(f), -0.1), marker="|", ms=6, color="0.3", ls="none",
               label="BBN not assessed (solver failure)" if j == 0 else None)
    A.set_xlim(lo, hi); A.set_ylim(-0.22, 1.1)
    if j == 1:
        logx(A, [3, 5, 10, 20]); A.tick_params(labelleft=False)
    else:
        A.set_ylabel("fraction with\n$Y_p$ within 2σ\n(window ±0.025)")
ax[2][0].set_xlabel(r"coupling $\beta$"); ax[2][1].set_xlabel(r"coupling $\beta$ (log scale)")
ax[0][0].set_title(rf"Draft: $Y_p$ excludes {100 * frac_ex:.0f}% of histories with $1.07\leq\beta<1.5$"
                   + (" and none above" if n_above == 0 else f" and {n_above} above"), loc="left")
h, l = ax[0][0].get_legend_handles_labels(); ax[0][1].legend(h, l, loc="upper right", frameon=False, fontsize=6)
h, l = ax[1][0].get_legend_handles_labels(); ax[1][1].legend(h[:1], l[:1], loc="upper right", frameon=False, fontsize=6)
h, l = ax[2][0].get_legend_handles_labels(); ax[2][1].legend(h, l, loc="center right", frameon=False, fontsize=6)
for A, L in zip([ax[0][0], ax[1][0], ax[2][0]], "abc"):
    panel_letter(A, L)
save(fig, "fig6_draft")