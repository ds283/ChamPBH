# fig6 — Figure 6 at the laboratory mass

**Draft of 2026-11-01**, for the user to review. It replaces the `2026.6.0` draft
([`../../2026-10-03-science-2026.6.0/fig6_draft/`](../../2026-10-03-science-2026.6.0/fig6_draft/NOTES.md)).
`fig6.py` was written by a Claude Code session at the user's request; the choices below marked
*ruled* are the user's, in their words in that session.

```bash
./venv/bin/python science-runs/2026-11-01-science-2026.8.0/fig6/fig6.py
```

It reads only `inputs/histories.csv`, an unchanged copy of
`var/science-2026.8.0/out/main/QCD_Cosmology/histories.csv` (SHA-256 `f7862e39…`, written by the
run's `plot_by_beta.py` step at 20:50 UTC on 2026-11-01), and the main store's sidecar in
`../registry/stores/`. It needs no Ray and no store. It writes `fig6.pdf`, `fig6.png` and
`fig6.provenance.json`, which records the script's and the input's SHA-256, the git commit and
whether the tree was dirty, and the store's digest; the PDF's metadata carries the same.

## What it shows

M = 4×10⁻³¹ M_P, φ* = 5 M_P, every α > 0 of the run: ΔY_p and Δ(D/H) relative to **PRyMordial +
ΛCDM**, and ΔN_eff today, against β. The baseline is PRyMordial with ρ_NP = 0, at the same version
(`bf24c3d+ri02+sr01+bt02`), flags and small network as each history's BBN
(`compute_SM_baseline`): Y_p = 0.24688, D/H = 2.4583×10⁻⁵. The label is the user's (2026-11-01),
in place of the draft's "same-route Standard Model". Four columns: the non-surfing
samples (0.5–1.0), the transition window (1.05–1.52), 1.52–3.07, and the tail to 20 on a log axis.

- **The band** spans every sampled α at each β (*ruled*): 13 values, 0.001–0.15, where blocks A and
  A2 both ran, that is the window 1.06–1.50 and a sparse grid above it (106 β). A's other β have the
  main 7 α only; drawn into the band they make it saw between grids, so there they appear as points
  only. At β = 1.1775, 1.18 and 1.1825 the band spans 12 α: α = 0.001 failed there
  (`classical_turning_point`).
- **Every history** is a point coloured by α, and **α = 0.08**, the one-loop estimate at laboratory M,
  is a line.
- **The tests are differential and two-sided** (*ruled*): |ΔY_p| ≤ 2 × 0.003/0.245 = 2.45% (PDG 2024)
  and |Δ(D/H)| ≤ 2 × 0.030/2.527 = 2.37% (Cooke et al. 2018), about zero, observational errors only.
  PRyMordial + ΛCDM stands in for the measurement, so PRyMordial's low D/H (2.458×10⁻⁵, about 2.3σ
  below Cooke) is not counted as room. ΔN_eff today is compared with 2 × 0.17 = 0.34, twice
  Planck 2018's 1σ on N_eff (TT,TE,EE+lowE+lensing+BAO); **that value is to be confirmed** against the
  reference the paper cites.
- **Beige:** not at the wall before BBN, where the delivery temperature is below 10 MeV: β < 1.066
  here. **Grey:** surfs the electroweak kick, past the cut, from where the delivery temperature jumps
  (1.68 GeV at β = 3.000, 14.3 GeV at 3.005; it is 3.020 at M = 10⁻³). **Dashed:** the analytic QCD
  surfing threshold, 1.1074. No measured QCD threshold is drawn: at this M the delivery temperature
  runs smoothly through it.
- **Crosses:** β = 0.5, 0.75 and 0.9, where no α gave a BBN result: PRyMordial's spline floor, a
  classical turning point, or no effective minimum. **Triangles:** β = 1.0, where ΔY_p = +76% and
  Δ(D/H) = +323% at every α, above the axes.

## What it says

Of the 1914 histories with yields, 893 are excluded: 561 by Y_p and 807 by D/H. None is excluded by
ΔN_eff, whose largest value for β ≥ 1.06 is 0.12 (at α = 0.15). Every α is excluded at β = 1.0, at
most of 1.06–1.12 and 1.1575–1.1925, and at 1.25 and 1.39; the band is jagged in β and no single α
sets its edges. The largest β at which any α is excluded is 1.94. The tail converges to zero shift.

**The sign.** Every shift that matters is positive. At β ≤ 1.06 all are (ΔY_p ≥ +3.3%). In
1.06–1.50, 57 of 1115 histories have ΔY_p < 0 and 21 have Δ(D/H) < 0, none below −0.33% and −0.39%,
at β where the shift crosses zero (1.14–1.15, 1.22–1.24, 1.44–1.50). Above 1.5 the shifts scatter
about zero with both signs, ΔY_p down to −0.45%. Every negative shift lies well inside the ±2.4%
tests. Where these small negative values come from is not established.

## Changes from the 2026.6.0 draft

Its review's four fixes: the tests are now two-sided by the user's ruling; no black triangle is
mislabelled (the crosses name what they are); no blue band ends at the analytic threshold (there is
none); and the figure is self-contained here. New: the band over α, the ΔN_eff panel, the
electroweak cut, and the differential tests in place of the measurements' absolute ranges.
