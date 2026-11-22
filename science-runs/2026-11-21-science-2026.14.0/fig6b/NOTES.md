# fig6b — Figure 6 around M_crit, 2026.14.0 (the exponential)

**Of 2026-11-22**, written by a Claude Code session at the user's request ("please write the notes"), from the figure
of `4ec8e68`. **Reviewed by the user on 2026-11-22** ("I've reviewed all those notes"). It repeats 2026.11.0's Figure 6b on this run's exponential store, with that
figure's rulings (the user's of 2026-11-09) and no new choice. Below the line is 2026.11.0's note, unchanged. That note
and 2026.10.0's say what each element is. Block C (M_crit) was built for the exponential alone (PLAN.md), so there is no
power-wall Figure 6b.

```bash
./venv/bin/python science-runs/2026-11-21-science-2026.14.0/fig6b/fig6b.py
```

It reads `../fig6/inputs/exp/histories.csv` and the exponential store's sidecar, and writes `fig6b.pdf`, `.png` and
`.provenance.json`. The provenance records `6bc8eed` and a clean tree. The port (2026-11-20):
- the one-loop rows are `one-loop-exponential-v2`;
- rows are filtered by `model` and `n`;
- `EW_BRACKET` is read from Figure 6's `WALLS["exp"]`, (2.980, 2.985) since 2026-11-22.

## What it shows

**Figure 6b is 2026.11.0's at M_crit to within a few percent in ΔN_eff and unchanged in every count.** The changes
between the trees (one-loop law v2, k_ex, the spectrum tables) hardly reach M_crit:
- the classical branch R S(L) is unchanged in v2;
- ln R at the first impact is unchanged to 8×10⁻⁷;
- block C's one-loop ΔN_eff moves by a median of 0 (5–95%: −3.3% to +0.8%, `../changes/`).

Against 2026.11.0's note, finding by finding:
- **The fixed law still does not depend on M here.** At 10⁻¹⁵ and 10⁻¹², ΔY_p and Δ(D/H) equal the laboratory mass's
  to 0.005 and 0.019 percentage points, and ΔN_eff to 1.5×10⁻³ and 4.2×10⁻³ relative. These are 2026.11.0's numbers.
- **The regimes, one-loop law, A = 1:**
  - 10⁻¹²: classical in all 25 surfing histories;
  - 10⁻¹⁵: 13 classical, 7 gap and 5 one-loop, as before;
  - ΔN_eff today runs from 1.8×10⁻²⁰ to 7.4×10⁻⁶ at 10⁻¹² (unchanged), and from 1.8×10⁻⁸ to 0.0378 at 10⁻¹⁵ (0.0382).
- **Exclusions at A = 1:**
  - 4 of 25 surfing β at each of 10⁻¹⁵ and 10⁻¹², up to β = 1.3, as before;
  - 59 of 183 at laboratory M, up to β = 1.5, as before; the laboratory mass's ΔN_eff tops out at 0.0675 (0.0710).
- **Failures at surfing β**: β = 1.4 at both M, as before.

The laboratory mass's lines are this run's Figure 6 for `exp`, whose changes are in `../fig6/NOTES.md`.

---

# fig6b — Figure 6 around M_crit, 2026.11.0

**Of 2026-11-14**, made by a Claude Code session (the run's orchestrator) at the user's request ("write the record and
commit any outstanding work, then the analyses"). **Not yet seen by the user.** It repeats the 2026.10.0 Figure 6b
([`../../2026-11-08-science-2026.10.0/fig6b/`](../../2026-11-08-science-2026.10.0/fig6b/NOTES.md)) on this run's
store, with that folder's rulings (the user's of 2026-11-09: Figure 6's columns, tests and first three rows, ln R at the
first impact as the fourth row, the fixed law drawn once) and no new choice. That NOTES.md says what each element is.

```bash
./venv/bin/python science-runs/2026-11-13-science-2026.11.0/fig6b/fig6b.py
```

It reads `../fig6/inputs/histories.csv` (this run's main-store export, committed with Figure 6) and the main store's
sidecar, needs no Ray and no store, and writes `fig6b.pdf`, `.png` and `.provenance.json`.

`fig6b.py` is 2026.10.0's with the run's name, the sidecar and `EW_BRACKET` changed, the last moved to 2.980–2.985 as
in this run's Figure 6. Committed in `9fc811f`, the commit the provenance file records (clean tree).

## The data

Block C: M = 10⁻¹⁵ and 10⁻¹² M_P, the sparse β grid, φ* = 5 M_P, the one-loop law at A = 1 and the fixed law at
α = 0.01 and 0.1; the laboratory mass's lines are this run's Figure 6.

- **The delivery temperatures** at both M equal the laboratory mass's to 4.3×10⁻⁷, so the regions are this run's
  Figure 6's: not at the wall below β = 1.069 (computed from the data), the electroweak cut at 2.980–2.985 and the QCD
  threshold. **β = 3.0 now surfs the electroweak kick at M_crit** (it delivers at 48 GeV), where in 2026.10.0 it did
  not.
- **The fixed law still does not depend on M here.** At every surfing (α, β) its ΔY_p and Δ(D/H) at 10⁻¹⁵ and 10⁻¹²
  equal the laboratory mass's to 0.005 and 0.019 percentage points, and its ΔN_eff to 1.5×10⁻³ (10⁻¹⁵) and 4.2×10⁻³
  (10⁻¹²) relative (2026.10.0: 4×10⁻⁴ and 2.4×10⁻³). It is drawn once.
- **Failures at surfing β**, one-loop A = 1: β = 1.4 at both M, at the 30-integration tuning cap, marked ✕ at the axis's
  foot. 2026.10.0 failed at β = 1.1 and 1.2 at 10⁻¹⁵ and 1.4 at 10⁻¹²: β = 1.1 and 1.2 at 10⁻¹⁵ converge here, and
  1.4 fails there instead (the run README, "Failed histories").

## What it shows

Against 2026.10.0's NOTES.md, finding by finding:

- **ln R at the first impact** sets the regime, as before. At 10⁻¹² every surfing history drains classically, ln R
  from −34 to −4.8 (−1.5 in 2026.10.0), ΔN_eff today 1.8×10⁻²⁰ to 7.4×10⁻⁶. At 10⁻¹⁵ it crosses R = 1 as before:
  classical up to β = 2.2 (ln R −6.6 to 1.5; ΔN_eff 1.8×10⁻⁸ to 8.3×10⁻⁶), the gap at β = 2.3–2.9 (ln R 2.0–3.8;
  ΔN_eff 2.5×10⁻³ to 1.8×10⁻²), and one-loop past the electroweak cut, **now from β = 3.0** (ln R 17.9–22.8, against
  20.6–26.1; ΔN_eff 2.0×10⁻³ to 3.8×10⁻²). The gap ends at 2.9 because β = 3.0 has moved across the cut. The
  laboratory mass's ln R is 122–165, far above the axis.
- **ΔN_eff today**: the one-loop drain still switches off between the laboratory mass and M_crit except past the
  electroweak cut at 10⁻¹⁵, where it exceeds the laboratory mass's at A = 1 for β = 3.0–10 (0.038 against 0.016 at
  β = 3.0, the largest at M_crit; 0.033 against 0.0061 at β = 3.5) and falls below it at β = 20. The fixed law drains
  its α at every M.
- **The yields** at M_crit differ from the laboratory mass's under the one-loop law: in the window 1.06–1.5 by up to
  2.9 percentage points in Y_p and 13 in D/H at both M (at 10⁻¹⁵ 2026.10.0 had 1.7 and 5.0, without β = 1.1 and 1.2,
  which failed there), above it by at most 0.4 and 1.1. Excluded surfing β at A = 1: **4 of 25 at 10⁻¹⁵, up to β = 1.3**
  (2026.10.0: 3 of 24, up to 1.4, the β = 1.4 history that fails here), and 4 of 25 at 10⁻¹², up to 1.3, as before;
  against 59 of 183 at the laboratory mass, up to β = 1.5 (60 of 179 in 2026.10.0, whose count began at its wall edge
  1.06625). They come from the field's motion, not dark radiation, and the sparse grid does not resolve the window.
- **The relativistic margins fail almost everywhere here**: the fixed law's weighted birth or reflection margin in all
  52 surfing histories at each M, and the one-loop law's all-generation birth or reflection margins in 25 of 25 at
  each M (23 of 24 at 10⁻¹⁵ in 2026.10.0). As in 2026.10.0 they are not marked on the figure.

## Observation, not acted on

2026.10.0's NOTES.md says "The tracker margin fails nowhere" here. By `histories.csv`'s
`all_worst_tracker_margin` (below ln 10), the one-loop law's tracker margin fails in 10 surfing histories at 10⁻¹⁵ and
9 at 10⁻¹², in that run's export as in this one, and that run's relativistic analysis records the worst tracker margin
at M_crit as −26 and −15 ("At M_crit it fails"). The sentence holds of the fixed law's weighted tracker share, which is
0 in every surfing history at M_crit in both runs. It is recorded here and the 2026.10.0 note is not edited.
