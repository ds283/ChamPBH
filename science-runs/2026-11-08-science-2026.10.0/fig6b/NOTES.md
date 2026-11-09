# fig6b — Figure 6 around M_crit

**Of 2026-11-09**, accepted by the user as drawn the same day. It replaces the plan of the 2026.8.0 run
([`../../2026-11-01-science-2026.8.0/fig6b/PLAN.md`](../../2026-11-01-science-2026.8.0/fig6b/PLAN.md)), which was
written before the one-loop law and for a block C of three M, 62 β and seven fixed α. `fig6b.py` was written by a
Claude Code session.

```bash
./venv/bin/python science-runs/2026-11-08-science-2026.10.0/fig6b/fig6b.py
```

It reads `../fig6/inputs/histories.csv` (committed with Figure 6) and the main store's sidecar, needs no Ray and
no store, and writes `fig6b.pdf`, `.png` and `.provenance.json`, which records the input's SHA-256, the git
commit and the numbers below.

**Ruled by the user, 2026-11-09:** the layout proposed in the session, "Yes, please try that layout"; ln R at the
first impact as the fourth row, "R at the first impact is probably the more helpful line (and that may also be
the case for Fig. 6 itself)"; and, after the session showed the fixed law to be M-independent here, the fixed law
drawn once.

## The data

Block C of the main store: M = 10⁻¹⁵ and 10⁻¹² M_P, the sparse β grid (30 values, 26 surfing), φ* = 5 M_P, the
one-loop law at A = 1 and the fixed law at α = 0.01 and 0.1 (the user's rulings of 2026-11-07 and 2026-11-08).
The laboratory mass's A = 1 and fixed-law lines are Figure 6's, on the dense grid.

- **The delivery temperatures** at both M equal the laboratory mass's to 4×10⁻⁷, so the regions (not at the wall
  below β ≈ 1.066, the electroweak cut at 3.000–3.005, the QCD threshold) are Figure 6's.
- **The fixed law does not depend on M here either.** At every surfing (α, β) its ΔY_p and Δ(D/H) at 10⁻¹⁵ and
  10⁻¹² equal the laboratory mass's to 0.007 and 0.03 percentage points, and its ΔN_eff to 4×10⁻⁴ (10⁻¹⁵) and
  2.4×10⁻³ (10⁻¹²) relative, with a median of 10⁻¹¹. At the non-surfing β = 1.0 ΔN_eff differs by 2.7% at 10⁻¹².
  These are invisible at the figure's scale, so it is drawn once. *(In the session the agreement was first
  quoted as 2×10⁻⁸ "up to 10⁻¹²"; that figure holds for the laboratory region, 10⁻⁴² to 10⁻³⁰, only.)*
- **Failures at surfing β**, one-loop A = 1: β = 1.1 and 1.2 at 10⁻¹⁵ and β = 1.4 at 10⁻¹², each at the
  30-integration tuning cap ("the normalization did not converge"), marked ✕ at the axis's foot. The fixed law
  has none there.

## What it shows

- **ln R at the first impact** sets the one-loop law's regime. At 10⁻¹² it is −34 to −1.5 over the surfing β:
  every history drains classically, by R·S(L), and ΔN_eff today is 10⁻²⁰ to 7×10⁻⁶. At 10⁻¹⁵ it crosses R = 1:
  classical up to β = 2.2 (ln R up to 1.5; ΔN_eff 10⁻⁷ to 10⁻⁵), the gap at β = 2.3–3.0 (ln R 2.0–3.8; ΔN_eff
  2.5×10⁻³ to 1.8×10⁻²), and one-loop past the electroweak cut (ln R 21–26; ΔN_eff 1.6×10⁻³ to 3.3×10⁻²). The
  jump at the cut is the higher impact energy of a history that surfs the electroweak kick. The laboratory
  mass's ln R is 35–168, far above the axis, and is not drawn.
- **ΔN_eff today** follows: the one-loop law's drain switches off between the laboratory mass and M_crit, except
  where ln R stays large: past the electroweak cut at 10⁻¹⁵, where ΔN_eff exceeds the laboratory mass's at A = 1
  for β = 3.5–10 (0.033 against 0.0076 at β = 3.5) and falls below it at β = 20. The fixed law
  drains its α at every M, as before.
- **The yields** at M_crit differ from the laboratory mass's under the one-loop law: in the window 1.06–1.5 by up
  to 1.7 (10⁻¹⁵) and 2.9 (10⁻¹²) percentage points in Y_p and 5.0 and 13 in D/H, above it by at most 0.4 and 1.1,
  because with almost no drain the field's own motion
  through BBN is different. Excluded surfing β at A = 1: 3 of 24 at 10⁻¹⁵ (up to β = 1.4), 4 of 25 at 10⁻¹²
  (up to β = 1.3), against 60 of 179 at the laboratory mass (up to β = 1.5, dense grid). These exclusions come
  from the field's motion, not dark radiation. The window's structure is not resolved on the sparse grid, so
  the M_crit lines there join samples 0.05–0.1 apart across features the laboratory lines show.
- **The relativistic margins** fail almost everywhere here: the fixed law's weighted birth or reflection margin
  in every surfing history at both M (52 of 52), the one-loop law's all-generation margins in 23 of 24 (10⁻¹⁵)
  and 25 of 25 (10⁻¹²). The tracker margin fails nowhere. They are not marked on the figure (the 2026.8.0 plan's
  option 3(a)); under the one-loop law the drains they concern are tiny at 10⁻¹² and outside the window at 10⁻¹⁵.

## Ruled

The user, 2026-11-09: "Yes, please leave this figure as drawn and commit it. Leave the ln R row out of Fig. 6."
This is the figure of record, and Figure 6 keeps its parking row.
