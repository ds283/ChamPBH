# m-trend — the trend in M across the laboratory region

**Of 2026-11-09**, kept by the user as generated the same day. Drawn at the user's request ("please try the 3-panel figure
first, and we'll see what it looks like"), after a survey in the session that found the trend. `m_trend.py` was
written by a Claude Code session.

```bash
./venv/bin/python science-runs/2026-11-08-science-2026.10.0/m-trend/m_trend.py
```

It reads `../fig6/inputs/histories.csv` (committed with Figure 6), `inputs/bounces/` (unchanged copies of the
run's `plot_ScalarHistory.py --no-plots` export: the bounce records at A = 1 for β = 1.2, 2 and 10 at every M, 21
files) and the main store's sidecar. It needs no Ray and no store, and writes `m_trend.pdf`, `.png` and
`.provenance.json`, which records every input's SHA-256, the git commit and the numbers below.

## The sample

M = 10⁻³⁰, 4×10⁻³¹, 10⁻³², 10⁻³³, 10⁻³⁶, 10⁻⁴⁰ and 10⁻⁴² M_P, on the sparse β grid (30 β; block A's dense grid at
4×10⁻³¹ holds it), φ* = 5 M_P, the one-loop law at all ten A and the fixed law at α = 0.01 and 0.1. The panels use
the 26 surfing β (≥ 1.06). The transition window 1.06–1.5 holds only seven of them, so the sparse grid does not
resolve the window's structure that Figure 6 shows at 4×10⁻³¹.

## What it shows

- **(a) ΔN_eff today relative to 4×10⁻³¹**, the median over the surfing β for each A, the range over β shaded
  for A = 1. At A ≤ 3 it falls as M falls, to 0.91 at 10⁻³², 0.86 at 10⁻³³, 0.75 at 10⁻³⁶ and 0.68–0.74 at
  10⁻⁴⁰, and is the same at 10⁻⁴² (panel (b) says why); at 10⁻³⁰ it is 1.02–1.03. A = 5 falls to 0.82, A = 10 only to 0.95, since
  their drains are near the cap. ΔN_eff at 1 MeV follows the same ratios. The fixed law is at 1 to within
  5×10⁻⁹ at every M (its yields to within 0.03%).
- **(b) ℓ = ln(k_ex/m_ta) at the first impact**, A = 1, β = 1.2, 2 and 10: the stored values (points), and the
  law's ℓ(M) at that impact's E (line), solved in closed form on V = V₀e^{M/φ}. With x = M/φ at the turnaround,
  V'' = V₀e^x x³(x + 2)/M², λ = x²/M, m_ta⁴ = 32π²E/ℓ, and

      ℓ = ½ ln(E/V₀) − ln 2 − x/2 + ½ ln(x/(x + 2)).

  The closed form reproduces every stored ℓ (to 2×10⁻¹⁴ at β = 1.2 and 2, 8×10⁻⁷ at β = 10, the fixed point's
  tolerance). As M falls the turnaround moves out of the wall's exponential part (x from about 40 at 10⁻³⁰ to
  0.25–0.44 at 10⁻⁴² for β ≤ 2) and **ℓ has a maximum**, ½ ln(E/V₀) − ln 2 − 1.024 (dashed), at
  x = √3 − 1. For β = 1.2 and 2 it falls at 10⁻⁴²; for β = 10, whose first impact carries more energy
  (ln(E/V₀) = 128 against 102 and 106), near 10⁻⁴⁴. **Below the maximum ℓ falls again**, by about 0.77 per
  decade (x ∝ M^{2/3} in the wall's 1/φ tail): at β = 2 the law gives 50.1 at 10⁻⁴⁴, 45.5 at 10⁻⁵⁰ and 39.3 at
  10⁻⁶⁰, about its value at 10⁻³³ (not sampled; in the provenance). So the levelling-off of panel (a) between
  10⁻⁴⁰ and 10⁻⁴² is this turning point, and ΔN_eff would rise again at smaller M. *(A first reading in the
  session took the dashed value without the −1.024 for a limit approached from below; the algebra and the
  stored values show the maximum.)* **The law is extrapolated there:** one-loop-drain 02d measured it for
  one-loop ℓ up to 44.98 (verification §2d.5); everything above ℓ = 45 (grey) lies beyond that, which includes
  every M ≤ 10⁻³⁶ at β = 10 and M ≤ 10⁻⁴⁰ at β ≤ 2.
- **(c) The largest excluded β** at each (A, M), with the count of the 26 surfing β excluded, by Figure 6's
  tests. At A = 0.05–0.2, 3 and 10 the exclusions are the same at every M. At A = 0.5–2 they move by up to three
  β, without a trend in M: the window's yields depend on the bounce sequence, which M reshuffles (D/H changes
  by a median 0.9%, up to 10.7%, in 1.06–1.5 between 4×10⁻³¹ and 10⁻⁴²; by 0.2% in 1.5–3; by less than 0.05%
  above 3). **A = 5 is the exception:** 9 β excluded up to 1.70 at the laboratory mass, 8 up to 1.60 at 10⁻³²
  and 10⁻³³, 2–3 up to 1.15–1.20 at 10⁻³⁶ and below. Its exclusions come from the dark radiation present during
  BBN (Figure 6's notes), which needs ΔN_eff at 1 MeV of about 0.17; the fall of about 12% in ΔN_eff with M takes
  most of its β below that.

## Ruled

The user, 2026-11-09: "For now keep that figure as generated, including law's range, and panel (c). I will
come back to the question of whether to add more histories at `M < 1E-42` later." The three panels stand as
drawn, the grey band marking ℓ > 45 included; building M below 10⁻⁴² is deferred by the user.
