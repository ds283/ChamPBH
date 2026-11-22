# m-trend — the trend in M across the laboratory region, 2026.14.0, one figure per wall

**Ported 2026-11-20, before the run; changed 2026-11-22, after it.** This note is rewritten once the run's figures have
been seen. Below the line is 2026.11.0's note, unchanged. `m_trend.py`'s docstring lists what the port changed.

**pl1 has no β = 10 in panel (b).** All 84 of pl1's histories at β = 10 failed (7 M × 12 rows): `(ln V)''` overflowed in
`InversePowerModel.d2_logV_dphi2` at φ ≈ 6.6 × 10⁻¹⁵⁶ to 10⁻¹⁶⁶ M_P. So the export has no bounce files for them, and
the first run of the port stopped at the first missing file. On 2026-11-22, at the user's request ("fix m_trend"), the
script was changed to skip any (β, M) that has no bounce file. It lists each one under `first_impact_missing` in the
provenance. A β with none at any M keeps its legend entry, which says "failed at every M, none drawn". Panels (a) and
(c) read only stored, successful histories, so they were unchanged by this; the β = 10 failures are absent from them as
from any figure. On `exp` and `pl2` nothing is missing and the output is unchanged.

---

# m-trend — the trend in M across the laboratory region, 2026.11.0

**Of 2026-11-14**, made by a Claude Code session (the run's orchestrator) at the user's request ("write the record and
commit any outstanding work, then the analyses"). **Not yet seen by the user.** It repeats the 2026.10.0 figure
([`../../2026-11-08-science-2026.10.0/m-trend/`](../../2026-11-08-science-2026.10.0/m-trend/NOTES.md)) on this run's
store, with that folder's ruling (the user's of 2026-11-09: the three panels, the law's range and the grey band at
ℓ > 45 kept as drawn) and no new choice. That NOTES.md says what each panel is and derives the closed form of panel
(b); none of that is repeated here.

```bash
./venv/bin/python science-runs/2026-11-13-science-2026.11.0/m-trend/m_trend.py
```

It reads `../fig6/inputs/histories.csv` (this run's main-store export), `inputs/bounces/` (unchanged copies of this
run's `plot_ScalarHistory.py --no-plots` export: the bounce records at A = 1 for β = 1.2, 2 and 10 at every M, the same
21 histories as 2026.10.0's) and the main store's sidecar. It writes `m_trend.pdf`, `.png` and `.provenance.json`.

`m_trend.py` is 2026.10.0's with the run's name and the sidecar changed; committed in `2072fab`, the commit the
provenance file records (clean tree).

## What it says

**Every finding of 2026.10.0's NOTES.md stands.** From `m_trend.provenance.json`:

- **(a) ΔN_eff today relative to 4×10⁻³¹** (median over the 26 surfing β of the sparse grid):

  | M / M_P | A ≤ 3 | A = 5 | A = 10 |
  |---|---|---|---|
  | 10⁻³⁰ | 1.017–1.026 | 1.009 | 1.003 |
  | 10⁻³² | 0.909–0.937 | 0.964 | 0.989 |
  | 10⁻³³ | 0.861–0.901 | 0.943 | 0.982 |
  | 10⁻³⁶ | 0.751–0.812 | 0.877 | 0.964 |
  | 10⁻⁴⁰ | 0.676–0.733 | 0.817 | 0.949 |
  | 10⁻⁴² | 0.677–0.726 | 0.816 | 0.947 |

  2026.10.0's to within 0.005 at every (A, M) (0.002 or less except at A = 0.2 and 0.5), except at A = 3, the top of
  the A ≤ 3 range at 10⁻³⁶–10⁻⁴², which falls by up to 0.017 (0.744 to 0.726 at 10⁻⁴²). The fall levels off between 10⁻⁴⁰ and 10⁻⁴² as before. The
  fixed law is at 1 to within 9×10⁻⁹ (5×10⁻⁹ in 2026.10.0).
- **(b) ℓ at the first impact**, A = 1: the closed form reproduces every stored ℓ (to 6×10⁻¹⁴ at β = 1.2 and 2, and
  2.9×10⁻⁶ at β = 10, the fixed point's tolerance). At β = 1.2 and 2 the stored ℓ equal 2026.10.0's to 6×10⁻⁴, and the
  maximum still falls at about 10⁻⁴². **At β = 10, which surfs the electroweak kick, the first impact carries less
  energy than in 2026.10.0**: ln(E/V₀) = 126.9 against 128.4, so ℓ is lower by 0.4–0.6 at every M (60.7 against 61.3
  at 10⁻⁴²), and its maximum is 61.7 (62.5), reached near 10⁻⁴⁴ as before. This is the electroweak-eos table's
  change in the plasma above 100 GeV, through which a β = 10 history surfs. The grey band (ℓ > 45, the law
  extrapolated beyond one-loop-drain 02d's measurement) still holds every M ≤ 10⁻³⁶ at β = 10 and M ≤ 10⁻⁴⁰ at β ≤ 2.
- **(c) The largest excluded β** at each (A, M) and their counts are 2026.10.0's in 69 of the 70 cells. The exception
  is A = 2 at 10⁻³²: 2 β excluded up to 1.15, where 2026.10.0 had 3 up to 1.25. It is one of the window's reshuffles
  that 2026.10.0's note describes, which have no trend in M. A = 5's fall from 9 β excluded at the laboratory mass to
  2–3 at 10⁻³⁶ and below is unchanged.

## What it does not show

As in 2026.10.0, the sparse grid holds seven β in the window 1.06–1.5 and does not resolve its structure, and M below
10⁻⁴² is not built (deferred by the user, 2026-11-09).
