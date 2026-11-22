# m-trend — the trend in M across the laboratory region, 2026.14.0, one figure per wall

**Of 2026-11-22**, written by a Claude Code session at the user's request ("please write the notes"), from the figures
of `4ec8e68`. **Reviewed by the user on 2026-11-22** ("I've reviewed all those notes"). It repeats 2026.11.0's figure for each wall, with that figure's rulings and
one new choice, the user's of 2026-11-19: on the power wall panel (b) shows ln R. Below the line is 2026.11.0's note,
unchanged. That note and 2026.10.0's say what each panel is.

```bash
./venv/bin/python science-runs/2026-11-21-science-2026.14.0/m-trend/m_trend.py --wall {exp,pl1,pl2}
```

It reads `../fig6/inputs/<wall>/histories.csv`, and `inputs/<wall>/bounces/`, the bounce records at A = 1 for β = 1.2,
2 and 10 at every M, copied unchanged from the wall's `plot_ScalarHistory.py --no-plots` export. It also reads the
wall's store's sidecar. It writes `m_trend-<wall>.pdf`, `.png` and `.provenance.json`. Every provenance file records a
clean tree: `exp`'s records `c79446a`, where it was regenerated after the ℓ curve was fixed (below), and `pl1`'s and
`pl2`'s record `6bc8eed`.

## The port and the changes after the build

- **The port, 2026-11-20** (`m_trend.py`'s docstring):
  - one figure per wall;
  - the one-loop rows are the wall's own drain model;
  - the n in the bounce path is the wall's;
  - on `exp` the region below 4×10⁻³¹ M_P is shaded in panels (a) and (b), and noted in (c), as law v2 used beyond its
    fitted domain ln(V₀/M⁴) ≤ 10 (PLAN.md P4, the user's ruling to extrapolate it);
  - on the power wall, panel (b) draws ln R at the first impact, as points only.
- **pl1 has no β = 10 in panel (b).** All 84 of pl1's histories at β = 10 failed (7 M × 12 rows): `(ln V)''`
  overflowed in `InversePowerModel.d2_logV_dphi2` at φ ≈ 6.6 × 10⁻¹⁵⁶ to 10⁻¹⁶⁶ M_P. So the export has no bounce files
  for them, and the port's first run stopped at the first missing file. On 2026-11-22, at the user's request ("fix
  m_trend"), the script was changed (`6bc8eed`) to skip any (β, M) with no bounce file:
  - each one is listed under `first_impact_missing` in the provenance;
  - a β with none at any M keeps its legend entry, which says "failed at every M, none drawn";
  - panels (a) and (c) read only stored, successful histories, so the change does not affect them.

  On `exp` and `pl2` nothing is missing. Their provenance equals that of a run before the change, apart from the script
  hash and git fields.
- **The ℓ curve, fixed later on 2026-11-22** at the user's request ("fix the ell_law curve for the new k_ex and
  regenerate that plot"), in `c79446a`. `exp` panel (b)'s line comes from `ell_law`, a closed form for ℓ on V₀ e^{M/φ}.
  That closed form still used the k_ex from before power-law-wall 07b, λE^{1/2}/2 with λ = x²/M, x = M/φ. It was 0.11,
  0.10 and 0.08 low at 4×10⁻³¹ and 2.6, 2.1 and 0.7 low at 10⁻⁴² (β = 1.2, 2, 10), and the stored ℓ at 10⁻⁴² lay
  above the dashed "maximum" drawn from it.

  `ell_law` now uses the pulse rate of `ExponentialModel._pulse_rate_hook` at n = 1, x(x² + 6x + 6)/((x + 2)M), so
  that k_ex = ½E^{1/2}|V'''/V''| as the code forms it. It reproduces all 21 stored first-impact ℓ to 1.4×10⁻¹⁴. With
  the turnaround's ln M substituted,

  ℓ = ½ ln(E/V₀) − ln 2 + h(x), h(x) = −½ ln x + ln(x² + 6x + 6) − (3/2) ln(x + 2) − x/2,

  and dh/dx < −½ for every x > 0. As M falls, x falls and ℓ rises without bound, so **under this k_ex ℓ has no
  maximum**. The old k_ex gave one at x = √3 − 1. The dashed line and the provenance's `ell_max` are removed, and the
  caption says so. Only `exp` was regenerated (`dcbce18`). The fix does not touch the power-wall path: a trial run at
  `c79446a` gave the same `pl1` and `pl2` provenance, apart from the script hash and git fields.

## What it says

- **(a) ΔN_eff today relative to 4×10⁻³¹** (median over the surfing β of the sparse grid: 26 on `exp` and `pl2`, 24 on
  `pl1`):

  | M / M_P | `exp` A ≤ 3 | A = 5 | A = 10 | `pl2` A ≤ 3 | A = 5 | A = 10 | `pl1` A ≤ 3 | A = 5 | A = 10 |
  |---|---|---|---|---|---|---|---|---|---|
  | 10⁻³⁰ | 1.018–1.026 | 1.010 | 1.003 | 1.009–1.012 | 1.007 | 1.002 | 1.006–1.008 | 1.005 | 1.001 |
  | 10⁻³² | 0.908–0.935 | 0.959 | 0.988 | 0.955–0.966 | 0.974 | 0.994 | 0.971–0.978 | 0.981 | 0.994 |
  | 10⁻³³ | 0.858–0.898 | 0.933 | 0.981 | 0.928–0.946 | 0.958 | 0.991 | 0.954–0.964 | 0.970 | 0.990 |
  | 10⁻³⁶ | 0.746–0.804 | 0.861 | 0.964 | 0.857–0.891 | 0.912 | 0.981 | 0.905–0.928 | 0.937 | 0.976 |
  | 10⁻⁴⁰ | 0.660–0.721 | 0.790 | 0.946 | 0.777–0.819 | 0.848 | 0.969 | 0.850–0.883 | 0.896 | 0.953 |
  | 10⁻⁴² | 0.635–0.697 | 0.770 | 0.941 | 0.740–0.791 | 0.823 | 0.958 | 0.825–0.861 | 0.876 | 0.941 |

  The fixed law is at 1 to within 9×10⁻⁹ on `exp` and `pl1` and to within 0 on `pl2` (as in 2026.11.0).

  **The power walls fall more gently, n = 1 most gently, and the walls converge.** The median ΔN_eff today at A = 1 over
  the surfing β is:

  | M / M_P | `exp` | `pl2` | `pl1` |
  |---|---|---|---|
  | 4×10⁻³¹ | 0.0425 | 0.0355 | 0.0315 |
  | 10⁻⁴² | 0.0263 | 0.0264 | 0.0258 |

  So the walls nearly meet at 10⁻⁴² from different starting points. The gentler fall of n = 1 is where it starts at
  laboratory M, not where it ends. The falling branch of each law accounts for this:
  - ln R at the first impact rises by 110.5 = 4 ln 10¹² from 10⁻³⁰ to 10⁻⁴² on both power walls, at β = 1.2 and 2 alike
    (507.4 → 617.9 on `pl1`, 305.3 → 415.8 on `pl2`), as R ∝ M⁻⁴ at fixed β;
  - on the falling branch y = A_f/(x − c), with y ≈ f this small, the same rise is a smaller fraction of a larger
    ln R: (617.9 − 15.0)/(511.1 − 15.0) = 1.22 on `pl1`, against 1.36 on `pl2`;
  - on the exponential the law reads ℓ, which rises from 32.4 to 51.8, a ratio (ℓ − 1.19) of 1.62.

  The first impact's f alone predicts ratios of 0.83, 0.74 and 0.63 at 10⁻⁴² (`pl1`, `pl2`, `exp`, A = 1, β = 1.2). The
  medians over β are 0.84, 0.77 and 0.67. That the three laws give nearly the same f at 10⁻⁴² (0.045 to 0.049 at the first
  impact) follows from where their separately fitted branches put them. These outputs cannot say whether that is
  physics or the fits.
- **(b) The first impact, A = 1.**
  - **`exp`, ℓ.** At laboratory M the stored ℓ equal 2026.11.0's to 0.1 (β = 1.2: 32.4 against 32.3). Below it they are
    higher, increasingly so as M falls: 51.8 against 49.2 at 10⁻⁴² at β = 1.2, 53.4 against 51.2 at β = 2, and 61.4
    against 60.7 at β = 10. This is the k_ex ruling (one k_ex = ½E^{1/2}|V'''/V''|, power-law-wall 07b–07d). ℓ no longer
    levels off between 10⁻⁴⁰ and 10⁻⁴² (49.9 → 51.8 at β = 1.2). Under the new k_ex it has no maximum and keeps
    rising below the sample: the closed form gives 53.3, 57.9 and 67.4 at 10⁻⁴⁴, 10⁻⁵⁰ and 10⁻⁶⁰ at β = 1.2. In
    2026.11.0 it had a maximum near 10⁻⁴², and near 10⁻⁴⁴ at β = 10. The drawn line passes through every point. The
    grey band (ℓ > 45) holds the same points as before: M ≤ 10⁻³⁶ at β = 10, M ≤ 10⁻⁴⁰ at β ≤ 2.
  - **`pl1` and `pl2`, ln R.** At every M, the stored ln R at the first impact equals `histories.csv`'s
    `log_R_first_impact` exactly. It runs from 507 to 638 on `pl1` (β = 1.2 and 2) and from 305 to 490 on `pl2` (β = 1.2
    to 10). `pl1` has no β = 10.
- **(c) The largest excluded β.** A = 5 falls from 9 β excluded at laboratory M to 2 at 10⁻⁴² on `exp` (as in
  2026.11.0), from 7 to 2 on `pl2`, and from 3 to 2 on `pl1`. From 10⁻³⁶ down, the three walls' A = 5 rows are alike
  (1.15–1.2, 2–3 β). On `exp`, 26 of the 70 cells differ from 2026.11.0's, in count or largest β or both. All of them are below
  A = 5 except two A = 5 cells (10⁻⁴⁰: 1.2 → 1.15; 10⁻³³: 1.6 → 1.5). They have no trend in M, like the reshuffles
  2026.11.0's note describes. A = 10 is at 1.80 (10 β) in every cell on every wall.

## Observations, not acted on

- **The power walls' panel (b) has no "beyond the measured range" marking.** Their law was checked to ln R = 100
  (n = 1) and 60 (n = 2), and every point drawn is at ln R ≥ 305 (`../fig6/NOTES.md`, observations).

  **Corrected later on 2026-11-22.** The law was measured to ln R = 554.5 (n = 1) and 344.7 (n = 2), not 100 and 60
  (`../fig6/NOTES.md` has the correction). Panel (b)'s points run to 638 at n = 1 and 490 at n = 2, so the points
  beyond the measured range are β = 1.2 and 2 at 10⁻³⁶ M_P and below on both walls, and every point of n = 2's β = 10
  (379.5 to 490.0). At 10⁻³³ M_P, β = 2 sits at the edge on both walls (554.5 and 344.7). The user ruled on 2026-11-22: "For
  now we'll accept the `ln R` extrapolation".

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
