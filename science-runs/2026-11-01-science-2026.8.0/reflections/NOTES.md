# reflections — the reflection structure against M

**Draft of 2026-11-01**, for the user to review. Written by a Claude Code session at the user's
request, from PLAN.md §5.3, with one change: the companion panel shows the number of reflections
against M rather than the parking temperature (below, "Changes from the plan").

```bash
./venv/bin/python science-runs/2026-11-01-science-2026.8.0/reflections/extract_inputs.py   # once
./venv/bin/python science-runs/2026-11-01-science-2026.8.0/reflections/reflections.py
```

- `extract_inputs.py` gathers four columns (`T_Jordan_GeV`, `phi_Einstein_Mp`, `pi_Einstein_Mp`,
  `rho_chi_over_rho_rad`) of the reflection store's 42 `fields.csv`, written by the run's
  `plot_ScalarHistory.py` step, into `inputs/fields.csv.gz`, unchanged, with every source's SHA-256
  in `inputs/sources.json`. The 42 files are 105 MB together; the extract is 7.7 MB.
- `reflections.py` reads that extract, `../fig6/inputs/histories.csv` (the main store's histories,
  for the reflection counts) and the stores' sidecars, and needs no Ray. It writes
  `reflections.pdf`, `reflections.png` and `reflections.provenance.json` (the script's and inputs'
  SHA-256, the commit and whether the tree was dirty, the stores' digests, and the parking
  temperatures it found).

## What it shows

Three columns, β = 1.2, 2.0 and 2.9; φ* = 5 M_P throughout.

1. **The field's kinetic fraction**, π²/6, from 3H²M_P²(1 − π²/6) = ρ_R(1 + f_m) + ρ_χ with π in M_P,
   against T_J: dissipation mode at α = 0.08 for M = 4×10⁻³¹ (laboratory), 10⁻¹⁵ and 10⁻¹² M_P, and
   natural mode at M = 10⁻³ and 0.5 M_P. Dissipation-mode histories park; the arrow marks where the
   laboratory history's fraction falls below 10⁻²⁰.
2. **The dark radiation**, ρ_χ/ρ_R, against T_J, in dissipation mode at α = 0.001, 0.01, 0.08 and
   0.15, for the same three M. Natural mode has none.
3. **The number of reflections against M**, from the main store, at α = 0.01, 0.08 and 0.15, the
   values sampled at every M there (α = 0.001 was run at the laboratory M only). Natural mode
   records no reflections: at M ≥ 10⁻³ the field turns in the potential without reaching the wall's
   reflection condition.

The beige band is an indicative BBN window, 2 MeV to 20 keV.

## What it says

**In dissipation mode the reflection structure does not depend on M**, from the laboratory value to
10⁻¹²: the three M's curves overlie each other in both rows, and the reflection counts are flat
from 10⁻³³ to 10⁻¹². The yields agree to the same degree: over the 124 (α ≥ 0.01, β = 1.2–3.0)
of the coarse grid with a result at all six M (10⁻³³ to 10⁻¹²), ΔY_p spreads across M by 0.003
percentage points at the median and 0.029 at most (α = 0.05, β = 2.3) (`histories.csv`), against
the ±2.45% test. The parking temperatures at 4×10⁻³¹ and 10⁻¹⁵ agree to 10⁻⁷ relative, and at
10⁻¹² to 2×10⁻⁶ at β = 2.0 and 2.9: 85 keV and 603 keV. At β = 1.2 the field parks at 3.36 eV.

The one departure is at **M = 10⁻¹², β = 1.2**, the top of the M_crit range (whose maximum is
8.2×10⁻¹³ M_P): 52 reflections at α = 0.08 against 56 below, parking at 3.59 eV against 3.36 eV,
and ΔY_p = +1.637% against +1.641%. At 10⁻¹³ there are 55.

**Natural mode at large M is different in kind.** Up to the first reflection the histories at all
five M coincide. After it, the natural-mode field keeps oscillating, with a kinetic fraction of
10⁻⁸–10⁻⁴ through BBN and after, where the dissipation-mode field has parked (at β = 2.0 and 2.9
inside the BBN window). At M = 0.5 the fraction grows again at late times. These are the histories
whose yields shift strongly negative (ΔY_p from +0.5% to −5% at M = 10⁻³, −21% to −30% at 0.1 and
−40% to −51% at 0.5, for β = 1.2–3.0); why is not yet established.

Since parking ends the reflections, the stage at which a history parks relative to BBN sets how
much of the reflection structure BBN sees: at β = 1.2 the field is still reflecting through BBN and
parks at 3.36 eV; at 2.0 and 2.9 it parks within the window.

## Changes from the plan

PLAN.md §5.3 proposed a companion panel of the parking temperature and the reflection count against
M. The parking temperature is not among `histories.csv`'s columns, and exporting it would mean a
change to `plot_by_beta.py` and a re-run on a cluster. Since the counts, the yields and (for the
three M of this store) the parking temperatures are all independent of M, the panel shows the
counts, which `histories.csv` already has, and this note gives the parking temperatures. The export
is still wanted for the comparison Figure 6, which is to mark failed relativistic margins.

## Restated 2026-11-02

At the user's request, after the diagnostic ([`../diagnostic/NOTES.md`](../diagnostic/NOTES.md))
found that at fixed α the dark radiation does not depend on M, so that the figure's intended claim
("a large-M history used as a proxy mis-computes ρ_χ") was not supported as drawn. Rows 2 and 3 are
restated; row 1 is unchanged.

- **Row 2** keeps its curves and overlays the diagnostic store's α = 0.08 histories at M = 10⁻⁹,
  10⁻⁶, 10⁻³ and 0.5 M_P (orange). They lie on the laboratory α = 0.08 curve: at the same α, every M
  up to 10⁻³ deposits the same dark radiation (to 3×10⁻⁴ today), and 0.5 deposits 1–3% less.
- **Row 3** replaces the reflection count with the test-field ratio 𝓡 at the first impact,
  log₁₀ 𝓡 against M, at α = 0.08 (the history to the first impact, and so 𝓡, is the same at every
  α): the main store's six M (circles) and the diagnostic store's four (squares). Below the line
  𝓡 = 1 an impact is elastic and produces no particles. 𝓡 ∝ M⁻⁴ to within 0.1 in log₁₀ 𝓡 up to
  10⁻³; it crosses 1 at M ≈ 4×10⁻¹⁶ (β = 1.2), 1.1×10⁻¹⁵ (2.0) and 2.6×10⁻¹⁵ (2.9), beside the
  briefing notes' median M_crit, 1.2×10⁻¹⁵ (dashed; dotted, their maximum 8.2×10⁻¹³). At the
  laboratory M, 𝓡 ≈ 10⁶⁰; at 10⁻⁶, 10⁻³⁶.

**What it now says.** The field's motion and, at a given α, the dark radiation are the same at every
M. What depends on M is whether the impacts produce particles at all. A large-M history is right to
reflect elastically at its own mass, and so deposits no dark radiation, where the laboratory history,
at the one-loop estimate α ≈ 0.08, ends with ρ_χ/ρ_R ≈ 0.4–0.7% (0.4–1.3% over α = 0.08–0.15). The
proxy's error is in α, which the code takes as an input, not in the dynamics.

The inputs: `extract_inputs.py` now also gathers the diagnostic store's 12 `fields.csv` (a `store`
column says which; `inputs/fields.csv.gz` is 10.0 MB), and `inputs/histories-large-M-dissipation.csv`
is `plot_by_beta.py`'s export of the diagnostic store by [`../run_figure_exports.sh`](../run_figure_exports.sh).
`../fig6/inputs/histories.csv` is that script's export of the main store, which carries
`log_R_first_impact`. The diagnostic store's sidecar has no fingerprint (it was registered after its
run), so the provenance takes its digest from the run's status record in `../registry/runs/`.
