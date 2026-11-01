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
