# Log 04 — The initial field as a run option, with a super-Planckian warning

**Prompt:** prompts/science-readiness/04-initial-field-option.md
**Commit:** the commit that adds this file ("Make the initial field a run option and warn on a super-Planckian start"); its SHA is in `git log`
**Model:** Sonnet 5.5
**Date:** 2026-10-02
**Result:** COMPLETE WITH DEVIATIONS

The work was done on top of `568c23a`. `VERSION_LABEL` (`"2026.6.0"`) and `PRYM_VERSION`
(`"bf24c3d+ri02+sr01"`) are unchanged. No schema change.

## What shipped

- `config/argument_parser.py`: `DEFAULT_PHI_INIT_MP = 5.0`; new `--phi-init-Mp` (float, help names
  M_P), after `--T-init-GeV`.
- `main.py`, `plot_by_beta.py`, `plot_ScalarModel.py`: the `phi_value` is built from
  `args.phi_init_Mp * units.PlanckMass` (`main.py:862`, `plot_by_beta.py:895`,
  `plot_ScalarModel.py:1611` before). No `5.0 * units.PlanckMass` literal remains. `π* = 0` stays.
- `pipeline_selection.py`: `super_planckian_couplings(couplings, phi_init, T_init, units) -> list`
  (couplings with `log_Omega(φ*) + ln T* > ln units.PlanckMass`, the same objects in the original
  order) and `warn_super_planckian(couplings, phi_init, T_init, units, emit=print) -> couplings`
  (one `"!! warning: beta=…, phi*=… M_P: Omega(phi*) T* = … M_P (super-Planckian start)"` line per
  coupling, then one count line; returns its first argument itself).
- `main.py`: imports `warn_super_planckian` and calls it on `Coupling_array` straight after the
  array is built, before the model-count print and step 1's batches, assigning the returned (same)
  list back.
- `tools/history_and_bbn.py`: `--phi-init-Mp` (default 5), used for the history's φ*.
- `ComputeTargets/tests/test_initial_field_option.py`: four tests, (a)–(d) of the prompt.

## Deviations from the prompt

### 1. The warning step lives in `pipeline_selection.py`, not `main.py` — STRUCTURALLY REQUIRED

The prompt says `main.py`'s warning step is "factored as a function" which test (d) calls.
`main.py` parses `sys.argv` at import (as log 02's deviation 1 found), so a test cannot import
from it. `warn_super_planckian` is in `pipeline_selection.py`, which the allowed-files list
names, and `main.py` calls it. It touches no §2 design fact. The warning text and its position
are as the prompt says; `main.py`'s own diff is the import, the call and the `phi_init` line.

## Verification performed

- Suites, from the repository root, before (log 03): CosmologyModels 18, ComputeTargets 82,
  Datastore 26. After, run by me: **18, 86, 26**, all OK (72 s, 78 s, 2 s).
- The new test file on the pre-change drivers: with `main.py`, `plot_by_beta.py` and
  `plot_ScalarModel.py` restored to `HEAD` (`git stash -- <the three>`), the other changes in
  place, test (b) **fails**, finding the literal at `main.py:862` (`[862] != []`). Tests (a), (c),
  (d) need `super_planckian_couplings`, `warn_super_planckian` and the option, none of which
  exist on `HEAD`, so they fail there on import or attribute.
- README §6.5 rows (test (a)): β ∈ {1, 6, 7, 25, 40}, φ* = 5, T* = 2×10⁴ GeV selects
  **{7, 25, 40}**; φ* = 1 selects **{40}**. ln(M_P/T*) = 32.433 (scratch arithmetic, reduced M_P
  = 2.435×10¹⁸ GeV).
- Stand-in: which β the check warns about, for each YAML grid, with `main.py`'s grid arithmetic
  (`num = round(5(β_hi − β_lo) + 0.5)`, `linspace`; scratch script, T* = 2×10⁴ GeV):
  - `exponential.yaml` (β 0.1–25, 125 values): φ* = 5 warns 93, from β = 6.526 to 25; φ* = 2 warns
    44, from β = 16.37.
  - `starobinsky.yaml` (β 0.1–13, 65 values): φ* = 5 warns 33, from β = 6.55; φ* = 2 none.
  - `recliner.yaml` (β 0.1–3, 7 values): none at either φ*.
- Driver, one history at a time, `tools/history_and_bbn.py 2 0.5` on this tree:
  - `--phi-init-Mp 5`: RHS=40580 accepted_steps=4469 reflections=0 samples=5392; bounce
    N=20.343026853 T_J=746.634744 MeV φ=4.573705e-03; Yp=0.249229266 DoH=2.560889654
    He3oH=1.054673338 Li7oH=5.241925487. **Identical to log 03** on every printed digit.
  - `--phi-init-Mp 2`: completes. RHS=41070 accepted_steps=4603 reflections=0 samples=4740;
    bounce N=14.476112028 T_J=659.393816 MeV φ=4.593074e-03; Yp=0.2487467993 DoH=2.584485137
    He3oH=1.053310071 Li7oH=5.21244781 (PRyMordial 7.7 s).
- `black --check` clean on the changed files. I did not run `main.py`.

## Observations not acted on

- The warning is emitted by `main.py` only. `plot_by_beta.py` and `plot_ScalarModel.py` read the
  option but print no warning, as the prompt specifies.

## State handed to the next prompt

- `--phi-init-Mp` (default 5.0, `args.phi_init_Mp`); `tools/history_and_bbn.py` takes it too, and
  at 5 reproduces log 03's three printed lines for β = 2, M = 0.5 exactly.
- `pipeline_selection.super_planckian_couplings(couplings, phi_init, T_init, units)` and
  `warn_super_planckian(couplings, phi_init, T_init, units, emit=print)`; both accept `phi_value`
  and `temperature` objects or floats.
- φ* = 2 at β = 2, M = 0.5 (driver, this tree): RHS 41070, 4603 accepted steps, first bounce
  N = 14.476112028, 659.39 MeV; Yp 0.2487467993, D/H 2.584485137e-5.
- Suite counts after this prompt: CosmologyModels 18, ComputeTargets 86, Datastore 26.
