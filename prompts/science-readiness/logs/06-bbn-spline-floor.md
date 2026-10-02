# Log 06 — Narrow the BBN spline to PRyMordial's range

**Prompt:** prompts/science-readiness/06-bbn-spline-floor.md
**Commit:** the commit that adds this file ("Narrow the BBN spline floor to 0.2 keV"); its SHA is in `git log`
**Model:** Sonnet 5.5
**Date:** 2026-10-02
**Result:** COMPLETE WITH DEVIATIONS

Worked on top of `ca0396d`. `VERSION_LABEL` (`"2026.6.0"`) and `PRYM_VERSION`
(`"bf24c3d+ri02+sr01"`) are unchanged; no schema column changed.

## What shipped

- `ComputeTargets/BBNData.py`, `compute_BBN_data` signature: `T_BBN_keV_spline_min` default
  `1e-4` -> `0.2`, with a comment saying why (PRyMordial's lowest query is 0.363 keV, measured; the
  pre-check then makes a history reach 20 eV). The pre-check and its failure reason are untouched
  (both temperatures are named, as before). The 100 MeV top and the domain guard are untouched.
- `ComputeTargets/tests/test_bbn_spline_floor.py` (new): (a) the pre-check arithmetic, no solve;
  (b) the lowest positive T PRyMordial queries, small network, one solve (~5 s measured).
- `ComputeTargets/tests/test_bbn_callbacks.py` (g): the expected-sample filter and two comments
  moved from `1e-4 keV` to `0.2 keV` (Deviation 1).
- `ComputeTargets/tests/test_prym_passenger.py` (d): stop 1 eV -> 100 eV and the floor `1e-4` ->
  `0.2` keV in the assertion (Deviation 1).
- Board, `review-remediation` Resolved line, `.documents/OPEN_ISSUES.md` (26 -> 25 open).

## Deviations from the prompt

### 1. Two existing tests encoded the old floor — STRUCTURALLY REQUIRED

The prompt names a new test file only. `test_prym_passenger (d)` used a 1 eV stop against the old
0.01 eV pre-check limit; with the 20 eV limit that stop passes the pre-check, and the test errored
(`'SimpleNamespace' has no attribute 'values'`). `test_bbn_callbacks (g)` filtered its expected
samples by `1e-4 keV`, which no longer matches the window. Both were changed to the new floor, in
`ComputeTargets/tests/`, which the prompt allows. No assertion was loosened: (d) checks the same
thing (failure reason names both temperatures) at 100 eV against 20 eV.

### 2. Test (b) does not fail on `HEAD~1` — IMPLEMENTATION CHOICE

(b) measures PRyMordial, which does not read `T_BBN_keV_spline_min`, so it passes on either tree.
The prompt asks only that it be there; I made no attempt to couple it to the default. Test (a)
carries the before/after behaviour (below).

### 3. Prompt text said the driver compares against "log 05's figures" — no deviation

Done as amended: point-input BBN, log 05's figures.

## Verification performed

- **Test (a) on `HEAD~1`'s `BBNData.py`** (stashed the one file; I ran this): fails with
  `'pre-check: T_Jordan_stop=10 eV is more than 0.1*T_BBN_spline_min=0.01 eV'`, `_run_PRyMordial`
  not reached. On the new tree it passes: 1e-8 GeV (10 eV) reaches `_run_PRyMordial` once; 1e-7 GeV
  (100 eV) returns the pre-check payload naming `T_Jordan_stop=0.1 keV` and `0.1*T_BBN_spline_min=20 eV`.
- **Test (b)** printed `calls=1944 lowest positive T = 0.3628 keV (floor 0.2 keV)`. (The planner's
  probe on `6aaa706` printed 4804 calls with the same lowest T; the call count differs because the
  test runs the Hubble-only route.)
- **Driver** (`./venv/bin/python tools/history_and_bbn.py 2 M`, full network, this tree, nothing else
  running), against log 05's figures (`a522005`'s code):
  ```
  M=0.5   Yp=0.249229266 DoH=2.560889654 He3oH=1.054673338 Li7oH=5.241925487
  M=0.001 Yp=0.2467606164 DoH=2.463862263 He3oH=1.042634494 Li7oH=5.409240365
  ```
  Both lines equal log 05's to every printed digit: relative change 0 (target <= 1e-5). BBN
  completed on both, so the domain guard did not fire (README §6.7).
- **Suites** (README §5 rule 6): CosmologyModels 18 -> 18, ComputeTargets 86 -> 88, Datastore 26 ->
  26, all OK. (One run mid-way failed on `test_prym_passenger (d)`, fixed per Deviation 1.)
- `black --check` clean on the four changed Python files.

## Observations not acted on

- `test_bbn_callbacks.py` keeps `T_MIN_MEV = 1e-7` and its comment "the pipeline's spline domain,
  compute_BBN_data's defaults: 1e-4 keV to 100 MeV" (line ~82). It is the builder tests' own domain
  and nothing reads `compute_BBN_data`'s default there, so I left it; the comment is now stale. Not
  worth an issue.

## State handed to the next prompt

- `compute_BBN_data`'s default floor is `T_BBN_keV_spline_min = 0.2` (keV); a history must reach
  `T_Jordan_stop <= 20 eV` (`--T-stop-GeV 1e-8`, or `T_CMB`) to pass the pre-check. 1e-7 GeV fails.
- BBN abundances on β = 2 at M = 0.5 and 10^-3 are unchanged from log 05 to every printed digit.
- Suite counts after this prompt: CosmologyModels 18, ComputeTargets 88, Datastore 26.
- `ComputeTargets/tests/test_bbn_spline_floor.py` is new; `PYTHONPATH=. ./venv/bin/python -m unittest
  ComputeTargets.tests.test_bbn_spline_floor` (~5 s).
