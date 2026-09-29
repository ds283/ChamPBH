# Log 01 — the temperature-law harness, and the guard that would have caught R1

**Prompt:** prompts/review-remediation/01-temperature-law-harness.md
**Commit:** the commit that adds this file — "Add the temperature-law harness and the R1/R5 guard"
(a commit cannot name its own SHA; `git log -1 -- prompts/review-remediation/logs/01-temperature-law-harness.md` gives it)
**Model:** Claude Opus 5.5
**Date:** 2026-09-29
**Result:** COMPLETE WITH DEVIATIONS

The deviations are all `IMPLEMENTATION CHOICE`. None touches a README §2 design fact. Every
acceptance item in the prompt's §4 is met, and no §5 stop condition fired. Two observations
became or touched board issues (see "Observations not acted on"). One of them is a threshold
that prompt 02 will miss as written.

## What shipped

No production file is in the diff. Tree at dispatch: `b9bc694`, whose production code is
identical to `f5896bb`.

- `CosmologyModels/tests/__init__.py` (new, empty). The package is discoverable from the root.
- `CosmologyModels/tests/eos_reference.py` (new). Pure functions with no `unittest`. Public symbols:
  - `exact_efolds(eos, T0_GeV, T1_GeV) -> float` is witness 1. It returns
    ln(T0/T1) + ⅓ ln[G_s(T0)/G_s(T1)] and reads `eos.G_s` only.
  - `integrate_temperature_law(eos, T0_GeV, T1_GeV, kappa=1.0, with_rho=False, rtol=1e-10,
    atol=1e-12, max_step=0.05) -> TemperatureLawResult` integrates
    d ln T/dN = −1/(1 + κ dG_s_dlogT/G_s/3) and, if `with_rho`, d ln ρ_R/dN = Σ − 4 with
    Σ = 1 − 3w(T). A terminal event (direction −1) at ln T₁ stops it, and the values returned are
    the event's. It starts from `thermodynamic_rho_R(eos, T0)`. It raises `RuntimeError` if the
    event is not reached.
  - `TemperatureLawResult(NamedTuple)`: `efolds: float`, `rho_R: Optional[float]` (GeV⁴),
    `rho_R_ratio: Optional[float]` (the integrated ρ_R over `thermodynamic_rho_R` at T₁).
  - `thermodynamic_rho_R(eos, T_GeV) -> float` returns `RadiationConstant · G_rho(T) · T⁴` in GeV⁴.
  - `derivative_convention_ratio(eos, T_GeV, h=1e-4) -> float` is witness 2. It returns
    `dG_s_dlogT(T)` over [G_s(T eʰ) − G_s(T e⁻ʰ)]/(2h).
  - `production_eos(units=None) -> Xav_EOS_spline` builds the class in `GeV_units()` by default.
  - `derivative_test_grid_GeV() -> np.ndarray` returns the 60-point grid; its geometry is below.
  - `T_CMB_GeV(eos) -> float` gives 2.7255 K in the EOS's units.
  - Constants `T_CMB_KELVIN = 2.7255`, `T_INIT_GEV = 2e4`, and `DERIVATIVE_GRID_POINTS`,
    `DERIVATIVE_GRID_T_LO_GEV`, `DERIVATIVE_GRID_T_HI_GEV`, `DERIVATIVE_GRID_JOIN_GEV`,
    `DERIVATIVE_GRID_JOIN_CLEARANCE`.
- `CosmologyModels/tests/test_temperature_law.py` (new). One class, `TestTemperatureLaw`, with
  six test methods, one per case of the prompt's §2.2. Every run is made once, in `setUpClass`.
  Every threshold is a named module constant with a comment naming the prompt that changes it.
  Setting `CHAMPBH_TEST_REPORT=1` prints the measured table to stderr.
  1. `test_guard_shipped_convention_offset_is_characterised`: `EXPECTED_EFOLD_OFFSET_SHIPPED`
     holds the audit's offsets, with `TEMPERATURE_LAW_EFOLD_OFFSET_TOLERANCE = 2e-3`.
  2. `test_guard_corrected_convention`: `CORRECTED_LAW_EFOLD_TOLERANCE = 1e-5` above the join.
     At and below it, `LOW_T_JOIN_EFOLD_RESIDUAL = 1.465e-4` ±
     `LOW_T_JOIN_EFOLD_RESIDUAL_TOLERANCE = 5e-6`. It also checks that residual + ⅓ ln[G_s(T_LO⁺)/G_s(T_LO)]
     is within `LOW_T_JOIN_STEP_ACCOUNTING_TOLERANCE = 1e-6` of zero.
  3. `test_derivative_convention_is_characterised`: `EXPECTED_DERIVATIVE_CONVENTION_RATIO = ln 10`,
     with `DERIVATIVE_CONVENTION_RATIO_TOLERANCE = 1e-3` relative.
  4. `test_spline_and_jax_implementations_disagree_by_ln10`: `skipUnless` jax.
     `EXPECTED_IMPLEMENTATION_RATIO = ln 10`, with `IMPLEMENTATION_RATIO_TOLERANCE = 1e-3`.
  5. `test_rho_R_witness_is_characterised`. From 5 MeV to 10 keV: 0.182 ± 2e-3 (κ = 1) and
     1.005 ± 5e-3 (κ = 1/ln 10). From 2×10⁴ GeV at 1 MeV, 70 keV and 10 keV: 0.0216 / 0.0044 /
     0.0041 ± 2e-4 (κ = 1) and 0.998 / 0.999 / 1.0026 ± 3e-3 (κ = 1/ln 10).
  6. `test_clamps`. At 10⁻⁵, 10⁻⁶, 10⁻⁹ and 2.35×10⁻¹³ GeV, and at 10¹⁶, 10¹⁷ and 10¹⁹ GeV, both
     derivatives are exactly 0.0. `G_s` and `G_rho` equal the imported `LOW_T_G_S_STAR`,
     `LOW_T_GSTAR` and `HIGH_T_GSTAR`.
- `VERSION_LABEL`: `"2026.1.1"` before and after (not touched).

### The 60-point derivative grid, and why it avoids the joins

`derivative_test_grid_GeV()` is log-spaced in two segments: 27 points on [20 keV, 80 MeV] and
33 points on [180 MeV, 5 TeV]. The spacing is 0.1385 and 0.1389 decades, split in proportion to
each segment's length in log T. The joins it keeps clear of:

- **the low clamp at 10 keV** (`SAIKAWA_SHIRAI_T_LO`): a factor 2 below the first point. The
  spline's ringing from R5's step dies within about 1 % in T of the clamp.
- **the Saikawa–Shirai branch switch at 120 MeV**, where the raw fit is discontinuous. `_raw_G_s`
  is 19.100021 at 0.12(1 − 10⁻⁹) GeV and 19.092871 at 0.12 GeV. That is a step of 0.0072, and the
  spline fits across it. The nearest points, 80 MeV and 180 MeV, are each a factor 1.5 away.
- **the top of Xav's table at 25 TeV**: a factor 5 above the last point. It affects `w`, not
  `G_s`.

The high clamp at 10¹⁶ GeV is far outside the grid.

## Deviations from the prompt

### D1 — two-segment derivative grid · IMPLEMENTATION CHOICE

The prompt asks for "60 points log-spaced in [20 keV, 5 TeV]" that are also "away from the
table's own joins by at least a factor 1.5 in T". A single `geomspace(2e-5, 5e3, 60)` puts two
points, 100.4 MeV and 139.4 MeV, within a factor 1.5 of the 120 MeV branch switch.

- **Alternatives.** (a) The single geomspace, reading "the joins" as only the 10 keV clamp and
  the 25 TeV table top. (b) The single geomspace with the offending points dropped, leaving 58.
  (c) Two segments with exactly 60 points (chosen).
- **Reason.** (c) satisfies every clause literally.
- **It makes no difference to the characterisation.** On the single geomspace, the two points
  near 120 MeV give a central-difference ratio within 2.6e-8 of ln 10, and a spline/jax ratio
  within 2.9e-7 (100.4 MeV) and 1.0e-6 (139.4 MeV) of it (scratch probe, `b9bc694`).

### D2 — case 5 asserts both starting points · IMPLEMENTATION CHOICE

The prompt allows asserting "either range, but say which". **Both are asserted**:

- 5 MeV → 10 keV, as the prompt's primary range;
- 2×10⁴ GeV → 1 MeV / 70 keV / 10 keV, because README §6.1's "now" column lists 0.022 / 0.0044 /
  0.0041 there, and acceptance item 4 wants that column reproduced from the tests.

README's "0.022" has two significant figures, and the measured 1 MeV value is 0.02164. The
shipped-law centres are therefore the measured values to two figures (0.0216 / 0.0044 / 0.0041),
± 2e-4. The corrected-law centres are README §6.1's 0.998 / 0.999, plus the measured-now 1.0026 at
10 keV, ± 3e-3 (the §6.1 tolerance). The 2×10⁴ GeV runs are the case-1 and case-2 runs, so this
costs nothing.

### D3 — the step-accounting tolerance · IMPLEMENTATION CHOICE

The prompt's stop condition requires the residual at the join to be "accounted for by
⅓ ln[G_s(T_LO⁺)/G_s(T_LO)]", but gives no tolerance. I assert
|residual + ⅓ ln ratio| ≤ 1e-6 (`LOW_T_JOIN_STEP_ACCOUNTING_TOLERANCE`). The measured
remainder is −2.8e-8 at 10 keV and −1.1e-7 at T_CMB in the test's geometry (`with_rho=True`), and
+1.3e-8 / +9.5e-9 with `with_rho=False`. Both are the same order as the ~4e-8 at every point
above the join; the ln ρ_R component changes the solver's step control slightly. T_LO⁺ = 10⁻⁵ (1 + 10⁻⁹) GeV, as in `low_t_join_probe.py`.

### D4 — additional public symbols in `eos_reference.py` · IMPLEMENTATION CHOICE

The prompt names five functions. I added:

- `TemperatureLawResult`, a NamedTuple, because the prompt says only "-> result";
- `derivative_test_grid_GeV()`, so that prompts 02 and 05 score exactly the same 60 points;
- `T_CMB_GeV(eos)`, and the constants `T_CMB_KELVIN`, `T_INIT_GEV` and `DERIVATIVE_GRID_*`.

`T_CMB_KELVIN = 2.7255` is copied from `CosmologyModels/LambdaCDM/Planck.py` rather than
imported, so that the test module does not import the ΛCDM model classes.

### D5 — an opt-in printed report · IMPLEMENTATION CHOICE

Acceptance item 4 asks for the §6.1 "now" column reproduced "from the tests themselves, with the
printed values". Failure messages print the measured value (`msg=`), but a passing run prints
nothing. `CHAMPBH_TEST_REPORT=1` makes `setUpClass` print the table below to stderr.

- **Alternative:** always print. Rejected, because it adds noise to every other run of the suite.

## Verification performed

All of it was run by me, from the repository root with `venv/bin/python`, on the tree at
`b9bc694` plus the new files.

**Audit scripts first, as the prompt asks**:

- `tlaw_check.py` reproduced §1's table of the audit to four decimals. The offsets κ = 1 minus
  exact are 0.1803, 0.7788, 0.9870, 0.9944, 1.3363, 1.4024, 1.4223, 1.4223.
- `eos_consistency.py` printed 0.18081 (5 MeV → 10 keV), 0.08008 (100 MeV → 10 keV) and 0.97131
  (5 MeV → 1 MeV).
- `low_t_join_probe.py ship` printed the following.
  - The join: G_s(T_LO⁺) = 3.938269 and G_s(T_LO) = 3.940000; G_rho(T_LO⁺) = 3.380577 and
    G_rho(T_LO) = 3.380000.
  - Corrected-law residuals: −6.864e-12 (1 GeV); −4.335e-08, −3.867e-08, −3.868e-08, −3.941e-08
    and −3.979e-08 (100 MeV to 70 keV); **+1.465e-04** (10 keV); **+1.464e-04** (T_CMB).
  - The ρ_R witness from 5 MeV: 1.00471 (corrected) and 0.18184 (κ = 1).

**The suite**, `CHAMPBH_TEST_REPORT=1 PYTHONPATH=. ./venv/bin/python -m unittest discover -s
CosmologyModels/tests -t . -v`, prints:

```
[test_temperature_law] from 2e4 GeV:
          T1    N exact  offset k=1  resid k=1/ln10   rho k=1  rho k=1/ln10
       1 GeV    10.0419     +0.1803      -6.864e-12   0.48344       0.98839
     0.1 GeV    12.8040     +0.7788      -4.335e-08   0.05058       0.99917
   0.005 GeV    15.9596     +0.9871      -3.867e-08   0.02228       0.99788
   0.001 GeV    17.5746     +0.9944      -3.868e-08   0.02164       0.99790
  0.0001 GeV    20.1398     +1.3364      -3.941e-08   0.00567       0.99902
   7e-05 GeV    20.5472     +1.4025      -3.979e-08   0.00437       0.99915
   1e-05 GeV    22.5080     +1.4223      +1.465e-04   0.00405       1.00258
       T_CMB    40.0746     +1.4223      +1.464e-04   0.00405       1.00258
rho_R witness 5 MeV -> 10 keV: k=1 0.18184, k=1/ln10 1.00471
join: G_s(T_LO+) = 3.938269, G_s(T_LO) = 3.940000, (1/3) ln ratio = -1.4648e-04
...
Ran 6 tests in 5.101s

OK
```

(The "offset" column prints to four decimals, so a trailing digit can differ from `tlaw_check.py`
by one in the last place. For example 0.98706 prints as 0.9871 here and as 16.9466 − 15.9596 =
0.9870 there.)

**README §6.1 "now" column, reproduced by the tests:**

| Quantity | README "now" | measured (this suite, `b9bc694`) | asserted |
|---|---|---|---|
| N to T_CMB, code vs exact | 41.497 vs 40.075 | 41.4969 vs 40.0746 (offset +1.4223) | +1.422 ± 2e-3 |
| offsets to 1 GeV / 100 MeV / 1 MeV / 70 keV / 10 keV | +0.180 / +0.779 / +0.994 / +1.403 / +1.422 | +0.1803 / +0.7788 / +0.9944 / +1.4025 / +1.4223 | each ± 2e-3 |
| offsets to 5 MeV / 100 keV (prompt's case 1) | +0.987 / +1.336 | +0.9871 / +1.3364 | ± 2e-3 |
| corrected law (÷ln 10) residual, 10 keV and T_CMB (R5) | +1.465e-4 | +1.4646e-4 and +1.4637e-4 | 1.465e-4 ± 5e-6 |
| corrected law residual, six points above the join | (README: "each ≤ 1e-5" after 02) | worst \|−4.335e-8\| (100 MeV) | ≤ 1e-5 — **passes today** |
| R5 step, −⅓ ln[G_s(T_LO⁺)/G_s(T_LO)] | +1.465e-4 | +1.4648e-4 (3.938269 vs 3.940000) | residual − step within 1e-6 |
| `dG_s_dlogT` vs central difference, worst over the 60 points | ratio 2.303 | ratio/ln 10 − 1: worst −4.68e-6 (20 keV); the rest ≤ 3.7e-7 | ln 10 to 1e-3 |
| spline vs jax `dG_s_dlogT` (jax 0.9.0 imported, **not skipped**) | ratio 2.303 | ratio/ln 10 − 1: worst −1.61e-5 (27.5 keV) | ln 10 to 1e-3 |
| ρ_R witness, 2×10⁴ GeV → 1 MeV / 70 keV / 10 keV, κ = 1 | 0.022 / 0.0044 / 0.0041 | 0.02164 / 0.00437 / 0.00405 | ± 2e-4 about 0.0216 / 0.0044 / 0.0041 |
| same, κ = 1/ln 10 | (target 0.998 / 0.999 / 0.999) | 0.99790 / 0.99915 / 1.00258 | ± 3e-3 about 0.998 / 0.999 / 1.0026 |
| ρ_R witness, 5 MeV → 10 keV, κ = 1 | 0.182 | 0.18184 | 0.182 ± 2e-3 |
| same, κ = 1/ln 10 | (target 1.001 after 02) | 1.00471 | 1.005 ± 5e-3 |

**Measured offsets to four decimals, for prompt 02 to quote as "before".** These are for the law
as shipped (κ = 1), from 2×10⁴ GeV, taken as N_code − N_exact:

| T₁ | 1 GeV | 100 MeV | 5 MeV | 1 MeV | 100 keV | 70 keV | 10 keV | T_CMB |
|---|---|---|---|---|---|---|---|---|
| offset | +0.1803 | +0.7788 | +0.9871 | +0.9944 | +1.3364 | +1.4025 | +1.4223 | +1.4223 |

Unrounded, they are 0.180313, 0.778829, 0.987062, 0.994355, 1.336378, 1.402465, 1.422278 and
1.422278 (scratch probe calling `eos_reference`).

**A simulated fix makes the characterisations fail.** A scratch runner divided
`SaikawaShirai_EOS_spline.dG_s_dlogT` and `dG_rho_dlogT` by ln 10 in memory, with no file
modified, and ran the module. Five tests failed and only `test_clamps` passed:

- cases 1, 3, 4 and 5, because the defect they characterise has gone;
- case 2, because κ = 1/ln 10 is then applied twice.

A later fix therefore announces itself, and prompt 02 has to flip the constants.

**Other checks:**

- **Suite wall-clock:** 5.1 s for the six tests. About 4 s of that is building and evaluating the
  jax class; the 18 integrations take under 1 s.
- **Convergence of the ρ_R witness in `max_step`.** From 5 MeV to 10 keV, max_step = 0.05 gives
  0.1818357 and 1.0047054, and max_step = 0.01 gives 0.1818356 and 1.0047075. So 0.05 is
  converged to 2e-6.
- **`black --check`** is clean on all three new files. `eos_reference.py` was reformatted once
  before the check.
- **No Ray, no datastore, no PRyMordial** is imported. The module needs the repository root as
  cwd, for the CSV.

**Per-package suite counts:**

| Package | before (`b9bc694`) | after |
|---|---|---|
| `CosmologyModels/tests` | 0 (directory absent; `discover` raises "Start directory is not importable") | **6**, jax case run, not skipped |
| `ComputeTargets/tests` | 0 (directory absent) | 0 (directory absent; prompt 03 creates it) |

## Observations not acted on

1. **`[00-xav-eos-w-above-the-table-returns-one-over-3.9]` does not match the tree.** At `b9bc694`,
   `Xav_EOS_spline.w` returns `1.0 / 3.0` for T ≥ `_T_max` (`Xav_EOS_spline.py:67–68`).
   - **Measured:** `w(3e4 GeV)` = 0.3333333 and `w(_T_max)` = 0.3333333, with `_T_max` =
     25 118.86 GeV.
   - **History:** commit `449de62` ("Fix typo in the implementation of Xav's equation of state",
     2026-01-16) changed `1.0 / 3.9` to `1.0 / 3.0`. `41b410d`, the tree the audit's §11 names,
     already has `1.0 / 3.0`.
   - **So the audit §11's "Correction to §4" is itself wrong**, and §4's original statement ("returns
     exactly 1/3 outside that range") was right.
   - **What I did:** added a dated measurement note to the board entry and to its index row. I did
     **not** close the issue, which is the board owner's call, and I did not touch the audit
     (additive only).
2. **Prompt 02's derivative target of ≤ 1e-6 relative will be missed on this grid, even with a
   perfect ÷ln 10.** The ratio residual r = d/(c · ln 10) − 1 is invariant under dividing d by
   ln 10, so these are exactly the residuals prompt 02 will measure.
   - **Measured on `derivative_test_grid_GeV()`** (scratch probe, `b9bc694`):
     - against the central difference: worst −4.68e-6 at 20 keV, then −3.7e-7 at 27.5 keV and
       −3.0e-7 at 11.5 GeV. **1 point** exceeds 1e-6.
     - against jax: worst −1.61e-5 at 27.5 keV, +4.21e-6 at 37.9 keV, −2.06e-6 at 20 keV and
       −1.73e-6 at 11.5 GeV. **4 points** exceed 1e-6.
   - **Cause: where the derivative is tiny, a relative comparison is ill-conditioned.**
     - dg_s/d log10 T is 1.36e-6 at 20 keV and 6.6e-4 at 27.5 keV. This is the e± Boltzmann tail
       at m_e/T ≈ 25.
     - The derivative dips to 0.245 near 10 GeV and crosses zero near 700 GeV and 3.5 TeV.
     - Against the central difference, round-off and O(h²) truncation in G_s at h = 1e-4 dominate.
       Against jax, the spline's own interpolation error at 250 samples per decade dominates.
   - **This is not a convention defect.** Every other point agrees to ≤ 3.7e-7 (central
     difference) and ≤ 6.7e-7 (jax).
   - **Opened as `[01-derivative-agreement-target-1e-6-is-missed-at-the-low-T-end]`.** Someone has
     to decide before prompt 02 runs whether that target is measured on a sub-grid, in an absolute
     norm, or at a different tolerance. I have not changed any target (README §6: do not loosen a
     target).
3. **Audit §11's explanation of `eos_consistency.py`'s 0.18081 is half right.** With a terminal
   event, `solve_ivp`'s last sample *is* the event, so "reads the last solver sample" is not the
   cause. The whole difference is `max_step`: `integrate_temperature_law` with `max_step=np.inf`
   gives 0.180807, and with 0.05 it gives 0.181836. The test and `low_t_join_probe.py` (both at
   max_step = 0.05, converged to 2e-6) are right; `eos_consistency.py` steps over structure in Σ.
   No action needed. The script stays as the record (prompt §3).
4. The "units of the output will be 1/GeV" comments in `dG_s_dlogT` / `dG_rho_dlogT` of both EOS
   classes are stale. That is already in prompt 02's scope, per audit §1 "Fix and regression test".
   No issue opened.

## State handed to the next prompt

- **Module and names.** `CosmologyModels/tests/eos_reference.py` holds `exact_efolds`,
  `integrate_temperature_law` (returns `TemperatureLawResult(efolds, rho_R, rho_R_ratio)`),
  `thermodynamic_rho_R`, `derivative_convention_ratio`, `production_eos`,
  `derivative_test_grid_GeV`, `T_CMB_GeV`, `T_INIT_GEV`. All temperatures are floats in GeV.
- **Constants prompt 02 flips**, all in `CosmologyModels/tests/test_temperature_law.py`:
  - `EXPECTED_EFOLD_OFFSET_SHIPPED`: every value → 0.0.
  - `TEMPERATURE_LAW_EFOLD_OFFSET_TOLERANCE`: 2e-3 → 1e-5.
  - `KAPPA_CORRECTED` → 1.0, once the EOS returns d/d ln T, so that case 2 does not divide twice.
    Alternatively, case 2 can merge into case 1.
  - `LOW_T_JOIN_EFOLD_RESIDUAL`: 1.465e-4 → 0.0, with `LOW_T_JOIN_EFOLD_RESIDUAL_TOLERANCE`: 5e-6
    → 1e-5.
  - `EXPECTED_DERIVATIVE_CONVENTION_RATIO`: ln 10 → 1.0, and `DERIVATIVE_CONVENTION_RATIO_TOLERANCE`:
    1e-3 → 1e-6. **This fails at 20 keV (4.68e-6); see issue
    `[01-derivative-agreement-target-1e-6-is-missed-at-the-low-T-end]`.**
  - `EXPECTED_IMPLEMENTATION_RATIO`: ln 10 → 1.0, and `IMPLEMENTATION_RATIO_TOLERANCE`: 1e-3 → 1e-6.
    **This fails at 4 points, worst 1.61e-5 at 27.5 keV; same issue.**
  - `RHO_R_WITNESS_SHIPPED`, `RHO_R_WITNESS_FROM_INIT_SHIPPED`: remove.
  - `RHO_R_WITNESS_CORRECTED`: re-centre on the post-R5 value. The probe gives 1.00135.
  - `RHO_R_WITNESS_FROM_INIT_CORRECTED[1e-5]`: re-centre. The probe gives 0.99922.
- **"Before" numbers.** Offsets to four decimals are in the table above. Corrected-law residuals
  before R5: ~−4e-8 above the join and +1.4646e-4 / +1.4637e-4 at 10 keV / T_CMB. Join values:
  G_s(T_LO⁺) = 3.938269 and G_s(T_LO) = 3.94. ρ_R witness, κ = 1/ln 10: 1.00471 from 5 MeV, and
  0.99790 / 0.99915 / 1.00258 from 2×10⁴ GeV at 1 MeV / 70 keV / 10 keV.
- **Case 6 is not affected by R5's fix.** It compares with the imported `LOW_T_G_S_STAR` and
  `LOW_T_GSTAR`, so it keeps passing when prompt 02 changes them.
- **Reproduce:**
  `CHAMPBH_TEST_REPORT=1 PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t . -v`
  from the repository root, about 5 s.
