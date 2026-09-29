# Log 02 — fix the entropy derivative and the 10 keV join, tighten the guard, bump the version

**Prompt:** prompts/review-remediation/02-fix-the-entropy-derivative.md
**Commit:** the commit that adds this file — "Fix the ln 10 in the entropy derivative and the 10 keV join"
(a commit cannot name its own SHA; `git log -1 -- prompts/review-remediation/logs/02-fix-the-entropy-derivative.md` gives it)
**Model:** Claude Opus 5.5
**Date:** 2026-09-29
**Result:** COMPLETE WITH DEVIATIONS

Every README §6.1 row is at its target. No §6 stop condition fired. The deviations are all
`IMPLEMENTATION CHOICE`, and none touches a README §2 design fact. The fix is in the EOS class;
`ScalarModel.py`, `Xav_EOS_spline.py` and the jax class are not in the diff.

**Consequence for every existing store: every store built under `VERSION_LABEL = "2026.1.1"` is
invalid.** The temperature law and the low-temperature g's both changed. No datastore lookup filters
on the version (`[00-datastore-lookups-ignore-the-version-column]`), so a corrected `main.py` pointed
at an old store would reuse its stale rows without complaint. The numerical campaign must start from
an empty database.

## What shipped

Tree at dispatch: `81b7e14`.

- **F1 — the derivatives.** `CosmologyModels/GenericEOS/SaikawaShirai_EOS_spline.py`:
  - New module constant `_LN_10 = log(10.0)` (`from math import log`), with a comment saying why it is
    there and naming R1 and `5962833`.
  - `dG_rho_dlogT` (`:114–130` before) and `dG_s_dlogT` (`:155–171` before) now return
    `self._dg_star…_spline(log10_T_in_GeV) / _LN_10`. That is d g/d ln T; it was d g/d log10 T.
  - Each method has a docstring that states "d g / d ln T = T dg/dT … dimensionless" and says it is
    exactly zero at and beyond the clamps. The stale comment "units of the output will be 1/GeV" is
    removed from both.
  - Unchanged: the grid (log10 T, 250 per decade, [0.8 T_LO, 1.2 T_HI]), the clamp tests against
    `_LOG10_SAIKAWA_SHIRAI_T_{HI,LO}`, `G_rho`, `G_s` and `w`.
  - `Xav_EOS_spline` overrides only `__init__`, `name`, `type_id` and `w` (`grep -n "def "`), so it
    inherits the corrected derivatives with no edit.
- **F1b — the low-T limits (R5).** `CosmologyModels/GenericEOS/SaikawaShirai_common.py`:
  - `LOW_T_GSTAR` 3.38 → **3.383**, and `LOW_T_G_S_STAR` 3.94 → **3.931**.
  - The comment block around them is replaced. It says:
    - the values are the fit's x → ∞ limits, 2.030 + 1.353 and 2.008 + 1.923, from Eqs. (C.3) and (C.4);
    - the e± terms are ~e⁻⁵¹ at 10 keV, so the clamp is continuous;
    - both values agree with N_eff = 3.046 when the neutrino T³ weight scales as (N_eff/3)^{3/4}
      (3.3835 and 3.9310), whereas scaling linearly gives 3.938, which rounds to 3.94;
    - see audit §11.
  - The commented-out 3.36 and 3.91, the `TODO: check` and the Weinberg-notes URL are removed.
  - Unchanged: `SAIKAWA_SHIRAI_T_LO`, the grid start and every fitting coefficient.
  - The raw fit now meets the clamp exactly: `_raw_G_s(1e-5)` = 3.931 and `_raw_G_rho(1e-5)` =
    3.383 (both exact in float).
  - The jax class imports both constants by name (`SaikawaShirai_EOS_jax_autodiff.py:41–42`), so it
    picks up the new values with no edit. Measured: `jax.G_s(9e-6 GeV)` = 3.931 and
    `jax.G_rho(9e-6 GeV)` = 3.383.
- **F2 — the consumer.** `ComputeTargets/ScalarModel.py` is not touched.
- **F3 — the guard.** `CosmologyModels/tests/test_temperature_law.py`:
  - `KAPPA_CORRECTED` 1/ln 10 → **1.0**.
  - `EXPECTED_EFOLD_OFFSET_SHIPPED`: every value → **0.0**. `TEMPERATURE_LAW_EFOLD_OFFSET_TOLERANCE`
    2e-3 → **1e-5**.
  - `LOW_T_JOIN_EFOLD_RESIDUAL` 1.465e-4 → **0.0**. `LOW_T_JOIN_EFOLD_RESIDUAL_TOLERANCE` 5e-6 →
    **1e-5**. `LOW_T_JOIN_STEP_ACCOUNTING_TOLERANCE` stays at 1e-6.
  - **Cases 3 and 4 change form** (README §6.1 as amended). Each now asserts
    |dG_s_dlogT − reference| / G_s ≤ 1e-6 at each of the 60 grid points. The reference is the
    central difference (case 3) or the jax class (case 4). The ratio and ln 10 still go into the
    failure message.
    - Removed: `EXPECTED_DERIVATIVE_CONVENTION_RATIO`, `DERIVATIVE_CONVENTION_RATIO_TOLERANCE`,
      `EXPECTED_IMPLEMENTATION_RATIO` and `IMPLEMENTATION_RATIO_TOLERANCE`.
    - Added: `DERIVATIVE_CONVENTION_TOLERANCE = 1e-6` and `IMPLEMENTATION_AGREEMENT_TOLERANCE = 1e-6`,
      both absolute in d ln g_s/d ln T.
  - **Case 5.** The κ = 1 half is removed: `RHO_R_WITNESS_SHIPPED(_TOLERANCE)`,
    `RHO_R_WITNESS_FROM_INIT_SHIPPED(_TOLERANCE)` and the 5 MeV κ = 1 run in `setUpClass`.
    - `RHO_R_WITNESS_CORRECTED` 1.005 → **1.00135** (± 5e-3, kept).
    - `RHO_R_WITNESS_FROM_INIT_CORRECTED[1e-5]` 1.0026 → **0.99922** (± 3e-3, kept). The 1 MeV and
      70 keV centres stay at 0.998 and 0.999.
  - Test methods renamed (D2): case 1 → `test_guard_temperature_law_matches_entropy_conservation`;
    case 3 → `test_derivative_convention_is_natural_log`; case 4 →
    `test_spline_and_jax_derivatives_agree`; case 5 → `test_rho_R_witness`. Cases 2 and 6 keep
    their names.
  - The module docstring, every constant's comment and the `CHAMPBH_TEST_REPORT` table are updated
    to say what prompt 02 set and what the values were before.
- **`CosmologyModels/tests/eos_reference.py`.** New public function
  `central_dG_s_dlogT(eos, T_GeV: float, h: float = 1e-4) -> float`. It returns
  [G_s(T eʰ) − G_s(T e⁻ʰ)]/(2h), the case-3 reference. `derivative_convention_ratio` now calls it;
  its result is unchanged.
- **F4 — the version label.** `VERSION_LABEL` goes from `"2026.1.1"` to **`"2026.2.0"`** in
  `main.py:83` (was `:80`) and `plot_by_beta.py:67`. `main.py` has a one-sentence comment above it:
  stores made under an earlier label are invalid because on 2026-09-29 the temperature law lost a
  factor ln 10 and the low-T g's changed.
- **F5 — the documentation note.** `.documents/numerical-strategies.md` §4.3 gets a dated paragraph
  after its bullets. It states the convention and the ln 10 error from `5962833` to this commit,
  gives the new low-T limits (R5) and names the guard. Additive: the §4.3 bullet that still says
  3.38 / 3.94 is left as written, and the note says those values are superseded.

## Deviations from the prompt

### D1 — F1: divide by ln 10 at the return · IMPLEMENTATION CHOICE

- **Alternatives.** (a) Divide by `ln 10` at the return (chosen). (b) Rebuild `_log_T_grid` in
  ln T, which means converting `_LOG10_SAIKAWA_SHIRAI_T_{HI,LO}` and every `np.log10` in the
  class.
- **Reason.** (a) is two changed lines, and the grid, the knots and the clamp comparisons stay
  bit-identical. `G_s` and `G_rho` above T_LO are therefore unchanged to the last bit, so every
  change in `G_s` comes from R5 alone. (b) moves the knots (250 per log10 decade is 108.6 per
  ln-decade unless the density is rescaled), and README §0.4 and prompt §4 forbid changing the
  sampling. The spline's derivative is exact for the spline, so dividing it by a constant loses
  nothing.
- **Cost.** The class still has a log10 grid, so a reader has to see the `/ _LN_10`. The module
  comment and the inline comment at each return say so.
- I used `from math import log; log(10.0)` rather than `math.log(10.0)`: the same value, in the
  style the module's other imports use.

### D2 — test methods renamed · IMPLEMENTATION CHOICE

The prompt says to flip constants and change the form of cases 3 and 4. It says nothing about the
method names. Two of prompt 01's names become false once the constants flip, and the other two
become misleading:

- `test_spline_and_jax_implementations_disagree_by_ln10` would assert that the two agree;
- the three `…_is_characterised` names describe a characterisation that no longer exists.

- **Alternatives.** (a) Keep the names, so that any text naming them still resolves. (b) Rename the
  four (chosen).
- **Reason.** A failing test's name is the first thing a reader sees. The old names are listed in
  "State handed to the next prompt", and the count is unchanged.

### D3 — case 2 kept, not folded into case 1 · IMPLEMENTATION CHOICE

With `KAPPA_CORRECTED = 1.0`, case 2 integrates the same law as case 1, so the `setUpClass` runs are
duplicates.

- **Kept (chosen).** Case 2 still does two things case 1 does not:
  - it splits the assertion at the join;
  - it checks that −⅓ ln[G_s(T_LO⁺)/G_s(T_LO)] accounts for the residual. That check sees R5
    independently of R1 (§"Deliberate breakage", run B).
- **Folding it in** would have moved the step accounting into case 1 and dropped a test, so the
  count would fall from 6 to 5. That is a stop under README §5.6.
- **Cost.** 8 duplicate integrations, well under a second.

### D4 — a helper added to `eos_reference.py` · IMPLEMENTATION CHOICE

Case 3's new form needs the central difference itself, not the ratio. The alternatives were:

- recovering it in the test as `dG_s_dlogT / ratio`, which divides by a number that can be close
  to 0/0 in the e± tail;
- inlining the finite difference in the test.

I added `central_dG_s_dlogT` beside the ratio instead, and made the ratio use it, so both have one
definition. The directory is inside the prompt's allowed files (`CosmologyModels/tests/`).

### D5 — case-5 centres at 1 MeV and 70 keV not moved · IMPLEMENTATION CHOICE

The prompt says to "re-centre the corrected half on the value you now measure", and prompt 01's
comment says "re-centre 10 keV".

- **Re-centred:** the 5 MeV value to 1.00135, and 10 keV from 2×10⁴ GeV to 0.99922. Both are the
  measured values, and both match the audit §11 probe to five decimals.
- **Left at README §6.1's 0.998 and 0.999:** 1 MeV and 70 keV. R5 does not move them: they
  measure 0.99790 and 0.99915 before and after (run B and the passing run). Both are inside the
  ± 3e-3 by a factor of more than ten.

## Verification performed

All of it was run by me, from the repository root with `venv/bin/python`, on `81b7e14` plus this
diff unless stated.

### The suite

`CHAMPBH_TEST_REPORT=1 PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t . -v`

```
[test_temperature_law] from 2e4 GeV:
          T1    N exact     N code  N code - N exact  rho ratio
       1 GeV    10.0419    10.0419        -6.864e-12    0.98839
     0.1 GeV    12.8040    12.8040        -3.626e-08    0.99917
   0.005 GeV    15.9596    15.9596        -3.618e-08    0.99788
   0.001 GeV    17.5746    17.5746        -3.617e-08    0.99790
  0.0001 GeV    20.1398    20.1398        -3.659e-08    0.99902
   7e-05 GeV    20.5472    20.5472        -3.605e-08    0.99915
   1e-05 GeV    22.5088    22.5088        -3.620e-08    0.99922
       T_CMB    40.0754    40.0754        -3.620e-08    0.99922
rho_R witness 5 MeV -> 10 keV: 1.00135
join: G_s(T_LO+) = 3.931000, G_s(T_LO) = 3.931000, (1/3) ln ratio = -3.7007e-17
test_clamps ... ok
test_derivative_convention_is_natural_log ... ok
test_guard_corrected_convention ... ok
test_guard_temperature_law_matches_entropy_conservation ... ok
test_rho_R_witness ... ok
test_spline_and_jax_derivatives_agree ... ok
Ran 6 tests in 5.516s
OK
```

N_exact at 10 keV and T_CMB moved from 22.5080 and 40.0746 to 22.5088 and 40.0754. That is
⅓ ln(3.94/3.931) = 7.6e-4, the change in the reference's own G_s(T₁). It is not a change in the law.

**Per-package counts:**

| Package | before (`81b7e14`) | after |
|---|---|---|
| `CosmologyModels/tests` | 6, OK (jax case run) | **6**, OK (jax case run, not skipped) |
| `ComputeTargets/tests` | 0 (directory absent) | 0 (directory absent; prompt 03 creates it) |

### README §6.1, every row

| Quantity | Before (`81b7e14`) | Target | Measured after | Witness |
|---|---|---|---|---|
| N from 2×10⁴ GeV to T_CMB, code vs exact | 41.4969 vs 40.0746 | agree to 1e-5 | 40.0754 vs 40.0754, **−3.620e-8** | guard case 1 |
| corrected law to 10 keV and T_CMB (R5's step) | +1.4646e-4 / +1.4637e-4 (κ = 1/ln 10, log 01) | ≤ 1e-5 | **−3.620e-8 / −3.620e-8** | case 2 |
| offsets to 1 GeV / 100 MeV / 1 MeV / 70 keV / 10 keV | +0.1803 / +0.7788 / +0.9944 / +1.4025 / +1.4223 | each ≤ 1e-5 | −6.9e-12 / −3.63e-8 / −3.62e-8 / −3.61e-8 / −3.62e-8 | case 1 |
| (also 5 MeV / 100 keV, case 1) | +0.9871 / +1.3364 | ≤ 1e-5 | −3.62e-8 / −3.66e-8 | case 1 |
| \|dG_s_dlogT − central\| / G_s, 60-point grid | ratio 2.303 | ≤ 1e-6 absolute | worst **1.26e-8** at 180 MeV; 0 of 60 over 1e-6 | case 3 |
| \|spline − jax\| / G_s, same grid (jax 0.9.0 imported) | ratio 2.303 | ≤ 1e-6 absolute | worst **2.44e-7** at 180 MeV; 0 of 60 over 1e-6 | case 4 |
| ρ_R witness from 2×10⁴ GeV, 1 MeV / 70 keV / 10 keV | 0.02164 / 0.00437 / 0.00405 | 0.998 / 0.999 / 0.999 ± 3e-3 | **0.99790 / 0.99915 / 0.99922** | case 5 |
| ρ_R witness 5 MeV → 10 keV | 0.18184 | 1.001 ± 5e-3 | **1.00135** | case 5 |
| `VERSION_LABEL` | `"2026.1.1"` | `"2026.2.0"` in both files | `main.py:83` and `plot_by_beta.py:67` are `"2026.2.0"` | grep |

**The offsets to four decimals, before and after**, κ = 1 (the law as shipped), from 2×10⁴ GeV,
N_code − N_exact:

| T₁ | 1 GeV | 100 MeV | 5 MeV | 1 MeV | 100 keV | 70 keV | 10 keV | T_CMB |
|---|---|---|---|---|---|---|---|---|
| before (`81b7e14`, run A below) | +0.1803 | +0.7788 | +0.9871 | +0.9944 | +1.3364 | +1.4025 | +1.4223 | +1.4223 |
| after | −0.0000 | −0.0000 | −0.0000 | −0.0000 | −0.0000 | −0.0000 | −0.0000 | −0.0000 |

**Derivative diagnostics** (scratch probe `deriv_probe.py`, which calls `eos_reference` and
both classes):

- Largest |d ln g_s/d ln T| on the grid: 1.273.
- The ratio residuals |ratio − 1| are unchanged from log 01: worst 4.68e-6 at 20 keV against the
  central difference, and 1.61e-5 at 27.5 keV against jax. That is why the assertion's form had to
  change.
- Join: G_s(T_LO⁺) = G_s(T_LO) = 3.931000000; G_rho(T_LO⁺) = G_rho(T_LO) = 3.383000000.

These match the planner's probe on `ec3a994` (1.26e-8 and 2.44e-7, both at 180 MeV) to the digits
quoted.

### Deliberate breakage (prompt §2)

The exact incantation. `$S` is my scratchpad. A backup of the two fixed production files was made
first, so nothing depended on `git stash`:

```bash
cp CosmologyModels/GenericEOS/SaikawaShirai_EOS_spline.py \
   CosmologyModels/GenericEOS/SaikawaShirai_common.py $S/fixed/
# run A: both production files at HEAD, new tests
git show HEAD:CosmologyModels/GenericEOS/SaikawaShirai_EOS_spline.py > CosmologyModels/GenericEOS/SaikawaShirai_EOS_spline.py
git show HEAD:CosmologyModels/GenericEOS/SaikawaShirai_common.py   > CosmologyModels/GenericEOS/SaikawaShirai_common.py
CHAMPBH_TEST_REPORT=1 PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t .
# run B: fixed spline, HEAD's common (R1 fixed, R5 not)
cp $S/fixed/SaikawaShirai_EOS_spline.py CosmologyModels/GenericEOS/
CHAMPBH_TEST_REPORT=1 PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t .
# run C: everything restored
cp $S/fixed/SaikawaShirai_common.py CosmologyModels/GenericEOS/
PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t .
```

`git status --short` was checked between runs. After run C it listed exactly the six files of this
diff, before the log, the board, the index and the documentation note were written.

**Run A: both production files at `HEAD`.** `FAILED (failures=140)`, in 6 tests. Only
`test_clamps` passed.

```
       1 GeV    10.0419    10.2222        +1.803e-01    0.48344
     0.1 GeV    12.8040    13.5828        +7.788e-01    0.05058
   0.005 GeV    15.9596    16.9466        +9.871e-01    0.02228
   0.001 GeV    17.5746    18.5690        +9.944e-01    0.02164
  0.0001 GeV    20.1398    21.4761        +1.336e+00    0.00567
   7e-05 GeV    20.5472    21.9496        +1.402e+00    0.00437
   1e-05 GeV    22.5080    23.9303        +1.422e+00    0.00405
       T_CMB    40.0746    41.4969        +1.422e+00    0.00405
rho_R witness 5 MeV -> 10 keV: 0.18184
join: G_s(T_LO+) = 3.938269, G_s(T_LO) = 3.940000, (1/3) ln ratio = -1.4648e-04
```

- **Case 1:** all 8 subtests fail. The offsets are the characterised ones, e.g. T_CMB
  `N_code - N_exact = +1.422278e+00 (N_code = 41.496882, N_exact = 40.074603)`.
- **Case 2:** 10 subtests fail: 6 above the join, 2 at or below it, and 2 step-accounting.
- **Case 3:** 59 of 60 fail. At every failing point the ratio is within 1e-6 of ln 10 =
  2.30258509 (within 1e-7 at 51 of them), and |Δ(d ln g_s/d ln T)| runs from 1.71e-5 to 1.66.
  The one point that passes is 20 keV, where d ln g_s/d ln T is 1.50e-7. The ln 10 error there is
  (ln 10 − 1) × 1.50e-7 = 1.95e-7 absolute, under the 1e-6 norm (see "Observations" 3).
- **Case 4:** 59 of 60 fail in the same way; 20 keV again passes.
- **Case 5:** all 4 subtests fail, at 0.18184 (5 MeV) and 0.02164 / 0.00437 / 0.00405
  (2×10⁴ GeV → 1 MeV / 70 keV / 10 keV). These are the characterised offset +1.422, ratio 2.303
  and ρ_R ratio 0.0041.

**Run B: R1 fixed, R5 not.** `FAILED (failures=5)`:

```
   1e-05 GeV    22.5080    22.5082        +1.464e-04    1.00258
       T_CMB    40.0746    40.0747        +1.464e-04    1.00258
rho_R witness 5 MeV -> 10 keV: 1.00471
join: G_s(T_LO+) = 3.938269, G_s(T_LO) = 3.940000, (1/3) ln ratio = -1.4648e-04
FAIL: test_guard_corrected_convention (T1='1e-05 GeV', region='at/below join')
FAIL: test_guard_corrected_convention (T1='T_CMB', region='at/below join')
FAIL: test_guard_temperature_law_matches_entropy_conservation (T1='1e-05 GeV')
FAIL: test_guard_temperature_law_matches_entropy_conservation (T1='T_CMB')
FAIL: test_rho_R_witness (case='2e4 GeV -> 1e-05 GeV')
```

- **Case 2's join half fails** with residuals **+1.4642e-4** (10 keV) and **+1.4641e-4** (T_CMB).
  The guard therefore sees R5 independently of R1.
- **Its step-accounting subtests pass**, because the residual is exactly the step, within 6e-8.
- **Case 1** fails at the same two points.
- **Case 5** fails at 2×10⁴ GeV → 10 keV: 1.00258 against 0.99922 ± 3e-3.
- Cases 3 and 4 pass: R5 is outside the derivative grid.

The values differ from log 01's +1.4646e-4 / +1.4637e-4 by ~4e-8. That is the ~4e-8 solver-level
residual present at every T₁: log 01 applied κ = 1/ln 10 in the ODE, and here the division is in
the class.

**Run C: restored.** `Ran 6 tests … OK`.

### Re-score with the audit scripts (not edited)

`venv/bin/python .documents/audit-2026-09-29/tlaw_check.py` (1.9 s):

```
e-folds from T_init=2e4 GeV to various T (exact entropy conservation vs code's law vs corrected):
     T_end     exact   kappa=1  kappa=1/ln10   rho_int/rho_thermo(kappa=1)  (kappa=1/ln10)
         1   10.0419   10.0419        9.9636         0.9884          1.3484
       0.1   12.8040   12.8040       12.4657         0.9992          3.6502
     0.005   15.9596   15.9596       15.5309         0.9979          5.2021
     0.001   17.5746   17.5746       17.1428         0.9979          5.2684
    0.0001   20.1398   20.1398       19.5594         0.9990          9.4409
     7e-05   20.5472   20.5472       19.9381         0.9991         10.5766
     1e-05   22.5088   22.5088       21.8908         0.9992         10.9591
  2.35e-13   40.0754   40.0754       39.4574         0.9992         10.9591
```

- **The `kappa=1` column now equals the exact column** at every row to the four decimals printed.
  The suite measures the difference as ≤ 3.7e-8.
- **The `kappa=1/ln10` column double-divides, as expected.** The script applies its own ÷ ln 10 to
  a derivative that is now already natural-log, so that column no longer means "corrected" and no
  longer equals the other two. The prompt's "must equal … to 1e-5" is met by the column that
  scores the shipped law. The script's κ = 1/ln 10 column is obsolete on this tree, and the script
  is left as the record (prompt §3).
- `low_t_join_probe.py` also applies ÷ ln 10 itself. On this tree it is not a re-score, and I did
  not run it; cases 1 and 2 replace it.

`venv/bin/python .documents/audit-2026-09-29/eos_consistency.py`:

```
     5 MeV ->  0.01 MeV: integrated rho_R / thermodynamic rho_R = 1.00135   (N elapsed 6.549)
   100 MeV ->  0.01 MeV: integrated rho_R / thermodynamic rho_R = 1.00005   (N elapsed 9.705)
     5 MeV ->     1 MeV: integrated rho_R / thermodynamic rho_R = 1.00002   (N elapsed 1.615)
T[MeV]   Sigma_table   Sigma_SS=4(1-gs/grho)
     3      0.0013     -0.0007
     1      0.0108     -0.0011
   0.5      0.0345     -0.0099
   0.2      0.0946     -0.1301
   0.1      0.0680     -0.4447
  0.05      0.0029     -0.6417
  0.02      0.0000     -0.6479
```

The three ratios are 1.00135, 1.00005 and 1.00002, all inside 0.99–1.01 as required.

- The script sets no `max_step` (log 01, observation 3), so I repeated the three runs with
  `integrate_temperature_law(..., max_step=0.05)` and `max_step=0.01`.
- Both give **1.00135, 1.00005 and 1.00002**, so the script's values are converged on this tree.

### Other checks

- **Other consumers of the derivatives** (prompt §6, fourth stop). I grepped `dG_s_dlogT`,
  `dG_rho_dlogT` and `dgstar_*_dlogT` over all `.py` outside `venv/` and `thirdparty/`.
  - Production consumers: the temperature-law RHS (`ScalarModel.py:357–360`, already written for
    d ln T), and the stored sample values (`ScalarModel.py:897–898`).
  - Those stored values are read by `plot_ScalarModel.py:277–279`, labelled "d g/d(log T)" with no
    base stated, and by `CodeComparisonTools/CompareMathematica.py:68`, labelled d/d ln T.
  - `LambdaCDM_GenericEOS` forwards the calls unchanged.
  - `claude-context/` holds non-production copies.
  - **Nothing assumes log10, so the stop does not fire.**
- **`black --check`** is clean on the six changed Python files (and `CosmologyModels/tests/__init__.py`).
  Five of them were checked black-clean at `81b7e14` before editing; `eos_reference.py` was clean at
  prompt 01 (log 01).
- **Nothing ran a pipeline, Ray or a datastore.** The whole prompt's compute was the suite (≈ 5 s)
  three times plus the audit scripts (≈ 3 s).

## Observations not acted on

1. **Three stale comments in `CosmologyModels/GenericEOS/` lie outside this prompt's allowed lines.**
   They are opened as `[02-stale-derivative-and-T_LO-comments-in-the-EOS-package]`:
   - `GenericEOS.py:68–75`: the abstract `dG_s_dlogT` docstring says it returns "d(g_S)/dT". Its
     sibling `dG_rho_dlogT` correctly says T d(g_rho)/dT.
   - `SaikawaShirai_EOS_jax_autodiff.py:204, 221`: the jax class still carries "units of the output
     will be 1/GeV". Editing the jax class is a stop condition here.
   - `SaikawaShirai_common.py:116–122` (after this diff): the comment above `SAIKAWA_SHIRAI_T_LO`
     says "We cut to the late-time asymptotic values at 600 keV", but the value is 10 keV. The
     prompt allows only the two constants' comment block.
   A comment-only commit would fix all three. None affects a number.
2. **README §6.4's "from 100 MeV, 1.003 ± 5e-3" is stale after R5.** On this tree the converged
   witness from 100 MeV to 10 keV is **1.00005** (the `max_step` 0.05 and 0.01 runs above). That is
   inside the stated band, but 3e-3 from its centre. The value is handed to prompt 05 below, and it
   is not an issue: the README is the planner's and prompt 05 "takes the value prompt 02 records".
3. **The absolute norm in cases 3 and 4 cannot see the ln 10 at 20 keV**, where
   d ln g_s/d ln T = 1.50e-7 and the ln 10 error is 1.95e-7 (run A: 59 of 60 points fail, 20 keV
   passes). This is the price of the
   restated target, and the planner accepted it. Cases 1, 2 and 5 see R1 at every T₁, so the guard
   as a whole does not depend on that point. No issue opened.
4. **Case 5 sees R5 only narrowly.**
   - The 5 MeV → 10 keV subtest cannot see it: R1 fixed without R5 gives 1.00471, inside
     1.00135 ± 5e-3.
   - The 2×10⁴ GeV → 10 keV subtest sees it by 3.4e-3 against its ± 3e-3 (run B).
   Cases 1 and 2 are R5's witnesses, at a margin of fourteen times their tolerance. Prompt 05
   tightens case 5, which will widen this margin. No issue opened.
5. **`CodeComparisonTools/CompareMathematica.py` compares ChamPBH's stored `dgstar_s_dlogT` with
   Xav's "dg*s/dlogT(interpolated)" column.** That column's convention is unknown (README §0.2).
   Any comparison CSV made before this commit carries the log10 value. It is covered by the
   version rule, and no issue is opened.

## State handed to the next prompt

- **`VERSION_LABEL = "2026.2.0"`** in `main.py` and `plot_by_beta.py`. **Every store built under
  `"2026.1.1"` is invalid**, and the numerical campaign starts from an empty database.
- **The convention.** `SaikawaShirai_EOS_spline.dG_s_dlogT` / `dG_rho_dlogT` (and hence
  `Xav_EOS_spline`'s) return d g/d ln T, the same as the jax class. The largest disagreement
  between them on `derivative_test_grid_GeV()` is 2.44e-7 in d ln g_s/d ln T, at 180 MeV.
- **The low-T limits.** `LOW_T_GSTAR = 3.383`, `LOW_T_G_S_STAR = 3.931`. `G_s` and `G_rho` are
  continuous at 10 keV to the digits printed (3.931000000, 3.383000000).
- **Test names changed** (old → new):
  - `test_guard_shipped_convention_offset_is_characterised` →
    `test_guard_temperature_law_matches_entropy_conservation`
  - `test_derivative_convention_is_characterised` → `test_derivative_convention_is_natural_log`
  - `test_spline_and_jax_implementations_disagree_by_ln10` → `test_spline_and_jax_derivatives_agree`
  - `test_rho_R_witness_is_characterised` → `test_rho_R_witness`
  - `test_guard_corrected_convention` and `test_clamps` are unchanged.
- **Constants for prompt 05 to tighten**, in `CosmologyModels/tests/test_temperature_law.py`:
  `RHO_R_WITNESS_CORRECTED = 1.00135` (± 5e-3) and
  `RHO_R_WITNESS_FROM_INIT_CORRECTED = {1e-3: 0.998, 7e-5: 0.999, 1e-5: 0.99922}` (± 3e-3).
  New helper: `eos_reference.central_dG_s_dlogT(eos, T_GeV, h=1e-4)`.
- **ρ_R witness values on this tree** (corrected law, `integrate_temperature_law`, `max_step` 0.05,
  converged to < 1e-5 against 0.01):
  - 5 MeV → 10 keV **1.00135**
  - 100 MeV → 10 keV **1.00005** (README §6.4 says 1.003; that figure predates R5)
  - 5 MeV → 1 MeV **1.00002**
  - 2×10⁴ GeV → 1 GeV / 100 MeV / 5 MeV / 1 MeV / 100 keV / 70 keV / 10 keV / T_CMB:
    0.98839 / 0.99917 / 0.99788 / 0.99790 / 0.99902 / 0.99915 / 0.99922 / 0.99922
- **N from 2×10⁴ GeV to T_CMB** is 40.0754, both exact and code, which differ by −3.62e-8. It
  was 40.0746 exact before R5, because the exact reference also reads G_s(T_CMB).
- **The audit scripts' κ = 1/ln 10 columns** (`tlaw_check.py`, `low_t_join_probe.py`) now
  double-divide. Read `tlaw_check.py`'s `kappa=1` column as the corrected law.
- **Reproduce:**
  `CHAMPBH_TEST_REPORT=1 PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t . -v`
  from the repository root, about 5.5 s.
