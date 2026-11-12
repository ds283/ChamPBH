# Log 01a — turning points resolved above the solver's tolerance

**Subject:** Keep an interior pair of turning points only if the solver resolves it
**Commit:** this commit, on branch `electroweak-eos-01a` (a worktree, U10), on top of `0479587` (prompt 01a and its
orchestrator note)
**Date:** 2026-11-12, by Claude Opus 5.5
**Result:** **COMPLETE, after one stop.** Gate Resolved holds as the user ruled it at the stop (§2.2, §3).
- **The rule** (U8): `resolved_sign_changes` and `turning_point_tolerance` in `ComputeTargets/ScalarHistory.py`.
  `observe_turning_points` calls them. An interior pair of sign changes of π is dropped when its excursion is not
  greater than τ = atol + rtol · max(|π_low|, |π_high|). A single change across the step's nodes is always kept.
- **A dropped pair** (U9) is as if it were not there. It is counted in the new, unstored
  `IntegrationResult.unresolved_turning_point_pairs`.
- **The old table.** Over 01's 36 histories to T_CMB, 241,380 turning points were compared. No pair is dropped down
  to 100 GeV, and everything is bitwise the baseline except two late near-stall pairs, at 0.175 τ and 0.95 τ.
  01a stopped on those, and the user ruled them the rule working as ruled. The suites pass with no test changed, and
  the three golden records pass.
- **The new table.** The early pair is dropped in all 24 scanned histories that had it and in all 31 test
  integrations that had it, and nowhere else but one late near-stall pair. At α = 0 everything else is bitwise,
  apart from `dropped_drains`, which the user ruled an allowed difference. 4 of the 22 tests now pass.
- **Tests:** a new module, `ComputeTargets/tests/test_resolved_turning_points.py`, with 19 tests (about 9 s).
  ComputeTargets goes 665 → 684.
- **Issues:** closes `[01-turning-points-read-below-the-solver-tolerance]`. Opens
  `[01a-surfing-turning-points-keeps-crossings-below-the-solver-tolerance]` (§5).

## 1. What shipped

- **`ComputeTargets/ScalarHistory.py`**:
  - `SignChange` (N_before, N_after, N_zero, kind): one sign change between consecutive samples.
  - `turning_point_tolerance(pi_low, pi_high, atol, rtol) -> float`: τ = atol + rtol · max(|π_low|, |π_high|).
  - `resolved_sign_changes(N_samples, pi_samples, tolerance) -> (List[SignChange], int)`:
    - finds the sign changes exactly as `observe_turning_points` found them (zero samples carry no sign; a change
      through zeros is recorded at the first of them);
    - walks the changes from the step's start. For a change i with a successor, the excursion is the largest |π|
      over the samples from change i's `N_after` to change i + 1's `N_before`. If excursion ≤ τ, both changes are
      dropped and the walk goes on at i + 2; otherwise change i is kept and the walk goes on at i + 1;
    - returns the kept changes and the number of pairs dropped.
  - `observe_turning_points` calls it with τ from `params.atol` and `params.rtol`, and adds the count to
    `unresolved_pairs`.
    - The end-to-end bracket is used only if exactly one change was found *and none was dropped*, with the nodes of
      opposite signs. Each kept change is refined on its sub-interval as before.
    - A step whose pair is dropped folds its samples as if it held no turning point. The code there is unchanged:
      no root, so `fold(..., inf)` only.
  - `IntegrationResult.unresolved_turning_point_pairs` is appended last, with default 0, and the loop passes it.
    Its comment says it is not a payload key, a column, or part of a record or fingerprint.
  - The comment on `TURNING_POINT_INTERIOR_SAMPLES` and the docstring paragraph "Turning points and the drain" state
    the rule, citing this prompt and README §8. The history of ruling 15 is left where it was.
- **`ComputeTargets/tests/test_resolved_turning_points.py`** (new; 19 tests):
  - Constructed samples (13 tests):
    - an unresolved pair shaped like 01's is dropped and counted;
    - the same pair at 100× is kept;
    - an excursion exactly τ is dropped, and the next float up is kept;
    - a pair between atol and τ is dropped by the rtol term (and kept by τ = atol alone);
    - the tolerance formula;
    - a single change across the nodes is kept below τ;
    - three changes, with the first pair unresolved, keep only the third; with it resolved, keep all three;
    - after a kept change, the next pair is tested;
    - zero samples behave as before, as does a pair through zeros;
    - 2000 random sign patterns agree with the old finder (copied as `reference_changes`).
  - The field (1 test).
  - Item (11)'s pair is kept, and nothing is dropped in that history (2 tests, one integration, about 9 s).
  - A history at rest with Σ patched to zero above 386.7 GeV, at α = 0 and 0.1 (4 tests, about 0.1 s each). It
    shows the pair in its samples; with the change it has no TurningPoint, count 1, `dropped_drains` 0, no
    injection, and the count is not a payload key.
- **`.documents/electroweak-eos/turning-points/`** (new):
  - the probes, `measurements.md`, `tests_broken_by_the_table.txt` (note 01 §0's 22 ids, as 01's `failing_ids.txt`);
  - `out/` (text): `compare_old.txt`, `compare_new.txt`, `closest_pairs_old.txt`, `tests_new_table.txt`,
    `item11_pair_{before,after}.txt`, `sigma_switched_off_{before,after}.txt`, `surfing_survey.txt`,
    `suites_{before,after}.txt` and `breakage.txt`.
- **`.documents/numerical-strategies.md` §6.2:** a dated note under "Turning points, at every α".
- **The board** (row 01a, the Resolved gate as ruled, the landing note, §3 opened, §4 closed, §5 counts) and
  **`.documents/OPEN_ISSUES.md`** (the 01 row removed, one row added in a new §1.25; 30 open).

## 2. Deviations from the prompt

### 2.1 The orchestrator's rulings (note §0)

1. **The worktree** (U10). Branch `electroweak-eos-01a` from `0479587`, a `venv` symlink, `?? venv` only, never staged.
   The worktree's own failures were 2 at the baseline, not note 01's ten, which were counted before the symlink:
   - `tools.tests.test_registry_config.test_roots_and_distributions` (`var/runs` resolves to the main checkout's);
   - `RunDrivers.tests.test_registered_run…test_the_script_is_main_py_with_its_hash_and_this_repositorys_head` (the
     script's path is the worktree's).

   Both fail after the change with byte-identical failure text (§4). *STRUCTURALLY REQUIRED*.
2. **Prompt 01's files and `CLAUDE.md` are not touched.** None is in this commit (`git diff --cached --name-status` in
   §4). *STRUCTURALLY REQUIRED*.
3. **The new table** was given as `ee01a-new-table.csv`, SHA-256 `690090c86e285f3ee30d03244a2c242dd2b5b3c3f405e4f94229b0e481d0119d`,
   shown before use. It was read through a redirect of `pandas.read_csv` inside `Xav_EOS_spline` and never swapped
   in. The worktree's CSV hashed `28edd854…29a8` before, during and at the end. *STRUCTURALLY REQUIRED*.
4. **The baselines** were taken by me on the unchanged code in the worktree.
   - On the new table they agree with 01's: the pair in 14 of 01's 20 histories, in 31 of 33 test integrations, and
     the same β pattern.
   - On the old table, 01 recorded no pair to 100 GeV; nothing is dropped there.

   *IMPLEMENTATION CHOICE*, ruled.

### 2.2 The stop, and the user's rulings

01a stopped once. Its scans, run to T_CMB rather than to 01's 100 GeV (D1), found two pairs dropped on the old table.
§0's stop list has "a history on the old table that drops a pair".
- **The pairs:** near-stalls at β = 10 and 20, M = 10⁻³ M_P, T_J ≈ 24 eV and 0.46 keV, excursions 0.175 τ and 0.95 τ
  (§3.1).
- **Raised with them:** at α = 0 on the new table, `dropped_drains` differs as well as `turning_points` and the count.
  It goes from 1 to 0 wherever the early pair's wall armed a drain that its plateau dropped. The stop list allows only
  the latter two to differ.
- **The options put:** (a) accept the late pairs as the rule working as ruled; (b) drop only pairs with π < 0 at both
  nodes; (c) another ruling.

**Ruled by the user, 2026-11-12:** "For the stop issue, choose (a). For the `dropped_drains` issue, add to the allowed
differences."
- **(a)** amends the gate's old-table clause and the matching stop condition. The clause now reads: bitwise everywhere
  else (turning points, `first_bounce`, nodes, interpolants, final state, nfev, steps, reflections, warnings,
  `dropped_drains`, `surfing_turning_points`); no pair dropped down to 100 GeV, 01's scan range; the late near-stall
  pairs recorded with their step, nodes' π, excursion and τ. The closest kept pair is recorded too. The rule is
  unchanged. *STRUCTURALLY REQUIRED* (the user's ruling).
- **`dropped_drains`** joins `turning_points` and the new count among the differences allowed at α = 0 on the new
  table, amending §0's stop list. *STRUCTURALLY REQUIRED* (the user's ruling; U9 has a dropped pair neither arm nor
  drop a drain).

Everything after the stop followed these: the verdicts in `compare_scans.py`, the corrected texts in
`numerical-strategies.md`, the board and `measurements.md`, and the closest-pair measurement.

### 2.3 Others

- **D1. The scans.**
  - They run to T_CMB, not to 100 GeV as 01's did, so that `first_bounce` and every later turning point are compared.
    They cover the union of 01's two grids, 36 histories, at α = 0.
  - 14 of them fail at small M on both trees and both tables (§3.1). They are compared through every TurningPoint
    constructed before the failure, plus the failure message.
  - *IMPLEMENTATION CHOICE*.
- **D2. How the scans were run.**
  - **One process per history, three or four at a time** (`scan_all.sh`), alongside the suites, which ran serially
    among themselves.
  - **The suite times** are therefore taken under the scans' load as well as the machine's, and are recorded with
    `uptime`.
  - **Two early attempts were abandoned**, and none of their output is used: a single-process run, too slow, and an
    `xargs -I` driver that macOS refused.
  - **The constructed-TurningPoint record was added after the first old-table baseline.**
    - Its 14 failed histories were re-run with it on the unchanged code, and their failure messages are identical.
    - The new-table baseline and every run with the change used it throughout.
  - **The closest-kept-pair field was added after the scans with the change.** The nine histories that have such
    pairs were re-run, and their records equal the earlier run's.
  - *IMPLEMENTATION CHOICE*.
- **D3. Two helpers beyond the one function:** `turning_point_tolerance`, so that the rtol term is tested and broken in
  the code under test, and `SignChange`. *IMPLEMENTATION CHOICE*.
- **D4. The function returns N values, not indices.**
  - The refinement loop reads `change.N_before` / `N_after` / `N_zero` where it read `Ns[j…]`. These are the same
    float objects.
  - With nothing dropped, the 2000-pattern test and the old-table scans show the output identical.
  - *IMPLEMENTATION CHOICE*.
- **D5. τ is computed from `params.atol` and `params.rtol`,** as the prompt says ("the values the step's Radau instance
  was built with"). It is not read from the instance, where SciPy's `validate_tol` raises rtol to at least
  100 ε ≈ 2.2e-14. The two agree for every rtol at or above that, which is every caller (§5). *IMPLEMENTATION CHOICE*.
- **D6. The integration test's patched Σ.**
  - It is a ramp to zero with a kink at 386.7 GeV, continuous, as the new table's spline is at its top row.
  - A jump to zero, tried first, also made the pair. But one probe history under it (β = 5.5, M = 0.5) had not
    finished after minutes, so it was not used.
  - *IMPLEMENTATION CHOICE*.
- **D7. Tests beyond the prompt's list:** the tolerance formula, the walk after a kept change, a pair through zero
  samples, the random-pattern comparison, the field's default, and "not a payload key". *IMPLEMENTATION CHOICE*.
- **D8. Two breakages beyond the prompt's three:** the old file itself, and τ × 1000 (which fails item (11)'s tests).
  *IMPLEMENTATION CHOICE*.
- **D9. The breakage record was run in a scratch export** (`git archive` of `0479587` plus the changed files). The
  changed files are byte-identical (`cmp`) to those committed. The before-runs of `item11_pair.py` and
  `sigma_switched_off.py` were run in an unchanged export of `0479587`, because by then the worktree held the change.
  *IMPLEMENTATION CHOICE*.
- **D10. The 22-test probe's first baseline run misreported** `test_surfing_scan.test_beta_1_5_tuned`, whose
  failures are subtests, as a pass. The probe was fixed to attribute a subtest to its parent and was re-run on the
  unchanged code; that second run is the one recorded. The fixed version also wraps `test_release._INTEGRATE`,
  which 01's run missed. *UNINTENDED DRIFT*, corrected before any comparison.
- **D11. Item (11) is measured.** Its pair has an excursion of 1.1761e-6 against τ = 1.000008e-8, which is 117.6 τ.
  The note's "about 10⁻⁸" is right. No deviation; recorded.

## 3. Verification performed

### 3.1 Gate Resolved, old table (`out/compare_old.txt`, `out/closest_pairs_old.txt`)

- **The 36 histories.**
  - 241,380 turning points before and 241,376 after.
  - 22 histories have a `first_bounce`, unchanged in all.
  - The 14 that fail are 1.5, 2, 2.5, 3, 4, 5.5, 10 and 20 at 4e-31, and 1.5, 2, 3, 5.5, 10 and 20 at 1e-12. Mostly
    this is the step budget at T_J ≈ 10⁻¹¹ GeV. 5.5/1e-12 and 10/1e-12 fail on φ ≤ 0, and 3/1e-12 on the guard against
    a reflection inside the wall. They fail identically before and after.
- **Bitwise equal in every history:** the nodes, the interpolants' SHA-256, `N_final`, the final state, nfev, the steps,
  the reflections, the warnings, `dropped_drains` and `surfing_turning_points`.
- **The turning points** are equal in 34 histories and lose one pair in two:

  | History | Step | π at nodes | Excursion | τ | Ratio |
  |---|---|---|---|---|---|
  | β = 10, M = 10⁻³ | [78.552502471, 78.553501033], T_J ≈ 23.7 eV | +8.19e-7, +7.94e-8 | 1.751e-9 | 1.0000008e-8 | 0.175 |
  | β = 20, M = 10⁻³ | [125.583816755, 125.585771760], T_J ≈ 0.461 keV | +2.25e-7, +6.93e-8 | 9.515e-9 | 1.0000002e-8 | 0.95 |

  The removed points are the plateau at N = 78.553227993 and the wall at 78.553302675, and the plateau at
  125.584798484 and the wall at 125.585311784.
- **The two verdicts:** "the prompt's gate as written: FAILS"; "the gate as ruled 2026-11-12, (a): HOLDS; late pairs
  dropped: 2".
- **No pair is dropped down to 100 GeV.**
- **The closest kept pairs:**
  - 1.097 τ at β = 4, M = 10⁻³ (N = 52.066);
  - 2.45 τ and 2.72 τ, one step before each dropped pair;
  - the rest at 8.9 τ or above.

  Only the nine M = 10⁻³ histories have a step showing two or more changes.
- **The three golden tests** pass against `rr03`. They are in the `[g-z]` half, and were also run alone, OK:
  6 tests, 43 s, at a load of 9.3.

### 3.2 Gate Resolved, new table (`out/compare_new.txt`, `out/tests_new_table.txt`)

- **The scans.** 231,904 turning points before and 231,854 after.
  - The early pair is dropped in all 24 histories that had it: 01's 14 of 20, plus all four M at β = 1.2, 1.5, 3, 5.5
    and 10. The excursions are 0.008 τ to 0.23 τ, with π < 0 at both nodes, at T_J 385–389 GeV.
  - No history keeps it.
  - One more pair is dropped: β = 20, M = 10⁻³ at N = 126.278 (0.59 τ), a late near-stall.
  - At α = 0, everything else is bitwise, with three allowed exceptions: the turning points (after = before less the
    pairs, in order), the count, and `dropped_drains`, which goes 1 → 0 in each history with the early pair, as ruled.
  - `first_bounce` is unchanged everywhere. Verdict: HOLDS.
- **The tests' integrations.** 31 of 33 had the pair before the change. After it, exactly those 31 drop exactly one
  pair, in the same call order, and none has an early TurningPoint.
- **The 22 tests,** before → after:
  - pass now (4): test_injection `test_1_…first_bounce`, `test_2_one_injection_per_wall_side…`,
    `test_2_the_turning_points_are_classified`; test_drain_store `test_the_extremes_and_the_row_count` (an error
    before);
  - still fail (18), with the first failing assertion of each in `out/tests_new_table.txt` and §6.

### 3.3 Item (11) and the patched history

- `out/item11_pair_{before,after}.txt`: as D11. It is the same history before and after (457 turning points), with
  count 0 after.
- `out/sigma_switched_off_{before,after}.txt`: before the change, wall/plateau pairs at T_J 387–388 GeV, with excursions
  0.06 τ to 0.77 τ. After it, none, with count 1 and `dropped_drains` 0. The test's β = 1.5 history has an excursion of
  6.17e-10 against τ = 1.0000000017e-8.

### 3.4 π is `y[1]` under the scalar tolerances

- The state is `StateVector` (φ_E, π_E, ln ρ_R,E, ln f_m, ln T_J); `ODERHS` returns dφ/dN = π, unscaled.
- Radau is built with `rtol=params.rtol, atol=params.atol` (`new_solver`), scalars from `StepControl`. Every caller
  passes floats.
- SciPy 1.17.0 `radau.py:480`: `scale = atol + np.maximum(np.abs(y), np.abs(y_new)) * rtol`. So component 1's scale
  is τ with π_old and π_new.
- `RadauDenseOutput` at t_old returns `y_old` exactly, since p = 0. At t_new it returns `y_new` to rounding.

## 4. Suites, and the breakage record

Run serially, ComputeTargets in two halves, in the worktree. Start times are GMT, with the one-minute load at each start.
The baseline scans and the 22-test probe ran alongside the suites before the change, and the scans with the change ran
alongside them after it.

| Suite | Before (16:00–16:20) | After (17:30–17:50) |
|---|---|---|
| CosmologyModels | 26 OK, 69 s (9.35) | 26 OK, 72 s (10.81) |
| ObjectFactories | 163 OK, 140 s (11.84) | 163 OK, 145 s (13.31) |
| RunDrivers | 76, 1 failure (worktree), 29 s (11.60) | 76, the same failure, 30 s (28.67) |
| ComputeTargets `[a-f]` | 219 OK, 154 s (10.01) | 219 OK, 159 s (23.60) |
| ComputeTargets `[g-z]` | 446 OK, 507 s (11.11) | 465 OK (+19), 510 s (34.71) |
| tools | 277, 1 failure (worktree), 293 s (17.78) | 277, the same failure, 290 s (8.35) |

`uptime` at the ends: before, 16:20, loads 17.05 / 15.14 / 13.71; after, 17:50, loads 10.10 / 12.71 / 16.97.
- **No count fell, and no existing test changed.**
- **The two worktree failures** (§2.1, ruling 1) fail before and after with identical text.
- **The breakage record** (`out/breakage.txt`). 19 tests each time, then restored (`cmp` equal) and OK:

  | Breakage | Tests that fail |
  |---|---|
  | B1, the comparison made `<` (drop only below τ) | 1: exactly τ |
  | B2, τ's rtol term removed | 2: the rtol case and the formula |
  | B3, no pair ever dropped (the old behaviour) | 10: the six constructed cases that drop a pair, and the patched history's two drop tests at both α |
  | B4, the old file | 27 failures and errors: `resolved_sign_changes` missing; the patched history keeps its pair |
  | B5, τ × 1000 | 7, including both item (11) tests |

- **black:** every Python file changed or added is clean.
- **Staging:**

```
$ git diff --cached --name-status
M	.documents/OPEN_ISSUES.md
A	.documents/electroweak-eos/turning-points/compare_scans.py
A	.documents/electroweak-eos/turning-points/item11_pair.py
A	.documents/electroweak-eos/turning-points/measurements.md
A	.documents/electroweak-eos/turning-points/out/breakage.txt
A	.documents/electroweak-eos/turning-points/out/closest_pairs_old.txt
A	.documents/electroweak-eos/turning-points/out/compare_new.txt
A	.documents/electroweak-eos/turning-points/out/compare_old.txt
A	.documents/electroweak-eos/turning-points/out/item11_pair_after.txt
A	.documents/electroweak-eos/turning-points/out/item11_pair_before.txt
A	.documents/electroweak-eos/turning-points/out/sigma_switched_off_after.txt
A	.documents/electroweak-eos/turning-points/out/sigma_switched_off_before.txt
A	.documents/electroweak-eos/turning-points/out/suites_after.txt
A	.documents/electroweak-eos/turning-points/out/suites_before.txt
A	.documents/electroweak-eos/turning-points/out/surfing_survey.txt
A	.documents/electroweak-eos/turning-points/out/tests_new_table.txt
A	.documents/electroweak-eos/turning-points/run_tests_new_table.py
A	.documents/electroweak-eos/turning-points/scan_all.sh
A	.documents/electroweak-eos/turning-points/scan_turning_points.py
A	.documents/electroweak-eos/turning-points/sigma_switched_off.py
A	.documents/electroweak-eos/turning-points/summarize_tests.py
A	.documents/electroweak-eos/turning-points/surfing_survey.py
A	.documents/electroweak-eos/turning-points/tests_broken_by_the_table.txt
M	.documents/numerical-strategies.md
M	ComputeTargets/ScalarHistory.py
A	ComputeTargets/tests/test_resolved_turning_points.py
M	prompts/electroweak-eos/IMPLEMENTATION_STATE.md
A	prompts/electroweak-eos/logs/01a-resolved-turning-points.md
```

## 5. Observations not acted on

- **`surfing_turning_points`** (prompt §2): opened as
  `[01a-surfing-turning-points-keeps-crossings-below-the-solver-tolerance]` (board §3).
  - **Not in the early pair's circumstances.** There π ≈ 10⁻⁹ M_P, so g = 1 + βπ/M_P ≈ 1, and no scanned history has a
    crossing within 385–389 GeV.
  - **But it keeps every sign change of g, with no tolerance.** In the captured histories (β = 10, 20 at M = 10⁻³ and
    0.5 M_P, both tables) it records 22 to 138 crossings over tens of e-folds, most at one T_J to 10⁻⁹. On the old
    table at β = 20, 71 of 88 lie at 846.17002981 GeV, some 7.5 × 10⁻⁶ apart in N.
  - **What that is:** g held at zero on the attractor, its sign set by the solver's error. g's excursions were not
    measured against g's tolerance, which is about β·τ_π/M_P.
  - **Impact:** `surfing_scan`'s `n_crossings` counts them. Its capture, largest-maximum and second-dip columns take the
    first crossings, which are resolved. Not changed (§2).
- **τ is a component's scale, not a bound on the interpolant.**
  - Radau accepts a step whose RMS over the five components of error/scale is at most 1, so one component's error can
    reach √5 τ.
  - The estimate is of the step's end, not of the interior where the pair lies.
  - U8 is ruled as τ; recorded only.
- **SciPy raises rtol to at least 100 ε** (`validate_tol`). τ uses `params.rtol`, so for rtol < 2.2e-14 it would be
  smaller than the solver's scale. No caller passes one (D5).
- **Late near-stall pairs are near the threshold.** The old table's closest kept pair is 1.10 τ (β = 4, M = 10⁻³ M_P,
  N = 52.066). Such pairs, and the two dropped ones, can move across the threshold with the tolerance. The interior
  sampling of ruling 15 was added for exactly such near-stalls, at larger excursions; item (11)'s is 117.6 τ.
- **Small-M histories fail at T_CMB** at the scans' setup, on both trees and both tables:
  - 4 × 10⁻³¹ and 10⁻¹² M_P at β ≥ 1.5, on the step budget, φ ≤ 0, or the guard on a reflection inside the wall;
  - none at β = 0.5 or 1.2.

  This is unrelated to 01a, which changes nothing in them.
- **At α > 0 on the new table, dropping the pair moves trajectories,** as U9 intends: the half-oscillation's A_k, and so
  the gate. Example: test_release's β = 5, M = 0.5 release check moves from N = 40.31009 to 40.31008. 01's re-pins and
  goldens should be made on this tree.
- **A pair split across two steps** (prompt §2) was not searched for systematically. On the new table after 01a, no
  scanned history and no test integration keeps a TurningPoint above 300 GeV within 10⁻⁶ M_P of φ*.
- **The scan probe's test attribution** names the last test started, which for a `setUpClass` integration is the
  previous class's. `run_tests_new_table.py` records the test-file frames as well.

## 6. State handed to the next prompt (01, on resume)

- **Bringing this commit in.**
  - The orchestrator fast-forwards `electroweak-eos` to this commit (U10). It touches none of 01's files and not
    `CLAUDE.md`.
  - No label bump (U11). `config/version.py` is untouched, and 01's `2026.11.0` covers both.
- **The new suite counts:** CosmologyModels 26, ComputeTargets 684 (219 + 465), ObjectFactories 163, tools 277,
  RunDrivers 76. 01's `test_build_w_table` makes tools 292.
- **`CLAUDE.md`'s counts wait for 01.** They are not changed here, because it holds the user's uncommitted edits.
- **On the new table with 01a,** 4 of the 22 tests pass and 18 still fail. The first failing assertion of each:
  - `test_golden_history` β = 1.5 and β = 5.5: "golden history … differs from golden_history_rr03.json";
  - `test_golden_reflecting_history`: "the reflecting golden history differs from golden_history_rr03_reflecting.json";
  - `test_tuning` (e): "the tuned golden history differs from golden_history_rr03_tuned.json";
  - `test_injection` test_11 (pair recorded): `len(multi) >= 1`, "0 not greater than or equal to 1". On the new table,
    item (11)'s β = 10 history no longer has its interior pair before 01a either: a table effect;
  - `test_reachability_roots` d1: Σ_peak 0.037757 against 0.037436; d2: 2.99918 against 3.01. 01 has re-pinned both in
    the main checkout;
  - `test_release` test_7: parks at candidate 8, not 6; test_2: `round(N_check, 5)` 40.31008 against 40.29068;
  - `test_wall_rounding` test_3: 4.88e-8 not ≤ 8.15e-10;
  - `test_drain_law` test_4: the measured-law tuple ('8.2277e-02', '9.1883e-02', …, '1.6185e-01') against
    ('8.2276e-02', '9.1886e-02', …, '1.6459e-01');
  - `test_adiabatic_dark_radiation` test_8a: `max_abs_Q_values` kp_over_H_1E1 13.966 against 18.991;
  - `test_tuned_history_round_trip` test_a: "sample 0: field H_Einstein differs…";
  - `test_history_and_bbn` test_a: 18033 against 15331, with 8 subtests (V_ref, normalization, phi_0, phi_ref,
    pi_0, residual_H, residual_phi, tracking); test_b: 18050 against 15331;
  - `test_history_and_bbn_alpha` test_10: the α = 0 line, RHS=33721 accepted_steps=3715 against 33838/3725;
  - `test_surfing_scan` β = 1.5 tuned: T_capture and T_deliver relative differences 1.38e-5 and 7.3e-6 against 1e-6;
  - `test_plot_surfing_histories` β = 3: 27.86 not ≤ 1e-6.
- **`dropped_drains` at α = 0** is 0 in every history that had the early pair; it was 1. **`first_bounce`** is unchanged.
- **01's handoff steps 2 to 12 stand,** run on this tree.
