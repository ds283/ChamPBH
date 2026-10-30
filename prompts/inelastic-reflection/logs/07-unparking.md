# Log 07 — unparking

**Subject:** Release a parked field where its tracker loses adiabaticity
**Commit:** `5397f8f`, on branch `inelastic-reflection`, on top of `0994a10`
**Date:** 2026-10-30, by Claude Opus 5.5
**Result:** **COMPLETE**, after one stop (ruling 9), ruled by the user as option (c). The gate
"Release" holds, with the lag tested for its sign and order. No issue opened; one narrowed.
- **The release ships.** A parked step end (or N_final on the step that ends the history) at which
  the tracker's m_eff/H_E is below 10 no longer fails the history. The field is released at the root
  of m_eff/H_E = 10 on that step's `ParkedSegment`. The parked phase hands the segment's state there
  back to the loop as a return value. The loop starts a new Radau instance with the drain disarmed,
  the infall assessment cleared and the gate's half-oscillation begun at the release. After a
  release no candidate makes the ratio test or calls the tracker (P24).
- **The gate "Release"** (README §8.4):
  - **α = 0 is bitwise.** Natural mode at M = 0.5 M_P, β = 2 is bitwise the dispatch commit's. The
    golden tests and the five other modules of prompt §1.6 item 1 pass unedited.
  - **Every history that does not release is bitwise 06's**: the 24 of §2.5, whole.
  - **The four released histories are bitwise 06's** up to a window inside the parked phase. The
    check that failed at 06 is the check the release record names, to the digits 06 printed.
  - **Each released history reaches T_CMB**, released at m/H = 10 to within 2.1×10⁻¹² (the derived
    bound is 2.6×10⁻¹¹ at β = 5).
  - **The release is continuous.** The state, H_E and the ledger are continuous at it: the last
    parked segment, the first Radau segment and the dense output agree bitwise there.
  - **No second parking**, although at β = 5 and 10 every candidate after the release would pass
    both of the cut's conditions.
  - **The lag after the release** has the sign of δ₁ and lies within the bound derived from each
    oscillation, on all eight oscillations. The ratio is 0.98–1.20 at β = 5 (tested) and 1.05–1.36
    at β = 10 (measured).
- **Counts:** 26 / 408 (96 + 312) / 103 / 115 / 64 before, 26 / **427** (96 + **331**) / 103 / 115 /
  64 after, all passing. The 19 new tests are in `ComputeTargets/tests/test_release.py`.
- No store was opened, no Ray started and nothing written to `var/`. No golden JSON was regenerated.

## 1. The physics and the numerics

### 1.1 The order in a parked step, with the release

`run_parked_phase` (prompt 06's, as amended):
1. Before each step after the first, the step's anchor is set and the left-node derivative is
   re-evaluated with it (06, unchanged).
2. DOP853 makes one step; the step budget is checked (06, unchanged).
3. The step's `ParkedSegment` is built.
4. **If the step ends the history** (ln T_J < log_T_stop at its end): the termination root N_end on
   the reduced interpolant, appended to `ts` unless the left node already sits on log_T_stop (06).
   Then `check(N_end, segment(N_end))`:
   - if m/H ≥ 10 the history ends there, parked, as in 06;
   - **otherwise the release comes first** (`release_on_step` on [left node, N_end]), and the
     oscillating phase ends the history.
5. **Otherwise** `psolver.t` and the segment are appended, and the tracker at the solver's state is
   checked (06's `y_end`):
   - **below 10, the release** (`release_on_step` on [left node, psolver.t]);
   - otherwise the anchor moves on, and N_stop or N_failsafe end the phase as in 06.

So at the step that reaches N_stop or N_failsafe the check comes first, as at 06.

**`check`** now returns `None` where m/H ≥ 10, folding m/H into the minimum. Below 10 it returns
the failing m/H, **unfolded** (ruling 4). For a history that never fails a check this is the same
float operation in the same order as 06's.

**`release_on_step(segment, appended, N_check, m_check)`** (ruling 2):
1. g(N) = `tracker_m_eff_over_H(policy, hubble, N, segment(N))[0]` − `PARKING_ADIABATICITY_THRESHOLD`,
   with `segment` called on scalars only and the ledger as it stands.
2. g at the left node (`segment.t_min`) is evaluated on the segment, not taken from the check.
3. **g(left) > 0:** g(N_check) is evaluated, and must be < 0. If it is not, the failure is "cannot
   bracket the release" (D3; never raised). N_rel = `brentq(g, left, N_check,
   xtol=RELEASE_ROOT_XTOL, rtol=RELEASE_ROOT_RTOL)`, and `ts[-1] = N_rel`: it replaces the node
   N_check, or N_end on the step that ends the history, and is never appended twice. The segment
   stays.
4. **g(left) ≤ 0:** the release is at the left node. The step's segment, if appended, is popped
   with its node, so no zero-length segment remains, and the step stays counted. **This never arose**
   on §2's histories: on all four the left node is strictly below N_rel (§5.2).
5. y_rel = `segment(N_rel)`, one scalar call (ruling 3). m_rel and H_rel = `tracker_m_eff_over_H` at
   (N_rel, y_rel); m_rel is folded into `Parking.min_m_eff_over_H`.
6. The `Release` record is set, with `candidate_index = len(injections)` and the accepted step count
   at the release kept for `steps_after`.
7. `_ParkedPhaseEnd(N_rel, y_rel, released=True)` is returned. Nothing is drained and nothing is added
   to the ledger.

### 1.2 The root and its tolerance on m/H (ruling 2)

- **brentq** stops once its bracket is narrower than xtol + rtol|N|, with xtol =
  `RELEASE_ROOT_XTOL` = 10⁻¹⁴ and rtol = `RELEASE_ROOT_RTOL` = 4ε (the smallest it accepts). So N_rel
  lies within that of a sign change of g. Two roots of one g lie within twice that,
  2(10⁻¹⁴ + 4ε|N|): 9.2×10⁻¹⁴ at N = 40.25.
- **g's own noise** is the jitter of φ_min's root. `minimum_on_trace` puts it within
  `root_tolerance(φ)` = 2(xtol_w + rtol_w|ln φ|) in ln φ, and it enters m/H through
  |∂(m/H)/∂ ln φ| (a central difference at δ ln φ = 10⁻⁶, in the test).
- **H_E's rounding** is that of exp(ln m − ln H) with logs of order 300, at most 10³ε of 10.
- **The tolerances** are `release_tolerances` in the test, derived per history from the release's own
  segment:
  - in N: 2(xtol + 4ε|N|) + 2·noise/|dg/dN|;
  - on m/H: |dg/dN|·2(xtol + 4ε|N|) + 2·noise + 10³ε·10.

  dg/dN is a central difference on the segment at h = 10⁻⁴.

| History | dg/dN (per e-fold) | g's noise | tolerance in N | tolerance on m/H | measured \|m/H − threshold\| |
|---|---|---|---|---|---|
| M = 0.5, β = 5 | −33.6 | 1.0×10⁻¹¹ | 7.0×10⁻¹³ | 2.6×10⁻¹¹ | 5.2×10⁻¹³ |
| M = 10⁻³, β = 1.2 | −15.4 | 3.9×10⁻¹³ | 1.5×10⁻¹³ | 4.6×10⁻¹² | 9.6×10⁻¹⁴ |
| M = 10⁻⁷, β = 1.2, threshold 100 (ruling 7) | −158 | 5.1×10⁻¹² | 1.7×10⁻¹³ | 4.9×10⁻¹¹ | 9.4×10⁻¹³ |

The root costs 10 evaluations of g (the two bracket ends, then brentq's 8) at 48–69 µs each:
0.44–0.71 ms (§5.1).

### 1.3 The hand-back, and what is reset (rulings 1, 3, 5)

- **`run_parked_phase` returns** `(_ParkedPhaseEnd, parked steps, smallest m/H)`.
  `_ParkedPhaseEnd(N, state, released)` is either the history's end (released False; the state a
  `StateVector`, as 06 returned) or the release (released True; the state `segment(N_rel)`).
- **`cut` returns** `None` (go on) or that `_ParkedPhaseEnd`, after `Parking` has been updated with
  the parked steps and the minimum. No exception carries the release.
- **The loop tells the two apart in both branches:**
  - at a reflection, `if parked.released: solver = resume_after_release(parked); continue`. No
    restart is made from `y_reflected`;
  - at a drain point, `solver = resume_after_release(parked); y_drained = None`. No restart is made
    from `y_drained`; `armed` is cleared after it as before;
  - otherwise `N_final, final_state = parked.N, parked.state; break`, as at 06.
- **`resume_after_release(end_)`** (nested; nonlocal):
  - sets `armed = None`, `armed_assessment = None` and `infall_assessment = None`;
  - sets `running_max = None` and `boundary_N = N_rel`;
  - returns `new_solver(N_rel, np.array(state, dtype=float))`.
- **Continuity is exact by construction** (ruling 3). Radau's dense output at its left node returns
  the state it was built from, the dense output is right-continuous at nodes (`alt_segment=True`),
  and `ts[-1]` is the same float N_rel. So the three floats agree bitwise, and H_E either side,
  formed from the same state and ledger, is the same float.
- **P24.** In `cut`, `if release is None and parking_ratio_test(index, ratio)`. After a release,
  neither `parking_ratio_test` nor `cut_tracker` is called. The ratio is still formed, for the
  guard's reason. The guard runs at every reflection. After a release its reason reads
  `m_eff/H_E = not computed (the field was released at N = <N_rel:.10g>)`. Before one it reads 06's
  "not computed (the ratio test did not pass)", or the tracker's m/H.
- **The release record's `steps_after`** is `accepted_steps` at the history's end less its value at
  the release, filled in when `IntegrationResult` is built.

**`running_max` and `boundary_N` at the switch** (ruling 6), at M = 0.5, β = 5:
- `boundary_N` = 37.990648, the wall-side turning point that armed candidate 105;
- `running_max` = 0.132, the largest A over the 104 samples in (37.990648, 38.004408].

The first turning point after the release (a plateau at N = 40.8417) has A_k = 0.688, the largest
over its own post-release samples. **So B4 does not show on the unpatched history**: the carried
0.132 is below 0.688, and the maximum is unchanged. The sentinel test (§2) shows it.

### 1.4 The lag after the release (ruling 9, as amended by the user)

- **The quantities.** x = φ − φ_min, with φ_min from `parked_tracker` on the history's own trace,
  searched from the release's φ. δ₁ = (dπ_min/dN − F)/(∂F/∂φ):
  - F is the right-hand side's π′ at (φ_min, π_min), the code's own equation linearised about the
    tracker;
  - dπ_min/dN is `np.gradient` on a grid of 2 000 points per e-fold (spacing 5×10⁻⁴);
  - ∂F/∂φ and ∂²F/∂φ² are central differences at h = 10⁻⁶ φ_min.
- **The comparison.** Per oscillation, from one wall-side turning point to the next: the means ⟨x⟩
  and ⟨δ₁⟩, trapezoid over the grid points inside the oscillation, divided by their span.
- **The bound derived from that oscillation:**
  - **T1, the non-closure:** |x(b) − x(a)|/2π, with x at the two turning points themselves. An
    oscillation whose amplitude changes by ΔA between two points of equal phase has window mean
    (ΔA/2π) sin(phase), at most ΔA/2π; x(b) − x(a) measures that change.
  - **T2, the anharmonic shift of a half-swing S:** |F_φφ/F_φ|·S²/4, with S = (max x − min x)/2 over
    the oscillation and the derivatives' means there. With x″ = F_φ x + F_φφx²/2, the time average
    gives ⟨x⟩ = −(F_φφ/F_φ)⟨x²⟩/2 = −(F_φφ/F_φ)S²/4.
- **The test:** on every oscillation sign(⟨x⟩) = sign(⟨δ₁⟩), and |⟨x⟩ − ⟨δ₁⟩| ≤ T1 + T2. Neither the
  bound nor the ratios are fitted.
- **What it catches:**
  - δ₁ × −1 fails it on all eight oscillations, by the sign (C1, §6);
  - **δ₁ × 2 and × ½ are not catchable**: the bound is 1.9–3.7 |⟨δ₁⟩|, against the 0.81–1.02 and
    0.48–0.70 |⟨δ₁⟩| by which they move the comparison (§5.4).

**The stop.** The prompt asked for a tolerance derived from the half-swing and the non-closure that
fails δ₁ × 2. The derivation above cannot fail it: the measured agreement (residual ≤ 0.20 |δ₁|) is
much tighter than any bound I could derive. I stopped (ruling 9). The user ruled option (c): "OK, go
with (c). I am not sure this matters a lot."

## 2. What shipped

### `ComputeTargets/ScalarHistory.py`

- **Constants**: `RELEASE_ROOT_XTOL = 1.0e-14` and `RELEASE_ROOT_RTOL = 4.0 * float(np.finfo(float).eps)`,
  beside the parking constants, neither a key. The comments on `PARKING_ADIABATICITY_THRESHOLD` and
  `tracker_adiabatic`'s docstring say a step end below it now releases.
- **Records**, appended, with comments:
  - `Release(N, log_T_Jordan, phi_Einstein, pi_Einstein, m_eff_over_H, N_check, m_eff_over_H_check,
    H_Einstein, candidate_index, steps_after)`, floats and ints;
  - `IntegrationResult(..., parking, release)`, with no default (ruling 11: no test builds one);
  - `_ParkedPhaseEnd(N, state, released)`, the hand-back (private);
  - `Parking`'s comments on `parked_steps` and `min_m_eff_over_H` say what they hold for a released
    history. Its fields are unchanged.
- **`integrate_scalar_history`**:
  - `release` and `accepted_at_release` beside `parking`;
  - in `run_parked_phase`: `fold_minimum`, `check` returning the failing m/H, `release_on_step`, and
    the two step ends of §1.1. **The raise of 06's adiabaticity failure, and its text, are gone**
    (ruling 12). Every other reason the parked phase can fail with keeps 06's text;
  - `cut`: P24 and the guard's reason;
  - `resume_after_release`;
  - the two branches of the loop;
  - `release` in the result.

  The docstring gains a paragraph on the release, and `run_parked_phase`'s and `cut`'s docstrings say
  what they return.
- **`integrate_history`**: the payload gains `"release"`, the record or `None`, not stored.
- **Unchanged**:
  - the cut's conditions and the constants;
  - `cut_tracker`, `parked_tracker`, `tracker_velocity_terms`, `tracker_m_eff_over_H`,
    `parking_leftover`, `ParkedRHS` and `ParkedSegment`;
  - the model methods, the policies, `ODERHS` and the ledger;
  - `ScalarHistory.store`, `build_extra_data`, `golden_history.record_of`, `TUNING_OUTPUT_KEYS`,
    `tune_history`, `compute_scalar_history`;
  - `"reflections"`, `"parking"`, and every existing record's fields.

### The tests (`ComputeTargets/tests/test_release.py`, new; 19 tests, 45 s at a load of 8–14)

| Test | Item | Asserts | Tolerance |
|---|---|---|---|
| `test_2_it_reaches_T_CMB_and_integrate_history_succeeds` | 2 | M = 0.5, β = 5: success; ln T_J(end) = ln T_CMB; payload's `"release"`, `"parking"` = the result's | 9 places |
| `test_2_it_parks_at_105_and_is_released_once_inside_the_step_06_failed_on` | 2, ruling 8 | parks at #105 (a drain point); `N_check` = 40.29089 to 5 decimals (06's failure); left node < N_rel < N_check, the segment's t_min and t_max; \|N_rel − 40.25454\| ≤ 5×10⁻⁶ + the tolerance in N; the ParkedSegments are exactly the parked steps, contiguous, ending at the release step; every later segment Radau's | 5×10⁻⁶ + 7.0×10⁻¹³ |
| `test_2_m_over_H_at_the_release_is_the_threshold` | 2, ruling 2 | \|m/H(root) − 10\| ≤ `release_tolerances`; dg/dN < 0; the tolerance < 10⁻⁹ | 2.6×10⁻¹¹ (derived) |
| `test_2_the_release_record_by_construction` | 2, ruling 4 | `N` = ts[k]; ln T_J, φ, π = the segment's at N_rel; m/H and H_E = `tracker_m_eff_over_H` there; m/H_check < 10; `candidate_index` = candidates before N_rel = 106; `steps_after` = segments after; `Parking.min_m_eff_over_H` = the root's m/H (no earlier check is smaller); floats and ints | bitwise |
| `test_2_the_readers` | 2 | `first_bounce` before the switch; the fixed-T values exist; T_J = 1 MeV and 70 keV located after the release | — |
| `test_4_continuity_at_the_release` | 4, ruling 3 | the last parked segment, the first Radau segment and the dense output at N_rel; the record's φ, π, ln T_J; H_E either side and the record's; K(nextafter(N_rel, 0)) = K(N_rel); no ledger entry between the switch and the first candidate after the release | bitwise |
| `test_5_the_candidates_after_the_release_drain` | 5 | 15 candidates after the release, all drain points with α_eff > 0; the ledger is exactly the candidates' (N, ΔK) with the leftover after #105's | bitwise |
| `test_5_no_ratio_test_and_no_tracker_after_the_release` | 5, ruling 5 | spies through the module's names: `parking_ratio_test` called at indices 0–105 only, `cut_tracker` never after the switch (its last call at the switch); not vacuous: of the 15 candidates after the release at least one has ratio < ε and tracker m/H ≥ 10 (all 15 have) | exact |
| `test_6_the_lag_after_the_release_against_delta_1` | 6, ruling 9 (c) | eight oscillations; on each, sign(⟨x⟩) = sign(⟨δ₁⟩) and \|⟨x⟩ − ⟨δ₁⟩\| ≤ T1 + T2 (§1.4); ⟨δ₁⟩ < 0 | derived per oscillation |
| `test_5_the_sentinel_marked_samples_before_the_switch` | 5, ruling 6 | the sentinel replaced samples (in (N_turn, N_switch]); the window holds a candidate after the release | — |
| `test_5_the_first_turning_point_and_candidate_after_the_release` | 5, ruling 6 | window to N = 41.5, with and without the sentinel 10³: the first turning point and candidate after the release carry no sentinel; turning points, injections, parking, release, ts, final state and the ledger equal | bitwise |
| `test_3_the_records` | 3 | window to N = 40.0 against the full history: no release; injections (106), reflections, turning points ≤ 40.0, the parking record but its last two fields, warnings | bitwise |
| `test_3_ts_and_every_segment_at_its_nodes` | 3 | every node but the window's last, and every segment but its last at both nodes (more than the parked steps) | bitwise |
| `test_7_parks_at_6_is_released_near_0_27_meV_and_reaches_T_CMB` | 7 | M = 10⁻³, β = 1.2: success; parks at #6; N_rel = 45.91 (2 places), T_J = 0.27 meV (2 places); m/H at the root; T_CMB; no candidate or turning point after it; continuity | 4.6×10⁻¹² on m/H |
| `test_8_a_T_final_inside_the_release_step_releases_and_ends_oscillating` | 8, ruling 8 | T_final = 9.5 MeV: released; ends in a Radau segment at T_final; `N_check` is the parked step's end at T_final, before the full history's; \|N_rel − full\| ≤ the tolerance in N; continuity | 7.0×10⁻¹³ (measured 1.4×10⁻¹⁴) |
| `test_8_a_T_final_above_the_release_ends_parked` | 8 | T_final = 10 MeV: parked, not released, `"release"` None; last segment parked; N_final < N_rel; min m/H ≥ 10 | 9 places |
| `test_8_an_N_stop_inside_the_release_step` | 8, ruling 8 | N_stop = N_rel + 0.01: released; N_final = `N_check` = N_stop; \|N_rel − full\| ≤ the tolerance in N + `dop853_root_shift`; not bitwise; continuity | 5.9×10⁻⁷ (measured 2.9×10⁻⁷) |
| `test_8_an_N_stop_past_the_release_step_is_bitwise` | 8, ruling 8 | N_stop = 40.5: the record but `steps_after` bitwise the full history's; N_final = 40.5; no candidate after the release yet; continuity | bitwise |
| `test_R7_parked_at_a_reflection_released_and_handed_back` | ruling 7 | M = 10⁻⁷, β = 1.2, threshold 100, to N = 45.95: parked at #6, an unresolved reflection; released; m/H at the root = 100; N_final = 45.95; more than one segment after the release, all Radau's, = `steps_after`; continuity | 4.9×10⁻¹¹ on m/H |

- **`dop853_root_shift`** (item 8's third end). With N_stop inside the release step, DOP853's last step
  is cut at N_stop, so the release's segment is a different interpolant. Each interpolant holds the
  reduced state within atol + rtol|y_j| per component (10⁻⁸ each). So g moves by at most
  Σ_j |∂(m/H)/∂y_j|(atol + rtol|y_j|) over (ln ρ_R,E, ln f_m, ln T_J), and the root by that over
  |dg/dN|. The bound is twice that, for two segments: 5.85×10⁻⁷, against a measured 2.87×10⁻⁷.
- **Helpers in the module**: `integrate` (test_injection's setup, with the result, the RHS and its
  supervisor captured, to T_CMB or a T_final, to an N_stop); `beta_5` (cached, with the two spies);
  `hubble_of`, `m_over_H`, `release_step`, `root_tolerance_N`, `release_tolerances`,
  `dop853_root_shift`, `lag_by_oscillation`, `lag_failures`, `sentinel_patch` and
  `assert_continuous_at_the_release`. It imports `golden_tuned_history`, `test_unresolved_turnarounds`
  (`window`) and `test_parking` (`root_tolerance`), none edited.

### The edit to `test_parking.py` (prompt §1.6 item 9)

- **The module docstring's item (7)**: "and a failure where the tracker loses adiabaticity" becomes
  "and, where the tracker loses adiabaticity, a release (inelastic-reflection prompt 07, which
  replaced 06's failure there; test_release tests the release itself)".
- **`TestAtHalfThePlanckMass`'s docstring**: "beta = 5 to T_CMB (it fails, about 5 s)" becomes "(it is
  released and reaches T_CMB since prompt 07, about 6 s; at 06 it failed)".
- **`test_7_at_beta_5_it_fails_where_the_tracker_loses_adiabaticity` is replaced by
  `test_7_at_beta_5_it_is_released_where_the_tracker_loses_adiabaticity`.** It asserts:
  - the history succeeds;
  - `"release"` is not None;
  - the parking record is the window's, but for `parked_steps` and `min_m_eff_over_H`;
  - the release's `m_eff_over_H_check` < 10;
  - both N_rel and `N_check` lie in the window the old test put the failure in,
    (`BETA_5_CROSSING_N` − 10⁻³, `BETA_5_CROSSING_N` + 10⁻³ + 0.1];
  - one warning.

  **Reason:** the test asserted 06's failure, which this prompt replaces with the release (prompt
  §0.3). Nothing else in `test_parking.py` changed. Its 34 tests keep their count.

### The documents (P12)

- `.documents/architecture-summary.md`: a dated note after 06's in §7.2, covering:
  - the constants, `Release`, `_ParkedPhaseEnd`, `IntegrationResult.release` and the payload key;
  - `Parking`'s two fields for a released history;
  - how a reader tells the phases apart;
  - the four released histories, and P24 at β = 5 and 10.
- `.documents/numerical-strategies.md`: new §6.6, dated: the release point and its root, the
  hand-back, the reset, P24, the lag, and large M with the four histories.
- Both correct the unit of 06's "0.25 eV" (§7) in their own new text. 06's notes are left as they
  were (`CLAUDE.md` invariant 6).

## 3. Deviations from the prompt

**The rulings of orchestrator note 07 (1–14), ruling 9 as amended by the user:**

| # | Ruling, as applied | Class |
|---|---|---|
| R1 | The hand-back is `_ParkedPhaseEnd`, a return value from `run_parked_phase` through `cut`. Both branches resume through `resume_after_release`: new Radau from the released state (no restart from `y_drained` or `y_reflected`); `armed`, `armed_assessment` and `infall_assessment` cleared; `running_max = None`, `boundary_N = N_rel` | IMPLEMENTATION CHOICE (ruled) |
| R2 | g on the segment by scalar calls; g(left) evaluated on the segment; brentq with xtol 10⁻¹⁴ and rtol 4ε; the tolerance on m/H derived in §1.2 from the root's tolerance, \|dg/dN\| and g's noise. g(left) ≤ 0 → release at the left node, the segment popped, the step counted: it never arose (§5.2), and it is not tested. N_rel replaces `ts[-1]`, never appended twice | IMPLEMENTATION CHOICE (ruled) |
| R3 | The released state is `segment(N_rel)`, one scalar call; `ts[-1]` is the same float. Item 4 asserts the three floats and H_E bitwise | STRUCTURALLY REQUIRED |
| R4 | `check` folds only a passing m/H; the failing check's m/H is `Release.m_eff_over_H_check`; the root's m/H is folded. Item 2 asserts `min_m_eff_over_H` = the root's, bitwise (no earlier check is smaller on β = 5) | STRUCTURALLY REQUIRED |
| R5 | `release is None and parking_ratio_test(...)`; both functions spied through the module's names; the guard's reason after a release; non-vacuity computed in the test (15 of 15 at β = 5) | IMPLEMENTATION CHOICE (ruled) |
| R6 | The sentinel 10³ on `step_nonadiabaticity`'s samples in (N_turn of #105, N_switch], window to N = 41.5: the marked history is bitwise the plain one; B4 fails it. B4 does not show on the unpatched history (§1.3) | STRUCTURALLY REQUIRED |
| R7 | The reflection branch tested at M = 10⁻⁷, β = 1.2, threshold 100, N_stop = 45.95 (released at 45.8826 in the step ending 45.9444). B7 fails it | IMPLEMENTATION CHOICE (ruled) |
| R8 | Item 2's reference: 40.25454 ± (5×10⁻⁶ + the tolerance in N), defined in `test_release` (`BETA_5_CROSSING_N` in `test_parking` is the oscillating crossing, 40.2547; the orchestrator agreed); N_check = 40.29089 to 5 decimals. Item 8's ends: T_final = 9.5 MeV and N_stop = N_rel + 0.01 within a stated bound, not bitwise; N_stop = 40.5 bitwise. "N_stop just after the release" is both of the last two | IMPLEMENTATION CHOICE (ruled) |
| R9 | **As amended by the user** ("OK, go with (c). I am not sure this matters a lot."): item 6 is a sign-and-order test on the β = 5 integration over the first eight oscillations. On each, ⟨φ − φ_min⟩ has the sign of ⟨δ₁⟩, and \|⟨φ − φ_min⟩ − ⟨δ₁⟩\| ≤ T1 + T2, computed from that oscillation. δ₁ × −1 fails it (C1). δ₁ × 2 and × ½ are not catchable (§5.4, with the bound beside them). β = 10 measured, not tested | IMPLEMENTATION CHOICE, ruled by the user |
| R10 | One worktree of `35a103c` (and one of the final tree, D7), removed. The 24 non-releasing histories whole; the four released ones through a window in the parked phase; the dispatch's failure reason against the record, equal as printed (§5.5) | IMPLEMENTATION CHOICE (ruled) |
| R11 | `Release` of floats and ints, appended last, no default (no test builds `IntegrationResult` positionally); `"release"` in the payload, not stored | IMPLEMENTATION CHOICE (ruled) |
| R12 | 06's adiabaticity failure and its text removed with the raise; every other parked-phase reason keeps 06's text. No history of §2.2 raised an oscillating-phase failure after a release | IMPLEMENTATION CHOICE (ruled) |
| R13 | Only the prompt's §6 files changed. `CosmologyConcepts/Models/`, `ComputeTargets/Policies/`, `dark_radiation.py`, `exceptions.py`, the golden modules and JSON, the tools and drivers, and every test module but `test_parking.py` (item 9's three places) are byte-identical | as ruled |
| R14 | Root and Radau costs: minima over 200 repetitions. Per-history costs: three rounds back to back. Suites serial, in halves, to scratch, [g-z] in the background. §2's histories four at a time under `timeout 900` (none reached it); the β = 5 tunings one at a time | as ruled |

**My own deviations:**

| # | Deviation | Class |
|---|---|---|
| D1 | The names: `Release` and its fields, `_ParkedPhaseEnd`, `RELEASE_ROOT_XTOL`/`RTOL`, `release_on_step`, `fold_minimum`, `resume_after_release` | IMPLEMENTATION CHOICE |
| D2 | The root's tolerances are module constants beside the parking ones. They are not P18's constants, which are unchanged | IMPLEMENTATION CHOICE |
| D3 | One new failure reason, "cannot bracket the release on the step […]", where g is not negative at the check on the segment although the check found m/H < 10. The check reads the solver's state and the segment its interpolant, which differ by rounding. Never raised | IMPLEMENTATION CHOICE |
| D4 | A NaN m/H at a check, which 06 failed with the adiabaticity text, now goes to the release, whose bracket test fails with D3's reason | IMPLEMENTATION CHOICE |
| D5 | Item 7's temperature is 0.27 **meV** (2.66×10⁻¹⁰ MeV), not 0.27 eV as prompt §0.3 and §1.6 item 7 print it; the test asserts meV (§7) | STRUCTURALLY REQUIRED |
| D6 | Item 8's N_stop inside the release step needs `dop853_root_shift` beside the root's tolerance: the two segments are different interpolants, 2.9×10⁻⁷ apart in N_rel, which the root's tolerance (7×10⁻¹³) alone cannot hold | STRUCTURALLY REQUIRED |
| D7 | The measurements ran in two scratch worktrees: one of `35a103c`, and one of `0994a10` holding copies of the final `ScalarHistory.py`, `test_release.py` and `test_parking.py` (md5 `4d9e74a6…`, `01f27dad…` and the final `test_release.py`'s). The breakages were replayed in the main checkout. An early development worktree of `35a103c` was used to draft the code before the stop. All three worktrees were removed | IMPLEMENTATION CHOICE |
| D8 | **The process slip.** At the start, before the baselines had finished, I made the first edits (the records) in the main checkout while ComputeTargets [a-f] was running. I set them aside and restored the file before [g-z] started. [a-f] had imported the module at discovery, about a minute before the edits, so its 96/OK stands. **No effect on the baselines.** All later development was in scratch until the baselines had finished | UNINTENDED DRIFT |
| D9 | `TestTheReleaseAtHalfThePlanckMass.setUpClass` tolerates a failed β = 5 history, so that B6 fails `test_2_it_reaches_T_CMB…` instead of erroring the class. The ruling-7 test checks N_final and the segments before continuity, so that B7 fails it rather than errors | IMPLEMENTATION CHOICE |
| D10 | Item 3 compares the parking record but its last two fields (`parked_steps`, `min_m_eff_over_H`), which a window ending earlier must differ in | STRUCTURALLY REQUIRED |
| D11 | The lag's means divide by the span of the grid points inside each oscillation, not by b − a as `lag.py` does, so the ratios differ from `lag.out` in the third digit | IMPLEMENTATION CHOICE |
| D12 | Tests beyond the prompt's list: item 2's readers, the sentinel's own sanity test, item 7's continuity, item 8's continuity at each released end | IMPLEMENTATION CHOICE |

## 4. Verification performed

### 4.1 The suites

Serially, ComputeTargets in two halves, output in my scratch directory. "Before" is at `35a103c`
before any edit (D8), "after" is on the final tree.

| Suite | Before (count, time, load before → after) | After |
|---|---|---|
| CosmologyModels | 26 OK, 86 s, 18.9 → 47.2 | 26 OK, 114 s, 42.7 → 138.7 |
| ComputeTargets [a-f] | 96 OK, 129 s, 47.2 → 13.2 | 96 OK, 160 s, 138.7 → 56.2 |
| ComputeTargets [g-z] | 312 OK, 377 s, 13.2 → 6.6 | **331** OK, 412 s, 56.2 → 8.1 |
| ObjectFactories | 103 OK, 17 s, 6.6 → 5.7 | 103 OK, 18 s, 8.1 → 7.9 |
| tools | 115 OK, 129 s, 5.7 → 7.8 | 115 OK, 141 s, 7.9 → 11.9 |
| RunDrivers | 64 OK, 27 s, 7.8 → 7.3 | 64 OK, 26 s, 11.9 → 9.1 |

- The baselines are prompt §3.1's: 26 / 408 (96 + 312) / 103 / 115 / 64.
- ComputeTargets rose by 19, the new module, which sorts into [g-z]. Nothing fell.
- **The golden tests passed unchanged**: `test_golden_history`, the tuned golden test,
  `test_first_bounce`, `test_surfing_turning_points`, `test_kinematic_cap_loop`,
  `test_unresolved_turnarounds`, `test_injection`, `test_gate_and_taper`, and `test_parking` but for
  item 9's test.
- `black --check` on the three changed Python files: all unchanged.

## 5. The measurement (prompt §2)

Every history: test_injection's setup (n = 1, φ* = 5 M_P, π* = 0, T* = 2×10⁴ GeV, to T_CMB, default
tolerances, the seed realization, untuned), α = 0.1 unless stated, `integrate_history` with its
sampling. The scripts are in my scratch directory, `agent07/meas/`: `common.py`, `released.py`,
`snap.py`, `run_snaps.sh`, `compare_snaps.py`, `cost_release.py`, `cost_hist.py`, `run_cost.sh`,
`tune.py`, `ends.py` and `test_numbers.py`.

### 5.1 The cost (prompt §2.1)

**The release.** Minima and medians of 200 repetitions at the release's own segment, load 12.2:

| History | g evaluations | root, bracket ends included: min (median) | one g | new Radau instance: min (median) |
|---|---|---|---|---|
| M = 0.5, β = 5 | 2 + 8 | 0.711 ms (0.749) | 68.9 µs | 0.215 ms (0.229) |
| M = 10⁻³, β = 1.2 | 2 + 7 | 0.439 ms (0.462) | 48.1 µs | 0.172 ms (0.183) |

**Per history.** `integrate_history` with sampling, the minimum of three rounds back to back, load
8.4–17.1:

| History | released (s) | steps | never parked, ε = 0 (s) | steps |
|---|---|---|---|---|
| M = 0.5, β = 5 | 5.420 (6.037, 5.420, 5.753) | 8 295 | 6.263 (6.862, 6.373, 6.263) | 9 270 |
| M = 0.5, β = 10 | 5.892 (5.911, 5.910, 5.892) | 8 485 | 7.989 (8.037, 8.026, 7.989) | 11 996 |
| M = 0.5, β = 25 | 6.137 (6.207, 6.137, 6.154) | 8 248 | 11.620 (11.620, 11.710, 11.782) | 17 232 |
| M = 10⁻³, β = 1.2 | 1.585 (1.604, 1.604, 1.585) | 2 044 | 5.880 (6.022, 5.880, 6.087) | 10 598 |

**M = 0.5, β = 2 (no release)**: dispatch `35a103c` 2.598 s (2.598, 2.691, 2.801); final 2.579 s
(2.579, 2.634, 2.742); 3 969 steps each. No measurable cost.

### 5.2 The released histories (prompt §2.2)

| | M = 0.5, β = 5 | M = 0.5, β = 10 | M = 0.5, β = 25 | M = 10⁻³, β = 1.2 |
|---|---|---|---|---|
| parks at | #105, drain point, N = 38.004408, 81.199 MeV, m/H 122.7 | #99, drain point, N = 61.095895, 358.04 MeV, m/H 267.2 | #83, drain point, N = 132.550768, 9 971.8 MeV, m/H 77.96 | #6, drain point, N = 28.371912, 11.467 keV, m/H 19.63 |
| released at (N, T_J) | 40.254538273, 9.74536 MeV | 65.327267386, 8.69547 MeV | 145.779285895, 40.2728 keV | 45.910203429, 0.266469 meV |
| m/H at the root | 10 − 5.2×10⁻¹³ | 10 + 1.9×10⁻¹³ | 10 − 2.1×10⁻¹² | 10 − 9.6×10⁻¹⁴ |
| the release step (left node, check that failed: N, m/H) | 40.19089006, **40.29089006, 8.9373** | 65.25546838, **65.34552793, 9.5492** | 145.7357552, **145.7894436, 9.1946** | 45.85242158, **45.95242158, 9.3640** |
| release at the left node? | no | no | no | no |
| steps before + after (parked steps) | 6 606 + 1 689 (23) | 6 225 + 2 260 (46) | 6 784 + 1 464 (140) | 2 031 + 13 (176) |
| turning points after (wall + plateau) | 30 (15 + 15) | 38 (19 + 19) | 2 (1 + 1) | 0 |
| candidates after (kinds) | 15, all drain points | 19, all drain points | 1, a drain point | 0 |
| Σ Δρ/ρ_R after | 3.368×10⁻¹⁰ | 2.151×10⁻¹⁰ | 1.042×10⁻⁸ | 0 |
| reflections after; guard fired | 0; no | 0; no | 0; no | 0; no |
| tracker's smallest m/H after; back above 10 at | 1.24 at N = 46.557; N = 51.448 | 1.65 at N = 71.490; N = 75.677 | 2.31 at N = 146.230; N = 149.581 | 8.25 at N_final = 46.030; never |
| candidates after with ratio < ε and tracker m/H ≥ 10 (would park but for P24) | **15 of 15** | **19 of 19** | 0 of 1 | 0 of 0 |
| outcome, wall | T_CMB, N = 62.7518551, 5.8 s | T_CMB, N = 86.7571360, 6.0 s | T_CMB, N = 159.8164007, 6.3 s | T_CMB, N = 46.0303778, 1.7 s |

- The left node is never the release, so ruling 2's g(left) ≤ 0 branch never arose.
- **The "tracker's smallest m/H after the release"** is on the tracker at the history's own trace, on a
  grid of 200 points per e-fold to N_final. At β = 5 and 10 the tracker climbs back above 10 within an
  e-fold, and every candidate after the release passes both conditions. Then it falls again, to
  1.2–1.7 several e-folds later, after the last candidate. At β = 25 one candidate
  follows, with ratio 2.2×10⁻⁶ > ε.
- **No oscillating-phase failure was raised after a release** on any history (ruling 12).

### 5.3 Released against never parked (prompt §2.3)

"max | end" is released less never parked, over the window from the switch (or the release) to the
earlier N_final, at 4 001 points. H_E is relative; H_J is relative to the never-parked H_E, since H_J
passes through zero at a bounce.

| | β = 5 | β = 10 | β = 25 | M = 10⁻³, β = 1.2 |
|---|---|---|---|---|
| N_final, released − never parked | −8.87×10⁻⁷ | −3.47×10⁻⁶ | −1.05×10⁻⁵ | −1.19×10⁻³ |
| Σ Δρ/ρ_R with the leftover (relative difference) | 0.00196674349 vs 0.00196580508 (+4.77×10⁻⁴) | 0.00140725495 vs 0.00140626294 (+7.05×10⁻⁴) | 0.000419996835 vs 0.00041912384 (+2.08×10⁻³) | 0.0150923579 vs 0.0150922738 (+5.58×10⁻⁶) |
| ln T_J after the switch, max \| end | 2.04×10⁻⁴ \| −1.96×10⁻⁶ | 2.34×10⁻⁴ \| −8.45×10⁻⁶ | −1.02×10⁻² \| −2.78×10⁻⁵ | −1.74×10⁻³ \| −1.26×10⁻³ |
| ln T_J after the release | 2.04×10⁻⁴ \| −1.96×10⁻⁶ | 2.35×10⁻⁴ \| −8.45×10⁻⁶ | −1.02×10⁻² \| −2.78×10⁻⁵ | −1.74×10⁻³ \| −1.26×10⁻³ |
| ln ρ_R,E after the switch | −6.27×10⁻⁶ \| −4.8×10⁻⁹ | 1.59×10⁻⁵ \| −8.7×10⁻⁷ | 2.51×10⁻⁵ \| −3.5×10⁻⁷ | 7.0×10⁻¹² \| −2.7×10⁻¹² |
| ln ρ_R,E after the release | 1.16×10⁻⁶ \| −4.8×10⁻⁹ | −1.14×10⁻⁶ \| −8.7×10⁻⁷ | 1.72×10⁻⁶ \| −3.5×10⁻⁷ | −2.8×10⁻¹² \| −2.7×10⁻¹² |
| H_E after the switch | 1.44×10⁻⁶ \| 4.84×10⁻⁷ | 8.72×10⁻⁷ \| 3.02×10⁻⁷ | 1.84×10⁻⁵ \| 8.39×10⁻⁶ | −8.31×10⁻⁵ \| −8.31×10⁻⁵ |
| H_E after the release | 1.44×10⁻⁶ \| 4.84×10⁻⁷ | 7.83×10⁻⁷ \| 3.02×10⁻⁷ | 1.84×10⁻⁵ \| 8.39×10⁻⁶ | −8.31×10⁻⁵ \| −8.31×10⁻⁵ |
| H_J/H_E after the switch | −0.105 \| 3.61×10⁻⁶ | −0.59 \| 2.76×10⁻⁶ | 10.4 \| 9.87×10⁻⁵ | −1.17×10⁻² \| −1.17×10⁻² |
| H_J/H_E after the release | 1.45×10⁻² \| 3.61×10⁻⁶ | −4.81×10⁻² \| 2.76×10⁻⁶ | 10 \| 9.87×10⁻⁵ | −1.17×10⁻² \| −1.17×10⁻² |
| φ at 1 MeV (relative) | 0.0082239338 vs 0.0082263665 (−2.96×10⁻⁴) | 0.0085663043 vs 0.0085694465 (−3.67×10⁻⁴) | 0.0093955972 vs 0.0094002536 (−4.95×10⁻⁴) | equal (before the switch) |
| ρ_NP/ρ_R,J at 1 MeV | −0.99011938 vs −0.99009634 | −0.99844637 vs −0.99844726 | −0.99995677 vs −0.99995643 | equal |
| φ at 70 keV (relative) | 0.0099408382 vs 0.0099393953 (+1.45×10⁻⁴) | 0.010449869 vs 0.010450172 (−2.9×10⁻⁵) | 0.01166641 vs 0.011666193 (+1.86×10⁻⁵) | equal |
| ρ_NP/ρ_R,J at 70 keV | −0.98984558 vs −0.98984562 | −0.99836485 vs −0.9983638 | −0.99995752 vs −0.99995773 | equal |

- **Where the largest differences are set.** At β = 5 the largest ln T_J difference after the release
  equals the one after the switch: it is set at or before the release, by the parking itself (R17).
  The same holds for the other three.
- **The large relative H_J differences** are the phase of an oscillation, where H_J passes near zero
  in one history and not in the other. They are not a difference of the expansion.
- **The released histories drain more** than the never-parked ones, as the probe found. The leftover
  is all of ρ_kin at the cut, while in the oscillation the cut ends, Hubble friction takes most of it.
- **At M = 10⁻³, β = 1.2** both histories reach 1 MeV and 70 keV before the switch (11.5 keV), so the
  fixed-T values are equal.

### 5.4 The lag (prompt §2.4)

**β = 5 and 10, per oscillation** over the first eight after the release. m/H is the tracker's mean
over the oscillation. "S" is the half-swing. T1 and T2 are §1.4's bound terms. The last two columns
are by how much δ₁ × 2 and × ½ move the comparison: the check fails only where that exceeds the bound
(units of |⟨δ₁⟩|).

| β | # | [a, b] | m/H | ⟨x⟩ (M_P) | ⟨δ₁⟩ (M_P) | ratio | S/\|δ₁\| | T1 | T2 | bound T1 + T2 | \|r − 2\| (×2) | \|r − ½\| (×½) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 5 | 1 | [41.1136, 41.6240] | 12.2 | −2.367e-6 | −1.981e-6 | 1.195 | 23.0 | 1.417 | 2.229 | 3.646 | 0.805 | 0.695 |
| 5 | 2 | [41.6240, 41.9670] | 18.2 | −1.002e-6 | −8.480e-7 | 1.181 | 35.0 | 1.469 | 2.149 | 3.618 | 0.819 | 0.681 |
| 5 | 3 | [41.9670, 42.2272] | 24.0 | −5.820e-7 | −5.395e-7 | 1.079 | 40.2 | 1.512 | 1.770 | 3.281 | 0.921 | 0.579 |
| 5 | 4 | [42.2272, 42.4379] | 29.6 | −4.386e-7 | −3.890e-7 | 1.127 | 43.1 | 1.358 | 1.447 | 2.805 | 0.873 | 0.627 |
| 5 | 5 | [42.4379, 42.6152] | 35.0 | −3.248e-7 | −3.061e-7 | 1.061 | 44.5 | 1.171 | 1.196 | 2.367 | 0.939 | 0.561 |
| 5 | 6 | [42.6152, 42.7703] | 40.2 | −2.367e-7 | −2.176e-7 | 1.088 | 53.3 | 0.969 | 1.207 | 2.177 | 0.912 | 0.588 |
| 5 | 7 | [42.7703, 42.9079] | 45.3 | −1.794e-7 | −1.833e-7 | 0.978 | 55.8 | 1.054 | 1.101 | 2.155 | 1.022 | 0.478 |
| 5 | 8 | [42.9079, 43.0316] | 50.3 | −1.748e-7 | −1.718e-7 | 1.017 | 52.4 | 0.960 | 0.904 | 1.864 | 0.983 | 0.517 |
| 10 | 1 | [65.9366, 66.3789] | 14.1 | −1.990e-6 | −1.460e-6 | 1.363 | 27.6 | 1.499 | 2.197 | 3.696 | 0.637 | 0.863 |
| 10 | 2 | [66.3789, 66.6888] | 20.1 | −9.348e-7 | −7.016e-7 | 1.332 | 39.4 | 1.534 | 2.105 | 3.639 | 0.668 | 0.832 |
| 10 | 3 | [66.6888, 66.9286] | 26.0 | −5.567e-7 | −4.740e-7 | 1.174 | 43.5 | 1.532 | 1.709 | 3.241 | 0.826 | 0.674 |
| 10 | 4 | [66.9286, 67.1246] | 31.8 | −4.126e-7 | −3.545e-7 | 1.164 | 45.7 | 1.338 | 1.388 | 2.726 | 0.836 | 0.664 |
| 10 | 5 | [67.1246, 67.2913] | 37.4 | −3.091e-7 | −2.547e-7 | 1.213 | 52.6 | 1.091 | 1.308 | 2.399 | 0.787 | 0.713 |
| 10 | 6 | [67.2913, 67.4377] | 42.9 | −1.922e-7 | −1.713e-7 | 1.122 | 68.2 | 1.108 | 1.462 | 2.570 | 0.878 | 0.622 |
| 10 | 7 | [67.4377, 67.5664] | 48.3 | −1.928e-7 | −1.845e-7 | 1.045 | 55.4 | 1.140 | 1.030 | 2.170 | 0.955 | 0.545 |
| 10 | 8 | [67.5664, 67.6832] | 53.5 | −1.472e-7 | −1.217e-7 | 1.210 | 74.7 | 0.956 | 1.223 | 2.178 | 0.790 | 0.710 |

- **Ruling 9's checks**, at β = 5 (and the same at β = 10):
  - δ₁ × −1 fails all eight oscillations by the sign;
  - **δ₁ × 2: not catchable**, failing none (moved 0.81–1.02 against a bound of 1.86–3.65);
  - **δ₁ × ½: not catchable**, failing none (0.48–0.70 against the same).
- **β = 25 and M = 10⁻³, β = 1.2, pointwise.** After the release the tracker's m/H stays below 10.

  | M = 0.5, β = 25: N | m/H | (φ − φ_min)/φ_min | δ₁/φ_min |
  |---|---|---|---|
  | 145.7793 | 10 | 0 | 4.64e-4 |
  | 146.0793 | 2.84 | 1.90e-2 | −0.541 |
  | 146.3793 | 2.60 | 4.80e-2 | −3.20e-2 |
  | 146.6793 | 2.93 | 5.67e-2 | −1.06e-2 |
  | 146.9793 | 3.33 | 3.89e-2 | −9.67e-3 |
  | 147.2793 | 3.76 | 1.28e-3 | −7.82e-3 |
  | 147.5793 | 4.25 | −2.79e-2 | −6.29e-3 |
  | 147.8793 | 4.85 | −6.73e-3 | −4.95e-3 |
  | 148.1793 | 5.55 | 1.38e-2 | −3.92e-3 |
  | 148.4793 | 6.28 | −1.84e-3 | −3.17e-3 |
  | 148.7793 | 7.12 | −1.06e-2 | −2.54e-3 |

  | M = 10⁻³, β = 1.2: N | m/H | (φ − φ_min)/φ_min | δ₁/φ_min |
  |---|---|---|---|
  | 45.9102 | 10 | 0 | −5.59e-2 |
  | 45.9222 | 9.82 | −3.95e-4 | −5.83e-2 |
  | 45.9463 | 9.45 | −3.39e-3 | −6.33e-2 |
  | 45.9703 | 9.10 | −8.92e-3 | −6.88e-2 |
  | 45.9943 | 8.75 | −1.64e-2 | −7.48e-2 |
  | 46.0184 | 8.42 | −2.54e-2 | −8.15e-2 |
  | 46.0304 | 8.25 | −3.03e-2 | −8.51e-2 |

  δ₁ is not a description of the field there. At M = 10⁻³ the field leaves the tracker towards the
  lag, with the sign of δ₁, but reaches a third of it by T_CMB.

### 5.5 What does not release is 06's (prompt §2.5)

The dispatch commit `35a103c` and the final tree, each in a scratch worktree, compared after
pickling: the records, the samples, the final state, N_final, nfev, accepted and rejected steps,
dropped drains, warnings, `ts`, every segment at both its nodes, the ledger, the fixed-T values and
`first_bounce`.

| History | at the final tree | against `35a103c` |
|---|---|---|
| M = 0.5, β = 1.2 | no parking | **bitwise** |
| M = 0.5, β = 2 | no parking | **bitwise** |
| M = 0.5, β = 2, α = 0 (natural mode) | — | **bitwise** |
| M = 10⁻³, β = 0.5 | no parking | **bitwise** |
| M = 10⁻³, β = 2 | parks at #48, no release | **bitwise** |
| M = 10⁻³, β = 3 | parks at #58, no release | **bitwise** |
| log 05's grid, β = 1.2 × M = 10⁻⁷, 10⁻⁸, 10⁻⁹, 10⁻¹⁰, 10⁻¹², 10⁻³⁰ | park at #6, no release | **bitwise**, all six |
| β = 2 × the same six M | park at #48, no release | **bitwise**, all six |
| β = 3 × the same six M | park at #58, no release | **bitwise**, all six |
| M = 0.5, β = 5, window to N = 40.0 (parked step before the release step) | parked at #105 | **bitwise** |
| M = 0.5, β = 10, window to N = 65.0 | parked at #99 | **bitwise** |
| M = 0.5, β = 25, window to N = 145.5 | parked at #83 | **bitwise** |
| M = 10⁻³, β = 1.2, window to N = 45.5 | parked at #6 | **bitwise** |

**The released histories against the dispatch's failure reason** (ruling 10). `35a103c` fails each
with "the parked tracker lost adiabaticity: m_eff/H_E = … at N = …". The record's `N_check` and
`m_eff_over_H_check`, formatted `.10g` and `.5g`, equal the reason's, as printed:

| History | dispatch: N, m/H | record |
|---|---|---|
| M = 0.5, β = 5 | 40.29089006, 8.9373 | equal |
| M = 0.5, β = 10 | 65.34552793, 9.5492 | equal |
| M = 0.5, β = 25 | 145.7894436, 9.1946 | equal |
| M = 10⁻³, β = 1.2 | 45.95242158, 9.364 | equal |

### 5.6 The tuning (prompt §2.6)

`tune_history` at α = 0.1, `main.py`'s defaults otherwise (tolerance 10⁻⁵, at most 30 integrations),
`progress="every"`, run one at a time:

| History | converged | integrations | final residual_H, residual_φ | where each integration parked and was released | warnings | wall |
|---|---|---|---|---|---|---|
| M = 10⁻³, β = 1.2 | yes | 4 (seed, plain, secant, secant) | −5.54×10⁻⁶, 3.20×10⁻⁶ | all at #6, N = 28.37178–28.37191 (11.467 keV); released at N = 45.8856–45.9102 (0.266–0.273 meV) | "nearly elastic" (R = 10^−49.665) | 6.3 s |
| M = 0.5, β = 5 | yes | 20 (3 guard steps) | 7.45×10⁻⁷, −8.44×10⁻⁶ | every integration parks (at #105–#115, N = 37.733–38.004, 81.2–104.0 MeV) and is released (N = 40.2545–40.3399, 9.01–9.75 MeV); the last at #110, 94.3 MeV, released at 9.378 MeV | "nearly elastic" (R = 10^−53.351) | 121.5 s |
| M = 0.5, β = 5, ε = 0 | yes | 20 (3 guard steps) | −7.92×10⁻⁸, −1.58×10⁻⁷ | never parks | "nearly elastic" | 135.5 s |

The per-integration residuals (H/H₀ − 1, φ_ref/φ_min − 1):
- M = 10⁻³, β = 1.2: (−2.12×10⁻², −1.87×10⁻³), (−5.53×10⁻⁴, −3.00×10⁻²), (+4.67×10⁻⁴,
  −2.35×10⁻³), (−5.54×10⁻⁶, +3.20×10⁻⁶).
- M = 0.5, β = 5: +1.26, +0.876, +0.535, −0.151, −4.56×10⁻², +4.67×10⁻², +4.51×10⁻³, +1.49×10⁻²,
  +0.810, −0.141, −4.81×10⁻², −1.18×10⁻², −7.29×10⁻³, +1.44×10⁻², −4.40×10⁻³, −9.86×10⁻⁴,
  −1.54×10⁻⁶, −1.19×10⁻⁵, −3.06×10⁻⁶, +7.45×10⁻⁷ in H. In φ: −1.59×10⁻², +4.66×10⁻², +0.103,
  +0.344, +0.220, +6.36×10⁻², +9.29×10⁻², +4.53×10⁻², −2.71, −0.307, −0.221, −9.03×10⁻², −3.24×10⁻²,
  +5.90×10⁻³, −7.31×10⁻³, −1.67×10⁻³, −7.03×10⁻⁵, −3.00×10⁻⁵, −2.49×10⁻⁵, −8.44×10⁻⁶.
- M = 0.5, β = 5, ε = 0: the same path to its eighth integration, then +0.804, −0.141, −4.78×10⁻²,
  −1.22×10⁻², −6.58×10⁻³, +9.04×10⁻³, −3.93×10⁻³, −9.44×10⁻⁴, −3.02×10⁻⁵, −1.67×10⁻⁵, +1.61×10⁻⁵,
  −7.92×10⁻⁸ in H.

"Converged" is: no failure, and both residuals below the tolerance 10⁻⁵ (`tuning_iterations` 4, 20
and 20). The full lines are in `agent07/meas/tune.out`. The tuning converges on released histories,
in as many integrations as on the never-parked one. The probe's β = 5 run, with its release at the
default brentq tolerance, took 21; the paths agree to about four digits for the first eight
integrations.

## 6. The deliberate-breakage record (P11)

- **The replay.** `make_breakages.py` wrote each diff against the final files (difflib).
  `replay.sh`, run in bash, checked each with `git apply --check`, applied it, checked it with
  `git apply -R --check`, ran `test_release` and `test_parking` (53 tests), reverted it, and
  compared the files' md5 with the final ones. All were restored. Both scripts are in
  `agent07/breakages/`; none is committed.
- **The final replay** was against the final test files, after D9. B6 and B7 were also replayed
  before D9, when B6 errored the item-2 class and B7 errored its test.

**B1 — the release at the step end where the check failed, not at the root** (items 2, 4). Fails 7:
item 2's `it_parks_at_105…` (N_rel is not near 40.25454) and `m_over_H…`; item 5's
`candidates_after_the_release_drain` (a different history after it); item 7; item 8's T_final and
N_stop inside the step; ruling 7's test. Item 4's continuity still holds under B1: the state handed
back is still the segment's at the node it chose.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -2198,15 +2198,7 @@
                         f"(m_eff/H_E - {PARKING_ADIABATICITY_THRESHOLD:.3g} = {g_left:.5g}, "
                         f"{g_check:.5g} on the segment; {m_eff_over_H_check:.5g} at the check)"
                     )
-                N_rel: float = float(
-                    brentq(
-                        g,
-                        N_left,
-                        N_check,
-                        xtol=RELEASE_ROOT_XTOL,
-                        rtol=RELEASE_ROOT_RTOL,
-                    )
-                )
+                N_rel: float = float(N_check)
                 ts[-1] = N_rel
             else:
                 N_rel = N_left
```

**B2 — π = 0 in the released state** (item 4). Fails 8, among them `test_4_continuity_at_the_release`,
the continuity in items 7 and 8 and ruling 7, and the lag.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -2429,7 +2429,9 @@
         infall_assessment = None
         running_max = None
         boundary_N = float(end_.N)
-        return new_solver(float(end_.N), np.array(end_.state, dtype=float))
+        y_ = np.array(end_.state, dtype=float)
+        y_[1] = 0.0
+        return new_solver(float(end_.N), y_)
 
     try:
         while True:
```

**B3 — a second parking allowed after the release** (item 5). Fails 9 and errors 2, among them
`test_5_no_ratio_test_and_no_tracker_after_the_release`, `test_5_the_candidates_after_the_release_drain`
and item 2's tests, and `test_parking`'s item-9 test. The history parks again at #106, the first
candidate after the release, which overwrites the `Parking` record (item 2: "106 != 105").
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -2356,7 +2356,7 @@
         ratio: float = exp(record_.log_rho_kin - float(y_post[2]))
 
         tracker: Optional[CutTracker] = None
-        if release is None and parking_ratio_test(index, ratio):
+        if parking_ratio_test(index, ratio):
             tracker = cut_tracker(policy, hubble, N_, y_post)
             if tracker_adiabatic(tracker.m_eff_over_H):
                 delta_K, delta_rho_over_rho_rad = parking_leftover(
```

**B4 — the gate's half-oscillation not reset at the release** (item 5, ruling 6). Fails
`test_5_the_first_turning_point_and_candidate_after_the_release`: the first turning point after the
release carries the sentinel. Nothing else fails: B4 does not show on the unpatched history (§1.3).
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -2427,8 +2427,6 @@
         armed = None
         armed_assessment = None
         infall_assessment = None
-        running_max = None
-        boundary_N = float(end_.N)
         return new_solver(float(end_.N), np.array(end_.state, dtype=float))
 
     try:
```

**B5 — the release skipped on the step that ends the history** (item 8). Fails
`test_8_a_T_final_inside_the_release_step_releases_and_ends_oscillating`: it ends parked, below the
threshold.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -2293,7 +2293,7 @@
                     interpolants.append(segment)
                 y_end = guarded(segment, N_end)
                 m_failed = check(N_end, y_end)
-                if m_failed is not None:
+                if False and m_failed is not None:
                     # the release comes first, and the oscillating phase ends the history (prompt 07
                     # §1.1 item 5)
                     released_ = release_on_step(segment, appended, N_end, m_failed)
```

**B6 — 06's failure restored in place of the release** (items 2, 7, 9). Fails
`test_2_it_reaches_T_CMB_and_integrate_history_succeeds`, item 7's test and `test_parking`'s item-9
test, and errors the rest of the β = 5 tests, whose history has no result (13 errors).
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -2158,7 +2158,11 @@
             """
             m_eff_over_H_, _ = guarded(tracker_m_eff_over_H, policy, hubble, N_, y_)
             if not tracker_adiabatic(m_eff_over_H_):
-                return m_eff_over_H_
+                raise failure(
+                    f"the parked tracker lost adiabaticity: m_eff/H_E = {m_eff_over_H_:.5g} < "
+                    f"{PARKING_ADIABATICITY_THRESHOLD:.3g} at N = {N_:.10g}, "
+                    f"T_J = {exp(y_[4]) / MeV:.5g} MeV"
+                )
             fold_minimum(m_eff_over_H_)
             return None
 
```

**B7 — the reflection branch's hand-back broken: the release taken as the end of the history**
(ruling 7). Fails `test_R7_parked_at_a_reflection_released_and_handed_back` (N_final is N_rel, not
N_stop).
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -2507,7 +2507,7 @@
                 if assess_wall:
                     parked = cut(N, y_reflected, kind, pi_)
                     if parked is not None:
-                        if parked.released:
+                        if False:
                             # the release (prompt 07): the loop resumes from the released state,
                             # not from y_reflected
                             solver = resume_after_release(parked)
```

**C1 — ruling 9's check δ₁ × −1** (test-only). Fails `test_6_the_lag_after_the_release_against_delta_1`
on all eight oscillations, by the sign. **δ₁ × 2 and × ½ are not catchable**: `lag_failures` finds no
oscillation failing either, the bound being 1.86–3.65 |δ₁| against moves of 0.81–1.02 and
0.48–0.70 |δ₁| (§5.4). They are not replayed as diffs.
```diff
--- a/ComputeTargets/tests/test_release.py
+++ b/ComputeTargets/tests/test_release.py
@@ -331,7 +331,7 @@
         F[j] = F0
         F_phi[j] = (Fp - Fm) / (2.0 * h)
         F_phiphi[j] = (Fp - 2.0 * F0 + Fm) / (h * h)
-    delta1 = delta1_scale * (d_pi_min - F) / F_phi
+    delta1 = -delta1_scale * (d_pi_min - F) / F_phi
     x = phi - phi_min
 
     rows = []
```

## 7. Observations not acted on

- **The anharmonic shift that the lag's bound allows does not appear in the data.**
  - For an oscillation of half-swing S in a potential with F_φφ/F_φ ≈ −8 700 /M_P, the cubic term
    shifts the time-averaged x outwards by |F_φφ/F_φ|S²/4. That is 0.9–2.2 |δ₁| at β = 5 and 1.0–2.2
    at β = 10, and the remainder ⟨R⟩/F_φ computed from the right-hand side agrees with it.
  - The measured ⟨φ − φ_min⟩ is δ₁ to within 0.20 |δ₁| (β = 5) and 0.36 (β = 10), and inward, not
    outward.
  - In the integrated force balance over one oscillation the anharmonic remainder is cancelled by the
    correlation between the swing and F_φ ∝ (m/H)², which grows by up to 2.6× over one oscillation. I
    cannot derive that cancellation.
  - So item 6 tests the lag's sign and order only (ruling 9 (c)), and δ₁ × 2 and × ½ are not caught.
    No issue opened (the user's ruling).
- **Prompt §0.3 and §1.6 item 7, log 06 §5.3 and §8, `.documents/architecture-summary.md`'s 06 note
  and `numerical-strategies.md` §6.5** give the M = 10⁻³, β = 1.2 release (06's failure) as 0.27 eV
  and 0.25 eV. It is 2.66×10⁻¹⁰ MeV = 0.27 meV (0.255 meV for 06's failing check), just above
  T_CMB = 0.235 meV. The new notes say so; the old text is left as it was (`CLAUDE.md` invariant 6).
- **After the release the tracker's m/H dips again.** At β = 5, 10 and 25 it falls to 1.2–2.3, 0.5–6
  e-folds after the release (N = 46.6, 71.5 and 146.2), and is back above 10 by N = 51.4, 75.7 and
  149.6. No candidate falls in the dip.
  - Without P24, the β = 5 and 10 histories would have met the cut's conditions again within an
    e-fold of their release (§5.2).
  - For 08: the late tracker passes through m/H ≈ 1 at these M.
- **`steps_after` at the release step.** On the step that ends the history (T_final inside the
  release step) the parked step and the oscillating steps after the release are all counted in
  `accepted_steps`, as at 06. The parked step is counted in `Parking.parked_steps`, not in
  `steps_after`.
- **D3's bracket failure** cannot arise on a real history unless m/H at a step end lies within
  rounding of 10. A NaN m/H would now give D3's text, not 06's.

## 8. State handed to the next prompt

- **The release record** (`ComputeTargets/ScalarHistory.py`), `Release`, on `IntegrationResult.release`
  and the payload's `"release"` (not stored); `None` if the history was not released:
  - `N`, `log_T_Jordan`: the release N_rel (the root of m_eff/H_E = 10 on the release step's segment),
    and ln T_J there. `N` is the node in `solution.ts` between the last `ParkedSegment` and the first
    Radau segment;
  - `phi_Einstein`, `pi_Einstein`: the tracker's (φ_min, π_min) at N_rel, the released state's field;
    the dense output at N_rel returns them bitwise;
  - `m_eff_over_H`: m_eff/H_E at N_rel, the threshold to about 10⁻¹¹ (log 07 §1.2);
  - `N_check`, `m_eff_over_H_check`: the parked step end (or N_final) whose check failed, and its m/H;
  - `H_Einstein`: H_E at N_rel, the same either side;
  - `candidate_index`: the index in `injections` of the first candidate after the release (=
    `len(injections)` if none follows);
  - `steps_after`: the accepted steps after the release, all oscillating.
- **`Parking`** for a released history: `parked_steps` counts the parked steps through the release
  step; `min_m_eff_over_H` is the minimum over the switch, the passing checks and the release (the
  root's value, the smallest on the four histories).
- **How a reader tells the phases apart:**
  - the oscillating phase before the switch: segments up to `Parking.N`, candidates
    0…`Parking.candidate_index`;
  - the parked phase: `ParkedSegment`s from `Parking.N` to `Release.N` (or to N_final if not released),
    no turning points or candidates;
  - the release: `Release.N`;
  - after it: Radau segments from `Release.N`; turning points with N > `Release.N`; the generations
    `injections[Release.candidate_index:]`, every one an ordinary candidate (drain point or
    reflection) with its A_k from samples after the release only.

  The ledger gains nothing at the release. The leftover's entry is at `Parking.N`, after the
  candidate's own.
- **The four released histories** (α = 0.1, seed, untuned):
  - M = 0.5, β = 5: parks #105, N = 38.0044 (81.2 MeV); released N = 40.25454 (9.745 MeV);
    15 generations after;
  - M = 0.5, β = 10: parks #99, N = 61.0959 (358 MeV); released N = 65.32727 (8.695 MeV); 19 after;
  - M = 0.5, β = 25: parks #83, N = 132.5508 (9.97 GeV); released N = 145.77929 (40.3 keV); 1 after;
  - M = 10⁻³, β = 1.2: parks #6, N = 28.3719 (11.5 keV); released N = 45.91020 (0.266 meV); none after.

  No history at M ≤ 10⁻⁷ M_P, nor M = 10⁻³ at β = 0.5, 2, 3, releases.
- **The cost**: the root 0.4–0.7 ms and a new Radau instance 0.2 ms, once per released history. A
  released history is cheaper than the never-parked one (5.4 against 6.3 s at β = 5; 1.6 against
  5.9 s at M = 10⁻³). The β = 2 history is unchanged (2.58 against 2.60 s). The tuning converges on
  released histories (4 integrations at M = 10⁻³, β = 1.2; 20 at M = 0.5, β = 5).
- **The constants**: `RELEASE_ROOT_XTOL = 1e-14`, `RELEASE_ROOT_RTOL = 4ε`, neither a key; the release
  threshold is `PARKING_ADIABATICITY_THRESHOLD` (10).
- **Issues**: opened none. Narrowed `[00-settling-at-physical-M-needs-a-parked-tracking-model]`
  (`integrator-remediation` board, this board §3.1, `.documents/OPEN_ISSUES.md` §1.20). The switch,
  the tracker and the release are built and measured; what remains is Q on the parked samples,
  09's.
- **Tests**: `ComputeTargets/tests/test_release.py` (19); `test_parking`'s item-7 test at β = 5 now
  asserts the release.
- **Counts after this prompt:** CosmologyModels 26, ComputeTargets 427 (96 + 331), ObjectFactories
  103, tools 115, RunDrivers 64. `CLAUDE.md`'s counts are still those of `tuning-followups` 02; this
  campaign's close (12) updates them.
