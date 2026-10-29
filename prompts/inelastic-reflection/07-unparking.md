# Prompt 07 — unparking

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-29, at the user's word ("Please write prompt 07"), after 06 landed (`e967cb7`) and was
reviewed (`7b75ae7`). It is written from:
- log 06 §8 ("State handed to the next prompt");
- README §8.4's charter for 07;
- the orchestrator's probes of the same day
  ([`source/planning-probes-2026-10-29d/`](source/planning-probes-2026-10-29d/README.md)).

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.
**Gate:**
- on branch `inelastic-reflection`, at or after the commit that adds this prompt, with no tracked change in
  `git status`;
- no ChamPBH Ray process is running. You start none.

**Closes:** nothing. It narrows `[00-settling-at-physical-M-needs-a-parked-tracking-model]` (§0.3).
**Opens:** only what the work finds.
**Recommended model:** **Opus**. It makes the parked phase end in a release that hands the history back
to the oscillating loop, and every history that does not release must stay bitwise 06's.

**Read first:**

1. [`README.md`](README.md):
   - **§8** in full, in particular:
     - R14 (the cut), R15 (the parked field), R16 (unparking) and R17 (parking at large M);
     - P18 (the constants), P21 (the guard), P22 (natural mode is the old code) and P24 (one park, one
       release);
     - §8.4's charters for 07 and 08;
     - §8.5 ("06 → 07", "07 → 08");
   - §2.1 (one prompt at a time) and §5 (the rules).
2. [`logs/06-parking.md`](logs/06-parking.md):
   - §8;
   - §1.1 (the order at a candidate);
   - §1.3 (the parked phase, its determinism, its end and its failures);
   - §5.2 and §5.3 (the histories that lose adiabaticity);
   - §7.
3. `ComputeTargets/ScalarHistory.py`:
   - the records `TurningPoint` (:226), `Injection` (:277), `Parking` (:324) and `IntegrationResult`
     (:375);
   - in `integrate_scalar_history`:
     - `close_half_oscillation` (:1621), and the gate's state that it closes;
     - `run_parked_phase` (:2020), in particular `check` (:2062) and the two ends of a parked step;
     - `cut` (:2165);
     - the two places the loop calls it, after a reflection (:2314) and at a drain point (:2452);
   - `integrate_history`'s payload (:3159).
4. `ComputeTargets/tests/test_parking.py`: the module docstring's item (7), and `TestAtHalfThePlanckMass`
   (:854), whose β = 5 test (:906) this prompt changes (§1.6 item 9).
5. The orchestrator's probes
   [`source/planning-probes-2026-10-29d/`](source/planning-probes-2026-10-29d/README.md). They are
   references, not code to copy: `release_patch.py` is a string-replaced copy of the loop.

---

## 0. Where this prompt sits

### 0.1 What it is for

In 06 a parked tracker that stops being adiabatic fails the history: at the end of a parked step,
m_eff/H_E < 10 raises. The user ruled (R16) that the field is released there instead. P24 adds that a
released field is integrated in full to T_CMB, draining at its candidates, and never parks again. Four
histories of log 06 fail this way, all at large M, where dissipation mode is "unofficial" (R17):
- M = 0.5 M_P at β = 5, 10 and 25;
- M = 10⁻³ M_P at β = 1.2.

At M ≤ 10⁻⁷ no history releases: the smallest m/H over a parked phase is 72 (log 06 §5.4). This
prompt therefore changes nothing in the laboratory-M histories. What it does is let every history that
parks reach T_CMB.

### 0.2 What the orchestrator's probes found (`7b75ae7`, α = 0.1)

A copy of the loop with a release was run on the four histories (`release_patch.py`). The release was at
the root of m_eff/H_E = 10 on the parked step's segment. It was followed by a new Radau instance from the
segment's state there, with the gate's half-oscillation and the infall reset, and no second parking.
"Never parked" is 06's history with ε patched to 0.0.

| History | parks at | released at | after the release | steps, wall (never parked) |
|---|---|---|---|---|
| M = 0.5, β = 5 | #105, 81.2 MeV | N = 40.254538, 9.745 MeV | 15 drain points; m/H rises to 62 within 3 e-folds | 8 310, 5.4 s (9 270, 7.7 s) |
| M = 0.5, β = 10 | #99, 358 MeV | N = 65.327267, 8.696 MeV | 19 drain points; m/H rises to 91 within 3 e-folds | 8 473, 7.1 s (11 996, 9.9 s) |
| M = 0.5, β = 25 | #83, 9.97 GeV | N = 145.779286, 40.3 keV | 1 drain point; m/H falls to 2.6, and is 7.1 at +3 e-folds | 8 246, 7.6 s (17 232, 13.8 s) |
| M = 10⁻³, β = 1.2 | #6, 11.5 keV | N = 45.910203, 0.266 eV | no turning point; m/H falls to 8.25 at T_CMB | 2 044, 2.1 s (10 598, 7.1 s) |

What follows from it:
- **Every released history reaches T_CMB**, faster than the never-parked one.
  - After the release no candidate is a reflection, so the guard never fires.
  - The release falls inside the parked step on which 06's history failed. At β = 5 it is at
    N = 40.25454, log 06 §5.2's crossing of the parked history, against the step end at 40.29089
    (m/H = 8.94) where 06 failed.
  - M = 10⁻³ at β = 0.5 never parks, and is unchanged.
- **The release is continuous** (`continuity.out`):
  - the last parked segment and the first Radau segment return the same floats at N_rel;
  - the dense output there returns the Radau segment's;
  - m/H at the root is 10 + 1.0×10⁻¹²;
  - one evaluation of m/H on a segment costs 75–83 µs, so the root costs about 1 ms.
- **P24 is the ruling, and it binds at β = 5 and 10.**
  - After the release the field oscillates about the tracker with ρ_kin/ρ_R ≈ 10⁻⁷, below ε, at every
    later drain point.
  - The tracker's m/H climbs back above 10 within about an e-fold.
  - So the cut's two conditions would hold again, and under P24 the field is not parked again.
  - Not re-parking costs little: the released history takes fewer steps than the never-parked one.
  - 07 measures how many post-release candidates would have passed both conditions (§2.2), and documents
    it.
- **The lag after the release** (`lag.out`). δ₁ is the first-order lag: the code's own π′ equation,
  linearised about the zero-lag tracker (`lag.py`).
  - The field is released at zero lag, so it oscillates about the lagged tracker φ_min + δ₁. The
    oscillation is not small: its half-swing is 23–75 |δ₁|, because m/H falls below 10 just after the
    release.
  - The lag is therefore compared per oscillation: the mean of φ − φ_min from one wall-side turning point
    to the next, against the mean of δ₁. Over the first eight oscillations after the release, the ratio is:
    - 0.99–1.19 at β = 5, where m/H goes from 12 to 50;
    - 0.99–1.36 at β = 10, where m/H goes from 14 to 54.

    It approaches 1 as m/H grows. The first-order error, (H/m)², is far smaller than these spreads.
    What limits the agreement is that each oscillation does not close.
  - At β = 25 and at M = 10⁻³, β = 1.2, m/H stays below 10, and δ₁ is not a description of the field.
- **Released against never parked** (`compare.out`), at the four histories in the order of the table:

  | | β = 5 | β = 10 | β = 25 | M = 10⁻³, β = 1.2 |
  |---|---|---|---|---|
  | N_final, released less never parked | −8.9×10⁻⁷ | −3.5×10⁻⁶ | −1.1×10⁻⁵ | −1.2×10⁻³ |
  | Σ Δρ/ρ_R, leftover included, relative | +4.8×10⁻⁴ | +7.1×10⁻⁴ | +2.1×10⁻³ | +5.6×10⁻⁶ |
  | ln T_J, largest difference after the switch | 2.0×10⁻⁴ | 2.3×10⁻⁴ | 1.0×10⁻² | 1.7×10⁻³ |

  - Each largest difference is set before the release, by the parking itself (R17).
  - The released histories drain more than the never-parked ones. The leftover is all of ρ_kin at the
    cut, while in the oscillation that the cut ended Hubble friction takes most of it.
- **The tuning converges on released histories** (`tune.out`, `main.py`'s defaults, α = 0.1):
  - M = 10⁻³, β = 1.2: in 4 integrations, 6.7 s;
  - M = 0.5, β = 5: in 21 integrations, 131 s, three of them guard steps. Every integration parks and
    is released; the release moves from 9.75 to 9.38 MeV as the normalization moves.

### 0.3 Departures from README §8.4's charter, and why

- **The release is at the root of m_eff/H_E = 10 on the parked step**, not at the step end where 06's
  check finds it below 10. The charter says only "at m/H < 10".
  - Why: the release point is then independent of the parked step grid (steps of up to 0.1 e-folds),
    and it is m/H = 10 to the root's tolerance.
  - The cost is about 1 ms per history (§0.2).
- **The test histories.** The charter names M = 10⁻³, "where the probe put the loss of adiabaticity near
  30–40 keV". That was β = 0.5, which never parks (log 06 §5.3). The tests therefore use:
  - M = 0.5, β = 5, released at 9.75 MeV, with 15 drains after the release;
  - M = 10⁻³, β = 1.2, released at 0.27 eV, with no turning point after it.
- **The lag is compared per oscillation**, not pointwise. The field is released at zero lag and
  oscillates about the lagged tracker (§0.2).
- **One existing test changes.** `test_parking`'s item-7 test at β = 5 asserts 06's failure. That
  failure is what this prompt replaces (§1.6 item 9).
- **`[00-settling-at-physical-M-needs-a-parked-tracking-model]` is narrowed, not closed.**
  - README §8.6 has 06 and 07 close it. The issue's own next step also names "what the parked field
    contributes to … the adiabatic diagnostic", and R15 gives that to 09 (`AdiabaticHistory` computes Q
    on parked samples).
  - So 07 narrows it, with the switch, the tracker and the release built, and 09 closes it.

---

## 1. What is wanted

### 1.1 The release (R16)

In the parked phase, at α > 0 on a model with a wall (only there does a history park):
1. **The check** at each parked step end (and at N_final on the step that ends the history) is 06's.
   Where `tracker_adiabatic` is False it no longer raises: the field is released.
2. **The release point** is the root of m_eff/H_E − `PARKING_ADIABATICITY_THRESHOLD` on that step's
   `ParkedSegment`, between the step's left node (where the check, or the switch, gave m/H ≥ the
   threshold) and the point the check failed at.
   - m_eff/H_E is `tracker_m_eff_over_H` at the segment's state, with the ledger as it stands.
   - The root finder and its tolerance are yours. Derive in the log the tolerance this gives on m/H at
     the root.
   - If the left node itself has m/H equal to the threshold, the release is there, and no zero-length
     segment is appended.
3. **The dense output.** The step's segment stays as it is. Its node in `ts` is N_rel, not the step's end,
   so the history's `OdeSolution` reads the parked phase up to N_rel.
4. **The state** at the release is the segment's at N_rel: (φ_min, π_min, ln ρ_R,E, ln f_m, ln T_J).
   Nothing is drained, and nothing is added to the ledger.
5. **The oscillating loop resumes** from that state at N_rel with a new Radau instance, as after a drain.
   Its own end, step budget, failsafe and failures apply. When a parked step ends the history (ln T_J <
   log_T_stop, or N_stop):
   - if m/H at N_final is at least the threshold, the history ends there, as in 06;
   - otherwise the release on that step comes first, and the oscillating phase ends the history.
6. **The state the loop carries** is reset at the release:
   - no drain is armed (already true at the switch);
   - no infall assessment;
   - the gate's half-oscillation begins at N_rel, with no sample. The samples before the switch never
     reach a candidate after the release.

   The warnings already made stand.
7. **Never again (P24).** After a release no candidate makes the ratio test or calls the tracker. The
   guard (P21) still runs at every reflection. Its reason says that the field was released (at what N),
   in place of 06's "not computed (the ratio test did not pass)".

The release is made in the parked phase and handed back to the loop. How it is passed is yours (a return
value, a record), but it must not be an exception that the loop's `try` sees as a failure. Every reason a
parked phase can still fail with keeps 06's text.

**Natural mode** (α = 0) runs none of this (P22).

### 1.2 The records

Append new fields last, with comments, as 06 did.
- **`IntegrationResult`** gains the release record, `None` if the history was not released. It holds:
  - N and ln T_J at the release;
  - φ and π there (the tracker's);
  - m_eff/H_E at the release, the root's value;
  - m_eff/H_E at the point where the check failed, and its N;
  - H_E at the release;
  - the index in `injections` of the first candidate after the release (= the number of candidates
    before it);
  - the accepted steps after the release.

  The names are yours. 08 needs to tell the generations after a release from those before, and 09 stores
  the N and T_J of release.
- **`Parking`** keeps its fields. For a released history:
  - `parked_steps` counts the parked steps up to and including the one the release falls in;
  - `min_m_eff_over_H` is the smallest of the switch's value, the checks before the release, and the
    release's own. The check that failed lies after the release, and is not part of the parked phase.

  Say so in their comments.
- **The payload** of `integrate_history` gains the record under a new key, `"release"`, which is not
  stored.
- **Unchanged:** `ScalarHistory.store`, `build_extra_data`, `golden_history.record_of`,
  `TUNING_OUTPUT_KEYS`, `"reflections"`, `"parking"`, and every existing record's fields.

### 1.3 What does not change

- The cut, the tracker, the parked right-hand side and the segments are 06's:
  - `cut_tracker`, `parked_tracker`, `tracker_velocity_terms`, `tracker_m_eff_over_H`,
    `parking_leftover`, `ParkedRHS`, `ParkedSegment`;
  - the constants.
- A history that parks and stays adiabatic is bitwise 06's.
- A history that never parks is bitwise 06's.

### 1.4 The tuning

`tune_history` is not changed. Its behaviour on released histories is measured (§2.6).

### 1.5 The documents (P12)

- **`.documents/architecture-summary.md`:** a dated note beside 06's, covering the release, its record
  and the payload key.
- **`.documents/numerical-strategies.md`:** a dated subsection 6.6 after 06's 6.5, covering:
  - the release point and its root;
  - the hand-back;
  - the reset of the gate's half-oscillation;
  - P24;
  - the lag after the release.
- **Both:** the large-M behaviour of R17 now ends in a release, not a failure. Give the four released
  histories of §2.2, with where each parks and is released. Say that after a release at β = 5 and 10
  the field would satisfy the cut again, and is not parked again (P24).

### 1.6 The tests

A new module, `ComputeTargets/tests/test_release.py`. No store and no Ray. Each test that integrates gives
its cost and the load in its docstring. Share integrations between items where you can.
1. **What does not release is 06's.**
   - The golden tests, `test_first_bounce.py`, `test_surfing_turning_points.py`,
     `test_kinematic_cap_loop.py`, `test_unresolved_turnarounds.py`, `test_injection.py` and
     `test_gate_and_taper.py` pass unedited.
   - So do all of `test_parking.py`'s tests but the one that item 9 below changes.
   - Its item (4) (M = 10⁻³⁰, β = 2) and the first two tests of item (7) (M = 0.5, β = 2) carry 06's
     bitwise claims through this prompt.
2. **The release at M = 0.5 M_P, β = 5, α = 0.1**, to T_CMB:
   - it parks at #105 and is released once, inside the parked step on which 06 failed. Use log 06 §5.2's
     crossing, N = 40.25454 on the parked history, with a slack you derive from the root's tolerance;
   - m_eff/H_E at the release equals the threshold to the tolerance of §1.1 item 2;
   - the record's fields, by construction;
   - it reaches T_CMB, and `integrate_history` succeeds. `first_bounce` comes before the switch, and
     T_J = 1 MeV and 70 keV are located after the release.
3. **Bitwise up to the release.** The same history to an `N_stop` inside the parked phase, before the
   release (06's `TestAWindowEndingInTheParkedPhase` is the model), matches the full history on:
   - the records;
   - `ts`, and every segment's value at its nodes, before the release step.
4. **Continuity at the release.**
   - The last parked segment and the first Radau segment return the same floats at N_rel, and the
     history's dense output there returns them.
   - H_E either side is the same float.
   - The ledger is unchanged across the release.
5. **After the release.**
   - The candidates after it drain: the ledger is the sum of the candidates' and the leftover's ΔK.
   - `cut_tracker` (spied, through the module's name) is called exactly as many times as before the
     release, although later candidates have ρ_kin/ρ_R < ε (P24).
   - The first candidate after the release has its A_k from samples after the release only.
6. **The lag** (§0.2). Over the first oscillations after the release, wall-side turning point to
   wall-side turning point, the mean of φ − φ_min against the mean of δ₁:
   - δ₁ from the right-hand side's π′, linearised about the tracker, as `lag.py` forms it;
   - φ_min from `parked_tracker` on the history's own trace;
   - the tolerance is yours, derived from what limits the agreement (the oscillation's half-swing, and how
     far one oscillation is from closing), not fitted to the numbers. It must fail if δ₁ has the wrong
     sign or is off by a factor of 2.
7. **M = 10⁻³ M_P, β = 1.2, α = 0.1:** it parks at #6, is released near N = 45.91 (0.27 eV), and reaches
   T_CMB with no candidate after the release.
8. **The ends.**
   - `integrate_history` to a T_final between the release's T_J and the T_J at the end of the parked step
     it falls in (β = 5: between 9.745 and 9.396 MeV): the history is released on the step that ends it,
     and ends in the oscillating phase at T_final.
   - To a T_final above the release's T_J: it ends parked, unreleased.
   - An `N_stop` just after the release: released, and it ends at `N_stop`.
9. **The one existing test that changes** (§0.3): `test_parking`'s
   `test_7_at_beta_5_it_fails_where_the_tracker_loses_adiabaticity` becomes the release:
   - the history succeeds;
   - it is released inside the window that test used for the failure;
   - the record names the release.

   `TestAtHalfThePlanckMass`'s docstring and the module docstring's item (7) change to match.
   Nothing else in `test_parking.py` changes. List the edit in the log, with its reason. Any other edit
   to an existing test is a deviation to classify, or a stop (§5).

**Each test is shown to catch a breakage** (P11). Record each breakage as a diff, with the tests it
fails; never commit one.
- B1: the release at the step end where the check failed, not at the root (items 2, 4).
- B2: π = 0 in the released state (item 4).
- B3: a second parking allowed after the release (item 5).
- B4: the gate's half-oscillation not reset at the release (item 5). If no real history shows it,
  make one show it by a patch, and say how.
- B5: the release skipped on the step that ends the history, so that the history ends parked below the
  threshold (item 8).
- B6: 06's failure restored in place of the release (items 2, 7, 9).

---

## 2. Measurement

Recorded as tables in the log. Every history uses `test_injection`'s setup (n = 1, φ* = 5 M_P, π* = 0,
T* = 2×10⁴ GeV, to T_CMB, default tolerances, the model's seed realization, untuned), α = 0.1, unless
stated. No store, no Ray. The dispatch commit's histories are run in a temporary worktree of it, which
is removed afterwards.
1. **The cost.**
   - The release, in ms: the root, and the new Radau instance.
   - Per history, for the four of §2.2: the final tree against the never-parked history (ε = 0).
   - M = 0.5, β = 2 against the dispatch commit (no release).

   Give the minima of three runs, with the load.
2. **The released histories**: M = 0.5 at β ∈ {5, 10, 25}, and M = 10⁻³ at β = 1.2. For each:
   - where it parks;
   - where it is released (N, T_J, m/H at the root), and where 06's check failed (N, m/H);
   - the steps before and after the release;
   - after the release:
     - the turning points, and the candidates with their kinds;
     - Σ Δρ/ρ_R;
     - the reflections, and whether the guard fired;
   - the tracker's smallest m/H after the release, and where it is back above 10, if it is;
   - how many candidates after the release have ρ_kin/ρ_R < ε **and** m/H ≥ 10 on the tracker, that is,
     how many would have parked again but for P24;
   - the outcome and the wall time.
3. **Released against never parked**, for the same four. Give:
   - N_final;
   - Σ Δρ/ρ_R with the leftover;
   - over the window after the switch and after the release: ln T_J, ln ρ_R,E, H_E and H_J, the largest
     difference and the difference at the end;
   - the fixed-temperature values at 1 MeV and 70 keV where both histories reach them.
4. **The lag**:
   - at β = 5 and 10, per oscillation over the first eight after the release: m/H, the two means, their
     ratio, and the half-swing over |δ₁|;
   - at β = 25 and at M = 10⁻³, β = 1.2, pointwise over the window after the release: φ − φ_min and δ₁,
     with m/H.
5. **What does not release is 06's**, against the dispatch commit:
   - every record, the samples and the final state for:
     - M = 0.5 at β ∈ {1.2, 2};
     - M = 10⁻³ at β ∈ {0.5, 2, 3};
     - log 05's grid: β ∈ {1.2, 2, 3} × M ∈ {10⁻⁷, 10⁻⁸, 10⁻⁹, 10⁻¹⁰, 10⁻¹², 10⁻³⁰} M_P;
   - the four released histories up to the release step;
   - natural mode at M = 0.5, β = 2.
6. **The tuning.** `tune_history` with `main.py`'s defaults otherwise:
   - M = 10⁻³, β = 1.2;
   - M = 0.5, β = 5, and the same with ε patched to 0.0, for comparison (each about 2 minutes, §0.2).

   For each give:
   - the integrations, and the residuals per integration;
   - whether it converged;
   - where each integration parked and was released;
   - the warnings;
   - the wall time.

   A tuning that does not converge is a finding, not a stop.

---

## 3. Verification

1. **Before any edit:** the five suites, run as `CLAUDE.md` says, with ComputeTargets in its two halves.
   Expect 26 / 408 (96 + 312) / 103 / 115 / 64 (log 06 §8). A different count, or a failure, is a stop.
2. **After:**
   - the five suites. ComputeTargets rises by the new tests only, and the golden tests pass unchanged;
     say so;
   - `black --check` on the files you changed.
3. **The breakages** of §1.6, each with the tests it fails.

## 4. Acceptance

1. **§1 as written, and the gate "Release"** (README §8.4):
   - α = 0 bitwise;
   - every history that does not release is bitwise 06's;
   - a history that parks is released at m/H = 10 and reaches T_CMB;
   - the state, H_E and the ledger are continuous at the release;
   - no second parking;
   - the lag after the release against δ₁.
2. **The measurement** of §2, as tables in the log.
3. **The log**, `logs/07-unparking.md`, in the template of `dk-and-rpk-migration` README §5.1. It holds:
   - the order in a parked step, with the release;
   - the root and its tolerance on m/H;
   - how the release is handed back to the loop, and what is reset;
   - the derivation of the lag test's tolerance;
   - the suite counts before and after;
   - the breakages as diffs;
   - the edit to `test_parking.py`, with its reason;
   - every deviation, classified;
   - "State handed to the next prompt", for 08:
     - the release record and what each field holds;
     - how a reader tells the parked phase, the release and the generations after it apart;
     - the four released histories, with where each releases;
     - the cost.
4. **The documents** of §1.5.
5. **The board:**
   - row 07 and the gate Release, in §1 and §2;
   - `### 5.8 After prompt 07 (<date>)` in §5;
   - §3 and §4 for anything opened;
   - narrow `[00-settling-at-physical-M-needs-a-parked-tracking-model]` in three places, in the same
     commit:
     - this board's §3.1;
     - its own entry on the `integrator-remediation` board, with a dated "Narrowed" line;
     - `.documents/OPEN_ISSUES.md` §1.20.

     Say that the switch, the tracker and the release are built and measured, and that what remains is
     Q on parked samples, which is 09's (§0.3).
6. **One commit**, in `CLAUDE.md`'s form, staged by explicit path only.

## 5. Stop conditions: stop and ask

- A baseline count differs from §3 item 1, or an existing test fails before you edit.
- Either golden history changes, or any of the existing tests of §1.6 item 1 needs an edit.
- A history of §2.5 is not bitwise 06's: in full where it does not release, and up to the release step
  where it does.
- A history of §2.2 does not reach T_CMB, is not released, or is released outside the parked step on
  which 06's history failed.
- The guard fires after a release on a history of §2.2.
- The state at the release differs from one side of N_rel to the other.
- The work seems to need a change to:
  - 04's model methods;
  - 06's tracker functions, `ParkedRHS` or `ParkedSegment`;
  - the cut's conditions or the constants;
  - the policies, the right-hand side or the ledger;
  - the store.
- An existing test needs an edit other than that of §1.6 item 9.

## 6. What this prompt does not do

- **No second parking** (P24). A released field that would satisfy the cut again is not parked.
- **No peak, no margin**: 08's. The records carry what 08 needs.
- **No store, key or column.** No change to `compute_scalar_history`'s signature, and no change to
  `AdiabaticHistory` (09).
- **No change to:**
  - the cut's conditions or the constants;
  - the model methods or `ExponentialModel`;
  - the policies, `ODERHS` or the ledger;
  - the tuning, the tools or the drivers.
- **Files it changes:**
  - `ComputeTargets/ScalarHistory.py`;
  - `ComputeTargets/tests/test_release.py` (new);
  - `ComputeTargets/tests/test_parking.py`: item 9's test, its class docstring and the module docstring's
    item (7) only;
  - `.documents/architecture-summary.md` and `.documents/numerical-strategies.md`;
  - the log, the board, the `integrator-remediation` board's entry for the issue, and
    `.documents/OPEN_ISSUES.md`.

  Any other file is a deviation to classify.
