# Prompt 05 — unresolved turnarounds

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-29, after 04 landed (`81b933f`) and was reviewed (`97a48d4`), from log 04 §8
("State handed to the next prompt") and README §8.4's charter for 05, at the user's word ("Please
write prompt 05"). If an orchestrator note is written for this prompt at dispatch, it governs where
it differs from this file.
**Gate:**
- on branch `inelastic-reflection`, at or after the commit that adds this prompt, with no tracked
  change in `git status`;
- no ChamPBH Ray process is running. You start none.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Opus**. It adds a second reflection rule to the step loop that every
history is integrated with, must leave α = 0 bitwise, and must leave α > 0 at M = 0.5 M_P bitwise
03's.

**Read first:**

1. [`README.md`](README.md):
   - **§8** in full: R6 (the two modes), R8 (what counts as unresolved), R10 (w_gate = 1 there), P18
     (the constants), P20 (the floor rule is unresolved), P22 (natural mode is the old code), P23
     (the warnings); §8.4's charter for 05;
   - §2.1 (one prompt at a time) and §5 (the rules).
2. [`logs/04-the-wall-through-the-model.md`](logs/04-the-wall-through-the-model.md) §8: the model
   methods, what they return, when they return `None` and when they raise, and their cost.
3. [`logs/03-gate-and-stopping-rule.md`](logs/03-gate-and-stopping-rule.md) §8, and
   [`logs/02-injection-at-turning-points.md`](logs/02-injection-at-turning-points.md) §1 (the drain,
   the floor-rule drain, the restart, the records).
4. `ComputeTargets/ScalarHistory.py`:
   - the records `Reflection`, `TurningPoint`, `Injection`, `IntegrationResult` (:138–250) and their
     comment blocks;
   - `_reflection_guards` (:670), `gate_weight` (:806);
   - `integrate_scalar_history` (:827), in particular `candidate` (:1061), `drain` (:1121), the floor
     branch (:1324–1350), the arming logic (:1427–1449) and the drain point (:1465–1478);
   - `first_bounce` (:1518), `integrate_history`'s payload (:2100–2150), `tune_history` (:2411).
5. `CosmologyConcepts/Models/AbstractModel.py`: `classical_turning_point` and `ClassicalTurningPoint`.
6. The planner's probe
   [`source/planning-probes-2026-10-29b/phi_t_scan.py`](source/planning-probes-2026-10-29b/README.md)
   (§4 of its README), which implemented this rule by editing a copy of the loop, and
   `scan_K_compare.out`, which compares it with the resolved histories. It is a reference, not code
   to copy: it used the kinetic energy alone, a closed-form-free φ_t of its own, and a parking cut
   that this prompt does not have.
7. The tests whose assertions this prompt changes: `ComputeTargets/tests/test_injection.py`
   (`TestAFloorRuleDrain`, item 8) and `ComputeTargets/tests/test_gate_and_taper.py`
   (`TestAFloorRuleCandidate`, item 8).

---

## 0. Where this prompt sits

### 0.1 What it is for

At laboratory M the wall is thinner than Radau can resolve: the probes of 2026-10-29 found failures
on Radau's float spacing at M = 10⁻¹⁰–10⁻¹² M_P, where the classical turning point φ_t is 10⁻¹²–10⁻¹⁴
M_P. The user ruled (R8, "with `K=3` and `phi_t = 1E-8 Mp` as you propose") that in dissipation mode
(α > 0) a turnaround whose classical turning point lies below 10⁻⁸ M_P, or whose test-field ratio 𝓡
exceeds 1, is **reflected at 3 φ_t without being resolved**, and drains with w_gate = 1 (R10). Every
floor-rule reflection is unresolved too (P20). Natural mode (α = 0) is the old code (P22). Two
printed warnings tell the user which regime a history is in (R6, P23).

This prompt does not park the field: 06 does. So at small M a drained history still fails at the
bottom of its cascade, as the probes found; this prompt measures how, for 06.

### 0.2 Departures from README §8.4's charter, and why

- **E is the energy that reaches the wall, ρ_kin + (V − V_floor).** The charter computes φ_t "from
  E = H_E²π²/2". Across the crossing, ρ_kin + V is conserved (expansion is neglected, as 04's methods
  neglect it), and far from the wall V = V_floor, so the E for which V(φ_t) − V_floor = E is the
  kinetic energy far out, E = H_E²π²/2 + (V(φ) − V_floor) at any point of the infall. With the kinetic
  energy alone, φ_t drifts outwards as the field climbs the wall. On the exponential wall with
  E ≫ V₀ the two differ at 3 φ_t by about (V₀/E)^{2/3}, negligible, so the probe's results stand. On a
  gentler wall they do not: on 04's power-law wall (p = 2), V − V_floor is E/9 at 3 φ_t. With this E,
  `classical_turning_point` cannot return `None` on an infalling step (ρ_kin > 0), except by rounding
  (§1.2).
- **The reflection is made at a step's start.** The rule is tested where the floor rule is, before
  each step, so the field is reflected at the first step start with φ < 3 φ_t. The kinematic cap moves
  the field inwards by at most f φ per step (f = 0.1), so the reflection lies in [2.7 φ_t, 3 φ_t), up
  to the change in φ_t between the two step starts.
- **The P1 harness at M = 10⁻¹⁰ keeps a floor-rule reflection.** The charter tests "an unresolved
  reflection at M = 10⁻¹⁰ on the P1 harness, which changes `test_injection` item (8)". There the floor
  rule fires at φ ≈ 10⁻¹¹–10⁻¹⁰ M_P (`test_kinematic_cap_loop.check_reflected`), outside 3 φ_t ≈ 2×10⁻¹²,
  so the reflection stays a floor-rule one. What changes is its weight: P20 makes it unresolved, with
  w_gate = 1. That is the change to item (8), and to `test_gate_and_taper`'s item (8). The 3 φ_t
  reflection itself is tested where it fires first (§1.6).
- **Natural mode evaluates φ_t at most once, for its warning.** P22 says that at α = 0 "no φ_t" is
  computed; P23 asks natural mode to warn when φ_t < 10⁻⁸ M_P at its first wall-side turnaround. Both
  hold if the warning's evaluation changes no value:
  - at a resolved wall-side turning point the field is at rest, so φ_t is φ there and no model call
    is needed;
  - at a floor reflection, which has no turning point, the model's `classical_turning_point` is called
    once, and a failure of that call is caught and turned into a warning. It never fails the history.
- **The natural-mode warning says "may fail", not "expected to fail".** At M = 10⁻⁷ M_P, φ_t ≈ 10⁻⁹
  M_P, below the threshold, yet natural mode resolves it without trouble (README §8.0), and U10 runs
  it as the reference. P23's wording would be wrong there.
- **The warnings are returned by the integrator and printed by the tuning layer.** P23 asks for
  "per history". `tune_history` integrates a history up to 30 times, so a line printed by the step
  loop would repeat. The loop returns them; `tune_history` prints each once per history.
- **An unresolved reflection is a `Reflection`.** It is appended to `reflections`, with a new field
  saying which kind, so that `first_bounce`, the supervisor's count and the payload's `"reflections"`
  see it. At α = 0 every reflection is a floor-rule one, so nothing changes there.

---

## 1. What is wanted

### 1.1 The constants (P18)

Two module constants in `ComputeTargets/ScalarHistory.py`, beside `NONADIABATICITY_SCALE`, each with a
comment citing R8 and P18. Neither is a key:
- the reflection factor K = 3: an unresolved turnaround is reflected as soon as φ < K φ_t;
- the threshold on φ_t, 10⁻⁸ M_P, stored in units of M_P and multiplied by the units' `PlanckMass`
  where it is used.

The names are yours. The rule reads both at call time, so that `unittest.mock.patch` of either changes
it (§1.6 item 5 relies on this).

### 1.2 The assessment of an infall

At α > 0, at the start of every step with π < 0 (the floor rule's test), the loop assesses the wall
ahead:
- **ln E** = ln(H_E²π²/2 + V(φ) − V_floor), formed in logs, with ln(V − V_floor) = −∞ where V rounds
  to V_floor or below. H_E is HubblePolicy's, from the state and the ledger as they stand, through
  `policy` and `hubble`, never through `fun`: `nfev` and the supervisor's records are untouched
  (as 03's samples are).
- **The classical turning point** is `model.classical_turning_point(φ, ln E)` (04).
  - `None` on an infalling step means ρ_kin is below the rounding of V − V_floor: the field is at its
    turning point to rounding. Make no decision at that step; the previous assessment of the infall
    stands. Say in the log how often it happened.
  - A `ComputationFailureError` from the model is a failure of the history, with its reason.
- **A model without a wall** (not `reflects_at_origin`, or no `log_V_floor`) is not assessed: the rule
  never fires, and its records carry no assessment. 02 and 03 run on any model, and must keep doing
  so.

Write the assessment and the decision as module-level functions, called by module name at call time
(as 03's are), so that they can be tested on synthetic values and patched.

### 1.3 The decision and the unresolved reflection

- **Unresolved** (R8): φ < K φ_t, and either φ_t < 10⁻⁸ M_P or ln 𝓡 > 0. All three comparisons are
  strict.
- **Order at a step start**, at α > 0 with π < 0: the assessment; then the unresolved rule; then the
  floor rule. If the unresolved rule fires, the reflection is of kind `"unresolved"`. If it does not
  and the floor rule fires, it is of kind `"floor"`. Either way it is the floor branch as it stands:
  - guards G1 and G2, and `max_wall_to_kinetic_ratio`;
  - a `Reflection` record and `supervisor.notify_reflection`;
  - an armed drain dropped and counted;
  - 03's half-oscillation closed;
  - the candidate drained;
  - the restart from π_out = −√(1 − α_eff) π_in.
- **The weight** (R10, P20): every unresolved reflection, floor-rule ones included, drains with
  w_gate = 1.0 exactly, so α_eff = α. `gate_weight` is not called for it. The infall's 𝒜_k is still
  closed and recorded in `nonadiabaticity`. Drain points after a resolved wall-side turning point keep
  03's gate.
- **Natural mode** (α = 0) runs none of this. No assessment is made, and the floor rule is the old
  code (P22). Natural mode's warning is §1.5's.

### 1.4 The records

Append new fields last, in each namedtuple and in its comment block. The names are yours; the contents
are these.
- **`Reflection`** gains its kind, `"floor"` or `"unresolved"`. At α = 0 every reflection is `"floor"`.
- **`Injection`** gains:
  - whether the candidate was resolved: `True` for a drain point after a wall-side turning point,
    `False` for a floor or unresolved reflection;
  - the assessment it was judged on: φ at the assessment, ln E, and the `ClassicalTurningPoint` (which
    holds φ_t, ln 𝓡, λ_t, ln k_ex and ℓ_t, which 08 needs);
  - its `kind` is `"turning_point"`, `"floor"` or `"unresolved"`.

  **Which assessment a candidate records:**
  - a reflection records the one made at its own step start;
  - a drain point records the last assessment of the infall that ended at its wall-side turning point.
    A wall-side turning point that arms a drain captures it, and a plateau turnaround or a reflection
    starts a new infall.

  A candidate whose infall was never assessed (a model without a wall, or `None` at every step of the
  infall) records `None` for all three.
- **`IntegrationResult`** gains the warnings (§1.5), as a tuple of strings, empty when there are none.
- **The payload** of `integrate_history` gains them under a new key, `"warnings"`. The key is not
  stored. `"reflections"` counts every reflection, of both kinds; correct its comment. At α = 0 the
  count is unchanged.
- **Unchanged:** `ScalarHistory.store`, `build_extra_data`, `golden_history.record_of` and
  `TUNING_OUTPUT_KEYS`.

### 1.5 The warnings (R6, P23)

Each is one line, per history, naming no M: M_crit depends on the potential. The wording is yours,
within what follows.
- **Dissipation mode** (α > 0): at the first candidate of the history (its first impact), if its
  assessment has ln 𝓡 < 0, the line says that the reflections are expected to be nearly elastic in this
  regime, with 𝓡 at the first impact. If the first candidate has no assessment, the line says that the
  regime could not be assessed.
- **Natural mode** (α = 0): at the first of these two events:
  - a wall-side `TurningPoint` with φ < 10⁻⁸ M_P (there φ is φ_t; no model call);
  - a floor-rule reflection whose classical turning point, from the model with §1.2's E, lies below
    10⁻⁸ M_P.

  The line says that the classical turning point lies below 10⁻⁸ M_P, where dissipation mode would
  reflect without resolving it, and that this history may fail on Radau's float spacing or the step
  budget. A failure of the model call at the floor reflection is caught: the line then says that the
  wall could not be assessed, and the history goes on. The call is made at most once per integration.
- **Printing.** `integrate_scalar_history` returns the warnings, and `integrate_history` passes them
  through in its payload, printing nothing. `tune_history` prints each distinct warning once per
  history, from the first integration that returns it, whatever `progress` is. Give the line the
  task label and the prefix the tuning layer uses for lines that must be seen. The tools that call
  `integrate_history` directly are prompt 10's.

### 1.6 The tests

A new module, `ComputeTargets/tests/test_unresolved_turnarounds.py`. No store and no Ray. Each test
that integrates gives its cost and the load in its docstring.
1. **α = 0 is the old code.**
   - The golden tests, `test_first_bounce.py`, `test_surfing_turning_points.py` and
     `test_kinematic_cap_loop.py` pass unedited.
   - Spy on the model's `classical_turning_point` (a `mock` spy on the model's class). It is called
     **zero** times on a golden history at α = 0, and **once** on the P1 harness at M = 10⁻¹⁰ at α = 0,
     for the natural-mode warning at its floor reflection. That run is bitwise `kcl.integrate`'s, as
     `test_injection`'s `test_8_at_alpha_zero_the_run_is_todays` asserts.
2. **The decision, on synthetic values.**
   - Each comparison of §1.3 at its edge: φ = K φ_t, φ_t = 10⁻⁸ M_P and ln 𝓡 = 0 do not fire; the next
     float on the other side does.
   - The assessment's ln E on 04's power-law wall (import `_PowerLawWall` from
     `test_wall_model.py`), at a state where V − V_floor is a known fraction of E, equals
     ln(H_E²π²/2 + V − V_floor) formed by the test, and φ_t is the root for that E.
3. **An unresolved reflection at 3 φ_t.** On a history whose first wall approach is judged
   unresolved before the floor rule fires (the probe suggests M = 10⁻⁸–10⁻⁷ M_P; find one and say
   why it fires first):
   - a `Reflection` of kind `"unresolved"` and an `Injection` of the same kind, `resolved` False;
   - w_gate == 1.0 and α_eff == α exactly;
   - φ at the reflection in [(1 − f) K φ_t, K φ_t), with the φ_t of the record;
   - π_out == −√(1 − α) π_in exactly, and the dense output returns it at the node;
   - the ledger equals the records with α_eff > 0;
   - H_E continuous across it, to 02's bound;
   - `first_bounce` reports the first reflection when it precedes every turning point.
4. **The floor rule is unresolved (P20).** Edit `test_injection`'s item (8) and `test_gate_and_taper`'s
   item (8): the floor reflection is kind `"floor"`, `resolved` False, w_gate == 1.0 and α_eff == α,
   with its A_k still recorded. These are the only edits to existing tests that this prompt
   anticipates. Each is listed in the log with its reason. Any other is a deviation to classify, or a
   stop (§5).
5. **The probe's band.** At M = 10⁻⁹ M_P, β = 2, α = 0.1, on `test_injection`'s setup, run to a window's
   end before the field's ρ_kin/ρ_R falls below 10⁻⁶ (the probes parked there). Compare 05's rule with
   the same history in which the φ_t threshold is patched to 0, so that every wall is resolved
   (𝓡 < 1 there):
   - the number of wall approaches, and of candidates;
   - Σ Δρ/ρ_R over the window, to a tolerance you derive from your measurement (§2.3), with its reason.
     The probes found the drain converged in M to about 10⁻³, and `scan_K_compare.out` gives 0.2664 for
     both at this point, with parking.
6. **M = 0.5 M_P at α = 0.1** (β = 2, as 03's item 3 history): every candidate resolved, with an
   assessment; no reflection; the run bitwise equal to the same run with the decision patched to never
   fire. Share the integration with item 7 where you can.
7. **The warnings.**
   - Dissipation mode at M = 0.5 warns "nearly elastic" once, with 𝓡 < 1.
   - Natural mode on the P1 harness at M = 10⁻¹⁰ warns once.
   - The golden histories at α = 0 return no warning.
   - With the model's `classical_turning_point` patched to raise, the natural-mode floor run at α = 0
     still succeeds, bitwise, with a "could not be assessed" warning.
   - `tune_history`, with `integrate_history` patched to return the same warning on three
     integrations, prints it once.
8. **The records.** The new fields' contents, by construction and on item 3's and item 6's histories;
   a drain point's assessment is its infall's last.

**Each test is shown to catch a breakage** (P11). Record each breakage as a diff, with the tests it
fails; never commit one.
- B1: K = 10 in place of 3 (item 5; the probe found 0.2902 against 0.2664 at this point).
- B2: ≤ in place of < in each comparison (item 2).
- B3: the gate kept at an unresolved reflection (item 3).
- B4: E = ρ_kin alone (item 2).
- B5: the floor rule gated again (item 4).
- B6: an unresolved reflection not appended to `reflections` (item 3).
- B7: the natural-mode model call unguarded (item 7).
- B8: the assessment run at α = 0 on every infalling step (item 1).
- B9: `tune_history` printing on every integration (item 7).

---

## 2. Measurement

Recorded as tables in the log. Every history uses `test_injection`'s setup (n = 1, φ* = 5 M_P,
π* = 0, T* = 2×10⁴ GeV, to T_CMB, default tolerances, the model's seed realization, untuned) unless
stated. Use a step budget of 2×10⁵, by patching `SM.StepControl` in the scratch script as the probe
did, so that no history runs for long. No store, no Ray.
1. **The cost.**
   - Per assessment, in µs: the H_E call and the model call, apart and together.
   - Per history: at M = 0.5 M_P, β = 2, α = 0.1, against the dispatch commit, run back to back in a
     temporary worktree of it that is removed afterwards. Minima of three, with the load.
   - The number of assessments per history.
2. **M = 0.5 M_P, α = 0.1, β ∈ {1.2, 2, 5, 25}**, against the dispatch commit (the same worktree):
   - the turning points, every pre-05 field of every `Injection`, the sample and the final state are
     bitwise 03's;
   - every candidate is resolved;
   - log₁₀ 𝓡 and φ_t at the first impact;
   - the warnings.
3. **The probe's band.** β = 2, α = 0.1, M ∈ {10⁻⁹, 3×10⁻¹⁰, 10⁻¹⁰, 3×10⁻¹¹} M_P. For each, 05's rule
   against the threshold patched to 0, to the same window's end. Give:
   - the candidates by kind;
   - Σ Δρ/ρ_R;
   - the failure, if any;
   - the probe's values from `scan_K_compare.out` beside yours. The probe used the kinetic E, gated
     floor reflections and parking, so say which differences those explain.
4. **Down to laboratory M.** β ∈ {1.2, 2, 3} × M ∈ {10⁻⁷, 10⁻⁸, 10⁻⁹, 10⁻¹⁰, 10⁻¹², 10⁻³⁰} M_P,
   α = 0.1, to T_CMB under the reduced budget. For each history:
   - the outcome, and for a failure its reason, N, T_J and |π| there;
   - the candidates by kind;
   - Σ Δρ/ρ_R;
   - the largest (V − V_floor)/E at an unresolved reflection;
   - the first impact's 𝓡 and warning;
   - the number of `None` assessments.

   Without 06's cut, failures at the bottom of a cascade are expected. Record how they fail, for 06;
   they are not this prompt's to fix.
5. **Natural mode**, α = 0, β = 2, M ∈ {0.5, 10⁻⁷, 10⁻¹⁰} M_P: the outcome, and the warning or its
   absence.

---

## 3. Verification

1. **Before any edit:** the five suites, run as `CLAUDE.md` says, with ComputeTargets in its two
   halves. Expect 26 / 339 (96 + 243) / 103 / 115 / 64 (log 04 §8). A different count, or a failure,
   is a stop.
2. **After:** the five suites. ComputeTargets rises by the new tests only, and the golden tests pass
   unchanged; say so. Then `black --check` on the files you changed.
3. **The breakages** of §1.6, each with the tests it fails.

## 4. Acceptance

1. §1 as written, and the gate **"Unresolved"** (README §8.4):
   - α = 0 bitwise;
   - an unresolved reflection is made at 3 φ_t with w_gate = 1, and every floor-rule reflection drains
     with w_gate = 1;
   - the reflected history equals the resolved one in the probe's band, within item 5's tolerance;
   - the warnings, once per history.
2. **The measurement** of §2, as tables in the log, with α > 0 at M = 0.5 bitwise 03's.
3. **The log**, `logs/05-unresolved-turnarounds.md`, in the template of `dk-and-rpk-migration`
   README §5.1. It holds:
   - the energy argument of §0.2 and the decision, with the constants;
   - the suite counts before and after;
   - the breakages as diffs;
   - every edit to an existing test, with its reason;
   - every deviation classified;
   - "State handed to the next prompt": the records' new fields and what each holds, the reflection
     kinds, where the assessment is made and what it costs, how the small-M histories fail, and the
     constants' names.
4. **The documents** (P12):
   - a dated note in `.documents/architecture-summary.md` beside 03's (the records, the reflection
     kinds, the warnings and the payload key);
   - a dated subsection 6.4 in `.documents/numerical-strategies.md`, after 03's 6.3 (the assessment,
     the rule and its constants).
5. **The board:**
   - row 05 and the gate Unresolved, in §1 and §2;
   - `### 5.6 After prompt 05 (<date>)` in §5;
   - §3 and §4 for anything opened, with `.documents/OPEN_ISSUES.md` in step.
6. **One commit**, in `CLAUDE.md`'s form, staged by explicit path only.

## 5. Stop conditions: stop and ask

- A baseline count differs from §3 item 1, or an existing test fails before you edit.
- Either golden history, `test_first_bounce`, `test_surfing_turning_points` or `test_kinematic_cap_loop`
  changes at α = 0.
- α > 0 at M = 0.5 M_P is not bitwise 03's in a pre-05 field (§2.2): the rule fired, or the assessment
  changed the state.
- An existing test needs an edit other than those of §1.6 item 4.
- In the probe's band the reflected and resolved histories differ by much more than the probes found:
  Σ Δρ/ρ_R by more than 1 %, or a different number of wall approaches.
- `classical_turning_point` raises on a history of §2.2 or §2.3. That would be a defect in 04's
  methods, whose fix is not this prompt's.
- The work seems to need a change to the model methods of 04, to the policies, or to the store.

## 6. What this prompt does not do

- **No parking**, no cut, no |π| guard: 06's (R14, P19, P21).
- **No peak, no margin, no ℓ**: 08's. The records carry what 08 needs.
- **No store, key or column**, and no change to `compute_scalar_history`'s signature.
- **No change to the model methods**, to `ExponentialModel`, or to the policies.
- **No change to the tools or the drivers.** `tools/history_and_bbn.py` and `main.py` print nothing
  new unless they go through `tune_history`.
- **Files it changes:**
  - `ComputeTargets/ScalarHistory.py`;
  - `ComputeTargets/tests/test_unresolved_turnarounds.py` (new);
  - `ComputeTargets/tests/test_injection.py` and `ComputeTargets/tests/test_gate_and_taper.py`, item (8)
    of each only;
  - `.documents/architecture-summary.md` and `.documents/numerical-strategies.md`;
  - the log, the board and, if anything is opened, `.documents/OPEN_ISSUES.md`.

  Any other file is a deviation to classify.
