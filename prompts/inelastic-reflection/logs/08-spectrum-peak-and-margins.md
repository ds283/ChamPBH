# Log 08 — the spectrum's peak and the relativistic margins

**Subject:** Compute the spectrum's peak and the relativistic margins per history
**Commit:** this commit, on branch `inelastic-reflection`, on top of `f0c139d`
**Date:** 2026-10-30/31, by Claude Opus 5.5
**Result:** **COMPLETE.** The gate "Margin" holds. One issue closed, none opened.
- **What shipped.** `integrate_history` computes, after the integration and outside its loop, each
  generation's peak by 𝓡 (classical, one-loop, or classical in the gap, flagged) and three margins
  ln(p/m) per generation: at birth, at every later event, and on the late tracker. The record
  (`Margins`) holds the worst of each over the generations carrying at least 10⁻³ of K, its N and
  generation, the failing share below ln 10, and the first failure. It also holds the weighted,
  below-weight and unassessed shares and the counts. The payload gains `"generations"`, `"margins"`
  and `"margin_warnings"` (ruling 1). None is stored. The step loop, `IntegrationResult` and every
  record the loop builds are unchanged.
- **The gate "Margin"** (README §8.4, prompt §4.1):
  - **The three margins equal a hand computation, energy-weighted**: `TestAConstructedHistory` and
    `TestTheEnergyWeight` (§2).
  - **The estimate is chosen by 𝓡, with the gap flagged.** At M = 10⁻¹⁶ M_P, β = 2, the three gap
    generations (log₁₀ 𝓡 = 2.221, 1.253, 0.318) are `"classical_gap"`, carrying 0.5876556; the one at
    4.238 is one-loop, carrying 0.2812482, with ℓ = 1.392 (§5.1).
  - **The classical birth margin is negative by construction**: ℓ_t bitwise, −0.70 to −0.71 on every
    resolved history (−3.0 at M = 10⁻³, β = 0.5).
  - **The coverage is within §1's bound**: no sample on 51 histories falls below it (§5.3). The
    parked segments are bounded by 0.006–0.021, against their 2.3–13.2 e-folds.
  - **NULL at α = 0**: the three keys are `None`, `None`, `()` on the golden histories and natural mode.
  - **Every value of the integration is bitwise**: 27 histories, 37 keys each, against `f0c139d` (§5.5).
- **R20's proviso holds as measured.** The largest shortfall of the field's own margin below
  min(b_j, r_j) is 1.8×10⁻³, against a bound of 0.225 (M = 10⁻³ M_P, β = 0.5). At M = 0.5, β = 25 it is
  3.0×10⁻⁴ against 0.015, `cover.out`'s case. The plateau turning points never bind.
- **Counts:** 26 / 439 (96 + 343) / 103 / 115 / 64 before, 26 / **471** (96 + **375**) / 103 / 115 / 64
  after, all passing. The 32 new tests are in `ComputeTargets/tests/test_margins.py`.
- No store was opened, no Ray started and nothing written to `var/`. No golden JSON was regenerated.

## 1. The physics and the derivation (prompt §1.6, ruling 7)

Written before the code, in my scratch directory, and transcribed here.

**Notation.** Generation j has ln q_j = ln k_ex,j + N_turn,j (Einstein frame, a_E = e^N). The field's own
margin at N is μ(N) = ln q − N − ln m(N), with m² = V''(φ) + Q T, Q = (ln A)'' + (ln A)'², and
T = ρ_R,E(Σ + f_m). An event's margin is μ_k^ev = ln q − N_k − ln m_k, with m_k the event's mass: the
estimate's at a candidate, the field's own at a plateau turning point or a wall-side one with no
candidate. δ_k = ln m_field(N_k) − ln m_k is the difference from the field's own mass there. It is zero
at a field-own event.

### 1.1 A swing

Take consecutive events N_a < N_b among the birth, the later turning points of both kinds and the
reflections. These are exactly the events of r_j:
- every wall-side turning point is a candidate's N_turn or a wall event with no candidate;
- every plateau turning point is an event;
- every reflection is a candidate.

π has one sign on (N_a, N_b), so φ is monotone there: a turning point is a sign change of π, a
reflection flips it, and a drain only scales it by √(1 − α_eff). The momentum falls as e^{−N}, so at
any N in the swing

    μ(N) ≥ ln q − N_b − max over the swing of ln m.

### 1.2 The mass on a swing

- **The exponential.** ln A = βφ/M_P, so (ln A)'' = 0 and Q = β²/M_P², independent of φ. V = e^{norm}
  exp((M/φ)ⁿ). (M/φ)ⁿ is completely monotone on φ > 0, and exp of a completely monotone function is
  completely monotone. So (−1)^k V^{(k)} ≥ 0: V''' < 0 and **V'' falls strictly with φ**. This is
  stronger than the prompt's convexity (§0 of note 08). At fixed trace, m_eff²(φ) falls strictly, and its
  maximum over a swing is at the inner end, φ_in = min(φ_a, φ_b).
- **The trace.** m²(N) ≤ V''(φ_in) + Q T_max = m_in² [1 + ε_in(T_max/T_in − 1)], where T_max is the
  largest trace on the swing and ε_in = Q T_in/m_in² is the trace's fraction of m² at the inner end. So

      ln m(N) ≤ ln m_in + τ,    τ = ½ log1p(ε_in (T_max/T_in − 1)),

  and τ = 0 where T_max = T_in. At fixed Σ + f_m, T falls as e^{−4ΔN}. So on an inward swing
  T_max/T_in ≤ e^{4ΔN}, a term in ln m of at most ½ log1p(ε_in(e^{4ΔN} − 1)). On an outward swing T_max
  is T_in unless a kick raises T (d ln T/dN = −4 + c g > 0, tracker_velocity_terms), which τ measures.
  At a wall the trace is a small part of m² (ε_in ≪ 1), so τ is small.
- **A generic model** must have m_eff² largest at an end of every swing at fixed trace: V'' + (ln A)'' T
  monotone, or quasi-convex, in φ on each swing. It also needs Q ≥ 0, so that the trace's term is
  bounded by T_max.

### 1.3 The bound

- **Inward swing** (inner end b): μ(N) ≥ ln q − N_b − ln m_b − τ = μ_b − τ.
- **Outward swing** (inner end a): μ(N) ≥ ln q − N − ln m_a − τ ≥ μ_a − (N − N_a) − τ ≥ μ_a − ΔN − τ.

With μ_in = μ_in^ev − δ_in, and μ_in^ev ≥ min(b_j, r_j) because the inner end is the birth or an event
of r_j, the shortfall is bounded:

    min(b_j, r_j) − μ(N) ≤ [ΔN = N_b − N_a if outward] + τ + max(0, δ_in).

`cover.out`'s 3×10⁻⁴ at β = 25 is the outward case. Measured here, it is 3.05×10⁻⁴ in a swing of
0.0148 e-folds whose bound is 0.0151 (§5.3). The margin is not changed to absorb the swing's term.

### 1.4 The estimate's mass against the field's own (ruling 7, second and third parts)

- **At a resolved wall-side turning point**, ω_t² = V''(φ_t), with φ_t from E at `phi_assessed`, the
  step start before it: expansion after that point is neglected, and the trace term is omitted. The
  field's own mass there is V''(φ_TP) + Q T. **Measured on every resolved candidate of §5.1** (the
  histories at M = 0.5 and 10⁻³ M_P, 488 candidates): δ = ln m_field(N_turn) − ½ ln ω_t² lies in
  [−7×10⁻⁷, +0.0037]. It is positive almost everywhere, largest at M = 0.5, β = 25, and
  φ_TP/φ_t = 0.99977–1.00000. **It enters the bound** as max(0, δ_in), and it is ≤ 0.0037 wherever it
  does.
- **At a floor or unresolved reflection** the code turns the field back at φ_refl > φ_t: at the floor
  rule's φ, or below 3φ_t. Measured, φ/φ_t = 1.19 to 5×10²¹. Physically the field goes on to φ_t
  (classical) or φ_ta (one-loop, outside φ_t), and there its mass is the estimate's, the largest of the
  bounce. So the estimate's mass is the physical one. Since V'' falls with φ, the code's own samples
  near the reflection have V''(φ) ≤ V''(φ_refl) < V''(φ_t). Measured, δ = −6.5 to −52 at every one
  of the 1 610 reflection candidates of §5.1, one-loop ones included. So the code's samples there lie
  above the event margin, and max(0, δ) = 0.

### 1.5 The parked segment of a released history (ruling 7, fourth part)

The interval runs from the cut candidate's N_turn to the first event after `Release.N`, in three pieces:
1. **[N_turn,cut, Parking.N]**, the outward part of the cut's swing: the bound is
   (Parking.N − N_turn,cut) + τ + max(0, δ_cut), by §1.3.
2. **[Parking.N, Release.N]**, the parked phase. There φ = φ_min and m = m_tr, so dμ/dN = −1 − d ln m_tr/dN.
   **If d ln m_tr/dN ≤ −1 throughout, μ does not fall**: μ(N) ≥ μ(Parking.N⁺) = ln q − Parking.N −
   ln m_tr(Parking.N). Then

       μ(N) ≥ μ_cut^ev − (Parking.N − N_turn,cut) − max(0, ln m_tr(Parking.N) − ln m_cut),

   which gives the bound **B_park = (Parking.N − N_turn,cut) + max(0, ln m_tr(Parking.N) − ln m_cut)**.
   It is not the segment's duration.
   - **Why the condition holds on the exponential.** On the tracker V' = −(ln A)' T. With u = (M/φ)ⁿ,
     V'' = (β T/M_P)(n u + n + 1)/φ, and far from the wall φ_min ∝ T^{−1/(n+1)}, so the V'' term goes
     as T^{(n+2)/(n+1)} (T^{3/2} at n = 1). The trace term goes as T. So d ln m_tr/d ln T lies between
     ½ (trace-dominated) and ¾ (n = 1). With d ln T/dN between −4 and −3, d ln m_tr/dN lies in −3 to
     −1.5.
   - **Measured**: d ln m_tr/d ln T = 0.744–0.750 at M = 10⁻³⁰ and 0.511–0.525 at M = 0.5, β = 25. On
     the three parked segments that lie before N_late (M = 0.5, β = 5, 10, 25) the largest
     d ln m_tr/dN over consecutive samples is −1.82, −1.34 and **−1.02**. At β = 25, d ln T/dN reaches
     −1.97, since the tracker's g = 1 + βπ_min/M_P is not 1 at β = 25, and the trace term dominates.
     The condition holds there, with 2 % to spare.
3. **[Release.N, N_e]**: a swing whose one end is the release, with the field's own mass there (φ = φ_min).
   §1.3 applies, with μ(Release.N) ≥ μ(Parking.N⁺) from piece 2.

**Measured (§5.3):** the bound is 0.0138, 0.0062 and 0.0210 at β = 5, 10 and 25, against segments of
2.25, 4.23 and 13.23 e-folds. The parked samples' field-own margins lie above min(b_j, r_j) by at least
2.9–3.3: no shortfall at all. So the derivation gives a bound far tighter than the duration, and the
measurement lies inside it. Ruling 7's stop does not fire.

**Where the condition fails.** On the late tracker at small M it fails, at T_J = 22.8–30.1 keV. There
d ln T/dN reaches +4.8 (M = 10⁻³⁰, β = 2) because Σ rises, the non-monotone band of
`[00-xav-sigma-is-non-monotone-at-25-35-keV]`. Those samples lie after N_late, in the tracker margin's
domain, where the margin is computed directly and nothing rests on the condition. No released
history's parked segment crosses 23–35 keV: β = 25 is released at 40.3 keV. Recorded in §7.

## 2. What shipped

### `ComputeTargets/ScalarHistory.py`

- **Constants**, beside the release's: `MARGIN_THRESHOLD = log(10.0)` (P6) and
  `MARGIN_WEIGHT_SHARE = 1.0e-3` (U9), neither a key, with their comment.
- **Warning kinds.** `WARNING_BIRTH_MARGIN`, `WARNING_REFLECTION_MARGIN`, `WARNING_TRACKER_MARGIN` and
  `WARNING_MARGINS_FAILED` join `WARNING_KINDS`, which `warning_kind` reads unchanged. None names an M
  (P23), and none is a prefix of another. The comment above them says that the margins' lines go on
  `"margin_warnings"`.
- **The records:**
  - `ESTIMATE_CLASSICAL`, `ESTIMATE_ONE_LOOP`, `ESTIMATE_CLASSICAL_GAP`;
  - `PeakEstimate`, `MarginCandidate`, `MarginEvent`, the core's inputs;
  - `Generation`, `MarginFailure` and `Margins`, its outputs (§8 lists their fields);
  - `MARGIN_KINDS`.
- **The functions:**
  - `peak_estimate(model, injection)`: §1.1 of the prompt; it catches only `ComputationFailureError` from
    `one_loop_turnaround`;
  - `tracker_log_m(model, phi_start, ln ρ_R,E, ln f_m, Σ)`: `minimum_on_trace` on the trace from `_log_trace`,
    then ½ `log_m_eff_squared` at φ_min (ruling 4);
  - `field_log_m(model, cosmology, state)`: ½ `log_m_eff_squared` at the state's own φ, Σ from `log_trace`
    (ruling 4);
  - `relativistic_margins(...)`, the pure core: plain values in, the generations and the record out;
  - `history_margins(result, sample, policy)`, the gatherer;
  - `margin_warning_lines(margins)`.
- **The caller.** `integrate_history` runs, after `fixed_T_values`, under the step loop's `assess_wall`
  expression verbatim (`alpha > 0.0 and model.reflects_at_origin and model.log_V_floor is not None`),
  `history_margins` and `margin_warning_lines`. A `ComputationFailureError` sets `None`, `None` and one
  `WARNING_MARGINS_FAILED` line. The payload gains the three keys. A failure payload does not.
- **`tune_history`** iterates `(*data.get("warnings", ()), *data.get("margin_warnings", ()))` through its one
  set of printed kinds. Its docstring and `integrate_history`'s `:return:` gained a sentence each (D4).
- `git diff f0c139d -- ComputeTargets/ScalarHistory.py` removes four lines: the two docstring lines
  replaced, the comment line and the `for` line of `tune_history`'s loop. `integrate_scalar_history`,
  `IntegrationResult` and every record the loop builds are untouched.

**The definitions as built.**
- **A generation**: a candidate with `delta_K > 0.0` and `classical_turning_point` not None. Its share is
  `delta_K / K_final`, and it is weighted iff share ≥ `MARGIN_WEIGHT_SHARE`.
- **The estimate** (`peak_estimate`):
  - `log_R <= 0.0`: classical, (`log_k_ex`, `0.5 * log_omega_t_squared`);
  - otherwise `model.one_loop_turnaround(phi_assessed, log_E_assessed)`. A result gives one-loop,
    (`log_k_ex`, `log_m_ta`). A `ComputationFailureError` or a `None` gives `"classical_gap"`, the
    classical pair, and `gap_reason` the message, or "one_loop_turnaround returned None".
- **ln q** = `log_k_ex + N_turn`.
- **Birth**: `log_k_ex - log_m`, which is the method's own expression (ruling 6), so `ell_t` or `ell`
  bitwise.
- **Reflection**: over every candidate (at `N_turn`, with its estimate's `log_m`, or `field_log_m` at
  `solution(N_turn)` if unassessed) and every `MarginEvent`:
  - `"plateau"` for each plateau turning point;
  - `"wall"` for each wall-side one whose N is no `"turning_point"` candidate's `N_turn`;

  each with `field_log_m` at `solution(tp.N)`. It is the minimum of `log_q - N_k - log_m_k` over the
  events with `N_k > N_turn`, in N order, strict, a stable sort putting a candidate before an event at
  the same N. None if there is no later event.
- **N_late**: `max` of every turning point's N and every candidate's `N_turn`, or −∞ if there are none.
- **Tracker**: over the samples, sorted by `raw_N`, with `raw_N > N_late`, then today, of
  `log_q - N - ln m_tr`. Each ln m_tr is `tracker_log_m` from the sample's own `phi_Einstein`, with its
  stored ln ρ_R,E, ln f_m and Σ. Today's is from the final state, with Σ from `log_trace` (ruling 3).
- **K_final** = `policy.dark_radiation.K(N_final)`. The unassessed K is `K_initial` + `Parking.delta_K`
  (ruling 9). A K_final of 0 gives no generation and zero shares (D2).
- **The worst**: over the weighted generations in index order, strict, so a tie keeps the lowest index.
  The failing share sums those below ln 10, and None is not failing. **The first failure**: per weighted
  generation, its N_turn (birth) or its first point below ln 10 in N order (reflection, tracker); then
  the minimum over the generations of (N, margin, index) (ruling 8).

### The tests (`ComputeTargets/tests/test_margins.py`, new; 32 tests, 43 s at a load of 5–7)

No store, no Ray, no PRyMordial solve. Six histories are integrated once per module (`history()`), and
one more under item 8's patch. Item 9 is the suites' (§4.1).

| Test | Item | Asserts | Tolerance |
|---|---|---|---|
| `test_1_the_generations_are_the_assessed_candidates_with_delta_K` | 1 | generations #0, #2, #3, estimates classical, one-loop, gap; ln q = ln k + N_turn; share = ΔK (K = 1) | equality (same operations) |
| `test_1_the_birth_margins` | 1 | b_j = ln k − ln m | equality |
| `test_1_the_reflection_margins_and_the_plateau_that_binds` | 1 | r₀, r₂ at the plateau N = 11.5 (it binds both), r₃ at the wall event 12.2; r₀ = the min over all seven events by hand | equality |
| `test_1_without_the_plateau_the_margin_would_be_another` | 1 | the plateaus left out, the worst is the wall event's | equality |
| `test_1_the_tracker_margins_read_the_late_samples_and_today_only` | 1 | samples at 12.0 and at N_late = 12.5 (ln m = 50) are not read; t_j and its N by hand; 3 late points | equality |
| `test_1_the_worst_margins` | 1 | (value, N, index) of each worst | equality |
| `test_1_the_failing_shares` | 1 | 0.5, 0.9, 0.9 as the same sums in index order | equality |
| `test_1_the_shares_and_the_counts` | 1 | weighted, below, unassessed (leftover + K_0 + the unassessed candidate); their sum within 6ε of 1 (six terms); the seven counts; N_late | equality; 6ε |
| `test_1_the_first_failures` | 1 | birth #0 at 10.0; reflection #0 at the plateau 10.25; tracker: three generations fail first at 13.0, the smallest margin's (#0) reported (ruling 8) | equality |
| `test_1_a_history_that_drained_nothing_has_no_generation` | 1 (D2) | K_final = 0: no generation, the shares 0, every worst and first failure None | exact |
| `test_1_no_later_event_is_a_margin_of_None_and_not_failing` | 1 | r = None, worst None, failing share 0 | exact |
| `test_2_the_threshold_is_1e_minus_3` | 2 | the two constants | exact |
| `test_2_just_below_the_weight_it_is_left_out` | 2 | share `nextafter(1e-3, 0)`: not weighted, not the worst, not failing; counted below the weight | exact |
| `test_2_at_the_weight_it_is_included` | 2 | share 1e-3: weighted, the worst (−6), failing share 1e-3 | exact |
| `test_2_the_leftover_is_counted_only_as_unassessed` | 2 | M = 10⁻³⁰, β = 2: the generations are exactly the candidates with ΔK > 0 and an assessment; unassessed share = leftover/K; the cut candidate's share is its own ΔK/K; partition within n·4ε | equality; (n + 1)·4ε |
| `test_3_the_history_succeeds` | 3 | M = 10⁻¹⁶, β = 2 succeeds with a record | — |
| `test_3_the_estimate_follows_R` | 3 | ln 𝓡 ≤ 0 classical; 0 < log₁₀ 𝓡 ≤ 2.92 gap, reason "no one-loop turnaround"; else ≥ 3.00 and one-loop | exact |
| `test_3_the_counts_and_shares_of_log_07b` | 3 | 3 gap carrying 0.5877, 1 one-loop carrying 0.2812, and the record's counts | 5×10⁻⁵ (ruling 9); measured 4.4×10⁻⁵ and 4.8×10⁻⁵ |
| `test_3_the_one_loop_birth_margin_is_its_ell` | 3 | birth = ℓ, ln k, ln m = the method's, ℓ > 0, ln q from the injection's N_turn | bitwise |
| `test_3_a_gap_generation_is_born_classical` | 3 | birth = ℓ_t, ln q = ln k_ex + the injection's N_turn | bitwise |
| `test_4_every_generation_is_classical_and_born_below_zero` | 4 | M = 0.5, β = 2: classical, ln 𝓡 ≤ 0, birth = ℓ_t < 0; N_turn < N (the drain point); ln q from the injection's N_turn (D3) | bitwise |
| `test_4_the_events_are_the_plateau_and_the_undrained_wall_turning_points` | 4, ruling 5 | plateau and wall event counts against the turning points | exact |
| `test_5_N_late_on_the_parked_history` | 5 | M = 10⁻³⁰: N_late as defined, ≤ Parking.N | exact |
| `test_5_the_parked_tracker_margin_is_the_recomputation` | 5 | late points = samples after N_late + 1; each t_j and its N against the test's own `minimum_on_trace` and `log_m_eff_squared` calls; t_j's N > N_late | bitwise |
| `test_5_the_released_history_includes_the_generations_after_the_release` | 5 | M = 0.5, β = 5: every drained candidate after `Release.candidate_index` is a generation; N_late > Release.N | exact |
| `test_6_the_golden_histories` | 6 | gh's β = 1.5 and 5.5 at α = 0: `None`, `None`, `()`; `"warnings"` = the loop's | exact |
| `test_6_natural_mode_at_large_M` | 6 | M = 0.5, β = 2 at α = 0: the same | exact |
| `test_7_the_phrases_are_kinds` | 7 | the four phrases are in `WARNING_KINDS`, and `warning_kind` returns them | exact |
| `test_7_birth_and_reflection_lines_at_large_M` | 7 | M = 0.5, β = 2: the birth and reflection lines, one per failing kind; the line carries the worst and the share; `"warnings"` = the loop's | string |
| `test_7_tune_history_prints_each_kind_once` | 7 | scripted payloads: elastic, then birth #0 (first integration), then tracker #0 (second), then the failure | exact |
| `test_8_the_history_succeeds_with_the_failure_as_its_warning` | 8 | `tracker_log_m` patched to raise: success, `None`, `None`, one `WARNING_MARGINS_FAILED` line with the reason | exact |
| `test_8_every_other_key_is_the_unpatched_runs` | 8 | every other key by `repr`, the z grid's values, `compute_steps`, `RHS_evaluations` | bitwise |

### The documents (P12)

- `.documents/architecture-summary.md`: a dated note after 07b's on the model methods. It covers the
  generations and their estimates, the margins, the core and the gatherer, the record, the payload
  keys and the warnings, and that nothing is stored.
- `.documents/numerical-strategies.md`: a new §6.8 after 07b's §6.7. It covers the definitions, the
  coverage bound with the parked segment, the weight and the unassessed share, and the margins measured
  across M with their cost.

## 3. Deviations from the prompt

**The rulings of orchestrator note 08 (1–16):**

| # | Ruling, as applied | Class |
|---|---|---|
| R1 | The margins' lines are on `"margin_warnings"`, never on `"warnings"`, which stays `result.warnings` bitwise (§5.5; tests 6, 7). The four phrases join `WARNING_KINDS`. It is `()` at α = 0 and one line where the margins fail; a failure payload does not gain the key. `tune_history` reads `"warnings"`, then `"margin_warnings"`, through one set of kinds (test 7). `test_unresolved_turnarounds:592` and `test_parking:933` pass unedited | STRUCTURALLY REQUIRED |
| R2 | Three keys, `None`, `None`, `()` at α = 0; the caller's condition is `assess_wall`'s expression verbatim; the gatherer after `fixed_T_values`; only `ComputationFailureError` caught | IMPLEMENTATION CHOICE (ruled) |
| R3 | Each late sample and today searched from its own field; nothing carried; a parked sample's φ is not reused as φ_min. The trace from the stored values by `_log_trace`; at a dense-output state and today by `log_trace`. No `minimum_on_trace` failure on any history of §5 | IMPLEMENTATION CHOICE (ruled) |
| R4 | `tracker_log_m` and `field_log_m`, module-level, called by their module names. Test 8 patches `tracker_log_m`, at M = 0.5, β = 2; `minimum_on_trace` is never patched | IMPLEMENTATION CHOICE (ruled) |
| R5 | Events: later candidates (at N_turn, §1.1's mass, unassessed ones the field's own), later plateau turning points, and later wall-side turning points with no candidate (`MarginEvent` "wall"). Counted on every history: **0** wall events on all 51 (§5.1). Each candidate's estimate and one-loop call is made once | IMPLEMENTATION CHOICE (ruled) |
| R6 | Birth as the method's expression; ℓ_t and ℓ bitwise, `assertEqual` in tests 3 and 4 | IMPLEMENTATION CHOICE (ruled) |
| R7 | §1's derivation: the monotone form of V'' on the exponential (§1.2); the trace term τ; δ at resolved turning points measured (≤ +0.0037, enters the bound) and at reflections (−6.5 to −52, does not); the parked segment's own bound with d ln m_tr/dN ≤ −1 (§1.5), reported per released history (§5.3). No stop | STRUCTURALLY REQUIRED |
| R8 | Ties: the worst keeps the lowest index; a first failure at a shared N reports the smallest margin, then the lowest index (test 1's tracker case); minima in injections order and in N order | IMPLEMENTATION CHOICE (ruled) |
| R9 | K from the ledger; K_0 counted unassessed; the three shares partition K (tests 1, 2); item 3's tolerance 5×10⁻⁵ on each share, against differences of 4.4×10⁻⁵ and 4.8×10⁻⁵ (§5.1) | IMPLEMENTATION CHOICE (ruled) |
| R10 | Counts per estimate over every generation; weighted and unassessed counts in the record; the list holds generations only, with their index | IMPLEMENTATION CHOICE (ruled) |
| R11 | Test 6 asserts the three keys and `"warnings"` = the loop's on gh's two golden setups (and natural mode at M = 0.5); bitwise against the dispatch commit is the golden tests' and §5.5's | IMPLEMENTATION CHOICE (ruled) |
| R12 | §5.6: the printed margin lines against the converged `"margin_warnings"`: the same kinds, different values. Nothing changed | IMPLEMENTATION CHOICE (ruled) |
| R13 | §5.2: every gap generation's reason is "no one-loop turnaround: ell … fell to"; no other kind and no `None` | IMPLEMENTATION CHOICE (ruled) |
| R14 | One worktree of `f0c139d` in scratch, removed afterwards; 37 keys by `repr` on 27 histories (§5.5) | IMPLEMENTATION CHOICE (ruled) |
| R15 | Only prompt §6's files changed. `CosmologyConcepts/`, every existing test module, the golden modules and JSON and the `integrator-remediation` board are byte-identical. In `ScalarHistory.py` only the items ruling 15 lists, plus D4's two docstring sentences | as ruled, with D4 |
| R16 | Costs: minimum of three rounds on M = 10⁻³, β = 1.2; M = 0.5, β = 5; β = 2 at 10⁻¹⁶ and 10⁻³⁰; the rest single rounds. Suites serial, ComputeTargets in halves, to scratch with `uptime`. §5's histories four at a time under `timeout 900` (none reached it). No failure needed a rerun | as ruled |

**My own deviations:**

| # | Deviation | Class |
|---|---|---|
| D1 | `Margins` also holds `plateau_events`, `wall_events`, `N_late` and `late_points`; `Generation` also holds `kind`, `weighted` and `gap_reason`. They report what the log counts (ruling 5) and why a generation is in the gap (ruling 13) | IMPLEMENTATION CHOICE |
| D2 | `unassessed_share` is 0.0 where K_final = 0. B7's first replay raised `ZeroDivisionError` at α = 0, which shows that a dissipation history that drained nothing would have failed instead of warning (§1.4 of the prompt). ΔK > 0 implies K_final > 0, so no other share divides by 0. Added after §5's measurement, which it cannot change (every measured K is positive); the suites ran on the final tree. Tested by `test_1_a_history_that_drained_nothing_has_no_generation` | STRUCTURALLY REQUIRED |
| D3 | Tests 3 and 4 assert ln q against the injection's N_turn, and test 4 that N_turn < N. In the first replay, B1 passed every test, because the tests compared against the generation's own N_turn, which B1 moves too. As 07b's D4 | IMPLEMENTATION CHOICE |
| D4 | A sentence added to `integrate_history`'s `:return:` and to `tune_history`'s docstring, naming the new keys and their printing | IMPLEMENTATION CHOICE |
| D5 | Three tests beyond the prompt's items: the plateau's binding shown by its removal; the events counted on a real history (which also catches a gatherer that drops the plateaus); and D2's case | IMPLEMENTATION CHOICE |
| D6 | B1 and B5 are made in the gatherer, and B3, B4 and B6 in the core. The core has no drain point, so "ln q from the drain point's N" is the gatherer passing `N` for `N_turn` | IMPLEMENTATION CHOICE |
| D7 | §5.5 also compares gth's model untuned at its seed (β = 3, M = 10⁻² M_P, α = 0, to T_CMB), one integration, for "the golden histories' setups"; the tuned golden test covers the tuning | IMPLEMENTATION CHOICE |

## 4. Verification performed

### 4.1 The suites

Run serially with `CLAUDE.md`'s commands, ComputeTargets in two halves, with the output in my scratch
directory. "Before" was at `f0c139d`, before any edit. "After" was on the final tree.

| Suite | Before (count, unittest's time, load before → after) | After |
|---|---|---|
| CosmologyModels | 26 OK, 77 s, 4.4 → 13.3 | 26 OK, 76 s, 2.9 → 3.3 |
| ComputeTargets [a-f] | 96 OK, 143 s, 13.3 → 14.2 | 96 OK, 142 s, 3.3 → 2.8 |
| ComputeTargets [g-z] | 343 OK, 484 s, 14.2 → 5.6 | **375** OK, 475 s, 2.8 → 37.7 |
| ObjectFactories | 103 OK, 18 s, 5.6 → 5.3 | 103 OK, 16 s, 37.7 → 34.1 |
| tools | 115 OK, 147 s, 5.3 → 3.5 | 115 OK, 128 s, 34.1 → 27.2 |
| RunDrivers | 64 OK, 27 s, 3.5 → 3.3 | 64 OK, 26 s, 27.2 → 29.0 |

"Before" ran 23:33–23:49 on 2026-10-30, "after" 00:10–00:25 on 2026-10-31. Each suite ran once and
passed, so nothing was re-run.

- The baselines are prompt §3.1's: 26 / 439 (96 + 343) / 103 / 115 / 64.
- ComputeTargets rose by 32, the new module, which sorts into [g-z]. Nothing fell.
- **The tests of prompt §1.8 item 9 passed unedited.** These are the golden tests,
  `test_first_bounce`, `test_surfing_turning_points`, `test_kinematic_cap_loop`, `test_injection`,
  `test_gate_and_taper`, `test_unresolved_turnarounds` (its `:592`), `test_parking` (its `:933`),
  `test_release`, `test_wall_model` and `test_wall_rounding`. No existing test module changed.
- `black --check` on the two changed Python files: unchanged.

## 5. The measurement (prompt §2)

Every history uses test_injection's setup: n = 1, φ* = 5 M_P, π* = 0, T* = 2×10⁴ GeV, to T_CMB, default
tolerances, the seed realization, untuned, α = 0.1 unless stated. The scripts are in my scratch
directory, `agent08/meas/`:
- `hist.py`: one history, its 37-key `repr` record, and at the final tree the margins analysis;
- `runjobs.sh`: four at a time under `timeout 900`;
- `bitwise.py` and `tables.py`: the comparison and the tables;
- `tune.py` (§5.6) and `dlnm.py` (§1.5).

The dispatch commit ran in a worktree of `f0c139d` (`agent08/wt-dispatch`), which was removed with
`git worktree remove`; `git worktree list` shows the main checkout alone. The load was 3–12 throughout.

### 5.1 The margins across M (prompt §2.1)

"gens" are classical / one-loop / gap, each with its share of K. Each worst margin has its N and its
generation. The failing shares are birth / reflection / tracker, and so are the first failures (#gen@N).
"W / below / unass." are the shares. The lines column gives the margin lines: b(irth), r(eflection),
t(racker). "Wall" is the history's wall time, margins included.

| History | gens (share) | worst birth | worst refl. | worst tracker | failing b / r / t | W / below / unass. | first failures | lines | wall (s) |
|---|---|---|---|---|---|---|---|---|---|
| M = 0.5, β = 1.2 | 7 (1.0000) / 0 / 0 | −0.710 (26.299, #5) | +0.014 (25.846, #3) | +7.376 (29.263, #5) | 1.0000 / 0.0384 / 0 | 1.0000 / 9.1e-6 / 0 | #0@17.662, #2@25.397, — | b, r | 1.7 |
| M = 0.5, β = 2 | 28 (1.0000) / 0 / 0 | −0.709 (29.229, #15) | −0.527 (29.351, #15) | +8.144 (33.861, #15) | 0.9971 / 0.9942 / 0 | 0.9971 / 2.9e-3 / 0 | #0@20.342, #0@21.371, — | b, r | 3.3 |
| M = 0.5, β = 5 | 121 (0.9991) / 0 / 0 | −0.704 (35.321, #23) | −0.640 (35.368, #23) | +8.586 (43.613, #23) | 0.9914 / 0.9914 / 0 | 0.9914 / 7.8e-3 / 8.8e-4 | #0@29.715, #0@30.621, — | b, r | 6.8 |
| M = 0.5, β = 10 | 119 (0.9991) / 0 / 0 | −0.703 (57.591, #21) | −0.481 (56.391, #10) | +10.301 (68.493, #21) | 0.9952 / 0.9952 / 0 | 0.9952 / 3.9e-3 / 9.0e-4 | #0@53.858, #0@54.631, — | b, r | 7.2 |
| M = 0.5, β = 25 | 85 (0.9977) / 0 / 0 | −0.703 (130.765, #37) | −0.668 (130.765, #36) | +20.022 (147.465, #37) | 0.9867 / 0.9867 / 0 | 0.9867 / 1.1e-2 / 2.3e-3 | #0@127.823, #0@128.558, — | b, r | 7.4 |
| M = 10⁻³, β = 0.5 | 13 (1.0000) / 0 / 0 | −2.999 (41.156, #11) | −2.375 (41.492, #11) | **−2.211** (41.498, #11) | 0.9998 / 0.9998 / 0.0673 | 0.9998 / 2.3e-4 / 0 | #0@36.955, #0@37.992, #11@41.498 | b, r, t | 2.0 |
| M = 10⁻³, β = 1.2 | 7 (0.9999) / 0 / 0 | −0.709 (26.284, #5) | +0.005 (25.846, #3) | +10.199 (28.374, #5) | 0.9999 / 0.0404 / 0 | 0.9999 / 5.6e-6 / 5.1e-5 | #0@17.668, #2@25.404, — | b, r | 1.9 |
| M = 10⁻³, β = 2 | 49 (0.9999) / 0 / 0 | −0.708 (29.267, #17) | −0.555 (29.343, #17) | +4.788 (30.381, #17) | 0.9925 / 0.9891 / 0 | 0.9925 / 7.4e-3 / 1.1e-4 | #0@20.352, #0@21.381, — | b, r | 5.4 |
| M = 10⁻³, β = 3 | 59 (0.9998) / 0 / 0 | −0.705 (28.694, #28) | −0.587 (28.739, #28) | +4.140 (29.410, #28) | 0.9931 / 0.9931 / 0 | 0.9931 / 6.8e-3 / 1.6e-4 | #0@24.499, #0@25.344, — | b, r | 6.8 |
| β = 1.2, M = 10⁻⁷ … 10⁻¹⁵ (7 histories) | 7 (0.9999) / 0 / 0 | −0.709 (26.284, #5) | +0.005 (25.846, #3) | +15.053, +16.278, +17.502, +18.737, +21.237, +22.492, +25.059 (28.364–28.373, #5) | 0.9999 / 0.0405 / 0 | 0.9999 / 5.7e-6 / 5.1e-5 | #0@17.668, #2@25.404, — | b, r | 1.7–2.0 |
| β = 1.2, M = 10⁻¹⁶ | 6 (0.1222) / 0 / 1 (0.8777) | −0.709 (26.284, #5) | +0.005 (25.846, #3) | +26.358 (28.364, #5) | 0.9999 / 0.0405 / 0 | 0.9999 / 5.7e-6 / 5.1e-5 | #0@17.668, #2@25.404, — | b, r | 2.0 |
| β = 1.2, M = 10⁻¹⁷ | 5 (0.0448) / 1 (0.8777) / 1 (0.0775) | −0.709 (26.284, #5) | +0.005 (25.846, #3) | +27.686 | 0.1222 / 0.0405 / 0 | ″ | #1@19.904, #2@25.404, — | b, r | 2.0 |
| β = 1.2, M = 10⁻¹⁸ | 5 (0.0448) / 2 (0.9552) / 0 | ″ | ″ | +29.044 | 0.1222 / 0.0405 / 0 | ″ | #1@19.904, #2@25.404, — | b, r | 2.0 |
| β = 1.2, M = 10⁻²⁰ | 2 (0.0043) / 3 (0.9724) / 2 (0.0232) | ″ | ″ | +31.890 | 0.0447 / 0.0405 / 0 | ″ | #2@24.782, #2@25.404, — | b, r | 2.0 |
| β = 1.2, M = 10⁻²² | 1 (2e-5) / 6 (0.9999) / 0 | +3.245 (26.284, #5) | +4.108 (26.284, #4) | +34.731 | 0 / 0 / 0 | ″ | —, —, — | — | 2.0 |
| β = 1.2, M = 10⁻²⁵ | 0 / 7 (0.9999) / 0 | +9.873 | +10.750 | +39.277 | 0 / 0 / 0 | ″ | — | — | 2.0 |
| β = 1.2, M = 10⁻³⁰ | 0 / 7 (0.9999) / 0 | +20.172 | +21.095 | +46.520 | 0 / 0 / 0 | ″ | — | — | 2.0 |
| β = 2, M = 10⁻⁷ … 10⁻¹³ (6 histories) | 49 (0.9999) / 0 / 0 | −0.708 (29.267, #17) | −0.555 (29.342, #17) | +9.511, +10.714, +11.918, +13.125, +15.549, +16.767 (30.356, #17) | 0.9924 / 0.9891 / 0 | 0.9924 / 7.5e-3 / 1.2e-4 | #0@20.352, #0@21.381, — | b, r | 4.8–7.6 |
| β = 2, M = 10⁻¹⁵ | 48 (0.7186) / 0 / 1 (0.2812) | ″ | ″ | +19.215 | 0.9924 / 0.9891 / 0 | ″ | ″ | b, r | 7.5 |
| β = 2, M = 10⁻¹⁶ | 45 (0.1310) / 1 (0.2812) / 3 (0.5877) | ″ | ″ | +20.448 | 0.9924 / 0.9891 / 0 | ″ | ″ | b, r | 7.4 |
| β = 2, M = 10⁻¹⁷ | 42 (0.0374) / 4 (0.8689) / 3 (0.0936) | ″ | ″ | +21.687 | 0.4804 / 0.7078 / 0 | ″ | #2@21.884, #2@22.268, — | b, r | 7.5 |
| β = 2, M = 10⁻¹⁸ | 40 (0.0229) / 8 (0.9736) / 1 (0.0033) | ″ | ″ | +22.934 | 0.0766 / 0.0732 / 0 | ″ | #5@23.386, #5@23.824, — | b, r | 7.5 |
| β = 2, M = 10⁻²⁰ | 28 (0.0049) / 11 (0.9811) / 10 (0.0139) | ″ | ″ | +25.456 | 0.0155 / 0.0155 / 0 | ″ | #9@27.760, #9@28.227, — | b, r | 7.5 |
| β = 2, M = 10⁻²² / 10⁻²⁵ / 10⁻³⁰ | 0 / 49 (0.9999) / 0 | +3.958 / +10.567 / +20.904 (29.267, #17) | +3.983 / +10.597 / +20.935 (29.342, #17) | +27.721 / +31.212 / +37.656 | 0 / 0 / 0 | ″ | — | — | 7.5–7.7 |
| β = 3, M = 10⁻⁷ … 10⁻¹³ (6 histories) | 59 (0.9998) / 0 / 0 | −0.705 (28.693, #28) | −0.587 (28.738, #28) | +8.839, +10.024, +11.211, +12.398, +14.776, +15.967 (29.396–29.397, #28) | 0.9930 / 0.9930 / 0 | 0.9930 / 6.8e-3 / 1.6e-4 | #0@24.499, #0@25.344, — | b, r | 5.8–8.9 |
| β = 3, M = 10⁻¹⁵ | 58 (0.8521) / 0 / 1 (0.1478) | ″ | ″ | +18.354 | 0.9930 / 0.9930 / 0 | ″ | ″ | b, r | 9.1 |
| β = 3, M = 10⁻¹⁶ | 50 (0.2167) / 2 (0.2638) / 7 (0.5194) | ″ | ″ | +19.550 | 0.9930 / 0.9930 / 0 | ″ | ″ | b, r | 8.8 |
| β = 3, M = 10⁻¹⁷ | 36 (0.0154) / 13 (0.9183) / 10 (0.0662) | ″ | ″ | +20.749 | 0.5432 / 0.7292 / 0 | ″ | #4@26.399, #4@26.587, — | b, r | 9.2 |
| β = 3, M = 10⁻¹⁸ | 3 (1e-4) / 29 (0.9930) / 27 (0.0068) | +0.467 (28.693, #28) | −0.642 (28.738, #28) | +21.894 | 0.0293 / 0.0748 / 0 | ″ | #17@27.862, #17@27.971, — | b, r | 9.2 |
| β = 3, M = 10⁻²⁰ / 10⁻²² / 10⁻²⁵ / 10⁻³⁰ | 0 / 59 (0.9998) / 0 | +5.435 / +9.932 / +16.516 / +27.107 | +5.464 / +9.963 / +16.548 / +27.139 | +24.052 / +26.214 / +29.422 / +34.598 | 0 / 0 / 0 | ″ | — | — | 8.9–9.0 |

"″" repeats the cell above it in the same β block.
- **Every margin line is b and r together**, except M = 10⁻³, β = 0.5, which also fails the tracker, and
  the histories with no line. The birth line fires on every history with a weighted classical or gap
  generation, as R13 expects.
- **The late weighted generations dominate the worst margins.** At β = 2 the worst birth margin is
  generation #17's at every M ≥ 10⁻²⁰, classical or not, because its 𝓡 is the smallest. At M = 10⁻¹⁶ the
  failing birth share is 0.9924: the classical and gap generations fail with ℓ_t ≈ −0.70, and the
  one-loop one, at ℓ = 1.39 < ln 10, does too.
- **The events** (ruling 5): plateau events 6–120 per history, **wall events with no candidate 0 on all
  51**, unassessed candidates 0. N_late is the cut candidate's N_turn on every parked, unreleased
  history, and the last generation's N_turn on the released ones (43.606, 68.488, 147.461 at M = 0.5,
  β = 5, 10, 25). The late points number 116–2 790.
- **Item 3's numbers** (M = 10⁻¹⁶, β = 2, at full precision): the gap generations #1, #2, #3 carry
  0.23077, 0.21836 and 0.13853, together 0.5876556460838358 (the log's 0.5877, off by 4.4×10⁻⁵); their
  births are −0.7029, −0.7031, −0.7033. The one-loop generation #0 carries 0.28124818254154527 (off by
  4.8×10⁻⁵ from 0.2812), with ℓ = 1.39249. Both lie within ruling 9's 5×10⁻⁵: deterministic, but close.

### 5.2 The gap (prompt §2.2, ruling 13)

| History | gap generations | log₁₀ 𝓡 | their births | their share | reason | nearest one-loop (log₁₀ 𝓡, ℓ) |
|---|---|---|---|---|---|---|
| β = 1.2, M = 10⁻¹⁶ | 1 | 2.41 | −0.703 | 0.8777 | ell fell (1) | — |
| β = 1.2, M = 10⁻¹⁷ | 1 | 1.07 | −0.704 | 0.0775 | ell fell (1) | #0: 6.41, 2.756 |
| β = 1.2, M = 10⁻²⁰ | 2 | 0.66–1.72 | −0.708 | 0.0232 | ell fell (2) | #2: 3.03, 0.395; #1: 13.07, 6.593 |
| β = 2, M = 10⁻¹⁵ | 1 | 0.24 | −0.702 | 0.2812 | ell fell (1) | — |
| β = 2, M = 10⁻¹⁶ | 3 | 0.32–2.22 | −0.703 | 0.5877 | ell fell (3) | #0: 4.24, 1.392 |
| β = 2, M = 10⁻¹⁷ | 3 | 0.46–2.83 | −0.704 | 0.0936 | ell fell (3) | #3: 4.32, 1.437; #2: 5.25, 2.039 |
| β = 2, M = 10⁻¹⁸ | 1 | 1.94 | −0.705 | 0.0033 | ell fell (1) | #7: 3.45, 0.812; #6: 4.46, 1.520 |
| β = 2, M = 10⁻²⁰ | 10 | 0.05–2.45 | −0.709 to −0.708 | 0.0139 | ell fell (10) | #10: **2.996**, 0.341; #9: 3.86, 1.085 |
| β = 3, M = 10⁻¹⁵ | 1 | 1.29 | −0.702 | 0.1478 | ell fell (1) | — |
| β = 3, M = 10⁻¹⁶ | 7 | 0.19–2.62 | −0.703 | 0.5194 | ell fell (7) | #1: 3.60, 0.947; #0: 5.29, 2.075 |
| β = 3, M = 10⁻¹⁷ | 10 | 0.11–2.78 | −0.704 | 0.0662 | ell fell (10) | #12: 3.09, 0.510; #11: 3.39, 0.776 |
| β = 3, M = 10⁻¹⁸ | 27 | 0.07–2.92 | −0.705 | 0.0068 | ell fell (27) | #28: 3.06, 0.467; #27: 3.22, 0.618 |

- **Every gap generation's reason is "no one-loop turnaround: ell = ln(k_ex/m_ta) fell to …"**, the
  saddle-node of log 04 §5.2. No other raise and no `None`; every gap candidate is a generation (none has
  ΔK = 0).
- **No stop.** No raise at log₁₀ 𝓡 ≥ 3.00, and no solution has ℓ ≤ 0: the smallest ℓ is 0.341. One
  solve lies at log₁₀ 𝓡 = 2.9964 (β = 2, M = 10⁻²⁰, #10), which log 07b printed as 3.00 (§7).
- The gap and the one-loop generations together match log 07b §5.3 on every history.

### 5.3 The coverage (prompt §2.3, R20's proviso, ruling 7)

For each weighted generation j, the field's own margin at every stored sample with N_turn,j < N ≤ N_late,
against min(b_j, r_j). "Shortfall" is min(b_j, r_j) − μ(sample), positive where the sample lies below.
The bound is §1.3's per sample, (ΔN if outward) + τ + max(0, δ_in), or §1.5's in the parked pieces.

| History | gens | smallest μ − min(b, r) over the gens | largest shortfall: gen, N, swing (N_a, N_b], ΔN, bound | samples above their bound | plateau binds |
|---|---|---|---|---|---|
| M = 0.5, β = 1.2 | 6 | +0.0013 | #4, −0.0013 at 25.8460, (25.8458, 26.0545], 0.209, 0.209 | 0 | 0 |
| M = 0.5, β = 2 | 16 | +0.0022 | #12, −0.0022 at 28.7773, (28.7767, 28.8636], 0.087, 0.087 | 0 | 0 |
| M = 0.5, β = 5 | 24 | +0.00027 | #7, −0.00027 at 33.9880, (33.9879, 34.0799], 0.092, 0.092 | 0 | 0 |
| M = 0.5, β = 10 | 22 | +0.0029 | #0, −0.0029 at 53.8578, (53.8577, 54.2579], 0.400, 0.400 | 0 | 0 |
| M = 0.5, β = 25 | 38 | **−0.00030** | #27, **+0.000305** at 130.4815, (130.4814, 130.4963], 0.0148, **0.0151** | 0 | 0 |
| M = 10⁻³, β = 0.5 | 12 | **−0.0018** | #11, **+0.00176** at 41.1576, (41.1561, 41.3803], 0.224, **0.225** | 0 | 0 |
| M = 10⁻³, β = 1.2 | 6 | +12.3 | #5, −12.3 at 28.3649, (26.6453, 28.3693], 1.72 (inward), 3.7e-6 | 0 | 0 |
| M = 10⁻³, β = 2 / 3 | 18 / 29 | +2.57 / +0.63 | outward swings of 0.015 / 0.054 | 0 | 0 |
| β = 1.2, M = 10⁻⁷ … 10⁻³⁰ (13) | 6 | +21.8 to +44.3 | outward swings of 0.361 or 0.699 | 0 | 0 |
| β = 2, M = 10⁻⁷ … 10⁻³⁰ (13) | 18 | +20.3 to +43.3 | outward swings of 0.038 or 0.502 | 0 | 0 |
| β = 3, M = 10⁻⁷ … 10⁻³⁰ (13) | 29 | +20.3 to +42.6 | outward swings of 0.022 or 0.413 | 0 | 0 |

- **No sample on any of the 51 histories lies below its bound.** The two positive shortfalls are both
  just after the inner end of an outward swing: the redshift since the turning point, and δ there. They
  are 3.0×10⁻⁴ (bound 0.0151) and 1.8×10⁻³ (bound 0.225). `cover.out` measured 3×10⁻⁴ at β = 25.
- **The plateau turning points never bind** r_j, on any weighted generation of any history, so they never
  set min(b_j, r_j). They are in the definition because R20's proviso needs them where they would, and
  test 1 shows the code honours them.
- **The parked segments of the released histories** (ruling 7). "Bound" is B_park of §1.5, and
  "d ln m_tr/dN" is over consecutive parked samples:

  | History | segment (Parking.N–Release.N, e-folds) | weighted gens covering it | smallest field-own margin there | largest shortfall | B_park | d ln m_tr/dN, max / min |
  |---|---|---|---|---|---|---|
  | M = 0.5, β = 5 | 38.0044–40.2545 (2.250) | #0–#23 | +2.596 | −3.30 (#23) | 0.0138 | −1.824 / −5.871 |
  | M = 0.5, β = 10 | 61.0959–65.3273 (4.231) | #0–#21 | +2.186 | −2.89 (#21) | 0.0062 | −1.335 / −5.903 |
  | M = 0.5, β = 25 | 132.5508–145.7793 (13.229) | #0–#37 | +2.586 | −3.29 (#37) | 0.0210 | −1.021 / −10.325 |
  | M = 10⁻³, β = 1.2 | 28.3719–45.9102 (17.538) | none: N_late = 28.369 lies before the switch | — | — | — | −1.572 / −2.236 |

  The field-own margins on the parked samples lie 2.9–3.3 above min(b_j, r_j). The bound is 0.006–0.021,
  against durations of 2.3–13.2 e-folds, so it is tighter than the duration and the measurement lies
  inside it. Ruling 7's stop does not fire.
- On the unreleased histories the parked phase lies after N_late, in the tracker margin's domain. There
  d ln m_tr/dN reaches +2.5 to +3.6, at T_J = 23–30 keV (§1.5, §7).

### 5.4 The late tracker (prompt §2.4)

Over the late samples and today: the largest |ln m_tr − ln m_field|, and the largest |t_j(tracker) −
t_j(field)| over the generations.

| History | late points | max Δ ln m | max Δ t_j |
|---|---|---|---|
| M = 0.5, β = 1.2 / 2 / 5 / 10 / 25 | 1 720 / 1 612 / 2 080 / 1 985 / 1 343 | 1.12 / 0.631 / 0.824 / 0.714 / 0.473 | 1.01 / 0.477 / 0.0144 / 0.0104 / 0.408 |
| M = 10⁻³, β = 0.5 / 1.2 | 116 / 1 919 | 0.21 / 0.046 | 0.176 / 0 |
| every parked history (M = 10⁻³, β = 2, 3; M ≤ 10⁻⁷) | 1 924–2 790 | 5.7×10⁻¹⁴ – 5.4×10⁻¹³ | 0 (2.8×10⁻¹⁴ once) |

On the parked tracker the field is φ_min to rounding, so the two masses agree to rounding. After the
last bounce of an oscillating history the field oscillates about the tracker, so its own mass
differs by up to 1.1 in ln m. The tracker margin is up to 1.0 larger with the tracker's mass (M = 0.5,
β = 1.2). Neither changes a verdict here: the tracker margin fails only at M = 10⁻³, β = 0.5, with both
masses.

### 5.5 What does not change (prompt §2.5, ruling 14)

The dispatch commit `f0c139d` and the final tree compared by `repr` on 37 keys: the 13 fields of
`IntegrationResult` other than `solution` (warnings, parking and release among them), `ts`, and the 23
payload keys that existed. The payload's `metadata` is compared by `compute_steps` and
`RHS_evaluations`, and `z_grid` by its values.

| Histories | against `f0c139d` | new keys | margin lines |
|---|---|---|---|
| log 05's grid: β = 1.2, 2, 3 × M = 10⁻⁷, 10⁻⁸, 10⁻⁹, 10⁻¹⁰, 10⁻¹² (15) | **bitwise**, all | the three | 2 (b, r) |
| β = 1.2, 2, 3 at M = 10⁻³⁰ (3) | **bitwise** | the three | 0 |
| M = 0.5, β = 1.2, 2, 5, 10, 25 (5) | **bitwise** | the three | 2 (b, r) |
| natural mode, M = 0.5, β = 2 | **bitwise** | `None`, `None`, `()` | 0 |
| golden β = 1.5, 5.5 (gh, to 1 MeV); gth's model at its seed, α = 0 (D7) | **bitwise** | `None`, `None`, `()` | 0 |

**All 27 are bitwise on all 37 keys**, `"warnings"` included with no exception (ruling 1). The payload
gains only `"generations"`, `"margins"` and `"margin_warnings"`.

### 5.6 The tuning (prompt §2.6, ruling 12)

`tune_history` at α = 0.1 (gth's tolerances, which are the defaults), with `history_margins` timed by a
wrapper. Load 5.5–11.7.

| History | integrations | tuning wall | margins per integration | printed lines | converged `"margin_warnings"` |
|---|---|---|---|---|---|
| M = 10⁻³, β = 1.2 | 4 | 8.3 s | 33.6–35.4 ms | nearly elastic; birth −0.7089 at 26.28429356 (#5), 0.9999; reflection 0.0047 at 25.84647261 (#3), 0.04045 | birth −0.7089 at 26.28429268 (#5); reflection **0.0048** at 25.84647205 (#3) |
| M = 0.5, β = 5 | 20 | 143.2 s | 90.6–157.0 ms | nearly elastic; birth −0.7039 at 35.32078837 (**#23**), 0.9914; reflection −0.6404 at 35.36753976 (#23), 0.9914 | birth −0.7030 at 35.33824827 (**#24**); reflection −0.6425 at 35.38113466 (#24); 0.9914 each |

- **The kinds agree; the values do not.** The printed lines are the seed's (05's convention). At β = 5 the
  worst generation is #23 at the seed and #24 at convergence, and the margins differ in the third
  decimal. Every one of the 4 and 20 integrations returned the same two kinds. Nothing was changed
  (ruling 12); recorded in §7.
- Converged margins: M = 10⁻³, β = 1.2: birth −0.709, reflection +0.0048, tracker +10.20; failing 0.9999
  / 0.0404 / 0. M = 0.5, β = 5: birth −0.703, reflection −0.642, tracker +8.62; failing 0.9914 / 0.9914 /
  0; 130 classical generations.
- The margins cost 1.7 % (β = 1.2: 4 × 34.5 ms of 8.3 s) and 1.6 % (β = 5: 20 × 112.7 ms of 143.2 s) of
  the tuning's wall time.

### 5.7 The cost (prompt §2.7)

The margins' total is `history_margins`. The parts are the one-loop calls (`peak_estimate` over every
candidate), the plateau masses (`field_log_m` at the events) and the tracker's samples (`tracker_log_m`
after N_late and today). Each is the minimum of three rounds on the four histories of ruling 16, and a
single round elsewhere.

| History | history wall (s) | margins (ms) | one-loop (ms) | plateau (ms) | tracker (ms) | share | load |
|---|---|---|---|---|---|---|---|
| M = 10⁻³, β = 1.2 (3 rounds) | 1.93 | 31.8 | 0.0 | 0.1 | 29.2 | 1.6 % | 6.7 |
| M = 0.5, β = 5 (3 rounds) | 6.77 | 79.0 | 0.0 | 1.7 | 57.7 | 1.2 % | 6.8 |
| β = 2, M = 10⁻¹⁶ (3 rounds) | 7.45 | 39.3 | 0.7 | 0.7 | 28.7 | 0.5 % | 7.0 |
| β = 2, M = 10⁻³⁰ (3 rounds) | 7.47 | 45.4 | 10.5 | 0.7 | 24.4 | 0.6 % | 7.0 |
| M = 0.5, β = 1.2 / 2 / 10 / 25 | 1.75 / 3.26 / 7.18 / 7.38 | 50.2 / 49.2 / 82.1 / 50.1 | ≤ 0.1 | 0.1–1.8 | 37.6–56.4 | 0.7–2.9 % | 6.1–6.6 |
| M = 10⁻³, β = 0.5 / 2 / 3 | 2.05 / 5.43 / 6.77 | 4.0 / 42.5 / 64.3 | 0.0 | 0.2–1.0 | 3.9–46.5 | 0.2–0.9 % | 6.3–6.7 |
| β = 1.2, M = 10⁻⁷ … 10⁻³⁰ | 1.70–2.03 | 24.7–46.0 | 0.0–2.1 | 0.1–0.2 | 21.1–30.1 | 1.2–2.4 % | 6.4–6.8 |
| β = 2, M = 10⁻⁷ … 10⁻²⁵ | 4.80–7.74 | 38.9–53.1 | 0.0–**43.4** | 0.7–0.9 | 25.8–68.6 | 0.5–0.9 % | 6.3–7.0 |
| β = 3, M = 10⁻⁷ … 10⁻³⁰ | 5.85–9.24 | 58.9–74.6 | 0.0–18.1 | 0.9–1.0 | 37.0–51.4 | 0.7–1.1 % | 6.7–7.6 |

The margins cost 4–83 ms per history, 0.2–2.9 % of its wall time. That is under 10 %, so the prompt's
condition is not met. The tracker's 1 000–2 800 `minimum_on_trace` calls dominate everywhere, and the
one-loop calls exceed them only at β = 2, M = 10⁻²⁵ (43 ms).

## 6. The deliberate-breakage record (P11)

- **The replay.** `make.py` wrote the eight diffs against the final `ScalarHistory.py` (difflib, three
  lines of context). `replay.sh`, run in bash, does the following for each one:
  - checks it with `git apply --check`, applies it, and checks it with `git apply -R --check`;
  - runs `ComputeTargets.tests.test_margins`;
  - reverts it, and compares the file's md5 with the final one.

  All eight were restored. The scripts are in `agent08/breakages/`; none is committed.
- **The first replay**, against the test module before D3, D2 and D5's third test, found B1 caught by
  nothing. It also found B7 raising `ZeroDivisionError` at α = 0, which led to D2. Its outputs are in
  `agent08/breakages/first/`. The record below is the final replay's.

**B1 — ln q from the drain point's N instead of N_turn (in the gatherer: the assessed candidates' N_turn passed as their N).** Fails 1: `test_4_every_generation_is_classical_and_born_below_zero` (20.343240432089264 != 20.34227113744418: the generation's N_turn is the drain point's). At M = 10⁻¹⁶ every candidate is a reflection, where N = N_turn, so item 3 passes. In the first replay, before D3, it failed nothing.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -3536,7 +3536,7 @@
                 MarginCandidate(
                     index=index,
                     kind=injection.kind,
-                    N_turn=injection.N_turn,
+                    N_turn=injection.N,
                     delta_K=injection.delta_K,
                     estimate=estimate.estimate,
                     log_k_ex=estimate.log_k_ex,
```

**B2 — a gap generation given no flag: `"classical"` in place of `"classical_gap"`.** Fails 2: `test_3_the_estimate_follows_R` ('classical' != 'classical_gap') and `test_3_the_counts_and_shares_of_log_07b` (0 != 3).
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -3255,7 +3255,7 @@
         )
     except ComputationFailureError as e:
         return PeakEstimate(
-            ESTIMATE_CLASSICAL_GAP, ct.log_k_ex, log_m_classical, ct.log_R, e.message
+            ESTIMATE_CLASSICAL, ct.log_k_ex, log_m_classical, ct.log_R, e.message
         )
     if turnaround is None:
         return PeakEstimate(
```

**B3 — the plateau turning points left out of the reflection margin (in the core).** Fails 3, all of item 1: `test_1_the_reflection_margins_and_the_plateau_that_binds` (−4.2 != −9.5: the wall event binds instead), `test_1_the_worst_margins` and `test_1_the_first_failures` (the first reflection failure moves from the plateau at 10.25 to the candidate at 10.5).
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -3353,7 +3353,7 @@
     # same N)
     all_events = sorted(
         [(float(c.N_turn), float(c.log_m)) for c in candidates]
-        + [(float(e.N), float(e.log_m)) for e in events],
+        + [(float(e.N), float(e.log_m)) for e in events if e.source != "plateau"],
         key=lambda e: e[0],
     )
     late = [(float(N), float(lm)) for N, lm in samples if N > N_late]
```

**B4 — the tracker margin over every sample after birth, reading A (in the core).** Fails 3, all of item 1: `test_1_the_tracker_margins_read_the_late_samples_and_today_only` (−51.5 != −3.0: the sample at N = 12.5, with ln m = 50, is read), `test_1_the_worst_margins` and `test_1_the_first_failures` (at 12.0 instead of 13.0). The gatherer still computes the tracker only after N_late, so on a real history B4 changes nothing; test 1's samples before N_late are what catch it.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -3356,7 +3356,7 @@
         + [(float(e.N), float(e.log_m)) for e in events],
         key=lambda e: e[0],
     )
-    late = [(float(N), float(lm)) for N, lm in samples if N > N_late]
+    late = [(float(N), float(lm)) for N, lm in samples]
     late.append((float(today[0]), float(today[1])))
 
     generations: List[Generation] = []
@@ -3398,7 +3398,7 @@
         reflection, reflection_N, reflection_first = minimum(
             log_q, all_events, c.N_turn
         )
-        tracker, tracker_N, tracker_first = minimum(log_q, late, float("-inf"))
+        tracker, tracker_N, tracker_first = minimum(log_q, late, c.N_turn)
         weighted: bool = share >= weight
         generations.append(
             Generation(
```

**B5 — the leftover counted as a generation, with its candidate's peak (in the gatherer).** Fails 2: `test_2_the_leftover_is_counted_only_as_unassessed` (the generations' indices gain a 50th, beyond the 49 candidates), and `test_3_a_gap_generation_is_born_classical` as an ERROR (IndexError: the extra generation's index is no injection's).
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -3597,7 +3597,10 @@
 
     K_unassessed: float = ledger.K_initial
     if result.parking is not None:
-        K_unassessed += result.parking.delta_K
+        cut = candidates[result.parking.candidate_index]
+        candidates.append(
+            cut._replace(index=len(candidates), delta_K=result.parking.delta_K)
+        )
 
     return relativistic_margins(
         candidates,
```

**B6 — the weight's comparison > in place of ≥.** Fails 1: `test_2_at_the_weight_it_is_included` (False is not true: a share of exactly 10⁻³ is left out).
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -3399,7 +3399,7 @@
             log_q, all_events, c.N_turn
         )
         tracker, tracker_N, tracker_first = minimum(log_q, late, float("-inf"))
-        weighted: bool = share >= weight
+        weighted: bool = share > weight
         generations.append(
             Generation(
                 index=c.index,
```

**B7 — the margins computed at α = 0 (the caller's condition without `alpha > 0.0`).** Fails 3: `test_6_the_golden_histories` (β = 1.5 and 5.5, two subtests) and `test_6_natural_mode_at_large_M`, each `() is not None`: at α = 0 there is no candidate, and the generations are an empty tuple. In the first replay, before D2, the core raised ZeroDivisionError there (K_final = 0), and the three were ERRORs.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -3922,7 +3922,7 @@
     generations: Optional[Tuple[Generation, ...]] = None
     margins: Optional[Margins] = None
     margin_warnings: Tuple[str, ...] = ()
-    if alpha > 0.0 and model.reflects_at_origin and model.log_V_floor is not None:
+    if model.reflects_at_origin and model.log_V_floor is not None:
         try:
             generations, margins = history_margins(result, sample, policy)
             margin_warnings = margin_warning_lines(margins)
```

**B8 — a failure in the margins propagated as the history's failure.** Fails 1 class: `TestAFailureInTheMargins` errors in `setUpClass` (the stub's ComputationFailureError leaves `integrate_history`, which catches it nowhere after the loop), so its 2 tests do not run (30 of 32 ran).
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -3927,6 +3927,7 @@
             generations, margins = history_margins(result, sample, policy)
             margin_warnings = margin_warning_lines(margins)
         except ComputationFailureError as e:
+            raise
             generations, margins = None, None
             margin_warnings = (f"{WARNING_MARGINS_FAILED}: {e.message}",)
 
```

## 7. Observations not acted on

- **The coverage condition on the parked tracker fails in the Σ wiggle.** On the late tracker at small M,
  d ln m_tr/dN reaches +3.6 at T_J = 22.8–30.1 keV. There d ln T/dN = +4.8, because Σ rises in the
  non-monotone band of `[00-xav-sigma-is-non-monotone-at-25-35-keV]`. Those samples lie after N_late,
  where the tracker margin is computed directly, so nothing rests on the condition. A released history
  whose parked segment crossed 23–35 keV with a generation before it would need the parked bound there,
  and none measured does. Not opened: it is that issue's effect, recorded against it here.
- **The parked bound's condition is tight at large β.** At M = 0.5, β = 25 the largest d ln m_tr/dN on the
  parked segment is −1.021: the trace term dominates and d ln T/dN = −1.97. At larger β the condition
  could fail, and §1.5's bound would then need the segment's measured margin. Not opened.
- **One one-loop solution lies at log₁₀ 𝓡 = 2.9964** (β = 2, M = 10⁻²⁰, candidate #10, ℓ = 0.341), which
  log 07b §5.3 printed as "≥ 3.00". The gap's upper edge is therefore at about 10^2.92–10^2.996. The
  prompt's stop (a raise at ≥ 3.00) is unaffected, and test 3's boundary of 2.92 is not crossed on its
  history.
- **`tune_history` prints the seed's margin lines**, not the converged history's (§5.6). The kinds agree
  on both tunings; the values and the worst generation can differ. This is 05's convention, kept by the
  prompt. A reader who wants the stored history's margins reads the payload (09's columns).
- **The worst margins at small M are set by the last generations.** At β = 2, 10⁻¹⁷ ≤ M ≤ 10⁻²⁰, the worst
  birth and reflection margins (−0.71, −0.56) belong to generation #17, which is classical. The failing
  shares (0.016–0.71) say how much of K that is. The worst margin alone overstates the failure there, so
  09's columns need the failing share beside it, as U9 has.
- **Item 3's shares lie close to their tolerance**: 4.4×10⁻⁵ and 4.8×10⁻⁵ against 5×10⁻⁵. They are
  deterministic, and a change of the integration would show up there first.

## 8. State handed to the next prompt

- **The payload keys** (`integrate_history`, not stored): `"generations"`, a tuple of `Generation`;
  `"margins"`, a `Margins`; and `"margin_warnings"`, a tuple of str. They are `None`, `None`, `()` at
  α = 0 or without a wall, and `None`, `None`, (one `WARNING_MARGINS_FAILED` line) where the margins
  failed. A failure payload has none of them. `"warnings"` is the loop's, unchanged.
  `compute_scalar_history`'s payload (tune_history's) carries them from the converged integration.
- **`Margins`**, its fields and types, with what 09 stores:

  | Field | Type | 09 |
  |---|---|---|
  | `worst_birth_margin`, `worst_reflection_margin`, `worst_tracker_margin` | float or None | column |
  | `worst_birth_margin_N`, `worst_reflection_margin_N`, `worst_tracker_margin_N` | float or None | column |
  | `worst_birth_margin_index`, `worst_reflection_margin_index`, `worst_tracker_margin_index` | int or None | JSON (with the first failures), or a column at 09's choice |
  | `birth_failing_share`, `reflection_failing_share`, `tracker_failing_share` | float | column |
  | `first_birth_failure`, `first_reflection_failure`, `first_tracker_failure` | `MarginFailure` or None | JSON (`_asdict()`) |
  | `weighted_share`, `below_weight_share`, `unassessed_share` | float | column (the unassessed share is R21's "reported beside the failing shares") |
  | `classical_generations`, `one_loop_generations`, `classical_gap_generations`, `weighted_generations`, `unassessed_candidates` | int | JSON, or columns at 09's choice |
  | `plateau_events`, `wall_events`, `late_points` | int | JSON or not stored |
  | `N_late` | float (−inf if no turning point or candidate) | JSON or not stored |

  `MarginFailure` is (`index` int, `N` float, `margin` float, `log_q` float, `log_m` float, `estimate` str).
- **`Generation`**: `index` int, `kind` str, `estimate` str, `N_turn`, `log_q`, `log_k_ex`, `log_m`,
  `log_R` and `share` floats, `weighted` bool, `birth_margin` float, `reflection_margin` and
  `reflection_margin_N` float or None, `tracker_margin` and `tracker_margin_N` floats, `gap_reason` str
  or None. Not stored by 09's charter.
- **The functions**: `peak_estimate`, `tracker_log_m`, `field_log_m`, `relativistic_margins`,
  `history_margins` and `margin_warning_lines`. The constants are `MARGIN_THRESHOLD = ln 10` and
  `MARGIN_WEIGHT_SHARE = 1e-3`, neither a key, and the estimates are `ESTIMATE_*`. The four warning
  kinds are in `WARNING_KINDS`.
- **The cost**: 4–83 ms per integration, 0.2–2.9 % of a history, dominated by the tracker's samples;
  34 and 91–157 ms per integration in the two tunings.
- **The measured state**, at α = 0.1, untuned: every history with a weighted classical or gap generation
  fails the birth margin (ℓ_t ≈ −0.70) and the reflection margin (−0.67 to +0.014); at M = 10⁻³, β = 0.5
  they are −3.0 and −2.4. The tracker passes except there (−2.2). At M ≤ 10⁻²² (β = 1.2, 2) and ≤ 10⁻²⁰ (β = 3) every margin passes.
- **Tests**: `ComputeTargets/tests/test_margins.py` (32).
- **Counts after this prompt:** CosmologyModels 26, ComputeTargets 471 (96 + 375), ObjectFactories 103,
  tools 115, RunDrivers 64. `CLAUDE.md`'s counts are still those of `tuning-followups` 02; this
  campaign's close (12) updates them.
- **Issues**: closed `[04-one-loop-chain-has-no-solution-just-above-R-equals-1]` (board §4, with its row
  removed from `.documents/OPEN_ISSUES.md` §1.19); opened none.
