# Log 02 — injection at turning points

**Subject:** Drain the condensate's kinetic energy at turning points
**Commit:** `fe2a944`, on branch `inelastic-reflection`, on top of `77af9c8`
**Date:** 2026-10-28, by Claude Opus 5.5
**Result:** **COMPLETE, after one stop.**
- §1.1–§1.5 shipped under orchestrator note 02, whose rulings 1–14 govern where they differ from the
  prompt.
- **The stop.** The §2 measurement found a step at β = 5, α = 0.1 with two sign changes of π. That is
  prompt §5's third condition, so I stopped without committing. The user ruled interior sampling
  (ruling 15, below), and the coordinator accepted the derived H bound in place of note 02's form
  (ruling 8a). The work was then finished under both.
- **The gate "Inject" holds.**
  - At α = 0 both golden histories pass unchanged and bitwise: `test_golden_history.py`, and
    `test_tuning.py` (e). This holds with `alt_segment=True` and the interior sampling.
  - At α > 0, H_E is continuous across every injection to a derived rounding bound. It was
    bitwise equal at 194 of the 195 drains measured, and 1.4e-14 at the other, against 97 ε.
  - The ledger equals the drains exactly (the same floats).
  - A floor-rule drain is exact: π_out = −√0.9 π_in bitwise.
- **Nothing a build stores moved.** No column, key, fingerprint format or version changed. No store
  was opened, no Ray started, nothing written to `var/`, and no golden JSON regenerated.
- **Counts:**
  - before: CosmologyModels 26, ComputeTargets 288 (96 + 192), ObjectFactories 103, tools 115,
    RunDrivers 64;
  - after: 26, **305** (96 + 209, +17, all in the new `test_injection.py`), 103, 115, 64.
  - All passing, before and after.
- Opened no issue and closed none. `.documents/OPEN_ISSUES.md` is unchanged.

## 1. The physics and the numerics

### 1.1 H_E is continuous across a drain (prompt §1.2)

With G = 1 − π²/6M_P², the constraint is 3M_P²H²G = V + ρ_R(1 + f_m) + ρ_χ. The drain changes only
π and ρ_χ: φ, ln ρ_R,E and ln f_m are untouched, so V and ρ_R(1 + f_m) are the same.
- **Before**, at H: 3M_P²H²(1 − π²/6M_P²) = V + ρ_R(1 + f_m) + ρ_χ.
- **After**, at the same H: 3M_P²H²(1 − (1 − α)π²/6M_P²) = [V + ρ_R(1 + f_m) + ρ_χ] +
  3M_P²H²·απ²/6M_P² = V + ρ_R(1 + f_m) + ρ_χ + α·½H²π² = V + ρ_R(1 + f_m) + (ρ_χ + αρ_kin).

So (√(1 − α)π, ρ_χ + αρ_kin) satisfies the constraint at the same H, exactly. H_J = H_E(1 +
(d ln A/dφ)π)/A jumps, because π does (P14). The floor rule's π_out = −√(1 − α)π_in has the same
π², so the same holds there.

### 1.2 The rounding bounds (§1.5 items 4 and 5; note 02 ruling 8, as amended by 8a)

**H's jump.** `HubblePolicy` rebuilds H² = exp(x)/3M_P², with x = ln V − ln(G·T) and
T = 1/(1 + (ρ_R/V)(1 + f_m + r_χ)) (or its other branch). ln V, ρ_R/V and f_m are the same floats
before and after. The two H therefore differ only through G, through r_χ and through the roundings
after them. On H², between the two:
- **G.** π_after carries 1.25 ε from √(1 − α) and the product. Its square and the division by
  6M_P² add ε, and 1 − s costs ε/2 of G. With c = (π_before²/6M_P²)/G_before, G is off its exact
  ratio by at most ε(4.5c + 1). This is the condition of G that ruling 8 named.
- **T.** Its sum, product and division cost 4 ε. r_χ = exp(ln K − 4N − ln ρ_R,E) is wrong by
  ε(|ln K| + 4|N| + |ln ρ_R,E| + 2), and it enters T with weight ω = ρ_χ/3M_P²H²G ≤ 1.
- **The drain's own ΔK.** It enters with weight αρ_kin/3M_P²H²G = α·c. It is wrong by item 5's
  bound plus twice H_before's own error, ε|x|.
- **The logs.** log(G·T) costs ε|ln(G·T)| over the two. **x is rounded to its float each time**:
  half an ulp of x, at most ε|x|/2, so ε|x| between the two.

H is half of H², and exp, the division by 3M_P² and sqrt add 2 ε to H. `test_injection.H_bound`
computes this per injection; it comes to 88–146 ε on the measured histories.

**Why not k·ε·(1 + c) (ruling 8a).** The term ε|x|, with |x| ≈ 180, is not of that form, and it
is the one that shows. At β = 25, α = 0.1, one injection has |ΔH/H| = 1.414e-14 at c = 9.7e-8. That
is 64 ε: one ulp of x (2^-45 at |x| between 128 and 256), halved for H. Any bound k·ε·(1 + c) would
need k ≥ 64 there. Every other drain measured has H_after == H_before exactly.

| β (α = 0.1) | drains | ΔH = 0 exactly | largest \|ΔH/H\| | c at it | bound at it |
|---|---|---|---|---|---|
| 1.2 | 7 | 7 | 0 | 0.138 | 2.35e-14 (106 ε) |
| 2 | 27 | 27 | 0 | 0.043 | 2.20e-14 (99 ε) |
| 5 | 86 | 86 | 0 | 0.0067 | 1.96e-14 (88 ε) |
| 25 | 75 | 74 | **1.414e-14** | 9.7e-8 | 2.15e-14 (97 ε) |
| floor, M = 1e-10 | 1 | 1 | 0 | 0.043 | 3.23e-14 (146 ε) |

The β rows' "c at it" and "bound at it" are those of the injection where ratio/bound is largest,
or of the first injection where every ratio is 0.

**ΔK against a recomputation (item 5).** The loop computes ΔK = exp(ln α + ln ρ_kin + 4N), with
ln ρ_kin = 2 ln H + 2 ln|π| − ln 2. The test recomputes it by products, αH²π²e^{4N}/2, which costs
about 4 ε. Each log carries an absolute rounding of ε/2 times its size, doubled by the factor 2.
Each of the four sums costs ε/2 of its partial result, and exp turns its argument's absolute error
into a relative one. So the bound, `delta_K_bound`, is
ε(2|ln H| + 2|ln|π|| + |ln ρ_kin| + |ln α| + 4|N| + |ln ΔK| + 8). At β = 2 the worst residual is
113 ε, against bounds of at least 541 ε. The ledger's entry equals the record's ΔK exactly.

## 2. What shipped

### `ComputeTargets/ScalarHistory.py`
- **The records.**
  - `TurningPoint(N, phi_Einstein, log_T_Jordan, kind)`, kind "wall" or "plateau".
  - `Injection(N, kind, N_turn, phi_turn, pi_before, pi_after, log_rho_kin, alpha_eff, delta_K,
    delta_rho_over_rho_rad, H_Einstein_before, H_Einstein_after)`, kind "turning_point" or
    "floor".
  - Both are module-level namedtuples with their fields in the prompt's order (ruling 11).
  - `IntegrationResult` gains `turning_points` and `injections`, appended last. Its comment block
    names them and says `solution` is right-continuous. Its one construction uses keywords.
  - New module constants: `LOG_2`, and `TURNING_POINT_INTERIOR_SAMPLES = 8` (ruling 15).
- **`integrate_scalar_history(..., alpha=0.0)`**, last.
  - `not (0.0 <= alpha < 1.0)` is a `ValueError` at entry, before any solver (ruling 10).
  - At α > 0 the ledger is `policy.dark_radiation`. It must not be `None`, and when `RHS` has a
    `policy`, its `dark_radiation` must be the same object. Either failing is a `ValueError` before
    the first solver (ruling 5).
  - **Detection**, `observe_turning_points()`, on every appended step: the last one up to
    N_final, and no step that is not appended (rulings 1, 3, 15).
    - π is read at the two ends by scalar calls, `interpolant(ts[k])[1]` and
      `interpolant(ts[k + 1])[1]`, and at the 8 interior points `np.linspace(t_low, t_high, 10)[1:-1]`
      in one vectorized call.
    - If every sample is strictly of one sign, the step has none; this is decided in numpy, as a
      fast path.
    - Otherwise the sign changes between consecutive samples are found as `surfing_turning_points`
      finds g's: a sample exactly 0.0 is a turning point if the signs either side differ, and is
      counted once.
    - One change with the two ends of opposite sign is bracketed by the whole step,
      `brentq(lambda t: interpolant(t)[1], t_low, t_high, xtol=1e-15)`: `first_bounce`'s call.
    - Two or more are each refined on their own sub-interval, whose ends are re-read by scalar
      calls. If those reads agree in sign (rounding), the end with the smaller |π| is taken, as
      `surfing_turning_points`' `refine` does.
    - φ and ln T_J are read from the interpolant at the root.
  - **The order after an accepted step** (ruling 3) is today's up to the append. The appended step
    is then classified, and its turning points are taken in order: a wall point arms a drain, and a
    plateau point drops one still armed.
    - If the step ends the history (the termination branch, or `finished` under `N_stop`), the
      loop ends with no drain.
    - Otherwise, if a drain is armed and `solver.y[1] > 0.0 and solver.f[1] <= 0.0` (ruling 2), it
      drains (at α_eff ≠ 0) and restarts, and the drain is disarmed.
    - A floor reflection drops an armed drain.
  - **`drain()`**:
    - H_before = `HubblePolicy(policy(N, y))`, from the ledger as it stands;
    - ln ρ_kin = 2 ln H + 2 ln|π| − ln 2, ΔK = exp(ln α_eff + ln ρ_kin + 4N) and
      Δρ/ρ_R = exp(ln α_eff + ln ρ_kin − ln ρ_R,E);
    - `ledger.add(N, ΔK)`;
    - π → sign·(√(1 − α_eff)·π), with sign = −1 for a floor event;
    - H_after from the drained state with the new ledger, and the `Injection` appended.

    The caller then builds the new Radau instance.
  - **The floor rule.** The guards run on π_in as today, then the `Reflection` and
    `notify_reflection` (ruling 4). At α_eff == 0.0 the restart is today's lines exactly
    (`y_reflected[1] = -pi_`); otherwise `drain(..., "floor", N, phi, -1.0)`.
  - **α_eff** is `alpha_effective()`, a local function that returns α. It is the one place 03
    multiplies in w_gate and w_rate.
  - **The result's solution** is `OdeSolution(ts, interpolants, alt_segment=True)` (ruling 6).
  - The docstring describes all of the above.
- **`integrate_history(..., alpha=0.0)`**, last.
  - The range check comes before any integration, and it is the function's first statement.
  - It passes `alpha=alpha` to `integrate_scalar_history`.
  - The success payload gains `"turning_points"` and `"injections"`. `"reflections"` stays the count
    of floor events.
- **`tune_history(..., alpha=0.0)`**, last. The range check sits beside the `progress` check, before
  the seed, and it passes `alpha=alpha` to every integration.
- **Unchanged, byte for byte:** `compute_scalar_history`, `build_extra_data`, `ScalarHistory.store`,
  `first_bounce`, `surfing_turning_points`, `fixed_T_value_at` and `_tuning_state`. None is in the
  diff.
- **Dropped drains** are not a field (ruling 11 names two new fields). At α > 0 in this prompt they
  are the wall-side turning points minus the injections of kind "turning_point".

**The payload on the way to the store, read again (prompt §1.4).** `build_extra_data` reads eight
named keys through `store_attr` and the three RHS-statistics keys. `ScalarHistory.store` reads
`failure`, `failure_reason`, `first_bounce`, `fixed_T_values`, the `TUNING_OUTPUT_KEYS`, `metadata`,
`sample`, `z_grid` and `solver_label` by name. Nothing iterates over the payload's keys, so the two
new keys are not stored. The payload pickles (item 10).

**Readers of `IntegrationResult` and `result.solution`**, checked with `git grep`:
- `first_bounce`, `surfing_turning_points`, `_T_Jordan_crossing_step` and `fixed_T_value_at` read
  `solution.ts` and `solution.interpolants` directly, so the segment side does not reach them.
- `integrate_history` calls `solution(N_forward)` at the samples and at N_final. N_final is ts[-1],
  where the index is clamped, so both sides give the same segment.
- `tools/plot_surfing_histories.py` calls `solution(np.linspace(ts[0], N_end, n))`.
- The tests' wrappers of `integrate_scalar_history` and `integrate_history` take `*args, **kwargs`
  or are `MagicMock`s, so the new keyword reaches them. No existing test was edited.

### The tests (`ComputeTargets/tests/test_injection.py`, new; 17 tests, about 6 s at a load of 6–25)
Each bound is stated as a number, and its docstring says why it is rounding.
- **`TestAlphaZeroIsTheOldCode`** (golden β = 1.5, one integration, about 1 s):
  - `test_1_no_injection_and_the_first_wall_turning_point_is_first_bounce`. The ledger is empty,
    the injections are empty in the result and the payload, and there are no reflections. The first
    wall `TurningPoint` equals `first_bounce` in N, φ and ln T_J, exactly.
- **`TestARealHistoryAtAlpha`** (M = 0.5, β = 2, α = 0.1, the planner's setup, with the RHS
  recorded; one integration, about 2 s):
  - `test_2_the_turning_points_are_classified`: each point lies strictly inside its step, with
    π(t_old) < 0 < π(t) for a wall point and the reverse for a plateau one. The kinds alternate,
    and there are at least 10 wall points.
  - `test_2_one_injection_per_wall_side_turning_point_and_none_after_a_plateau`.
    - Every injection is "turning_point" at α_eff = 0.1, and its N_turn and φ_turn are those of a
      wall point.
    - Each wall point that is not the history's last is followed by exactly one injection before
      the next turning point; the last by at most one.
    - No injection follows a plateau point.
  - `test_3_the_drain_point_is_the_first_step_end_past_the_maximum`.
    - At each drain node π_before > 0 and π′ ≤ 0. Between the turning point's own step end and the
      drain node, every step end has π > 0 and π′ > 0.
    - π′ is ODERHS's friction + reflecting + kicking. It is evaluated at the accepted state, rebuilt
      exactly: the drained state at the node with π_before put back, or the start of the next
      interpolant. The ledger is replayed to how it stood then, so π′ is `solver.f[1]` bitwise.
    - It also asserts that not every drain is on the turning point's own step: at β = 2 none of the
      27 is.
  - `test_4_H_Einstein_is_continuous_across_every_injection`: |ΔH/H| ≤ `H_bound` (§1.2). It writes
    the largest ratio, with its c and bound, to stderr (ruling 8a). Here that is 0.0 at c = 0.043,
    bound 99 ε.
  - `test_5_the_ledger_is_the_sum_of_the_drains`: `ledger.injections == ((i.N, i.delta_K), …)`
    exactly; `pi_after == sqrt(1.0 - 0.1) * pi_before` exactly; and ΔK against the products to
    `delta_K_bound`.
  - `test_6_the_dense_output_at_a_drain_node_is_the_drained_state`.
    - `result.solution(N_j)` equals interpolant k + 1 at its start, elementwise and exactly, with
      π == π_after.
    - H_E rebuilt there with the ledger equals `H_Einstein_after` exactly.
    - SciPy's default `OdeSolution(ts, interpolants)` gives a π ≠ π_after, within 1e-6·|π_before −
      π_after| of π_before.
  - `test_7_every_RHS_evaluation_after_a_drain_sees_the_new_K` (35 642 calls recorded).
    - The new instance's first call is identified as (N_j, π_after).
    - From it until the next drain's, every call is at N ≥ N_j and sees K == K(N_j), the drain
      included.
  - `test_10_the_payload_pickles_with_its_records`: the copy has the same `injections`,
    `turning_points` and ledger, and `reflections == 0`.
- **`TestTwoTurningPointsOnOneStep`** (M = 0.5, β = 5, α = 0.1; one integration, about 3 s;
  ruling 15):
  - `test_11_a_plateau_and_wall_pair_inside_one_step_is_recorded`.
    - At least one step holds two turning points, [plateau, wall], both strictly inside it, with
      π > 0 at both ends.
    - π changes sign across ±1e-6 of a step at each point.
    - The records alternate, and the injections' N_turn are exactly the wall points.
  - `test_11_no_step_holds_more_sign_changes_than_the_loop_recorded`: at 17 points, with a 0.0
    sample not counted as a sign, no step shows more changes than the loop recorded on it. This is
    ruling 15's narrowed stop, as a test.
- **`TestAFloorRuleDrain`** (`test_kinematic_cap_loop`'s P1 harness at M = 1e-10, with a ledger;
  about 0.2 s each):
  - `test_8_the_floor_reflection_drains`.
    - There is one `Reflection` and one injection of kind "floor", with N = N_turn = the
      reflection's N and φ_turn = its φ.
    - π_before == π_in, and π_after == −(√0.9·π_in) exactly, which is also `solution(N)[1]`.
    - The ledger equals the drains; ΔK is checked against `delta_K_bound` and H against `H_bound`.
  - `test_8_at_alpha_zero_the_run_is_todays`: with a ledger and α = 0, there is no injection and
    the ledger stays empty. `solution(N)[1] == −π_in`, and the reflections, nfev, accepted steps and
    final state equal `kcl.integrate`'s run exactly.
- **`TestTheParameter`** (milliseconds):
  - `test_9_integrate_scalar_history_refuses_a_bad_alpha_before_any_solver`: −0.1, 1.0, 1.5 and NaN
    raise `ValueError`, with `SM.Radau` patched and not called.
  - `test_9_alpha_needs_the_ledger_the_RHS_reads`: a policy with no ledger raises, and so does a
    policy whose ledger is not the RHS's. Radau is not called.
  - `test_9_integrate_history_refuses_before_any_integration`: `integrate_scalar_history` is
    patched and not called, with `None` inputs that would fail later.
  - `test_9_tune_history_refuses_before_the_seed`: `integrate_history` is patched and not called,
    and the model is a `Mock` whose `seed` is not called.

### The documents (P12)
- `.documents/numerical-strategies.md`: new §6.2, dated 2026-10-28, after §6.1. It covers the
  detection with its interior samples, the drain point, the drain and its order, the floor rule,
  H's continuity and its one-ulp jump, the dense output's side, the cost at α = 0, and the counts.
- `.documents/architecture-summary.md`: a dated note at the end of §7.2, after 01's. It covers the
  parameter, the two records, `IntegrationResult`'s new fields and side, and the payload keys.

## 3. Deviations from the prompt

**The rulings of orchestrator note 02, and the two that followed the stop:**

| # | Deviation | Class |
|---|---|---|
| R1 | Classification reads π on the interpolant at ts[k] and ts[k + 1] (N_final on the last step), with the strict inequalities and `first_bounce`'s brentq call, on every appended step and no other | STRUCTURALLY REQUIRED (item 1's bitwise equality) |
| R2 | The drain-point test reads the accepted state, `solver.y[1] > 0.0 and solver.f[1] <= 0.0`, after the step and before any restart. A rounding disagreement with the interpolant leaves the drain armed | IMPLEMENTATION CHOICE (ruled) |
| R3 | Order after a step: today's up to the append; classify; the history's last step ends with no drain; otherwise drain if due, then restart. No appended interpolant is touched | STRUCTURALLY REQUIRED |
| R4 | No change to `Quadrature/supervisors/`. `notify_reflection` is for floor events only, and restarts are counted from the records (floor reflections + "turning_point" injections) | IMPLEMENTATION CHOICE (ruled) |
| R5 | At α > 0 the ledger must exist and be the RHS's policy's ledger, else a `ValueError` before the first solver. Not checked at α = 0 | IMPLEMENTATION CHOICE (ruled) |
| R6 | `alt_segment=True`; item 6 asserts the drained state bitwise | STRUCTURALLY REQUIRED |
| R7 | A temperature crossing exactly at a drain node is recorded under §6, not fixed | as ruled |
| R8 → 8a | Item 4's bound is the derived term-by-term bound of §1.2 (`H_bound`), not k·ε·(1 + c). Its exponent term ε\|x\|, \|x\| ≈ 180, cannot be covered by a small k: the β = 25 drain at 1.414e-14, c = 9.7e-8, needs 64 ε. The test reports the largest ratio and c. Ruled by the coordinator (8a) after my stop report | STRUCTURALLY REQUIRED |
| R9 | No α = 0 count differs from §0's table. The only node with π exactly 0.0 is step 0's start (π* = 0), which neither the strict test nor the probe's counts | as ruled |
| R10 | `alpha` last, passed by keyword; the check is `not (0.0 <= alpha < 1.0)`. `compute_scalar_history`, `build_extra_data` and `store` are unchanged. No existing test failed | as ruled |
| R11 | Module-level namedtuples, the prompt's field order, the comment block naming the fields; `"reflections"` stays the floor count | as ruled |
| R12 | The measurement extends the planner's probe in my scratch directory, with the same setup | as ruled |
| R13 | Timing: three back-to-back alternations of HEAD (a scratch worktree of `77af9c8`, since removed) and the change | as ruled |
| R14 | Suites serial, output to scratch, `uptime` beside each | as ruled |
| **R15** | **Interior sampling of π, a deviation from prompt §1.1's end-point test.** The user chose "Interior sampling" from the coordinator's three options, after my stop at β = 5, α = 0.1, step 6051. 8 interior points in one vectorized call, the ends read by ruling 1's scalar calls; one end-to-end change bracketed by the whole step; two or more refined on their sub-intervals; zeros as `surfing_turning_points`. `first_bounce` is unchanged. The stop condition is narrowed to a step whose 17-point count exceeds the loop's record. A new test (item 11) and breakage B9 | STRUCTURALLY REQUIRED (ruled by the user) |

**My own deviations:**

| # | Deviation | Class |
|---|---|---|
| D1 | Dropped drains are not recorded as a field. They are derived: at α > 0, wall points minus "turning_point" injections. Ruling 11 names two new fields; 03 must count them if a weight can make α_eff = 0 at a wall point | IMPLEMENTATION CHOICE |
| D2 | A second wall point while a drain is armed re-arms it on the new one (the old one is dropped). It cannot happen while the kinds alternate | IMPLEMENTATION CHOICE |
| D3 | ΔK and Δρ/ρ_R are computed in logs, with ln ρ_kin recorded, so that nothing overflows at large N or underflows at small H | IMPLEMENTATION CHOICE (the prompt allows it) |
| D4 | A numpy fast path in the detection: a step whose ten samples are all strictly of one sign returns at once, without building the sample lists. It gives the same result as the loop (the measurement was identical before and after it). Its effect on the golden timings is inside their noise (§4) | IMPLEMENTATION CHOICE |
| D5 | When a sub-interval's scalar re-reads agree in sign, the end with the smaller \|π\| is taken, as `surfing_turning_points` does, so brentq never sees an unbracketed interval | IMPLEMENTATION CHOICE |
| D6 | Extra assertions beyond §1.5: test 3 checks every step end between the turn and the drain, not only the last; test 6 checks the drained state elementwise; test 8 checks N_turn and φ_turn; test 9 checks a mismatched ledger, NaN, and that `model.seed` is not called | IMPLEMENTATION CHOICE |
| D7 | Test 4 writes its largest ratio to stderr, one line, which is how it "reports" (ruling 8a) | IMPLEMENTATION CHOICE |
| D8 | B6 is written so that it isolates the order. The drain is made as shipped; the ledger's last entry is taken out, the new Radau instance is built, and the entry is put back. So H_after and ΔK are unchanged, and only the restart's view of K differs | IMPLEMENTATION CHOICE |

No UNINTENDED DRIFT. Before the stop, two of my tests raised errors under B1 rather than failing
(log(0) in the bound with an empty ledger, and an index into an empty tuple). I fixed them: the bound
takes ln K = 0 at K = 0, and the tuple is compared before it is indexed.

## 4. Verification performed

**Suites**, serially, ComputeTargets in two halves. "Before" was at `77af9c8` before any edit;
"after" was on the final tree.

| Suite | Before (count, wall, 1-min load before → after) | After |
|---|---|---|
| CosmologyModels | 26 OK, 65 s, 11.1 → 13.6 | 26 OK, 71 s, 24.0 → 11.5 |
| ComputeTargets [a-f] | 96 OK, 120 s, 13.6 → 12.1 | 96 OK, 130 s, 11.5 → 8.2 |
| ComputeTargets [g-z] | 192 OK, 242 s, 12.1 → 14.2 | **209** OK, 339 s, 8.2 → 32.6 |
| ObjectFactories | 103 OK, 17 s, 14.2 → 13.8 | 103 OK, 20 s, 32.6 → 31.9 |
| tools | 115 OK, 114 s, 13.8 → 20.4 | 115 OK, 135 s, 31.9 → 13.6 |
| RunDrivers | 64 OK, 25 s, 20.4 → 16.9 | 64 OK, 27 s, 13.6 → 26.5 |

- The baselines are board §5.1's.
- ComputeTargets rises by 17 (`test_injection.py` sorts into [g-z]), and nothing fell.
- `black --check` on the two changed Python files: both unchanged.

**The golden tests alone, before any edit** (load 17): `test_golden_history` passed, 3 tests in
9.4 s (11.0 s real), and the tuned golden test 22.9 s (24.2 s real).

**α = 0 is bitwise** (order 2 and 3 of the note). The golden tests, the tuned golden test,
`test_first_bounce`, `test_surfing_turning_points` and `test_kinematic_cap_loop` passed unedited,
35 tests, once with the end-point detection and `alt_segment=True`, and again with the interior
sampling. Item 1 holds on the golden β = 1.5 history under ruling 15.

**The cost at α = 0 (ruling 13).** The unittest "Ran … in" times of `test_golden_history` and of
`test_tuning.TestAConvergedHistory.test_e_the_tuned_golden_history`, alternating HEAD (`77af9c8`, a
scratch worktree) and the final tree, three rounds, at a 1-minute load of 4.9–15 (one spike to 38
after the last run):

| | HEAD (s) | min | change (s) | min |
|---|---|---|---|---|
| golden (two histories) | 9.73, 10.04, 9.96 | **9.73** | 10.76, 11.50, 10.91 | **10.76** |
| tuned golden (tuning + test e) | 30.35, 23.21, 29.02 | **23.21** | 28.64, 25.27, 28.01 | **25.27** |

- **This is measurable**, about +1.0 s (+11%) on the two golden histories. The change's minimum is
  above HEAD's maximum.
- On the tuned test it is +2.1 s (+9%) on the minima, inside HEAD's own spread of 7 s at that load.
- An earlier set, before D4's fast path (load 4–12), gave golden 9.44 → 10.70 s and tuned 23.31 →
  25.88 s on the minima.
- **Microbenchmark** on the golden β = 5.5 interpolants (26 331 steps), best of 3: the two scalar
  end reads cost 9.6 µs a step, and the end reads plus the interior sample and the fast-path test
  19.7 µs. So the interior sampling costs about 10 µs a step, 0.27 s on that history.
- **`brentq` calls added** (counted by patching `SM.brentq` around `golden_history.run_history`):
  - β = 1.5: 2 → 8, +6 (3 wall, 3 plateau);
  - β = 5.5: 3 → 448, +445 (223 wall, 222 plateau).

  Every turning point on the golden histories is a single end-to-end change, so the interior
  sampling adds no `brentq` call there.

## 5. The measurement (prompt §2)

The setup is the planner's (§0), copied and extended in my scratch directory (`measure.py`):
- M = 0.5 M_P, n = 1, at the seed realization, untuned;
- φ* = 5 M_P, π* = 0, T* = 2×10⁴ GeV, to T_CMB;
- atol = rtol = 1e-8, main.py's axis.

The numbers are from the final tree. The counts were identical on the tree before D4's fast path.
"Probe" counts are the planner's own loop over the step ends. "≥ 2 at 17" is the steps whose
interpolant, sampled at 17 points, holds two or more sign changes, by the probe's `np.sign` test.
"Loop ≥ 2" is the steps on which the loop recorded two or more turning points. "17 > loop" is the
steps whose 17-point count (zero-aware) exceeds the loop's record, which is the narrowed stop.

**α = 0** (load 23–37):

| β | wall time | accepted steps | RHS evaluations | floor | wall-side | plateau | probe wall/plateau | ≥ 2 at 17 | loop ≥ 2 | 17 > loop | first / last wall-side T_J |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1.2 | 1.2 s | 2 177 | 19 106 | 0 | 9 | 8 | 9 / 8 | 0 | 0 | 0 | 231 MeV / 12 eV |
| 2 | 1.8 s | 3 725 | 33 838 | 0 | 23 | 22 | 23 / 22 | 0 | 0 | 0 | 747 MeV / 62 keV |
| 5 | 5.9 s | 11 887 | 111 979 | 0 | 141 | 140 | 141 / 140 | 0 | 0 | 0 | 176 GeV / 169 keV |
| 25 | 9.5 s | 22 344 | 204 798 | 0 | 353 | 352 | 353 / 352 | 0 | 0 | 0 | 1.06 TeV / 6.5 keV |

- **Every count equals prompt §0's table**: steps, floor reflections, wall-side and plateau points,
  and steps with ≥ 2 changes.
- The wall times are at a load of 23–37, against the planner's 5. The first α = 0 run, at a load of
  25–29, gave 0.8, 1.4, 4.5 and 8.5 s.
- At α = 0 there are no injections, no restarts, and Σ Δρ/ρ_R = r_χ = 0 everywhere.
- The only step end with π exactly 0.0 is step 0's start (π* = 0), on every history (ruling 9).

**α = 0.1** (load 23–39):

| β | wall-side | plateau | injections (turning point / floor) | dropped | floor | restarts | accepted steps | RHS evaluations | wall time | Σ Δρ/ρ_R | r_χ at 1 MeV | r_χ at 70 keV | r_χ at the end | ≥ 2 at 17 | loop ≥ 2 | 17 > loop |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1.2 | 7 | 6 | 7 / 0 | 0 | 0 | 7 | 1 935 | 17 387 | 0.9 s | 1.50e-2 | 1.008e-2 | 8.82e-3 | 8.78e-3 | 0 | 0 | 0 |
| 2 | 27 | 26 | 27 / 0 | 0 | 0 | 27 | 3 836 | 35 642 | 1.7 s | 1.25e-2 | 8.16e-3 | 6.95e-3 | 6.93e-3 | 0 | 0 | 0 |
| 5 | 86 | 85 | 86 / 0 | 0 | 0 | 86 | 7 494 | 71 156 | 3.3 s | 1.98e-3 | 9.65e-4 | 8.09e-4 | 8.06e-4 | **1** | **1** (step 6051) | 0 |
| 25 | 75 | 74 | 75 / 0 | 0 | 0 | 75 | 12 843 | 117 973 | 6.1 s | 4.39e-4 | 2.07e-4 | 1.74e-4 | 1.73e-4 | 0 | 0 | 0 |

- **The drains differ in number from α = 0's wall-side points**: 7 against 9, 27 against 23, 86
  against 141 and 75 against 353. Each drain changes the trajectory after it. The first wall-side
  T_J is unchanged to 4 digits, and the last moves to 4.7 keV, 57 keV, 0.70 MeV and 6.6 keV for
  β = 1.2, 2, 5 and 25.
- **The largest single Δρ/ρ_R** is the first drain: 1.38e-2 (β = 1.2), 4.30e-3, 6.69e-4 and 3.39e-5
  (β = 25).
- **No drain was dropped, none was made on the turning point's own step (0 of 7, 27, 86 and 75),
  and no floor reflection occurred.** So restarts = injections.
- **The pair at β = 5** (ruling 15) is on step 6051, [42.888058853998494, 42.89345717089215],
  h = 5.40e-3, at T_J ≈ 0.705 MeV and φ ≈ 0.0083 M_P.
  - π is 1.87e-6 at the step's start and 1.19e-6 at its end, and dips to −1.01e-6 between.
  - At 401 points the changes are near N = 42.88922 (plateau) and 42.89266 (wall).
  - Before ruling 15 the loop saw neither: 85 wall, 84 plateau, 85 injections, 7 487 steps. Now it
    records both, and the wall point's drain adds one injection and 7 steps.
  - The probe's end-point loop still counts 85 / 84. This is the one place the probe and the
    records differ, by design.
- r_χ falls by 12–16% between 1 MeV and 70 keV on each history.

**The tuning at β = 2, α = 0.1** (`tune_history` from the model's seed, `progress="every"`; load
25–30): **it converges**, in 11 integrations (the seed, 1 plain and 9 secant steps, no guard), in
18.7 s.

| it | kind | H/H₀ − 1 | φ_ref/φ_min − 1 |
|---|---|---|---|
| 0 | seed | +9.186e-1 | −7.065e-2 |
| 1 | plain | −1.112e-1 | +3.821e-1 |
| 2 | secant | −5.780e-2 | +2.630e-1 |
| 3 | secant | +7.779e-2 | −8.800e-2 |
| 4 | secant | −1.895e-2 | +9.286e-2 |
| 5 | secant | −3.054e-4 | +2.853e-2 |
| 6 | secant | +2.297e-3 | +8.567e-3 |
| 7 | secant | +6.413e-4 | +5.020e-3 |
| 8 | secant | +4.528e-6 | −6.166e-4 |
| 9 | secant | −6.973e-7 | +4.734e-5 |
| 10 | secant | +5.275e-8 | −1.954e-7 |

The final history has 28 wall-side turning points and 28 injections. The H residual is not
monotone: integration 6 moves away from H₀ (+2.3e-3 after −3.1e-4). Whether the residual is continuous in the
normalization across a change in the number of drains is 03's to measure (§2's note).

## 6. The deliberate-breakage record (P11)

- Each diff was written against the final tree by a script (`scratchpad/agent-02/make_breakages.py`,
  difflib).
- Each was checked with `git apply --check`, applied, and checked with `git apply -R --check`. It
  was then run in bash (`replay_breakages.sh`) through `test_injection` and `test_golden_history`,
  plus the tuned golden test for B8. Each was reverted, and the source's md5 was compared with the
  final tree's after each. All nine were restored.
- None is committed. The record below is the final replay, against the final test file.
- Item 11's pair test fails under B1, B4 and B6 too. Those breakages change the β = 5 trajectory,
  so the near-stall pair no longer falls inside one step ("0 not greater than or equal to 1"). That
  failure is incidental. Each breakage's own failure is the one named first.

**B1 — drain π without adding ΔK to the ledger.** It fails 4 tests:
- `test_5_the_ledger_is_the_sum_of_the_drains`: `() != ((20.343240432089264, 4.8359540766999365e-43), …)`;
- `test_4_H_Einstein_is_continuous_across_every_injection`: 2.14e-3 against 2.18e-14;
- `test_8_the_floor_reflection_drains`: `() != ((20.352089723400805, 1.861142491350521e-40),)`;
- `test_11_a_plateau_and_wall_pair_inside_one_step_is_recorded` (incidental).
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -858,7 +858,7 @@
         log_alpha: float = log(alpha_eff)
         delta_K: float = exp(log_alpha + log_rho_kin + 4.0 * N_)
         delta_rho_over_rho_rad: float = exp(log_alpha + log_rho_kin - float(y_[2]))
 
-        ledger.add(N_, delta_K)
 
         y_drained = y_.copy()
         y_drained[1] = sign * (sqrt(1.0 - alpha_eff) * pi_before)
```

**B2 — ΔK computed from π after the drain.** It fails 3 tests:
- `test_5_the_ledger_is_the_sum_of_the_drains`: ΔK off its recomputation by 0.09999999999998721,
  against 1.2e-13;
- `test_4_H_Einstein_is_continuous_across_every_injection`: 2.14e-4 against 2.20e-14;
- `test_8_the_floor_reflection_drains`: ΔK off by 0.10000000000000187, against 1.16e-13.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -854,7 +854,11 @@
 
         # rho_kin,E = H_E^2 pi^2/2 and Delta K = alpha_eff rho_kin,E e^{4N}, in logs, so that no
         # intermediate overflows or underflows
-        log_rho_kin: float = 2.0 * log(H_before) + 2.0 * log(abs(pi_before)) - LOG_2
+        log_rho_kin: float = (
+            2.0 * log(H_before)
+            + 2.0 * log(abs(sqrt(1.0 - alpha_eff) * pi_before))
+            - LOG_2
+        )
         log_alpha: float = log(alpha_eff)
         delta_K: float = exp(log_alpha + log_rho_kin + 4.0 * N_)
         delta_rho_over_rho_rad: float = exp(log_alpha + log_rho_kin - float(y_[2]))
```

**B3 — plateau turnarounds drain too** (armed, with the mirrored drain point π < 0, π′ ≥ 0). It
fails 3 tests:
- `test_2_one_injection_per_wall_side_turning_point_and_none_after_a_plateau`: an injection's
  N_turn = 20.844264130404422 is not a wall point's;
- `test_3_the_drain_point_is_the_first_step_end_past_the_maximum`: π_before = −0.42 at a drain;
- `test_11_a_plateau_and_wall_pair_inside_one_step_is_recorded`: the injections' N_turn are not the
  wall points.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -1076,7 +1076,7 @@
         # 15), in order along the step: a wall-side one arms a drain, and a plateau turnaround
         # drops one still armed
         for turning_point in observe_turning_points():
-            armed = turning_point if turning_point.kind == "wall" else None
+            armed = turning_point
 
         if solver.status == "finished":
             if stop_at_bound:
@@ -1091,7 +1091,10 @@
 
         # the drain point (README §6.2 P3; orchestrator note 02, ruling 2): the first accepted state
         # since the wall-side turning point with pi > 0 and pi' <= 0, read before any restart
-        if armed is not None and solver.y[1] > 0.0 and solver.f[1] <= 0.0:
+        if armed is not None and (
+            (armed.kind == "wall" and solver.y[1] > 0.0 and solver.f[1] <= 0.0)
+            or (armed.kind == "plateau" and solver.y[1] < 0.0 and solver.f[1] >= 0.0)
+        ):
             alpha_eff = alpha_effective()
             if alpha_eff != 0.0:
                 y_drained = drain(
```

**B4 — drain at the end of the turning point's own step, whatever π′.** It fails 2 tests:
- `test_3_the_drain_point_is_the_first_step_end_past_the_maximum`: π′ = 2517.5 > 0 at the first
  drain, where π_before = 6.9e-3;
- `test_11_a_plateau_and_wall_pair_inside_one_step_is_recorded` (incidental).
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -1091,7 +1091,7 @@
 
         # the drain point (README §6.2 P3; orchestrator note 02, ruling 2): the first accepted state
         # since the wall-side turning point with pi > 0 and pi' <= 0, read before any restart
-        if armed is not None and solver.y[1] > 0.0 and solver.f[1] <= 0.0:
+        if armed is not None:
             alpha_eff = alpha_effective()
             if alpha_eff != 0.0:
                 y_drained = drain(
```

**B5 — the floor reflection without √(1 − α).** It fails 1 test:
- `test_8_the_floor_reflection_drains`: π_after = 0.49765115381152175, not 0.47211333787655796.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -861,7 +861,9 @@
         ledger.add(N_, delta_K)
 
         y_drained = y_.copy()
-        y_drained[1] = sign * (sqrt(1.0 - alpha_eff) * pi_before)
+        y_drained[1] = (
+            -pi_before if kind == "floor" else sign * (sqrt(1.0 - alpha_eff) * pi_before)
+        )
         H_after: float = H_Einstein_at(N_, y_drained)
 
         injections.append(
```

**B6 — `ledger.add` after `new_solver`** (D8: the order alone). It fails 2 tests:
- `test_7_every_RHS_evaluation_after_a_drain_sees_the_new_K`: the new instance's first call at
  N = 20.343240432089264 sees K = 0.0, not 4.8359540766999365e-43;
- `test_11_a_plateau_and_wall_pair_inside_one_step_is_recorded` (incidental).
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -1103,7 +1103,11 @@
                     armed.phi_Einstein,
                     1.0,
                 )
+                entry = ledger.injections[-1]
+                for held in (ledger._N, ledger._delta_K, ledger._K, ledger._log_K):
+                    held.pop()
                 solver = new_solver(solver.t, y_drained)
+                ledger.add(*entry)
             armed = None
 
     return IntegrationResult(
```

**B7 — `OdeSolution` without `alt_segment=True`.** It fails 3 tests:
- `test_6_the_dense_output_at_a_drain_node_is_the_drained_state`: `solution(N_j)[1]` =
  0.49712396001373366 (π_before), not 0.47161319792576056;
- `test_8_the_floor_reflection_drains`: −0.4976… (π_in), not +0.4721…;
- `test_8_at_alpha_zero_the_run_is_todays`: −0.4976…, not +0.4976….
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -1113,7 +1113,7 @@
         # right-continuous at the nodes (inelastic-reflection prompt 02, §1.4): at a drain or a
         # reflection it returns the state after it, which the ledger's N_j <= N pairs with the new
         # K. At alpha = 0 only a query at an interior node exactly could see a difference
-        solution=OdeSolution(ts, interpolants, alt_segment=True),
+        solution=OdeSolution(ts, interpolants),
         N_final=N_final,
         final_state=final_state,
         nfev=nfev,
```

**B8 — at α = 0, a restart at each would-be drain point with y unchanged.** **The golden histories
fail on it, so a restart is not free at α = 0.** It fails 3 tests:
- `test_history_beta_1_5`: accepted steps 1233 → 1237, RHS evaluations 9870 → 10208. Sample 0's
  H_Einstein differs (`0x1.c1f13d990de81p-71` → `0x1.c1f129c0239f5p-71`), as do the samples' SHA and
  `fixed_T_values`.
- `test_history_beta_5_5`: steps 26331 → 26395, RHS evaluations 232530 → 247125. Sample 0's
  H_Einstein differs (`0x1.69ce2539312c0p-13` → `0x1.69ce2bd849ea7p-13`).
- `test_e_the_tuned_golden_history`: RHS evaluations 140823 → 146039. Sample 0's H_Einstein differs
  (`0x1.da9f8667e9461p-50` → `0x1.da9f7fe0ccfa5p-50`).

A restart discards Radau's step-size history and its Jacobian. The new instance starts from
`select_initial_step` and a fresh finite-difference Jacobian, so the step sequence, and every value
after it, changes.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -1104,6 +1104,8 @@
                     1.0,
                 )
                 solver = new_solver(solver.t, y_drained)
+            else:
+                solver = new_solver(solver.t, solver.y.copy())
             armed = None
 
     return IntegrationResult(
```

**B9 — detection at the step ends only** (ruling 15). It fails 2 tests:
- `test_11_a_plateau_and_wall_pair_inside_one_step_is_recorded`: no step holds two turning points;
- `test_11_no_step_holds_more_sign_changes_than_the_loop_recorded`: step 6051 shows 2 changes at 17
  points, and the loop recorded 0.
```diff
--- a/ComputeTargets/ScalarHistory.py
+++ b/ComputeTargets/ScalarHistory.py
@@ -912,8 +912,8 @@
         ]
         pi_interior = interpolant_(N_interior)[1]
 
-        Ns = [t_low] + [float(x) for x in N_interior] + [t_high]
-        pis = [pi_low] + [float(x) for x in pi_interior] + [pi_high]
+        Ns = [t_low, t_high]
+        pis = [pi_low, pi_high]
 
         # the sign changes between consecutive samples: (j_before, j_after, j_zero or None, kind),
         # where j_zero is the first sample exactly 0.0 since the last non-zero one
```

## 7. Observations not acted on

- **The loop's first wall `TurningPoint` and `first_bounce` can differ** (ruling 15).
  `first_bounce` is unchanged, because it feeds stored values, and it reads step ends only. If an
  interior plateau-and-wall pair ever came before the first end-to-end wall point, the loop would
  record that pair's wall point first, and `first_bounce` would return the later one. It does not
  happen on the golden β = 1.5 history (item 1), nor on any history measured here: the one pair is
  at β = 5, α = 0.1, step 6051, long after the first bounce. No issue is opened.
- **A temperature crossing exactly at a drain node** (ruling 7). `_T_Jordan_crossing_step` returns
  `(k, t_high)` when ln T_J − log_T is exactly 0.0 at a step's end. `fixed_T_value_at` then reads
  `interpolants[k]` at t_high, which is the state before the drain, paired by `policy` with the K
  after it. This needs ln T_J at a drain point to equal 1 MeV or 70 keV bitwise. Not fixed, and no
  issue opened.
- **The detection costs about 10% at α = 0** on the golden histories (§4), about 20 µs per
  accepted step, half of it the interior sample. It runs at every α by the prompt's design (03 needs
  the plateau points), so `main.py`'s histories carry it too.
- **The near-stall at β = 5, α = 0.1**, T_J ≈ 0.7 MeV, φ ≈ 0.0083 M_P: π passes through ±1e-6, about
  100 × atol, inside one step. It is the kind of small oscillation 03's gate must separate from a
  wall bounce. Its wall point is drained in 02, at N = 42.958 (0.065 e-folds later, where
  π = 6.2e-4), with Δρ/ρ_R = 6.5e-9 against a median of 2.5e-7 over that history's 86 drains.
- **`CLAUDE.md`'s ComputeTargets counts are stale**: 269 there, 305 now; the [g-z] half is 192 there,
  209 now; [a-f] is 77 there, 96 now. That is 08's.

## 8. State handed to the next prompt

- **The records** (`ComputeTargets/ScalarHistory.py`, module level):
  - `TurningPoint(N, phi_Einstein, log_T_Jordan, kind)`, kind "wall" (π − → +) or "plateau"
    (+ → −). It is recorded at every α, α = 0 included, for every sign change the loop finds,
    across the history, after the first bounce as well.
  - `Injection(N, kind, N_turn, phi_turn, pi_before, pi_after, log_rho_kin, alpha_eff, delta_K,
    delta_rho_over_rho_rad, H_Einstein_before, H_Einstein_after)`, kind "turning_point" or
    "floor". 03 and 04 append their fields (λ, k_ex, m_ta, 𝒜_k, w_gate, w_rate, ln q); there are no
    placeholders.
  - `IntegrationResult.turning_points` and `.injections`, in order, are the last two fields. The
    payload carries both under the same names, beside `"dark_radiation"`. None is stored.
- **Detection** (ruling 15). It reads the two step ends by scalar calls plus 8 interior points per
  step (`TURNING_POINT_INTERIOR_SAMPLES`). One end-to-end change is located exactly as
  `first_bounce` locates it; two or more on a step are refined on their sub-intervals. A pair closer
  than a ninth of a step would be missed. `test_11_no_step_holds_more_sign_changes_than_the_loop_recorded`
  is the check on that, at β = 5. **`first_bounce` is unchanged**, so the loop's first wall
  `TurningPoint` would differ from it if an interior pair ever preceded the first end-to-end wall
  point (§7).
- **Where α_eff is set**: `alpha_effective()`, a local function in `integrate_scalar_history`,
  called once per drain point and once per floor reflection. It returns α. It is the one place 03
  multiplies in w_gate·w_rate. It will need the turning point (for 𝒜_k) and the injection history
  (for S_k), so its signature will grow. At α_eff == 0.0 no drain, ledger entry or restart is made.
- **The drain point and the dropped-drain rule.**
  - A wall point arms. The drain is at the first accepted step end, from the turning point's own
    step on, with `solver.y[1] > 0 and solver.f[1] <= 0`.
  - A plateau point, a floor reflection, a second wall point, or the history's end drops an armed
    drain. The step that ends the history never drains.
  - Dropped drains are not a field: in 02 they are walls minus "turning_point" injections. **If a
    weight can make α_eff = 0 at a wall point, that subtraction stops being the count**, and 03 must
    add one.
- **The restart** is `new_solver(N, y_drained)`, after `ledger.add`. Restarts = floor reflections +
  "turning_point" injections, from the records. `notify_reflection` is for floor events only.
- **The dense output is right-continuous** (`alt_segment=True`). At a drain node,
  `result.solution(N_j)` is the drained state exactly. `fixed_T_value_at`'s exact-node edge is in
  §7.
- **H_E across a drain** is equal bitwise, or one ulp of ln(3H²M_P²) apart (about 64 ε). The test's
  bound is `test_injection.H_bound`.
- **The counts 03 will gate**, at M = 0.5 M_P, untuned, to T_CMB:
  - **at α = 0**, wall/plateau: β = 1.2 9/8, β = 2 23/22, β = 5 141/140, β = 25 353/352. The first
    wall points are at 231 MeV, 747 MeV, 176 GeV and 1.06 TeV, and the last at 12 eV, 62 keV,
    169 keV and 6.5 keV.
  - **at α = 0.1**, drains: 7, 27, 86 and 75, with Σ Δρ/ρ_R = 1.50e-2, 1.25e-2, 1.98e-3 and
    4.39e-4, and r_χ(today) = 8.8e-3, 6.9e-3, 8.1e-4 and 1.7e-4. None was dropped. One drain, at
    β = 5, is the near-stall pair's (§7).
  - **The tuning** at β = 2, α = 0.1 converges in 11 integrations; 03 measures its residual's
    continuity.
- **The parameter.** `alpha` is on `integrate_scalar_history`, `integrate_history` and
  `tune_history`, last, by keyword, with 0 ≤ α < 1 checked at entry. `compute_scalar_history`
  does not take it (05).
- **Not done:** no gate, no taper and no stopping rule (03); no margin (04); no store, key or column
  (05); no option (06). `AdiabaticHistory` is unchanged. `CLAUDE.md`'s counts are 08's.
- **Counts after this prompt:** CosmologyModels 26, ComputeTargets 305 (96 + 209), ObjectFactories
  103, tools 115, RunDrivers 64.
