# Orchestrator — prompt 03, the gate and the stopping rule

Read [`../README.md`](../README.md) first: §0.2, §1, §2.2 (03's and 04's charters), §2.3 (the gate
"Taper"), §4, §5, §6.1 (U4 as ruled (b′), U5, the ruling of 𝒜₀) and §6.2 P4, P6, P11, P12 and P17.
Then the prompt, log 02 §1, §2, §7 and §8, and the board. This note binds 03 only. Note 02's
conventions (a tolerance is a number with its reason; timing under load; the suites run serially)
are restated here, and bind.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`03-gate-and-stopping-rule.md`](../03-gate-and-stopping-rule.md)
· **Gate:** "Taper" · **Closes:** nothing · **Opens:** only what the work finds · **Model:** Opus,
as the prompt recommends · **Network:** none. Nothing is pushed.

**Gate**, checked at `80dac75` on 2026-10-28 at 19:13 GMT, before this note was committed:
- `inelastic-reflection` checked out in the main checkout, at `80dac75` (prompt 03 written), after
  02's review `bc5314e`. `git diff bc5314e 80dac75` touches only `prompts/`.
  `git status --porcelain --untracked-files=all` is empty; `git worktree list` shows the main
  checkout alone. `HEAD` is the commit that adds this note, which touches only `orchestrator/`.
- `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing. No Ray is started by this prompt.
- `./venv/bin/python -m runregistrykit list`: 11 runs, all finished, none `running` or stale.
- The suites were not re-run for this note. The agent measures the baselines (prompt §3.1):
  26 / 305 (96 + 209) / 103 / 115 / 64.
- The load averages were 5.3 / 7.9 / 7.8. Other workloads share the machine.

## 0. What makes this prompt unusual

**It sets the fraction every drain takes from quantities the loop has never computed, inside the
step loop that every history is integrated with, and must leave α = 0 bitwise.** The orchestrator
read at `80dac75` every file the prompt names, and the planner's probe. Line numbers are right for
this tree. What it found, where the prompt is silent, loose or wrong:

- **The loop arms at every α.** `observe_turning_points` runs at α = 0 too, and
  `armed = turning_point` is set at every α (`ComputeTargets/ScalarHistory.py:1123-1124`). At α = 0
  the drain-point test still fires and `alpha_effective()` returns 0.0 (`:1137-1149`). So at α = 0,
  `alpha_effective()` is reached, and 03's weights would run there unless it returns before them.
  The prompt's "armed drains that never reached a drain point" is also defined at α = 0. Rulings 1
  and 7.
- **The drain's order is inverted for 03.** `drain()` (`:839`) computes H_E and ln ρ_kin *after*
  α_eff is handed to it, but 03's α_eff needs c, which needs ρ_kin/ρ_R first (w_rate depends on S,
  S on c). And `drain()` takes `log(alpha_eff)`, which fails at the α_eff = 0.0 record the prompt
  now asks for. Ruling 4.
- **The probe's half-oscillation is not the prompt's.** `nonadiabaticity.py` attributes whole steps:
  every step from the one holding N_{k−1} to the one holding N_k (`:55-57`), so samples on either
  side of each boundary enter both half-oscillations. Prompt §1.1 splits a step's samples at the
  turning point. A reference "as the probe does" would differ from the loop by the convention as
  well as the sampling, and the 0.05 test of §1.6 item 3 would mix the two. Ruling 3.
- **The reference shares nothing with the loop unless told not to.** If item 3's reference calls the
  module's mass function, a wrong mass passes it. And the exponential coupling has (ln A)″ = 0
  (`CosmologyConcepts/Models/ExponentialModel.py:135`), so the (ln A)″ term of m_eff² is exercised by
  no real history. Ruling 2.
- **Σ, f_m and H_E come from one policy call.** `ODEPolicy.__call__` already returns `Sigma` and
  `fm` (`:383-471`), and `HubblePolicy` needs its data anyway. A mass function that calls
  `cosmology.w` again doubles the spline cost per sample. 04 must call the same function on stored
  samples, which hold φ, ln ρ_R,E, ln f_m and Σ, not a cosmology. Ruling 2.
- **The samples must not reach `fun`.** `nfev` counts `fun`'s calls (`:798-801`), and the
  supervisor records RHS statistics. 03's samples go through `policy` and `hubble` only. Ruling 5.
- **The ledger at a drain node.** A step's end sample is evaluated before the drain at that end, and
  the ledger's rule N_j ≤ N would pair it with the new K if it were evaluated after. Ruling 5.
- **A half-oscillation can hold no sample.** A floor reflection at the first state, or two turning
  points in adjacent sub-intervals where one root is a sample that is exactly 0.0, leave the
  running maximum empty. The prompt does not say what 𝒜_k is then. Ruling 6.
- **The bound's interval count is off at an integer span.** Prompt §1.4 covers a span of ΔN e-folds
  with ⌈ΔN⌉ one-e-fold intervals. The covering intervals must be half-open, (a, a + 1], for
  "every earlier candidate of I is in the trailing window of each later one" to hold at their
  edges; and ⌈ΔN⌉ half-open intervals do not cover a closed span whose ΔN is an integer (candidates
  at exactly 0 and 25 need 26). The correct count is ⌊ΔN⌋ + 1, which equals ⌈ΔN⌉ unless ΔN is an
  integer. Item 5 is "over 25 e-folds". Ruling 9.
- **o = c(1 − w_rate) cancels.** When w_rate ≈ 1, 1 − w_rate loses all its digits; c·ε²/(S² + ε²)
  does not. Ruling 9.
- **The weights at the edges of float range.** 𝒜² overflows past 1.3×10¹⁵⁴, and 𝒜²/(𝒜² + 𝒜₀²) is
  then inf/inf. A NaN 𝒜 would reach π. Ruling 8.
- **The residual scan may cross no threshold.** The normalization scales the whole potential, so a
  ±10⁻³ span of residual_H moves the wall by about 10⁻³ in M/φ, and the turning-point count is
  unlikely to change on it. Then the gate's "continuous across a threshold" is not exercised. And
  with rtol = 10⁻⁸ (`config/defaults.py:32`) the α = 0 baseline's own deviation from a cubic may not
  be under 10⁻⁶; the prompt makes that a stop only when α = 0.1 fails and the baseline passes, and is
  silent when both fail. Ruling 11.
- **The comparison patch.** "α_eff = α in a scratch-only patch" invites an edit of a tracked file.
  Ruling 11.
- **"More than doubles" against what.** 02's times (log 02 §5) were taken at another load. Ruling 12.
- **The floor harness.** P1 at M = 10⁻¹⁰ (`ComputeTargets/tests/test_kinematic_cap_loop.py:257`) has
  an infall of several steps before its one reflection, so its 𝒜_k is defined; its value is not
  known. `test_injection.py`'s floor test asserts α_eff = α (`:619`) and π_out = −√(1 − α)π_in
  (`:617`). If w_gate there is small, the floor test still holds with α_eff, but its drain is small.
- **Other `test_injection.py` tests that read α_eff.** `delta_K_bound` takes `log(i.alpha_eff)`
  (`:178`), and test 6 asserts π_after ≠ π at the node (`:450`), which fails if
  √(1 − α_eff) rounds to 1.0 (α_eff below about 1.1×10⁻¹⁶). On the β = 2 history 𝒜_k ≥ 1.6 at
  every wall point (prompt §0.1), so neither is reached there; at β = 5 they could be. Ruling 10.

**Rulings.** Each binds the agent and is recorded in the log with its class.

1. **At α = 0 nothing of 03 runs.** `alpha_effective()` (or whatever replaces it) returns 0.0 before
   any weight is computed when α == 0.0; no sample state is kept beyond what 02 keeps, no mass, H_E
   or 𝒜 is computed, no `Injection` is made, and `TurningPoint`'s two new fields are `None`. The
   loop calls the mass function, the weights and the window sum through module-level names looked up
   at call time, so that `unittest.mock.patch` on `ComputeTargets.ScalarHistory.<name>` intercepts
   them (item 1, B8 and ruling 11 rely on it). *STRUCTURALLY REQUIRED*.
2. **The mass function.** One module-level function, written for 04 to reuse, taking the realized
   model, φ_E, ln ρ_R,E, ln f_m and Σ (no cosmology, no N), and returning ln m_eff² (or m_eff²; the
   log says which and why). In the loop Σ and f_m come from the `ODEPolicyData` of the same policy
   call that feeds `HubblePolicy` for H_E at that sample: one policy call per sample, no second
   `cosmology.w`. A non-positive or non-finite m_eff² is a `ComputationFailureError` naming the
   state. Tests, besides the prompt's:
   - **a hand value** of m_eff² at one fixed state, on a stub model with (ln A)″ ≠ 0 (as
     `test_adiabatic_mass.py` builds one), so that the (ln A)″ term is tested;
   - **item 3's reference computes m_eff² by its own expression** from the model's `log_V`,
     `d_logV_dphi`, `d2_logV_dphi2`, `d_logOmega_dphi` and `d2_logOmega_dphi2`, never by calling
     the module's function;
   - **B10**: the mass function without its trace term ([(ln A)″ + (ln A)′²] ρ_R,E(Σ + f_m)
     dropped). It must fail the hand value and item 3. Record which.

   *IMPLEMENTATION CHOICE*, ruled here.
3. **One attribution rule for the loop and the reference.** A sample belongs to the
   half-oscillation (N_{k−1}, N_k] that contains its N: samples with N ≤ N_k to the one ending at
   k, samples with N > N_k to the next. d ln m_eff²/dN is `np.gradient` over the step's own samples,
   all of them, before they are attributed. The reference (item 3) does the same at 33 points per
   step; it differs from the loop only in the number of points. The two samples at a shared node
   (one step's end, the next step's start) both count, each with its own step's gradient.
   *STRUCTURALLY REQUIRED* (item 3 must measure the sampling, not a convention).
4. **A candidate's order of operations.** At a drain point or a floor reflection, with the state
   and ledger before any change:
   1. H_E, ln ρ_kin,E and ρ_kin,E/ρ_R,E (02's expressions, in logs);
   2. w_gate from 𝒜_k; c = α · w_gate · ρ_kin,E/ρ_R,E, with no intermediate that overflows, and
      c = 0.0 when w_gate == 0.0;
   3. S from the window, the candidate's own c included; w_rate; α_eff = (α · w_gate) · w_rate;
      o (ruling 9);
   4. if α_eff > 0.0, 02's drain with that α_eff (ledger, π, restart) and its record; if
      α_eff == 0.0, a record with `pi_after` = `pi_before` (−`pi_before` at a floor), `delta_K` =
      `delta_rho_over_rho_rad` = 0.0 and `H_Einstein_after` = `H_Einstein_before` (the same float, no
      second call), no ledger entry and no restart beyond what α = 0 does there (the floor's
      elastic restart stays).

   The window uses each candidate's own N: the drain point's (`Injection.N`), or the reflection's.
   A candidate enters the window with its c whatever its α_eff. *STRUCTURALLY REQUIRED*.
5. **The samples.** At α > 0, on every appended step, the last included and none that is not
   appended, the ten states are the ones `observe_turning_points` already reads (the two scalar
   end calls and the one vectorized interior call), kept as full states, on the no-change early
   return too. H_E and the mass at each sample go through `policy` and `hubble`, never through
   `fun` or `RHS`: `nfev`, the supervisor's records and the Radau instance are untouched by them.
   They are computed before any drain at the step's end, with the ledger as it stands then.
   *STRUCTURALLY REQUIRED*.
6. **An empty half-oscillation** has 𝒜_k = 0.0, so w_gate = 0.0 and its candidate is recorded with
   α_eff = 0.0. The log counts how often it happened on every history it ran (expected: never at
   M = 0.5). *IMPLEMENTATION CHOICE*, ruled here.
7. **`dropped_drains` is counted at every α**, by the arming logic 02 runs at every α: a drain
   armed and then dropped by a plateau turnaround, a floor reflection, a second wall point or the
   history's end. At α > 0 it equals walls − "turning_point" candidates (item 7). At α = 0 it is
   counted but checked by no record; item 1 asserts only that it is an `int`. Its comment says it
   counts at every α. *IMPLEMENTATION CHOICE*, ruled here.
8. **The weights are finite at the edges.** For every finite 𝒜 ≥ 0 and S ≥ 0, w_gate and w_rate
   are finite and in [0, 1]; for 𝒜 above 𝒜₀ (and S above ε) they may be computed as
   1/(1 + (𝒜₀/𝒜)²), which overflows nowhere. A non-finite 𝒜_k or c is a
   `ComputationFailureError`. Item 2 adds 𝒜 = 10²⁰⁰ and S = 10²⁰⁰. The hand values are asserted to
   a stated bound of a few ε, derived from the form used. *IMPLEMENTATION CHOICE*, ruled here.
9. **The omitted part and the bound.**
   - o is computed as c · ε²/(S² + ε²). Item 7 asserts |o − c(1 − w_rate)| ≤ k · ε_mach · c, k
     derived in the log from the operations.
   - The bound is over half-open one-e-fold intervals (a, a + 1], and the total over a span of ΔN is
     at most (π/2) ε (⌊ΔN⌋ + 1). The log's proof says both, including the step from the sum to the
     integral (each o_j ≤ ∫ over [s_{j−1}, s_j], since ε²/(s² + ε²) decreases and s_j includes c_j).
   - Item 5's synthetic sequence has candidates at both ends of an integer span, so the ⌊ΔN⌋ + 1
     count is the one tested. Item 4 also asserts that B3's form (the candidate left out of S) omits
     the first candidate whole, o_1 = c_1.
   - The window's edge test (item 2) uses values exact in binary (for example N = 3.5, N_j = 2.5),
     and the comparison is written as N_j > N − `RATE_WINDOW_EFOLDS`.

   *STRUCTURALLY REQUIRED* (the integer-span count corrects prompt §1.4); the rest *IMPLEMENTATION
   CHOICE*, ruled here.
10. **The edits to `test_injection.py`** are the prompt's (α → `i.alpha_eff` in items 2, 5 and the
    floor test) and, where they are reached, these: `delta_K_bound` and test 6's inequality are
    restricted to records with α_eff > 0.0, and test 6's π_after ≠ π to records whose
    √(1 − α_eff) is not 1.0. Each is listed in the log with its reason. Any other edit is a stop.
    *STRUCTURALLY REQUIRED*.
11. **The residual's continuity (prompt §2 item 2).**
    - **The scans** are 21 points evenly spaced in `model.normalization_coordinate`, the tuner's own
      variable, with φ_ref fixed.
    - **The comparison** (α_eff = α) is made in the scratch script by `unittest.mock.patch` of the
      module's weight functions to return 1.0, never by editing a tracked file.
    - **If no turning-point or candidate count changes inside the ±10⁻³ scan** at α = 0.1, the log
      says that the threshold clause was not exercised by it, and the agent then finds one: widen the
      bracket outward in the coordinate (doubling, at most 6 integrations per side) until the wall
      count differs between its ends, bisect to a width like the main scan's, and run a 21-point scan
      across it at α = 0.1 (03's rule) and under the comparison patch. It reports the residual's jump
      at the count change under each, against the scan's cubic. At most 40 integrations per β for the
      search; if none is found, report it and go on.
    - **If the α = 0 baseline itself deviates by 0.1 tolerance or more**, it is not a stop. The log
      reports all three scans' deviations side by side, and the clause "less than a tenth of the
      tuning tolerance" is judged at the review, against the baseline, and goes to the user. The
      stop of prompt §5 (α = 0.1 fails while the baseline passes) stands.
12. **Timing.**
    - At α = 0: prompt §3.4, the golden tests at the dispatch commit and with the change, back to
      back, three times each, minima and load. A difference inside the spread is "nothing
      measurable"; say so with the numbers.
    - At α = 0.1: "more than doubles" is judged against the dispatch commit run back to back with
      the change, on the same history, minima of three, in a temporary worktree of the dispatch
      commit that is removed afterwards. The cost per accepted step of the 𝒜 computation is
      reported from that difference, with the policy calls added counted, not estimated.
    - If the linear ln H_E option is taken, the log gives its error bound and item 3's result under
      it, and the choice is *IMPLEMENTATION CHOICE*.
13. **The records.** `TurningPoint`, `Injection` and `IntegrationResult` stay module-level
    namedtuples, their new fields appended last in the prompt's order. The comment blocks above them
    name the new fields. The payload's new key is `"dropped_drains"`; `"reflections"` stays the count
    of floor events, and `"injections"` the list. `compute_scalar_history`, `build_extra_data`,
    `ScalarHistory.store`, `golden_history.record_of` and `TUNING_OUTPUT_KEYS` are byte-identical.
14. **Test cost.** Items 9 and test_injection's item 11 at β = 5, α = 0.1 share one integration
    where they are in one module (`setUpClass`); items 3 and 7 share the β = 2 one. Each docstring
    gives the cost and the load it was measured at.
15. **Suites.** Run `CLAUDE.md`'s commands serially, with ComputeTargets in its two halves. Send
    output to a file in the agent's own scratch directory, grep the verdict from it, and record
    `uptime` beside each. A run that may pass 600 s goes in the background (`run_in_background`),
    and the agent waits for its notification without polling. A failure is re-run alone before it
    is called real.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status`
  before committing. Count, then paste. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The five suites, and the two golden tests alone with their times, before any edit.
  2. The weights, the window, the omitted part and the mass function as module-level functions,
     with items 2, 4, 5, 6 and ruling 2's hand value.
  3. The loop at α > 0 (rulings 1, 3–7), and α = 0 bitwise: the golden tests,
     `test_first_bounce.py`, `test_surfing_turning_points.py`, `test_kinematic_cap_loop.py`.
  4. The records, the payload key, the edits to `test_injection.py`.
  5. Items 1, 3, 7, 8 and 9; then the breakages B1–B10; then the measurement; then the suites and
     `black --check`.
  6. The records: log, documents, board.
- **Breakage diffs** are checked with `git apply --check` and `-R --check`, and replayed in `bash`,
  not zsh. None is committed.
- **Scratch.** The agent uses its own subdirectory of the session scratchpad, never `/tmp`. No
  scratch file goes under the repository. Any worktree is removed afterwards (`git worktree list`
  shows only the main checkout).
- **Stores and Ray.** No store is opened. No Ray is started. Nothing is written to `var/`. No
  golden JSON is regenerated.
- **Stop conditions are real.** They are the prompt's §5, plus an existing test failing for a
  reason this note does not name. A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

Done at `80dac75`, before this note was committed:
1. **The Gate above**, and the reading of §0.
2. **m_eff², by the orchestrator.** This is not given to the agent, which writes its own (prompt
   §1.1). The kick in `ODEPolicy` is −3M_P² E (ln Ω)′ R = −ρ_R,E(Σ + f_m)(ln Ω)′/H², so the force is
   V′ + (ln Ω)′ρ_R,E(Σ + f_m) and the trace that couples is ρ_R,E(Σ + f_m) = Aρ̃_c. With V_eff = V +
   Aρ̃_c and ρ̃_c fixed, V_eff″ = V″ + A″ρ̃_c = V″ + [(ln A)″ + (ln A)′²]ρ_R,E(Σ + f_m), and Ω is A
   (`log_Omega` = βφ/M_P). For the exponential model this is the probe's m², since (ln A)″ = 0. It
   is not the RHS's ∂π′/∂φ at fixed state, which has no (ln A)′² term (ln f_m is a state component);
   the log should say why the quanta's mass is the response at fixed ρ̃_c.
3. **The bound, by the orchestrator.** With S_j ≥ s_j = Σ_{i ≤ j, i ∈ I} c_i on a half-open
   interval I of one e-fold, o_j = c_j f(S_j) ≤ c_j f(s_j) ≤ ∫_{s_{j−1}}^{s_j} f, f(s) = ε²/(s² + ε²)
   decreasing, so Σ o_j ≤ ∫₀^∞ f = (π/2)ε. Without the candidate in S (B3), the first term is c_1
   f(0) = c_1, unbounded.
4. **The suites' baselines** are the agent's (Gate).

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout. Give it:
- the prompt, the README, the board and log 02;
- `HEAD` and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the prompt's §6 files. Its log is
  `logs/03-gate-and-stopping-rule.md`, in `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **No Ray, no store, no golden JSON written.**
- **Stop and ask** on any of the prompt's §5 conditions or §0's.
- **It replies with:**
  - the log's path and the message as committed;
  - `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - the golden tests' times before and after (ruling 12), and the cost of the 𝒜 computation at
    α = 0.1 per accepted step and per history;
  - its m_eff² derivation and the bound's proof, in brief;
  - the new tests by name, each with what it asserts, its tolerance and its cost;
  - for item 3: the largest |Δw_gate| and the largest relative difference in 𝒜_k, at wall and
    plateau points;
  - item 8's w_gate at the floor, and item 9's result (and whether β = 5 still holds an interior
    pair under the gate, with item 11's outcome);
  - the measurement tables, the tuning at β = 2 and 5, and the three residual scans (ruling 11);
  - each breakage, with the tests it failed and how; B8's cost;
  - every edit to `test_injection.py` with its reason;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — ten checks

1. **Scope.** `git show --stat <agent>` lists exactly the prompt's §6 files (plus
   `.documents/OPEN_ISSUES.md` only if something was opened), and its parent is this note's commit
   or a later commit that is not the agent's. `git status --porcelain --untracked-files=all` is
   empty, and nothing is pushed.
   `git diff <dispatch> <agent> -- ComputeTargets/AdiabaticHistory.py ComputeTargets/Policies ComputeTargets/dark_radiation.py Quadrature ObjectFactories config main.py tools RunDrivers plot_inputs.py ComputeTargets/tests/data ComputeTargets/tests/golden_history.py ComputeTargets/tests/golden_tuned_history.py`
   is empty, or each change is a classed deviation. `compute_scalar_history`, `build_extra_data`,
   `ScalarHistory.store`, `first_bounce` and `TUNING_OUTPUT_KEYS` are byte-identical to
   `<dispatch>`'s.
2. **α = 0.** `test_golden_history` and the tuned golden test pass alone at `<agent>`, and the JSONs
   are byte-identical. `test_first_bounce.py`, `test_surfing_turning_points.py` and
   `test_kinematic_cap_loop.py` are unedited and pass. I read the α = 0 path of every changed line
   of `integrate_scalar_history` and confirm that nothing of 03 is reached (ruling 1).
3. **The loop.** Every changed line against rulings 3–7: the samples are 02's, kept whole; the
   attribution splits at the turning point and resets at a floor reflection; the order of a
   candidate; the window by the candidate's N; α_eff == 0.0 leaves no ledger entry and no restart;
   no sample goes through `fun`.
4. **The mass and 𝒜.** The function against ruling 2 and §1 item 2. A probe of my own (session
   scratchpad, not committed): one β = 2, α = 0.1 history at M = 0.5 M_P; recompute 𝒜_k at every
   turning point from the dense output at 33 points per step by ruling 3, and w_gate, c, S, w_rate,
   α_eff and o for every candidate from the records alone; compare.
5. **The breakages.** Replay each diff with `git apply` in `bash`. Each fails the test the log
   names, and is reverted. B3, B8 and B10 in particular.
6. **The numerics of the tests.** Ruling 8's edge values; ruling 9's bound on o, the half-open
   intervals and ⌊ΔN⌋ + 1; item 3's 0.05 against the reported differences; the edits of ruling 10
   only where reached.
7. **The measurement.** The tables have every column of prompt §2; `17>loop` is 0 on every history;
   Σ o is under (π/2)ε(⌊ΔN⌋ + 1); the dropped drains are reported. I re-run one β of it at α = 0.1
   myself and compare the counts. The residual scans are judged under ruling 11.
8. **Nothing reaches the store.** Reread `ScalarHistory.store`, `build_extra_data`,
   `golden_history.record_of` and `TUNING_OUTPUT_KEYS` at `<agent>`. The ObjectFactories, tools and
   RunDrivers counts are unchanged.
9. **The records.** The log is in §5.1's form, with the m_eff² form and how it differs from
   `M2eff_over_H2`, the bound checked, every deviation classed and §0's rulings among them. The two
   `.documents/` notes are dated and additive only. The board (row 03, gate Taper, §5.3) and
   `.documents/OPEN_ISSUES.md`, if anything was opened, agree with each other.
10. **The suites** at `<agent>`, serially: the counts the log gives, each at least the baselines,
    with ComputeTargets up by the new tests only. `black --check` on the changed `.py` files.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, in the board, the log and `.documents/OPEN_ISSUES.md` wherever it
  appears;
- the board's row 03 landed, the gate Taper held, and this note added to the list of orchestrator
  notes, marked "used for 03";
- under `### 5.3`, an *Orchestrator review of prompt 03* paragraph holding what each check found.

Residue the review finds is fixed in that commit or a follow-up of its own. Anything that is not
residue is opened on the board's §3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the gate Taper, and the residual-continuity clause as judged (ruling 11);
- the counts;
- the measurement, briefly: w_gate's distribution, the injected and omitted fractions against
  02's, ρ_χ/ρ_R today, and the cost;
- anything opened;
- the records as handed to 04;
- that 04 is written next, from 03's "State handed to the next prompt" (README §2.1).
