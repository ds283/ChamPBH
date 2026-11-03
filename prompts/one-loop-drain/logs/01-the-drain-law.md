# Log 01 — the drain law

**Subject:** Add the one-loop drain law, the wall-search fix and ruling (b)
**Commit:** `b99f6a4`, on branch `one-loop-drain`, on top of `b1415a3`
**Date:** 2026-11-03, by Claude Opus 5.5
**Result:** **COMPLETE.** The Law gate holds (`.documents/one-loop-drain-verification.md` §1.10).
- **The one-loop law** sits beside the fixed one as an input (`drain_law`) of `integrate_scalar_history`,
  `integrate_history` and `tune_history`, with A carried by `alpha`. Each candidate is classified once, in the loop, and
  drains by its regime. The `"drain"` payload key carries the record.
  - Under the fixed law the goldens pass bitwise and the 42 replay equal `reflection-restart` 04's.
  - Under the one-loop law 2197 and 2188 reproduce the drain probe's table bitwise at A = 1 and 0.1, and 3016 and 3233
    drain nothing, bitwise the probe's.
- **The wall-search fix** (P21) answers the 19 measured failing inputs. 2197, 2188 and the nine now complete under the
  fixed law. The predicate costs 8.4 × 10⁻⁴ of 2197's wall time.
- **3873's stop** is the unresolved turnaround firing late, inside 2φ_t, in the wall's linear tail, where G2 refuses.
  The same mechanism is behind 21 of the 2026.8.0 store's failures. The work stopped and asked. The user ruled (b),
  and 3873 now completes.
- **Issues:** `[00-fixed-alpha-laboratory-M-histories-fail-in-classical-turning-point]` and
  `[00-a-zero-drain-history-stops-at-the-inside-wall-guard]` are closed. Index: 30 open.
- **Suites** before anything else: 26, 514 (122 + 392), 130, 147, 68, all passing. After: 26, 566 (163 + 403),
  130, 156, 68, all passing.

The orchestrator's note for this prompt (§0, rulings 1–9 and the conventions) governed where it differs from the
prompt. Each ruling is classified in §2.

## 1. What shipped

- **`ComputeTargets/ScalarHistory.py`**
  - **The law's constants.** `DRAIN_LAW_FIXED`, `DRAIN_LAW_ONE_LOOP`, `DRAIN_LAWS`; `DRAIN_C1` = 2.7,
    `DRAIN_ALPHA_CAP` = 0.99, `DRAIN_ELL_EDGE` = 4.0, each with its source; `REGIME_*` and `DRAIN_REGIMES`;
    `GAP_PRESCRIPTION_*`, `GAP_PRESCRIPTIONS` and the temporary `_GAP_PRESCRIPTION` = `"zero"`, whose comment cites
    README §8.2 and its removal by 03.
  - **The functions.**
    - `check_drain_law`: the fixed law's check and message as before; under the one-loop law, A finite and > 0.
    - `drain_regime(model, assessment)`: (regime, ℓ, PeakEstimate).
    - `drain_alpha_effective(regime, ℓ, A, w_gate)`: ruling 3's arithmetic; `_GAP_PRESCRIPTION` is read at call time.
    - `_assessed_peak`: `peak_estimate`'s body, moved unchanged, so that `drain_regime` computes the identical
      PeakEstimate (P18).
    - `unresolved_reflection_refused`, `_wall_and_kinetic` and `_g2_refuses`: ruling (b). G2's two sides and its
      comparison are factored out of `_reflection_guards`, whose raise and message are unchanged.
  - **The records.** `DrainCandidate` (regime, ell, alpha_eff, w_gate, log_E, log_H). `DrainRecord` (law, A,
    gap_prescription, unresolvable_assessments, candidates, unresolved_declined).
  - **`IntegrationResult`** gains `drain` and `peak_estimates`, both defaulting to None (ruling 5).
  - **`integrate_scalar_history`** takes `drain_law`. Under the one-loop law, `candidate` computes w_gate as
    `alpha_effective` does, then the regime, then α_eff. It appends each candidate's `DrainCandidate` and PeakEstimate
    after its Injection.
  - **The counts** (both laws):
    - the loop counts each None assessment at which the predicate holds; natural mode's `natural_floor_warning` does
      the same and takes φ for φ_t;
    - the loop declines an unresolved decision that G2 would refuse, and counts it.
  - **The failure path.** A loop failure carries `e.drain`.
  - **`history_margins`** reuses `result.peak_estimates` where it is not None.
  - **`integrate_history`** takes `drain_law` and passes it on. Its payload, a failure's included, gains `"drain"`.
  - **`tune_history`** takes `drain_law` and passes it to every integration. Its failures carry the `"drain"` of the
    integration that ended them.
- **`CosmologyConcepts/Models/AbstractModel.py`**
  - `_least_finite_log_V_minus_floor`, which steps up with `nextafter`, is cached, and is bounded at 4(K + 2) steps.
  - `_turning_point_unresolvable`, whose docstring argues that every input it accepts raised before.
  - `classical_turning_point` returns None where it holds, after `r_0 >= 0`, with a docstring paragraph.
- **`tools/history_and_bbn.py`**
  - `--drain-law {fixed,one_loop}`, and `--gap-prescription {zero,edge}` (one_loop only, set and restored in-process).
  - `--alpha` is checked per law after parsing; under the fixed law, alpha_fraction's messages are reproduced.
  - `drain_values` and `drain_line`; under one_loop, the history line names `drain_law=one_loop A=… gap=…`.
- **Tests (new)**
  - `ComputeTargets/tests/test_unresolvable_turning_points.py` (11);
  - `ComputeTargets/tests/test_drain_law.py` (41);
  - `tools/tests/test_history_and_bbn_drain_law.py` (9).
- **`.documents/one-loop-drain/law/`**, each script with a docstring:
  - `failing_inputs.py` (§2.1.3);
  - `compare_replay.py` (§2.1.2);
  - `predicate_cost.py` (§2.1.4 counts, §2.1.6);
  - `fixed_law.py` (§2.1.4–§2.1.6);
  - `one_loop_run.py` and `one_loop_runs.sh` (§2.2, §1.7);
  - `probe_table.py` (§2.2);
  - `inside_wall_3873.py` and `inside_wall_summary.py` (§1.7);
  - `inside_wall_store.py` (the 21 stored failures);
  - `store_failures.py` (3460 and 4397);
  - `compare_final.py` (the final tree against the first run).

  The outputs in `out/` are text.
- **`.documents/one-loop-drain-verification.md`** (new) §1.
- **Dated notes** in `.documents/numerical-strategies.md` (§3.5) and `.documents/architecture-summary.md` (§7.2).
- **Records:** `CLAUDE.md`'s test counts; the board (row 01, §1.1, the Law gate, §3/§4, §5, Last updated);
  `.documents/OPEN_ISSUES.md`.

## 2. Deviations from the prompt

1. **Where it runs: the main checkout, not the worktree** (orchestrator ruling 1). *STRUCTURALLY REQUIRED*: the
   worktree had been removed. Only ruling 9's files are written, with outputs under `var/one-loop-drain/01/`.
2. **The batches run through the scanner, from 04's task lists** (ruling 2), at 6 + 2 and then 6 + 1 + 1 workers.
   *IMPLEMENTATION CHOICE*, ruled.
3. **The law's arithmetic, left to right** (ruling 3), so that with w_gate = 1 it is the probe's bit for bit.
   *IMPLEMENTATION CHOICE*, ruled.
4. **ℓ ≤ 0** (ruling 4): the regime is gap, and the PeakEstimate is `peak_estimate`'s, unchanged. *STRUCTURALLY
   REQUIRED*. None arises: `one_loop_turnaround` raises where ℓ ≤ 0.
5. **No existing type gains a field that an existing payload key carries** (ruling 5). The reuse travels in
   `IntegrationResult.peak_estimates`, and `peak_estimate`'s signature is unchanged. *STRUCTURALLY REQUIRED*.
6. **The failing calls are 19, not 11** (ruling 6), with literals in the test module and the store check in a script.
   *As the prompt intends; made exact.*
7. **The probe comparison** (ruling 7), built as `drain_probe.py` builds it, with `alpha=A, drain_law="one_loop"`.
   *IMPLEMENTATION CHOICE*, ruled.
8. **Ruling 8** (the test counts): followed. No existing test was edited.
9. **Ruling 9** (the files): followed. *STRUCTURALLY REQUIRED*.
10. **The user's ruling (b) on the 3873 stop, 2026-11-03:** "I see. I had assumed that was an energy balance that meant
    the field and moved beyond its reflection point, but I see that it's not. In which case, (b) is correct."
    *STRUCTURALLY REQUIRED*: prompt §1.7 reserves a change to the unresolved rule or G2 for the user.
    - Implemented as the coordinator specified: G2's own expression and comparison, shared with `_reflection_guards`.
      The floor rule is tested as usual at a declined step start, and the declines are counted
      (`unresolved_declined`).
    - Tests were added (`TestTheDeclinedUnresolvedReflection`).
    - §2.1's batches were run again on the final tree (equal), and §2.2.1 and §2.2.3 again (equal).
    - The prompt §3's "no change to … the floor rule, G1/G2" is superseded here by the ruling. The floor rule and G2
      themselves are unchanged; only the unresolved turnaround's decision is declined where G2 would refuse it.
11. **The walk's bound is 4(K + 2) steps, not the prompt's K + 2.** At ln V_floor = −2^m the floats above have half its
    ulp, and the walk needs about 2K + 1 steps; a test covers it at ln V_floor = −1. The grid's normalizations take 17.
    *IMPLEMENTATION CHOICE*.
12. **The constant names** `DRAIN_C1`, `DRAIN_ALPHA_CAP`, `DRAIN_ELL_EDGE`, and the record as namedtuples
    (`DrainRecord`, `DrainCandidate`), plain values as the prompt requires. *IMPLEMENTATION CHOICE*.
13. **Tuning failures carry `"drain"`** (from the integration that ended the tuning). The prompt names only the
    accepted integration's. *IMPLEMENTATION CHOICE*, so that 02 sees a failed history's counts.
14. **The `drain` line's K shares are of K(N_final)**, the parking leftover's included (`K_share_parking`), and the line
    also carries `unresolved_declined`. *IMPLEMENTATION CHOICE*.
15. **Natural mode's constructed case** moves 2197's failing input to the P1 harness's own ln V_floor (the same distance
    below the least value, φ = 10⁵ M_P in the tail). The literal input is tied to its own normalization.
    *IMPLEMENTATION CHOICE*.
16. **The tool's `--alpha` is parsed as text and checked after `--drain-law` is known.** Under the fixed law,
    alpha_fraction's messages are reproduced as `argument --alpha: …`, and the existing tests pass unedited.
    *STRUCTURALLY REQUIRED*: the check depends on the law.
17. **The predicate's cost was measured on the tree before ruling (b).** The change does not touch the predicate, and the
    final pair's digests equal the first. *IMPLEMENTATION CHOICE*.

No UNINTENDED DRIFT.

## 3. Callers of `classical_turning_point` and `assess_infall` (prompt §1.6)

| caller | with the raise | with None |
|---|---|---|
| `RealizedModel.classical_turning_point` | passed it on | passes it on |
| `assess_infall` | passed it on | returns (ln E, None) |
| the step loop's assessment (α > 0, a wall) | the history's failure | no decision; the infall's latest assessment stands; counted when the predicate holds |
| `natural_floor_warning` (α = 0) | `WARNING_WALL_UNASSESSED`, the history went on | φ taken for φ_t, so "below" where φ < 10⁻⁸ M_P; counted |

This can change no value of a history that succeeds today: only warning lines, which are not stored. The constructed
case is `TestTheUnresolvableCount`: the run is bitwise today's, and the warning moves from unassessed to below.
`one_loop_turnaround` does not call `classical_turning_point`.

## 4. Verification performed

**Suites** (`CLAUDE.md`'s commands; ComputeTargets in two halves; outputs in the agent's scratchpad):

| suite | before (06:57–07:13 UTC) | after (final tree, 08:48–09:04 UTC) |
|---|---|---|
| CosmologyModels | 26, 64.7 s | 26, 66.3 s |
| ComputeTargets `test_[a-f]` | 122, 136.4 s | 163, 136.9 s |
| ComputeTargets `test_[g-z]` | 392, 460.3 s | 403, 466.0 s |
| ObjectFactories | 130, 80.7 s | 130, 98.3 s |
| tools | 147, 157.3 s | 156, 178.2 s |
| RunDrivers | 68, 22.4 s | 68, 27.5 s |

`uptime` at the starts: before, 3.20 / 3.29 / 4.12 and then 2.67 / 3.11 / 3.99 / 5.76 / 4.10 / 4.24 …; after,
9.00 / 10.58 / 10.72 at the first suite, 4.64 / 7.80 / 9.50 at `test_[g-z]`, and 55.79 / 23.63 / 14.53 at
`tools/tests` (another session's load). All pass. The new tests, by name:
- `test_unresolvable_turning_points` (11):
  - `TestThePredicateAtItsBoundary`: `test_1_the_last_true_and_the_first_false`,
    `test_1_the_least_value_is_the_first_finite_one`, `test_2_where_it_holds_the_search_raises`;
  - `TestWhereItDoesNotHold`: `test_3_at_and_above_the_boundary`, `test_3_the_twelve_inputs_test_wall_rounding_pins`;
  - `TestTheNineteenFailingInputs`: `test_4_there_are_nineteen`, `test_4_each_returns_None_now_with_the_predicate_holding`,
    `test_4_each_raises_with_the_pre_fix_copy`;
  - `TestTheCache`: `test_5_a_second_call_does_not_walk_again`, `test_5_the_cached_value_is_the_walks`,
    `test_5_a_call_costs_microseconds`.
- `test_drain_law` (41):
  - `TestTheConstants` (1);
  - `TestTheLawsInputs` (8): the refusals at each entry point, today's message, A > 1 handed on, the default, the
    tuning's hand-on, the floor harness at A = 1.5;
  - `TestTheRegimeAndAlphaEff` (13): each regime against `peak_estimate`, ℓ ≤ 0, another exception, the one-loop
    α_eff, the cap, w_gate = 0, classical and unassessed, both prescriptions read at call time, an unknown prescription
    and regime;
  - `TestTheFixedLawIsUnchanged` (4): the key set, the fixed record, α_eff = α·w_gate, a failure's `"drain"`;
  - `TestTheOneLoopLawOn2197` (6): the record, plain values, the reuse, one solve per candidate, the margins, the
    regime;
  - `TestTheUnresolvableCount` (4): the constructed input, natural mode, before the fix, the loop's count;
  - `TestTheDeclinedUnresolvedReflection` (5): `test_6a_the_comparison_is_G2s`, `test_6a_either_side_of_the_boundary`,
    `test_6b_the_infall_goes_on_to_a_resolved_turning_point`, `test_6b_before_the_ruling_the_window_raised`,
    `test_6c_the_count_is_the_declined_step_starts`.
- `test_history_and_bbn_drain_law` (9): `TestTheOptions` (6), `TestTheDrainValues` (2), `TestAOneLoopRun` (1).

**The Law gate** (verification §1.3–§1.10):
- **§2.1.1:** the goldens (50 tests) pass on the first tree and on the final one.
- **§2.1.2:** 42 of 42 equal 04's, on both trees.
- **§2.1.3:** 19 of 19.
- **§2.1.4:** 2197 and 2188 complete; their numbers are in the verification table.
- **§2.1.5:** all nine succeed.
- **§2.1.6:** 8.36 × 10⁻⁴ and 8.22 × 10⁻⁴ of the wall time.
- **§2.2.1:** bitwise, and the production gate changes nothing.
- **§2.2.2:** edge equals zero.
- **§2.2.3:** 3016 and 3233 drain nothing, bitwise the probe's.
- **§2.2.4:** 3873, below.
- **§2.2.5:** the record holds for every run.

**3873.**
- The branch that asks is the unresolved turnaround. The assessment is the step start's own, with
  φ_t = 9.9805 × 10⁻⁹ M_P, ln R = −215.13 and φ = 1.6734 φ_t.
- The cause: the linear tail (W/E = φ_t/φ), with φ_t crossing the 10⁻⁸ M_P threshold during the infall, so the rule
  fires inside 2φ_t, where G2 refuses. 87 of 1177 unresolved reflections fired late.
- At α = 0.03 the field parks at N = 37.52 with φ_t ≈ 10⁻¹³ M_P, in the steep part of the wall, and the largest ratio
  is 0.002.
- What was done: the stop, the user's ruling (b), its implementation, and 3873 completing (19 declined).

**Runs.** Each was detached (`nohup`, pid recorded), checked once, and waited on by a background command. At most 8
workers ran at once. All had ended before the commit. Outputs are under `var/one-loop-drain/01/` (first tree) and
`var/one-loop-drain/01/final/` (final tree).

## 5. The deliberate-breakage record

Each break was made in place by a script (`breakage.py`, in the agent's scratchpad), with the named tests run on it and
the file restored from a copy. Afterwards the six code and test files' SHA-256 digests were checked equal to those taken
before the first break.

| break | file | tests run | result |
|---|---|---|---|
| B1: the rule removed (`if False and _turning_point_unresolvable(...)`) | `AbstractModel.py` | `test_unresolvable_turning_points` (11) | FAILED, errors = 27: the 19 inputs no longer return None (each raises), and `test_2_where_it_holds_the_search_raises` at its 8 inputs |
| B2: w_gate dropped from the one-loop α_eff | `ScalarHistory.py` | `TestTheRegimeAndAlphaEff` (13) | FAILED, failures = 3: `test_2_alpha_eff_at_a_one_loop_turnaround` (w = 0.37), `test_2_the_cap`, `test_2_w_gate_zero_drains_nothing` |
| B3: ruling (b) removed (`if False and unresolved_reflection_refused(...)`) | `ScalarHistory.py` | `TestTheDeclinedUnresolvedReflection` (5) | FAILED, errors = 2: `test_6b_the_infall_goes_on_to_a_resolved_turning_point` and `test_6c_...` raise "reflection requested inside the wall" |
| B4: P18's reuse removed (`reused = None`) | `ScalarHistory.py` | `TestTheOneLoopLawOn2197` (6) | FAILED, failures = 1: `test_4_the_one_loop_solve_runs_once_per_candidate` (the solve runs twice) |
| B5: A = 0 accepted under the one-loop law (`alpha >= 0.0`) | `ScalarHistory.py` | `TestTheLawsInputs` (8) | FAILED, failures = 2, errors = 2: `test_1_integrate_scalar_history_refuses` and `test_1_tune_history_refuses_before_the_seed` at A = 0.0 and −0.0. `test_1_integrate_history_refuses_before_any_integration` did not catch it: there, A = 0 reaches the mock model, which raises a ValueError of its own |

Every break was caught.

## 6. Observations not acted on

- **The unresolved turnaround fires late wherever φ_t crosses the 10⁻⁸ M_P threshold during an infall** (87 of 1177 in
  3873 under the one-loop law). Ruling (b) answers only the late firings that G2 would refuse. Those that pass, at
  2–3 φ_t, still reflect with W/ρ_kin up to 0.88, in a region of the wall's linear tail where a resolved turnaround
  would be the better model. Not acted on: it would change the rule where it succeeds, and so the fixed law.
- **3873 under the one-loop law has 2486 candidates and no parking.** After N ≈ 43.765 every infall turns at a resolved
  turning point (1047 of them, all classical). Each integration took 113–119 s, against 12.8 s at the stored α. 02's
  sample at large M will see these costs.
- **Of the 21 stored fixed-law inside-wall failures, only two (3460, 4397) were re-run.** The other 19 are recorded
  only as a measurement (verification §1.7), as the coordinator ruled.
- **The fixed law's late classical gap candidates.** 2197 at α = 10⁻³ has 170 `classical_gap` generations, and 2188
  has 216 (the store's `extra_data`). The one-loop runs reach no gap. Prompt 02's measurement of the gap's reach
  starts here.

## 7. State handed to the next prompt

For 02 (measure the law):
- **The options.**
  - `tools/history_and_bbn.py BETA M --drain-law one_loop --alpha A [--gap-prescription zero|edge]` tunes as
    `main.py` does; `--fixed-normalization NORM PHI_REF_MP` gives one untuned integration.
  - In code: `tune_history(..., alpha=A, drain_law="one_loop")` with `ScalarHistory._GAP_PRESCRIPTION` set in the
    process.
  - `tools/outward_step_scan.py` does not take the law. A one-loop batch needs the law passed through, as
    `.documents/one-loop-drain/law/one_loop_run.py` does.
- **The payload.**
  - `"drain"` is `DrainRecord(law, A, gap_prescription, unresolvable_assessments, candidates, unresolved_declined)`.
  - `candidates` is a tuple of `DrainCandidate(regime, ell, alpha_eff, w_gate, log_E, log_H)`, parallel to
    `"injections"`.
  - The regime shares come from `Injection.delta_K` by regime over the ledger's K(N_final). The parking leftover is
    `"parking"`'s delta_K.
- **The costs.** Laboratory M under the one-loop law takes 2–10 s an integration, against 400–500 s at α = 10⁻³ under
  the fixed law. M = 10⁻¹² M_P takes 24–37 s (3016, 3233) and 113–119 s (3873). A tuning takes 3–16 integrations.
- **What bears on 02's sample.**
  - Every laboratory-M candidate measured is a floor reflection (w_gate = 1), so the gate never bit (U9's question).
  - No gap candidate and no ℓ ≤ 0 arose. No unresolvable assessment arose under the one-loop law.
  - The large-M one-loop histories drain nothing and may decline unresolved reflections late (3873: 19).
  - Under the fixed law, 2197 and 2188 answer 131 and 46 unresolvable assessments.
  - 4389 (β 1.13, M 10⁻¹², α 0.01) still fails tuned: "the effective minimum was not found"
    (`[04-a-recomputed-history-does-not-find-the-effective-minimum]`).
