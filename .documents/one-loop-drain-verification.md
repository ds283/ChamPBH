# one-loop-drain — verification

**Campaign:** [`prompts/one-loop-drain/`](../prompts/one-loop-drain/README.md) · **Board:**
[`IMPLEMENTATION_STATE.md`](../prompts/one-loop-drain/IMPLEMENTATION_STATE.md) · **Source:** the drain probe
[`source/drain-probe_2026-11-02.md`](../prompts/one-loop-drain/source/drain-probe_2026-11-02.md).

This document holds the campaign's measurements (README §8.6, P16). It is additive (`CLAUDE.md` invariant 6): a
later run that supersedes a figure gets a new dated subsection, and the figures below stay as they are, correct for
the tree they were taken on.

## 1. Prompt 01 — the drain law (2026-11-03)

**Tree:** branch `one-loop-drain` at `b1415a3` plus prompt 01's changes, committed as prompt 01's commit
(`b99f6a4`). Python 3.13.16, SciPy 1.17.0, NumPy 2.3.5. **Scripts:** [`one-loop-drain/law/`](one-loop-drain/law/), each
with a docstring. **Committed outputs:** [`one-loop-drain/law/out/`](one-loop-drain/law/out/). **Raw output** (not
committed): `var/one-loop-drain/01/`, with the batches' `.jsonl`, `.log`, `.pid`, `.launch` and `.end`. The first
run there was made on the tree before the user's ruling (b) of §1.7; the run on the final tree is in
`var/one-loop-drain/01/final/`. Both are recorded, and §1.9 compares them.

### 1.1 What prompt 01 changes

- **The drain law** (README §8.2, P22): `integrate_scalar_history`, `integrate_history` and `tune_history` take
  `drain_law`, either `"fixed"` (the default, today's α_eff = α w_gate) or `"one_loop"`. Under `"one_loop"`,
  `alpha` carries A, which must be finite and > 0 (`check_drain_law`).
  - Under the one-loop law each candidate is classified in the loop by `drain_regime`, which is peak_estimate's
    classification computed once. Its α_eff is then `drain_alpha_effective(regime, ℓ, A, w_gate)`:
    - one-loop: min(0.99, A·2.7/ℓ)·w_gate;
    - gap: by `_GAP_PRESCRIPTION`, either 0 (zero) or min(0.99, A·2.7/4)·w_gate (edge);
    - classical and unassessed: 0.
  - The payload gains `"drain"`, a `DrainRecord`. `history_margins` reuses the loop's PeakEstimates (P18).
  - Nothing is stored: `build_extra_data`, the factories and the drivers are unchanged.
- **The wall-search fix** (README §8.8, P21): `_turning_point_unresolvable(ln V_floor, ln E)`, and
  `classical_turning_point` returning None where it holds (§1.4).
- **The user's ruling (b) on the inside-wall stop** (§1.7): an unresolved reflection that G2 would refuse is
  declined at its step start.
- **The tool route** (P25): `tools/history_and_bbn.py --drain-law` and `--gap-prescription`.

### 1.2 The suites

| suite | before (06:57 UTC, load 3.20 / 3.29 / 4.12) | after, final tree (08:48 UTC, load 9.00 / 10.58 / 10.72) |
|---|---|---|
| CosmologyModels | 26, 64.7 s | 26, 66.3 s |
| ComputeTargets `test_[a-f]` | 122, 136.4 s | 163 (+41, `test_drain_law`), 136.9 s |
| ComputeTargets `test_[g-z]` | 392, 460.3 s | 403 (+11, `test_unresolvable_turning_points`), 466.0 s |
| ObjectFactories | 130, 80.7 s | 130, 98.3 s |
| tools | 147, 157.3 s | 156 (+9, `test_history_and_bbn_drain_law`), 178.2 s |
| RunDrivers | 68, 22.4 s | 68, 27.5 s |

All pass. The loads at each suite's start are in prompt 01's log (§4). No existing test was edited.

### 1.3 The Law gate under the fixed law (prompt §2.1)

1. **The goldens.** `test_golden_history`, `test_golden_reflecting_history`, `test_tuning` and
   `ObjectFactories/tests/test_store_fingerprint_golden.py` (50 tests) pass unedited on the first tree (142 s) and on
   the final tree (149 s).
2. **The replay.** `reflection-restart` 04's 42 tasks were run through the scanner (`--jobs 6 --timeout 1800`).
   All 42 equal their lines in `var/reflection-restart/04/the-42.jsonl` in outcome, both SHA-256 digests,
   n_samples, accepted_steps, RHS_evaluations, reflections and restart_retakes. 4389 fails with the same reason. This
   holds on the first tree ([`out/compare_replay.txt`](one-loop-drain/law/out/compare_replay.txt)) and on the final
   tree ([`out/compare_replay-final.txt`](one-loop-drain/law/out/compare_replay-final.txt)).
3. **The fix answers the failing calls** (§1.4): 19 of 19.
4. **2197 and 2188 at their stored normalization, α = 10⁻³, now complete**
   ([`out/fixed_law.txt`](one-loop-drain/law/out/fixed_law.txt), first tree):

   | history | wall (s) | unresolvable assessments | reflections | parking N | r_χ today | ΔN_eff today |
   |---|---|---|---|---|---|---|
   | 2197, this tree | 455.8 | 131 | 6648 | 45.15719176 | 8.651957e-4 | 0.006443996 |
   | 2197, the 2026.8.0 store | 624.3 (compute_time) | — | 6648 | 45.15719185 | 8.651959e-4 | 0.006443997 |
   | 2188, this tree | 399.5 | 46 | 5741 | 43.80732962 | 3.167920e-4 | 0.002359473 |
   | 2188, the 2026.8.0 store | 652.7 (compute_time) | — | 5741 | 43.80732951 | 3.167920e-4 | 0.002359473 |

   The store's figures are given as a measurement, not a bound. These histories are not bitwise to the store,
   because `reflection-restart` 03's F3 moved them; their accepted steps are 929806 against 906090, and 817087
   against 795508. The unresolvable counts come from `predicate_cost.py`'s run of the same tasks, whose sample
   digests equal the scanner's.
5. **The nine** of `reflection-restart` 04 §0.2, tuned, `--timeout 3600`: all nine succeed. That includes the
   eight that failed on the fixed tree in `reflection-restart` 04 with "not monotone". The three at α = 10⁻³ take
   468–476 s each, in one integration. There was no timeout and no wall-search failure. Their parking N and r_χ
   today are in `out/fixed_law.txt`.
6. **The predicate's cost**, measured by a timer around every call, which bounds it from above:

   | history | calls in `classical_turning_point` | calls in the step loop | total | share of wall time |
   |---|---|---|---|---|
   | 2197 | 856147 | 131 | 0.387 s of 463.0 s | 8.36 × 10⁻⁴ |
   | 2188 | 752752 | 46 | 0.318 s of 387.2 s | 8.22 × 10⁻⁴ |

   Both are under the 1% stop. The ruling-(b) change does not touch the predicate.

### 1.4 The wall-search fix (prompt §1.6)

**The predicate.** `_least_finite_log_V_minus_floor(ln V_floor)` steps up from ln V_floor with `math.nextafter` to the
first argument at which `_log_V_minus_floor` is finite, and returns that value. It is cached per
(ln V_floor, `WALL_ROUNDING_QUANTA`). The predicate is `ln E <` that value.
- At both normalizations of the tests the walk takes 17 steps (K + 1) to ln V − ln V_floor = 17 ulp.
- At the laboratory-M normalization (−276.63072859401797), the least value is −304.29599119459886 and the last ln E
  for which the predicate is True is −304.2959911945989.
- At M = 10⁻¹² M_P (3233's, −276.63073574674894), they are −304.2959983473298 and −304.2959983473299.
- The test checks over 10⁴ floats that `_log_V_minus_floor` does not fall below its first finite value.

**Why every input it accepts raised before.** Where the predicate holds, every finite residual of the search is
positive and every other residual is −∞. The search starts at r₀ = −∞, since `classical_turning_point` returns None
first where r₀ ≥ 0. Its outer end therefore stays −∞, and every path through `_search_log_field` raises: "not
monotone", "the bracket's outer end is still −inf", or no bracket. So the rule changes only calls that raised. The
full argument is in the function's docstring. The search, `WALL_ROUNDING_QUANTA` and `WALL_SEARCH_HALVINGS` are
unchanged.

**The 19 failing inputs** ([`out/failing_inputs.txt`](one-loop-drain/law/out/failing_inputs.txt), measured on the fix
alone, before the law was written). Each is a literal of `test_unresolvable_turning_points`, and the script checks it
against its source:
- 2197 and 2188, from the drain probe's logs;
- the nine stored failures, from the 2026.8.0 store, read-only;
- the eight failures of the nine on the fixed tree, from `var/reflection-restart/04/the-nine.jsonl`.

For every input, the predicate holds, the call now returns None, and the pre-fix copy raises with its source's
message: equal to it for the probe's two, and extending its stored prefix for the other 17. The root margins agree
with `reflection-restart` 04's: 1.5107 and 1.5830 quanta (2197, 2188), 0.1058–16.5357 (the nine stored), and
0.6528–6.6963 (the eight).

**Every caller.**
- `RealizedModel.classical_turning_point` passes through.
- `assess_infall` returns (ln E, None).
- The step loop already read None as "no decision", and the infall's latest assessment stands. It now counts the
  assessment (`DrainRecord.unresolvable_assessments`). In `assess_infall`, ln E is at least the finite
  ln(V − V_floor) at φ wherever that is finite, so where the predicate holds the None is the fix's, never the
  r₀ ≥ 0 test's.
- Natural mode's `natural_floor_warning` caught the raise as `WARNING_WALL_UNASSESSED`. It now takes φ for φ_t, and
  warns "below the threshold" where φ < 10⁻⁸ M_P. That changes a warning line, never a value. The test shows it on a
  constructed floor reflection on the P1 harness (`TestTheUnresolvableCount`): the run is bitwise today's, the warning
  kind moves from unassessed to below, and the count is 1.
- `one_loop_turnaround` runs its own search and does not call `classical_turning_point`, so no one-loop
  classification moves.

### 1.5 The Law gate under the one-loop law (prompt §2.2)

All of these runs are at each history's stored normalization, built as the drain probe builds it
([`one_loop_run.py`](one-loop-drain/law/one_loop_run.py)), and compared by
[`probe_table.py`](one-loop-drain/law/probe_table.py)
([`out/probe_table.txt`](one-loop-drain/law/out/probe_table.txt) on the first tree,
[`out/probe_table-final.txt`](one-loop-drain/law/out/probe_table-final.txt) on the final one).

1. **The probe's table.** With `gate_weight` patched to 1.0 (the probe's law), 2197 and 2188 at A = 1 and 0.1, gap
   zero, equal the probe bitwise in every candidate's (N, α_eff, ΔK), in order, and in the candidate and drain counts,
   by_regime's counts and its ℓ and α_eff extremes, the parking N, r_χ today and ΔN_eff today:

   | run | candidates = drains | parking N | r_χ today | ΔN_eff today | wall (s) |
   |---|---|---|---|---|---|
   | 2197, A = 1 | 10 | 33.123237745059676 | 8.218413e-3 | 0.06121091 | 2.0 |
   | 2197, A = 0.1 | 95 | 38.44696564046158 | 1.532300e-3 | 0.01141260 | 9.0 |
   | 2188, A = 1 | 15 | 34.786619586267996 | 7.647890e-4 | 0.005696164 | 2.3 |
   | 2188, A = 0.1 | 92 | 37.45221407328659 | 3.554132e-4 | 0.002647125 | 9.5 |

   On the production code (U9, the gate kept) no candidate's α_eff differs from the probe's. Every candidate of these
   four runs is a floor reflection, unresolved, with w_gate = 1.0 exactly, which is README §0.1's statement, measured.
   Every candidate is one-loop.
2. **Both prescriptions.** 2197 at A = 1 with gap edge equals gap zero in every candidate and in the summary. There is
   no gap candidate. The records name `'zero'` and `'edge'`.
3. **Large M at A = 1.** Every α_eff is 0, all candidates are classical, and both runs are bitwise the probe's:
   - 3016: 330 candidates, 330 reflections, parks at N = 40.42807, r_χ today 9.918 × 10⁻⁷;
   - 3233: 301 candidates, 228 reflections, does not park, r_χ today 0.
4. **3873:** §1.7.
5. **The `"drain"` key.** In every run there is one record per Injection, in order. The regime agrees with
   peak_estimate's at every candidate, and the reused PeakEstimate equals a fresh one at every candidate. No one-loop
   solution with ℓ ≤ 0 arises: `one_loop_turnaround` raises where ℓ ≤ 0, so P24's branch cannot be reached with this
   model. Every run has unresolvable assessments = 0.

### 1.6 The cost under the one-loop law

Each laboratory-M integration takes 2–10 s, against 400–500 s under the fixed law at α = 10⁻³. 3016 and 3233 take
24–37 s, and 3873 takes 113–119 s after ruling (b).

### 1.7 3873 and the inside-wall guard (prompt §1.7)

**The stop.** On the tree before ruling (b), 3873 (M = 10⁻¹² M_P, β = 1.11) under the one-loop law at A = 1 stopped at
N = 43.76462032 with the probe's message to the character: "reflection requested inside the wall",
W/(π²/2) = 1.485. Observed by [`inside_wall_3873.py`](one-loop-drain/law/inside_wall_3873.py)
([`out/inside_wall_3873.txt`](one-loop-drain/law/out/inside_wall_3873.txt)):
- **The run.** It asked for 1440 reflections, 1177 unresolved and 263 floor; the largest floor-branch ratio was 0.085.
- **The failing reflection.** It was unresolved, judged on its own step start's assessment, with
  ln E = ρ_kin + W at that state exactly. There φ_t = 9.9805 × 10⁻⁹ M_P, ln R = −215.13 and φ = 1.6734 φ_t.
- **Why the field is inside the wall.** With nothing drained, E/V₀ ≈ e^{−9.2} at N ≈ 43.8. The wall is in its linear
  tail, V − V_floor ≈ V₀M/φ, so W/E = φ_t/φ and W/ρ_kin = 0.5 at 3φ_t (the median over the 1440 reflections is 0.512).
  G2 refuses wherever the rule fires below 2φ_t.
- **Why the rule fired that deep.** The rule needs φ < 3φ_t **and** φ_t < 10⁻⁸ M_P (ln R < 0). Along the failing
  infall φ_t falls from 1.80 to 0.998 times the threshold. The second condition first holds at the 30th step start,
  where φ is already 1.67 φ_t.
- **How often.** 87 of the 1177 unresolved reflections fired late in the same way; the last three fired at 2.36, 2.13
  and 1.67 φ_t.
- **At the stored α = 0.03.** The field parks at N = 37.52 with E about e^{19} larger and φ_t ≈ 10⁻¹³ M_P, in the
  steep exponential part of the wall (ln W − ln E ≈ −6.4 at 3φ_t). All 34 of its unresolved reflections fire at the
  first step start below 3φ_t, and the largest ratio is 0.002.

**The same failure under the fixed law** ([`inside_wall_store.py`](one-loop-drain/law/inside_wall_store.py),
[`out/inside_wall_store.txt`](one-loop-drain/law/out/inside_wall_store.txt)). In the 2026.8.0 store, 21 of 3846
histories (of 128 failures) failed with this message. They span M = 10⁻¹⁵–10⁻¹² M_P, β 0.5–1.0 and α 0.01–0.15, with
W/(π²/2) from 1.0003 to 7.48, all at φ = 0.96–1.98 × 10⁻⁸ M_P, which is the threshold crossing again. This is
recorded as a measurement; the 21 were not re-run.

**The user's ruling (b), 2026-11-03:** "I see. I had assumed that was an energy balance that meant the field and
moved beyond its reflection point, but I see that it's not. In which case, (b) is correct."

G2 tests whether the mirror model is valid: it detects a field already stepped into the wall. It is not an energy
balance, since π → −π conserves E.

**What (b) is.** `unresolved_reflection_refused(policy, N, y)` evaluates G2 with `_reflection_guards`' own expressions
and comparison. Both now share `_wall_and_kinetic` and `_g2_refuses`; the guards' raise and message are unchanged.
- Where `unresolved_turnaround` fires but G2 would refuse, the step start makes no unresolved decision, and the count
  goes to `DrainRecord.unresolved_declined` (under either law).
- The floor rule is then tested as usual, and G2 applies to its reflection as before. Otherwise Radau integrates the
  turnaround itself, and the resolved candidate's drain point lies past the maximum of |π|, outside the wall.
- Every state where (b) acts raised before, and a failed integration ends a tuning as a failure. So no history or
  tuning that succeeds today moves. §1.9 measures this.

**After (b)** ([`out/inside_wall_3873-final.txt`](one-loop-drain/law/out/inside_wall_3873-final.txt),
`out/probe_table-final.txt`):
- **3873** under the one-loop law at A = 1 completes in 113 s. It has 19 declined step starts, 1439 reflections and
  2486 candidates, all classical, so nothing is drained. It does not park, and r_χ today is 0.
- **3016 and 3233** are bitwise their first runs, with no step start declined.
- **3460 and 4397,** two of the 21 stored failures that still failed on the tree before (b), run tuned under the fixed
  law ([`store_failures.py`](one-loop-drain/law/store_failures.py); `var/one-loop-drain/01/final/store-failures/`):

  | history | outcome | integrations | wall (s) | declined (accepted integration) | reflections | parking N | r_χ today | ΔN_eff today |
  |---|---|---|---|---|---|---|---|---|
  | 3460 (β 0.75, α 0.1) | success | 3 | 36.0 | 4 | 123 | 41.4412 | 0.4722 | 3.517 |
  | 4397 (β 1.0, α 0.01) | success | 16 | 608.2 | 13 | 302 | none | 0.04324 | 0.3220 |

**The tests** (`TestTheDeclinedUnresolvedReflection`):
- the decision on synthetic states either side of G2's boundary, against `_reflection_guards` on the same states;
- a window of 3873 from the state just after its reflection at N = 43.763255440356026, to N + 0.0025. The rule is
  declined 9 times and the infall goes on to a wall-side turning point and a resolved drain-point candidate. With
  the decision forced as before the ruling, the same window raises "reflection requested inside the wall";
- the count.

### 1.8 The tool route

`tools/history_and_bbn.py`:
- `--drain-law {fixed,one_loop}`. Under `one_loop`, `--alpha` is A, which must be finite and > 0.
- `--gap-prescription {zero,edge}`, accepted under `one_loop` only. It sets `_GAP_PRESCRIPTION` in the tool's own
  process for the integration and restores it afterwards.
- Under `one_loop` the history line names `drain_law=one_loop A=… gap=…`, and a `drain` line follows the dissipation
  line: counts by regime, K shares by regime and of the parking leftover, ℓ_min, unresolvable assessments and declined
  reflections.
- Under the fixed law every line is as before: `test_history_and_bbn` and `test_history_and_bbn_alpha` pass unedited.

### 1.9 The final tree against the first run

[`compare_final.py`](one-loop-drain/law/compare_final.py),
[`out/compare_final.txt`](one-loop-drain/law/out/compare_final.txt).
- The 42, the pair and the nine are equal task by task to the first run, in every field compared in §1.3, for every
  task that succeeded first. 4389 fails as before.
- Every one-loop run that succeeded first is equal to it in every candidate, count, parking N, r_χ and ΔN_eff, with
  no step start declined.
- 3873, which failed first, now completes. The goldens pass on the final tree.

### 1.10 The Law gate

The gate holds:
- **Fixed law:** every golden passes bitwise; the payload is unchanged but for `"drain"` (the test pins the key set);
  the replay equals `reflection-restart` 04's.
- **One-loop law:** 2197 and 2188 reproduce the probe's table bitwise at A = 1 and 0.1, and the production gate
  changes nothing, since every candidate is unresolved; 3016 and 3233 drain nothing.
- **3873:** the failure is explained, and after the user's ruling (b) it completes.

## 2. Prompt 02 — measure the law (2026-11-03)

**Tree:** branch `one-loop-drain` at `b1d6bc5`; its computing code is `b99f6a4`'s (the Gate's diff is empty). Prompt 02's
commit (`3f3f0cf`) adds no code outside [`one-loop-drain/measure/`](one-loop-drain/measure/). Python 3.13.16, SciPy
1.17.0, NumPy 2.3.5. **Scripts:** [`one-loop-drain/measure/`](one-loop-drain/measure/), each with a docstring.
**Committed outputs:** [`one-loop-drain/measure/out/`](one-loop-drain/measure/out/). **Raw output** (not committed):
`var/one-loop-drain/02/`:
- `sample.jsonl`: the 180 task lines, 110 run and 70 derived;
- `spot.jsonl`: the spot runs and the tool checks;
- `tasks.json` and `fixed_law.json`;
- `tasks/`: each task's stdout (`<id>.log`), stderr, partial file and result;
- `batches/<step>/`: each batch's `batch.pid`, `batch.log` and `batch.end`, beside the `.ids` files.

Every number below comes from a file of `measure/out/`, or from the line of `sample.jsonl` that it names. The orchestrator
note's rulings 1–10 governed. No store was written and no Ray was started. The 2026.8.0 store was opened `mode=ro` only,
by `tasks.py` and `fixed_law.py`, through its main shards (`science-2026.8.0-shard*.db`; ruling 8).

### 2.1 The Gate and the suites

The Gate was checked again at `b1d6bc5` before anything ran, and it held:
- the tree was clean;
- `b99f6a4` and `d7cef0e` are ancestors of `HEAD`, and the code-path diff from `b99f6a4` is empty;
- `runregistrykit list` showed 23 runs, all finished;
- this document had §1 only, and `measure/` did not exist;
- all 25 serials of §1.1 are in the shards named, validated successes, with the table's M (relative 2.2 × 10⁻¹⁶ or 0),
  β and α ([`out/tasks.txt`](one-loop-drain/measure/out/tasks.txt)).

The suites, serially, before anything else:

| suite | count | time | load at start |
|---|---|---|---|
| CosmologyModels | 26 OK | 67.7 s | 12.99 |
| ComputeTargets `test_[a-f]` | 163 OK | 139.1 s | 20.74 |
| ComputeTargets `test_[g-z]` | 403 OK | 483.8 s | 7.67 |
| ObjectFactories | 130 OK | 83.5 s | 5.71 |
| tools | 156 OK | 162.9 s | 4.64 |
| RunDrivers | 68 OK | 23.6 s | 6.33 |

The suites after the batches are in the log (§4 there). The counts are unchanged.

### 2.2 The driver, and its checks

[`run_sample.py`](one-loop-drain/measure/run_sample.py) runs each task in a fresh `./venv/bin/python` (ruling 3). In that
process it calls `tools.history_and_bbn.main(argv)`, with argv `BETA M --drain-law one_loop --alpha A
--gap-prescription G`. It makes three captures around the call:
- `ScalarHistory.integrate_scalar_history`, which reduces each integration at once and appends it, flushed, to the
  task's partial file (ruling 4);
- `tune_history`, to keep the payload;
- `compute_BBN_data`, through a stand-in whose `_function` calls the real one.

The parent runs at most the batch's worker count at once. It reaps each child with `os.wait4`, whose `ru_maxrss` is the
line's `peak_rss`. A child past 3600 s gets `SIGTERM`, then `SIGKILL` 30 s later. No task reached the timeout. The
timeout path was exercised once before the batches, with the limit set to 8 s by an environment variable, on 3873: the
line was written with outcome `timeout` and the child was reaped.

**The tool checks (ruling 9; [`out/tool_check.txt`](one-loop-drain/measure/out/tool_check.txt)).** For `P-2197-A0.1-zero`
and `L-M1e-16-A1-zero`, `tools/history_and_bbn.py` was run in its own subprocess with the same argv. Its stdout equals the
driver task's line for line once `wall=` and `PRyM_time=` are masked: 13 of 13 lines, and 15 of 15. The `history`,
`drain` and `bbn` lines carry the driver's JSON values, 22 of 22 in each. **The driver agrees with the tool.**

**The spot checks (§1.2, ruling 6; [`out/spot_check.txt`](one-loop-drain/measure/out/spot_check.txt)).** Each was compared
in every value of the line but the wall times, the peak RSS, PRyMordial's time and the record's own field. Each was also
compared in the full regime sequence and the injections' (N, α_eff, ΔK) of every integration, from the partial files.

| shortcut | task run | against | integrations | candidates | result |
|---|---|---|---|---|---|
| S1 | `P-2197-A1-edge` | `P-2197-A1-zero` | 1 | 10 | bitwise equal; the record's `gap_prescription` 'edge' against 'zero' |
| S1 | `R-865-A0.1-edge` | `R-865-A0.1-zero` | 1 | 78 | bitwise equal |
| S2 | `C-3016-A0.01-zero` | `C-3016-A1-zero` | 1 | 330 | bitwise equal; the record's `A` 0.01 against 1.0 |
| S2 | `C-3233-A0.1-zero` | `C-3233-A1-zero` | 3 | 903 | bitwise equal (the step-1 run, under ruling 1) |

S1 applied to both histories the prompt names, so no substitute was needed. **Ruling 1's free check of S2:** S2(zero)
holds for L-M5e-16, L-M4e-15, C-3016, C-3233 and C-3873. In each of the five, the run at A = 0.1 equals the run at
A = 1 under ruling 6 ([`out/decisions.txt`](one-loop-drain/measure/out/decisions.txt), step 1b).

**The r_χ form of ruling 7.** On `P-2197-A1-zero`, K(N_final) e^{−4N_final}/ρ_R,E(last sample) is 0.008218413249662701.
`rho_chi_ratio_today` is 0.008218413249662944, a relative difference of −2.95 × 10⁻¹⁴. The last sample's raw N equals
N_final ([`out/regimes.txt`](one-loop-drain/measure/out/regimes.txt), last section).

### 2.3 The batches and the projection

The order is ruling 1's ([`out/decisions.txt`](one-loop-drain/measure/out/decisions.txt) is decide.py re-run on the final
sample; it gives the same id files as the batches ran). Every batch was launched with `nohup`, checked once that it
had started, and waited on by a command that ended with it.

| batch | tasks | workers | start (UTC) | wall | loads at start | loads at end | largest task RSS |
|---|---|---|---|---|---|---|---|
| step1a: zero, A = 1 then 0.1 | 60 | 6 | 14:44:36 | 2155.6 s | 4.82 8.03 13.35 | 6.22 8.54 10.37 | 1348 MiB |
| step1b: edge at A = 1, 0.1 not covered by S1 | 8 | 4 | 15:05:34 | 964.5 s | 7.49 9.16 10.70 | 5.86 8.02 10.04 | 1354 MiB |
| step2a: zero, A = 0.01, P's two | 2 | 2 | 15:21:56 | 914.1 s | 6.03 7.93 9.96 | 5.14 5.26 6.76 | 1354 MiB |
| step2b: zero, A = 0.01, R B W L less S2's | 23 | 6 | 15:37:26 | 1856.5 s | 4.89 5.20 6.71 | 5.35 11.50 12.67 | 1351 MiB |
| step3: edge at A = 0.01 not covered by S1 | 17 | 6 | 16:08:46 | 1934.9 s | 5.29 11.08 12.49 | 10.34 11.21 12.00 | 1352 MiB |
| step4: spot runs and tool checks | 5 | 5 | 16:41:23 | 46.2 s | 7.90 10.59 11.76 | 6.21 9.82 11.42 | 289 MiB |

Step1b was launched while 3873's two zero tunings were still running in step1a. It covered the 29 other histories,
decided once their two zero runs had ended, and used 4 workers, so that 6 ran in all. 3873 was decided after step1a
ended and needed no edge run (log §2, D3). From the first launch to the last end took 1 h 58 min. The 110 runs took
31 367 s of task wall time.

**Ruling 5's projection**, after P's two at A = 0.01:
- walls 910.1 and 914.1 s, a mean of 912.1 s;
- peak RSS 1.307 and 1.322 GiB, so the workers are min(6, ⌊8 GiB / 1.322 GiB⌋) = 6;
- 25 zero tasks (W, R, B, L) plus 27 edge tasks (P, W, R, B, L) make 52;
- 52 × 912.1 s / 6 = 7905 s = **2.20 h**, against the 4 h limit and the floor of three workers.

So it did not stop. In the event, 23 zero tasks and 17 edge tasks ran at A = 0.01: S2 spared 2 of the zero tasks
counted, and S1 spared 10 of the edge tasks. Steps 2b and 3 took 1.05 h together.

### 2.4 The sample measured (§2.1 of the prompt)

[`out/sample.txt`](one-loop-drain/measure/out/sample.txt): 180 task lines.
- 110 were run: 81 succeeded and 29 failed, with none timed out, crashed or in error;
- 70 were derived: 65 by S1 and 5 by S2.

| failure | where | count |
|---|---|---|
| step budget exhausted (2 × 10⁶ accepted steps) | A = 0.01: 4 zero (P-2188, W-1631, W-2209, W-2185) and 11 edge (P-2197, all ten W); A = 0.1: R-632 edge, L-M5e-20 zero and edge | 18 |
| "the effective minimum was not found" after the seed integration | A = 0.01: 9 zero (P-2197, W-2187, -2179, -2078, -2192, -2183, -2172, -2178, R-632) and R-632 edge; A = 0.1: R-632 zero | 11 |

None is the wall search or the inside-wall guard. None points at `drain_regime` or `drain_alpha_effective`. Each failed in
its seed integration (the step budget) or right after it (the effective minimum). Every laboratory-M history in the transition
window (P, W, and R-632 at β = 1.0) fails at A = 0.01 under both prescriptions, except P-2188 under `edge`, which
succeeds in 54 s. R-632 also fails at A = 0.1. So does the ladder's 5 × 10⁻²⁰ M_P, which succeeds at A = 1 and 0.01.
The step-budget failures stopped at N = 43.0–47.7, after 13 573–14 638 reflections. The P and W integrations at
A = 0.01 met 9992–13 497 classical candidates (§2.5).
They are opened as an issue on the board (§3, `[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]`).

### 2.5 The regime shares and the gap's reach (§2.2 of the prompt)

[`out/regimes.txt`](one-loop-drain/measure/out/regimes.txt) holds, per history, A and prescription:
- the candidates by regime;
- the K shares by regime and the parking leftover's;
- the drains, ℓ_min, the least ln R at a drain, and U9's count;
- the regimes met, with the gap's reach and the R·S(L) bound.

The regimes met by the zero tuning over all its integrations, failures included:

| group | ln R_first | A = 1 | A = 0.1 | A = 0.01 |
|---|---|---|---|---|
| P, W (β 1.06–1.1875, 4 × 10⁻³¹) | 101.7–138.1 | one-loop only, 9–19 drains, then parks | one-loop only, 90–96 drains | gap and R ≤ 1 in every one: 167–214 one-loop, 420–713 gap, 9992–13 497 classical; every tuning fails |
| R-632 (β 1.0) | 49.9 | one-loop only, 13 | gap (200) and R ≤ 1 (4515); fails | gap (40) and R ≤ 1 (353); fails |
| R-1193, R-865, R-611 (β 1.15, 2, 10) | 137.7–166.3 | one-loop only | one-loop only | one-loop only (64–662 drains) |
| B, 10⁻³³ and 10⁻³⁰ (β 1.2, 2, 10) | 134.5–190.3 | one-loop only | one-loop only | one-loop only (30–664 drains) |
| L-M5e-20 | 40.16 | one-loop only, 14 | gap and R ≤ 1; fails (step budget) | gap at candidate 7 (ln R = 2.66), R ≤ 1 at 9 |
| L-M8e-18 | 19.86 | gap at 3 (ln R = 6.67), R ≤ 1 at 161 | gap at 5, R ≤ 1 at 8 | gap at 4, R ≤ 1 at 6 |
| L-M1e-16 | 9.758 | gap at 1 (ln R = 3.97), R ≤ 1 at 10 | gap at 1, R ≤ 1 at 4 | gap at 1, R ≤ 1 at 4 |
| L-M5e-16 | 3.320 | gap at 0, R ≤ 1 at 1 | the same (S2 holds) | the same (S2) |
| L-M4e-15 | −4.998 | R ≤ 1 from the first impact | the same | the same |
| C (10⁻¹²) | −6.2, −119.6, −33.1 | R ≤ 1 from the first impact; nothing drains | the same | the same |

- **The ladder's ln R_first** against §1.1's targets of 40, 20, 10, 3 and −5 are 40.16, 19.86, 9.758, 3.320 and
  −4.998.
- **K shares.** Wherever the cascade stays one-loop, the one-loop share is 0.9611–1.0000 and the parking leftover takes
  the rest:
  - 1.9 × 10⁻⁸ to 8.6 × 10⁻⁴ at A = 1 (P, W, R, B);
  - 4.7 × 10⁻⁴ to 5.7 × 10⁻³ at A = 0.1;
  - 8.3 × 10⁻³ to 3.9 × 10⁻² at A = 0.01.

  Where nothing drains but the parking (L-M5e-16 and L-M4e-15 at gap zero, C-3016), the parking leftover is all of K.
  3233 and 3873 do not park and have K = 0. Under `edge` the gap share is:
  - 0.0002–0.054 for the ladder's 8 × 10⁻¹⁸ and 5 × 10⁻²⁰;
  - 0.009–0.445 for 10⁻¹⁶;
  - 0.9975–1 for 5 × 10⁻¹⁶;
  - 0.026 for P-2188 at A = 0.01.
- **ℓ_min** at laboratory M is 5.4–10.2 at A = 1 and 1.55–2.43 at A = 0.1 (P, W); R-632 at A = 1 has 0.473. Before
  the gap: 0.147 (P-2188, A = 0.01, edge), 0.59–4.0 on the ladder.
- **U9:** no one-loop candidate has w_gate < 1, 0 of 7203 over the 81 successful runs' accepted integrations, and no
  gap candidate either, 0 of 535. **P19:** no candidate is unassessed, 0 of 34 664.
- **Unresolvable assessments and declined reflections** in the accepted integrations: only 3873, 27 declined at every A.
  In failed integrations: R-632 A = 0.1 zero had 6 unresolvable; W-2187 and W-2178 at A = 0.01 zero had 1 each; L-M5e-20
  A = 0.1 zero declined 1.
- **The R·S(L) bound** (ruling 7):
  - no classical candidate in any successful P, W, R or B run;
  - on the ladder where a one-loop or a gap candidate drains, its sum is 3.3 × 10⁻⁸ to 9.5 × 10⁻⁴ of K. As an r_χ today
    it is at most 1.3 × 10⁻⁷, against an r_χ today of 1.3 × 10⁻⁴ to 0.02;
  - where only the parking drains (L-M5e-16 zero, L-M4e-15, C-3016), it is 0.142, 0.0033 and 1.4 × 10⁻⁴ of K. Those
    shares are of the parking leftover;
  - where K = 0 (3233, 3873) the share is None. As an r_χ today it is 0 for 3233, whose 301 classical candidates all lie
    at L < 40, and 9.3 × 10⁻²¹ for 3873, of which 2 lie in range and 2487 outside;
  - outside S(L)'s range: L-M1e-16 at A = 0.1 zero has 9714 of 9729 outside (L down to 10⁻⁸), and L-M5e-16 at A = 1
    edge has 9932 of 9962.

### 2.6 The spread between the prescriptions (§2.3 of the prompt)

[`out/spread.txt`](one-loop-drain/measure/out/spread.txt). The prescriptions differ only where the cascade enters the gap.
Elsewhere `edge` equals `zero` by S1, which held for 65 of the 90 (history, A) pairs. Both prescriptions succeeded in 10
of the pairs that entered the gap, all on the ladder. Where both ran:

| | largest edge − zero | where |
|---|---|---|
| Y_p | +1.306 × 10⁻³ (+0.53%) | L-M5e-16, A = 1 |
| D/H | +0.0454 × 10⁻⁵ (+1.8%) | L-M5e-16, A = 1 |
| ΔN_eff at 1 MeV | +0.0965 | L-M5e-16, A = 1 |
| ΔN_eff today | +0.0995 | L-M5e-16, A = 1 (zero: 6.7 × 10⁻⁶) |
| r_χ today | +0.0134 | L-M5e-16, A = 1 |

At 10⁻¹⁶ the ΔN_eff today rises with `edge` by 0.0014, 0.0197 and 0.0023 at A = 1, 0.1 and 0.01. At 8 × 10⁻¹⁸ it rises by
2.4 × 10⁻⁵, 0.0014 and 0.0003. At laboratory M only P-2188 at A = 0.01 succeeds under `edge`, and its `zero` fails, so
no laboratory-M pair can be compared. Both prescriptions succeed in no history of P, W or R-632 at A = 0.01.

### 2.7 Against the fixed law (§2.4 of the prompt)

[`out/fixed.txt`](one-loop-drain/measure/out/fixed.txt) sets each one-loop value beside every stored α at the same
(M, β): ΔN_eff at 1 MeV and today, r_χ today, Y_p, D/H and parking N. There are 261 references, 13 at each laboratory-M β
of block A2 and 4–7 elsewhere ([`out/fixed_law.txt`](one-loop-drain/measure/out/fixed_law.txt)), all at φ* = 5 M_P and
T* = 2 × 10⁴ GeV, small network. [`out/u14.txt`](one-loop-drain/measure/out/u14.txt) sets the values beside the stored α
of each named serial. The store's values were measured on the 2026.8.0 tree; they are not a bound. For example:
- **2197** (β 1.185): the fixed law at α = 10⁻³ gives ΔN_eff today 0.00644, Y_p 0.25840, D/H 2.8762. The one-loop
  law gives 0.0612, 0.25688, 2.5227 at A = 1, and 0.0114, 0.25862, 2.8623 at A = 0.1. The fixed law reaches 0.0612
  between α = 0.08 (0.0567) and 0.1 (0.0707).
- **W** at A = 1 gives ΔN_eff today 0.0137–0.0683, against the fixed law's 0.0036–0.0067 at α = 10⁻³. At A = 0.1 it
  gives 0.0047–0.0130.
- **R, B** (β ≥ 1.15) at A = 0.1 sit near the fixed law at α = 0.01: R-865 gives 0.00468 against 0.00548, B-2498 0.00481
  against 0.00548. R-865, which has the full α grid, at A = 1 gives 0.0455, between α = 0.08 (0.0424) and 0.1 (0.0524), and
  at A = 0.01 gives 0.00049, against 0.00056 at α = 10⁻³.
- **β = 1.0** (R-632, C-3233): Y_p 0.4345 and D/H 10.41 under both laws. ΔN_eff today is 0.337 for R-632 at A = 1,
  against 0.331 at α = 0.01. At 1 MeV it is 0 under both laws.
- **C (10⁻¹²):** ΔN_eff today is 7.4 × 10⁻⁶ for 3016, from its parking leftover, and 0 for 3233 and 3873. The fixed law
  gives 0.0020–0.0097 (3016), 0.33 (3233) and 0.0098–0.116 (3873). 3873's Y_p is 0.27307 under the one-loop law, against
  0.2585–0.2721 under the fixed law.

### 2.8 The cost (§2.5 of the prompt)

[`out/cost.txt`](one-loop-drain/measure/out/cost.txt). Per tuning:

| group | A = 1 | A = 0.1 | A = 0.01 |
|---|---|---|---|
| P, W | 14–16 s | 20–22 s | 729–979 s (each failing in its first integration); P-2188 edge 54 s |
| R | 14–22 s | 14–349 s; R-632 edge 965 s | 20–88 s; R-632 edge 42 s |
| B | 14–22 s | 12–122 s | 16–86 s |
| L | 14–38 s; edge 14–748 s | 16–924 s; edge 18–944 s | 18–20 s; edge 16–22 s |
| C | 50–86 s; 3873 1832 s | 48–88 s; 3873 1823 s | (S2) |

- **Integrations per tuning:** 105 of the 110 runs took one integration. B-2774 at A = 0.1 took 2, C-3233 took 3 at
  A = 1 and 0.1, and C-3873 took **16** at A = 1 and 0.1, at 110–118 s each.
- **Peak RSS** is about 250 MiB for a short task and about 1.35 GiB for one that reaches the step budget (2 × 10⁶
  accepted steps).
- **The batches** are in §2.3.

### 2.9 The handover for U13 and U14 (§2.6 of the prompt)

This is a measurement and the options it bears on, not a choice.

**U13 — the gap prescription of record.**
- **Where the prescriptions differ.** Only where the cascade enters the gap. On this sample that is:
  - the ladder: 8 × 10⁻¹⁸ and 10⁻¹⁶ at every A; 5 × 10⁻¹⁶ at every A, where the first impact is in the gap; and
    5 × 10⁻²⁰ at A = 0.1 and 0.01;
  - laboratory M at A = 0.01 (P, W) and R-632 (β = 1.0) at A = 0.1 and 0.01.

  At A = 1 and 0.1 it enters nowhere at laboratory M (β 1.06–10) or in block B; R-632 (β = 1.0) enters it at
  A = 0.1. Large M (3016, 3233, 3873) and the ladder's 4 × 10⁻¹⁵ never enter the gap; their cascade starts at R ≤ 1.
- **By how much.** Where both succeed (ladder only), Δ(ΔN_eff today) is up to +0.0995, Δ(Y_p) up to +1.3 × 10⁻³ and
  Δ(D/H) up to +0.045 × 10⁻⁵, all at 5 × 10⁻¹⁶, A = 1. There `zero` drains nothing but the parking leftover, and `edge`
  drains the one gap candidate at the first impact. At 10⁻¹⁶ Δ(ΔN_eff today) is 0.0014–0.0197.
- **The share of K in the gap under `edge`:** 0.0002–0.054 at 8 × 10⁻¹⁸ and 5 × 10⁻²⁰; 0.009–0.445 at 10⁻¹⁶;
  0.9975–1 at 5 × 10⁻¹⁶; 0.026 for P-2188 at A = 0.01.
- **What the sample cannot say.** At laboratory M the gap is reached only at A = 0.01. There every tuning fails under
  `zero`, and all but P-2188 fail under `edge`. So the prescriptions cannot be compared at laboratory M on this code.

**U14 — the A values of the rebuild of record.**
- **How the observables move with A** ([`out/u14.txt`](one-loop-drain/measure/out/u14.txt)). Laboratory M first (Fig 6):
  - ΔN_eff today falls roughly in proportion to A: P/W 0.0057–0.068 at A = 1, 0.0026–0.013 at A = 0.1; R and B
    (β ≥ 1.15) 0.0033–0.064 at A = 1, 0.0008–0.0061 at A = 0.1, 0.00016–0.00087 at A = 0.01;
  - Y_p and D/H move with ΔN_eff in the transition window (2197: Y_p 0.25688 → 0.25862, D/H 2.5227 → 2.8623 from
    A = 1 to 0.1), and hardly at β ≥ 2;
  - parking N moves later as A falls: 2197 from 33.12 to 38.45.
- **Which histories change regime between A values:**
  - P and W: one-loop only at A = 1 and 0.1; gap and R ≤ 1 at A = 0.01, where every tuning fails but P-2188's under
    `edge`;
  - R-632 (β = 1.0): one-loop at A = 1; gap and R ≤ 1 at A = 0.1 and 0.01, failing;
  - L-M5e-20: one-loop at A = 1; gap and R ≤ 1 at 0.1 (failing) and 0.01;
  - R-1193, R-865, R-611 and block B: one-loop at every A;
  - the ladder's 8 × 10⁻¹⁸ to 4 × 10⁻¹⁵ M_P, and C: the same regimes at every A.
- **What it bears on.** On this code A = 0.01 cannot be built at laboratory M for P, W and R-632 (β 1.0–1.1875): the
  step budget is reached, or the effective minimum is not found after the seed integration. It was built for R-1193,
  R-865 and R-611 (β 1.15, 2, 10) at laboratory M, in block B, on the ladder and at large M. At large M nothing drains at any A, so A has no effect
  there (S2).

### 2.10 The Measure gate

The gate holds, item by item (prompt §2):
1. **The sample measured.** All 180 tasks, by group, A and prescription, with S1 and S2 marked. The 29 failures are listed
   with their reasons, and there were no timeouts (§2.4).
2. **The regime shares and the gap's reach across M and β** (§2.5), with U9's count (0) and P19's (0).
3. **The spread between the prescriptions** for every pair where both succeeded, with the largest of each (§2.6). At
   laboratory M none could be compared.
4. **Against the fixed law** for every stored (M, β) (§2.7).
5. **The cost**: wall time and integrations by group and A, the batches, the workers, the loads and the peak RSS
   (§2.3, §2.8).
6. **The handover for U13 and U14** (§2.9).

## 2b. Prompt 02b — measure the law over a wider A (2026-11-04)

**Tree:** branch `one-loop-drain` at `62d3f2f`; its computing code is `b99f6a4`'s (the Gate's diff is empty), the code
§2 measured. Prompt 02b's commit (`cc1f449`) adds no code outside
[`one-loop-drain/measure-02b/`](one-loop-drain/measure-02b/), and §1 and §2 above are unchanged. Python 3.13.16.
**Scripts:** [`one-loop-drain/measure-02b/`](one-loop-drain/measure-02b/), each with a docstring. They import 02's
scripts from [`one-loop-drain/measure/`](one-loop-drain/measure/) and override some of their module globals; the
one copied function (02's `child`) differs from 02's by two lines (the log lists every import, override and copy).
**Committed outputs:** [`one-loop-drain/measure-02b/out/`](one-loop-drain/measure-02b/out/). **Raw output** (not
committed): `var/one-loop-drain/02b/`:
- `sample.jsonl`: 02b's 360 task lines, 171 run and 189 derived;
- `repro.jsonl` and `spot.jsonl`: the reproduction runs, the spot runs and the tool check;
- `tasks.json`: 540 tasks, 02's 180 among them unchanged;
- `tasks/`: each task's stdout, stderr, partial file and result (prefix `repro-` or `spot-` where so run);
- `batches/<name>/`: each batch's ids, `batch.pid`, `batch.log` and `batch.end`.

02's lines at A = 1, 0.1 and 0.01 are `var/one-loop-drain/02/sample.jsonl`, reused as they stand once the reproduction
check held (§2b.2). Every number below comes from a file of `measure-02b/out/`, or from the line of one of the two
`sample.jsonl` files that it names. Orchestrator note 02b's rulings 1–10 governed, with note 02's rulings where they
say so. No store was written and no Ray was started; the 2026.8.0 store was opened `mode=ro` by `tasks_wide.py` only,
through 02's `tasks.py`.

### 2b.1 The Gate and the suites

The Gate was checked again at `62d3f2f` before anything ran, and it held:
- the tree was clean, and `3f3f0cf`, `2732535` and `5ae8648` are ancestors of `HEAD`;
- the code-path diff from `b99f6a4` is empty;
- `runregistrykit list` showed 23 runs, all finished;
- 02's `sample.jsonl` has 180 lines, with every run's partial file, and the store's 16 shards exist;
- this document had §1 and §2 and no §2b, and `measure-02b/` and `var/one-loop-drain/02b/` did not exist.

[`out/tasks.txt`](one-loop-drain/measure-02b/out/tasks.txt): 02's store check, re-run, holds for all 25 serials, and
02's 180 tasks are in the 540 unchanged. The suites, serially, before anything else (2026-11-04, local time = UTC):

| suite | count | time | load at start |
|---|---|---|---|
| CosmologyModels | 26 OK | 71.7 s | 10.71 18.97 22.10 (15:47) |
| ObjectFactories | 130 OK | 83.8 s | (same chain) |
| RunDrivers | 68 OK | 27.3 s | (same chain; 7.04 13.22 19.02 at its end, 15:50) |
| ComputeTargets `test_[a-f]` | 163 OK | 141.4 s | 6.78 12.96 18.87 (15:50) |
| ComputeTargets `test_[g-z]` | 403 OK | 477.4 s | 17.12 14.84 18.56 (15:53) |
| tools | 156 OK | 160.6 s | 7.49 10.21 14.38 (16:01) |

The suites after the batches are in the log (§4 there).

### 2b.2 The reproduction check (ruling 4)

[`out/reproduce.txt`](one-loop-drain/measure-02b/out/reproduce.txt). 02's `P-2197-A1-zero` and `R-865-A0.1-zero`
were re-run by 02b's driver, `run_wide.py`, in one batch at two workers, before anything else ran. Each was compared
with 02's line under note 02's ruling 6, with neither the record's `gap_prescription` nor its `A` excused, and its
partial file with 02's, per integration, in every value but the wall and maxrss. The fields that 02b adds were left
out (each line's `accepted.first_impact`, each integration's two cap counts, and the partial file's `ells`).
- `P-2197-A1-zero`: **bitwise equal**; 1 integration, 10 candidates, samples SHA-256 `56eef32a…0737` in both, Y_p
  0.25688455988399567 and D/H 2.522682869802573 in both.
- `R-865-A0.1-zero`: **bitwise equal**; 1 integration, 78 candidates, samples SHA-256 `a2f3ddc5…eeca` in both, Y_p
  0.24690202527404065 and D/H 2.459245914546649 in both.
- Their stdout equals 02's, 13 of 13 lines each, once `wall=` and `PRyM_time=` are masked.

So 02's lines at A = 1, 0.1 and 0.01 are reused. As a probe of the comparison itself, the reproduced `P-2197-A1-zero`
against 02's `P-2197-A0.1-zero` reports 54 differences in the line and 11 in the partial file. Against 02's spot run
`P-2197-A1-edge` it reports only `drain.gap_prescription` (log §5).

### 2b.3 The driver, the shortcuts and their checks

**The driver** is 02's (note 02b ruling 3): `run_wide.py` imports 02's `run_sample.py`, moves its paths to
`var/one-loop-drain/02b/`, and launches each child through itself. A fresh process per task, the three captures, the
streamed partial files, the 3600 s timeout with `SIGTERM` and then `SIGKILL`, and the peak RSS by `os.wait4` are 02's.
The child also keeps each candidate's ℓ in the partial file and two cap counts per integration (ruling 6). No task
reached the timeout.

**S2, generalised (prompt §1.2).** Re-read on this tree: in `integrate_scalar_history` A is read only by
`check_drain_law` (:1933), as `DrainRecord.A` (:1948), in the tests `alpha > 0.0` / `alpha == 0.0` (:1961, :2066,
:2353, :2375, :2433, :2448, :2474, :2945, :3131), in `drain_alpha_effective` (:2195, under the one-loop law), and in
the fixed law's `alpha_effective` (:2130, :2134), which the one-loop law does not call. `integrate_history` reads it in
`check_drain_law` (:4430), passes it on (:4525), and tests `alpha > 0.0` (:4647); `tune_history` reads it in
`check_drain_law` (:5087) and passes it on (:5126). `check_drain_law` (:1662) has no upper bound under the one-loop
law. So the sites are note 02's, and any A > 0 makes the same integrations where 02's A = 1 tuning met no one-loop
candidate. From 02's A = 1 zero lines
([`out/decisions.txt`](one-loop-drain/measure-02b/out/decisions.txt), step 2a), S2 holds under `zero` for L-M5e-16,
L-M4e-15, C-3016, C-3233 and C-3873. Under `edge` it holds for the last four too, but not for L-M5e-16, which meets a gap
candidate at its first impact. So each of 02b's six A has 25 `zero` runs and 5 `zero` lines derived by S2 (30 in all).

**S1** is 02's, decided at each A from the `zero` task (run, or S2's).

**The spot checks** ([`out/spot_check.txt`](one-loop-drain/measure-02b/out/spot_check.txt)) were each compared in the
line and in the partial file, every value but the wall times, the peak RSS and the record's own field:

| shortcut | task run | against | integrations | candidates | result |
|---|---|---|---|---|---|
| S1 | `P-2197-A10-edge` | 02b's `P-2197-A10-zero` | 1 | 6 | bitwise equal; `gap_prescription` 'edge' against 'zero' |
| S1 | `P-2197-A0.05-edge` | 02b's `P-2197-A0.05-zero` | 1 | 134 | bitwise equal |
| S2 | `C-3016-A10-zero` | 02's `C-3016-A1-zero` | 1 | 330 | bitwise equal; `A` 10.0 against 1.0 |
| S2 | `C-3233-A0.05-zero` | 02's `C-3233-A1-zero` | 3 | 903 | bitwise equal; `A` 0.05 against 1.0 |

S1 applied to P-2197, the first history of P, R and B in 02's table order, at both A = 10 and A = 0.05, so no
fallback A was needed.

**The tool check (ruling 8; [`out/tool_check.txt`](one-loop-drain/measure-02b/out/tool_check.txt)).**
`tools/history_and_bbn.py 1.185 4e-31 --drain-law one_loop --alpha 10.0 --gap-prescription zero` was run in its own
subprocess. Its stdout equals the driver task's stdout 13 of 13 lines once `wall=` and `PRyM_time=` are masked, and its
`history`, `drain` and `bbn` lines carry the driver's values, 22 of 22. (The script is 02's `tool_check.py`, imported;
its closing line still says "on both tasks", from 02, where there is one.) **The driver agrees with the tool.**

### 2b.4 The batches and the projection

Ruling 1's order ([`out/decisions.txt`](one-loop-drain/measure-02b/out/decisions.txt),
[`out/derive.txt`](one-loop-drain/measure-02b/out/derive.txt),
[`out/cost.txt`](one-loop-drain/measure-02b/out/cost.txt)). Every batch was launched with `nohup`, checked once that it
had started, and waited on by a command that ended with it. Times are UTC, 2026-11-04.

| batch | tasks | workers | start | wall | loads at start | loads at end | largest task RSS |
|---|---|---|---|---|---|---|---|
| repro: the reproduction check | 2 | 2 | 16:04:50 | 22.1 s | 6.58 8.73 12.98 | 6.73 8.61 12.81 | 248 MiB |
| step2a: zero at A = 10, 5, 2, 1.5, 0.5 less S2's | 125 | 6 | 16:05:38 | 325.2 s | 6.83 8.44 12.62 | 12.19 12.07 13.31 | 260 MiB |
| step2b: edge at those A not covered by S1 | 16 | 6 | 16:11:25 | 72.3 s | 11.17 11.85 13.20 | 27.16 15.57 14.44 | 257 MiB |
| step3a: zero at A = 0.05, P's two | 2 | 2 | 16:12:49 | 26.1 s | 33.05 17.35 15.09 | 51.69 23.38 17.35 | 246 MiB |
| step3b: zero at A = 0.05, W R B L less S2's | 23 | 6 | 16:13:36 | 996.5 s | 44.77 23.65 17.59 | 5.75 13.67 18.49 | 1354 MiB |
| step3c: edge at A = 0.05 not covered by S1 | 5 | 6 | 16:30:28 | 942.3 s | 5.22 13.16 18.23 | 9.50 19.63 26.96 | 1358 MiB |
| spot: spot runs and the tool check | 5 | 5 | 16:46:35 | 86.3 s | 9.25 18.78 26.44 | 14.34 20.28 26.41 | 280 MiB |

From the first launch to the last end took **43 min 12 s**. The 171 sample runs took 8442 s of task wall time. The
loads above 20 at step 2b's end and step 3a came from other work on the shared machine. At most 7 of this prompt's
computing processes ran at once (a batch parent and six children), beside the idle shells that waited on them.

**Ruling 5's projection**, after P's two at A = 0.05:
- walls 26.1 s and 26.1 s: both P tunings stay one-loop at A = 0.05 and succeed;
- the largest peak RSS seen in 02b so far was 0.254 GiB, so the workers are min(6, ⌊8 GiB / 0.254 GiB⌋) = 6;
- 23 `zero` tasks (W, R, B, L less S2's) plus 26 `edge` tasks (every history but the four that S2 spares under `edge`)
  make 49;
- 49 × 26.1 s / 6 = 213 s = **0.06 h**, against the 3 h limit and the floor of three workers.

So it did not stop. In the event 23 `zero` and 5 `edge` tasks ran at A = 0.05, in 0.54 h of batch wall time. The
projection was low by a factor of about nine, because the ladder's 5 × 10⁻²⁰, 8 × 10⁻¹⁸ and 10⁻¹⁶ M_P took 711–942 s
each against P's 26 s. It stayed under the limit. The later batches' workers by ruling 5 stay 6: the largest peak RSS,
1358 MiB = 1.326 GiB, gives ⌊8 / 1.326⌋ = 6.

### 2b.5 The sample measured (§2.1 of the prompt)

[`out/sample.txt`](one-loop-drain/measure-02b/out/sample.txt) holds 540 task lines, 02's 180 and 02b's 360. Of 02b's
360:
- 171 were run: 167 succeeded and 4 failed, with none timed out, crashed or in error;
- 189 were derived: 159 by S1 and 30 by S2.

Every tuning at A = 10, 5, 2, 1.5 and 0.5 succeeded, under both prescriptions. The four failures are all at A = 0.05:

| task | reason | met in its one integration |
|---|---|---|
| `R-632-A0.05-zero` | the effective minimum was not found after the seed integration (V_eff' of one sign on the bracket, values 3.27795–3.27798 × 10⁻¹²¹) | 37 one-loop, 98 gap, 1488 classical |
| `R-632-A0.05-edge` | the same kind (values 3.27795 × 10⁻¹²¹); 7 unresolvable assessments | 37 one-loop, 58 gap, 4193 classical |
| `L-M5e-20-A0.05-zero` | step budget (2 × 10⁶ accepted steps) at N = 46.436, 14 020 reflections | 8 one-loop, 2 gap, 14 010 classical |
| `L-M5e-20-A0.05-edge` | step budget at N = 46.066, 14 116 reflections | 8 one-loop, 2 gap, 14 106 classical |

Each is of 02's two kinds, at the two histories whose A = 0.1 tunings failed in 02 (§2.4). None is the wall search or
the inside-wall guard, and none points at `drain_regime` or `drain_alpha_effective`. So none is a stop (ruling 9), and
they extend `[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]` (board §3). **At laboratory M every P and W
history succeeds at A = 0.05**, one-loop only, where all but P-2188 under `edge` fail at A = 0.01.

### 2b.6 The regime shares and the gap's reach across nine A (§2.2 of the prompt)

[`out/regimes.txt`](one-loop-drain/measure-02b/out/regimes.txt) holds 02's tables over the nine A: the candidates and K
shares by regime, the drains, ℓ_min, the least ln R at a drain, the gap's reach and the R·S(L) bound, per history, A
and prescription. The regimes met by the `zero` tuning over all its integrations (G: the gap; C: R ≤ 1; x: failed):

| group | A = 10, 5, 2, 1.5 | A = 1 | A = 0.5 | A = 0.1 | A = 0.05 | A = 0.01 |
|---|---|---|---|---|---|---|
| P, W (β 1.06–1.1875) | one-loop only, 3–12 drains | one-loop | one-loop, 27–32 | one-loop, 90–96 | one-loop, 125–138 | G C x |
| R-632 (β 1.0) | one-loop only, 4–9 | one-loop, 13 | one-loop 20, then 1 gap (the last candidate, ln R = 6.30) | G C x | G C x | G C x |
| R-1193, R-865, R-611; B | one-loop only | one-loop | one-loop | one-loop | one-loop, 38–720 | one-loop |
| L-M5e-20 (ln R_first 40.2) | one-loop only, 4–11 | one-loop | one-loop, 17 | G C x | G C x | G C |
| L-M8e-18 (19.9) | 3 one-loop, then 1 gap; parks | G C | G C | G C | G C | G C |
| L-M1e-16 (9.76) | 1 one-loop, 9 gap, 54 classical | G C | G C | G C | G C | G C |
| L-M5e-16 (3.32) | gap at the first impact, then R ≤ 1 (S2) | same | same | same | same | same |
| L-M4e-15, C | R ≤ 1 from the first impact; nothing drains but the parking (S2) | same | same | same | same | same |

- **K shares** where the cascade stays one-loop (P, W, R-1193, R-865, R-611, B; the zero task): the one-loop share is
  0.999999–1.000000 at A = 10 for P and W (0.999826–1.000000 for R and B), and 0.997568–0.999627 for P and W at
  A = 0.05 (0.990423–0.999466 for R and B). The parking leftover takes the rest: 5.8 × 10⁻⁹ to 9.6 × 10⁻⁷ of K at
  A = 10 for P and W, 3.7 × 10⁻⁴ to 2.4 × 10⁻³ at A = 0.05, and up to 9.6 × 10⁻³ (R and B) at A = 0.05.
- **ℓ_min** for P and W is 20–27.2 at A = 10, 13.2–22 at A = 5, 6.3–14.1 at A = 2, 6.2–15.7 at A = 1.5, 4.3–5.7 at
  A = 0.5 and 0.31–1.27 at A = 0.05. R-632's is 1.12–3.22 at A ≥ 0.5.
- **The gap's reach on the ladder** at 02b's A: L-M8e-18 enters the gap at candidate 3 (ln R = 4.02) at A ≥ 1.5, and
  only reaches R ≤ 1 at A ≤ 0.5 (candidate 76 at A = 0.5, 7 at A = 0.05). L-M1e-16 enters at candidate 1 and reaches
  R ≤ 1 at candidate 10 (A ≥ 1.5), 8 (A = 0.5) and 4 (A = 0.05). L-M5e-20 never enters the gap at A ≥ 0.5.
- **U9:** no one-loop candidate has w_gate < 1 at 02b's A, 0 of 6358 over the 167 successful runs' accepted integrations;
  no gap candidate either, 0 of 158. **P19:** no candidate is unassessed, 0 of 50 935.
- **Unresolvable assessments and declined reflections** at 02b's A: `R-632-A0.05-edge` (failed) had 7 unresolvable;
  `L-M8e-18-A0.05-edge` (succeeded) declined 1 unresolved reflection. None elsewhere but 3873's 27 declined, as in 02.
- **The R·S(L) bound** at 02b's A: no classical candidate in any successful P, W, R or B run. On the ladder, where a
  one-loop or gap candidate drains and classical candidates follow, its share of K is 1.5 × 10⁻⁸ (L-M1e-16, A = 1.5,
  `edge`) to 1.9 × 10⁻⁴ (L-M5e-16, A = 0.05, `edge`), and as an r_χ today at most 1.2 × 10⁻⁷, against an r_χ today of
  6.7 × 10⁻⁴ to 0.0198 in the same runs. At A = 0.05 the long classical tails of L-M8e-18 and L-M1e-16 lie
  mostly outside S(L)'s range (9765 of 9777 and 9599 of 9618 candidates, L down to 8 × 10⁻¹⁰). Where only the parking
  drains (S2's histories) the values are 02's at every A.

### 2b.7 The first impact and the cap (§2.3 of the prompt)

[`out/first.txt`](one-loop-drain/measure-02b/out/first.txt) gives, per history, A and prescription, from the accepted
integration (ruling 6): the first candidate's regime and N; its α_eff, the fraction of the first impact's kinetic
energy drained; its ΔK as a share of K(N_final); the drains in all, after the first impact before parking, and after
parking; the parking N and the parking leftover's share; the cap's count; r_χ and ΔN_eff at 1 MeV and today; Y_p and
D/H. The parking leftover is the `Parking` record's ΔK, a ledger entry of its own and **not among the injections**
(which are parallel to the candidates). So it is counted apart, and the drains before parking are injections only.
- **The cap** is counted by ℓ (A·c₁/ℓ ≥ α_cap) on 02b's runs, and by α_eff = 0.99 exactly on 02's partials. On the 167
  successful 02b runs, 43 with a capped one-loop drain, **the two counts agree in every run**. The line's
  `first_impact` (from the payload) agrees with the partial file's in all 167.
- **The first impact is one-loop** wherever the cascade starts one-loop (every P, W, R, B history and L-M5e-20 to
  L-M1e-16); its N is fixed by the history (17.63 for 2197) and does not move with A.

**A = 5 and 10 beside A = 1 and 2** (the zero task; laboratory M and block B):

| | A = 1 | A = 2 | A = 5 | A = 10 |
|---|---|---|---|---|
| α_eff at the first impact, P and W | 0.084–0.118 | 0.167–0.237 | 0.418–0.591 | 0.837–0.99 (P-2188 capped) |
| its share of K, P and W | 0.254–0.923 | 0.333–0.960 | 0.432–0.996 | 0.902–0.990 |
| drains after it before parking, P and W | 8–18 | 2–11 | 5–10 | 2–5 |
| parking N, P and W | 33.06–35.79 | 29.61–35.03 | 23.79–29.94 | 18.77–24.68 |
| capped one-loop drains, P and W (per history) | 0 | 0 | 0 (P-2188: 1) | 1–3 |
| α_eff at the first impact, R-1193, R-865, R-611 | 0.068–0.084 | 0.137–0.168 | 0.342–0.420 | 0.684–0.840 |
| α_eff at the first impact, B | 0.060–0.086 | 0.120–0.172 | 0.300–0.429 | 0.600–0.858 |
| R-632 (β 1.0) | 0.303; 12 more; 2 capped | 0.606; 6 more; 3 capped | 0.99; 3 more; 4 capped | 0.99; 3 more; 4 capped |

At **A = 10** the first impact drains 84–99% of the condensate's kinetic energy at laboratory M in the transition
window. That impact alone holds 90–99% of K(N_final) there. The field then parks after 2–5 more drains, at N = 18.8–24.7,
against 33.1–35.8 at A = 1. At **A = 5** it drains 42–59%, holds 43–99.6% of K, and the field parks after 5–10 more,
at N = 23.8–29.9. The one-loop drains continue after the first impact; none of these histories enters the gap. For
2197, from A = 1 to 2, 5 and 10: ΔN_eff at 1 MeV 0.0538, 0.108, 0.268, 0.5465; ΔN_eff today 0.0612, 0.119, 0.277, 0.564;
r_χ today 0.00822, 0.0160, 0.0372, 0.0757; Y_p 0.25688, 0.25094, 0.25029, 0.25410; D/H 2.5227, 2.5241, 2.5483, 2.6437
(× 10⁻⁵). Across P and W at A = 10, ΔN_eff today is 0.029 (P-2188), 0.037 (W-1631), 0.081 (W-2187) and 0.29–0.645 for the
other nine; Y_p is 0.24669–0.25509 and D/H 2.4664–2.6702.

Where the cap binds at the first impact the result stops moving with A. R-632 at A = 5 and 10 is the same history
(ΔN_eff today 0.3399), and L-M5e-20, L-M8e-18 and L-M1e-16 at A ≥ 5 give ΔN_eff today 0.1475, 0.1475 and 0.1460 (`zero`).
At β = 2 the ladder and R-865, B-2496 and B-2498 reach 0.134–0.148 at A = 5 and 10, close to the ladder's capped value.
At β = 10 (R-611, B-2772, B-2774) ΔN_eff today peaks at A = 5 (0.0060–0.0061) and is 0.0053–0.0055 at A = 10.

**Gap drains capped under `edge`:** A·c₁/ℓ_edge ≥ 0.99 for A ≥ 1.4667, so at A = 1.5–10 every `edge` gap drain is
capped at 0.99 (L-M8e-18, L-M1e-16, L-M5e-16; one each).

### 2b.8 The spread between the prescriptions (§2.4 of the prompt)

[`out/spread.txt`](one-loop-drain/measure-02b/out/spread.txt). At 02b's A, `edge` was run for 21 (history, A) pairs and
S1 covered the other 159. Both succeeded in 19, all on the ladder or R-632 at A = 0.5:

| | largest edge − zero | where |
|---|---|---|
| Y_p | +2.184 × 10⁻³ (+0.89%) | L-M5e-16, A = 1.5–10 |
| D/H | +0.0444 × 10⁻⁵ (+1.8%) | L-M5e-16, A = 1.5–10 |
| ΔN_eff at 1 MeV | +0.1415 | L-M5e-16, A = 1.5–10 |
| ΔN_eff today | +0.1460 | L-M5e-16, A = 1.5–10 (zero: 6.7 × 10⁻⁶) |
| r_χ today | +0.0196 | L-M5e-16, A = 1.5–10 |

- R-632 at A = 0.5 enters the gap only at its last candidate (a K share of 5 × 10⁻⁷ under `edge`): the two are equal in
  every observable to the digits printed.
- L-M8e-18 at A = 1.5–10: equal, since the one gap candidate comes after the field's last drain (gap share
  9.6 × 10⁻⁷). At A = 0.5, ΔN_eff today rises by 4.0 × 10⁻⁵; at A = 0.05 by 1.7 × 10⁻³.
- L-M1e-16: ΔN_eff today rises by 1.4 × 10⁻³ at A = 1.5–10, 3.7 × 10⁻³ at A = 0.5 and 0.0108 at A = 0.05.
- L-M5e-16: 0.146 at A ≥ 1.5, 0.0498 at A = 0.5 and 0.0050 at A = 0.05.
- At laboratory M no pair differs: P and W meet no gap candidate at any of 02b's A, so S1 covers every one.

### 2b.9 Against the fixed law (§2.5 of the prompt)

[`out/fixed.txt`](one-loop-drain/measure-02b/out/fixed.txt) sets each one-loop value at the nine A beside every stored
α at the same (M, β), from 02's `fixed_law.json` (261 references, read as it stands). The store's values were
measured on the 2026.8.0 tree; they are not a bound. For example:
- **2197**: the fixed law's largest stored α, 0.15, gives ΔN_eff today 0.104. The one-loop law passes it between A = 1.5
  (0.0917) and 2 (0.119), and reaches 0.277 at A = 5 and 0.564 at A = 10, outside the stored α's range. At A = 0.05 it
  gives 0.00861, between α = 0.003 (0.00775) and 0.005 (0.00906), with Y_p 0.25838 and D/H 2.8698 against the fixed
  law's 0.2584–0.2587 and 2.87.
- **R-865** (β 2.0): 0.0024 at A = 0.05, 0.0243 at 0.5, 0.082 at 2, 0.140 at 5 and 0.146 at 10.

### 2b.10 The cost (§2.6 of the prompt)

[`out/cost.txt`](one-loop-drain/measure-02b/out/cost.txt). Every one of the 171 runs took **one integration**. Per
tuning:

| group | A = 10, 5, 2, 1.5 | A = 0.5 | A = 0.05 |
|---|---|---|---|
| P, W | 12–14 s | 14–16 s | 24–26 s |
| R | 12–18 s | 14–26 s | 18–131 s; R-632 edge 354 s (both R-632 fail) |
| B | 14–20 s | 14–30 s | 18–90 s |
| L | 12–22 s; edge 12–48 s | 14–32 s; edge 14–24 s | 711–912 s; edge 22–942 s |
| C | (S2) | (S2) | (S2) |

- **Peak RSS** is about 250 MiB for a short task, up to 526 MiB for R-632 at A = 0.05, and 1135–1358 MiB for L-M5e-20,
  L-M8e-18 and L-M1e-16 at A = 0.05, whose classical tails run near or to the step budget.
- **The batches** are in §2b.4: 43 min 12 s from first launch to last end, at most six workers.

### 2b.11 The handover for U14 (§2.7 of the prompt)

This is a measurement and the options it bears on, not a choice. The matrices are in
[`out/u14.txt`](one-loop-drain/measure-02b/out/u14.txt), its last part.

**How each observable moves with A over the nine values, laboratory M first (Fig 6).**
- **ΔN_eff today** (`zero`), A = 10, 5, 2, 1.5, 1, 0.5, 0.1, 0.05, 0.01:
  - 2197: 0.564, 0.277, 0.119, 0.0917, 0.0612, 0.0333, 0.0114, 0.0086, fails;
  - P and W: 0.029–0.645, 0.024–0.335, 0.0098–0.131, 0.0076–0.101, 0.0057–0.068, 0.0039–0.038, 0.0026–0.013,
    0.0025–0.0094, every one fails (but P-2188 under `edge`, 0.0024);
  - R-1193 (β 1.15) and B at β = 1.2: 0.47–0.62 at A = 10, down to 0.0026–0.0031 at A = 0.05 and 0.00065–0.00087 at 0.01;
  - β = 2 (R-865, B-2496, B-2498): 0.1445–0.146 at A = 10, 0.134–0.141 at 5, 0.072–0.084 at 2, 0.038–0.047 at 1,
    0.0020–0.0025 at 0.05;
  - β = 10 (R-611, B-2772, B-2774): 0.0051–0.0061 at A ≥ 2, down to 0.00016–0.00019 at 0.01.
- **ΔN_eff at 1 MeV** is close to proportional to A at laboratory M in the transition window: 2197's is 0.5465, 0.2683,
  0.1077, 0.0809, 0.0538, 0.0269, 0.00536, 0.00268, that is 0.0535–0.0547 × A. ΔN_eff today falls more slowly than A
  below A = 0.5 (2197: 0.0114 at 0.1, 0.0086 at 0.05), because more of the drain comes after 1 MeV.
- **Y_p and D/H** at laboratory M in the transition window do not move monotonically with A. For 2197, Y_p is
  0.25410, 0.25029, 0.25094, 0.25286, 0.25688, 0.25857, 0.25862, 0.25838 from A = 10 down to 0.05; over P and W it spans
  0.24669–0.27423 across the eight A that build. D/H is 2.4664–2.9341 × 10⁻⁵. Beside the criteria of
  `bbn_constraints_explainer.tex` (Y_p ≤ 0.251, D/H ≤ 2.587 × 10⁻⁵), as context only: at β ≥ 2 (R-865, R-611, B and
  the ladder) Y_p ≤ 0.2489 and D/H ≤ 2.5072 × 10⁻⁵ at every A that builds.
- **The parking N** falls as A rises: P and W 38.1–39.1 at A = 0.05, 33.1–35.8 at A = 1 and 18.8–24.7 at A = 10.
- **The cap** binds at the first impact at laboratory M only for P-2188 at A = 10 and R-632 at A ≥ 5. Where it binds
  there, the result stops moving with A: R-632 at A = 5 and 10, and the ladder from A = 5 (5 × 10⁻²⁰ M_P), A = 2
  (8 × 10⁻¹⁸) and A = 1 (10⁻¹⁶) upward (§2b.7).

**Which histories change regime between A values** (the zero tuning, any integration):
- P and W: one-loop only from A = 10 down to 0.05; gap and R ≤ 1 at A = 0.01, where every tuning fails but P-2188's under
  `edge`;
- R-632 (β 1.0): one-loop at A ≥ 1; one gap candidate, the last, at 0.5; gap and R ≤ 1 at 0.1, 0.05 and 0.01, failing;
- L-M5e-20: one-loop at A ≥ 0.5; gap and R ≤ 1 at 0.1 and 0.05 (failing) and at 0.01 (succeeding);
- L-M8e-18: gap without R ≤ 1 at A ≥ 1.5; gap and R ≤ 1 at A ≤ 1;
- R-1193, R-865, R-611 and block B: one-loop at all nine A;
- L-M1e-16, L-M5e-16, L-M4e-15 and C: the same regimes at all nine A.

**Which (history, A) cannot be built on this code** (zero or edge failing): P and W at A = 0.01 (but P-2188 under
`edge`); R-632 at A = 0.1, 0.05 and 0.01; L-M5e-20 at A = 0.1 and 0.05. Everything else builds, at every A from 0.05 to
10, under both prescriptions.

**What it bears on.**
- At laboratory M the lowest A that builds in the transition window on this code is 0.05; A = 0.01 does not.
- A ≥ 5 puts most of the condensate's kinetic energy into the first impact. At A = 10 the cap acts there for P-2188 and
  R-632, and ΔN_eff today reaches 0.29–0.645 for nine of the twelve P and W histories.
- β = 1.0 at laboratory M builds only at A ≥ 0.5.

### 2b.12 The Wide gate

The gate holds, item by item (prompt §2):
1. **The sample measured.** All 360 of 02b's tasks, by group, A and prescription, with S1 and S2 marked, beside 02's
   180; the four failures with their reasons, and no timeouts (§2b.5). The reproduction check is bitwise (§2b.2).
2. **The regime shares and the gap's reach across the nine A** (§2b.6), with U9's count (0 of 6358) and P19's (0 of
   50 935) at 02b's A.
3. **The first impact and the cap** per history and A, with A = 5 and 10 beside A = 1 and 2 (§2b.7).
4. **The spread between the prescriptions** at 02b's A, for the 19 pairs where both succeeded (§2b.8).
5. **Against the fixed law** at all nine A (§2b.9).
6. **The cost**: wall time and integrations by group and A, the batches, the workers and the loads (§2b.4, §2b.10).
7. **The handover for U14** (§2b.11).

## 2c. Prompt 02c — the measured law (2026-11-05)

**Tree:** branch `one-loop-drain` at `1647f7a`, whose computing code is 02b's review's (`1edb56c`); prompt 02c's commit
(`18add05`) changes `ComputeTargets/ScalarHistory.py`, `CosmologyConcepts/Models/AbstractModel.py`,
`CosmologyConcepts/Models/ExponentialModel.py` and `tools/history_and_bbn.py`, and §1, §2 and §2b above are unchanged.
**Scripts:** [`one-loop-drain/law-02c/`](one-loop-drain/law-02c/), each with a docstring: `make_inputs.py`, `batch.py`,
`compare_maps.py`, `l_form_probe.py`, `compare_runs.py`. **Committed outputs:**
[`one-loop-drain/law-02c/out/`](one-loop-drain/law-02c/out/). **Raw output** (not committed): `var/one-loop-drain/02c/`:
- `inputs/`: the ladder's copy at A = 0.2 and 2197's at A = 1;
- `maps/` and `batches/maps/`: the batch on the final tree; `maps-run1/` and `batches/maps-run1/`: the batch before L moved
  into the model class (§2c.5);
- `tool-P-2197-A0.05.txt` (final tree) and `tool-P-2197-A0.05-run1.txt`;
- `probe/`: `l_form_probe.py`'s outputs on both trees.

Orchestrator note 02c's rulings governed, with two rulings of the user made during the work, both 2026-11-05: on the gap
below L = e^{−1/q}, "I think probably we set `f = 0` there"; and "I suggest we move `L` into the model class, with no
default. Then when a new potential is implemented, breakage is loud, and the implementer has to think about what they
are changing." A third, "Decision 2: yes", accepts the header differences of §2c.3 as ruling 4's exception. No store was
written and no Ray was started.

### 2c.1 The Gate and the suites

The Gate held at `1647f7a`: the tree was clean; the diff from `1edb56c` outside `prompts/` and `.documents/` was empty,
and inside `.documents/` touched only `OPEN_ISSUES.md`; U15 is ruled (b); `runregistrykit list` showed 23 runs, all
finished; the four inputs exist under `var/one-loop-drain/02b/tasks/` with tuning records.

The suites, serially (2026-11-05, local time = UTC):

| suite | before (`1647f7a`, from 02:04) | after the law (from 02:22) | final tree (from 10:25) |
|---|---|---|---|
| CosmologyModels | 26 OK, 64.1 s | 26 OK, 65.7 s | 26 OK, 72.6 s |
| ComputeTargets `test_[a-f]` | 163 OK, 139.5 s | 162 OK, 138.1 s | 162 OK, 143.4 s |
| ComputeTargets `test_[g-z]` | 403 OK, 468.0 s | 426 OK, 468.8 s | 432 OK, 489.5 s |
| ObjectFactories | 130 OK, 81.1 s | 130 OK, 85.9 s | 130 OK, 82.8 s |
| tools | 156 OK, 161.7 s | 156 OK, 167.1 s | 156 OK, 170.2 s |
| RunDrivers | 68 OK, 23.1 s | 68 OK, 24.5 s | 68 OK, 24.8 s |
| load at the first start | 20.31 25.52 29.78 | 10.65 7.94 13.18 | 10.80 11.78 11.84 |

The loads at each suite's start and end are in the log (§4 there). Deleted, from `test_drain_law` (the `test_[a-f]`
half): `test_2_the_gap_prescriptions_read_at_call_time` and `test_2_an_unknown_prescription_is_refused_at_the_gap`.
Added: `test_drain_law.TestTheOneLoopLawOn2197.test_4_the_values_under_the_measured_law`, and
`test_measured_drain_law` (29 tests, the `test_[g-z]` half).

### 2c.2 The fixed law is unchanged (prompt §2.1)

- `test_golden_history` (3 tests), `test_golden_reflecting_history` (2), `test_tuning` (40) and
  `ObjectFactories/tests/test_store_fingerprint_golden.py` (5) are unchanged files, discovered in the suites above, and
  pass on both trees.
- The fixed law's `DrainRecord` is `("fixed", None, "fixed", count, None, declined)`: `model` stands where
  `gap_prescription` stood, and its value under the fixed law is `DRAIN_MODEL_FIXED` (`test_drain_law`
  `TestTheFixedLawIsUnchanged`, `TestTheUnresolvableCount`). The payload's keys are unchanged.
- Under the fixed law a model that does not define `_drain_L` runs as before: bit for bit ExponentialModel's on the
  floor harness at α = 0.1 (`test_measured_drain_law.TestTheModelHook.test_h_the_same_model_runs_under_the_fixed_law`).

### 2c.3 The law equals its reference (prompt §2.2)

`test_measured_drain_law.TestTheReference` compares `drain_fraction` and `drain_alpha_effective` with
`fraction_adopted`, copied verbatim from `prompts/one-loop-drain/source/crossover/measured_law_map.py` at `1b16c62`
with its table loading (the table's 21 rows copied as text and read by the same `np.loadtxt`). The grid is 13 ℓ (from
10⁻⁶ to 50, with 1 and its two neighbours) × 27 L (10⁻⁶ to 10⁶, with e^{−1/q}, 0.3, 133.9, 134 and 134.1) × 15 ln R
(−100 to 138.07, with 0, ±10⁻¹², r₀ and its upper neighbour) × 3 M (4 × 10⁻³¹, 5 × 10⁻²⁰, 10⁻¹² M_P), over the one-loop,
gap and classical regimes: 18 225 values of f, each with α_eff at three (A, w_gate). L is M/φ_t, by the reference's
expression, handed to the production law as a number.
- **Bitwise everywhere** but in two places, where it is bitwise against the reference with the departure substituted:
  - the classical regime above L = 134, where S is S(134)(134/L)² as README §8.10 rules (the reference's `np.interp`
    holds S(134));
  - the gap where 1 + q ln L < 0 and ln R > r₀, where f is 0.0 by the user's ruling (the reference is negative).
- No other value departs (the one-loop count of departures is 0 in the test).
- S at the 21 nodes equals the table to 10⁻¹⁴ relative (one rounding of ln S), and equals the reference's `S_of`
  bitwise; between nodes it equals an independent log-linear evaluation to 10⁻¹³ relative.
- L, now `ExponentialModel._drain_L`: at x = −30, 0 and 30 it equals log1p(e^x) to 10⁻¹⁵; at x = 800 it returns 800
  where log1p(e^x) overflows; at the maps' x within 2 ulp. At n = 1 it equals M/φ_t to 10⁻¹⁰ relative (three M, five
  x); at n = 2 it is ln(1 + E/V₀), and (M/φ_t)² equals it to 10⁻¹⁰.

### 2c.4 Crossover's adopted-law maps reproduced (prompt §2.3)

`batch.py` ran `cascade_map.py` unpatched on the four maps, the control, and 2197 at A = 1 both ways, at most five at
once; `compare_maps.py` compared them ([`out/compare_maps.txt`](one-loop-drain/law-02c/out/compare_maps.txt)). Inputs:
02b's task records unchanged for P-2197, W-2187 (A = 0.05) and R-632 (A = 0.5); for the ladder, 02b's
`L-M5e-20-A0.5-zero.result.json` with `--alpha` set to "0.2" ([`out/make_inputs.txt`](one-loop-drain/law-02c/out/make_inputs.txt)).

| map | rows | rows equal | `parking_N` | `N_final` | candidates | wall |
|---|---|---|---|---|---|---|
| P-2197-A0.05 | 203 | all | equal | 46.00064338832537 against …325116 | equal | 19.8 s |
| W-2187-A0.05 | 199 | all | 39.02200520613764 against …137614 | 45.46314335865381 against …5364 | equal | 19.5 s |
| R-632-A0.5 | 32 | all | equal | equal | equal | 5.7 s |
| L-M5e-20-A0.2 | 94 | all | 30.70914570727863 against …278893 | 50.07564351531626 against …1618 | equal | 14.3 s |

- **The control**, `measured_law_map.py --law=adopted` on P-2197 at A = 0.05 with the adapted patch, is byte-identical to
  crossover's file (20 639 bytes).
- **No row differs by the gap ruling:** no gap candidate of the four maps has 1 + q ln L < 0; the least L at a gap
  candidate is 2.578 (R-632).
- **The headers' last digits.** `parking_N` and `N_final` are printed at full precision; they differ at 10⁻¹⁴–10⁻¹⁵
  relative in three maps. [`out/l_form_probe.txt`](one-loop-drain/law-02c/out/l_form_probe.txt) runs each history again
  with only the production L replaced by M/φ_t: then every header and every row equals crossover's exactly. So the cause
  is U15 (b)'s form of L, carried by the integration. Over the candidates, |L/(M/φ_t) − 1| ≤ 2.8 × 10⁻¹⁴, 2.6 × 10⁻¹⁴,
  3.3 × 10⁻¹⁴ and 1.3 × 10⁻¹⁴ (P, W, R, L), and the production α_eff differs from α_eff with M/φ_t on the same inputs by
  at most 2.7 × 10⁻¹⁵, 4.9 × 10⁻¹⁵, 6.3 × 10⁻¹⁵ and 2.5 × 10⁻¹⁴ relative, the first at candidates 11, 6, 1 and 7 at
  3–4 × 10⁻¹⁶. This met ruling 4 (ii) (≤ 10⁻¹⁰), but not (i) as worded, which speaks of a row; the work stopped, and the
  user ruled it the exception: "Decision 2: yes".
- **2197 at A = 1** (the re-pinned `TestTheOneLoopLawOn2197`): unpatched production equals the patched reference
  exactly, rows and header: five candidates, all one-loop, α_eff 8.2276 × 10⁻², 9.1886 × 10⁻², 0.11431, 0.12001 and
  0.16459, parking at N = 29.035264723467392.
- **Classical candidates now drain.** The ladder at A = 0.2 meets 83, each drained A·R·S(L)·w_gate, from 9.3 × 10⁻¹³ to
  6.2 × 10⁻⁶; the map took 14.3 s. The other three maps have none.
- **The batch** took 19.9 s, at loads 43.06 42.42 28.28 at its start and 37.69 41.24 28.19 at its end.

### 2c.5 The tool route, and the re-verification after L moved (prompt §2.4)

`tools/history_and_bbn.py 1.185 4e-31 --drain-law one_loop --alpha 0.05 --fixed-normalization -276.63072859401797
8.645101631245453e-16` ([`out/tool_P-2197-A0.05.txt`](one-loop-drain/law-02c/out/tool_P-2197-A0.05.txt)), 27–28 s:
- its history line names `drain_law=one_loop A=0.05 model=one-loop-measured-v1`;
- its `drain` line gives 203 candidates, 192 one-loop and 11 gap, and ℓ_min = 0.0977756, as the P-2197 map (192, 11,
  0.0978).

After the user's ruling moved L into the model class, the five suites (§2c.1), the batch and the tool run were repeated
([`out/compare_runs.txt`](one-loop-drain/law-02c/out/compare_runs.txt)): every map, the control and both 2197 A = 1 runs
are byte-identical to the first batch's, and the tool's lines are identical but for the wall times. The batch took
19.8 s (loads 9.06 10.16 11.55 at its start, 9.38 10.18 11.53 at its end); the tool run 28 s. `l_form_probe.py` on the
final tree gives output identical to the first.

### 2c.6 The Measured gate

The gate holds, item by item (prompt §2):
1. **The fixed law is unchanged** (§2c.2): the goldens pass bitwise, unchanged, and the fixed-law payload differs only in
   `DrainRecord.model`.
2. **The law equals its reference** (§2c.3): bitwise on the grid, but at README §8.10's continuation above L = 134 and the
   user's gap ruling, each named.
3. **Crossover's maps are reproduced** (§2c.4): every candidate row of the four maps equal, the candidate counts equal, the
   control byte-identical; the headers' parking and final N differ in their last digits by the U15 (b) form of L, which the
   user ruled ruling 4's exception.
4. **The tool route** (§2c.5) names the model version, and its `drain` line agrees with the P-2197 map on the counts by
   regime and ℓ_min.

## 2d. Prompt 02d — measure the measured law (2026-11-05)

**Tree:** branch `one-loop-drain` at `f4da906`; its computing code is `18add05`'s (the Gate's diff is empty), the
measured law that §2c verified. Prompt 02d's commit (`98b1601`) adds no code outside
[`one-loop-drain/measure-02d/`](one-loop-drain/measure-02d/), and §1, §2, §2b and §2c above are unchanged. Python
3.13.16. **Scripts:** [`one-loop-drain/measure-02d/`](one-loop-drain/measure-02d/), each with a docstring:
`tasks_02d.py` (the task list), `run_02d.py` (the driver), `checks_02d.py` (the tool and determinism checks),
`plan_02d.py` (the batches' lists, the workers, the projection), `analyse_02d.py` (the gate's tables) and
`copies_02d.py` (the copies' diffs). They are self-contained: they import nothing of 02's or 02b's scripts, and every
function copied from those names its source ([`out/copies.txt`](one-loop-drain/measure-02d/out/copies.txt): 43 copied
names, 25 unchanged, 18 changed, each with its diff). **Committed outputs:**
[`one-loop-drain/measure-02d/out/`](one-loop-drain/measure-02d/out/). **Raw output** (not committed):
`var/one-loop-drain/02d/`:
- `sample.jsonl`: the 240 task lines, one per (history, A), every one run;
- `checks.jsonl`: the tool check and the determinism runs (tags `check1-`, `check2-`);
- `tasks.json`: the 240 tasks;
- `tasks/`: each task's stdout, stderr, partial file (with every candidate's `DrainCandidate` whole) and result;
- `batches/<name>/`: each batch's ids, `batch.pid`, `batch.log` and `batch.end`.

The old law's lines are `var/one-loop-drain/02/sample.jsonl` (A = 1, 0.1) and `var/one-loop-drain/02b/sample.jsonl` (the
other six A), read as they stand; the fixed law's references are `var/one-loop-drain/02/fixed_law.json`, read as it
stands. Every number below comes from a file of `measure-02d/out/`, or from the line of one of those files that it
names. Orchestrator note 02d's rulings 1–10 governed, with notes 02's and 02b's rulings where it names them. No store
was written and no Ray was started; the 2026.8.0 store was opened `mode=ro` by `tasks_02d.py` only.

**Its numbers replace §2's and §2b's as the campaign's measurement of the one-loop law** (prompt 02d §0.1). §2 and §2b
stand as the record of the old law (A·2.7/ℓ with a gap prescription).

### 2d.1 The Gate and the suites

The Gate held at `f4da906` (the note's commit, whose parent `d40bb3f` adds the prompt): the tree was clean;
`18add05`, `2747741` and `ff7aaeb` are ancestors of `HEAD`; the code-path diff from `18add05` is empty;
`runregistrykit list` showed 23 runs, all finished; 02's `sample.jsonl` has 180 lines and 02b's 360, and 02's
`fixed_law.json` exists; the store's 16 shards exist; this document had §2c and no §2d, and neither `measure-02d/` nor
`var/one-loop-drain/02d/` existed. [`out/tasks.txt`](one-loop-drain/measure-02d/out/tasks.txt): the store check holds
for all 25 serials; 30 histories, 240 tasks.

The suites, serially, before anything else (2026-11-05, local time = UTC; the `test_[g-z]` half and `tools/tests` in
the background):

| suite | count | time | load at start |
|---|---|---|---|
| CosmologyModels | 26 OK | 72.4 s | 17.70 18.14 17.15 (12:12) |
| ComputeTargets `test_[a-f]` | 162 OK | 144.2 s | 15.97 17.57 17.03 (12:13) |
| ComputeTargets `test_[g-z]` | 432 OK | 476.6 s | 13.22 17.77 17.36 (12:16) |
| ObjectFactories | 130 OK | 83.0 s | 7.62 10.52 14.03 (12:24) |
| tools | 156 OK | 171.1 s | 6.49 9.49 13.28 (12:25) |
| RunDrivers | 68 OK | 27.0 s | 11.68 10.53 12.98 (12:28) |

The suites after the batches are in the log (§4 there); their counts are the same.

### 2d.2 The driver and its checks

**The driver** (`run_02d.py`) is 02's `run_sample.py` with 02b's `first_impact` beside it, copied (note 02d ruling 1). It
keeps 02's properties: a fresh `./venv/bin/python` per task; the tool's own `main`, with
`integrate_scalar_history` wrapped before the tool wraps it; every integration reduced at once and streamed to the
partial file; SIGTERM and then SIGKILL after 30 s on the 3600 s timeout; the peak RSS from `os.wait4`; nothing outliving
the batch. It differs from 02's by the paths, the task fields (no `gap`), the record's `model` in place of its gap
prescription, the R·S(L) bound removed, the timeout's variable renamed (`ONE_LOOP_DRAIN_02D_TIMEOUT_S`, default 3600 s),
and ruling 6's additions; and by one count that no ruling names, the accepted steps after an integration's first
classical candidate (for §2d.10; the log classes it).

**Ruling 6, the law against its own record** ([`out/lawcheck.txt`](one-loop-drain/measure-02d/out/lawcheck.txt)):
- every one of the 464 integrations of 02d's 245 lines (sample and checks, failed and timed-out tunings included) has a
  `DrainRecord`, and its `model` is `one-loop-measured-v1` in all 464;
- α_eff recomputed by `drain_alpha_effective(c.regime, c.ell, c.L, c.log_R, record.A, c.w_gate)` at all 616 270
  candidates equals the record's by `repr` at every one;
- recomputed again by the analysis from the 240 tasks' partial files (459 integrations, 615 946 candidates): 0 differ;
- the cap (A·f ≥ 0.99) acts at 50 one-loop candidates in 24 accepted integrations and 6 gap candidates in 6, never at a
  classical one; on one capped run, `P-2188-A10`, the 3 capped candidates have α_eff = `DRAIN_ALPHA_CAP`·w_gate exactly.

**The tool check** ([`out/tool_check.txt`](one-loop-drain/measure-02d/out/tool_check.txt)): `tools/history_and_bbn.py
1.185 4e-31 --drain-law one_loop --alpha 10.0` against the driver's `check1-P-2197-A10`: 13 of 13 stdout lines
byte-equal once `wall=` and `PRyM_time=` are masked, and 23 of 23 values of the `history` (the model among them),
`drain` and `bbn` lines equal to the driver's JSON line.

**The determinism check** ([`out/determinism.txt`](one-loop-drain/measure-02d/out/determinism.txt)): P-2197 and
L-M1e-16 at A = 1, run in `check-1` and again in `check-2`, are bitwise equal under note 02's ruling 6 with nothing of
the record excused: 0 line differences, 0 partial differences (5 and 154 candidates, compared whole by `repr`), and
stdout equal after masking. Each is also bitwise equal to the A1 pass's untagged run of the same task (not required).

### 2d.3 The batches and the projection

From `batches/*/batch.log` ([`out/cost.txt`](one-loop-drain/measure-02d/out/cost.txt), last section); times UTC,
2026-11-05:

| batch | tasks | workers | wall | loads start → end | largest task RSS |
|---|---|---|---|---|---|
| check-1 | 4 (a tool check among them) | 2 | 48.1 s | 13.97 11.11 13.09 → 16.54 12.24 13.39 | 233 MiB |
| check-2 | 2 | 2 | 32.1 s | 41.92 18.36 15.56 → 39.90 20.92 16.64 | 240 MiB |
| A1 | 30 | 6 | 3324.0 s | 31.71 20.29 16.51 → 6.25 6.65 8.70 | 984 MiB |
| wide | 150 | 6 | 4096.2 s | 6.09 6.58 8.62 → 41.36 39.24 43.56 | 1217 MiB |
| P005 | 2 | 2 | 50.2 s | 29.75 36.50 42.35 → 28.34 35.07 41.49 | 265 MiB |
| small | 58 | 6 | 3604.0 s | 25.33 34.07 41.02 → 7.74 10.08 11.24 | 1514 MiB |

From the first launch (12:29:11) to the last end (15:37:03): 3 h 07 min 52 s. The workers were min(6, ⌊8 GiB / the
largest peak RSS so far⌋) = 6 at every batch (largest 1.189 GiB before `small`). Within each batch C-3873's tasks went
first (note 02d ruling 3); C-3873 set the wall of `A1`, `wide` and `small`.

**Ruling 4's projection** ([`out/projection.txt`](one-loop-drain/measure-02d/out/projection.txt)), after `P005`: P's
measured ratio of A = 0.05 to A = 0.5 wall is 3.121 (P-2188; 3.119 for P-2197), larger than 02b's ratio for every
history but R-632 (9.302), R-611 (3.314), B-2774 (3.237) and the ladder at 5 × 10⁻²⁰, 8 × 10⁻¹⁸ and 10⁻¹⁶ M_P (64.95,
22.73, 25.33); the five S2 histories use 02's A = 0.1 / A = 1 ratio (0.96–1.02), raised to P's. 58 tasks, a summed cost
of 28 013 s (7.78 h of CPU), 4669 s over six workers against a longest task of 3630 s: **1.30 h**, under the three-hour
limit, at six workers. The batch took 1.00 h, bounded by C-3873's two timeouts.

### 2d.4 The sample measured (§2 item 1)

[`out/sample.txt`](one-loop-drain/measure-02d/out/sample.txt): 240 lines, one per (history, A), no gap and no duplicate.
**230 succeed, 3 fail, 7 time out.**
- **P and W** (laboratory M, the transition window): all 96 succeed, each in one integration.
- **R:** R-632 (β = 1.0) **fails at A = 0.1 and 0.05**, by "the effective minimum was not found" after the seed
  integration (V_eff' of one sign on the bracket, values 3.27795–3.27799 × 10⁻¹²¹). Its integration met 44 one-loop, 88
  gap and 3269 classical candidates at A = 0.1, and 43, 52 and 800 at A = 0.05, and returned without parking. Every
  other R task succeeds.
- **B:** all 48 succeed.
- **L:** the ladder's 5 × 10⁻²⁰ M_P **fails at A = 0.05**, by the step budget (2 × 10⁶ accepted steps at N = 46.07, after
  14 115 reflections; 8 one-loop, 2 gap and 14 105 classical candidates). **It succeeds at A = 0.1**, where it failed in
  02 by the step budget: it parks at N = 47.60 (N_final 50.08) after 13 342 drains, 13 333 of them classical. Every other
  L task succeeds.
- **C:** C-3016 and C-3233 succeed at every A. **C-3873 times out at seven of the eight A** (all but A = 1), after 24–27
  tuning integrations, each of which returned; at A = 1 it succeeds after 28 integrations in 3324 s. A timeout is not a
  failure of 02's two kinds (note 02d ruling 9); its line holds the integrations it finished.

No failure is of another kind, and none points at `drain_fraction`, `drain_alpha_effective`, `drain_law_inputs`,
`_drain_L`, the wall search or the inside-wall guard: each failing integration returned (R-632) or raised the step
budget (L-M5e-20), and ruling 6's check holds in each. The tool check and the determinism check are §2d.2.

### 2d.5 The regime shares across the eight A (§2 item 2)

[`out/regimes.txt`](one-loop-drain/measure-02d/out/regimes.txt) per history and A; [`out/summary.txt`](one-loop-drain/measure-02d/out/summary.txt)
by A.
- **Laboratory M (P, W, R but R-632) and B stay one-loop at A ≥ 0.1:** every drain is one-loop, K_share_one_loop ≥ 0.990,
  and the parking leftover holds 10⁻⁹–10⁻² of K. ℓ_min at P and W falls with A: 20.0–27.3 at A = 10, 5.1–15.5 at A = 1,
  1.03–2.05 at A = 0.1.
- **At A = 0.05 ten of the twelve P and W histories enter the gap** before parking (5–22 gap candidates each, all
  drained; P-2188 and W-1631 do not), with ℓ_min 0.079–0.73. The gap's share of K is small (5.2 × 10⁻⁴ for P-2197). None
  reaches R ≤ 1.
- **R-632** enters the gap at A = 0.5 (3 gap candidates, after 29 one-loop); at A ≤ 0.1 it fails (§2d.4).
- **The ladder** (β = 2): 8 × 10⁻¹⁸ to 5 × 10⁻¹⁶ M_P enter the gap at every A, 5 × 10⁻²⁰ at A ≤ 0.5; 10⁻¹⁶ and
  5 × 10⁻¹⁶ reach R ≤ 1 at every A, 8 × 10⁻¹⁸ at A ≤ 0.5 and 5 × 10⁻²⁰ at A = 0.1. 5 × 10⁻¹⁶ meets a gap candidate at its
  first impact. 4 × 10⁻¹⁵ is classical from its first impact, and its ρ_χ is the parking leftover's (≥ 0.968 of K).
- **C** is classical throughout, every candidate drained, by α_eff = A·R·S(L)·w_gate of 3 × 10⁻⁷ (C-3016, A = 10) down to
  2 × 10⁻⁵⁷ (C-3233, A = 0.05) at the first impact.
- **Where the cascade enters the gap and reaches R ≤ 1:** the first gap and classical candidates' N and ln R, per
  history and A, are in `regimes.txt` (e.g. L-M1e-16 at A = 1: gap at N = 20.730, ln R = 4.857; R ≤ 1 at N = 21.674,
  ln R = −0.150).
- **The law's edges**, over the accepted integrations: **the clamp (1 + q ln L ≤ 0) is never met** (the least gap L is
  2.578); a gap candidate at ln R ≤ r₀ 24 times (it drains nothing); the continuation (classical L > 134) never;
  **S held (classical L < 0.3) at 37 922 classical candidates** (C and the ladder's tails; classical L reaches
  1.0 × 10⁻¹¹); the underflow (classical α_eff = 0 with w_gate > 0) never. Over every integration, failed and timed out
  included: clamp 0, threshold 53, continuation 0, S held 464 927, underflow 0.
- **U9** (w_gate < 1 at a drain): 0 at every one-loop and gap drain; **at classical drains of C-3233 (73 at every A) and
  C-3873 (1048 at A = 1)**. **P19** (unassessed): 0 candidates in all.
- **The ranges**, over every accepted integration: one-loop L 4.012–128.4, ln R 6.942–190.3, ℓ 0.0790–44.98; gap L
  2.578–106.0, ln R 0.0947–14.92; classical L 1.007 × 10⁻¹¹–125.6, ln R −239.4 to −0.0354.

### 2d.6 Parking and the first impact (§2 item 3)

[`out/parking.txt`](one-loop-drain/measure-02d/out/parking.txt), per history and A.
- **221 of the 230 successful tasks park;** the 9 that do not are C-3233 at all eight A and C-3873 at A = 1.
- **At laboratory M (P and W), the first impact is one-loop at every A and drains α_eff = A·f₁**, f₁ = 0.0823–0.115 at
  the first impact's ℓ (22.8–32.3): 0.82–0.99 of the kinetic energy at A = 10 (P-2188 capped at 0.99), 0.41–0.57 at A = 5,
  0.082–0.115 at A = 1, 0.0041–0.0057 at A = 0.05. Its share of K(N_final) runs from 0.90–0.99 at A = 10 to 0.04–0.35 at
  A = 0.05.
- **The parking N at P and W:** 18.9–24.7 at A = 10, 29.0–35.9 at A = 1, 37.7–38.7 at A = 0.1, 38.4–39.4 at A = 0.05; the
  drains before parking, 3–6, 5–21, 116–124 and 190–203. No drain follows parking in any task.
- **Where the first impact caps:** R-632 and the ladder at 5 × 10⁻²⁰ M_P at A = 5 and 10 (α_eff 0.99, and these are the
  same history at both A), 8 × 10⁻¹⁸ at A ≥ 2, 10⁻¹⁶ at A ≥ 1.5; 5 × 10⁻¹⁶'s first impact is a gap candidate capped at
  A ≥ 5. The cap acts by regime at 1–4 one-loop candidates per capped run (§2d.2).
- **The late parkers:** where the ladder's cascade runs a long classical tail it parks late, after 8 500–13 300 classical
  drains: 5 × 10⁻²⁰ at A = 0.1 (N = 47.60), 8 × 10⁻¹⁸ at A = 0.05 (46.87), 10⁻¹⁶ at A = 0.5 and 0.1 (45.56, 47.48),
  5 × 10⁻¹⁶ at A = 1 and 0.5 (44.99, 46.74); N_final is 50.08 there.

### 2d.7 The observables (§2 item 4)

[`out/observables.txt`](one-loop-drain/measure-02d/out/observables.txt), laboratory M first; Y_p and D/H are context
beside `bbn_constraints_explainer.tex`'s criteria, never a pass or a failure. **P-2197** (D/H in 10⁻⁵):

| A | 10 | 5 | 2 | 1.5 | 1 | 0.5 | 0.1 | 0.05 |
|---|---|---|---|---|---|---|---|---|
| ΔN_eff today | 0.5554 | 0.2727 | 0.1170 | 0.09026 | 0.06009 | 0.03280 | 0.01131 | 0.008558 |
| ΔN_eff 1 MeV | 0.5383 | 0.2636 | 0.1059 | 0.07945 | 0.05291 | 0.02639 | 0.005262 | 0.002630 |
| Y_p | 0.25400 | 0.24998 | 0.25092 | 0.25280 | 0.25692 | 0.25866 | 0.25863 | 0.25839 |
| D/H | 2.6411 | 2.5453 | 2.5571 | 2.6578 | 2.5343 | 2.7640 | 2.8628 | 2.8698 |

Across P and W ([`out/summary.txt`](one-loop-drain/measure-02d/out/summary.txt)): ΔN_eff today 0.029–0.634 at A = 10,
0.0056–0.067 at A = 1, 0.0026–0.0128 at A = 0.1 and 0.0025–0.0093 at A = 0.05; ΔN_eff at 1 MeV is A times a constant of
each history (P-2197: 0.0526–0.0538 × A), 0 for P-2188, whose first impact comes after 1 MeV. Y_p 0.2469–0.2550 at
A = 10 and 0.2552–0.2742 at A = 0.05 (W-2192 the highest at A ≤ 1); D/H 2.47–2.67 at A = 10 and 2.58–2.94 at A = 0.05.
Off laboratory M: the ladder's 5 × 10⁻²⁰ M_P gives ΔN_eff today 0.147 at A = 10 and 5 (capped), 0.128 at A = 1 and
0.018 at A = 0.1; at 4 × 10⁻¹⁵ M_P and in C it is 7 × 10⁻⁶ or less (C-3233: 10⁻⁵⁵), the parking leftover's or the
classical drains'.

### 2d.8 Against the old law (§2 item 5)

[`out/oldlaw.txt`](one-loop-drain/measure-02d/out/oldlaw.txt): each 02d line beside the old law's `zero` line, 02's at
A = 1 and 0.1, 02b's at the other six; 02b's S2 lines are marked "S2 of" their source.
- **Outcomes:** 229 pairs both succeed; **L-M5e-20 at A = 0.1 now succeeds** (old: the step budget); C-3873's seven
  timeouts stand where the old law succeeded; the three failures (R-632 at A = 0.1 and 0.05, L-M5e-20 at 0.05) failed
  under the old law too, of the same kinds.
- **Where they agree:** 8 pairs are equal exactly in ΔN_eff today, Y_p, D/H and the parking N: P-2188 at A = 10, R-632
  and the ladder at 5 × 10⁻²⁰ M_P at A = 10 and 5, and 8 × 10⁻¹⁸ at A = 10, 5 and 2, where both laws cap every drain
  before parking at 0.99 (`oldlaw.txt` lists them).
  At laboratory M the two laws agree closely: ΔN_eff today within 0.0085 (P) and 0.011 (W, R), Y_p within 3.8 × 10⁻⁴, D/H
  within 0.034 × 10⁻⁵; the parking N moves by up to 4.1 e-folds (P-2197 at A = 1 parks at 29.04 against 33.12, and at
  A = 2 at 33.52 against 29.64).
- **Where they differ most:** the ladder near M_crit, where the old `zero` prescription drained nothing in the gap and
  the measured law drains it. L-M5e-16 (first impact in the gap): ΔN_eff today 0.146 against 6.7 × 10⁻⁶ at A = 10 and 5,
  0.061 at A = 2, 0.031 at A = 1; Y_p up by 2.2 × 10⁻³ at A = 10; parking N 25.33 against 31.27 at A = 10, 46.74 against
  31.27 at A = 0.5. L-M1e-16 at A = 0.5: ΔN_eff today 0.0925 against 0.143, parking 45.56 against 30.68; at A = 0.05
  parking 34.81 against 46.77. In C the classical drains add ΔN_eff today of 10⁻⁸ or less (C-3233: 10⁻⁵⁵).
- **Parking:** of the 229 pairs, 220 park under both laws and 9 under neither (C-3233 at eight A, C-3873 at A = 1).

### 2d.9 Against the fixed law (§2 item 6)

[`out/fixed.txt`](one-loop-drain/measure-02d/out/fixed.txt): each history's eight A beside its 261 fixed-law references
(the 2026.8.0 store, a measurement on that tree, not a bound). At laboratory M the measured law at A = 0.05–0.1 gives
ΔN_eff today near the stored α = 10⁻³–10⁻² (P-2197: 0.0086 and 0.0113, against 0.0064 at α = 10⁻³ and 0.0123 at
α = 0.01), and at A = 1 near α = 0.08–0.1 (0.0601, against 0.0567 and 0.0707); it parks 5.7 (A = 0.05) to 26.3 (A = 10)
e-folds earlier than the stored α = 10⁻³ (45.16). At M = 10⁻¹² M_P the stored fixed-law ΔN_eff today (C-3016 0.0079 at α = 0.1, C-3233 0.335 at
α = 0.15, C-3873 0.023 at α = 0.03) falls to 7 × 10⁻⁶, 10⁻⁵⁵ and 7 × 10⁻²⁰ under the measured law, whose classical
drains are R·S(L) ≪ α; Y_p and D/H at C-3233 (0.4345, 10.41) belong to the history, not to the law.

### 2d.10 The cost (§2 item 7)

[`out/cost.txt`](one-loop-drain/measure-02d/out/cost.txt), per task beside the old law's line; walls under load
(§2d.3).
- **240 tasks, 40 985 s of task wall (11.38 h of CPU).** 220 tunings took one integration, 3 two, 9 three (C-3233 at
  every A, B-2774 at A = 1), and C-3873 24–28. Peak RSS of a task 1514 MiB (L-M5e-20 at A = 0.1).
- **P, W, R, B at A ≥ 0.5:** 12–70 s a task, against 12–30 s under the old law. At A = 0.1 and 0.05 the medians by
  group are 24–82 s, against 20–86 s (R-632's two failures took 279 and 82 s); P at A = 0.05 took 50 s against 26 s (the
  gap's first bounces, and load).
- **The ladder's classical tails:** where the cascade runs one, it now drains every classical candidate with a restart,
  and the steps after the first classical candidate are 0.57–1.00 of the integration's (0.999 in the long tails): L-M5e-16 at A = 1 took
  1 296 101 steps and 632 s, against 6 419 and 16 s under the old law (which parked it at 31.27); L-M1e-16 at A = 0.5,
  1 679 257 and 947 s against 23 975 and 28 s; L-M5e-20 at A = 0.1, 1 962 144 and 1115 s, where the old law hit the step
  budget. Where the old law already ran the tail, the cost is about the same (L-M8e-18 at A = 0.05 and L-M1e-16 at A = 0.1:
  1 479 155 and 1 441 817 steps, against 1 524 505 and 1 469 417), or far less where the field now parks early
  (L-M1e-16 at A = 0.05: 7 033 against 1 498 943, parking at 34.81).
- **C:** each tuning's integrations cost what the old law's did (C-3016 60 360 steps both; C-3233 53 872 against
  53 855; C-3873 251 665 at A = 1 against 252 008, 0.998 of them in the classical tail): a classical drain at R·S(L) ≪ 1 adds a
  restart but changes the step count by under 0.1%. **C-3873's cost is its tuning:** 28 integrations at A = 1 (3324 s)
  against 16 under the old law (1832 s), at 114–158 s each; under the batches' loads (up to 50) the other seven A reached
  24–27 integrations by the 3600 s timeout.

### 2d.11 The handover (§2 item 8)

[`out/handover.txt`](one-loop-drain/measure-02d/out/handover.txt). Measurements and the options they bear on; no choice
is made here.

**For 03, the bounce rows a store must hold** (one per candidate of the accepted integration):
- **3 to 13 343 candidates per history** (P-2188 at A = 10; L-M5e-20 at A = 0.1); 82 139 over the 230 successful tasks,
  82 115 of them drained. At laboratory M (P, W) 3–6 at A = 10 and 190–203 at A = 0.05; 8 500–13 300 where the ladder runs
  a classical tail; 301–2487 in C at every A.
- **By regime:** one-loop 11 256 candidates (L 4.012–128.4, ln R 6.942–190.3, ℓ 0.0790–44.98); gap 210 (L 2.578–106.0,
  ln R 0.0947–14.92); classical 70 673 (L 1.007 × 10⁻¹¹–125.6, ln R −239.4 to −0.0354); unassessed 0. The least α_eff at
  a drain is 2.3 × 10⁻¹⁰⁸, so a store column must carry a float of that range, and L below 10⁻¹⁰.
- **Record fields:** `DrainCandidate`'s eight fields reproduce α_eff exactly by `drain_alpha_effective` at every
  candidate (§2d.2), so a store of the candidates (with A) carries the law's inputs and outputs. The cap and the edges are
  derivable from them. 24 unresolved reflections declined, 0 unresolvable assessments, in the accepted integrations.

**For the user:**
- **What cannot be built on this code, under the measured law:** R-632 (β = 1.0) at A = 0.1 and 0.05 (the effective
  minimum); the ladder's 5 × 10⁻²⁰ M_P at A = 0.05 (the step budget); and C-3873 at A ≠ 1 within an hour on this machine
  under load (it tunes in 28 integrations at A = 1). Every P, W and B history builds at all eight A.
- **How the observables move with A** (§2d.7): at laboratory M, ΔN_eff at 1 MeV is proportional to A; ΔN_eff today falls
  more slowly than A below A = 0.5 (P-2197: 0.0328 at 0.5, 0.0113 at 0.1, 0.0086 at 0.05); Y_p and D/H in the transition
  window are not monotonic in A. Against the old law the measured law moves laboratory M by at most 0.011 in ΔN_eff today
  and 3.8 × 10⁻⁴ in Y_p, and the ladder near M_crit by up to 0.146.
- **The options these bear on:** whether R-632's β and the 5 × 10⁻²⁰ ladder rung are built at A = 0.05 and 0.1 (U14
  stands, with A = 0.05 kept regardless); and how C-3873's tuning cost is met in the rebuild (its trajectory is the same at
  every A, since its drains are R·S(L) ≈ 10⁻¹⁹ of the kinetic energy).

### 2d.12 The Remeasure gate

The gate holds, item by item (prompt §2):
1. **The sample measured:** 240 tasks by history and A, every one run: 230 succeed, 3 fail (of 02's two kinds), 7 time
   out (C-3873), with their reasons (§2d.4); the tool check and the determinism check bitwise (§2d.2).
2. **The regime shares across the eight A**, with ℓ_min, the gap's entry and R ≤ 1's, the law's edges, U9's and P19's
   counts (§2d.5).
3. **Parking and the first impact**, with the drains before parking and the cap by regime (§2d.6).
4. **The observables**, laboratory M first (§2d.7).
5. **Against the old law**, with where they differ most and where they agree (§2d.8).
6. **Against the fixed law** at all eight A (§2d.9).
7. **The cost**, with the classical tails' share of the steps and the batches (§2d.3, §2d.10).
8. **The handover** for 03 and for the user (§2d.11).

## 3. Prompt 03 — the store (2026-11-06)

**Tree:** branch `one-loop-drain` at `d3069cb` (the orchestrator note's commit; its parent `2a6825b` adds the prompt),
whose code and tests are those 02d's review passed. Prompt 03's commit changes the code (the key, the bounce table,
the history and sample columns, the guard, `FINGERPRINT_FORMAT` 7, `VERSION_LABEL` 2026.10.0; `architecture-summary.md`
and `numerical-strategies.md`, notes of 2026-11-06); §1–§2d above are unchanged. Python 3.13.16. **Scripts:**
[`one-loop-drain/store-03/`](one-loop-drain/store-03/), each with a docstring: `fixed_law_payloads.py` and
`compare_fixed_law.py` (§3.2), `cost_03.py` and `launch_03.sh` (§3.4). **Committed outputs:**
[`one-loop-drain/store-03/out/`](one-loop-drain/store-03/out/): `fixed_law.txt`, `cost.txt`. **Raw output** (not
committed): `var/one-loop-drain/03/`:
- `fixed_law_base.json`, `fixed_law_tip.json`: the three fixed-law payloads, key by key, at `d3069cb` (a scratch
  worktree, removed afterwards) and at `fe4c917`'s tree;
- `<id>.result.json`, `<id>.log`, `<id>.pid`, `<id>.done` for each of §3.4's six histories, and `stores/<id>/{full,
  base}/`, their temporary stores.

Orchestrator note 03's rulings 1–12 governed. No Ray was started, nothing was registered, and no store was opened but
the tests' temporary ones and §3.4's under `var/one-loop-drain/03/stores/`.

### 3.1 The Gate and the suites

The Gate held at `d3069cb`: the tree was clean; `runregistrykit list` showed 23 runs, all finished.

The suites before any change were measured on a scratch worktree of `d3069cb` (a first chain on the main checkout was
stopped, since edits had begun while it ran; see log 03 §4), serially, at 2026-11-06 00:45–01:06 local time, the
machine shared and loaded (one-minute loads 13–81):

| suite | count | time | load at start |
|---|---|---|---|
| CosmologyModels | 26 OK | 109.0 s | 65.25 23.89 13.40 (00:45) |
| ComputeTargets `test_[a-f]` | 162 OK | 176.9 s | 58.05 35.90 19.54 (00:47) |
| ComputeTargets `test_[g-z]` | 432 OK | 494.7 s | 37.21 45.42 27.24 (00:50) |
| ObjectFactories | 130 OK | 129.5 s | 13.31 16.48 19.62 (00:59) |
| tools | 156; 155 OK, 1 failing for the worktree's root (`test_registry_config`) | 163.8 s | 4.73 17.75 26.24 (01:13) |
| RunDrivers | 68; 67 OK, 1 failing for the worktree's root (`test_registered_run`, the script's path) | 25.0 s | 7.49 12.83 22.61 (01:16) |

The tools and RunDrivers rows are a second run with the checkout's `venv` linked into the worktree; in the first, ten
`tools/tests` failed for the worktree's lack of `./venv` and one for a batch timeout at a load of 41–81. The two that fail
in the worktree read the repository root (the run registry's `var/runs`, the script's path), and pass in the main
checkout's after-chain, unchanged.

and after the change, on `fe4c917`'s tree:

| suite | count | time | load at start |
|---|---|---|---|
| CosmologyModels | 26 OK | 63.2 s | 6.38 7.11 12.08 (01:36) |
| ComputeTargets `test_[a-f]` | 190 OK | 141.0 s | 4.94 6.52 11.52 (01:37) |
| ComputeTargets `test_[g-z]` | 432 OK | 459.5 s | 6.11 6.20 10.59 (01:40) |
| ObjectFactories | 149 OK | 118.4 s | 4.12 4.66 7.93 (01:47) |
| tools | 156 OK | 159.1 s | 4.54 4.63 7.48 (01:49) |
| RunDrivers | 68 OK | 22.8 s | 4.05 4.43 6.91 (01:52) |

ComputeTargets goes from 594 to 622 (`test_drain_store_records`, 28, in the `test_[a-f]` half) and ObjectFactories from 130
to 149 (`test_drain_store`, 19). No count fell.

### 3.2 The fixed law is unchanged (prompt §2.1)

- **The goldens** pass unchanged: `test_golden_history`, `test_golden_reflecting_history` and `test_tuning`, in the
  `test_[g-z]` half (§3.1).
- **The payloads, key by key** ([`out/fixed_law.txt`](one-loop-drain/store-03/out/fixed_law.txt)): three fixed-law
  runs at `d3069cb` and at this tree — `integrate_history` on the tuned golden history at its stored normalization
  (α = 0), on the reflecting setup (β = 1.5, M = 10⁻¹² M_P, α = 0.05, its seed realization), and `tune_history` on the
  golden history. Every key the base has is equal in the tip by `repr` (30, 30 and 45 keys; `metadata` compared by its
  `compute_steps` and `RHS_evaluations`, `z_grid` by its points), and the tip's only new keys are `"bounces"` (None) and
  `"log_m_eff"`, `"log_m_tracker"`.
- **The store's columns.** `test_dissipation_store` passes with no change but its version test's two literals (2026.10.0,
  7), and `test_tuned_history_round_trip` with none: every pre-existing column of a fixed-law row, history and samples,
  is written as before. `ObjectFactories/tests/test_drain_store.py` adds that a fixed-law row (α = 0.1 and α = 0) has no
  bounce row, every column of the drain record NULL, and the masses written.

### 3.3 The round trip and the fingerprint (prompt §2.2, §2.3)

**The round trip** (`ComputeTargets/tests/test_drain_store_records.py`, 28 tests; `ObjectFactories/tests/test_drain_store.py`,
19 tests), all passing:
- a one-loop history (2197 at A = 1, five one-loop candidates, parked) through a temporary store: every bounce column,
  every drain-record column and both masses raw on the row and on the object, bitwise to ruling 9's conversions; the
  bounce rows in ordinal order with their count, which `validate` checks; the same read with `_do_not_populate`, the
  record read and the rows not;
- a history with a gap candidate (8 × 10⁻¹⁸ M_P, β = 2, A = 2) and one with 28 classical candidates (4 × 10⁻¹⁵ M_P,
  A = 1), both parked;
- constructed extremes: α_eff = 2.3 × 10⁻¹⁰⁸, L = 10⁻¹¹, ln R = −239 and 13 400 bounce rows;
- the fixed law at α > 0 and at α = 0 (§3.2), a failure row (every new column NULL, no row);
- the key: one value under the two models is two `alpha_value` rows and two histories, a lookup under one finds neither
  the other's row nor its history, a lookup with no model is `fixed`; A = 0 and A < 0 under the one-loop model, α = 1.5
  under the fixed one and an unknown model are refused with no row written;
- the guard (P28): a one-loop payload under a fixed key, a fixed payload under a one-loop key, and a one-loop key with no
  drain record each store nothing; a fixed key with no drain record is accepted;
- in `ComputeTargets`: the bounce-row builder on constructed records (every regime, ΔK = 0, unassessed, events of both
  sources, ties in N, the margins failed); K before each candidate against a constructed ledger with K₀ and a parking
  leftover; the history record and its shares' sum (to 8 ε, with and without K₀ and parking), K_final = 0; the
  all-generation margins against the core with `MARGIN_WEIGHT_SHARE` patched to 0, ties included; the sample masses'
  NULLs; `compute_scalar_history` passing `drain_law` (default fixed) and `ScalarHistory.compute` passing the key's law;
  on 2197, ln R of every candidate equal to its `PeakEstimate`'s bitwise, the regime and the estimate in correspondence,
  the K chain against the ledger, the shares equal to `tools/history_and_bbn.drain_values` by `repr`; and
  `tune_history`'s masses equal to `integrate_history`'s for the same integration, by `repr`.

**The fingerprint.** `FINGERPRINT_FORMAT` is 7, with `SPEC_DIGESTS[7]` generated by `spec_digests` and the pins of
formats 1–6 unchanged (`test_fingerprint_format_guard` passes; `test_drain_store` checks that format 6's pin is as it
was). The golden at 7 is the new file `ObjectFactories/tests/data/store_fingerprint_one-loop-drain-03.json`, beside the
old ones, which are unchanged; `test_store_fingerprint_golden` passes against it.
- **Spec digests that moved:** `alpha_value`'s alone (its leaf `drain_model`, from its own factory; 14 distinct digests
  where format 6 had 13). `ScalarHistory`'s spec digest did not move: its spec names its parent `alpha` by the same
  column and table, and no new column.
- **Store-fingerprint digests that moved** (golden at 6 against golden at 7, same store but the `drain_model` of its one
  `alpha_value` row): the classes `alpha_value` (its key gains `drain_model`), `ScalarHistory` (its key resolves its parent
  `alpha`), `AdiabaticHistory` and `BBNData` (their keys resolve their history's), and the store's digest.

### 3.4 The cost and the volume (prompt §2.4; P12)

Six of 02d's accepted integrations, each run once, untuned, at its accepted normalization and φ_ref (the line's
`tuning`) and A, under the one-loop law, with T_stop = T_CMB, the tool's z grid and the default tolerances
(`cost_03.py`): one-loop only (P-2197, A = 1), the gap (W-2187, A = 0.05: 22 gap candidates), a mixed ladder rung
(8 × 10⁻¹⁸ M_P, A = 0.5: one-loop, gap and classical), a classical tail of 8 536 candidates (5 × 10⁻¹⁶ M_P, A = 1), and two
of C (3016, parked; 3233, not parked, 73 resolved candidates). C-3873 is excluded (its 02d tunings timed out), and none of
the six had a 02d wall above 1800 s. All six were launched detached at once at 01:02 and had ended by 01:13
([`out/cost.txt`](one-loop-drain/store-03/out/cost.txt); times in s; "added" is the bounce key, the masses, the record and
the write and read in full less the same history written and read with no bounce key and NULL masses):

| task | candidates (02d) | rows | integration | bounce key | masses | record | write / read, full | write / read, base | added | ratio | NULL masses (eff, tr) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| P-2197-A1 | 5 (5) | 9 | 2.64 | 0.000 | 0.381 | 0.0001 | 0.495 / 0.216 | 0.230 / 0.193 | 0.670 | 0.254 | 0, 0 |
| W-2187-A0.05 | 199 (199) | 397 | 37.69 | 0.004 | 0.411 | 0.0003 | 0.324 / 0.243 | 0.308 / 0.082 | 0.593 | 0.016 | 0, 0 |
| L-M8e-18-A0.5 | 48 (48) | 95 | 15.81 | 0.002 | 0.433 | 0.0001 | 0.222 / 0.121 | 0.292 / 0.152 | 0.435 | 0.028 | 0, 0 |
| L-M5e-16-A1 | 8 536 (8 536) | 17 071 | 652.62 | 0.167 | 0.188 | 0.0095 | 0.729 / 0.383 | 0.160 / 0.075 | 1.242 | 0.002 | 0, 0 |
| C-3016-A1 | 330 (330) | 659 | 77.98 | 0.007 | 0.380 | 0.0004 | 0.418 / 0.244 | 0.601 / 0.164 | 0.388 | 0.005 | 0, 0 |
| C-3233-A1 | 301 (301) | 602 | 58.48 | 0.011 | 0.200 | 0.0004 | 0.287 / 0.135 | 0.407 / 0.308 | 0.212 | 0.004 | 0, 0 |

**The median ratio is 0.0104, the largest 0.254** (P-2197, whose integration is 2.6 s); the stop (median > 0.25) is not
met. Counting the whole write and read as added instead, the median is 0.020 and the largest 0.414 (`cost.txt`, column
"whole"). The loads were heavy and uneven: one-minute 46.3 at the start, 4.8–103.3 at the six ends.

**The checks**, every one holding at all six: each `DrainCandidate` equal by `repr` to the last line of its 02d partial
file (so regime, α_eff and ln R per candidate are 02d's bitwise); ln R of every assessed candidate equal to its
`PeakEstimate`'s bitwise (0 differing of 9 419), and the regime and the estimate in correspondence (0 differing); the
stored shares, counts, ℓ_min and the drain record's two counts equal to 02d's `drain_values` by `repr`; the rows read back
equal to the payload's through the conversions, and no nonzero value converting to 0.0 or ±∞; rows stored, read and
`bounce_rows` equal. The margins were computed at all six. **ln T_J of the rows** (ruling 7): 73 candidates took their
wall-side `TurningPoint`'s (all at C-3233), 9 346 the dense output's (reflections), and 9 414 events their
`TurningPoint`'s. Rows are about twice the candidates: each candidate is followed by a plateau turning point.

### 3.5 The Store gate

The gate holds, item by item (prompt §2):
1. **The fixed law is unchanged** (§3.2): the goldens pass; the three fixed-law payloads equal the parent's key by key but
   for the new keys; every pre-existing column written as before.
2. **The round trip** (§3.3): exact, bounce rows in order; the fixed law and α = 0 write no bounce row and NULL drain
   columns; the key and the guard.
3. **The fingerprint** (§3.3): format 7 beside formats 1–6, its golden beside the old ones; `alpha_value`'s spec digest
   moved, and `ScalarHistory`'s did not.
4. **The cost and the volume** (§3.4): median added time 0.0104 of the integration, 17 071 rows for 8 536 candidates, every
   check holding.

## 4. Prompt 04 — drivers and exports (2026-11-06)

**Tree:** branch `one-loop-drain` at `8df05c1` (the orchestrator note's commit; its parent `6f6557e` adds the prompt, and the code
is 03's reviewed code, `50e93c7`/`fe4c917`). Prompt 04's commit changes the code (`--A-values`, the path and label helpers, the three
exports, the tools and the scope; `architecture-summary.md`, note of 2026-11-06); §1–§3 above are unchanged. Python 3.13.16.
**Scripts:** [`one-loop-drain/export-04/`](one-loop-drain/export-04/), each with a docstring: `build_store.py` (§4.2), `export_tree.py`
(one tree's three exports), `compare_exports.py` (§4.4), `check_new_columns.py` (§4.3). **Committed outputs:**
[`one-loop-drain/export-04/out/`](one-loop-drain/export-04/out/): `build_store.txt`, `compare.txt`, `new_columns.txt`. **Raw output** (not
committed): `var/one-loop-drain/04/`: `store/` (the two store files), `manifest.json`, `export-base/`, `export-tip/` (the exported files,
`labels.json`, `sha256.json`).

Orchestrator note 04's rulings 1–9 governed. No Ray was started, nothing was registered, and no store was opened but the tests'
temporary ones and §4.2's.

### 4.1 The Gate and the suites

The Gate held at `8df05c1`: the tree was clean on branch `one-loop-drain`; `runregistrykit list` showed 23 runs, all finished.

The suites before any change were measured on a scratch worktree of `8df05c1` (a first chain on the main checkout was stopped when
my edits began while it ran), serially, 2026-11-06, the machine shared (one-minute loads 8–38):

| suite | count | time | load at start |
|---|---|---|---|
| CosmologyModels | 26 OK | 70.5 s | 20.02 18.61 19.98 (09:48) |
| ComputeTargets `test_[a-f]` | 190 OK | 146.9 s | 37.66 23.72 21.76 (09:49) |
| ComputeTargets `test_[g-z]` | 432 OK | 487.0 s | 10.44 19.24 20.39 (09:52) |
| ObjectFactories | 149 OK | 126.4 s | 8.56 10.34 15.41 (10:00) |
| tools | 156; 155 OK, 1 failing for the worktree's root (`test_registry_config`) | 165.5 s | 19.10 12.68 10.34 (10:31) |
| RunDrivers | 68; 67 OK, 1 failing for the worktree's root (`test_registered_run`, the script's path) | 23.2 s | 7.53 10.53 9.88 (10:34) |

The tools and RunDrivers rows are a second run with the checkout's `venv` linked into the worktree (the first, without the link,
failed ten `tools/tests` for the worktree's lack of `./venv`, and one RunDrivers test); the two that fail in the worktree read the
repository root (the run registry's `var/runs`, the script's path), as in 03's record, and pass in the main checkout, which is where
the dispatch Gate (and 03's review) measured 156 and 68 passing. The main checkout could not be measured before the change: a first
chain was started there and stopped when edits began.

and after the change, on this prompt's tree (the main checkout):

| suite | count | time | load at start |
|---|---|---|---|
| CosmologyModels | 26 OK | 65.8 s | 8.23 8.80 12.05 (10:09) |
| ComputeTargets `test_[a-f]` | 219 OK | 144.9 s | 7.32 8.40 11.66 (10:10) |
| ComputeTargets `test_[g-z]` | 432 OK | 482.0 s | 6.43 7.27 10.65 (10:12) |
| ObjectFactories | 163 OK | 133.1 s | 9.79 7.19 8.24 (10:29) |
| tools | 173 OK | 260.9 s | 9.09 8.39 9.37 (10:23) |
| RunDrivers | 72 OK | 47.6 s | 4.25 6.04 8.05 (10:27) |

ComputeTargets goes from 622 to 651 (`test_a_values_option`, 23, and `test_a_values_plot_inputs`, 6, both in the `test_[a-f]` half),
ObjectFactories from 149 to 163 (`test_export_04`, 14), tools from 156 to 173 (`test_one_loop_tools`, 14, and 3 in `test_inventory_alpha`) and
RunDrivers from 68 to 72 (`test_scope_a_values`, 4). No count fell. ObjectFactories' first after-run failed
`test_alpha_figures.TestDarkRadiationFigure.test_8_the_dissipation_columns_of_the_record`, which pinned the dissipation columns as the last
before `failure_reasons`; the drain block now sits between (log 04 §2.1); the table is the second run (128.5 s and one failure in the first).

### 4.2 The store (prompt §2.2)

`build_store.py` writes seven histories through the harness of `test_drain_store` (`_DrainStoreCase`: the package's actor on a one-shard
store, in the stand-in cluster) and copies the store to `var/one-loop-drain/04/store/` (14 s; `out/build_store.txt`). Each is
`test_injection`'s setup (n = 1, φ* = 5 M_P, π* = 0, T* = 2×10⁴ GeV, to T_CMB, untuned at the model's seed realization):

| name | law and key | what it is |
|---|---|---|
| `F-alpha0` | fixed, α = 0 | M = 0.5 M_P, β = 2 |
| `F-alpha0.1` | fixed, α = 0.1 | M = 0.5 M_P, β = 2: reflects and drains (`test_dissipation_store`'s TEST_1) |
| `F-fail` | fixed, α = 0.1 | a failure row, M = 4×10⁻³¹ M_P, β = 1.185 |
| `O-2197` | one-loop, A = 1 | P-2197, M = 4×10⁻³¹ M_P, β = 1.185: five one-loop candidates, parked; 9 bounce rows |
| `O-classical` | one-loop, A = 1 | L-M4e-15, M = 4×10⁻¹⁵ M_P, β = 2: 28 classical candidates, parked; 55 bounce rows |
| `O-gap` | one-loop, A = 2 | L-M8e-18, M = 8×10⁻¹⁸ M_P, β = 2: three one-loop candidates and one in the gap; 7 bounce rows |
| `O-fail` | one-loop, A = 0.5 | a failure row |

**The store is read through the package's actor opened `read_only=True`** (with the label's lookup serial), not through
`ShardedPool(read_only=True)`: the harness writes the shard without registering shard keys in the primary, and the pool's read-only route
refuses `beta_value`'s shard key. Both trees read it the same way. BBN and Q are unavailable stand-ins in both trees (`export_tree.py`).

**The SHA-256 of both store files** (`store-shard0000.db` `88eeb523…501a`, `store.db` `6b5115ea…a266`) is the same before and after each
export, and the same under both trees (`out/compare.txt`, top).

### 4.3 The new columns equal the stored record (prompt §2.1; ruling 1)

The rule, the same for every new column of every file (ruling 1), true in any units: a cell read back from the file equals
`to_column(from_column(raw, kind, units), kind, units)` of the raw column, read by SQL from the store's file (mode=ro): by `float.hex`
for a float, as a value for an integer and a string, 0 or 1 for a Boolean, and blank for NULL. `check_new_columns.py` on the tip's export
(`out/new_columns.txt`), in the store's units (`GeV = 4.106×10⁻¹⁹`, `M_P = 1`):

| file | cells compared | failures of the rule | cells differing from the raw column (measurement) |
|---|---|---|---|
| histories.csv | 168 (`drain_model` and the 23 drain columns, 7 rows: 5 successes and 2 failures; the fixed rows' cells blank against NULL) | 0 | 0 |
| fields.csv | 52 886 (`log_m_eff_Mp` and `log_m_tracker_Mp`, every sample of 5 histories, matched by redshift) | 0 | 0 |
| bounces.csv | 2 130 (71 rows × 30 columns, in `ordinal` order, over the three one-loop histories) | 0 | 0 |

Zero cells differ from the raw column here: the stored value is `x − u` with x and u of one magnitude (Sterbenz), so adding and
subtracting u again is exact; ln M_P = 0 in `Planck_units`. **The rule does not rest on that.** `test_export_04` writes bounces.csv and
the mass cells from constructed rows under `Mpc_units` (GeV = 1.56×10³⁸, M_P = 3.8×10⁵⁶, neither 1) and checks every cell against
`bounce_row_columns(row, units)` and `to_column` by `float.hex`; breaking `to_column` in the mass cells leaves the tests in `Planck_units`
passing and fails that one. The same tests read the P-2197 (A = 1) history, a fixed history at α = 0.1 and one at α = 0 back from a
temporary store and check the rule against the raw columns; the one-loop history with no bounce row writes a header alone.

### 4.4 The existing columns are unchanged (prompt §2.2)

`export_tree.py` was run from the root of each tree: this checkout and a scratch worktree of `8df05c1` (removed afterwards), each with this
checkout's interpreter and `PYTHONPATH=.`. Neither copies code: the tree's `extract_common` functions are called, and the tree's own
`write_csv_file` is **extracted from its `plot_ScalarHistory.py` by `ast` and executed unedited** (the script parses its arguments on
import). `compare_exports.py` projects the tip's file onto the base's columns, in the base's order, written with the base's writer settings
(`csv.writer` defaults, line terminator `\r\n` for histories.csv and `\n` for fields.csv), and compares byte for byte (`out/compare.txt`):

| product | base bytes | projected tip bytes | identical |
|---|---|---|---|
| histories.csv, the fixed-law records (`F-alpha0`, `F-alpha0.1`, `F-fail`) | 1 144 | 1 144 | **yes** |
| histories.csv, the one-loop records (`O-2197`, `O-classical`, `O-gap`, `O-fail`) | 1 685 | 1 685 | yes |
| fields.csv `F-alpha0` (5 287 rows) | 2 428 214 | 2 428 214 | **yes** |
| fields.csv `F-alpha0.1` (5 287 rows) | 2 532 012 | 2 532 012 | **yes** |
| fields.csv `O-2197` (4 995 rows) | 2 391 847 | 2 391 847 | yes |
| fields.csv `O-classical` (5 437 rows) | 2 618 309 | 2 618 309 | yes |
| fields.csv `O-gap` (5 437 rows) | 2 608 383 | 2 608 383 | yes |

The tip's files add exactly `drain_model` and the 23 drain columns to histories.csv and `log_m_eff_Mp` and `log_m_tracker_Mp` to fields.csv,
the base's columns kept in the base's order. **Every existing column agrees for the one-loop histories too:** nothing to explain.

**Paths and labels**, as each tree's own code gives them (`labels.json`; the strings are those of `alpha_path`, of the two figure-label
functions drawn on a `Figure`, and of the drivers' f-strings, evaluated by `ast` on stand-in names; `compare.txt` lists all of them):

| history | product | base | tip |
|---|---|---|---|
| fixed, α = 0.1 | directory | `<label>/alpha=0.1` | `<label>/alpha=0.1` (equal) |
| fixed, α = 0.1 | cosmology line of the figures | `Cosmology: label, α = 0.1` | equal |
| fixed, α = 0.1 | header line of every `run_pipeline`, `build_beta_plot` titles and failure line | `… alpha = 0.1`, `label, α = 0.1: M = …`, `… alpha=0.1: no valid BBN data …` | equal, string for string |
| one-loop, A = 1 | directory | `<label>/alpha=1` | `<label>/one-loop-measured-v1/A=1` |
| one-loop, A = 1 | cosmology line | `Cosmology: label, α = 1` | `Cosmology: label, A = 1 (one-loop-measured-v1)` |
| one-loop, A = 1 | header line | `… alpha = 1` | `… A = 1 (one-loop-measured-v1)` |
| one-loop, A = 1 | `build_beta_plot` title; failure line | `label, α = 1: M = …`; `… alpha=1: …` | `label, A = 1 (one-loop-measured-v1): M = …`; `… A=1 (one-loop-measured-v1): …` |
| all four α > 0 in the store | `Delta_N_eff` legend | `$\alpha$ = 0.1, 0.5, 1, 2` | `$\alpha$ = 0.1`, then `A = 0.5 (…)`, `A = 1 (…)`, `A = 2 (…)` (fixed first) |

The base's one-loop rows show what the base would have written for them (a `fixed`-looking `alpha=1` directory), which is why the model enters
the path.

### 4.5 The drivers refuse a law that does not match the store (prompt §2.3)

- **The parse refusals of `--A-values`** (`ComputeTargets/tests/test_a_values_option.py`): 0, −0, −1, nan, inf and an unparsable text each
  exit 2 with `argument --A-values:` and `check_drain_model`'s message, before any Ray; 10, 1000 and 1e6 are accepted; duplicates collapse
  in the order of first appearance.
- **`plot_inputs.parameters`** on a temporary store with α = 0 and 0.5 under `fixed` and A = 0.5 and 1 under the one-loop model
  (`test_a_values_plot_inputs`): the rows come back fixed first, each with its model; an A stored only as an α is
  `PlotOptionNotStored` naming `--A-values`, the value and the model; an α stored only as an A is refused naming `--alpha-values`; the
  first missing row in the order is named after the parameters before it; the read-only pool writes no byte and creates no row.
- **`test_alpha_plot_inputs`' handler test** still shows both scripts catching `PlotOptionNotStored`, printing it to stderr and exiting 2
  (unchanged, passing).

### 4.6 The Export gate

The gate holds, item by item (prompt §2):
1. **The new columns equal the stored record** (§4.3): 168 + 52 886 + 2 130 cells, no failure of ruling 1's rule; 0 cells differ from the
   raw column in the store's units; the rule exercised under a second `UnitsLike`.
2. **The existing columns are unchanged** (§4.4): histories.csv (fixed-law records) and fields.csv (every history), projected onto the
   base's columns, byte-identical to the dispatch tree's from the same store; the one-loop histories' existing columns agree too; the
   fixed-law paths and titles are equal, and the one-loop ones follow P35. The store's SHA-256 is unchanged under both exports.
3. **The drivers refuse a law that does not match the store** (§4.5).

## 5. Prompt 05 — the derivation tool (2026-11-06)

**Tree:** branch `one-loop-drain` at `88c6ae5` (orchestrator note 05's commit; `4820df5` records U16 and `8378b4f` adds the
prompt; the code is 04's reviewed code). Prompt 05's commit adds `ComputeTargets/quanta_spectrum.py` and
`tools/quanta_measures.py` with their tests, `extract_common.history_directory` (and `plot_ScalarHistory.py`'s two calls),
and the dated notes of `architecture-summary.md` and `numerical-strategies.md`; §1–§4 above are unchanged. Python 3.13.16,
numpy 2.3.5, scipy 1.17.0.
**Scripts:** [`one-loop-drain/derive-05/`](one-loop-drain/derive-05/), each with a docstring: `kernels_05.py` (§5.3), `probe_05.py`
(§5.5), `run_05.py` and `launch_05.sh` (§5.6), `report_05.py` (§5.6, §5.7). **Committed outputs:**
[`one-loop-drain/derive-05/out/`](one-loop-drain/derive-05/out/): `kernels.txt`, `probe.txt`, `run.txt` (the scheduler's log and
summary), `tool-assoc1.txt`, `tool-assoc0.txt`, `tool-assoc10.txt` (the tool's lines at the three scales), `report.txt`.
**Raw output** (not committed): `var/one-loop-drain/05/`: `run/` (each child's pid, log, result and end marker), `records/`,
`parts/`, `exports/QCD_Cosmology/` (the merged histories.csv, 230 history directories with fields.csv, bounces.csv and the
tool's files at the three scales, and `quanta_histories*.csv`), `probe/` (§5.5's export), `scheduler.*`.

Orchestrator note 05's rulings 1–12 governed. No Ray was started, nothing was registered, and no store was opened but the
tests' and the run's temporary ones, all deleted.

### 5.1 The Gate and the suites

The Gate held at `88c6ae5` (11:56): the tree clean on `one-loop-drain`; `runregistrykit list` 23 runs, all finished; 02d's
`sample.jsonl` 240 lines and its `tasks/` present. The suites, serially (log 05 §4 has the tables): before, 26, 651 (219 +
432), 163, 173, 72, all passing (ObjectFactories, tools and RunDrivers with the helper's additive edit already in the tree,
log 05 §2); after, on this prompt's tree, 26, 665 (219 + 446), 163, 191, 72, all passing (`test_quanta_spectrum` adds 14 to
the `test_[g-z]` half; `test_quanta_measures` 16 and `test_history_directory` 2 to tools).

### 5.2 The spectrum and its constants (prompt §1.1; rulings 1, 2)

`SPECTRUM_MODEL = "U4-U17-v1"`. s = s_dir + c_a s_assoc; s_dir = x² (x ≤ x_p), x_p² e^{−κ(x − x_p)} above; s_assoc = 49 x_*³/x
(x ≤ x_p), 49 x_*³/x · e^{−κ(x − x_p)} above (U17, the user's ruling of 2026-11-06).

| constant | value | source |
|---|---|---|
| x_p | 0.62 | `production_energy_ratio.tex` §"Results" and Table tab:beta: k_peak = 0.31 ω_t, k_ex = ω_t/2 (U4) |
| κ | 3.9657820823215935 | κ_L = ln(\|β\|²_max,L/10⁻²⁰)/(10 − x_p) at L = 90 (\|β\|²_max = 1.43 × 10⁻⁴), the median row; the same section's "about 10⁻²⁰ at k = 5 ω_t" (x = 10) |
| κ_L, the five rows | 4.1334 (40), 4.0178 (70), 3.9658 (90), 3.9235 (110), 3.8821 (134) | the fit's spread, 6.4% |
| x_* | 0.0058 | `kb_partB_report.tex` §"The soft occupation": q̂ = 0.0029 ω_t |
| 49 | 50 − 1 | the same: n_q there is "fifty times the test field's \|β_q\|²" |

`BriefingNotes/scripts/` does not hold the spectrum's script (`beta_k_reflection.py`, named in §"Provenance"), so the fit is to
the Table and the statement. Checked by the tests: s(x_*) = 50 s_dir(x_*) at c_a = 1 (to 10⁻¹²); s continuous at x_p; κ equal
to the formula; the associated piece's share of I 2.95 × 10⁻⁵ at c_a = 1.

| c_a | I | J |
|---|---|---|
| 1 | 0.09300715800293864 | 1.0560436029665003 |
| 0 | 0.09300441145732584 | 1.0560324249819 |
| 10 | 0.09303187691345381 | 1.056144175127589 |

Each in closed form, and equal to quadrature within 6.7 × 10⁻¹⁶ (`out/kernels.txt`).

### 5.3 The kernels against direct quadrature (prompt §2.1; ruling 3)

Grids: ln y ∈ [ln 10⁻³, ln 50], step 0.01 (1 083 nodes); u ∈ [0, 4], step 0.005 (801 nodes), Ω(u) = e^{4u} Ω(−u) below 0. Tables
built in 0.013 s (G) and 0.16 s (Ω) per scale. `kernels_05.py` against scipy's adaptive quad, split at x_p and at y (G) or
x_p e^u (Ω) and at the tails' scales, whose stated error is at most 4.3 × 10⁻¹⁴ at every point:

| kernel | points per scale | largest relative difference, c_a = 1 / 0 / 10 |
|---|---|---|
| G, every node and midpoint | 2 165 | 8.1 × 10⁻¹² / 8.1 × 10⁻¹² / 8.1 × 10⁻¹² |
| G, beyond the table (to four decades each way) | 400 | 1.6 × 10⁻¹¹ / 6.0 × 10⁻¹² / 5.6 × 10⁻¹¹ |
| Ω, every node and midpoint on [0, 4], every fourth mirrored | 2 001 | 2.25 × 10⁻⁹ (at u = 0.0025) at all three |
| Ω, beyond (to \|u\| = 14) | 200 | 4.4 × 10⁻¹⁵ / 8.4 × 10⁻¹⁵ / 4.0 × 10⁻¹⁵ |
| M_1 … M_10 | 10 | 4.4 × 10⁻¹⁶ |

The asymptotes at the tables' ends, term by term (relative to the leading term), and the series against quadrature:

| asymptote, at | kept terms | series − quad |
|---|---|---|
| G, y = 10⁻³ | M_1y²/2; −Cy³/3: 5.2 × 10⁻⁸ (c_a = 1), 5.2 × 10⁻⁷ (10), 0 (0); B_4y⁴: −6.4 × 10⁻⁷; left out O(y⁴ ln y) = 6.6 × 10⁻¹² | −7.1 × 10⁻¹² |
| G, y = 50 | M_2y/I; −1: −1.9 × 10⁻²; M_4/2y: 2.1 × 10⁻⁴; −M_6/8y³: −4.2 × 10⁻⁸; M_8/16y⁵: 3.0 × 10⁻¹¹; left out ≤ 4.4 × 10⁻¹⁴ | 4.4 × 10⁻¹⁴ |
| Ω, u = 4 | e^{−2u}M_5/I (4.9 × 10⁻⁴) + Ce^uM_2/I (5.5 × 10⁻⁴ at 1, 5.5 × 10⁻³ at 10, 0 at 0); left out of order e^{−134} | ≤ 1.8 × 10⁻¹⁵ |

Every difference is below 10⁻⁶, and every asymptote is within 10⁻⁷ at its end: §2.1 holds.

### 5.4 The tool on constructed exports (prompt §1.7)

`tools/tests/test_quanta_measures.py` writes histories.csv, fields.csv and bounces.csv by 04's writers from constructed rows
(five generations, two of them beyond Ω's table, an unassessed drain, a candidate that drains nothing, a plateau event; r_χ
from a `DarkRadiationLedger`) and checks, at c_a = 1, 0 and 10, E at every sample for both masses, ln n_g, n̄_j, δ_stim and the
backreaction ratio against direct quadrature in q over the full spectrum (no kernel), to 10⁻⁶, and the phase slip against
`phase_slip.py`'s formula; K(N) with no leftover, with the leftover after the last candidate and with it bracketed by a later
candidate; K_0 > 0; the refusals of ruling 6; a failed check (nothing written, exit 1); the check in three `UnitsLike`; the
duplicate directory, the fixed row, the failed history, the missing file and the non-integer n; every column, its blanks and
the model's columns, and the exit statuses. 16 tests, all passing; with the spectrum's 14 and the helper's 2.

### 5.5 Against the scoping probe, peak-collapsed (prompt §2.2; ruling 5)

`probe_05.py` (`out/probe.txt`): P-2197 at A = 1 by `one_loop_payload`, written on a temporary store, read back and exported to
`var/one-loop-drain/05/probe/`. The tool's `matter_excess` with `PEAK_SPECTRUM` on the exported files, against the probe's
`excess` extracted by `ast` and run unedited on the payload. 4 995 samples, 5 generations (the payload's 5), the leftover after
the last candidate at N = 29.035.

Tolerance: |E_tool − E_probe| ≤ 2ε Σ_born ΔK_g/K(N) + 8ε n_born |E_probe| (reasons in the script's docstring).

| mass | compared | equal bit for bit | matched blanks: mass None / K(N) = 0 | beyond the tolerance | largest \|diff\|/(a part) | largest \|diff\|/(b part) | largest \|diff\|/\|E_probe\| |
|---|---|---|---|---|---|---|---|
| field | 3 081 | 1 806 | 0 / 1 914 | 0 | 1.2 × 10⁻⁴¹ | 0.30 | 5.3 × 10⁻¹⁶ (2.4ε) |
| tracker | 3 081 | 1 782 | 0 / 1 914 | 0 | 3.9 × 10⁻³⁴ | 0.15 | 5.6 × 10⁻¹⁶ (2.5ε) |

P-2197's masses put every y far below e^{−20}, so no term falls in the probe's cancellation band (−20 < ln x < −8: 0 samples)
and the a part, which allows for it, is never used. None of the 1 914 samples with K(N) = 0 has a generation born.
**K(N):** the tool's rebuilt K equals the payload ledger's (by `to_column`) bit for bit at all 4 995 samples, before the
leftover (3 153) and after it (1 842). §2.2 holds.

### 5.6 The run on 02d's sample (prompt §1.6, §2.3; ruling 11)

`launch_05.sh` started `run_05.py scheduler` once (pid 26565, 12:31:33); it ended at 12:58:15 (`scheduler.done`), 1 601 s, five
workers, the largest first. **All 230 tasks succeeded**; none was stopped at its wall limit. Every child's step 2 held: its
`DrainCandidate`s equal to the last line of 02d's partial by `repr` (230 of 230; 82 139 candidates); its samples 02d's count
(1 299 063 in all); fields.csv and bounces.csv read back with the object's samples and bounce rows (and the stored rows and
`bounce_rows`); its temporary store deleted (none left). **The merge:** 230 records, the merged histories.csv's header each
part's and its 230 data rows the parts' rows byte for byte. No pid recorded under `var/one-loop-drain/05/` answers `kill -0`.

| step | sum | median | max |
|---|---|---|---|
| integrate | 6 821.6 s | 4.28 s | 981.3 s (L-M5e-20-A0.1; 02d 1 079 s) |
| store and read back | 429.5 s | 1.81 s | 3.6 s |
| export | 55.4 s | 0.15 s | 3.5 s |
| read back the export, delete the store | 0.8 s | 0.00 s | 0.02 s |

The integration's wall against 02d's per-integration wall: median 1.02 (0.36–1.31). Peak RSS 1.26 GiB (L-M1e-16-A0.5), median
0.25 GiB. One-minute loads at the children's starts and ends 2.8–40.0.

**The tool** on `var/one-loop-drain/05/exports/QCD_Cosmology/` (`out/tool-assoc*.txt`): at c_a = 1, 0 and 10, 230 measured, 0
skipped, missing, refused or failing the check, exit 0; 53.0 s, 50.7 s and 51.9 s. Its wall per history against that history's
integration: median 0.032, max 0.12 (L-M1e-16-A0.05); the largest, 2.1 s, is L-M5e-20-A0.1, 13 342 generations, about
8.9 × 10⁷ evaluations of Ω for n̄ (untruncated). Run again at c_a = 1, its 461 files are byte-identical.
- **The leftover's placement:** after the last candidate in 221 histories, none in 9; bracketed by a later candidate in none.
- **The K check:** largest relative difference 1.46 × 10⁻¹¹ (C-3016-A0.05, whose parking share is 0.999993), against the
  tolerance 10⁻⁹. Its two sources measured over the run (`report.txt` §8): |ln K| + 4|N| + |ln ρ_R,E| + 4|ln(GeV/M_P)| at most
  882.8, so 2.0 × 10⁻¹³ per pass; parking_share/(1 − parking_share) at most 1.44 × 10⁵, so 3.2 × 10⁻¹¹; the margin 31.
  K_before + ΔK equals the next K_before bit for bit at all 81 909 pairs.
- **Ruling 7:** no sample has K(N) = 0 with a generation born; 45 samples, in 9 histories, lie between a generation's N_turn
  and its N (the most, 29, in C-3873-A1). **Ruling 8:** no history has a drain without a spectrum.

§2.3 holds: every end marker, every step 2 check equal, no failed task, the tool at the three scales.

### 5.7 The measures on the sample (prompt §1.6's report)

`out/report.txt` §5 gives, per A and per group, the median and the maximum at c_a = 1 of δ_stim, n̄_max, ln n_g max, the E
maxima and r_χ E's for both masses, the least producing phase slip (with its minimum) and the backreaction maximum; §6 the same at
c_a = 0 and 10 as per-history ratios to c_a = 1. In brief:
- **δ_stim** over the 230 histories: 2.2 × 10⁻⁷⁵ (B-2772-A10) to 1.34 × 10⁻³ (L-M1e-16-A0.5), median 3.5 × 10⁻¹⁷. By group, the
  maximum is 4.8 × 10⁻⁵ (P), 9.0 × 10⁻⁵ (W), 6.2 × 10⁻¹¹ (B), 1.3 × 10⁻³ (L), 1.8 × 10⁻⁶ (R), 8.0 × 10⁻⁴ (C); by A, the median falls
  from 1.0 × 10⁻⁵ at A = 0.05 to 2.6 × 10⁻⁵³ at A = 10, and the maximum stays between 1.6 × 10⁻⁴ and 1.3 × 10⁻³.
- **n̄_max** is below 0.01 in every P, W, B and R history; above 1 in 33 histories, all on the L ladder (5 × 10⁻²⁰ to 4 × 10⁻¹⁵
  M_P, β = 2) and in group C (10⁻¹² M_P), up to 9.0 × 10³¹ (L-M5e-20-A0.1, whose δ_stim is 4.2 × 10⁻⁹: the large overlaps sit at
  bounces that drain almost nothing). ln n_g max likewise, up to 98.
- **E** is negligible everywhere: E_field max at most 5.4 × 10⁻²⁰, E_tracker max at most 7.2 × 10⁻¹⁶, r_χ E at most 1.9 × 10⁻²¹
  (field) and 7.4 × 10⁻²¹ (tracker).
- **The phase slip** at producing bounces is at least 7.8 × 10²¹ rad in every P, W, B and R history (median 6 × 10²²–3 × 10²⁸ by
  A); it falls below 1 rad in 15 histories, on the L ladder (5 × 10⁻²⁰ to 5 × 10⁻¹⁶ M_P) and in group C at 10⁻¹² M_P, β = 1.0 and
  1.11 (least 5.1 × 10⁻¹⁰ rad, C-3873-A1).
- **The backreaction ratio** has a median of 10⁻⁹–6 × 10⁻² by A and a maximum of 1.8 × 10³ (on the L ladder); P ≤ 244, W ≤ 258,
  R ≤ 918, B ≤ 0.10, C ≤ 2.8 × 10⁻⁷.
- **The associated piece's scale** moves only δ_stim and n̄: at c_a = 0 the ratio to c_a = 1 is 1 at the median, down to
  5.6 × 10⁻⁸ (δ_stim) and 3.5 × 10⁻¹⁶ (n̄_max) at the least; at c_a = 10 it is 1 at the median and at most 10 (both). Every
  other measure (n_g, E, r_χ E, the slip, the backreaction) moves by less than the third significant figure.

These are measurements, not bounds (P49); no threshold refuses or warns on any of them.

### 5.8 The Derive gate

The gate holds, item by item (prompt §2):
1. **Against direct quadrature** (§5.3, §5.4): every kernel and moment within 2.3 × 10⁻⁹ of quadrature on the grid and beyond it at
   c_a = 1, 0 and 10; every asymptote within 10⁻⁷ at its end; every measure of the tool within 10⁻⁶ of a direct computation on
   constructed generations.
2. **Against the scoping probe, peak-collapsed** (§5.5): E within 2.5ε relative of `excess` at all 3 081 compared samples for both
   masses, every blank matched in its own case; K(N) equal to the ledger's bit for bit.
3. **On the sample** (§5.6): 230 of 230 tasks, every step 2 check equal, the merged histories.csv equal to its parts, the tool at the
   three scales with every history measured and its K check within 1.5 × 10⁻¹¹.

## 6. Prompt 06 — the rehearsal (2026-11-07)

**Tree:** branch `one-loop-drain` at `41d18c4` (orchestrator note 06's commit, on `d05b625`, which adds the prompt); the code is
05's reviewed code, `VERSION_LABEL` 2026.10.0, `FINGERPRINT_FORMAT` 7. The base build ran from a detached worktree of `1561b60`
(2026.9.0, format 6), since removed. §1–§5 above are unchanged. Nothing in the code changed.
**Scripts:** [`one-loop-drain/rehearse-06/`](one-loop-drain/rehearse-06/), each with a docstring: `code_probe.py` (§6.1),
`histories.py` (§6.3), `compare_base.py` (§6.4), `compare_02d.py` (§6.5), `compare_exports.py` (§6.5), and the helpers copied from
`inelastic-reflection` 11 (`wait_for_line.py`, `beat_times.py`, `ray_state.py`, `silences.py`, `check.sh`, `suite.sh`; each
adaptation is a diff in `out/06-*.diff`). **Committed outputs:** [`one-loop-drain/rehearse-06/out/`](one-loop-drain/rehearse-06/out/):
the runs' logs (ANSI codes removed), recorders, manifests, status files and the three sidecars; the probe's three outputs; the
comparisons; the histories readers; histories.csv and quanta_histories.csv at the three scales; the readers' logs and the
store shasums; the loads, the cluster, the worktree and the suite records. **Raw output** (not committed):
`var/one-loop-drain/06/` (`runs/`, `readers/`, `plot-reference/`, `plot-resumed/` with the figures, and the files copied to `out/`);
the stores `var/datastores/one-loop-drain-06-{base,reference,resumed}*` with their sidecars; the run records under `var/runs/`.

Orchestrator note 06's rulings 1–9 governed. The machine was shared: one-minute loads of 8.6–37 at the launches, up to 149
during the suites after.

### 6.1 The cluster, the base tree and the probe (prompt §1.1, §1.2; rulings 1, 6)

- **The base tree** ([`06-base-code.txt`](one-loop-drain/rehearse-06/out/06-base-code.txt),
  [`06-worktree.txt`](one-loop-drain/rehearse-06/out/06-worktree.txt)). `git diff --name-only 1561b60 9616e04` lists six
  documents and no code. At `1561b60`, `VERSION_LABEL = "2026.9.0"` and `FINGERPRINT_FORMAT = 6`. `git diff --stat 1561b60 HEAD`
  over `PRyMrates PRyM CosmologyModels/GenericEOS exponential.yaml requirements.txt runregistrykit.toml thirdparty` is empty.
  `main.py`'s SHA-256 is `92117838…acc0` at `1561b60` and `2b8956ff…c0e9` at the tip. The worktree's `git status` before the
  first base run and after the last: `?? var`, `?? venv` (the two links) and nothing else.
- **The cluster** ([`06-cluster.txt`](one-loop-drain/rehearse-06/out/06-cluster.txt)): `tools/ray_cluster.sh start
  "one-loop-drain 06 (rehearsal)"` from the main checkout with `PYTHONPATH` unset, exit 0, `started in 8s; gcs_server pid 67063`
  at 08:25 UTC; `status` and the lock's `status`: HELD, purpose `one-loop-drain 06 (rehearsal)`. The token's value appears in no
  output (`grep -c -F -f ~/.ray/auth_token`: 0).
- **The probe**, from each tree's root, as ruling 1 gives it
  ([`06-probe-base-before.txt`](one-loop-drain/rehearse-06/out/06-probe-base-before.txt),
  [`06-probe-tip-before.txt`](one-loop-drain/rehearse-06/out/06-probe-tip-before.txt),
  [`06-probe-tip-after-base.txt`](one-loop-drain/rehearse-06/out/06-probe-tip-after-base.txt)); each exit 0:

  | run | process | `ScalarHistory.py`, `ObjectFactories/__init__.py` under | labels | `sys.path[0]` | cwd | `PYTHONPATH` |
  |---|---|---|---|---|---|---|
  | worktree, before B0 | driver | the worktree | 2026.9.0, 6 | ray's `thirdparty_files` (then the probe's dir, the worktree) | the worktree | `.` |
  | | task, actor | the worktree | 2026.9.0, 6 | the worktree | **the main checkout** | None |
  | main checkout, before B0 | driver | the main checkout | 2026.10.0, 7 | ray's `thirdparty_files` (then the probe's dir, the checkout) | the main checkout | `.` |
  | | task, actor | the main checkout | 2026.10.0, 7 | the main checkout | the main checkout | None |
  | main checkout, after B0 | all three | the main checkout | 2026.10.0, 7 | as before | the main checkout | as before |

  The job's `py_driver_sys_path` was `[<probe dir>, <the tree's root>]` each time: Ray puts the driver's tree root first on each
  worker's path, so a worker imports its own driver's tree. A worker's working directory is the cluster's, the main checkout,
  for a base driver too; ruling 1 allows it, since the data files read from it are identical at both commits (above).

### 6.2 The builds (prompt §1.3, §1.4)

Each run went through the wrapper, detached through `sh`, with a recorder beside it; registration seen in 2.0 s each; one
check ([`06-checks.txt`](one-loop-drain/rehearse-06/out/06-checks.txt): the pid file equals `status.json`'s pid, state
`running`); its end waited on in the background by `wait_for_line.py --timeout 3600`. The 17 histories of each fixed-law
grid and the 9 one-loop histories are as prompt §1.3 gives them; the one-loop ones are 02d's tasks P-2188, P-2197, R-865 and
R-611 at A = 0.1 and 1 and C-3233 at A = 1, each a 02d `success` whose `argv` is `<β> <M> --drain-law one_loop --alpha <A>` and
nothing else ([`06-02d-tasks.txt`](one-loop-drain/rehearse-06/out/06-02d-tasks.txt)).

| Run id | Slug | Store | pid | State | Exit | Launched → ended (UTC) | Wall | Load at launch | `git_head` | `git_dirty` | `script_sha256` | Fingerprint at finish (format, digest, taken) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `one-loop-drain-06-base-a-20261107T082601` | `base-a` | base | 67682 | `done` | 0 | 08:26:00 → 08:29:38 | 3 min 38 s | 17.40 | `1561b60` | true | `92117838…acc0` | 6, `79f848b2…0c2b`, 08:29:13 |
| `one-loop-drain-06-base-b-20261107T082957` | `base-b` | base | 68244 | `done` | 0 | 08:29:55 → 08:34:37 | 4 min 42 s | 37.11 | `1561b60` | true | `92117838…acc0` | 6, `248381db…5517`, 08:34:12 |
| `one-loop-drain-06-base-c-20261107T083454` | `base-c` | base | 69351 | `done` | 0 | 08:34:53 → 08:35:51 | 58 s | 9.47 | `1561b60` | true | `92117838…acc0` | 6, `ff38053c…f4a7`, 08:35:26 |
| `one-loop-drain-06-reference-a-20261107T083701` | `reference-a` | reference | 69796 | `done` | 0 | 08:37:00 → 08:42:46 | 5 min 46 s | 13.87 | `41d18c4` | true | `2b8956ff…c0e9` | 7, `b1bd5716…7f20`, 08:42:21 |
| `one-loop-drain-06-reference-b-20261107T084318` | `reference-b` | reference | 70472 | `done` | 0 | 08:43:16 → 08:47:57 | 4 min 40 s | 10.49 | `41d18c4` | true | `2b8956ff…c0e9` | 7, `d4fd784b…801f`, 08:47:31 |
| `one-loop-drain-06-reference-c-20261107T084809` | `reference-c` | reference | 75085 | `done` | 0 | 08:48:07 → 08:50:48 | 2 min 41 s | 9.64 | `41d18c4` | true | `2b8956ff…c0e9` | 7, `7c7641d2…dd04`, 08:50:23 |
| `one-loop-drain-06-resumed-a-20261107T085145` | `resumed-a` | resumed | 90059 | `killed` | 143 | 08:51:44 → 08:56:12 | 4 min 28 s | 10.20 | `41d18c4` | true | `2b8956ff…c0e9` | 7, `baf4bc32…3e57`, 08:55:47 |
| `one-loop-drain-06-resumed-a2-20261107T085643` | `resumed-a2` | resumed | 90981 | `done` | 0 | 08:56:42 → 08:59:17 | 2 min 35 s | 8.64 | `41d18c4` | true | `2b8956ff…c0e9` | 7, `b1bd5716…7f20`, 08:58:53 |
| `one-loop-drain-06-resumed-b-20261107T085932` | `resumed-b` | resumed | 91430 | `done` | 0 | 08:59:31 → 09:04:24 | 4 min 53 s | 9.20 | `41d18c4` | true | `2b8956ff…c0e9` | 7, `d4fd784b…801f`, 09:03:59 |
| `one-loop-drain-06-resumed-c-20261107T090440` | `resumed-c` | resumed | 95407 | `done` | 0 | 09:04:38 → 09:07:15 | 2 min 37 s | 12.92 | `41d18c4` | true | `2b8956ff…c0e9` | 7, `7c7641d2…dd04`, 09:06:50 |

- **The manifests** ([`06-<slug>.manifest.json`](one-loop-drain/rehearse-06/out/)): every base run records `git_head`
  `1561b60fdf9d…` and `script_sha256` `92117838…acc0`, the SHA-256 of `git show 1561b60:main.py`; every tip run `41d18c4` and
  `2b8956ff…c0e9`, `main.py`'s at the tip. `git_dirty` is `true` throughout: in the worktree from the two links, in the main
  checkout from this prompt's untracked scripts under `.documents/one-loop-drain/rehearse-06/` (no tracked file differed;
  `git status --porcelain` showed that directory alone). `results_store_id` is `null` for every run: each store's sidecar was
  created after its last run (as in 11's reference store), so each run's `fingerprint_sidecar` says "not written … (there is none)".
- **The stores** ([`06-*-store.sidecar.json`](one-loop-drain/rehearse-06/out/)): `store create` from the worktree's root
  (ruling 1) after `base-c`, `store_id` `2f6e8a7c74e54965865a781e8430f66c`, its history recording `git_head` `1561b60` (dirty);
  `store fingerprint` from the worktree at that time: **format 6**, digest `ff38053c…f4a7`, equal to `base-c`'s at finish
  ([`06-fingerprint-base.txt`](one-loop-drain/rehearse-06/out/06-fingerprint-base.txt)). From the main checkout: reference
  `2b6fd51c22ad445abaf2cf3e5573582a` after `reference-c`, resumed `23db39c668b4486cba95c7006cdffa65` after `resumed-c`.
- **The stage times** (each log's `ALL WORK ITEMS COMPLETE`, scalar / adiabatic / BBN):

  | run | α or A: scalar / adiabatic / BBN |
  |---|---|
  | `base-a` | α 0.01: 1m 40s / 1.21 s / 16.9 s; α 0.1: 24.4 s / 1.16 s / 16.2 s |
  | `base-b` | α 0: 2m 39.6s / 0.959 s / 19.1 s; α 0.1: 44.4 s / 0.87 s / 16.2 s |
  | `base-c` | α 0.15: 9.68 s / 0.406 s / 8.87 s |
  | `reference-a` | α 0.01: 1m 42.6s / 1.38 s / 19.2 s; α 0.1: 25.4 s / 1.17 s / 16.1 s; A 0.1: 1m 11.2s / 1.27 s / 16.4 s; A 1: 15.8 s / 1.27 s / 16.5 s |
  | `reference-b` | α 0: 2m 36.7s / 0.933 s / 18.6 s; α 0.1: 45.2 s / 1.1 s / 16.2 s |
  | `reference-c` | α 0.15: 10.3 s / 0.443 s / 9.16 s; A 1: 1m 29.9s / 0.493 s / 8.47 s |
  | `resumed-a` | α 0.01: 1m 41.5s / 1.3 s / 16.9 s; α 0.1: 24.3 s / 1.12 s / 16 s; A 0.1: interrupted |
  | `resumed-a2` | α 0.01 and 0.1: nothing to compute (0.02–0.2 s); A 0.1: 1m 1.14s / 1.79 s / 17.2 s; A 1: 16.2 s / 1.17 s / 16.8 s |
  | `resumed-b` | α 0: 2m 42.6s / 1.16 s / 20.5 s; α 0.1: 47.8 s / 1.26 s / 17.6 s |
  | `resumed-c` | α 0.15: 10.7 s / 0.503 s / 9.91 s; A 1: 1m 21.4s / 0.447 s / 8.17 s |

  **The builds' walls**: base 9 min 18 s (three runs), reference 13 min 7 s, resumed 14 min 33 s (four runs); the whole
  prompt's Ray work, from the cluster's start to its stop, 08:25–09:15. No log holds a `Traceback`; every stage prints `0 models
  skipped`. Every log carries the raylet's `file_system_monitor` warning that `/tmp/ray-cpbh` is "over 95% full" (44.6 → 39.3 GB
  free of 926 GB); no object creation failed (§6.8).
- **The silences** ([`06-silences.txt`](one-loop-drain/rehearse-06/out/06-silences.txt)): the longest heartbeat silence was
  **160 s** (`resumed-b`, the α = 0 scalar stage; `base-b` 157 s, `reference-b` 155 s; the others 24–90 s), against the 900 s window.

### 6.3 The interruption (prompt §1.4; ruling 7)

- **The window** ([`06-delay.txt`](one-loop-drain/rehearse-06/out/06-delay.txt)). `reference-a`'s recorder saw `>> RUNNING PIPELINE
  FOR MODEL QCD_Cosmology, A = 0.1 (one-loop-measured-v1)` at 08:40:18.930 UTC. The beats hold no line per stored history (the
  heartbeat changes only at stage banners, and the other lines of that stage are the raylet's disk warnings), so the opening
  was read from the reference store's `ScalarHistory.timestamp`s, as `inelastic-reflection` 11 read its window: β = 1.06 at
  +11.280 s (**the opening**), β = 2 at +11.676 s, β = 1.185 at +12.606 s, β = 10 at +69.058 s. **The close**: the A = 0.1 scalar
  stage's `-- ALL WORK ITEMS COMPLETE in time 1m 11.2s` line at 08:41:29.978, +71.048 s. The window is 59.8 s long, its midpoint
  41.164 s: **`--after 41`**. Load at `reference-a`'s launch 13.87, at `resumed-a`'s 10.20.
- **The signal** ([`06-resumed-a.interrupt.log`](one-loop-drain/rehearse-06/out/06-resumed-a.interrupt.log)): banner seen at
  08:55:05 (the recorder: 08:55:05.211), `sent SIGTERM to pid 90059` at 08:55:46, +40.8 s, **inside the window**. The wrapper:
  `** SIGTERM at 2026-11-07 08:55:47 +0000: recorded killed, exit code 143`; `main.py ended with SystemExit(1)`; `** launcher saw
  exit status 143`. One attempt; no miss.
- **What was stored** ([`06-histories-resumed-after-a.txt`](one-loop-drain/rehearse-06/out/06-histories-resumed-after-a.txt)):
  the eight fixed histories of run a; **A = 0.1 β = 1.06, 2 and 1.185, each `validated = 1`**, stored 08:55:16.4–08:55:17.8, +11.2
  to +12.6 s after the banner; β = 10 not stored. `ScalarHistory` 11, `ScalarHistoryBounce` 629 (231 + 247 + 151), `AdiabaticHistory`
  8, `BBNData` 8 (the fixed rows').
- **Ray after it** ([`06-ray-state.txt`](one-loop-drain/rehearse-06/out/06-ray-state.txt)): every job `SUCCEEDED` (Ray's record of a
  `SIGTERM`ed driver), workers 159 recorded and 0 alive, actors 122 and 0 `ALIVE`; `ray_cluster.sh status` exit 0, the lock HELD.
- **The resume** (`resumed-a2`, `--prune-unvalidated`): no prune report (no row was unvalidated). At α = 0.01 and 0.1 every stage
  had 0 lookups; at A = 0.1 the scalar stage looked up, computed, stored and validated **one** history (β = 10), and the adiabatic
  and BBN stages all four; at A = 1, all four, as a fresh build ([`06-stage-counts.txt`](one-loop-drain/rehearse-06/out/06-stage-counts.txt)).
  `resumed-a2`'s fingerprint at finish, `b1bd5716…7f20`, equals `reference-a`'s.

### 6.4 The gate Bitwise (prompt §2.1; P53)

`compare_base.py` on the base and reference stores ([`06-compare-base.txt`](one-loop-drain/rehearse-06/out/06-compare-base.txt)),
exit 0, **`RESULT: every compared value bitwise equal, new columns as 03 sets them`**, `DIFFERENCES: none`.
- **Matched:** the base's 17 histories, each once in the tip store under `drain_model` = `fixed`, by (M, β, φ*, α) through
  `ExponentialModel`, `M_value`, `beta_value`, `phi_value` and `alpha_value`, all by `float.hex`; and the tip's 17 fixed histories
  each once. Samples by ln(1 + z) through each shard's `redshift`: every sample matched, none on one side only, no duplicate
  (4 620 to 9 780 per history; 101 335 in all).
- **Compared, bitwise:** `ScalarHistory`'s 55 shared columns (the type ids, `failure`, `failure_reason`, the tuning's outputs, the
  first bounce, the fixed-temperature values, the 24 dissipation columns, `RHS_evaluations`, `compute_steps`, `z_samples`,
  `validated`, …) and `extra_data` key by key (5 keys each, none on one side only); `ScalarHistoryValue`'s 19 value columns;
  `AdiabaticHistory`'s 6 and `AdiabaticHistoryValue`'s 5; `BBNData`'s 10 (`failure_reason` and `PRyM_version` included) and
  `BBNDataValue`'s 4; the key rows each history references (`QCD_Cosmology` 7 columns, `ExponentialModel.n_value`, `M_value`,
  `beta_value`, the three `tolerance` rows, `alpha_value.value`, `phi_value`, `pi_value`, `temperature`, `IntegrationSolver`'s
  `stepping` and `label`).
- **Left out, by name:** `serial` and every `*_serial` column, `version`, `label` (of the history tables), `timestamp`,
  `compute_time`, `mean_RHS_time`, `max_RHS_time`, `min_RHS_time`, `NP_compute_time`, `BBN_compute_time`.
- **The count:** `ScalarHistory` 935, `extra_data` 107, `ScalarHistoryValue` 1 925 365, `AdiabaticHistory` 102,
  `AdiabaticHistoryValue` 506 675, `BBNData` 170, `BBNDataValue` 90 952, key tables 323 (`QCD_Cosmology` 119, `tolerance` 51,
  `IntegrationSolver` 34, the others 17 each): **2 524 629 values, every one equal.**
- **The tip's new columns** at the 17: `alpha_value.drain_model` = `fixed` 17 of 17; the drain record's 23 columns (`bounce_rows`
  included) NULL at 17 of 17; no `ScalarHistoryBounce` row at 17 of 17. **The sample masses**: of 101 335 samples, `log_m_eff_Mp`
  and `log_m_tracker_Mp` are filled at every one, NULL at none. Checked by recomputing each sample's pair with
  `ComputeTargets.ScalarHistory.sample_masses` on the `ExponentialModel` realized at the stored normalization and φ_ref in
  `Planck_units`, from the sample's columns read back as `ObjectFactories` reads them: no NULL where it gives a value, no value
  where it gives None, and all 202 670 filled values equal to the recomputation bit for bit.
- **The exceptions** (README §8.14): **none**. No history failed at either commit. The two BBN failures (M = 10⁻³ M_P, β = 0.5,
  α = 0 and 0.1: `T_in_MeV=0.00019824 MeV` and `0.00019825 MeV is smaller than T_BBN_spline_min=0.0002 MeV`) are rows of
  `BBNData` with equal reasons at both, and compared as such.
- An early run of the script on the same two stores at 08:51, after B1 and before B2, gave the same output byte for byte (its
  first two attempts failed on two bugs of the script, fixed then: §6.8's version is the fixed one; log 06 §2).

### 6.5 The gate R (prompt §2.2; P54; rulings 2, 3)

**(i) The stores agree.** `tools/compare_stores.py` on the reference and resumed stores
([`06-compare-stores.txt`](one-loop-drain/rehearse-06/out/06-compare-stores.txt)), exit 0, **`RESULT: equal`**, `problems: none`.
Every table matched with 0 rows on either side only, **`ScalarHistoryBounce` 2 562 among them**; `ScalarHistory` 26,
`ScalarHistoryValue` 156 508, `AdiabaticHistoryValue` 156 508, `BBNDataValue` 36 571, `alpha_value` 6, `redshift` 75 001. The
bookkeeping columns that differ: timestamps, labels and the compute times only. **`store fingerprint`** on each closed store
([`06-fingerprint-reference.txt`](one-loop-drain/rehearse-06/out/06-fingerprint-reference.txt),
[`06-fingerprint-resumed.txt`](one-loop-drain/rehearse-06/out/06-fingerprint-resumed.txt)): both **`7c7641d2aade2d926319113781d8515ea8488b826d2f4b488b3c48105636dd04`**,
**format 7**, `problems: none`, every class's count and digest identical.

**(ii) Against 02d's tool route.** `compare_02d.py` on each store ([`06-compare-02d-reference.txt`](one-loop-drain/rehearse-06/out/06-compare-02d-reference.txt),
[`06-compare-02d-resumed.txt`](one-loop-drain/rehearse-06/out/06-compare-02d-resumed.txt)), exit 0, **`RESULT: every bitwise item equal;
Yp and D/H within 5e-10`**, `DIFFERENCES: none`, on both:
- the nine histories matched once each, failure 0, validated 1;
- **the tuning's fifteen keys**, through the columns' units: 135 values equal;
- **every field of `accepted.drain_values`** against the stored drain record (13 per history): 117 equal;
- **the candidate rows** by `candidate_index`, ruling 2: their number equal to the partial line's `candidates` and `injections`
  in each history, indices 0, 1, …; 1 285 candidate rows × 11 values (the `DrainCandidate`'s eight through `to_column` in
  `Planck_units`, and the injection's N, α_eff, ΔK) = **14 135 values equal**; all 14 135 also equal the raw values bit for bit
  (a measurement). The event rows, counted and not compared: 1 277 `plateau` rows, no `wall` row;

  | task | candidates (regime) | plateau rows | bounce rows |
  |---|---|---|---|
  | P-2188-A0.1 | 116 (one-loop) | 115 | 231 |
  | P-2188-A1 | 17 (one-loop) | 16 | 33 |
  | P-2197-A0.1 | 124 (one-loop) | 123 | 247 |
  | P-2197-A1 | 5 (one-loop) | 4 | 9 |
  | R-865-A0.1 | 76 (one-loop) | 75 | 151 |
  | R-865-A1 | 45 (one-loop) | 44 | 89 |
  | R-611-A0.1 | 536 (one-loop) | 535 | 1 071 |
  | R-611-A1 | 65 (one-loop) | 64 | 129 |
  | C-3233-A1 | 301 (classical) | 301 | 602 |

- **the 24 dissipation values** (ΔN_eff today and at 1 MeV among them): 216 values equal, bit for bit (ruling 3);
- **Y_p and D/H** at full precision: **equal in all nine, relative difference 0** in both stores; the 5 × 10⁻¹⁰ allowance was not
  used.

**(iii) The exports.** `compare_exports.py` ([`06-compare-exports.txt`](one-loop-drain/rehearse-06/out/06-compare-exports.txt)) on
`plot-reference/QCD_Cosmology` and `plot-resumed/QCD_Cosmology`, after R6, exit 0: 101 files in each, none on one side only;
**93 byte-identical** (histories.csv, 26 fields.csv, 9 bounces.csv, and the tool's 9 + 9 + 9 per-history files and 3
quanta_histories at each of the three scales); **8 differ only in timing columns** (plot_by_beta's per-(row, M) `data.csv`:
`scalar_compute_time`, `BBN_compute_time`, `NP_compute_time`); 0 differ otherwise.
- **The nine bounces.csv against 05's export** of the same 02d tasks under `var/one-loop-drain/05/exports/QCD_Cosmology/`
  ([`06-compare-bounces-05.txt`](one-loop-drain/rehearse-06/out/06-compare-bounces-05.txt)): **9 of 9 byte-identical.**
- **quanta_histories.csv against 05's rows** (a measurement; [`06-compare-quanta-histories-05.txt`](one-loop-drain/rehearse-06/out/06-compare-quanta-histories-05.txt)):
  at c_a = 1, 0 and 10, all 27 rows equal to 05's, cell for cell. 05's fields.csv had no adiabatic or BBN columns behind it, but
  the tool reads neither: E and the K check use the sample masses, `raw_N` and `rho_chi_over_rho_rad`, which are the same.

### 6.6 The readers (prompt §1.5; P56; rulings 4, 5)

Each unregistered, from the main checkout, with `--config exponential.yaml --database <stem>.db --shards 16`, the plot scripts
with `--ray-address 127.0.0.1:6380`. `shasum -a 256` of the store's 17 files and its sidecar before and after each: **unchanged
every time** ([`06-shasums.txt`](one-loop-drain/rehearse-06/out/06-shasums.txt), [`06-readers.txt`](one-loop-drain/rehearse-06/out/06-readers.txt)).

| reader | store | exit | wall | load at start |
|---|---|---|---|---|
| R1 `main.py --inventory` | reference | 0 | 26.3 s | 11.30 |
| R2 `plot_by_beta.py` | reference | 0 | 33.4 s | 10.94 |
| R3 `plot_ScalarHistory.py` | reference | 0 | 45.2 s | 17.81 |
| R4 `plot_by_beta.py --A-values 0.5` | reference | **2** | 11.1 s | 16.54 |
| R5 `plot_by_beta.py` | resumed | 0 | 30.9 s | 17.69 |
| R5 `plot_ScalarHistory.py` | resumed | 0 | 40.6 s | 17.57 |
| R6 `quanta_measures.py`, ×6 | the two exports | 0 | 3.4–3.6 s | 9.25–11.74 |
| R7 `bbn_from_store.py`, fixed α = 0.1 | base | 0 | 12.4 s | 19.15 |
| R7 the same | reference | 0 | 11.1 s | 26.41 |
| R7 `--drain-law one_loop --alpha 1` | reference | 0 | 12.3 s | 33.24 |

- **R1** ([`06-R1-inventory.log`](one-loop-drain/rehearse-06/out/06-R1-inventory.log)): `@@ alpha_value (replicated): 6 records`,
  `values = [ 0, 0.01, 0.1, 0.15, A=0.1 (one-loop-measured-v1), A=1 (one-loop-measured-v1) ]`; each one-loop history's line ends
  `A=<A> (one-loop-measured-v1)`; `ScalarHistory (sharded): 26 records: 26 validated, 0 unvalidated`. 16 classes are listed;
  `ScalarHistoryBounce` has no inventory spec (03).
- **R2 and R3** (`plot-reference/QCD_Cosmology/`): `alpha=0/`, `alpha=0.01/`, `alpha=0.1/`, `alpha=0.15/` and
  `one-loop-measured-v1/A=0.1/`, `one-loop-measured-v1/A=1/`, each with `csv/` and `plots/`; at the label's level histories.csv
  and `plots/M=<M>eV_n=1/Delta_N_eff.pdf` for each of the three M. **histories.csv** ([`06-histories.csv`](one-loop-drain/rehearse-06/out/06-histories.csv))
  holds **54 rows**, in P36's sort (`drain_model`, α, M, n, β): the **26 stored histories**, and **28 rows marked `no ScalarHistory
  row in the store`**: the plot script writes a row for every (row, M, β) of the stored grid, and the three runs' grids are not
  a rectangle (§6.8). **26 fields.csv**; **9 bounces.csv**, one under each one-loop history's directory, **none under `alpha=*`**.
- **R4** ([`06-R4-plot-unstored.log`](one-loop-drain/rehearse-06/out/06-R4-plot-unstored.log)): exit 2, `!! plot: the store holds no
  alpha_value for --A-values 0.5 (one-loop-measured-v1); main.py was not run with it, so no history in this store can match. Give
  the plot the options the build used`; `var/one-loop-drain/06/plot-unstored` does not exist afterwards.
- **R6** (05's log §7's commands; [`06-R6-*.log`](one-loop-drain/rehearse-06/out/)): at each scale and on each export, `measured 9,
  skipped as fixed-law 36, failed histories 9, missing files 0, refused 0, failed the K check 0`; the nine "failed" are the
  one-loop grid rows with no stored history (R2's 28, of which 9 are one-loop). K bitwise at every candidate but the leftover,
  the K check within 2.84 × 10⁻¹⁴. δ_stim at c_a = 1: 4.1 × 10⁻⁵⁸ (R-611, A = 1) to 8.0 × 10⁻⁵ (C-3233, A = 1). My first four
  calls at c_a = 0 and 10 passed `--assoc-scale <c>` as one shell word and were refused (exit 2, usage); they were re-run as two
  words (the readers' record says so).
- **R7** (ruling 4: `--small-network --variant prod --phi-init-Mp 5 --beta 1.185 --M-Mp 4e-31`, from the main checkout):

  | store | law | `stored:` Y_p, D/H | `prod` re-solve |
  |---|---|---|---|
  | base (format 6) | fixed, α = 0.1 | 0.25595621877075797, 2.4855627505745383 | 0.2559562188, 2.485562751 |
  | reference | fixed, α = 0.1 | 0.25595621877075797, 2.4855627505745383 | 0.2559562188, 2.485562751 |
  | reference | one-loop, A = 1 | 0.25692050931671995, 2.534332574064248 | 0.2569205093, 2.534332574 |

  Each re-solve equals the stored value rounded to the ten significant digits it prints. The base store, with no `drain_model`
  column, was read as `fixed` (P39). My first three R7 calls were malformed by a shell loop (an empty store name; exit 2 at
  argument parsing, nothing opened) and were re-run one by one.
- **The cluster's stop**, after every driver's pid answered `no such process`, every recorder had written its exit line and
  `interrupt_when.py` its `sent` line: Ray state, 18 jobs `SUCCEEDED`, workers 336 recorded and 0 alive, actors 253 and 0 `ALIVE`.
  `ray_cluster.sh stop`, exit 0: `grace period of 15s over; SIGKILL to: 67063 67064`, `no process names /tmp/ray-cpbh; ports 6380
  6381 8267 52367 are free`, `released the rehearsal lock`, `stopped in 19s`. Then `status`, exit 0: `lock: free`, no Ray process,
  the four ports free; `raypoolkit.rehearsal_lock status`, exit 0, `lock: free`. `ray stop` was never run.

### 6.7 The figures, the costs, the suites

- **The figures**, looked at as PNG (none committed). `plot-reference/QCD_Cosmology/plots/M=0.00097413eV_n=1/Delta_N_eff.png`: two
  panels, ΔN_eff(T_CMB) above and ΔN_eff(1 MeV) below against β on log axes, one curve per (model, value) with value > 0
  (P36): α = 0.01, α = 0.1, A = 0.1 and A = 1 (one-loop-measured-v1), and a legend entry for α = 0.15 with no point at this M. A = 1
  lies just below α = 0.1 (0.060 against 0.071 at β = 1.185 today) and A = 0.1 just below α = 0.01; β = 1.06's zero at 1 MeV is not
  drawn on the log axis. `one-loop-measured-v1/A=1/plots/beta=1.185/M=0.00097413eV_n=1/dark_radiation.png` (P-2197 at A = 1):
  Δg* and ρ_χ/ρ_R against N from the first impact at N ≈ 17.6, a step near N = 20 and three small steps through 25–26, the BBN
  window shaded, the parking at N = 29.04 marked, and ρ_χ/ρ_R flat at 8.07 × 10⁻³ after it; the footer reads "Reflections (elastic
  model): 5".
- **The costs**, for 08's handover: the builds' walls above; per history (the store's `timestamp` less the time in its label, so the
  compute task's queueing included; [`06-histories-reference.txt`](one-loop-drain/rehearse-06/out/06-histories-reference.txt)):
  fixed-law tunings 1–5 integrations, 6.7–128 s (β = 5.5 at M = 10⁻³ the longest); one-loop 1 integration at laboratory M, 4.2–59 s
  (R-611 at A = 0.1 the longest, 536 drains), C-3233 at A = 1 3 integrations, 89 s. Bounce rows per one-loop history 9–1 071, **2 562
  in all**. On disk: base 117 344 KiB, reference 137 352 KiB, resumed 137 868 KiB (17 files each).
- **The suites**, serially, never with the cluster up ([`06-suites.txt`](one-loop-drain/rehearse-06/out/06-suites.txt); `unittest`'s
  time, the one-minute load before the root): before (08:06–08:24), CosmologyModels 26 OK (68.5 s; 8.68), ComputeTargets
  `test_[a-f]*` 219 OK (144.1 s; 6.82), `test_[g-z]*` 446 OK (487.9 s; 6.35), ObjectFactories 163 OK (133.7 s; 40.99), tools 191 OK
  (211.5 s; 18.57), RunDrivers 72 OK (23.3 s; 12.40); after (09:17–09:39, other work loading the machine to 107–200), the same
  counts, all OK: 76.9 s, 162.9 s, 545.8 s, 159.1 s, 273.4 s, 33.3 s.
- **The token**: `grep -r -F -l -f ~/.ray/auth_token` over every file this prompt commits finds nothing (exit 1).
- **The registry** after: 33 runs, all finished; this prompt's ten `done` but `resumed-a` `killed`. No pid recorded under
  `var/one-loop-drain/06/` answers `kill -0`.

### 6.8 The commands, verbatim

From the main checkout's root unless stated. `<R>` is `.documents/one-loop-drain/rehearse-06`, `<L>` `var/one-loop-drain/06/runs`,
`<W>` the worktree `<scratch>/base-1561b60`, `<scratch>` the session scratchpad's `agent-06`, `ADDRESS=127.0.0.1:6380`,
`S=<main checkout>/var/datastores`. Each suite, launch, wait, start, stop and status was its own Bash call.

```bash
# the Gate
git status --porcelain; git worktree list; tools/ray_cluster.sh status; ./venv/bin/python -m raypoolkit.rehearsal_lock status
./venv/bin/python -m runregistrykit list
# the suites, before and after (ComputeTargets in halves; the g-z half and tools in the background)
SUITES=<scratch>/suites sh <R>/suite.sh before CosmologyModels CosmologyModels/tests          # and the four others, as CLAUDE.md
# the base tree
git worktree add --detach <W> 1561b60; cd <W> && ln -s <main checkout>/venv venv && ln -s <main checkout>/var var
# the cluster (alone; PYTHONPATH unset)
tools/ray_cluster.sh start "one-loop-drain 06 (rehearsal)" > var/one-loop-drain/06/start.txt 2>&1
# the probe, from each tree's root
PYTHONPATH=. ./venv/bin/python -u <main checkout>/<R>/code_probe.py > <scratch>/probe-<tree>-<when>.txt 2>&1
# a base run (from <W>; b and c alike, with prompt §1.3's grid)
echo "base-a launch: $(date '+%F %T %z') $(uptime)" | tee -a var/one-loop-drain/06/06-load.txt
cd <W> && export ADDRESS S L=<main checkout>/<L> && nohup sh -c 'PYTHONPATH=. ./venv/bin/python -u RunDrivers/registered_run.py --campaign one-loop-drain --prompt 06 --register base-a --purpose "one-loop-drain 06 Bitwise: run a of the base build at 1561b60 (beta 1.06 1.185 2 10 at M 4e-31 Mp, phi* 5, alpha 0.01 0.1)" -- --config exponential.yaml --database $S/one-loop-drain-06-base.db --shards 16 --job-name one-loop-drain-06-base-a --ray-address $ADDRESS --M-values-Mp 4e-31 --beta-values 1.06 1.185 2 10 --phi-init-Mp 5 --alpha-values 0.01 0.1 > $L/base-a.log 2>&1 & echo $! > $L/base-a.pid; wait $!; echo "** launcher saw exit status $?" >> $L/base-a.log' > /dev/null 2>&1 &
nohup ./venv/bin/python -u $PWD/<R>/beat_times.py $L/base-a.log $L/base-a.beats > $L/base-a.beats.err 2>&1 &
./venv/bin/python <R>/wait_for_line.py <L>/base-a.log "** registered_run: registered as" --timeout 120 && sh <R>/check.sh base-a
./venv/bin/python <R>/wait_for_line.py <L>/base-a.log "** launcher saw exit status" --timeout 3600 --poll 5      # in the background
cd <W> && PYTHONPATH=. ./venv/bin/python -m runregistrykit store create var/datastores/one-loop-drain-06-base.db --purpose "one-loop-drain 06 Bitwise: the base build at 1561b60 (runs a, b, c: 17 fixed-law histories), kept for the user"
cd <W> && PYTHONPATH=. ./venv/bin/python -m runregistrykit store fingerprint var/datastores/one-loop-drain-06-base.db
# a tip run (reference-*, resumed-*): the same from the main checkout, with the tip's grid (--A-values 0.1 1 on a, --A-values 1 on c);
# reference's store create after reference-c, resumed's after resumed-c, from the main checkout
# the interruption, beside resumed-a (after its registration line)
nohup ./venv/bin/python .documents/dk-and-rpk-migration/rehearsal/interrupt_when.py $L/resumed-a.log $L/resumed-a.pid --banner "RUNNING PIPELINE FOR MODEL QCD_Cosmology, A = 0.1 (one-loop-measured-v1)" --after 41 > $L/resumed-a.interrupt.log 2>&1 &
RAY_AUTH_MODE=token ./venv/bin/python <R>/ray_state.py; tools/ray_cluster.sh status
./venv/bin/python <R>/histories.py var/datastores/one-loop-drain-06-resumed > var/one-loop-drain/06/histories-resumed-after-a.txt
# then resumed-a2: run a's arguments into …-resumed.db with --prune-unvalidated; then resumed-b, resumed-c
# the readers (each through a wrapper that takes shasum -a 256 of <stem>*.db and <stem>.manifest.json before and after)
PYTHONPATH=. ./venv/bin/python -u main.py --config exponential.yaml --database $PWD/var/datastores/one-loop-drain-06-reference.db --shards 16 --inventory
PYTHONPATH=. ./venv/bin/python -u plot_by_beta.py --config exponential.yaml --database $PWD/var/datastores/one-loop-drain-06-reference.db --shards 16 --phi-init-Mp 5 --alpha-values 0 0.01 0.1 0.15 --A-values 0.1 1 --output $PWD/var/one-loop-drain/06/plot-reference --ray-address 127.0.0.1:6380
PYTHONPATH=. ./venv/bin/python -u plot_ScalarHistory.py --config exponential.yaml --database $PWD/var/datastores/one-loop-drain-06-reference.db --shards 16 --phi-init-Mp 5 --alpha-values 0 0.01 0.1 0.15 --A-values 0.1 1 --output $PWD/var/one-loop-drain/06/plot-reference --ray-address 127.0.0.1:6380
PYTHONPATH=. ./venv/bin/python -u plot_by_beta.py --config exponential.yaml --database $PWD/var/datastores/one-loop-drain-06-reference.db --shards 16 --phi-init-Mp 5 --A-values 0.5 --output $PWD/var/one-loop-drain/06/plot-unstored --ray-address 127.0.0.1:6380
# R5: R2 and R3 on …-resumed.db with --output $PWD/var/one-loop-drain/06/plot-resumed
RAY_AUTH_MODE=token ./venv/bin/python <R>/ray_state.py; kill -0 <each pid in <L>/*.pid>
# the stop, then status and the lock (each alone)
tools/ray_cluster.sh stop; tools/ray_cluster.sh status; ./venv/bin/python -m raypoolkit.rehearsal_lock status
# R6, on plot-reference and plot-resumed
PYTHONPATH=. ./venv/bin/python tools/quanta_measures.py var/one-loop-drain/06/plot-<store>/QCD_Cosmology [--assoc-scale 0 | --assoc-scale 10]
# R7
PYTHONPATH=. ./venv/bin/python -u tools/bbn_from_store.py var/datastores/one-loop-drain-06-base --beta 1.185 --M-Mp 4e-31 --phi-init-Mp 5 --drain-law fixed --alpha 0.1 --small-network --variant prod
PYTHONPATH=. ./venv/bin/python -u tools/bbn_from_store.py var/datastores/one-loop-drain-06-reference --beta 1.185 --M-Mp 4e-31 --phi-init-Mp 5 --drain-law fixed --alpha 0.1 --small-network --variant prod
PYTHONPATH=. ./venv/bin/python -u tools/bbn_from_store.py var/datastores/one-loop-drain-06-reference --beta 1.185 --M-Mp 4e-31 --phi-init-Mp 5 --drain-law one_loop --alpha 1 --small-network --variant prod
# the comparisons
PYTHONPATH=. ./venv/bin/python <R>/compare_base.py var/datastores/one-loop-drain-06-base var/datastores/one-loop-drain-06-reference
PYTHONPATH=. ./venv/bin/python tools/compare_stores.py var/datastores/one-loop-drain-06-reference.db var/datastores/one-loop-drain-06-resumed.db
./venv/bin/python -m runregistrykit store fingerprint var/datastores/one-loop-drain-06-<reference|resumed>.db
PYTHONPATH=. ./venv/bin/python <R>/compare_02d.py var/datastores/one-loop-drain-06-<reference|resumed>
./venv/bin/python <R>/compare_exports.py var/one-loop-drain/06/plot-reference/QCD_Cosmology var/one-loop-drain/06/plot-resumed/QCD_Cosmology
./venv/bin/python <R>/compare_exports.py var/one-loop-drain/06/plot-reference/QCD_Cosmology var/one-loop-drain/05/exports/QCD_Cosmology --subset --names bounces.csv
./venv/bin/python <R>/compare_exports.py var/one-loop-drain/06/plot-reference/QCD_Cosmology var/one-loop-drain/05/exports/QCD_Cosmology --key-rows quanta_histories.csv   # and -assoc0, -assoc10
./venv/bin/python <R>/histories.py var/datastores/one-loop-drain-06-<base|reference|resumed>
./venv/bin/python .documents/dk-and-rpk-migration/rehearsal/stage_counts.py <L>/<each slug>.log
./venv/bin/python <R>/silences.py <L>/*.beats
# the worktree's removal (the two links unlinked first, so no --force)
rm <W>/var <W>/venv; git worktree remove <W>; git worktree prune; git worktree list
# the token
grep -r -F -l -f ~/.ray/auth_token <each committed path>
```

### 6.9 Observed, not acted on

- **histories.csv's grid rows.** `plot_by_beta.py` writes a row for every (row, M, β) of the stored grid (`plot_by_beta.py:882`,
  since `abccc23`), marked `no ScalarHistory row in the store` where nothing is stored. The three runs' grids are not a rectangle,
  so histories.csv holds 54 rows, 28 of them such markers, where prompt §3 expected 26; the 26 stored histories are there, in
  P36's sort. The tool counts the 9 one-loop markers as "failed histories". `Delta_N_eff.pdf` draws a legend entry for α = 0.15 at
  laboratory M, where it has no point. Earlier rehearsals' grids were rectangles, so this had not shown.
- **The shard warning.** Each plot script prints `!! WARNING: number of shards read from database (=16) does not match specified
  number of shards (=10)` though given `--shards 16` (`datastorekit`'s `ShardedPool`); `inelastic-reflection` 11's plot logs carry it
  too. The store opens read-only with its 16 shards.
- **"Reflections (elastic model)"** in the footer of a one-loop history's figures (the dark-radiation figure of P-2197 at A = 1).
- **The disk.** Ray's raylet warned every 10 s that the volume holding `/tmp/ray-cpbh` was over 95% full (44.6 → 39.3 GB free of
  926 GB during the prompt). Nothing failed; a longer build (the rebuild of record) may meet Ray's object-creation limit on this
  machine if the free space falls further.
- **The probe's findings** (§6.1): workers run in the cluster's working directory, the main checkout, whatever tree the driver
  is in. Harmless here (the data files are identical at both commits), but a worktree run on a shared cluster reads its relative
  data paths from the cluster's start directory, not its own.

## 7. Prompt 07 — the figure tools (2026-11-07)

**Tree:** branch `one-loop-drain` at `d4031dc` (orchestrator note 07's commit, on `5bb6c40`, which adds the prompt); the code is
06's reviewed code, `VERSION_LABEL` 2026.10.0, `FINGERPRINT_FORMAT` 7, plus the new tools. §1–§6 above are unchanged. No value,
schema, export or measure changed. **Scripts and outputs:** [`one-loop-drain/figures-07/`](one-loop-drain/figures-07/):
`pick_histories.py`, `run_demo.sh`, `check_figures.py`, `copy_pngs.py`, the 12 committed PNGs and `out/` (each run's printed
output, with its exit and wall; `picks.txt`; `check_figures.txt`). **Raw output** (not committed): `var/one-loop-drain/07/`.

### 7.1 What was added

`tools/quanta_figures.py` (the shared module: regime colours, ruling 1's classes, the model check, the `--output` mapping),
`tools/plot_quanta_grid.py` (ten heat maps per one-loop A) and `tools/plot_quanta_history.py` (six panels sharing N per history);
both read 04's exports and 05's outputs only. `extract_common.add_ScalarHistory_labels` says "Reflections: N" (P64). Log:
[`07-figure-tools.md`](../prompts/one-loop-drain/logs/07-figure-tools.md).

### 7.2 The gate Figures (prompt §2), item by item

1. **Tests.** 68 added to `tools/tests` (`test_quanta_figures` 12, `test_plot_quanta_grid` 26, `test_plot_quanta_history` 30);
   `tools/tests` 191 → 259, the other suites unchanged (26, 665 = 219 + 446, 163, 72); all pass. Three manual breakages together
   fail ten of the 68 (log §4).
2. **06's exports.** Grid: two figures (A = 0.1: drawn 4; A = 1: drawn 5); history: nine figures, every one-loop history of that
   tree; both exit 0 (`out/grid-06.txt`, `out/history-06.txt`).
3. **05's exports.** Grid at the eight A (drawn 27, 28, 29, 30, 29, 29, 29, 29) and at A = 1 for c_a = 0 and 10 (30 each); history on
   four histories; all exit 0.
4. **Counts against the files, by script** (`check_figures.py`; `out/check_figures.txt`): 58 checks, 0 failures. Each grid line's
   `drawn` equals the drawn rows of histories.csv at its A and, less its "no quanta row" (0), the rows of quanta_histories<suffix>.csv
   at that A; each history line's generations equal that history's `generations` in quanta_histories.csv (06's nine, and 05's
   13 342, 64, 301 and 4).

**The gate holds.**

### 7.3 The commands, times and sizes

As in the log §3.2, with the one-minute loads (3.3-4.1, a quiet machine): grid on 06's tree 2.4 s for two figures; history on
06's nine 7.8 s; grid on 05's eight 12.5 s (1.4-1.6 s a figure); the c_a = 0 and 10 pages 1.7 s each; history figures 0.8-0.9 s at
64-600 generations and 2.4 s for the 13 342-generation history. Sizes: a grid page 51-72 kB PDF, 185-232 kB PNG; a history figure
36-60 kB PDF (400 kB for 13 342 generations) and 250-293 kB PNG. 25 PDFs, 1.75 MB; the 12 committed PNGs, 2.9 MB.
The two input trees were listed with mtimes and sizes (3 194 files) before and after: identical.

### 7.4 The six picks and the degenerate ranges

Four histories, from the six columns (`out/picks.txt`): A = 0.1, β = 2, M = 5e-20 (generations 13 342, `n_bar_max` 9.0e31,
`phase_slips_below_1` 6 302); A = 5, β = 2, M = 5e-16 (`drained_share_gap` 0.99998); A = 10, β = 1, M = 1e-12
(`drained_share_classical` 1.0000000000000007, above 1 by rounding); A = 2, β = 2, M = 8e-18 (`backreaction_max` 1 785). The
degenerate colour ranges: 06's A = 0.1, panel (b) no positive value; 06's A = 1, panel (b) one value; none on 05's tree.

### 7.5 The figures, by eye

Per figure in log §3.6: 05's eight grid pages show one set of cells (a band at the laboratory mass, a column at β = 2 and a few
more) with the one-loop share 1.0 on the band, the non-one-loop shares zero there, 4 or 5 hatched `ell_min` cells at each A and
margins mostly positive; the four history figures show the generations by regime, alpha_eff and the drained fraction, K(N), the
excess, the occupations and the phase slips on one e-fold axis. Nothing reads wrongly that is the tool's: the first render's two
faults (white empty cells indistinguishable from a diverging map's zero; a footer clipped at the right edge) and a 1 rad line off
the axes were fixed before the figures were committed.

## 8. The law and the measures, for the paper (2026-11-07, prompt 08)

**Tree:** branch `one-loop-drain` at `d0d8918` (orchestrator note 08's commit, on `e951b8e`, which adds the prompt). Its code is
07's reviewed code (`867b267`, `c8b87d6`): `VERSION_LABEL` 2026.10.0 (`config/version.py:88`), `FINGERPRINT_FORMAT` 7
(`config/fingerprint.py:95`). §1–§7 above are unchanged. Nothing in this section was measured for it: every number is quoted from
the section, log or file it names, and every formula and constant from the code at the `file:line` given (lines of `d0d8918`;
`ComputeTargets/ScalarHistory.py` unless a file is named). It is written for a reader who will quote it; it edits neither the paper
nor the briefing notes. Where a number spans tens of decades it is given as the record gives it.

### 8.1 The two laws, as coded

**Where a drain happens.** A *candidate* is a drain point after a resolved wall-side turning point, or a reflection (unresolved or
at the floor), at α > 0 (`numerical-strategies.md` §6.11.1 steps 1–3). At each one the condensate's kinetic energy
ρ_kin,E = H_E² π²/2 loses the fraction α_eff: ΔK = α_eff ρ_kin,E e^{4N} enters the ledger K (ρ_χ,E = K e^{−4N}), π is scaled by
√(1 − α_eff), and Radau restarts (`candidate`, `:2323`). The two laws differ only in α_eff.

**The gate**, common to both (U9; inelastic-reflection U4 (b′)): w_gate = 𝒜_k²/(𝒜_k² + 𝒜₀²) (`gate_weight`, `:1355`–`:1371`),
𝒜₀ = `NONADIABATICITY_SCALE` = 1 (`:116`, the user's ruling of 2026-10-28), 𝒜_k = max |dm_eff/dt|/m_eff² over the half-oscillation
that ends at the candidate. At an unresolved reflection w_gate = 1 exactly, and the gate is not evaluated (`:2296` under the fixed
law, `:2357` under the one-loop law).

**The fixed law** (`DRAIN_LAW_FIXED`, model `"fixed"`): α_eff = α · w_gate (`alpha_effective`, `:2284`–`:2298`), with
0 ≤ α < 1 (`check_drain_law`, `CosmologyConcepts/drain_model.py:105`). α = 0 is natural mode: `alpha_effective` returns 0.0
before the weight is formed (`:2293`) and no candidate drains. This is the law of every store to `VERSION_LABEL` 2026.9.0,
unchanged: the gate Bitwise compared 2 524 629 values of 17 fixed-law histories built at the base and at the tip, every one equal
(§6.4).

**The one-loop law** (`DRAIN_LAW_ONE_LOOP`, model `"one-loop-measured-v1"`, `drain_model.py:48`):

    α_eff = min(α_cap, A · f) · w_gate          (drain_alpha_effective, :1872–:1894)

with α_cap = `DRAIN_ALPHA_CAP` = 0.99 (`:211`; U10, "0.99 is OK": the drain refuses α_eff ≥ 1) and A the law's strength, finite
and > 0 with no upper bound (`check_drain_law`, `drain_model.py:122`; A = 0 is natural mode, asked for as the fixed law at α = 0).
f, the fraction of the kinetic energy one bounce gives to quanta, is `drain_fraction` (`:1831`–`:1869`), by regime:

| regime | f | constants (`:211`–`:221`) |
|---|---|---|
| one-loop, ℓ ≥ 1 | d = ℓ + b₀ − c ln L; f = min(f_cap, 1 − e^{−a/d}) if d > 0, else f_cap | a = `DRAIN_ONE_LOOP_A` = 2.69, b₀ = `DRAIN_ONE_LOOP_B0` = 0.9691, c = `DRAIN_ONE_LOOP_C` = 0.41, f_cap = `DRAIN_F_CAP` = 0.95, the edge ℓ = `DRAIN_ELL_PLATEAU` = 1 |
| one-loop, ℓ < 1 | f = f_plateau (L and ln R not read) | f_plateau = `DRAIN_F_PLATEAU` = 0.9 |
| gap | f = 0 if ln R ≤ r₀ or s = 1 + q ln L ≤ 0; else f = 1 − exp[−g (ln R − r₀)^p s] | g = `DRAIN_GAP_G` = 0.0198, r₀ = `DRAIN_GAP_R0` = 1.5601, p = `DRAIN_GAP_P` = 1.5472, q = `DRAIN_GAP_Q` = 0.841 |
| classical | f = R · S(L) = e^{ln R} S(L) | S(L), below |
| unassessed | f = 0; α_eff = 0.0 exactly, L and ln R not read (`:1888`–`:1889`) | — |

The two clauses of the gap row that end in 0 are the threshold of the fit (R ≤ e^{1.5601} = 4.76) and the user's ruling of
2026-11-05 on the region L ≤ e^{−1/0.841} = 0.3045, where the fitted expression is negative: "I think probably we set `f = 0`
there" (README §8.10; the comment at `:209`–`:210`).

**S(L)** (`drain_S_of_L`, `:1818`–`:1828`), the classical wall's production energy ratio: the 21 values of `DRAIN_S_TABLE_L` and
`DRAIN_S_TABLE_S` (`:230`–`:275`), L = 0.3, 0.5, 0.7, 1, 2, …, 110, 134, S = 1.0041 × 10⁻² at L = 0.3 to 1.4204 × 10⁻⁵ at 134;
linear interpolation in (ln L, ln S) (`np.interp` on the table's logarithms); S(0.3) held below L = 0.3; S(134)·(134/L)² above
L = 134, since L²S tends to a constant.

**The inputs.**
- **ℓ** = ln(k_ex/m_ta) is the one-loop turnaround's: m_ta⁴ ℓ = 32π² E, V''(φ_ta) = m_ta², k_ex = λ_ta E^{1/2}/2
  (`one_loop_turnaround`, `CosmologyConcepts/Models/AbstractModel.py:730`).
- **ln R** is the assessment's classical turning point's, R = ω_t⁴/8π²E with ω_t² = V''(φ_t) and V(φ_t) − V_floor = E
  (`ClassicalTurningPoint.log_R`, `AbstractModel.py:721`).
- **L** is the model's wall variable at the assessment's ln E (`drain_law_inputs`, `:1803`–`:1815`; `RealizedModel.drain_L`,
  `AbstractModel.py:421`). The base hook `AbstractModel._drain_L` has no default and raises (`AbstractModel.py:517`–`:532`), by the
  user's ruling of 2026-11-05 ("I suggest we move `L` into the model class, with no default"). Under the one-loop law a model whose
  class does not define it is refused before any integration (`check_drain_law_model`, `:1755`–`:1770`). `ExponentialModel` defines
  L = ln(1 + E/V₀), the wall exponent (M/φ_t)^n at every n (`CosmologyConcepts/Models/ExponentialModel.py:81`–`:91`), computed as
  x + log1p(e^{−x}) for x = ln E − ln V₀ > 0 and log1p(e^x) otherwise. At n = 1 it is M/φ_t, the variable the law was measured on
  (U15 (b), ruled 2026-11-05: "OK, let's take option (b) for now"); the two differ there by at most 3.3 × 10⁻¹⁴ relative (§2c;
  README §8.10).

**The regimes** (`drain_regime`, `:1773`–`:1800`), from the assessment the candidate is judged on, computed once in the loop (P18):
- **unassessed:** no assessment (no wall, or every assessment of the infall returned None);
- **classical:** ln R ≤ 0;
- **one-loop:** ln R > 0 and `one_loop_turnaround` returns a solution with ℓ > 0; ℓ is its;
- **gap:** ln R > 0 and the turnaround raises, returns None, or returns ℓ ≤ 0 (P24); ℓ is None.

**The sources of the constants.**
- a = 2.69 is the Born coefficient of the one-loop fraction (`BriefingNotes/wall_backreaction.tex`, "The inelastic fraction"),
  held fixed in the fit.
- b₀, c and the gap's g, r₀, p, q are the side-campaign `crossover`'s fits to every stage-2 point
  (`prompts/one-loop-drain/source/crossover/out/fit_law.txt`): one-loop within 0.074 of 27 points (rms 0.021), gap within 0.062 of
  34 (rms 0.024). Stage 2 is a self-consistent one-loop solve with massive modes on the n = 1 wall (`source/crossover/README.md`,
  "Stage 2").
- f_cap = 0.95 and f_plateau = 0.9 are judgement: the user adopted them for qualitative behaviour, where stage 2 measured 0.88–0.98
  and 0.80–0.97 (README §8.10).
- S(L) is stage 1a's table (`source/crossover/out/stage1a_S_of_L.txt`, the classical bounce off V₀(e^{M/φ} − 1)), 0.1% high from
  its quadrature (crossover stage 2, V2q), which README §8.10 records as immaterial.
- The reference implementation is `fraction_adopted` in `source/crossover/measured_law_map.py`. The code equals it bitwise but in
  the two departures the user ruled: the continuation above L = 134 (the reference holds S(134)) and the gap's clamp (§2c.3).
- α_cap = 0.99 is U10. The version string moves whenever `drain_fraction`, its constants or the S table change
  (`drain_model.py:42`–`:46`).

**Under the key.** A history's law is part of its key: `alpha_value` is the pair (value, `drain_model`) (README §8.11 P27;
`ObjectFactories/alpha_value.py`). Under `"fixed"` the value is α, 0 ≤ α < 1; α = 0, natural mode, exists only there. Under
`"one-loop-measured-v1"` the value is A > 0. A lookup that names no model asks for `"fixed"` (`ObjectFactories/alpha_value.py:71`),
so every store and command of before this campaign reads as the fixed law.

**The law's domain.**
- **Fitted on** the n = 1 wall in stage 2 (27 one-loop and 34 gap points; `fit_law.txt`), and checked on the n = 2 wall: with
  L = ln(1 + E/V₀) the law is within 0.125 of the n = 2 one-loop points and within 0.10 of the gap points histories can reach
  (rms 0.05); with L = M/φ_t it is low by about 0.1 (README §8.10, "After closing: the n = 2 wall" in `source/crossover/README.md`).
- **Not measured:** n ≠ 1, 2. U15 (b) applies the law at every n "for now"; a later measurement revises it through the model's
  version. The fitted constants and S(L) are themselves measured on this family of walls, so moving L into the model does not make
  them model-independent (log 02c, "Observations not acted on"; README §8.10).
- **The gap** (ln R > 0 with no one-loop chain) is a fit to stage-2 points, not a solve of the physics there.

### 8.2 What the law did on the sample

02d's measurement (§2d) replaced 02's and 02b's (§2, §2b, which stand as the record of the old law, A·2.7/ℓ with a gap
prescription). The sample is 30 histories in six groups, re-tuned at U14's eight A ∈ {0.05, 0.1, 0.5, 1, 1.5, 2, 5, 10}, by the tool
route (`tools/history_and_bbn.py`): P and W, laboratory M (4 × 10⁻³¹ M_P) in the transition window, β 1.06–1.1875; R, laboratory M
at β 1.0, 1.15, 2, 10; B, 10⁻³³ and 10⁻³⁰ M_P at β 1.2, 2, 10; L, a ladder at β = 2 from 5 × 10⁻²⁰ to 4 × 10⁻¹⁵ M_P; C, 10⁻¹² M_P
at β 4.0, 1.0, 1.11 (`measure-02d/out/tasks.txt`).

- **Outcomes** (§2d.4): 240 tasks, **230 succeed, 3 fail, 7 time out.** Every P, W and B history builds at all eight A. R-632
  (β = 1.0) fails at A = 0.1 and 0.05 ("the effective minimum was not found" after the seed integration); the ladder's
  5 × 10⁻²⁰ M_P fails at A = 0.05 (the step budget, 2 × 10⁶ accepted steps, at N = 46.07 after 14 115 reflections); C-3873
  (β = 1.11, 10⁻¹² M_P) times out (3600 s) at seven A after 24–27 tuning integrations and succeeds at A = 1 after 28 in 3324 s.
- **The law against its own record** (§2d.2): α_eff recomputed by `drain_alpha_effective` equals the record's by `repr` at all
  616 270 candidates of 464 integrations.
- **The regime shares** (§2d.5). At laboratory M (P, W, and R but R-632) and in B, at A ≥ 0.1, every drain is one-loop, the
  one-loop share of K is ≥ 0.990 and the parking leftover holds 10⁻⁹–10⁻² of K. ℓ_min at P and W falls with A: 20.0–27.3 at
  A = 10, 5.1–15.5 at A = 1, 1.03–2.05 at A = 0.1. In 05's export of the 230 successes the one-loop share is 0.9998 at the
  median, 0 to 1 over the grid (README §8.15).
- **Where the cascade reaches the gap and R ≤ 1** (§2d.5). At A = 0.05 ten of the twelve P and W histories enter the gap before
  parking (5–22 gap candidates each, all drained), with ℓ_min 0.079–0.73 and a small gap share (5.2 × 10⁻⁴ of K for P-2197); none
  reaches R ≤ 1. R-632 enters the gap at A = 0.5. On the ladder, 8 × 10⁻¹⁸ to 5 × 10⁻¹⁶ M_P enter the gap at every A and
  5 × 10⁻²⁰ at A ≤ 0.5; 10⁻¹⁶ and 5 × 10⁻¹⁶ reach R ≤ 1 at every A, 8 × 10⁻¹⁸ at A ≤ 0.5 and 5 × 10⁻²⁰ at A = 0.1;
  4 × 10⁻¹⁵ is classical from its first impact. C is classical throughout, at α_eff = A·R·S(L)·w_gate of 3 × 10⁻⁷ (C-3016,
  A = 10) down to 2 × 10⁻⁵⁷ (C-3233, A = 0.05) at the first impact.
- **The law's edges** over the accepted integrations (§2d.5): the clamp (1 + q ln L ≤ 0) never met, the least gap L being 2.578; a
  gap candidate at ln R ≤ r₀ 24 times; the continuation (classical L > 134) never; S held at S(0.3) at 37 922 classical
  candidates (classical L down to 1.0 × 10⁻¹¹). The ranges: one-loop L 4.012–128.4, ln R 6.942–190.3, ℓ 0.0790–44.98; gap L
  2.578–106.0, ln R 0.0947–14.92; classical L 1.007 × 10⁻¹¹–125.6, ln R −239.4 to −0.0354.
- **The gate and the unassessed** (§2d.5): w_gate < 1 at no one-loop or gap drain; at classical drains of C-3233 (73 at every A)
  and C-3873 (1048 at A = 1). No candidate is unassessed in any task.
- **Parking and the first impact** (§2d.6). 221 of the 230 successes park; the 9 that do not are C-3233 at all eight A and C-3873
  at A = 1. At laboratory M the first impact is one-loop at every A and drains A·f₁, f₁ = 0.0823–0.115 at its ℓ of 22.8–32.3:
  0.82–0.99 of the kinetic energy at A = 10, 0.082–0.115 at A = 1 and 0.0041–0.0057 at A = 0.05. P and W park at
  N = 18.9–24.7 (A = 10), 29.0–35.9 (A = 1), 37.7–38.7 (A = 0.1) and 38.4–39.4 (A = 0.05), after 3–6, 5–21, 116–124 and 190–203
  drains. No drain follows parking. Where the ladder runs a classical tail it parks late, at N = 44.99–47.60 after 8 500–13 300
  classical drains.
- **The cap** (§2d.2, §2d.6): A·f ≥ 0.99 at 50 one-loop candidates in 24 accepted integrations and 6 gap candidates in 6, never at
  a classical one. The first impact caps for R-632 and the 5 × 10⁻²⁰ M_P ladder rung at A = 5 and 10, 8 × 10⁻¹⁸ at A ≥ 2 and
  10⁻¹⁶ at A ≥ 1.5.
- **ΔN_eff, Y_p and D/H** (§2d.7; Y_p and D/H as context, never a pass or a failure). P-2197 (β = 1.185), ΔN_eff today: 0.5554
  (A = 10), 0.06009 (A = 1), 0.01131 (A = 0.1), 0.008558 (A = 0.05). Across P and W: ΔN_eff today 0.029–0.634 at A = 10,
  0.0056–0.067 at A = 1, 0.0026–0.0128 at A = 0.1 and 0.0025–0.0093 at A = 0.05; ΔN_eff at 1 MeV is A times a constant of each
  history (P-2197: 0.0526–0.0538 × A), and 0 for P-2188, whose first impact comes after 1 MeV. Y_p 0.2469–0.2550 at A = 10 and
  0.2552–0.2742 at A = 0.05; D/H (10⁻⁵) 2.47–2.67 at A = 10 and 2.58–2.94 at A = 0.05.
- **Against the old law** (§2d.8): at laboratory M ΔN_eff today within 0.0085 (P) and 0.011 (W, R), Y_p within 3.8 × 10⁻⁴, D/H
  within 0.034 × 10⁻⁵; the parking N moves by up to 4.1 e-folds. The ladder near M_crit moves most, where the old `zero`
  prescription drained nothing in the gap: L-M5e-16's ΔN_eff today is 0.146 against 6.7 × 10⁻⁶ at A = 10 and 5.
- **Against the fixed law at laboratory M** (§2d.9; the 2026.8.0 store, a measurement on that tree): P-2197's ΔN_eff today at
  A = 0.05 and 0.1 (0.0086, 0.0113) is near the stored α = 10⁻³ and 10⁻² (0.0064, 0.0123), and at A = 1 (0.0601) near
  α = 0.08–0.1 (0.0567, 0.0707); it parks 5.7 (A = 0.05) to 26.3 (A = 10) e-folds earlier than at α = 10⁻³ (N = 45.16). At
  10⁻¹² M_P the stored fixed-law ΔN_eff today (C-3016 0.0079 at α = 0.1, C-3233 0.335 at α = 0.15, C-3873 0.023 at α = 0.03)
  falls to 7 × 10⁻⁶, 10⁻⁵⁵ and 7 × 10⁻²⁰ under the one-loop law.
- **The cost** (§2d.10): 40 985 s of task wall for 240 tasks under loads up to 50 on six workers; P, W, R and B 12–70 s a task
  at A ≥ 0.5; the ladder's classical tails 632–1115 s (L-M5e-20 at A = 0.1: 1 962 144 steps, 1115 s); C-3873's tuning 3324 s at
  A = 1.

### 8.3 The stored measures (model-free)

Since `FINGERPRINT_FORMAT` 7 (§3). None of them moves a value of the integration (§3.2; the gate Bitwise, §6.4).

- **The bounce rows** (table `ScalarHistoryBounce`; columns `BOUNCE_COLUMNS`, `ObjectFactories/ScalarHistory.py:165`–`:196`; built
  by `bounce_rows`, `:4713`). **One-loop law only**: the fixed law and α = 0 write none. One row per candidate, and one per turning
  point of the reflection margin that is not a candidate (`source` `candidate`, `plateau` or `wall`), in the event order of
  `relativistic_margins`, `ordinal` 0, 1, … (30 columns). Each candidate's row holds the drain (`delta_K_Mp4`, K in M_P⁴;
  `alpha_eff`; `w_gate`; `nonadiabaticity`; `K_before_Mp4`), the law's inputs and regime (`regime`, `ell`, `L`, `log_R`,
  `log_E_Mp4`, `log_H_Mp`), the peak estimate (`log_k_ex_Mp`, `log_m_Mp`, `log_q_Mp`) and, for a generation, its `share` of
  K_final, `weighted` and its three margins with their N. N and N_turn are e-folds; a `log_*_Mp`, `_Mp4` or `_GeV` column is ln of
  the quantity in that unit; the rest are dimensionless. The history's `bounce_rows` counts them, and `validate` checks the count.
  bounces.csv exports them cell for cell (04). On 02d's sample: 3 to 13 343 candidates per history, 82 139 in all (§2d.11).
- **The history's drain record** (columns of `ScalarHistory`, the fields of `DrainHistoryValues`, `:4913`–`:4929`, formed by
  `drain_history_values`, `:4966`–`:5017`). **One-loop law only**; NULL under the fixed law, at α = 0 and on a failure row. All
  dimensionless:
  - `drained_share_{one_loop,gap,classical,unassessed}`: the K drained in each regime over K_final; `parking_share`, the parking
    leftover's ΔK over K_final (0 if the history did not park). With K₀/K_final they sum to 1 to rounding;
  - `candidates_<regime>`: the candidates per regime, ΔK = 0 included; `ell_min`, the least ℓ over the one-loop candidates;
    `log_R_min_drain`, the least ln R over the candidates that drained; `unresolvable_assessments`, `unresolved_declined` (01's
    two counts);
  - the all-generation margins (below), and `bounce_rows`.
- **The all-generation margins** (`all_generation_margins`, `:4932`–`:4963`; P3, P30). For each of the birth, reflection and tracker
  margins (`MARGIN_KINDS`, `:4059`): the worst value over every generation, its N (the birth margin's N its N_turn) and the failing
  share, the summed `share` of the generations whose margin is below `MARGIN_THRESHOLD` = ln 10 (`:161`). Columns
  `all_worst_<kind>_margin`, `all_worst_<kind>_margin_N`, `all_<kind>_failing_share`. One-loop law only.
  - **How they differ from `inelastic-reflection`'s weighted margins.** The weighted ones (that campaign's prompt 09, stored at
    every α > 0) run over the generations holding at least `MARGIN_WEIGHT_SHARE` = 10⁻³ of K(N_final) (`:162`) only. In the low-α
    transition window at laboratory M the others held 80–98% of ρ_χ: on 2188 and 2197 at α = 0.001 the tracker margin over all
    generations was 7.3 and 1.6 against a stored 68.3 and 69.0, and the reflection margin −17.5 and −20.4 against 17.9 and 17.5,
    with 0.92 and 0.84 of ρ_χ below zero against a stored failing share of 0 (board §4,
    `[00-weighted-margins-hide-the-least-relativistic-generations]`, closed by 03). Under the fixed law the record stays the
    weighted one; the all-generation values and the generations are not stored there (ruling 2 of README §6.3).
- **The sample masses** (`ScalarHistoryValue.log_m_eff_Mp` and `.log_m_tracker_Mp`, ln(m/M_P), nullable; `MASS_COLUMNS`,
  `ObjectFactories/ScalarHistory.py:200`–`:203`; `sample_masses`, `:4544`). **Every law and every α**, natural mode included
  (U5, P31): at each stored sample, the field's own mass m_eff from its stored state and the zero-lag tracker's m_tr; NULL where
  the model call raises (m_eff² ≤ 0, no tracker minimum). fields.csv exports them (04).

### 8.4 The derived measures

Computed outside the integrator, from 04's exports, by `tools/quanta_measures.py` with the spectrum of
`ComputeTargets/quanta_spectrum.py` (05). A better spectrum is a re-run of the tool, not a rebuild. Every row of its three outputs
names the spectrum's version and c_a (log 05 §7).

**The spectrum model `U4-U17-v1`** (`SPECTRUM_MODEL`, `quanta_spectrum.py:103`; U4, U16, U17). One shape for every generation,
whatever its regime, in x = k/k_ex of the generation's own k_ex:

    s(x) = s_dir(x) + c_a s_assoc(x)
    s_dir(x) = x²  (x ≤ x_p),          x_p² e^{−κ(x − x_p)}  (x > x_p)                    (:189–:192)
    s_assoc(x) = 49 x_*³/x  (x ≤ x_p),  49 x_*³/x · e^{−κ(x − x_p)}  (x > x_p)              (:195–:203)

| constant | value | source |
|---|---|---|
| x_p (`X_PEAK`, `:108`) | 0.62 | `production_energy_ratio.tex` §"Results" and Table tab:beta: k_peak = 0.31 ω_t, with k_ex = ω_t/2 |
| κ (`KAPPA`, `:129`) | 3.9657820823215935 | κ_L = ln(\|β\|²_max,L/10⁻²⁰)/(10 − x_p) at the Table's median row L = 90 (\|β\|²_max = 1.43 × 10⁻⁴), from the same section's "about 10⁻²⁰ at k = 5 ω_t"; the five rows give 3.8821–4.1334, a spread of 6.4% (§5.2) |
| x_* (`X_STAR`, `:135`) | 0.0058 | `kb_partB_report.tex` §"The soft occupation": q̂ = 0.0029 ω_t |
| 49 (`ASSOC_COEFFICIENT`, `:137`) | 50 − 1 | the same: there n_q is fifty times the test field's \|β_q\|², so s(x_*) = 50 s_dir(x_*) at c_a = 1 |
| c_a | 1 by default (`DEFAULT_ASSOC_SCALE`, `:144`); 0 and 10 measured | U16 ("Keep the associated piece as the default"); P46 |
| the 1/x piece's end | 1/x to x_p, then e^{−κ(x − x_p)} | U17, the user's ruling of 2026-11-06; it carries 2.95 × 10⁻⁵ of I at c_a = 1 |

The moments M_k = ∫x^k s dx are in closed form; I = M_3 and J = M_2/M_3 (`:305`–`:306`): I = 0.09300715800293864 and
J = 1.0560436029665003 at c_a = 1 (§5.2). The kernels are G(y) = ∫x³ s [√(1 + y²/x²) − 1] dx / I, the matter-like excess of a
generation at y = m/q, and Ω(u) = ∫x³ s(x) s(x e^{−u}) dx / I, the overlap of two generations an e-fold distance u apart; each is
tabulated once per c_a and continued by closed-form asymptotes (§5.3; `numerical-strategies.md` §3.5's note of 2026-11-06).

**The measures**, as `tools/quanta_measures.py` computes them (a *generation* g is a candidate row with ΔK_g > 0 and a `log_q`,
born at N_turn,g with momentum Q_g = e^{log_q}; `Generations`, `:406`–`:428`):

| measure | formula | code |
|---|---|---|
| n_g, the occupation at birth at the direct peak | ln n_g = ln(2π² ΔK_g/(Q_g⁴ I)) + ln s(x_p), with s(x_p) = x_p² + c_a 49 x_*³/x_p | `:606`–`:613`; `quanta_spectrum.py:310` |
| n̄_j, the occupation already in generation j's band | n̄_j = Σ_{g: N_turn,g < N_turn,j} A_g Ω(ln Q_g − ln Q_j), A_g = 2π² ΔK_g/(Q_g⁴ I), untruncated | `occupation_overlaps`, `:451`–`:463` |
| δ_stim | δ_stim = Σ_j 2 n̄_j ΔK_j / K_final | `:644` |
| E(N), the matter-like excess | E(N) = Σ_{g: N_turn,g ≤ N} ΔK_g G(m e^N/Q_g) / K(N), with m the field's mass (`E_field`) or the tracker's (`E_tracker`), each from fields.csv; r_χ E beside it | `matter_excess`, `:431`–`:448` |
| the phase slip | over consecutive assessed candidates a, x, b: \|θ(x, b) − θ(a, x)\|, θ(p, q) = e^{ln Q_x − (N_p + N_q)/2 − ln H_p} (N_q − N_p), ln Q_x = ln k_ex,x + N_x; interior candidates only | `phase_slips`, `:466`–`:486` |
| k_ex/H | e^{ln k_ex − ln H_E}, both before the candidate | `:474` |
| the backreaction ratio | at each candidate k with a mass and an energy: n_χa³ e^{−3N_turn,k} m_k/E_k, n_χa³ = Σ_{g: N_turn,g < N_turn,k} ΔK_g J/Q_g | `backreaction_ratios`, `:489`–`:506` |

K(N) is rebuilt from the bounce rows (the parking leftover included) and checked at every sample against r_χ ρ_R,E e^{4N} to
10⁻⁹ relative (`K_check`, `:369`; `K_CHECK_TOLERANCE`, `:108`).

**The assumptions.** The spectrum is U4's shape at every generation. δ_stim is first order in the occupations, with random phases
between generations; rescattering is bounded by it (about 2n) and is not computed separately (README §8.3). E and the backreaction
ratio use the stored masses at the sample or at the candidate. The parking leftover enters K but has no spectrum, so it enters
neither E's numerator nor any occupation (P43).

**What was checked against what** (§5.3–§5.5). The kernels and moments against scipy's adaptive quadrature: G within 8.1 × 10⁻¹²
inside its table and 5.6 × 10⁻¹¹ beyond, Ω within 2.25 × 10⁻⁹ inside and 8.4 × 10⁻¹⁵ beyond, the moments within 4.4 × 10⁻¹⁶,
every asymptote within 10⁻⁷ at its end, at c_a = 1, 0 and 10. Every measure against a direct quadrature in q over the full spectrum
on constructed generations, to 10⁻⁶ (16 tests). With the spectrum collapsed to its peak, E on P-2197 at A = 1 against the
scoping probe's `excess` within 2.5ε relative at all 3 081 compared samples, and K(N) equal to the payload's ledger bit for bit.

### 8.5 What they say on the sample

At c_a = 1 on 02d's 230 successes (§5.7; `derive-05/out/report.txt`), unless stated:
- **δ_stim**: 2.2 × 10⁻⁷⁵ (B-2772-A10) to 1.34 × 10⁻³ (L-M1e-16-A0.5), median 3.5 × 10⁻¹⁷. The largest by group: 4.8 × 10⁻⁵ (P),
  9.0 × 10⁻⁵ (W), 1.8 × 10⁻⁶ (R), 6.2 × 10⁻¹¹ (B), 1.3 × 10⁻³ (L), 8.0 × 10⁻⁴ (C). Its median falls with A, from 1.0 × 10⁻⁵ at
  A = 0.05 to 2.6 × 10⁻⁵³ at A = 10, while its largest stays between 1.6 × 10⁻⁴ and 1.3 × 10⁻³.
- **n̄_max**: below 0.01 in every P, W, B and R history; above 1 in 33, all on the L ladder (5 × 10⁻²⁰ to 4 × 10⁻¹⁵ M_P, β = 2) and in
  group C (10⁻¹² M_P), up to 9.0 × 10³¹ (L-M5e-20-A0.1). ln n_g max likewise, up to 98.
- **E**: E_field max at most 5.4 × 10⁻²⁰, E_tracker max at most 7.2 × 10⁻¹⁶; r_χ E at most 1.9 × 10⁻²¹ (field) and 7.4 × 10⁻²¹
  (tracker). The one-loop generations stay relativistic.
- **The phase slip** at producing bounces: at least 7.8 × 10²¹ rad in every P, W, B and R history (median 6 × 10²²–3 × 10²⁸ by A);
  below 1 rad in 15 histories, on the ladder (5 × 10⁻²⁰ to 5 × 10⁻¹⁶ M_P) and in C at β = 1.0 and 1.11; the least 5.1 × 10⁻¹⁰ rad
  (C-3873-A1).
- **The backreaction ratio**: median 10⁻⁹–6 × 10⁻² by A; the largest 1.8 × 10³ (on the ladder: A = 2, 8 × 10⁻¹⁸ M_P, β = 2, log 07
  §6); by group P ≤ 244, W ≤ 258, R ≤ 918, B ≤ 0.10, C ≤ 2.8 × 10⁻⁷.
- **The all-generation margins** in 05's export: worst birth −26 to +41, reflection −15 to +42, tracker −36 to +43 (README §8.15);
  mostly positive over the grid and negative at a few cells (log 07 §6). ℓ_min is 0.079 to 41 where there is a one-loop drain, and
  blank in 33 of the 230 (README §8.15).
- **c_a**: only δ_stim and n̄ move. At c_a = 0 the ratio to c_a = 1 is 1 at the median and down to 5.6 × 10⁻⁸ (δ_stim) and
  3.5 × 10⁻¹⁶ (n̄_max) at the least; at c_a = 10 it is 1 at the median and at most 10 (both). n_g, E, r_χ E, the slip and the
  backreaction move by less than the third significant figure (§5.7).
- **Where the extremes sit.** The four histories drawn whole by 07 hold them (§7.4; log 07 §6): A = 0.1, β = 2, 5 × 10⁻²⁰ M_P
  (13 342 generations, n̄_max 9.0 × 10³¹, 6 302 of its slips below 1 rad, in its long classical tail); A = 5, β = 2, 5 × 10⁻¹⁶ M_P
  (gap share 0.99998); A = 10, β = 1, 10⁻¹² M_P (classical share 1.0000000000000007, above 1 by rounding); A = 2, β = 2,
  8 × 10⁻¹⁸ M_P (backreaction 1 785).

These are measurements, not bounds (P49): no threshold refuses or warns on any of them.

### 8.6 Where the reading does not hold, and what is not modelled

- **The random-phase premise of δ_stim** does not hold in 33 histories, where n̄ > 1 (the L ladder and group C, above), and in 15
  of them the phase slip at a producing bounce falls below 1 rad (§5.7; log 05 §6). The large overlaps sit at bounces that drain
  almost nothing: L-M5e-20-A0.1, whose n̄_max is 9.0 × 10³¹, has δ_stim 4.2 × 10⁻⁹. There the first-order, random-phase number is
  not a measure of stimulated emission, and nothing else here is. At laboratory M and in B (n̄ < 0.01, slips ≥ 7.8 × 10²¹ rad) the
  premise holds. Open issue: `[00-later-bounces-act-on-quanta-already-present]` (left open, as the user ruled).
- **The backreaction ratio above 1**, in P (≤ 244), W (≤ 258), R (≤ 918) and L (≤ 1.8 × 10³), not in B or C (§5.7): the produced
  quanta's rest energy at the mass of a later bounce exceeds that bounce's impact energy. The integration does not include
  n_χ m(φ). No open issue; out of scope (README §8.1, the user's ruling of source §7).
- **The parking leftover** has no spectrum: it enters K, not E or any occupation. Its share is stored (`parking_share`) and is
  0 to 0.99999 over the 230, 7 × 10⁻⁵ at the median (README §8.15), largest where the drains are classical (4 × 10⁻¹⁵ M_P,
  ≥ 0.968; C-3016-A0.05, 0.999993; §2d.5, §5.6). Open issue: `[00-the-parking-leftover-has-no-spectrum]`.
- **The fixed law has no instrumentation**: no bounce row, a NULL drain record, the weighted margins only (README §8.1, the user's
  ruling 2: "limited meaning there"). The sample masses are stored at every law. No issue.
- **Not modelled** (README §8.1, out of scope): stimulated emission and rescattering (`[00-later-bounces-act-on-quanta-already-present]`);
  the backreaction n_χ m(φ) (no issue); production at R < 1 in the 1/φ tail (no issue; the classical regime drains R·S(L) of the
  wall); the gap's physics, bracketed by a fit (no issue); parking at physical M, and the parking leftover's spectrum
  (`[00-the-parking-leftover-has-no-spectrum]`). The small-A tunings that do not park before today are
  `[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]`.

## 9. Prompt 08 — the close (2026-11-07)

**Tree:** branch `one-loop-drain` at `d0d8918` (orchestrator note 08's commit, on `e951b8e`, which adds the prompt); no code
changed, so the tree that ran every check below is the tree this commit leaves. §1–§8 above are §8's only addition. **Scripts:**
[`one-loop-drain/close-08/`](one-loop-drain/close-08/): `run_inventories.sh` (§9.3) and `check_help.py` (§9.4). **Committed
outputs:** [`one-loop-drain/close-08/out/`](one-loop-drain/close-08/out/): `inventory-reference.txt`, `inventory-basecopy.txt`
(the session scratchpad's directory written `<scratch>`), `check_help.txt` and each command's `help-<name>.txt`. Orchestrator note
08's rulings 1–11 governed. No Ray was started, no store was built, and no build, plot script, derivation tool, figure tool or
registered run was run. Log: [`08-close.md`](../prompts/one-loop-drain/logs/08-close.md).

### 9.1 The Gate

Re-checked at 20:38 GMT: `one-loop-drain` at `d0d8918`, `git status --porcelain --untracked-files=all` empty, the main checkout the
only worktree; `tools/ray_cluster.sh status` exit 0 (the lock free, no Ray process of the cluster, ports 6380, 6381, 8267 and
52367 free); `runregistrykit list` 33 runs, all finished; `one-loop-drain-06-reference`, `-base` and `-resumed` 18 files each;
`.documents/OPEN_ISSUES.md` 29 open, §1.23 the board's three, none assigned (board §3.1); `git merge-base --is-ancestor main
one-loop-drain` exit 0.

### 9.2 The suites

Once, serially, before any edit, with `CLAUDE.md`'s commands (`unittest`'s time; the one-minute load at the start; the
`test_[g-z]` half and `tools/tests` in the background):

| suite | count | time | load at start |
|---|---|---|---|
| CosmologyModels | 26 OK | 66.8 s | 3.51 (20:38) |
| ComputeTargets `test_[a-f]` | 219 OK | 148.1 s | 5.21 (20:39) |
| ComputeTargets `test_[g-z]` | 446 OK | 491.1 s | 5.02 (20:42) |
| ObjectFactories | 163 OK | 125.9 s | 5.08 (20:50) |
| tools | 259 OK | 266.5 s | 2.30 (20:52) |
| RunDrivers | 72 OK | 25.0 s | 15.47 (20:57) |

07's counts exactly (26, 665 = 219 + 446, 163, 259, 72); none failed. No `.py` file outside `close-08/` changed afterwards.

### 9.3 The inventories (§2 item 4; ruling 3)

- **`one-loop-drain-06-reference`, in place:** `main.py --config exponential.yaml --database
  var/datastores/one-loop-drain-06-reference.db --shards 16 --inventory`, exit 0 in 25.3 s, no Ray: "read-only; 16 shards",
  75,121 records in 16 classes, `problems: none`, label 2026.10.0; `alpha_value` 6 records (0, 0.01, 0.1, 0.15, A = 0.1 and A = 1
  under `one-loop-measured-v1`); `ScalarHistory` 26 (26 validated); `BBNData` 26. `shasum -a 256` of its 18 files before and after:
  identical (the primary `848435c3…51e8`, the manifest `1f7f59bb…f7ee`).
- **A copy of `one-loop-drain-06-base`** (2026.9.0, format 6): its 18 files copied by `cp -p` into `<scratch>/basecopy/`, the
  copy's 18 hashes equal to the originals' before anything ran. `main.py --inventory` on the copy: **exit 1 in 1.6 s**, `!!
  --inventory: Cannot read sharded datastore …: shard #0 … differs from the tables this code declares (it lacks the table(s)
  "ScalarHistoryBounce"; its table "alpha_value" lacks the column(s) "drain_model"; its table "ScalarHistory" lacks the column(s)
  [the 23 of the drain record]; its table "ScalarHistoryValue" lacks the column(s) "log_m_eff_Mp", "log_m_tracker_Mp"), so nothing
  was read`. The copy's 18 hashes after: identical (the primary `64516736…1f87`, the manifest `4d1b3bbb…679f`), and the directory
  holds the same 18 files. The tip refuses a format-6 store and writes nothing to it. Both results are the orchestrator's (note 08
  §0) exactly.

### 9.4 The `--help` check (§2 item 3; ruling 4)

`check_help.py`, run after §4.20's text was final ([`out/check_help.txt`](one-loop-drain/close-08/out/check_help.txt)): ten
commands with `--help`, each exit 0 (`main.py`, `plot_by_beta.py`, `plot_ScalarHistory.py`, `RunDrivers/registered_run.py`,
`tools/quanta_measures.py`, `tools/plot_quanta_grid.py`, `tools/plot_quanta_history.py`, `tools/bbn_from_store.py`,
`tools/history_and_bbn.py`, `runregistrykit list`); the 30 options §4.20 names, each found in its own command's help as §4.20
spells it (63 checks), `--stale-after` in `runregistrykit list`'s; and §4.20's one git command, `git merge-base --is-ancestor main
one-loop-drain`, run as written, exit 0. 0 failures. A first run counted `--is-ancestor` as an option of no command, and the
script was given its git command (log 08 §2).

### 9.5 The twenty numbers (§2 item 5)

Ten of §8 and ten of §4.20, each found at its cited source (the lines are of `d0d8918`'s files, or of this commit's for
`close-08/out/`); none differs.

| # | number, as written | where written | source line |
|---|---|---|---|
| 1 | α_eff equal at 616 270 candidates | §8.2 | this document `:1099` (§2d.2) |
| 2 | ℓ_min 1.03–2.05 at A = 0.1 (P, W) | §8.2 | `:1168` (§2d.5) |
| 3 | S held at 37 922 classical candidates | §8.2 | `:1183` (§2d.5) |
| 4 | f₁ = 0.0823–0.115 at ℓ 22.8–32.3 | §8.2 | `:1195` (§2d.6) |
| 5 | P-2197 ΔN_eff today 0.06009 at A = 1 | §8.2 | `:1215` (§2d.7) |
| 6 | one-loop fit within 0.074 of 27 points | §8.1 | `source/crossover/out/fit_law.txt:30` |
| 7 | κ = 3.9657820823215935 | §8.4 | `:1663` (§5.2) |
| 8 | I = 0.09300715800293864 at c_a = 1 | §8.4 | `:1674` (§5.2) |
| 9 | δ_stim median 3.5 × 10⁻¹⁷ | §8.5 | `:1772` (§5.7) |
| 10 | least producing phase slip 5.1 × 10⁻¹⁰ rad (C-3873-A1) | §8.5, §8.6 | `:1782` (§5.7) |
| 11 | 2 524 629 values equal (Bitwise) | §4.20 point 1 | `:1947` (§6.4) |
| 12 | C-3873 at A = 1: 28 integrations, 3324 s | §4.20 point 3 | `measure-02d/out/cost.txt:243` |
| 13 | L-M5e-20 at A = 0.1: 1 962 144 steps, 1115 s | §4.20 point 3 | `:1270` (§2d.10) |
| 14 | `--tuning-max-integrations` default 30 | §4.20 point 3 | `ComputeTargets/ScalarHistory.py:5453` |
| 15 | step budget 2 000 000 accepted steps | §4.20 point 3 | `ComputeTargets/ScalarHistory.py:380` |
| 16 | longest heartbeat silence 160 s | §4.20 point 3 | `:1902` (§6.2) |
| 17 | 44.6 → 39.3 GB free | §4.20 point 3 | `:1899` (§6.2) |
| 18 | reference store 137 352 KiB, `store_id` `2b6fd51c…` | §4.20 point 5 | `prompts/one-loop-drain/logs/06-rehearse.md:236` |
| 19 | 75,121 records, 25.3 s | §4.20 points 1, 5 | `close-08/out/inventory-reference.txt:28`, `:153` |
| 20 | block C, 836.6 s per (M, β) over the eight A (the medians 112.3, 112.4, 112.3, 110.3, 94.2, 110.3, 92.4, 92.4) | §4.20 point 3 | `measure-02d/out/cost.txt:289`–`:296` ("C A=…") |

### 9.6 The rest of §2

- **§2 item 2**, the lines addressed to 08: the table is log 08 §3; each is done or named as left, with why.
- **§2 item 6**, the board and the index agree: `.documents/OPEN_ISSUES.md` counts 29 rows in its §1 and says 29; its §1.23
  holds exactly the board's three §3 entries, none assigned (board §3.1 "None"); `prompts/INDEX.md`'s row says "§1.23; 3 open; 29
  open in the whole index", and its header 17 campaigns, all complete, as its 17 rows say.
- **§2 item 7**, `git diff --stat d0d8918` before the commit: the files of prompt §4 only (log 08 §4); no Python file changed but
  `close-08/check_help.py`, which `black` formats. No existing line of this document, `numerical-strategies.md`,
  `architecture-summary.md` or `review-remediation-verification.md` is deleted (`git diff --numstat`: 0 deleted lines in each).
