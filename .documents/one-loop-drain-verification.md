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
§2 measured. Prompt 02b's commit (this commit) adds no code outside
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
