# one-loop-drain — verification

**Campaign:** [`prompts/one-loop-drain/`](../prompts/one-loop-drain/README.md) · **Board:**
[`IMPLEMENTATION_STATE.md`](../prompts/one-loop-drain/IMPLEMENTATION_STATE.md) · **Source:** the drain probe
[`source/drain-probe_2026-11-02.md`](../prompts/one-loop-drain/source/drain-probe_2026-11-02.md).

This document holds the campaign's measurements (README §8.6, P16). It is additive (`CLAUDE.md` invariant 6): a
later run that supersedes a figure gets a new dated subsection, and the figures below stay as they are, correct for
the tree they were taken on.

## 1. Prompt 01 — the drain law (2026-11-03)

**Tree:** branch `one-loop-drain` at `b1415a3` plus prompt 01's changes, committed as prompt 01's commit (this
commit). Python 3.13.16, SciPy 1.17.0, NumPy 2.3.5. **Scripts:** [`one-loop-drain/law/`](one-loop-drain/law/), each
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
