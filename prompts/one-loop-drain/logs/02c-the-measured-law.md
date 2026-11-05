# Log 02c — the measured law

**Subject:** Put the measured drain law in the code and remove the gap prescriptions
**Commit:** this commit, on branch `one-loop-drain`, on top of `1647f7a`
**Date:** 2026-11-05, by Claude Opus 5.5
**Result:** **COMPLETE.** The Measured gate holds (`.documents/one-loop-drain-verification.md` §2c.6).
- **The law.** Under the one-loop law each candidate now drains min(0.99, A·f)·w_gate, with f the law the user adopted
  from `crossover` (README §8.10). The `zero` and `edge` gap prescriptions are removed, and so is 2.7/ℓ.
- **The two forms agree.** The law is bitwise equal to the reference `fraction_adopted` on an 18 225-point grid, apart
  from two ruled departures: the S(L) continuation above L = 134, and f = 0 where 1 + q ln L ≤ 0 (the user's ruling).
- **The maps.** Crossover's four adopted-law maps are reproduced candidate by candidate, and the control is
  byte-identical. The headers' full-precision parking and final N differ in their last digits. That comes from the
  U15 (b) form of L (proven), and the user ruled it the exception.
- **L moved into the model class**, with no default, by the user's ruling. Under the one-loop law a model without it is
  refused at entry. After the move, every output was re-run, and each is byte-identical to the first run.
- **Issues:** none opened, narrowed or closed. Index: 31 open, unchanged.
- **Suites:** before anything else, 26, 566 (163 + 403), 130, 156, 68, all passing. After: 26, 594 (162 + 432), 130,
  156, 68, all passing.

Orchestrator note 02c (§0, rulings 1–11 and the conventions) governed where it differs from the prompt. Each ruling is
classified in §2, beside the user's three rulings made during the work.

## 1. What shipped

- **`ComputeTargets/ScalarHistory.py`**
  - **The law's constants**, each with its source (P17):
    - `DRAIN_ONE_LOOP_A` = 2.69, `DRAIN_ONE_LOOP_B0` = 0.9691, `DRAIN_ONE_LOOP_C` = 0.41, `DRAIN_ELL_PLATEAU` = 1;
    - `DRAIN_F_CAP` = 0.95 and `DRAIN_F_PLATEAU` = 0.9;
    - `DRAIN_GAP_G` = 0.0198, `DRAIN_GAP_R0` = 1.5601, `DRAIN_GAP_P` = 1.5472, `DRAIN_GAP_Q` = 0.841;
    - `DRAIN_ALPHA_CAP` = 0.99 is kept.
  - **The S table:** `DRAIN_S_TABLE_L` and `DRAIN_S_TABLE_S`, the 21 values of `stage1a_S_of_L.txt` as the same decimal
    literals.
  - **The drain-model names:** `DRAIN_MODEL_FIXED` = `"fixed"` and `DRAIN_MODEL_ONE_LOOP` = `"one-loop-measured-v1"`.
    The comment says the version moves whenever the law, its constants or the table change.
  - **Pure functions:**
    - `drain_S_of_L(L)`: `np.interp` on the table's `np.log`, as `S_of` does; S(0.3) below; S(134)(134/L)² above 134.
    - `drain_fraction(regime, ell, L, log_R)`: README §8.10's f, term for term as `fraction_adopted`, with the gap
      clamped to 0 where s = 1 + q ln L ≤ 0.
    - `drain_alpha_effective(regime, ell, L, log_R, A, w_gate)`: min(`DRAIN_ALPHA_CAP`, A·f)·w_gate. Unassessed gives
      0.0 without reading L or ln R.
    - `drain_law_inputs(model, assessment)`: (`model.drain_L(ln E)`, ln R), or (None, None) if unassessed.
    - `check_drain_law_model(drain_law, model, context)`: under the one-loop law, a model whose class does not override
      `_drain_L` is a `ValueError` naming the class and the hook.
  - **`candidate`** computes L and ln R once (`drain_law_inputs`). It hands them to `drain_alpha_effective` and writes
    them into the `DrainCandidate`.
  - **`integrate_scalar_history`** (after `policy` is fixed, before the ledger checks and the loop), **`integrate_history`**
    and **`tune_history`** (before the seed) call `check_drain_law_model` beside `check_drain_law`.
  - **The records.** `DrainCandidate` appends `L` and `log_R`. `DrainRecord` has `model` where `gap_prescription` stood.
  - **Removed:** `_GAP_PRESCRIPTION`, `GAP_PRESCRIPTION_ZERO`, `GAP_PRESCRIPTION_EDGE`, `GAP_PRESCRIPTIONS`, `DRAIN_C1`
    and `DRAIN_ELL_EDGE`.
- **`CosmologyConcepts/Models/AbstractModel.py`**
  - `AbstractModel._drain_L(normalization, log_E)`. The base raises `NotImplementedError` naming the class and "the
    one-loop drain law". It is not abstract.
  - `AbstractModel.drain_L_implemented`: the class overrides the hook.
  - `RealizedModel.drain_L(log_E)` and `RealizedModel.drain_L_implemented`.
- **`CosmologyConcepts/Models/ExponentialModel.py`:** `_drain_L` = ln(1 + E/V₀), as x + log1p(e^{−x}) for
  x = ln E − ln V₀ > 0 and log1p(e^x) otherwise. Its comment names it the wall exponent (M/φ_t)^n, U15 (b) "for now",
  on the family the law was fitted on. `math`'s `exp` and `log1p` are imported.
- **`tools/history_and_bbn.py`**
  - `--gap-prescription` is removed, with its check and the attribute's save and restore; it is now an unknown argument.
  - Under `--drain-law one_loop` the history line names `drain_law=one_loop A=<A> model=<DrainRecord.model>`.
  - The `drain` line is as before. It already gives the K share by regime, so nothing was added.
- **Tests**
  - `ComputeTargets/tests/test_measured_drain_law.py`, new, 29 tests: the constants and their removal; the S table;
    the law row by row; the reference grid; the gap's zero below the boundary; L and U15 on the model's method; the
    model hook and the refusal.
  - `ComputeTargets/tests/test_drain_law.py`, changed (§4).
  - `tools/tests/test_history_and_bbn_drain_law.py`, changed (§4).
- **`prompts/one-loop-drain/source/crossover/`** (prompt §1.5):
  - `cascade_map.py` sets `SM._GAP_PRESCRIPTION` only where it exists;
  - `measured_law_map.py`'s patched `drain_alpha_effective` takes `(regime, ell, *args)` and reads A and w_gate as the
    last two, so the same patch applies the same law on trees before and after this commit.

  The diff is in §3.
- **`.documents/one-loop-drain/law/README.md`, `measure/README.md` and `measure-02b/README.md`**, new, each holding only
  the dated note of ruling 6. The scripts are byte-identical.
- **`.documents/one-loop-drain/law-02c/`**: `make_inputs.py`, `batch.py`, `compare_maps.py`, `l_form_probe.py` and
  `compare_runs.py`, with their text outputs in `out/`.
- **Documents:**
  - `.documents/one-loop-drain-verification.md` §2c (§1, §2 and §2b unchanged);
  - dated notes in `numerical-strategies.md` §3.5 (the law, its constants, S(L), the clamp, L in the model, the
    removals) and in `architecture-summary.md` (the functions, the model hook and the refusal, `DrainRecord.model`,
    the new `DrainCandidate` fields, the tool's option);
  - `CLAUDE.md`'s test counts;
  - this log, and the board.

## 2. Deviations from the prompt

Each of orchestrator note 02c's rulings, then the user's.
1. **Note ruling 1, the signature.** `drain_alpha_effective(regime, ell, L, log_R, A, w_gate)`: the law's inputs come
   first, then its strength and the gate. f itself is `drain_fraction(regime, ell, L, log_R)`, so the reference test
   compares f directly. `drain_law_inputs` computes L and ln R once per candidate in `candidate`, and the same two
   numbers go into the `DrainCandidate`. *IMPLEMENTATION CHOICE.*
2. **Note ruling 2, L computed stably.** x + log1p(e^{−x}) for x > 0, log1p(e^x) otherwise, tested at x = −30, 0, 30 and
   800 and at the maps' x. *STRUCTURALLY REQUIRED.* Under the user's ruling B it lives in `ExponentialModel._drain_L`
   (item 13).
3. **Note ruling 3, the reference made exact.**
   - `fraction_adopted` and `S_of` are copied verbatim into `test_measured_drain_law`, naming the file and `1b16c62`.
   - The table loading is kept verbatim, but it reads the table file's rows copied as text into the module
     (`io.StringIO` in place of the path), so the test depends on no file under `prompts/`.
   - The production S table uses the same literals and the same `np.interp` on `np.log`, and is bitwise equal at
     L ≤ 134.
   - The two forms of L are tested apart, the n = 1 agreement to 10⁻¹⁰.

   *IMPLEMENTATION CHOICE* (the table as embedded text), as the ruling allows.
4. **Note ruling 4, §2.3 made exact.** Inputs, comparison, control and exception as ruled (`compare_maps.py`). Ruling A
   (item 12) records the one difference.
5. **Note ruling 5, the detached batch.** One batch, `nohup`, at most five at once, with `batch.log`, `batch.pid` and
   `batch.end`, one check that it started, and a background wait.
   - It has seven runs, not five: two more for the re-pinned 2197 test, 2197 at A = 1 unpatched and patched, 3 s each.
     *IMPLEMENTATION CHOICE.*
   - The tool run took 27–28 s, in the foreground.
6. **Note ruling 6, the dated notes.** A `README.md` in each of `law/`, `measure/` and `measure-02b/`, naming which of
   its scripts name which removed symbol. *IMPLEMENTATION CHOICE* and *STRUCTURALLY REQUIRED*, as ruled.
7. **Note ruling 7, the record's fields.** `DrainCandidate` appends `L` and `log_R`. `DrainRecord` puts `model` where
   `gap_prescription` stood. Every positional construction in the tests is updated. *IMPLEMENTATION CHOICE*, as ruled.
8. **Note ruling 8, the suites.** Run serially, three times, in one background chain (§4). The deleted tests are named
   in §4. *As ruled.*
9. **Note ruling 9, the gap below L ≈ 0.304: replaced by the user's ruling** (item 11). No issue was opened.
10. **Note rulings 10 and 11.** The handover is in §7. The files are prompt §5's, plus ruling 11's extras: the three
    READMEs, the two crossover scripts, and (ruling B) `AbstractModel.py` and `ExponentialModel.py`. Nothing under
    `var/` is committed. `OPEN_ISSUES.md` is unchanged, since no issue moved. *STRUCTURALLY REQUIRED.*
11. **The user's ruling on note ruling 9, 2026-11-05:** "I think probably we set `f = 0` there".
    - In the gap, f = 0 where ln R ≤ r₀ or s = 1 + q ln L ≤ 0. The test is on s, not on L against a rounded 0.304.
    - Production therefore departs from `fraction_adopted` in a second place: where the reference is negative,
      production is max(0.0, reference). The reference test checks exactly that.
    - The boundary test: f is 0.0 at the first float below the boundary, and positive a relative 10⁻⁶ above it. At the
      first float above it, s ≈ 10⁻¹⁶ and 1 − e^{−y} rounds to 0.0, so the test there asserts f ≥ 0.
    - `DRAIN_MODEL_ONE_LOOP` stays `one-loop-measured-v1`.

    *STRUCTURALLY REQUIRED* (the user's ruling).
12. **Ruling A, the header differences:** "Decision 2: yes". A deviation from note ruling 4 (i) as worded.
    - Every row of the four maps is equal, and the control byte-identical.
    - The headers' `parking_N` and `N_final`, printed at full precision, differ at 10⁻¹⁴–10⁻¹⁵ relative for P-2197
      (`N_final`), W-2187 and L-M5e-20 (both).
    - `l_form_probe.py` shows the only cause is the U15 (b) form of L:
      - with M/φ_t in its place, every header and row equals crossover's;
      - α_eff differs by at most 2.5 × 10⁻¹⁴ relative, which meets (ii).
    - (i) speaks of a row, so the work stopped and asked. The user ruled it ruling 4's exception.

    *STRUCTURALLY REQUIRED* (the user's ruling).
13. **Ruling B, L in the model class, 2026-11-05:** "I suggest we move `L` into the model class, with no default. Then
    when a new potential is implemented, breakage is loud, and the implementer has to think about what they are
    changing." This deviates from prompt §1.1 (L computed in `ScalarHistory.py` from ln E and ln V_floor), from note §0's
    second point (no model file changes) and from ruling 11's file list.
    - **The hook.** `AbstractModel._drain_L(normalization, log_E)` raises; `RealizedModel.drain_L(log_E)` is its route;
      `ExponentialModel` implements it. `drain_law_L` left `ScalarHistory.py`, and no copy of the formula stays there.
    - **Detection:** `AbstractModel.drain_L_implemented` is `type(self)._drain_L is not AbstractModel._drain_L`: does
      the class override the hook? `RealizedModel` delegates to it. `check_drain_law_model` reads it under the
      one-loop law only, at the three entry points.
    - **Not an `abc.abstractmethod`,** so that `_PowerLawWall` and `_MinimalModel` still instantiate. The tests show
      both lack it.
    - **The re-verification is byte-identical:** the suites, the batch (all seven outputs) and the tool run (every line
      but the wall times) equal the first run's, and `l_form_probe.py`'s output too (verification §2c.5).

    *STRUCTURALLY REQUIRED* (the user's ruling).
14. **The re-pinned test.** `TestTheOneLoopLawOn2197` pinned no values under the old law, only the record's structure
    and its agreement with `drain_alpha_effective`. It is re-pinned with a new test,
    `test_4_the_values_under_the_measured_law`: the five regimes, each α_eff at `:.4e`, and the parking N to 10⁻⁹.
    These were cross-checked against `measured_law_map.py --law=adopted` on the same inputs (P-2197's record at
    A = 1, `var/one-loop-drain/02c/inputs/P-2197-A1.result.json`), and they agree exactly in rows and header. The
    class's other tests were updated for the record's new fields (`model`; `L` and `log_R` checked against
    `model.drain_L` and the turning point, and L against M/φ_t to 10⁻¹⁰). *IMPLEMENTATION CHOICE.*

## 3. The changes to the crossover scripts

```diff
--- a/prompts/one-loop-drain/source/crossover/cascade_map.py
+++ b/prompts/one-loop-drain/source/crossover/cascade_map.py
@@ -64,8 +64,11 @@ def main(result_json: str, tuning_json=None) -> int:
     SM.integrate_scalar_history = capture
-    gap_before = SM._GAP_PRESCRIPTION
-    SM._GAP_PRESCRIPTION = gap
+    # one-loop-drain prompt 02c removed _GAP_PRESCRIPTION: set it only where it exists
+    has_gap = hasattr(SM, "_GAP_PRESCRIPTION")
+    if has_gap:
+        gap_before = SM._GAP_PRESCRIPTION
+        SM._GAP_PRESCRIPTION = gap
     try:
@@ -81,7 +84,8 @@ def main(result_json: str, tuning_json=None) -> int:
     finally:
         SM.integrate_scalar_history = integrate
-        SM._GAP_PRESCRIPTION = gap_before
+        if has_gap:
+            SM._GAP_PRESCRIPTION = gap_before
--- a/prompts/one-loop-drain/source/crossover/measured_law_map.py
+++ b/prompts/one-loop-drain/source/crossover/measured_law_map.py
@@ -106,7 +106,10 @@ def drain_regime(model, assessment):
-def drain_alpha_effective(regime, ell, A, w_gate):
+def drain_alpha_effective(regime, ell, *args):
+    # (regime, ell, A, w_gate) before one-loop-drain prompt 02c, (regime, ell, L, ln R, A, w_gate) after it; the
+    # law applied is LAW's either way, with the turning point kept by drain_regime
+    A, w_gate = args[-2], args[-1]
     return min(SM.DRAIN_ALPHA_CAP, A * LAW(regime, ell, _last.get("tp"))) * w_gate
```

## 4. Verification performed

**The suites**, serially, in one background chain each time (CosmologyModels; ComputeTargets `test_[a-f]`, then
`test_[g-z]`; ObjectFactories; tools; RunDrivers), 2026-11-05. Load is the 1-, 5- and 15-minute average at each
suite's start → end.

| suite | before, at `1647f7a` | after the law | final tree (after ruling B) |
|---|---|---|---|
| CosmologyModels | 26 OK, 64.1 s (20.31 25.52 29.78 → 16.10 23.37 28.67) | 26 OK, 65.7 s (10.65 7.94 13.18 → 6.07 7.13 12.49) | 26 OK, 72.6 s (10.80 11.78 11.84 → 18.90 13.83 12.59) |
| CT `test_[a-f]` | 163 OK, 139.5 s (→ 7.48 17.50 25.51) | 162 OK, 138.1 s (→ 3.48 5.82 11.16) | 162 OK, 143.4 s (→ 11.74 13.61 12.76) |
| CT `test_[g-z]` | 403 OK, 468.0 s (→ 3.37 6.71 16.41) | 426 OK, 468.8 s (→ 22.64 16.89 14.38) | 432 OK, 489.5 s (→ 8.50 12.12 12.66) |
| ObjectFactories | 130 OK, 81.1 s (→ 4.19 6.17 15.28) | 130 OK, 85.9 s (→ 89.86 39.07 23.04) | 130 OK, 82.8 s (→ 10.99 11.75 12.44) |
| tools | 156 OK, 161.7 s (→ 5.72 6.20 13.68) | 156 OK, 167.1 s (→ 43.50 42.41 27.60) | 156 OK, 170.2 s (→ 7.86 9.98 11.56) |
| RunDrivers | 68 OK, 23.1 s (→ 10.18 7.20 13.83) | 68 OK, 24.5 s (→ 44.61 42.64 28.10) | 68 OK, 24.8 s (→ 11.34 10.64 11.75) |

Each suite starts at the load the previous one ended at. The machine has 10 cores and is shared. The load of 89.9
during the second chain came from no process of this work.

**The counts.** ComputeTargets goes from 566 to 594.
- **Deleted**, both from `test_drain_law` (`test_[a-f]`), because they tested the removed prescriptions:
  - `TestTheRegimeAndAlphaEff.test_2_the_gap_prescriptions_read_at_call_time`;
  - `TestTheRegimeAndAlphaEff.test_2_an_unknown_prescription_is_refused_at_the_gap`.
- **Added:** `TestTheOneLoopLawOn2197.test_4_the_values_under_the_measured_law` (`test_[a-f]`), and
  `test_measured_drain_law`'s 29 (`test_[g-z]`).
- **Rewritten, count unchanged:**
  - in `test_drain_law`: `test_2_the_constants_and_their_names`; `test_2_alpha_eff_at_a_one_loop_turnaround`;
    `test_2_the_cap`; `test_2_w_gate_zero_drains_nothing`; `test_2_nothing_where_R_is_at_most_one_or_unassessed`,
    renamed `test_2_nothing_where_unassessed`; `test_2_an_unknown_regime_is_refused`; and the `DrainRecord`
    constructions in four tests (three fixed-law records and one one-loop sentinel);
  - in `tools/tests/test_history_and_bbn_drain_law`: `test_5_the_gap_prescription_needs_the_one_loop_law`, renamed
    `test_5_the_gap_prescription_is_an_unknown_argument`; and
    `test_5_the_tuned_path_hands_the_law_A_and_the_prescription_on`, renamed `test_5_the_tuned_path_hands_the_law_and_A_on`;
    and the record's constructions and the history line's pattern in `TestTheDrainValues` and `TestAOneLoopRun`.

**The goldens** (prompt §2.1) are unchanged files and were discovered in the passing chains: `test_golden_history` 3,
`test_golden_reflecting_history` 2, `test_tuning` 40, `test_store_fingerprint_golden` 5.

**The Measured gate**, item by item (verification §2c):
1. **§2.1.** The goldens pass bitwise. The fixed-law payload differs only in `DrainRecord.model` = `"fixed"`.
2. **§2.2.** `drain_fraction` and `drain_alpha_effective` equal `fraction_adopted` bitwise on the grid: 18 225 f
   values, each with three (A, w_gate). The exceptions are the classical regime above L = 134 and the gap where
   1 + q ln L < 0, where they equal the reference with the ruled departure substituted. S at the nodes is bitwise
   `S_of`'s.
3. **§2.3.** Every candidate row of the four maps is equal (203, 199, 32, 94), the candidate counts are equal, and the
   control is byte-identical (20 639 bytes). The headers' last digits are ruling A's (item 12). Ruling 9's count, now
   under the user's rule: 0 gap candidates with 1 + q ln L < 0 in the four maps and the tool run; the least L at a gap
   candidate is 2.578. Walls: P 19.8 s, W 19.5 s, R-632 5.7 s, ladder 14.3 s, control 19.8 s. The batch took 19.9 s,
   at loads 43.06 42.42 28.28 → 37.69 41.24 28.19.
4. **§2.4.** The tool run took 27 s (loads 12.49 31.49 26.03 → 9.66 28.99 25.31). Its history line names
   `model=one-loop-measured-v1`, and its `drain` line gives 192 one-loop and 11 gap, with ℓ_min = 0.0977756, as the
   map gives (192, 11, 0.0978).

**The re-verification after ruling B.**
- The batch again took 19.8 s, at loads 9.06 10.16 11.55 → 9.38 10.18 11.53. All seven outputs are byte-identical
  to the first batch's.
- The tool run took 28 s (9.38 10.18 11.53 → 9.08 10.05 11.44). Its `drain` line is identical, and every other line
  is identical but for the wall times (`out/compare_runs.txt`).
- `compare_maps.txt` and `l_form_probe.py`'s output are identical to the first run's.

## 5. The deliberate-breakage record

None was made. The two departures from the reference are each checked by the reference test, which counts them per
regime and requires the one-loop count to be zero. The refusal of a model without `_drain_L` is checked at all three
entry points, with `Radau`, `integrate_scalar_history`, `integrate_history` and the model's `seed` mocked and
asserted not called.

## 6. Observations not acted on

- **The fitted constants and S(L) are also measured on one family.** The one-loop and gap constants and the S table
  come from stage 1a and stage 2 on the exponential wall, at n = 1 (and for U15, n = 2). Moving L into the model class
  does not make them model-independent.
- **The ladder's 83 classical candidates now drain** A·R·S(L)·w_gate, 9.3 × 10⁻¹³ to 6.2 × 10⁻⁶ at A = 0.2. Each one
  takes the drain branch and its restart, where the old law took the α_eff = 0 branch. The ladder's map took 14.3 s.
  02d measures what this costs on the whole sample.
- **L underflows to 0.0 for x = ln E − ln V₀ below about −745,** and then the law's log L would raise. An assessed
  candidate has x > −28 (note §0), so it is not reached.
- **Crossover recorded no wall times for the four maps**, so the costs here have nothing to compare against there.
  02b's A = 0.05 runs under the old law took 711–942 s for the ladder's tunings. That is not comparable either: those
  were tunings, at another A.

## 7. State handed to the next prompt

For **02d** (re-measure the sample under the measured law):
- **The tool route.** `tools/history_and_bbn.py BETA M --drain-law one_loop --alpha A` (tuned by default;
  `--fixed-normalization NORM PHI_REF_MP` for one integration).
  - `--gap-prescription` is gone and is now an unknown argument, so 02's and 02b's task lists, which pass it, need it
    dropped.
  - The history line names `drain_law=one_loop A=<A> model=one-loop-measured-v1`.
  - The `drain` line keeps its fields: counts by regime, the K shares by regime and parking, ℓ_min,
    `unresolvable_assessments` and `unresolved_declined`.
- **The record.**
  - `DrainRecord(law, A, model, unresolvable_assessments, candidates, unresolved_declined)`, with `model`
    `"one-loop-measured-v1"` or `"fixed"`.
  - `DrainCandidate(regime, ell, alpha_eff, w_gate, log_E, log_H, L, log_R)`, with L and ln R the law's inputs (None
    where unassessed).
  - 03 stores the drain-model key from `DRAIN_MODEL_*`.
- **A model must define `_drain_L`** to run under the one-loop law. Otherwise the three entry points refuse it with a
  `ValueError` before integrating. `ExponentialModel` defines it, at every n.
- **The costs measured here** (one integration each, at the 02b normalizations, under load 9–43): P-2197 and W-2187 at
  A = 0.05, about 20 s; R-632 at A = 0.5, 5.5–5.7 s; the ladder at A = 0.2, 14.3 s; 2197 at A = 1, 3 s. One tool run
  including the PRyMordial solve took 27–28 s.
- **Classical candidates now drain** (decision 3). The ladder at A = 0.2 meets 83, each drained 10⁻¹² to 10⁻⁵ and
  each taking a restart. In crossover, R-632 at A = 0.1 met 3269 without parking. Crossover recorded no wall times for
  these maps.
- **The gap fraction is 0 where 1 + q ln L ≤ 0** (L ≤ 0.3045), by the user's ruling. No gap candidate of §2.3's runs
  reaches it (least L 2.578). The B group at M = 10⁻³³ M_P is where it is likeliest; 02d can count it from
  `DrainCandidate.L`.
- **From §2.3, for the sample:**
  - at their 02b normalizations, P-2197 and W-2187 park at A = 0.05 (N = 39.44 and 39.02), mostly after the gap's
    first bounces (11 and 22 gap candidates);
  - R-632 parks at A = 0.5 (N = 38.65, 3 gap candidates);
  - the ladder parks at A = 0.2 (N = 30.71, after 3 gap and 83 classical candidates);
  - crossover's floors stand: R-632 does not park at A = 0.2, and the ladder not at A = 0.1. These were not re-run
    here.
- **02's and 02b's scripts no longer run on this tree.** `.documents/one-loop-drain/law/`, `measure/` and
  `measure-02b/` name the removed symbols, and each folder's new README says so. 02d needs its own driver, or 02b's
  adapted in a new folder.
