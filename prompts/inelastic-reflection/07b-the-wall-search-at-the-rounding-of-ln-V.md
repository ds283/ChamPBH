# Prompt 07b — the wall search at the rounding of ln V

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-30, after 07 landed (`5397f8f`) and was reviewed (`0b8f420`). The user had asked
for prompt 08 ("Please write prompt 08"). The orchestrator's probes for 08 found dissipation mode
failing across twelve decades of M, and the user chose to insert this prompt first ("Insert 07b before
08"; README §8.1 R18). It is written from:
- the issue `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]` (board §3);
- log 04 §1 and §8 (the search and the methods), and log 05 §5.5 (where the issue was found);
- the orchestrator's probes of the same day
  ([`source/planning-probes-2026-10-30/`](source/planning-probes-2026-10-30/README.md) §1).

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.
**Gate:**
- on branch `inelastic-reflection`, at or after the commit that adds this prompt, with no tracked change in
  `git status`;
- no ChamPBH Ray process is running. You start none.

**Closes:** `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]`.
**Opens:** only what the work finds.
**Recommended model:** **Opus**. The change is a few lines in 04's generic search. Its width must be
derived, not chosen, and every history outside the band must stay bitwise 07's.

**Read first:**

1. [`README.md`](README.md):
   - §8.1, R9 (no closed forms of the potential) and R18 (this prompt);
   - §8.4's charter for 07b;
   - §2.1 (one prompt at a time) and §5 (the rules).
2. The board's entry for `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]`, with its
   narrowing by 06 and its measurement of 2026-10-30.
3. [`logs/04-the-wall-through-the-model.md`](logs/04-the-wall-through-the-model.md) §1 (the search) and
   §8; [`logs/05-unresolved-turnarounds.md`](logs/05-unresolved-turnarounds.md) §5.5.
4. `CosmologyConcepts/Models/AbstractModel.py`:
   - the constants `WALL_ROOT_RTOL`, `WALL_ROOT_XTOL`, `WALL_SEARCH_MAX_DOUBLINGS` and
     `WALL_SEARCH_HALVINGS` (:93–:105);
   - `_log_V_minus_floor` (:130), `_evaluate` (:137) and `_search_log_field` (:167);
   - `classical_turning_point` (:522), and `one_loop_turnaround`'s use of the helper (:665).
5. `ComputeTargets/ScalarHistory.py`: `assess_infall` (:1038) and the guard G2 in
   `_reflection_guards` (:902). Neither changes (§1.2).
6. `ComputeTargets/tests/test_wall_model.py`, `TestTheCases` (:600), in particular
   `test_5_a_bump_between_the_field_and_the_wall_is_refused` (:687), which must still pass.
7. The probes, [`source/planning-probes-2026-10-30/`](source/planning-probes-2026-10-30/README.md) §1:
   `fixprobe.py`, `band_shipped.out`, `band_fixed.out`, `bitwise.out`. They are references, not code to
   copy.

---

## 0. Where this prompt sits

### 0.1 What it is for

04's generic `classical_turning_point` searches for the root of ln(V − V_floor) = ln E inwards from the
field. It forms ln(V − V_floor) from ln V and ln V_floor (`_log_V_minus_floor`). Where V − V_floor is a
few ulps of ln V, the difference ln V − ln V_floor is quantized. On the exponential with ln V ≈ −276.6
the quantum is 5.68×10⁻¹⁴, so M/φ ≈ 3×10⁻¹⁴ lies a few quanta from zero. Two probes of the search can
then see the same residual, and the search raises "not monotone" (log 05 §5.5). At α > 0 the step loop
assesses the wall at every infalling step start, so the raise fails the history.

Log 05 found this at M = 10⁻³⁰, at the bottom of the cascades, after the point where 06's cut now parks
the field. The issue was narrowed when every history measured parked first. Those grids were
10⁻⁷–10⁻¹² and 10⁻³⁰ M_P.

### 0.2 What the orchestrator's probes found (`0b8f420`, α = 0.1)

- **The band.** With the shipped code every history fails, at its first infall or soon after, at:
  - β = 2: M = 10⁻¹³, 10⁻¹⁴, 10⁻¹⁵, 10⁻¹⁶, 10⁻¹⁸, 10⁻²⁰, 10⁻²², 10⁻²⁵ M_P;
  - β = 1.2 and 3: M = 10⁻¹⁶ and 10⁻²⁰.

  Each fails with "not monotone" at a φ with M/φ ≈ 3×10⁻¹⁴. β = 2 runs at 3×10⁻¹³ and at 10⁻²⁸
  (`band_shipped.out`, `peaks.out`).
- **A generic fix works.** `_log_V_minus_floor` was patched to return −∞ where
  ln V − ln V_floor ≤ K ulp(max(|ln V|, |ln V_floor|)), with K = 64. Then:
  - all twelve histories reach T_CMB;
  - they park at candidates 6, 48 and 58 (β = 1.2, 2, 3), at N = 28.364, 30.349 and 29.394, as every
    drained history of log 06 does (`band_fixed.out`).
- **It changes nothing outside the band.** β = 2 at M = 10⁻¹² and 10⁻³⁰, β = 5 at M = 0.5 and β = 1.2
  at M = 10⁻⁷ are equal with and without the patch. That covers every record, the assessments, the
  samples, the final state, `nfev`, `ts`, and the parking and release records (`bitwise.out`).
- **The band crosses the one-loop gap.** At β = 2 the candidates' ln 𝓡 spans −19.7 to +4.2 decades at
  M = 10⁻¹⁶. So histories there have generations with 1 < 𝓡 ≲ 10^2.9, where
  `[04-one-loop-chain-has-no-solution-just-above-R-equals-1]` says the one-loop chain has no solution.
  08 needs to know which (R19).

### 0.3 Why the fix is in the helper, and generic

- **−∞ already means "V rounds to V_floor".** The search allows it (repeated −∞ is monotone), and the
  halvings go on while the bracket's outer end is −∞. A difference within the rounding of ln V carries no
  more information than a zero one. Treating it as zero puts the search's outer probes in the regime it
  already handles.
- **The turning point does not move.** φ_t lies where ln(V − V_floor) = ln E. That is resolved unless E
  itself is within the rounding of V, and then no search could resolve it.
- **No closed form is needed** (R9). An `ExponentialModel` override with expm1 would fix one potential
  and leave the defect in every other.

---

## 1. What is wanted

### 1.1 The helper

`_log_V_minus_floor(log_V, log_V_floor)` returns −∞ where ln V − ln V_floor is within the rounding of
ln V. That is:

    ln V − ln V_floor ≤ K · ulp(max(|ln V|, |ln V_floor|)),

with K a module constant beside `WALL_SEARCH_HALVINGS`, with its comment. Elsewhere it returns exactly
the shipped expression, `log_V + log(-expm1(log_V_floor - log_V))`. Where ln V ≤ ln V_floor it returns
−∞, as now.

**K is derived in the log, not chosen.** The probe's 64 is an existence proof. The derivation has two
parts:
- **the rounding of the difference:** ln V and ln V_floor are each the model's floats, each within some
  ulps of its true value. Say how many for the exponential, where ln V = normalization + (M/φ)ⁿ, and what
  a generic model may add;
- **what the search must resolve:** the smallest change of ln(V − V_floor) between two probes the search
  compares. These are consecutive doubling probes, and a halving midpoint against its bracket's ends. A
  difference of j quanta has a relative rounding of about 1/j in V − V_floor, so its logarithm is noisy
  by about 1/j. That noise must be below the smallest change the comparisons rely on.

  Give the inequality, the K it gives, and why the probe's 64 does or does not satisfy it.

The comparison is ≤, so that a difference of exactly K quanta is −∞.

### 1.2 What does not change

- **`_search_log_field`, `_evaluate`, the constants of the search, and the methods' signatures.**
  `classical_turning_point` and `one_loop_turnaround` call the helper as now. The one-loop method's
  `outside_classical` (:665) sees −∞ < ln E, which is True: a turnaround where V rounds to V_floor is
  outside the classical one. Say so in the log.
- **`assess_infall`** (`ScalarHistory.py:1038`) forms ln E = ln(ρ_kin + V − V_floor) with its own
  expression. It is a sum, not a search, and it needs no width. Leave it unchanged, so that ln E stays
  bitwise. **G2** (`:902`) likewise.
- **ExponentialModel** and every other model: no override.
- **The integrator.** Nothing in `ComputeTargets/` changes but the tests.

### 1.3 The documents (P12)

- **`.documents/architecture-summary.md`:** a dated note beside 04's on the model methods. It covers:
  - the helper's width and why;
  - the band where the shipped search failed;
  - that −∞ is "V rounds to V_floor, or lies within its rounding".
- **`.documents/numerical-strategies.md`:** a dated subsection after 07's 6.6. It covers:
  - the quantization of ln V − ln V_floor;
  - K's derivation;
  - the band measured before and after.

### 1.4 The tests

A new module, `ComputeTargets/tests/test_wall_rounding.py`. No store and no Ray. Each test that
integrates gives its cost and the load in its docstring.
1. **The helper at its edge**, on synthetic floats. Take ln V_floor = the exponential's normalization
   at M = 10⁻¹⁶ (−276.63072869517…, `band_shipped.out`), and ln V = ln V_floor + j quanta, built with
   `nextafter`:
   - j = 0 … K returns −∞;
   - j = K + 1 returns the shipped expression bitwise;
   - ln V < ln V_floor returns −∞;
   - a difference of order 1 returns the shipped expression bitwise.
2. **`classical_turning_point` on the inputs it refused.** Take the twelve (model, normalization, φ,
   ln E) of `band_shipped.out`, at least the β = 2 ones at M = 10⁻¹³, 10⁻¹⁶ and 10⁻²⁵ and one each at
   β = 1.2 and 3. At each, it returns a turning point whose φ_t equals the exponential's closed form:

       M/φ_t = log1p(E/V₀),  V₀ = e^normalization, for n = 1.

   The tolerance is derived from `WALL_ROOT_RTOL`, `WALL_ROOT_XTOL` and the rounding of ln V. The test
   imports nothing from the probe.
3. **One history in the band, to T_CMB:** M = 10⁻¹⁶ M_P, β = 2, α = 0.1, `test_injection`'s setup.
   - It reaches T_CMB and `integrate_history` succeeds.
   - It parks at candidate 48, a floor reflection, as every β = 2 drained history does (log 06 §5.4).
   - Σ Δρ/ρ_R with the leftover equals log 06 §5.4's value at M = 10⁻³⁰ (0.012616907) to the M-dependence
     that log measured, 2×10⁻⁸ relative at β = 2. If it does not, give both and stop.
4. **What does not change.** These pass unedited:
   - the golden tests, `test_wall_model.py` (its bump test in particular), `test_first_bounce.py`,
     `test_surfing_turning_points.py`, `test_kinematic_cap_loop.py`, `test_unresolved_turnarounds.py`;
   - `test_injection.py`, `test_gate_and_taper.py`, `test_parking.py` and `test_release.py`.

**Each test is shown to catch a breakage** (P11). Record each breakage as a diff, with the tests it
fails; never commit one.
- B1: the shipped helper restored (items 1, 2, 3).
- B2: a width of one quantum, K = 1 (item 1; and item 2 or 3 if it fails there: say which).
- B3: < in place of ≤ (item 1).
- B4: the width applied in units of ulp(ln V − ln V_floor) instead of ulp(ln V), which is never true
  (items 1, 2).

---

## 2. Measurement

Recorded as tables in the log. Every history uses `test_injection`'s setup (n = 1, φ* = 5 M_P, π* = 0,
T* = 2×10⁴ GeV, to T_CMB, default tolerances, the model's seed realization, untuned), α = 0.1, unless
stated. No store, no Ray. The dispatch commit's histories are run in a temporary worktree of it, which
is removed afterwards.
1. **The band**, at the dispatch commit and at the final tree:
   - β ∈ {1.2, 2, 3} × M ∈ {10⁻¹³, 10⁻¹⁴, 10⁻¹⁵, 10⁻¹⁶, 10⁻¹⁷, 10⁻¹⁸, 10⁻²⁰, 10⁻²², 10⁻²⁵, 10⁻²⁷} M_P;
   - for each: the dispatch's outcome, with N and φ, M/φ and the residuals of the failing search; the final
     tree's outcome, where it parks, N_final, Σ Δρ/ρ_R with the leftover against the M = 10⁻³⁰ value, and
     the wall time.
2. **The band's edges** at β = 2, at the dispatch commit: the largest and the smallest M that fail, each
   bracketed to a quarter of a decade.
3. **The gap, for 08** (R19). For each history of §2.1 at the final tree, over its candidates with an
   assessment:
   - the range of ln 𝓡;
   - how many have 1 < 𝓡;
   - of those, how many `one_loop_turnaround(phi_assessed, log_E_assessed)` raises on, returns `None` on,
     or solves.

   For each, the share of K they carry. This is measured, not acted on. 08 uses it.
4. **What does not change**, at the final tree against the dispatch commit, bitwise: every record (the
   candidates' assessments included), the samples, the final state, `N_final`, `nfev`, `ts`, and the
   parking and release records. The histories:
   - log 05's grid: β ∈ {1.2, 2, 3} × M ∈ {10⁻⁷, 10⁻⁸, 10⁻⁹, 10⁻¹⁰, 10⁻¹², 10⁻³⁰} M_P;
   - M = 0.5 at β ∈ {1.2, 2, 5, 10, 25};
   - M = 10⁻³ at β ∈ {0.5, 1.2, 2, 3};
   - natural mode at M = 0.5, β = 2.
5. **Natural mode in the band:** M = 10⁻¹⁶, β = 2, α = 0, against the dispatch commit:
   - the values bitwise;
   - the warnings of both, listed. Natural mode's warning at the first floor reflection calls the wall
     search, so its text may change. That is expected. Say whether it did.
6. **The cost:** the helper per call, shipped against final (the minimum of at least 10⁵ calls), and one
   band history's wall time beside the M = 10⁻³⁰ one. Give the load.

---

## 3. Verification

1. **Before any edit:** the five suites, run as `CLAUDE.md` says, with ComputeTargets in its two halves.
   Expect 26 / 427 (96 + 331) / 103 / 115 / 64 (log 07 §8). A different count, or a failure, is a stop.
2. **After:**
   - the five suites. ComputeTargets rises by the new tests only, and the tests of §1.4 item 4 pass
     unchanged; say so;
   - `black --check` on the files you changed.
3. **The breakages** of §1.4, each with the tests it fails.

## 4. Acceptance

1. **§1 as written, and the gate "Band"** (README §8.4):
   - α = 0 bitwise;
   - every history of §2.4 bitwise the dispatch commit's;
   - every history of §2.1 reaches T_CMB at the final tree;
   - K derived.
2. **The measurement** of §2, as tables in the log.
3. **The log**, `logs/07b-the-wall-search-at-the-rounding-of-ln-V.md`, in the template of
   `dk-and-rpk-migration` README §5.1. It holds:
   - the derivation of K;
   - what −∞ now means, at each place the helper is used;
   - the suite counts before and after;
   - the breakages as diffs;
   - every deviation, classified;
   - "State handed to the next prompt", for 08:
     - the gap table of §2.3;
     - which histories cross the gap;
     - the band's edges;
     - the cost.
4. **The documents** of §1.3.
5. **The board:**
   - row 07b and the gate Band, in §1 and §2;
   - `### 5.9 After prompt 07b (<date>)` in §5;
   - `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]` moved from §3 to §4 with a
     "Closed" line naming this commit;
   - in the same commit, its row deleted from `.documents/OPEN_ISSUES.md` §1.19, with the count and
     the date corrected (`CLAUDE.md`);
   - anything opened, on §3 and in the index.
6. **One commit**, in `CLAUDE.md`'s form, staged by explicit path only.

## 5. Stop conditions: stop and ask

- A baseline count differs from §3 item 1, or an existing test fails before you edit.
- Either golden history changes, or any test of §1.4 item 4 needs an edit.
- A history of §2.4 is not bitwise the dispatch commit's.
- A history of §2.1 does not reach T_CMB at the final tree, for any reason.
- K cannot be derived: no width resolves the search's comparisons and leaves §2.4 bitwise.
- Item 3's Σ differs from the M = 10⁻³⁰ value by more than log 06's M-dependence.
- The work seems to need a change to:
  - `_search_log_field`, `_evaluate` or the search's constants;
  - a method's signature;
  - `assess_infall`, G2, or anything else in `ComputeTargets/`;
  - a model's override;
  - the store.

## 6. What this prompt does not do

- **No peak, no margin**: 08's. §2.3 measures the gap for it.
- **No change to the search's logic**, its constants or its failure texts. Only the helper's width.
- **No closed form** of any potential (R9).
- **Files it changes:**
  - `CosmologyConcepts/Models/AbstractModel.py`: the helper and its constant only;
  - `ComputeTargets/tests/test_wall_rounding.py` (new);
  - `.documents/architecture-summary.md` and `.documents/numerical-strategies.md`;
  - the log, the board and `.documents/OPEN_ISSUES.md`.

  Any other file is a deviation to classify.
