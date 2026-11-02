# Prompt 01 — the drain law

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-02, on branch `one-loop-drain` after `reflection-restart` was merged in (`1561b60`, U1) and the
user ruled the wall-search fix into this prompt (README §8.8, `0807902`). It is written from:
- README §8 whole, §8.8 in particular (the ruling and P21–P25);
- the drain probe: `source/drain-probe_2026-11-02.md`, `source/probe/drain/one_loop_drain.patch`, `drain_probe.py`
  and `results/`;
- `reflection-restart`'s record of the wall-search limit: its board §3.1, the entry
  `[00-fixed-alpha-laboratory-M-histories-fail-in-classical-turning-point]`, and
  `.documents/reflection-restart-verification.md` §3.2–§3.6, with the scripts in
  `.documents/reflection-restart/close/`;
- the merged tree's code, read for this prompt (§0.2).

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Where it runs.** In the worktree `/Users/ds283/Documents/Code/ChamPBH/.claude/worktrees/quanta-diagnostics`,
on branch `one-loop-drain`, from that worktree's root, with `PYTHONPATH=.` and the main checkout's interpreter
`/Users/ds283/Documents/Code/ChamPBH/venv/bin/python`. The worktree has no `var/`. Outputs go to the main
checkout's `/Users/ds283/Documents/Code/ChamPBH/var/one-loop-drain/01/`, and the 2026.8.0 store is read, read-only,
at `/Users/ds283/Documents/Code/ChamPBH/var/datastores/science-2026.8.0`. The main checkout is on
`reflection-restart` and is not touched.

**Gate:**
- on branch `one-loop-drain`, at or after the commit that adds this prompt, with no tracked change in
  `git status`; `1561b60` is an ancestor of `HEAD`;
- `git diff reflection-restart HEAD -- ComputeTargets CosmologyConcepts CosmologyModels ObjectFactories tools
  config RunDrivers main.py plot_by_beta.py plot_ScalarHistory.py` is empty (the merged tree's code is
  `reflection-restart`'s);
- `./venv/bin/python -m runregistrykit list`, run from the main checkout: no run `running`, stale or unknown;
- the store's shards exist, with 2197 in `science-2026.8.0-shard0005.db`, 2188 in `shard0003`, 3016 and 3233 in
  `shard0002`, 3873 in `shard0007`;
- `.documents/one-loop-drain-verification.md` does not exist.

If any of these fails, stop and ask.

**Closes:** `[00-fixed-alpha-laboratory-M-histories-fail-in-classical-turning-point]`, if §1.6's gate holds.
**Closes or narrows:** `[00-a-zero-drain-history-stops-at-the-inside-wall-guard]` (§1.7). **Opens:** only what the
work finds.

**Recommended model:** **Opus**. The law is a small change in a large step loop, the wall-search fix must provably
move nothing that succeeds today, and 3873 is an investigation.

**Read first:**
1. [`README.md`](README.md) §8 whole; §0.4; §6.3 (the rulings in the user's words).
2. The board's §3, every entry, and §5.
3. The drain probe: `source/drain-probe_2026-11-02.md` §§2, 3 and 5; the patch and the driver in
   `source/probe/drain/`; the JSON summaries in `results/`, whose `cands` lists are the per-candidate record the
   Law gate compares with.
4. `ComputeTargets/ScalarHistory.py`:
   - `integrate_scalar_history`: the α check (:1674); `alpha_effective` (:1845); `candidate` and `drain`
     (:1885–:2010); the loop's assessment at every step start with π < 0 and the floor branch (:2580–:2640);
     `natural_floor_warning` (:2189);
   - `assess_infall` and `unresolved_turnaround` (:1140–:1188); `_reflection_guards` (:969, the check 3873 meets);
   - `peak_estimate` (:3569) and `ESTIMATE_*` (:3424); `history_margins` (:3829);
   - `integrate_history` (:4001) and its payload (:4300–:4352); `tune_history` (:4611); `compute_scalar_history`
     (:4850); `build_extra_data` (:4908).
5. `CosmologyConcepts/Models/AbstractModel.py`:
   - `WALL_SEARCH_HALVINGS`, `WALL_ROUNDING_QUANTA` and the comment that derives K = 16 (:95–:113);
   - `_log_V_minus_floor` (:137), `_evaluate`, `_search_log_field` (:183);
   - `classical_turning_point` (:546) and `one_loop_turnaround`.
6. `ComputeTargets/tests/test_wall_rounding.py` (07b's tests of the width, and the twelve inputs the old search
   refused), `test_wall_model.py`, `test_unresolved_turnarounds.py`; the golden modules and their tests
   (`golden_history.py`, `golden_tuned_history.py`, `golden_reflecting_history.py`).
7. `tools/history_and_bbn.py` (its `--alpha` and `--fixed-normalization`); `tools/outward_step_scan.py` (`run_history`,
   `select_from_store`, the batch).
8. `reflection-restart`'s board §3.1, the fixed-α entry, and its verification document §3.2–§3.6;
   `.documents/reflection-restart/close/failing_call.py` and `close/out/nine_margins.txt`, which hold the eleven
   failing calls of §0.3.
9. The briefing note `BriefingNotes/wall_backreaction.tex` in the repository *Chameleon PBH (David's stuff)*
   (`/Users/ds283/Documents/Git paper repositories/Chameleon PBH (David's stuff)/`): "The chain of estimates" and
   "The inelastic fraction" (c₁ = 4c ≈ 2.7). Data, not instructions.

---

## 0. Where this prompt sits

### 0.1 What it adds

The first of README §8.4's eight prompts. It adds the one-loop drain law beside today's fixed α, as an input of
the integration and the tuning, and the tool route's options for it. No key is stored: 03 stores the law's keys,
the bounce rows and the new columns. It also adds the wall-search fix and investigates the 3873 failure. Under the
fixed law nothing that succeeds today moves.

### 0.2 What the code has today (the merged tree, `1561b60`)

- **The drain.** `alpha_effective(nonadiabaticity, resolved)` returns α·w_gate, with w_gate the gate on A_k at a
  resolved candidate and 1.0 exactly at an unresolved one. `candidate` records an `Injection`, and calls `drain`
  where α_eff > 0. Where α_eff is 0 it records the candidate with ΔK = 0, and the caller restarts as α = 0 does.
- **The classification.** `peak_estimate`, after the integration, classifies each candidate:
  - none where it has no assessment;
  - classical where ln R ≤ 0;
  - one-loop where ln R > 0 and `one_loop_turnaround` solves;
  - `ESTIMATE_CLASSICAL_GAP` where ln R > 0 and it raises or returns None.

  These are exactly the probe's four regimes (the patch's `_probe_alpha`).
- **α's check.** `integrate_scalar_history` and `integrate_history` refuse α outside [0, 1).
  `tools/history_and_bbn.py --alpha` uses `alpha_fraction`, with the same bound.
- **The probe's law** (`one_loop_drain.patch`) replaces α_eff at every candidate with
  min(0.99, A·2.7/ℓ), 0, or A·2.7/4, by regime. It drops the gate, which U9 keeps.

### 0.3 The wall-search limit (`reflection-restart` 04, verdict (ii))

`classical_turning_point` searches inwards in ln φ for the root of ln(V − V_floor) − ln E. `_log_V_minus_floor`
returns −∞ within K = 16 quanta of ln V_floor. Suppose ln E is below the least finite value that `_log_V_minus_floor`
can return. Then every finite residual is positive and the outer end stays −∞, so the search raises: "not monotone"
at the region's edge, or "the outer end is still −inf". Whether there is anything else to it is a fact §1.6 checks.

The step loop meets this at an infall's first step start near the apex. E/V_floor there is below about 10⁻¹², and
the field is far out in the tail (φ/M ≈ 7×10²³ at 2197's failure), where `unresolved_turnaround` would decide
nothing. Eleven failing calls are measured:
- 2197 and 2188 on the fixed tree, at 1.51 and 1.58 quanta;
- the nine "not monotone" failures of the 2026.8.0 store, at 0.11–16.54 quanta;
- eight of those nine again on the fixed tree, at 0.65–6.70 quanta.

The inputs are in `reflection-restart`'s `close/failing_call.py` and `close/out/nine_margins.txt`.

### 0.4 3873

3873 (M = 10⁻¹² M_P, β = 1.11) runs at its stored normalization with nothing drained (the one-loop law at A = 1,
on `2d50b95`). It stops at N = 43.76462032 in `_reflection_guards`: "reflection requested inside the wall",
W/(π²/2) = 1.485, φ_E = 1.6701×10⁻⁸, π_E = −0.0014122 (`results/3873-A1.log`). At its stored α = 0.03 it passes.
The check dates from `fc97233` (the integrator audit). Under the one-loop law every large-M history drains nothing,
so all of them take this path. How many stop is not known.

## 1. What is wanted

### 1.1 The law as an input (P22)

- `integrate_scalar_history`, `integrate_history` and `tune_history` take `drain_law`: `"fixed"` (the default) or
  `"one_loop"`, as module constants `DRAIN_LAW_FIXED` and `DRAIN_LAW_ONE_LOOP`.
  - Under the fixed law, `alpha` is α as today, with today's check.
  - Under the one-loop law, `alpha` carries A, which must be finite and > 0. A ≤ 0, NaN or ±∞ is a `ValueError`
    before any integration. A = 0 is natural mode and is asked for as the fixed law at α = 0.
  - Any other `drain_law` is a `ValueError`.
- Every α > 0 code path runs unchanged under the one-loop law: assessments, candidates, the gate, parking and the
  release. Only α_eff differs (§1.2).
- `compute_scalar_history`, `ScalarHistory.compute`, `main.py` and the plot scripts are **not** changed. They have no
  way to ask for the one-loop law, so no one-loop history can be stored under a fixed-law key until 03 and 04.

### 1.2 The regime in the loop, and α_eff by regime (README §8.2, U9, U10, P18, P23, P24)

Under the one-loop law, each candidate is classified in `candidate`, before α_eff, from the assessment it is given.
The classification is `peak_estimate`'s, computed once:

| regime | when | α_eff |
|---|---|---|
| `unassessed` | the candidate has no assessment | 0 |
| `classical` | ln R ≤ 0 | 0 |
| `one_loop` | ln R > 0, `one_loop_turnaround(φ_assessed, ln E_assessed)` returns a solution with ℓ > 0 | min(α_cap, A·c₁/ℓ)·w_gate |
| `gap` | ln R > 0, and it raises `ComputationFailureError`, returns None, or returns ℓ ≤ 0 (P24) | by the gap prescription (§1.3), times w_gate |

- w_gate is today's: the gate at a resolved candidate, 1.0 exactly at an unresolved one.
- **The constants** sit beside the margin constants, each with its source quoted (P17, P23): c₁ = 2.7 exactly
  (`wall_backreaction.tex`, "The inelastic fraction": 4c, c ≈ 0.68, written 2.7 there); α_cap = 0.99 (U10, since
  the drain refuses α_eff ≥ 1); ℓ_edge = 4.
- **The one-loop solve runs once** per candidate (P18). Under the one-loop law the post-integration `peak_estimate`
  reuses the candidate's result (its estimate, ln k_ex, ln m and reason) instead of calling `one_loop_turnaround`
  again. The result must be identical to what `peak_estimate` would compute; a test shows it.
- Under the fixed law `peak_estimate` runs as today, and `alpha_effective` is unchanged.

### 1.3 The gap prescription (README §8.2, P20, P25)

- A module attribute `_GAP_PRESCRIPTION` in `ComputeTargets/ScalarHistory.py`, read at call time, set to `"zero"`
  (the default, which drains 0) or `"edge"` (min(α_cap, A·c₁/ℓ_edge)). Any other value is a `ValueError` at the
  first gap candidate.
- The attribute is temporary: 03 fixes the prescription as the user rules (U13) and removes it. Its comment says
  so, citing README §8.2. The precedent is `reflection-restart` 01's `_PROBE_OUTWARD_FIX`.
- The prescription's name is recorded in the payload (§1.4), so that 02's two runs of one history are told apart
  without the attribute.

### 1.4 The `"drain"` payload key (README §8.4, row 01)

`integrate_history`'s payload gains one key, `"drain"`, a record of plain values (floats, ints, strings, None, and
tuples of them, so that it crosses Ray and pickles):
- `law`: `"fixed"` or `"one_loop"`;
- `A`: A under the one-loop law, None under the fixed law;
- `gap_prescription`: the attribute's value under the one-loop law, None under the fixed law;
- `unresolvable_assessments`: §1.6's count, under either law;
- `candidates`: under the one-loop law, a tuple in candidate order, one record per `Injection`, of `regime`, `ell`
  (one-loop only, else None), `alpha_eff`, `w_gate`, `log_E` (the assessed impact energy, None if unassessed) and
  `log_H` (ln H_E before the candidate). Under the fixed law it is None: no instrumentation there (ruling 2).

A failure payload carries `"drain"` as far as the integration reached, or None if it failed before the loop.
`build_extra_data` is **not** changed, so `extra_data` and every stored value are as today. The tuning returns the
accepted integration's `"drain"`.

### 1.5 The tool route (P25)

`tools/history_and_bbn.py` gains:
- `--drain-law {fixed,one_loop}`, default `fixed`. Under `one_loop`, `--alpha` is A and is checked as §1.1 says,
  not by `alpha_fraction`.
- `--gap-prescription {zero,edge}`, accepted under `one_loop` only, which sets `_GAP_PRESCRIPTION` in its own
  process before the integration. It is temporary, removed by 03 with the attribute.
- Under `one_loop`, its history line names `drain_law=one_loop A=… gap=…`, and a `drain` line gives the counts by
  regime, the K share drained in each regime, ℓ_min and `unresolvable_assessments`. Under the fixed law every line
  it prints is as today: the existing tests that pin its output pass unchanged.

`tools/outward_step_scan.py` is not changed. Under the fixed law it is the replay tool of §2.1.

### 1.6 The wall-search fix (README §8.8, P21; the user's ruling)

- **The predicate.** A module-level function in `AbstractModel.py`, beside `_log_V_minus_floor`:
  `_turning_point_unresolvable(log_V_floor, log_E)`. It is True iff ln E is below the least finite value that
  `_log_V_minus_floor(·, log_V_floor)` can return.
  - **Exact by construction:** compute that least value from `_log_V_minus_floor` itself. Step up from ln V_floor
    through representable floats (`math.nextafter`) to the first argument with a finite result. That takes at most
    `WALL_ROUNDING_QUANTA` + 2 steps.
  - **Cached** per ln V_floor (a history has one), so that its cost is negligible: §2.1 measures it.
  - Its docstring states the argument for why every input it accepts raises in the search today.
- **The rule.** In `classical_turning_point`, after the existing `r_0 >= 0` test and before the search: if the
  predicate holds, return None. The docstring's "Returns None" paragraph gains this second case: the turning point
  lies inside the region the search reads as −∞, below the resolution of ln V, and cannot be resolved.
- **The count.** In `integrate_scalar_history`, each assessment whose turning point is None while the predicate
  holds adds one to `unresolvable_assessments` (§1.4). That covers the loop's assessments at every step start with
  π < 0, and natural mode's one assessment. Nothing else changes in the loop: a None turning point already makes no
  decision, and the infall's latest assessment stands.
- **Every caller is read.** The log lists each caller of `classical_turning_point` and `assess_infall`, what it did
  with the raise and what it does with None.
  - Natural mode's `natural_floor_warning` caught the raise as `WARNING_WALL_UNASSESSED`. It now takes φ for φ_t.
    The log says whether this can change a history that succeeds today, and shows it on a constructed case.
- **The search itself, `_search_log_field`, and `WALL_ROUNDING_QUANTA` are not changed.**
- **Not chosen** (P21): a closed-form hook for ln(V − V_floor), which moves every wall search's digits; reporting a
  root inside the −∞ region, which changes the shared search. If the work shows P21's form cannot meet §2.1's
  gate, stop and ask.

### 1.7 3873 (`[00-a-zero-drain-history-stops-at-the-inside-wall-guard]`)

Reproduce the stop on this tree under the one-loop law at A = 1, at the stored normalization. Then find which branch
asks for the reflection (unresolved or floor) and the assessment it was judged on. Show why the field is inside the
wall there, with W/(π²/2) > 1, when the same infall at α = 0.03 is not. Bound the work at a few hours of agent
time.
- **If the cause is a defect of this code with a fix inside this prompt's files** that moves nothing under the fixed
  law (§2.1), make the fix. Close the issue, and run 3873, 3016 and 3233 through it.
- **Otherwise stop and ask**, with what was measured. Not fixed unasked are: a change to G2 or its tolerance, to
  K = 3 or the φ_t threshold of the unresolved turnaround, to the floor rule, or to the parking cut. Each would be a
  ruling.
- Whatever is unsettled at the bound is recorded on the issue, narrowed, with the evidence.

### 1.8 Tests

New modules in `ComputeTargets/tests/` (and the tool's in `tools/tests/`). No Ray and no persistent store. A test
that runs a whole history says so and its cost in its docstring.
- **The law's inputs:** the checks of §1.1, under both laws, at each of the three entry points.
- **The regime and α_eff**, on synthetic assessments: each row of §1.2's table, the cap, w_gate's two cases, ℓ ≤ 0,
  and both gap prescriptions, with `_GAP_PRESCRIPTION` read at call time.
- **The fixed law is unchanged:** `alpha_effective` and the payload (every key but `"drain"`) on a short reflecting
  window, as before, and `"drain"`'s fixed-law form.
- **P18's reuse:** under the one-loop law, `peak_estimate`'s reuse equals a fresh call, for each estimate kind.
- **The predicate and the rule:**
  - at the boundary: the last ln E for which it is True and the first for which it is False, at laboratory M and at
    10⁻¹² M_P normalizations;
  - where it is True, `_search_log_field` on the same residual raises, called directly;
  - where it is False, `classical_turning_point` returns what it returned before, bitwise. A test compares it with a
    copy of the pre-fix method kept in the test module.
- **The eleven calls of §0.3:** each returns None now, and each raises with the pre-fix copy.
- **The tool's options,** as `tools/tests` tests the tool's other options.

## 2. The Law gate (README §8.5)

### 2.1 Under the fixed law

1. **The goldens:** `test_golden_history`, `test_golden_reflecting_history` and `test_tuning` (in
   `ComputeTargets/tests/`), and `ObjectFactories/tests/test_store_fingerprint_golden.py`, pass bitwise, unchanged.
2. **A replay.** `reflection-restart` 04's 42 tasks (`/Users/ds283/Documents/Code/ChamPBH/var/reflection-restart/04/tasks-42.json`)
   are run through the scanner on this tree, detached (`--jobs 6`, `--timeout 1800`).
   - Each is equal to its line in `the-42.jsonl` in outcome, both SHA-256 digests, `n_samples`, `accepted_steps`,
     `RHS_evaluations`, `reflections` and `restart_retakes`. 4389 fails with the same reason.
   - A difference is a stop.
3. **The fix answers the eleven calls,** and the predicate holds at each, at its stored or recorded inputs, by a
   script of `.documents/one-loop-drain/law/`.
4. **2197 and 2188 at their stored normalization, α = 10⁻³:** they now complete.
   - Record for each: wall time, `unresolvable_assessments`, reflections, the parking N, r_χ and ΔN_eff today.
   - Record the same numbers from the store beside them, as a measurement, not a bound. F3 moved these histories,
     so they are not expected to be bitwise.
   - A failure is recorded with its reason. If it is the wall search again, that is a stop.
5. **The nine** of `reflection-restart` 04 §0.2, tuned, under the fixed law, detached, with `--timeout 3600`: each
   outcome, and for a failure its reason. The three at α = 10⁻³ may take an hour each if their tuning now proceeds.
   A timeout is recorded with its progress and is not rerun. None of these is a stop, except a wall-search failure.
6. **The cost of the predicate:** its share of 2197's wall time, from a profile or a timed count. Over 1% is a stop.

### 2.2 Under the one-loop law

1. **The probe's table.** 2197 and 2188 run at their stored normalization at A = 1 and A = 0.1, gap `zero`.
   - First with `gate_weight` patched to return 1.0, in a script's own process, which is the probe's law. Each
     candidate's (N, α_eff, ΔK) must equal the probe's `cands` bitwise, and the drain count, the parking N, r_χ today
     and ΔN_eff today must equal the probe's JSON. A difference is a stop, with the first differing candidate.
   - Then on the production code (U9, gate kept). The differences from the probe are stated: each candidate whose
     α_eff differs, with its `resolved` and w_gate, and the summary numbers. README §0.1 says every laboratory-M
     turnaround is a floor reflection with w_gate = 1; this measures it.
2. **Both prescriptions.** 2197 at A = 1 with gap `edge` equals gap `zero` (the probe: no gap candidate). Its
   `"drain"` names the prescription.
3. **Large M at A = 1.** 3016 and 3233 drain nothing: every α_eff is 0. Their reflection counts (330 and 301 in the
   probe), parking or not, and r_χ today, beside the probe's.
4. **3873:** §1.7's outcome.
5. **The `"drain"` key** of each run: its candidates in order, one per `Injection`; its regime counts; and its
   regime agreeing with `peak_estimate`'s for every candidate.

## 3. What is not wanted

- **No stored key, column, table or label.** That means no `drain_law` on `alpha_value` (03), no
  `ScalarHistoryBounce` (03), and no change to `build_extra_data`, the factories, `config/`, `main.py` or the plot
  scripts. `VERSION_LABEL` stays 2026.9.0, `FINGERPRINT_FORMAT` 6 and the stepper label as it is.
- **No change to the search** (`_search_log_field`), `WALL_ROUNDING_QUANTA`, `WALL_SEARCH_HALVINGS`, F3 or the
  guard, the gate, the unresolved turnaround's constants, the floor rule, G1/G2 or the parking cut. The one
  exception is a fix of §1.7 that meets its conditions.
- **None of the instrumentation of README §8.3** beyond the `"drain"` key, and no derived measure (05).
- **No store is written** but a test's temporary one. No Ray and no cluster.
- No merge and no push. Nothing in the main checkout but reading `var/` and writing under
  `var/one-loop-drain/01/`.
- `PRyM/`, `thirdparty/` and the packages are not touched.

## 4. Verification

- **The suites, before anything else**, per `CLAUDE.md` (ComputeTargets in two halves; tools alone), with counts,
  times and `uptime`. They must stand at 26, 514 (122 + 392), 130, 147 and 68, all passing; a count that differs is
  a stop. Run them again after the change. A count may rise only by this prompt's new tests, named in the log.
- **The runs.** Each batch or script is detached (`nohup`, pid recorded), checked once that it started, and waited on
  by a background command that ends with it, not by polling. Expected costs:
  - the replay, about 7 min at six workers (`reflection-restart` 04);
  - 2197 and 2188 under the fixed law, about 8 min each;
  - the one-loop runs, 2–40 s each (the probe);
  - the nine, up to an hour or more.

  At most eight workers run at once over everything; the machine is shared. Outputs go under
  `/Users/ds283/Documents/Code/ChamPBH/var/one-loop-drain/01/`, and probes and suite outputs in the agent's own
  subdirectory of the session scratchpad. Nothing goes in `/tmp`. No run outlives the prompt.
- **Reproducibility.** Every number of the verification document comes from a script in
  `.documents/one-loop-drain/law/`, with its output in `law/out/` (text only), or from a JSONL line under
  `var/one-loop-drain/01/` that the document names.

## 5. The commit, the log and the board

One commit (invariant 1), with:
- the code: `ComputeTargets/ScalarHistory.py`, `CosmologyConcepts/Models/AbstractModel.py` and
  `tools/history_and_bbn.py`; the new test modules; any existing test whose pinned payload keys must follow the new
  key, each named in the log;
- `.documents/one-loop-drain/law/` and its `out/`;
- `.documents/one-loop-drain-verification.md` (new), §1 holding §2's gate item by item, §1.6's evidence and §1.7's
  findings;
- dated notes in `.documents/numerical-strategies.md` (the law, the constants, the predicate) and
  `.documents/architecture-summary.md` (the `drain_law` argument and the `"drain"` key);
- `CLAUDE.md`'s test counts;
- the log `logs/01-the-drain-law.md` (template: `dk-and-rpk-migration` README §5.1), with every deviation classified,
  "Observations not acted on", and "State handed to the next prompt" (what 02 needs: the options, the payload, the
  costs, and anything of §2 that bears on 02's sample);
- the board: row 01, its items, the Law gate, §3/§4 for the two issues, and **Last updated**;
  `.documents/OPEN_ISSUES.md` in the same commit.

Format the Python files you change with `black`. Stage by explicit path, and show `git diff --cached --name-status`
before committing. The commit message follows `CLAUDE.md` invariant 5. Write "this commit" where the SHA goes; the
orchestrator substitutes it.
