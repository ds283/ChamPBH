# Log 02 — measure the law

**Subject:** Measure the one-loop drain law across U11's sample
**Commit:** `3f3f0cf`, on branch `one-loop-drain`, on top of `b1d6bc5`
**Date:** 2026-11-03, by Claude Opus 5.5
**Result:** **COMPLETE.** The Measure gate holds (`.documents/one-loop-drain-verification.md` §2.10). U13 and U14 are
open for the user, with the measurements of verification §2.9.
- **The sample.** The thirty histories of U11 were run by the tool route, tuned, at A ∈ {1, 0.1, 0.01} and under both gap
  prescriptions: 180 tasks. Of these, 110 were run (81 succeeded, 29 failed, none timed out), 65 were derived by S1 and
  5 by S2. The spot checks of both shortcuts are bitwise, and so is ruling 1's free check of S2 on five histories. The
  driver agrees with `tools/history_and_bbn.py` on both tool checks.
- **The law across the grid.**
  - At laboratory M and in block B at A = 1 and 0.1, the cascade is one-loop only and parks (β 1.06–10), with
    1 − 3.9 × 10⁻² or more of K drained one-loop.
  - At A = 0.01 every transition-window history at laboratory M reaches the gap and R ≤ 1, and its tuning fails: by the
    step budget, or because the effective minimum is not found after the seed integration.
  - The ladder enters the gap from 5 × 10⁻²⁰ M_P (A ≤ 0.1) to 5 × 10⁻¹⁶ M_P, and 4 × 10⁻¹⁵ M_P starts at R ≤ 1.
  - At 10⁻¹² M_P nothing drains at any A.
  - U9: no one-loop candidate has w_gate < 1. P19: no candidate is unassessed.
- **The prescriptions** differ only where the gap is entered. Where both succeed (the ladder) `edge` raises ΔN_eff today
  by up to 0.0995. At laboratory M no pair could be compared.
- **Cost:** 1 h 58 min from the first launch to the last end, at most six workers; the projection was 2.20 h.
- **Issue opened:** `[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]`. Index: 31 open.
- **Suites** before anything else: 26, 566 (163 + 403), 130, 156, 68, all passing. After: the same counts, all passing.

The orchestrator's note for this prompt (§0, rulings 1–10 and the Conventions) governed where it differs from the
prompt. Each ruling is classified in §2.

## 1. What shipped

No code outside `.documents/one-loop-drain/measure/` changed (prompt §1.5).

- **`.documents/one-loop-drain/measure/`**, each script with its docstring and its command:
  - `tasks.py`: §1.1's table, checked against the store (`mode=ro`), written as `var/one-loop-drain/02/tasks.json` (180
    tasks);
  - `fixed_law.py`: the fixed-law references, every validated success at each stored (M, β) of §1.1 in the main store's
    shards, with its BBN row (261 in all);
  - `run_sample.py`: the driver (rulings 2–5). The parent runs a batch of task ids at a given worker count and reaps
    each child by `os.wait4`. The child calls the tool's `main` under the three captures of ruling 3, and streams each
    integration to a partial file (ruling 4). A `tool:` line runs the CLI itself (ruling 9);
  - `common.py`: the S1 and S2 premises, and the comparison under ruling 6;
  - `decide.py`: ruling 1's order. It covers the batches' id files, S2 with its free check, S1, ruling 5's projection,
    and the derived lines;
  - `spot_check.py` and `tool_check.py`: §1.2's spot checks and §1.3's tool checks;
  - `analyse.py`: the Measure gate's tables (§2.1–§2.5), and a table for U14 (§2.6).
- **`measure/out/`** (text only): `tasks.txt`, `fixed_law.txt`, `decisions.txt`, `derive.txt`, `spot_check.txt`,
  `tool_check.txt`, `sample.txt`, `regimes.txt`, `spread.txt`, `fixed.txt`, `cost.txt`, `u14.txt`.
- **`.documents/one-loop-drain-verification.md` §2**, added after §1, which is unchanged.
- **The board**: row 02, three items, the Measure gate, U13 and U14 marked open with 02's measurements, the new §3
  issue, §5 and **Last updated**. **`.documents/OPEN_ISSUES.md`**: the new row under §1.23, and the count, 31.

Not committed: everything under `var/one-loop-drain/02/`, namely:
- `sample.jsonl` (180 lines) and `spot.jsonl`;
- `tasks.json` and `fixed_law.json`;
- `tasks/`, with each task's stdout, stderr, partial file and result;
- `batches/`, with each batch's `.ids`, `batch.pid`, `batch.log` and `batch.end`.

## 2. Deviations from the prompt

**The orchestrator's rulings**, each as its note classes it:
1. **The order of §1.4, made definite** — *IMPLEMENTATION CHOICE*, ruled; followed:
   - zero at A = 1 then 0.1 for all 30;
   - the free S2 check (five histories, all bitwise);
   - S1 and the `edge` runs at A = 1 and 0.1;
   - P's two alone at A = 0.01 on two workers, then the projection;
   - R, B, W, L at A = 0.01 less S2's, with group C S2's;
   - the `edge` runs at A = 0.01; then the spot and tool checks.

   See D2 for one overlap.
2. **Detached, not registered** — *STRUCTURALLY REQUIRED*. Each batch was launched with `nohup` and its pid written to
   `batch.pid`, with its stdout in `batch.log` and `batch.end` written at its end. Each was checked once and waited on by
   a command that ended with it. See D1 for where these files sit.
3. **The driver runs the tool's own `main`** — *IMPLEMENTATION CHOICE*, ruled; followed. All three captures were made as
   ruled, so the fallback (composing the stages) was not needed. The module must be taken as
   `importlib.import_module("ComputeTargets.ScalarHistory")`, as the tool takes it. `ComputeTargets/__init__.py` exports
   the class `ScalarHistory` under the module's name, and the first test of the child failed on it with an
   `AttributeError`, before any batch ran.
4. **Each integration is streamed** — *STRUCTURALLY REQUIRED*; followed. No task reached the 3600 s timeout. The path
   was exercised once before the batches, on 3873 with the limit set to 8 s through `ONE_LOOP_DRAIN_02_TIMEOUT_S` (D6).
5. **Memory, workers and the projection** — *IMPLEMENTATION CHOICE*, ruled; followed. Six workers at A = 1 and 0.1. P's
   two at A = 0.01 peaked at 1.307 and 1.322 GiB, so min(6, ⌊8/1.322⌋) = 6 workers. The projection was 52 × 912.1 s / 6 =
   7905 s = 2.20 h, under 4 h, so it did not stop.
6. **"Bitwise", made exact** — as the prompt intends; followed. The peak RSS, the tuning's and the BBN stage's own wall
   times, and PRyMordial's time are also left out of the comparison, as wall times are, since they vary from run to run
   (D7).
7. **The R·S(L) bound** — *IMPLEMENTATION CHOICE*, ruled; followed. The r_χ form was checked on `P-2197-A1-zero`: the
   relative difference is −2.95 × 10⁻¹⁴.
8. **The fixed-law references from the main store only** — as the prompt intends; followed.
9. **The tool check** — as the prompt intends; followed. Both tasks were byte-equal after masking, and their values
   equal the driver's.
10. **No file but these** — *STRUCTURALLY REQUIRED*; followed (§1, and the staged list in §4).

**The agent's own deviations:**
- **D1 — the batch files sit in `var/one-loop-drain/02/batches/<step>/`**, one directory a batch. Ruling 2 names
  `var/one-loop-drain/02/batch.pid`, `batch.log` and `batch.end`, but there were six batches. *IMPLEMENTATION CHOICE*.
- **D2 — step 1b began before step 1a ended.** 3873's two zero tunings took about 30 min each (16 integrations, §5).
  The other 29 histories' S2 and S1 were decided once their zero runs had ended, and step 1b's eight `edge` runs started
  on 4 workers while 3873 ran, 6 workers in all. 3873 was decided after step 1a ended (`decide.py step1b … 'C-3873'`),
  and it needed no `edge` run, since S1 holds. No decision depends on 3873's lines but 3873's own. *IMPLEMENTATION
  CHOICE*, to keep within U11's bound.
- **D3 — S2's spot check on 3233 at A = 0.1 is the step-1 run itself.** Ruling 1 runs the zero pass at A = 0.1 for every
  history, so there is no derived task to spot-check there. The comparison (bitwise, three integrations, 903 candidates)
  is the free check's. *STRUCTURALLY REQUIRED* by ruling 1.
- **D4 — a derived line carries a copy of its source run's values**, with `obtained` (S1 or S2), `equals` (the task
  that its own shortcut equates it with) and `values_from` (the run). The record's `A` or `gap_prescription` in the
  copy is the source's: the one value that differs by construction. *IMPLEMENTATION CHOICE*.
- **D5 — `out/decisions.txt` was regenerated at the end**, by `decide.py` re-run on the final `sample.jsonl`, with its
  id files sent to the scratchpad. All of them equal the id files the batches ran. The first record was cut short when a
  `tee` into it was piped through `head`. The record as launched is in the agent's scratchpad (`redo/`). *UNINTENDED
  DRIFT*, corrected.
- **D6 — two pieces of test scaffolding.**
  - `run_sample.py` reads `ONE_LOOP_DRAIN_02_TIMEOUT_S` (default 3600), so that the timeout path could be tested. No
    batch set it.
  - Before the batches, one child (`P-2197-A1-zero`), a two-task batch with a tool check, and the 8 s timeout on 3873
    were run under the tag `test-`. Their files were deleted before step 1a.

  *IMPLEMENTATION CHOICE*.
- **D7 — the comparison leaves out `peak_rss`, `maxrss`, `tuning_wall` and the BBN stage's `wall`**, beside the wall
  times ruling 6 names. *IMPLEMENTATION CHOICE*.
- **D8 — `analyse.py` gives more than §2 lists:**
  - `u14.txt`, a per-history table for U14;
  - the regimes met in failed integrations;
  - the gap candidates with w_gate < 1, the α_eff extremes, and the one-loop candidates after the first gap and after
    the first classical.

  *IMPLEMENTATION CHOICE*.
- **D9 — `black` was run on the scripts after the batches had run.** Every output was then re-made and compared: each
  file of `out/` but `decisions.txt` and `derive.txt` is byte-equal, and so is `tasks.json`. `derive` appends, so it
  was not re-run. *IMPLEMENTATION CHOICE*.
- **D10 — a stray file in `/tmp`.** A shell line wrote `/tmp/x` (a list of task names, from a pipeline). It was deleted
  at once and read by nothing. *UNINTENDED DRIFT*, corrected.
- **D11 — the suites after the batches ran as one background chain**, still serially and in the same halves, waited on
  by its notification. *IMPLEMENTATION CHOICE*.

## 3. The run

The batches, from `batches/*/batch.log` (verification §2.3):

| batch | tasks | workers | wall | loads start → end |
|---|---|---|---|---|
| step1a | 60 | 6 | 2155.6 s | 4.82 8.03 13.35 → 6.22 8.54 10.37 |
| step1b | 8 | 4 | 964.5 s | 7.49 9.16 10.70 → 5.86 8.02 10.04 |
| step2a | 2 | 2 | 914.1 s | 6.03 7.93 9.96 → 5.14 5.26 6.76 |
| step2b | 23 | 6 | 1856.5 s | 4.89 5.20 6.71 → 5.35 11.50 12.67 |
| step3 | 17 | 6 | 1934.9 s | 5.29 11.08 12.49 → 10.34 11.21 12.00 |
| step4 | 5 | 5 | 46.2 s | 7.90 10.59 11.76 → 6.21 9.82 11.42 |

- **Outcomes.** 110 run, of which 81 succeeded and 29 failed: 18 by the step budget and 11 by "the effective minimum was
  not found". None timed out. 70 were derived, 65 by S1 and 5 by S2.
- **Stop conditions.** None of the failures is the wall search or the inside-wall guard, and none points at
  `drain_regime` or `drain_alpha_effective`, so none is a stop.
- **Workers and memory.** At most 6 children ran at once. The peak RSS of a task was 1354 MiB, for a task that reaches
  the step budget, and about 250 MiB otherwise.
- **Nothing outlives the batches.** Every `batch.pid` fails `kill -0`, and no `run_sample.py` process is alive.
  `runregistrykit list` is unchanged at 23 runs, all finished.

## 4. Verification performed

- **The Gate**, again at `b1d6bc5` before anything ran: it held (verification §2.1).
- **The suites before** (14:26–14:43 UTC, serially; the `test_[g-z]` half and `tools/tests` in the background):

  | suite | count | time | uptime at start |
  |---|---|---|---|
  | CosmologyModels | 26 OK | 67.7 s | load 12.99 17.13 19.42 |
  | ComputeTargets `test_[a-f]` | 163 OK | 139.1 s | 20.74 18.85 19.88 |
  | ComputeTargets `test_[g-z]` | 403 OK | 483.8 s | 7.67 14.01 17.75 |
  | ObjectFactories | 130 OK | 83.5 s | 5.71 13.33 17.39 |
  | tools | 156 OK | 162.9 s | 4.64 10.97 16.07 |
  | RunDrivers | 68 OK | 23.6 s | 6.33 9.12 14.36 |

- **The suites after** (one serial background chain, 16:43–16:58 UTC, after every batch had ended):

  | suite | count | time | load at start |
  |---|---|---|---|
  | CosmologyModels | 26 OK | 63.0 s | 3.84 8.41 10.75 (16:43 UTC) |
  | ComputeTargets `test_[a-f]` | 163 OK | 136.2 s | 2.90 7.26 10.15 |
  | ComputeTargets `test_[g-z]` | 403 OK | 474.2 s | 14.77 10.64 11.03 |
  | ObjectFactories | 130 OK | 87.2 s | 3.63 7.86 9.72 |
  | tools | 156 OK | 162.5 s | 15.08 10.53 10.55 |
  | RunDrivers | 68 OK | 22.9 s | 9.67 10.45 10.54 (load at the end 17.21 11.94 11.06) |

  The counts equal those before: 26, 566 (163 + 403), 130, 156, 68.

- **The tool checks** (`out/tool_check.txt`): 13/13 and 15/15 lines byte-equal after masking, and 22/22 values each.
- **The spot checks** (`out/spot_check.txt`): S1 on `P-2197` at A = 1 and `R-865` at A = 0.1, and S2 on `C-3016` at
  A = 0.01 and `C-3233` at A = 0.1. All four are bitwise equal, including the full regime sequences and the injections'
  triples from the partial files.
- **Ruling 1's free check of S2** (`out/decisions.txt`): L-M5e-16, L-M4e-15, C-3016, C-3233 and C-3873 at A = 0.1 against
  A = 1. All are equal.
- **The outputs after black** were re-made and are byte-equal (D9).
- **`git diff --cached --name-status`** before the commit:

  ```
  M	.documents/OPEN_ISSUES.md
  M	.documents/one-loop-drain-verification.md
  A	.documents/one-loop-drain/measure/analyse.py
  A	.documents/one-loop-drain/measure/common.py
  A	.documents/one-loop-drain/measure/decide.py
  A	.documents/one-loop-drain/measure/fixed_law.py
  A	.documents/one-loop-drain/measure/out/cost.txt
  A	.documents/one-loop-drain/measure/out/decisions.txt
  A	.documents/one-loop-drain/measure/out/derive.txt
  A	.documents/one-loop-drain/measure/out/fixed.txt
  A	.documents/one-loop-drain/measure/out/fixed_law.txt
  A	.documents/one-loop-drain/measure/out/regimes.txt
  A	.documents/one-loop-drain/measure/out/sample.txt
  A	.documents/one-loop-drain/measure/out/spot_check.txt
  A	.documents/one-loop-drain/measure/out/spread.txt
  A	.documents/one-loop-drain/measure/out/tasks.txt
  A	.documents/one-loop-drain/measure/out/tool_check.txt
  A	.documents/one-loop-drain/measure/out/u14.txt
  A	.documents/one-loop-drain/measure/run_sample.py
  A	.documents/one-loop-drain/measure/spot_check.py
  A	.documents/one-loop-drain/measure/tasks.py
  A	.documents/one-loop-drain/measure/tool_check.py
  M	prompts/one-loop-drain/IMPLEMENTATION_STATE.md
  A	prompts/one-loop-drain/logs/02-measure-the-law.md
  ```

## 5. The deliberate-breakage record

None: this prompt changes no code. The checks that could fail did run on real data:
- the spot checks and the free S2 check (nine comparisons);
- the tool checks;
- the store check in `tasks.py`, which exits 1 on a mismatch.

The comparison of `common.py` was exercised, by hand, on pairs known to differ:
- `P-2197-A1-edge` (spot run) against `P-2197-A1-zero`, compared as if under S2, reports the one difference
  `integrations[0].drain.gap_prescription: 'edge' != 'zero'`. Under S1 it reports none;
- `P-2197-A0.1-zero` against `P-2197-A1-zero`, under S2, where S2 does not hold, reports 53 differences, the first
  being K_final, N_final and RHS_evaluations.

## 6. Observations not acted on

- **3873's tuning takes 16 integrations under the one-loop law**, at 110–118 s each, so about 30 min a tuning. Its
  residuals jump at the 10⁻⁴ level between iterates. The prompt's "a tuning costs about one integration" (§0.4) holds
  for 105 of the 110 runs.
- **L-M5e-20 is not monotone in A.** At A = 1 it parks after 14 one-loop drains. At A = 0.1 it reaches the step
  budget, after 13 835 candidates. At A = 0.01 it parks after 37 candidates (7 one-loop, 2 gap, 28 classical).
- **P-2188 at A = 0.01 succeeds under `edge` (54 s) where `zero` fails.** The edge drains of 331 gap candidates park
  the field at N = 39.61, before the classical tail.
- **Unresolvable assessments arise under the one-loop law**, in failed integrations only: 6 in R-632 at A = 0.1, and 1
  each in W-2187 and W-2178 at A = 0.01. Log 01 §7 recorded none under the one-loop law.
- **β = 1.0** (R-632, C-3233): Y_p 0.4345 and D/H 10.41 × 10⁻⁵ under both laws, with ΔN_eff at 1 MeV zero. The stored
  fixed law gives the same Y_p and D/H.
- **The R·S(L) bound is mostly outside S(L)'s range in the long classical tails:** L down to 10⁻⁸, with 9714 of 9729
  candidates outside in L-M1e-16 at A = 0.1, and all 301 of 3233's outside. The bound covers only L ∈ [40, 134].
- **The effective-minimum failures resemble `[04-a-recomputed-history-does-not-find-the-effective-minimum]`** (V_eff' of
  one sign across the bracket, values about 3.3–3.9 × 10⁻¹²¹). Not diagnosed. The new issue records them.
- **The step budget** (2 × 10⁶, integrator-remediation prompt 02) binds at A = 0.01 at laboratory M. The probe's
  untuned 2197 took 1.97 × 10⁶ steps (prompt §0.4). Of the tuned seed integrations that ended in "the effective minimum
  was not found", those of P and W at A = 0.01 completed to T_CMB in 1.53–1.97 × 10⁶ steps, and R-632's in
  7.7 × 10⁴ to 7.2 × 10⁵. The other failures met the budget.

## 7. State handed to the next prompt

For 03 (the store):
- **The regimes that arise:** one-loop, gap and classical. No candidate was unassessed (0 of 34 664 in the accepted
  integrations). No one-loop candidate had w_gate < 1 (0 of 7203), and no gap candidate either (0 of 535).
- **The gap prescription's name in the record:** `DrainRecord.gap_prescription`, `'zero'` or `'edge'`, read once at the
  start of each integration. The spot checks confirm that it is the only value S1 changes. 03 fixes the prescription as
  U13 is ruled, and removes `_GAP_PRESCRIPTION` and `--gap-prescription`.
- **What the store will have to carry that 01's record lacks**, for the measures 02 computed from the payload:
  - per candidate, its N, ln ρ_kin,E (`Injection.log_rho_kin`), ln R and φ_t (from `classical_turning_point`), for the
    R·S(L) bound with L = M/φ_t;
  - ΔK, for the regime shares;
  - K(N_final), or the ledger's total, to form the shares.

  `DrainCandidate` holds regime, ℓ, α_eff, w_gate, ln E and ln H, but not N, ln R, φ_t or ln ρ_kin. The bounce rows of
  README §8.3 must carry them, or the derivation tool must find them.
- **For 06's gate R** ("02's tool route for the same histories"): `var/one-loop-drain/02/sample.jsonl` holds, for each
  task, both samples' SHA-256, the tuning record, and each integration's digests of its regime sequence and of its
  injections' (N, α_eff, ΔK). The partial files hold the full sequences.
- **U13 and U14** are open for the user (verification §2.9).
- **`[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]`** is open (board §3). A = 0.01 at laboratory M in
  the transition window does not build on this code.
