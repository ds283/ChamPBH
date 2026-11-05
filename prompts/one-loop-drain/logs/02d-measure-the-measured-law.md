# Log 02d — measure the measured law

**Subject:** Measure the measured drain law on the sample at U14's eight A
**Commit:** `98b1601`, on branch `one-loop-drain`, on top of `f4da906`
**Date:** 2026-11-05, by Claude Opus 5.5
**Result:** **COMPLETE.** The Remeasure gate holds (`.documents/one-loop-drain-verification.md` §2d.12). Its numbers replace
02's and 02b's as the campaign's measurement of the one-loop law.
- **The checks.** The tool check (P-2197 at A = 10) and the determinism check (P-2197 and L-M1e-16 at A = 1, run twice)
  are bitwise. The record's model is `one-loop-measured-v1` in all 464 integrations, and α_eff recomputed by
  `drain_alpha_effective` from each candidate's record equals the record's at all 616 270 candidates.
- **The sample.** 02's thirty histories at A ∈ {10, 5, 2, 1.5, 1, 0.5, 0.1, 0.05}, re-tuned, every task run: 240 tasks.
  230 succeed, 3 fail, 7 time out.
  - Every P, W and B history builds at all eight A, in one integration each but B-2774 at A = 1.
  - R-632 (β = 1.0) fails at A = 0.1 and 0.05 ("the effective minimum was not found"), and the 5 × 10⁻²⁰ M_P ladder at
    A = 0.05 (the step budget): 02's two kinds. The ladder's 5 × 10⁻²⁰ M_P now builds at A = 0.1, where 02 failed.
  - C-3873 times out at every A but 1 (24–27 tuning integrations in 3600 s; 28 at A = 1, in 3324 s).
- **The law on the sample.** At laboratory M the first impact is one-loop and drains A·f₁, f₁ = 0.082–0.115: 82–99% of the
  kinetic energy at A = 10, 0.4–0.6% at A = 0.05. Every P and W history parks one-loop at A ≥ 0.1 (N = 37.7–38.7 at
  A = 0.1); at A = 0.05 ten of them enter the gap for 5–22 drains first and park at N = 38.4–39.4. None reaches R ≤ 1.
  2197's ΔN_eff today is 0.555, 0.273, 0.117, 0.0903, 0.0601, 0.0328, 0.0113 and 0.00856 from A = 10 down to 0.05.
- **Against the old law.** At laboratory M the measured law moves ΔN_eff today by at most 0.011 and Y_p by 3.8 × 10⁻⁴;
  near M_crit (the ladder at 5 × 10⁻¹⁶ M_P, whose first impact is in the gap) by up to 0.146. Eight pairs where both laws
  cap every drain are equal exactly.
- **The law's edges.** The gap clamp is never met (least gap L 2.578); S is held below L = 0.3 at 37 922 classical
  candidates of the accepted integrations; the continuation above L = 134 and the classical underflow are never met.
- **Cost:** 3 h 07 min 52 s from the first launch to the last end, at six workers; 11.38 h of task CPU. Ruling 4's
  projection for the last batch was 1.30 h.
- **Issues:** none opened or closed. `[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]` is extended by a
  dated paragraph, and its index row now says what holds under the measured law (prompt §1.5). Index: 31 open,
  unchanged.
- **Suites** before anything else: 26, 594 (162 + 432), 130, 156, 68, all passing. After: the same counts, all passing.

Orchestrator note 02d (§0, rulings 1–10 and the conventions) governed where it differs from the prompt, with notes 02's
and 02b's rulings where it names them. Each ruling is classified in §2.

## 1. What shipped

No code outside `.documents/one-loop-drain/measure-02d/` changed (prompt §1.6), and nothing under
`.documents/one-loop-drain/law/`, `law-02c/`, `measure/` or `measure-02b/` changed.

- **`.documents/one-loop-drain/measure-02d/`**, each script with its docstring and its command. They import nothing of
  02's or 02b's scripts (prompt §1.2):
  - `tasks_02d.py`: the task list, a copy of 02's `tasks.py`, written as `var/one-loop-drain/02d/tasks.json` (240 tasks);
  - `run_02d.py`: the driver, a copy of 02's `run_sample.py` with 02b's `first_impact`, and ruling 6's additions;
  - `checks_02d.py`: the tool check and the determinism check, from copies of 02's `common.py` and `tool_check.py`;
  - `plan_02d.py`: ruling 3's batch lists, ruling 5's workers and ruling 4's projection;
  - `analyse_02d.py`: the gate's tables (`sample`, `lawcheck`, `regimes`, `parking`, `observables`, `oldlaw`, `fixed`,
    `cost`, `handover`, `summary`);
  - `copies_02d.py`: every copied name against its original, as a unified diff.
- **`measure-02d/out/`** (text only): `tasks.txt`, `copies.txt`, `tool_check.txt`, `determinism.txt`, `projection.txt`,
  `sample.txt`, `lawcheck.txt`, `regimes.txt`, `parking.txt`, `observables.txt`, `oldlaw.txt`, `fixed.txt`, `cost.txt`,
  `handover.txt`, `summary.txt`.
- **`.documents/one-loop-drain-verification.md` §2d**, added after §2c, which is unchanged with §1, §2 and §2b.
- **The board**: row 02d landed, two items, the Remeasure gate, the dated paragraph on
  `[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]`, §5, the status line and **Last updated**.
- **`.documents/OPEN_ISSUES.md`**: that issue's row, as prompt §1.5 asks; the count (31) and the date stand.

Not committed: everything under `var/one-loop-drain/02d/`, namely `sample.jsonl` (240 lines), `checks.jsonl`,
`tasks.json`, `tasks/` (each task's stdout, stderr, partial file and result) and `batches/` (six batches, each with its
ids, `batch.pid`, `batch.log` and `batch.end`).

**The copies (ruling 1).** `out/copies.txt` holds each copy's diff against its original: 43 copied names, 25 unchanged and
18 changed. The changes:

| 02d name | source | what changed |
|---|---|---|
| `tasks_02d.py`: `A_VALUES`, `task_id`, `main` | 02's `tasks.py` | U14's eight A; the id `<history>-A<A>`; one task per (history, A), the argv without `--gap-prescription`, no `gap` field; `main` also makes the output's directory (D2) |
| `run_02d.py`: `OUT`, `SELF`, `TIMEOUT_S` | 02's `run_sample.py` | 02d's paths; the variable `ONE_LOOP_DRAIN_02D_TIMEOUT_S`, default 3600 s |
| `run_02d.py`: `reduce_result` | the same | the record's `model` for `gap_prescription`; the partial line keeps `candidates` (each `DrainCandidate` as its eight fields); `candidates_sha256`, `law_check`, `cap` and `edges` per integration (ruling 6); `solution_nodes` and `steps_after_first_classical` (D1) |
| `run_02d.py`: `accepted_values` | the same | the R·S(L) bound removed; `model`, `by_regime`, `cap`, `edges` and `first_impact` added (ruling 6); `r_chi_check` kept (D3) |
| `run_02d.py`: `child`, `read_partial` | the same | `candidates` dropped from the task's line, beside `regimes` and `injections` |
| `run_02d.py`: `TASK_FIELDS`, `batch` | the same | no `gap`; `batch` makes the task directory (D2) |
| `run_02d.py`: `first_impact` | 02b's `run_wide.py` | the first candidate's L and record ln R; the cap by regime by the law's f (`DRAIN_C1` is gone); the drains before parking by regime |
| `checks_02d.py`: `NOT_COMPARED`, `compare_lines`, `compare_partials` | 02's `common.py` | no `gap`; nothing of the record excused (no shortcut); `candidates` compared too (ruling 8); 02d's paths |
| `checks_02d.py`: `tool` | 02's `tool_check.py` `main` | one task, its line from `checks.jsonl`, the model among the values checked |
| `analyse_02d.py`: `short_reason` | 02's `analyse.py` | the integrations counted from the line |

The unchanged copies are `STORE`, `LAB_M`, `HISTORIES`, `history_id`, `a_label` and `store_row` (tasks); `TASKS_JSON`,
`TASK_DIR`, `BATCH_DIR`, `now_utc`, `loads`, `load_tasks`, `digest`, `build_line` and `main` (driver);
`INTEGRATION_NOT_COMPARED`, `BBN_NOT_COMPARED`, `read_jsonl`, `_diff`, `MASK`, `masked` and `fields` (checks);
`read_jsonl`, `g` and `met_counts` (analysis).

## 2. Deviations from the prompt

**The orchestrator's rulings**, each as its note classes it:
1. **The scripts** — *as the prompt says; made exact*. Followed: self-contained copies, each naming its source, every
   copy's diff in `out/copies.txt` (§1). Two additions beyond the ruling's list are D1 and D2 below.
2. **Detached, not registered** — *STRUCTURALLY REQUIRED*. Each of the six batches was launched with `nohup` and
   `disown`, its pid in `batches/<name>/batch.pid`, with `batch.log` and `batch.end`; each was checked once that it had
   started, and waited on by a background command that ended with `batch.end` or the pid's exit.
3. **The batches, in order** — *IMPLEMENTATION CHOICE*, ruled. Followed: `check-1`, `check-2`, the comparison, `A1`,
   `wide`, `P005`, the projection, `small`, one at a time, with no suite while a batch ran. C-3873's tasks first in each
   batch. `sample.jsonl` has exactly 240 lines; the checks' tagged lines are in `checks.jsonl`.
4. **The projection, made exact** — *IMPLEMENTATION CHOICE*, ruled. Followed (`out/projection.txt`): 1.30 h at six
   workers, under the limit, so no stop. It overstated `small`'s wall (1.00 h), as an upper estimate should.
5. **Workers** — *as the prompt says*. min(6, ⌊8 GiB / the largest peak RSS so far⌋) = 6 at `A1`, `wide` and `small`
   (largest 0.234, 0.961 and 1.189 GiB); the checks and `P005` at two. At most seven processes of this prompt computed at
   once (a parent and six children).
6. **The record, kept and checked** — *IMPLEMENTATION CHOICE*, ruled. Followed: `candidates` kept whole in the partial
   line and dropped from the task's line; the model and α_eff checked in every integration (464 and 616 270, none
   differing); the cap by regime from `drain_fraction`, with `P-2188-A10`'s 3 capped candidates at α_cap·w_gate exactly;
   the edges by the module's constants; the by-regime values with the K share from `drain_values`.
7. **Against the old law** — *as the prompt intends; made exact*. Followed (`out/oldlaw.txt`): 02's `zero` lines at A = 1
   and 0.1, 02b's at the other six, S2 lines marked "S2 of" their source and their cost shown as "—". Nothing re-run.
8. **"Bitwise"** — *as the prompt says; made exact*. Followed: nothing of the record excused, `candidates` compared whole
   by `repr` per integration.
9. **The failures** — *as the prompt says; made exact*. The three failures are of 02's two kinds and extend the issue
   (§1.5); the seven timeouts open nothing.
10. **No file but these** — *STRUCTURALLY REQUIRED*; followed (§4's staged list).

**The agent's own deviations:**
- **D1 — the driver counts the steps of the classical tail.** `reduce_result` records, for a returned integration, the
  OdeSolution's node count and the accepted steps after its first classical candidate. No ruling names it, and ruling 1
  lists "the changes, and no others"; prompt §2 item 7 asks for "the classical drains' share of the steps", which the
  record cannot give otherwise. It reads the result only. *IMPLEMENTATION CHOICE*.
- **D2 — two scripts make their output directories** (`tasks_02d.py`'s `main` and the driver's `batch`), which 02's
  scripts left to the caller. *IMPLEMENTATION CHOICE*.
- **D3 — `accepted_values` keeps 02's `r_chi_check`** (K(N_final)e^{−4N}/ρ_R against the payload's r_χ), which note 02
  added beside the R·S(L) bound. It is not the bound, so it was not removed. *IMPLEMENTATION CHOICE*.
- **D4 — the determinism check also compares check-1 with the A1 pass's untagged runs** of the same two tasks; both are
  bitwise. Not required; reported apart. *IMPLEMENTATION CHOICE*.
- **D5 — outputs beyond the gate's items:** `copies.txt`, `projection.txt`, `determinism.txt` and `summary.txt` (the
  headline numbers by A that §2d cites). *IMPLEMENTATION CHOICE*.
- **D6 — `analyse_02d.py` was corrected twice after the batches began.** Its `handover` failed on a task not yet run
  when tried on the A1 lines, and was made to print it as missing; `oldlaw` was made to list the exactly-equal pairs it
  counted, so that §2d.8 cites them from an output. The trial outputs were in the scratchpad; every committed output was
  written after the last batch, by the final script. *UNINTENDED DRIFT*, corrected.
- **D7 — the §2d draft was checked against the lines before commit**, and two placeholders and several ranges stated
  from memory were replaced by the outputs' numbers. *UNINTENDED DRIFT*, corrected.

## 3. The run

The batches, from `batches/*/batch.log` (verification §2d.3); times UTC, 2026-11-05:

| batch | tasks | workers | wall | loads start → end | largest task RSS | outcomes |
|---|---|---|---|---|---|---|
| check-1 | 4 | 2 | 48.1 s | 13.97 11.11 13.09 → 16.54 12.24 13.39 | 233 MiB | tool rc 0; 3 success |
| check-2 | 2 | 2 | 32.1 s | 41.92 18.36 15.56 → 39.90 20.92 16.64 | 240 MiB | 2 success |
| A1 | 30 | 6 | 3324.0 s | 31.71 20.29 16.51 → 6.25 6.65 8.70 | 984 MiB | 30 success |
| wide | 150 | 6 | 4096.2 s | 6.09 6.58 8.62 → 41.36 39.24 43.56 | 1217 MiB | 145 success, 5 timeout |
| P005 | 2 | 2 | 50.2 s | 29.75 36.50 42.35 → 28.34 35.07 41.49 | 265 MiB | 2 success |
| small | 58 | 6 | 3604.0 s | 25.33 34.07 41.02 → 7.74 10.08 11.24 | 1514 MiB | 53 success, 3 failure, 2 timeout |

- **Outcomes:** 240 run; 230 succeeded, 3 failed (`R-632-A0.1`, `R-632-A0.05`: the effective minimum;
  `L-M5e-20-A0.05`: the step budget), 7 timed out (`C-3873` at A = 10, 5, 2, 1.5, 0.5, 0.1, 0.05, each SIGTERMed at
  3600 s and gone within 2 s, no SIGKILL needed).
- **Ruling 4's projection:** 58 tasks, 28 013 s summed, 4669 s over six workers, longest task 3630 s: 1.30 h.
- **Stop conditions.** None met: the suites stand; the tool and determinism checks are bitwise; the model is
  `one-loop-measured-v1` in every integration and no α_eff differs; the projection was under three hours at six workers;
  no failure points at the law's code, the wall search or the inside-wall guard; nothing §1.6 forbids became necessary.
- **Nothing outlives the batches.** Every `batch.pid` fails `kill -0`, every batch has its `batch.end`, and no
  `run_02d.py` or `history_and_bbn.py` process is alive. `runregistrykit list` is unchanged at 23 runs, all finished.
- **Load.** Other work on the shared machine raised the 1-minute load to 30–50 during `wide`, `P005` and `small`; the
  walls are taken under it.

## 4. Verification performed

- **The Gate**, at `f4da906` before anything ran: it held (verification §2d.1).
- **The suites before** (2026-11-05, local time = UTC; serially, the `test_[g-z]` half and `tools/tests` in the
  background):

  | suite | count | time | uptime at start |
  |---|---|---|---|
  | CosmologyModels | 26 OK | 72.4 s | 12:12, load 17.70 18.14 17.15 |
  | ComputeTargets `test_[a-f]` | 162 OK | 144.2 s | 12:13, 15.97 17.57 17.03 |
  | ComputeTargets `test_[g-z]` | 432 OK | 476.6 s | 12:16, 13.22 17.77 17.36 (end 12:24, 7.94 10.63 14.09) |
  | ObjectFactories | 130 OK | 83.0 s | 12:24, 7.62 10.52 14.03 |
  | tools | 156 OK | 171.1 s | 12:25, 6.49 9.49 13.28 (end 12:28, 12.09 10.59 13.02) |
  | RunDrivers | 68 OK | 27.0 s | 12:28, 11.68 10.53 12.98 (end 12:29, 15.19 11.30 13.17) |

- **The suites after** (one serial background chain, after every batch had ended):

  | suite | count | time | uptime at start |
  |---|---|---|---|
  | CosmologyModels | 26 OK | 66.6 s | 15:37, load 7.30 9.65 11.02 |
  | ComputeTargets `test_[a-f]` | 162 OK | 135.7 s | 15:38, 7.62 9.40 10.83 |
  | ComputeTargets `test_[g-z]` | 432 OK | 460.3 s | 15:41, 5.85 8.03 10.06 |
  | ObjectFactories | 130 OK | 78.5 s | 15:48, 5.22 6.37 8.36 |
  | tools | 156 OK | 183.1 s (405 s with start-up, under other work) | 15:50, 6.05 6.29 8.14 (end 15:56, 79.01 23.07 13.84) |
  | RunDrivers | 68 OK | 66.7 s (176 s with start-up) | 15:56, 79.01 23.07 13.84 (end 15:59, 63.65 34.59 19.22) |

  The counts equal those before: 26, 594 (162 + 432), 130, 156, 68.

- **The tool check** (`out/tool_check.txt`): 13 of 13 lines byte-equal after masking; 23 of 23 values of the `history`,
  `drain` and `bbn` lines equal to the driver's JSON line.
- **The determinism check** (`out/determinism.txt`): both pairs bitwise, with 5 and 154 candidates compared whole.
- **Ruling 6's checks** (`out/lawcheck.txt`): 464 of 464 integrations name `one-loop-measured-v1`; 0 of 616 270 α_eff
  differ; recomputed again from the partial files, 0 of 615 946.
- **`black`** was run on the six scripts before the first batch, and on `analyse_02d.py` and `copies_02d.py` again after
  their last change; `run_02d.py`, `tasks_02d.py`, `plan_02d.py` and `checks_02d.py` did not change after the first batch.
- **`git diff --cached --name-status`** before the commit:

  ```
  M	.documents/OPEN_ISSUES.md
  M	.documents/one-loop-drain-verification.md
  A	.documents/one-loop-drain/measure-02d/analyse_02d.py
  A	.documents/one-loop-drain/measure-02d/checks_02d.py
  A	.documents/one-loop-drain/measure-02d/copies_02d.py
  A	.documents/one-loop-drain/measure-02d/out/copies.txt
  A	.documents/one-loop-drain/measure-02d/out/cost.txt
  A	.documents/one-loop-drain/measure-02d/out/determinism.txt
  A	.documents/one-loop-drain/measure-02d/out/fixed.txt
  A	.documents/one-loop-drain/measure-02d/out/handover.txt
  A	.documents/one-loop-drain/measure-02d/out/lawcheck.txt
  A	.documents/one-loop-drain/measure-02d/out/observables.txt
  A	.documents/one-loop-drain/measure-02d/out/oldlaw.txt
  A	.documents/one-loop-drain/measure-02d/out/parking.txt
  A	.documents/one-loop-drain/measure-02d/out/projection.txt
  A	.documents/one-loop-drain/measure-02d/out/regimes.txt
  A	.documents/one-loop-drain/measure-02d/out/sample.txt
  A	.documents/one-loop-drain/measure-02d/out/summary.txt
  A	.documents/one-loop-drain/measure-02d/out/tasks.txt
  A	.documents/one-loop-drain/measure-02d/out/tool_check.txt
  A	.documents/one-loop-drain/measure-02d/plan_02d.py
  A	.documents/one-loop-drain/measure-02d/run_02d.py
  A	.documents/one-loop-drain/measure-02d/tasks_02d.py
  M	prompts/one-loop-drain/IMPLEMENTATION_STATE.md
  A	prompts/one-loop-drain/logs/02d-measure-the-measured-law.md
  ```

## 5. The deliberate-breakage record

None: this prompt changes no code. The checks that could fail ran on real data: the store check of `tasks_02d.py` (exit
1 on a mismatch), the tool check and the determinism check (exit 3 on a difference), ruling 6's model and α_eff checks at
every candidate, and the projection's stop (exit 3). The determinism comparison's sensitivity is the same as 02's
`compare_lines` and `compare_partials`, of which it is a copy with nothing excused; it reports a difference in any value
of the line or any `repr` of the partial file's lists.

## 6. Observations not acted on

- **C-3873's tuning, not its integration, sets its cost.** Its classical drains are α_eff ≈ 10⁻¹⁹ of the kinetic energy,
  so its trajectory is the same at every A and its integrations cost what the old law's did (251 665 steps against
  252 008 at A = 1). But it tuned in 28 integrations at A = 1 (3324 s), against 16 under the old law (1832 s), and under
  loads of up to 50 reached only 24–27 at the other seven A by the 3600 s timeout. A rebuild of record at M = 10⁻¹² M_P,
  β = 1.11 will meet the same cost through `tune_history`. Not diagnosed.
- **Late parking on the ladder.** Where the cascade runs a long classical tail and drains each candidate by R·S(L), the
  field parks at N = 45–47.6 (T_J ≈ e^{−24} to e^{−26.6} GeV) after 8 500–13 300 classical drains, close to N_final 50.08:
  10⁻¹⁶ M_P at A = 0.5 and 0.1, 5 × 10⁻¹⁶ at A = 1 and 0.5, 8 × 10⁻¹⁸ at A = 0.05 and 5 × 10⁻²⁰ at A = 0.1. These are the
  costly tasks (632–1115 s, 1.3–2.0 × 10⁶ steps), within 2 % of the step budget for 5 × 10⁻²⁰ at A = 0.1.
- **Parking N is not monotonic in A at laboratory M.** P-2197 parks at 29.04 at A = 1 but 35.21 at A = 1.5 and 33.52 at
  A = 2; under the old law at A = 1 it parked at 33.12. The drains before parking are correspondingly 5, 16 and 11.
- **U9 under the measured law is met only at classical drains:** w_gate < 1 at 73 classical drains of C-3233 at every A
  and 1048 of C-3873 at A = 1; at no one-loop or gap drain.
- **S held below L = 0.3** is the classical regime's common case off laboratory M: 37 922 of the 70 673 classical
  candidates of the accepted integrations, with L down to 1.0 × 10⁻¹¹. The S table starts at 0.3, so these drains rest on
  S(0.3), the held value README §8.10 rules.
- **β = 1.0 at laboratory M (R-632) is the same history at A = 5 and 10**, and so are the ladder's 5 × 10⁻²⁰ and
  8 × 10⁻¹⁸ M_P at A = 5 and 10 (and 2, for 8 × 10⁻¹⁸): every drain before parking is capped at 0.99 under both laws.
- **W-2192 has the largest Y_p at laboratory M at A ≤ 1** (0.267 at A = 1, 0.274 at A = 0.05), and Y_p and D/H in the
  transition window are not monotonic in A, as 02b observed under the old law.
- **The projection's P ratio (3.12) was set by load**, not by the law: P's tasks took 50 s at A = 0.05 under a load of 30,
  and 16 s at A = 0.5 under a load of 6.

## 7. State handed to the next prompt

For **03** (the store):
- **The bounce rows** (one per candidate of the accepted integration): 3 to 13 343 per history (P-2188 at A = 10; the
  5 × 10⁻²⁰ M_P ladder at A = 0.1), 82 139 over the 230 successful tasks, 82 115 drained. At laboratory M 3–6 at A = 10
  and 190–203 at A = 0.05; 301–2487 in C at every A.
- **The ranges a column must hold:** one-loop L 4.012–128.4, ln R 6.942–190.3, ℓ 0.0790–44.98; gap L 2.578–106.0, ln R
  0.0947–14.92; classical L 1.007 × 10⁻¹¹–125.6, ln R −239.4 to −0.0354; the least α_eff at a drain 2.3 × 10⁻¹⁰⁸. No
  candidate is unassessed.
- **What 02c's tests did not meet, met here:** classical candidates in quantity, each draining with a restart (70 673 in
  the accepted integrations); S held below L = 0.3 in integrations (37 922; 02c's tests exercise it at synthetic L);
  w_gate < 1 at a classical drain (C-3233, C-3873); gap candidates at ln R ≤ r₀, which drain nothing (24). Not met here
  either: the gap clamp (1 + q ln L ≤ 0), the continuation above L = 134, a classical α_eff underflowing to 0.0, and an
  unassessed candidate.
- **The record suffices:** `DrainCandidate`'s eight fields and the record's A reproduce α_eff exactly at every candidate,
  so the cap, the edges and the first impact are derivable from stored bounce rows; the parking leftover is the `Parking`
  record's ΔK, outside the injections.
- **The drain model key** is `one-loop-measured-v1` in every integration measured.

For **the user**:
- Under the measured law, R-632 (β = 1.0) cannot be built at A = 0.1 and 0.05, nor the 5 × 10⁻²⁰ M_P ladder at A = 0.05,
  on this code (`[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]`, extended); C-3873 tunes slowly (28
  integrations at A = 1). Every P, W and B history builds at all eight A.
- At laboratory M, ΔN_eff at 1 MeV is proportional to A, and ΔN_eff today falls more slowly below A = 0.5 (§2d.7, §2d.11).
