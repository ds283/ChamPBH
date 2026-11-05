# Orchestrator — prompt 02d, measure the measured law

Read [`../README.md`](../README.md) first: §5, and §8 whole, in particular §8.10 (the measured law, U14 as it stands,
the three rulings made during 02c, the Remeasure gate). Then the prompt, and orchestrator notes 02
([`prompt-02.md`](prompt-02.md)) and 02b ([`prompt-02b.md`](prompt-02b.md)), whose rulings bind here where the prompt
or this note does not replace them. This note binds 02d only, and governs where it differs from the prompt (prompt,
"Written"). Note 01's conventions bind as well: a tolerance is a number with its reason, timing is taken under load,
and the suites run serially.

**The prompt:** [`02d-measure-the-measured-law.md`](../02d-measure-the-measured-law.md) · **Gate:** "Remeasure" ·
**Closes:** nothing · **Narrows or extends:** `[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]` ·
**Opens:** only what the work finds · **Model:** Opus, as the prompt recommends · **Network:** none. Nothing is
pushed.

**When:** at once, at the user's request of 2026-11-05 ("Please write the orchestration note and dispatch it, and
afterwards carry out a review of the implementation agent's work"), in a fresh session.

**Gate**, checked at `d40bb3f` on 2026-11-05 at 12:07 local time, before this note was committed:
- `one-loop-drain` is checked out in the main checkout at `d40bb3f`, the commit that adds prompt 02d.
  `git status --porcelain` is empty. `18add05`, `2747741` and `ff7aaeb` are ancestors of `HEAD`.
- `git diff 18add05 HEAD --` over the prompt's code paths is empty, so the code is the code 02c's review passed.
- `./venv/bin/python -m runregistrykit list`: 23 runs, all finished; none running, stale or unknown.
- `var/one-loop-drain/02/sample.jsonl` has 180 lines and `02b/sample.jsonl` 360; `02/fixed_law.json` exists.
- The 2026.8.0 store's 16 shards (`var/datastores/science-2026.8.0-shard*.db`) exist.
- `.documents/one-loop-drain-verification.md` has §2c (§2c.1–§2c.6) and no §2d. Neither
  `.documents/one-loop-drain/measure-02d/` nor `var/one-loop-drain/02d/` exists.
- The suites were not re-run for this note. 02c's review left them at 26, 594 (162 + 432), 130, 156 and 68, all
  passing, and nothing under any `tests/` has changed since. The agent measures them again first (prompt §3).
- Load averages 19.33 / 16.74 / 16.23 at 12:07. The machine has 10 cores and **16 GB of memory**, and it is shared.

## 0. What the orchestrator found

For this note the orchestrator read:
- `.documents/one-loop-drain/measure/run_sample.py` whole, `tasks.py` whole, and their dated README;
- `measure-02b/run_wide.py`'s head and `first_impact`, and its dated README;
- 02's and 02b's `sample.jsonl`: the outcomes, how derived lines are written, and the walls and peak RSS of C and L;
- `ComputeTargets/ScalarHistory.py`: the law's constants (:195–:278), `Injection` (:514), `IntegrationResult` (:677),
  `DrainCandidate` (:714), `DrainRecord` (:737), `drain_law_inputs`, `drain_S_of_L`, `drain_fraction` and
  `drain_alpha_effective` (:1811–:1906);
- `tools/history_and_bbn.py`'s `drain_values` (:340).

Here is what it found where the prompt is silent, loose or wrong.

- **02's partials keep too little for the cap.** `reduce_result` keeps each candidate's regime and each injection's
  (N, α_eff, ΔK), and 02b added ℓ. Recomputing f by `drain_fraction(regime, ell, L, log_R)` (prompt §1.3) needs L
  and ln R as well, and the edges of §1.3 need them for every candidate. `DrainCandidate` is eight plain values, so
  the whole tuple can be kept. JSON writes a float by `repr`, which round-trips exactly, so f recomputed from the
  partial is f as the loop computed it (ruling 6).
- **The law can be checked against its own record at every candidate.** α_eff is
  `drain_alpha_effective(regime, ell, L, log_R, A, w_gate)` of the candidate's own fields and the record's A, by
  construction (`drain_law_inputs` hands the same two numbers to both). Recomputing it costs nothing, and a
  difference can only come from the law's code or the record's. That is §1.5's last stop (ruling 6).
- **Where the law's edges sit, by name.** The clamp is `1 + DRAIN_GAP_Q·ln L ≤ 0`; the gap's threshold is
  `ln R ≤ DRAIN_GAP_R0`; the table's ends are `DRAIN_S_TABLE_L[0]` (0.3) and `DRAIN_S_TABLE_L[-1]` (134). The
  analysis should use the module's constants, not literals (ruling 6).
- **The tool's `drain` line already gives the K shares by regime and the parking leftover's.** `drain_values` sums
  the injections' ΔK by regime and reads the Parking record's ΔK, over K(N_final). The child already calls it (02's
  `accepted_values`), so prompt §1.3's "drained share of K(N_final)" is that, not a new sum.
- **Some of the old law's lines were never run.** In 02b, the `zero` lines of L-M5e-16, L-M4e-15, C-3016, C-3233 and
  C-3873 are S2's at every one of the six A (5 per A), derived from 02's A = 1 lines; their `values_from` names the
  source. They carry values but no wall. In 02, all five were run at A = 1 and 0.1. So:
  - prompt §2 item 5 can set each 02d line beside a 02b S2 line's values, which are its source's, with A substituted;
  - prompt §2 item 7 and the projection of §1.4 have no 02b wall for them (rulings 4 and 7).
- **"Its own history's A = 0.5 wall time"** (§1.4) is ambiguous between 02b's and 02d's. Only 02d's measures the
  measured law. 02b's ratio of A = 0.05 to A = 0.5 is the old law's (ruling 4).
- **The old law's costs, for the order.** In 02, C-3873 took 1832 s at A = 1 and 1823 s at A = 0.1; every other C or
  L task at A ≥ 0.1 took 14–88 s, bar L-M1e-16 (708 s) and L-M5e-20 (924 s, failed) at A = 0.1. The largest peak
  RSS of any run task in 02 or 02b was 1.33 GiB, which gives six workers. A batch whose C-3873 task starts last ends
  about half an hour after the rest (ruling 3).
- **02's timeout variable is named for 02** (`ONE_LOOP_DRAIN_02_TIMEOUT_S`), and exists only to test the timeout
  path. A copy keeps the variable's purpose under a 02d name, and production runs at 3600 s (ruling 1).

**Rulings.** Each is recorded in the log, with its class.
1. **The scripts.** `measure-02d/` is self-contained (prompt §1.2). The agent names its files.
   - The driver is a copy of 02's `run_sample.py` (child, `reduce_result`, `accepted_values`, `read_partial`,
     `build_line`, `batch`, `main`), with 02b's `first_impact` copied beside it. The task list is a copy of 02's
     `tasks.py`.
   - The changes, and no others:
     - the paths, to `var/one-loop-drain/02d/` and the driver's own path;
     - the task list's A values and argv, without `gap` (prompt §1.2);
     - `DrainRecord.model` in place of `gap_prescription`;
     - the R·S(L) bound and `l2s` removed;
     - ruling 6's additions;
     - the timeout's environment variable renamed for 02d, default 3600 s.
   - Each copy names its source file and function in a comment. The log lists every copy, with a diff against the
     original for each.

   *As the prompt says; made exact.*
2. **Detached, not registered,** as note 02's ruling 2: `nohup`, pid recorded, `batch.log` and `batch.end` under
   `var/one-loop-drain/02d/batches/<name>/`, one check that it started, and a background command that waits for
   `batch.end` or the pid's exit, not polling. *STRUCTURALLY REQUIRED* (precedent: 02, 02b).
3. **The batches, in order.** No two batches run at once, and no suite runs while a batch does.
   1. `check-1`, two workers: the tool check's CLI run and its driver task (P-2197 at A = 10, tagged), and the first
      determinism runs of P-2197 and L-M1e-16 at A = 1 (tagged).
   2. `check-2`, two workers: the second determinism runs (tagged differently).
   3. Compare. A difference is a stop.
   4. `A1`: the thirty at A = 1.
   5. `wide`: the 150 at A = 10, 5, 2, 1.5 and 0.5.
   6. `P005`, two workers: P-2197 and P-2188 at A = 0.05. Then the projection (ruling 4).
   7. `small`: the rest of A = 0.1 and 0.05, 58 tasks.

   Within a batch, C-3873's tasks are queued first, then the rest in the task list's order, so that the longest
   task does not start last. The order changes no result. Every task id runs once untagged, so `sample.jsonl` has
   exactly 240 lines; the checks' tagged lines go to their own files. *IMPLEMENTATION CHOICE*, ruled here.
4. **The projection, made exact.**
   - Each remaining task is costed at its own history's 02d A = 0.5 wall, times a ratio r:
     - r is 02b's A = 0.05 wall over 02b's A = 0.5 wall, both `zero` lines that were run;
     - for the five histories whose 02b lines are S2's (§0, fifth point), r is 02's A = 0.1 wall over 02's A = 1
       wall;
     - where P's measured ratio (the larger of P-2197's and P-2188's 02d A = 0.05 / A = 0.5) is larger, it is used.
   - The same r is used for A = 0.1 as for A = 0.05. It overstates A = 0.1, and is stated as an upper estimate.
   - Each task's cost is capped at 3630 s (the timeout and the kill grace). A history that failed or timed out at
     A = 0.5 in 02d is costed at its 02d A = 0.5 wall times r all the same.
   - The projected wall is the larger of the summed cost over the workers and the longest single task.
   - Over three hours, or fewer than three workers: **stop and ask**, with the table of tasks, costs and r.

   *IMPLEMENTATION CHOICE*, ruled here (§0, fifth and sixth points).
5. **Workers.** min(6, ⌊8 GiB / the largest peak RSS seen so far in 02d⌋), as note 02's ruling 5, re-evaluated at
   each batch; the checks run at two. *As the prompt says.*
6. **The record, kept and checked.**
   - **Kept.** The partial line keeps, beside 02's `regimes` and `injections`, `candidates`: each `DrainCandidate` as
     a list of its eight fields in order. Like `regimes` and `injections`, it is dropped from the task's line and
     from a timed-out or crashed task's line.
   - **Checked.** For every integration that returned or raised with a `DrainRecord`:
     - `model` is `DRAIN_MODEL_ONE_LOOP`;
     - at every candidate, `drain_alpha_effective(c.regime, c.ell, c.L, c.log_R, record.A, c.w_gate)` equals
       `c.alpha_eff` by `repr`.

     The counts of each check go in the task's line. A model other than `one-loop-measured-v1`, or one α_eff that
     differs, is a stop (prompt §3; §1.5's last point).
   - **The cap.** f = `drain_fraction(c.regime, c.ell, c.L, c.log_R)`; capped where A·f ≥ `DRAIN_ALPHA_CAP`, by
     regime. On one run with capped drains, every capped candidate's α_eff is `DRAIN_ALPHA_CAP · w_gate` exactly; this
     follows from the check above and is stated, not counted again.
   - **The edges** use the module's constants: the clamp, `1 + DRAIN_GAP_Q·log(L) ≤ 0` at a gap candidate; the
     threshold, `log_R ≤ DRAIN_GAP_R0` at a gap candidate; the continuation, `L > DRAIN_S_TABLE_L[-1]` at a
     classical candidate; S held, `L < DRAIN_S_TABLE_L[0]` at a classical candidate; the underflow, a classical
     candidate with `alpha_eff == 0.0` and `w_gate > 0`.
   - **By regime**, from the accepted integration: the count; the drains (α_eff > 0); the K share, which is
     `drain_values`'s; and the range of L and of ln R.

   *IMPLEMENTATION CHOICE*, ruled here (§0, first to fourth points).
7. **Against the old law.**
   - Each 02d line is set beside the `zero` line of the same history and A: 02's at A = 1 and 0.1, 02b's at the other
     six.
   - A 02b line obtained by S2 is used with its own values, which are its source's, and marked "S2 of" its
     `values_from`. Its cost is shown as "—" beside the source's wall.
   - Nothing of the old lines is re-run.

   *As the prompt intends; made exact* (§0, fifth point).
8. **"Bitwise"** for the determinism and tool checks is note 02's rulings 6 and 9, with two changes: the record's
   `model` stands where `gap_prescription` stood, and the partial file's `candidates` lists are compared whole, by
   `repr`, per integration. *As the prompt says; made exact.*
9. **The failures** are prompt §1.5, unchanged. A timeout is not a failure of 02's two kinds. It is reported with
   what it finished, and it opens nothing by itself. *As the prompt says; made exact.*
10. **No file but these:**
    - `.documents/one-loop-drain/measure-02d/` and its `out/` (text only);
    - `.documents/one-loop-drain-verification.md` (§2d added after §2c);
    - `logs/02d-measure-the-measured-law.md`;
    - the board;
    - `.documents/OPEN_ISSUES.md`, as §1.5 says.

    Nothing under `var/` is committed. Nothing under `.documents/one-loop-drain/law/`, `law-02c/`, `measure/` or
    `measure-02b/` changes. *STRUCTURALLY REQUIRED*; followed.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status` before
  committing. Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The Gate, and the five suites before anything else. They run serially: ComputeTargets in its two halves, the
     `test_[g-z]` half and `tools/tests` in the background, waiting for each notification.
  2. The scripts and the task list.
  3. The batches of ruling 3, with the checks' comparison after `check-2` and the projection after `P005`.
  4. The analysis scripts, over 02d's lines with 02's and 02b's beside them.
  5. The suites again; the documents; the records; the commit.
- **Load.** Record the 1-, 5- and 15-minute loads at each batch's start and end, beside its wall time.
- **Scratch.** Outputs go under `var/one-loop-drain/02d/`. Probes and suite outputs go in the agent's own
  subdirectory of the session scratchpad. Nothing goes in `/tmp`. Nothing is written under `var/one-loop-drain/02/`,
  `02b/` or `02c/`.
- **No Ray.** Nothing starts a cluster, and no `ray` command is run. No store is written; if the store is opened, it
  is opened `mode=ro`.
- **Stop conditions are real:**
  - prompt §3's list;
  - ruling 6's two checks;
  - ruling 4's projection or worker floor.

  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

Done at `d40bb3f`, before this note was committed:
1. **The Gate above.**
2. **The reading of §0.**

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout, in the background. Give it:
- the prompt, the README, the board, this note, and orchestrator notes 02 and 02b (whose rulings it applies as this
  note says);
- `HEAD`, and the Gate as measured.

The agent reads `orchestrator/prompt-02.md`, `prompt-02b.md` and this note, and no other file of `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the files of ruling 10. Its log is `logs/02d-measure-the-measured-law.md`,
  in `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **It starts nothing that outlives its turn** but the batches of ruling 3, and every one of them must have ended
  before it commits.
- **Stop and ask** on any of the conventions' stop conditions.
- **It replies with:**
  - the log's path, the message as committed, and `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - the scripts: each copy and what changed (ruling 1);
  - the tool check and the determinism check, each with its result;
  - the batches: tasks run, failed and timed out, with each batch's wall time, workers, loads and peak RSS;
  - ruling 4's projection, with its numbers;
  - ruling 6's checks, with their counts;
  - the Remeasure gate, item by item (prompt §2, items 1–8), with the headline numbers, laboratory M first;
  - what §1.5 did to the issue: extended, narrowed or neither, with the rows;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — seven checks

1. **Scope.**
   - `git show --stat <agent>` lists only ruling 10's files. Its parent is this note's commit, or a later commit
     that is not the agent's.
   - `git status --porcelain --untracked-files=all` is empty, and nothing is pushed.
   - `git diff <agent>^ <agent> --` over the code paths, and over `law/`, `law-02c/`, `measure/` and `measure-02b/`,
     is empty.
   - `measure-02d/out/` holds text only.
   - `var/one-loop-drain/02/`, `02b/` and `02c/` hold no file newer than this note.
2. **Nothing left running.** No pid under `var/one-loop-drain/02d/` answers `kill -0`, `batch.end` exists for every
   batch, and `runregistrykit list` is unchanged.
3. **The sample is complete.** `sample.jsonl` has one line for each of the 240 (history, A) tasks, with no gaps and
   no duplicates, each run, with an outcome. The tagged check lines are elsewhere.
4. **The law against its record, independently.** In a scratch script of my own, read every partial file's
   `candidates` and recompute α_eff by `drain_alpha_effective`, and also by `fraction_adopted` imported from
   `source/crossover/measured_law_map.py`, with S(L) continued above 134 and f = 0 below the clamp. Expect the first
   to be bitwise equal everywhere, and the second wherever the two forms agree.
5. **One task, re-run.** Re-run one task by 02d's driver, in a process of my own: one of group C or L at small A if
   any is cheap enough, else a B task at A = 0.5. Compare under ruling 8.
6. **The tool and the numbers.**
   - Run `tools/history_and_bbn.py` on one laboratory-M task at A = 5 (not 2197). Compare its `history`, `drain`,
     `dissipation` and `bbn` lines with the driver's line for it.
   - Recompute from `sample.jsonl` and the partials, in a scratch script: the cap counts by regime of one run; 2197's
     ΔN_eff today at all eight A; the count of failures at A = 0.1 and 0.05; the clamp count.
7. **The records.**
   - The log is in §5.1's form, with every deviation classed and §0's rulings among them.
   - The verification document's §1, §2, §2b and §2c are unchanged, and §2d follows prompt §2's eight items. The
     handover states measurements and options, and makes no choice.
   - On the board: row 02d, its items, the Remeasure gate, the issue as §1.5 left it, and **Last updated**.
     `.documents/OPEN_ISSUES.md` matches §3, with its count and date.
   - `CLAUDE.md` is unchanged, and so are the suites' counts.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 02d landed, the Remeasure gate held, and this note added to the list of orchestrator notes, marked
  "used for 02d";
- an *Orchestrator review of prompt 02d* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s campaign row.

Residue is fixed in that commit or a follow-up of its own. Anything that is not residue is opened on the board's §3
and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the Remeasure gate: the sample, the failures, parking and the first impact, the observables across the eight A at
  laboratory M, the change from the old law, and the cost;
- what §1.5 did to the issue, and, if it was narrowed to A = 0.01, the question whether it closes, for the user to
  rule in their own words;
- that 03 (the store) is written next.
