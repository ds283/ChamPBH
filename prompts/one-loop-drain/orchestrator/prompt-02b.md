# Orchestrator — prompt 02b, measure the law over a wider A

Read [`../README.md`](../README.md) first: §5, and §8 whole, in particular §8.4 (row 02b), §8.5 (the Wide gate)
and §8.6 (U13 as ruled, P26, and the user's request for 02b). Then the prompt, and orchestrator note 02
([`prompt-02.md`](prompt-02.md)), whose rulings bind here where this note does not replace them. This note binds 02b
only. Note 01's conventions bind as well: a tolerance is a number with its reason, timing is taken under load, and
the suites run serially.

**You do not write code.** You may:
- run the suites, the driver, the tool and the comparisons;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`02b-measure-wide-A.md`](../02b-measure-wide-A.md) · **Gate:** "Wide" · **Closes:** nothing ·
**Opens:** only what the work finds · **Hands the user:** U14, with the measurements at nine A · **Model:** Opus, as
the prompt recommends · **Network:** none. Nothing is pushed.

**When:** at once, at the user's request of 2026-11-04 ("Please write the orchestration note, and then dispatch
it"), in this session.

**Gate**, checked at `02e6233` on 2026-11-04 at 15:44 local time, before this note was committed:
- `one-loop-drain` is checked out in the main checkout at `02e6233`, the commit that adds prompt 02b.
  `git status --porcelain` is empty. `3f3f0cf`, `2732535` and `5ae8648` are ancestors of `HEAD`.
- `git diff b99f6a4 HEAD --` over the prompt's code paths is empty, so the code is 02's.
- `./venv/bin/python -m runregistrykit list`: 23 runs, all finished.
- `var/one-loop-drain/02/sample.jsonl` has 180 lines: 110 `run`, 65 `S1`, 5 `S2`. Each of the 110 runs has its
  partial file under `var/one-loop-drain/02/tasks/`.
- The 2026.8.0 store's 16 shards exist.
- `.documents/one-loop-drain-verification.md` has §1 and §2 (§2.1–§2.10), and no §2b. `.documents/one-loop-drain/`
  holds `law/` and `measure/`, and `var/one-loop-drain/` holds `01/` and `02/`.
- The suites were not re-run for this note. The review of 02 left them at 26, 566 (163 + 403), 130, 156 and 68, all
  passing, and nothing under any `tests/` has changed since. The agent measures them again first (prompt §3).
- Load averages 5.94 / 27.26 / 25.27 at 15:44. The machine has 10 cores and **16 GB of memory**, and it is shared.

## 0. What the orchestrator found

For this note the orchestrator read:
- `.documents/one-loop-drain/measure/`: `run_sample.py`, `common.py` and the function lists of the others;
- 02's `sample.jsonl` and partial files;
- `ComputeTargets/ScalarHistory.py`: `check_drain_law`, `DrainCandidate`, `drain_record`, the drain's call site, and
  the margins' `alpha > 0` test.

Here is what it found where the prompt is silent, loose or wrong.

- **S2's premise still holds, at any A > 0.** The code is 02's, and note 02's reading stands:
  - A is read as `DrainRecord.A` (:1948), in `drain_alpha_effective` (:2195), and in tests `alpha > 0.0` (:4647
    among them);
  - `check_drain_law` (:1647) asks only that A be finite and > 0 under the one-loop law, with no upper bound
    (:1663). So A = 10 is accepted, and `DRAIN_ALPHA_CAP` clamps.

  The prompt asks the agent to re-check this itself. It should find these same sites.
- **Where S2 applies, from 02's lines.** 02 derived five tasks by S2, all at A = 0.01 under `zero`: L-M5e-16,
  L-M4e-15, C-3016, C-3233 and C-3873. 02's A = 0.1 tasks for the same histories were run, as ruling 1's free check.
  - Under `zero`, then, these five histories are S2's at every A of 02b, so each A has 25 `zero` runs.
  - Under `edge`, S2 holds only where 02's A = 1 `edge` tuning (run, or S1's) also met no gap candidate. L-M5e-16
    meets a gap candidate at its first impact, so its `edge` tasks are not S2's.

  The agent decides each case from the lines.
- **02's scripts cannot be pointed elsewhere by an argument.** Each fixes its own place:
  - `run_sample.py` fixes `OUT` (:68), `TASKS_JSON`, `TASK_DIR` and `BATCH_DIR` from it, and `SELF` (:73), the path
    by which the parent launches each child;
  - `common.py` fixes `OUT`, `SAMPLE`, `SPOT` and `TASKS_JSON` (:17–:21). Some functions take these as default
    arguments (`run_lines(path=SAMPLE)`), which are bound when the module is imported. Setting the module's
    globals afterwards does not move those defaults;
  - `A_VALUES` is fixed in `common.py` and `tasks.py`.

  So "use 02's scripts by import" works for the functions that read globals at call time, and not for the rest
  (ruling 3).
- **The cap count needs ℓ, which 02's partials do not keep.** `reduce_result` keeps each candidate's regime and each
  injection's (N, α_eff, ΔK), but not `DrainCandidate.ell` (:627–:628). In 02's partials α_eff = 0.99 exactly where
  the cap acted, since 02 found no one-loop candidate with w_gate < 1 (0 of 7203). In 02b's runs ℓ can be kept
  (ruling 6).
- **The first impact is in 02's partials.** Each line of a partial file has `regimes` and `injections`, one entry per
  candidate, in order, with α_eff = 0 where nothing drained. The accepted line has `K_final`. So the first-impact
  fields of prompt §1.3 can be computed for 02's lines at A = 0.01, 0.1 and 1 without re-running anything
  (ruling 6).
- **The cost at A ≥ 0.5 is small.** In 02, A = 1's `zero` pass took 0.68 h of CPU and A = 0.1's 1.28 h. The largest
  task at A = 1 took 30.5 min, and was in group C, which S2 removes here. The peak RSS of a task was at most about
  1.35 GiB.

**Rulings.** Each is recorded in the log, with its class.
1. **The order of §1.4, made definite.**
   - Step 2 is one detached batch for the `zero` passes at A = 10, 5, 2, 1.5 and 0.5, in that order: 125 tasks,
     less any S2 that the agent's own reading adds. Then come the S1 decisions at those A, and one batch for the
     `edge` runs that S1 does not cover.
   - Step 3 starts with P's two alone, `zero` at A = 0.05, at two workers, and then makes the projection of
     ruling 5.

   *IMPLEMENTATION CHOICE*, ruled here.
2. **Detached, not registered,** as note 02's ruling 2: `nohup`, pid recorded, `batch.log` and `batch.end` under
   `var/one-loop-drain/02b/batches/<name>/`, one check that it started, and a background wait. *STRUCTURALLY
   REQUIRED* (precedent: 02).
3. **The scripts.**
   - 02b's scripts import 02's modules and use their functions wherever those read their paths at call time.
   - Where a function takes a path as a default argument bound at import, or a module fixes `OUT`, `SELF` or
     `A_VALUES`, the 02b script may override the module's globals before use. Otherwise it may copy the function,
     or the whole file, changing only those values and what ruling 6 adds.
   - Every override and every copy is listed in the log, with a diff against 02's original for each copy.
   - The child must be launched by 02b's own driver path, so that 02b's partial files and lines go under
     `var/one-loop-drain/02b/`, and nothing is written under `var/one-loop-drain/02/`.
   - The reproduction check (prompt §1.2) is the test that the copy changed nothing it should not.

   *IMPLEMENTATION CHOICE*, ruled here (§0, third point).
4. **The reproduction check, made exact.** `P-2197-A1-zero` and `R-865-A0.1-zero` are re-run by 02b's driver, in
   one batch at two workers, before anything else runs.
   - Each line is compared with 02's, under note 02's ruling 6.
   - Each partial file is compared with 02's, per integration, under the same ruling.
   - The fields that ruling 6 excludes are not compared: wall times, `started`/`ended`, `peak_rss`/`maxrss`, the
     tag and the task file names. Nor are the fields that 02b adds and 02's lines lack.

   A difference is a stop. *As the prompt intends; made exact.*
5. **Workers and the projection.**
   - Step 2's batches start at six workers. Every later batch has min(6, ⌊8 GiB / the largest peak RSS seen so far
     in 02b⌋), as note 02's ruling 5.
   - **The projection at A = 0.05**, after P's two:
     - the remaining `zero` tasks at A = 0.05 (W, R, B and L, less S2's), plus an `edge` task for every history
       that S2 does not spare under `edge`;
     - each costed at the mean wall time of P's two;
     - the total divided by the workers so found.
   - If the projection exceeds three hours, or the workers come to fewer than three, **stop and ask** with the
     numbers.

   *IMPLEMENTATION CHOICE*, ruled here.
6. **The first impact and the cap, made exact.** All of these are from the accepted integration (the partial file's
   last returned line):
   - **The first candidate** is the first entry of `regimes` and `injections`. Its regime and its N are recorded.
   - **The fraction drained at the first impact** is that entry's α_eff, which is 0 where it drained nothing.
   - **Its share** is its ΔK / `K_final`, which is None where `K_final` = 0.
   - **The drains before parking** are the injections with α_eff > 0 and N ≤ `parking_N`, or all of them where
     the field does not park. Say whether the parking leftover's drain is among the injections, and count it
     apart if it is.
   - **The cap.**
     - In 02b's runs the child also keeps each candidate's ℓ (`DrainCandidate.ell`). The cap acted at a one-loop
       candidate where A · `DRAIN_C1` / ℓ ≥ `DRAIN_ALPHA_CAP`.
     - For 02's lines, where ℓ is not kept, it acted where α_eff = `DRAIN_ALPHA_CAP` exactly. This relies on 02's
       count of no one-loop candidate with w_gate < 1, which is stated beside it.
     - On one 02b run with capped drains, check the two counts against each other. A difference is reported, not a
       stop.

   *IMPLEMENTATION CHOICE*, ruled here (§0, fourth and fifth points).
7. **"Bitwise"** for S1, S2, the reproduction check and the spot checks is note 02's ruling 6, unchanged. Under S2
   across A, the record's `A` differs by construction; under S1 its `gap_prescription` does. *As note 02.*
8. **The tool check** is note 02's ruling 9, unchanged, on `P-2197` at A = 10, gap `zero`. *As note 02.*
9. **The failures.**
   - A failure at A = 0.05 of 02's two kinds (the step budget; "the effective minimum was not found") extends
     `[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]`. It is added to the board entry as a dated
     paragraph; the entry's text is not rewritten. Its row in `.documents/OPEN_ISSUES.md` changes only if its hook
     no longer holds.
   - A failure of another kind, at any A, opens an issue of its own.
   - A failure whose reason points at the drain law's own code (`drain_regime`, `drain_alpha_effective`), or at the
     wall search or the inside-wall guard, is a stop.

   *As the prompt intends; made exact.*
10. **No file but these:**
    - `.documents/one-loop-drain/measure-02b/` and its `out/` (text only);
    - `.documents/one-loop-drain-verification.md` (§2b added after §2);
    - `logs/02b-measure-wide-A.md`;
    - the board;
    - `.documents/OPEN_ISSUES.md`, if §3 changes.

    Nothing under `var/` is committed, and nothing under `.documents/one-loop-drain/measure/` changes. *STRUCTURALLY
    REQUIRED*; followed.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status` before
  committing. Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The Gate, and the five suites before anything else. They run serially: ComputeTargets in its two halves, the
     `test_[g-z]` half and `tools/tests` in the background, waiting for each notification.
  2. The scripts, then the reproduction check (ruling 4).
  3. §1.4's steps under ruling 1, each a detached batch.
  4. The spot checks and the tool check.
  5. The analysis scripts, over 02's lines and 02b's together.
  6. The suites again; the documents; the records; the commit.
- **Load.** Record the 1-, 5- and 15-minute loads at each batch's start and end, beside its wall time.
- **Scratch.** Outputs go under `var/one-loop-drain/02b/`. Probes and suite outputs go in the agent's own
  subdirectory of the session scratchpad. Nothing goes in `/tmp`. Nothing is written under `var/one-loop-drain/02/`.
- **No Ray.** Nothing starts a cluster, and no `ray` command is run. No store is written; if the store is opened, it
  is opened `mode=ro`.
- **Stop conditions are real:**
  - prompt §3's list;
  - ruling 4's reproduction check;
  - ruling 5's projection or worker floor;
  - ruling 9's last point.

  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

Done at `02e6233`, before this note was committed:
1. **The Gate above.**
2. **The reading of §0.**

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout, in the background. Give it:
- the prompt, the README, the board, and orchestrator note 02 (whose rulings 2–9 it applies as this note says);
- `HEAD`, and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent reads `orchestrator/prompt-02.md` but no other file of `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the files of ruling 10. Its log is `logs/02b-measure-wide-A.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **It starts nothing that outlives its turn** but the batches of §1.4, and every one of them must have ended before
  it commits.
- **Stop and ask** on any of the conventions' stop conditions.
- **It replies with:**
  - the log's path, the message as committed, and `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - the scripts: each import, override and copy (ruling 3);
  - the reproduction check, with its result;
  - the batches: tasks run, derived by S1 and by S2, timed out and failed, with each batch's wall time, workers,
    loads and peak RSS;
  - ruling 5's projection, with its numbers;
  - the spot checks and the tool check, each with its result;
  - the Wide gate, item by item (prompt §2.1–§2.7), with the headline numbers, and the first impact at A = 5 and 10
    in full;
  - the handover for U14, verbatim;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — seven checks

1. **Scope.**
   - `git show --stat <agent>` lists only ruling 10's files. Its parent is this note's commit, or a later commit
     that is not the agent's.
   - `git status --porcelain --untracked-files=all` is empty, and nothing is pushed.
   - `git diff <agent>^ <agent> --` over the code paths, and over `.documents/one-loop-drain/measure/`, is empty.
   - `measure-02b/out/` holds text only.
   - `var/one-loop-drain/02/` is unchanged: the same 180 lines of `sample.jsonl`, and no file newer than this note.
2. **Nothing left running.** No pid under `var/one-loop-drain/02b/` answers `kill -0`, `batch.end` exists for every
   batch, and `runregistrykit list` is unchanged.
3. **The sample is complete.** 02b's `sample.jsonl` has one line for each of the 360 (history, A, prescription)
   tasks, each `run`, `S1`, `S2` or `timeout`, with no gaps and no duplicates. Re-derive every S1 and S2 decision
   myself, S2's from 02's A = 1 lines.
4. **A shortcut, re-run.** Run one task that S1 derived and one that S2 derived, other than the spot checks', with
   02b's driver in a process of my own. Compare each with its source under note 02's ruling 6.
5. **The tool, re-run.** Run `tools/history_and_bbn.py` on one laboratory-M task at A = 5 (not 2197). Compare its
   `history`, `drain`, `dissipation` and `bbn` lines with the driver's line for it.
6. **The numbers.**
   - Recompute from the two `sample.jsonl` files and the partials, in a scratch script:
     - the first-impact fraction of every P and W history at A = 10;
     - 2197's ΔN_eff today at all nine A;
     - the count of histories that fail at A = 0.05.
   - Recompute the cap count for one run from its partial file and the ℓ kept.
7. **The records.**
   - The log is in §5.1's form, with every deviation classed and §0's rulings among them.
   - The verification document's §1 and §2 are unchanged, and §2b follows prompt §2's seven items. The U14
     handover states measurements and options, and makes no choice.
   - On the board: row 02b, its items, the Wide gate, U14 "open for the user, with 02's and 02b's measurements",
     and **Last updated**. `.documents/OPEN_ISSUES.md` matches §3, with its count and date.
   - `CLAUDE.md` is unchanged, and so are the suites' counts.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 02b landed, the Wide gate held, and this note added to the list of orchestrator notes, marked
  "used for 02b";
- an *Orchestrator review of prompt 02b* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s campaign row.

Residue is fixed in that commit or a follow-up of its own. Anything that is not residue is opened on the board's §3
and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the Wide gate: the sample, the first impact and what follows it at A = 5 and 10, the regime shares across the
  nine A, the failures, and the cost;
- U14, put to the user with the measurements, for the user to rule in their own words;
- that 03 (the store) is written after that ruling.
