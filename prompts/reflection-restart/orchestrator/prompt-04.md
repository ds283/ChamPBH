# Orchestrator — prompt 04, the close

Read [`../README.md`](../README.md) first: §0.1, §2.1, §5, §6.3, §6.4 whole (the handover as the user restated
it on 2026-11-02) and §7. Then the prompt. This note binds 04 only. The conventions of `inelastic-reflection`'s
notes and of notes 01 and 03 bind here as well: a tolerance is a number with its reason, timing is taken under
load, and the suites run serially.

**You do not write code.** You may:
- run the suites, the scanner, and the scripts of `.documents/reflection-restart/close/` that the log names;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`04-close.md`](../04-close.md) · **Gate:** "Close" · **Closes:**
`[run-2026.8.0-an-uncapped-outward-step-after-a-reflection-crosses-the-wall]`, and the campaign · **Narrows:**
`[00-fixed-alpha-laboratory-M-histories-fail-in-classical-turning-point]` · **Opens:** only what the work finds
· **Model:** Opus, as the prompt recommends · **Network:** none. Nothing is pushed.

**When:** at once, at the user's request of 2026-11-02 ("Please write the orchestration note for prompt 04, and
dispatch it").

**Gate**, checked at `a27eb1d` on 2026-11-02 at 22:26 UTC, before this note was committed:
- `reflection-restart` is checked out in the main checkout at `a27eb1d`, the commit that adds prompt 04;
  `git status --porcelain` is empty; `b81eab8` is an ancestor of `HEAD`. `git worktree list` also shows
  `.claude/worktrees/quanta-diagnostics` (branch `one-loop-drain`, `0136a3d`), another session's. Nothing here
  touches it.
- `./venv/bin/python -m runregistrykit list`: 23 runs, all finished.
- The store `var/datastores/science-2026.8.0` has 16 shards. Over all of them, 42 histories have a
  `failure_reason` containing `log_m_eff_squared`, and 9 have one beginning `classical_turning_point` and
  containing "not monotone": the counts of prompt §0.2.
- `git show one-loop-drain:prompts/one-loop-drain/source/probe/drain/results/2197-base-stored-alpha.log`
  prints the failure that prompt §0.2 quotes (β = 1.185, φ = 2.950090845531173e-07,
  ln E = −306.7166549210319).
- `.documents/reflection-restart-verification.md` has §1 and §2 and no §3.
- The suites were not re-run for this note; 03 and its review left them at 26, 514 (122 + 392), 130, 147 and
  68, all passing. The agent measures them before anything else (prompt §3).
- Load averages 4.88 / 5.81 / 4.69, on 10 cores. The machine is shared.

## 0. What the orchestrator found

For this note the orchestrator read the scanner (`select_from_store`, `scan_one`, `run_batch`, its timeout
record), the scanner at `89a0760`, `_restart_first_step` and the wall search's constants, log 03 §7–§8, the
store's failure rows over all 16 shards, and the run logs of blocks A and A2 for the nine. Here is what it found
where the prompt is silent, loose or wrong.

- **The store is sharded, and `select_from_store` reads every shard.** A spec's `store` names its shard by
  path. 2197 is in `science-2026.8.0-shard0005.db` and 2188 in `shard0003.db`; neither serial appears in another
  shard. The nine sit in shards 0000 (1001, 2735), 0001 (766), 0002 (1592), 0003 (2208), 0004 (2199) and 0015
  (403, 2386, 2480). The 42 are spread over 13 shards. The prompt's Gate names `shard0000.db` only as the
  store's first file; the counts are over all 16.
- **A spec carries no failure reason.** `select_from_store` returns `"failure": bool` only. Filtering the 42 on
  `log_m_eff_squared` needs `failure_reason`, read read-only from the spec's shard by serial.
- **The stored failure reasons are truncated.** The nine's `failure_reason` ends inside the bracket ("a midpoint
  of the bracket [-42"). φ, ln E and the normalization are complete, which is all prompt §1.2 (c) takes from
  them. The probes are not stored, and the run's stdout lines are no more complete.
- **The nine failed in the tuning's seed integration.** In block A2, 1592 (β = 1.1775, α = 10⁻³) was in a batch
  that started at 14:59:25 UTC; its status updates came at 15:23 and 15:28, and it failed in the seed
  integration at about 15:30, under the cluster's load. On the fixed tree, if a seed integration gets past its
  failure, the tuning goes on, at about 600 s per integration for these histories (2197 stored 624 s, 2188
  653 s). The three at α = 10⁻³ may therefore pass `--timeout 3600`. A timeout record holds the progress the task
  had reached.
- **The batch spawns its workers** (`multiprocessing.get_context("spawn")`). A patch made in a parent process
  does not reach them. Step (b)'s patch, and step (c)'s recording of candidates, need a script that calls
  `scan_one` in its own process.
- **The scanner at `89a0760` takes the same specs.** It has `select_from_store`, mode `store`, `--timeout` and
  the timeout record; a spec without `"variant"` runs the production code of that tree; its records have no
  `restart_retakes`. The worktree needs nothing untracked: `thirdparty/` and `PRyMrates/` are tracked, the
  scanner runs no BBN, and the shard paths and `--output` can be absolute.
- **F3 is replaced cleanly.** `_restart_first_step(y, global_max_step, h_floor, cap_fraction)` returns `None`
  (SciPy's own choice) where π ≤ 0. The step loop looks it up at call time (`ScalarHistory.py:1734`), so
  replacing it with a function that returns `None` restores SciPy's choice at every restart, with the guard left
  live.
- **The constants:** `WALL_SEARCH_HALVINGS = 4` (`AbstractModel.py:105`), `WALL_ROUNDING_QUANTA = 16` (`:112`);
  `_log_V_minus_floor` at `:137`, `_search_log_field` at `:183`; `classical_turning_point` at `:351` and `:546`.

**Rulings.** Each is recorded in the log, with its class.
1. **The specs.**
   - The 42: from `select_from_store("var/datastores/science-2026.8.0", blocks=["C"])`, keep those whose
     `failure_reason`, read from `spec["store"]` read-only, contains `log_m_eff_squared`. Then build as prompt
     §1.1 says.
   - 2197 and 2188: `select_from_store(…)` filtered on `(shard name, serial)`, (`shard0005`, 2197) and
     (`shard0003`, 2188), with absolute shard paths.
   - The nine: as the 42, keyed on their nine (shard, serial) pairs.

   Each list is written under `var/reflection-restart/04/` before it is run. *STRUCTURALLY REQUIRED* (§0, first
   two points).
2. **The runs and their budget.**
   - At most eight scan workers at once over all batches and scripts.
   - The 42 first, alone (`--jobs 6`, `--timeout 1800`).
   - Then the nine (`--timeout 3600`), with the three at α = 10⁻³ first in the task list. They may run
     alongside (a)–(c), within the eight.
   - (a), and the scripts of (b) and (c), use a timeout of 3600 s.

   A timeout is recorded with the progress it reached, and is not a stop. It is not rerun with a longer limit.
   For a nine's timeout, the log says whether the progress is past the stored failure's φ. *IMPLEMENTATION
   CHOICE*, ruled here (§0, fourth point).
3. **The worktree of `89a0760`.**
   - Created by `git worktree add --detach <scratchpad>/rr04/wt-89a0760 89a0760`, where `<scratchpad>` is the
     session scratchpad.
   - It is run from that worktree's root, with `PYTHONPATH=.` and
     `/Users/ds283/Documents/Code/ChamPBH/venv/bin/python`. The output goes to this checkout's
     `var/reflection-restart/04/`, by absolute path.
   - Each script of `close/` that runs there prints `ComputeTargets.ScalarHistory.__file__`, and refuses to run
     unless that path lies under its working directory.
   - The worktree is removed with `git worktree remove` before the commit, and `git worktree list` is shown
     after.

   *IMPLEMENTATION CHOICE*, ruled here.
4. **Step (b) runs in process.**
   - A script of `close/` replaces `ScalarHistory._restart_first_step` with a function that returns `None`, then
     calls the scanner's `scan_one` on the history's spec in its own process: one process per history, detached.
   - Its record is a JSONL line under `var/reflection-restart/04/`, compared with that history's `89a0760` line:
     `outcome`, `samples_sha256`, `log_one_plus_z_sha256`, `n_samples`, `accepted_steps`, `RHS_evaluations` and
     `reflections`, plus `restart_retakes` and `scan.signature_discarded` (both expected 0).

   *STRUCTURALLY REQUIRED* (§0, fifth and seventh points).
5. **Step (c)'s candidates are recorded passively.**
   - A script of `close/` wraps the model's `classical_turning_point` so that each call's φ, ln E, the returned
     turning point and the margin are written out. The margin is ln V − ln V_floor at the root, in quanta of
     ulp(max(|ln V|, |ln V_floor|)), as `_log_V_minus_floor` measures it. The wrapper changes nothing it passes
     back.
   - It runs once per history on each tree, in process.
   - Each run must reproduce that tree's (a) record: `outcome`, `samples_sha256` and `accepted_steps` for a
     success, `failure_reason` for a failure. A difference means the wrapper is not passive, and is a stop.
   - The failing call alone is reproduced in a script of its own.

   *IMPLEMENTATION CHOICE*, ruled here.
6. **What is compared with the probe.**
   - On this tree, 2197 and 2188 must fail with the probe's reason: the same φ and ln E to every printed digit,
     and the same bracket.
   - The probe's wall time (117 s, 115 s) is not compared, since the scanner observes every step.
   - Where the scanner's record and the probe's JSON differ in a field both hold, the log says which, and by
     how much.

   *As the prompt says; made exact.*
7. **The verdict's evidence.**
   - Verdict (ii) needs three things:
     - the failing call raises when run alone;
     - its root lies within `WALL_ROUNDING_QUANTA` + 1 quanta;
     - the nine's roots, computed from their stored φ and ln E, lie within the same bound.
   - The 1001 case (about 16.5 quanta in prompt §0.2) is reported with its number, whichever side of the bound
     it falls.
   - If any of the nine, or 2197 or 2188 on this tree, fails with its root outside that bound, that is not
     verdict (ii), and is a stop.

   *As the prompt says; made exact.*
8. **The issues.**
   - Under (ii), the fixed-α issue is narrowed on the board's §3.1, and its index row's hook follows. The row
     stays in §1.21; the orchestrator moves it at the review or at the merge.
   - The uncapped-step issue closes exactly as prompt §1.3 says, with the precedent named.
   - A failure of a kind not open as an issue (ruling 2, prompt §1.1) is opened on §3 and in the index in the
     same commit, with the count corrected.

   *As the prompt says; restated.*
9. **No file but these changes:**
   - `.documents/reflection-restart/close/` and its `out/`;
   - `.documents/reflection-restart-verification.md` (§3 added);
   - the five handover files of prompt §1.4;
   - the `inelastic-reflection` board (its Closed pointer line);
   - `logs/04-close.md`;
   - this campaign's `IMPLEMENTATION_STATE.md`;
   - `.documents/OPEN_ISSUES.md`;
   - `prompts/INDEX.md`.

   The task lists and the JSONL outputs stay in `var/`. *STRUCTURALLY REQUIRED*; followed.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status` before
  committing. Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The Gate, and the five suites before anything else: serially, ComputeTargets in its two halves, the
     `test_[g-z]` half and `tools/tests` in the background, waiting for each notification.
  2. The 42 (§1.1), detached.
  3. The worktree; (a) on both trees; (b); (c); the nine (d). These run detached, within ruling 2's budget.
  4. The verdict; the issues; the handover; the verification document; the board; the commit.
- **Long runs.** Every batch and script is launched detached (`nohup`, pid recorded), checked once, and waited
  on by a background command that ends with it, not by polling. Nothing in `ComputeTargets/ScalarHistory.py`,
  `CosmologyConcepts/` or the scanner is edited at any point (prompt §2).
- **Scratch.** Outputs go under `var/reflection-restart/04/`. Probes, suite outputs and the worktree go in
  `<scratchpad>/rr04/`. Nothing goes in `/tmp`.
- **No Ray.** Nothing starts a cluster, and no `ray` command is run. No store is written, and the 2026.8.0 store
  is opened read-only.
- **Stop conditions are real:**
  - a suite count other than 26, 514 (122 + 392), 130, 147, 68, or a failing test;
  - a count of the 42 other than 31 / 10 / 1;
  - any `log_m_eff_squared` failure or kept signature step among the 42;
  - a wrapper that is not passive (ruling 5);
  - verdict (i) or (iii), or a root outside ruling 7's bound;
  - any change that prompt §2 forbids becoming necessary.

  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch, and another session works in the `one-loop-drain`
  worktree. The agent does not assume `HEAD` is its own, never checks out `one-loop-drain`, never touches that
  worktree, and reads that branch by `git show` only.

## 1. Before you dispatch

Done at `a27eb1d`, before this note was committed:
1. **The Gate above.**
2. **The reading of §0**: the failure rows over all 16 shards (42 and 9, and the shards of 2197, 2188 and the
   nine), the A2 log's timing of 1592, the scanner at both trees, and F3's hook.

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout, in the background. Give it:
- the prompt, the README, the board;
- `HEAD`, and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the files of ruling 9. Its log is `logs/04-close.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **It starts nothing that outlives its turn.** Every run must have ended, and the worktree must be gone, before
  it commits.
- **Stop and ask** on any of the conventions' stop conditions.
- **It replies with:**
  - the log's path and the message as committed; `git show --stat` of its commit;
  - the suites, with counts, times and `uptime`;
  - the 42: counts by outcome, the guard's retakes, the signature counts, and every failure with its reason;
  - 2197 and 2188: the records of (a) on both trees, (b)'s comparison, (c)'s margins (the failing root, and the
    least margin on each tree), and the failing call alone;
  - the nine: each outcome, and for a failure its reason, φ, ln E and root margin;
  - the verdict, with its evidence;
  - the issues as closed, narrowed and opened, and the index count;
  - the handover's five notes, by file and section;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — seven checks

1. **Scope.**
   - `git show --stat <agent>` lists only ruling 9's files, and no code or test file. Its parent is this note's
     commit, or a later commit that is not the agent's.
   - `git status --porcelain --untracked-files=all` is empty. Nothing is pushed.
   - `git worktree list` shows the main checkout and the `one-loop-drain` worktree only.
   - `git diff a27eb1d <agent> -- ComputeTargets CosmologyConcepts tools config` is empty.
2. **Nothing left running.** No recorded pid answers `kill -0`, and `runregistrykit list` is unchanged.
3. **The 42.** Count the JSONL's records by outcome myself, and the signature and retake fields. Re-run two
   tasks on the commit's tree: one at 10⁻¹² and the one at 10⁻¹³.
4. **The failing call.** Run the script that reproduces the failing call alone, and the margin computation for
   the nine. Check one margin by hand from its φ, ln E and the normalization.
5. **The two trees.** Read (a)'s records on both trees and (b)'s comparison. Recompute the bitwise
   comparison of (b) from the JSONL lines myself.
6. **The verdict.** Read it against ruling 7 and prompt §1.2. Check that the issue's narrowed entry and its
   index hook say the same, and that the proposed destination is a proposal, not a ruling.
7. **The records.**
   - The log is in §5.1's form, with every deviation classed and §0's rulings among them.
   - The verification document gains a §3 and leaves §1–§2 alone; `out/` holds text only.
   - The five handover notes are dated and additive.
   - The `inelastic-reflection` board's pointer line names the precedent.
   - The board is CLOSED, with row 04 and the Close gate. `.documents/OPEN_ISSUES.md`'s count and date are
     right. `prompts/INDEX.md`'s row is closed.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 04 landed, and this note added to the list of orchestrator notes, marked "used for 04";
- an *Orchestrator review of prompt 04* paragraph on the board, holding what each check found.

Residue is fixed in that commit or a follow-up of its own. Anything that is not residue is opened on the board's
§3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the Close gate: the 42, the issue closed, and the handover;
- the verdict on 2197 and 2188, and the destination proposed for the fixed-α issue, which is theirs to rule;
- that `reflection-restart` is closed and ready to merge into `one-loop-drain` (its U1) at their word. Nothing is
  merged or pushed.
