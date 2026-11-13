# Orchestrator — prompt 02, measure the effect

> **Used for 02.** Dispatched 2026-11-13 at `e1fd6bc`. 02 stopped once, at the code probe, and was resumed on the
> user's ruling (one cluster per side). It landed as `be81f8b` and was reviewed in the commit that adds this line. The
> review is on the board.

Read [`../README.md`](../README.md) first: §0, §1, §2.1, §3, §5, §6.1 as ruled, and §9. The user's words on the plan
are "U1 to U6 are fine as proposed; U7 doesn't need to be in scope here." §9's P13–P20 were written on 2026-11-13 at
the user's request ("Please write prompt 02 and its orchestration note") and stand unless the user rules otherwise.
Then read the prompt. This note binds 02 only. The conventions of earlier notes bind here as well: a tolerance is a
number with its reason, timing is taken under load, and the suites run serially.

**You do not write code.** You may:
- run the suites, the scans, the comparison scripts and the scripts the log names;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`02-measure-the-effect.md`](../02-measure-the-effect.md) · **Gates:** "Threshold" and "Effect" ·
**Closes:** nothing · **Opens:** only what the work finds · **Model:** Opus, as the prompt recommends · **Network:**
none. Nothing is pushed.

**When:** written 2026-11-13. It is dispatched on the user's word, from this context or a fresh one, which re-checks
the Gate before dispatch (§1).

**Gate**, as measured by the planner at `850cc72` on 2026-11-13 at 09:30 GMT, before this note and the prompt were
committed:
- `electroweak-eos` is checked out in the main checkout. `5947b4f` is an ancestor of `HEAD`. `git worktree list` shows the
  main checkout only.
- `git status --porcelain` shows **the user's own uncommitted work**, which is not the campaign's and is not touched:
  - ` M CLAUDE.md`;
  - ` M science-runs/2026-11-08-science-2026.10.0/README.md` and ` M science-runs/README.md`;
  - `?? science-runs/2026-11-08-science-2026.10.0/relativistic/`.
  Nothing else is listed, apart from this note, the prompt and README §9 before their commit.
- `./venv/bin/python -m runregistrykit list`: 42 runs, all finished.
- `tools/ray_cluster.sh status`: no process of `/tmp/ray-cpbh`; ports 6380, 6381, 8267 and 52367 free.
- `var/datastores/science-2026.10.0.db` and `science-2026.10.0-reflections.db` exist with their sidecars. The run's
  exports are under `var/science-2026.10.0/out/` (18 GB). No `electroweak-eos-02-*` store exists.
- `df -h .`: **91 % used, 85 GB free** of 927 GB.
- Load averages 19.27 / 41.91 / 37.34 at 09:30, after **123.61** at 09:23. The machine is shared.

## 0. What the orchestrator found

The planner ran two probes on the new tree at `850cc72`, from the session scratchpad. Their outputs are not committed.

- **The new window, at M = 10⁻³ M_P, tuned** (`tools/surfing_scan.py --beta-values …`, as §1.2 will run it):

  | β | 2.90 | 2.94 | 2.96 | 2.97 | 2.98 | 2.985 | 2.99 | 2.995 | 2.998 | 3.000 | 3.002 | 3.004 | 3.006 | 3.010 |
  |---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
  | captured | QCD | QCD | QCD | QCD | no | no | no | no | no | no | no | EW | EW | EW |
  | T_deliver (GeV) | 1.870 | 1.972 | 1.912 | 1.714 | 1.953 | 20.65 | 35.19 | 43.16 | 46.57 | 48.49 | 50.19 | 51.72 | 53.09 | 55.47 |

  - The EW threshold lies in (3.002, 3.004], against S5's (3.019, 3.020], the reachability prediction 2.99918 and the
    notebook's 3.00285.
  - QCD's last capture lies in [2.97, 2.98). The window is about 0.03 wide, against S5's 0.021.
  - β = 3.000 delivers at 48.4902 GeV, as 01's tuned golden history does (48.49).
  - Across the window the first bounce rises from 1.95 GeV (2.98) to 20.6 (2.985) and 50.2 (3.002), where the old
    table's rose from 1.64 GeV (2.999) to 48.8 (3.019).
  - So S1-new will show QCD at 2.95 and EW at 3.05, and F-new will run from 2.950 to 3.050: **101 β**.
- **The cost of a tuned history** at M = 10⁻³ M_P was 34–44 s, 4 integrations each, at one-minute loads of 19–124.
  At that rate S1-new takes about 50 min, F-new about 65 and F-old about 40, each serial.
- **Σ at 300 GeV,** from `Xav_EOS_spline` with GeV units, is 5.685 × 10⁻³ on the old table (read through the redirect) and
  1.889 × 10⁻³ on the new. The code probe (P16) expects these values. At 200 GeV the two tables give 1.107 × 10⁻² and
  1.062 × 10⁻²; at 150 GeV, 1.681 × 10⁻² and 1.884 × 10⁻².
- **The old tree is the run's code.** `git diff --stat 5837a47 ec6bca8` restricted to Python changes only drivers'
  options (`--no-alpha-values`, `--no-plots`) and their tests. `git diff --stat ec6bca8 HEAD` over `main.py`, the plot
  scripts, `config/argument_parser.py`, `tools/surfing_scan.py` and `tools/history_and_bbn.py` is empty. Of what a
  history computes, only `ComputeTargets/ScalarHistory.py` (01a) and the CSV (01) differ.
- **01a is on the new side only.** On the old table, 01a changed 2 of 36 histories, both only at eV–keV, by dropping
  a late near-stall pair (β = 10 and 20, M = 10⁻³ M_P; log 01a §3). So the comparison is the table's change, together
  with 01a's wherever a history has such a pair. No sampled history has
  either β.
- **The run's export, as the comparison will read it.**
  - `fig6/inputs/histories.csv` holds the main store's 7956 rows (A, B, C and D): 6120 one-loop, 1836 fixed.
  - The stored M are converted values: 10⁻⁴² reads back as `9.999999999999999e-43`, 10⁻³² and 10⁻³⁰ likewise. Hence
    the prompt's match to a relative 10⁻¹².
  - The main store's per-history export (`out/main/`) holds one-loop rows only. The reflection store's (`out/reflections/`)
    holds every row, so R-natural's β = 2, M = 0.5, α = 0 history has its `fields.csv` there.
  - `fields.csv` carries `log_rhorad_Jordan_GeV4`, `gstar_rho` and `T_Jordan_GeV`, which is all P20's per-history drift needs.
- **The lab-M edge, old table.** In the run, at M = 4 × 10⁻³¹ M_P and A = 1, T_deliver is 1.69 GeV at β = 2.995 and
  1.68 at 3.000, then 14.26 at 3.005 and 34.66 at 3.010 (`fig6/inputs/histories.csv`). So `a-edge`'s 13 β straddle the
  old edge. Where the new edge lies at this M is the sample's to measure.
- **Every sampled β is in its block's grid.** P14's β were checked against `run_science_2026.10.0.sh`'s generators:
  1.2, 2.0 and 5.0 and the 13 in the dense grid; 2.0 and 3.0 in the sparse and the coarse; 2.0 in R-ladder's.
- **The disk.** 85 GB free, 37 GB short of Ray's 95 %. The sample's stores and exports are small, but the volume is
  shared. The prompt's Gate stops at 93 %.

**Rulings.** Each is recorded in the log, with its class.
1. **The user's uncommitted work.** `CLAUDE.md`, the two `science-runs/` READMEs and `relativistic/` are the user's.
   The agent never stages, edits, stashes or reverts them. While they are in the tree, the new side's manifests record
   `git_dirty` true. That is recorded, not a stop. 02 adds no test, so `CLAUDE.md`'s counts are not at issue. If at
   dispatch the user has committed them, this ruling lapses. *STRUCTURALLY REQUIRED*.
2. **The order of the work.**
   1. The Gate.
   2. The worktree (§1.1).
   3. The three scans, concurrently with one another, each in the background.
   4. The bath drift's tables-only part.
   5. Once the scans have ended: the cluster, the probes, the old side, the new side, the exports, the cluster stopped.
   6. The comparisons, the drift per history, the suites, the documents, the commit.
   The cluster does not run beside the scans. Neither would be wrong, but the machine is shared and loaded, and wall
   times taken on a machine running both would say little. *IMPLEMENTATION CHOICE*, ruled here.
3. **F-new's range.** It is read from S1-new as the prompt says. If S1-new's edges are 2.95 and 3.05 as §0 expects,
   F-new is 2.950–3.050, 101 β. If they are elsewhere, F-new follows S1-new and the log says why it differs from §0. A
   range over 151 β is a stop: it would mean S1-new found something §0 did not. *As the prompt says; made exact.*
4. **The code probe's breakage.** The probe takes an option, for the breakage record only, that makes its task and
   actor build `Xav_EOS_spline` from a given path instead of the installed CSV. Pointed at
   `prompts/electroweak-eos/data/pipeline_w_table_ec6bca8.csv` from the main checkout, it must exit 1. The installed CSV
   is never changed. *IMPLEMENTATION CHOICE*, ruled here.
5. **The 10 GeV label** (P18) is a label for one list, not a threshold. It is stated with the reason the prompt gives,
   and no conclusion rests on it. *As the prompt says*.
6. **Failures.** A history may fail on either side, as the run's 304 did. A new-side failure whose reason is of a kind
   the run record lists is recorded. Any other is a stop. A sampled history that failed in the run, and so has no
   stored values, is matched on its failure reason only. *IMPLEMENTATION CHOICE*, ruled here.
7. **No file but the prompt's §4 list.** *STRUCTURALLY REQUIRED*.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status` before
  committing. Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes it in its review.
- **Waiting.** Every long step runs in the background and the agent waits for its notification, or for its end line
  with `wait_for_line.py`. It never polls, and never sleeps in the foreground.
- **Scratch.** Probes, the worktree and suite outputs go in the agent's own subdirectory of the session scratchpad.
  Stores go under `var/datastores/`, and launch logs and exports under `var/electroweak-eos/02/`. Nothing goes in `/tmp`.
- **Ray.** Only through `tools/ray_cluster.sh` and `--ray-address 127.0.0.1:6380`. No `ray stop`. No shell command that
  runs the script names `/tmp/ray-cpbh`.
- **Stop conditions are real:**
  - the Gate;
  - ruling 3's bound, or a fine grid with more than one window or a capture between uncaptured β;
  - the lock's exit 3, or the script's exit 4 at `stop`;
  - a code probe that shows a worker on the other tree's code or table;
  - a side not ended three hours after its launch (do not kill it unasked);
  - any difference between the old side and the run (P17);
  - a new-side failure of an unlisted kind (ruling 6);
  - a plot script that refuses with no options that match the build;
  - a suite count that differs from 01's;
  - any change to code becoming necessary.
  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`. At a stop it leaves the
  cluster as it is and says so.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

The dispatching context repeats these on the commit that adds this note.
1. **The Gate**, as above and as the prompt states it, including `df` and the lock's `status`. If `git status` shows the
   user's files no longer modified, ruling 1 lapses: say so to the agent.
2. **`runregistrykit list`**, and the load average. A load above 30 is not a stop, but say so to the user before
   dispatching: the scans and the builds will take longer.
3. **The probes of §0** are the planner's. The dispatching context need not repeat them.

## 2. Dispatch

Start one fresh-context subagent, on **Opus**, in the main checkout, in the background. Give it:
- the prompt, the README and the board;
- `HEAD`, and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the prompt's §4 files. Its log is `logs/02-measure-the-effect.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **Stop and ask** on any of the conventions' stop conditions.
- **At the end it replies with:**
  - the log's path and the message as committed; `git show --stat` of its commit;
  - the old tree's checks, and the worktree removed;
  - the threshold: each scan's range, wall time and load; the edges on F-new and F-old; the window on each; the EW
    threshold beside 3.020, 3.00285 and 2.99918;
  - the code probe's output from both trees, and its breakage;
  - every registered run: id, state, wall, load at launch, `git_head` and `git_dirty`;
  - the exports' options;
  - the old side against the run: the count matched and the differences (none expected);
  - the effect: by block, the largest and median change of each observable; the histories whose T_deliver crossed
    10 GeV; the bounce rows' first divergence by history; the failures on either side;
  - the figures: for each, what it plots, and how far the sampled histories on it moved, in its units;
  - the bath drift: tables only on both tables, beside the issue's figures, and per history on three exports;
  - the suites, with counts, times and `uptime`;
  - the breakage record;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — six checks

1. **Scope.**
   - `git show --stat <agent>` lists only the prompt's §4 files. Its parent is this note's commit, or a later commit
     that is not the agent's. Nothing under `var/` is in it.
   - The user's files of ruling 1 are still modified and unstaged. `git worktree list` shows the main checkout only.
   - Nothing is pushed.
2. **The threshold.**
   - Read F-new's and F-old's rows at each edge. Re-run, myself on the new tree, the last uncaptured β and the first
     EW-captured β of F-new, and check that the rows equal the CSV's as strings, `wall_s` apart.
   - Each sidecar names its tree and label as the prompt says.
3. **The two trees.** Read the code probe's outputs: from the worktree every module, label, CSV hash and Σ is the old
   tree's, and from the main checkout the new tree's, in the driver, the task and the actor. Re-run
   `compare_with_run.py` myself and see it report every history matched and no difference.
4. **The effect.**
   - Re-run `compare_sample.py` and check that it reproduces the committed `out/` files.
   - Check three histories by hand against the two exports: one of `a-edge`, one of `c` and one of `d`.
   - The T_deliver list is complete: every history of `effect.csv` whose T_deliver lies on opposite sides of 10 GeV
     on the two trees is in it.
   - The figures' section states, and does not judge.
5. **The bath drift.** Re-run `bath_drift.py`. On the old table it reproduces the issue's figures to the digits the
   issue gives. The issue's board entry has the dated addendum. The issue is still open, and its index row is
   unchanged, or updated in the same commit if the agent narrowed it.
6. **The records.**
   - The log is in §5.1's form, with every deviation classed.
   - The verification document has §2, and §1 is unchanged (`git diff` on it adds lines only after §1).
   - On the board: row 02, its items, the Threshold and Effect gates. Any issue opened is on §3 and in
     `.documents/OPEN_ISSUES.md`.
   - `runregistrykit list` shows every run of 02 `done`. `ray_cluster.sh status` shows no process, and the lock is free.
   - The suites' counts equal 01's.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 02 landed, the Threshold and Effect gates held, and this note marked "used for 02";
- an *Orchestrator review of prompt 02* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s campaign row.

**Report to the user:**
- the agent's commit and the review commit;
- the threshold and the window, beside the old and the notebook's;
- what the sample moved, by block, and which histories changed their kind of delivery;
- what moved on each figure, in its units;
- the bath drift on the new table;
- that 03 (the documents, the issues and the handover) is written next.
