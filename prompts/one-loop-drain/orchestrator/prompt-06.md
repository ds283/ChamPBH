# Orchestrator — prompt 06, the rehearsal

Read [`../README.md`](../README.md) first: §5, and §8 whole, in particular §8.4 (row 06), §8.5 (the rows Bitwise and R),
§8.12 (P33–P40) and §8.14 (P50–P57). Then the prompt. This note binds 06 only, and governs where it differs from the
prompt (prompt, "Written"). Note 01's conventions bind as well: a tolerance is a number with its reason, timing is
taken under load, and the suites run serially.

**The prompt:** [`06-rehearse.md`](../06-rehearse.md) · **Gates:** "Bitwise" and "R" · **Closes:** nothing ·
**Opens:** only what the work finds · **Model:** Opus, as the prompt recommends · **Network:** none. Nothing is pushed.

**When:** at once, at the user's request of 2026-11-07 ("Please write the orchestration note and dispatch it, and
afterwards carry out a review of the implementation agent's work").

**Gate**, checked at `d05b625` on 2026-11-07 between 07:58 and 08:03 local time, before this note was committed:
- `one-loop-drain` is checked out in the main checkout at `d05b625`, the commit that adds prompt 06.
  `git status --porcelain` is empty, and `git worktree list` shows the main checkout only.
- `tools/ray_cluster.sh status` exits 0: the lock free, no Ray process of `/tmp/ray-cpbh`, ports 6380, 6381, 8267
  and 52367 free. `raypoolkit.rehearsal_lock status`: free, exit 0.
- `./venv/bin/python -m runregistrykit list`: 23 runs, all finished; none running, stale or unknown.
- `var/one-loop-drain/02d/sample.jsonl` has 240 lines and `tasks/` 977 files; `var/one-loop-drain/05/exports/QCD_Cosmology/`
  exists, with 230 bounces.csv. None of `var/datastores/one-loop-drain-06-*`, `var/one-loop-drain/06/` and
  `.documents/one-loop-drain/rehearse-06/` exists.
- The suites were not re-run for this note. 05's review left them at 26, 665 (219 + 446), 163, 191 and 72, all passing,
  and nothing has touched code since. The agent measures them again first (prompt §4).
- **The load.** 61.55 / 61.58 / 51.00 at 07:58, with little CPU in use (OrbStack's VM, Spotlight and two other Claude
  sessions led `ps`); 8.63 / 32.51 / 40.96 at 08:03. The machine has 10 cores and 16 GB of memory, and it is shared.
- `PYTHONPATH` is unset in this session's shell.

## 0. What the orchestrator found

For this note the orchestrator read: prompt 06 and README §8.4, §8.5 and §8.14; `inelastic-reflection`'s verification
§5 (the commands) and its `11-compare-tool.txt`; `ray/_private/worker.py` around `_py_driver_sys_path`;
`tools/ray_cluster.sh`'s start; `RunDrivers/registered_run.py`'s launch of `main.py`; `config/argument_parser.py` at
`1561b60` and at the tip; `main.py`'s alpha rows, its banner and its units; `CosmologyConcepts/drain_model.alpha_label`;
`tools/compare_stores.py`'s header; `tools/bbn_from_store.py`'s options and its `stored:` line, and
`dk-and-rpk-migration`'s `out/10-readers.txt`; `ObjectFactories/ScalarHistory.py`'s `to_column`, `BOUNCE_COLUMNS` and
`bounce_row_columns`; `ComputeTargets/ScalarHistory.py`'s `DrainCandidate` and `bounce_rows`; `measure-02d/run_02d.py`'s
partial lines; `derive-05/run_05.py`'s comparison; the nine 02d tasks' `tasks.json` entries and `sample.jsonl` lines;
and one bounces.csv of 05's export.

Here is what it found where the prompt is silent, loose or wrong.

- **The nine one-loop histories are as §1.3 says.** Each is a 02d `success` at the build's (β, M, A), and each `argv` is
  `<β> <M> --drain-law one_loop --alpha <A>` and nothing else. Their 02d walls are 14–94 s (C-3233-A1 the longest,
  R-611-A0.1 76 s), and every one is seeded `tracking`. In `inelastic-reflection` 11 the build and the tool route agreed
  on all fifteen tuning keys by `float.hex`, so §2.2 (ii)'s tuning item has a precedent.
- **The base runs the prompt's commands.** `config/argument_parser.py` at `1561b60` has every option of §1.3 but
  `--A-values`, and no default differs from the tip's. `main.py` adds no α = 0 row when `--alpha-values` is given (the
  α = 0 of a run "given only `--A-values`" is `--alpha-values`' own default), so the counts 17 and 26 stand.
- **What the workers import.** In the venv's Ray, a driver appends to the job's `_py_driver_sys_path` its script's
  directory (`realpath(sys.argv[0])`'s, when that is on the driver's `sys.path`) and its working directory. Under
  `registered_run.py`, `sys.argv[0]` is `main.py`, so both are the tree's root. How a worker inserts those paths is in
  compiled code, so the probe is the only evidence; that is what §1.2 is for.
- **The working directory of a worker is probably the cluster's, the main checkout,** for a base driver too. PRyMordial
  reads `PRyMrates/` from `os.getcwd()`, and `Xav_EOS_spline` reads its CSV by a relative path. Both are tracked, and
  `git diff 1561b60 HEAD` over `PRyMrates/`, `PRyM/`, `CosmologyModels/GenericEOS/`, `exponential.yaml`,
  `requirements.txt`, `runregistrykit.toml` and `thirdparty/` is empty. No ignored file is needed at run time. So a
  worker reading data from the main checkout's working directory reads what the base would (ruling 1).
- **`PYTHONPATH` in the cluster's environment would point base workers at the main checkout.** `inelastic-reflection`
  11 started the cluster with none, and so does this session's shell (ruling 1).
- **§2.2 (ii)'s "same ordinal" is wrong.** A one-loop history's bounce rows interleave candidates with plateau events.
  For example, 05's P-2197 at A = 1 has ordinals 0–8: five `candidate` rows, `candidate_index` 0–4, and four `plateau`
  rows. A candidate row is matched to its `DrainCandidate` by `candidate_index` (ruling 2).
- **"By `repr`" depends on the units** (note 04, ruling 1; the user: units are a choice). The stored row holds `log_E` as
  `log_E_Mp4` and `log_H` as `log_H_Mp`, through `to_column`. That is the raw value bit for bit only because `main.py`
  builds in `Planck_units` (`main.py`, `units = Planck_units()`). The partial line also holds each injection's
  (N, α_eff, ΔK), and ΔK is stored as `delta_K_Mp4` (ruling 2).
- **`sample.jsonl` prints every float in full,** by JSON's `repr`. So "to the digits the line prints" in §2.2 (ii) means
  bit for bit. That is right for the dissipation values and ΔN_eff, which the integration computes. The build's Y_p and
  D/H come from a BBN stage that reads the history back from the store, while the tool's come from memory (ruling 3).
- **R7 as written does not reproduce the stored values.** `tools/bbn_from_store.py` uses PRyMordial's full network by
  default. `main.py` has built with the small one since `bbn-tolerance` 02. With `--small-network --variant prod`,
  `dk-and-rpk-migration` 10's re-solve printed the stored Y_p and D/H to all ten digits it prints (ruling 4).
- **§1.5's commands are shortened.** The plot scripts and `main.py --inventory` also take `--config exponential.yaml
  --database … --shards 16`, and the plot scripts take `--ray-address $ADDRESS`, as 11's §5 gives them. They read M and β
  from the stored grid (`plot_inputs.stored_grid`), so §1.5 rightly gives neither. 11's §5 names the cluster script at its
  old path; it is `tools/ray_cluster.sh` now (ruling 5).
- **`compare_stores.py` compares every client table it finds on the shards,** so `ScalarHistoryBounce` is in its list
  whenever a store holds it. §2.2 (i)'s stop applies only if it is missing all the same.

**Rulings.** Each is recorded in the log, with its class.
1. **The probe and the cluster's environment.**
   - The cluster is started as 11 started it, from the main checkout's root with `PYTHONPATH` unset:
     `tools/ray_cluster.sh start "one-loop-drain 06 (rehearsal)"`, alone in its Bash call.
   - The probe is run from each tree's root as `PYTHONPATH=. ./venv/bin/python -u <abs>/code_probe.py`, with `<abs>` the
     main checkout's `.documents/one-loop-drain/rehearse-06`. Its script directory is then on the workers' path too. It
     holds no package of ChamPBH, so the imports resolve as they do under `main.py`. The probe prints the job's
     `_py_driver_sys_path` as the driver set it.
   - Beyond §1.2's list, the probe prints from each process `os.getcwd()` and `os.environ.get("PYTHONPATH")`.
   - **Not a stop:** a worker whose working directory is the main checkout. The data files read from it are identical
     at both commits (§0, fourth point). Record what each process printed.
   - **A stop,** as §1.2 says: a module file outside the driver's tree, or a label or format that is not its tree's.
   - **The manifests.** Each base run's manifest must record `git_head` `1561b60` and the script hash of `main.py` at
     `1561b60` (`git show 1561b60:main.py`). Each tip run's must record a tip SHA and the tip's hash. A mismatch is a
     stop.
   - `store create` for the base store runs from the worktree's root with `PYTHONPATH=.`. Its sidecar must record format
     6; format 7 is a stop, since it would mean the tip's fingerprint code ran.

   *IMPLEMENTATION CHOICE*, ruled here (§0, third to fifth points).
2. **§2.2 (ii)'s candidates, exactly.**
   - **Matching.** Among a history's stored bounce rows, those with `source` = `candidate` are matched to the last
     partial line's `candidates` and `injections` by `candidate_index`. Their number must equal the length of both lists,
     and their indices must run 0, 1, …. Event rows (`plateau`, `wall`) are counted per history and reported. They are not
     compared, since 02d recorded none of them.
   - **What is compared, by `float.hex`** (None only with NULL, a string exactly):
     - from the `DrainCandidate`: `regime`, `ell`, `alpha_eff`, `w_gate`, `L`, `log_R`, `log_E` and `log_H`;
     - from the injection triple: `N`, `alpha_eff` and `delta_K`.
   - **The comparison is against the store's own conversion.** The expected column is
     `ObjectFactories.ScalarHistory.to_column(value, kind, units)`, with `kind` from `BOUNCE_COLUMNS` and `units` the
     build's (`Planck_units`, as `main.py` makes them). It never compares against the raw value. So the gate holds in
     any `UnitsLike` in which the build and 02d both ran. The tuning's fifteen keys are compared the same way, through
     whatever conversion their columns carry (none, if they are stored as they are).
   - **The count.** The number of values compared, per history and in total. The number that also equal the raw value
     bit for bit is reported as a measurement, never asserted.
   - **Not in 02d** (`K_before`, `log_k_ex`, `log_m`, the generation's fields, `log_T_Jordan`, `log_rho_kin`,
     `nonadiabaticity`): not compared in (ii), and named as such. (iii)'s byte comparison with 05's bounces.csv covers
     them.

   *STRUCTURALLY REQUIRED* (§0, sixth and seventh points). The prompt's "same ordinal" would compare a plateau row with a
   candidate.
3. **§2.2 (ii)'s values to their digits.**
   - **Bit for bit, a difference a stop:**
     - every field of `accepted.drain_values` against the stored drain record;
     - the dissipation values;
     - ΔN_eff today and at 1 MeV.
   - **Y_p and D/H.** Compared at full precision.
     - Equality is expected. The relative difference is reported with its size, for each of the nine histories.
     - A difference up to 5 × 10⁻¹⁰ relative is reported and is not a stop. That is ten significant digits, the precision
       11's tool route printed and its gate compared. The log says what in the store's round trip of the samples could
       cause it, or says it is unexplained.
     - A larger difference is a stop.

   *As the prompt says; made exact* (§0, eighth point).
4. **R7.**
   - Each call takes `--small-network --variant prod`, and `--phi-init-Mp 5`, `--beta` and `--M-Mp` as §1.5 says.
   - "Must print the stored Y_p and D/H" means two things. The `stored:` line is read and printed from the store, the base
     store included. And the re-solve's `prod` line equals that stored Y_p and D/H, rounded to the ten significant digits
     it prints.
   - Each call runs from the main checkout, with the tip's code.
   - A refusal, an error, or a re-solve that differs is a stop (prompt §6, "A driver or reader fails").

   *STRUCTURALLY REQUIRED* (§0, ninth point).
5. **The commands.** §1.4–§1.5 follow 11's §5 verbatim, with three changes:
   - this prompt's paths, slugs and options;
   - `tools/ray_cluster.sh` for the old path;
   - the base's launches from the worktree's root through its own `RunDrivers/registered_run.py`.

   The readers take `--config exponential.yaml --database <stem>.db --shards 16`, and the plot scripts take
   `--ray-address $ADDRESS` as well. The verification document's commands section gives every command as it was run.

   *As the prompt says; made exact* (§0, tenth point).
6. **The base worktree.**
   - The worktree is `<scratch>/base-1561b60`, with `<scratch>` =
     `/private/tmp/claude-35086/-Users-ds283--claude-spares-spare-3KG1mP/ac4eb94d-0bc8-4fda-a4e4-f553733c5d07/scratchpad/agent-06`.
   - The links are `ln -s /Users/ds283/Documents/Code/ChamPBH/venv venv` and the same for `var`, made in the worktree's
     root.
   - Nothing under the worktree is edited. Its `git status` is recorded before the first base run and after the last.
   - After `git worktree remove` and `git worktree prune`, `git worktree list` shows the main checkout only. The removal
     needs `--force` only if the links alone make the worktree dirty. If `--force` is used, the log says why.

   *IMPLEMENTATION CHOICE*, ruled here.
7. **The interruption's delay.**
   - The window is read from `reference-a`'s beats (`beat_times.py`), measured from the A = 0.1 banner. It opens at the
     first line that shows an A = 0.1 history stored, and closes at the line that ends that pipeline's scalar stage. The
     log names both lines.
   - `--after` is the window's midpoint, rounded down to a whole second.
   - If the window is shorter than 10 s, the delay is its opening + 2 s, and the log says so.
   - The load at `reference-a`'s launch and at `resumed-a`'s is recorded beside the choice.

   *IMPLEMENTATION CHOICE*, ruled here.
8. **The load.** The machine is shared, and its load has ranged from 8 to 62 this morning.
   - Each build's and reader's wall is recorded with the load at its start.
   - The 60-minute bound per run stands.
   - No run is delayed or repeated for load.

   *As the prompt says.*
9. **Files.** Prompt §7's list, and nothing else. The README is not edited.

   *STRUCTURALLY REQUIRED*; followed.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status` before
  committing. Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The Gate, and the five suites, before anything else. They run serially: ComputeTargets in its two halves, with
     the `test_[g-z]` half and `tools/tests` in the background, waiting for each notification.
  2. The scripts of §1.6, and the 02d check of §1.3.
  3. The worktree (§1.1), the cluster and the probe (§1.2).
  4. B0, the probe again, B1, B2 (§1.4).
  5. The readers (§1.5), the cluster's stop, R6, R7.
  6. The comparisons (§2), the worktree's removal (§1.1).
  7. The suites again; the documents; the records; the commit.
- **Scratch.** Run outputs go under `var/one-loop-drain/06/`. Probes and suite outputs go under `<scratch>` (ruling 6).
  Nothing goes in `/tmp` but the cluster's own directory, which only `ray_cluster.sh` names. Nothing is written under
  `var/one-loop-drain/0[1-5]*/`.
- **Ray.** One cluster, through `tools/ray_cluster.sh` alone. No `ray stop`, ever. A Ray CLI command takes
  `RAY_AUTH_MODE=token`. The token is never printed.
- **Waiting.** Launch, one check, then one background wait per run for its end line. No polling from the turn.
- **Stop conditions are real:**
  - prompt §6's, whole;
  - ruling 1's manifest or sidecar mismatch;
  - ruling 3's Y_p or D/H beyond 5 × 10⁻¹⁰ relative;
  - ruling 4's re-solve that differs;
  - a suite count that falls.

  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`. At a stop, the agent leaves
  the cluster up and stores where they are, and reports what is running, unless the stop is the lock's.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

Done at `d05b625`, before this note was committed:
1. **The Gate above.**
2. **The reading of §0.**

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout, in the background. Give it:
- the prompt, the README, the board and this note;
- `HEAD`, and the Gate as measured.

The agent reads this note and no other file of `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with prompt §7's files. Its log is `logs/06-rehearse.md`, in `dk-and-rpk-migration`
  README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **Nothing it starts outlives its turn**, but the three stores and their sidecars. The cluster is stopped, the lock is
  released, the worktree is removed, and no pid recorded under `var/one-loop-drain/06/` answers `kill -0`.
- **Stop and ask** on any of the conventions' stop conditions.
- **It replies with:**
  - the log's path, the message as committed, and `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - the probe's output from both trees, before and after B0;
  - the per-run table (§1.4's record), and each manifest's `git_head`, `git_dirty` and script hash;
  - the interruption: the window, the delay, when the signal fell, what was stored and pruned;
  - the gate Bitwise item by item, with the counts per table, the new columns' checks and any exceptions;
  - the gate R item by item, with (ii)'s counts and Y_p's and D/H's largest relative differences;
  - the readers R1–R7, with exits, walls and `shasum`s;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — seven checks

1. **Scope.**
   - `git show --stat <agent>` lists only prompt §7's files. Its parent is this note's commit, or a later commit that is
     not the agent's.
   - `git status --porcelain --untracked-files=all` is empty, and nothing is pushed. `git worktree list` shows the main
     checkout only.
   - No line of code or test changes, nor `CLAUDE.md`, `.gitignore` or `PRyM/`. Nothing under `var/` is tracked.
     No PDF and no PNG is committed.
2. **The suites, re-run by me**, serially as the conventions say: the counts the log states, all passing, none fallen.
3. **The cluster, the registry and the code.**
   - `ray_cluster.sh status` and the lock's `status`: free, no process.
   - `runregistrykit list`: every run of this prompt `done` or `killed`, as the log says.
   - Each manifest's `git_head` and script hash against `git show <sha>:main.py`, by my own `shasum`.
   - The probe's outputs as committed: every module file under its own tree, the labels each tree's.
   - The sidecars' formats, 6 for the base and 7 for the others.
4. **Bitwise, independently.** A read-only script of my own over the base and reference stores, by `sqlite3` `mode=ro`:
   - the 17 histories matched;
   - every shared column of `ScalarHistory` and `ScalarHistoryValue`, and the BBN values, by `float.hex`;
   - the tip's new columns against 03's rules;
   - all compared with the agent's counts.
5. **R, independently.**
   - `tools/compare_stores.py` on the reference and resumed stores, and `store fingerprint` on each, run by me.
   - My own comparison of the nine one-loop histories against 02d:
     - the tuning keys;
     - `drain_values`;
     - every candidate row by `candidate_index` through `to_column`;
     - ΔN_eff, Y_p and D/H.
6. **The readers and exports.**
   - `histories.csv` of `plot-reference`: 26 rows in P36's sort, and a bounces.csv for each one-loop history and none for
     a fixed-law one.
   - The nine bounces.csv against 05's, by `cmp`, run by me.
   - The tool's outputs at c_a = 1 re-run by me on `plot-reference`, and byte-identical to the agent's.
   - R4's log and the absence of its output. R7's lines against the stored values. The `shasum`s unchanged.
7. **The records.**
   - The log is in §5.1's form, with every deviation classed and §0's rulings among them. Its §7 gives 07 the exports
     and 08 the stores, the gates and the costs, as prompt §5 asks.
   - The verification document's §1–§5 are unchanged, and §6 follows prompt §3 with the commands verbatim.
   - The board: row 06, its items, the gates Bitwise and R in §2, an entry in §5, and **Last updated**.
     `.documents/OPEN_ISSUES.md` matches anything opened.
   - The token grep over the commit finds nothing.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 06 landed, the gates Bitwise and R held, and this note added to the list of orchestrator notes, marked
  "used for 06";
- an *Orchestrator review of prompt 06* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s campaign row.

Residue is fixed in that commit or a follow-up of its own. Anything that is not residue is opened on the board's §3
and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the gate Bitwise: what matched, and any exceptions;
- the gate R: the stores, 02d, the exports;
- the readers, and the costs measured for the rebuild;
- anything opened;
- that 07 (the figure tools) is written next.
