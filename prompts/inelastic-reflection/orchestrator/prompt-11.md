# Orchestrator — prompt 11, the measurement and the rehearsal

Read [`../README.md`](../README.md) first: §5 (the rules), §8.1 (R6, R12–R14, R17, R22, R24, R26–R31), §8.2
(P14, P18, P22, P23, P40–P47), §8.3 (U10, U11), and 11's charter in §8.4. Then the prompt and log 10 §8. This
note binds 11 only. The conventions of notes 02–10 bind here as well, and are restated below: a tolerance is
a number with its reason, timing is taken under load, and the suites run serially.

**You do not write code.** You may:
- run the suites, the measurement scripts and the readers the log names;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`11-measure.md`](../11-measure.md) · **Gates:** "Measure" and "R" · **Closes:** nothing ·
**Opens:** only what the work finds · **Model:** Opus, as the prompt recommends · **Network:** none. Nothing
is pushed.

**When:** the user asked on 2026-11-01 at 02:28 GMT for the dispatch to wait until 05:00, and to go ahead
only if the machine is quiet then: another workload was running. The defaults stand, by the user's word
("Please leave the defaults"): `ray_cluster.sh start` with its 10 CPUs, `run_grid.py --jobs 3`.

**Gate**, checked at `0a47ec4` on 2026-11-01 at 02:30 GMT, before this note was committed:
- `inelastic-reflection` is checked out in the main checkout, at `0a47ec4`, the commit that adds prompt 11.
  That commit came after 10 landed (`8c12afa`) and was reviewed (`5f7c3af`).
  - `git status --porcelain --untracked-files=all` is empty, and `git worktree list` shows the main
    checkout alone.
  - `HEAD` is the commit that adds this note, which touches only `orchestrator/`.
- `ray_cluster.sh status`: the lock free, no process of `/tmp/ray-cpbh`, ports 6380, 6381, 8267 and 52367
  free. `raypoolkit.rehearsal_lock status`: free.
- `./venv/bin/python -m runregistrykit list`: 11 runs, all finished, none `running` or stale.
- `var/datastores/jordan-normalization-09-reference.db`, its 16 shards and its manifest exist. Its sidecar
  names runs `a`, `b` and `c`, all `done`. The agent runs `store show` and checks `problems: none` (prompt
  Gate).
- Neither `var/inelastic-reflection-11/` nor `.documents/inelastic-reflection/` exists, and no
  `inelastic-reflection-11-*` store.
- `jordan-normalization` 09's helpers are in `var/jordan-normalization-09/tools/` (`beat_times.py`,
  `check.sh`, `compare_tool.py`, `histories.py`, `ray_state.py`, `silences.py`, `suite.sh`,
  `wait_for_line.py`, and two diffs).
- The suites were not re-run for this note. The agent measures the baselines (prompt §3.1):
  26 / 497 (122 + 375) / 129 / 123 / 68.
- The load averages were 5.8 / 10.0 / 9.9. The 05:00 check records them again before dispatch.

## 0. What makes this prompt unusual

**It is the first real run of the campaign's code under Ray, and its gate compares the new store with a store
of an older version, at a schema that has grown. The prompt names the comparison, the interruption and the
SM baseline loosely, and in one place its tool would run the wrong network.** For this note the orchestrator
read `jordan-normalization` verification §1.4, §1.5 and §1.14, `interrupt_when.py --help`,
`tools/bbn_from_store.py --help` and its `--sm-baseline` branch, `tools/history_and_bbn.py`'s network option,
the schemas of `ScalarHistory`, `ScalarHistoryValue`, `AdiabaticHistory` and `BBNData` in the 2026.7.0
reference's shards, its stored `label`, `version` and `extra_data`, `config/version.py`, the plot scripts'
order of `ray.init`, `plot_inputs.parameters` and the first `mkdir`, and PRyMordial's save flags. Here is what
it found where the prompt is silent, loose or wrong.

- **`tools/bbn_from_store.py` runs the full network by default.** Its `--help`: "default: the full network".
  `main.py` and `history_and_bbn.py` run the small one. Prompt §1.3 says "(small network)" but gives no flag.
  `--sm-baseline` needs no store stem. Ruling 1.
- **The 2026.7.0 reference cannot equal the new store in every column both schemas hold.**
  - `serial` and every `*_serial` column are row ids. The new store's shard key now includes α, so a history
    may sit on another shard under another serial.
  - `version` is a serial into the version table. `config/version.py` has `VERSION_LABEL = "2026.8.0"`; the
    reference is 2026.7.0.
  - `label` holds the job name and a timestamp
    (`jordan-normalization-09-reference-a-ScalarHistory-ExponentialModel(M=0.001Mp,n=1,beta=0.5)-2026-10-26T19:56:15`).
    `timestamp` is the time of the store.
  - The compute times (`compute_time`, `mean_RHS_time`, `max_RHS_time`, `min_RHS_time`, `NP_compute_time`,
    `BBN_compute_time`) are timings.
  - `extra_data` is JSON. In the reference it holds `cap_fraction`, `cap_floor`, `cap_global_max_step`,
    `jacobian_factor_max` and `accepted_steps`. The campaign may have added keys (note 08's
    `margin_warnings`).
  - `ScalarHistoryValue` matches its sample by `z_serial`, a row id into the redshift table.
  
  Ruling 2.
- **Prompt §5 has the tool runs only in a stop condition.** `jordan-normalization` 09's H3 ran them every
  time. They are cheap at M = 10⁻³ (JN 09: seven histories in about 6 CPU-minutes). Ruling 3.
- **`interrupt_when.py` takes one banner and signals after a fixed delay from it.**
  - "`CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID`" appears twice, once per α, so its first match is
    α = 0's. Only the pipeline banner `RUNNING PIPELINE FOR MODEL QCD_Cosmology, alpha = 0.1` is unique.
    `alpha = 0` does not contain it.
  - The delay must then cover the α = 0.1 pipeline's stages before its scalar stage, and fall between that
    stage's first and last stored history.
  - Since `tuning-followups` 02 the tune lines are quiet, so JN 09 §1.4's way (the last tune line of each
    history) may not be there to read. The reference's own `ScalarHistory.timestamp` gives the time each
    history was stored.
  - Dissipation histories at small M converge on the seed in a few seconds (log 06 §5.6). The window may be
    short.
  
  Ruling 4.
- **Prompt §1.2's reader commands are abbreviated.** They omit `--config`, `--database`, `--shards` and
  `--ray-address`, which JN 09 §1.14's plot command carries. Ruling 5.
- **The plot scripts make their output directories lazily.** The first `mkdir` is inside a figure or CSV
  writer, after `plot_inputs.parameters`, so "nothing under its output" can be read as "the output path does
  not exist". Ruling 5.
- **A build's wait may pass the ten-minute cap of a Bash call.** JN 09 waited 570 s for one α. The machine is
  shared. Ruling 6.
- **What does not need a ruling, checked:**
  - **The parking cut.** `PARKING_EPSILON` is used only in `ComputeTargets/ScalarHistory.py` (`_parking_cut`
    reads it at call time). Patching the module attribute is enough (R30).
  - **PRyMordial in parallel.** Its `save_bckg_flag`, `save_nTOp_flag` and `save_nTOp_thermal_flag` are
    `False`, and nothing in `PRyM/` asks Numba for an on-disk cache. Three solves at once write no shared
    file.
  - **The banner.** `>> RUNNING PIPELINE FOR MODEL QCD_Cosmology` is JN 09's banner (its log, line 17).
    Since 10 it ends `, alpha = <α:g>`.
  - **`--prune-unvalidated`** is still `main.py`'s option (`config/argument_parser.py` :356).
  - **The reference's histories.** Run `a` stored the four at M = 10⁻³, φ* = 5. Run `b` (φ* = 1) and run
    `c` (M = 0.1 and 10⁻⁵) do not collide with them on (M, β, φ*).

**Rulings.** Each binds the agent and is recorded in the log with its class.

1. **The SM baseline runs the small network.**
   - The command is `PYTHONPATH=. ./venv/bin/python tools/bbn_from_store.py --sm-baseline --small-network`,
     with no stem, output to `var/inelastic-reflection-11/sm-baseline.txt` with `uptime` before and after.
   - It is P43's Standard Model reference. Every point's Yp and D/H shift is taken from it, and every point
     ran the small network.

   *STRUCTURALLY REQUIRED* (§0, first point).
2. **What §2.7 compares, and what it leaves out.**
   - **Matched:** a history by (M, β, φ*), through its model's row. A sample by `ScalarHistoryValue`'s ln(1 + z),
     joined through `z_serial` to the redshift table of its own store. `AdiabaticHistory` and `BBNData` by
     their history.
   - **Left out, by name:** `serial` and every `*_serial` column; `version`; `label`; `timestamp`; the six
     timing columns of §0. The script prints this list.
   - **Compared bitwise:** every other column the two schemas both hold. `extra_data` is compared key by key
     over the keys both hold. A key only one side holds is listed, and is not a difference.
   - **The count:** the number of values compared, per table.
   - **New columns** (α, the dissipation columns) are listed, not compared. At α = 0 the script gives their
     values for the four histories.
   - A difference in a compared value is a stop, as the prompt says.

   *STRUCTURALLY REQUIRED* (§0, second point).
3. **The eight tool runs are always made.**
   - After the cluster is stopped and before the grid, one Bash call each:
     `tools/history_and_bbn.py <β> 1e-3 --phi-init-Mp 5`, with `--alpha 0.1` for the α = 0.1 four.
   - The output goes to `var/inelastic-reflection-11/tool/<β>-a<α>.txt`, with `uptime`.
   - `compare_tool.py` is adapted to α. The normalization, φ_ref, residuals and iterations of each must
     equal the stored history's bitwise.
   - The comparison is H3's. A disagreement is prompt §5's stop.

   *IMPLEMENTATION CHOICE*, ruled here, from §0's third point.
4. **The interruption.**
   - **The banner** is `RUNNING PIPELINE FOR MODEL QCD_Cosmology, alpha = 0.1`.
   - **The delay** is read from the reference run.
     - Take the time of that banner in its recorder's output.
     - Take the `timestamp` of each α = 0.1 `ScalarHistory` row the reference stored. Read it from the
       shards, read-only, after the reference has finished. It is UTC; the recorder's time zone is said.
     - Call the first stored and the last stored, from the banner, t₁ and t₄. The delay is (t₁ + t₄)/2,
       rounded to the nearest second.
     - If t₄ − t₁ is under 4 s, the delay is still the midpoint, and the log says the window was short.
   - **A signal that misses** stores no α = 0.1 history, or stores all four. The log records it, the resume
     and the comparison go ahead, and the agent reports it as gate R's interruption not shown. It is not a
     stop, and nothing is rerun.
   - **What the log gives:** the arithmetic, the timestamps, the delay and what the signal found stored (the
     stored-values reader on the resumed store before the resume). This is JN 09 §1.4–§1.5's form.

   *STRUCTURALLY REQUIRED* (§0, fourth point).
5. **The readers' commands.**
   - Each carries `--config exponential.yaml --database $S/inelastic-reflection-11-reference.db --shards 16
     --ray-address $ADDRESS`, as JN 09 §1.14's plot did. `main.py --inventory` takes no `--ray-address`.
   - Each runs in the foreground, one Bash call each, with its log under
     `var/inelastic-reflection-11/rehearsal/`.
   - `shasum -a 256` of the store's 17 files and its manifest is taken before and after each.
   - **For the unstored α** "nothing written" means `$L/plot-unstored` does not exist after the run. The
     exit status, and the message's line naming `--alpha-values` and `0.3`, are quoted.

   *STRUCTURALLY REQUIRED* (§0, fifth and sixth points).
6. **Waits and timeouts.**
   - **A build's registration** is waited on in the foreground, `--timeout 120`, as JN 09 did.
   - **A build's end** (`** launcher saw exit status`) is waited on by a `wait_for_line.py` run as a
     background Bash call, with `--timeout 3000`. Wait for its notification without polling.
   - **The grid's wait** is a background `wait_for_line.py` on `** grid done` with `--timeout 3600`, as the
     prompt says.
   - **A timeout** is reported with `runregistrykit list` and `uptime`. It is a stop for a build, and the
     prompt's §5 stall for the grid.
7. **The load and the record of it.**
   - Record `uptime` at every launch, as JN 09's `09-load.txt` did, and at the start and end of every suite.
   - The machine is shared. A slow run is recorded, not called a defect.
   - The 05:00 readings this note's dispatch was made on are in the brief. Copy them into the log.
8. **The records.**
   - The committed logs have ANSI codes removed (`sed -E 's/\x1b\[[0-9;]*m//g'`).
   - The copied helpers are committed as `.documents/inelastic-reflection/out/11-*.py.txt`, with a `.diff`
     against JN 09's copy where adapted.
   - No PDF and no PNG is committed. The four figures the log looks at are named by path under
     `var/inelastic-reflection-11/`.
   - The verification document's §5 holds the commands as run. It never names the token's path in a command
     that prints it, and §3's `grep -f ~/.ray/auth_token` runs over every committed file.
9. **No file but the prompt's §6 list changes.** In particular:
   - no `.py` outside `.documents/inelastic-reflection/measure/`;
   - no test, no golden, no `CLAUDE.md`;
   - nothing under `var/` committed;
   - `PARKING_EPSILON` in the source byte-identical (`git diff` on `ComputeTargets/` empty).

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status`
  before committing. Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order** (P45).
  1. The Gate, and the five suites before anything else. Run them serially, ComputeTargets in its two halves.
     The `test_[g-z]` half runs in the background; wait for its notification.
  2. The measurement scripts of §1.1, and their two checks. Before the cluster: `eps_point.py`'s check and
     `run_grid.py --only` on two points. Those two points stay done and are skipped later.
  3. The cluster, R1, R2 (the interruption, then the resume), R3, and the stop.
  4. The eight tool runs (ruling 3), the §2.7 comparison, `compare_stores.py` and the fingerprints.
  5. The SM baseline (ruling 1), then the grid, detached.
  6. `summarise_grid.py`, the tables, the figures looked at.
  7. The suites again, `runregistrykit list`, the lock's `status`, the token grep.
  8. The records: the verification document, `out/`, the log, the board and `.documents/OPEN_ISSUES.md`.
- **Ray.** Only `ray_cluster.sh start` / `status` / `stop`, each alone in its Bash call. No `ray stop`, and no
  command that runs the script names `/tmp/ray-cpbh`. Exit 3 from the script or the lock is a stop: never
  wait, retry or remove the lock. Exit 4: run `stop`, then stop and ask.
- **Scratch.** The rehearsal and the grid write under `var/inelastic-reflection-11/` and
  `var/datastores/`. The agent's probes and suite outputs go in its own subdirectory of the session
  scratchpad. Nothing goes in `/tmp` and nothing else goes under the repository.
- **Stop conditions are real.** They are the prompt's §5, read with rulings 2, 4 and 6. A stop goes to the
  user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

Done at `0a47ec4`, before this note was committed:
1. **The Gate above**, and the reading of §0.
2. **The comparison's columns.** `pragma table_info` of the four tables on shard 0 of the reference, and the
   `label`, `version` and `extra_data` of its seven histories, across the 16 shards.
3. **The tools' options.** `interrupt_when.py --help`, `tools/bbn_from_store.py --help`, and
   `history_and_bbn.py`'s `--small-network` default.

At 05:00 GMT, before the brief is sent:
4. **Quiet.** `uptime`, `runregistrykit list`, `raypoolkit.rehearsal_lock status`, and the top processes by
   CPU. Quiet means: the lock is free, no ChamPBH run is `running`, and the 1- and 5-minute load averages are
   both below 3. If it is not quiet, do not dispatch: check again 30 minutes later, and stop at 08:00 with a
   report to the user.

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout. Give it:
- the prompt, the README, the board, log 10;
- `HEAD`, the Gate as measured, and the 05:00 readings;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the files of the prompt's §6. Its log is `logs/11-measure.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **It starts the isolated cluster and stops it, and starts nothing else that outlives its turn but the
  grid.**
- **Stop and ask** on any of the prompt's §5 conditions, read with rulings 2, 4 and 6.
- **It replies with:**
  - the log's path and the message as committed;
  - `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - the runs: id, state, exit, wall, the load at launch;
  - the interruption: the delay's arithmetic and what was stored at the signal;
  - `compare_stores.py`'s verdict, the fingerprints, and §2.7's verdict with its counts and the columns left
    out;
  - the eight tool runs against the store;
  - the readers: exit statuses, the products' paths, `histories.csv`'s rows, the `shasum`s;
  - the grid: points done and failed, its wall, the five tables' headlines;
  - the gates Measure and R, item by item;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — eight checks

1. **Scope.**
   - `git show --stat <agent>` lists only the prompt's §6 files.
   - Its parent is this note's commit, or a later commit that is not the agent's.
   - `git status --porcelain --untracked-files=all` is empty, `git worktree list` shows the main
     checkout alone, and nothing is pushed.
   - `git diff <note> <agent> -- '*.py' ':!.documents/inelastic-reflection/measure/'` is empty.
2. **Nothing left running.**
   - `ray_cluster.sh status`: no process, the ports free.
   - The lock's `status`: free.
   - `runregistrykit list`: this prompt's runs `done` or `killed`, none `running` or stale.
   - `run_grid.pid`'s pid does not answer `kill -0`.
3. **The scripts** against prompt §1.1 and R29–R30: the 48 points against P40, the skip and `.part` rule, the
   footer, no retry, `eps_point.py`'s order. Replay its check myself: `eps_point.py 1e-6 -- 2 4e-31 --alpha
   0.1` against the plain tool, masked.
4. **The stores.**
   - `compare_stores.py` on the two stores myself, and both fingerprints.
   - My own read of §2.7, independent of the agent's script, on two of the four α = 0 histories: the tuning
     outputs and 100 samples spread over ln(1 + z), bitwise.
5. **The interruption.** The delay's arithmetic against the recorder and the timestamps, and what the
   reader found stored at the signal.
6. **The grid.** Every point has a `.txt` and a row in `grid.csv`. Re-derive three numbers of each table
   from the `.txt` files: the drain's relative difference across M at one (β, α), ΔN_eff at the four α at
   one (M, β), and one ε difference.
7. **The figures.** Look at `Delta_N_eff.pdf` and one `dark_radiation.pdf` from R3.
8. **The records.**
   - **The log:** §5.1's form, the gates item by item, every deviation classed, §0's rulings among them, and
     "State handed to the next prompt" as prompt §4.4 lists it.
   - **`.documents/`:** the verification document, dated and in JN's form; `out/` holds text only.
   - **The token grep** over the commit, myself.
   - **The board:** row 11, the gates Measure and R, §5.13, and anything opened on §3 and in
     `.documents/OPEN_ISSUES.md`.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, in the board, the log, the verification document and `.documents/OPEN_ISSUES.md`
  wherever it appears;
- the board's row 11 landed, the gates Measure and R held, and this note added to the list of orchestrator
  notes, marked "used for 11";
- under `### 5.13`, an *Orchestrator review of prompt 11* paragraph holding what each check found.

Residue the review finds is fixed in that commit or a follow-up of its own. Anything that is not residue is
opened on the board's §3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the gates Measure and R;
- the headline numbers: the drain across M, ΔN_eff against α, natural mode at M = 10⁻⁷, the ε shifts, and
  Yp and D/H against the SM;
- the rehearsal: the interruption, the comparison, α = 0 against 2026.7.0, and the readers;
- the stores kept for the user;
- anything opened;
- that 12, which closes the campaign, is written next, from 11's "State handed to the next prompt".
