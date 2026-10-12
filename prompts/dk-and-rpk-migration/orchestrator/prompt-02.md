# Orchestrator — prompt 02, rehearse the start tree (R0)

Read [`../README.md`](../README.md) first: §0.2 ("The stores"), §2.2 (02's charter), §3, §4.1,
§4.2, §4.3, §5 and §6.1 (U2). The conventions are those of
[`prompt-01.md`](prompt-01.md) §0 ("Conventions"), which are not repeated here except where 02
changes them. Prompt 01's review (board §5.2) hands this note three points, taken up below.

**You do not write code.** You may:
- run the suites, `compare_stores.py`, `stage_counts.py` and `--snapshot` on the agent's stores;
- read the agent's stores with `sqlite3` in `mode=ro`, never through the layer;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`02-rehearse-the-start-tree.md`](../02-rehearse-the-start-tree.md)
**Board items:** L1–L4 (R0), V1; gate R0 · **Closes:** nothing · **Narrows:** nothing ·
**Opens:** only what the work finds · **Model:** Opus, as the prompt recommends.

**Gate:**
- 01 landed (`d7791c5`) and was reviewed (`f5fe207`); its scripts and tool are as log 01 describes.
- `HEAD` is the commit that adds this note, or a later one that touches no code ("code" as
  prompt-01.md's gate defines it). `git diff --stat d7791c5 HEAD -- . ':!prompts'` is empty at
  `f5fe207`.
- One prompt at a time in this checkout, and **no other session runs a suite or a Ray job on this
  machine while 02's builds run** (it changes the load, and with it the interrupt's timing).

## 0. What makes this prompt unusual

**It is the first prompt that runs `main.py` on a store, and it changes no code.** Everything it
produces is a measurement. The review's job is to reproduce each measurement from the artefacts
the agent leaves behind (the logs, the stores, the readers' directory), not to trust the log.

**It sets the criterion for R1 and R2.** If `compare_stores` of `reference` against `resumed`
is empty but for bookkeeping, R1 and R2 are held to bitwise equality. A vacuous "equal" here
(a comparison run on the wrong pair, or on a store the resume never touched) would set the
strictest criterion on false evidence. So the review re-runs the comparison itself.

**The resume is only a test if it is partial.** `resumed-a` must be killed while some
`AdiabaticHistory` rows are stored and some are not. The log gives no help in timing this:
- the stage's 5 histories are 5 independent Ray tasks (`main.py:503-518`, one `object_get` per
  proxy), each stored and validated as it finishes;
- the pool prints progress no more often than every `MIN_NOTIFY_INTERVAL` = 300 s (`main.py:81`,
  passed at `:554`), and the pilot's stage took 2 min 19 s, so **the log shows only the banner and
  the final summary**. Nothing in it says when each history was stored.

  The evidence is in the `reference` store itself: each row's `timestamp` (`datetime.now()`, local
  time) and `label`. The agent may read its own `reference` store, after that build has ended,
  with `sqlite3` in `mode=ro` only. The last `ScalarModel` timestamp is just before the stage
  banner. The spread of the five `AdiabaticHistory` timestamps after it says where "about half"
  falls. Also part of the stage's time is the reload of the five `ScalarModel`s, which happens
  before any compute.

**Every stored row says which build wrote it.** `label` begins with the job name
(`main.py:257`, `:520`, `:783`): `dk-and-rpk-migration-02-resumed-a-…` or `…-resumed-b-…`. So
counting `AdiabaticHistory` rows in `02-resumed.db` by label prefix gives exactly what `resumed-a`
stored and kept, independently of `stage_counts.py`. The agent records this count beside the
stage counts, and the review repeats it. If `resumed-a` stored **0 or 5** `AdiabaticHistory` rows,
the resume did not test a partial stage: that is **not** a stop. The agent records it plainly,
finishes the protocol, and says so in its reply. The orchestrator then takes it to the user (a
repeat would need store names the prompt does not give).

**Handed on from 01's review (board §5.2):**
- **`--prune-unvalidated` is on by default** (`config/argument_parser.py:257-261`,
  `BooleanOptionalAction`, `default=True`). Every build prunes at open, and `resumed-b`'s flag
  repeats the default. L2 reads every build's prune report, not only `resumed-b`'s. The readers
  prune too, so L3's before/after comparison also covers that.
- **A parent that cannot pair lists its children unpaired.** If `compare_stores` reports a
  difference, thousands of value rows may follow it in the JSON. The named problem or the parent
  row is the finding; the children are its consequence.
- **`build.sh` and `run_readers.sh` refuse an existing log or readers directory.** A failed start
  cannot simply be re-run under the same name. That is a stop (prompt §4), not a reason to delete
  a log.

**The facts, checked by the orchestrator on 2026-10-12 at `f5fe207`.** Pass them on.
- **The tree.** Branch `dk-and-rpk-migration`, `HEAD` `f5fe207` before this note. Untracked: the
  user's 17 `pilot-2026.6.0*.db` files, `pilot-main.log`, `pilot-plot.log`, `pilot-out/`,
  `full_run_2026.6.0.sh`, `full_run_2026.6.0-refresh.sh` and `prompts/jordan-normalization/`. None
  is the agent's.
- **Ray.** `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing; `./venv/bin/ray status` says
  "Could not find any running Ray instance". Ray is `2.53.0`, and the state API imports
  (`ray.util.state`, `aiohttp`), so `./venv/bin/ray list actors --filter "state=ALIVE"` is
  available once a cluster is up.
- **The actors are named, not detached.** `SerialPoolBroker` (`ShardedPool.py:96`) and
  `shardNNNN-store` (`:158`, `:179`) use `.options(name=…)` with no `lifetime`, and `ray.init`
  passes no namespace (`main.py:95`). So each driver's actors live in the job's anonymous
  namespace and should die with the job when the driver is killed. A survivor after a minute is
  the prompt's stop.
- **The venv.** `pip show datastorekit raypoolkit` finds neither. Every provenance line should
  read `>> packages: datastorekit not installed; raypoolkit not installed`.
- **The machine.** 10 CPUs, 16 GB. The load average was **56 / 34 / 23 at 00:57** and
  **19.6 / 27 / 21 at 00:59** on 2026-10-12, from other sessions and system daemons (no Ray, no
  suite). That is above the CPU count. The prompt says record it and continue; it does not ask the
  agent to wait. The disk has 67 GB free; the pilot store is 40 MB.
- **`~/ScienceData/` does not exist.** `build.sh` creates `$REHEARSAL_ROOT/logs` with `mkdir -p`;
  the prompt also allows `mkdir -p` of the root first.
- **The BBN wall-clock limit** is `DEFAULT_BBN_WALL_CLOCK_LIMIT = 600.0` s
  (`ComputeTargets/BBNData.py:53`), and `build.sh` does not override it. A small-network solve is
  about 10 s on an idle machine, so the limit should not be met even at this load. If it is, the
  failure is stored, and §4.3's candidate applies.
- **What the drivers print that is not a failure:**
  - Ray's `FutureWarning` about `RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO` (in every pilot log);
  - under load, `(raylet) worker_pool.cc:586: Some workers of the worker process(…) have not
    registered within the timeout` (in `pilot-plot.log`);
  - the three `!! warning:` super-Planckian lines (β = 10 and 25 at φ* = 5) and the
    `@@ Parametrized equation-of-state …` line from each `Datastore` actor, which Ray deduplicates
    as `[repeated 15x across cluster]`.

  None of these is a traceback. A line containing `Traceback` or `Error` that is not one of them
  is read in full and recorded.
- **The prune report** comes from the `Datastore` actors (`Datastore/SQL/Datastore.py:441-467`),
  so it reaches the driver's log through Ray's log forwarding, prefixed `(Datastore pid=…)`. Its
  lines are `!! INTEGRITY WARNING (…): datastore "…"`, then for example
  `>> AdiabaticHistory instances` and `-- "<label>" (store_id=N)` with
  `contains R z-sample values | expected=Z` (`ObjectFactories/AdiabaticHistory.py:324-357`).
  **No such line means nothing was unvalidated**, which is the likely result: a store and its
  validation are a fraction of a second apart.
- **Expected stage counts** (the queue summary's `compute` figure):

  | Stage | `reference` | `resumed-a` | `resumed-b` |
  |---|---|---|---|
  | `CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID` | 5 | 5 | 0, with "no compute items were evaluated" |
  | `CALCULATE ADIABATIC TRANSGRESSION PARAMETERS` | 5 | killed, no summary | 5 − *k*, *k* = rows `resumed-a` kept |
  | `CALCULATE BBN DATA` | 5 | not reached | 5 |

  `resumed-b`'s `lookup` count is 5 in each stage. A different pattern is a finding to explain
  (L4), not a failure by itself, unless it means `resumed-b` recomputed something stored.
- **The readers.**
  - `plot_by_beta.py` also runs the SM baseline, one more PRyMordial solve, unless
    `--no-baseline` is given, and `run_readers.sh` does not give it.
  - Its `histories.csv` goes to `02-readers/plot_by_beta/QCD_Cosmology/histories.csv`, as the
    pilot's went to `pilot-out/QCD_Cosmology/histories.csv`.
  - `tools/bbn_from_store.py … --small-network --variant prod` runs production's own callback and
    `_run_PRyMordial` on the stored grid. Its header line `store beta=… stored: …` gives the
    stored abundances, and its `prod Yp=… DoH=…` line gives the re-solve. **Equality is
    expected.** A difference is recorded as a finding about the tool or the network flag (prompt
    §2, L3) and is not a stop.
- **The suites at `d7791c5`** (01's review, load about 19): CosmologyModels 18, ComputeTargets 114
  (61 + 53 in two pattern groups, `test_[a-f]*.py` and `test_[g-z]*.py`), Datastore 66. The agent
  runs them **before `ray start` or after `ray stop`, never during a build**: a suite's load
  changes the interrupt's timing and could push a BBN solve towards its limit.
- **Expected duration.** The pilot took about 9 minutes a build on 2026-10-02, with BBN on the full
  network (1 min 40 s); the small network is faster. Under today's load, allow 10–20 minutes a
  build, and about an hour in all.

**What the review exists to establish.**
- **(E1) The builds.** Each log has the provenance line, the banners, the summaries and
  `** driver exit status`, as prompt §2 L1 says. `resumed-a` died by the signal in the
  `AdiabaticHistory` stage, and its actors were gone before `resumed-b`.
- **(E2) The resume.** By the stage counts and by the labels in `02-resumed.db`: `resumed-b`
  computed exactly what `resumed-a` had not kept; nothing was pruned but unvalidated rows.
- **(E3) §4.3.** `compare_stores 02-reference.db 02-resumed.db`, re-run by the orchestrator, gives
  the same result and exit code as the agent's. If it is 0, R0 is bitwise reproducible.
- **(E4) The readers.** All five steps exit 0. The before/after comparison is empty but for
  bookkeeping. The Yp and D/H re-solve agrees with the stored row, or the difference is recorded.
- **(E5) The baseline and the snapshot.** The schema snapshot and the stage counts are committed,
  and re-running either on the stores gives the same file.
- **(E6) Hygiene.** No code changed; no store but the agent's own was opened; no Ray left running.
- **(E7) The records.** The verification document's introduction and §1, the board, the log.

**Conventions particular to 02.** prompt-01.md's conventions all hold. In addition:
- **Background tasks.** Each `build.sh` runs as a background Bash task, so that the agent is
  notified when it ends. `interrupt_when.py` runs as a second background task beside
  `resumed-a`'s. If the harness does not notify the agent, it waits with the `Monitor` tool on the
  log's `** driver exit status` line, **never with a loop of `sleep` and reads**.
- **Ray.** The agent starts one cluster (`./venv/bin/ray start --head --disable-usage-stats`), uses
  its address for every driver, and runs `./venv/bin/ray stop` at the end **and after any stop**.
  Before it stops, it records `./venv/bin/ray list actors --filter "state=ALIVE"`.
- **Stores.** The agent opens only `02-reference.db`, `02-resumed.db` and the readers' `before/`
  copy, through the drivers, `compare_stores.py`, `stage_counts.py` and `sqlite3` `mode=ro`. Not
  the pilot store, not `~/ChamPBH-stores/`, not `~/ChamPBH-stores-bt02/`. It deletes nothing under
  `$REHEARSAL_ROOT` (README §3), even after a stop.
- **Commands verbatim.** The log and the verification document quote each command as run, with
  the actual address and `S`.
- **Outputs committed** are small text only, under `.documents/dk-and-rpk-migration/out/02-*`. A
  JSON file over about 1 MB is a finding to report before committing, not something to commit.

## 1. Before you dispatch

1. **The tree.** Record the branch and `HEAD`. `git status --short` shows only the untracked
   entries of §0, and `git diff --cached` is empty. `git log --oneline f5fe207..HEAD` touches no
   code.
2. **Ray.** `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing.
3. **The venv.** `./venv/bin/pip show datastorekit raypoolkit` finds neither.
4. **The load.** `uptime`, recorded and passed on.
5. **The stores' mtimes**, for check 9: `/usr/bin/stat -f '%m %z %N'` over
   `~/ChamPBH-stores/*.db`, `~/ChamPBH-stores-bt02/*.db`, `pilot-2026.6.0*.db`, `pilot-main.log`
   and `pilot-plot.log`, saved in the scratchpad (53 files on 2026-10-12).
6. `~/ScienceData/` does not exist.

The suites are not re-run by the orchestrator: no code has changed since `d7791c5`, and the agent
runs them (§0).

## 2. Dispatch

Dispatch one fresh-context subagent, on **Opus**. Give it:
- the prompt;
- the campaign README and this board;
- prompt 01's log;
- `HEAD`, the load and §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the
  session scratchpad. The agent does not read `orchestrator/`, and does not read prompts 03–04.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/02-rehearse-the-start-tree.md`, in README §5.1's
  form, with the prompt's §6 additions.
- **The files it may add or change** are the prompt's §5 list (the verification document,
  `.documents/dk-and-rpk-migration/out/02-*`, the log, the board) and `.documents/OPEN_ISSUES.md`
  if an issue opens. **No code, no script of 01's, no tool.**
- **It runs the protocol as written,** waits on each build by notification, and keeps the suites
  away from the builds.
- **Stop and ask** on any of the prompt's §4 conditions, after `ray stop`, with the cluster's state
  and the logs' paths. A §4.3 difference is a stop *after* the readers and the records are done:
  the agent finishes §1.4, writes the log and the verification document, and **does not commit**.
  It reports, and waits for the orchestrator.
- **It replies with:**
  - the log's path and the commit;
  - the load, the address and each build's start, end, wall time and exit status;
  - `S`, and the evidence it was chosen from;
  - the actor check;
  - the stage counts and the label counts;
  - each prune report;
  - the readers' statuses, the Yp/D/H comparison and `histories.csv`;
  - `compare_stores`' `RESULT:` line and exit code, and its full text if not 0;
  - the three suites;
  - every deviation with its tag;
  - the "State handed to the next prompt" section, verbatim.

## 3. The review — ten checks

1. **Scope.** At the agent's commit, `git diff HEAD~1 HEAD --stat` touches only the verification
   document, `.documents/dk-and-rpk-migration/out/02-*`, `logs/02-…`, the board and, if an issue
   opened, the index. `git status` shows the §1 untracked set and nothing new.
2. **Ray.** `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing. The log shows `ray start`, the
   address, the actor list before `resumed-b`, and `ray stop`.
3. **E1, the logs.** Read all three in `$REHEARSAL_ROOT/logs/`.
   - The provenance line: once each, both packages not installed.
   - The banners and summaries. `grep -n 'Traceback\|Error'` gives only §0's known lines.
   - `reference` and `resumed-b` end `** driver exit status 0`. `resumed-a` ends with 143
     (128 + `SIGTERM`, as bash's `wait` reports it), after the `ADIABATIC` banner and before any `BBN` banner.
   - `interrupt_when.py`'s output matches: the banner seen, `S` waited, `SIGTERM` to the pid in
     `02-resumed-a.pid`.
4. **E2, the counts.** Re-run `stage_counts.py` on the three logs and diff with the committed
   output. Then, `mode=ro` on `02-resumed.db`'s shards, count `ScalarModel`, `AdiabaticHistory`
   and `BBNData` rows by label prefix. Expected: `ScalarModel` 5 `resumed-a`; `AdiabaticHistory`
   *k* `resumed-a` and 5 − *k* `resumed-b`; `BBNData` 5 `resumed-b`. *k* must agree with
   `resumed-b`'s `AdiabaticHistory` compute count. Also count the same tables in
   `02-reference.db`: 5 each, all `reference`.
5. **E2, the prune.** Every prune report in the three logs, read in full. Each names only rows
   whose `validated` was false or null. If none printed, check that the agent says so for each
   build.
6. **E3, §4.3.** Re-run
   `tools/compare_stores.py $REHEARSAL_ROOT/02-reference.db $REHEARSAL_ROOT/02-resumed.db --json
   <scratch>` and compare its `RESULT:` line, exit code and JSON with the committed
   `out/02-compare-reference-resumed.*`. Check that the table list covers every client table, with
   row counts that make sense (5 `ScalarModel`, and so on). Read the bookkeeping section: the
   differences should be only `timestamp`, `label` and the timings.
7. **E4, the readers.**
   - `02-readers/status.txt` has steps 0–5, each exit 0.
   - `5-compare_stores.log` is empty but for bookkeeping, with exit 0.
   - `histories.csv` has the five histories.
   - `4-bbn_from_store.log`'s stored and `prod` Yp and D/H, as the log reports them.
   - `1-inventory.log` holds what the log says it printed.
8. **E5, the snapshot.** Re-run `compare_stores.py --snapshot $REHEARSAL_ROOT/02-reference.db --out
   <scratch>` and diff with `out/02-reference-schema.json`: identical.
9. **E6, the stores.** The §1 mtimes are unchanged. `$REHEARSAL_ROOT` holds exactly the two stores
   (17 files each), `logs/` (three logs and three pidfiles) and `02-readers/`.
10. **E7, the records and the suites.**
    - The verification document: an introduction pointing at README §4, and §1 R0 with the
      cluster, the load, the three builds, L1–L4, §4.3's result and the commands verbatim.
    - The board: the §1 row (✅, "this commit", the log), L1–L4 and V1, gate R0, and
      `### 5.3 After prompt 02 (<date>)`.
    - Any §3 entry, with the index's row, count and date in the same commit.
    - The suites as the agent ran them: 18, 114, 66.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 02* paragraph in §5.3: what each
  check found, the re-run comparison, and anything handed on. Fix small residue in a follow-up
  commit of its own:
  - "this commit" → the SHA;
  - this note added to the board's list of orchestrator notes, marked "used for 02".
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - §4.3's answer, in one sentence, and what it means for R1 and R2;
  - the builds' times under the measured load;
  - the resume's *k*, and what `resumed-b` recomputed;
  - the readers, the Yp/D/H re-solve against the stored row;
  - anything opened.
- **Then prompts 05–06 are written** (README §2.1): bitwise equality if §4.3 is empty, or the
  user's ruling if it is not. 03 and 04 are already written and can be dispatched once this review
  is recorded.
