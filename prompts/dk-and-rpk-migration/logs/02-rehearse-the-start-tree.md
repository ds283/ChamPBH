# Log 02 — rehearse the start tree (R0)

**Subject:** Record the R0 rehearsal of the start tree
**Commit:** `6cbf953`, on branch `dk-and-rpk-migration`, on top of `a72ca5e`
**Date:** 2026-10-12, by Claude Opus 5.5
**Result:** **COMPLETE.**
- L1–L3 hold, L4's baseline is recorded, and the schema snapshot is saved.
- **§4.3: R0 is bitwise reproducible.** `compare_stores` of `02-reference.db` against
  `02-resumed.db` gives `RESULT: equal`, exit 0. So R1 and R2 are held to bitwise equality with
  `02-reference.db` (P4).
- No code changed. No §4 stop condition was met. No §3 issue opened.

The full record, with every table, is
[`.documents/dk-and-rpk-migration-verification.md`](../../../.documents/dk-and-rpk-migration-verification.md)
§1. This log gives what was run and found, the deviations and the observations.

## What shipped

- `.documents/dk-and-rpk-migration-verification.md`, created: the introduction, and §1 R0.
- `.documents/dk-and-rpk-migration/out/`:
  - `02-compare-reference-resumed.txt` and `.json`: §4.3's comparison, 4 KB and 11 KB;
  - `02-reference-schema.json`: the start tree's schema snapshot, 32 KB;
  - `02-stage-counts.txt` and `.json`: `stage_counts.py` on the three build logs;
  - `02-readers.txt`: the readers' output, abridged. It holds `status.txt`; `--inventory` from its
    banner; the SM baseline line; `histories.csv` whole; `bbn_from_store.py` without the EOS
    banner; and the before/after comparison from its bookkeeping heading.
- This log; the board (§1 row, L1–L4 (R0), V1, gate R0, §5.3).

Nothing else. No store, log or figure is committed. The stores, logs and reader outputs are under
`~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/` (`$R`), and nothing there was deleted.

## The run

**Gate, checked at 01:01–01:02 BST.**
- `HEAD` was `a72ca5e`, and `git diff --stat d7791c5 HEAD -- . ':!prompts'` was empty.
- No tracked change; the untracked set is the user's 23 entries, unchanged throughout.
- `./venv/bin/ray status` gave "Could not find any running Ray instance", and
  `pgrep -lf 'gcs_server|raylet|ray::'` found nothing.
- `pip show datastorekit raypoolkit` found neither, and `~/ScienceData` did not exist.

**Load and cluster.**
- 10 CPUs. Load average 13.38 / 21.86 / 20.18 at 01:01, and 12.12 / 15.24 / 18.03 at 01:12:27.
- It peaked at **119.10 / 139.14 / 112.57** when `resumed-a` started (01:24:20), and was
  23.35 / 69.42 / 105.38 when the readers ended. **It was above the CPU count throughout**,
  recorded and not waited out, as the prompt says. The sources were system daemons and, from
  01:29:50, another session's DatastoreKit suite (Observation 4).
- `./venv/bin/ray start --head --disable-usage-stats` at 01:12:29. Address **`127.0.0.1:6379`**
  (D1). `./venv/bin/ray stop` at 01:37:44; no instance was running at 01:37:53.

**The builds** (start and end from `date` around each `build.sh`):

| Build | Started | Ended | Wall | Exit status |
|---|---|---|---|---|
| `build.sh 02 reference reference 127.0.0.1:6379` | 01:12:49 | 01:23:32 | 10 min 43 s | 0 |
| `build.sh 02 resumed resumed-a 127.0.0.1:6379` | 01:24:20 | 01:32:08 | 7 min 48 s | 134 (`SIGTERM` at 01:32:05; Observation 1) |
| `build.sh 02 resumed resumed-b 127.0.0.1:6379 --prune-unvalidated` | 01:33:06 | 01:34:41 | 1 min 35 s | 0 |
| `run_readers.sh 02 resumed 127.0.0.1:6379` | 01:35:07 | 01:36:53 | 1 min 46 s | 0 |

Every command, verbatim, is in the verification document §1.8.

**`S` = 5 s, and why.**
- After `reference` ended, its store was read with `sqlite3 "file:…?mode=ro"`.
- The adiabatic banner fell at about 01:22:07.3 (from the log's one progress line: 01:22:00 at
  5m 17.4s running, and the stage's 5m 24.7s).
- The five `AdiabaticHistory` rows were stored at banner + 4.6, 4.9, 5.6, 6.4 and 7.6 s; the stage
  took 12.6 s, not the pilot's 2 min 19 s.
- `interrupt_when.py` polls once a second, so `S` = 5 s puts the signal at banner + 5–6 s, inside
  the spread.

**`interrupt_when.py`** (second background task), in UTC:
- `00:24:23` waiting; driver pid 97614;
- `00:32:00` saw the banner; waiting 5 s;
- `00:32:05` sent `SIGTERM` to pid 97614; exit 0.

**What `resumed-a` kept** (`mode=ro`, before `resumed-b`):
- `AdiabaticHistory` β = 1.15, 1.1 and 1.2, validated;
- β = 10 stored at 01:32:03.95 and **unvalidated**, with 9776 of 9776 values;
- β = 25 not stored.

The kill was partial, and also fell between a store and its validation.

**The actor check** (01:32:33, 28 s after the signal):
- `./venv/bin/ray list actors --filter "state=ALIVE"` gave `No resource in the cluster`.
- `ray list actors --limit 200` gave 34 actors, all `DEAD`: 17 per job (16 `Datastore`
  `shardNNNN-store`, 1 `SerialPoolBroker`), job `02000000` being `resumed-a`.
- Before `ray stop`: still none alive. 102 dead actors (6 jobs × 17), all six jobs `SUCCEEDED`.

## Verification performed

**Suites at dispatch** (foreground, serially, before `ray start`; output in the scratch directory,
verdicts grepped). 02 changes no code, so these are also the counts after.

| Root | `d7791c5` (log 01) | At dispatch (`a72ca5e`), 01:02–01:12 |
|---|---|---|
| `CosmologyModels/tests` | 18 | **18 OK** (228.0 s) |
| `ComputeTargets/tests` | 114 (61 + 53) | **114 OK**: `test_[a-f]*.py` 61 (175.7 s) + `test_[g-z]*.py` 53 (151.1 s) |
| `Datastore/tests` | 66 | **66 OK** (12.2 s) |

**L1.**
- `reference` and `resumed-b` end `** driver exit status 0`, with all three stages `complete`.
- `resumed-a` ended by the signal in the adiabatic stage: `INCOMPLETE; no queue summary`, status
  134 (Observation 1).
- `grep -c Traceback` is 0 for all eight logs (three builds, five readers).
- The provenance line `>> packages: datastorekit not installed; raypoolkit not installed` heads
  every build log and each Ray reader's log.

**L2.**
- The prune at open: none for `reference` or `resumed-a`, which created their stores. One for
  `resumed-b`: `shard0002-store`, the unvalidated β = 10 `AdiabaticHistory` (store_id 4, 9776 of
  9776 values), "pruned from the datastore". None for the readers.
- **Stage counts** (`compute`; `02-stage-counts.txt`):

  | Stage | `reference` | `resumed-a` | `resumed-b` |
  |---|---|---|---|
  | scalar | 5 | 5 | 0, no compute items evaluated (lookup 5) |
  | adiabatic | 5 | killed | **2** (lookup 2) |
  | BBN | 5 | — | 5 |

- **Label counts** (`mode=ro`, all shards):
  - `02-reference.db`: 5 / 5 / 5 `…-02-reference-…` in `ScalarModel` / `AdiabaticHistory` /
    `BBNData`.
  - `02-resumed.db`: `ScalarModel` 5 `resumed-a`; `AdiabaticHistory` **3 `resumed-a` + 2
    `resumed-b`**; `BBNData` 5 `resumed-b`. All validated.

  `resumed-b` computed exactly what `resumed-a` had not kept, and pruned only the unvalidated row.
- The adiabatic lookup figure is 2, not the brief's expected 5 (Observation 2). It is how that
  stage's task builder counts, and nothing stored was recomputed.

**L3.**
- All six `run_readers.sh` steps exit 0.
- `--inventory`: 5 validated `ScalarModel`, `AdiabaticHistory` and `BBNData` each, no unvalidated
  rows, version `2026.6.0`.
- `histories.csv`: five rows, β = 1.1, 1.15, 1.2, 10, 25, no failure reasons, 0 reflections.
  SHA-256 `d7c62c37d8ad637a63854583ab11bc737109dcbfba92168dbddbaea75182c522`. The rows are in the
  verification document §1.5 and whole in `02-readers.txt`.
- **`bbn_from_store.py` β = 1.15:**
  - stored `Yp=0.24687349447579363 DoH=2.4859924812264325`;
  - re-solve `prod Yp=0.2468734945 DoH=2.485992481`.

  **Equal to all 10 printed figures** (Observation 5).
- Before/after comparison: `RESULT: equal`, bookkeeping differences `none`, problems `none`. The
  readers wrote nothing.

**§4.3.** `compare_stores.py $R/02-reference.db $R/02-resumed.db --json …` gives
**`RESULT: equal`, exit 0**:
- 27 tables, every row matched; 0 A-only, 0 B-only, no near miss, no problem;
- notes: only `LambdaCDM` on both primaries.

The bookkeeping columns differ on every matched row (labels, timestamps, timings), which shows
that the stores were built independently. **R0 is bitwise reproducible.**

**Snapshot.**
- `--snapshot $R/02-reference.db` gives 27 client tables;
- `--schema … --against` the file gives `RESULT: the schemas agree`.

**Hygiene.**
- No tracked file changed but the nine files `6cbf953` adds or edits.
- Only `02-reference.db`, `02-resumed.db` and `02-readers/before/` were opened (the last only by
  `run_readers.sh`'s step 5).
- The pilot store's mtimes are unchanged (2026-10-02). No file under `~/ChamPBH-stores/` or
  `~/ChamPBH-stores-bt02/` has been modified since 2026-10-04.
- No Ray process is left.
- No scratch file is in the repository.

## Deviations from the prompt

- **D1 — `IMPLEMENTATION CHOICE`.** Ray 2.53.0's `ray start --head` printed no address line, only
  `Local node IP: 127.0.0.1` and a bare `ray.init()`. The address `127.0.0.1:6379` was read from
  Ray's own `/tmp/ray/ray_current_cluster`, confirmed by `ray status`, and logged by every driver
  as the cluster it connected to.
- **D2 — `IMPLEMENTATION CHOICE`.** `S` was chosen from `reference`'s store timestamps
  (5 s), not from the pilot's 2 min 19 s, which this tree and machine did not reproduce: the
  stage took 12.6 s. The prompt asks for "about half" the stage, read from `reference`; the brief
  says where the evidence is.
- **D3 — `IMPLEMENTATION CHOICE`.** The schema snapshot (§1.4 step 3) was taken while `resumed-b`
  ran, and `stage_counts.py` (step 4) while the readers ran. Each reads only a closed store or
  finished logs, so the order changes nothing.
- **D4 — `IMPLEMENTATION CHOICE`.** Two outputs beyond the prompt's list:
  - `02-stage-counts.json`, `stage_counts.py`'s own `--json`;
  - `02-readers.txt`, the abridged reader output the prompt's §1.1 allows.

No `STRUCTURALLY REQUIRED` and no `UNINTENDED DRIFT` deviation.

## The deliberate-breakage record

None. 02 changes no code (prompt §3.5; README §5 rule 8). Its checks are each one a bad build would
fail:
- a non-zero exit or missing `ALL WORK ITEMS COMPLETE` (L1);
- a resume that recomputes a kept row, or prunes a validated one (L2: the label counts and the
  prune report);
- a reader that fails or writes (L3: `status.txt`, the before/after comparison);
- a single changed bit in any stored science value (§4.3).

## Observations not acted on

1. **A driver killed by `SIGTERM` exits 134, not 143.**
   - Ray's driver installs absl's failure-signal handler. On `SIGTERM` it logs
     `*** SIGTERM received at time=… ***` and a native stack, twice (once on stderr and once
     through `logging.cc`, prefixed `[… E <pid> …]`), and the process then aborts (`Abort trap: 6`).
   - The stack is the main thread waiting in `ray::core::CoreWorker::Wait()`. The handler's dump is
     not a Python traceback and contains neither `Traceback` nor `Error`.
   - `ray list jobs` reports the killed driver as `SUCCEEDED`.
   - 06 and 10 run the same Ray 2.53.0 and should expect the same. Not opened: it is Ray's
     behaviour and not a defect of ChamPBH or the scripts. `build.sh` records the status
     faithfully.
2. **The two stages count lookups differently on a resume.**
   - `build_solver_batch`'s "which models are missing?" comprehension (`main.py:219-226`) keeps
     every model, whatever its vectorized lookup returned. So each stored `ScalarModel` is looked
     up again by an individual `object_get(…, _do_not_populate=True)`, and the pool reports
     lookup 5, compute 0.
   - `build_adiabatic_batch` filters with `select_missing` (`main.py:432-437`), so the pool sees
     only the missing items: lookup 2, compute 2.
   - The outcome is correct either way. The scalar stage's extra lookups cost under a second
     here.
   - Not opened as an issue: nothing is wrong in the result, and whether the comprehension is
     meant to filter is a question for the code's owner. The orchestrator may open one. The
     baseline (L4) is the measured pattern.
3. **The plot scripts warn of a shard-count mismatch on every open.** `plot_by_beta.py:1011` and
   `plot_ScalarModel.py:1581` construct `ShardedPool` without `shards=args.shards`. The pool's
   default of 10 is then compared with the 16 recorded shards, giving
   `!! WARNING: number of shards read from database (=16) does not match specified number of shards (=10)`.
   The recorded shards are used, so it is cosmetic. It is already in the user's `pilot-plot.log`
   of 2026-10-02. 08 rewrites these constructions; a housekeeping candidate. Not opened.
4. **The machine was not idle.** System daemons kept the load above 10 before any build. From
   01:29:50 another session ran DatastoreKit's suite (pid 504, `python -m unittest discover -s
   datastorekit/tests -t .`, with its working directory in a DatastoreKit session's scratchpad).
   The orchestrator's gate asked that no other session run a suite or a Ray job during 02's
   builds. It overlapped `resumed-a`'s last minutes, `resumed-b` and the readers. It was not
   interfered with. **The bitwise result held under it**, and no BBN solve failed.
5. **`bbn_from_store.py` prints its re-solve to 10 significant figures**, so L3's Yp and D/H check
   shows equality to 10 figures, not bitwise. The stored value is printed in full and equals
   `histories.csv` exactly. The tool is not this prompt's to change.
6. **`ray stop` kills a shell whose command line names Ray's processes.** The final
   `ray list … ; ray stop ; … ; pgrep -lf 'gcs_server|raylet|ray::'` ran as one shell command, and
   `ray stop` matched that shell's own command line and killed it (status 144) after stopping the
   cluster. A separate check confirmed the cluster was down. A tip for 06 and 10: run `ray stop`
   on its own.

## State handed to the next prompt

- **R0 holds and is bitwise reproducible.** R1 (06) and R2 (10) are held to `compare_stores`
  `RESULT: equal` (exit 0) against
  `~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/02-reference.db`. Bookkeeping
  differences are expected; a science difference of one bit is a miss. No tolerance was chosen.
- **The stores**, kept and not to be deleted (README §3):
  - `$R/02-reference.db` and `$R/02-resumed.db`, 16 shards each;
  - the logs `$R/logs/02-{reference,resumed-a,resumed-b}.log`;
  - the readers' directory `$R/02-readers/`.
- **L4's baseline:**
  - stage `compute` counts 5 / 5 / 5 (`reference`), 5 / killed (`resumed-a`), 0 / 2 / 5
    (`resumed-b`), with `resumed-b`'s lookups 5 / 2 / 5;
  - the prune at open removed exactly the rows `resumed-a` stored but had not validated.

  What a resume keeps depends on the signal's timing, so 06 and 10 compare the pattern (computes
  exactly what was not kept; prunes only unvalidated rows), checked by label counts, and not the
  numbers.
- **`S`.** On this tree and machine the adiabatic stage takes about 12 s, its rows landing
  4.6–7.6 s after the banner. `S` = 5 s gave *k* = 3 kept plus 1 unvalidated. 06 and 10 should
  derive their `S` from their own `reference` store's timestamps.
- **`resumed-a` exits 134** under `SIGTERM` at Ray 2.53.0 (Observation 1).
- **The schema snapshot** for 05 and 08: `.documents/dk-and-rpk-migration/out/02-reference-schema.json`
  (27 client tables).
- **`histories.csv`** for 06 and 10's byte comparison: SHA-256
  `d7c62c37d8ad637a63854583ab11bc737109dcbfba92168dbddbaea75182c522`, whole in
  `out/02-readers.txt`.
- **Suites at `6cbf953`:** CosmologyModels 18, ComputeTargets 114 (61 + 53), Datastore 66.
- **No Ray cluster is running.** No §3 issue opened. The board's one open issue is unchanged.
