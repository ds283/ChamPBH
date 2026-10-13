# Log 10 — rehearse through both packages (R2)

**Subject:** Record the R2 rehearsal through both packages
**Commit:** `b0d258a`, on branch `dk-and-rpk-migration`, on top of `60e000c`
**Date:** 2026-10-13, by Claude Opus 5.5
**Result:** **COMPLETE.**
- **The criterion holds: R2 equals R1 and R0 bit for bit.** `compare_stores` of `06-reference.db`
  against `10-reference.db`, and against `10-resumed.db`, and of `02-reference.db` against
  `10-reference.db`, each give `RESULT: equal`, exit 0; so does `10-reference.db` against
  `10-resumed.db`. The schema snapshot agrees with 08's and R0's.
- L1–L3 hold. L4: the counts, the resume's pattern and the pool's printed lines are R1's.
- **H1 holds**: the package's shard-key audit is clean on both stores; a `sharded_store copy` opens
  read-only, compares equal and was deleted by `delete_store`, its 17 files and nothing else.
- **Gate R2 and DatastoreKit's G3 hold.** The G3 record's text is in "State handed to the next
  prompt".
- **Two things for the review** (neither changes a stored value):
  1. **`resumed-a` exited 15, not 134.** Ray's own driver `SIGTERM` handler (`sys.exit(signum)`)
     ran, so the `with ShardedPool` block closed the pool before the driver exited. The agent read
     this as "ends by the signal, in the adiabatic stage" and went on (D3); a reviewer who reads
     §2's "(134)" as binding may rule otherwise.
  2. **`resumed-a` stored one `AdiabaticHistory` row and validated none**: the "0" case by
     validated rows, partial (1 of 5) by stored rows (Observation 2). Recorded, not a stop.
- No code changed. No other §4 stop condition was met. No §3 issue opened.

The full record is
[`.documents/dk-and-rpk-migration-verification.md`](../../../.documents/dk-and-rpk-migration-verification.md)
§3. This log gives what was run and found, the deviations and the observations.

## What shipped

- `.documents/dk-and-rpk-migration-verification.md`: **§3 R2** added after §2 (§§3.1–3.10, with H1
  in §3.8 and G3 in §3.9). §1 and §2 are not edited (the diff only adds lines).
- `.documents/dk-and-rpk-migration/out/` (20 files, 180 KB in all; the largest is the schema
  snapshot, 32 KB):
  - `10-compare-06-reference-10-reference.txt` / `.json`, `10-compare-06-reference-10-resumed.txt` /
    `.json`, `10-compare-02-reference-10-reference.txt` / `.json`,
    `10-compare-10-reference-10-resumed.txt` / `.json`: the four comparisons;
  - `10-reference-schema.json`: the snapshot of `10-reference.db`;
  - `10-stage-counts.txt` / `.json`: `stage_counts.py` on the three build logs;
  - `10-printed-lines.txt`: L4's printed-lines comparison;
  - `10-readers.txt`: the readers' output, abridged (both inventories' headers; step 1's whole);
  - H1: `10-h1-audit-10-reference.txt`, `10-h1-audit-10-resumed.txt`, `10-h1-copy.txt`,
    `10-h1-copy-inventory.txt`, `10-h1-compare-10-reference-10-reference-copy.txt` / `.json`,
    `10-h1-delete.txt`.
- This log; the board (status line, §1 row, L1–L4 (R2), V3, H1, gates R2 and DatastoreKit G3,
  §5.15).

Nothing else. No store, log or figure is committed. The stores, logs and reader outputs are under
`~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/` (`$R`). Nothing there was deleted, moved
or renamed but the copy `10-reference-copy*` (17 files), by `delete_store`.

## The run

**Gate, checked at 01:00 BST.**
- `HEAD` `60e000c`; no tracked change; nothing staged; the untracked set the user's 23 entries.
- `pip freeze`: 101 lines, `ray==2.53.0`,
  `datastorekit @ git+https://github.com/ds283/DatastoreKit@2f5417ea76cdcd0d15d64d80d9085429bfacad21`,
  `raypoolkit @ git+https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976`;
  `pip check` clean. `utilities.installed_package_provenance()` gave both with their commit,
  `requested_revision` `v0.3.0` / `v0.2.0` and **`editable: False`**; `format_package_provenance`
  gave log 07's line exactly.
- `ray_cluster.sh status`, exit 0: the lock free, no ChamPBH process, 6380 / 6381 / 8267 / 52367
  free. No Ray process on the machine; `/tmp/ray/ray_current_cluster` absent;
  `/tmp/ray-cpbh/ray_current_cluster` stale from R1 (`127.0.0.1:6380`). `$R` held 376 entries, no
  `10-*`; `~/ScienceData` held only `ChamPBH-rehearsals`. The mtime and size of every entry under
  `$R` was recorded for the hygiene check.
- 10 CPUs. Load 8.27 / 8.90 / 7.82 at 01:00. Top processes: macOS daemons (`searchpartyd`,
  `spotlightknowledged`, `analyticsd`, `mediaanalysisd`) and two Claude Code sessions; no foreign
  suite or Ray job.

**The suites** ran next, before `start`, in the foreground, serially, output in the scratch
directory (§"Verification performed").

**The cluster.**
- `ray_cluster.sh start "dk-and-rpk-migration 10 (R2)"` at 01:05:30, exit 0, 6 s wall: address
  `127.0.0.1:6380`, dashboard `http://127.0.0.1:8267`, actor line
  `./venv/bin/ray list actors --address 127.0.0.1:6380`, "started in 5s; gcs_server pid 3759".
  Load 8.17 / 8.67 / 8.11 just before.
- Owner file: `project: ChamPBH` / `purpose: dk-and-rpk-migration 10 (R2)` /
  `acquired: 2026-10-13T01:05:36+01:00` / `host: mps026088` / `ray_temp_dir: /tmp/ray-cpbh` /
  `ray_address: 127.0.0.1:6380` / `pid: 3759`.
- `./venv/bin/ray status --address 127.0.0.1:6380`: one node, `0.0/10.0 CPU`, 8.61 GiB memory,
  2.00 GiB object store, no failures.
- `ray_cluster.sh stop` at 01:17:58, a command on its own, with nothing else running (both
  `Monitor` tasks had ended or been stopped; no background task named `/tmp/ray-cpbh`): exit 0,
  18 s; `SIGTERM` to 18 processes, `SIGKILL` to 3759 and 3771 after the 15 s grace; "no process
  names /tmp/ray-cpbh; ports 6380 6381 8267 52367 are free"; "released the rehearsal lock".
  `ray_cluster.sh status` then, exit 0: lock free, no ChamPBH process, ports free. **`ray stop` was
  never run.**

**The builds** (start and end from `date` around each script; R1's beside):

| Build | Started | Ended | Wall | Exit status | R1 |
|---|---|---|---|---|---|
| `build.sh 10 reference reference 127.0.0.1:6380` | 01:05:43 | 01:09:16 | 3 min 33 s | 0 | 11 min 7 s, 0 |
| `build.sh 10 resumed resumed-a 127.0.0.1:6380` | 01:10:49 | 01:13:46 | 2 min 57 s | **15** (`SIGTERM` at 01:13:44) | 8 min 14 s, 134 |
| `build.sh 10 resumed resumed-b 127.0.0.1:6380 --prune-unvalidated` | 01:14:47 | 01:15:48 | 1 min 1 s | 0 | 2 min 42 s, 0 |
| `run_readers.sh 10 resumed 127.0.0.1:6380` | 01:16:07 | 01:17:13 | 1 min 6 s | 0 | 1 min 56 s, 0 |

Stage times: `reference` 1m 54.6s · 4.35s · 21.2s (R1 5m 6.61s · 16.2s · 1m 5.62s); `resumed-a`
1m 54.8s · killed; `resumed-b` 0.513s · 5.16s · 21s (R1 1.41s · 12s · 56.5s). Driver pids 4009,
7671, 10377. Load at each start and end is in verification §3.1 (8–10 before, up to 68 during the
builds, from their own workers).

**The connect line.** Every driver that starts Ray (three builds, two plot scripts) logged
`Connecting to existing Ray cluster at address: 127.0.0.1:6380...` and `View the dashboard at
http://127.0.0.1:8267`. No log names 6379. The two `--inventory` runs start no Ray.

**`S` = 1 s, and why.**
- After `reference` ended, `10-reference.db` was read `mode=ro` (a scratch script, Python's
  `sqlite3` with `file:…?mode=ro`, over the 16 shards).
- **`10-reference.log` has no progress line** (the scalar stage took 1m 54.6s, under the pool's
  300 s notify interval), so 06's way of dating the banner was not available (D1). The banner was
  dated from Ray's task records,
  `./venv/bin/ray list tasks --address 127.0.0.1:6380 --limit 2000 --detail --format json`: the
  scalar stage's last `Datastore.object_validate` ended at 01:08:48.979 and the adiabatic stage's
  first `Datastore.object_get` was created at 01:08:49.005, so the banner fell at about
  **01:08:49.0**; the stage's 4.35 s then ends where its last validation ends (53.345).
- The five `AdiabaticHistory` rows: 01:08:49.92 (β = 1.1), 50.26 (1.15), 50.36 (1.2), 50.68 (10),
  52.17 (25) — banner + **0.9, 1.3, 1.4, 1.7, 3.2 s**. Their `object_store` calls returned at
  banner + 1.5, 2.1, 2.3, 2.9, 4.2 s; their validations at + 2.4, 2.7, 2.8, 4.3, 4.3 s.
- The prompt's rule (the earliest row less about 3 s) gives a negative delay here. `S` = 1 s puts
  the signal at banner + 1–2 s (one-second polling): after `reference`'s first commit and before its
  third, before any validation, and 2 s or more before β = 25's commit, leaving room for
  `resumed-a` to run somewhat ahead of `reference`, as at R0 and R1 (D2).

**`interrupt_when.py`** (second background task), in UTC: `00:10:49` waiting; `00:10:50` driver
pid 7671; `00:13:43` saw the banner, waiting 1 s; `00:13:44` sent `SIGTERM` to pid 7671; exit 0.

**How `resumed-a` ended** (from its log and from Ray's task records for job `02000000`, `ray list
tasks --address 127.0.0.1:6380 --limit 5000 --detail --format json --filter "job_id=02000000"`):
- The banner fell at about 01:13:43.36 (the scalar stage's last validation ended at 43.358; the
  first adiabatic `object_get` was created at 43.387). The five computes ran 44.107–44.703. The
  first `object_store` (β = 1.1, actor `9a1ac6ab`, `shard0015-store`) was created at 44.257: at
  banner + 0.90 s, against `reference`'s + 0.86 s, so the stage ran at `reference`'s pace.
- `SIGTERM` at 01:13:44.48 (absl `time=1791850424`; `logging.cc` 01:13:44.485): banner + about
  1.1 s. The driver's main thread was in `submit_actor_task` → `prepare_args_internal` →
  `_pickle_Pickler_dump`, the program counter in `_PyErr_SetKeyError`: it was pickling the
  arguments of the next store when the signal came, and no second store was submitted.
- Ray's absl handler printed the dump twice, as at R0 and R1, but the driver then **exited 15**
  (`>> build.sh: 10-resumed-a ended, driver exit status 15`; `** driver exit status 15`) instead of
  aborting. `ray.init` installs a driver `SIGTERM` handler, `sys.exit(signum)`
  (`venv/lib/python3.13/site-packages/ray/_private/worker.py:1664-1665`); here it ran. Its
  `SystemExit` unwound through `main.py:1223`'s `with ShardedPool(…)`, and the package's
  `ShardedPool.__exit__` (`datastorekit/SQL/ShardedPool.py:857-884`) ran: 16 `Datastore.__exit__`
  calls created at 01:13:44.500–44.501, all `FINISHED` by 44.985 (each disposes its engine and
  cleans up its serial manager and profile batcher), then the actors were killed. The β = 1.1
  `object_store` returned at 44.917, before that actor's `__exit__`. `SystemExit` prints no
  traceback; the log has none (D3, Observation 1).

**What `resumed-a` kept** (`mode=ro`, before `resumed-b`):
- `ScalarModel`: all five, validated.
- `AdiabaticHistory`: **one stored, unvalidated**: β = 1.1 (shard 15, serial 1, 01:13:44.27,
  4947 values / 4947 `z_samples`). **β = 1.15, 1.2, 10 and 25 never stored.**
- `BBNData`: none.

**The actor check.**
- 01:13:54 (10 s after the signal): `./venv/bin/ray list actors --address 127.0.0.1:6380 --filter
  "state=ALIVE"` → `No resource in the cluster`.
- 01:14:22 (38 s): `--limit 200` → 34 actors, all `DEAD`: 16 `Datastore` (`shard0000-store` …
  `shard0015-store`) and 1 `SerialPoolBroker` for each of job `01000000` (`reference`) and job
  **`02000000` (`resumed-a`)**. `ray list jobs --address 127.0.0.1:6380`: both `SUCCEEDED`, the
  killed driver (pid 7671) included, as at R0 and R1.
- 01:17:51, before the stop: none alive; 85 actors, all dead, 17 for each of five jobs
  `01`–`05000000` (three builds, `plot_by_beta.py`, `plot_ScalarModel.py`; 80 `Datastore`, 5
  `SerialPoolBroker`), all five `SUCCEEDED`. `--inventory` starts no Ray now, so there is no sixth
  job (R1 had six).

**The prune report.** `resumed-b`'s open pruned one row, β = 1.1 on `shard0015-store`. The
driver's log carries the report in full, and so does the actor's own worker log, read before the
stop by one command,
`grep -h -A6 "INTEGRITY WARNING" /tmp/ray-cpbh/session_2*/logs/old/worker-*-03000000-*.out /tmp/ray-cpbh/session_2*/logs/worker-*-03000000-*.out`,
which ended before anything else ran. R2's session is `session_2026-10-13_01-05-32_375874_3758`;
its job-`03000000` file holding the report is
`logs/worker-04b4a1d61962e704ea36952d2b60765f51bdf7573302d6ce76b91425-03000000-10411.out` (under
`logs/`, not `logs/old/`, at that time). The report, in full:

```text
!! INTEGRITY WARNING (2026-10-13T01:14:52): datastore "shard0015-store" (physical file /Users/ds283/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/10-resumed-shard0015.db)
>> AdiabaticHistory instances
     The following unvalidated models were detected in the datastore:
       -- "dk-and-rpk-migration-10-resumed-a-AdiabaticHistory-ExponentialPotential(M=0.001Mp,Lambda=0.001eV)-ExponentialCoupling(beta=1.1)-2026-10-13T01:13:44" (store_id=1)
          contains 4947 z-sample values | expected=4947
     ** Note: these values have been pruned from the datastore.
```

No other `INTEGRITY WARNING` in any worker log of R2's session. The glob also matched R1's session
(`session_2026-10-12_16-43-24_306437_80134`), so the same command printed R1's three reports from
its `logs/old/` too; they are R1's, already recorded in log 06, and are not R2's evidence (D6). No
`>> Pruned the unvalidated rows of …` line appears in any R2 log.

## Verification performed

**Suites at dispatch** (foreground, serially, before `start`, no cluster up; output in the scratch
directory). 10 changes no code, so these are also the counts after.

| Root | `47cfcfb` (log 09) | At dispatch (`60e000c`), 01:00:30–01:05:24 | Load at start |
|---|---|---|---|
| `CosmologyModels/tests` | 18 | **18 OK** (67.6 s) | 12.19 |
| `Datastore/tests` | 97 | **97 OK** (14.1 s) | — |
| `ComputeTargets/tests` | 114 (61 + 53) | **114 OK**: `test_[a-f]*.py` 61 (102.4 s) + `test_[g-z]*.py` 53 (92.1 s) | 8.68 |

The load was 12.19 / 9.81 / 8.19 at 01:00:30 and 8.19 / 8.68 / 8.11 at 01:05:24.

**L1** (verification §3.3).
- `reference` and `resumed-b`: `** driver exit status 0`, all three stages `complete`.
- `resumed-a`: **15**, ended in the adiabatic stage (`INCOMPLETE; no queue summary`), Ray's absl
  dump twice; R1's and R0's were 134 (D3).
- `grep -c Traceback`: 0 in all three build logs and all six reader logs (with `1b`). `grep -c
  Error`: 0 in each but `10-resumed-a.log`, whose 2 hits are the dump's
  `PC: @ 0x100dcad84 (unknown) _PyErr_SetKeyError` line, once per copy.
- The provenance line
  `>> packages: datastorekit 0.3.0 (https://github.com/ds283/DatastoreKit@2f5417ea76cdcd0d15d64d80d9085429bfacad21, requested v0.3.0); raypoolkit 0.2.0 (https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976, requested v0.2.0)`
  is the first line of every build log, of each Ray reader's log and of both `--inventory` logs,
  once each (`grep -cxF` of the whole line). `bbn_from_store.py` and `compare_stores.py` print none,
  as at R1.
- Expected lines only: the `FutureWarning`, three super-Planckian warnings per build, the
  `@@ Parametrized equation-of-state` line `[repeated 15x across cluster]`, the plot scripts'
  16-against-10 shard warning. No `(raylet) … have not registered` line. `>> Created sharded
  datastore "…" with 16 shards` (`reference`, `resumed-a`) and `>> Opened existing sharded datastore
  "…" with 16 shards` (`resumed-b` and the plot scripts): R1's text.

**L2** (verification §3.4).
- The prune: only the one unvalidated row above; nothing validated existed in `resumed-a`'s
  adiabatic stage to be kept.
- **Stage counts**, `compute` (R1 in brackets): `reference` 5 / 5 / 5 (5 / 5 / 5); `resumed-a`
  5 / killed (5 / killed); `resumed-b` **0 / 5 / 5** (0 / 3 / 5), lookups **5 / 5 / 5** (5 / 3 / 5).
- **Label counts** (`mode=ro`): `10-reference.db` 5 / 5 / 5 `…-10-reference-…`. `10-resumed.db`:
  `ScalarModel` 5 `resumed-a`; `AdiabaticHistory` **5 `resumed-b`**; `BBNData` 5 `resumed-b`. All
  validated. `BBNData` `failure = 0`, `small_network = 1`, `PRyM_version = bf24c3d+ri02+sr01+bt02`
  in all ten rows.

  `resumed-b` computed exactly what `resumed-a` had not kept: 5 − *k* = 5 histories with *k* = 0
  validated rows kept, and every BBN row.
- The §4.3 comparison is below.

**L3** (verification §3.5).
- All six steps exit 0 (`status.txt`), and the second `--inventory`, on `10-reference.db`, exit 0.
- **The two inventories**, headers:
  ```text
  @@ DATASTORE INVENTORY: /Users/ds283/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/10-resumed.db
     read-only; 16 shards (#0, #1, #2, #3, #4, #5, #6, #7, #8, #9, #10, #11, #12, #13, #14, #15); 21,292 records in 21 classes
     problems: none
  @@ DATASTORE INVENTORY: /Users/ds283/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/10-reference.db
     read-only; 16 shards (#0, #1, #2, #3, #4, #5, #6, #7, #8, #9, #10, #11, #12, #13, #14, #15); 21,292 records in 21 classes
     problems: none
  ```
  **No problem line** in either. Each lists `ScalarModel (sharded): 5 records: 5 validated, 0
  unvalidated`, `AdiabaticHistory (sharded): 5 records: 5 validated, 0 unvalidated` and `BBNData
  (sharded): 5 records: 5 validated, 0 unvalidated`, each `version = 2026.6.0`, `unvalidated:
  none`, every record by its physical labels (β = 1.1, 1.15, 1.2, 10, 25; `failure=False`), each
  BBN record `PRyM_version=bf24c3d+ri02+sr01+bt02`. The two are identical but for the store's name
  and the commit times.
- **`histories.csv`: SHA-256 `d7c62c37d8ad637a63854583ab11bc737109dcbfba92168dbddbaea75182c522`**,
  R0's and R1's; `cmp` against `$R/06-readers/plot_by_beta/QCD_Cosmology/histories.csv` and against
  `$R/02-readers/…` each exit 0.
- **`bbn_from_store.py` β = 1.15:** stored `Yp=0.24687349447579363 DoH=2.4859924812264325`;
  re-solve `prod Yp=0.2468734945 DoH=2.485992481` (8.5 s). R0's figures exactly.
- Before/after comparison: `RESULT: equal`, bookkeeping differences `none`, problems `none`.

**The criterion** (verification §3.6), each run after the stop:

| Comparison | `RESULT:` | Exit |
|---|---|---|
| `compare_stores.py $R/06-reference.db $R/10-reference.db --json …` | `RESULT: equal` | 0 |
| `compare_stores.py $R/06-reference.db $R/10-resumed.db --json …` | `RESULT: equal` | 0 |
| `compare_stores.py $R/02-reference.db $R/10-reference.db --json …` | `RESULT: equal` | 0 |
| `compare_stores.py $R/10-reference.db $R/10-resumed.db --json …` | `RESULT: equal` | 0 |

Each: 27 tables, every row matched (5 / 42617 / 5 for `ScalarModel` / `ScalarModelValue` / tags,
the same for `AdiabaticHistory`, 5 / 7475 / 5 for `BBNData`, 21250 `redshift`), 0 A-only, 0 B-only,
no near miss, `problems: none`; the same 29 bookkeeping columns differing on every matched row as at
R1 (labels, timestamps, timings). **Notes:** in the three comparisons with an R0 or R1 store, one,
`A: LambdaCDM is listed as replicated by the primary but no shard holds it; not compared` (P8: R2's
primary does not list it); in `10-reference` against `10-resumed`, none. The four texts are in
`out/` whole; `10-compare-02-reference-10-reference.txt` differs from R1's
`06-compare-02-reference-06-reference.txt` only in the line naming B and the dropped B note, and its
JSON only in `b` and `notes`.

**The schema.** `--snapshot $R/10-reference.db` → `out/10-reference-schema.json` (27 client
tables); `--schema $R/10-reference.db --against out/08-standin-schema.json` → `RESULT: the schemas
agree`, exit 0; `--against out/02-reference-schema.json` → `RESULT: the schemas agree`, exit 0. The
three JSON files are equal in `format`, `version` and `tables`, and differ only in `source`. So the
schema is 08's, and differs from R0's by nothing, which is 08's list (empty).

**L4** (verification §3.7).
- Counts and pattern: above.
- **The printed lines** (`out/10-printed-lines.txt`), by 06's method:
  - `06-reference.log` against `10-reference.log`: **10 banner and completion lines agree in text
    and order**; `RESULT: the printed lines keep their layout`, exit 0. R0's `02-reference.log`
    against `10-reference.log`: the same.
  - **`10-reference.log` has no progress line**: no stage ran past the pool's 300 s notify
    interval. The check that every R2 progress shape occurs in R1's log therefore holds vacuously,
    and is said so in the file's header (Observation 3).
  - `06-resumed-b.log` against `10-resumed-b.log`: 11 lines agree, the no-compute line included.
  - Negative control: `10-reference.log` against `10-resumed-b.log` is reported as differing
    (10 against 11 lines), exit 1.
- `stage_counts.py` parsed all three of 10's logs.

**H1** (verification §3.8), on closed stores, no cluster:
1. **The two audits**, `./venv/bin/python -m datastorekit.tools.shard_key_audit $R/10-reference.db`
   (and `10-resumed.db`), each exit 0:
   ```text
   >> /Users/ds283/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/10-reference.db: current-generation schema (key_serial present)
   >> shard_keys row count: 5
   >> per-shard key distribution: {0: 1, 1: 1, 2: 1, 3: 1, 15: 1}
   >> cross-file check against shard #0: /Users/ds283/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/10-reference-shard0000.db (record '10-reference-shard0000.db')
   >> 'beta_value' table (shard #0) row count: 5
   VERDICT: OK -- no inconsistency found in /Users/ds283/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/10-reference.db.
   ```
   and the same for `10-resumed.db`, with its own name and shard #0 record
   (`'10-resumed-shard0000.db'`).
2. **The copy**, `./venv/bin/python -m datastorekit.tools.sharded_store copy $R/10-reference.db
   $R/10-reference-copy.db`: exit 0, `>> copy: "…/10-reference-copy.db" with 16 shards` and shards
   #0–#15 at `…/10-reference-copy-shard0000.db` … `-shard0015.db`. 17 files; none existed before.
3. **Its inventory**, `main.py --config exponential.yaml --database $R/10-reference-copy.db
   --inventory`: exit 0, `read-only; 16 shards (#0, …, #15); 21,292 records in 21 classes`,
   `problems: none`; from its third line it is byte-identical to `1b-inventory-reference.log`.
   **Its shard records** (`sqlite3 "file:$R/10-reference-copy.db?mode=ro" "select * from shards
   order by 1;"`): `0|10-reference-copy-shard0000.db` … `15|10-reference-copy-shard0015.db`, its
   own shards, bare.
4. **Its comparison**, `compare_stores.py $R/10-reference.db $R/10-reference-copy.db`: `RESULT:
   equal`, exit 0, 27 tables matched, bookkeeping differences `none`, problems `none`.
5. **The delete**, `./venv/bin/python -I <scratch>/delete_copy.py`, exit 0. `closed_store_files`
   gave 17 paths, shards `10-reference-copy-shard0000.db` … `-shard0015.db` then
   `10-reference-copy.db`, all under `$R`, equal to the 17 `10-reference-copy*` files on disk;
   `delete_store` returned the same 17 paths in the same order; `delete_store's list equals
   closed_store_files': True`; `files matching 10-reference-copy* left: 0`. Both lists are in
   `out/10-h1-delete.txt` whole. The script, in full:

   ```python
   """dk-and-rpk-migration prompt 10, H1 step 5: delete the copy 10-reference-copy.db with the
   package's own ShardedPool.delete_store, on the copy's primary alone.
   
   Run from the repository root with ./venv/bin/python -I (no PYTHONPATH; the package is the
   installed datastorekit 0.3.0). It first lists, by closed_store_files, what delete_store will
   delete; checks that list is exactly the copy's 17 files (16 shards and the primary, each in the
   rehearsal root, each named 10-reference-copy*); and only then calls delete_store, prints what it
   deleted, and checks the two lists are equal and that no 10-reference-copy* file remains.
   """
   
   import glob
   import sys
   from pathlib import Path
   
   from datastorekit.SQL.ShardedPool import ShardedPool
   
   ROOT = Path.home() / "ScienceData" / "ChamPBH-rehearsals" / "dk-and-rpk-migration"
   PRIMARY = ROOT / "10-reference-copy.db"
   
   expected = sorted(
       [ROOT / f"10-reference-copy-shard{i:04d}.db" for i in range(16)]
   ) + [PRIMARY]
   on_disk = sorted(Path(p) for p in glob.glob(str(ROOT / "10-reference-copy*")))
   
   print(f">> primary: {PRIMARY}")
   print(f">> on disk before: {len(on_disk)} files matching 10-reference-copy*")
   for p in on_disk:
       print(f"     {p}")
   
   planned = ShardedPool.closed_store_files(PRIMARY)
   print(f">> closed_store_files: {len(planned)} files")
   for p in planned:
       print(f"     {p}")
   
   if [Path(p).resolve() for p in planned] != [p.resolve() for p in expected]:
       print("!! closed_store_files is not exactly the copy's 17 files; nothing deleted")
       sys.exit(1)
   if sorted(Path(p).resolve() for p in planned) != [p.resolve() for p in on_disk]:
       print("!! closed_store_files differs from the copy's files on disk; nothing deleted")
       sys.exit(1)
   print(">> closed_store_files is exactly the copy's 17 files (16 shards, then the primary)")
   
   deleted = ShardedPool.delete_store(PRIMARY)
   print(f">> delete_store returned: {len(deleted)} files")
   for p in deleted:
       print(f"     {p}")
   
   same = [Path(p) for p in deleted] == [Path(p) for p in planned]
   left = glob.glob(str(ROOT / "10-reference-copy*"))
   print(f">> delete_store's list equals closed_store_files': {same}")
   print(f">> files matching 10-reference-copy* left: {len(left)}")
   for p in left:
       print(f"     {p}")
   sys.exit(0 if same and not left else 1)
   ```

**Hygiene.**
- No tracked file changed but the two this commit edits (the verification document and the
  board); it adds this log and the 20 `out/10-*` files.
- Opened: `10-reference.db`, `10-resumed.db` (by the builds and readers, `mode=ro` by the scratch
  scripts, and by the package's tools), `10-reference-copy.db` (made, inventoried, compared,
  deleted), `10-readers/before/` (by `run_readers.sh`'s step 5); R0's and R1's stores by
  `compare_stores.py` and `sqlite3` `mode=ro` only (the scratch row lister was tried on
  `06-resumed` before the builds, and the first `beta_value` row's timestamps were read on
  `06-reference`), and `cmp` of their `histories.csv`. Of the 376 entries under `$R` at the gate,
  every one has its mtime and size but the root and `logs/` directories, which gained the 10
  entries (188 `10-*` entries now at all depths).
- The pilot store in the repository root, `~/ChamPBH-stores/` and `~/ChamPBH-stores-bt02/` were not
  opened.
- The venv: `pip freeze` equals the copy taken at the gate (101 lines).
- After the stop: no process matching `ray-cpbh|gcs_server|raylet|ray::|ray.util.client`; the lock
  absent; `~/ScienceData` holds only `ChamPBH-rehearsals`; `/tmp/ray/ray_current_cluster` absent.
  No background task of this session is running.
- Scratch files are in the session scratchpad, not in the repository. Nothing was written in a
  sibling repository; DatastoreKit's tools were read in the venv and its checklist by `git show
  v0.3.0:…`.

## Deviations from the prompt

- **D1 — `STRUCTURALLY REQUIRED`.** The adiabatic banner's time, for `S`, was taken from Ray's task
  records (`ray list tasks --address 127.0.0.1:6380 --detail`), not from a progress line as 06 did:
  `10-reference.log` has no progress line. The timestamps used for `S` are still `10-reference.db`'s
  own, as the prompt asks. The same records were read for `resumed-a` (job `02000000`) to say where
  the signal fell. Every such call carried `--address 127.0.0.1:6380`.
- **D2 — `IMPLEMENTATION CHOICE`.** `S` = 1 s. The prompt's rule (the earliest of `reference`'s
  rows less about 3 s) gives a negative delay at this load, where the whole adiabatic stage took
  4.35 s; 1 s is the smallest delay that leaves the signal after `reference`'s first commit. The
  result was one row stored and none validated.
- **D3 — `IMPLEMENTATION CHOICE`, flagged for the review.** `resumed-a` exited **15**, where §1.3
  and §2 (L1) expect 134, and §4 lists "does not end as §2 says" as a stop. The agent judged it is
  not that condition and went on: the build ended by the signal the protocol sent, in the
  adiabatic stage, with no traceback; 15 is the signal's own number, given by Ray's driver
  `SIGTERM` handler (`sys.exit(signum)`), and the same absl dump preceded it. The stored result was
  then compared and is equal. What differs from R1 is that the pool was closed by `SystemExit`
  before the driver exited (Observation 1). If the orchestrator reads "(134)" as binding, this is a
  stop the agent did not take, and the user should rule; nothing after `resumed-a` depended on the
  status number.
- **D4 — `IMPLEMENTATION CHOICE`.** Outputs beyond the prompt's list, as at R0 and R1:
  `10-stage-counts.json` (the tool's own `--json`) and `10-readers.txt` (abridged reader output);
  `10-printed-lines.txt` also holds the two `resumed-b` logs' comparison, R0 against R2, and a
  negative control. H1's outputs are named `10-h1-*.txt` (and the comparison's `.json`), as the
  dispatch facts ask.
- **D5 — `IMPLEMENTATION CHOICE`.** The suites ran in the order CosmologyModels, Datastore, then the
  two halves of ComputeTargets, rather than the order 06 lists. All before `start`.
- **D6 — `UNINTENDED DRIFT`, harmless.** The prune-report `grep`'s glob, `session_2*`, matched every
  session under `/tmp/ray-cpbh`, R1's included, so it printed R1's three reports beside R2's one.
  It read only; R2's report is identified by its session and its job. Separately, the first write of
  `10-printed-lines.txt` recorded the exit codes of `tail`/`sed` for its sections 3 and 4; it was
  regenerated before the commit with the script's own codes (section 4's is 1, as a negative
  control must be).

## The deliberate-breakage record

None. 10 changes no code (prompt §3.6; README §5 rule 8). Its checks are each one a bad build would
fail:
- a non-zero exit of `reference` or `resumed-b`, a missing `ALL WORK ITEMS COMPLETE`, a traceback,
  or a provenance line that does not name `datastorekit` 0.3.0 at `2f5417ea…` and `raypoolkit` 0.2.0
  at `eaef0ce6…` (L1);
- a driver attached to the wrong cluster (its connect line would not name 6380);
- a package actor that loses the factory instances, or a broker that leases wrong serials: the
  build would fail, or store rows the comparison does not match;
- a resume that recomputes a kept row or prunes a validated one (L2: label counts, the prune
  report);
- a reader that fails or writes (L3: `status.txt`, the before/after comparison), an `--inventory`
  that reports a problem, a `histories.csv` differing from R0's by one byte, or a different stored
  or re-solved Yp or D/H;
- **a single changed bit in any stored science value** against R1 or R0 (the criterion), or a
  changed schema;
- a pool that prints its banners and completion lines in another layout (L4; the extraction reports
  a difference when the line sequences differ, shown by the negative control);
- a store the package's audit finds inconsistent, a copy that does not open read-only or compare
  equal, or a `delete_store` that names a file not the copy's (H1).

## Observations not acted on

1. **`resumed-a` exited 15, and its pool was closed before the driver exited.** At R0 and R1 a
   `SIGTERM`ed driver aborted (134) with its main thread in native Ray code; here the main thread
   was in the pickler (Python), Ray's driver handler raised `SystemExit(15)`, and `main.py`'s
   `with ShardedPool` block ran the package's `__exit__`: every actor's `__exit__`, then the kill.
   The in-flight store committed first. It is Ray's behaviour, and it is timing-dependent; a
   killed build can end either way. Nothing stored differs (the criterion). Not opened: not a
   defect in ChamPBH or the package. The prompt's expectation of 134 (§1.3, §2) is the protocol's
   to update, if the orchestrator wishes.
2. **`resumed-a` validated no `AdiabaticHistory` row and stored one.** By validated rows this is
   the "0" case; by stored rows it is partial. So the resume tested four histories absent from the
   store (the case R1 did not reach) and the prune of one unvalidated row; it did not test the reuse
   of a stored, validated row in the adiabatic stage, which R1 did (two) and the scalar stage does
   here (five). The stage was 4.35 s long and ran at `reference`'s pace, so a one-second poll cannot
   place the signal much finer. Not repeated: the prompt gives no store names for a repeat.
3. **No progress line at this load.** The builds ran three times faster than R1's (load near the
   CPU count), so no stage reached the pool's 300 s notify interval, and L4's progress-shape check
   holds vacuously for R2. The banners, completion and summary lines, and the no-compute line, are
   R1's in text and order. RayPoolKit is the same commit as at R1, where the progress pair was shown
   in R0's shapes.
4. **The banner's time** had to come from Ray's task records (D1). They give each actor call's
   creation, start and end time, which date the stages to the millisecond; a later rehearsal could
   choose `S` from them directly.
5. **The plot scripts' shard-count warning** (log 02, Observation 3) is unchanged, as expected.

## State handed to the next prompt

- **R2 holds, bitwise.** `10-reference.db` and `10-resumed.db` each equal
  `~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/06-reference.db`, and `10-reference.db`
  equals `02-reference.db`, by `compare_stores`, `RESULT: equal`, exit 0; so do the two R2 stores.
  07–09 (the `datastorekit` pin, the layer's replacement, the read-only inventory) changed nothing a
  build stores or reads, and the pool prints what it printed.
- **The package has run under real Ray at 2.53.0**: three builds and two Ray readers, each job with
  16 `shardNNNN-store` actors and one `SerialPoolBroker`; ChamPBH's registry of factory instances
  crossed into the package's actors intact (README §0.3, point 2); the package's prune at open
  printed the factory's report and removed exactly the unvalidated row.
- **The stores**, kept and not to be deleted (README §3): `$R/10-reference.db` and
  `$R/10-resumed.db`, 16 shards each; `$R/logs/10-{reference,resumed-a,resumed-b}.{log,pid}`;
  `$R/10-readers/` (with `1b-inventory-reference.log`). The copy `10-reference-copy*` is gone.
- **L4's figures at R2**:
  - `reference` `compute` 5 / 5 / 5; stages 1m 54.6s · 4.35s · 21.2s; wall 3 min 33 s;
  - `resumed-a` 5 / killed, **exit 15**; it stored 1 `AdiabaticHistory` row and validated 0;
  - `resumed-b` 0 / 5 / 5, lookups 5 / 5 / 5; the prune removed exactly the 1 unvalidated row.
- **The cluster**: `ray_cluster.sh start` took 6 s at a load of 8 and `stop` 18 s; both behaved as
  log 05a describes. Every Ray driver connected to `127.0.0.1:6380`.
- **Outputs**: `.documents/dk-and-rpk-migration/out/10-*` (20 files), including the R2 schema
  snapshot `10-reference-schema.json`, equal to 08's and R0's but for its `source`.
- **Suites at `60e000c`:** CosmologyModels 18, ComputeTargets 114 (61 + 53), Datastore 97.
- **No ChamPBH Ray process is running, the rehearsal lock is free**, and the four ports are free.
  No §3 issue opened; the board's four open issues are unchanged (`.documents/OPEN_ISSUES.md`: 27
  open, 4 on this board).
- **For 11:** the G3 record below, for its draft for DatastoreKit's board; the protocol's
  expectation that a killed driver exits 134 (Observation 1), if 11's records restate it.
- **The DatastoreKit G3 record**, for prompt 11 to carry into its draft for DatastoreKit's board
  (P16):

> **DatastoreKit G3 holds for ChamPBH** (the ChamPBH checklist's item 9, with the `v0.3.0`
> addendum: "CPBH has adopted `v0.2.3` or later"), at ChamPBH commit `b0d258a` (branch
> `dk-and-rpk-migration`), 2026-10-13, by prompts 07–10:
> 1. **The 9 layer files are gone, and ChamPBH imports `datastorekit` at the pinned tag.**
>    `Datastore/object.py`, `Datastore/SQL/{ClientPool,Datastore,ProfileAgent,SerialPoolBroker,ShardedPool}.py`
>    and `Datastore/SQL/ObjectFactories/base.py` are deleted, and `Datastore/__init__.py` and
>    `Datastore/SQL/__init__.py` are 0-byte files (P6) (prompt 08, `857ec73`); DatastoreKit's
>    measuring script at that tree reports 0 layer imports, 0 string literals and 0
>    `import_module` calls outside `prompts/`. `requirements.txt:17` holds
>    `datastorekit @ git+https://github.com/ds283/DatastoreKit@v0.3.0`, installed at
>    `2f5417ea76cdcd0d15d64d80d9085429bfacad21`, not editable (prompt 07, `617a1dc`).
> 2. **Its suites pass:** `CosmologyModels/tests` 18, `ComputeTargets/tests` 114,
>    `Datastore/tests` 97, at `60e000c` (the code of `47cfcfb`).
> 3. **A pilot store is rebuilt through the package.** The user's 2026-10-02 pilot grid (five
>    histories at M = 10⁻³ M_P), built twice under Ray 2.53.0 through the package's `ShardedPool`,
>    `Datastore` actors and `SerialPoolBroker` (once straight through; once killed in the adiabatic
>    stage and resumed with `--prune-unvalidated`), and read by every reader. Each store equals the
>    rehearsal built at the tree before the swap (R1, ChamPBH's own layer) and the start tree (R0)
>    bit for bit in every stored science column, by ChamPBH's `tools/compare_stores.py` (`RESULT:
>    equal`, exit 0); the schema equals R0's.
>
> Beyond item 9: the package's prune at open removed exactly the unvalidated row a killed build
> left; `main.py --inventory` reads the rebuilt stores through `read_inventory` with no problem; and
> `python -m datastorekit.tools.shard_key_audit` gives `VERDICT: OK` on both stores, while a
> `python -m datastorekit.tools.sharded_store copy` opens read-only, compares equal and is deleted by
> `ShardedPool.delete_store`, its 17 files and nothing else. ChamPBH's record:
> `.documents/dk-and-rpk-migration-verification.md` §3. The record on DatastoreKit's own extraction
> board is a commit in that repository (P16).
