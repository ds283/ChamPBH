# Log 06 — rehearse before the DatastoreKit swap (R1)

**Subject:** Record the R1 rehearsal before the DatastoreKit swap
**Commit:** `c9e6bc2`, on branch `dk-and-rpk-migration`, on top of `5bb2be6`
**Date:** 2026-10-12, by Claude Opus 5.5
**Result:** **COMPLETE.**
- **The criterion holds: R1 equals R0 bit for bit.** `compare_stores` of `02-reference.db` against
  `06-reference.db`, and against `06-resumed.db`, each give `RESULT: equal`, exit 0; so does
  `06-reference.db` against `06-resumed.db`. The schema snapshot agrees with R0's.
- L1–L3 hold. L4: the counts, the resume's pattern and the pool's printed lines are R0's.
- **`resumed-a` stored all five `AdiabaticHistory` rows and validated two** (Observation 1): by
  stored rows, the "5" case the brief names. Recorded, not a stop; the prune at open removed the
  three unvalidated rows and `resumed-b` recomputed them.
- No code changed. No §4 stop condition was met. No §3 issue opened.

The full record is
[`.documents/dk-and-rpk-migration-verification.md`](../../../.documents/dk-and-rpk-migration-verification.md)
§2. This log gives what was run and found, the deviations and the observations.

## What shipped

- `.documents/dk-and-rpk-migration-verification.md`: **§2 R1** added after §1 (§§2.1–2.8). §1 is
  not edited.
- `.documents/dk-and-rpk-migration/out/` (11 files, 98 KB in all; the largest is the schema
  snapshot, 32 KB):
  - `06-compare-02-reference-06-reference.txt` / `.json`, `06-compare-02-reference-06-resumed.txt` /
    `.json`, `06-compare-06-reference-06-resumed.txt` / `.json`: the three comparisons;
  - `06-reference-schema.json`: the snapshot of `06-reference.db`;
  - `06-stage-counts.txt` / `.json`: `stage_counts.py` on the three build logs;
  - `06-printed-lines.txt`: L4's printed-lines comparison;
  - `06-readers.txt`: the readers' output, abridged as `02-readers.txt` is.
- This log; the board (§1 row, L1–L4 (R1), V2, gate R1, §5.10).

Nothing else. No store, log or figure is committed. The stores, logs and reader outputs are under
`~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/` (`$R`), and nothing there was deleted,
moved or renamed.

## The run

**Gate, checked at 16:30–16:31 BST.**
- `HEAD` `5bb2be6`; no tracked change; nothing staged; the untracked set the user's 23 entries.
- `pip freeze`: 100 lines, `ray==2.53.0`, the one `raypoolkit @ git+…@eaef0ce6…` line, no
  `datastorekit`; `pip check` clean. `utilities.installed_package_provenance()` gave
  `raypoolkit` 0.2.0, commit `eaef0ce6…`, requested `v0.2.0`, **`editable: False`**, and
  `datastorekit` `None`; `format_package_provenance` gave the prompt's L1 line exactly.
- `ray_cluster.sh status`, exit 0: the lock free, no ChamPBH process, 6380 / 6381 / 8267 / 52367
  free. `$R` held R0's files only, no `06-*`; `~/ScienceData` held only `ChamPBH-rehearsals`.
- 10 CPUs. Load 29.17 / 27.57 / 23.69 at 16:31. **Two foreign suites were running**
  (`python -m unittest discover -s datastorekit/tests -t .`, pids 69287 and 69288, about 90 % CPU
  each, from a DatastoreKit session's scratchpad). Recorded and continued, as the prompt says; both
  had ended by 16:43:15, before the cluster started (Observation 4).

**The suites** ran next, before `start`, in the foreground, serially, output in the scratch
directory (§"Verification performed").

**The cluster.**
- `ray_cluster.sh start "dk-and-rpk-migration 06 (R1)"` at 16:43:19, exit 0, 22 s wall: address
  `127.0.0.1:6380`, dashboard `http://127.0.0.1:8267`, actor line
  `./venv/bin/ray list actors --address 127.0.0.1:6380`, "started in 21s; gcs_server pid 80158".
  Load 45.30 / 40.09 / 30.74 just before.
- Owner file: `project: ChamPBH` / `purpose: dk-and-rpk-migration 06 (R1)` /
  `acquired: 2026-10-12T16:43:41+01:00` / `host: mps026088` / `ray_temp_dir: /tmp/ray-cpbh` /
  `ray_address: 127.0.0.1:6380` / `pid: 80158`.
- `./venv/bin/ray status --address 127.0.0.1:6380`: one node, `0.0/10.0 CPU`, 8.42 GiB memory,
  2.00 GiB object store.
- `ray_cluster.sh stop` at 17:09:48, a command on its own, with nothing else running: exit 0, 19 s;
  `SIGTERM` to 18 processes, `SIGKILL` to 80158 and 80161 after the 15 s grace; the four ports
  free; the lock released. `ray_cluster.sh status` then, exit 0: lock free, no ChamPBH process,
  ports free. **`ray stop` was never run.**

**The builds** (start and end from `date` around each script; R0's beside):

| Build | Started | Ended | Wall | Exit status | R0 |
|---|---|---|---|---|---|
| `build.sh 06 reference reference 127.0.0.1:6380` | 16:43:53 | 16:55:00 | 11 min 7 s | 0 | 10 min 43 s, 0 |
| `build.sh 06 resumed resumed-a 127.0.0.1:6380` | 16:55:30 | 17:03:44 | 8 min 14 s | 134 (`SIGTERM` at 17:03:41) | 7 min 48 s, 134 |
| `build.sh 06 resumed resumed-b 127.0.0.1:6380 --prune-unvalidated` | 17:04:28 | 17:07:10 | 2 min 42 s | 0 | 1 min 35 s, 0 |
| `run_readers.sh 06 resumed 127.0.0.1:6380` | 17:07:30 | 17:09:26 | 1 min 56 s | 0 | 1 min 46 s, 0 |

Stage times: `reference` 5m 6.61s · 16.2s · 1m 5.62s (R0 5m 24.7s · 12.6s · 1m 8.73s);
`resumed-a` 4m 29.7s · killed; `resumed-b` 1.41s · 12s · 56.5s. Load rose to
**159.55 / 157.88 / 127.42** at `reference`'s end and fell to 42–50 by the readers (verification
§2.1).

**The connect line.** Every driver (three builds, three Ray readers) logged
`Connecting to existing Ray cluster at address: 127.0.0.1:6380...` and the dashboard at
`http://127.0.0.1:8267`. No log names 6379.

**`S` = 6 s, and why.**
- After `reference` ended, `06-reference.db` was read `mode=ro` (a scratch script using Python's
  `sqlite3` with `file:…?mode=ro`).
- The adiabatic banner fell at about **16:53:34.7**: the scalar stage's one progress line is at
  16:53:34, 5m 5.89s running, and the stage took 5m 6.61s.
- The five `AdiabaticHistory` rows: 16:53:39.84, 40.73, 41.07, 42.90, 44.48 — banner + **5.1, 6.0,
  6.4, 8.2, 9.8 s**. The stage took 16.2 s, not R0's 12.6 s.
- `S` = 6 s puts the signal at banner + 6–7 s (one-second polling), inside the spread between the
  second and fourth rows, with 3 s of margin before the last, because at R0 `resumed-a`'s rows came
  about 2.6 s earlier after the banner than `reference`'s.

**`interrupt_when.py`** (second background task), in UTC: `15:55:30` waiting; `15:55:31` driver
pid 86620; `16:03:35` saw the banner, waiting 6 s; `16:03:41` sent `SIGTERM` to pid 86620; exit 0.

**What `resumed-a` kept** (`mode=ro`, before `resumed-b`):
- `ScalarModel`: all five, validated.
- `AdiabaticHistory`: **all five stored**; β = 1.15 (17:03:37.45) and β = 1.1 (38.01) validated;
  **β = 1.2 (38.51), 10 (39.41) and 25 (40.25) unvalidated**, each with all its value rows
  (5002, 9776, 17917).
- `BBNData`: none.

The stage ran faster after the banner than in `reference`: every row was stored within 5.2 s of the
banner being seen, and the signal fell after the last store and before three validations
(Observation 1).

**The actor check.**
- 17:03:55 (14 s after the signal): `./venv/bin/ray list actors --address 127.0.0.1:6380 --filter
  "state=ALIVE"` → `No resource in the cluster`.
- 17:04:09 (28 s): `--limit 200` → 34 actors, all `DEAD`: 16 `Datastore` and 1 `SerialPoolBroker`
  for each of job `01000000` (`reference`) and job **`02000000` (`resumed-a`)**.
  `ray list jobs --address 127.0.0.1:6380`: both `SUCCEEDED`, the killed driver (pid 86620)
  included, as at R0.
- 17:09:42, before the stop: none alive; 102 actors, all dead, 17 for each of six jobs
  `01`–`06000000` (three builds, then `--inventory`, `plot_by_beta.py`, `plot_ScalarModel.py`), all
  six `SUCCEEDED`.

**The prune reports.** `resumed-b`'s open pruned three rows, one per shard:
`shard0001-store` β = 1.2 (store_id 3, 5002 of 5002 values), `shard0002-store` β = 10 (store_id 4,
9776 of 9776), `shard0003-store` β = 25 (store_id 5, 17917 of 17917), each "pruned from the
datastore". The driver's log prints the β = 1.2 report in full and the β = 25 report with
`[repeated 2x across cluster]`: Ray's deduplication folded the β = 10 one away (Observation 2).
All three were read in full from Ray's worker logs for job `03000000`
(`/tmp/ray-cpbh/session_2026-10-12_16-43-24_306437_80134/logs/worker-*-03000000-91246.out`,
`-91247.out`, `-91249.out`), by one `grep`, before the stop.

## Verification performed

**Suites at dispatch** (foreground, serially, before `start`, no cluster up; output in the scratch
directory). 06 changes no code, so these are also the counts after.

| Root | `babd9d5` (log 05a) | At dispatch (`5bb2be6`), 16:31–16:43 | Load at start |
|---|---|---|---|
| `CosmologyModels/tests` | 18 | **18 OK** (190.2 s) | 27.87 |
| `ComputeTargets/tests` | 114 (61 + 53) | **114 OK**: `test_[a-f]*.py` 61 (237.1 s) + `test_[g-z]*.py` 53 (246.3 s) | — |
| `Datastore/tests` | 73 | **73 OK** (6.9 s) | — |

The load was 27.87 / 27.37 / 23.69 at 16:31:18 and 48.28 / 40.58 / 30.86 at 16:43:10; the two
foreign DatastoreKit suites ran during the first minutes.

**L1** (verification §2.3).
- `reference` and `resumed-b`: `** driver exit status 0`, all three stages `complete`.
- `resumed-a`: 134, ended in the adiabatic stage (`INCOMPLETE; no queue summary`), Ray's absl dump
  twice; R0's was 134 too.
- `grep -c Traceback` and `grep -c Error`: 0 in all three build logs and all five reader logs.
- The provenance line
  `>> packages: datastorekit not installed; raypoolkit 0.2.0 (https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976, requested v0.2.0)`
  is the first line of every build log and of each Ray reader's log, once each (counted by
  `grep -c` of the whole line, anchored).

**L2** (verification §2.4).
- The prune: only the three unvalidated rows above; the two validated `resumed-a` rows survive.
- **Stage counts**, `compute` (R0 in brackets): `reference` 5 / 5 / 5 (5 / 5 / 5); `resumed-a`
  5 / killed (5 / killed); `resumed-b` **0 / 3 / 5** (0 / 2 / 5), lookups **5 / 3 / 5** (5 / 2 / 5).
- **Label counts** (`mode=ro`): `06-reference.db` 5 / 5 / 5 `…-06-reference-…`. `06-resumed.db`:
  `ScalarModel` 5 `resumed-a`; `AdiabaticHistory` **2 `resumed-a` + 3 `resumed-b`**; `BBNData`
  5 `resumed-b`. All validated. `BBNData` `failure = 0`, `small_network = 1`,
  `PRyM_version = bf24c3d+ri02+sr01+bt02` in all ten rows.

  `resumed-b` computed exactly what `resumed-a` had not kept: 5 − *k* = 3 histories with *k* = 2,
  and every BBN row.
- The §4.3 comparison is below.

**L3** (verification §2.5).
- All six steps exit 0 (`status.txt`).
- `--inventory`: 5 validated `ScalarModel`, `AdiabaticHistory` and `BBNData` each, none unvalidated,
  version `2026.6.0`.
- **`histories.csv`: SHA-256 `d7c62c37d8ad637a63854583ab11bc737109dcbfba92168dbddbaea75182c522`**,
  R0's; `cmp` against `$R/02-readers/plot_by_beta/QCD_Cosmology/histories.csv` exits 0.
- **`bbn_from_store.py` β = 1.15:** stored `Yp=0.24687349447579363 DoH=2.4859924812264325`;
  re-solve `prod Yp=0.2468734945 DoH=2.485992481` (12.5 s). R0's figures exactly.
- Before/after comparison: `RESULT: equal`, bookkeeping differences `none`, problems `none`.

**The criterion** (verification §2.6), each run after the stop:

| Comparison | `RESULT:` | Exit |
|---|---|---|
| `compare_stores.py $R/02-reference.db $R/06-reference.db --json …` | `RESULT: equal` | 0 |
| `compare_stores.py $R/02-reference.db $R/06-resumed.db --json …` | `RESULT: equal` | 0 |
| `compare_stores.py $R/06-reference.db $R/06-resumed.db --json …` | `RESULT: equal` | 0 |

Each: 27 tables, every row matched (5 / 42617 / 5 for `ScalarModel` / `ScalarModelValue` / tags,
the same for `AdiabaticHistory`, 5 / 7475 / 5 for `BBNData`, 21250 `redshift`), 0 A-only, 0 B-only,
no near miss, `problems: none`; bookkeeping differing on every matched row (labels, timestamps,
timings); notes only `LambdaCDM` on both primaries. The three texts differ from each other, and
from R0's `02-compare-reference-resumed.txt`, only in the header lines naming the stores.

**The schema.** `--snapshot $R/06-reference.db` → `out/06-reference-schema.json` (27 client tables);
`--schema $R/06-reference.db --against out/02-reference-schema.json` → `RESULT: the schemas agree`;
the two JSON files differ only in `source`.

**L4** (verification §2.7).
- Counts and pattern: above.
- **The printed lines** (`out/06-printed-lines.txt`): from `02-reference.log` and
  `06-reference.log`, normalised. **The 10 banner and completion lines agree in text and order.**
  06's log **has** a progress pair (scalar stage, 5m 5.89s running), and both its shapes occur in
  R0's log, the `inflight … | completed …` line with its four columns including `store`. So the
  check was not vacuous. In addition, `02-resumed-b.log` against `06-resumed-b.log`: 11 lines agree,
  the no-compute line included. As a negative control, the same extraction reports
  `02-reference.log` against `02-resumed-b.log` as differing.
- `stage_counts.py` parsed all three of 06's logs.

**Hygiene.**
- No tracked file changed but the 14 this commit adds or edits.
- Opened: `06-reference.db`, `06-resumed.db` (by the builds and readers, and `mode=ro` by the scratch
  script), `06-readers/before/` (by `run_readers.sh`'s step 5), and R0's two stores by
  `compare_stores.py` only. No `02-*` file under `$R` has an mtime after 02:00 today.
- The pilot store's primary is still dated 2026-10-02 23:33. No file under `~/ChamPBH-stores/` or
  `~/ChamPBH-stores-bt02/` is newer than 2026-10-05.
- The venv: 100 lines of `pip freeze`, the same `raypoolkit` line, no `datastorekit`.
- After the stop: no process matching `ray-cpbh|gcs_server|raylet|ray::|ray.util.client`; the lock
  absent; `~/ScienceData` holds only `ChamPBH-rehearsals`; `/tmp/ray/ray_current_cluster` absent.
  No background task of this session is running.
- Scratch files are in the session scratchpad, not in the repository.

## Deviations from the prompt

- **D1 — `IMPLEMENTATION CHOICE`.** `S` = 6 s was set at the upper part of `reference`'s spread
  rather than its middle, leaving 3 s before the last row, because R0's `resumed-a` ran about 2.6 s
  faster after the banner than its `reference`. `resumed-a` ran faster still, and stored all five
  rows (Observation 1).
- **D2 — `IMPLEMENTATION CHOICE`.** The prune report for β = 10 was read from Ray's worker log under
  `/tmp/ray-cpbh/session_…/logs/`, by one `grep` run and finished before the stop, because the
  driver's log had deduplicated it (Observation 2). That `grep` named `/tmp/ray-cpbh` but did not
  outlive its call.
- **D3 — `IMPLEMENTATION CHOICE`.** Outputs beyond the prompt's list, as at R0: `06-stage-counts.json`
  (the tool's own `--json`) and `06-readers.txt` (abridged reader output in `02-readers.txt`'s
  form). `06-printed-lines.txt` also holds a second comparison, the two `resumed-b` logs, and the
  raw extracted lines of each log.
- **D4 — `UNINTENDED DRIFT`, corrected.** The first attempt at the three comparisons was a `zsh`
  loop that relied on word-splitting (`set -- $pair`), which `zsh` does not do. Each
  `compare_stores.py` call was given one path with a space in it and `""`, refused at once
  (`!! … primary "…/02-reference 06-reference.db" is not a file`, exit 2), and opened nothing. Its
  three 140-byte error files, written under `out/` with spaces in their names, were deleted, and
  the comparisons were run as the three explicit commands of verification §2.8. No store was
  touched.

No `STRUCTURALLY REQUIRED` deviation.

## The deliberate-breakage record

None. 06 changes no code (prompt §3.5; README §5 rule 8). Its checks are each one a bad build would
fail:
- a non-zero exit, a missing `ALL WORK ITEMS COMPLETE`, a traceback, or a provenance line that does
  not name `raypoolkit` 0.2.0 at `eaef0ce6…` (L1);
- a driver attached to the wrong cluster (its connect line would name 6379);
- a resume that recomputes a kept row or prunes a validated one (L2: label counts, prune reports);
- a reader that fails or writes (L3: `status.txt`, the before/after comparison), a `histories.csv`
  differing from R0's by one byte, or a different stored or re-solved Yp or D/H;
- **a single changed bit in any stored science value** against R0 (the criterion), or a changed
  schema;
- a pool that prints its banners, completion lines or progress lines in another layout (L4; the
  extraction is shown to report a difference when the line sequences differ).

## Observations not acted on

1. **`resumed-a` stored all five `AdiabaticHistory` rows, and validated two.**
   - The signal fell 6 s after the banner was seen; the five rows had been stored 2.4–5.2 s after
     it, and β = 1.2, 10 and 25 were still awaiting validation. In `reference` the same rows came
     5.1–9.8 s after the banner. At R0, too, `resumed-a`'s rows came earlier than `reference`'s.
     The reload of the five `ScalarModel`s at the stage's start seems to be what varies.
   - **By stored rows, this is the "5" case that the brief names**: no row was *never stored*, so
     the resume did not exercise a history absent from the store (R0's β = 25 was). What it did
     exercise is a larger prune at open (three unvalidated rows, against R0's one) and their
     recomputation beside two kept rows: *k* = 2 by validated rows, a partial stage. The
     comparison shows the result equal to R0's.
   - Not repeated: a repeat needs store names the prompt does not give. The orchestrator may take it
     to the user. For R2, a smaller `S` (about 3 s after the banner is seen) would sit inside both
     builds' spreads as measured here and at R0.
2. **Ray deduplicates the actors' prune reports in the driver's log.** Three actors printed
   near-identical reports at the same second; the driver's log shows one in full and one marked
   `[repeated 2x across cluster]`, and the β = 10 report does not appear in it. The worker logs
   hold all three. L2's prune evidence therefore needs Ray's worker logs (before the cluster's
   stop, or from `/tmp/ray-cpbh/session_…` afterwards) or the label counts, not the driver's log
   alone. R0's single report was not affected. `build.sh` could set `RAY_DEDUP_LOGS=0`, but the
   scripts are run unchanged (README §4), so this is for 10's note, not a change here. Not opened:
   Ray's display behaviour, not a defect in ChamPBH or the scripts; the orchestrator may open one.
3. **The killed driver's native stack differs from R0's**: the main thread was in
   `CoreWorker::submit_actor_task` (pickling arguments), where R0's waited in `CoreWorker::Wait()`.
   The driver was submitting the validations when it died. Same status (134), same handler, no
   `Traceback` or `Error`. Nothing to open.
4. **The machine was not idle.** Two foreign DatastoreKit suites (pids 69287, 69288) ran at the gate
   and through the suites' first minutes, ending before `start`. The load then rose to about 160
   during `reference`, unattributed (the process list was not sampled during the builds; the Ray
   workers of this build are part of it). The bitwise result held, and no BBN solve came near its
   limit.
5. **The plot scripts' shard-count warning** (log 02, Observation 3) is unchanged, as expected.

## State handed to the next prompt

- **R1 holds, bitwise.** `06-reference.db` and `06-resumed.db` each equal
  `~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/02-reference.db` by `compare_stores`,
  `RESULT: equal`, exit 0; so do the two R1 stores. 03–05 (`raypoolkit`, the data move, the mapping
  shard keys) changed nothing a build stores, reads or prints. **R2 (10) is held to `RESULT: equal`
  against `06-reference.db`** (and so `02-reference.db`).
- **`raypoolkit` 0.2.0 has run under real Ray at 2.53.0**: three builds and three readers, every
  pool's banners, progress pair, completion and summary lines in R0's layout.
- **The stores**, kept and not to be deleted (README §3): `$R/06-reference.db` and
  `$R/06-resumed.db`, 16 shards each; `$R/logs/06-{reference,resumed-a,resumed-b}.{log,pid}`;
  `$R/06-readers/`.
- **L4's figures at R1**:
  - `reference` `compute` 5 / 5 / 5; stages 5m 6.61s · 16.2s · 1m 5.62s; wall 11 min 7 s;
  - `resumed-a` 5 / killed, exit 134; it stored 5 `AdiabaticHistory` rows and validated 2;
  - `resumed-b` 0 / 3 / 5, lookups 5 / 3 / 5; the prune removed exactly the 3 unvalidated rows.
- **`S` for R2.** Here the adiabatic stage took 16.2 s in `reference`, rows at banner + 5.1–9.8 s,
  but `resumed-a`'s rows came 2.4–5.2 s after the banner was seen; at R0, 4.6–7.6 s and 2.0–4.0 s.
  `S` = 6 s was too late for a row to go unstored. 10 should allow for `resumed-a` running 3 s or
  more ahead of its `reference`.
- **Ray's log deduplication can hide prune reports** in the driver's log (Observation 2): read the
  worker logs in `/tmp/ray-cpbh/session_*/logs/` or rely on the label counts.
- **The cluster**: `ray_cluster.sh start` took 22 s at a load of about 45 and `stop` 19 s; both
  behaved as log 05a describes. Every driver connected to `127.0.0.1:6380`.
- **Outputs**: `.documents/dk-and-rpk-migration/out/06-*` (11 files), including the R1 schema
  snapshot `06-reference-schema.json`, equal to R0's but for its `source`.
- **Suites at `c9e6bc2`:** CosmologyModels 18, ComputeTargets 114 (61 + 53), Datastore 73.
- **No ChamPBH Ray process is running, the rehearsal lock is free**, and the four ports are free.
  No §3 issue opened; the board's three open issues are unchanged.
