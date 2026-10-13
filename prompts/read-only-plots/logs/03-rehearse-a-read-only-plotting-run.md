# Log 03 — rehearse a read-only plotting run

**Subject:** Rehearse a read-only plotting run on a real store under Ray
**Commit:** this commit, on branch `read-only-plots`, on top of `27f77d4`
**Date:** 2026-10-13, by Claude Opus 5.5
**Result:** **COMPLETE.** H1–H4 done; the gate "R" holds. The build exited 0 with 5 / 5 / 5 / 5 in
each stage; all five readers exited 0; the store's 17 files are byte-identical, with their mtimes
and sizes, before the readers, after them and after the three refusals; `compare_stores` `RESULT:
equal` with no bookkeeping difference; `histories.csv` is R0's. The three refusals each exited 2
with their messages, the `T_stop` one through a real actor. No §3 stop condition was met. No code,
test or script changed. No issue opened; `.documents/OPEN_ISSUES.md` unchanged (26).

## What shipped

- **H1 — the build.** `REHEARSAL_ROOT=$R build.sh 03 reference reference 127.0.0.1:6380`
  (`R=~/ScienceData/ChamPBH-rehearsals/read-only-plots`), 22:29:39 – 22:33:20, **3 min 41 s, exit
  0**; stages 2m 12s · 3.59s · 24.1s (R2 3 min 33 s; 1m 54.6s · 4.35s · 21.2s). `stage_counts.py`:
  lookup 5, compute 5, store 5, validation 5 in each of the three stages, 0 skipped. The provenance
  line is the log's first line, once, R2's:
  `>> packages: datastorekit 0.3.0 (https://github.com/ds283/DatastoreKit@2f5417ea76cdcd0d15d64d80d9085429bfacad21, requested v0.3.0); raypoolkit 0.2.0 (https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976, requested v0.2.0)`.
  The job name reads `dk-and-rpk-migration-03-reference`, because `build.sh` is run unchanged, as
  the prompt says it would.
- **H2 — the readers.** `REHEARSAL_ROOT=$R run_readers.sh 03 reference 127.0.0.1:6380`, 22:33:43 –
  22:34:55 (1 min 12 s), every step exit 0. Both plot logs print the read-only open line once, and
  never the read-write one. The SM baseline, `histories.csv` (SHA-256 `d7c62c37…c522`), the β = 1.15
  stored and re-solved Yp and D/H, the inventory's head and counts, and the stage counts are R2's
  recorded figures. `5-compare_stores.log`: `RESULT: equal`, bookkeeping `none`, problems `none`.
  The 17 files' SHA-256s, sizes and mtimes: `cmp` exit 0 against the list before.
- **H3 — the refusals**, each exit 2, the messages verbatim in the verification document §1.6:
  `--ray-address auto` (01's message, no new job); `--T-stop-GeV 1e-7` (02's message, through job
  `04000000`'s real `Datastore` actors, before the baseline and any figure, its output directory not
  created, its 16 actors `DEAD`); `--drop bbn-data` with the valid address (the `--drop` message, no
  new job). The 17 files unchanged after them.
- **H4 — the record.** `.documents/read-only-plots-verification.md` §1 (new), and ten small text
  outputs under `.documents/read-only-plots/out/` (56 KB in all; the largest 14.8 KB):
  `03-build.txt`, `03-checksums.txt`, `03-cluster.txt`, `03-compare_stores.{txt,json}`,
  `03-inventory.txt`, `03-readers.txt`, `03-refusals.txt`, `03-stage-counts.{txt,json}`.
- **The board**: row 03, H1–H4, the gate "R", §5.4.

## Deviations from the prompt

1. **`start`'s exit status was not captured directly.** I ran
   `ray_cluster.sh start "read-only-plots 03 (R)" 2>&1 | tee <scratch>/start.txt; echo "start exit
   ${PIPESTATUS[0]}"`, and the shell here is zsh, where `PIPESTATUS` is unset, so the line printed
   `start exit ` with no number. It was not re-run, since a second `start` would have exited 3 on
   the lock it had just taken. The exit status is 0 by the script's code, whose only path printing
   `started in` returns 0 on the next line (`ray_cluster.sh:336-337`), and by `status` straight
   after, exit 0, showing the lock held for this purpose and 18 ChamPBH processes. Every later
   command's status was taken by `echo $?` directly after it, with no pipe. **UNINTENDED DRIFT**
   (in my command, not in the rehearsal).
2. **The refusals' output names** under `$R/03-refusals/`: `ray-address-auto`, `T-stop-1e-7` and
   `drop-bbn-data`, each with its `.log`. The prompt leaves `<name>` open. **IMPLEMENTATION
   CHOICE.**
3. **Per-reader wall times** are from consecutive output files' mtimes (approximate to a second),
   since `run_readers.sh` does not time its steps and runs unchanged. **IMPLEMENTATION CHOICE.**
4. **The committed logs have ANSI colour codes removed** (`sed -E 's/\x1b\[[0-9;]*m//g'`) and are
   otherwise verbatim; the `.json` files are copied unchanged. **IMPLEMENTATION CHOICE.**
5. **The checksum lists were also compared as a whole.** Step 6's "byte-identical, with mtimes and
   sizes unchanged" is `cmp` of the two `shasum` lists and of the two `stat -f '%m %z %N'` lists,
   each exit 0, rather than file by file. **IMPLEMENTATION CHOICE.**

## Verification performed

- **Gate**, at 22:23: `HEAD` `27f77d4`, no tracked change; `ray_cluster.sh status` exit 0, the lock
  free, no ChamPBH process, the four ports free; `$R` absent and its parent empty;
  `/tmp/ray/session_*` 160; no Ray process.
- **Suites** before `start`, serially, in the foreground, no cluster up: CosmologyModels **18** OK
  (78.3 s, load 6.7 → 6.1); ComputeTargets **61** OK (113.5 s, 6.1 → 3.0) + **91** OK (116.1 s,
  3.0 → 2.5) = 152; ObjectFactories **63** OK (5.3 s, 2.4 → 2.8); tools **34** OK (7.7 s, 2.8).
  02's counts exactly; no re-run needed (`test_prym_passenger` passed at load ≈ 3).
- **`installed_package_provenance()`** before `start`: both packages at the pinned commits and
  `requested_revision`s, `editable: False`.
- **The cluster.** `start` (6 s; deviation 1), lock owner `project: ChamPBH`, `purpose:
  read-only-plots 03 (R)`, `acquired: 2026-10-13T22:29:24+01:00`, `host: mps026088`,
  `ray_temp_dir: /tmp/ray-cpbh`, `ray_address: 127.0.0.1:6380`, `pid: 89356`. `ray status --address
  127.0.0.1:6380` exit 0: one node, 10 CPU. Every Ray driver's log names 6380 and never 6379.
- **The build**, waited on by its background task's notification and a `Monitor` on the log's
  stage and exit lines; the `Monitor` was stopped as soon as the build ended. No `Traceback` or
  `Error` in the log.
- **The checksums**: 17 files before (22:33:39), after the readers (22:35:00) and after the
  refusals (22:36:13); both `cmp`s exit 0 each time; no `-journal`, `-wal` or `-shm` found.
- **The readers**, waited on by the background task's notification. `status.txt`: steps 0–5 exit
  0. No `Traceback`/`Error` in any log; neither plot log has the read-write open line.
- **`histories.csv`**: `d7c62c37d8ad637a63854583ab11bc737109dcbfba92168dbddbaea75182c522`, equal
  to `dk-and-rpk-migration/out/10-readers.txt:116`.
- **Jobs and actors** at the end: four jobs `01`–`04000000`, all `SUCCEEDED`; 65 actors, all
  `DEAD`, none `ALIVE` (16 `Datastore` + 1 `SerialPoolBroker` for the build; 16 `Datastore` for
  each read-only job).
- **The stop** at 22:36:22, as a command on its own: exit 0, 18 s, the lock released, the ports
  free; `status` exit 0, the lock free, no process. `ray stop` never run. No background task or
  monitor was running across it.
- **Nothing outside `$R`**: `git status` after the stop shows only the user's untracked set;
  `~/ScienceData` holds only `ChamPBH-rehearsals/`; `/tmp/ray/session_*` still 160;
  `pip freeze` before and after, `cmp` exit 0, 101 lines, `ray==2.53.0`. Neither the user's pilot
  store nor `~/ChamPBH-stores*` was opened.
- **Sizes**: every committed output under 15 KB.

## The deliberate-breakage record

None. This prompt changes no code, test or script, so there is nothing to break.

## Observations not acted on

1. **A read-only pool starts no `SerialPoolBroker`.** Jobs `02`–`04000000` each have 16 `Datastore`
   actors and no broker (R2's plot jobs had a broker each). The package builds read-only actors
   with `serial_broker=None` (`datastorekit/SQL/ShardedPool.py:563` at `v0.3.0`), so this is by its
   design; the brief's "and its broker must end `DEAD`" did not anticipate it. Not an issue.
2. **PRyMordial solves.** The prompt's "one PRyMordial solve" undercounts: the build makes five
   (one per β), `plot_by_beta.py` one (the SM baseline) and `bbn_from_store.py` one; seven in all.
3. **Ray forwards an actor's line after the refusal.** In the `T_stop` log the last line is a
   `Datastore` actor's `@@ Parametrized equation-of-state` line, printed after the `!! plot:` message
   (Ray forwards worker output asynchronously). It is not a traceback and does not follow from the
   refusal.
4. **The load peaked at 100** as the build ended (R2: 63), from the build's own Ray workers; the
   build ran 8 s slower than R2's, its scalar stage 17 s slower. Not a finding.
5. **Shards 4–14 are the eleven smallest files** (1,748,992 bytes each, one mtime), and they have
   one of two SHA-256s (`e6aa5936…` for 4, 7, 9, 12; `0afca84a…` for the other seven). Not
   investigated: they are unchanged by the readers, which is what H2 asks.
6. `main.py --inventory` is given `--ray-address` by `run_readers.sh` and ignores it (U1), as at R2.

## State handed to the next prompt

- The gate "R" holds: under real Ray, on a store `main.py` built, `plot_by_beta.py`,
  `plot_ScalarModel.py`, `main.py --inventory`, `bbn_from_store.py` and `compare_stores.py` change no
  byte, mtime or size of the store's 17 files, and leave no journal. Both plot scripts print
  `>> Opened existing sharded datastore "…" read-only with 16 shards (version label "2026.6.0",
  serial 1)`; the 16-against-10 shard warning stays (P4).
- Under real Ray the refusals are: `--ray-address auto`, exit 2 before `ray.init`; an unstored
  `--T-stop-GeV`, exit 2 with `!! plot: the store holds no temperature for --T-stop-GeV 1e-07; …`
  after the pool opens and before the baseline (the `ReadOnlyMiss` crosses `ray.get` and is
  caught); `--drop`, exit 2 before Ray. A read-only pool starts no broker.
- `histories.csv` is still `d7c62c37…c522`; the SM baseline and the β = 1.15 BBN figures are R2's.
- `$R` = `~/ScienceData/ChamPBH-rehearsals/read-only-plots/` is kept, 172 paths by `find` with
  the root (the store's 17 files, `logs/`, `03-readers/` with `before/`, `03-refusals/`); removing
  it is the user's (README §3). The cluster is stopped, the lock free.
- The record is `.documents/read-only-plots-verification.md` §1 and
  `.documents/read-only-plots/out/03-*`. 04 adds what it needs to the documents; it changes neither.
- Suites at this commit, as at `9dc23db`: CosmologyModels 18, ComputeTargets 152 (61 + 91),
  ObjectFactories 63, tools 34.
- No issue opened; `.documents/OPEN_ISSUES.md` holds 26, unchanged.
