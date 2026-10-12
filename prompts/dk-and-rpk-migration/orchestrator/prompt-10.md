# Orchestrator — prompt 10, rehearse through both packages (R2)

Read [`../README.md`](../README.md) first: §2.2 (10's charter), §3, §4.1 (as amended for U5),
§4.2, §4.3, §5 (rules 8, 9, 11, 14, 15, 16) and §6.6. The conventions are those of
[`prompt-01.md`](prompt-01.md) §0 ("Conventions"), as [`prompt-06.md`](prompt-06.md) applied them
to R1, and are not repeated here except where 10 changes them. **This note is 06's note at 09's
tree**; 06's review (board §5.10) is the model for this one.

**You do not write code.** You may:
- run the suites, `compare_stores.py`, `stage_counts.py`, `--snapshot`, `main.py --inventory` and
  DatastoreKit's two read-only tools on the agent's stores, and R0's and R1's through
  `compare_stores.py`;
- read every store with `sqlite3` in `mode=ro`, never through a pool;
- run `ray_cluster.sh status`;
- fix small residue in a follow-up commit of your own (§4).

**You never run `ray stop`**, and you start no cluster: the review needs none.

**The prompt:** [`10-rehearse-through-both-packages.md`](../10-rehearse-through-both-packages.md)
**Board items:** L1–L4 (R2), V3, H1; gates R2 and DatastoreKit G3 · **Closes:** nothing ·
**Opens:** only what the work finds · **Model:** Opus, as the prompt recommends · **No network.**

**Gate:**
- 09 landed (`47cfcfb`) and was reviewed (`a17ea8a`).
- `HEAD` is the commit that adds this note, or a later one that touches no code.
  `git diff --stat 47cfcfb HEAD -- . ':!prompts' ':!.documents'` is empty at `a17ea8a`.
- `ray_cluster.sh status` shows the lock free, no ChamPBH process and the four ports free. No `10-*`
  entry exists under the rehearsal root. If either fails at dispatch, stop and ask the user.
- One prompt at a time in this checkout; nothing installs into the venv while 10 runs.

## 0. What makes this prompt unusual

**It is the first time the package's actors and broker run under real Ray.** Every test since 08
ran DatastoreKit's undecorated actor inside ChamPBH's stand-in cluster; no Ray process has yet
held a `datastorekit` `Datastore` or `SerialPoolBroker`. So three things are shown here for the
first time, and none can be shown any other way: ChamPBH's registry of factory *instances*
crosses into the package's actors intact (README §0.3, point 2; U3); the broker leases serials to
16 shards under a real pool; and the actors' prune at open, now the package's code calling the
factories' `validate_on_startup`, removes the same rows ChamPBH's did. A failure in any of them is
a §4 stop, never a workaround (README §5 rule 11).

**The criterion is bitwise, as at R1.** R0 and R1 were bitwise equal, and a vacuous "equal" (the
wrong pair, a store the build never wrote, a table skipped) would let 07–09 through unmeasured. So
the review re-runs the four comparisons and checks the stored bits of every history again with
its own script, against `02-reference.db` and `06-reference.db`.

**The resume is only a test if it is partial.** R1's `resumed-a` stored all five
`AdiabaticHistory` rows and validated two; the case of a history absent from the store was R0's
alone. The prompt now aims `S` near 3 s. If `resumed-a` stores **0 or 5** rows, or validates 0 or
5, that is **not** a stop: the agent says so plainly, finishes the protocol, and the orchestrator
takes it to the user.

**What the orchestrator checked before dispatch** (in the session scratchpad, no cluster, nothing
in the repository or under the rehearsal root written):
- **H1's tool chain, on a 16-shard stand-in store** built by 08's method
  (`StandinCluster.open_pool(…, shards=16, version_label="2026.6.0", serial_batch_sizes=…)`,
  closed; log 08 §2.3.4). `python -m datastorekit.tools.shard_key_audit <primary>`:
  `VERDICT: OK`, exit 0 (on an empty store it reports `shard_keys row count: 0` and does the
  cross-file check against shard #0). `python -m datastorekit.tools.sharded_store copy SRC DST`:
  exit 0, prints the destination's 16 shard paths; the copy's `shards` table names
  `store-copy-shard0000.db`, … (bare). A second `copy` onto the same `DST` refuses, exit 1.
  `main.py --config exponential.yaml --database <copy> --inventory`: exit 0,
  `read-only; 16 shards (#0, …, #15)`, `problems: none`. `compare_stores.py <store> <copy>`:
  `RESULT: equal`, exit 0. Then, from a scratch script run with `./venv/bin/python -I`,
  `ShardedPool.closed_store_files(copy)` (17 paths) equals `ShardedPool.delete_store(copy)`'s
  return (both are `@staticmethod`s; `datastorekit.SQL.ShardedPool`, `:2688`, `:2703`), and no
  `store-copy*` file remains. **The agent should call `closed_store_files` first and compare**: it
  names exactly what `delete_store` will delete, and refuses as it would, deleting nothing.
- **`compare_stores` reads R1's store against a package-built one**: `06-reference.db` against the
  stand-in store runs to the end with `problems: none` and one per-store note,
  `A: LambdaCDM is listed as replicated by the primary but no shard holds it; not compared` (P8;
  board §5.13's hand-on). That note is expected in every comparison of an R0 or R1 store with an R2
  store, and is not a difference.

**The facts, checked by the orchestrator on 2026-10-13 at `a17ea8a`.** Pass them on.
- **The tree.** Branch `dk-and-rpk-migration`, `HEAD` `a17ea8a` before this note. Untracked: the
  user's 17 `pilot-2026.6.0*.db` files, `pilot-main.log`, `pilot-plot.log`, `pilot-out/`,
  `full_run_2026.6.0.sh`, `full_run_2026.6.0-refresh.sh` and `prompts/jordan-normalization/`.
  Nothing is staged. None is the agent's. **No code changed since 09** (`47cfcfb`); since R1's tree
  (`c9e6bc2`) the code changed by 07–09 only. `build.sh`, `interrupt_when.py`, `stage_counts.py`,
  `run_readers.sh`, `ray_cluster.sh`, `tools/compare_stores.py`, `tools/bbn_from_store.py` and
  `utilities.py` are byte-for-byte as R1 ran them (`git diff c9e6bc2 HEAD` on them is empty).
- **Ray, the lock, the ports**, at 00:55. `ray_cluster.sh status` exits 0: the lock free
  (`~/ScienceData/.rehearsal-lock` absent), no ChamPBH process, 6380 / 6381 / 8267 / 52367 free. No
  Ray process at all on the machine; Ray's default ports 6379 / 10001 / 8265 / 52365 free.
  `~/ScienceData` holds only `ChamPBH-rehearsals/`. `/tmp/ray-cpbh/ray_current_cluster` is stale
  from R1 (`127.0.0.1:6380`); `start` rewrites it. `/tmp/ray/ray_current_cluster` does not exist.
- **The venv.** 101 lines of `pip freeze`; `ray==2.53.0`; `datastorekit @ git+…@2f5417ea…`,
  `raypoolkit @ git+…@eaef0ce6…`; `pip check` clean. From the root, with `PYTHONPATH=.`,
  `utilities.format_package_provenance(installed_package_provenance())` is exactly
  `>> packages: datastorekit 0.3.0 (https://github.com/ds283/DatastoreKit@2f5417ea76cdcd0d15d64d80d9085429bfacad21, requested v0.3.0); raypoolkit 0.2.0 (https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976, requested v0.2.0)`,
  log 07's line, both `editable: False`. Every build log, every Ray reader's log and both
  `--inventory` logs should begin with it.
- **The machine.** 10 CPUs. Load **5.52 / 5.67 / 6.26 at 00:55** (Screen Sharing, WindowServer,
  Spotlight); no foreign suite or Ray job. R1 ran at loads above 10, peaking near 160.
- **The rehearsal root** `~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/` holds R0's and
  R1's two stores each (17 files), `logs/` (six logs, six pidfiles), `02-readers/` and
  `06-readers/`: **376 entries** at all depths, and no `10-*`. Nothing named `02-*` or `06-*` is
  opened except by `compare_stores.py` and `sqlite3` `mode=ro`, and nothing there is deleted, moved
  or renamed. The only deletion the campaign allows is the copy of §1.4.6, by `delete_store`.
- **What R1 measured** (verification §2), for L4 and for choosing `S`:
  - `reference` 11 min 7 s, stages 5m 6.61s · 16.2s · 1m 5.62s; R0 10 min 43 s;
  - `S` = 6 s: `reference`'s five `AdiabaticHistory` rows at banner + 5.1, 6.0, 6.4, 8.2, 9.8 s (the
    banner time from the scalar stage's end); `resumed-a`'s at about + 2.4 – 5.2 s after the banner
    was *seen* (17:03:35; rows 37.45 – 40.25), so the signal came after all five stores and before
    three validations. `interrupt_when.py` polls once a second;
  - `resumed-a` exited **134** (absl dump, twice, no `Traceback`); all 34 actors of the two jobs
    dead at 28 s;
  - `resumed-b` 2 min 42 s, computed 0 / 3 / 5, lookups **5** / 3 / 5 (board §3
    `[02-scalar-stage-missing-filter-keeps-every-model]`, still open: the scalar stage's lookups stay
    5 with compute 0);
  - the prune: three reports (β = 1.2, 10, 25), only two reaching the driver's log (Ray's
    deduplication); all three read in full from
    `/tmp/ray-cpbh/session_*/logs/old/worker-*-<job id>-*.out` (`logs/old/` once the job has ended,
    not `logs/` as log 06 says);
  - readers 1 min 56 s; `histories.csv` SHA-256 `d7c62c37…c522`; β = 1.15 Yp/D/H R0's;
  - the printed lines: 10 banner and completion lines equal in text and order to R0's, and one
    scalar-stage progress pair in R0's two shapes.
- **What changes at R2, by design, and is not a finding:**
  - the provenance line (above), and nothing else about the connect lines (still 6380 / 8267);
  - the primary is the package's: shards recorded by bare name, `replication_in_flight`,
    `LambdaCDM` not replicated (the note above);
  - **the schema**: 08 found **no** difference between the package's schema at this registry and
    R0's (`out/08-standin-schema.json` equals `02-reference-schema.json` but for `source`). So
    "differs from R0's only as 08 listed" means **agrees with both**; any difference is a stop;
  - `--inventory` is 09's read-only report, before Ray: `1-inventory.log` (run by `run_readers.sh`
    with `--shards 16 --ray-address`, both unused by that branch) and §1.4.1's
    `1b-inventory-reference.log`. Its layout is new (board §5.14's P15 table), so its lines are not
    in L4's comparison. On R2's stores it should print `problems: none`, 21 classes, and the five
    `ScalarModel`, `AdiabaticHistory` and `BBNData` records validated under version `2026.6.0`,
    none unvalidated, each with its physical labels and BBN's `PRyM_version`.
- **What should not change:** the `>> Created` / `>> Opened existing sharded datastore … with 16
  shards` lines (the package prints the same text); the `!! INTEGRITY WARNING` header and the
  factories' prune lines (the package's actor prints what `validate_on_startup` returns); the actor
  names (`shardNNNN-store` ×16, one `SerialPoolBroker`, per job); every pool line (RayPoolKit is
  unchanged since R1). The three validating classes are all sharded, so the prune is the actors',
  not the pool's `_prune_replicated_tables`; a `>> Pruned the unvalidated rows of …` line from the
  pool would be unexpected, and recorded.
- **What the drivers print that is not a failure** (as at R1): Ray's `FutureWarning` about
  `RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO`; the three super-Planckian `!! warning:` lines; the
  `@@ Parametrized equation-of-state …` line from the actors (deduplicated by Ray); in the plot
  scripts, `!! WARNING: number of shards read from database (=16) does not match specified number
  of shards (=10)`; under load, `(raylet) … have not registered within the timeout`; the killed
  driver's absl dump. Anything else containing `Traceback` or `Error` is read in full and recorded.
- **The suites at `47cfcfb`**: CosmologyModels 18, ComputeTargets 114 (61 with
  `-p 'test_[a-f]*.py'`, 53 with `-p 'test_[g-z]*.py'`), Datastore **97**. The agent runs them
  **before `start`**, never with a cluster up. `test_inventory_report` runs `main.py` in four
  subprocesses at an address nothing listens on; that is not a cluster.
- **`.documents/OPEN_ISSUES.md`**: 27 open, 4 on this board, last updated 2026-10-13. An issue 10
  opens goes under this board's section, with the count and the header's prose corrected.
- **The siblings.** DatastoreKit `04a2ed9`, RayPoolKit `1b84df0`, SecondaryGWKit `a5a6f6d`,
  StochasticInstantons `5fb4a1d`, each clean. DatastoreKit's tools are read at the tag
  (`git -C ../DatastoreKit show v0.3.0:<path>`) or in the venv (which equals the tag); nothing is
  written in any sibling.
- **Expected duration.** About 11 minutes for `reference`, 8 for `resumed-a`, 3 for `resumed-b`, 2
  for the readers, a few seconds for H1, plus the suites (about 8 minutes) and the start and stop:
  under an hour.

**What the review exists to establish.**
- **(E1) The builds.** Each log has the provenance line naming both tags' commits, the connect line
  naming 6380, the banners, the summaries and `** driver exit status`. `resumed-a` died by the
  signal in the adiabatic stage, and its actors were gone before `resumed-b`.
- **(E2) The resume.** By the stage counts and the labels in `10-resumed.db`: `resumed-b` computed
  exactly what `resumed-a` had not kept; the package's prune removed only unvalidated rows.
- **(E3) The criterion.** The four comparisons, re-run, give the agent's results; the stored bits of
  each history in both R2 stores equal `02-reference.db`'s and `06-reference.db`'s by the
  orchestrator's own script.
- **(E4) The readers.** All six steps exit 0; the store unchanged by them; `histories.csv`
  byte-identical; the β = 1.15 re-solve R0's; both `--inventory` reports clean.
- **(E5) Schema and L4.** The schema agrees with 08's and R0's snapshots; R1's counts and resume
  pattern; the pool's printed lines in R1's layout.
- **(E6) H1.** The audit clean on both stores; the copy opened read-only, compared equal, and was
  deleted by `delete_store`, its 17 files and nothing else.
- **(E7) The cluster and hygiene.** Started and stopped by the script alone, under the lock; nothing
  left running; R0's and R1's files and every other store untouched; the venv unchanged; no code
  changed.
- **(E8) The records.** Verification §3 R2, §1 and §2 unedited; `out/10-*`; the board, both gates;
  the G3 record's text in the log; the log; the suites.

**Conventions particular to 10.** 06's hold unchanged (git by pathspec; the cluster by
`ray_cluster.sh` only; background tasks waited on by notification, or `Monitor` on the log's
`** driver exit status` line, never a `sleep` loop; no command naming `/tmp/ray-cpbh` left running
across a stop; no parallel Bash calls while a `stop` runs; commands verbatim with the real address
and `S`). In addition:
- **Stores.** The agent opens only `10-reference.db`, `10-resumed.db`, `10-reference-copy.db` and
  `10-readers/before/`, and R0's and R1's read-only through `compare_stores.py` (and `sqlite3`
  `mode=ro`). Not the pilot store, not `~/ChamPBH-stores/`, not `~/ChamPBH-stores-bt02/`.
  `main.py --inventory` is run on R2's stores and the copy only.
- **Outputs committed** are small text only, under `.documents/dk-and-rpk-migration/out/10-*`. A
  file over about 1 MB is a finding to report before committing. Name the comparisons
  `10-compare-06-reference-10-reference`, `10-compare-06-reference-10-resumed`,
  `10-compare-02-reference-10-reference` and `10-compare-10-reference-10-resumed` (`.json` and
  `.txt`); H1's outputs `10-h1-*.txt`.
- **The delete script** lives in the agent's scratch directory, is run with `./venv/bin/python -I`,
  calls `closed_store_files` and then `delete_store` on `10-reference-copy.db` only, and prints both
  lists. It is quoted in the log in full.
- **A miss against `06-reference.db` or `02-reference.db`** is a stop *after* the readers, H1 and the
  records are done: the agent finishes §1.4 (the stop of the cluster included), writes the log and
  the verification document, and **does not commit**.

## 1. Before you dispatch

1. **The tree.** Record the branch and `HEAD`. `git status --short` shows only §0's untracked
   entries, and `git diff --cached` is empty. `git log --oneline a17ea8a..HEAD` touches no code.
2. **The cluster.** `ray_cluster.sh status` exits 0 and shows the lock free, no process, the four
   ports free; `ps` finds no Ray process. No `10-*` under the rehearsal root.
3. **The venv.** `pip freeze` saved in the scratchpad (101 lines); `pip check` clean; the
   provenance line as §0.
4. **The load.** `uptime`, and the top of `ps -Ao pid,pcpu,comm -r`, recorded and passed on.
5. **Hygiene baselines**, for check 11: `/usr/bin/stat -f '%m %z %N'` over every entry under the
   rehearsal root (376), and over `~/ChamPBH-stores/*.db`, `~/ChamPBH-stores-bt02/*.db`,
   `pilot-2026.6.0*.db`, `pilot-main.log` and `pilot-plot.log` (53 files); the four siblings'
   `HEAD`s.
6. **The pre-dispatch H1 prototype** (§0), its scratch store deleted by `delete_store`; the
   `06-reference` against stand-in comparison kept for check 9.

The suites are not re-run by the orchestrator at dispatch: no Python has changed since `47cfcfb`,
where they were 18 / 114 / 97, and the agent runs them.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Opus**. Give it:
- the prompt;
- the campaign README and this board;
- prompt 06 (the protocol 10 repeats) and its log; logs 07, 08 and 09 for what the prompt's "Read
  first" names;
- `HEAD`, the load and §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the
  session scratchpad. The agent does not read `orchestrator/`, and does not read prompt 11 (none
  is written).

Tell it plainly:
- **One commit, at the end, by pathspec.** Its log is `logs/10-rehearse-through-both-packages.md`,
  in README §5.1's form, with the prompt's §6 additions.
- **The files it may add or change** are the prompt's §5 list (the verification document's new §3,
  `.documents/dk-and-rpk-migration/out/10-*`, the log, the board) and `.documents/OPEN_ISSUES.md`
  if an issue opens. **No code, no script of 01's or 05a's, no tool, nothing in a sibling, nothing
  in the venv.**
- **It runs the protocol as written,** waits on each build by notification, and keeps the suites
  away from the cluster.
- **Never `ray stop`.** No parallel Bash calls while a `stop` runs; no `stop` while a build runs.
- **Stop and ask** on any of the prompt's §4 conditions, after `ray_cluster.sh stop` (unless the
  stop is itself what failed), with `status`'s output and the logs' paths. A failure of the
  package's actor, broker or pool under Ray is recorded in full and stopped on; it is never
  worked around.
- **It replies with:**
  - the log's path and the commit; `git diff --cached --name-only` before it and
    `git show --stat HEAD` after it;
  - the load, the cluster's start and stop (each command's output, its timing, the owner file),
    and `ray status --address`;
  - each build's start, end, wall time and exit status, and the connect line;
  - `S`, and the timestamps it was chosen from;
  - the actor check, with the job ids;
  - the stage counts, the label counts, and what `resumed-a` kept;
  - each prune report, in full;
  - the readers' statuses, both inventories' headers and problem lines, the Yp/D/H comparison and
    `histories.csv`'s SHA-256;
  - each comparison's `RESULT:` line and exit code, its notes, and its full text if not 0;
  - the schema comparisons;
  - the printed-lines comparison;
  - H1: the two audits, the copy, its inventory and shard records, its comparison, the delete
    script and both lists;
  - the three suites, with the load;
  - the G3 record's text;
  - every deviation with its tag;
  - the "State handed to the next prompt" section, verbatim.

## 3. The review — twelve checks

1. **Scope.** At the agent's commit, `git show --stat HEAD` touches only the verification document,
   `out/10-*`, `logs/10-…`, the board and, if an issue opened, the index.
   `git diff HEAD~1 HEAD -- .documents/dk-and-rpk-migration-verification.md` adds lines after §2
   and changes none of §1's or §2's (invariant 6). `git status` shows §0's untracked set and nothing
   new; no leftover branch; no other commit of the agent's.
2. **The cluster.** `ray_cluster.sh status`: lock free, no process, ports free; `ps` finds no Ray
   process. The log shows `start`'s output (address, owner file with purpose
   `dk-and-rpk-migration 10 (R2)`), `ray status --address` with 10 CPUs, `stop` exit 0 with its
   timing, and `status` after it. `grep -n 'ray stop'` finds no invocation.
3. **E1, the logs.** Read all three build logs in full, and the readers' logs.
   - The provenance line once each, exactly §0's, in every build log, the three Ray readers' logs
     and both inventory logs.
   - The connect line names `127.0.0.1:6380`; no line names 6379 or 8265.
   - The banners and summaries; `grep -n 'Traceback\|Error'` gives only §0's known lines.
   - `reference` and `resumed-b` end `** driver exit status 0`; `resumed-a` ends 134 after the
     adiabatic banner and before any BBN banner, its pid the one in `10-resumed-a.pid`.
   - The actor names, from the log's `ray list actors` output: 16 `shardNNNN-store` and one
     `SerialPoolBroker` per job, of the package's classes.
4. **E2, the counts.** Re-run `stage_counts.py` on the three logs and diff with the committed text
   and JSON. Then, with the orchestrator's own `sqlite3` `mode=ro` script over every shard, count
   `ScalarModel`, `AdiabaticHistory` and `BBNData` rows by label prefix, and `validated`.
   Expected: `10-reference.db` 5 / 5 / 5, all `reference`; `10-resumed.db` `ScalarModel` 5
   `resumed-a`, `AdiabaticHistory` *k* `resumed-a` + 5 − *k* `resumed-b`, `BBNData` 5
   `resumed-b`; *k* agrees with `resumed-b`'s adiabatic compute count; no unvalidated row left.
5. **E2, the prune.** Every prune report, read in full in the committed record and against the
   log's table of what `resumed-a` kept. Each names only rows unvalidated in `resumed-a`'s store.
   If none printed, the agent says so, and why (nothing unvalidated was kept).
6. **E3, the criterion.** Re-run the four comparisons into the scratchpad and compare the `RESULT:`
   line, exit code, text and JSON with the committed `out/10-compare-*`. Each lists all 27 client
   tables with R0's counts (5 histories; 42,617 `ScalarModelValue` and `AdiabaticHistoryValue`
   rows; 7,475 `BBNDataValue`; 21,250 `redshift`); the bookkeeping differences are only
   `timestamp`, `label` and the timings; the only note is `LambdaCDM`'s. **Then, independently of
   the tool**, the orchestrator's digest script (06's review, check 6: each history resolved to its
   β, `coupling_serial` and `solver_serial` resolved) on both R2 stores against `02-reference.db`
   and `06-reference.db`, with R0's own pair as a control and a one-ulp change on a scratch copy as
   a positive control. All digests equal.
7. **E4, the readers.** `10-readers/status.txt` has steps 0–5, each exit 0; `5-compare_stores.log`
   is `RESULT: equal` with no science difference; `histories.csv`'s SHA-256 is `d7c62c37…c522` and
   `cmp` with R1's is silent; `4-bbn_from_store.log`'s stored and `prod` Yp and D/H are R0's. Both
   inventory logs: exit 0, `problems: none`, 21 classes, 5 validated of each compute target,
   version `2026.6.0`, none unvalidated. The orchestrator re-runs `main.py --inventory` on both R2
   stores and diffs with the committed logs (the path lines aside).
8. **E5, the schema.** Re-run `--snapshot` of `10-reference.db` and diff with
   `out/10-reference-schema.json`; that, with `out/08-standin-schema.json` and
   `out/02-reference-schema.json`: equal but for `source`.
9. **E5, L4.** Re-extract the pool's lines from `06-reference.log` and `10-reference.log`, and from
   the two `resumed-b` logs, with the orchestrator's own scratch script (banners,
   `ALL WORK ITEMS COMPLETE`, `Queue summary`, any progress pair; numbers, times and rate units
   replaced). Compare with `out/10-printed-lines.txt` and the agent's conclusion. The counts against
   R1's: `reference` 5 / 5 / 5; `resumed-b` scalar lookups 5 with compute 0, adiabatic lookups equal
   to its computes.
10. **E6, H1.** Re-run `shard_key_audit` on both R2 stores: `VERDICT: OK`, exit 0. No
    `10-reference-copy*` file exists; the log's two lists are equal, 17 paths each, every one
    `10-reference-copy*`; the copy's shard records (from the log) bare. `10-reference.db`'s 17
    files are unchanged by H1 (their mtimes against the log's record after the readers).
11. **E7, hygiene.** The 376 rehearsal entries of §1 (every `02-*` and `06-*` file, both readers'
    directories, `logs/`) have unchanged mtimes and sizes, but the root and `logs/` directories.
    The root now holds, besides them, the two 10 stores (17 files each), `logs/10-*` (three logs,
    three pidfiles) and `10-readers/`, and nothing else. The 53 user stores and pilot logs
    unchanged. `pip freeze` equals §1's copy; `pip check` clean. The siblings at their §1 `HEAD`s,
    clean (a move by another session's commit is recorded, not a finding). No scratch file in the
    repository.
12. **E8, the records and the suites.**
    - Verification §3 R2 in §2's shape: the cluster and the load; the builds; L1; L2; L3; the
      criterion; L4; H1; the commands verbatim, with the address and `S`.
    - The board: the §1 row (✅, "this commit", the log), L1–L4 (R2), V3, H1; gates R2 and
      DatastoreKit G3 in §2; `### 5.15 After prompt 10 (<date>)`. Any §3 entry, with the index's row,
      count and date in the same commit.
    - The G3 record's text in the log's "State handed to the next prompt": names the 9 layer files
      gone (two emptied), 0 imports by the package's script, the pin, the three roots' counts, and
      R2's result; it is a draft for 11, not a commit in DatastoreKit.
    - The suites as the agent ran them: 18, 114 (61 + 53), 97, with no cluster up. The orchestrator
      re-runs `Datastore/tests` at the agent's commit: 97 OK.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 10* paragraph in §5.15: what each
  check found, the re-run comparisons and the independent bit check, and anything handed on. Fix
  small residue in a follow-up commit of its own, by pathspec:
  - "this commit" → the SHA (board and log);
  - this note added to the board's list of orchestrator notes, marked "used for 10";
  - the status line: 11 landed; R2 holds and DatastoreKit G3 holds (or not).
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - R2's answer in one sentence: whether both R2 stores equal R1's and R0's bitwise, and the
    independent check;
  - that the package's actors and broker ran under Ray 2.53.0 with ChamPBH's registry of instances;
  - the builds' times under the measured load, and the cluster's start and stop;
  - the resume's *k*, the prune, and what `resumed-b` recomputed (and whether the resume was
    partial this time);
  - the readers, the two inventories, `histories.csv`, and the Yp/D/H re-solve;
  - H1;
  - whether DatastoreKit G3 holds, and the record's text for DatastoreKit's board (P16: the user's
    commit, drafted by 11);
  - anything opened;
  - that 11 can now be written (README §2.1).
- **Then 11 is written** (README §2.1), in a planning commit of its own; its note comes after.
