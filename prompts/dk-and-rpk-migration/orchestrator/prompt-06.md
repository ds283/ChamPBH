# Orchestrator — prompt 06, rehearse before the DatastoreKit swap (R1)

Read [`../README.md`](../README.md) first: §2.2 (06's charter), §3, §4.1 (as amended for U5),
§4.2, §4.3, §5 (rules 8, 9, 14, 15, 16) and §6.6. The conventions are those of
[`prompt-01.md`](prompt-01.md) §0 ("Conventions"), as [`prompt-02.md`](prompt-02.md) adapted them
for a rehearsal, and are not repeated here except where 06 changes them. 02's note is R0's, and
this note follows it; 05a's review (board §5.9) hands on the script.

**You do not write code.** You may:
- run the suites, `compare_stores.py`, `stage_counts.py` and `--snapshot` on the agent's stores;
- read the agent's stores and R0's with `sqlite3` in `mode=ro`, never through the layer;
- run `ray_cluster.sh status`;
- fix small residue in a follow-up commit of your own (§4).

**You never run `ray stop`**, and you start no cluster: the review needs none.

**The prompt:** [`06-rehearse-before-the-datastore-swap.md`](../06-rehearse-before-the-datastore-swap.md)
**Board items:** L1–L4 (R1), V2; gate R1 · **Closes:** nothing · **Narrows:** nothing ·
**Opens:** only what the work finds · **Model:** Opus, as the prompt recommends · **No network.**

**Gate:**
- 05 landed (`8e77578`) and was reviewed (`7ed8546`, residue `e513e8c`); 05a landed (`babd9d5`)
  and was reviewed (`ca2779d`); the script's usage and argument guard were corrected as
  housekeeping (`288b385`); SI's handover (`6ca907e`) and U6 (`a4894ea`) are records only.
- `HEAD` is the commit that adds this note, or a later one that touches no code ("code" as
  prompt-01.md's gate defines it, and `.documents/dk-and-rpk-migration/rehearsal/`).
  `git log --oneline babd9d5..HEAD -- . ':!prompts'` names only `288b385`.
- `ray_cluster.sh status` shows the lock free, no ChamPBH process and the four ports free (the
  prompt's gate). No `06-*` entry exists under the rehearsal root. If either fails at dispatch,
  stop and ask the user.
- One prompt at a time in this checkout.

## 0. What makes this prompt unusual

**It is R1, and its criterion is the strictest the campaign has: bitwise equality with
`02-reference.db`.** R0 was bitwise reproducible (board §5.3), so a single differing bit in a
science column is a miss, and no tolerance is chosen. A vacuous "equal" (the wrong pair compared,
a store the build never wrote, a table skipped) would let 03–05 through unmeasured, and then 08's
swap would be measured against a baseline no one checked. So the review re-runs the three
comparisons itself, and checks the stored bits of every history independently of the tool, as
02's review did.

**It is the first run of `raypoolkit` under real Ray, and the first rehearsal under the lock.**
Two things changed between R0 and R1 that a test could not show: the pool, at Ray 2.53.0 (which
neither package's suite runs), and the cluster, now on `/tmp/ray-cpbh` and port 6380 rather than
Ray's defaults. Neither should move a stored value. Both are visible in what the logs print, and
L4's printed-line comparison is the check on the first.

**What changed in the pool, read by the orchestrator** (`diff -u` of `RayTools/RayWorkPool.py` at
`a72ca5e`, R0's tree, against the installed `raypoolkit/RayWorkPool.py`, which equals the tag):
- in-flight work is keyed by `ref.hex()`, a string, where RayTools keyed it by the bound method
  `ref.hex`;
- the compute result's `obj.store()` is now the default `store_handler`, and `pool.object_store(obj)`
  the default `persist_handler`. The order is RayTools' order. ChamPBH's three compute sites
  (`main.py:284`, `:543`, `:830`) take both defaults;
- the `post_handler` is called on the object the store returned, not the original. **No ChamPBH
  site passes a `post_handler`**, and every compute site passes a `validation_handler`;
- the "available" rate's bookkeeping is corrected. **No ChamPBH site passes an
  `available_handler`**;
- the constructor validates its arguments (all 17 sites construct: log 04);
- `format_time` (`raypoolkit/_timing.py`) is byte-for-byte `utilities.format_time`;
- the progress and summary lines' text is unchanged. The `store` column is now shown when a
  persist handler is set, which, for these sites, is exactly when RayTools showed it.

So the expected printed lines are R0's, number for number in their shapes.

**The progress lines may be absent.** `MIN_NOTIFY_INTERVAL` is 300 s (`main.py:81`), passed to
every compute site. At R0 only the scalar stage (5 min 24.7 s, under a load near 119) printed a
progress pair, at 5 min 17.4 s. If 06's scalar stage ends inside 300 s, 06's logs hold no progress
line, and "every progress-line shape in 06's log occurs in 02's" holds vacuously. That is not a
finding; the log says so plainly, and the banners, completion and queue-summary lines are then the
whole of the comparison.

**The resume is only a test if it is partial.** As at R0: `S` comes from `06-reference.db`'s own
`AdiabaticHistory` timestamps (verification §1.2 and §1.8 say how). If `resumed-a` stored **0 or
5** `AdiabaticHistory` rows, the resume did not test a partial stage. That is **not** a stop: the
agent records it plainly, finishes the protocol, and says so in its reply. The orchestrator takes
it to the user (a repeat would need store names the prompt does not give).

**The cluster's stop kills by pattern.** `ray_cluster.sh stop` signals every process whose command
line names `/tmp/ray-cpbh`, and the Ray client server on 6380, and their descendants. The
drivers' and `build.sh`'s command lines name neither, so a build running when `stop` is called
is not stopped by it but loses its cluster under it: **the agent never calls `stop` while a build
or reader runs**,
except after a §4 stop condition, and then it says so. A shell or background task of the agent's
that names `/tmp/ray-cpbh` (a `tail -f` of a Ray log, an `ls` loop) would be killed, so none is
left running across a stop, and **no two Bash calls run in parallel while a `stop` runs**.

**The facts, checked by the orchestrator on 2026-10-12 at `a4894ea`.** Pass them on.
- **The tree.** Branch `dk-and-rpk-migration`, `HEAD` `a4894ea` before this note. Untracked: the
  user's 17 `pilot-2026.6.0*.db` files, `pilot-main.log`, `pilot-plot.log`, `pilot-out/`,
  `full_run_2026.6.0.sh`, `full_run_2026.6.0-refresh.sh` and `prompts/jordan-normalization/`.
  **Nothing is staged** (`git diff --cached` is empty; the `phiref_iter.py` that was staged during
  05a's dispatch is untracked again, and is not this prompt's). None is the agent's. The agent
  still commits by pathspec (below), so that anything staged by another session meanwhile is left
  where it is.
- **Code since R0's tree** (`git diff --stat a72ca5e HEAD -- . ':!prompts' ':!.documents'`):
  exactly 03–05's: `requirements.txt` (+1, the pin), `RayTools/` deleted, one import line in each
  of the three drivers, `config/datastore.py`, `Datastore/SQL/{Datastore,ClientPool}.py`,
  `main.py`'s seven vectorized gets, and `Datastore/tests/test_datastore_config.py`. Under
  `.documents/`, only 05a's `ray_cluster.sh` was added. **`build.sh`, `interrupt_when.py`,
  `stage_counts.py`, `run_readers.sh`, `tools/compare_stores.py` and `utilities.py` are as at
  `d7791c5`**, the scripts R0 ran.
- **Ray, the lock, the ports**, at 16:25. `ray_cluster.sh status` exits 0: the lock free
  (`~/ScienceData/.rehearsal-lock` absent), no ChamPBH process, 6380 / 6381 / 8267 / 52367 free.
  No Ray process at all on the machine (`ps -axww`, filtered for
  `gcs_server|raylet|ray::|ray.util.client|dashboard|log_monitor|ray-cpbh`), and Ray's default
  ports 6379 / 10001 / 8265 / 52365 free. `~/ScienceData` holds only `ChamPBH-rehearsals/`.
- **The temp dirs.** `/tmp/ray`, `/tmp/ray-cpbh`, `/tmp/ray-cpbh-decoy`, `/tmp/ray-decoy`,
  `/tmp/ray-si` exist from earlier runs. `/tmp/ray-cpbh/ray_current_cluster` exists, stale from
  05a (`127.0.0.1:6380`); `start` rewrites it. `/tmp/ray/ray_current_cluster` does not exist.
- **The venv.** 100 lines of `pip freeze`; `ray==2.53.0`; the one
  `raypoolkit @ git+https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976`
  line; no `datastorekit`; `pip check` clean. From the root, under `./venv/bin/python -I` with the
  repository on `sys.path`, `utilities.installed_package_provenance()` gives
  `{'datastorekit': None, 'raypoolkit': {'version': '0.2.0', 'url': 'https://github.com/ds283/RayPoolKit', 'commit': 'eaef0ce60e2fc354bb32f0ad576010e6a83c1976', 'requested_revision': 'v0.2.0', 'editable': False}}`,
  and the formatted line is exactly the prompt's L1 line. Every build log and the three Ray readers'
  logs should begin with it. 06 installs nothing.
- **The machine.** 10 CPUs. Load **28.5 / 22.5 / 20.7 at 16:25**, from macOS daemons (`suggestd`,
  `launchd`, `runningboardd`, a FindMy widget, Spotlight's `mdworker_shared`), not from any Ray
  job or suite. One other Claude session was busy at dispatch (a DatastoreKit session); it may run
  DatastoreKit's suite. R0's result held under exactly that (verification §1.1). The prompt says
  record the load and any foreign suite, and continue.
- **The rehearsal root** `~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/` holds R0's two
  stores (17 files each), `logs/` (three `02-*.log` and three `.pid`) and `02-readers/`, and no
  `06-*` entry. **Nothing named `02-*` is opened except by `compare_stores.py` and `sqlite3`
  `mode=ro`**, and nothing in the root is deleted, moved or renamed, even after a stop.
- **What R0 measured**, for L4 and for choosing `S` (verification §1):
  - `reference` 10 min 43 s, stages 5 min 24.7 s · 12.6 s · 1 min 8.73 s, at loads up to 119;
  - `S` = 5 s: the five `AdiabaticHistory` rows fell at banner + 4.6, 4.9, 5.6, 6.4 and 7.6 s, the
    first 4.6 s being the reload of the five `ScalarModel`s. `interrupt_when.py` polls once a
    second, so the signal lands at banner + `S` to `S` + 1 s;
  - `resumed-a` exited **134**; it kept 3 validated rows and 1 unvalidated (β = 10), which
    `resumed-b`'s open pruned;
  - `resumed-b` computed 0 / 2 / 5, with lookups **5** / 2 / 5 (the scalar stage looks up every
    model again: board §3, `[02-scalar-stage-missing-filter-keeps-every-model]`, unchanged at this
    tree; the adiabatic stage's lookups equal its computes);
  - all 17 actors of the killed job (16 `shardNNNN-store` and one `SerialPoolBroker`) were dead
    28 s after the signal.
- **The actor check** now needs the address: `./venv/bin/ray list actors --address 127.0.0.1:6380
  --filter "state=ALIVE"` (the form `start` prints, plus the filter), and for the dead ones
  `--limit 200`, and `./venv/bin/ray list jobs --address 127.0.0.1:6380`. The bare form finds no
  cluster here, or the wrong one if another project's is up.
- **What the drivers print that is not a failure** (as at R0): Ray's `FutureWarning` about
  `RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO`; the three `!! warning:` super-Planckian lines; the
  `@@ Parametrized equation-of-state …` line from each `Datastore` actor, deduplicated by Ray
  (`[repeated 15x across cluster]`); in the plot scripts, `!! WARNING: number of shards read from
  database (=16) does not match specified number of shards (=10)`; and, under load,
  `(raylet) … have not registered within the timeout`. The killed driver's absl dump
  (`*** SIGTERM received …`, a native stack, twice) contains neither `Traceback` nor `Error`.
  Anything else containing `Traceback` or `Error` is read in full and recorded.
- **The connect line changes**, and is expected to: R0's drivers logged `Connecting to existing Ray
  cluster at address: 127.0.0.1:6379` and the dashboard at `http://127.0.0.1:8265`; 06's should
  name `127.0.0.1:6380` and `8267`. A driver that names 6379 attached to the wrong cluster: that is
  a stop.
- **The suites at `babd9d5`**: CosmologyModels 18, ComputeTargets 114 (61 with
  `-p 'test_[a-f]*.py'`, 53 with `-p 'test_[g-z]*.py'`), Datastore 73. No code changed since but
  the shell script. The agent runs them **before `start`**, never with a cluster up.
- **`.documents/OPEN_ISSUES.md`** has **26 open**, 3 on this board, last updated 2026-10-12. An
  issue 06 opens goes under `### 1.11 dk-and-rpk-migration`, with the count and the header's prose
  corrected in the same commit.
- **The siblings.** SI at `5fb4a1d`, SGK at `a5a6f6d` (branch `handover-remedial`), DatastoreKit
  at `5f972be`, RayPoolKit at `1b84df0`; each tree clean. 06 reads RayPoolKit's README with
  `git -C … show v0.2.0:README.md` and writes nothing in any of them.
- **Expected duration.** About 10–15 minutes for `reference`, 8 for `resumed-a`, 2 for
  `resumed-b`, 2 for the readers, plus the suites (about 8 minutes) and the start and stop: about
  an hour in all.

**What the review exists to establish.**
- **(E1) The builds.** Each log has the provenance line naming `raypoolkit` 0.2.0 at `eaef0ce6…`,
  the connect line naming 6380, the banners, the summaries and `** driver exit status`, as L1 says.
  `resumed-a` died by the signal in the adiabatic stage, and its actors were gone before
  `resumed-b`.
- **(E2) The resume.** By the stage counts and by the labels in `06-resumed.db`: `resumed-b`
  computed exactly what `resumed-a` had not kept; nothing was pruned but unvalidated rows.
- **(E3) The criterion.** The three comparisons, re-run by the orchestrator, give the agent's
  results, and the stored bits of each history in `06-reference.db` and `06-resumed.db` equal
  `02-reference.db`'s by the orchestrator's own script.
- **(E4) The readers.** All six steps exit 0; the before/after comparison is empty but for
  bookkeeping; `histories.csv` is byte-identical to R0's; the β = 1.15 re-solve is R0's.
- **(E5) L4.** R0's counts and resume pattern; the printed lines in R0's layout, re-extracted by
  the orchestrator.
- **(E6) The cluster and hygiene.** Started and stopped by the script alone, under the lock, never
  by `ray stop`; nothing left running, no lock, the ports free; R0's files and every other store
  untouched; the venv unchanged; no code changed.
- **(E7) The records.** Verification §2 R1 in §1's shape, §1 unedited; `out/06-*`; the board; the
  log; the suites.

**Conventions particular to 06.** prompt-01.md's conventions hold, with 02's for a rehearsal,
except that **the cluster is started and stopped by `ray_cluster.sh` only**. In addition:
- **git.** The only commit is the agent's one, at the end, **by pathspec**: `git add <its files>`,
  show `git diff --cached --name-only`, then `git commit -F <msg> -- <its files>`. No `checkout`,
  `switch`, `stash`, `reset`, `restore --staged` or `rebase`. If anything not the agent's appears
  in `git diff --cached`, it says so and leaves it staged.
- **Ray.** `ray_cluster.sh start "dk-and-rpk-migration 06 (R1)"` once; every driver gets
  `--ray-address 127.0.0.1:6380` (through `build.sh` and `run_readers.sh`), and every `ray` CLI
  call `--address 127.0.0.1:6380`. `stop` as a command of its own, then `status` in another.
  **Never `ray stop`.** Exit 3 or 4 from `start`, or a non-zero `stop`, is a stop: report with
  `status`'s output, and do not clean up by hand.
- **Background tasks.** Each `build.sh` and `run_readers.sh` runs as a background Bash task, so the
  agent is notified when it ends; `interrupt_when.py` runs as a second background task beside
  `resumed-a`'s. If the harness does not notify, wait with the `Monitor` tool on the log's
  `** driver exit status` line, **never with a loop of `sleep` and reads**. None of these commands
  names `/tmp/ray-cpbh`.
- **Stores.** The agent opens only `06-reference.db`, `06-resumed.db`, `06-readers/before/`, and
  R0's two stores read-only through `compare_stores.py` (and `sqlite3` `mode=ro` if it wants R0's
  figures). Not the pilot store, not `~/ChamPBH-stores/`, not `~/ChamPBH-stores-bt02/`. It
  deletes nothing under the rehearsal root, even after a stop.
- **Outputs committed** are small text only, under `.documents/dk-and-rpk-migration/out/06-*`. A
  JSON file over about 1 MB is a finding to report before committing. Name the three comparisons'
  files `06-compare-02-reference-06-reference`, `06-compare-02-reference-06-resumed` and
  `06-compare-06-reference-06-resumed` (`.json` and `.txt`).
- **A miss against `02-reference.db`** is a stop *after* the readers and the records are done: the
  agent finishes §1.4 (the stop of the cluster included), writes the log and the verification
  document, and **does not commit**. It reports the full `compare_stores` output and waits.
- **Commands verbatim.** The log and the verification document quote each command as run, with
  the actual address and `S`.

## 1. Before you dispatch

1. **The tree.** Record the branch and `HEAD`. `git status --short` shows only §0's untracked
   entries, and `git diff --cached` is empty. `git log --oneline babd9d5..HEAD -- . ':!prompts'`
   names only `288b385`.
2. **The cluster.** `ray_cluster.sh status` exits 0 and shows the lock free, no process, the four
   ports free; `ps` finds no Ray process.
3. **The venv.** `pip freeze` saved in the scratchpad; `pip check` clean; the helper's output as §0.
4. **The load.** `uptime`, and the top of `ps -Ao pid,pcpu,comm -r`, recorded and passed on.
5. **Hygiene baselines**, for the review: `/usr/bin/stat -f '%m %z %N'` over every entry under the
   rehearsal root (189 entries, `02-readers/` included), and over `~/ChamPBH-stores/*.db`,
   `~/ChamPBH-stores-bt02/*.db`, `pilot-2026.6.0*.db`, `pilot-main.log` and `pilot-plot.log` (53
   files); the four siblings' `HEAD`s.

The suites are not re-run by the orchestrator at dispatch: no Python has changed since `babd9d5`,
where they were 18 / 114 / 73, and the agent runs them.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Opus**. Give it:
- the prompt;
- the campaign README and this board;
- `HEAD`, the load and §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the
  session scratchpad. The agent does not read `orchestrator/`, and does not read prompts 07 onward
  (none is written).

Tell it plainly:
- **One commit, at the end, by pathspec.** Its log is `logs/06-rehearse-before-the-datastore-swap.md`,
  in README §5.1's form, with the prompt's §6 additions.
- **The files it may add or change** are the prompt's §5 list (the verification document's new §2,
  `.documents/dk-and-rpk-migration/out/06-*`, the log, the board) and `.documents/OPEN_ISSUES.md`
  if an issue opens. **No code, no script of 01's or 05a's, no tool, nothing in a sibling, nothing
  in the venv.**
- **It runs the protocol as written,** waits on each build by notification, and keeps the suites
  away from the cluster.
- **Never `ray stop`.** No parallel Bash calls while a `stop` runs; no `stop` while a build runs.
- **Stop and ask** on any of the prompt's §4 conditions, after `ray_cluster.sh stop` (unless the
  stop is itself what failed), with `status`'s output and the logs' paths.
- **It replies with:**
  - the log's path and the commit; `git diff --cached --name-only` before it and
    `git show --stat HEAD` after it;
  - the load, the cluster's start and stop (each command's output, its timing, the owner file),
    and `ray status --address`;
  - each build's start, end, wall time and exit status, and the connect line;
  - `S`, and the timestamps it was chosen from;
  - the actor check, with the job ids;
  - the stage counts, the label counts, and what `resumed-a` kept;
  - each prune report;
  - the readers' statuses, the inventory, the Yp/D/H comparison and `histories.csv`'s SHA-256;
  - each comparison's `RESULT:` line and exit code, and its full text if not 0;
  - the schema comparison;
  - the printed-lines comparison;
  - the three suites, with the load;
  - every deviation with its tag;
  - the "State handed to the next prompt" section, verbatim.

## 3. The review — eleven checks

1. **Scope.** At the agent's commit, `git show --stat HEAD` touches only the verification
   document, `out/06-*`, `logs/06-…`, the board and, if an issue opened, the index.
   `git diff HEAD~1 HEAD -- .documents/dk-and-rpk-migration-verification.md` adds lines after §1
   and changes none of §1's (invariant 6). `git status` shows §1's untracked set and nothing new;
   no leftover branch; no other commit of the agent's.
2. **The cluster.** `ray_cluster.sh status`: lock free, no process, ports free; `ps` finds no Ray
   process of ChamPBH's. The log shows `start`'s output (the address, the actor line, the owner
   file with purpose `dk-and-rpk-migration 06 (R1)`), `ray status --address` with 10 CPUs, `stop`
   exiting 0 with its timing, and `status` after it. `grep -n 'ray stop'` in the log finds no
   invocation.
3. **E1, the logs.** Read all three build logs in full, and the readers' logs.
   - The provenance line once each, exactly L1's.
   - The connect line names `127.0.0.1:6380`; no line names 6379 or 8265.
   - The banners and summaries. `grep -n 'Traceback\|Error'` gives only §0's known lines.
   - `reference` and `resumed-b` end `** driver exit status 0`; `resumed-a` ends 134 after the
     adiabatic banner and before any BBN banner, its pid the one in `06-resumed-a.pid`.
   - `interrupt_when.py`'s output: the banner seen, `S` waited, `SIGTERM` to that pid.
4. **E2, the counts.** Re-run `stage_counts.py` on the three logs and diff with the committed
   text and JSON. Then, with the orchestrator's own `sqlite3` `mode=ro` script over every shard,
   count `ScalarModel`, `AdiabaticHistory` and `BBNData` rows by label prefix, and `validated`.
   Expected: `06-reference.db` 5 / 5 / 5, all `reference`; `06-resumed.db` `ScalarModel` 5
   `resumed-a`, `AdiabaticHistory` *k* `resumed-a` + 5 − *k* `resumed-b`, `BBNData` 5
   `resumed-b`; *k* agrees with `resumed-b`'s adiabatic compute count; no unvalidated row left.
5. **E2, the prune.** Every prune report in the three logs, read in full. Each names only rows
   that were unvalidated in `resumed-a`'s store (the log's table of what `resumed-a` kept). If none
   printed, the agent says so for each build.
6. **E3, the criterion.** Re-run the three comparisons into the scratchpad and compare the
   `RESULT:` line, exit code, text and JSON with the committed `out/06-compare-*`. Each lists all
   27 client tables with R0's counts (5 histories; 42,617 `ScalarModelValue` and
   `AdiabaticHistoryValue` rows; 7,475 `BBNDataValue`; 21,250 `redshift`), and its bookkeeping
   differences are only `timestamp`, `label` and the timings. **Then, independently of the tool**,
   the orchestrator's script resolves each row to its β and compares stored bits against
   `02-reference.db`, for both 06 stores: each history's `ScalarModelValue` rows (10 float
   columns, ordered by `z`, hashed), its `AdiabaticHistoryValue` rows, its `ScalarModel` and
   `AdiabaticHistory` science columns, and its `BBNData` abundances, `failure`, `small_network`
   and `PRyM_version`. All five histories equal on all of them.
7. **E4, the readers.** `06-readers/status.txt` has steps 0–5, each exit 0; `5-compare_stores.log`
   is `RESULT: equal` with no science difference; `histories.csv`'s SHA-256 is `d7c62c37…c522` and
   `cmp` with R0's is silent; `4-bbn_from_store.log`'s stored and `prod` Yp and D/H are R0's;
   `1-inventory.log` lists 5 validated rows of each kind, version `2026.6.0`.
8. **E5, the schema.** Re-run `--snapshot` of `06-reference.db` and diff with
   `out/06-reference-schema.json`, and that with `out/02-reference-schema.json`: identical, or
   the difference is the agent's stop.
9. **E5, L4.** Re-extract the pool's lines from `02-reference.log` and `06-reference.log` with a
   scratch script of the orchestrator's own: the banners, `ALL WORK ITEMS COMPLETE` and
   `Queue summary` lines, and any progress pair, with every number, time and rate unit replaced by
   a placeholder. Compare with the committed `out/06-printed-lines.txt` and with the agent's
   conclusion: the banners and completion lines agree in text and order, and every progress shape
   in 06's log occurs in 02's (or 06 has none, said plainly). The counts against R0's: `reference`
   5 / 5 / 5; `resumed-b` scalar lookups 5 with compute 0, adiabatic lookups equal to its computes.
10. **E6, hygiene.** The 189 rehearsal entries of §1 (every `02-*` file, `logs/02-*`,
    `02-readers/`) have unchanged mtimes and sizes; the 53 stores and pilot logs too. The root now
    holds, besides them, the two 06 stores (17 files each), `logs/06-*` (three logs, three
    pidfiles) and `06-readers/`. `pip freeze` equals §1's copy; `pip check` clean. The siblings at
    their §1 `HEAD`s, clean (a sibling moved by another session's commit is recorded, not a
    finding, if nothing in it is the agent's). No scratch file in the repository.
11. **E7, the records and the suites.**
    - The verification document's §2 R1 has §1's shape: the cluster and the load; the builds; L1;
      L2; L3; the criterion; L4; the commands verbatim, with the address and `S`.
    - The board: the §1 row (✅, "this commit", the log), L1–L4 (R1) and V2, gate R1 in §2, and
      `### 5.10 After prompt 06 (<date>)`. Any §3 entry, with the index's row, count and date in
      the same commit.
    - The suites as the agent ran them: 18, 114 (61 + 53), 73, with no cluster up. The
      orchestrator re-runs `Datastore/tests` at the agent's commit: 73 OK.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 06* paragraph in §5.10: what
  each check found, the re-run comparisons and the independent bit check, and anything handed on.
  Fix small residue in a follow-up commit of its own, also by pathspec:
  - "this commit" → the SHA (board and log);
  - this note added to the board's list of orchestrator notes, marked "used for 06";
  - the status line: 7 landed; R1 holds (or not).
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - R1's answer in one sentence: whether both 06 stores equal `02-reference.db` bitwise, and the
    independent check;
  - the builds' times under the measured load, and the cluster's start and stop;
  - the resume's *k*, the prune, and what `resumed-b` recomputed;
  - the readers, `histories.csv`, and the Yp/D/H re-solve;
  - the printed lines, and whether the progress comparison had anything to compare;
  - anything opened;
  - that 07–10 can now be written (README §2.1), with U6's `v0.3.0` as the pin.
- **Then 07–10 are written** (README §2.1), in a planning commit of their own; their notes come
  one at a time, after.
