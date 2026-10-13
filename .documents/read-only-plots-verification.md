# read-only-plots — rehearsal verification

**Campaign:** [`prompts/read-only-plots/`](../prompts/read-only-plots/README.md) ·
**Board:** [`IMPLEMENTATION_STATE.md`](../prompts/read-only-plots/IMPLEMENTATION_STATE.md) ·
**Outputs:** [`read-only-plots/out/`](read-only-plots/out/) ·
**Scripts:** [`dk-and-rpk-migration/rehearsal/`](dk-and-rpk-migration/rehearsal/), run unchanged

This document records the campaign's one rehearsal (prompt 03, by the user's ruling U5). It is
`dk-and-rpk-migration`'s protocol (that README §4.1), cut down to one build, no resume, the five
readers and three refusals. It shows that under real Ray, on a store `main.py` built, both plot
scripts and `main.py --inventory` run to completion, draw what they drew before, and change no byte
of any file of the store. It also shows 01's refusal of `--ray-address auto` and 02's stop on an
unstored option, each once under real Ray.

It is additive (`CLAUDE.md` invariant 6). A re-run that supersedes a figure gets a new dated
subsection, and the figures below stay as they are, correct for the tree they were taken on.

The store, its logs and the reader outputs live outside the repository, under
`~/ScienceData/ChamPBH-rehearsals/read-only-plots/`, written `$R` below (README §3). They are kept;
removing them is the user's. Only small text outputs are committed, under `read-only-plots/out/03-*`.
R0–R2's stores were removed by the user after `dk-and-rpk-migration` closed, so every comparison
with R2 below is against R2's **recorded** figures
([`dk-and-rpk-migration-verification.md`](dk-and-rpk-migration-verification.md) §3 and
[`dk-and-rpk-migration/out/10-readers.txt`](dk-and-rpk-migration/out/10-readers.txt)).

---

## 1. The rehearsal (prompt 03, 2026-10-13)

**Tree:** `27f77d4`, whose code is `9dc23db`'s (02's): `27f77d4` is the orchestrator's note and
changes no code. `datastorekit` 0.3.0 at `2f5417ea…` and `raypoolkit` 0.2.0 at `eaef0ce6…`, neither
editable; Ray 2.53.0; the venv's `pip freeze` 101 lines, identical before and after the rehearsal.
The rehearsal scripts (`ray_cluster.sh`, `build.sh`, `run_readers.sh`, `stage_counts.py`) ran
unchanged, with `REHEARSAL_ROOT=$R`.

**Result: the gate "R" holds. A real plotting run on a real store changes no file of it.**
- The build exited 0, with 5 / 5 / 5 / 5 in each of its three stages.
- All five readers exited 0. The store's 17 files are byte-identical, with their mtimes and sizes,
  before the readers, after them, and after the three refusals. No `-journal`, `-wal` or `-shm` file
  appeared beside any of them.
- `compare_stores` of the copy made before the readers against the store gives `RESULT: equal`,
  with no bookkeeping column differing.
- `histories.csv` is R0's, R1's and R2's, byte for byte by SHA-256. The SM baseline, the β = 1.15
  BBN figures, the inventory and the stage counts are R2's.
- Both plot scripts printed the **read-only** open line, once each, and never the read-write one.
- The three refusals each exited 2 with their messages, the `T_stop` one through a real
  `Datastore` actor and `ray.get`, before the baseline and before any figure.

### 1.1 The cluster and the load

- 10 CPUs, host `mps026088`.
- **The load.** At the gate the top of the load was macOS daemons and other Claude sessions; no Ray
  process was on the machine. During the build the load is the build's own (its Ray workers), as at
  R2; it peaked at 100 just as the build ended (R2: 63):

  | When (BST) | Load average (1 / 5 / 15 min) |
  |---|---|
  | 22:23:21, the gate | 7.54 / 14.58 / 11.63 |
  | 22:23:28 – 22:29:11, the suites | 6.68 → 2.81 (1 min) |
  | 22:29:18, before `start` | 2.93 / 6.59 / 8.76 |
  | 22:29:39, the build starts | 2.72 / 6.31 / 8.61 |
  | 22:33:20, the build ends | 100.49 / 88.94 / 46.80 |
  | 22:33:43, the readers start | 67.88 / 82.19 / 45.58 |
  | 22:34:55, the readers end | 51.38 / 74.66 / 45.64 |
  | 22:35:35, the `T_stop` refusal | 32.70 / 66.98 / 44.16 |
  | 22:36:13, after the refusals | 22.75 / 60.32 / 42.78 |
  | 22:36:45, after the stop | 15.53 / 54.97 / 41.45 |

- **The cluster**, started at 22:29:18 from the repository root:
  `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh start "read-only-plots 03 (R)"`, 6 s
  wall, ending
  ```text
  >> ray_cluster: address 127.0.0.1:6380
  >> ray_cluster: dashboard http://127.0.0.1:8267
  >> ray_cluster: actors: ./venv/bin/ray list actors --address 127.0.0.1:6380
  >> ray_cluster: started in 6s; gcs_server pid 89356
  ```
  Its exit status was not captured: the command was piped through `tee` in zsh, where
  `${PIPESTATUS[0]}` is unset (log 03, deviation 1). The script's only path that prints `started
  in` returns 0 on the next line (`ray_cluster.sh:336-337`), and `status` straight after it showed
  the lock held by this purpose and the cluster up.
  The lock's owner file: `project: ChamPBH`, `purpose: read-only-plots 03 (R)`,
  `acquired: 2026-10-13T22:29:24+01:00`, `host: mps026088`, `ray_temp_dir: /tmp/ray-cpbh`,
  `ray_address: 127.0.0.1:6380`, `pid: 89356`. `ray_cluster.sh status`, exit 0: 18 ChamPBH
  processes, the four ports held. `./venv/bin/ray status --address 127.0.0.1:6380`, exit 0: one
  active node, `0.0/10.0 CPU`, 8.20 GiB memory, 2.00 GiB object store, no failures.
  `/tmp/ray-cpbh/ray_current_cluster` held `127.0.0.1:6380`; `/tmp/ray/ray_current_cluster` did
  not exist, before or after.
- **Every driver that started Ray logged `Connecting to existing Ray cluster at address:
  127.0.0.1:6380` and the dashboard at `http://127.0.0.1:8267`**: the build, both plot scripts and
  the `T_stop` refusal. No log names 6379.
- **Jobs and actors**, at the end (22:36:13), before the stop: `./venv/bin/ray list jobs --address
  127.0.0.1:6380` lists four jobs, all `SUCCEEDED`: `01000000` the build, `02000000`
  `plot_by_beta.py`, `03000000` `plot_ScalarModel.py`, `04000000` the `T_stop` refusal. The `auto`
  and `--drop` refusals added none: the count was 3 before and after the `auto` refusal, and 4
  before and after the `--drop` one. `--filter "state=ALIVE"`: `No resource in the cluster`. Of 65
  actors, all `DEAD`: 16 `Datastore` and one `SerialPoolBroker` for the build, and 16 `Datastore`
  and **no broker** for each of the three read-only jobs (R2: 17 per job, a broker each). The
  read-only pool builds its actors with `serial_broker=None` and no broker
  (`datastorekit/SQL/ShardedPool.py:563` at `v0.3.0`).
- **The stop**, at 22:36:22, with no build, reader, monitor or background task running:
  `ray_cluster.sh stop`, **exit 0, 18 s**: `SIGTERM` to the 18 processes naming `/tmp/ray-cpbh`
  and their descendants, the 15 s grace, `SIGKILL` to 89356 (`gcs_server`) and 89357; "no process
  names /tmp/ray-cpbh; ports 6380 6381 8267 52367 are free"; "released the rehearsal lock". Then
  `ray_cluster.sh status`, exit 0: the lock free, no ChamPBH process, the four ports free.
  `ray stop` was never run. `/tmp/ray/session_*` was 160 before and after.

Output: [`03-cluster.txt`](read-only-plots/out/03-cluster.txt).

### 1.2 The build (H1)

`REHEARSAL_ROOT=$R .documents/dk-and-rpk-migration/rehearsal/build.sh 03 reference reference
127.0.0.1:6380`, as a background task, waited on by its notification and a `Monitor` on its log's
stage and `** driver exit status` lines. The job name reads `dk-and-rpk-migration-03-reference`,
because `build.sh` runs unchanged; the store is `$R/03-reference.db` with 16 shards.

| Build | Started (BST) | Ended | Wall | Exit | Stages' times | R2 `reference` |
|---|---|---|---|---|---|---|
| `reference` | 22:29:39 | 22:33:20 | 3 min 41 s | **0** | 2m 12s · 3.59s · 24.1s | 3 min 33 s; 1m 54.6s · 4.35s · 21.2s |

Driver pid 89899. `stage_counts.py` ([`03-stage-counts.txt`](read-only-plots/out/03-stage-counts.txt)):
**lookup 5, compute 5, store 5, validation 5** in each of the scalar, adiabatic and BBN stages, all
`complete`, 0 skipped, `driver exit status: 0`: R2's `reference` exactly.

- **The provenance line** is the log's first line, exactly once (`grep -cxF`):
  `>> packages: datastorekit 0.3.0 (https://github.com/ds283/DatastoreKit@2f5417ea76cdcd0d15d64d80d9085429bfacad21, requested v0.3.0); raypoolkit 0.2.0 (https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976, requested v0.2.0)`,
  R2's. Before the cluster started, `utilities.installed_package_provenance()` gave both entries at
  those commits and `requested_revision`s, with `editable: False`.
- `>> Created sharded datastore "…/read-only-plots/03-reference.db" with 16 shards`.
- No `Traceback` and no `Error` in the log. The other unusual lines are R2's kinds: Ray's
  `FutureWarning` about `RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO`; the three super-Planckian warnings
  (β = 10 and 25, and the count); and the `Datastore` actors' `@@ Parametrized equation-of-state`
  line (`[repeated 15x across cluster]`).

Output: [`03-build.txt`](read-only-plots/out/03-build.txt).

### 1.3 The readers (H2)

`REHEARSAL_ROOT=$R .documents/dk-and-rpk-migration/rehearsal/run_readers.sh 03 reference
127.0.0.1:6380`, as a background task waited on by its notification, 22:33:43 to 22:34:55
(1 min 12 s; R2 1 min 6 s), exit 0. `run_readers.sh` does not time its steps; the times below are
from consecutive output files' mtimes, so each is approximate to a second.

| Step | Exit | Wall (approx.) |
|---|---|---|
| 0 copy-before (`cp -p` into `03-readers/before/`) | 0 | 1 s |
| 1 `main.py --inventory` | 0 | 9 s |
| 2 `plot_by_beta.py --phi-init-Mp 5` | 0 | 23 s (pool stage 5.36 s) |
| 3 `plot_ScalarModel.py` | 0 | 18 s (pool stage 8.5 s) |
| 4 `tools/bbn_from_store.py … --beta 1.15 --M-Mp 1e-3 --small-network --variant prod` | 0 | 12 s (solve 10.0 s) |
| 5 `compare_stores.py before/03-reference.db 03-reference.db` | 0 | 9 s |

- **The open line.** Each plot log has, once, and neither has the read-write line:
  ```text
  !! WARNING: number of shards read from database (=16) does not match specified number of shards (=10)
  >> Opened existing sharded datastore "/Users/ds283/ScienceData/ChamPBH-rehearsals/read-only-plots/03-reference.db" read-only with 16 shards (version label "2026.6.0", serial 1)
  ```
  The shard-count warning is R2's and expected (P4: the plot scripts pass no `shards=`).
- **`--inventory`** ([`03-inventory.txt`](read-only-plots/out/03-inventory.txt)) is R2's report:
  `read-only; 16 shards (#0, …, #15); 21,292 records in 21 classes`, **`problems: none`**;
  `ScalarModel`, `AdiabaticHistory` and `BBNData` 5 records each, 5 validated, 0 unvalidated;
  42,617 / 42,617 / 7,475 value rows; per-β value counts 4,947 / 4,975 / 5,002 / 9,776 / 17,917;
  every group `version = 2026.6.0`; every BBN record `PRyM_version=bf24c3d+ri02+sr01+bt02`.
- **`plot_by_beta.py`**: `@@ plot_by_beta: SM baseline (rho_NP = 0,
  PRyM_version=bf24c3d+ri02+sr01+bt02, small_network=True): Yp=0.24688, D/H x1e5=2.45829, 3He/H
  x1e5=1.04193, 7Li/H x1e10=5.48637`, R2's exactly; "no models used elastic reflections (5 model(s)
  checked)"; 1 lookup, no compute item evaluated. It wrote 16 files (14 figures).
- **`plot_ScalarModel.py`**: 5 lookups, no compute item evaluated (8.5 s; R2 7.69 s); 75 files
  (70 figures).
- **`bbn_from_store.py`, β = 1.15**, shard 0000, serial 1, small network:

  | | Yp | D/H (×10⁵) |
  |---|---|---|
  | stored row | 0.24687349447579363 | 2.4859924812264325 |
  | `prod` re-solve | 0.2468734945 | 2.485992481 |

  **R2's figures exactly**, stored and re-solved; He3/H 1.046802492 and Li7/H 5.403050409, R2's.
- **`compare_stores`** ([`03-compare_stores.txt`](read-only-plots/out/03-compare_stores.txt)):
  27 tables, every row matched, none only in A or B; `bookkeeping columns that differ between
  matched rows (not compared): none`; `problems: none`; **`RESULT: equal`**.
- No `Traceback` and no `Error` in any reader log; none names 6379.

Output: [`03-readers.txt`](read-only-plots/out/03-readers.txt).

### 1.4 The checksums

`shasum -a 256 03-reference*.db` and `/usr/bin/stat -f '%m %z %N' 03-reference*.db`, run in `$R`
over the 17 files (the primary and its 16 shards; the glob does not reach `03-readers/before/`):
- **before** the readers, at 22:33:39, after the build;
- **after the readers**, at 22:35:00: `cmp` of each list against the one before, **exit 0** both;
- **after the refusals**, at 22:36:13: `cmp` again, **exit 0** both.

`find $R -name '*-journal' -o -name '*-wal' -o -name '*-shm'` printed nothing, after the readers and
after the refusals. All three lists in full: [`03-checksums.txt`](read-only-plots/out/03-checksums.txt).

| File | SHA-256 (the same all three times) | Size (bytes) | mtime (epoch s) |
|---|---|---|---|
| `03-reference-shard0000.db` | `8b8d641d481746760b4615f8476009300330e866e31beb358f662bc7c7af1a20` | 3305472 | 1791927186 |
| `03-reference-shard0001.db` | `5d23aa99b846cd9e66f37808bb3a4c4957643163121723edb107f9b57987d369` | 3317760 | 1791927186 |
| `03-reference-shard0002.db` | `72c2e9d1308ae2ec072fddd7fac29e14478b82bc23996a7bb1ae7fa9af17e33c` | 4759552 | 1791927198 |
| `03-reference-shard0003.db` | `4a79811de10cb73537a61a2c28bf51ad7154df1abef1e8a2a6b3ad0c8287e69e` | 7319552 | 1791927198 |
| `03-reference-shard0004.db` | `e6aa59360c0bccfe9279a570063622c139ee251f56404db96ed312aab807e4ad` | 1748992 | 1791927038 |
| `03-reference-shard0005.db` | `0afca84a6c88800f4541792eef658b3155ab80c8722373b9f94d1bcc0ccc5270` | 1748992 | 1791927038 |
| `03-reference-shard0006.db` | `0afca84a6c88800f4541792eef658b3155ab80c8722373b9f94d1bcc0ccc5270` | 1748992 | 1791927038 |
| `03-reference-shard0007.db` | `e6aa59360c0bccfe9279a570063622c139ee251f56404db96ed312aab807e4ad` | 1748992 | 1791927038 |
| `03-reference-shard0008.db` | `0afca84a6c88800f4541792eef658b3155ab80c8722373b9f94d1bcc0ccc5270` | 1748992 | 1791927038 |
| `03-reference-shard0009.db` | `e6aa59360c0bccfe9279a570063622c139ee251f56404db96ed312aab807e4ad` | 1748992 | 1791927038 |
| `03-reference-shard0010.db` | `0afca84a6c88800f4541792eef658b3155ab80c8722373b9f94d1bcc0ccc5270` | 1748992 | 1791927038 |
| `03-reference-shard0011.db` | `0afca84a6c88800f4541792eef658b3155ab80c8722373b9f94d1bcc0ccc5270` | 1748992 | 1791927038 |
| `03-reference-shard0012.db` | `e6aa59360c0bccfe9279a570063622c139ee251f56404db96ed312aab807e4ad` | 1748992 | 1791927038 |
| `03-reference-shard0013.db` | `0afca84a6c88800f4541792eef658b3155ab80c8722373b9f94d1bcc0ccc5270` | 1748992 | 1791927038 |
| `03-reference-shard0014.db` | `0afca84a6c88800f4541792eef658b3155ab80c8722373b9f94d1bcc0ccc5270` | 1748992 | 1791927038 |
| `03-reference-shard0015.db` | `0e5d4641d20e2d18ff3121588eff6ffb36511166be402e98d7a5e6132709e329` | 3284992 | 1791927186 |
| `03-reference.db` | `2cd0a313bf52ba2fe18ce587563ef3048c2a8ba411225fedbdca6945a2812d5e` | 36864 | 1791927038 |

### 1.5 `histories.csv`

`$R/03-readers/plot_by_beta/QCD_Cosmology/histories.csv`: SHA-256
**`d7c62c37d8ad637a63854583ab11bc737109dcbfba92168dbddbaea75182c522`**, R0's, R1's and R2's
(`dk-and-rpk-migration/out/10-readers.txt:116`). R2's file no longer exists, so the comparison is by
the recorded hash, not `cmp`.

### 1.6 The refusals under real Ray (H3)

Each run from the repository root on the same store, with its output directory and log under
`$R/03-refusals/`, its exit status by `echo $?` straight after it. Logs verbatim:
[`03-refusals.txt`](read-only-plots/out/03-refusals.txt).

| Refusal | When | Exit | New Ray job | Output directory created |
|---|---|---|---|---|
| `--ray-address auto` | 22:35:27–31 | **2** | none (3 jobs before and after) | no |
| `--T-stop-GeV 1e-7 --ray-address 127.0.0.1:6380` | 22:35:35–44 | **2** | `04000000` | no |
| `--drop bbn-data --ray-address 127.0.0.1:6380` | 22:36:03–05 | **2** | none (4 before and after) | no |

- **`auto`**: after the provenance line, on stderr, before `ray.init`:
  `!! --ray-address is required whenever Ray starts. Give the address of a running cluster (ChamPBH's on this machine is 127.0.0.1:6380, started by .documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh), or 'local' for a private instance. 'auto' is refused: with the projects' clusters isolated, it attaches to whichever cluster it finds first.`
  01's message. The log has no Ray line.
- **`T_stop`**: the run attached to `127.0.0.1:6380`, printed the read-only open line, looked up
  the parameters, potentials and couplings (none skipped: no warning line), and then stopped on the
  `T_stop` lookup with
  `!! plot: the store holds no temperature for --T-stop-GeV 1e-07; main.py was not run with it, so no history in this store can match. Give the plot the options the build used`,
  02's message character for character. So the `ReadOnlyMiss` raised in a real `Datastore` actor
  reached the driver through `ray.get` and was caught by `except ReadOnlyMiss` with its
  `class_name` read through. No `@@ plot_by_beta: SM baseline` line, no `RUNNING PIPELINE` line, no
  figure, and `$R/03-refusals/T-stop-1e-7` was not created. No `Traceback` in the log, from the
  driver or forwarded from an actor. The last line is a `Datastore` actor's
  `@@ Parametrized equation-of-state` line, forwarded by Ray after the message. The job's 16
  `Datastore` actors ended `DEAD`, none `ALIVE`; it has no broker (§1.1). Ray gives the job
  `SUCCEEDED`.
- **`--drop`**: after the provenance line, on stderr, before 01's check and before Ray:
  `!! --drop: a plot run drops nothing, since it opens its store read-only; --drop is main.py's option`.
  It was given the valid address, so the refusal does not depend on a bad one.
- After the three, the 17 files' checksums, sizes and mtimes are unchanged (§1.4).

### 1.7 Nothing written outside `$R`, but by design

- `git status` after the stop shows the user's untracked set of the gate and nothing else (before
  this commit's files were written).
- `~/ScienceData` holds `ChamPBH-rehearsals/` alone after the stop: the lock was created by `start`
  and removed by `stop`.
- `/tmp/ray-cpbh/` is the cluster's temp dir; `/tmp/ray/session_*` stayed 160 and
  `/tmp/ray/ray_current_cluster` absent.
- The venv: `pip freeze` before the suites and after the stop, `cmp` exit 0 (101 lines).
- Under `$R`, 172 paths by `find $R | wc -l` (the root included): the 17 store files,
  `logs/03-reference.{log,pid}`, `03-readers/` (with `before/`'s 17 copies, the six logs,
  `status.txt` and the plot outputs) and `03-refusals/` (three logs). Nothing under `$R` was
  deleted.

### 1.8 The suites

Before `start`, serially, each in the foreground, with no cluster up:

| Root | Tests | Result | Time | Load (1 min), start → end |
|---|---|---|---|---|
| CosmologyModels | 18 | OK | 78.3 s | 6.68 → 6.07 |
| ComputeTargets `test_[a-f]*` | 61 | OK | 113.5 s | 6.07 → 3.04 |
| ComputeTargets `test_[g-z]*` | 91 | OK | 116.1 s | 3.04 → 2.54 |
| ObjectFactories | 63 | OK | 5.3 s | 2.41 → 2.78 |
| tools | 34 | OK | 7.7 s | 2.78 → 2.81 |

02's counts exactly (ComputeTargets 152). This prompt changes no code, so they were not re-run after.

### 1.9 The commands, verbatim

From the repository root, with `R=~/ScienceData/ChamPBH-rehearsals/read-only-plots`:

```bash
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status
./venv/bin/pip freeze > <scratch>/pip-freeze-before.txt
PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t .
PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t . -p 'test_[a-f]*.py'
PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t . -p 'test_[g-z]*.py'
PYTHONPATH=. ./venv/bin/python -m unittest discover -s ObjectFactories/tests -t .
PYTHONPATH=. ./venv/bin/python -m unittest discover -s tools/tests -t .

.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh start "read-only-plots 03 (R)" 2>&1 | tee <scratch>/start.txt
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status
./venv/bin/ray status --address 127.0.0.1:6380

REHEARSAL_ROOT=$R .documents/dk-and-rpk-migration/rehearsal/build.sh 03 reference reference 127.0.0.1:6380
./venv/bin/python .documents/dk-and-rpk-migration/rehearsal/stage_counts.py $R/logs/03-reference.log --json <scratch>/03-stage-counts.json > <scratch>/03-stage-counts.txt

cd $R && shasum -a 256 03-reference*.db > <scratch>/03-checksums-before.txt && /usr/bin/stat -f '%m %z %N' 03-reference*.db > <scratch>/03-stat-before.txt
REHEARSAL_ROOT=$R .documents/dk-and-rpk-migration/rehearsal/run_readers.sh 03 reference 127.0.0.1:6380
cd $R && shasum -a 256 03-reference*.db > <scratch>/03-checksums-after-readers.txt && /usr/bin/stat -f '%m %z %N' 03-reference*.db > <scratch>/03-stat-after-readers.txt
cmp <scratch>/03-checksums-before.txt <scratch>/03-checksums-after-readers.txt
cmp <scratch>/03-stat-before.txt <scratch>/03-stat-after-readers.txt
find $R -name '*-journal' -o -name '*-wal' -o -name '*-shm'
shasum -a 256 $R/03-readers/plot_by_beta/QCD_Cosmology/histories.csv

./venv/bin/ray list jobs --address 127.0.0.1:6380
mkdir -p $R/03-refusals
PYTHONPATH=. ./venv/bin/python -u plot_by_beta.py --config exponential.yaml --database $R/03-reference.db --shards 16 --phi-init-Mp 5 --output $R/03-refusals/ray-address-auto --ray-address auto > $R/03-refusals/ray-address-auto.log 2>&1; echo "exit $?"
./venv/bin/ray list jobs --address 127.0.0.1:6380
PYTHONPATH=. ./venv/bin/python -u plot_by_beta.py --config exponential.yaml --database $R/03-reference.db --shards 16 --phi-init-Mp 5 --T-stop-GeV 1e-7 --output $R/03-refusals/T-stop-1e-7 --ray-address 127.0.0.1:6380 > $R/03-refusals/T-stop-1e-7.log 2>&1; echo "exit $?"
./venv/bin/ray list jobs --address 127.0.0.1:6380
./venv/bin/ray list actors --address 127.0.0.1:6380 --filter "state=ALIVE"
./venv/bin/ray list actors --address 127.0.0.1:6380 --limit 200 --format json
PYTHONPATH=. ./venv/bin/python -u plot_by_beta.py --config exponential.yaml --database $R/03-reference.db --shards 16 --phi-init-Mp 5 --drop bbn-data --output $R/03-refusals/drop-bbn-data --ray-address 127.0.0.1:6380 > $R/03-refusals/drop-bbn-data.log 2>&1; echo "exit $?"
./venv/bin/ray list jobs --address 127.0.0.1:6380
cd $R && shasum -a 256 03-reference*.db > <scratch>/03-checksums-after-refusals.txt && /usr/bin/stat -f '%m %z %N' 03-reference*.db > <scratch>/03-stat-after-refusals.txt
cmp <scratch>/03-checksums-before.txt <scratch>/03-checksums-after-refusals.txt
cmp <scratch>/03-stat-before.txt <scratch>/03-stat-after-refusals.txt
find $R -name '*-journal' -o -name '*-wal' -o -name '*-shm'
./venv/bin/ray list actors --address 127.0.0.1:6380 --filter "state=ALIVE"
./venv/bin/ray list actors --address 127.0.0.1:6380 --limit 200 --format json
./venv/bin/ray list jobs --address 127.0.0.1:6380

.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh stop
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status
./venv/bin/pip freeze > <scratch>/pip-freeze-after.txt
cmp <scratch>/pip-freeze-before.txt <scratch>/pip-freeze-after.txt
```

`build.sh` and `run_readers.sh` each ran as a background task, waited on by its notification (the
build also by a `Monitor` on its log, stopped before the cluster was). The committed outputs are the
logs with ANSI colour codes removed (`sed -E 's/\x1b\[[0-9;]*m//g'`), and otherwise verbatim; the
`.json` files are copied unchanged.
