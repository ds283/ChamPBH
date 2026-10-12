# dk-and-rpk-migration — rehearsal verification

**Campaign:** [`prompts/dk-and-rpk-migration/`](../prompts/dk-and-rpk-migration/README.md) ·
**Board:** [`IMPLEMENTATION_STATE.md`](../prompts/dk-and-rpk-migration/IMPLEMENTATION_STATE.md) ·
**Outputs:** [`dk-and-rpk-migration/out/`](dk-and-rpk-migration/out/) ·
**Scripts:** [`dk-and-rpk-migration/rehearsal/`](dk-and-rpk-migration/rehearsal/)

This document records the campaign's three rehearsals. Each rebuilds the user's 2026-10-02 pilot
grid twice under a real Ray cluster, once straight through and once killed and resumed, and runs
every reader on the result. The protocol is the campaign README's §4.1, the comparison by value
is §4.2 (`tools/compare_stores.py`), and the reproducibility criterion is §4.3.
- **§1 R0** is the start tree: ChamPBH's own layer and `RayTools/` (prompt 02).
- **§2 R1** is the tree before the DatastoreKit swap (prompt 06).
- **§3 R2** is the tree after both swaps (prompt 10).
- **§4** is the close (prompt 11).

It is additive (`CLAUDE.md` invariant 6). A re-run that supersedes a figure gets a new dated
subsection, and the figures below stay as they are, correct for the tree they were taken on.

The stores, logs and reader outputs live outside the repository, under
`~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/` (README §3, P2), written `$R` below. Only
small text outputs are committed, under `dk-and-rpk-migration/out/NN-*`.

---

## 1. R0 — the start tree (prompt 02, 2026-10-12)

**Tree:** `a72ca5e`, whose code is `d7791c5`'s (`git diff --stat d7791c5 HEAD -- . ':!prompts'`
is empty). Branch `dk-and-rpk-migration`. Neither `datastorekit` nor `raypoolkit` is installed.
Ray 2.53.0, Python 3.13 (the venv).

**Result: R0 holds. The start tree builds, resumes and reads the pilot cleanly (L1–L3), and two
independent builds of the grid are equal by value, bit for bit, in every stored science column
(§4.3). So R1 and R2 are held to bitwise equality with `02-reference.db` (P4).**

### 1.1 The cluster and the load

- 10 CPUs (`sysctl -n hw.ncpu`), 16 GB.
- **The load average was above the CPU count throughout**, and is recorded rather than waited out,
  as the prompt says:

  | When (BST) | Load average (1 / 5 / 15 min) |
  |---|---|
  | 01:01, at dispatch | 13.38 / 21.86 / 20.18 |
  | 01:12:27, before `ray start` (after the suites) | 12.12 / 15.24 / 18.03 |
  | 01:24:20, `resumed-a` starts | **119.10 / 139.14 / 112.57** |
  | 01:33:06, `resumed-b` starts | 40.16 / 117.92 / 129.19 |
  | 01:34:41, `resumed-b` ends | 35.76 / 96.10 / 119.60 |
  | 01:36:53, the readers end | 23.35 / 69.42 / 105.38 |

  The load came from outside this rehearsal: system daemons (`mds`, `suggestd`,
  `mediaanalysisd`) and, from 01:29:50, another session's suite. That suite was
  `python -m unittest discover -s datastorekit/tests -t .`, run from a DatastoreKit session's
  scratch directory and not from this repository (pid 504, read with `ps` and `lsof`). It ran
  through `resumed-a`'s last minutes, `resumed-b` and the readers. No BBN solve came near its
  600 s wall-clock limit: every `BBNData` row in both stores has `failure = 0`.
- **The cluster.** `./venv/bin/ray start --head --disable-usage-stats`, started at 01:12:29 from
  the repository root. Ray 2.53.0's `ray start` printed `Local node IP: 127.0.0.1` and a bare
  `ray.init()`, but no address line. The address, **`127.0.0.1:6379`**, was read from Ray's
  `/tmp/ray/ray_current_cluster`. `ray status` then showed one node with 10 CPUs, and each driver
  logged `Connecting to existing Ray cluster at address: 127.0.0.1:6379`.
- `./venv/bin/ray stop` at 01:37:44. At 01:37:53, `ray status` found no running instance, and
  `pgrep -lf 'gcs_server|raylet|ray::|dashboard'` found nothing.

### 1.2 The builds

| Build | Store | Started (BST) | Ended | Wall | Exit status | Stages' times |
|---|---|---|---|---|---|---|
| `reference` | `02-reference.db` | 01:12:49 | 01:23:32 | 10 min 43 s | **0** | 5m 24.7s · 12.6s · 1m 8.73s |
| `resumed-a` | `02-resumed.db` | 01:24:20 | 01:32:08 | 7 min 48 s | **134**, by `SIGTERM` at 01:32:05 | 3m 43.6s · killed |
| `resumed-b` | `02-resumed.db` | 01:33:06 | 01:34:41 | 1 min 35 s | **0** | 0.865s · 7.23s · 36.4s |

The stages are, in order, `CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID`,
`CALCULATE ADIABATIC TRANSGRESSION PARAMETERS` and `CALCULATE BBN DATA`. Start and end times are
the `date` lines around each `build.sh`. The pilot of 2026-10-02 took about 9 minutes, with BBN on
the full network (1 min 40 s). Here the adiabatic stage took 12.6 s, against the pilot's 2 min 19 s.

**The interrupt delay `S` = 5 s**, chosen from `reference`'s store after that build had ended,
read with `sqlite3` in `mode=ro` (the commands are in §1.8).
- `reference`'s log has one progress line in the scalar stage: at 01:22:00, 5m 17.4s running. So
  the stage began at 01:16:42.6 and ended, at 5m 24.7s, at about **01:22:07.3**. The adiabatic
  banner follows at once.
- The last `ScalarModel` row is timestamped 01:21:56.6, just before the banner.
- The five `AdiabaticHistory` rows are timestamped 01:22:11.89, 12.16, 12.90, 13.69 and 14.92.
  That is banner + 4.6, 4.9, 5.6, 6.4 and 7.6 s; the first 4.6 s is the reload of the five
  `ScalarModel`s.
- `interrupt_when.py` polls each second, so it sees the banner up to 1 s late. With `S` = 5 s the
  signal falls at banner + 5–6 s, inside the spread. The pilot's 2 min 19 s ("about half") did not
  apply to this tree and machine.

**What `resumed-a` kept.** `interrupt_when.py` saw the banner at 01:32:00 and sent `SIGTERM` to the
driver (pid 97614) at 01:32:05. In `02-resumed.db` (`mode=ro`), before `resumed-b`:

| `AdiabaticHistory` | Timestamp | `validated` | Value rows / `z_samples` |
|---|---|---|---|
| β = 1.15 (shard 0, store_id 1) | 01:32:01.99 | 1 | 4975 / 4975 |
| β = 1.1 (shard 15, store_id 2) | 01:32:03.03 | 1 | 4947 / 4947 |
| β = 1.2 (shard 1, store_id 3) | 01:32:03.51 | 1 | 5002 / 5002 |
| β = 10 (shard 2, store_id 4) | 01:32:03.95 | **0** | 9776 / 9776 |
| β = 25 | not stored | — | — |

So the kill was partial, as the protocol needs, and it was better than intended: it also fell
between one history's store and its validation. `resumed-b`'s open therefore had a real
unvalidated row to prune.

**The actor check, between `resumed-a` and `resumed-b`.** At 01:32:33, 28 s after the signal,
`./venv/bin/ray list actors --filter "state=ALIVE"` printed `No resource in the cluster`.
`./venv/bin/ray list actors --limit 200` listed 34 actors, all `DEAD`: 17 of job `01000000`
(`reference`) and 17 of job `02000000` (`resumed-a`), each 16 `Datastore` (`shard0000-store` …
`shard0015-store`) and one `SerialPoolBroker`, each job in its own anonymous namespace. Before
`ray stop`, the same check again printed `No resource in the cluster`. By then 102 actors had been
recorded, all dead: 6 jobs × 17, the three builds and the three Ray-using readers.
`./venv/bin/ray list jobs` gave all six as `SUCCEEDED`, the killed `resumed-a` included.

### 1.3 L1 — every build ends as it should

- **`reference`** and **`resumed-b`**: `** driver exit status 0`, and every stage prints
  `-- ALL WORK ITEMS COMPLETE` and its queue summary (`stage_counts.py`: all three `complete`).
- **`resumed-a`** ended by the signal, in the stage meant. The adiabatic banner is the last stage
  line, and `stage_counts.py` gives that stage as `INCOMPLETE; no queue summary`. The driver's
  status is **134** (`Abort trap: 6`), not 143. Ray's driver installs absl's failure-signal
  handler, which logs `*** SIGTERM received at time=1791765125 ***` and a native stack (the main
  thread in `ray::core::CoreWorker::Wait()`), twice (stderr and `logging.cc`), and then aborts.
  That is Ray's way of dying on `SIGTERM`, not a failure of the build. 06 and 10 should expect the
  same (log 02, Observation 1).
- **No Python traceback in any log.** `grep -c Traceback` gives 0 for all three build logs and all
  five reader logs. The only other unusual lines are the three expected kinds: Ray's
  `FutureWarning` about `RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO`, the three super-Planckian warnings
  (β = 10, 25 at φ* = 5), and the `@@ Parametrized equation-of-state` line from the `Datastore`
  actors.
- **The provenance line** is the first line of every log, the three builds and the three Ray
  drivers among the readers: `>> packages: datastorekit not installed; raypoolkit not installed`.

### 1.4 L2 — the resume

**The prune at open, every build** (`--prune-unvalidated` is on by default; log 01, Observation 1).
- `reference` and `resumed-a` each created a new store (`>> Created sharded datastore … with 16
  shards`) and printed no integrity report.
- **`resumed-b`** opened the existing store and printed one report, from `shard0002-store`:

  ```text
  !! INTEGRITY WARNING (2026-10-12T01:33:15): datastore "shard0002-store" (physical file …/02-resumed-shard0002.db)
  >> AdiabaticHistory instances
       The following unvalidated models were detected in the datastore:
         -- "dk-and-rpk-migration-02-resumed-a-AdiabaticHistory-ExponentialPotential(M=0.001Mp,Lambda=0.001eV)-ExponentialCoupling(beta=10)-2026-10-12T01:32:01" (store_id=4)
            contains 9776 z-sample values | expected=9776
       ** Note: these values have been pruned from the datastore.
  ```

  That is exactly the one unvalidated row of the table above. Nothing validated was pruned: the
  three validated `resumed-a` histories are still in the store (label counts below).
- None of the readers printed an integrity report.

**The stage counts** (`stage_counts.py`, the queue summary without rates;
[`02-stage-counts.txt`](dk-and-rpk-migration/out/02-stage-counts.txt)):

| Stage | `reference` | `resumed-a` | `resumed-b` |
|---|---|---|---|
| scalar histories | lookup 5, compute 5, store 5, validation 5 | lookup 5, compute 5, store 5, validation 5 | lookup 5, compute 0, store 0, validation 0; *no compute items evaluated* |
| adiabatic | 5 / 5 / 5 / 5 | INCOMPLETE, no summary | **lookup 2, compute 2, store 2, validation 2** |
| BBN | 5 / 5 / 5 / 5 | not reached | 5 / 5 / 5 / 5 |
| skipped (adiabatic; BBN) | 0; 0 and 0 stored failures | — | 0; 0 and 0 stored failures |
| driver exit status | 0 | 134 | 0 |

**The label counts** (`sqlite3` `mode=ro`, all 16 shards of each store, after `resumed-b` and the
readers):

| Table | `02-reference.db` | `02-resumed.db` |
|---|---|---|
| `ScalarModel` | 5 `…-02-reference-…` | 5 `…-02-resumed-a-…` |
| `AdiabaticHistory` | 5 `…-02-reference-…` | **3 `…-02-resumed-a-…`** (β = 1.1, 1.15, 1.2) + **2 `…-02-resumed-b-…`** (β = 10, 25) |
| `BBNData` | 5 `…-02-reference-…` | 5 `…-02-resumed-b-…` |

All rows are `validated = 1`.

**So `resumed-b` computed exactly what `resumed-a` had not kept.**
- No `ScalarModel` was computed: `resumed-a` had stored all five.
- **Two** `AdiabaticHistory` rows were computed: 5 − *k* with *k* = 3 kept and validated. β = 10
  had been pruned at open, and β = 25 had never been stored.
- All five `BBNData` rows were computed: `resumed-a` never reached that stage.

**One pattern differs from the expected table in the orchestrator's brief**, and it is not a
recomputation: the adiabatic stage's `lookup` figure in `resumed-b` is 2, not 5.
- The adiabatic stage's task builder finds what is missing with its own vectorized gets first
  (`select_missing`, `main.py:432-437`), and hands the pool work items only for those, so the
  pool sees 2 lookups and 2 computes.
- The scalar stage's builder (`main.py:219-226`) hands the pool every model, whatever its lookup
  returned, so it sees 5 lookups and 0 computes.

See log 02, Observation 2. L4's baseline is the measured pattern.

**The comparison** of `reference` against `resumed` is §1.6.

### 1.5 L3 — the readers

`./.documents/dk-and-rpk-migration/rehearsal/run_readers.sh 02 resumed 127.0.0.1:6379`, 01:35:07 to
01:36:53, on the closed `02-resumed.db`. Abridged output:
[`02-readers.txt`](dk-and-rpk-migration/out/02-readers.txt).

| Step | Exit |
|---|---|
| 0 copy-before (`cp -p` into `02-readers/before/`) | 0 |
| 1 `main.py --inventory` | 0 |
| 2 `plot_by_beta.py --phi-init-Mp 5` | 0 |
| 3 `plot_ScalarModel.py` | 0 |
| 4 `tools/bbn_from_store.py … --beta 1.15 --M-Mp 1e-3 --small-network --variant prod` | 0 |
| 5 `compare_stores.py before/02-resumed.db 02-resumed.db` | 0 |

- **`--inventory`** printed:
  - 21250 redshifts;
  - β = [1.1, 1.15, 1.2, 10, 25];
  - the temperatures 2.3493e-13 GeV and 20000 GeV;
  - M = 2.436e+24 eV, Λ = 0.001 eV, φ = 5 M_P, π = 0;
  - 5 validated `ScalarModel`s (all `resumed-a`, each with T_init = 20000 GeV,
    T_stop = 2.3493e-13 GeV, φE_init = 5 Mp, πE_init = 0, `failure=False`);
  - 5 validated `AdiabaticHistory` rows (3 `resumed-a`, 2 `resumed-b`);
  - 5 validated `BBNData` rows (all `resumed-b`, `failure=False`);
  - no unvalidated row in any of the three, and version `2026.6.0` throughout.
- **`plot_by_beta.py`.**
  - Its SM baseline: `Yp=0.24688, D/H x1e5=2.45829, 3He/H x1e5=1.04193, 7Li/H x1e10=5.48637`
    (small network, `PRyM_version=bf24c3d+ri02+sr01+bt02`).
  - "no models used elastic reflections (5 model(s) checked)".
  - **`histories.csv`** (SHA-256 `d7c62c37d8ad637a63854583ab11bc737109dcbfba92168dbddbaea75182c522`;
    06 and 10 compare it byte for byte), the five rows abridged to their first columns:

    | β | Yp_BBN | D/H | δYp | δ(D/H) | T_deliver / GeV | reflections |
    |---|---|---|---|---|---|---|
    | 1.1 | 0.27190786953584556 | 2.821839542608145 | 0.1013757144549777 | 0.1478881504469497 | 0.14343463093291087 | 0 |
    | 1.15 | 0.24687349447579363 | 2.4859924812264325 | −2.7208398138646662e-05 | 0.01126987137705732 | 0.2040424810765032 | 0 |
    | 1.2 | 0.25671377602976314 | 2.6012559126805233 | 0.03983131848246185 | 0.058157557635686745 | 0.23110752386033478 | 0 |
    | 10 | 0.24676985062644302 | 2.4589590648096062 | −0.00044702272282974326 | 0.0002730241423306133 | 404.9257748914721 | 0 |
    | 25 | 0.2469917606294866 | 2.460803860070592 | 0.0004518342634135867 | 0.0010234632045491919 | 1065.1632841557907 | 0 |

    `failure_reasons` is empty in every row. The full rows are in `02-readers.txt`.
- **`plot_ScalarModel.py`**: 5 lookups, no compute item evaluated, a `fields.csv` and plots for
  each β.
- Both plot scripts print `!! WARNING: number of shards read from database (=16) does not match
  specified number of shards (=10)`, as the user's `pilot-plot.log` of 2026-10-02 does: they
  construct `ShardedPool` without `shards=`. Cosmetic; log 02, Observation 3.
- **`bbn_from_store.py`, β = 1.15**, on shard 0000, serial 1, small network:

  | | Yp | D/H (×10⁵) |
  |---|---|---|
  | stored row | 0.24687349447579363 | 2.4859924812264325 |
  | `prod` re-solve | 0.2468734945 | 2.485992481 |

  **Equal to every digit the tool prints.** It prints the re-solve to 10 significant figures, so
  the check shows agreement to 10 figures, not bitwise. The re-solve took 12.1 s (He3/H 1.046802492,
  Li7/H 5.403050409). The stored row equals `histories.csv`'s β = 1.15 row exactly.
- **The store is unchanged by the readers.** Step 5 gives `RESULT: equal` with **no bookkeeping
  column differing** and no problem, so the readers wrote no row at all (27 tables; e.g.
  `ScalarModelValue` 42617 matched, `redshift` 21250).

### 1.6 §4.3 — reproducibility: bitwise

The command (§1.8) produced
[`02-compare-reference-resumed.txt`](dk-and-rpk-migration/out/02-compare-reference-resumed.txt) and
[`.json`](dk-and-rpk-migration/out/02-compare-reference-resumed.json). It ran in 7 s.

**`RESULT: equal`, exit 0.** 27 client tables, every row matched, none in one store only, no near
miss, no problem. The `LambdaCDM` note on both primaries is expected (log 01).

| Table | Matched | A only | B only |
|---|---|---|---|
| `ScalarModel` / `ScalarModelValue` / `_tags` | 5 / 42617 / 5 | 0 | 0 |
| `AdiabaticHistory` / `AdiabaticHistoryValue` / `_tags` | 5 / 42617 / 5 | 0 | 0 |
| `BBNData` / `BBNDataValue` / `_tags` | 5 / 7475 / 5 | 0 | 0 |
| `ExponentialCoupling` | 5 | 0 | 0 |
| `redshift` | 21250 | 0 | 0 |
| the 15 other replicated tables | 1–6 each (0 for the four unused potentials) | 0 | 0 |

**Why this is not a vacuous "equal".**
- The two stores were built by separate jobs, each history integrated, transgressed and
  BBN-solved independently, under very different load. The bookkeeping columns show it: they
  differ on **every** matched row. That covers every `label` and `timestamp` of `ScalarModel`,
  `AdiabaticHistory` and `BBNData` and their tags; `ScalarModel.compute_time` and the three RHS
  timings; `AdiabaticHistory.compute_time`; `BBNData.BBN_compute_time` and `NP_compute_time`; and
  the timestamps of every parameter table, 21250 redshifts among them.
- Every science column is compared by its stored bits (`float.hex`), with foreign keys resolved
  to the identity of the row they name. That includes `compute_steps`, `RHS_evaluations`,
  `extra_data`, `z_samples`, the abundances, `small_network` and `PRyM_version`, and every value
  row.
- The resumed store's histories come from two builds (`resumed-a`, `resumed-b`) and were reloaded
  across a kill and a prune.

**So R0 is bitwise reproducible, and R1 (06) and R2 (10) are held to bitwise equality with
`02-reference.db` by README §4.3 and P4.** No tolerance is needed or chosen.

### 1.7 L4 — the baseline, and the schema snapshot

**The baseline** for 06 and 10 is §1.4's stage counts and label counts, and the resume's pattern:
- the prune at open removed one unvalidated `AdiabaticHistory` (`resumed-a`'s β = 10, 9776 of 9776
  values) and nothing else;
- `resumed-b` computed 0 / 2 / 5 in the three stages, with lookups 5 / 2 / 5.

How many rows a resume keeps depends on where the signal lands, so R1 and R2 compare the pattern:
`resumed-b` computes exactly what `resumed-a` did not keep, and the prune removes only unvalidated
rows. They do not compare the number.

**The schema snapshot** of `02-reference.db` is
[`02-reference-schema.json`](dk-and-rpk-migration/out/02-reference-schema.json), the start
tree's, for 05 and 08:
- format `champbh-compare-stores-schema`, version 1;
- 27 client tables: `AdiabaticHistory`, `AdiabaticHistoryValue`, `AdiabaticHistory_tags`,
  `BBNData`, `BBNDataValue`, `BBNData_tags`, `ExponentialCoupling`, `ExponentialPotential`,
  `IntegrationSolver`, `InversePowerPotential`, `Lambda_value`, `M_value`, `QCD_Cosmology`,
  `ReclinerPotential`, `ReflectingPotential`, `ScalarModel`, `ScalarModelValue`,
  `ScalarModel_tags`, `StarobinskyPotential`, `beta_value`, `phi_value`, `pi_value`, `redshift`,
  `store_tag`, `temperature`, `tolerance`, `version`;
- `--schema` of the store against the file gives `RESULT: the schemas agree`.

### 1.8 The commands, verbatim

From the repository root, with `R=~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration`:

```bash
mkdir -p ~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration
./venv/bin/ray start --head --disable-usage-stats
cat /tmp/ray/ray_current_cluster            # 127.0.0.1:6379
./.documents/dk-and-rpk-migration/rehearsal/build.sh 02 reference reference 127.0.0.1:6379

# S, from reference's store (mode=ro), for each f in $R/02-reference-shard*.db
#   and T in ScalarModel AdiabaticHistory BBNData:
sqlite3 "file:$f?mode=ro" "select '$T', '$(basename $f)', serial, timestamp, validated, label from $T"

./.documents/dk-and-rpk-migration/rehearsal/build.sh 02 resumed resumed-a 127.0.0.1:6379
./venv/bin/python .documents/dk-and-rpk-migration/rehearsal/interrupt_when.py $R/logs/02-resumed-a.log $R/logs/02-resumed-a.pid --banner "** CALCULATE ADIABATIC TRANSGRESSION PARAMETERS" --after 5

./venv/bin/ray list actors --filter "state=ALIVE"
./venv/bin/ray list actors --limit 200
./venv/bin/ray list jobs

./.documents/dk-and-rpk-migration/rehearsal/build.sh 02 resumed resumed-b 127.0.0.1:6379 --prune-unvalidated
./.documents/dk-and-rpk-migration/rehearsal/run_readers.sh 02 resumed 127.0.0.1:6379

PYTHONPATH=. ./venv/bin/python -u tools/compare_stores.py $R/02-reference.db $R/02-resumed.db --json .documents/dk-and-rpk-migration/out/02-compare-reference-resumed.json > .documents/dk-and-rpk-migration/out/02-compare-reference-resumed.txt 2>&1
./venv/bin/python tools/compare_stores.py --snapshot $R/02-reference.db --out .documents/dk-and-rpk-migration/out/02-reference-schema.json
./venv/bin/python tools/compare_stores.py --schema $R/02-reference.db --against .documents/dk-and-rpk-migration/out/02-reference-schema.json
./venv/bin/python .documents/dk-and-rpk-migration/rehearsal/stage_counts.py $R/logs/02-reference.log $R/logs/02-resumed-a.log $R/logs/02-resumed-b.log --json .documents/dk-and-rpk-migration/out/02-stage-counts.json > .documents/dk-and-rpk-migration/out/02-stage-counts.txt

./venv/bin/ray list actors --filter "state=ALIVE"
./venv/bin/ray stop
```

Each `build.sh` and `run_readers.sh` ran as a background task. `interrupt_when.py` ran as a second
background task, started just after `resumed-a`'s `build.sh`. The label counts are the same
`sqlite3 … mode=ro` select over both stores' shards, grouped by the label's
`dk-and-rpk-migration-02-<build>` prefix.

**Left on disk under `$R`, none deleted** (README §3):
- `02-reference.db` and `02-resumed.db`, each with 16 shards;
- `logs/02-{reference,resumed-a,resumed-b}.{log,pid}`;
- `02-readers/`, with its `before/` copy, the five step logs, `status.txt`, and the
  `plot_by_beta/` and `plot_ScalarModel/` outputs.

---

## 2. R1 — before the DatastoreKit swap (prompt 06, 2026-10-12)

**Tree:** `5bb2be6`, whose code is `a4894ea`'s: 05's tree (`8e77578`) with 05a's `ray_cluster.sh` and
the housekeeping `288b385`. Since R0's tree (`a72ca5e`) the code differs by 03–05 only: the
`raypoolkit` pin, `RayTools/` deleted and the three drivers importing `from raypoolkit import
RayWorkPool`, and ChamPBH's data moved into `config/datastore.py` with the seven vectorized gets in
the mapping form. Still on ChamPBH's own layer. `raypoolkit` 0.2.0 at `eaef0ce6…`, not editable;
no `datastorekit`. Ray 2.53.0, Python 3.13 (the venv, 100 lines of `pip freeze`, `pip check` clean).
`build.sh`, `interrupt_when.py`, `stage_counts.py`, `run_readers.sh` and `tools/compare_stores.py`
are as R0 ran them; the cluster is 05a's `ray_cluster.sh` (U5).

**Result: R1 holds. Both R1 stores equal `02-reference.db` bit for bit in every stored science
column (`compare_stores` `RESULT: equal`, exit 0, twice), and `06-reference.db` equals
`06-resumed.db` (§4.3 again). The schema is R0's. L1–L3 hold, and L4's counts, resume pattern and
printed lines are R0's. So 03–05 changed nothing a build stores, reads or prints.** One difference
from R0 in how the resume fell, not in what it produced: `resumed-a` stored all five
`AdiabaticHistory` rows but validated only two (§2.2).

### 2.1 The cluster and the load

- 10 CPUs, host `mps026088`.
- **The load average was above the CPU count throughout**, as at R0, and is recorded rather than
  waited out:

  | When (BST) | Load average (1 / 5 / 15 min) |
  |---|---|
  | 16:31, the gate | 29.17 / 27.57 / 23.69 |
  | 16:43:15, before `start` (after the suites) | 45.30 / 40.09 / 30.74 |
  | 16:43:53, `reference` starts | 54.17 / 43.12 / 32.28 |
  | 16:55:00, `reference` ends | **159.55 / 157.88 / 127.42** |
  | 16:55:30, `resumed-a` starts | 104.33 / 144.71 / 123.70 |
  | 17:03:44, `resumed-a` ends | 76.96 / 131.08 / 138.16 |
  | 17:04:28, `resumed-b` starts | 51.95 / 117.83 / 132.98 |
  | 17:07:10, `resumed-b` ends | 50.77 / 95.54 / 121.54 |
  | 17:07:30 – 17:09:26, the readers | 42.56 / 90.76 / 119.22 → 48.32 / 83.34 / 113.10 |
  | 17:12, after the stop | 42.33 / 64.65 / 100.09 |

  At the gate (16:31) the top processes were two foreign suites,
  `python -m unittest discover -s datastorekit/tests -t .` (pids 69287 under Python 3.12 and 69288
  under 3.13, each about 90 % CPU, working directory a DatastoreKit session's scratchpad), with
  macOS daemons (`suggestd`, `backupd`, `trustd`, `fseventsd`, Spotlight). Both suites had ended by
  16:43:15, before the cluster started; no foreign suite or Ray process was seen after that. The
  process list was not sampled during the builds, so the peak near 160 is not attributed. No BBN
  solve came near its wall-clock limit: every `BBNData` row in both stores has `failure = 0`.
- **The cluster**, started at 16:43:19 from the repository root:
  `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh start "dk-and-rpk-migration 06 (R1)"`,
  exit 0, 22 s wall, ending
  ```text
  >> ray_cluster: address 127.0.0.1:6380
  >> ray_cluster: dashboard http://127.0.0.1:8267
  >> ray_cluster: actors: ./venv/bin/ray list actors --address 127.0.0.1:6380
  >> ray_cluster: started in 21s; gcs_server pid 80158
  ```
  The lock's owner file: `project: ChamPBH`, `purpose: dk-and-rpk-migration 06 (R1)`,
  `acquired: 2026-10-12T16:43:41+01:00`, `host: mps026088`, `ray_temp_dir: /tmp/ray-cpbh`,
  `ray_address: 127.0.0.1:6380`, `pid: 80158`. `./venv/bin/ray status --address 127.0.0.1:6380`:
  one active node, `0.0/10.0 CPU`, 8.42 GiB memory, 2.00 GiB object store, no failures.
  `/tmp/ray-cpbh/ray_current_cluster` held `127.0.0.1:6380`; `/tmp/ray/ray_current_cluster` did not
  exist, before or after. **Every driver logged `Connecting to existing Ray cluster at address:
  127.0.0.1:6380` and the dashboard at `http://127.0.0.1:8267`**; no log names 6379.
- **The stop**, at 17:09:48, after the readers and with no build, reader or background task running:
  `ray_cluster.sh stop`, exit 0, 19 s: `SIGTERM` to the 18 processes naming `/tmp/ray-cpbh`, the
  15 s grace, `SIGKILL` to 80158 (`gcs_server`) and 80161; "no process names /tmp/ray-cpbh; ports
  6380 6381 8267 52367 are free"; "released the rehearsal lock". Then `ray_cluster.sh status`, exit 0:
  the lock free, no ChamPBH process, the four ports free. `ray stop` was never run.

### 2.2 The builds

| Build | Store | Started (BST) | Ended | Wall | Exit status | Stages' times | R0 |
|---|---|---|---|---|---|---|---|
| `reference` | `06-reference.db` | 16:43:53 | 16:55:00 | 11 min 7 s | **0** | 5m 6.61s · 16.2s · 1m 5.62s | 10 min 43 s; 5m 24.7s · 12.6s · 1m 8.73s |
| `resumed-a` | `06-resumed.db` | 16:55:30 | 17:03:44 | 8 min 14 s | **134**, by `SIGTERM` at 17:03:41 | 4m 29.7s · killed | 7 min 48 s; 3m 43.6s · killed |
| `resumed-b` | `06-resumed.db` | 17:04:28 | 17:07:10 | 2 min 42 s | **0** | 1.41s · 12s · 56.5s | 1 min 35 s; 0.865s · 7.23s · 36.4s |

Start and end times are the `date` lines around each `build.sh`. The driver pids were 80492, 86620
and 91149.

**The interrupt delay `S` = 6 s**, chosen from `06-reference.db`'s own timestamps after that build
ended, read with `sqlite3` `mode=ro`, by §1.2's method.
- `reference`'s log has one progress line in the scalar stage: at 16:53:34, 5m 5.89s running. So
  the stage began at 16:48:28.1 and ended, at 5m 6.61s, at about **16:53:34.7**, where the
  adiabatic banner follows.
- The five `AdiabaticHistory` rows are timestamped 16:53:39.84 (β = 1.1), 40.73 (1.15), 41.07
  (1.2), 42.90 (10) and 44.48 (25): banner + **5.1, 6.0, 6.4, 8.2 and 9.8 s**. The stage took 16.2 s
  (R0: 12.6 s, rows at + 4.6 – 7.6 s).
- `interrupt_when.py` polls each second, so the signal falls at banner + `S` to `S` + 1 s, plus up
  to a second's lateness in seeing the banner. `S` = 6 s puts it at + 6 – 7 s by `reference`'s
  timing: inside the spread, between the second and fourth rows, and with three seconds' margin
  before the last. The margin was taken because at R0 `resumed-a`'s rows came about 2.6 s earlier
  after the banner than `reference`'s.

**What `resumed-a` kept.** `interrupt_when.py` saw the banner at 17:03:35 and sent `SIGTERM` to
pid 86620 at 17:03:41. In `06-resumed.db` (`mode=ro`), before `resumed-b`:

| `AdiabaticHistory` | Timestamp | `validated` | Value rows / `z_samples` |
|---|---|---|---|
| β = 1.15 (shard 0, serial 1) | 17:03:37.45 | 1 | 4975 / 4975 |
| β = 1.1 (shard 15, serial 2) | 17:03:38.01 | 1 | 4947 / 4947 |
| β = 1.2 (shard 1, serial 3) | 17:03:38.51 | **0** | 5002 / 5002 |
| β = 10 (shard 2, serial 4) | 17:03:39.41 | **0** | 9776 / 9776 |
| β = 25 (shard 3, serial 5) | 17:03:40.25 | **0** | 17917 / 17917 |

All five `ScalarModel`s were stored and validated; no `BBNData` row.

**So `resumed-a` stored all five rows, and validated two.** The stage ran faster after the banner
than in `reference` (rows at about + 2.4 – 5.2 s after the banner was seen, against + 5.1 – 9.8 s),
as at R0, and more so: the signal fell after the last store and before three validations. Plainly:
**by stored rows this is the "5" case** that the orchestrator's brief names, in which the resume
does not test a stage whose rows were never stored (R0's β = 25). What the resume does test is the
prune at open of three unvalidated rows and their recomputation, alongside two kept ones (*k* = 2):
a partial stage by validated rows. This is recorded, not repeated (a repeat needs store names the
prompt does not give).

**The actor check, between `resumed-a` and `resumed-b`.** At 17:03:55, 14 s after the signal,
`./venv/bin/ray list actors --address 127.0.0.1:6380 --filter "state=ALIVE"` printed `No resource in
the cluster`. At 17:04:09 (28 s), `--limit 200` listed 34 actors, all `DEAD`: 17 of job `01000000`
(`reference`) and 17 of job `02000000` (`resumed-a`), each 16 `Datastore` (`shardNNNN-store`) and
one `SerialPoolBroker`. `./venv/bin/ray list jobs --address 127.0.0.1:6380` gave both `SUCCEEDED`,
the killed driver included (as at R0). Before the stop (17:09:42): none alive; 102 actors, all
dead, 17 for each of six jobs (`01`–`06000000`: the three builds and the three Ray readers), all
six `SUCCEEDED`.

### 2.3 L1 — every build ends as it should

- **`reference`** and **`resumed-b`**: `** driver exit status 0`, and every stage prints
  `-- ALL WORK ITEMS COMPLETE` and its queue summary (`stage_counts.py`: all three `complete`).
- **`resumed-a`** ended by the signal, in the adiabatic stage: its banner is the last stage line,
  `stage_counts.py` gives `INCOMPLETE; no queue summary`, and the status is **134** (`Abort trap: 6`
  from `build.sh`), as R0's. Ray's absl handler logged `*** SIGTERM received at time=1791821021 ***`
  and a native stack twice (stderr and `logging.cc`). The main thread was in
  `CoreWorker::submit_actor_task` (pickling an actor call's arguments), where R0's was in
  `CoreWorker::Wait()`: the driver was submitting work when the signal came. The dump contains
  neither `Traceback` nor `Error`.
- **No Python traceback in any log.** `grep -c Traceback` is 0, and `grep -c Error` is 0, for the
  three build logs and the five reader logs. The other unusual lines are the expected kinds only:
  Ray's `FutureWarning` about `RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO`; the three super-Planckian
  warnings in each build; the `@@ Parametrized equation-of-state` line from the `Datastore` actors
  (`[repeated 15x across cluster]`); and in each plot script the 16-against-10 shard-count warning.
  No `(raylet) … have not registered within the timeout` line appeared.
- **The provenance line** is the first line of every build log and of each Ray reader's log
  (`1-inventory`, `2-plot_by_beta`, `3-plot_ScalarModel`), exactly once:
  `>> packages: datastorekit not installed; raypoolkit 0.2.0 (https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976, requested v0.2.0)`.
  Before the cluster started, `utilities.installed_package_provenance()` gave
  `{'datastorekit': None, 'raypoolkit': {'version': '0.2.0', 'url': 'https://github.com/ds283/RayPoolKit', 'commit': 'eaef0ce60e2fc354bb32f0ad576010e6a83c1976', 'requested_revision': 'v0.2.0', 'editable': False}}`.

### 2.4 L2 — the resume

**The prune at open.** `reference` and `resumed-a` created their stores and printed no integrity
report. **`resumed-b`** opened the existing store, and three `Datastore` actors each pruned one
row:

| Shard | `AdiabaticHistory` pruned | store_id | Values / expected |
|---|---|---|---|
| `shard0001-store` | `…-06-resumed-a-…(beta=1.2)-2026-10-12T17:03:36` | 3 | 5002 / 5002 |
| `shard0002-store` | `…-06-resumed-a-…(beta=10)-2026-10-12T17:03:36` | 4 | 9776 / 9776 |
| `shard0003-store` | `…-06-resumed-a-…(beta=25)-2026-10-12T17:03:36` | 5 | 17917 / 17917 |

each with "** Note: these values have been pruned from the datastore." These are exactly the three
unvalidated rows of §2.2. **The driver's log shows the β = 1.2 report in full and the β = 25 one
marked `[repeated 2x across cluster]`: Ray's log deduplication folded the β = 10 report into it.**
All three were read in full, before the stop, from Ray's own worker logs
(`/tmp/ray-cpbh/session_2026-10-12_16-43-24_306437_80134/logs/worker-*-03000000-912{46,47,49}.out`,
job `03000000` = `resumed-b`). Nothing validated was pruned: the two validated `resumed-a` rows are
still in the store (below). None of the readers printed an integrity report.

**The stage counts** (`stage_counts.py`; [`06-stage-counts.txt`](dk-and-rpk-migration/out/06-stage-counts.txt)),
R0's beside:

| Stage | `reference` | `resumed-a` | `resumed-b` | R0 `resumed-b` |
|---|---|---|---|---|
| scalar histories | lookup 5, compute 5, store 5, validation 5 | 5 / 5 / 5 / 5 | lookup 5, compute 0, store 0, validation 0; *no compute items evaluated* | the same |
| adiabatic | 5 / 5 / 5 / 5 | INCOMPLETE, no summary | **lookup 3, compute 3, store 3, validation 3** | 2 / 2 / 2 / 2 |
| BBN | 5 / 5 / 5 / 5 | not reached | 5 / 5 / 5 / 5 | the same |
| skipped (adiabatic; BBN) | 0; 0 and 0 stored failures | — | 0; 0 and 0 stored failures | the same |
| driver exit status | 0 | 134 | 0 | 0 |

`reference` is R0's 5 / 5 / 5 exactly.

**The label counts** (`sqlite3` `mode=ro`, all 16 shards of each store, after the readers):

| Table | `06-reference.db` | `06-resumed.db` |
|---|---|---|
| `ScalarModel` | 5 `…-06-reference-…` | 5 `…-06-resumed-a-…` |
| `AdiabaticHistory` | 5 `…-06-reference-…` | **2 `…-06-resumed-a-…`** (β = 1.1, 1.15) + **3 `…-06-resumed-b-…`** (β = 1.2, 10, 25) |
| `BBNData` | 5 `…-06-reference-…` | 5 `…-06-resumed-b-…` |

All rows are `validated = 1`.

**So `resumed-b` computed exactly what `resumed-a` had not kept**, the pattern R0 set:
- no `ScalarModel`, since all five were kept; the scalar stage looked up all five again (lookup 5,
  compute 0), as at R0 (board §3, `[02-scalar-stage-missing-filter-keeps-every-model]`, unchanged);
- three `AdiabaticHistory` rows, 5 − *k* with *k* = 2; the adiabatic lookups equal its computes (3),
  as at R0 (2 / 2);
- all five `BBNData` rows, which `resumed-a` never reached.

The comparison of `reference` against `resumed` is §2.6.

### 2.5 L3 — the readers

`./.documents/dk-and-rpk-migration/rehearsal/run_readers.sh 06 resumed 127.0.0.1:6380`, 17:07:30 to
17:09:26 (1 min 56 s; R0 1 min 46 s), on the closed `06-resumed.db`. Abridged output:
[`06-readers.txt`](dk-and-rpk-migration/out/06-readers.txt).

| Step | Exit |
|---|---|
| 0 copy-before | 0 |
| 1 `main.py --inventory` | 0 |
| 2 `plot_by_beta.py --phi-init-Mp 5` | 0 |
| 3 `plot_ScalarModel.py` | 0 |
| 4 `tools/bbn_from_store.py … --beta 1.15 --M-Mp 1e-3 --small-network --variant prod` | 0 |
| 5 `compare_stores.py before/06-resumed.db 06-resumed.db` | 0 |

- **`--inventory`**: 21250 redshifts; β = [1.1, 1.15, 1.2, 10, 25]; the temperatures 2.3493e-13 GeV
  and 20000 GeV; M = 2.436e+24 eV, Λ = 0.001 eV, φ = 5 M_P, π = 0; **5 validated `ScalarModel`**
  (all `resumed-a`, `failure=False`), **5 validated `AdiabaticHistory`** (2 `resumed-a`, 3
  `resumed-b`), **5 validated `BBNData`** (all `resumed-b`, `failure=False`); no unvalidated row in
  any of the three; version `2026.6.0` throughout. As R0's but for the 2 + 3 split.
- **`plot_by_beta.py`.** SM baseline `Yp=0.24688, D/H x1e5=2.45829, 3He/H x1e5=1.04193,
  7Li/H x1e10=5.48637` (small network, `PRyM_version=bf24c3d+ri02+sr01+bt02`), R0's; "no models
  used elastic reflections (5 model(s) checked)".
- **`histories.csv` is byte-identical to R0's.** SHA-256
  `d7c62c37d8ad637a63854583ab11bc737109dcbfba92168dbddbaea75182c522`, R0's, and
  `cmp $R/06-readers/plot_by_beta/QCD_Cosmology/histories.csv $R/02-readers/plot_by_beta/QCD_Cosmology/histories.csv`
  exits 0. Its rows are §1.5's table.
- **`plot_ScalarModel.py`**: 5 lookups, no compute item evaluated.
- **`bbn_from_store.py`, β = 1.15**, on shard 0000, serial 1, small network:

  | | Yp | D/H (×10⁵) |
  |---|---|---|
  | stored row | 0.24687349447579363 | 2.4859924812264325 |
  | `prod` re-solve | 0.2468734945 | 2.485992481 |

  **R0's figures exactly**, stored and re-solved, equal to every digit printed (10 significant
  figures for the re-solve; log 02, Observation 5). The re-solve took 12.5 s (R0 12.1 s);
  He3/H 1.046802492 and Li7/H 5.403050409, R0's.
- **The store is unchanged by the readers.** Step 5 gives `RESULT: equal` with **no bookkeeping
  column differing** and no problem (27 tables).

### 2.6 The criterion — bitwise equality with R0

The three comparisons (§2.8), after the cluster had stopped:

| A | B | Output | Result | Exit | Time |
|---|---|---|---|---|---|
| `02-reference.db` | `06-reference.db` | [`06-compare-02-reference-06-reference.txt`](dk-and-rpk-migration/out/06-compare-02-reference-06-reference.txt) / [`.json`](dk-and-rpk-migration/out/06-compare-02-reference-06-reference.json) | **`RESULT: equal`** | **0** | 7 s |
| `02-reference.db` | `06-resumed.db` | [`06-compare-02-reference-06-resumed.txt`](dk-and-rpk-migration/out/06-compare-02-reference-06-resumed.txt) / [`.json`](dk-and-rpk-migration/out/06-compare-02-reference-06-resumed.json) | **`RESULT: equal`** | **0** | 8 s |
| `06-reference.db` | `06-resumed.db` | [`06-compare-06-reference-06-resumed.txt`](dk-and-rpk-migration/out/06-compare-06-reference-06-resumed.txt) / [`.json`](dk-and-rpk-migration/out/06-compare-06-reference-06-resumed.json) | **`RESULT: equal`** | **0** | 6 s |

In each: 27 client tables, every row matched, none in one store only, no near miss, no problem; the
only notes are `LambdaCDM` on both primaries, as at R0. The matched counts are R0's: 5 rows each in
`ScalarModel`, `AdiabaticHistory`, `BBNData` and their tags, 42617 in each of `ScalarModelValue`
and `AdiabaticHistoryValue`, 7475 in `BBNDataValue`, 21250 `redshift`, 5 `ExponentialCoupling` and
`beta_value`, 6 `IntegrationSolver`. The three text outputs differ from each other, and from R0's
`02-compare-reference-resumed.txt`, only in their two header lines naming the stores.

**Why this is not a vacuous "equal".** The bookkeeping columns differ on every matched row in all
three comparisons: every `label` and `timestamp` of the three record tables and their tags,
`ScalarModel.compute_time` and its three RHS timings, `AdiabaticHistory.compute_time`,
`BBNData.BBN_compute_time` and `NP_compute_time`, and the timestamps of every parameter table, the
21250 redshifts among them. So each pair was built by separate jobs. Every science column, the
value rows included, is compared by its stored bits, with foreign keys resolved to the rows they
name. The criterion compares stores built at two trees, on two clusters (`/tmp/ray`, 6379 at R0;
`/tmp/ray-cpbh`, 6380 here), two pools (`RayTools` and `raypoolkit`) and two forms of the shard key
in the vectorized gets.

**So R1 equals R0 bit for bit**, and §4.3 holds again at this tree.

**The schema snapshot** of `06-reference.db` is
[`06-reference-schema.json`](dk-and-rpk-migration/out/06-reference-schema.json). `--schema` of the
store against `02-reference-schema.json` gives **`RESULT: the schemas agree`**, and the two JSON
files differ only in their `source` line.

### 2.7 L4 — against R0

**Counts and the resume's pattern.** §2.4: `reference` 5 / 5 / 5, R0's; `resumed-b` computes
exactly what `resumed-a` did not keep (0 / 3 / 5 here, 0 / 2 / 5 at R0), with lookups 5 / 3 / 5
(R0 5 / 2 / 5): the scalar stage looks every stored model up again, and the adiabatic stage's
lookups equal its computes. The prune removed only unvalidated rows (three here, one at R0). The
numbers differ with the signal's timing; the pattern is R0's.

**The printed lines keep their layout** ([`06-printed-lines.txt`](dk-and-rpk-migration/out/06-printed-lines.txt)).
The pool's lines were taken from `02-reference.log` and `06-reference.log` (banners, the progress
pair, `ALL WORK ITEMS COMPLETE`, `Queue summary`, and the no-compute line), ANSI codes stripped,
and normalised: date-time stamps, rates with their units, `format_time` durations and every other
number replaced by placeholders.
- The banners and completion lines agree **in text and order**: 10 lines each, the three stage
  banners (and `main.py`'s own `** BUILDING GRID OF MODELS TO SAMPLE`, printed the same way), each
  stage's completion line and queue summary.
- **06's log has a progress pair**, in the scalar stage (at 16:53:34, 5m 5.89s running, the stage
  running past `MIN_NOTIFY_INTERVAL`'s 300 s as at R0), and both its shapes occur in R0's: the
  `   -- <STAMP> (<TIME> running): <N>/<N> work items remaining = <N>% complete (may be waiting for
  compute/store/validate tasks to finish)` line, and the `      inflight: <N> lookup, <N> compute,
  <N> store, <N> validation | completed: …` line with four rate columns, the `store` column
  included. The check did not hold vacuously.
- In addition, `02-resumed-b.log` against `06-resumed-b.log`: 11 lines agree in text and order,
  the scalar stage's `(no compute items were evaluated - all assets read from datastore)` among
  them; neither has a progress line.
- The extraction is not a check that cannot fail: `02-reference.log` against `02-resumed-b.log`
  (a different line sequence) is reported as differing.

`stage_counts.py` parsed all three of 06's logs (§2.4), which is part of the same evidence.

### 2.8 The commands, verbatim

From the repository root, with `R=~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration`:

```bash
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status
PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t .
PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t . -p 'test_[a-f]*.py'
PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t . -p 'test_[g-z]*.py'
PYTHONPATH=. ./venv/bin/python -m unittest discover -s Datastore/tests -t .

.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh start "dk-and-rpk-migration 06 (R1)"
./venv/bin/ray status --address 127.0.0.1:6380
./.documents/dk-and-rpk-migration/rehearsal/build.sh 06 reference reference 127.0.0.1:6380

# S, from 06-reference.db (mode=ro): for each f in $R/06-reference-shard*.db and T in
#   ScalarModel AdiabaticHistory BBNData, in Python's sqlite3 with uri=True:
#   connect("file:$f?mode=ro").execute("select serial, timestamp, validated, label from T")

./.documents/dk-and-rpk-migration/rehearsal/build.sh 06 resumed resumed-a 127.0.0.1:6380
./venv/bin/python .documents/dk-and-rpk-migration/rehearsal/interrupt_when.py $R/logs/06-resumed-a.log $R/logs/06-resumed-a.pid --banner "** CALCULATE ADIABATIC TRANSGRESSION PARAMETERS" --after 6

./venv/bin/ray list actors --address 127.0.0.1:6380 --filter "state=ALIVE"
./venv/bin/ray list jobs --address 127.0.0.1:6380
./venv/bin/ray list actors --address 127.0.0.1:6380 --limit 200

./.documents/dk-and-rpk-migration/rehearsal/build.sh 06 resumed resumed-b 127.0.0.1:6380 --prune-unvalidated
grep -A6 "INTEGRITY WARNING" /tmp/ray-cpbh/session_latest/logs/worker-*.out
./.documents/dk-and-rpk-migration/rehearsal/run_readers.sh 06 resumed 127.0.0.1:6380
./venv/bin/ray list actors --address 127.0.0.1:6380 --filter "state=ALIVE"
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh stop
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status

PYTHONPATH=. ./venv/bin/python -u tools/compare_stores.py $R/02-reference.db $R/06-reference.db --json .documents/dk-and-rpk-migration/out/06-compare-02-reference-06-reference.json > .documents/dk-and-rpk-migration/out/06-compare-02-reference-06-reference.txt 2>&1
PYTHONPATH=. ./venv/bin/python -u tools/compare_stores.py $R/02-reference.db $R/06-resumed.db --json .documents/dk-and-rpk-migration/out/06-compare-02-reference-06-resumed.json > .documents/dk-and-rpk-migration/out/06-compare-02-reference-06-resumed.txt 2>&1
PYTHONPATH=. ./venv/bin/python -u tools/compare_stores.py $R/06-reference.db $R/06-resumed.db --json .documents/dk-and-rpk-migration/out/06-compare-06-reference-06-resumed.json > .documents/dk-and-rpk-migration/out/06-compare-06-reference-06-resumed.txt 2>&1
./venv/bin/python tools/compare_stores.py --snapshot $R/06-reference.db --out .documents/dk-and-rpk-migration/out/06-reference-schema.json
./venv/bin/python tools/compare_stores.py --schema $R/06-reference.db --against .documents/dk-and-rpk-migration/out/02-reference-schema.json
./venv/bin/python .documents/dk-and-rpk-migration/rehearsal/stage_counts.py $R/logs/06-reference.log $R/logs/06-resumed-a.log $R/logs/06-resumed-b.log --json .documents/dk-and-rpk-migration/out/06-stage-counts.json > .documents/dk-and-rpk-migration/out/06-stage-counts.txt
shasum -a 256 $R/06-readers/plot_by_beta/QCD_Cosmology/histories.csv
cmp $R/06-readers/plot_by_beta/QCD_Cosmology/histories.csv $R/02-readers/plot_by_beta/QCD_Cosmology/histories.csv
```

Each `build.sh` and `run_readers.sh` ran as a background task, waited on by its notification;
`interrupt_when.py` ran as a second background task, started just after `resumed-a`'s `build.sh`.
The label counts are the same `mode=ro` select over both stores' shards, grouped by the label's
`dk-and-rpk-migration-06-<build>` prefix and `validated`. The printed lines were extracted and
compared by a scratch script, described in `06-printed-lines.txt`'s header.

**Left on disk under `$R`, none deleted** (README §3): `06-reference.db` and `06-resumed.db`, each
with 16 shards; `logs/06-{reference,resumed-a,resumed-b}.{log,pid}`; `06-readers/`, with its
`before/` copy, the five step logs, `status.txt` and the plot outputs. R0's files were opened only by
`compare_stores.py` (and `cmp` of R0's `histories.csv`), and none has changed.
