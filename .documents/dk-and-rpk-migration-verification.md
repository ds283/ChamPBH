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

---

## 3. R2 — through both packages (prompt 10, 2026-10-13)

**Tree:** `60e000c`, whose code is `47cfcfb`'s (09's): `git diff --stat 47cfcfb HEAD -- .
':!prompts'` is empty. Since R1's tree (`5bb2be6`, code `a4894ea`) the code differs by 07–09 only:
the `datastorekit` pin (`requirements.txt:17`, `v0.3.0`), ChamPBH's layer deleted (the seven
modules gone, `Datastore/__init__.py` and `Datastore/SQL/__init__.py` 0 bytes) and every import
rewritten to the package, the pools given `factories=`, `serial_batch_sizes=` and `drop_tables=`,
`LambdaCDM` out of `replicated_tables`, and `--inventory` read-only through the package's
`read_inventory`, before Ray. `datastorekit` 0.3.0 at `2f5417ea…` and `raypoolkit` 0.2.0 at
`eaef0ce6…`, neither editable. Ray 2.53.0, Python 3.13 (the venv, 101 lines of `pip freeze`, `pip
check` clean). `build.sh`, `interrupt_when.py`, `stage_counts.py`, `run_readers.sh`,
`ray_cluster.sh`, `tools/compare_stores.py`, `tools/bbn_from_store.py` and `utilities.py` are as R1
ran them (`git diff c9e6bc2 HEAD` on them is empty).

**Result: R2 holds. Both R2 stores equal `06-reference.db`, and `10-reference.db` equals
`02-reference.db`, bit for bit in every stored science column (`compare_stores` `RESULT: equal`,
exit 0, three times), and `10-reference.db` equals `10-resumed.db` (§4.3 again). The schema is
08's snapshot and R0's. L1–L3 hold, and L4's counts, resume pattern and printed lines are R1's.
So 07–09 changed nothing a build stores or reads, and the pool prints what it printed. H1 holds:
the package's shard-key audit is clean on both stores, and a copy made by the package's tool opens
read-only, compares equal and is deleted by `delete_store`, its 17 files and nothing else.**
Two differences from R1 in how the resume fell, not in what it produced (§3.2): `resumed-a`'s
driver exited **15**, not 134, through Ray's own Python `SIGTERM` handler, which closed the pool
before the driver exited; and `resumed-a` **stored one `AdiabaticHistory` row and validated
none**, so four histories were absent from the store at the resume and the fifth was pruned.

This is the first run of the package's `Datastore` actor, `SerialPoolBroker` and `ShardedPool`
under real Ray, and ChamPBH's evidence, at Ray 2.53.0, that its registry of factory *instances*
crosses into the package's actors intact (README §0.3, point 2): 16 `shardNNNN-store` actors and
one `SerialPoolBroker` per job, five jobs, stores written, validated, pruned at open and read back,
equal to R0's by value.

### 3.1 The cluster and the load

- 10 CPUs, host `mps026088`.
- **The load.** The machine was close to idle at the gate; the load during the builds is the
  builds' own (the Ray workers of each job), and fell between them:

  | When (BST) | Load average (1 / 5 / 15 min) |
  |---|---|
  | 01:00, the gate | 8.27 / 8.90 / 7.82 |
  | 01:05:27, before `start` (after the suites) | 8.17 / 8.67 / 8.11 |
  | 01:05:43, `reference` starts | 9.69 / 8.99 / 8.23 |
  | 01:09:16, `reference` ends | 62.86 / 49.93 / 27.80 |
  | 01:10:49, `resumed-a` starts | 20.67 / 39.00 / 25.79 |
  | 01:13:46, `resumed-a` ends | 67.97 / 61.15 / 38.59 |
  | 01:14:47, `resumed-b` starts | 31.25 / 51.81 / 36.63 |
  | 01:15:48, `resumed-b` ends | 46.19 / 52.41 / 37.89 |
  | 01:16:07 – 01:17:13, the readers | 34.60 / 49.37 / 37.13 → 31.63 / 46.16 / 36.86 |
  | 01:18, after the stop | 16.70 / 38.96 / 34.83 |

  At the gate and before `start` the top processes were macOS daemons (`searchpartyd`,
  `spotlightknowledged`, `analyticsd`, `mediaanalysisd`, `MenuBarAgent`, `WindowServer`) and two
  Claude Code sessions; no foreign suite, no Ray process on the machine. R1 ran at loads of 29–160.
  Every `BBNData` row in both stores has `failure = 0`.
- **The cluster**, started at 01:05:30 from the repository root:
  `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh start "dk-and-rpk-migration 10 (R2)"`,
  exit 0, 6 s wall, ending
  ```text
  >> ray_cluster: address 127.0.0.1:6380
  >> ray_cluster: dashboard http://127.0.0.1:8267
  >> ray_cluster: actors: ./venv/bin/ray list actors --address 127.0.0.1:6380
  >> ray_cluster: started in 5s; gcs_server pid 3759
  ```
  The lock's owner file: `project: ChamPBH`, `purpose: dk-and-rpk-migration 10 (R2)`,
  `acquired: 2026-10-13T01:05:36+01:00`, `host: mps026088`, `ray_temp_dir: /tmp/ray-cpbh`,
  `ray_address: 127.0.0.1:6380`, `pid: 3759`. `./venv/bin/ray status --address 127.0.0.1:6380`:
  one active node, `0.0/10.0 CPU`, 8.61 GiB memory, 2.00 GiB object store, no failures.
  `/tmp/ray-cpbh/ray_current_cluster` held `127.0.0.1:6380`; `/tmp/ray/ray_current_cluster` did not
  exist, before or after. **Every driver that starts Ray logged `Connecting to existing Ray cluster
  at address: 127.0.0.1:6380` and the dashboard at `http://127.0.0.1:8267`** (three builds, two
  plot scripts); no log names 6379. The two `--inventory` runs start no Ray (09).
- **The stop**, at 01:17:58, after the readers and the second inventory, with no build, reader or
  background task running: `ray_cluster.sh stop`, exit 0, 18 s: `SIGTERM` to the 18 processes
  naming `/tmp/ray-cpbh` and their descendants, the 15 s grace, `SIGKILL` to 3759 (`gcs_server`)
  and 3771; "no process names /tmp/ray-cpbh; ports 6380 6381 8267 52367 are free"; "released the
  rehearsal lock". Then `ray_cluster.sh status`, exit 0: the lock free, no ChamPBH process, the
  four ports free. `ray stop` was never run.

### 3.2 The builds

| Build | Store | Started (BST) | Ended | Wall | Exit status | Stages' times | R1 |
|---|---|---|---|---|---|---|---|
| `reference` | `10-reference.db` | 01:05:43 | 01:09:16 | 3 min 33 s | **0** | 1m 54.6s · 4.35s · 21.2s | 11 min 7 s; 5m 6.61s · 16.2s · 1m 5.62s |
| `resumed-a` | `10-resumed.db` | 01:10:49 | 01:13:46 | 2 min 57 s | **15**, by `SIGTERM` at 01:13:44 | 1m 54.8s · killed | 8 min 14 s, 134; 4m 29.7s · killed |
| `resumed-b` | `10-resumed.db` | 01:14:47 | 01:15:48 | 1 min 1 s | **0** | 0.513s · 5.16s · 21s | 2 min 42 s; 1.41s · 12s · 56.5s |

Start and end times are the `date` lines around each `build.sh`. The driver pids were 4009, 7671
and 10377. The builds ran three times faster than R1's, at a load near the CPU count.

**The interrupt delay `S` = 1 s**, chosen after `reference` ended from `10-reference.db`'s own
`AdiabaticHistory` timestamps (`sqlite3` `mode=ro`), as the prompt asks, with the banner's time
from Ray's task records.
- **No progress line.** At this load the scalar stage took 1m 54.6s, under the pool's 300 s notify
  interval, so `10-reference.log` has no progress line from which to date the banner (06's
  method). The banner was dated instead from `./venv/bin/ray list tasks --address 127.0.0.1:6380
  --detail --format json` (job `01000000`): the scalar stage's last `object_validate` ended at
  01:08:48.979 and the adiabatic stage's first `object_get` was created at 01:08:49.005, so the
  banner fell at about **01:08:49.0**. The stage's 4.35 s then ends at 01:08:53.35, where its last
  `object_validate` ends (53.345): consistent.
- The five `AdiabaticHistory` rows are timestamped 01:08:49.92 (β = 1.1), 50.26 (1.15), 50.36
  (1.2), 50.68 (10) and 52.17 (25): banner + **0.9, 1.3, 1.4, 1.7 and 3.2 s**. By the task records
  their `object_store` calls returned (the rows committed) at banner + 1.5, 2.1, 2.3, 2.9 and 4.2 s,
  and their validations at + 2.4, 2.7, 2.8, 4.3 and 4.3 s. The stage took 4.35 s (R1: 16.2 s, rows
  at + 5.1 – 9.8 s; R0: 12.6 s, + 4.6 – 7.6 s).
- The prompt's rule, the earliest of `reference`'s rows less about 3 s, gives a negative delay
  here. The signal falls at banner + `S` + (0 to 1 s), the second being the polling. **`S` = 1 s**
  puts it at + 1 – 2 s: between `reference`'s first and third commits, before any validation, and
  more than 2 s before β = 25's commit, so that a `resumed-a` running somewhat ahead of `reference`
  (as at R0 and R1) should still leave rows unstored. `S` = 0 would risk no row stored; `S` = 2
  risked four or five stored.

**What `resumed-a` kept.** `interrupt_when.py` saw the banner at 01:13:43 and sent `SIGTERM` to
pid 7671 at 01:13:44 (the driver's dump: `time=1791850424`, `logging.cc` at 01:13:44.485). By the
task records of job `02000000`, the banner fell at about 01:13:43.36, so the signal came at about
banner + 1.1 s; the stage ran at `reference`'s pace (its first `object_store` created at banner +
0.90 s against `reference`'s + 0.86 s). In `10-resumed.db` (`mode=ro`), before `resumed-b`:

| `AdiabaticHistory` | Timestamp | `validated` | Value rows / `z_samples` |
|---|---|---|---|
| β = 1.1 (shard 15, serial 1) | 01:13:44.27 | **0** | 4947 / 4947 |
| β = 1.15, 1.2, 10, 25 | — | never stored | — |

All five `ScalarModel`s were stored and validated; no `BBNData` row.

**So `resumed-a` stored one row and validated none.** The first `object_store` (β = 1.1, to
`shard0015-store`) was submitted at 01:13:44.257 and returned at 44.917, after the signal; the
driver was pickling the arguments of its next actor call when the signal came (its stack:
`submit_actor_task` → `prepare_args` → `_pickle_Pickler_dump`), so no second store was submitted.
Plainly: **by validated rows this is the "0" case** the orchestrator's brief names; by stored rows
it is partial (1 of 5). The resume therefore tests what R1's did not: four histories absent from
the store, recomputed beside the prune at open of one unvalidated row.

**The exit status is 15, not 134.** Ray's absl handler printed `*** SIGTERM received at
time=1791850424 ***` and the native stack twice, as at R0 and R1, but the process then exited with
status 15 rather than aborting. 15 is the signal's number: Ray's `ray.init` installs a driver
`SIGTERM` handler, `sys.exit(signum)` (`ray/_private/worker.py:1664-1665` at 2.53.0), and here it
ran. The `SystemExit` unwound through `main.py`'s `with ShardedPool(…)` (`main.py:1223`), so the
package's `ShardedPool.__exit__` ran: at 01:13:44.50 it called `__exit__` on all 16 shard actors
(each finished by 44.99; each disposes its engine and releases its serial leases), then killed
the actors. At R0 and R1 the main thread was in native code (`CoreWorker::Wait()`,
`submit_actor_task`) and the process aborted (134) without closing the pool; here the program
counter was in `_PyErr_SetKeyError`, inside the pickler. Which of the two paths a `SIGTERM` takes
was not determined; it is Ray's behaviour, not ChamPBH's or the package's. `SystemExit` prints no
traceback, and the log has none. The resume after it is the same: the one row stored was
unvalidated and was pruned at open.

**The actor check, between `resumed-a` and `resumed-b`.** At 01:13:54, 10 s after the signal,
`./venv/bin/ray list actors --address 127.0.0.1:6380 --filter "state=ALIVE"` printed `No resource in
the cluster`. At 01:14:22 (38 s), `--limit 200` listed 34 actors, all `DEAD`: 17 of job `01000000`
(`reference`) and 17 of job **`02000000` (`resumed-a`)**, each 16 `Datastore`
(`shard0000-store` … `shard0015-store`) and one `SerialPoolBroker`. `./venv/bin/ray list jobs
--address 127.0.0.1:6380` gave both `SUCCEEDED`, the killed driver included (as at R0 and R1).
Before the stop (01:17:51): none alive; 85 actors, all dead, 17 for each of five jobs
(`01`–`05000000`: the three builds, `plot_by_beta.py` and `plot_ScalarModel.py`; `--inventory`
starts no Ray now), 80 `Datastore` and 5 `SerialPoolBroker`, all five jobs `SUCCEEDED`.

### 3.3 L1 — every build ends as it should

- **`reference`** and **`resumed-b`**: `** driver exit status 0`, and every stage prints
  `-- ALL WORK ITEMS COMPLETE` and its queue summary (`stage_counts.py`: all three `complete`).
- **`resumed-a`** ended by the signal, in the adiabatic stage: its banner is the last stage line,
  `stage_counts.py` gives `INCOMPLETE; no queue summary`, and the status is **15** (§3.2; R0 and R1:
  134). The absl dump appears twice (stderr and `logging.cc`), as before.
- **No Python traceback in any log.** `grep -c Traceback` is 0 for the three build logs and the
  six reader logs (`1-inventory`, `1b-inventory-reference`, `2-plot_by_beta`, `3-plot_ScalarModel`,
  `4-bbn_from_store`, `5-compare_stores`). `grep -c Error` is 0 in each but `10-resumed-a.log`,
  where its 2 hits are the absl dump's `PC: @ … _PyErr_SetKeyError` line, once in each copy of the
  dump. The other unusual lines are the expected kinds only: Ray's `FutureWarning` about
  `RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO`; the three super-Planckian warnings in each build; the
  `@@ Parametrized equation-of-state` line from the `Datastore` actors (`[repeated 15x across
  cluster]`); and in each plot script the 16-against-10 shard-count warning. No `(raylet) … have
  not registered within the timeout` line appeared.
- **The provenance line** is the first line of every build log, of each Ray reader's log and of
  both `--inventory` logs, exactly once (counted by `grep -cxF` of the whole line):
  `>> packages: datastorekit 0.3.0 (https://github.com/ds283/DatastoreKit@2f5417ea76cdcd0d15d64d80d9085429bfacad21, requested v0.3.0); raypoolkit 0.2.0 (https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976, requested v0.2.0)`,
  log 07's line. Before the cluster started, `utilities.installed_package_provenance()` gave both
  entries with that commit and `requested_revision`, and **`editable: False`** for each.
- **What R2 prints that R1 did not, by design:** the provenance line above; and nothing else in the
  builds. `reference` and `resumed-a` print `>> Created sharded datastore "…" with 16 shards`, and
  `resumed-b` `>> Opened existing sharded datastore "…" with 16 shards`, R1's text exactly.

### 3.4 L2 — the resume

**The prune at open, by the package's actor.** `reference` and `resumed-a` created their stores
and printed no integrity report. **`resumed-b`** opened the existing store, and one `Datastore`
actor pruned one row. Its report, read in full from both the driver's log and the actor's own Ray
worker log
(`/tmp/ray-cpbh/session_2026-10-13_01-05-32_375874_3758/logs/worker-04b4…-03000000-10411.out`, job
`03000000` = `resumed-b`, by one `grep` before the stop; the file was still under `logs/`, not
`logs/old/`):

```text
!! INTEGRITY WARNING (2026-10-13T01:14:52): datastore "shard0015-store" (physical file /Users/ds283/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/10-resumed-shard0015.db)
>> AdiabaticHistory instances
     The following unvalidated models were detected in the datastore:
       -- "dk-and-rpk-migration-10-resumed-a-AdiabaticHistory-ExponentialPotential(M=0.001Mp,Lambda=0.001eV)-ExponentialCoupling(beta=1.1)-2026-10-13T01:13:44" (store_id=1)
          contains 4947 z-sample values | expected=4947
     ** Note: these values have been pruned from the datastore.
```

The text is the factory's `validate_on_startup` report, printed by the package's actor as
ChamPBH's actor printed it at R0 and R1 (§2.4), word for word but for the names. It is exactly the
one unvalidated row of §3.2. There was only one report, so Ray's deduplication had nothing to fold;
no worker log of job `03000000` holds another `INTEGRITY WARNING`. No `>> Pruned the unvalidated
rows of …` line appears in any log: the three validating classes are sharded, so the prune is the
actors', not the pool's. None of the readers printed an integrity report.

**The stage counts** (`stage_counts.py`; [`10-stage-counts.txt`](dk-and-rpk-migration/out/10-stage-counts.txt)),
R1's beside:

| Stage | `reference` | `resumed-a` | `resumed-b` | R1 `resumed-b` |
|---|---|---|---|---|
| scalar histories | lookup 5, compute 5, store 5, validation 5 | 5 / 5 / 5 / 5 | lookup 5, compute 0, store 0, validation 0; *no compute items evaluated* | the same |
| adiabatic | 5 / 5 / 5 / 5 | INCOMPLETE, no summary | **lookup 5, compute 5, store 5, validation 5** | 3 / 3 / 3 / 3 |
| BBN | 5 / 5 / 5 / 5 | not reached | 5 / 5 / 5 / 5 | the same |
| skipped (adiabatic; BBN) | 0; 0 and 0 stored failures | — | 0; 0 and 0 stored failures | the same |
| driver exit status | 0 | 15 | 0 | 0 |

`reference` is R1's (and R0's) 5 / 5 / 5 exactly.

**The label counts** (`sqlite3` `mode=ro`, all 16 shards of each store, after `resumed-b` and
again after the readers):

| Table | `10-reference.db` | `10-resumed.db` |
|---|---|---|
| `ScalarModel` | 5 `…-10-reference-…` | 5 `…-10-resumed-a-…` |
| `AdiabaticHistory` | 5 `…-10-reference-…` | **5 `…-10-resumed-b-…`** |
| `BBNData` | 5 `…-10-reference-…` | 5 `…-10-resumed-b-…` |

All rows are `validated = 1`. `BBNData`: `failure = 0`, `small_network = 1`,
`PRyM_version = bf24c3d+ri02+sr01+bt02` in all ten rows.

**So `resumed-b` computed exactly what `resumed-a` had not kept**, the pattern R0 and R1 set:
- no `ScalarModel`, since all five were kept; the scalar stage looked up all five again (lookup 5,
  compute 0), as at R0 and R1 (board §3, `[02-scalar-stage-missing-filter-keeps-every-model]`,
  still open);
- five `AdiabaticHistory` rows, 5 − *k* with *k* = 0 validated rows kept (one stored and pruned,
  four never stored); the adiabatic lookups equal its computes (5), as at R0 (2 / 2) and R1 (3 / 3);
- all five `BBNData` rows, which `resumed-a` never reached.

The comparison of `reference` against `resumed` is §3.6.

### 3.5 L3 — the readers

`./.documents/dk-and-rpk-migration/rehearsal/run_readers.sh 10 resumed 127.0.0.1:6380`, 01:16:07 to
01:17:13 (1 min 6 s; R1 1 min 56 s), on the closed `10-resumed.db`. Then, separately (prompt
§1.4.1), `main.py --config exponential.yaml --database $R/10-reference.db --inventory` into
`$R/10-readers/1b-inventory-reference.log`, at 01:17:37, exit 0. Abridged output:
[`10-readers.txt`](dk-and-rpk-migration/out/10-readers.txt).

| Step | Exit |
|---|---|
| 0 copy-before | 0 |
| 1 `main.py --inventory` | 0 |
| 2 `plot_by_beta.py --phi-init-Mp 5` | 0 |
| 3 `plot_ScalarModel.py` | 0 |
| 4 `tools/bbn_from_store.py … --beta 1.15 --M-Mp 1e-3 --small-network --variant prod` | 0 |
| 5 `compare_stores.py before/10-resumed.db 10-resumed.db` | 0 |
| (1b) `main.py --inventory` on `10-reference.db` | 0 |

- **`--inventory`** is 09's report: read-only, before Ray, `read_inventory` on the closed store.
  Both reports head `read-only; 16 shards (#0, …, #15); 21,292 records in 21 classes` and
  **`problems: none`**. Each lists `ScalarModel (sharded): 5 records: 5 validated, 0 unvalidated;
  42,617 ScalarModelValue rows`, `AdiabaticHistory (sharded): 5 records: 5 validated, 0
  unvalidated; 42,617 AdiabaticHistoryValue rows` and `BBNData (sharded): 5 records: 5 validated,
  0 unvalidated; 7,475 BBNDataValue rows`, each group `version = 2026.6.0`, `unvalidated: none`.
  Each record is given by its physical labels (T_init 20000 GeV, T_stop 2.3493e-13 GeV, φE 5 and
  πE 0 `PlanckMass`, `ExponentialPotential(M=2.436e+24 eV, Lambda=0.001 eV, n_value=1)`,
  `ExponentialCoupling(beta=…)` for β = 1.1, 1.15, 1.2, 10, 25, `failure=False`, tag
  `SamplesPerLog10Z_250`, value counts 4947 / 4975 / 5002 / 9776 / 17917), and each BBN record
  with `PRyM_version=bf24c3d+ri02+sr01+bt02`. The two reports are identical but for the store's
  name and the commit times. Their layout is 09's (board §5.14's P15 table), so they are not in
  L4's comparison.
- **`plot_by_beta.py`.** SM baseline `Yp=0.24688, D/H x1e5=2.45829, 3He/H x1e5=1.04193,
  7Li/H x1e10=5.48637` (small network, `PRyM_version=bf24c3d+ri02+sr01+bt02`), R0's and R1's; "no
  models used elastic reflections (5 model(s) checked)".
- **`histories.csv` is byte-identical to R0's and R1's.** SHA-256
  `d7c62c37d8ad637a63854583ab11bc737109dcbfba92168dbddbaea75182c522`, and `cmp` of
  `$R/10-readers/plot_by_beta/QCD_Cosmology/histories.csv` against
  `$R/06-readers/plot_by_beta/QCD_Cosmology/histories.csv` and against R0's in `02-readers/` each
  exit 0.
- **`plot_ScalarModel.py`**: 5 lookups, no compute item evaluated (7.69 s).
- **`bbn_from_store.py`, β = 1.15**, on shard 0000, serial 1, small network:

  | | Yp | D/H (×10⁵) |
  |---|---|---|
  | stored row | 0.24687349447579363 | 2.4859924812264325 |
  | `prod` re-solve | 0.2468734945 | 2.485992481 |

  **R0's and R1's figures exactly**, stored and re-solved; He3/H 1.046802492 and Li7/H 5.403050409,
  theirs. The re-solve took 8.5 s (R1 12.5 s).
- **The store is unchanged by the readers.** Step 5 gives `RESULT: equal` with **no bookkeeping
  column differing** and no problem (27 tables).

### 3.6 The criterion — bitwise equality with R1 and R0

The four comparisons (§3.10), after the cluster had stopped:

| A | B | Output | Result | Exit | Time |
|---|---|---|---|---|---|
| `06-reference.db` | `10-reference.db` | [`10-compare-06-reference-10-reference.txt`](dk-and-rpk-migration/out/10-compare-06-reference-10-reference.txt) / [`.json`](dk-and-rpk-migration/out/10-compare-06-reference-10-reference.json) | **`RESULT: equal`** | **0** | 7 s |
| `06-reference.db` | `10-resumed.db` | [`10-compare-06-reference-10-resumed.txt`](dk-and-rpk-migration/out/10-compare-06-reference-10-resumed.txt) / [`.json`](dk-and-rpk-migration/out/10-compare-06-reference-10-resumed.json) | **`RESULT: equal`** | **0** | 7 s |
| `02-reference.db` | `10-reference.db` | [`10-compare-02-reference-10-reference.txt`](dk-and-rpk-migration/out/10-compare-02-reference-10-reference.txt) / [`.json`](dk-and-rpk-migration/out/10-compare-02-reference-10-reference.json) | **`RESULT: equal`** | **0** | 7 s |
| `10-reference.db` | `10-resumed.db` | [`10-compare-10-reference-10-resumed.txt`](dk-and-rpk-migration/out/10-compare-10-reference-10-resumed.txt) / [`.json`](dk-and-rpk-migration/out/10-compare-10-reference-10-resumed.json) | **`RESULT: equal`** | **0** | 7 s |

In each: 27 client tables, every row matched, none in one store only, no near miss, no problem. The
matched counts are R0's and R1's: 5 rows each in `ScalarModel`, `AdiabaticHistory`, `BBNData` and
their tags, 42617 in each of `ScalarModelValue` and `AdiabaticHistoryValue`, 7475 in
`BBNDataValue`, 21250 `redshift`, 5 `ExponentialCoupling` and `beta_value`, 6 `IntegrationSolver`.
**The only note** in the three comparisons with an R0 or R1 store is `A: LambdaCDM is listed as
replicated by the primary but no shard holds it; not compared`: R0's and R1's primaries list
`LambdaCDM`, R2's does not (P8; 08), so the note R1's comparisons gave for both stores is now given
for A only. `10-reference` against `10-resumed` has no note. `10-compare-02-reference-10-reference.txt`
differs from R1's `06-compare-02-reference-06-reference.txt` only in the line naming B and the
dropped B note; the JSON files differ only in `b` and `notes`.

**Why this is not a vacuous "equal".** The bookkeeping columns differ on every matched row in all
four comparisons, the same 29 columns as at R1: every `label` and `timestamp` of the three record
tables and their tags, `ScalarModel.compute_time` and its three RHS timings,
`AdiabaticHistory.compute_time`, `BBNData.BBN_compute_time` and `NP_compute_time`, and the
timestamps of every parameter table, the 21250 redshifts among them. So each pair was built by
separate jobs. Every science column, the value rows included, is compared by its stored bits, with
foreign keys resolved to the rows they name. The stores compared were written by two layers
(ChamPBH's and the package's), in two primary formats (absolute shard records; bare names and
`replication_in_flight`), on three clusters, two pools and three trees.

**So R2 equals R1 and R0 bit for bit**, and §4.3 holds again at this tree.

**The schema snapshot** of `10-reference.db` is
[`10-reference-schema.json`](dk-and-rpk-migration/out/10-reference-schema.json) (27 client tables).
`--schema` of the store against `08-standin-schema.json` gives **`RESULT: the schemas agree`**, and
against `02-reference-schema.json` the same. The three JSON files are equal in `format`, `version`
and `tables`, and differ only in `source`. 08 found no difference between the package's schema at
this registry and R0's, so "differs from R0's only as 08 listed" means agreeing with both, which it
does.

**The primary is the package's**, as expected (prompt §0): its own tables are `shards`,
`shard_key_config`, `shard_keys`, `replicated_tables` (17 names, no `LambdaCDM`), `sharded_tables`
and `replication_in_flight` (0 rows); its `shards` table names `10-reference-shard0000.db` …
`-shard0015.db`, bare; and a replicated row carries one timestamp on every shard (the first
`beta_value` row: 1 distinct timestamp over the 16 shards of `10-reference.db`, 16 over
`06-reference.db`'s).

### 3.7 L4 — against R1

**Counts and the resume's pattern.** §3.4: `reference` 5 / 5 / 5, R1's and R0's; `resumed-b` computes
exactly what `resumed-a` did not keep (0 / 5 / 5 here, 0 / 3 / 5 at R1, 0 / 2 / 5 at R0), with
lookups 5 / 5 / 5 (R1 5 / 3 / 5, R0 5 / 2 / 5): the scalar stage looks every stored model up again,
and the adiabatic stage's lookups equal its computes. The prune removed only unvalidated rows (one
here, three at R1, one at R0). The numbers differ with the signal's timing; the pattern is R1's.

**The printed lines keep their layout** ([`10-printed-lines.txt`](dk-and-rpk-migration/out/10-printed-lines.txt)).
The pool's lines were taken from `06-reference.log` and `10-reference.log` and normalised by 06's
method (its `06-printed-lines.txt` header).
- The banners and completion lines agree **in text and order**: 10 lines each, the three stage
  banners (and `main.py`'s `** BUILDING GRID OF MODELS TO SAMPLE`), each stage's completion line
  and queue summary. Against R0's `02-reference.log`, the same.
- **`10-reference.log` has no progress line, said plainly.** Every stage ended inside the pool's
  300 s notify interval (the scalar stage in 1m 54.6s; R1's ran 5m 6.61s and printed one pair), so
  the check that every R2 progress shape occurs in R1's log holds vacuously. RayPoolKit is
  unchanged since R1 (`eaef0ce6…`), whose progress pair R1 showed in R0's two shapes.
- In addition, `06-resumed-b.log` against `10-resumed-b.log`: 11 lines agree in text and order,
  the scalar stage's `(no compute items were evaluated - all assets read from datastore)` among
  them; neither has a progress line.
- The extraction is not a check that cannot fail: `10-reference.log` against `10-resumed-b.log` (a
  different line sequence) is reported as differing, exit 1.

`stage_counts.py` parsed all three of 10's logs (§3.4), which is part of the same evidence.

### 3.8 H1 — the stores by the package's tools

On closed stores, after the cluster's stop, with the installed `datastorekit` 0.3.0 (the tag's
files).
1. **The shard-key audit.** `./venv/bin/python -m datastorekit.tools.shard_key_audit
   $R/10-reference.db`, and the same on `10-resumed.db`: each `current-generation schema (key_serial
   present)`, `shard_keys row count: 5`, `per-shard key distribution: {0: 1, 1: 1, 2: 1, 3: 1, 15:
   1}`, the cross-file check against its own shard #0 (`'beta_value' table (shard #0) row count:
   5`), **`VERDICT: OK`**, exit 0 ([`10-h1-audit-10-reference.txt`](dk-and-rpk-migration/out/10-h1-audit-10-reference.txt),
   [`10-h1-audit-10-resumed.txt`](dk-and-rpk-migration/out/10-h1-audit-10-resumed.txt)).
2. **The copy.** `./venv/bin/python -m datastorekit.tools.sharded_store copy $R/10-reference.db
   $R/10-reference-copy.db`: exit 0, `>> copy: "…/10-reference-copy.db" with 16 shards` and the
   16 shard paths `10-reference-copy-shard0000.db` … `-shard0015.db` ([`10-h1-copy.txt`](dk-and-rpk-migration/out/10-h1-copy.txt));
   17 files on disk. No `10-reference-copy*` file existed before.
3. **The copy opens read-only.** `main.py --config exponential.yaml --database
   $R/10-reference-copy.db --inventory`: exit 0, `read-only; 16 shards (#0, …, #15); 21,292 records
   in 21 classes`, **`problems: none`**; from its third line on it is byte-identical to
   `1b-inventory-reference.log` (the copy keeps every row's timestamp). Its `shards` table
   (`sqlite3` `mode=ro`) names `10-reference-copy-shard0000.db` … `-shard0015.db` for serials 0–15:
   its own shards, a bare name each ([`10-h1-copy-inventory.txt`](dk-and-rpk-migration/out/10-h1-copy-inventory.txt)).
4. **It compares equal.** `tools/compare_stores.py $R/10-reference.db $R/10-reference-copy.db`:
   **`RESULT: equal`**, exit 0, 27 tables matched, and **no bookkeeping column differing**
   ([`10-h1-compare-10-reference-10-reference-copy.txt`](dk-and-rpk-migration/out/10-h1-compare-10-reference-10-reference-copy.txt) / [`.json`](dk-and-rpk-migration/out/10-h1-compare-10-reference-10-reference-copy.json)).
5. **It is deleted by the package's `delete_store`.** A scratch script (quoted in log 10), run as
   `./venv/bin/python -I <scratch>/delete_copy.py` from the repository root, listed the 17
   `10-reference-copy*` files on disk, called `ShardedPool.closed_store_files($R/10-reference-copy.db)`
   (17 paths: shards #0–#15, then the primary), checked that list equal to the copy's 17 files and
   to what was on disk, then called `ShardedPool.delete_store` on the same primary. It returned the
   same 17 paths in the same order, and **no `10-reference-copy*` file remains**
   ([`10-h1-delete.txt`](dk-and-rpk-migration/out/10-h1-delete.txt)). This is the one deletion under
   `$R` the campaign allows (README §3).

**H1 holds.**

### 3.9 DatastoreKit's G3

By DatastoreKit's ChamPBH checklist, item 9 (G3): the layer's 9 files are gone (seven deleted, two
emptied; 08, `857ec73`) and ChamPBH imports `datastorekit` at the pinned tag (`requirements.txt:17`,
`v0.3.0`; 07, `617a1dc`); its suites pass (18 / 114 / 97 at this tree, run before the cluster
started); and **a pilot store has been rebuilt through the package** and compared with R1's and R0's
by value: equal, bit for bit (§3.6). **G3 holds.** The record on DatastoreKit's own extraction board
is 11's draft and the user's commit (P16); its text is in log 10's "State handed to the next
prompt".

### 3.10 The commands, verbatim

From the repository root, with `R=~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration`:

```bash
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status
PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t .
PYTHONPATH=. ./venv/bin/python -m unittest discover -s Datastore/tests -t .
PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t . -p 'test_[a-f]*.py'
PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t . -p 'test_[g-z]*.py'

.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh start "dk-and-rpk-migration 10 (R2)"
./venv/bin/ray status --address 127.0.0.1:6380
./.documents/dk-and-rpk-migration/rehearsal/build.sh 10 reference reference 127.0.0.1:6380

# S, from 10-reference.db (mode=ro): for each f in $R/10-reference-shard*.db and T in
#   ScalarModel AdiabaticHistory BBNData, in Python's sqlite3 with uri=True:
#   connect("file:$f?mode=ro").execute("select serial, timestamp, validated, label from T")
# and the banner's time from Ray's task records:
./venv/bin/ray list tasks --address 127.0.0.1:6380 --limit 2000 --detail --format json

./.documents/dk-and-rpk-migration/rehearsal/build.sh 10 resumed resumed-a 127.0.0.1:6380
./venv/bin/python .documents/dk-and-rpk-migration/rehearsal/interrupt_when.py $R/logs/10-resumed-a.log $R/logs/10-resumed-a.pid --banner "** CALCULATE ADIABATIC TRANSGRESSION PARAMETERS" --after 1

./venv/bin/ray list actors --address 127.0.0.1:6380 --filter "state=ALIVE"
./venv/bin/ray list actors --address 127.0.0.1:6380 --limit 200
./venv/bin/ray list jobs --address 127.0.0.1:6380
./venv/bin/ray list tasks --address 127.0.0.1:6380 --limit 5000 --detail --format json --filter "job_id=02000000"

./.documents/dk-and-rpk-migration/rehearsal/build.sh 10 resumed resumed-b 127.0.0.1:6380 --prune-unvalidated
grep -h -A6 "INTEGRITY WARNING" /tmp/ray-cpbh/session_2*/logs/old/worker-*-03000000-*.out /tmp/ray-cpbh/session_2*/logs/worker-*-03000000-*.out
./.documents/dk-and-rpk-migration/rehearsal/run_readers.sh 10 resumed 127.0.0.1:6380
PYTHONPATH=. ./venv/bin/python -u main.py --config exponential.yaml --database $R/10-reference.db --inventory > $R/10-readers/1b-inventory-reference.log 2>&1
./venv/bin/ray list actors --address 127.0.0.1:6380 --filter "state=ALIVE"
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh stop
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status

PYTHONPATH=. ./venv/bin/python -u tools/compare_stores.py $R/02-reference.db $R/10-reference.db --json .documents/dk-and-rpk-migration/out/10-compare-02-reference-10-reference.json > .documents/dk-and-rpk-migration/out/10-compare-02-reference-10-reference.txt 2>&1
PYTHONPATH=. ./venv/bin/python -u tools/compare_stores.py $R/06-reference.db $R/10-reference.db --json .documents/dk-and-rpk-migration/out/10-compare-06-reference-10-reference.json > .documents/dk-and-rpk-migration/out/10-compare-06-reference-10-reference.txt 2>&1
PYTHONPATH=. ./venv/bin/python -u tools/compare_stores.py $R/06-reference.db $R/10-resumed.db --json .documents/dk-and-rpk-migration/out/10-compare-06-reference-10-resumed.json > .documents/dk-and-rpk-migration/out/10-compare-06-reference-10-resumed.txt 2>&1
PYTHONPATH=. ./venv/bin/python -u tools/compare_stores.py $R/10-reference.db $R/10-resumed.db --json .documents/dk-and-rpk-migration/out/10-compare-10-reference-10-resumed.json > .documents/dk-and-rpk-migration/out/10-compare-10-reference-10-resumed.txt 2>&1
./venv/bin/python tools/compare_stores.py --snapshot $R/10-reference.db --out .documents/dk-and-rpk-migration/out/10-reference-schema.json
./venv/bin/python tools/compare_stores.py --schema $R/10-reference.db --against .documents/dk-and-rpk-migration/out/08-standin-schema.json
./venv/bin/python tools/compare_stores.py --schema $R/10-reference.db --against .documents/dk-and-rpk-migration/out/02-reference-schema.json
./venv/bin/python .documents/dk-and-rpk-migration/rehearsal/stage_counts.py $R/logs/10-reference.log $R/logs/10-resumed-a.log $R/logs/10-resumed-b.log --json .documents/dk-and-rpk-migration/out/10-stage-counts.json > .documents/dk-and-rpk-migration/out/10-stage-counts.txt
shasum -a 256 $R/10-readers/plot_by_beta/QCD_Cosmology/histories.csv
cmp $R/10-readers/plot_by_beta/QCD_Cosmology/histories.csv $R/06-readers/plot_by_beta/QCD_Cosmology/histories.csv
cmp $R/10-readers/plot_by_beta/QCD_Cosmology/histories.csv $R/02-readers/plot_by_beta/QCD_Cosmology/histories.csv

./venv/bin/python -m datastorekit.tools.shard_key_audit $R/10-reference.db > .documents/dk-and-rpk-migration/out/10-h1-audit-10-reference.txt 2>&1
./venv/bin/python -m datastorekit.tools.shard_key_audit $R/10-resumed.db > .documents/dk-and-rpk-migration/out/10-h1-audit-10-resumed.txt 2>&1
./venv/bin/python -m datastorekit.tools.sharded_store copy $R/10-reference.db $R/10-reference-copy.db > .documents/dk-and-rpk-migration/out/10-h1-copy.txt 2>&1
PYTHONPATH=. ./venv/bin/python -u main.py --config exponential.yaml --database $R/10-reference-copy.db --inventory
sqlite3 "file:$R/10-reference-copy.db?mode=ro" "select * from shards order by 1;"
PYTHONPATH=. ./venv/bin/python -u tools/compare_stores.py $R/10-reference.db $R/10-reference-copy.db --json .documents/dk-and-rpk-migration/out/10-h1-compare-10-reference-10-reference-copy.json > .documents/dk-and-rpk-migration/out/10-h1-compare-10-reference-10-reference-copy.txt 2>&1
./venv/bin/python -I <scratch>/delete_copy.py > .documents/dk-and-rpk-migration/out/10-h1-delete.txt 2>&1
```

Each `build.sh` and `run_readers.sh` ran as a background task, waited on by its notification (for
`reference`, also a `Monitor` on the log's stage and `** driver exit status` lines);
`interrupt_when.py` ran as a second background task, started just after `resumed-a`'s `build.sh`.
The label counts are the same `mode=ro` select over both stores' shards, grouped by the label's
`dk-and-rpk-migration-10-<build>` prefix and `validated`. The printed lines were extracted and
compared by a scratch script, described in `10-printed-lines.txt`'s header.

**Left on disk under `$R`, none deleted but the copy** (README §3): `10-reference.db` and
`10-resumed.db`, each with 16 shards; `logs/10-{reference,resumed-a,resumed-b}.{log,pid}`;
`10-readers/`, with its `before/` copy, the six step logs (`1b-inventory-reference.log` among them),
`status.txt` and the plot outputs. 188 entries named `10-*` at all depths; the 376 entries present
before have their mtimes and sizes but the root and `logs/` directories, which gained the 10
entries. R0's and R1's files were opened only by `compare_stores.py`, `sqlite3` `mode=ro` and
`cmp`, and none has changed.

---

## 4. The close (prompt 11, 2026-10-13)

**Tree:** `ee79455`, whose code is `2623905`'s (a housekeeping commit that removed two unused imports
from `Datastore/tests/test_datastore_config.py`), and 11's commit differs from it in comments, two
documentation strings of `tools/compare_stores.py` and docstrings only (`ast`-equal with docstrings
removed, but for those two strings). Branch `dk-and-rpk-migration`. `datastorekit` 0.3.0 at
`2f5417ea…` and `raypoolkit` 0.2.0 at `eaef0ce6…`, neither editable; Ray 2.53.0, Python 3.13. 11
runs no build, opens no store and starts no Ray: §4.1 is read from §1–§3, §4.2 is measured, and
§4.3–§4.5 are records. The scripts of §4.2 read a commit, not the working tree, so they were run at
`ee79455`, the commit 11 starts from; the orchestrator re-runs them at 11's own commit.

### 4.1 R0, R1 and R2 side by side

From §1.2, §1.6, §2.2, §2.6, §3.2 and §3.6. The stores are under `~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/`
(README §3), 16 shards each, and are kept.

| | R0 (§1, prompt 02) | R1 (§2, prompt 06) | R2 (§3, prompt 10) |
|---|---|---|---|
| **Tree** | `a72ca5e` (code `d7791c5`): ChamPBH's own layer and `RayTools/` | `5bb2be6` (code `a4894ea`): ChamPBH's own layer, `raypoolkit` adopted, ChamPBH's data in `config/datastore.py` | `60e000c` (code `47cfcfb`): both packages adopted, `--inventory` read-only |
| **Packages** | none | `raypoolkit` 0.2.0 at `eaef0ce6…` | `raypoolkit` 0.2.0 at `eaef0ce6…` and `datastorekit` 0.3.0 at `2f5417ea…` |
| **Cluster** | Ray 2.53.0 default cluster (`/tmp/ray`, 6379) | Ray 2.53.0, ChamPBH's isolated cluster (`/tmp/ray-cpbh`, 6380; U5) | the same, 6380 |
| **Stores** | `02-reference.db`, `02-resumed.db` | `06-reference.db`, `06-resumed.db` | `10-reference.db`, `10-resumed.db` |
| **`reference` build** | exit **0** (10 min 43 s) | exit **0** (11 min 7 s) | exit **0** (3 min 33 s) |
| **`resumed-a` (killed by `SIGTERM`)** | exit **134** (7 min 48 s); stored 4 `AdiabaticHistory` rows, validated 3 | exit **134** (8 min 14 s); stored 5, validated 2 | exit **15** (2 min 57 s; Ray's own handler closed the pool); stored 1, validated 0 |
| **`resumed-b` (resume)** | exit **0** (1 min 35 s) | exit **0** (2 min 42 s) | exit **0** (1 min 1 s) |
| **Comparison, by `tools/compare_stores.py`** | `02-reference` against `02-resumed`: `RESULT: equal`, exit 0 (§4.3: bitwise reproducible) | `06-reference` and `06-resumed` each against `02-reference`: `RESULT: equal`, exit 0; and against each other | `10-reference` and `10-resumed` each against `06-reference`, `10-reference` against `02-reference` and against `10-resumed`: `RESULT: equal`, exit 0 |
| **Schema** (`--schema`) | `02-reference-schema.json` (27 client tables) | agrees with R0's (differs in `source` only) | agrees with `08-standin-schema.json` and with R0's (`source` only) |
| **Readers** | all steps exit 0 | all steps exit 0; `histories.csv` equal to R0's | all steps exit 0; `histories.csv` SHA-256 `d7c62c37…c522`, `cmp`-equal to R1's and R0's |
| **Landed** | `6cbf953` | `c9e6bc2` | `b0d258a` |

Each comparison reads every stored science column by its bits and resolves foreign keys to the rows
they name; the bookkeeping columns (labels, timestamps, timings; 29 of them at R1 and R2) differ on
every matched row, so each pair was built by separate jobs. The one note in the R1 and R2 comparisons
is `LambdaCDM`'s, listed as replicated by R0's and R1's primaries and held by no shard (P8). So
**R2 equals R1 equals R0, bit for bit**, which is the criterion README §4.3 and P4 set.

### 4.2 The final measurements (Z4)

At `ee79455`, from the files `prompts/dk-and-rpk-migration/measure/out/11-*`, against the figures the
prompt took at `1954adf` (the code of `2623905`).

| Measure | At `1954adf` (prompt's §1.1) | At `ee79455` |
|---|---|---|
| **DatastoreKit's `measure_client_imports.py`**: layer modules absent | 7 (the two `__init__.py` present) | 7 absent, the two `__init__.py` present |
| `.py` files read / not parsed | 204 / 0 | 204 / 0 |
| import statements naming a layer module | 1, in `prompts/run-integrity/planning-probes/datastore_version_probe.py:18`; 0 outside `prompts/` | 1, the same line; 0 outside `prompts/` |
| string literals / `import_module` calls | 0 / 0 | 0 / 0 |
| factory-module imports | 18 (17 in `config/`, 1 top level) | 18 (17 in `config/`, 1 top level) |
| imports of the moved names | 0 | 0 |
| **RayPoolKit's `measure_client_pools.py`**: `RayTools/` | absent | absent |
| imports / string literals / `import_module` or `__import__` calls | 0 / 0 / 0 | 0 / 0 / 0 |
| `RayWorkPool(...)` calls; verdicts | 17; `constructs` 17 | 17; `constructs` 17 |
| `ray.is_initialized()` after the verdicts | False | False |
| non-`.py` lines naming `RayTools` or `RayWorkPool` | 240 (25 in `.documents/`, 215 in `prompts/`) | 252 (25 in `.documents/`, 227 in `prompts/`) |
| **Suites**: `CosmologyModels/tests` | 18 | 18 before 11's edits, 18 after |
| `ComputeTargets/tests` (`-p 'test_[a-f]*.py'` + `-p 'test_[g-z]*.py'`) | 114 (61 + 53) | 114 (61 + 53) before, 114 (61 + 53) after |
| `Datastore/tests` | 97 | 97 before, 97 after |
| **The venv**: `pip freeze` | 101 lines | 101 lines |
| `datastorekit`, `raypoolkit` | at `2f5417ea…`, at `eaef0ce6…` | at `2f5417ea76cdcd0d15d64d80d9085429bfacad21`, at `eaef0ce60e2fc354bb32f0ad576010e6a83c1976`, both `git+https` pins, not editable |
| `pip check` | clean | `No broken requirements found.` |

- **The non-`.py` count moves by 12, all in `prompts/`**: 11's own prompt file and the
  orchestrator's notes that name the pool, committed after `1954adf`. The script counts `prompts/`
  records as lines, and they are history (P14). The `.documents/` figure (25 in 3 files, of which
  `architecture-summary.md`'s 20) is of `ee79455`, before 11's documents: this §4 and
  `review-remediation-verification.md` §4.12 name `RayTools` and `RayWorkPool` too, and the
  orchestrator's re-run at 11's own commit counts them.
- **The venv** is 07's: the sorted `pip freeze` differs from the baseline `52142d7-pip-freeze.txt`
  (99 lines) by exactly the two added lines, `datastorekit` and `raypoolkit`; and
  `utilities.format_package_provenance(installed_package_provenance())` prints exactly log 07's
  line: `>> packages: datastorekit 0.3.0 (https://github.com/ds283/DatastoreKit@2f5417ea76cdcd0d15d64d80d9085429bfacad21, requested v0.3.0); raypoolkit 0.2.0 (https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976, requested v0.2.0)`.
- **`Datastore/tests`, by module** (the three roots' totals above): `test_compare_stores` 21, `test_datastore_config` 14, `test_inventory_report` 13, `test_inventory_specs` 10, `test_package_provenance` 10, `test_version_keyed_lookups` 9, `test_bbn_failure_lookup` 6, `test_first_bounce_round_trip` 5, `test_fixed_T_values_round_trip` 5, `test_scalarmodel_failure_reason` 4 (97).
- **The stale-path sweep** (prompt 11 §2.1) printed 19 lines before 11's edits, in 8 files, and prints
  10 after: the 8 in dated records and in the guard against the old layer's arguments that stay,
  and the 2 lines of the dated note that opens §4 of `architecture-summary.md`, which name the
  deleted modules as deleted (log 11).

### 4.3 DatastoreKit's G3, drafted for its board (P16)

For the cell `| **G3**: CPBH adopted `v0.2.3` or later (…) | … |` in DatastoreKit's
`prompts/extraction/IMPLEMENTATION_STATE.md` §2 (at `04a2ed9`, where it begins `⬜` and carries
dated italic notes). The text is in the form of that board's G2 cell, `✅ <date>: holds. …`, mapping
each part of the checklist's §9 (and its `v0.3.0` addendum, "`v0.2.3` or later") to the ChamPBH
commit and rehearsal that hold it, from log 10's record, and then **the cell's own notes, verbatim**
(the notes are copied from the board, not retyped). **Neither this cell nor the next is committed in
its repository: the user makes those commits, or has a session there make them.** The record's
`ee79455` is the commit the measurement was taken at; the orchestrator re-runs the scripts at 11's own
commit.

```text
✅ 2026-10-13: holds. CPBH adopted `v0.3.0` (the pin at CPBH `617a1dc`, installed at `2f5417ea…`, not editable) in its campaign `dk-and-rpk-migration`, and a rehearsal through the package reproduces, bit for bit, the rehearsals made before the swap. Against the checklist, [`docs/adoption/champbh.md`](../../docs/adoption/champbh.md) §9, with its `v0.3.0` addendum ("CPBH has adopted `v0.2.3` or later"): (1) **the 9 layer files are deleted and the tag imported**: the 7 modules removed and the two `__init__.py` emptied (0 bytes, P6) by CPBH `857ec73`; `docs/extraction/measure_client_imports.py cpbh --commit ee79455`, run from this repository with its own venv, finds the 7 absent and, outside CPBH's `prompts/` record, 0 import statements, 0 string literals, 0 `import_module` calls and 0 imports of the moved names naming a layer module (the 1 statement it counts is in `prompts/`, `prompts/run-integrity/planning-probes/datastore_version_probe.py:18`, a probe kept as history); `requirements.txt:17` holds `datastorekit @ git+https://github.com/ds283/DatastoreKit@v0.3.0`, installed at `2f5417ea76cdcd0d15d64d80d9085429bfacad21`, not editable, `pip check` clean (CPBH `617a1dc`); (2) **its suites pass**: `CosmologyModels/tests` 18, `ComputeTargets/tests` 114, `Datastore/tests` 97, all OK, at CPBH `60e000c` (R2's tree) and again at 11's tree (`prompts/dk-and-rpk-migration/measure/out/11-suites.txt`: 18 / 114 / 97); (3) **a pilot store is rebuilt through the package**: R2, CPBH `b0d258a`, the user's 2026-10-02 pilot grid (five histories at M = 10⁻³ M_P) built twice under Ray 2.53.0 through the package's `ShardedPool`, `Datastore` actors and `SerialPoolBroker`, once straight through and once killed and resumed with `--prune-unvalidated`, and read by every reader; `10-reference.db` and `10-resumed.db` each equal R1's `06-reference.db` (CPBH's own layer, before the swap) and `10-reference.db` equals R0's `02-reference.db` (the start tree), in every stored science column, by CPBH's `tools/compare_stores.py` (`RESULT: equal`, exit 0), and the schema equals R0's. Beyond §9: the package's prune at open removed exactly the unvalidated row a killed build left; `main.py --inventory` reads the stores through `read_inventory` with `problems: none`; `python -m datastorekit.tools.shard_key_audit` gives `VERDICT: OK` on both; and a `python -m datastorekit.tools.sharded_store copy` opens read-only, compares equal and is deleted by `ShardedPool.delete_store`, its 17 files and nothing else. CPBH's record is `.documents/dk-and-rpk-migration-verification.md` §3 and §4 (campaign `dk-and-rpk-migration`). *(2026-10-09: the adoption checklist is [`docs/adoption/champbh.md`](../../docs/adoption/champbh.md), measured at CPBH `52142d7` by 07a.)* *(2026-10-09: `v0.2.0` is made, annotated, on 06's commit `240028e` (tag object `9eaf542`) and pushed, after CI passed there at both ends, [run 37934881027](https://github.com/ds283/DatastoreKit/actions/runs/37934881027): Python 3.12.15 / Ray 2.43.0 / SQLAlchemy 2.0.39 and 3.13.16 / 2.55.1 / 2.0.46, SQLite 3.45.1 at both, each `Ran 458 tests … OK`. CPBH may now adopt it.)* *(2026-10-10: `v0.2.1` is ready to be tagged on extraction prompt 10's commit once CI passes there at both ends, as U23 and U28; 10 makes no tag and pushes nothing. CPBH's addendum is [`docs/adoption/champbh.md`](../../docs/adoption/champbh.md#addendum-v021-extraction-prompt-10-2026-10-10).)* *(2026-10-10: `v0.2.1` is made, annotated, on 10's commit `33778b0` (tag object `0ba2e4d`) and pushed, after CI passed there at both ends, [run 38054748108](https://github.com/ds283/DatastoreKit/actions/runs/38054748108): Python 3.12.15 / Ray 2.43.0 / SQLAlchemy 2.0.39 and 3.13.16 / 2.55.1 / 2.0.46, SQLite 3.45.1 at both, each `Ran 486 tests … OK`. CPBH may now adopt it.)* *(2026-10-11: `v0.2.2` is ready to be tagged on `actor-names` prompt 02's commit once CI passes there at both ends, as U23 and U28; 02 makes no tag and pushes nothing. CPBH's addendum is [`docs/adoption/champbh.md`](../../docs/adoption/champbh.md#addendum-v022-actor-names-prompt-02-2026-10-11).)* *(2026-10-11: `v0.2.2` is made, annotated, on 02's commit `6d8ee9a` (tag object `c32d027`) and pushed, after CI passed there at both ends, [run 38131949852](https://github.com/ds283/DatastoreKit/actions/runs/38131949852): Python 3.12.15 / Ray 2.43.0 / SQLAlchemy 2.0.39 and 3.13.16 / 2.55.1 / 2.0.46, SQLite 3.45.1 at both, each `Ran 494 tests … OK`. CPBH may now adopt it.)* *(2026-10-11: `v0.2.3` is ready to be tagged on `factory-defaults` prompt 02's commit once CI passes there at both ends, as U23 and U28; 02 makes no tag and pushes nothing. CPBH's addendum is [`docs/adoption/champbh.md`](../../docs/adoption/champbh.md#addendum-v023-factory-defaults-prompt-02-2026-10-11).)* *(2026-10-11: `v0.2.3` is made, annotated, on `factory-defaults` 02's commit `0de2b16` (tag object `8dac562`) and pushed, after CI passed there at both ends, [run 38177954330](https://github.com/ds283/DatastoreKit/actions/runs/38177954330): Python 3.12.15 / Ray 2.43.0 / SQLAlchemy 2.0.39 and 3.13.16 / 2.55.1 / 2.0.46, SQLite 3.45.1 at both, each `Ran 504 tests … OK`. CPBH may now adopt it.)* *(2026-10-12: `v0.3.0` is ready to be tagged on `object-timestamp` prompt 02's commit once CI passes there at both ends, as U23 and U28; 02 makes no tag and pushes nothing. CPBH may stay on `v0.2.3` or move to `v0.3.0`, as it chooses (`object-timestamp` U6). CPBH's addendum is [`docs/adoption/champbh.md`](../../docs/adoption/champbh.md#addendum-v030-object-timestamp-prompt-02-2026-10-12).)* *(2026-10-12: `v0.3.0` is made, annotated, on `object-timestamp` 02's commit `2f5417e` (tag object `bf0a3b2`) and pushed, after CI passed there at both ends, [run 38220924369](https://github.com/ds283/DatastoreKit/actions/runs/38220924369): Python 3.12.15 / Ray 2.43.0 / SQLAlchemy 2.0.39 and 3.13.16 / 2.55.1 / 2.0.46, SQLite 3.45.1 at both, each `Ran 509 tests … OK`. CPBH may adopt `v0.2.3` or `v0.3.0`, as it chooses (`object-timestamp` U6).)*
```

### 4.4 RayPoolKit's G3, drafted for its board (P16)

For the row `| **G3** | not yet (CPBH adopts `v0.2.0`). Checklist: … |` in RayPoolKit's
`prompts/extraction/IMPLEMENTATION_STATE.md` §2 (at `1b84df0`), in the form of its G1 and G2 cells
(`**holds** <date>: …`): the five parts of the checklist's §8, items 1–5, each with its CPBH commit
(`303324b`, `2f61943`) and the scripts' figures, from log 04's record; R1 and R2 as the runs under
real Ray; and the checklist pointer the cell carries now. Item 6 of §8 is this record on that board.

```text
| **G3** | **holds** 2026-10-12: CPBH adopted `v0.2.0` in its campaign `dk-and-rpk-migration`, and the five parts of the checklist's item 8 hold. (1) `RayTools/` is gone: `git ls-tree 303324b RayTools` is empty (CPBH's prompt 04), and so it is at `ee79455`. (2) No import of it: `docs/adoption/measure_client_pools.py cpbh --commit 303324b`, run from this repository with its own venv, gives 0 imports, 0 string literals and 0 `import_module` / `__import__` calls naming `RayTools`, and the same at `ee79455`. (3) The pin is in place and the venv installed from it: `requirements.txt:80` (`:79` at `303324b`; CPBH's DatastoreKit pin, added at `617a1dc`, sits above it), `raypoolkit` 0.2.0 at `eaef0ce60e2fc354bb32f0ad576010e6a83c1976`, not editable (CPBH `2f61943`), recorded by the provenance line of every R1 and R2 build log. (4) CPBH's suites pass: at `303324b`, `CosmologyModels/tests` 18, `ComputeTargets/tests` 114, `Datastore/tests` 66, all OK; at `60e000c` 18 / 114 / 97 (the Datastore count after CPBH's DatastoreKit adoption), all OK, and the same at 11's tree (`prompts/dk-and-rpk-migration/measure/out/11-suites.txt`: 18 / 114 / 97). (5) Every site constructs: the same script gives 17 `RayWorkPool(...)` calls, every one `constructs`, at `303324b` and at `ee79455`, with `ray.is_initialized()` False; no call site changed (the three drivers' diff at `303324b` is the one import line each), the 14 lookup-only sites keep `store_handler=None` and the 3 compute sites use the defaults. R1 (CPBH `5bb2be6`) and R2 (CPBH `60e000c`) ran `raypoolkit` under real Ray 2.53.0, on ChamPBH's isolated cluster: three builds each, and three readers that start Ray at R1 and two at R2, every store equal to the start tree's, bit for bit, by `tools/compare_stores.py`, and the pool's printed lines in the start tree's layout (R1's progress lines in R0's shapes; none printed at R2's load). CPBH's record is `.documents/dk-and-rpk-migration-verification.md` §1–§4 (campaign `dk-and-rpk-migration`), and its handover §4.12 of `.documents/review-remediation-verification.md`. Checklist: [`docs/adoption/champbh.md`](../../docs/adoption/champbh.md), measured at CPBH `52142d7`; its item 8 is the acceptance |
```

Optional, in the manner of the short paragraph that followed each of SGK's G2 records under that
board's §2 (the line after "After the close"):

```text
**G3 recorded (2026-10-13).** CPBH adopted `v0.2.0` in its campaign `dk-and-rpk-migration` (prompts 03–04, run under real Ray by 06 and 10, closed by 11). The row above takes CPBH's draft. With `RayTools/` deleted in CPBH, U5 no longer constrains it; it still freezes SI's copy until G4.
```

### 4.5 What goes stale in the two checklists, and where it was done

**DatastoreKit's checklist §10** (`docs/adoption/champbh.md` at `v0.3.0`):

| Item | Where done |
|---|---|
| `CLAUDE.md:66`, the `Datastore/tests` command: the directory keeps only CPBH's own tests (its item 7) | **08** (`857ec73`): the 4 + 2 tests the package carries and the 3 layer-identity tests deleted (A7); the command is the same and **11** gives it its count (`CLAUDE.md`, 97) |
| `CLAUDE.md:68-72`: the undecorated actor is `datastorekit.SQL.Datastore.Datastore`, taking `replicated_tables` and `factories=`, and writing no version row | **08**: `CLAUDE.md` now says "through `datastorekit`'s undecorated actor (`standin_pool.DatastoreClass`) inside ChamPBH's stand-in cluster" (A6, A9); **11** checked it and corrected the five test modules' own docstrings (Z3) |
| `CLAUDE.md:74-75`: record the per-package test counts, and `CLAUDE.md` itself holds none | **11** (Z2): the three commands and ComputeTargets' two halves carry their counts |
| `.documents/architecture-summary.md`: the tree at `:38-48`, §4 (`:213-393`) and §§12–14 (from `:975`) | **04** did the pool's lines (W4); **11** (Z1): the tree, §4 rewritten for the package, §5.2, §9's example (the mapping form, 05) and §§12–14 |
| `[00-two-files-are-not-black-clean]` (`.documents/OPEN_ISSUES.md:48`) names the deleted `base.py` | **08**: the issue narrowed to the one file left (`OPEN_ISSUES.md`); its dated entry stays as the record |

**RayPoolKit's checklist §9**:

| Item | Where done |
|---|---|
| `architecture-summary.md:35-36`, the layout's `RayTools/` and `RayWorkPool.py` | **04** (W4): the line now names the package |
| `:153` and the constructor table from `:157` (`store_handler`'s old meaning; no `persist_handler` row) | **04**: §3.2 titled `RayWorkPool` (`raypoolkit`), the `pool` type, the `store_handler` row and a new `persist_handler` row |
| `:585`, "handles steps 3–4 automatically via its default `compute_handler` and `store_handler`" | **04**: it names the three defaults |
| `:648`, `:920-925`, `:1086-1094`, `:155`, `:244`, `:355`, `:829`, `:855`, `:856`, `:862`, `:868`, `:969`, `:1056`, `:1108`, `:1110`, `:1116`: true of the package | left, read line by line in **11**: all true of `raypoolkit` `v0.2.0` (log 11); the one inside §9's diagram, `payload={"small_network": True}`, is corrected by §9's dated notes and is not 11's |
| `:999`, "`RayWorkPool` (entire file)" among components reusable with no change | **11** (Z1): §12 now says "the `raypoolkit` package (`RayWorkPool`)" |
| `.documents/integrator-audit-2026-09-30/README.md:69` (`RayTools/RayWorkPool.py` calls `obj.store()`) | left: a dated audit, and the package's default store handler does the same |
| Campaign records: `prompts/`, 21 lines in 11 files | left: history (P14) |

### 4.6 The commands, verbatim

From the repository root; the two scripts from their own repositories with their own venvs. `<sha>` is
`ee79455` in full.

```bash
git status --short                      # nothing tracked modified before 11's edits
for suite in Cosmology CTa CTb DS; do uptime; ./venv/bin/python -m unittest discover -s <root> -t . [-p <glob>]; done   # PYTHONPATH=.
./venv/bin/python -I <scratch>/stale_paths.py .                    # the sweep, before and after
./venv/bin/python -I <scratch>/ast_prose_only.py <HEAD copy> <file>  # each of the six Python files
(cd /Users/ds283/Documents/Code/DatastoreKit && ./venv/bin/python docs/extraction/measure_client_imports.py cpbh --commit <sha>)
(cd /Users/ds283/Documents/Code/RayPoolKit && ./venv/bin/python docs/adoption/measure_client_pools.py cpbh --commit <sha>)
./venv/bin/pip freeze; ./venv/bin/pip check
./venv/bin/python -I -c "import utilities; print(utilities.format_package_provenance(utilities.installed_package_provenance()))"
```
