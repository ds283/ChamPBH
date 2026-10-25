# ray-sqla-align — verification

**Campaign:** [`prompts/ray-sqla-align/`](../prompts/ray-sqla-align/README.md) ·
**Board:** [`IMPLEMENTATION_STATE.md`](../prompts/ray-sqla-align/IMPLEMENTATION_STATE.md) ·
**Outputs:** [`ray-sqla-align/out/`](ray-sqla-align/out/) ·
**Scripts:** [`dk-and-rpk-migration/rehearsal/`](dk-and-rpk-migration/rehearsal/) (`ray_cluster.sh`,
`interrupt_when.py`, `stage_counts.py`), run unchanged

This document records the campaign's rehearsal (prompt 02) in §1. 01's record is its log and its
outputs `ray-sqla-align/out/01-*`. §1 shows that under `ray` 2.59.0 and `SQLAlchemy` 2.0.55, on
ChamPBH's isolated cluster in Ray's token mode, a registered ChamPBH build stores what it stored
under `ray` 2.53.0 and `SQLAlchemy` 2.0.46 in `rrk-adoption` 04, through every path 04 exercised but
the hard kill: one clean build, one `SIGTERM`ed, its resume to the end, and `plot_by_beta.py` on the
clean store.

It is additive (`CLAUDE.md` invariant 6). A re-run that supersedes a figure gets a new dated
subsection, and the figures below stay as they are, correct for the tree they were taken on.

The stores, the run records and the launch logs live in the repository's `var/`, ignored by git:
the stores in `var/datastores/`, the runs in `var/runs/`, the launch logs, pid files and working
outputs in `var/ray-sqla-align-02/`. They are kept; retiring the stores is the user's (README §3).
Only small text outputs are committed, under `ray-sqla-align/out/02-*`. Every claim below is a line
in one of them, or in a command's output quoted here.

---

## 1. The rehearsal (prompt 02, 2026-10-25)

**Tree:** `b16c435` (`b16c4355ff91adbf8d27a08e5956e0020fc0a716`, the orchestrator's note for 02, on
top of `1bb2f6d`), whose code is 01's `668d30f`: `git diff 668d30f HEAD -- '*.py' '*.sh'
requirements.txt runregistrykit.toml` is empty, and `git diff --name-only 668d30f HEAD` names the
board, 01's log and the note alone. `main.py` SHA-256
`685b615bc85b3fbfa6ceea30f4a68865f5d97a5ca8e0cf39d980247df1307bbe`. `git status --porcelain
--untracked-files=all` empty at the Gate and after the suites at the end. Python 3.13.16; `ray`
2.59.0, `SQLAlchemy` 2.0.55, numpy 2.3.5, scipy 1.17.0; `datastorekit` 0.4.0 at `c8570e84…`,
`raypoolkit` 0.3.0 at `67a75b97…`, `runregistrykit` 0.2.0 at `0bae3a3c…`, none editable. The venv's
`pip freeze`, 102 lines, is `cmp`-equal to 01's `freeze-after.txt` at the Gate and to itself after
the stop (`cmp` exit 0 both times); `pip check`: `No broken requirements found.`
Output: [`02-gate.txt`](ray-sqla-align/out/02-gate.txt), [`02-after.txt`](ray-sqla-align/out/02-after.txt).

**Result: gate FP holds, and so does gate Token's 02 part.**
- `reference` ran to `done`/0; `resumed-a` was recorded `killed`/143, its handler's record in the
  same second as the signal; `resumed-c` ran to `done`/0, computed only the two missing scalar
  histories, and wrote its fingerprint to the sidecar.
- **Both stores fingerprint to `2dd4b39b61b9cc1b39bfcf5068ca2d00e84b52fee9da61958a11781931b0932f`**,
  `rrk-adoption` 04's: in `reference`'s and `resumed-c`'s statuses, in both sidecars, and by `store
  fingerprint` on each closed store. Each of the 21 classes' counts and digests is as in 04's
  committed `store fingerprint` tables (`diff` empty, §1.6).
- `compare_stores` of the two: **`RESULT: equal`**, `problems: none`; its whole table and bookkeeping
  list are 04's (`diff` empty).
- **`histories.csv`** from the reference store: SHA-256
  **`d7c62c37d8ad637a63854583ab11bc737109dcbfba92168dbddbaea75182c522`**, R0's and 04's. The plot left
  every file of the reference store unchanged.
- `start` printed no token warning; the cluster's `gcs_server` and `raylet` ran with
  `RAY_AUTH_MODE=token`; every driver attached; no `start` output or launch or reader log holds
  `Token authentication is enabled`, `Generated new authentication token`, `InvalidAuthToken` or
  `AuthenticationError`; `~/.ray/auth_token` is unchanged, and no record of the campaign holds it.
- Every manifest: `git_head` `b16c4355…`, `git_dirty` **false**, `script_sha256` `main.py`'s,
  `packages` the seven with `ray` 2.59.0 and `SQLAlchemy` 2.0.55.
- No Ray worker or actor of any job was alive after its driver ended, and nothing of the cluster
  outlived the stop.

### 1.1 The cluster and the load

- 10 CPUs, host `mps026088` (`mps026088.phys.susx.ac.uk` in the manifests).
- **The load.** At the Gate it was other workloads'; no Ray process of the cluster was on the
  machine. During each build it is the builds' own, recorded and not acted on (the note's ruling 12):

  | When (GMT) | Load average (1 / 5 / 15 min) |
  |---|---|
  | 20:53:41, the Gate | 2.44 / 3.37 / 5.55 |
  | 20:54 – 21:00, the suites | 3.54 → 11.89 (1 min, before each root) |
  | 21:01:04, before `start` | 8.07 / 5.49 / 5.50 |
  | 21:01:48, `reference` launched | 5.48 / 5.14 / 5.37 |
  | 21:05:29, `reference` ended | 10.84 / 11.90 / 8.67 |
  | 21:06:10, `resumed-a` launched | 6.98 / 10.78 / 8.41 |
  | 21:08:51, `resumed-a` ended | 10.12 / 12.86 / 9.84 |
  | 21:09:15, `resumed-c` launched | 7.20 / 11.95 / 9.60 |
  | 21:12:32, `resumed-c` ended | 14.55 / 11.63 / 9.81 |
  | 21:12:42, `plot_by_beta.py` | 13.01 / 11.40 / 9.74 |
  | 21:13:23, before `stop` | 8.35 / 10.42 / 9.46 |
  | 21:15 – 21:22, the suites after | 37.04 → 8.00 (1 min, before each root) |

  The 37.04 at 21:15 came after the stop, with no ChamPBH process left: another workload's.
- **The start**, at 21:01:04, from the repository root, alone in its Bash call:
  `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh start "ray-sqla-align 02 (R)" >
  var/ray-sqla-align-02/start.txt 2>&1; echo $?`: **exit 0**, ending
  ```text
  recorded pid 10995 in /Users/ds283/.raypoolkit/rehearsal-lock/owner
  >> ray_cluster: address 127.0.0.1:6380
  >> ray_cluster: dashboard http://127.0.0.1:8267
  >> ray_cluster: actors: RAY_AUTH_MODE=token ./venv/bin/ray list actors --address 127.0.0.1:6380
  >> ray_cluster: started in 8s; gcs_server pid 10995
  ```
  `grep -c` of `Token authentication is enabled`, `Generated new authentication token`,
  `InvalidAuthToken` and `AuthenticationError` in `start.txt`: 0 each; `grep -c -i warn`: 0.
- `ray_cluster.sh status`, exit 0: the lock held, `purpose: ray-sqla-align 02 (R)`, `acquired:
  2026-10-25T21:01:08+00:00`, owner pid 10995 alive; 19 processes; the four ports held. The
  `raylet` is pid 11031 in that listing.
- **The environments**, by pid, `ps -E -ww -o command= -p <pid> | grep -o 'RAY_AUTH_MODE=[a-z]*'`
  and the control `… | grep -c 'PATH='`: `gcs_server` 10995 `RAY_AUTH_MODE=token`, control 1;
  `raylet` 11031 `RAY_AUTH_MODE=token`, control 1. No environment was printed whole.
- `RAY_AUTH_MODE=token ./venv/bin/ray status --address 127.0.0.1:6380`, exit 0: one active node,
  no failures, `0.0/10.0 CPU`, `0.0/1.0 GPU`, `0B/7.03GiB memory`, `0B/2.00GiB object_store_memory`.
- **The Ray state** (`ray_state.py`, the note's ruling 11):
  - after `start`: no job, no worker, no actor;
  - after `resumed-a`'s launcher line (21:08:56): jobs `01000000` (`reference`, driver pid 11468)
    and `02000000` (`resumed-a`, 12370), both `SUCCEEDED`; workers 44 recorded, **0 alive**; actors
    34 recorded, **0 `ALIVE`**: each job's 16 `Datastore` and one `SerialPoolBroker`, all `DEAD`.
    So nothing of the `SIGTERM`ed job was left to wait for;
  - after the plot, before `stop` (21:13:16): four jobs, all `SUCCEEDED`, `03000000` `resumed-c`
    (13137) and `04000000` `plot_by_beta.py` (13927) added; workers 86 recorded, 0 alive; actors 67
    recorded, 0 `ALIVE`: 16 `Datastore` for each of the four jobs and a `SerialPoolBroker` for each
    of the three builds.
  Ray gives the `SIGTERM`ed driver's job `SUCCEEDED`, as 04 found.
- **The stop**, at 21:13, with every driver's pid answering `kill -0` with `no such process`
  (11468, 12370, 13137) and `interrupt_when.py`'s `sent` line printed: `ray_cluster.sh stop >
  var/ray-sqla-align-02/stop.txt 2>&1; echo $?`, **exit 0**: `SIGTERM to 19 process(es)` (the 19 of
  `status`'s listing, 10995–11037), `grace period of 15s over; SIGKILL to: 10995 11000`, `no process
  names /tmp/ray-cpbh; ports 6380 6381 8267 52367 are free`, `released the rehearsal lock`, `stopped
  in 20s`. Then `ray_cluster.sh status`, **exit 0**: `lock: free (/Users/ds283/.raypoolkit/rehearsal-lock
  does not exist)`, `Ray processes of /tmp/ray-cpbh: none`, nothing `stop` would signal, the four
  ports free; and `./venv/bin/python -m raypoolkit.rehearsal_lock status`, exit 0, `lock: free`.
  `ray stop` was never run.

Outputs: [`02-cluster.txt`](ray-sqla-align/out/02-cluster.txt),
[`02-ray-state.txt`](ray-sqla-align/out/02-ray-state.txt), [`02-load.txt`](ray-sqla-align/out/02-load.txt).

### 1.2 The builds

Each launched detached through `sh` (§1.10), checked once, and waited on by `wait_for_line.py`,
which reads the launch log only. Wall time is from the launch to the launcher's line as the waiter
saw it (±1 s). The fingerprint's time is from `taken.when` to the final `status.json` heartbeat.

| Run id | Slug | Store | pid | State | Exit | Launched → ended (GMT) | Wall | Load at launch | `results_store_id` | Fingerprint at finish |
|---|---|---|---|---|---|---|---|---|---|---|
| `ray-sqla-align-02-reference-20261025T210149` | `reference` | `reference` | 11468 | `done` | 0 | 21:01:48 → 21:05:24 | 3 min 36 s | 5.48 | `null` | `2dd4b39b…932f`, 21:05:16 → 21:05:23, ≤ 7 s |
| `ray-sqla-align-02-resumed-a-20261025T210611` | `resumed-a` | `resumed` | 12370 | `killed` | 143 | 21:06:10 → 21:08:45 | 2 min 35 s | 6.98 | `null` | `0d612337…0f13`, 21:08:36 → 21:08:44, ≤ 8 s |
| `ray-sqla-align-02-resumed-c-20261025T210916` | `resumed-c` | `resumed` | 13137 | `done` | 0 | 21:09:15 → 21:12:27 | 3 min 12 s | 7.20 | `9919583b8b04499da3e286c0814a0114` | `2dd4b39b…932f`, 21:12:19 → 21:12:26, ≤ 7 s |

The stages' times, from each log's `ALL WORK ITEMS COMPLETE` lines:

| Build | Scalar | Adiabatic | BBN | `stage_counts.py` (lookup, compute, store, validation) |
|---|---|---|---|---|
| `reference` | 2m 9.96s | 2.11s | 23s | 5 / 5 / 5 / 5 in each stage, exit status 0 |
| `resumed-a` | — | — | — | scalar `INCOMPLETE; no queue summary`, exit status 143 |
| `resumed-c` | 2m 2.99s | 2.91s | 23.9s | scalar 5 / **2** / **2** / **2**; adiabatic and BBN 5 / 5 / 5 / 5; exit status 0 |
| 04's `reference` (Ray 2.53.0) | 3m 46.5s | 9.48s | 32.9s | 5 / 5 / 5 / 5 |
| 04's `resumed-c` | 1m 51.1s | 5.07s | 30.7s | scalar 5 / 2 / 2 / 2; 5 / 5 / 5 / 5 |

No log has a `Traceback`. 04's builds ran at loads up to 68; these at up to about 15.

**The one check after each launch** (the note's ruling 4), in one Bash call each: the pid file
equalled `status.json`'s pid, the state was `running`, and `list` showed the run.

| Build | Check at | Heartbeat | Age | Stage | `list` | Log born | Manifest `created` / born |
|---|---|---|---|---|---|---|---|
| `reference` | 21:02:00 | 21:01:51 | 9 s | `Created sharded datastore "…/ray-sqla-align-02-reference.db" with 16 shards` | `running`, `AGE 12s`, `1 alive` | 21:01:48 | 21:01:49 / 21:01:49 |
| `resumed-a` | 21:06:18 | 21:06:12 | 6 s | `Created sharded datastore "…/ray-sqla-align-02-resumed.db" with 16 shards` | `running`, `AGE 7s`, `1 alive, 1 finished` | 21:06:10 | 21:06:11 / 21:06:11 |
| `resumed-c` | 21:09:19 | 21:09:17 | 2 s | `Opened existing sharded datastore "…/ray-sqla-align-02-resumed.db" with 16 shards` | `running`, `AGE 4s`, `1 alive, 2 finished` | 21:09:15 | 21:09:16 / 21:09:16 |

In every launch log the three `** registered_run:` lines are lines 1–3 and `>> packages:` is line 4,
so each manifest was written before `main.py`'s first line. The final status's last beat against the
launcher's line: 21:05:23 / 21:05:24, 21:08:44 / 21:08:45, 21:12:26 / 21:12:27.

**Each manifest** ([`02-<slug>.manifest.json`](ray-sqla-align/out/), summarised in
[`02-manifests.txt`](ray-sqla-align/out/02-manifests.txt)):
- `script` `main.py`, `script_sha256` `685b615b…7bbe`, which is `shasum -a 256 main.py`;
- `git_head` `b16c4355ff91adbf8d27a08e5956e0020fc0a716`, the dispatch commit; `git_dirty` **`false`**;
- `results` `var/datastores/ray-sqla-align-02-<store>.db`, relative to the client root;
  `results_store_id` as in the table;
- `packages` seven, each `editable: false`: `runregistrykit` 0.2.0 (`0bae3a3c…`, `v0.2.0`),
  `datastorekit` 0.4.0 (`c8570e84…`, `v0.4.0`), `raypoolkit` 0.3.0 (`67a75b97…`, `v0.3.0`), and,
  with no commit, URL or revision, `ray` 2.59.0, `SQLAlchemy` 2.0.55, `numpy` 2.3.5, `scipy` 1.17.0;
- `host` `id` `8d2b247824882486`, `arm64`, `Darwin` `27.0.0`, Python 3.13.16.

**Each driver's `>> packages:` line** (`reference`, `resumed-a`, `resumed-c` and the plot) is
byte-identical to 01's in `01-distributions.txt` (`cmp` 0):
```text
>> packages: datastorekit 0.4.0 (https://github.com/ds283/DatastoreKit@c8570e849334d09085bbd84876095fc9e22bd005, requested v0.4.0); raypoolkit 0.3.0 (https://github.com/ds283/RayPoolKit@67a75b97a656accb98195a4bda93f64e6a5f3fb2, requested v0.3.0); ray 2.59.0 (no recorded origin); SQLAlchemy 2.0.55 (no recorded origin); numpy 2.3.5 (no recorded origin); scipy 1.17.0 (no recorded origin)
```

**`reference`'s record.** Its log ends `** driver exit status 0`, `** launcher saw exit status 0`.
`list`: `done`, `1 finished`. `status.json`: `done`, `exit_code` 0, the fingerprint (digest
`2dd4b39b…932f`, `problems` `{}`, `taken` by its own run id with `git_dirty` false), and
`fingerprint_sidecar`: `not written: …/var/datastores/ray-sqla-align-02-reference.manifest.json is
not a problem-free registry sidecar (there is none)`, as there was none yet.

Outputs: the launch logs [`02-reference.log`](ray-sqla-align/out/02-reference.log),
[`02-resumed-a.log`](ray-sqla-align/out/02-resumed-a.log),
[`02-resumed-c.log`](ray-sqla-align/out/02-resumed-c.log); each run's `manifest.json` and
`status.json` whole; the checks [`02-checks.txt`](ray-sqla-align/out/02-checks.txt); the counts
[`02-stage-counts.txt`](ray-sqla-align/out/02-stage-counts.txt).

### 1.3 The delay for `resumed-a` (the note's ruling 6)

**The five `compute_time`s**, after `reference`'s `store create` and `store fingerprint --write`,
read from its shards through `sqlite3` with `file:…?mode=ro` URIs only
([`02-compute-times.txt`](ray-sqla-align/out/02-compute-times.txt)):

| β | `compute_time` | Steps | `validated` | Row's `timestamp` (GMT) |
|---|---|---|---|---|
| 1.15 | 6.65 s | 217426 | 1 | 21:02:51.4 |
| 1.1 | 6.75 s | 227521 | 1 | 21:02:52.2 |
| 1.2 | 7.66 s | 254491 | 1 | 21:02:52.5 |
| 10 | 42.45 s | 1075123 | 1 | 21:03:37.1 |
| 25 | 107.07 s | 2572397 | 1 | 21:04:47.6 |

The step counts are 04's, row for row. `reference`'s scalar banner is not timed in its log; from
the stage's 2m 9.96s ending just after the last row, it was at about 21:02:38, and the rows were
stored at about +14, +15, +15, +59 and +130 s, against the ruling's `compute_time` + 20 s (+27, +27,
+28, +62, +127).

**`<s>` = 90 s.** `t10 = 42.45` s and `t25 = 107.07` s; `t10 + 20 = 62.45` s and `t25 + 20 = 127.07`
s; the gap is 64.62 s, over the ruling's 60 s floor; `(t10 + t25)/2 + 20 = 94.76` s, which rounds to
**90 s**. Expected: β = 1.1, 1.15 and 1.2 stored and validated, β = 10 stored, β = 25 not.

**What `resumed-a` stored** (the same query on the resumed store after it, before its `store
create`): **four rows**, β = 1.15 (6.50 s), 1.1 (6.64 s) and 1.2 (7.45 s) with `validated 1`, and
β = 10 (43.70 s, stored 21:08:03.5) with **`validated 0`**. `interrupt_when.py` saw the banner at
21:07:06 UTC, so the rows were stored at about +10, +11, +12 and +57 s, and the signal came at +90
s, 33 s after β = 10's row was stored and before its validation. That is the expected outcome, and
04's: β = 10 stored and left unvalidated (04's for 65 s). `resumed-a`'s status fingerprint is
`0d612337e93525f107d5aed73b10fa14c337744b9e155057d7d8416ea34e0f13`, the digest 04's `resumed-a`
recorded: the partial store, too, holds what 04's held.

### 1.4 The signal (the note's ruling 7)

`interrupt_when.py` ([`02-resumed-a.interrupt.log`](ray-sqla-align/out/02-resumed-a.interrupt.log)),
in UTC:

```text
[2026-10-25T21:07:06+00:00] interrupt_when: saw "CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID"; waiting 90 s
[2026-10-25T21:08:36+00:00] interrupt_when: sent SIGTERM to pid 12370
```

`resumed-a`'s log, after the scalar banner:

```text
** SIGTERM at 2026-10-25 21:08:36 +0000: recorded killed, exit code 143; stopping main.py; the process will exit with status 143
** SIGTERM: main.py ended with SystemExit(1); recording killed, exit code 143
(Datastore pid=12447) @@ Parametrized equation-of-state LambdaCDM-like model "QCD equation of state based on Saikawa & Shirai parametrization (arXiv:1803.01038), with adjustments (splined) | Planck2018 68% central values TT+TE+EE+lowP+lensing+BAO" [repeated 15x across cluster] (Ray deduplicates logs by default. Set RAY_DEDUP_LOGS=0 to disable log deduplication, or see https://docs.ray.io/en/master/ray-observability/user-guides/configure-logging.html#log-deduplication for more options.)
** driver exit status 143
** launcher saw exit status 143
```

The same lines, in the same order, as 04's.
- **Signal → the `killed` record**: the handler's line, 21:08:36 +0000, against `sent SIGTERM` at
  21:08:36 UTC: **0 s**, to the one-second resolution of the two clocks. The expectation (0 or 1 s)
  is met.
- **Signal → the final record**: `status.json`'s heartbeat 21:08:44 +00:00 against 21:08:36:
  **8 s**. The fingerprint was `taken` at 21:08:36.
- `main.py` ended with `SystemExit(1)`, the known shape (04's and SI's), and the run is
  `killed`/143 and exited 143.
- No `*** SIGTERM received` banner and no `** cancelled an alarm` line (`grep -ci 'SIGTERM
  received\|alarm'` 0): under Ray 2.59.0, as under 2.53.0, the wrapper's handler, not Ray's, ran.
- `status.json` ([`02-resumed-a.status.json`](ray-sqla-align/out/02-resumed-a.status.json)):
  `killed`, `exit_code` 143, the fingerprint `0d612337…0f13`, `fingerprint_sidecar` "not written …
  (there is none)". `list`: `killed`, `2 finished`.
- Then `store create var/datastores/ray-sqla-align-02-resumed.db --purpose "ray-sqla-align 02 R2-R3:
  the pilot grid built by a resume after SIGTERM under ray 2.59.0, kept for the user"`, exit 0, on
  the closed store: `store_id` **`9919583b8b04499da3e286c0814a0114`**. `store show`: `resumed-a`,
  `killed`, `finished`, `by results`.

### 1.5 The resume and the stores

- **`resumed-c`**: `done`/0; each stage's `ALL WORK ITEMS COMPLETE`. Its open with
  `--prune-unvalidated` printed the prune report (the note's ruling 9), from the `Datastore` actor
  of `shard0002`:
  ```text
  (Datastore pid=13160) !! INTEGRITY WARNING (2026-10-25T21:09:21): datastore "shard0002-store" (physical file /Users/ds283/Documents/Code/ChamPBH/var/datastores/ray-sqla-align-02-resumed-shard0002.db)
  (Datastore pid=13160) >> ScalarModel instances
  (Datastore pid=13160)      The following unvalidated models were detected in the datastore:
  (Datastore pid=13160)        -- "ray-sqla-align-02-resumed-a-ScalarModel-ExponentialPotential(M=0.001Mp,Lambda=0.001eV)-ExponentialCoupling(beta=10)-2026-10-25T21:07:15" (store_id=61, log10_atol=-8.0, log10_rtol=-8.0)
  (Datastore pid=13160)           contains 9776 z-sample values | expected=9776
  (Datastore pid=13160)      ** Note: these values have been pruned from the datastore.
  ```
  So the scalar stage looked up 5 and computed, stored and validated **2** (β = 10 and 25); every
  adiabatic and BBN item was computed (5 / 5 / 5 / 5), since `resumed-a` reached neither stage. 04's
  `store_id=61` and `contains 9776 z-sample values | expected=9776` are the same.
  `fingerprint_sidecar` `"written"`. The fingerprint in its status, the sidecar's after it (`taken`
  by `ray-sqla-align-02-resumed-c-20261025T210916` at 21:12:19) and `store fingerprint` on the closed
  store are one digest, `2dd4b39b61b9cc1b39bfcf5068ca2d00e84b52fee9da61958a11781931b0932f`.
  `results_store_id` is the resumed store's id (the note's ruling 8).
- **`compare_stores`**: `PYTHONPATH=. ./venv/bin/python tools/compare_stores.py
  var/datastores/ray-sqla-align-02-reference.db var/datastores/ray-sqla-align-02-resumed.db`, exit 0,
  **`RESULT: equal`**, `problems: none`. Every table matched with 0 rows in either only, among them
  `ScalarModel` 5, `ScalarModelValue` 42617, `AdiabaticHistory` 5, `AdiabaticHistoryValue` 42617,
  `BBNData` 5, `BBNDataValue` 7475 and `redshift` 21250. The bookkeeping columns that differ, and are
  not compared, are 04's list: the timestamps, the labels (which carry the job name), and
  `ScalarModel.compute_time`, `mean_RHS_time`, `max_RHS_time`, `min_RHS_time`,
  `AdiabaticHistory.compute_time`, `BBNData.BBN_compute_time` and `NP_compute_time`. The output from
  `tables:` to `RESULT` is 04's `04-compare-stores.txt` line for line (`diff` empty). No BBN solve
  met `--bbn-wall-clock-limit` in one build and not the other: the BBN rows match.
- **`store fingerprint`**, without `--write`, on each closed store after the stop: the reference,
  digest `2dd4b39b…932f`, `problems: none`, "matches the fingerprint taken 2026-10-25T21:05:42+00:00
  by a person, at b16c4355…"; the resumed, the same digest, "matches the fingerprint taken
  2026-10-25T21:12:19+00:00 by run ray-sqla-align-02-resumed-c-20261025T210916, at b16c4355…". The
  two sidecars' `fingerprint.classes`, 21 classes each, compare equal as JSON. And each store's
  per-class table (count and digest of each class and tag set) is the one 04's `store fingerprint`
  printed for its store of the same name (`04-store.txt`; `diff` empty for both).
- **The reference's sidecar**: `store create var/datastores/ray-sqla-align-02-reference.db --purpose
  "ray-sqla-align 02 R1: the reference build of the pilot grid under ray 2.59.0, kept for the
  user"`, exit 0, `store_id` `16b5fa1051ce4a7090c2b6b370c6baf7`; `store fingerprint … --write`, exit
  0, digest `2dd4b39b…932f`, equal to `reference`'s status, "wrote the fingerprint into the sidecar;
  it replaced none". No run of this prompt carries the reference's id.
- **`store show`**, after the stop, the runs naming each store:
  ```text
  reference:  ray-sqla-align-02-reference-20261025T210149  done      finished  by results
  resumed:    ray-sqla-align-02-resumed-c-20261025T210916  done      finished  by results and results_store_id
              ray-sqla-align-02-resumed-a-20261025T210611  killed    finished  by results
  ```

Outputs: [`02-compare-stores.txt`](ray-sqla-align/out/02-compare-stores.txt),
[`02-store.txt`](ray-sqla-align/out/02-store.txt) (every `store` command's output whole),
[`02-reference-store.sidecar.json`](ray-sqla-align/out/02-reference-store.sidecar.json),
[`02-resumed-store.sidecar.json`](ray-sqla-align/out/02-resumed-store.sidecar.json).

### 1.6 `histories.csv` and what was written

- **`histories.csv`**: `plot_by_beta.py` on the reference store, unregistered, in the foreground,
  21:12:42 → 21:13:07 (25 s), exit 0. It printed `>> Opened existing sharded datastore
  "…/ray-sqla-align-02-reference.db" read-only with 16 shards (version label "2026.6.0", serial 1)`
  and the SM baseline line 04's plot printed (`Yp=0.24688, D/H x1e5=2.45829, …`, the line `diff`
  empty). `var/ray-sqla-align-02/plot-reference/QCD_Cosmology/histories.csv`: SHA-256
  **`d7c62c37d8ad637a63854583ab11bc737109dcbfba92168dbddbaea75182c522`**, R0's.
- **The reference store, unchanged by the plot**: `shasum -a 256` of its 17 files and its sidecar
  before and after the plot are equal (`diff` exit 0). [`02-plot.txt`](ray-sqla-align/out/02-plot.txt).
- **Written**: `var/datastores/` (the two stores, 17 files each, and their sidecars; 38 entries
  with `rrk-adoption` 04's two sidecars, which were not opened, and are dated Oct 20 08:19 as
  before), `var/runs/` (the three run directories), `var/ray-sqla-align-02/` (the launch logs, pid
  files, the signaller's log, the plot's log and outputs, the working outputs and the helpers), the
  lock under `~/.raypoolkit/`, taken by `start` and released by `stop`, and the cluster's temp dir.
  `var/datastores/` holds 88 MB. Nothing under `var/` was deleted. No other store was opened. The
  tree was clean throughout.

### 1.7 The token checks (gate Token, 02's part)

[`02-token.txt`](ray-sqla-align/out/02-token.txt).
- `grep -c` of `Token authentication is enabled`, `Generated new authentication token`,
  `InvalidAuthToken` and `AuthenticationError` in `start.txt`, the three launch logs and the plot
  log: **0 for each of the twenty**.
- The cluster's environments: §1.1, `RAY_AUTH_MODE=token` for the `gcs_server` and the `raylet`.
  Every driver attached (`Connecting to existing Ray cluster at address: 127.0.0.1:6380...` and
  `Connected to Ray cluster.` in each), which 01's V0 showed they cannot on the pins alone. In each
  build the wrapper imports `ray` (`RunDrivers/registered_run.py:814`) before `runpy` runs `main.py`,
  so `main.py`'s `import config.ray_auth` found Ray loaded and refreshed its config (README §0.2);
  that is read, not measured separately here.
- The token file, after the suites at the end: `-rw------- 1 ds283 staff 64 Oct 25 12:51
  /Users/ds283/.ray/auth_token`; `/usr/bin/stat -f '%m %Sm'`: `1792932677 Oct 25 12:51:17 2026`. As
  at the Gate and at 01. `env | grep -c RAY_AUTH_MODE` in the agent's shell: 0.
- `grep -r -F -l -f ~/.ray/auth_token .documents/ray-sqla-align/
  .documents/ray-sqla-align-verification.md prompts/ray-sqla-align/`, run after every record of the
  commit was written: no output, **exit 1** (the log, §Verification).

### 1.8 The suites

Serially, each in the foreground, never with the cluster up
([`02-suites.txt`](ray-sqla-align/out/02-suites.txt)); the time is `unittest`'s, the load the
one-minute load before the root:

| Root | Tests | Before `start` | After `stop` |
|---|---|---|---|
| CosmologyModels | 18 | OK, 79.2 s; 3.54 | OK, 82.6 s; 37.04 |
| ComputeTargets `test_[a-f]*` | 61 | OK, 115.1 s; 2.73 | OK, 114.6 s; 11.33 |
| ComputeTargets `test_[g-z]*` | 115 | OK, 121.9 s; 3.17 | OK, 130.6 s; 4.19 |
| ObjectFactories | 78 | OK, 8.8 s; 2.66 | OK, 9.7 s; 15.19 |
| tools | 102 | OK, 40.4 s; 2.78 | OK, 38.9 s; 13.27 |
| RunDrivers | 61 | OK, 26.8 s; 11.89 | OK, 28.0 s; 8.00 |

01's counts exactly. Nothing failed, so nothing was re-run.

### 1.9 Observed, not acted on

- **The stop needed `SIGKILL`** after its 15 s grace, for the `gcs_server` (10995) and 11000, a
  `Python -u` of the venv's Ray started with the head, as at 01's V1 (99355, 99362). The script then
  found no process and the four ports free, released the lock, and exited 0. Not identified further:
  that would take a command naming the cluster's temp dir.
- **`ray status` reports `0.0/1.0 GPU`** under Ray 2.59.0, as 01 and SI recorded. The fingerprints
  show that the builds did not depend on it.
- **β = 10's row was stored and left unvalidated** in `resumed-a` for the 33 s until the signal,
  as 04's was for 65 s (`rrk-adoption-verification.md` §1.11 gives the likely cause in the queue's
  `ray.wait` setting). Its cost is the history recomputed by `resumed-c`.
- **Ray's driver lines moved within `worker.py`**: `worker.py:1836` and `:2024` for 04's `:1821` and
  `:1998`, with the same text. Only line numbers.

### 1.10 The commands, verbatim

From the repository root. `<scratch>` is `var/ray-sqla-align-02/tools/`. 04's three helpers were
copied there unchanged (`cmp` 0 against `rrk-adoption/out/04-*.py.txt`) and are committed again
as [`02-wait_for_line.py.txt`](ray-sqla-align/out/02-wait_for_line.py.txt),
[`02-compute_times.py.txt`](ray-sqla-align/out/02-compute_times.py.txt) and
[`02-ray_state.py.txt`](ray-sqla-align/out/02-ray_state.py.txt); the three written here are
[`02-suite.sh.txt`](ray-sqla-align/out/02-suite.sh.txt) (one root, with its summary),
[`02-check.sh.txt`](ray-sqla-align/out/02-check.sh.txt) (the one check) and
[`02-log_kinds.py.txt`](ray-sqla-align/out/02-log_kinds.py.txt) (the log kinds). The committed
logs have ANSI colour codes removed (`sed -E 's/\x1b\[[0-9;]*m//g'`), and are otherwise verbatim;
the `.json` files are copied unchanged.

```bash
# the Gate
git status --porcelain --untracked-files=all | wc -l
git diff 668d30f HEAD -- '*.py' '*.sh' requirements.txt runregistrykit.toml | wc -l
shasum -a 256 main.py
./venv/bin/pip freeze > var/ray-sqla-align-02/pip-freeze-before.txt
cmp var/ray-sqla-align-02/pip-freeze-before.txt var/ray-sqla-align-01/freeze-after.txt
./venv/bin/pip check
./venv/bin/python -m runregistrykit list
ls -l ~/.ray/auth_token
/usr/bin/stat -f '%m %Sm' ~/.ray/auth_token
env | grep -c RAY_AUTH_MODE
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status > var/ray-sqla-align-02/gate-status.txt 2>&1; echo $?
./venv/bin/python -m raypoolkit.rehearsal_lock status > var/ray-sqla-align-02/gate-lock-status.txt 2>&1; echo $?

# the suites, each root in its own Bash call (<scratch>/suite.sh runs CLAUDE.md's command)
sh <scratch>/suite.sh before CosmologyModels CosmologyModels/tests
sh <scratch>/suite.sh before ComputeTargets-a-f ComputeTargets/tests 'test_[a-f]*.py'
sh <scratch>/suite.sh before ComputeTargets-g-z ComputeTargets/tests 'test_[g-z]*.py'
sh <scratch>/suite.sh before ObjectFactories ObjectFactories/tests
sh <scratch>/suite.sh before tools tools/tests
sh <scratch>/suite.sh before RunDrivers RunDrivers/tests

# the cluster: each command alone in its Bash call
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh start "ray-sqla-align 02 (R)" > var/ray-sqla-align-02/start.txt 2>&1; echo $?
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status > var/ray-sqla-align-02/status-up.txt 2>&1; echo $?
ps -E -ww -o command= -p 10995 | grep -o 'RAY_AUTH_MODE=[a-z]*'
ps -E -ww -o command= -p 10995 | grep -c 'PATH='
ps -E -ww -o command= -p 11031 | grep -o 'RAY_AUTH_MODE=[a-z]*'
ps -E -ww -o command= -p 11031 | grep -c 'PATH='
RAY_AUTH_MODE=token ./venv/bin/ray status --address 127.0.0.1:6380 > var/ray-sqla-align-02/ray-status.txt 2>&1; echo $?
RAY_AUTH_MODE=token ./venv/bin/python <scratch>/ray_state.py

# reference: one Bash call
export ADDRESS=127.0.0.1:6380 S=$PWD/var/datastores L=$PWD/var/ray-sqla-align-02 && nohup sh -c 'PYTHONPATH=. ./venv/bin/python -u RunDrivers/registered_run.py --campaign ray-sqla-align --prompt 02 --register reference --purpose "ray-sqla-align 02 R1: the pilot grid straight through under ray 2.59.0, the reference store" -- --config exponential.yaml --database $S/ray-sqla-align-02-reference.db --shards 16 --job-name ray-sqla-align-02-reference --M-values-Mp 1e-3 --beta-values 1.1 1.15 1.2 10 25 --ray-address $ADDRESS > $L/reference.log 2>&1 & echo $! > $L/reference.pid; wait $!; echo "** launcher saw exit status $?" >> $L/reference.log' > /dev/null 2>&1 &
./venv/bin/python <scratch>/wait_for_line.py var/ray-sqla-align-02/reference.log "** registered_run: registered as" --timeout 120
sh <scratch>/check.sh reference
./venv/bin/python <scratch>/wait_for_line.py var/ray-sqla-align-02/reference.log "** launcher saw exit status" --timeout 570
./venv/bin/python .documents/dk-and-rpk-migration/rehearsal/stage_counts.py var/ray-sqla-align-02/reference.log
./venv/bin/python -m runregistrykit list
./venv/bin/python -m runregistrykit store create var/datastores/ray-sqla-align-02-reference.db --purpose "ray-sqla-align 02 R1: the reference build of the pilot grid under ray 2.59.0, kept for the user"
./venv/bin/python -m runregistrykit store fingerprint var/datastores/ray-sqla-align-02-reference.db --write
./venv/bin/python -m runregistrykit store show var/datastores/ray-sqla-align-02-reference.db
./venv/bin/python <scratch>/compute_times.py var/datastores/ray-sqla-align-02-reference

# resumed-a: one Bash call, then interrupt_when.py in the next
export ADDRESS=127.0.0.1:6380 S=$PWD/var/datastores L=$PWD/var/ray-sqla-align-02 && nohup sh -c 'PYTHONPATH=. ./venv/bin/python -u RunDrivers/registered_run.py --campaign ray-sqla-align --prompt 02 --register resumed-a --purpose "ray-sqla-align 02 R2: the pilot grid into the resumed store under ray 2.59.0, interrupted with SIGTERM in the scalar stage" -- --config exponential.yaml --database $S/ray-sqla-align-02-resumed.db --shards 16 --job-name ray-sqla-align-02-resumed-a --M-values-Mp 1e-3 --beta-values 1.1 1.15 1.2 10 25 --ray-address $ADDRESS > $L/resumed-a.log 2>&1 & echo $! > $L/resumed-a.pid; wait $!; echo "** launcher saw exit status $?" >> $L/resumed-a.log' > /dev/null 2>&1 &
export L=$PWD/var/ray-sqla-align-02 && nohup ./venv/bin/python .documents/dk-and-rpk-migration/rehearsal/interrupt_when.py $L/resumed-a.log $L/resumed-a.pid --banner "CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID" --after 90 > $L/resumed-a.interrupt.log 2>&1 &
# then the registration wait, the one check and the end wait, as for reference
RAY_AUTH_MODE=token ./venv/bin/python <scratch>/ray_state.py
./venv/bin/python <scratch>/compute_times.py var/datastores/ray-sqla-align-02-resumed
./venv/bin/python -m runregistrykit store create var/datastores/ray-sqla-align-02-resumed.db --purpose "ray-sqla-align 02 R2-R3: the pilot grid built by a resume after SIGTERM under ray 2.59.0, kept for the user"
./venv/bin/python -m runregistrykit store show var/datastores/ray-sqla-align-02-resumed.db

# resumed-c: one Bash call
export ADDRESS=127.0.0.1:6380 S=$PWD/var/datastores L=$PWD/var/ray-sqla-align-02 && nohup sh -c 'PYTHONPATH=. ./venv/bin/python -u RunDrivers/registered_run.py --campaign ray-sqla-align --prompt 02 --register resumed-c --purpose "ray-sqla-align 02 R3: the resume of the resumed store under ray 2.59.0, straight through" -- --config exponential.yaml --database $S/ray-sqla-align-02-resumed.db --shards 16 --job-name ray-sqla-align-02-resumed-c --M-values-Mp 1e-3 --beta-values 1.1 1.15 1.2 10 25 --ray-address $ADDRESS --prune-unvalidated > $L/resumed-c.log 2>&1 & echo $! > $L/resumed-c.pid; wait $!; echo "** launcher saw exit status $?" >> $L/resumed-c.log' > /dev/null 2>&1 &
# then the registration wait, the one check and the end wait, as for reference

# the plot
shasum -a 256 var/datastores/ray-sqla-align-02-reference*
PYTHONPATH=. ./venv/bin/python -u plot_by_beta.py --config exponential.yaml --database $S/ray-sqla-align-02-reference.db --shards 16 --phi-init-Mp 5 --output $L/plot-reference --ray-address $ADDRESS > $L/plot-reference.log 2>&1
shasum -a 256 $L/plot-reference/QCD_Cosmology/histories.csv
shasum -a 256 var/datastores/ray-sqla-align-02-reference*
RAY_AUTH_MODE=token ./venv/bin/python <scratch>/ray_state.py
kill -0 11468; kill -0 12370; kill -0 13137

# the stop: each command alone in its Bash call
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh stop > var/ray-sqla-align-02/stop.txt 2>&1; echo $?
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status > var/ray-sqla-align-02/status-after.txt 2>&1; echo $?
./venv/bin/python -m raypoolkit.rehearsal_lock status > var/ray-sqla-align-02/lock-status-after.txt 2>&1; echo $?

# the comparisons
PYTHONPATH=. ./venv/bin/python tools/compare_stores.py var/datastores/ray-sqla-align-02-reference.db var/datastores/ray-sqla-align-02-resumed.db
./venv/bin/python -m runregistrykit store fingerprint var/datastores/ray-sqla-align-02-reference.db
./venv/bin/python -m runregistrykit store fingerprint var/datastores/ray-sqla-align-02-resumed.db
./venv/bin/python -m runregistrykit store show var/datastores/ray-sqla-align-02-reference.db
./venv/bin/python -m runregistrykit store show var/datastores/ray-sqla-align-02-resumed.db
./venv/bin/python .documents/dk-and-rpk-migration/rehearsal/stage_counts.py var/ray-sqla-align-02/reference.log var/ray-sqla-align-02/resumed-a.log var/ray-sqla-align-02/resumed-c.log
./venv/bin/python <scratch>/log_kinds.py var/ray-sqla-align-02/reference.log .documents/rrk-adoption/out/04-reference.log var/ray-sqla-align-02/resumed-a.log .documents/rrk-adoption/out/04-resumed-a.log var/ray-sqla-align-02/resumed-c.log .documents/rrk-adoption/out/04-resumed-c.log var/ray-sqla-align-02/plot-reference.log .documents/rrk-adoption/out/04-plot-reference.log

# the suites again, as above, with "after"; then
./venv/bin/pip freeze > var/ray-sqla-align-02/pip-freeze-after.txt
cmp var/ray-sqla-align-02/pip-freeze-before.txt var/ray-sqla-align-02/pip-freeze-after.txt
./venv/bin/python -m runregistrykit list
grep -r -F -l -f ~/.ray/auth_token .documents/ray-sqla-align/ .documents/ray-sqla-align-verification.md prompts/ray-sqla-align/
```
