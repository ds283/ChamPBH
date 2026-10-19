# rrk-adoption — verification

**Campaign:** [`prompts/rrk-adoption/`](../prompts/rrk-adoption/README.md) ·
**Board:** [`IMPLEMENTATION_STATE.md`](../prompts/rrk-adoption/IMPLEMENTATION_STATE.md) ·
**Outputs:** [`rrk-adoption/out/`](rrk-adoption/out/) ·
**Scripts:** [`dk-and-rpk-migration/rehearsal/`](dk-and-rpk-migration/rehearsal/) (`ray_cluster.sh`,
`interrupt_when.py`, `stage_counts.py`), run unchanged

This document records the campaign's rehearsal (prompt 04, by the user's ruling U5 for the hard-kill
route) in §1. 05 adds §2. It shows that under real Ray, on ChamPBH's isolated cluster, the wrapper
`RunDrivers/registered_run.py` records a build from before its first line to its end, and that the
registry's account of four builds is what happened to them: one clean, one `SIGTERM`ed, one
`SIGKILL`ed and ended by a person, and one resume to the end.

It is additive (`CLAUDE.md` invariant 6). A re-run that supersedes a figure gets a new dated
subsection, and the figures below stay as they are, correct for the tree they were taken on.

The stores, the run records and the launch logs live in the repository's `var/`, ignored by git
(README §3, R4): the stores in `var/datastores/`, the runs in `var/runs/`, the launch logs and pid
files in `var/rrk-adoption-04/`. They are kept; removing them is the user's. Only small text outputs
are committed, under `rrk-adoption/out/04-*`. Every claim below is a line in one of them, or in a
command's output quoted here.

---

## 1. The rehearsal (prompt 04, 2026-10-19)

**Tree:** `07aa626` (the orchestrator's note for 04, on top of `5d02258`), whose code is `d5dd930`'s
plus `5d02258`'s one-clause docstring change. `main.py` SHA-256
`243a50264cc1f694b1472a1c4afe51fe8622a5a2183f177638d96e2996037d59`, unchanged since `3cd2be4`
(`git diff 3cd2be4 -- main.py` empty, before and after). `datastorekit` 0.4.0 at `c8570e84…`,
`raypoolkit` 0.2.0 at `eaef0ce6…`, `runregistrykit` 0.2.0 at `0bae3a3c…`, none editable; Ray
2.53.0. The venv's `pip freeze`, 102 lines, is identical before the suites and after the stop
(`cmp` exit 0).

**Result: the gate "R" holds.**
- `reference` ran to `done`/0; `resumed-a` was recorded `killed`/143, with its handler's record in
  the same second as the signal; `resumed-b` was `SIGKILL`ed, showed `stale` with `!!`, made a
  further run on its store be refused with nothing created, and was ended `lost` by `end`;
  `resumed-c` ran to `done`/0, computed only the two missing histories, and wrote its fingerprint to
  the sidecar.
- Every manifest was written before `main.py`'s first line. `results_store_id` is `null` for the two
  runs begun with no sidecar and the resumed store's id for the two begun after `store create`.
- `compare_stores` of the reference and the resumed store: **`RESULT: equal`**, bookkeeping columns
  aside. Both stores' fingerprints are one digest,
  `2dd4b39b61b9cc1b39bfcf5068ca2d00e84b52fee9da61958a11781931b0932f`.
- `histories.csv` from the reference store: SHA-256
  `d7c62c37d8ad637a63854583ab11bc737109dcbfba92168dbddbaea75182c522`, R0's.
- Ray attached to the external cluster installs its own Python `SIGTERM` handler (the probe, §1.2),
  and the wrapper's replaced it: no Ray banner and no `** cancelled an alarm` line.
- No Ray worker, actor or `/tmp/ray-cpbh` process outlived any of the builds, the `SIGKILL`ed one
  included, and none outlived the stop.

### 1.1 The cluster and the load

- 10 CPUs, host `mps026088` (`mps026088.phys.susx.ac.uk` in the manifests).
- **The load.** At the gate the load was other Claude sessions'; no Ray process was on the machine.
  During each build it is the build's own, and it peaked as each build's BBN stage ended, as at the
  last rehearsal (100 there):

  | When (BST) | Load average (1 / 5 / 15 min) |
  |---|---|
  | 23:34:36, the gate | 6.08 / 7.14 / 7.47 |
  | 23:34:59 – 23:40, the suites | 6.39 → 8.04 (1 min) |
  | 23:40:32, before `start` | 7.63 / 6.35 / 6.88 |
  | 23:41:11, `reference` launched | 9.83 / 7.02 / 7.09 |
  | 23:46:51, `reference` ends | 68.58 / 41.95 / 24.92 |
  | 23:48:38, `resumed-a` launched | 15.59 / 30.77 / 22.58 |
  | 23:53:08, `resumed-a` ends | 28.36 / 70.65 / 47.99 |
  | 23:54:46, `resumed-b` launched | 8.26 / 52.28 / 43.26 |
  | 23:55:49, after its `SIGKILL` | 14.53 / 46.14 / 41.66 |
  | 23:56:46, `resumed-c` launched | 8.47 / 39.13 / 39.33 |
  | 23:59:51, `resumed-c` ends | 59.22 / 41.90 / 39.87 |
  | 00:00:48, `plot_by_beta.py` | 26.89 / 35.73 / 37.70 |
  | 00:01:28, before `stop` | 30.10 / 35.96 / 37.73 |
  | 00:02:11 – 00:07, the suites after | 16.68 → 3.57 (1 min) |

- **The start**, at 23:40:32, from the repository root:
  `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh start "rrk-adoption 04 (R)"`, output to
  a file (no pipe), **exit 0, 5 s**, ending
  ```text
  >> ray_cluster: address 127.0.0.1:6380
  >> ray_cluster: dashboard http://127.0.0.1:8267
  >> ray_cluster: actors: ./venv/bin/ray list actors --address 127.0.0.1:6380
  >> ray_cluster: started in 5s; gcs_server pid 40013
  ```
  The lock's owner file: `project: ChamPBH`, `purpose: rrk-adoption 04 (R)`,
  `acquired: 2026-10-19T23:40:37+01:00`, `host: mps026088`, `ray_temp_dir: /tmp/ray-cpbh`,
  `ray_address: 127.0.0.1:6380`, `pid: 40013`. `ray_cluster.sh status`, exit 0: 18 ChamPBH
  processes, the four ports held. `./venv/bin/ray status --address 127.0.0.1:6380`, exit 0: one
  active node, `0.0/10.0 CPU`, 8.19 GiB memory, 2.00 GiB object store, no failures. `ray list jobs`,
  `ray list workers --filter "is_alive=True"` and `ray list actors --filter "state=ALIVE"`: each
  `No resource in the cluster`.
- **Every driver that started Ray** logged `Connecting to existing Ray cluster at address:
  127.0.0.1:6380`: the probe, the four builds that reached `ray.init` and `plot_by_beta.py`. The
  refused `resumed-c` stopped before Ray.
- **The jobs**, at 00:01:28, before the stop, all `SUCCEEDED`: `01000000` the probe (pid 40396),
  `02000000` `reference` (40540), `03000000` `resumed-a` (43850), `04000000` `resumed-b` (46306),
  `05000000` `resumed-c` (47488), `06000000` `plot_by_beta.py` (49368). So the prompt's "four builds,
  one plotting run" is five Ray jobs, and the probe makes six (the note's ruling 8). Ray gives the
  `SIGKILL`ed driver's job `SUCCEEDED` too. Actors: 84 recorded, none `ALIVE`: 16 `Datastore` and
  one `SerialPoolBroker` for each of the four builds, 16 `Datastore` and no broker for the plot.
- **The stop**, at 00:01, as a command on its own, with no build, waiter or signaller running:
  `ray_cluster.sh stop`, **exit 0, 19 s**: `SIGTERM` to the 18 processes naming `/tmp/ray-cpbh`
  (the baseline's 18 pids, 40013–40042), the 15 s grace, `SIGKILL` to 40013 (`gcs_server`) and
  40014; "no process names /tmp/ray-cpbh; ports 6380 6381 8267 52367 are free"; "released the
  rehearsal lock". Its exit status was not echoed: the Bash tool reports a non-zero exit and reported
  none, and the script prints `stopped in` only on its `return 0` (`ray_cluster.sh:354-355`). Then,
  at 00:02:00, `ray_cluster.sh status`, exit 0: the lock free, no ChamPBH process, the four ports
  free. `/tmp/ray/ray_current_cluster` does not exist. `ray stop` was never run.

Output: [`04-cluster.txt`](rrk-adoption/out/04-cluster.txt).

### 1.2 The probe (P5, ruling 8)

After the baseline and before `reference`, from the repository root:

```bash
./venv/bin/python -c 'import signal, ray; a = signal.getsignal(signal.SIGTERM); ray.init(address="127.0.0.1:6380"); b = signal.getsignal(signal.SIGTERM); print("before", a); print("after", b, getattr(b, "__module__", None), getattr(b, "__qualname__", None)); ray.shutdown()'
```

exit 0, printing

```text
before 0
after <function init.<locals>.sigterm_handler at 0x10c0798a0> ray._private.worker init.<locals>.sigterm_handler
```

`0` is `SIG_DFL`. So `ray.init`, attached to an external cluster, installs Ray's own Python
`SIGTERM` handler, `ray._private.worker`'s `init.<locals>.sigterm_handler`, as SecondaryGWKit found
on a Ray it started itself. It added the job `01000000`, `SUCCEEDED`. It opened no store and ran no
solve. Output: [`04-probe-p5.txt`](rrk-adoption/out/04-probe-p5.txt).

### 1.3 The four builds

Each launched detached through `sh` (§1.10), checked once, and waited on by a waiter that reads the
launch log only (`wait_for_line.py`, ruling 3). Wall time is from the launch to the launcher's line
as the waiter saw it (±1 s). The fingerprint's time is from `taken.when`, which the registry stamps
before it reads the store (`runregistrykit/stores.py:1421-1427`), to the final `status.json` write,
whose heartbeat every write stamps (`Run._update`).

| Run id | Slug | Store | pid | State | Exit | Launched → ended (BST) | Wall | `results_store_id` | Fingerprint at finish |
|---|---|---|---|---|---|---|---|---|---|
| `rrk-adoption-04-reference-20261019T234113` | `reference` | `reference` | 40540 | `done` | 0 | 23:41:11 → 23:46:51 | 5 min 40 s | `null` | `2dd4b39b…932f`, 23:46:44 → 23:46:51, ≤ 7 s |
| `rrk-adoption-04-resumed-a-20261019T234840` | `resumed-a` | `resumed` | 43850 | `killed` | 143 | 23:48:38 → 23:53:08 | 4 min 30 s | `null` | `0d612337…0f13`, 23:52:59 → 23:53:07, ≤ 8 s |
| `rrk-adoption-04-resumed-b-20261019T235447` | `resumed-b` | `resumed` | 46306 | `running` → `lost` | `null` | 23:54:46 → 23:55:37 (`SIGKILL` 23:55:36) | 51 s | `1e9b249c0f2e4af5b0b529b0b403aaa6` | none (no `finish`) |
| `rrk-adoption-04-resumed-c-20261019T235647` | `resumed-c` | `resumed` | 47488 | `done` | 0 | 23:56:46 → 23:59:51 | 3 min 5 s | `1e9b249c0f2e4af5b0b529b0b403aaa6` | `2dd4b39b…932f`, 23:59:44 → 23:59:51, ≤ 7 s |

The refused launch of `resumed-c` (§1.6), pid 47288, ran 23:56:23 → 23:56:30 and exited 2.

The stages' times, from each log's `ALL WORK ITEMS COMPLETE` lines:

| Build | Scalar | Adiabatic | BBN | `stage_counts.py` (lookup, compute, store, validation) |
|---|---|---|---|---|
| `reference` | 3m 46.5s | 9.48s | 32.9s | 5 / 5 / 5 / 5 in each stage, exit status 0 |
| `resumed-a` | — | — | — | scalar `INCOMPLETE; no queue summary`, exit status 143 |
| `resumed-b` | — | — | — | scalar `INCOMPLETE; no queue summary`, exit status not recorded |
| `resumed-c` | 1m 51.1s | 5.07s | 30.7s | scalar 5 / **2** / **2** / **2**; adiabatic and BBN 5 / 5 / 5 / 5; exit status 0 |
| last rehearsal's `reference` (`read-only-plots` §1.2) | 2m 12s | 3.59s | 24.1s | 5 / 5 / 5 / 5 |

`reference`'s scalar stage took 3m 46.5s, against 2m 12s at the last rehearsal: its β = 25 history
took 195.74 s (§1.5). No log has a `Traceback`. The other unusual lines are the last rehearsal's
kinds: Ray's `FutureWarning` about `RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO`, the three super-Planckian
warnings, and the `Datastore` actors' `@@ Parametrized equation-of-state` line.

**Each manifest** ([`04-<slug>.manifest.json`](rrk-adoption/out/)):
- `script` `main.py`, `script_sha256` `243a5026…7d59`, which is `shasum -a 256 main.py`;
- `results` `var/datastores/rrk-adoption-04-<store>.db`, relative to the client root; `argv` the
  wrapper's absolute path and its arguments as given, with `$S` absolute (ruling 5);
- `packages` the three, each with `editable: false` and its tag; `host` `id` `8d2b247824882486`,
  `arm64`, `Darwin` `27.0.0`, Python 3.13.16;
- `git_head` `07aa626e9c17a140a48bc203fef6b38565dccbbd`, `git_dirty` `true` (§1.9);
- `scope`, for `reference`: `config exponential.yaml; potential Exponential; β 1.1, 1.15, 1.2, 10,
  25; M 0.001 Mp; Λ 0.001 eV; φ* 5 Mp; 16 shards; job rrk-adoption-04-reference`, and the same with
  each run's job name for the others; `heartbeat_means` as `5d02258` wrote it, the clause "or at once
  when the line changes the stage" included.

**Written before the driver's first line** (ruling 4). In every one of the four launch logs the
three `** registered_run:` lines are lines 1–3, and `main.py`'s first line, `>> packages: …`, is line
4. And each log, which `sh` creates at the launch, is born before its manifest:

| Build | Log born | Manifest `created` | Manifest born |
|---|---|---|---|
| `reference` | 23:41:11 | 23:41:13 | 23:41:13 |
| `resumed-a` | 23:48:39 | 23:48:40 | 23:48:40 |
| `resumed-b` | 23:54:46 | 23:54:47 | 23:54:47 |
| `resumed-c` | 23:56:47 | 23:56:47 | 23:56:48 |

**`reference`'s record.** Its log ends `** driver exit status 0`, `** launcher saw exit status 0`.
`list`: `done`, `1 finished`. `status.json`: `done`, `exit_code` 0, the fingerprint (digest
`2dd4b39b…932f`, `problems` `{}`, `taken` by its own run id), and
`fingerprint_sidecar`: `not written: …/var/datastores/rrk-adoption-04-reference.manifest.json is
not a problem-free registry sidecar (there is none)`, as there was none yet (README §0.2 item 6).
`resumed-a`'s status says the same of the resumed store.

Outputs: the launch logs [`04-reference.log`](rrk-adoption/out/04-reference.log),
[`04-resumed-a.log`](rrk-adoption/out/04-resumed-a.log),
[`04-resumed-b.log`](rrk-adoption/out/04-resumed-b.log),
[`04-resumed-c.log`](rrk-adoption/out/04-resumed-c.log); each run's `manifest.json` and
`status.json` whole, as `04-<slug>.manifest.json` and `04-<slug>.status.json`; the checks
[`04-checks.txt`](rrk-adoption/out/04-checks.txt); the counts
[`04-stage-counts.txt`](rrk-adoption/out/04-stage-counts.txt).

### 1.4 The heartbeat

`status.json` keeps only the latest beat, so no history of beats is claimed. **`list`'s `AGE` column
is the run's age since its manifest's `created`, not the heartbeat's age**
(`runregistrykit/__main__.py:652`: `now - entry['created_epoch']`). So the heartbeat's age at each
check is the check's time against `status.json`'s `heartbeat`, both read in the one call:

| Build | Check at | `status.json` heartbeat | Age | Stage in `status.json` | `list` at the check |
|---|---|---|---|---|---|
| `reference` | 23:41:23 | 23:41:14 | 9 s | `Created sharded datastore "…/rrk-adoption-04-reference.db" with 16 shards` | `running`, `AGE 11s`, `1 alive` |
| `resumed-a` | 23:48:54 | 23:48:40 | 14 s | `Created sharded datastore "…/rrk-adoption-04-resumed.db" with 16 shards` | `running`, `AGE 15s`, `1 alive, 1 finished` |
| `resumed-b` | 23:55:16 | 23:54:48 | 28 s | `Opened existing sharded datastore "…/rrk-adoption-04-resumed.db" with 16 shards` | `running`, `AGE 30s`, stage `CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID`, `1 alive, 2 finished` |
| `resumed-c` | 23:57:01 | 23:56:49 | 12 s | `Opened existing sharded datastore "…/rrk-adoption-04-resumed.db" with 16 shards` | `running`, `AGE 15s`, `1 alive, 3 finished` |

At `resumed-b`'s check the scalar banner beat between the status read and `list`'s read, a second
or so apart: `list` shows the banner as the stage, and the status left behind by the kill has the
heartbeat 23:55:16 and that stage.

**The final status's last beat against the build's end:**
- `reference` 23:46:51, the launcher's line 23:46:51;
- `resumed-a` 23:53:07, the launcher's line 23:53:08;
- `resumed-c` 23:59:51, the launcher's line 23:59:51;
- `resumed-b`'s last beat is 23:55:16, the scalar banner, 20 s before the `SIGKILL` at 23:55:36. At
  `end`, 23:56:39, its age was 83.1 s (the `ended` record's `heartbeat_age`).

**Against the longest silence 03 expected** (log 03 §2.3: the scalar stage, about 2¼ min at the
last rehearsal's load): `reference`'s log has no line between the scalar banner (line 24) and its
completion line (line 25), so that stage printed nothing for its whole 3m 46.5s, longer than 2¼ min
because the stage ran longer, and far inside the 900 s window. No `list` taken while a
build ran showed it `stale`. `resumed-c`'s scalar stage, 1m 51.1s, has the same shape.

### 1.5 Choosing the two delays (ruling 6)

**The five `compute_time`s**, after `reference` had finished and after its `store create` and `store
fingerprint --write`, read from its shards through `sqlite3` with `file:…?mode=ro` URIs only
(`04-compute_times.py.txt`):

```sql
SELECT b.value, s.compute_time, s.compute_steps, s.validated, s.failure, s.timestamp
FROM ScalarModel s JOIN ExponentialCoupling c ON s.coupling_serial = c.serial
JOIN beta_value b ON c.beta_serial = b.serial
```

| β | `compute_time` | Steps | Row's `timestamp` (BST) |
|---|---|---|---|
| 1.15 | 7.28 s | 217426 | 23:42:25.4 |
| 1.1 | 7.33 s | 227521 | 23:42:26.6 |
| 1.2 | 8.12 s | 254491 | 23:42:27.4 |
| 10 | 50.91 s | 1075123 | 23:43:23.5 |
| 25 | 195.74 s | 2572397 | 23:45:53.8 |

The five run at once, so each completes at about its own time after the banner plus its share of
the load. `reference`'s banner is not timed in its log; from the stage's 3m 46.5s ending just after
the last row, it was at about 23:42:08, and the rows were stored at about +17, +18, +19, +75 and
+226 s.

- **`<s>` for `resumed-a` = 150 s.** The widest gap between consecutive completions is β = 10's
  (+75 s) to β = 25's (+226 s); 150 s is near its middle and leaves about 75 s on each side. The
  choice expected β = 1.1, 1.15, 1.2 and 10 stored and β = 25 not.
- **What `resumed-a` stored** (the same query on the resumed store after it,
  `04-compute-times.txt`): four rows, β = 1.15 (10.41 s), 1.1 (10.60 s) and 1.2 (12.32 s) with
  `validated 1`, and β = 10 (62.85 s, stored 23:51:53.4) with **`validated 0`**. The signal came at
  23:52:58, 65 s after that row was stored, before its validation. `interrupt_when.py` saw the
  banner at 22:50:28 UTC, so the rows were stored at about +17, +20, +21 and +85 s. So the outcome
  differs from the expectation in one row: β = 10 was stored and not validated. It is recorded, not
  a stop (ruling 6), and it exercised the prune: `resumed-b`'s open printed an `INTEGRITY WARNING` on
  `shard0002-store`, naming the unvalidated β = 10 model (`store_id=61`, `contains 9776 z-sample
  values | expected=9776`) and `** Note: these values have been pruned from the datastore.` §1.11
  records what may hold the validation back.
- **The delay for `resumed-b` = 20 s.** The histories still missing at its open were β = 10
  (pruned) and β = 25. The shortest `compute_time` among them is β = 10's, 50.91 s in `reference`
  (62.85 s in `resumed-a`); half of it is 25.5 s, and the floor is 10 s. 20 s lies between, and it
  also holds if the prune had kept β = 10 (then half of 195.74 s). After the kill the resumed store
  held the same three validated rows and nothing else (`04-compute-times.txt`): `resumed-b` stored
  nothing, and `resumed-c` printed no prune report.

### 1.6 The signal (H2, P5, ruling 7)

`interrupt_when.py` (`04-resumed-a.interrupt.log`), in UTC:

```text
[2026-10-19T22:50:28+00:00] interrupt_when: saw "CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID"; waiting 150 s
[2026-10-19T22:52:58+00:00] interrupt_when: sent SIGTERM to pid 43850
```

`resumed-a`'s log, after the scalar banner:

```text
** SIGTERM at 2026-10-19 23:52:58 +0100: recorded killed, exit code 143; stopping main.py; the process will exit with status 143
** SIGTERM: main.py ended with SystemExit(1); recording killed, exit code 143
** driver exit status 143
** launcher saw exit status 143
```

(with one forwarded `Datastore` line between the second and the third).

- **Signal → the `killed` record**: the handler's line, 23:52:58 +0100 = 22:52:58 UTC, against
  `sent SIGTERM` at 22:52:58 UTC: **0 s, ±1 s**. The handler writes the record before it prints the
  line (`make_killed_handler`, steps 2 and 3). So "within about a second" holds to the resolution the
  two clocks give, and nothing finer can be told from them.
- **Signal → the final record**, `finish_after_exec`'s with the fingerprint: `status.json`'s
  heartbeat 23:53:07 +0100 against 22:52:58 UTC: **9 s, ±1 s**. Of that, the fingerprint began at
  23:52:59 (`taken.when`); the rest is `main.py` unwinding its `with ShardedPool`.
- `main.py` ended with `SystemExit(1)`, not 143: the handler's `SystemExit(143)`, raised inside
  `ray.wait`, reaches the driver as `SystemExit(1)` (`registered_run.py:343-345`, SecondaryGWKit's
  audit S1). The latch carried the signal, and the run is `killed`/143 and exited 143.
- **No `** cancelled an alarm` line**, and no `*** SIGTERM received` banner from Ray, in any log
  (`grep -i 'SIGTERM received\|alarm'`). With the probe (§1.2) that is P5's measurement: on Ray 2.53
  attached to an external cluster, `ray.init` installs Ray's Python `SIGTERM` handler, the wrapper's
  replaces it, and Ray's handler did not run, so absl's alarm was never armed.
- `status.json` (`04-resumed-a.status.json`): `killed`, `exit_code` 143, the fingerprint (digest
  `0d612337e93525f107d5aed73b10fa14c337744b9e155057d7d8416ea34e0f13`, `taken` by its run id at
  23:52:59), `fingerprint_sidecar` "not written … (there is none)". `list`: `killed`, `2 finished`.
- Then `store create var/datastores/rrk-adoption-04-resumed.db --purpose "rrk-adoption 04 H2-H4: the
  pilot grid built by resumes after SIGTERM and SIGKILL, kept for the user"`, exit 0, on the closed
  store: `store_id` `1e9b249c0f2e4af5b0b529b0b403aaa6`. `store show`: `resumed-a`, `killed`,
  `finished`, `by results`.

### 1.7 The hard-kill route (H3, U5)

Line by line, in order:

1. **The kill.** `kill_when.py` (`interrupt_when.py` with `SIGTERM` changed to `SIGKILL` and nothing
   else, ruling 2; `04-kill_when.diff`):
   ```text
   [2026-10-19T22:55:16+00:00] interrupt_when: saw "CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID"; waiting 20 s
   [2026-10-19T22:55:36+00:00] interrupt_when: sent SIGKILL to pid 46306
   ```
   It signalled the pid in `resumed-b.pid` alone.
2. **The log** ends with the scalar banner, one forwarded `Datastore` line and
   `** launcher saw exit status 137`. There is no `** driver exit status` line (`grep -c` 0).
3. **The status left behind**: `running`, `exit_code` `null`, `pid` 46306, `heartbeat`
   23:55:16, stage `CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID`, no fingerprint.
4. **What the kill left in Ray**, at 23:55:49 (ruling 9's fourth point): `ray_cluster.sh status`,
   the 18 baseline pids exactly (`diff` empty); `ray list workers`, 66 recorded, **0 alive**; `ray
   list actors`, 51 recorded, **0 `ALIVE`**: `04000000`'s 16 `Datastore` and its `SerialPoolBroker`
   all `DEAD`; job `04000000` `SUCCEEDED`. `kill -0 46306`: `no such process`. So the driver,
   attached to an external cluster, left nothing behind (README §0.2), and there was nothing to wait
   for.
5. **`stale`**, at 23:56:08, `list` stdout:
   ```text
   !! rrk-adoption-04-resumed-b-20261019T235447  running           -       1m  rrk-adoption 04 H3: a resume of the resumed store, killed with SIGKILL in the scalar stage  · CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID
   3 run(s) under /Users/ds283/Documents/Code/ChamPBH/var/runs: 2 finished, 1 stale; 0 with no host recorded.
   ```
   and stderr:
   ```text
   !! 1 run(s) say they are running and are not: the pid does not answer kill -0, the heartbeat is older than 900 s, or this machine has rebooted since. Nothing here has killed or cleaned anything — go and look. Once you know one is over, end it with `python -m runregistrykit end RUN_ID --reason TEXT`.
   ```
6. **The refusal** (ruling 10), at 23:56:23: step 6's command with the launch files
   `resumed-c-refused.log` and `.pid`. Its whole log:
   ```text
   !! Cannot begin run rrk-adoption-04-resumed-c-20261019T235624 with results "/Users/ds283/Documents/Code/ChamPBH/var/datastores/rrk-adoption-04-resumed.db": run rrk-adoption-04-resumed-b-20261019T235447 is running (stale, pid 46306) and names this store by its results and results_store_id. A stale run is ended by a person, with `python -m runregistrykit end RUN_ID --reason TEXT`, before another run is begun on its store; the registry does not decide that a run is dead. No run directory was created
   ** driver exit status 2
   ** launcher saw exit status 2
   ```
   Exit status 2; one `!! ` line, naming `resumed-b`'s run and `python -m runregistrykit end`; the
   marker. `find var/runs | sort` before and after: `diff` empty. No Ray job was added (four before
   and after), and `ls var/datastores` after it shows the two stores' files and their sidecars only.
7. **`end`**, at 23:56:38, after `kill -0 46306` again answered `no such process`:
   ```text
   $ ./venv/bin/python -m runregistrykit end rrk-adoption-04-resumed-b-20261019T235447 --reason "SIGKILLed on purpose by rrk-adoption 04 to rehearse the hard-kill route"
   >> end: rrk-adoption-04-resumed-b-20261019T235447 is now lost, verified: its pid 46306 does not answer kill -0 on this host
   ```
   exit 0. The launch through `sh` reaped the driver, so `end` was not refused by an unreaped process
   (RunRegistryKit's `[04-a-killed-process-not-yet-reaped-answers-kill-0]`).
8. **`lost`**: `list`, `rrk-adoption-04-resumed-b-20261019T235447  lost`, `3 finished`. The status's
   `ended` record:
   ```json
   "ended": {
     "at": "2026-10-19T23:56:39+01:00",
     "by": "ds283",
     "evidence": {"boot": "this", "heartbeat": "2026-10-19T23:55:16+01:00", "heartbeat_age": 83.147749,
                  "host": "this", "pid": 46306, "pid_answers": false, "state": "running", "status_readable": true},
     "git_head": "07aa626e9c17a140a48bc203fef6b38565dccbbd",
     "host": {"boot": "376461AF-4985-4F42-8E41-F4E914F5E3E6", "id": "8d2b247824882486", "name": "mps026088.phys.susx.ac.uk"},
     "not_verified_because": null,
     "reason": "SIGKILLed on purpose by rrk-adoption 04 to rehearse the hard-kill route",
     "verified": true
   }
   ```
   with `state` `lost` and `exit_code` `null`; every other field kept.
9. **The resume**, `resumed-c`, was then begun on the same store and accepted (§1.8).

Outputs: [`04-resumed-b.kill.log`](rrk-adoption/out/04-resumed-b.kill.log),
[`04-refusal.txt`](rrk-adoption/out/04-refusal.txt), [`04-checks.txt`](rrk-adoption/out/04-checks.txt)
(the `list` after the kill and `end`), [`04-resumed-b.status.json`](rrk-adoption/out/04-resumed-b.status.json).

### 1.8 The resume (H4) and the stores (H5)

- **`resumed-c`**: `done`/0; each stage's `ALL WORK ITEMS COMPLETE`; the scalar stage looked up 5
  and computed, stored and validated **2**, the two missing (β = 10 and 25); every adiabatic and
  BBN item was computed, since `resumed-a` and `resumed-b` reached neither stage.
  `fingerprint_sidecar` `"written"`. The fingerprint in its status, the sidecar's after it (`taken`
  by `rrk-adoption-04-resumed-c-20261019T235647` at 23:59:44) and `store fingerprint` on the closed
  store are one digest, `2dd4b39b61b9cc1b39bfcf5068ca2d00e84b52fee9da61958a11781931b0932f`
  (ruling 12).
- **`compare_stores`**: `PYTHONPATH=. ./venv/bin/python tools/compare_stores.py
  var/datastores/rrk-adoption-04-reference.db var/datastores/rrk-adoption-04-resumed.db`, exit 0,
  **`RESULT: equal`**, `problems: none`. Every table matched with 0 rows in either only, among them
  `ScalarModel` 5, `ScalarModelValue` 42617, `AdiabaticHistory` 5, `AdiabaticHistoryValue` 42617,
  `BBNData` 5, `BBNDataValue` 7475 and `redshift` 21250. The bookkeeping columns that differ, and
  are not compared, are the timestamps, the labels (which carry the job name), and the compute
  times: `ScalarModel.compute_time`, `mean_RHS_time`, `max_RHS_time`, `min_RHS_time`,
  `AdiabaticHistory.compute_time`, `BBNData.BBN_compute_time` and `NP_compute_time`. That is what
  `dk-and-rpk-migration` README §4.2 allows. No BBN solve met `--bbn-wall-clock-limit` in one build
  and not the other (§4.3): the BBN rows match.
- **`store fingerprint`** on each, at 00:00: the reference, digest `2dd4b39b…932f`, "matches the
  fingerprint taken 2026-10-19T23:47:55+01:00 by a person"; the resumed, the same digest, "matches
  the fingerprint taken 2026-10-19T23:59:44+01:00 by run rrk-adoption-04-resumed-c-20261019T235647".
  The two sidecars' per-class `classes` objects, all 21 classes, compare equal as JSON. So the
  fingerprints compare as `compare_stores` says.
- **The reference's sidecar**: `store create var/datastores/rrk-adoption-04-reference.db --purpose
  "rrk-adoption 04 H1: the reference build of the pilot grid, kept for the user"`, exit 0,
  `store_id` `8b8f0e389f854df4897876184dbbb307`; `store fingerprint … --write`, exit 0, the digest
  `2dd4b39b…932f`, equal to the one in `reference`'s status, "wrote the fingerprint into the sidecar;
  it replaced none". No run of this prompt carries the reference's id (ruling 11).
- **`store show`**, at 00:00, the runs naming each store:
  ```text
  reference:  rrk-adoption-04-reference-20261019T234113  done      finished  by results
  resumed:    rrk-adoption-04-resumed-c-20261019T235647  done      finished  by results and results_store_id
              rrk-adoption-04-resumed-b-20261019T235447  lost      finished  by results and results_store_id
              rrk-adoption-04-resumed-a-20261019T234840  killed    finished  by results
  ```

Outputs: [`04-compare-stores.txt`](rrk-adoption/out/04-compare-stores.txt),
[`04-store.txt`](rrk-adoption/out/04-store.txt) (every `store` command's output whole).

### 1.9 `histories.csv`, `git_dirty`, and what was written

- **`histories.csv`**: `plot_by_beta.py` on the reference store, unregistered, 00:00:48 → 00:01:21,
  exit 0, with `run_readers.sh:115-116`'s options. It printed `>> Opened existing sharded datastore
  "…/rrk-adoption-04-reference.db" read-only with 16 shards`. `var/rrk-adoption-04/plot-reference/QCD_Cosmology/histories.csv`:
  SHA-256 **`d7c62c37d8ad637a63854583ab11bc737109dcbfba92168dbddbaea75182c522`**, R0's (and R1's,
  R2's and `read-only-plots`'). `list` showed the same four runs after it.
- **`git_dirty`** is `true` in every manifest, store history and fingerprint. The tree had no
  tracked change and nothing staged, before and after; the dirt is the user's 22 untracked entries at
  the root (`pilot-*.db`, `pilot-out/`, `pilot-*.log`, `full_run_2026.6.0*.sh`), which README §7
  hands to the user (R6). `var/` is ignored (`.gitignore:7`) and does not count.
- **Written**: `var/datastores/` (the two stores, 17 files each, and their sidecars),
  `var/runs/` (the four run directories), `var/rrk-adoption-04/` (the launch logs, pid files, the
  signallers' logs, the plot's log and outputs), the lock under `~/ScienceData/`, created by `start`
  and removed by `stop`, and `/tmp/ray-cpbh/`, the cluster's. `var/` holds 95 MB. Nothing under
  `var/` was deleted. No other store was opened.

### 1.10 The suites

Serially, each in the foreground, never with the cluster up:

| Root | Tests | Before `start` | After `stop` |
|---|---|---|---|
| CosmologyModels | 18 | OK, 68 s | OK, 78 s |
| ComputeTargets `test_[a-f]*` | 61 | OK, 99 s | OK, 99 s |
| ComputeTargets `test_[g-z]*` | 115 | OK, 107 s | OK, 102 s |
| ObjectFactories | 78 | OK, 10 s | OK, 8 s |
| tools | 84 | OK, 15 s | OK, 15 s |
| RunDrivers | 61 | OK, 20 s | OK, 20 s |

03's counts exactly. Output: [`04-suites.txt`](rrk-adoption/out/04-suites.txt).

### 1.11 Observed, not acted on

- **β = 10's row was stored and left unvalidated for 65 s** in `resumed-a`, until the signal
  (§1.5). Log 03 §2.3 describes `main.py`'s queues as waiting in `ray.wait` "until 3 of the
  in-flight … items complete, or all of them if fewer are in flight". With β = 10's validation and
  β = 25's compute the only two in flight, such a wait returns only when both have completed, which
  would hold the validation back behind the longest history. This is consistent with what was seen,
  and is not measured here. Its cost is a completed history recomputed after an interruption. It is
  RayPoolKit's and `main.py`'s queue settings', not the wrapper's.
- **Ray records a `SIGKILL`ed driver's job as `SUCCEEDED`** (`04000000`). The registry's `stale`,
  not Ray's job status, is what told the kill.
- **`list`'s `AGE` is the run's age**, not the heartbeat's (§1.4). A reader who wants the heartbeat's
  age reads `status.json`.

### 1.12 The commands, verbatim

From the repository root. `<scratch>` is the agent's scratch directory; the four scripts there are
committed as text: [`04-wait_for_line.py.txt`](rrk-adoption/out/04-wait_for_line.py.txt),
[`04-kill_when.py.txt`](rrk-adoption/out/04-kill_when.py.txt),
[`04-compute_times.py.txt`](rrk-adoption/out/04-compute_times.py.txt) and
[`04-ray_state.py.txt`](rrk-adoption/out/04-ray_state.py.txt). The committed logs have ANSI colour
codes removed (`sed -E 's/\x1b\[[0-9;]*m//g'`), and are otherwise verbatim; the `.json` files are
copied unchanged.

```bash
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status
./venv/bin/python -m runregistrykit list
./venv/bin/pip freeze > <scratch>/pip-freeze-before.txt
PYTHONPATH=. ./venv/bin/python -m unittest discover -s CosmologyModels/tests -t .
PYTHONPATH=. ./venv/bin/python -m unittest discover -s ObjectFactories/tests -t .
PYTHONPATH=. ./venv/bin/python -m unittest discover -s tools/tests -t .
PYTHONPATH=. ./venv/bin/python -m unittest discover -s RunDrivers/tests -t .
PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t . -p 'test_[a-f]*.py'
PYTHONPATH=. ./venv/bin/python -m unittest discover -s ComputeTargets/tests -t . -p 'test_[g-z]*.py'

.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh start "rrk-adoption 04 (R)" > <scratch>/start.txt 2>&1
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status
./venv/bin/ray status --address 127.0.0.1:6380
./venv/bin/ray list jobs --address 127.0.0.1:6380
./venv/bin/ray list workers --address 127.0.0.1:6380 --filter "is_alive=True" --limit 500
./venv/bin/ray list actors --address 127.0.0.1:6380 --filter "state=ALIVE"
./venv/bin/python -c 'import signal, ray; a = signal.getsignal(signal.SIGTERM); ray.init(address="127.0.0.1:6380"); b = signal.getsignal(signal.SIGTERM); print("before", a); print("after", b, getattr(b, "__module__", None), getattr(b, "__qualname__", None)); ray.shutdown()'
mkdir -p var/rrk-adoption-04

# H1: one Bash call
export ADDRESS=127.0.0.1:6380 S=$PWD/var/datastores L=$PWD/var/rrk-adoption-04 && nohup sh -c 'PYTHONPATH=. ./venv/bin/python -u RunDrivers/registered_run.py --campaign rrk-adoption --prompt 04 --register reference --purpose "rrk-adoption 04 H1: the pilot grid straight through, the reference store" -- --config exponential.yaml --database $S/rrk-adoption-04-reference.db --shards 16 --job-name rrk-adoption-04-reference --M-values-Mp 1e-3 --beta-values 1.1 1.15 1.2 10 25 --ray-address $ADDRESS > $L/reference.log 2>&1 & echo $! > $L/reference.pid; wait $!; echo "** launcher saw exit status $?" >> $L/reference.log' > /dev/null 2>&1 &
./venv/bin/python <scratch>/wait_for_line.py var/rrk-adoption-04/reference.log "** registered_run: registered as" --timeout 120
# the one check: the manifest from the log's "**   manifest" line, status.json's state and pid against the pid file, the manifest's created, the log's birth time, list, the log's first lines
./venv/bin/python <scratch>/wait_for_line.py var/rrk-adoption-04/reference.log "** launcher saw exit status" --timeout 540
./venv/bin/python .documents/dk-and-rpk-migration/rehearsal/stage_counts.py var/rrk-adoption-04/reference.log
./venv/bin/python -m runregistrykit list
shasum -a 256 main.py
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status
./venv/bin/python <scratch>/ray_state.py
./venv/bin/python -m runregistrykit store create var/datastores/rrk-adoption-04-reference.db --purpose "rrk-adoption 04 H1: the reference build of the pilot grid, kept for the user"
./venv/bin/python -m runregistrykit store fingerprint var/datastores/rrk-adoption-04-reference.db --write
./venv/bin/python -m runregistrykit store show var/datastores/rrk-adoption-04-reference.db
./venv/bin/python <scratch>/compute_times.py var/datastores/rrk-adoption-04-reference

# H2: one Bash call
export ADDRESS=127.0.0.1:6380 S=$PWD/var/datastores L=$PWD/var/rrk-adoption-04 && nohup sh -c 'PYTHONPATH=. ./venv/bin/python -u RunDrivers/registered_run.py --campaign rrk-adoption --prompt 04 --register resumed-a --purpose "rrk-adoption 04 H2: the pilot grid into the resumed store, interrupted with SIGTERM in the scalar stage" -- --config exponential.yaml --database $S/rrk-adoption-04-resumed.db --shards 16 --job-name rrk-adoption-04-resumed-a --M-values-Mp 1e-3 --beta-values 1.1 1.15 1.2 10 25 --ray-address $ADDRESS > $L/resumed-a.log 2>&1 & echo $! > $L/resumed-a.pid; wait $!; echo "** launcher saw exit status $?" >> $L/resumed-a.log' > /dev/null 2>&1 &
nohup ./venv/bin/python .documents/dk-and-rpk-migration/rehearsal/interrupt_when.py $L/resumed-a.log $L/resumed-a.pid --banner "CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID" --after 150 > $L/resumed-a.interrupt.log 2>&1 &
# then the registration wait, the one check, the end wait, stage_counts, list, ray_cluster.sh status and ray_state.py, as for H1
./venv/bin/python <scratch>/compute_times.py var/datastores/rrk-adoption-04-resumed
./venv/bin/python -m runregistrykit store create var/datastores/rrk-adoption-04-resumed.db --purpose "rrk-adoption 04 H2-H4: the pilot grid built by resumes after SIGTERM and SIGKILL, kept for the user"
./venv/bin/python -m runregistrykit store show var/datastores/rrk-adoption-04-resumed.db

# H3: one Bash call
export ADDRESS=127.0.0.1:6380 S=$PWD/var/datastores L=$PWD/var/rrk-adoption-04 && nohup sh -c 'PYTHONPATH=. ./venv/bin/python -u RunDrivers/registered_run.py --campaign rrk-adoption --prompt 04 --register resumed-b --purpose "rrk-adoption 04 H3: a resume of the resumed store, killed with SIGKILL in the scalar stage" -- --config exponential.yaml --database $S/rrk-adoption-04-resumed.db --shards 16 --job-name rrk-adoption-04-resumed-b --M-values-Mp 1e-3 --beta-values 1.1 1.15 1.2 10 25 --ray-address $ADDRESS --prune-unvalidated > $L/resumed-b.log 2>&1 & echo $! > $L/resumed-b.pid; wait $!; echo "** launcher saw exit status $?" >> $L/resumed-b.log' > /dev/null 2>&1 &
nohup ./venv/bin/python <scratch>/kill_when.py $L/resumed-b.log $L/resumed-b.pid --banner "CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID" --after 20 > $L/resumed-b.kill.log 2>&1 &
# then the registration wait, the one check, the end wait
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status
./venv/bin/python <scratch>/ray_state.py
kill -0 46306
./venv/bin/python -m runregistrykit list
./venv/bin/python <scratch>/compute_times.py var/datastores/rrk-adoption-04-resumed
find var/runs | sort > <scratch>/runs-find-before-refusal.txt
export ADDRESS=127.0.0.1:6380 S=$PWD/var/datastores L=$PWD/var/rrk-adoption-04 && nohup sh -c 'PYTHONPATH=. ./venv/bin/python -u RunDrivers/registered_run.py --campaign rrk-adoption --prompt 04 --register resumed-c --purpose "rrk-adoption 04 H4: the last resume of the resumed store, straight through" -- --config exponential.yaml --database $S/rrk-adoption-04-resumed.db --shards 16 --job-name rrk-adoption-04-resumed-c --M-values-Mp 1e-3 --beta-values 1.1 1.15 1.2 10 25 --ray-address $ADDRESS --prune-unvalidated > $L/resumed-c-refused.log 2>&1 & echo $! > $L/resumed-c-refused.pid; wait $!; echo "** launcher saw exit status $?" >> $L/resumed-c-refused.log' > /dev/null 2>&1 &
./venv/bin/python <scratch>/wait_for_line.py var/rrk-adoption-04/resumed-c-refused.log "** launcher saw exit status" --timeout 120
find var/runs | sort > <scratch>/runs-find-after-refusal.txt
diff <scratch>/runs-find-before-refusal.txt <scratch>/runs-find-after-refusal.txt
kill -0 46306
./venv/bin/python -m runregistrykit end rrk-adoption-04-resumed-b-20261019T235447 --reason "SIGKILLed on purpose by rrk-adoption 04 to rehearse the hard-kill route"
./venv/bin/python -m runregistrykit list

# H4: one Bash call
export ADDRESS=127.0.0.1:6380 S=$PWD/var/datastores L=$PWD/var/rrk-adoption-04 && nohup sh -c 'PYTHONPATH=. ./venv/bin/python -u RunDrivers/registered_run.py --campaign rrk-adoption --prompt 04 --register resumed-c --purpose "rrk-adoption 04 H4: the last resume of the resumed store, straight through" -- --config exponential.yaml --database $S/rrk-adoption-04-resumed.db --shards 16 --job-name rrk-adoption-04-resumed-c --M-values-Mp 1e-3 --beta-values 1.1 1.15 1.2 10 25 --ray-address $ADDRESS --prune-unvalidated > $L/resumed-c.log 2>&1 & echo $! > $L/resumed-c.pid; wait $!; echo "** launcher saw exit status $?" >> $L/resumed-c.log' > /dev/null 2>&1 &
# then the registration wait, the one check, the end wait, stage_counts, list

# H5
PYTHONPATH=. ./venv/bin/python tools/compare_stores.py var/datastores/rrk-adoption-04-reference.db var/datastores/rrk-adoption-04-resumed.db
./venv/bin/python -m runregistrykit store fingerprint var/datastores/rrk-adoption-04-reference.db
./venv/bin/python -m runregistrykit store fingerprint var/datastores/rrk-adoption-04-resumed.db
./venv/bin/python -m runregistrykit store show var/datastores/rrk-adoption-04-reference.db
./venv/bin/python -m runregistrykit store show var/datastores/rrk-adoption-04-resumed.db
PYTHONPATH=. ./venv/bin/python -u plot_by_beta.py --config exponential.yaml --database $S/rrk-adoption-04-reference.db --shards 16 --phi-init-Mp 5 --output $L/plot-reference --ray-address 127.0.0.1:6380 > $L/plot-reference.log 2>&1
shasum -a 256 $L/plot-reference/QCD_Cosmology/histories.csv
./venv/bin/python <scratch>/ray_state.py
./venv/bin/python -m runregistrykit list

.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh stop
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status
# the six suites again, as above
./venv/bin/pip freeze > <scratch>/pip-freeze-after.txt
cmp <scratch>/pip-freeze-before.txt <scratch>/pip-freeze-after.txt
```
