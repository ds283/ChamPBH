# jordan-normalization — verification

**Campaign:** [`prompts/jordan-normalization/`](../prompts/jordan-normalization/README.md) ·
**Board:** [`IMPLEMENTATION_STATE.md`](../prompts/jordan-normalization/IMPLEMENTATION_STATE.md) ·
**Outputs:** [`jordan-normalization/out/`](jordan-normalization/out/) ·
**Scripts:** [`dk-and-rpk-migration/rehearsal/`](dk-and-rpk-migration/rehearsal/) (`ray_cluster.sh`,
`interrupt_when.py`, `stage_counts.py`) and `tools/compare_stores.py`, run unchanged

This document records the campaign's rehearsal (prompt 09) in §1. The earlier prompts' records are
their logs. §1 shows that under real Ray, through the wrapper, on ChamPBH's isolated cluster, tuned
histories are computed, stored, read back by the adiabatic and BBN stages and by `plot_by_beta.py`;
that a build `SIGTERM`ed in its scalar stage resumes to a store equal to an uninterrupted one; and
that every stored tuned value equals the one `tools/history_and_bbn.py` computes standalone, bit
for bit.

It is additive (`CLAUDE.md` invariant 6). A re-run that supersedes a figure gets a new dated
subsection, and the figures below stay as they are, correct for the tree they were taken on.

The stores, the run records and the launch logs live in the repository's `var/`, ignored by git:
the stores in `var/datastores/`, the runs in `var/runs/`, the launch logs, pid files, recorders and
working outputs in `var/jordan-normalization-09/`. They are kept; retiring the stores is the user's
(README §3, §7). Only small text outputs are committed, under `jordan-normalization/out/09-*`. Every
claim below is a line in one of them, or in a command's output quoted here.

---

## 1. The rehearsal (prompt 09, 2026-10-26)

**Tree:** `4dc31ea` (`4dc31eae6d3a523abd58346aa9628ea24ad97cfe`, the orchestrator's note for 09, on
top of `a5e7850`), whose code is 08's `e624f1a`: `git diff e624f1a HEAD --stat` names five records
(the index, this campaign's board, 08's log, note 09 and the `surfing-temperature` board) and no
code. `main.py` SHA-256 `ec6144aa02f5014dbc1aaecee81dd22efa030b2ae507c4eb6b7965b9e489f87d`.
`git status --porcelain --untracked-files=all` empty at the Gate and after the suites at the end.
Output: [`09-gate.txt`](jordan-normalization/out/09-gate.txt).

**Result: the gate "R" holds.**
- **Seven histories, six models** (the note's ruling 1): run `a` β = 0.5, 1.5, 3, 5.5 at
  M = 10⁻³ M_P, φ* = 5; run `b` β = 0.5 at M = 10⁻³, φ* = 1; run `c` β = 3 at M = 0.1 and 10⁻⁵,
  φ* = 5. The reference build ran `a`, `b`, `c` to `done`/0; the resumed build's `a` was recorded
  `killed`/143 with β = 0.5, 1.5 and 3 stored and validated, and `a` again, `b` and `c` ran to
  `done`/0.
- **Every history converged** (failure 0, validated 1, |residual| < 10⁻⁵ for both residuals), in
  3 to 9 integrations.
- **Every stored tuned value equals the standalone tool's, bitwise**: 7 histories × 15 keys × 2
  stores, by `float.hex` of the printed repr, ints and `seed_kind` exactly. No NaN was printed.
  The stored inputs are the tool's: atol = rtol = 10⁻⁸, tuning tolerance 10⁻⁵, T* = 2 × 10⁴ GeV,
  π* = 0, φ* and M and β as given, and each row's `z_samples` the tool's `samples=`.
- **`compare_stores`: `RESULT: equal`**, `problems: none`; `ExponentialModel` 6, `ScalarHistory` 7.
  **Both stores fingerprint to `3b92d064512079e30bb2291d6d82732fed10c187e50c61e4b3d54cb47b3415a7`**.
- Six of the seven histories' normalization and φ_ref equal prompt 06's probes' RESULT hex, and the
  seventh (β = 0.5 at φ* = 5) the orchestrator's probe line, bitwise.
- Against README §0.3: Λ lower by 2.768–2.795 × 10⁻⁴ (05's units), φ_ref within 1.6 × 10⁻⁵.
- The one BBN failure is β = 0.5 at φ* = 5, the same in both stores, with the reason the tool gives.
- **The longest heartbeat silence was 58 s** (`reference-c`, the scalar stage), against the 900 s
  window.
- Every manifest: `git_head` `4dc31eae…`, `git_dirty` **false**, `script_sha256` `main.py`'s.
- No Ray worker or actor was alive after any driver ended, and nothing of the cluster outlived the
  stop. No super-Planckian warning was printed.

### 1.1 The cluster and the load

- 10 CPUs, host `mps026088` (`mps026088.phys.susx.ac.uk` in the manifests).
- **The load** ([`09-load.txt`](jordan-normalization/out/09-load.txt); the suites' in
  [`09-suites.txt`](jordan-normalization/out/09-suites.txt), the tool runs' in their outputs). Other
  workloads share the machine; no ChamPBH Ray process was up at the Gate.

  | When (GMT) | Load average (1 / 5 / 15 min) |
  |---|---|
  | 19:43:42, the Gate | 3.45 / 4.26 / 3.81 |
  | 19:45 – 19:54, the suites | 2.92 → 8.36 (1 min, before each root) |
  | 19:54:53, before `start` | 3.59 / 4.35 / 4.03 |
  | 19:55:42, `reference-a` launched | 2.44 / 3.96 / 3.91 |
  | 20:00:12, `reference-b` launched | 5.90 / 6.19 / 5.08 |
  | 20:01:31, `reference-c` launched | 3.13 / 5.32 / 4.84 |
  | 20:04:56, `resumed-a` launched | 3.20 / 4.61 / 4.67 |
  | 20:08:23, `resumed-a2` launched | 4.96 / 5.67 / 5.22 |
  | 20:11:28, `resumed-b` launched | 3.43 / 4.52 / 4.81 |
  | 20:12:48, `resumed-c` launched | 4.62 / 4.73 / 4.87 |
  | 20:16:05, `plot_by_beta.py` | 4.99 / 5.20 / 5.05 |
  | 20:16:49, before `stop` | 6.21 / 5.53 / 5.18 |
  | 20:18 – 20:23, the seven tool runs | 6.61 → 10.73 (1 min, at each start) |
  | 20:28 – 20:36, the suites after | 3.31 → 7.73 (1 min, before each root) |

- **The start**, from the repository root, alone in its Bash call:
  `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh start "jordan-normalization 09" >
  var/jordan-normalization-09/start.txt 2>&1; echo $?`: **exit 0**, ending
  ```text
  recorded pid 35393 in /Users/ds283/.raypoolkit/rehearsal-lock/owner
  >> ray_cluster: address 127.0.0.1:6380
  >> ray_cluster: dashboard http://127.0.0.1:8267
  >> ray_cluster: actors: RAY_AUTH_MODE=token ./venv/bin/ray list actors --address 127.0.0.1:6380
  >> ray_cluster: started in 8s; gcs_server pid 35393
  ```
  `grep -c` of `Token authentication is enabled`, `Generated new authentication token`,
  `InvalidAuthToken` and `AuthenticationError` in `start.txt`: 0 each; `grep -ci warn`: 0.
- `ray_cluster.sh status`, exit 0: `lock: HELD`, `purpose: jordan-normalization 09`, `acquired:
  2026-10-26T19:54:58+00:00`, owner pid 35393 alive; 19 processes (35393–35429); the four ports held.
  The `gcs_server` (35393) and the `raylet` (35427) each run with `RAY_AUTH_MODE=token` (`ps -E`,
  grepped for that variable alone).
- `RAY_AUTH_MODE=token ./venv/bin/ray status --address 127.0.0.1:6380`, exit 0: one active node,
  no failures, `0.0/10.0 CPU`, `0.0/1.0 GPU`, `0B/6.57GiB memory`, `0B/2.00GiB object_store_memory`.
- **The Ray state** (`ray_state.py`, the note's ruling 11;
  [`09-ray-state.txt`](jordan-normalization/out/09-ray-state.txt)):
  - after `start` (19:55:28): `No resource in the cluster` for jobs, workers and actors;
  - after `resumed-a`'s launcher line (20:07:45): jobs `01000000`–`04000000` (`reference-a`,
    `-b`, `-c`, `resumed-a`; driver pids 35782, 36512, 36956, 37417), all `SUCCEEDED`; **workers
    85 recorded, 0 alive; actors 68 recorded, 0 `ALIVE`**: 16 `Datastore` and one
    `SerialPoolBroker` for each job, all `DEAD`. `ray_cluster.sh status` then, exit 0: the same 19
    processes as after `start` (the pid lists `diff` empty) and nothing else. Nothing of the
    `SIGTERM`ed job was left to wait for;
  - after the plot, before `stop` (20:16:42): eight jobs, all `SUCCEEDED`, `05000000`–`07000000`
    (`resumed-a2`, `-b`, `-c`; 37955, 38353, 38606) and `08000000` (`plot_by_beta.py`, 39163) added;
    workers 169 recorded, 0 alive; actors 135 recorded, 0 `ALIVE`.
  Ray gives the `SIGTERM`ed driver's job `SUCCEEDED`, as `rrk-adoption` 04 found.
- **The stop**, straight after the check of 20:16:49, once every driver's pid answered `kill -0` with `no such process`
  (35782, 36512, 36956, 37417, 37955, 38353, 38606), every recorder had written its exit line and
  `interrupt_when.py` had printed its `sent` line
  ([`09-cluster.txt`](jordan-normalization/out/09-cluster.txt), "before stop"):
  `ray_cluster.sh stop > var/jordan-normalization-09/stop.txt 2>&1; echo $?`, **exit 0**: `SIGTERM
  to 19 process(es)` (35393–35429), `grace period of 15s over; SIGKILL to: 35393 35394`, `no process
  names /tmp/ray-cpbh; ports 6380 6381 8267 52367 are free`, `released the rehearsal lock`,
  `stopped in 20s`. Then `ray_cluster.sh status`, **exit 0**: `lock: free
  (/Users/ds283/.raypoolkit/rehearsal-lock does not exist)`, `Ray processes of /tmp/ray-cpbh: none`,
  nothing `stop` would signal, the four ports free; and `./venv/bin/python -m
  raypoolkit.rehearsal_lock status`, exit 0, `lock: free`. `ray stop` was never run.

Outputs: [`09-cluster.txt`](jordan-normalization/out/09-cluster.txt) (every `start`, `status`,
`stop` and lock output whole, `ray status`, the environments),
[`09-ray-state.txt`](jordan-normalization/out/09-ray-state.txt).

### 1.2 The builds

Each launched detached through `sh` (§1.13), checked once, and waited on by `wait_for_line.py`,
which reads the launch log only; a recorder (`beat_times.py`, the note's ruling 7) ran beside each.
Launched is the log's birth (`/usr/bin/stat`); ended is the launcher's line as the recorder saw it.
The fingerprint's time is from `taken.when` to the final `status.json` heartbeat.

| Run id | Slug | Store | pid | State | Exit | Launched → ended (GMT) | Wall | Load at launch | `results_store_id` | Fingerprint at finish |
|---|---|---|---|---|---|---|---|---|---|---|
| `jordan-normalization-09-reference-a-20261026T195544` | `reference-a` | `reference` | 35782 | `done` | 0 | 19:55:43 → 19:59:24 | 3 min 41 s | 2.44 | `null` | `752c6c07…7838`, 19:58:59 → 19:59:23, 24 s |
| `jordan-normalization-09-reference-b-20261026T200014` | `reference-b` | `reference` | 36512 | `done` | 0 | 20:00:12 → 20:01:15 | 1 min 3 s | 5.90 | `b5721a5e4c544f02bfb78e1b9ea7e9d9` | `f953123a…0985`, 20:00:50 → 20:01:14, 24 s; sidecar `written` |
| `jordan-normalization-09-reference-c-20261026T200132` | `reference-c` | `reference` | 36956 | `done` | 0 | 20:01:31 → 20:04:38 | 3 min 7 s | 3.13 | `b5721a5e4c544f02bfb78e1b9ea7e9d9` | `3b92d064…15a7`, 20:04:14 → 20:04:37, 23 s; sidecar `written` |
| `jordan-normalization-09-resumed-a-20261026T200457` | `resumed-a` | `resumed` | 37417 | `killed` | 143 | 20:04:56 → 20:07:35 | 2 min 39 s | 3.20 | `null` | `454e3435…c2b6`, 20:06:59 → 20:07:24, 25 s |
| `jordan-normalization-09-resumed-a2-20261026T200824` | `resumed-a2` | `resumed` | 37955 | `done` | 0 | 20:08:23 → 20:11:13 | 2 min 50 s | 4.96 | `5f9ffeeba8994e4ba41504eb24040cc0` | `752c6c07…7838`, 20:10:48 → 20:11:12, 24 s; sidecar `written` |
| `jordan-normalization-09-resumed-b-20261026T201130` | `resumed-b` | `resumed` | 38353 | `done` | 0 | 20:11:28 → 20:12:33 | 1 min 5 s | 3.43 | `5f9ffeeba8994e4ba41504eb24040cc0` | `f953123a…0985`, 20:12:07 → 20:12:31, 24 s; sidecar `written` |
| `jordan-normalization-09-resumed-c-20261026T201250` | `resumed-c` | `resumed` | 38606 | `done` | 0 | 20:12:48 → 20:15:53 | 3 min 5 s | 4.62 | `5f9ffeeba8994e4ba41504eb24040cc0` | `3b92d064…15a7`, 20:15:28 → 20:15:52, 24 s; sidecar `written` |

`reference-a` and `resumed-a2` leave their store with the same digest (`752c6c07…7838`), as do
`reference-b` and `resumed-b` (`f953123a…0985`) and `reference-c` and `resumed-c`
(`3b92d064…15a7`): after the resume, each run of the resumed build left its store equal to the
reference's after the same run. `reference-a`'s and `resumed-a`'s `fingerprint_sidecar` is "not
written: … is not a problem-free registry sidecar (there is none)", as no sidecar existed yet.

The stages' times, from each log's `ALL WORK ITEMS COMPLETE` lines, and `stage_counts.py`
([`09-stage-counts.txt`](jordan-normalization/out/09-stage-counts.txt)):

| Build | Scalar | Adiabatic | BBN | `stage_counts.py` (lookup, compute, store, validation) |
|---|---|---|---|---|
| `reference-a` | 2m 20.7s | 2.37s | 20.7s | 4 / 4 / 4 / 4 in each stage, exit status 0 |
| `reference-b` | 14.4s | 0.358s | 8.68s | 1 / 1 / 1 / 1 in each stage, exit status 0 |
| `reference-c` | 2m 15s | 0.837s | 10.4s | 2 / 2 / 2 / 2 in each stage, exit status 0 |
| `resumed-a` | — | — | — | scalar `INCOMPLETE; no queue summary`, exit status 143 |
| `resumed-a2` | 1m 51.1s | 1.53s | 18.4s | scalar **1 / 1 / 1 / 1**; adiabatic and BBN 4 / 4 / 4 / 4; exit status 0 |
| `resumed-b` | 15.5s | 0.357s | 8.89s | 1 / 1 / 1 / 1 in each stage, exit status 0 |
| `resumed-c` | 2m 13.9s | 1s | 10.3s | 2 / 2 / 2 / 2 in each stage, exit status 0 |

No log has a `Traceback`, an `INTEGRITY WARNING`, a `*** SIGTERM received` or an alarm line, or a
super-Planckian warning (`grep -c`, [`09-token.txt`](jordan-normalization/out/09-token.txt)). Each
prints `-- AdiabaticHistory: 0 models skipped …` and `-- BBNData: 0 models skipped …; 0
computations skipped because of a stored failure`.

**The one check after each launch** (one Bash call, [`09-checks.txt`](jordan-normalization/out/09-checks.txt)):
the pid file equalled `status.json`'s pid, the state was `running`, `list` showed the run, and
lines 1–3 of each log are the wrapper's and line 4 is `>> packages:`, so each manifest was written
before `main.py`'s first line. Each registration wait returned after 2.0 s.

**Each manifest** ([`09-<slug>.manifest.json`](jordan-normalization/out/), summarised in
[`09-manifests.txt`](jordan-normalization/out/09-manifests.txt)):
- `script` `main.py`, `script_sha256` `ec6144aa…f87d`, which is `shasum -a 256 main.py`;
- `git_head` `4dc31eae6d3a523abd58346aa9628ea24ad97cfe`, the dispatch commit; `git_dirty` **`false`**;
- `results` `var/datastores/jordan-normalization-09-<store>.db`; `results_store_id` as in the table;
- `packages` seven, each `editable: false`: `runregistrykit` 0.2.0 (`0bae3a3c…`, `v0.2.0`),
  `datastorekit` 0.4.0 (`c8570e84…`, `v0.4.0`), `raypoolkit` 0.3.0 (`67a75b97…`, `v0.3.0`), and,
  with no commit, URL or revision, `ray` 2.59.0, `SQLAlchemy` 2.0.55, `numpy` 2.3.5, `scipy` 1.17.0;
- `scope`, as 08's `describe_scope` writes it, e.g. `reference-a`'s `config exponential.yaml; β 0.5,
  1.5, 3, 5.5; M 0.001 Mp; n 1; φ* 5 Mp; tuning tolerance 1e-05; 16 shards; job
  jordan-normalization-09-reference-a`, `reference-b`'s `… β 0.5; M 0.001 Mp; n 1; φ* 1 Mp; …` and
  `reference-c`'s `… β 3; M 0.1, 1e-05 Mp; n 1; φ* 5 Mp; …`.

Outputs: the launch logs `09-<slug>.log`; each run's `manifest.json` and `status.json` whole.

### 1.3 The histories

From the reference store (the reader, [`09-histories-reference.txt`](jordan-normalization/out/09-histories-reference.txt))
and the reference runs' recorders ([`09-silences.txt`](jordan-normalization/out/09-silences.txt)).
Integrations is `tuning_iterations`, the seed's included. The tuning wall is the sum of the
history's `-- tune` lines' `wall=`, each that integration's; last line is its last tune line's
arrival after the scalar banner. `compute_time` is the final integration's alone (note 09, §0).
Λ = exp(normalization/4) × `REDUCED_PLANCK_MASS_EV` × 10³ meV.

| β | M/M_P | φ* | Integrations | Plain / secant / guard | Seed | Tuning wall (tune lines) | Last line | `compute_time` | Λ (meV) | φ_ref/M_P | residual_H | residual_φ |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0.5 | 1e-3 | 5 | 4 | 1 / 2 / 0 | tracking | 3.3 s (4 lines) | +5.5 s | 0.69 s | 2.1924036 | 0.06421013 | −6.582e-06 | +6.279e-06 |
| 1.5 | 1e-3 | 5 | 4 | 1 / 2 / 0 | tracking | 16.4 s (3 lines; the seed's lost, §1.11) | +24.7 s | 5.22 s | 2.1253218 | 0.03496227 | +1.272e-07 | +1.790e-06 |
| 3 | 1e-3 | 5 | 4 | 1 / 2 / 0 | tracking | 34.4 s (4 lines) | +36.8 s | 8.02 s | 2.0739961 | 0.02366290 | −1.440e-06 | +9.425e-06 |
| 5.5 | 1e-3 | 5 | 5 | 1 / 3 / 0 | tracking | 113.3 s (5 lines) | +138.1 s | 22.45 s | 2.0206153 | 0.01671936 | −9.983e-10 | +5.114e-08 |
| 0.5 | 1e-3 | 1 | 4 | 1 / 2 / 0 | tracking | 11.3 s (4 lines) | +13.1 s | 2.73 s | 2.1849491 | 0.06382165 | +9.519e-07 | +3.826e-06 |
| 3 | 0.1 | 5 | 9 | 1 / 7 / 0 | static | 29.2 s (9 lines) | +31.2 s | 2.71 s | 1.3889187 | 0.14772170 | +1.278e-08 | −2.884e-07 |
| 3 | 1e-5 | 5 | 3 | 1 / 1 / 0 | tracking | 129.6 s (3 lines) | +132.0 s | 41.85 s | 2.2264733 | 0.00267048 | +8.998e-07 | +3.999e-06 |

The tune lines seen add up to 337.5 s of tuning for the seven (β = 1.5's seed line aside), about
5.6 CPU-minutes, against 06's secant walls of 24–214 s per point at load 6–12 (§0.3 item 7's
estimate: about 10 CPU-minutes). The resumed build's lines give the same counts and walls within a
few per cent ([`09-silences.txt`](jordan-normalization/out/09-silences.txt)).

### 1.4 The delay for `resumed-a` (the note's ruling 8)

From `reference-a`'s recorder ([`09-reference-a.beats.txt`](jordan-normalization/out/09-reference-a.beats.txt)),
the scalar banner arrived at 19:56:15.501, and the four histories' last tune lines at
**t₁ = +5.5 s** (β = 0.5), **t₂ = +24.7 s** (β = 1.5), **t₃ = +36.8 s** (β = 3) and **t₄ = +138.1 s**
(β = 5.5). t₄ − t₃ = 101.3 s, over the ruling's 60 s floor. (t₃ + t₄)/2 = 87.45 s, which rounds to
**`<s>` = 90 s**. Expected: β = 0.5, 1.5 and 3 stored, β = 5.5 not.

**What `resumed-a` stored** (the reader on the resumed store after it, before its `store create`;
[`09-histories-resumed-after-a.txt`](jordan-normalization/out/09-histories-resumed-after-a.txt)):
**three `ScalarHistory` rows, β = 0.5, 1.5 and 3, each `validated = 1`**, stored at 20:05:52.565,
20:05:52.864 and 20:06:04.993, that is +24.5, +24.8 and +36.9 s after the banner (20:05:28.097);
β = 5.5 not stored. `ExponentialModel` 4 (the grid's models are stored before the scalar stage),
`AdiabaticHistory` 0, `BBNData` 0. That is the expected outcome. No row was stored and unvalidated
at the signal, so P19's wait in that form was not seen here (§1.10 has the form that was).
`resumed-a`'s status fingerprint is `454e3435c1dfa287e144307b1b3bac926610e47ecbad9693b42e3ea4bcabc2b6`.

### 1.5 The signal (the note's ruling 9)

`interrupt_when.py` ([`09-resumed-a.interrupt.log`](jordan-normalization/out/09-resumed-a.interrupt.log)), in UTC:

```text
[2026-10-26T20:05:28+00:00] interrupt_when: saw "CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID"; waiting 90 s
[2026-10-26T20:06:58+00:00] interrupt_when: sent SIGTERM to pid 37417
```

`resumed-a`'s log after the banner, the tune lines shortened (`…`) to their history and iteration:

```text
(compute_scalar_history pid=37554) -- tune …(M=0.001Mp,n=1,beta=0.5)… it=0 … it=3 (four lines)
(Datastore pid=37516) @@ Parametrized equation-of-state … [repeated 15x across cluster] (Ray deduplicates logs by default. …)
(compute_scalar_history pid=37552) -- tune …(M=0.001Mp,n=1,beta=3)… it=0 kind=seed … wall=8.5 [repeated 2x across cluster]
(compute_scalar_history pid=37553) -- tune …(M=0.001Mp,n=1,beta=1.5)… it=1, it=2 / beta=3 it=1 / beta=1.5 it=3 / beta=3 it=2, it=3
(compute_scalar_history pid=37553) -- tune …(M=0.001Mp,n=1,beta=5.5)… it=0, it=1, it=2
** SIGTERM at 2026-10-26 20:06:59 +0000: recorded killed, exit code 143; stopping main.py; the process will exit with status 143
** SIGTERM: main.py ended with SystemExit(1); recording killed, exit code 143
(compute_scalar_history pid=37553) -- tune jordan-normalization-09-resumed-a-ScalarHistory-ExponentialModel(M=0.001Mp,n=1,beta=5.5)-2026-10-26T20:05:51 it=3 kind=secant H/H0-1=-5.377e-06 phi_ref/phi_min-1=+2.855e-05 norm=-277.056939828 phi_ref=1.6719773356e-02 wall=21.9
** driver exit status 143
** launcher saw exit status 143
```

- **Signal → the `killed` record**: the handler's line, 20:06:59 +0000, against `sent SIGTERM` at
  20:06:58 UTC: **1 s**, to the one-second resolution of the two clocks; the expectation (0 or 1 s)
  is met. The recorder saw the handler's two lines at 20:06:59.831.
- **Signal → the final record**: `status.json`'s heartbeat 20:07:24 +00:00 against 20:06:58:
  **26 s**. The fingerprint was `taken` at 20:06:59. The driver's marker arrived at 20:07:34.616,
  10 s after the final record.
- `main.py` ended with `SystemExit(1)`, the known shape, and the run is `killed`/143 and exited 143.
  No `*** SIGTERM received` banner and no `** cancelled an alarm` line (`grep -ci` 0).
- **β = 5.5's worker went on** after the record: its `it=3` line arrived at 20:07:19.489, 20 s after
  the handler's, and refreshed the heartbeat with the state `killed` (the recorder's `H` lines at
  20:07:19 and 20:07:24). At 20:07:45 no worker was alive (§1.1).
- `status.json` ([`09-resumed-a.status.json`](jordan-normalization/out/09-resumed-a.status.json)):
  `killed`, `exit_code` 143, the fingerprint `454e3435…c2b6`, `fingerprint_sidecar` "not written …
  (there is none)". `list`: `killed`.
- Then `store create var/datastores/jordan-normalization-09-resumed.db --purpose "jordan-normalization
  09 H2: the tuned pilot grid built by a resume after SIGTERM (runs a, a2, b, c), kept for the user"`,
  exit 0: `store_id` **`5f9ffeeba8994e4ba41504eb24040cc0`**. `store show`: `resumed-a`, `killed`,
  `finished`, `by results`.

### 1.6 The resume and the stores

- **`resumed-a2`** (`--prune-unvalidated`): `done`/0. Its open printed **no prune report** (no
  `INTEGRITY WARNING` and no `** Note: these values have been pruned` line: `grep -c` 0), since no
  row was unvalidated. Its scalar stage looked up, computed, stored and validated **one** history
  (β = 5.5), the one `resumed-a` left missing; its adiabatic and BBN stages computed all four.
- **`compare_stores`**: `PYTHONPATH=. ./venv/bin/python tools/compare_stores.py
  var/datastores/jordan-normalization-09-reference.db var/datastores/jordan-normalization-09-resumed.db`,
  exit 0, **`RESULT: equal`**, `problems: none`. Every table matched with 0 rows in either only:
  `AdiabaticHistory` 7, `AdiabaticHistoryValue` 39397, `AdiabaticHistory_tags` 7, `BBNData` 7,
  `BBNDataValue` 8910, `BBNData_tags` 7, **`ExponentialModel` 6**, `IntegrationSolver` 6, `M_value`
  3, `QCD_Cosmology` 1, **`ScalarHistory` 7**, `ScalarHistoryValue` 39397, `ScalarHistory_tags` 7,
  `beta_value` 4, `phi_value` 2, `pi_value` 1, `redshift` 75001, `store_tag` 1, `temperature` 1,
  `tolerance` 2, `version` 1. The bookkeeping columns that differ and are not compared: every
  table's `timestamp`, the labels (`AdiabaticHistory`, `BBNData`, `ScalarHistory`; they carry the
  job name), `AdiabaticHistory.compute_time`, `BBNData.BBN_compute_time` and `NP_compute_time` (6
  rows each: the failed row has none), and `ScalarHistory.compute_time`, `max_RHS_time`,
  `mean_RHS_time`, `min_RHS_time`. No `extra_data` column is listed. No BBN solve met
  `--bbn-wall-clock-limit` in one build and not the other: the BBN rows match.
- **`store fingerprint`**, without `--write`, on each closed store after the stop: the reference,
  digest `3b92d064…15a7`, `problems: none`, "matches the fingerprint taken 2026-10-26T20:04:14+00:00
  by run jordan-normalization-09-reference-c-20261026T200132, at 4dc31eae…"; the resumed, **the same
  digest**, "matches the fingerprint taken 2026-10-26T20:15:28+00:00 by run
  jordan-normalization-09-resumed-c-20261026T201250, at 4dc31eae…". The fifteen classes' counts and
  digests print identically for the two stores (`ExponentialModel 6 08514d009311`, `ScalarHistory 7
  7a5192c30cf4`, `AdiabaticHistory 7 d1777cb45a3b`, `BBNData 7 1b41b717fd0f`, …).
- **The reference's sidecar**: `store create var/datastores/jordan-normalization-09-reference.db
  --purpose "jordan-normalization 09 H1: the reference build of the tuned pilot grid (runs a, b, c),
  kept for the user"`, after `reference-a`, exit 0, `store_id` `b5721a5e4c544f02bfb78e1b9ea7e9d9`.
  `reference-b` and `reference-c` carry that id and wrote their fingerprints to it.
- **`store show`**, after the stop, the runs naming each store:
  ```text
  reference:  jordan-normalization-09-reference-c-20261026T200132  done      finished  by results and results_store_id
              jordan-normalization-09-reference-b-20261026T200014  done      finished  by results and results_store_id
              jordan-normalization-09-reference-a-20261026T195544  done      finished  by results
  resumed:    jordan-normalization-09-resumed-c-20261026T201250  done      finished  by results and results_store_id
              jordan-normalization-09-resumed-b-20261026T201130  done      finished  by results and results_store_id
              jordan-normalization-09-resumed-a2-20261026T200824  done      finished  by results and results_store_id
              jordan-normalization-09-resumed-a-20261026T200457  killed    finished  by results
  ```

Outputs: [`09-compare-stores.txt`](jordan-normalization/out/09-compare-stores.txt),
[`09-store.txt`](jordan-normalization/out/09-store.txt) (every `store` command's output whole),
[`09-reference-store.sidecar.json`](jordan-normalization/out/09-reference-store.sidecar.json),
[`09-resumed-store.sidecar.json`](jordan-normalization/out/09-resumed-store.sidecar.json).

### 1.7 The stored values against the tuning layer (H3)

After the stop, `tools/history_and_bbn.py BETA M --phi-init-Mp X` once per history, serially, each
alone in its Bash call, every other option at its default (`main.py`'s), 20:18:59 → 20:26:03,
at a one-minute load of 6.6–10.7 ([`09-tool-*.txt`](jordan-normalization/out/)). The reader
(`histories.py`) on both stores, and `compare_tool.py` on the three
([`09-compare-tool.txt`](jordan-normalization/out/09-compare-tool.txt)), which ends
`SUMMARY: inputs all as the tool; converged all; bitwise all 7 x 15 x 2 equal`, exit 0.

- **The inputs are the tool's**: every row's `log10_tol` −8.0 for atol and rtol and −5.0 for the
  tuning tolerance; T* stored as `20000.0` (`0x1.3880000000000p+14`) GeV, which is (2 × 10⁴ ×
  `Planck_units().GeV`)/`GeV` by `float.hex`, and times `GeV` is 2 × 10⁴ × `GeV` by `float.hex`;
  π* 0.0; φ* 5.0 or 1.0; n 1; β as given; M as stored (`value_eV`) equal by `float.hex` to
  M × `PlanckMass`/`eV`; and each row's `z_samples` the tool's `samples=` (4620, 5161, 5973, 7328,
  4403, 5933, 5979).
- **Converged**: every row `failure` 0, `validated` 1, and `abs(residual_H) < tolerance and
  abs(residual_phi) < tolerance`, `tuning_converged`'s strict inequality
  (`ComputeTargets/ScalarHistory.py:1423-1429`); the largest |residual| is 9.425 × 10⁻⁶ (β = 3,
  M = 10⁻³, residual_φ).
- **Bitwise**: for each history, each of the `tuning` line's fifteen values against both stores'
  row: the floats by `float.hex` of the printed repr, the four counts and `seed_kind` exactly. **All
  210 comparisons equal.** No NaN was printed and no tuning column is NULL. The key-by-key table
  (the printed repr, its hex, and each store's hex) is in `09-compare-tool.txt`; for example,
  β = 5.5: `normalization -277.0569213884148 | -0x1.150e92666daf4p+8 | -0x1.150e92666daf4p+8 |
  -0x1.150e92666daf4p+8`.
- **Against 06** (measured): each stored normalization and φ_ref equals the RESULT hex of 06's
  `*_secant.log` for the six points 06 ran (β = 1.5, 3, 5.5 at M = 10⁻³; β = 0.5 at φ* = 1; β = 3
  at M = 0.1 and 10⁻⁵), and β = 0.5 at φ* = 5 equals note 09's probe line (`-276.7305358530105`,
  `0.06421012992791278`): all equal.
- **The BBN stage against the tool**: for the six histories with a BBN result, the tool's `bbn`
  line prints Yp and D/H equal to the stored values to its ten printed digits (β = 1.5:
  `Yp=0.2375649458 DoH=2.22274769`, stored 0.23756494579531887 and 2.222747690175223); for β = 0.5
  at φ* = 5 it prints `FAILURE network=small wall=1.0 s reason=PRyMordial: ComputationFailureError:
  T_in_MeV=0.00019824 MeV is smaller than T_BBN_spline_min=0.0002 MeV`, the stored reason.

### 1.8 The tuned values against README §0.3 (measured; the note's ruling 15)

| β | M/M_P | φ* | Λ (meV) | §0.3's Λ | Λ/Λ_§0.3 − 1 | φ_ref/M_P | §0.3's φ_ref | φ_ref/φ_ref,§0.3 − 1 |
|---|---|---|---|---|---|---|---|---|
| 1.5 | 1e-3 | 5 | 2.1253218 | 2.125913 | −2.7809e-04 | 0.0349623 | 0.0349624 | −3.8e-06 |
| 3 | 1e-3 | 5 | 2.0739961 | 2.074576 | −2.7954e-04 | 0.0236629 | 0.0236629 | +1.5e-07 |
| 5.5 | 1e-3 | 5 | 2.0206153 | 2.021177 | −2.7789e-04 | 0.0167194 | 0.0167195 | −8.3e-06 |
| 0.5 | 1e-3 | 1 | 2.1849491 | 2.185554 | −2.7679e-04 | 0.0638217 | 0.0638212 | +7.1e-06 |
| 3 | 0.1 | 5 | 1.3889187 | 1.389305 | −2.7804e-04 | 0.1477217 | 0.1477194 | +1.6e-05 |
| 3 | 1e-5 | 5 | 2.2264733 | 2.227091 | −2.7734e-04 | 0.0026705 | 0.0026705 | −8.6e-06 |

Λ is lower by 2.768–2.795 × 10⁻⁴ at every point, 05's units shift, and φ_ref agrees within
1.6 × 10⁻⁵, inside the tolerance on φ_ref/φ_min − 1. These are 06's figures to the digit (06's
table's last columns), as they must be, the values being 06's bitwise. §0.3 gives Λ to 7 digits and
φ_ref to 5–6 significant figures, so the last column resolves no better than about 2 × 10⁻⁵ at
M = 10⁻⁵ (φ_ref = 0.0026705). β = 0.5 at φ* = 5 has no §0.3 entry: Λ = 2.1924036 meV,
φ_ref = 0.0642101 M_P.

### 1.9 The BBN stage (the note's ruling 16)

Seven `BBNData` rows in each store ([`09-histories-reference.txt`](jordan-normalization/out/09-histories-reference.txt),
[`09-histories-resumed.txt`](jordan-normalization/out/09-histories-resumed.txt)), all validated,
`small_network` 1 on the six successes. **One failure, the same in both stores**: β = 0.5,
M = 10⁻³, φ* = 5, `failure 1`, reason `'PRyMordial: ComputationFailureError: T_in_MeV=0.00019824
MeV is smaller than T_BBN_spline_min=0.0002 MeV'`, as note 09 §0 expected and the tool prints. No
other BBN failure. The six successes' Yp and D/H are equal between the stores to the last digit
(e.g. β = 3, M = 0.1: `Yp 0.1724216306592671 D/H 1.0118770918897275` in both). The BBN stage's log
line counts no failure ("0 computations skipped because of a stored failure"): the failure is
stored, not skipped.

### 1.10 The silences, the stage times and the RayPoolKit wait (H4)

`silences.py` on the seven recorders ([`09-silences.txt`](jordan-normalization/out/09-silences.txt)).
The heartbeat values are the registry's own, to the second.

| Run | (a) Longest heartbeat silence, stage | (b) Longest line gap in the scalar stage | Other stages' longest gap |
|---|---|---|---|
| `reference-a` | 46 s, scalar (19:57:23 → 19:58:09) | 23.7 s (β = 5.5 it=2 → it=3) | start 28.2 s (the store's creation); BBN 24.7 s (the fingerprint) |
| `reference-b` | 24 s, after the BBN stage (20:00:50 → 20:01:14, the fingerprint) | 4.5 s | start 10.6 s; BBN 24.2 s |
| `reference-c` | **58 s**, scalar (20:02:19 → 20:03:17) | 42.8 s (M = 10⁻⁵ it=0 → it=1) | start 13.1 s; BBN 23.7 s |
| `resumed-a` | 36 s, scalar (20:05:28 → 20:06:04) | 22.2 s | start 29.7 s |
| `resumed-a2` | 45 s, scalar (20:08:37 → 20:09:22) | 23.7 s | start 10.1 s; BBN 24.2 s |
| `resumed-b` | 24 s, after the BBN stage (the fingerprint) | 4.5 s | start 10.1 s; BBN 24.7 s |
| `resumed-c` | 46 s, scalar (20:13:03 → 20:13:49) | 42.8 s | start 10.6 s; BBN 23.7 s |

**The longest heartbeat silence of the rehearsal is 58 s, against the 900 s window** (P12; not
changed). In the scalar stage the beats come from the forwarded tune lines, at most once per 30 s
(`RunDrivers/registered_run.py:304-312`); `reference-c`'s 58 s spans M = 10⁻⁵'s 42.8 s gap between
two tune lines, its integrations taking 42–45 s each (`wall=`). After the BBN stage the silence is the fingerprint at finish, 23–25 s for
every run.

**The stage times** (banner to `ALL WORK ITEMS COMPLETE`, as the recorder saw them) are the logs'
own to within 0.4 s: §1.2's table.

**The RayPoolKit wait** (P19). No row was stored and left unvalidated (§1.4). What was seen is a
**computed history waiting to be stored** behind a slower one, from each row's `timestamp` against
its last tune line's arrival:

| Run | History | Last tune line | Row stored | Wait |
|---|---|---|---|---|
| `reference-a` | β = 0.5 | 19:56:21.049 | 19:56:40.521 | 19.5 s (stored with β = 1.5, whose last line came 19:56:40.200) |
| `resumed-a` | β = 0.5 | 20:05:33.644 | 20:05:52.565 | 18.9 s (stored with β = 1.5, 20:05:51.792) |
| `reference-c` | M = 0.1 | 20:02:19.197 | 20:04:01.150 | **102.0 s** (stored with M = 10⁻⁵, 20:03:59.961) |
| `resumed-c` | M = 0.1 | 20:13:33.069 | 20:15:15.599 | **102.5 s** (stored with M = 10⁻⁵, 20:15:14.364) |

Every other history was stored within 1.1 s of its last line. An interruption inside such a wait
loses the waiting history, which a resume recomputes. The explanation (the queue's `ray.wait`
settings, `rrk-adoption` 04's observation) fits but is not measured here.

### 1.11 `histories.csv` and the plot

- **The plot**: `plot_by_beta.py` on the reference store, unregistered, in the foreground,
  20:16:05 → 20:16:28 (23 s), **exit 0** ([`09-plot-reference.log`](jordan-normalization/out/09-plot-reference.log)).
  It printed `>> Opened existing sharded datastore "…/jordan-normalization-09-reference.db" read-only
  with 16 shards (version label "2026.7.0", serial 1)`; six `!! plot: no ExponentialModel stored for
  M=…, n=1, β=…; no history can use it; skipped` lines and `!! plot: 6 of 12 ExponentialModel
  combinations of the stored M and β are not stored, and were skipped`; the SM baseline line
  `Yp=0.24688, D/H x1e5=2.45829, 3He/H x1e5=1.04193, 7Li/H x1e10=5.48637`; and, for M = 10⁻³ M_P,
  `1 model(s) dropped from the BBN plots` with `beta=0.5, M=2.4353e+24 eV, n=1: PRyMordial:
  ComputationFailureError: T_in_MeV=0.00019824 MeV is smaller than T_BBN_spline_min=0.0002 MeV`.
- **`histories.csv`**, `var/jordan-normalization-09/plot-reference/QCD_Cosmology/histories.csv`:
  SHA-256 **`02400fd7caadaffe6ed996c6cb717472514f7987c42c0e760630d5c431f7c77d`**, six rows (the six
  histories at φ* = 5), each row's `normalization` and `phi_ref_Mp` equal by `float.hex` to the
  stored values; β = 0.5's row carries the BBN failure in `failure_reasons` and no Yp
  ([`09-plot.txt`](jordan-normalization/out/09-plot.txt), [`09-histories.csv`](jordan-normalization/out/09-histories.csv)).
  The file as written has CRLF line endings (Python's `csv` module); the committed copy is stored
  with LF (this repository's `core.autocrlf input`), and its blob's SHA-256 is
  `124328fe2802072c09e958ddea5f694ccccf6e34465a6dc83fd55f2148ab457d`. The digest above is the
  written file's.
- **The reference store, unchanged by the plot**: `shasum -a 256` of its 17 files and its sidecar
  before and after are equal (`diff` exit 0).

### 1.12 The token checks, the suites, and what was written

- **Token** ([`09-token.txt`](jordan-normalization/out/09-token.txt)): `grep -c` of `Token
  authentication is enabled`, `Generated new authentication token`, `InvalidAuthToken` and
  `AuthenticationError` in `start.txt`, the seven launch logs and the plot log: 0 for each of the
  36. Every driver attached (`Connected to Ray cluster.` once in each log; no log names 6379). The
  token file, after the tool runs: `-rw------- 1 ds283 staff 64 Oct 25 12:51`, `/usr/bin/stat -f '%m
  %Sm'` `1792932677 Oct 25 12:51:17 2026`, as at the Gate. `grep -r -F -l -f ~/.ray/auth_token
  .documents/jordan-normalization/ .documents/jordan-normalization-verification.md
  prompts/jordan-normalization/logs/09-rehearse-a-tuned-build.md`, run after every record was
  written: no output, **exit 1** (the log, "Verification performed").
- **The suites**, serially, in the foreground, never with the cluster up
  ([`09-suites.txt`](jordan-normalization/out/09-suites.txt)); the time is `unittest`'s, the load
  the one-minute load before the root:

  | Root | Tests | Before `start` | After `stop` |
  |---|---|---|---|
  | CosmologyModels | 26 | OK, 67.0 s; 3.07 | OK, 64.4 s; 3.31 |
  | ComputeTargets `test_[a-f]*` | 76 | OK, 95.2 s; 3.06 | OK, 94.4 s; 4.29 |
  | ComputeTargets `test_[g-z]*` | 167 | OK, 216.2 s; 2.92 | OK, 214.9 s; 3.76 |
  | ObjectFactories | 93 | OK, 14.2 s; 8.36 | OK, 14.3 s; 7.73 |
  | tools | 115 | OK, 113.9 s; 7.69 | OK, 115.8 s; 6.40 |
  | RunDrivers | 64 | OK, 23.3 s; 3.40 | OK, 24.6 s; 6.02 |

  08's counts exactly. Nothing failed, so nothing was re-run. `var/runs` held the same entries
  before and after each set (the RunDrivers suite registered nothing).
- **Written**: `var/datastores/` (the two stores, 17 files each, and their sidecars; 206 MB with the
  four sidecars of dropped stores, which were not opened), `var/runs/` (seven run directories),
  `var/jordan-normalization-09/` (2.5 MB: the launch logs, pid files, recorders, the signaller's
  log, the plot's log and outputs, the tool outputs, the working outputs and the helpers), the lock
  under `~/.raypoolkit/`, taken by `start` and released by `stop`, and the cluster's temp dir.
  Nothing under `var/` was deleted. No other store was opened. The tree was clean throughout.

### 1.13 Observed, not acted on

- **Ray's log deduplication drops tune lines of parallel histories.** Ray forwards worker output
  through a deduplicator that keys a line on its words without digits
  (`ray/_private/ray_logging/__init__.py`, `_canonicalise_log_line`) and, for a key already seen
  from another process within 5 s, prints only the latest line with `[repeated Nx across
  cluster]`. Every tune line has the same key, so in `reference-a` and in `resumed-a` β = 1.5's seed
  line (it=0) was swallowed into β = 3's (`… wall=8.8 [repeated 2x across cluster]`), and the log
  holds three of β = 1.5's four lines. The stored `tuning_iterations` is right; the log's per-line
  record is not complete when histories run in parallel. Opened as
  `[09-ray-deduplicates-the-tune-lines-of-parallel-histories]` (board §3).
- **The fingerprint at finish takes 23–25 s**, against 7–8 s on `ray-sqla-align` 02's pilot store:
  this store's `redshift` table holds 75001 rows (300 decades at 250 per decade) against 21250.
  It is the longest silence after the BBN stage, and far inside the window.
- **A `SIGTERM`ed build's worker runs on** while the wrapper records and fingerprints: β = 5.5's
  `it=3` line arrived 20 s after the `killed` record and beat the heartbeat; its work is lost, and
  `resumed-a2` recomputed the history from its seed (five integrations, 107 s).
- **The stop needed `SIGKILL`** after its 15 s grace, for the `gcs_server` (35393) and 35394, as at
  `ray-sqla-align` 02. The script then found no process and the four ports free, released the lock,
  and exited 0.
- **`plot_by_beta.py` prints `!! WARNING: number of shards read from database (=16) does not match
  specified number of shards (=10)`** although it was given `--shards 16`, as in `rrk-adoption` 04's
  and `ray-sqla-align` 02's plot logs. The plot read all 16 shards.
- **`histories.csv`'s `M_Mp` for M = 10⁻⁵ is `9.999999999999999e-06`**, the stored eV value
  converted back; M = 10⁻³ and 0.1 print exactly.
- **`ray status` reports `0.0/1.0 GPU`**, as `ray-sqla-align` 01 and 02 recorded.

### 1.14 The commands, verbatim

From the repository root. `<scratch>` is `var/jordan-normalization-09/tools/`.
`ray-sqla-align` 02's `wait_for_line.py` and `ray_state.py` were copied there unchanged (`cmp` 0
against `ray-sqla-align/out/02-*.py.txt`) and are committed again as
[`09-wait_for_line.py.txt`](jordan-normalization/out/09-wait_for_line.py.txt) and
[`09-ray_state.py.txt`](jordan-normalization/out/09-ray_state.py.txt); its `check.sh` and
`suite.sh` were adapted to `var/jordan-normalization-09` and `09-` names
([`09-check.sh.txt`](jordan-normalization/out/09-check.sh.txt),
[`09-suite.sh.txt`](jordan-normalization/out/09-suite.sh.txt); the diffs
[`09-check.sh.diff`](jordan-normalization/out/09-check.sh.diff),
[`09-suite.sh.diff`](jordan-normalization/out/09-suite.sh.diff)). Written here:
[`09-beat_times.py.txt`](jordan-normalization/out/09-beat_times.py.txt) (the recorder),
[`09-silences.py.txt`](jordan-normalization/out/09-silences.py.txt) (its summariser),
[`09-histories.py.txt`](jordan-normalization/out/09-histories.py.txt) (the stored-values reader;
these three stdlib only) and [`09-compare_tool.py.txt`](jordan-normalization/out/09-compare_tool.py.txt)
(the comparison; it imports `Units`). The committed logs have ANSI colour codes removed
(`sed -E 's/\x1b\[[0-9;]*m//g'`), and are otherwise verbatim; the `.json` files are copied
unchanged; the recorders' outputs are `09-<slug>.beats.txt`.

```bash
# the Gate (its record, 09-gate.txt, also holds git log, git diff --stat, shasum, list, the token file's stat)
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status > var/jordan-normalization-09/gate-status.txt 2>&1; echo $?
./venv/bin/python -m raypoolkit.rehearsal_lock status > var/jordan-normalization-09/gate-lock-status.txt 2>&1; echo $?

# the suites, each root in its own Bash call (<scratch>/suite.sh runs CLAUDE.md's command); "after" likewise
sh <scratch>/suite.sh before CosmologyModels CosmologyModels/tests
sh <scratch>/suite.sh before ComputeTargets-a-f ComputeTargets/tests 'test_[a-f]*.py'
sh <scratch>/suite.sh before ComputeTargets-g-z ComputeTargets/tests 'test_[g-z]*.py'
sh <scratch>/suite.sh before ObjectFactories ObjectFactories/tests
sh <scratch>/suite.sh before tools tools/tests
sh <scratch>/suite.sh before RunDrivers RunDrivers/tests

# the cluster: each command alone in its Bash call
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh start "jordan-normalization 09" > var/jordan-normalization-09/start.txt 2>&1; echo $?
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status > var/jordan-normalization-09/status-up.txt 2>&1; echo $?
ps -E -ww -o command= -p 35393 | grep -o 'RAY_AUTH_MODE=[a-z]*'; ps -E -ww -o command= -p 35427 | grep -o 'RAY_AUTH_MODE=[a-z]*'
RAY_AUTH_MODE=token ./venv/bin/ray status --address 127.0.0.1:6380 > var/jordan-normalization-09/ray-status.txt 2>&1; echo $?
RAY_AUTH_MODE=token ./venv/bin/python <scratch>/ray_state.py

# each launch: the load, then one Bash call for the build, one for its recorder (and, for resumed-a, one for interrupt_when.py)
echo "reference-a launch: $(date '+%F %T %z') $(uptime)" | tee -a var/jordan-normalization-09/09-load.txt
export ADDRESS=127.0.0.1:6380 S=$PWD/var/datastores L=$PWD/var/jordan-normalization-09 && nohup sh -c 'PYTHONPATH=. ./venv/bin/python -u RunDrivers/registered_run.py --campaign jordan-normalization --prompt 09 --register reference-a --purpose "jordan-normalization 09 H1: run a of the reference build (beta 0.5 1.5 3 5.5 at M 1e-3 Mp, phi* 5)" -- --config exponential.yaml --database $S/jordan-normalization-09-reference.db --shards 16 --job-name jordan-normalization-09-reference-a --ray-address $ADDRESS --M-values-Mp 1e-3 --beta-values 0.5 1.5 3 5.5 --phi-init-Mp 5 > $L/reference-a.log 2>&1 & echo $! > $L/reference-a.pid; wait $!; echo "** launcher saw exit status $?" >> $L/reference-a.log' > /dev/null 2>&1 &
export L=$PWD/var/jordan-normalization-09 && nohup ./venv/bin/python -u $L/tools/beat_times.py $L/reference-a.log $L/reference-a.beats > $L/reference-a.beats.err 2>&1 &
./venv/bin/python <scratch>/wait_for_line.py var/jordan-normalization-09/reference-a.log "** registered_run: registered as" --timeout 120
sh <scratch>/check.sh reference-a
./venv/bin/python <scratch>/wait_for_line.py var/jordan-normalization-09/reference-a.log "** launcher saw exit status" --timeout 570
./venv/bin/python <scratch>/silences.py var/jordan-normalization-09/reference-a.beats
./venv/bin/python -m runregistrykit store create var/datastores/jordan-normalization-09-reference.db --purpose "jordan-normalization 09 H1: the reference build of the tuned pilot grid (runs a, b, c), kept for the user"
./venv/bin/python -m runregistrykit store show var/datastores/jordan-normalization-09-reference.db

export ADDRESS=127.0.0.1:6380 S=$PWD/var/datastores L=$PWD/var/jordan-normalization-09 && nohup sh -c 'PYTHONPATH=. ./venv/bin/python -u RunDrivers/registered_run.py --campaign jordan-normalization --prompt 09 --register reference-b --purpose "jordan-normalization 09 H1: run b of the reference build (beta 0.5 at M 1e-3 Mp, phi* 1)" -- --config exponential.yaml --database $S/jordan-normalization-09-reference.db --shards 16 --job-name jordan-normalization-09-reference-b --ray-address $ADDRESS --M-values-Mp 1e-3 --beta-values 0.5 --phi-init-Mp 1 > $L/reference-b.log 2>&1 & echo $! > $L/reference-b.pid; wait $!; echo "** launcher saw exit status $?" >> $L/reference-b.log' > /dev/null 2>&1 &
export ADDRESS=127.0.0.1:6380 S=$PWD/var/datastores L=$PWD/var/jordan-normalization-09 && nohup sh -c 'PYTHONPATH=. ./venv/bin/python -u RunDrivers/registered_run.py --campaign jordan-normalization --prompt 09 --register reference-c --purpose "jordan-normalization 09 H1: run c of the reference build (beta 3 at M 0.1 and 1e-5 Mp, phi* 5)" -- --config exponential.yaml --database $S/jordan-normalization-09-reference.db --shards 16 --job-name jordan-normalization-09-reference-c --ray-address $ADDRESS --M-values-Mp 0.1 1e-5 --beta-values 3 --phi-init-Mp 5 > $L/reference-c.log 2>&1 & echo $! > $L/reference-c.pid; wait $!; echo "** launcher saw exit status $?" >> $L/reference-c.log' > /dev/null 2>&1 &
export ADDRESS=127.0.0.1:6380 S=$PWD/var/datastores L=$PWD/var/jordan-normalization-09 && nohup sh -c 'PYTHONPATH=. ./venv/bin/python -u RunDrivers/registered_run.py --campaign jordan-normalization --prompt 09 --register resumed-a --purpose "jordan-normalization 09 H2: run a into the resumed store, interrupted with SIGTERM in the scalar stage" -- --config exponential.yaml --database $S/jordan-normalization-09-resumed.db --shards 16 --job-name jordan-normalization-09-resumed-a --ray-address $ADDRESS --M-values-Mp 1e-3 --beta-values 0.5 1.5 3 5.5 --phi-init-Mp 5 > $L/resumed-a.log 2>&1 & echo $! > $L/resumed-a.pid; wait $!; echo "** launcher saw exit status $?" >> $L/resumed-a.log' > /dev/null 2>&1 &
export L=$PWD/var/jordan-normalization-09 && nohup ./venv/bin/python .documents/dk-and-rpk-migration/rehearsal/interrupt_when.py $L/resumed-a.log $L/resumed-a.pid --banner "CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID" --after 90 > $L/resumed-a.interrupt.log 2>&1 &
RAY_AUTH_MODE=token ./venv/bin/python <scratch>/ray_state.py
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status > var/jordan-normalization-09/status-after-resumed-a.txt 2>&1; echo $?
./venv/bin/python <scratch>/histories.py var/datastores/jordan-normalization-09-resumed > var/jordan-normalization-09/histories-resumed-after-a.txt 2>&1
./venv/bin/python -m runregistrykit store create var/datastores/jordan-normalization-09-resumed.db --purpose "jordan-normalization 09 H2: the tuned pilot grid built by a resume after SIGTERM (runs a, a2, b, c), kept for the user"
./venv/bin/python -m runregistrykit store show var/datastores/jordan-normalization-09-resumed.db
export ADDRESS=127.0.0.1:6380 S=$PWD/var/datastores L=$PWD/var/jordan-normalization-09 && nohup sh -c 'PYTHONPATH=. ./venv/bin/python -u RunDrivers/registered_run.py --campaign jordan-normalization --prompt 09 --register resumed-a2 --purpose "jordan-normalization 09 H2: run a again into the resumed store, the resume after SIGTERM" -- --config exponential.yaml --database $S/jordan-normalization-09-resumed.db --shards 16 --job-name jordan-normalization-09-resumed-a2 --ray-address $ADDRESS --M-values-Mp 1e-3 --beta-values 0.5 1.5 3 5.5 --phi-init-Mp 5 --prune-unvalidated > $L/resumed-a2.log 2>&1 & echo $! > $L/resumed-a2.pid; wait $!; echo "** launcher saw exit status $?" >> $L/resumed-a2.log' > /dev/null 2>&1 &
export ADDRESS=127.0.0.1:6380 S=$PWD/var/datastores L=$PWD/var/jordan-normalization-09 && nohup sh -c 'PYTHONPATH=. ./venv/bin/python -u RunDrivers/registered_run.py --campaign jordan-normalization --prompt 09 --register resumed-b --purpose "jordan-normalization 09 H2: run b into the resumed store (beta 0.5 at M 1e-3 Mp, phi* 1)" -- --config exponential.yaml --database $S/jordan-normalization-09-resumed.db --shards 16 --job-name jordan-normalization-09-resumed-b --ray-address $ADDRESS --M-values-Mp 1e-3 --beta-values 0.5 --phi-init-Mp 1 > $L/resumed-b.log 2>&1 & echo $! > $L/resumed-b.pid; wait $!; echo "** launcher saw exit status $?" >> $L/resumed-b.log' > /dev/null 2>&1 &
export ADDRESS=127.0.0.1:6380 S=$PWD/var/datastores L=$PWD/var/jordan-normalization-09 && nohup sh -c 'PYTHONPATH=. ./venv/bin/python -u RunDrivers/registered_run.py --campaign jordan-normalization --prompt 09 --register resumed-c --purpose "jordan-normalization 09 H2: run c into the resumed store (beta 3 at M 0.1 and 1e-5 Mp, phi* 5)" -- --config exponential.yaml --database $S/jordan-normalization-09-resumed.db --shards 16 --job-name jordan-normalization-09-resumed-c --ray-address $ADDRESS --M-values-Mp 0.1 1e-5 --beta-values 3 --phi-init-Mp 5 > $L/resumed-c.log 2>&1 & echo $! > $L/resumed-c.pid; wait $!; echo "** launcher saw exit status $?" >> $L/resumed-c.log' > /dev/null 2>&1 &
# each followed by its recorder, the registration wait, the one check and the end wait, as for reference-a

# the plot
shasum -a 256 var/datastores/jordan-normalization-09-reference*
export ADDRESS=127.0.0.1:6380 S=$PWD/var/datastores L=$PWD/var/jordan-normalization-09 && PYTHONPATH=. ./venv/bin/python -u plot_by_beta.py --config exponential.yaml --database $S/jordan-normalization-09-reference.db --shards 16 --phi-init-Mp 5 --output $L/plot-reference --ray-address $ADDRESS > $L/plot-reference.log 2>&1
shasum -a 256 var/datastores/jordan-normalization-09-reference*
RAY_AUTH_MODE=token ./venv/bin/python <scratch>/ray_state.py
kill -0 35782; kill -0 36512; kill -0 36956; kill -0 37417; kill -0 37955; kill -0 38353; kill -0 38606

# the stop: each command alone in its Bash call
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh stop > var/jordan-normalization-09/stop.txt 2>&1; echo $?
.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status > var/jordan-normalization-09/status-after.txt 2>&1; echo $?
./venv/bin/python -m raypoolkit.rehearsal_lock status > var/jordan-normalization-09/lock-status-after.txt 2>&1; echo $?

# the comparisons
PYTHONPATH=. ./venv/bin/python tools/compare_stores.py var/datastores/jordan-normalization-09-reference.db var/datastores/jordan-normalization-09-resumed.db
./venv/bin/python -m runregistrykit store fingerprint var/datastores/jordan-normalization-09-reference.db
./venv/bin/python -m runregistrykit store fingerprint var/datastores/jordan-normalization-09-resumed.db
./venv/bin/python -m runregistrykit store show var/datastores/jordan-normalization-09-reference.db
./venv/bin/python -m runregistrykit store show var/datastores/jordan-normalization-09-resumed.db
./venv/bin/python .documents/dk-and-rpk-migration/rehearsal/stage_counts.py var/jordan-normalization-09/{reference-a,reference-b,reference-c,resumed-a,resumed-a2,resumed-b,resumed-c}.log
./venv/bin/python <scratch>/histories.py var/datastores/jordan-normalization-09-reference
./venv/bin/python <scratch>/histories.py var/datastores/jordan-normalization-09-resumed
# the seven tool runs, each alone in its Bash call, output to var/jordan-normalization-09/tool/<beta>-<M>-phi<X>.txt with uptime
PYTHONPATH=. ./venv/bin/python tools/history_and_bbn.py 0.5 1e-3 --phi-init-Mp 5
PYTHONPATH=. ./venv/bin/python tools/history_and_bbn.py 1.5 1e-3 --phi-init-Mp 5
PYTHONPATH=. ./venv/bin/python tools/history_and_bbn.py 3 1e-3 --phi-init-Mp 5
PYTHONPATH=. ./venv/bin/python tools/history_and_bbn.py 5.5 1e-3 --phi-init-Mp 5
PYTHONPATH=. ./venv/bin/python tools/history_and_bbn.py 0.5 1e-3 --phi-init-Mp 1
PYTHONPATH=. ./venv/bin/python tools/history_and_bbn.py 3 0.1 --phi-init-Mp 5
PYTHONPATH=. ./venv/bin/python tools/history_and_bbn.py 3 1e-5 --phi-init-Mp 5
PYTHONPATH=. ./venv/bin/python <scratch>/compare_tool.py var/jordan-normalization-09/histories-reference.txt var/jordan-normalization-09/histories-resumed.txt var/jordan-normalization-09/tool prompts/jordan-normalization/logs/06-probes
./venv/bin/python <scratch>/silences.py var/jordan-normalization-09/{reference-a,reference-b,reference-c,resumed-a,resumed-a2,resumed-b,resumed-c}.beats

# the suites again, as above, with "after"; then
./venv/bin/python -m runregistrykit list
grep -r -F -l -f ~/.ray/auth_token .documents/jordan-normalization/ .documents/jordan-normalization-verification.md prompts/jordan-normalization/logs/09-rehearse-a-tuned-build.md
```
