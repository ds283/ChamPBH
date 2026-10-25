# Log 02 — rehearse registered runs under Ray 2.59

**Subject:** Rehearse registered builds under ray 2.59.0 and SQLAlchemy 2.0.55
**Commit:** `e04fbb8`, on branch `ray-sqla-align`, on top of the dispatch commit `b16c435`
**Date:** 2026-10-25 (the rehearsal, 20:53–21:23 GMT), by Claude Opus 5.5
**Result:** **COMPLETE.** R1–R5 and W1 shipped; gate **FP** holds, and gate **Token**'s 02 part
holds. Three builds through the wrapper on ChamPBH's isolated cluster, in Ray's token mode, under
`ray` 2.59.0 and `SQLAlchemy` 2.0.55: `reference` `done`/0; `resumed-a` `killed`/143; `resumed-c`
`done`/0, computing the two missing histories. Both stores fingerprint to
`2dd4b39b61b9cc1b39bfcf5068ca2d00e84b52fee9da61958a11781931b0932f`, `rrk-adoption` 04's;
`compare_stores`: `RESULT: equal`; `histories.csv` `d7c62c37…c522`, R0's. No token warning or
authentication error anywhere; the token file unchanged. The suites 18, 61 + 115, 78, 102 and 61,
OK before and after. No §4 stop condition was met. No code changed; no issue opened, so
`.documents/OPEN_ISSUES.md` is unchanged.

The orchestrator's note for 02 (its §0, rulings 1–19) governed where it differs from the prompt.
Each ruling applied is classified below as the note gives it. The record is
[`.documents/ray-sqla-align-verification.md`](../../../.documents/ray-sqla-align-verification.md)
§1; the figures below are its. Outputs are in
[`.documents/ray-sqla-align/out/02-*`](../../../.documents/ray-sqla-align/out/), written while
working to `var/ray-sqla-align-02/` (ignored) and copied before the token grep.

## What shipped

- **R1 — `reference`**: `ray-sqla-align-02-reference-20261025T210149`, pid 11468, into
  `var/datastores/ray-sqla-align-02-reference.db`; `done`/0; status fingerprint `2dd4b39b…932f`;
  after `store create` (`store_id` `16b5fa1051ce4a7090c2b6b370c6baf7`) and `store fingerprint
  --write`, the sidecar's and `store fingerprint`'s are the same digest.
- **R2 — `resumed-a`**: `ray-sqla-align-02-resumed-a-20261025T210611`, pid 12370, into
  `var/datastores/ray-sqla-align-02-resumed.db`, `SIGTERM`ed 90 s after the scalar banner;
  `killed`/143, the handler's line in the same second as the signal; the store partial (β = 1.1,
  1.15, 1.2 validated, β = 10 stored unvalidated, β = 25 absent); status fingerprint `0d612337…0f13`,
  the digest 04's `resumed-a` recorded. Then `store create` (`store_id`
  `9919583b8b04499da3e286c0814a0114`).
- **R3 — `resumed-c`**: `ray-sqla-align-02-resumed-c-20261025T210916`, pid 13137, with
  `--prune-unvalidated`; the prune report naming β = 10; `done`/0; scalar 5 / 2 / 2 / 2, adiabatic
  and BBN 5 / 5 / 5 / 5; `fingerprint_sidecar` `written`; `2dd4b39b…932f` in the status, the sidecar
  and `store fingerprint`; `results_store_id` the resumed store's.
- **R4 — the comparisons**: `compare_stores` `RESULT: equal`, `problems: none`, its table and
  bookkeeping list 04's line for line; the two sidecars' `classes` equal as JSON, and each store's
  per-class table 04's; the three manifests `git_head` `b16c4355…`, `git_dirty` false,
  `script_sha256` `685b615b…7bbe`, `packages` seven with `ray` 2.59.0 and `SQLAlchemy` 2.0.55.
- **R5 — the plot**: `plot_by_beta.py` on the reference store, exit 0, 25 s; `histories.csv`
  `d7c62c37d8ad637a63854583ab11bc737109dcbfba92168dbddbaea75182c522`; the store's 17 files and its
  sidecar unchanged (`shasum` before and after, `diff` exit 0).
- **W1 — `.documents/ray-sqla-align-verification.md`** (new), §1, laid out as the note's ruling 19
  says.
- **The records**: 34 files `.documents/ray-sqla-align/out/02-*` (the largest, `02-store.txt`, is
  36 KB), this log, and the board.

## The runs

| Run id | Slug | Store | pid | State | Exit | Launched → ended (GMT) | Wall | `uptime` at launch (1 / 5 / 15) | `results_store_id` | Fingerprint at finish |
|---|---|---|---|---|---|---|---|---|---|---|
| `ray-sqla-align-02-reference-20261025T210149` | `reference` | `reference` | 11468 | `done` | 0 | 21:01:48 → 21:05:24 | 3 min 36 s | 5.48 / 5.14 / 5.37 | `null` | `2dd4b39b…932f`, 21:05:16 → 21:05:23 |
| `ray-sqla-align-02-resumed-a-20261025T210611` | `resumed-a` | `resumed` | 12370 | `killed` | 143 | 21:06:10 → 21:08:45 | 2 min 35 s | 6.98 / 10.78 / 8.41 | `null` | `0d612337…0f13`, 21:08:36 → 21:08:44 |
| `ray-sqla-align-02-resumed-c-20261025T210916` | `resumed-c` | `resumed` | 13137 | `done` | 0 | 21:09:15 → 21:12:27 | 3 min 12 s | 7.20 / 11.95 / 9.60 | `9919583b8b04499da3e286c0814a0114` | `2dd4b39b…932f`, 21:12:19 → 21:12:26 |
| `plot_by_beta.py` (unregistered) | — | `reference`, read-only | 13927 | — | 0 | 21:12:42 → 21:13:07 | 25 s | 13.01 / 11.40 / 9.74 | — | — |

`list`, each `status.json` and each launch log agree on every state and exit. The plot's pid is the
driver pid Ray's job `04000000` records.

## Deviations from the prompt

**The orchestrator's rulings** (its note, §0):

1. **`STRUCTURALLY REQUIRED`** (ruling 1). The shell's `stat` and `date` are GNU's. The token file's
   mtime was always `/usr/bin/stat -f '%m %Sm' ~/.ray/auth_token` (`1792932677 Oct 25 12:51:17 2026`
   at the Gate and at the end), and the birth times of the logs and manifests `/usr/bin/stat -f
   '%SB'`.
2. **`IMPLEMENTATION CHOICE`, ruled** (ruling 2). 04's `wait_for_line.py`, `compute_times.py` and
   `ray_state.py` copied unchanged to `var/ray-sqla-align-02/tools/`, each `cmp` 0 against its
   `04-*.py.txt`; `ray_state.py` always run as `RAY_AUTH_MODE=token ./venv/bin/python …`. Three
   helpers written there as the ruling allows, and copied to `out/` as `.txt`: `suite.sh` (one
   root, ruling 13), `check.sh` (the one check, ruling 4) and `log_kinds.py` (ruling 16).
3. **`IMPLEMENTATION CHOICE`, ruled** (ruling 3). Each wait one foreground call of
   `wait_for_line.py` at a 600 000 ms tool timeout: `--timeout 120` for `** registered_run:
   registered as` (each seen at once, 0.0 s), `--timeout 570` for `** launcher saw exit status`
   (200.9 s, 143.6 s, 184.7 s). None timed out, so none was repeated. No `sleep` loop, `pgrep`,
   `tail -f`, `Monitor` or `run_in_background`.
4. **`IMPLEMENTATION CHOICE`** (ruling 4). The one check, a single Bash call of `check.sh` after
   each registration wait: the manifest's path from the log's `**   manifest` line and its `ls -l`;
   `status.json`'s `state`, `pid`, `heartbeat`, `stage`; the pid file; the manifest's `created` and
   `results_store_id`; the log's and the manifest's birth times; `list`; the log's first four lines.
   [`02-checks.txt`](../../../.documents/ray-sqla-align/out/02-checks.txt).
5. **`IMPLEMENTATION CHOICE`** (ruling 5). Each launch one Bash call exporting
   `ADDRESS=127.0.0.1:6380`, `S=$PWD/var/datastores` and `L=$PWD/var/ray-sqla-align-02`, absolute,
   with 04's `nohup sh -c '…'` form; `interrupt_when.py` with `nohup` in its own Bash call right
   after `resumed-a`'s. Each recorded exactly as run in the verification document §1.10 (each
   launch call was preceded, in the same call, by a `date`/`uptime` append to
   `var/ray-sqla-align-02/launch-uptimes.txt`).
6. **`IMPLEMENTATION CHOICE`, ruled** (ruling 6). `t10 = 42.45` s, `t25 = 107.07` s; the gap
   between `t10 + 20` (62.45 s) and `t25 + 20` (127.07 s) is 64.62 s, over 60; `(t10 + t25)/2 + 20 =
   94.76` s, rounded to 10 s: **`<s>` = 90 s**. Outcome as expected: β = 1.1, 1.15, 1.2 stored and
   validated, β = 10 stored with `validated 0`, β = 25 not stored.
7. **`NOTED`** (ruling 7). `sent SIGTERM` 21:08:36 UTC; the handler's line `** SIGTERM at
   2026-10-25 21:08:36 +0000`: **0 s**. Signal to the final heartbeat (21:08:44): **8 s**. The lines
   after the banner are the handler's, `** SIGTERM: main.py ended with SystemExit(1); recording
   killed, exit code 143`, one forwarded `Datastore` line, `** driver exit status 143`, `** launcher
   saw exit status 143`: 04's. No `*** SIGTERM received` banner and no `** cancelled an alarm` line.
8. **`STRUCTURALLY REQUIRED`** (ruling 8). `results_store_id`: `reference` `null`, `resumed-a`
   `null`, `resumed-c` `9919583b8b04499da3e286c0814a0114`, the resumed store's `store_id` as `store
   create` printed it.
9. **`NOTED`** (ruling 9). β = 10 was stored unvalidated, so `resumed-c`'s open printed the
   `INTEGRITY WARNING` on `shard0002-store` naming it (`store_id=61`, `contains 9776 z-sample values
   | expected=9776`) and `** Note: these values have been pruned from the datastore.`; the scalar
   stage computed 2; adiabatic and BBN 5 / 5 / 5 / 5.
10. **`STRUCTURALLY REQUIRED`** (ruling 10). `ray_cluster.sh start`, `stop` and `status`, and the
    lock module's `status`, each ran alone in its Bash call, `> var/ray-sqla-align-02/<name>.txt
    2>&1`, the exit by `echo $?`, the file read in a later call; `start` once, `stop` once. No
    command line of mine named the cluster's temp dir: the `gcs_server`'s pid (10995) came from
    `start.txt`, the `raylet`'s (11031) from the saved `status` listing. The `ps -E` checks gave
    `RAY_AUTH_MODE=token` and a control of 1 for each. The builds, the waits, `interrupt_when.py`,
    `ray status`, `ray_state.py` and the `ps` checks ran in Bash calls of their own; every Ray CLI
    command carried `RAY_AUTH_MODE=token`. `stop` ran once 11468, 12370 and 13137 answered `kill -0`
    with `no such process` and `interrupt_when.py` had printed its `sent` line. `start` exit 0;
    `stop` exit 0 with `SIGKILL to: 10995 11000` after the grace, confirming no process and the
    ports free and releasing the lock; `status` exit 0, the lock free. No `ray stop`.
11. **`IMPLEMENTATION CHOICE`, ruled** (ruling 11). `ray_state.py` at its three points: after
    `start` no job, worker or actor; after `resumed-a`'s launcher line 2 jobs `SUCCEEDED`, workers
    44 / 0 alive, actors 34 / 0 `ALIVE`, so nothing to wait for; after the plot 4 jobs `SUCCEEDED`,
    workers 86 / 0 alive, actors 67 / 0 `ALIVE`.
12. **`NOTED`** (ruling 12). One-minute load: 2.44 at the Gate, 3.54–11.89 before the suites'
    roots, **8.07 before `start`** (under 20), 5.48 / 6.98 / 7.20 at the three launches, 13.01 at the
    plot, 8.35 before `stop`, 37.04–4.19 before the suites after (another workload's; the cluster was
    stopped).
13. **`IMPLEMENTATION CHOICE`** (ruling 13). `CLAUDE.md`'s commands, serially, in the foreground,
    never with the cluster up, `ComputeTargets` in two halves, each Bash call at 600 000 ms, through
    `suite.sh`: output to `var/ray-sqla-align-02/suites/<point>-<root>.txt`, a summary (the command,
    `uptime`, the wall time, `Ran`, the verdict, the exit) appended to `02-suites.txt`. Nothing
    failed; nothing re-run.
14. **`NOTED`** (ruling 14). The manifests as expected: `script` `main.py`, `script_sha256`
    `685b615b…7bbe` = `shasum -a 256 main.py`; `git_head` `b16c4355ff91adbf8d27a08e5956e0020fc0a716`;
    `git_dirty` **false** in all three (and in every fingerprint's `taken` and every sidecar
    history); `packages` seven as listed; `results` relative. Each `>> packages:` line `cmp`-equal
    to 01's `01-distributions.txt`.
15. **`NOTED`** (ruling 15). `compare_stores` `RESULT: equal`, `problems: none`, the row counts and
    bookkeeping list 04's; `store fingerprint` without `--write` on each closed store
    `2dd4b39b…932f`, each "matches" its sidecar; the sidecars' `classes` equal as JSON.
16. **`IMPLEMENTATION CHOICE`, ruled** (ruling 16). The log kinds by `log_kinds.py` (ANSI stripped,
    the campaign prefixes `ray-sqla-align-02` / `rrk-adoption-04` replaced by `<P>`, every digit run
    by `#`), each 02 log against 04's at the same slug and the plot log against
    `04-plot-reference.log`. Below, "What is new in the logs".
17. **`STRUCTURALLY REQUIRED`** (ruling 17). The token grep run as `grep -r -F -l -f
    ~/.ray/auth_token .documents/ray-sqla-align/ .documents/ray-sqla-align-verification.md
    prompts/ray-sqla-align/`, after every record of the commit was written (this log and the board
    included): no output, exit 1.
18. **`STRUCTURALLY REQUIRED`** (ruling 18). The board's section is `### 5.2 After prompt 02
    (2026-10-25)`, not the prompt's `### 5.3`.
19. **`NOTED`** (ruling 19). The verification document new, its header like
    `rrk-adoption-verification.md`'s, its §1 in the ruling's order, less the hard-kill parts.

**My own:**

20. **`IMPLEMENTATION CHOICE`.** The lock module's `status` (`./venv/bin/python -m
    raypoolkit.rehearsal_lock status`) was run at the Gate and after `stop`, besides the script's
    `status`, each alone in its call. Both said `lock: free`.
21. **`IMPLEMENTATION CHOICE`.** Records beyond those the note lists: `02-gate.txt` (the Gate
    re-checked at 20:53), `02-checks.txt`, `02-manifests.txt` (the manifests' fields side by side),
    `02-plot.txt` (the plot's exit, `histories.csv`'s hash, the store's `shasum`s), `02-load.txt` and
    `02-after.txt` (the venv, `list`, the tree and `var/` at the end). The two store sidecars are
    committed as `02-reference-store.sidecar.json` and `02-resumed-store.sidecar.json`, so that they
    are not confused with the runs' `02-<slug>.manifest.json`.
22. **`IMPLEMENTATION CHOICE`.** Beyond the prompt, two comparisons with 04's committed records,
    read only: each store's `store fingerprint` per-class table against 04's in `04-store.txt`, and
    `compare_stores`' output from `tables:` to `RESULT` against `04-compare-stores.txt`. All three
    `diff`s empty. `rrk-adoption` 04's two sidecars in `var/datastores/` were not opened.
23. **`UNINTENDED DRIFT`**, in a record only. The plot's call timed itself with `date -r EPOCH`,
    which GNU `date` reads as a file name, so the call printed `date: 1792962762: No such file or
    directory` and `… 1792962787 …` and an empty time span. The plot itself ran and exited 0
    (`plot exit 0, 25 s`). `02-plot.txt`'s time line was written afterwards from those two epochs
    with `date -d @EPOCH`: 21:12:42 → 21:13:07.

## Verification performed

### The Gate (20:53, [`02-gate.txt`](../../../.documents/ray-sqla-align/out/02-gate.txt))

`HEAD` `b16c4355ff91adbf8d27a08e5956e0020fc0a716`; `git status --porcelain --untracked-files=all`
empty; the main checkout alone; `git diff 668d30f HEAD -- '*.py' '*.sh' requirements.txt
runregistrykit.toml` empty; `main.py` `685b615b…7bbe`; `pip freeze` 102 lines, `cmp` 0 against 01's
`freeze-after.txt`, `ray==2.59.0`, `SQLAlchemy==2.0.55`; `pip check` clean; `ray_cluster.sh status`
exit 0, lock free, no process, ports free; `runregistrykit list`: `no runs under …/var/runs`;
`var/datastores/` held `rrk-adoption` 04's two sidecars only, and `var/ray-sqla-align-02/` did not
exist (`test -e` 1, at 20:53:25, before I made it); the token file `-rw------- 1 ds283 staff 64 Oct
25 12:51`, `1792932677 Oct 25 12:51:17 2026`; `env | grep -c RAY_AUTH_MODE` 0. Load 2.44.

### The suites ([`02-suites.txt`](../../../.documents/ray-sqla-align/out/02-suites.txt))

| Root | Before `start` | After `stop` |
|---|---|---|
| CosmologyModels | 18 OK, 79.2 s; 3.54 | 18 OK, 82.6 s; 37.04 |
| ComputeTargets `[a-f]` | 61 OK, 115.1 s; 2.73 | 61 OK, 114.6 s; 11.33 |
| ComputeTargets `[g-z]` | 115 OK, 121.9 s; 3.17 | 115 OK, 130.6 s; 4.19 |
| ObjectFactories | 78 OK, 8.8 s; 2.66 | 78 OK, 9.7 s; 15.19 |
| tools | 102 OK, 40.4 s; 2.78 | 102 OK, 38.9 s; 13.27 |
| RunDrivers | 61 OK, 26.8 s; 11.89 | 61 OK, 28.0 s; 8.00 |

Each cell: tests, verdict, `unittest`'s time; the one-minute load before it. 01's counts exactly.

### The cluster ([`02-cluster.txt`](../../../.documents/ray-sqla-align/out/02-cluster.txt))

- `start`, 21:01:04, load 8.07: **exit 0**, `started in 8s; gcs_server pid 10995`; **0 warnings**
  (`grep -c` 0 for each of the four token strings, and `grep -c -i warn` 0). Its `actors:` line:
  `>> ray_cluster: actors: RAY_AUTH_MODE=token ./venv/bin/ray list actors --address 127.0.0.1:6380`.
- `status`, exit 0: lock held (`ray-sqla-align 02 (R)`), owner 10995 alive, 19 processes, ports
  held; `raylet` 11031.
- `ps -E`: `gcs_server` 10995 `RAY_AUTH_MODE=token`, `PATH=` control 1; `raylet` 11031
  `RAY_AUTH_MODE=token`, control 1.
- `ray status`, exit 0: `0.0/10.0 CPU`, `0.0/1.0 GPU`, `0B/7.03GiB memory`, `0B/2.00GiB
  object_store_memory`; one active node, no failures.
- `stop`, exit 0: `SIGTERM to 19 process(es)`, `grace period of 15s over; SIGKILL to: 10995 11000`,
  no process left, the four ports free, `released the rehearsal lock`, `stopped in 20s`. `status`,
  exit 0: `lock: free (/Users/ds283/.raypoolkit/rehearsal-lock does not exist)`, no process, nothing
  to signal, the ports free. The lock module's `status`, exit 0: `lock: free`.

### The builds, the signal, the resume, the stores and the plot

As the verification document §1.2–§1.6 records, quoted from the outputs:
- **The five `compute_time`s** of `reference`: β = 1.15 6.65 s, 1.1 6.75 s, 1.2 7.66 s, 10 42.45 s,
  25 107.07 s ([`02-compute-times.txt`](../../../.documents/ray-sqla-align/out/02-compute-times.txt));
  `<s>` = 90 s (deviation 6).
- **What `resumed-a` stored**: four rows; β = 1.15 (6.50 s), 1.1 (6.64 s), 1.2 (7.45 s) `validated
  1`; β = 10 (43.70 s) `validated 0`.
- **The signal**:
  ```text
  [2026-10-25T21:07:06+00:00] interrupt_when: saw "CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID"; waiting 90 s
  [2026-10-25T21:08:36+00:00] interrupt_when: sent SIGTERM to pid 12370
  ```
  and the log after the banner as deviation 7; the intervals 0 s and 8 s.
- **`stage_counts.py`** on the three logs: `reference` 5 / 5 / 5 / 5 in each stage, exit 0;
  `resumed-a` scalar `INCOMPLETE; no queue summary`, exit 143; `resumed-c` scalar 5 / 2 / 2 / 2,
  adiabatic and BBN 5 / 5 / 5 / 5, exit 0.
- **`compare_stores`**: `RESULT: equal`, `problems: none`; `ScalarModel` 5, `ScalarModelValue`
  42617, `AdiabaticHistory` 5, `AdiabaticHistoryValue` 42617, `BBNData` 5, `BBNDataValue` 7475,
  `redshift` 21250, every table 0 in either only.
- **`store fingerprint`**: reference `2dd4b39b61b9cc1b39bfcf5068ca2d00e84b52fee9da61958a11781931b0932f`,
  "matches the fingerprint taken 2026-10-25T21:05:42+00:00 by a person"; resumed the same digest,
  "matches the fingerprint taken 2026-10-25T21:12:19+00:00 by run
  ray-sqla-align-02-resumed-c-20261025T210916".
- **`histories.csv`**: `d7c62c37d8ad637a63854583ab11bc737109dcbfba92168dbddbaea75182c522`.
- **The reference store's `shasum`s** before and after the plot: equal, `diff` exit 0 (17 files and
  the sidecar; e.g. `ray-sqla-align-02-reference.db`
  `7bedaca3e0a4026c224d299d63dcd39e2a5560aa57b5aaff0e02ba50895c5c0f` both times).
- **`list`** at the end: the three runs, `3 finished`: `resumed-c` `done`, `resumed-a` `killed`,
  `reference` `done`.

### Token ([`02-token.txt`](../../../.documents/ray-sqla-align/out/02-token.txt))

- `grep -c` per file, in the order `Token authentication is enabled`, `Generated new
  authentication token`, `InvalidAuthToken`, `AuthenticationError`: `start.txt` 0 0 0 0;
  `reference.log` 0 0 0 0; `resumed-a.log` 0 0 0 0; `resumed-c.log` 0 0 0 0; `plot-reference.log`
  0 0 0 0.
- The token file at 21:15 and again at 21:22: `-rw------- 1 ds283 staff 64 Oct 25 12:51
  /Users/ds283/.ray/auth_token`; `/usr/bin/stat -f '%m %Sm'`: `1792932677 Oct 25 12:51:17 2026`.
  Unchanged since the Gate and 01.
- The final `grep -r -F -l -f ~/.ray/auth_token .documents/ray-sqla-align/
  .documents/ray-sqla-align-verification.md prompts/ray-sqla-align/`: no output, **exit 1**.

### The venv and the tree

`pip freeze` after the suites `cmp`-equal to the one at the Gate (exit 0, 102 lines). `git status
--porcelain --untracked-files=all` empty after the suites; `HEAD` still `b16c435`; `git diff 668d30f
-- main.py` empty.

### The gates

- **FP ✅**: both stores `2dd4b39b…932f`; `compare_stores` equal but for bookkeeping;
  `histories.csv` `d7c62c37…c522`.
- **Token, 02's part ✅**: no token warning or authentication error in `start`'s output or any
  launch or reader log; the `gcs_server` and `raylet` in token mode; every driver attached; the
  token file unchanged at 64 bytes, mode 600, 12:51:17; no record holds it.

## What is new in the logs

[`02-log-kinds.txt`](../../../.documents/ray-sqla-align/out/02-log-kinds.txt). The kinds in one log
and not the other, 02's against 04's at the same slug:

| Pair | In 02, not 04 | In 04, not 02 |
|---|---|---|
| `reference` | 2: the `>> packages:` line (1x); `   -- ALL WORK ITEMS COMPLETE in time 23s` (1x) | 3: the old `>> packages:` line; Ray's `FutureWarning: Tip: In future versions of Ray, Ray will no longer override accelerator visible devices env var …` (1x) and its `  warnings.warn(` (1x) |
| `resumed-a` | 1: the `>> packages:` line | 3: the old `>> packages:` line; the `FutureWarning` and its `warnings.warn(` |
| `resumed-c` | 7: the `>> packages:` line; the six lines of the prune report, from `(Datastore pid=13160) !! INTEGRITY WARNING (2026-10-25T21:09:21): datastore "shard0002-store" …` to `(Datastore pid=13160)      ** Note: these values have been pruned from the datastore.` (1x each) | 3: the old `>> packages:` line; the `FutureWarning` and its `warnings.warn(` |
| plot | 2: the `>> packages:` line; `      Queue summary: 1 lookup (avg 12.53/min)` | 4: the old `>> packages:` line; the `FutureWarning` and its `warnings.warn(`; `      Queue summary: 1 lookup (avg 482.5/hr)` |

Explained:
- **The `>> packages:` line** names `raypoolkit` 0.3.0 at `67a75b97…` (04's had 0.2.0 at
  `eaef0ce6…`: `rpk-v030-adoption` moved that pin after 04) and the four new entries `ray 2.59.0`,
  `SQLAlchemy 2.0.55`, `numpy 2.3.5` and `scipy 1.17.0`, each `(no recorded origin)`: 01's D1.
  Expected.
- **Ray's accelerator `FutureWarning` is gone** from every log (two lines each in 04's: the warning
  from `ray/_private/worker.py:2046` and its `warnings.warn(`). SI found it gone under 2.59.0.
  Expected.
- **The prune report** in `resumed-c`: expected (the note's ruling 9). In 04 it was in `resumed-b`'s
  log, whose open came first, so 04's `resumed-c` had none.
- **`in time 23s`**: RayPoolKit's `format_time` prints the seconds with `:.3g`, so a BBN stage of
  about 23.0 s prints `23s` where 04's 32.9 s printed `32.9s`. The same line, not a new kind.
- **`avg 12.53/min`** against **`avg 482.5/hr`**: the plot's one item went quicker (4.79 s against
  7.46 s), and the queue summary chose minutes for the rate. The same line, not a new kind.
- Not a kind, since the comparison replaces digits: Ray's driver lines now come from `worker.py:1836`
  and `:2024`, 04's from `:1821` and `:1998`, with the same text.

No new `DeprecationWarning`, `FutureWarning` or Ray line, and none bears on correctness. No §3
issue opened.

## The deliberate-breakage record

None: this prompt changes no code and no test (the prompt's §5).

## Observations not acted on

- **The stop needed `SIGKILL`** after the grace for the `gcs_server` (10995) and for 11000, a
  `Python -u` of the venv's Ray started with the head, the same pair as at 01's V1 (99355, 99362).
  `stop` confirmed no process and the ports free, released the lock and exited 0; `status` said so.
  Not identified further, which would need a command naming the cluster's temp dir.
- **`ray status` reports `0.0/1.0 GPU`** under 2.59.0, as 01 and SI recorded. The fingerprints show
  the builds did not depend on it.
- **`resumed-a`'s partial store** fingerprints to `0d612337…0f13`, the digest 04's `resumed-a`
  recorded under Ray 2.53.0 at a 150 s delay: the same four rows, β = 10 stored and unvalidated, in
  both.
- **β = 10 stored and left unvalidated for 33 s** in `resumed-a` before the signal (04's: 65 s), the
  queue behaviour `rrk-adoption-verification.md` §1.11 describes. Not this campaign's.
- **`ray_state.py`'s empty listings** print Ray's `No resource in the cluster` text, since `ray
  list … --format json` gives no JSON when there is nothing: the helper's fallback. Read as 0.
- **The load after the stop** reached 37.04 (21:15) with no ChamPBH process alive: another
  workload's. The suites passed at it.

## State handed on

- Branch `ray-sqla-align` at `e04fbb8`, on top of `b16c435`. **Not pushed.** The orchestrator's
  review of 02 closes the campaign; then `ray-sqla-align` is merged into `main` and `main` into
  `jordan-normalization` before that campaign's 09 (README §4.2), as the user directs.
- **The stores** `var/datastores/ray-sqla-align-02-reference.db` (`store_id`
  `16b5fa1051ce4a7090c2b6b370c6baf7`) and `var/datastores/ray-sqla-align-02-resumed.db`
  (`9919583b8b04499da3e286c0814a0114`), 16 shards each, with their sidecars, are kept for the user to
  retire with `store retire` (README §3). The three run directories stay under `var/runs/`;
  `runregistrykit list` shows them `finished` (`done`, `killed`, `done`). `var/ray-sqla-align-02/`
  holds the launch logs, pid files, the plot's outputs, the working outputs and the helpers. Nothing
  in `var/` was deleted; `rrk-adoption` 04's two sidecars untouched.
- ChamPBH's cluster is stopped and the lock free (`status` exit 0 after `stop`). The token file is
  unchanged.
- The venv holds `ray` 2.59.0 and `SQLAlchemy` 2.0.55, its freeze unchanged by this prompt.
- The roots: CosmologyModels 18, ComputeTargets 61 + 115, ObjectFactories 78, tools 102,
  RunDrivers 61.
- No issue opened; `.documents/OPEN_ISSUES.md` unchanged.
