# Log 04 — rehearse registered runs

**Subject:** Rehearse registered, interrupted and hard-killed builds
**Commit:** this commit, on branch `rrk-adoption`, on top of `07aa626`
**Date:** 2026-10-19 (the rehearsal, 23:34–00:07 BST) and 2026-10-20 (the commit), by Claude Opus 5.5
**Result:** **COMPLETE.** H1–H6 shipped; the gate "R" holds. Four builds through the wrapper on
ChamPBH's isolated cluster: `reference` `done`/0; `resumed-a` `killed`/143; `resumed-b` `SIGKILL`ed,
`stale`, a resume refused, then `lost` by `end`; `resumed-c` `done`/0. `compare_stores` of the two
stores: `RESULT: equal`. Both fingerprints `2dd4b39b…932f`. `histories.csv` R0's
(`d7c62c37…c522`). The suites 18, 61 + 115, 78, 84 and 61, OK before and after. No §3 stop condition
was met. No code changed; no issue opened.

The orchestrator's note for 04 (§0, twelve rulings) governed where it differs from the prompt; each
ruling is classified below as it says. The record is
[`.documents/rrk-adoption-verification.md`](../../../.documents/rrk-adoption-verification.md) §1;
the figures below are its.

## What shipped

- **H1 — `reference`**, `rrk-adoption-04-reference-20261019T234113`, pid 40540, into
  `var/datastores/rrk-adoption-04-reference.db`. 23:41:11 → 23:46:51 (5 min 40 s), the stages
  3m 46.5s · 9.48s · 32.9s, 5 / 5 / 5 / 5 in each. Log last lines `** driver exit status 0`,
  `** launcher saw exit status 0`. `list`: `done`. `status.json`: `done`/0, the fingerprint
  `2dd4b39b61b9cc1b39bfcf5068ca2d00e84b52fee9da61958a11781931b0932f`, `fingerprint_sidecar` "not
  written … (there is none)". Manifest: `script` `main.py`, `script_sha256` `243a5026…7d59` =
  `shasum -a 256 main.py`, `results` `var/datastores/rrk-adoption-04-reference.db`,
  `results_store_id` `null`, the three packages not editable, `git_head` `07aa626`, `git_dirty`
  `true`, `scope` and `heartbeat_means` as 03 (and `5d02258`) wrote them. Then `store create`
  (`store_id` `8b8f0e389f854df4897876184dbbb307`), `store fingerprint --write` (the same digest as
  the status's) and `store show`.
- **H2 — `resumed-a`**, `rrk-adoption-04-resumed-a-20261019T234840`, pid 43850, into
  `var/datastores/rrk-adoption-04-resumed.db`, `SIGTERM`ed by `interrupt_when.py --after 150`:
  23:48:38 → 23:53:08 (4 min 30 s). The handler's `** SIGTERM at 2026-10-19 23:52:58 +0100: recorded
  killed, exit code 143; …`, then `** SIGTERM: main.py ended with SystemExit(1); recording killed,
  exit code 143`, `** driver exit status 143`, `** launcher saw exit status 143`. `killed`/143 with
  the fingerprint `0d612337…0f13`. No `** cancelled an alarm` line. Signal → `killed` record 0 s
  (±1 s); signal → final record 9 s (±1 s). Then `store create` (`store_id`
  `1e9b249c0f2e4af5b0b529b0b403aaa6`).
- **H3 — `resumed-b`**, `rrk-adoption-04-resumed-b-20261019T235447`, pid 46306, on the resumed
  store with `--prune-unvalidated`, `SIGKILL`ed 20 s after the scalar banner (23:55:36). The log ends
  `** launcher saw exit status 137`, with no driver marker. `list`: `!!`, `running`, `1 stale`, with
  the stderr summary naming `end`. Nothing left in Ray (the 18 baseline pids, 0 alive workers, 0
  `ALIVE` actors). The refused `resumed-c`: exit 2, one `!! ` line naming `resumed-b` and `python -m
  runregistrykit end`, the marker `** driver exit status 2`, `var/runs` unchanged by `diff`. `end`:
  accepted, "now lost, verified: its pid 46306 does not answer kill -0 on this host"; `list` `lost`.
- **H4 — `resumed-c`**, `rrk-adoption-04-resumed-c-20261019T235647`, pid 47488, on the resumed
  store with `--prune-unvalidated`: 23:56:46 → 23:59:51 (3 min 5 s), `done`/0, every stage's
  `ALL WORK ITEMS COMPLETE`; scalar 5 / 2 / 2 / 2 (only the two missing), adiabatic and BBN 5 / 5 /
  5 / 5. `fingerprint_sidecar` `written`; status, sidecar and `store fingerprint` all `2dd4b39b…932f`.
- **H5 — the stores and the readers.** `compare_stores`: `RESULT: equal`, exit 0, bookkeeping
  columns aside (timestamps, labels, compute times). `store fingerprint` on each: one digest,
  `2dd4b39b…932f`; the sidecars' per-class detail equal. `plot_by_beta.py` (unregistered) on the
  reference: exit 0, read-only, `histories.csv` SHA-256
  `d7c62c37d8ad637a63854583ab11bc737109dcbfba92168dbddbaea75182c522`, R0's. `store show`: the
  reference named by `reference` (`done`); the resumed by `resumed-c` (`done`), `resumed-b` (`lost`)
  and `resumed-a` (`killed`).
- **H6 — the record.** `.documents/rrk-adoption-verification.md` (new), §1; the outputs under
  `.documents/rrk-adoption/out/04-*` (30 files, 208 KB, the largest 36.5 KB): the four launch logs
  and the refused one, the two signallers' logs and the plot's; each run's `manifest.json` and
  `status.json` whole; the checks, `list`, `end`, `store` and `stage_counts.py` outputs;
  `compare_stores`; the cluster's record (`start`, ruling 9's four points and one before the stop,
  `ray status`, the jobs, `stop`, `status` after); the probe; the `compute_time`s; the refusal; the
  suites; and the scratch scripts as text (`04-wait_for_line.py.txt`, `04-kill_when.py.txt` and
  `04-kill_when.diff`, `04-compute_times.py.txt`, `04-ray_state.py.txt`).
- **The board**: row 04, H1–H6, the gate "R", and §5.5.

## Deviations from the prompt

1. **`STRUCTURALLY REQUIRED`: `resumed-b` was killed in the scalar stage, not the adiabatic one**
   (the orchestrator's note for 04, §0 ruling 1). README §2.2 item 3 says "during the adiabatic
   stage"; the prompt's step 5 says the scalar stage, and the adiabatic queue (9.48 s here) is too
   short to aim at. The prompt governs.
2. **`IMPLEMENTATION CHOICE`: the `SIGKILL` was sent by `kill_when.py`** (ruling 2), a copy of
   `interrupt_when.py` in the scratch directory with `SIGTERM` changed to `SIGKILL` and nothing
   else. The diff, as `diff interrupt_when.py kill_when.py` gives it
   (`04-kill_when.diff`):
   ```diff
   17c17
   < Send SIGTERM to a rehearsal driver once its log shows a stage banner and a stated delay has
   ---
   > Send SIGKILL to a rehearsal driver once its log shows a stage banner and a stated delay has
   25c25
   < line containing TEXT, then SECONDS more, then sends SIGTERM to the pid in PIDFILE, and prints
   ---
   > line containing TEXT, then SECONDS more, then sends SIGKILL to the pid in PIDFILE, and prints
   76c76
   <         description="Send SIGTERM to the pid in PIDFILE once LOG holds a line containing "
   ---
   >         description="Send SIGKILL to the pid in PIDFILE once LOG holds a line containing "
   100c100
   <         f'waiting for "{args.banner}" in {args.log}; then {args.after:g} s; then SIGTERM'
   ---
   >         f'waiting for "{args.banner}" in {args.log}; then {args.after:g} s; then SIGKILL'
   139,140c139,140
   <     os.kill(pid, signal.SIGTERM)
   <     _say(f"sent SIGTERM to pid {pid}")
   ---
   >     os.kill(pid, signal.SIGKILL)
   >     _say(f"sent SIGKILL to pid {pid}")
   ```
   Its lines still say `interrupt_when:`, the prefix being unchanged. The rehearsal directory was
   not touched; the copy is committed only as text.
3. **`IMPLEMENTATION CHOICE`: every wait was `wait_for_line.py`, not `Monitor`** (ruling 3). The
   `Monitor` tool could be loaded, but it streams events as notifications while the agent goes on
   working, and a subagent cannot end its turn to be woken by one; so the ruling's other branch was
   taken. `wait_for_line.py LOG TEXT --timeout SECONDS` reads the file only. Each wait was one
   foreground call: `--timeout 120` for the registration lines and the refusal, `--timeout 540`
   with the Bash tool at 600 000 ms for each build's end. **No wait timed out**: the longest was
   317.8 s (`reference`). No `sleep` loop, `pgrep -f` or `tail -f` was used. The signallers
   (`interrupt_when.py`, `kill_when.py`) ran detached with `nohup`, and had exited before the stop.
4. **`IMPLEMENTATION CHOICE`: the one check after each launch** (ruling 4): after the registration
   line, one Bash call printing the manifest's path and `ls -l`, `status.json`'s state, pid,
   heartbeat and stage, the pid file, the manifest's `created`, the log's birth time, `list` and the
   log's first four lines. The order of the log shows each manifest written before `main.py`'s first
   line (lines 1–3 are the wrapper's, line 4 `>> packages:`), and each log is born before its
   manifest (the table in the document §1.3).
   **`UNINTENDED DRIFT`: in `reference`'s check, the birth-time line failed**: the `stat` first on
   `PATH` is not macOS's, and `stat -f '%SB'` printed an error. `/usr/bin/stat` was run straight
   after, in a second call (log born 23:41:11, manifest born 23:41:13); a birth time does not change,
   so the figure is the one the check would have given. The other three checks used `/usr/bin/stat`.
5. **`IMPLEMENTATION CHOICE`: the launch lines** (ruling 5): each launch was one Bash call that
   exported `ADDRESS=127.0.0.1:6380`, `S=$PWD/var/datastores` and `L=$PWD/var/rrk-adoption-04`
   (absolute) and ran the `nohup sh -c '…'` line with the slug spelt out; recorded verbatim in the
   document §1.12. The manifests' `results` are relative to the client root, `argv` holds the
   absolute paths. `resumed-a`, into an empty store, was given no `--prune-unvalidated`, as step 4
   gives none; `resumed-b` and both `resumed-c` launches had it. No `SIGINT` was sent.
6. **`IMPLEMENTATION CHOICE`: the two delays** (ruling 6), chosen from the five `compute_time`s read
   through `sqlite3` `mode=ro` after H1's `store create` and `store fingerprint --write`: β = 1.15
   7.28 s, 1.1 7.33 s, 1.2 8.12 s, 10 50.91 s, 25 195.74 s, stored at about +17, +18, +19, +75 and
   +226 s after the banner.
   - `<s>` = 150 s, near the middle of the widest gap (β = 10 to β = 25), expecting β = 1.1, 1.15,
     1.2 and 10 stored. **The outcome differed in one row**: four rows were stored, but β = 10's
     (stored 23:51:53) was still `validated 0` at the signal (23:52:58), and `resumed-b`'s open
     pruned it with an `INTEGRITY WARNING`. Recorded, not a stop (ruling 6); see "Observations".
   - The delay for `resumed-b` = 20 s: the missing histories at its open were β = 10 and 25, and
     half the shorter one's `compute_time` is 25.5 s (floor 10 s). After the kill the store held the
     same three validated rows: `resumed-b` stored nothing.
7. **`IMPLEMENTATION CHOICE`: the signal's timing** (ruling 7), by the handler's line (local time,
   brought to UTC) against `interrupt_when.py`'s `sent SIGTERM` (UTC): both 22:52:58 UTC, 0 s ±1 s;
   and by `status.json`'s final heartbeat, 23:53:07 BST, 9 s ±1 s. No instrumentation was added.
   "Within about a second" holds to the clocks' resolution; nothing finer can be told.
8. **`IMPLEMENTATION CHOICE`: the P5 probe** (ruling 8), after the baseline and before H1, exactly as
   ruled: `before 0` (`SIG_DFL`), `after <function init.<locals>.sigterm_handler …>
   ray._private.worker init.<locals>.sigterm_handler`, exit 0, job `01000000`.
9. **`IMPLEMENTATION CHOICE`: what a kill leaves behind** (ruling 9), at the four points and once more
   before the stop: `ray_cluster.sh status`, and `ray list workers` / `ray list actors` with
   `--filter "is_alive=True"` / `--filter "state=ALIVE"` at the baseline, as ruled. At the later points
   a small reader, `ray_state.py` (the scratch directory; committed as text), ran `ray list jobs`,
   `workers` and `actors` with `--format json --limit 1000` and summarised them: the
   alive-filtered tables print every column and are too wide to read. In Ray 2.53, `ray list workers`
   gives no job id for a worker, so the test for a left-behind worker is that none is alive; the
   actors carry their job id. At every point after a job: 0 alive workers, 0 `ALIVE` actors, the 18
   baseline pids (`diff` against the baseline's list empty). Nothing was left, so there was nothing
   to wait for by pid.
10. **`IMPLEMENTATION CHOICE`: the refused launch** (ruling 10) wrote
    `$L/resumed-c-refused.log` and `.pid`, with step 6's flags and arguments otherwise unchanged; its
    four results are in "What shipped".
11. **`STRUCTURALLY REQUIRED`: every manifest's `results_store_id`** (ruling 11): `reference` and
    `resumed-a` `null`; `resumed-b` and `resumed-c` `1e9b249c0f2e4af5b0b529b0b403aaa6`, the resumed
    store's `store_id` as `store create` printed it. As expected.
12. **`IMPLEMENTATION CHOICE`: the fingerprints** (ruling 12): the two stores' digests are equal and
    `compare_stores` says equal, so no stop arose. `resumed-c`'s status, the sidecar after it and
    `store fingerprint` on the closed store are one digest.
13. **`STRUCTURALLY REQUIRED`: the heartbeat's age is read from `status.json`, not from `list`.** The
    prompt's §1.2 asks for the heartbeat's "age and stage as `list` showed them". `list`'s `AGE` is
    the run's age since its `created` (`runregistrykit/__main__.py:652`), not the heartbeat's. The
    document gives both: the age the check computed from `status.json`'s `heartbeat`, and `list`'s
    line as it was.
14. **`IMPLEMENTATION CHOICE`: two scratch scripts beyond the note's two are committed as text**,
    `04-compute_times.py.txt` (the query of ruling 6, which the note names) and `04-ray_state.py.txt`
    (deviation 9), so that the commands in the document can be re-run. Both are under
    `.documents/rrk-adoption/out/04-*`, which the note lists; neither is code of the tree.
15. **`UNINTENDED DRIFT`: `stop`'s exit status was not echoed.** It was run as a command on its own,
    as the prompt asks, but without `; echo $?`. The Bash tool reports a non-zero exit (as it did for
    a later, unrelated `ls`) and reported none, and the script prints `stopped in` only on its
    `return 0` (`ray_cluster.sh:354-355`); so exit 0. Its output, not captured to a file, is
    transcribed verbatim from the call into `04-cluster.txt`. `status` after it exited 0 with the
    lock free, no ChamPBH process and the ports free.

## Verification performed

**The gate** (23:34:36): branch `rrk-adoption`, `HEAD` `07aa626`, no tracked change, 22 untracked
entries, no `var/`; `ray_cluster.sh status` exit 0, the lock free, no ChamPBH process, the four ports
free; `list`: "no runs under …/var/runs", exit 0; neither store nor `var/rrk-adoption-04/` existed;
load 6.08 (under the note's 20).

**The suites**, serially, in the foreground, with no cluster up; output to the scratch directory and
the verdict grepped:

| Suite | Before `start` | After `stop` |
|---|---|---|
| CosmologyModels | 18 OK, 68 s, load 6.4 | 18 OK, 78 s, load 16.7 |
| ComputeTargets `test_[a-f]*` | 61 OK, 99 s, load 5.2 | 61 OK, 99 s, load 5.8 |
| ComputeTargets `test_[g-z]*` | 115 OK, 107 s, load 4.8 | 115 OK, 102 s, load 3.8 |
| ObjectFactories | 78 OK, 10 s, load 5.6 | 78 OK, 8 s, load 7.5 |
| tools | 84 OK, 15 s, load 5.5 | 84 OK, 15 s, load 7.9 |
| RunDrivers | 61 OK, 20 s, load 5.2 | 61 OK, 20 s, load 6.9 |

The counts 03 handed on, exactly; none fell.

**The cluster**: `start` at 23:40:32, exit 0, 5 s, `gcs_server` pid 40013; `stop` at 00:01, exit 0
(deviation 15), 19 s; `status` after, exit 0, all free. `ray stop` never run. Every driver that
started Ray connected to `127.0.0.1:6380`; no log names 6379.

**The builds**: the table in the document §1.3. Every check, `list`, `store` and `end` output is in
`.documents/rrk-adoption/out/04-*`, as the document cites.

**Acceptance (prompt §2):**
1. H1–H6: as above.
2. The cluster started and stopped by the script alone, under the lock; `start`'s output, `ray
   status` and `stop`'s exit status and timing recorded; nothing left running.
3. Nothing written outside `var/`, `.documents/` and the scratch directory, but the lock under
   `~/ScienceData/` (`start`'s and `stop`'s) and `/tmp/ray-cpbh/` (the cluster's). No store opened
   but the two. `pip freeze` before and after: `cmp` exit 0, `diff` empty, 102 lines.
4. `git diff 3cd2be4 -- main.py`: empty, before and after.
5. One commit; `git diff --cached --name-only` shown before it; `var/` not committed.

`git status --short` after the stop: the 22 untracked entries and nothing else, before this commit's
files were written.

## The deliberate-breakage record

None. This prompt changes no code, so there is nothing to break; the deliberate interruptions are
the rehearsal itself (`SIGTERM`, `SIGKILL`, the refusal).

## Observations not acted on

- **A stored history's validation can wait behind the longest one.** In `resumed-a`, β = 10's row was
  stored at 23:51:53 and was still unvalidated 65 s later, when the signal came; the resume then
  pruned it and recomputed it (63 s of work under that load). Log 03 §2.3 describes `main.py`'s
  queues waiting in `ray.wait` for `min(3, in flight)` returns, which with two items in flight (β =
  10's validation and β = 25's compute) would hold the validation until β = 25 finished. That
  explanation fits what was seen but is not measured here. It is RayPoolKit's and `main.py`'s queue
  settings', not the wrapper's, and the prompt does not ask for it. It matters for
  `jordan-normalization`'s longer histories: an interruption can cost completed work. Left for the
  orchestrator to decide whether it is an issue (and, if a RayPoolKit one, for that repository).
- **Ray records a `SIGKILL`ed driver's job as `SUCCEEDED`** (`04000000`). Only the registry's
  `stale` showed that the run had not ended.
- **`list`'s `AGE` is the run's age, not the heartbeat's** (deviation 13). A reader who wants the
  heartbeat's age reads `status.json`; the staleness test itself uses the heartbeat.
- **The scalar stage ran 3m 46.5s in `reference`**, against 2m 12s at the last rehearsal, with
  nothing printed between its banner and its completion line. That is longer than the 2¼ min log 03
  §2.3 expected as the pilot's longest healthy silence, and far inside the 900 s window. Under heavy
  load the pilot's silence grows with the β = 25 history.

## State handed to the next prompt

- Branch `rrk-adoption`; this commit is on top of `07aa626`. No tracked change; the 22 untracked
  entries at the root are the user's. `var/` (ignored, 95 MB) holds:
  - `var/datastores/rrk-adoption-04-reference.db` and `…-resumed.db`, 16 shards each, with registry
    sidecars (`store_id` `8b8f0e389f854df4897876184dbbb307` and
    `1e9b249c0f2e4af5b0b529b0b403aaa6`), each with the fingerprint
    `2dd4b39b61b9cc1b39bfcf5068ca2d00e84b52fee9da61958a11781931b0932f`, kept for the user (README §3,
    §7);
  - `var/runs/`: four runs, all finished — `rrk-adoption-04-reference-20261019T234113` `done`,
    `rrk-adoption-04-resumed-a-20261019T234840` `killed`, `rrk-adoption-04-resumed-b-20261019T235447`
    `lost`, `rrk-adoption-04-resumed-c-20261019T235647` `done`. `list` shows none running or stale;
  - `var/rrk-adoption-04/`: the launch logs, pid files, the signallers' logs and the plot's output.
- The cluster is stopped and the rehearsal lock free (`ray_cluster.sh status` exit 0).
- **For 05**, quoting the verification document §1:
  - P5 measured: `ray.init` attached to an external cluster installs Ray's Python `SIGTERM` handler,
    and the wrapper's replaces it; `killed` is recorded in the signal's second, the final record
    with the fingerprint 9 s later; no alarm;
  - the hard-kill route works as the handover will tell it: after a `SIGKILL` the run is `stale`, a
    resume on its store is refused naming `end`, `end RUN_ID --reason …` records it `lost`, and the
    resume with `--prune-unvalidated` then runs to `done`; the launch through `sh` reaps the driver,
    so `end` is not refused;
  - `store create --purpose …` after the first close; from then on each run's fingerprint is
    written to the sidecar;
  - the fingerprint at finish takes about 7–8 s on the pilot's store under load;
  - `list`'s `AGE` is the run's age; the heartbeat's is in `status.json`.
- Suites: CosmologyModels 18, ComputeTargets 176 (61 + 115), ObjectFactories 78, tools 84,
  RunDrivers 61. `CLAUDE.md`'s ObjectFactories `# 69` and tools `# 79` remain 05's.
