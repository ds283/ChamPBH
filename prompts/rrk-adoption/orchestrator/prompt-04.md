# Orchestrator — prompt 04, rehearse registered runs

Read [`../README.md`](../README.md) first: §0.2 (what SecondaryGWKit's wrapper does), §2.2 (04's
charter), §3, §5 (rules 15 and 16, the one-off exception, "No agent polls a run") and §6 (U5,
P4–P6). The conventions are note 01's ([`prompt-01.md`](prompt-01.md) §0, "Conventions"),
restated here where 04 needs them.

**You do not write code.** You may:
- run the suites, `compare_stores.py`, `stage_counts.py` and the registry's read-only commands
  (`list`, `store show`, `store fingerprint` without `--write`) on 04's own stores and runs;
- read the agent's logs, records and outputs under `var/rrk-adoption-04/`, `var/runs/` and
  `.documents/rrk-adoption/out/`;
- run a probe from the session scratchpad, never committing one, and never starting Ray;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`04-rehearse-registered-runs.md`](../04-rehearse-registered-runs.md)
**Board items:** H1–H6 · **Gate:** "R" · **Closes:** nothing · **Opens:** only what the work finds ·
**Model:** Opus, as the prompt recommends · **Network:** none.

**Gate:**
- 01 (`b0f3488`, reviewed `caeadc0`), 02 (`c9eea37`, reviewed `87caa99`) and 03 (`d5dd930`,
  reviewed `a61f684`) landed; the gates "Pin", "Fingerprint" and "Wrapper" hold (board §2).
  `5d02258` is 03's housekeeping: it changed `HEARTBEAT_MEANS` and the module docstring of
  `RunDrivers/registered_run.py` only. `HEAD` is the commit that adds this note, on top of it.
- The prompt's own gate (its header): no tracked change; the lock free, no ChamPBH process, the
  four ports free; `list` shows no run; no `var/` at all (so neither store nor
  `var/rrk-adoption-04/`).
- One prompt at a time in this checkout. 05 is not dispatched until 04 is reviewed.

## 0. What makes this prompt unusual

**It is the only prompt of the campaign that runs real builds, and it kills two of them on
purpose.** Nothing in it is code. What it produces is a record: that the registry's account of four
builds is what happened to them, under real Ray, attached to ChamPBH's isolated cluster. A mistake
here is not a wrong value but a wrong claim in the verification document, so every claim in it must
be a line the agent saw, quoted, with the command that produced it.

**Twelve statements in the prompt are wrong, loose or silent. Ruled here:**

1. **`resumed-b` is killed in the scalar stage, not the adiabatic one.** README §2.2 item 3 says
   "`SIGKILL`ed during the adiabatic stage". The prompt's step 5 says the scalar stage, "while work
   remains". The adiabatic queue took 3.59 s in the last pilot (`read-only-plots` verification
   §1.2), too short to aim a kill at. **Ruled:** the prompt governs. The log records the
   difference from the charter as `STRUCTURALLY REQUIRED`, citing this note.

2. **`interrupt_when.py` sends only `SIGTERM`, and runs unchanged.** Step 5's kill needs the same
   "banner, then a delay" with `SIGKILL`. **Ruled:** the agent copies `interrupt_when.py` into its
   scratch directory as `kill_when.py`, changes `SIGTERM` to `SIGKILL` in the signal sent and in its
   messages, and nothing else. It records the diff between the two in the log, and commits neither.
   The rehearsal directory is not touched. Classify as `IMPLEMENTATION CHOICE`.

3. **How to wait without polling.** The prompt asks for a `Monitor` on the launcher's line. The
   agent may not have `Monitor`. It cannot end its turn and be woken, either. **Ruled:**
   - **if `Monitor` is among its tools**, it uses it, as the prompt says;
   - **otherwise** it writes one waiter in its scratch directory, `wait_for_line.py LOG TEXT
     --timeout SECONDS`. The waiter reads `LOG` until a line contains `TEXT`, prints that line with
     the time, and exits 0. At the deadline it exits 1. It reads the file only: no `pgrep`, no
     `kill -0`, and no Ray. Each wait is **one foreground call**, with the Bash tool's timeout at
     600 000 ms and `--timeout 540`;
   - a wait that times out may be repeated **once**, and the log records it. A second time-out is a
     stop: a build of about 3½ min that has not ended in 18 min is not healthy;
   - no `sleep` loop, no `pgrep -f`, and no `tail -f`. The cluster's `stop` kills anything whose
     command line names `/tmp/ray-cpbh`, and none of these may.

   Classify as `IMPLEMENTATION CHOICE`.

4. **The one check after a launch must not race the registration.** `nohup … &` returns at once,
   and the wrapper registers only after its imports. **Ruled:** after each launch, wait (ruling 3,
   `--timeout 120`) for the log's `** registered_run: registered as` line. Then make the one check
   in a **single** Bash call:
   - the manifest's path from that line, and that the manifest exists;
   - `status.json`'s `state` and `pid`, against `$L/<slug>.pid`;
   - `list`'s line for the run, with the heartbeat's age and the stage.

   **The manifest is written before the driver's first line** (the prompt's §3) is shown by the
   order of the log: the `** registered_run:` lines come before `main.py`'s first line, `>>
   packages: …`. Also compare the manifest's `created` with the log file's birth time (`stat -f
   %SB`). Classify as `IMPLEMENTATION CHOICE`.

5. **The launch line, as `sh` will see it.** The prompt's command puts `$L` inside single quotes.
   `sh -c` expands it only if it is in the environment. The agent's shell keeps no state between
   Bash calls. **Ruled:**
   - each launch is one Bash call, which sets and **exports** `ADDRESS`, `S` and `L` and then runs
     the `nohup sh -c '…'` line, with the slug spelt out;
   - `S` and `L` are **absolute** (`$PWD/var/datastores`, `$PWD/var/rrk-adoption-04`). Then the
     wrapper and `list` agree on the store's path, whatever `sh`'s working directory is. The
     manifest still records `results` relative to the client root (note 03, ruling 4);
   - the log records the command exactly as run, the exports included.

   The `sh` that `nohup` starts runs the driver as an asynchronous list, so the driver starts with
   `SIGINT` ignored. This rehearsal sends no `SIGINT`, and the agent does not test it. Classify as
   `IMPLEMENTATION CHOICE`.

6. **Choosing the two delays.** "Choose `<s>` from H1's scalar-stage time" is not enough. The pilot's
   five histories are five Ray tasks on 10 CPUs and run **at the same time**. So the stage's time is
   the slowest history's, and what a delay stores depends on each history's own time. **Ruled:**
   - after H1 has finished, and after its `store create` and `store fingerprint --write`, the agent
     reads each `ScalarModel` row's `compute_time` from the reference store's shards. It reads them
     through `sqlite3` with a `file:…?mode=ro` URI only, as `compare_stores.py` does. Record the
     query and the five times, with β;
   - **`<s>` for H2** lies in the widest gap between consecutive completion times. Each history
     completes at about its `compute_time` after the banner, plus a few seconds for the store and
     the validation. Leave at least 10 s on each side where the gap allows. Say which histories the
     choice expects to be stored;
   - **the delay for H3** is chosen after H2, from the histories `resumed-a` stored
     (`stage_counts.py` on its log, and the prune report at `resumed-b`'s open). It is at most half
     of the shortest `compute_time` among the histories **still missing**, and at least 10 s, so
     that work remains;
   - an outcome other than the one expected (H2 stores none, or all) is **recorded, not a stop**:
     the resume is exercised either way. Say so in the document.

   Classify as `IMPLEMENTATION CHOICE`.

7. **The signal's timing, to the second.** `interrupt_when.py` prints UTC to the second
   (`interrupt_when.py:41-46`). The wrapper's handler line prints local time to the second, with its
   offset (`registered_run.py:389`, `:436`). The registry's timestamps are also to the second.
   **Ruled:**
   - the interval from the signal to the `killed` record is the handler's line against
     `interrupt_when.py`'s `sent SIGTERM` line, both brought to UTC, so to ±1 s;
   - the time from the signal to the **final** record (`finish_after_exec`'s, with the fingerprint) is
     `status.json`'s own finish time, if it has one, against the same line;
   - add no instrumentation. If the prompt's "within about a second" cannot be told from ±1 s,
     write that.

   Classify as `IMPLEMENTATION CHOICE`.

8. **P5 needs a positive measurement, not only an absent line.** No `** cancelled an alarm` line
   (step 4) shows that absl's alarm was not armed when the signal came. It does not show that
   `ray.init`, attached to an external cluster, installs Ray's own Python handler. That is the part
   of SecondaryGWKit's finding that P5 asks 04 to confirm. **Ruled:** after `start` and before H1,
   one probe from the agent's scratch directory, run from the repository root:

   ```bash
   ./venv/bin/python -c 'import signal, ray; a = signal.getsignal(signal.SIGTERM); ray.init(address="127.0.0.1:6380"); b = signal.getsignal(signal.SIGTERM); print("before", a); print("after", b, getattr(b, "__module__", None), getattr(b, "__qualname__", None)); ray.shutdown()'
   ```

   Record the output, and the job it adds to `ray list jobs`. It opens no store and runs no
   solve. The prompt's "four builds, one plotting run" is then five Ray jobs and this one. Classify
   as `IMPLEMENTATION CHOICE`.

9. **What a `SIGKILL` leaves behind: `ray_cluster.sh status` does not show workers.** `status`
   lists the processes whose command line names `/tmp/ray-cpbh`. A Ray worker renames itself with
   `setproctitle` (`ray::IDLE`, `ray::Datastore`, …; `ray/_private/worker.py:2528`, `:2785`), so
   its command line need not name it. `read-only-plots`' `03-cluster.txt` lists 18 processes after
   `start`, with no job yet, and none is recorded for any later point. **Ruled:** at each of four points, record three things:
   - `ray_cluster.sh status`;
   - `./venv/bin/ray list workers --address 127.0.0.1:6380 --filter "is_alive=True"` (or the filter Ray 2.53 accepts, as run), with each
     worker's job id and pid;
   - `./venv/bin/ray list actors --address 127.0.0.1:6380 --filter "state=ALIVE"`.

   The four points are: after `start` (the baseline), after H1 ends, after H2 ends, and straight
   after H3's kill. **A process left behind** is a live worker or actor of `resumed-b`'s job
   (its job id from `ray list jobs`), or a `/tmp/ray-cpbh` process that is not in the baseline. If
   there is one, wait for it **by pid**, with a waiter of the same form as ruling 3 that tests
   `kill -0` on that pid alone. The ten-minute limit is the prompt's §3. Classify as `IMPLEMENTATION
   CHOICE`.

10. **The refused `resumed-c` keeps its own launch log.** "Launch `resumed-c` exactly as step 6
    will" would write `$L/resumed-c.log` and `.pid`, which step 6 then overwrites. **Ruled:** the
    refused launch is step 6's command with the launch files `$L/resumed-c-refused.log` and
    `.pid`, and nothing else changed: the same wrapper flags, `--register resumed-c`, and the same
    `main.py` arguments. Wait for its `** launcher saw exit status` line (ruling 3). Then show
    four things:
    - the exit status 2;
    - one `!! ` line naming `resumed-b`'s run id and `python -m runregistrykit end`;
    - the marker `** driver exit status 2`;
    - `ls var/runs` the same before and after, by `diff`.

    Classify as `IMPLEMENTATION CHOICE`.

11. **What each manifest's `results_store_id` should be.** `begin` reads the store's sidecar when
    there is one (`runregistrykit/__init__.py:1088-1113`). So:
    - `reference` and `resumed-a` begin with no sidecar: `null`;
    - `resumed-b` and the real `resumed-c` begin after step 4's `store create`: the resumed store's
      `store_id`, as `store show` prints it.

    The prompt names only H1's. **Ruled:** the document's table gives all four, and any other value
    is a stop ("a record is wrong"). The reference store's sidecar is made after H1 has finished,
    so no run of this prompt carries the reference's id. Classify as `STRUCTURALLY REQUIRED`.

12. **The fingerprints of the two stores.** The fingerprint is taken over each class's
    `inventory_spec`. `compare_stores` sets bookkeeping columns aside (dk-and-rpk-migration README
    §4.2). The expectation is that the two digests are equal when `compare_stores` says `equal`,
    and the prompt says only "compare as `compare_stores` says". **Ruled:**
    - a digest that differs where `compare_stores` says equal is **not** a stop during the
      rehearsal. Finish the protocol, stop the cluster, and record the per-class detail of both
      fingerprints, with the classes whose counts or digests differ. Then **stop and ask before
      committing**: whether the difference is a finding about the fingerprinter is the user's
      ruling;
    - the fingerprint in `resumed-c`'s status, the sidecar's after it, and `store fingerprint` on
      the closed resumed store are one digest, or that is a stop (§3, "a fingerprint is not what
      §1.1 says").

    Classify as `IMPLEMENTATION CHOICE`.

**Paths and expectations, checked by the orchestrator on 2026-10-19 at `5d02258`.** Pass them on.
- **The tree.** Branch `rrk-adoption`; nothing staged, no tracked change; the untracked set is note
  01's 22 entries; no `var/`. The main checkout. `git_head` in every manifest will be the commit
  that adds this note, and `git_dirty` `true`, because of the 22 untracked entries (README §0.3, R6).
  `var/` is ignored and does not count.
- **Ray.** `ray_cluster.sh status` exits 0: the lock free, no ChamPBH process, the four ports free.
  Ray is 2.53.0. Load at the check: 13.2 / 11.2 / 8.2. Other Claude sessions share the machine. If the
  one-minute load at the gate is over 20, **stop and ask** before `start`, as dk-and-rpk-migration
  README §4.1 asks for heavy load.
- **The wrapper's lines** (`RunDrivers/registered_run.py`):
  - at registration, `** registered_run: registered as <id>`, then `**   manifest <path>`, then
    `**   list it with: …` (`:785-788`);
  - the handler's, on stderr: `** SIGTERM at <local time>: recorded killed, exit code 143; stopping
    main.py; the process will exit with status 143` (`:436-439`);
  - then possibly `** SIGTERM: main.py ended with SystemExit(143); recording killed, exit code 143`
    (`:516`);
  - `** cancelled an alarm with N s left: Ray's own signal handler ran` (`:496`), which should
    **not** appear;
  - the marker `** driver exit status N` (`:119`), printed last, on a line of its own;
  - no marker after a `SIGKILL`.
- **Nothing creates `var/datastores/` but the build.** `ShardedPool` makes its primary's parent
  directory (`datastorekit/SQL/ShardedPool.py:251`), and `begin` makes the run directory
  (`runregistrykit/__init__.py:1129`). `mkdir -p $L` is the only `mkdir` the agent needs.
- **The registry's commands** at `v0.2.0`:
  - `store create --purpose TEXT PRIMARY`;
  - `store fingerprint [--write] PRIMARY`, which refuses a store an unfinished run names;
  - `store show PRIMARY`;
  - `end RUN_ID --reason TEXT`, which refuses a run whose pid answers here;
  - `list [--stale-after SECONDS]`, default 900.

  All take the client config from the repository root.
- **`histories.csv`** is written to `<--output>/QCD_Cosmology/histories.csv`
  (`read-only-plots-verification.md` §1.5). Here that is
  `$L/plot-reference/QCD_Cosmology/histories.csv`. R0's SHA-256 is `d7c62c37…c522`.
- **The last pilot** (`read-only-plots` verification §1.2, 2026-10-13): 3 min 41 s, the stages
  2m 12s · 3.59 s · 24.1 s, 5 / 5 / 5 / 5 in each stage, peak load 100 at the end. Log 03's §2.3:
  no progress line in any pilot stage; the longest healthy silence is the scalar stage, about 2¼
  min. So a healthy pilot never shows stale.
- **What a resume may differ in:** `compare_stores` reports bookkeeping columns (serial, timestamp,
  any column carrying a job name, a time or a host) and never compares them (dk-and-rpk-migration
  README §4.2). R0–R2 and `read-only-plots`' rehearsal were bitwise equal by it. A BBN solve that met
  `--bbn-wall-clock-limit` under load in one build and not the other is §4.3's named cause of a
  difference, and a stop here.

**What moves on purpose:** the prompt's §2 item 5, and nothing else:
- `.documents/rrk-adoption-verification.md` (new), §1 only (05 adds §2);
- `.documents/rrk-adoption/out/04-*` (new): the four launch logs and the refused one; each run's
  `manifest.json` and `status.json` whole; the `list`, `store show`, `store fingerprint`,
  `stage_counts.py` and `compare_stores.py` outputs; the cluster's record (`start`, `status` at
  ruling 9's points, `ray status`, `ray list jobs`, `stop`); the probe of ruling 8; the
  `compute_time` query of ruling 6; and the two copies `kill_when.py` and `wait_for_line.py`, as
  text, so that the commands can be re-run. Not the run directories' `stdout.log` and `stderr.log`,
  which repeat the launch logs. A file over about 1 MB is reported before committing;
- `logs/04-rehearse-registered-runs.md` and the board (row 04, H1–H6, the gate "R", `### 5.5 After
  prompt 04 (<date>)`). The index is untouched unless an issue opens.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-only`
  before committing. Count, then paste. The document states only what a quoted line shows.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its
  follow-up.
- **Suites.** `CLAUDE.md`'s commands, serially, in the foreground, ComputeTargets in its two halves,
  output to a file in the agent's scratch directory, the verdict grepped from it, `uptime` beside
  each. A failure is re-run alone before it is called real. Never with the cluster up.
- **Scratch.** The agent uses its own subdirectory of the session scratchpad, never `/tmp`. Its
  scripts live there. Run outputs go to `$L`, and copies of small ones to
  `.documents/rrk-adoption/out/`.
- **Ray.** Only `ray_cluster.sh` starts and stops the cluster. Every driver and the probe get
  `127.0.0.1:6380`, and every `ray` CLI call `--address 127.0.0.1:6380`. **No `ray stop`, ever.**
- **Stores.** Only `$S/rrk-adoption-04-reference.db` and `$S/rrk-adoption-04-resumed.db` and their
  shards are opened. The user's pilot store at the root, `~/ChamPBH-stores*` and
  `~/ScienceData/ChamPBH-rehearsals/` are not opened, listed into or written. `var/` is not
  committed and nothing in it is deleted.
- **`pip freeze`** before and after, compared by `diff`.
- **Stop conditions are real** (the prompt's §3, and rulings 3, 11 and 12 here). Stop the cluster
  first unless the stop is what failed. A stop goes to the user through the orchestrator, and the
  agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. Check `git log` before committing, and never
  assume `HEAD` is your own.

## 1. Before you dispatch

1. **The tree.** Branch and `HEAD`; `git status --short` shows only §0's untracked set; nothing
   staged; `git log --oneline 5d02258..HEAD` is this note's commit only.
2. **Ray and the registry.** `ray_cluster.sh status` exits 0; `list` prints no runs; no `var/`.
3. **Baselines.** The five roots at `5d02258`, serially, with the load. Give the agent the numbers.
4. **`pip freeze`**, kept in the orchestrator's scratch directory for the review.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Opus**. Give it:
- the prompt;
- the campaign README and this board;
- `HEAD` and the baselines;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad.
  The agent does not read `orchestrator/`, and does not read prompt 05.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/04-rehearse-registered-runs.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **The files it may add or change are §0's list, and nothing else.** No code, test or script is
  committed; its scratch scripts are committed only as text copies under `out/`.
- **§0's twelve rulings** bind it, each classified as §0 says.
- **Stop and ask** on any of the prompt's §3 conditions or §0's. The cluster is stopped first.
- **It replies with:**
  - the log's path and the message as committed;
  - the four runs: id, state, exit code, wall time, `results_store_id`, and the fingerprint's
    digest at finish;
  - the probe's output (ruling 8), and whether an alarm line appeared;
  - the signal's two intervals (ruling 7);
  - the two delays and how each was chosen, with the five `compute_time`s (ruling 6);
  - the hard-kill route line by line: `stale`, the refusal, `end`, `lost`, and what ruling 9 found
    after the kill;
  - `compare_stores`' result, the two digests, and `histories.csv`'s SHA-256;
  - the cluster's `start` and `stop`, with their exit statuses and times;
  - the suites before and after;
  - every deviation with its tag;
  - "State handed to the next prompt", verbatim.

## 3. The review — twelve checks

1. **Scope.** `git diff <agent>~1 <agent> --stat` lists §0's files only: no file under any code
   directory. `git status` shows §0's untracked set and nothing new. `git diff 3cd2be4 <agent> --
   main.py plot_by_beta.py plot_ScalarModel.py utilities.py` is empty. `var/` is not in the commit.
2. **The venv.** `pip freeze` now equals the one of §1, step 4.
3. **The cluster.** The lock free; no ChamPBH process; the four ports free (`ray_cluster.sh
   status`, exit 0). No live `ray::` process whose parent was ChamPBH's raylet. The log has
   `start`'s output, `ray status`, and `stop`'s exit status with its time.
4. **The runs, from the registry itself.** `list` now shows four runs: three `done`/`killed`, as
   H1–H4 say, and `resumed-b` `lost`. Read each `manifest.json` and `status.json` under `var/runs/`
   and check them against the committed copies and the document's table. That covers the state, the
   exit code, `script` and its SHA-256 against `shasum -a 256 main.py`, `results`,
   `results_store_id` (ruling 11), `git_head`, `git_dirty`, `scope` and `heartbeat_means` (as of
   `5d02258`). It also covers `ended` for `resumed-b`, with its reason.
5. **The order of the records.** In each launch log, the `** registered_run:` lines come before `>>
   packages:`. The manifest's `created` is no later than the log's birth time plus the imports'
   time, as ruling 4 says.
6. **H2's signal.** The log has the handler's line, then the marker 143, then the launcher's 143,
   and no alarm line. Recompute the interval of ruling 7 from the two quoted lines.
7. **H3's route.** The launcher's 137 and no marker. Check four things against the committed
   outputs:
   - the `stale` line;
   - the refused launch's three lines and the `ls var/runs` diff;
   - `end`'s output;
   - the `lost` record.

   Check ruling 9's four snapshots for a worker or actor of `resumed-b`'s job.
8. **The delays.** Re-read the `compute_time`s by the same read-only query on the reference store.
   Check that each delay follows ruling 6 from them, and that the document's account of what H2
   stored matches `stage_counts.py` on `resumed-a`'s and `resumed-b`'s logs.
9. **The stores, re-measured.** `compare_stores.py` on the two stores: the logged result.
   `store fingerprint` on each, without `--write`: the logged digests, and each equal to its
   sidecar's (`recorded`). `store show` on each: the runs as logged. `stage_counts.py` on the four
   logs: the logged counts.
10. **`histories.csv`.** `shasum -a 256 $L/plot-reference/QCD_Cosmology/histories.csv` is R0's.
11. **The records.** The board: row 04 ✅ with "this commit" and the log; H1–H6 ✅; the gate "R" ✅;
    `### 5.5`. The log in §5.1's form, every deviation classified, the commands verbatim. The
    document's figures match the outputs under `out/` and the commands as run. Nothing is claimed
    that no quoted line shows (the heartbeat's history above all: `status.json` keeps the latest
    beat only).
12. **The suites** at the agent's commit, serially: 18, 61 + 115, 78, 84, 61. `black` has nothing to
    check: no Python file is committed. If the two scratch scripts' copies are `.py` files under
    `out/`, they are text, not modules: no `__init__.py` makes them importable, and no test root
    reaches them.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 04* paragraph in §5.5. Fix small
  residue in a follow-up commit of its own:
  - "this commit" → the SHA (board, log and document);
  - the notes list, with this one marked "used for 04";
  - the status line: 4 of 5 landed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the message as committed;
  - the rulings of §0 that read the prompt otherwise than it was written: the kill in the scalar
    stage (1), the waits (3), the P5 probe (8), and what a `SIGKILL` left (9);
  - the four runs and the hard-kill route, in a few lines;
  - `compare_stores`, the digests and `histories.csv`;
  - the two stores and the run records left under `var/` for the user (README §3, §7).
- **Then 05**, whose note is written after this review.
