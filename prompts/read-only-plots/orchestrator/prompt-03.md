# Orchestrator — prompt 03, rehearse a read-only plotting run

Read [`../README.md`](../README.md) first: §0.3, §2.2 (03's charter), §3, §5 (rules 15 and 16, and
the one-off exception) and §6.1 (U5, U6). Then the board's §5.2 and §5.3, which record what 01 and
02 handed on. The conventions are those of [`prompt-02.md`](prompt-02.md) §0 ("Conventions"),
restated here where 03 needs them, and amended for a prompt that starts Ray.

**You do not write code.** You may:
- run the suites;
- read the rehearsal's store and outputs, `mode=ro` only, and re-take its checksums;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

You do **not** start a second cluster or re-run a driver: the rehearsal is the agent's, and a
second run of it is a stop for the user.

**The prompt:** [`03-rehearse-a-read-only-plotting-run.md`](../03-rehearse-a-read-only-plotting-run.md)
**Board items:** H1–H4 · **Gate:** "R" · **Closes:** nothing · **Opens:** only what the work finds ·
**Model:** Opus, as the prompt recommends · **No network.**

**Gate:**
- 01 and 02 have landed and been reviewed (`04486f4`, review `000be0c`; `9dc23db`, review
  `4ec9879`). `HEAD` is the commit that adds this note, or a later one that touches no code
  ("code" is anything outside `prompts/`; since `9dc23db` nothing outside `prompts/` has changed).
- `ray_cluster.sh status`: lock free, no ChamPBH process, the four ports free. No Ray process on the
  machine (`pgrep -lf 'gcs_server|raylet|ray::'`).
- `~/ScienceData/ChamPBH-rehearsals/read-only-plots/` does not exist.
- One prompt at a time in this checkout. 04 is not dispatched until 03 is reviewed.

## 0. What makes this prompt unusual

**It is the first prompt of the campaign that starts Ray, and the only one that runs `main.py` with
a datastore** (U5, the one-off exception, README §5). Everything 01 and 02 proved, they proved on
stand-ins and by `ast`. 03 is the only evidence of three things:
1. **A real plotting run, on a store `main.py` built, changes no byte of it.** 02's test 5 hashed a
   stand-in store; here the actors are real Ray actors opening real files `mode=ro`.
2. **`except ReadOnlyMiss` catches the refusal through `ray.get`.** Under Ray an actor's exception
   reaches the driver as `RayTaskError.as_instanceof_cause()` (Ray 2.53.0,
   `ray/_private/worker.py:1015-1016`, `:2978-2979`), a dual class that is an instance of the
   cause's class and forwards attribute reads to it (`ray/exceptions.py:182-183`). `plot_inputs`
   reads `e.class_name` before converting (`plot_inputs.py:90-95`). The `--T-stop-GeV 1e-7` run is
   the one place this is seen.
3. **The pool opens read-only under Ray.** The package prints a distinct line for it
   (`datastorekit/SQL/ShardedPool.py:681-685` at `v0.3.0`):
   `>> Opened existing sharded datastore "…" read-only with 16 shards (version label "2026.6.0", serial N)`.
   R2's plot logs printed the read-write line (`>> Opened existing sharded datastore "…" with 16
   shards`); this one must print the read-only line, once per plot script, and never the
   read-write one.

**The old rehearsals are gone.** `~/ScienceData/ChamPBH-rehearsals/` exists but is empty (its mtime
2026-10-13 11:49: the user removed R0–R2's stores after `dk-and-rpk-migration` closed). So nothing
of R0–R2 can be `cmp`'d. Every comparison with R2 is against its **recorded** numbers:
- `histories.csv`: SHA-256 `d7c62c37d8ad637a63854583ab11bc737109dcbfba92168dbddbaea75182c522`
  (`.documents/dk-and-rpk-migration/out/10-readers.txt:116`);
- the SM baseline line (`10-readers.txt:112`): `Yp=0.24688, D/H x1e5=2.45829, 3He/H x1e5=1.04193,
  7Li/H x1e10=5.48637`, `PRyM_version=bf24c3d+ri02+sr01+bt02`, `small_network=True`;
- `bbn_from_store`, β = 1.15 (`10-readers.txt:126-127`): stored `Yp=0.24687349447579363`
  `DoH=2.4859924812264325`; `prod` `Yp=0.2468734945 DoH=2.485992481 He3oH=1.046802492
  Li7oH=5.403050409`;
- the inventory (verification §3.5): `read-only; 16 shards (#0, …, #15); 21,292 records in 21
  classes`, `problems: none`, 5 / 5 / 5 records validated, value rows 42,617 / 42,617 / 7,475,
  per-β value counts 4947 / 4975 / 5002 / 9776 / 17917;
- the stage counts (`10-stage-counts.txt`): `reference` 5 / 5 / 5 / 5 in each of the three stages;
- `plot_by_beta`: "no models used elastic reflections (5 model(s) checked)".

A difference in any of these is a stop, except where the next paragraph expects one.

**What is expected to differ from R2, by design.**
- The build's job name and labels read `dk-and-rpk-migration-03-reference`, since `build.sh` is run
  unchanged. The store is `$R/03-reference.db`.
- The plot scripts' open line is the read-only one (above). The 16-against-10 shard warning stays
  (P4: the plot scripts pass no `shards=`, and `run_readers.sh` passes `--shards 16`).
- `plot_by_beta.py`'s Ray job name is `plot_by_beta` (02); in R2 both plot jobs were named
  `plot_ScalarModel`.
- The provenance line is R2's exactly (`installed_package_provenance()` today:
  `datastorekit 0.3.0 @ 2f5417ea76cdcd0d15d64d80d9085429bfacad21`, `requested v0.3.0`;
  `raypoolkit 0.2.0 @ eaef0ce60e2fc354bb32f0ad576010e6a83c1976`, `requested v0.2.0`; both
  `editable: False`). The venv: 101 lines of `pip freeze`, `ray==2.53.0`.
- Timings and the load (below).

**The store's files.** `$R/03-reference*.db` matches exactly the primary and its 16 shards (17
files). `run_readers.sh` step 0 copies them with `cp -p` into `$R/03-readers/before/`, a
subdirectory the glob does not reach. The checksums are taken on the 17 originals only.
- **What reads them, and how.** `--inventory` through the package's `read_inventory` (`mode=ro`, no
  Ray); the two plot scripts through `ShardedPool(read_only=True)` (actors `mode=ro`);
  `tools/bbn_from_store.py` and `tools/compare_stores.py` through `sqlite3` `file:…?mode=ro` (their
  `connect_ro`, `bbn_from_store.py:197-203`, `compare_stores.py:218-225`). None opens read-write, so
  any changed byte, mtime or size is a real finding and a stop.
- **What may be written outside `$R`, by design:** the shared lock
  (`~/ScienceData/.rehearsal-lock`, by `ray_cluster.sh` alone), `/tmp/ray-cpbh/` (the cluster's temp
  dir), the agent's scratch directory, and the committed outputs under `.documents/`. Nothing else:
  in particular no file in the repository root (`git status` shows the §1 untracked set and
  nothing new), and nothing under `/tmp/ray/` (its `session_*` count, 160 at writing, unchanged).

**The refusals of step 7, as the code now stands.** Each is run from the repository root, with
`--config exponential.yaml --database $R/03-reference.db --shards 16`, its own `--output
$R/03-refusals/<name>` and its log `$R/03-refusals/<name>.log`. Give each its exit status by `echo
$?` directly after it.
- **`--ray-address auto`** (and `--phi-init-Mp 5`): 01's check, `plot_by_beta.py:121-124`, exit 2,
  `RAY_ADDRESS_PROBLEM` on stderr, before `ray.init`. No new job in `ray list jobs`.
- **`--phi-init-Mp 5 --T-stop-GeV 1e-7 --ray-address $ADDRESS`**: this run **does** attach (it is a
  new job, the fourth) and opens the pool read-only. `T_stop` is looked up after the potentials and
  couplings and before the baseline (`plot_by_beta.py:1074-1080`, 02's deviation 1), so it must
  exit 2 with exactly
  `!! plot: the store holds no temperature for --T-stop-GeV 1e-07; main.py was not run with it, so no history in this store can match. Give the plot the options the build used`
  (`plot_inputs.py:64-83`, `_value` is `.10g`), with **no** `@@ plot_by_beta: SM baseline` line,
  no figure, and `$R/03-refusals/<name>` not created (the scripts create output directories only in
  `run_pipeline`, `plot_by_beta.py:315` ff.). The build stored `T_stop` = `T_CMB` = 2.3493e-13 GeV
  only, so 1e-7 is not stored. `sys.exit(2)` is inside the `with ShardedPool(…)`, so the pool's
  `__exit__` runs: the job's 16 `Datastore` actors and its broker must end `DEAD`, none `ALIVE`.
  The driver prints no `Traceback`; a traceback forwarded from an actor's worker (prefixed
  `(shardNNNN-store pid=…)`) is recorded verbatim and classified, not a stop by itself.
- **`--drop bbn-data --ray-address $ADDRESS`** (and `--phi-init-Mp 5`): the `--drop` refusal,
  `plot_by_beta.py:110-117`, exit 2 before 01's check and before Ray: no new job. Giving the valid
  address shows the refusal does not depend on a bad one.

Ray's status for the fourth job (`SUCCEEDED` or `FAILED` after `sys.exit(2)`) is recorded as Ray
gives it; it is Ray's bookkeeping, not a criterion. The expected job list at the end is four jobs:
the build (`01000000`), `plot_by_beta` (`02`), `plot_ScalarModel` (`03`), the `T_stop` refusal
(`04`). `--inventory` and the other two refusals start no Ray.

**PRyMordial solves.** The build makes five (one per β), `plot_by_beta.py` one (the SM baseline)
and `bbn_from_store.py` one. The prompt's "one PRyMordial solve" undercounts; the log says so as an
observation, not a deviation.

**The load.** At writing (22:20 BST) the 1-minute load was 30, from macOS daemons, Safari, a Time
Machine backup (`backupd-helper`) and other Claude sessions; no Ray, no foreign suite. R2's build
took 3 min 33 s at loads 9–63; R1's 11 min 7 s at 29–160. Record `uptime` at each step. A slow
build is not a stop; a build still running after 30 minutes is reported before anything else.
`test_prym_passenger.test_a_oscillating_case_completes` has a 60 s wall bound that fails at load
≳ 70 (board §5.2, check 10): if it fails, re-run it alone and record the load before calling it
anything.

**What the review exists to establish.**
- **(R1) The protocol.** The cluster started and stopped by `ray_cluster.sh` alone, under the lock;
  every driver given `--ray-address 127.0.0.1:6380` and every log naming 6380, never 6379; no
  `ray stop`; nothing left running; nothing written outside `$R` but §0's list.
- **(R2) H1.** The build exit 0; 5 / 5 / 5 / 5 in each stage; the provenance line.
- **(R3) H2.** Every reader exit 0; the 17 files byte-identical, mtimes and sizes unchanged, no
  `-journal`, `-wal` or `-shm`; `compare_stores` `RESULT: equal` with no bookkeeping difference;
  `histories.csv`, the baseline, the β = 1.15 BBN figures and the inventory equal to R2's recorded
  ones; the read-only open line in both plot logs.
- **(R4) H3.** The three refusals, each with its exit status and message verbatim; the `T_stop` one
  through a real actor, before the baseline and any figure; the store byte-identical after them.
- **(R5) H4 and the records.** The verification document, its outputs, the log and the board.

**Conventions.** All of these bind the agent, and the brief says so.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached
  --name-only` before committing. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its
  follow-up.
- **Suites.** Before `start` only, never with a cluster up: `CLAUDE.md`'s commands, ComputeTargets
  in its two halves, each in the foreground, serially, output to a file in the agent's scratch
  directory with the verdict grepped from it, and the load beside each. Expected: CosmologyModels
  18, ComputeTargets 61 + 91 (152), ObjectFactories 63, tools 34 (board §5.3, check 11).
- **Waiting.** Each background `build.sh` or `run_readers.sh` is waited on by its notification or a
  `Monitor` on its log, never a `sleep` loop.
- **Ray.** `ray_cluster.sh` alone starts and stops the cluster. Every driver gets `--ray-address
  127.0.0.1:6380`, every `ray` CLI call `--address 127.0.0.1:6380`. **No `ray stop`, ever.** A held
  lock (exit 3) is a stop: never wait on it, never break it.
- **Stores.** None opened but `$R`'s, and that one only by the five readers and by `sqlite3`
  `mode=ro` if the agent needs a count. The pilot store in the repository root is the user's and
  is never opened. `$R` is kept.
- **Scratch.** The agent's own subdirectory of the session scratchpad, never `/tmp`. `pip freeze`
  before and after goes there.
- **No code, test or script changes.** The rehearsal scripts run unchanged.
- **Stop conditions are real** (the prompt's §3). A stop is made after `ray_cluster.sh stop`, unless
  the stop is what failed; it goes to the user through the orchestrator, and the agent resumes with
  `SendMessage`.
- **Other sessions** may land commits on this branch, or hold the shared lock. Review against the
  agent's commit.

## 1. Before you dispatch

1. **The tree.** Branch and `HEAD`; `git status --short` shows only the untracked set (17
   `pilot-2026.6.0*.db`, `pilot-main.log`, `pilot-plot.log`, `pilot-out/`, `full_run_2026.6.0.sh`,
   `full_run_2026.6.0-refresh.sh`, `prompts/jordan-normalization/`); nothing staged;
   `git log --oneline 9dc23db..HEAD` touches no code.
2. **Ray and the lock.** `ray_cluster.sh status`; `pgrep -lf 'gcs_server|raylet|ray::'`; the count
   of `/tmp/ray/session_*` (160 at writing).
3. **`$R`** does not exist; `df -h ~/ScienceData` (71 GB free at writing; the pilot store is a few
   tens of MB).
4. **Baselines.** No code has changed since `9dc23db`, whose suites the 02 review ran (18, 61 + 91,
   63, 34). The agent runs them again as its step 1; the orchestrator does not duplicate them.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Opus**, in the background. Give it:
- the prompt;
- the campaign README and this board;
- `HEAD` and the baselines;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad.
  The agent does not read `orchestrator/`, and does not read prompt 04.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/03-rehearse-a-read-only-plotting-run.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **The files it may add or change** are `.documents/read-only-plots-verification.md`,
  `.documents/read-only-plots/out/03-*`, the log and this board. Nothing else; no code, test or
  script.
- **It starts Ray only through `ray_cluster.sh`, never runs `ray stop`, and opens no store but
  `$R`'s.**
- **Stop and ask** on any of the prompt's §3 conditions, after `ray_cluster.sh stop`.
- **It replies with** the log's path; the message as committed; `start`'s and `stop`'s output and
  exit statuses, with the lock's owner file; the build's wall time, stage times, stage counts and
  provenance line; each reader's exit status and wall time; the two plot logs' open lines and
  warnings, verbatim; the checksum table's verdict (before, after the readers, after the
  refusals), with any `-journal`/`-wal`/`-shm` search; `compare_stores`' result line;
  `histories.csv`'s SHA-256; the baseline line; the β = 1.15 figures; the inventory's head; each
  refusal's command, exit status and message verbatim; `ray list jobs` and the final actor states;
  the `pip freeze` comparison; every deviation with its tag; and "State handed to the next prompt",
  verbatim.

## 3. The review — eleven checks

1. **Scope.** `git diff <agent>~1 <agent> --stat` touches only §2's list. `git status` shows §1's
   untracked set and nothing new. Each file under `.documents/read-only-plots/out/` under about
   1 MB.
2. **R1, nothing left running.** `ray_cluster.sh status` (lock free, no process, ports free);
   `pgrep` empty; `/tmp/ray/session_*` still as §1 step 2. The log shows `start`'s output, `ray
   status`, and `stop`'s exit status and timing; no command in the transcript is `ray stop`.
3. **R1, the address.** `grep -l 6379` over `$R/logs/` and `$R/03-readers/*.log` and
   `$R/03-refusals/*.log` finds nothing; each Ray driver's log names `127.0.0.1:6380`.
4. **R2, the build.** `$R/logs/03-reference.log` ends `** driver exit status 0`; re-run
   `stage_counts.py` on it (5 / 5 / 5 / 5 per stage); the provenance line is the first line, once,
   and is R2's; `grep -c Traceback` 0.
5. **R3, the bytes, independently.** Re-take `shasum -a 256` and `/usr/bin/stat -f '%m %z %N'` of
   the 17 files and compare with the agent's before-table (committed): equal. `ls $R` shows no
   `-journal`, `-wal` or `-shm`. Also `cmp` each original against its `03-readers/before/` copy:
   that copy was taken by `cp -p` before any reader, so this is a second, independent before-image.
6. **R3, the readers' outputs.** `status.txt` all exit 0; `shasum` of
   `$R/03-readers/plot_by_beta/QCD_Cosmology/histories.csv` equals `d7c62c37…c522`; the baseline
   line, the reflections line and the β = 1.15 figures equal §0's recorded ones by `grep`;
   `5-compare_stores.log` `RESULT: equal`, and its JSON shows no bookkeeping difference; the
   inventory's head and counts equal §0's. Both plot logs print the read-only open line and no
   read-write one; `grep -c Traceback` 0 in all five step logs.
7. **R4, the refusals.** Each log under `$R/03-refusals/` read in full against §0: the messages
   verbatim, no baseline line and no figure in the `T_stop` run, its output directory absent;
   the `auto` and `--drop` logs start no Ray (no "Connecting to existing Ray cluster" line). The
   committed `ray list jobs` shows four jobs, and the actor listing none `ALIVE` before the stop.
8. **R1, nothing written outside `$R`.** `pip freeze` before and after equal (the agent's record,
   and `wc -l` now 101 with the three pins as §0); the repository's untracked set unchanged; the
   user's pilot store's mtimes unchanged since before dispatch (take them in §1).
9. **R5, the document.** `.documents/read-only-plots-verification.md` §1 holds everything H4 lists;
   the commands verbatim; the numbers match the outputs; additive in form (a dated §1 that later
   sections can follow).
10. **R5, the records.** Row 03 ✅, H1–H4 ✅, the gate "R" ✅, `### 5.4 After prompt 03 (<date>)`;
    `.documents/OPEN_ISSUES.md` unchanged unless the log opens an issue, and then in step. The log
    in §5.1's form, every deviation classified; the PRyMordial-count observation.
11. **The suites** at the agent's commit are not re-run if the agent's pre-`start` run passed and
    the commit touches no code (check 1); otherwise serially, as before.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 03* paragraph in §5.4. Fix small
  residue in a follow-up commit of its own:
  - "this commit" → the SHA (the board, the log, the verification document);
  - the orchestrator notes' list on the board, with this one marked "used for 03";
  - the status line: 3 of 4 landed; `prompts/INDEX.md`'s row.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:** the message as committed; the build; that the readers changed no byte of
  the store (the checksums, twice); `histories.csv` against R0–R2; the three refusals under real
  Ray; that `$R` is kept for them to remove; anything opened.
- **Then 04.** Its note is written after this review.
