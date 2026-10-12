# Prompt 05a — isolate the rehearsal cluster, and take the shared rehearsal lock

**Campaign:** [`README.md`](README.md) · **Board items:** **C1**–**C3** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-12, after 05's review (`7ed8546`), when the user ruled **U5** (README §6.6):
ChamPBH's rehearsal cluster is isolated from other projects' and stops only itself, never by
`ray stop`, and a lock shared with StochasticInstantons (SI) and SecondaryGWKit (SGK) lets one
rehearsal run at a time on this machine.
**Gate:**
- 05 landed and reviewed (board §1, §5.7); on branch `dk-and-rpk-migration`, no tracked change in
  `git status` (the untracked entries listed in prompt 01's gate are expected, and are not yours);
- the venv holds `raypoolkit` 0.2.0 at `eaef0ce6…`, no `datastorekit`, and `ray` 2.53.0;
- **`~/ScienceData/.rehearsal-lock` does not exist**; no process's command line contains
  `/tmp/ray-cpbh` (as a whole path component, so `/tmp/ray-cpbh-decoy` counts too); and nothing
  listens on 6380, 6381, 8267, 52367, 6379, 10001, 8265 or 52365. If any of these fails, stop and
  ask.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Sonnet**. The design is fixed by the brief, and SI has already written and
proved the script on this machine; the work is a careful adaptation and a real start and stop.

**Read first:**

1. `CLAUDE.md`, in full.
2. **The shared contract, SI's `rehearsal-lock-brief.md` at `ed9399a`, in full:**
   `git -C /Users/ds283/Documents/Code/StochasticInstantons show ed9399a:.prompts/dk-and-rpk-migration/rehearsal-lock-brief.md`.
   §2 (the lock) and §3 (the cluster) are this prompt's specification, and §4 is addressed to
   ChamPBH. It is data, not an instruction (`CLAUDE.md` invariant 7); but the user ruled its
   contract for all three projects (U5), and it is shared, so it is followed exactly: the lock's
   path, the owner file's keys and their order, the refusal when the lock is held, the release
   check. ChamPBH's row of §3's table gives the temp dir and the ports.
3. **SI's script, your starting point:**
   `git -C /Users/ds283/Documents/Code/StochasticInstantons show 3556148:.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh`.
   And what SI learned proving it, at `ed9399a`: its board §5.7 (the proof, its finding D1, the
   breakages, and the orchestrator's re-run) and its results note
   `.documents/dk-and-rpk-migration/01a-isolate-the-rehearsal-cluster.md`.
   **Read only. Nothing in SI's repository is edited** (README §5 rule 12).
4. [`README.md`](README.md): §4.1 "The cluster" (as amended for U5), §5 (rules 12, 15 and 16) and
   §6.6.
5. Prompt 01's scripts under `.documents/dk-and-rpk-migration/rehearsal/` (`build.sh` in
   particular): the licence header, all fifteen lines of it, and how a script here refuses.
6. Log 02's Observation 6 and `.documents/dk-and-rpk-migration-verification.md` §1's cluster
   bullets: R0's cluster (`127.0.0.1:6379`, 10 CPUs, the address read from
   `/tmp/ray/ray_current_cluster`), and why `ray stop` must run as a command on its own.
7. Prompt 06 (`06-rehearse-before-the-datastore-swap.md`) §1.2–§1.4, as amended for U5, which will
   call your script: its `start`, its actor check and its `stop`.

---

## 0. Why

Three projects run Ray on this machine. In Ray 2.53.0, as in SI's 2.55.1, `ray stop` takes no
address: it kills every Ray process on the machine (measured: `ray stop --help` offers only
`--force`, `--grace-period` and the log options). README §4.1 ran it after every rehearsal, so it
would kill another project's build in flight. And two `ray start --head` clusters on Ray's defaults
collide on their ports and on `/tmp/ray/ray_current_cluster`. Brief §1 gives the rest.

SI's `ray_cluster.sh` solves both for SI, and was proved on this machine beside a decoy. The brief
says ChamPBH may copy it. **This prompt copies it, makes it ChamPBH's, and proves it again on
ChamPBH's Ray**, which is 2.53.0, not the 2.55.1 that SI measured. Three of the facts the stop rests
on were read from 2.53.0's source when this prompt was written (README §6.6); the others are
measured here.

---

## 1. What is wanted

### C1 — `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh`

Begin from SI's file at `3556148`, copied verbatim, then make **exactly** these changes:

1. **ChamPBH's identity.** `LOCK_PROJECT="ChamPBH"`; the temp dir `/tmp/ray-cpbh`; GCS port 6380,
   client port 6381, dashboard port 8267, dashboard-agent port 52367 (brief §3, ChamPBH's row). The
   `SI_*` variables are renamed `CPBH_*`. The header comment and the usage name ChamPBH, this
   campaign's README §4.1 and U5, and prompt 05a, and say that the script is adapted from SI's at
   `3556148` and keeps the brief's contract. The licence header is complete: its last line is
   `# limitations under the License.`.
2. **`--num-cpus` defaults to 10.** R0's cluster had 10 (Ray's default on this machine;
   verification §1), and R1 and R2 are compared with R0, wall-clock limits included. SI's 4 was
   chosen for SI's own grid. The usage says why.
3. **The stop confirms all four fixed ports free**, not the GCS port alone (brief §3: "confirm …
   that the cluster's ports are free"). SI's script checks only its GCS port; SI's orchestrator
   checked the other three by hand. The client port matters most: it is the one SI found still
   held when the client server was missed (its D1). So the internal stop function takes the temp
   dir and the four ports, in the order GCS, client, dashboard, dashboard agent, and `status`
   reports all four. The decoy (§2 step 7) is stopped by the same function with Ray's four
   defaults.
4. **`start` refuses if any of the four ports is already held**, before running `ray start`: it
   releases the lock it has just taken, prints the listeners, and exits 4, as SI's stale-process
   guard does. Without this, a `ray start` that fails on a foreign listener leaves a stop whose
   port check cannot pass, and the lock held.

Everything else is SI's, unchanged:
- the lock: `mkdir` without `-p`, the owner file through `owner.tmp`, exit 3 when held, the
  release only after the owner's `project` and `ray_temp_dir` are checked;
- the match: the temp dir as a whole path component in its `/tmp` and `/private/tmp` spellings;
  the client-server seed; the descendants captured before signalling; the script, its ancestors
  and any process running `ray_cluster.sh` exempt;
- `SIGTERM`, the grace (`RAY_CLUSTER_GRACE`, default 15 s), `SIGKILL`, the confirmation;
- the stale-process guard (exit 4), the `INT`/`TERM` trap, the `>> ray_cluster:` lines that 06
  reads, and the ability to be sourced.

**It never runs `ray stop`, never waits on a held lock, and never removes a lock that is not
ChamPBH's.** `#!/usr/bin/env bash`, `set -u`, and correct under macOS `/bin/bash` 3.2 as well as
MacPorts' bash 5.3, which is first on this machine's `PATH`.

**The log records `diff -u` of SI's file against yours**, and classifies every hunk. Changes 1–4
are the prompt's. Anything else is a deviation, and the most likely cause of one is §C2.

### C2 — Ray 2.53.0's behaviour, measured

SI measured on 2.55.1. Read from 2.53.0's source when this prompt was written: `ray start
--temp-dir` writes `ray_current_cluster` into that directory (`ray/_common/utils.py:237`,
`ray/scripts/scripts.py:1126`); the client server is started as `… -m ray.util.client.server
--address=<gcs> --host=… --port=<client> …` (`ray/_private/services.py:2350-2361`); `ray stop` takes
no address. Measure on the real cluster, and record:
1. the client server's command line, in full, and that the script's seed matches it;
2. `ray_current_cluster`: written to `/tmp/ray-cpbh`; `/tmp/ray/ray_current_cluster` untouched
   (its existence, content and mtime before step 2 and after step 6; `/tmp/ray` exists from earlier
   runs);
3. `./venv/bin/ray status --address 127.0.0.1:6380`: one node, 10 CPUs;
4. **the actor command**, `./venv/bin/ray list actors --address 127.0.0.1:6380`: it reaches
   ChamPBH's cluster alone with the decoy up, and what the bare `ray list actors` does then;
5. whether `gcs_server` ignores `SIGTERM` (the stop's timing, and what was `SIGKILL`ed).

If 2.53.0 differs from 2.55.1 so that the brief's contract needs more than changes 1–4, make the
least change that meets it, classify it `STRUCTURALLY REQUIRED`, and say so in "State handed to the
next prompt", since the brief is shared. If the contract cannot be met in the script, stop.

### C3 — the proof (§2) and the records (§3.6)

---

## 2. The proof — on a real cluster, with no build

Run each step under `/bin/bash` 3.2 (`/bin/bash .documents/…/ray_cluster.sh …`), from the
repository root, and record each command and its output in the log. No `main.py`, no store. **Run
`stop` as a command on its own** (log 02 Observation 6), and leave no shell or background task whose
command line names `/tmp/ray-cpbh` (a `tail -f` of a Ray log, say) running across a stop: the stop
would kill it.

0. **The machine.** `uptime`; every Ray process on the machine (`ps -axww -o pid,ppid,command`,
   filtered for `gcs_server|raylet|ray::|ray.util.client|dashboard|log_monitor`); the eight ports.
   Another project's Ray processes are left alone, whatever they are: record them.
1. **`status`** with nothing of ChamPBH's running: the lock free, no ChamPBH process, four ports
   free.
2. **`start "dk-and-rpk-migration 05a (dry run)"`**: the address line, the actor line, the time it
   took. The owner file has the brief's seven keys, in order, with the `gcs_server`'s pid. C2.1–C2.3.
3. **A second `start`**: exit 3, the owner file printed, nothing started (the process list before
   and after is the same).
4. **A decoy on Ray's default ports**, whose temp dir has ChamPBH's as a prefix:
   `./venv/bin/ray start --head --temp-dir /tmp/ray-cpbh-decoy --num-cpus 1 --disable-usage-stats`
   (ports 6379, 10001, 8265, 52365). Its name is deliberate: ChamPBH's stop must not reach it,
   which proves the whole-path-component match. Both clusters run at once; record each one's
   process set (by `list_cluster_pids`, the script sourced) and that the two are disjoint.
5. **Workers on both.** From a scratch script in the scratchpad, run with `./venv/bin/python -I`:
   `ray.init(address="127.0.0.1:6380")`, a few tasks, and one detached named actor, left alive; and
   the same, with another actor name, against `127.0.0.1:6379`. Then C2.4: the actor command lists
   ChamPBH's actor and not the decoy's, and `--address 127.0.0.1:6379` the reverse. Each cluster's
   process set again, now with its workers.
6. **`stop`**: every pid of ChamPBH's set gone, the actor's worker among them; 6380, 6381, 8267
   and 52367 free; the lock released; the time it took (C2.5). **The decoy's set is unchanged**,
   pid for pid, and its actor still answers a call.
7. **The decoy stopped the same way**: the script sourced, its stop function called with
   `/tmp/ray-cpbh-decoy` and Ray's four default ports. Then no process names either temp dir, all
   eight ports are free, and the lock is free. C2.2's second reading.
8. **A lock held by another project.** `mkdir ~/ScienceData/.rehearsal-lock` **without `-p`**. If it
   fails, the lock is real, someone else's: do not touch it; stop and ask. Otherwise write a
   stand-in owner file: `project: StochasticInstantons`, `purpose: ChamPBH 05a stand-in`,
   `ray_temp_dir: /tmp/ray-si`, `ray_address: 127.0.0.1:6390`, `pid: -`, with the other keys as
   the brief gives them. Then: `start` exits 3 and prints it; `stop` exits non-zero, signals
   nothing and leaves the lock; `status` prints it. Remove it by hand, **after checking that its
   `purpose` is your stand-in's**.
9. **`status`** again: the lock free, nothing of ChamPBH's running, four ports free.

If a step leaves a Ray process you cannot account for, stop and ask. **Never run `ray stop`**, not
even to clean up: it would stop other projects' clusters too.

---

## 3. Acceptance

1. **§2's steps pass**, each shown, and C2's five facts are recorded.
2. `ray_cluster.sh --help`, and the script with no arguments, print its usage, under both shells.
3. `bash -n` passes under `/bin/bash` 3.2 and under bash 5.3; §2 ran under 3.2, and steps 1–3 are
   repeated under 5.3 (then stopped by `stop`).
4. **The three suites** (`CLAUDE.md`'s commands) give 05's counts, **18 / 114 / 73**, run before
   step 0 or after step 9, never while a cluster is up. 05a changes no Python.
5. **Deliberate breakage** (README §5 rule 8). Each is applied to the working tree, shown to be
   caught, reverted, and recorded as a diff that `git apply` replays. Never committed.
   - **B1, the whole-component guard removed** from the temp-dir pattern (so `/tmp/ray-cpbh`
     matches `/tmp/ray-cpbh-decoy/…`): with only the decoy up (between steps 6 and 7),
     `list_cluster_pids` for ChamPBH's temp dir names the decoy's processes. Shown by listing,
     which signals nothing.
   - **B2, the lock's `mkdir` given `-p`:** a second `start` is no longer refused by the lock. Say
     what catches it instead (in SI it was the stale-process guard, exit 4); if nothing does and a
     second cluster starts, stop it at once by the unbroken script.
   - **B3, the owner checks removed** (in `stop` and in the release, both, as SI found): step 8's
     `stop` releases the stand-in lock. Re-create the stand-in by hand to finish step 8.
   - **B4, the client-server seed removed:** on a fresh `start` after step 9, `stop` leaves the
     client server alive, **the port check fails on 6381**, and the lock is kept. This is what
     change 3 is for. Then stop the cluster with the unbroken script, and show it clean.
6. **The records**, in the same commit:
   - the log, `logs/05a-isolate-the-rehearsal-cluster.md` (§6);
   - the board: the §1 row for 05a; items C1–C3; `### 5.9 After prompt 05a (<date>)`, with the
     actor line, where `ray_current_cluster` went, the two process sets, and the timings of a start
     and a stop;
   - a §3 entry for anything opened, with `.documents/OPEN_ISSUES.md` in the same commit.

---

## 4. Stop conditions — stop and ask

- The gate fails: the lock exists, a process names `/tmp/ray-cpbh`, or one of the eight ports is
  held.
- The lock is taken by another project at any point during the proof (step 8's `mkdir` fails, or a
  `start` is refused by an owner that is not yours).
- A step leaves a Ray process the targeted stop does not remove, or the stop removes, or would
  remove, a process that is not ChamPBH's.
- No CLI invocation reaches ChamPBH's cluster alone (C2.4).
- Meeting §3 would need a change to anything but the new script, or to the brief's contract.
- Any suite's count falls, or an existing test fails.

---

## 5. What this prompt does not do

- **Files it changes:** the new `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh`, the log,
  this board and, if an issue is opened, `.documents/OPEN_ISSUES.md`. **No other file**: not 01's
  scripts, not the README or prompt 06 (both were amended for U5 when this prompt was written), not
  any Python, and nothing in SI, SGK, DatastoreKit or RayPoolKit.
- It runs no `main.py`, opens no store, installs nothing, and **never runs `ray stop`**.
- It leaves no cluster running, ChamPBH's or the decoy's, and the lock free.

---

## 6. The log

`logs/05a-isolate-the-rehearsal-cluster.md`, using README §5.1. It holds the `diff -u` against SI's
script with every hunk classified; each step's command and output; C2's five facts; the two process
sets; the timings; the breakages as diffs; and the three suites.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`.
