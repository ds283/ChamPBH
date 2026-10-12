# Orchestrator — prompt 05a, isolate the rehearsal cluster

Read [`../README.md`](../README.md) first: §4.1 "The cluster" (as amended for U5), §5 (rules 8, 12,
15, 16) and §6.6. The conventions are those of [`prompt-01.md`](prompt-01.md) §0 ("Conventions"),
which are not repeated here except where 05a changes them.

**You do not write code.** You may:
- run the agent's script (`status`, `start`, `stop`, and its functions sourced), the suites and
  read-only checks, from the session scratchpad;
- start a decoy cluster on Ray's defaults, and Ray tasks and actors against either cluster, from a
  scratch script, as the prompt's proof does;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- fix small residue in a follow-up commit of your own (§4).

**You never run `ray stop`**, and you start nothing while the rehearsal lock is another project's.

**The prompt:** [`05a-isolate-the-rehearsal-cluster.md`](../05a-isolate-the-rehearsal-cluster.md)
**Board items:** C1–C3 · **Closes:** nothing · **Narrows:** nothing · **Opens:** only what the work
finds · **Model:** Sonnet, as the prompt recommends · **No network.**

**Gate:**
- 05 landed (`8e77578`) and was reviewed (`7ed8546`, residue `e513e8c`); 05a was written and
  README §4.1, §5, §6.6 and prompt 06 amended for U5 (`d538aa5`).
- `HEAD` is the commit that adds this note, or a later one that touches no code ("code" as
  prompt-01.md's gate defines it, and `.documents/dk-and-rpk-migration/rehearsal/`). `git log
  --oneline 7ed8546..HEAD -- . ':!prompts'` names only `e513e8c` (licence headers).
- The rehearsal lock is free, no process names `/tmp/ray-cpbh`, and none of the eight ports is
  held (the prompt's gate). If any of these fails at dispatch, stop and ask the user.
- One prompt at a time in this checkout.

## 0. What makes this prompt unusual

**It runs real processes, and its stop kills by pattern.** Every earlier prompt of this campaign
but 02 started no Ray. 05a starts two clusters and stops them by signalling every process whose
command line matches a regular expression. A wrong match does not fail a test: it kills something.
So the review re-runs the start, the decoy and the stop itself, and checks the match against the
process table before anything is signalled.

**It copies a shared contract, so a difference from SI's script is the thing to look at.** The
lock is shared with StochasticInstantons (SI) and SecondaryGWKit (SGK): ChamPBH's script must take
and release it exactly as SI's does, or one project could break another's lock. The prompt fixes
the changes (1–4); `diff -u` against SI's file at `3556148` must show those and nothing else
unclassified. **A hunk that touches the lock functions (`acquire_lock`, `release_lock`,
`write_owner`, `owner_field`, `print_owner`) beyond the names is a finding**, whatever its tag.

**Three of the four changes have no breakage of their own.** B4 shows change 3 (the four-port
check) is needed; nothing in the prompt's §2 or §3 shows change 4 (`start` refuses a held port)
working. The review exercises it (check 6).

**The user's staged file.** Since 05's review, `prompts/jordan-normalization/source/phiref_iter.py`
is **staged as a new file** (`A` in `git status`), by the user or another session: it is not
05a's, and it predates this note. A plain `git commit` would sweep it into 05a's commit. So **the
agent commits by pathspec**: `git add <its files>`, show `git diff --cached --name-only` (which
lists `phiref_iter.py` too, and the agent says so), then `git commit -F <msg> -- <its files>`. A
pathspec commit takes only the named paths and leaves the user's entry staged (checked on a scratch
repository). **The agent never unstages, restores or commits it.** The review checks it is still
staged, with blob `b89b3ed7…`, and absent from 05a's commit.

**What moves on purpose:**
- **New** `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh`, mode `100755` as SI's and as
  01's `build.sh` and `run_readers.sh`.
- The log, the board (§1 row, C1–C3, `### 5.9`), and `.documents/OPEN_ISSUES.md` only if an issue
  opens.
- Nothing else: no Python, not 01's scripts, not the README or prompt 06.

**The facts, checked by the orchestrator on 2026-10-12 at `d538aa5`.** Pass them on.
- **The tree.** Branch `dk-and-rpk-migration`, `HEAD` `d538aa5` before this note. Untracked: the
  user's 17 `pilot-2026.6.0*.db` files, `pilot-main.log`, `pilot-plot.log`, `pilot-out/`,
  `full_run_2026.6.0.sh`, `full_run_2026.6.0-refresh.sh`, and
  `prompts/jordan-normalization/source/brief_jordan_normalization.md`. Staged: `A
  prompts/jordan-normalization/source/phiref_iter.py` (above). None is the agent's.
- **Ray and the lock.** No Ray process (`ps -axww`, filtered for
  `gcs_server|raylet|ray::|ray.util.client|dashboard|log_monitor|ray-cpbh`): none. Ports 6380,
  6381, 8267, 52367, 6379, 10001, 8265, 52365: none held (`lsof -nP -iTCP:<p> -sTCP:LISTEN`).
  `~/ScienceData/.rehearsal-lock` absent; `~/ScienceData` holds only `ChamPBH-rehearsals/`.
  SGK's cluster that 05's review found up at 09:19 is gone.
- **The temp dirs.** `/tmp/ray` (mtime `1791799001`, 2026-10-12 10:56), `/tmp/ray-si` and
  `/tmp/ray-decoy` (SI's 01a) exist. **`/tmp/ray/ray_current_cluster` does not exist**, so C2.2's
  "before" reading is its absence, and "untouched" means it is still absent after step 6 and step 7.
  `/tmp/ray-cpbh` and `/tmp/ray-cpbh-decoy` do not exist.
- **The venv.** `ray==2.53.0`; the one `raypoolkit @ git+https://github.com/ds283/RayPoolKit@eaef0ce6…`
  line; no `datastorekit`; `pip check` clean; 100 lines of `pip freeze`. `./venv/bin/ray --version`
  is `2.53.0`. The state API and dashboard import (`ray.dashboard`, `aiohttp`, `ray.util.state`), so
  `ray list actors` can work. 05a installs nothing.
- **The shells.** `bash` on `PATH` is MacPorts' `/opt/local/bin/bash`, 5.3.20; `/bin/bash` is
  3.2.57. So `#!/usr/bin/env bash` gives 5.3 when the script is run as `./…/ray_cluster.sh`; the
  proof runs it as `/bin/bash …/ray_cluster.sh` to get 3.2. `shellcheck` is not installed; `nc` is
  (`/usr/bin/nc`).
- **The machine.** 10 CPUs (`hw.ncpu`), host `mps026088`. **The load was high at dispatch**: 36 at
  11:41, then 253 / 114 / 68 at 11:46, from macOS's Time Machine (`backupd`,
  `TimeMachineSettings`) and Spotlight, not from any campaign. Ray's start is slower under load; the
  stop's 15 s grace is the same.
- **SI's script at `3556148`**, 363 lines, mode `100755`:
  - `list_cluster_pids DIR GCS_PORT` (`:152-181`) seeds on the temp-dir pattern and on the client
    server `ray[.]util[.]client[.]server --address=127[.]0[.]0[.]1:<GCS>( |$)`, closes over
    children, and exempts itself, its ancestors and any command line naming `ray_cluster.sh`. Its
    signature needs only the GCS port, and change 3 does not need to change it: the prompt's step 4
    calls it with the temp dir and GCS port.
  - `stop_cluster DIR GCS_PORT` (`:194-244`) checks the GCS port alone (`:237-241`); `cmd_status`
    (`:346`) reports the GCS port alone. These are change 3's places.
  - `cmd_start` (`:248-304`): `acquire_lock`, then the stale-process guard (exit 4, `:265-269`),
    then the trap and `ray start`. Change 4's check belongs beside the guard, after the lock is
    taken, before the trap.
  - With no arguments it prints the usage on stdout and exits 2; `--help` exits 0.
- **SI's proof** (its results note at `ed9399a`): start 11 s under no load; stop about 19 s,
  because `gcs_server` ignores `SIGTERM` and the grace runs out; `ray_current_cluster` written into
  the temp dir and kept after the stop (stale); the bare `ray list actors` with two clusters up
  fails with "Found multiple active Ray instances"; the dashboard address without a scheme times
  out.
- **Ray 2.53.0's source** agrees with SI's 2.55.1 in the three places README §6.6 names. Behaviour
  is C2's to measure.
- **What the breakages will do, read from SI's code** (the agent confirms by running them):
  - **B2** (`mkdir -p` on the lock): the second `start` passes `acquire_lock`, **overwrites the
    running cluster's owner file** with `pid: -`, and then a guard (stale processes, or change 4's
    held ports, whichever comes first) refuses with exit 4 **and calls `release_lock`, which
    removes the live cluster's lock**, since the owner is ChamPBH's. So after B2 the cluster runs
    unlocked. The log records the lock's state after B2, and the unbroken `stop` then finds no lock
    to release ("no lock to release"), which is expected. If the agent re-creates nothing, that is
    right: the cluster is stopped by the unbroken script and `status` then shows the lock free.
  - **B3**: `stop` with the stand-in lock and nothing of ChamPBH's running finds no process, sees
    the four ports free and releases the stand-in lock. Caught: the stand-in is gone.
  - **B4**: SI's D1. `stop` leaves the client server (parent `launchd`) alive on 6381; with change
    3 the port check fails and the lock is kept.
- **Workers and actors (step 5).** A detached named actor is found again only in its namespace: the
  scratch driver passes `namespace=` to `ray.init` and `ray.get_actor`. The driver exits after
  creating it (its command line names no temp dir, so a driver left running would neither be
  stopped nor counted). Run with `./venv/bin/python -I`, from the repository root.
- **Two hazards of the agent's own tools.**
  - The stop kills any process whose command line names `/tmp/ray-cpbh`, the agent's own shells
    included, unless they are the stopping script's ancestors. **No two Bash calls run in parallel
    while a `stop` runs**, and no background task whose command names `/tmp/ray-cpbh` (a
    `tail -f`, a `sleep`-and-check loop) is left running across one.
  - `/tmp/ray-cpbh` is a prefix of `/tmp/ray-cpbh-decoy`. A pattern or `rm` that uses the shorter as
    a plain prefix reaches the decoy's. The script's match is by whole component; the agent's own
    `ps | grep` filters should be too.
- **The suites at `e513e8c`**: CosmologyModels 18, ComputeTargets 114 (61 with `-p
  'test_[a-f]*.py'`, 53 with `-p 'test_[g-z]*.py'`), Datastore 73. Re-run by the orchestrator at
  dispatch: Datastore 73 OK, in 15.9 s under the load (3.5 s at 05's dispatch). Run them before
  step 0 or after step 9, never with a cluster up.
- **`.documents/OPEN_ISSUES.md`** has **26 open**, 3 on this board, last updated 2026-10-12. If 05a
  opens an issue, its row goes under `### 1.11 dk-and-rpk-migration`, and the count and the
  header's prose for this board are corrected in the same commit.
- **The siblings.** SI at `ed9399a` (branch `dk-and-rpk-migration`), SGK at `f11e3e7` (branch
  `handover-remedial`, another session's), DatastoreKit at `5f972be`, RayPoolKit at `1b84df0`; each
  tree clean. SI is read with `git -C … show`, never checked out. Nothing is written in any of the
  four.
- **The rehearsal stores** under `~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/` (42
  entries with `logs/`) are 02's. 05a opens none of them, and no other store.

**What the review exists to establish.**
- **(E1) C1 is SI's script with changes 1–4.** Every hunk of `diff -u` is one of them or a
  classified deviation; the lock functions differ by the names alone; no `ray stop` is run by it.
- **(E2) The proof holds when re-run by the orchestrator**: the start, the refused second start, the
  disjoint process sets beside the decoy, a stop that leaves the decoy pid for pid, the lock
  released, and the decoy stopped by the same function.
- **(E3) C2's five facts** are measured on 2.53.0 and agree with the orchestrator's re-run.
- **(E4) The breakages are caught**, B1–B4 replayed from the log's diffs; and change 4 refuses a
  held port, releasing the lock and starting nothing.
- **(E5) Nothing else moved.** Only §5's files; the user's staged file still staged; the venv; no
  store opened; the siblings untouched; no Ray process, no lock and no port left behind; the suites
  at 18 / 114 / 73.
- **(E6) The records.** The board's §1 row, C1–C3, `### 5.9` with the actor line, where
  `ray_current_cluster` went, the two process sets and the timings; the log in README §5.1's form.

**Conventions particular to 05a.** prompt-01.md's conventions hold, except that **the agent starts
Ray**, by the script and by the decoy's `ray start` only. In addition:
- **git.** The only commit on `dk-and-rpk-migration` is the agent's one, made by pathspec (above).
  No `checkout`, `switch`, `stash`, `reset`, `restore --staged` or `rebase`.
- **Scratch.** The proof script, the worker script and any captured output live in the agent's
  subdirectory of the session scratchpad. Nothing under `.documents/` but the new script, even for
  a moment; the breakages are applied to the script in place and reverted.
- **Ray.** Never `ray stop`. No `main.py` or other driver of ChamPBH's. Nothing left running.

## 1. Before you dispatch

1. **The tree.** Record the branch and `HEAD`. `git status --short` shows only the entries of §0,
   the staged `phiref_iter.py` among them. `git log --oneline 7ed8546..HEAD` touches no code.
2. **Ray, the lock, the ports**: as §0, by `ps`, `ls` and `lsof`.
3. **The venv.** `pip freeze` saved in the scratchpad; `pip check` clean.
4. **The load.** `uptime`, recorded and passed on.
5. **Hygiene baselines**, for check 9: the rehearsal stores' mtimes (`/usr/bin/stat -f '%m %z %N'`
   over the directory and its `logs/`, 42 entries); the four siblings' `HEAD`s; `git status
   --porcelain` of this tree; the mtime of `/tmp/ray`.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Sonnet**. Give it:
- the prompt;
- the campaign README and this board;
- `HEAD`, the load and §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the
  session scratchpad. The agent does not read `orchestrator/`, and does not read prompts 06 onward
  beyond what the prompt's "Read first" item 7 names.

Tell it plainly:
- **One commit, at the end, by pathspec.** Its log is `logs/05a-isolate-the-rehearsal-cluster.md`,
  in README §5.1's form, with the prompt's §6 additions.
- **The files it may change** are the prompt's §5 list. **No Python, nothing in SI, SGK,
  DatastoreKit or RayPoolKit, nothing in the venv, and not the user's staged file.**
- **Never `ray stop`.** No parallel Bash calls while a `stop` runs.
- **Stop and ask** on any of the prompt's §4 conditions.
- **It replies with:**
  - the log's path and the commit;
  - `git diff --cached --name-only` before the commit, and `git show --stat HEAD` after it;
  - `diff -u` against SI's file, every hunk classified;
  - each proof step's command and output, and C2's five facts;
  - the two process sets, and the timings of each start and stop;
  - the four breakages, each as a diff, and what caught each;
  - the step 3 and acceptance item 3 runs under bash 5.3, and `bash -n` under both;
  - the three suites, with the load;
  - every deviation with its tag;
  - the "State handed to the next prompt" section, verbatim.

## 3. The review — ten checks

1. **Scope.** At the agent's commit, `git show --stat HEAD` touches only the new script, the log
   and the board (and the index, if an issue opened). `git ls-tree HEAD` gives the script mode
   `100755`. `git status` shows §1's set and nothing new; `phiref_iter.py` is still `A`, its staged
   blob `b89b3ed7…`, and not in the commit. `git log` since this note shows no other commit of the
   agent's, and `git branch` no leftover branch.
2. **E1, the diff.** From the scratchpad, `diff -u` of SI's file at `3556148` against the agent's.
   Every hunk is change 1 (names, the temp dir, the ports, the header and usage), 2 (`ncpus=10` and
   the usage's reason), 3 (the stop and `status` take four ports, in the order GCS, client,
   dashboard, agent) or 4 (the held-port refusal, exit 4, the lock released), or a deviation the
   log classifies. The lock functions differ only by `SI_` → `CPBH_` and the project name. The
   licence header is 15 lines and ends `# limitations under the License.`. `grep` for an invocation
   of `ray stop` finds only the usage and comments.
3. **E1, statically.** `bash -n` under `/bin/bash` and `/opt/local/bin/bash`. `--help` exits 0 and
   no arguments exit 2, each printing the usage, under both.
4. **E2–E3, the re-run.** If the lock is free and the eight ports free, run under `/bin/bash`, from
   the root, each step a command of its own: `status`; `start "dk-and-rpk-migration 05a (review)"`;
   the owner file (seven keys, in order, the `gcs_server`'s pid); `./venv/bin/ray status --address
   127.0.0.1:6380` (one node, 10 CPUs); the client server's command line; the decoy
   (`/tmp/ray-cpbh-decoy`, Ray's defaults); `list_cluster_pids` for each, sourced, and their
   intersection empty; `ray list actors --address` each; `stop`, timed; the decoy's set pid for pid
   after it; the decoy stopped by the sourced `stop_cluster` with its four ports; `status`. Then
   `/tmp/ray/ray_current_cluster` still absent; `/tmp/ray-cpbh/ray_current_cluster` holds
   `127.0.0.1:6380`. The figures agree with the log's: the same counts of processes per cluster
   before workers, and a stop of about 19 s.
5. **E4, the breakages.** In `bash`, each diff from the log: `git apply --check`, `-R --check`,
   apply, show, revert. B1 by listing, with only the decoy up, inside check 4 (after ChamPBH's stop,
   before the decoy's). B2 with ChamPBH's cluster up, inside check 4 (before its stop): record the
   lock after it. B3 with a stand-in lock (`purpose: ChamPBH 05a review stand-in`), removed by hand
   after checking the purpose. B4 on a fresh start after check 4: the stop exits non-zero naming
   6381, the lock kept; the unbroken stop cleans up. `git status` clean after each.
6. **E4, change 4.** With nothing of ChamPBH's running and the lock free, hold 6381 with
   `nc -l 127.0.0.1 6381` in a background task; `start` exits 4, prints the listener, leaves the
   lock free and starts nothing (no process names `/tmp/ray-cpbh`). Stop the `nc` by its pid. Its
   command line names no temp dir, so no stop would reach it.
7. **E3, C2 recorded.** The log's five facts: the client server's full command line and the seed
   matching it; `ray_current_cluster`'s place, before and after; `ray status`; the actor command
   alone with the decoy up, and the bare form's failure; `gcs_server` and `SIGTERM` (the timing, and
   which pids were `SIGKILL`ed). Each agrees with check 4.
8. **E5, the suites.** The agent's: 18, 114 (61 + 53), 73, with the load, run with no cluster up.
   The orchestrator re-runs `Datastore/tests` at the agent's commit: 73 OK.
9. **E5, hygiene.** `pip freeze` equals §1's copy; `pip check` clean. The 42 rehearsal mtimes
   unchanged. The siblings at their §1 `HEAD`s, clean (a sibling moved by another session's commit
   is recorded, not a finding, if nothing in it is the agent's). No Ray process, no lock, the eight
   ports free. No scratch file in the repository; `/tmp/ray-cpbh*` may remain (Ray's session dirs,
   not removed by a stop, as SI found).
10. **E6, the records.** The board's §1 row (✅, "this commit", the log), C1–C3, and
    `### 5.9 After prompt 05a (<date>)` with the actor line, where `ray_current_cluster` went, the
    two process sets, the timings and the rollback (`git revert` alone; nothing in the venv). The
    log follows README §5.1, with every command and its output, and the breakages as diffs.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 05a* paragraph in §5.9: what each
  check found, and anything handed on to 06. Fix small residue in a follow-up commit of its own,
  also by pathspec:
  - "this commit" → the SHA (board and log);
  - this note added to the board's list of orchestrator notes, marked "used for 05a";
  - the status line: 6 landed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **A difference from SI's script that SI would want** (change 3's four-port check, and change 4)
  is SI's to take up (README §6.6). It is reported to the user, not written into SI.
- **Report to the user:**
  - the script in one line, and its diff from SI's (four changes, and any deviation);
  - the start and stop timings, and the two process sets' sizes;
  - C2's five facts, and anything that differs from SI's 2.55.1;
  - the breakages, and change 4's refusal;
  - the suites;
  - the user's staged `phiref_iter.py`, left as found;
  - that 06 can be dispatched, under the lock.
- **Then 06** (R1): its note is written after this review.
