# Prompt 04 — rehearse registered runs

**Campaign:** [`README.md`](README.md) · **Board items:** **H1**–**H6** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-19, after the user's rulings (README §6, U5: the hard-kill route rehearsed). It
follows `dk-and-rpk-migration`'s rehearsal protocol (that README §4.1) for the grid and the
resume, with every build launched through the wrapper. Its orchestrator note, written after 03's
review, governs where it differs from this file.
**Gate:**
- 01, 02 and 03 landed and reviewed, with the gates "Pin", "Fingerprint" and "Wrapper" holding
  (board §2);
- on branch `rrk-adoption`, no tracked change in `git status`;
- `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh status`: the lock free, no ChamPBH
  process, the four ports free;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- neither `var/datastores/rrk-adoption-04-reference.db` nor `var/datastores/rrk-adoption-04-resumed.db`
  exists, and nor does `var/rrk-adoption-04/`.

If any of these fails, stop and ask.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Opus**. Four real builds under real Ray, two of them killed on purpose,
and the registry's records read against what happened.

**Read first:**

1. [`README.md`](README.md): §0.2, §2.2 (04's charter), §3, §5 (rules 15 and 16, the one-off
   exception for this prompt, and "No agent polls a run"), §6 (U5, P4–P6).
2. `CLAUDE.md`: the section "Long-running jobs — the run registry" (01, 03), "Ray on this
   machine", and the test commands.
3. `prompts/dk-and-rpk-migration/README.md` §4.1: the pilot grid, the `reference` / `resumed-a` /
   `resumed-b` protocol, L1 and L2; and §4.2–§4.3, what `compare_stores` may report between a
   store and its resume.
4. `.documents/read-only-plots-verification.md` §1 (03 of that campaign): the last rehearsal, its
   times, and the `histories.csv` hash. Its commands are the model for yours.
5. The rehearsal tools, run **unchanged**: `ray_cluster.sh` (`start`, `stop`, `status`),
   `interrupt_when.py`, `stage_counts.py` (`.documents/dk-and-rpk-migration/rehearsal/`). Read each
   one's `--help`. `build.sh` is **not** used: it runs `main.py` directly, and this prompt's builds
   go through the wrapper.
6. `RunDrivers/registered_run.py`'s docstring and log 03: the usage, the heartbeat, the marker, the
   refusals and the longest silence 03 expects.
7. RunRegistryKit's `README.md` at `v0.2.0`: "Ending a run whose end was never recorded", and the
   `store` commands. Its open issue `[04-a-killed-process-not-yet-reaped-answers-kill-0]`: a killed
   process its parent has not reaped still answers `kill -0`, and `end` refuses it.

---

## 1. What is wanted

The proof that 03's tests cannot give: under real Ray, on ChamPBH's isolated cluster, the wrapper
records a build from its first second to its end:
- a clean build, `done`;
- a build interrupted with `SIGTERM`, `killed`;
- a build killed outright with `SIGKILL`, which stays `running` until a person ends it with `end`,
  and whose store refuses a resume until then;
- a resume after both interruptions, `done`, equal to the clean build.

And the store layer acts on a real store: `store create` after the first close, the fingerprint
recorded at finish, `store fingerprint` and `store show`.

This prompt is a one-off exception to `CLAUDE.md`'s "no campaign prompt starts" a `main.py` run with
a datastore (README §5). It is four builds of about 3½ min each at the load the last rehearsal saw,
one plotting run, and no PRyMordial solve outside the builds.

### 1.1 The protocol

Throughout:
- `ADDRESS=127.0.0.1:6380`, `S=var/datastores` and `L=var/rrk-adoption-04` (the launch logs and
  pid files). Both are under the repository root and ignored by git (R4).
- Every build is the pilot grid, given to the wrapper after `--`:

  ```
  --config exponential.yaml --database $S/rrk-adoption-04-<store>.db --shards 16 \
  --job-name rrk-adoption-04-<slug> --M-values-Mp 1e-3 --beta-values 1.1 1.15 1.2 10 25 \
  --ray-address $ADDRESS
  ```

  The resumes also get `--prune-unvalidated`. The wrapper's own flags are `--campaign rrk-adoption
  --prompt 04 --register <slug> --purpose "<one line>"`.
- **Every build is launched detached** (rule 3). The launch is through `sh`, so that `sh` reaps
  the driver when it dies; then `end` is not refused by an unreaped process (the open issue above):

  ```
  nohup sh -c 'PYTHONPATH=. ./venv/bin/python -u RunDrivers/registered_run.py <flags> -- <args> \
      > $L/<slug>.log 2>&1 & echo $! > $L/<slug>.pid; wait $!; echo "** launcher saw exit status $?" >> $L/<slug>.log' \
      > /dev/null 2>&1 &
  ```

  Adapt the quoting as the shell needs, and record the command exactly as run.
- **One check after each launch** (rule 4): the manifest exists, `status.json` says `running` with
  the pid in `$L/<slug>.pid`, and `list` shows the run. Then wait on the build by a `Monitor` whose
  condition is the log's `** launcher saw exit status` line, never a `sleep` loop and never a
  `pgrep`.
- Every driver gets `--ray-address $ADDRESS`, and every `ray` CLI call `--address $ADDRESS`. **No
  `ray stop`, ever.**

**Steps:**
1. **The suites**, before `start`, serially, never with a cluster up: the counts 03 handed on.
2. `ray_cluster.sh start "rrk-adoption 04 (R)"`. If the lock is held, it exits 3: **stop and ask**;
   never wait on it, never break it. Record `start`'s output and `ray status --address $ADDRESS`.
   `mkdir -p $L`.
3. **H1 — `reference`**, into `$S/rrk-adoption-04-reference.db`, straight through.
   - The log ends `** driver exit status 0`. `stage_counts.py` gives `compute` 5 / 5 / 5.
   - `list` shows it `finished`, `done`.
   - Its `status.json` has a `fingerprint`, and `fingerprint_sidecar` saying "not written", since
     there is no sidecar yet (§0.2 item 6).
   - Its manifest: `script` is `main.py` with `main.py`'s SHA-256 (`shasum -a 256 main.py`);
     `results` is the store; `results_store_id` is `null`; `packages`, `host`, `git_head`,
     `git_dirty`, `scope` and `heartbeat_means` are as 03 wrote them.
   - Then `store create $S/rrk-adoption-04-reference.db --purpose "…"`, `store fingerprint …
     --write`, and `store show …`. The fingerprint `store fingerprint` reads equals the one in the
     run's status.
4. **H2 — `resumed-a`**, into `$S/rrk-adoption-04-resumed.db`, interrupted with `SIGTERM`:
   `interrupt_when.py $L/resumed-a.log $L/resumed-a.pid --banner "CALCULATE SCALAR FIELD HISTORIES
   FOR SAMPLE GRID" --after <s>`. Choose `<s>` from H1's scalar-stage time, so that some histories
   are stored and some are not, and say how you chose it.
   - The log has the wrapper's `** SIGTERM at …: recorded killed` line, then `** driver exit status
     143`, then `** launcher saw exit status 143`.
   - The status is `killed`/143, with the fingerprint. The time from the signal (by
     `interrupt_when.py`'s line) to the `killed` record (`status.json`'s heartbeat, or the
     handler's line) is measured.
   - No `** cancelled an alarm` line. If one appears, Ray's own handler ran: record it. It is the
     measurement P5 asks for.
   - Then `store create` on the closed store.
5. **H3 — `resumed-b`**, on the same store with `--prune-unvalidated`, killed with **`SIGKILL`**:
   - once its log shows the scalar banner and a delay has passed while work remains, `kill -KILL
     $(cat $L/resumed-b.pid)`, by pid alone. A progress line cannot be waited for: `main.py`'s
     queues print one at most every 300 s, and the pilot's scalar stage is shorter than that.
     Choose the delay from `resumed-a`'s log (how many histories it stored) and H1's stage time,
     and say how you chose it;
   - the log ends with `** launcher saw exit status 137` and no `** driver exit status`;
   - `list` shows it `stale`, `!!`, its pid not answering. Record the line and the stderr summary;
   - `ray_cluster.sh status`: any ChamPBH process the kill left behind, by pid. A driver attached
     to an external cluster should leave none (README §0.2); if one is left, record it and **wait,
     by pid, for it to exit** before step 6. Ending it is a person's: stop and ask if it is still
     there ten minutes after the kill;
   - **the refusal**: launch `resumed-c` exactly as step 6 will. The wrapper refuses it: exit 2, one
     `!! ` line naming `resumed-b`'s run and `python -m runregistrykit end`. Nothing new appears
     under `var/runs/`;
   - **`end`**: `./venv/bin/python -m runregistrykit end <resumed-b's run id> --reason "SIGKILLed on
     purpose by rrk-adoption 04 to rehearse the hard-kill route"`. It is accepted, and `list` shows
     it `finished`, `lost`. The status's `ended` record is quoted in the log.
6. **H4 — `resumed-c`**, on the same store with `--prune-unvalidated`, straight through:
   - `done`/0, every stage's "ALL WORK ITEMS COMPLETE";
   - `fingerprint_sidecar` "written", and the sidecar's `fingerprint` equals the status's;
   - `stage_counts.py`: the resume computed only what was missing.
7. **H5 — the stores and the readers.**
   - `PYTHONPATH=. ./venv/bin/python tools/compare_stores.py $S/rrk-adoption-04-reference.db
     $S/rrk-adoption-04-resumed.db`: equal, or different only where `dk-and-rpk-migration` §4.2–§4.3
     say a resume may be (bookkeeping columns).
   - `store fingerprint` on each: the two compare as `compare_stores` says.
   - `plot_by_beta.py` on the reference store, **unregistered**, with the build's options as
     `run_readers.sh:115-116` gives them (`--config exponential.yaml --database
     $S/rrk-adoption-04-reference.db --shards 16 --phi-init-Mp 5 --output $L/plot-reference
     --ray-address $ADDRESS`): `histories.csv`'s SHA-256 is R0's,
     `d7c62c37d8ad637a63854583ab11bc737109dcbfba92168dbddbaea75182c522`.
   - `store show` on each store: the runs that name it, and their states.
8. `ray_cluster.sh stop`, as a command on its own; then `status`, which must show the lock free and
   no ChamPBH process.
9. **The suites** again, after `stop`.

### 1.2 The items

- **H1–H4** as steps 3–6 say, each with its run id, its log's last lines, its `status.json`
  (less the fingerprint's per-class detail) and its `list` line.
- **H5** as step 7 says.
- **H6 — the record.** A new `.documents/rrk-adoption-verification.md`, §1:
  - the cluster and the load;
  - the four builds, as a table: run id, slug, store, pid, state, exit code, wall time, and the
    fingerprint's time at finish (from the status's last heartbeat before and after it, or the
    wrapper's lines);
  - the heartbeat: its age and stage as `list` showed them at each build's one check, and the
    final status's last beat against the build's end, set against the longest silence 03
    expected. `status.json` keeps only the latest beat, so no history of beats is claimed;
  - the signal's measurements (step 4) and what they say about P5;
  - the hard-kill route, line by line;
  - the stores compared, the fingerprints and `histories.csv`;
  - `git_dirty` as recorded, and why: the user's untracked files at the root (README §7, R6);
  - the commands, verbatim.

  Small text outputs go under `.documents/rrk-adoption/out/04-*`: the logs, the `list` and `store
  show` outputs, the manifests and statuses. A file over about 1 MB is a finding to report before
  committing.

---

## 2. Acceptance

1. H1–H6 as §1.2 says.
2. The cluster was started and stopped by the script alone, under the lock. The log shows `start`'s
   output, `ray status`, and `stop`'s exit status with its timing. Nothing is left running.
3. **Nothing outside `var/` and `.documents/`** is written, and no existing store is opened: not
   the user's pilot store, not `~/ChamPBH-stores*`, not `~/ScienceData/ChamPBH-rehearsals/`. The
   venv is unchanged (`pip freeze` before and after).
4. `main.py` is byte-identical (`git diff 3cd2be4 -- main.py` empty).
5. **One commit**, with the log `logs/04-rehearse-registered-runs.md`. The commit holds:
   - the verification document;
   - `.documents/rrk-adoption/out/04-*`;
   - the board: row 04, H1–H6, the gate "R", and a `### 5.5 After prompt 04 (<date>)`.

   No code, no test, no script. Write "this commit" where the SHA goes. **`var/` is not
   committed**; the stores stay there for the user (README §3).

## 3. Stop conditions: stop and ask (after `ray_cluster.sh stop`, unless the stop is what failed)

- The lock is held, or the cluster will not start.
- A build fails other than by the signal it was sent, or `reference` differs from R0's
  `histories.csv`.
- A record is wrong:
  - a state, an exit code or a fingerprint is not what §1.1 says;
  - a manifest is written after its driver's first line;
  - a `SIGTERM`ed run is not recorded `killed`;
  - the refusal in step 5 is not made, or creates anything.
- `end` refuses `resumed-b` although its pid does not answer, or accepts it while it does.
- A Ray process outlives the kill by more than ten minutes.
- The resumed store differs from the reference beyond what `dk-and-rpk-migration` §4.2–§4.3 allow.
- The wrapper needs a change. That is a lettered prompt, 03b (README §4), never an edit here.

## 4. What this prompt does not do

- It changes no code, test or script.
- It registers no plotting run, and runs no `--inventory` through the wrapper.
- It keeps its stores and runs under `var/`. Removing them is the user's (README §3, §7).

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`.
