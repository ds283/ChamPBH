# Prompt 02 — rehearse registered runs under Ray 2.59

**Campaign:** [`README.md`](README.md) · **Board items:** **R1**–**R5**, **W1** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-25, with the plan, at `a144c83`. **Held** until 01 has landed and been
reviewed (README §2). Its orchestrator note, written after 01's review, governs where it differs
from this file; it fills in 01's commit and counts.
**Where it runs:** on the laptop, in the main checkout, on branch `ray-sqla-align`. The checkout and
its venv are shared with `jordan-normalization`, which must be idle while 02 runs.
**Gate:**
- 01 landed and reviewed, with gate V holding (board §2);
- `git status` clean, with no untracked file; `git diff <01's commit> HEAD -- '*.py' '*.sh'
  requirements.txt runregistrykit.toml` empty, so the code the builds run is 01's;
- the venv holds `ray` 2.59.0 and `SQLAlchemy` 2.0.55, its freeze equal to the last in 01's
  `out/01-pip.txt`, `pip check` clean;
- `ray_cluster.sh status`: the lock free, no ChamPBH process, the four ports free;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- none of `var/datastores/ray-sqla-align-02-reference.db`, `var/datastores/ray-sqla-align-02-resumed.db`
  (or their shards and sidecars) and `var/ray-sqla-align-02/` exists;
- `~/.ray/auth_token`: 64 bytes, mode 600, the mtime 01 recorded; `env | grep -c RAY_AUTH_MODE` 0.

If any of these fails, stop and ask.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Opus**. Three real builds under real Ray, one stopped on purpose, and the
registry's records read against what happened.

**Read first:**

1. [`README.md`](README.md): §0.2, §0.4 (the reference digests), §2.2 (the gates), §3, §5, §7.
2. `CLAUDE.md`: "Ray on this machine", "The rehearsal lock", "Ray's token mode" (01's), and
   "Long-running jobs — the run registry".
3. `prompts/rrk-adoption/04-rehearse-registered-runs.md` §1.1 (the protocol, the pilot grid, the
   detached launch through `sh`) and `.documents/rrk-adoption-verification.md` §1: the rehearsal this
   prompt repeats, its commands, its times and its outputs, committed as
   `.documents/rrk-adoption/out/04-*`. Its helpers are there as `.txt` copies
   (`04-wait_for_line.py.txt`, `04-compute_times.py.txt`, `04-ray_state.py.txt`): copy the ones you
   use to a scratch directory under `var/ray-sqla-align-02/`, unchanged, and say so.
4. 01's log and `.documents/ray-sqla-align/out/01-v1.txt`: V1, the token checks this prompt repeats
   at build scale.
5. The rehearsal tools, run **unchanged**: `ray_cluster.sh`, `interrupt_when.py`, `stage_counts.py`
   (`.documents/dk-and-rpk-migration/rehearsal/`); each one's `--help`. `build.sh` is not used.
6. StochasticInstantons' results note, `.documents/ray-sqla-align/01-move-ray-and-sqlalchemy-with-token-mode.md`
   (read only), "R" and corrections 4 and 12: the same checks on SI's cluster.

---

## 1. What is wanted

The proof that 01's tests and V1 cannot give: **under `ray` 2.59.0 and `SQLAlchemy` 2.0.55, a
registered ChamPBH build stores exactly what it stored under 2.53.0 and 2.0.46, through every path
`rrk-adoption` 04 exercised but the hard kill, with every Ray process in token mode.**
- `reference`: a clean build, `done`;
- `resumed-a`: a build stopped with `SIGTERM`, `killed`/143;
- `resumed-c`: its resume, `done`, computing only what was missing;
- `plot_by_beta.py` on the reference store, read-only.

Both stores must fingerprint to `2dd4b39b…932f` and `histories.csv` must be `d7c62c37…c522`
(README §0.4). The commit holds records only.

This prompt is named in README §5, so it is within `CLAUDE.md`'s "what stays": three pilot builds
of about 3–6 min each, at the load `rrk-adoption` 04 saw, and one plotting run.

### 1.1 The protocol

`rrk-adoption` 04's, without its `resumed-b`. Throughout:
- `ADDRESS=127.0.0.1:6380`, `S=var/datastores`, `L=var/ray-sqla-align-02`.
- Every build is the pilot grid, given to the wrapper after `--`:

  ```
  --config exponential.yaml --database $S/ray-sqla-align-02-<store>.db --shards 16 \
  --job-name ray-sqla-align-02-<slug> --M-values-Mp 1e-3 --beta-values 1.1 1.15 1.2 10 25 \
  --ray-address $ADDRESS
  ```

  `resumed-c` also gets `--prune-unvalidated`. The wrapper's flags are `--campaign ray-sqla-align
  --prompt 02 --register <slug> --purpose "<one line>"`.
- **Every build is launched detached** through `sh`, as 04's §1.1 gives it, with its pid file and
  the launcher's `** launcher saw exit status` line. Record each command exactly as run.
- **One check after each launch** (rule 4): the manifest exists, `status.json` says `running` with
  the pid in `$L/<slug>.pid`, and `list` shows the run. Then wait for the launcher's line by **one
  foreground call** of 04's `wait_for_line.py` with a timeout; never a `sleep` loop, never `pgrep`,
  never `tail -f`. A timeout is a stop, not a retry.
- **Each lock or script command is a shell command of its own**, its output redirected to a file,
  never piped, and no shell command that runs the script or a lock command names `/tmp/ray-cpbh`.
  The builds, the waits, `ray status` and the `ps` checks run in calls of their own. **No `ray
  stop`.**
- **Every Ray CLI command** against the cluster carries `RAY_AUTH_MODE=token` in its environment.
- **The token is never printed**: `ls -l`/`stat` only, the `ps` checks through `grep -o`, and the
  final `grep -F -l -f`.

## 2. What to do

1. **The suites**, before `start`, serially, never with a cluster up: the counts 01 handed on, into
   `$L/02-suites.txt`. `uptime` before.
2. **`start`.** `ray_cluster.sh start "ray-sqla-align 02 (R)" > $L/start.txt 2>&1`; `echo $?`.
   Exit 3: **stop and ask**. Then, in calls of their own: `RAY_AUTH_MODE=token ./venv/bin/ray status
   --address $ADDRESS`; the `gcs_server`'s and the `raylet`'s `RAY_AUTH_MODE` by 01's §2.9 step 3,
   with its `PATH=` control. `start.txt` holds no `Token authentication` line.
3. **`reference`.** Launch; check; wait. Expected `done`/0. Then, as 04's H1: `store create
   $S/ray-sqla-align-02-reference.db --purpose "…"`, `store fingerprint … --write`, `store show`.
   The status's fingerprint, the sidecar's and `store fingerprint`'s must be one digest,
   `2dd4b39b…932f`.
4. **`resumed-a`.** Launch; check; then `interrupt_when.py` on its log and pid file, on the banner
   `CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID`, with the delay the orchestrator's note sets
   from `reference`'s logged `compute_time`s (04 used 150 s, in the β = 10 / β = 25 gap, so that the
   store is partial). Wait. Expected `killed`/143, the wrapper's handler line in the same second as
   `interrupt_when`'s `sent SIGTERM`, the marker 143 and the launcher's 143. Then `store create` on
   the resumed store. Record how many histories it holds.
5. **`resumed-c`.** Launch with `--prune-unvalidated`; check; wait. Expected `done`/0, computing only
   what `resumed-a` left; `fingerprint_sidecar` `written`; the status's, the sidecar's and `store
   fingerprint`'s digest one, `2dd4b39b…932f`. `results_store_id` the resumed store's.
6. **The plot.** `PYTHONPATH=. ./venv/bin/python -u plot_by_beta.py --config exponential.yaml
   --database $S/ray-sqla-align-02-reference.db --shards 16 --phi-init-Mp 5 --output
   $L/plot-reference --ray-address $ADDRESS > $L/plot-reference.log 2>&1`, in the foreground,
   unregistered, as 04's was. Exit 0; `shasum -a 256 $L/plot-reference/QCD_Cosmology/histories.csv`
   is `d7c62c37…c522`; the reference store's files unchanged (`shasum` of each before and after).
7. **`stop`**, then `status`: exit 0; the lock free, no ChamPBH process, the ports free.
8. **The comparisons.** `tools/compare_stores.py` on the two stores: `RESULT: equal` but for
   bookkeeping. `stage_counts.py` on the three launch logs: `reference` 5 / 5 / 5 / 5. The
   manifests: `git_head` the `HEAD` of the Gate, `git_dirty` false, `script_sha256` that of 01's
   `main.py`, `packages` naming seven distributions with `ray` 2.59.0, `SQLAlchemy` 2.0.55, numpy
   2.3.5 and scipy 1.17.0, the three git packages at their commits. Each launch log's `>> packages:`
   line names the six.
9. **Token.** `grep -c` for `Token authentication is enabled`, `Generated new authentication token`,
   `InvalidAuthToken` and `AuthenticationError` in `start.txt`, the three launch logs and the plot
   log: all 0. `ls -l` and `stat` of the token file: unchanged.
10. **What is new in the logs.** Each launch log against 04's at the same slug
    (`.documents/rrk-adoption/out/04-<slug>.log`), and the plot log against `04-plot-reference.log`:
    every line kind that 04's do not have, and every kind 04's have that these lose, recorded and
    explained. The `>> packages:` line's four new entries are expected.
11. **The suites again**, no cluster up: the same counts. `list`: the three runs, finished.
12. **The records.** Copy to `.documents/ray-sqla-align/out/02-*`: the small text outputs, the
    three manifests and statuses, both sidecars, `compare_stores`'s output, the stage counts and
    the launch logs (as 04's were). Then `grep -r -F -l -f ~/.ray/auth_token
    .documents/ray-sqla-align/ prompts/ray-sqla-align/` prints nothing (exit 1). Write
    `.documents/ray-sqla-align-verification.md` §1 (the rehearsal, as `.documents/rrk-adoption-verification.md`
    §1 is laid out), the log and the board (§6).

## 3. Acceptance

1. **FP**: both stores `2dd4b39b…932f`; `compare_stores` equal; `histories.csv` `d7c62c37…c522`.
2. **Token**: §2.2 and §2.9 as expected; the token unchanged and in no record.
3. **The runs**: `reference` `done`/0, `resumed-a` `killed`/143, `resumed-c` `done`/0, each as
   `list`, `status.json` and the launch log agree; the manifests as §2.8.
4. **The suites** at 01's counts before and after.
5. **Nothing else moved**: the reference store unchanged by the plot; nothing under `var/` but this
   prompt's runs, stores and `$L`; the cluster stopped, the lock free.
6. `git diff --cached --name-only` is: `.documents/ray-sqla-align-verification.md`,
   `.documents/ray-sqla-align/out/02-*`, the log and the board. Nothing else.

## 4. Stop conditions — stop and ask

- A Gate item is not as stated.
- `start` exits 3 or 4; a Ray process lacks `RAY_AUTH_MODE=token`; any log holds a token warning or
  an authentication error.
- A build ends in a state or exit code other than §2's; a wait times out.
- **A fingerprint is not `2dd4b39b…932f`, `compare_stores` finds a difference beyond bookkeeping,
  or `histories.csv` differs.** Change nothing; record both digests, `compare_stores`'s output and
  the logs. The bisection is the user's (README §2.2).
- The plot changes a file of the reference store.
- `stop` fails, leaves the lock held, or leaves a process of `/tmp/ray-cpbh`.
- The token file changes, or appears in a record.
- A suite count differs, beyond a known wall-clock test that passes alone.
- Anything suggests a defect in a package: record it on the board and in
  `.documents/OPEN_ISSUES.md`, and stop.

**If you stop with the cluster up or the lock held**, say so and leave them, with `status`'s output.
A run left `running` is reported with its pid and heartbeat, never ended by hand.

## 5. What this prompt does not do

- It changes no code and no test; it touches no other repository.
- It does not `SIGKILL` a build (README §1).
- It does not retire its stores: that is the user's, after the review (README §3).
- It does not merge or push.

## 6. The log and the board

**The log**, at `logs/02-rehearse-registered-runs-under-ray-2.59.md`, in README §5.1's form: the
runs' table (run id, slug, store, pid, state, exit, launched → ended, wall, `results_store_id`,
fingerprint at finish), with `uptime` at each launch; the token checks; "What is new in the logs".

**The board:** the header; the §1 row; items R1–R5 and W1; gates FP and Token;
`### 5.3 After prompt 02 (<date>)`. `.documents/OPEN_ISSUES.md` only if §3 gains an entry.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`. Nothing pushed.
