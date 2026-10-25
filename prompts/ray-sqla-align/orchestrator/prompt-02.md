# Orchestrator — prompt 02, rehearse registered runs under Ray 2.59

Read [`../README.md`](../README.md) first: §0.2 (the mechanism), §0.4 (the reference digests), §2.2
(gates FP and Token), §3, §4.2, §5 (the rules) and §7. Then the prompt, the board, 01's log and
note 01 ([`prompt-01.md`](prompt-01.md)), whose conventions hold here where this note does not
restate them. `rrk-adoption`'s note for its 04 (`prompts/rrk-adoption/orchestrator/prompt-04.md`)
ruled the rehearsal this prompt repeats; where its rulings still apply they are restated in §0, so
that the agent need not read it.

**You do not write code.** You may:
- run the suites, `compare_stores.py`, `stage_counts.py`, 04's `compute_times.py` and the registry's
  read-only commands (`list`, `store show`, `store fingerprint` without `--write`) on 02's own stores
  and runs; `ray_cluster.sh status` and the lock's `status`, each alone in its Bash call;
- read the agent's outputs under `var/ray-sqla-align-02/`, `var/runs/` and
  `.documents/ray-sqla-align/out/`;
- run a probe from the session scratchpad (`orch02/`), never committing one, and never starting Ray;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`02-rehearse-registered-runs-under-ray-2.59.md`](../02-rehearse-registered-runs-under-ray-2.59.md)
· **Board items:** R1–R5, W1 · **Gates:** **FP**, and **Token**'s 02 part · **Closes:** nothing ·
**Opens:** only what the work finds · **Model:** Opus, as the prompt recommends · **Network:** none.
Nothing is pushed.

**Gate**, checked at `1bb2f6d` on 2026-10-25 at 20:49 GMT, before this note was committed:
- 01 landed (`668d30f`) and reviewed (`1bb2f6d`); gate **V** and **Token**'s V1 part hold (board §2).
  `ray-sqla-align` checked out in the main checkout at `1bb2f6d`; `git status --porcelain
  --untracked-files=all` empty; `git worktree list` shows the main checkout alone. `HEAD` is the
  commit that adds this note, which touches only `orchestrator/`: it is the prompt's **dispatch
  commit**, and every manifest's `git_head` will be it. `jordan-normalization` is idle at `58c1c93`
  and stays idle until 02 is reviewed.
- `git diff 668d30f 1bb2f6d -- '*.py' '*.sh' requirements.txt runregistrykit.toml` empty: `1bb2f6d`
  touched the board and 01's log alone. So the code the builds run is 01's. `main.py`'s SHA-256 is
  **`685b615bc85b3fbfa6ceea30f4a68865f5d97a5ca8e0cf39d980247df1307bbe`**, at `668d30f` and in the tree.
- The venv: `pip freeze`, 102 lines, `cmp`-equal to 01's `var/ray-sqla-align-01/freeze-after.txt`
  (kept as `orch02/pip-freeze-before.txt`); `ray==2.59.0`, `SQLAlchemy==2.0.55`; `pip check`: `No
  broken requirements found.`
- `ray_cluster.sh status`, exit 0: `lock: free (…/rehearsal-lock does not exist)`, no Ray process of
  the cluster, nothing `stop` would signal, ports 6380, 6381, 8267 and 52367 free.
- `./venv/bin/python -m runregistrykit list`: `no runs under …/var/runs`.
- `var/` holds `datastores/`, `ray-sqla-align-01/` and `runs/` (empty). `var/datastores/` holds two
  sidecars and nothing else: `rrk-adoption-04-reference.manifest.json` and
  `rrk-adoption-04-resumed.manifest.json`, left when 04's stores were dropped. They are not 02's, are
  not a Gate failure, and are not opened, changed or removed. None of 02's store paths and no
  `var/ray-sqla-align-02/` exists.
- `ls -l ~/.ray/auth_token`: `-rw------- 1 ds283 staff 64 Oct 25 12:51`; `/usr/bin/stat -f '%m %Sm'`:
  `1792932677 Oct 25 12:51:17 2026`. `env | grep -c RAY_AUTH_MODE`: 0.
- The one-minute load was 4.56 (4.56 / 4.89 / 6.76). Other workloads share the machine.

## 0. What makes this prompt unusual

**It is the campaign's only proof that Ray 2.59 and SQLAlchemy 2.0.55 store what Ray 2.53 and
SQLAlchemy 2.0.46 stored**, and the comparison is with digests alone: `rrk-adoption` 04's stores were
dropped (README §0.4). It runs three real builds and one plot, stops one build on purpose, and
commits nothing but records. A mistake here is not a wrong value but a wrong claim in the
verification document, so **every claim in it is a line the agent saw, quoted, with the command
that produced it.**

**Corrections and rulings**, each recorded in the log as a deviation of the class given.

1. **`stat` and `timeout` are GNU's in this shell** (`STRUCTURALLY REQUIRED`; note 01, ruling 1).
   Every `stat` of the token file is **`/usr/bin/stat -f '%m %Sm' ~/.ray/auth_token`**, expected
   `1792932677 Oct 25 12:51:17 2026`. `timeout` is GNU coreutils'.

2. **The helpers** (`IMPLEMENTATION CHOICE`, ruled). Copy, unchanged, `.documents/rrk-adoption/out/`
   `04-wait_for_line.py.txt`, `04-compute_times.py.txt` and `04-ray_state.py.txt` to
   `var/ray-sqla-align-02/tools/` as `wait_for_line.py`, `compute_times.py` and `ray_state.py`, and
   confirm each by `cmp` against its source. `ray_state.py` runs `./venv/bin/ray list` in a
   subprocess, which inherits its environment, so it always runs as **`RAY_AUTH_MODE=token
   ./venv/bin/python var/ray-sqla-align-02/tools/ray_state.py`**. Write nothing else; if a helper is
   needed beyond these, it goes in `var/ray-sqla-align-02/tools/` and is copied to `out/` as `.txt`.

3. **The waits** (`IMPLEMENTATION CHOICE`, ruled; it reads the prompt's "a timeout is a stop"
   otherwise). Each wait is **one foreground call** of `wait_for_line.py`, the Bash tool's timeout at
   600 000 ms:
   - for the registration line, `"** registered_run: registered as"`, `--timeout 120`;
   - for the end, `"** launcher saw exit status"`, `--timeout 570`.

   A build of 04's took up to 5 min 40 s at a load of 68. Other workloads may load the machine
   meanwhile, so **an end wait that times out may be repeated once**, and the log says so. A
   registration wait that times out, or a second end-wait time-out, is the prompt's stop. No `sleep`
   loop, no `pgrep`, no `tail -f`.

4. **The one check after each launch** (`IMPLEMENTATION CHOICE`; 04's ruling 4). After the
   registration wait, in a **single** Bash call: the manifest's path from the log's `**   manifest`
   line and that the file exists; `status.json`'s `state` and `pid` against `$L/<slug>.pid`; the
   manifest's `created` against the log's birth time (`/usr/bin/stat -f %SB`); `list`'s line for the
   run; and the log's first four lines, to show the three `** registered_run:` lines come before
   `>> packages:`.

5. **The launch line** (`IMPLEMENTATION CHOICE`; 04's ruling 5). Each launch is one Bash call that
   sets and **exports** `ADDRESS=127.0.0.1:6380`, `S=$PWD/var/datastores` and
   `L=$PWD/var/ray-sqla-align-02` — **absolute**, not the prompt's relative `S` — and runs 04's
   `nohup sh -c '…'` form (`.documents/rrk-adoption-verification.md` §1.12) with the slug spelt out,
   ray-sqla-align's campaign, prompt and job names, and the pid file and launcher line. Record each
   exactly as run, the exports included. `interrupt_when.py` is launched the same way, with `nohup`,
   in its own Bash call right after `resumed-a`'s.

6. **The delay for `resumed-a`** (`IMPLEMENTATION CHOICE`, ruled; the prompt leaves it to this
   note). After `reference`'s `store create` and `store fingerprint --write`, read its five
   `compute_time`s with `compute_times.py` (`sqlite3`, `file:…?mode=ro` URIs only). Write `t10` and
   `t25` for β = 10's and β = 25's. Each history completes at about its `compute_time` plus 20 s after
   the scalar banner (04's offsets were +17 to +31 s). **Set `<s>` to the middle of the gap between
   `t10 + 20` and `t25 + 20`, rounded to 10 s**, which is `(t10 + t25)/2 + 20`; at 04's times that
   gives 140 s against 04's 150 s. If the gap is under 60 s, stop and ask. Expected: β = 1.1, 1.15 and
   1.2 stored and validated, β = 10 stored (validated or not: 04's was stored and left unvalidated),
   β = 25 not stored. Then run `compute_times.py` on the resumed store after `resumed-a` and record
   what it holds. **A different outcome is recorded, not a stop**, so long as `resumed-a` is
   `killed`/143: the resume is exercised either way. A `resumed-a` that reaches `done` is the
   prompt's stop.

7. **The signal's timing** (`NOTED`; 04's ruling 7). `interrupt_when.py` prints UTC to the second,
   the wrapper's handler line local time to the second with its offset. The prompt's "in the same
   second" is checked to that resolution: **0 or 1 s between `sent SIGTERM` and the handler's line
   is the expectation met** (SI's was about 1 s). Also give the time from the signal to the final
   `status.json` heartbeat. Expected lines, after the scalar banner: the handler's `** SIGTERM at …:
   recorded killed, exit code 143; …`, then `** SIGTERM: main.py ended with SystemExit(1); recording
   killed, exit code 143` (04's and SI's; `SystemExit(1)` is the known shape, not a fault), the marker
   `** driver exit status 143`, the launcher's 143. A `*** SIGTERM received` banner or a `**
   cancelled an alarm` line would be **new under Ray 2.59**: recorded and explained, and a stop only
   if the run is not `killed`/143.

8. **`results_store_id`** (`STRUCTURALLY REQUIRED`; 04's ruling 11). `reference` and `resumed-a`
   begin with no sidecar: `null`. `resumed-c` begins after step 4's `store create`: the resumed
   store's `store_id`, as `store create` printed it. Any other value is a stop.

9. **`resumed-c`'s open and counts** (`NOTED`). If `resumed-a` left β = 10 stored and unvalidated,
   `--prune-unvalidated` prints an `INTEGRITY WARNING` naming it and `** Note: these values have been
   pruned from the datastore.`, and the scalar stage computes 2 (β = 10 and 25), as 04's `resumed-c`
   did (5 / 2 / 2 / 2). If β = 10 was validated, it computes 1. The adiabatic and BBN stages compute
   5 / 5 / 5 / 5 either way. Quote the prune report, or say there was none.

10. **The cluster** (`STRUCTURALLY REQUIRED`; note 01, rulings 5 and 6, restated).
    - **`ray_cluster.sh start`, `stop` and `status`, and the lock module's `status`, are each the
      only process of their Bash call**, output redirected to a file under `$L` (`> F 2>&1`), never
      piped, the exit taken with the builtin `echo $?` in the same call, the file read in a later
      call.
    - **No command line of the agent's names `/tmp/ray-cpbh`**, from `start` to the end of `stop`:
      no `cat`, `grep`, `ls`, `lsof`, `pgrep -f` or `ps … | grep` naming it. The `gcs_server`'s pid
      comes from `start.txt`, the `raylet`'s from a `status` listing saved to a file.
    - `ps -E -ww -o command= -p <pid> | grep -o 'RAY_AUTH_MODE=[a-z]*'` and `… | grep -c 'PATH='`
      only, for each of the two pids; **never an environment printed whole**. Expected
      `RAY_AUTH_MODE=token` and 1 for each (01's V1). A control of 1 without `RAY_AUTH_MODE=token`
      is the prompt's stop; a control of 0 is recorded, and ruled at review.
    - The builds, the waits, `interrupt_when.py`, `ray status`, `ray_state.py` and the `ps` checks
      each run in Bash calls of their own, never with a script or lock command. **`stop` runs only
      once every driver's pid has answered `kill -0` with `no such process`** (by pid, from the pid
      files; liveness rule 2) and `interrupt_when.py` has printed its `sent` line.
    - `start` exits 3 if the lock is held or its old location exists, 4 if a port is held or a
      process of the cluster is alive: **either is a stop and ask**, never a wait, never a retry,
      never a removal. SGK's and SI's sessions may start a cluster at any time.
    - **Every Ray CLI command carries `RAY_AUTH_MODE=token`.** **No `ray stop`, ever.**
    - `stop` needed `SIGKILL` after its grace for the `gcs_server` (and, at 01's V1, for one `Python
      -u` of the venv's Ray) under both Ray versions. That is not a stop, so long as `stop` exits 0,
      confirms no process and the ports free, and releases the lock, and `status` then says so.

11. **The Ray state** (`IMPLEMENTATION CHOICE`, ruled). `ray_state.py` (ruling 2) at three points:
    after `start` (the baseline), after `resumed-a`'s launcher line, and after the plot, before
    `stop`. After `resumed-a` no worker or actor of its job is alive, as 04 found under Ray 2.53; if
    one is, wait for it **by pid** with `kill -0` alone, at most ten minutes, and record it. Ray's
    `SUCCEEDED` for a `SIGTERM`ed driver's job is 04's observation, not a fault.

12. **The load** (`NOTED`). Record `uptime` before the suites, before `start`, at each launch, at the
    plot and before `stop`. **If the one-minute load before `start` is over 20, stop and ask**: a BBN
    solve that meets `--bbn-wall-clock-limit` under load in one build and not the other is the named
    cause of a fingerprint difference (`dk-and-rpk-migration` README §4.3). During the builds the
    load is the builds' own (04's peaked at 68) and is recorded, not acted on.

13. **The suites** (`IMPLEMENTATION CHOICE` where the prompt is silent). `CLAUDE.md`'s commands,
    serially, in the foreground, **never with the cluster up**, `ComputeTargets` in its two halves;
    each Bash call at 600 000 ms; output to `$L/suites/<point>-<root>.txt`, a summary (the command,
    `uptime`, `Ran`, the verdict, the exit) into `$L/02-suites.txt`. Expected at both points:
    CosmologyModels 18, ComputeTargets 61 + 115, ObjectFactories 78, tools **102**, RunDrivers 61. A
    failure is re-run alone before it is called real; an existing test that fails is a stop.

14. **The manifests** (`NOTED`, the expected values). `script` `main.py`, `script_sha256`
    `685b615b…7bbe` (`shasum -a 256 main.py`); `git_head` the full SHA of this note's commit,
    `git_dirty` **false** (the tree is clean: `var/` is ignored); `packages` seven: `runregistrykit`
    0.2.0, `datastorekit` 0.4.0 and `raypoolkit` 0.3.0 at the freeze's commits, each `editable:
    false`, and `ray` 2.59.0, `SQLAlchemy` 2.0.55, numpy 2.3.5 and scipy 1.17.0. `results`
    relative to the client root. Each launch log's `>> packages:` line names the six of
    `utilities.RECORDED_DISTRIBUTIONS`, as 01's `out/01-distributions.txt`. **`git_dirty` true is a
    stop**: it means the tree moved under the builds.

15. **The comparisons** (`NOTED`). `compare_stores.py` on the two stores: `RESULT: equal`,
    `problems: none`; the bookkeeping that differs is 04's list (timestamps, labels carrying the job
    name, `ScalarModel.compute_time` and its RHS times, `AdiabaticHistory.compute_time`,
    `BBNData.BBN_compute_time` and `NP_compute_time`). `store fingerprint` **without `--write`** on
    each closed store: `2dd4b39b…932f`, each "matches" its sidecar; the two sidecars' `classes` equal
    as JSON. Give the row counts of 04's §1.8 (`ScalarModel` 5, `ScalarModelValue` 42617, …) as
    `compare_stores` prints them.

16. **"What is new in the logs"** (`IMPLEMENTATION CHOICE`, ruled). Strip ANSI colour codes from 02's
    logs as 04's committed ones were (`sed -E 's/\x1b\[[0-9;]*m//g'`). A line's **kind** is the line
    with every run of digits replaced by `#`, and the run, job and store names replaced by a
    placeholder; compare the sets of kinds, 02's against 04's at the same slug (`resumed-a` with
    `resumed-a`, `resumed-c` with `resumed-c`) and the plot log with `04-plot-reference.log`. List
    every kind in one and not the other, with a count and one example each, and explain it. Expected:
    the `>> packages:` line's four new entries; Ray's `FutureWarning` about
    `RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO` gone (SI found it gone under 2.59); the prune report present
    or not as ruling 9. The helper goes in `var/ray-sqla-align-02/tools/` and to `out/` as `.txt`.

17. **The token grep covers the verification document** (`STRUCTURALLY REQUIRED`). The prompt's
    `grep -r -F -l -f ~/.ray/auth_token .documents/ray-sqla-align/ prompts/ray-sqla-align/` misses
    `.documents/ray-sqla-align-verification.md`, which sits beside the directory. **Run it as
    `grep -r -F -l -f ~/.ray/auth_token .documents/ray-sqla-align/ .documents/ray-sqla-align-verification.md
    prompts/ray-sqla-align/`**, after every record is written. `wc -lc < ~/.ray/auth_token` is `0
    64`, so the pattern is one line (note 01, ruling 7). Exit 1 is the pass; exit 0 lists a file
    (remove that copy, report it, do not commit it); exit 2 is a stop.

18. **The board's section is `### 5.2`** (`STRUCTURALLY REQUIRED`). The prompt's §6 says `### 5.3`;
    the board has `### 5.0` and `### 5.1`, so 02's is **`### 5.2 After prompt 02 (2026-10-25)`** (or
    the date it lands). Header: "**Status: 02 landed, awaiting review — 2 of 2 landed, 1 reviewed.**"
    or words to that effect; the §1 row ✅ with "this commit"; R1–R5 and W1 as found; gate **FP** as
    found; gate **Token** "V1 part ✅ 01; 02's part" as found. `.documents/OPEN_ISSUES.md` only if §3
    gains an entry.

19. **The verification document** (`NOTED`). `.documents/ray-sqla-align-verification.md` is new,
    additive, its header like `rrk-adoption-verification.md`'s (campaign, board, outputs, scripts run
    unchanged), and its §1 laid out as that document's §1, less the hard-kill parts: the tree; the
    result; the cluster and the load; the builds' table; the delay; the signal; the resume and the
    stores; `histories.csv` and what was written; the token checks; the suites; observed, not acted
    on; the commands, verbatim, with `<scratch>` for `var/ray-sqla-align-02/tools/`.

**What moves on purpose:** in the repository, exactly the prompt's §3 item 6:
`.documents/ray-sqla-align-verification.md` (new), `.documents/ray-sqla-align/out/02-*` (new), the
log `logs/02-rehearse-registered-runs-under-ray-2.59.md` and the board. The `out/02-*` files are:
`start.txt` and `stop.txt` with the `status` outputs, `ray status` and the `ps` matches (as one
`02-cluster.txt` or several); the three launch logs and the plot log, ANSI stripped; the
`interrupt_when.py` log; each run's `manifest.json` and `status.json` whole; both sidecars; every
`store` command's output; `compare_stores`'s output; `stage_counts.py`'s; the `compute_time`
readings; `ray_state.py`'s; the suites' summary; the token checks; the log-kind comparison; and the
helpers as `.txt`. Not the run directories' `stdout.log` and `stderr.log`, which repeat the launch
logs. A file over about 1 MB is reported before committing. Outside the tree: `var/datastores/`
(02's two stores, their shards and sidecars), `var/runs/` (three run directories),
`var/ray-sqla-align-02/`, the lock under `~/.raypoolkit/` (by `start`, removed by `stop`), and
`/tmp/ray-cpbh/`.

**What must not move:** every file of the tree outside the list above, code and tests above all;
the venv (`pip freeze` `cmp`-equal before the suites and after `stop`); `~/.ray/auth_token` (never
printed, copied, regenerated or `chmod`ed); `rrk-adoption`'s two sidecars in `var/datastores/`;
`orchestrator/`; `prompts/INDEX.md`; every other campaign's records; SGK, SI and the three packages.
Nothing in `var/` is deleted. No store but 02's two is opened.

**Conventions.** All of these bind the agent.
- **Order.** The Gate; the helpers (ruling 2); the suites; `start` and its checks; `reference` and its
  sidecar; the delay; `resumed-a` and the resumed store's `store create`; `resumed-c`; the plot; the
  Ray state; `stop` and `status`; the comparisons; the token checks; the log kinds; the suites again;
  the records, the token grep, the document, the log and the board.
- **Staging and reporting.** One commit, staged by explicit path only, after showing `git diff
  --cached --name-only`, which is the list above exactly. Check `git log -1` before committing:
  `HEAD` must still be this note's commit. Count, then paste. A record states only what a quoted
  line shows. "this commit" where the SHA goes; the orchestrator substitutes it.
- **Scratch.** Everything saved while working goes under `var/ray-sqla-align-02/` (`$L`); helpers in
  `$L/tools/`. Never `/tmp`, never the session scratchpad, never the tree.
- **Stop conditions are real**: the prompt's §4 and §0 here (rulings 3, 6, 8, 10, 12, 13, 14, 17).
  The cluster is stopped first, unless the stop is what failed or a build is still running: a stop
  with the cluster up, the lock held or a run `running` leaves them so, with `status`'s output and
  the run's pid and heartbeat. **If FP fails, change nothing**: finish the protocol if it is safe to,
  stop the cluster, record both digests, the sidecars' per-class detail, `compare_stores`'s output and
  the logs, and stop and ask before committing (README §2.2). A stop goes to the user through the
  orchestrator; the agent resumes by message.
- **Not touched:** `orchestrator/`, `prompts/INDEX.md`, the board beyond the prompt's §6 and ruling
  18, any other campaign's records, `jordan-normalization`.

## 1. Before you dispatch

Done at `1bb2f6d`, before this note was committed: the Gate above; `pip freeze` kept as
`orch02/pip-freeze-before.txt` in the session scratchpad; `ray_cluster.sh status` (free). The suites'
baselines are the agent's (its step 1), with 01's counts as the expectation.

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout. Give it: the prompt, the README, the
board, `HEAD`, the Gate as measured, and §0 of this note up to "## 1. Before you dispatch", as a copy
in the session scratchpad. The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, of records only, with exactly §0's list. Its log is
  `logs/02-rehearse-registered-runs-under-ray-2.59.md`, in README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **It starts Ray only through the script**, once, and opens no store but 02's two.
- **The token is never printed, copied or changed.**
- **Stop and ask** on any of the prompt's §4 conditions or §0's.
- **It replies with:**
  - the log's path; the message as committed; `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - `start`'s exit and its warning count; the `actors:` line; the two `ps -E` matches and controls
    with their pids; `ray status`'s resources;
  - the runs' table (run id, slug, store, pid, state, exit, launched → ended, wall,
    `results_store_id`, fingerprint at finish), with the load at each launch;
  - the five `compute_time`s, `<s>` and how ruling 6 gave it, and what `resumed-a` stored;
  - the signal: `interrupt_when.py`'s two lines, the log's lines after the banner, the two intervals;
  - `resumed-c`'s prune report (or none) and `stage_counts.py` on the three logs;
  - `compare_stores`' result and row counts; the two `store fingerprint` digests; `histories.csv`'s
    SHA-256; the reference store's `shasum`s before and after the plot;
  - the manifests' `git_head`, `git_dirty`, `script_sha256` and `packages`; the `>> packages:` line;
  - the token checks: the four `grep -c`s per file, the token file's `ls -l` and `/usr/bin/stat`,
    the final grep's exit;
  - `ray_state.py` at its three points; `stop`'s and `status`'s exits and what `status` said;
  - "What is new in the logs";
  - every deviation with its class; "Observations not acted on" and "State handed on", verbatim.

## 3. The review — eleven checks

1. **Scope.** `git show --stat <agent>` lists exactly §0's files; its parent is this note's commit.
   `git diff <dispatch> <agent> -- '*.py' '*.sh' '*.toml' '*.txt' ':!.documents' CLAUDE.md
   prompts/INDEX.md .documents/OPEN_ISSUES.md` is empty unless an issue opened. `git status
   --porcelain --untracked-files=all` empty; `var/` in no commit; nothing pushed.
2. **The venv and the token, here.** `pip freeze` `cmp`-equal to `orch02/pip-freeze-before.txt`;
   `pip check` clean. The token file: 64 bytes, mode 600, `1792932677`. Ruling 17's grep exits 1,
   and so does `git grep -F -l -f ~/.ray/auth_token <agent>`.
3. **The cluster, here.** `ray_cluster.sh status` (alone): the lock free, no process, the ports free.
   The lock module's `status`: free, no old location.
4. **The runs, from the registry.** `list`: three runs, `reference` `done`, `resumed-a` `killed`,
   `resumed-c` `done`. Each `manifest.json` and `status.json` under `var/runs/` read against the
   committed copies and the document's table: state, exit code, `script_sha256` against `shasum -a
   256 main.py`, `results`, `results_store_id` (ruling 8), `git_head` this note's commit, `git_dirty`
   false, the seven `packages` (ruling 14).
5. **The order and the signal.** In each launch log the three `** registered_run:` lines precede
   `>> packages:`. `resumed-a`'s log: the handler's line, the `SystemExit` line, the marker 143, the
   launcher's 143; the interval recomputed from the quoted lines (ruling 7); no alarm line, or its
   explanation.
6. **The delay.** `compute_times.py` re-run here on the reference store (`mode=ro`): the five times
   as logged, and `<s>` as ruling 6 gives it from them. The account of what `resumed-a` stored
   matches the resumed store's rows and `resumed-c`'s prune report and stage counts.
7. **The stores, re-measured.** `compare_stores.py` on the two: `RESULT: equal`, the logged row
   counts. `store fingerprint` on each, without `--write`: `2dd4b39b…932f`, each matching its
   sidecar. `store show` on each: the runs as logged. `stage_counts.py` on the three logs: as logged.
8. **`histories.csv`.** `shasum -a 256 var/ray-sqla-align-02/plot-reference/QCD_Cosmology/histories.csv`
   is `d7c62c37…c522`; the reference store's files' `shasum`s as logged after the plot.
9. **Token, 02's part.** The four patterns' `grep -c` re-run here on `start.txt`, the three launch
   logs and the plot log: all 0. The two `ps -E` matches in the committed record. **Gate FP and gate
   Token as found.**
10. **The records.** The document's §1 against the outputs under `out/` and the commands as run;
    nothing claimed that no quoted line shows. "What is new in the logs" re-derived here for one log
    pair by ruling 16's method. The board (ruling 18) and the log in §5.1's form, every deviation
    classed, §0's among them. The message in `CLAUDE.md`'s form.
11. **The suites** at `<agent>`, serially, no cluster up: 18 / 61 + 115 / 78 / 102 / 61. No `.py`
    file is committed, so `black` has nothing to check; the helpers' copies are `.txt`.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, on the board, in the log and in the document;
- the board's list of orchestrator notes, with this one marked "used for 02";
- `### 5.2`'s *Orchestrator review of prompt 02* paragraph, holding what each check found;
- the header and §1: 02 ✅ reviewed; **the campaign closed** (README §2: the review of 02 closes it),
  with what remains for the user: the merge of `ray-sqla-align` into `main` and the push, then `main`
  into `jordan-normalization` before its 09 (README §4.2), and the two stores to retire.
- `prompts/INDEX.md`'s row for the campaign, if it carries a status, brought to "closed".
Residue the review finds is fixed in that commit or a follow-up of its own. Anything that is not
residue is opened on the board's §3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:** the commit and the review commit; gate FP (both digests, `compare_stores`,
`histories.csv`) and gate Token; the three runs; the rulings that read the prompt otherwise than it
was written (the end wait repeatable once, absolute `S` and `L`, the token grep widened to the
document, `### 5.2`); and that the merges and the stores are the user's, at their word.
