# Orchestrator — prompt 01, move `ray` and `SQLAlchemy`, with token mode named

Read [`../README.md`](../README.md) first: §0.2 (the mechanism), §0.3 (the distributions), §1, §2.2
(gate V and gate Token), §4.1 (rollback) and §5 (the rules). Then the prompt and the board.
StochasticInstantons' orchestrator note for its own 01 (`.prompts/ray-sqla-align/orchestrator/prompt-01.md`
at SI `0bf788e`) is the model for this one; where it ruled on something ChamPBH's prompt also
asks, the ruling is restated in §0 here, so that the agent need not read it.

**You do not write code.** You may:
- run the suites, the new test module, `pip` read-only commands (`freeze`, `check`, `install
  --dry-run`), the lock's `status` and `ray_cluster.sh status`, each script or lock command alone in
  its Bash call;
- read the agent's outputs under `var/ray-sqla-align-01/` and `.documents/ray-sqla-align/out/`;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad (`orch01/`), never committing one, and never starting Ray;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`01-move-ray-and-sqlalchemy-with-token-mode.md`](../01-move-ray-and-sqlalchemy-with-token-mode.md)
· **Board items:** B1, P1, V0, D1, T1–T3, V1, C1 · **Gates:** **V**, and **Token**'s V1 part ·
**Closes:** nothing · **Opens:** only what the work finds · **Model:** Opus, as the prompt
recommends · **Network:** PyPI and GitHub, for the dry run and the install of §2.2 alone. Nothing is
pushed.

**Gate**, checked at `67e0346` on 2026-10-25 at 19:32 GMT, before this note was committed:
- `ray-sqla-align` checked out in the main checkout at the planning commit `67e0346`, on `main` at
  `a144c83`; `git status --short` empty; `git worktree list` shows the main checkout alone.
  `HEAD` is the commit that adds this note, which touches only `orchestrator/`: it is the prompt's
  **dispatch commit**. `jordan-normalization` is idle at `58c1c93` (it contains `a144c83`) and
  stays idle until 01 is reviewed.
- `./venv/bin/python -m runregistrykit list`: `no runs under …/var/runs`. `var/` holds `datastores/`
  and `runs/` only; `var/ray-sqla-align-01/` and `.documents/ray-sqla-align/` do not exist.
- The venv: Python 3.13.16; `ray==2.53.0`, `SQLAlchemy==2.0.46`, numpy 2.3.5, scipy 1.17.0;
  `datastorekit` at `c8570e849334…`, `raypoolkit` at `67a75b97a656…`, `runregistrykit` at
  `0bae3a3c501a…`, none editable; `pip freeze` 102 lines (kept as `orch01/pip-freeze-before.txt`).
- `ray_cluster.sh status`, exit 0: lock free (`~/.raypoolkit/rehearsal-lock` does not exist), no
  Ray process of the cluster, nothing `stop` would signal, ports 6380, 6381, 8267 and 52367 free.
  The lock module's `status`: free, no old location reported.
- `ls -l ~/.ray/auth_token`: `-rw------- 1 ds283 staff 64 Oct 25 12:51`; `/usr/bin/stat -f '%m %Sm'`:
  `1792932677 Oct 25 12:51:17 2026`. `env | grep -c RAY_AUTH_MODE`: 0.
- The prompt's line numbers hold: `requirements.txt:79` (`ray==2.53.0`) and `:92`
  (`SQLAlchemy==2.0.46`); the script's `cmd_start` at `:240`, `local t0=$SECONDS` at `:276`,
  `"$RAY_BIN" start --head` at `:277`, `local rc=$?` at `:280`, the `actors:` line at `:310`, usage
  at `:38`; `utilities.py:174`; `tools/tests/test_registry_config.py:64`;
  `ObjectFactories/tests/test_package_provenance.py:270-277`. `import ray` at `main.py:23`,
  `plot_by_beta.py:35`, `plot_ScalarModel.py:36`, and each driver's `ray.init` a top-level
  statement at column 0 (`main.py:133`, `plot_by_beta.py:125`, `plot_ScalarModel.py:106`), not
  inside a function or a `__main__` block (SI's correction 16 does not arise here).
- `black --check` on the six existing `.py` files 01 changes: all unchanged, so `black` on them
  touches only 01's lines.
- The one-minute load was 19.9 at the gate. Other workloads share the machine.

## 0. What makes this prompt unusual

**It moves the venv under every branch of this checkout, and its failure mode is an
authentication refusal at the first `ray.init`.** Once §2.2 installs, `jordan-normalization` runs on
Ray 2.59.0 too, without the token module (README §4.2), and the way back is README §4.1's one
`pip` line. It starts the real cluster twice under the lock shared with SecondaryGWKit and
StochasticInstantons: once to show the refusal (V0), once to show the fix (V1). **Every claim in
the log is a line the agent saw, quoted, with the command that produced it.**

**Corrections and rulings**, each recorded in the log as a deviation of the class given.

1. **`stat -f` is not BSD `stat` in this shell** (`STRUCTURALLY REQUIRED`). The shell's `stat` is
   GNU coreutils (`/opt/local/libexec/gnubin/stat`), which reads `-f` as "file system" and fails on
   `'%m %Sm'`, as it did when this note's gate was taken. Every `stat -f '%m %Sm'` of the prompt
   (§2.1, §2.9 step 7) is **`/usr/bin/stat -f '%m %Sm' ~/.ray/auth_token`**. The expected value is
   `1792932677 Oct 25 12:51:17 2026`. `timeout` is GNU coreutils' too
   (`/opt/local/libexec/gnubin/timeout`), and is what the prompt means.

2. **T3's driver checks are three methods, not subtests** (`IMPLEMENTATION CHOICE`, ruled). The
   prompt allows either; `unittest` counts methods, not subtests, so the choice sets the count.
   **Ruled:** one method per driver, each naming its file (for instance
   `test_main_py_imports_it_before_ray_init`), so the module has **nine test methods** and
   `tools/tests` goes from **93 to 102**. Every other root is unchanged: CosmologyModels 18,
   ComputeTargets 61 + 115, ObjectFactories 78, RunDrivers 61. A different number of methods is
   allowed only if the log says why, and then §2.8's tools count is 93 plus that number.

3. **Ray's token warning is a `logging` call, not a `warnings.warn`** (`NOTED`). Ray 2.59.0 emits
   `Token authentication is enabled for this Ray cluster…` by `logger.warning`
   (`ray/_private/authentication/authentication_token_setup.py:85`, SI's reading). So §2.3's `-W
   default` runs record Python warnings only, and cannot show it; and since no ChamPBH test starts
   Ray (`CLAUDE.md`), no suite of this repository could print it anyway. **The warning is measured
   where the prompt measures it: in `start`'s output at V0 (present) and V1 (absent).** No local
   `ray.init(address="local")` probe is run: ChamPBH never starts Ray that way, and SI's probe
   existed because SI's suites do.

4. **What V1 shows, and the refresh** (`NOTED`). A driver attaching by address never logs the
   warning, with or without the fix (README §0.2, the third route), so a driver log free of it shows
   nothing by itself. V1's evidence is `start`'s output without the warning, the `gcs_server`'s and
   the `raylet`'s environments, and the driver attaching at all, which V0 shows it cannot on the
   pins alone. **V1's driver exercises the refresh**: it imports `ray` before `config.ray_auth`, as
   `RunDrivers/registered_run.py:814` imports `ray` before `runpy` runs `main.py`, so the module finds
   `ray._raylet` loaded and must call `Config.initialize("")`. The log says so.

5. **The lock and the process table** (`STRUCTURALLY REQUIRED`; `CLAUDE.md`, README §5).
   - **`ray_cluster.sh start`, `stop` and `status`, and the lock module's `status`, are each the only
     process of their Bash call**, output redirected to a file (`> F 2>&1`), never piped, the exit
     taken with the builtin `echo $?` in the same call, the file read in a later call.
   - No `PURPOSE`, and **no command line of the agent's, names `/tmp/ray-cpbh`**: no `cat`, `grep`,
     `ls`, `lsof`, `pgrep -f` or `ps … | grep` naming it, from `start` to the end of `stop`. Pids are
     taken from `start`'s output (the `gcs_server`) and from `status`'s listing (the `raylet`), read
     from the saved files.
   - The driver, the `ps -E` checks and `ray status` each run in a Bash call of their own, never with
     a script or lock command; `stop` runs with no driver alive.
   - `start` exits 3 if the lock is held or its old location exists, and 4 if a port is held or a
     process of the cluster is alive: **either is a stop and ask**, never a wait, never a retry,
     never a removal. SGK's and SI's campaigns closed today, but either may start a cluster at any
     time.
   - **No `ray stop`, ever.**

6. **The cluster's environment** (`NOTED`, measured by the orchestrator). `ps -E -ww -o command= -p
   <pid>` prints a process's environment after its command on this macOS (this shell: `grep -c
   'PATH='` 1; SI's 01 found `RAY_AUTH_MODE=token` for its `gcs_server` and `raylet` the same way).
   Pipe it to `grep -o 'RAY_AUTH_MODE=[a-z]*'` or `grep -c 'PATH='` only; **never print an
   environment whole**. If either pid shows no environment at all (the control 0), record that and go
   on: the orchestrator rules on that part of **Token** at review. It is not a stop. A control of 1
   with no `RAY_AUTH_MODE=token` is the prompt's stop.

7. **The final token grep is meaningful** (`NOTED`, measured without reading the token). `wc -lc <
   ~/.ray/auth_token` gives `0 64`: no newline, so `grep -F -f` reads one 64-character pattern, and
   no empty pattern that would match every file. Exit 1 is the pass; exit 0 lists a file (remove
   that copy from the tree, report it, and do not commit it); exit 2 is an error, and a stop.

8. **Where the usage sentence goes** (`IMPLEMENTATION CHOICE`, ruled). The heredoc is quoted
   (`'EOF2'`), so the sentence is literal. **Ruled:** a paragraph of its own after the `status`
   entry and before "Never runs `ray stop`.", wrapped as its neighbours are. `tools/tests/test_ray_cluster_lock.py`
   must still pass unchanged.

9. **The install** (`NOTED`). pip resolves the three git requirements again and prints `Resolved …
   to commit <sha>` for each: those are the annotated tag objects (SI's deviation 19). The check is
   the freeze, whose three git lines must not move. `pip` is not upgraded, even if it suggests it.

10. **The roots** (`IMPLEMENTATION CHOICE` where the prompt is silent).
    - `CLAUDE.md`'s commands, serially, in the foreground, **never with the cluster up**,
      `ComputeTargets` in its two halves; each Bash call's tool timeout at 600 000 ms; output to a
      file under `var/ray-sqla-align-01/`, the verdict grepped from it, `uptime` beside each.
    - §2.3's `-W default` re-run is the same five commands with `-W default` after `python`. A
      warning is "new" if 2.1's output of the same root does not contain it; the log quotes each
      new one once, with its count and source line.
    - A failure is re-run alone before it is called real. An existing test that fails is a stop,
      never a reason to edit it; the two tests of §2.5 are the only existing tests 01 edits.
    - The tests that read the drivers with `ast` or by text, and which §2.6 asks to keep passing,
      live in the roots as they are: running the roots runs them. The log names any that needed
      thought.

11. **The breakages** (`IMPLEMENTATION CHOICE`, as the prompt asks, made checkable). Each of B1–B5
    is recorded as `01-B<n>.diff`, a diff that `git apply` replays from the fix tree, checked with
    `git apply --check` and `git apply -R --check` before it is applied, then reverted by `git apply
    -R` and confirmed by `cmp` against the saved copy. B5's diff is against the tree of §2.5 step 1
    (before the four names), and the log says so. None is committed; `git status` shows only the
    work afterwards.

12. **The board** (`NOTED`). The board's §5 has `### 5.0 Planning`; the prompt's `### 5.1 After
    prompt 01 (2026-10-25)` follows it, so no renumbering. Header: "**Status: 01 landed, awaiting
    review — 0 of 2 reviewed.**" or words to that effect; the §1 row ✅ with "this commit"; the
    items B1–C1 ✅ (or as found); gate **V** as found; gate **Token** "V1 part as found; 02's part
    ⬜". `.documents/OPEN_ISSUES.md` only if §3 gains an entry.

13. **The load** (`NOTED`). 19.9 at the gate, from other workloads. Nothing in 01 depends on it but
    the roots' wall times. Record `uptime` before the roots and before each `start`. No threshold.

**What moves on purpose:** the files of the prompt's §3 item 7 and nothing else in the tree; the
venv's `ray` and `SQLAlchemy`. Outside the repository, only what Ray and the script write: the lock
under `~/.raypoolkit/` (by `start`, removed by `stop`), and `/tmp/ray-cpbh/`.

**What must not move:** `~/.ray/auth_token` (never printed, copied, regenerated or `chmod`ed);
every other pin and the interpreter; every file under `ComputeTargets/`, `CosmologyModels/`,
`PRyM/`, `RunDrivers/`, `StoreFixtures/`, and `ObjectFactories/` but the one test; `config/` but the
new module; the rehearsal directory but `ray_cluster.sh`; every record of another campaign;
`prompts/INDEX.md`; `orchestrator/`; SGK, SI and the three packages. `var/` is not committed and
nothing in it is deleted.

**Conventions.** All of these bind the agent.
- **Order.** The gate checks; §2.1's baselines (before the install); §2.2's dry run, then the
  install; §2.3's roots on the pins; V0; §2.5; §2.6; §2.7's breakages; §2.8's roots and `black`;
  V1 last among the checks, once everything else passes; then `CLAUDE.md`, the records, the token
  grep, the log and the board.
- **Staging and reporting.** One commit, staged by explicit path only, after showing `git diff
  --cached --name-only`, which is §3 item 7's list exactly. Check `git log -1` before committing:
  `HEAD` must still be this note's commit. Count, then paste. A log states only what the diff, the
  tests and the transcripts show. "this commit" where the SHA goes; the orchestrator substitutes it.
- **Scratch.** Everything saved while working goes under `var/ray-sqla-align-01/` (the prompt's
  rule); a helper script, if any, goes in the agent's own subdirectory of the session scratchpad,
  never `/tmp` and never the repository, and is copied to `out/` as `.txt` if the log relies on it.
- **Stop conditions are real**: the prompt's §4 and §0 here (rulings 5, 6 and 7). The cluster is
  stopped first, unless the stop is what failed; a stop with the cluster up or the lock held leaves
  them so, with `status`'s output. A stop after the install says that the venv holds the new pins
  and gives README §4.1's line. A stop goes to the user through the orchestrator; the agent resumes
  by message.
- **Not touched:** `orchestrator/`, `prompts/INDEX.md`, the board beyond the prompt's §6, any other
  campaign's records, `jordan-normalization`.

## 1. Before you dispatch

Done at `67e0346`, before this note was committed: the gate above; `pip freeze` kept as
`orch01/pip-freeze-before.txt` in the session scratchpad; the lock module's `status` (free). The
roots' baselines are the agent's (§2.1), taken before the install.

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout. Give it: the prompt, the README, the
board, `HEAD`, the gate as measured, and §0 of this note up to "## 1. Before you dispatch", as a copy
in the session scratchpad. The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly §3 item 7's files. Its log is
  `logs/01-move-ray-and-sqlalchemy-with-token-mode.md`, in README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **It starts Ray only at V0 and V1**, through the script, and opens no store.
- **The token is never printed, copied or changed.**
- **Stop and ask** on any of the prompt's §4 conditions, §0's, or an existing test failing.
- **It replies with:**
  - the log's path; the message as committed; `git show --stat` of its commit;
  - the dry-run line, the `pip freeze` diff, `pip check`, and `01-distributions.txt`'s line;
  - the five roots at 2.1, 2.3 and 2.8, with counts, times and `uptime`, and every new warning of
    the `-W default` runs;
  - V0 step by step: `start`'s exit and its warning line quoted; the driver's exit, time, `grep -c
    InvalidAuthToken`, and its last error line quoted; `stop`'s and `status`'s exits and what
    `status` said;
  - `config/ray_auth.py` whole, and the `git diff` of the script and the three drivers;
  - T3's nine tests by name with what each asserts, and the module's standalone time;
  - B1–B5, each with the tests it failed;
  - V1 step by step: `start`'s exit and the warning count; the `actors:` line; the two `ps -E`
    matches and controls with their pids; the driver's exit, output and three counts; `ray
    status`'s CPUs and GPUs; `stop`'s and `status`'s exits; the token file's `ls -l` and
    `/usr/bin/stat`;
  - the final token grep's exit;
  - every deviation with its class; "Observations not acted on" and "State handed on", verbatim.

## 3. The review — ten checks

1. **Scope.** `git show --stat <agent>` lists exactly §3 item 7's files; its parent is this note's
   commit. `git diff <dispatch> <agent> -- ComputeTargets CosmologyModels PRyM RunDrivers
   StoreFixtures config/datastore.py config/sharding.py prompts/INDEX.md` is empty. `git status
   --short` clean; `var/` in no commit; nothing pushed.
2. **The venv, measured here.** `pip freeze` against `orch01/pip-freeze-before.txt`: exactly the
   `ray` and `SQLAlchemy` lines moved; the git packages unmoved and not editable; `pip check` clean;
   `pip install --dry-run -r requirements.txt` installs nothing. `01-pip.txt` has the dry run
   before the install, naming the two packages alone.
3. **The token file, here.** `ls -l` and `/usr/bin/stat`: 64 bytes, mode 600,
   `1792932677`. `grep -r -F -l -f ~/.ray/auth_token .documents/ray-sqla-align/ prompts/ray-sqla-align/`
   exits 1, and so does `git grep -F -l -f ~/.ray/auth_token <agent>`.
4. **The fix, by reading.** `config/ray_auth.py`: the licence header; the constant; set only when
   absent; the refresh only when `ray._raylet` is in `sys.modules`; no Ray import; the comment's
   points as §2.6 lists them. The three importers: two lines each (the comment and the import),
   straight after `import ray`, before the repository's first import, nothing else changed. The
   script: the export before `local t0=$SECONDS` with the `:-token` default and its comment; the
   `actors:` line; the usage paragraph where ruling 8 puts it; no hunk in the lock half, the stop,
   the ports or `ray start`'s arguments. `bash -n`. `--help` carries the sentence.
5. **The distributions.** The two lists equal, the six names in the order ruled, both comments
   naming the campaign and U3; the equality test's docstring; `test_default_names`'s six and its
   docstring. `format_package_provenance(installed_package_provenance())` here: `ray 2.59.0` and
   `SQLAlchemy 2.0.55`, the four PyPI ones `(no recorded origin)`.
6. **T3 and the breakages.** `tools/tests/test_ray_auth.py`'s nine methods cover §2.6's groups,
   start no Ray, write nothing, read no `HOME`; each probe's environment is `os.environ` less
   `RAY_AUTH_MODE` plus what it sets. It runs here standalone, with its time. Each of B1–B5
   replayed here from the committed diffs with `git apply`, its check run, its failure seen as the
   log says, and reverted; `git status` clean afterwards.
7. **V0, from `01-v0.txt` and its files.** `start` 0 with the warning; the driver's refusal lines
   and non-zero exit, nothing from `cluster_resources()`; `stop` 0; `status` free, no process, the
   ports free.
8. **V1, from `01-v1.txt` and its files.** `start` 0, warning count 0, the `actors:` line carrying
   the variable; `RAY_AUTH_MODE=token` for both pids, each control 1 (or ruling 6's case, ruled
   here); the driver's `42`, exit 0 and its three counts 0; `ray status` 0; `stop` 0; `status`
   free. Gate **V** and **Token**'s V1 part as found.
9. **The records.** `CLAUDE.md`'s new bullet after "The rehearsal lock", which is unchanged, with
   §2.10's points; the `tools/tests` count 102 and the date sentence. The board's header, §1 row,
   items, gates and `### 5.1`. The log in §5.1's form, every deviation classed, §0's among them.
   `OPEN_ISSUES.md` untouched unless an issue opened, and then in the same commit. The message in
   `CLAUDE.md`'s form, ending with the `Co-Authored-By` line.
10. **The roots** at `<agent>`, serially, no cluster up: 18 / 61 + 115 / 78 / 102 / 61. `black
    --check` on the changed `.py` files. Then `ray_cluster.sh status` (a command of its own): lock
    free, no process, ports free; `runregistrykit list` no runs.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, on the board and in the log;
- the board's list of orchestrator notes, with this one marked "used for 01";
- `### 5.1`'s *Orchestrator review of prompt 01* paragraph, holding what each check found;
- the header and §1: 01 ✅ reviewed; 02 released from its hold (⏸ removed), to be dispatched at the
  user's word.
Residue the review finds is fixed in that commit or a follow-up of its own. Anything that is not
residue is opened on the board's §3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:** the commit and the review commit; the venv on `ray` 2.59.0 and
`SQLAlchemy` 2.0.55, shared with `jordan-normalization` (which must not start Ray until `main` is
merged into it, README §4.2); V0's refusal and V1's fix; the five roots; the rulings that read the
prompt otherwise than it was written (GNU `stat`, three driver methods and so 102); that P1–P12
stood as proposed; and that 02 is ready for dispatch at the user's word.
