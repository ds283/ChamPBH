# Orchestrator — prompt 03, the wrapper

Read [`../README.md`](../README.md) first: §0.2 (what SecondaryGWKit's wrapper does), §0.3 (`main.py`'s
shape; RayWorkPool's printing), §2.2 (03's charter), §5 and §6 (R2, R3, U2, U3, P1–P7, P9). The
conventions are note 01's ([`prompt-01.md`](prompt-01.md) §0, "Conventions"), restated here where 03
needs them.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`03-the-wrapper.md`](../03-the-wrapper.md)
**Board items:** W1–W6 · **Gate:** "Wrapper" · **Closes:** nothing · **Opens:** only what the work
finds · **Model:** Opus, as the prompt recommends · **Network:** none.

**Gate:**
- 01 landed (`b0f3488`, reviewed `caeadc0`) and 02 landed (`c9eea37`, reviewed `87caa99`); the gates
  "Pin" and "Fingerprint" hold (board §2). `HEAD` is the commit that adds this note, on top of
  `256cf38` (02's licence-header housekeeping, which touched 02's three new Python files only).
- The prompt's own gate (its header): no tracked change, no `var/`, no ChamPBH Ray process.
- One prompt at a time in this checkout. 04 is not dispatched until 03 is reviewed.

## 0. What makes this prompt unusual

**It is the code every registered run will go through, and its tests must stand in for Ray.** Nothing
in it computes, but a mistake in it is recorded as a wrong history of a run: a `done` that was a
refusal, a `failed` that was a signal, a heartbeat that says less than it claims. SecondaryGWKit's
`RunDrivers/scoped_pipeline_run.py` at `031bcb4` is the model, read whole by the orchestrator; its
`StageTracker`, `RegisteredStream`, `terminal_state`, `SignalLatch`, `make_killed_handler`,
`install_killed_handler`, `finish_after_exec` and `register` carry over. It prints no `** driver exit
status` marker and has no `main()` seam, so those two are new here, and §0's rulings 7 and 8 say how
they go.

**Eleven statements in the prompt are wrong, loose or silent. Ruled here:**

1. **A missing database is refused by `main.py`'s parser, not by `main.py`.** The prompt's W2 (and
   README §0.3) says that with no database `main.py` "would print its help and `sys.exit()` with no
   code, which would be recorded `done`". It would not. `--database` is `required=True`
   (`config/argument_parser.py:56-62`, since `2f5ccb6`), so `create_argument_parser().parse_args([])`
   prints the usage and `error: the following arguments are required: --database` and exits **2**.
   So does a `--config` YAML file whose `database:` has no value (the orchestrator's probe).
   `main.py:90-92` cannot be reached from the command line. The refusal stands, for a different
   reason: the parse is the wrapper's, before `begin`. **Ruled:**
   - the wrapper parses `main.py`'s arguments with `create_argument_parser()`, unchanged, and replaces
     `error` **on that instance** by a function that raises `Refusal(f"main.py's arguments:
     {message}")`. A missing database, an unrecognised option and a bad value are then each one `!! `
     line, exit 2, before `begin`. The orchestrator measured that `configargparse` routes all three
     through the instance's `error`;
   - after the parse, a database that is empty (`--database ""`) is refused the same way;
   - `-- --help` prints `main.py`'s help and exits 0, and nothing is registered.

   Classify the reason as `STRUCTURALLY REQUIRED` and the `error` override as `IMPLEMENTATION
   CHOICE`, each citing this note.

2. **The wrapper's own flags: split by hand, refused in one line.** argparse's `required=True` error
   is two lines on stderr (the usage, then `error: …`), not the one `!! ` line W2 and test 6 ask for.
   And `argparse.REMAINDER` handles `--` differently across 3.12 and 3.13. **Ruled:**
   - `argv` is split at its **first** bare `--`. What comes before it is parsed by the wrapper's
     parser. What comes after it is `main.py`'s, verbatim. No `--` at all is a `Refusal` saying that
     `main.py`'s arguments follow a bare `--`;
   - the wrapper's parser has its `error` replaced the same way, so a missing or unknown flag of
     its own is one `!! ` line, exit 2;
   - each of the four flags is then checked non-empty after `strip()`. An empty one is refused by
     name.

   Classify as `IMPLEMENTATION CHOICE`, citing this note.

3. **`RAY_ADDRESS_PROBLEM` already begins with `!! `** (`config/argument_parser.py:322`). Printed
   as `!! {exc}` it would read `!! !! --ray-address …`. **Ruled:** the `Refusal` carries the text
   with the leading `!! ` removed. Test 6 asserts that the line starts `!! ` and not `!! !!`.
   Classify as `IMPLEMENTATION CHOICE`.

4. **What `begin` records, and the `argv` the manifest keeps.** Read at `v0.2.0` (`runregistrykit/
   __init__.py:1082-1158`):
   - `begin` takes `git_head` and `git_dirty` from the **directory of `script`** (`start`, `:1082`
     and `:1158`), not from `client_root`;
   - `script` and `results` are recorded relative to the client root where they lie under it
     (`_repo_path`), and absolute otherwise;
   - `argv` is `sys.argv` unless one is passed (`:1142`).

   So in production the manifest says `script: "main.py"`, the `main.py` hash, this repository's
   `git_head`, and `results: "var/datastores/…"`. In a test, with a stand-in in a temporary
   directory, `script` and `results` are absolute paths, `git_head` is `"unknown"` and `git_dirty`
   is `false`, and a bare `sys.argv` would be the test runner's. **Ruled:**
   - `register` passes `argv=[str(Path(__file__).resolve()), *argv]`, where `argv` is the wrapper's
     own arguments, `sys.argv[1:]` in production. Test 1 asserts that the manifest's `argv[0]` is
     the wrapper's path and that `argv[1:]` is what `main()` was given;
   - test 1 asserts the stand-in's values as above, `git_head` `"unknown"` included, and says why in
     a comment;
   - **one more test** calls `register` alone, with `program=MAIN_PY` and a temporary runs root,
     and runs nothing. That is safe, since `register` reads no `main.py` text and starts nothing.
     It asserts `script == "main.py"`, `script_sha256` equal to `main.py`'s own, and `git_head`
     equal to this repository's `HEAD`, read by `git rev-parse` in the test. This pins U3 in its
     production form. It restores the streams and both handlers it took over, as SecondaryGWKit's
     `test_hard_killed_predecessor.register` does, observing before restoring.

   Classify the `argv` as `IMPLEMENTATION CHOICE` and the extra test as `STRUCTURALLY REQUIRED`
   (the stand-in cannot show U3), each citing this note.

5. **`sys.argv[0]` must be the program while it runs, and it is.** Ray adds the directory of
   `os.path.realpath(sys.argv[0])` to every worker's `sys.path` when it is on the driver's
   (`ray/_private/worker.py:2635-2641`), and the current directory. `runpy.run_path` sets
   `sys.argv[0]` to the program's path for the run (`runpy.py`, `_ModifiedArgv0`). The
   orchestrator's probe shows `__name__ == "__main__"` and `sys.modules["__main__"]` is the
   program's namespace. So `main.py` run through the wrapper gives its workers the paths a direct
   run gives them: the root, twice. **Ruled:** the stand-in records `sys.argv`, `__name__` and
   `__file__` (in a file, or on stdout). A test asserts that `argv[0]` is the program's path,
   `argv[1:]` is `main.py`'s arguments, and `__name__` is `"__main__"`. Classify as `IMPLEMENTATION
   CHOICE`.

6. **Ray writes the workers' output into the driver's `sys.stdout`, from a thread of its own.**
   `ray.init` starts a `ray_print_logs` thread, since `log_to_driver` defaults to true, and
   `print_to_stdstream` writes each worker line to `sys.stdout` or `sys.stderr` as they are then
   (`ray/_private/worker.py:2153-2171`, `:2713-2722`). Under the wrapper those are the proxies. So:
   - **a beat can come from a worker's own line**, not only from RayWorkPool's. `heartbeat_means`
     must say so. SecondaryGWKit's sentence does not, and the lesson carries over, not the omission;
   - **`_observe` runs on two threads.** SecondaryGWKit's `_pending` buffer is not guarded, so two
     threads can garble a line's stage. **Ruled:** each proxy holds a `threading.RLock` around
     `_observe` only. The real write stays outside it, first and unconditional. The lock is
     re-entrant, so a signal handler that prints on the main thread inside `_observe` does not
     deadlock;
   - **a test:** a line written from a second `threading.Thread` reaches the real stream and
     `stdout.log`, and beats.

   A worker line carries Ray's `(pid=…)` prefix, which none of the three patterns matches, so it
   beats without moving the stage. Classify as `IMPLEMENTATION CHOICE`, citing this note.

7. **The marker: printed once, by the script's entry, and never written to `status.json`.** The
   prompt asks for `** driver exit status N` as the last stdout line and tests it in-process (its
   test 3). Three facts constrain where it goes:
   - the line begins `** `, which `_STAGE_BANNER` matches. Printed through a beating proxy, it would
     write a heartbeat and the stage `driver exit status 0` **after** the terminal state;
   - SecondaryGWKit's clean path closes the copies, and its failure path re-raises so that the
     interpreter prints the traceback. Ray installs its own `sys.excepthook` (`worker.py:2150`), so
     the re-raise is what keeps that traceback "what it always was";
   - a `Refusal` is printed by the `__main__` block, not by `main()`.

   **Ruled:**
   - `main(argv=None, *, program=MAIN_PY, runs_root=None)` is the work, as the prompt's seam. It
     returns, raises `main.py`'s exception, or raises `SystemExit`, as SecondaryGWKit's `main` does;
   - a second function, `run(argv=None, *, program=MAIN_PY, runs_root=None)`, is the script's
     entry, and the `__main__` block is `run()` alone. `run`:
     - calls `main`;
     - prints a `Refusal` as one `!! ` line on stderr, with status 2;
     - in a `finally`, stops every proxy beating (a method on `RegisteredStream`) and **then**
       prints the marker with the status the process will exit with:
       - 0 for a return;
       - Python's own mapping for a `SystemExit`: `None` is 0, an `int` is itself, anything else
         is 1;
       - 2 for a `Refusal`;
       - 1 for any other exception;
     - then closes the copies on the clean path only, as SecondaryGWKit does;
     - then exits with that status, or re-raises `main.py`'s exception;
   - so the marker is the last line on stdout and in `stdout.log` for every outcome after
     registration, and is printed for refusals and `--help` too. `status.json`'s `stage` is never
     the marker. A failed run's traceback, printed by the interpreter after `run` unwinds, still
     reaches `stderr.log`, after the marker in a merged log;
   - **no marker** means the process died of a signal without unwinding: a `SIGKILL`, or a
     `SIGTERM`/`SIGINT` before `ray.init`, which `terminal` records as `killed` and then re-raises
     with the default disposition. The docstring says both.

   Classify as `IMPLEMENTATION CHOICE`, citing this note.

8. **Which tests run in a subprocess.** In-process, nothing prints a traceback and nothing exits,
   so the prompt's test 3 ("the wrapper's own exit status", "the traceback reaches `stderr.log`")
   and test 6 ("exit 2, one `!! ` line, no traceback") need the real entry. **Ruled:**
   - **a helper module**, not a test module (`RunDrivers/tests/stubbed_run.py` or similar), is run
     as `python -u -m <it> <the runs root> <the program> -- <the wrapper's arguments>`, with `cwd`
     the repository root. It patches `ray.init` and `ray.shutdown` with stand-ins and calls
     `registered_run.run(…, program=…, runs_root=…)`. Its `ray.init` stand-in installs a Ray-like
     `SIGTERM` handler, `sys.exit(signum)`, as `ray.init` does (`worker.py:1663-1668`), so that the
     wrapper's must replace it. `-u` is required: through a pipe, the stand-in's banner is otherwise
     block-buffered and the signal test would wait for ever;
   - **by that subprocess:**
     - test 3's four outcomes. Each asserts the exit status, the state and code in `status.json`,
       and the marker as the last line of both the captured stdout and `stdout.log`. The
       `RuntimeError` case also asserts that `Traceback` and the error's text are in `stderr.log`;
     - test 4's real `SIGTERM`: exit 143, `killed`/143, and the marker `143` last;
     - test 6's `begin` refusal: exit 2, stderr one `!! ` line naming `python -m runregistrykit
       end`, no `Traceback`, and the runs root unchanged;
   - **by the real script** (`./venv/bin/python RunDrivers/registered_run.py …`, `cwd` the root,
     no stub): test 6's refusals before `begin` (each of the four flags, no `--`, no database, an
     unrecognised `main.py` option, `--inventory`, `--ray-address auto`). These reach neither
     `begin` nor Ray, so `var/` is not created. Each asserts exit 2, stderr exactly one line
     starting `!! `, no `Traceback`, and stdout's last line `** driver exit status 2`. Where `var/`
     exists afterwards it is the user's; the test asserts that it was not created and, if it
     existed, that `var/runs`' listing is unchanged (02's choice, accepted at 02's review);
   - **in-process:** everything else, with `mock.patch.object(ray, "init"/"shutdown")`, and a cleanup
     registered before the call that restores `sys.stdout`, `sys.stderr`, the `SIGTERM` and `SIGINT`
     handlers, and closes the proxies' copies;
   - every subprocess is reaped by a cleanup registered before its first read, as SecondaryGWKit's
     `test_hard_killed_predecessor._reap`. Every test, and each subprocess's helper, ends with
     `ray.is_initialized()` false.

   Classify as `STRUCTURALLY REQUIRED`, citing this note.

9. **Test 7 does not pin `main.py`'s bytes.** "`git diff 3cd2be4 -- main.py …` is empty …, written
   as a test that reads the files" can only be a pin of each file's SHA-256 at `3cd2be4`. That test
   would fail the first legitimate edit to `main.py` by a later campaign: `jordan-normalization`
   changes what the pipeline computes. The campaign's invariant is "byte-identical at every commit
   **of the campaign**" (README §5). R2's lasting content is that `main.py` runs without the package.
   **Ruled:**
   - the test asserts, by `ast`, that none of `main.py`, `plot_by_beta.py`, `plot_ScalarModel.py`
     and `utilities.py` imports `runregistrykit` or `RunDrivers` (any `import` or `from … import`
     whose module is either name or begins with it and a dot);
   - byte-identity since `3cd2be4` is the log's `git diff --stat 3cd2be4 -- <the four>` (empty),
     and the review's.

   Classify as `IMPLEMENTATION CHOICE`, citing this note.

10. **Test 5's store is 02's golden store, by import.** "Built as 02's golden test builds it" and
    "equals the golden digest" ask for the same rows. **Ruled:**
    - the test class subclasses `_GoldenStoreCase` from
      `ObjectFactories.tests.test_store_fingerprint_golden`, and imports `GOLDEN` and
      `without_taken` from it, nothing else;
    - `build_store()` closes the actors and returns the primary. With a sidecar, the sidecar is
      made by `runregistrykit.stores.create_sidecar(primary, purpose=…, client_root=REPO_ROOT)`
      **before** the run, as a person does after a first close;
    - the stand-in returns without touching the store;
    - **with the sidecar:** the status has `state: done`, `fingerprint_sidecar: "written"`, and a
      `fingerprint` whose `digest` is the golden's, `without_taken` equal to the golden's. The
      sidecar's `fingerprint` has the same `digest`;
    - **without one:** `fingerprint_sidecar` starts `"not written"`, `state` is `done`, and there is
      no `fingerprint_error`.

    The text after "not written" is the package's (`__init__.py:865-868`), so the test does not
    pin it. This is the only test module of this root that builds a store, and it is subject to
    `_TempStoreCase`'s rule that Ray is never initialised. Classify as `IMPLEMENTATION CHOICE`.

11. **`scope` is the arguments as given, never the grid.** `main.py` computes its grids itself, with
    a blacklist, rounding and units (`main.py:1079-1130`), and a wrapper that re-derived them would
    be a second copy that can drift. **Ruled:**
    - `scope` is one line from the parsed `Namespace`. It covers: the config file (or "no config
      file"); the potential type as given (`None` printed as `none`, for which `main.py:1006`
      raises `ValueError` after Ray starts); the β, M and Λ choices; φ*; the shards; and the job name;
    - a choice given as values reads as the values when there are at most six, and otherwise as a
      count and a range (`7 values, 0.1–3`). A choice given as a range reads as its arguments
      (`β 0.1–3.0, 5 per unit`). The M and Λ choices name their units (`Mp` or `eV`);
    - a test asserts the scope of 04's pilot arguments (`--config exponential.yaml --M-values-Mp 1e-3
      --beta-values 1.1 1.15 1.2 10 25 --shards 16 --job-name …`) as a literal.

    Classify as `IMPLEMENTATION CHOICE`.

**`heartbeat_means`** says, in ChamPBH's words, each of:
- a beat is taken from a line on stdout, at most once every 30 s, and the line is the stage;
- RayWorkPool prints a progress line only after `ray.wait` has returned completed work, and in this
  repository no more often than every 300 s (`MIN_NOTIFY_INTERVAL`), so a fresh beat from it means
  work completed within about five minutes;
- Ray forwards the workers' own output to the driver's stdout, so a worker's line beats too (§0
  ruling 6);
- a stage that prints nothing for longer than the staleness window (900 s, `list --stale-after`)
  shows stale while it works. Stale means go and look.

**Signals, read and not measured** (04 measures; P5). `ray.init` installs `sys.exit(signum)` as the
Python `SIGTERM` handler at its start (`worker.py:1663-1668`), before the address is resolved, so for
any address. SecondaryGWKit's audit S1–S4 measured the absl handler on a Ray it started itself. Do
not simplify the design on the assumption that an attached driver differs.

**The ast test (8).** SecondaryGWKit's `_finish_calls`, with `_HANDLERS = ("terminal",
"killed_handler")`. Expect **4** `run.finish(` calls outside a handler, each passing
`fingerprint=True` (in `finish_after_exec`: done, `terminal_state`, failed, killed), and **2** inside,
neither passing it. Any other count is a design change; say why in the log.

**The deliberate breakages, read concretely.** Each must fail a named new test:
- *handler before the wrapped `ray.init` only*: drop the `install_killed_handler` call from the
  wrapper. The `getsignal` test fails, and so does the subprocess `SIGTERM` (the stand-in's
  `sys.exit(15)` gives `killed`/15 by `terminal_state`, exit 15);
- *`fingerprint=True` passed by the handler*: test 8 fails, and so does the direct-call test;
- *the wrapper exits 0 whatever*: `run`'s status forced to 0. Test 3's subprocess cases fail;
- *the copy failure drops the line*: put the real write inside the `try`, after the copy. Test 2's
  case with a copy whose `write` raises fails. **That case is required**, and the prompt's test 2
  does not name it;
- *`script` = the wrapper*: test 1 and the `register(program=MAIN_PY)` test of ruling 4 fail;
- *the `ConfigError` caught as a refusal*: remove the `except runregistrykit.ConfigError: raise`.
  A test with `runregistrykit.begin` patched to raise `ConfigError` asserts that `main` raises
  `ConfigError`, not `Refusal`, and that nothing is created. **That test is required**, and the
  prompt's test 6 does not name it. SecondaryGWKit's `ast` check of the handlers' order may be added
  beside it.

**What moves on purpose:** the prompt's §5 list, and nothing else.
- `RunDrivers/__init__.py`, `RunDrivers/registered_run.py`, `RunDrivers/tests/__init__.py`,
  `RunDrivers/tests/test_registered_run*.py` and one non-test helper module (new). Each Python file
  carries the repository's licence header **whole**, ending `# limitations under the License.`
  (01's and 02's new files each needed a housekeeping commit for it).
- **`CLAUDE.md`, three edits only:**
  - the line `PYTHONPATH=. ./venv/bin/python -m unittest discover -s RunDrivers/tests -t .   # N
    tests` after `tools/tests`;
  - the parenthesis `(The counts are those of 2026-10-14.)` becomes `(The counts are those of
    2026-10-14, and RunDrivers/tests' of 2026-10-19.)`;
  - one sentence in "Repository mechanics": the launchers of registered runs live in `RunDrivers/`,
    with their tests in `RunDrivers/tests/`, and `tools/` keeps repository-level tooling.

  **Not** the ObjectFactories count (`# 69`, now 78; 02 handed it to 05). **Not** `:136`'s "which
  `rrk-adoption` prompt 03 adds; until it lands, no run is registered" (05's Z2 makes it definite).
- The log `logs/03-the-wrapper.md` and the board. No issue is expected; the index is untouched
  unless one opens.

**The facts, checked by the orchestrator on 2026-10-19 at `256cf38`.** Pass them on.
- **The tree.** Branch `rrk-adoption`. Nothing staged, no tracked change. The untracked set is
  note 01's 22 entries. No `var/`, no `RunDrivers/`. The main checkout.
- **Ray.** `ray_cluster.sh status` exits 0: lock free, no ChamPBH process, ports free. `pgrep -lf
  'gcs_server|raylet|ray::'` finds nothing. `RAY_ADDRESS` is unset. Ray is 2.53.0.
- **`main.py`** is unchanged since `3cd2be4`. It reads no `__file__`, installs no handler, and calls
  `ray.init(address=args.ray_address)` at `:133` as an attribute of `ray`, so patching `ray.init`
  is seen. `--config exponential.yaml` gives `potential-type` (the rehearsal's `build.sh`).
- **RayWorkPool** (`raypoolkit/RayWorkPool.py`): the banner `\n** {title}` at `:206`; progress at
  `:588` and `:592`, reached only after `ray.wait` (`:356`) returns, when more than
  `notify_min_time_interval` has passed since the last line and either `notify_time_interval`
  (default 300) has passed or more than `notify_batch_size` items have been processed. `main.py`'s
  three titled queues (`:326`, `:585`, `:872`) set the floor to 300 and the batch to 2; its other
  queues have `title=None` and print nothing. `!! WARNING` lines (`:541`) go to stdout and match no
  pattern. `main.py`'s own `>>` and `**` lines are at `:168` and `:1080`.
- **RunRegistryKit `v0.2.0`:** `begin` at `__init__.py:987` (the keyword-only `client_root`;
  `argv`); `finish` at `:805`, which writes `state` whatever the earlier state was, so the handler's
  `killed` and `finish_after_exec`'s are two writes; `"written"` / `"not written: …"` at `:865-868`;
  `ConfigError(RuntimeError)` at `client.py:84`; `end RUN --reason TEXT --root DIR` in
  `__main__.py:499-518`; `create_sidecar(primary, purpose, *, client_root=None)` at
  `stores.py:846`.
- **SecondaryGWKit at `031bcb4`** (read with `git -C … show 031bcb4:<path>`; never edit): its
  driver and its `RunDrivers/tests/`. Carry the form over. Not its source substitution, model filter,
  local Ray, existing-store refusal or `--allow-existing`.
- **The baselines** at `256cf38`, run by the orchestrator: see §1. The agent runs its own before
  any edit (the prompt's §2.1), and its own govern.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-only`
  before committing. Count, then paste. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its
  follow-up.
- **Suites.** `CLAUDE.md`'s commands, serially, in the foreground, ComputeTargets in its two
  halves, output to a file in the agent's scratch directory, the verdict grepped from it, `uptime`
  beside each. A failure is re-run alone before it is called real. The new root runs **three times**
  in a row at the end, since it has subprocesses, signals and threads: a flaky test is a stop.
- **Breakage diffs** are recorded so that `git apply` replays them from a clean tree at the agent's
  commit. Check each with `git apply --check` and `-R --check`, and replay in `bash`, not zsh.
- **Scratch.** The agent uses its own subdirectory of the session scratchpad, never `/tmp`. No
  scratch file goes under `RunDrivers/`, `config/` or `.documents/`.
- **Ray.** The agent starts no Ray, runs no driver against a cluster, and runs `main.py` neither
  directly nor through the wrapper. `registered_run.py --help` and the refusal subprocesses are the
  only real-script runs.
- **`var/`.** It must not exist at the commit. Check with `ls -d var` after the suites.
- **Stop conditions are real.** A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. Review against the agent's commit, and check
  `git log` before measuring at `HEAD`.

## 1. Before you dispatch

1. **The tree.** Branch and `HEAD`; `git status --short` shows only §0's untracked set; nothing
   staged; `git log --oneline 256cf38..HEAD` is this note's commit only.
2. **Ray.** `ray_cluster.sh status` and `pgrep -lf 'gcs_server|raylet|ray::'`.
3. **Baselines.** The four roots at `HEAD`, serially, with the load. Give the agent the numbers.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Opus**. Give it:
- the prompt;
- the campaign README and this board;
- `HEAD` and the baselines;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad.
  The agent does not read `orchestrator/`, and does not read prompts 04–05.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/03-the-wrapper.md`, in `dk-and-rpk-migration` README
  §5.1's form.
- **The files it may add or change are §0's list, and nothing else.**
- **§0's eleven rulings** bind it, each classified as §0 says.
- **It starts no Ray, runs no `main.py`, opens no store but its temporary ones, and creates no
  `var/`.**
- **Stop and ask** on any of the prompt's §4 conditions. An existing test that fails is a stop,
  never a reason to edit it.
- **It replies with:**
  - the log's path and the message as committed;
  - the module's public names, with one line each;
  - `HEARTBEAT_MEANS` as committed, and the pilot's `scope` literal;
  - the new tests by name, with counts and wall time, and the three runs of the new root;
  - each breakage with the tests it failed and how;
  - §2.3's timing on paper: the longest silence of a healthy pilot build;
  - the four suites and the new root;
  - every deviation with its tag;
  - "State handed to the next prompt", verbatim.

## 3. The review — twelve checks

1. **Scope.** `git diff <agent>~1 <agent> --stat` lists §0's files only. `git status` shows §0's
   untracked set and nothing new. No `var/`. `git diff 3cd2be4 <agent> -- main.py plot_by_beta.py
   plot_ScalarModel.py utilities.py` is empty.
2. **`CLAUDE.md`.** The diff is the three edits of §0 and nothing else; the count matches the root's.
3. **The module, by reading against SecondaryGWKit's.** Diff `StageTracker`, `RegisteredStream`,
   `terminal_state`, `SignalLatch`, `make_killed_handler`, `install_killed_handler` and
   `finish_after_exec` against `031bcb4`'s, function by function. Every difference is ruled in §0
   or named in the log. Nothing of the substitution, model filter or local Ray came across.
4. **The refusals and the seam.** The hand split at `--`; both parsers' `error` replaced; the
   empty-flag and empty-database checks; the `!! ` prefix stripped; `--inventory` and the address
   refused before `begin`; `begin`'s `try` with `ConfigError` re-raised first; nothing taken over
   before `begin` returns.
5. **Registration.** `begin`'s arguments: `script=program`, `results` absolute,
   `client_root=REPO_ROOT`, `root=runs_root`, the `argv` of ruling 4, `scope` and `heartbeat_means`.
   `HEARTBEAT_MEANS` says the four things of §0.
6. **The run and the finish.** `sys.argv` set, `runpy.run_path(str(program), run_name="__main__")`,
   the `try`, `finish_after_exec(…, ray.shutdown)`, the bare re-raise. `run`'s `finally`: stop
   beating, marker, copies, status. No code reads `main.py`'s text.
7. **By probe**, from the scratchpad, with no Ray. Run the helper subprocess on a stand-in of the
   orchestrator's own: one that prints a banner and 50 progress lines, then `sys.exit(3)`. Check:
   - exit 3 and `failed`/3;
   - the marker last in stdout and in `stdout.log`;
   - `status.json`'s `stage` is the last progress line's sentence, not the marker;
   - the heartbeat count (one beat for the banner, then at most one per 30 s).

   Then run `registered_run.py` on `-- --database x --inventory --ray-address 127.0.0.1:6380`: exit
   2, one line.
8. **The tests, by reading.** Every in-process test restores streams and handlers; every subprocess
   is reaped by a cleanup registered before its first read; `ray.is_initialized()` is asserted; no
   test writes outside a `tempfile` directory. Test 7 is the `ast` form of ruling 9. Test 8 expects
   (4, 2).
9. **The breakages.** In `bash`, replay each diff with `git apply`, run the named modules, see each
   fail as the log says, revert with `git apply -R`, `git status` clean.
10. **The records.** The board: row 03 ✅ with "this commit" and the log; W1–W6 ✅; the gate
    "Wrapper" ✅; `### 5.4 After prompt 03 (<date>)`. §3 and the index untouched, or, if one opened,
    the index row, count and date in the same commit. The log in §5.1's form, every deviation
    classified, the breakage diffs, §2.3 on paper.
11. **The suites** at the agent's commit, serially: 18, 61 + 115, 78, 84, and `RunDrivers/tests`
    at the log's count, **twice**. `black --check` on every new Python file. `registered_run.py
    --help` exits 0. `runregistrykit list` still prints no runs. No `var/`.
12. **Ray.** After the suites, `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing, and
    `ray_cluster.sh status` exits 0.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 03* paragraph in §5.4. Fix small
  residue in a follow-up commit of its own:
  - "this commit" → the SHA (board and log);
  - the notes list, with this one marked "used for 03";
  - the status line: 3 of 5 landed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the message as committed;
  - the rulings of §0 that read the prompt otherwise than it was written: the parser refuses a
    missing database (1); the marker printed by `run` and kept out of `status.json` (7); test 7
    narrowed to imports (9); the worker-output beat (6);
  - the new root's count and the four roots';
  - the longest silence a healthy pilot build can have, for 04.
- **Then 04**, whose note is written after this review.
