# Prompt 03 — the wrapper

**Campaign:** [`README.md`](README.md) · **Board items:** **W1**–**W6** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-19, after the user's rulings (README §6; P1 amended: the wrapper lives in
`RunDrivers/`). Its orchestrator note, written after 01's and 02's reviews, hands it their counts and
governs where it differs from this file.
**Gate:**
- 01 and 02 landed and reviewed, with the gates "Pin" and "Fingerprint" holding (board §2);
- on branch `rrk-adoption`, no tracked change in `git status`;
- no ChamPBH Ray process is running. You start none, and no test of yours may start one.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Opus**. Signal handling around Ray, a stream proxy, and the order of a
run's last writes. Each was measured the hard way in SecondaryGWKit; carry the lessons over, not
the bugs.

**Read first:**

1. [`README.md`](README.md): §0.2 (what SecondaryGWKit does, and what it does that this repository
   does not need), §0.3 (`main.py`'s shape; RayWorkPool's printing), §2.2 (03's charter), §5, §6
   (R2, R3, U2, U3, P1–P7, P9).
2. SecondaryGWKit at `031bcb4` (`git -C /Users/ds283/Documents/Code/SecondaryGWKit show
   031bcb4:<path>`; read, never edit):
   - **`RunDrivers/scoped_pipeline_run.py`**, the model. Its docstring ("REGISTERING THE RUN",
     "What the heartbeat means, exactly", "SIGNALS"); `HEARTBEAT_MEANS`, `BEAT_MIN_INTERVAL` and the
     three patterns; `StageTracker`, `RegisteredStream`, `terminal_state`, `SignalLatch`,
     `make_killed_handler`, `install_killed_handler`, `finish_after_exec`, `register`; and `main()`'s
     wiring of them (its `ray.init` wrapper, the `try` around the `exec`, and the `__main__` block's
     `Refusal`);
   - `docs/a3-v2-readiness-audit.md` S1–S4: what Ray does to a driver's `SIGTERM`, measured;
   - its tests in `RunDrivers/tests/`: `test_killed_handler.py`, `test_pipeline_adoption.py`,
     `test_clean_refusals.py`, `test_hard_killed_predecessor.py`, `test_finish_calls.py`, and the
     helpers `registry_fixtures.py`;
   - its `CLAUDE.md`: the paragraph on `RunDrivers/` and the test-root list.
3. RunRegistryKit at `v0.2.0`: its `README.md`, "A run", "Ending a run whose end was never
   recorded" and "The store layer"; `begin`, `Run.heartbeat` and `Run.finish` in
   `runregistrykit/__init__.py`.
4. This repository:
   - `main.py:84-133` (`MIN_NOTIFY_INTERVAL`; the provenance line; `--inventory`; the refusals; the
     address check; `ray.init`) and `:1232-1252` (the pool);
   - `config/argument_parser.py` (`create_argument_parser`, `ray_address_problem` at `:331`);
   - `raypoolkit/RayWorkPool.py` in the venv (`:206`, `:560-600`): what it prints, and when;
   - `.documents/dk-and-rpk-migration/rehearsal/build.sh` and `interrupt_when.py`: the
     `** driver exit status` marker, and how a pid file is used;
   - `tools/tests/test_inventory_report.py` (`_InventoryStoreCase`, `build_store`) and 02's golden
     test: a closed temporary store with real rows;
   - `CLAUDE.md`'s section "Long-running jobs — the run registry", as 01 wrote it.

---

## 1. What is wanted

A `main.py` run can be launched as a registered run, with `main.py` itself unchanged and still
launchable on its own (R2). The wrapper:
1. records the run before anything starts;
2. keeps its heartbeat and stage current from `main.py`'s own output;
3. records how it ended, a signal included;
4. takes the store's fingerprint once the store is closed;
5. exits as `main.py` would have.

It is SecondaryGWKit's `scoped_pipeline_run.py`, less the three things this repository does not
need (README §0.2): the local Ray bootstrap, the source substitution and the refusal of an existing
store.

### 1.1 The items

- **W1 — the package.** `RunDrivers/__init__.py` (a one-line docstring) and
  `RunDrivers/tests/__init__.py`, after SecondaryGWKit's. The wrapper is
  `RunDrivers/registered_run.py`, run from the repository root:

  ```
  PYTHONPATH=. ./venv/bin/python RunDrivers/registered_run.py --campaign C --prompt N \
      --register SLUG --purpose "one line" -- <main.py's arguments, verbatim>
  ```

  It puts the repository root first on `sys.path` itself, as `tools/surfing_scan.py` does, so that it
  also works without `PYTHONPATH`. `REPO_ROOT` is `Path(__file__).resolve().parents[1]` and
  `MAIN_PY` is `REPO_ROOT / "main.py"`. The module docstring is this file's §1 in your words: what
  it changes (nothing of `main.py`), what the heartbeat means, the signals, and the usage.
- **W2 — its arguments and its refusals**, all before `begin()`, each a one-line `!! ` refusal on
  stderr with **exit status 2** (P2, P3, U2). ChamPBH's drivers refuse with 2; SecondaryGWKit's
  wrapper uses 1. Say so in the log, as an `IMPLEMENTATION CHOICE`.
  - `--campaign`, `--prompt`, `--register` and `--purpose` are required, each a non-empty string
    (P2). `main.py`'s arguments follow a bare `--`.
  - `main.py`'s arguments are parsed with `config.argument_parser.create_argument_parser()`, so that
    a `--database` or a `--ray-address` given in a `--config` YAML file is seen (P3). The parse is
    for reading only: `main.py` parses its own `sys.argv` again.
  - **Refused:**
    - no database: `main.py` would print its help and `sys.exit()` with no code, which would be
      recorded `done`;
    - `--inventory` (U2): it is short, read-only and starts no Ray;
    - an address that `ray_address_problem` refuses: `main.py` would refuse it after
      registration, and the run would be recorded `failed` for a typing error;
    - `main.py`'s own `--drop` and the other checks are **not** repeated: they are `main.py`'s.
      A run that `main.py` then refuses is recorded `failed` with exit code 2, which is true.
  - A `Refusal(RuntimeError)` class, printed by the `__main__` block as one `!! ` line with no
    traceback, as SecondaryGWKit's `test_clean_refusals.py` pins. A `ConfigError` from the
    package is not a refusal and keeps its traceback.
- **W3 — registration**, a function `register(...)` called before Ray and before `main.py` is read.
  It calls
  `runregistrykit.begin(campaign, prompt, slug, purpose, script=<the program>, results=<the database>, scope=…, heartbeat_means=HEARTBEAT_MEANS…, root=runs_root, client_root=REPO_ROOT)`.
  - **`script` is the program run, `main.py` in production (U3)**, so that its hash is the hash of
    what computed. `client_root=REPO_ROOT` is passed always, so that the config, the fingerprinter
    and the runs root are this repository's whatever the program's directory.
  - `results` is the database as the parser resolved it, made absolute.
  - `scope` is one line from the parsed arguments: the config file, the β and M sets (as counts and
    ranges when long), φ*, the shards and the job name. It is derived, never typed (the package's
    docstring on `scope`).
  - `heartbeat_means` is ChamPBH's version of SecondaryGWKit's sentence, and says:
    - a beat is taken from stdout, at most once every 30 s, and the line it came from is the stage;
    - `RayWorkPool` prints a progress line only after completed work, and in this repository no
      more often than every 300 s (`MIN_NOTIFY_INTERVAL`), so a fresh beat means work completed
      within about five minutes;
    - a queue whose items each take longer than the staleness window shows stale while it works:
      stale means go and look (P4).
  - `runs_root` is `None` in production, which is the config's `var/runs`. A test passes a
    temporary one.
  - A `RuntimeError` from `begin` becomes a `Refusal`, with `begin`'s text, which names
    `python -m runregistrykit end` when the store is held by a run that has not finished. Nothing is
    created. A `ConfigError` is re-raised first, since it is a `RuntimeError` at `v0.2.0`
    (SecondaryGWKit's `test_clean_refusals.py`, item 2).
  - Then the pre-Ray handler (SecondaryGWKit's `terminal`) for `SIGTERM` and `SIGINT`, and the two
    stream proxies:
    - **stdout** beats, with `BEAT_MIN_INTERVAL = 30.0`, and its stage is read by `StageTracker`
      with SecondaryGWKit's three patterns. Check them against `RayWorkPool.py:206` and `:588`, and
      against `main.py`'s own `>>` and `**` lines; the patterns are carried over unchanged unless
      a line of this repository's breaks them;
    - **stderr** is copied and never beats.

    Each is copied into the run directory's `stdout.log` and `stderr.log`. It prints the run id, the
    manifest's path and the `list` command.
- **W4 — Ray and the signals**, SecondaryGWKit's design (P5):
  - a `SignalLatch`, `make_killed_handler` and `install_killed_handler` for `SIGTERM` and `SIGINT`;
  - `ray.init` replaced, before `main.py` runs, by a wrapper. It calls the real `ray.init` **with
    the arguments `main.py` gave it, unchanged**, and installs the handler once it returns.

  The wrapper changes no address: `main.py` attaches to the cluster its `--ray-address` names.
  SecondaryGWKit's audit S1–S4 was measured on a Ray it started itself. 04 measures that Ray 2.53
  attached to an external cluster installs its handler as S1 says (P5). Until then, do not
  simplify on the assumption that it does not.
- **W5 — running `main.py` and finishing.**
  - `sys.argv = [str(MAIN_PY), *main_args]`, then `runpy.run_path(str(program),
    run_name="__main__")` (P7). Nothing in `main.py`'s text is read, substituted or compiled by the
    wrapper.
  - The `try` around it, and `finish_after_exec`, as SecondaryGWKit's, with `ray.shutdown` as the
    shutdown:
    - `alarm(0)` first;
    - then `done`/0 for a return, `terminal_state(code)` for a `SystemExit`, and `failed`/1 for any
      other exception, each with `fingerprint=True`. The fingerprint is taken only here, after
      `main.py`'s `with ShardedPool` has closed (P6), and never in a signal handler;
    - on a signal, `killed`, `128 + signum`, with the fingerprint, and `SystemExit(128 + signum)`;
    - otherwise the caller re-raises `main.py`'s own exception, so that its traceback is what it
      always was.
  - At the very end it prints `** driver exit status N`, with the status the process will exit
    with, on stdout. That is `build.sh`'s marker, so that `interrupt_when.py` and a `Monitor` on the
    log work unchanged (README §2.2, 03 item 7). A `SIGKILL` prints nothing, which is how a person
    tells it apart.
  - A seam for the tests: `main(argv=None, *, program=MAIN_PY, runs_root=None)`, so that a test runs
    a stand-in program into a temporary runs root. Production calls `main()`.
- **W6 — the tests**, `RunDrivers/tests/test_registered_run*.py` and a non-test helper module if
  you need one (as SecondaryGWKit's `registry_fixtures.py`).
  - **The rules for every test.** None starts Ray: `ray.init` and `ray.shutdown` are replaced by
    stand-ins with `unittest.mock.patch.object(ray, …)` before the wrapper wraps them, and every
    test asserts `ray.is_initialized()` is false at the end. Every run directory is in a temporary
    runs root, and every store in a `tempfile` directory. Nothing under `var/` is read or created.
  - **The stand-in program** is a small script written into the test's temporary directory. It
    calls `ray.init(address=…)` as `main.py` does, prints `RayWorkPool`'s banner and progress lines
    (copied from `RayWorkPool.py`'s format strings, with its timestamps), and then returns, exits
    with a code, raises, or waits to be signalled.
  1. **Registration:**
     - the manifest exists before the stand-in's first line runs;
     - it records `script` as the stand-in's path and hash, `results` as the absolute database,
       `scope`, and `heartbeat_means` with the 300 s floor stated;
     - its `packages` names `runregistrykit`, `datastorekit` and `raypoolkit`;
     - its `argv` is the wrapper's.
  2. **The heartbeat:**
     - lines closer together than 30 s beat once, with a fake clock (patch `time.monotonic`);
     - a banner changes the stage at once;
     - the stage reads `"<title>, 40.00% (3 of 5 work items remaining)"` for the progress line, and
       `"<title>, all work items complete"`;
     - stderr writes do not beat;
     - every write reaches the real stream unaltered, and `stdout.log` holds it.
  3. **The outcomes**, for a return, `sys.exit(2)`, `sys.exit(1)` and a `RuntimeError`:
     - the status is `done`/0, `failed`/2, `failed`/1 and `failed`/1;
     - the wrapper's own exit status is the same;
     - `** driver exit status N` is the last stdout line;
     - the `RuntimeError`'s traceback reaches `stderr.log`.
  4. **The signals:**
     - the handler, called directly with a real `Run`, records `killed`/143 at once, without a
       fingerprint, and raises `SystemExit(143)`;
     - `finish_after_exec` keeps `killed` whatever exception then arrives, `SystemExit(1)`
       included, and adds the fingerprint;
     - a second signal is ignored with a line;
     - a stand-in `ray.init` that installs its own `SIGTERM` handler, as Ray does, is followed by
       the wrapper's: after the wrapped call, `signal.getsignal(SIGTERM)` is the wrapper's;
     - **one subprocess**: the wrapper on a stand-in that waits, sent a real `SIGTERM` once its
       banner is on stdout. The status is `killed`/143 and the exit status 143. The subprocess
       stubs `ray.init` through the helper module, and is reaped in a cleanup registered before
       its first read, as SecondaryGWKit's `test_hard_killed_predecessor.py` does.
  5. **The fingerprint**, on a closed temporary store built as 02's golden test builds it:
     - with a sidecar made by `runregistrykit.stores.create_sidecar(…, client_root=REPO_ROOT)`, a
       `done` run's status has `fingerprint_sidecar: "written"`, and the sidecar's fingerprint
       equals the golden digest;
     - without one, the status says "not written", and the run is still `done`.
  6. **The refusals** (exit 2, one `!! ` line, no traceback, nothing created in the runs root):
     - each of the four required flags missing;
     - no database;
     - `--inventory`;
     - `--ray-address auto`;
     - `begin` on a store that an unfinished run in the same runs root names. After
       `python -m runregistrykit end <that run> --reason … --root <the runs root>` (by subprocess,
       on a dead child's run as SecondaryGWKit's test builds it), the same registration is
       accepted.
  7. **`main.py` is unchanged.** `git diff 3cd2be4 -- main.py plot_by_beta.py plot_ScalarModel.py
     utilities.py` is empty, and none of them imports `runregistrykit` (by `ast`). Write this as a
     test that reads the files, not as a `git` call.
  8. **By `ast`**, as SecondaryGWKit's `test_finish_calls.py`: every `run.finish(` in the wrapper
     that lies inside a signal handler passes no `fingerprint=True`, and every one after the
     program passes it.
- **The prose this commit makes false.** `CLAUDE.md`:
  - the test commands gain `PYTHONPATH=. ./venv/bin/python -m unittest discover -s RunDrivers/tests
    -t .`, with its count;
  - "Repository mechanics" says, in one sentence, that the launchers of registered runs live in
    `RunDrivers/`, with their tests in `RunDrivers/tests/`, and `tools/` keeps repository-level
    tooling (README §6.2, P1 as amended).

  Nothing else.

---

## 2. Verification

1. **Before any edit:** the four roots at the counts 02 handed on, serially, ComputeTargets in two
   halves.
2. **After:**
   - the four roots unchanged, and the new root `RunDrivers/tests` with its count;
   - `black --check` on every new file;
   - `PYTHONPATH=. ./venv/bin/python RunDrivers/registered_run.py --help`, exit 0;
   - `ray.is_initialized()` false after the whole new root, and no `var/` created by it;
   - `./venv/bin/python -m runregistrykit list` from the root: still "No runs".
3. **The beat's timing, once, on paper.** From `RayWorkPool.py:560-600` and `main.py`'s queue
   settings (`notify_min_time_interval=300`, `notify_batch_size`), say in the log how often a
   progress line can be printed in each stage. Say the longest silence a healthy pilot build could
   have, so that 04 knows what to expect.
4. **Deliberate breakages** (README §5: each recorded as a diff, never committed). Each must fail at
   least one new test, named in the log:
   - the handler installed before the wrapped `ray.init` only, not after;
   - `fingerprint=True` passed by the handler;
   - the wrapper exits 0 whatever `main.py` exited with;
   - the stdout proxy drops a line when the copy fails;
   - `script` recorded as the wrapper instead of the program;
   - the `ConfigError` caught as a refusal.

## 3. Acceptance

1. W1–W6 as §1.1 says, and `CLAUDE.md`'s two lines.
2. §2's checks in the log, each with its command and output.
3. Suites: the four roots as 02 left them, and `RunDrivers/tests` new.
4. **One commit**, with the log `logs/03-the-wrapper.md`. The board: row 03, W1–W6, the gate
   "Wrapper", and a `### 5.4 After prompt 03 (<date>)`. Write "this commit" where the SHA goes.

## 4. Stop conditions: stop and ask

- `main.py` cannot be run by `runpy.run_path` as it is: something in it depends on being `exec`'d
  differently, or on `__file__`'s value in a way `run_path` changes. Find what before stopping.
- A test cannot be written without starting Ray, or without a running cluster.
- RunRegistryKit behaves otherwise than its README says for `begin`, `finish` or `end`. That is a
  package defect, reported there, never worked around (README §5).
- A choice here would change what `main.py` computes, or would need an edit to `main.py`, the plot
  scripts or `utilities.py`.
- Anything would need an edit outside §5's list.

## 5. What this prompt does not do

- **Files it changes:**
  - `RunDrivers/__init__.py`, `RunDrivers/registered_run.py` (new);
  - `RunDrivers/tests/__init__.py`, `RunDrivers/tests/test_registered_run*.py` and any non-test
    helper there (new);
  - `CLAUDE.md` (the test command and the sentence of §1.1);
  - this board, and the log.
- It runs no real build, starts no Ray, and opens no store but its tests' temporary ones.
- It does not touch `main.py`, the plot scripts, `utilities.py`, the rehearsal scripts or the
  user's run scripts.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`.
