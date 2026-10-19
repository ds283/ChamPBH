# Log 03 — the wrapper

**Subject:** Add the registered-run wrapper for main.py and its tests
**Commit:** `d5dd930`, on branch `rrk-adoption`, on top of `06ae4bf`
**Date:** 2026-10-19, by Claude Opus 5.5
**Result:** **COMPLETE.** W1–W6 shipped. A new test root, `RunDrivers/tests`, of **61** tests,
run three times in a row, OK each time. CosmologyModels 18, ComputeTargets 176 (61 + 115),
ObjectFactories 78 and tools 84 unchanged. All six deliberate breakages caught. No §4 stop
condition was met. No issue opened. `main.py`, the plot scripts and `utilities.py` are unchanged
since `3cd2be4`.

The orchestrator's note for 03 (§0, eleven rulings) governed where it differs from the prompt; each
ruling is classified below as it says.

## What shipped

- **W1 — the package.** `RunDrivers/__init__.py` (the licence header and a one-line docstring),
  `RunDrivers/tests/__init__.py` (the licence header), and `RunDrivers/registered_run.py`, run from
  the root as `PYTHONPATH=. ./venv/bin/python RunDrivers/registered_run.py --campaign C --prompt N
  --register SLUG --purpose "…" -- <main.py's arguments>`. It puts `REPO_ROOT` first on `sys.path`
  as `tools/surfing_scan.py` does, so it also runs without `PYTHONPATH` (the refusal tests run it
  so). `REPO_ROOT = Path(__file__).resolve().parents[1]`, `MAIN_PY = REPO_ROOT / "main.py"`. The
  module docstring says what it changes (nothing of `main.py`), what the heartbeat means, the
  signals, when there is no marker, the refusals and the usage.
- **The module's public names:**
  - `REPO_ROOT`, `MAIN_PY`: the root and the program run in production;
  - `Refusal(RuntimeError)`: a check that stops the driver before anything is registered, and
    `begin`'s own refusal; printed by `run` as one `!! ` line, exit 2;
  - `EXIT_MARKER = "** driver exit status"`: `build.sh`'s marker;
  - `BEAT_MIN_INTERVAL = 30.0`; `NOTIFY_FLOOR = 300.0` (`main.py`'s `MIN_NOTIFY_INTERVAL`);
    `HEARTBEAT_MEANS` (a format string, below); `KILLED_SIGNALS = (SIGTERM, SIGINT)`;
    `SCOPE_MAX_VALUES = 6`; `USAGE_TAIL`;
  - `StageTracker`: the stage, read off the run's own lines (SecondaryGWKit's three patterns,
    unchanged);
  - `RegisteredStream`: the pass-through proxy with its copy and, on stdout, the beat; `write`,
    `writelines`, `flush`, `mid_line`, `stop_beating`, `close_copy`;
  - `terminal_state(code)`: the state for a `SystemExit` no signal caused;
  - `SignalLatch`, `make_killed_handler`, `install_killed_handler`, `finish_after_exec`:
    SecondaryGWKit's signal design, carried over;
  - `parse_arguments(argv)`: the split at the first `--`, both parses and the refusals before
    `begin`; returns `(args, main_args, parsed)`;
  - `describe_scope(parsed)`: the manifest's `scope`;
  - `register(args, parsed, argv, *, program=MAIN_PY, runs_root=None)`: `begin`, the pre-Ray
    handler `terminal`, the two proxies and the three registration lines;
  - `main(argv=None, *, program=MAIN_PY, runs_root=None)`: the work, and the tests' seam;
  - `exit_status(code)`: Python's own status for `SystemExit(code)`;
  - `run(argv=None, *, program=MAIN_PY, runs_root=None)`: the script's entry; the `__main__` block
    is `run()` alone.
- **W2 — the flags and the refusals, all before `begin`, each one `!! ` line on stderr, exit 2.**
  The four flags are required and checked non-empty after `strip()`. `argv` is split at its first
  bare `--`; no `--` at all is refused (unless `-h`/`--help` is among the wrapper's own arguments,
  which prints its help). Both parsers have `error` replaced on the instance by a function that
  raises `Refusal`, so a missing flag, an unknown option, a bad value and a missing `--database` are
  each one line. Refused after the parse: an empty database, `--inventory` (U2), and an address
  `ray_address_problem` refuses, with its leading `!! ` removed. `main.py`'s `--drop` and other
  checks are not repeated. `-- --help` prints `main.py`'s help and exits 0, and nothing is
  registered.
- **W3 — registration.** `register` calls `runregistrykit.begin(campaign, prompt, slug=--register,
  purpose, argv=[<the wrapper's path>, *argv], script=str(program), results=os.path.abspath(<the
  database>), scope=describe_scope(parsed), heartbeat_means=HEARTBEAT_MEANS.format(interval=30,
  notify=300, stale=runregistrykit.DEFAULT_STALE_AFTER), root=runs_root,
  client_root=str(REPO_ROOT))`. A `ConfigError` is re-raised; any other `RuntimeError` becomes
  `Refusal(str(e)) from e`. Then `terminal` for `SIGTERM` and `SIGINT`, the stdout proxy (beats,
  `BEAT_MIN_INTERVAL`, a `StageTracker`), the stderr proxy (copies, never beats), each copied into
  the run directory's `stdout.log` and `stderr.log`, and three lines: the run id, the manifest's
  path, and `./venv/bin/python -m runregistrykit list`. Each proxy holds a `threading.RLock`
  around its bookkeeping (`_observe`) only; the real write is outside it, first and unconditional.
- **W4 — Ray and the signals.** `main` replaces `ray.init` by `_registered_ray_init(*init_args,
  **init_kwargs)`, which calls the real `ray.init` with `main.py`'s arguments unchanged and then
  `install_killed_handler(run, latch)`. No address is changed.
- **W5 — the program and the finish.** `sys.argv = [str(program), *main_args]`, then
  `runpy.run_path(str(program), run_name="__main__")` in a `try` whose one handler is `except
  BaseException as exc: finish_after_exec(run, exc, latch, ray.shutdown); raise`, and
  `finish_after_exec(run, None, latch, ray.shutdown)` after it. `finish_after_exec` is
  SecondaryGWKit's: `alarm(0)` first; `done`/0, `terminal_state(code)`, `failed`/1, each with
  `fingerprint=True`; after a signal, `killed`/`128 + signum` with the fingerprint and
  `SystemExit(128 + signum)`. `run` calls `main`; prints a `Refusal` as one `!! ` line (status 2);
  in a `finally`, stops every proxy beating and then prints `** driver exit status N`, on a line of
  its own, with the status the process will exit with; closes the copies on the clean path only;
  then exits with that status or re-raises `main.py`'s own exception.
- **W6 — the tests**: 61 in six modules, and one non-test helper, `RunDrivers/tests/stubbed_run.py`
  (below). `CLAUDE.md`: the test command for the new root with its count (`# 61 tests`), the counts'
  parenthesis, and the sentence on `RunDrivers/` in "Repository mechanics".

**`HEARTBEAT_MEANS` as committed** (the format string; the manifest holds it formatted):

```
"Refreshed when the run writes a line to stdout, at most once per {interval:.0f} s, and the "
"line it came from is the `stage` in status.json. RayWorkPool prints a progress line only "
"after ray.wait has returned completed work, and in this repository no more often than every "
"{notify:.0f} s (main.py's MIN_NOTIFY_INTERVAL), so a fresh heartbeat from it means work "
"completed within about five minutes. Ray also forwards the workers' own output to the "
"driver's stdout, so a worker's line beats too, without moving the stage. It does NOT mean "
"any Ray actor is progressing, and the converse does not hold: a stage that prints nothing "
"for longer than the staleness window ({stale:.0f} s, `list --stale-after`), such as a queue "
"whose items each take longer than that, is reported stale while it works. Stale means go "
"and look."
```

Formatted, as a stubbed run's manifest holds it:

> Refreshed when the run writes a line to stdout, at most once per 30 s, and the line it came from
> is the `stage` in status.json. RayWorkPool prints a progress line only after ray.wait has
> returned completed work, and in this repository no more often than every 300 s (main.py's
> MIN_NOTIFY_INTERVAL), so a fresh heartbeat from it means work completed within about five
> minutes. Ray also forwards the workers' own output to the driver's stdout, so a worker's line
> beats too, without moving the stage. It does NOT mean any Ray actor is progressing, and the
> converse does not hold: a stage that prints nothing for longer than the staleness window (900 s,
> `list --stale-after`), such as a queue whose items each take longer than that, is reported stale
> while it works. Stale means go and look.

**The pilot's `scope`** (04's arguments, `TestScope.test_the_pilots_scope`, a literal):

```
config exponential.yaml; potential Exponential; β 1.1, 1.15, 1.2, 10, 25; M 0.001 Mp; Λ 0.001 eV; φ* 5 Mp; 16 shards; job rrk-adoption-04-reference
```

(Λ comes from `exponential.yaml`'s `Lambda-values-eV: 1E-3`; the file's `M-values-Mp: 0.5` is
overridden by the command line, as `configargparse` does.)

**The tests** (all in `RunDrivers/tests/`; the counts and times are each module's own run):

| Module | Tests | Time | What |
|---|---|---|---|
| `test_registered_run.py` | 14 | 2.2 s | registration in-process: the manifest before the program's first line; `script`, its hash, `results`, `scope`, `git_head` `"unknown"`, `packages`; `heartbeat_means`; `argv`; the three printed lines; the program's `argv`, `__name__`, `__file__`; `ray.init`'s arguments; the handler after `ray.init`; `done` at its last stage; `register(program=MAIN_PY)` (U3); the pilot's scope and a long one; `ConfigError` and `RuntimeError` from `begin` |
| `test_registered_run_streams.py` | 13 | 0.9 s | the beat with a fake clock (patched `time.monotonic`); the stages; `main.py`'s banners; stderr does not beat; `stop_beating`; every write passed through and copied; a failing copy; a failing heartbeat; a line from a second thread; two threads; `mid_line`; the attributes |
| `test_registered_run_signals.py` | 12 | 2.3 s | the handler on a real `Run` (`killed`/143, no fingerprint); a second signal; `SIGINT`; a failing write; once decided; `install`; after a signal, `killed` whatever arrives, with the fingerprint; the alarm first; no signal, every outcome; **one subprocess**, a real `SIGTERM` |
| `test_registered_run_outcomes.py` | 14 | 14.0 s | by subprocess: the four outcomes; the marker after an unfinished line; `begin`'s refusal, `end`, and the same registration accepted; by the real script: nine refusal tests (the four flags as subtests), and `--help` twice |
| `test_registered_run_fingerprint.py` | 2 | 0.6 s | 02's golden store: with a sidecar, `written` and the golden digest; without, `not written…` and `done` |
| `test_registered_run_source.py` | 6 | 0.04 s | by `ast`: no driver imports the registry or the wrapper; the `run.finish(` calls (4 outside, 2 inside); `begin`'s two handlers; the `ray.init` wrapper; no `ray` at module scope; `NOTIFY_FLOOR` = `main.py`'s `MIN_NOTIFY_INTERVAL` |

By name:
- `test_registered_run`: `TestRegistration` (`test_the_manifest_is_written_before_the_programs_first_line`,
  `test_the_manifest_records_the_program_the_store_the_scope_and_the_packages`,
  `test_heartbeat_means_states_the_floor_the_throttle_the_workers_and_stale`,
  `test_the_manifests_argv_is_the_wrappers`, `test_it_prints_the_run_id_the_manifest_and_the_list_command`);
  `TestTheProgram` (`test_it_runs_as_main_from_its_own_file_with_main_pys_arguments`,
  `test_ray_init_gets_the_programs_arguments_unchanged`,
  `test_the_wrappers_handler_replaces_rays_after_ray_init`, `test_a_returning_program_is_done_at_its_last_stage`);
  `TestRegisterInProduction` (`test_the_script_is_main_py_with_its_hash_and_this_repositorys_head`);
  `TestScope` (`test_the_pilots_scope`, `test_more_than_six_values_read_as_a_count_and_a_range`);
  `TestBeginsErrors` (`test_a_config_error_is_not_a_refusal`, `test_a_runtime_error_is_a_refusal_with_its_text`).
- `test_registered_run_streams`: `TestTheBeat` (`test_lines_closer_than_thirty_seconds_beat_once`,
  `test_a_banner_changes_the_stage_at_once`, `test_the_progress_and_completion_stages`,
  `test_main_pys_own_banners_are_stages`, `test_stderr_does_not_beat`,
  `test_a_stream_that_has_stopped_beating_still_copies`); `TestThePassThrough`
  (`test_every_write_reaches_the_real_stream_unaltered_and_the_copy`,
  `test_a_copy_that_fails_costs_neither_the_line_nor_the_beat`, `test_a_heartbeat_that_fails_costs_no_line`,
  `test_a_line_from_another_thread_reaches_both_and_beats`, `test_two_threads_writing_at_once_lose_no_line`,
  `test_it_knows_when_the_last_write_left_a_line_unfinished`, `test_attributes_are_the_real_streams`).
- `test_registered_run_signals`: `TestTheHandler` (`test_on_a_real_run_it_records_killed_143_at_once_without_a_fingerprint`,
  `test_a_second_signal_is_ignored_with_a_line`, `test_sigint_gives_130`,
  `test_a_write_that_fails_does_not_stop_the_raise`, `test_once_the_outcome_is_decided_a_signal_is_only_noted`);
  `TestInstall` (`test_it_takes_sigterm_and_sigint`); `TestAfterASignal`
  (`test_it_is_killed_143_with_the_fingerprint_whatever_reaches_the_driver`,
  `test_on_a_real_run_the_fingerprint_is_taken_and_the_state_stays_killed`, `test_the_alarm_is_cancelled_first`);
  `TestWithNoSignal` (`test_every_outcome_is_recorded_with_the_fingerprint`,
  `test_a_shutdown_that_fails_does_not_replace_the_exception`); `TestARealSigterm`
  (`test_it_is_killed_143_and_exits_143_with_the_marker_last`).
- `test_registered_run_outcomes`: `TestTheOutcomes` (`test_each_outcome`, four subtests);
  `TestTheMarkerIsALineOfItsOwn` (`test_after_an_unfinished_line`); `TestBeginsRefusal`
  (`test_refused_then_accepted_after_end`); `TestTheRefusalsBeforeBegin` (`test_each_of_the_four_flags_missing`,
  `test_an_empty_flag`, `test_no_bare_double_dash`, `test_no_database`, `test_an_empty_database`,
  `test_an_unrecognised_main_py_option`, `test_inventory`, `test_ray_address_auto`, `test_no_ray_address`);
  `TestHelp` (`test_the_wrappers_help`, `test_main_pys_help_registers_nothing`).
- `test_registered_run_fingerprint`: `TestTheFingerprintAtFinish`
  (`test_with_a_sidecar_the_golden_fingerprint_is_written_to_it`,
  `test_without_one_it_is_not_written_and_the_run_is_still_done`).
- `test_registered_run_source`: `TestMainPyRunsWithoutTheRegistry`, `TestTheFinishCalls`,
  `TestBeginsHandlers`, `TestTheRayInitWrapper` (two), `TestTheNotifyFloor`.

**The helper, `stubbed_run.py`.** Run as `python -u -m RunDrivers.tests.stubbed_run RUNS_ROOT PROGRAM
-- <the wrapper's arguments>` from the root, it patches `ray.init` (a stand-in that installs a
Ray-like `SIGTERM` handler, `sys.exit(signum)`) and `ray.shutdown`, and calls `registered_run.run`;
it ends with `ray.is_initialized()` false, or says so and exits 99. As a module it holds the
stand-in program (written into the test's temporary directory; its first statement lists the
manifests under the runs root; it records `sys.argv`, `__name__`, `__file__` and the `SIGTERM`
handler before and after its `ray.init(address=…)`; it prints RayWorkPool's banner, progress and
completion lines with RayWorkPool's format strings and timestamps; then it returns, exits 2 or 1,
raises, waits after its banner, or ends on an unfinished line), `StringWriter`, `Recorder`,
`TakeOver` (saves and restores `sys.stdout`, `sys.stderr`, `sys.argv` and both handlers, closing
any proxy's copies), `in_process` (the cleanup registered before the call; what was taken over is
observed before restoring), `reap`, and the subprocess launchers.

**The rules held.** No test starts Ray: `ray.init` and `ray.shutdown` are stand-ins, patched before
the wrapper wraps them, and every test case's `tearDown` asserts `ray.is_initialized()` false (the
fingerprint module through `_TempStoreCase`'s). Every run directory is in a temporary runs root,
and the one store is 02's golden store in `_TempStoreCase`'s `tempfile` directory. The real script
runs only for refusals before `begin` and for `--help`.

## Deviations from the prompt

1. **`STRUCTURALLY REQUIRED`: a missing database is refused by `main.py`'s parser, not by
   `main.py`** (the orchestrator's note for 03, ruling 1). W2's reason (help and a bare
   `sys.exit()`, recorded `done`) does not hold: `--database` is `required=True`
   (`config/argument_parser.py:56-62`), and the parse exits 2. The refusal stands because the parse
   is the wrapper's, before `begin`.
   **`IMPLEMENTATION CHOICE`** (ruling 1): `error` is replaced on the parser instance by a function
   that raises `Refusal(f"main.py's arguments: {message}")`, so a missing database, an
   unrecognised option and a bad value are each one `!! ` line, exit 2; an empty `--database ""` is
   refused after the parse; `-- --help` prints `main.py`'s help, exit 0, nothing registered.
2. **`IMPLEMENTATION CHOICE`: the wrapper's own flags are split by hand and refused in one line**
   (ruling 2): split at the first `--`; the wrapper's parser has `error` replaced the same way; each
   flag checked non-empty after `strip()`. One addition of mine: with no `--`, `-h`/`--help` among
   the wrapper's arguments still prints its help (exit 0), so that `registered_run.py --help` works
   as §2.2 asks; anything else with no `--` is the no-`--` refusal.
3. **`IMPLEMENTATION CHOICE`: `RAY_ADDRESS_PROBLEM`'s own `!! ` is removed** (ruling 3); the refusal
   tests assert `!! ` and not `!! !!`.
4. **`IMPLEMENTATION CHOICE`: the manifest's `argv` is `[<the wrapper's path>, *argv]`** (ruling 4),
   `argv` being the wrapper's own arguments.
   **`STRUCTURALLY REQUIRED`: one more test calls `register` alone with `program=MAIN_PY`**
   (ruling 4; the stand-in cannot show U3), `TestRegisterInProduction`: `script == "main.py"`, its
   SHA-256, `git_head` equal to `git rev-parse HEAD`. It observes the streams and handlers before
   restoring them.
5. **`IMPLEMENTATION CHOICE`: the stand-in records `sys.argv`, `__name__` and `__file__`** (ruling 5),
   in `<program>.json`; `TestTheProgram` asserts them.
6. **`IMPLEMENTATION CHOICE`: a re-entrant lock around each proxy's bookkeeping, and the workers in
   `heartbeat_means`** (ruling 6), with the second-thread test; I added a two-thread test of 400
   lines (one `write` per line: `print`'s two writes can interleave between threads on any
   stream).
7. **`IMPLEMENTATION CHOICE`: `main` is the work and `run` the script's entry** (ruling 7): the
   refusal print, the marker after `stop_beating`, the copies closed on the clean path only, the
   exit or the re-raise. The status for the marker: 0 for a return; `exit_status(code)` for a
   `SystemExit` (`None` 0, an `int` itself, else 1); 2 for a `Refusal`; 1 for any other exception.
   **One addition of mine**: a `KeyboardInterrupt` reaching `run` gives 130, the status the
   interpreter's own exit by `SIGINT` gives, not 1. It can reach `run` only before registration;
   afterwards `SIGINT` is the wrapper's.
8. **`IMPLEMENTATION CHOICE` (mine): the marker starts a line of its own.** `RegisteredStream` keeps
   `mid_line` (the last write ended without a newline), and `run` prints a newline before the marker
   when it is set. Found on the first replay of breakage B4: a `SIGTERM` the test sent as soon as it
   read the banner landed while the stand-in was still printing, between `print`'s text and its
   newline, and the marker joined the unfinished line (`…40.00% complete** driver exit status
   143`). The handler runs between any two bytecodes of the main thread, so the same can happen to
   `main.py`; `interrupt_when.py` reads the marker with `startswith`. Tested by
   `test_after_an_unfinished_line` and `test_it_knows_when_the_last_write_left_a_line_unfinished`.
   The "wait" stand-in prints only its banner before it waits, so that the real-`SIGTERM` test
   interrupts no `print` half-way and does not depend on timing.
9. **`STRUCTURALLY REQUIRED`: which tests run in a subprocess** (ruling 8): the outcomes, the real
   `SIGTERM` and `begin`'s refusal through `stubbed_run` (`-u`); the refusals before `begin` and
   `--help` by the real script, with no `PYTHONPATH`; everything else in-process. `var/` is
   asserted unchanged (exists or not, and `var/runs`' listing), not absent (02's accepted choice).
10. **`IMPLEMENTATION CHOICE`: test 7 is an `ast` test of imports, not a pin of bytes** (ruling 9).
    Byte-identity since `3cd2be4` is the `git diff --stat` below (empty).
11. **`IMPLEMENTATION CHOICE`: test 5 uses 02's golden store by import** (ruling 10):
    `_GoldenStoreCase`, `GOLDEN` and `without_taken`, nothing else; the sidecar by
    `create_sidecar(primary, purpose=…, client_root=REPO_ROOT)` before the run.
12. **`IMPLEMENTATION CHOICE`: `scope` is the arguments as given** (ruling 11), as the pilot's literal
    above. Ranges read as their arguments as parsed (`β 0.1–3.0, 5 per unit`; `M 10^25–10^26.5
    eV, 6 per decade`); values as `:g`.
13. **`IMPLEMENTATION CHOICE`: refusals exit 2**, as ChamPBH's drivers refuse; SecondaryGWKit's wrapper
    exits 1 (the prompt's W2).
14. **`IMPLEMENTATION CHOICE`: `sys.argv = [str(program), *main_args]`**, where the prompt writes
    `[str(MAIN_PY), …]`. In production `program` is `MAIN_PY`, so they are the same; in a test the
    stand-in sees its own path, which `runpy.run_path` sets as `sys.argv[0]` during the run in any
    case.
15. **`IMPLEMENTATION CHOICE`: `NOTIFY_FLOOR = 300.0` is a constant of the wrapper**, since `main.py`
    is never imported; `TestTheNotifyFloor` reads `main.py`'s `MIN_NOTIFY_INTERVAL` by `ast` and
    checks they agree. The 900 s in `heartbeat_means` is the package's `DEFAULT_STALE_AFTER`.
16. **`IMPLEMENTATION CHOICE`: the `__init__.py` files carry the licence header** (the note's "Each
    Python file"); SecondaryGWKit's are empty. The package's holds the one-line docstring W1 asks for.
17. **`IMPLEMENTATION CHOICE`: six test modules and one helper**, all matching
    `test_registered_run*.py`; the helper holds both the subprocess entry and the shared fixtures, so
    that there is one non-test module, as the note allows. Tests beyond the prompt's list: both
    `--help` tests, the missing `--ray-address`, the empty database and empty flag, the two-thread
    test, the mid-line pair, the `ray.init`-wrapper and module-scope `ast` checks, and the notify
    floor.

No `UNINTENDED DRIFT`.

**The `ast` count (test 8)** is the note's: 4 `run.finish(` outside a handler, each passing
`fingerprint=True` (`finish_after_exec`: done, `terminal_state`, failed, killed), and 2 inside
(`terminal`, `killed_handler`), neither passing it.

**`runpy.run_path` and `main.py` (§4's first stop condition, checked and not met).** `main.py` reads
no `__file__`, `__name__` or `sys.argv` of its own (its parser reads `sys.argv`, which is set),
installs no handler, defines no class and no `@ray.remote` function (its lambdas run on the driver),
and calls `ray.init(address=args.ray_address)` as an attribute of `ray` (`:133`), so the wrapper's
replacement is seen. `runpy.run_path(…, run_name="__main__")` runs it as `__main__`, with
`sys.modules["__main__"]` its namespace for the run and `sys.argv[0]` its path. Ray adds
`dirname(realpath(sys.argv[0]))` to the workers' path when it is on the driver's: that is the root,
which the wrapper puts on `sys.path`.

## Verification performed

**Before any edit** (at `06ae4bf`, serially; scratch `agent03/`, `uptime` beside each; wall time
the runner's):

| Suite | Before | After |
|---|---|---|
| CosmologyModels | 18 OK, 78 s, load 6.7 | 18 OK, 67 s, load 6.9 |
| ComputeTargets `test_[a-f]*` | 61 OK, 97 s, load 25.9 | 61 OK, 100 s, load 5.0 |
| ComputeTargets `test_[g-z]*` | 115 OK, 105 s, load 12.9 | 115 OK, 105 s, load 6.3 |
| ObjectFactories | 78 OK, 11 s, load 12.7 | 78 OK, 8 s, load 4.8 |
| tools | 84 OK, 17 s, load 17.0 | 84 OK, 16 s, load 4.7 |
| **RunDrivers** (new) | — | **61 OK**, three runs in a row: 20 s (load 4.5), 20 s (3.8), 21 s (4.8) |

`ls -d var` after the suites: no such directory. `pgrep -lf 'gcs_server|raylet|ray::'`: nothing.

```
$ ./venv/bin/black --check RunDrivers/__init__.py RunDrivers/registered_run.py RunDrivers/tests/*.py
All done! ✨ 🍰 ✨
10 files would be left unchanged.

$ ./venv/bin/python RunDrivers/registered_run.py --help ; echo "exit $?"     # (tail)
form: registered_run.py --campaign C --prompt N --register SLUG --purpose TEXT
-- <main.py's arguments>
** driver exit status 0
exit 0

$ ./venv/bin/python -m runregistrykit list ; echo "exit $?"
no runs under /Users/ds283/Documents/Code/ChamPBH/var/runs
exit 0

$ git diff --stat 3cd2be4 -- main.py plot_by_beta.py plot_ScalarModel.py utilities.py
(empty)
```

(`--help` was run with `./venv/bin/python` and no `PYTHONPATH`; `TestHelp` runs it the same way.)

**One stubbed run, by hand**, on the pilot's arguments, in the scratch directory (`python -u -m
RunDrivers.tests.stubbed_run <scratch>/runs <scratch>/standin_return.py -- --campaign rrk-adoption
--prompt 03 --register probe --purpose … -- --config exponential.yaml --database <scratch>/store.db
--shards 16 --job-name rrk-adoption-04-reference --M-values-Mp 1e-3 --beta-values 1.1 1.15 1.2 10
25 --ray-address 127.0.0.1:6380`): exit 0; stdout ended `** driver exit status 0`; the manifest's
`scope` was the pilot's literal; `packages` datastorekit 0.4.0, raypoolkit 0.2.0, runregistrykit
0.2.0; `status.json` `done`/0, stage `CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE GRID, all work
items complete`, and a `fingerprint_error` saying the primary does not exist (there was none).

**`begin`'s refusal**, as printed (the dead run made as the test makes it):

```
!! Cannot begin run rrk-adoption-03-resume-20261019T225637 with results "<scratch>/store.db": run example-01-hard-killed-20261019T225636 is running (stale, pid 18530) and names this store by its results. A stale run is ended by a person, with `python -m runregistrykit end RUN_ID --reason TEXT`, before another run is begun on its store; the registry does not decide that a run is dead. No run directory was created
** driver exit status 2
```

**RunRegistryKit behaved as its README says** for `begin` (the refusal, nothing created), `finish`
(the state written whatever the fingerprint did; `written` / `not written: …`) and `end` (`lost`,
then `begin` accepted). No stop.

### §2.3 — the beat's timing, on paper

From `raypoolkit/RayWorkPool.py:340-360` and `:560-610`, and `main.py`'s queues:
- **What prints, and when.** A titled queue prints its banner, `\n** {title}`, when it is built
  (`:206`). Each pass of its loop submits up to `create_batch_size` items and then blocks in
  `ray.wait(…, num_returns=min(process_batch_size, inflight))`. After the batch is processed, a
  progress notification (two lines, `:588` and `:592`) is printed only if more than
  `notify_min_time_interval` has passed since the last one (or the queue's start) **and** either
  more than `notify_time_interval` has passed or more than `notify_batch_size` items were processed.
  The completion line (`:606`) is printed when the queue ends, whatever the time.
- **`main.py`'s settings.** Its three titled queues, `CALCULATE SCALAR FIELD HISTORIES FOR SAMPLE
  GRID` (`:326`), `CALCULATE ADIABATIC TRANSGRESSION PARAMETERS` (`:585`) and `CALCULATE BBN DATA`
  (`:872`), each set `notify_min_time_interval=MIN_NOTIFY_INTERVAL` (300 s), `notify_batch_size=2`,
  `process_batch_size=3`, `create_batch_size=2`, `max_task_queue=24`, and leave
  `notify_time_interval` at its default, 300 s. Both time conditions are then 300 s, so **a progress
  notification is printed at the first `ray.wait` return more than 300 s after the last one**: never
  sooner, and later by however long that wait takes (until 3 of the in-flight lookup, compute, store
  or validate items complete, or all of them if fewer are in flight). Its other queues have
  `title=None` and print nothing. `main.py` itself prints `>> packages…` (`:95`), `** BUILDING GRID
  OF MODELS TO SAMPLE` and its `--` lines (`:1080-1191`), `>> RUNNING PIPELINE FOR MODEL …`
  (`:168`), and the three `-- <class>:` summaries; DatastoreKit prints `>> Created sharded
  datastore …` at the pool's open; Ray forwards the workers' lines.
- **So, per stage:** a stage shorter than 300 s prints its banner and its completion line and
  nothing from RayWorkPool between; a longer one prints a notification every 300 s plus one `ray.wait`.
- **The pilot** (04's grid, five histories): the most recent pilot log on this machine,
  `~/ScienceData/ChamPBH-rehearsals/read-only-plots/logs/03-reference.log` (2026-10-13, read only),
  shows `CALCULATE SCALAR FIELD HISTORIES` complete in 2m 12s, the adiabatic queue in 3.59 s and the
  BBN queue in 24.1 s. Every stage is under 300 s, so **no progress line is printed at all**: each
  queue gives its banner and its completion line. **The longest silence a healthy pilot build can
  have is the scalar-history stage, about 2¼ minutes** at that load (anything printed by a worker in
  it shortens it). Since a banner and a completion line each change the stage and beat at once, and a
  line arriving 30 s or more after the last beat beats, the heartbeat's age in a healthy pilot stays
  under that silence plus 30 s, about 2¾ minutes: far inside the 900 s window. A pilot that `list`
  shows stale is not healthy.
- **For longer builds** (not measured here): the gap between notifications is 300 s plus one wait
  for 3 completions. With `jordan-normalization`'s histories of 1.4–263 s today, 3–7 integrations
  each once Λ and φ_ref are tuned, a wait can pass 900 s, and the run then shows stale while it
  works, as `heartbeat_means` says (P4).

## The deliberate-breakage record

Each diff is replayable with `git apply` from a clean tree at `d5dd930`; each was checked with
`git apply --check` before applying and `git apply -R --check` once applied, and replayed in `bash`
(`replay.sh` in the scratch directory: apply, run `RunDrivers/tests`, reverse). None is committed.
Every other test of the root passed under each. The tests named are of `RunDrivers/tests`.

### B1 — the handler installed before the wrapped `ray.init` only, not after

61 run, failures=3:
- `test_registered_run.TestTheProgram.test_the_wrappers_handler_replaces_rays_after_ray_init`:
  `'ray_like_sigterm_handler' != 'make_killed_handler.<locals>.killed_handler'`;
- `test_registered_run_signals.TestARealSigterm.test_it_is_killed_143_and_exits_143_with_the_marker_last`:
  exit **15** `!= 143` (the stand-in's Ray-like `sys.exit(15)` ran, and `terminal_state(15)` made it
  `killed`/15);
- `test_registered_run_source.TestTheRayInitWrapper.test_it_passes_the_arguments_unchanged_then_installs_the_handler`:
  `0 != 1 : the wrapper does not install the handler`.

```diff
diff --git a/RunDrivers/registered_run.py b/RunDrivers/registered_run.py
--- a/RunDrivers/registered_run.py
+++ b/RunDrivers/registered_run.py
@@ -817,7 +817,6 @@
 
     def _registered_ray_init(*init_args, **init_kwargs):
         context = _real_ray_init(*init_args, **init_kwargs)
-        install_killed_handler(run, latch)
         return context
 
     ray.init = _registered_ray_init
```

### B2 — `fingerprint=True` passed by the handler

61 run, failures=10:
- `test_registered_run_source.TestTheFinishCalls.test_handlers_pass_no_fingerprint_and_every_later_finish_passes_it`:
  `True is not false : [True, False]`;
- `test_registered_run_signals.TestTheHandler.test_on_a_real_run_it_records_killed_143_at_once_without_a_fingerprint`:
  `'fingerprint_error' unexpectedly found` in the status (the handler took a fingerprint with the
  pool open);
- `TestTheHandler.test_a_second_signal_is_ignored_with_a_line` and `test_sigint_gives_130`, and the six
  subtests of `TestAfterASignal.test_it_is_killed_143_with_the_fingerprint_whatever_reaches_the_driver`:
  the first call is `('killed', 143, True)`, not `('killed', 143, False)`.

```diff
diff --git a/RunDrivers/registered_run.py b/RunDrivers/registered_run.py
--- a/RunDrivers/registered_run.py
+++ b/RunDrivers/registered_run.py
@@ -427,7 +427,7 @@
         code = 128 + signum
         if run is not None:
             try:
-                run.finish("killed", exit_code=code)
+                run.finish("killed", exit_code=code, fingerprint=True)
             except Exception as e:  # noqa: BLE001
                 _say(stream, f"** {name}: could not record killed at once: {e!r}")
         _say(
```

### B3 — the wrapper exits 0 whatever `main.py` exited with

61 run, failures=4:
- `test_registered_run_outcomes.TestTheOutcomes.test_each_outcome`, subtests `exit-2`, `exit-1` and
  `raise`: exit `0 != 2`, `0 != 1`, `0 != 1`;
- `test_registered_run_signals.TestARealSigterm.test_it_is_killed_143_and_exits_143_with_the_marker_last`:
  `0 != 143`.

```diff
diff --git a/RunDrivers/registered_run.py b/RunDrivers/registered_run.py
--- a/RunDrivers/registered_run.py
+++ b/RunDrivers/registered_run.py
@@ -876,14 +876,12 @@
         _say(sys.stderr, f"!! {exc}")
         raise SystemExit(2) from None
     except SystemExit as exc:
-        status = exit_status(exc.code)
-        raise
+        status = 0
     except KeyboardInterrupt:
         status = 128 + int(signal.SIGINT)
         raise
     except BaseException:
-        status = 1
-        raise
+        status = 0
     finally:
         for proxy in _proxies():
             proxy.stop_beating()
```

### B4 — the stdout proxy drops a line when the copy fails

Read concretely as the note says: the real write moved inside the `try`, after the bookkeeping, and
the copy's own guard removed, so that a failing copy raises out of `_observe` and the swallowed
exception takes the line with it. 61 run, failures=2:
- `test_registered_run_streams.TestThePassThrough.test_a_copy_that_fails_costs_neither_the_line_nor_the_beat`:
  the real stream is `''`, not the banner;
- `TestThePassThrough.test_a_heartbeat_that_fails_costs_no_line`: the banner is lost, only the
  worker's line is there.

(On its first replay, before deviation 8, B4 also failed the real-`SIGTERM` test: the signal landed
mid-`print`, the bookkeeping raised `SystemExit` before the real write, and the marker joined an
unfinished line. That replay is what found deviation 8.)

```diff
diff --git a/RunDrivers/registered_run.py b/RunDrivers/registered_run.py
--- a/RunDrivers/registered_run.py
+++ b/RunDrivers/registered_run.py
@@ -234,13 +234,14 @@
         return getattr(self._stream, name)
 
     def write(self, text):
-        written = self._stream.write(text)
+        written = None
         if text:
             self._mid_line = not text.endswith("\n")
         # the output is the run's; the bookkeeping is ours, and never costs it an exception
         try:
             with self._lock:
                 self._observe(text)
+            written = self._stream.write(text)
         except Exception:  # noqa: BLE001
             pass
         return written
@@ -292,10 +293,7 @@
         if self._copy is not None:
             # a copy that has failed once is dropped rather than retried, and must not cost the
             # stage tracking or the heartbeat below
-            try:
-                self._copy.write(text)
-            except Exception:  # noqa: BLE001
-                self._copy = None
+            self._copy.write(text)
         if not self._beats:
             return
         self._pending += text
```

### B5 — `script` recorded as the wrapper instead of the program

61 run, failures=2:
- `test_registered_run.TestRegistration.test_the_manifest_records_the_program_the_store_the_scope_and_the_packages`:
  `'RunDrivers/registered_run.py' != '<tmp>/standin_return.py'`;
- `test_registered_run.TestRegisterInProduction.test_the_script_is_main_py_with_its_hash_and_this_repositorys_head`:
  `'RunDrivers/registered_run.py' != 'main.py'`.

```diff
diff --git a/RunDrivers/registered_run.py b/RunDrivers/registered_run.py
--- a/RunDrivers/registered_run.py
+++ b/RunDrivers/registered_run.py
@@ -742,7 +742,7 @@
             slug=args.register,
             purpose=args.purpose,
             argv=[str(Path(__file__).resolve()), *argv],
-            script=str(program),
+            script=__file__,
             # the store the run's results live in, which the registry names and never writes; not
             # `checkpoint`, which is a unit ledger this run does not have
             results=os.path.abspath(parsed.database),
```

### B6 — the `ConfigError` caught as a refusal

61 run, failures=2:
- `test_registered_run.TestBeginsErrors.test_a_config_error_is_not_a_refusal`:
  `Refusal('no runregistrykit.toml was found') is not ConfigError('no runregistrykit.toml was found')`;
- `test_registered_run_source.TestBeginsHandlers.test_the_one_begin_call_has_the_two_handlers_in_order`:
  `['RuntimeError'] != ['runregistrykit.ConfigError', 'RuntimeError']`.

```diff
diff --git a/RunDrivers/registered_run.py b/RunDrivers/registered_run.py
--- a/RunDrivers/registered_run.py
+++ b/RunDrivers/registered_run.py
@@ -755,8 +755,6 @@
             root=runs_root,
             client_root=str(REPO_ROOT),
         )
-    except runregistrykit.ConfigError:
-        raise
     except RuntimeError as e:
         raise Refusal(str(e)) from e
 
```

## Observations not acted on

- **`main.py`'s provenance line is a stage.** `>> packages: datastorekit … ; raypoolkit …` (`:95`)
  and DatastoreKit's `>> Created sharded datastore "…" with N shards` match `_STAGE_BANNER`, so each
  is the stage until `** BUILDING GRID OF MODELS TO SAMPLE`. They do not break the patterns (the
  stage is the line it came from), so the patterns are carried over unchanged.
- **RayWorkPool's "remaining" counts items not yet submitted.** Its progress line prints
  `len(self._todo)`, the items not yet created, so the stage's `(3 of 5 work items remaining)` and
  its percentage say how much has been *submitted*, and `100.00% complete (may be waiting for
  compute/store/validate tasks to finish)` can stand for a long time. That is RayPoolKit's wording,
  read and not changed; 04 should read the stage with it in mind.
- **`build.sh` appends its marker with `echo >>`** after the driver exits, so a driver killed
  mid-`print` would leave it on an unfinished line there too (deviation 8's case). The rehearsal
  scripts are out of scope (prompt §5); the wrapper's own marker is guarded.
- **`RunDrivers/` is on `sys.path`.** Python puts the script's directory first when the wrapper runs;
  the wrapper puts the root before it. Nothing in the tree imports a top-level `tests`,
  `registered_run` or `stubbed_run` (checked by `grep`), and Ray gives the workers only the
  directory of `sys.argv[0]` (the root, while `main.py` runs) and the working directory.
- **`terminal`, SecondaryGWKit's pre-Ray handler, does not guard its `run.finish`**: a status write
  that raised would leave the process running with the handler having returned. Carried over
  unchanged, as the prompt asks.

## State handed to the next prompt

- Branch `rrk-adoption`; `d5dd930` is on top of `06ae4bf`. Tree clean of tracked changes; the 22
  untracked entries are the user's; no `var/`.
- `RunDrivers/registered_run.py` launches a registered `main.py` run:
  `PYTHONPATH=. ./venv/bin/python RunDrivers/registered_run.py --campaign C --prompt N --register
  SLUG --purpose "…" -- <main.py's arguments>`, from the root. It registers before Ray (manifest
  `script: "main.py"`, `results` the absolute database, `scope` from the arguments,
  `heartbeat_means` as above), beats from stdout at most every 30 s, installs its signal handler
  after `ray.init`, records `done`/`failed`/`killed` with the fingerprint after `main.py` returns,
  prints `** driver exit status N` last, on a line of its own, and exits with `main.py`'s status
  (143 after a `SIGTERM` once Ray is up). Refusals before `begin` exit 2 with one `!! ` line and
  create nothing.
- **For 04.** A `SIGTERM` or `SIGINT` before `ray.init` returns is recorded `killed` by `terminal` and
  the process dies by the default disposition, with no marker; after it, `killed`/`128 + signum`
  and the marker. A `SIGKILL` leaves the run `running` and no marker. The pilot prints no progress
  line (every stage is under 300 s), so its stages are banners and completions, and its longest
  healthy silence is the scalar-history stage, about 2¼ minutes (§2.3). The wrapper does not change
  the address: pass `--ray-address 127.0.0.1:6380`. The handler's behaviour on Ray 2.53 attached to
  an external cluster is 04's to measure (P5).
- Suites: CosmologyModels 18, ComputeTargets 176 (61 + 115), ObjectFactories 78, tools 84,
  RunDrivers 61. `CLAUDE.md` gives the RunDrivers count; the ObjectFactories `# 69` is 05's.
