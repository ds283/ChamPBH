# Orchestrator — prompt 01, require the Ray address

Read [`../README.md`](../README.md) first: §0.2, §0.3 ("`--ray-address` today"), §2.2 (01's
charter), §5 and §6.1 (U1, U2). This is the campaign's first note. Its conventions are those of
`dk-and-rpk-migration`'s notes ([`../../dk-and-rpk-migration/orchestrator/prompt-01.md`](../../dk-and-rpk-migration/orchestrator/prompt-01.md)
§0, "Conventions"), restated here where 01 needs them.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`01-require-the-ray-address.md`](../01-require-the-ray-address.md)
**Board items:** A1–A4 · **Gate:** "Ray address" · **Closes:** nothing · **Opens:** only what the
work finds · **Model:** Sonnet, as the prompt recommends · **No network.**

**Gate:**
- The prompts are written (`aa2aa8a`). `HEAD` is the commit that adds this note, or a later one that
  touches no code ("code" is anything outside `prompts/`; since `17fad8b` the only change outside
  `prompts/` is `bacca64`'s `.documents/OPEN_ISSUES.md`).
- `ray_cluster.sh status`: lock free, no ChamPBH process. No Ray process on the machine at all
  (`pgrep -lf 'gcs_server|raylet'`).
- One prompt at a time in this checkout. 02 is not dispatched until 01 is reviewed.

## 0. What makes this prompt unusual

**It is small, and its tests run the drivers.** The change is one function, one parser line and
three calls. The risk is not the change but its tests: the subprocess tests start a real driver,
and a driver that gets past its check calls `ray.init`. Today that call, with the default `auto`,
scans the machine for any Ray head (`ray/_private/services.py:379-410`) and would attach to another
project's cluster, and open a pool there on the test's temporary path. With the default `None` and
the check missing, it starts a private Ray instance on Ray's default ports. So:
- **A subprocess test is never run against a tree in which any driver lacks the check.** Write the
  check into all three drivers before running them, and in the breakage replays (§2 of the prompt)
  run only the tests that do not start a driver (below).
- **Give each subprocess the environment `RAY_ADDRESS=127.0.0.1:9`** (an address nothing listens
  on, `test_inventory_report.NO_RAY`). Ray reads `RAY_ADDRESS` when the address it is given is
  `None` or `auto` (`services.py:392-397`), so a regression fails to connect to a dead address
  instead of attaching to a cluster or starting one. It is a guard, not the test: the assertions
  stay exit 2 and the message. It is the agent's implementation choice, and it may decline it with
  a reason; the first bullet holds either way.

**Exit 2 is not enough to tell the check from `argparse`.** `--database` is already `required=True`
in the parser (`main.py` with no arguments exits 2 with "the following arguments are required:
--database"), and `argparse` exits 2 on every usage error. A test that asserts only the exit code
passes when the check is missing but `--ray-address` has become a required argument. Each
subprocess test asserts the message (or a stable part of it) on stderr, and that stdout or stderr
carries the provenance line (which shows the driver got past parsing).

**What moves on purpose:**
- `config/argument_parser.py`: `DEFAULT_RAY_ADDRESS` deleted; `--ray-address` with default `None`
  and a new help text; `ray_address_problem()` added beside `create_argument_parser`.
- `main.py`, `plot_by_beta.py`, `plot_ScalarModel.py`: the import of the function, the check
  immediately before `ray.init`, and the corrected comment above `ray.init`. Nothing else.
- New: `ComputeTargets/tests/test_ray_address.py`.
- `CLAUDE.md` ("Ray on this machine"), `.documents/architecture-summary.md:451` (§5.1's line).
- The log (`logs/` exists and is empty) and the board.
- No issue is expected. The index (`.documents/OPEN_ISSUES.md`, 27 open) is untouched unless one
  opens.

**The facts, checked by the orchestrator on 2026-10-13 at `aa2aa8a`.** Pass them on.
- **The tree.** Branch `read-only-plots`, `HEAD` `aa2aa8a` before this note; nothing staged; the
  untracked set is the prompt's gate list (17 `pilot-2026.6.0*.db`, `pilot-main.log`,
  `pilot-plot.log`, `pilot-out/`, `full_run_2026.6.0.sh`, `full_run_2026.6.0-refresh.sh`,
  `prompts/jordan-normalization/`).
- **Ray.** `ray_cluster.sh status` exits 0: lock free, no ChamPBH process, ports 6380 / 6381 / 8267
  / 52367 free. `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing. `RAY_ADDRESS` is unset in the
  shell. `/tmp/ray/` holds 160 `session_*` directories (old, other projects'), and no
  `ray_current_cluster`.
- **Ray 2.53.0.** `canonicalize_bootstrap_address` (`services.py:518-546`) sends `None` and `auto`
  to `get_ray_address_from_environment`; `local` and `None`-after-lookup start a private instance.
- **The parser.** `config/argument_parser.py` (317 lines, black-clean): `DEFAULT_RAY_ADDRESS =
  "auto"` at `:11`; the switch at `:310-315`; `configargparse.YAMLConfigFileParser` (`:46-47`);
  `--config` is `is_config_file=True` (`:52-53`). Probe: a file holding `ray-address:
  127.0.0.1:6380`, given with `--config`, parses to the string `'127.0.0.1:6380'`. With
  `--database u.db` alone today: `ray_address == 'auto'`, `inventory False`, `drop []`.
- **`git grep -n "ray_address\|ray-address\|DEFAULT_RAY_ADDRESS" -- ':!prompts'`** gives 26 lines:
  the parser (`:11`, `:311`, `:312`); the three `ray.init` lines (`main.py:124`,
  `plot_by_beta.py:101`, `plot_ScalarModel.py:81`); `tools/tests/test_inventory_report.py:397`;
  `CLAUDE.md:121`; `architecture-summary.md:451` (the default, to correct) and `:868` (`ray.init`,
  still true); the rehearsal scripts (`build.sh:33`, `:39`, `:99`; `run_readers.sh:31`, `:111`,
  `:116`, `:122`; `ray_cluster.sh:105`); and dated records (`dk-and-rpk-migration-verification.md:413`,
  `:782`; `review-remediation-verification.md:1425`, `:1428`, `:1429`, `:1436`, `:1438`, `:1464`),
  which stay as they are (`CLAUDE.md` invariant 6). No module other than the parser reads
  `DEFAULT_RAY_ADDRESS`.
- **The drivers' start-up** is as the prompt says (`main.py:82-124`, `plot_by_beta.py:84-101`,
  `plot_ScalarModel.py:70-81`): parse; `if args.database is None` (dead, since `--database` is
  required); the provenance print; in `main.py` the `--inventory` branch (`:93-111`) and the
  `--drop` check (`:113-121`); the comment "defaults to 'auto' meaning a locally running cluster";
  `ray.init`. All three black-clean.
- **Pre-images** for the `ast` comparison (`git rev-parse --short=8 aa2aa8a:<path>`):
  `config/argument_parser.py` `839f4ff5`, `main.py` `8b1bcf39`, `plot_by_beta.py` `53201923`,
  `plot_ScalarModel.py` `62857177`, `CLAUDE.md` `bdcadc01`, `architecture-summary.md` `0c172ae9`.
- **Import time.** Each driver run with no arguments (so it stops at the parser) took
  `plot_by_beta.py` 6.9 s, `plot_ScalarModel.py` 5.7 s, `main.py` 4.4 s, at load 39. The six
  subprocesses of A3.3 should take well under a minute together.
- **`test_inventory_report`** runs `main.py` by `subprocess.run([sys.executable, "main.py",
  "--ray-address", NO_RAY, …], cwd=REPO_ROOT, capture_output=True, text=True, timeout=…)` (`:395-402`),
  and its absent-store test (`:410-417`) expects exit 1 and `!! --inventory: Cannot read sharded
  datastore`. That test passes `--ray-address` today; it stays unchanged and keeps passing.
- **The three parse-only tests** (`test_extraction.py:331-336`, `test_initial_field_option.py:115-118`,
  `test_pipeline_selection.py:157-162`) read `band_half_width`, `phi_init_Mp` and
  `retry_failed_bbn`, never `ray_address`, so they need no change under A1.
- **`ObjectFactories/tests/test_package_provenance.py:316-345`** asserts exactly one `ray.init(`
  per driver and the provenance print before it. The check must not add a second `ray.init` call
  or move the print.
- **The suites' split.** `test_ray_address.py` falls in the second half (`test_[g-z]*.py`), so the
  halves become 61 and 53 + the new tests.
- **The rehearsal scripts** pass `--ray-address "$ADDRESS"` at `build.sh:99` and
  `run_readers.sh:111`, `:116`, `:122` (`:111` is the `--inventory` call, which will ignore it).
- **Not touched, and staying:** every factory, `ObjectFactories/`, `StoreFixtures/`, `tools/`,
  `RayTools/`, `requirements.txt`, every existing test, the rehearsal scripts, the user's run
  scripts and pilot files, and `prompts/jordan-normalization/`.

**What the review exists to establish.**
- **(E1) The function and the parser.** `ray_address_problem` refuses `None`, empty or
  whitespace-only, and `auto` in any case with any surrounding whitespace; accepts everything else
  unchanged; returns the message, which names `127.0.0.1:6380`, `ray_cluster.sh`, `local` and why
  `auto` is refused. The parser has no default and no `DEFAULT_RAY_ADDRESS`; a YAML value supplies
  the address.
- **(E2) The drivers.** In each, the check is the last thing before `ray.init`, prints to stderr
  and exits 2; in `main.py` after the `--inventory` branch and the `--drop` check. Every other
  top-level statement unchanged but the comment.
- **(E3) The behaviour.** Each driver refuses a missing, empty or `auto` address with exit 2 and the
  message, starting no Ray and creating no file; `main.py --inventory` with none behaves exactly as
  before (exit 1 on an absent store, 0 on a real one, 2 with `--drop`).
- **(E4) The pins.** The four breakages each fail a named new test, and the tests that start a
  driver are never run on a broken tree.
- **(E5) The prose and records.** `CLAUDE.md` and §5.1's line true; the log, the board, the counts.

**Conventions.** All of these bind the agent, and the brief says so.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-only`
  before committing. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its
  follow-up.
- **Suites.** `CLAUDE.md`'s commands, ComputeTargets in its two halves, each in the foreground,
  serially, output to a file in the agent's scratch directory with the verdict grepped from it, and
  the load (`uptime`) beside each. A failure is re-run alone before it is called real.
- **Breakage diffs** are recorded so that `git apply` replays them from a clean tree at the agent's
  commit. Check each with `git apply --check` and `-R --check`, and replay in `bash`, not zsh.
  Breakages 2–4 are replayed with **only** the new module's non-subprocess tests (by class name);
  breakage 1 may run the whole module, since the check still refuses `auto`.
- **Scratch.** The agent's own subdirectory of the session scratchpad, never `/tmp`. Python's
  `tempfile` in tests is fine. No scratch file goes under `config/`, `ComputeTargets/` or
  `.documents/`.
- **The venv.** Nothing installed or uninstalled.
- **Stores.** None opened but a test's temporary one (and A3's subprocesses open none).
- **Ray.** The agent starts no Ray and runs no driver past its check. `ray stop` is never run.
- **Stop conditions are real.** A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. Review against the agent's commit.

## 1. Before you dispatch

1. **The tree.** Branch and `HEAD`; `git status --short` shows only §0's untracked set; nothing
   staged; `git log --oneline aa2aa8a..HEAD` touches no code.
2. **Ray.** `ray_cluster.sh status` and `pgrep -lf 'gcs_server|raylet|ray::'`; the count of
   `/tmp/ray/session_*` (160 at writing), for check 6.
3. **Baselines.** The four roots at `HEAD`, serially, with the load. Give the agent the numbers.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Sonnet**. Give it:
- the prompt;
- the campaign README and this board;
- `HEAD` and the baselines;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad.
  The agent does not read `orchestrator/`, and does not read prompts 02–04.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/01-require-the-ray-address.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **The files it may add or change are the prompt's §5 list, and nothing else.**
- **No subprocess test runs on a tree where a driver lacks the check**, and breakages 2–4 replay
  without them.
- **It starts no Ray, opens no real store and installs nothing.**
- **Stop and ask** on any of the prompt's §4 conditions. An existing test that fails is current
  behaviour moving, never a reason to edit it.
- **It replies with** the log's path; the message as committed; the function's accepted and refused
  values; the new tests by class, with counts and the subprocess tests' wall time; the four suites;
  every deviation with its tag; and "State handed to the next prompt", verbatim.

## 3. The review — ten checks

1. **Scope.** `git diff <agent>~1 <agent> --stat` touches only the prompt's §5 list. `git status`
   shows §0's untracked set and nothing new.
2. **E1, by reading.** The function and the parser in full; `git grep DEFAULT_RAY_ADDRESS` empty.
   The help text says required whenever Ray starts, names `127.0.0.1:6380` and `local`, and says
   `auto` is refused.
3. **E1, a probe** from the scratchpad: `ray_address_problem` on `None`, `""`, `"   "`, `"auto"`,
   `"Auto"`, `"\tAUTO\n"`, `"local"`, `"local "`, `"127.0.0.1:6380"`, `"ray://127.0.0.1:10001"`.
   The refused ones return the same message; the accepted ones `None`. The parser with
   `--ray-address=""`, with `--ray-address auto`, and with a YAML file setting `ray-address: auto`:
   each value reaches the function and is refused.
4. **E2, by `ast` against the pre-images.** For each driver, the module body differs from
   `aa2aa8a`'s by the import (or an extended import) and one `If` whose test calls
   `ray_address_problem(args.ray_address)` (or the agent's equivalent), placed immediately before
   the `ray.init` expression, whose body prints to `sys.stderr` and calls `sys.exit(2)`. Every
   other top-level statement dumps identically. In `main.py` the `If` follows the `if args.inventory:`
   block and the `--drop` `Try`. `git diff -U0` on each driver: the comment line is the only other
   change. `config/argument_parser.py`: `create_argument_parser` differs only in the switch.
5. **E3, the drivers by hand**, from the repository root, each with `--database <scratch>/absent.db`
   and `RAY_ADDRESS=127.0.0.1:9` in the environment: no `--ray-address`; `--ray-address auto`;
   `--ray-address AUTO`; `--ray-address ""`. Each exits 2 with the message on stderr and no
   traceback; the scratch directory stays empty. `main.py --inventory` without the switch: exit 1,
   `!! --inventory: Cannot read sharded datastore`; with `--drop` added: exit 2, the `--drop`
   refusal, not the address message.
6. **No Ray started.** After checks 5 and 10, `pgrep` finds nothing and `/tmp/ray/session_*`
   still counts as §1 step 2, and `/tmp/ray-cpbh` is unchanged.
7. **E4.** In `bash`, replay each breakage diff with `git apply`, run the named test (breakages
   2–4 without the subprocess tests), see it fail for the stated reason, revert with
   `git apply -R`, `git status` clean. Then read every subprocess test: each asserts the message
   on stderr as well as the exit code (§0, "Exit 2 is not enough"), so a driver that failed at
   `argparse` instead of at the check would fail it.
8. **E5, the prose.** `git diff` of `CLAUDE.md`: inside "Ray on this machine" only, saying the
   drivers refuse to start Ray without the switch and refuse `auto`, and that `--inventory` needs
   none. `architecture-summary.md`: line 451 only, no dated note touched.
9. **E5, the records.** The board: row 01 ✅ with "this commit" and the log; A1–A4 ✅; the gate "Ray
   address" ✅; `### 5.2 After prompt 01 (<date>)`. §3 and the index untouched, or, if one opened,
   the index row, count and date in the same commit. The log in §5.1's form, every deviation
   classified, the grep's before and after classified line by line.
10. **The suites** at the agent's commit, serially: 18, 61 + (53 + new), 63, 34. `black --check`
    on every touched Python file.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 01* paragraph in §5.2. Fix
  small residue in a follow-up commit of its own:
  - "this commit" → the SHA (board and log);
  - a list of the orchestrator notes on the board, with this one marked "used for 01";
  - the status line: 1 of 4 landed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:** the message as committed; what the drivers now do with a missing, empty
  or `auto` address, and `--inventory`; the tests added and the breakages; the four roots' counts;
  that the user's run scripts now need `--ray-address 127.0.0.1:6380` on every driver call but the
  `--inventory` one (04 writes the handover's lines).
- **Then 02.** Its note is written after this review.
