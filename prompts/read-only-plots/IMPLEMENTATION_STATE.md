# read-only-plots campaign — implementation state

**Last updated:** 2026-10-14 · **Status: COMPLETE — 4 of 4 prompts landed (01, 02 and 03 each
reviewed; 04 closes). Final suites: CosmologyModels 18, ComputeTargets 152 (61 + 91), ObjectFactories
63, tools 34. U1–U6 and P1–P8 ruled by the user on 2026-10-13, all as proposed (README §6).**
The plan is [`README.md`](README.md), written on 2026-10-13 on branch `read-only-plots`, cut from
`main` at `17fad8b`.

The campaign makes `--ray-address` required, with `auto` refused, and opens both plot scripts on
read-only pools. Their shared store-reading setup moves into an importable module, so that a test
can drive it on a stand-in store. If U5 is so ruled, a rehearsal shows that a real plotting run
changes no file of the store it reads.

**Campaign:** [`README.md`](README.md) ·
**Code:** `config/argument_parser.py`, `main.py`, `plot_by_beta.py`, `plot_ScalarModel.py`, a new
`plot_inputs.py` ·
**Verification:** `.documents/read-only-plots-verification.md` (created by 03, if U5 is "rehearse") ·
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) §1.12

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, or an issue assigned to this campaign closes, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md)
> is updated **in the same commit**. See `CLAUDE.md`.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [Require the Ray address](01-require-the-ray-address.md) | `--ray-address` without a default; `auto` refused (U1, U2) | Sonnet | ✍️ 2026-10-13 | ✅ 2026-10-13 | `04486f4` | [`logs/01-require-the-ray-address.md`](logs/01-require-the-ray-address.md) |
| 02 | [Read-only plot pools](02-read-only-plot-pools.md) | `plot_inputs.py`; both plot scripts `read_only=True`; misses per U3, U4; closes `[08-plot-scripts-open-read-write-pools]` | Opus | ✍️ 2026-10-13 | ✅ 2026-10-13 | `9dc23db` | [`logs/02-read-only-plot-pools.md`](logs/02-read-only-plot-pools.md) |
| 03 | [Rehearse a read-only plotting run](03-rehearse-a-read-only-plotting-run.md) | one pilot build and the readers, checksums before and after (U5) | Opus | ✍️ 2026-10-13 | ✅ 2026-10-13 | `e5ef6cb` | [`logs/03-rehearse-a-read-only-plotting-run.md`](logs/03-rehearse-a-read-only-plotting-run.md) |
| 04 | [Close](04-close.md) | documents, handover, index, board | Sonnet | ✍️ 2026-10-13 | ✅ 2026-10-14 | `1905065` | [`logs/04-close.md`](logs/04-close.md) |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

**Held, by design (README §2.1).** 01–04 are written once the user has ruled on README §6.
*Written 2026-10-13 (§5.1).*

Orchestrator notes are written one prompt at a time, in [`orchestrator/`](orchestrator/), just
before each dispatch:
- [`orchestrator/prompt-01.md`](orchestrator/prompt-01.md) (`8c5a7f1`), used for 01.
- [`orchestrator/prompt-02.md`](orchestrator/prompt-02.md) (`e0cf6f0`), used for 02.
- [`orchestrator/prompt-03.md`](orchestrator/prompt-03.md) (`27f77d4`), used for 03.
- [`orchestrator/prompt-04.md`](orchestrator/prompt-04.md) (`a95658b`), used for 04.

### 1.1 Items

| Item | Prompt | What | State |
|---|---|---|---|
| **A1** | 01 | `--ray-address` with no default; `ray_address_problem()` refusing a missing value and `auto` | ✅ |
| **A2** | 01 | the check before each driver's `ray.init`, exit 2; `main.py --inventory` exempt (U1) | ✅ |
| **A3** | 01 | `ComputeTargets/tests/test_ray_address.py`: the function, the parser, the drivers by subprocess and `ast` | ✅ |
| **A4** | 01 | `CLAUDE.md`'s "Ray on this machine"; `architecture-summary.md` §5.1 | ✅ |
| **O1** | 02 | `plot_inputs.py`: the shared setup as functions of a pool; potentials and couplings one at a time (P1, P2) | ✅ |
| **O2** | 02 | a missing parameter stops the run (U4); a missing potential or coupling is skipped with a warning (U3) | ✅ |
| **O3** | 02 | both plot scripts on `plot_inputs.py`, `read_only=True`; `--drop` refused (P3) | ✅ |
| **O4** | 02 | `ComputeTargets/tests/test_plot_inputs.py`, on read-only stand-in stores; every file unchanged | ✅ |
| **O5** | 02 | `[08-plot-scripts-open-read-write-pools]` closed; the text tests (P6) | ✅ |
| **H1** | 03 | one pilot build under real Ray | ✅ |
| **H2** | 03 | the readers: every store file byte-identical; `histories.csv` equal to R0's | ✅ |
| **H3** | 03 | under real Ray: `auto` refused, a missing `T_stop` stops, `--drop` refused | ✅ |
| **H4** | 03 | `.documents/read-only-plots-verification.md` §1 | ✅ |
| **Z1** | 04 | `architecture-summary.md`: `plot_inputs.py` and read-only plot pools | ✅ |
| **Z2** | 04 | `CLAUDE.md` | ✅ |
| **Z3** | 04 | the handover, `.documents/review-remediation-verification.md` §4.13 | ✅ |
| **Z4** | 04 | the board closed; `prompts/INDEX.md`; `.documents/OPEN_ISSUES.md` | ✅ |

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| Ray address | every driver refuses a run that starts Ray without `--ray-address`, and refuses `auto`, before `ray.init` | 01 | ✅ `04486f4` |
| Read-only plots | both plot scripts open `read_only=True`; on a stand-in store their lookups find every row, skip a missing potential or coupling, stop on a missing parameter, and change no file | 02 | ✅ `9dc23db` |
| R | a real plotting run on a real store changes no file of it (U5) | 03 | ✅ `e5ef6cb` |

## 3. Active and unresolved issues

None opened by this campaign.

### 3.1 Assigned to this campaign

None open. `[08-plot-scripts-open-read-write-pools]`, assigned to 02, was closed by 02 on
2026-10-13 (`9dc23db`): it is on the [`dk-and-rpk-migration`](../dk-and-rpk-migration/IMPLEMENTATION_STATE.md)
board §4, with its **Resolved** line, and its row has left `.documents/OPEN_ISSUES.md` §1.12.

## 4. Resolved issues

None.

## 5. Dispatch record

### 5.0 Planning (2026-10-13)

Planned at the user's request, after `dk-and-rpk-migration` closed and its branch was merged
(`78769a0`). The user had already ruled that the science rebuild waits for `jordan-normalization`,
so this campaign does not touch it.
- **The measurement** (README §0.3) was read-only at `17fad8b`. A subagent read both plot scripts,
  their helpers, every factory's `build` and the package at `v0.3.0`; the orchestrator checked the
  load-bearing lines.
- **Rows 1–9 can be refused under `read_only=True`.**
  - Rows 1–5, 8 and 9 are the parameters; each is found if the plot run is given the build's
    options.
  - Row 6 is the potentials, over every stored M × Λ, which `main.py` never stored as a product.
  - Row 7 is the couplings: a β whose coupling an interrupted build never stored.
- **The history lookups can never be refused**: their `build`s return an unavailable object on a
  miss.
- **Precedent.** StochasticInstantons made `--ray-address` required at its `d6d550b`, and
  SecondaryGWKit assigned the same change to its `handover` campaign (its `0fbbc4e`).
- **The issue.** `[08-plot-scripts-open-read-write-pools]` is assigned to 02 (P7). Its row moves to
  a new `.documents/OPEN_ISSUES.md` §1.12. The count stays 27.

### 5.1 Ruled, and prompts 01–04 written (2026-10-13)

The user accepted U1–U6 and P1–P8 as proposed:
- **U1:** `--ray-address` required whenever Ray starts; `main.py --inventory` exempt.
- **U2:** `auto` refused.
- **U3:** a missing potential or coupling skipped with a warning.
- **U4:** a missing parameter row stops the run.
- **U5:** rehearse (03).
- **U6:** the package's refusal of another `VERSION_LABEL` accepted.

The user also kept `CLAUDE.md`'s "Ray on this machine" rule, added by `dk-and-rpk-migration`'s
orchestrator. Prompts 01–04 were written the same day, from the measurement of README §0.3 and a
read of the drivers' start-up code at `bacca64`.
- **The store fixture.** `StoreFixtures/standin_pool.py` patches `ray.get`, so a module that calls
  `ray.get` runs inside it. Its `open_pool` takes `read_table_config=` and `read_only=` as keyword
  arguments.
- **The rehearsal scripts.** `build.sh` and `run_readers.sh` read `REHEARSAL_ROOT`, so 03 runs them
  unchanged under `read-only-plots/`.

### 5.2 After prompt 01 (2026-10-13)

Prompt 01 landed in `04486f4` ([log](logs/01-require-the-ray-address.md)). A1–A4 done; the "Ray
address" gate holds.
- `--ray-address` has no default; `config.argument_parser.ray_address_problem` refuses `None`, an
  empty or blank string and `auto` (any case); each driver calls it just before `ray.init` and
  exits 2 with the message on stderr. `main.py --inventory` exits earlier and needs none (U1).
- `ComputeTargets/tests/test_ray_address.py` adds 17 tests (10 without a subprocess, 7 by
  subprocess, about 30 s): ComputeTargets 114 -> 131. The other roots are unchanged (18, 63, 34).
  The three parse-only tests needed no change.
- The four deliberate breakages each fail a named new test (log).
- No issue opened; `.documents/OPEN_ISSUES.md` unchanged.

**Orchestrator review of prompt 01 (2026-10-13).** Against the note's ten checks, at `04486f4`:
1. **Scope.** The diff is the prompt's §5 list exactly; the untracked set is the user's, unchanged.
2. **The function and the parser, by reading.** As A1 says; `DEFAULT_RAY_ADDRESS` is gone; the help
   text names `127.0.0.1:6380` and `local`, and says `auto` is refused.
3. **Probe.** `None`, `""`, `"   "`, `auto`, `Auto`, `"\tAUTO\n"` refused with the one message;
   `local`, `127.0.0.1:6380`, `ray://127.0.0.1:10001` accepted, and `"local "` passed on unchanged
   for Ray to judge, as the prompt specifies. `--ray-address=`, `--ray-address auto` and a YAML
   `ray-address: auto` each reach the function and are refused.
4. **`ast` against `8c5a7f1`.** Each driver's module body differs by the extended import and the
   two statements of the check (`_ray_problem = …`, `if _ray_problem is not None:` printing to stderr
   and `sys.exit(2)`), immediately before `ray.init`; `git diff -U0` adds only the comment.
   `create_argument_parser` differs only in the `--ray-address` statement.
5. **The drivers by hand**, with `RAY_ADDRESS=127.0.0.1:9`: all three, with no address, `auto`,
   `AUTO` and `""`, exit 2 with the message and no traceback, and create no file. `main.py
   --inventory` without the switch exits 1 with the reader's refusal of the absent store; with
   `--drop bbn-data` it exits 2 with the `--drop` refusal.
6. **No Ray.** `pgrep` empty after checks 5, 7 and 10; `/tmp/ray/session_*` still 160;
   `/tmp/ray-cpbh` unchanged.
7. **Breakages.** The log's four diffs replayed in bash, 2–4 without the subprocess tests: each
   fails exactly the tests the log names, and each reverts to a clean tree. Every subprocess test
   asserts the exact message and the provenance line, so an `argparse` exit 2 cannot pass it.
8. **Prose.** `architecture-summary.md:451` only; `CLAUDE.md` inside "Ray on this machine" only.
   Residue: the new sentence broke the paragraph's wrap past 100 columns; re-wrapped in the
   follow-up, wording unchanged.
9. **Records.** Row 01, A1–A4, the gate and §5.2 as required; §3 and the index untouched (27).
10. **Suites** at `04486f4`, serially, at load 66–128: CosmologyModels 18 OK, ObjectFactories 63 OK,
    tools 34 OK, ComputeTargets 61 OK + 70 with one failure:
    `test_prym_passenger.test_a_oscillating_case_completes` took 77.0 s against its 60 s wall bound
    (one PRyMordial solve, "about 9 s" by its docstring). It imports nothing 01 changed. Re-run
    alone, it took 61.4 s at load 75 (fail) and 57.7 s at load 65 (OK), and the agent's run at the
    same commit passed it at load 48. It is a timing bound under this machine's load, not a
    regression; recorded here, not opened. ComputeTargets 131. `black --check` clean on the five
    Python files; the drivers byte-compile.

Handed to 02: every driver run needs `--ray-address`; 02 must leave the check and `ray.init` where
`TestCheckPlacementByAst` expects them.

### 5.3 After prompt 02 (2026-10-13)

Prompt 02 landed in `9dc23db` ([log](logs/02-read-only-plot-pools.md)). O1–O5 done; the
"Read-only plots" gate holds.
- **`plot_inputs.py`** (new, top level): `parameters`, `stored_grid`, `potentials`, `couplings`,
  `models`, `T_stop`, and the exception `PlotOptionNotStored`. It parses nothing and starts no Ray
  at import. Each potential and coupling is looked up on its own (P2); each lookup's payload is the
  scripts' old one, key for key (log, the payload table).
- **The misses.** A parameter row not stored (rows 1–5, 8, 9) raises `PlotOptionNotStored`, naming
  the option or constant and the value (U4). A potential (`ReadOnlyMiss`) or a coupling
  (`ReadOnlyWrite`, "an insert" of `ExponentialCoupling`) not stored is skipped, with one warning
  line each and a count; the rest are returned in the old order (U3). Nothing else is caught.
- **The scripts.** Both call `plot_inputs` in place of their setup, open `read_only=True` (other
  pool arguments unchanged), catch `PlotOptionNotStored` around the setup and exit 2, and refuse
  `--drop` before 01's check (P3); `plot_by_beta.py`'s `job_name` is `"plot_by_beta"`. Every
  model's `T_stop` is looked up before the baseline and the loop, so that a miss stops the run
  before any figure (log, deviation 1). The history lookups, `run_pipeline`, the baseline and the
  figures are unchanged (by `ast` against `000be0c`).
- **Tests.** `ComputeTargets/tests/test_plot_inputs.py`, 21 tests in about 4 s, on stand-in stores
  built by `main.py`'s lookups and reopened read-only: ComputeTargets 131 -> 152 (61 + 91). The other
  roots are unchanged (18, 63, 34). `test_datastore_config`'s `EXPECTED_COUNTS` gains
  `plot_inputs.py` (P6); no other existing test changed.
- **P2's cost**, stand-in, relative only: equal on test 1's store (2 potentials, 2 couplings);
  1.29x the vectorized time on 20 β x 5 M x 4 Λ (4.9 ms against 3.8 ms; 40 actor calls against 21).
- The four deliberate breakages each fail the named test (log).
- `[08-plot-scripts-open-read-write-pools]` closed on the `dk-and-rpk-migration` board §4 (its
  header: three issues stay open); `.documents/OPEN_ISSUES.md` 27 -> 26, §1.12 empty. No issue
  opened.

Handed to 03: run the scripts as 02 left them, with `--ray-address 127.0.0.1:6380`. Under real Ray
a refused lookup reaches `ray.get` as an instance of its cause's class (Ray 2.53.0,
`ray/_private/worker.py:1015-1016`, `:2978-2979`; `ray/exceptions.py:170-192`), so the same catches
apply; 03 is where that is seen.

**Orchestrator review of prompt 02 (2026-10-13).** Against the note's twelve checks, at `9dc23db`:
1. **Scope.** The diff is the prompt's §5 list exactly (nine files); the untracked set is the
   user's, unchanged, with no stray store, journal or figure.
2. **The module, by reading.** `plot_inputs.py` parses nothing and has no `ray.init` and no `args`
   parameter; imported in a fresh interpreter it leaves `ray.is_initialized()` False.
3. **The payloads.** Each moved lookup read against `000be0c`: the classes, keys and value
   expressions are the old ones, `n = 1` only for Exponential and InversePower, and an unknown
   type is still `ValueError` with the old message. The coupling's scalar sharded get sends the
   actor exactly what the old vectorized path sent per β.
4. **The misses, by reading.** Each `except` wraps the `ray.get` of one reference, and each is
   narrowed to its own lookup's refusal (`class_name`, and for the coupling `operation == "an
   insert"`). Ray 2.53.0's dual exception forwards attribute reads to the cause
   (`ray/exceptions.py:182-183`, `__getattr__`), so `e.class_name` and `e.operation` read there
   as in the stand-in. The survivors keep the old order.
5. **The scripts by `ast` against `000be0c`.** In each, the module bodies differ only in the
   docstring, the imports (`itertools` from `plot_by_beta.py`, `BaseCosmology` and
   `build_model_list` from both, `plot_inputs` added), the `--drop` `If` (before 01's check) and
   the `with` statement. Every function dumps identically. In the `with`, the pool's keywords
   differ by `read_only=True` (and `job_name` in `plot_by_beta.py`); the setup became one `try`;
   the baseline is unchanged; in the loop, `tags = []` and the `run_pipeline` call dump
   identically, and only the `T_stop` lookup left it (log deviation 1). No function reads a removed
   module global, and no name is unbound.
6. **`--drop` by hand**, with `RAY_ADDRESS=127.0.0.1:9`, with no address and with
   `--ray-address 127.0.0.1:9`: both scripts exit 2 with the `--drop` message and no traceback,
   creating nothing. `--help` does not mention that `--drop` and `--prune-unvalidated` are
   `main.py`'s; the module docstrings do, which O3 allows.
7. **The tests.** `test_plot_inputs.py` read in full: the stores are built with `main.py`'s
   payloads, closed, copied and reopened `read_only=True`; test 1 also asserts every object came
   from the store (`_deserialized`), not from an insert; test 5 hashes the primary and the three
   shards while the pool is open and after, and looks for `-journal`, `-wal` and `-shm`. Test 7,
   run against the two pre-images, fails all six subtests.
8. **The breakages.** The log's four diffs, extracted from the log and replayed in bash: each
   applies cleanly and fails exactly what the log says (14; 2 errors + 4 failures; 3; 1), and each
   reverts to a clean tree. An extra probe, the couplings read by one `ray.get` of the list, fails
   test 3 and test 5's `no_coupling` subtest.
9. **No Ray.** `pgrep` empty after checks 6, 8 and 11; `/tmp/ray/session_*` still 160;
   `/tmp/ray-cpbh` unchanged.
10. **Records.** The `dk-and-rpk-migration` entry moved whole to §4 (the old text identical, the
    **Resolved** line added) and its header says three stay open; the index 26 by `grep -c`,
    §1.12 empty, header and §1.11 prose consistent. Row 02, O1–O5, the gate and §5.3 as required.
    The log in §5.1's form, every deviation classified, the Ray lines cited.
11. **Suites** at `9dc23db`, serially, at load 4–7: CosmologyModels 18 OK, ComputeTargets 61 OK +
    91 OK (152), ObjectFactories 63 OK, tools 34 OK. `black --check` clean on the five Python
    files; the scripts and `plot_inputs.py` byte-compile.
12. **Residue, fixed here:** "this commit" → `9dc23db` (board ×4, the log's header, the
    `dk-and-rpk-migration` board's **Resolved** line; the log's prose uses stay); this note added
    to the list; the status line; `prompts/INDEX.md`'s row, which still said "0 landed" after 01.

The log's Observation 1 (`plot_by_beta.py`'s `ProfileAgent` label still says `plot_ScalarModel`)
is cosmetic and offered to the user as housekeeping; no issue opened. Nothing else found.

### 5.4 After prompt 03 (2026-10-13)

Prompt 03 landed in `e5ef6cb` ([log](logs/03-rehearse-a-read-only-plotting-run.md)). H1–H4 done;
the gate "R" holds. The record is `.documents/read-only-plots-verification.md` §1, with ten small
outputs under `.documents/read-only-plots/out/03-*`.
- **The cluster**, by `ray_cluster.sh` alone, under the shared lock (`purpose: read-only-plots 03
  (R)`), at `127.0.0.1:6380`; `stop` exit 0 in 18 s, the lock released; `ray stop` never run. Every
  Ray driver's log names 6380, never 6379. `start`'s exit status was not captured (zsh has no
  `PIPESTATUS`; log deviation 1); its code path and the `status` after it show it started.
- **H1.** `build.sh 03 reference reference`, 3 min 41 s, exit 0; 5 / 5 / 5 / 5 in each stage; R2's
  provenance line. The job name is `dk-and-rpk-migration-03-reference` (the script unchanged).
- **H2.** All five readers exit 0. The store's 17 files are byte-identical, with their sizes and
  mtimes, before and after the readers and after the refusals; no journal, WAL or SHM file.
  `compare_stores` `RESULT: equal`, bookkeeping `none`. `histories.csv` SHA-256 `d7c62c37…c522`,
  R0's; the SM baseline, the β = 1.15 Yp and D/H and the inventory are R2's. Both plot scripts
  print the read-only open line, once each.
- **H3.** `--ray-address auto` exit 2 before Ray (no new job); `--T-stop-GeV 1e-7` exit 2 with 02's
  message, through a real actor and `ray.get`, before the baseline and any figure (job
  `04000000`, its 16 actors `DEAD`); `--drop bbn-data` exit 2 before Ray (no new job).
- **Observed, not opened:** a read-only pool starts no `SerialPoolBroker` (the package's design,
  `ShardedPool.py:563`); the rehearsal made seven PRyMordial solves, not one.
- Suites before `start`: CosmologyModels 18, ComputeTargets 61 + 91 (152), ObjectFactories 63,
  tools 34, at load 2.4–6.7. The venv unchanged (`pip freeze`, 101 lines). Nothing outside `$R`
  written but the lock, `/tmp/ray-cpbh/` and the committed outputs.
- `$R` = `~/ScienceData/ChamPBH-rehearsals/read-only-plots/` is kept; removing it is the user's.
- No issue opened; `.documents/OPEN_ISSUES.md` unchanged (26).

Handed to 04: the verification document §1 is the evidence for the read-only plot pools under
Ray; the refusals' messages there are verbatim.

**Orchestrator review of prompt 03 (2026-10-13).** Against the note's eleven checks, at `e5ef6cb`:
1. **Scope.** The diff is the note's §2 list exactly (13 files: the verification document, ten
   outputs, the log, this board); no code, test or script. The ten outputs are 56 KB in all. The
   untracked set is the user's, unchanged.
2. **Nothing left running.** `ray_cluster.sh status` exit 0: the lock free, no ChamPBH process, the
   four ports free; `pgrep` empty; `/tmp/ray/session_*` still 160, `/tmp/ray/ray_current_cluster`
   absent. The log shows `start`'s output, the lock's owner file, `ray status`, and `stop`'s exit 0
   in 18 s. No `ray stop` was run.
3. **The address.** `grep -l 6379` over `$R/logs/`, `03-readers/*.log` and `03-refusals/*.log`
   finds nothing. Each Ray driver's log names `127.0.0.1:6380`. The `auto` and `--drop` logs have
   no Ray line; the `auto` one names 6380 only inside 01's message.
4. **The build.** `** driver exit status 0`. `stage_counts.py`, re-run, gives 5 / 5 / 5 / 5 in each
   stage, with 0 skipped. The provenance line is the first line, once, and is R2's. No `Traceback`.
5. **The bytes, independently.** `shasum -a 256` and `stat -f '%m %z %N'` of the 17 files,
   re-taken after the stop, equal the committed before-table (`03-checksums.txt`, `cmp` exit 0 for
   both). Each file `cmp`s equal to its `03-readers/before/` copy, which was taken with `cp -p`
   before any reader. No `-journal`, `-wal` or `-shm` under `$R`.
6. **The readers.** `status.txt`: steps 0–5 exit 0.
   - `histories.csv` is `d7c62c37…c522`.
   - The SM baseline line, the reflections line, the β = 1.15 stored and `prod` figures, and the
     inventory's head and per-β value counts are R2's recorded ones.
   - `compare_stores`: `RESULT: equal`, with bookkeeping `none` (`"bookkeeping_differences": {}` in
     the JSON).
   - Both plot logs print the read-only open line once, and no read-write one.
   - No `Traceback` in any log.
7. **The refusals.** The three logs, read in full, match §0 of the note.
   - The messages are verbatim.
   - The `T_stop` run attached, opened read-only, and stopped with 02's message, with no baseline
     line and no figure; its output directory is absent.
   - `ray list jobs` shows four jobs, all `SUCCEEDED`, and no actor is `ALIVE`.
   - **The note was wrong in one place:** it expected a `SerialPoolBroker` in the `T_stop` job.
     A read-only pool creates none (`datastorekit/SQL/ShardedPool.py:562-563` at `v0.3.0`: "no
     broker … `serial_broker=None`"). The agent recorded this correctly as its Observation 1.
8. **Nothing outside `$R`.** `pip freeze` now equals the agent's before and after lists (101
   lines). The mtimes and sizes of the user's pilot store, taken before dispatch, are unchanged,
   and the untracked set is unchanged.
9. **The document.** §1 holds everything H4 lists. Its commands are verbatim, its figures match
   the outputs, and it is dated and additive.
10. **The records.** Row 03, H1–H4, the gate and §5.4 are as required. `.documents/OPEN_ISSUES.md`
    is untouched (26). The log is in §5.1's form, with every deviation classified. Deviation 1
    (`start`'s exit status lost to zsh's lack of `PIPESTATUS`) is covered by `status` straight
    after and by the script's code path; it is not a gap in the rehearsal.
11. **The suites** were not re-run: the commit touches no code, and the agent's run before `start`
    passed with 02's counts (18, 61 + 91, 63, 34) at loads 2.4–6.7.

**Residue, fixed here:**
- "this commit" → `e5ef6cb` on this board (×3), in the log's header and in the verification
  document §1.7. The log's prose uses stay.
- This note added to the list.
- The status line.
- `prompts/INDEX.md`'s row.

The log's Observation 5 (shards 4–14 fall into two byte-identical groups) is about how the package
lays out shards that hold only replicated rows. It does not bear on H2, and nothing was opened.
Nothing else found.

### 5.5 After prompt 04 (2026-10-14)

Prompt 04 landed in `1905065` ([log](logs/04-close.md)). Z1–Z4 done; **the campaign is closed**,
4 of 4 prompts landed, no issue opened, none open. It added no code and no test.
- **Z1** `.documents/architecture-summary.md`: the tree gains `plot_inputs.py`; a dated note
  (2026-10-14) after the `science-readiness` note says what the module holds, that both plot
  scripts open read-only, what a parameter miss and a potential or coupling miss each do, and that
  `--drop` is refused; §4.2 says the plot scripts' pools are read-only; §5.1's `--prune-unvalidated`
  and `--drop` lines say they are `main.py`'s. The 11 earlier dated notes are byte-identical.
- **Z2** `CLAUDE.md`, "Repository mechanics" only: one bullet on the plot scripts, and the
  ComputeTargets counts corrected to 152 and 91 (the orchestrator's addition) with the date of the
  measurement.
- **Z3** `.documents/review-remediation-verification.md` §4.13: what the campaign did, what it
  supersedes in §4.12 by statement, every call in the two run scripts by line (two would now refuse
  to start; `--inventory` would not), the rule that the plot loop must be given the build's options,
  and the φ*-and-C5 dependence.
- **Z4** this board; `prompts/INDEX.md` (8 campaigns: 0 planned, 0 live, 8 closed); and
  `.documents/OPEN_ISSUES.md` (header; 26 open, by counting).

**Left to the user:**
- the merge of `read-only-plots` into `main` (P5); 04 does not merge;
- 03's store, `~/ScienceData/ChamPBH-rehearsals/read-only-plots/`, kept; removing it is the
  user's (README §3);
- the run scripts' lines (§4.13 point 1): `--ray-address 127.0.0.1:6380` on `run()`'s `main.py`
  call and on the plot line, and the cluster started and stopped with `ray_cluster.sh`;
- the science rebuild, held until `jordan-normalization` lands (the user's ruling of 2026-10-13).

**Recorded and not opened** (README §1, "Out of scope"; they hold in both open modes):
- with `tags=[]`, two validated rows for one model that differ only by tag raise
  `MultipleResultsFound`;
- `AdiabaticHistory.build` and `BBNData.build` do not filter on `validated`
  (`dk-and-rpk-migration` log 09, Observation 3).

Offered at 02's review as housekeeping and not done: `plot_by_beta.py`'s `ProfileAgent` label still
reads `plot_ScalarModel` (log 02, Observation 1; cosmetic).

**Orchestrator review of prompt 04 (2026-10-14).** Against the note's nine checks, at `1905065`:
1. **Scope.** `git diff --name-status`: `A` for the log, `M` for exactly the note's six files. The
   untracked set is the user's 23 paths, unchanged; no other commit, no branch.
2. **Additive.** `review-remediation-verification.md` 83 / 0, all between §4.12's last line and the
   `---`; `OPEN_ISSUES.md` 4 / 2, the header only; `architecture-summary.md` 28 / 3, the §4.2 line
   (re-wrapped) and §5.1's two lines (amendment 2).
3. **The dated notes**, extracted at both commits by the orchestrator's own script: 11 → 12, the 11
   present byte for byte, the one added dated and naming the campaign.
4. **`architecture-summary.md`, by reading.** The tree line, §4.2 and §5.1 are right against the
   code. **One error, fixed here:** the new note's list of parameters that stop a run left out the
   constant cosmology (`QCD_Cosmology`, `plot_inputs.models`) and the cosmology's `T_CMB`, which is
   what `T_stop` looks up when `--T-stop-GeV` is not given (`plot_inputs.T_stop`). Both are U4 stops;
   §4.13 point 2 already names them. Two lines of the note now do too.
5. **`CLAUDE.md`.** One bullet ("The plot scripts …"), true of the code; 114 → 152, 53 → 91, the date
   2026-10-14; "Ray on this machine" unchanged.
6. **§4.13 against the scripts.** Every line it cites read in both scripts: `run()`'s `main.py` call
   (`:23-24` / `:24-26`) and the plot loop (`:54-57` / `:56-59`) exit 2 for want of `--ray-address`
   (01's check precedes `ray.init` in all three drivers; `main.py:101` returns `--inventory` before
   it at `:129-133`), and `set -euo pipefail` with `tee` ends the script there. The supersessions are
   all present by statement and §4.12 is unedited. The options rule, the φ*-and-C5 dependence and the
   rebuild held until `jordan-normalization` are stated. `main.py:1182` stores the potentials over
   that run's `M_grid × Lambda_grid`, so with the YAML's one Λ none is skipped, as point 3 says.
7. **Nothing run or copied.** Both scripts' SHA-256, size and mtime are those recorded before dispatch
   (`0b27c02e…ac0d`, 3076 B, 2026-10-03 00:15; `2c70e4e3…90ed`, 3122 B, 22:55); neither is tracked.
8. **The records.** The board COMPLETE with the final suites, row 04, Z1–Z4 and §5.5 as the note
   asks, including log 02's `ProfileAgent` label. `prompts/INDEX.md` 0 / 0 / 8, the row complete, no
   other row changed. `OPEN_ISSUES.md` 26 by `grep -c`, the header's count, date and clause right.
9. **The suites** at `1905065`, serially: CosmologyModels 18 OK, ComputeTargets 61 OK + 91 OK,
   ObjectFactories 63 OK, tools 34 OK, at load 6–39 (another session's load, not Ray: no Ray process,
   the lock free).

**Residue, fixed here:** "this commit" → `1905065` on this board (×2), in the log's header and in
`prompts/INDEX.md`'s row (the log's prose use stays); the note's two lines (check 4). The status
line and the notes' list were already right. Nothing opened. The merge of `read-only-plots` into
`main` (P5) is the user's to ask for.
