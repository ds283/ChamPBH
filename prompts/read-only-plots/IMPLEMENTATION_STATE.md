# read-only-plots campaign — implementation state

**Last updated:** 2026-10-13 · **Status: LIVE — 4 of 4 prompts written, 1 of 4 landed (01,
reviewed). U1–U6 and P1–P8 ruled by the user on 2026-10-13, all as proposed (README §6).**
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
| 02 | [Read-only plot pools](02-read-only-plot-pools.md) | `plot_inputs.py`; both plot scripts `read_only=True`; misses per U3, U4; closes `[08-plot-scripts-open-read-write-pools]` | Opus | ✍️ 2026-10-13 | ⬜ | — | — |
| 03 | [Rehearse a read-only plotting run](03-rehearse-a-read-only-plotting-run.md) | one pilot build and the readers, checksums before and after (U5) | Opus | ✍️ 2026-10-13 | ⬜ | — | — |
| 04 | [Close](04-close.md) | documents, handover, index, board | Sonnet | ✍️ 2026-10-13 | ⬜ | — | — |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

**Held, by design (README §2.1).** 01–04 are written once the user has ruled on README §6.
*Written 2026-10-13 (§5.1).*

Orchestrator notes are written one prompt at a time, in [`orchestrator/`](orchestrator/), just
before each dispatch:
- [`orchestrator/prompt-01.md`](orchestrator/prompt-01.md) (`8c5a7f1`), used for 01.

### 1.1 Items

| Item | Prompt | What | State |
|---|---|---|---|
| **A1** | 01 | `--ray-address` with no default; `ray_address_problem()` refusing a missing value and `auto` | ✅ |
| **A2** | 01 | the check before each driver's `ray.init`, exit 2; `main.py --inventory` exempt (U1) | ✅ |
| **A3** | 01 | `ComputeTargets/tests/test_ray_address.py`: the function, the parser, the drivers by subprocess and `ast` | ✅ |
| **A4** | 01 | `CLAUDE.md`'s "Ray on this machine"; `architecture-summary.md` §5.1 | ✅ |
| **O1** | 02 | `plot_inputs.py`: the shared setup as functions of a pool; potentials and couplings one at a time (P1, P2) | ⬜ |
| **O2** | 02 | a missing parameter stops the run (U4); a missing potential or coupling is skipped with a warning (U3) | ⬜ |
| **O3** | 02 | both plot scripts on `plot_inputs.py`, `read_only=True`; `--drop` refused (P3) | ⬜ |
| **O4** | 02 | `ComputeTargets/tests/test_plot_inputs.py`, on read-only stand-in stores; every file unchanged | ⬜ |
| **O5** | 02 | `[08-plot-scripts-open-read-write-pools]` closed; the text tests (P6) | ⬜ |
| **H1** | 03 | one pilot build under real Ray | ⬜ |
| **H2** | 03 | the readers: every store file byte-identical; `histories.csv` equal to R0's | ⬜ |
| **H3** | 03 | under real Ray: `auto` refused, a missing `T_stop` stops, `--drop` refused | ⬜ |
| **H4** | 03 | `.documents/read-only-plots-verification.md` §1 | ⬜ |
| **Z1** | 04 | `architecture-summary.md`: `plot_inputs.py` and read-only plot pools | ⬜ |
| **Z2** | 04 | `CLAUDE.md` | ⬜ |
| **Z3** | 04 | the handover, `.documents/review-remediation-verification.md` §4.13 | ⬜ |
| **Z4** | 04 | the board closed; `prompts/INDEX.md`; `.documents/OPEN_ISSUES.md` | ⬜ |

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| Ray address | every driver refuses a run that starts Ray without `--ray-address`, and refuses `auto`, before `ray.init` | 01 | ✅ `04486f4` |
| Read-only plots | both plot scripts open `read_only=True`; on a stand-in store their lookups find every row, skip a missing potential or coupling, stop on a missing parameter, and change no file | 02 | ⬜ |
| R | a real plotting run on a real store changes no file of it (U5) | 03 | ⬜ |

## 3. Active and unresolved issues

None opened by this campaign.

### 3.1 Assigned to this campaign

- `[08-plot-scripts-open-read-write-pools]` (on the [`dk-and-rpk-migration`](../dk-and-rpk-migration/IMPLEMENTATION_STATE.md)
  board §3, which carries the **Assigned** line): to 02.

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
