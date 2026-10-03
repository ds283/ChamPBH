# Log 02b — `tools/history_and_bbn.py` runs production's network

**Prompt:** prompts/bbn-tolerance/02b-history-tool-follows-production.md
**Commit:** the commit that adds this file ("Run history_and_bbn on production's network"); its SHA is in `git log`
**Model:** Claude Sonnet 5.5
**Date:** 2026-10-03
**Result:** COMPLETE WITH DEVIATIONS (one implementation choice: the tool's module-docstring usage
line, `[--small-network]` to `[--no-small-network]`). No stop condition met. The tool's
`--small-network` default is `True`; the comment in `_configure_PRyMordial` is corrected;
`test_network_flag` (c) reads the tool's default and fails on `HEAD`'s tool. Suites 18, 114, 31
before and after.

Everything was run on `9e51437` plus this prompt's uncommitted diff, the branch head at dispatch.

## What shipped

`VERSION_LABEL` stays `"2026.6.0"`; `PRYM_VERSION` stays `"bf24c3d+ri02+sr01+bt02"`. No new public
symbol.

- **`tools/history_and_bbn.py`, the `--small-network` argument (`:255–258`)**: `default=False` ->
  `default=True`; help `"run PRyMordial's 12-reaction network (default: the full network, as
  main.py)"` -> `"run PRyMordial's 12-reaction network (default: True, the small network, as
  main.py runs; --no-small-network for the full network)"`, in the words prompt 02 used for
  `tools/bbn_baseline.py`. Nothing else in the argument or in the tool's code.
- **`tools/history_and_bbn.py`, the module docstring (`:34`)**: the usage line
  `[--small-network]` -> `[--no-small-network]` (deviation 1).
- **`ComputeTargets/BBNData.py`, `_configure_PRyMordial` (`:255–259`)**: the comment "False
  (production) runs the full network" -> "True (production since bbn-tolerance prompt 02) runs the
  small network, False the full one"; the production-readiness history sentence is kept, re-wrapped
  to the file's 100 columns. Comment lines only; the code line `PRyMini.smallnet_flag =
  small_network` is untouched.
- **`ComputeTargets/tests/test_network_flag.py`, `test_c_production_defaults_are_the_small_network`**:
  the block that read `tools/bbn_baseline.py`'s `--small-network` default now loops over
  `("bbn_baseline.py", "history_and_bbn.py")` with the same `ast` reading, asserting exactly one
  `--small-network` `add_argument` call in each and `defaults == [True]`, with the tool's name as
  the assertion message. The docstring names the tool and prompt 02b. No new test; the count is
  unchanged.

## Deviations from the prompt

### 1. The module docstring's usage line — IMPLEMENTATION CHOICE

The prompt allows "the `--small-network` default and help only" in `tools/history_and_bbn.py`. The
module docstring (the tool's `--help` description source and its reference text) showed the usage
as `[--small-network]`, which with a default of `True` is a no-op flag and misleads. I changed it
to `[--no-small-network]`, one token, same file, same argument. *Alternative:* leave it and record
an observation; rejected because the flag the usage line advertises would do nothing, which is the
mistake the prompt corrects in the help. `argparse`'s own usage line (printed by `--help`) now
reads `[--small-network | --no-small-network]` regardless.

## Verification performed

- **Breakage check (ran).** With `HEAD`'s (= `HEAD~1` of this commit's) `tools/history_and_bbn.py`
  restored and everything else as in this diff, `test_c_production_defaults_are_the_small_network`
  failed on the value, not on an import: `First differing element 0: False / True`,
  `- [False] + [True] : history_and_bbn.py`. With the new tool it passes. This is the §6.2b
  witness.
- **`black --check`** on the three changed Python files: clean ("3 files would be left unchanged").
- **`tools/history_and_bbn.py --help` (ran).** The help text prints as above; the tool was not run
  on a history (no solve, no store).
- **Stop condition 1 checked.** `grep -rn history_and_bbn --include='*.py'` finds
  `tools/history_and_bbn.py` itself, `test_scalarmodel_failure_reason.py` (imports the module as
  `driver` for `LAMBDA_EV`, `T_INIT_GEV`, `PHI_INIT_MP`, `PI_INIT` and `_z_grid()`; none touches
  the network) and a docstring mention in `test_fixed_T_values.py`. No test other than
  `test_network_flag` (c) depends on the default.
- **Suites** (`PYTHONPATH=. ./venv/bin/python -m unittest discover -s <package>/tests -t .`, from the
  repository root, on `9e51437` plus the diff):

  | package | before | after |
  |---|---|---|
  | CosmologyModels | 18, OK | 18, OK |
  | ComputeTargets | 114, OK | 114, OK |
  | Datastore | 31, OK | 31, OK |

  The prompt and README §6.2b quote 113 for ComputeTargets; that is the count after prompt 02.
  Prompt 01b added a test, so the count at dispatch was 114 (board header; measured before my
  edits). "Counts unchanged" is what holds.
- No store opened, no PRyMordial solve beyond what the suites already run.

## Observations not acted on

None new. `tools/history_and_bbn.py`'s `network` label (`:372`) and the rest of the tool follow
`args.small_network` and need no change.

## State handed to the next prompt

- **`tools/history_and_bbn.py`** now runs the small network by default, as `main.py` does;
  `--no-small-network` selects the full network. Its output line `bbn ...: network=small|full`
  reports which. `tools/bbn_from_store.py` keeps the full network as its default (P15); a
  reproduction command of log 01 or 01c must still pass `--small-network` explicitly where it needs
  the small one.
- **All production defaults now follow `main.py`:** `compute_BBN_data`, `BBNData.compute`'s
  fallbacks, `plot_by_beta.py`'s SM baseline, `tools/bbn_baseline.py` and
  `tools/history_and_bbn.py`, held together by `test_network_flag` (c).
- **Prompt 03** need not mention this prompt beyond the handover's list of what changed. The
  comment in `_configure_PRyMordial` now says production runs the small network.
- **Suites after this prompt:** CosmologyModels 18, ComputeTargets 114, Datastore 31.
- `VERSION_LABEL` `"2026.6.0"`, `PRYM_VERSION` `"bf24c3d+ri02+sr01+bt02"`, both unchanged.
