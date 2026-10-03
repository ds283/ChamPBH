# Orchestrator — prompt 02b, the history tool runs production's network

Read [`README.md`](README.md) and [`../README.md`](../README.md) §0.2 (U5), §6.2b first.
**You do not write code.**

**The prompt:** [`../02b-history-tool-follows-production.md`](../02b-history-tool-follows-production.md)
**Board item:** H · **Closes:** `[02-two-places-still-say-production-runs-the-full-network]`

## 1. Before you dispatch

1. Prompt 02 landed with no unresolved miss, and the board's Decisions record U5. On branch
   `bbn-tolerance`; `git status` clean apart from the user's untracked run files; record `HEAD`.
2. The three suite counts (18, 113, 31 after prompt 02).
3. `venv/bin/black --check tools/history_and_bbn.py ComputeTargets/BBNData.py ComputeTargets/tests/test_network_flag.py`.
   Record any file that is not clean already.

## 2. Dispatch

One fresh-context subagent, **Sonnet**, using the template in `README.md` with `NN-<name>` =
`02b-history-tool-follows-production`. Add:

> *Your diff may touch only `tools/history_and_bbn.py` (the `--small-network` default and help),
> `ComputeTargets/BBNData.py` (the one comment in `_configure_PRyMordial`),
> `ComputeTargets/tests/test_network_flag.py` (test (c)), the log, this campaign's board and
> `.documents/OPEN_ISSUES.md`.*

## 3. The review — four checks

1. **Allowed files.** `git diff --stat HEAD~1` names nothing else. The `BBNData.py` hunk is
   comment lines only. The `tools/history_and_bbn.py` hunk is the argument's `default` and `help`
   only.
2. **The breakage, run by you.** Check out `tools/history_and_bbn.py` from `HEAD~1`.
   `test_network_flag` (c) must fail on the value (`[False]`), not on an import. Restore it, and
   confirm `git status` is clean.
3. **The help text.** `venv/bin/python tools/history_and_bbn.py --help` shows the small network as
   the default and names `--no-small-network`.
4. **Suites.** All three pass at 18, 113 and 31; `black --check` is clean on the files changed.

Then the board: H done; the issue closed; the index corrected.

## 4. Report, then stop

As `README.md` says. Tell the user that prompt 03 needs prompt 01b landed first, if it has not.
