# Orchestrator — prompt 01, the mechanism and the scan

Read [`README.md`](README.md) and [`../README.md`](../README.md) §0.2–§0.4, §2, §4, §6.0–§6.1
first. **You do not write code.**

**The prompt:** [`../01-mechanism-and-tolerance-scan.md`](../01-mechanism-and-tolerance-scan.md)
**Board item:** M; measures S and N · **Closes:** nothing

## 1. Before you dispatch

1. **The board's Decisions record the user's ruling on README §0.2 P1–P9.** If they do not, stop
   and ask; U1 alone is not enough.
2. On branch `bbn-tolerance`; `git status` clean apart from the user's untracked run files; record
   `HEAD`.
3. The three suite counts and their wall-clocks.
4. The store's mtimes (`README.md` rule 10).
5. `venv/bin/black --check` is not needed: no existing Python file may change.

## 2. Dispatch

One fresh-context subagent, **Opus**, using the template in `README.md`. Add:

> *Your diff may touch only:*
> - *`tools/bbn_from_store.py` (new);*
> - *`ComputeTargets/tests/test_bbn_from_store.py` (new);*
> - *`prompts/bbn-tolerance/logs/` and `prompts/bbn-tolerance/IMPLEMENTATION_STATE.md`;*
> - *`.documents/OPEN_ISSUES.md`.*
>
> *No production file and nothing in `PRyM/` may change. The scan may run 8–10 solves at a time
> (README §0.2 U1); the cost measurement must run alone, after the scan.*

## 3. The review — seven checks

1. **Allowed files.** `git diff --stat HEAD~1` names nothing outside the list, and in particular
   nothing in `PRyM/`, `ComputeTargets/*.py` or `Datastore/`.
2. **The stand-in, run by you.** Run the tool on the control β = 1.6 at M = 10⁻³ and on one failure
   (β = 2.4 at M = 10⁻⁵), `prod`, no override. The control must give the stored Yp 0.2468948390
   and D/H 2.461511946 to every printed digit. The failure must fail in
   `low-T nuclear network (full)` at the stored `t reached`, 1.284e+06.
3. **The override, run by you.** Run test (b). Read the test and confirm that it compares every
   other call's keyword arguments, not only the low-T one.
4. **The tests.** `test_bbn_from_store` passes, and the suites pass with ComputeTargets risen.
5. **The store.** The mtimes are unchanged.
6. **The scan is complete.** Every cell of README §2 (c) is in `logs/01-probes/scan.csv`. Count
   the rows against 245 + 98 + 20. Missing cells are a deviation the log must name.
7. **The recommendation.** It follows P3's rule from the log's own table, with each criterion's
   value quoted with provenance. The cost figures are from the serial, idle measurement. The
   stop conditions of the prompt's §8 are each addressed.

Then the board: M done; S and N carry dated **Narrowed** lines; the index's hooks, count and date
corrected.

## 4. Report, then stop

As `README.md` says. **In addition, put the decision to the user plainly:** the recommended
setting, its four P3 values, the pinned-value table (P7), and whether the mechanism confirmed the
low-T stage. Prompt 02 cannot be dispatched until the user's ruling is on the board.
