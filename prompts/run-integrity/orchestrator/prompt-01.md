# Orchestrator — prompt 01, key the compute-target lookups on the version

Read [`README.md`](README.md) and [`../README.md`](../README.md) §0.4, §2 (a), §6.1 first. **You
do not write code.**

**The prompt:** [`../01-version-keyed-lookups.md`](../01-version-keyed-lookups.md)
**Board item:** V · **Closes:** `[00-datastore-lookups-ignore-the-version-column]`

## 0. What makes this prompt unusual

The fix is a filter in three queries. The risk is in what else it touches. The review is four
things:

- a row stored under one label is invisible under another;
- the key cannot be bypassed;
- no parameter table is keyed;
- there is one label, and its value is unchanged.

## 1. Before you dispatch

1. The planning commit is on `run-integrity`; record `HEAD`; `git status` clean.
2. Baselines: all three suite counts (18, 30, 0) and wall-clocks.
3. Run the planning probe and keep its output:
   ```bash
   PYTHONPATH=. ./venv/bin/python prompts/run-integrity/planning-probes/datastore_version_probe.py 2>&1 | grep "^\["
   ```
   Line [3] must show the old row returned under the new label.
4. Record:
   ```bash
   grep -rn "VERSION_LABEL =" --include='*.py' . | grep -v "venv/\|thirdparty/\|claude-context/"   # 3 lines
   git diff --stat 27a32bc..HEAD
   ```
5. `venv/bin/black --check Datastore/SQL/Datastore.py Datastore/SQL/ObjectFactories/ScalarModel.py Datastore/SQL/ObjectFactories/AdiabaticHistory.py Datastore/SQL/ObjectFactories/BBNData.py main.py plot_by_beta.py plot_ScalarModel.py`.

## 2. Dispatch

One fresh-context subagent, **Opus**, template in `README.md`. Add: *"Your diff may touch
`Datastore/SQL/Datastore.py`; the `ScalarModel`, `AdiabaticHistory` and `BBNData` factories
under `Datastore/SQL/ObjectFactories/`; `config/version.py` (new); the label lines and imports of
`main.py`, `plot_by_beta.py` and `plot_ScalarModel.py`; `Datastore/tests/` (new); one test
command in `CLAUDE.md`; `.documents/architecture-summary.md` (an additive note); the log; this
campaign's board; the `review-remediation` board (one Resolved line); and
`.documents/OPEN_ISSUES.md`. Not `Datastore/SQL/ObjectFactories/base.py`, not any other factory,
not `PRyM/`. The label's value stays "2026.3.0"."*

## 3. The review — eight checks

1. **Allowed files.** `git diff --stat HEAD~1 HEAD`: nothing outside the dispatch's list.
2. **The breakage, run by you.** Use the generic procedure in `README.md`, with `T` =
   `Datastore.tests.test_version_keyed_lookups` and `F` = `Datastore/SQL/Datastore.py` plus the
   three factories. On `HEAD~1`:
   - (a)–(c) fail because the row is returned under the second label;
   - (d) fails because nothing raises.

   For (f), check out `HEAD~1`'s `main.py`, `plot_by_beta.py` and `plot_ScalarModel.py`: it
   fails because each assigns the label. Restore, and confirm everything passes.
3. **No bypass.** Read each keyed `build()`: a missing serial raises, and no branch builds the
   query without the filter.
4. **No schema change.** In `git diff HEAD~1 HEAD -- Datastore/SQL/ObjectFactories/`, each
   `register()` gains only the new flag. No column is added or removed.
5. **Parameter tables untouched.** No other factory is in the diff, and (e) passes.
6. **One label.** §1 item 4's grep finds exactly one line, `config/version.py`, value
   `"2026.3.0"`. `main.py`'s dated comment is there verbatim, with one new dated sentence.
7. **The probe agrees.** Re-run §1 item 3. Line [3] now reports `available=False`.
8. **Housekeeping.**
   - `Datastore/tests` is ≥ 5, and the other two suites are unchanged (18, 30).
   - The new `CLAUDE.md` line runs as written.
   - `black --check` is clean.
   - The board: V done, and the header states the consequence.
   - The index row is gone, and the count and date corrected.
   - Exactly one Resolved line on the `review-remediation` board.
   - `.documents/architecture-summary.md` shows only added lines.

## 4. Stop and ask the user

- Any of the prompt's §5.
- A keyed lookup can run unfiltered; or any `register()` changes a column.
- The label's value changed, or more than one definition remains.

## 5. After it lands

Report:

- the commit;
- the breakage record;
- the probe's line [3] before and after;
- the names chosen for the flag and the key;
- the three suite counts;
- the consequence sentence.

Then stop.
