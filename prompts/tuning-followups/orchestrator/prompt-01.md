# Orchestrator — prompt 01, retry failed scalar histories

Read [`../README.md`](../README.md) first: §0.1, §0.2, §0.4, §1, §2.1 (the gate "Retry"), §5 and §6.
Then the prompt and the board. `jordan-normalization`'s note for its 08
(`prompts/jordan-normalization/orchestrator/prompt-08.md`) recorded the finding this prompt answers;
nothing in it binds 01 beyond what is restated here.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`01-retry-failed-scalar.md`](../01-retry-failed-scalar.md)
· **Board items:** F1–F8 · **Gate:** "Retry" · **Closes:**
`[08-a-history-that-did-not-converge-is-never-retried]` (`jordan-normalization`) · **Opens:**
`[01-bbn-from-store-refuses-a-history-with-a-retried-bbn-row]` (ruling 8) and only what else the
work finds · **Model:** Opus, as the prompt recommends · **Network:** none. Nothing is pushed.

**Gate**, checked at `4930fac` on 2026-10-27 at 01:55 GMT, before this note was committed:
- `tuning-followups` checked out in the main checkout; `git status --porcelain
  --untracked-files=all` empty; `git worktree list` shows the main checkout alone. `HEAD` is the
  commit that adds this note, which touches only `orchestrator/`, on top of `4930fac`.
- `git diff d049296 4930fac --stat -- '*.py'`: `config/argument_parser.py` alone, the
  `--samples-log10-z` help (`abe8b24`). So the code is `jordan-normalization` 10's reviewed tree
  but for one help string, and README §0.4's baselines stand: CosmologyModels **26**,
  ComputeTargets **243** (76 + 167), ObjectFactories **93**, tools **115**, RunDrivers **64**. The
  agent measures them again before it changes anything.
- `./venv/bin/python -m runregistrykit list`: 11 runs, all finished (`done`, `killed` or `lost`),
  none `running` or stale. No Ray is started by this prompt (README §3, U3).
- `main.py --help` runs without Ray and lists `--retry-failed-bbn` (`:114-116` of its output) and no
  `--retry-failed-scalar`.
- The one-minute load was 11.4 (11.43 / 7.69 / 6.76). Other workloads share the machine.

## 0. What makes this prompt unusual

**It changes the one lookup every stage and both plot scripts read.** The ordering must be
invisible on every store that holds one row per key, which is every store made so far, and the
whole of its effect must appear only once a retry stores a second row. The orchestrator read every
file the prompt names at `4930fac`, and DatastoreKit's actor (`datastorekit/SQL/Datastore.py`,
`store_inventory.py`, `store_fingerprint.py` at `v0.4.0`) where the prompt leans on it. The prompt's
line numbers are right for this tree. What it found, where the prompt is silent, loose or wrong:

- **The factory's `store` inserts unconditionally** (`ObjectFactories/ScalarHistory.py:639-`), and
  DatastoreKit's `object_store` calls it with no lookup first (`Datastore.py:661-730`). So a
  history returned unpopulated by F3's `failure=False` lookup is computed and stored beside the
  failure with nothing else in the way, and `validate` (`:785-`) marks it as it marks any success.
  The prompt's second stop condition ("cannot be made to skip the failure without changing
  `_do_not_populate`…") is not expected to arise.
- **The columns the ordering needs exist.** `failure` is `Boolean`, `nullable=False`, default
  `False` (`:211`), so `failure.asc()` puts the successes first with no NULL to place; `timestamp`
  is added by DatastoreKit for a table whose schema says `"timestamp": True` (`:97`, `:153`), as on
  `BBNData`.
- **`build_solver_batch` runs once per batch.** The first pass is inside the task builder
  (`main.py:189-290`), which RayWorkPool calls in the driver, once per batch of eight models. The
  count of stored failures (F2, F6) is therefore accumulated across calls, in a closure dict, as the
  adiabatic stage does with `adiabatic_counts` (`main.py:344`), and printed after
  `solver_queue.run()`.
- **There is no seam for F3 without Ray.** The second lookup is built inline in the closure, from
  `args`, and handed to `pool.object_get`. Its premise, however, can be tested on a temporary store:
  `failure=False` on a key holding only a failure returns an unavailable history. The prompt's F7
  does not list that case; ruling 4 adds it.
- **The inventory names two failures under one key as a `duplicate`.** `ScalarHistory`'s
  `inventory_spec` makes `failure` a leaf of the key (`:282-`), so a success beside its failure is
  two keys and no problem; but a retry that fails again leaves two failures under one key, which
  `store_inventory` reports as the named problem `duplicate` ("all are kept") and
  `store_fingerprint` counts by name. Neither refuses. That is what `--retry-failed-bbn` already does
  to `BBNData`, and it is correct: both rows are real. Ruling 7.
- **One more reader would trip over a retry, but not this one.** `tools/bbn_from_store.py`'s
  `find_model` raises `LookupError` when a history has more than one `BBNData` row
  (`:268-273`, `_BBN_QUERY` at `:226-230` has no `failure` filter or order). A scalar retry cannot
  cause that: the retried success has a new serial and its own BBN rows, and a failure has none.
  `--retry-failed-bbn` can, and has since `run-integrity` 03. It is not F5's and is not fixed here.
  Ruling 8.
- **No other reader bypasses the factory.** `grep` over the tree (outside `venv/`, `thirdparty/`,
  `.git/`) for SQL naming `ScalarHistory` finds `tools/bbn_from_store.py` alone, besides tests and
  `config/datastore.py`'s registry. `tools/compare_stores.py` compares stores table by table, not by
  lookup, and is not a reader in the prompt's sense. The prompt's third stop condition is not
  expected to arise; if the agent finds one, it stops.
- **`find_model` on a store holding only a failure** changes its message: today it finds the failure
  and returns a `StoredHistory` with no samples; after F5 it raises "no ScalarHistory with …". That
  is the intended reading of P5 and is recorded, not a stop.

**Rulings**, each recorded in the log as a deviation of the class given where it reads the prompt
otherwise, or as `NOTED` where it only fixes what the prompt leaves open.

1. **F2's counter** (`IMPLEMENTATION CHOICE`, ruled). `count_stored_failures(lookups)` in
   `pipeline_selection.py`, beside `missing_models`, returns the number of lookups that are
   `available` and whose `failure` is true; a docstring like its neighbours'. `missing_models` keeps
   its signature, gains `retry_failed=False`, and its docstring says what the flag does. In
   `main.py`, a closure dict (`scalar_counts = {"stored_failures": 0}`, beside
   `scalar_history_failure_reasons`) is incremented in `build_solver_batch` from each bin's
   lookups, before the early `return []`, so a batch with nothing missing still counts.

2. **F6's lines, exactly** (`NOTED`). After `solver_queue.run()`, after the existing
   "histories failed in this run" block and separate from it, when the count is positive:
   - with the flag: `-- ScalarHistory: N histories with a stored failure retried (--retry-failed-scalar)`;
   - without: `-- ScalarHistory: N histories skipped because of a stored failure`.

   Nothing when N is 0. No other printed line of the stage changes.

3. **F3's wiring** (`IMPLEMENTATION CHOICE`, ruled). No new seam: the second `pool.object_get`'s
   keyword arguments gain `failure=False` when `args.retry_failed_scalar` is true, and are otherwise
   **exactly today's**: no `failure=None` is passed without the flag, though it would mean the same.
   A comment names this campaign and says why (the factory's `failure=None` lookup would find the
   stored failure, and nothing would be computed). The log says the wiring is verified by reading,
   because the closure is reachable only through `RayWorkPool` and a Ray pool, and points at
   ruling 4's premise test.

4. **F7's factory tests** (`IMPLEMENTATION CHOICE`, ruled). A new module,
   `ObjectFactories/tests/test_scalar_history_failure_lookup.py`, in
   `ObjectFactories/tests/test_bbn_failure_lookup.py`'s form: `_TempStoreCase`, rows through the
   datastore's own inserter (`_insert`) with **explicit, increasing serials**, lookups through
   `object_get` (so the version key is present) with `_do_not_populate=True`, a module docstring
   that says which tests raise or fail on the tree before this prompt and which are regression
   guards for unchanged filters. Its cases: the prompt's F7 list, **plus the premise of F3**: a key
   holding only a failure, looked up with `failure=False`, gives `available` false. A success row
   needs what `build` checks for a success (a solver row, since `:429-436` raise on a success with no
   solver label; and whatever `_tuning_from_row` reads); how the module supplies it is the agent's
   choice, said in the log. The vectorized case goes through `object_get_vectorized` with
   `payload_data`, as `test_c3_vectorized_lookup_is_keyed_too` does. Under a minute alone; its time
   in its docstring.

5. **F4's comment and the tag join** (`NOTED`). The comment above the ordering names
   `tuning-followups` prompt 01 and `run-integrity` prompt 03, and says the ordering is invisible
   while a key holds one row. The log states what makes the `limit 1` safe across the joins: the
   solver outer join and the two tolerance joins are many-to-one, and each tag join is inner on
   (history, tag); whether `ScalarHistory_tags` can hold one (history, tag) pair twice is read in its
   factory and the factory's `store`, and said. If it can, that is recorded under "Observations not
   acted on", not fixed. Two validated successes under one key, which no route writes, would now give
   the newer rather than raise: recorded as a consequence, as `BBNData`'s.

6. **F5's test** (`IMPLEMENTATION CHOICE`, ruled). A new test in
   `ComputeTargets/tests/test_bbn_from_store.py` on test (c)'s fixture, or a helper factored from
   it, with a failure row (`failure` 1, a lower serial, no samples, no BBN row) under the target's β,
   M and φ: `find_model` returns serial 7 and no `LookupError`. Test (c) is not modified; if the
   fixture is factored out of it, the log lists the change and says (c)'s assertions are unchanged.

7. **The inventory's `duplicate`** (`NOTED`). The agent confirms §0's reading of
   `store_inventory` by reading, and may run a probe in its scratch directory (two failures under
   one key on a `_TempStoreCase`-style store, then `read_inventory`), never committed. The log records
   what it found. `architecture-summary.md`'s note on the ordered lookup says, in one sentence, that a
   retry which fails again leaves two failures under the key and the inventory names them
   `duplicate`, as for `BBNData`. No test is required; no code changes for it.

8. **The issue this prompt opens** (`NOTED`; found by this note). On this board's §3:
   `[01-bbn-from-store-refuses-a-history-with-a-retried-bbn-row]`. What: §0's bullet, with the lines
   (`tools/bbn_from_store.py:226-230`, `:268-273`). Impact: `bbn_from_store.py` stops with
   `LookupError` on any history whose BBN computation was retried with `--retry-failed-bbn` and so
   holds a failure and a success; the tool is a diagnostic, and the pipeline and plot scripts are
   unaffected (they look up through `sqla_BBNDataFactory.build`, which orders). Next step: for the
   user; order `_BBN_QUERY` as the factory does, or filter it to the success. Not assigned. The index
   gains a heading `### 1.18 tuning-followups — [board §3](…)` with its row. One issue closes (F1–F8)
   and one opens, so **the count stays 25**; the date 2026-10-27.

9. **The baselines and counts** (`NOTED`). Before: the five suites, serially, `ComputeTargets` in its
   two halves, each Bash call at 600 000 ms, output to the agent's scratch directory. Expected 26 /
   76 + 167 / 93 / 115 / 64. After: the same, each count at least the baseline and rising only by
   F7's tests; the log gives every count before and after, with times and `uptime`. A failure is
   re-run alone before it is called real. An existing test that fails is a stop.

10. **The breakages** (`STRUCTURALLY REQUIRED`). The prompt's B1–B4, plus:
    - B5, the `failure=False` premise: the factory's `failure` filter skipped when `failure is
      False` (only that branch): ruling 4's premise test fails;
    - B6, F2's counter: `count_stored_failures` returns 0 always: its test fails.

    Each a diff `git apply` replays from the agent's commit, saved in its scratch directory and
    quoted in the log, with every test that fails and how. B1 is also the evidence for ruling 4's
    docstring ("raise on the tree before"). None committed.

11. **The documents** (`NOTED`). F8's four notes, each opening
    `> **Note, 2026-10-27 (`tuning-followups`, prompt 01).**` (or the date it lands), additive only.
    `git diff` of each document must show added lines only (`git diff -U0 <file> | grep '^-[^-]'`
    empty), and the log says so.

12. **The board** (`NOTED`). Header: "**Status: LIVE — 01 landed, awaiting review; 02 written.**" or
    words to that effect; §1's row 01 ✅ with "this commit" and the log; §1.1's F1–F8 marked; §2's
    gate "Retry" as found; §3 for ruling 8; §3.1's row for the closed issue marked "closed by 01";
    `### 5.2 After prompt 01 (<date>)`, in §5.1's form: what landed, the counts, the breakages, the
    issue opened. `jordan-normalization`'s board: the issue moved from §3 to §4 with a **Resolved**
    line naming this prompt and "this commit". `.documents/OPEN_ISSUES.md`: the §1.17 row deleted,
    §1.18 added (ruling 8), the header's count and the sentence about these two boards corrected.

**What moves on purpose:** exactly the prompt's §5 list, with the test module named by ruling 4:
`config/argument_parser.py`, `pipeline_selection.py`, `main.py`, `ObjectFactories/ScalarHistory.py`,
`tools/bbn_from_store.py`; `ComputeTargets/tests/test_pipeline_selection.py`,
`ObjectFactories/tests/test_scalar_history_failure_lookup.py` (new),
`ComputeTargets/tests/test_bbn_from_store.py`; `.documents/architecture-summary.md`,
`.documents/numerical-strategies.md`; `prompts/jordan-normalization/IMPLEMENTATION_STATE.md`,
`.documents/OPEN_ISSUES.md`, `prompts/tuning-followups/IMPLEMENTATION_STATE.md`, and the log
`prompts/tuning-followups/logs/01-retry-failed-scalar.md` (new).

**What must not move:** every other file. In particular `ComputeTargets/ScalarHistory.py` and the
tune lines (02's), `config/fingerprint.py` (`FINGERPRINT_FORMAT`), `VERSION_LABEL`, the golden
histories and the golden fingerprint, `RunDrivers/`, both plot scripts, `plot_inputs.py`,
`process_batch_size`, `CLAUDE.md`, `prompts/INDEX.md`, the README, `orchestrator/`, `science-runs/`,
every other campaign's records but `jordan-normalization`'s board §3–§4, the venv, the three
packages, SGK and SI.

**Conventions.** All of these bind the agent.
- **Order.** The Gate; the baselines; F4 and its tests (show they fail first, which is B1); F1, F2,
  F3, F6 and their tests; F5 and its test; the breakages; the suites after; `black`; `main.py --help`
  and the parser check of the prompt's §2 item 3; the documents; the records; the commit.
- **Staging and reporting.** One commit, staged by explicit path only, after showing `git diff
  --cached --name-only`, which is the list above exactly. Check `git log -1` before committing:
  `HEAD` must still be this note's commit. The message in `CLAUDE.md`'s form. A record states only
  what a command's output shows. "this commit" where the SHA goes; the orchestrator substitutes it.
- **Scratch.** Everything saved while working goes under `var/tuning-followups-01/` (ignored by git);
  never `/tmp`, the session scratchpad or the tree.
- **`black`** on the changed `.py` files only (`CLAUDE.md`), never a file not otherwise changed.
- **Stop conditions are real**: the prompt's §4, and: a golden history or the golden fingerprint
  fails; a suite's count falls; an existing test fails for a reason this note does not name (never
  edit it to pass); anything in DatastoreKit would need to change. A stop goes to the user through
  the orchestrator; the agent resumes by message.

## 1. Before you dispatch

Done at `4930fac`, before this note was committed: the Gate above; the reading of §0. The suites'
baselines are the agent's (ruling 9).

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout. Give it: the prompt, the README, the
board, `HEAD`, the Gate as measured, and §0 of this note up to "## 1. Before you dispatch", as a copy
in the session scratchpad. The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly §0's list. Its log is `logs/01-retry-failed-scalar.md`,
  in `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **No Ray, no store but temporary ones.**
- **Stop and ask** on any of the prompt's §4 conditions or §0's.
- **It replies with:** the log's path; the message as committed; `git show --stat` of its commit;
  the suites before and after, with counts, times and `uptime`; the new tests by name, each with the
  breakage that makes it fail; the B1–B6 diffs' results; the F6 lines as written; the parser check's
  output and `main.py --help`'s lines for the flag; the tag-join finding (ruling 5) and the inventory
  finding (ruling 7); every deviation with its class; "Observations not acted on" and "State handed
  on", verbatim.

## 3. The review — nine checks

1. **Scope.** `git show --stat <agent>` lists exactly §0's files; its parent is this note's commit.
   `git status --porcelain --untracked-files=all` empty; nothing pushed. `git diff <dispatch>
   <agent> -- ComputeTargets/ScalarHistory.py config/fingerprint.py RunDrivers plot_by_beta.py
   plot_ScalarHistory.py plot_inputs.py CLAUDE.md` empty.
2. **The factory.** Read the diff of `build` against `BBNData.py:195-210`: the three branches, the
   order keys, the `limit`, the comment; `failure=False` unchanged. Replay B1 and B4 and see their
   tests fail; revert.
3. **The passes.** Read `main.py`'s diff: the counter (ruling 1) before the early return; the second
   lookup's kwargs exactly today's without the flag (ruling 3); F6's two strings (ruling 2) against
   `main.py:872-877`'s form. `missing_models` with and without the flag and `count_stored_failures`
   read against their tests; replay B2 and B6.
4. **The premise.** Run the new module alone; replay B5. Run a probe of my own on a
   `_TempStoreCase` store: a failure, then `failure=False` (unavailable), then a stored success
   through the factory's `store` and `validate` as the pool would, then the default lookup (the
   success) and the vectorized one (the success). That is the gate "Retry" end to end, short of Ray.
5. **`bbn_from_store`.** The filter and its comment; the new test; replay B3. Test (c) unchanged in
   its assertions.
6. **The flag.** The parser check; `main.py --help`; `plot_by_beta.py --help` and
   `plot_ScalarHistory.py --help` list it (the shared parser) and neither reads
   `args.retry_failed_scalar` (`grep`); the wrapper's `scope` does not name it.
7. **Values do not move.** The golden histories and the golden fingerprint tests pass at `<agent>`;
   `git diff <dispatch> <agent> -- config/ ComputeTargets/` shows only the parser.
8. **The records.** The log in §5.1's form, every deviation classed, §0's rulings among them; the
   documents' diffs additive only (ruling 11); the board (ruling 12); `jordan-normalization`'s §4
   entry; `.documents/OPEN_ISSUES.md`'s rows, heading, count and date against the boards.
9. **The suites** at `<agent>`, serially: the counts the log gives, each at least 26 / 76 + 167 / 93 /
   115 / 64. `black --check` on the changed `.py` files.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, on both boards and in the log;
- the board's list of orchestrator notes (new, under §1), with this one marked "used for 01";
- `### 5.2`'s *Orchestrator review of prompt 01* paragraph, holding what each check found;
- the header: 01 ✅ reviewed; next, the note for 02.

Residue the review finds is fixed in that commit or a follow-up of its own. Anything that is not
residue is opened on the board's §3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:** the agent's commit and the review commit; the gate "Retry"; the counts; the
issue opened (ruling 8); and the rulings that read the prompt otherwise than it was written (the
premise test added, the counter accumulated across batches, F3 verified by reading).
