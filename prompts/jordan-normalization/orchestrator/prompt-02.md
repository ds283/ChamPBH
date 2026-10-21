# Orchestrator — prompt 02, rename to `ScalarHistory`

Read [`../README.md`](../README.md) first: §0.3 item 6, §2.2 (02's charter), §5, §6.0 R9, §6.1 U1
and §6.2 (P2, P3). Read 01's log too, in particular its "State handed to the next prompt". Its
conventions are those of [`prompt-01.md`](prompt-01.md) §0, "Conventions", restated below where 02
needs them.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`02-rename-to-scalarhistory.md`](../02-rename-to-scalarhistory.md)
**Board items:** N1–N4 · **Gate:** "Rename" · **Closes:** nothing · **Opens:** one, by ruling 3
(and anything else the work finds) · **Model:** Sonnet, as the prompt recommends · **Network:** none.

**Gate:**
- 01 landed (`a1c88b6`) and reviewed (`d56f908`); the gate "Strip" holds (board §2). `HEAD` is the
  commit that adds this note, on `jordan-normalization`, with `git status` clean.
- The prompt's own gate: no tracked change, the five suites at 01's counts, no ChamPBH Ray process.
- One prompt at a time in this checkout. 03 is not dispatched until 02 is reviewed.

## 0. What makes this prompt unusual

**It is large, mechanical and computes nothing new.** Its witness is 01's golden history, which
must pass with its JSON untouched. Measured at `d56f908`:
`git grep -E 'ScalarModel|scalar_model|scalar-model|SCALAR_MODEL|scalarmodel|model_serial'` outside
`prompts/`, `science-runs/` and `.documents/` finds **685 occurrences in 59 Python files**. It also
finds `CLAUDE.md` and the two committed golden fingerprint files. (README §0.3 item 6 counted 478
in 62, with a narrower pattern and before 01.) Five tracked files carry the name in their path:
`ComputeTargets/ScalarModel.py`, `ObjectFactories/ScalarModel.py`, `plot_ScalarModel.py`, and
`test_scalarmodel_failure_reason.py` in both `ComputeTargets/tests/` and `ObjectFactories/tests/`.

**Baselines at `d56f908`**, serially, at a load of 36–86 (another workload was on the machine):
CosmologyModels **18**, ComputeTargets **61 + 118**, ObjectFactories **78**, tools **84**,
RunDrivers **61**, all passing. The golden history took 11 s at load 6–8 in 01's review.

**Twelve statements in the prompt are loose, silent, or wrong for this tree. They are ruled here.**

1. **The name map, exactly.** The rule is that every identifier, string, comment and docstring that
   names the class, its module or its tables changes, in every form:
   - `ScalarModel` → `ScalarHistory`, `ScalarModelValue` → `ScalarHistoryValue`,
     `ScalarModelProxy` → `ScalarHistoryProxy`, `ScalarModel_tags` → `ScalarHistory_tags`, the
     factories `sqla_ScalarModel*` → `sqla_ScalarHistory*`;
   - `compute_scalar_model` → `compute_scalar_history` (its `task_label` default too),
     `plot_ScalarModel` (the function and the `job_name`) → `plot_ScalarHistory`,
     `add_ScalarModel_labels` → `add_ScalarHistory_labels`, `report_dropped_scalar_models` →
     `report_dropped_scalar_histories`;
   - constants: `SCALAR_MODEL_STEPPER_LABEL` → `SCALAR_HISTORY_STEPPER_LABEL`, **with its value
     `"Radau+kinematic-cap-stepping0"` unchanged** (it is stored as `solver_label` and pinned by the
     golden history); `SCALAR_MODEL_ALONE` (in `test_datastore_config.py`) → `SCALAR_HISTORY_ALONE`;
   - plurals: `ScalarModels` → `ScalarHistories`, `scalar_models` → `scalar_histories`;
   - test methods whose names carry it: `test_e3_an_unavailable_scalar_model_is_reported_not_queried`,
     `test_plot_scalar_model_without_an_address`, `test_plot_scalar_model_with_auto`,
     `test_scalar_model_alone_is_refused_naming_the_groups_to_add`,
     `test_scalar_model_with_one_dependent_group_is_refused`,
     `test_a_scalar_model_is_returned_only_under_its_own_label`, `test_the_scalar_models`. Each
     becomes `…scalar_history…` (`test_the_scalar_histories`), and the log lists each, old name and
     new;
   - module paths in comments and docstrings (`ComputeTargets/ScalarModel.py:NNN`), past-tense
     ones included (`SaikawaShirai_EOS_spline.py:50`): the file moved, and `git log --follow`
     reaches its past. A line number cited in a moved file's comment is left as it is.

   Classify as `STRUCTURALLY REQUIRED`, citing this note.

2. **The columns.** The prompt renames every `model_serial` to `history_serial`. On this tree that
   name already exists: `AdiabaticHistoryValue` and `AdiabaticHistory_tags` reference their own
   `AdiabaticHistory` row by `history_serial` (`ObjectFactories/AdiabaticHistory.py:47`, `:133-134`).
   If `AdiabaticHistory`'s reference to the scalar history were also called `history_serial`, the
   one name would mean two different parents in one module. A join of the two tables on it would
   then be silently wrong. Ruled:
   - **`history_serial`** on `ScalarHistoryValue` and `ScalarHistory_tags`, as the prompt says.
     It is the row's own history, as on `AdiabaticHistoryValue`;
   - **`scalar_history_serial`** on `AdiabaticHistory` and `BBNData`: the scalar history the row
     was computed from;
   - with them, every `ForeignKey("ScalarHistory.serial")`, every `Parent(…)` column, every query
     (`table.c.…`, `redshift.py:107`'s join), the raw SQL of `tools/bbn_from_store.py`
     (`:208-230`), the payload keys of the factories' `store` and `build` that carry the column
     name, and every test that inserts rows by column name (`test_store_fingerprint_golden.py`,
     `test_inventory_report.py`, `test_compare_stores.py` and others);
   - no `model_serial` is left in code (07 introduces the name again, for the Lagrangian's row).

   Classify as `IMPLEMENTATION CHOICE`, citing this note, and name the collision in the log.

3. **What is not renamed: the lowercase "model".** The prompt renames names that say `ScalarModel`.
   Many lowercase names mean a scalar history too, but they do not say `ScalarModel`:
   - the parent field `"model"` in the inventory specs of `AdiabaticHistory` and `BBNData`
     (`:131`, `:130`);
   - `tools/inventory_report.py`'s `model_text_of`, `model_parts`, `model_sort`, its `"model"`
     literals (`:349`, `:465`, `:470`) and its printed `model: {…}`
     (`test_inventory_report.py:321` expects it);
   - the payload key `model_proxy` and the attribute `_model_proxy`; locals and parameters named
     `model`; prose "scalar model".

   **They stay as they are.** Renaming them is not a rename of the class. It would change the
   inventory's key fields and printed text, and 07 rewrites these same specs and readers. The
   agent records them in "Observations not acted on". In the same commit it opens
   **`[02-the-scalar-history-parent-is-still-called-model]`** on this board's §3, with *What*,
   *Impact* ("none to values; once 07 gives `model` to the Lagrangian, `model` would mean two
   things") and *Next step* ("07, which rewrites these specs and readers, renames them"). It adds
   the row to `.documents/OPEN_ISSUES.md` §1.16, puts the count at **28**, rewrites the header's
   `jordan-normalization` clause to say prompt 02 opened one (2026-10-21), and sets **Last updated**.
   `SampleValues`, `ModelFunctions`, the potential and coupling classes, `AdiabaticHistory` and
   `BBNData` do not change (the prompt's N1).

4. **Messages and stored strings.** They are renamed too, and four of them reach a store:
   - the row label `main.py:300` builds (`…-ScalarHistory-…`);
   - failure reasons raised inside the integration, which name `compute_scalar_history`
     (`ComputeTargets/ScalarModel.py:494` and the like);
   - the golden store's row labels (`test_store_fingerprint_golden.py:217`);
   - the stream-test fixture `RunDrivers/tests/test_registered_run_streams.py:174`, which copies a
     message.

   None of these is a key, a leaf or a computed value. The label holds a time and is never
   compared, and no lookup matches a failure reason. Classify as `IMPLEMENTATION CHOICE`, citing
   this note, and say in the log that new rows' labels and failure reasons name the new names.
   **The one exception** is `config/fingerprint.py`. `SPEC_DIGESTS[1]` and `[2]`, with their
   `"ScalarModel"` pairs, and the format-1 and format-2 comments are records of those formats.
   They stay byte for byte.

5. **The registry, the drop group and the option.**
   - In `config/datastore.py`, `config/sharding.py` and the batch sizes, each key is renamed
     **in place**, so the registry's order does not change. The fingerprint reads that order, and
     a name-sorted listing does not move either ("ScalarH…" sorts where "ScalarM…" did).
   - `drop_groups`: `"scalar-history": ["ScalarHistory_tags", "ScalarHistoryValue", "ScalarHistory"]`.
     `allowed_drop_actions` follows. `tables_to_drop`'s docstring and `test_datastore_config.py`'s
     expected groups and refusal messages follow too.
   - `--drop`'s help, exactly: `drop one or more data categories (scalar-history replaced
     scalar-model in jordan-normalization prompt 02)`. The old name is in it on purpose.
   - **A new test** in `ObjectFactories/tests/test_datastore_config.py`: `create_argument_parser()`
     given `["--database", "x", "--drop", "scalar-model"]` exits with `SystemExit(2)`, and its
     stderr contains `invalid choice: 'scalar-model'` and `scalar-history`. No existing test can
     fail the prompt's first breakage, and this one can. (The parser checks a choice before it
     checks the required arguments; the orchestrator confirmed that at `d56f908`'s parser with an
     unknown group.) ObjectFactories goes **78 → 79**. Classify as `STRUCTURALLY REQUIRED`.

6. **Format 3.**
   - `FINGERPRINT_FORMAT = 3`, and a format-3 paragraph in the comment, in the form of the first
     two, naming the renamed classes and the columns of ruling 2;
   - `SPEC_DIGESTS[3]` is generated by `spec_digests(factories)` and never typed. **Expected:** 17
     pairs and 14 distinct digests, with `"ScalarHistory"` where format 2 has `"ScalarModel"`, and
     three digests moved: `ScalarHistory`'s, `AdiabaticHistory`'s and `BBNData`'s. Any other count
     is explained in the log before the commit. The comment's counts for format 3 follow, and the
     log shows the line that checks them;
   - `SPEC_DIGESTS[1]` and `[2]` are byte-identical to `d56f908`'s;
   - the new golden file is `ObjectFactories/tests/data/store_fingerprint_jordan-normalization-02.json`,
     written once from `fingerprinter.take(primary, None)` as 01's was, and `GOLDEN` names it. The
     two older golden files are not touched. `LABEL` and `T0` do not change. The agent states
     whether a row label appears in the new file (`grep -c golden- …`); 01's review found that
     labels are not fingerprinted;
   - `RunDrivers/tests/test_registered_run_fingerprint.py` follows `GOLDEN` without an edit; do
     not edit it.

   Classify as `STRUCTURALLY REQUIRED`.

7. **The golden history.** `ComputeTargets/tests/golden_history.py` imports from
   `ComputeTargets.ScalarHistory` and calls `compute_scalar_history._function`, and its
   docstrings follow. `test_golden_history.py`'s docstring follows too. 01's handover requires
   this, and it is a change of the setup, not of the pinned values.
   `ComputeTargets/tests/data/golden_history_01.json` is **never written**. After the rename the
   agent runs the generator with its output redirected into its scratch directory (never over the
   committed file), and compares every pinned item with the committed JSON; the provenance block
   is not compared. Then it runs `test_golden_history` alone. Both must agree; a difference is a
   stop. Classify as `STRUCTURALLY REQUIRED`, citing 01's handover.

8. **The moves.** The five files of §0, by `git mv`, staged with their edits in the one commit.
   Before committing, `git diff --cached -M --name-status` must show each of the five as an `R`
   line. A file shown as `D` plus `A` has been edited past git's rename detection, and is a stop.
   After the commit, `git log --follow --oneline -2 -- <new path>` reaches a commit before this
   one. The test modules stay in their roots and halves: `test_scalarhistory_failure_reason.py`
   is still in `[g-z]`. So no count moves except ruling 5's.

9. **The closing `grep`.** The prompt's `grep -rn` misses `SCALAR_MODEL`, `scalarmodel` and the
   columns. Ruled:
   `git grep -n -E 'ScalarModel|scalar_model|scalar-model|SCALAR_MODEL|scalarmodel|model_serial' -- . ':!prompts' ':!science-runs' ':!.documents'`
   (no `\b`, so `-E` serves). Its only hits are:
   - `config/fingerprint.py`: format 1's and 2's pins and comments, and format 3's comment;
   - `ObjectFactories/tests/data/store_fingerprint_rrk-adoption-02.json` and
     `store_fingerprint_jordan-normalization-01.json`;
   - `config/argument_parser.py`: the `--drop` help of ruling 5;
   - `ObjectFactories/tests/test_datastore_config.py`: ruling 5's new test.

   `CLAUDE.md` has none. The log pastes the command and its whole output, and gives each hit's
   reason. For the two current documents (ruling 10), it lists by line what remains, each with
   its reason. For `prompts/`, `science-runs/` and the rest of `.documents/` it gives counts only.
   Classify as `STRUCTURALLY REQUIRED`, citing this note.

10. **The current documents (N3).**
    - `.documents/architecture-summary.md`. The text that describes the tree in the present tense
      becomes true of it: the directory trees (`:31`, `:51`, `:60`), §1's layer list, the registry
      table (`:421-423`), the sharding block (`:614-615`), `--drop`'s choices (`:555`), the drivers
      named at `:395`, `:496` and `:577`, §7.2's heading and body, the proxy paragraph, the stage
      sequences (`:1015-1094`), and the mapping tables (`:1143-1180`). **Dated notes**
      (`> **Note, <date> …`) and citations of records keep the old names. Add one dated note
      beside 01's (`:950`): the rename by prompt 02 (R9), with the columns of ruling 2 and the
      drop group.
    - `.documents/numerical-strategies.md`, the same rule, with one dated note.
    - `CLAUDE.md`: the "Redshift" bullet (`ScalarHistoryValue`), the plot-scripts bullet, and the
      drivers named in "Ray on this machine". **Not** its suite counts, which are stale since 01
      and belong to 10 (C2).
    - **Not edited:** `prompts/INDEX.md` (its row describes the campaign, and 10 owns it);
      `.documents/numerical-methods-for-paper.md` (dated 2026-09-29); every other file under
      `.documents/`, the rehearsal scripts under `.documents/dk-and-rpk-migration/rehearsal/`
      among them; and, in `.documents/OPEN_ISSUES.md`, every existing row. Issue ids such as
      `[01-scalarmodelvalue-build-checks-the-stored-phi-against-pi]` are keys across boards and
      never change.

    Classify as `STRUCTURALLY REQUIRED`.

11. **N4, the run scripts.** `science-runs/2026-10-03-science-2026.6.0/*.sh` are not edited, and the
    log says how many name `plot_ScalarModel.py` or `scalar-model`. The `dk-and-rpk-migration`
    rehearsal scripts that name `plot_ScalarModel.py` (`run_readers.sh`) are records in the same
    way. They no longer run on this tree. The log says so under "Observations not acted on", and
    no issue is opened, since a record is not edited.

12. **The checks of §2 that run a driver.** `main.py --help` and `plot_ScalarHistory.py --help`, and
    the refusal `PYTHONPATH=. ./venv/bin/python main.py --database <scratch>/absent.db --drop scalar-model`.
    It must exit 2 naming `scalar-history`, and the database path must lie in the agent's scratch
    directory. All three exit inside the argument parser, before Ray and before any store. They
    are the only driver invocations allowed.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only (moves included). Show
  `git diff --cached -M --name-status` before committing. Count, then paste. A log states only
  what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its
  follow-up.
- **Order.** The five suites at the start, before any edit. Then the moves, then the edits. Then
  ruling 7's comparison and the golden test, the grep, the suites, the breakages, and the records.
- **Suites.** Run `CLAUDE.md`'s commands serially, in the foreground, with ComputeTargets in its
  two halves. Send output to a file in the agent's scratch directory, grep the verdict from it,
  and record `uptime` beside each. A failure is re-run alone before it is called real.
- **A search-and-replace is reviewed, never blind.** Run it file by file or with a reviewed list,
  then read the diff of each file. `history_serial` on the `AdiabaticHistory*` tables is not
  touched (ruling 2).
- **Breakage diffs** are recorded so that `git apply` replays them from a clean tree at the agent's
  commit. Check each with `git apply --check` and `-R --check`, and replay in `bash`, not zsh. The
  two the prompt names:
  1. `"scalar-model"` added back to `allowed_drop_actions`. It fails ruling 5's test, and also
     `test_datastore_config.py`'s test that `allowed_drop_actions` equals the groups;
  2. `BBNData`'s `Parent("scalar_history_serial", "ScalarHistory")` left as
     `Parent("model_serial", "ScalarHistory")`. It fails the format guard, naming `BBNData`, and
     the golden fingerprint fails or errors (say which, and how).
- **Scratch.** The agent uses its own subdirectory of the session scratchpad, never `/tmp`. No
  scratch file goes under the repository.
- **Stores and Ray.** No store is opened except the tests' temporary ones. No Ray is started. No
  driver runs, except ruling 12's three. Nothing is written to `var/`.
- **Stop conditions are real.** A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. Review against the agent's commit, and check
  `git log` before measuring at `HEAD`.

## 1. Before you dispatch

Done at `d56f908`, before this note was committed:
1. **The tree.** `jordan-normalization`, `git status --short` empty.
2. **Ray.** `ray_cluster.sh status`: lock free, no processes, ports free.
   `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing.
3. **The registry.** `./venv/bin/python -m runregistrykit list`: no runs.
4. **Baselines.** §0's, all passing.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Sonnet**. Give it:
- the prompt;
- the campaign README, this board, and 01's log;
- `HEAD` and the baselines;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad.
  The agent does not read `orchestrator/` or prompts 03–10.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/02-rename-to-scalarhistory.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **The files it may add, change, move or delete** are the 59 Python files of §0, the five moves,
  `CLAUDE.md`, the two current documents, the new golden fingerprint file, this board,
  `.documents/OPEN_ISSUES.md`, and the log. Nothing else.
- **§0's twelve rulings** bind it, each classified as §0 says.
- **The golden history's JSON is never written.**
- **It starts no Ray, opens no store but a test's, and runs no driver but ruling 12's three.**
- **Stop and ask** on any of the prompt's §4 conditions, or on ruling 8's `D` + `A`. An existing
  test that fails for a reason §0 does not cover is a stop, never a reason to edit it.
- **It replies with:**
  - the log's path and the message as committed;
  - `git show --stat -M` of its commit, with the five `R` lines;
  - ruling 7's comparison and the golden test's time;
  - the closing `grep` and its output, and what remains in the two documents by line;
  - every test method renamed or added, and every test changed beyond a name, with what it asserts;
  - each breakage, with the tests it failed and how;
  - the five suites, before and after, with each count's change explained;
  - the format-3 pin's pair and distinct-digest counts, and which digests moved;
  - every deviation with its tag;
  - "State handed to the next prompt", verbatim.

## 3. The review — ten checks

1. **Scope.** `git show --stat -M <agent>` lists only §2's files, with five renames and no
   deletion. `git status` is clean.
2. **Nothing but names.** In a temporary worktree at `<agent>` (removed afterwards), apply the
   inverse map to every changed code file except `config/fingerprint.py`:
   `ScalarHistor(y|ies)` → `ScalarModel(s)`, `scalar_histor(y|ies)` → `scalar_model(s)`,
   `scalar-history` → `scalar-model`, `SCALAR_HISTORY` → `SCALAR_MODEL`, `scalarhistory` →
   `scalarmodel`, `scalar_history_serial` → `model_serial`. Undo the moves too, and diff against
   `<agent>~1`. What remains is accounted for by name: ruling 2's `history_serial` on the two
   `ScalarHistory` child tables (read by eye, since the map cannot invert it), ruling 5's test and
   help, and black's re-wrapping of lines whose length changed. Anything else is a finding.
3. **The golden history.** Run the generator into the scratchpad at `<agent>`, and compare every
   pinned item with the committed JSON. Its SHA-256 is that of 01 (`6898a432…62d6`), and
   `git diff <agent>~1 <agent> -- ComputeTargets/tests/data/` is empty. Run `test_golden_history`
   alone, with its time.
4. **The grep.** Run ruling 9's command; only its four kinds of hit. Read the two documents'
   residue against the log's reasons.
5. **The columns.** `ForeignKey`, `Parent`, queries and raw SQL agree with ruling 2, read in
   `ObjectFactories/{ScalarHistory,AdiabaticHistory,BBNData,redshift}.py` and
   `tools/bbn_from_store.py`. `AdiabaticHistory`'s own `history_serial` is untouched. As the
   orchestrator's own probe, revert `redshift.py`'s join column alone to a name that does not
   exist, and run the ObjectFactories and ComputeTargets suites. If no test fails, the join is
   unexercised; record that as an observation, not a finding. Then revert the probe.
6. **The moves.** Each of the five shows as an `R` line. `git log --follow` reaches its past.
   `main.py --help` and `plot_ScalarHistory.py --help` exit 0, and ruling 12's refusal exits 2.
7. **Format 3.** `FINGERPRINT_FORMAT` 3. `SPEC_DIGESTS[1]` and `[2]` are byte-identical to
   `d56f908`'s. `SPEC_DIGESTS[3]` equals `spec_digests(factories)`, recomputed by the orchestrator,
   with 17 pairs, 14 distinct digests, and the three digests that moved named. The new golden file
   is format 3 with `taken` null and no problems, and the two old ones are unchanged.
8. **The breakages.** In `bash`, replay each diff with `git apply`, run the named tests, see them
   fail as the log says, revert with `git apply -R`, and check that `git status` is clean.
9. **The records.** This board (row 02, N1–N4, the gate "Rename", §3's new issue, `### 5.3`),
   `.documents/OPEN_ISSUES.md` (the row in §1.16, 28, the clause, the date), the documents as
   ruling 10 says, and `prompts/INDEX.md` and every record untouched. The log in §5.1's form, with
   every deviation classified and every test renamed or added listed.
10. **The suites** at the agent's commit, serially: 18; 61 + 118; **79**; 84; 61. Run
    `black --check` on every changed or new Python file.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 02* paragraph in §5.3. Fix
  small residue in a follow-up commit of its own:
  - replace "this commit" with the SHA (board, log, index);
  - add this note to the board's list of orchestrator notes, marked "used for 02";
  - update the status line: 2 of 10 landed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the message as committed;
  - the rulings that read the prompt otherwise than it was written: `scalar_history_serial` on
    `AdiabaticHistory` and `BBNData` (ruling 2); the lowercase "model" left for 07, with its issue
    (ruling 3); the new `--drop` test (ruling 5);
  - that the golden history held bitwise;
  - the five suites' counts;
  - **separately, as housekeeping and not 02's:** `[01-plot-by-beta-profile-label-names-plot-scalarmodel]`
    (`run-integrity` board §3, index §1.5) is stale. `plot_by_beta.py:134` has named itself since
    `5740690`, "Label plot_by_beta's profile agent with its own name". The issue was never closed.
    Offer to close it in a standalone commit.
- **Then 03.** Its note is written after this review.
