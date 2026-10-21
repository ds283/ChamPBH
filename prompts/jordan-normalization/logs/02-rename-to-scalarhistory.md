# Log 02 — rename to `ScalarHistory`

**Subject:** Rename `ScalarModel` to `ScalarHistory` throughout; fingerprint format 3
**Commit:** `abccc23`, on branch `jordan-normalization`, on top of `481b65e`
**Date:** 2026-10-21, by Claude Sonnet 5.5
**Result:** **COMPLETE**, after one stop (Deviation 9), which the user ruled. N1–N4 shipped. The golden
history passes unchanged (its JSON was not written). CosmologyModels 18, ComputeTargets 179
(61 + 118), tools 84 and RunDrivers 61 unchanged; ObjectFactories 78 → 79 (+1, the refusal of
`--drop scalar-model`). Both deliberate breakages caught. One issue opened.

## What shipped

- **N1 — the code.** By `git mv` (all five show as `R` in `git diff --cached -M --name-status`:
  R095, R092, R091, R081, R092): `ComputeTargets/ScalarModel.py`, `ObjectFactories/ScalarModel.py`,
  `plot_ScalarModel.py`, and `test_scalarmodel_failure_reason.py` in `ComputeTargets/tests/` and
  `ObjectFactories/tests/`. Every class, function, factory, table, constant, plural, message and
  path comment named in note ruling 1 was renamed by a reviewed script, then each diff read.
  `SCALAR_HISTORY_STEPPER_LABEL` keeps its value `"Radau+kinematic-cap-stepping0"`. The tables
  `ScalarHistory`, `ScalarHistory_tags`, `ScalarHistoryValue` are renamed in place in
  `config/datastore.py`, `config/sharding.py` and the batch sizes (registry order unchanged). The
  drop group is `scalar-history`; `--drop`'s help is the ruled text.
  - **Columns.** `history_serial` on `ScalarHistoryValue` and `ScalarHistory_tags`;
    `scalar_history_serial` on `AdiabaticHistory` and `BBNData`. No `model_serial` is left in code.
    `AdiabaticHistoryValue` and `AdiabaticHistory_tags` keep their own `history_serial`.
- **N2 — format 3.** `FINGERPRINT_FORMAT = 3` with its history paragraph and the extended count
  paragraph; `SPEC_DIGESTS[3]` pasted from `spec_digests(factories)`. **17 pairs, 14 distinct
  digests** (format 2: 17 and 14). Moved: `ScalarHistory`, `AdiabaticHistory`, `BBNData`. The other
  14 pairs equal format 2's. The checking line:
  `python -c "…d=spec_digests(factories); print(len(d), len(set(x for _,x in d)))"` printed `17 14`,
  and the names whose digest differs from format 2's (with `ScalarHistory` read as `ScalarModel`)
  were `['ScalarHistory', 'AdiabaticHistory', 'BBNData']`. `SPEC_DIGESTS[1]` and `[2]` are
  byte-identical to `d56f908`'s (`git diff d56f908 -- config/fingerprint.py` removes only
  `FINGERPRINT_FORMAT = 2` and one comment line). The golden file
  `ObjectFactories/tests/data/store_fingerprint_jordan-normalization-02.json` was written once from
  `fingerprinter.take(primary, None)` by a scratch probe that asserts the file is absent
  (`fingerprint_format` 3, `taken` null, `problems` `{}`, 17 classes, digest `ebbd384e…8075`,
  SHA-256 `3fa1b426…65a8`). `grep -c golden-` on it: **0**, so row labels are not fingerprinted.
  The two older golden files are untouched (`jordan-normalization-01` still `7e68c348…`).
  `RunDrivers/tests/test_registered_run_fingerprint.py` was not edited and passes.
- **N3 — the documents.** `CLAUDE.md` (the "Redshift" bullet, the plot-scripts bullet and the
  drivers in "Ray on this machine"; its suite counts untouched). `.documents/architecture-summary.md`
  and `.documents/numerical-strategies.md`: present-tense text only, plus one dated note each.
  `prompts/INDEX.md` not edited (ruling 10).
- **N4 — the run scripts** are not edited. See Deviation 11 for what they contain.
- **The records.** The board (row 02, gate "Rename", §3 with the new issue, §5.3);
  `.documents/OPEN_ISSUES.md` (§1.16 row, **27 → 28 open**, header clause, Last updated 2026-10-21).

**File count.** 60 tracked Python files changed: the 59 of the note (which include
`config/fingerprint.py` and `ObjectFactories/tests/test_store_fingerprint_golden.py`) and
`.documents/dk-and-rpk-migration/rehearsal/stage_counts.py` (Deviation 9). Non-Python: `CLAUDE.md`, the two
documents, the board, `.documents/OPEN_ISSUES.md`, the new golden file, this log. All 60 Python files
were black-clean at `481b65e`; `black --check` passes on all 60 now (9 were reformatted).

No Ray was started; no store was opened but the tests' temporary ones; nothing was written to `var/`.

## Tests renamed, added or changed

Renamed (old → new), `STRUCTURALLY REQUIRED` (ruling 1):

| Old | New |
|---|---|
| `test_e3_an_unavailable_scalar_model_is_reported_not_queried` | `test_e3_an_unavailable_scalar_history_is_reported_not_queried` |
| `test_plot_scalar_model_without_an_address` | `test_plot_scalar_history_without_an_address` |
| `test_plot_scalar_model_with_auto` | `test_plot_scalar_history_with_auto` |
| `test_scalar_model_alone_is_refused_naming_the_groups_to_add` | `test_scalar_history_alone_is_refused_naming_the_groups_to_add` |
| `test_scalar_model_with_one_dependent_group_is_refused` | `test_scalar_history_with_one_dependent_group_is_refused` |
| `test_a_scalar_model_is_returned_only_under_its_own_label` | `test_a_scalar_history_is_returned_only_under_its_own_label` |
| `test_the_scalar_models` | `test_the_scalar_histories` |

The two modules `test_scalarmodel_failure_reason.py` are renamed likewise (they stay in their roots
and halves).

Added: `ObjectFactories/tests/test_datastore_config.py::TestDropPolicy::test_the_old_drop_group_name_is_refused_naming_the_new_one`
(ruling 5). `create_argument_parser().parse_args(["--database", "x", "--drop", "scalar-model"])` raises
`SystemExit` with code 2 and stderr containing `invalid choice: 'scalar-model'` and `scalar-history`.
`STRUCTURALLY REQUIRED`.

Changed beyond a name:

| Test | Change |
|---|---|
| `test_store_fingerprint_golden.py::TestCommandLine::test_store_fingerprint` | the asserted `fingerprint_format` literal 2 → 3 (Deviation 8); `GOLDEN` names the new file; the docstring gains a format-3 paragraph |
| same file, builder and `TestGolden` tests | the rows are inserted into the renamed tables, and the columns `history_serial` (tags, values) and `scalar_history_serial` (`AdiabaticHistory`, `BBNData`); the assertions are as before |
| `test_inventory_report.py`, `test_compare_stores.py`, `test_bbn_from_store.py`, `test_foreign_bbn_provenance.py`, `test_validated_lookups.py`, `test_version_keyed_lookups.py`, `test_datastore_config.py` | tables and column names in inserted rows and raw SQL, expected groups and refusal messages; assertions otherwise as before |
| `test_compare_stores.py` fixture log (lines 1198, 1203) | `ScalarHistory` in the two `main.py` summary lines, so the fixture mirrors `main.py`; the parser it feeds was changed under Deviation 9 |
| `ComputeTargets/tests/golden_history.py`, `test_golden_history.py` | imports from `ComputeTargets.ScalarHistory` and calls `compute_scalar_history._function`; docstrings follow (ruling 7). The pinned values are not touched |

## Deviations from the prompt

Every ruling cited is the orchestrator's note for prompt 02, §0.

1. **`STRUCTURALLY REQUIRED` (ruling 1): the name map.** As ruled, including the test methods, the path
   comments and `SCALAR_HISTORY_ALONE`. A cited line number in a moved file's comment is left as it is.
2. **`IMPLEMENTATION CHOICE` (ruling 2): the columns.** `history_serial` / `scalar_history_serial`.
   The collision: `AdiabaticHistoryValue` and `AdiabaticHistory_tags` already reference their own
   `AdiabaticHistory` row as `history_serial` (`ObjectFactories/AdiabaticHistory.py:47`, `:133-134`), so
   `AdiabaticHistory`'s reference to the scalar history cannot also be `history_serial` without one name
   meaning two parents in one module. `ObjectFactories/tests/test_datastore_config.py`'s orphan-table
   test uses `scalar_history_serial` (a table that points at a history; either name serves there).
3. **`STRUCTURALLY REQUIRED` (ruling 3): the lowercase "model" stays.** Listed under Observations; the
   issue `[02-the-scalar-history-parent-is-still-called-model]` is opened.
4. **`IMPLEMENTATION CHOICE` (ruling 4): messages and stored strings.** The row label of `main.py`
   (`…-ScalarHistory-…`), the failure reasons that name `compute_scalar_history`, the golden store's row
   labels and the stream-test fixture are renamed. New rows' labels and failure reasons name the new names.
   `SPEC_DIGESTS[1]` and `[2]` and the format-1 and 2 comments are byte for byte.
5. **`STRUCTURALLY REQUIRED` (ruling 5): the registry, group and option.** As ruled; the new test above.
6. **`STRUCTURALLY REQUIRED` (ruling 6): format 3.** See N2. The expected counts (17, 14, three moved)
   held, so no explanation was needed.
7. **`STRUCTURALLY REQUIRED` (ruling 7): the golden history.** See Verification. `golden_history.py` imports
   from `ComputeTargets.ScalarHistory`; that is the setup, not the pinned values.
8. **`STRUCTURALLY REQUIRED` (ruling 6, as the orchestrator confirmed after my report):
   `test_store_fingerprint_golden.py` asserts the golden's `fingerprint_format` literal.** It was 2 and
   is 3. The first run of ObjectFactories failed on it (`3 != 2`); 01 changed the same literal.
9. **`STRUCTURALLY REQUIRED` (the user's ruling after a stop): the rehearsal parser.** I stopped, committing
   nothing, because `tools/tests/test_compare_stores.py::TestStageCounts::test_stage_counts_parses_a_complete_and_an_interrupted_log`
   errored (`ValueError: not enough values to unpack`). Ruling 4 renames `main.py`'s two summary lines
   (`…models skipped because their ScalarHistory failed`); the test loads
   `.documents/dk-and-rpk-migration/rehearsal/stage_counts.py`, whose `SKIPPED` regex held the literal
   `ScalarModel`; and ruling 10 forbids editing the rehearsal scripts. The user ruled "Edit the
   parser": the `SKIPPED` regex is `(?:ScalarModel|ScalarHistory)`, with a one-line comment naming this
   prompt, so dated logs and new ones both parse; its dated outputs under `.documents/*/out/`, `run_readers.sh`
   and every other rehearsal script are not touched. After it the test passes.
10. **`STRUCTURALLY REQUIRED` (rulings 8, 12): the moves and the driver checks** as ruled. After the commit,
    `git log --follow --oneline -2 -- <new path>` reaches a commit before this one for each of the five
    (the orchestrator's follow-up checks it at the SHA; I could not run it before the commit).
11. **Ruling 11 was wrong on the facts.** `science-runs/2026-10-03-science-2026.6.0/*.sh` (two files) name
    neither `plot_ScalarModel.py` nor `scalar-model` (0 and 0). They use `--drop bbn-data`, which is
    still valid, and each has one comment quoting a stored failure reason, "no ScalarModel row in the
    store" (lines 56 and 58). They are not edited. The rehearsal script that names the old driver is
    `.documents/dk-and-rpk-migration/rehearsal/run_readers.sh` (6 hits on `plot_ScalarModel`); see below.

No `UNINTENDED DRIFT`.

## Verification performed

Load is the one-minute `uptime` figure at the start of each run; outputs are in the scratch directory
`agent-02/` (`before_*`, `after_*`, `final_*`).

| Suite | Before (`481b65e`) | After (final) | Change |
|---|---|---|---|
| CosmologyModels | 18 OK, 93 s, load 25 | 18 OK, 72 s, load 19 | none |
| ComputeTargets `test_[a-f]*` | 61 OK, 109 s, load 50 | 61 OK, 106 s, load 19 | none |
| ComputeTargets `test_[g-z]*` | 118 OK, 113 s, load 31 | 118 OK, 113 s, load 17 | none |
| ObjectFactories | 78 OK, 14 s, load 53 | **79 OK**, 7 s, load 18 | **+1**: ruling 5's test |
| tools | 84 OK, 15 s, load 73 | 84 OK, 14 s, load 20 | none |
| RunDrivers | 61 OK, 19 s, load 65 | 61 OK, 19 s, load 21 | none |

Counts: 18 / 179 / 78 / 84 / 61 before; 18 / 179 / 79 / 84 / 61 after. Between, two runs failed (Deviations 8
and 9) and passed after those; no count fell.

**The golden history (ruling 7).** I called the generator's `main()` with `golden_history.GOLDEN_PATH`
pointed into the scratch directory (the committed file was never opened for writing), and compared every
pinned item of both histories with `first_difference`: **0 differences** for `beta=1.5` and `beta=5.5`;
provenance not compared. The committed JSON's SHA-256 is the same before and after
(`shasum -c`: OK). `test_golden_history` alone: 3 tests OK in 9.7 s, at load 60 (wall 11 s for the
generator at the same load).

**Ruling 12's invocations**, all from the repository root, all exiting in the argument parser:
- `main.py --help`: exit 0; `--drop {scalar-history,adiabatic-history,bbn-data}`.
- `plot_ScalarHistory.py --help`: exit 0.
- `main.py --database <scratch>/absent.db --drop scalar-model`: **exit 2**,
  `main.py: error: argument --drop: invalid choice: 'scalar-model' (choose from 'scalar-history', 'adiabatic-history', 'bbn-data')`;
  no file was created at the path.

**The closing grep**, ruling 9's command:

```
git grep -n -E 'ScalarModel|scalar_model|scalar-model|SCALAR_MODEL|scalarmodel|model_serial' -- . ':!prompts' ':!science-runs' ':!.documents'
```

Run before the commit, on the tree the commit holds (the new golden file is untracked until staged, so it
is not listed; it has no hit). Whole output:

```
ObjectFactories/tests/data/store_fingerprint_jordan-normalization-01.json:91:    "ScalarModel": {
ObjectFactories/tests/data/store_fingerprint_rrk-adoption-02.json:124:    "ScalarModel": {
ObjectFactories/tests/test_datastore_config.py:416:        """`--drop scalar-model` (the name before jordan-normalization prompt 02) exits 2 in the
ObjectFactories/tests/test_datastore_config.py:422:                    ["--database", "x", "--drop", "scalar-model"]
ObjectFactories/tests/test_datastore_config.py:426:        self.assertIn("invalid choice: 'scalar-model'", message)
config/argument_parser.py:268:        help="drop one or more data categories (scalar-history replaced scalar-model in jordan-normalization prompt 02)",
config/fingerprint.py:60:# ReflectingPotential, and ScalarModel's spec loses their ids from the ``potential`` parent's types.
config/fingerprint.py:62:# Format 3: jordan-normalization prompt 02, the rename of ScalarModel to ScalarHistory. The classes
config/fingerprint.py:63:# ScalarModel, ScalarModelValue and ScalarModel_tags (and their tables) become ScalarHistory,
config/fingerprint.py:83:# and ``ReflectingPotential`` shared; ``ExponentialPotential`` keeps its own, and ``ScalarModel``'s
config/fingerprint.py:85:# counts as format 2, with ``ScalarHistory`` where format 2 has ``ScalarModel``; the digests of
config/fingerprint.py:157:            "ScalarModel",
config/fingerprint.py:218:            "ScalarModel",
```

Reasons: the two older golden files are the records of formats 1 and 2; the three hits in
`test_datastore_config.py` are ruling 5's new test; `config/argument_parser.py:268` is `--drop`'s help, which
names the old group on purpose; `config/fingerprint.py` holds format 1's and 2's pins (`:157`, `:218`), the
format-2 comments (`:60`, `:83`) and format 3's comment (`:62`, `:63`, `:85`). `CLAUDE.md` has none.

**What remains in the two current documents (by line), each in a dated note or an "Added" section, left by
invariant 6** (the line numbers are those of the tree committed):
- `.documents/architecture-summary.md`: 152, 184, 187, 189, 209, 577, 581, 663, 801, 804, 826, 831, 834, 841,
  882, 950 (dated notes, quoting the names of their day); 952, 957, 960 (this prompt's own note, naming the
  old names to say what was renamed); 1062, 1065, 1069, 1079, 1083 (the run-integrity and science-readiness
  "Note added" lists of the stage text, which describe `main.py` of their day).
- `.documents/numerical-strategies.md`: 109, 110, 111 (this prompt's note); 189, 201, 208, 324, 325, 327,
  366, 372, 385, 386, 387 (§3.5 and §3.6, the "Added 2026-10-01" and "Added 2026-10-02" sections); 466 (§3.6.2);
  533 (the "Note added 2026-09-29"); 965 (§7.6); 1278 (§7.7). These sections cite the paths and the
  names of their day.
- `.documents/dk-and-rpk-migration/rehearsal/stage_counts.py`: lines 46 and 48 (the `SKIPPED` regex and its
  comment) keeps `ScalarModel` on purpose, so that logs made before the rename still parse (Deviation 9).

**Counts for the excluded places** (lines matching the closing grep's pattern): `prompts/` 170 files, 1334
lines; `science-runs/` 30 files, 15206 lines; the rest of `.documents/` and the two current documents
together 69 files, 486 lines (before the files above were edited it was the same set plus this prompt's
notes).

`black --check` on the 60 changed Python files: all unchanged.

## The deliberate-breakage record

Each is a `diff -u` against the tree of `abccc23` (taken from a copy before the mutation), reverted by
copying back, never committed. Each checked with `git apply --check`, then applied, `git apply -R --check`
and reversed, in `bash`; the tree was byte-identical after.

**1. `"scalar-model"` added back to `allowed_drop_actions`.** `test_datastore_config`: two failures,
**`test_the_old_drop_group_name_is_refused_naming_the_new_one`** (the new test) and
`test_the_command_line_choices_are_the_groups` (the choices no longer equal the groups).

```diff
--- a/config/argument_parser.py
+++ b/config/argument_parser.py
@@ -30,7 +30,7 @@
 DEFAULT_LOG10_LAMBDA_HIGH_EV = 1
 DEFAULT_SAMPLES_PER_LOG10_LAMBDA_EV = 6
 
-allowed_drop_actions = ["scalar-history", "adiabatic-history", "bbn-data"]
+allowed_drop_actions = ["scalar-model", "scalar-history", "adiabatic-history", "bbn-data"]
 potential_types = ["Exponential"]
 
 
```

**2. `BBNData`'s parent left as `model_serial`.** In the three modules run (`test_fingerprint_format_guard`,
`test_store_fingerprint_golden`, `test_inventory_specs`): 3 failures and 3 errors. The format guard,
`test_every_spec_is_pinned_for_the_current_format`, fails naming it ("spec changed: BBNData"). The golden
fingerprint **errors** (`KeyError: 'model_serial'`) in `TestGolden`'s three tests, and
`TestCommandLine.test_store_fingerprint` fails (the subprocess exits 1 with the same `KeyError`).
`test_inventory_specs.test_every_named_column_is_a_column_of_its_table` fails for `BBNData`
(`'model_serial' not found`).

```diff
--- a/ObjectFactories/BBNData.py
+++ b/ObjectFactories/BBNData.py
@@ -127,7 +127,7 @@
             leaves=("failure", "PRyM_version"),
             parents={
                 "version": Parent("version", "version"),
-                "model": Parent("scalar_history_serial", "ScalarHistory"),
+                "model": Parent("model_serial", "ScalarHistory"),
             },
             tags=("BBNData_tags", "bbn_serial"),
             values=("BBNDataValue", "bbn_serial"),
```

## Observations not acted on

- **The lowercase "model"** (ruling 3), now the issue `[02-the-scalar-history-parent-is-still-called-model]`
  on the board and in `.documents/OPEN_ISSUES.md` §1.16: the parent field `"model"` in the inventory specs
  of `AdiabaticHistory` and `BBNData` (`ObjectFactories/AdiabaticHistory.py:131`, `BBNData.py:130`);
  `tools/inventory_report.py`'s `model_text_of`, `model_parts`, `model_sort`, its `"model"` literals and its
  printed `model: {…}`; the payload key `model_proxy` and the attribute `_model_proxy`; locals and
  parameters named `model`. 07 renames them.
- **The rehearsal scripts are records** (ruling 11). `.documents/dk-and-rpk-migration/rehearsal/run_readers.sh`
  names `plot_ScalarModel.py` six times and no longer runs on this tree. Not edited; no issue opened.
- **The dated outputs under `.documents/*/out/`** carry `…their ScalarModel failed` lines; the parser reads them.
- Some comments in `ObjectFactories/AdiabaticHistory.py` and a few other files are now longer than 100
  columns; `black` does not wrap comments and none was touched beyond the rename.

## State handed to the next prompt

- **Tree:** `abccc23`, on `jordan-normalization`, on top of `481b65e`. No `ScalarModel` name remains
  outside the records, the two older golden files, `config/fingerprint.py`'s format 1 and 2 records, the
  `--drop` help and its test, and the rehearsal parser's regex.
- **Names:** `ComputeTargets/ScalarHistory.py`, `ObjectFactories/ScalarHistory.py`, `plot_ScalarHistory.py`;
  `ScalarHistory`, `ScalarHistoryValue`, `ScalarHistoryProxy`, `compute_scalar_history`;
  `sqla_ScalarHistoryFactory`, `sqla_ScalarHistoryTagAssociation_factory`, `sqla_ScalarHistoryValue_factory`;
  tables `ScalarHistory`, `ScalarHistory_tags`, `ScalarHistoryValue`; drop group `scalar-history`.
  Columns: `history_serial` on the value and tag tables, `scalar_history_serial` on `AdiabaticHistory` and
  `BBNData`. `SCALAR_HISTORY_STEPPER_LABEL` still has the value `"Radau+kinematic-cap-stepping0"`.
  The parent field `"model"` and `model_proxy` are unchanged (the open issue).
- **Fingerprint:** `FINGERPRINT_FORMAT = 3`; `SPEC_DIGESTS[1]`, `[2]`, `[3]` (17 pairs, 14 distinct digests
  for 2 and 3); the current golden file is
  `ObjectFactories/tests/data/store_fingerprint_jordan-normalization-02.json` (`test_store_fingerprint_golden.GOLDEN`);
  digest `ebbd384e…8075`. **03 makes format 4**: a new pin generated by `spec_digests`, a new golden file
  `store_fingerprint_jordan-normalization-03.json`, `GOLDEN` pointed at it, and the format literal in
  `TestCommandLine.test_store_fingerprint` (now 3) moved with it. The golden store is unchanged: two
  `ExponentialPotential` rows and two `ScalarHistory` rows, the first tagged.
- **The golden history** (`ComputeTargets/tests/golden_history.py` and its JSON, SHA-256 `6898a432…62d6`)
  passes unchanged. `run_history` calls `compute_scalar_history._function` with the signature of `ac187c8`'s
  (`cosmology, T_init, T_stop, phi, pi, z_grid, potential, coupling`). 03 and 05 regenerate it deliberately;
  04 must pass it unchanged and change `run_history` to follow the new interface.
- **Counts:** CosmologyModels 18, ComputeTargets 179 (61 + 118), ObjectFactories 79, tools 84, RunDrivers 61.
- **Open issues:** one opened, `[02-the-scalar-history-parent-is-still-called-model]` (07). Seven remain
  assigned to this campaign (board §3.1). `.documents/OPEN_ISSUES.md`: 28 open.
- **The closing grep** is ruling 9's `git grep -n -E '…'` (no `\b`); `-P` is needed only for patterns with `\b`.
- **Breakage replay:** the two diffs above apply to `abccc23`'s tree (`git apply --check` passes for each
  from the tree of `abccc23`; after applying each, `git apply -R --check` passes and the reverse restores
  the tree), checked in `bash`.
