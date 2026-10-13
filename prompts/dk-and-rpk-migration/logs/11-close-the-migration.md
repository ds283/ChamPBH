# Log 11 — close the migration

**Prompt:** [`11-close-the-migration.md`](../11-close-the-migration.md) · **Commit:** this commit, on
branch `dk-and-rpk-migration`, on top of `ee79455` (the orchestrator's note; its code is `2623905`'s) ·
**Model:** Sonnet · **Date:** 2026-10-13
**Result:** Z1–Z7 landed. The campaign is closed. The sweep printed 19 lines before and 20 after, all
classified below; the six touched Python files are `ast`-equal with docstrings removed
(`tools/compare_stores.py` but for its two reason strings); the three roots pass at 18 / 114 / 97
before and after. No code behaviour changed, no store was opened, no Ray was started, nothing was
installed, no process of anyone else was touched. `full_run_2026.6.0.sh` and `…-refresh.sh` were read
only.

## What shipped

- **Z1** `.documents/architecture-summary.md` (214 +, 178 −): the tree (`Datastore/` as it is, a line
  naming `datastorekit`, `config/datastore.py`); §4 rewritten for the package (opens with a dated note;
  4.1 the package and its modules with pointers to the client contract at `v0.3.0`; 4.2 what ChamPBH
  supplies and the registry table; 4.3 what the package does, `_do_not_populate`, the version-keyed
  lookups re-pointed to `datastorekit.contract`, the two-pass pattern; 4.4 a factory and how to add a
  class); §5.2 (`LambdaCDM` out, `read_table_config`, `get_shard_key_store_id`, `config/datastore.py`);
  §9's example in the mapping form; §12–§14 for the packages. The ten dated `> **Note` blocks of the
  file are untouched (checked by script: none gone, one added).
- **Z2** `CLAUDE.md`, "Repository mechanics" only (24 +, 4 −): a packages bullet (the upstream-defect
  rule; `v0.2.3` the precedent), the three test commands with 18 / 114 / 97 and ComputeTargets' two
  halves (61, 53) with the ten-minute sentence, and the Ray bullet.
- **Z3** prose in six files (below). **Z4** `measure/out/11-*` (10 files). **Z5** verification §4
  (154 lines appended). **Z6** `review-remediation-verification.md` §4.12 (107 lines appended).
  **Z7** the board (COMPLETE, row 11, Z1–Z7, §5.17), `prompts/INDEX.md`, `.documents/OPEN_ISSUES.md`.

## Deviations from the prompt

1. **IMPLEMENTATION CHOICE** (orchestrator's facts govern). The sweep and the `ast` check are the
   orchestrator's scripts (`stale_paths.py`, `ast_prose_only.py`), used as given; the sweep's pattern is
   in the orchestrator's facts, not re-quoted here. Run as
   `./venv/bin/python -I <scratch>/stale_paths.py .` and `… ast_prose_only.py OLD NEW`.
2. **IMPLEMENTATION CHOICE.** The prompt's list says RayPoolKit's checklist is read "at the tag"; it is
   not at `v0.2.0` (`git show v0.2.0:docs/adoption/champbh.md` fails) and exists only at the checkout's
   `HEAD` (`1b84df0`), where I read it (§8, §9), as the orchestrator's facts read the board.
3. **IMPLEMENTATION CHOICE.** The orchestrator's facts say the measurements are taken at the agent's
   `HEAD` before it commits: both scripts ran at `ee79455`, and the verification document says so. The
   prompt's "at `2623905`/`1954adf`" figures are compared with them.
4. **IMPLEMENTATION CHOICE.** The tree in §2 of `architecture-summary.md` was rewritten from `ls`, so it
   also corrects `StarobinskyPotential.py` to the real `Starobinsky.py` and adds the factories'
   `__init__.py` and `Datastore/tests/`. `*_tags` rows of the registry table say "with its owner (not
   listed in `config/sharding.py`)", where the old table said "Sharded", since `config/sharding.py`
   does not list them.
5. **IMPLEMENTATION CHOICE.** §1.11's lead-in in `OPEN_ISSUES.md` does not state the campaign's state;
   I added one sentence (closed, 12 of 12, four rows stay open, unassigned) beside the header's, since
   the other closed campaigns' sections say so. No row moved.
6. **IMPLEMENTATION CHOICE.** Extra files under `measure/out/` beyond the prompt's list, all `11-*`:
   the sweep before and after, the `ast` output, the suites with their loads, Datastore by module, and
   the pip and provenance outputs.
7. **IMPLEMENTATION CHOICE.** The after-suites ran `-v` for Cosmology and Datastore (the latter for
   Z4's by-module counts); verbose output changes no count. The machine load was 21–27 during the
   after-run (other work on the machine; no suite of mine overlapped), so ComputeTargets took 341 s
   and 261 s against 96 s and 88 s before; nothing failed.
8. **IMPLEMENTATION CHOICE.** While looking for `shard_key_audit`'s usage I ran it with `--help`; it
   takes a path, found no file `…/--help` and exited. It opened no store.

No `UNINTENDED DRIFT` and no `STRUCTURALLY REQUIRED` deviation.

## Verification performed

**The tree at the start:** `HEAD` `ee79455`, no tracked change. **Suites** (serial, foreground; output in
`measure/out/11-suites.txt`):

| | Before (load 1-min at start) | After |
|---|---|---|
| CosmologyModels | 18 OK, 66.6 s (16.4) | 18 OK, 95.4 s (6.8) |
| ComputeTargets `test_[a-f]*` | 61 OK, 96.1 s (11.0) | 61 OK, 341.5 s (8.2) |
| ComputeTargets `test_[g-z]*` | 53 OK, 88.3 s (9.3) | 53 OK, 261.2 s (26.4) |
| Datastore | 97 OK, 11.6 s (12.2) | 97 OK, 48.7 s (21.7) |

Datastore by module: `test_compare_stores` 21, `test_datastore_config` 14, `test_inventory_report`
13, `test_inventory_specs` 10, `test_package_provenance` 10, `test_version_keyed_lookups` 9,
`test_bbn_failure_lookup` 6, `test_first_bounce_round_trip` 5, `test_fixed_T_values_round_trip` 5,
`test_scalarmodel_failure_reason` 4. No count moved. `black --check` on the six Python files: clean (and
the six `HEAD` copies were clean).

**The sweep** (`measure/out/11-stale-path-sweep-before.txt`, `-after.txt`). Before: **19 lines**, the
prompt's, at its line numbers. After: **20**.

| After-line | Class |
|---|---|
| `.documents/OPEN_ISSUES.md:53` | stays: dated record (the narrowing of `[00-two-files-are-not-black-clean]`) |
| `.documents/dk-and-rpk-migration-verification.md:12`, `:362`, `:620` | stay: dated record |
| `.documents/integrator-audit-2026-09-30/README.md:69` | stays: dated audit |
| `.documents/review-remediation-verification.md:1248` | stays: dated record |
| `Datastore/tests/test_datastore_config.py:255`, `:282` | stay: the guard that no pool passes the old layer's arguments (moved from `:254`, `:281` by one line by Z3's docstring) |
| `.documents/architecture-summary.md:216`, `:218` | new, **as deleted**: the dated note that opens §4 names the layer's modules and the old base class as what the layer *was* |
| `.documents/dk-and-rpk-migration-verification.md:1223`, `:1254`, `:1258`, `:1270`, `:1307`, `:1314`, `:1333`, `:1338` | new, **as deleted or as history**: §4's R0/R1 tree row, the script's `RayTools/ absent` row and non-`.py` count, the sentence on what the count includes, the RayPoolKit G3 draft (the checklist's `RayTools/` is gone) and its paragraph, and §4.5's "what goes stale" rows |
| `.documents/review-remediation-verification.md:1363`, `:1384` | new, **as deleted**: §4.12 says what the campaign replaced, and that §4.11's `_drop_actions` was the old layer's |

No line names a deleted path as current; no miss. The eight that stay are the prompt's. The six lines
the prompt marked to rewrite (`architecture-summary.md` 7, `compare_stores.py` 3, `test_compare_stores.py`
1) are gone.

**The `ast` check** (`11-ast-prose-only.txt`): equal with docstrings removed for
`Datastore/tests/test_bbn_failure_lookup.py`, `ComputeTargets/tests/test_foreign_bbn_provenance.py`,
`Datastore/tests/test_scalarmodel_failure_reason.py`, `Datastore/tests/test_compare_stores.py` and
`Datastore/tests/test_datastore_config.py`. `tools/compare_stores.py` differs in exactly the two
`GLOBAL_BOOKKEEPING` constants:

```
line 108 -> 109:
  - "the layer's key, leased from the serial broker at insert (Datastore/SQL/Datastore.py:309-314, :670-674); foreign keys are resolved through it"
  + "the layer's key, leased from the serial broker at insert (datastorekit/SQL/Datastore.py:794-806, in _insert from :760); foreign keys are resolved through it"
line 110 -> 112:
  - 'datetime.now() at insert (Datastore/SQL/Datastore.py:679-680)'
  + "datetime.now() at insert (datastorekit/SQL/Datastore.py:813); a replicated row has the pool's one timestamp, the same on every shard (:514, :666)"
```

Changed lines, from `git diff -U0`: `tools/compare_stores.py` `:89-92` (comment, now 5 lines) and
`:107-111`; `test_bbn_failure_lookup.py:30-31` (docstring); `test_foreign_bbn_provenance.py:38-40`
(docstring); `test_scalarmodel_failure_reason.py:95` (comment); `test_compare_stores.py:844-845`
(docstring); `test_datastore_config.py:112` (docstring). The cited lines were re-read in the installed
`datastorekit` 0.3.0, which equals `v0.3.0` (`cmp` of `SQL/Datastore.py` and `SQL/ShardedPool.py`
against `git show v0.3.0:`): `_write_shard_data` creates the six primary tables at
`SQL/ShardedPool.py:989-995`; `_insert` is at `SQL/Datastore.py:760`, leasing the serial at `:794-806`
and stamping `insert_timestamp if … else datetime.now()` at `:813`; the pool passes one timestamp at
`SQL/ShardedPool.py:514` and `:666`. The comment now says a primary built by ChamPBH's layer holds
five of the six and one built through the package all six, which `LAYER_TABLES` already lists.

**Z4** (`measure/out/11-*`; both scripts at `ee79455`, from each package's root with its own venv).
Against the prompt's figures: DatastoreKit's: 7 absent, 204 `.py` read, 0 unparsed, 1 layer import in
`prompts/run-integrity/planning-probes/datastore_version_probe.py:18`, 0 outside `prompts/`, 0
literals, 0 `import_module`, 18 factory-module imports (17 in `config/`, 1 top level), 0 moved names:
**equal**. RayPoolKit's: `RayTools/` absent, 0 / 0 / 0, 17 calls, `constructs` 17, `ray.is_initialized()`
False: **equal**; non-`.py` lines **252** (25 in `.documents/`, 227 in `prompts/`) against 240 (25, 215),
as the orchestrator said it would grow; 563 files tracked, 206 `.py` parsed. `pip freeze` 101 lines,
the baseline's 99 plus `datastorekit` and `raypoolkit` (sorted diff), `pip check` clean, the provenance
line exactly log 07's. The scripts read a commit, not the working tree; the orchestrator re-runs them
at this commit.

**Documents.** `git diff --numstat` on `.documents/`: deletions only in `architecture-summary.md` (178)
and `OPEN_ISSUES.md` (2, the header's description of this board and the §1.11 lead-in's first line); 0 in
either verification document. The index: `grep -c '^- \`\[' .documents/OPEN_ISSUES.md` = **27**, the
header's count; §1.11 has **4** rows, which are the board's §3's four; `prompts/INDEX.md`'s row says
"§1.11; 4" and its header "0 planned, 0 live, 7 closed", dated 2026-10-13.

**Citations in Z1 and Z6** (checked by import and `os.path.exists`; 0 missing, Ray not initialised):
`datastorekit` (`DatastoreObject`), `.object`, `.SQL.factory_base.SQLAFactoryBase`, `.SQL.schema`
(`build_schema`, `dependent_tables`, `drop_order`), `.store_reader.open_read_only`,
`.store_inventory` (`read_inventory`, `InventorySpec`), `.contract` (`VERSION_SERIAL_KEY`,
`require_version_serial`), `.SQL.ShardedPool` (`ShardedPool` with `object_get`, `object_get_vectorized`,
`object_read_batch`, `object_store`, `object_validate`, `read_table`, `copy_store`, `move_store`,
`closed_store_files`, `delete_store`), `.SQL.Datastore`, `.SQL.SerialPoolBroker`, `.SQL.ProfileAgent`,
`.SQL.ClientPool`, `.tools.sharded_store`, `.tools.shard_key_audit`; `config.datastore` (`factories`,
`drop_groups`, `serial_batch_sizes`, `tables_to_drop`), `config.sharding` (`read_table_config`,
`get_shard_key_store_id`, `ShardKeyType`), `config.version.VERSION_LABEL`,
`pipeline_selection.BBN_REFRESH_ROUTE`, `tools.inventory_report.format_inventory_report`; the files
`Datastore/tests/test_version_keyed_lookups.py`, `test_inventory_specs.py`, `standin_pool.py`,
`ray_cluster.sh`, `config/argument_parser.py` (`--prune-unvalidated` default True at `:257-261`;
`--ray-address` default `auto`), `tools/compare_stores.py`, `tools/bbn_from_store.py`. Contract sections
cited (§1–§9) exist at `v0.3.0`; the two factory facts (`key_on_version` in the three compute-target
factories, `store`/`validate`/`validate_on_startup` in the same three, `_do_not_populate` read by
`ScalarModel` and `BBNData`'s `build`) were read in the tree. The run scripts' line numbers were read
with `cat -n`; `tables_to_drop(["bbn-data"])` returns `['BBNData_tags', 'BBNDataValue', 'BBNData']`.

**The 20 `RayWorkPool` lines of `architecture-summary.md`** (new numbering; the old in brackets): all
true of `raypoolkit` `v0.2.0` and **left**, but where noted. `:35` (35, done at 04: the work-queue
line); `:152`, `:154` (152, 154: §3.2, 04); `:254` (244: the `DatastoreObject` row, rewritten with §4,
keeps the sentence that `available` is the sentinel the pool uses); `:382` (355: pass 2 of the two-pass
pattern, rewritten with §4, same claim); `:615` (585, 04); `:678` (648, true); `:859` (829, true);
`:885`, `:886`, `:892` (855, 856, 862: the stage diagram, true); `:898` (868: says
`payload={"small_network": True}`, which §9's dated notes correct; **not 11's**); `:950` (920: the
example, its `object_get_vectorized` line corrected to the mapping form, the rest unchanged);
`:1001` (969: resilience, true); `:1034` (999: "`RayWorkPool` (entire file)", **rewritten** to "the
`raypoolkit` package"); `:1089` (1056, true); `:1122` (1086: Step 5's example, constructs); `:1144`,
`:1146`, `:1152` (1108, 1110, 1116: gotchas, true).

**The G3 drafts** are in verification §4.3 (DatastoreKit's cell; its existing italic notes copied from
the board by script at DatastoreKit `04a2ed9`, not retyped) and §4.4 (RayPoolKit's row at `1b84df0`, and
the optional "G3 recorded" paragraph). Neither is committed in its repository.

## The deliberate-breakage record

None: 11 adds no code and no test (prompt §3.6). The sweep's non-vacuity is its 19 lines before any edit.

## Observations not acted on

1. `architecture-summary.md` §11's "Database startup cleanup" says each `Datastore` actor deletes
   unvalidated objects under `prune_unvalidated`; under the package the pool prunes the replicated
   classes and each actor its sharded ones (contract §1, row 11). The sentence is not a `RayWorkPool`
   line and §11 is not in the prompt's list, so it is left. Not opened as an issue.
2. `--shards` is unused by `--inventory` since 09; §5.1's argument list in `architecture-summary.md` does
   not say so (§4.12 does). Left.
3. The tree in `architecture-summary.md` still omits `config/version.py` (it existed before this
   campaign); only `config/datastore.py` was added, as the prompt says.
4. The rehearsal stores' `10-reference.log` was restored by the user on 2026-10-13 (board §5.15); 11 did
   not touch the rehearsal root.
5. The suites ran slowly because of load 21–27 on the machine during the after-run; recorded, not a
   finding (timing is no acceptance row).

## State handed to the next prompt

There is no next prompt: the campaign is closed. What the orchestrator and the user hold:

- **This commit** closes `dk-and-rpk-migration`: 12 of 12 prompts landed; gates R0, R1, R2, RayPoolKit G3
  and DatastoreKit G3 hold; the board is COMPLETE (§5.17), `prompts/INDEX.md` reads "0 planned, 0 live,
  7 closed", and `.documents/OPEN_ISSUES.md` stays at **27** open, **4** on this board (§1.11), none
  assigned.
- **For the orchestrator:** substitute this commit's SHA for "this commit" (the board, in its table rows, §1.1 and §5.17; the
  index row; and this log); re-run both packages' scripts at this commit (they should equal
  `measure/out/11-*` but for the commit line, the file counts and RayPoolKit's non-`.py` count, which
  also counts verification §4 and §4.12); check the sweep prints the 20 lines above; weigh `CLAUDE.md`'s
  Ray bullet, which is the orchestrator's addition (README §2.2), for the user.
- **Left for the user** (README §6.4, board §5.17): the merge of `dk-and-rpk-migration` into `main`
  (P17); the two G3 commits, from verification §4.3 (DatastoreKit's `✅ 2026-10-13: holds …` cell) and
  §4.4 (RayPoolKit's `**holds** 2026-10-12 …` row and its optional paragraph) (P16); the science rebuild
  and the run scripts, line by line in `.documents/review-remediation-verification.md` §4.12 (best made
  once, after `jordan-normalization` if that is planned next; on ChamPBH's isolated cluster with
  `--ray-address 127.0.0.1:6380`); removing the rehearsal stores; read-only plot pools
  (`[08-plot-scripts-open-read-write-pools]`); renaming `Datastore` (P6); a required `--ray-address`.
- **Suites:** 18 / 114 (61 + 53) / 97. **No ChamPBH Ray process** was started or is running; the
  untracked pilot files, run scripts and `prompts/jordan-normalization/` are untouched.
