# dk-and-rpk-migration campaign — implementation state

**Last updated:** 2026-10-12 · **Status: LIVE — 4 of 11 prompts written (01–04), 2 landed (01, 02); R0 bitwise reproducible. P1–P17
ruled; DatastoreKit `v0.2.3` released (README §0.3).**
The plan is [`README.md`](README.md), written on 2026-10-11 at `52142d7` with the user's rulings
U1–U4. Prompts 01–04 are written once the user has reviewed it (README §2.1).

The campaign adopts `raypoolkit` `v0.2.0` and then `datastorekit` `v0.2.3`, a release requested
from DatastoreKit by [`datastorekit-v0.2.3-brief.md`](datastorekit-v0.2.3-brief.md). A rehearsal by value
comes before any change (R0), before the DatastoreKit swap (R1) and after it (R2). Each package's
gate G3 is recorded here when it holds.

**Campaign:** [`README.md`](README.md) ·
**Checklists:** DatastoreKit's and RayPoolKit's `docs/adoption/champbh.md` (read, never edited) ·
**Code:** `utilities.py`, `tools/`, `requirements.txt`, `RayTools/`, `Datastore/`, `config/`,
`main.py`, `plot_by_beta.py`, `plot_ScalarModel.py`, `pipeline_selection.py` ·
**Verification:** `.documents/dk-and-rpk-migration-verification.md` (created by 02) ·
**Index:** [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md), a section opened with
this board's first issue

> **Maintenance rule.** Whenever an entry is added to, narrowed in, or closed out of §3 or §4
> below, [`.documents/OPEN_ISSUES.md`](../../.documents/OPEN_ISSUES.md) is updated **in the same
> commit**: the row is added, moved or deleted, and the count and date in its header are
> corrected. See `CLAUDE.md`.

### Decisions

**Ruled by the user, 2026-10-11 (README §6.1):**

| Decision | Decided |
|---|---|
| **U1** — the order | RayPoolKit first, then DatastoreKit |
| **U2** — the rehearsals | the agent runs a small pilot (README §4.1) at R0, R1 and R2: reference, killed-and-resumed, readers. A one-off exception to `CLAUDE.md`'s limits, for prompts 02, 06 and 10 |
| **U3** — the factories (reopened the same day) | fix the package: DatastoreKit `v0.2.3` makes `store`, `validate` and `validate_on_startup` non-abstract defaults; ChamPBH keeps its registry of instances. First ruled "register the classes", then reopened when the user asked why the conventions differed (README §6.1) |
| **U4** — a drop that leaves references | refused before Ray starts, naming the groups to add |

**Proposed by the planner (README §6.2), ruled by the user 2026-10-11:** P1, P3–P8 and P10–P17
accepted (P1 on the condition that the 2026-10-02 pilot's grid still exists, which it does); P2
amended (the rehearsal stores under `~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/`); P9
accepted after README §6.3's explanation (`VERSION_LABEL` stays in `config/version.py`; the two
layer-owned names are imported directly from `datastorekit.contract`). The user's notes on P5 (SGK's `RunRegistry` to
come), P6 (the folder structure to be tidied) and P11 (read-only plot pools wanted eventually) are
recorded beside each proposal.

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [Rehearsal tooling and run provenance](01-rehearsal-tooling-and-run-provenance.md) | P3, P5; README §4.1–§4.2 | Opus | ✍️ 2026-10-11 | ✅ 2026-10-12 | `d7791c5` | [log](logs/01-rehearsal-tooling-and-run-provenance.md) |
| 02 | [Rehearse the start tree (R0)](02-rehearse-the-start-tree.md) | **R0**; §4.3's measurement | Opus | ✍️ 2026-10-11 | ✅ 2026-10-12 | `6cbf953` | [log](logs/02-rehearse-the-start-tree.md) |
| 03 | [Pin `raypoolkit` `v0.2.0`](03-pin-raypoolkit-v0.2.0.md) | the RayPoolKit pin | Sonnet | ✍️ 2026-10-11 | ⬜ | — | — |
| 04 | [Adopt `raypoolkit`](04-adopt-raypoolkit.md) | the RayPoolKit swap; **RayPoolKit G3** | Sonnet | ✍️ 2026-10-11 | ⬜ | — | — |
| 05 | Move ChamPBH's data out of the layer | `config/datastore.py`; the vectorized gets | Sonnet | ⬜ | ⬜ | — | — |
| 06 | Rehearse before the DatastoreKit swap (R1) | **R1** | Opus | ⬜ | ⬜ | — | — |
| 07 | Pin `datastorekit` `v0.2.3` | the DatastoreKit pin (**U3**; `0de2b16`) | Sonnet | ⬜ | ⬜ | — | — |
| 08 | Adopt `datastorekit` | the DatastoreKit swap; **U4** | Opus | ⬜ | ⬜ | — | — |
| 09 | Port the inventory | `inventory_spec`, `tools/inventory_report.py` | Opus | ⬜ | ⬜ | — | — |
| 10 | Rehearse through both packages (R2) | **R2**; **DatastoreKit G3** | Opus | ⬜ | ⬜ | — | — |
| 11 | Close the migration | documents, handover, close | Sonnet | ⬜ | ⬜ | — | — |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

**Held, by design (README §2.1).**
- **01–04** are written after the user's review of the plan.
- **05–06** are written after 02's review: R0's reproducibility sets R1's criterion.
- **07–10** are written after 06's review (`v0.2.3` is released).
- **11** is written after 10's review.

Orchestrator notes are written one prompt at a time, in [`orchestrator/`](orchestrator/), just
before each dispatch:
- [`orchestrator/prompt-01.md`](orchestrator/prompt-01.md) (`0e64ef0`) — used for 01.
- [`orchestrator/prompt-02.md`](orchestrator/prompt-02.md) (`a72ca5e`) — used for 02.

### 1.1 Items

| Item | Prompt | What | State |
|---|---|---|---|
| **T1** | 01 | `utilities.installed_package_provenance()` and `format_package_provenance()`; one line in each driver; `Datastore/tests/test_package_provenance.py` | ✅ `d7791c5`: 10 tests; the line reads `>> packages: datastorekit not installed; raypoolkit not installed` today (format in log 01) |
| **T2** | 01 | `tools/compare_stores.py` (comparison by value, schema snapshot) and `Datastore/tests/test_compare_stores.py` | ✅ `d7791c5`: 21 tests (with T3's); bookkeeping list and polymorphic map in log 01 |
| **T3** | 01 | `.documents/dk-and-rpk-migration/rehearsal/`: `build.sh`, `interrupt_when.py`, `stage_counts.py`, `run_readers.sh` | ✅ `d7791c5`: usage checked; never run on a build |
| **L1**–**L4** (R0) | 02 | README §4.1 at 01's tree; §4.3's reproducibility measured; L4 recorded as the baseline | ✅ `6cbf953`: L1–L3 hold; **§4.3 bitwise** (`compare_stores` `RESULT: equal`, exit 0); L4: `compute` 5/5/5, 5/killed, 0/2/5; the prune removed only `resumed-a`'s one unvalidated row (verification §1) |
| **V1** | 02 | `.documents/dk-and-rpk-migration-verification.md` created, with §1 R0; R0's schema snapshot | ✅ `6cbf953`: §1 R0; `out/02-reference-schema.json` (27 client tables) |
| **K1** | 03 | the `raypoolkit` pin in `requirements.txt` | ⬜ |
| **K2** | 03 | the venv installed from it | ⬜ |
| **K3** | 03 | the proof: one distribution added, at `eaef0ce6…`, the tag's files, not editable | ⬜ |
| **W1** | 04 | `RayTools/` deleted | ⬜ |
| **W2** | 04 | the 3 imports rewritten to `from raypoolkit import RayWorkPool` | ⬜ |
| **W3** | 04 | no pool site changed (P7) | ⬜ |
| **W4** | 04 | `.documents/architecture-summary.md`'s `RayWorkPool` lines made true (P14) | ⬜ |

Items of 05–11 are added when they are written.

The measurement the plan was written against is [`measure/out/`](measure/out/): both packages'
scripts at `52142d7`, each equal to its checklist's appendix byte for byte; the three roots'
counts (18 / 114 / 35); and the venv's `pip freeze` (README §0.2).

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| R0 | the start tree builds the pilot, resumes it, and its readers run (README §4.1); its reproducibility is measured (§4.3) | 02 | ✅ **holds** 2026-10-12 (`6cbf953`): bitwise reproducible, so R1 and R2 are held to bitwise equality with `02-reference.db` (P4) |
| RayPoolKit G3 | `RayTools/` is gone, 0 imports, the pin is installed, the three roots pass, all 17 sites construct (RayPoolKit checklist item 8) | 04 (R1, 06, adds the run under real Ray) | ⬜ |
| R1 | 05's tree equals R0 by value | 06 | ⬜ |
| DatastoreKit `v0.2.3` | tagged on DatastoreKit's remote, with `store`, `validate` and `validate_on_startup` defaulted (U3; made in DatastoreKit, not here) | — (07 pins it) | ✅ **released** 2026-10-11: tag object `8dac562`, commit `0de2b16`, checked on the remote and by `git show` of `factory_base.py` (README §0.3) |
| DatastoreKit G3 | the 9 layer files are gone (two emptied, P6), the pin is installed, the three roots pass, and a pilot store rebuilt through the package (R2) equals R1 by value | 08, 09, 10 | ⬜ |

Recording each gate on its package's own extraction board is a commit in that repository, not
here (README P16).

## 3. Active and unresolved issues

### `[01-scalarmodelvalue-build-checks-the-stored-phi-against-pi]`

**Opened:** 2026-10-12 by prompt 01 (log 01, Observation 2), seen in passing while reading the
factories for the bookkeeping list. **Not fixed:** a factory is outside prompt 01's scope, and
outside this campaign's (README §1, "The factories' form").

- **What.** `sqla_ScalarModelValue_factory.build`, on finding a stored row, checks the stored φ
  against the expected φ and then, meaning to check π, compares the stored **φ** with the
  expected **π**: `fabs(row_data.phi_Einstein_Mp - pi_Einstein_Mp) > DEFAULT_FLOAT_PRECISION`
  (`Datastore/SQL/ObjectFactories/ScalarModel.py:1063-1069`; the message prints
  `row_data.pi_Einstein_Mp`). Present since `d132774`.
- **Impact.** None measured, and none expected today: nothing calls
  `object_get("ScalarModelValue", …)` (`git grep`); the value rows are written by
  `sqla_ScalarModelFactory.store` through the inserter and read back by its `build`. If the path
  were used, a stored row whose φ and π differ by more than `DEFAULT_FLOAT_PRECISION` would raise
  `ValueError` spuriously, and a wrong stored π would pass unchecked. No stored value is affected.
- **Next step.** A one-token fix (`row_data.pi_Einstein_Mp`), or deleting the unused lookup path,
  in a later campaign that may touch the factories, with a test that drives the path.

### `[02-scalar-stage-missing-filter-keeps-every-model]`

**Opened:** 2026-10-12 by the orchestrator's review of prompt 02, from log 02's Observation 2,
which left the question open. **Not fixed:** `main.py`'s pipeline is outside this campaign's scope
(README §1), and R0's baseline is the behaviour as it stands.

- **What.** `build_solver_batch`'s "which models are missing?" comprehension is
  `[m for obj, m in zip(query_outcomes, binned_batch[key])]` (`main.py:223`, since `b662d4b`,
  2026-01-24). It never reads `obj`, the vectorized lookup's result, so it keeps every model. Each
  stored `ScalarModel` is then looked up a second time by its own
  `object_get(…, _do_not_populate=True)` (`main.py:234-258`). The `num_missing == 0` early return
  (`:229-230`) can never fire. The adiabatic stage filters with `select_missing`
  (`main.py:432-437`), and so does the BBN stage (`:694`).
- **Impact.** No stored value is affected: R0's resume found every stored model on the second
  lookup and computed none (log 02, stage counts 5 lookup / 0 compute), and the two stores are
  bitwise equal. On the five-model pilot the extra lookups cost under a second. On a re-run of a
  large store each stored model costs one extra individual lookup. A stored *failed* `ScalarModel`
  goes through the same second lookup, and what the pool then does with it was not measured here.
- **Next step.** Filter on `obj.available` (or use `select_missing`, as the later stages do), in a
  campaign that may touch `main.py`, with a test that a fully stored batch returns no work.
  06 and 10 compare against R0's lookup pattern (5 / 2 / 5 on `resumed-b`), so a fix that lands
  before 10 changes that pattern, and L4 must say so.

## 4. Resolved issues

None yet.

## 5. Dispatch record

### 5.0 Planning (2026-10-11)

- Re-measured at `52142d7`: both checklists' appendices reproduced byte for byte; suites 18 / 114
  / 35; neither package installed; both tags on their remotes (README §0.2).
- The user ruled U1–U4 in the planning conversation. U3 was first ruled "register the classes"
  (a prompt 05 of its own), then reopened the same day and ruled as a DatastoreKit fix; the
  class-registration prompt was withdrawn and the prompts renumbered, 12 to 11. The request to
  DatastoreKit is [`datastorekit-v0.2.3-brief.md`](datastorekit-v0.2.3-brief.md).

### 5.1 The release and the rulings (2026-10-11)

- DatastoreKit's `factory-defaults` campaign released `v0.2.3` (tag object `8dac562`, commit
  `0de2b16`), and relayed four points the brief had not asked about (README §0.3): the neutral
  client's `ABSTRACT_HOOKS` corrected; a registered instance shown to survive Ray's serialisation
  only by its smoke script's new step I, since the stand-in pool pickles nothing; SI's import-time
  instantiations fixed too; and its observation §7.3 recorded as addressed.
- The user ruled on P1–P17 (above). The rehearsal path moved to `~/ScienceData/`, which does not
  exist yet; 02 creates it.

### 5.2 After prompt 01 (2026-10-12)

- **Landed:** `d7791c5`, log [`01-rehearsal-tooling-and-run-provenance.md`](logs/01-rehearsal-tooling-and-run-provenance.md).
  **COMPLETE**: T1–T3; no §4 stop met.
- **T1.** `utilities.installed_package_provenance()` / `format_package_provenance()`; each driver
  prints one line, before `ray.init(` and its `ShardedPool(`. Today:
  `>> packages: datastorekit not installed; raypoolkit not installed`.
- **T2.** `tools/compare_stores.py`, reading every file `mode=ro` through `sqlite3`, the shards by
  glob and never by the `shards` table's records. Bookkeeping: `serial`, `timestamp` everywhere;
  `label` and the timings of `ScalarModel`, `AdiabaticHistory` (including `compute_time`, which the
  prompt's list missed) and `BBNData`; the seconds elapsed in a BBN wall-clock failure's reason
  masked (D2, for review). Polymorphic map: `ScalarModel`'s three pairs, cosmology 3 →
  `QCD_Cosmology` (0–2 declared unstored), potentials 0–4, coupling 0. `LambdaCDM` is a note;
  `ExponentialCoupling` and the `*_tags` tables are sharded. A ChamPBH primary holds no client
  table (from the code).
- **T3.** The four rehearsal scripts; usage checked; never run on a build.
- **Suites** (foreground, serial): CosmologyModels 18 → 18; ComputeTargets 114 → 114 (61 + 53);
  Datastore 35 → **66** (+10 `test_package_provenance`, +21 `test_compare_stores`).
- **Breakages:** all five of prompt §3.5 caught, each recorded in the log as a diff that
  `git apply` replays.
- **Opened:** §3 `[01-scalarmodelvalue-build-checks-the-stored-phi-against-pi]` (latent; indexed).
- **For 02:** `--prune-unvalidated` defaults to true, so every build prunes at open (log 01,
  Observation 1).

**Orchestrator review of prompt 01 (2026-10-12)**, by orchestrator note
[`prompt-01.md`](orchestrator/prompt-01.md), at `d7791c5`. All ten checks pass; one residue fixed.
- **(1) Scope.** 14 files, all on the prompt's §5 list plus the index (an issue opened). The
  untracked set is unchanged.
- **(2–3) T1.** The helper reads each field from its PEP 610 place. Eleven odd cases and a
  `read_text` raising `PermissionError` all return without raising (`vcs_info`/`dir_info` a list
  or a string; `editable` false or the string `"true"`, both `False`; no `requested_revision`; a
  JSON list; not JSON; not UTF-8). Against the real `importlib.metadata`, with a stand-in
  `datastorekit-0.2.3.dist-info` first on `sys.path` and no mock, a VCS install at `v0.2.3` /
  `0de2b161…` gives every field and the line
  `>> packages: datastorekit 0.2.3 (git+https://github.com/ds283/DatastoreKit@0de2b161ba09362adc36231a7b5b12d99dec8bb3, requested v0.2.3); raypoolkit not installed`.
  An editable install reads `(editable, file:///…)`, and one with no `direct_url.json` reads
  `(no recorded origin)`.
- **(4) Drivers, by `ast` against `ea3d192`.** Each differs by one import (in `main.py`, an
  extended one) and one module-level print, before `ray.init(` and the `with ShardedPool(`. Every
  other top-level statement dumps identically. In `utilities.py` only the two imports, the
  constant and the two functions are new.
- **(5) Opens.** On 16-shard copies of the 2026-10-02 pilot store, with a logger around
  `sqlite3.connect`: 34 opens, all `file:…?mode=ro` with `uri=True`, all inside the copies, and
  none of the originals that the copies' `shards` tables name. The tool imports no layer, Ray or
  SQLAlchemy, and has no `ATTACH`.
- **(6) The pilot copy.** A against B gives exit 0: 27 client tables, ~120,000 rows, 17 s, with
  `LambdaCDM` noted. Each perturbation was applied to B only:
  - one `ScalarModelValue.raw_N` moved one ulp gives exit 1, paired as a near miss with both hex
    values (relative difference 1.6e-16);
  - `ScalarModel.label` and `compute_time` changed give exit 0, both reported as bookkeeping;
  - `potential_type = 99` gives exit 1 and a named "unknown type value", with no traceback;
  - one `redshift.z` on shard 7 only gives exit 1 and a named "replicated copies differ";
  - one `BBNData` row deleted gives exit 1, with the A-only row named by its labels;
  - one missing shard file gives exit 2;
  - the schema snapshot round-trips, and a dropped `ScalarModel` index is reported.

  The copies were deleted afterwards.
- **(7) The maps.** Each bookkeeping entry was confirmed where it is written. The potential map
  matches `CosmologyConcepts/Potentials/model_ids.py` (0–4), and `QCD_Cosmology.type_id` returns
  `XAV_IMPROVED_EOS_IDENTIFIER` (3). Test 13 checks every map entry against the registered
  factories and the identifier modules, so it is not vacuous. **D2 (the masked elapsed seconds in
  `BBNData.failure_reason`) is accepted.** It is the prompt's own rule for `extra_data`, applied
  to the one other column carrying a time. A wall-clock failure in one build and a result in the
  other (README §4.3's candidate) still differ, on `failure` and on the rest of the reason.
- **(8) Scripts and breakages.**
  - `build.sh`'s command matches README §4.1 token for token.
  - `interrupt_when.py` was tried on a `sleep 600`: it waited for the banner and then the 4 s
    delay, sent `SIGTERM` to that pid only (exit 143), and left a second process alone. When the
    process exited before the banner, it said so and returned 1.
  - All four scripts print their usage. `stage_counts.py` parses `pilot-main.log` correctly.
  - Replayed in bash, B1, B2, B3 and B5 fail their named tests for the stated reasons. **B4 did
    not apply as recorded:** the log's diff had lost its trailing blank context line. With
    `--recount` it fails `test_editable_install` on the value. *Residue, fixed in this
    follow-up:* the context line is restored in the log, and all five now pass
    `git apply --check`.
- **(9) Stores.** The 52 store files under `~/ChamPBH-stores*/` and the pilot store have
  unchanged mtimes. `~/ScienceData/` does not exist.
- **(10) Records and suites.** The §1 row, T1–T3, §5.2, the §3 entry and index §1.11 are present
  (the issue was confirmed at `ScalarModel.py:1063-1069`). At `d7791c5` the suites give
  CosmologyModels 18, ComputeTargets 114 (61 + 53) and Datastore 66, all OK. `black` is clean on
  all nine Python files.
- **Handed to 02's note:**
  - `--prune-unvalidated` is on by default (log 01, Observation 1), so README §4.1's flag on
    `resumed-b` is a no-op, and L2 must read every build's prune report.
  - A difference in a parent that cannot pair (an unmapped type value, a deleted row) lists its
    children unpaired, thousands of rows in the JSON, alongside the named problem.
  - `build.sh` and `run_readers.sh` refuse an existing log or readers directory.

### 5.3 After prompt 02 (2026-10-12)

- **Landed:** `6cbf953`, log [`02-rehearse-the-start-tree.md`](logs/02-rehearse-the-start-tree.md);
  the record is `.documents/dk-and-rpk-migration-verification.md` §1. **COMPLETE**: L1–L4 (R0),
  V1; gate R0 holds; no §4 stop met; no code changed.
- **Cluster and load.** One cluster, `127.0.0.1:6379`, started 01:12:29 and stopped 01:37:44. The
  load average was above the 10 CPUs throughout (peak 119 / 139 / 113 at 01:24). The sources were
  system daemons and, from 01:29:50, another session's DatastoreKit suite (log 02, Observation 4).
- **Builds.**
  - `reference`: 10 min 43 s, exit 0.
  - `resumed-a`: killed by `SIGTERM` 5 s after the adiabatic banner (`S` from `reference`'s
    timestamps: the stage took 12.6 s, rows at banner + 4.6–7.6 s). Exit 134, Ray's absl handler
    aborting (log 02, Observation 1). It kept 3 validated `AdiabaticHistory` rows and 1 unvalidated
    (β = 10); β = 25 was not stored.
  - `resumed-b`: 1 min 35 s, exit 0. Its open pruned exactly the unvalidated β = 10 row. It
    computed 0 / 2 / 5. Label counts: `AdiabaticHistory` 3 `resumed-a` + 2 `resumed-b`.
- **Actors.** All 17 of `resumed-a`'s were dead 28 s after the signal, and none was alive before
  `ray stop`.
- **Readers.** All six steps exit 0, and the before/after comparison is equal with no bookkeeping
  difference. The β = 1.15 re-solve equals the stored Yp and D/H to the 10 figures printed.
  `histories.csv` SHA-256 `d7c62c37…c522`.
- **§4.3: `RESULT: equal`, exit 0.** 27 tables, every row matched, bookkeeping differing on every
  matched row. **R0 is bitwise reproducible; R1 and R2 are held to bitwise equality** with
  `02-reference.db`.
- **Suites** (before `ray start`): 18 / 114 (61 + 53) / 66, unchanged.
- **Observations not opened:**
  1. a `SIGTERM`ed driver exits 134;
  2. the scalar stage re-looks-up every stored model, so its lookup count differs from the
     adiabatic stage's (`main.py:219-226` against `:432-437`);
  3. the plot scripts' cosmetic shard-count warning (they pass no `shards=`);
  4. the load, and the foreign suite;
  5. `bbn_from_store.py` prints its re-solve to 10 figures;
  6. `ray stop` kills a shell whose command line names Ray's processes.

**Orchestrator review of prompt 02 (2026-10-12)**, by orchestrator note
[`prompt-02.md`](orchestrator/prompt-02.md), at `6cbf953`. All ten checks pass. Residue is fixed
in this follow-up, and one issue is opened from Observation 2.
- **(1) Scope.** 9 files: the verification document, six `out/02-*` files (59 KB in all), the log
  and the board. No code, and the index is untouched. The untracked set is unchanged.
- **(2) Ray.** No Ray process remains, and `ray status` finds no instance.
- **(3) The logs, read in full.**
  - Each of the three has the provenance line once, with both packages not installed.
  - `reference` and `resumed-b` end `** driver exit status 0` with every stage complete.
    `resumed-a` ends with 134 after the adiabatic banner and before the BBN one.
  - The note expected 143. The log shows why it is 134: Ray's absl failure-signal handler dumped
    the main thread's native stack (waiting in `CoreWorker::Wait()`) and aborted, under pid 97614,
    the one in `02-resumed-a.pid`.
  - No `Traceback` or `Error` line in any build or reader log.
- **(4) The counts, repeated.** `stage_counts.py` on the three logs gives the committed text, and
  JSON equal to the committed JSON. Separately, a raw `sqlite3` `mode=ro` script of the
  orchestrator's own read both stores:
  - label counts: `reference` 5 / 5 / 5; `resumed` `ScalarModel` 5 `resumed-a`,
    `AdiabaticHistory` 3 `resumed-a` + 2 `resumed-b`, `BBNData` 5 `resumed-b`;
  - no unvalidated row in either store.

  So *k* = 3. That agrees with `resumed-b`'s adiabatic compute count of 2, the β = 10 row having
  been stored unvalidated, pruned and recomputed.
- **(5) The prune.** One report in all three logs, in `resumed-b`, from `shard0002-store`. It names
  only the unvalidated β = 10 `AdiabaticHistory` (store_id 4, 9776 of 9776 values) and says it was
  pruned.
- **(6) §4.3, re-run.** `compare_stores` of `02-reference.db` against `02-resumed.db` gives exit 0
  and `RESULT: equal` in 7 s. Its text is identical to the committed file, and its JSON equal.
  - All 27 client tables are listed with plausible counts: 5 histories, 42,617 value rows each in
    `ScalarModelValue` and `AdiabaticHistoryValue`, 7,475 in `BBNDataValue`, 21,250 `redshift`.
  - The differing bookkeeping columns are only `timestamp`, `label` and the timings.
  - **Independently of the tool**, the orchestrator's script resolved each row to its β and
    compared stored bits:
    - the `ScalarModelValue` rows of each history (10 float columns, ordered by `z`, BLAKE2b);
    - its `AdiabaticHistoryValue` rows;
    - its `BBNData` abundances, `failure`, `small_network` and `PRyM_version`.

    All five histories are equal on all three. **R0 is bitwise reproducible**, confirmed.
- **(7) The readers.** `status.txt` shows steps 0–5, each exit 0. The before/after comparison is
  `RESULT: equal`, with no bookkeeping difference. `histories.csv` has 5 rows, SHA-256
  `d7c62c37…c522` as logged. `bbn_from_store` gives stored Yp 0.24687349447579363 and D/H
  2.4859924812264325 for β = 1.15, and the `prod` re-solve 0.2468734945 and 2.485992481, equal to
  the 10 figures printed. The inventory lists 5 validated rows of each kind.
- **(8) The snapshot.** `--snapshot` of `02-reference.db` is byte-identical to
  `out/02-reference-schema.json`.
- **(9) Stores.** The 53 files of the note's §1 have unchanged mtimes and sizes. `$R` holds the two
  stores (17 files each), three logs and three pidfiles, and `02-readers/`.
- **(10) Records.** The verification document has its introduction and §1 R0 (§§1.1–1.8, with the
  commands verbatim). The board has the §1 row, L1–L4, V1, gate R0 and §5.3. Suites 18 / 114 / 66.
  - *Residue, fixed here:* "this commit" → `6cbf953` (board and log); the log's "five" changed
    files → nine; this note added to the list of orchestrator notes.
- **Deviations D1–D4** are accepted. D2 is what the note asked for. In D3 the snapshot read a
  closed store while another build wrote a different one.
- **Observations.**
  - *Opened:* Observation 2, as §3 `[02-scalar-stage-missing-filter-keeps-every-model]`. The
    comprehension at `main.py:223` never reads its lookup result.
  - *Not opened:*
    - Observation 1 is Ray's behaviour.
    - Observation 3 is a housekeeping candidate that 08 overtakes.
    - Observations 4–6 are recorded for the next notes.
  - The foreign DatastoreKit suite (Observation 4) broke the note's gate. The bitwise result held
    under it, and no BBN solve came near its limit.
- **Handed to 06's note.**
  - The criterion is bitwise equality with `02-reference.db` (P4).
  - `resumed-a` exits 134, not 143.
  - Run `ray stop` as a command of its own.
  - `ray start` at Ray 2.53.0 prints no address. It is `127.0.0.1:6379` by default, from
    `/tmp/ray/ray_current_cluster`.
  - The adiabatic stage takes about 12 s, not the pilot's 2 min 19 s, so `S` comes from 06's own
    `reference` timestamps.
  - L4 compares the resume's *pattern* (label counts; computes only what was not kept; prunes
    only unvalidated rows), with R0's lookups 5 / 2 / 5 on `resumed-b`. The figures depend on
    the signal's timing.
  - Hold other sessions' suites off the machine for the builds.
