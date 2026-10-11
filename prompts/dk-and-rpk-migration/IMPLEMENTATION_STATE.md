# dk-and-rpk-migration campaign — implementation state

**Last updated:** 2026-10-12 · **Status: LIVE — 4 of 11 prompts written (01–04), 1 landed (01). P1–P17
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
| 02 | [Rehearse the start tree (R0)](02-rehearse-the-start-tree.md) | **R0**; §4.3's measurement | Opus | ✍️ 2026-10-11 | ⬜ | — | — |
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

### 1.1 Items

| Item | Prompt | What | State |
|---|---|---|---|
| **T1** | 01 | `utilities.installed_package_provenance()` and `format_package_provenance()`; one line in each driver; `Datastore/tests/test_package_provenance.py` | ✅ `d7791c5`: 10 tests; the line reads `>> packages: datastorekit not installed; raypoolkit not installed` today (format in log 01) |
| **T2** | 01 | `tools/compare_stores.py` (comparison by value, schema snapshot) and `Datastore/tests/test_compare_stores.py` | ✅ `d7791c5`: 21 tests (with T3's); bookkeeping list and polymorphic map in log 01 |
| **T3** | 01 | `.documents/dk-and-rpk-migration/rehearsal/`: `build.sh`, `interrupt_when.py`, `stage_counts.py`, `run_readers.sh` | ✅ `d7791c5`: usage checked; never run on a build |
| **L1**–**L4** (R0) | 02 | README §4.1 at 01's tree; §4.3's reproducibility measured; L4 recorded as the baseline | ⬜ |
| **V1** | 02 | `.documents/dk-and-rpk-migration-verification.md` created, with §1 R0; R0's schema snapshot | ⬜ |
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
| R0 | the start tree builds the pilot, resumes it, and its readers run (README §4.1); its reproducibility is measured (§4.3) | 02 | ⬜ |
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
