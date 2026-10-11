# dk-and-rpk-migration campaign — implementation state

**Last updated:** 2026-10-11 · **Status: LIVE — 4 of 11 prompts written (01–04), none landed. P1–P17
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
| 01 | [Rehearsal tooling and run provenance](01-rehearsal-tooling-and-run-provenance.md) | P3, P5; README §4.1–§4.2 | Opus | ✍️ 2026-10-11 | ⬜ | — | — |
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
before each dispatch.

### 1.1 Items

| Item | Prompt | What | State |
|---|---|---|---|
| **T1** | 01 | `utilities.installed_package_provenance()` and `format_package_provenance()`; one line in each driver; `Datastore/tests/test_package_provenance.py` | ⬜ |
| **T2** | 01 | `tools/compare_stores.py` (comparison by value, schema snapshot) and `Datastore/tests/test_compare_stores.py` | ⬜ |
| **T3** | 01 | `.documents/dk-and-rpk-migration/rehearsal/`: `build.sh`, `interrupt_when.py`, `stage_counts.py`, `run_readers.sh` | ⬜ |
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

None yet.

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
