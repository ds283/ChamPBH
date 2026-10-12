# dk-and-rpk-migration campaign — implementation state

**Last updated:** 2026-10-12 · **Status: LIVE — 11 of 12 prompts written (01–10, 05a), 8 landed (01–07, 05a); R0 bitwise reproducible; R1 equals R0 bitwise; RayPoolKit G3 holds. P1–P17
ruled; U5 ruled (README §6.6); U6 ruled (README §6.7): DatastoreKit `v0.3.0` is the pin (README §0.4).**
The plan is [`README.md`](README.md), written on 2026-10-11 at `52142d7` with the user's rulings
U1–U4. Prompts 01–04 are written once the user has reviewed it (README §2.1).

The campaign adopts `raypoolkit` `v0.2.0` and then `datastorekit` `v0.3.0` (U6), which carries
`v0.2.3`, the release requested from DatastoreKit by [`datastorekit-v0.2.3-brief.md`](datastorekit-v0.2.3-brief.md). A rehearsal by value
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

**Ruled by the user, 2026-10-12 (README §6.5):** 09 also proves that every registered class is
covered by a spec, read through one, or excluded with a reason (09's acceptance item 5), because
the fingerprint of SGK's RunRegistryKit, which ChamPBH is expected to adopt after this campaign,
reads only what the specs declare. Measured at `12c82e8`, 09's list already covers all 27 entries.

**Ruled by the user, 2026-10-12 (README §6.6):**

| Decision | Decided |
|---|---|
| **U5** — other projects' Ray clusters | isolate and lock. ChamPBH's cluster runs on `/tmp/ray-cpbh` and ports 6380 / 6381 / 8267 / 52367 and stops only itself, never by `ray stop`; one lock, `~/ScienceData/.rehearsal-lock`, shared with StochasticInstantons and SecondaryGWKit, lets one rehearsal run at a time. Contract: SI's `rehearsal-lock-brief.md` at `ed9399a`; ChamPBH's side is 05a |

**Ruled by the user, 2026-10-12 (README §6.7):**

| Decision | Decided |
|---|---|
| **U6** — which DatastoreKit release | **`v0.3.0`** (tag object `bf0a3b2`, commit `2f5417e`; README §0.4), in place of `v0.2.3`. It carries `v0.2.3`'s change unchanged (`factory_base.py` identical) and adds an optional, read-only `DatastoreObject.timestamp` that ChamPBH neither passes nor reads. 07 pins it |

---

## 1. Prompts

| # | Prompt | Covers | Model | Written? | Landed? | Commit | Log |
|---|---|---|---|---|---|---|---|
| 01 | [Rehearsal tooling and run provenance](01-rehearsal-tooling-and-run-provenance.md) | P3, P5; README §4.1–§4.2 | Opus | ✍️ 2026-10-11 | ✅ 2026-10-12 | `d7791c5` | [log](logs/01-rehearsal-tooling-and-run-provenance.md) |
| 02 | [Rehearse the start tree (R0)](02-rehearse-the-start-tree.md) | **R0**; §4.3's measurement | Opus | ✍️ 2026-10-11 | ✅ 2026-10-12 | `6cbf953` | [log](logs/02-rehearse-the-start-tree.md) |
| 03 | [Pin `raypoolkit` `v0.2.0`](03-pin-raypoolkit-v0.2.0.md) | the RayPoolKit pin | Sonnet | ✍️ 2026-10-11 | ✅ 2026-10-12 | `2f61943` | [log](logs/03-pin-raypoolkit-v0.2.0.md) |
| 04 | [Adopt `raypoolkit`](04-adopt-raypoolkit.md) | the RayPoolKit swap; **RayPoolKit G3** | Sonnet | ✍️ 2026-10-11 | ✅ 2026-10-12 | `303324b` | [log](logs/04-adopt-raypoolkit.md) |
| 05 | [Move ChamPBH's data out of the layer](05-move-champbh-data-out-of-the-layer.md) | `config/datastore.py`; the vectorized gets | Sonnet | ✍️ 2026-10-12 | ✅ 2026-10-12 | `8e77578` | [log](logs/05-move-champbh-data-out-of-the-layer.md) |
| 05a | [Isolate the rehearsal cluster](05a-isolate-the-rehearsal-cluster.md) | **U5**; `ray_cluster.sh` | Sonnet | ✍️ 2026-10-12 | ✅ 2026-10-12 | `babd9d5` | [log](logs/05a-isolate-the-rehearsal-cluster.md) |
| 06 | [Rehearse before the DatastoreKit swap (R1)](06-rehearse-before-the-datastore-swap.md) | **R1** | Opus | ✍️ 2026-10-12 | ✅ 2026-10-12 | `c9e6bc2` | [log](logs/06-rehearse-before-the-datastore-swap.md) |
| 07 | [Pin `datastorekit` `v0.3.0`](07-pin-datastorekit-v0.3.0.md) | the DatastoreKit pin (**U3**, **U6**; `2f5417e`) | Sonnet | ✍️ 2026-10-12 | ✅ 2026-10-12 | `617a1dc` | [log](logs/07-pin-datastorekit-v0.3.0.md) |
| 08 | [Adopt `datastorekit`](08-adopt-datastorekit.md) | the DatastoreKit swap; **U4** | Opus | ✍️ 2026-10-12 | ⬜ | — | — |
| 09 | [Port the inventory](09-port-the-inventory.md) | `inventory_spec`, `tools/inventory_report.py`; coverage (§6.5) | Opus | ✍️ 2026-10-12 | ⬜ | — | — |
| 10 | [Rehearse through both packages (R2)](10-rehearse-through-both-packages.md) | **R2**; **DatastoreKit G3** | Opus | ✍️ 2026-10-12 | ⬜ | — | — |
| 11 | Close the migration | documents, handover, close | Sonnet | ⬜ | ⬜ | — | — |

**Legend.** ✍️ written · ⏸ held, with what it waits on · ⬜ not yet · ✅ landed.

**Held, by design (README §2.1).**
- **01–04** are written after the user's review of the plan.
- **05–06** are written after 02's review: R0's reproducibility sets R1's criterion.
- **05a** is written after 05's review, for U5. 06 waits on it.
- **07–10** are written after 06's review (`v0.3.0`, the pin, is released; U6). *Written 2026-10-12 (§5.11).*
- **11** is written after 10's review.

Orchestrator notes are written one prompt at a time, in [`orchestrator/`](orchestrator/), just
before each dispatch:
- [`orchestrator/prompt-01.md`](orchestrator/prompt-01.md) (`0e64ef0`) — used for 01.
- [`orchestrator/prompt-02.md`](orchestrator/prompt-02.md) (`a72ca5e`) — used for 02.
- [`orchestrator/prompt-03.md`](orchestrator/prompt-03.md) (`e287a40`) — used for 03.
- [`orchestrator/prompt-04.md`](orchestrator/prompt-04.md) (`12c82e8`) — used for 04.
- [`orchestrator/prompt-05.md`](orchestrator/prompt-05.md) (`891ae82`) — used for 05.
- [`orchestrator/prompt-05a.md`](orchestrator/prompt-05a.md) (`b114692`) — used for 05a.
- [`orchestrator/prompt-06.md`](orchestrator/prompt-06.md) (`5bb2be6`) — used for 06.
- [`orchestrator/prompt-07.md`](orchestrator/prompt-07.md) (`4f3f44d`) — used for 07.

### 1.1 Items

| Item | Prompt | What | State |
|---|---|---|---|
| **T1** | 01 | `utilities.installed_package_provenance()` and `format_package_provenance()`; one line in each driver; `Datastore/tests/test_package_provenance.py` | ✅ `d7791c5`: 10 tests; the line reads `>> packages: datastorekit not installed; raypoolkit not installed` today (format in log 01) |
| **T2** | 01 | `tools/compare_stores.py` (comparison by value, schema snapshot) and `Datastore/tests/test_compare_stores.py` | ✅ `d7791c5`: 21 tests (with T3's); bookkeeping list and polymorphic map in log 01 |
| **T3** | 01 | `.documents/dk-and-rpk-migration/rehearsal/`: `build.sh`, `interrupt_when.py`, `stage_counts.py`, `run_readers.sh` | ✅ `d7791c5`: usage checked; never run on a build |
| **L1**–**L4** (R0) | 02 | README §4.1 at 01's tree; §4.3's reproducibility measured; L4 recorded as the baseline | ✅ `6cbf953`: L1–L3 hold; **§4.3 bitwise** (`compare_stores` `RESULT: equal`, exit 0); L4: `compute` 5/5/5, 5/killed, 0/2/5; the prune removed only `resumed-a`'s one unvalidated row (verification §1) |
| **V1** | 02 | `.documents/dk-and-rpk-migration-verification.md` created, with §1 R0; R0's schema snapshot | ✅ `6cbf953`: §1 R0; `out/02-reference-schema.json` (27 client tables) |
| **K1** | 03 | the `raypoolkit` pin in `requirements.txt` | ✅ `2f61943`: one line, between `ray` and `referencing` (`:79`) |
| **K2** | 03 | the venv installed from it | ✅ `2f61943`: dry run and install report only `raypoolkit-0.2.0`; no other distribution moved |
| **K3** | 03 | the proof: one distribution added, at `eaef0ce6…`, the tag's files, not editable | ✅ `2f61943`: freeze differs by one line; `pip check` clean; `direct_url.json` VCS, no `dir_info`; the 4 modules equal `git archive v0.2.0`, no `tests/`; import starts no Ray |
| **W1** | 04 | `RayTools/` deleted | ✅ `303324b`: both files `git rm`'d, and the ignored `__pycache__/` and the empty directory removed; `git ls-tree` empty |
| **W2** | 04 | the 3 imports rewritten to `from raypoolkit import RayWorkPool` | ✅ `303324b`: `main.py:50`, `plot_ScalarModel.py:44`, `plot_by_beta.py:38`; 162 imported names resolve by `ast` and `find_spec`, drivers byte-compile |
| **W3** | 04 | no pool site changed (P7) | ✅ `303324b`: the drivers' diff is one line each; script at the staged tree: 17 calls, `constructs` 17 |
| **W4** | 04 | `.documents/architecture-summary.md`'s `RayWorkPool` lines made true (P14) | ✅ `303324b`: layout, §3.2 title, `pool` type, `store_handler` row and a new `persist_handler` row, the in-flight keying, the lifecycle's store/persist pair (`:195-196`, named by the brief), and `:585` |

| **M1** | 05 | `config/datastore.py`: `factories`, `drop_groups`, `serial_batch_sizes`, moved verbatim | ✅ `8e77578`: 27 entries / 23 classes, 3 groups, 24 sizes with `LambdaCDM`; equal to the capture taken before the first edit |
| **M2** | 05 | the old layer imports them under its old names; `_drop_order` stays | ✅ `8e77578`: the layer's diff is the removed definitions and imports plus two import statements; `_drop_order` kept; the schema agrees with 04's tree and with R0's snapshot |
| **M3** | 05 | the 7 vectorized gets in `main.py` pass `{"shard_key": …}` | ✅ `8e77578`: 7 hunks; `black` splits the `AdiabaticHistory` call, so lines after `:418` move by +2 |
| **M4** | 05 | `Datastore/tests/test_datastore_config.py`: the data captured, the layer reading it, the `ast` test of the drivers; the schema unchanged against R0's snapshot | ✅ `8e77578`: 7 tests (Datastore 66 → 73); four breakages caught (log 05) |
| **C1** | 05a | `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh start\|stop\|status`, adapted from SI's at `3556148`: ChamPBH's temp dir and ports, 10 CPUs, all four ports confirmed free, a start refused on a held port | ✅ `babd9d5`: SI's file with changes 1–4 only (12 hunks, all classified; the lock functions differ by names); mode 100755; `bash -n` under 3.2 and 5.3 |
| **C2** | 05a | Ray 2.53.0's behaviour measured: the client server's command line, `ray_current_cluster`'s place, the actor command, `gcs_server` under `SIGTERM` | ✅ `babd9d5`: all five facts agree with SI's 2.55.1 (log 05a §4.4); `gcs_server` ignores `SIGTERM`, a stop takes 17–20 s |
| **C3** | 05a | the proof on a real cluster beside a decoy (`/tmp/ray-cpbh-decoy`, Ray's default ports), with no build; four breakages | ✅ `babd9d5`: steps 1–9 under `/bin/bash` 3.2, steps 1–3 and stop under 5.3; the decoy's set unchanged pid for pid; B1–B4 caught; the held-port refusal exercised with `nc` |
| **L1**–**L4** (R1) | 06 | README §4.1 at 05's tree; each store equal to `02-reference.db`, bitwise; L4 against R0, the printed lines included | ✅ `c9e6bc2`: L1–L3 hold; **`06-reference.db` and `06-resumed.db` each `RESULT: equal` against `02-reference.db`, exit 0**, and against each other; schema agrees; L4: `compute` 5/5/5, 5/killed, 0/3/5 (lookups 5/3/5); the prune removed only `resumed-a`'s three unvalidated rows; the printed lines in R0's layout (verification §2) |
| **V2** | 06 | `.documents/dk-and-rpk-migration-verification.md` §2 R1 | ✅ `c9e6bc2`: §2 R1 (§§2.1–2.8); `out/06-*` (11 files), `06-reference-schema.json` equal to R0's but for `source` |

| **K4** | 07 | the `datastorekit` pin in `requirements.txt` | ✅ `617a1dc`: one line at `:17`, between `cycler` and `diffrax`; the file is 101 lines |
| **K5** | 07 | the venv installed from it | ✅ `617a1dc`: dry run and install report only `datastorekit-0.3.0`; `raypoolkit` re-resolved and left; nothing else moved |
| **K6** | 07 | the proof: one distribution added, at `2f5417ea…`, the tag's files, not editable | ✅ `617a1dc`: freeze differs by one line; `pip check` clean; `direct_url.json` VCS, no `dir_info`; the 20 modules equal `git archive v0.3.0`, no `tests/`; import starts no Ray |
| **K7** | 07 | U3 and U6 on the installed base: a two-hook subclass instantiates; ChamPBH's 27 registry entries rebuilt on it instantiate; `timestamp` defaults to `None` | ✅ `617a1dc`: script run against the installed package; 27 of 27 instantiate (3 classes define all three hooks, 20 none); `timestamp` `None` and read-only |
| **A1** | 08 | the 7 layer modules deleted; the two `__init__.py` emptied (P6) | ⬜ |
| **A2** | 08 | the layer imports rewritten by the module map; P9's two names from `datastorekit.contract` | ⬜ |
| **A3** | 08 | the factories change nothing but those imports (U3) | ⬜ |
| **A4** | 08 | the three pools take `factories=`, `serial_batch_sizes=` (and `drop_tables=`); `LambdaCDM` out (P8) | ⬜ |
| **A5** | 08 | U4: `tables_to_drop`, refusing before Ray, naming the groups to add | ⬜ |
| **A6** | 08 | the harness on the package's actor, through ChamPBH's copy of the stand-in cluster | ⬜ |
| **A7** | 08 | the tests the package carries deleted (4 + 2), and the 3 layer-identity tests; `_registry()`'s fallback removed | ⬜ |
| **A8** | 08 | new tests: tables declared, pools given the config, U4, the cycle gone, `--inventory` refusing | ⬜ |
| **A9** | 08 | `--inventory` refuses (P12); the refresh route (P13); prose (P14); the issue moves | ⬜ |
| **I1** | 09 | `inventory_spec()` on the 21 owners; `inventory_classes` accepts the registry | ⬜ |
| **I2** | 09 | `tools/inventory_report.py` (P15) | ⬜ |
| **I3** | 09 | `main.py --inventory` read-only, before Ray | ⬜ |
| **I4** | 09 | the six `inventory()` hooks and `inventory_config` deleted | ⬜ |
| **I5** | 09 | coverage: every registered class has a spec, is read through one, or is excluded with a reason (§6.5) | ⬜ |
| **L1**–**L4** (R2) | 10 | README §4.1 at 09's tree; each store equal to `06-reference.db`, bitwise; L4 against R1 | ⬜ |
| **V3** | 10 | `.documents/dk-and-rpk-migration-verification.md` §3 R2 | ⬜ |
| **H1** | 10 | the R2 stores by the package's tools: the shard-key audit; a copy that opens read-only, compares equal and is deleted by `delete_store` | ⬜ |

Items of 11 are added when it is written.

The measurement the plan was written against is [`measure/out/`](measure/out/): both packages'
scripts at `52142d7`, each equal to its checklist's appendix byte for byte; the three roots'
counts (18 / 114 / 35); and the venv's `pip freeze` (README §0.2).

## 2. The gates

| Gate | Holds when | Prompt | State |
|---|---|---|---|
| R0 | the start tree builds the pilot, resumes it, and its readers run (README §4.1); its reproducibility is measured (§4.3) | 02 | ✅ **holds** 2026-10-12 (`6cbf953`): bitwise reproducible, so R1 and R2 are held to bitwise equality with `02-reference.db` (P4) |
| RayPoolKit G3 | `RayTools/` is gone, 0 imports, the pin is installed, the three roots pass, all 17 sites construct (RayPoolKit checklist item 8) | 04 (R1, 06, adds the run under real Ray) | ✅ **holds** 2026-10-12 (`303324b`), by checklist item 8, items 1–5; item 6 is 11's (P16). R1 (06) ran it under real Ray 2.53.0: three builds and three readers, stores equal to R0's, printed lines in R0's layout |
| R1 | 05's tree equals R0 by value | 06 | ✅ **holds** 2026-10-12 (`c9e6bc2`): both R1 stores equal `02-reference.db` bitwise (`RESULT: equal`, exit 0); R2 is held to equality with `06-reference.db` |
| R2 | 09's tree, through both packages, equals R1 by value | 10 | ⬜ |
| DatastoreKit `v0.2.3` | tagged on DatastoreKit's remote, with `store`, `validate` and `validate_on_startup` defaulted (U3; made in DatastoreKit, not here) | — (07 pins it) | ✅ **released** 2026-10-11: tag object `8dac562`, commit `0de2b16`, checked on the remote and by `git show` of `factory_base.py` (README §0.3) |
| DatastoreKit `v0.3.0` | tagged on DatastoreKit's remote; the pin (U6) | — (07 pins it) | ✅ **released** 2026-10-12: tag object `bf0a3b2`, commit `2f5417e`, checked on the remote; only `datastorekit/object.py` changed in the package since `v0.2.3` (README §0.4) |
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

### `[05-config-datastore-cannot-be-imported-before-the-layer]`

**Opened:** 2026-10-12 by prompt 05 (log 05, §"The import-order hazard"), measured at its own tree.
**Not fixed:** the cycle is in the old layer, which 08 deletes; emptying `Datastore/SQL/__init__.py`
is 08's (P6), and `ShardedPool.py:24` imports the actor through it.

- **What.** `Datastore/SQL/__init__.py` imports the actor, so importing any factory module runs
  `Datastore/SQL/Datastore.py`, which imports `config.datastore`, which imports the factories. A
  fresh `import config.datastore` therefore fails with `ImportError: cannot import name
  'serial_batch_sizes' from partially initialized module 'config.datastore'`. Importing
  `Datastore.SQL.Datastore`, `Datastore.SQL.ClientPool` or any factory module first works.
- **Impact.** None today: every driver and actor imports the layer first (`check_imports.py`: 162
  names resolve). One test meets it silently: `Datastore/tests/test_compare_stores.py`'s
  `_registry()` tries `from config.datastore import factories` first, catches the `ImportError`
  and falls back to the layer's `_factories`, which is `config.datastore.factories` itself
  (identity measured), so its 21 tests pass; the fallback would also hide a genuine `ImportError`
  in `config.datastore`. `Datastore/tests/test_datastore_config.py` imports the layer first, with
  a comment.
- **Next step.** 08 removes the cycle by deleting the layer and emptying the two `__init__.py`
  files (P6), and its acceptance includes `python -c "import config.datastore"` from the root.

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

### 5.4 After prompt 03 (2026-10-12)

- **Landed:** `2f61943`, log [`03-pin-raypoolkit-v0.2.0.md`](logs/03-pin-raypoolkit-v0.2.0.md).
  **COMPLETE**: K1–K3; no §4 stop met; no code or test changed; no §3 issue opened.
- **The pin.** `requirements.txt:79` is `raypoolkit @ git+https://github.com/ds283/RayPoolKit@v0.2.0`,
  between `ray==2.53.0` and `referencing==0.37.0`.
- **The venv** (shared, outside git). The dry run and the install each reported only `raypoolkit`
  (0.2.0). `pip freeze` differs from `measure/out/52142d7-pip-freeze.txt` by exactly
  `raypoolkit @ git+https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976`;
  `pip check` is clean. `direct_url.json` is a VCS record at that commit, requested `v0.2.0`, with
  no `dir_info`. The four modules equal `git archive v0.2.0`'s byte for byte and `tests/` is not
  installed. Pip's `Resolved … to commit 4e7eb153…` is the tag object, not the commit.
- **Provenance line:** `>> packages: datastorekit not installed; raypoolkit 0.2.0
  (https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976, requested v0.2.0)`.
- **Suites** (after the install): 18 / 114 (61 + 53) / 66, unchanged. Nothing imports the package.
- **Rollback:** `git revert` of `2f61943` **plus** `./venv/bin/pip uninstall -y raypoolkit`
  (README §4.4); the venv is not in git.

**Orchestrator review of prompt 03 (2026-10-12)**, by orchestrator note
[`prompt-03.md`](orchestrator/prompt-03.md), at `2f61943`. All ten checks pass, and no issue is
opened. The residue is fixed in this follow-up.
- **(1) Scope.** 3 files: `requirements.txt`, the log and the board. The index is untouched, the
  untracked set is unchanged, and no Ray process is running.
- **(2) K1.** One added line at `:79`, between `ray==2.53.0` and `referencing==0.37.0`; the file
  now has 100 lines.
- **(3) The freeze, read from the venv.** It differs from the **committed**
  `measure/out/52142d7-pip-freeze.txt` by exactly the one `eaef0ce6…` line, and from the
  orchestrator's pre-dispatch copy by the same line. `pip check` is clean.
- **(4) The origin.** `direct_url.json`, read in site-packages, is a VCS record: `url`
  `https://github.com/ds283/RayPoolKit`, `commit_id` `eaef0ce6…`, `requested_revision` `v0.2.0`,
  and no `dir_info`. `INSTALLER` is `pip`. No `__editable__`, `.egg-link` or `.pth` names
  RayPoolKit; the only `.pth` is setuptools' `distutils-precedence.pth`.
- **(5) The files.** The tag's archive was extracted afresh and compared with the installed
  package, `__pycache__` aside: the same four `.py` files, each identical under `cmp`, and no
  `tests/`. The dist-info's `RECORD` lists 8 `raypoolkit/` entries: the four modules and their
  `.pyc` files. Every hash it gives matches.
- **(6) Import and helper.** The import one-liner prints `False`. The helper gives version `0.2.0`,
  the URL, commit and revision, `editable: False`, and `datastorekit` `None`. `pip show` gives
  `Requires: ray`.
  - **Correction to the note.** The provenance line is
    `>> packages: datastorekit not installed; raypoolkit 0.2.0 (https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976, requested v0.2.0)`,
    with no `git+`.
  - The note expected `git+https://…`, from the line §5.2's probe printed. That probe's stand-in
    `direct_url.json` carried a `git+` URL. PEP 610 records a VCS URL without the prefix, and pip
    does so here. The helper prints `url` as recorded, which is right; the probe's stand-in was
    not.
  - Nothing is changed. §5.2's line stands as the record of that probe (`CLAUDE.md`
    invariant 6). The log's Observation 1 is accepted as recorded.
- **(7) The pip account.**
  - The dry run reports `Would install raypoolkit-0.2.0` and nothing else. The install reports
    `Successfully installed raypoolkit-0.2.0` and nothing else.
  - The log says that `Resolved … 4e7eb153…` is the tag object.
  - The built wheel went to a temporary directory: pip does not keep a wheel built from a tag in
    its persistent cache (`~/Library/Caches/pip` does not hold it). It is not in the venv.
- **(8) Hygiene.**
  - RayPoolKit's working tree is clean, and its `HEAD` is still `1b84df0`. Its ignored
    `raypoolkit.egg-info/` and `__pycache__/` date from 2026-10-10.
  - The 42 rehearsal-store entries have unchanged mtimes and sizes.
- **(9) Suites.** The agent's: 18, 114 (61 + 53, then whole in 254 s), 66, all OK, at a load of
  about 5–8. The orchestrator re-ran `Datastore/tests` at `2f61943`: 66, OK.
- **(10) Records.** The log follows README §5.1, with every §2 check's command and output. The
  board has the §1 row, K1–K3, §5.4 and the rollback.
  - *Residue, fixed here:* "this commit" → `2f61943` (board ×6, log ×2); this note added to the
    list of orchestrator notes.
- **D1** (ComputeTargets run split and then whole) is accepted.
- **Handed to 04's note.**
  - The provenance line above is what each driver now prints.
  - `raypoolkit` imports cleanly with no Ray started.
  - 04's suites start from 18 / 114 / 66.

### 5.5 After prompt 04 (2026-10-12)

- **Landed:** `303324b`, log [`04-adopt-raypoolkit.md`](logs/04-adopt-raypoolkit.md).
  **COMPLETE**: W1–W4; no §4 stop met; no §3 issue opened.
- **W1.** `RayTools/RayWorkPool.py` and `RayTools/__init__.py` are deleted, and the ignored
  `RayTools/__pycache__/` and the empty directory with them (D1). `git grep -n RayTools --
  ':!prompts' ':!.documents'` is empty.
- **W2–W3.** `main.py:50`, `plot_ScalarModel.py:44` and `plot_by_beta.py:38` now read
  `from raypoolkit import RayWorkPool`; the diff of the drivers is those three lines. No pool site
  changed. The call lines are as at `12c82e8`.
- **The script** (RayPoolKit's `measure_client_pools.py cpbh`, from its root with its own venv,
  `Verdicts by: raypoolkit 0.2.0`): at `12c82e8` (before), 3 imports, 0 string literals, 0
  `import_module`, 17 calls, `constructs` 17, differing from the `52142d7` appendix only in the call
  lines (+8 in `main.py`, +4 in the plot scripts: 01's provenance import and print), the file counts
  and the non-`.py` and test-module counts, each explained in the log. At a dangling commit of the
  staged tree (`2d79710…`, built by `commit-tree` on `12c82e8`, on no branch): `RayTools/` empty, **0 imports, 0
  string literals, 0 `import_module` calls, 17 calls, `constructs` 17**.
- **The imports** resolve (`ast` + `find_spec`, 162 names, 0 failures; `ray.is_initialized()`
  `False`), and `py_compile` of the three drivers succeeds.
- **Breakages**, each caught and recorded as a diff (the fourth as a command): the old import in
  `main.py` (import check fails; script reports 1 import); a misspelt name (`RayWorkPoool`, import
  check fails); `store_handler=lambda obj, pool: None` at `main.py:201` (script: that site is
  `ValueError: No compute handler was supplied, so store_handler would never be called…`,
  `constructs` 16); `raypoolkit` unavailable (`sys.modules["raypoolkit"] = None`: all three drivers'
  imports fail).
- **W4.** `architecture-summary.md` corrected to `raypoolkit` `v0.2.0`'s README ("The constructor"):
  the `store_handler` row, a `persist_handler` row, the `pool` type, the in-flight keying text, the
  lifecycle pair and `:585`. Nothing else in the document changed.
- **Suites:** 18 / 114 (61 + 53) / 66, unchanged, at a load of 4–12. `black --check` clean.
- **RayPoolKit G3 holds** (§2), by checklist item 8, items 1–5. The record's text is in the log's
  "State handed to the next prompt", for 11 to carry (P16).
- **Rollback:** `git revert` of `303324b`; nothing in the venv (README §4.4).

**Orchestrator review of prompt 04 (2026-10-12)**, by orchestrator note
[`prompt-04.md`](orchestrator/prompt-04.md), at `303324b`. All eleven checks pass, and no issue is
opened. The residue is fixed in this follow-up.
- **(1) Scope.** 8 files: the two deletions, the three drivers, `architecture-summary.md`, the log
  and the board. The index is untouched. `ls RayTools` fails, the untracked set is unchanged, no
  branch was left behind, and no Ray process is running.
  - `HEAD` moved under the agent: `64ef9aa` ("Require prompt 09 to prove every class is covered by
    a spec") landed between the note and the agent's commit. It touches only the README and the
    board (`git diff --stat 12c82e8 64ef9aa`: 2 files, 32 insertions), so the agent's measurements
    at `12c82e8`'s code stand. D7 is accepted. The log calls it "the orchestrator's"; it was a
    planning commit from another session, not this note's orchestrator.
- **(2) The diff.** `git ls-tree 303324b RayTools` is empty. The drivers' diff is three one-line
  hunks, the import, in place, at `main.py:50`, `plot_by_beta.py:38` and `plot_ScalarModel.py:44`.
  No line in any `RayWorkPool(` call moved.
- **(3) The script at the real commit**, run by the orchestrator from RayPoolKit's root at
  `--commit 303324b`: 0 imports, 0 string literals, 0 `import_module` calls, 17 calls,
  `constructs` 17. Its call tables equal the orchestrator's pre-dispatch measure at `61df8f8`
  line for line. Only the header, `Files:`, the `RayTools/` and import tables and the non-`.py` count
  (222, the records) differ. The verdicts equal the agent's throwaway measure.
- **(4) Imports.** The log's `check_imports.py`, extracted from the log and re-run from the
  repository root: 162 names, 0 failures, `ray.is_initialized()` `False`. `py_compile` of the three
  drivers succeeds.
- **(5) Breakages**, replayed in `bash` from the log's diffs. Each passes `git apply --check` and
  then `-R --check`.
  - 1 fails the import check on `main.py:50`, "module not found".
  - 2 fails it on `plot_by_beta.py:38`, "no such name".
  - 3 passes the import check. At an orchestrator throwaway commit (`commit-tree`, from a copy of
    the index) the script gives `main.py:201` the expected `ValueError`, with `constructs` 16.
  - 4, the `sys.modules` wrapper, fails all three `raypoolkit` imports (failures 3).

  The tree was clean after each.
- **(6) The venv.** `pip freeze` equals the pre-dispatch copy; `pip check` is clean.
- **(7) W4, line by line.** The diff touches only the layout (`:35-36`, now one line), the §3.2
  title, the table's `pool` and `store_handler` rows plus a new `persist_handler` row, `:180`,
  `:195-196` and `:585`. Each statement agrees with the package at `v0.2.0`:
  - README "The constructor": the `DatastorePool` protocol, the store handler's `obj.store()`
    default with its return ignored, and the persist handler's `pool.object_store(obj)` default
    returning the persisted object;
  - the source, `RayWorkPool.py:462-487`: the store handler, then the persist handler, whose ref
    is tagged `"store"`; in-flight work keyed on `ref.hex()`.

  D2 and D4 are accepted. A layout entry in parentheses is unusual in a tree listing, but it is
  accurate. Leaving out `notify_min_time_interval` was allowed.
- **(8) The log's measurements.**
  - The re-measure's account matches the orchestrator's: +8 / +4 line shifts, and the
    test-module comment. Its `Files:` 510 and non-`.py` count 171 are higher than the
    orchestrator's 509 and 145 at `61df8f8` by this note's own commit.
  - The throwaway `2d79710` has parent `12c82e8` and tree `d818519f…`. `git diff --name-only
    2d79710 303324b` lists only the board, the log and the README, so its code is the commit's.
- **(9) Hygiene.**
  - RayPoolKit's working tree is clean, and its `HEAD` is still `1b84df0`.
  - The 42 rehearsal-store entries have unchanged mtimes and sizes.
  - No scratch file was left in either repository.
- **(10) Suites.** The agent's: 18, 114 (61 + 53), 66, all OK, at a load of 4–12. The orchestrator
  re-ran `Datastore/tests` at `303324b`: 66, OK. `black --check` on the three drivers is clean.
- **(11) Records.** The log follows README §5.1, with each check's command and output and the G3
  text for 11. The board has the §1 row, W1–W4, the RayPoolKit G3 row (holds) and §5.5 with the
  rollback.
  - *Residue, fixed here:*
    - "this commit" → `303324b` (board ×8, log ×5);
    - the status line updated to 4 landed, with RayPoolKit G3 holding;
    - this note added to the list of orchestrator notes.
- **Handed on to 05–06.**
  - The pool is the package. The call sites are at the lines of §0 of the note (`main.py:201` …
    `:828`; the compute sites are `:284`, `:541`, `:828`), and 04 moved none of them.
  - R1 (06) is the first run of `raypoolkit` under real Ray, at Ray 2.53.0, and is held to bitwise
    equality with `02-reference.db`.
  - The suites start from 18 / 114 / 66.

### 5.6 Prompts 05–06 written (2026-10-12)

Written after the reviews of 02 (R0 bitwise, so R1 is held to bitwise equality with
`02-reference.db`) and 04, at `cb0c675`. The facts they rest on were measured read-only that day.
- **05.** Re-read at `cb0c675`:
  - the registry, 27 instances (`Datastore/SQL/Datastore.py:94-122`);
  - the drop groups (`:128-136`) and the drop order (`:138-142`);
  - the serial batch sizes, 24 entries with `LambdaCDM` (`ClientPool.py:24-49`);
  - the 7 vectorized gets, at `main.py:204` … `:747`, each passing `x["shard_key"]`. Every sharded
    table's shard-key field is `"shard_key"` (`config/sharding.py`), so the old layer takes the
    mapping (`ShardedPool.py:592-602`).

  Three measurements shaped the prompt:
  - **An import-order hazard.** It was found on a scratch copy of the tree with the move applied.
    `Datastore/SQL/__init__.py` imports the actor, so importing `config.datastore` *first* fails
    with a partially-initialised-module `ImportError`. Importing the layer first works, as every
    driver and actor does. 05 opens it as an issue for 08 to close (P6 empties the inits) and does
    not work around it.
  - **The old names stay.** `test_compare_stores.py:713` imports `_factories` from the layer, so
    the layer keeps its module-level names as aliases of `config.datastore`'s.
  - **The schema check needs no Ray.** The undecorated actor builds a one-file store. Given a
    one-row `shards` primary, `compare_stores --snapshot` reads it. Built so at 04's tree it
    agrees with R0's `02-reference-schema.json`.
- **06** is 02's protocol at 05's tree. It is held to `RESULT: equal` against `02-reference.db`
  for both of its stores, to `histories.csv` byte for byte (SHA-256 `d7c62c37…c522`), and to R0's
  printed-line layout. It carries R0's run facts:
  - no address is printed by `ray start`, so the address is `127.0.0.1:6379`;
  - `ray stop` runs alone;
  - exit 134 under `SIGTERM`;
  - `S` comes from 06's own `reference` timestamps;
  - the scalar stage's resume lookups are 5 with compute 0 (§3, the `02-…` issue).
- Board items M1–M4, L1–L4 (R1) and V2 are added. 05 records `### 5.7`, 06 `### 5.8`.

### 5.7 After prompt 05 (2026-10-12)

Prompt 05 moved ChamPBH's three registries into `config/datastore.py` and made the seven
vectorized gets pass a mapping, on the old layer. Log:
[`logs/05-move-champbh-data-out-of-the-layer.md`](logs/05-move-champbh-data-out-of-the-layer.md).
- **State.** M1–M4 done (§1.1). The schema agrees with 04's tree and with R0's snapshot. The
  suites are 18 / 114 / 73 (Datastore 66 + 7). One §3 issue opened,
  `[05-config-datastore-cannot-be-imported-before-the-layer]`.
- **Rollback.** `git revert` of `8e77578` alone. It touches nothing in the venv.
- **For 06 (R1).** `main.py` moved by +2 after `:418`, because `black` splits the
  `AdiabaticHistory` vectorized get over three lines. The `RayWorkPool(` sites are now `:201`,
  `:284`, `:363`, `:413`, `:483`, `:543`, `:621`, `:673`, `:746`, `:830`; those from `:483` on
  were `:481`, `:541`, `:619`, `:671`, `:744`, `:828`. The compute handlers are at `:288`, `:547`,
  `:834`; `with ShardedPool(` is `:1348` (was `:1346`). The three compute sites (`:284`, `:543`,
  `:830`) keep their handlers.

**Orchestrator review of prompt 05 (2026-10-12)**, by orchestrator note
[`prompt-05.md`](orchestrator/prompt-05.md), at `8e77578`. All twelve checks pass, and no issue is
opened beyond the prompt's own. The residue is fixed in `e513e8c` (code comment) and in this
follow-up (records).
- **(1) Scope.** 8 files: the two new ones, the layer's two, `main.py`, the log, the board and the
  index. The untracked set is unchanged (23 entries), and no branch was left behind. The agent
  amended its own commit once, to shorten the subject; the reflog shows `4e24a85` → `8e77578` with
  parent `891ae82` both times, so nothing else was folded in.
- **(2) The layer's diff.** It is the removed definitions, the `CosmologyConcepts` import and the
  17 factory-module imports, and two added imports from `config.datastore`. By an `ast` check from
  the scratchpad, no import in either file is unused, none was unused before, and every other
  top-level statement dumps identically to `891ae82`'s. `_drop_order` is at the layer (`:48`).
- **(3) Verbatim.** The three dict literals in `config/datastore.py` dump by `ast` identically to
  `_factories`, `_drop_actions` and `_default_serial_batch_size` at `891ae82`. The module imports
  nothing it does not use. Its docstring names each object, the old layer's aliases until 08, and
  the hazard.
- **(4) `main.py`.** 7 hunks: 7 lines removed, 9 added. With each `object_get_vectorized` call's
  second argument normalised, the `ast` of `main.py` equals `891ae82`'s.
- **(5) The captured data.** The test's three literals equal the orchestrator's capture taken
  before dispatch, entry by entry and in order. The log's `capture.py` reads the layer, not
  `config.datastore`.
- **(6) The tests and the breakages.** `test_datastore_config.py` imports the layer first, with
  the comment. It checks identity against the module, found by `importlib`, and asserts 7 / 0 / 0.
  Its 7 tests pass. The four breakage diffs, extracted from the log and replayed in `bash`, pass
  `git apply --check` and `-R --check`. Each fails only its named test:
  - 1, `main.py:676`, "shard key is not a dict literal";
  - 2, the batch-size data test;
  - 3, the registry data test;
  - 4, only the identity test, with the data tests passing.

  The tree was clean after each.
- **(7) The schema.** The note's build script, run at `8e77578`:
  `ray.is_initialized()` is `False`. Compared against the orchestrator's `0f1c25b` snapshot and
  against `02-reference-schema.json`: `RESULT: the schemas agree` both times.
- **(8) The hazard and the issue.**
  - `import config.datastore` fails with the partially-initialised `ImportError` (on
    `serial_batch_sizes`).
  - `import Datastore.SQL.Datastore, config.datastore` works.
  - `test_compare_stores` run alone passes 21.
  - The §3 entry gives what, impact (naming `_registry()`'s fallback, and that it would hide a real
    `ImportError`) and next step (08, P6).
  - The index has the row under §1.11, 26 open, this board's 3, and the date.
- **(9) The drivers.** Log 04's `check_imports.py`, extracted and re-run: 162 names, 0 failures,
  `ray.is_initialized()` `False`. `py_compile` of `main.py` succeeds. `black --check` is clean on
  the five files.
- **(10) Suites.** The agent's: 18, 114 (61 + 53), 73 (66 + 7), all OK, at a load of 44–108. The
  orchestrator re-ran `Datastore/tests` at `8e77578`: 73, OK.
- **(11) Hygiene.**
  - `pip freeze` equals the pre-dispatch copy, and `pip check` is clean.
  - The 42 rehearsal entries have unchanged mtimes and sizes.
  - RayPoolKit is at `1b84df0`, clean. DatastoreKit moved `30d2fc8` → `24051ae` → `5f972be`, and
    SecondaryGWKit `cf978c6` → `fe16d8e`, both by other sessions' planning and record commits. Both
    trees are clean, and nothing in either is the agent's.
  - No scratch file was left in the repository.
  - **A Ray cluster is up, and it is not this campaign's.** It was started at 09:19 from
    SecondaryGWKit's venv (Python 3.12; its `ray::Datastore` workers are SGK's). The agent started
    none.
- **(12) Records.** The log follows README §5.1, with every command and output and the breakages as
  diffs. The board has the §1 row, M1–M4, the §3 issue and §5.7 with the rollback and the new
  pool-site lines.
  - *Residue, fixed:*
    - both new files' licence headers lacked their last line, `# limitations under the
      License.`; restored in `e513e8c`;
    - "this commit" → `8e77578` (board ×6, log ×4);
    - this note added to the list of orchestrator notes;
    - §5.7's last bullet reflowed.
- **Handed on to 06.**
  - R1 runs at this tree, held to `RESULT: equal` against `02-reference.db`.
  - **README §4.1's cluster check will find SecondaryGWKit's cluster if it is still up**, and
    must stop and ask rather than stop it. 06's note checks it at dispatch.
  - The suites start from 18 / 114 / 73.
  - DatastoreKit's checklist now carries a `v0.3.0` addendum: ChamPBH may pin `v0.2.3` or
    `v0.3.0`. That is the user's choice, for 07, which is written after 06's review.

### 5.8 Prompt 05a written, for U5 (2026-10-12)

Written after 05's review, at `7ed8546`, when the user handed this campaign StochasticInstantons'
`rehearsal-lock-brief.md` (revised at `ed9399a`) and ruled U5 (README §6.6). The planning commit
also amends README §2, §4, §4.1, §5 (rules 12, 15, 16) and §6.6, and prompt 06's gate, §1.2, §1.3.3,
§1.4.2, §4 and §5, which now start and stop the cluster with 05a's script. The facts it rests on
were measured read-only that day.
- **Ray 2.53.0, by its source**, agrees with SI's 2.55.1 measurements: `ray_current_cluster` is
  written into the `--temp-dir` (`ray/_common/utils.py:237`); the client server's command line is
  `… -m ray.util.client.server --address=<gcs> --host=… --port=<client> …`
  (`ray/_private/services.py:2350-2361`), which SI's seed matches; `ray start` takes the four port
  options and `--temp-dir`; `ray stop` takes no address. The behaviour (`gcs_server` and
  `SIGTERM`, the actor command, the client server's address) is 05a's to measure (C2).
- **What 05a changes from SI's script**, besides the names: `--num-cpus` defaults to 10, R0's (SI's
  4 suits SI's grid); the stop confirms **all four** ports free, where SI's checks only the GCS
  port, though the brief's §3 asks for all four and SI's orchestrator checked them by hand; `start`
  refuses a held port before `ray start`. The decoy's temp dir, `/tmp/ray-cpbh-decoy`, has
  ChamPBH's as a prefix, so that the whole-component match is proved too.
- **The machine.** No Ray process running; none of the eight ports held; the lock absent.
  `/tmp/ray`, `/tmp/ray-si` and `/tmp/ray-decoy` exist from earlier runs.
- **Renumbering.** 05a records `### 5.9`; 06's entry becomes `### 5.10` (prompt 06 §3.6 amended).
- Board items C1–C3 added. No issue opened; `.documents/OPEN_ISSUES.md` unchanged.

### 5.9 After prompt 05a (2026-10-12)

Prompt 05a wrote ChamPBH's `ray_cluster.sh` from SI's at `3556148` and proved it on a real cluster
beside a decoy. Log: [`logs/05a-isolate-the-rehearsal-cluster.md`](logs/05a-isolate-the-rehearsal-cluster.md).
- **State.** C1–C3 done (§1.1). No issue opened; `.documents/OPEN_ISSUES.md` unchanged. The suites
  are 18 / 114 / 73, run with no cluster up. No Python, venv or store changed.
- **The actor line, as `start` prints it:**
  `>> ray_cluster: actors: ./venv/bin/ray list actors --address 127.0.0.1:6380`. It reaches
  ChamPBH's cluster alone with the decoy up (`cpbh_actor`, not `decoy_actor`); `--address
  127.0.0.1:6379` the reverse; the bare `ray list actors` fails ("Found multiple active Ray
  instances"). `./venv/bin/ray status --address 127.0.0.1:6380`: one node, 10 CPUs.
- **Where `ray_current_cluster` went.** Into `/tmp/ray-cpbh/` (`127.0.0.1:6380`), and into the
  decoy's `/tmp/ray-cpbh-decoy/`. `/tmp/ray/ray_current_cluster` did not exist before step 2 and
  does not exist at the end. The file survives a stop.
- **The two process sets** (by `list_cluster_pids`, disjoint): ChamPBH's 18 at start (`gcs_server`,
  `monitor.py`, the client server, `dashboard.py` with nine heads, `log_monitor.py`, `raylet` with
  its `agent.py` and runtime-env agent), 24 with five idle workers and one actor worker; the
  decoy's 18, 19 with its actor. A stop left the decoy's 19 pid for pid, and its actor answered.
- **Timings.** A start: 13 s under a load of about 66, 3–6 s at about 25. A stop: 17–20 s (19 s
  for the 24 processes, then the decoy's 18 s), because `gcs_server` ignores `SIGTERM`: the 15 s
  grace runs and the `gcs_server` (and `monitor.py` in two stops) is `SIGKILL`ed.
- **Breakages**, each caught: B1 (whole-component guard) the decoy's processes listed under
  ChamPBH's name; B2 (`mkdir -p`) not refused by the lock, caught by the stale-process guard
  (exit 4), whose release **removed the live cluster's lock**; B3 (owner checks) the stand-in lock
  released; B4 (client-server seed) port 6381 still held, exit 1, lock kept.
- **Rollback.** `git revert` of `babd9d5` alone; it adds the script and changes no code, venv or store.
- **For 06 (R1).** Call `ray_cluster.sh start|stop|status` as §1.2 says. Exit 3: the lock is held,
  stop and ask. Exit 4: a ChamPBH process or a port is already there, stop and ask. The brief's
  contract needed no change for Ray 2.53.0.

**Orchestrator review of prompt 05a (2026-10-12)**, by orchestrator note
[`prompt-05a.md`](orchestrator/prompt-05a.md), at `babd9d5`. All ten checks pass, and no issue is
opened. The residue is fixed in this follow-up (records only).
- **(1) Scope.** 3 files: the script (mode `100755`), the log and the board. No other commit of the
  agent's, and no branch left behind. **The user's staged `phiref_iter.py` was unstaged by someone
  else during the run**: at dispatch it was `A`, and at 12:07 `prompts/jordan-normalization/` was
  untracked again. The agent's transcript has no `git` command that touches this checkout's index
  except its own `git add` of its three files (its only `git add -A` ran in a scratch repository).
  The file is on disk, untracked, and in neither commit.
- **(2) The diff from SI's script.** `diff -u` against SI's at `3556148` is 12 hunks, each change 1,
  2, 3 or 4. `acquire_lock`, `owner_field` and `print_owner` are byte-identical; `write_owner` and
  `release_lock` differ only by `SI_` → `CPBH_` and the project name. The licence header is complete.
  The script never invokes `ray stop`; the phrase appears only in the header comment and the usage.
- **(3) Statically.** `bash -n` passes under 3.2.57 and 5.3.20. `--help` exits 0 and no arguments
  exit 2 under both, with byte-identical output.
- **(4) The re-run**, under `/bin/bash` 3.2, each step a command of its own, at a load of about 21:
  - `status` free.
  - `start "dk-and-rpk-migration 05a (review)"` took 5 s. The owner file has the seven keys in
    order, with the `gcs_server`'s pid.
  - `ray status --address 127.0.0.1:6380` shows one node, 0.0/10.0 CPU.
  - The client server is `… -m ray.util.client.server --address=127.0.0.1:6380 --host=127.0.0.1
    --port=6381 --mode=proxy …`, with parent 1.
  - A second `start` exits 3, and the process set is unchanged.
  - The decoy started. Each cluster's set has 18 pids, and the two are disjoint.
  - A worker on each cluster made a detached actor. `ray list actors --address` lists each actor
    under its own cluster only, and the bare form fails ("Found multiple active Ray instances").
    With workers the sets are 24 and 19, still disjoint.
  - `stop` took 19 s. It sent `SIGTERM` to 24 pids and `SIGKILL` to the `gcs_server` and
    `monitor.py`. None of the 24 is alive, the four ports are free and the lock is released.
  - The decoy's 19 pids are unchanged, and its actor answered.
  - The decoy was stopped by the sourced `stop_cluster` with Ray's four default ports: 19 pids, the
    same `SIGKILL` pair, exit 0.
  - `/tmp/ray/ray_current_cluster` was absent throughout, and `/tmp/ray`'s mtime is unchanged.
    `/tmp/ray-cpbh/ray_current_cluster` holds `127.0.0.1:6380`.
- **(5) The breakages**, extracted from the log and replayed in `bash`. Each passes
  `git apply --check` and `-R --check`, and the script is clean after each:
  - **B1**, with only the decoy up, lists 19 pids where the unbroken script lists 0. 18 of them
    are the decoy's, all but its client server. The 19th had exited by the time it was looked up.
    It is consistent with the listing's own `awk`, whose `-v re=…/tmp/ray-cpbh` argument matches
    once the whole-component guard is gone; the unbroken pattern does not match it.
  - **B3**: with the stand-in lock, the unbroken `stop` exits 1 and keeps the lock. The broken
    `stop` releases it.
  - **B4**, on a fresh start: the broken `stop` signals 17 pids and fails with 6381 held by the
    client server, exit 1, the lock kept. The unbroken `stop` removes that one pid and releases the
    lock.
  - **B2**, on a fresh start: the second `start` exits 4 on the stale-process guard, and the live
    cluster's lock is gone, as the note predicted. The unbroken `stop` says "no lock to release".
- **(6) Change 4.** With `nc` holding 6381, `start` prints the listener, releases the lock and exits
  4. With `nc` on 52367, it again exits 4. Nothing started either time.
- **(7) C2.** The log's five facts agree with check 4: the client server's command line and the
  seed, `ray_current_cluster`'s place, `ray status`, the actor command, and `gcs_server` under
  `SIGTERM`.
- **(8) Suites.** The agent's are 18, 114 (61 + 53) and 73, run with no cluster up. The
  orchestrator re-ran `Datastore/tests` at `babd9d5`: 73, OK.
- **(9) Hygiene.** No Ray process; the eight ports free; no lock; `~/ScienceData` holds only
  `ChamPBH-rehearsals`. `pip freeze` equals the pre-dispatch copy and `pip check` is clean. The 42
  rehearsal entries are unchanged. SI, SGK, DatastoreKit and RayPoolKit are at their dispatch
  `HEAD`s, clean. No scratch file is in the repository. The orchestrator ran no `ray stop`.
- **(10) Records.** The log follows README §5.1, with every command and its output, and the
  breakages as diffs. The board has the §1 row, C1–C3 and this §5.9.
  - *Residue, fixed here:*
    - "this commit" → `babd9d5` (board ×5, log ×2);
    - the rollback line made precise;
    - this note added to the list of orchestrator notes.
- **Handed on.**
  - 06 (R1) can be dispatched. Its note checks the lock, the ports and the load at dispatch.
  - Changes 3 and 4 are ChamPBH's additions to SI's script: the four-port check and the
    held-port refusal. Whether SI takes them up is SI's business (README §6.6); nothing is written
    in SI.

**SI's handover (2026-10-12)**, `rehearsal-lock-handover-2026-10-12.md` at SI's `5fb4a1d`, weighed in
README §6.6.
- SI took changes 3 and 4 into its script (`8e04a7c`). It added a five-argument guard on
  `stop_cluster`. ChamPBH took the guard, and corrected its usage text on the client server, as
  housekeeping in `288b385`, checked under both shells without starting a cluster.
- The brief, amended at SI's `d6d550b`, forbids `--ray-address auto` for any run under the lock.
  06's protocol already gives every driver and `ray` CLI call the address. README §7 now says the
  same of a long build on this machine.
- SI's drivers now require `--ray-address`; ChamPBH's keep `auto`. That is left for the user
  (README §6.4).
- No issue opened; `.documents/OPEN_ISSUES.md` unchanged.

### 5.9a U6 ruled: DatastoreKit `v0.3.0` is the pin (2026-10-12)

The user ruled **U6** after 05's review raised the choice (README §6.7): the campaign pins
DatastoreKit **`v0.3.0`**, not `v0.2.3`. Numbered 5.9a so that 06 keeps §5.10, as its prompt says.
- **Checked read-only that day** (README §0.4):
  - `git ls-remote`: `refs/tags/v0.3.0` → `bf0a3b25…`, `^{}` → `2f5417ea76cdcd0d15d64d80d9085429bfacad21`;
    `pyproject.toml` at the tag says `0.3.0`;
  - between `v0.2.3` and `v0.3.0` the package changes only `datastorekit/object.py` (an optional,
    keyword-only `timestamp`, kept as `_timestamp` and read by a read-only property), plus tests
    and the version. `datastorekit/SQL/` is unchanged, so U3 holds as at `v0.2.3`;
  - ChamPBH's 19 `DatastoreObject.__init__(…)` calls pass the store id alone, and no ChamPBH class
    defines or assigns `timestamp` or `_timestamp` (`git grep` at `7ed8546`).
- **Amended:** README §0, §0.4 (new), §2's table, §2.1, 07's charter (pin, figures, and an added
  acceptance item for the `timestamp` default), 08's citation of the base, §4 and §6.7 (new); this
  board's status line, decisions, §1's 07 row and the gates. No written prompt changes: 06 runs on
  ChamPBH's own layer and cites no release.
- No issue opened; `.documents/OPEN_ISSUES.md` unchanged.

### 5.10 After prompt 06 (2026-10-12)

- **Landed:** `c9e6bc2`, log [`06-rehearse-before-the-datastore-swap.md`](logs/06-rehearse-before-the-datastore-swap.md);
  the record is `.documents/dk-and-rpk-migration-verification.md` §2. **COMPLETE**: L1–L4 (R1),
  V2; gate R1 holds; no §4 stop met; no code changed; no issue opened (`.documents/OPEN_ISSUES.md`
  unchanged).
- **The criterion.** `compare_stores` of `02-reference.db` against `06-reference.db` and against
  `06-resumed.db`: `RESULT: equal`, exit 0, each; `06-reference.db` against `06-resumed.db` the same.
  27 tables, every row matched, bookkeeping differing on every matched row. The schema snapshot
  agrees with R0's. **03–05 changed nothing a build stores.**
- **Cluster and load.** `ray_cluster.sh start "dk-and-rpk-migration 06 (R1)"` at 16:43:19 (22 s;
  `127.0.0.1:6380`, gcs_server 80158; `ray status --address`: one node, 10 CPUs); `stop` at 17:09:48
  (19 s, exit 0, ports free, lock released), then `status` clean. Never `ray stop`. Every driver
  connected to 6380. The load was above 10 throughout, peaking near 160 at `reference`'s end; two
  foreign DatastoreKit suites ran at the gate and ended before `start`.
- **Builds.** `reference` 11 min 7 s, exit 0 (R0 10 min 43 s). `resumed-a` killed 6 s after the
  adiabatic banner, exit 134; **it had stored all five `AdiabaticHistory` rows and validated two**
  (β = 1.1, 1.15). `resumed-b` 2 min 42 s, exit 0: its open pruned exactly the three unvalidated
  rows (β = 1.2, 10, 25) and it computed 0 / 3 / 5, lookups 5 / 3 / 5. Label counts:
  `AdiabaticHistory` 2 `resumed-a` + 3 `resumed-b`.
- **Actors.** None alive 14 s after the signal; all 34 (17 per job) dead at 28 s; 102 dead, six jobs
  `SUCCEEDED`, before the stop.
- **Readers.** All six steps exit 0; before/after `RESULT: equal` with no bookkeeping difference;
  `histories.csv` SHA-256 `d7c62c37…c522`, `cmp`-equal to R0's; the β = 1.15 stored and re-solved
  Yp and D/H are R0's.
- **L4.** `reference` 5 / 5 / 5; the resume's pattern is R0's. The pool's printed lines keep R0's
  layout: 10 banner and completion lines equal in text and order, and 06's progress pair (scalar
  stage, 5m 5.89s) in R0's two shapes.
- **Suites** (before `start`): 18 / 114 (61 + 53) / 73, unchanged.
- **Observations not opened:**
  1. `resumed-a` stored all five rows (the "5" case by stored rows; *k* = 2 by validated rows), so
     no history was absent from the store at the resume; R2's `S` should allow for `resumed-a`
     running 3 s or more ahead of `reference`;
  2. Ray's log deduplication hid the β = 10 prune report from the driver's log; all three were
     read from Ray's worker logs;
  3. the killed driver's stack was in `submit_actor_task`, not `Wait()`;
  4. the load, and the foreign suites at the gate;
  5. the plot scripts' shard-count warning, unchanged.

**Orchestrator review of prompt 06 (2026-10-12)**, by orchestrator note
[`prompt-06.md`](orchestrator/prompt-06.md), at `c9e6bc2`. All eleven checks pass, and no issue is
opened. The residue is fixed in this follow-up (records only).
- **(1) Scope.** 14 files: the verification document, eleven `out/06-*` files, the log and the
  board. The verification document's diff only adds lines after §1. The untracked set is
  unchanged; nothing staged; no other commit of the agent's, no leftover branch. The agent's
  three stray error files from its zsh loop (log D4) are gone from `out/`.
- **(2) The cluster.** `ray_cluster.sh status`: lock free, no process, the four ports free; no Ray
  process on the machine. The log has `start`'s output and owner file (purpose
  `dk-and-rpk-migration 06 (R1)`), `ray status --address` with 10 CPUs, `stop` exit 0 in 19 s, and
  `status` after it. `ray stop` appears only as "never run".
- **(3) The logs, read.** Each build log and each of the three Ray readers' logs has L1's
  provenance line once (`raypoolkit 0.2.0 (…@eaef0ce6…, requested v0.2.0)`, `datastorekit not
  installed`). Every driver connected to `127.0.0.1:6380`, dashboard 8267; no log names 6379.
  No `Traceback` or `Error` in any build or reader log. `reference` and `resumed-b` end `** driver
  exit status 0`, every stage complete; `resumed-a` ends 134 after the adiabatic banner, its absl
  dump under pid 86620, the one in `06-resumed-a.pid`.
- **(4) The counts.** `stage_counts.py` re-run on the three logs gives the committed text and
  equal JSON. The orchestrator's own `sqlite3` `mode=ro` script gives the label counts:
  `06-reference` 5 / 5 / 5, all `reference`; `06-resumed` `ScalarModel` 5 `resumed-a`,
  `AdiabaticHistory` 2 `resumed-a` + 3 `resumed-b`, `BBNData` 5 `resumed-b`; every row validated.
  So *k* = 2 validated rows kept, agreeing with `resumed-b`'s adiabatic compute of 3.
- **(5) The prune.** Two reports reach the driver's log, β = 1.2 in full and β = 25 marked
  `[repeated 2x across cluster]`. All three were read in full from Ray's worker logs (under
  `/tmp/ray-cpbh/session_2026-10-12_16-43-24_306437_80134/logs/old/`, not `logs/` as log 06 says):
  `shard0001` β = 1.2 (5002 values), `shard0002` β = 10 (9776), `shard0003` β = 25 (17917), each
  `resumed-a`'s, each unvalidated, each pruned. Nothing validated was pruned.
- **(6) The criterion, re-run.** The three comparisons re-run into the scratchpad: each
  `RESULT: equal`, exit 0, text identical to the committed file but for the lines naming the
  stores, JSON equal. 27 tables with R0's counts; the bookkeeping differences are only
  `timestamp`, `label` and the timings. **Independently of the tool**, the orchestrator's script
  resolved each history to its β and hashed, by stored bits: its `ScalarModel` science columns
  (with the solver, temperatures, tolerances and initial φ, π resolved to the rows they name), its
  `ScalarModelValue` rows ordered by `z`, its `AdiabaticHistory` and `AdiabaticHistoryValue` rows,
  and its `BBNData` row and `BBNDataValue` rows. Run first on R0's own pair as a control, then on
  both 06 stores: **all 35 digests of each 06 store equal `02-reference.db`'s.** The script's
  first version hashed the raw `coupling_serial` and `solver_serial` and so failed R0's control;
  it was corrected to resolve them, as `compare_stores` does. On a scratch copy of `06-reference`
  with one `AdiabaticHistoryValue` float moved one ulp, it reports β = 10 differing. **R1 holds,
  confirmed.**
- **(7) The readers.** `status.txt` steps 0–5, each exit 0. The before/after comparison is
  `RESULT: equal` with no bookkeeping difference. `histories.csv` SHA-256 `d7c62c37…c522`, `cmp`
  silent against R0's. β = 1.15 stored Yp 0.24687349447579363, D/H 2.4859924812264325; `prod`
  re-solve 0.2468734945, 2.485992481: R0's. The inventory lists 5 validated rows of each kind,
  version `2026.6.0`, none unvalidated.
- **(8) The schema.** `--snapshot` of `06-reference.db` is byte-identical to
  `out/06-reference-schema.json`, which differs from `02-reference-schema.json` only in `source`.
- **(9) L4.** The orchestrator's own extraction: `02-reference.log` against `06-reference.log`,
  the banner, completion and summary lines equal in text and order, and both of 06's progress
  shapes (the scalar stage's pair, at 5m 5.89s) occur in R0's; `02-resumed-b.log` against
  `06-resumed-b.log`, equal, the no-compute line included. Not vacuous.
- **(10) Hygiene.** Every one of the 189 entries under the rehearsal root at dispatch has its
  mtime and size, but the root and `logs/` directories, which gained the 06 entries. The root now
  holds also the two 06 stores (17 files each), `logs/06-*` (three logs, three pidfiles) and
  `06-readers/`. The 53 stores and pilot logs unchanged. `pip freeze` equals the dispatch copy;
  `pip check` clean. SI, SGK and RayPoolKit at their dispatch `HEAD`s; DatastoreKit moved
  `5f972be` → `04a2ed9` by another session's planning commits (16:38, 16:48), nothing the
  agent's; all clean. `/tmp/ray/ray_current_cluster` still absent. No scratch file in the
  repository.
- **(11) Records and suites.** Verification §2 R1 (§§2.1–2.8) in §1's shape, with the commands
  verbatim. The board has the §1 row, L1–L4 (R1), V2, gate R1 and §5.10. The agent's suites 18 /
  114 (61 + 53) / 73, before `start`; the orchestrator re-ran `Datastore/tests` at `c9e6bc2`: 73,
  OK.
  - *Residue, fixed here:* "this commit" → `c9e6bc2` (board ×5, log ×2); this note added to the
    list of orchestrator notes.
- **Deviations D1–D4** are accepted. D1 (`S` = 6 s) is the cause of Observation 1. D4 opened no
  store and its files were removed.
- **Observation 1, weighed.** The note asked for "0 or 5 stored" to be taken to the user. Here
  `resumed-a` stored five and validated two, so the resume still tested the prune at open (three
  rows) and the reuse of stored validated rows (two). What it did not test, and R0 did (β = 25), is
  a history absent from the store. Reported to the user; no repeat is proposed, since R0 covered
  that case on the same layer and R1's stores equal R0's.
- **Handed on.**
  - R2 (10) is held to `RESULT: equal` with `02-reference.db` (and so `06-reference.db`).
  - 10's `S` must allow for `resumed-a` running 2–5 s ahead of its `reference` (Observation 1).
  - Prune reports may be deduplicated by Ray in the driver's log; 10 reads Ray's worker logs
    (`session_*/logs/old/` after the job ends) before its `stop`, or relies on the label counts.
  - 07–10 can be written (README §2.1), with `v0.3.0` the pin (U6).

### 5.11 Prompts 07–10 written (2026-10-12)

Written after 06's review (`fa874a5`), from a read-only measurement that day: ChamPBH at `fa874a5`,
DatastoreKit at the tag `v0.3.0` (`git show`; its working tree is ahead), SecondaryGWKit at
`a5a6f6d`. Nothing was changed in any repository and no store was opened but the R1 readers' text
log of `--inventory`.
- **07** is 03 for DatastoreKit: the pin between `cycler` (`requirements.txt:16`) and `diffrax`
  (`:17`); the tag `bf0a3b2` / `2f5417e`; 20 non-test files installed, 54 under `tests/` not. K7 adds
  to the charter's U3 check a stronger one: each of ChamPBH's 27 registry entries, rebuilt on the
  installed base from its class's own namespace, instantiates.
- **08.** DatastoreKit's script at `fa874a5` counts 43 layer imports in 39 files (42 in 38 outside
  `prompts/`), one nested and one `import_module` call. The measurement found four things the charter
  did not cover, and README §2.2 is amended for them:
  - `Datastore/tests/test_datastore_config.py` (05) imports the layer at module level, its
    `TestLayerReadsConfig` (3 tests) asserts the layer's aliases, and its batch-size literal holds
    `LambdaCDM`. The three tests are deleted and replaced by an `ast` test that each pool is given
    `config.datastore`'s objects, so the Datastore count is 73 − 4 − 2 − 3 + 08's tests.
  - `test_compare_stores.py`'s `_registry()` falls back to the layer; the fallback goes.
  - **The package's actor needs a serial broker**, and its tests supply one from
    `datastorekit/tests/standin_pool.py`, which the wheel does not ship. ChamPBH keeps its own copy
    (adoption README §3), changed only to open ChamPBH's registry; the harness, 08's schema check and
    09's test stores are built through it, with no Ray.
  - Test a reads `_version.store_id`, which the package's actor does not have: the one test body
    that changes.

  U4's refusal is `config.datastore.tables_to_drop`, after SGK's, over `dependent_tables`, before
  `ray.init`. 08 closes `[05-config-datastore-cannot-be-imported-before-the-layer]`, narrows
  `[00-two-files-are-not-black-clean]` and opens `[08-plot-scripts-open-read-write-pools]` (P11). It
  leaves `tools/compare_stores.py` unchanged (README §4), though its comments cite the deleted
  layer's lines: those are 11's.
- **09.** All 21 owners' specs follow from their lookups, with polymorphic parents for
  `ScalarModel`'s three type/serial pairs (DatastoreKit's `Parent(type_column=, types=)`; the maps are
  `tools/compare_stores.py`'s). README §6.5's count holds: 21 owners and 6 tag and value tables are
  all 27 entries. Two points of P15 cannot be met as written, and the charter is amended:
  - the package's inventory has one timestamp range per class, and records carry none, so the report
    gives the class's range, not one per validated group;
  - `failure` (`ScalarModel`) and `PRyM_version` (`BBNData`) are declared as leaves beyond the
    lookups, so that the report can show them; the stored `label` is not, since it carries the job
    name and time. Today's report never printed `PRyM_version`; the new one does.

  Today's `--inventory` was not read-only (it opened a read-write pool after `ray.init`, pruning by
  default and creating an absent store); the new one reads a closed store before Ray. The charter's
  `units` argument is dropped: each stored `value_<unit>` is already in its default unit.
- **10** is 06 at 09's tree, held to `RESULT: equal` against `06-reference.db` for both stores and
  against `02-reference.db` for `reference`. It carries R1's facts: exit 134; `S` aimed about 3 s
  earlier than `reference`'s first row, since R1's 6 s let `resumed-a` store all five; the prune
  reports read from the actors' logs before the stop. H1 runs the package's store tools: the
  shard-key audit, a `sharded_store copy` that opens read-only and compares equal, and its deletion
  by `delete_store`. The board records DatastoreKit's G3 when 10 holds.
- Board items K4–K7, A1–A9, I1–I5, L1–L4 (R2), V3 and H1 are added, and gate R2. 07 records
  `### 5.12`, 08 `### 5.13`, 09 `### 5.14`, 10 `### 5.15`. No issue opened;
  `.documents/OPEN_ISSUES.md` unchanged.

### 5.12 After prompt 07 (2026-10-12)

- **Landed:** `617a1dc`, log [`07-pin-datastorekit-v0.3.0.md`](logs/07-pin-datastorekit-v0.3.0.md).
  **COMPLETE**: K4–K7; no §4 stop met; no code or test changed; no §3 issue opened.
- **The pin.** `requirements.txt:17` is `datastorekit @ git+https://github.com/ds283/DatastoreKit@v0.3.0`,
  between `cycler==0.12.1` and `diffrax==0.7.0`; the file has 101 lines.
- **The venv** (shared, outside git). The dry run and the install each reported only
  `datastorekit` (0.3.0); `raypoolkit` was re-resolved and left (its files keep mtime
  `1791766684`). `pip freeze` differs from the saved one by exactly
  `datastorekit @ git+https://github.com/ds283/DatastoreKit@2f5417ea76cdcd0d15d64d80d9085429bfacad21`;
  `pip check` is clean. `direct_url.json` is a VCS record at that commit, requested `v0.3.0`, with
  no `dir_info`. The 20 non-test modules equal `git archive v0.3.0`'s byte for byte and `tests/` is
  not installed. Pip's `Resolved … to commit bf0a3b25…` is the tag object, not the commit.
- **Provenance line:** `>> packages: datastorekit 0.3.0
  (https://github.com/ds283/DatastoreKit@2f5417ea76cdcd0d15d64d80d9085429bfacad21, requested
  v0.3.0); raypoolkit 0.2.0
  (https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976, requested v0.2.0)`.
- **K7, on the installed package** (script in the log): a `register`+`build` subclass instantiates
  and a `register`-only one raises `TypeError` naming `build`; all 27 registry entries (23 classes),
  rebuilt on the installed base, instantiate (3 classes define `store`, `validate` and
  `validate_on_startup`; 20 define none); `DatastoreObject.timestamp` is `None` and read-only.
- **Suites** (after the install): 18 / 114 (61 + 53) / 73, unchanged. Nothing imports the package.
- **Rollback:** `git revert` of `617a1dc` **plus** `./venv/bin/pip uninstall -y datastorekit`
  (README §4.4); the venv is not in git.

**Orchestrator review of prompt 07 (2026-10-12)**, by orchestrator note
[`prompt-07.md`](orchestrator/prompt-07.md), at `617a1dc`. All eleven checks pass, and no issue is
opened. The residue is fixed in this follow-up.
- **(1) Scope.** 3 files: `requirements.txt`, the log and the board. The index is untouched, the
  untracked set is unchanged, and `617a1dc` is the only commit since the note.
- **(2) K4.** One added line at `:17`, between `cycler==0.12.1` and `diffrax==0.7.0`; the file
  now has 101 lines.
- **(3) The freeze, read from the venv.** It differs from the **committed**
  `measure/out/52142d7-pip-freeze.txt` by exactly two added lines, `datastorekit` at `2f5417ea…`
  (`:17`) and 03's `raypoolkit` at `eaef0ce6…`, and from the orchestrator's pre-dispatch copy by
  the `datastorekit` line alone. `pip check` is clean. RayPoolKit's dist-info and four modules keep
  mtime `1791766684`: it was re-resolved, not reinstalled.
- **(4) The origin.** `direct_url.json`, read in site-packages, is a VCS record: `url`
  `https://github.com/ds283/DatastoreKit`, `commit_id` `2f5417ea…`, `requested_revision` `v0.3.0`,
  and no `dir_info`. `INSTALLER` is `pip`. site-packages differs from the pre-dispatch listing by
  `datastorekit/` and `datastorekit-0.3.0.dist-info/` only; the only `.pth` is still setuptools'.
- **(5) The files.** The tag's archive was extracted afresh and compared with the installed
  package, `__pycache__` aside: the same 20 `.py` paths, each identical under `cmp`, no `tests/`
  and no other file. The `RECORD` lists 40 `datastorekit/` entries, the 20 modules and their
  `.pyc` files; every hash it gives matches.
- **(6) Import and helper.** The import one-liner prints `False`; `sharded_store --help` exits 0.
  The helper gives `datastorekit` `0.3.0`, the URL, commit and revision, `editable: False`, and
  `raypoolkit` unchanged. The line is the note's expected one:
  `>> packages: datastorekit 0.3.0 (https://github.com/ds283/DatastoreKit@2f5417ea76cdcd0d15d64d80d9085429bfacad21, requested v0.3.0); raypoolkit 0.2.0 (https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976, requested v0.2.0)`.
  `pip show datastorekit` gives `Requires: ray, sqlalchemy`.
- **(7) The pip account.** The dry run reports `Would install datastorekit-0.3.0` and the install
  `Successfully installed datastorekit-0.3.0`, nothing else. The log says that both `Resolved …`
  lines name tag objects and that RayPoolKit's re-clone is a re-resolution.
- **(8) K7.** The agent's script imports the package from site-packages (its first line of output
  says so), imports the layer before `config.datastore`, builds each class from its own `vars()`
  less `__dict__` and `__weakref__` on the installed base, and passes `ObjectType` to the six
  quantity entries. Its output: 27 of 27 instantiate; 23 classes, 3 defining all three hooks
  (`sqla_AdiabaticHistoryFactory`, `sqla_BBNDataFactory`, `sqla_ScalarModelFactory`), 20 none, none
  some; the `register`-only subclass's `TypeError` names `build`; `timestamp` `None` and
  unassignable. **The orchestrator's replay**, its own pre-dispatch probe with the archive taken
  off `sys.path`, imported the installed package and gave the same figures and messages. **U3 and
  U6 hold on the installed release.**
- **(9) Hygiene.** DatastoreKit `04a2ed9`, RayPoolKit `1b84df0`, SecondaryGWKit `a5a6f6d`,
  StochasticInstantons `5fb4a1d`, each at its dispatch `HEAD` with a clean working tree. The 104
  rehearsal-root entries (depth 2) have unchanged mtimes and sizes. No Ray process. No scratch
  file in the repository.
- **(10) Suites.** The agent's: 18, 114 (61 + 53), 73, all OK, at loads of 4–22. The orchestrator
  re-ran `Datastore/tests` at `617a1dc`: 73, OK.
- **(11) Records.** The log follows README §5.1, every §2 check with its command and output, and
  K7's script whole. The board has the §1 row, K4–K7, §5.12 and the rollback.
  - *Residue, fixed here:* "this commit" → `617a1dc` (board ×7, log ×2); this note added to the
    list of orchestrator notes.
- **Deviations D1–D3** are accepted. D3 (quantity entries found by class name) picks out exactly
  the six entries whose factories keep `ObjectType`, as the output's table shows.
- **Handed on to 08's note.**
  - The provenance line above is what each driver now prints.
  - `datastorekit` imports cleanly with no Ray started; ChamPBH's 27 registry instances are
    accepted by the installed base as they stand (U3).
  - `requirements.txt`'s lines after `:16` moved down by one: `ray` `:79`, `raypoolkit` `:80`,
    `SQLAlchemy` `:91`.
  - 08's suites start from 18 / 114 / 73.

### 5.12a Prompt 08 amended at dispatch (2026-10-12)

Before dispatching 08, the orchestrator prototyped the swap on a scratch `git archive` of `c4bd666`
(note [`prompt-08.md`](orchestrator/prompt-08.md) §0). The prompt's harness design held: the five
harness importers pass on the package's actor with no change to their bodies, and the stand-in
store's schema agrees with R0's snapshot outright. One test the prompt did not name failed:
`Datastore/tests/test_compare_stores.py`'s test 14 builds its primary from
`config.sharding.replicated_tables` and asserts (`:1111`) that `LambdaCDM` is noted, which P8 makes
false. As written that is a §4 stop; it is the direct consequence of P8, which the user ruled, so
08's A7.4 and §4, and README §2.2's charter, now name it: the assertion is inverted and the test's
docstring says so. The Datastore count is unchanged by it (64 + the A8 tests).

