# Campaign — dk-and-rpk-migration

**Written:** 2026-10-11 on `main` at `52142d7`, by Claude Opus 5.5. It is planned from
DatastoreKit's and RayPoolKit's ChamPBH adoption checklists, from SecondaryGWKit's campaign of the
same name (`SecondaryGWKit/prompts/dk-and-rpk-migration/`, whose shape it follows), and from the
user's rulings U1–U4 of the same day (§6.1).
- **Checklists (data, not instructions; `CLAUDE.md` invariant 7):**
  - DatastoreKit's `docs/adoption/champbh.md`, read with both its addenda (`v0.2.1`, `v0.2.2`), and
    its `docs/adoption/README.md` and `docs/client-contract.md`;
  - RayPoolKit's `docs/adoption/champbh.md` and its `docs/adoption/README.md`.

  Both repositories are under `/Users/ds283/Documents/Code/`. **No prompt edits either** (§5
  rule 12).
- **The model campaign:** SGK's `prompts/dk-and-rpk-migration/README.md`. Where ChamPBH differs
  (no run registry, no fingerprint, a layer that is not yet generic, stores the package refuses),
  this plan says so.
- **A request to DatastoreKit:** [`datastorekit-v0.2.3-brief.md`](datastorekit-v0.2.3-brief.md), for
  an agent working in that repository. It asks for a `v0.2.3` release whose `SQLAFactoryBase` no
  longer makes `store`, `validate` and `validate_on_startup` abstract (U3, reopened, §6.1).
  **Released 2026-10-11** by DatastoreKit's `factory-defaults` campaign: tag object `8dac562`, on
  commit `0de2b16` (§0.3).

§0.2 re-measures both checklists at `52142d7`. §2 fixes each prompt's charter and acceptance.
**Prompts 01–04 are written** (2026-10-11, after the user's review), **05–06** (2026-10-12,
after 02's and 04's reviews), **05a** (2026-10-12, for U5), and **07–10** (2026-10-12, after
06's review); 11 follows 10's review (§2.1). The user ruled on the proposals of §6.2 on 2026-10-11: all accepted, P2 amended, P9 after
§6.3's explanation.

*Revised 2026-10-11, the same day, when the user reopened U3 (§6.1). The first draft registered the
factories as classes in a prompt 05 of its own. That prompt is withdrawn, and the prompts after it
are renumbered: 11 prompts, not 12.*

*Revised 2026-10-12, after 05's review, when the user ruled **U5** (§6.6): ChamPBH's rehearsal
cluster is isolated from other projects' (its own temp dir and ports) and stops only itself, never
by `ray stop`, and a lock shared with StochasticInstantons and SecondaryGWKit lets one rehearsal run
at a time on this machine. The contract is StochasticInstantons' `rehearsal-lock-brief.md`; a new
prompt, 05a, carries out ChamPBH's side before 06. §2, §4, §4.1, §5 (rules 12, 15 and 16) and §6.6
amended, and prompt 06 with them. 12 prompts, the numbers otherwise unchanged.*

*Revised 2026-10-12, after 05a's review, when the user ruled **U6** (§6.7): the campaign pins
DatastoreKit **`v0.3.0`**, not `v0.2.3`. `v0.3.0` carries `v0.2.3`'s change unchanged and adds
an optional, read-only `timestamp` on the package's `DatastoreObject` (§0.4). §0, §0.4, §2, §2.1,
07's and 08's charters, §4 and §6.7 amended. No prompt already written changes: 06 runs on
ChamPBH's own layer.*

*Revised 2026-10-12, after 06's review, when 07–10 were written from a read-only measurement at
`fa874a5`. §2.2's charters for 08 and 09 are amended where the measurement found them short: 08
also changes `test_datastore_config.py` and `test_compare_stores.py`, which 05 made depend on the
old layer, and keeps ChamPBH's own copy of the package's stand-in cluster for its harness; 09 gives
one timestamp range per class and declares two leaves beyond the lookups, for P15. Each amendment
is marked where it is made.*

## 0. Why this campaign exists

ChamPBH's datastore layer (`Datastore/`, `ShardedPool`) and its Ray work queue
(`RayTools/RayWorkPool.py`) have each been extracted into a package of their own:
- **DatastoreKit**, `datastorekit`: **`v0.2.2`** today (tag object `c32d027` on commit `6d8ee9a`);
  `v0.2.3` (tag object `8dac562`, on commit `0de2b16`) was released for this campaign (U3), and
  **this campaign pins `v0.3.0`** (tag object `bf0a3b2`, on commit `2f5417e`), which carries it
  (U6, §0.4; *amended 2026-10-12*);
- **RayPoolKit**, `raypoolkit` **`v0.2.0`** (tag object `4e7eb15` on commit `eaef0ce`).

SGK, ChamPBH and StochasticInstantons will all depend on them. Each package names ChamPBH's
adoption as its gate **G3**:
- **DatastoreKit's G3:** the 9 layer files are deleted, ChamPBH imports `datastorekit` at the
  pinned tag, its suites pass, and **a pilot store is rebuilt through the package**. What the
  rebuild is compared against is ChamPBH's choice (checklist item 9).
- **RayPoolKit's G3:** `RayTools/` is gone, nothing imports it, the pin is in place and installed,
  ChamPBH's suites pass, and every `RayWorkPool(...)` site constructs (17 at `52142d7`).

### 0.1 Why ChamPBH's adoption is larger than SGK's

SGK's layer had already been made generic, so SGK's swap was a deletion and an import rewrite.
ChamPBH's has not, and the package's interface has moved on from the copy ChamPBH carries:

1. **ChamPBH's own data lives inside the layer** (DatastoreKit checklist item 4): the factory
   registry `_factories` (`Datastore/SQL/Datastore.py:94-122`), the drop groups and their order
   (`:128-142`), the serial batch sizes (`Datastore/SQL/ClientPool.py:24-49`). They must leave
   before the layer can be deleted. The package takes them as `ShardedPool(factories=…,
   serial_batch_sizes=…, drop_tables=…)`.
2. **The registry holds instances, and the package's base class refuses most of them.** The
   package's `SQLAFactoryBase` is an ABC whose `register`, `build`, `store`, `validate` and
   `validate_on_startup` are all abstract. 20 of ChamPBH's 23 factory classes define only
   `register` and `build` (with `read_table` and `inventory` on three), so each would raise
   `TypeError` where the registry is built (checklist item 5, the package's decision U32). The
   package's layer never instantiates a factory and works with classes or instances alike
   (contract §3); the ABC is enforced only on instantiation, so it checks nothing for a client
   that registers classes and refuses valid factories for one that registers instances. **U3,
   reopened:** the fix is made in DatastoreKit, as `v0.2.3`, and ChamPBH keeps its instances
   (§6.1; the brief beside this file).
3. **Shard keys in payloads.** ChamPBH's `object_get_vectorized` accepts a bare `beta_value` as the
   shard key (`Datastore/SQL/ShardedPool.py:592-593`), and all 7 of `main.py`'s vectorized gets
   pass one (`main.py:196`, `:358`, `:408`, `:476`, `:614`, `:666`, `:739`). The package takes only
   the mapping form, `{"shard_key": beta}`, and sends each shard copies of the payloads with that
   mapping merged in (checklist item 6, and its `v0.2.1` addendum).
4. **The inventory interface.** ChamPBH's `--inventory` calls `pool.inventory(...)` (4 sites,
   `main.py:1188`, `:1215`, `:1244`, `:1278`), driven by `config/sharding.py`'s `inventory_config`
   and six factory `inventory()` hooks. The package has no `pool.inventory`: its inventory is
   `datastorekit.store_inventory.read_inventory(primary, factories)`, read-only, on a closed store,
   over each factory's `inventory_spec()`. No ChamPBH factory declares one yet.
5. **Drops.** The package takes tables, not groups, and refuses a drop that leaves references.
   `--drop scalar-model` alone is refused, since `AdiabaticHistory` and `BBNData` name
   `ScalarModel.serial`. Today it silently leaves orphans. **U4** says what the CLI does.
6. **Every ChamPBH store is refused by the package** (checklist item 8; decision D4: *rebuilt, not
   migrated*). A read-write open stops first on the primary's missing `replication_in_flight`
   table; behind that are the absolute shard records and the per-shard timestamps of replicated
   rows. A read-only open of even a rebuilt store is refused while `replicated_tables` names
   `LambdaCDM`, which no factory registers. **So the science store must be rebuilt after this
   campaign**, and that rebuild is the user's (§7).
7. **The test harness.** `_TempStoreCase` (`Datastore/tests/test_version_keyed_lookups.py:187-201`)
   builds ChamPBH's actor with two arguments and relies on its constructor inserting the version
   row. The package's actor takes `replicated_tables` and `factories=`, inserts no version row, and
   refuses a keyed lookup until `set_version` has been called. The harness has five importers.

And three things that SGK had and ChamPBH does not:
- **no run registry**, so no manifest, no `begin()`, no liveness records;
- **no fingerprint**, so no single digest to reproduce. §4.2 compares stores by value instead;
- **no store that survives the swap**, so the rehearsal before the DatastoreKit swap and the one
  after it must be compared through something that reads both formats. §4.2's comparison reads
  the SQLite files directly and never through either layer.

### 0.2 Reconciliation against `52142d7`

Measured on 2026-10-11. The outputs are in [`measure/out/`](measure/out/).

**`52142d7` is `HEAD` of `main`, and it is the checklists' commit.** Both measuring scripts, run
from each package's root with its own venv at `--commit 52142d7`, equal their checklist's appendix
**byte for byte**:
- DatastoreKit's `docs/extraction/measure_client_imports.py cpbh`
  ([`52142d7-datastorekit-measure_client_imports.md`](measure/out/52142d7-datastorekit-measure_client_imports.md)):
  41 import statements of the layer in 38 files, none nested; 0 string literals; 0 `import_module`
  calls; 1 factory import outside the factory package (`main.py:46`); 4 imports of the two names
  that move into `datastorekit.contract`.
- RayPoolKit's `docs/adoption/measure_client_pools.py cpbh`
  ([`52142d7-raypoolkit-measure_client_pools.md`](measure/out/52142d7-raypoolkit-measure_client_pools.md)):
  3 imports of `RayTools`, all at module level; 17 `RayWorkPool(...)` calls (14 with no compute
  handler, 3 with one), every one "constructs"; 0 string literals; 0 test modules naming either.

So every figure the checklists give is today's. A prompt re-measures at its own `HEAD` before it
acts, and its numbers govern.

**The tags are on their remotes.** `git ls-remote --tags` gives `refs/tags/v0.2.2` → `c32d027`
(`^{}` `6d8ee9ae2c05…`) for DatastoreKit, and `refs/tags/v0.2.0` → `4e7eb15` (`^{}` `eaef0ce60e2f…`)
for RayPoolKit. `v0.2.3` was tagged after this measurement; §0.3 records it.

**The venv.**
- Python 3.13.16 (`venv/pyvenv.cfg`), `ray==2.53.0` (`requirements.txt:78`), `SQLAlchemy==2.0.46`
  (`requirements.txt:89`). Python and SQLAlchemy equal both packages' tested high end. **Ray 2.53.0
  is between the tested ends (2.43.0, 2.55.1) and is not itself tested by either package**, so the
  rehearsals under real Ray (R1, R2) are the first runs of each package at 2.53.0.
- `pip show datastorekit raypoolkit` finds neither, and `pip check` reports nothing broken.
  `pip freeze` is saved as [`52142d7-pip-freeze.txt`](measure/out/52142d7-pip-freeze.txt), for 03
  and 07 to compare against.
- Each package declares only `ray>=2.43` (and `datastorekit` `sqlalchemy>=2.0.39,<2.1`), which the
  venv satisfies, so each pin should add one distribution and move none.

**The three test roots at `52142d7`**, run in the foreground, serially
([`52142d7-suites.txt`](measure/out/52142d7-suites.txt)):

| Root | Tests | Result |
|---|---|---|
| `CosmologyModels/tests` | 18 | OK |
| `ComputeTargets/tests` | 114 | OK |
| `Datastore/tests` | 35 | OK |

`Datastore/tests` by module: `test_bbn_failure_lookup` 6, `test_first_bounce_round_trip` 5,
`test_fixed_T_values_round_trip` 5, `test_scalarmodel_failure_reason` 4,
`test_shard_key_assignment` 4 (added at `52142d7`, after the bbn-tolerance close's 31),
`test_version_keyed_lookups` 11. `ComputeTargets/tests/test_foreign_bbn_provenance` (6) imports
the Datastore harness.

**Why the registry holds instances, measured for U3.**
- ChamPBH began as a copy of SGK's layer: at its initial commit `dec2d45` (2025-12-09) the base was
  SGK's ABC with `@staticmethod @abstractmethod` hooks, and the registry named classes. Commit
  `5ef07b4` (2026-01-05, "In progress changes") added the two quantity factories, each one class
  serving several tables told apart by an `ObjectType` held on the instance. To do it, it made the
  base a plain class whose hooks raise `NotImplementedError`, removed `@staticmethod` from every
  hook of every factory, and made every registry entry an instance.
- Today only those two hold state: `sqla_dimensionful_quantity_factory` (`ObjectType` and
  `value_col`, `Datastore/SQL/ObjectFactories/DimensionfulQuantity.py:30-32`) and
  `sqla_dimensionless_quantity_factory` (`ObjectType`, `DimensionlessQuantity.py:30`), which fill
  6 of the 27 registry entries. Every other `__init__` is `pass`.
- DatastoreKit's base is SGK's, carried over at extraction (`PROVENANCE.md`: SGK's `base.py` →
  `datastorekit/SQL/factory_base.py`). Its decision U32 (2026-10-09) kept the ABC and asked the
  instance-registering clients (ChamPBH, StochasticInstantons) to change. Its own contract §3
  already describes `store` as "Not defined: the base's `NotImplementedError`", and its base already
  gives `revalidate`, `owned_serials` and `inventory_spec` non-abstract defaults. No DatastoreKit
  test depends on the three hooks being abstract (`git grep` at `v0.2.2`).

**The stores.** None is opened by this campaign but its own rehearsal stores (§3).
- The pilot store in the working tree, `pilot-2026.6.0.db` and 16 shards (untracked), built
  2026-10-02 at 23:28 by the user, before `bbn-tolerance` moved production to PRyMordial's small
  network. Its BBN rows are therefore not today's, and it is not a reference for this campaign.
- The science store `~/ChamPBH-stores/science-2026.6.0.db` and its BBN-refresh copy
  `~/ChamPBH-stores-bt02/`, named only.
- **A hazard the checklist records, that this campaign cures.** ChamPBH records each shard by the
  absolute path of the resolved primary (`Datastore/SQL/ShardedPool.py:88`, `:296`, read back as
  is at `:342`), so a file-level copy of a ChamPBH store in another directory opens the
  *original's* shards. The bbn-tolerance refresh route (`cp -p` to a new directory, then
  `--drop bbn-data`; `.documents/review-remediation-verification.md` §4.11, and
  `pipeline_selection.BBN_REFRESH_ROUTE`) is exposed to it. The package records shards by bare
  name, so after 08 a copy opens its own shards, and the route becomes the package's
  `copy_store` (08, P13). The campaign does not open either store to see what the refresh did.

**What the swaps touch, by grep at `52142d7`.**
- `CLAUDE.md:64-66` (test commands) and `:68-72` (`Datastore.__ray_actor_class__`) describe the
  layer; `CLAUDE.md` names no `RayTools` path.
- `.documents/architecture-summary.md` (1,118 lines): `RayWorkPool` at `:35-36`, `:153-165`, `:585`
  (RayPoolKit checklist item 9); the database layer in §4 (`:213-393`) and §§12–14 (from `:975`)
  (DatastoreKit checklist item 10).
- `.documents/OPEN_ISSUES.md:48`, `[00-two-files-are-not-black-clean]`, names
  `Datastore/SQL/ObjectFactories/base.py`, which 08 deletes.
- `tools/bbn_from_store.py` reads a store's shards read-only through `sqlite3`, by the glob
  `STEM-shard*.db` (`:257`), never through the layer. The package names shards
  `<stem>-shard<NNNN><suffix>` (`datastorekit/shard_paths.py:58-68`), as ChamPBH does
  (`Datastore/SQL/ShardedPool.py:113`), so the glob still holds for a `.db` primary.

**No run is live.** No Ray cluster of this repository is up (the rehearsals start their own, §4.1).

### 0.3 DatastoreKit `v0.2.3`, as delivered (2026-10-11)

Released by DatastoreKit's `factory-defaults` campaign (closed at `83068cb`), from the brief.
- **The tag.** `git ls-remote` gives `refs/tags/v0.2.3` → `8dac562e048bbb80f4179c49f77d3d327b5af7f8`,
  `^{}` → `0de2b161ba09362adc36231a7b5b12d99dec8bb3`. The pin is
  `datastorekit @ git+https://github.com/ds283/DatastoreKit@v0.2.3`.
- **What U3 needs.** At the tag, `datastorekit/SQL/factory_base.py` keeps `register` and `build`
  `@staticmethod @abstractmethod`, and gives `store`, `validate` and `validate_on_startup`
  non-abstract `@staticmethod` defaults that raise `NotImplementedError`, documented as called on
  "the registry's entry, a class or an instance alike". Read through `git show`; 07 checks it on the
  installed package.
- **Four points the implementing agent reported, which the brief did not ask for:**
  1. DatastoreKit's `test_neutral_client.py` had an `ABSTRACT_HOOKS` list, corrected to
     `("register", "build")`. Nothing for ChamPBH.
  2. **The unit tests cannot show that a registered instance survives Ray's serialisation**, since
     the stand-in pool pickles nothing. A new step I of DatastoreKit's smoke script showed it under
     real Ray at both tested ends (Ray 2.43.0 and 2.55.1). ChamPBH's own evidence at Ray 2.53.0 is
     R2 (10, acceptance 3), whose every build passes ChamPBH's 27 instances to the actors.
  3. StochasticInstantons' three factory instantiations at import time also stop raising under
     `v0.2.3`, and its checklist addendum says so.
  4. DatastoreKit's extraction verification document records 07a's observation §7.3 (the contract
     silent on instances) as addressed, in new sections §4.8 and §8.1.

### 0.4 DatastoreKit `v0.3.0`, the pin (U6, 2026-10-12)

Released by DatastoreKit's `object-timestamp` campaign (closed at `5f972be`). The user ruled on
2026-10-12 that this campaign pins it in place of `v0.2.3` (§6.7). Checked read-only that day:
- **The tag.** `git ls-remote` gives `refs/tags/v0.3.0` → `bf0a3b25afa5b4379f9cdc01bca93805ba2a695b`,
  `^{}` → `2f5417ea76cdcd0d15d64d80d9085429bfacad21`; `pyproject.toml` at the tag says `0.3.0`. The
  pin is `datastorekit @ git+https://github.com/ds283/DatastoreKit@v0.3.0`.
- **What changed since `v0.2.3`.** In the package, only `datastorekit/object.py`:
  `DatastoreObject.__init__(self, store_id, *, timestamp=None)` keeps the value as `_timestamp`
  and exposes a read-only `timestamp` property, which the layer never sets or reads. The rest
  is tests and the version. `git diff v0.2.3 v0.3.0 -- datastorekit/SQL/` is empty, so
  **`factory_base.py` is `v0.2.3`'s**, and U3 holds at `v0.3.0` as §0.3 describes it.
- **What it means for ChamPBH.** ChamPBH's 19 `DatastoreObject.__init__(…)` calls pass the store
  id alone, positionally, and no ChamPBH class defines or assigns `timestamp` or `_timestamp`
  (`git grep` at `7ed8546`). Every call is accepted at `v0.3.0` as at `v0.2.3`, and nothing
  ChamPBH stores or reads changes. A factory *may* pass a row's `timestamp` to the object it
  builds; this campaign does not (§1: the factories' form is out of scope). DatastoreKit's ChamPBH
  checklist says the same in its `v0.3.0` addendum, and reads its G3 as "ChamPBH has adopted
  `v0.2.3` or later".

## 1. Scope

**In scope:**
- run provenance in the drivers' output, and the rehearsal tooling (01);
- the three rehearsals R0, R1, R2, and their verification document (02, 06, 10);
- `requirements.txt`: the two pins (03, 07);
- `RayTools/`, deleted, and its 3 imports rewritten (04);
- ChamPBH's data moved out of the layer into `config/datastore.py`, and the 7 vectorized gets put
  in the mapping form (05);
- the 9 layer files (two `__init__.py` kept, emptied, P6), every layer import outside `prompts/`
  rewritten by the module map, the pool constructions, the drop policy (U4), the test harness, and
  the tests the package now carries (08);
- the inventory: `inventory_spec()` on the factories, a ChamPBH report over `read_inventory`, and
  `main.py --inventory` on a closed store (09);
- prose that a prompt's own commit makes false (P14), and the documents and handover at the close
  (11).

**Out of scope:**
- **What the code computes.** No stored science value may change (§5 rule 9). `VERSION_LABEL`
  stays `2026.6.0` (P10).
- **DatastoreKit and RayPoolKit themselves** (§5 rule 12). The `v0.2.3` release is made in
  DatastoreKit, by an agent working there from the brief; no prompt here makes it. A defect found
  in either package stops the prompt (§5 rule 11).
- **The factories' form.** They stay instances of classes whose hooks take `self` (U3). No hook
  becomes a `staticmethod`, and the quantity factories keep their constructor argument.
- **Migrating stores.** D4: ChamPBH's stores are rebuilt, not migrated. No converter is written.
- **The science store and its refresh copy.** Not opened, read or rebuilt by any prompt. Their
  rebuild is the user's, after the close (§7).
- **The untracked run scripts** `full_run_2026.6.0.sh` and `full_run_2026.6.0-refresh.sh`. They are
  the user's; 11 says what in them goes stale.
- **Read-only pools in the plot scripts** (P11).
- **`prompts/`**: its one layer import and 21 lines naming `RayTools` are history.
- **`prompts/jordan-normalization/`** (untracked, a brief for a campaign not yet planned).
- **Dated `.documents/` audits and verification records** (P14).

## 2. Prompts

| # | Prompt | Covers | Model |
|---|---|---|---|
| 01 | `01-rehearsal-tooling-and-run-provenance.md` | P5. Each driver prints the installed `datastorekit` and `raypoolkit`. `tools/compare_stores.py`, the layer-independent comparison of two stores by value (§4.2), with its tests. The rehearsal scripts under `.documents/dk-and-rpk-migration/rehearsal/` | Opus |
| 02 | `02-rehearse-the-start-tree.md` | **R0.** §4.1's protocol at 01's tree. It measures whether two builds of one tree agree bit for bit, which sets R1's and R2's criterion (§4.3) | Opus |
| 03 | `03-pin-raypoolkit-v0.2.0.md` | The `raypoolkit` pin in a commit of its own, installed, one distribution added | Sonnet |
| 04 | `04-adopt-raypoolkit.md` | **The RayPoolKit swap.** `RayTools/` goes and 3 imports are rewritten; all 17 sites construct. **RayPoolKit's G3 holds** by its checklist | Sonnet |
| 05 | `05-move-champbh-data-out-of-the-layer.md` | `config/datastore.py` holds the registry, the drop groups and the serial batch sizes, and the old layer reads them from there. The 7 vectorized gets take `{"shard_key": beta}`. On the old layer; the schema does not change | Sonnet |
| 05a | `05a-isolate-the-rehearsal-cluster.md` | **U5.** `ray_cluster.sh start\|stop\|status`, adapted from StochasticInstantons': ChamPBH's cluster on its own temp dir and ports, stopped by a targeted stop, never `ray stop`, under the shared rehearsal lock. Proved on a real cluster at Ray 2.53.0 beside a decoy, with no build | Sonnet |
| 06 | `06-rehearse-before-the-datastore-swap.md` | **R1.** §4.1 at 05's tree: `raypoolkit` and the data move, still on ChamPBH's layer. Equal to R0 | Opus |
| 07 | `07-pin-datastorekit-v0.3.0.md` | The `v0.3.0` pin (§0.4; U6) in a commit of its own, installed as 03; ChamPBH's 20 partial factory classes instantiate on the installed base | Sonnet |
| 08 | `08-adopt-datastorekit.md` | **The DatastoreKit swap.** The 9 files go (two emptied, P6); every layer import outside `prompts/` is rewritten; the pools take `factories=`, `serial_batch_sizes=` and `drop_tables=`; U4's drop refusal; `LambdaCDM` leaves `replicated_tables`; the harness is rebuilt on the package's actor; the carried tests are deleted; `--inventory` refuses until 09 | Opus |
| 09 | `09-port-the-inventory.md` | `inventory_spec()` on the factories the report covers; `tools/inventory_report.py` over `read_inventory`; `main.py --inventory` on the closed store, before Ray; the six `inventory()` hooks and `inventory_config` deleted | Opus |
| 10 | `10-rehearse-through-both-packages.md` | **R2.** §4.1 through both packages. Equal to R1 by §4.2. **DatastoreKit's G3 holds** | Opus |
| 11 | `11-close-the-migration.md` | The documents (`architecture-summary.md`, `CLAUDE.md`'s test section); the final measurements; the G3 records drafted for both packages' boards; the handover of the science-store rebuild (§7); the index; the board closed | Sonnet |

### 2.1 When each prompt file is written

Every charter below is fixed by a checklist, by U1–U4 or by §4, so no prompt waits on another's
*scope*. The files are still written in batches, each just before its dispatch, so that each
carries the previous review's findings:
- **01–04** are written first, once the user has reviewed this plan and ruled on P1–P17.
- **05–06** follow 02's review: R0's reproducibility (§4.3) fixes 06's criterion, and 05's schema
  check uses R0's tree.
- **05a** follows 05's review and the ruling U5. *Written 2026-10-12.* 06 waits on it.
- **07–10** follow 06's review: 06's log fixes the counts and names they cite. `v0.3.0`'s tag and
  commit are §0.4's (U6); its ChamPBH checklist addenda, `v0.2.3`'s and `v0.3.0`'s, are read when
  07 and 08 are written. *Written 2026-10-12, after 06's review (`fa874a5`).*
- **11** follows 10's review.

`v0.2.3` was released on 2026-10-11 with what the brief asked (§0.3), and `v0.3.0`, the pin (U6),
on 2026-10-12 (§0.4), so no prompt waits on a release.

### 2.2 The charters

**01 — Rehearsal tooling and run provenance** (Opus).
- **What it adds.**
  - **P5, provenance.** A helper, `utilities.installed_package_provenance(names) -> dict`, giving
    for each distribution `None` if it is not installed, else `{"version", "url", "commit",
    "requested_revision", "editable"}` from `importlib.metadata` and its PEP 610
    `direct_url.json` (`None` for any field the install does not record). It is SGK's
    `RunRegistry.package_provenance()` in shape. `main.py`, `plot_by_beta.py` and
    `plot_ScalarModel.py` each print one line from it, for `datastorekit` and `raypoolkit`, before
    they open a pool. It never refuses, even for an editable install.
  - **`tools/compare_stores.py`**, §4.2's comparison, with its tests in
    `Datastore/tests/test_compare_stores.py`, on temporary SQLite files built by `sqlite3` alone.
    It also writes a store's **schema snapshot** (§4.2), which 05 and 08 compare.
  - **The rehearsal scripts** under `.documents/dk-and-rpk-migration/rehearsal/` (§4.1):
    `build.sh` (one build, given the store, the slug and whether it resumes), `interrupt_when.py`
    (sends `SIGTERM` to a driver when its log shows a stage banner and a stated delay has passed),
    `stage_counts.py` (each stage's queue-summary counts from a log, without the rates), and
    `run_readers.sh` (§4.1's L3).
- **Acceptance.**
  1. The provenance tests, with stand-in distributions and no real install, cover four cases: not
     installed; a VCS install at a tag; an editable install; a `direct_url.json` that is absent or
     unreadable. In today's venv both entries print as not installed.
  2. The comparison's tests cover: two equal stores (empty diff, exit 0); a store with one float
     changed in its last bit (named, exit 1); a row present in one only (named); serials and shard
     assignment that differ while content is equal (empty diff); a bookkeeping column that differs
     (reported, not counted); a primary with absolute shard records and one with bare names (both
     read); and that no file it reads changes (checksums before and after).
  3. Each script runs with `--help` and is `black`-clean. No script is run on a real build yet.
  4. Suites: CosmologyModels 18 and ComputeTargets 114 unchanged; Datastore 35 plus the new tests.
- **Out.** Any run of `main.py` on a store (02 does that). The science store.

**02 — Rehearse the start tree, R0** (Opus).
- **What it does.** §4.1's protocol, slug prefix `dk-and-rpk-migration-02`, at 01's tree, on
  ChamPBH's layer and `RayTools/`.
- **Acceptance.**
  1. §4.1's L1–L3 hold.
  2. Every driver's provenance line names both packages as not installed.
  3. **§4.3, measured.** `compare_stores` of `reference` against `resumed` is recorded in full. If
     it is empty, R0 is **bitwise reproducible**, and R1 and R2 are held to bitwise equality. If
     it is not, the log says which rows and columns differ and by how much, and the prompt stops
     for the user to rule on R1's criterion before 05–06 are written.
  4. L4's baseline is recorded: each stage's counts, and what the resume recomputed.
  5. The schema snapshot of `reference` is saved, as the start tree's.
- **Its own stop condition.** A build that fails, or an L1–L3 failure, at the start tree is
  ChamPBH's, not this campaign's to fix. The prompt stops and reports, and nothing after it is
  dispatched.
- **It creates** `.documents/dk-and-rpk-migration-verification.md` and adds its §1.

**03 — Pin `raypoolkit` `v0.2.0`** (Sonnet).
- **What it changes.** `requirements.txt` gains
  `raypoolkit @ git+https://github.com/ds283/RayPoolKit@v0.2.0`, in sorted position; nothing else.
  The venv is installed from it (`./venv/bin/pip install -r requirements.txt`; needs the network).
  Never `-e`, a local path, `--upgrade`, `--force-reinstall` or `--no-deps`.
- **Acceptance.**
  1. `pip freeze` differs from `52142d7-pip-freeze.txt` by exactly one added line, `raypoolkit` at
     `eaef0ce60e2f…`; `pip check` is clean.
  2. `installed_package_provenance()["raypoolkit"]` gives `0.2.0`, commit `eaef0ce6…`, requested
     revision `v0.2.0`, `editable: false`; `["datastorekit"]` is `None`.
  3. The installed `raypoolkit/` equals `git -C <RayPoolKit> archive v0.2.0 raypoolkit`, file by
     file, less any test package.
  4. The three roots give 01's counts. Nothing imports the package yet.
- **Rollback** is the revert plus `./venv/bin/pip uninstall -y raypoolkit`.

**04 — Adopt `raypoolkit`** (Sonnet).
- **What it changes.**
  1. It deletes `RayTools/RayWorkPool.py` and `RayTools/__init__.py`. `utilities.format_time` stays
     ChamPBH's.
  2. It rewrites the 3 imports (`main.py:50`, `plot_ScalarModel.py:44`, `plot_by_beta.py:38`) to
     `from raypoolkit import RayWorkPool`. With them goes `RayTools/RayWorkPool.py:23`'s
     `from Datastore.SQL.ShardedPool import ShardedPool`, so DatastoreKit's rewrite list loses one
     line and no U12 exception is needed.
  3. It changes no pool site: the 14 lookup-only sites keep `store_handler=None` (RayPoolKit's
     advice; P7).
  4. `.documents/architecture-summary.md`'s lines that this commit makes false (P14): the layout at
     `:35-36`, the section title at `:153`, and the constructor table from `:157`, whose
     `store_handler` row is the old meaning (RayPoolKit decision U10) and which has no
     `persist_handler` row.
- **Acceptance.**
  1. `git ls-tree HEAD RayTools` is empty.
  2. **By RayPoolKit's script, at the commit:** 0 imports, 0 string literals and 0 `import_module`
     calls name `RayTools`; 17 calls, every one "constructs".
  3. The three roots give 03's counts; `black --check` is clean on every touched file.
  4. `main.py`, `plot_by_beta.py` and `plot_ScalarModel.py` byte-compile, and each module they
     import resolves, by `ast` (none is executed).
  5. The deliberate breakages of §5 rule 8 are caught. At least: one import reverted to
     `RayTools.RayWorkPool` (the script reports it and the import fails); `raypoolkit` uninstalled
     (the import check fails).
  6. **The board records RayPoolKit's G3 as holding** by its checklist item 8, items 1–5. R1 (06)
     adds the run under real Ray.

**05 — Move ChamPBH's data out of the layer** (Sonnet). On ChamPBH's own layer.
- **What it changes.**
  1. A new `config/datastore.py` holds `factories` (the registry `_factories`, moved verbatim with
     its imports: 27 entries, each an instance, as today), `drop_groups` (`_drop_actions`'s three
     groups, verbatim) and `serial_batch_sizes` (`_default_serial_batch_size`, verbatim, `LambdaCDM`
     included until 08). This is SGK's module name for the same data.
  2. The old layer imports them from there: `Datastore/SQL/Datastore.py` its registry and drop
     groups, `Datastore/SQL/ClientPool.py` its batch sizes. `_drop_order` stays in the layer,
     which 08 deletes, since the package orders a drop by foreign key. Nothing else in the layer
     changes.
  3. The 7 vectorized gets in `main.py` pass `{"shard_key": <beta>}` in place of the bare
     `beta_value`, which the old layer already accepts (`ShardedPool.py:595-602`). Nothing else in
     their calls changes.
- **Acceptance.**
  1. **The schema does not change.** A temporary store built by the old actor at 05's tree has the
     schema snapshot of a store built at 04's tree, table by table. The log shows both.
  2. A test that `config.datastore`'s three objects equal what the layer held at 04's tree, captured
     as data (the registry by name, class and constructor argument); and an `ast` test that every
     `object_get_vectorized` call in the drivers passes a mapping as its shard key (P8).
  3. The three roots give 04's counts plus the new tests.
  4. Deliberate breakages caught: one vectorized get reverted to the bare key; a batch size changed;
     a registry entry given another quantity type.

**05a — Isolate the rehearsal cluster** (Sonnet; added for U5).
- **What it adds.** `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh`, copied from
  StochasticInstantons' at `3556148` and made ChamPBH's: `start` takes the shared lock and starts
  ChamPBH's cluster on `/tmp/ray-cpbh`, ports 6380 / 6381 / 8267 / 52367, 10 CPUs as R0's; `stop`
  stops it by a targeted stop, confirms all four ports free, and releases the lock; `status`
  reports both. The contract is the brief's §2–§3 (§6.6), shared with SI and SGK.
- **Acceptance.** A real start and stop at Ray 2.53.0, with no build: a second acquire refused; a
  decoy on Ray's default ports, whose temp dir has ChamPBH's as a prefix, left running by ChamPBH's
  stop; ChamPBH's actors listed by an invocation that reaches its cluster alone; a lock held by
  another project neither broken nor released. Four breakages. The suites unchanged.
- **Out.** Any build; any change to 01's scripts or to Python; SI and SGK, which carry out the
  brief themselves.

**06 — Rehearse before the DatastoreKit swap, R1** (Opus).
- **What it does.** §4.1's protocol, slug prefix `dk-and-rpk-migration-06`, at 05's tree:
  `raypoolkit` and the data move, on ChamPBH's layer.
- **Acceptance.**
  1. L1–L3 hold.
  2. Every provenance line names `raypoolkit` at `eaef0ce6…`, `editable: false`, and
     `datastorekit` as not installed.
  3. **Against R0, by §4.2 and §4.3:** each R1 store equals R0's `reference`.
  4. **L4 against R0:** each stage's counts and what the resume recomputed are R0's; the queue
     banner, progress and summary lines keep their layout (RayPoolKit's "printed lines are part of
     the interface").
- **If R1 misses,** the change is in 03–05, all on ChamPBH's layer. The prompt stops and records
  the diff; the user may have a reference build run at 04's tree to tell `raypoolkit` from the
  data move.

**07 — Pin `datastorekit` `v0.3.0`** (Sonnet; *amended 2026-10-12 for U6*). As 03, for
`datastorekit @ git+https://github.com/ds283/DatastoreKit@v0.3.0` (§0.4). It reads the ChamPBH
checklist's `v0.2.3` and `v0.3.0` addenda, and re-checks the tag on the remote before installing.
- **Acceptance.**
  1. `pip freeze` differs from 03's by exactly one line, `datastorekit` at
     `2f5417ea76cdcd0d15d64d80d9085429bfacad21`; `pip check` is clean.
  2. Provenance gives `0.3.0`, commit `2f5417ea…`, requested revision `v0.3.0`, `editable: false`.
  3. The installed `datastorekit/` equals `git archive v0.3.0 datastorekit` less
     `datastorekit/tests` (adoption README §3).
  4. **What U3 needs of the release, checked on the installed package without touching ChamPBH's
     tree:** a throwaway script, outside the repository, defines a subclass of the installed
     `datastorekit.SQL.factory_base.SQLAFactoryBase` with only `register` and `build`, as 20 of
     ChamPBH's classes have, and instantiates it; and a subclass without `build` still raises
     `TypeError`. Both are shown in the log.
  5. **What U6 adds, checked the same way:** a subclass of the installed
     `datastorekit.object.DatastoreObject`, constructed as ChamPBH's are (`store_id` alone,
     positionally), has `timestamp` `None`.
  6. The three roots give 05's counts.

**08 — Adopt `datastorekit`** (Opus). One commit, because the tree cannot pass between its parts.
- **What it changes.**
  1. **The files.** It deletes the 7 layer modules (`Datastore/object.py`,
     `Datastore/SQL/{ClientPool,Datastore,ProfileAgent,SerialPoolBroker,ShardedPool}.py`,
     `Datastore/SQL/ObjectFactories/base.py`) and replaces `Datastore/__init__.py` and
     `Datastore/SQL/__init__.py` with empty files (P6). The factories stay in
     `Datastore/SQL/ObjectFactories/`.
  2. **The imports**, rewritten by the adoption README §2 module map, module path only:
     `DatastoreObject` (13), `SQLAFactoryBase` (17, to `datastorekit.SQL.factory_base`),
     `ShardedPool` (4: `main.py`, both plot scripts, `config/model_list.py`), `ProfileAgent` (3),
     and the actor in the harness. `VERSION_SERIAL_KEY` and `require_version_serial` come from
     `datastorekit.contract` and leave `config/version.py`, whose `VERSION_LABEL` stays (P9;
     checklist item 3's hazard of the two `VERSION_LABEL`s).
  3. **The factories change nothing but that import** (U3). They stay instances; their hooks keep
     `self`; the 20 partial classes instantiate on the pinned base (`v0.3.0`'s, which is
     `v0.2.3`'s; §0.4).
  4. **The pools.** The three constructions pass `factories=config.datastore.factories` and
     `serial_batch_sizes=…`; `main.py`'s passes `drop_tables=`; none passes `drop_actions=` or
     `inventory_config=`. `LambdaCDM` leaves `replicated_tables` and `serial_batch_sizes` (P8).
  5. **U4, the drop policy.** `main.py` turns `--drop` groups into tables. Before Ray starts, it
     computes each requested table set's closure with `datastorekit.SQL.schema.dependent_tables`
     and, if a group's dependents are not all requested, refuses with a message naming the groups
     to add (for `scalar-model` alone: "add `--drop adiabatic-history bbn-data`"). A test pins the
     message for `scalar-model` alone and acceptance for each closed set.
  6. **The harness.** `_TempStoreCase.open(label)` builds the package's actor through
     `datastorekit.SQL.Datastore.__ray_actor_class__`, with `replicated_tables` and `factories=`,
     gets the `version` row for the label and calls `set_version` with its serial, as the pool
     does. The worked example is the package's `test_version_keyed_lookups.TestTheActor`, read in
     DatastoreKit's repository and not imported. Its five importers change only their imports, if
     at all. *Amended 2026-10-12:* the package's actor needs a serial broker, which its tests supply
     from `datastorekit/tests/standin_pool.py`, which the wheel does not ship. ChamPBH keeps its own
     copy, `Datastore/tests/standin_pool.py`, from the tag, changed only to open ChamPBH's registry
     (adoption README §3); the harness and 09's test stores are built through it. Test a's read of
     `_version.store_id`, which the package's actor lacks, is the one test body that changes.
  7. **The tests the package carries.** `test_shard_key_assignment.py` (4) is deleted; the package
     pins the same fix. In `test_version_keyed_lookups.py`, d2 and d4 (layer tests the package
     carries) are deleted; a, b, c1–c3, d, d3, e and f stay. *Amended 2026-10-12:* 05's
     `test_datastore_config.py` imports the layer at module level, and its `TestLayerReadsConfig`
     (3 tests) asserts that the layer's names are `config.datastore`'s objects: the imports go, the
     three tests are deleted (there is no layer to read the config), and its batch-size literal loses
     `LambdaCDM` (P8). A new test that each pool is given `config.datastore`'s objects takes their
     place. `test_compare_stores.py`'s `_registry()` loses its fallback to the layer.
  8. **`--inventory`** prints that it is unavailable until 09 and exits non-zero, before Ray
     starts (P12). The four `pool.inventory` calls and their helpers are deleted.
  9. **The refresh route.** `pipeline_selection.BBN_REFRESH_ROUTE` names
     `python -m datastorekit.tools.sharded_store copy SRC DST` in place of a copy (P13).
  10. **Prose made false by this commit** (P14): `CLAUDE.md:64-72`'s harness paragraph;
      `.documents/OPEN_ISSUES.md:48` and its board entry narrowed, since `base.py` is deleted.
- **Acceptance.**
  1. **By DatastoreKit's script, at the commit.** None of the 7 deleted modules is present; the two
     `__init__.py` are present and 0 bytes. Outside `prompts/` there are 0 import statements, 0
     string literals and 0 `import_module` calls naming a layer module; `prompts/` still holds its
     one. No import of `config.version`'s two moved names remains.
  2. **The suites.** Datastore gives exactly 05's count − 4 − 2 + the tests 08 adds; the log derives
     it by module. *Amended 2026-10-12:* 05's count − 4 − 2 − 3 (item 7's amendment) + the tests 08
     adds. ComputeTargets and CosmologyModels give 05's counts. `black --check` is clean on
     every touched file.
  3. **Without Ray:** `config.datastore` imports, so all 27 registry entries are built on the
     package's base; `python -m datastorekit.tools.sharded_store --help` exits 0. The drivers
     byte-compile, and every module they import resolves (by `ast`; none is executed). A temporary
     store built through the rebuilt harness has 05's schema snapshot, but for the columns the
     package adds or orders differently, each listed in the log.
  4. **The deliberate breakages of §5 rule 8**, at least: a factory's `SQLAFactoryBase` import
     pointed back at `Datastore.SQL.ObjectFactories.base`; a `from Datastore import
     DatastoreObject` restored; `LambdaCDM` restored to `replicated_tables`; the drop check
     removed (U4's test fails); `set_version` dropped from the harness.
- **Out.** The inventory (09). Any store but the harness's temporary ones; 08 runs no build.

**09 — Port the inventory** (Opus).
- **What it changes.**
  1. Each factory whose class the report covers defines `inventory_spec()`: `version`,
     `store_tag`, `redshift`, `tolerance`, the six quantities, the potentials, `ExponentialCoupling`,
     `QCD_Cosmology`, `IntegrationSolver`, `ScalarModel`, `AdiabaticHistory` and `BBNData`, with
     their tags and value tables. Every class a spec depends on declares one (contract §4). The
     package reads it by `getattr` on the registry entry, so an instance method serves. Which
     columns are leaves and which are parents is the prompt's to derive from each factory's `build`
     lookup, and is recorded in the log, table by table. *Amended 2026-10-12 (P15):* beyond the
     lookups, `ScalarModel` declares `failure` and `BBNData` declares `PRyM_version` as leaves, the
     two fields P15 needs that a lookup does not fix; the stored `label`, which carries the job name
     and time, is not a leaf.
  2. A new `tools/inventory_report.py`, `format_inventory_report(inventory, db_name, units,
     verbose=False)`, renders a `StoreInventory` and reads nothing itself. It is modelled on SGK's
     `tools/inventory_report.py`, read in that repository and not imported. It shows at least what
     today's report shows (P15): for each quantity, its count, timestamp range and values in its
     default unit; for `ScalarModel`, `AdiabaticHistory` and `BBNData`, validated and unvalidated
     counts, version labels, timestamp ranges and each record by its physical labels (T_init,
     T_stop, φ*, π*, failure, and `PRyM_version` for BBN); and every named problem, never truncated.
     *Amended 2026-10-12:* the package's inventory carries one timestamp range per class, not one per
     validated group, and records hold no timestamp, so the report gives the class's range. The
     `units` argument is dropped: each stored `value_<unit>` column is already in its default unit.
  3. `main.py --inventory` reads the closed store with `read_inventory(args.database,
     factories)` **before `ray.init`**, prints the report, and exits. `--show-all` maps to
     `verbose`. `--inventory` with `--drop` is refused.
  4. The six `inventory()` hooks and `config/sharding.py`'s `inventory_config` are deleted.
- **Acceptance.**
  1. Tests on temporary stores built through the harness: `inventory_classes` accepts the
     registry (no `ValueError`); each spec's leaves and parents resolve; a store with one failed
     and one successful `ScalarModel` and a `BBNData` row renders every field of P15's table;
     problems are printed in full.
  2. `main.py --inventory` on a missing store fails cleanly, and needs no Ray.
  3. The three roots give 08's counts plus the new tests.
  4. Deliberate breakages caught: one spec's parent removed; one leaf misnamed; `--inventory`
     moved after `ray.init` (an `ast` test).
  5. **Coverage, for the fingerprint** (§6.5, ruled 2026-10-12). A test walks the registry
     (`Datastore/SQL/Datastore.py`'s `_factories`, or `config/datastore.py`'s once 05 has moved it)
     and puts every registered class in exactly one of three places: it declares a spec; it is a
     tag, value or parent-set table read through an owner's spec, the test naming the owner and
     the slot; or it is excluded, by name, in a table in the test with a one-line reason. The test
     fails on a class in none of the three, and on an exclusion naming a class no longer
     registered. Measured at `12c82e8`, all 27 entries fall in the first two (§6.5), so the
     exclusion table starts empty. Deliberate breakages: the spec removed from one owner, and a
     new registered class with none, each fail it.

**10 — Rehearse through both packages, R2** (Opus).
- **What it does.** §4.1's protocol, slug prefix `dk-and-rpk-migration-10`, at 09's tree.
- **Acceptance.**
  1. L1–L3 hold; `main.py --inventory` reports no named problem on either store.
  2. Both provenance entries name their tag's commit, `editable: false`.
  3. **Against R1, by §4.2 and §4.3:** each R2 store equals R1's `reference` (and so R0's). These
     builds are also ChamPBH's evidence, at Ray 2.53.0, that its registry of instances crosses into
     the package's actors intact (§0.3, point 2).
  4. **L4 against R1:** the stage counts, the progress lines and what the resume recomputed are
     R1's. The resume's prune at open is the package's code now.
  5. **Store hygiene by the package:** `python -m datastorekit.tools.shard_key_audit` on each store
     reports nothing; a `sharded_store copy` of `reference` opens read-only and compares equal to
     it by §4.2; the copy is then deleted with `delete_store`.
  6. The board records **DatastoreKit's G3 as holding**.

**11 — Close the migration** (Sonnet).
- **What it changes.**
  1. `.documents/architecture-summary.md`: §4 "The Database Layer" and §§12–14 rewritten for the
     packages (what ChamPBH supplies, what the package does, with pointers to the client contract
     rather than copies of it); `RayWorkPool`'s remaining lines checked. `CLAUDE.md`'s
     "Repository mechanics": the packages, the rule that a package defect is reported upstream and
     never patched here, and the test commands with their counts.
  2. The final measurements at `HEAD` by both packages' scripts, saved, with the three roots'
     counts.
  3. In the verification document, the G3 records for DatastoreKit's and RayPoolKit's extraction
     boards, drafted; neither is committed there (P16).
  4. The handover (§7), as `.documents/review-remediation-verification.md` §4.12, which is where
     the user's next run starts.
  5. `prompts/INDEX.md` and `.documents/OPEN_ISSUES.md`; the board closed.
- **Acceptance.**
  1. The index's count and date agree with its rows.
  2. Nothing outside `prompts/` and dated `.documents/` records names a deleted file as a current
     path.
  3. The three roots pass.

## 3. Stores

**The campaign builds six throwaway stores and touches no other.** Each rehearsal builds two, at
`~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/NN-reference.db` and `NN-resumed.db` with their shards,
for NN = 02, 06 and 10 (P2). They are outside the repository, so nothing can commit them, and they
keep the `.db` suffix that `tools/bbn_from_store.py`'s glob expects. `~/ScienceData` does not
exist on 2026-10-11; 02 creates the directory.

**No prompt deletes a rehearsal store** (10's copy aside). R0's and R1's are compared with later
ones. The user removes them after 11's review, or not.

**The 02 and 06 stores are refused by the package**, like every ChamPBH store (D4). That is
expected and is why §4.2 reads files directly. Neither 08 nor 09 opens one through the package.

## 4. The interfaces between prompts

- **01 ships** `utilities.installed_package_provenance()`, read by 03, 06, 07 and 10;
  `tools/compare_stores.py` and its schema snapshot, run by 02, 05, 06, 08 and 10; and the
  rehearsal scripts, which 06 and 10 run **unchanged**. A change to one is a deviation, logged and
  reviewed, since L4 compares their outputs.
- **05a ships** `ray_cluster.sh` (U5), with which 06 and 10 start and stop ChamPBH's cluster under
  the shared rehearsal lock (§4.1). It joins the rehearsal scripts, and is run unchanged.
- **02, 06 and 10 each add a section** to `.documents/dk-and-rpk-migration-verification.md`, which
  02 creates: §1 R0, §2 R1, §3 R2. 11 adds §4, the close.
- **03 and 07 ship the pins**, exactly as §2.2 gives them, in `requirements.txt` and in no other
  file.
- **05 ships** `config/datastore.py` with `factories`, `drop_groups` and `serial_batch_sizes`. 08
  removes `LambdaCDM` from `serial_batch_sizes` and passes all three to the package.
- **08 ships** the harness on the package's actor; 09's tests build their stores through it.
- **DatastoreKit ships `v0.2.3`**, outside this campaign, from the brief, and **`v0.3.0`**, which
  carries it and is the pin (U6). 07 reads `v0.3.0`'s tag and both ChamPBH checklist addenda.

### 4.1 The rehearsal protocol

Run by the agent in the session, by the user's ruling **U2**. It is a one-off exception to
`CLAUDE.md`'s "no campaign prompt starts" a `main.py` run with a datastore, and to its limit of "a
few PRyMordial solves", for prompts 02, 06 and 10 only. NN is the prompt's number.

**The cluster** (*amended 2026-10-12 for U5*; §6.6).
- Before anything, the agent starts ChamPBH's cluster with 05a's
  `.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh start "dk-and-rpk-migration NN (Rk)"`.
  It takes the rehearsal lock shared with StochasticInstantons and SecondaryGWKit
  (`~/ScienceData/.rehearsal-lock`), and starts a cluster on `/tmp/ray-cpbh` and ChamPBH's own
  ports, 10 CPUs as R0's, at `127.0.0.1:6380`. **If the lock is held, the script refuses (exit 3)
  and prints its owner; the agent stops and asks**: another project's rehearsal is running, or a
  stale lock needs the user. It never waits on the lock and never breaks it.
- It passes `127.0.0.1:6380` to every driver with `--ray-address`, and gives every `ray` CLI call
  its address (`--address 127.0.0.1:6380`), as `start` prints. A driver run without
  `--ray-address` takes `config/argument_parser.py`'s default, `"auto"`, which follows
  `/tmp/ray/ray_current_cluster` to another project's cluster or to none; so none is.
- At the end, and after any stop, it runs `ray_cluster.sh stop`, as a command on its own, which
  stops ChamPBH's processes alone, confirms its ports free and releases the lock. **No prompt runs
  `ray stop`** (§5 rule 15): it stops every Ray process on the machine, other projects' included.
  The stop also kills anything whose command line names `/tmp/ray-cpbh`, so no shell or background
  task that names it (a `tail -f` of a Ray log) is left running across a stop.
- Other projects' clusters may be up meanwhile, isolated and not rehearsing; the agent leaves them
  alone. The lock keeps rehearsals apart, not test suites, so the agent records the load and asks
  the user to hold other heavy work off if it is high.
- R0 ran before U5, on Ray's default cluster (`127.0.0.1:6379`, `/tmp/ray`). Isolation changes no
  stored value, so R1 and R2 are still compared with it.

**The grid** is the user's pilot of 2026-10-02 (`pilot-main.log`), so that it is a configuration
the user has already run, about 9 minutes a build there (P1):

```bash
PYTHONPATH=. ./venv/bin/python -u main.py --config exponential.yaml \
    --database ~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/NN-<store>.db --shards 16 \
    --job-name dk-and-rpk-migration-NN-<build> --M-values-Mp 1e-3 \
    --beta-values 1.1 1.15 1.2 10 25 --ray-address <address> [--prune-unvalidated]
```

Five histories, two of them super-Planckian at φ* = 5 (β = 10 and 25), which are warned about and
computed (`[[warn-dont-filter-initial-data]]`).

**The builds,** each detached, its output in `~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/logs/`,
with the driver's exit status appended:
1. **`reference`**, on the `reference` store, straight through.
2. **`resumed-a`**, on the `resumed` store. `interrupt_when.py` sends `SIGTERM` once the log shows
   `** CALCULATE ADIABATIC TRANSGRESSION PARAMETERS` and the stated delay has passed, so that some
   `AdiabaticHistory` rows are stored and some are not.
3. **`resumed-b`**, on the same store with `--prune-unvalidated`, straight through.

The agent waits on each build's exit (a background task), not by polling.

**What must hold.**
- **L1 — every build ends as it should.** `reference` and `resumed-b` exit 0 with every stage's
  "ALL WORK ITEMS COMPLETE"; `resumed-a` exits by the signal. No traceback in any log.
- **L2 — the resume is the reference.** `compare_stores reference resumed` is §4.3's result. The
  resume recomputed nothing it had stored (by `stage_counts.py`), and the prune at open removed only
  unvalidated rows (by its printed report).
- **L3 — every reader.** Run on the closed `resumed` store, writing under
  `~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/NN-readers/`:
  - `main.py --inventory` (at 02 and 06 through ChamPBH's pool; at 10 through `read_inventory`);
  - `plot_by_beta.py --phi-init-Mp 5`, whose `histories.csv` 06 and 10 compare with 02's, byte for
    byte;
  - `plot_ScalarModel.py`, exit 0;
  - `tools/bbn_from_store.py` on one history (`--beta 1.15 --M-Mp 1e-3`, the `prod` variant: one
    PRyMordial solve), whose Yp and D/H equal the stored row's.

  Each exits 0. The readers open read-write pools (P11), so the check that they changed nothing is
  `compare_stores` of the store before and after them: empty, bookkeeping aside.
- **L4 — equal to the previous rehearsal.** Against the campaign's previous rehearsal (none for
  R0): each stage's work-item counts, and the rows the resume pruned and recomputed. If L1–L3 pass
  and L4 differs, that is a finding to explain, not an automatic failure.

**If a comparison misses** (§4.3), the prompt stops, records `compare_stores`'s full output, and
changes no code. R0's miss belongs to ChamPBH (02). R1's is in 03–05. R2's is reported to the
package's repository as a defect if it is the package's (§5 rule 11), and is ChamPBH's if it is in
08–09.

### 4.2 Comparing stores by value

`tools/compare_stores.py A B` compares two ChamPBH stores by what they hold, never by how they are
laid out, and **reads both through `sqlite3` with `mode=ro` only, never through either layer**. So
it reads a store of either format: ChamPBH's (absolute shard records) and the package's (bare
names, `replication_in_flight`). It finds a store's shards by the glob `<stem>-shard*<suffix>`
beside the primary, and checks the count against the primary's `shards` table.

- **What it compares:** every row of every client table, on every shard. A table's rows are taken
  as a multiset of **identities**. A row's identity is its columns but the bookkeeping ones, with
  each foreign-key column (`PRAGMA foreign_key_list`) replaced by the identity of the row it
  names, recursively. A float is compared by its stored bits (`float.hex`). So serials, shard
  assignment and insertion order do not matter, and every stored science value does.
- **Bookkeeping columns** are `serial` and `timestamp` everywhere, and any column whose value
  carries a job name, a time or a host. 01 derives the list by reading every factory, and tests it.
  They are reported, never compared.
- **Not compared:** the primary's own tables, which are the layer's (`shards`, `shard_key_config`,
  `shard_keys`, `replicated_tables`, `sharded_tables`, and `replication_in_flight` where present);
  and a replicated table's copies on other shards, except that each shard's copy is checked equal to
  the others' and a difference is reported.
- **Its output:** per table, the rows in A only and in B only, each with its identity resolved to
  physical labels; and, for a row of the same lookup key in both whose science columns differ, each
  differing column with both values and their relative difference. Exit 0 when nothing differs, 1
  when something does, 2 when it cannot read a store.
- **The schema snapshot:** each client table's columns (name, type, nullability, primary key) and
  indices, as sets, written as JSON for one store and compared for two. A difference is a finding,
  not a miss: the package orders its prepended columns as `serial`, `version`, `timestamp`,
  `stepping` (contract §2).

It is also the tool for the user's next step: comparing the rebuilt science store with
`~/ChamPBH-stores-bt02/` by value (§7).

### 4.3 Reproducibility, and the criterion it sets

Nothing in the build should depend on the layer: the histories are integrated in one Ray task each,
and the values stored are what the integrator and PRyMordial return. So the expectation is that two
builds of one grid agree **bit for bit**. R0 measures it rather than assuming it: `reference` and
`resumed` compute every history independently, on one tree.
- **If R0's two stores are equal,** R1 and R2 are held to equality with R0's `reference`.
- **If they differ,** the cause is recorded (a candidate is a BBN solve that met
  `--bbn-wall-clock-limit` under load, which stores a failure in one build and a result in the
  other), and the user rules on R1's and R2's criterion before 05–06 are written. No tolerance is
  chosen by an agent.

### 4.4 Rollback

Each prompt is one commit, reverted by `git revert`.
- **The venv is shared**, so reverting 03 or 07 also needs `./venv/bin/pip uninstall -y <package>`.
- **Reverting 04 or 08 alone needs nothing in the venv.** An installed, unimported package is inert.
- **Reverting 08 makes the R2 stores unreadable** by the tree, as every package-written store is to
  ChamPBH's layer. Its rehearsal stores are throwaway (§3).
- **The campaign runs in the main checkout**, on its branch (P17), not a worktree: a worktree has no
  venv of its own.

## 5. The rules this campaign runs under

The project-wide ones in `CLAUDE.md` (invariants 1–7, and "Repository mechanics"), plus:

1. **One commit per prompt.** The commit boundary is the rollback boundary.
2. **Every prompt writes a log** to `logs/NN-<name>.md`, using §5.1, classifying every deviation as
   `STRUCTURALLY REQUIRED`, `IMPLEMENTATION CHOICE` or `UNINTENDED DRIFT`.
3. **Every prompt updates `IMPLEMENTATION_STATE.md` in its own commit,** and
   `.documents/OPEN_ISSUES.md` whenever a §3 or §4 entry moves, in the same commit.
4. **Do not fix things the prompt did not ask for.** Record them, and open a §3 issue.
5. **Commit messages** in `CLAUDE.md`'s form, ending with `Co-Authored-By:` naming the model.
6. **Verification documents are additive.**
7. **Existing tests are not modified**, except where a prompt names the test and the reason: 08's
   harness and its deletions.
8. **Deliberate breakage.** Each prompt names mutations its tests must catch. The log records each
   as a diff, exactly as applied, so that the orchestrator can replay it with `git apply`.
   Mutations are never committed.
9. **No stored science value changes.** §4.2 is the measurement, and a prompt that cannot keep it
   stops.
10. **No test needs Ray, and no test opens a store outside a `tempfile` directory.** A rehearsal's
    reads of its own stores are verification steps, not tests.
11. **A defect in a package is not patched or worked around in ChamPBH.** The prompt stops and
    records it. The user reports it to that package's repository, where it is fixed in a new
    release, and a new pin prompt moves ChamPBH to it. U3 is the first such case, met in planning.
12. **No change is made in DatastoreKit, RayPoolKit, SecondaryGWKit or StochasticInstantons by a
    prompt here.** Their repositories are read, and the two packages' measuring scripts run from their roots with their
    own venvs. The `v0.2.3` release is DatastoreKit's own work, from the brief.
13. **Pins.** Each pin is a tag, in a commit of its own, never an editable install, and ChamPBH
    keeps its own `ray` and `SQLAlchemy` pins.
14. **Rehearsals** follow §4.1, and only 02, 06 and 10 run `main.py` on a store (U2).
15. **Ray** (U5, ruled 2026-10-12). **No prompt runs `ray stop`**, which stops every Ray process on
    the machine. From 05a on, a cluster is started and stopped only by 05a's `ray_cluster.sh`, and
    every driver and `ray` CLI call is given its address. ChamPBH's tests need no Ray (rule 10).
16. **The rehearsal lock** (U5). A rehearsal (06, 10), and 05a's proof, hold
    `~/ScienceData/.rehearsal-lock` from before the cluster starts until after it has stopped
    (the brief's §2). A held lock is a stop, never a wait.

### 5.1 The log template

- the subject, commit and result;
- **What shipped**;
- **Deviations from the prompt**, each classified;
- **Verification performed**, with every count against §0.2's;
- **The deliberate-breakage record**;
- **Observations not acted on**;
- **State handed to the next prompt**.

## 6. Decisions

### 6.1 Ruled by the user (2026-10-11), and not reopened here

| | Decided | Rejected |
|---|---|---|
| **U1** — the order | **RayPoolKit first**: its swap deletes `RayTools/` and rewrites 3 imports, changes no call site, and removes `RayWorkPool.py:23`'s layer import, so the DatastoreKit swap never touches `RayTools/` | DatastoreKit first, as SGK did: SGK's reason (a test edited only to be deleted) has no counterpart here, and U12's exception would be needed |
| **U2** — the rehearsals | **The agent runs a small pilot** (§4.1): reference, killed-and-resumed, and readers, at R0, R1 and R2. A one-off exception to `CLAUDE.md`'s limits, for 02, 06 and 10 | The user runs every build, each rehearsal split around a halt. Straight-through builds only, which leave the package's prune and repair at open unexercised under real Ray |
| **U3** — the factories (reopened, see below) | **Fix the package, not the client.** DatastoreKit releases `v0.2.3`, whose `SQLAFactoryBase` keeps `register` and `build` abstract and gives `store`, `validate` and `validate_on_startup` defaults that raise `NotImplementedError`. ChamPBH keeps its registry of instances and its hooks' `self`, and pins `v0.2.3` (07). The request is [`datastorekit-v0.2.3-brief.md`](datastorekit-v0.2.3-brief.md) | Registering the classes (the first ruling): about 60 hooks made static, and six quantity subclasses, to satisfy a check that protects nothing. A ChamPBH intermediate base, or the three hooks written out on each of the 20 classes: each works around a package defect in the client (§5 rule 11) |
| **U4** — a drop that leaves references | **Refuse before Ray starts, naming the groups to add** (08) | Expanding the request silently, with a notice. Leaving the refusal to `ShardedPool` at open |

**How U3 was reopened.** The user first ruled "register the classes", on a question the planner
had framed as a client-side choice, following DatastoreKit's checklist. The user then asked which
convention had changed, and why. §0.2 records the answer: ChamPBH moved to instances at `5ef07b4`
to parameterise its quantity factories; DatastoreKit inherited SGK's ABC at extraction; the
package's layer is indifferent to classes or instances; and the ABC is enforced only on
instantiation, so it checks nothing for SGK and refuses valid factories for ChamPBH and
StochasticInstantons, while the contract already treats the three hooks as optional. Relaxing it is
backward-compatible: everything that satisfies the `v0.2.2` base satisfies the `v0.2.3` one. The
user reopened U3 and ruled the package fix the same day. DatastoreKit's decision U32 is that
repository's to revisit; the brief asks it to.

### 6.2 Proposed by the planner; ruled by the user 2026-10-11

Accepted as proposed unless a ruling is given beside the proposal.

- **P1 — the rehearsal grid** is the user's 2026-10-02 pilot (§4.1), five histories at M = 10⁻³.
  *Accepted, "if it still exists".* It does: the grid is recorded in `pilot-main.log` (untracked,
  in the working tree), and §4.1 carries the command, so no prompt depends on the log. The pilot was
  the user's end-to-end check of `main.py` and `plot_by_beta.py` on 2026-10-02, after
  `science-readiness` merged (`4ae25b4`) and before the science run of 2026-10-03; its β span the
  threshold region (1.1–1.2) and two super-Planckian starts (10, 25).
- **P2 — the rehearsal stores** live outside the repository, with the `.db` suffix (§3). *Amended
  by the user:* under `~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/`.
- **P3 — comparison by value** (§4.2), by a lasting tool in `tools/`, tested, layer-independent.
  It is also the user's check of the science rebuild (§7).
- **P4 — the criterion** is R0's measured reproducibility (§4.3), bitwise if R0 is bitwise, and
  otherwise the user's ruling.
- **P5 — run provenance in the drivers' output**, ChamPBH's counterpart of SGK's U4: one line,
  never a refusal (01). *Accepted; the user notes that ChamPBH will eventually import SGK's
  `RunRegistry`*, which records the same facts in a run's manifest. So 01 keeps the helper in the
  shape of SGK's `package_provenance()`, the same fields with the same meaning, so that the move is
  a substitution.
- **P6 — `Datastore/__init__.py` and `Datastore/SQL/__init__.py` become empty files**, and the
  factories stay where they are (SGK's U3). Whether a package named `Datastore` that holds no
  datastore is renamed is left for another day. *Accepted; the user will tidy the folder structure
  later.*
- **P7 — the 14 lookup-only pool sites keep `store_handler=None`**, so 04's diff is the imports.
- **P8 — `LambdaCDM` leaves `replicated_tables` and `serial_batch_sizes`** at 08. No factory
  registers it, and the package's read-only open refuses a store whose record names it. Removing
  it earlier would make the old layer refuse every existing store, so it waits for the swap.
- **P9 — `VERSION_SERIAL_KEY` and `require_version_serial` come from `datastorekit.contract`** and
  are deleted from `config/version.py`; `config.version.VERSION_LABEL` stays, and test f, which
  checks it, stays. *Accepted, after §6.3's explanation: `VERSION_LABEL` stays in
  `config/version.py`; only the two layer-owned names move, and they are imported directly.*
- **P10 — no `VERSION_LABEL` bump.** Nothing the code computes changes, and no store crosses the
  swap: the package refuses ChamPBH's, and the old layer would resolve the package's bare shard
  names against the working directory. *Accepted.* The user asked whether rows now carry a version
  serial: they already do, and the lookups are already keyed on it (§6.3).
- **P11 — the plot scripts keep read-write pools.** A read-only pool would refuse a miss
  (`ReadOnlyMiss`, `ReadOnlyWrite`) where today a lookup inserts a parameter row, and finding out
  where that happens is a change of behaviour this campaign does not need. 08 opens an issue for
  it. *Accepted; the user wants the plot scripts on read-only pools eventually*, so 08's issue says
  so, as a goal and not only an observation.
- **P12 — `--inventory` refuses between 08 and 09**, with a message, rather than failing with an
  `AttributeError`.
- **P13 — the BBN refresh route** becomes the package's `sharded_store copy` (08). It is a message
  only; the route itself is the user's (§7).
- **P14 — stale prose.** A statement that a prompt's own commit makes false is corrected in that
  commit: `architecture-summary.md`'s `RayWorkPool` lines (04), `CLAUDE.md`'s harness paragraph
  and the black-clean issue (08). The rewrite of `architecture-summary.md`'s database sections
  and `CLAUDE.md`'s test commands is 11's, once the inventory has landed. Dated `.documents/`
  audits and verification records, and `prompts/`, are left as the record of the tree they
  describe.
- **P15 — the inventory report shows at least what today's does** (09, item 2), and every named
  problem in full. Its layout is new, and nothing parses it.
- **P16 — the gates.** This board records each G3 when it holds (04, 10). Recording G3 on
  DatastoreKit's and RayPoolKit's extraction boards is a records commit in each of those
  repositories: 11 drafts the text, and the user makes the commits, or has a session there make
  them.
- **P17 — the branch.** `dk-and-rpk-migration`, cut from `main`, with the planning commit on it. It
  is merged into `main` at the close, as earlier campaigns were. *Accepted: from `52142d7`, "or
  whatever later commit we end up at".*

### 6.3 Version serials in ChamPBH, for P9 and P10

ChamPBH has stamped rows with a version serial since its start, and has keyed its compute-target
lookups on it since 2026-09-30. Neither comes new from the package.
- **Stamping.** A factory whose `register()` says `"version": True` gets a `version` column with a
  foreign key to the `version` table's `serial`, and every insert carries the serial of the label
  the pool was opened with (`config/version.py`'s `VERSION_LABEL`, `2026.6.0`). ChamPBH inherited
  this from SGK's layer: `ScalarModel` has declared it since `e4dbd13` (2025-12-15), and so do the
  potentials, `ExponentialCoupling`, `AdiabaticHistory` and `BBNData`.
- **Keying.** `run-integrity`'s commit `bacddd8` ("Key the compute-target lookups on the version
  label", 2026-09-30) made `ScalarModel`, `AdiabaticHistory` and `BBNData` declare
  `"key_on_version": True`. The actor then hands each keyed factory's `build` the lookup serial in
  the payload, under the reserved key `VERSION_SERIAL_KEY = "_version_serial"`, and the factory
  filters on it through `require_version_serial(payload, cls_name)`, which refuses rather than look
  up unfiltered. So a row made under another label is a miss. Both names are defined in
  `config/version.py:55-71` today, beside `VERSION_LABEL`, and are imported by ChamPBH's layer
  (`Datastore/SQL/Datastore.py:85`) and the three keyed factories.
- **The direction was ChamPBH to the package.** DatastoreKit took `key_on_version` into `v0.2.0`
  (its extraction prompt 06) because ChamPBH uses it (its adoption README: "CPBH because its
  factories use version-keyed lookups"). SGK's layer did not key lookups. The package's
  `datastorekit.contract` now defines `VERSION_SERIAL_KEY` (the same string) and
  `require_version_serial` (the same behaviour), and its actor is what sets the key.
- **So P9.** After 08 the actor that writes `_version_serial` into the payload is the package's.
  If ChamPBH kept its own definitions, the key would be defined twice, once by the writer and once
  by the readers, and nothing would keep the two in step. Importing both from `datastorekit.contract`
  makes the package the one definition, as the contract intends (contract §8: "its name is the
  layer's, not a client's").
- **The hazard P9 guards against.** `datastorekit.contract` also defines a `VERSION_LABEL`, but it is
  `"label"`, the *name of the version table's label column*. ChamPBH's `config.version.VERSION_LABEL`
  is `"2026.6.0"`, the label every driver opens the store under. A rewrite that moved every
  `config.version` import to `datastorekit.contract` would bind the wrong one, silently. P9 moves
  only the two names; test f already checks that the three drivers take `VERSION_LABEL` from
  `config.version`.
- **The alternative.** `config/version.py` could keep the two names as re-exports
  (`from datastorekit.contract import VERSION_SERIAL_KEY, require_version_serial`), so that the
  factories' imports stay as they are. One definition either way; the planner prefers the direct
  import, which says where the name lives.

### 6.4 Left for the user

- ~~Before 07: the `v0.2.3` release~~ (U3): released 2026-10-11 (§0.3).
- **At the close:**
  - the science-store rebuild (§7), and with it the run scripts;
  - removing the rehearsal stores (§3);
  - the two G3 records (P16);
  - whether the plot scripts should open read-only (P11's issue);
  - whether to rename the `Datastore` package (P6);
  - whether the drivers' `--ray-address` should become required, as SI's are, rather than default
    to `auto` (§6.6, SI's handover).

### 6.5 Ruled by the user 2026-10-12: a spec for every class that holds state

SGK is extracting its `RunRegistry` into a package of its own, RunRegistryKit, which ChamPBH is
expected to adopt after this campaign closes (P5's note). Its store layer fingerprints a store
through `read_inventory`, which reads only the classes that declare `inventory_spec()` and passes
over the rest **silently**. 09's charter covers "each factory whose class the report covers": the
right test for a report, but a fingerprint must not omit a class that holds state. The user ruled
that 09 also proves coverage (acceptance item 5), and that RunRegistryKit's store layer will not
work without a fingerprinter, so this is where ChamPBH earns one. The reasoning is SGK's
`docs/runregistry-extraction-audit.md` §0 (D4, D5) and §7.

**What it costs here.** Measured read-only on 2026-10-12 at `12c82e8`: the registry
(`Datastore/SQL/Datastore.py:94-122`) has 27 entries. 09's list names all 21 owners; the other six
are the tag and value tables of `ScalarModel`, `AdiabaticHistory` and `BBNData`, one of each per
owner. Nothing falls outside a spec. So for ChamPBH the ruling adds a test, not work. It changes
no other charter and no prompt already written, and adopting RunRegistryKit stays out of this
campaign's scope (§1).

### 6.6 Ruled by the user 2026-10-12: isolate the cluster, and share a lock (U5)

On 2026-10-12, SGK's `runregistry-extraction` rehearsal, SI's suites and this campaign were live on
this machine within hours of each other. SI's orchestrator wrote the shared contract,
`rehearsal-lock-brief.md` (`/Users/ds283/Documents/Code/StochasticInstantons/.prompts/dk-and-rpk-migration/`,
revised at `ed9399a` after SI's prompt 01a proved it), and the user handed it to this campaign.

| | Decided | Rejected |
|---|---|---|
| **U5** — other projects' Ray clusters | **Isolate and lock.** ChamPBH's cluster runs on `/tmp/ray-cpbh` and its own ports (GCS 6380, client 6381, dashboard 8267, dashboard agent 52367) and stops only itself, never by `ray stop`. One lock, `~/ScienceData/.rehearsal-lock`, shared with SI and SGK, lets one rehearsal run at a time. ChamPBH adopts SI's `ray_cluster.sh` (05a) | The default cluster and `ray stop`, which stop other projects' builds in flight and collide on ports and on `/tmp/ray/ray_current_cluster` |

**Why each part.**
- **`ray stop` stops every Ray process on the machine.** At ChamPBH's Ray 2.53.0, as at SI's
  2.55.1, it takes no address (`ray stop --help`: `--force`, `--grace-period` and the log options
  only). §4.1 ran it after every rehearsal.
- **Two default clusters collide**: on ports 6379, 10001, 8265 and 52365, and on
  `/tmp/ray/ray_current_cluster`, which `ray.init("auto")`, ChamPBH's drivers' default, follows.
- **Load moves results.** ChamPBH's BBN solve has a wall-clock limit (§4.3), so a rehearsal beside
  another project's build could store a failure where R0 stored a result. The lock keeps
  rehearsals apart.

**What was checked for 05a, read-only, on 2026-10-12.**
- Ray 2.53.0's source agrees with what SI measured on 2.55.1 in three places:
  - `ray start --temp-dir` writes `ray_current_cluster` into that directory
    (`ray/_common/utils.py:237`, called from `ray/scripts/scripts.py:1126`);
  - the client server's command line is `… -m ray.util.client.server --address=<gcs> --host=…
    --port=<client> …` (`ray/_private/services.py:2350-2361`), so SI's seed matches it;
  - `ray start` accepts `--temp-dir`, `--ray-client-server-port`, `--dashboard-port` and
    `--dashboard-agent-listen-port`.
- What is behaviour rather than source is 05a's to measure: `gcs_server` and `SIGTERM`, the actor
  command, and the client server's address.
- **SI's script checks only its GCS port when it stops**, where the brief's revised §3 asks for all
  four. SI's orchestrator checked the other three by hand. 05a's copy checks all four, and the
  difference is SI's to take up, not this campaign's.
- R0's cluster had 10 CPUs (verification §1), so 05a's default is 10, not SI's 4.
- No Ray process was running and none of the eight ports was held.

**SI's handover, after 05a (2026-10-12).** SI's
`rehearsal-lock-handover-2026-10-12.md` (at SI's `5fb4a1d`) reports three changes there, read and
weighed the same day:
- **The brief, amended at SI's `d6d550b`**, forbids `--ray-address auto` for any run that takes
  the lock, a long build as much as a rehearsal. With the clusters isolated, `auto` finds no
  `/tmp/ray/ray_current_cluster` and takes the first GCS address it finds among the running
  raylets, with no warning. §2 of the brief, the lock, is unchanged. **ChamPBH's rehearsals
  already meet it:** §4.1 gives every driver `--ray-address 127.0.0.1:6380` and every `ray` CLI
  call `--address`, and `build.sh` and `run_readers.sh` pass the address through. The long builds
  are the user's (§7).
- **SI took 05a's changes 3 and 4 into its script** (SI's `8e04a7c`), and added a guard that
  `stop_cluster` returns 2 unless given all five arguments. ChamPBH took the guard, and corrected
  its usage text ("the client server on 6380": it listens on 6381, and is found by its
  `--address=127.0.0.1:6380`), in `288b385`. In ChamPBH's script a shorter call already failed,
  aborting a sourcing shell under `set -u`, so the guard changes the failure, not whether it is
  seen. 06 and 10 both run the corrected script.
- **SI made `--ray-address` required** in its drivers, by its user's ruling. **ChamPBH keeps the
  default `auto` for now** (§6.4). Making it required changes the three drivers' interface, which
  no prompt here asks for (§5 rule 4). The risk it removes is small here: a driver of one project
  attaching to another's cluster fails at `ray.init` today, on the Ray or Python version check
  (ChamPBH Ray 2.53.0, SI 2.55.1, SGK 2.43.0), and ChamPBH's tests start no Ray instance that a
  driver could find by mistake.
- Not acted on, with SI: `lsof` run as a user does not see a listener owned by root or another
  user, so the port checks would miss one. Negligible on a single-user machine.

### 6.7 Ruled by the user 2026-10-12: pin `v0.3.0` (U6)

DatastoreKit released `v0.3.0` on 2026-10-12, after this plan fixed `v0.2.3` as the pin, and its
ChamPBH checklist addendum lets ChamPBH pin either. 05's review put the choice to the user, for 07.

| | Decided | Rejected |
|---|---|---|
| **U6** — which DatastoreKit release | **`v0.3.0`** (tag object `bf0a3b2`, commit `2f5417e`; §0.4). 07 pins and installs it; 08–10 run on it | `v0.2.3`, released for this campaign (U3) and carried unchanged by `v0.3.0` |

**Why.** `v0.3.0` contains every change ChamPBH needs from `v0.2.3`, with `factory_base.py`
unchanged, and adds only an optional, keyword-only `timestamp` that ChamPBH neither passes nor
reads (§0.4). Pinning it now means ChamPBH adopts the release SGK and SI will be on, and a later
move to it is not a second pin prompt.

**What it changes.** 07's file name, its pin and its acceptance figures (§2.2); 08's citation of
the base. 07–10 are not yet written. R2's criterion is unchanged: the layer never sets or reads the
new attribute, so nothing stored moves. `datastorekit-v0.2.3-brief.md` and §0.3 stay as written:
they are the record of the release U3 asked for.

## 7. What this campaign hands to the user

- **Every existing ChamPBH store becomes unreadable by the tree** at 08 (D4), the science store
  and the bbn-tolerance refresh copy among them. They stay on disk, readable by any commit before
  08, by `tools/bbn_from_store.py`, and by `tools/compare_stores.py`.
- **The next science run is a full rebuild through the packages**, into a fresh primary. The run
  scripts' store path, their `--inventory` step (now read-only, after the pool closes) and any
  `--drop` (U4) are what change; 11 lists them line by line. The bbn-tolerance refresh is subsumed:
  a fresh build computes BBN on today's small network.
- **If a long build runs on this machine**, it takes the shared rehearsal lock and runs on an
  isolated cluster (U5, §6.6), and every driver in the run scripts is given that cluster's address
  explicitly (`--ray-address 127.0.0.1:6380` on 05a's cluster), never `auto`, as the brief requires
  since SI's `d6d550b`. `full_run_2026.6.0.sh` and its `-refresh` variant pass no `--ray-address`
  today, which is harmless on a machine with one cluster. 11 includes this in its list.
- **The check of that rebuild** is `tools/compare_stores.py` against `~/ChamPBH-stores-bt02/`: the
  same histories, value by value, with any difference named.
- **If `jordan-normalization` is planned next** (its brief is in `prompts/jordan-normalization/`,
  untracked), it changes what the code computes, so the rebuild is best made once, after it.
