# Prompt 09 — port the inventory

**Campaign:** [`README.md`](README.md) · **Board items:** **I1**–**I5** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-12, with prompts 07, 08 and 10, after 06's review (`fa874a5`). The facts below
were measured at `fa874a5`, before 08; 08's log says what moved, and your own measurement governs.
**Gate:**
- 08 landed and reviewed (board §1): the layer is gone, the pools are the package's, `--inventory`
  refuses (P12), and `Datastore/tests/standin_pool.py` and the rebuilt harness exist;
- on branch `dk-and-rpk-migration`, no tracked change in `git status` (the untracked entries listed
  in prompt 01's gate are expected, and are not yours);
- no ChamPBH Ray process is running. You start none.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Opus**. Twenty-one specs, each derived from a factory's lookup, a report
modelled on SGK's, and a coverage proof for the fingerprint to come.

**Read first:**

1. [`README.md`](README.md): §0.1 item 4, §2.2 (**09's charter**, as amended 2026-10-12), §5, and
   §6.2 **P15**, §6.5 (the coverage ruling).
2. Prompt 08's log: the harness, ChamPBH's `standin_pool.py`, and what moved in `main.py`.
3. DatastoreKit **at the tag** (`git -C /Users/ds283/Documents/Code/DatastoreKit show v0.3.0:<path>`;
   read, never edit): `docs/client-contract.md` §4 (the inventory's declarations) and §5;
   `datastorekit/store_inventory.py` (1,045 lines): `Parent` (`:206-264`, polymorphic parents by
   `type_column` and `types`), `ParentSet`, `InventorySpec` (`:292-329`), `Record` (`:178-203`),
   `ClassInventory` (`:371-410`), `StoreInventory` and `resolve` (`:413-496`), the problem kinds
   (module docstring, `:60-70`), `inventory_classes` (`:929-976`) and `read_inventory`
   (`:979-1045`); `datastorekit/store_reader.py` (`open_read_only` and its refusals); the neutral
   client's specs (`datastorekit/tests/client/factories.py`, for example `:1460-1473`).
4. SecondaryGWKit at `/Users/ds283/Documents/Code/SecondaryGWKit` (read, never import):
   `tools/inventory_report.py` (696 lines; `format_inventory_report` `:653-696`, `_class_lines`
   `:566-650`, `_Renderer` `:168-563`), its tests `tools/tests/test_inventory_report.py` (19) and
   `test_inventory_report_labels.py` (19), and `main.py:283-305` (its `--inventory`, before `ray.init`).
5. Today's report, as R1 printed it: `~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/06-readers/1-inventory.log`
   (72 lines; a text log, read it, open no store), and the code that produced it at `fa874a5`
   (`git show fa874a5:main.py`, lines `1196-1344`; the six hooks below).

---

## 1. What is wanted

ChamPBH's `--inventory` went with its layer (08 left a refusal). The package's inventory is
`datastorekit.store_inventory.read_inventory(primary, factories)`: read-only, on a closed store,
with no Ray, over each registered class's `inventory_spec()`. This prompt declares the specs, writes
ChamPBH's report over the result, and puts `main.py --inventory` on it. It also proves that every
registered class is covered, because the fingerprint of SGK's RunRegistryKit, which ChamPBH is
expected to adopt next, reads only what the specs declare (README §6.5).

**Two things today's report did that the new one is not asked to repeat:** today's `--inventory`
opened a read-write pool after `ray.init` (`fa874a5:main.py:95`, `:1348-1363`), so it pruned
unvalidated rows (on by default), applied any `--drop`, and created the store if the path was absent.
The new one reads, and changes nothing.

### 1.1 The facts (at `fa874a5`)

- **The registry** (`config/datastore.py:99-127`): 27 entries, **21 owners** (`version`, `store_tag`,
  `redshift`, `tolerance`, `beta_value`, `M_value`, `Lambda_value`, `temperature`, `phi_value`,
  `pi_value`, the five potentials, `ExponentialCoupling`, `QCD_Cosmology`, `IntegrationSolver`,
  `ScalarModel`, `AdiabaticHistory`, `BBNData`) and **6 tag and value tables**
  (`ScalarModel_tags`, `ScalarModelValue`, `AdiabaticHistory_tags`, `AdiabaticHistoryValue`,
  `BBNData_tags`, `BBNDataValue`). The tag tables register `serial: False`, so they declare no spec
  (contract §4); each table is read through its owner's `tags` or `values`.
- **The lookups, from each factory's `build`**, which fix each spec's key:

  | Class | `build` matches on | Proposed spec |
  |---|---|---|
  | `version`, `store_tag` | `label` | leaves `("label",)` (contract §5 requires `store_tag`'s) |
  | `redshift` | `z` | leaves `("z",)` |
  | `tolerance` | `log10_tol` | leaves `("log10_tol",)` |
  | `beta_value` | `value` | leaves `("value",)` |
  | `M_value`, `Lambda_value` | `value_eV` | leaves `("value_eV",)` |
  | `temperature` | `value_GeV` | leaves `("value_GeV",)` |
  | `phi_value`, `pi_value` | `value_PlanckMass` | leaves `("value_PlanckMass",)` |
  | `InversePowerPotential`, `ExponentialPotential` | `M_serial`, `Lambda_serial`, `n_value` | leaves `("n_value",)`; parents `M` → `M_value`, `Lambda` → `Lambda_value` |
  | `StarobinskyPotential`, `ReclinerPotential`, `ReflectingPotential` | `M_serial`, `Lambda_serial` | parents `M`, `Lambda` |
  | `ExponentialCoupling` (sharded) | `beta_serial` | parents `beta` → `beta_value` |
  | `QCD_Cosmology` | `omega_m`, `omega_cc`, `h`, `f_baryon`, `T_CMB_Kelvin`, `Neff` (not `name`) | those six leaves |
  | `IntegrationSolver` | `label`, `stepping` | leaves `("label", "stepping")`, as SGK's |
  | `ScalarModel` | the three `*_type`/`*_serial` pairs, `T_Jordan_init_serial`, `T_Jordan_stop_serial`, `phi_Einstein_init_serial`, `pi_Einstein_init_serial`, `atol_serial`, `rtol_serial`, `version`; `failure` only when the payload gives it | leaves the three `*_type` columns; parents `version`, polymorphic `cosmology`, `potential`, `coupling`, and `T_init`, `T_stop` → `temperature`, `phi_init` → `phi_value`, `pi_init` → `pi_value`, `atol`, `rtol` → `tolerance`; tags `("ScalarModel_tags", "model_serial")`, values `("ScalarModelValue", "model_serial")`, validated `"validated"` |
  | `AdiabaticHistory` | `model_serial`, `version` | parents `version`, `model` → `ScalarModel`; tags, values (`history_serial`), validated |
  | `BBNData` | `model_serial`, `version`, `failure` (default `False`) | leaves `("failure",)`; parents `version`, `model` → `ScalarModel`; tags, values (`bbn_serial`), validated |

  The polymorphic maps (type value → class) are those of `CosmologyModels/model_ids.py`,
  `CosmologyConcepts/Potentials/model_ids.py` and `CosmologyConcepts/ConformalCouplings/model_ids.py`,
  as `tools/compare_stores.py:172-201` derives them (`QCD_Cosmology` carries
  `XAV_IMPROVED_EOS_IDENTIFIER = 3`; cosmology values 0–2 name no stored class, so a row carrying
  one is an `unresolved-parent` problem, which is right). `inventory_classes` requires a
  polymorphic parent's `type_column` to be a leaf (`store_inventory.py:950-955`). Every sharded
  parent is on its child's shard (each takes its shard key from the coupling's β), so no parent needs
  `cross_shard`; check it from the code, and say so. **These are proposals, measured by reading the
  code; you derive each from its `build` and record it, table by table.**
- **Today's report** (`fa874a5:main.py:1196-1344`; the six hooks `redshift.py:118`,
  `DimensionlessQuantity.py:95`, `DimensionfulQuantity.py:122`, `ScalarModel.py:804`,
  `AdiabaticHistory.py:387`, `BBNData.py:453`; merged by `config/sharding.py:58-103`,
  `inventory_config`):
  - for `redshift`, `beta_value` and the five dimensionful quantities: the count; the timestamp
    range; the values sorted, `.5g`, with the default unit for the dimensionful ones (`GeV`, `eV`,
    `eV`, `PlanckMass`, `PlanckMass`), the first and last ten when there are 20 or more. The stored
    column `value_<unit>` is already in the default unit: today's code multiplies by the unit and
    divides by it again, so **no units object is needed**;
  - for `ScalarModel`, `AdiabaticHistory` and `BBNData`: validated and unvalidated groups, each with
    its count, **its own timestamp range**, its version label(s), and each record by its stored
    `label` (job name and time), ScalarModel's followed by `T_init`, `T_stop`, `phiE_init`,
    `piE_init`, `failure`, BBN's by `failure`. `--show-all` (`config/argument_parser.py:69-74`)
    lifts the truncation of the record lists. **`PRyM_version` was collected and never printed.**

### 1.2 Two points P15 cannot have as written, decided here

- **A timestamp range per validated group.** `ClassInventory` carries one range per class
  (`earliest_timestamp`, `latest_timestamp`), and a `Record` carries no timestamp. The report gives
  the class's range, once. The renderer reads nothing itself (charter item 2), so a per-group range
  is not available; the log says so.
- **Fields that are not identity.** A spec's leaves are the record's key, and the key is what the
  inventory, and the fingerprint after it, treats as identity. P15 asks for `failure` on `ScalarModel`
  (matched by its lookup only when the payload gives it) and `PRyM_version` on `BBNData` (never
  matched). **Declare both as leaves**, as the two fields of P15 that the lookup does not fix,
  each named in the log with its consequence: two rows that differ only there are two records, not a
  `duplicate`. Declare nothing else beyond the lookup. In particular **not the stored `label`**, which
  carries the job name and the time and would make two equal stores differ: the report shows records
  by their physical labels, as P15 says, not by their stored labels.

### 1.3 The items

- **I1 — the specs.** Each of the 21 owners' factories defines `inventory_spec(self)` (an instance
  method; the package reads it by `getattr` on the registry entry) returning an `InventorySpec`, per
  §1.1 and §1.2, derived from its `build` and recorded table by table. The two quantity factories
  return a spec whose leaf is their own value column (`value` or `value_<default unit>`, from
  `self.ObjectType`). `inventory_classes(factories)` accepts the registry.
- **I2 — the report.** A new `tools/inventory_report.py`, `format_inventory_report(inventory,
  db_name, verbose=False) -> str`, renders a `StoreInventory` and reads nothing itself (no store, no
  file; the registry only to learn which leaves are floats, as SGK's does, if needed). It is modelled
  on SGK's and written for ChamPBH: read it, never import it. It shows at least (P15, with §1.2):
  - the store, its shards, and every named problem, **never truncated**, under its class;
  - for each quantity: count, timestamp range, and values in its default unit (`.5g`), the first and
    last ten of 20 or more unless verbose;
  - for `ScalarModel`, `AdiabaticHistory` and `BBNData`: validated and unvalidated counts, the version
    label(s) by group, the class's timestamp range, and each record by its physical labels:
    `T_init`, `T_stop` (GeV), φ\*, π\* (M_P), the potential (its class and M, Λ, n), β, `failure`;
    for `BBNData` also `PRyM_version`; for each, its value-row count. Records are listed in full up
    to a limit, all of them when verbose.

  The charter named a `units` argument; it is not needed (§1.1), and is left out.
- **I3 — `main.py --inventory`** reads the closed store with `read_inventory(args.database,
  factories)` **before `ray.init`**, prints the report, and exits 0. `--show-all` maps to `verbose`.
  `--inventory` with `--drop` is refused (stderr, exit 2). A `RuntimeError` from the reader (a
  missing store, a refused one) prints `!! --inventory: <message>` to stderr and exits 1. 08's refusal
  and its test go.
- **I4 — what the old inventory leaves behind is deleted:** the six `inventory()` hooks and
  `config/sharding.py`'s `inventory_config` (`:58-103`), with any import of it.
- **I5 — coverage, for the fingerprint** (README §6.5, the user's ruling). A test walks
  `config.datastore.factories` and puts every registered class in exactly one of three places: it
  declares a spec; it is a tag or value table read through an owner's spec, the test naming the owner
  and the slot; or it is excluded, by name, in a table in the test with a one-line reason. It fails on
  a class in none of them, and on an exclusion naming a class no longer registered. At `fa874a5`
  all 27 entries fall in the first two, so the exclusion table starts empty.

---

## 2. What to do

### 2.1 Before

1. Record `HEAD` and the three suites (08's counts).
2. Re-read each factory's `build` and `register`, and derive the spec table of §1.1 yourself. Where
   yours differs from §1.1's proposal, yours governs and the log says why.
3. `git grep -n "inventory" -- '*.py' ':!prompts'`: every hit is accounted for by I1–I4. Anything
   else is a stop.

### 2.2 The change

I1–I5. `black` the files you change.

### 2.3 The tests

In `Datastore/tests/` (ChamPBH tests its tools there, as `test_compare_stores.py`), in two new
modules, `test_inventory_specs.py` and `test_inventory_report.py`. Each store is built in a temporary
directory **through 08's stand-in** (`standin_pool`: a real `ShardedPool` on stand-in shards, and the
harness's helpers to insert rows), closed, and then read by `read_inventory`; no Ray.
1. `inventory_classes(factories)` accepts the registry, and orders it.
2. **Each spec resolves**: every leaf, parent column, `type_column`, tag, value and validated column it
   names is a column of the table the package builds (`build_schema`), and every parent's class is
   registered and declares a spec.
3. **A store with one failed and one successful `ScalarModel`, an `AdiabaticHistory` and a `BBNData`
   row**, each with value rows: `read_inventory` reports no problem, and the report renders every
   field of §1.3 I2, each asserted by value (T_init, T_stop, φ\*, π\*, the potential, β, failure,
   PRyM_version, the value counts, the version label, validated and unvalidated counts).
4. **Problems are printed in full**: a store with an orphan value row, and one with a `ScalarModel`
   whose `cosmology_type` is 0 (no stored class): each problem appears in the report whole, however
   long.
5. **The driver**, as subprocesses with a timeout: `main.py --database <absent> --inventory` exits 1
   cleanly, creates nothing and starts no Ray; `--inventory --drop bbn-data` exits 2; on the store of
   test 3, `--inventory` exits 0 and prints the report; `--show-all` gives the verbose form. And by
   `ast`, the `--inventory` branch returns before `ray.init`.
6. **I5's coverage test.**

### 2.4 Verification

1. The suites give 08's counts **minus 08's A8.5 `--inventory` refusal test, plus the new tests**,
   derived by module. Every new test leaves `ray.is_initialized()` `False`.
2. The rehearsal's log of today's report (`06-readers/1-inventory.log`) beside the new report on a
   test store: field by field, what is kept, what is new (`PRyM_version`, value counts, tags,
   problems), and what is not kept (the per-group timestamp ranges, the stored labels), each with
   §1.2's reason. This is the P15 table of the log.
3. `black --check` is clean on every Python file touched or added; the drivers' imports resolve (04's
   `check_imports.py`).

### 2.5 Deliberate breakage (README §5 rule 8)

Each applied, shown to be caught, reverted, and recorded as a diff:
1. One spec's parent removed (`AdiabaticHistory`'s `model`): test 2 or 3 fails.
2. One leaf misnamed (`redshift`'s `z` → `zz`): test 2 fails.
3. The `--inventory` branch moved after `ray.init`: test 5's `ast` check fails.
4. The spec removed from one owner (`tolerance`): I5 fails, and `inventory_classes` refuses.
5. A new registered class with no spec (a scratch entry added to the registry in the test's copy):
   I5 fails, naming it.

---

## 3. Acceptance

1. **I1:** 21 specs; `inventory_classes` accepts the registry; the spec table in the log, each row
   derived from its factory's `build`.
2. **I2–I3:** tests 3–5 pass; `--inventory` reads before Ray, refuses `--drop`, fails cleanly on a
   missing store.
3. **I4:** `git grep -n "def inventory(\|inventory_config"` outside `prompts/` finds nothing.
4. **I5:** the coverage test passes with an empty exclusion table, and its two breakages fail it.
5. The suites give §2.4.1's counts.
6. **The records**, in the same commit: this board's §1 row for 09, items I1–I5 and
   `### 5.14 After prompt 09 (<date>)`, with the P15 table; a §3 entry for anything opened, with
   `.documents/OPEN_ISSUES.md` in the same commit.

---

## 4. Stop conditions — stop and ask

- A factory's lookup cannot be expressed as an `InventorySpec` without changing the factory's `build`.
- `inventory_classes` refuses the registry for a reason the specs cannot fix within §1.2's rule.
- A test store cannot be built without Ray through 08's stand-in.
- §2.1.3 finds an inventory use that I1–I4 does not account for.
- Anything in the package behaves otherwise than its contract says (README §5 rule 11).
- An existing test fails, or a suite's count falls by more than the one test I3 replaces.

---

## 5. What this prompt does not do

- **Files it changes:** the 21 owners' factory modules (the spec, and the six `inventory()` hooks
  deleted), `config/sharding.py` (`inventory_config` deleted), `main.py` (I3), the new
  `tools/inventory_report.py`, the two new test modules, `Datastore/tests/test_datastore_config.py`
  (08's refusal test removed), the log and this board. Nothing else.
- It changes no factory's `register` or `build`, and no stored column.
- It runs no build, starts no Ray and opens no store but temporary ones. The rehearsal is 10's.
- It does not adopt RunRegistryKit (README §1); I5 is what makes that possible later.

---

## 6. The log

`logs/09-port-the-inventory.md`, using README §5.1. It holds the spec table, class by class, with
each row's `build` reference and §1.2's two declared non-lookup leaves; the P15 table of §2.4.2; a
sample report on a test store; the tests by module and the suites beside 08's; and the
deliberate-breakage record as diffs.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`, and made with `git commit -- <paths>`.
