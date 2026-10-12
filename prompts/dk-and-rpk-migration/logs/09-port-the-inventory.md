# Log 09 — port the inventory

**Subject:** Port the datastore inventory to datastorekit's read-only reader
**Commit:** `47cfcfb`, on branch `dk-and-rpk-migration`, on top of `f84c3f0` (code as at `857ec73`)
**Date:** 2026-10-13, by Claude Opus 5.5
**Result:** **COMPLETE.** No §4 stop condition was met.
- I1: 17 `inventory_spec` definitions, one per factory module, serving the 21 owners; each derived
  from its factory's `build` (table below). `inventory_classes(factories)` accepts the registry.
  No factory's `register` or `build` changed.
- I2: `tools/inventory_report.py`, `format_inventory_report(inventory, db_name, verbose=False)`,
  renders a `StoreInventory`; it opens nothing and reads no store.
- I3: `main.py --inventory` reads the closed store with `read_inventory` before `ray.init`, prints
  the report and exits 0; `--show-all` is `verbose`; with `--drop` it exits 2; a reader refusal
  prints `!! --inventory: <message>` and exits 1. 08's refusal and its two tests are gone.
- I4: the six `inventory()` hooks and `config/sharding.py`'s `inventory_config` are deleted. The
  acceptance grep is **not empty** as written: it finds 08's pool-guard test (2 lines, which the
  orchestrator's note says stays) and one line of `.documents/architecture-summary.md` (11's under
  P14); restricted to code it is empty (D1).
- I5: the coverage test walks the registry: 21 classes declare a spec, 6 are read through an
  owner's `tags` or `values`, the exclusion table is empty; both breakages fail it.
- Suites 18 / 114 (61 + 53) / **97** (76 − 2 + 23), all OK. U4's table unchanged; `TestDropPolicy`
  passes unchanged. Five deliberate breakages caught. One §3 issue opened
  (`[09-redshift-lookup-at-zero-takes-abs-of-the-comparison]`).

## What shipped

`git diff --cached --name-only`, as committed:

```text
.documents/OPEN_ISSUES.md
Datastore/SQL/ObjectFactories/AdiabaticHistory.py
Datastore/SQL/ObjectFactories/BBNData.py
Datastore/SQL/ObjectFactories/DimensionfulQuantity.py
Datastore/SQL/ObjectFactories/DimensionlessQuantity.py
Datastore/SQL/ObjectFactories/ExponentialCoupling.py
Datastore/SQL/ObjectFactories/ExponentialPotential.py
Datastore/SQL/ObjectFactories/InversePowerPotential.py
Datastore/SQL/ObjectFactories/QCD_Cosmology.py
Datastore/SQL/ObjectFactories/ReclinerPotential.py
Datastore/SQL/ObjectFactories/ReflectingPotential.py
Datastore/SQL/ObjectFactories/ScalarModel.py
Datastore/SQL/ObjectFactories/Starobinsky.py
Datastore/SQL/ObjectFactories/integration_metadata.py
Datastore/SQL/ObjectFactories/redshift.py
Datastore/SQL/ObjectFactories/store_tag.py
Datastore/SQL/ObjectFactories/tolerance.py
Datastore/SQL/ObjectFactories/version.py
Datastore/tests/test_datastore_config.py
Datastore/tests/test_inventory_report.py
Datastore/tests/test_inventory_specs.py
config/sharding.py
main.py
prompts/dk-and-rpk-migration/IMPLEMENTATION_STATE.md
prompts/dk-and-rpk-migration/logs/09-port-the-inventory.md
tools/inventory_report.py
```

- **The 17 factory modules** (`Datastore/SQL/ObjectFactories/`): each gains `inventory_spec(self)`,
  an instance method placed just before `build`, with a one-line comment naming what `build`
  matches, and the import of `InventorySpec` (and `Parent` where the spec has parents) from
  `datastorekit.store_inventory`. `ScalarModel.py` also imports the type constants of the three
  `model_ids` modules for its polymorphic maps (D5). The six hooks are deleted from
  `redshift.py`, `DimensionlessQuantity.py`, `DimensionfulQuantity.py`, `ScalarModel.py`,
  `AdiabaticHistory.py` and `BBNData.py`, and with them the `from datetime import datetime` of the
  first three, which only the hooks used. Nothing else in any factory moved.
- **`config/sharding.py`**: `inventory_config` (`:57-102`) deleted; it was imported by nothing.
- **`main.py`**: two imports (`read_inventory`, `format_inventory_report`) and the `--inventory`
  branch, in the place of 08's refusal, after the provenance line and before the `--drop` closure
  and `ray.init` (below).
- **`tools/inventory_report.py`** (new, 572 lines).
- **`Datastore/tests/test_inventory_specs.py`** (new, 10 tests) and
  **`Datastore/tests/test_inventory_report.py`** (new, 13 tests).
- **`Datastore/tests/test_datastore_config.py`**: `TestInventoryRefuses` and its two tests deleted;
  the module docstring's sentence on its subprocess tests made true ("One test runs a subprocess
  …: ``import config.datastore`` alone"). Nothing else.
- This log; the board (§1 row, I1–I5, §3, `### 5.14`); `.documents/OPEN_ISSUES.md` (27 open, 4 on
  this board).

`main.py`'s branch (`:93-111`):

```python
# --inventory reads the closed store read-only (datastorekit.store_inventory.read_inventory), before
# Ray starts and before any pool opens: a pool's open would apply --drop, prune unvalidated rows and
# create a store that is not there. It changes nothing, and a path that is not a store is refused by
# the reader (dk-and-rpk-migration prompt 09)
if args.inventory:
    if len(args.drop) > 0:
        print(
            f"!! --inventory: the inventory is read-only and drops nothing; it refuses --drop "
            f"({', '.join(args.drop)}). Run the drop without --inventory",
            file=sys.stderr,
        )
        sys.exit(2)
    try:
        inventory = read_inventory(args.database, factories)
    except RuntimeError as e:
        print(f"!! --inventory: {e}", file=sys.stderr)
        sys.exit(1)
    print(format_inventory_report(inventory, args.database, verbose=args.show_all))
    sys.exit(0)
```

Nothing was changed in DatastoreKit (read at the tag through `git archive v0.3.0` into scratch),
RayPoolKit or SecondaryGWKit (read in place, never imported); nothing in the venv. No Ray was
started. No store was opened but temporary ones built through the stand-in, under `tempfile` or the
session scratchpad; `main.py --inventory` was run on no rehearsal store and on none of the user's
pilot files.

### I1 — the spec table

Every row is derived from the factory's `build` at `47cfcfb` (line numbers are `47cfcfb`'s). "Leaves" and "parents" make the key;
`tags`, `values` and `validated` are read beside it. Every parent named is on the child's shard:
the sharded parents are `ExponentialCoupling` (named by `ScalarModel`) and `ScalarModel` (named by
`AdiabaticHistory` and `BBNData`), and each of those children takes its shard key from the same
coupling's β (`ComputeTargets/ScalarModel.py:1358-1359`, `ComputeTargets/AdiabaticHistory.py:344`,
`:367-368`, `ComputeTargets/BBNData.py:533`, `:588-589`, `CosmologyConcepts/ConformalCouplings/ExponentialCoupling.py:45-46`);
every other parent is in `config.sharding.replicated_tables`, so on every shard. **No parent is
`cross_shard`**, and `test_no_parent_crosses_a_shard` pins it.

| Class | `build` matches (file:line) | Leaves | Parents (field → column → class) | tags / values / validated |
|---|---|---|---|---|
| `version` | `label ==` (`version.py:43`) | `label` | — | — |
| `store_tag` | `label ==` (`store_tag.py:43`); contract §5 requires the `label` leaf | `label` | — | — |
| `redshift` | `z`, relative, or absolute at 0 (`redshift.py:52-61`) | `z` | — | — |
| `tolerance` | `log10_tol` within `DEFAULT_FLOAT_PRECISION` (`tolerance.py:49-53`) | `log10_tol` | — | — |
| `beta_value` | `value`, relative (`DimensionlessQuantity.py:49-62`) | `value` | — | — |
| `M_value`, `Lambda_value`, `temperature`, `phi_value`, `pi_value` | `self.value_col` = `value_<ObjectType.default_unit>`, relative (`DimensionfulQuantity.py:66-82`) | `value_eV`, `value_eV`, `value_GeV`, `value_PlanckMass`, `value_PlanckMass` (`self.value_col`) | — | — |
| `QCD_Cosmology` | `omega_m`, `omega_cc`, `h`, `f_baryon`, `T_CMB_Kelvin`, `Neff`, each within `DEFAULT_FLOAT_PRECISION`; **not `name`** (`QCD_Cosmology.py:61-75`) | the six | — | — |
| `IntegrationSolver` | `label ==` and `stepping >=` (a minimum; `"stepping": "minimum"`) (`integration_metadata.py:48-52`) | `label`, `stepping` (as SGK's) | — | — |
| `InversePowerPotential`, `ExponentialPotential` | `M_serial ==`, `Lambda_serial ==`, `n_value ==` (`InversePowerPotential.py:72-77`, `ExponentialPotential.py:72-77`); `"version": True` but no `key_on_version`, and no `version` in the filter | `n_value` | `M` → `M_serial` → `M_value`; `Lambda` → `Lambda_serial` → `Lambda_value` | — |
| `StarobinskyPotential`, `ReclinerPotential`, `ReflectingPotential` | `M_serial ==`, `Lambda_serial ==` (`Starobinsky.py:69-73`, `ReclinerPotential.py:69-73`, `ReflectingPotential.py:69-73`); `version` as above | — | `M`, `Lambda` as above | — |
| `ExponentialCoupling` (sharded) | `beta_serial ==` (`ExponentialCoupling.py:56-59`); `version` as above | — | `beta` → `beta_serial` → `beta_value` (replicated) | — |
| `ScalarModel` (sharded) | `validated == True`, `version ==`, `cosmology_type`/`_serial`, `potential_type`/`_serial`, `coupling_type`/`_serial`, `T_Jordan_init_serial`, `T_Jordan_stop_serial`, `phi_Einstein_init_serial`, `pi_Einstein_init_serial`, `atol_serial`, `rtol_serial` (`ScalarModel.py:337-352`); `failure ==` only when the payload gives it (`:284`, `:355-357`); each payload tag by a join on `ScalarModel_tags` (`:359-371`) | `cosmology_type`, `potential_type`, `coupling_type`; **`failure`** (§1.2) | `version` → `version` → `version`; `cosmology` → `cosmology_serial`, by `cosmology_type`: {3 `XAV_IMPROVED_EOS_IDENTIFIER`: `QCD_Cosmology`}; `potential` → `potential_serial`, by `potential_type`: {0: `InversePowerPotential`, 1: `ExponentialPotential`, 2: `ReflectingPotential`, 3: `StarobinskyPotential`, 4: `ReclinerPotential`}; `coupling` → `coupling_serial`, by `coupling_type`: {0: `ExponentialCoupling`}; `T_init`, `T_stop` → `T_Jordan_init_serial`, `T_Jordan_stop_serial` → `temperature`; `phi_init` → `phi_Einstein_init_serial` → `phi_value`; `pi_init` → `pi_Einstein_init_serial` → `pi_value`; `atol`, `rtol` → `atol_serial`, `rtol_serial` → `tolerance` | (`ScalarModel_tags`, `model_serial`) / (`ScalarModelValue`, `model_serial`) / `validated` |
| `AdiabaticHistory` (sharded) | `model_serial ==`, `version ==` (`AdiabaticHistory.py:156-159`); tags by join (`:161-174`); **no `validated` filter** | — | `version` → `version`; `model` → `model_serial` → `ScalarModel` | (`AdiabaticHistory_tags`, `history_serial`) / (`AdiabaticHistoryValue`, `history_serial`) / `validated` |
| `BBNData` (sharded) | `model_serial ==`, `version ==` (`BBNData.py:167-170`), `failure ==` (default `False`, `:145`, `:173-174`); tags by join (`:177-189`) | `failure`; **`PRyM_version`** (§1.2) | `version` → `version`; `model` → `model_serial` → `ScalarModel` | (`BBNData_tags`, `bbn_serial`) / (`BBNDataValue`, `bbn_serial`) / `validated` |

Where mine differs from §1.1's proposal: **nowhere in substance.** The key field names are the
proposal's (`M`, `Lambda`, `beta`, `version`, `cosmology`, `potential`, `coupling`, `T_init`,
`T_stop`, `phi_init`, `pi_init`, `atol`, `rtol`, `model`). Three details of the lookups, recorded
and not stops:
- `IntegrationSolver.build` matches `stepping >=`, not equality; the leaf is the stored `stepping`.
- The five potentials and `ExponentialCoupling` register `"version": True`, so their tables carry
  a `version` column, but their lookups do not match on it; by §1.2's rule it is not in their keys.
- `QCD_Cosmology.build` does not match `name`; it is not a leaf.

**§1.2's two declared non-lookup leaves, and their consequence.**
- `ScalarModel.failure`: matched by `build` only when the payload gives it. As a leaf, two
  `ScalarModel` rows that differ only in `failure` are two records, not a `duplicate`.
- `BBNData.PRyM_version`: never matched. As a leaf, two `BBNData` rows for one model, version and
  `failure` that differ only in `PRyM_version` are two records, not a `duplicate`. (`failure`
  itself *is* matched, by default as `False`.) Conversely, two failed rows for one model and version
  with the same PRyMordial version are a `duplicate` (Observation 2).
- The stored `label` (job name and time) is in no key.

**`inventory_classes(factories)`**, measured:

```text
version, store_tag, redshift, tolerance, beta_value, M_value, Lambda_value, temperature,
phi_value, pi_value, QCD_Cosmology, IntegrationSolver, InversePowerPotential,
StarobinskyPotential, ExponentialPotential, ReclinerPotential, ReflectingPotential,
ExponentialCoupling, ScalarModel, AdiabaticHistory, BBNData
```

21 classes; 17 distinct factory classes; the orchestrator's prototype order exactly.

### I2 — the report

`format_inventory_report(inventory, db_name, verbose=False) -> str`. It takes the inventory and
nothing else; its only other input is the registry's schema, built in memory with `build_schema`
to learn which leaves are floats (as SGK's `_schema_tables` does). It opens no file, no engine and
no store. It indexes each class's records by reference digest once, rather than calling
`StoreInventory.resolve` (which scans a class per lookup), so a large store renders in linear time.
Layout: a title (`@@ DATASTORE INVENTORY: <db_name>`, today's heading), the shards and counts, the
problem count; then five categories (metadata; redshifts, tolerances and parameter values;
cosmologies and solvers; potentials and couplings; compute targets), and "Other classes" for any
class no category names. A one-leaf class prints its values; any other class its records by
physical labels; the three compute targets their validated groups. Every problem is printed whole,
`!! <problem>`, under its class. The values and records of a list of 20 or more show the first and
last ten, unless `verbose`.

**A sample report** on a test store (test 3's store, built by `make_sample.py` in scratch through
the tests' own builder; one shard), as `main.py --database <scratch>/sample/clean/store.db
--ray-address 127.0.0.1:9 --inventory` printed it, exit 0, whole:

```text
>> packages: datastorekit 0.3.0 (…@2f5417ea…, requested v0.3.0); raypoolkit 0.2.0 (…@eaef0ce6…, requested v0.2.0)
@@ DATASTORE INVENTORY: <scratch>/sample/clean/store.db
   read-only; 1 shard (#0); 46 records in 21 classes
   problems: none

   -- Datastore metadata
      @@ version (replicated): 1 record
         label = [ 2026.6.0 ]
      @@ store_tag (replicated): 1 record | committed at Mon 12 Oct 2026 16:55:55
         label = [ pilot ]

   -- Redshifts, tolerances and parameter values
      @@ redshift (replicated): 25 records | committed at Mon 12 Oct 2026 16:55:55
         values = [ 0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, ... 5 values not shown (--show-all shows every one) ..., 7.5, 8, 8.5, 9, 9.5, 10, 10.5, 11, 11.5, 12 ]
      @@ tolerance (replicated): 2 records | committed at Mon 12 Oct 2026 16:55:55
         log10_tol = [ -10, -8 ]
      @@ beta_value (replicated): 2 records | committed at Mon 12 Oct 2026 16:55:55
         values = [ 1.1, 25 ]
      @@ temperature (replicated): 2 records | committed at Mon 12 Oct 2026 16:55:55
         values = [ 2.3493e-13 GeV, 20000 GeV ]
      @@ M_value (replicated): 1 record | committed at Mon 12 Oct 2026 16:55:55
         values = [ 2.436e+24 eV ]
      @@ Lambda_value (replicated): 1 record | committed at Mon 12 Oct 2026 16:55:55
         values = [ 0.001 eV ]
      @@ phi_value (replicated): 1 record | committed at Mon 12 Oct 2026 16:55:55
         values = [ 5 PlanckMass ]
      @@ pi_value (replicated): 1 record | committed at Mon 12 Oct 2026 16:55:55
         values = [ 0 PlanckMass ]

   -- Cosmologies and solvers
      @@ QCD_Cosmology (replicated): 1 record | committed at Mon 12 Oct 2026 16:55:55
            - omega_m=0.3111, omega_cc=0.6889, h=0.6766, f_baryon=0.1575, T_CMB_Kelvin=2.7255, Neff=3.046
      @@ IntegrationSolver (replicated): 1 record | committed at Mon 12 Oct 2026 16:55:55
            - label=solve_ivp+RK45, stepping=0

   -- Potentials and couplings
      @@ InversePowerPotential (replicated): 0 records
      @@ StarobinskyPotential (replicated): 0 records
      @@ ExponentialPotential (replicated): 1 record | committed at Mon 12 Oct 2026 16:55:55
            - M=2.436e+24 eV, Lambda=0.001 eV, n_value=1
      @@ ReclinerPotential (replicated): 0 records
      @@ ReflectingPotential (replicated): 0 records
      @@ ExponentialCoupling (sharded): 2 records | committed at Mon 12 Oct 2026 16:55:55
            - beta=1.1
            - beta=25

   -- Compute targets
      @@ ScalarModel (sharded): 2 records: 2 validated, 0 unvalidated; 3 ScalarModelValue rows | committed between Mon 12 Oct 2026 16:59:55 and Mon 12 Oct 2026 17:03:55
         common to every model: cosmology=QCD_Cosmology(omega_m=0.3111, omega_cc=0.6889, h=0.6766, f_baryon=0.1575, T_CMB_Kelvin=2.7255, Neff=3.046), log10_atol=-10, log10_rtol=-8
         validated: 2 records; version = 2026.6.0
            - T_init=20000 GeV, T_stop=2.3493e-13 GeV, phiE_init=5 PlanckMass, piE_init=0 PlanckMass, potential=ExponentialPotential(M=2.436e+24 eV, Lambda=0.001 eV, n_value=1), coupling=ExponentialCoupling(beta=1.1), failure=True  | values: 0
            - T_init=20000 GeV, T_stop=2.3493e-13 GeV, phiE_init=5 PlanckMass, piE_init=0 PlanckMass, potential=ExponentialPotential(M=2.436e+24 eV, Lambda=0.001 eV, n_value=1), coupling=ExponentialCoupling(beta=25), failure=False  tags: pilot  | values: 3
         unvalidated: none
      @@ AdiabaticHistory (sharded): 1 record: 1 validated, 0 unvalidated; 2 AdiabaticHistoryValue rows | committed at Mon 12 Oct 2026 17:05:55
         validated: 1 record; version = 2026.6.0
            - model: {T_init=20000 GeV, T_stop=2.3493e-13 GeV, phiE_init=5 PlanckMass, piE_init=0 PlanckMass, potential=ExponentialPotential(M=2.436e+24 eV, Lambda=0.001 eV, n_value=1), coupling=ExponentialCoupling(beta=25), failure=False, tags=[pilot]}  | values: 2
         unvalidated: none
      @@ BBNData (sharded): 1 record: 0 validated, 1 unvalidated; 4 BBNDataValue rows | committed at Mon 12 Oct 2026 17:06:55
         validated: none
         unvalidated: 1 record; version = 2026.6.0
            - failure=False, PRyM_version=bf24c3d, model: {T_init=20000 GeV, T_stop=2.3493e-13 GeV, phiE_init=5 PlanckMass, piE_init=0 PlanckMass, potential=ExponentialPotential(M=2.436e+24 eV, Lambda=0.001 eV, n_value=1), coupling=ExponentialCoupling(beta=25), failure=False, tags=[pilot]}  | values: 4
```

**The `--show-all` form** of the same store, exit 0, differs in one line (`diff`):

```text
14c14
<          values = [ 0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, ... 5 values not shown (--show-all shows every one) ..., 7.5, 8, 8.5, 9, 9.5, 10, 10.5, 11, 11.5, 12 ]
---
>          values = [ 0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5, 5.5, 6, 6.5, 7, 7.5, 8, 8.5, 9, 9.5, 10, 10.5, 11, 11.5, 12 ]
```

**Problems**, on the store of test 4 (the same rows, plus five `ScalarModelValue` rows naming
`ScalarModel` serials 10¹⁸…10¹⁸ + 4, and a `ScalarModel` with `cosmology_type` 0), exit 0; the
lines that differ from the sample are the problem count and:

```text
@@ DATASTORE INVENTORY: /private/tmp/claude-35086/-Users-ds283-Documents-Code-ChamPBH/f140dedc-e4d0-49ff-88eb-ae2c23d1871e/scratchpad/09/agent/sample/problems/store.db
   problems: 2, each listed in full under its class
         2 problems:
            !! orphan-value: ScalarModel: 5 ScalarModelValue row(s) on shard #0 name 5 ScalarModel row(s) that are not there; e.g. parent serials 1000000000000000000, 1000000000000000001, 1000000000000000002, 1000000000000000003, 1000000000000000004
            !! unresolved-parent: ScalarModel: 1 row(s) on shard #0 reference a parent that cannot be resolved (field(s) cosmology), and are not records; e.g. serials 3
```

### I3 — the three driver outcomes

`<s>` is an empty scratch directory; `127.0.0.1:9` an address nothing listens on.

```text
$ ./venv/bin/python main.py --database <s>/x.db --ray-address 127.0.0.1:9 --inventory
>> packages: datastorekit 0.3.0 (…@2f5417ea…, requested v0.3.0); raypoolkit 0.2.0 (…@eaef0ce6…, requested v0.2.0)
!! --inventory: Cannot read sharded datastore "<s>/x.db": the primary "<s>/x.db" does not exist. Nothing was read or repaired
(exit 1)
$ ./venv/bin/python main.py --database <s>/x.db --ray-address 127.0.0.1:9 --inventory --drop bbn-data
>> packages: …
!! --inventory: the inventory is read-only and drops nothing; it refuses --drop (bbn-data). Run the drop without --inventory
(exit 2)
$ ./venv/bin/python main.py --database <scratch>/sample/clean/store.db --ray-address 127.0.0.1:9 --inventory
>> packages: …
@@ DATASTORE INVENTORY: <scratch>/sample/clean/store.db
… (the sample above)
(exit 0)
```

`<s>` held nothing afterwards; the store directory held the same two files; no Ray process
(`pgrep -fl "raylet|gcs_server"`: none) before or after.

### I4 — what is deleted

`git grep -n "def inventory(\|inventory_config" -- . ':!prompts'` at the staged tree:

```text
.documents/architecture-summary.md:320:| `inventory_config` | Merge policies for cross-shard inventory aggregation |
Datastore/tests/test_datastore_config.py:256:    passes ``drop_tables=``; none passes the old layer's ``drop_actions=`` or ``inventory_config=``.
Datastore/tests/test_datastore_config.py:283:                for arg in ("drop_actions", "inventory_config"):
```

The two test lines are 08's `TestPoolsAreGivenTheConfig`, which asserts that no pool passes
`inventory_config=`; the orchestrator's note says it stays. The document line is
`architecture-summary.md`'s description of the deleted layer, whose database sections are 11's
(P14). Neither file is on §5's list (D1). Restricted to code,
`git grep -n "def inventory(\|inventory_config" -- '*.py' ':!prompts' ':!Datastore/tests/test_datastore_config.py'`
is **empty**, and so is `git grep -n "def inventory("` outside `prompts/`.

### I5 — coverage

`Datastore/tests/test_inventory_specs.py`: `READ_THROUGH` names the six tables by owner and slot
(`ScalarModel_tags` → `ScalarModel.tags`, `ScalarModelValue` → `ScalarModel.values`, and so for
`AdiabaticHistory` and `BBNData`); `EXCLUDED` is `{}`. `coverage_failures(registry, read_through,
excluded)` walks the registry's keys and checks: a spec (`inventory_spec()` returns an
`InventorySpec`), or a read-through whose owner's spec names that table in that slot, or an
exclusion with a reason; exactly one of the three; and every read-through and exclusion still
registered. On the registry: 21 + 6 + 0 = 27, no failure.

## Deviations from the prompt

- **D1 — STRUCTURALLY REQUIRED (I4's grep).** Acceptance 3's `git grep … -- . ':!prompts'` is not
  empty: three lines remain, in two files not on §5's list (above). Emptying it would mean editing
  08's pool-guard test (an existing test, README §5 rule 7, and one the orchestrator's note says
  stays) and `.documents/architecture-summary.md` (11's, P14). The definitions and every use are
  gone; the remaining text names the old argument in order to assert, or describe, its absence.
- **D2 — IMPLEMENTATION CHOICE (units and names in the report).** A dimensionful value is printed
  with its stored column's unit (`GeV`, `eV`, `PlanckMass`), so φ\* and π\* read
  `phiE_init=5 PlanckMass` where today's report printed `5 Mp`; the quantity lists print
  `PlanckMass`, as today's did. Key fields keep their stored names (`n_value`, `log10_tol`, shown
  as `log10_atol`/`log10_rtol` on a model). Floats are `.5g` in both forms; `--show-all` changes
  truncation only, as today.
- **D3 — IMPLEMENTATION CHOICE (more than P15 asks).** The report covers every class of the
  inventory (the note's expectation), shows tags and value counts on every record, and prints a
  `ScalarModel`'s cosmology and tolerances once, on a "common to every model" line, when every
  model shares them, and otherwise on each record's line. Without that, two models that differ
  only there would print as one line. Records are sorted by physical value (β first), not by the
  stored label. `--show-all` also lifts the truncation of the value lists, as I2 says (today's
  `--show-all` lifted it for the record lists only).
- **D4 — IMPLEMENTATION CHOICE (`--drop` with `--inventory`).** The message starts
  `!! --inventory: ` like the reader's refusals, and names the groups given; exit 2.
- **D5 — IMPLEMENTATION CHOICE (the type maps).** `ScalarModel`'s spec builds its three maps from
  the constants of `CosmologyModels/model_ids.py`, `CosmologyConcepts/Potentials/model_ids.py`
  and `CosmologyConcepts/ConformalCouplings/model_ids.py` (three imports added to
  `ScalarModel.py`, which the spec needs), not from literals; `test_the_polymorphic_maps_are_the_model_ids`
  pins them. The maps are those of `tools/compare_stores.py:172-187`.
- **D6 — IMPLEMENTATION CHOICE (tests beyond §2.3's six).** `test_inventory_specs.py` has 10 tests:
  test 1 (the order; and 17 definitions serve 21 classes, each quantity on its own column), test 2
  (columns; referenced classes; the polymorphic maps; no cross-shard parent), test 6 (coverage) and
  three positive controls of `coverage_failures` (a class in none, a stale exclusion, a wrong slot).
  `test_inventory_report.py` has 13: test 3 as seven methods (no problem; the quantities; the
  models; the history and BBN; the other classes; no stored label; verbose), test 4 as one, test 5
  as five (four subprocesses, one `ast`).
- **D7 — IMPLEMENTATION CHOICE (test 3's store).** The failed `ScalarModel` has no value rows, as a
  failed model has none (its line asserts `values: 0`); the successful model, the history and the
  BBN row have 3, 2 and 4. The BBN row is unvalidated, so the store exercises both groups. Rows
  carry fixed timestamps (the actor's `insert_timestamp`), so the timestamp ranges are asserted
  by value. 25 redshifts make the truncation visible.
- **D8 — IMPLEMENTATION CHOICE (`test_datastore_config.py`'s imports).** `import os` and
  `import tempfile` were used only by the deleted class and are now unused. The dispatch allowed
  only the class, its tests and the docstring sentence, so they stay (Observation 4).
- **D9 — UNINTENDED DRIFT (the first "before" run).** I started the suites in the background at
  `f84c3f0` and began the factory edits before the last batch (`ComputeTargets` `test_[g-z]*`,
  started 00:05:15) had finished; that batch had already imported its modules, but the run is not
  a clean "before" for it, and my runner's `tail` lost the `Ran` lines of three batches. I
  re-measured on a `git archive HEAD` copy in scratch with the repository's venv, untouched by any
  edit: that is the "before" below. Nothing else was affected.

## Verification performed

### §2.1 Before

- `HEAD` `f84c3f0` (`git rev-parse HEAD`); `git diff --cached` empty; untracked: the 23 entries of
  dispatch, none mine. No Ray process (`pgrep -fl "raylet|gcs_server|ray::"`: none).
- **The suites at `f84c3f0`** (on `git archive HEAD` in scratch, repository venv):

| Suite | Count | Result | Time | Load at start → end |
|---|---|---|---|---|
| `Datastore/tests` | 76 | `OK` | 6.6 s | 3.83 (00:07) → 4.24 |
| `CosmologyModels/tests` | 18 | `OK` | 66.1 s | 4.24 (00:07) → 3.70 |
| `ComputeTargets/tests`, `test_[a-f]*.py` | 61 | `OK` | 96.9 s | 3.70 (00:08) → 4.43 |
| `ComputeTargets/tests`, `test_[g-z]*.py` | 53 | `OK` | 86.1 s | 4.43 (00:10) → 6.52 (00:11) |

  By module (a `unittest` loader count): Datastore `bbn_failure_lookup` 6, `compare_stores` 21,
  `datastore_config` 16, `first_bounce_round_trip` 5, `fixed_T_values_round_trip` 5,
  `package_provenance` 10, `scalarmodel_failure_reason` 4, `version_keyed_lookups` 9 = 76;
  CosmologyModels 6 + 6 + 6 = 18; ComputeTargets 61 + 53 = 114. Equal to 08's.
- **§2.1.3's grep**, `git grep -n "inventory" -- '*.py' ':!prompts'`: 26 hits, each accounted for,
  none a stop:
  - the six hooks (I4): `AdiabaticHistory.py:387`, `BBNData.py:453`, `DimensionfulQuantity.py:122`,
    `DimensionlessQuantity.py:95`, `ScalarModel.py:804`, `redshift.py:118`;
  - `inventory_config` (I4): `config/sharding.py:57`, imported by nothing;
  - 08's refusal (I3): `main.py:91`, `:93`, `:95`;
  - 08's two refusal tests and the docstring sentence (I3 as amended):
    `test_datastore_config.py:25`, `:441`, `:444`, `:445`, `:457`, `:465`, `:477`, `:483`;
  - 08's pool guard, which stays (the note): `test_datastore_config.py:256`, `:283`;
  - U4 reading the specs: `config/datastore.py:43`, `:171`, `:180`;
  - the help of `--inventory` and `--show-all`: `config/argument_parser.py:64`, `:67`, `:73`; true
    again once I3 lands, and not changed.
- **The spec table** was derived from each `build` before any spec was written (above).

### §2.3 The tests, by module

| Module | Tests |
|---|---|
| `test_inventory_specs` (10) | `TestInventoryClasses`: `test_the_registry_is_accepted_and_ordered` (1), `test_seventeen_definitions_serve_twenty_one_classes`; `TestSpecsResolve`: `test_every_named_column_is_a_column_of_its_table` (2), `test_every_referenced_class_is_registered_and_declares_a_spec` (2), `test_the_polymorphic_maps_are_the_model_ids`, `test_no_parent_crosses_a_shard`; `TestCoverage`: `test_every_registered_class_is_covered` (6), `test_a_class_in_none_of_the_three_is_named`, `test_a_stale_exclusion_is_named`, `test_a_wrong_slot_is_named` |
| `test_inventory_report` (13) | `TestReportOnAStore` (3): `test_no_problem_and_every_class`, `test_the_quantities`, `test_the_scalar_models`, `test_the_history_and_the_bbn_data`, `test_the_other_classes`, `test_the_stored_labels_are_not_shown`, `test_verbose_lists_every_value`; `TestProblemsAreShownWhole` (4): `test_an_orphan_value_row_and_an_unresolved_cosmology`; `TestTheDriver` (5): `test_an_absent_store_exits_1_and_creates_nothing`, `test_drop_is_refused_with_exit_2`, `test_a_store_prints_the_report_and_exits_0`, `test_show_all_prints_the_verbose_report`, `test_the_inventory_branch_returns_before_ray_starts` |

Every store is built in a `tempfile` directory through `_TempStoreCase.open` (a real one-shard
`ShardedPool` through `standin_pool.StandinCluster.open_pool`, closed; the package's undecorated
actor with the shared stand-in broker, notified, `set_version`). Rows are inserted with **no
explicit serial**, so every serial is leased from the stand-in broker: the first tests to use it
(08's Observation 6). The actors are closed before `read_inventory`. The harness's own row helpers
are not used (their parent serials name no rows). Each test class's `tearDown` asserts
`ray.is_initialized()` is `False`. Against vacuity (the note's §0):
- test 3 asserts rendered decimal text against values the test chose (`2.3493e-13 GeV`,
  `2.436e+24 eV`, `beta=25`, `PRyM_version=bf24c3d`, `values: 3`, the timestamp range…), never a key;
- test 4's orphan-value problem is 234 characters (asserted > 200), and each problem's full string,
  as `read_inventory` returned it, is asserted to be one line of the report;
- the coverage test's walk is over the registry's keys, and a read-through entry is checked against
  its owner's spec slot; breakage 5 is the proof;
- the `ast` test asserts exactly one `if args.inventory:` branch calling `read_inventory` and
  `sys.exit`, exactly one `ray.init`, their order, and that the branch ends in `sys.exit(0)`.

### §2.4.1 The suites, after the change

| Suite | 08 | After | Result | Time | Load at start → end |
|---|---|---|---|---|---|
| `Datastore/tests` | 76 | **97** | `OK` | 11.6 s | 5.69 (00:41) → 5.83 |
| `CosmologyModels/tests` | 18 | 18 | `OK` | 64.2 s | 5.83 (00:41) → 4.87 |
| `ComputeTargets/tests`, `test_[a-f]*.py` | 61 | 61 | `OK` | 95.8 s | 4.87 (00:42) → 5.82 |
| `ComputeTargets/tests`, `test_[g-z]*.py` | 53 | 53 | `OK` | 85.1 s | 5.82 (00:44) → 4.77 (00:45) |

**Datastore, derived by module:**

| Module | Before | After | Why |
|---|---|---|---|
| `bbn_failure_lookup` | 6 | 6 | |
| `compare_stores` | 21 | 21 | |
| `datastore_config` | 16 | 14 | − `TestInventoryRefuses` (2), I3 as amended |
| `first_bounce_round_trip` | 5 | 5 | |
| `fixed_T_values_round_trip` | 5 | 5 | |
| `inventory_report` | — | 13 | new |
| `inventory_specs` | — | 10 | new |
| `package_provenance` | 10 | 10 | |
| `scalarmodel_failure_reason` | 4 | 4 | |
| `version_keyed_lookups` | 9 | 9 | |
| **total** | **76** | **97** | 76 − 2 + 23 |

ComputeTargets and CosmologyModels: per-module counts identical to before (`diff` of the loader
counts: empty). No test failed at any point except under a breakage.

**`TestDropPolicy`** (08's U4 tests, 7) passes unchanged. **U4's table did not move:**
`tables_to_drop` over all eight subsets of the three groups, at `f84c3f0` (0 specs) and with the 21
specs, `diff`: empty. `scalar-model` alone is refused with 08's pinned message; with one of the
other two it is refused naming the third; the five closed sets are accepted.

### §2.4.2 The P15 table

Today's report is `~/ScienceData/ChamPBH-rehearsals/dk-and-rpk-migration/06-readers/1-inventory.log`
(R1's, 72 lines, read as text) and the code at `fa874a5:main.py:1196-1344`; the new one is the
sample above.

| Field | Today's | The new report | |
|---|---|---|---|
| How it reads | a read-write pool after `ray.init`: prunes unvalidated rows (default), applies `--drop`, creates an absent store | `read_inventory` on the closed store, read-only, before Ray; refuses `--drop` (2), an absent or refused store (1) | changed (§1) |
| Store and shards | "Opened existing sharded datastore … with 16 shards" (the pool's line) | title with the store; `read-only; N shards (#…); N records in N classes` | kept, and more |
| Problems | — | the count; each problem whole under its class | **new** |
| Redshifts: count, range, values (`.5g`, first/last ten of ≥ 20) | yes | yes; header `N records \| committed between … and …`, same timestamp format; values as before | kept |
| β, T, M, Λ, φ, π: count, range, values with unit (`GeV`, `eV`, `eV`, `PlanckMass`, `PlanckMass`) | yes | yes, same units, `.5g` | kept |
| `version`, `store_tag`, `tolerance`, `QCD_Cosmology`, `IntegrationSolver`, the five potentials, `ExponentialCoupling` | not shown | each with count, range and records/values | **new** |
| ScalarModel / AdiabaticHistory / BBNData: validated and unvalidated groups, counts | yes | yes, and in the header | kept |
| … version label(s) per group | yes | yes | kept |
| … timestamp range per group | yes | **the class's range, once** | not kept: the inventory carries one range per class, records no timestamp (§1.2) |
| … each record by its stored `label` (job name and time) | yes | **not shown** | not kept: the stored label is not identity and is not in the key (§1.2) |
| … `T_init`, `T_stop` (GeV), `phiE_init`, `piE_init` | yes (`Mp`) | yes (`PlanckMass`, D2) | kept |
| … `failure` (models; BBN) | yes | yes (a leaf of each) | kept |
| … the potential (class, M, Λ, n) and the coupling (β) | only inside the stored label text | `potential=ExponentialPotential(M=… eV, Lambda=… eV, n_value=…)`, `coupling=ExponentialCoupling(beta=…)` | **new** as fields |
| … `PRyM_version` (BBN) | collected, never printed | printed on each BBN record | **new** |
| … value-row counts | — | per record and summed in the header | **new** |
| … tags | — | per record (`tags: …`; a history's or BBN row's model's in `tags=[…]`) | **new** |
| … cosmology, tolerances | — | "common to every model" line, or per record where they differ | **new** (D3) |
| An AdiabaticHistory or BBNData record | its own stored label | its model's physical labels | changed |
| Truncation | records: first/last ten of ≥ 20 unless `--show-all`; values: always | both: first/last ten of ≥ 20 unless `--show-all` | kept, and `--show-all` reaches the values |
| Empty group | `no values committed` | `validated: none` | kept |

### §2.4.3 `black`, imports

- `black --check` (26.1.0, no configuration) on the 23 added or modified Python files: "23 files
  would be left unchanged".
- `check_imports.py` (log 04's, unchanged, in scratch), from the root: **165 imported names, 0
  failures**, `ray.is_initialized() = False`, exit 0; 165 = 163 + the two new `main.py` imports:

```text
main.py:46: from datastorekit.store_inventory import read_inventory: ok
main.py:65: from tools.inventory_report import format_inventory_report: ok
checked 165 imported names; failures 0; ray.is_initialized() = False
```

- `./venv/bin/python -m py_compile main.py plot_by_beta.py plot_ScalarModel.py`: exit 0.

## The deliberate-breakage record

Each was applied to the working tree with the whole change staged (working tree equal to the index
before each), shown caught by `Datastore.tests.test_inventory_specs` and
`Datastore.tests.test_inventory_report`, and reverted with `git apply -R`. Each diff below is
exactly the `git diff` of the mutation; `git apply -R --check` passed before each revert and
`git apply --check` on the restored tree after it, and `git diff --quiet` was true (for breakage 3
the `--quiet` check reported the board and index edits I had made meanwhile, unstaged; `main.py`
itself was restored, and they were staged before breakage 4). So each replays with `git apply` from a
clean tree at `47cfcfb`. Nothing was committed.

**Breakage 1 — `AdiabaticHistory`'s `model` parent removed.**

```diff
diff --git a/Datastore/SQL/ObjectFactories/AdiabaticHistory.py b/Datastore/SQL/ObjectFactories/AdiabaticHistory.py
index cdef663..78fcaf0 100644
--- a/Datastore/SQL/ObjectFactories/AdiabaticHistory.py
+++ b/Datastore/SQL/ObjectFactories/AdiabaticHistory.py
@@ -128,7 +128,6 @@ class sqla_AdiabaticHistoryFactory(SQLAFactoryBase):
         return InventorySpec(
             parents={
                 "version": Parent("version", "version"),
-                "model": Parent("model_serial", "ScalarModel"),
             },
             tags=("AdiabaticHistory_tags", "history_serial"),
             values=("AdiabaticHistoryValue", "history_serial"),
```

Caught by test 3, test 4 and test 5: `Ran 23`, `FAILED (failures=2, errors=8)`. The eight errors
are `KeyError: 'model'` in the renderer, where the history's record has no `model` field (all of
`TestReportOnAStore`, whose `setUp` renders, and `TestProblemsAreShownWhole`); the two failures are
the driver's store and `--show-all` tests (exit 1, the same `KeyError`). Test 2 passes: a spec
without a parent names no wrong column, so it is test 3 that sees the history lose its model.

**Breakage 2 — `redshift`'s leaf `z` → `zz`.**

```diff
diff --git a/Datastore/SQL/ObjectFactories/redshift.py b/Datastore/SQL/ObjectFactories/redshift.py
index a6727de..a5be250 100644
--- a/Datastore/SQL/ObjectFactories/redshift.py
+++ b/Datastore/SQL/ObjectFactories/redshift.py
@@ -43,7 +43,7 @@ class sqla_redshift_factory(SQLAFactoryBase):
 
     def inventory_spec(self):
         # build() matches z alone, relatively (dk-and-rpk-migration prompt 09)
-        return InventorySpec(leaves=("z",))
+        return InventorySpec(leaves=("zz",))
 
     def build(self, payload, conn, table, inserter, tables, inserters):
         z = payload["z"]
```

Caught by test 2: `test_every_named_column_is_a_column_of_its_table` fails at `redshift: leaf zz`;
and by tests 3–5 (`read_inventory` raises `KeyError: 'zz'`, the package's contract for a leaf the
table lacks): `Ran 23`, `FAILED (failures=3, errors=8)`.

**Breakage 3 — the `--inventory` branch moved after `ray.init`.**

```diff
diff --git a/main.py b/main.py
index 1f2f1f2..fc96ac9 100644
--- a/main.py
+++ b/main.py
@@ -94,6 +94,19 @@ print(format_package_provenance(installed_package_provenance()), flush=True)
 # Ray starts and before any pool opens: a pool's open would apply --drop, prune unvalidated rows and
 # create a store that is not there. It changes nothing, and a path that is not a store is refused by
 # the reader (dk-and-rpk-migration prompt 09)
+# the --drop groups become the tables the pool drops; a drop that would leave rows naming dropped
+# rows is refused here, before Ray starts (dk-and-rpk-migration prompt 08; README U4)
+specified_drop_actions = [x.lower() for x in args.drop]
+drop_actions = [x for x in specified_drop_actions if x in allowed_drop_actions]
+try:
+    drop_tables = tables_to_drop(drop_actions)
+except ValueError as e:
+    print(f"!! --drop: {e}", file=sys.stderr)
+    sys.exit(2)
+
+# connect to ray cluster on supplied address; defaults to 'auto' meaning a locally running cluster
+ray.init(address=args.ray_address)
+
 if args.inventory:
     if len(args.drop) > 0:
         print(
@@ -110,19 +123,6 @@ if args.inventory:
     print(format_inventory_report(inventory, args.database, verbose=args.show_all))
     sys.exit(0)
 
-# the --drop groups become the tables the pool drops; a drop that would leave rows naming dropped
-# rows is refused here, before Ray starts (dk-and-rpk-migration prompt 08; README U4)
-specified_drop_actions = [x.lower() for x in args.drop]
-drop_actions = [x for x in specified_drop_actions if x in allowed_drop_actions]
-try:
-    drop_tables = tables_to_drop(drop_actions)
-except ValueError as e:
-    print(f"!! --drop: {e}", file=sys.stderr)
-    sys.exit(2)
-
-# connect to ray cluster on supplied address; defaults to 'auto' meaning a locally running cluster
-ray.init(address=args.ray_address)
-
 # instantiate a ProfileAgent to profile database operations; this is passed as an argument to ShardedPool below
 profile_agent = None
 if args.profile_db is not None:
```

Caught by test 5's `ast` check: `test_the_inventory_branch_returns_before_ray_starts` fails
(`39 not less than 38`). `Ran 23`, `FAILED (failures=1, errors=4)`, in 1,201.7 s: the four
subprocess tests now reach `ray.init` on `127.0.0.1:9`, which nothing answers, and each ran to the
subprocess timeout of 300 s (4 × 300 s). The errors' text was lost to my output filter; their
number and the run's length are what was recorded. No Ray process was left (`pgrep`: none).

**Breakage 4 — the spec removed from `tolerance`.**

```diff
diff --git a/Datastore/SQL/ObjectFactories/tolerance.py b/Datastore/SQL/ObjectFactories/tolerance.py
index 6dc3e5c..8c5e77f 100644
--- a/Datastore/SQL/ObjectFactories/tolerance.py
+++ b/Datastore/SQL/ObjectFactories/tolerance.py
@@ -34,10 +34,6 @@ class sqla_tolerance_factory(SQLAFactoryBase):
             "columns": [sqla.Column("log10_tol", sqla.Float(64))],
         }
 
-    def inventory_spec(self):
-        # build() matches log10_tol alone (dk-and-rpk-migration prompt 09)
-        return InventorySpec(leaves=("log10_tol",))
-
     def build(self, payload, conn, table, inserter, tables, inserters):
         log10_tol = payload.get("log10_tol", None)
         if log10_tol is None:
```

Caught by I5: `test_every_registered_class_is_covered` fails with exactly
`"tolerance: declares no inventory_spec, is read through no owner's spec, and is not excluded"`.
`inventory_classes` refuses: `ValueError: inventory_classes(): ScalarModel references tolerance,
which declares no inventory_spec` (test 1 errors with it, and so does every store test, since
`read_inventory` derives the order). `Ran 23`, `FAILED (failures=8, errors=9)`.

**Breakage 5 — a scratch class with no spec in the test's copy of the registry.**

```diff
diff --git a/Datastore/tests/test_inventory_specs.py b/Datastore/tests/test_inventory_specs.py
index 6ca4d5d..2d0f69b 100644
--- a/Datastore/tests/test_inventory_specs.py
+++ b/Datastore/tests/test_inventory_specs.py
@@ -262,6 +262,7 @@ class TestCoverage(_NoRay):
 
     def test_every_registered_class_is_covered(self):
         registry = dict(factories)
+        registry["ScratchTable"] = factories["BBNDataValue"]
         self.assertEqual(coverage_failures(registry, READ_THROUGH, EXCLUDED), [])
         self.assertEqual(EXCLUDED, {})
 
```

Caught by I5, naming it: `Ran 10`, `FAILED (failures=1)`;
`["ScratchTable: declares no inventory_spec, is read through no owner's spec, and is not excluded"] != []`.

## Observations not acted on

1. **`redshift.build`'s `z == 0` branch** filters on
   `sqla.func.abs((table.c.z - z) < DEFAULT_REDSHIFT_PRECISION)` (`redshift.py:53-54`): `abs` of
   the comparison, not of the difference. For `z >= 0`, which is every redshift the code stores, it
   selects the same rows. A factory's `build` is outside 09's scope. **Opened**
   `[09-redshift-lookup-at-zero-takes-abs-of-the-comparison]` (board §3; index).
2. **`BBNData` duplicates by design.** The BBN lookup with `failure=True` returns the newest of
   several failed rows (review-remediation prompt 03, the user's decision), so more than one failed
   row for one model and version is a state the code allows. With the same `PRyM_version` and tags,
   the inventory names them a `duplicate` (all kept). A measurement of what the inventory will say
   about such a store, not a defect; not opened.
3. **`AdiabaticHistory.build` does not filter on `validated`** (`AdiabaticHistory.py:156-159`),
   unlike `ScalarModel.build`. The spec reads the flag beside the key, as the others do, so the
   inventory is unaffected. Recorded with the spec table; not a defect of 09's to judge.
4. **`test_datastore_config.py`'s `import os` and `import tempfile`** are unused since
   `TestInventoryRefuses` went (D8). Housekeeping, for 11 or a standalone commit.
5. **`.documents/architecture-summary.md:320`** still lists `inventory_config` among the layer's
   inputs; 11's under P14, with the database sections (D1).
6. **`config/argument_parser.py`**: the `--inventory` and `--show-all` help are true again; not
   changed (not on §5's list, and needs no change).
7. **Every pre-migration store is refused by the new `--inventory`** (exit 1), as the orchestrator
   measured on a scratch copy of R1's store (README §0.1 item 6): R0's and R1's stores, the user's
   untracked `pilot-2026.6.0.db` and the science store, until each is rebuilt. Not re-measured here:
   no such store was opened.

No §3 issue is opened for 2–7. One issue opened (1).

## State handed to the next prompt

- `HEAD` is `47cfcfb` on `dk-and-rpk-migration`, on top of `f84c3f0`; planning and orchestration
  commits may land after it. No tracked change is left; the untracked entries of dispatch (the
  user's 17 `pilot-2026.6.0*.db` files, `pilot-main.log`, `pilot-plot.log`, `pilot-out/`, the two
  `full_run_2026.6.0*.sh` scripts, `prompts/jordan-normalization/`) are not this campaign's.
- **The specs.** Every one of the 21 owners' factories defines `inventory_spec(self)` (17
  definitions in the 17 modules of `Datastore/SQL/ObjectFactories/`); the six tag and value tables
  declare none and are read through their owners' `tags` and `values`. The keys are the lookups
  of each `build`, plus `ScalarModel.failure` and `BBNData.PRyM_version` (§1.2); no stored `label`
  of a compute target is in a key; no parent is `cross_shard`. `inventory_classes(factories)`
  orders the 21 as this log's I1 gives. U4's `tables_to_drop` reads the specs' parents and its
  table is unchanged.
- **The report.** `tools/inventory_report.py`'s `format_inventory_report(inventory, db_name,
  verbose=False)` renders `read_inventory`'s result and opens nothing; it builds the registry's
  schema in memory to know which leaves are floats.
- **`main.py --inventory`** (`main.py:93-111`) reads the closed store with
  `read_inventory(args.database, factories)` before the `--drop` closure and `ray.init`, prints the
  report (`--show-all` → `verbose`) and exits 0; with `--drop` it exits 2; on a reader refusal
  (`RuntimeError`, e.g. an absent store) it prints `!! --inventory: <message>` and exits 1. It
  creates, prunes and drops nothing, and starts no Ray. **Every store built before 08 is refused**
  (exit 1), so R0's and R1's stores cannot be inventoried by it; R2's (10) can.
- **Gone:** the six `inventory()` hooks, `config/sharding.py`'s `inventory_config`, 08's refusal
  and `TestInventoryRefuses`. Left, by D1: 08's pool guard naming `inventory_config=` in
  `test_datastore_config.py:256`, `:283`, and `.documents/architecture-summary.md:320` (11's).
- **Coverage (I5, README §6.5).** `Datastore/tests/test_inventory_specs.py`'s `READ_THROUGH` (6) and
  `EXCLUDED` (empty) with `coverage_failures`: a class added to the registry without a spec, or a
  read-through or exclusion, fails it by name. RunRegistryKit's fingerprint will read every class
  that holds state.
- **Counts.** CosmologyModels 18, ComputeTargets 114 (61 + 53), **Datastore 97** (by module:
  `bbn_failure_lookup` 6, `compare_stores` 21, `datastore_config` 14, `first_bounce_round_trip` 5,
  `fixed_T_values_round_trip` 5, `inventory_report` 13, `inventory_specs` 10,
  `package_provenance` 10, `scalarmodel_failure_reason` 4, `version_keyed_lookups` 9).
  `check_imports.py`: 165 names, 0 failures. The new module `test_inventory_report` runs `main.py`
  in four subprocesses, a few seconds in all.
- **For 10 (R2):** `--inventory` on R2's stores is the readers' natural check: it should report no
  problem, 21 classes, and the pilot's five models, histories and BBN rows in the validated groups.
  R2's stores are the first real ones it will read.
- **For 11:** Observations 4 and 5 (two unused imports in `test_datastore_config.py`;
  `architecture-summary.md:320`), with 08's list.
- **Issues.** `[09-redshift-lookup-at-zero-takes-abs-of-the-comparison]` opened (board §3). Index:
  27 open, 4 on this board.
- **The venv** is as 07 left it; nothing installed. No Ray started; no store opened but temporary
  ones built through the stand-in. DatastoreKit, RayPoolKit and SecondaryGWKit untouched.
- **Rollback** is `git revert` of `47cfcfb` alone; nothing in the venv (README §4.4). It restores
  08's refusal.
