# Log 01 — strip to the exponential model

**Subject:** Strip the tree to the exponential potential and coupling; pin a golden history; fingerprint format 2
**Commit:** this commit, on branch `jordan-normalization`, on top of `701ed4c`
**Date:** 2026-10-20, by Claude Sonnet 5.5
**Result:** **COMPLETE.** X1–X5 shipped. The golden history was generated on the unmodified tree
(code tree `ac187c8`'s), passed twice there, and passes unchanged after the strip. CosmologyModels
18, ObjectFactories 78, tools 84 and RunDrivers 61 unchanged; ComputeTargets 176 → 179 (+3, the
golden history's tests). All three deliberate breakages caught. No §4 stop condition was met. No
issue opened; `[00-declare-reflects-at-origin-for-the-other-potentials]` closed (moot).

## What shipped

- **X1 — the golden history.** On the unmodified tree, before anything was deleted:
  - `ComputeTargets/tests/golden_history.py` (not a test module): the setup (`QCD_Cosmology` on
    `Planck2018` in `Planck_units`; `ExponentialPotential(M = 1e-3 M_P, Λ = 1e-3 eV, n = 1)`;
    `ExponentialCoupling(β)`, β = 1.5 and 5.5; φ* = 5 M_P, π* = 0, T* = 2e4 GeV, T_stop = 1e-3 GeV;
    atol = rtol = 1e-8; main.py's z grid, 21250 points to 1 + z = 1e85), `run_history`,
    `record_of`, `first_difference`, and a `__main__` that writes the JSON.
  - `ComputeTargets/tests/data/golden_history_01.json`, 904 lines, SHA-256
    `6898a432fb8f7af3b3ceddd64cf54add07528c73317aca311fe3e2aa3ddb62d6`, generated at `701ed4c`
    (the JSON says so, and that `git diff --stat ac187c8 HEAD -- . ':!prompts' ':!.documents'` was
    empty, so the code is `ac187c8`'s), on `macOS-27.0.1-arm64`, Python 3.13.16, NumPy 2.3.5,
    SciPy 1.17.0. Regenerate with
    `PYTHONPATH=. ./venv/bin/python -m ComputeTargets.tests.golden_history`.
  - `ComputeTargets/tests/test_golden_history.py`, 3 tests: `test_history_beta_1_5`,
    `test_history_beta_5_5` (each recomputes the history once and compares every pinned item
    exactly), and `test_the_record_holds_exactly_the_two_histories_and_their_excerpts` (the file's
    own shape: both histories, an excerpt every 250th sample and the last, `SampleValues`' field
    names in order).
- **X2 — the classes.** `git rm` of `CosmologyConcepts/Potentials/{InversePower,Starobinsky,Recliner,Reflecting}Potential.py`,
  `ObjectFactories/{InversePowerPotential,Starobinsky,ReclinerPotential,ReflectingPotential}.py`,
  `starobinsky.yaml` and `recliner.yaml`; their exports in `CosmologyConcepts/Potentials/__init__.py`
  (including `INVERSE_POWER_POTENTIAL`) and their ids in `model_ids.py` (`EXPONENTIAL_POTENTIAL = 1`
  kept).
- **X3 — the registries.** `config/datastore.py` (imports, `factories`, `serial_batch_sizes`),
  `config/sharding.py`, `ObjectFactories/ScalarModel.py` (the imports and the `potential` parent's
  `types`, which keeps only `EXPONENTIAL_POTENTIAL`), `config/argument_parser.py` and
  `plot_by_beta.py` (`potential_types = ["Exponential"]`), `main.py`'s `convert_to_potential` and
  `plot_inputs.py`'s `POTENTIAL_CLASSES` (one case; an unknown type still raises `ValueError`),
  `tools/compare_stores.py` and `tools/inventory_report.py`. The registry goes from 27 classes to
  23 (`len(config.datastore.factories)`), of which 17 declare a spec, from 13 factory types.
- **X4 — the tests**, listed under "Tests removed, renamed or changed" below.
- **X5 — format 2.** `FINGERPRINT_FORMAT = 2` with its history line; `SPEC_DIGESTS[2]` of **17 pairs
  and 14 distinct digests**, pasted from `spec_digests(factories)`, never typed (`len(set(d for _, d
  in SPEC_DIGESTS[2]))` is 14; format 1's is 21 and 15); format 1's pin and comment are untouched;
  `ObjectFactories/tests/data/store_fingerprint_jordan-normalization-01.json` beside format 1's
  `store_fingerprint_rrk-adoption-02.json` (SHA-256
  `7e68c34837e5c1c93cbf21173d50eb5c1acebf033354e5824ffb7c5bdfa773ae`; digest
  `6f7deb85a3a6053fbd8dc773b4a6430deb36a43dba4f7e9be692b138bcc4c252`; `fingerprint_format` 2,
  `encoding_format` 1, `taken` null, `problems` `{}`; counts `ExponentialPotential` 2, `ScalarModel`
  2, `redshift` 5, `temperature` 2, `tolerance` 2, every other class 1), written once from
  `fingerprinter.take(primary, None)` as `json.dumps(fp, indent=2, sort_keys=True) + "\n"`, by a
  probe in the scratch directory that asserts the file does not exist. Of the 17 specs only
  `ScalarModel`'s differs from format 1's.
- **The records.** The board (row 01, gate "Strip", §3, §3.1, §4, §5.2);
  `integrator-remediation`'s board (the entry moved from §3 to §4 with a **Resolved** line);
  `.documents/OPEN_ISSUES.md` (the row left §1.15; **28 → 27 open**; the `integrator-remediation`
  clause 9 → 8); `.documents/architecture-summary.md` (trees, registry table, `--potential-type`,
  replicated tables, §8.3's list, with one dated note); `.documents/numerical-strategies.md` §2.2
  (one dated note, the paragraph untouched).

No Ray was started, no driver run, no store opened but the tests' temporary ones; nothing was
written under `var/`.

## Tests removed, renamed or changed

No test method was deleted. One was renamed. Every test whose input or expectation moved is here.

| Test | What it asserted | Change |
|---|---|---|
| `ObjectFactories/tests/test_inventory_specs.py::TestInventoryClasses::test_seventeen_definitions_serve_twenty_one_classes` | 21 registered classes declare a spec, served by 17 factory types; the six quantity entries share two factory classes | **Renamed** `test_thirteen_definitions_serve_seventeen_classes`; asserts 17 and 13 (note ruling 7). The quantity-factory half is unchanged |
| same file, `test_the_registry_is_accepted_and_ordered` | `inventory_classes(factories) == EXPECTED_ORDER` | `EXPECTED_ORDER` loses the four potentials (21 → 17 entries) |
| same file, `test_the_polymorphic_maps_are_the_model_ids` | the `potential` parent's `types` equal the five ids of `model_ids` | the expected map is `{EXPONENTIAL_POTENTIAL: "ExponentialPotential"}` |
| `ObjectFactories/tests/test_datastore_config.py::TestConfigDatastoreData::test_factories_registry` | the registry's names, factory types and quantity types equal `EXPECTED_FACTORIES` | the four potentials leave the literal (27 → 23 entries) |
| same file, `test_serial_batch_sizes_literal_and_ordered` | `serial_batch_sizes` equals `EXPECTED_SERIAL_BATCH_SIZES`, in order | the four leave the literal |
| `ObjectFactories/tests/test_fingerprint_format_guard.py::TestFormatGuard::test_the_pin_has_one_pair_per_class_with_a_spec` | the current format's pin has 21 pairs, in `inventory_specs`' order | 17 (note ruling 4); the module docstring's counts follow |
| `ObjectFactories/tests/test_store_fingerprint_golden.py` (builder and constants) | the golden store has five `ScalarModel` rows, one per potential type | two `ExponentialPotential` rows (n = 1, 2) and two `ScalarModel` rows, the first tagged; `GOLDEN` names the new file (ruling 6) |
| same file, `TestGolden::test_the_store_reads_with_no_problem_and_every_class` | no problems; 21 classes; every class has a row; `ScalarModel` count 5; each potential class count 1 | 17 classes; `ScalarModel` 2; `ExponentialPotential` 2 |
| same file, `TestCommandLine::test_store_fingerprint` | the command prints the golden's digest and format, and format is 1 | the format asserted is 2 |
| same file, `TestGolden::test_the_golden_fingerprint`, `test_two_tag_sets_are_read` | the store's fingerprint equals the golden, twice; two tag sets | **code unchanged**; they read the new `GOLDEN` and pass |
| `ComputeTargets/tests/test_plot_inputs.py::...::test_the_potentials` | each `--potential-type` looks up its class with its payload (with or without `n`) | keeps the `Exponential` row; gains three subtests (`InversePower`, `Starobinsky`, `Recliner`) asserting `plot_inputs.potentials` raises `ValueError("Unknown potential type: <name>")` and looks nothing up (ruling 4) |
| `RunDrivers/tests/test_registered_run.py::...::test_more_than_six_values_read_as_a_count_and_a_range` | more than six β values read as a count and a range in the scope | `--potential-type Exponential`, and the scope reads `potential Exponential`; what it tests is unchanged (ruling 4) |
| `tools/tests/test_inventory_report.py::TestReportOnAStore::test_no_problem_and_every_class` | no problems; 21 classes; the line `46 records in 21 classes` | 17 classes; `46 records in 17 classes` (the 46 records are unchanged: the four removed classes had no rows in that store). Not named in the note; ruling 7's "any other test whose literal count moves with the registry" |

`tools/tests/test_compare_stores.py` (the map and the unstored set together equal the ids
`model_ids` defines) and `ComputeTargets/tests/test_kinematic_cap_loop.py`
(`NonReflectingExponentialPotential`) pass **unchanged**.

## Deviations from the prompt

Every ruling cited is the orchestrator's note for prompt 01, §0.

1. **`IMPLEMENTATION CHOICE` (ruling 1): the golden history's setup.** Exactly as the note rules:
   `Planck_units()`, `Planck2018()`, `QCD_Cosmology(0, …)`; `M_value(0, 1e-3 * PlanckMass)`,
   `Lambda_value(0, 1e-3 * eV)`, `n = 1`; `temperature(0, 2e4 * GeV)` and `temperature(1, 1e-3 *
   GeV)`; `phi_value(0, 5 * PlanckMass)`, `pi_value(0, 0.0)`; atol and rtol passed explicitly. The
   grid is main.py's arithmetic unchanged: `num = int(round(250 * 85 + 0.5, 0))` = 21250, `np.logspace(0,
   85, num) - 1`, each value `redshift(i, float(z))`. Both histories succeed (a failure payload
   raises).
2. **`IMPLEMENTATION CHOICE` (ruling 2): what is pinned.** As the note rules, with the four wall-clock
   `metadata` times excluded and named as excluded, with the reason, in the JSON and the generator's
   docstring. Excerpts: indices 0, 250, … and the last, each with z and its 17 fields by name as
   `float.hex`: **12** for β = 1.5 (2722 samples), **21** for β = 5.5 (4894 samples). Provenance:
   the generating commit, the code-tree statement, the command, the environment.
3. **`IMPLEMENTATION CHOICE` (ruling 3): the generator and the test**, as the note rules. On a
   mismatch the test names the first excerpted sample and field that differ with both hex values
   (or says the difference lies between excerpts), prints the recorded and current environment,
   never skips. The third test (the file's own shape) is mine, beyond the recipe.
4. **`STRUCTURALLY REQUIRED` (ruling 4): the prompt's §5 list is incomplete.** Also changed, as the
   note lists: `plot_by_beta.py` (`potential_types`, whose only reader is nothing, see
   Observations); `tools/compare_stores.py` (the potential map keeps `1: "ExponentialPotential"`;
   the four ids are **not** added to `UNSTORED_TYPE_VALUES`); `tools/inventory_report.py`;
   `RunDrivers/tests/test_registered_run.py`; `ComputeTargets/tests/test_plot_inputs.py`;
   `ObjectFactories/tests/test_fingerprint_format_guard.py`; the `INVERSE_POWER_POTENTIAL`
   re-export; `.documents/numerical-strategies.md` §2.2 (a dated note after the paragraph).
5. **`STRUCTURALLY REQUIRED` (ruling 5): what the closing `grep` may still find.** See "The closing
   grep" below. Two things to say about it. (a) **The note's command as written does not work on
   this machine:** `git grep -E` here is BSD ERE, which does not read `\b` as a word boundary, so
   it found only the `INVERSE_POWER`-style names and missed every `…Potential` name (before the
   strip it did not list `config/datastore.py` at all). I ran it with `-P`, which does. (b) Two hits
   are mine and are required: `config/fingerprint.py`'s format-2 comment lines (the note's ruling 8
   asks for them) and `test_plot_inputs.py`'s three refused names (ruling 4).
6. **`IMPLEMENTATION CHOICE` (ruling 6): the golden store.** Two `ExponentialPotential` rows,
   `n_value` 1 and 2, each with its own `ScalarModel`, the first tagged, value rows on the first;
   `LABEL` and `T0` unchanged; `GOLDEN` names the new file, written once; format 1's file stays and
   is not edited. `RunDrivers/tests/test_registered_run_fingerprint.py` imports `GOLDEN` and
   `_GoldenStoreCase` and was **not edited**; it passes.
7. **`STRUCTURALLY REQUIRED` (ruling 7): the registry's counts.** The rename and the counts in the
   table above. **Two files beyond the note's list** carry a literal that moves with the registry
   and were changed under ruling 7's last sentence: `ObjectFactories/tests/test_datastore_config.py`
   (the registry and batch-size literals) and `tools/tests/test_inventory_report.py` (21 → 17
   classes). The second was found by the suite, not by `grep`: it names no deleted class.
8. **`STRUCTURALLY REQUIRED` (ruling 8): the records**, as listed under "What shipped". Three small
   additions to what the note spells out, so that the text stays true: `OPEN_ISSUES.md`'s header
   clause for `jordan-normalization` says one of the eight was closed, leaving seven;
   `integrator-remediation`'s §3 gets a one-line italic pointer to the move; and `OPEN_ISSUES.md` §1.6's
   "Nine are open" and "Two of the nine were assigned" sentences say eight and that one of the two
   closed. The board's "Written?" column is left as the planner set it.

No `UNINTENDED DRIFT`.

## Verification performed

Load is the one-minute `uptime` figure at the start of each run. Output files are in the scratch
directory `agent01/` (`before_*.txt`, `after_*.txt`); wall time is `time -p`'s.

**The suites**, serially, in the foreground (ComputeTargets in its two halves):

| Suite | Before (`701ed4c`, code = `ac187c8`) | After | Change |
|---|---|---|---|
| CosmologyModels | 18 OK, 80.6 s, load 46→15 | 18 OK, 77.9 s, load 11 | none |
| ComputeTargets `test_[a-f]*` | 61 OK, 116.5 s, load 8 | 61 OK, 115.1 s, load 3.6 | none |
| ComputeTargets `test_[g-z]*` | 115 OK, 126.2 s, load 4 | **118 OK**, 140.1 s, load 2.6 | **+3**: `test_golden_history` (X1) |
| ObjectFactories | 78 OK, 9.7 s, load 13 | 78 OK, 8.9 s, load 16.6 | none: no test added or removed (one renamed; X4's edits are in test data and expectations) |
| tools | 84 OK, 17.5 s, load 11 | 84 OK, 17 s (second run, after the one failure below) | none |
| RunDrivers | 61 OK, 22.7 s, load 8 | 61 OK, 22 s, load 12.5 | none |

(Before and after counts: 18 / 176 / 78 / 84 / 61 and 18 / 179 / 78 / 84 / 61.) The first run of
the tools suite after the strip had **one failure**, `test_inventory_report.TestReportOnAStore.test_no_problem_and_every_class`
(`17 != 21`), the literal named under Deviation 7; it was a real consequence of the strip and was
fixed there, and the suite then passed. No count fell. `ls -d var/runs/*` finds nothing: nothing was
written to `var/`.

**The golden history on the old tree (X1).** The orchestrator's SHAs and mine, compared before the JSON
was kept:

| | β = 1.5 | β = 5.5 |
|---|---|---|
| samples SHA-256 | `0d2c641c7fc774cae24f8fac18cbae473343a283378df9d44853a724c3f72857` **equal** | `29a5cd4364f1b29fc78edb865c29f605204740a381c82f8e2d854690f9019dc8` **equal** |
| z SHA-256 | `3802cc774fbd2b63547c497c3ce9271ff120694b1ae3f3b640245fc06d35d570` **equal** | `ecd92935903e034055e4b125ddb3b344355f684320f2e7d86ad536582ac77f10` **equal** |
| samples | 2722 | 4894 |
| accepted steps / RHS evaluations | 1231 / 9887 | 26343 / 232810 |
| reflections | 0 | 0 |
| first bounce | N = `0x1.2a4db4587ad6cp+4`, not reflected | N = `0x1.00dc79d22755bp+5`, not reflected |

The generator wrote the JSON in 11.3 s (both histories), at load 5. The golden test then passed
**twice in a row on the old tree** (before any deletion): 3 tests, 11.53 s (wall 13.5 s) at load 5,
and 11.40 s (wall 12.7 s) at load 10. So three runs of the code agree bit for bit (the generation
and two tests), as the note's probe found. After the strip it passes **unchanged** (the JSON is
not regenerated; its SHA-256 is as above): 11.05 s (wall 12.8 s) at load 15, and in the `[g-z]`
half of the suite. Well inside the minute.

**The format-2 pin.** `len(SPEC_DIGESTS[2])` = 17, `len(set(d for _, d in SPEC_DIGESTS[2]))` = 14
(format 1: 21 and 15). The distinct digests fall by the one that `StarobinskyPotential`,
`ReclinerPotential` and `ReflectingPotential` shared; `ExponentialPotential` keeps its digest
(`b468e71a…`, still equal to nothing else, since `InversePowerPotential` is gone) and `ScalarModel`'s
changes (`9c09dfcf…` → `bc113078…`). Every other pair is equal to format 1's.

### The closing grep

The note's command, run with `-P` (see Deviation 5):

```
$ git grep -nP '\b(InversePower|Starobinsky|Recliner|Reflecting)|INVERSE_POWER|STAROBINSKY|RECLINER|REFLECTING_POTENTIAL|starobinsky\.yaml|recliner\.yaml' -- . ':!prompts' ':!science-runs' ':!.documents'
ComputeTargets/tests/test_plot_inputs.py:467:        for potential_type in ("InversePower", "Starobinsky", "Recliner"):
ObjectFactories/tests/data/store_fingerprint_rrk-adoption-02.json:58:    "InversePowerPotential": {
ObjectFactories/tests/data/store_fingerprint_rrk-adoption-02.json:102:    "ReclinerPotential": {
ObjectFactories/tests/data/store_fingerprint_rrk-adoption-02.json:113:    "ReflectingPotential": {
ObjectFactories/tests/data/store_fingerprint_rrk-adoption-02.json:142:    "StarobinskyPotential": {
config/fingerprint.py:59:# registry loses InversePowerPotential, StarobinskyPotential, ReclinerPotential and
config/fingerprint.py:60:# ReflectingPotential, and ScalarModel's spec loses their ids from the ``potential`` parent's types.
config/fingerprint.py:71:# ``InversePowerPotential`` and ``ExponentialPotential``, and ``StarobinskyPotential``,
config/fingerprint.py:72:# ``ReclinerPotential`` and ``ReflectingPotential``. Hence a list of pairs and not a map from
config/fingerprint.py:76:# distinct digests: format 1's 15, less the one that ``StarobinskyPotential``, ``ReclinerPotential``
config/fingerprint.py:77:# and ``ReflectingPotential`` shared; ``ExponentialPotential`` keeps its own, and ``ScalarModel``'s
config/fingerprint.py:116:            "InversePowerPotential",
config/fingerprint.py:120:            "StarobinskyPotential",
config/fingerprint.py:128:            "ReclinerPotential",
config/fingerprint.py:132:            "ReflectingPotential",
```

Each hit is one of: **format 1's pin and comment** in `config/fingerprint.py` (lines 71–72 and
116–132, untouched), **the format-2 comment lines** that say what the strip removed (59–60, 76–77),
**format 1's golden file**, never edited, and **the three refused names** in `test_plot_inputs.py`.
The same pattern without the word boundary, minus `NonReflecting…` (`test_kinematic_cap_loop.py`,
a subclass of `ExponentialPotential` that is not one of the four), finds exactly the same lines.

`black --check` on every changed or added Python file (21 files): all unchanged.

## The deliberate-breakage record

Each is a diff against the tree of this commit, written with `diff -u` from a copy taken before the
mutation, so that `git apply` replays it from a clean tree at this commit; each was reverted
(`cp` back) and never committed. Checked, after the commit, with `git apply --check` and
`git apply -R --check` in `bash`, from the repository root (the result is in the "State" section).

**1. One bit of one sample changed.** The `phi_Einstein` of the β = 1.5 history's excerpt at index
250 multiplied by `1 + 2⁻⁵²`: `0x1.3ffe1997f6be2p+2` → `0x1.3ffe1997f6be3p+2` (seen to change).
`test_golden_history`: **`test_history_beta_1_5` FAILED**, naming the field, the other two passed:
`sample 250: field phi_Einstein differs: recorded 0x1.3ffe1997f6be3p+2, now 0x1.3ffe1997f6be2p+2`
(followed by the recorded and current environment and the regeneration command).

```diff
--- a/ComputeTargets/tests/data/golden_history_01.json
+++ b/ComputeTargets/tests/data/golden_history_01.json
@@ -46,7 +46,7 @@
             "log_fm": "-0x1.cb07091808480p+4",
             "log_rhorad_Einstein": "-0x1.a5c6ff61db71cp+6",
             "log_rhorad_Jordan": "-0x1.0ee3247d6bfc8p+7",
-            "phi_Einstein": "0x1.3ffe1997f6be2p+2",
+            "phi_Einstein": "0x1.3ffe1997f6be3p+2",
             "pi_Einstein": "-0x1.f120c68d47eb2p-13",
             "raw_N": "0x1.27d8a4e789db0p+1",
             "reflecting_term": "0x1.41dadbff660fep-265"
```

**2. The format left at 1 after X3** (`FINGERPRINT_FORMAT = 1`). Four failures in
`test_fingerprint_format_guard` and `test_store_fingerprint_golden`:
`test_every_spec_is_pinned_for_the_current_format` ("the inventory specs no longer match the pin for
format 1: spec changed: ScalarModel; class removed from the registry or its spec:
InversePowerPotential, StarobinskyPotential, ReclinerPotential, ReflectingPotential"),
`test_the_pin_has_one_pair_per_class_with_a_spec` (`21 != 17`), `TestGolden.test_the_golden_fingerprint`
(`2 != 1`, the golden's format against the config's) and `TestCommandLine.test_store_fingerprint`
(the digest line).

```diff
--- a/config/fingerprint.py
+++ b/config/fingerprint.py
@@ -58,7 +58,7 @@
 # Format 2: jordan-normalization prompt 01, the four non-exponential potentials removed. The
 # registry loses InversePowerPotential, StarobinskyPotential, ReclinerPotential and
 # ReflectingPotential, and ScalarModel's spec loses their ids from the ``potential`` parent's types.
-FINGERPRINT_FORMAT = 2
+FINGERPRINT_FORMAT = 1
 
 # The spec pin (campaign README P9; RunRegistryKit's docs/adoption/README.md §6, "The Q5 guard").
 # For each format that has a pin: [class, SHA-256] for every class of the registry whose factory
```

**3. A deleted potential's id left in `ScalarModel`'s spec** (`0: "InversePowerPotential"` in the
`potential` parent's `types`). Five failures and four errors across `test_fingerprint_format_guard`,
`test_store_fingerprint_golden` and `test_inventory_specs`: the guard's
`test_every_spec_is_pinned_for_the_current_format` ("no longer match the pin for format 2: spec
changed: ScalarModel"); in `test_inventory_specs`
`test_the_polymorphic_maps_are_the_model_ids` (`{0: 'InversePowerPotential', 1:
'ExponentialPotential'} != {1: 'ExponentialPotential'}`), `test_every_referenced_class_is_registered_and_declares_a_spec`
and `test_no_parent_crosses_a_shard` (`'InversePowerPotential' not found`), and errors in
`test_the_registry_is_accepted_and_ordered`; and in `test_store_fingerprint_golden` the
`TestGolden` tests (errors) and `TestCommandLine.test_store_fingerprint`.

```diff
--- a/ObjectFactories/ScalarModel.py
+++ b/ObjectFactories/ScalarModel.py
@@ -236,6 +236,7 @@
                     "potential_serial",
                     type_column="potential_type",
                     types={
+                        0: "InversePowerPotential",
                         EXPONENTIAL_POTENTIAL: "ExponentialPotential",
                     },
                 ),
```

## Observations not acted on

- **`plot_by_beta.py`'s `potential_types`** is read by nothing in the file (`grep -n potential_types
  plot_by_beta.py` finds only the definition); changed to `["Exponential"]` as the note rules, not
  deleted. 08 removes `--potential-type`.
- **`plot_inputs.py`'s `POTENTIAL_CLASSES` flag `has_n`** is now `True` for its only row, so
  `_potential_payload`'s without-`n` branch is dead; and `main.py`'s `convert_to_potential` is an
  `if … else raise` around one case. Both are for 08's removal of the option, not this prompt.
- **The note's closing `grep` does not use a portable word boundary**; later prompts that copy it
  (02's, 03's) should use `git grep -P`, or they will report a clean tree that is not.
- **`ObjectFactories/tests/data/` and `ComputeTargets/tests/data/`** hold the golden JSON files;
  `ComputeTargets/tests/data/` has no `__init__.py`, as the note says; unittest discovery ignores it.
- **`.documents/architecture-summary.md`** still describes `ReclinerPotential`-era behaviour in the
  dated notes of 2026-10-01 (`:961`: "implemented on `ExponentialPotential` only" is still true;
  the sentence "Removing the region properties is open issue …" is 04's). Left, by invariant 6.
- **The registry and the test count.** `len(factories)` is 23 and 19 distinct factory types
  (13 with a spec); the note's "27 → 23" holds.

## State handed to the next prompt

- **Tree:** this commit, on `jordan-normalization`, on top of `701ed4c`. The only potential class is
  `ExponentialPotential`; `--potential-type` accepts only `Exponential` (08 removes the option);
  `model_ids.EXPONENTIAL_POTENTIAL = 1`.
- **Fingerprint:** `FINGERPRINT_FORMAT = 2`; `SPEC_DIGESTS[1]` and `[2]` (17 pairs, 14 distinct); the
  current golden file is `ObjectFactories/tests/data/store_fingerprint_jordan-normalization-01.json`
  (`test_store_fingerprint_golden.GOLDEN`); format 1's file stays. **02 makes format 3**: a new pin
  generated by `spec_digests`, a new golden file `store_fingerprint_jordan-normalization-02.json`,
  `GOLDEN` pointed at it. The golden store has two `ExponentialPotential` rows (n = 1, 2) and two
  `ScalarModel` rows, the first tagged; `LABEL` and `T0` are unchanged.
- **The golden history:** `ComputeTargets/tests/test_golden_history.py`, `golden_history.py`,
  `data/golden_history_01.json` (SHA-256 `6898a432…62d6`), about 11 s. **02 and 04 must pass it
  unchanged** (bitwise). **03 and 05 regenerate it deliberately**
  (`PYTHONPATH=. ./venv/bin/python -m ComputeTargets.tests.golden_history`) and log the largest
  relative change per field; the generator calls `compute_scalar_model._function` with the
  signature of `ac187c8`'s (`cosmology, T_init, T_stop, phi, pi, z_grid, potential, coupling`), so
  02 (a rename) and 04 (the model interface) must change `golden_history.run_history` to follow
  the new names and signature, and say so in their logs; **a change to `run_history` is a change
  of the setup, not of the pinned values**, and the pinned values still may not move.
- **Counts:** CosmologyModels 18, ComputeTargets 179 (61 + 118), ObjectFactories 78, tools 84,
  RunDrivers 61.
- **Open issues:** none opened. `[00-declare-reflects-at-origin-for-the-other-potentials]` closed
  (moot); seven remain assigned to this campaign (board §3.1, `.documents/OPEN_ISSUES.md` §1.15).
- **The closing `grep`** needs `git grep -P` for `\b` on this machine.
- **Breakage replay:** the three diffs above apply to this commit's tree: `git apply --check` passes for each, extracted from this log, from the tree of this commit, and after applying each, `git apply -R --check` passes and the reverse leaves the tree as it was (checked in `bash`, before the commit, on the tree that the commit holds).
