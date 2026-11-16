# Log 05 — the store

**Subject:** `InversePowerModel` gets a table and goes through the store. That covers its factory, keyed
(M, n, β), the registry and the sharding, `ScalarHistory`'s polymorphic key (type id 2), the key's drain model checked
when a `ScalarHistory` is constructed, the store tools that join a model's table (filtered by `model_type`;
`bbn_from_store`'s `--model` and `--n`), `FINGERPRINT_FORMAT` 8 and `VERSION_LABEL` 2026.12.0 (P33–P44, U6).
**Commit:** this commit, on branch `power-law-wall`, on top of `fb8bef2`
**Date:** 2026-11-16, by Claude Opus 5.5 (one agent, in the worktree `.claude/worktrees/power-law-wall`)
**Result:** **COMPLETE.** The gate Store holds (§4).
- **One stop, ruled by the user.** Ruling 4: one existing test built a `ScalarHistory` with a key the new check
  refuses. The user's ruling (a) of 2026-11-16 changed the test's stand-in model (§2.1, ruling 4; D1).
- **The table:** `ObjectFactories/InversePowerModel.py` is registered immediately after `ExponentialModel`, with
  batch size 50, sharded by β, and in no drop group. Its spec, and so its digest, is the exponential's.
- **The key:** `ScalarHistory`'s `model` parent maps 1 → `ExponentialModel` and 2 → `InversePowerModel`. A
  power-law history at laboratory M, β = 2 and n = 1 round-trips under the fixed law at α = 0.1 and under the
  one-loop law at A = 1 (`one-loop-measured-pl-v1`). Every column, sample and bounce row is equal.
- **The two models' serials collide:** the first row of each table is serial 1. Every raw join of a model table in
  production now matches `model_type` as well. There were three such joins, all in prompt §5's files.
- **The labels:**
  - `FINGERPRINT_FORMAT` is 8, and `SPEC_DIGESTS[8]` has 17 pairs and 14 distinct digests. Only `ScalarHistory`'s
    digest moved, and `InversePowerModel`'s equals `ExponentialModel`'s.
  - The golden at 8 differs from format 7's in exactly the format, the empty class and the top digest.
  - `VERSION_LABEL` is 2026.12.0.
- **The exponential:** no exponential value moved. The goldens pass unedited. The exponential tool route is
  identical to 04's file with timings masked.
- **Issues:** none opened, none closed.
- **Suites:**
  - Base: 26, 732 (219 + 513), 163, 302, 76.
  - After: 26, 739 (226 + 513), 175, 307, 76.
  - In both runs only the two worktree failures of note 01's ruling 7 failed.

## 1. What shipped

- **`ObjectFactories/InversePowerModel.py`** (new): `sqla_InversePowerModel_factory`, table `InversePowerModel`. It is
  written as `ObjectFactories/ExponentialModel.py` is: the same columns (`M_serial`, `n_value`, `beta_serial`), the same
  `inventory_spec` and the same `build`, which returns an `InversePowerModel`. `ExponentialModel.py` is unchanged.
- **`config/datastore.py`:** the factory sits after `"ExponentialModel"`, with batch size 50, in no drop group.
  **`config/sharding.py`:** `"InversePowerModel": "shard_key"`.
- **`ObjectFactories/ScalarHistory.py`:** the `model` parent's `types` gains `INVERSE_POWER_MODEL: "InversePowerModel"`,
  and the comment above the spec is updated. The comments above the specs of `ObjectFactories/AdiabaticHistory.py`
  and `ObjectFactories/BBNData.py` are corrected ("a ScalarHistory's model, an ExponentialModel or an
  InversePowerModel"), as is the comment on `INVERSE_POWER_MODEL` in `CosmologyConcepts/Models/model_ids.py`. No spec,
  column or code path of theirs changes.
- **`ComputeTargets/ScalarHistory.py`** (§1.4 only):
  - The new function `check_key_drain_model(model, alpha, context)` sits after `check_drain_law_model`.
  - The `ScalarHistory` constructor calls it first.
  - Under `fixed` nothing is asked of the model. Under any other drain model, a key that is not the model's own
    `one_loop_drain_model` is a `ValueError` naming the class, its `one_loop_drain_model` and the key's model.
  - The model's own `ValueError` (InversePowerModel at n ∉ {1, 2}) propagates.
  - The guard in `store()` is unchanged.
- **`tools/compare_stores.py`:** `POLYMORPHIC_REFERENCES["ScalarHistory"]["model_serial"]` maps 2 →
  `"InversePowerModel"`. The `model_type` entry of `UNSTORED_TYPE_VALUES` is deleted together with its emptied dictionary
  (D3), and the comments above both maps are updated.
- **`tools/inventory_report.py`:** "Models" lists `ExponentialModel`, `InversePowerModel`.
- **`tools/bbn_from_store.py`** (P38):
  - `--model {exponential, inverse-power}` and `--n` take the names, defaults (exponential, 1), type (`positive_int`)
    and help of `tools/history_and_bbn.py`.
  - The find queries join that model's table (`_FIND_QUERIES`: the exponential's two queries as they were, and a
    power-law copy of each), then match `sm.model_type` and `m.n_value` before the α and drain-model matches.
  - Under `one_loop` the drain model matched is the model's own `one_loop_drain_model` at n (`_drain_model`). At an n
    with no one-loop law the tool exits 2 at parse, through `parser.error("argument --n: …")`.
  - The header line, both `LookupError` messages and the CSV row's `input` gain ` model=<model> n=<n>` after `M=`, but
    only away from the defaults (`model_label`), so every existing command's output is unchanged.
  - `model_of_law` is no longer imported here; the function itself is unchanged.
- **`tools/outward_step_scan.py`** (P39):
  - The store read filters `h.model_type = EXPONENTIAL_MODEL`.
  - It counts the rows of any other model, whatever their drain model, and names them on a second stderr line after
    the drain-model line (D4).
  - The drain-model count is now taken over exponential rows only, so on a store of one model it is as before.
  - There is no `--model` option.
- **`config/fingerprint.py`:** `FINGERPRINT_FORMAT = 8`, with its paragraph in the format history. `SPEC_DIGESTS[8]` was
  generated by `spec_digests` and has its own paragraph. Formats 1–7 are untouched: the diff removes no line but
  `FINGERPRINT_FORMAT = 7`.
- **`config/version.py`:** `VERSION_LABEL = "2026.12.0"`, with its paragraph. It says what moves, what does not, and
  that this tree cannot read a 2026.11.0 store (U6, P41).
- **Tests, new:**
  - `ObjectFactories/tests/test_inverse_power_store.py`, 12 tests: the factory, the two models apart, the two real
    round trips, the key at entry, the inventory and the configuration.
  - `ComputeTargets/tests/test_bbn_from_store_model.py`, 7 tests: a hand-written mixed store.
  - `tools/tests/test_power_law_store_tools.py`, 5 tests: `compare_stores`, `inventory_report` and
    `outward_step_scan` on a mixed store.
  - `ObjectFactories/tests/data/store_fingerprint_power-law-wall-05.json`, the golden at 8.
- **Tests, changed:** §3.6.
- **`CLAUDE.md`:** the counts and the date of the last change to a count, and nothing else.
- **The board:** row 05, the gate Store, the baselines, the status line and its history. No §3/§4 entry, so
  `.documents/OPEN_ISSUES.md` is unchanged.
- **Not committed:** the base and after suites' outputs, the golden-writing script, the tool-route run and the
  breakage backups, all in the agent's scratch directory `…/scratchpad/agent05/`.

## 2. Deviations from the prompt

### 2.1 The orchestrator's rulings (note 05 §0)

1. **The worktree and the main checkout.** All work was done in the worktree with `./venv/bin/python` and
   `PYTHONPATH=.`. The main checkout was neither read nor touched, `venv` was never staged, `/tmp/ray-cpbh` was never
   named, and nothing went in `/tmp` but the session scratchpad. *STRUCTURALLY REQUIRED.*
2. **The exponential does not move.** The following are unchanged:
   - `ObjectFactories/ExponentialModel.py`;
   - the exponential's spec digest (`5bb13de4…`, as at format 7);
   - `build_store` and the golden store's rows;
   - the goldens' expectations.

   `test_golden_history`, `test_golden_reflecting_history` and `test_tuning` pass unedited. The tool route equals 04's
   file with timings masked (§4). *STRUCTURALLY REQUIRED.*
3. **The golden's diff** is exactly the three differences of note §0 (§3.3). *IMPLEMENTATION CHOICE, ruled.*
4. **The entry check — a stop, ruled by the user.**
   - With the check in place, `ComputeTargets/tests/test_drain_store_records.py`,
     `TestTheLawThroughTheStoredPath.test_compute_passes_the_keys_law` (subTest `law='one_loop'`) was refused. It
     constructs a `ScalarHistory` with `model=mock.Mock()` and `alpha=alpha_value(8, 0.5, "one-loop-measured-v1")`.
     The Mock's `one_loop_drain_model` is a Mock, not the key's model:
     `ValueError: ScalarHistory: the key names the drain model 'one-loop-measured-v1', but
     Mock.one_loop_drain_model is <Mock …>`.
   - It was the only refusal in the five suites. No production caller is refused: `main.py`, `plot_inputs.py`,
     `plot_by_beta.py` and `plot_ScalarHistory.py` build only `ExponentialModel` histories, under `fixed` or
     `one-loop-measured-v1`, the exponential's own.
   - The agent stopped without changing the caller. The user's ruling of 2026-11-16, in their words: **"(a) here
     please"**. That is option (a): the test's stand-in model is given `one_loop_drain_model="one-loop-measured-v1"`,
     and the check is not changed (D1).

   *IMPLEMENTATION CHOICE, ruled; the stop ruled by the user.*
5. **Existing tests** were changed only for the label, the format, the new class and the map (§3.6), with the one
   exception D1, made by the user's ruling on the stop. *IMPLEMENTATION CHOICE, ruled.*
6. **Other joins.** Searched with `git grep -n -i "join \(ExponentialModel\|InversePowerModel\)\|model_serial" -- '*.py'
   '*.sh' ':!.documents' ':!science-runs' ':!prompts'`, outside `tests/`. Besides the factory's own `build`, which
   already filters `model_type`, the only raw joins are the three of note §0 (§3.5). None is outside prompt §5's files.
   *IMPLEMENTATION CHOICE, ruled.*
7. **The two worktree failures** (`test_registered_run…this_repositorys_head`, `test_registry_config…
   test_roots_and_distributions`) failed at the base and after, and nothing else failed but the stop's test, before
   the ruling. *IMPLEMENTATION CHOICE, ruled.*
8. **The machine.** The suites ran serially, with `uptime` at every start and end (§3.1). *IMPLEMENTATION CHOICE,
   ruled.*
9. **A finding about production** is recorded (§6), not fixed. No issue is opened. *IMPLEMENTATION CHOICE, ruled.*
10. **No file but prompt §5's list**, plus D1's test file, which the user's ruling names. *STRUCTURALLY REQUIRED.*

### 2.2 Others

- **D1 — `ComputeTargets/tests/test_drain_store_records.py`, an existing test changed outside ruling 5's four.** In
  `test_compute_passes_the_keys_law` the argument `model=mock.Mock()` became
  `model=mock.Mock(one_loop_drain_model="one-loop-measured-v1")`. Nothing else in the test or the module changed. It
  was made by the user's ruling (a) of 2026-11-16 on the ruling-4 stop ("(a) here please"). *STRUCTURALLY REQUIRED*:
  the check of prompt §1.4 refuses the test's key otherwise, and the user ruled that the stand-in change it.
- **D2 — `bbn_from_store` reads a shard without `model_type` or `n_value` as before.**
  - The existing tests' hand-written imitation stores have `ExponentialModel` without `n_value` and `ScalarHistory`
    without `model_type`: `test_bbn_from_store` and `test_bbn_from_store_alpha`.
  - `test_one_loop_tools` asserts `_find_query(<empty db>, …) is _FIND_QUERY_WITHOUT_ALPHA` with parameters
    `(BETA, 5.0)`.
  - Filtering unconditionally would have meant editing them, which ruling 5 forbids, and the gate §2.1 asks that the
    tools give the old output "under their existing tests".
  - So the `model_type` match is applied only where `ScalarHistory` has the column (a shard without it holds no
    power-law history), and the `n_value` match only where the model's table has it. A shard without
    `InversePowerModel`'s table holds none of its histories.
  - No store of format 5 or later lacks either column, so on every real store both matches apply. The model and drain
    matches are bound before the α match, so the drain model's parameter stays last, as `test_one_loop_tools` asserts.

  *IMPLEMENTATION CHOICE.*
- **D3 — the emptied `UNSTORED_TYPE_VALUES["ScalarHistory"]["model_type"]` is removed.** Before prompt 01 the map had no
  `model_type` key, and the tool and test 13 read it with `.get(type_column, {})`, so the module's own convention is no
  key for a type column with nothing unstored. The comment above the map says so. *IMPLEMENTATION CHOICE.*
- **D4 — `outward_step_scan` names the other model's rows on a line of their own.** Prompt §1.5 asks that they be
  "counted with the rows it already skips … and named in its output where those are".
  - They are counted in the same pass (every shard, before the filters) and named in the same place (stderr, after
    the last shard), but on a second line: `!! outward_step_scan: N ScalarHistory row(s) of PATH of another model than
    'ExponentialModel' skipped; this tool rebuilds a history as ExponentialModel`.
  - The first line now counts only exponential rows under another drain model, so on a store of one model it is
    byte-identical to before, and the two reasons are not merged into one number.

  *IMPLEMENTATION CHOICE.*
- **D5 — `bbn_from_store`'s CSV `input` gains the model label**, empty at the defaults, beside the header. A power-law
  solve is then not written to the CSV as if it were the exponential's, and every existing command's rows are
  unchanged. *IMPLEMENTATION CHOICE.*
- **D6 — `bbn_from_store` reads the model's `one_loop_drain_model` from a model built at the run's M and β**, or M_P
  and 1 where `--sm-baseline` gives neither. The hook reads neither value on either class. This is the model
  interface, with no power-law expression in the tool. *IMPLEMENTATION CHOICE.*
- **D7 — the round trips are at n = 1** (prompt §1.7: "say which"). These are 04's `ip-n1-M4e-31-b2.0-fixed0.1` and
  `…-oneloop1`, each tuned in one integration at its tracking seed. The one-loop history has 37 candidates and bounce
  rows. *IMPLEMENTATION CHOICE.*
- **D8 — `test_inventory_specs`' test of the definitions is renamed** `test_thirteen_definitions_serve_sixteen_classes`
  → `test_fourteen_definitions_serve_seventeen_classes`, as its predecessors were renamed with their counts. The
  new class gives 17 classes and 14 definitions. *IMPLEMENTATION CHOICE.*

## 3. Verification performed

### 3.1 The suites (P44)

Serial, from the worktree root. The cells give the one-minute load from `uptime` at the start → end. For the first
three base suites, the end load is the start load of the suite after.

The after run was 10:08–10:30. Its loads, up to 147 at its start, were not this campaign's: another process on the
machine was running. The times are close to the base's all the same.

| Suite | Base `fb8bef2` (2026-11-16 01:45–02:06) | After (with D1; 2026-11-16 10:08–10:30) |
|---|---|---|
| CosmologyModels | 26 / OK / 65.5 s / 5.22 → 4.18 | 26 / OK / 83.7 s / 146.97 → 115.50 |
| ObjectFactories | 163 / OK / 131 s / 4.18 → 4.47 | 175 / OK / 165 s / 115.50 → 54.82 |
| RunDrivers | 76 / 1 failure (ruling 7) / 24 s / 4.47 → 4.54 | 76 / 1 failure (ruling 7) / 28 s / 54.82 → 47.79 |
| ComputeTargets `[a-f]` | 219 / OK / 147 s / 4.54 → 7.78 | 226 / OK / 154 s / 47.79 → 49.46 |
| ComputeTargets `[g-z]` | 513 / OK / 544 s / 7.29 → 6.54 | 513 / OK / 547 s / 49.46 → 11.14 |
| tools | 302 / 1 failure (ruling 7) / 293 s / 7.15 → 4.27 | 307 / 1 failure (ruling 7) / 318 s / 11.14 → 16.94 |

No count fell. The rises: ObjectFactories +12 (`test_inverse_power_store`), `[a-f]` +7 (`test_bbn_from_store_model`),
tools +5 (`test_power_law_store_tools`).

There was an earlier after-run (02:14–02:36), made before the ruling.
- Its counts were the same.
- `[a-f]` failed only the stop's test, and the two known failures recurred.
- `test_datastore_config` failed twice, on its pins of the registry and the batch sizes. They were then given the new
  class (§3.6) and the module passed alone (16 tests).
- The run above repeats every suite after both changes.

### 3.2 The spec digests (prompt §1.6, §2.3)

`spec_digests(factories)` at the tip: **17 pairs, 14 distinct digests**, against format 7's 16 and 14.
- `InversePowerModel` `5bb13de42840b317…` equals `ExponentialModel`'s, and is NEW.
- `ScalarHistory` moved, `743fb2f2…` → `ee7b250afb624ad5…`, because its parent `model` maps type id 2 as well as 1.
- The other fifteen pairs equal format 7's.
- `SPEC_DIGESTS[8]` holds these pairs as generated, in registry order. `test_fingerprint_format_guard` and
  `test_inverse_power_store.TestTheConfiguration` pin it.

### 3.3 The golden at 8 (P40, ruling 3)

`build_store` is unchanged. The new file was written from `config.fingerprint.fingerprinter.take(primary, None)` as
`json.dumps(fp, indent=2, sort_keys=True) + "\n"`, by a scratch script that runs `_GoldenStoreCase.build_store`.
`diff store_fingerprint_one-loop-drain-03.json store_fingerprint_power-law-wall-05.json`:
```
46a47,51
>     "InversePowerModel": {
>       "count": 0,
>       "digest": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
>       "tag_sets": []
>     },
187c192
<   "digest": "79cf14174e833458ed1acbdfedf1b7407b899bc21e1f0ff1670bb01db6e3cca7",
---
>   "digest": "e0e0493f4a77514d3cc9132cdfb7a4de6dfa0e91b91f3709e9d3cc139c6d9c44",
189c194
<   "fingerprint_format": 7,
---
>   "fingerprint_format": 8,
```
The format, the empty class and the top digest, and nothing else. `ScalarHistory`'s class entry does not move: the
spec's digest is not part of the content.

### 3.4 The entry check (§1.4)

- **Where it is.** `check_key_drain_model` in `ComputeTargets/ScalarHistory.py`, called first in
  `ScalarHistory.__init__`.
- **What it refuses.** An α whose `drain_model` is neither `fixed` nor `model.one_loop_drain_model`.
  - `test_inverse_power_store.TestTheKeyAtEntry` checks the refusals, with `tune_history`, `integrate_history`,
    `integrate_scalar_history` and `compute_scalar_history` patched to fail if called: `InversePowerModel` at n = 1
    and n = 2 under `one-loop-measured-v1`, and `ExponentialModel` under `one-loop-measured-pl-v1`. Each message
    names the class, its model and the key's model.
  - Under the fixed model, power law n = 1, 2, 3 and exponential n = 1, 2 are accepted, as is each under its own
    one-loop model.
  - n = 3 under either one-loop model raises the model's own `ValueError`.
- **Callers.** The one existing test it refused is ruling 4's (§2.1). No production caller is refused.

### 3.5 The raw joins (P37)

| File | Line (after) | Join | Now |
|---|---|---|---|
| `tools/bbn_from_store.py` | 250 (`_FIND_QUERY`) | `join ExponentialModel m on m.serial = sm.model_serial` | `+ and sm.model_type = ? and m.n_value = ?`; a power-law copy joins `InversePowerModel` |
| `tools/bbn_from_store.py` | 271 (`_FIND_QUERY_WITHOUT_ALPHA`) | the same | the same |
| `tools/outward_step_scan.py` | 878 | `join ExponentialModel e on h.model_serial = e.serial` | `where h.model_type = ? [and a.drain_model = ?]` |

There is no other one in production code. `ObjectFactories/ScalarHistory.py`'s `build` already filtered
`model_type == model.type_id`.

### 3.6 The existing tests changed, old → new

- **`ObjectFactories/tests/test_store_fingerprint_golden.py`**
  - `GOLDEN`: `store_fingerprint_one-loop-drain-03.json` → `store_fingerprint_power-law-wall-05.json`.
  - `len(inventory.classes)`: 16 → 17. The every-class `count >= 1` now excludes `InversePowerModel`, which must be
    `== 0`.
  - `TestCommandLine`: `golden["fingerprint_format"] == 7` → `8`.
  - The docstring gains a "Format 8" paragraph, and its count of classes notes `InversePowerModel` (new class, format).
- **`ObjectFactories/tests/test_fingerprint_format_guard.py`:** `len(pinned)` 16 → 17. The docstring's counts change
  from 16 classes and 12 definitions to 17 and 14, and name the test renamed in D8 (new class).
- **`ObjectFactories/tests/test_inventory_specs.py`**
  - `EXPECTED_ORDER` gains `"InversePowerModel"` after `"ExponentialModel"`.
  - `len(owners)` 16 → 17 and the definitions 13 → 14, with the test renamed (D8).
  - The polymorphic map `{1: "ExponentialModel"}` → `{1: "ExponentialModel", 2: "InversePowerModel"}` (new class,
    map).
- **`ObjectFactories/tests/test_datastore_config.py`:** `EXPECTED_FACTORIES` gains
  `("InversePowerModel", "sqla_InversePowerModel_factory", None)` after the exponential's.
  `EXPECTED_SERIAL_BATCH_SIZES` gains `"InversePowerModel": 50`, and the docstring one sentence (new class).
- **`ObjectFactories/tests/test_drain_store.py`:** `FINGERPRINT_FORMAT` 7 → 8. `sorted(SPEC_DIGESTS)` `[1..7]` →
  `[1..8]`, and the docstring's configuration bullet notes 8 (format).
- **`ObjectFactories/tests/test_dissipation_store.py`:** `VERSION_LABEL` `"2026.11.0"` → `"2026.12.0"`, and
  `FINGERPRINT_FORMAT` 7 → 8, with the docstring's bullet 10 and the comment (label, format).
- **`tools/tests/test_inventory_report.py`:** `len(self.inventory.classes)` 16 → 17. The line `"read-only; 1 shard (#0);
  45 records in 16 classes"` → `"… 45 records in 17 classes"` (new class).
- **`ComputeTargets/tests/test_drain_store_records.py`:** D1, by the user's ruling: `model=mock.Mock()` →
  `model=mock.Mock(one_loop_drain_model="one-loop-measured-v1")`.

`test_compare_stores` tests 13 and 13b pass unedited with id 2 mapped: 13b reads `InversePowerModel.type_id` by `ast`
and finds `INVERSE_POWER_MODEL`, which is 2. So does test 14 on ChamPBH's schema, whose shards now carry an empty
`InversePowerModel` table. `test_golden_history`, `test_golden_reflecting_history`, `test_tuning`,
`test_tuned_history_round_trip`, `test_bbn_from_store`, `test_bbn_from_store_alpha`, `test_one_loop_tools` and
`test_outward_step_scan` are unedited.

### 3.7 `black`

`black` was run on every Python file changed or added. `black --check` on the 25 changed or new Python files leaves
all of them unchanged. No file outside the commit was reformatted.

## 4. The gate Store, item by item (prompt §2)

**2.1 The exponential is unchanged.**
- *The goldens:* `test_golden_history`, `test_golden_reflecting_history` and `test_tuning` are in the `[g-z]` half
  (513, OK) and are unedited.
- *The store round trips:* `test_tuned_history_round_trip` is unedited and passes in ObjectFactories.
  `test_drain_store` and `test_dissipation_store` pass, changed only in their format and label literals (§3.6).
- *The tool route:* `PYTHONPATH=. ./venv/bin/python tools/history_and_bbn.py 2.0 4e-31 --drain-law one_loop --alpha 1`
  (02:13, load 3.39 → 3.09, exit 0) was masked (`#` lines dropped, `wall=` and `PRyM_time=` values replaced) and
  compared with `diff` against `.documents/power-law-wall/sample-04/tool-route/exp-M4e-31-b2.0-oneloop1.txt`,
  masked the same way. Output: `IDENTICAL`, 13 lines each.
- *The store tools on an exponential store:* `tools/bbn_from_store.py` and `tools/outward_step_scan.py` pass their
  existing tests unedited (`test_bbn_from_store` 4, `test_bbn_from_store_alpha` 6, `test_one_loop_tools`,
  `test_outward_step_scan`), with the header line, the `LookupError` messages and the stderr line unchanged at the
  defaults.

**2.2 The round trip.**
- `PYTHONPATH=. ./venv/bin/python -m unittest ObjectFactories.tests.test_inverse_power_store` (02:10, load 8.31):
  `Ran 12 tests in 23.555s`. The inventory test failed on that first run because of a harness defect (an unstored
  cosmology), not the code. After the fix it gave `OK` alone, and the module passes in the full suite. The tests are:
  - the factory;
  - the two models apart (serial 1 in both tables, two histories, each lookup finds its own);
  - the fixed-law (α = 0.1) and one-loop (A = 1, `one-loop-measured-pl-v1`) round trips at laboratory M, β = 2,
    n = 1, which check every tuning, dissipation, drain-record, first-bounce and fixed-T column, every sample with
    its masses and ratio, and every bounce row; the realized model is rebuilt as `InversePowerModel`;
  - the key at entry;
  - the inventory (no problem, each history's `model` resolved to its own table);
  - the configuration.
- `PYTHONPATH=. ./venv/bin/python -m unittest tools.tests.test_power_law_store_tools`: 5 tests, OK.
  - `compare_stores` finds a copy equal (2 histories, 1 row in each model table) and one changed `Delta_N_eff_today`
    in the power-law row a near miss rendered as an `InversePowerModel` row.
  - `inventory_report` lists both classes.
  - `outward_step_scan` selects the exponential's history only and prints one line counting the other.
- `PYTHONPATH=. ./venv/bin/python -m unittest ComputeTargets.tests.test_bbn_from_store_model`: 7 tests, OK.
  - Each (model, n) finds its own history and never the other model's of the same serial.
  - Under `one_loop` the power law matches `one-loop-measured-pl-v1` (50) and never the decoy under `-v1` (60).
  - `--model inverse-power --n 3 --drain-law one_loop` exits 2, naming `--n`.
  - The header names the model away from the defaults only.

**2.3 The fingerprint.** `SPEC_DIGESTS[8]` and the golden at 8 pass in ObjectFactories (`test_fingerprint_format_guard`,
`test_store_fingerprint_golden`). Formats 1–7's pins are unchanged (the diff removes no line from them). The golden's
diff is §3.3's. The digest that moved is `ScalarHistory`'s (its `model` parent's `types`); `InversePowerModel`'s is
new and equals the exponential's.

**The gate Store holds.**

## 5. The deliberate-breakage record

Each was made on the tree with a backup copy, run, and the file restored (`cmp` with the backup: restored).
1. **`bbn_from_store` without the `model_type` match** (`_MODEL_TYPE_MATCH = " and ? > 0"`): `test_bbn_from_store_model`
   gave 8 errors, for example `LookupError: 2 ScalarHistories match beta=2 M=4e-31 phi=5: modelstore-shard0000.db:10,
   modelstore-shard0000.db:20`. That is the serial collision P37 names.
2. **`outward_step_scan` without the `model_type` filter** (`where ? > 0`):
   `test_the_exponentials_rows_only_and_the_other_counted` FAIL.
3. **The `types` map without id 2:** `TestTheInventory` FAIL (the unresolved parent of note §0) and
   `test_every_spec_is_pinned_for_the_current_format` FAIL.
4. **The constructor without the check:** `TestTheKeyAtEntry` gave 5 FAILs (the three refusals and n = 3 under both
   one-loop models).

## 6. Observations not acted on

- **`tools/outward_step_scan.py` re-runs every selected exponential row at n = 1.** `run_history` builds
  `ExponentialModel(…, 1, …)`, and the store read does not filter `n_value`, so an exponential row at n = 2 would be
  re-run and compared as if it were n = 1. The exponential's science stores hold n = 1 only (`--n-values` defaults to
  [1]), so no stored run is affected. Prompt §1.5 puts this outside 05. Not an issue.
- **Constructing a `ScalarHistory` now reads `alpha.drain_model`, and under a one-loop model
  `model.one_loop_drain_model`.** A stand-in that lacks the first is an `AttributeError` at construction, where it was
  read before only in `store()`. Every stand-in in the suites has it (`test_version_keyed_lookups._alpha`). D1's
  stand-in now has the second.
- **The hand-written imitation stores** of `test_bbn_from_store` and `test_bbn_from_store_alpha` lack `model_type` and
  `n_value`, columns every real store of format 5 or later has. D2 keeps the tool reading them; a later prompt that
  may edit those tests could give them the columns and drop the fallback.
- **The prompt's cost estimate held.** The two round-trip tunings are one integration each, and the whole new
  ObjectFactories module ran in 23.6 s at a load of 8.3.

## 7. State handed to the next prompt

- **The schema 06 reads.**
  - Table `InversePowerModel` (`serial`, `version`, `timestamp`, `M_serial` → `M_value`, `n_value`, `beta_serial` →
    `beta_value`), sharded by β, beside `ExponentialModel`, with the same columns. A `ScalarHistory` names its model by
    `(model_type, model_serial)`: 1 is `ExponentialModel` and 2 is `InversePowerModel`.
  - **The serials of the two tables repeat each other**, so every read must match `model_type`. In the factories this
    means a lookup through `pool.object_get("InversePowerModel", M=…, n=…, beta=…, units=…)`, which returns an
    `InversePowerModel`, then `object_get("ScalarHistory", model=<that object>, …)`. `ScalarHistory.build` filters
    `model_type == model.type_id`.
- **The label and the format.** `VERSION_LABEL = "2026.12.0"` and `FINGERPRINT_FORMAT = 8`. This tree cannot read a
  2026.11.0 store (DatastoreKit's `StoreSchemaMismatch` / `RuntimeError` for the missing table); the two models are
  compared through their exports (U6).
- **The drain-model check.** `ScalarHistory(…)` refuses a key whose drain model is neither `fixed` nor
  `model.one_loop_drain_model`, with a `ValueError`, before any lookup or computation. For the power law the one-loop
  rows must be looked up as `alpha_value(value=A, drain_model=model.one_loop_drain_model)`: `one-loop-measured-pl-v1`
  at n = 1, 2. At n = 3 under the one-loop law the model raises.
- **What 06's drivers must do for the power law, which the store now allows.**
  - `main.py` needs a model option (P12): build `InversePowerModel` rows through the factory, and look up the one-loop
    `alpha_value` rows under the model's own `one_loop_drain_model`, not `DRAIN_MODEL_ONE_LOOP`, or every power-law
    history is refused at construction.
  - `plot_inputs.py` and the plot scripts must look up `MODEL_CLASS` and the stored α rows the same way (P17).
  - `histories.csv` needs its `model` column, and the power law its export paths (P11).
  - `tools/bbn_from_store.py` already takes `--model` and `--n`.
  - `outward_step_scan` stays exponential-only.
