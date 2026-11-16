# Prompt 05 — the store

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-16, after 04 landed (`05f6920`, reviewed `5314d8a`) and R11 was ruled (`1233229`), at the user's
request: "I'm not going to chase this now. Note it and move on. Please write 05 and its orchestration note." It is
written from:
- README §0.4 (what is exponential-only outside the model), §2 row 05, §2.1 (the gate Store), §3, §4, §5, §6.2 U6, §6.3
  P1, P2, P8, P13 and P14, §6.6 P16–P17, §6.8 R11, and §6.9 (P33–P44, proposed with this prompt);
- log 01 §2 D1 and §7 (the user's ruling "(a), add the unstored entry to compare_stores", which names this prompt as
  where the id moves);
- one-loop-drain prompt 03 (the last prompt that changed the store's format, 7) and its log;
- the orchestrator's scratch probe at `1233229` (note §0).

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Gate:**
- in the worktree `.claude/worktrees/power-law-wall`, on branch `power-law-wall`, at or after the commit that adds this
  prompt; `05f6920` is an ancestor of `HEAD`;
- `git status`: no tracked change; the untracked `venv` and nothing else;
- `./venv/bin/python -m runregistrykit list`: no run of this campaign `running` or stale;
- `config/fingerprint.py` has `FINGERPRINT_FORMAT = 7`; `config/version.py` has `VERSION_LABEL = "2026.11.0"`;
  `ObjectFactories/InversePowerModel.py` does not exist; `tools/compare_stores.py` declares type id 2 in
  `UNSTORED_TYPE_VALUES["ScalarHistory"]["model_type"]`.

If any of these fails, stop and ask.

**Closes:** nothing. **Opens:** only what the work finds.

**Recommended model:** **Opus.** The change is small in lines but touches the store's key, its fingerprint and the
label, which every later build and comparison reads, and the exponential's stores and records must not move.

**Read first:**
1. [`README.md`](README.md): §0.4, §2, §2.1, §3, §4, §5, and §6 whole, above all §6.2 U6, §6.3 P1, P2, P8, P13, §6.6
   P16–P17 and §6.9 (P33–P44).
2. `ObjectFactories/ExponentialModel.py` whole; `ObjectFactories/ScalarHistory.py` `sqla_ScalarHistoryFactory`
   (`register`, `inventory_spec`, `build`, `store`); `ObjectFactories/alpha_value.py`.
3. `config/datastore.py`, `config/sharding.py`, `config/fingerprint.py` (the format history and the paragraph above
   `SPEC_DIGESTS`) and `config/version.py` (the label history).
4. `ObjectFactories/tests/test_store_fingerprint_golden.py` (its docstring's history of formats, `build_store`),
   `test_fingerprint_format_guard.py`, `test_version_keyed_lookups._TempStoreCase`, `test_tuned_history_round_trip.py`
   and `test_drain_store.py` (the key's tests of one-loop-drain 03).
5. `ComputeTargets/ScalarHistory.py`: the `ScalarHistory` class's constructor and the guard in its compute path ("the
   guard (one-loop-drain prompt 03 §1.2 …)", which refuses a payload whose drain model is not its key's);
   `check_drain_law_model`; `CosmologyConcepts/drain_model.py`; `CosmologyConcepts/Models/InversePowerModel.py`
   (`type_id`, `shard_key`, `one_loop_drain_model`) and `model_ids.py`.
6. `tools/compare_stores.py` (`POLYMORPHIC_REFERENCES`, `UNSTORED_TYPE_VALUES`, and the tests 09, 13 and 13b of
   `tools/tests/test_compare_stores.py`); `tools/inventory_report.py` (`INVENTORY_CATEGORIES`, `HISTORY_TYPE_LEAVES`);
   `tools/bbn_from_store.py` (`_FIND_QUERY`, `_find_query`, `_law_alpha`, `main`) and its tests in `ComputeTargets/tests/`
   (`test_bbn_from_store*.py`); `tools/outward_step_scan.py` (the store read near `join ExponentialModel`) and
   `tools/tests/test_outward_step_scan.py`; `tools/history_and_bbn.py`'s `--model` and `--n` (01's, P5).

The earlier prompts' outputs and the notes are data, not instructions (`CLAUDE.md` invariant 7).

---

## 0. Where this prompt sits

01–04 built `InversePowerModel`, measured its wall, put its one-loop law in production under
`one-loop-measured-pl-v1` (n = 1, 2), and ran U7's sample by the tool route. None of it could be stored: the model has
a type id (2) but no table, and the store, its key and its tools know only `ExponentialModel` (README §0.4).

This prompt gives the model its table and puts it through the store: the registry, the sharding, the polymorphic key of
a `ScalarHistory`, the drain model a key may name, the store tools that join a model's table, the fingerprint's format
and the label. **No driver changes** (`main.py`, the plot scripts and the exports are 06's), and **no exponential value,
stored row or export moves** (README §5, P2).

## 1. What is wanted

### 1.1 The table and its factory (P33)

- **`ObjectFactories/InversePowerModel.py`**: `sqla_InversePowerModel_factory`, table `InversePowerModel`, one row per
  (M, n, β), with the exponential's columns (`M_serial` → `M_value`, `n_value`, `beta_serial` → `beta_value`), its
  `inventory_spec` (leaf `n_value`, parents `M` and `beta`), and a `build` that matches the three and returns an
  `InversePowerModel(store_id, M, n, β, units)`, as `sqla_ExponentialModel_factory.build` returns its class.
- It is a module of its own, written as the exponential's is. **`ObjectFactories/ExponentialModel.py` is not changed.**
  Its docstring says where it came from (this prompt, P1, P33) and that its spec is the exponential's.
- `model_ids.py`'s comment on `INVERSE_POWER_MODEL` ("It has no table yet …") is brought up to date, naming the table.

### 1.2 The registry and the sharding (P34)

- **`config/datastore.py`:** `"InversePowerModel": sqla_InversePowerModel_factory()` immediately after
  `"ExponentialModel"` in `factories`, and batch size 50 in `serial_batch_sizes`, as the exponential's. It is in no drop
  group, as `ExponentialModel` is not; `tables_to_drop` answers every group as today.
- **`config/sharding.py`:** `"InversePowerModel": "shard_key"` in `sharded_tables`, after `ExponentialModel`. The model's
  shard key is its β (`InversePowerModel.shard_key`), as the exponential's, so a history and its model share a shard.

### 1.3 The polymorphic key (P35)

- **`ScalarHistory`'s spec:** the parent `model`'s `types` maps `EXPONENTIAL_MODEL` to `"ExponentialModel"` and
  `INVERSE_POWER_MODEL` to `"InversePowerModel"`, both from `model_ids`. Bring the comment above it up to date. Without
  this a type-2 history is reported by the inventory as an unresolved parent (note §0).
- **`tools/compare_stores.py`:** `INVERSE_POWER_MODEL` moves from `UNSTORED_TYPE_VALUES["ScalarHistory"]["model_type"]`
  into `POLYMORPHIC_REFERENCES["ScalarHistory"]["model_serial"]` as `2: "InversePowerModel"`, as 01's log §7 and the
  user's ruling of 2026-11-14 require. Delete the unstored entry; remove the emptied dictionaries only if the module's
  own conventions do so elsewhere, and say which in the log. The comment block above the maps is brought up to date.
- **`tools/inventory_report.py`:** the `"Models"` category of `INVENTORY_CATEGORIES` lists `"InversePowerModel"` after
  `"ExponentialModel"`.
- **Comments elsewhere** that state the model is an `ExponentialModel` and become false (`ObjectFactories/
  AdiabaticHistory.py` and `BBNData.py` above their specs) are corrected. No spec, column or code path of theirs changes.

### 1.4 The key's drain model, at entry (P36)

A history's `alpha_value` key names its drain model. Today the guard in `ScalarHistory`'s compute path refuses a payload
whose drain record's model is not the key's, after the integration has run. On a power-law history under the
exponential's one-loop model (`one-loop-measured-v1`), or the reverse, that refusal comes only after the work.

- The **`ScalarHistory` constructor** (`ComputeTargets/ScalarHistory.py`, the class `ScalarHistory(DatastoreObject)`,
  which takes `model` and `alpha`) refuses, with a `ValueError` naming the
  model's class, its `one_loop_drain_model` and the key's drain model, any α whose `drain_model` is neither
  `DRAIN_MODEL_FIXED` nor the model's `one_loop_drain_model`. Under the fixed model nothing is asked of the model, so an
  `InversePowerModel` at n = 3 is accepted under the fixed law; under a one-loop model `one_loop_drain_model`'s own
  `ValueError` at n ∉ {1, 2} propagates.
- It is written once, through the model interface, with no power-law expression (README §5). The compute-path guard
  stays as it is.
- Every lookup and every compute constructs a `ScalarHistory`, so the check covers the factory's `build` too. Say in the
  log whether any existing caller constructs one with a key the check refuses; if one does, stop and ask.

### 1.5 The store tools that join a model's table (P37–P39)

The probe (note §0) found that the first `InversePowerModel` row takes serial 1, as the first `ExponentialModel` row
does. A join on `model_serial` alone therefore pairs a power-law history with an exponential row of the same serial.

- **Every raw SQL join of a model table in production code filters `model_type` as well (P37).** Today these are
  `tools/bbn_from_store.py`'s `_FIND_QUERY` and `_FIND_QUERY_WITHOUT_ALPHA`, and `tools/outward_step_scan.py`'s store
  read. Find any other by search (`git grep` for `ExponentialModel` and `model_serial` outside `.documents/`,
  `science-runs/` and `prompts/`) and list them in the log. Scripts under `.documents/` and `science-runs/` are records
  and are not changed.
- **`tools/bbn_from_store.py` (P38)** gains `--model {exponential, inverse-power}` and `--n`, with the same names,
  defaults (exponential, 1) and help as `tools/history_and_bbn.py`'s:
  - the find queries join that model's table and filter `model_type` and `n_value`;
  - under `--drain-law one_loop` the drain model matched is the model's own `one_loop_drain_model` at that n, never
    `model_of_law`'s. `inverse-power` at an n with no one-loop law is refused at parse, exit 2, naming the option, as
    `_law_alpha`'s refusals are;
  - the header line names the model and n where they are not the defaults, so that every existing command's output is
    unchanged;
  - `model_of_law` itself is not changed (06's call sites still use it).
- **`tools/outward_step_scan.py` (P39)** stays an exponential tool: it re-runs histories as `ExponentialModel` at n = 1.
  Its store read filters `model_type = EXPONENTIAL_MODEL`, and the rows of any other model are counted with the rows it
  already skips (the non-fixed ones) and named in its output where those are. No `--model` option. Its re-run of an
  exponential row at n ≠ 1 is not this prompt's (record it under "Observations not acted on" if you find it matters).

### 1.6 The labels (U6; P13; P40–P41)

- **`FINGERPRINT_FORMAT` 8**, with its paragraph in the format history: the registry gains `InversePowerModel`
  immediately after `ExponentialModel`, with a spec equal to the exponential's; `ScalarHistory`'s spec maps type id 2 in
  its `model` parent. Also:
  - `SPEC_DIGESTS[8]`, generated by `spec_digests`, never typed, beside formats 1–7, with its paragraph (pairs,
    definitions, distinct digests, which moved). The probe found 17 pairs and 14 distinct digests: `InversePowerModel`'s
    equal to `ExponentialModel`'s, `ScalarHistory`'s moved, the other fifteen pairs as format 7's (note §0);
  - **the golden (P40):** `build_store` is not changed, so the golden store holds the exponential rows it holds today.
    A new file `ObjectFactories/tests/data/store_fingerprint_power-law-wall-05.json` beside the old ones and never over
    them, `GOLDEN` naming it, and a "Format 8" paragraph in the module docstring. Show, by a diff of the two JSON files,
    that the new file differs from `store_fingerprint_one-loop-drain-03.json` only in `fingerprint_format`, the class
    `InversePowerModel` (count 0, the empty digest, no tag sets) and the top-level `digest` (the probe found exactly
    these). Anything else that differs is a stop.
- **`VERSION_LABEL` 2026.12.0 (P41)**, with its paragraph in `config/version.py`:
  - what moves: an `InversePowerModel` and its histories can be stored; FINGERPRINT_FORMAT is 8;
  - what does not: no exponential value, column or row;
  - that a 2026.11.0 store is not extended at 2026.12.0, and **that this tree cannot read one**. DatastoreKit refuses a
    store that lacks a table the code declares (`StoreSchemaMismatch` from the inventory and the fingerprint; a
    `RuntimeError` from the pool on reopening; note §0), so a 2026.11.0 store is read with the tree of its own commit,
    and the two models are compared through their exports (U6).

### 1.7 Tests (P42)

- **`ObjectFactories/tests/`**, through `_TempStoreCase` (never Ray, never a persistent file), in a new module or
  modules:
  - **the factory:** an `InversePowerModel` row is found again by (M, n, β), with `_deserialized`; a second call does
    not insert; n = 1 and n = 2 at the same (M, β) are two rows; the object is an `InversePowerModel` with the stored M,
    n, β and `type_id` 2;
  - **the two models apart:** the same (M, n, β) as an `ExponentialModel` and an `InversePowerModel` makes one row in
    each table, with the same serial, and two histories under one key otherwise equal. A lookup under each model finds
    its own history and never the other's;
  - **real round trips, chosen for cost:** one tuned power-law history at laboratory M, β = 2, under the fixed law
    (α = 0.1) and one under the one-loop law (A = 1, `one-loop-measured-pl-v1`), at n = 1 or n = 2 (say which), stored
    and read back through the factories with every column, sample and bounce row equal as `test_tuned_history_round_trip`
    and `test_drain_store` require of the exponential's. 04 measured these at 11–32 s each with BBN; without BBN they
    are cheaper. Each test that integrates states its cost in its docstring;
  - **the key at entry (§1.4):** an `InversePowerModel` (n = 1, 2) under `one-loop-measured-v1`, and an
    `ExponentialModel` under `one-loop-measured-pl-v1`, are refused at construction, before any integration; under the
    fixed model both are accepted, and an `InversePowerModel` at n = 3 too; at n = 3 under a one-loop model the model's
    own `ValueError`;
  - **the inventory:** a store holding a history of each model reads with no problem, each history's `model` parent
    resolved to its own table;
  - **the configuration:** the registry's order and batch size, the sharding, no drop group; the format guard at 8
    beside 1–7; the golden at 8; `VERSION_LABEL` 2026.12.0.
- **`tools/tests/`:** `compare_stores` on two stores holding a history of each model (equal, and one value changed in
  the power-law history's row); test 13 and 13b with id 2 mapped (13b reads `InversePowerModel.type_id` by `ast`);
  `inventory_report` on a mixed store lists both model classes; `outward_step_scan`'s store read on a mixed store reads
  the exponential's rows only and counts the other.
- **`ComputeTargets/tests/`:** `bbn_from_store` on a mixed store (a temporary SQLite store, written as its tests write
  theirs) finds each model's history by `--model` and `--n`, and never the other's; under `one_loop` it matches the
  model's own drain model; the refusal at parse. A test that runs PRyMordial says so in its docstring.
- **Existing tests.** Some pin today's label, format or maps (`test_dissipation_store`'s version test,
  `test_fingerprint_format_guard`, `test_store_fingerprint_golden`, `test_compare_stores` 11 and 13, the inventory's).
  Change only what the new label, format, class or map requires, and name each change in the log with its old and new
  value. **No suite's count may fall.**

## 2. The Store gate (README §2.1)

### 2.1 The exponential is unchanged

- The goldens pass bitwise, unchanged: `test_golden_history`, `test_golden_reflecting_history`, `test_tuning`; and
  `test_tuned_history_round_trip`, `test_drain_store` and `test_dissipation_store` pass with no change but the label and
  format literals.
- The exponential's tool route at laboratory M, β = 2, A = 1 (`tools/history_and_bbn.py 2.0 4e-31 --drain-law one_loop
  --alpha 1`) is identical to 04's `.documents/power-law-wall/sample-04/tool-route/exp-M4e-31-b2.0-oneloop1.txt` with the
  timings masked.
- `tools/bbn_from_store.py` and `tools/outward_step_scan.py` give, on an exponential store, the output they gave before,
  under their existing tests.

### 2.2 The round trip

The tests of §1.7 pass: both models' histories stored and read back exactly, never confused, the key's drain model
checked at entry, and the inventory and `compare_stores` reading a mixed store.

### 2.3 The fingerprint

`SPEC_DIGESTS[8]` and the golden at 8 pass, formats 1–7's pins are unchanged, and the golden's diff is §1.6's: the
format, the empty class and the top digest. Say which spec digests moved and why.

## 3. What is not wanted

- **No driver option and no export.** `main.py`, `plot_by_beta.py`, `plot_ScalarHistory.py`, `plot_inputs.py`,
  `extract_common.py`, `pipeline_selection.py`, `config/argument_parser.py` and `exponential.yaml` are 06's.
  `model_of_law` and `DRAIN_MODEL_ONE_LOOP`'s other call sites are not changed.
- **No value of the integration moves**, for either model: no change to the step loop, the law, the tuning or the
  model classes' hooks. The one change to `ComputeTargets/ScalarHistory.py` is §1.4's check.
- **No migration** of a format-7 store and no reader for one. No store opened but a test's temporary one.
- **No change** to `ObjectFactories/ExponentialModel.py`, to the exponential's spec, or to the golden store's rows.
- No Ray, no cluster, no rehearsal (07), no merge, no push. `PRyM/`, `thirdparty/` and the packages are not touched. If
  DatastoreKit's interface blocks the table or the map, stop and ask; do not work around it (`CLAUDE.md`, "The
  packages").

## 4. Verification

- **The five suites** at the base and after, per `CLAUDE.md` (ComputeTargets in its two halves, `tools/tests` alone),
  from the worktree root, serially, with the time and the load at each start. The two worktree failures of note 01's
  ruling 7 are expected at both; any other failure is a stop.
- **The Store gate** item by item (§2), each with the command and its output.
- `black` on every Python file changed or added; `git diff --stat` shows nothing outside §5's files.

## 5. The commit, the log and the board

One commit (invariant 1), with:
- `ObjectFactories/InversePowerModel.py`; `config/datastore.py`, `config/sharding.py`, `config/fingerprint.py`,
  `config/version.py`; `ObjectFactories/ScalarHistory.py`; the comments of §1.3 (`ObjectFactories/AdiabaticHistory.py`,
  `ObjectFactories/BBNData.py`, `CosmologyConcepts/Models/model_ids.py`); `ComputeTargets/ScalarHistory.py` (§1.4 only);
  `tools/compare_stores.py`, `tools/inventory_report.py`, `tools/bbn_from_store.py`, `tools/outward_step_scan.py`;
- the tests, new and changed, and `ObjectFactories/tests/data/store_fingerprint_power-law-wall-05.json`;
- `CLAUDE.md`'s test counts and the date of the last change to a count, and nothing else in it;
- the log `prompts/power-law-wall/logs/05-the-store.md` (template: `dk-and-rpk-migration` README §5.1), with every
  deviation classified, every changed test listed with its old and new value, "Observations not acted on", and "State
  handed to the next prompt": the schema 06 reads, the label and format, the drain-model check, and what 06's drivers
  must do for the power law that the store now allows;
- the board: row 05, its items, the gate Store, the baselines, and §3/§4 for any issue opened, with
  `.documents/OPEN_ISSUES.md` in the same commit.

Stage by explicit path. Never stage `venv`. The commit message follows `CLAUDE.md` invariant 5. Write "this commit"
where the SHA goes; the orchestrator substitutes it.
