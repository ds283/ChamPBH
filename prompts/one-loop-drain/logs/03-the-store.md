# Log 03 — the store

**Subject:** Store the one-loop drain law: the drain-model key, bounce rows and masses
**Commit:** `fe4c917`, on branch `one-loop-drain`, on top of `d3069cb`
**Date:** 2026-11-06, by Claude Opus 5.5
**Result:** **COMPLETE.** The Store gate holds (`.documents/one-loop-drain-verification.md` §3.5).
- **The key.** `alpha_value` is the pair (value, `drain_model`): `"fixed"` (α) or `"one-loop-measured-v1"` (A). A lookup that
  names no model is `"fixed"`, so the drivers are unchanged. The constants and `check_drain_law` moved to the leaf module
  `CosmologyConcepts/drain_model.py`; `alpha_value` has a factory of its own.
- **The bounce rows.** Under the one-loop law a history stores a row per candidate and per turning point of the reflection
  margin that is not one, in the table `ScalarHistoryBounce`, and its drain record (regime shares and counts, ℓ_min, the
  least ln R at a drain, the all-generation margins, the row count) as columns of `ScalarHistory`. Under the fixed law and
  at α = 0 there is no row and the record is NULL.
- **The sample masses** ln m_eff and ln m_tr are stored at every sample under every law.
- **The guard.** `store()` refuses a payload whose drain model is not the key's.
- **No value of the integration moves**: three fixed-law payloads equal the parent's key by key but for the three new keys;
  the goldens pass.
- **Labels:** `FINGERPRINT_FORMAT` 7 (only `alpha_value`'s spec digest moved), `VERSION_LABEL` 2026.10.0.
- **Cost:** median added time 0.0104 of the integration on six of 02d's histories (stop above 0.25), largest 0.254.
- **Issues:** two closed (`[00-weighted-margins-hide-the-least-relativistic-generations]`,
  `[00-fixed-alpha-drain-overstates-late-reflections]`); none opened. Index: 31 → 29.
- **Suites:** before (on a worktree of `d3069cb`): 26, 594 (162 + 432), 130, 156, 68. After: 26, 622 (190 + 432), 149, 156, 68, all passing.

Orchestrator note 03 (§0, rulings 1–12 and the conventions) governed where it differs from the prompt. Each ruling is
classified in §2.

## 1. What shipped

- **`CosmologyConcepts/drain_model.py`** (new, ruling 1): `DRAIN_LAW_FIXED`, `DRAIN_LAW_ONE_LOOP`, `DRAIN_LAWS`,
  `DRAIN_MODEL_FIXED`, `DRAIN_MODEL_ONE_LOOP`, `DRAIN_MODELS`, `law_of` (the one model → law mapping; an unknown model is a
  `ValueError`), `check_drain_law` (moved verbatim; messages unchanged) and `check_drain_model` (the pair). It imports only
  `math`. The comment above the model constants says the version moves whenever f, its constants or the S table change.
- **`CosmologyConcepts/alpha_value.py`**: `alpha_value(store_id, value, drain_model="fixed")`, checking the pair on
  construction; the property `drain_law`.
- **`ObjectFactories/alpha_value.py`** (new): `sqla_alpha_value_factory`, columns `value` and `drain_model` (String, not
  null, indexed), spec leaves `("value", "drain_model")`; `build` checks the pair before it queries or inserts, matches the
  value as the dimensionless factory does (relative, or absolute at 0) and the model exactly, with `"fixed"` where the
  payload names none; `read_table` returns both.
- **`ComputeTargets/ScalarHistory.py`**:
  - imports the names above from the leaf module (every `SM.<name>` the same object); the law's fitted constants, the S
    table and the long comment stay, with pointers;
  - `sample_masses(model, cosmology, sample)` (ruling 2): two tuples of Python floats or None;
  - `MarginInputs` and `margin_inputs` (ruling 4): `history_margins`' gathering, moved unchanged; `history_margins` is now
    `relativistic_margins(*margin_inputs(...))`;
  - `BounceRow`, `BounceRecord`, `BOUNCE_SOURCE_*`; `ledger_K_before` (ruling 6); the pure builder `bounce_rows`; and
    `bounce_record`, which computes from the dense output and the ledger what the builder needs (rulings 4–7);
  - `DrainHistoryValues`, `all_generation_margins` (P30), `drain_history_values` (ruling 8);
  - `integrate_history`: the keyword `compute_sample_masses=True`; it calls the gatherer and the core itself, keeps the
    inputs, and puts `"bounces"` (a `BounceRecord` under the one-loop law, None otherwise) and, unless turned off,
    `"log_m_eff"` and `"log_m_tracker"` on a success's payload;
  - `tune_history`: `compute_sample_masses=False` on each integration; the masses once on the converged payload, with
    the converged `rm`;
  - `compute_scalar_history`: `drain_law`, default fixed, passed to `tune_history`;
  - `ScalarHistory`: `compute` passes `self._alpha.drain_law`; `store()` has the guard (ruling 3), refuses a payload with
    samples but without the masses, and sets `drain_history` and `bounces`; the properties `drain_history` (readable with
    `_do_not_populate`) and `bounces` (not); the payload keys `"drain_history"` and `"bounces"` of the constructor;
  - `ScalarHistoryValue`: keyword arguments `log_m_eff`, `log_m_tracker` (default None) and their properties.
- **`ComputeTargets/dark_radiation.py`**: the read-only accessor `DarkRadiationLedger.cumulative_K` (ruling 6).
- **`ObjectFactories/ScalarHistory.py`**: ruling 9's helpers (`log_unit`, `to_column`, `from_column`, the kinds
  `LOG_MP`, `LOG_MP4`, `LOG_GEV`, `LINEAR_MP4`), `BOUNCE_COLUMNS`, `MASS_COLUMNS`, `bounce_row_columns`,
  `bounce_row_from_columns`, `_drain_history_columns`, `_drain_history_from_row`; the history's drain-record columns, the
  value table's two mass columns; the bounce rows written in `store`, read in `build` with the samples (in ordinal order,
  counted against `bounce_rows`), counted in `validate`, pruned in `validate_on_startup`; and
  `sqla_ScalarHistoryBounce_factory`, with no inventory spec.
- **`config/datastore.py`**: `alpha_value`'s own factory; `ScalarHistoryBounce` after `ScalarHistoryValue`, in
  `scalar-history` before `ScalarHistory`, batch size 250; the docstring's count of quantity entries (five).
  **`config/sharding.py`**: `ScalarHistoryBounce` sharded by `shard_key`. **`config/fingerprint.py`**: format 7 with its
  history and paragraph, `SPEC_DIGESTS[7]` generated. **`config/version.py`**: 2026.10.0 with its paragraph.
- **Tests (new):** `ComputeTargets/tests/test_drain_store_records.py` (28), `ObjectFactories/tests/test_drain_store.py`
  (19); the golden file `ObjectFactories/tests/data/store_fingerprint_one-loop-drain-03.json`.
- **Scripts:** `.documents/one-loop-drain/store-03/` (`fixed_law_payloads.py`, `compare_fixed_law.py`, `cost_03.py`,
  `launch_03.sh`; `out/fixed_law.txt`, `out/cost.txt`).
- **Documents:** verification §3; dated notes in `architecture-summary.md` (§7.2, after 02c's) and
  `numerical-strategies.md` (§3.5, after 02c's); `CLAUDE.md` (the counts, and "Redshift"'s α sentence).

### 1.1 The bounce columns, as built (`ScalarHistoryBounce`)

| column | unit | null | source |
|---|---|---|---|
| `history_serial` | — | not null, indexed, FK | the history; the shard key is the history's |
| `ordinal` | — | not null | 0, 1, … in `relativistic_margins`' event order |
| `source` | — | not null | `candidate`, `plateau`, `wall` |
| `candidate_index` | — | null for an event | index in `injections` |
| `N` | — | not null | a candidate's drain N, an event's N |
| `N_turn`, `kind`, `resolved` | — | null for an event | `Injection` |
| `log_T_Jordan_GeV` | ln(T_J/GeV) | | ruling 7 |
| `delta_K_Mp4` | M_P⁴ (linear) | | `Injection` |
| `log_rho_kin_Mp4` | ln(ρ/M_P⁴) | | `Injection` |
| `nonadiabaticity`, `alpha_eff`, `w_gate` | — | | `Injection` |
| `K_before_Mp4` | M_P⁴ (linear) | | `ledger_K_before` |
| `regime`, `ell`, `L`, `log_R` | — | | `DrainCandidate` |
| `log_E_Mp4` | ln(E/M_P⁴) | | `DrainCandidate` |
| `log_H_Mp` | ln(H_E/M_P) | | `DrainCandidate` |
| `log_k_ex_Mp`, `log_m_Mp` | ln(·/M_P) | | `PeakEstimate`; for an unassessed candidate and an event, `log_m` is the field's own mass the gatherer used |
| `log_q_Mp` | ln(q/M_P) | | `Generation` |
| `share`, `weighted`, `birth_margin`, `reflection_margin`, `reflection_margin_N`, `tracker_margin`, `tracker_margin_N` | — | | `Generation` |

### 1.2 The history columns, as built (`ScalarHistory`, all nullable, all dimensionless)

`drained_share_one_loop`, `drained_share_gap`, `drained_share_classical`, `drained_share_unassessed`, `parking_share`
(Float); `candidates_one_loop`, `candidates_gap`, `candidates_classical`, `candidates_unassessed` (Integer); `ell_min`,
`log_R_min_drain` (Float); `unresolvable_assessments`, `unresolved_declined` (Integer); `all_worst_birth_margin`,
`all_worst_birth_margin_N`, `all_birth_failing_share`, and the same for `reflection` and `tracker` (Float); `bounce_rows`
(Integer). `ScalarHistoryValue`: `log_m_eff_Mp`, `log_m_tracker_Mp` (Float, nullable; ln(m/M_P)).

## 2. Deviations from the prompt

The note's rulings, each as followed:
1. **The key's home** — `CosmologyConcepts/drain_model.py`. *STRUCTURALLY REQUIRED* (the import cycle); the name *IMPLEMENTATION
   CHOICE*. `law_of` and `check_drain_model` are added there too (the mapping and the pair, one statement each).
2. **The masses: one function, two callers** — `sample_masses`; `integrate_history(compute_sample_masses=True)`, which only
   `tune_history` passes as False. *STRUCTURALLY REQUIRED.* `integrate_history` alone computes them (the prompt left it open).
3. **The guard, made exact** — as ruled, before anything is set on the object (it runs before `_compute_ref` is cleared).
   *IMPLEMENTATION CHOICE* (ruled). The exception is `RuntimeError`, as the ratio's refusal.
4. **The bounce key** — `history_margins` split into `margin_inputs` + the core; `integrate_history` calls both; the payload
   key `"bounces"` is a `BounceRecord(rows, K_final)`, None under the fixed law, absent on a failure. *IMPLEMENTATION CHOICE*
   (ruled). K_final is carried in the record so that `store()` forms the shares from the payload alone.
5. **Where the margins fail** — no event row; candidate rows in full but the Generation columns and an unassessed
   candidate's `log_m_Mp`; the `all_*` columns NULL. *IMPLEMENTATION CHOICE* (ruled). Tested on constructed inputs; it did not
   arise at §2.4's six (margins computed at all).
6. **K before the candidate** — the walk by position over the ledger's entries (one per candidate with α_eff > 0, which is
   when the drain adds one, plus the leftover after the cut candidate), with `DarkRadiationLedger.cumulative_K`. *As the
   prompt intends; made exact.* The walk checks each entry's (N, ΔK) against the candidate's and that every entry is used,
   raising RuntimeError otherwise. Note: the walk keys on α_eff > 0, the drain's own condition, not ΔK > 0 (ΔK can
   underflow where α_eff > 0).
7. **The row's ln T_J** — as ruled; on §2.4's six: 73 from the wall-side `TurningPoint` (C-3233), 9 346 from the dense
   output, 9 414 events from their `TurningPoint`. *As the prompt says; made exact.*
8. **The shares** — NULL where K_final = 0, otherwise as `drain_values` forms them; equal to 02d's `drain_values` by `repr`
   at §2.4's six. *IMPLEMENTATION CHOICE* (ruled).
9. **The units** — one helper per kind in `ObjectFactories/ScalarHistory.py`, used by the store, the read-back and the tests;
   no nonzero value converted to 0.0 or ±∞ in the tests or §2.4. *As the prompt says; made exact.*
10. **§2.4, made exact** — as ruled; the six named in verification §3.4. *As the prompt says; made exact.* "Added time" is
    the bounce key + masses + record + (full write and read − base write and read), the base being the same history with
    no bounce key and NULL masses: what this prompt adds to a store's write and read, not the write and read a fixed-law
    history already had (P12, "the added time"). *IMPLEMENTATION CHOICE*; the whole write and read counted instead gives a
    median of 0.020 (both reported).
11. **Nothing in `tools/`, `main.py` or the plot scripts changes** but what a test forces — followed: one tool,
    `tools/outward_step_scan.py`, changes, because its own tests failed on the factory's new `store` (§2.1), and three
    `tools/tests` modules change, forced (§2.1 below).
12. **Files** — followed; the commit holds the prompt's §5 files, the leaf module, `ObjectFactories/alpha_value.py`,
    `ComputeTargets/dark_radiation.py` and the forced test changes (§2.1). *STRUCTURALLY REQUIRED*; followed.

Further deviations:
- **`ScalarHistory`'s spec digest did not move**, where the prompt (§2.3) expected "at least `alpha_value`'s and
  `ScalarHistory`'s". Its spec is unchanged (the parent `alpha` by the same column and table; no new column is named), so
  only `alpha_value`'s spec digest moved. In the store fingerprint, `ScalarHistory`'s class digest (and `AdiabaticHistory`'s
  and `BBNData`'s) did move, through the key of its parent. *STRUCTURALLY REQUIRED* (nothing to move it but a change the
  prompt does not ask for).
- **`ScalarHistoryValue`'s mass arguments default to None**, unlike the ratio's required keyword: `tools/history_and_bbn.py`
  and `tools/outward_step_scan.py` build `ScalarHistoryValue`s from payloads and never store them, and ruling 11 keeps them
  unchanged. `store()` still refuses a payload with samples and no masses. *STRUCTURALLY REQUIRED* (ruling 11).
- **`sample_masses` returns Python floats** (`float()` of each, exact): a realization from `tune_history`'s secant carries
  numpy floats, which made the tuned payload's masses `np.float64` and its `repr` differ from the direct one's.
  *IMPLEMENTATION CHOICE.*
- **`ScalarHistoryBounce` is in `test_inventory_specs.EXCLUDED`**, with P29's reason: the coverage test requires a registered
  class to have a spec, be read through one, or be excluded. *STRUCTURALLY REQUIRED* (P29).
- **The "before" suites were run on a scratch worktree of `d3069cb`.** A first chain on the main checkout was stopped: edits
  had started while it ran, so its later suites imported changed code (its ObjectFactories run errored mid-edit). In the
  worktree, 10 `tools/tests` and 1 `RunDrivers/tests` tests failed for the worktree's own reasons (no `./venv` there; the
  repository root and the run registry's root being the worktree; one batch-timeout test at a load of 41–81); with `venv`
  linked into the worktree they were re-run (§4). *UNINTENDED DRIFT* in procedure (the first chain), corrected; no result
  depends on it.

### 2.1 Existing tests changed (forced)

- `ComputeTargets/tests/test_drain_law.py` (3): the fixed law's payload keys gain `"bounces"` (asserted None),
  `"log_m_eff"`, `"log_m_tracker"`.
- `ComputeTargets/tests/test_margins.py` (8): `"log_m_tracker"` added to the keys skipped, since the masses call the patched
  `tracker_log_m` too (it is None at every sample of the patched run).
- `ComputeTargets/tests/test_gate_and_taper.py` (1) and `ComputeTargets/tests/test_parking.py` (1): each counts calls of a
  model function during a golden history at α = 0 and asserts none (`log_m_eff_squared`; `minimum_on_trace` and
  `d_minimum_d_log_trace`). The sample masses call the first two at every sample, at every α by design (U5), after the
  integration: 5 446 and 2 723 calls. Both tests now patch `sample_masses` to a stub returning NULLs for the spied run,
  so the count is again the integration's; nothing else in them changes.
- **`tools/outward_step_scan.py`** (`store_rows`) and its tests: the tool mirrors `ScalarHistory.store()` with a duck-typed
  object and runs the factory's `store` with capturing inserters; the factory now reads `_drain_history` and `_bounces`
  and writes the masses, so its `TestTheStoreComparison` (5 tests) errored. The mirror gains `_drain_history`
  (`drain_history_values(data)`), `_bounces=()` and the masses from the payload (None where it has none), as `store()`
  sets them. The factory looks up the bounce inserter only when it has rows to write (so a caller with none need not
  supply it). No tool test changed.
- `ObjectFactories/tests/test_dissipation_store.py` (10): `VERSION_LABEL` 2026.10.0, `FINGERPRINT_FORMAT` 7.
- `ObjectFactories/tests/test_store_fingerprint_golden.py`: `GOLDEN` names the new file; the one `alpha_value` row is written
  with `drain_model="fixed"`; the command line's format literal 7; the docstring's format-7 paragraph.
- `ObjectFactories/tests/test_datastore_config.py`: the registry (`alpha_value`'s factory, `ScalarHistoryBounce`), the drop
  group and the batch sizes.
- `ObjectFactories/tests/test_inventory_specs.py`: 13 definitions serve 16 classes (the test renamed
  `test_thirteen_definitions_serve_sixteen_classes`), `alpha_value`'s leaves; `EXCLUDED` holds `ScalarHistoryBounce`, and
  the coverage and stale-exclusion tests take it into account.
- `ObjectFactories/tests/test_version_keyed_lookups.py`: the stand-in `_alpha` gains `drain_model="fixed"`, which the guard
  reads (`test_first_bounce_round_trip` and `test_fixed_T_values_round_trip` use it).
- `tools/tests/test_compare_stores.py`: the raw `alpha_value` insert gains `drain_model="fixed"` (NOT NULL).
- `tools/tests/test_inventory_report.py`, `tools/tests/test_inventory_alpha.py`: the `alpha_value` inserts gain
  `drain_model="fixed"`, and the expected report lines follow the unchanged report on the new spec (`- value=0,
  drain_model=fixed`; `alpha=alpha_value(value=0, drain_model=fixed)`).

No count fell: the renamed test replaces its predecessor one for one.

## 3. The Store gate

Item by item in verification §3.5: (1) the fixed law unchanged, §3.2 (`out/fixed_law.txt`); (2) the round trip, §3.3; (3) the
fingerprint, §3.3; (4) the cost and the volume, §3.4 (`out/cost.txt`; median 0.0104).

## 4. Verification performed

**The suites**, serially, each chain in the background. Before any change, on a scratch worktree of `d3069cb` (a first
chain on the main checkout was stopped, since my edits had begun while it ran):

| suite | count | time | load at start |
|---|---|---|---|
| CosmologyModels | 26 OK | 109.0 s | 65.25 23.89 13.40 (00:45) |
| ComputeTargets `test_[a-f]` | 162 OK | 176.9 s | 58.05 35.90 19.54 (00:47) |
| ComputeTargets `test_[g-z]` | 432 OK | 494.7 s | 37.21 45.42 27.24 (00:50) |
| ObjectFactories | 130 OK | 129.5 s | 13.31 16.48 19.62 (00:59) |
| tools | 156; 155 OK, 1 failing for the worktree's root (`test_registry_config`) | 163.8 s | 4.73 17.75 26.24 (01:13) |
| RunDrivers | 68; 67 OK, 1 failing for the worktree's root (`test_registered_run`, the script's path) | 25.0 s | 7.49 12.83 22.61 (01:16) |

The tools and RunDrivers rows are a second run with the checkout's `venv` linked into the worktree; in the first, ten
`tools/tests` failed for the worktree's lack of `./venv` and one for a batch timeout at a load of 41–81. The two that fail
in the worktree read the repository root (the run registry's `var/runs`, the script's path), and pass in the main
checkout's after-chain, unchanged.

After, on `fe4c917`'s tree (a first after-chain at 01:16–01:34 failed `test_gate_and_taper` and `test_parking`'s α = 0
call-count tests and errored `test_outward_step_scan`'s five store-comparison tests, which led to §2.1's last three
changes; this second chain is the record):

| suite | count | time | load at start |
|---|---|---|---|
| CosmologyModels | 26 OK | 63.2 s | 6.38 7.11 12.08 (01:36) |
| ComputeTargets `test_[a-f]` | 190 OK | 141.0 s | 4.94 6.52 11.52 (01:37) |
| ComputeTargets `test_[g-z]` | 432 OK | 459.5 s | 6.11 6.20 10.59 (01:40) |
| ObjectFactories | 149 OK | 118.4 s | 4.12 4.66 7.93 (01:47) |
| tools | 156 OK | 159.1 s | 4.54 4.63 7.48 (01:49) |
| RunDrivers | 68 OK | 22.8 s | 4.05 4.43 6.91 (01:52) |

ComputeTargets goes from 594 to 622 (`test_drain_store_records`, 28, in the `test_[a-f]` half) and ObjectFactories from 130
to 149 (`test_drain_store`, 19). No count fell.

The scratch worktree was removed afterwards (`git worktree remove`).

**The goldens** (`test_golden_history`, `test_golden_reflecting_history`, `test_tuning`) are unchanged files and pass in the
after-chain's `test_[g-z]` half.

**§2.1, the fixed law, key by key**: `fixed_law_payloads.py` at the worktree of `d3069cb` and at this tree;
`compare_fixed_law.py`: every base key equal by `repr` in three runs (30, 30, 45 keys), new keys only `"bounces"` (None)
and the masses.

**§2.4**: six detached runs launched at 01:02 (`launch_03.sh`, pids recorded), one check that all six were alive, a
background wait on the six end markers, all ended by 01:13; none alive afterwards (`kill -0`). `cost_03.py report` holds.

## 5. The deliberate-breakage record

- The guard: a one-loop payload under a fixed key, a fixed payload under a one-loop key, and a missing record under a
  one-loop key each raise and store nothing (`test_drain_store.TestTheGuard`).
- `validate` returns False after one bounce row of a validated history is deleted (`test_the_rows_are_in_ordinal_order_and_validated`).
- `ledger_K_before` raises on a ledger whose entries do not match the candidates (a parking leftover left out, or
  candidates missing).
- The refusals of `alpha_value` (A = 0, A < 0, A = ∞, α ≥ 1, an unknown model) on construction and in the factory, with no row
  written.
- The first after-chain (log §4) failed the two α = 0 call-count tests and errored `test_outward_step_scan`'s five
  store-comparison tests, which led to the changes of §2.1; the second chain passed.
- Before `_alpha` gained `drain_model`, six round-trip tests of `test_first_bounce_round_trip` and
  `test_fixed_T_values_round_trip` failed at the guard (AttributeError), and the coverage test named `ScalarHistoryBounce`
  until it was excluded: each was seen failing first.

## 6. Observations not acted on

- **The inventory does not count bounce rows.** DatastoreKit's `InventorySpec` names a single value table per owner
  (`values=`), which `ScalarHistory`'s spec gives to `ScalarHistoryValue`; `ScalarHistoryBounce` declares no spec and is
  excluded in `test_inventory_specs` (P29). A store's fingerprint therefore does not see bounce rows. Recorded, not worked
  around (`CLAUDE.md`, the packages); a second value slot would be DatastoreKit's change.
- **The raw-SQL readers of `alpha_value`** — `tools/bbn_from_store.py:222` and `tools/outward_step_scan.py:848` join
  `alpha_value` on `alpha_serial` and read `value` only; on a store with both models they would not tell them apart. Before 04
  no driver can write a one-loop row. For 04 (ruling 11).
- **`tools/inventory_report.py`** prints an `alpha_value` row as `- value=0, drain_model=fixed` and a history's α as
  `alpha=alpha_value(value=0, drain_model=fixed)`, where it printed `values = [ 0 ]` and `alpha=0`: the report follows the
  spec's two leaves. Its tests were changed to the new lines; a compact label (for example `alpha=0` under the fixed model,
  `A=1 (one-loop-measured-v1)` under the other) is 04's to decide.
- **The guard checks the model, not A.** A one-loop payload computed at one A and stored under a key of another A passes
  `store()`. The stored path cannot do this (the key's value is what `compute` passes as A); a hand-built payload could.
- **A one-loop payload without `"bounces"`** (hand-built) is stored with no bounce row and a NULL drain record; `store()` does
  not refuse it. Every payload `integrate_history` returns carries the key.
- **The masses' cost** is the largest of the added parts at laboratory M: about 0.2–0.4 s per history (two model calls at
  each of about 5 000 samples), 0.14 of P-2197's 2.6 s integration under load. `tracker_log_m` at the samples after N_late
  is computed twice (the margins' tracker margin and the masses), with the same inputs; reusing the margins' values would
  save a fraction of it.
- **Bounce rows are about twice the candidates** at every one of §2.4's histories: each candidate is followed by a plateau
  turning point. A history of 13 343 candidates (02d's largest) would store about 26 700 rows.
- **`tools/outward_step_scan.py` against a store made before this prompt**: its store comparison reports a column the
  rebuilt row has and the stored row lacks as `missing_in_store`, so on a 2026.9.0 store the new drain-record columns
  (None under the fixed law) and, for the values, the mass columns would make a history read as not bitwise. Its tests
  build their stores with this code and pass; a scan of an old store would need those columns excluded. Not acted on (no
  scan is run here; ruling 11).
- **The prompt's §2.3** expected `ScalarHistory`'s spec digest to move; it is the store-fingerprint class digest that moves
  (§2).

## 7. State handed to the next prompt

**For 04 (drivers and exports):**
- **The key.** `alpha_value` is (value, `drain_model`). `drain_model` ∈ `CosmologyConcepts.drain_model.DRAIN_MODELS` =
  (`"fixed"`, `"one-loop-measured-v1"`); value is α under `"fixed"` (0 ≤ α < 1) and A under the one-loop model (finite,
  > 0); `law_of(model)` gives the law (`"fixed"`, `"one_loop"`) and `alpha_value.drain_law` the same. Look up with
  `pool.object_get("alpha_value", value=a, drain_model=m)`; without `drain_model` it is `"fixed"`. `ScalarHistory.compute`
  passes the key's law to `compute_scalar_history`, which passes it to `tune_history`. A plot script that matches stored keys
  must match the pair (and refuse a model with no row, as `--alpha-values` refuses an α).
- **The history columns** (`ScalarHistory`, dimensionless, NULL under the fixed law, at α = 0 and on a failure):
  `drained_share_{one_loop,gap,classical,unassessed}`, `parking_share`, `candidates_{one_loop,gap,classical,unassessed}`,
  `ell_min`, `log_R_min_drain`, `unresolvable_assessments`, `unresolved_declined`, `all_worst_{birth,reflection,tracker}_margin`,
  `all_worst_{…}_margin_N`, `all_{…}_failing_share`, `bounce_rows`. Read back as `ScalarHistory.drain_history`
  (`DrainHistoryValues`, the same names; readable with `_do_not_populate`).
- **The bounce rows**: `ScalarHistory.bounces`, a tuple of `ComputeTargets.ScalarHistory.BounceRow` in ordinal order, in
  the cosmology's units (§1.1 gives each column's stored unit; `ObjectFactories.ScalarHistory.BOUNCE_COLUMNS` maps field,
  column and conversion, and `to_column`/`from_column` convert). Not read under `_do_not_populate`; empty under the fixed
  law.
- **The sample masses**: `ScalarHistoryValue.log_m_eff`, `.log_m_tracker` (ln m in the cosmology's units, None where not
  defined); columns `log_m_eff_Mp`, `log_m_tracker_Mp`.
- The raw-SQL readers and the inventory report's α label (§6).

**For 05 (the derivation tool)**, from `bounces.csv` and `fields.csv` as 04 exports them: n_g needs `delta_K_Mp4` and
`log_q_Mp` (and `share`); E(N) needs the sample masses (`log_m_eff_Mp`, `log_m_tracker_Mp`) with the samples' N; the phase
slip needs `log_k_ex_Mp`, `log_H_Mp` and `N` (`N_turn`); the backreaction ratio needs `log_m_Mp` and `log_E_Mp4` (with
`delta_K_Mp4` and `log_q_Mp` for n_χ a³). Under decision 3 the classical regime drains R·S(L) and its rows carry it as
`alpha_eff`; no separate bound is needed. `K_before_Mp4` gives K before each candidate. No column 05 needs is missing.
