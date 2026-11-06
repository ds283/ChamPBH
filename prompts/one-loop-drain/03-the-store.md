# Prompt 03 — the store

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-06, on branch `one-loop-drain` in the main checkout, after 02d landed and was reviewed
(`98b1601`, `f830085`). It is written from:
- README §8.3 (the measures and where each is computed), §8.6 (U5, U8, U12, P2, P7–P9, P12), §8.10 (decision 1, the
  key) and §8.11 (the review before this prompt, and P27–P32);
- 02d's handover (`.documents/one-loop-drain-verification.md` §2d.11) and log 02d, "Observations not acted on";
- `inelastic-reflection` prompt 09 and its tests (`ObjectFactories/tests/test_dissipation_store.py`), which stored
  dissipation mode and are this prompt's precedent;
- the tree's code at the commit that adds this prompt.

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Where it runs.** In the main checkout `/Users/ds283/Documents/Code/ChamPBH`, on branch `one-loop-drain`, from the
repository root, with `PYTHONPATH=.` and `./venv/bin/python`. Outputs go under `var/one-loop-drain/03/`.

**Gate:**
- on branch `one-loop-drain`, at or after the commit that adds this prompt, with no tracked change in `git status`;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- the suites stand at the counts of `CLAUDE.md` (26, 594 = 162 + 432, 130, 156, 68), all passing.

If any of these fails, stop and ask.

**Closes:**
- `[00-weighted-margins-hide-the-least-relativistic-generations]` (board §3): the all-generation margins and the
  generations are stored under the one-loop law. Under the fixed law the record stays as it is, and 08 documents that.
- `[00-fixed-alpha-drain-overstates-late-reflections]` (board §3): the law's key is stored.

**Narrows:** nothing. **Opens:** only what the work finds.

**Recommended model:** **Opus.** It changes the schema, a key of every history, and the fingerprint, and it must
leave every fixed-law value bitwise as it is.

**Read first:**
1. [`README.md`](README.md) §8.3, §8.6 (U5, U8, U12, P2, P7–P9, P12), §8.10 (decision 1) and §8.11 whole.
2. The board's §3: the two entries this prompt closes.
3. `ComputeTargets/ScalarHistory.py`:
   - the law's constants and `DRAIN_MODEL_FIXED`, `DRAIN_MODEL_ONE_LOOP`, the regimes (:150–:280);
   - `Injection`, `TurningPoint`, `DrainCandidate`, `DrainRecord` and their comments (:430–:746);
   - `check_drain_law`, `check_drain_law_model`, `drain_law_inputs` (:1736–:1825);
   - `PeakEstimate`, `MarginCandidate`, `MarginEvent`, `Generation`, `Margins` (:3932–:4066), `relativistic_margins`
     (:4186) and `history_margins` (:4360);
   - `DissipationValues`, `dissipation_values` (:3824–:3915), `tracker_log_m` (:4141), `field_log_m` (:4166);
   - `integrate_history`'s payload (:4834–:4905), `tune_history`'s success payload (:5345–:5370),
     `compute_scalar_history` (:5421), `build_extra_data` (:5479);
   - `ScalarHistory` (:5548): `__init__`, `compute`, `store`; `ScalarHistoryValue` (:6058).
4. `ComputeTargets/dark_radiation.py`: `DarkRadiationLedger` (`K`, `injections`, `K_initial`).
5. `ObjectFactories/ScalarHistory.py` whole: the unit conversions (:56–:118), the three factories, `inventory_spec`,
   `build`, `store`, `validate`, `validate_on_startup`.
6. `CosmologyConcepts/alpha_value.py`, `ObjectFactories/DimensionlessQuantity.py`.
7. `config/datastore.py` (`factories`, `drop_groups`, `serial_batch_sizes`), `config/sharding.py`,
   `config/fingerprint.py` (`FINGERPRINT_FORMAT`, `SPEC_DIGESTS` and their histories), `config/version.py`.
8. The tests: `ObjectFactories/tests/test_dissipation_store.py`, `test_fingerprint_format_guard.py`,
   `test_store_fingerprint_golden.py`, `test_datastore_config.py`, `test_inventory_specs.py`, and `_TempStoreCase` in
   `test_version_keyed_lookups.py`; `ComputeTargets/tests/test_drain_law.py`, `TestTheOneLoopLawOn2197`.

---

## 0. Where this prompt sits

The one-loop law is in the code (01, 02c) and measured on the sample (02d), but nothing of it can be stored: no key says
which law a history was built under, and the instrumentation is dropped at the store. This prompt stores both.

It changes no value of the integration. It adds a key, a table, columns and payload keys. The drivers' options and the
exports are 04's. The spectrum-dependent measures are 05's, computed from what this prompt stores.

## 1. What is wanted

### 1.1 The key (U8, decision 1; P27)

- **`alpha_value` gains `drain_model`**, a string, beside its value. A row is the pair (value, drain_model), unique.
  - Its values are `DRAIN_MODEL_FIXED` (`"fixed"`) and `DRAIN_MODEL_ONE_LOOP` (`"one-loop-measured-v1"`), the
    code's current constants, and nothing else. An unknown model is a `ValueError` before any row is written.
  - The value is α under `fixed` (0 ≤ α < 1, α = 0 natural mode) and A under the one-loop model (finite, > 0). These
    are `check_drain_law`'s rules, applied to the pair. Keep one statement of the rules, and do not copy them.
  - The law follows from the model by one mapping: `fixed` gives `DRAIN_LAW_FIXED`, and the one-loop model gives
    `DRAIN_LAW_ONE_LOOP`.
  - Where the constants live must not create an import cycle between `CosmologyConcepts` and `ComputeTargets`. If one
    moves, say where in the log.
- **A lookup that names no model asks for `fixed`.** So `object_get("alpha_value", value=a)` is today's lookup, and
  `main.py`, `plot_inputs.py`, `tools/` and `RunDrivers/` are unchanged. 04 gives the drivers their option.
- **The factory.** `alpha_value` stops sharing `sqla_dimensionless_quantity_factory` with `beta_value`. Its factory and
  `inventory_spec` carry the pair, and `beta_value`'s are unchanged. The table stays replicated, and its batch size
  stays 200.
- **The comment.** Above the constants, say that the version moves whenever f or its constants change, so that a new
  version is a new key value.

### 1.2 The stored path

- **`compute_scalar_history` gains `drain_law`**, passed to `tune_history`, default `DRAIN_LAW_FIXED`. With the default
  it is today's call, bitwise.
- **`ScalarHistory.compute`** passes the law that its `alpha_value`'s model gives. It gains no constructor argument,
  since the key is on `alpha`.
- **The guard (P28).** `ScalarHistory.store()` raises on a success payload whose `"drain"` record's `model` is not the
  key's `drain_model`, before anything is stored. A failure payload is stored as today.

### 1.3 The bounce rows (§8.3; P6, P7 as revised; P29)

**The table `ScalarHistoryBounce`**, written under the one-loop law only. The fixed law (α > 0 or α = 0) and a failure
write no row.

- **Rows.** There is one row for each candidate (`Injection`), ΔK = 0 included. There is also one for each turning
  point of the reflection margin that is not a candidate: each plateau turning point, and each wall-side turning point
  with no candidate. These are `history_margins`' `MarginEvent`s, with their `source`.
- **Order.** Rows are in `relativistic_margins`' event order: by N (a candidate at its N_turn), stable, with a
  candidate before an event at the same N. `ordinal` counts 0, 1, … in that order.
- **Where the rows come from.** A payload key (`"bounces"`, or your name) is built in `integrate_history` beside
  `"margins"`, from records it already holds: the injections, the `DrainRecord`'s candidates, the reused
  `PeakEstimate`s, the generations, the events, the turning points and the ledger.
  - The function that builds it is pure, on plain values, so that it is tested on constructed records.
  - `history_margins` computes the events but does not return them. Expose them so that `Generation`, `Margins` and
    every existing return value stay bitwise as they are (for example through a function it calls, or a value it
    returns beside them). Say which in the log.
  - No model call is added beyond those `history_margins` already makes.
- **Where `history_margins` fails** (its warning line; the history does not fail), the rows carry what does not depend
  on it, and NULL for the rest. Say in the log which fields those are, and whether event rows are written.

**The columns.** The names are proposals; rename only for clarity, and list every column in the log. A converted
column carries the unit suffix the sample table uses: `_Mp` for M_P, `_Mp4` for M_P⁴, `_GeV` for T_J in GeV. Every
column is nullable unless marked.

| column | source |
|---|---|
| `history_serial` (indexed, not null), the shard key | as `ScalarHistoryValue` |
| `ordinal` (not null), `source` (not null: `candidate`, `plateau`, `wall`) | the order above |
| `candidate_index` | the index in `injections`; NULL for an event |
| `N` (not null) | a candidate's drain N, or an event's N |
| `N_turn`, `kind`, `resolved` | the `Injection` |
| `log_T_Jordan_GeV` | ln T_J at N_turn, or at the event: the `TurningPoint`'s own where the row has one, else the dense output's. Say which in the log |
| `delta_K_Mp4`, `log_rho_kin_Mp4`, `nonadiabaticity`, `alpha_eff`, `w_gate` | the `Injection` |
| `K_before_Mp4` | the ledger's K in force just before the candidate (the ledger's own value, not a re-summation) |
| `regime`, `ell`, `L`, `log_R`, `log_E_Mp4`, `log_H_Mp` | the `DrainCandidate` |
| `log_k_ex_Mp`, `log_m_Mp` | the `PeakEstimate`, or for an unassessed candidate and an event, the field's own mass that `history_margins` used |
| `log_q_Mp`, `share`, `weighted`, `birth_margin`, `reflection_margin`, `reflection_margin_N`, `tracker_margin`, `tracker_margin_N` | the `Generation`, where the candidate is one (ΔK > 0 and assessed); NULL otherwise |

- **One `log_R`.** `PeakEstimate.log_R` and `DrainCandidate.log_R` are the same number under the one-loop law (P18).
  Store one, and check bitwise equality at every candidate in the tests and in §2.4's runs. A difference is a stop.
- **No `estimate` column.** The regime determines it (`one_loop` ↔ `ESTIMATE_ONE_LOOP`, `gap` ↔
  `ESTIMATE_CLASSICAL_GAP`, `classical` ↔ `ESTIMATE_CLASSICAL`, `unassessed` ↔ no estimate). Check that
  correspondence wherever both exist, as for `log_R`.
- **Ranges.** 02d's accepted integrations reach α_eff = 2.3 × 10⁻¹⁰⁸, L = 1.0 × 10⁻¹¹ and ln R = −239. Up to 13 343
  candidates fall in one history (§2d.11), so a column holds such values exactly.
- **What 05 reads.** n_g needs ΔK and q; E(N) needs the sample masses (§1.5); the phase slip needs k_ex, H and N; the
  backreaction ratio needs m and E. If one of these is missing from the table, stop and ask; do not add a column
  that 05 does not need.

### 1.4 The history columns (§8.3; P9 as revised; P30)

These are columns of `ScalarHistory`, written under the one-loop law only and NULL under the fixed law, at α = 0 and on
a failure row. Collect them in a record of their own beside `DissipationValues`, whose fields stay as they are, read
from the payload with `.get` as `dissipation_values` reads its own. K_final is the ledger's K(N_final), the
denominator of `Generation.share`.

| column(s) | value |
|---|---|
| `drained_share_one_loop`, `_gap`, `_classical`, `_unassessed` | Σ ΔK of the candidates in the regime / K_final |
| `parking_share` | the parking leftover's ΔK / K_final; 0.0 if the history did not park |
| `candidates_one_loop`, `_gap`, `_classical`, `_unassessed` | the candidates per regime, ΔK = 0 included |
| `ell_min` | the least ℓ over the one-loop candidates; NULL if there is none |
| `log_R_min_drain` | the least ln R over the candidates with ΔK > 0 and an assessment; NULL if there is none |
| `unresolvable_assessments`, `unresolved_declined` | the `DrainRecord`'s counts |
| `all_worst_<kind>_margin`, `all_worst_<kind>_margin_N`, `all_<kind>_failing_share`, for birth, reflection and tracker | over every generation, the weight ignored (P3, P30) |
| `bounce_rows` | the number of bounce rows (P29) |

- **The shares.** The four drained shares, `parking_share` and K_0/K_final sum to 1 to rounding. Test it.
- **The all-generation margins (P30)** come from the generations `relativistic_margins` returns, which hold every
  generation's margins. This takes one O(G) pass, not a second run of the core.
  - Each value equals the weighted record's value with `MARGIN_WEIGHT_SHARE` patched to 0 (worst, its N, failing
    share; ties as the core breaks them). Test that on constructed generations.
  - NULL where the margins were not computed (their warning).
- `injections`, `log_R_first_impact`, the parking and release columns and the weighted margins stay as they are, and
  are not duplicated.

### 1.5 The sample masses (U5; P8 as revised; P31)

- **Two columns of `ScalarHistoryValue`:** `log_m_eff_Mp`, the field's own ln m_eff at the sample (`field_log_m`), and
  `log_m_tracker_Mp`, the tracker's (`tracker_log_m`, searched from the sample's own field, as `history_margins`
  searches it). Both are nullable, and NULL where the model call raises `ComputationFailureError` (m_eff² ≤ 0, or no
  tracker minimum).
- **Every law and every α**, natural mode included. One code path.
- **Once per stored history (P31)**, on the payload `tune_history` returns, with that integration's realized model and
  cosmology: not on each tuning integration. Whether `integrate_history` called alone computes them is your choice;
  say which in the log.
- **Payload keys** parallel to `"sample"`, as `"rho_chi_over_rho_rad"` is. `store()` refuses a success payload that has
  samples but lacks them, as it refuses one without the ratio.
- **The counts.** Count the NULLs of each per history, and report them in §2.4.

### 1.6 The registry, the store and the read-back

- **`config/datastore.py`:**
  - `ScalarHistoryBounce` is registered after `ScalarHistoryValue`, with batch size 250 (P7);
  - it is in the drop group `scalar-history`, before `ScalarHistory`;
  - `tables_to_drop(["scalar-history"])` still raises as today, naming the dependants.
- **`config/sharding.py`:** sharded by `shard_key`, as `ScalarHistoryValue` is.
- **`ObjectFactories/ScalarHistory.py`:**
  - the bounce rows are written in `store`, read in `build`, counted in `validate` (against `bounce_rows`, as the
    samples against `z_samples`) and pruned in `validate_on_startup`, wherever `ScalarHistoryValue` is;
  - they are read with the samples and skipped under `_do_not_populate`;
  - the history's new columns are read on every lookup, as the dissipation columns are.
- **The objects.** `ScalarHistory` gains read-only access to its bounce rows (a tuple of records, in `ordinal` order)
  and to §1.4's record. `ScalarHistoryValue` gains the two masses. Each conversion uses the unit of its suffix, forward
  at the store and inverse at the read, as `_dissipation_columns` and `_dissipation_from_row` do.
- **No inventory spec for the table (P29).** DatastoreKit's `InventorySpec` names one value table, so the inventory does
  not count bounce rows. Record that in the log; do not count them by other means here. If you judge that the
  inventory must cover them, stop and ask.
- **`extra_data`** is unchanged under the fixed law. Under the one-loop law, add nothing to it that §1.4's columns
  hold.

### 1.7 The labels (U12)

- **`FINGERPRINT_FORMAT` 7**, with its history comment: the registry gains `ScalarHistoryBounce`; `alpha_value` has its
  own factory and spec (§1.1); ScalarHistory and ScalarHistoryValue gain columns that no spec names. Also:
  - `SPEC_DIGESTS[7]`, generated by `spec_digests`, never typed, beside formats 1–6, with its paragraph (pairs,
    distinct digests, which moved);
  - a new golden file `ObjectFactories/tests/data/store_fingerprint_one-loop-drain-03.json`, beside the old ones and
    never over them, with `GOLDEN` naming it.
- **`VERSION_LABEL` 2026.10.0**, with its paragraph in `config/version.py`: what moves (a one-loop history can be
  stored; under the fixed law nothing moves but the new columns), and that a 2026.9.0 store is not extended (P32).

### 1.8 Tests

- **`ComputeTargets/tests/`:**
  - the bounce-row builder on constructed records (candidates of every regime, ΔK = 0, unassessed, events of both
    sources, ties in N);
  - the history record and its shares' sum;
  - the all-generation margins against the patched-weight core (§1.4);
  - the sample masses' NULLs;
  - the key's model ↔ law mapping and its refusals;
  - `compute_scalar_history`'s `drain_law` passed through, with the default unchanged.
- **`ObjectFactories/tests/`**, through `_TempStoreCase` (never Ray, never a persistent file):
  - **the round trip of a one-loop history:** every new column, raw on the row and on the object, bitwise to the
    conversion definition `test_dissipation_store` uses (the row equals the forward conversion of the payload's value;
    the object equals the inverse conversion of the row); the bounce rows in order with their count; the sample masses;
    the same read with `_do_not_populate`, without the rows;
  - **real one-loop histories, chosen for cost:** at least one with one-loop candidates (2197 at A = 1 takes seconds),
    one with classical candidates, and one parked; a gap history too, if one is cheap. Each test that integrates states
    its cost in its docstring;
  - **constructed extremes:** α_eff = 2.3 × 10⁻¹⁰⁸, L = 10⁻¹¹, and one history of more than 13 343 rows, to exercise the
    batch size;
  - **the fixed law at α > 0 and at α = 0:** no bounce row, §1.4's columns NULL, the masses written, and every
    pre-existing column as today;
  - **a failure row:** every new column NULL, no row;
  - **the key:** the same value under the two models makes two rows and two histories, and a lookup under one finds
    neither the other's row nor its history; a lookup with no model is `fixed`; A = 0 under the one-loop model and an
    unknown model are refused;
  - **the guard (P28):** a payload whose model differs from the key's stores nothing;
  - **the configuration:** the registry, the drop group, the sharding and the batch size; the format guard at 7 beside
    1–6; the golden at 7; `VERSION_LABEL` 2026.10.0.
- **Existing tests.** Some pin today's literals or build `alpha_value` rows by raw insert (for example
  `test_dissipation_store`'s version test, `tools/tests/test_compare_stores.py`, `test_inventory_alpha.py`). Change only
  what the new label, format, column or factory requires, and name each change in the log. **No suite's count may
  fall.**

## 2. The Store gate (README §8.5)

### 2.1 The fixed law is unchanged

- The goldens pass bitwise, unchanged: `test_golden_history`, `test_golden_reflecting_history`, `test_tuning`.
- `integrate_history`'s and `tune_history`'s payloads under the fixed law differ only by the new keys: the sample
  masses, and the bounce key, which is absent or None. Show this on a golden history, key by key.
- `test_dissipation_store`'s and `test_tuned_history_round_trip`'s column checks pass with no change but the label and
  format literals: every pre-existing column of a fixed-law row, history and samples, is written as before.

### 2.2 The round trip

The tests of §1.8 pass: the round trip exact, the bounce rows in order, the fixed law and α = 0 writing no row and NULL
columns, the key and the guard.

### 2.3 The fingerprint

`SPEC_DIGESTS[7]` and the golden at 7 pass, and formats 1–6's pins are unchanged. Say which digests moved and why: at
least `alpha_value`'s and `ScalarHistory`'s.

### 2.4 The cost and the volume (P12)

**What to run.** Choose six histories from 02d's accepted integrations (`var/one-loop-drain/02d/sample.jsonl`) and name
them. Between them they cover one-loop drains only, the gap (P or W at A = 0.05), a classical tail of more than 8 000
candidates (the ladder), and C. Run each **once, untuned**, at its accepted normalization and A, through
`integrate_history`, then the sample masses and the store's write and read on a temporary store.

**What to report**, per history:
- the integration's wall time;
- the added time of the bounce key, the history record, the sample masses (and their NULL counts), and the store's write
  and read;
- the rows, beside 02d's candidate counts;
- each history's α_eff, ln R and regime per candidate, equal to 02d's partial file bitwise (the code of the integration
  has not changed).

**The stop.** If the median added time exceeds a quarter of the integration's (P12), stop and ask, with the breakdown.

Run them detached, check once that they started, and wait by a background command, not by polling. At most six at
once; the machine is shared.

## 3. What is not wanted

- **No driver option, no export.**
  - `main.py`, `plot_by_beta.py`, `plot_ScalarHistory.py`, `plot_inputs.py`, `extract_common.py` and `tools/` keep their
    options and their outputs; 04 adds them.
  - The tools change only where a test of §1.8 forces it.
- **No spectrum-dependent measure** (n_g, n̄_j, δ_stim, E, the phase slip, the backreaction ratio): those are 05's.
- **No value of the integration moves**, under either law: no change to the step loop, the law, the classification,
  the gate, the margins' core or the tuning.
- **No migration of a format-6 store (P32),** and no store opened but a test's temporary one and §2.4's.
- No Ray, no cluster, no rehearsal (06), no merge, no push. `PRyM/`, `thirdparty/` and the packages are not touched.
  If DatastoreKit's interface blocks the table, stop and ask; do not work around it.

## 4. Verification

- **The suites**, before the change and after, per `CLAUDE.md`: ComputeTargets in two halves, `tools/tests` alone.
  Record counts, times and `uptime`.
- **Where things go.** §2.4's outputs go under `var/one-loop-drain/03/`, and its scripts in
  `.documents/one-loop-drain/store-03/`, with text outputs in `store-03/out/`. Scratch goes in the agent's subdirectory
  of the session scratchpad, never `/tmp`.
- **Reproducibility.** Every number of the verification section comes from a script in `store-03/` or a test, or from
  a file under `var/one-loop-drain/03/` that the section names.

## 5. The commit, the log and the board

One commit (invariant 1), with:
- **the code:** `ComputeTargets/ScalarHistory.py`, `CosmologyConcepts/alpha_value.py`, `ObjectFactories/` (the
  factories), `config/datastore.py`, `config/sharding.py`, `config/fingerprint.py`, `config/version.py`, and any file a
  forced test change touches; the new and changed tests; the new golden file;
- **the scripts:** `.documents/one-loop-drain/store-03/` and its `out/`;
- **the verification document:** `.documents/one-loop-drain-verification.md`, a new §3 holding §2's gate item by item.
  It is additive (invariant 6): §1–§2d stay as they are;
- **dated notes** in `.documents/architecture-summary.md` (the key, the table and its columns, the history and sample
  columns, the guard, format 7, 2026.10.0) and `.documents/numerical-strategies.md` (the sample masses, and the
  all-generation margins taken from the generations);
- **`CLAUDE.md`:** the test counts, and in "Redshift" the sentence on α's key, which now names the drain model;
- **the log** `logs/03-the-store.md` (template: `dk-and-rpk-migration` README §5.1):
  - every deviation from this prompt classified;
  - "Observations not acted on", including the inventory's single value table;
  - a "State handed to the next prompt" that tells 04 the key, the column names and units, and the read-back API, and
    tells 05 which columns it reads;
- **the board:** row 03, its items, the Store gate, §3 → §4 for the two issues closed, and **Last updated**;
  `.documents/OPEN_ISSUES.md` in the same commit (the two rows deleted, the count from 31 to 29).

Format the Python files you change with `black`. Stage by explicit path, and show `git diff --cached --name-status`
before committing. The commit message follows `CLAUDE.md` invariant 5. Write "this commit" where the SHA goes; the
orchestrator substitutes it.
