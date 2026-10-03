# Log 02 — Move production to the small network, set both low-T tolerances, and warn on foreign BBN rows

**Prompt:** prompts/bbn-tolerance/02-low-T-tolerance-patch.md
**Commit:** the commit that adds this file ("Move BBN to PRyMordial's small network and set low-T rtol"); its SHA is in `git log`
**Model:** Claude Opus 5.5
**Date:** 2026-10-03
**Result:** COMPLETE WITH DEVIATIONS (six implementation choices; no acceptance row missed, no
stop condition met). Both low-T calls carry their ruled `rtol` (small 1e-6, full 1e-5). Production
runs the small network. `PRYM_VERSION` is `+bt02`. The pinned constants re-derive from their
provenance to the expected values. The 49 acceptance solves reproduce log 01c's T1 rows at 1e-6 in
every field (49 of 49), all 11 full-network failures included. The control's serial cost is 10.11 s
against 10.05 s (1.006×). The suites pass at 18, 113 (106 + 7 new) and 31. The six deviations
are implementation choices; none touches a §2 design fact. The prompt opens one issue
(Observations 1).

Everything was run on `2bc124b` plus this prompt's uncommitted diff. That was the branch head at
dispatch, and nothing landed on the branch while the prompt ran. The tool's CSV `commit` column
reads `2bc124b+dirty`. The store was opened only through the tool's `connect_ro`.

## What shipped

**`VERSION_LABEL`** stays `"2026.6.0"`. **`PRYM_VERSION`** goes from `"bf24c3d+ri02+sr01"` to
`"bf24c3d+ri02+sr01+bt02"`.

**(a) `PRyM/PRyM_main.py`** (the two low-T calls only; not reformatted). There is one marker line
in the file's style, above each new keyword argument. The hunks, for re-application on an upgrade:

```diff
@@ -1346,6 +1346,8 @@ class PRyMclass(object):   # the small network's low-T solve_ivp
                         wall_clock_start,
                         wall_clock_limit,
                     ),
+                    # ChamPBH bbn-tolerance prompt 02: rtol 1e-6; upstream passes no rtol, so SciPy's 1e-3 applied (measured: bbn-tolerance log 01c)
+                    rtol=1.0e-6,
                     atol=1.0e-11,
                 )
@@ -1426,6 +1428,8 @@ class PRyMclass(object):   # the full network's low-T solve_ivp
                         wall_clock_start,
                         wall_clock_limit,
                     ),
+                    # ChamPBH bbn-tolerance prompt 02: rtol 1e-5; upstream passes no rtol, so SciPy's 1e-3 applied (measured: bbn-tolerance log 01)
+                    rtol=1.0e-5,
                     atol=1.0e-15,
                 )
```

Nothing else in `PRyM/` changed: no other stage, no rate, no Julia branch, and not
`_check_solve_ivp`.

**(b) The version.**

- `ComputeTargets/BBNData.py:48`: `PRYM_VERSION = "bf24c3d+ri02+sr01+bt02"`. A dated sentence on
  "bt02" was added to the comment above it.
- `config/version.py`: one dated history entry (2026-10-03, bbn-tolerance prompt 02) after the
  last one. Under the same label, production runs the small network with both low-T `rtol` set,
  and `PRyM_version` is `+bt02`. BBN rows are refreshed by a copy and `--drop bbn-data`, and both
  drivers warn about foreign rows. The 2026-09-30 entry that says production runs the full
  network is untouched; it is history.

**(c) Production runs the small network.**

- **`main.py`.**
  - `:791–801`: `BBN_SMALL_NETWORK = True`, inside `run_pipeline` just above
    `compute_bbn_data_batch`.
  - `:807`: the payload's `"small_network": False` is now `"small_network": BBN_SMALL_NETWORK`.
  - No command-line flag. The comment on the assignment reads:

    ```
    # The PRyMordial network production runs: the small one (bbn-tolerance prompt 02; that
    # campaign's ruling U4). PRyMordial's full network fails on about 1 % of histories near
    # T_J = 1 keV. The cause is its Li8(p,d)Li7 reverse rate, exp(gamma/T9) times a quadratic
    # spline of the forward-rate table that rings in sign there (bbn-tolerance log 01;
    # [01-prymordial-li8-p-d-li7-rate-rings-near-1-kev]). No tolerance removes it, and the small
    # network has no Li8. The small network's 7Li/H is less reliable, and is not used for
    # constraints (bbn-tolerance README section 0.2 U3). Setting this to False runs the full
    # network: its low-T rtol is 1e-5, and its failures remain. There is deliberately no
    # command-line flag: BBNData treats PRyMordial as a black box, and a change of BBN code is
    # handled by the versioning mechanism (PRyM_version).
    BBN_SMALL_NETWORK = True
    ```
- **`plot_by_beta.py`.**
  - `:70–76`: the module-level `BBN_SMALL_NETWORK = True`. Its comment says that it must match
    `main.py`'s, and why: a baseline on the other network is off by the network offset, about
    3×10⁻⁴ in D/H (log 01c, row 6).
  - `:1112`: `compute_SM_baseline(small_network=BBN_SMALL_NETWORK)`. The comment above it no longer
    says "small_network=False is what main.py passes".
- **P15.**
  - `compute_BBN_data`'s default (`ComputeTargets/BBNData.py:397`) is now `small_network: bool = True`.
  - `BBNData.compute`'s two fallbacks (`:746`, `:752`) are now `True`.
  - `tools/bbn_baseline.py`'s `--small-network` defaults to `True`. Its help says so and names
    `--no-small-network`.
  - `tools/bbn_from_store.py` keeps its default, the full network. Only its help text changed: it
    no longer says "as main.py", and says that logs 01 and 01c's reproduction commands assume the
    full network.
- `extract_common.add_BBN_info_labels` is unchanged, as the prompt says.

**(d) The warning** (`pipeline_selection.py`).

- **New public names.**
  - `BBNProvenance` (frozen dataclass): `foreign: Dict[Tuple[Any, Any], int]`, `not_stored: int`,
    `current: int`.
  - `BBN_REFRESH_ROUTE: str`.
  - `foreign_bbn_provenance(bbn_objects, prym_version: str, small_network: bool) -> BBNProvenance`.
    It skips objects that are not `available`. For the rest it reads `failure` first: a failure
    row counts in `not_stored`. Otherwise it groups the object by
    `(PRyM_version, small_network)` against the given pair.
  - `warn_foreign_bbn_provenance(bbn_objects, prym_version: str, small_network: bool, emit=print) -> None`.
- **The printed warning** (from `test_foreign_bbn_provenance`'s stand-ins):

  ```
  !! warning: 6 stored BBNData row(s) were not made by this code's PRyMordial (PRyM_version=bf24c3d+ri02+sr01+bt02, small network); they are used as stored
  !! warning:   3 x PRyM_version=bf24c3d+ri02+sr01, small network
  !! warning:   2 x PRyM_version=bf24c3d+ri02+sr01, full network
  !! warning:   1 x PRyM_version=bf24c3d+ri02+sr01+bt02, full network
  !! warning:   4 failure row(s) store no provenance and cannot be classified
  !! warning: to refresh BBN, copy the store and run main.py on the copy with --drop bbn-data
  ```

  If no row is foreign it prints nothing.
- **The callers.**
  - **`main.py`.** `bbn_lookup_results` (`:559`) collects every result of the BBN stage's lookup
    (`:681`). After `bbn_data_queue.run()` the stage calls
    `warn_foreign_bbn_provenance(bbn_lookup_results, prym_version=PRYM_VERSION, small_network=BBN_SMALL_NETWORK)`
    (`:838`), just before its summary line.
  - **`plot_by_beta.py`.** `bbn_rows_read` (`:688`) collects the rows each potential's lookup
    reads (`:940`). After `work_queue.run()`, `run_pipeline` makes the same call (`:978`) with
    its own `BBN_SMALL_NETWORK`.
  - Nothing is skipped, filtered or recomputed because of the warning.
- **New imports.** `main.py` and `plot_by_beta.py` import `PRYM_VERSION` from
  `ComputeTargets.BBNData`. `plot_by_beta.py` also imports `warn_foreign_bbn_provenance`.

**(e) Tests** (`ComputeTargets/tests/`).

- **New: `test_lowT_tolerance.py` (1 test, two solves).**
  `test_a_low_T_calls_receive_the_ruled_tolerances` records every `solve_ivp` call of a small-
  and a full-network SM baseline through the tool's `stage_tolerance_override({})`. It asserts:
  - the small network's low-T call has `rtol` 1e-6 and `atol` 1e-11;
  - the full network's low-T call has `rtol` 1e-5 and `atol` 1e-15;
  - every other call has `rtol` 1e-6 and `atol` 1e-9;
  - every call announces a stage, and every call succeeds.
- **New: `test_foreign_bbn_provenance.py` (6 tests, no solve).**
  - (b1): the counts on stand-ins.
  - (b2): the printer.
  - (b3): the `ast` check on both drivers.
  - (b4), three tests: real `BBNData` rows looked up from a temporary SQLite datastore, with
    `_do_not_populate`, as `main.py` and `plot_by_beta.py` look them up (deviation 5).
- **Re-pinned constants.** Each keeps its old value in a comment with its commit, and has a dated
  line naming this prompt. **No bound changed.**

| constant (test, bound) | old | new | provenance of the new value |
|---|---|---|---|
| `CONST_HONLY_SMALL_YP` (`test_prym_passenger` (c), 1e-6) | 0.2536690816 | **0.253669508** | `pinned_reference_7b518c9.py const-honly-small --rtol 1e-6` on `7b518c9` |
| `CONST_HONLY_SMALL_D_OVER_H_E5` (same) | 2.6481673 | **2.649288446** | same |
| `CONST_HONLY_FULL_YP` (`test_network_flag` (b), 1e-5) | 0.2536754614 | **0.2536731562** | `… const-honly-full --rtol 1e-5` on `7b518c9` |
| `CONST_HONLY_FULL_D_OVER_H_E5` (same) | 2.648809882 | **2.649990509** | same |
| `BUILDER_CONST_HONLY_FULL_YP` (`test_bbn_callbacks` (h), 1e-6) | 0.2536761805 | **0.2536745605** | `… builder-honly-full --rtol 1e-5` on `7b518c9` |
| `BUILDER_CONST_HONLY_FULL_D_OVER_H_E5` (same) | 2.648529359 | **2.649973638** | same |
| `README_BASELINE` (`test_bbn_callbacks` (i), 1e-4) | Yp 0.24689, D/H 2.4623, ³He/H 1.042, ⁷Li/H 5.423 (review-remediation README §2 (f) row 1; full network, default) | **Yp 0.2468802117, D/H 2.458287893, ³He/H 1.041932695, ⁷Li/H 5.486373007** | `01c-probes/scan.csv`, T1, SM, `lowT_rtol == "1e-06"`, to 10 significant figures (P16) |
| `PRYM_VERSION` (`test_bbn_solver_failures` (e)) | `"bf24c3d+ri02+sr01"` | **`"bf24c3d+ri02+sr01+bt02"`** | — |
| `LOWT_SMALL_RTOL_AS_PASSED` (`test_bbn_from_store` (b)) | `None` | **`1e-6`** | log 01, deviation 7 |
| `OVERRIDE_RTOL` (`test_bbn_from_store` (b)) | `1e-6` | **`1e-8`** | a value of the scan; an override of 1e-6 is invisible once the call passes 1e-6 |

- **Other test changes.**
  - `test_bbn_callbacks` (i) now calls `compute_SM_baseline(True)` and asserts
    `small_network is True` (P16). Its docstring and the comment above `README_BASELINE` give the
    new provenance and keep the old one.
  - `test_bbn_from_store` (b): the comment and docstring that said the call passes no `rtol` are
    updated.
- **`test_network_flag`.**
  - (c) is renamed `test_c_production_defaults_are_the_small_network` and asserts `True` at all
    four places it reads:
    - `compute_BBN_data`'s default;
    - `plot_by_beta.py`'s call, run through the stubbed `PRyMclass`: the baseline's
      `small_network` and `smallnet_flag`;
    - `main.py`'s payload;
    - `tools/bbn_baseline.py`'s default.
  - The drivers' names are resolved to the literal they are assigned once (new helpers
    `_literal_of_name` and `_resolve`).
  - The module docstring now says production passes the small network. (b)'s logic is unchanged.

**(f) Probes** (`prompts/bbn-tolerance/logs/02-probes/`).

- **Scripts.**
  - `run_jobs.py`: the 49 acceptance solves, adapted from `01c-probes/run_jobs.py`.
  - `compare.py`: acceptance against log 01c.
  - `cost.py`: `01c-probes/cost.py` copied with only its docstring and usage path changed.
- **Data.** `acceptance.csv` (49 rows), `cost.csv` (6 rows).
- **Printed outputs.** `compare.txt`, `cost_output.txt`, `pinned_rederived.txt`.
- `run_jobs.py`'s per-invocation stdout (`out/`) was deleted, as log 01c did; the CSV holds every
  value it printed.

## Deviations from the prompt

### 1. Where the marker comments sit in `PRyM/PRyM_main.py` — IMPLEMENTATION CHOICE

The prompt asks for "a comment above it", in the file's marker style. Each marker is one line,
directly above the new `rtol=` argument, inside the call. Two alternatives were considered:

- **Above `sol_at_LT = solve_ivp(`.** There the `_check_wall_clock` call and its own marker
  already sit, so the new marker would be separated from the line it explains.
- **A multi-line comment.** That would make the hunk larger to re-apply.

The file already puts markers inside calls ("fun (and jac) behind the wall-clock limit"), so this
placement is in its style. The marker lines are longer than 88 columns. `PRyM/` is not
reformatted (README §5 rule 7).

### 2. Each driver warns once per run, not once per lookup — IMPLEMENTATION CHOICE

- **The problem.** The prompt says each driver "calls it on the BBN rows its lookup returns". Both
  drivers look up BBN rows many times: `main.py` once per batch of 8 pairs, and `plot_by_beta.py`
  once per potential. A call after each lookup would print a warning per batch (about 90 on the
  science store) or per potential.
- **What was done.** Each driver collects the lookup results in a list and calls the printer once:
  - `main.py`, at the end of the BBN stage, beside its summary line;
  - `plot_by_beta.py`, at the end of each model's `run_pipeline`.
- **The cost.** The lists hold the `_do_not_populate` objects, which carry no sample values. Each
  holds a `ScalarModelProxy` whose object is a `_do_not_populate` `ScalarModel`, which is small;
  about 710 of each on the science store.
- **The trade-off.** `main.py` prints the warning after the stage computes the missing rows, not
  before. A per-batch summary would print earlier but much more often.

### 3. The network's name and where it is assigned — IMPLEMENTATION CHOICE

- **The name.** Both drivers use the same name, `BBN_SMALL_NETWORK`, so that a reader can find
  them together.
- **Where.**
  - In `main.py` it is assigned inside `run_pipeline`, just above `compute_bbn_data_batch`, which
    is where the literal was. U4 asks for the comment "at that line".
  - In `plot_by_beta.py` it is assigned at module level. Both the SM baseline (module level) and
    `run_pipeline` (a function defined earlier in the file) read it.
- **The test.** `test_network_flag`'s `_literal_of_name` requires exactly one assignment of the
  name in each file, so a second assignment would fail (c).

### 4. The printer's lines — IMPLEMENTATION CHOICE

The prompt asks for "one warning naming each foreign pair and its count, and the count of failure
rows … It ends with one line naming the refresh route". The warning is one block of `!! warning`
lines:

- a header with the total foreign count and the driver's own pair;
- one line per foreign pair, sorted by count;
- one line with the not-stored count;
- the route line.

`emit` defaults to `print`, as in `warn_super_planckian`, so the test can capture the lines. A
`small_network` that is neither `True` nor `False` (it cannot be, on a success row) would print
as its `repr` rather than raise.

### 5. The fields are confirmed on stored objects in a test, through `Datastore/tests`' stand-ins — IMPLEMENTATION CHOICE

The prompt asks to confirm that the fields are readable "on the objects the drivers hold". I did
it in a test, (b4), rather than once by hand, so that it stays checked:

- **The store.** It builds the undecorated `Datastore` on a temporary SQLite file. It reuses
  `Datastore/tests/test_version_keyed_lookups`' helpers (`_TempStoreCase`, `_insert`,
  `_model_proxy`, …), as `Datastore/tests/test_bbn_failure_lookup.py` does.
- **The rows.** It inserts a success row with a chosen `(PRyM_version, small_network)`, and a
  failure row.
- **The lookups.** It looks them up through `object_get` with `_do_not_populate=True`, as
  `main.py` does (`failure=None`) and as `plot_by_beta.py` does (`failure=False`).
- **The result.** `PRyM_version` and `small_network` are readable on both. A failure row is
  counted only as provenance not stored.

The planner's reading of `build` (`:262–278`) and the constructor (`:558–578`) is right. The
alternative was a one-off probe, which nothing would re-run. The test imports from
`Datastore.tests`, a cross-package test dependency, and so adds three tests to ComputeTargets'
count.

### 6. The re-derivation also re-ran the old pins — IMPLEMENTATION CHOICE

Before re-deriving at the new settings, `pinned_reference_7b518c9.py` was run on `7b518c9` with
no override for all three cases. All three old pins reproduced to every printed digit
(`pinned_rederived.txt` §1), so the provenance tree was the same as logs 01 and 01c used. This
costs three solves more than the prompt asks for.

## Verification performed

**Suites.** Run from the repository root with `PYTHONPATH=. ./venv/bin/python -m unittest discover
-s <pkg>/tests -t .`.

| package | before (`2bc124b`) | after (`2bc124b` + this prompt's diff) |
|---|---|---|
| CosmologyModels | 18, OK | 18, OK |
| ComputeTargets | 106, OK | **113, OK** (+1 `test_lowT_tolerance`, +6 `test_foreign_bbn_provenance`) |
| Datastore | 31, OK | 31, OK |

`black --check` is clean on every non-`PRyM/` file changed: the seven production and tool files,
`ComputeTargets/tests/` and `02-probes/`. `py_compile` passes on `main.py` and `plot_by_beta.py`.

**The breakage checks** (run, not reasoned).

- **How they were run.** A temporary worktree of `2bc124b` (the commit before this one) was made.
  The three new or changed test modules were copied into it and run. The worktree was then
  removed.
- **Test (a) fails on both networks.** The message is "'rtol' not found in {'method': 'BDF',
  'jac': …, 'atol': 1e-11}: the low-T call passes no rtol", and likewise with `atol` 1e-15.
- **(b3) fails for both drivers**: "main.py does not name its network" and "plot_by_beta.py does
  not name its network".
- **(b1), (b2) and (b4) error.** `pipeline_selection` has no `foreign_bbn_provenance`.
- **`test_network_flag` (c) fails**: "False is not True".
- **The totals.** 8 tests ran: 5 failures, 6 errors (sub-tests included).

**The store stayed read-only.** `/usr/bin/stat -f "%N %m %z"` on all 17 files of
`~/ChamPBH-stores/science-2026.6.0*` was identical before the first solve and after the cost run.
No `-wal` or `-shm` file appeared.

### §6.2 row by row

**1. The low-T calls' `rtol` and `atol`.** Test (a) passes. It records small 1e-6/1e-11, full
1e-5/1e-15, and every other call 1e-6/1e-9. It fails on `2bc124b` (above).

**2. Production's network.** `test_network_flag` (c) passes:

- `main.py`'s one name is `True`, with the Li8 comment;
- `plot_by_beta.py`'s baseline is `True`;
- `compute_BBN_data`'s default and `tools/bbn_baseline.py`'s default are `True`;
- there is no flag.

It fails on `2bc124b`. `BBNData.compute`'s two fallbacks are `True` by reading
(`ComputeTargets/BBNData.py:746`, `:752`); no test reads them, and none did before.

**3. The patched tree, no override, small network: roster × 3 variants + SM, 49 solves: 49 of
49 identical to log 01c.**

- **How it was run.** `run_jobs.py --jobs 8`: 17 invocations, 8 at a time, in 127 s, all with
  exit code 0. Each runs `tools/bbn_from_store.py … --small-network` with no `--lowT-rtol`. The
  CSV's `lowT_rtol` and `lowT_atol` are empty on all 49 rows, which `compare.py` asserts.
- **How it was compared.** `compare.py` (`compare.txt`) compares each row with
  `01c-probes/scan.csv`'s `tag == "T1"`, `lowT_rtol == "1e-06"` row for the same input and
  variant. The fields are status, failure stage, `t reached`, `t target`, Yp, D/H, ³He/H, ⁷Li/H
  and the failure reason, compared as the CSV strings were written (`repr`, 17 significant
  digits).
- **The result.** 49 identical, 0 differing, 0 missing, 0 non-ok outcomes. **All 11 histories
  that fail on the full network complete**, in all three variants.
- **Examples.**
  - SM: Yp 0.24688021169088586, D/H 2.4582878928660548, ³He/H 1.0419326951489363, ⁷Li/H
    5.48637300688257.
  - Control β = 1.6, M = 10⁻³, `prod`: Yp 0.24688971029506177, D/H 2.4609141011473543.
- **Conclusion.** The patch and log 01's override are the same change.

**4. `PRYM_VERSION`; `VERSION_LABEL`.** `"bf24c3d+ri02+sr01+bt02"` (`test_bbn_solver_failures`
(e), updated, passes) and `"2026.6.0"` (unchanged; `git diff config/version.py` adds comment
lines only).

**5. The pinned constants.**

- **Re-derived from their provenance.** The script is `01-probes/pinned_reference_7b518c9.py`,
  run unmodified in a temporary `7b518c9` worktree with this repository's venv. The worktree was
  removed afterwards (`git worktree list` shows only the main tree). The output is in
  `pinned_rederived.txt`.
- **No override.** All three old pins reproduce to every printed digit.
- **At the U4 settings:**
  - `const-honly-small --rtol 1e-6`: Yp **0.253669508**, D/H **2.649288446**;
  - `const-honly-full --rtol 1e-5`: Yp **0.2536731562**, D/H **2.649990509**;
  - `builder-honly-full --rtol 1e-5`: Yp **0.2536745605**, D/H **2.649973638**.

  Each equals the prompt's §1 (e) expected value to every printed digit, so the §5 stop is not
  met.
- **The patched tree with no override** (the test prints; ComputeTargets suite):

| test | value now | against the re-pinned value | bound | result |
|---|---|---|---|---|
| `test_prym_passenger` (c) | Yp 0.253669508, D/H 2.649288446 | 1.20e-10, 9.92e-11 | 1e-6 | pass |
| `test_network_flag` (b), full vs pins | Yp 0.2536731562, D/H 2.649990509 | 1.72e-10, 1.48e-10 | 1e-5 | pass |
| `test_bbn_callbacks` (h) | Yp 0.2536745605, D/H 2.649973638 | 9.54e-11, 1.20e-10 | 1e-6 | pass |
| `test_bbn_callbacks` (i), small | Yp 0.2468802117, D/H 2.458287893, ³He/H 1.041932695, ⁷Li/H 5.486373007 | equal to 10 printed digits | 1e-4 | pass |

- **`test_network_flag` (b)'s ⁷Li/H shift**, small at 1e-6 against full at 1e-5, is
  **1.033e-02** (bound ≥ 5e-3; passes). Small ⁷Li/H is 5.186233632 and full is 5.133203422. The
  same test prints a small-against-full D/H shift of 2.649e-04 and a Yp shift of 1.438e-05;
  those are not bounded.

**6. The foreign-provenance warning.**

- `test_foreign_bbn_provenance` passes:
  - (b1): the counts;
  - (b2): one line per foreign pair, nothing when nothing is foreign, and a return of `None`;
  - (b3): the `ast` check;
  - (b4): stored rows read through `object_get` with `_do_not_populate`.
- (b3) fails on `2bc124b`. Nothing is skipped: both callers discard the return value, and the
  lists they pass are the same lists they use as before.

**7. Serial cost of the control, small network** (`cost.py small:default`, `cost_output.txt`,
`cost.csv`).

- **How it was run.** Alone, after the acceptance solves and the suites had finished and no
  process of this prompt was left (`ps` showed none). I waited for the 1-minute load to fall
  below 10, as log 01c did. There was one warm-up per network, discarded, then three repeats.
- **The load.** The 1-minute load average was 8.86 before the run, 6.42, 6.05 and 5.91 during the
  three repeats, and 5.91 after. The 15-minute average was still falling from a background peak
  (20.7 → 19.2): PyCharm, OrbStack, `mediaanalysisd` and an idle Ray dashboard, none of them this
  prompt's.
- **The result.** Control: 10.02, 10.11 and 10.12 s, a median of **10.11 s**, 1.006× log 01c's
  10.05 s. The target is 1.2×, so it passes. SM: 8.74, 8.76 and 8.89 s, a median of 8.76 s
  (log 01c: 8.79 s).
- **The abundances.** Every repeat gave the T1 abundances at 1e-6 bitwise.

**8. Suites.** All pass, and the counts did not fall: 18, 113, 31.

## Observations not acted on

1. **Two places still describe the full network as production's.**
   - **`tools/history_and_bbn.py:255–258`.** Its `--small-network` defaults to `False`, with help
     "(default: the full network, as main.py)". It runs `compute_BBN_data._function` with that
     value, so by default it now solves a different network from production, while its help says
     it is production's. It is not in this prompt's allowed list, and P15 does not name it.
   - **`ComputeTargets/BBNData.py:256`**, in `_configure_PRyMordial`. A comment says "False
     (production) runs the full network". This prompt may touch only the named hunks of that
     file.

   Opened as **`[02-two-places-still-say-production-runs-the-full-network]`** on this board (§3).
   The first is a default, not only text, and P15's logic ("the other production defaults follow
   `main.py`") would extend to it. The user should decide whether it follows, or keeps the full
   network as `tools/bbn_from_store.py` does.
2. **`tools/bbn_baseline.py`'s module docstring** still gives `--small-network` as its second
   usage example, which is now the default. The allowed list names only the argument's default
   and help. It is harmless and not opened: with `BooleanOptionalAction` the flag is still valid.
   It is cosmetic, for housekeeping or prompt 03.
3. **`main.py` warns after the BBN stage computes**, not before (deviation 2). A user who runs
   `main.py` on a store that was not refreshed, without `--drop bbn-data`, sees the warning at the
   end of the BBN stage. That is consistent with the stage's other summary lines; noted for
   prompt 03's handover text.

## State handed to the next prompt

- **The tree.**
  - `PRYM_VERSION = "bf24c3d+ri02+sr01+bt02"`; `VERSION_LABEL = "2026.6.0"`.
  - Production runs the small network, through `main.py`'s `BBN_SMALL_NETWORK = True` (inside
    `run_pipeline`) and `plot_by_beta.py`'s module-level `BBN_SMALL_NETWORK = True`.
  - The low-T `rtol` is 1e-6 on the small network and 1e-5 on the full one; `atol` is 1e-11 and
    1e-15, unchanged.
  - Setting `main.py`'s name to `False` selects the full network; `plot_by_beta.py`'s name must
    follow it.
- **The warning.** `pipeline_selection.warn_foreign_bbn_provenance(bbn_objects, prym_version,
  small_network, emit=print)` and `foreign_bbn_provenance(...) -> BBNProvenance`. Text as in
  "What shipped" (d). A store that was not refreshed, such as the science store, will print it
  with every successful row counted under `PRyM_version=bf24c3d+ri02+sr01, full network`.
- **The roster on the final tree (README §6.3's target).** Prompt 03's re-measure must equal
  `02-probes/acceptance.csv` (49 rows, tag `A02`). That file is identical in every outcome field
  to `01c-probes/scan.csv`'s T1 rows at `lowT_rtol == "1e-06"`.
  - SM (small): Yp 0.24688021169088586, D/H 2.4582878928660548, ³He/H 1.0419326951489363, ⁷Li/H
    5.48637300688257.
  - Control β = 1.6, M = 10⁻³, `prod`: Yp 0.24688971029506177, D/H 2.4609141011473543.
- **The cost.** Small network, no override: control median 10.11 s and SM 8.76 s, serial, at
  1-minute load 5.9–6.4. Log 01c gave the full network at the old default as 8.86 s and 7.61 s.
- **The re-pinned values**: the table in "What shipped" (e).
- **The P9 residuals** at the production setting are log 01c's: D/H spread median 4.4e-5, and
  1.5e-4 on β = 2.4, M = 10⁻⁵; Yp spread median 1.5e-5, max 4.3e-5; convergence against 1e-8 of
  D/H ≤ 5.5e-5 and Yp ≤ 2.1e-7. They were not re-measured here.
- **Suite counts:** 18, 113, 31.

**Reproduction commands.** Run from the root.

```bash
./venv/bin/python prompts/bbn-tolerance/logs/02-probes/run_jobs.py --jobs 8   # acceptance.csv
./venv/bin/python prompts/bbn-tolerance/logs/02-probes/compare.py            # compare.txt
./venv/bin/python prompts/bbn-tolerance/logs/02-probes/cost.py small:default # alone; cost.csv
# P7, in a 7b518c9 worktree, with PYTHONPATH=. and this repository's venv:
#   prompts/bbn-tolerance/logs/01-probes/pinned_reference_7b518c9.py CASE [--rtol X]
#   CASE: const-honly-small (--rtol 1e-6), const-honly-full / builder-honly-full (--rtol 1e-5)
./venv/bin/python tools/bbn_from_store.py --sm-baseline --small-network     # SM, small, as production
```
