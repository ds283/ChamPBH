# Log 02 — read-only plot pools

**Subject:** Open the plot scripts read-only through a shared plot_inputs module
**Commit:** this commit, on branch `read-only-plots`, on top of `e0cf6f0`
**Date:** 2026-10-13, by Claude Opus 5.5
**Result:** **COMPLETE.** O1–O5 shipped. ComputeTargets 131 → 152 (+21 `test_plot_inputs`);
CosmologyModels 18, ObjectFactories 63 and tools 34 unchanged. All four deliberate breakages
caught. No §4 stop condition was met. `[08-plot-scripts-open-read-write-pools]` closed; no issue
opened; `.documents/OPEN_ISSUES.md` 27 → 26.

## What shipped

- **O1 — `plot_inputs.py`** (new, top level, 302 lines). It parses nothing and starts no Ray at
  import; it imports `ray`, `datastorekit.replication` (`ReadOnlyMiss`, `ReadOnlyWrite`),
  `MetadataConcepts.tolerance` and `config.model_list.build_model_list`, and calls `ray.get` as the
  scripts did. Its public names:
  - `PlotOptionNotStored(class_name, option, value)`, a `RuntimeError`, whose `str` is the whole
    line the script prints; the pool's `ReadOnlyMiss` is chained as `__cause__`;
  - `parameters(pool, abs_tol, rel_tol, T_init_GeV, phi_init_Mp, units) -> PlotParameters`
    (a `NamedTuple` `atol, rtol, T_init, phi_init, pi_init`; rows 1–5). All five lookups are
    submitted, then each reference is read on its own, in that order; the first `ReadOnlyMiss` of
    the class looked up raises `PlotOptionNotStored`;
  - `stored_grid(pool, units) -> StoredGrid` (`beta, M, Lambda`, by `read_table`; row 17). Raises
    nothing of its own;
  - `potentials(pool, potential_type, M_table, Lambda_table, units) -> list` (row 6). An unknown
    type raises `ValueError("Unknown potential type: ...")`, the old message, before any lookup.
    One lookup per `itertools.product(M_table, Lambda_table)` combination; each reference read on
    its own; a `ReadOnlyMiss` whose `class_name` is the potential's class **skips** that
    combination with one warning line, and a count line follows the last;
  - `couplings(pool, beta_table, units) -> list` (row 7). One lookup per beta; a `ReadOnlyWrite`
    with `operation == "an insert"` and `class_name == "ExponentialCoupling"` (the read-only
    inserter's refusal for a sharded class, `datastorekit/SQL/Datastore.py:296-312` at `v0.3.0`)
    **skips** that beta with one warning line, and a count line follows the last;
  - `models(pool, units) -> list` (row 8): `build_model_list`, with a `ReadOnlyMiss` of
    `QCD_Cosmology` raised as `PlotOptionNotStored`;
  - `T_stop(pool, T_stop_GeV, cosmology, units)` (row 9): `--T-stop-GeV` if given, else the
    cosmology's `T_CMB`; a `ReadOnlyMiss` of `temperature` raises `PlotOptionNotStored`.

  Any other exception propagates unchanged from every function. Warnings and counts are printed to
  stdout; nothing is printed when nothing is skipped.
- **O2 — the messages**, verbatim from the tests:
  - a parameter: `!! plot: the store holds no temperature for --T-stop-GeV 1e-07; main.py was not run with it, so no history in this store can match. Give the plot the options the build used`
    (the prompt's example, character for character); `--rel-tol 1e-06` likewise;
  - the constants: `!! plot: the store holds no temperature for the cosmology's constant T_CMB 2.7255 K (the T_stop used when --T-stop-GeV is not given); main.py was not run with it, ...`
    and `!! plot: the store holds no QCD_Cosmology for the constant cosmology Planck2018 of config/model_list.py; main.py was not run with it, ...`;
    `pi_value` reads `for the constant initial momentum pi = 0`;
  - a potential: `!! plot: no ExponentialPotential stored for M=1e+25 eV, Λ=1 eV; no history can use it; skipped`,
    then `!! plot: 2 of 6 ExponentialPotential combinations of the stored M and Λ are not stored, and were skipped`;
  - a coupling: `!! plot: no ExponentialCoupling stored for β=1; no history can use it; skipped`,
    then `!! plot: 1 of 3 stored β have no ExponentialCoupling, and were skipped`.
- **O2 — under real Ray.** Read at Ray 2.53.0 (installed): `ray/_private/worker.py:1015-1016` and
  `:2978-2979` raise `value.as_instanceof_cause()` for a `RayTaskError`; `ray/exceptions.py:156-192`
  builds a class deriving from both `RayTaskError` and the cause's class, whose `__getattr__`
  returns the cause's attributes (`:182-183`). So `except ReadOnlyMiss` / `except ReadOnlyWrite`
  catch it at `ray.get`, and `class_name` and `operation` read through. Not run here; 03 runs it.
- **O3 — the scripts.** In each of `plot_by_beta.py` and `plot_ScalarModel.py`:
  - a module docstring saying the store is opened read-only, what a missing parameter and a
    missing potential or coupling do, and that `--drop` and `--prune-unvalidated` are `main.py`'s
    (refused and ignored respectively);
  - `import plot_inputs`; the imports the move left unused removed (`plot_by_beta.py`:
    `itertools`, `BaseCosmology`, `build_model_list`; `plot_ScalarModel.py`: `BaseCosmology`,
    `build_model_list`, its `itertools` is still used by `run_pipeline`);
  - after the provenance line and before 01's check, a top-level `if len(args.drop) > 0:` printing
    `!! --drop: a plot run drops nothing, since it opens its store read-only; --drop is main.py's option`
    to stderr and `sys.exit(2)`. 01's `_ray_problem = ...` assignment, its `if` and `ray.init` are
    untouched and adjacent;
  - `ShardedPool(...)` gains `read_only=True`; every other argument as it was
    (`prune_unvalidated=False`, no `drop_tables`, no `shards=`); `plot_by_beta.py`'s
    `job_name="plot_ScalarModel"` is now `"plot_by_beta"`;
  - the setup (`plot_by_beta.py:1031-1116` and `:1129-1142`, `plot_ScalarModel.py:1601-1686` and
    `:1688-1701` at `000be0c`) is replaced by one `try:` calling `plot_inputs.parameters`,
    `stored_grid`, `potentials`, `couplings`, `models` and `T_stop` for every model (into
    `T_stop_list`), with `except plot_inputs.PlotOptionNotStored as e:` printing `str(e)` to stderr
    and `sys.exit(2)` inside the `with`, so the pool's `__exit__` closes it. The loop is now
    `for model_data, T_stop in zip(model_list, T_stop_list):`, its body `tags = []` and the
    `run_pipeline(...)` call unchanged. `plot_by_beta.py`'s baseline is unchanged and still follows
    the setup.
- **O4 — `ComputeTargets/tests/test_plot_inputs.py`** (new, 21 tests, about 4 s, no Ray, no
  PRyMordial solve, said in its docstring). Three stores are built once per module, read-write,
  through `StoreFixtures/standin_pool.py`'s `open_pool` with `main.py`'s lookups (classes and
  payloads written out as `main.py` makes them, not taken from `plot_inputs`), closed, and copied
  per test; each copy is reopened `read_only=True` with `read_table_config=`. By class:
  - `TestAStoreMainBuilt` (2): **test 1**, two beta, two M, one Λ, `Exponential`: every function
    returns the stored rows (each `_deserialized`), potentials in M-outer order, couplings in beta
    order, `T_stop` = `T_CMB`, no warning; and a `--T-stop-GeV` the build used is found;
  - `TestAnIncompleteProduct` (1): **test 2**, runs {M1, M2} × {Λ1} then {M3} × {Λ1, Λ2}: (M1, Λ2) and
    (M2, Λ2) skipped with their warnings and "2 of 6"; the other four returned in order;
  - `TestABetaWithoutACoupling` (1): **test 3**, β = 1.0 stored without its coupling: skipped with
    its warning and "1 of 3";
  - `TestAParameterNotStored` (4): **test 4**, `--T-stop-GeV 1e-07` and `--rel-tol 1e-06` not
    stored, and the two constants (the `T_CMB` row and the `QCD_Cosmology` row deleted from a
    copy): `PlotOptionNotStored` with the exact message, `class_name`, and a `ReadOnlyMiss` cause;
  - `TestNothingWritten` (1, 7 subtests): **test 5**, the SHA-256 of every file
    (`standin_pool.store_checksums`) unchanged while the pool is open and after it closes, for each
    of tests 1–4's call sequences, and no `-journal`, `-wal` or `-shm` file;
  - `TestAMissingStore` (1): **test 6**, the read-only open raises `RuntimeError` ("does not
    exist"), the directory is not created, and no actor call is made;
  - `TestTheScripts` (3): **test 7** by `ast`: one `ShardedPool(` per script with `read_only=True`,
    `prune_unvalidated=False`, no `drop_tables`, no `shards`, `job_name` the script's stem; calls of
    all six `plot_inputs` functions and a handler for `plot_inputs.PlotOptionNotStored`, no call
    of `build_model_list` and no `convert_to_potential` or `convert_to_coupling` defined; a
    top-level `if` on `args.drop` ending in `sys.exit(2)`, before 01's check and before `ray.init`;
  - `TestThePayloads` (4), on a pool that records each lookup: the class and payload of every
    lookup, key for key, as the scripts built them (the parameters; the potentials for all four
    `--potential-type`s; the couplings; `T_stop` both ways);
  - `TestOtherRefusalsPropagate` (4), on a pool that refuses every lookup: a `ReadOnlyMiss` of
    another class from a parameter or a potential lookup, and a `ReadOnlyWrite` of another
    operation or class from a coupling lookup, propagate unchanged; an unknown potential type
    raises the old `ValueError`.
- **O5.** `[08-plot-scripts-open-read-write-pools]` moved from the `dk-and-rpk-migration` board §3
  to its §4 with a **Resolved (2026-10-13)** line; that board's header says three issues stay open.
  `.documents/OPEN_ISSUES.md`: the §1.12 row deleted and the section says none is open; §1.11's
  prose and the header's count (27 → 26) and `dk-and-rpk-migration` tally (4 → 3) corrected.
  `test_datastore_config.TestDriversPassShardKeyMapping.EXPECTED_COUNTS` gains
  `"plot_inputs.py": 0` (P6). This board: row 02, O1–O5, the gate, §3.1, §5.3.

## Deviations from the prompt

1. **STRUCTURALLY REQUIRED.** Every model's `T_stop` is looked up in the setup, into
   `T_stop_list`, before `plot_by_beta.py`'s baseline and before the loop, and the loop iterates
   `zip(model_list, T_stop_list)`; the loop's unused `cosmology: BaseCosmology = ...` line went with
   the old lookup. U4 requires a missing parameter to stop the run before any figure; looked up
   inside the loop, a later model's missing `T_stop` would come after the first model's figures
   (and, in `plot_by_beta.py`, after the baseline's PRyMordial solve). With today's one model the
   lookups and their payloads are the same; only their position moved ahead of the baseline.
2. **IMPLEMENTATION CHOICE.** Rows 1–5 are one function, `parameters`, returning a `NamedTuple`
   that unpacks into the scripts' five names. The five lookups are submitted in the scripts' order
   and then read one at a time, where the scripts read them in three `ray.get`s; the payloads are
   unchanged.
3. **IMPLEMENTATION CHOICE.** Each catch is narrowed to the refusal its lookup can make: a
   parameter's and a potential's `ReadOnlyMiss` by `class_name`, the cosmology's by
   `class_name == "QCD_Cosmology"`, and a coupling's `ReadOnlyWrite` by
   `operation == "an insert"` and `class_name == "ExponentialCoupling"`. Reason: `ReadOnlyWrite` is
   also the package's backstop for a hit that writes, which is not a missing coupling and must not
   be skipped silently. The attributes read through Ray's dual exception (`exceptions.py:182-183`).
4. **IMPLEMENTATION CHOICE.** The count line is printed only when something was skipped, so that a
   complete store prints nothing (test 1's "no warning"). Warnings go to stdout by `print`, as
   `pipeline_selection`'s warnings do; the stop message goes to stderr (U4).
5. **IMPLEMENTATION CHOICE.** `PlotOptionNotStored`'s message is the whole line, `!! plot:` prefix
   included, and the scripts print `str(e)`. Option values are formatted `{:.10g}` (`1e-07`,
   `1e-06`, `2.7255`). The wording for the constants (`pi = 0`, `T_CMB`, the cosmology) is mine,
   in the prompt's form.
6. **IMPLEMENTATION CHOICE.** The scripts import the module (`import plot_inputs`) and call
   `plot_inputs.<name>`, so that test 7 can find the calls by `ast`; one `try` covers the whole
   setup.
7. **IMPLEMENTATION CHOICE.** The `--prune-unvalidated` / `--drop` note is a module docstring in each
   script (the prompt's second option): `plot_ScalarModel.py` has no extra arguments, and the
   shared parser is 01's and `config/`'s. The `--drop` message wording is mine.
8. **IMPLEMENTATION CHOICE.** The imports left unused by the move were removed, each named under
   O3 above; `tolerance` stays (the `run_pipeline` annotations), and the local
   `T_init_GeV` / `T_stop_GeV` names went (the options are passed directly).
9. **IMPLEMENTATION CHOICE.** Tests beyond O4's seven: `TestThePayloads` (the §2.2 table as a
   test), `TestOtherRefusalsPropagate` (nothing else is caught), the two constants in test 4, a
   found `--T-stop-GeV` in test 1, and test 7's extra checks (`prune_unvalidated`, `shards`,
   `job_name`, the handler). The stores are built once per module and copied per test, as the
   package's test does.
10. **IMPLEMENTATION CHOICE.** The test pools pass `version_label=VERSION_LABEL` and
    `serial_batch_sizes=` as the scripts do. `StandinCluster.open_pool` always passes `shards=3`
    (the store's count); the scripts pass none (P4), and a read-only open takes the count from the
    primary, so the two opens agree.

No UNINTENDED DRIFT found.

**The orchestrator's facts against my measurement.** Every fact of the brief that I checked held:
the line ranges of the setup at `000be0c`, the textual identity of the two copies, the four
`_do_not_populate` literals in `run_pipeline`, `args.phi_init_Mp` (still passed by both scripts to
`plot_inputs.parameters`), the stand-in's patched `ray.get`, the package's exception lines and the
Ray lines. The baselines at `e0cf6f0` were the brief's counts.

## Verification performed

All from the repository root, serially, in the foreground, output in the agent's scratch directory;
`uptime` load averages beside each.

**Suites.**

| Root | Before, at `e0cf6f0` (load) | After (load) |
|---|---|---|
| CosmologyModels | 18 OK, 77.2 s (3.2) | 18 OK, 76.8 s (7.3) |
| ComputeTargets `test_[a-f]*.py` | 61 OK, 113.5 s (2.5) | 61 OK, 115.2 s (4.0) |
| ComputeTargets `test_[g-z]*.py` | 70 OK, 114.4 s (2.7) | 91 OK, 119.4 s (3.1) |
| ObjectFactories | 63 OK, 5.0 s (3.7) | 63 OK, 5.0 s (3.9) |
| tools | 34 OK, 7.9 s (4.0) | 34 OK, 8.1 s (4.2) |

ComputeTargets 131 → 152 (`test_plot_inputs.py` sorts into the second half: 70 + 21 = 91). No count
fell; no failure, so nothing was re-run.

**§2.2 — the payloads are unchanged.** Old: `plot_by_beta.py` at `000be0c` (`plot_ScalarModel.py`'s
copy is textually identical, at `:1601-1701`). New: `plot_inputs.py` at this commit. Each old
vectorized item is now one lookup's keyword payload; the actor turns a scalar get into
`payload_data=[kwargs]` and builds each item alike (`datastorekit/SQL/Datastore.py:507-590` at
`v0.3.0`), and the pool's vectorized sharded get already sent `object_get.remote(cls_name, **item)`
per item (`ShardedPool.py:3365-3387`).

| Row | Old (file:line) | Old class and payload | New (file:line) | New class and payload | Equal |
|---|---|---|---|---|---|
| 1 | `plot_by_beta.py:1034` | `tolerance` (the class), `tol=args.abs_tol` | `plot_inputs.py:122` | `tolerance` (the class), `tol=abs_tol` | yes |
| 2 | `:1035` | `tolerance`, `tol=args.rel_tol` | `:128` | `tolerance`, `tol=rel_tol` | yes |
| 3 | `:1046` | `"temperature"`, `value=T_init_GeV * units.GeV, units=units` | `:134` | the same | yes |
| 4 | `:1051-1055` | `"phi_value"`, `value=args.phi_init_Mp * units.PlanckMass, units=units` | `:140-144` | the same, from `phi_init_Mp` | yes |
| 5 | `:1056` | `"pi_value"`, `value=0.0, units=units` | `:150` | the same | yes |
| 6 | `:1062-1092` | per item of `payload_data`: `{"M", "Lambda", "n": 1, "units"}` (Exponential `:1065`, InversePower `:1073`), `{"M", "Lambda", "units"}` (Starobinsky `:1081`, Recliner `:1089`), class by type, over `itertools.product(M_table, Lambda_table)` | `:54-58`, `:175-180`, `:201-206` | the same four classes and dicts, one lookup per combination, same product order | yes |
| 7 | `:1099-1104` | `"ExponentialCoupling"`, per item `{"shard_key": beta, "beta": beta, "units": units}` over `beta_table` | `:243` | `"ExponentialCoupling"`, `shard_key=beta, beta=beta, units=units`, one lookup per beta | yes |
| 8 | `:1116` | `build_model_list(pool, units)` | `:272` | the same call (`config/model_list.py` unchanged) | yes |
| 9 | `:1132-1140` | `"temperature"`, `value=T_CMB, units=units` (`T_CMB = cosmology._params.T_CMB_Kelvin * units.Kelvin`), or `value=T_stop_GeV * units.GeV, units=units` | `:291-301` | the same two | yes |
| 17 | `:1107-1109` | `read_table("beta_value")`, `("M_value", units)`, `("Lambda_value", units)`, read in turn | `:168-172` | the same three, read in turn | yes |

**Verdict: every payload equal key for key.** `TestThePayloads` asserts rows 1–7 and 9 on a recording
pool, for all four potential types.

**The scripts by `ast` against `000be0c`** (scratch script, `difflib` over `ast.unparse` of the
top-level statements and of the `with` body). `plot_by_beta.py`: the docstring in, three imports
out, `import plot_inputs` in, the `--drop` `if` in, and the `with` statement; inside it, the old
setup statements replaced by the one `try`, `units = Planck_units()`, the baseline's two statements
and the loop's last two statements (`tags = []`, `run_pipeline(...)`) equal. `plot_ScalarModel.py`:
the same, without the baseline. Every function definition (`run_pipeline`, the figure builders)
and every other top-level statement is equal. The pool's keywords differ by `read_only=True` and,
in `plot_by_beta.py`, `job_name`.

**§2.3 — P2's cost** (scratch `p2_timing.py`; read-only stand-in; median of 7 passes of the
potential and coupling lookups; load about 6; relative only, since the stand-in runs every actor
call in-process):

| Store | Lookups | Actor calls per pass, vectorized / one at a time | Vectorized | One at a time | Ratio |
|---|---|---|---|---|---|
| test 1's (2 β × 2 M × 1 Λ) | 2 potentials, 2 couplings | 3 / 4 | 0.5 ms | 0.5 ms | 1.01 |
| 20 β × 5 M × 4 Λ | 20 potentials, 20 couplings | 21 / 40 | 3.8 ms | 4.9 ms | 1.29 |

The couplings cost the same calls either way (the pool already split the vectorized sharded get into
one call per beta); the potentials go from one call to one per combination. Every file of both
stores was unchanged after the timing.

**The text tests (P6).**
- `test_datastore_config.TestDriversPassShardKeyMapping.EXPECTED_COUNTS` gains
  `"plot_inputs.py": 0`: the plot scripts' setup now lives there, so "no `object_get_vectorized` in
  the plot scripts" covers it. The only existing test changed.
- Passing unchanged: `test_extraction.TestDriverReadsTheColumns` (the four `"_do_not_populate":
  True` literals are in `run_pipeline`, untouched); `test_kinematic_cap_loop`'s stepper label;
  `test_initial_field_option.TestNoLiteralLeft` (both scripts still read `args.phi_init_Mp`,
  passed to `plot_inputs.parameters`); `test_datastore_config.TestPoolsAreGivenTheConfig`;
  `test_package_provenance.TestDriversPrintProvenance`; `test_ray_address.TestCheckPlacementByAst`.
  Also read and passing, though not in README §0.3's list: `test_foreign_bbn_provenance`,
  `test_network_flag` (the baseline call) and `test_version_keyed_lookups`' `VERSION_LABEL` check.
  Run together before the suites: 65 OK.

**Other checks.**
- `./venv/bin/black --check` on the five Python files touched: "5 files would be left unchanged".
- `./venv/bin/python -m py_compile plot_by_beta.py plot_ScalarModel.py plot_inputs.py`: ok.
- `git grep -c object_get_vectorized plot_inputs.py plot_by_beta.py plot_ScalarModel.py`: none.
- `.documents/OPEN_ISSUES.md`: `grep -c '^- `\['` gives 26; §1.12 says none is open and agrees with
  both boards.
- No Ray started; no plot script run; no store opened outside a `tempfile` directory.

## The deliberate-breakage record

Each diff was taken against the tree of this commit (staged), applied, tested with
`test_plot_inputs`, and reverted with `git apply -R`; each was checked with `git apply --check`
(exit 0 on the clean tree) and `git apply -R --check` (exit 1 on the clean tree). Replay from a
clean tree at this commit with `bash -c 'git apply B.diff'`, and restore with `git apply -R B.diff`.

**1. `read_only=True` removed from the test pools:** test 5,
`TestNothingWritten.test_every_file_unchanged_after_tests_1_to_4s_calls`, fails in all seven
subtests (the primary's SHA-256 changes while the pool is open, through the read-write pool's
replication records). Tests 2, 3, 4 and 6 fail too (a miss inserts instead of being refused; a
missing store is created); test 1 passes (every lookup hits).

```diff
diff --git a/ComputeTargets/tests/test_plot_inputs.py b/ComputeTargets/tests/test_plot_inputs.py
index 2512871..57e42a5 100644
--- a/ComputeTargets/tests/test_plot_inputs.py
+++ b/ComputeTargets/tests/test_plot_inputs.py
@@ -279,7 +279,6 @@ class _ReadOnlyCase(unittest.TestCase):
             pool = self.cluster.open_pool(
                 primary,
                 version_label=VERSION_LABEL,
-                read_only=True,
                 read_table_config=read_table_config,
                 serial_batch_sizes=serial_batch_sizes,
             )
```

**2. The per-combination potential lookup reverted to one vectorized call:** test 2,
`TestAnIncompleteProduct`, errors with `ReadOnlyMiss` (one miss refuses the whole call), and so does
test 5's `incomplete` subtest; `TestThePayloads.test_the_potentials` fails for all four types.

```diff
diff --git a/plot_inputs.py b/plot_inputs.py
index fe5a293..e3768e8 100644
--- a/plot_inputs.py
+++ b/plot_inputs.py
@@ -198,26 +198,16 @@ def potentials(
     cls_name, _ = POTENTIAL_CLASSES[potential_type]
 
     combinations = list(itertools.product(M_table, Lambda_table))
-    refs = [
-        pool.object_get(
-            cls_name, **_potential_payload(potential_type, M, Lambda, units)
-        )
-        for M, Lambda in combinations
-    ]
+    ref = pool.object_get(
+        cls_name,
+        payload_data=[
+            _potential_payload(potential_type, M, Lambda, units)
+            for M, Lambda in combinations
+        ],
+    )
 
-    found = []
+    found = ray.get(ref)
     skipped = 0
-    for (M, Lambda), ref in zip(combinations, refs):
-        try:
-            found.append(ray.get(ref))
-        except ReadOnlyMiss as e:
-            if e.class_name != cls_name:
-                raise
-            skipped += 1
-            print(
-                f"!! plot: no {cls_name} stored for M={_value(M.value / units.eV)} eV, "
-                f"Λ={_value(Lambda.value / units.eV)} eV; no history can use it; skipped"
-            )
     if skipped > 0:
         print(
             f"!! plot: {skipped} of {len(combinations)} {cls_name} combinations of the stored M "
```

**3. The parameter `ReadOnlyMiss` swallowed:** test 4's `test_a_T_stop_the_build_did_not_use`,
`test_a_rel_tol_the_build_did_not_use` and `test_T_CMB_not_stored_names_the_constant` fail (no
exception). The cosmology test does not go through `_get_parameter` and passes.

```diff
diff --git a/plot_inputs.py b/plot_inputs.py
index fe5a293..4800702 100644
--- a/plot_inputs.py
+++ b/plot_inputs.py
@@ -92,7 +92,7 @@ def _get_parameter(ref, class_name: str, option: str, value: str):
     except ReadOnlyMiss as e:
         if e.class_name != class_name:
             raise
-        raise PlotOptionNotStored(class_name, option, value) from e
+        return None
 
 
 class PlotParameters(NamedTuple):
```

**4. `read_only=True` removed from `plot_ScalarModel.py`'s `ShardedPool(`:** test 7,
`TestTheScripts.test_the_pool_is_read_only_with_no_drop_tables` (`script='plot_ScalarModel.py'`),
fails, and nothing else.

```diff
diff --git a/plot_ScalarModel.py b/plot_ScalarModel.py
index 2ea4115..68919a7 100644
--- a/plot_ScalarModel.py
+++ b/plot_ScalarModel.py
@@ -1614,7 +1614,6 @@ with ShardedPool(
     profile_agent=profile_agent,
     job_name="plot_ScalarModel",
     prune_unvalidated=False,
-    read_only=True,
     read_table_config=read_table_config,
     factories=factories,
     serial_batch_sizes=serial_batch_sizes,
```

## Observations not acted on

1. `plot_by_beta.py`'s `ProfileAgent` label still reads `--plot_ScalarModel-primarydb-...`
   (`plot_by_beta.py`, the `profile_agent` block after `ray.init`); the prompt renames `job_name`
   only. Cosmetic.
2. `--potential-type` has no default (`config/argument_parser.py:98-103`): a plot run without it
   raises `ValueError: Unknown potential type: None` after the parameter lookups, as before this
   prompt. Unchanged.
3. A refusal at open (a missing store, a journal, another `VERSION_LABEL`, U6) reaches the user as
   the package's `RuntimeError` with a traceback; for a missing store the message is
   `Cannot read sharded datastore "<path>": the primary "<path>" does not exist. Nothing was read
   or repaired`. Catching it for a neater message was not asked for (brief).
4. A read-only replicated get draws a random shard per lookup
   (`ShardedPool._get_impl_replicated_table_read_only`), so the one-at-a-time potential lookups are
   spread over the shards; nothing depends on which shard answers.

## State handed to the next prompt

- Both plot scripts open `ShardedPool(read_only=True)` (`prune_unvalidated=False`, no
  `drop_tables`, no `shards=`) and read their setup through `plot_inputs.py`. A parameter the build
  did not store stops the run with `!! plot: the store holds no ...` on stderr and exit 2, before
  the baseline and any figure; a potential or coupling never stored is skipped with `!! plot: no
  ... stored for ...; skipped` and a count on stdout. `--drop` is refused with exit 2 before Ray
  starts; `--prune-unvalidated` is ignored.
- 03 runs the scripts as left here, with `--ray-address 127.0.0.1:6380`. Under real Ray a refused
  lookup reaches `ray.get` as an instance of its cause's class with the cause's attributes (read at
  Ray 2.53.0, not run); 03 is where it is seen. H3's three refusals are, in the scripts:
  `auto` (01's check), a `--T-stop-GeV` the build did not use (`PlotOptionNotStored`, exit 2), and
  `--drop` (the new check, exit 2).
- The pool arguments, the history lookups (rows 10–16), `run_pipeline`, the baseline, the figures
  and `histories.csv` are unchanged, so 03's `histories.csv` should equal R0's.
- Suites at this commit: CosmologyModels 18, ComputeTargets 152 (61 + 91), ObjectFactories 63,
  tools 34. `test_plot_inputs.py` takes about 4 s.
- `[08-plot-scripts-open-read-write-pools]` is closed (`dk-and-rpk-migration` board §4);
  `.documents/OPEN_ISSUES.md` holds 26, with §1.12 empty. No issue opened.
