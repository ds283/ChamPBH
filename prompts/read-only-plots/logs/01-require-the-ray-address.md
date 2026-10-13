# Log 01 — require the Ray address

**Subject:** Require `--ray-address` before Ray starts, and refuse `auto`
**Commit:** `04486f4`, on branch `read-only-plots`, on top of `8c5a7f1`
**Date:** 2026-10-13, by Claude Sonnet 5.5
**Result:** **COMPLETE.** A1–A4 shipped. ComputeTargets 114 → 131 (+17 `test_ray_address`:
10 without a subprocess, 7 by subprocess); CosmologyModels 18, ObjectFactories 63 and tools 34
unchanged. All four deliberate breakages caught. No §4 stop condition was met. No issue opened.

## What shipped

- **A1 — `config/argument_parser.py`.** `DEFAULT_RAY_ADDRESS` deleted (nothing else read it).
  `--ray-address` has `default=None` and a help text saying it is required whenever Ray starts,
  that `main.py --inventory` needs none, that ChamPBH's cluster is `127.0.0.1:6380`, that `local`
  starts a private instance, and that `auto` is refused. New: the module constant
  `RAY_ADDRESS_PROBLEM` (the message, the prompt's wording unchanged) and
  `ray_address_problem(address: Optional[str]) -> Optional[str]`: it returns the message for
  `None`, for a string that is empty or whitespace only, and for `auto` in any case with any
  surrounding whitespace; it returns `None` for everything else, unchanged (`local`, `host:port`,
  `ray://...`).
- **A2 — the drivers.** In each of `main.py`, `plot_by_beta.py`, `plot_ScalarModel.py`: the import
  of `ray_address_problem`, and, immediately before the one `ray.init`, `_ray_problem =
  ray_address_problem(args.ray_address)` followed by `if _ray_problem is not None:` printing it to
  stderr and `sys.exit(2)`. The comment above `ray.init`, which said the address "defaults to
  'auto'", is replaced. In `main.py` the check is after the `--inventory` branch and the `--drop`
  check; in the plot scripts it is after the provenance line. Nothing else changed.
- **A3 — `ComputeTargets/tests/test_ray_address.py`**, 17 tests, no Ray:
  - `TestRayAddressProblem` (3): the refused set (`None`, `""`, `"   "`, `auto`, `AUTO`, ` AUTO `,
    `Auto\n`); the message names `127.0.0.1:6380`, `ray_cluster.sh`, `'local'` and why `auto` is
    refused; the accepted set (`127.0.0.1:6380`, `local`, `ray://127.0.0.1:10001`, `auto2`).
  - `TestParser` (3): no default; a command-line value is kept; a YAML `--config` supplies it.
  - `TestDriversBySubprocess` (7, 30.7 s at load ~41; ~5 s each): the two plot scripts and
    `main.py`, each with no address and with `--ray-address auto` (exit 2, the message on stderr,
    `>> packages:` on stdout so the driver got past the parser, no `Traceback`, no file created in
    the temp directory); and `main.py --inventory` with no address on an absent store (exit 1, the
    reader's `!! --inventory:` refusal, no mention of `--ray-address`: U1's exemption).
  - `TestCheckPlacementByAst` (4): in each driver exactly one top-level check and one `ray.init`,
    the check before it, followed by an `if` on the check's result containing `sys.exit(2)`;
    in `main.py` after the `if args.inventory:` block and after the `--drop` check.
- **A4.** `CLAUDE.md` "Ray on this machine" says the drivers refuse to start Ray without
  `--ray-address`, refuse `auto`, and that `main.py --inventory` needs none.
  `.documents/architecture-summary.md` §5.1's line now reads "no default; required whenever Ray
  starts, `auto` is refused, `main.py --inventory` needs none".
- **The three parse-only tests** (`test_extraction.py:331-336`, `test_initial_field_option.py:115-118`,
  `test_pipeline_selection.py:157-162`) pass unchanged. No existing test is modified.

## Deviations from the prompt

1. **IMPLEMENTATION CHOICE.** The message is held in a module constant `RAY_ADDRESS_PROBLEM`, and
   the tests import it. The prompt asked for a function returning the message; it does, and the
   constant lets the tests compare the exact text. The wording is the prompt's, untightened.
2. **IMPLEMENTATION CHOICE.** The subprocesses run with `RAY_ADDRESS=127.0.0.1:9` (the brief's
   guard, adopted): Ray reads `RAY_ADDRESS` when given `None` or `auto`, so a regression past the
   check would fail to connect rather than attach to another cluster. The assertions do not rely
   on it.
3. **IMPLEMENTATION CHOICE.** The check's result is bound to a module-level name `_ray_problem`
   in each driver (a three-line statement, not a walrus or a helper), so the `ast` test can look
   for the assignment and its guard.
4. **IMPLEMENTATION CHOICE.** The comment above each `ray.init` is rewritten as a two-line comment
   naming this prompt, not only corrected.
5. **STRUCTURALLY REQUIRED.** `--ray-address` help text is longer than "specify address of Ray
   cluster": A1 asks for it to name the forms and the refusal.
6. **STRUCTURALLY REQUIRED.** `from typing import Optional` is added to `config/argument_parser.py`
   (the function's signature).

No UNINTENDED DRIFT found.

## Verification performed

All from the repository root, serially, in the foreground, output in the agent's scratch directory;
`uptime` load averages beside each.

| Root | Before (load) | After (load) |
|---|---|---|
| CosmologyModels | 18 OK, 132.1 s (24) | 18 OK, 504.6 s (64.8 at start; 350 at its end, from other load) |
| ComputeTargets `test_[a-f]*.py` | 61 OK, 236.7 s (23.5) | 61 OK, 317.9 s (60) |
| ComputeTargets `test_[g-z]*.py` | 53 OK, 190.2 s (41.6) | 70 OK, 256.7 s (47.7) |
| ObjectFactories | 63 OK, 11.1 s (27) | 63 OK, 11.1 s (58) |
| tools | 34 OK, 16.0 s (29) | 34 OK, 17.1 s (54) |

ComputeTargets: 114 → 131. No count fell.

- **`git grep -n "ray_address\|ray-address\|DEFAULT_RAY_ADDRESS" -- ':!prompts'`.** Before: 26 lines,
  as the brief says. After: 33 lines outside the new test module, and 20 inside it. Classification
  of the lines outside it:
  - `config/argument_parser.py`: the switch (`:311`), the message (`:323`), the function (`:331`);
    `DEFAULT_RAY_ADDRESS` (`:11`) is gone.
  - `main.py:54,129,133`, `plot_by_beta.py:40,102,106`, `plot_ScalarModel.py:47,82,86`: the
    import, the check, and the unchanged `ray.init(address=args.ray_address)`.
  - `CLAUDE.md:121,123`: the existing rule, and the new sentence.
  - `.documents/architecture-summary.md:451` (corrected), `:868` (`ray.init`, still true).
  - `tools/tests/test_inventory_report.py:397`: `main.py --ray-address NO_RAY`, unchanged.
  - the rehearsal scripts (`build.sh:33,39,99`, `run_readers.sh:31,111,116,122`,
    `ray_cluster.sh:105`) and the dated records (`dk-and-rpk-migration-verification.md:413,782`,
    `review-remediation-verification.md:1425-1464`): unchanged; dated, per `CLAUDE.md` invariant 6.
- **`black --check`** on the five Python files touched: "5 files would be left unchanged".
- **Byte-compile.** `./venv/bin/python -m py_compile` on `main.py`, `plot_by_beta.py`,
  `plot_ScalarModel.py` and `config/argument_parser.py`: ok.
- **The rehearsal scripts still pass the switch** (read, not run): `build.sh:99`
  (`--ray-address "$ADDRESS"`), `run_readers.sh:111` (the `--inventory` call, which ignores it),
  `:116`, `:122`.
- **`ray_cluster.sh status`** before work: lock free, no ChamPBH process. `pgrep -lf
  'gcs_server|raylet|ray::'` after the subprocess tests: nothing. All three drivers carried the
  check before the first subprocess test ran.
- **Ordering of the work.** Baselines before any edit; the check written into all three drivers
  before any subprocess test; then the breakages.

## The deliberate-breakage record

Each diff was taken against the tree at `04486f4` (staged, before the docs), checked with
`git apply --check` (forward, from the clean tree: exit 0) and `git apply -R --check` (exit 1 on
the clean tree, as it must), applied, tested, and reverted with `git apply -R`. Replay from a clean
tree at `04486f4` with `bash -c 'git apply B.diff'` and `git apply -R B.diff` to restore.
Breakages 2-4 ran only the non-subprocess classes (`TestRayAddressProblem`, `TestParser`,
`TestCheckPlacementByAst`); breakage 1 ran the whole module (17 tests, 28 s), safe since the check
still refuses `auto`.

**1. The default restored to `"auto"`:** fails `TestParser.test_no_default`, and nothing else
(the subprocess tests still pass, since the check refuses `auto`).

```diff
diff --git a/config/argument_parser.py b/config/argument_parser.py
index 0a535ab..11eff6e 100644
--- a/config/argument_parser.py
+++ b/config/argument_parser.py
@@ -309,7 +309,7 @@ def create_argument_parser() -> configargparse.ArgumentParser:
     )
     parser.add_argument(
         "--ray-address",
-        default=None,
+        default="auto",
         type=str,
         help="address of the Ray cluster; required whenever Ray starts (main.py --inventory starts no "
         "Ray and needs none). On this machine ChamPBH's cluster is 127.0.0.1:6380; 'local' starts a "
```

**2. `main.py`'s check after `ray.init`:** fails `TestCheckPlacementByAst.test_check_comes_before_ray_init_and_exits_2`
(`driver='main.py'`).

```diff
diff --git a/main.py b/main.py
index f7fa483..8b9fc28 100644
--- a/main.py
+++ b/main.py
@@ -124,13 +124,13 @@ except ValueError as e:
     print(f"!! --drop: {e}", file=sys.stderr)
     sys.exit(2)
 
+ray.init(address=args.ray_address)
 # connect to the Ray cluster at the supplied address. There is no default: a missing address or
 # 'auto' is refused before Ray starts (read-only-plots prompt 01; README U1, U2)
 _ray_problem = ray_address_problem(args.ray_address)
 if _ray_problem is not None:
     print(_ray_problem, file=sys.stderr)
     sys.exit(2)
-ray.init(address=args.ray_address)
 
 # instantiate a ProfileAgent to profile database operations; this is passed as an argument to ShardedPool below
 profile_agent = None
```

**3. `auto` accepted:** fails `TestRayAddressProblem.test_refused` (four `auto` spellings) and
`test_message_names_both_forms_and_the_reason` (the call returns `None`).

```diff
diff --git a/config/argument_parser.py b/config/argument_parser.py
index 0a535ab..4a48b3a 100644
--- a/config/argument_parser.py
+++ b/config/argument_parser.py
@@ -334,6 +334,6 @@ def ray_address_problem(address: Optional[str]) -> Optional[str]:
     A missing or empty address is refused, and so is 'auto' (any case, surrounding whitespace
     ignored). Anything else is accepted unchanged; Ray checks the address itself.
     """
-    if address is None or address.strip().lower() in ("", "auto"):
+    if address is None or address.strip().lower() in ("",):
         return RAY_ADDRESS_PROBLEM
     return None
```

**4. The check removed from `plot_ScalarModel.py`:** fails
`TestCheckPlacementByAst.test_check_comes_before_ray_init_and_exits_2` and
`test_the_guard_tests_the_checks_result` (`driver='plot_ScalarModel.py'`).

```diff
diff --git a/plot_ScalarModel.py b/plot_ScalarModel.py
index 819ba7a..1bb9c6d 100644
--- a/plot_ScalarModel.py
+++ b/plot_ScalarModel.py
@@ -79,10 +79,6 @@ print(format_package_provenance(installed_package_provenance()), flush=True)
 
 # connect to the Ray cluster at the supplied address. There is no default: a missing address or
 # 'auto' is refused before Ray starts (read-only-plots prompt 01; README U1, U2)
-_ray_problem = ray_address_problem(args.ray_address)
-if _ray_problem is not None:
-    print(_ray_problem, file=sys.stderr)
-    sys.exit(2)
 ray.init(address=args.ray_address)
 
 # instantiate a Datastore actor: this runs on its own node, and acts as a broker between
```

## Observations not acted on

- The comment text "ray.init(address=args.ray_address)" in `architecture-summary.md:868` is still
  true and untouched.
- `if args.database is None: parser.print_help(); sys.exit()` in each driver is dead code
  (`--database` is `required=True`); not touched.
- The module-level `_ray_problem` name stays defined in each driver after the check; harmless.

## State handed to the next prompt

- The three drivers refuse a missing, empty or `auto` `--ray-address` with exit 2 and the message
  on stderr before `ray.init`; `main.py --inventory` is exempt. `config.argument_parser.
  ray_address_problem` and `RAY_ADDRESS_PROBLEM` exist.
- **Every driver run now needs `--ray-address`.** 02's tests that run a driver (if any) and 03's
  rehearsals must pass `--ray-address 127.0.0.1:6380` (03: after `ray_cluster.sh start`), or
  `local` for a private instance; never `auto`.
- `plot_by_beta.py:~102-106` and `plot_ScalarModel.py:~82-86` hold the check, then `ray.init`: 02
  edits the pool opening further down (`plot_by_beta.py:~1011+`, `plot_ScalarModel.py:~1581+`, line
  numbers shifted by about 6) and must leave the check and `ray.init` as they are
  (`TestCheckPlacementByAst`).
- Suites at `04486f4`: CosmologyModels 18, ComputeTargets 131 (61 + 70), ObjectFactories 63,
  tools 34. `test_ray_address.py` is in the second half (`test_[g-z]*.py`): 53 + 17 = 70. Its
  subprocess class takes about 30 s.
- No issue opened; `.documents/OPEN_ISSUES.md` unchanged (27).
