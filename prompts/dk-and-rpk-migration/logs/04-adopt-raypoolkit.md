# Log 04 — adopt `raypoolkit`

**Subject:** Adopt raypoolkit's RayWorkPool and delete RayTools
**Commit:** `303324b`, on branch `dk-and-rpk-migration`, on top of `64ef9aa` (code as at `12c82e8`)
**Date:** 2026-10-12, by Claude Sonnet 5.5
**Result:** **COMPLETE.**
- W1: `RayTools/RayWorkPool.py` and `RayTools/__init__.py` deleted, and no `RayTools` directory
  left in the working tree.
- W2–W3: three imports rewritten, one line each, in place; no pool site changed.
- W4: `.documents/architecture-summary.md` made true for `v0.2.0`.
- RayPoolKit's script, at a commit of the staged tree: 0 imports, 0 string literals, 0
  `import_module` calls naming `RayTools`; 17 calls, `constructs` 17.
- The imports resolve (162 names, 0 failures), the drivers byte-compile, the suites are at
  18 / 114 / 66, and no §4 stop condition was met. No §3 issue opened.

## What shipped

`git diff --cached --name-only`, as committed (plus this log and the board):

```text
.documents/architecture-summary.md
RayTools/RayWorkPool.py
RayTools/__init__.py
main.py
plot_ScalarModel.py
plot_by_beta.py
```

The drivers' diff (`git diff --cached -U1`), the one import line each:

```diff
diff --git a/main.py b/main.py
index 44075e6..743c28d 100644
--- a/main.py
+++ b/main.py
@@ -49,3 +49,3 @@ from Datastore.SQL.ShardedPool import ShardedPool
 from Quadrature.integration_metadata import IntegrationSolver
-from RayTools.RayWorkPool import RayWorkPool
+from raypoolkit import RayWorkPool
 from Units import Planck_units
diff --git a/plot_ScalarModel.py b/plot_ScalarModel.py
index 6b6a717..4980621 100644
--- a/plot_ScalarModel.py
+++ b/plot_ScalarModel.py
@@ -43,3 +43,3 @@ from Datastore.SQL.ShardedPool import ShardedPool
 from MetadataConcepts import tolerance, store_tag
-from RayTools.RayWorkPool import RayWorkPool
+from raypoolkit import RayWorkPool
 from Units import Planck_units
diff --git a/plot_by_beta.py b/plot_by_beta.py
index 0342986..c15d19a 100644
--- a/plot_by_beta.py
+++ b/plot_by_beta.py
@@ -37,3 +37,3 @@ from Datastore.SQL.ShardedPool import ShardedPool
 from MetadataConcepts import tolerance, store_tag
-from RayTools.RayWorkPool import RayWorkPool
+from raypoolkit import RayWorkPool
 from Units import Planck_units
```

The 14 lookup-only sites keep `store_handler=None`; the 3 compute sites (`main.py:284`, `:541`,
`:828`) keep their handlers and use the defaults (P7).

The `architecture-summary.md` diff:

```diff
diff --git a/.documents/architecture-summary.md b/.documents/architecture-summary.md
index c8c70c9..4981186 100644
--- a/.documents/architecture-summary.md
+++ b/.documents/architecture-summary.md
@@ -32,8 +32,7 @@ ChamPBH/
 ├── starobinsky.yaml
 ├── recliner.yaml
 │
-├── RayTools/
-│   └── RayWorkPool.py               # Generic distributed work queue
+├── (the work queue, `RayWorkPool`, is the `raypoolkit` package, pinned in requirements.txt)
 │
 ├── Datastore/
 │   ├── object.py                    # DatastoreObject base class
@@ -150,7 +149,7 @@ ChamPBH/
 
 All expensive computations are dispatched as Ray remote tasks or actors. The driver (main.py) never blocks waiting for a single result; it maintains a bounded queue of in-flight Ray `ObjectRef`s and processes completions as they arrive. This allows the driver to pipeline database lookups, compute tasks, and store tasks concurrently.
 
-### 3.2 `RayWorkPool` (`RayTools/RayWorkPool.py`)
+### 3.2 `RayWorkPool` (`raypoolkit`)
 
 `RayWorkPool` is the single generic orchestrator for all computation phases. It is instantiated with a list of work items and a set of handler callables, and its `run()` method drives a state machine until every item has been processed.
 
@@ -158,11 +157,12 @@ All expensive computations are dispatched as Ray remote tasks or actors. The dri
 
 | Parameter | Type | Default | Purpose |
 |-----------|------|---------|---------|
-| `pool` | `ShardedPool` | required | Database access |
+| `pool` | `raypoolkit.DatastorePool` (a `typing.Protocol` with one method, `object_store(obj)`; a type hint, not checked at run time) | required | Database access |
 | `task_list` | iterable | required | Items to process |
 | `task_builder` | `Callable(item) → ObjectRef \| list[ObjectRef] \| None` | required | Converts each work item to a Ray DB lookup |
 | `compute_handler` | `Callable(obj, label?, payload?) → ObjectRef` | `obj.compute()` | Dispatches ODE/computation |
-| `store_handler` | `Callable(obj, pool) → ObjectRef` | `pool.object_store(obj)` | Persists result |
+| `store_handler` | `Callable(obj, pool) → None` | `obj.store()` | Local store step, run on the driver when the compute task has completed; its return is ignored |
+| `persist_handler` | `Callable(obj, pool) → ObjectRef` | `pool.object_store(obj)` | Datastore round-trip, run straight after the store handler; the ref's value is the persisted object |
 | `available_handler` | `Callable(obj) → ObjectRef` | None | Optional: handle already-computed objects |
 | `validation_handler` | `Callable(obj) → ObjectRef` | None | Optional: post-store integrity check |
 | `post_handler` | `Callable(obj) → obj?` | None | Optional: called at task exit |
@@ -177,7 +177,7 @@ All expensive computations are dispatched as Ray remote tasks or actors. The dri
 
 **Task lifecycle (state machine):**
 
-Each work item passes through states tracked by the string tag in `self._data[ref.hex]`:
+Each work item passes through states tracked by a tag, keyed on each ref's value:
 
 ```
 task_builder(item)
@@ -192,8 +192,8 @@ task_builder(item)
     → post_handler exit
 
 "compute" completes:
-    obj.store()  (mutates obj in-place)
-    store_handler(obj) → ObjectRef tagged "store"
+    store_handler(obj, pool)  (default obj.store(); mutates obj in-place)
+    persist_handler(obj, pool) → ObjectRef tagged "store"
 
 "store" completes:
     replacement_obj = ray.get(ref)  (has store_id set)
@@ -582,7 +582,7 @@ Every compute target follows the same pattern:
 4. **Has a `store()` method** that populates the object's internal data fields after `ray.get(compute_ref)` completes.
 5. **Has a `failure` property** to indicate that computation was attempted but failed (distinguishing "not yet computed" from "failed to compute").
 
-The `RayWorkPool` handles steps 3–4 automatically via its default `compute_handler` and `store_handler`.
+The `RayWorkPool` handles steps 3–4 automatically via its three defaults: `compute_handler`, `store_handler` and `persist_handler`.
 
 ### 7.2 `ScalarModel` (`ComputeTargets/ScalarModel.py`)
 
```

Beside the lines the prompt names, the diff has one the brief adds: the lifecycle block's
`obj.store()` / `store_handler(obj)` pair (old `:195-196`), which named the store handler for what
`v0.2.0` calls the persist step. Each statement agrees with the package's README at `v0.2.0`
("The constructor") and its source: `pool` is `raypoolkit.DatastorePool`, a `typing.Protocol`
with one method, `object_store(obj)`, not checked at run time; `store_handler` default
`obj.store()`, given `(obj, pool)`, return ignored; `persist_handler` default
`pool.object_store(obj)`, given `(obj, pool)` straight after, returning an `ObjectRef` whose value is
the persisted object. The other rows (sizes 5 / 1 / 200, `notify_batch_size` 500,
`notify_time_interval` 300, `title`, `store_results`) already agreed and are untouched. The lines
checklist item 9 lists as staying true were not edited.

Nothing was changed in RayPoolKit (read only; its `git status` is clean at `1b84df0`). No Ray was
started, no driver was imported or run, and no store was opened. The venv was not touched.

## Deviations from the prompt

- **D1 — STRUCTURALLY REQUIRED.** `RayTools/__pycache__/` (ignored; four `.pyc` files) survives
  `git rm` and keeps the directory. It was removed with `rm -rf`, then the empty `RayTools/` with
  `rmdir`, so that no `RayTools` directory remains (brief §0, "The tree"). It is generated
  bytecode of the deleted module; nothing else was under it.
- **D2 — STRUCTURALLY REQUIRED.** The lifecycle block's `:195-196` (above) is corrected as part of
  W4's "lifecycle text", which the prompt cites only by `:180`. The swap makes it false (P14).
- **D3 — IMPLEMENTATION CHOICE.** The throwaway commit is made with `git write-tree` and
  `git commit-tree`, on no branch, rather than on a temporary branch. It is dangling and
  `git gc` collects it. It touches neither the index nor the working tree.
- **D4 — IMPLEMENTATION CHOICE.** The layout (`architecture-summary.md:35`) has no `RayTools/`
  entry; one parenthetical line in its place says the queue is the `raypoolkit` package, pinned
  in `requirements.txt`. The `persist_handler` row is added to the constructor table, and the
  `pool` row's type is corrected. The table's missing `notify_min_time_interval` row is not added
  (the brief allows leaving it out).
- **D5 — IMPLEMENTATION CHOICE.** A fourth breakage (`raypoolkit` unavailable) is run, as the brief
  adds it from README §2.2; it is recorded as a command and its output, not as a diff.
- **D6 — IMPLEMENTATION CHOICE.** ComputeTargets was run in its two pattern groups (61 + 53), not
  whole; the brief allows it.
- **D7 — IMPLEMENTATION CHOICE.** `HEAD` advanced from `12c82e8` to `64ef9aa` ("Require prompt 09
  to prove every class is covered by a spec", the orchestrator's) while this prompt was run. That
  commit changes only `prompts/dk-and-rpk-migration/README.md` (§6.5, 09's acceptance item 5) and the
  board's decisions section; it touches no code. This commit is on top of it. The measurements, the
  suites and the throwaway commits were made at `12c82e8`'s code, which `64ef9aa` leaves identical;
  the script is also run at `303324b` (below).
- **Reading of §2.3.5** ("no test names `RayTools` or `RayWorkPool`"). `Datastore/tests/test_compare_stores.py:1163`
  is a comment in a fixture ("RayWorkPool and build.sh print; …"), added by 01. It imports and
  constructs nothing, so it is not a §4 stop, and the test is not edited.

No other deviation.

## Verification performed

Each command was run from the repository root (RayPoolKit's script from RayPoolKit's root, with
its own venv, whose `raypoolkit` is an editable install of the tag's code at `1b84df0`; the
header reads `Verdicts by: raypoolkit 0.2.0`). The script reads git, not the working tree.

**Before (§2.1).**
- Tree: branch `dk-and-rpk-migration`, `HEAD` `12c82e8`, no tracked change (the untracked entries
  of prompt 01's gate only). No Ray process (`pgrep -lf 'gcs_server|raylet|ray::'` empty).
  `git diff --cached` empty. Load 3.96 / 5.84 / 11.64 at 02:19.
- `git grep -n RayTools -- ':!prompts' ':!.documents'` gave exactly the three import lines
  (`main.py:50`, `plot_ScalarModel.py:44`, `plot_by_beta.py:38`).

**The re-measure (§2.1.1)**, `measure_client_pools.py cpbh --commit 12c82e8…`, exit 0: **3 imports**
(all module level, the same three lines), **0 string literals, 0 `import_module` calls, 17 calls,
`constructs` 17**, 0 flags. Its difference from the `52142d7` appendix
([`measure/out/52142d7-raypoolkit-measure_client_pools.md`](../measure/out/52142d7-raypoolkit-measure_client_pools.md)),
each line explained:
- the title and commit: `52142d7` → `12c82e8`; and `Files:` 478 → 510 tracked, 203 → 208 `.py`
  parsed: prompt 01 added `tools/compare_stores.py`, two test modules and two rehearsal scripts,
  and the campaign's records since (including the orchestrator's notes) are tracked;
- **every call's line moved**: `main.py` sites by +8 (`:193` → `:201`, … `:820` → `:828`), the plot
  scripts' by +4 (`plot_ScalarModel.py:1556` → `:1560`; `plot_by_beta.py:715` → `:719` … `:959` →
  `:963`): 01's provenance import and print. The compute sites are now `main.py:284`, `:541`,
  `:828`; the "handlers" and "notify" tables move with them. Every other column is unchanged;
- non-`.py` lines naming `RayTools`/`RayWorkPool` 43 → 171: `prompts/` 21 → 148 (this campaign's
  records, orchestrator notes included) and `.documents/` 22 → 23
  (`.documents/dk-and-rpk-migration-verification.md:12`, 02's);
- test modules naming either 0 → 1, `Datastore/tests/test_compare_stores.py:1163`, a comment (see
  the reading of §2.3.5).
No call, verdict or import count differs. Saved: `remeasure-head.md` (scratch).

**By RayPoolKit's script, at a commit of the staged tree (§2.3.1–2).** The staged tree
`d818519f7daf0f84c8a5fb1ca13059af88fe371c`, committed with `commit-tree` on `12c82e8` as the dangling
`2d797104ad90f51af03192a41bc336509d4ced13`: `RayTools/` table empty; **0 imports** (module level 0, nested 0),
**0 string literals, 0 `import_module` / `__import__` calls** naming `RayTools`; **17 calls**
(14 with `compute_handler=None`, 3 with a compute handler given, `store_results=True` 13, 17 with a
`title` keyword: 12 `None`, 5 set); **`constructs` 17**; 0 flagged; `ray.is_initialized()` after the
verdicts `False`; `Files:` 508 tracked, 208 `.py`. The call lines are those of `12c82e8`
(`main.py:201` … `:828`, `plot_ScalarModel.py:1560`, `plot_by_beta.py:719` … `:963`).
Its differences from the re-measure: the 3 imports become 0 (and the imports table is "None."),
`Files:` falls 510 → 508 (the two deleted files), and the non-`.py` lines naming either word fall
171 → 170: `architecture-summary.md`'s layout entry had two such lines (old `:35`, `:36`) and has one
(`:35`), the `:152` title and `:585` lines still name `RayWorkPool`, and the `.documents/` list
outside the campaign records goes from 23 to 22 lines. `git ls-tree <that commit> RayTools` is empty.

The handler-keyword table at that commit: `None / None / — / None / None / None / None` 1 call
(`plot_ScalarModel.py`), `None / None / — / None / None / — / None` 13 calls, `set / — / — / — / set /
— / set` 3 calls.

**The imports resolve (§2.3.3).** `check_imports.py`, kept here, run from the repository root with
`./venv/bin/python`:

```python
"""Check that every import in the three drivers resolves, without executing a driver.

Run from the repository root.  Exit status 1 if any import fails.
"""
import ast
import importlib
import importlib.util
import sys

sys.path.insert(0, ".")
DRIVERS = ["main.py", "plot_by_beta.py", "plot_ScalarModel.py"]
failures = 0
total = 0


def find(name):
    try:
        return importlib.util.find_spec(name)
    except (ImportError, ValueError, AttributeError) as e:
        return None


for driver in DRIVERS:
    tree = ast.parse(open(driver).read(), driver)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                total += 1
                ok = find(a.name) is not None
                print(f"{driver}:{node.lineno}: import {a.name}: {'ok' if ok else 'FAIL'}")
                failures += not ok
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                print(f"{driver}:{node.lineno}: relative import: FAIL")
                failures += 1
                continue
            mod = node.module
            for a in node.names:
                total += 1
                if find(mod) is None:
                    print(f"{driver}:{node.lineno}: from {mod} import {a.name}: FAIL (module not found)")
                    failures += 1
                    continue
                try:
                    m = importlib.import_module(mod)
                except Exception as e:
                    print(f"{driver}:{node.lineno}: from {mod} import {a.name}: FAIL ({type(e).__name__}: {e})")
                    failures += 1
                    continue
                if a.name == "*" or hasattr(m, a.name) or find(f"{mod}.{a.name}") is not None:
                    print(f"{driver}:{node.lineno}: from {mod} import {a.name}: ok")
                else:
                    print(f"{driver}:{node.lineno}: from {mod} import {a.name}: FAIL (no such name)")
                    failures += 1

import ray

print(f"checked {total} imported names; failures {failures}; ray.is_initialized() = {ray.is_initialized()}")
sys.exit(1 if failures else 0)
```

Output (162 imported names; the three `raypoolkit` lines, then the last line; every other line is
`ok`; no line contains `FAIL`):

```text
main.py:50: from raypoolkit import RayWorkPool: ok
plot_by_beta.py:38: from raypoolkit import RayWorkPool: ok
plot_ScalarModel.py:44: from raypoolkit import RayWorkPool: ok
checked 162 imported names; failures 0; ray.is_initialized() = False
```

`./venv/bin/python -m py_compile main.py plot_by_beta.py plot_ScalarModel.py`: exit 0.

**The suites (§2.3.5)**, from the repository root, in the foreground and serially, after the
change. Baselines are 03's (18 / 114 / 66).

| Root | Baseline | Result | Load at the check |
|---|---|---|---|
| `CosmologyModels/tests` | 18 | `Ran 18 tests`, `OK` (68 s) | 11.87 / 9.31 / 12.19 (02:21) |
| `Datastore/tests` | 66 | `Ran 66 tests`, `OK` (3 s) | 5.90 / 7.97 / 11.42 (02:22) |
| `ComputeTargets/tests`, `test_[a-f]*.py` | 61 | `Ran 61 tests`, `OK` (98 s) | 4.27 / 6.81 / 10.56 (02:24) |
| `ComputeTargets/tests`, `test_[g-z]*.py` | 53 | `Ran 53 tests`, `OK` (89 s) | 4.84 / 6.46 / 10.05 (02:26) |

61 + 53 = 114. No count moved. No Ray process afterwards.

**`black --check main.py plot_by_beta.py plot_ScalarModel.py`:** `3 files would be left unchanged.`

## The deliberate-breakage record

Each was applied to the working tree (on top of the staged change), shown to be caught, and
reverted with `git apply -R`. Each diff `git apply --check`s on the clean tree at `303324b` and
`git apply -R --check`s on the mutated tree. Nothing was committed; the drivers' working tree
equals the index after each.

**Breakage 1 — the old import in `main.py`.**

```diff
diff --git a/main.py b/main.py
index 743c28d..44075e6 100644
--- a/main.py
+++ b/main.py
@@ -47,7 +47,7 @@ from Datastore.SQL.ObjectFactories import tolerance
 from Datastore.SQL.ProfileAgent import ProfileAgent
 from Datastore.SQL.ShardedPool import ShardedPool
 from Quadrature.integration_metadata import IntegrationSolver
-from raypoolkit import RayWorkPool
+from RayTools.RayWorkPool import RayWorkPool
 from Units import Planck_units
 from Units.base import UnitsLike
 from config.argument_parser import create_argument_parser, allowed_drop_actions
```

Caught by `check_imports.py`: `main.py:50: from RayTools.RayWorkPool import RayWorkPool: FAIL
(module not found)`, `failures 1`, exit 1. The script at a throwaway commit of the mutated tree
(index copy via `GIT_INDEX_FILE`, `bcd1e4a…`) reports **1 import** of a `RayTools` module, at
`main.py:50`, `constructs` 17.

**Breakage 2 — a misspelt name in `plot_by_beta.py`.**

```diff
diff --git a/plot_by_beta.py b/plot_by_beta.py
index c15d19a..f5793fe 100644
--- a/plot_by_beta.py
+++ b/plot_by_beta.py
@@ -35,7 +35,7 @@ from CosmologyModels import BaseCosmology
 from Datastore.SQL.ProfileAgent import ProfileAgent
 from Datastore.SQL.ShardedPool import ShardedPool
 from MetadataConcepts import tolerance, store_tag
-from raypoolkit import RayWorkPool
+from raypoolkit import RayWorkPoool
 from Units import Planck_units
 from config.argument_parser import create_argument_parser
 from config.model_list import build_model_list
```

Caught by `check_imports.py`: `plot_by_beta.py:38: from raypoolkit import RayWorkPoool: FAIL (no
such name)`, `failures 1`, exit 1.

**Breakage 3 — a callable `store_handler` at a lookup-only site** (U10's old meaning).

```diff
diff --git a/main.py b/main.py
index 743c28d..5126ec8 100644
--- a/main.py
+++ b/main.py
@@ -206,7 +206,7 @@ def run_pipeline(
             ),
             available_handler=None,
             compute_handler=None,
-            store_handler=None,
+            store_handler=lambda obj, pool: None,
             validation_handler=None,
             label_builder=None,
             title=None,
```

Not caught by the import check (the file still parses and imports); that is not its job. The
script at a throwaway commit of the mutated tree (`9e37d75…`): the verdict of `main.py:201` is
**`ValueError: No compute handler was supplied, so store_handler would never be called; leave it
out, or pass None`**, and the totals read `ValueError` 1 and `constructs` 16. This is the
package's rule in "Which handlers a pool needs", and agrees with the orchestrator's measurement.
It measures the script, not ChamPBH.

**Breakage 4 — `raypoolkit` unavailable** (README §2.2; the venv is not touched). Added by the
orchestrator's brief; recorded as the exact command and its output, not a diff:

```text
$ ./venv/bin/python -c 'import sys, runpy; sys.modules["raypoolkit"] = None; runpy.run_path("<scratch>/check_imports.py", run_name="__main__")'
main.py:50: from raypoolkit import RayWorkPool: FAIL (module not found)
plot_by_beta.py:38: from raypoolkit import RayWorkPool: FAIL (module not found)
plot_ScalarModel.py:44: from raypoolkit import RayWorkPool: FAIL (module not found)
checked 162 imported names; failures 3; ray.is_initialized() = False
(exit 1)
```

(All other lines were `ok` and are omitted.) The script fails on all three drivers' `raypoolkit`
import.

## Observations not acted on

1. `Datastore/tests/test_compare_stores.py:1163` names `RayWorkPool` in a comment (see the reading of §2.3.5). Not edited (README §5 rule 7). No issue: it is accurate and harmless.
2. The script's non-`.py` count (the lines naming `RayTools`/`RayWorkPool`) still counts
   `prompts/` and `.documents/` lines; they are records and stay (P14).
3. `.documents/architecture-summary.md` still describes other pool behaviour at the lines checklist
   item 9 lists (`:155`, `:244`, `:355`, …). They stay true; 11 rewrites the database sections.

## State handed to the next prompt

- `RayTools/` is gone. ChamPBH's three drivers import `from raypoolkit import RayWorkPool` (the
  installed `raypoolkit 0.2.0`, `eaef0ce6…`); no other file imports the pool.
- No pool site changed: 14 lookup-only sites keep `store_handler=None`; the 3 compute sites
  (`main.py:284`, `:541`, `:828`) use the defaults, `obj.store()` then `pool.object_store(obj)`.
- `git grep -n RayTools -- ':!prompts' ':!.documents'` is empty. The call lines are as at `12c82e8`
  (the swap changes no line count: one line is replaced by one).
- `Datastore.SQL.ShardedPool` is no longer imported by the pool, so DatastoreKit's rewrite list
  (prompt 08) loses one line and needs no U12 exception.
- Suites: 18 / 114 / 66, unchanged. The venv is as 03 left it (04 installed nothing).
- The `raypoolkit` import is still unrun under real Ray: that is R1 (prompt 06).
- **Rollback is `git revert` of `303324b`** and nothing in the venv (README §4.4): an installed
  package that nothing imports is inert.
- **The RayPoolKit G3 record**, for prompt 11 to carry into its draft for RayPoolKit's board (P16):

> **RayPoolKit G3 holds for ChamPBH** (checklist item 8, items 1–5), at ChamPBH commit `303324b`
> (branch `dk-and-rpk-migration`), 2026-10-12, by prompt 04:
> 1. `RayTools/` is gone: `git ls-tree <commit> RayTools` is empty, and no `RayTools` directory is
>    left in the working tree.
> 2. No import of it: `measure_client_pools.py cpbh --commit <commit>` reports 0 imports, 0 string
>    literals and 0 `import_module` / `__import__` calls naming `RayTools`.
> 3. `requirements.txt:79` holds `raypoolkit @ git+https://github.com/ds283/RayPoolKit@v0.2.0`, and
>    the venv is installed from it (prompt 03, `2f61943`): `raypoolkit 0.2.0` at `eaef0ce6…`, not
>    editable.
> 4. The suites pass: `CosmologyModels/tests` 18, `ComputeTargets/tests` 114, `Datastore/tests` 66.
> 5. Every site constructs: the script gives 17 calls, every one `constructs`; no call changed
>    (the drivers' diff is the one import line each).
>
> Item 6, the record on RayPoolKit's own extraction board, is a commit in that repository, drafted
> by ChamPBH prompt 11 (P16). The run under real Ray is ChamPBH's R1 (prompt 06). Beyond item 8:
> no call site was changed, the 14 lookup-only sites keep `store_handler=None`, and ChamPBH's
> `architecture-summary.md` §3.2 now states the `v0.2.0` constructor, with `persist_handler`.
