# Log 07 — pin `datastorekit` `v0.3.0`

**Subject:** Pin datastorekit v0.3.0 and install it into the venv
**Commit:** `617a1dc`, on branch `dk-and-rpk-migration`, on top of `4f3f44d`
**Date:** 2026-10-12, by Claude Sonnet 5.5
**Result:** **COMPLETE.**
- K4: one line added to `requirements.txt`, in sorted position (`:17`).
- K5: the venv installed from it. Pip added one distribution, `datastorekit 0.3.0`, and moved none
  (`raypoolkit` was re-resolved, not reinstalled).
- K6: the install is the tag's commit `2f5417ea…`, not editable, with the tag's 20 non-test
  modules byte for byte and no `tests/`.
- K7: U3 and U6 hold on the installed package: all 27 of ChamPBH's registry entries, rebuilt on the
  installed base, instantiate; `timestamp` defaults to `None` and is read-only.
- The three roots are at 18 / 114 / 73, unchanged. No §4 stop condition was met. No §3 issue opened.

## What shipped

- `requirements.txt`: one added line (below).
- This log, and the board (§1 row, K4–K7, §5.12).

No code and no test changed, so `black` had nothing to do. Nothing was changed in DatastoreKit or
RayPoolKit (DatastoreKit read only through `git ls-remote`, `rev-parse` and `archive`; no command
was run from its root). No Ray was started and no store was opened.

```diff
@@ -14,6 +14,7 @@ ConfigArgParse==1.7.1
 contourpy==1.3.3
 cryptography==46.0.4
 cycler==0.12.1
+datastorekit @ git+https://github.com/ds283/DatastoreKit@v0.3.0
 diffrax==0.7.0
 distlib==0.4.0
 equinox==0.13.2
```

## Deviations from the prompt

- **D1 — IMPLEMENTATION CHOICE.** ComputeTargets was run in its two pattern groups (61 + 53) only,
  as the dispatch allowed, not also whole (03 added a whole run; here the two groups sum to the
  baseline's 114). Nothing differed.
- **D2 — IMPLEMENTATION CHOICE.** The "before" `pip freeze` was compared with the baseline by
  sorting both sides (`sort freeze-before | diff - sort(baseline + the raypoolkit line)`), which
  gave no difference. The committed baseline is
  `prompts/dk-and-rpk-migration/measure/out/52142d7-pip-freeze.txt` (99 lines).
- **D3 — IMPLEMENTATION CHOICE.** K7's registry check treats an entry as a quantity factory when its
  class name contains `quantity_factory`, and passes `inst.ObjectType` to the rebuilt class. This
  is how the prompt describes the two quantity classes.

No other deviation. Where my measurements differ from the orchestrator's facts: none. The dry run,
the figures in K7 (27 entries, 23 classes, 3 / 20 / 0 hooks), the 20 / 54 file counts and the
RayPoolKit mtimes agree with the facts file.

## Verification performed

Each command was run from the repository root. Saved outputs are in the agent's scratch directory.

**Before (§2.1).**
- `./venv/bin/pip freeze`: 100 lines; equals the baseline plus exactly
  `raypoolkit @ git+https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976`.
- `pip check`: `No broken requirements found.`
- `pip show datastorekit`: `WARNING: Package(s) not found: datastorekit` (exit 1).
- No Ray process (`pgrep -lf 'gcs_server|raylet|ray::'` empty). `git status` shows no tracked
  change (before the edit). Load at the first check: 5.08 / 16.17 / 21.38.

**The tag (§2.2).** `git ls-remote --tags https://github.com/ds283/DatastoreKit v0.3.0 'v0.3.0^{}'`:

```text
bf0a3b25afa5b4379f9cdc01bca93805ba2a695b	refs/tags/v0.3.0
2f5417ea76cdcd0d15d64d80d9085429bfacad21	refs/tags/v0.3.0^{}
```

`git -C /Users/ds283/Documents/Code/DatastoreKit rev-parse 'v0.3.0^{commit}'` gives
`2f5417ea76cdcd0d15d64d80d9085429bfacad21`, in agreement.

**K4.** `requirements.txt` is now 101 lines; the diff is the one line above, between
`cycler==0.12.1` (`:16`) and `diffrax==0.7.0` (now `:18`). `ray==2.53.0` and `SQLAlchemy==2.0.46`
are unchanged.

**The dry run** (`./venv/bin/pip install --dry-run -r requirements.txt`, exit 0), without the 99
"Requirement already satisfied" lines (temporary directories abbreviated):

```text
Collecting datastorekit @ git+https://github.com/ds283/DatastoreKit@v0.3.0 (from -r requirements.txt (line 17))
  Cloning https://github.com/ds283/DatastoreKit (to revision v0.3.0) to <tmp>
  Running command git clone --filter=blob:none --quiet https://github.com/ds283/DatastoreKit <tmp>
  Running command git checkout -q bf0a3b25afa5b4379f9cdc01bca93805ba2a695b
  Resolved https://github.com/ds283/DatastoreKit to commit bf0a3b25afa5b4379f9cdc01bca93805ba2a695b
  Installing build dependencies: started
  Installing build dependencies: finished with status 'done'
  Getting requirements to build wheel: started
  Getting requirements to build wheel: finished with status 'done'
  Preparing metadata (pyproject.toml): started
  Preparing metadata (pyproject.toml): finished with status 'done'
Collecting raypoolkit @ git+https://github.com/ds283/RayPoolKit@v0.2.0 (from -r requirements.txt (line 80))
  Cloning https://github.com/ds283/RayPoolKit (to revision v0.2.0) to <tmp>
  Running command git clone --filter=blob:none --quiet https://github.com/ds283/RayPoolKit <tmp>
  Running command git checkout -q 4e7eb153ca9837c6aa6ec51d702b5fa6ccf08fef
  Resolved https://github.com/ds283/RayPoolKit to commit 4e7eb153ca9837c6aa6ec51d702b5fa6ccf08fef
  Installing build dependencies: started
  Installing build dependencies: finished with status 'done'
  Getting requirements to build wheel: started
  Getting requirements to build wheel: finished with status 'done'
  Preparing metadata (pyproject.toml): started
  Preparing metadata (pyproject.toml): finished with status 'done'
Would install datastorekit-0.3.0
```

Only `datastorekit` is reported. Pip re-resolved `raypoolkit` (a VCS requirement already in the
file) and left it. `Resolved … to commit bf0a3b25…` is the **tag object**, not the commit; the
installed commit is judged on `pip freeze` and `direct_url.json` below. Likewise `4e7eb153…` is
RayPoolKit's tag object.

**K5, the install** (`./venv/bin/pip install -r requirements.txt`, exit 0), without the 99
"already satisfied" lines:

```text
Collecting datastorekit @ git+https://github.com/ds283/DatastoreKit@v0.3.0 (from -r requirements.txt (line 17))
  (clone, checkout of bf0a3b25…, build dependencies and metadata, as in the dry run)
Collecting raypoolkit @ git+https://github.com/ds283/RayPoolKit@v0.2.0 (from -r requirements.txt (line 80))
  (clone, checkout of 4e7eb153…, build dependencies and metadata, as in the dry run)
Building wheels for collected packages: datastorekit
  Building wheel for datastorekit (pyproject.toml): started
  Building wheel for datastorekit (pyproject.toml): finished with status 'done'
  Created wheel for datastorekit: filename=datastorekit-0.3.0-py3-none-any.whl size=99488 sha256=9d469f35144ec4539d15be05a93ce2678f7978f4514cd3f647b29e6ce8e34b54
  Stored in directory: <tmp>/pip-ephem-wheel-cache-…/wheels/bc/05/cd/3be6521f75315c983a5e9308cb3165f6dfc4c93de2fc85c3d2
Successfully built datastorekit
Installing collected packages: datastorekit
Successfully installed datastorekit-0.3.0
```

No `raypoolkit` appears in a `Would install` or `Successfully installed` line. Its dist-info and
four modules still have mtime `1791766684` (03's install).

**K6.1 — `pip freeze`** (101 lines) against the saved one (100): exactly one added line.

```text
16a17
> datastorekit @ git+https://github.com/ds283/DatastoreKit@2f5417ea76cdcd0d15d64d80d9085429bfacad21
```

**K6.2 — `pip check`:** `No broken requirements found.`

**K6.3 — the provenance helper.** `utilities.installed_package_provenance()`:
- `["datastorekit"]` = `{'version': '0.3.0', 'url': 'https://github.com/ds283/DatastoreKit',
  'commit': '2f5417ea76cdcd0d15d64d80d9085429bfacad21', 'requested_revision': 'v0.3.0',
  'editable': False}`
- `["raypoolkit"]` = `{'version': '0.2.0', 'url': 'https://github.com/ds283/RayPoolKit', 'commit':
  'eaef0ce60e2fc354bb32f0ad576010e6a83c1976', 'requested_revision': 'v0.2.0', 'editable': False}`,
  unchanged.
- `format_package_provenance` prints (no `git+` before the URL, as at 03):

  ```text
  >> packages: datastorekit 0.3.0 (https://github.com/ds283/DatastoreKit@2f5417ea76cdcd0d15d64d80d9085429bfacad21, requested v0.3.0); raypoolkit 0.2.0 (https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976, requested v0.2.0)
  ```

The installed `datastorekit-0.3.0.dist-info/direct_url.json`, verbatim:

```json
{"url": "https://github.com/ds283/DatastoreKit", "vcs_info": {"commit_id": "2f5417ea76cdcd0d15d64d80d9085429bfacad21", "requested_revision": "v0.3.0", "vcs": "git"}}
```

A VCS record, with no `dir_info`: not editable, not a local path.

**K6.4 — the installed files are the tag's.** `git -C /Users/ds283/Documents/Code/DatastoreKit
archive v0.3.0 datastorekit`, extracted into a new scratch directory, against
`venv/lib/python3.13/site-packages/datastorekit/` (from `find_spec`).

| | files |
|---|---|
| archive, `.py` outside `datastorekit/tests/` | 20 (root 9, `SQL/` 8, `tools/` 3) |
| archive, under `datastorekit/tests/` | 54 files of all kinds (51 `.py`) |
| installed `.py`, anywhere (ignoring `__pycache__`) | 20: the same paths |

- `cmp` of each of the 20: identical, byte for byte.
- `datastorekit/tests/` does not exist in the venv.
- The installed set of `.py` paths equals the archive's non-test set (`diff` empty), and the
  installed package holds no non-`.py` file outside `__pycache__`.

Result: **the installed files are the tag's**.

**K6.5 — the import.**
`./venv/bin/python -c "import ray, datastorekit, datastorekit.SQL.factory_base, datastorekit.object, datastorekit.contract, datastorekit.store_inventory; print(ray.is_initialized())"`
prints `False`.

`./venv/bin/python -m datastorekit.tools.sharded_store --help` exits 0, printing
`usage: python -m datastorekit.tools.sharded_store [-h] {copy,move} src dst`, the description
"Copy or move a closed ShardedPool datastore under a new name.", its three limits and the
`copy|move`, `src`, `dst` arguments.

## K7 — U3 and U6 on the installed package

The script (`<scratch>/k7.py`, never committed), run as
`PYTHONPATH=. ./venv/bin/python <scratch>/k7.py` from the repository root. Neither the extracted
archive nor DatastoreKit's checkout is on `sys.path`; the first line of output is the proof.

```python
# K7 (prompt 07): U3 and U6 on the installed datastorekit. Throwaway; never committed.
import datastorekit

print("datastorekit.__file__ =", datastorekit.__file__)

from datastorekit.SQL.factory_base import SQLAFactoryBase
from datastorekit.object import DatastoreObject

# --- U3, in the abstract ---------------------------------------------------
class Two(SQLAFactoryBase):
    @staticmethod
    def register(*a, **k):
        pass

    @staticmethod
    def build(*a, **k):
        pass

Two()
print("U3 abstract: register+build subclass instantiates: OK")


class One(SQLAFactoryBase):
    @staticmethod
    def register(*a, **k):
        pass

try:
    One()
    print("U3 abstract: register-only subclass instantiated: FAIL")
except TypeError as e:
    print("U3 abstract: register-only subclass raises TypeError:", e)

# --- U3, on ChamPBH's own registry (layer first, then config) --------------
import Datastore.SQL.Datastore  # noqa: F401
import config.datastore as cd

HOOKS = ("store", "validate", "validate_on_startup")
rows = []
n_ok = 0
for table, inst in cd.factories.items():
    cls = type(inst)
    ns = {k: v for k, v in vars(cls).items() if k not in ("__dict__", "__weakref__")}
    new = type(cls.__name__, (SQLAFactoryBase,), ns)
    if "quantity_factory" in cls.__name__:
        obj = new(inst.ObjectType)
        arg = "ObjectType=" + inst.ObjectType.__name__
    else:
        obj = new()
        arg = "no argument"
    n_ok += 1
    rows.append((table, cls.__name__, arg, tuple(h for h in HOOKS if h in vars(cls))))
    print(f"  {table:32s} {cls.__name__:42s} {arg:34s} instantiated")
print(f"U3 registry: {n_ok} of {len(cd.factories)} entries instantiate")

by_class = {}
for _, cname, _, hooks in rows:
    by_class.setdefault(cname, hooks)
print("distinct classes:", len(by_class))
for cname, hooks in sorted(by_class.items()):
    if hooks:
        print("  defines", hooks, "->", cname)
print(
    "classes defining all three:", sum(1 for h in by_class.values() if h == HOOKS),
    "| none:", sum(1 for h in by_class.values() if not h),
    "| some-but-not-all:", sum(1 for h in by_class.values() if h and h != HOOKS),
)

# --- U6 ---------------------------------------------------------------------
class D(DatastoreObject):
    def __init__(self, store_id):
        DatastoreObject.__init__(self, store_id)

d = D(5)
print("U6: timestamp =", repr(d.timestamp))
try:
    d.timestamp = 1.0
    print("U6: assignment succeeded: FAIL")
except AttributeError as e:
    print("U6: assigning timestamp raises AttributeError:", e)
```

Its output, whole:

```text
datastorekit.__file__ = /Users/ds283/Documents/Code/ChamPBH/venv/lib/python3.13/site-packages/datastorekit/__init__.py
U3 abstract: register+build subclass instantiates: OK
U3 abstract: register-only subclass raises TypeError: Can't instantiate abstract class One without an implementation for abstract method 'build'
  version                          sqla_version_factory                       no argument                        instantiated
  store_tag                        sqla_store_tag_factory                     no argument                        instantiated
  redshift                         sqla_redshift_factory                      no argument                        instantiated
  tolerance                        sqla_tolerance_factory                     no argument                        instantiated
  beta_value                       sqla_dimensionless_quantity_factory        ObjectType=beta_value              instantiated
  M_value                          sqla_dimensionful_quantity_factory         ObjectType=M_value                 instantiated
  Lambda_value                     sqla_dimensionful_quantity_factory         ObjectType=Lambda_value            instantiated
  temperature                      sqla_dimensionful_quantity_factory         ObjectType=temperature             instantiated
  phi_value                        sqla_dimensionful_quantity_factory         ObjectType=phi_value               instantiated
  pi_value                         sqla_dimensionful_quantity_factory         ObjectType=pi_value                instantiated
  InversePowerPotential            sqla_InversePowerPotential_factory         no argument                        instantiated
  StarobinskyPotential             sqla_StarobinskyPotential_factory          no argument                        instantiated
  ExponentialPotential             sqla_ExponentialPotential_factory          no argument                        instantiated
  ReclinerPotential                sqla_ReclinerPotential_factory             no argument                        instantiated
  ReflectingPotential              sqla_ReflectingPotential_factory           no argument                        instantiated
  ExponentialCoupling              sqla_ExponentialCoupling_factory           no argument                        instantiated
  QCD_Cosmology                    sqla_QCDCosmology_factory                  no argument                        instantiated
  IntegrationSolver                sqla_IntegrationSolver_factory             no argument                        instantiated
  ScalarModel                      sqla_ScalarModelFactory                    no argument                        instantiated
  ScalarModel_tags                 sqla_ScalarModelTagAssociation_factory     no argument                        instantiated
  ScalarModelValue                 sqla_ScalarModelValue_factory              no argument                        instantiated
  AdiabaticHistory                 sqla_AdiabaticHistoryFactory               no argument                        instantiated
  AdiabaticHistory_tags            sqla_AdiabaticHistoryTagAssociation_factory no argument                        instantiated
  AdiabaticHistoryValue            sqla_AdiabaticHistoryValue_factory         no argument                        instantiated
  BBNData                          sqla_BBNDataFactory                        no argument                        instantiated
  BBNData_tags                     sqla_BBNDataTagAssociation_factory         no argument                        instantiated
  BBNDataValue                     sqla_BBNDataValue_factory                  no argument                        instantiated
U3 registry: 27 of 27 entries instantiate
distinct classes: 23
  defines ('store', 'validate', 'validate_on_startup') -> sqla_AdiabaticHistoryFactory
  defines ('store', 'validate', 'validate_on_startup') -> sqla_BBNDataFactory
  defines ('store', 'validate', 'validate_on_startup') -> sqla_ScalarModelFactory
classes defining all three: 3 | none: 20 | some-but-not-all: 0
U6: timestamp = None
U6: assigning timestamp raises AttributeError: property 'timestamp' of 'D' object has no setter
```

Reading: `datastorekit.__file__` is under `venv/lib/python3.13/site-packages/`. A subclass defining
`register` and `build` instantiates; one defining `register` alone raises `TypeError` naming
`build`. All 27 entries (23 classes; the six quantity entries are on two classes) instantiate once
rebuilt on the installed base from their own class namespaces; 3 classes define all of `store`,
`validate`, `validate_on_startup`, 20 define none, none some. `timestamp` is `None` and
assigning it raises `AttributeError`.

## The suites (§2.6)

After the install, foreground, serially, from the repository root.

| Root | 06's count | Result | Load at start |
|---|---|---|---|
| `Datastore/tests` | 73 | `Ran 73 tests`, `OK` | 4.43 / 13.96 / 20.20 |
| `CosmologyModels/tests` | 18 | `Ran 18 tests`, `OK` (87 s) | 21.74 / 19.74 / 21.93 |
| `ComputeTargets/tests`, `test_[a-f]*.py` | 61 | `Ran 61 tests`, `OK` (117 s) | 14.47 / 17.61 / 20.87 |
| `ComputeTargets/tests`, `test_[g-z]*.py` | 53 | `Ran 53 tests`, `OK` (104 s) | 8.03 / 14.51 / 19.23 |

18 / 114 / 73. No count moved. Load at the end: 8.92 / 12.88 / 18.04.

## The deliberate-breakage record

None. 07 adds no code and no test (prompt §3.5). §2.4's and §2.5's checks are its proof, and each
is one a wrong install or a wrong release would fail: a version-only install lacks the VCS record,
an editable install carries `dir_info` and a different freeze line, a local-path install shows a
file URL, a stale or partial install fails the byte comparison, a K7 run against the checkout
(ahead of the tag) or the archive would show a `__file__` outside site-packages, and a base whose
`store`, `validate` or `validate_on_startup` were still abstract would make the 20 partial classes
fail to instantiate.

## Observations not acted on

1. The provenance line has no `git+` before the URL (as at 03); `Resolved … to commit` in pip's
   output names the tag object (`bf0a3b25…`), not the commit (`2f5417ea…`). Neither is changed.
2. `config.datastore` cannot be imported before `Datastore.SQL.Datastore` (the board's
   `[05-config-datastore-cannot-be-imported-before-the-layer]`, which 08 closes). K7 imports in the
   required order; nothing was changed.
3. The archive's 54 files under `datastorekit/tests/` include 3 that are not `.py`. None is
   installed, as required.

## State handed to the next prompt

- The venv holds `datastorekit 0.3.0` installed from the GitHub tag `v0.3.0` (commit
  `2f5417ea76cdcd0d15d64d80d9085429bfacad21`, `direct_url.json` a VCS record, not editable) and
  `raypoolkit 0.2.0` (unchanged, `eaef0ce6…`). Its `pip freeze` is
  `measure/out/52142d7-pip-freeze.txt` plus the `raypoolkit` line (03's) and
  `datastorekit @ git+https://github.com/ds283/DatastoreKit@2f5417ea76cdcd0d15d64d80d9085429bfacad21`
  (101 lines); `pip check` is clean.
- `requirements.txt` carries the pin on line 17 (101 lines). `ray==2.53.0` is now `:79`,
  `raypoolkit` `:80` and `SQLAlchemy==2.0.46` `:91`.
- Nothing in ChamPBH imports `datastorekit` yet. `Datastore/SQL/` and its layer are intact, so
  prompt 08's swap starts from 06's tree plus this pin.
- The provenance line each driver now prints is
  `>> packages: datastorekit 0.3.0 (https://github.com/ds283/DatastoreKit@2f5417ea76cdcd0d15d64d80d9085429bfacad21, requested v0.3.0); raypoolkit 0.2.0 (https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976, requested v0.2.0)`.
- On the installed base, K7 shows: ChamPBH's 27 registry entries, rebuilt from their classes'
  namespaces, instantiate (3 classes define `store`, `validate` and `validate_on_startup`; 20 define
  none); `DatastoreObject.timestamp` is `None` for `DatastoreObject.__init__(self, store_id)`, and
  read-only.
- Suites: 18 / 114 (61 + 53) / 73, unchanged.
- **Rollback is `git revert` of `617a1dc` plus `./venv/bin/pip uninstall -y datastorekit`**: the
  venv is outside git (README §4.4).
