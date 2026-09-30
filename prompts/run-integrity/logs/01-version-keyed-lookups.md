# Log 01 — Key the compute-target lookups on the version

**Prompt:** prompts/run-integrity/01-version-keyed-lookups.md
**Commit:** the commit that adds this file ("Key the compute-target lookups on the version label"); its SHA is in `git log`
**Model:** Opus 5.5
**Date:** 2026-09-30
**Result:** COMPLETE

Every README §6.1 row is met (Verification). No stop condition of prompt §5 was met: the serial
reaches `build()` through `Datastore.object_get` alone, with no change to any `build()` signature
and no edit to `base.py`. The deviations below are all IMPLEMENTATION CHOICEs.

## What shipped

`VERSION_LABEL` is `"2026.3.0"` before and after. `PRYM_VERSION` is untouched
(`"bf24c3d+cham03"`). No schema change.

**V1 — one label.**

- **New `config/version.py`.**
  - `:19–27`: `main.py`'s dated comment block, moved verbatim.
  - `:28–30`: one new dated sentence. From run-integrity prompt 01, lookups of the three compute
    targets return only rows made under this label, and the label is defined here once.
  - `:31`: `VERSION_LABEL = "2026.3.0"`.
  - `:36`: `VERSION_SERIAL_KEY = "_version_serial"`, the reserved payload key (V3).
  - `:39–52`: `require_version_serial(payload, cls_name: str) -> int` (V4).
- **The three scripts.** Each now has `from config.version import VERSION_LABEL`, and none
  assigns it:
  - `main.py`: the import is at `:65`. `:80–89` (the comment block and the assignment) are
    deleted.
  - `plot_by_beta.py`: the import is at `:49`; the old `:79` is deleted.
  - `plot_ScalarModel.py`: the import is at `:57`; the old `:78` (`"2026.1.1"`) is deleted.
    **This script moves from `"2026.1.1"` to the pipeline's `"2026.3.0"`**, as README §2 (a)
    intends.
- The profile labels the scripts build (`main.py:88, 90`, `plot_by_beta.py:87`,
  `plot_ScalarModel.py:86`) are unchanged in format.

**V2 — the flag.**

- **The three factories.** `register()` of each declares `"key_on_version": True`, with a comment
  line: `ScalarModel.py:109`, `AdiabaticHistory.py:99`, `BBNData.py:86`. Nothing else in any
  `register()` changed.
- **`Datastore._build_schema`** (`Datastore.py:329–338`) reads the flag into
  `schema["key_on_version"]`. It raises `RuntimeError` if the flag is set on a table registered
  without `"version": True`. The table-less branch sets it `False` (`:390`).

**V3 — delivery.**

- **In `Datastore.object_get`** (`:503–509`): for a keyed factory, `payload_data` is replaced by
  a list of copies, `[self._with_version_serial(cls_name, p) for p in payload_data]`. This runs
  before the `build()` loop, so it covers the scalar path (`payload_data = [kwargs]`) and the
  vectorized one (`payload_data=` from `ShardedPool.object_get_vectorized`).
- **New private method `Datastore._with_version_serial(self, cls_name: str, payload: Mapping) -> dict`**
  (`:544–555`). It returns `{**payload, VERSION_SERIAL_KEY: self._version.store_id}`. It raises
  `KeyError` if the caller's payload already carries the reserved key. The caller's dict is never
  mutated.

**V4 — the filter.** Each keyed `build()` calls `require_version_serial(payload, "<class>")`,
which raises `RuntimeError` if the key is absent or `None`. It then adds
`table.c.version == version_serial` to its lookup query:

- `ScalarModel.py:212` and `:254`: inside the existing `.filter(...)`, beside `validated == True`;
- `AdiabaticHistory.py:131` and `:144`;
- `BBNData.py:130` and `:151`.

Nothing else in the queries changed: not the failure handling, not `failure=True`'s newest-first
order and limit, not the tags.

**V5 — tests.**

- New `Datastore/tests/__init__.py` (licence header only).
- New `Datastore/tests/test_version_keyed_lookups.py`, with 11 methods: (a), (b), (c1), (c2),
  (c3), (d), (d2), (d3), (d4), (e) and (f); see Verification.
- The new `CLAUDE.md` line, verbatim, beside the other two test commands:

  ```
    PYTHONPATH=. ./venv/bin/python -m unittest discover -s Datastore/tests -t .
  ```

**V6 — documents.** `.documents/architecture-summary.md` §4.3 has a new dated paragraph, placed
after "Serial ID management": "**Version-keyed lookups (added 2026-09-30, `run-integrity` prompt
01).**". It says which tables are keyed, how the serial reaches `build()`, and that parameter
tables are not keyed. Nothing existing was rewritten.

## Deviations from the prompt

### 1. The names: `key_on_version` and `_version_serial` — IMPLEMENTATION CHOICE

These are the names the README suggests. The flag is `"key_on_version"` in `register()`. The
reserved key is `"_version_serial"`, held as `config.version.VERSION_SERIAL_KEY`. A leading
underscore matches the one existing reserved payload key, `_do_not_populate`. No other names were
considered worth the difference from the README.

### 2. The key and its check live in `config/version.py` — IMPLEMENTATION CHOICE

The key constant and the absent-key check need one home that `Datastore.py` and the three
factories can all import. Four alternatives were considered:

- **`base.py`.** The prompt forbids it.
- **`Datastore.py`.** It imports the factories at module load, so a factory importing it back
  would be circular.
- **One of the three factory modules.** The other two would then import from a sibling
  compute-target factory. That is an odd dependency.
- **A new module under `Datastore/SQL/`.** It is not in the prompt's list of files.

`config/version.py` is a leaf module (it imports nothing), is new in this prompt, and is already
"the version" module. So it holds `VERSION_SERIAL_KEY` and a three-line helper,
`require_version_serial`, which the three `build()`s call. That keeps the raise in one place, not
three copies. The cost is that a datastore detail sits in `config/`; the comment above the
constant says what it is for.

### 3. A caller-supplied reserved key is refused, not overwritten — IMPLEMENTATION CHOICE

The prompt says only that the copy carries the current serial. There were two options for a caller
that already puts `_version_serial` in its payload:

- **Silently overwrite it.** The datastore stays authoritative, but a caller trying to choose a
  version would be ignored without a word.
- **Refuse it** with a `KeyError` naming the key.

The second was chosen: no caller in the tree sets the key (grep), and a caller that tries to pick
another label's rows is a bug that should be loud. Test (d2) covers it.

### 4. `require_version_serial` also raises on an explicit `None` — IMPLEMENTATION CHOICE

A payload with `_version_serial: None` would filter on `version IS NULL`, which matches no row.
It would look like "not found" and send the target for recomputation. The helper treats `None`
like an absent key and raises. `object_get` always supplies `self._version.store_id`, which is an
integer once `__init__` has run.

### 5. Tests beyond (a)–(f) — IMPLEMENTATION CHOICE

Five methods were added beyond the six the prompt lists. Each guards a §2 (a) or §6.1 property
that (a)–(f) do not reach directly:

- **(c3)**: the vectorized route (`payload_data=`) is keyed too.
- **(d2)**: the caller's payload is not mutated, and a caller-supplied key is refused.
- **(d3)**: exactly `{ScalarModel, AdiabaticHistory, BBNData}` carry the flag. This is §6.1's "no
  key on any parameter, value or tag table", checked over the whole registry rather than by
  reading.
- **(d4)**: `_build_schema` refuses the flag on a table without a version column. It uses a bare
  `DS.__new__` instance holding only the attributes `_build_schema` touches, so the production
  factory registry is not altered.
- **(c1) and (c2)** are (c)'s two halves: `failure=True` on a failed row, and the default lookup
  on a success row with `_do_not_populate`.

Each of (a), (b), (c1) and (c2) also reopens the file under A after B, and checks the row is
still returned with the same serial (and, for (c1), the same reason). That is §6.1's "the same
three rows looked up under A: returned, unchanged".

A test that pinned `VERSION_LABEL == "2026.3.0"` was written and then removed before the commit.
Prompt 02 bumps the label, and such a test would fail there in a file that prompt does not own.
The value is witnessed by grep below instead.

### 6. `config.version` is imported inside tests, not at module top — IMPLEMENTATION CHOICE

The test module imports `config.version` only inside (d2). At module level it would make the whole
file an import error on `HEAD~1`. Imported locally, each test fails on `HEAD~1` on its own
assertion, which is what the orchestrator's check needs. The module docstring says so.

## Verification performed

All commands run from the repository root with `venv/bin/python`, on the working tree over
`90b2c86` unless stated. "`HEAD~1`" means `90b2c86`, checked out as a detached worktree in the
session scratchpad. The new test files were copied into it and run from its root.

### Suite counts

| Suite | Before (`90b2c86`, clean worktree) | After |
|---|---|---|
| `CosmologyModels/tests` | 18, OK | 18, OK |
| `ComputeTargets/tests` | 30, OK | 30, OK |
| `Datastore/tests` | 0 (no directory) | **11, OK** (0.9 s) |

The "before" figures come from the clean worktree, not the working copy: an earlier "before" run
in the working copy overlapped my first edits, so it was discarded. That run also gave 18 and 30,
both OK.

### README §6.1, row by row

| Row | Target | Measured | Witness |
|---|---|---|---|
| `ScalarModel` stored under A, looked up under B | not returned | `available=False` | test (a); planning probe line [3] now prints `available=False, store_id=None` (was `store_id=1` on `27a32bc`) |
| same, `AdiabaticHistory` | not returned | `available=False` | test (b) |
| same, `BBNData`, `failure=True` and default | not returned under either | `available=False` under both | tests (c1), (c2); probe line [5] now prints `available=False, reason=None` (was `'first failure'`) |
| the three rows looked up under A | returned, unchanged | returned; same store_id; (c1) same `failure_reason` | tests (a), (b), (c1), (c2), both before and after the store is reopened under B |
| a keyed `build()` given no serial | raises | `RuntimeError` for each of the three | test (d) |
| `ExponentialCoupling`, same β, under A then B | one row, same serial | store_id 21 under both; `count(*) = 1` | test (e); passes on `HEAD~1` too, as intended |
| `VERSION_LABEL` definitions | 1, in `config/version.py`, value `"2026.3.0"` | `config/version.py:31:VERSION_LABEL = "2026.3.0"`, the only line | the grep below; test (f) |
| `main.py`'s dated comment | in `config/version.py`, verbatim | verbatim | checked by script: `main.py`'s old `:80–88` text is a substring of `config/version.py` |
| schema | unchanged | byte-identical `sqlite_master` | the schema dump below |
| `Datastore/tests` count | ≥ 5, in `CLAUDE.md` | 11; the line is in `CLAUDE.md` | suite; `git diff CLAUDE.md` |

**The label grep** (prompt §4 item 2):

```
$ grep -rn "VERSION_LABEL =" --include='*.py' . | grep -v "venv/\|thirdparty/\|claude-context/"
config/version.py:31:VERSION_LABEL = "2026.3.0"
```

**The schema.** A scratch script built a fresh store through `Datastore.__ray_actor_class__` and
printed every `sqlite_master` entry: 27 tables plus their indexes, 81 lines. It was run on
`90b2c86` and on the working tree. The two outputs are byte-identical (`cmp`; both have SHA-1
`1cd8f3eb6aebcf776bbb9a0e92497d83bad49750`). The script, kept here rather than as a file:

```python
import sqlite3, tempfile
from pathlib import Path
from Datastore.SQL.Datastore import Datastore
db = Path(tempfile.mkdtemp()) / "s.db"
s = Datastore.__ray_actor_class__(version_label="2026.3.0", db_name=db)
s._engine.dispose()
con = sqlite3.connect(db)
for name, sql in sorted(con.execute("select name, sql from sqlite_master where sql is not null")):
    print(name, "::", " ".join(sql.split()))
```

The `git diff` of the three `register()`s shows only the flag and its comment line.

**The planning probe on the new tree**
(`PYTHONPATH=. venv/bin/python prompts/run-integrity/planning-probes/datastore_version_probe.py`):

```
[1] ScalarModel under the label that stored it: available=True, store_id=1
[2] version serials: 2026.3.0 -> 1, 2026.4.0 -> 2
[3] ScalarModel under a NEW label: available=False, store_id=None   <- on 27a32bc the old row is returned
[4] BBNData, default lookup (failure=False, what main.py uses): available=False   <- ...
[5] BBNData, failure=True under the NEW label: available=False, reason=None   <- on 27a32bc, the row stored under 2026.3.0
[6] BBNData, failure=None, two failed rows: available=False
```

Line [6] no longer raises `MultipleResultsFound`, but only because the probe stores its rows under
2026.3.0 and looks them up under 2026.4.0. See "State handed to the next prompt".

### The new tests fail on `HEAD~1`

The command, run from the worktree at `90b2c86` with `Datastore/tests/` copied in:
`PYTHONPATH=. venv/bin/python -m unittest discover -s Datastore/tests -t .`. It printed
`Ran 11 tests`, `FAILED (failures=13, errors=1)`, counting subtests. The assertion lines:

- **(a)** `AssertionError: True is not false : ScalarModel stored under 2026.3.0 was returned under 2026.4.0 (store_id=1)`
- **(b)** `AssertionError: True is not false : AdiabaticHistory stored under 2026.3.0 was returned under 2026.4.0 (store_id=1)`
- **(c1)** `AssertionError: True is not false : failed BBNData stored under 2026.3.0 was returned under 2026.4.0 (store_id=1)`
- **(c2)** `AssertionError: True is not false : BBNData stored under 2026.3.0 was returned under 2026.4.0 (store_id=1)`
- **(c3)** `AssertionError: Lists differ: [True, True] != [False, False]`
- **(d)** `AssertionError: RuntimeError not raised`, once each for `ScalarModel`, `AdiabaticHistory`
  and `BBNData`
- **(d2)** `ModuleNotFoundError: No module named 'config.version'`
- **(d3)** `AssertionError: Items in the second set but not the first: ...` (the keyed set is empty)
- **(d4)** `AssertionError: RuntimeError not raised`
- **(f)** `AssertionError: Lists differ: [89] != []` (`main.py`), `[79] != []`
  (`plot_by_beta.py`), `[78] != []` (`plot_ScalarModel.py`); these are the old assignment lines
- **(e)** passed, as it must: it is the regression guard for the unkeyed parameter tables.

### Formatting

`black` was run on the ten changed or new Python files. It reformatted one, the new test file,
and left the other nine unchanged. All seven pre-existing files among them were black-clean on
`90b2c86` (`black --check`). `PRyM/` and `base.py` were not touched.

### What was reasoned, not run

- `ShardedPool.object_get` and `object_get_vectorized` reach the keyed code through
  `Datastore.object_get`. I checked this by reading `ShardedPool.py:458–606`; the tests exercise
  `Datastore.object_get` directly, with no Ray.
- One label has one serial across shards (`ShardedPool.py:151–181`), so every shard filters on
  the same serial. Unchanged here, and not run.
- No other route reaches a compute-target `build()`. `grep -rn "\.build("` finds only
  `Datastore.object_get`. `object_read_batch` calls `factory.read_batch`, which none of the
  three factories defines, and nothing in the tree calls it.
- **Needs a run the user must do:** a pipeline run against an existing store under the same label
  should find its rows as before. One under a new label should recompute everything beside the old
  rows.

## Observations not acted on

1. **`AdiabaticHistory.build` and `BBNData.build` do not require `validated == True`.**
   `ScalarModel.build` does (`ScalarModel.py:253`). The other two filter only on `model_serial`,
   now with `version`. Rows are stored with `validated=False` and validated afterwards.
   - **Without `--prune-unvalidated`**, a row left unvalidated by an interrupted run is served as
     found. For `AdiabaticHistory` with a partial value set, `build()` then raises "Fewer z-samples
     than expected". For `BBNData` looked up with `_do_not_populate`, the sample count is never
     checked, so the row counts as done.
   - Reasoned from the code; not run. Out of scope: "nothing else in the query changes".
   - Opened as §3 `[01-adiabatic-and-bbn-lookups-do-not-require-validated-rows]`.
2. **`plot_by_beta.py:87` builds its profile label as `...--plot_ScalarModel-...`**, a copy of
   `plot_ScalarModel.py`'s. The prompt says these labels keep their format, so it was not changed.
   Cosmetic: it affects only the name of a profiling run. Opened as §3
   `[01-plot-by-beta-profile-label-names-plot-scalarmodel]`.
3. **Dead branch.** `Datastore._build_schema` calls `registration_data.get(...)` (`:295`) before
   its `if registration_data is not None` test, so the `else` branch is dead. No factory returns
   `None`. Not actionable; no issue opened.

## State handed to the next prompt

- **Names.**
  - The label is `config.version.VERSION_LABEL`, currently `"2026.3.0"` at
    `config/version.py:31`. It is the only definition.
  - The dated comment block sits directly above it. Prompt 02's bump and its dated sentence go
    there, and nowhere else.
- **The keying.**
  - The reserved payload key is `config.version.VERSION_SERIAL_KEY == "_version_serial"`.
  - The factory flag is `"key_on_version": True` in `register()`.
  - The check is `config.version.require_version_serial(payload, cls_name)`.
  - `Datastore.object_get` adds the serial. A test that calls a keyed factory's `build()` directly
    must add `{VERSION_SERIAL_KEY: store._version.store_id}` to its payload, or it raises
    `RuntimeError`.
- **For prompt 03.**
  - **Store both rows under one label.** The planning probe's line [6]
    (`BBNData.build(failure=None)` with two failed rows) no longer reproduces
    `MultipleResultsFound`. The probe stores its rows under 2026.3.0 and looks them up under
    2026.4.0, and they are now invisible there. To show the `failure=None` defect on this tree, a
    test must store both failed rows, and look them up, under one label. The pattern is
    `_insert_failed_bbn` in `Datastore/tests/test_version_keyed_lookups.py` against one
    `DS(version_label=..., db_name=...)`.
  - **The fixture.** That test module's stand-ins (`_scalar_model_query`, `_model_proxy`, the
    `_insert_*` helpers) can be imported or copied for prompt 03's SQLite tests.
- **Suite counts after this prompt:** `CosmologyModels/tests` 18, `ComputeTargets/tests` 30,
  `Datastore/tests` 11. All OK.
- **The consequence**, as stated on the board's header: from this prompt, a lookup returns only
  rows made under the current label; opening an old store recomputes every compute target beside
  the old rows.
