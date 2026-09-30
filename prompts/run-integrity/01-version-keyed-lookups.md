# Prompt 01 — Key the compute-target lookups on the version

**Campaign:** [`README.md`](README.md) · **Board item:** **V** ·
**Board:** `IMPLEMENTATION_STATE.md`. Update your row and V.
**Closes:** `[00-datastore-lookups-ignore-the-version-column]` on the `review-remediation` board.
**Recommended model:** **Opus**. The diff is small but spread across the datastore and three
factories. The judgement is in making the key impossible to bypass, and in touching nothing that
is not a compute target.

**Read first:**

1. [`README.md`](README.md) §0, §2 (a), §5, §6.1.
2. `prompts/review-remediation/IMPLEMENTATION_STATE.md` §3, the entry for the issue above.
3. [`planning-probes/datastore_version_probe.py`](planning-probes/datastore_version_probe.py).
   Run it (about 1 s). It is the pattern for your tests: the undecorated `Datastore` on a
   temporary SQLite file, rows inserted through the datastore's own inserter with explicit
   serials, and stand-ins for the objects a lookup keys on.
4. `Datastore/SQL/Datastore.py`:
   - `:151–225`, construction and the version row;
   - `:281–330`, `_build_schema`, where `"version": True` becomes a column;
   - `:455–520`, `object_get`, the one route to every `build()`;
   - `:613–650`, `_insert`, where the serial is written.
5. `Datastore/SQL/ShardedPool.py:140–190` (one serial across shards) and `:581–610`
   (`object_get_vectorized`).
6. The three compute-target factories: `register()` and `build()` in
   - `Datastore/SQL/ObjectFactories/ScalarModel.py:104–290`,
   - `AdiabaticHistory.py:94–160`,
   - `BBNData.py:81–180`.

   And one parameter factory, `ExponentialCoupling.py:25–84`, which must stay as it is.
7. `main.py:76–100` (the label, its comment and the pool label), `plot_by_beta.py:74–90`,
   `plot_ScalarModel.py:74–90`.
8. `CLAUDE.md`, "Repository mechanics", the test commands.

---

## 1. The changes

**V1 — one label.** Create `config/version.py` holding `VERSION_LABEL`.

- Move `main.py`'s dated comment block (`:80–88`) above it **verbatim**. Add one dated sentence
  beneath: from `run-integrity` prompt 01, lookups of `ScalarModel`, `AdiabaticHistory` and
  `BBNData` return only rows made under this label, and the label is defined here once.
- **The value stays `"2026.3.0"`.** Prompt 02 bumps it.
- `main.py`, `plot_by_beta.py` and `plot_ScalarModel.py` import it and define no label of their
  own. `plot_ScalarModel.py` moves from `"2026.1.1"` to the shared label; that is intended
  (README §2 (a)).
- The label strings those scripts build (`main.py:98–100` and the others) keep their format.

**V2 — the flag.** Each compute-target factory's `register()` declares that it keys on the
version: `ScalarModel`, `AdiabaticHistory`, `BBNData`.

- `_build_schema` records the flag. It raises if the flag is set on a table without
  `"version": True`.
- **No other registration changes.** Not a column, an index or a flag on any other table.

**V3 — delivery.** In `object_get`, for a keyed factory, each payload handed to `build()` is a
**copy** carrying the current serial, `self._version.store_id`, under one reserved key.

- The caller's dict is not mutated.
- The scalar and vectorized paths both go through this code.

**V4 — the filter.** Each keyed `build()` reads the reserved key and adds
`table.c.version == <serial>` to its lookup query.

- **If the key is absent, it raises.** A keyed lookup never falls back to unfiltered.
- Nothing else in the query changes: not the failure handling, not the ordering, not the tags.

**V5 — tests register a third suite.** `Datastore/tests/`, with an `__init__.py`. Add its
`unittest discover` command to `CLAUDE.md`'s test commands, beside the other two. That line is
the only edit to `CLAUDE.md`.

**V6 — documents, additively.** `.documents/architecture-summary.md` describes the datastore.
Add a dated note where it describes lookups or the version column, saying:

- which tables are keyed;
- how the serial reaches `build()`;
- that parameter tables are not keyed.

Do not rewrite what is there (CLAUDE.md rule 6).

Names in V2–V4 — the flag and the reserved key — are an IMPLEMENTATION CHOICE; the README
suggests `key_on_version` and `_version_serial`. Record your choice.

---

## 2. Tests — `Datastore/tests/test_version_keyed_lookups.py`

No Ray, no solve. Build the undecorated `Datastore.__ray_actor_class__` on a file in a
`tempfile` directory, and reopen the same file under a second label. Insert rows through
`store._schema[<table>]["insert"]` with explicit serials, as the probe does, so the version
column is filled by production code. Use stand-ins for the key objects.

- **(a) `ScalarModel`.** A failed row stored under A:
  - is returned under A;
  - is **not** returned under B.

  **Must fail on `HEAD~1`.**
- **(b) `AdiabaticHistory`.** The same, for a row keyed to a `ScalarModel` serial. **Must fail
  on `HEAD~1`.**
- **(c) `BBNData`.** The same, both with `failure=True` and with the default lookup. Use a
  failed row for the first and a success row (no values, `_do_not_populate`) for the second.
  **Must fail on `HEAD~1`.**
- **(d) The key cannot be bypassed.** Calling a keyed factory's `build()` directly, with a payload
  that lacks the reserved key, raises. **Must fail on `HEAD~1`.**
- **(e) Parameter tables are unkeyed.** An `ExponentialCoupling` obtained under A and then under B,
  for the same β stand-in, has the same serial, and the table has one row. This passes on
  `HEAD~1` too. It is a regression guard; its docstring says so.
- **(f) One label.** Parse `main.py`, `plot_by_beta.py` and `plot_ScalarModel.py` with `ast`,
  without importing them: `main.py` runs `ray.init` at import. None assigns `VERSION_LABEL`,
  and each imports it from `config.version`. **Must fail on `HEAD~1`.**

`Datastore/tests` rises from 0 to the number of methods you add. The other two suites must pass
unchanged: `CosmologyModels/tests` 18, `ComputeTargets/tests` 30.

---

## 3. What this prompt does not do

- No schema change (README §0.4). No migration.
- No key on any parameter table, or on a value or tag table.
- No change to the `version` factory, to `validate_on_startup`, to `inventory`, or to
  `read_table`.
- No change to `main.py`'s pipeline logic. The lookups in `run_pipeline` are prompt 03's.
- No version bump. The value is `"2026.3.0"` before and after.
- No edit to `Datastore/SQL/ObjectFactories/base.py`, which is not black-clean. If the design
  seems to need one, stop and ask.

## 4. Acceptance

1. README §6.1, every row, with measured values in the log.
2. `grep -rn "VERSION_LABEL =" --include='*.py' . | grep -v "venv/\|thirdparty/\|claude-context/"`
   finds exactly one line, in `config/version.py`.
3. All three suites pass. `black --check` is clean on the changed Python files.
4. The board and the index:
   - V done;
   - the issue closed: its row deleted from `.documents/OPEN_ISSUES.md`, and a dated
     **Resolved** line added to its `review-remediation` board entry (README §5 rule 4);
   - count and date corrected.

## 5. Stop conditions — stop and ask the user

- The serial cannot reach `build()` without changing the `build()` signature of every factory, or
  without editing `base.py`.
- A lookup other than the three compute targets turns out to key on the version, or to need to.
- A test cannot be built without Ray.
- Either existing suite fails, or its count changes.

## 6. The log and the board

`logs/01-version-keyed-lookups.md`, in the README §5.1 template. Beyond the template:

- the names you chose for the flag and the reserved key;
- how each of (a)–(d) and (f) was shown to fail on `HEAD~1`, with what it printed;
- the new `CLAUDE.md` line verbatim;
- the consequence, on the board's header: **from this prompt, a lookup returns only rows made
  under the current label; opening an old store recomputes every compute target beside the old
  rows.**
