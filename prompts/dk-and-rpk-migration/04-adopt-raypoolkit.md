# Prompt 04 — adopt `raypoolkit`

**Campaign:** [`README.md`](README.md) · **Board items:** **W1**–**W4**; gate **RayPoolKit G3** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-11, with prompts 01–03, after the user reviewed the plan (`97f50f8`).
**Gate:**
- 03 landed and reviewed (board §1), and the venv holds `raypoolkit` 0.2.0 at `eaef0ce6…`, not
  editable (`utilities.installed_package_provenance()`);
- on branch `dk-and-rpk-migration`, no tracked change in `git status` (the untracked entries listed
  in prompt 01's gate are expected, and are not yours).

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Sonnet**. Two files deleted, three imports rewritten, and a documentation
table corrected, each measured.

**Read first:**

1. [`README.md`](README.md): §0.1, §2.2 (04's charter), §4.4, §5, and §6.2 (P7, P14, P16).
2. RayPoolKit's adoption checklist for ChamPBH, `/Users/ds283/Documents/Code/RayPoolKit/docs/adoption/champbh.md`,
   in full: items 2–9 are this prompt's facts at `52142d7`. Its `docs/adoption/README.md` §2 (the
   module map) and §6. **Read; do not edit anything in that repository** (README §5 rule 12).
3. RayPoolKit's `README.md` at the tag (`git -C <RayPoolKit> show v0.2.0:README.md`): "Changes in
   `v0.2.0`", "The constructor", "Which handlers a pool needs" and "The printed lines are part of the
   interface".
4. `RayTools/RayWorkPool.py`, which you delete: its constructor (from `:84`), so that you know what
   `store_handler` meant in ChamPBH's copy (the persist step; RayPoolKit decision U10).
5. `.documents/architecture-summary.md` §3.2 (from `:153`) and `:585`.

---

## 1. What is wanted

ChamPBH's `RayTools/RayWorkPool.py` is replaced by the packaged `raypoolkit.RayWorkPool`. By the
checklist, no call site changes: under `v0.2.0` every one of ChamPBH's 17 constructions constructs as
it stands and does what ChamPBH's copy does (checklist item 4). So the change is a deletion and three
imports.

- **W1 — delete `RayTools/`:** `RayTools/RayWorkPool.py` and `RayTools/__init__.py`. With them goes
  the copy's `from Datastore.SQL.ShardedPool import ShardedPool` (`RayTools/RayWorkPool.py:23`), so
  DatastoreKit's rewrite list loses one line and no U12 exception is needed (README U1).
  `utilities.format_time`, which the copy imports (`:24`), stays ChamPBH's.
- **W2 — rewrite the three imports** to `from raypoolkit import RayWorkPool`:
  `main.py:50`, `plot_ScalarModel.py:44`, `plot_by_beta.py:38`. Re-measure first (§2.1); if the
  script finds any other import, string literal or `import_module` naming `RayTools`, stop.
- **W3 — change no pool site** (P7). The 14 lookup-only sites keep `store_handler=None`, and the 3
  compute sites (`main.py:276`, `:533`, `:820`) keep their handlers. `git diff` of the three drivers
  is the one import line each.
- **W4 — the prose this commit makes false** (P14), in `.documents/architecture-summary.md`:
  - the repository layout, `:35-36`: `RayTools/` and `RayWorkPool.py` leave it, and a line says
    the queue is the `raypoolkit` package, pinned in `requirements.txt`;
  - §3.2's title, `:153`: "`RayWorkPool` (`raypoolkit`)", or similar;
  - §3.2's constructor table, from `:157`. Its `store_handler` row (`:165`) gives the copy's
    meaning, "`Callable(obj, pool) → ObjectRef` | `pool.object_store(obj)` | Persists result".
    Under `v0.2.0` `store_handler` is the local step, default `obj.store()`, and the datastore
    round-trip is `persist_handler`, default `pool.object_store(obj)`, a row the table lacks. Take
    both rows, and any other default or type the table states, from the package's README, "The
    constructor";
  - §3.2's lifecycle text, which says each work item is "tracked by the string tag in
    `self._data[ref.hex]`" (`:180`): the package keys in-flight work on each ref's value (RayPoolKit
    checklist item 5). Correct it, or say it plainly without the private detail;
  - `:585`, "via its default `compute_handler` and `store_handler`": under `v0.2.0` the steps are
    three defaults, compute, store and persist.

  Change nothing else in the document. The dated audit `.documents/integrator-audit-2026-09-30/README.md:69`,
  which names `RayTools/RayWorkPool.py`, is a record of its tree and stays (P14), as does every line
  under `prompts/`. Other lines in §3.2 and elsewhere that describe what the pool does, and stay true
  (checklist item 9 lists them), are left as they are; 11 rewrites the database sections.

---

## 2. What to do

### 2.1 Before

1. **Re-measure**, from RayPoolKit's root with its own venv:
   ```bash
   ./venv/bin/python docs/adoption/measure_client_pools.py cpbh --commit <ChamPBH HEAD's hash>
   ```
   Expect 3 imports, 0 string
   literals, 0 `import_module` calls, 17 calls, every one "constructs", as at `52142d7`
   ([`measure/out/52142d7-raypoolkit-measure_client_pools.md`](measure/out/52142d7-raypoolkit-measure_client_pools.md)),
   but for the line numbers that 01's provenance line moved. Save the output; show its difference
   from the `52142d7` appendix, and explain each line of it.
2. `git grep -n RayTools -- ':!prompts' ':!.documents'` lists only `RayTools/` itself and the three
   imports.

### 2.2 The change

W1–W4, as §1 says.

### 2.3 Verification

1. **By RayPoolKit's script, at a commit of the staged tree.** Make a throwaway commit of the staged
   change on a temporary branch (never on `dk-and-rpk-migration`), run the script at it, then delete
   the branch. It reports 0 imports, 0 string literals and 0 `import_module` calls naming
   `RayTools`; 17 calls, every one "constructs"; and `RayTools/` absent. Save the output.
2. `git ls-tree <that commit> RayTools` is empty.
3. **The imports resolve, without running a driver.** A short script, kept in the log, parses each
   of the three drivers with `ast` and, for every `import` and `from … import` in it, finds the
   module with `importlib.util.find_spec` and, for a `from` import, checks the name is an attribute
   of the imported module. Every one resolves. It never executes a driver (they connect to Ray at
   import).
4. `./venv/bin/python -m py_compile main.py plot_by_beta.py plot_ScalarModel.py` succeeds.
5. **The suites:** all three roots at 03's counts. No test names `RayTools` or `RayWorkPool`
   (checklist item 6), so none changes.
6. `black --check` is clean on every Python file touched.

### 2.4 Deliberate breakage (README §5 rule 8)

Each applied to the working tree, shown to be caught, reverted, and recorded as a diff:
1. `main.py`'s import reverted to `from RayTools.RayWorkPool import RayWorkPool`: §2.3.3 fails (the
   module is gone), and the script at a throwaway commit reports one import.
2. `plot_by_beta.py`'s import misspelled as `from raypoolkit import RayWorkPoool`: §2.3.3 fails on
   the name.
3. One lookup-only site's `store_handler=None` replaced by `store_handler=lambda obj, pool: None`
   (a callable in the copy's old meaning, which U10 says must become `persist_handler`): the script's
   verdict for that site is not "constructs", or, if it is, record exactly what it reports. This is a
   measurement of the script, not a test of ChamPBH.

---

## 3. Acceptance

1. **W1:** `RayTools/` is gone, at the commit.
2. **W2–W3:** `git diff` of the three drivers is the one import line each.
3. **By RayPoolKit's script:** §2.3.1's figures.
4. **The imports resolve** (§2.3.3), and the drivers byte-compile.
5. **The suites** give 03's counts; `black --check` is clean.
6. **W4:** `git diff .documents/architecture-summary.md` touches only the lines §1 names, and each
   corrected statement agrees with the package's README at `v0.2.0`.
7. **The breakages of §2.4** are caught, and recorded as diffs.
8. **RayPoolKit's G3** is recorded on this board as holding, by its checklist item 8, items 1–5:
   `RayTools/` gone; no import of it by the script; the pin in `requirements.txt` and the venv
   installed from it (03); the suites passing; every site constructing, 17 found. Item 6, the record
   on RayPoolKit's own extraction board, is a commit in that repository, drafted at 11 (P16). R1 (06)
   adds the run under real Ray.
9. **The records**, in the same commit: this board's §1 row for 04, items W1–W4, the RayPoolKit G3
   row of §2, and `### 5.5 After prompt 04 (<date>)`; a §3 entry for anything opened, with
   `.documents/OPEN_ISSUES.md` in the same commit.

---

## 4. Stop conditions — stop and ask

- The re-measure (§2.1) finds an import, string literal or `import_module` naming `RayTools` that
  the checklist does not, or a call whose verdict is not "constructs", or a number of calls other
  than 17.
- A site turns out to need a change to construct, or to keep its behaviour. P7 and the checklist say
  none does; if one does, that is a finding about the package or the checklist.
- An import does not resolve, or any suite's count or result moves.
- A defect in `raypoolkit` itself: it is not patched or worked around here (README §5 rule 11).

---

## 5. What this prompt does not do

- **Files it changes:** deletes `RayTools/RayWorkPool.py` and `RayTools/__init__.py`; one line in
  each of `main.py`, `plot_by_beta.py` and `plot_ScalarModel.py`; the lines of
  `.documents/architecture-summary.md` that §1 names; the log and this board. Nothing else.
- It touches no `Datastore/` file: `RayTools/RayWorkPool.py:23`'s layer import is deleted with the
  file, not rewritten.
- It runs no build and starts no Ray. The run under real Ray is 06's.
- It changes nothing in RayPoolKit.

---

## 6. The log

`logs/04-adopt-raypoolkit.md`, using README §5.1. It holds the re-measure and the measure at the
throwaway commit, with their difference from the `52142d7` appendix; the import-resolution script and
its output; the deliberate-breakage record as diffs; the three suites beside the dispatch baselines;
and the text of the RayPoolKit G3 record, for 11 to carry into its draft for RayPoolKit's board.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only (`git rm` for the two deletions),
after showing `git diff --cached --name-only`.
