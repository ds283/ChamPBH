# Prompt 07 — pin `datastorekit` `v0.3.0`

**Campaign:** [`README.md`](README.md) · **Board items:** **K4**–**K7** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-12, with prompts 08–10, after 06's review (`fa874a5`).
**Gate:**
- 06 landed and reviewed (board §1), with gate R1 recorded in board §2;
- on branch `dk-and-rpk-migration`, no tracked change in `git status` (the untracked entries listed
  in prompt 01's gate are expected, and are not yours);
- the venv holds `raypoolkit` 0.2.0 at `eaef0ce6…` and no `datastorekit`;
- no ChamPBH Ray process is running (`.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh
  status`). You start none.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Sonnet**. One line of `requirements.txt` and an install, measured
closely, as 03.
**Needs the network**: `git ls-remote` and the install reach GitHub. The orchestrator dispatches
this prompt with that permission.

**Read first:**

1. [`README.md`](README.md): §0.3 and **§0.4** (the release, U6), §2.2 (07's charter), §4.4
   (rollback), §5 (rules 11–13) and §6.7.
2. [`03-pin-raypoolkit-v0.2.0.md`](03-pin-raypoolkit-v0.2.0.md) and its log: 07 is 03 for the
   other package, and 03's log says what each check printed (in particular that a PEP 610 VCS
   `url` carries no `git+`, and that pip's "Resolved … to commit" names the tag object).
3. DatastoreKit at `/Users/ds283/Documents/Code/DatastoreKit`, **at the tag**, through
   `git -C … show v0.3.0:<path>`: `docs/adoption/README.md` §1 and §3, and
   `docs/adoption/champbh.md`'s `v0.2.3` and `v0.3.0` addenda. Its working tree is ahead of the
   tag (other campaigns); the tag governs. **Read; do not edit anything in that repository**
   (README §5 rule 12).
4. `pyproject.toml` at the tag: it declares `ray>=2.43` and `sqlalchemy>=2.0.39,<2.1`, and
   `packages.find` excludes `datastorekit.tests`.

---

## 1. What is wanted

ChamPBH depends on a tagged release, pinned in `requirements.txt`, in a commit of its own (README §5
rule 13). This prompt adds the pin and installs the venv from it. **Nothing in ChamPBH imports the
package until 08**, so every driver's provenance line changes and nothing else does.

- **K4 — the pin.** `requirements.txt` gains exactly this line:
  ```text
  datastorekit @ git+https://github.com/ds283/DatastoreKit@v0.3.0
  ```
  in sorted position: the file is in `pip freeze`'s order, case-insensitive, so between
  `cycler==0.12.1` (`requirements.txt:16`) and `diffrax==0.7.0` (`:17`). No other line changes;
  `ray==2.53.0` and `SQLAlchemy==2.0.46` stay as they are.
- **K5 — the install.** `./venv/bin/pip install -r requirements.txt`, from the repository root.
  Never `-e`, a local path, `--upgrade`, `--force-reinstall` or `--no-deps`. Both declared
  dependencies are already satisfied, so pip should add one distribution and move none. **First a
  dry run** (`--dry-run`), which must report only `datastorekit` to be installed.
- **K6 — the proof that the venv holds the tag.**
- **K7 — what U3 and U6 need of the release, on the installed package** (§2.5), without touching
  ChamPBH's tree.

---

## 2. What to do

1. **Before.** Save into your scratch directory:
   - `./venv/bin/pip freeze`. It must equal `measure/out/52142d7-pip-freeze.txt` plus exactly the
     one line `raypoolkit @ git+https://github.com/ds283/RayPoolKit@eaef0ce60e2fc354bb32f0ad576010e6a83c1976`
     (03's), 100 lines. If it does not, stop: the venv has moved since 03;
   - `./venv/bin/pip check`;
   - `./venv/bin/pip show datastorekit`, which must find nothing.
2. **The tag.** `git ls-remote --tags https://github.com/ds283/DatastoreKit v0.3.0 'v0.3.0^{}'`
   gives tag object `bf0a3b25afa5b4379f9cdc01bca93805ba2a695b` and commit
   `2f5417ea76cdcd0d15d64d80d9085429bfacad21` (README §0.4), and
   `git -C /Users/ds283/Documents/Code/DatastoreKit rev-parse 'v0.3.0^{commit}'` agrees.
3. **K4**, then the dry run, then **K5**. Keep pip's full output.
4. **K6, after:**
   1. **`pip freeze`** differs from the saved one by exactly one added line:
      `datastorekit @ git+https://github.com/ds283/DatastoreKit@2f5417ea76cdcd0d15d64d80d9085429bfacad21`.
      Show the diff.
   2. **`pip check`** reports no broken requirement.
   3. **`utilities.installed_package_provenance()["datastorekit"]`** gives version `0.3.0`, URL
      `https://github.com/ds283/DatastoreKit`, commit `2f5417ea…`, requested revision `v0.3.0`,
      `editable: False`; `["raypoolkit"]` is unchanged (`0.2.0`, `eaef0ce6…`). Show the line
      `format_package_provenance` prints: it is what every build and reader prints from now on, and
      10 checks it.
   4. **The installed files are the tag's.** Compare the installed `datastorekit/` (from
      `importlib.util.find_spec("datastorekit").submodule_search_locations`) with
      `git -C /Users/ds283/Documents/Code/DatastoreKit archive v0.3.0 datastorekit`, extracted into a
      new directory outside both repositories:
      - every `.py` file the archive holds outside `datastorekit/tests/` is installed, byte for byte
        (20 files at the tag: the root's 9, `SQL/`'s 8, `tools/`'s 3);
      - nothing under `datastorekit/tests/` is installed (the archive has 54 files there);
      - nothing else `.py` is installed. Ignore `__pycache__`.
   5. **The import.** `./venv/bin/python -c "import ray, datastorekit, datastorekit.SQL.factory_base, datastorekit.object, datastorekit.contract, datastorekit.store_inventory; print(ray.is_initialized())"`
      prints `False`. Start no Ray. Also `./venv/bin/python -m datastorekit.tools.sharded_store --help`
      exits 0 (10 runs it).
5. **K7, from a throwaway script in your scratch directory**, run with `./venv/bin/python` from the
   repository root, never committed:
   1. **U3, in the abstract.** A subclass of the installed
      `datastorekit.SQL.factory_base.SQLAFactoryBase` that defines only `register` and `build`, as
      20 of ChamPBH's classes do, instantiates. A subclass that defines `register` alone raises
      `TypeError` at instantiation, naming `build`. Show both.
   2. **U3, on ChamPBH's own registry.** Import `Datastore.SQL.Datastore` first and then
      `config.datastore` (the import order of board §3's
      `[05-config-datastore-cannot-be-imported-before-the-layer]`, which 08 closes). For each of the
      27 entries of `config.datastore.factories`, build a class on the installed base from the
      entry's class's own namespace, `type(cls.__name__, (SQLAFactoryBase,), ns)` with `ns` the
      class's `vars()` less `__dict__` and `__weakref__`, so that a hook the class does not define
      falls to the package's base as it will after 08; and instantiate it with the entry's
      constructor argument (the quantity class for the two quantity factories, which keep it as
      `self.ObjectType`; none for the rest). **All 27 instantiate.** Report, by class, which of
      `store`, `validate` and `validate_on_startup` it defines: 3 classes all three, 20 none.
      Nothing in ChamPBH's tree is changed by this.
   3. **U6.** A subclass of the installed `datastorekit.object.DatastoreObject`, constructed as
      ChamPBH's classes construct theirs (`DatastoreObject.__init__(self, store_id)`, the store id
      alone and positionally), has `timestamp` `None`; assigning `timestamp` raises
      `AttributeError`.
6. **The suites.** All three roots at 06's counts: 18 / 114 (61 + 53) / 73. Nothing imports the
   package yet, so nothing may move.

---

## 3. Acceptance

1. **K4:** `git diff` of `requirements.txt` is the one added line, in sorted position.
2. **K5–K6:** every check of §2.4 holds, and each is shown in the log.
3. **K7:** §2.5's three checks, each shown with the script and its output.
4. The three roots give 06's counts.
5. **No deliberate breakage** (README §5 rule 8): 07 adds no code and no test. §2.4's and §2.5's
   checks are its proof; each is one a wrong install or a wrong release would fail.
6. **The records**, in the same commit: this board's §1 row for 07, items K4–K7 and
   `### 5.12 After prompt 07 (<date>)`, naming the rollback: `git revert` plus
   `./venv/bin/pip uninstall -y datastorekit` (README §4.4); a §3 entry for anything opened, with
   `.documents/OPEN_ISSUES.md` in the same commit.

---

## 4. Stop conditions — stop and ask

- `pip freeze` before the install is not 03's.
- The tag on the remote is not `bf0a3b2` / `2f5417e`.
- The dry run, or the install, would change, upgrade or add any distribution but `datastorekit`,
  or `pip check` reports a broken requirement. **Stop before accepting any such change.** If pip
  has already moved something, record exactly what, restore it from the saved `pip freeze`, and
  stop.
- The installed commit is not `2f5417ea…`, the installed files differ from the tag's, or the
  install is editable.
- Any of §2.5's checks fails. That is a finding about the release, for the user to take to
  DatastoreKit (README §5 rule 11), not something to work around here.
- The install cannot reach GitHub. Do not substitute a local path, a wheel built from the checkout,
  or an editable install.
- Any suite's count or result moves.

---

## 5. What this prompt does not do

- **Files it changes:** `requirements.txt`, the log and this board. Nothing else.
- It imports the package from no ChamPBH file, and deletes nothing. The swap is 08's.
- It changes nothing in DatastoreKit, and reads it only at the tag.
- It starts no Ray and opens no store.

---

## 6. The log

`logs/07-pin-datastorekit-v0.3.0.md`, using README §5.1. It holds every check of §2 with its
command and output; pip's dry-run and install output, abridged to what it collected, built and
installed; §2.5's script and its output; and the three suites beside 06's.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`, and made with `git commit -- <paths>`.
