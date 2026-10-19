# Prompt 02 — the fingerprinter

**Campaign:** [`README.md`](README.md) · **Board items:** **F1**–**F4** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-19, after the user's rulings (README §6). Its orchestrator note, written after
01's review, hands it 01's counts and governs where it differs from this file.
**Gate:**
- 01 landed and reviewed, with the gate "Pin" holding (board §2);
- on branch `rrk-adoption`, no tracked change in `git status` (the untracked entries are the
  user's, as 01's gate lists them, plus nothing of yours);
- no ChamPBH Ray process is running. You start none.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Sonnet**. A small module after SecondaryGWKit's, a guard test from a
published recipe, and a golden test on a temporary store built by an existing harness.

**Read first:**

1. [`README.md`](README.md): §0.3 ("The factory registry"), §2.2 (02's charter), §5, §6.2 (P8, P9).
2. RunRegistryKit at `v0.2.0` (read, never edit):
   - `runregistrykit/fingerprint.py`: the protocol, `DatastoreKitFingerprinter`,
     `load_fingerprinter`;
   - `docs/adoption/README.md` §6, "The Q5 guard": the recipe and its sketch;
   - `docs/adoption/champbh.md` items 3, 6 and 8: this repository's registry, its specs and its
     three polymorphic parents, measured at `c21cdfb`;
   - its `README.md`, "The store layer".
3. SecondaryGWKit at `031bcb4` (`git -C /Users/ds283/Documents/Code/SecondaryGWKit show
   031bcb4:<path>`; read, never edit), the model for every file here:
   - `config/fingerprint.py`;
   - `ObjectFactories/tests/test_fingerprint_format_guard.py`, including `test_a_key_keeps_its_type`;
   - `ObjectFactories/tests/test_store_fingerprint_golden.py`, with `TestGolden` and
     `TestConfigFingerprint`;
   - `ObjectFactories/tests/data/`, its golden files and their naming.
4. This repository:
   - `config/datastore.py` (`factories`, 27 entries);
   - `ObjectFactories/ScalarModel.py`'s `inventory_spec` (the polymorphic `Parent.types` at `:237`,
     `:242` and `:253`) and the three `model_ids` modules it keys by;
   - `ObjectFactories/tests/test_inventory_specs.py` (coverage, and its docstring's sentence about
     RunRegistryKit at `:25-29`);
   - `ObjectFactories/tests/test_version_keyed_lookups.py` (`_TempStoreCase`);
   - `tools/tests/test_inventory_report.py` (`_InventoryStoreCase`, `insert`, `build_store`): a
     store with real rows, its actors closed before it is read;
   - `runregistrykit.toml`, as 01 left it.

---

## 1. What is wanted

The store layer can take, compare and record the fingerprint of a ChamPBH store. What goes into
the fingerprint is this repository's: the factories' inventory specs, the registry and a format
number. A spec that changes without the number moving fails a test, naming the class.

### 1.1 The items

- **F1 — `config/fingerprint.py`**, after SecondaryGWKit's:
  - a module docstring saying who does what (the specs and the format are ChamPBH's; encoding and
    digest are DatastoreKit's; when and where it is taken is RunRegistryKit's), and naming the
    test that guards the format;
  - `FINGERPRINT_FORMAT = 1`, the first of a history (P8), with a comment saying when the number
    moves: a change to any spec, or to the registry's classes or order;
  - `SPEC_DIGESTS = {1: [[class, sha256], …]}`, one pair per class with a spec, in registry order,
    computed by F3's functions and pasted in;
  - `fingerprinter = DatastoreKitFingerprinter(factories, FINGERPRINT_FORMAT)`.

  Importing it loads nothing that `config.datastore` does not already load: check by
  `python -X importtime`, or by `sys.modules` before and after, and say which.
- **F2 — the config names it.** `runregistrykit.toml` gains:
  ```toml
  [stores]
  fingerprinter = "config.fingerprint:fingerprinter"
  ```
- **F3 — `ObjectFactories/tests/test_fingerprint_format_guard.py`**, the recipe:
  - `plain(value)`: dataclasses as objects of their fields, sequences as lists, and **every mapping
    key written as its own JSON**, so that `0` (`0`) and `"0"` (`"0"`) stay apart;
  - `spec_digests(factories)`: `[[name, sha256(canonical_json(plain(spec)))], …]` over
    `datastorekit.store_inventory.inventory_specs(factories)`, in registry order.

  The tests:
  - `spec_digests(factories) == SPEC_DIGESTS[FINGERPRINT_FORMAT]`. A failure names the classes
    that differ and says to bump the format and add a pin;
  - a pin exists for the current format;
  - **the key keeps its type**: a stand-in `Parent` whose `types` is keyed by `0`, and one keyed by
    `"0"`, give different digests, and `canonical_json` refuses the first unconverted.

  The count of pairs is the number of classes with a spec. Measured at `c21cdfb` it was 17; state
  your own count, and that it equals `test_inventory_specs`'s.
- **F4 — `ObjectFactories/tests/test_store_fingerprint_golden.py`.**
  - **`TestGolden`.** A store is built in a `tempfile` directory with the harness of
    `tools/tests/test_inventory_report.py` (`_InventoryStoreCase`, through `_TempStoreCase`). It
    holds at least one row of every class with a spec, and `ScalarModel` rows of at least two
    potential types, so that the polymorphic resolution is read. You may import that module's
    `build_store` and extend its rows, or build your own in the same way; say which in the log.
    The test:
    - closes the actors;
    - takes the fingerprint with `config.fingerprint.fingerprinter.take(primary, taken={})`;
    - checks that `problems` is empty;
    - checks that the fingerprint, with `taken` removed, equals the committed golden file
      `ObjectFactories/tests/data/store_fingerprint_rrk-adoption-02.json`.

    The file is written once by this prompt, from the store's fingerprint, and never overwritten
    (P8). The test's docstring says how a later format adds a file beside it.
  - **`TestConfigFingerprint`.**
    - `runregistrykit.fingerprint.load_fingerprinter(find_client_root(client_root=<root>))` returns
      `config.fingerprint.fingerprinter`, an instance and a `Fingerprinter`;
    - its `fingerprint_format` is `FINGERPRINT_FORMAT`;
    - its `factories` is `config.datastore.factories`.
  - **The command line, once.** `python -m runregistrykit --client-root <root> store fingerprint
    <the temporary store> --runs-root <a temporary runs root>`, by subprocess. It prints the golden
    digest and exits as RunRegistryKit's README says for a store with no sidecar. `--runs-root` keeps
    the command from reading the repository's `var/runs`. Record its output in the log.

  No Ray is initialised (the harness asserts it). Nothing under `var/` is opened or created.

---

## 2. Verification

1. **Before any edit:** the four roots at the counts 01 handed on, serially, ComputeTargets in two
   halves.
2. **After:** the four roots, ObjectFactories with the new tests and the others unchanged; `black
   --check` on every new file. Check again that no `var/` exists.
3. **The golden file's provenance**: the log records how it was written (the command or the few
   lines that wrote it), its SHA-256, and its overall `digest`.
4. **Deliberate breakages** (README §5: each recorded as a diff, never committed). Each must fail
   the named test:
   - a leaf added to one spec, with no bump: F3, naming the class;
   - `FINGERPRINT_FORMAT = 2` with no new pin: F3;
   - a `Parent.types` key changed from `EXPONENTIAL_COUPLING` (`0`) to `"0"`: F3;
   - one value changed in the golden store's rows: F4's `TestGolden`;
   - `fingerprinter` named as the class in the config: F4's `TestConfigFingerprint` (the loader
     refuses a class).

## 3. Acceptance

1. F1–F4 as §1.1 says.
2. §2's checks in the log, each with its command and output.
3. Suites: CosmologyModels 18, ComputeTargets 176, ObjectFactories 69 + the new tests, tools as 01
   left it.
4. **One commit**, with the log `logs/02-the-fingerprinter.md`. The board: row 02, F1–F4, the gate
   "Fingerprint", and a `### 5.3 After prompt 02 (<date>)`. Write "this commit" where the SHA goes.

## 4. Stop conditions: stop and ask

- `read_inventory` reports a problem on the golden store that the harness's rows should not
  have. The rows are wrong, or a spec is; either way it is not this prompt's to change a spec.
- The count of specs differs from `test_inventory_specs`'s.
- `load_fingerprinter` or `store fingerprint` behaves otherwise than RunRegistryKit's README says.
  That would be a package defect, reported there, never worked around (README §5).
- A spec, a factory or the registry would need a change.
- Anything would need an edit outside §5's list.

## 5. What this prompt does not do

- **Files it changes:**
  - `config/fingerprint.py` (new);
  - `runregistrykit.toml` (`[stores]`);
  - `ObjectFactories/tests/test_fingerprint_format_guard.py` (new);
  - `ObjectFactories/tests/test_store_fingerprint_golden.py` (new);
  - `ObjectFactories/tests/data/store_fingerprint_rrk-adoption-02.json` (new);
  - this board, and the log.
- It changes no spec, factory or registry entry, and no existing test. `test_inventory_specs.py`'s
  docstring still says ChamPBH "is expected to adopt" RunRegistryKit; that is 05's to note, not
  this prompt's to edit (`CLAUDE.md` invariant 4).
- It opens no store but its temporary one, starts no Ray, and creates no `var/`.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`.
