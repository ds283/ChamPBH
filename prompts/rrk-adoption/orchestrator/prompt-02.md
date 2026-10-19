# Orchestrator — prompt 02, the fingerprinter

Read [`../README.md`](../README.md) first: §0.3 ("The factory registry"), §2.2 (02's charter), §5
and §6.2 (P8, P9, P11). The conventions are note 01's ([`prompt-01.md`](prompt-01.md) §0,
"Conventions"), restated here where 02 needs them.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`02-the-fingerprinter.md`](../02-the-fingerprinter.md)
**Board items:** F1–F4 · **Gate:** "Fingerprint" · **Closes:** nothing · **Opens:** only what the
work finds · **Model:** Sonnet, as the prompt recommends · **Network:** none.

**Gate:**
- 01 landed (`b0f3488`) and was reviewed (`caeadc0`); the gate "Pin" holds (board §2). `HEAD` is
  the commit that adds this note, on top of `7331391` (01's licence-header housekeeping, which
  touched `tools/tests/test_registry_config.py` only).
- The prompt's own gate (its header): no tracked change, no `var/`, no ChamPBH Ray process.
- One prompt at a time in this checkout. 03 is not dispatched until 02 is reviewed.

## 0. What makes this prompt unusual

**It writes a golden file that is never overwritten** (P8). Whatever store the test builds, its
fingerprint is committed once and binds every later format of this repository. So the store must
be built from literals the golden module owns, deterministically, and it must exercise every class
and every polymorphic type it can. The orchestrator built two harness stores in a row at `7331391`
and their fingerprints were equal (`7eba2c6d…`), `version` row included; the harness's
`version_label` and the fixed insert timestamps make it so.

**Four statements in the prompt are wrong, loose or silent. Ruled here:**

1. **The count of pairs is 21, not 17.** The prompt's F3 says "Measured at `c21cdfb` it was 17".
   That figure was the count of `def inventory_spec` **definitions** (RunRegistryKit's checklist,
   item 8 of its appendix). `inventory_specs(factories)` returns one entry per **registered class**
   with a spec, and 17 definitions serve 21 classes: `DimensionfulQuantity`'s serves `M_value`,
   `Lambda_value`, `temperature`, `phi_value` and `pi_value`. `test_inventory_specs.py`
   says the same (`test_seventeen_definitions_serve_twenty_one_classes`, `:162-165`). So
   `SPEC_DIGESTS[1]` holds **21 pairs**, with **15 distinct digests**. The shared ones, measured by
   the orchestrator: `version` = `store_tag`; `M_value` = `Lambda_value`; `phi_value` =
   `pi_value`; `InversePowerPotential` = `ExponentialPotential`; `StarobinskyPotential` =
   `ReclinerPotential` = `ReflectingPotential`. The comment above `SPEC_DIGESTS` says pairs may
   share a digest, as SecondaryGWKit's does, which is why it is a list of pairs and not a map from
   digest. The prompt's stop "The count of specs differs from `test_inventory_specs`'s" compares
   against 21. Classify as `STRUCTURALLY REQUIRED`, citing this note.

   The order is `inventory_specs`' order, which is **registry order**: `…, pi_value,
   InversePowerPotential, StarobinskyPotential, ExponentialPotential, ReclinerPotential,
   ReflectingPotential, ExponentialCoupling, QCD_Cosmology, IntegrationSolver, ScalarModel,
   AdiabaticHistory, BBNData`. This is **not** `inventory_classes`' order (`EXPECTED_ORDER` in
   `test_inventory_specs.py`), which puts `QCD_Cosmology` and `IntegrationSolver` before the
   potentials. The pin follows `inventory_specs`, as the recipe says.

2. **"Loads nothing that `config.datastore` does not already load" is not literally true, and the
   docstring must not say it.** SecondaryGWKit's docstring says it; it is loose. Measured by the
   orchestrator: after `import config.datastore`, importing `runregistrykit.fingerprint` adds **9
   modules**: `runregistrykit`, `runregistrykit.client`, `runregistrykit.fingerprint`, and the
   standard library's `tomllib` (four modules), `getpass` and `termios`. It adds no module of
   `ray`, `sqlalchemy` or `datastorekit`, which `config.datastore` has already loaded. **Ruled:** F1's
   check is by `sys.modules` before and after `import config.fingerprint` (with `config.datastore`
   imported first), in a probe run from the agent's scratch directory, whose output goes in the
   log. The docstring says what is true: importing it adds RunRegistryKit's own modules and the
   standard library's they need, and nothing of Ray, SQLAlchemy or DatastoreKit beyond what
   `config.datastore` loads, because `DatastoreKitFingerprinter` imports DatastoreKit only inside its
   methods. Classify as `IMPLEMENTATION CHOICE`, citing this note.

3. **The golden store: its own builder, in the golden module, with a row of every class and a
   `ScalarModel` of every potential type.** The prompt allows importing
   `tools/tests/test_inventory_report.py`'s `build_store` or building one "in the same way". Ruled:
   **in the same way**. `build_store` cannot be extended (it closes the actors before it returns, and
   existing tests are not modified), and the orchestrator's probe shows its store has **no row** of
   `InversePowerPotential`, `StarobinskyPotential`, `ReclinerPotential` or `ReflectingPotential`
   (count 0 each), so it does not meet F4's "at least one row of every class with a spec". And a
   golden store's rows must not move when another test module's helpers move.
   - The module subclasses `_TempStoreCase` (from
     `ObjectFactories/tests/test_version_keyed_lookups.py`, the harness the prompt names) directly,
     and carries its own `insert`, `_filled` and actor-closing, in the form of
     `_InventoryStoreCase` (`tools/tests/test_inventory_report.py:69-109`). The docstring says the
     form was taken from there. It does **not** import anything from `tools.tests`.
   - Its store: a version label of its own; one row of each of the 21 classes; **five
     `ScalarModel` rows, one for each of the five `potential_type` keys**, each with its own
     potential row, so that every polymorphic key of `ScalarModel`'s spec is resolved (`cosmology`
     and `coupling` have one type each). At least one `ScalarModel` tagged with a `store_tag` and at
     least one not, so that two tag sets are read. Value rows on at least one `ScalarModel`, the
     `AdiabaticHistory` and the `BBNData`, so that the value slots are read. Fixed values and fixed
     insert timestamps throughout.
   - The orchestrator's probe built a store of this shape at `7331391` (every potential row with
     `M_serial` and `Lambda_serial`, `n_value` for `InversePowerPotential` and
     `ExponentialPotential`): `read_inventory` reported **no problem**, every class counted at
     least 1, `ScalarModel` 5. So a problem on the agent's store means a wrong row, which is the
     prompt's first stop.

   Classify as `IMPLEMENTATION CHOICE`, citing this note.

4. **The golden file's form and `taken`.** The prompt says the test takes the fingerprint with
   `take(primary, taken={})` and compares "with `taken` removed". Ruled, as SecondaryGWKit's:
   - the file is written once from `fingerprinter.take(primary, None)`, so its `taken` is `null`,
     as `json.dumps(fp, indent=2, sort_keys=True) + "\n"` (the form of SecondaryGWKit's golden files,
     checked byte for byte by the orchestrator);
   - the test takes `take(primary, taken={})`, as the prompt says, asserts the golden's `taken` is
     `None`, its `fingerprint_format` is `FINGERPRINT_FORMAT` and its `encoding_format` is
     DatastoreKit's `ENCODING_FORMAT`, and compares the two with `taken` popped from both
     (`without_taken`);
   - the test builds the store twice and asserts both fingerprints equal the golden, as
     SecondaryGWKit's does, since a golden that a second build misses is no golden.

   Classify as `IMPLEMENTATION CHOICE`, citing this note.

**The command line (F4, "once") is a test**, `TestCommandLine` in the golden module, not only a log
entry. The orchestrator ran the command on a probe store with a probe client root at `7331391`:
`python -m runregistrykit --client-root <root> store fingerprint <primary> --runs-root <empty dir>`
prints `store:`, `digest:`, `format:`, the class table, `problems: none`, and ends **`recorded: none
recorded (the sidecar is absent)`**, and exits **0** (`runregistrykit/__main__.py:337` and
`:371-373`: with nothing recorded the comparison is `None`, and the exit is 0). It wrote nothing
beside the store and nothing under the runs root. So the test, by subprocess with `cwd` the
repository root and `--client-root` the repository root:
- asserts exit 0;
- asserts the `digest:` line is the golden's `digest`, and the `format:` line is `1`;
- asserts the last line is `recorded: none recorded (the sidecar is absent)`;
- asserts the temporary directory holds afterwards only the store's own files (no sidecar), and
  that no `var/` exists at the root.

Paste the command's whole output in the log, once.

**The deliberate breakages, read concretely:**
- *A leaf added to one spec*: add one to a spec with a single class, such as `tolerance`'s
  (`ObjectFactories/tolerance.py`), so the failure names one class. A leaf added to
  `DimensionfulQuantity`'s would name five, which is right but a weaker demonstration. The leaf
  need not be a column; `test_inventory_specs` may fail too, and that is not the point.
- *`FINGERPRINT_FORMAT = 2` with no new pin*: F3 fails with its "no pin" message (SecondaryGWKit's
  form fails with a message, not a `KeyError`). `TestGolden` fails too, on the format; say so.
- *A `Parent.types` key `0` → `"0"`*: in `ObjectFactories/ScalarModel.py`'s `coupling` parent,
  `{EXPONENTIAL_COUPLING: …}` → `{"0": …}`. F3 fails naming `ScalarModel`.
  `test_inventory_specs.test_the_polymorphic_maps_are_the_model_ids` fails too; say so.
- *A value changed in the golden store's rows*: one literal in the builder. `TestGolden` and
  `TestCommandLine` fail.
- *The class named in the config*: `fingerprinter = "config.fingerprint:DatastoreKitFingerprinter"`
  (the class is in that module's namespace). `TestConfigFingerprint` errors with the package's
  `ConfigError` ("is the class … not an instance"), and `TestCommandLine` fails (exit 1, `!!`).
  `tools/tests/test_registry_config.py` still passes; say so.

**The guard's key test** is the prompt's stronger form, not SecondaryGWKit's bare dicts: two stand-in
`datastorekit.store_inventory.Parent`s, `Parent("coupling_serial", type_column="coupling_type",
types={0: "ExponentialCoupling"})` and the same with `"0"`. The orchestrator measured: `plain`
gives `{"0": …}` and `{"\"0\"": …}`, so their canonical JSON and digests differ; and
`canonical_json(dataclasses.asdict(<the int-keyed one>))` raises `TypeError: canonical_json(): a key
must be a string, not 0`, while the string-keyed one is accepted. Assert the `TypeError` with
`assertRaises`.

**What moves on purpose:** the prompt's §5 list, and nothing else.
- `config/fingerprint.py` (new), with the licence header every module carries (01's test lacked
  it; `7331391` added it).
- `runregistrykit.toml`: the `[stores]` table, appended. The two-line comment at its top stays.
  Note 01's ruling 2 made `tools/tests/test_registry_config.py` accept it, and the orchestrator's
  review of 01 confirmed it by probe. **That test is not edited.**
- `ObjectFactories/tests/test_fingerprint_format_guard.py` and
  `ObjectFactories/tests/test_store_fingerprint_golden.py` (new), each with the licence header.
- `ObjectFactories/tests/data/` (new directory) with `store_fingerprint_rrk-adoption-02.json`
  only. No `__init__.py` there.
- The log `logs/02-the-fingerprinter.md` and the board.
- No issue is expected. The index (`.documents/OPEN_ISSUES.md`) is untouched unless one opens.

**The facts, checked by the orchestrator on 2026-10-19 at `7331391`.** Pass them on.
- **The tree.** Branch `rrk-adoption`. Nothing staged, no tracked change. The untracked set is
  note 01's 22 entries (17 `pilot-2026.6.0*.db`, `pilot-main.log`, `pilot-plot.log`, `pilot-out/`,
  `full_run_2026.6.0.sh`, `full_run_2026.6.0-refresh.sh`). No `var/`. The main checkout.
- **Ray.** `ray_cluster.sh status` exits 0: lock free, no ChamPBH process, ports free.
  `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing. `RAY_ADDRESS` is unset.
- **The venv.** `runregistrykit` 0.2.0 at `0bae3a3`, `datastorekit` 0.4.0
  (`ENCODING_FORMAT = 1`), Python 3.13.16, black 26.1.0. No install of any kind in this prompt.
- **The packages' API** (read at `v0.2.0` and in the venv):
  - `DatastoreKitFingerprinter(factories, fingerprint_format)`: attributes `factories` and
    `fingerprint_format`; `take(primary, taken)` returns `(fingerprint, inventory)`;
  - `load_fingerprinter(client)` refuses a class before the protocol check, and each refusal is a
    `ConfigError`;
  - `fingerprint_of(inventory, fmt, taken=None)` gives the keys `classes`, `digest`,
    `encoding_format`, `fingerprint_format`, `problems`, `taken`, with `taken` copied or `None`;
  - `datastorekit.store_inventory` exports `canonical_json`, `inventory_specs` and `Parent`.
- **SecondaryGWKit at `031bcb4`** (read with `git -C … show 031bcb4:<path>`; never edit): its
  `config/fingerprint.py`, its guard (with the failure message that sorts changed, added and removed
  classes and the reordering case), its golden test and `ObjectFactories/tests/data/`. Carry the
  form over. Not its history (formats 2–6, `runregistry-extraction`, `datastore-integrity`), and
  not its `StoreFixtures.real_store_fixtures`, which ChamPBH does not have.
- **The baselines** at `7331391`, run by the orchestrator: see §1. The agent runs its own before
  any edit (the prompt's §2.1), and its own govern.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-only`
  before committing. Count, then paste. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its
  follow-up.
- **Suites.** `CLAUDE.md`'s commands, serially, in the foreground, ComputeTargets in its two
  halves, output to a file in the agent's scratch directory, the verdict grepped from it, `uptime`
  beside each. A failure is re-run alone before it is called real.
- **Breakage diffs** are recorded so that `git apply` replays them from a clean tree at the agent's
  commit. Check each with `git apply --check` and `-R --check`, and replay in `bash`, not zsh.
- **The golden file's provenance.** The few lines that wrote it go in the log verbatim, with the
  file's SHA-256 (`shasum -a 256`) and its `digest`. It is written once; if it must be rewritten
  before the commit (a builder change), say so and why.
- **Scratch.** The agent uses its own subdirectory of the session scratchpad, never `/tmp`. No
  scratch file goes under `config/`, `ObjectFactories/` or `.documents/`.
- **Stores.** None is opened but the tests' temporary ones; the pilot store is never named. No
  `runregistrykit store` command is run except `TestCommandLine`'s, on its temporary store.
- **Ray.** The agent starts no Ray and runs no driver.
- **`var/`.** It must not exist at the commit. Check with `ls -d var` after the suites.
- **Stop conditions are real.** A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. Review against the agent's commit, and check
  `git log` before measuring at `HEAD`.

## 1. Before you dispatch

1. **The tree.** Branch and `HEAD`; `git status --short` shows only §0's untracked set; nothing
   staged; `git log --oneline 7331391..HEAD` is this note's commit only.
2. **Ray.** `ray_cluster.sh status` and `pgrep -lf 'gcs_server|raylet|ray::'`.
3. **Baselines.** The four roots at `HEAD`, serially, with the load. Give the agent the numbers.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Sonnet**. Give it:
- the prompt;
- the campaign README and this board;
- `HEAD` and the baselines;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad.
  The agent does not read `orchestrator/`, and does not read prompts 03–05.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/02-the-fingerprinter.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **The files it may add or change are the prompt's §5 list, and nothing else.**
- **§0's four rulings** bind it, each classified as §0 says, and the command line is a test.
- **It starts no Ray, opens no store but its temporary ones, and creates no `var/`.**
- **Stop and ask** on any of the prompt's §4 conditions. An existing test that fails is a stop,
  never a reason to edit it.
- **It replies with:**
  - the log's path and the message as committed;
  - `SPEC_DIGESTS[1]` as committed (21 pairs);
  - the import probe's output;
  - the golden file's writing lines, its SHA-256 and its `digest`;
  - `TestCommandLine`'s command and the command's whole output;
  - the new tests by name, with counts and wall time;
  - each breakage with the tests it failed and how;
  - the four suites;
  - every deviation with its tag;
  - "State handed to the next prompt", verbatim.

## 3. The review — ten checks

1. **Scope.** `git diff <agent>~1 <agent> --stat` lists the prompt's §5 files only. `git status`
   shows §0's untracked set and nothing new. No `var/`. `main.py` unchanged since `3cd2be4`.
2. **F2.** `runregistrykit.toml` read whole: the comment, the three keys, and the `[stores]` table
   with the one key, nothing else.
3. **F1, by reading.** The docstring says who does what and names the guard, and says only what is
   true of the imports (§0 ruling 2). `FINGERPRINT_FORMAT = 1` with the rule for moving it.
   `SPEC_DIGESTS` has exactly one key, `1`, and 21 pairs in `inventory_specs` order. `fingerprinter`
   is an instance over `config.datastore.factories`. No SecondaryGWKit history came across.
4. **F1, by probe.** From the scratchpad: the orchestrator's own `spec_digests` (the recipe, written
   afresh, not imported from the test) equals `SPEC_DIGESTS[1]`; the distinct-digest count is 15;
   and the `sys.modules` difference is the 9 modules of §0 ruling 2.
5. **F3, by reading.** `plain` and `spec_digests` are the recipe; the failure message names
   changed, added and removed classes and the reordering case, and says to bump and pin; a test
   for a missing pin; the key test with two stand-in `Parent`s, the `TypeError` asserted.
6. **F4, by reading.** The builder is the golden module's own, subclasses `_TempStoreCase`, imports
   nothing from `tools.tests`, and holds the rows §0 ruling 3 asks for. `TestGolden` builds twice,
   checks `problems` is empty, and compares without `taken`. `TestConfigFingerprint` as the prompt.
   `TestCommandLine` as §0. The docstring says how a later format adds a file beside this one.
7. **The golden file.** `shasum -a 256` equals the log's. It is `json.dumps(…, indent=2,
   sort_keys=True) + "\n"` of itself (re-serialise and compare bytes). `taken` is `null`; every
   class of the 21 has `count` ≥ 1; `ScalarModel`'s count is at least 5; `problems` is `{}`.
8. **The breakages.** In `bash`, replay each diff with `git apply`, run the named modules, see each
   fail as the log says, revert with `git apply -R`, `git status` clean.
9. **The records.** The board: row 02 ✅ with "this commit" and the log; F1–F4 ✅; the gate
   "Fingerprint" ✅; `### 5.3 After prompt 02 (<date>)`. §3 and the index untouched, or, if one
   opened, the index row, count and date in the same commit. The log in §5.1's form, every
   deviation classified, the golden's provenance, the breakage diffs.
10. **The suites** at the agent's commit, serially: 18, 61 + 115, 69 + the new tests, 84.
    `black --check` on the three new Python files.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 02* paragraph in §5.3. Fix small
  residue in a follow-up commit of its own:
  - "this commit" → the SHA (board and log);
  - the notes list, with this one marked "used for 02";
  - the status line: 2 of 5 landed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the message as committed;
  - §0's four rulings, each of which reads the prompt otherwise than it was written (21 pairs, not
    17; the import sentence; the builder; the golden's form);
  - the golden's `digest` and the spec pin's size;
  - the four roots' counts.
- **Then 03**, whose note is written after this review.
