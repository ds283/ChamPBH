# Prompt 02 — read-only plot pools

**Campaign:** [`README.md`](README.md) · **Board items:** **O1**–**O5** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-13, after the user's rulings (README §6: U3, U4, U6 and P1–P4, P6, P7 as
proposed). Measured at `bacca64`. If 01 has landed first, its lines in the drivers have moved;
your own measurement governs.
**Gate:**
- on branch `read-only-plots`, no tracked change in `git status` (the untracked entries are the
  user's; see prompt 01's gate);
- no ChamPBH Ray process is running. You start none.

**Closes:** `[08-plot-scripts-open-read-write-pools]` (on the `dk-and-rpk-migration` board §3,
assigned here; index §1.12). **Opens:** only what the work finds.
**Recommended model:** **Opus**. A module carved out of two drivers that cannot be imported, three
kinds of miss handled differently, and tests on read-only stand-in stores.

**Read first:**

1. [`README.md`](README.md): §0.1, **§0.3 in full** (the table of lookups is this prompt's map),
   §2.2 (02's charter), §5, §6.1 (U3, U4, U6) and §6.2 (P1–P4, P6, P7).
2. The issue: `prompts/dk-and-rpk-migration/IMPLEMENTATION_STATE.md` §3,
   `[08-plot-scripts-open-read-write-pools]`, with its **Assigned** line.
3. The setup the two scripts share:
   - `plot_by_beta.py:1011-1151`: the pool, rows 1–9 and 17 of §0.3, `convert_to_potential`,
     `convert_to_coupling`, the baseline, and the per-model `T_stop`;
   - `plot_ScalarModel.py:1581-1709`: the same, without the baseline;
   - `config/model_list.py`.
4. DatastoreKit **at the tag** (`git -C /Users/ds283/Documents/Code/DatastoreKit show v0.3.0:<path>`;
   read, never edit):
   - `docs/client-contract.md` §1, row 14 (`read_only`);
   - `datastorekit/replication.py:193-260` (`ReadOnlyMiss`, `ReadOnlyWrite`);
   - `datastorekit/SQL/Datastore.py:296-312` (the read-only inserter);
   - **`datastorekit/tests/test_read_only_pool.py`**, the worked example of a read-only pool on
     the stand-in cluster.
5. `StoreFixtures/standin_pool.py`: `open_pool` (`:383-414`) passes keyword arguments such as
   `read_only=` and `read_table_config=` on to the pool, and the cluster patches `ray.get`
   (`:326`). The harness is `ObjectFactories/tests/test_version_keyed_lookups.py`, `_TempStoreCase`.
6. `pipeline_selection.py` and `extract_common.py`: modules that exist so that a test can import what
   a driver does (P1's precedent).
7. The text tests of README §0.3: `ComputeTargets/tests/test_extraction.py:339-368`,
   `test_kinematic_cap_loop.py:413-424`, `test_initial_field_option.py:79-105`, and
   `ObjectFactories/tests/test_datastore_config.py:170-193`, `:253-293`.

---

## 1. What is wanted

Reading a store for figures must never change it. Both plot scripts open their pools read-only. A
lookup that would have inserted a row is handled by kind (README §0.3, U3, U4):
- a **parameter row** (§0.3 rows 1–5, 8, 9) that is not stored means the plot run was given options
  the build did not use: **stop**;
- a **potential or coupling** (rows 6, 7) that is not stored has no history: **skip it**, with a
  warning.

The history lookups (rows 10–16) are unchanged; they never insert. To make this testable, the setup
the two scripts share moves into one importable module (P1).

### 1.1 The items

- **O1 — `plot_inputs.py`** (P1), at the top level beside `pipeline_selection.py`. It does no
  argument parsing and starts no Ray at import, and calls `ray.get` as the scripts do. It holds the
  shared setup as functions of `pool`, the option values and `units`, each named for what it
  returns:
  - **the parameters**: the two tolerances, `T_init`, `phi_init` and `pi_init`, from `--abs-tol`,
    `--rel-tol`, `--T-init-GeV` and `--phi-init-Mp` (rows 1–5);
  - **the stored grid**: β, M and Λ by `read_table` (row 17);
  - **the potentials** over every stored M × Λ, for `--potential-type` (row 6), and **the couplings**
    for every stored β (row 7). Each is looked up **one combination at a time** (P2): submit every
    lookup, then `ray.get` each reference on its own, so that one miss cannot hide the others;
  - **the models**, through `config.model_list.build_model_list` (row 8);
  - **`T_stop`** for a model: `--T-stop-GeV` if given, else the cosmology's `T_CMB` (row 9).

  Pass option values, not `args`, so a test needs no parser. Keep each lookup's payload exactly as
  the scripts build it today: the same classes, keys and values, so that a store `main.py` built
  still matches.
- **O2 — what a miss does.**
  - **A parameter row** (rows 1–5, 8, 9): `ReadOnlyMiss` becomes a ChamPBH exception of
    `plot_inputs.py`'s own, carrying a message that names the option and the value and says the
    store's build was not run with it. For example:
    ```
    !! plot: the store holds no temperature for --T-stop-GeV 1e-07; main.py was not run with it, so no history in this store can match. Give the plot the options the build used
    ```
    For `T_CMB` and the cosmology, say which constant. The script prints it to stderr and exits
    **2**, before any figure (U4). The `with ShardedPool` block closes the pool on the way out.
  - **A potential or coupling** (rows 6, 7): `ReadOnlyMiss` for a potential, and `ReadOnlyWrite` for a
    coupling (a sharded class, README §0.3), are caught **for that lookup only**. The combination
    is skipped, and one warning line names it:
    ```
    !! plot: no ExponentialPotential stored for M=…, Λ=…; no history can use it; skipped
    ```
    One count line follows each kind. Every other combination is returned (U3). Any other
    exception propagates unchanged.
  - **Under real Ray**, a task's exception reaches `ray.get` as `RayTaskError(...)
    .as_instanceof_cause()`, an instance of the cause's class (`ray/_private/worker.py:1015`,
    `:2979`), so `except ReadOnlyMiss` catches it there as it does in the stand-in. Read those lines
    at Ray 2.53.0 and say so in the log. 03 shows it under real Ray.
- **O3 — the two scripts.**
  - Each calls `plot_inputs.py` in place of its copy of the shared setup. The baseline stays in
    `plot_by_beta.py`. Everything after the setup is unchanged: the history lookups, `run_pipeline`,
    the figures, `histories.csv`.
  - Each `ShardedPool(...)` gains `read_only=True`, and keeps `prune_unvalidated=False` and every
    other argument it has. No `drop_tables` and no `shards=` (P4).
  - **`--drop` is refused** before Ray starts, exit 2, with a message saying a plot run drops
    nothing and `--drop` is `main.py`'s (P3). `--prune-unvalidated` is not refused, since it cannot
    be told from its default. Say in the scripts' own help (their extra arguments, or the module
    docstring) that it is `main.py`'s.
  - **`plot_by_beta.py`'s `job_name="plot_ScalarModel"`** becomes `"plot_by_beta"`. It is
    cosmetic: `dk-and-rpk-migration` log 08, Observation 5, recorded it for housekeeping, and this
    prompt edits the same call. Nothing reads `job_name` (contract §1, row 10).
- **O4 — the tests**, a new `ComputeTargets/tests/test_plot_inputs.py`, on temporary stores built
  through the stand-in cluster (`StoreFixtures/standin_pool.py`; the harness as you need it), with no
  Ray. Build each store read-write with the lookups `main.py` makes, then close it and reopen it
  `read_only=True` (with `read_table_config=`), as `test_read_only_pool.py` does at the tag.
  1. **A store `main.py`'s lookups would build**, for two β, two M and one Λ, one potential type: every
     function returns what the store holds. The potentials and couplings are the stored ones, in a
     stable order, and no warning is printed.
  2. **An incomplete M × Λ product**: two runs' grids, so that two combinations were never stored.
     Those two are skipped with their warnings and the count; the rest are returned (U3).
  3. **A β without a coupling**: it is skipped with its warning (U3).
  4. **A parameter not stored**: a `--T-stop-GeV`, and separately a `--rel-tol`, the build did not
     use. Each raises the module's exception, with the message naming the option and value (U4).
  5. **Nothing written**: the SHA-256 of every file of the store (primary and shards) is unchanged
     after tests 1–4's calls, and no `-journal`, `-wal` or `-shm` file appears.
  6. **A missing store**: the read-only open is refused, and nothing is created at the path.
  7. **By `ast`**:
     - each plot script's `ShardedPool(` has `read_only=True` and no `drop_tables`;
     - each calls `plot_inputs` and no longer defines `convert_to_potential` or
       `convert_to_coupling`;
     - each refuses `--drop` before `ray.init`.

  Say in the docstring that no test runs a PRyMordial solve.
- **O5 — the issue and the text tests.**
  - `[08-plot-scripts-open-read-write-pools]` moves to the `dk-and-rpk-migration` board's §4 with a
    **Resolved (date):** line naming this prompt, the commit and what was found:
    - the parameter rows are found when the options match;
    - a missing potential or coupling is skipped;
    - a missing parameter stops the run.

    Its index row leaves `.documents/OPEN_ISSUES.md` §1.12, and the section says it emptied. The
    count goes 27 → 26, with the header's prose and date. This board's §3.1 says it closed.
  - **The text tests** (P6). `test_datastore_config.py:170-193`'s "no `object_get_vectorized` in
    the plot scripts" also covers `plot_inputs.py`. `:253-293`'s pool checks still hold. Each other
    text test of §0.3 must still pass unchanged. If one cannot, because 02 moved the text it
    reads, update that test only to read the text where it now is, and name it in the log with
    the reason. `test_extraction`'s four `_do_not_populate` literals stay in `plot_by_beta.py`,
    because the history lookups do not move.

---

## 2. Verification

1. **Before any edit:** the four roots (18 / 114 (61 + 53) / 63 / 34, or 01's counts if 01 has
   landed), serially, in the foreground, with the load.
2. **The payloads are unchanged.** For every lookup that moves, the log gives the old payload
   (file:line) and the new one, side by side, and they are equal key for key.
3. **P2's cost.** On test 1's store, and on one with 20 β × 5 M × 4 Λ, time the potential and coupling
   lookups one at a time against one vectorized call (stand-in, so relative only), and report it.
4. **After:** the four roots, with ComputeTargets' count rising by O4's tests and none falling.
   `black --check` on every touched Python file; the two scripts and `plot_inputs.py` byte-compile.
5. **Deliberate breakages** (each a diff in the log, never committed):
   - `read_only=True` removed from `plot_inputs`' test pools (test 5 fails);
   - the per-combination lookup reverted to one call (test 2 fails);
   - the parameter `ReadOnlyMiss` swallowed (test 4 fails);
   - `read_only=True` removed from one script's `ShardedPool(` (test 7 fails).
6. **The index**: `.documents/OPEN_ISSUES.md`'s rows counted (26), and §1.12 agreeing with the
   boards.

## 3. Acceptance

1. O1–O5 as §1.1 says, and §2's checks in the log with commands and outputs.
2. No plotted or stored value changes. §2.2 is the evidence here, and 03 checks `histories.csv`
   under real Ray.
3. **One commit**, with the log `logs/02-read-only-plot-pools.md` in the template of
   `dk-and-rpk-migration` README §5.1. The board: row 02, O1–O5, the gate "Read-only plots", §3.1
   and a `### 5.3 After prompt 02 (<date>)`. The `dk-and-rpk-migration` board's §3 and §4.
   `.documents/OPEN_ISSUES.md`. Write "this commit" where the SHA goes.

## 4. Stop conditions: stop and ask

- The stand-in cluster cannot open a read-only pool on a store it built. That is a fixture gap, to
  be recorded, not worked around.
- A package behaviour differs from the contract at the tag (a read-only miss that inserts; an
  exception of another class). That is a package defect (README §5).
- A payload cannot be kept exactly as the scripts build it today.
- A lookup of rows 10–16 would need to change.
- A text test needs a change other than following moved text.
- Any suite count falls.

## 5. What this prompt does not do

- **Files it changes:**
  - `plot_inputs.py` (new);
  - `plot_by_beta.py`, `plot_ScalarModel.py`;
  - the new test module, and the text tests of O5, only as O5 allows;
  - `prompts/dk-and-rpk-migration/IMPLEMENTATION_STATE.md` (the issue's move only);
  - `.documents/OPEN_ISSUES.md`;
  - this board, and the log.
- It does not change `main.py`'s pool, any factory, `config/`, or the parser (01's).
- It runs no plot script, starts no Ray, and opens no store outside a `tempfile` directory.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`.
