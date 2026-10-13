# Prompt 01 — require the Ray address

**Campaign:** [`README.md`](README.md) · **Board items:** **A1**–**A4** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-13, after the user's rulings (README §6: U1, U2 as proposed). Measured at
`bacca64`; your own measurement governs.
**Gate:**
- on branch `read-only-plots`, no tracked change in `git status` (the untracked entries are the user's:
  the 17 `pilot-2026.6.0*.db` files, `pilot-main.log`, `pilot-plot.log`, `pilot-out/`, the two
  `full_run_2026.6.0*.sh` scripts, `prompts/jordan-normalization/`; none is yours);
- no ChamPBH Ray process is running (`.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh
  status`). You start none.

**Closes:** nothing. **Opens:** only what the work finds.
**Recommended model:** **Sonnet**. One parser change, one check called in three places, tests.

**Read first:**

1. [`README.md`](README.md): §0.2, §0.3 ("`--ray-address` today"), §2.2 (01's charter), §5, §6.1 (U1,
   U2).
2. `CLAUDE.md`: "Repository mechanics", in particular "Ray on this machine" and the test commands.
3. `config/argument_parser.py` (`DEFAULT_RAY_ADDRESS` at `:11`, the switch at `:310-315`); the
   drivers' start-up: `main.py:82-124`, `plot_by_beta.py:84-101`, `plot_ScalarModel.py:70-81`.
4. StochasticInstantons' `d6d550b` (`git -C /Users/ds283/Documents/Code/StochasticInstantons show
   d6d550b`; read, never edit): the same change there, which removed the default and set
   `required=True`. This prompt differs from it on U1 and U2 (§1).
5. `tools/tests/test_inventory_report.py` (`NO_RAY` at `:66`, `_main` at `:395-400`): how a test runs
   `main.py` in a subprocess with no Ray.

---

## 1. What is wanted

Every driver run that starts Ray names its cluster. `auto` is refused, because with the projects'
clusters isolated it attaches to whichever Ray head it finds first (README §0.2). Two rulings shape
it:
- **U1.** The address is required **whenever Ray starts**. `main.py --inventory` reads a closed store
  and exits before `ray.init` (`main.py:93-111`), so it is exempt. `configargparse`'s
  `required=True` cannot express that, so the parser keeps the switch optional, with no default,
  and a check refuses a missing value just before `ray.init`.
- **U2.** `auto` is refused by name, in the same check.

### 1.1 The items

- **A1 — the parser and the check.**
  - In `config/argument_parser.py`, delete `DEFAULT_RAY_ADDRESS` (no other module reads it: check by
    `git grep`). Give `--ray-address` the default `None`, and a help text that says it is required
    whenever Ray starts, names `127.0.0.1:6380` and `local`, and says `auto` is refused.
  - Add a pure function beside the parser, `ray_address_problem(address) -> Optional[str]`. It returns
    `None` for an address it accepts, and otherwise the message:
    ```
    !! --ray-address is required whenever Ray starts. Give the address of a running cluster (ChamPBH's on this machine is 127.0.0.1:6380, started by .documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh), or 'local' for a private instance. 'auto' is refused: with the projects' clusters isolated, it attaches to whichever cluster it finds first.
    ```
    The wording may be tightened, but the message must name both accepted forms and say why `auto`
    is refused.
  - It refuses `None`, the empty string and `auto` (case-insensitive, surrounding whitespace
    ignored). It accepts anything else, unchanged: `local`, `host:port`, `ray://…`. Ray checks the
    address itself.
- **A2 — the three drivers.** Each calls the check immediately before its `ray.init`. On a problem it
  prints the message to stderr and exits **2**, having started no Ray and opened no pool.
  - **`main.py`:** after the `--inventory` branch and the `--drop` check, so `--inventory` never
    reaches it (U1).
  - **The plot scripts:** after the provenance line. They have no branch that avoids Ray.

  The comment above each `ray.init`, which says the address "defaults to 'auto'", is corrected.
  Nothing else in the drivers changes. `ObjectFactories/tests/test_package_provenance.py:317-345`
  still holds: provenance is printed before `ray.init`.
- **A3 — the tests.**
  - **Three existing tests** parse with `--database unused.db` alone (README §0.3):
    `ComputeTargets/tests/test_extraction.py:331-336`, `test_initial_field_option.py:115-118` and
    `test_pipeline_selection.py:157-162`. Under A1 they still parse, because the parser does not
    require the switch, so **they need no change**. Confirm it by running them. If one does need a
    change, it is the only one allowed (README §5), and you name it.
  - **A new module, `ComputeTargets/tests/test_ray_address.py`**, with no Ray:
    1. `ray_address_problem`: `None`, `""`, `"auto"`, `" AUTO "` refused with the message;
       `"127.0.0.1:6380"`, `"local"` and `"ray://127.0.0.1:10001"` accepted.
    2. The parser: no default (`None`); a value given on the command line is kept; and a YAML file
       given with `--config` that sets `ray-address: 127.0.0.1:6380` supplies it (write the file
       in a `tempfile` directory).
    3. **By subprocess, from the repository root**, as `test_inventory_report._main` does.
       - `plot_by_beta.py` and `plot_ScalarModel.py`, each given `--database <tempdir>/absent.db`:
         - with no `--ray-address`: exit 2, the message on stderr, and no file created in the
           temp directory;
         - with `--ray-address auto`: the same.
       - `main.py`, given the same `--database`:
         - with no `--ray-address`: exit 2 with the message;
         - with `--inventory` and no `--ray-address`: exit **1** with the reader's
           `!! --inventory:` refusal of the absent store, not the address message. This is U1's
           exemption.

       Each subprocess stops before Ray. Say in the module docstring how long they take: a plot
       script imports its plotting stack first.
    4. **By `ast`**, for each of the three drivers: the check's call comes before the `ray.init`
       call, and is followed by `sys.exit(2)` on a problem. In `main.py` it also comes after the
       `if args.inventory:` block.
- **A4 — the prose this commit makes false.**
  - `CLAUDE.md`, "Ray on this machine": add that the drivers refuse to start Ray without
    `--ray-address`, and refuse `auto`, and that `main.py --inventory` needs none.
  - `.documents/architecture-summary.md` §5.1 lists "`--ray-address STR` (default 'auto')". Correct
    the line. That section describes the code, and is not a dated record.

  Nothing else.

---

## 2. Verification

1. **Before any edit:** the four roots at README §0.3's counts (18 / 114 (61 + 53) / 63 / 34),
   serially, in the foreground, with the load (`uptime`).
2. `git grep -n "ray_address\|ray-address\|DEFAULT_RAY_ADDRESS" -- ':!prompts'` before and after;
   classify every line in the log.
3. **After:**
   - the four roots: ComputeTargets 114 + the new tests, the others unchanged;
   - `black --check` on every touched Python file;
   - the three drivers byte-compile.
4. **The rehearsal scripts still pass the switch**: `build.sh:99` and `run_readers.sh:111-122`.
   Read them; run nothing.
5. **Deliberate breakages** (README §5, rule 8: each recorded as a diff, never committed):
   - the default restored to `"auto"` in the parser;
   - `main.py`'s check moved after `ray.init`;
   - `auto` accepted by `ray_address_problem`;
   - the check removed from `plot_ScalarModel.py`.

   Each must fail at least one new test, named in the log.

## 3. Acceptance

1. A1–A4 as §1.1 says.
2. §2's checks, each with its command and output in the log.
3. Suites: CosmologyModels 18, ComputeTargets 114 + the new tests, ObjectFactories 63, tools 34.
4. **One commit**, with the log `logs/01-require-the-ray-address.md` in the template of
   `dk-and-rpk-migration` README §5.1. The board: row 01, A1–A4, the gate "Ray address", and a
   `### 5.2 After prompt 01 (<date>)`. Write "this commit" where the SHA goes.

## 4. Stop conditions: stop and ask

- An existing test fails for a reason other than the address.
- A driver cannot call the check before `ray.init` without moving other start-up code.
- A subprocess test would start Ray, or would need a running cluster.
- Anything would need an edit outside §5's list.

## 5. What this prompt does not do

- **Files it changes:**
  - `config/argument_parser.py`;
  - `main.py`, `plot_by_beta.py`, `plot_ScalarModel.py` (the check and the comment only);
  - the new test module;
  - `CLAUDE.md` ("Ray on this machine");
  - `.documents/architecture-summary.md` (§5.1's line);
  - this board, and the log.
- It runs no driver past its check, starts no Ray, and opens no store.
- It does not touch the user's run scripts, or `dk-and-rpk-migration`'s rehearsal scripts.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only, after showing
`git diff --cached --name-only`.
