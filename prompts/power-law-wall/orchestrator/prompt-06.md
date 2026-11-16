# Orchestrator — prompt 06, drivers and exports

**Written** 2026-11-16 with the prompt, at the user's request: "Please write 06 and its orchestration note." First
written on U10 (`8daf91d`). **Re-planned the same day on R12** (README §6.10), the user's ruling that the potentials are
treated symmetrically: "Yes, re-plan 06 on the symmetric design". Not dispatched. P45–P58 as re-planned stand unless the
user rules otherwise; none needs a ruling first. 06 is dispatched at the user's word.

Read [`../README.md`](../README.md) first: §2, §2.1 (the gate Export as amended), §4, §5, and §6 as ruled (R1–R12,
U1–U9) and as proposed (§6.10, P45–P58). Then read the prompt. This note binds 06 only. The conventions of earlier notes
bind here as well: a tolerance is a number with its reason, timing is taken under load, and the suites run serially.

**You do not write code.** You may:
- run the suites and the scripts the log names;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`06-drivers-and-exports.md`](../06-drivers-and-exports.md) · **Gate:** "Export" (as amended by R12) ·
**Closes:** nothing · **Opens:** only what the work finds · **Model:** Opus (P58; README §2's table said Sonnet) ·
**Network:** none. Nothing is pushed.

**Gate**, as measured by the planner on 2026-11-16 at 10:57 local, before the first version of this note was committed
(`8daf91d`). The re-plan changed only the prompt, this note and README §6.10:
- **The worktree.** `.claude/worktrees/power-law-wall` was on `power-law-wall` at `3ce578d` (review of 05), on top of
  `bdb6ae7` (05). `git status --porcelain` showed `?? venv`, plus the planning files before their commit.
- **The main checkout** is on `main` at `9df9e13`, clean. Nothing of this campaign has touched it.
- **The registry.** `runregistrykit list`: 62 runs, all finished, none `running` or stale, none of this campaign.
- **The rehearsal lock** is free.
- **The code.**
  - `FINGERPRINT_FORMAT = 8` and `VERSION_LABEL = "2026.12.0"`.
  - `config/argument_parser.py` registers no `--model` or `--models`.
  - `inverse_power.yaml` and `CosmologyConcepts/Models/model_options.py` do not exist.
  - `tools/quanta_measures.py:98` defines `SPECTRUM_WALL = qs.WALL_EXPONENTIAL`.
- **Load and disk.** Load averages 26.20 / 16.71 / 18.60 (another process on the machine); 107 GB free.
- **The suites.** Not re-run by the planner. At `bdb6ae7` 05's agent measured 26, 739 (226 + 513), 175, 307, 76, with
  only the two worktree failures of note 01's ruling 7. 05's review counted the same by discovery.

## 0. What the planner found before writing 06

A survey of the tree at `3ce578d`, and one scratch probe (from the worktree root, in-process, nothing written). The probe
script is not kept; its results below are the record.

- **What names one potential today** (production code, outside `.documents/`, `science-runs/` and `prompts/`):
  - `config/argument_parser.py:205`: `drain_strength_A` checks A under `DRAIN_MODEL_ONE_LOOP`; `--n-values`' help names
    V = V0 exp((M/phi)^n) only; `REMOVED_OPTIONS["--potential-type"]` says "there is one model, ExponentialModel".
  - `main.py`:
    - `:990`: `convert_to_model` builds `"ExponentialModel"`;
    - `:1016`: the A rows are looked up under `DRAIN_MODEL_ONE_LOOP`;
    - `:1076`: the grid's line, "using model ExponentialModel, V = V0 exp((M/phi)^n)";
    - `:1159`: the A line names `DRAIN_MODEL_ONE_LOOP`;
    - the loop: `for alpha in alpha_rows: run_pipeline(…, Model_array, …)` over one array of models.
  - `plot_inputs.py:63` (`MODEL_CLASS = "ExponentialModel"`), and `:197` and `:200` (the A rows).
  - `plot_by_beta.py:1173` and `plot_ScalarHistory.py:1646`: the `@@ … A values` lines filter `== DRAIN_MODEL_ONE_LOOP`.
  - `plot_by_beta.py:1022–1024`: the groups are keyed `(model._M.store_id, model._n)`. `M_value` is one table shared by
    both potentials, so an exponential and a power-law model at the same M and n would fall into one group. This is why
    the prompt has one pass per potential (§1.5).
  - `pipeline_selection.py:223` and `:242`: docstrings only.
  - The quanta tools: `SPECTRUM_WALL = qs.WALL_EXPONENTIAL` at `tools/quanta_measures.py:98`,
    `tools/plot_quanta_history.py:76`, `tools/plot_quanta_grid.py:79` and `tools/quanta_figures.py:50`, each commented
    "until power-law-wall prompt 06".
  - `RunDrivers/registered_run.py`'s `describe_scope` names no potential.
  - The other production uses of `DRAIN_MODEL_ONE_LOOP` are the law's own and stay:
    `CosmologyConcepts/drain_model.py`, `ExponentialModel.one_loop_drain_model`, comments in
    `ComputeTargets/ScalarHistory.py`, and `alpha_value`'s docstrings.
- **The export layout today.**
  - `plot_by_beta.py` writes:
    - `<output>/<label>/histories.csv` (`:1084`);
    - `<output>/<label>/plots/M=…eV_n=…/Delta_N_eff.pdf`;
    - each row's `<output>/<label>/<alpha_path>/{plots,csv}/M=…eV_n=…/` (`:181–182`, `:1058–1059`).
  - `plot_ScalarHistory.py` writes `<output>/<label>/<alpha_path>/{plots,csv}/beta=…/M=…eV_n=…/` (`:1394–1399`).
  - `<output>` is `--output` (default `data-out`), and `<label>` is the cosmology's label from `config/model_list.py`
    (`QCD_Cosmology`, the only one). Every root is `Path(args.output).resolve() / f"{model_label}"`, at four sites.
  - `histories.csv`'s `CSV_COLUMNS` begin `beta, M_Mp, n, phi_init_Mp, alpha, drain_model, …` and name no potential.
- **R12**, in short: the first plan (U10) gave the power law a root of its own and left the exponential's exports as they
  were. The user ruled that no potential is privileged. So every potential is exported under `<output>/<label>/<model>/`
  with a `model` column, and one run may build and export both. The exponential's exports therefore move one directory
  down and gain a column, and the gate Export compares them with that accounted for. Its histories and stored rows do
  not move.
- **The 2026.11.0 analyses' readers**, read by `git show main:science-runs/2026-11-13-science-2026.11.0/<path>`:
  - `fig6/fig6.py`, `fig6b/fig6b.py`, `m-trend/m_trend.py` and `changes/changes.py` read a copy of `histories.csv` with
    `pd.read_csv` and select by column name;
  - `m_trend.py` also reads copies of `bounces.csv` at `inputs/bounces/beta=<β>/M=<M>eV_n=1/`;
  - none reads a path of the export itself;
  - the relativistic-quanta analysis is `tools/quanta_measures.py` on an export root, which finds a row's directory by
    `alpha_path` and `history_directory` below the root.

  So an extra column does not disturb them, and the potential's directory is above every path they name.
- **The probe** (`3ce578d`, Planck units, M = M_P, β = 1):
  - `ExponentialModel.one_loop_drain_model` is `one-loop-measured-v1` at n = 1, 2 and 3.
    `InversePowerModel.one_loop_drain_model` is `one-loop-measured-pl-v1` at n = 1 and 2, and raises `ValueError` at
    n = 3. So a potential's drain model depends on its class and n only.
  - Both classes have `_M`, `_n`, `_beta` and `_formatter`, which `extract_common`'s labels read. Their `name`s are
    `ExponentialModel(M=1Mp,n=1,beta=1)` and `InversePowerModel(M=1Mp,n=1,beta=1)`.
  - `DRAIN_MODELS` is (`fixed`, `one-loop-measured-v1`, `one-loop-measured-pl-v1`). Both one-loop models map to
    `DRAIN_LAW_ONE_LOOP`, and `check_drain_model` refuses A = 0 and accepts 10⁻³ and 10⁶ under each.
- **The hooks.** `AbstractModel.one_loop_drain_model` is a property whose base raises `NotImplementedError`; it is not an
  `abstractmethod`. Two test stand-ins subclass `AbstractModel`: `_MinimalModel` at
  `ComputeTargets/tests/test_model_interface.py:107` and `_PowerLawWall` at `test_wall_model.py:107`.
- **The tests that pin what this prompt changes.**
  - `ComputeTargets/tests/test_driver_options.py` test c pins `exponential.yaml`: no `potential-type`, `n_values` [1],
    `shards` 16. Its assertions still hold with `models` added. Test d checks that no driver reads `potential_type`.
  - `RunDrivers/tests/test_registered_run.py` pins a scope string ("config exponential.yaml; β 1.1, …; n 1; …"), as do
    `test_scope_*.py`. Each moves by P57's `models` part.
  - The quanta tools' fixtures (`tools/tests/quanta_exports.py`) write `histories.csv` without `model`, so they move by
    P51.
  - `tools/tests/test_no_plots.py` says the drivers' behaviour needs a store and Ray. That is why the gate's evidence is
    a harness store exported by two trees.
- **The base of the comparison.** The dispatch commit carries 05's schema, so the base tree's plot code can read a mixed
  store. It looks up `ExponentialModel` only, which makes it the reference for the exponential's exports.
- **The cost.** The harness store needs tuned histories. 05's round trips tuned one power-law history per law at
  laboratory M in about 10 s each, so at n = 1 and 2 under both laws, with the exponential's, a few minutes at most.

**Rulings.** Each is recorded in the log, with its class.
1. **The worktree and the main checkout.** As note 01's ruling 1:
   - the agent works only in the worktree, from its root, with `./venv/bin/python` and `PYTHONPATH=.`;
   - it never stages `venv` or `var/`, and never names `/tmp/ray-cpbh`;
   - it never reads or touches the main checkout's files. It reads the 2026.11.0 analyses by `git show main:<path>`
     only;
   - the base worktree of §2.1 is made under the session scratchpad, and removed with `git worktree remove` before the
     commit.

   *STRUCTURALLY REQUIRED.*
2. **The exponential's histories do not move, and its exports move only as R12 says.** Any of these is a stop:
   - a difference between the base's and the tip's exponential exports beyond R12's directory and column, as prompt
     §2.1 states them;
   - a printed line of an exponential-only `main.py` run that differs. Check every changed line against the base's
     string;
   - a quanta tool's output on the exponential root that differs from the base's, timing fields aside;
   - an edit to a golden's expectation.

   *STRUCTURALLY REQUIRED* (README §5 and P2, as amended by R12).
3. **The order of a one-potential run.** `main.py`'s lookups and pipelines for one potential are in today's order. A
   change to that order is a stop: it would move an exponential-only store's serials, which 07 compares bitwise.
   *STRUCTURALLY REQUIRED.*
4. **Existing tests** are changed only where they pin a string, path, column or signature that the prompt changes
   (§1.10). Each change is listed with its old and new value. A change for any other reason is a stop.
   *IMPLEMENTATION CHOICE, ruled here.*
5. **The A rows.** After the change, no driver looks up an A row under a drain model other than the potential's own. The
   log lists every production use of `DRAIN_MODEL_ONE_LOOP` that remains (§1.3), each with why it stays. One in a driver,
   the parser or `plot_inputs.py` that still selects or looks up rows is a stop. *IMPLEMENTATION CHOICE, ruled here.*
6. **The two worktree failures** of note 01's §0.1 are expected in the suites at the base and after. Any other failure is
   a stop. *IMPLEMENTATION CHOICE, ruled here.*
7. **The machine.** The suites run serially, and the load is recorded beside every timing. The load was high when this
   note was written (26 at one minute). Record it; it is not a stop. *IMPLEMENTATION CHOICE, ruled here.*
8. **The verification store** of §2.1 is built through the stand-in cluster, never Ray, under `var/power-law-wall/06/`.
   Its files' SHA-256 are taken before and after every export, and a change is a stop. Nothing under `var/` is
   committed; `export-06/out/` holds the committed outputs. *IMPLEMENTATION CHOICE, ruled here.*
9. **A finding about production** outside the prompt's scope is recorded and reported, not fixed. The agent may open a
   §3 issue for it, with `.documents/OPEN_ISSUES.md` in the same commit. *IMPLEMENTATION CHOICE, ruled here.*
10. **No file but the prompt's §5 list.** *STRUCTURALLY REQUIRED.*

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status` before committing.
  Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit", and the orchestrator substitutes it.
- **Order.**
  1. The Gate, and the five suites on the base: serially, ComputeTargets in its two halves, the `test_[g-z]` half and
     `tools/tests` in the background, waiting for each notification.
  2. The base worktree, the verification store, and the base's exports, before any edit, so that the base is the
     dispatch commit's code.
  3. The option, the config files, the A rows and the parse refusal; then `main.py`; then `plot_inputs.py` and the plot
     scripts.
  4. The export root and the column; `spectrum_wall`; the quanta tools; the scope.
  5. The tests; then the tip's exports, the comparison, the read-back, `quanta_measures` on the three roots, and §2.3's
     table.
  6. The suites after, `CLAUDE.md`'s counts, the documents, the base worktree removed, and the commit.
- **Scratch.** Probes and suite outputs go in the agent's own subdirectory of the session scratchpad. Nothing goes in
  `/tmp`.
- **No Ray.** Nothing starts a cluster, and no `ray` command is run. `main.py` is not run against a store. No store
  but a test's temporary one and §2.1's is opened, and nothing is registered.
- **Stop conditions are real:**
  - a test failing at the base other than the two known;
  - ruling 2, 3, 4 or 5;
  - a production change outside the prompt's §5 files becoming necessary;
  - an import cycle that §1.7's fallback does not resolve.

  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

1. **The Gate above**, on the commit that adds this version of the note, or a later one.
2. **`runregistrykit list`**, the rehearsal lock (`./venv/bin/python -m raypoolkit.rehearsal_lock status`), and the load
   average. A cluster or lock of another session is left alone; 06 needs neither.

## 2. Dispatch

Start one fresh-context subagent, on **Opus**, in the worktree, in the background. Give it:
- the prompt, the README and the board;
- `HEAD`, and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the files of ruling 10. Its log is `logs/06-drivers-and-exports.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **Stop and ask** on any of the conventions' stop conditions. A stop is a reply headed **STOP**, with the work left
  uncommitted.
- **At the end it replies with:**
  - the log's path and the message as committed; `git show --stat` of its commit;
  - the suites at the base and after, with counts, times and `uptime`;
  - the Export gate item by item (prompt §2), each with its command and output;
  - the comparison's result, file by file and path by path, and the store's SHA-256 before and after;
  - the parse refusal's message, and each potential's A rows' drain model;
  - how a one-potential run's order is kept (ruling 3);
  - the remaining `DRAIN_MODEL_ONE_LOOP` uses (ruling 5);
  - §2.3's table;
  - every existing test changed, with its old and new value;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — six checks

1. **Scope.**
   - `git show --stat <agent>` lists only ruling 10's paths. `venv` and `var/` are not in it.
   - `git diff <base> <agent>` shows no change to `ComputeTargets/ScalarHistory.py`, `ObjectFactories/`,
     `config/fingerprint.py`, `config/version.py`, or the tools that §3 of the prompt names.
   - In the model classes the diff is `spectrum_wall` alone.
   - `exponential.yaml`'s diff is the `models` key alone.
   - No base worktree is left (`git worktree list`).
   - The commit's parent is this note's commit, or a later one that is not the agent's.
   - The main checkout is untouched. Nothing is pushed.
2. **The exponential.**
   - Re-run `export_tree.py` from a base worktree of my own and from the tip, on the agent's store, and run the agent's
     `compare_exports.py` and a `cmp` of my own on every file the base wrote, against `<label>/exponential/` (with
     `histories.csv`'s `model` column removed).
   - Run, serially and timed, `test_golden_history`, `test_golden_reflecting_history`, `test_tuning`,
     `test_driver_options`, the quanta tools' tests and `RunDrivers/tests`.
   - Read `main.py`'s diff. Every printed line of an exponential-only run is the base's string, and one potential's
     lookups and pipelines are in the base's order (ruling 3). Confirm the order with the agent's P48 test.
3. **The option and the rows.** Parse, myself:
   - `--models inverse-power --n-values 1 2 --A-values 1`: accepted, with rows under `one-loop-measured-pl-v1`;
   - `--models exponential inverse-power --A-values 1`: each potential under its own drain model;
   - `--models inverse-power --n-values 3 --A-values 1` and `--n-values 1 3`: exit 2, the message naming the options,
     the potential and the n;
   - `--models inverse-power --n-values 3 --alpha-values 0.1`: accepted;
   - a command without `--models`: as today;
   - `exponential.yaml` and `inverse_power.yaml`.

   Then check that `git grep DRAIN_MODEL_ONE_LOOP` matches the log's list.
4. **The power law's exports.**
   - Read the tip's `<label>/inverse-power/` root: the path, the `model` column and the rows.
   - Run `tools/quanta_measures.py` on it myself, and check each row's `spectrum_model` against its n.
   - In a scratch probe of my own (never committed), run `plot_inputs` on the agent's store with `--models
     inverse-power`, with the exponential, and with both; and on an exponential-only store with `--models
     inverse-power`, which must be refused naming `--models`.
5. **The analyses' readers.** Spot-check two rows of §2.3's table against `git show main:` of each reader.
6. **The records.**
   - The log is in §5.1's form, every deviation classed, every changed test listed with its old and new value.
   - The board has row 06, its items, the gate Export and the baselines.
   - Verification §5 is added, and §1–§4 are unchanged.
   - `CLAUDE.md` changes only in the counts and their date, and test discovery matches them.
   - `black --check` passes on every changed Python file.
   - Any issue opened is on §3 and in `.documents/OPEN_ISSUES.md`.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 06 landed, the gate Export held, and this note marked "used for 06";
- an *Orchestrator review of prompt 06* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s campaign row.

**Report to the user:**
- the agent's commit and the review commit;
- the gate Export, and the comparison;
- the option, each potential's drain model and the parse refusal;
- the export layout as built, and what the 2026.11.0 analyses need from it;
- every existing test changed;
- any observation that bears on 07 (the rehearsal) or on the science run;
- that 07 is written only when the user asks for it.
