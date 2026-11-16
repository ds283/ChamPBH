# Orchestrator — prompt 06, drivers and exports

**Written** 2026-11-16 with the prompt, at the user's request: "Please write 06 and its orchestration note." Not
dispatched. **U10 (README §6.10) is the user's to rule first.** P45–P58 are proposed with it and stand unless the user
rules otherwise. 06 is dispatched at the user's word, after U10 is ruled and recorded.

Read [`../README.md`](../README.md) first: §2, §4, §5, and §6 as ruled (R1–R11, U1–U9) and as proposed (§6.9 as
landed, §6.10 P45–P58 and U10). Then read the prompt. This note binds 06 only. The conventions of earlier notes bind
here as well: a tolerance is a number with its reason, timing is taken under load, and the suites run serially.

**You do not write code.** You may:
- run the suites and the scripts the log names;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`06-drivers-and-exports.md`](../06-drivers-and-exports.md) · **Gate:** "Export" · **Closes:** nothing ·
**Opens:** only what the work finds · **Model:** Opus (P58; README §2's table said Sonnet) · **Network:** none. Nothing
is pushed.

**Gate**, as measured by the planner on 2026-11-16 at 10:57 local, before this note and the prompt were committed:
- **The worktree.** `.claude/worktrees/power-law-wall` is on `power-law-wall` at `3ce578d` (review of 05), on top of
  `bdb6ae7` (05). `git status --porcelain` shows `?? venv`, plus the prompt, this note and README §6.10 before their
  commit.
- **The main checkout** is on `main` at `9df9e13`, clean. Nothing of this campaign has touched it.
- **The registry.** `runregistrykit list`: 62 runs, all finished, none `running` or stale, none of this campaign.
- **The rehearsal lock** is free.
- **The code.** `FINGERPRINT_FORMAT = 8`; `VERSION_LABEL = "2026.12.0"`; `config/argument_parser.py` registers no
  `--model`; `inverse_power.yaml` and `CosmologyConcepts/Models/model_options.py` do not exist;
  `tools/quanta_measures.py:98` defines `SPECTRUM_WALL = qs.WALL_EXPONENTIAL`.
- **Load and disk.** Load averages 26.20 / 16.71 / 18.60 (another process on the machine); 107 GB free.
- **The suites.** Not re-run by the planner. At `bdb6ae7` 05's agent measured 26, 739 (226 + 513), 175, 307, 76, with
  only the two worktree failures of note 01's ruling 7, and 05's review counted the same by discovery.

## 0. What the planner found before writing 06

A survey of the tree at `3ce578d`, and one scratch probe (from the worktree root, in-process, nothing written). The probe
script is not kept; its results below are the record.

- **What names one model today** (production code, outside `.documents/`, `science-runs/` and `prompts/`):
  - `config/argument_parser.py:205`: `drain_strength_A` checks A under `DRAIN_MODEL_ONE_LOOP`; `--n-values`' help names
    V = V0 exp((M/phi)^n) only; `REMOVED_OPTIONS["--potential-type"]` says "there is one model, ExponentialModel".
  - `main.py:990` (`convert_to_model` builds `"ExponentialModel"`), `:1016` (the A rows under `DRAIN_MODEL_ONE_LOOP`),
    `:1076` (the grid's line, "using model ExponentialModel, V = V0 exp((M/phi)^n)"), `:1159` (the A line names
    `DRAIN_MODEL_ONE_LOOP`).
  - `plot_inputs.py:63` (`MODEL_CLASS = "ExponentialModel"`), `:197` and `:200` (the A rows).
  - `plot_by_beta.py:1173` and `plot_ScalarHistory.py:1646`: the `@@ … A values` lines filter `== DRAIN_MODEL_ONE_LOOP`.
  - `pipeline_selection.py:223` and `:242`: docstrings only.
  - The quanta tools: `SPECTRUM_WALL = qs.WALL_EXPONENTIAL` at `tools/quanta_measures.py:98`,
    `tools/plot_quanta_history.py:76`, `tools/plot_quanta_grid.py:79` and `tools/quanta_figures.py:50`, each commented
    "until power-law-wall prompt 06".
  - `RunDrivers/registered_run.py` `describe_scope` names no model.
  - The rest of `DRAIN_MODEL_ONE_LOOP`'s production uses are the law's own (`CosmologyConcepts/drain_model.py`,
    `ExponentialModel.one_loop_drain_model`, comments in `ComputeTargets/ScalarHistory.py`, `alpha_value`'s docstrings)
    and stay.
- **The export layout today.**
  - `plot_by_beta.py`: `<output>/<label>/histories.csv` (`:1084`), `<output>/<label>/plots/M=…eV_n=…/Delta_N_eff.pdf`,
    and each row's `<output>/<label>/<alpha_path>/{plots,csv}/M=…eV_n=…/` (`:181–182`, `:1058–1059`).
  - `plot_ScalarHistory.py`: `<output>/<label>/<alpha_path>/{plots,csv}/beta=…/M=…eV_n=…/` (`:1394–1399`).
  - `<label>` is the cosmology's (`QCD_Cosmology`). Every root is `Path(args.output).resolve() / f"{model_label}"`, at
    four sites.
  - `histories.csv`'s `CSV_COLUMNS` begin `beta, M_Mp, n, phi_init_Mp, alpha, drain_model, …` and have no model.
- **Why U10.** P11 (2026-11-13) says `histories.csv` "gains a `model` column" and that "the exponential's paths and
  bytes are unchanged"; the gate Export and README §5 require the exponential's exports byte-identical. A column added to
  every `histories.csv` changes the exponential's bytes. A column added to the power law's file only, with its folders
  under the per-(M, n) directories as P11 places them, leaves both models' `histories.csv` at `<output>/<label>/`, where
  one plot run overwrites the other's. (a) moves the model to the root, which needs one helper at the four sites and
  nothing below them; (b) is P11 word for word, with the gate amended.
- **The 2026.11.0 analyses' readers**, read by `git show main:science-runs/2026-11-13-science-2026.11.0/<path>`:
  `fig6/fig6.py`, `fig6b/fig6b.py`, `m-trend/m_trend.py` and `changes/changes.py` read a copy of `histories.csv` with
  `pd.read_csv` and select by column name; `m_trend.py` also reads copies of `bounces.csv` at
  `inputs/bounces/beta=<β>/M=<M>eV_n=1/`. None reads a path of the export itself. The relativistic-quanta analysis is
  `tools/quanta_measures.py` on an export root, which finds a row's directory by `alpha_path` and `history_directory`
  below the root.
- **The probe** (`3ce578d`, Planck units, M = M_P, β = 1):
  - `ExponentialModel.one_loop_drain_model` is `one-loop-measured-v1` at n = 1, 2 and 3.
    `InversePowerModel.one_loop_drain_model` is `one-loop-measured-pl-v1` at n = 1 and 2, and raises `ValueError` at
    n = 3. So the drain model depends on the class and n only, and P47's helper can read it at any M and β.
  - Both classes have `_M`, `_n`, `_beta` and `_formatter`, which `extract_common`'s labels read. Their `name`s are
    `ExponentialModel(M=1Mp,n=1,beta=1)` and `InversePowerModel(M=1Mp,n=1,beta=1)`.
  - `DRAIN_MODELS` is (`fixed`, `one-loop-measured-v1`, `one-loop-measured-pl-v1`). Both one-loop models map to
    `DRAIN_LAW_ONE_LOOP`, and `check_drain_model` refuses A = 0 and accepts 10⁻³ and 10⁶ under each. So
    `drain_strength_A` can check the law's rule rather than one model's, and accept and refuse the same values.
- **The hooks.** `AbstractModel.one_loop_drain_model` is a property whose base raises `NotImplementedError`. It is not an
  `abstractmethod`, so that a model without it still constructs. Two test stand-ins subclass `AbstractModel`
  (`ComputeTargets/tests/test_model_interface.py:107` `_MinimalModel`, `test_wall_model.py:107` `_PowerLawWall`). An
  abstract `spectrum_wall` would break them, which is why the prompt §1.7 declares it as `one_loop_drain_model` is.
- **The tests that pin today's strings.**
  - `ComputeTargets/tests/test_driver_options.py`: test c pins `exponential.yaml` (no `potential-type`, `n_values` [1],
    `shards` 16); test d checks that no driver reads `potential_type`.
  - `tools/tests/test_no_plots.py` says the drivers' behaviour needs a store and Ray. That is why the gate's evidence is
    a harness store exported by two trees, as `.documents/one-loop-drain/export-04/` made it.
  - `RunDrivers/tests/test_scope_*.py` pin scope strings.
- **The base of the comparison.** The dispatch commit carries 05's schema, so the base tree's plot code can read a mixed
  store. It looks up `ExponentialModel` only, which makes it the reference for the exponential's exports.
- **The cost.** The harness store needs tuned histories. 05's round trips tuned one power-law history per law at
  laboratory M in about 10 s each. At n = 1 and 2 under both laws, plus the exponential's, that is a few minutes at most.

**Rulings.** Each is recorded in the log, with its class.
1. **The worktree and the main checkout.** As note 01's ruling 1: the agent works only in the worktree, from its root,
   with `./venv/bin/python` and `PYTHONPATH=.`. It never stages `venv` or `var/`, never reads or touches the main
   checkout's files (the 2026.11.0 analyses are read by `git show main:<path>` only), and never names `/tmp/ray-cpbh`.
   The base worktree of §2.1 is made under the session scratchpad and removed with `git worktree remove` before the
   commit. *STRUCTURALLY REQUIRED.*
2. **The exponential does not move.** Any of these is a stop:
   - a difference between the base's and the tip's exponential exports, in any byte or path;
   - a printed line of an exponential `main.py` or plot run that differs. Check every changed line against the base's
     string;
   - a quanta tool's output on the exponential root that differs, timing fields aside;
   - an edit to a golden's expectation.

   *STRUCTURALLY REQUIRED* (README §5, P2).
3. **U10 as ruled.** The prompt is written on (a). If the user rules (b), the orchestrator gives the agent these changes
   at dispatch:
   - §1.6 becomes: `histories.csv` gains `model` directly after `n` for every model (`exponential` in the exponential's
     rows), and the power law's per-(M, n) folders are `<model>/M=…eV_n=…` under `plots/` and `csv/`, and
     `beta=…/<model>/M=…eV_n=…` under `plot_ScalarHistory.py`'s, through `history_directory` and the group paths. The
     root is unchanged, and `export_root` is not added;
   - §2.1 compares the exponential's exports byte for byte except `histories.csv`, which is compared on every column but
     `model`, with `model` equal to `exponential` in every row;
   - §1.10's export tests and §2.3's table follow.

   *IMPLEMENTATION CHOICE, ruled by the user.*
4. **Existing tests** are changed only where they pin a string or signature that the prompt changes (§1.10). Each change
   is listed with its old and new value. A change for any other reason is a stop. *IMPLEMENTATION CHOICE, ruled here.*
5. **The A rows.** After the change, no driver looks up an A row under a drain model other than the model's own. The log
   lists every production use of `DRAIN_MODEL_ONE_LOOP` that remains (§1.3), each with why it stays. One in a driver, the
   parser or `plot_inputs.py` that still selects or looks up rows is a stop. *IMPLEMENTATION CHOICE, ruled here.*
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
  2. The base worktree and the verification store, and the base's exports (before any edit, so that the base is the
     dispatch commit's code).
  3. The option, the config file, the A rows and the parse refusal; `main.py`; `plot_inputs.py` and the plot scripts.
  4. The export root and the column; `spectrum_wall`; the quanta tools; the scope.
  5. The tests; then the tip's exports, the comparison, the read-back and `quanta_measures` on both roots; §2.3's table.
  6. The suites after, `CLAUDE.md`'s counts, the documents, the base worktree removed, and the commit.
- **Scratch.** Probes and suite outputs go in the agent's own subdirectory of the session scratchpad. Nothing goes in
  `/tmp`.
- **No Ray.** Nothing starts a cluster, and no `ray` command is run. `main.py` is not run against a store. No store
  but a test's temporary one and §2.1's is opened, and nothing is registered.
- **Stop conditions are real:**
  - a test failing at the base other than the two known;
  - ruling 2, 4 or 5;
  - a production change outside the prompt's §5 files becoming necessary;
  - an import cycle that §1.7's fallback does not resolve.

  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

1. **U10 is ruled** and recorded in README §6.10 (the user's words), in a commit before dispatch. Set ruling 3 to what
   was ruled.
2. **The Gate above**, on that commit or a later one.
3. **`runregistrykit list`**, the rehearsal lock (`./venv/bin/python -m raypoolkit.rehearsal_lock status`), and the load
   average. A cluster or lock of another session is left alone; 06 needs neither.

## 2. Dispatch

Start one fresh-context subagent, on **Opus**, in the worktree, in the background. Give it:
- the prompt, the README and the board;
- `HEAD`, and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad, with ruling 3 as ruled.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the files of ruling 10. Its log is `logs/06-drivers-and-exports.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **Stop and ask** on any of the conventions' stop conditions. A stop is a reply headed **STOP** with the work left
  uncommitted.
- **At the end it replies with:**
  - the log's path and the message as committed; `git show --stat` of its commit;
  - the suites at the base and after, with counts, times and `uptime`;
  - the Export gate item by item (prompt §2), each with its command and output;
  - the comparison's result, file by file and path by path, and the store's SHA-256 before and after;
  - the parse refusal's message, and the A rows' drain model for each model;
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
   - No base worktree is left (`git worktree list`).
   - The commit's parent is this note's commit, or a later one that is not the agent's.
   - The main checkout is untouched. Nothing is pushed.
2. **The exponential is unchanged.**
   - Re-run `export_tree.py` from a base worktree of my own and from the tip, on the agent's store, and `cmp` every
     exponential file and path.
   - Run, serially and timed, `test_golden_history`, `test_golden_reflecting_history`, `test_tuning`,
     `test_driver_options`, the quanta tools' tests and `RunDrivers/tests`.
   - Check that every changed printed line of `main.py` and the plot scripts gives the base's string for the
     exponential, by reading the diff.
3. **The option and the rows.**
   - Parse, myself, `--model inverse-power --n-values 1 2 --A-values 1` (accepted, rows under `one-loop-measured-pl-v1`),
     `--n-values 3 --A-values 1` and `--n-values 1 3 --A-values 1` (exit 2, the message names the three options), and
     `--n-values 3 --alpha-values 0.1` (accepted).
   - Parse `exponential.yaml` and `inverse_power.yaml`.
   - `git grep DRAIN_MODEL_ONE_LOOP` matches the log's list.
4. **The power law's exports.**
   - Read the tip's power-law root: the path, the `model` column (or U10 (b)'s layout), the rows.
   - Run `tools/quanta_measures.py` on it myself, and check each row's `spectrum_model` against its n.
   - In a scratch probe of my own (never committed), run `plot_inputs` on the agent's store with `--model inverse-power`
     and with the exponential, and on an exponential-only store with `--model inverse-power` (refused, naming `--model`).
5. **The analyses' readers.** Spot-check two rows of §2.3's table against `git show main:` of the reader.
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
- the option, the rows' drain model and the parse refusal;
- the export layout as built, and what the 2026.11.0 analyses need from it;
- every existing test changed;
- any observation that bears on 07 (the rehearsal) or on the science run;
- that 07 is written only when the user asks for it.
