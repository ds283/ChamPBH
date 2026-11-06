# Orchestrator — prompt 04, drivers and exports

Read [`../README.md`](../README.md) first: §5, and §8 whole, in particular §8.5 (the Export row), §8.10 (decision 1)
and §8.12 (P33–P40). Then the prompt. This note binds 04 only, and governs where it differs from the prompt (prompt,
"Written"). Note 01's conventions bind as well: a tolerance is a number with its reason, timing is taken under load,
and the suites run serially.

**The prompt:** [`04-drivers-and-exports.md`](../04-drivers-and-exports.md) · **Gate:** "Export" · **Closes:** nothing
· **Opens:** only what the work finds · **Model:** Sonnet, as the prompt recommends · **Network:** none. Nothing is
pushed.

**When:** at once, at the user's request of 2026-11-06 ("Please write the orchestration note and dispatch it, and
afterwards carry out a review of the implementation agent's work"), in a fresh session.

**Gate**, checked at `6f6557e` on 2026-11-06 at 09:43 local time, before this note was committed:
- `one-loop-drain` is checked out in the main checkout at `6f6557e`, the commit that adds prompt 04.
  `git status --porcelain` is empty. `fe4c917` and `50e93c7` are ancestors of `HEAD`.
- `./venv/bin/python -m runregistrykit list`: 23 runs, all finished; none running, stale or unknown.
- The suites were not re-run for this note. 03's review left them at 26, 622 (190 + 432), 149, 156 and 68, all
  passing, and nothing under any `tests/` has changed since. The agent measures them again first (prompt §4).
- Load averages 23.94 / 17.71 / 20.28 at 09:43. The machine has 10 cores and 16 GB of memory, and it is shared.

## 0. What the orchestrator found

For this note the orchestrator read: `CosmologyConcepts/drain_model.py` whole; `config/argument_parser.py`
(`alpha_fraction`, `distinct_values`, `--alpha-values`); `main.py`'s alpha rows, grid lines, loop and `run_pipeline`
header; `plot_inputs.py` whole; `plot_by_beta.py`'s `build_beta_plot`, `run_pipeline`, `write_alpha_summaries` and
driver block; `plot_ScalarHistory.py`'s `write_csv_file`, the label calls and the driver block; `extract_common.py`'s
`_cosmology_line`, `alpha_path`, `CSV_COLUMNS` and its comment, `build_history_record`, `write_histories_csv` and
`plot_Delta_N_eff`; `BounceRow`, `DrainHistoryValues` and the accessors in `ComputeTargets/ScalarHistory.py`;
`ObjectFactories/ScalarHistory.py`'s conversions, `BOUNCE_COLUMNS`, `MASS_COLUMNS` and the bounce read-back (:823);
`tools/bbn_from_store.py`'s find query and `--alpha`; `tools/history_and_bbn.py`'s `_law_alpha`;
`tools/outward_step_scan.py`'s store reader; `RunDrivers/registered_run.py`'s `describe_scope`; 03's
`ObjectFactories/tests/test_drain_store.py` round-trip checks.

Here is what it found where the prompt is silent, loose or wrong.

- **"Bitwise to the raw column" depends on the units.** A converted column goes to the store by `to_column` and
  comes back on the object by `from_column` (the factory's read-back, `bounce_row_from_columns` and the samples').
  The export writes `to_column` of the object's value. So an exported cell is `to_column(from_column(raw))`. Whether
  that equals `raw` bit for bit depends only on the unit's numerical value in the cosmology's `UnitsLike`: in
  `Planck_units` ln M_P = 0 and M_P = 1, so the M_P columns come back exactly and the GeV column
  (`log_T_Jordan_GeV`) does not; in units where GeV = 1 it would be the other way round. The orchestrator first put
  this to the user as a question about one column. The user answered (2026-11-06): *"The whole question is
  misconceived. The other columns might be stored in `M_P` or powers of it, but it's only an accident that we are
  using units where `M_P=1`. … The whole point of the units system is so that we can change the units if we want
  to."* Prompt §1.9's "bitwise, read raw by SQL" and §2.1's "compare every new cell … with the raw column" are
  therefore restated as identities of the conversions, the same for every column, in ruling 1.
- **The label's spelling of α is not one string today.** `_cosmology_line` and the titles write `α = <α:g>`; the
  `run_pipeline` header lines of `main.py`, `plot_by_beta.py` and `plot_ScalarHistory.py` write `alpha = <α:g>`; the
  legend of `plot_Delta_N_eff` writes `$\alpha$ = <α:g>`. Prompt §1.4 asks for one helper giving `α = <α:g>`, and
  also asks that every fixed-law string and every printed line be unchanged, character for character. Both cannot
  hold with one fixed spelling (ruling 2).
- **`tools/bbn_from_store.py`'s `--alpha` has `type=alpha_fraction`,** which refuses any value ≥ 1 at parse, before
  `--drain-law` is known. Under `one_loop`, A = 10 must parse. `tools/history_and_bbn.py`'s `_law_alpha` (01) is the
  precedent: the option is read as text, then checked by the law after the parse (ruling 4).
- **An argparse type that raises `ValueError`** is reported by argparse as `invalid <type name> value: '…'`, without
  the message. `check_drain_model` raises `ValueError`, so a type that calls it must re-raise as
  `ArgumentTypeError` for its message to reach the user (ruling 5).
- **A default model is a trap.** If the path helper, the label helper or `build_history_record` default the model to
  `fixed`, a one-loop caller that forgets it writes under `alpha=<A>/` with an α label, silently (ruling 3).
- **§2.2's store and BBN.** The store needs no `BBNData` or `AdiabaticHistory` rows for the comparison: no code of
  this prompt touches the BBN columns, and `write_csv_file`'s BBN and adiabatic parts add no column. A BBN solve per
  history costs about 10 s (ruling 6).
- **`tools/outward_step_scan.py`'s count** is loose: rows of which table, before or after its filters, per shard or
  per store (ruling 7).

**Rulings.** Each is recorded in the log, with its class.
1. **The export's equality, in any units** (user, 2026-11-06, quoted in §0).
   - **The rule, the same for every new column of every file.** Read back by `float()` (an integer and a string as
     values), each exported cell equals the value the store's own conversions give from the raw column:
     `to_column(from_column(raw, kind, units), kind, units)`, compared by `float.hex`; a blank cell against NULL;
     a Boolean cell (0/1) against the raw integer. For a column with kind None this is the raw value itself. No
     column is singled out, and no test or script may rely on a unit being 1.
   - **This replaces** prompt §1.9's "bitwise, read raw by SQL" for histories.csv's drain cells, fields.csv's mass
     cells and every bounces.csv cell, and §2.1's "compare every new cell … with the raw column". The raw columns are
     still read by SQL from the store's file, as `test_dissipation_store` reads them.
   - **The units, exercised.** One test writes bounces.csv and the fields.csv mass cells from constructed
     `BounceRow`s and values under a second `UnitsLike` whose GeV and M_P are both ≠ 1 (for example
     `Units.GeV_units`), and checks each cell equals `bounce_row_columns(row, units)`'s / `to_column`'s value by
     `float.hex`. Instant.
   - **A measurement, not a test.** §2.1's script also reports, per column and in the units the store was built
     with, how many cells differ from the raw column by `float.hex` and the largest |cell − raw|. It is recorded and
     is never a stop.
   - A cell that fails the rule is a stop.

   *STRUCTURALLY REQUIRED* (§0, first point); the restatement follows the user's remark.
2. **The label helper.**
   - One helper gives the label of a row from its value and model, with the spelling of α as a parameter (default
     `α`). Under `fixed` it gives `<spelling> = <α:g>`; under any other model `A = <A:g> (<drain_model>)`, whatever
     the spelling.
   - The `run_pipeline` header lines pass `alpha`; the legend of `plot_Delta_N_eff` passes `$\alpha$`; the titles
     and `_cosmology_line` take the default. Every fixed-law string is then today's, character for character.
   - The tests check the fixed-law strings of all three spellings by literal.

   *STRUCTURALLY REQUIRED* (§0, second point).
3. **The model is never defaulted.** The path helper, the label helper and `build_history_record` take the model
   (or the row) with no default. Each existing call and test that changes for it is named in the log. The `ast`
   tests check that every per-row path and label in the three drivers passes the row's model.
   *IMPLEMENTATION CHOICE*, ruled here (§0, fifth point).
4. **`tools/bbn_from_store.py`'s `--alpha`**, after `_law_alpha`'s precedent. `tools/history_and_bbn.py` is not
   edited.
   - `--alpha` is read as text and checked after the parse, by the law.
   - Under `fixed` (the default): absent is 0; otherwise `alpha_fraction`'s check, and its message as argparse gave
     it today (`argument --alpha: must be a fraction …`), exit 2.
   - Under `one_loop`: `--alpha` is required, and checked by `check_drain_model(DRAIN_MODEL_ONE_LOOP, …)`; a refusal
     is `parser.error("argument --alpha: …")`, exit 2.
   - The law → model inverse goes in `drain_model.py` (the agent names it), with a test that it inverts `law_of`.

   *STRUCTURALLY REQUIRED* (§0, third point).
5. **`--A-values`' type** is `float(text) + 0.0`, then `check_drain_model(DRAIN_MODEL_ONE_LOOP, value, "--A-values")`,
   whose `ValueError` is re-raised as `argparse.ArgumentTypeError` with its message. The parse tests check that
   stderr names `--A-values` and carries that message. *As the prompt says; made exact* (§0, fourth point).
6. **§2.2's store and the two exports.**
   - No `BBNData` or `AdiabaticHistory` row is required. histories.csv's records are built with `bbn=None` and
     `baseline=None` in both trees; fields.csv is written with unavailable stand-ins for `BBN` and `Q` in both trees,
     or with real rows if the agent builds them. The log says which.
   - Both trees open the store read-only. The SHA-256 of every file of the store is taken before and after each
     tree's export, and equal.
   - The base worktree runs from its own root, with this checkout's `./venv/bin/python` and `PYTHONPATH` set to the
     worktree.
   - **The comparison.** Each tree writes histories.csv once for the fixed-law records alone and once for the
     one-loop records alone; the fixed-law projection is compared byte for byte, the one-loop one reported.
     fields.csv is compared per history.

   *IMPLEMENTATION CHOICE*, ruled here (§0, sixth point).
7. **`tools/outward_step_scan.py`.**
   - The query keeps only `alpha_value` rows whose `drain_model` is `DRAIN_MODEL_FIXED`, passed as a bound parameter
     from the constant.
   - The count is of `ScalarHistory` rows under any other model, over every shard of the store, before its M, β, α,
     block and success filters, so that it does not depend on them. One line, only when the count is > 0. The agent
     chooses where the line goes, and says which.
   - A shard whose `alpha_value` has no `drain_model` column is read as today, with nothing counted.

   *As the prompt says; made exact* (§0, seventh point).
8. **The printed lines.**
   - Every existing line that names an α row keeps its text under `fixed`.
   - Under the one-loop model the same lines name the row by ruling 2's helper.
   - The new A lines (`main.py`'s grid line, the plot scripts' line beside "alpha values") are printed only when
     `--A-values` is given, and name the model once.

   *As the prompt says; made exact.*
9. **Files.**
   - The commit holds prompt §5's files, and `CosmologyConcepts/drain_model.py` for ruling 4's inverse.
   - Also, where a forced change touches them, the existing test modules, each named in the log.
   - Nothing under `var/` is committed. Nothing under `.documents/one-loop-drain/` but `export-04/` changes.

   *STRUCTURALLY REQUIRED*; followed.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status` before
  committing. Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The Gate, and the five suites, before anything else. They run serially: ComputeTargets in its two halves, with
     the `test_[g-z]` half and `tools/tests` in the background, waiting for each notification.
  2. The option, the helpers and the drivers (§1.1–§1.4), with their tests.
  3. The exports (§1.5–§1.7), with their tests.
  4. The tools and the scope (§1.8), with their tests.
  5. §2's store, the two exports and the comparisons.
  6. `black`; the suites again; the documents; the records; the commit.
- **Scratch.** Outputs go under `var/one-loop-drain/04/`. Probes, suite outputs and the base worktree go in the
  agent's own subdirectory of the session scratchpad. Nothing goes in `/tmp`. Nothing is written under
  `var/one-loop-drain/0[1-3]*/`. The base worktree is removed (`git worktree remove`, then `git worktree prune`)
  before the commit.
- **No Ray.** Nothing starts a cluster, and no `ray` command is run. No store is opened but the tests' temporary ones
  and §2.2's.
- **Stop conditions are real:**
  - the prompt's: the exports needing a schema, factory, key, fingerprint or label change; 05 unable to work from
    bounces.csv's columns; a fixed-law byte difference;
  - a cell failing ruling 1's rule;
  - a store file whose SHA-256 moves under an export;
  - a suite count that falls.

  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

Done at `6f6557e`, before this note was committed:
1. **The Gate above.**
2. **The reading of §0,** and the question of ruling 1 put to the user and answered.

## 2. Dispatch

One fresh-context subagent, on **Sonnet**, in the main checkout, in the background. Give it:
- the prompt, the README, the board and this note;
- `HEAD`, and the Gate as measured.

The agent reads this note and no other file of `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with ruling 9's files. Its log is `logs/04-drivers-and-exports.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **It starts nothing that outlives its turn.**
- **Stop and ask** on any of the conventions' stop conditions.
- **It replies with:**
  - the log's path, the message as committed, and `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - the option, its refusals, and the rows' order;
  - the helpers' signatures and strings under both models;
  - the three exports' columns as built, with units;
  - the Export gate, item by item (prompt §2.1–§2.3), with ruling 1's counts and measurement;
  - every existing test changed, and why;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — seven checks

1. **Scope.**
   - `git show --stat <agent>` lists only ruling 9's files. Its parent is this note's commit, or a later commit that
     is not the agent's.
   - `git status --porcelain --untracked-files=all` is empty, `git worktree list` shows the main checkout alone, and
     nothing is pushed.
   - No line under `ComputeTargets/` or `ObjectFactories/` but tests changes; `FINGERPRINT_FORMAT` is 7 and
     `VERSION_LABEL` 2026.10.0. `tools/history_and_bbn.py` is unchanged.
2. **The suites, re-run by me**, serially as the conventions say: the counts the log states, all passing, none fallen.
3. **The exports, independently.** In a scratch script of mine: store P-2197 at A = 1 and one fixed-law history at
   α > 0 on a temporary store, export them with the agent's functions, and check every new cell by ruling 1's rule
   against raw SQL. Then the same writers under a second `UnitsLike` with constructed rows.
4. **The fixed law, byte for byte.** In a scratch worktree at the agent's parent, export §2.2's store's fixed-law
   histories with the base code by my own script, and compare with the agent's exports' projections.
5. **The drivers.** Read the diffs of `main.py`, `plot_inputs.py` and both plot scripts. Check by my own probe:
   `--A-values` refusals (exit 2, the option named), the rows' order, the paths and labels under both models, and
   `plot_inputs.parameters` refusing an A stored only as α and an α stored only as A.
6. **The tools.** By my own probe: `bbn_from_store.py`'s find query under both laws and on a format-6 imitation;
   `--alpha 10` accepted under `one_loop` and refused under `fixed` with today's message; `outward_step_scan.py`'s
   count; the inventory report on a fixed-only store equal to its pre-03 lines; `describe_scope` with and without
   `--A-values`.
7. **The records.**
   - The log is in §5.1's form, with every deviation classed and §0's rulings among them; its §7 gives 05 the format
     as prompt §5 asks.
   - The verification document's §1–§3 are unchanged, and §4 follows prompt §2.
   - The board: row 04, its items, the Export gate, and **Last updated**.
   - `CLAUDE.md`'s counts are those measured, and "The plot scripts" names `--A-values` and the one-loop path.
   - `architecture-summary.md` has its dated note.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 04 landed, the Export gate held, and this note added to the list of orchestrator notes, marked
  "used for 04";
- an *Orchestrator review of prompt 04* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s campaign row.

Residue is fixed in that commit or a follow-up of its own. Anything that is not residue is opened on the board's §3
and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the Export gate: the option, the paths, the three exports, the fixed law unchanged, and the tools;
- anything opened;
- that 05 (the derivation tool) is written next.
