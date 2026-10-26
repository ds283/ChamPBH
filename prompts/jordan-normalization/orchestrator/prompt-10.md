# Orchestrator — prompt 10, close

Read [`../README.md`](../README.md) first: §0.3 items 1, 2 and 7, §1, §2.2 (10's charter), §3, §5,
§6, §7. Then the prompt, the board, every log (`logs/01`–`09`, and `logs/06-probes/table.txt`),
`.documents/jordan-normalization-verification.md`, and `.documents/review-remediation-verification.md`
§4.12–§4.15, whose form §4.16 follows.

**You do not write code.** You may:
- run the suites, `main.py --inventory` on 09's reference store, and the `--help` of every command
  §4.16 names;
- run the registry's read-only commands (`list`, `store show`, `store fingerprint` without `--write`);
- run a probe from the session scratchpad, never committing one, and never starting Ray;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`10-close.md`](../10-close.md)
**Board items:** C1–C5 · **Gate:** none of its own (it closes the board) · **Closes:** nothing ·
**Opens:** only what the work finds · **Model:** Sonnet, as the prompt recommends · **Network:**
none. Nothing is pushed.

**Gate**, checked at `e894323` on 2026-10-26 at 21:40 GMT, before this note was committed:
- 09 landed (`2d3520b`) and reviewed (`e894323`); the gate "R" holds (board §2). No code changed
  since `e624f1a`.
- `jordan-normalization` checked out in the main checkout; `git status --porcelain
  --untracked-files=all` empty. `HEAD` is the commit that adds this note, which touches only
  `orchestrator/`: it is the prompt's **dispatch commit**.
- `ray_cluster.sh status`, exit 0: the lock free (`~/.raypoolkit/rehearsal-lock` does not exist), no
  Ray process of the cluster, nothing `stop` would signal, ports 6380, 6381, 8267 and 52367 free.
- `./venv/bin/python -m runregistrykit list`: eleven runs, all finished — 09's seven (six `done`,
  `resumed-a` `killed`), `ray-sqla-align` 02's three, and `rrk-adoption-03-unit-20261026T151946`
  `lost`. None `running`, stale or unknown.
- **09's resumed store was retired by the user** on 2026-10-26 at 21:39 UTC (`store show
  var/datastores/jordan-normalization-09-resumed.db`: `state: retired`, reason "jordan-normalization 09
  rehearsal; equal to the reference store, not used by 10", the fingerprint `3b92d064…15a7` matched at
  retirement). Its shards are gone; its sidecar stays, as a tombstone. **Only the reference store**,
  `var/datastores/jordan-normalization-09-reference.db` (16 shards and its sidecar), is handed over.
- `.documents/OPEN_ISSUES.md` §1.15 is empty ("None remains"): **the prompt's first stop condition
  does not trigger.** 25 open in the index, six of them on this board (§1.16).
- The one-minute load was 4.6 (4.63 / 5.95 / 5.66).

## 0. What makes this prompt unusual

It changes no code and runs nothing long. Its claims are of three kinds: what the code does now
(each with a `file:line` at the dispatch commit, read in the code, not copied from a log); what the
campaign measured (each with the log or the record that holds it); and estimates (each said to be an
estimate, with how it was made). **A sentence that is none of the three is not written.**

The orchestrator read the files the prompt names at `e894323` and ran the probes of §1. What it
found, where the prompt is silent, loose or wrong for this tree:

- **The documents are layered records.** `architecture-summary.md` keeps its first text and adds
  dated notes (`> **Note, <date> (<campaign>, prompt NN).**`); `numerical-strategies.md` adds dated
  subsections (`### 3.5 Added 2026-10-01 …`, `### 3.6 Added 2026-10-02 …`, `### 7.6`, `### 7.7`).
  Prompts 01 and 02 of this campaign edited body text in place for the strip and the rename; prompt
  07 instead added notes that end "The text before this note describes … as it was", so the body
  before them is now, by their own statement, a record. `CLAUDE.md` invariant 6 makes everything
  under `.documents/` additive. Rewriting that body would make 07's notes false.
- **Much of C1 is already written.** `architecture-summary.md` has 07's notes on the tree (§2), the
  registry (§4.2), the options and sharding (§5.2), the key, outputs and compute path (§7.2) and the
  pipeline (§9); 05's on the electronvolt (§8.1); 04's §8.4a (the interface) and its notes in §8.3 and
  §8.4. **What no document yet describes:**
  - 03's axis: ln(1 + z_E) stored, the grid as exact multiples of ln 10/250, the absolute lookup at
    `DEFAULT_LOG_ONE_PLUS_Z_PRECISION = 1e-10` (`config/defaults.py`), ln(1 + z_J) on every sample,
    `SampleValues`' 18 fields, the default extent of 300 decades;
  - 06's tuning layer in `numerical-strategies.md` (only 01 and 02 touched that file): the two
    conditions, the update (seed, one plain step, the secant), the guards, the four failure clauses,
    the progress line, the outputs and what 06 and 09 measured;
  - 07's removal of `ExponentialModel.from_rows` and of `Potentials/` and `ConformalCouplings/`, as
    it bears on §8.3, §8.4 and §8.4a;
  - 08's driver and tools: `--potential-type` gone, `--tuning-tolerance`, `--tuning-max-integrations`
    and `--n-values` (§5.1), the super-Planckian warning at the seed's φ_ref (§9), the wrapper's
    scope, `tools/history_and_bbn.py` tuned with `--fixed-normalization`, the surfing tools tuned with
    their window to `--T-window-low-GeV`, and `histories.csv`'s columns.
- **Statements now false of the present, with no later note**, found by the orchestrator's grep (a
  starting list; the agent's own grep, ruling 3, decides):
  - `architecture-summary.md`: §5.3's listing `DEFAULT_REDSHIFT_PRECISION = 1e-7` (`:665`; 03's log
    names it); §7.2's `ODERHS` "The function passed to `scipy.integrate.solve_ivp`" (`:792`; 04's log
    names it) — its "Integration procedure" (`:822-829`) is already superseded by
    integrator-remediation's note at `:831`; §8.2's `Lambda_value` (`:985`) and "redshift values"
    (`:988`); §9's step 3
    "(β, M, Λ)" and step 5 "`Potential_array` and `Coupling_array`" (`:1140-1142`), which 07's note at
    `:1238` covers in part; §12's adaptation guide naming `ExponentialCoupling` and the two deleted
    directories (`:1300`, `:1335-1336`).
  - `numerical-strategies.md`: §1's "grids of potential parameters (M, Λ)" (`:13`); §1.1's re-map
    "`z = exp(N) - 1`" (`:27`); §3.5.3 G1's `reflects_at_origin` "new on `AbstractPotential`" and
    `log_V_floor` "`Λ⁴` for the exponential" (`:271-274`); §3.6.1's `fixed_T_values(result, policy,
    coupling, units)` (`:387`); §4.1, which describes `ScalarHistory._create_functions` (board issue
    `[03-scalar-history-functions-splines-z-but-evaluates-ln-one-plus-z]`); §6's "`4 log Λ`" cached
    (`:727`); §7.6's `--T-stop-GeV` pre-check advice (`:941`, `:1050-1052`), an option that is gone;
    §9 item 4, "Endpoint re-mapping z ↔ N" (`:1324-1326`).
- **The brief's R6 audit finds nothing to correct.** `git grep -n -i -E 'scale[sd]? out|scaling
  out|A\(0\) *= *1|Ω\(0\)'` outside `prompts/` finds nothing, and no current document forms 2βφ/M_P
  as an offset. Ω(φ) = exp(βφ/M_P) at `architecture-summary.md:1050` is the coupling's own Ω, which
  §8.4's note of 04 already sets against A(φ_ref) = 1. The two notes for the paper,
  `.documents/numerical-methods-for-paper.md` (2026-09-29) and
  `.documents/paper-corrections-numerical-section.md` (2026-10-01), are dated records written for the
  authors, outside the prompt's file list. They describe the old normalization (A = Ω with Ω(0) = 1,
  a fixed Λ, z stored, M_P at 2.436 × 10²⁷ eV), and the authors will rewrite the paper from them.
  The two `peakewsurf-*` reports are LaTeX derivations, with no such statement (grep above).
- **The logs' "made false" lines.** No log uses the phrase. The lines the prompt means are in the
  logs' "Observations not acted on" and "State handed to the next prompt": statements in a
  document, a docstring, a comment or a help text that a prompt's change made false. What the
  orchestrator found:
  - 01: the dated notes of `architecture-summary.md` (left by invariant 6, as 01 said);
  - 03: `architecture-summary.md:636` (now `:665`), `DEFAULT_REDSHIFT_PRECISION` (C1, above);
    `--samples-log10-z`'s help, "per log10(z)" (`config/argument_parser.py`; still so at `e894323`,
    `main.py --help` line 47). **The grid was a `logspace` in 1 + z before 03 too** (README §0.3
    item 4), so 03 did not make it false: **not a stop**;
  - 04: `architecture-summary.md` §7.2, `ODERHS` "passed to `scipy.integrate.solve_ivp`" (`:792`; C1,
    above). 04 did not make it false (integrator-remediation did), and its log leaves it to 10;
  - 07: `test_golden_history.py`'s docstring — **fixed** in 07's review (`f8fced8`; the docstring now
    names `integrate_history`, `ComputeTargets/tests/test_golden_history.py:6-7`); `config/sharding.py`'s
    comment "mostly independent of (M, Lambda)" (`:51`), left by 07 as a statement of physics;
  - 08: the plot scripts accept `--tuning-max-integrations`, which `CLAUDE.md`'s plot-scripts bullet
    already says (true, not false);
  - 08's "The prose" names `README`, `architecture-summary.md` and `numerical-strategies.md` as 10's.
- **`CLAUDE.md` is nearly right.** The "Units", "Redshift" and plot-scripts bullets were written by
  05, 03 and 08 and read true at `e894323`; the counts are 08's and equal 09's. What C2 changes: the
  parenthesis "(The counts are those of 2026-10-26, after `jordan-normalization` 08. …)" to name 10.
- **The run scripts are tracked records** (`science-runs/2026-10-03-science-2026.6.0/`), with the
  SHA-256s §4.15 point 1 gave (`9c25562f…60e4`, `3e07d2e1…7761`, unchanged). `full_run_2026.6.0.sh`
  passes **none** of the removed options (08's Observation 2): with this tree's `exponential.yaml` it
  would build tuned histories at n = 1 into a new store. What in it is now false, by line:
  - `:2` "VERSION_LABEL 2026.6.0 (tree 4ae25b4 or later)": now `2026.7.0`;
  - `:7` "seven main.py invocations": eight (§4.15 point 1 said so);
  - `:13-14` `STORE_DIR` outside the repository and `science-2026.6.0.db`: §4.15 point 2 moves it to
    `var/datastores`; the label in the file name is now 2026.7.0;
  - `:27-28` `run()` calls `main.py` directly: the wrapped command of §4.15 point 2, with
    `--campaign jordan-normalization --prompt 10`;
  - `:38` "beta >~ 6.5 starts super-Planckian at phi* = 5": still so, now at the seed's φ_ref
    (β > 6.506 at M = 10⁻³, 08's log, D2);
  - `:56-57` the comment on rows reading "no ScalarModel row in the store": `plot_by_beta.py` now skips
    an unstored (M, n, β) with one line `!! plot: no ExponentialModel stored for M=…, n=1, β=…; no
    history can use it; skipped` (`plot_inputs.py:213-224`; `var/jordan-normalization-09/plot-reference.log:6-9`).
  The refresh script (`--drop bbn-data` on a copy of the 2026.6.0 store) has no role in a rebuild into
  a new store.
- **The cost of the rebuild can be estimated block by block.** The 2026.6.0 histories each ran one
  integration to T_CMB (`--T-stop-GeV` defaulted to T_CMB, `git show ac187c8:config/argument_parser.py`).
  A tuned history runs `tuning_iterations` of them. So the tuned scalar stage of a block is about
  its 2026.6.0 scalar-stage time (the `ALL WORK ITEMS COMPLETE` line of its log under
  `science-runs/2026-10-03-science-2026.6.0/logs/science/`) times the integrations per history that
  06 and 09 measured in that block's region (`logs/06-probes/table.txt`; verification §1.3). The
  adiabatic and BBN stages are not tuned, and cost what they cost then. Ruling 6 gives the numbers.
- **The heartbeat's silence is bounded by one integration.** Every integration of a tuning prints a
  `-- tune` line with `flush=True` (`ComputeTargets/ScalarHistory.py:1672-1677`), Ray forwards it,
  holding a line for at most its 5 s deduplication window (board issue
  `[09-ray-deduplicates-the-tune-lines-of-parallel-histories]`), and the wrapper beats on each line.
  RayPoolKit prints its progress line only after a `ray.wait` returns, and at most every 5 minutes
  (`raypoolkit/RayWorkPool.py:354-357`, `:564-595` of the pinned `v0.3.0`: `DEFAULT_NOTIFY_TIME_INTERVAL
  = 5 * 60`, `DEFAULT_NOTIFY_MIN_INTERVAL = 60`). 09's longest silence was 58 s. The slowest single
  integration measured is about 130 s (β = 25 at M = 10⁻³: 648 s for five, 06's table, load 6–12), and
  the slowest 2026.6.0 history, one integration, 263 s (README §0.3 item 7). **The default 900 s
  window suffices**; `list --stale-after` exists (`./venv/bin/python -m runregistrykit list --help`:
  "heartbeat staleness window, default 900").
- **`main.py --inventory` on the reference store** exits 0 in 29 s, starts no Ray, and leaves every
  file of the store byte-identical (`shasum -a 256` before and after); it lists 75,050 records in 15
  classes, `problems: none`, 6 `ExponentialModel`, 7 `ScalarHistory` (all validated), label 2026.7.0.
- **Every command §4.16 will name has a working `--help`** (exit 0): `RunDrivers/registered_run.py`,
  `main.py`, `plot_by_beta.py`, `plot_ScalarHistory.py`, `tools/surfing_scan.py`,
  `tools/history_and_bbn.py`, `tools/plot_surfing_histories.py`, `tools/plot_surfing_temperature.py`,
  `ray_cluster.sh`, and `runregistrykit list` and `store create`.
- **`prompts/INDEX.md`'s header is stale and one row is misplaced.** The header reads "12 campaigns:
  0 planned, 0 live, 12 closed"; the table has 13 rows, `jordan-normalization` live. The
  `ray-sqla-align` row sits after the closing paragraph, outside the table, and says "not yet merged
  or pushed", but `main` is at its review commit `396526b` and `jordan-normalization` contains it
  (`git log jordan-normalization..main` is empty); `main` is not pushed (`github/main` is `a144c83`).
- **The board's header** lists `plot_ScalarModel.py` among the code (`IMPLEMENTATION_STATE.md:22`);
  the file is `plot_ScalarHistory.py`.

**Baselines** at `2d3520b`, from 09's review (serially, load 3–9; `e894323` changed only records):
CosmologyModels **26**, ComputeTargets **76 + 167**, ObjectFactories **93**, tools **115**,
RunDrivers **64**, all passing. The golden histories are `ComputeTargets/tests/test_golden_history.py`
(`data/golden_history_05.json`) and its tuned test (e) (`data/golden_history_06_tuned.json`), both in
the ComputeTargets suite.

**These rulings bind the agent.** Each is recorded in the log as a deviation of the class given,
where it reads the prompt otherwise; `NOTED` ones are expectations, recorded as found.

1. **Additive, in the documents' own form** (`STRUCTURALLY REQUIRED`; `CLAUDE.md` invariant 6). In
   `architecture-summary.md`, `numerical-strategies.md` and `review-remediation-verification.md`,
   **no existing line is deleted or reworded.** The prompt's "corrected where it describes the
   present" is done by a dated note at the statement, which says what is true now and points at
   where the campaign's account is. Its form is the file's:
   - `architecture-summary.md`: `> **Note, 2026-10-26 (`jordan-normalization`, prompt 10).** …`,
     directly after the paragraph, list or code block it corrects; one note per place, however many
     prompts it covers, naming the prompts and their commits;
   - `numerical-strategies.md`: one new dated subsection, **`### 3.7 Added 2026-10-26
     (`jordan-normalization`): the model, the tuning to H0 and φ_min, the sampling axis and the Planck
     mass`**, after §3.6.2 and before §4, with numbered sub-subsections (`#### 3.7.1` …); and a
     one-line dated note at each earlier statement the grep finds false, pointing to §3.7's
     sub-subsection.

   A note goes only where the text is false of the present and no later dated note already says so.
   The dated notes and dated sections are records and are not edited, 07's "as it was" sentences
   included.

2. **What C1 writes** (`IMPLEMENTATION CHOICE`, ruled). Each item from the code at the dispatch
   commit, with `file:line`, and its numbers from the logs and the verification document:
   - **`numerical-strategies.md` §3.7**, in this order: (1) the model and the normalization:
     `AbstractModel`, `ExponentialModel` (M, n, β), `RealizedModel`, A(φ_ref) = 1 by subtraction in
     `RealizedModel` (`CosmologyConcepts/Models/AbstractModel.py`), the normalization as ln V₀, Eq. 7's
     ratio unchanged in form so the BBN interface is not (README §1); (2) the two layers,
     `integrate_history` (any T_stop) and `tune_history` (T_CMB) (`ComputeTargets/ScalarHistory.py:1137`,
     `:1575`); (3) the conditions, H_J(T_CMB) = H0 at fixed ω_m and φ_ref = φ_min(T_CMB), and the
     residuals as stored, with `tuning_converged`'s inequality (`:1423`); (4) the update: the seed
     (tracking, static, plain; `ExponentialModel.seed`, `:195` of its file), one plain step, then the
     secant (`TuningSecant`, `:1439`), the guards (`TUNING_GUARD_LIMIT = 4`, the V_target ≤ 0 guard,
     the effective-minimum clause) and the cap; (5) the failures, the four first clauses, and what
     `_tuning_state` catches (07's Observation 2: only `ValueError`); (6) the progress line, and that
     Ray's deduplication can hold some back (the board issue); (7) the fifteen stored outputs; (8)
     what was measured: 06's thirteen points (secant 73 integrations against the plain update's 116,
     never worse; methods agree to 2 × 10⁻⁵), the φ*-dependence of README §0.3 item 2, and 09's seven
     histories (3–9 integrations, the stored values equal to the tool's bitwise); (9) the sampling
     axis (03); (10) the Planck mass (05), pointing at `architecture-summary.md` §8.1's note rather than
     repeating it.
   - **`architecture-summary.md`**: the notes of ruling 1 at the places §0 lists and any the grep adds;
     for 03 and 08, one note each in §5.1 (the options: the axis options and their defaults, the three
     tuning options, `--potential-type` refused by `RemovedOption`), §5.3 (the precision constant), §7.2
     (`SampleValues`' 18th field and the two layers, pointing to `numerical-strategies.md` §3.7), §8.2,
     §8.4a (what 07 removed: `from_rows`; the model now a stored row), §9 (the warning at the seed's
     φ_ref, `Model_array` over (M, n, β), the scope) and §2's tools (the history tool and the surfing
     tools tuned, `histories.csv`'s columns).

3. **The R6 audit and the stale-statement grep** (`IMPLEMENTATION CHOICE`, ruled). The agent runs
   its own greps over the two documents and `CLAUDE.md`, at least for: `Lambda`, `Λ`, `T_stop`,
   `T-stop`, `potential-type`, `Potentials/`, `ConformalCouplings`, `ExponentialPotential`,
   `ExponentialCoupling`, `from_rows`, `z_serial`, `REDSHIFT_PRECISION`, `logspace`, `exp(N)`,
   `2.436`, `ScalarModel`, `scale out`, `A(0)`, `Ω(0)`, `2βφ`, `Planck units`. It records each command
   and, per hit, one of: "noted at §x (`:line`)", "already noted at `:line`", "a dated record, left",
   or "true". The two paper notes and the `peakewsurf-*` reports are read for the audit and **not
   edited** (outside the file list); the log lists, per paper note, each statement the campaign makes
   false (its line and what is true now), so that the authors can find them, and §4.16 points at that
   list (ruling 7, point 7).

4. **The "made false" lines** (`NOTED`). The agent's own pass over logs 01–09, in its log as a table:
   log, the line, fixed by whom (commit) or left, and why. Expected: §0's list. **`--samples-log10-z`'s
   help is not a stop** (§0): the log records it as left, a help string in code outside 10's files,
   imprecise before 03 too. `config/sharding.py:51` is left with 07's reason. Any other line that only
   a code change would fix is the prompt's second stop.

5. **`CLAUDE.md`** (`NOTED`). The suite counts are C5's (expected unchanged), and the parenthesis names
   `jordan-normalization` 10 with the date. The "Units", "Redshift" and plot-scripts bullets are read
   against the code and left if true; each is listed in the log as checked. No other line changes
   unless the agent finds it false, and then the log says which.

6. **The cost** (`IMPLEMENTATION CHOICE`, ruled; §0). In §4.16 point 3, a table per block: the
   2026.6.0 scalar-stage time from its log, the integrations per history from `logs/06-probes/table.txt`
   and verification §1.3, and their product; then the adiabatic and BBN stages as they were. Said to
   be an estimate on the machine and load of 2026-10-03, read, not measured. The orchestrator's
   figures, which the agent's must equal or explain:

   | Block | 2026.6.0 scalar stage | Integrations | Tuned scalar stage |
   |---|---|---|---|
   | C1 (β 1.00–3.00, M 10⁻³) | 21m 54.6s | 4 | ≈ 88 min |
   | C4 (β 1.100–1.700, M 10⁻³) | 4m 57.1s | 4 | ≈ 20 min |
   | L (β 3.25–25, M 10⁻³) | 18m 24.9s | 5 | ≈ 92 min |
   | C5 (β 0.50–1.10, φ* 1, 2, 5) | 38.1 + 51.7 + 29.1 s | 4 | ≈ 8 min |
   | C3 (M 0.5, 0.1, 0.03, 0.01) | 8m 25.6s | 5 (M ≤ 0.03, tracking) to 9–12 (M ≥ 0.1, static) | ≈ 42–101 min |
   | C2 (M 10⁻⁵) | 48m 14.9s | 3–4 | ≈ 145–193 min |

   about 6.6–8.4 hours for the scalar stages, against 104 minutes then; the adiabatic stages took
   12 minutes and BBN 42 in all. The slowest histories: β = 25 at M = 10⁻³ (06: 648 s, five
   integrations, load 6–12) and the M = 10⁻⁵ block (06: 214 s at β = 3; 09: 130 s). The heartbeat: as
   §0 reads it; **no `--stale-after` is needed**; if a run shows `stale` while its pid answers `kill
   -0` on its host, the run is alive and `list --stale-after 1800` reads it so. Each "read, not
   measured" is said.

7. **§4.16's points** (`IMPLEMENTATION CHOICE`, ruled). Its heading `### 4.16 Addendum 2026-10-26 —
   the `jordan-normalization` campaign`, after §4.15 and before `---`/`## 5`; its first paragraph and
   its "This supersedes, by statement and not by edit" list in §4.15's form (what the campaign did,
   prompt by prompt with commits; the board, the plan, the verification document; that the branch is
   not merged into `main`). Then the prompt's six points, and:
   - point 2 a table by line of `full_run_2026.6.0.sh`, in §4.15 point 2's form, from §0's list, with
     the gone options and the new ones and their defaults stated even where the script passes none,
     `--drop scalar-history` for whoever drops histories, and `plot_ScalarHistory.py`; the new scripts
     go in a new folder under `science-runs/` when the user writes them;
   - point 4 the S1 and S5 commands on the tuned tool (§4.14 point 2's options; `--T-stop-GeV` gone,
     `--T-window-low-GeV` 1e-3 by default), **each with a new `--out`**: the committed CSVs under
     `.documents/surfing-temperature/out/` are records of the fixed normalization, and the tool refuses
     them on resume (board §4, 08's closing line); 08's predicted 55 and 35 minutes at load 5–8;
   - point 5 P19 with `RayWorkPool.py:354-357`'s `ray.wait`, 09's 102.5 s, and what it means at the
     rebuild's costs (a computed history waits for two more to finish, and at a block's end for every
     one still in flight); for the user to rule upstream (`[04-the-pool-waits-for-every-in-flight-item-when-fewer-than-three]`,
     `rrk-adoption`'s board);
   - a point 7, **left to the user**: the merge into `main` (P15); the reference store under
     `var/datastores/` and its seven runs (the resumed store retired by the user, its sidecar a
     tombstone); the six open issues on this board (§1.16), three of them for the user to rule
     (`[08-a-history-that-did-not-converge-is-never-retried]`,
     `[09-ray-deduplicates-the-tune-lines-of-parallel-histories]`, and the RayPoolKit wait's); and the
     two paper notes, pointing at the log's list (ruling 3).

   Every command it gives is one the agent ran with `--help` (exit 0), and `main.py --inventory` on the
   reference store, with the store's `shasum -a 256` before and after, unchanged (§2 item 3).

8. **The board closed** (`NOTED`). Row 10 ✅ with "this commit"; C1–C5 ✅; the gates as they stand
   (no new gate); §3 as it stands, with its first paragraph saying the campaign closed with six open;
   §3.1 unchanged; §4 unchanged; `### 5.11 After prompt 10 (2026-10-26)` in §5.10's form; a closing
   paragraph; the status line "CLOSED 2026-10-26 — 10 of 10 landed …, the merge into `main` the user's
   (P15)"; the header's `plot_ScalarModel.py` corrected to `plot_ScalarHistory.py` (§0). The list of
   orchestrator notes is the orchestrator's (§4).

9. **The indexes** (`NOTED`).
   - `prompts/INDEX.md`: the `jordan-normalization` row **complete**, in the closed rows' form (prompts
     and commits, the verification document and §4.16, the final suites, the branch from `ac187c8`
     not yet merged, the rulings); the header's date, and its count "13 campaigns: 0 planned, 0 live,
     13 closed". **The `ray-sqla-align` row is not moved or edited** (another campaign's row): the log
     records its placement and its stale "not yet merged" under "Observations not acted on", for the
     orchestrator to offer as housekeeping.
   - `.documents/OPEN_ISSUES.md`: the header's date and the `jordan-normalization` clause (closed
     2026-10-26, 10 of 10 landed, prompt 10 opening and closing none), the count unchanged at 25 unless
     the work opens one; §1.15's paragraph says the campaign closed with none remaining; §1.16's
     paragraph its six as they stand, unchanged.

10. **The suites** (`NOTED`). `CLAUDE.md`'s commands, serially, in the foreground, `ComputeTargets` in
    its two halves, each call at 600 000 ms, before the edits and after, with `uptime`. Expected at
    both: the baselines. A failure is re-run alone before it is called real
    (`test_prym_passenger.test_a_oscillating_case_completes` is a 60 s wall-clock test that fails under
    load); an existing test that fails is a stop. No `.py` file changes, so `black` has nothing to do.

11. **The log** (`STRUCTURALLY REQUIRED`). `logs/10-close.md` in `dk-and-rpk-migration` README §5.1's
    form: what shipped, item by item; the deviations with their classes, these rulings among them;
    verification (the suites before and after, the greps with their commands and counts, the
    `--help`s, the inventory with its hashes); the "made false" table (ruling 4); the audit (ruling 3);
    the paper notes' list; "Observations not acted on"; "State handed to the next prompt" (the
    campaign's end state, for the user).

**What moves on purpose:** exactly the prompt's §5 list: `.documents/architecture-summary.md`,
`.documents/numerical-strategies.md`, `.documents/review-remediation-verification.md` (§4.16 only),
`CLAUDE.md`, the board, `prompts/INDEX.md`, `.documents/OPEN_ISSUES.md`, `logs/10-close.md`.

**What must not move:** every other file of the tree, code, tests, `exponential.yaml` and
`science-runs/` above all; `orchestrator/`; the README; every other campaign's records;
`.documents/jordan-normalization-verification.md` and `.documents/jordan-normalization/`; the two
paper notes; `var/`, the reference store above all (`--inventory` reads it read-only; nothing else
opens it); the venv; SGK, SI and the three packages. No Ray is started, no store is built, no surfing
scan is run.

**The stop conditions.** The prompt's §4, as §0 reads them (§1.15 is empty; `--samples-log10-z`'s
help is not a stop), and: a suite that falls; `--inventory` changing a file of the store; any command
§4.16 would name that does not run as written. A stop goes to the user through the orchestrator; the
agent resumes by message.

**Conventions.** All of these bind the agent.
- **Order.** The Gate (the prompt's, re-checked); the suites; the greps and the reading; the
  documents; `CLAUDE.md`; §4.16 with its `--help`s and the inventory; the board, the indexes; the
  suites again; the log.
- **Staging and reporting.** One commit, staged by explicit path only, after showing `git diff
  --cached --name-only`, which is the eight files exactly. Check `git log -1` before committing: `HEAD`
  must still be this note's commit. A record states only what a command or a file shows. "this commit"
  where the SHA goes; the orchestrator substitutes it. The message in `CLAUDE.md`'s form.
- **Scratch.** Anything saved while working goes under `var/jordan-normalization-10/`; never `/tmp`,
  never the session scratchpad, never the tree.
- **Not touched:** `orchestrator/`, the README, any other campaign's records.

## 1. Before you dispatch

Done at `e894323`, before this note was committed: the Gate above; `main.py --inventory` on the
reference store with its hashes (`orch10/inventory.txt`, `ref-before.txt`, `ref-after.txt` in the
session scratchpad); the `--help` of each command (`orch10/help_*.txt`); the greps and the cost
figures of §0 and ruling 6. The suites' baselines are 09's review's.

## 2. Dispatch

One fresh-context subagent, on **Sonnet**, in the main checkout. Give it: the prompt; the README; the
board; `HEAD`; the Gate as measured; and §0 of this note up to "## 1. Before you dispatch", as a copy
in the session scratchpad. It reads the logs, the documents and the code itself. It does not read
`orchestrator/`.

Tell it plainly:
- **One commit, at the end**, of exactly the eight files.
- **§0's rulings bind it**, each recorded with its class.
- **No existing line of the three documents is deleted or reworded** (ruling 1).
- **It starts no Ray, builds nothing, and opens no store but the reference one, through
  `--inventory`.**
- **Stop and ask** on any of the prompt's §4 conditions or §0's.
- **It replies with:**
  - the log's path; the message as committed; `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - each note and section it added, by file and line, with what it corrects;
  - the greps (ruling 3) with their counts and dispositions; the "made false" table (ruling 4);
  - the paper notes' list;
  - `CLAUDE.md`'s diff;
  - §4.16's points in one line each; the cost table (ruling 6) and any difference from §0's figures;
  - the `--help` exits and the inventory's result and hashes;
  - the board's status line and §5.11; the INDEX row and header; OPEN_ISSUES's header;
  - every deviation with its class; "Observations not acted on" and "State handed to the next
    prompt", verbatim.

## 3. The review — ten checks

1. **Scope.** `git show --stat <agent>` lists exactly the eight files; its parent is this note's
   commit. `git diff <dispatch> <agent> -- '*.py' '*.sh' '*.toml' '*.yaml' requirements.txt
   science-runs/ orchestrator/ README.md` is empty. `git status --porcelain --untracked-files=all`
   empty; `var/` in no commit; nothing pushed.
2. **Additive.** `git diff <dispatch> <agent> --` on the three documents has no `-` line (bar a
   moved blank line, explained), and `review-remediation-verification.md`'s diff starts after §4.15's
   last line.
3. **The code claims.** Ten `file:line` references from the new notes and §3.7, chosen across the
   items of ruling 2, read in the code at `<agent>`: each says what the note says. The secant, the
   guards and the four failure clauses read in `tune_history` and `TuningSecant` against §3.7.
4. **The numbers.** Each number in §3.7 and §4.16 traced to its log, table or record, at least
   twelve, the cost table among them, recomputed here from the 2026.6.0 logs and `table.txt`.
5. **The grep.** The orchestrator's own grep of the two documents for ruling 3's terms: every hit
   either noted, already noted, a dated record, or true, as the log says; none false and unnoted.
6. **The handover's commands.** Every command in §4.16 run here with `--help` (exit 0); `main.py
   --inventory` on the reference store, its files hashed before and after (unchanged); the wrapped
   command's arguments parsed by `main.py`'s parser in a scratch probe (`create_argument_parser`) for
   one block, giving the block's β, M and φ*, n `[1]`, tolerance 1e-5.
7. **The "made false" table** against §0's list and the logs.
8. **`CLAUDE.md`, the board and the indexes** as rulings 5, 8 and 9 say; INDEX's header count by
   counting the table's rows; OPEN_ISSUES's count by counting its rows.
9. **The log** in §5.1's form, every deviation classed, §0's rulings among them; the message in
   `CLAUDE.md`'s form.
10. **The suites** at `<agent>`, serially, no cluster up: 26 / 76 + 167 / 93 / 115 / 64.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, on the board, in the log, in §4.16 and in `prompts/INDEX.md`;
- the board's list of orchestrator notes, with this one marked "used for 10";
- `### 5.11`'s *Orchestrator review of prompt 10* paragraph, holding what each check found;
- the status line: closed, 10 of 10 landed and reviewed.

Residue the review finds is fixed in that commit or a follow-up of its own. Anything that is not
residue is opened on the board's §3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Offer the user, as housekeeping** (standalone commits, at the user's word): `--samples-log10-z`'s
help ("per decade of 1 + z"); the `ray-sqla-align` row of `prompts/INDEX.md` moved into the table and
its merge stated.

**Report to the user:** the commit and the review commit; that the campaign is closed and the
rebuild unheld; §4.16's cost estimate and the heartbeat's bound; the rulings that read the prompt
otherwise than it was written (additive notes, not rewrites; no "made false" stop; the paper notes
left and listed); that the merge into `main` and the push are the user's (P15); and the reference
store kept under `var/datastores/`.
