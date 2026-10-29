# Orchestrator — prompt 04, the wall through the model

Read [`../README.md`](../README.md) first: §5 (the rules), §8 in full (the re-plan: R7–R9, P25,
04's charter in §8.4 and the interfaces of §8.5). Then the prompt, log 03 §8 and the board. This
note binds 04 only. The conventions of notes 02 and 03 (a tolerance is a number with its reason;
timing under load; the suites run serially) are restated here, and bind.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`04-the-wall-through-the-model.md`](../04-the-wall-through-the-model.md)
· **Gate:** "Wall" · **Closes:** nothing · **Opens:** only what the work finds · **Model:** Opus,
as the prompt recommends · **Network:** none. Nothing is pushed.

**Gate**, checked at `eda13dd` on 2026-10-29 at 14:09 GMT, before this note was committed:
- `inelastic-reflection` checked out in the main checkout, at `eda13dd` (prompt 04 written).
  `git status --porcelain --untracked-files=all` is empty; `git worktree list` shows the main
  checkout alone. No code has changed since 03's review `d1d5ccf` except `67cb5f8` (two docstrings
  of `test_injection.py`). `HEAD` is the commit that adds this note, which touches only
  `orchestrator/`.
- `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing. No Ray is started by this prompt.
- `./venv/bin/python -m runregistrykit list`: 11 runs, all finished, none `running` or stale.
- The suites were not re-run for this note. The agent measures the baselines (prompt §3.1):
  26 / 317 (96 + 221) / 103 / 115 / 64.
- The load averages were 9.6 / 7.5 / 8.0. Other workloads share the machine.

## 0. What makes this prompt unusual

**It writes root-finders on the model that must work, through the hooks alone, from M = 0.5 M_P
down to field values of 10⁻³² M_P, and must not touch the integrator.** The orchestrator read at
`eda13dd` every file the prompt names and the notes' sections, and ran two probes from the session
scratchpad. Line numbers are right for this tree. What it found, where the prompt is silent, loose
or wrong:

- **The exception cannot be imported at the top of `AbstractModel.py`.** `ExponentialModel.py`
  imports `ComputeTargets.exceptions`, which initializes `ComputeTargets`, whose `__init__` imports
  `AdiabaticHistory` and `ScalarHistory`, which import `RealizedModel` from `AbstractModel`
  (`ComputeTargets/__init__.py:1-11`, `ComputeTargets/AdiabaticHistory.py:8`). That is why
  `CosmologyConcepts/Models/__init__.py:16-18` imports `AbstractModel` first. The orchestrator
  prepended `from ComputeTargets.exceptions import ComputationFailureError` to `AbstractModel.py`
  through an import hook (nothing tracked was edited): `import CosmologyConcepts.Models` in a fresh
  interpreter fails with "cannot import name 'AbstractModel' from partially initialized module";
  importing `ComputeTargets` first hides it. So the test suites could pass while a fresh import
  fails. Ruling 1.
- **The prompt's tolerance fails its own test.** Prompt §1.5 asks for `brentq` "with a relative
  tolerance near 10⁻¹³ in ln φ". At φ_t ≈ 10⁻³² M_P, |ln φ| ≈ 74, so the bound is about 7×10⁻¹² in
  ln φ, and ln 𝓡 inherits it times 2(y + 4). The orchestrator's probe (the planner's
  `classical_turning_point`, field at 0.1 M_P, normalization −279.87, ln E ∈ {−180, −120, −60}, at
  the four masses of item 1) measured |Δ ln 𝓡| = 1.3×10⁻¹⁰ against the closed form at M = 10⁻¹⁶,
  ln E = −120 with rtol = 10⁻¹³, which fails item 1's 10⁻¹⁰. With rtol = 4ε_mach (SciPy's floor) and
  xtol = 10⁻¹⁴, the largest was 3.0×10⁻¹² in ln 𝓡 and 6.8×10⁻¹⁵ in φ_t at every point. Ruling 2.
- **The hooks limit E/V₀ from below.** ln V = normalization + (M/φ)ⁿ is rounded at
  ε_mach·|normalization| ≈ 6×10⁻¹⁴ (|normalization| ≈ 280 in Planck units), so at the root
  (M/φ_t)ⁿ = ln(1 + E/V₀) carries that absolute error, and φ_t a relative error of about
  6×10⁻¹⁴/(n ln(1 + E/V₀)). Item 1's 10⁻¹² needs E/V₀ above about 0.1. Every infall the integrator
  meets before today has E ≫ V₀. Ruling 2.
- **A second crossing cannot be seen by signs.** Prompt §1.5 asks for the sign at the bracket's
  outer end and at its midpoints. The doubling stops at the first probe with the inner sign, so
  every probe before it has the outer sign by construction, and three signs on a bracket that holds
  three roots look like three on a bracket that holds one. What can be seen cheaply is the *value*:
  the quantity searched (ln(V − V_floor), ln V″, or the minimum's log difference) must move
  monotonically along the probes. Ruling 3.
- **The search can leave float range.** Doubling from the field in ln φ: after ten doublings the
  probe is 1023 e-folds in, and φ = exp(ln φ) underflows to 0.0 below ln φ ≈ −745. Then
  `ExponentialModel`'s hooks divide by zero (`ExponentialModel.py:84`, `:99`, `:115`), and
  `ZeroDivisionError` is not among the exceptions they catch. A hook's own `OverflowError` path
  prints a "!!" line and raises `ComputationFailureError`. The 64-doubling cap is never reached.
  Ruling 3.
- **The one-loop root may lie behind the field.** Prompt §1.1's `None` covers only the classical
  point. If V″ at the field is already at or above m_ta², the one-loop search from the field has no
  bracket. Where the classical point is behind the field, φ_t ≥ φ, so `outside_classical` is still
  decidable. Ruling 4.
- **phi_ref enters nothing.** The classical and one-loop methods read only the V hooks. The minimum
  reads (ln A)′ and (ln A)″, which `RealizedModel` passes through unshifted (`AbstractModel.py:120-124`).
  The prompt says "taking the normalization as the hooks do"; `effective_minimum` also takes
  `phi_ref`, because it reads ln A itself. Ruling 5.
- **(ln A)″ = 0 on every model the prompt names except the one it leaves open.** The exponential
  has (ln A)″ = 0 (`ExponentialModel.py:135`), so the breakage "a missing (ln A)″ term" is caught by
  no exponential test. The prompt gives the power-law wall's V, not its A. Ruling 6.
- **ln E carries E ≤ 0.** The methods take ln E. E = 0 is ln E = −∞, and a negative E has no log,
  so "E ≤ 0, raising" is tested as ln E = −∞ and NaN. Ruling 7.
- **The worked example checks out.** The orchestrator evaluated `sec:params`' equations in
  Planck units (M = 10⁻¹² GeV, V₀ = 10⁻⁴⁸ GeV⁴, E = (1.18 × 0.23 GeV)⁴): classical y_t = 105.307;
  one-loop from ℓ₀ = 32, y = 39.080, Λ = 53.792, k_ex = 5.625×10¹³ GeV, m_ta² = 0.2300 GeV²,
  ℓ = 32.3956388253975, converged to |Δℓ| ≤ 10⁻¹² ℓ on the sixth iterate with a contraction of about
  0.007 per step. These are within prompt §1.6 item 2's tolerances. They are given here so that a
  disagreement shows at once; the agent derives its own and does not copy these into its tests.
- **The notes' 0.25 GeV².** At ℓ = 32 the chain gives π(c_β T_s)² = 0.2314 GeV², not 0.25. The
  prompt already says not to assert the notes' rounding; the log reports both.

**Rulings.** Each binds the agent and is recorded in the log with its class.

1. **`ComputationFailureError` is imported inside the methods that raise it**, as a function-level
   import with a comment naming the cycle, never at the top of `AbstractModel.py`.
   `CosmologyConcepts/Models/__init__.py` is unchanged, and the new namedtuples are imported from
   `AbstractModel` where they are needed. A test runs, in a fresh subprocess from the repository
   root, `import CosmologyConcepts.Models` alone, and then `import ComputeTargets` followed by it,
   and asserts both exit 0. *STRUCTURALLY REQUIRED*.
2. **The root tolerance.** Every `brentq` in ln φ (or ln x) uses rtol = 4ε_mach (SciPy's floor,
   `4 * np.finfo(float).eps`) and an absolute xtol of 10⁻¹⁴, both module constants with a comment
   giving this reason. This replaces prompt §1.5's "near 10⁻¹³". Item 1 uses the probe's points
   (field at 0.1 M_P, normalization −279.87, ln E ∈ {−180, −120, −60} at each of the four masses),
   or a superset of them, and asserts E/V₀ > 1 at each; the log gives the conditioning bound
   ε_mach·|normalization|/(n ln(1 + E/V₀)) beside the measured errors. **B7** replays rtol = 10⁻¹³,
   xtol left at SciPy's default, and must fail item 1's ln 𝓡 at (10⁻¹⁶, −120) or name the point
   where it does. *STRUCTURALLY REQUIRED* (prompt §1.5 against §1.6 item 1).
3. **The search.**
   - It doubles in ln φ from the field (ln x from the caller's point, for the minimum) and stops at
     the first probe with the inner sign. Before `brentq` it may halve the last bracket a fixed
     number of times; the log says how many and why.
   - **Monotonicity is checked on values.** Along the probes from the field inwards (and at any
     halving midpoints), the searched quantity must move strictly towards its target; repeated −∞
     values of ln(V − V_floor), where V rounds to V_floor, are allowed. A violation raises,
     naming the probes and their values. Nothing is searched exhaustively.
   - **A probe that leaves float range ends the search.** A probe whose φ underflows to 0.0 or is
     subnormal, or at which a hook raises or returns a non-finite value where the target is finite,
     is a `ComputationFailureError` naming the probe and the last bracket; the hook's own
     exception is chained, not swallowed. The 64-doubling cap stays as a backstop.
   - **One test model** shows the check working: a test-only reflecting wall with a bump between
     the field and its turning point, built so that a doubling probe sees the searched quantity
     move away from its target, raises. **B8** removes the check and must fail that test.

   *IMPLEMENTATION CHOICE*, ruled here (prompt §1.5's sign check cannot see a second crossing).
4. **The one-loop turnaround behind the field.** If V″ at the field is at or above m_ta² at an
   iterate, the method returns `None`, as prompt §1.1's classical exception, and its docstring says
   so: the field is already inside its one-loop turnaround to the accuracy of the iteration.
   `outside_classical` is φ_ta > φ_t where the classical point is ahead of the field, and `False`
   where it is behind (then φ_t ≥ φ ≥ φ_ta). Item 5 adds a test of the `None`. *IMPLEMENTATION
   CHOICE*, ruled here.
5. **The signatures take the normalization and no phi_ref.** The `AbstractModel` methods call the
   hooks directly with the normalization; the `RealizedModel` mirrors pass `self._normalization`
   and nothing else. The minimum's docstring says why phi_ref does not enter: it reads (ln A)′ and
   (ln A)″, never ln A. Item 6 asserts each mirror calls the model's method with the normalization
   and the caller's arguments and returns its result unchanged (a `unittest.mock` spy on the
   model). *IMPLEMENTATION CHOICE*, ruled here.
6. **The power-law wall has a coupling with (ln A)″ ≠ 0**: ln A = βφ/M_P + γφ²/M_P², or another
   with (ln A)′ > 0 on the range tested, as `_MinimalModel` in `test_model_interface.py` is built.
   Item 4's residual and derivative tests run on it as well as on the exponential, so that **B4**
   (the missing (ln A)″ term) fails a test. Its hooks are written in the test, with no import from
   `ExponentialModel`. *STRUCTURALLY REQUIRED* (the prompt's B4 must fail a test).
7. **Inputs.** φ ≤ 0 or non-finite, and ln E or ln T that is NaN or ±∞, raise
   `ComputationFailureError` naming the model, the inputs and the reason. Item 5's "E ≤ 0" is
   tested as ln E = −∞ and as NaN. The minimum checks `reflects_at_origin` and does not read
   `log_V_floor`; the turning-point methods check both. *IMPLEMENTATION CHOICE*, ruled here.
8. **The rounding bounds of item 3** are the residual's sensitivity to ln φ times the root's
   tolerance, plus the rounding of the residual's own evaluation, plus, for m_ta⁴ℓ = 32π²E, the
   iteration's stopping tolerance. Each is derived in the log from the operations, stated as a
   number, and never loosened to make a test pass. A bound that fails is a stop (prompt §5).
   *STRUCTURALLY REQUIRED*.
9. **ℓ in the record is the test's to recompute.** Item 3 recomputes ℓ = ln(k_ex/m_ta) from the
   returned ln k_ex and ln m_ta and asserts it equals the returned ℓ and that m_ta⁴ℓ = 32π²E; so
   **B6** (the iteration stopped after one step) fails item 3 as well as item 2. *IMPLEMENTATION
   CHOICE*, ruled here.
10. **The worked example is evaluated by the test**, in Planck units through `units.GeV`, by its own
    loop over `sec:params`' equations, never by calling the methods. The printed values are asserted
    within prompt §1.6 item 2's tolerances; the log prints m_ta² at ℓ = 32 and at the converged ℓ.
    *STRUCTURALLY REQUIRED*.
11. **No file but the prompt's §6 list changes.** In particular `ExponentialModel.py`,
    `CosmologyConcepts/Models/__init__.py`, `ComputeTargets/ScalarHistory.py` and
    `ComputeTargets/tests/test_model_interface.py` are byte-identical. The new test module may
    import helpers from `test_model_interface.py` (`exponential_model`, `rel`) but not edit it.
12. **Timing and the suites.** Prompt §2.3's costs are minima of at least 1000 calls per method per
    M, with the load (`uptime`) beside them. Run `CLAUDE.md`'s commands serially, with
    ComputeTargets in its two halves; send output to a file in the agent's own scratch directory,
    grep the verdict from it, and record `uptime` beside each. A run that may pass 600 s (the
    second half, under this load) goes in the background, and the agent waits for its notification
    without polling. A failure is re-run alone before it is called real.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status`
  before committing. Count, then paste. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The five suites before any edit.
  2. The classical turning point, with items 1, the classical half of 2, and 3 on it.
  3. The one-loop turnaround, with the rest of items 2 and 3.
  4. The minimum and its derivative, with item 4.
  5. Items 5 and 6, ruling 1's import test; the breakages B1–B8; the measurement of §2.
  6. The suites and `black --check`.
  7. The records: log, `.documents/architecture-summary.md`, board, and
     `.documents/OPEN_ISSUES.md` if anything was opened.
- **Breakage diffs** are checked with `git apply --check` and `-R --check`, and replayed in `bash`,
  not zsh. None is committed. B1–B6 are the prompt's, in its order (wrong sign in V″; λ from V″;
  the exponential closed form for the generic root; the missing (ln A)″ term; a wrong factor in 𝓡;
  the iteration stopped after one step); B7 and B8 are rulings 2 and 3's.
- **Scratch.** The agent uses its own subdirectory of the session scratchpad, never `/tmp`. No
  scratch file goes under the repository. Any worktree is removed afterwards (`git worktree list`
  shows only the main checkout).
- **Stores and Ray.** No store is opened. No Ray is started. Nothing is written to `var/`. No
  integration is run, and no golden JSON is regenerated.
- **Stop conditions are real.** They are the prompt's §5, plus an existing test failing for a
  reason this note does not name. A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

Done at `eda13dd`, before this note was committed:
1. **The Gate above**, and the reading of §0.
2. **The import cycle**, by the orchestrator's hook probe (`scratchpad/orch04/cycle.py`), as in §0.
3. **The tolerance and the worked example**, by the orchestrator's probe
   (`scratchpad/orch04/tol.py`), as in §0. These are not given to the agent as code; it writes its
   own.
4. **The suites' baselines** are the agent's (Gate).

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout. Give it:
- the prompt, the README, the board and log 03;
- `HEAD` and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the prompt's §6 files. Its log is
  `logs/04-the-wall-through-the-model.md`, in `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **No Ray, no store, no integration.**
- **Stop and ask** on any of the prompt's §5 conditions or §0's.
- **It replies with:**
  - the log's path and the message as committed;
  - `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - the methods' names and signatures, and the namedtuples' fields;
  - its derivation of each method from the notes, in brief, with the equations by label;
  - the tolerances and each rounding bound of item 3, with the measured residuals;
  - the new tests by name, each with what it asserts and its tolerance;
  - the worked example's chain beside the notes' values, and m_ta² at ℓ = 32 and at its own ℓ;
  - the crossover tables of §2.2, and the jump in ln k_ex at 𝓡 = 1;
  - the costs per call;
  - each breakage, with the tests it failed and how;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — nine checks

1. **Scope.** `git show --stat <agent>` lists exactly the prompt's §6 files (plus
   `.documents/OPEN_ISSUES.md` only if something was opened), and its parent is this note's commit
   or a later commit that is not the agent's. `git status --porcelain --untracked-files=all` is
   empty, and nothing is pushed. `git diff <dispatch> <agent> -- ComputeTargets CosmologyModels
   CosmologyConcepts/Models/ExponentialModel.py CosmologyConcepts/Models/__init__.py ObjectFactories
   config main.py tools RunDrivers plot_inputs.py` shows only the new test module.
2. **The import.** `import CosmologyConcepts.Models` alone in a fresh interpreter, and after
   `import ComputeTargets`, both succeed; `AbstractModel.py` has no module-top import of
   `ComputeTargets` (ruling 1).
3. **The methods** against the prompt and rulings 2–7, line by line: logs throughout; the doubling,
   the monotonic check on values, the failure of a probe out of float range; the tolerances; the
   `None` cases; the one-loop iteration's stopping rule and its failure on ℓ ≤ 0 or no convergence;
   the minimum's equation, its sign logic and its derivative; nothing exponentiated that can
   overflow; the docstrings say fixed T against `log_m_eff_squared`'s fixed ρ̃_c.
4. **An independent probe** of my own (session scratchpad, not committed): through the hooks with
   my own root-finder, φ_t, ln 𝓡, k_ex at item 1's points; the worked example's chain; the n = 2
   and power-law models' defining equations at the returned values; φ_min and ∂φ_min/∂ ln T at two
   M. Compare with the methods' returns.
5. **The breakages.** Replay each diff with `git apply` in `bash`. Each fails the test the log
   names, and is reverted. B4, B6, B7 and B8 in particular.
6. **The numerics of the tests.** Each tolerance against its stated derivation (ruling 8); the
   worked example evaluated by the test and not by the methods (ruling 10); item 4's finite
   difference step against its truncation and rounding.
7. **The measurement.** The tables of §2 are complete; I recompute the 𝓡 = 1 crossover at the
   worked example's E myself and compare; the jump in ln k_ex is reported, not smoothed.
8. **The records.** The log is in §5.1's form, with each method's derivation by equation label, every
   deviation classed and §0's rulings among them. The `.documents/` note is dated and additive
   only. The board (row 04, gate Wall, §5.5) and `.documents/OPEN_ISSUES.md`, if anything was
   opened, agree with each other.
9. **The suites** at `<agent>`, serially: the counts the log gives, each at least the baselines,
   with ComputeTargets up by the new tests only, and the golden tests passing unchanged.
   `black --check` on the changed `.py` files.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, in the board, the log and `.documents/OPEN_ISSUES.md` wherever it
  appears;
- the board's row 04 landed, the gate Wall held, and this note added to the list of orchestrator
  notes, marked "used for 04";
- under `### 5.5`, an *Orchestrator review of prompt 04* paragraph holding what each check found.

Residue the review finds is fixed in that commit or a follow-up of its own. Anything that is not
residue is opened on the board's §3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the gate Wall;
- the counts;
- the worked example and the 𝓡 = 1 crossover, briefly, with the jump in ln k_ex;
- the costs, against 05's use on every infalling step;
- anything opened;
- the methods as handed to 05, 06 and 08;
- that 05 is written next, from 04's "State handed to the next prompt" (README §2.1).
