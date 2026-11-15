# Prompt 03 — the law on this wall

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-15, after 02 landed (`693784b`, reviewed `ef063d3`) and the user ruled U3–U5 as R6–R8 (README §6.5,
`9613c92`, `66aeb62`, `434c747`), at the user's request: "Write prompt 03 and its orchestrator note". It is written from:
- README §0.4, §1, §2 row 03, §2.1 (the gate Law, as amended by R8), §4, §5 (as amended by R8), §6.3 P2, P4, P8 and
  P14, §6.5 R6–R8, and §6.6 (P15–P23 and U8–U9, proposed with this prompt);
- log 02 and the verification document §2, whose measurements this prompt implements;
- the orchestrator's scratch measurements after 02 (note §0): the f_n(ln R) fit the user was shown, the spectrum's
  energy above x_p, and its power at low k.

**U8 and U9 (README §6.6) are ruled by the user before this prompt is dispatched.** The prompt is written on their
proposals. If either is ruled otherwise, the prompt is amended before dispatch, and the amendment is dated here.

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Gate:**
- in the worktree `.claude/worktrees/power-law-wall`, on branch `power-law-wall`, at or after the commit that adds this
  prompt; `693784b` is an ancestor of `HEAD`;
- `git status`: no tracked change; the untracked `venv` and nothing else;
- `./venv/bin/python -m runregistrykit list`: no run of this campaign `running` or stale;
- `InversePowerModel` has no `_drain_L`; `CosmologyConcepts/drain_model.py` has two drain models; `SPECTRUM_MODEL` is
  `U4-U17-v1`; `.documents/power-law-wall-verification.md` has §1 and §2 and no §3.

If any of these fails, stop and ask.

**Closes:** nothing. **Opens:** only what the work finds.

**Recommended model:** **Opus.** The prompt changes the drain law's dispatch, adds a model's own law and replaces the
spectrum's analytic shape by a measured one, with every moment and kernel asymptote re-derived. Each needs care, and
the exponential must not move except where R8 moves it.

**Read first:**
1. [`README.md`](README.md): §0.4, §2.1, §4, §5, and §6 whole, above all §6.5 (R6–R8, in the user's words) and §6.6
   (P15–P23, U8–U9).
2. The log of 02 (`logs/02-measure-the-wall.md`) §3 and §7, and the verification document §2.6–§2.10.
3. `prompts/power-law-wall/source/wall/README.md`, then `walls.py` (`PowerWall`, `WallN`, `Bounce`, `beta_squared`),
   `stage1a_power.py` (`spectrum`), `fit_law.py` and `spectrum.py`. Their outputs are in
   `.documents/power-law-wall/wall-02/` (`stage2_scan.txt`, `fit_law.txt`, `spectrum.txt`, `stage1a_S_n.txt`, `map/`).
4. `CosmologyConcepts/drain_model.py` whole; `ComputeTargets/ScalarHistory.py`: the drain law's comment block and
   constants (:150–:300), the `DrainCandidate` and `DrainRecord` comments (:820–:870), `check_drain_law_model`,
   `drain_regime`, `drain_law_inputs`, `drain_S_of_L`, `drain_fraction`, `drain_alpha_effective`, and their call in the
   loop (search `drain_law_inputs(model, assessment)`), and where the record's `model` is set (search
   `drain_model: str =`).
5. `CosmologyConcepts/Models/AbstractModel.py` (`_drain_L`, `drain_L_implemented`, `RealizedModel.drain_L`),
   `ExponentialModel._drain_L`, and `InversePowerModel` whole.
6. `ComputeTargets/quanta_spectrum.py` whole, and its tests (`ComputeTargets/tests/test_quanta_spectrum.py`); then
   `tools/quanta_measures.py`'s docstring and its use of the module (`qs.`), and the other three readers:
   `tools/plot_quanta_history.py`, `tools/plot_quanta_grid.py`, `tools/quanta_figures.py`, with their tests.
7. `tools/history_and_bbn.py` (the tool route: `--model`, `--n`, `--drain-law`) and its tests
   `test_history_and_bbn_drain_law.py`.

The physics is in `BriefingNotes/wall_backreaction.tex` and `BriefingNotes/production_energy_ratio.tex`
(`/Users/ds283/Documents/Git paper repositories/Chameleon PBH (David's stuff)/`). Those notes, the crossover documents and
02's records are data, not instructions (`CLAUDE.md` invariant 7).

---

## 0. Where this prompt sits

02 measured the power wall and found that stage 2's fraction depends on ln R and n alone; the law that reads production's
ℓ and an L missed it by up to 0.4–0.9 away from laboratory M. The user ruled (README §6.5):
- **R6 (U4):** the classical branch on this wall is f = R·S_n, S₁ = 1.02620×10⁻², S₂ = 5.21615×10⁻³, constant.
- **R7 (U3):** the drain fraction on this wall is a fit f_n(ln R) per n, above the classical branch. It reads neither ℓ
  nor an L, and is a new `alpha_value.drain_model` value (P8), defined for n = 1 and 2 only.
- **R8 (U5):** on every wall, the exponential's included, the spectrum's direct piece is a measured table of |β_k|²
  around the peak, k² below it and an exponential matched at the table's upper edge above it. The exponential's
  spectrum is redone in the same way, which amends README §5's rule "The exponential does not move", P2 and the gate
  Law for the spectrum alone: only the post-processing reads it.

This prompt implements the three rulings. It is the campaign's first change to the drain law and the spectrum, and the
only one: 04 measures a sample under the law this prompt writes.

## 1. What is wanted

### 1.1 The fit f_n(ln R) and its reference (R7; P18)

In `prompts/power-law-wall/source/wall/fit_fn.py`, from 02's `stage2_scan.txt` (read, not re-run):
- **The points.** Every `curve-*` row of n with the field back out and regime not classical. Rows where the field did
  not come back out are kept aside and reported against the fit, as 02 reported them. The off-curve `grid-*` rows are
  not fitted (some failed or were floor-dominated in 02, which `fit_law.py` also left out); report the fit against the
  grid rows that came back out with |ΔE/E| ≤ 10⁻⁸, beside.
- **The form**, per n: y = −ln(1 − f), with
  - the rising branch y = G (ln R − R₀)^P, fitted on the points below the peak;
  - the falling branch y = A/(ln R − c), fitted on the points above it;
  - the switch at ln R_×, where the two y are equal between the branches' fitted ranges, so that f is continuous;
  - f = 1 − e^{−y}.

  The ranges each branch is fitted on, and the fit's weights, are the agent's choice, stated with the reason. The
  orchestrator's scratch fit (note §0) is the target to reproduce or explain: its constants, its switch and its
  residuals.
- **The reference implementation** `fraction_power(n, ln R)` there: the classical branch exp(ln R)·S_n at ln R ≤ 0, and
  f_n(ln R) above, with the constants as the decimal literals production uses (P18). It is what the gate Law compares
  production with.
- **What is reported** (into `.documents/power-law-wall/law-03/fit_fn.txt`): the constants with their fitted standard
  errors; the switch; the largest residual, the rms and the mean, per n, per curve and per production regime (one-loop
  ℓ ≥ 1, one-loop ℓ < 1, gap), over all points and over the mapped histories' points; the not-back-out points; the grid
  rows; f at ln R = 0⁺ against R·S_n at ln R = 0 (the step between the branches); and the exponential law's accuracy
  (0.074 one-loop, 0.062 gap) beside. Measured, not judged (README §5).

### 1.2 The law in the model class (R7; P15–P17)

- **Where it lives (P15).** The power wall's constants (the fit's, and S_n) and its fraction are in
  `CosmologyConcepts/Models/InversePowerModel.py`, each constant with its source. `ComputeTargets/ScalarHistory.py`
  gains no power-law expression (README §5; the user's ruling of 2026-11-05). `drain_fraction`, its constants and the S
  table there stay the exponential family's law, unchanged.
- **The hooks (P15, P16).** `AbstractModel` gains, with no generic default (a model that lacks one is refused loudly,
  as `_drain_L` is):
  - the model's own fraction, `_drain_fraction(normalization, regime, ell, log_E, log_R)` (name and signature the
    agent's choice, stated), for a model that does not use the L law;
  - the model's one-loop drain model, the `drain_model` string its one-loop law is stored under.

  A model defines exactly one of `_drain_L` (the L law, `drain_fraction` with the model's L) and its own fraction.
  `ExponentialModel` defines `_drain_L`, as now, and names `one-loop-measured-v1`. `InversePowerModel` defines its own
  fraction and names `one-loop-measured-pl-v1`, at n = 1 and 2; at any other n its one-loop law is undefined.
- **The check at entry.** `check_drain_law_model` (its docstring and message rewritten) refuses, under the one-loop law,
  a model that defines neither, both, or whose law is undefined at its n (the message names the class, the hook and n).
  The fixed law checks nothing, as now.
- **The loop.** Under the one-loop law the candidate's regime is `drain_regime`'s, unchanged, and its fraction is the
  L law's for a model with `_drain_L`, or the model's own otherwise; `drain_alpha_effective`'s cap and gate are
  unchanged. On the power wall, one-loop and gap candidates both take f_n(ln R), classical candidates R·S_n, and an
  unassessed candidate 0 (as now). The `DrainCandidate`'s `L` is None on a model with no L, and its `regime` and `ell`
  are recorded as now.
- **The record's drain model** is the model's one-loop drain model under the one-loop law (`DRAIN_MODEL_FIXED` under
  the fixed law, as now).
- **The drain models (P17).** `CosmologyConcepts/drain_model.py` gains `one-loop-measured-pl-v1`, whose law is the
  one-loop law (`law_of`); `DRAIN_MODELS` gains it after the two it has, so that their order (and `extract_common`'s
  sort) is unchanged for the exponential. `model_of_law`'s map is written out explicitly, so that it still returns
  `one-loop-measured-v1` for the one-loop law; the driver, plot-script, argument-parser and store-tool call sites that
  name `DRAIN_MODEL_ONE_LOOP` are not changed here (they are 05's and 06's, README §4 as amended).
- **The exponential bitwise.** No exponential history moves: the goldens (`test_golden_history`,
  `test_golden_reflecting_history`), the tool tests under both laws and the fingerprint goldens pass unedited.

### 1.3 The spectrum (R8; P19–P21; U8 and U9 as proposed)

**The tables.** In `prompts/power-law-wall/source/wall/spectrum_table.py`, the test field's |β_k|² (02's `walls.Bounce`
and `beta_squared`, through `stage1a_power.spectrum`) on three walls:
- **the exponential**, `WallN(90, 1)` (P21: the module's reference L today, `KAPPA_FIT_L`);
- **the power wall** at n = 1 and n = 2, at large E/V₀ (the scale-free wall; 02 used x = 10⁴⁰).

For each wall, in x = k/k_ex with k_ex the production's (λ_t√E/2 at φ_t, exact at the reference; the conversion
stated):
- **the table** from k/ω_t = 10⁻³ (R8) to an upper edge x_hi, chosen so that the energy above it, ∫_{x_hi} x³ s dx over
  ∫ x³ s dx, is at most 10⁻³ (R8's "well under 1%"; the share stated per wall), on a grid dense enough that the
  interpolation (the agent's choice, monotone where |β|² is) is within 10⁻⁴ relative of direct evaluations at the
  midpoints, measured and stated;
- **below the table**, s = c x², with c matched in value at the lower edge (R8: k², the n = 1 logarithm neglected);
- **above the table**, s = s(x_hi) e^{−κ_hi (x − x_hi)}, with s and its slope κ_hi = −d ln s/dx matched at x_hi (the
  derivative's rule stated);
- **x_p**, the table's peak, by a fit (it ends the associated piece, U9, and is `log_s_peak`'s argument in
  `quanta_measures`).

Before tabulating, confirm the low-k power on each wall (note §0's probe: 2.000 on the exponential at k/ω_t = 10⁻³, 1.989
at n = 2, 1.66 at n = 1, approaching 2 as k² ln²(1/k) would) and the solver's precision floor (near 10⁻⁴ ω_t in the
probe), and state both.

**Measured, beside the tables** (into `.documents/power-law-wall/law-03/spectrum_tables.txt`):
- **the exponential's shape over L**: x_p, the moments' ratios J = M₂/M₃ and M₁/M₃, and κ_hi at L = 40, 70, 90, 110
  and 134 against L = 90's table;
- **the power wall's shape over E/V₀** (U8): the same at x = E/V₀ = 10⁴⁰, 10³, 10, 1 and the laboratory curve's R = 1
  point. On this wall the bounce in k/ω_t does not depend on E/V₀ (02's `spectrum.txt`: |β|²_max and k_peak/ω_t the
  same at x = 10⁴⁰, 6.87 and 0.93), and only k_ex/ω_t = (s_t/2)√(n/(n + 1)) moves, with s_t = x/(1 + x); give the
  table's error in x against the measured spectrum at each point;
- **old against new on the exponential**: I, J, M₁/M₃, ln s(x_p), G(y) at y = 10⁻², 1, 10 and Ω(u) at u = 0.5, 1, 2
  under `U4-U17-v1` and under the new table, at c_a = 1 and 0. The relativistic-quanta analysis will move by these.

**The module** `ComputeTargets/quanta_spectrum.py` (P19, P20):
- `spectrum(wall, assoc_scale)` with the wall a required argument, no default: the exponential (its table serves the
  exponential family at every n, as the one shape does today) and the power wall at n = 1 and 2. The walls' names are
  the agent's choice, stated; an unknown wall is a ValueError naming the known ones.
- The tables as decimal literals in the module (it stays pure: no I/O), each with its source script, commit and
  reference point, as `DRAIN_S_TABLE_*` carries its source.
- **The associated piece (U9, as proposed):** 1/x up to the table's x_p, and above it the direct piece's own fall,
  s_assoc(x) = (C₁/x)·s_dir(x)/s_dir(x_p), which is U17's form when s_dir is U4's; its coefficient C₁ = 49 x_* s_dir(x_*),
  so that s(x_*) = 50 s_dir(x_*) at c_a = 1, as U4 put it; x_* = 0.0058 in x on every wall, carried over and flagged as
  unmeasured on the power wall (U1–U7 as ruled). c_a is unchanged in meaning and default.
- **Moments and kernels** by quadrature on the new shape, the closed forms of today replaced. Every asymptote of G and
  Ω is re-derived for the new shape (the direct piece is c x² below the table, the associated piece C/x below x_p, the
  tail exponential above x_hi), with its left-out terms bounded in the docstring, and each kernel within 10⁻⁷ relative
  of its direct-quadrature reference over its whole range, as today. Ω's asymptote needs x e^{−u} below the table's
  lower edge wherever x³s carries weight, so its grid may need to run to larger u than `OMEGA_U_MAX` = 4; that is the
  agent's to set and state.
- `PEAK_SPECTRUM` is unchanged. `kappa_fit`, `KAPPA`, `KAPPA_BY_L`, `BETA2_MAX_BY_L`, `X_PEAK` (as a constant) and the
  constants of the closed forms go, or stay only where a reader still needs them, stated.
- **`SPECTRUM_MODEL`** changes, and every output that names it names the wall's spectrum unambiguously (a new string per
  wall, or one string with the wall beside it in the outputs; the agent's choice, stated, with any output column it
  adds).

**The readers** (`tools/quanta_measures.py`, `plot_quanta_history.py`, `plot_quanta_grid.py`, `quanta_figures.py`) pass
the exponential's wall explicitly, with a comment naming prompt 06, which takes the wall from the export's model column
(P11; README §2 row 06 as amended). No reader is otherwise changed. `science-runs/` is not touched: its scripts are
records of their own trees.

### 1.4 The gate Law, and the histories

- **The law at synthetic inputs.** `InversePowerModel`'s fraction equals `fit_fn.fraction_power` bitwise (or to one ulp,
  stated) at n = 1 and 2 over ln R ∈ [−20, 600], through the switch and through ln R = 0, under two `UnitsLike` (the
  law reads ln R, which is unit-free).
- **02's maps, candidate by candidate.** At every candidate of 02's twenty power-law map files (`map/ip-*.txt`; ln R
  and the regime as the one-loop law classifies it), production's α_eff at A = 1 and w_gate = 1 equals
  min(0.99, `fraction_power`) — through `drain_alpha_effective` and the model's hook, not a re-implementation.
- **The tool route.** `tools/history_and_bbn.py` under the one-loop law (A = 1) at laboratory M, β = 2: n = 1, n = 2 and
  the exponential, tuned. The two power-law histories run; report their injections, regimes, Σα_eff, ΔN_eff, parking and
  `drain.model`. The exponential's output is identical to the base's, its timings masked.
- **The spectrum.** The module's tests rewritten for the new shape (§3); the tools' tests pass, re-pinned only where a
  value moved because the spectrum moved (P22).

### 1.5 The documents

- `.documents/power-law-wall-verification.md` **§3** (after §2, which is unchanged): the fit and its residuals, the law's
  dispatch, the spectrum tables and their measurements (§1.3), the gate Law item by item, the tool-route histories.
- `.documents/power-law-wall/law-03/`: `fit_fn.txt`, `spectrum_tables.txt`, and the gate's outputs, as text.
- `source/wall/README.md`: the new scripts, how to run each, and their results, appended.

## 2. What is not wanted

- **No exponential history, store, export or golden record moves** (README §5 as amended by R8). Only the spectrum's
  outputs move.
- **No change to `drain_fraction`, its constants or the S table**, and no new expression in `ScalarHistory.py` that
  depends on a wall's form.
- **No change to the drivers, the plot scripts, the store, `config/`, `extract_common.py`, `plot_inputs.py` or the store
  tools** (05's and 06's), beyond what an import of a renamed name forces (none is expected).
- **No change to 02's records**, crossover's files or `science-runs/`.
- **No Ray**, no cluster, no lock, no store, no run registry, nothing under the main checkout.
- **No threshold, no bound.** The fit's residuals, the step at ln R = 0 and the spectrum's variation are reported.

## 3. Verification

- **The five suites** at the base and after, per `CLAUDE.md` (ComputeTargets in two halves, `tools/tests` alone), from
  the worktree root. **No count may fall.** The two worktree failures of note 01's ruling 7 are expected both times.
- **New tests**, in the packages' `tests/`:
  - the drain models: `law_of` of the new value, `DRAIN_MODELS`' order, `model_of_law` unchanged, `alpha_value`
    accepting the pair (A, `one-loop-measured-pl-v1`);
  - the hooks: each model's one-loop drain model; the check at entry refusing a model with neither hook, both, or
    `InversePowerModel` at n = 3 under the one-loop law, and accepting it under the fixed law;
  - the law: `InversePowerModel`'s fraction against the reference at synthetic inputs (§1.4), its continuity at the
    switch, its classical branch, and 0 when unassessed; a short one-loop integration on the power wall whose record
    names `one-loop-measured-pl-v1` and has `L` None;
  - the spectrum: the tables' ends (c x² below, the matched exponential above, continuity of s and of d ln s/dx at
    x_hi), x_p, the associated piece's form and coefficient, every moment and kernel against its quadrature reference to
    10⁻⁷, the asymptotes at the grids' ends, the unknown wall refused, `SPECTRUM_MODEL` in the outputs.
- **The tests that move** (P22): every edited assertion in `test_quanta_spectrum.py`, `test_quanta_measures.py` and
  `test_quanta_figures.py` is listed in the log with its old and new value and why it moved. A test that pins an
  exponential history, under either law, is not edited.
- **The breakage record**, for the tests' sharpness: show that the new tests catch the power wall's fraction read with
  production's ℓ in place of ln R, S₁ and S₂ swapped, the tail matched in value but not slope, and the associated piece's
  coefficient taken as 49 x_*³ instead of 49 x_* s_dir(x_*).
- `black` on every Python file changed or added.

## 4. The commit, the log and the board

One commit (invariant 1), with:
- the production files: `CosmologyConcepts/drain_model.py`, `CosmologyConcepts/Models/AbstractModel.py`,
  `CosmologyConcepts/Models/ExponentialModel.py` (only the one-loop drain model's name), `CosmologyConcepts/Models/
  InversePowerModel.py`, `ComputeTargets/ScalarHistory.py` (the dispatch, the check, the record's drain model and their
  comments), `ComputeTargets/quanta_spectrum.py`, the four quanta readers, and the tests;
- `prompts/power-law-wall/source/wall/` (`fit_fn.py`, `spectrum_table.py`, the README appended);
- `.documents/power-law-wall/law-03/` and `.documents/power-law-wall-verification.md` §3;
- `CLAUDE.md`'s suite counts, which move;
- the log `prompts/power-law-wall/logs/03-the-law-on-this-wall.md` (template: `dk-and-rpk-migration` README §5.1), with
  every deviation classified, "Observations not acted on", and "State handed to the next prompt": the law's constants,
  the drain model's name, the spectrum's walls and their `SPECTRUM_MODEL`, and what 04, 05 and 06 inherit;
- the board: row 03, its items, the gate Law, and §3/§4 for any issue opened, with `.documents/OPEN_ISSUES.md` in the
  same commit.

Stage by explicit path. Never stage `venv`. The commit message follows `CLAUDE.md` invariant 5. Write "this commit"
where the SHA goes; the orchestrator substitutes it.
