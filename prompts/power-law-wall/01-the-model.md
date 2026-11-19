# Prompt 01 — the model

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-13, after the plan (`c540caa`) and the user's rulings of the same day: R1–R3 (README §6.1) and
"U1–U7 as proposed" (§6.2). It is written from:
- README §0.2 (the form), §0.4 (what the code has), §2.1 (the Model gate), §5, and §6.3 P1–P5 (P5 as amended
  2026-11-13) and P14;
- `CosmologyConcepts/Models/ExponentialModel.py` and `AbstractModel.py`, and the test-only `_PowerLawWall` in
  `ComputeTargets/tests/test_wall_model.py:105`.

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Gate:**
- in the worktree `.claude/worktrees/power-law-wall`, on branch `power-law-wall`, at or after the commit that adds
  this prompt; `c540caa` is an ancestor of `HEAD`;
- `git status`: no tracked change; the untracked `venv` (a symlink to the main checkout's venv) and nothing else;
- `./venv/bin/python -m runregistrykit list`: no run of this campaign. The 2026.11.0 science run may be `running`;
  it builds from the main checkout and is never touched;
- `CosmologyConcepts/Models/InversePowerModel.py` does not exist, and `model_ids.py` defines `EXPONENTIAL_MODEL`
  only;
- `.documents/power-law-wall-verification.md` does not exist.

If any of these fails, stop and ask.

**Closes:** nothing. **Opens:** only what the work finds.

**Recommended model:** **Opus**. The class is short, but its hooks must stay finite across about 90 decades of φ,
and the generic wall searches in `AbstractModel` meet a wall they were not written against.

**Read first:**
1. [`README.md`](README.md): §0 whole, §2.1 (the Model gate), §5, §6.1, §6.3 P1–P5 and P14.
2. `CosmologyConcepts/Models/AbstractModel.py`: the interface (`RealizedModel`, the abstract hooks, `_log_V_floor`,
   `reflects_at_origin`, `_drain_L` and `drain_L_implemented`, `renormalize`, the normalization coordinate, `seed`),
   and the wall searches `classical_turning_point`, `one_loop_turnaround`, `minimum_on_trace` and
   `d_minimum_d_log_trace` (their docstrings state what each returns and raises).
3. `CosmologyConcepts/Models/ExponentialModel.py` whole: the form every hook takes, the overflow handling, and the
   analytic seed (`_analytic_seed`, `seed`).
4. `ComputeTargets/tests/test_model_interface.py` (the exponential's interface tests, the seed's at :408) and
   `ComputeTargets/tests/test_wall_model.py` (the defining-equation tests, :330–:600, and `_PowerLawWall`).
5. `ComputeTargets/ScalarHistory.py`: `check_drain_law_model` (:1861) and its three call sites.
6. `tools/history_and_bbn.py` (its docstring, `main`, where the model is built, :511) and `tools/surfing_scan.py`
   (:289), with their tests in `tools/tests/`.

---

## 0. Where this prompt sits

The campaign adds V = V₀[1 + (M/φ)ⁿ] beside the exponential V₀e^{(M/φ)ⁿ}, with the same coupling, and takes it
through the pipeline under both drain laws. This prompt adds the model class and lets the tool route run it under
natural mode and the fixed law. The one-loop law needs the wall's L, which 02 measures and 03 defines, so this prompt
leaves `_drain_L` unimplemented and the one-loop law refuses the model.

Nothing in this prompt changes the store, `main.py`, the plot scripts, the integrator or any exponential code path.

## 1. What is wanted

### 1.1 The class (P1)

`CosmologyConcepts/Models/InversePowerModel.py`, a subclass of `AbstractModel` (not of `ExponentialModel`), with
`INVERSE_POWER_MODEL = 2` added to `model_ids.py` and the class exported from `CosmologyConcepts/Models/__init__.py`
in the order its comment requires.

- **The constructor** takes `(store_id, M: M_value, n: int, beta: beta_value, units)`, as the exponential's does;
  n is an integer ≥ 1 (an assertion).
- **The potential.** With u = (M/φ)ⁿ and ln u = n ln(M/φ):
  - ln V = normalization + ln(1 + u), computed from ln u so that nothing overflows: ln u + log1p(e^{−ln u}) for
    ln u > 0, log1p(e^{ln u}) otherwise;
  - (ln V)′ = −n s/φ and (ln V)″ = [n(n + 1)s − n²s²]/φ², with s = u/(1 + u) computed from ln u (as
    `_PowerLawWall._s` does, including its branch for very negative ln u).
- **φ ≤ 0 and values that cannot be represented** raise `ComputationFailureError` naming the model, φ in M_P and the
  hook, as the exponential's hooks do for overflow and `ValueError`. ln(M/φ) at φ ≤ 0 is otherwise a bare
  `ValueError` from `math.log`, which escapes the integrator (orchestrator note §0). Whether a different continuation
  to φ ≤ 0 is wanted is not decided here (§1.6).
- **The coupling** is the exponential's: ln Ω = βφ/M_P, (ln Ω)′ = β/M_P, (ln Ω)″ = 0. The three methods are written
  in this class, not shared with `ExponentialModel` by a refactor (P2: no exponential code path changes).
- **The floor and the wall.** `_log_V_floor` returns the normalization (ln V₀); `reflects_at_origin` is True, since V
  rises without bound as φ → 0⁺ and V′ < 0 for every φ > 0. Each with a comment saying why, in the exponential's
  form.
- **No `_drain_L`.** The base raises, and `check_drain_law_model` refuses the model under the one-loop law (P4). A
  comment says that 02 measures the wall and 03 defines L, and why no definition is guessed here.
- **`renormalize`, the coordinate and the name.** The normalization is ln V₀ and multiplies V, so `renormalize` is
  normalization + ln V_target − ln V(φ), with V_target ≤ 0 a `ValueError`, and the coordinate is the normalization
  itself. `name` is `InversePowerModel(M=…,n=…,beta=…)` in the exponential's format; `type_id` is
  `INVERSE_POWER_MODEL`; `shard_key` is β.
- **The docstring** states the form, the user's ruling R1 in their words, the relation to the exponential (equal to
  O((M/φ)^{2n}) for φ ≫ M), and that `_drain_L` waits for 02 and 03.

### 1.2 The seed (P3)

`seed` returns the tracking analytic minimum, else the static one, else the base class's plain start, as the
exponential's does, but at **every n**. The exponential's loop solves V′ = −V M/φ² (n = 1 only). Here V′ = −n(V − V₀)/φ
exactly, so at A(φ_ref) = 1 the minimum today satisfies

    n V u / ((1 + u) φ) = β ρ_m0 / M_P,    u = (M/φ)ⁿ,

with V from the same Friedmann condition the exponential's loop uses for each kind, and the normalization
ln V − ln(1 + u). Solve it in ln φ by a bracketed root-find or by the exponential's fixed-point form, whichever
converges, and return None where a kind has no solution, under the exponential's conditions (V, G, 1 + βφ′/M_P
positive). State the method and its stopping rule in the docstring.

### 1.3 Its tests

`ComputeTargets/tests/test_inverse_power_model.py` (new), each test's runtime in its docstring:
- **The hooks** against closed forms (V − V₀ = V₀(M/φ)ⁿ; V′ = −nV₀Mⁿ/φ^{n+1}; V″ = n(n + 1)V₀Mⁿ/φ^{n+2}, through
  `RealizedModel`'s V″ = V[(ln V)″ + (ln V)′²]) and against central differences of the hook below, at n = 1 and 2,
  M = 4×10⁻³¹ and 10⁻³ M_P, and φ from 10⁻⁹⁰ M to 10 M_P. Each tolerance stated with its reason.
- **φ ≤ 0** raises `ComputationFailureError` from each hook, naming the model.
- **The wall searches against closed forms**, at n = 1 and 2:
  - `classical_turning_point`: φ_t = M(V₀/E)^{1/n}, ln ω_t² = ln[n(n + 1)E] − 2 ln φ_t, λ_t = n s_t/φ_t with
    s_t = (E/V₀)/(1 + E/V₀), and ln R, ln k_ex and ℓ_t from the docstring's definitions;
  - `minimum_on_trace`: V′ is exactly −nV₀Mⁿφ^{−n−1}, so φ_min = (nV₀Mⁿ M_P/(β T))^{1/(n+1)};
  - `d_minimum_d_log_trace`: its closed form, −φ_min/(n + 1);
  - `one_loop_turnaround`: no closed form; its defining equations hold at the returned values, as
    `test_wall_model.py`'s (3) checks them.
- **In `test_wall_model.py`**: `InversePowerModel` at n = 1 and 2 is added beside the exponential with n = 2 and
  `_PowerLawWall` in `_NO_CLOSED_FORM` (or the module's equivalent list), so that every generic test there runs on it.
  `_PowerLawWall` stays: its γ and its `reflects`/`floor` switches test things the production class does not have.
- **The seed** at n = 1 and 2, at the β of `test_model_interface.py`'s seed tests: its kind, and its φ_ref and
  normalization against an independent root-find of §1.2's equation.
- **`renormalize`** reaches its target, refuses V_target ≤ 0, and the coordinate round-trips exactly.
- **The one-loop law refuses it**: `check_drain_law_model` raises naming the class and `_drain_L`; the fixed law
  accepts it.
- **A short integration** under natural mode and under the fixed law (α = 0.1), untuned at a fixed normalization,
  to a stopping temperature chosen so the test takes under about 30 s: it succeeds, and its reflections and samples
  are stated. If no such integration is short enough, say so and leave the integration to §1.5's tool runs.
- **Pickling** round-trips, as the exponential's test (h) checks.

### 1.4 The tool route (P5 as amended)

`tools/history_and_bbn.py` and `tools/surfing_scan.py` gain `--model {exponential, inverse-power}` (default
exponential) and `--n` (default 1). With neither given, or with their defaults given, every line each tool writes is
byte-identical to today's: the tests that pin those lines pass unchanged. Under `--model inverse-power` each output
names the model and n (in the label `history_and_bbn` prints, and on `surfing_scan`'s stdout), so that no two models'
lines can be mistaken for each other; `surfing_scan`'s CSV gains no column. `--drain-law one_loop` with
`--model inverse-power` exits non-zero with `check_drain_law_model`'s message, before any integration. The docstrings
say all of this. Tests in `tools/tests/` cover each statement.

Crossover's `cascade_map.py` is a record of a closed campaign and is not changed (P5 as amended).

### 1.5 The runs, and the verification document

By the tool route, tuned, from the worktree root (no Ray, no store), each its own process, three at a time:

| n | M / M_P | β | law |
|---|---|---|---|
| 1, 2 | 4×10⁻³¹ | 1.2, 2.0, 2.9 | fixed, α = 0.1 |
| 1, 2 | 10⁻³ | 1.2, 2.0, 2.9 | natural (α = 0) |

That is twelve. For each: success or the failure and its reason, the wall time, the tuning's integrations and seed
kind, reflections, injections and `log_R_first_impact`, ΔN_eff (today and 1 MeV), Y_p and D/H. Beside each, the same
(M, β, row) from the exponential, run the same way on this tree. The script and its output go in
`.documents/power-law-wall/model-01/` (text only).

`.documents/power-law-wall-verification.md` (new), §1: the class and its hooks, the tests and their tolerances, the
tool route, and these runs, measured and not judged.

### 1.6 Natural mode at large M (a measurement, not a fix)

The planner's probe found the natural-mode histories at 10⁻³ M_P failing at φ ≤ 0 on both n, and the n = 2 one
warning that its classical turning point is 9.4 × 10⁻²⁷ M_P (orchestrator note §0). Under §1.1 they will fail with a
stored reason. Establish, and record in the log and the verification document:
- where each fails (N, φ, π, the step and whether the point at φ ≤ 0 was a trial stage or an accepted step) and the
  turning-point depth φ_t at that impact;
- in a **scratch probe that is not committed**, what the same histories do if the hooks are continued to φ < 0 as the
  even extension V(|φ|), with (ln V)′ odd and (ln V)″ even: whether the step control then rejects the trial points,
  and whether any accepted step has φ ≤ 0.

This is for the user's ruling. Nothing in the integrator changes here.

## 2. What is not wanted

- **No exponential change.** No exponential hook, constant or code path; no refactor that moves its code. Its goldens
  are bitwise (P2).
- **No `_drain_L`**, and no definition of L "for now".
- **No store**: no factory, table, registry entry, `types` map, fingerprint or `VERSION_LABEL` change. They are 05's.
- **No driver or plot script** (`main.py`, `plot_*.py`, `plot_inputs.py`, `extract_common.py`, `pipeline_selection.py`):
  06's.
- **No integrator change** (`ComputeTargets/ScalarHistory.py`, the policies). If the model cannot run without one,
  stop.
- **No Ray**, no cluster, no lock; no store but temporary ones in tests; nothing under the main checkout.

## 3. Verification

- The suites, before you change anything and after, per `CLAUDE.md` (ComputeTargets in two halves, tools alone),
  from the worktree root with `./venv/bin/python`. Record both sets of counts and anything that fails on the baseline.
  **No count may fall.**
- The exponential goldens (`test_golden_history`, `test_golden_reflecting_history`, `test_tuning` (e)) bitwise, and
  `test_store_fingerprint_golden` and `test_fingerprint_format_guard` unchanged and passing.
- The Model gate (README §2.1), item by item.
- **The breakage record:** each new test shown to fail when what it guards is broken (a hook's sign, the seed's
  equation, the φ ≤ 0 guard, the tools' default output).
- `black` on the Python files you changed or added.

## 4. The commit, the log and the board

One commit (invariant 1), with:
- `CosmologyConcepts/Models/InversePowerModel.py`, `model_ids.py`, `__init__.py`;
- `ComputeTargets/tests/test_inverse_power_model.py` and `test_wall_model.py`;
- `tools/history_and_bbn.py`, `tools/surfing_scan.py` and their tests;
- `.documents/power-law-wall/model-01/` (scripts, and `out/` as text only) and
  `.documents/power-law-wall-verification.md`;
- the log `logs/01-the-model.md` (template: `dk-and-rpk-migration` README §5.1), with every deviation classified,
  "Observations not acted on", and "State handed to the next prompt", which includes §1.6's findings and the twelve
  runs' `log_R_first_impact` for 02;
- the board: row 01, its items, the Model gate, and §3/§4 for any issue opened, with `.documents/OPEN_ISSUES.md` in
  the same commit;
- `CLAUDE.md`'s test counts.

Stage by explicit path. Never stage `venv`. The commit message follows `CLAUDE.md` invariant 5. Write "this commit"
where the SHA goes; the orchestrator substitutes it.
